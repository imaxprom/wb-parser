#!/usr/bin/env python3
"""Single durable worker for MpHub authorized WB card-stock snapshots."""

import argparse
import base64
import datetime as dt
import hashlib
import hmac
import json
import logging
import os
import socket
import sqlite3
import time
import uuid
from pathlib import Path
from urllib.parse import urlsplit

from curl_cffi import requests as curl_requests
from playwright.sync_api import sync_playwright

import config
import proxy_positions
from wb_session_runtime import refresh_saved_session
from wb_health import (
    ANTIBOT_HTTP_STATUSES,
    RATE_LIMIT_HTTP_STATUSES,
    WB_CURL_IMPERSONATE,
    WbAntibotError,
    WbAuthExpiredError,
    WbRateLimitError,
    build_antibot_error,
    get_access_health,
    probe_delay_seconds,
    record_antibot,
    record_auth_expired,
    record_network_error,
    record_rate_limit,
    record_success,
)


WB_CARD_ENDPOINT = "https://www.wildberries.ru/__internal/card/cards/v4/detail"
WB_CARD_PATH = "/__internal/card/cards/v4/detail"
BATCH_SIZE = 10
BATCH_DELAY_SECONDS = 0.5
HEARTBEAT_SECONDS = 60
SITE_TIMEOUT_SECONDS = 30
WB_TIMEOUT_SECONDS = 30
MAX_OUTBOX_DELIVERY_ATTEMPTS = 18
WB_HEALTH_SCOPE = "cart_stock"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("cart-stock-worker")


def systemd_notify(message: str):
    """Notify systemd when this process is ready and still making progress."""
    notify_socket = os.getenv("NOTIFY_SOCKET")
    if not notify_socket:
        return
    address = f"\0{notify_socket[1:]}" if notify_socket.startswith("@") else notify_socket
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as client:
            client.connect(address)
            client.sendall(message.encode("utf-8"))
    except OSError as error:
        logger.warning("Could not notify systemd: %s", error)


class SiteRequestError(RuntimeError):
    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


class Outbox:
    def __init__(self, path: str):
        self.connection = sqlite3.connect(path)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS cart_stock_outbox (
                job_id INTEGER PRIMARY KEY,
                payload_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT
            )
            """
        )
        self.connection.commit()

    def put(self, job_id: int, payload: dict):
        self.connection.execute(
            """
            INSERT INTO cart_stock_outbox (job_id, payload_json, created_at)
            VALUES (?, ?, ?)
            ON CONFLICT(job_id) DO UPDATE SET payload_json=excluded.payload_json
            """,
            (job_id, json.dumps(payload, ensure_ascii=False), time.time()),
        )
        self.connection.commit()

    def rows(self):
        return self.connection.execute(
            "SELECT job_id, payload_json, attempts FROM cart_stock_outbox ORDER BY created_at, job_id"
        ).fetchall()

    def delete(self, job_id: int):
        self.connection.execute("DELETE FROM cart_stock_outbox WHERE job_id = ?", (job_id,))
        self.connection.commit()

    def mark_error(self, job_id: int, error: str):
        self.connection.execute(
            """
            UPDATE cart_stock_outbox
            SET attempts = attempts + 1, last_error = ?
            WHERE job_id = ?
            """,
            (error[:2000], job_id),
        )
        self.connection.commit()

    def count(self) -> int:
        return int(self.connection.execute("SELECT COUNT(*) FROM cart_stock_outbox").fetchone()[0])


def utc_iso(timestamp: float | int | None = None) -> str:
    value = time.time() if timestamp is None else timestamp
    return dt.datetime.fromtimestamp(value, dt.timezone.utc).isoformat().replace("+00:00", "Z")


def bearer_expiry(token: str) -> int | None:
    try:
        part = token.split(".")[1]
        part += "=" * (-len(part) % 4)
        payload = json.loads(base64.urlsafe_b64decode(part))
        return int(payload.get("exp"))
    except Exception:
        return None


class CartStockWorker:
    def __init__(self):
        if not config.MPHUB_CART_STOCK_URL:
            raise RuntimeError("MPHUB_CART_STOCK_URL is not configured")
        if not config.MPHUB_CART_STOCK_WORKER_SECRET:
            raise RuntimeError("MPHUB_CART_STOCK_WORKER_SECRET is not configured")
        self.worker_id = config.CART_STOCK_WORKER_ID
        self.site_base = config.MPHUB_CART_STOCK_URL.rstrip("/")
        self.site_session = curl_requests.Session(impersonate=WB_CURL_IMPERSONATE)
        self.outbox = Outbox(os.path.join(config.DATA_DIR, "cart_stock_worker.db"))
        self.last_heartbeat = 0.0
        self.last_wb_success_at: str | None = None
        self.last_error: str | None = None
        self.auth_state = "unknown"
        self.last_cooldown_log_at = 0.0
        self.browser_playwright = None
        self.browser = None
        self.browser_context = None
        self.browser_page = None

    def _signed_headers(self, method: str, path: str, raw_body: str) -> dict:
        timestamp = str(int(time.time()))
        nonce = str(uuid.uuid4())
        body_hash = hashlib.sha256(raw_body.encode("utf-8")).hexdigest()
        message = "\n".join((timestamp, nonce, method.upper(), path, body_hash))
        signature = hmac.new(
            config.MPHUB_CART_STOCK_WORKER_SECRET.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return {
            "Content-Type": "application/json",
            "X-MpHub-Worker-Id": self.worker_id,
            "X-MpHub-Worker-Timestamp": timestamp,
            "X-MpHub-Worker-Nonce": nonce,
            "X-MpHub-Worker-Signature": signature,
        }

    def site_request(self, path: str, payload: dict | None = None) -> dict:
        body = json.dumps(payload or {}, ensure_ascii=False, separators=(",", ":"))
        response = self.site_session.post(
            f"{self.site_base}{path}",
            data=body.encode("utf-8"),
            headers=self._signed_headers("POST", path, body),
            timeout=SITE_TIMEOUT_SECONDS,
        )
        try:
            data = response.json()
        except Exception:
            data = {}
        if response.status_code < 200 or response.status_code >= 300 or not data.get("ok"):
            message = data.get("error") or f"MpHub returned HTTP {response.status_code}"
            raise SiteRequestError(message, response.status_code)
        return data

    def _auth_headers(self) -> tuple[dict, str | None]:
        proxy_positions._load_token_cache()
        proxy_positions._load_wb_session()
        headers = proxy_positions._build_headers("__direct__", with_bearer=True)
        authorization = headers.get("Authorization", "")
        token = authorization.removeprefix("Bearer ") if authorization.startswith("Bearer ") else ""
        required = (authorization, headers.get("deviceid"), headers.get("Cookie"))
        if not all(required):
            raise RuntimeError("WB buyer session is incomplete; anonymous fallback is forbidden")
        return headers, token

    def auth_metadata(self) -> dict:
        try:
            _, token = self._auth_headers()
            expires_at = bearer_expiry(token or "")
            expired = bool(expires_at and expires_at <= time.time())
            return {
                "authState": "error" if expired else self.auth_state if self.auth_state != "unknown" else "ok",
                "bearerExpiresAt": utc_iso(expires_at) if expires_at else None,
            }
        except Exception as error:
            return {"authState": "error", "bearerExpiresAt": None, "authError": str(error)}

    def heartbeat(self, force: bool = False):
        now = time.time()
        if not force and now - self.last_heartbeat < HEARTBEAT_SECONDS:
            return
        auth = self.auth_metadata()
        access = get_access_health(WB_HEALTH_SCOPE)
        access_error = access.get("last_error") if access.get("state") != "healthy" else None
        error = access_error or self.last_error or auth.get("authError")
        last_success_at = access.get("last_success_at") or 0
        self.site_request(
            "/api/internal/cart-stock/worker/heartbeat",
            {
                "authState": auth["authState"],
                "bearerExpiresAt": auth["bearerExpiresAt"],
                "lastWbSuccessAt": self.last_wb_success_at or (
                    utc_iso(last_success_at) if last_success_at else None
                ),
                "lastError": error,
                "outboxCount": self.outbox.count(),
                "wbAccessState": access.get("state"),
                "wbHttpStatus": access.get("last_status"),
                "wbRetryAt": utc_iso(access.get("retry_at")) if access.get("retry_at") else None,
            },
        )
        self.last_heartbeat = now

    def refresh_auth(self):
        logger.warning("Refreshing saved WB buyer session")
        self.auth_state = "refreshing"
        self.close_browser()
        try:
            self.heartbeat(force=True)
        except Exception as error:
            logger.warning("Could not report auth refresh state to MpHub: %s", error)
        try:
            refresh_saved_session()
            proxy_positions._load_token_cache()
            proxy_positions._load_wb_session()
            self.auth_state = "ok"
        except WbAntibotError as error:
            # The saved buyer tokens still exist; WB blocked this IP/browser instead.
            record_antibot(
                error.status_code,
                str(error),
                "cart_stock_auth_refresh",
                minimum_cooldown_seconds=error.retry_after_seconds or 0,
                scope=WB_HEALTH_SCOPE,
            )
            self.auth_state = "ok"
            raise
        except WbAuthExpiredError as error:
            record_auth_expired(
                error.status_code,
                str(error),
                "cart_stock_auth_refresh",
                scope=WB_HEALTH_SCOPE,
            )
            self.auth_state = "error"
            raise

    def close_browser(self):
        for resource_name in ("browser_context", "browser"):
            resource = getattr(self, resource_name, None)
            if resource is not None:
                try:
                    resource.close()
                except Exception:
                    pass
                setattr(self, resource_name, None)
        playwright = getattr(self, "browser_playwright", None)
        if playwright is not None:
            try:
                playwright.stop()
            except Exception:
                pass
            self.browser_playwright = None
        self.browser_page = None

    def _ensure_browser_page(self):
        page = getattr(self, "browser_page", None)
        if page is not None and not page.is_closed():
            return page

        state_path = Path(config.DATA_DIR) / "wb_playwright_state.json"
        if not state_path.exists():
            raise RuntimeError("Saved WB Playwright state is missing; manual login is required")

        self.close_browser()
        self.browser_playwright = sync_playwright().start()
        try:
            launch_args = {
                "headless": False,
                "args": ["--disable-blink-features=AutomationControlled"],
            }
            if config.CART_STOCK_BROWSER_PROXY:
                launch_args["proxy"] = {"server": config.CART_STOCK_BROWSER_PROXY}
            self.browser = self.browser_playwright.chromium.launch(
                **launch_args,
            )
            self.browser_context = self.browser.new_context(
                storage_state=str(state_path),
                viewport={"width": 1920, "height": 1080},
                locale="ru-RU",
                timezone_id="Europe/Moscow",
            )
            self.browser_context.add_init_script(
                'Object.defineProperty(navigator, "webdriver", {get: () => undefined});'
            )
            self.browser_page = self.browser_context.new_page()
            # WB can challenge the HTML document while still allowing the
            # authenticated card request from a real browser context.  The
            # navigation establishes the first-party origin and cookies; the
            # card response below is the authoritative health signal.
            self.browser_page.goto(
                "https://www.wildberries.ru/",
                timeout=30000,
                wait_until="domcontentloaded",
            )
            self.browser_page.wait_for_timeout(8000)
            return self.browser_page
        except Exception:
            self.close_browser()
            raise

    def _browser_card_request(self, articles: list[str], headers: dict) -> tuple[int, str]:
        page = self._ensure_browser_page()
        allowed_headers = {
            key: value
            for key, value in headers.items()
            if key.lower() in {
                "authorization",
                "deviceid",
                "x-requested-with",
            }
        }
        request = {
            "path": WB_CARD_PATH,
            "params": {
                "appType": "1",
                "curr": "rub",
                "dest": str(config.CART_STOCK_DEST),
                "hide_dtype": "13",
                "spp": "30",
                "ab_testing": "false",
                "lang": "ru",
                "nm": ";".join(articles),
            },
            "headers": allowed_headers,
        }
        for attempt in range(3):
            try:
                result = page.evaluate(
                    """async ({path, params, headers}) => {
                        const query = new URLSearchParams(params);
                        const response = await fetch(`${path}?${query.toString()}`, {
                            method: "GET",
                            credentials: "include",
                            headers,
                        });
                        return {status: response.status, text: await response.text()};
                    }""",
                    request,
                )
                break
            except Exception:
                if attempt >= 2:
                    raise
                page.wait_for_timeout(3000)
        return int(result.get("status") or 0), str(result.get("text") or "")

    def browser_proxy_available(self) -> bool:
        if not config.CART_STOCK_BROWSER_PROXY:
            return True
        parsed = urlsplit(config.CART_STOCK_BROWSER_PROXY)
        if not parsed.hostname or not parsed.port:
            self.last_error = "WB browser proxy configuration is invalid"
            return False
        try:
            with socket.create_connection((parsed.hostname, parsed.port), timeout=2):
                return True
        except OSError:
            self.last_error = "WB browser tunnel is unavailable; job remains queued"
            return False

    def _request_wb_batch(self, articles: list[str], allow_refresh: bool = True) -> list[dict]:
        headers, _ = self._auth_headers()
        try:
            status_code, response_text = self._browser_card_request(articles, headers)
        except Exception as error:
            self.close_browser()
            record_network_error(
                None,
                f"WB browser request failed: {error}",
                "cart_stock_worker",
                scope=WB_HEALTH_SCOPE,
            )
            raise
        if status_code == 401 and allow_refresh:
            self.refresh_auth()
            return self._request_wb_batch(articles, allow_refresh=False)
        if status_code == 401:
            error = WbAuthExpiredError(
                "WB rejected the buyer session after refresh: HTTP 401",
                status_code=401,
            )
            record_auth_expired(401, str(error), "cart_stock_worker", scope=WB_HEALTH_SCOPE)
            self.auth_state = "error"
            raise error
        if status_code in ANTIBOT_HTTP_STATUSES:
            error = build_antibot_error(status_code, response_text[:2000])
            health = record_antibot(
                status_code,
                str(error),
                "cart_stock_worker",
                scope=WB_HEALTH_SCOPE,
            )
            retry_minutes = max(1, (probe_delay_seconds(health) + 59) // 60)
            logger.warning(
                "WB anti-bot detected: HTTP %s; auth refresh skipped; retry in %d minutes",
                status_code,
                retry_minutes,
            )
            raise error
        if status_code in RATE_LIMIT_HTTP_STATUSES:
            error = WbRateLimitError(
                f"WB rate limit is active: HTTP {status_code}",
                status_code=status_code,
            )
            record_rate_limit(
                status_code,
                str(error),
                "cart_stock_worker",
                scope=WB_HEALTH_SCOPE,
            )
            raise error
        if status_code != 200:
            error = f"Authorized WB card returned HTTP {status_code}"
            if status_code >= 500:
                record_network_error(
                    status_code,
                    error,
                    "cart_stock_worker",
                    scope=WB_HEALTH_SCOPE,
                )
            raise RuntimeError(error)
        data = json.loads(response_text)
        products = data.get("products")
        if not isinstance(products, list):
            raise RuntimeError("Authorized WB card returned no products array")
        record_success("cart_stock_worker", scope=WB_HEALTH_SCOPE)
        return products

    @staticmethod
    def normalize_product(article: str, product: dict | None) -> dict:
        if not product:
            return {
                "articleWB": article,
                "wbName": "",
                "clientTotalQuantity": 0,
                "missing": True,
                "stocks": [],
                "sizes": [],
            }
        warehouse_quantities: dict[int, int] = {}
        normalized_sizes: list[dict] = []
        for size in product.get("sizes", []) or []:
            size_warehouse_quantities: dict[int, int] = {}
            for stock in size.get("stocks", []) or []:
                warehouse_id = int(stock.get("wh", 0) or 0)
                quantity = int(stock.get("qty", 0) or 0)
                if warehouse_id > 0 and quantity > 0:
                    warehouse_quantities[warehouse_id] = warehouse_quantities.get(warehouse_id, 0) + quantity
                    size_warehouse_quantities[warehouse_id] = (
                        size_warehouse_quantities.get(warehouse_id, 0) + quantity
                    )
            normalized_sizes.append({
                "optionId": str(size.get("optionId") or ""),
                "name": str(size.get("name") or ""),
                "originalName": str(size.get("origName") or size.get("name") or ""),
                "stocks": [
                    {"warehouseId": warehouse_id, "quantity": quantity}
                    for warehouse_id, quantity in size_warehouse_quantities.items()
                ],
            })
        return {
            "articleWB": article,
            "wbName": str(product.get("name") or ""),
            "clientTotalQuantity": int(product.get("totalQuantity", 0) or 0),
            "missing": False,
            "stocks": [
                {"warehouseId": warehouse_id, "quantity": quantity}
                for warehouse_id, quantity in warehouse_quantities.items()
            ],
            "sizes": normalized_sizes,
        }

    def collect(self, job: dict) -> dict:
        articles = [str(article) for article in job.get("articles", []) if str(article).isdigit()]
        if not articles:
            raise RuntimeError("Cart stock job contains no valid articles")

        returned: dict[str, dict] = {}
        for index in range(0, len(articles), BATCH_SIZE):
            batch = articles[index:index + BATCH_SIZE]
            for product in self._request_wb_batch(batch):
                returned[str(product.get("id"))] = product
            if index + BATCH_SIZE < len(articles):
                time.sleep(BATCH_DELAY_SECONDS)

        normalized = [self.normalize_product(article, returned.get(article)) for article in articles]
        missing_articles = [product["articleWB"] for product in normalized if product["missing"]]
        if missing_articles:
            logger.warning(
                "Authorized WB card omitted %d/%d requested articles; delivering the partial snapshot: %s",
                len(missing_articles),
                len(articles),
                ",".join(missing_articles),
            )

        self.last_wb_success_at = utc_iso()
        self.last_error = None
        self.auth_state = "ok"
        auth = self.auth_metadata()
        return {
            "jobId": int(job["jobId"]),
            "claimToken": str(job["claimToken"]),
            "status": "success",
            "capturedAt": self.last_wb_success_at,
            "destinationIds": [str(config.CART_STOCK_DEST)],
            "products": normalized,
            "authenticated": True,
            "endpoint": WB_CARD_PATH,
            "bearerExpiresAt": auth.get("bearerExpiresAt"),
        }

    def flush_outbox(self) -> bool:
        all_sent = True
        for job_id, payload_json, attempts in self.outbox.rows():
            if attempts >= MAX_OUTBOX_DELIVERY_ATTEMPTS:
                logger.error(
                    "Dropping outbox job %s after %s failed deliveries; its server lease will requeue it",
                    job_id,
                    attempts,
                )
                self.outbox.delete(job_id)
                continue
            try:
                payload = json.loads(payload_json)
                self.site_request("/api/internal/cart-stock/worker/result", payload)
                self.outbox.delete(job_id)
                logger.info("Delivered cart-stock result for job %s", job_id)
            except SiteRequestError as error:
                if error.status == 409:
                    logger.warning("Dropping stale outbox result for job %s: %s", job_id, error)
                    self.outbox.delete(job_id)
                    continue
                self.outbox.mark_error(job_id, str(error))
                logger.warning("Could not deliver outbox job %s: %s", job_id, error)
                all_sent = False
                break
            except Exception as error:
                self.outbox.mark_error(job_id, str(error))
                logger.warning("Could not deliver outbox job %s: %s", job_id, error)
                all_sent = False
                break
        return all_sent

    def claim(self) -> dict | None:
        return self.site_request("/api/internal/cart-stock/worker/claim", {}).get("job")

    def process_once(self) -> bool:
        if not self.flush_outbox():
            return False
        if not self.browser_proxy_available():
            self.heartbeat()
            return False
        if getattr(self, "last_error", None) == "WB browser tunnel is unavailable; job remains queued":
            self.last_error = None
        self.heartbeat()
        access = get_access_health(WB_HEALTH_SCOPE)
        retry_delay = probe_delay_seconds(access)
        if retry_delay > 0:
            now = time.time()
            if now - self.last_cooldown_log_at >= HEARTBEAT_SECONDS:
                logger.warning(
                    "WB jobs paused: access state=%s, retry in %d seconds",
                    access.get("state"),
                    retry_delay,
                )
                self.last_cooldown_log_at = now
            return False
        job = self.claim()
        if not job:
            return False

        logger.info("Claimed cart-stock job %s attempt %s", job.get("jobId"), job.get("attempt"))
        try:
            payload = self.collect(job)
        except Exception as error:
            self.last_error = str(error)[:2000]
            if isinstance(error, WbAuthExpiredError):
                self.auth_state = "error"
            elif isinstance(error, (WbAntibotError, WbRateLimitError)):
                self.auth_state = "ok"
            logger.exception("Cart-stock job %s failed", job.get("jobId"))
            payload = {
                "jobId": int(job["jobId"]),
                "claimToken": str(job["claimToken"]),
                "status": "error",
                "error": self.last_error,
                "authenticated": not isinstance(error, WbAuthExpiredError),
                "endpoint": WB_CARD_PATH,
            }
        self.outbox.put(int(job["jobId"]), payload)
        self.flush_outbox()
        self.heartbeat(force=True)
        return True

    def run(self, once: bool = False):
        logger.info("Starting single cart-stock worker %s", self.worker_id)
        systemd_notify("READY=1\nSTATUS=Polling MpHub cart-stock jobs")
        while True:
            systemd_notify("WATCHDOG=1\nSTATUS=Processing MpHub cart-stock queue")
            try:
                self.process_once()
            except Exception as error:
                self.last_error = str(error)[:2000]
                logger.exception("Cart-stock worker loop failed")
            systemd_notify("WATCHDOG=1\nSTATUS=Waiting for the next MpHub cart-stock job")
            if once:
                return
            time.sleep(config.CART_STOCK_WORKER_POLL_SECONDS)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    worker = CartStockWorker()
    if args.check:
        auth = worker.auth_metadata()
        access = get_access_health(WB_HEALTH_SCOPE)
        host = urlsplit(worker.site_base).hostname
        print(json.dumps({
            "ok": auth.get("authState") == "ok" and access.get("state") in ("healthy", "unknown"),
            "workerId": worker.worker_id,
            "siteHost": host,
            "authState": auth.get("authState"),
            "wbAccessState": access.get("state"),
            "wbHttpStatus": access.get("last_status"),
            "wbRetryAt": utc_iso(access.get("retry_at")) if access.get("retry_at") else None,
            "bearerExpiresAt": auth.get("bearerExpiresAt"),
            "outboxCount": worker.outbox.count(),
        }, ensure_ascii=False))
        return
    worker.run(once=args.once)


if __name__ == "__main__":
    main()
