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
import sqlite3
import time
import uuid
from pathlib import Path
from urllib.parse import urlsplit

from curl_cffi import requests as curl_requests

import config
import proxy_positions
from wb_session_runtime import refresh_saved_session


WB_CARD_ENDPOINT = "https://www.wildberries.ru/__internal/card/cards/v4/detail"
WB_CARD_PATH = "/__internal/card/cards/v4/detail"
BATCH_SIZE = 20
HEARTBEAT_SECONDS = 60
SITE_TIMEOUT_SECONDS = 30
WB_TIMEOUT_SECONDS = 30

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("cart-stock-worker")


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
            "SELECT job_id, payload_json FROM cart_stock_outbox ORDER BY created_at, job_id"
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
        self.site_session = curl_requests.Session(impersonate="chrome")
        self.wb_session = curl_requests.Session(impersonate="chrome")
        self.outbox = Outbox(os.path.join(config.DATA_DIR, "cart_stock_worker.db"))
        self.last_heartbeat = 0.0
        self.last_wb_success_at: str | None = None
        self.last_error: str | None = None
        self.auth_state = "unknown"

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
        error = self.last_error or auth.get("authError")
        self.site_request(
            "/api/internal/cart-stock/worker/heartbeat",
            {
                "authState": auth["authState"],
                "bearerExpiresAt": auth["bearerExpiresAt"],
                "lastWbSuccessAt": self.last_wb_success_at,
                "lastError": error,
                "outboxCount": self.outbox.count(),
            },
        )
        self.last_heartbeat = now

    def refresh_auth(self):
        logger.warning("Refreshing saved WB buyer session")
        self.auth_state = "refreshing"
        try:
            self.heartbeat(force=True)
        except Exception as error:
            logger.warning("Could not report auth refresh state to MpHub: %s", error)
        refresh_saved_session()
        proxy_positions._load_token_cache()
        proxy_positions._load_wb_session()
        self.auth_state = "ok"

    def _request_wb_batch(self, articles: list[str], allow_refresh: bool = True) -> list[dict]:
        headers, _ = self._auth_headers()
        response = self.wb_session.get(
            WB_CARD_ENDPOINT,
            params={
                "appType": "1",
                "curr": "rub",
                "dest": str(config.CART_STOCK_DEST),
                "hide_dtype": "13",
                "spp": "30",
                "ab_testing": "false",
                "lang": "ru",
                "nm": ";".join(articles),
            },
            headers=headers,
            timeout=WB_TIMEOUT_SECONDS,
        )
        if response.status_code in (401, 498) and allow_refresh:
            self.refresh_auth()
            return self._request_wb_batch(articles, allow_refresh=False)
        if response.status_code != 200:
            raise RuntimeError(f"Authorized WB card returned HTTP {response.status_code}")
        data = response.json()
        products = data.get("products")
        if not isinstance(products, list):
            raise RuntimeError("Authorized WB card returned no products array")
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
            }
        warehouse_quantities: dict[int, int] = {}
        for size in product.get("sizes", []) or []:
            for stock in size.get("stocks", []) or []:
                warehouse_id = int(stock.get("wh", 0) or 0)
                quantity = int(stock.get("qty", 0) or 0)
                if warehouse_id > 0 and quantity > 0:
                    warehouse_quantities[warehouse_id] = warehouse_quantities.get(warehouse_id, 0) + quantity
        return {
            "articleWB": article,
            "wbName": str(product.get("name") or ""),
            "clientTotalQuantity": int(product.get("totalQuantity", 0) or 0),
            "missing": False,
            "stocks": [
                {"warehouseId": warehouse_id, "quantity": quantity}
                for warehouse_id, quantity in warehouse_quantities.items()
            ],
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

        normalized = [self.normalize_product(article, returned.get(article)) for article in articles]
        if any(product["missing"] for product in normalized):
            missing = sum(1 for product in normalized if product["missing"])
            raise RuntimeError(f"Authorized WB card missed {missing}/{len(articles)} requested articles")

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
        for job_id, payload_json in self.outbox.rows():
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
        self.heartbeat()
        job = self.claim()
        if not job:
            return False

        logger.info("Claimed cart-stock job %s attempt %s", job.get("jobId"), job.get("attempt"))
        try:
            payload = self.collect(job)
        except Exception as error:
            self.last_error = str(error)[:2000]
            self.auth_state = "error" if "session" in self.last_error.lower() else self.auth_state
            logger.exception("Cart-stock job %s failed", job.get("jobId"))
            payload = {
                "jobId": int(job["jobId"]),
                "claimToken": str(job["claimToken"]),
                "status": "error",
                "error": self.last_error,
                "authenticated": False,
                "endpoint": WB_CARD_PATH,
            }
        self.outbox.put(int(job["jobId"]), payload)
        self.flush_outbox()
        self.heartbeat(force=True)
        return True

    def run(self, once: bool = False):
        logger.info("Starting single cart-stock worker %s", self.worker_id)
        while True:
            try:
                self.process_once()
            except Exception as error:
                self.last_error = str(error)[:2000]
                logger.exception("Cart-stock worker loop failed")
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
        host = urlsplit(worker.site_base).hostname
        print(json.dumps({
            "ok": auth.get("authState") == "ok",
            "workerId": worker.worker_id,
            "siteHost": host,
            "authState": auth.get("authState"),
            "bearerExpiresAt": auth.get("bearerExpiresAt"),
            "outboxCount": worker.outbox.count(),
        }, ensure_ascii=False))
        return
    worker.run(once=args.once)


if __name__ == "__main__":
    main()
