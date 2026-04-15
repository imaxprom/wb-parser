"""Organic + Promo position fetcher via curl_cffi + rotating proxies.

Drop-in replacement for chrome_positions.py.
Same interface: get_positions(article, keywords, dest) -> {kw: {promo_pos, organic_pos, is_advertised}}

Algorithm (same as evirma extension):
1. Fetch normal search (2 pages x 300) — find promo position + build organic map
2. Fetch no_promo search (2 pages x 300) — find organic position via orgMap
"""

import asyncio
import itertools
import json
import logging
import os
import time
from typing import Optional

from curl_cffi import requests as curl_requests

import config

logger = logging.getLogger(__name__)

# ── Proxy rotation ──

_proxy_cycle = None


def _init_proxy_cycle():
    global _proxy_cycle
    proxies = config.WB_PROXIES
    if not proxies:
        raise RuntimeError("No WB_PROXY_* configured in .env")
    _proxy_cycle = itertools.cycle(proxies)
    logger.info("Proxy rotation initialized: %d proxies", len(proxies))


def _next_proxy() -> str:
    """Get next proxy URL in round-robin."""
    global _proxy_cycle
    if _proxy_cycle is None:
        _init_proxy_cycle()
    raw = next(_proxy_cycle)  # user:pass@host:port
    return f"http://{raw}"


# ── wbaas token management (per-proxy) ──

_WBAAS_CACHE = os.path.join(config.DATA_DIR, "wbaas_proxy_tokens.json")

# {proxy_string: {"token": ..., "updated_at": ...}}
_token_cache: dict[str, dict] = {}


def _load_token_cache():
    global _token_cache
    try:
        with open(_WBAAS_CACHE) as f:
            _token_cache = json.load(f)
    except Exception:
        _token_cache = {}


def _save_token_cache():
    with open(_WBAAS_CACHE, "w") as f:
        json.dump(_token_cache, f)


def _get_wbaas_token(proxy_raw: str) -> str:
    """Get cached x_wbaas_token for a proxy."""
    entry = _token_cache.get(proxy_raw, {})
    token = entry.get("token", "")
    updated = entry.get("updated_at", 0)
    # Token lives ~14 days, refresh if older than 12 days
    if token and (time.time() - updated) < 12 * 86400:
        return token
    return ""


def _refresh_wbaas_token(proxy_raw: str = None) -> str:
    """Get x_wbaas_token via Playwright. proxy_raw=None for direct mode."""
    from playwright.sync_api import sync_playwright

    proxy_conf = None
    if proxy_raw:
        parts = proxy_raw.split("@")
        user_pass = parts[0]
        host_port = parts[1]
        username, password = user_pass.split(":")
        host, port = host_port.rsplit(":", 1)
        proxy_conf = {
            "server": f"http://{host}:{port}",
            "username": username,
            "password": password,
        }
        label = f"proxy {host}:{port}"
    else:
        label = "direct"

    logger.info("Refreshing wbaas token (%s)...", label)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        ctx_kwargs = {
            "user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/141.0.0.0 Safari/537.36"
            ),
        }
        if proxy_conf:
            ctx_kwargs["proxy"] = proxy_conf
        ctx = browser.new_context(**ctx_kwargs)
        ctx.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )
        page = ctx.new_page()
        page.goto("https://www.wildberries.ru/", timeout=30000)
        page.wait_for_timeout(12000)
        cookies = {c["name"]: c["value"] for c in ctx.cookies()}
        browser.close()

    token_key = proxy_raw or "__direct__"
    token = cookies.get("x_wbaas_token", "")
    if token:
        _token_cache[token_key] = {"token": token, "updated_at": time.time()}
        _save_token_cache()
        logger.info("wbaas token refreshed OK (%s, %d chars)", label, len(token))
    else:
        logger.error("Failed to get wbaas token (%s)", label)

    return token


def refresh_wbaas_tokens():
    """Refresh wbaas tokens and load auth session. Call before asyncio loop."""
    _load_token_cache()
    _load_wb_session()

    if config.WB_PROXIES:
        # Proxy mode: token per proxy
        for proxy_raw in config.WB_PROXIES:
            existing = _get_wbaas_token(proxy_raw)
            if existing:
                logger.info("wbaas token still valid for proxy %s", proxy_raw.split("@")[1])
                continue
            try:
                _refresh_wbaas_token(proxy_raw)
            except Exception as e:
                logger.error("Failed to refresh wbaas for %s: %s", proxy_raw.split("@")[1], e)
    else:
        # Direct mode: one token
        existing = _get_wbaas_token("__direct__")
        if existing:
            logger.info("wbaas token still valid (direct)")
            return
        try:
            _refresh_wbaas_token()
        except Exception as e:
            logger.error("Failed to refresh wbaas (direct): %s", e)


# ── Search API ──

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/141.0.0.0 Safari/537.36"
)

SEARCH_URL = "https://www.wildberries.ru/__internal/search/exactmatch/ru/common/v18/search"

# Authenticated session data (from wb_session.json)
_wb_session: dict = {}

_WB_SESSION_FILE = os.path.join(config.DATA_DIR, "wb_session.json")


def _load_wb_session():
    """Load authenticated session (Bearer, PoW, cookies) from file."""
    global _wb_session
    try:
        with open(_WB_SESSION_FILE) as f:
            _wb_session = json.load(f)
        ls = _wb_session.get("localStorage", {})
        has_bearer = bool(ls.get("wbx__tokenData"))
        has_pow = bool(ls.get("session-pow-token"))
        logger.info("WB session loaded: Bearer=%s PoW=%s", has_bearer, has_pow)
    except Exception:
        _wb_session = {}
        logger.warning("No wb_session.json — running without auth")


def _build_headers(token_key: str) -> dict:
    """Build request headers with full auth if available."""
    headers = {
        "User-Agent": _UA,
        "Accept": "*/*",
        "Accept-Language": "ru-RU,ru;q=0.9",
        "Referer": "https://www.wildberries.ru/",
        "x-requested-with": "XMLHttpRequest",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "sec-fetch-dest": "empty",
    }

    ls = _wb_session.get("localStorage", {})
    cookies = _wb_session.get("cookies", {})

    # Bearer token
    td_raw = ls.get("wbx__tokenData", "")
    if td_raw:
        try:
            td = json.loads(td_raw)
            bearer = td.get("token", "")
            if bearer:
                headers["Authorization"] = f"Bearer {bearer}"
                # Extract userid from JWT
                import base64
                raw = bearer.split(".")[1]
                raw += "=" * (4 - len(raw) % 4)
                jwt_data = json.loads(base64.urlsafe_b64decode(raw))
                headers["x-userid"] = str(jwt_data.get("user", "0"))
        except Exception as e:
            logger.warning("Failed to parse Bearer: %s", e)

    # PoW token
    pow_raw = ls.get("session-pow-token", "")
    if pow_raw:
        try:
            pow_data = json.loads(pow_raw)
            pow_token = pow_data.get("token", "")
            if pow_token:
                headers["X-Pow"] = pow_token
        except Exception:
            pass

    # X-Queryid
    wbauid = cookies.get("_wbauid", "")
    if wbauid:
        import datetime
        ts = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        headers["X-Queryid"] = f"qid{wbauid}{ts}"

    # Cookies: x_wbaas_token + _wbauid
    cookie_parts = []
    wbaas = _get_wbaas_token(token_key)
    if wbaas:
        cookie_parts.append(f"x_wbaas_token={wbaas}")
    elif cookies.get("x_wbaas_token"):
        cookie_parts.append(f"x_wbaas_token={cookies['x_wbaas_token']}")
    if wbauid:
        cookie_parts.append(f"_wbauid={wbauid}")
    if cookie_parts:
        headers["Cookie"] = "; ".join(cookie_parts)

    return headers


def _search_sync(headers: dict, params: dict, proxy_url: str = None,
                 session: curl_requests.Session = None) -> tuple[dict, bool]:
    """Single search request via curl_cffi. Returns (data, is_error)."""
    try:
        kwargs = {
            "params": params,
            "headers": headers,
            "impersonate": "chrome",
            "timeout": 10,
        }
        if proxy_url:
            kwargs["proxies"] = {"https": proxy_url, "http": proxy_url}

        client = session if session else curl_requests
        resp = client.get(SEARCH_URL, **kwargs)
        if resp.status_code == 200:
            return resp.json(), False
        elif resp.status_code == 429:
            logger.warning("429 rate limit")
            return {}, True
        elif resp.status_code in (451, 498):
            logger.warning("%d anti-bot block", resp.status_code)
            return {}, True
        else:
            logger.warning("HTTP %d from WB", resp.status_code)
            return {}, True
    except Exception as e:
        logger.error("Request error: %s", e)
        return {}, True


def _fetch_keyword_sync(proxy_raw: str, query: str, sku: int, dest: int,
                        session: curl_requests.Session = None) -> dict:
    """Fetch promo + organic positions for one keyword.

    Direct mode (no proxy): 2 pairs with Session — normal p1+p2, then nopromo p1+p2.
    Proxy mode: 4 fetches SEQUENTIALLY (proxy can't handle parallel TCP).
    """
    proxy_url = f"http://{proxy_raw}" if proxy_raw else None
    token_key = proxy_raw or "__direct__"
    headers = _build_headers(token_key)

    base_params = {
        "ab_testing": "false",
        "appType": "1",
        "curr": "rub",
        "dest": str(dest),
        "hide_dflags": "131072",
        "hide_dtype": "10;14",
        "inheritFilters": "false",
        "lang": "ru",
        "query": query,
        "resultset": "catalog",
        "sort": "popular",
        "spp": "31",
        "suppressSpellcheck": "false",
        "limit": "300",
    }

    result = {"query": query, "promo_pos": None, "organic_pos": None, "is_advertised": False, "error": False}

    def do_fetch(page, ab_testid=None):
        p = {**base_params, "page": str(page)}
        if ab_testid:
            p["ab_testid"] = ab_testid
        return _search_sync(headers, p, proxy_url, session)

    import concurrent.futures

    if proxy_url:
        # Proxy: sequential to avoid timeouts
        d_n1, e1 = do_fetch(1)
        if e1:
            result["error"] = True
            return result
        d_n2, e2 = do_fetch(2)
        d_np1, e3 = do_fetch(1, "no_promo")
        if e3:
            result["error"] = True
            return result
        d_np2, e4 = do_fetch(2, "no_promo")
    else:
        # Direct: 4 fetches in parallel (T07 — fastest, 100% stable with auth)
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            f_n1 = pool.submit(do_fetch, 1)
            f_n2 = pool.submit(do_fetch, 2)
            f_np1 = pool.submit(do_fetch, 1, "no_promo")
            f_np2 = pool.submit(do_fetch, 2, "no_promo")
        d_n1, e1 = f_n1.result()
        d_n2, e2 = f_n2.result()
        d_np1, e3 = f_np1.result()
        d_np2, e4 = f_np2.result()
        if e1 or e3:
            result["error"] = True
            return result

    n1_products = d_n1.get("products", [])
    n2_products = d_n2.get("products", []) if not e2 else []
    np1_products = d_np1.get("products", [])
    np2_products = d_np2.get("products", []) if not e4 else []

    # Detect incomplete/empty response = WB glitch
    incomplete = False
    if len(n1_products) == 0:
        incomplete = True  # normal page 1 empty
    if len(np1_products) == 0:
        incomplete = True  # nopromo page 1 empty
    if len(n1_products) >= 300 and len(n2_products) == 0 and not e2:
        incomplete = True  # normal page 2 empty when page 1 full
    if len(np1_products) >= 300 and len(np2_products) == 0 and not e4:
        incomplete = True  # nopromo page 2 empty when page 1 full

    normal_products = n1_products + n2_products
    nopromo_products = np1_products + np2_products

    # Build organic map + find promo position
    org_map = {}
    org_counter = 0
    promo_pos = None
    is_ad = False

    for i, product in enumerate(normal_products):
        pid = product.get("id")
        logs = product.get("logs", "")
        if not logs:
            org_counter += 1
            org_map[org_counter] = i + 1
        if pid == sku:
            promo_pos = i + 1
            is_ad = bool(logs)

    # Find organic position via no_promo -> orgMap
    organic_pos = None
    for j, product in enumerate(nopromo_products):
        if product.get("id") == sku:
            nopromo_pos = j + 1
            organic_pos = org_map.get(nopromo_pos)
            break

    result["promo_pos"] = promo_pos
    result["organic_pos"] = organic_pos
    result["is_advertised"] = is_ad

    # Mark as error — triggers retry
    if incomplete and promo_pos is None and organic_pos is None:
        result["error"] = True
    # Ad product found in normal but missing in nopromo = nopromo data incomplete
    if is_ad and promo_pos is not None and organic_pos is None:
        result["error"] = True
    # Both None = either WB glitch or genuinely not found, check incomplete
    if promo_pos is None and organic_pos is None and incomplete:
        result["error"] = True

    return result


# ── Public API (same interface as chrome_positions) ──

DEST = -951305  # Moscow region


async def get_positions(article: int, keywords: list[str],
                        dest: int = DEST) -> dict[str, dict]:
    """Get organic + promo positions for an article across multiple keywords.

    Same logic as old Chrome JS approach:
    - Keywords processed SEQUENTIALLY (one after another)
    - Inside each keyword: 4 fetches in PARALLEL (Promise.all equivalent)
    - Max 4 concurrent requests to WB at any moment
    - Proxy rotates per keyword (round-robin)

    Returns: {
        "keyword": {"promo_pos": 16, "organic_pos": 436, "is_advertised": True},
        ...
    }
    """
    if not _token_cache:
        _load_token_cache()

    # Direct mode (no proxies) or proxy mode
    proxy_raw = config.WB_PROXIES[0] if config.WB_PROXIES else ""
    mode = "proxy" if proxy_raw else "direct"
    token_key = proxy_raw or "__direct__"

    logger.info("Fetching positions for %d: %d keywords (%s)", article, len(keywords), mode)

    result = {}

    # Session for connection reuse (T08 strategy)
    session = curl_requests.Session(impersonate="chrome") if not proxy_raw else None

    for kw in keywords:

        if not _get_wbaas_token(token_key):
            logger.warning("No wbaas token, refreshing...")
            try:
                await asyncio.to_thread(_refresh_wbaas_token, proxy_raw or None)
            except Exception as e:
                logger.error("wbaas refresh failed: %s", e)

        item = await asyncio.to_thread(_fetch_keyword_sync, proxy_raw, kw, article, dest, session)

        # Retry once if error (empty page, HTTP error)
        if item.get("error"):
            logger.warning("Retry '%s' for %d (empty/error response)", kw, article)
            await asyncio.sleep(0.5)
            item = await asyncio.to_thread(_fetch_keyword_sync, proxy_raw, kw, article, dest, session)

        result[kw] = {
            "promo_pos": item["promo_pos"],
            "organic_pos": item["organic_pos"],
            "is_advertised": item["is_advertised"],
            "error": item.get("error", False),
        }
        logger.info(
            "Positions for %d '%s': promo=%s organic=%s is_ad=%s%s",
            article, kw, item["promo_pos"], item["organic_pos"], item["is_advertised"],
            " ERROR" if item.get("error") else "",
        )

    # Close session
    if session:
        session.close()

    # Fill missing keywords
    for kw in keywords:
        if kw not in result:
            result[kw] = {"promo_pos": None, "organic_pos": None, "is_advertised": False, "error": True}

    return result
