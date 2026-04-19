"""Wildberries Search API parser (v18 endpoint).

Optimized:
- Groups articles by query to minimize API calls
- Single aiohttp session for all requests (reuses TCP connection)
- No retry on 429 (marks as error instead of wasting time)
"""

import aiohttp
import asyncio
import logging
import random
from typing import Optional
from collections import defaultdict
from config import WB_SEARCH_URL, WB_DEST, WB_ITEMS_PER_PAGE, WB_APP_TYPE
import db

logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
]

HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "ru-RU,ru;q=0.9",
    "Referer": "https://www.wildberries.ru/",
    "x-requested-with": "XMLHttpRequest",
    "x-spa-version": "14.2.3",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "sec-fetch-dest": "empty",
}

REQUEST_DELAY = 0.5  # seconds between requests


import os as _os
import json as _json

_WBAAS_CACHE_FILE = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "data", "wbaas_token.json")


def refresh_wbaas_token_sync():
    """Get x_wbaas_token via Playwright (sync). Call BEFORE asyncio loop starts."""
    import time
    try:
        from playwright.sync_api import sync_playwright
        logger.info("Refreshing x_wbaas_token via Playwright...")
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
            ctx = browser.new_context(user_agent=USER_AGENTS[0])
            ctx.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
            page = ctx.new_page()
            page.goto("https://www.wildberries.ru/", timeout=30000)
            page.wait_for_timeout(8000)
            cookies = {c["name"]: c["value"] for c in ctx.cookies()}
            browser.close()

        token = cookies.get("x_wbaas_token", "")
        if token:
            with open(_WBAAS_CACHE_FILE, "w") as f:
                _json.dump({"token": token, "updated_at": time.time()}, f)
            logger.info("x_wbaas_token refreshed OK")
        return token
    except Exception as e:
        logger.error(f"Failed to get x_wbaas_token: {e}")
        return _get_wbaas_token()


def _get_wbaas_token() -> str:
    """Get x_wbaas_token from cache file."""
    try:
        with open(_WBAAS_CACHE_FILE) as f:
            data = _json.load(f)
        return data.get("token", "")
    except Exception:
        return ""


def _build_headers() -> tuple[dict, Optional[dict]]:
    """Build headers with Bearer auth + x_wbaas_token cookie. Returns (headers, token_row)."""
    headers = {**HEADERS, "User-Agent": random.choice(USER_AGENTS)}

    # x_wbaas_token cookie (antibot)
    wbaas = _get_wbaas_token()
    if wbaas:
        headers["Cookie"] = f"x_wbaas_token={wbaas}"

    # Bearer token (buyer auth) + x-userid
    token_row = db.get_next_wb_token()
    if token_row:
        headers["Authorization"] = f"Bearer {token_row['token']}"
        db.mark_wb_token_used(token_row["id"])
        try:
            import base64
            raw = token_row["token"].split(".")[1]
            raw += "=" * (4 - len(raw) % 4)  # proper padding
            data = _json.loads(base64.urlsafe_b64decode(raw))
            headers["x-userid"] = data.get("user", "0")
        except Exception:
            headers["x-userid"] = "0"

    return headers, token_row


def find_positions(data: dict, skus: set[str], page: int) -> dict[str, int]:
    """Find positions of multiple SKUs in one search result page."""
    products = data.get("products", [])
    found = {}
    for i, product in enumerate(products):
        pid = str(product.get("id"))
        if pid in skus:
            found[pid] = (page - 1) * WB_ITEMS_PER_PAGE + i + 1
    return found


def find_positions_with_ad(data: dict, skus: set[str], page: int) -> dict[str, dict]:
    """Find positions of SKUs with organic/promo distinction.
    
    WB marks promoted products with a 'log' field containing ad tracking data.
    Products without 'log' (or empty log) are organic.
    
    Returns: {sku: {"position": N, "is_ad": bool, "page": N}}
    """
    products = data.get("products", [])
    found = {}
    for i, product in enumerate(products):
        pid = str(product.get("id"))
        if pid in skus:
            pos = (page - 1) * WB_ITEMS_PER_PAGE + i + 1
            # WB помечает рекламные товары полем 'log' с данными трекинга
            log = product.get("log", "")
            is_ad = bool(log) and len(str(log)) > 5
            found[pid] = {"position": pos, "is_ad": is_ad, "page": page}
    return found


def _is_ad_by_logs(product: dict) -> bool:
    """Determine if product is an ad by logs field length.
    Organic products have logs length <= 88, ads have > 88.
    Based on evirma extension analysis: isAdvert = !!product.logs
    where logs encoding differs for ad vs organic products.
    """
    logs = product.get("logs", "")
    if not logs:
        return False
    return len(str(logs)) > 88


async def search_organic_promo(session: aiohttp.ClientSession, query: str,
                                sku: str, pages_depth: int = 6,
                                token_row: Optional[dict] = None) -> dict:
    """Find promo and organic positions.
    
    Scans normal search results, determines ad/organic by logs field length.
    promo_pos = position in full results (what buyer sees)
    organic_pos = position counting only organic (non-ad) products
    
    Returns: {"promo_pos": N or None, "organic_pos": N or None}
    """
    promo_pos = None
    organic_pos = None
    
    # Step 1: Normal search — find promo position
    for page in range(1, pages_depth + 1):
        data, is_error = await _search(session, query, page, token_row)
        if is_error:
            break
        products = data.get("products", [])
        if not products:
            break
        for i, product in enumerate(products):
            if str(product.get("id")) == sku:
                promo_pos = (page - 1) * WB_ITEMS_PER_PAGE + i + 1
                break
        if promo_pos is not None:
            break
        await asyncio.sleep(REQUEST_DELAY)
    
    await asyncio.sleep(REQUEST_DELAY)
    
    # Step 2: no_promo search — find organic position (without ads)
    for page in range(1, pages_depth + 1):
        data, is_error = await _search_no_promo(session, query, page, token_row)
        if is_error:
            break
        products = data.get("products", [])
        if not products:
            break
        for i, product in enumerate(products):
            if str(product.get("id")) == sku:
                organic_pos = (page - 1) * WB_ITEMS_PER_PAGE + i + 1
                break
        if organic_pos is not None:
            break
        await asyncio.sleep(REQUEST_DELAY)
    
    return {"promo_pos": promo_pos, "organic_pos": organic_pos}


async def _search_no_promo(session: aiohttp.ClientSession, query: str, page: int = 1,
                            token_row: Optional[dict] = None) -> tuple[dict, bool]:
    """Search request WITHOUT promoted products (ab_testid=no_promo)."""
    params = {
        "appType": WB_APP_TYPE,
        "curr": "rub",
        "dest": WB_DEST,
        "lang": "ru",
        "page": page,
        "query": query,
        "resultset": "catalog",
        "sort": "popular",
        "spp": "30",
        "ab_testid": "no_promo",
    }

    for attempt in range(2):
        try:
            async with session.get(
                WB_SEARCH_URL, params=params,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    return data, False
                elif resp.status == 429:
                    if attempt == 0:
                        await asyncio.sleep(1)
                        continue
                    return {}, True
                else:
                    logger.warning(f"{resp.status} on no_promo '{query}' page {page}")
                    return {}, True
        except Exception as e:
            logger.error(f"Request error no_promo '{query}' page {page}: {e}")
            if attempt == 0:
                await asyncio.sleep(1)
                continue
            return {}, True

    return {}, True


async def _search(session: aiohttp.ClientSession, query: str, page: int = 1,
                  token_row: Optional[dict] = None) -> tuple[dict, bool]:
    """Single search request with retry on 429. Returns (data, is_error)."""
    params = {
        "appType": WB_APP_TYPE,
        "curr": "rub",
        "dest": WB_DEST,
        "lang": "ru",
        "page": page,
        "query": query,
        "resultset": "catalog",
        "sort": "popular",
        "spp": "30",
    }

    for attempt in range(2):
        try:
            async with session.get(
                WB_SEARCH_URL, params=params,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    return data, False
                elif resp.status == 401:
                    logger.warning("401 Unauthorized — token may be expired")
                    if token_row:
                        db.mark_wb_token_error(token_row["id"], "401")
                    return {}, True
                elif resp.status == 429:
                    if attempt == 0:
                        await asyncio.sleep(1)
                        continue
                    return {}, True
                else:
                    logger.warning(f"{resp.status} on '{query}' page {page}")
                    return {}, True
        except Exception as e:
            logger.error(f"Request error '{query}' page {page}: {e}")
            if attempt == 0:
                await asyncio.sleep(1)
                continue
            return {}, True

    return {}, True


async def search_query_all_pages(session: aiohttp.ClientSession, query: str,
                                  skus: set[str], pages_depth: int,
                                  token_row: Optional[dict] = None) -> dict[str, dict]:
    """Search one query across pages, find ALL skus."""
    results = {sku: {"position": None, "page": None, "total_found": 0, "error": False} for sku in skus}
    remaining = set(skus)

    for page in range(1, pages_depth + 1):
        if not remaining:
            break

        data, is_error = await _search(session, query, page, token_row)

        if is_error:
            for sku in remaining:
                results[sku]["error"] = True
            break

        products = data.get("products", [])
        if not products:
            break

        if page == 1:
            total = data.get("metadata", {}).get("rs", 0)
            for sku in skus:
                results[sku]["total_found"] = total

        found = find_positions(data, remaining, page)
        for sku, pos in found.items():
            results[sku]["position"] = pos
            results[sku]["page"] = page
            remaining.discard(sku)

        if not remaining:
            break

        await asyncio.sleep(REQUEST_DELAY)

    return results


async def run_auto_parse(telegram_id: int) -> dict[str, list[dict]]:
    """Parse only articles with auto_check=1 for a user."""
    pages_depth = int(db.get_setting(telegram_id, "pages_depth") or 3)
    articles = db.get_auto_articles(telegram_id)
    if not articles:
        return {}
    return await _parse_articles(telegram_id, articles, pages_depth)


async def run_full_parse(telegram_id: int) -> dict[str, list[dict]]:
    """Parse all articles for a user."""
    pages_depth = int(db.get_setting(telegram_id, "pages_depth") or 3)
    articles = db.get_articles(telegram_id)
    if not articles:
        return {}
    return await _parse_articles(telegram_id, articles, pages_depth)


async def _parse_articles(telegram_id: int, articles: list[dict], pages_depth: int) -> dict[str, list[dict]]:
    """Core parse logic for a list of articles."""

    query_groups = defaultdict(list)
    for article in articles:
        queries = db.get_queries(telegram_id, article["id"])
        for q in queries:
            query_groups[q["query"]].append({
                "article_id": article["id"],
                "sku": article["sku"],
                "query_id": q["id"],
            })

    if not query_groups:
        return {}

    all_results: dict[str, list[dict]] = defaultdict(list)
    total_queries = len(query_groups)
    headers, used_token = _build_headers()

    logger.info(f"Parsing {total_queries} unique queries for {len(articles)} articles")

    async with aiohttp.ClientSession(headers=headers) as session:
        # First pass
        failed_queries = []
        for idx, (query_text, article_entries) in enumerate(query_groups.items()):
            skus = {entry["sku"] for entry in article_entries}
            positions = await search_query_all_pages(session, query_text, skus, pages_depth, used_token)

            has_error = False
            for entry in article_entries:
                sku = entry["sku"]
                pos_data = positions.get(sku, {})
                result = {
                    "sku": sku,
                    "query": query_text,
                    "position": pos_data.get("position"),
                    "page": pos_data.get("page"),
                    "total_found": pos_data.get("total_found", 0),
                    "error": pos_data.get("error", False),
                }
                if result["error"]:
                    has_error = True
                else:
                    db.save_result(
                        telegram_id=telegram_id,
                        article_id=entry["article_id"],
                        query_id=entry["query_id"],
                        position=result["position"],
                        page=result["page"],
                        total_found=result["total_found"],
                    )
                all_results[sku].append(result)

            if has_error:
                failed_queries.append((query_text, article_entries))

            if idx < total_queries - 1:
                await asyncio.sleep(REQUEST_DELAY)

        # Retry pass — repeat only failed queries
        if failed_queries:
            logger.info(f"Retrying {len(failed_queries)} failed queries...")
            await asyncio.sleep(REQUEST_DELAY * 2)

            for query_text, article_entries in failed_queries:
                skus = {entry["sku"] for entry in article_entries}
                positions = await search_query_all_pages(session, query_text, skus, pages_depth, used_token)

                for entry in article_entries:
                    sku = entry["sku"]
                    pos_data = positions.get(sku, {})

                    # Find and update the error result
                    for r in all_results[sku]:
                        if r["query"] == query_text and r["error"]:
                            if not pos_data.get("error", False):
                                r["position"] = pos_data.get("position")
                                r["page"] = pos_data.get("page")
                                r["total_found"] = pos_data.get("total_found", 0)
                                r["error"] = False
                                db.save_result(
                                    telegram_id=telegram_id,
                                    article_id=entry["article_id"],
                                    query_id=entry["query_id"],
                                    position=r["position"],
                                    page=r["page"],
                                    total_found=r["total_found"],
                                )
                            break

                await asyncio.sleep(REQUEST_DELAY)

    return dict(all_results)


_PARSE_SEMAPHORE = asyncio.Semaphore(3)


async def _parse_single_query(session: aiohttp.ClientSession, telegram_id: int,
                               article_id: int, sku: str, q: dict,
                               pages_depth: int, used_token) -> dict:
    """Parse one query under semaphore."""
    async with _PARSE_SEMAPHORE:
        pos_data = await search_query_all_pages(session, q["query"], {sku}, pages_depth, used_token)
        p = pos_data.get(sku, {})

        result = {
            "sku": sku,
            "query": q["query"],
            "position": p.get("position"),
            "page": p.get("page"),
            "total_found": p.get("total_found", 0),
            "error": p.get("error", False),
        }

        if not result["error"]:
            db.save_result(
                telegram_id=telegram_id,
                article_id=article_id,
                query_id=q["id"],
                position=result["position"],
                page=result["page"],
                total_found=result["total_found"],
            )

        return result


async def run_parse(telegram_id: int, article_id: int, sku: str, queries: list[dict],
                    pages_depth: int = 3) -> list[dict]:
    """Parse queries for a single article. Parallel with semaphore."""
    headers, used_token = _build_headers()

    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = [
            _parse_single_query(session, telegram_id, article_id, sku, q, pages_depth, used_token)
            for q in queries
        ]
        results = await asyncio.gather(*tasks)

    # Preserve query order
    return list(results)


_GEO_SEMAPHORE = asyncio.Semaphore(3)


async def _geo_scan_region(session: aiohttp.ClientSession, sku: str, query: str,
                           region: dict, pages_depth: int) -> dict:
    """Scan one region for a SKU. Runs under semaphore."""
    async with _GEO_SEMAPHORE:
        position = None
        for page in range(1, pages_depth + 1):
            params = {
                "appType": WB_APP_TYPE,
                "curr": "rub",
                "dest": region["dest"],
                "lang": "ru",
                "page": page,
                "query": query,
                "resultset": "catalog",
                "sort": "popular",
                "spp": "30",
            }
            for attempt in range(2):
                try:
                    async with session.get(
                        WB_SEARCH_URL, params=params,
                        timeout=aiohttp.ClientTimeout(total=10),
                    ) as resp:
                        if resp.status == 429:
                            if attempt == 0:
                                await asyncio.sleep(1)
                                continue
                            break
                        if resp.status != 200:
                            break
                        data = await resp.json(content_type=None)
                        products = data.get("products", [])
                        for i, p in enumerate(products):
                            if str(p.get("id")) == sku:
                                position = (page - 1) * WB_ITEMS_PER_PAGE + i + 1
                                break
                    break
                except Exception as e:
                    logger.error(f"geo_scan error {region['short']}: {e}")
                    if attempt == 0:
                        await asyncio.sleep(1)
                        continue
                    break

            if position is not None:
                break
            await asyncio.sleep(0.2)

        return {
            "short": region["short"],
            "name": region["name"],
            "position": position,
        }


async def geo_scan(sku: str, query: str, regions: list[dict], pages_depth: int = 5) -> list[dict]:
    """Scan one SKU + query across multiple regions in parallel. Returns [{short, name, position}, ...]."""
    # No Bearer token for geo scan — it personalizes results and hides items in some regions
    headers = {**HEADERS, "User-Agent": random.choice(USER_AGENTS)}
    wbaas = _get_wbaas_token()
    if wbaas:
        headers["Cookie"] = f"x_wbaas_token={wbaas}"

    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = [_geo_scan_region(session, sku, query, r, pages_depth) for r in regions]
        results = await asyncio.gather(*tasks)

    # Preserve region order
    order = {r["short"]: i for i, r in enumerate(regions)}
    return sorted(results, key=lambda x: order[x["short"]])


# ============================================================
# WB card info (brand lookup via basket CDN)
# ============================================================

_BASKET_RANGES = [
    (143, "01"), (287, "02"), (431, "03"), (719, "04"), (1007, "05"),
    (1061, "06"), (1115, "07"), (1169, "08"), (1313, "09"), (1601, "10"),
    (1655, "11"), (1919, "12"), (2045, "13"), (2189, "14"), (2405, "15"),
    (2621, "16"), (2837, "17"), (3053, "18"), (3269, "19"), (3485, "20"),
]


def _get_basket_host(vol: int) -> int:
    """Get basket host number for a given vol."""
    for max_vol, host in _BASKET_RANGES:
        if vol <= max_vol:
            return int(host)
    # For vol > 3485: approximate, will try nearby hosts as fallback
    return 21 + (vol - 3486) // 260


async def fetch_brand(sku: str) -> str:
    """Fetch brand name for a WB article from basket CDN. Returns brand or empty string."""
    try:
        nm = int(sku)
    except ValueError:
        return ""

    vol = nm // 100000
    part = nm // 1000
    base_host = _get_basket_host(vol)

    async with aiohttp.ClientSession() as session:
        # Try calculated host and nearby (±4) in case mapping is slightly off
        for offset in [0, 1, -1, 2, -2, 3, -3, 4, -4]:
            host_num = base_host + offset
            if host_num < 1:
                continue
            host = f"{host_num:02d}"
            url = f"https://basket-{host}.wbbasket.ru/vol{vol}/part{part}/{nm}/info/ru/card.json"
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=3)) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        return data.get("selling", {}).get("brand_name", "")
            except Exception:
                continue
    return ""


# ============================================================
# Recommendation shelf scan
# ============================================================

RECOM_URL = "https://www.wildberries.ru/__internal/recom/recom/ru/common/v8/search"


async def _recom_search(competitor_sku: str,
                        dest: str = WB_DEST) -> tuple[list[dict], bool]:
    """Fetch recommendation shelf via curl_cffi with chrome TLS impersonation
    and full buyer auth (Bearer + PoW + wbaas) — same as proxy_positions."""
    import proxy_positions
    from curl_cffi import requests as curl_requests

    params = {
        "appType": "1",
        "curr": "rub",
        "dest": dest,
        "hide_vflags": "4294967296",
        "lang": "ru",
        "page": "1",
        "query": f"похожие {competitor_sku}",
        "resultset": "catalog",
    }

    def do_request() -> tuple[list[dict], bool]:
        headers = proxy_positions._build_headers("__direct__")
        try:
            resp = curl_requests.get(
                RECOM_URL, params=params, headers=headers,
                impersonate="chrome", timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                return data.get("products", []), False
            if resp.status_code in (429, 451, 498):
                logger.warning("recom %d anti-bot/rate for '%s'",
                               resp.status_code, competitor_sku)
                return [], True
            logger.warning("recom %d for '%s'", resp.status_code, competitor_sku)
            return [], True
        except Exception as e:
            logger.error("recom error '%s': %s", competitor_sku, e)
            return [], True

    for attempt in range(2):
        products, is_error = await asyncio.to_thread(do_request)
        if not is_error:
            return products, False
        if attempt == 0:
            await asyncio.sleep(1)
    return [], True


async def recom_scan_all(our_sku: str, competitors: list[str],
                         dest: str = WB_DEST) -> dict[str, dict]:
    """Scan recommendation shelves of competitors, find our_sku position.

    Returns: {competitor_sku: {"position": N or None, "error": bool}}
    """
    results = {}
    for comp_sku in competitors:
        products, is_error = await _recom_search(comp_sku, dest)

        if is_error:
            results[comp_sku] = {"position": None, "error": True}
        else:
            position = None
            for i, p in enumerate(products):
                if str(p.get("id")) == our_sku:
                    position = i + 1
                    break
            results[comp_sku] = {"position": position, "error": False}

        await asyncio.sleep(REQUEST_DELAY)

    return results
