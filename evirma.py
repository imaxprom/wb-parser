"""Organic + Promo position fetcher.

Uses evirma API for organic positions (accurate, cached by evirma extension).
Uses WB Search API for promo positions (real-time).
"""

import aiohttp
import asyncio
import logging
from typing import Optional
import parser as wb_parser

logger = logging.getLogger(__name__)

# Evirma API (organic positions from WB advertising cabinet extension)
EVIRMA_URL = "https://evirma.ru/api/v1/plog/get"
EVIRMA_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6ImEwMGNhNmUxLWVkN2YtNGE2ZC1hNWUyLWJlYzI3YTdkOTRhNiIsInR5cGUiOiJhY2Nlc3MiLCJ0aW1lIjoxNzc0OTkxNDc0LCJleHAiOjE4MDY2MTM4NzR9.5EItE858S8CwO2eNXeGWvQZi2yNwd2Er8BHV3p3-qbc"
EVIRMA_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {EVIRMA_TOKEN}",
    "evirma-wb-deviceid": "976d0198-67d3-4c9a-b766-b9bad42e8f4d",
    "evirma-wb-sellerid": "e0334427-4f82-4bc3-a0ab-43394e58b6ac",
    "evirma-wb-userid": "19845108",
    "Origin": "https://cmp.wildberries.ru",
    "Referer": "https://cmp.wildberries.ru/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}


async def _get_evirma_positions(article: int, keywords: list[str]) -> dict[str, dict]:
    """Get organic+promo positions from evirma API."""
    result = {}
    try:
        async with aiohttp.ClientSession() as session:
            payload = {"type": "search", "article": article, "region": 1, "keywords": keywords}
            async with session.post(EVIRMA_URL, json=payload, headers=EVIRMA_HEADERS,
                                    timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    return result
                data = await resp.json()
                if data.get("error"):
                    return result
                for kw_data in data.get("data", {}).get("keywords", []):
                    keyword = kw_data.get("keyword", "")
                    dates = kw_data.get("dates", [])
                    if dates:
                        latest = dates[0]
                        result[keyword] = {
                            "organic_pos": latest.get("organic_pos"),
                            "promo_pos": latest.get("promo_pos"),
                        }
    except Exception as e:
        logger.error(f"Evirma API error: {e}")
    return result


async def _get_wb_promo_positions(session: aiohttp.ClientSession, sku: str, 
                                   keywords: list[str], token_row) -> dict[str, int]:
    """Get real-time promo positions from WB Search API."""
    result = {}
    for keyword in keywords:
        try:
            for page in range(1, 7):
                data, err = await wb_parser._search(session, keyword, page, token_row)
                if err:
                    break
                products = data.get("products", [])
                if not products:
                    break
                for i, product in enumerate(products):
                    if str(product.get("id")) == sku:
                        result[keyword] = (page - 1) * wb_parser.WB_ITEMS_PER_PAGE + i + 1
                        break
                if keyword in result:
                    break
                await asyncio.sleep(wb_parser.REQUEST_DELAY)
        except Exception as e:
            logger.error(f"WB search error for '{keyword}': {e}")
        await asyncio.sleep(0.3)
    return result


async def get_positions(article: int, keywords: list[str], region: int = 1,
                        pages_depth: int = 6) -> dict[str, dict]:
    """
    Get organic + promo positions for an article.
    
    Organic: from evirma API (accurate, based on WB advertising cabinet data)
    Promo: from WB Search API (real-time)
    
    Returns: {
        "трусы женские": {"organic_pos": 444, "promo_pos": 16},
        ...
    }
    """
    sku = str(article)
    
    # Get evirma data (organic + cached promo)
    evirma_data = await _get_evirma_positions(article, keywords)
    
    # Get real-time promo positions from WB
    headers, token_row = wb_parser._build_headers()
    async with aiohttp.ClientSession(headers=headers) as session:
        wb_promo = await _get_wb_promo_positions(session, sku, keywords, token_row)
    
    # Merge: WB promo (real-time) + evirma organic
    result = {}
    for keyword in keywords:
        ev = evirma_data.get(keyword, {})
        promo = wb_promo.get(keyword) or ev.get("promo_pos")
        organic = ev.get("organic_pos")
        result[keyword] = {"promo_pos": promo, "organic_pos": organic}
    
    return result
