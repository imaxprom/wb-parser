"""Organic + Promo position fetcher via Chrome (AppleScript).

Uses Chrome browser with WB buyer authentication to fetch positions
from WB Internal API — no rate limits, real-time data.

Algorithm (same as evirma extension):
1. Fetch normal search results (2x300 = 600 products) with auth headers
2. Build organic map: organic_counter -> position_in_normal
3. Fetch no_promo search results (2x300 = 600 products)
4. For target SKU: promo_pos = position in normal, organic_pos = orgMap[nopromo_pos]

v2: UUID-based variables + Semaphore to prevent collisions between
    concurrent requests from different users/articles.
"""

import subprocess
import json
import logging
import asyncio
import os
import tempfile
import urllib.parse
import uuid

logger = logging.getLogger(__name__)

DEST = -951305  # Moscow region

# Semaphore: max 3 concurrent Chrome JS executions
_chrome_semaphore = asyncio.Semaphore(3)

CHUNK_SIZE = 10  # keywords per chunk


def _build_js(query: str, sku: int, dest: int = DEST) -> str:
    """Build JavaScript code to execute in Chrome."""
    encoded_query = urllib.parse.quote(query)
    js = (
        "window.__posResult = null;\n"
        "var td = JSON.parse(localStorage.getItem('wbx__tokenData') || '{}');\n"
        "var pd = JSON.parse(localStorage.getItem('session-pow-token') || '{}');\n"
        "var token = td.token || '';\n"
        "var pow = pd.token || '';\n"
        "var uid = document.cookie.match(/_wbauid=([^;]+)/);\n"
        "var qid = uid ? 'qid' + uid[1] + new Date().toISOString().replace(/\\D+/gi,'').substring(0,14) : '';\n"
        "var headers = {'Authorization':'Bearer '+token,'X-Pow':pow,'X-Queryid':qid};\n"
        f"var base = 'https://www.wildberries.ru/__internal/search/exactmatch/ru/common/v18/search?ab_testing=false&appType=1&curr=rub&dest={dest}&hide_dflags=131072&hide_dtype=10%3B14&inheritFilters=false&lang=ru&query={encoded_query}&resultset=catalog&sort=popular&spp=31&suppressSpellcheck=false&limit=300';\n"
        f"var SKU = {sku};\n"
        "var opts = {headers:headers,credentials:'include',cache:'no-store'};\n"
        "Promise.all([\n"
        "  fetch(base+'&page=1',opts).then(function(r){return r.json()}),\n"
        "  fetch(base+'&page=2',opts).then(function(r){return r.json()}),\n"
        "  fetch(base+'&page=1&ab_testid=no_promo',opts).then(function(r){return r.json()}),\n"
        "  fetch(base+'&page=2&ab_testid=no_promo',opts).then(function(r){return r.json()})\n"
        "]).then(function(arr) {\n"
        "  var normal = (arr[0].products||[]).concat(arr[1].products||[]);\n"
        "  var nopromo = (arr[2].products||[]).concat(arr[3].products||[]);\n"
        "  var orgMap = {}; var cnt = 0; var skuP = null; var tO = 0; var tA = 0;\n"
        "  var isAd = false;\n"
        "  for (var i = 0; i < normal.length; i++) {\n"
        "    if (!normal[i].logs) { cnt++; orgMap[cnt] = i + 1; tO++; } else { tA++; }\n"
        "    if (normal[i].id === SKU) { skuP = i + 1; isAd = !!normal[i].logs; }\n"
        "  }\n"
        "  var skuNp = null; var skuOrg = null;\n"
        "  for (var j = 0; j < nopromo.length; j++) {\n"
        "    if (nopromo[j].id === SKU) { skuNp = j + 1; skuOrg = orgMap[skuNp] || null; break; }\n"
        "  }\n"
        "  window.__posResult = JSON.stringify({ok:true,promo_pos:skuP,organic_pos:skuOrg,nopromo_pos:skuNp,is_advertised:isAd,total_normal:normal.length,total_nopromo:nopromo.length,total_organic:tO,total_ad:tA});\n"
        "}).catch(function(e) {\n"
        "  window.__posResult = JSON.stringify({ok:false,error:e.message});\n"
        "});\n"
    )
    return js


def _run_applescript(js_code: str, var_name: str = "__posResult", timeout: int = 20) -> str:
    """Execute JavaScript in Chrome WB tab via AppleScript and return result."""
    import time

    js_file = tempfile.mktemp(suffix='.js')
    with open(js_file, 'w') as f:
        f.write(js_code)

    exec_script = f'''
    set jsFile to POSIX file "{js_file}"
    set jsCode to read jsFile as «class utf8»
    tell application "Google Chrome"
        repeat with w in windows
            set tabList to tabs of w
            repeat with i from 1 to count of tabList
                set t to item i of tabList
                if URL of t contains "wildberries.ru" then
                    tell t to execute javascript jsCode
                    return "OK"
                end if
            end repeat
        end repeat
        return "NO_WB_TAB"
    end tell
    '''

    read_script = f'''
    tell application "Google Chrome"
        repeat with w in windows
            set tabList to tabs of w
            repeat with i from 1 to count of tabList
                set t to item i of tabList
                if URL of t contains "wildberries.ru" then
                    tell t
                        set result to execute javascript "window.{var_name} || 'NOT_READY'"
                        return result
                    end tell
                end if
            end repeat
        end repeat
        return "NO_WB_TAB"
    end tell
    '''

    # Cleanup script: delete variable after reading
    cleanup_script = f'''
    tell application "Google Chrome"
        repeat with w in windows
            set tabList to tabs of w
            repeat with i from 1 to count of tabList
                set t to item i of tabList
                if URL of t contains "wildberries.ru" then
                    tell t to execute javascript "delete window.{var_name};"
                    return "OK"
                end if
            end repeat
        end repeat
    end tell
    '''

    try:
        proc = subprocess.run(
            ['osascript', '-e', exec_script],
            capture_output=True, text=True, timeout=10
        )
        if proc.returncode != 0:
            logger.error(f"AppleScript exec error: {proc.stderr}")
        if "NO_WB_TAB" in proc.stdout:
            return json.dumps({"ok": False, "error": "No WB tab found in Chrome"})

        for _ in range(int(timeout / 0.3)):
            time.sleep(0.3)
            proc = subprocess.run(
                ['osascript', '-e', read_script],
                capture_output=True, text=True, timeout=5
            )
            result = proc.stdout.strip()
            if result and result != 'NOT_READY' and result != 'NO_WB_TAB':
                # Cleanup: remove variable from window
                try:
                    subprocess.run(
                        ['osascript', '-e', cleanup_script],
                        capture_output=True, text=True, timeout=3
                    )
                except Exception:
                    pass
                return result

        return json.dumps({"ok": False, "error": "Timeout waiting for Chrome response"})
    except subprocess.TimeoutExpired:
        return json.dumps({"ok": False, "error": "AppleScript timeout"})
    except Exception as e:
        return json.dumps({"ok": False, "error": str(e)})
    finally:
        try:
            os.unlink(js_file)
        except:
            pass


def _build_multi_js(queries: list[str], sku: int, request_id: str, dest: int = DEST) -> str:
    """Build JS that fetches keywords for one SKU. Uses unique request_id variable."""
    queries_json = json.dumps(queries, ensure_ascii=False)
    var_name = f"__res_{request_id}"
    js = (
        f"window.{var_name} = null;\n"
        "var td = JSON.parse(localStorage.getItem('wbx__tokenData') || '{}');\n"
        "var pd = JSON.parse(localStorage.getItem('session-pow-token') || '{}');\n"
        "var token = td.token || '';\n"
        "var pow = pd.token || '';\n"
        "var uid = document.cookie.match(/_wbauid=([^;]+)/);\n"
        "var qid = uid ? 'qid' + uid[1] + new Date().toISOString().replace(/\\D+/gi,'').substring(0,14) : '';\n"
        "var headers = {'Authorization':'Bearer '+token,'X-Pow':pow,'X-Queryid':qid};\n"
        "var opts = {headers:headers,credentials:'include',cache:'no-store'};\n"
        f"var SKU = {sku};\n"
        f"var queries = {queries_json};\n"
        f"var dest = {dest};\n"
        "var baseUrl = 'https://www.wildberries.ru/__internal/search/exactmatch/ru/common/v18/search?ab_testing=false&appType=1&curr=rub&dest='+dest+'&hide_dflags=131072&hide_dtype=10%3B14&inheritFilters=false&lang=ru&resultset=catalog&sort=popular&spp=31&suppressSpellcheck=false&limit=300';\n"
        "\n"
        "function doQuery(q) {\n"
        "  var eq = encodeURIComponent(q);\n"
        "  var url = baseUrl + '&query=' + eq;\n"
        "  return Promise.all([\n"
        "    fetch(url+'&page=1',opts).then(function(r){return r.json()}),\n"
        "    fetch(url+'&page=2',opts).then(function(r){return r.json()}),\n"
        "    fetch(url+'&page=1&ab_testid=no_promo',opts).then(function(r){return r.json()}),\n"
        "    fetch(url+'&page=2&ab_testid=no_promo',opts).then(function(r){return r.json()})\n"
        "  ]).then(function(arr) {\n"
        "    var normal = (arr[0].products||[]).concat(arr[1].products||[]);\n"
        "    var nopromo = (arr[2].products||[]).concat(arr[3].products||[]);\n"
        "    var orgMap = {}; var cnt = 0; var skuP = null; var isAd = false;\n"
        "    for (var i = 0; i < normal.length; i++) {\n"
        "      if (!normal[i].logs) { cnt++; orgMap[cnt] = i + 1; }\n"
        "      if (normal[i].id === SKU) { skuP = i + 1; isAd = !!normal[i].logs; }\n"
        "    }\n"
        "    var skuOrg = null;\n"
        "    for (var j = 0; j < nopromo.length; j++) {\n"
        "      if (nopromo[j].id === SKU) { skuOrg = orgMap[j+1] || null; break; }\n"
        "    }\n"
        "    return {query:q,promo_pos:skuP,organic_pos:skuOrg,is_advertised:isAd};\n"
        "  });\n"
        "}\n"
        "\n"
        "// Sequential: one keyword at a time to avoid rate limit\n"
        "async function runAll() {\n"
        "  var results = [];\n"
        "  for (var k = 0; k < queries.length; k++) {\n"
        "    var r = await doQuery(queries[k]);\n"
        "    results.push(r);\n"
        "  }\n"
        "  return results;\n"
        "}\n"
        "runAll().then(function(results) {\n"
        f"  window.{var_name} = JSON.stringify({{ok:true,results:results}});\n"
        "}).catch(function(e) {\n"
        f"  window.{var_name} = JSON.stringify({{ok:false,error:e.message}});\n"
        "});\n"
    )
    return js


def get_positions_chunk_sync(sku: int, keywords: list[str], request_id: str, dest: int = DEST) -> dict:
    """Get positions for one SKU across a chunk of keywords. Uses unique request_id."""
    var_name = f"__res_{request_id}"
    js = _build_multi_js(keywords, sku, request_id, dest)
    # Each keyword = 4 fetches × ~3s = ~12s; chunk of 10 = ~60s max
    timeout = max(45, len(keywords) * 8)
    raw = _run_applescript(js, var_name=var_name, timeout=timeout)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"ok": False, "error": f"Invalid JSON: {raw[:100]}"}


# Keep old function for backward compatibility (single-keyword use)
def get_positions_multi_sync(sku: int, keywords: list[str], dest: int = DEST) -> dict:
    """Legacy wrapper — now uses UUID internally."""
    rid = uuid.uuid4().hex[:8]
    return get_positions_chunk_sync(sku, keywords, rid, dest)


async def _run_chunk(article: int, chunk: list[str], dest: int, max_retries: int = 2) -> list[dict]:
    """Run one chunk of keywords through Chrome with semaphore and retry on rate limit."""
    for attempt in range(max_retries + 1):
        rid = uuid.uuid4().hex[:8]
        async with _chrome_semaphore:
            data = await asyncio.to_thread(get_positions_chunk_sync, article, chunk, rid, dest)
        if data.get("ok"):
            return data.get("results", [])
        error = data.get("error", "")
        # Rate limit: WB returns HTML instead of JSON
        if "DOCTYPE" in error or "not valid JSON" in error:
            if attempt < max_retries:
                wait = 5 * (attempt + 1)  # 5s, 10s
                logger.warning(f"WB rate limit for article={article} (attempt {attempt+1}), retrying in {wait}s...")
                await asyncio.sleep(wait)
                continue
        logger.error(f"Chrome chunk error (article={article}, rid={rid}): {error}")
        return [{"query": kw, "promo_pos": None, "organic_pos": None, "is_advertised": False} for kw in chunk]
    return [{"query": kw, "promo_pos": None, "organic_pos": None, "is_advertised": False} for kw in chunk]


async def get_positions(article: int, keywords: list[str],
                         dest: int = DEST) -> dict[str, dict]:
    """
    Get organic + promo positions for an article across multiple keywords.

    Keywords are split into chunks of CHUNK_SIZE, each chunk runs with a
    unique UUID variable in Chrome. Semaphore limits concurrent executions to 3.

    Returns: {
        "трусы женские": {"promo_pos": 16, "organic_pos": 436, "is_advertised": True},
        ...
    }
    """
    result = {}

    try:
        # Split keywords into chunks
        chunks = [keywords[i:i + CHUNK_SIZE] for i in range(0, len(keywords), CHUNK_SIZE)]
        logger.info(f"Fetching positions for {article}: {len(keywords)} keywords in {len(chunks)} chunks")

        # Run chunks SEQUENTIALLY — Chrome can't handle parallel JS in same tab
        chunk_results = []
        for chunk in chunks:
            items = await _run_chunk(article, chunk, dest)
            chunk_results.append(items)

        # Merge results
        for items in chunk_results:
            for item in items:
                kw = item.get("query", "")
                result[kw] = {
                    "promo_pos": item.get("promo_pos"),
                    "organic_pos": item.get("organic_pos"),
                    "is_advertised": item.get("is_advertised", False),
                }
                logger.info(
                    f"Positions for {article} '{kw}': "
                    f"promo={item.get('promo_pos')} organic={item.get('organic_pos')} "
                    f"is_ad={item.get('is_advertised')}"
                )
    except Exception as e:
        logger.error(f"Chrome positions error: {e}")
        for kw in keywords:
            result[kw] = {"promo_pos": None, "organic_pos": None, "is_advertised": False}

    # Fill missing keywords
    for kw in keywords:
        if kw not in result:
            result[kw] = {"promo_pos": None, "organic_pos": None, "is_advertised": False}

    return result
