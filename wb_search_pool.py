"""Three durable search lanes and explicitly configured standby credentials.

Only account identifiers are persisted here. Credentials remain immutable bundles
owned by wb_sessions. A failed lane may borrow a reserve; a 429 never does so.
"""
import asyncio
from contextlib import contextmanager
import fcntl
import hashlib
import json
from pathlib import Path
import time

import config
import wb_sessions
import wb_search_recovery as recovery


def settings():
    value = wb_sessions.read_json(Path(config.DATA_DIR) / "wb_search_pool.json")
    working, reserve = value.get("working", []), value.get("reserve", [])
    if not isinstance(working, list) or not isinstance(reserve, list):
        return None
    ids = working + reserve
    if (value.get("enabled") is not True or len(working) != 3 or not reserve
            or any(not isinstance(s, str) or not s.isalnum() for s in ids)
            or len(set(ids)) != len(ids)):
        return None
    return {"working": working, "reserve": reserve}


def busy():
    return {"error": True, "error_state": "parser_busy", "local_pause": True,
            "retry_at": time.time() + 5, "retry_after": 5,
            "error_message": "Ожидаем доступности рабочей WB-сессии."}


def paused():
    error = recovery.cooldown_error(recovery.session_generation())
    return {**error, "error": True, "local_pause": True} if error else None


@contextmanager
def lane(configured):
    directory = Path(config.DATA_DIR) / "search_pool"
    directory.mkdir(exist_ok=True)
    end = time.monotonic() + 90
    while time.monotonic() < end:
        if paused():
            yield None
            return
        for index in range(len(configured["working"])):
            handle = (directory / f"lane-{index}.lock").open("a+")
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                handle.close()
                continue
            try:
                if select(configured, index) is None:
                    continue
                yield index
            finally:
                handle.close()
            return
        time.sleep(.02)
    yield None


def select(configured, index, excluded=()):
    """Called while the lane is owned. Never assign a reserve to two lanes."""
    directory = Path(config.DATA_DIR) / "search_pool"
    path = directory / "state.json"
    fingerprint = hashlib.sha256(json.dumps(configured, sort_keys=True).encode()).hexdigest()
    with (directory / "state.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        state = wb_sessions.read_json(path)
        slots = state.get("slots") if state.get("config") == fingerprint else None
        if not isinstance(slots, list) or len(slots) != 3:
            slots = list(configured["working"])
        rows = {s["id"]: s for s in wb_sessions.list_sessions()}
        def healthy(sid):
            row = rows.get(sid, {})
            return (sid not in excluded and row.get("state") == "healthy"
                    and row.get("revision") and (row.get("retry_at") or 0) <= time.time())
        # Return a recovered primary to work, making the standby available again.
        primary = configured["working"][index]
        primary_health = rows.get(primary, {})
        credential_failure = (primary_health.get("state") in ("antibot", "auth_expired", "login_required")
                              and primary_health.get("status_code") in (401, 403, 498))
        already_borrowed = slots[index] in configured["reserve"]
        candidates = [primary]
        if already_borrowed:
            candidates.append(slots[index])
        if credential_failure:
            candidates.extend(configured["reserve"])
        occupied = {sid for n, sid in enumerate(slots) if n != index}
        chosen = next((sid for sid in candidates if sid not in occupied and healthy(sid)), None)
        slots[index] = chosen
        wb_sessions.atomic_json(path, {"config": fingerprint, "slots": slots})
        return wb_sessions.snapshot(chosen) if chosen else None


def execute(fetch):
    """One complete query, at most one fresh retry on an account-specific failure."""
    configured = settings()
    if not configured:
        raise RuntimeError("Search pool is disabled")
    initial_pause = paused()
    if initial_pause:
        return initial_pause
    with lane(configured) as index:
        if index is None:
            return paused() or busy()
        excluded = set()
        for _ in range(2):
            if paused():
                return paused() or busy()
            selected = select(configured, index, excluded)
            if not selected:
                return busy()
            started = time.time()
            item = fetch(selected)
            if not item.get("error"):
                # Health writes are revision guarded. Do not clear a pause created
                # by another request while this one was in flight.
                wb_sessions.record_health(selected, {"state": "healthy", "status": 200, "started_at": started})
                recovery.record_success(selected["session"].get("saved_at", 0), started)
                return item
            if (item.get("local_pause") or item.get("error_state") == "rate_limited"
                    or item.get("status_code") == 429 or item.get("retry_after")):
                return item
            if item.get("error_state") not in ("antibot", "auth_expired") or item.get("status_code") not in (401, 403, 498):
                return item
            wb_sessions.record_health(selected, {"state": item["error_state"], "status": item["status_code"]})
            excluded.add(selected["id"])
        return busy()


async def get_positions(article, keywords, dest=None):
    """RPC entry point. Each phrase owns one lane and one immutable auth bundle."""
    import proxy_positions as parser
    if not settings():
        return await parser.get_positions(article, keywords, **({"dest": dest} if dest is not None else {}))
    dest = parser.DEST if dest is None else dest
    proxy = config.WB_PROXIES[0] if config.WB_PROXIES else ""
    parser._load_token_cache()
    result = {}
    for phrase in keywords:
        def fetch(selected):
            with parser.using_session(selected["session"]):
                with parser.curl_requests.Session(impersonate=parser.WB_CURL_IMPERSONATE) as session:
                    item = parser._fetch_keyword_sync(proxy, phrase, article, dest, session)
                    if item.get("error_state") == "network_error" and not item.get("retry_after"):
                        time.sleep(.5)
                        item = parser._fetch_keyword_sync(proxy, phrase, article, dest, session)
                    return item
        result[phrase] = await asyncio.to_thread(execute, fetch)
    return result
