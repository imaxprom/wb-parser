"""Optional shared pacing of individual search requests across bot and RPC."""

from contextlib import contextmanager
from contextvars import ContextVar
import fcntl
from functools import wraps
import json
import os
from pathlib import Path
import re
import sys
import time

import config

_client = ContextVar("wb_search_pacing_client", default=None)


@contextmanager
def client_scope(name):
    """Select a known traffic source; propagated by asyncio.to_thread."""
    if name not in ("bot", "rpc", "benchmark"):
        raise ValueError("Unknown search client")
    token = _client.set(name)
    try:
        yield
    finally:
        _client.reset(token)


def client_name():
    if _client.get():
        return _client.get()
    program = Path(getattr(sys.modules.get("__main__"), "__file__", "interactive")).name
    return {"bot.py": "bot", "positions_rpc.py": "rpc"}.get(program, "default")


def _priority_file():
    path = Path(config.DATA_DIR) / "wb_search_bot_priority.lock"
    return os.fdopen(os.open(path, os.O_RDWR | os.O_CREAT, 0o600), "a+")


def reserve_bot_priority():
    """Reserve across requests; process exit also releases the reservation."""
    if client_name() != "bot":
        return None
    handle = _priority_file()
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_SH)
        return handle
    except BaseException:
        handle.close()
        raise


def bot_priority_active():
    with _priority_file() as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        return False


@contextmanager
def bot_priority():
    handle = reserve_bot_priority()
    try:
        yield
    finally:
        if handle is not None:
            handle.close()


def bot_priority_job(function):
    """Keep priority across all articles and awaits of one Telegram check."""
    @wraps(function)
    async def wrapped(*args, **kwargs):
        with client_scope("bot"), bot_priority():
            return await function(*args, **kwargs)
    return wrapped


def _read(path):
    try:
        value = json.loads(path.read_text())
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def policy():
    raw = _read(Path(config.DATA_DIR) / "wb_search_pacing.json")
    if raw.get("enabled") is not True:
        return None
    scope = None
    profiles = raw.get("profiles")
    if isinstance(profiles, dict):
        scope = client_name()
        override = profiles.get(scope, {})
        if isinstance(override, dict):
            raw = {**raw, **{k: override[k] for k in
                   ("name", "gap_ms", "batch_size", "batch_pause_ms", "experiment", "expires_at")
                   if k in override}}
    try:
        name = raw.get("name", "paced")
        if not isinstance(name, str) or not re.fullmatch(r"[a-zA-Z0-9_]{1,40}", name):
            raise ValueError
        expires = float(raw.get("expires_at") or 0)
        experiment = raw.get("experiment") is True
        if (expires and expires <= time.time()) or (experiment and not expires):
            raise ValueError
        return {"name": name, "gap_ms": max(0, min(5000, int(raw.get("gap_ms", 1500)))),
                "batch_size": max(1, min(100, int(raw.get("batch_size", 1)))),
                "batch_pause_ms": max(0, min(30000, int(raw.get("batch_pause_ms", 0)))),
                "experiment": experiment, **({"scope": scope} if scope else {})}
    except (ValueError, TypeError, OverflowError):
        # An expired controller lease must not restore the old request bursts.
        return {"name": "fallback", "gap_ms": 1500, "batch_size": 1,
                "batch_pause_ms": 0, "experiment": False, **({"scope": scope} if scope else {})}


class SearchPaused(Exception):
    def __init__(self, error):
        self.error = {**error, "local_pause": True}
        super().__init__("Search cooldown is active")


@contextmanager
def request_slot():
    directory = Path(config.DATA_DIR)
    with (directory / "wb_search_request.lock").open("a+") as lock:
        from wb_search_recovery import cooldown_error, session_generation
        state_path = directory / "wb_search_pacing_state.json"
        while True:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            if client_name() != "bot" and bot_priority_active():
                # The already admitted HTTP request may finish. No later
                # website request starts until all bot reservations finish.
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
                time.sleep(.05)
                continue
            state = _read(state_path)
            pause = cooldown_error(session_generation())
            if pause:
                raise SearchPaused(pause)
            current = policy()
            if current is None:
                yield None
                return
            scope = current.get("scope")
            clients = state.get("clients", {})
            own = clients.get(scope, {}) if scope else state
            count = own.get("count", 0) if own.get("name") == current["name"] else 0
            gap = current["gap_ms"] / 1000
            if count and count % current["batch_size"] == 0:
                gap = max(gap, current["batch_pause_ms"] / 1000)
            ready_at = max(own.get("last_finished_at", 0) + gap,
                           state.get("last_finished_at", 0) + current["gap_ms"] / 1000)
            remaining = ready_at - time.time()
            if remaining <= 0:
                break
            # A bot's inter-batch pause must not monopolize the shared HTTP
            # slot. Re-read state under the lock before actually sending.
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
            time.sleep(min(.2, remaining))
        try:
            yield current
        finally:
            new_state = {"last_finished_at": time.time(), "name": current["name"], "count": count + 1}
            if scope:
                new_state["clients"] = {**clients, scope: dict(new_state)}
            temporary = state_path.with_suffix(f".tmp.{os.getpid()}")
            temporary.write_text(json.dumps(new_state))
            os.chmod(temporary, 0o600)
            os.replace(temporary, state_path)
