"""Optional shared pacing of individual search requests across bot and RPC."""

from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import re
import time

import config


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
                "experiment": experiment}
    except (ValueError, TypeError, OverflowError):
        # An expired controller lease must not restore the old request bursts.
        return {"name": "fallback", "gap_ms": 1500, "batch_size": 1,
                "batch_pause_ms": 0, "experiment": False}


class SearchPaused(Exception):
    def __init__(self, error):
        self.error = {**error, "local_pause": True}
        super().__init__("Search cooldown is active")


@contextmanager
def request_slot():
    if policy() is None:
        yield None
        return
    directory = Path(config.DATA_DIR)
    with (directory / "wb_search_request.lock").open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        from wb_search_recovery import cooldown_error, session_generation
        state_path = directory / "wb_search_pacing_state.json"
        state = _read(state_path)
        while True:
            pause = cooldown_error(session_generation())
            if pause:
                raise SearchPaused(pause)
            current = policy()
            if current is None:
                yield None
                return
            count = state.get("count", 0) if state.get("name") == current["name"] else 0
            gap = current["gap_ms"] / 1000
            if count and count % current["batch_size"] == 0:
                gap = max(gap, current["batch_pause_ms"] / 1000)
            remaining = state.get("last_finished_at", 0) + gap - time.time()
            if remaining <= 0:
                break
            time.sleep(min(.2, remaining))
        try:
            yield current
        finally:
            new_state = {"last_finished_at": time.time(), "name": current["name"], "count": count + 1}
            temporary = state_path.with_suffix(f".tmp.{os.getpid()}")
            temporary.write_text(json.dumps(new_state))
            os.chmod(temporary, 0o600)
            os.replace(temporary, state_path)
