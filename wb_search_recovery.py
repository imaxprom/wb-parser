"""Persistent, bounded recovery for ordinary WB search batches.

No periodic requests or SMS: resume the saved login only after an access failure.
The browser worker validates the candidate before replacing the saved session.
"""

import fcntl
import json
import logging
import os
from pathlib import Path
import time

import config

logger = logging.getLogger(__name__)


def _read(path):
    try:
        value = json.loads(path.read_text())
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def _state_path():
    return Path(config.DATA_DIR) / "wb_search_recovery.json"


def session_generation():
    return _read(Path(config.DATA_DIR) / "wb_session.json").get("saved_at", 0)


def _save(state):
    path = _state_path()
    temporary = path.with_suffix(f".tmp.{os.getpid()}")
    temporary.write_text(json.dumps(state))
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def cooldown_error(generation):
    state = _read(_state_path())
    # A manual/external login clears session-specific failure state, but cannot
    # cancel a server-requested delay or a rate limit.
    if state.get("session_saved_at") != generation and not state.get("server_delay"):
        return None
    remaining = max(0, int(state.get("retry_at", 0) - time.time() + 0.999))
    if not remaining:
        return None
    return {"error_state": state.get("error_state", "antibot"),
            "status_code": state.get("status_code"), "retry_after": remaining,
            "recovery_reason": state.get("last_result"),
            "error_message": state.get("error_message", "WB session recovery is pending")}


def _defer(previous, error, generation, *, reason="rejected", minimum=0):
    failures = (previous.get("failures", 0) if previous.get("session_saved_at") == generation else 0) + 1
    requested = error.get("retry_after") or 0
    delay = max(minimum, requested, min(900 * 2 ** min(failures - 1, 3), 7200))
    if previous.get("server_delay"):
        delay = max(delay, previous.get("retry_at", 0) - time.time())
    message = "WB временно недоступен; автоматическая повторная проверка разрешена после паузы."
    if reason == "login_required":
        delay = max(delay, 21600)
        message = "WB требует повторного входа с подтверждением; обновите WB-сессию через меню."
    state = {**previous, "session_saved_at": generation, "failures": failures,
             "retry_at": time.time() + delay,
             "error_state": "auth_expired" if reason == "login_required" else error.get("error_state", "antibot"),
             "status_code": error.get("status_code"), "error_message": message,
             "server_delay": bool(requested or error.get("error_state") == "rate_limited"),
             "last_result": reason}
    _save(state)
    logger.warning("WB search recovery deferred: reason=%s delay=%ss", reason, delay)


def _refresh():
    # Reuse the exact bounded WB.ID path exercised by the 12-hour observation.
    from scripts.wb_session_monitor import refresh_subprocess
    return refresh_subprocess()


def recover(error, observed_generation, *, allow_refresh=True):
    """Return True only when the caller can retry with a replaced session.

    A process lock coalesces concurrent failures. A persisted cooldown survives
    service restarts and prevents each queued article from opening a browser.
    """
    directory = Path(config.DATA_DIR)
    with (directory / "wb_search_recovery.lock").open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        current = session_generation()
        state = _read(_state_path())
        if error.get("retry_after") or error.get("error_state") == "rate_limited":
            _defer(state, error, current, reason="server_pause")
            return False
        if cooldown_error(current):
            return False
        if allow_refresh and current and current != observed_generation:
            return True
        if (not allow_refresh or error.get("error_state") not in ("antibot", "auth_expired")
                or error.get("status_code") not in (401, 403, 498)):
            _defer(state, error, current)
            return False
        if time.time() - state.get("last_refresh_at", 0) < 900:
            _defer(state, error, current, reason="recent_refresh")
            return False
        # Avoid waiting for an interactive login/cart refresh inside the child.
        # The child takes this same lock for the actual browser operation.
        with (directory / "wb_session_refresh.lock").open("a+") as browser_lock:
            try:
                fcntl.flock(browser_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                _save({**state, "session_saved_at": current, "retry_at": time.time() + 30,
                       "server_delay": False, "error_state": "auth_expired",
                       "status_code": error.get("status_code"), "last_result": "login_in_progress",
                       "error_message": "Выполняется обновление WB-сессии. Повторите проверку немного позже."})
                return False
        # If the parent is killed, a new process must not immediately repeat a
        # possibly still-running login. Normal child lifetime is at most 160s.
        _save({**state, "session_saved_at": current, "retry_at": time.time() + 180,
               "server_delay": False, "error_state": error.get("error_state"),
               "status_code": error.get("status_code"), "last_result": "recovering"})
        logger.info("WB search recovery started")
        try:
            renewal = _refresh()
        except Exception as exc:
            logger.warning("WB search recovery worker failed: %s", type(exc).__name__)
            renewal = {"state": "refresh_error"}
        verification = renewal.get("verification", {})
        if (renewal.get("state") == "refreshed" and verification.get("state") == "healthy"
                and session_generation() != current):
            _save({"session_saved_at": session_generation(), "failures": 0,
                   "retry_at": 0, "last_refresh_at": time.time(), "last_result": "refreshed"})
            logger.info("WB search recovery succeeded; resuming batch")
            return True
        failed = dict(error)
        failed["retry_after"] = max(failed.get("retry_after") or 0, verification.get("retry_after") or 0)
        if verification.get("state") == "rate_limited":
            failed["error_state"] = "rate_limited"
        _defer(state, failed, session_generation(), reason=renewal.get("state", "refresh_error"))
        return False
