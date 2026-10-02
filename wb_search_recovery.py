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

RATE_LIMIT_INITIAL_DELAY = 300


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
    # A separate append-only metadata journal also covers subprocess/CLI users
    # whose stdout is not captured by the bot's systemd journal.
    fields = ("session_saved_at", "retry_at", "last_result", "status_code", "failures",
              "last_refresh_at", "pause_origin", "request_source", "response_id",
              "server_retry_after_seconds", "last_login_reason", "experiment_stage", "experiment_429_count")
    event = {"at": time.time(), "pid": os.getpid(), **{key: state[key] for key in fields if key in state}}
    try:
        fd = os.open(path.with_name("wb_search_recovery_events.jsonl"), os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
        with os.fdopen(fd, "a") as log:
            log.write(json.dumps(event) + "\n")
    except OSError as exc:
        logger.error("WB recovery audit append failed: %s", type(exc).__name__)


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
            "retry_at": state.get("retry_at"), "retry_session_saved_at": state.get("session_saved_at"),
            "recovery_reason": state.get("last_result"),
            "error_message": state.get("error_message", "WB session recovery is pending")}


def record_success(generation, started_at):
    """Reset backoff after a complete keyword fetched after the last pause.

    A concurrent request that started before a newer failure must not clear its
    pause, even if that request finishes after the deadline. Old-session results
    cannot acknowledge recovery of a replacement session either.
    """
    if not _read(_state_path()).get("failures"):
        return False
    with (Path(config.DATA_DIR) / "wb_search_recovery.lock").open("a+") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        state = _read(_state_path())
        if (not state.get("failures") or session_generation() != generation
                or state.get("retry_at", 0) > started_at):
            return False
        _save({"session_saved_at": generation, "failures": 0, "retry_at": 0,
               "last_refresh_at": state.get("last_refresh_at", 0),
               **{k: state[k] for k in ("experiment_stage", "experiment_429_count") if k in state},
               "last_result": "search_healthy", "status_code": 200})
        logger.info("WB search recovered without login; failure backoff reset")
        return True


def _defer(previous, error, generation, *, reason="rejected", minimum=0):
    failures = (previous.get("failures", 0) if previous.get("session_saved_at") == generation else 0) + 1
    requested = error.get("retry_after") or 0
    initial = RATE_LIMIT_INITIAL_DELAY if error.get("error_state") == "rate_limited" else 900
    delay = max(minimum, requested, min(initial * 2 ** min(failures - 1, 5), 7200))
    if error.get("error_state") == "rate_limited":
        from wb_search_pacing import policy
        pacing = policy()
        if pacing and pacing["experiment"]:
            checks = previous.get("experiment_429_count", 0) if previous.get("experiment_stage") == pacing["name"] else 0
            previous = {**previous, "experiment_stage": pacing["name"], "experiment_429_count": checks + 1}
            delay = max(minimum, requested, (60, 120, 300)[min(checks, 2)])
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
    source = error.get("request_source")
    state["request_source"] = source if source in ("search_http", "recovery_verification") else "unspecified"
    response_id = error.get("response_id", "")
    state["response_id"] = response_id if isinstance(response_id, str) and len(response_id) == 32 and all(c in "0123456789abcdef" for c in response_id) else None
    state["server_retry_after_seconds"] = requested
    state["pause_origin"] = ("retry_after" if requested else "local_rate_limit_backoff"
                             if error.get("error_state") == "rate_limited" else "local_recovery_backoff")
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
    if error.get("local_pause") or error.get("pause_registered"):
        return False  # No new HTTP failure: do not extend an existing pause.
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
        if verification:
            failed["request_source"] = "recovery_verification"
            failed["status_code"] = verification.get("status")
            failed.pop("response_id", None)
        reason = renewal.get("state", "refresh_error")
        state["last_login_reason"] = renewal.get("reason")
        state["last_login_diagnostics"] = renewal.get("diagnostics", {})
        if reason == "login_required" and renewal.get("reason") == "phone_or_sms_required":
            # One fresh browser seeing a phone form is not enough to infer a
            # revoked WB.ID login. Confirm in a second attempt after backoff.
            same_session = state.get("session_saved_at") == session_generation()
            previous_check = state.get("last_result") in ("login_unconfirmed", "login_required")
            checks = state.get("login_required_checks", 0) if same_session and previous_check else 0
            state["login_required_checks"] = checks + 1
            if checks == 0:
                reason = "login_unconfirmed"
                failed["error_state"] = "auth_expired"
        else:
            state["login_required_checks"] = 0
        _defer(state, failed, session_generation(), reason=reason)
        return False
