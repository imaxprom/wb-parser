"""Persistent, bounded recovery for ordinary WB search batches.

No periodic requests or SMS: resume the saved login only after an access failure.
The browser worker validates the candidate before replacing the saved session.
"""

import fcntl
import json
import logging
import math
import os
from pathlib import Path
import time

import config
import wb_sessions
from contextlib import contextmanager

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
    return wb_sessions.generation()


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
    remaining = max(0, math.ceil(state.get("retry_at", 0) - time.time()))
    if not remaining:
        return None
    local_wait = wb_sessions.enabled() and not state.get("server_delay")
    # The website has its own durable retry deadlines and a short-lived Meta
    # cache. Advertise a local recheck, not a six-hour wait after manual login.
    retry_at = min(state.get("retry_at", 0), time.time() + 15) if local_wait else state.get("retry_at")
    remaining = min(remaining, 15) if local_wait else remaining
    return {"error_state": "parser_busy" if local_wait else state.get("error_state", "antibot"),
            "access_error_state": state.get("error_state"), "local_pause": True,
            "status_code": state.get("status_code"), "retry_after": remaining,
            "retry_at": retry_at, "retry_session_saved_at": state.get("session_saved_at"),
            "recovery_reason": state.get("last_result"),
            "error_message": state.get("error_message", "WB session recovery is pending")}


def record_success(generation, started_at):
    """Reset backoff after a complete keyword fetched after the last pause.

    A concurrent request that started before a newer failure must not clear its
    pause, even if that request finishes after the deadline. Old-session results
    cannot acknowledge recovery of a replacement session either.
    """
    latest = _read(_state_path())
    if latest.get("failures") and (session_generation() != generation or latest.get("retry_at", 0) > started_at):
        return False
    if wb_sessions.enabled():
        value = wb_sessions.snapshot()
        if value["session"].get("saved_at", 0) == generation:
            row = next((s for s in wb_sessions.list_sessions() if s["active"]), None)
            if row and (row["state"] != "healthy" or row["checked_at"] < time.time() - 60):
                wb_sessions.record_health(value, {"state": "healthy", "status": 200})
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


@contextmanager
def _state_lock():
    with (Path(config.DATA_DIR) / "wb_search_recovery.lock").open("a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        yield


def _finish_defer(previous, error, generation, **kwargs):
    with _state_lock():
        latest = _read(_state_path())
        if latest.get("server_delay") and latest.get("retry_at", 0) > time.time():
            return False
        if session_generation() != generation:
            return True
        _defer(previous, error, generation, **kwargs)
    return False


def _finish_success(result):
    with _state_lock():
        latest = _read(_state_path())
        if latest.get("server_delay") and latest.get("retry_at", 0) > time.time():
            return False
        _save({"session_saved_at": session_generation(), "failures": 0,
               "retry_at": 0, "last_refresh_at": time.time(), "last_result": result})
    return True


def recover(error, observed_generation, *, allow_refresh=True):
    """Coalesce recovery, without holding the state lock during network I/O.

    A separate operation lock orders failover/manual activation. HTTP 429 can
    still publish a global pause while a browser or reserve probe is running.
    """
    if error.get("local_pause") or error.get("pause_registered"):
        return False
    observed_generation = error.get("session_generation", observed_generation)
    with _state_lock():
        current = session_generation()
        state = _read(_state_path())
        if error.get("retry_after") or error.get("error_state") == "rate_limited":
            _defer(state, error, current, reason="server_pause")
            return False
        if not allow_refresh:
            if not cooldown_error(current) and current == observed_generation:
                _defer(state, error, current)
            return False
    with wb_sessions.lock("switch"):
        with _state_lock():
            current = session_generation()
            state = _read(_state_path())
            if cooldown_error(current):
                return False
            if current and current != observed_generation:
                return True
            if (error.get("error_state") not in ("antibot", "auth_expired")
                    or error.get("status_code") not in (401, 403, 498)):
                _defer(state, error, current)
                return False
            _save({**state, "session_saved_at": current, "retry_at": time.time() + 180,
                   "server_delay": False, "error_state": error.get("error_state"),
                   "status_code": error.get("status_code"), "last_result": "recovering"})
        replacement = wb_sessions.failover(error, current)
        if replacement:
            if replacement.get("state") == "switched":
                return _finish_success("switched")
            if replacement.get("state") in ("paused", "rate_limited"):
                return False
        if time.time() - state.get("last_refresh_at", 0) < 900:
            return _finish_defer(state, error, current, reason="recent_refresh")
        # Manual login holds only its own slot's lock. Legacy mode keeps the
        # original lock contract for operational tools and single-session tests.
        value = wb_sessions.snapshot()
        lock_path = (wb_sessions.root() / ("slot_" + value["id"] + ".lock") if value.get("id")
                     else Path(config.DATA_DIR) / "wb_session_refresh.lock")
        with lock_path.open("a+") as browser_lock:
            try:
                fcntl.flock(browser_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return _finish_defer(state, error, current, reason="login_in_progress", minimum=30)
        logger.info("WB search recovery started")
        try:
            renewal = _refresh()
        except Exception as exc:
            logger.warning("WB search recovery worker failed: %s", type(exc).__name__)
            renewal = {"state": "refresh_error"}
        verification = renewal.get("verification", {})
        if (renewal.get("state") == "refreshed" and verification.get("state") == "healthy"
                and session_generation() != current):
            return _finish_success("refreshed")
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
            same_session = state.get("session_saved_at") == session_generation()
            previous_check = state.get("last_result") in ("login_unconfirmed", "login_required")
            checks = state.get("login_required_checks", 0) if same_session and previous_check else 0
            state["login_required_checks"] = checks + 1
            if checks == 0:
                reason = "login_unconfirmed"
                failed["error_state"] = "auth_expired"
        else:
            state["login_required_checks"] = 0
        if reason == "login_required":
            wb_sessions.record_health(value, {"state": "login_required", "status": failed.get("status_code")})
        return _finish_defer(state, failed, current, reason=reason)
