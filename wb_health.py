"""Shared WB access classification and cross-process cooldown state."""

import fcntl
import json
import os
import re
import time
from pathlib import Path

import config


WB_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/142.0.0.0 Safari/537.36"
)
WB_CURL_IMPERSONATE = "chrome142"

ANTIBOT_HTTP_STATUSES = frozenset({403, 451, 498})
RATE_LIMIT_HTTP_STATUSES = frozenset({429})
AUTH_HTTP_STATUSES = frozenset({401})

ANTIBOT_BASE_COOLDOWN_SECONDS = 15 * 60
ANTIBOT_MAX_COOLDOWN_SECONDS = 2 * 60 * 60
RATE_LIMIT_COOLDOWN_SECONDS = 10 * 60
NETWORK_ERROR_COOLDOWN_SECONDS = 2 * 60
AUTH_EXPIRED_RECHECK_SECONDS = 6 * 60 * 60

_STATE_PATH = Path(config.DATA_DIR) / "wb_access_health.json"
_LOCK_PATH = Path(config.DATA_DIR) / "wb_access_health.lock"

_ANTIBOT_MARKERS = (
    "подозрительная активность",
    "проверяем браузер",
    "что-то не так",
    "captcha-support@rwb.ru",
)


class WbAccessError(RuntimeError):
    """Base exception for a classified WB access failure."""

    state = "error"

    def __init__(self, message: str, *, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class WbAntibotError(WbAccessError):
    state = "antibot"

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retry_after_seconds: int | None = None,
    ):
        super().__init__(message, status_code=status_code)
        self.retry_after_seconds = retry_after_seconds


class WbRateLimitError(WbAccessError):
    state = "rate_limited"


class WbAuthExpiredError(WbAccessError):
    state = "auth_expired"


class WbNetworkError(WbAccessError):
    state = "network_error"


def is_antibot_response(status_code: int | None, body: str = "") -> bool:
    normalized = (body or "").lower()
    return status_code in ANTIBOT_HTTP_STATUSES or any(
        marker in normalized for marker in _ANTIBOT_MARKERS
    )


def parse_challenge_retry_seconds(body: str) -> int | None:
    match = re.search(
        r"(?:новая попытка через|retry[^0-9]{0,20})\s*(\d{1,2}):(\d{2})",
        body or "",
        re.I,
    )
    if not match:
        return None
    return int(match.group(1)) * 60 + int(match.group(2))


def build_antibot_error(status_code: int | None, body: str = "") -> WbAntibotError:
    details = []
    challenge_id = re.search(r"\bID:\s*([a-f0-9]{12,})", body or "", re.I)
    blocked_ip = re.search(r"\bIP:\s*([0-9a-f:.]+)", body or "", re.I)
    if blocked_ip:
        details.append(f"IP {blocked_ip.group(1)}")
    if challenge_id:
        details.append(f"challenge {challenge_id.group(1)}")
    suffix = f" ({', '.join(details)})" if details else ""
    status = status_code if status_code is not None else "unknown"
    return WbAntibotError(
        f"WB anti-bot protection blocked the request: HTTP {status}{suffix}",
        status_code=status_code,
        retry_after_seconds=parse_challenge_retry_seconds(body),
    )


def _default_state() -> dict:
    return {
        "state": "unknown",
        "consecutive_failures": 0,
        "retry_at": 0.0,
        "last_status": None,
        "last_error": "",
        "last_checked_at": 0.0,
        "last_success_at": 0.0,
        "source": "",
    }


def _paths(scope: str | None = None) -> tuple[Path, Path]:
    """Return isolated state files for a WB consumer.

    The legacy/default scope is intentionally unchanged so position parsing and
    the Telegram bot keep their existing cooldown.  Independent consumers such
    as the cart-stock worker must not inherit that cooldown: WB can reject one
    internal endpoint while another one remains available.
    """
    if not scope:
        return _STATE_PATH, _LOCK_PATH
    safe_scope = re.sub(r"[^a-zA-Z0-9_-]+", "_", scope).strip("_")
    if not safe_scope:
        raise ValueError("WB health scope must contain letters or digits")
    return (
        _STATE_PATH.with_name(f"{_STATE_PATH.stem}_{safe_scope}{_STATE_PATH.suffix}"),
        _LOCK_PATH.with_name(f"{_LOCK_PATH.stem}_{safe_scope}{_LOCK_PATH.suffix}"),
    )


def _load_unlocked(state_path: Path) -> dict:
    try:
        saved = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        saved = {}
    return {**_default_state(), **saved}


def _save_unlocked(state: dict, state_path: Path):
    temporary = state_path.with_name(f"{state_path.name}.tmp.{os.getpid()}")
    temporary.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    os.replace(temporary, state_path)


def get_access_health(scope: str | None = None) -> dict:
    state_path, lock_path = _paths(scope)
    with lock_path.open("a+") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_SH)
        return _load_unlocked(state_path)


def probe_delay_seconds(state: dict | None = None, now: float | None = None) -> int:
    current = state or get_access_health()
    current_time = time.time() if now is None else now
    return max(0, int(float(current.get("retry_at") or 0) - current_time))


def can_probe_wb(state: dict | None = None, now: float | None = None) -> bool:
    return probe_delay_seconds(state, now) <= 0


def _record_failure(
    state_name: str,
    status_code: int | None,
    error: str,
    source: str,
    cooldown_seconds: int,
    *,
    escalating: bool = False,
    scope: str | None = None,
) -> dict:
    now = time.time()
    state_path, lock_path = _paths(scope)
    with lock_path.open("a+") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        state = _load_unlocked(state_path)
        previous_failures = int(state.get("consecutive_failures") or 0)
        failures = previous_failures + 1 if state.get("state") == state_name else 1
        if escalating:
            cooldown_seconds = min(
                cooldown_seconds * (2 ** (failures - 1)),
                ANTIBOT_MAX_COOLDOWN_SECONDS,
            )
        state.update({
            "state": state_name,
            "consecutive_failures": failures,
            "retry_at": now + cooldown_seconds,
            "last_status": status_code,
            "last_error": error[:1000],
            "last_checked_at": now,
            "source": source,
        })
        _save_unlocked(state, state_path)
        return state


def record_antibot(
    status_code: int | None,
    error: str,
    source: str,
    *,
    minimum_cooldown_seconds: int = 0,
    scope: str | None = None,
) -> dict:
    return _record_failure(
        "antibot",
        status_code,
        error,
        source,
        max(ANTIBOT_BASE_COOLDOWN_SECONDS, minimum_cooldown_seconds),
        escalating=True,
        scope=scope,
    )


def record_rate_limit(
    status_code: int | None,
    error: str,
    source: str,
    *,
    scope: str | None = None,
) -> dict:
    return _record_failure(
        "rate_limited",
        status_code,
        error,
        source,
        RATE_LIMIT_COOLDOWN_SECONDS,
        scope=scope,
    )


def record_network_error(
    status_code: int | None,
    error: str,
    source: str,
    *,
    scope: str | None = None,
) -> dict:
    return _record_failure(
        "network_error",
        status_code,
        error,
        source,
        NETWORK_ERROR_COOLDOWN_SECONDS,
        scope=scope,
    )


def record_auth_expired(
    status_code: int | None,
    error: str,
    source: str,
    *,
    scope: str | None = None,
) -> dict:
    return _record_failure(
        "auth_expired",
        status_code,
        error,
        source,
        AUTH_EXPIRED_RECHECK_SECONDS,
        scope=scope,
    )


def record_success(source: str, *, scope: str | None = None) -> dict:
    now = time.time()
    state_path, lock_path = _paths(scope)
    with lock_path.open("a+") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        state = _load_unlocked(state_path)
        state.update({
            "state": "healthy",
            "consecutive_failures": 0,
            "retry_at": 0.0,
            "last_status": 200,
            "last_error": "",
            "last_checked_at": now,
            "last_success_at": now,
            "source": source,
        })
        _save_unlocked(state, state_path)
        return state
