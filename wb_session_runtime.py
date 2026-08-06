"""Shared, locked refresh of the saved WB buyer browser session."""

import fcntl
import json
import os
import re
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

import config
from wb_health import (
    WB_BROWSER_USER_AGENT,
    WbAuthExpiredError,
    build_antibot_error,
    is_antibot_response,
)


USER_AGENT = WB_BROWSER_USER_AGENT


def _atomic_json_dump(path: Path, data):
    temporary = path.with_name(f"{path.name}.tmp.{os.getpid()}")
    temporary.write_text(
        json.dumps(data, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _safe_body_text(page, limit: int = 1600) -> str:
    try:
        return re.sub(r"\s+", " ", page.inner_text("body"))[:limit]
    except Exception:
        return ""


def _save_session(ctx, page) -> dict:
    cookies_list = ctx.cookies()
    cookies = {cookie["name"]: cookie["value"] for cookie in cookies_list}
    local_storage = page.evaluate(
        """() => {
            const data = {};
            for (let index = 0; index < localStorage.length; index += 1) {
                const key = localStorage.key(index);
                data[key] = localStorage.getItem(key);
            }
            return data;
        }"""
    )

    checks = {
        "sys_auth": local_storage.get("_sys_auth", ""),
        "bearer": bool(local_storage.get("wbx__tokenData")),
        "pow": bool(local_storage.get("session-pow-token")),
        "wbaas": bool(cookies.get("x_wbaas_token")),
        "wbauid": bool(cookies.get("_wbauid")),
    }
    if not all((checks["bearer"], checks["pow"], checks["wbaas"], checks["wbauid"])):
        raise RuntimeError(f"WB session refresh returned an incomplete session: {checks}")

    data_dir = Path(config.DATA_DIR)
    session_data = {
        "cookies": cookies,
        "cookies_full": [dict(cookie) for cookie in cookies_list],
        "localStorage": local_storage,
        "saved_at": time.time(),
    }
    _atomic_json_dump(data_dir / "wb_session.json", session_data)

    state_path = data_dir / "wb_playwright_state.json"
    temporary_state = state_path.with_name(f"{state_path.name}.tmp.{os.getpid()}")
    ctx.storage_state(path=str(temporary_state))
    os.replace(temporary_state, state_path)

    cache_path = data_dir / "wbaas_proxy_tokens.json"
    try:
        cache = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        cache = {}
    cache["__direct__"] = {
        "token": cookies["x_wbaas_token"],
        "updated_at": time.time(),
    }
    _atomic_json_dump(cache_path, cache)
    return checks


def refresh_saved_session() -> dict:
    """Refresh WB cookies from storage state, with a cross-process file lock."""
    data_dir = Path(config.DATA_DIR)
    state_path = data_dir / "wb_playwright_state.json"
    if not state_path.exists():
        raise RuntimeError("Saved WB Playwright state is missing; manual login is required")

    lock_path = data_dir / "wb_session_refresh.lock"
    with lock_path.open("a+") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled"],
            )
            try:
                context = browser.new_context(
                    storage_state=str(state_path),
                    user_agent=USER_AGENT,
                    viewport={"width": 1920, "height": 1080},
                    locale="ru-RU",
                    timezone_id="Europe/Moscow",
                )
                context.add_init_script(
                    'Object.defineProperty(navigator, "webdriver", {get: () => undefined});'
                )
                page = context.new_page()
                home_response = page.goto(
                    "https://www.wildberries.ru/",
                    timeout=30000,
                    wait_until="domcontentloaded",
                )
                page.wait_for_timeout(3000)
                home_body = _safe_body_text(page)
                home_status = home_response.status if home_response else None
                if is_antibot_response(home_status, home_body):
                    raise build_antibot_error(home_status, home_body)

                lk_response = page.goto(
                    "https://www.wildberries.ru/lk",
                    timeout=30000,
                    wait_until="domcontentloaded",
                )
                page.wait_for_timeout(5000)

                body = _safe_body_text(page)
                lk_status = lk_response.status if lk_response else None
                if is_antibot_response(lk_status, body):
                    raise build_antibot_error(lk_status, body)

                local_storage = page.evaluate(
                    """() => ({
                        sysAuth: localStorage.getItem("_sys_auth") || "",
                        bearer: !!localStorage.getItem("wbx__tokenData")
                    })"""
                )
                login_url = "/security/login" in page.url or "id.wb.ru" in page.url
                logged_out = (
                    login_url
                    or not local_storage.get("bearer")
                    or local_storage.get("sysAuth") in ("", "unauth")
                )
                if logged_out:
                    raise WbAuthExpiredError(
                        "Saved WB session no longer opens the buyer account; manual SMS login is required"
                    )
                return _save_session(context, page)
            finally:
                browser.close()
