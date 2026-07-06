#!/usr/bin/env python3
"""Manual WB auth in a visible local browser.

Run this on a trusted desktop, complete WB ID login in the opened browser,
then the script saves data/wb_session.json, data/wb_playwright_state.json,
and data/wbaas_proxy_tokens.json.
"""

import argparse
import json
import os
import time
from pathlib import Path

from curl_cffi import requests as curl_requests
from playwright.sync_api import sync_playwright

import config
import proxy_positions


UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/141.0.0.0 Safari/537.36"
)


def atomic_json_dump(path: Path, data: dict):
    tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


def read_www_auth(ctx, page) -> tuple[dict, dict, dict]:
    page.goto("https://www.wildberries.ru/", timeout=30000, wait_until="domcontentloaded")
    page.wait_for_timeout(5000)
    cookies_list = ctx.cookies()
    cookies = {c["name"]: c["value"] for c in cookies_list}
    local_storage = page.evaluate(
        """() => {
            const d = {};
            for (let i = 0; i < localStorage.length; i++) {
                const k = localStorage.key(i);
                d[k] = localStorage.getItem(k);
            }
            return d;
        }"""
    )
    checks = {
        "sys_auth": local_storage.get("_sys_auth", ""),
        "bearer": bool(local_storage.get("wbx__tokenData")),
        "pow": bool(local_storage.get("session-pow-token")),
        "wbaas": bool(cookies.get("x_wbaas_token")),
        "wbauid": bool(cookies.get("_wbauid")),
    }
    return cookies, local_storage, checks


def verify_search() -> tuple[int, int]:
    proxy_positions._load_token_cache()
    proxy_positions._load_wb_session()
    headers = proxy_positions._build_headers("__direct__", with_bearer=True)
    params = {
        "ab_testing": "false",
        "appType": "1",
        "curr": "rub",
        "dest": config.WB_DEST,
        "hide_dflags": "131072",
        "hide_dtype": "10;14",
        "inheritFilters": "false",
        "lang": "ru",
        "query": "трусы женские",
        "resultset": "catalog",
        "sort": "popular",
        "spp": "31",
        "suppressSpellcheck": "false",
        "limit": "300",
        "page": "1",
    }
    resp = curl_requests.get(
        proxy_positions.SEARCH_URL,
        params=params,
        headers=headers,
        impersonate="chrome",
        timeout=15,
    )
    if resp.status_code != 200:
        return resp.status_code, 0
    return resp.status_code, len(resp.json().get("products", []))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout-minutes", type=int, default=15)
    parser.add_argument(
        "--profile-dir",
        default=str(Path(config.DATA_DIR) / "wb_manual_auth_profile"),
        help="Persistent local browser profile directory.",
    )
    parser.add_argument("--headless", action="store_true")
    args = parser.parse_args()

    data_dir = Path(config.DATA_DIR)
    data_dir.mkdir(exist_ok=True)
    profile_dir = Path(args.profile_dir)
    profile_dir.mkdir(parents=True, exist_ok=True)

    print("Opening visible browser for WB login.")
    print("Complete login in that browser. I will detect auth tokens automatically.")
    print(f"Profile: {profile_dir}")

    deadline = time.time() + args.timeout_minutes * 60
    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            str(profile_dir),
            headless=args.headless,
            args=["--disable-blink-features=AutomationControlled"],
            user_agent=UA,
            viewport={"width": 1440, "height": 1000},
            locale="ru-RU",
            timezone_id="Europe/Moscow",
        )
        ctx.add_init_script(
            'Object.defineProperty(navigator, "webdriver", {get: () => undefined});'
        )
        login_page = ctx.pages[0] if ctx.pages else ctx.new_page()
        login_page.goto("https://www.wildberries.ru/security/login", timeout=30000)
        check_page = ctx.new_page()

        last_checks = {}
        while time.time() < deadline:
            try:
                cookies, ls_data, checks = read_www_auth(ctx, check_page)
                last_checks = checks
                print(f"Auth check: {checks}")
                if checks["bearer"] and checks["pow"] and checks["wbaas"]:
                    session_data = {
                        "cookies": cookies,
                        "cookies_full": [dict(c) for c in ctx.cookies()],
                        "localStorage": ls_data,
                        "saved_at": time.time(),
                    }
                    atomic_json_dump(data_dir / "wb_session.json", session_data)
                    ctx.storage_state(path=str(data_dir / "wb_playwright_state.json"))

                    cache_path = data_dir / "wbaas_proxy_tokens.json"
                    try:
                        wbaas_cache = json.loads(cache_path.read_text(encoding="utf-8"))
                    except Exception:
                        wbaas_cache = {}
                    wbaas_cache["__direct__"] = {
                        "token": cookies["x_wbaas_token"],
                        "updated_at": time.time(),
                    }
                    atomic_json_dump(cache_path, wbaas_cache)

                    print("Saved WB session files.")
                    status_code, products_count = verify_search()
                    print(f"Search check: HTTP {status_code}, products={products_count}")
                    if status_code != 200 or products_count <= 0:
                        raise SystemExit("Saved session, but WB search check is not healthy.")
                    print("AUTH_OK")
                    return
            except Exception as e:
                print(f"Auth check error: {type(e).__name__}: {e}")

            time.sleep(8)

        raise SystemExit(f"Timed out waiting for WB auth tokens. Last checks: {last_checks}")


if __name__ == "__main__":
    main()
