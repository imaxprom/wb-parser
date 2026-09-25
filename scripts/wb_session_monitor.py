"""Bounded, restartable WB session observation; logs contain no credentials.

Run on the production host with its saved buyer login. No Telegram messages,
SMS requests, account changes or cart mutations are made by this program.
"""

import argparse
import base64
from collections import Counter
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config
import proxy_positions as positions
from wb_health import is_antibot_response

QUERY = "трусы женские"
PARAMS = {
    "ab_testing": "false", "appType": "1", "curr": "rub",
    "dest": config.WB_DEST, "hide_dflags": "131072", "hide_dtype": "10;14",
    "inheritFilters": "false", "lang": "ru", "query": QUERY,
    "resultset": "catalog", "sort": "popular", "spp": "31",
    "suppressSpellcheck": "false", "limit": "300", "page": "1",
}


def atomic_json(path, value):
    temporary = path.with_suffix(f".tmp.{os.getpid()}")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2))
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def read_json(path, default=None):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {} if default is None else default


def session_metadata():
    data = read_json(Path(config.DATA_DIR) / "wb_session.json")
    storage = data.get("localStorage", {})
    saved = data.get("saved_at")
    result = {"saved_at": saved, "age_seconds": time.time() - saved if saved else None}
    # Only opaque fingerprints and expiry timestamps leave the credential file.
    for label, raw in (
        ("bearer", storage.get("wbx__tokenData", "")),
        ("pow", storage.get("session-pow-token", "")),
        ("wbaas", data.get("cookies", {}).get("x_wbaas_token", "")),
    ):
        try:
            item = json.loads(raw) if label != "wbaas" else {}
        except (ValueError, TypeError):
            item = {}
        token = item.get("token", "") if label != "wbaas" else raw
        result[label + "_fingerprint"] = hashlib.sha256(token.encode()).hexdigest()[:16] if token else None
        if label == "pow":
            expiry = item.get("expiresAt")
            result["pow_expires_at"] = expiry if isinstance(expiry, (int, float)) else None
        if label == "bearer":
            try:
                payload = json.loads(base64.urlsafe_b64decode(token.split(".")[1] + "==="))
                result["bearer_expires_at"] = payload.get("exp")
                result["bearer_issued_at"] = payload.get("iat")
            except (ValueError, IndexError, TypeError):
                pass
    return result


def full_probe(sku=0):
    """Exercise the real four-page parser, recording only response metadata."""
    started = time.monotonic()
    responses = []
    with positions.curl_requests.Session(impersonate=positions.WB_CURL_IMPERSONATE) as client:
        class ObservedClient:
            def get(self, *args, **kwargs):
                item = {"page": kwargs["params"].get("page"),
                        "variant": kwargs["params"].get("ab_testid", "normal")}
                responses.append(item)
                response = client.get(*args, **kwargs)
                item["status"] = response.status_code
                response_headers = getattr(response, "headers", {})
                retry = response_headers.get("Retry-After", "")
                item["retry_after"] = int(retry) if retry.isdigit() else 0
                body = getattr(response, "text", "")
                item["antibot"] = is_antibot_response(response.status_code, body[:2000])
                # Values of Set-Cookie, auth headers and response bodies must
                # never enter the observation log; names/flags are sufficient.
                item["cookie_names_received"] = sorted(getattr(response, "cookies", {}).keys())
                item["content_type"] = response_headers.get("Content-Type", "")
                item["body_length"] = len(body)
                if response.status_code != 200:
                    item["body_markers"] = [word for word in ("captcha", "token", "expired", "invalid", "limit", "подозрительная", "токен", "лимит") if word in body.lower()]
                    item["rate_limit_headers"] = {key: int(response_headers[key]) for key in ("X-Ratelimit-Limit", "X-Ratelimit-Reset", "X-Ratelimit-Retry") if str(response_headers.get(key, "")).isdigit()}
                try:
                    item["products"] = len(response.json().get("products", []))
                except (ValueError, AttributeError, TypeError):
                    pass
                return response
        parsed = positions._fetch_keyword_sync("", QUERY, sku, int(config.WB_DEST), ObservedClient())
    state = (parsed.get("error_state") or "invalid_response") if parsed.get("error") else "healthy"
    if any(r.get("antibot") for r in responses):
        state = "antibot"
    return {
        "state": state,
        "status": parsed.get("status_code") if parsed.get("error") else 200,
        "retry_after": max((r.get("retry_after", 0) for r in responses), default=0),
        "mode": "full_parser", "responses": responses,
        "promo_pos": parsed.get("promo_pos"), "organic_pos": parsed.get("organic_pos"),
        "elapsed_ms": round((time.monotonic() - started) * 1000),
    }


def probe(*, reload=True, full=False, sku=0):
    if reload:
        positions._load_token_cache()
        positions._load_wb_session()
    started = time.monotonic()
    status = None
    try:
        if full:
            return full_probe(sku)
        response = positions.curl_requests.get(
            positions.SEARCH_URL, params=PARAMS,
            headers=positions._build_headers("__direct__"),
            impersonate=positions.WB_CURL_IMPERSONATE, timeout=15,
        )
        status = response.status_code
        retry_after = response.headers.get("Retry-After", "")
        retry_seconds = int(retry_after) if retry_after.isdigit() else 0
        if status == 429:
            state = "rate_limited"
        elif status == 401:
            state = "auth_expired"
        elif is_antibot_response(status, response.text[:2000]):
            state = "antibot"
        elif status != 200:
            state = "http_error"
        else:
            data = response.json()
            products = data.get("products", data.get("data", {}).get("products", []))
            if isinstance(products, list) and products and all(isinstance(p, dict) and "id" in p for p in products):
                return {"state": "healthy", "status": status, "products": len(products),
                        "elapsed_ms": round((time.monotonic() - started) * 1000)}
            state = "invalid_response"
        return {"state": state, "status": status, "products": 0,
                "retry_after": retry_seconds,
                "elapsed_ms": round((time.monotonic() - started) * 1000)}
    except Exception as error:
        return {"state": "network_error" if status is None else "invalid_response",
                "status": status, "error_type": type(error).__name__,
                "elapsed_ms": round((time.monotonic() - started) * 1000)}


def resume_login():
    """Resume an already authorized WB.ID account, validate before publishing."""
    # Reuse the same helpers as the proven interactive flow, without its SMS
    # or Telegram callbacks. Importing bot does not start polling/scheduling.
    import bot
    from playwright.sync_api import sync_playwright
    from wb_session_runtime import _save_session

    directory = Path(config.DATA_DIR)
    with (directory / "wb_session_refresh.lock").open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        state = bot._load_clean_wb_login_state(str(directory / "wb_login_working_state.json"))
        if not state:
            state = bot._load_clean_wb_login_state(str(directory / "wb_playwright_state.json"))
        if not state:
            return {"state": "login_required", "reason": "no_saved_wbid"}
        display_process, display = bot._start_wb_virtual_display()
        if not display:
            return {"state": "refresh_error", "reason": "no_display"}
        try:
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(
                    headless=False, env={**os.environ, "DISPLAY": display},
                    args=["--disable-blink-features=AutomationControlled"],
                )
                try:
                    ctx = browser.new_context(
                        storage_state=state, user_agent=positions.WB_BROWSER_USER_AGENT,
                        viewport={"width": 1920, "height": 1080},
                        locale="ru-RU", timezone_id="Europe/Moscow",
                    )
                    ctx.add_init_script('Object.defineProperty(navigator, "webdriver", {get: () => undefined});')
                    page = ctx.new_page()
                    page.goto("https://www.wildberries.ru/", timeout=30000, wait_until="domcontentloaded")
                    page.wait_for_timeout(3000)
                    page.goto("https://www.wildberries.ru/security/login", timeout=30000, wait_until="domcontentloaded")
                    account_selected = consent_accepted = False
                    deadline = time.monotonic() + 75
                    while time.monotonic() < deadline:
                        has_tokens, _ = bot._wb_context_has_auth_tokens(ctx, page)
                        if has_tokens and "wildberries.ru" in page.url:
                            cookies = {c["name"]: c["value"] for c in ctx.cookies()}
                            storage = page.evaluate("() => Object.fromEntries(Object.entries(localStorage))")
                            positions._wb_session = {"cookies": cookies, "localStorage": storage}
                            positions._token_cache = {}
                            verification = probe(reload=False)
                            if verification["state"] != "healthy":
                                return {"state": "candidate_rejected", "verification": verification}
                            _save_session(ctx, page)
                            atomic_json(directory / "wb_login_working_state.json", ctx.storage_state())
                            return {"state": "refreshed", "verification": verification}
                        action = bot._wb_saved_login_action(page, account_selected=account_selected,
                                                           consent_accepted=consent_accepted)
                        if action:
                            name, button = action
                            button.click(timeout=10000)
                            account_selected |= name == "account"
                            consent_accepted |= name == "consent"
                        elif page.locator('input[name="phoneNumber"]:not([type="radio"]), input[data-test-id="field_phone_input"], input#wb-phone-number').first.is_visible():
                            return {"state": "login_required", "reason": "phone_or_sms_required"}
                        page.wait_for_timeout(2000)
                    return {"state": "refresh_blocked", "reason": "browser_did_not_resume"}
                finally:
                    browser.close()
        finally:
            bot._stop_wb_virtual_display(display_process)


def refresh_subprocess():
    child = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "--refresh"],
                             stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                             text=True, start_new_session=True)
    try:
        stdout, _ = child.communicate(timeout=160)
        return json.loads(stdout) if child.returncode == 0 else {"state": "refresh_error"}
    except subprocess.TimeoutExpired:
        os.killpg(child.pid, signal.SIGKILL)
        child.communicate()
        return {"state": "refresh_timeout"}
    except ValueError:
        return {"state": "refresh_error", "reason": "invalid_child_output"}


def delay_for(result, failures, interval):
    state = result["state"]
    if state == "login_required":
        return 6 * 3600
    if state == "rate_limited":
        return max(interval, 600, result.get("retry_after", 0))
    if state in ("antibot", "refresh_blocked", "candidate_rejected", "refresh_error", "refresh_timeout"):
        return max(interval, min(900 * 2 ** min(max(failures - 1, 0), 3), 7200), result.get("retry_after", 0))
    return interval


def emit(directory, event, **data):
    record = {"at": time.time(), "event": event, **data}
    line = json.dumps(record, ensure_ascii=False)
    with (directory / "events.jsonl").open("a") as output:
        output.write(line + "\n")
        output.flush()
    print(line, flush=True)


def production_load(since, until):
    """Count instrumented production requests without copying user log text."""
    try:
        result = subprocess.run(
            ["journalctl", "-u", "wb-parser.service", "--since", f"@{since:.3f}",
             "--until", f"@{until:.3f}", "--no-pager", "-o", "cat"],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode:
            return {"available": False}
        statuses = re.findall(r"WB search response: status=(\d+)", result.stdout)
        return {"available": True, "search_responses": len(statuses), "statuses": dict(Counter(statuses))}
    except (OSError, subprocess.TimeoutExpired):
        return {"available": False}


def report(directory, state):
    events = []
    for line in (directory / "events.jsonl").read_text().splitlines():
        try:
            events.append(json.loads(line))
        except ValueError:
            continue
    probes = [e for e in events if e["event"] in ("probe", "verification")]
    recoveries = [e for e in events if e["event"] == "refresh"]
    counts = Counter(e["result"]["state"] for e in probes)
    elapsed = min(time.time(), state["end_at"]) - state["start_at"]
    def stamp(t):
        from zoneinfo import ZoneInfo
        return datetime.fromtimestamp(t, ZoneInfo("Europe/Moscow")).isoformat(timespec="seconds")
    lines = ["# Наблюдение за WB-сессией", "",
             f"Начало: {stamp(state['start_at'])}. Плановое окончание: {stamp(state['end_at'])}.",
             f"Статус: {'завершено' if state.get('complete') else 'идёт наблюдение'}. Наблюдалось часов: {elapsed / 3600:.2f}.",
             f"Контроль: один замер каждые {state['interval']} секунд, с паузами при отказах.",
             "Используется сохранённый покупательский аккаунт на production, текущий поисковый endpoint и фиксированный запрос.",
             f"Ответы контрольного поиска: {dict(counts)}. Попыток обновления: {len(recoveries)}.",
             f"Контрольный SKU: {state.get('sku', 0)}. Режимы замеров: {dict(Counter(e['result'].get('mode', 'single_page') for e in probes))}. Полный цикл содержит до четырёх последовательных HTTP-запросов; переходы между этапами записаны в events.jsonl.",
             "", "## Хронология отказов и восстановления", ""]
    for event in events:
        if event["event"] == "refresh" or (event["event"] in ("probe", "verification") and event["result"]["state"] != "healthy"):
            lines.append(f"- {stamp(event['at'])}: {event['event']}: {json.dumps(event['result'], ensure_ascii=False)}; возраст сохранённой сессии: {event.get('session', {}).get('age_seconds')} сек.")
    lines += ["", "## Ограничения выводов", "",
              "В коде HTTP 498 классифицируется как антибот-отказ; сам по себе он не доказывает истечение Bearer или фиксированный срок жизни сессии.",
              "Время отказа находится между последним успешным и первым неуспешным замером. При cooldown этот интервал шире пяти минут.",
              "Первый цикл начинается с ранее созданной сессии; её возраст не равен длительности наблюдения.",
              "Обновление меняет несколько компонентов сразу. Совпадение восстановления с обновлением не определяет единственный виновный токен.",
              "Производственные поисковые ответы учитываются отдельно по журналу; браузерные фоновые запросы не посчитаны.",
              "Публичная документация API продавцов не устанавливает срок жизни этой покупательской web-сессии.", ""]
    lines += ["## Интервалы доступности", ""]
    last_success = None
    seen_failure = set()
    for event in probes:
        session = event.get("session", {})
        saved_at = session.get("saved_at")
        if not saved_at:
            continue
        if event["result"]["state"] == "healthy":
            last_success = event
        elif saved_at not in seen_failure:
            seen_failure.add(saved_at)
            lower = None
            if last_success and last_success.get("session", {}).get("saved_at") == saved_at:
                lower = round(last_success["at"] - saved_at)
            lines.append(f"- Сессия от {stamp(saved_at)}: последний успех на возрасте {lower} сек.; первый отказ на возрасте {round(event['at'] - saved_at)} сек. ({event['result']['state']}).")
    if not seen_failure:
        lines.append("Отказов контрольного поиска не обнаружено; верхняя граница срока работы сессии не установлена.")
    loads = [e["result"] for e in events if e["event"] == "production_load"]
    lines += ["", f"Ответов production-поиска за наблюдение: {sum(e.get('search_responses', 0) for e in loads)}. Интервалов с недоступным журналом: {sum(not e.get('available') for e in loads)}.", ""]
    temporary = directory / "report.tmp"
    temporary.write_text("\n".join(lines))
    os.replace(temporary, directory / "report.md")


def run(directory, hours, interval, full_after_hours=2, sku=0):
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (Path(config.DATA_DIR) / "wb_session_monitor.lock").open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        state_path = directory / "state.json"
        state = read_json(state_path)
        if not state:
            now = time.time()
            state = {"start_at": now, "end_at": now + hours * 3600, "interval": interval,
                     "next_at": now, "failures": 0, "recover_pending": False}
            atomic_json(state_path, state)
            emit(directory, "start", session=session_metadata(), interval=interval, end_at=state["end_at"])
        interval = state["interval"]
        state.setdefault("full_after_hours", full_after_hours)
        state.setdefault("sku", sku)
        while time.time() < state["end_at"]:
            if time.time() < state["next_at"]:
                time.sleep(max(0, min(5, state["next_at"] - time.time(), state["end_at"] - time.time())))
                continue
            observed_at = time.time()
            emit(directory, "production_load", result=production_load(state.get("load_until", state["start_at"]), observed_at))
            state["load_until"] = observed_at
            full = observed_at - state["start_at"] >= state["full_after_hours"] * 3600
            mode = "full_parser" if full else "single_page"
            if state.get("mode") != mode:
                emit(directory, "phase", mode=mode, sku=state["sku"])
                state["mode"] = mode
            result = probe(full=full, sku=state["sku"])
            metadata = session_metadata()
            emit(directory, "probe", result=result, session=metadata)
            # After a cooldown, test the old session first: distinguish natural
            # recovery from recovery caused by renewing the saved login.
            if result["state"] in ("antibot", "auth_expired") and (state["recover_pending"] or result["state"] == "auth_expired") and state["end_at"] - time.time() > 180:
                renewal = refresh_subprocess()
                emit(directory, "refresh", result=renewal, session=session_metadata(), previous_session=metadata)
                if renewal["state"] == "refreshed":
                    result = probe(full=full, sku=state["sku"])
                    emit(directory, "verification", result=result, session=session_metadata())
                else:
                    result = renewal
            if result["state"] == "healthy":
                state.update(failures=0, recover_pending=False, last_success_at=time.time())
            else:
                state["failures"] += 1
                state["recover_pending"] = result["state"] not in ("rate_limited", "network_error", "http_error", "invalid_response")
            state["next_at"] = time.time() + delay_for(result, state["failures"], interval)
            atomic_json(state_path, state)
            report(directory, state)
        state["complete"] = True
        atomic_json(state_path, state)
        emit(directory, "complete")
        report(directory, state)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hours", type=float, default=12)
    parser.add_argument("--interval", type=int, default=300)
    parser.add_argument("--full-after-hours", type=float, default=2)
    parser.add_argument("--sku", type=int, default=0)
    parser.add_argument("--output", type=Path, default=Path(config.DATA_DIR) / "session-observation")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    os.umask(0o077)
    logging.basicConfig(level=logging.ERROR, stream=sys.stderr)
    if args.refresh:
        try:
            result = resume_login()
        except Exception as error:
            result = {"state": "refresh_error", "error_type": type(error).__name__}
        print(json.dumps(result))
    elif args.once:
        print(json.dumps({"result": probe(), "session": session_metadata()}))
    else:
        if args.hours <= 0 or args.interval < 60 or args.full_after_hours < 0:
            parser.error("hours must be positive; interval must be at least 60 seconds")
        run(args.output, args.hours, args.interval, args.full_after_hours, args.sku)


if __name__ == "__main__":
    main()
