"""WB Auth via Playwright on VPS — v3. Handle custom login form."""
import json, time, sys
from playwright.sync_api import sync_playwright

PHONE = "9607474717"  # without country code, WB adds +7

print("Starting WB auth v3...")

with sync_playwright() as p:
    browser = p.chromium.launch(
        headless=True,
        args=["--disable-blink-features=AutomationControlled"]
    )
    ctx = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
        viewport={"width": 1920, "height": 1080}
    )
    ctx.add_init_script('Object.defineProperty(navigator, "webdriver", {get: () => undefined});')
    page = ctx.new_page()

    # Step 1: Go to login page
    print("[1] Opening login page...")
    page.goto("https://www.wildberries.ru/security/login", timeout=30000)
    page.wait_for_timeout(12000)

    # Remove overlays
    page.evaluate('document.querySelectorAll("[class*=overlay], [class*=modal], [class*=onboarding]").forEach(e => e.remove())')
    page.wait_for_timeout(1000)

    print(f"    URL: {page.url}")

    # Step 2: Find and interact with phone input
    print(f"\n[2] Entering phone: {PHONE}")

    # Try clicking on the phone input area to activate it
    phone_field = page.query_selector('input[name="phoneNumber"][class*="input--BeCbN"]')
    if phone_field:
        # Force visibility and interact
        page.evaluate('''
            var inp = document.querySelector('input[name="phoneNumber"][class*="input--BeCbN"]');
            if (inp) {
                inp.style.display = "block";
                inp.style.visibility = "visible";
                inp.style.opacity = "1";
                inp.focus();
            }
        ''')
        page.wait_for_timeout(500)
        phone_field.fill(PHONE)
        page.wait_for_timeout(1000)
        print("    Phone entered via direct input")
    else:
        # Try clicking the form area and using keyboard
        print("    Trying click-and-type approach...")
        # Find the login form container
        form_area = page.query_selector('[class*="login"] [class*="phone"]') or page.query_selector('[class*="Phone"]') or page.query_selector('[class*="login-form"]')
        if form_area:
            form_area.click()
            page.wait_for_timeout(500)
        # Type using keyboard
        page.keyboard.type(PHONE, delay=100)
        page.wait_for_timeout(1000)
        print("    Phone typed via keyboard")

    page.screenshot(path="/tmp/wb_auth3_phone.png")

    # Step 3: Submit - request code
    print("\n[3] Requesting SMS code...")
    # Try button with "Получить код" or similar
    btn = None
    for selector in ['button:has-text("Получить")', 'button:has-text("код")', 'button:has-text("Войти")',
                     'button:has-text("Продолжить")', 'button[type="submit"]', '[class*="login"] button']:
        try:
            el = page.query_selector(selector)
            if el and el.is_visible():
                btn = el
                print(f"    Found button: '{el.inner_text().strip()}'")
                break
        except:
            continue

    if not btn:
        # List all visible buttons
        btns = page.query_selector_all("button")
        print("    All buttons:")
        for b in btns:
            try:
                vis = b.is_visible()
                print(f"      '{b.inner_text().strip()[:50]}' visible={vis}")
                if vis and b.inner_text().strip():
                    btn = b
            except:
                pass

    if btn:
        btn.click()
        page.wait_for_timeout(5000)
        print(f"    Clicked! URL: {page.url}")
    else:
        print("    No button found, trying Enter key...")
        page.keyboard.press("Enter")
        page.wait_for_timeout(5000)

    page.screenshot(path="/tmp/wb_auth3_after_submit.png")

    # Check if we need SMS code now
    print(f"\n[4] Current state:")
    print(f"    URL: {page.url}")
    # Check page content
    content = page.content()
    if "код" in content.lower() or "code" in content.lower() or "sms" in content.lower():
        print("    SMS code page detected!")
    else:
        print("    Page content (first 500 chars):")
        text = page.inner_text("body")[:500]
        print(f"    {text}")

    # Wait for SMS code
    print("\n" + "=" * 50)
    print("Waiting for SMS code in /tmp/wb_sms_code ...")
    print("=" * 50)
    sys.stdout.flush()

    with open("/tmp/wb_auth_waiting", "w") as f:
        f.write("waiting_for_sms")

    for i in range(180):  # 3 minutes
        try:
            with open("/tmp/wb_sms_code") as f:
                sms_code = f.read().strip()
            if sms_code:
                break
        except FileNotFoundError:
            pass
        time.sleep(1)
    else:
        print("TIMEOUT")
        browser.close()
        sys.exit(1)

    print(f"\n[5] Entering SMS code: {sms_code}")

    # Find code inputs
    code_inputs = page.query_selector_all('input[inputmode="numeric"]')
    if not code_inputs:
        code_inputs = page.query_selector_all('input[type="tel"]')
    if not code_inputs:
        code_inputs = page.query_selector_all('input[type="number"]')

    print(f"    Found {len(code_inputs)} code inputs")

    if len(code_inputs) >= 4:
        for i, ch in enumerate(sms_code):
            if i < len(code_inputs):
                code_inputs[i].fill(ch)
                page.wait_for_timeout(100)
    elif len(code_inputs) == 1:
        code_inputs[0].fill(sms_code)
    else:
        # Fallback: type via keyboard
        page.keyboard.type(sms_code, delay=150)

    page.wait_for_timeout(10000)
    print(f"    URL after code: {page.url}")
    page.screenshot(path="/tmp/wb_auth3_after_code.png")

    # Navigate to main page to ensure all tokens are loaded
    page.goto("https://www.wildberries.ru/", timeout=30000)
    page.wait_for_timeout(10000)

    # Step 6: Extract everything
    cookies_list = ctx.cookies()
    cookies = {c["name"]: c["value"] for c in cookies_list}
    ls_data = page.evaluate("""() => {
        var d = {};
        for (var i = 0; i < localStorage.length; i++) {
            var k = localStorage.key(i);
            d[k] = localStorage.getItem(k);
        }
        return d;
    }""")

    print("\n" + "=" * 50)
    print("AUTH RESULT")
    print("=" * 50)
    sys_auth = ls_data.get("_sys_auth", "NOT FOUND")
    has_bearer = bool(ls_data.get("wbx__tokenData"))
    has_pow = bool(ls_data.get("session-pow-token"))
    has_wbaas = bool(cookies.get("x_wbaas_token"))
    wbauid = cookies.get("_wbauid", "NOT FOUND")

    print(f"_sys_auth: {sys_auth}")
    print(f"Bearer: {'YES' if has_bearer else 'NO'}")
    print(f"PoW: {'YES' if has_pow else 'NO'}")
    print(f"wbaas cookie: {'YES' if has_wbaas else 'NO'}")
    print(f"_wbauid: {wbauid}")

    if has_bearer:
        td = json.loads(ls_data["wbx__tokenData"])
        print(f"Bearer length: {len(td.get('token', ''))}")

    if sys_auth and sys_auth != "unauth" and sys_auth != "NOT FOUND":
        print("\n*** AUTH SUCCESS ***")
        session_data = {
            "cookies": cookies,
            "cookies_full": [dict(c) for c in cookies_list],
            "localStorage": ls_data,
            "saved_at": time.time(),
        }
        with open("data/wb_session.json", "w") as f:
            json.dump(session_data, f, indent=2)
        ctx.storage_state(path="data/wb_playwright_state.json")
        print("Session saved to data/wb_session.json")
        print("Playwright state saved to data/wb_playwright_state.json")
    else:
        print("\n*** AUTH FAILED ***")

    browser.close()

import os
try:
    os.unlink("/tmp/wb_auth_waiting")
    os.unlink("/tmp/wb_sms_code")
except:
    pass
