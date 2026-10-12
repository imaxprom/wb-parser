"""Real Chromium DOM/redirect tests; every HTTP request is fulfilled locally."""
import asyncio
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, patch

from playwright.sync_api import sync_playwright

import bot
import config
import wb_sessions as pool


class WbLoginCompletionBrowserTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.playwright = sync_playwright().start()
        if not Path(cls.playwright.chromium.executable_path).exists():
            cls.playwright.stop()
            raise unittest.SkipTest("Installed Chromium is required for browser integration tests")
        cls.browser = cls.playwright.chromium.launch(headless=True, args=["--no-sandbox"])

    @classmethod
    def tearDownClass(cls):
        cls.browser.close()
        cls.playwright.stop()

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        patcher = patch.object(config, "DATA_DIR", temporary.name)
        patcher.start(); self.addCleanup(patcher.stop)
        pool.initialize()
        self.active = pool.create()
        pool.publish(self.active, {"session": {"cookies": {"x_wbaas_token": "old"},
            "localStorage": {"wbx__tokenData": "old", "wbx__sessionID": "old"}}, "browser": {}, "login": {}})
        self.before = pool.snapshot()
        self.reserve = pool.create()
        pool.bind_phone(self.reserve, "79991234567")
        self.job = bot.WbSessionJob(phone="79991234567", chat_id=1, session_id=self.reserve)
        token = bot._wb_login_target.set(self.job)
        self.addCleanup(bot._wb_login_target.reset, token)
        self.context = self.browser.new_context()
        self.addCleanup(self.context.close)
        self.page = self.context.new_page()
        self.requests = []
        self.mode = "consent"
        self.context.route("**/*", self.route)

    def route(self, route):
        url = route.request.url
        self.requests.append(url)
        storefront = """<h1>Вход завершён</h1><p>Ошибка загрузки рекламного блока</p>
        <script>
        localStorage.setItem('wbx__tokenData', JSON.stringify({token: 'test-fresh-token'}));
        localStorage.setItem('wbx__sessionID', 'new-device');
        localStorage.setItem('user-info-test', JSON.stringify({phone: '79991234567'}));
        document.cookie = 'x_wbaas_token=test; path=/';
        document.cookie = '_wbauid=test; path=/';
        setTimeout(() => localStorage.setItem('session-pow-token', 'test-pow'), 200);
        </script>"""
        if url == "https://www.wildberries.ru/lk":
            body = storefront
        elif url == "https://www.wildberries.ru/":
            body = "<h1>Войти</h1>"  # Premature navigation loses the OAuth flow.
        elif self.mode == "wbid_tokens":
            body = storefront.replace("Вход завершён", "Ожидается подтверждение WB.ID")
        else:
            body = """<input data-test-id="field_code_input" inputmode="numeric" maxlength="6">
            <script>
            function consent() {
              document.body.innerHTML = '<h1>Разрешить доступ Wildberries</h1><button id="accept">Принять</button>';
              document.querySelector('#accept').onclick = () => location.href = 'https://www.wildberries.ru/lk';
            }
            document.querySelector('input').oninput = (event) => {
              if (event.target.value.length !== 6) return;
              MODE_ACTION
            };
            </script>"""
            action = {
                "consent": "setTimeout(consent, 150);",
                "account": "document.body.innerHTML = '<button class=account-select-button>Подтверждённый аккаунт</button>'; document.querySelector('button').onclick = consent;",
                "rejected": "document.body.innerHTML = '<p>Неверный код</p>';",
            }[self.mode]
            body = body.replace("MODE_ACTION", action)
        route.fulfill(status=200, content_type="text/html; charset=utf-8", body=body)

    def enter_code(self):
        self.page.goto("https://id.wb.ru/confirm")
        self.page.locator("input").fill("123456")

    def finish(self, timeout=3):
        with patch.object(pool, "probe", return_value={"state": "healthy", "status": 200, "products": 1}), \
             patch.object(bot, "_save_wb_login_debug"):
            return bot._complete_wb_login(self.context, self.page, self.job, Mock(), timeout=timeout, poll_ms=25)

    def assert_reserved_login_published(self):
        self.assertEqual(pool.snapshot(), self.before)
        self.assertIsNotNone(pool.snapshot(self.reserve)["revision"])
        self.assertEqual(pool.snapshot(self.reserve)["phone"], "79991234567")
        self.assertNotIn("https://www.wildberries.ru/", self.requests)

    def test_code_then_delayed_consent_redirect_and_delayed_tokens(self):
        self.enter_code()
        self.finish()
        self.assert_reserved_login_published()
        self.assertEqual(self.page.url, "https://www.wildberries.ru/lk")

    def test_code_then_account_chooser_then_consent(self):
        self.mode = "account"
        self.enter_code()
        self.finish()
        self.assert_reserved_login_published()

    def test_explicit_wrong_code_does_not_publish_or_replace_active_login(self):
        self.mode = "rejected"
        self.enter_code()
        with self.assertRaises(bot.WbLoginError) as raised:
            self.finish()
        self.assertEqual(raised.exception.reason, "code_rejected")
        self.assertEqual(pool.snapshot(), self.before)
        self.assertIsNone(pool.snapshot(self.reserve)["revision"])

    def test_tokens_on_wbid_origin_cannot_be_published_as_storefront_login(self):
        self.mode = "wbid_tokens"
        self.page.goto("https://id.wb.ru/confirm")
        with self.assertRaises(bot.WbLoginError) as raised:
            self.finish(timeout=.2)
        self.assertEqual(raised.exception.reason, "completion_timeout")
        self.assertEqual(pool.snapshot(), self.before)
        self.assertIsNone(pool.snapshot(self.reserve)["revision"])

    def test_cancel_during_confirmation_does_not_publish(self):
        self.enter_code()
        self.job.cancel_event.set()
        with self.assertRaises(bot.WbLoginError) as raised:
            self.finish()
        self.assertEqual(raised.exception.reason, "cancelled")
        self.assertEqual(pool.snapshot(), self.before)
        self.assertIsNone(pool.snapshot(self.reserve)["revision"])


class WbLoginFailureMessageTest(unittest.TestCase):
    def test_timeout_is_not_reported_as_wrong_code_or_bare_runtime_error(self):
        job = bot.WbSessionJob(phone="79991234567", chat_id=1)
        with patch.object(bot, "_run_wb_session_login_sync", side_effect=bot.WbLoginError("completion_timeout")), \
             patch.object(bot.bot, "send_message", new_callable=AsyncMock) as send:
            asyncio.run(bot._run_wb_session_job(1, job))
        message = send.call_args.args[1]
        self.assertIn("не завершил авторизацию", message)
        self.assertNotIn("RuntimeError", message)
        self.assertNotIn(job.phone, message)


if __name__ == "__main__":
    unittest.main()
