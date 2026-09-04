import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from aiogram.types import ReplyKeyboardMarkup

import wb_health
import bot
import proxy_positions
import queue_worker
from cart_stock_worker import CartStockWorker


class WbHealthTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        directory = Path(self.temporary.name)
        self.state_patch = patch.object(wb_health, "_STATE_PATH", directory / "health.json")
        self.lock_patch = patch.object(wb_health, "_LOCK_PATH", directory / "health.lock")
        self.state_patch.start()
        self.lock_patch.start()

    def tearDown(self):
        self.lock_patch.stop()
        self.state_patch.stop()
        self.temporary.cleanup()

    def test_challenge_page_is_antibot_even_with_http_200(self):
        body = "Что-то не так. Подозрительная активность. Новая попытка через 01:23"
        self.assertTrue(wb_health.is_antibot_response(200, body))
        self.assertEqual(wb_health.parse_challenge_retry_seconds(body), 83)

    def test_antibot_cooldown_escalates_and_success_resets_it(self):
        with patch.object(wb_health.time, "time", side_effect=(1000.0, 2000.0, 3000.0)):
            first = wb_health.record_antibot(498, "blocked", "test")
            second = wb_health.record_antibot(498, "blocked again", "test")
            healthy = wb_health.record_success("test")

        self.assertEqual(first["retry_at"] - first["last_checked_at"], 15 * 60)
        self.assertEqual(second["retry_at"] - second["last_checked_at"], 30 * 60)
        self.assertEqual(healthy["state"], "healthy")
        self.assertEqual(healthy["consecutive_failures"], 0)
        self.assertEqual(healthy["retry_at"], 0)

    def test_worker_does_not_refresh_auth_on_498(self):
        worker = CartStockWorker.__new__(CartStockWorker)
        worker._auth_headers = Mock(return_value=({"Authorization": "Bearer token"}, "token"))
        worker.refresh_auth = Mock()
        worker._browser_card_request = Mock(
            return_value=(498, "Подозрительная активность"),
        )

        with self.assertRaises(wb_health.WbAntibotError):
            worker._request_wb_batch(["123"])

        worker.refresh_auth.assert_not_called()
        self.assertEqual(wb_health.get_access_health("cart_stock")["state"], "antibot")
        self.assertEqual(wb_health.get_access_health()["state"], "unknown")

    def test_position_cooldown_does_not_pause_cart_stock_worker(self):
        wb_health.record_antibot(498, "search endpoint blocked", "bot_pre_parse")
        worker = CartStockWorker.__new__(CartStockWorker)
        worker.flush_outbox = Mock(return_value=True)
        worker.browser_proxy_available = Mock(return_value=True)
        worker.heartbeat = Mock()
        worker.claim = Mock(return_value=None)
        worker.last_cooldown_log_at = 0.0

        self.assertFalse(worker.process_once())

        worker.claim.assert_called_once_with()
        self.assertEqual(wb_health.get_access_health()["state"], "antibot")
        self.assertEqual(wb_health.get_access_health("cart_stock")["state"], "unknown")

    def test_cart_stock_cooldown_still_pauses_cart_stock_worker(self):
        wb_health.record_antibot(
            498,
            "card endpoint blocked",
            "cart_stock_worker",
            scope="cart_stock",
        )
        worker = CartStockWorker.__new__(CartStockWorker)
        worker.flush_outbox = Mock(return_value=True)
        worker.browser_proxy_available = Mock(return_value=True)
        worker.heartbeat = Mock()
        worker.claim = Mock(return_value=None)
        worker.last_cooldown_log_at = 0.0

        self.assertFalse(worker.process_once())

        worker.claim.assert_not_called()

    def test_unavailable_browser_tunnel_does_not_claim_a_job(self):
        worker = CartStockWorker.__new__(CartStockWorker)
        worker.flush_outbox = Mock(return_value=True)
        worker.browser_proxy_available = Mock(return_value=False)
        worker.heartbeat = Mock()
        worker.claim = Mock(return_value=None)

        self.assertFalse(worker.process_once())

        worker.heartbeat.assert_called_once_with()
        worker.claim.assert_not_called()

    def test_position_search_classifies_401_as_expired_auth(self):
        session = Mock()
        session.get.return_value = SimpleNamespace(status_code=401)

        data, error = proxy_positions._search_sync({}, {}, session=session)

        self.assertEqual(data, {})
        self.assertEqual(error["error_state"], "auth_expired")
        self.assertEqual(error["status_code"], 401)

    def test_login_resume_state_drops_antibot_cookies(self):
        path = Path(self.temporary.name) / "login-state.json"
        path.write_text(json.dumps({
            "cookies": [
                {"name": "wbid-refresh", "domain": ".id.wb.ru", "value": "keep"},
                {"name": "__zzatw-wb-buyer", "domain": ".id.wb.ru", "value": "drop"},
                {"name": "x_wbaas_token", "domain": "www.wildberries.ru", "value": "drop"},
            ],
            "origins": [
                {
                    "origin": "https://id.wb.ru",
                    "localStorage": [
                        {"name": "wbIdAccessToken", "value": "keep"},
                        {"name": "__zzatw-wb-buyer", "value": "drop"},
                    ],
                },
                {"origin": "https://www.wildberries.ru", "localStorage": []},
            ],
        }), encoding="utf-8")

        clean = bot._load_clean_wb_login_state(str(path))

        self.assertEqual([c["name"] for c in clean["cookies"]], ["wbid-refresh"])
        self.assertEqual(clean["origins"], [{
            "origin": "https://id.wb.ru",
            "localStorage": [{"name": "wbIdAccessToken", "value": "keep"}],
        }])

    def test_saved_login_accepts_oauth_consent_after_account_selection(self):
        consent_button = Mock()
        consent_button.is_visible.return_value = True
        consent_button.inner_text.return_value = "Принять"
        page = Mock()
        page.query_selector_all.return_value = [consent_button]

        action = bot._wb_saved_login_action(
            page,
            account_selected=True,
            consent_accepted=False,
        )

        self.assertEqual(action, ("consent", consent_button))


class BotWbHealthTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        directory = Path(self.temporary.name)
        self.state_patch = patch.object(wb_health, "_STATE_PATH", directory / "health.json")
        self.lock_patch = patch.object(wb_health, "_LOCK_PATH", directory / "health.lock")
        self.state_patch.start()
        self.lock_patch.start()

    def tearDown(self):
        self.lock_patch.stop()
        self.state_patch.stop()
        self.temporary.cleanup()

    async def test_position_queue_ignores_legacy_health_file(self):
        wb_health.record_antibot(498, "blocked", "test")
        queue = queue_worker.PositionQueue(pause=0)
        expected = {
            "query": {
                "promo_pos": 7,
                "organic_pos": None,
                "is_advertised": True,
                "error": False,
            }
        }
        get_positions = AsyncMock(return_value=expected)

        with patch.object(queue_worker._positions_module, "get_positions", get_positions):
            await queue.start()
            try:
                future = await queue.submit(1, 123, ["query"], label="123")
                result = await asyncio.wait_for(future, timeout=1)
            finally:
                await queue.stop()

        get_positions.assert_awaited_once_with(123, ["query"])
        self.assertEqual(result, expected)

    async def test_manual_button_does_not_run_a_preflight_check(self):
        callback = SimpleNamespace(
            from_user=SimpleNamespace(id=1),
            data="evirma_10",
            answer=AsyncMock(),
            message=SimpleNamespace(
                chat=SimpleNamespace(id=2),
                message_id=3,
                edit_text=AsyncMock(),
            ),
        )
        background_task = Mock()
        verification = Mock()
        parse_one = Mock(return_value=object())
        with (
            patch.object(bot.db, "get_article_by_id", return_value={"id": 10, "sku": "123"}),
            patch.object(bot.db, "get_queries", return_value=[{"id": 20, "query": "query"}]),
            patch.object(bot, "_do_evirma_one", parse_one),
            patch.object(bot.asyncio, "create_task", return_value=background_task),
            patch.object(bot, "_verify_current_wb_session_sync", verification),
        ):
            try:
                await bot.run_evirma_handler(callback)
            finally:
                bot._background_tasks.discard(background_task)

        callback.answer.assert_awaited_once_with()
        callback.message.edit_text.assert_awaited_once()
        verification.assert_not_called()

    async def test_search_menu_edits_the_existing_bot_message(self):
        callback = SimpleNamespace(
            from_user=SimpleNamespace(id=1),
            answer=AsyncMock(),
            message=SimpleNamespace(edit_text=AsyncMock()),
        )
        state = SimpleNamespace(clear=AsyncMock())
        articles = [{"id": 10, "sku": "123", "name": "Товар"}]
        with (
            patch.object(bot.db, "get_articles", return_value=articles),
            patch.object(bot.db, "get_queries", return_value=[{"id": 20, "query": "запрос"}]),
        ):
            await bot.show_search_menu(callback, state)

        state.clear.assert_awaited_once_with()
        callback.answer.assert_awaited_once_with()
        callback.message.edit_text.assert_awaited_once()
        _, kwargs = callback.message.edit_text.await_args
        callback_data = [
            button.callback_data
            for row in kwargs["reply_markup"].inline_keyboard
            for button in row
        ]
        self.assertIn("evirma_10", callback_data)
        self.assertNotIn("menu_main", callback_data)

    async def test_main_menu_stays_persistent_above_the_input(self):
        keyboard = bot.main_kb()

        self.assertIsInstance(keyboard, ReplyKeyboardMarkup)
        button_texts = [
            button.text
            for row in keyboard.keyboard
            for button in row
        ]
        self.assertEqual(
            button_texts,
            [
                "Поиск", "Полки", "Авто", "📈 Графики",
                "🌍 Гео-сканер", "⚙️ Настройки",
            ],
        )

    async def test_section_screen_is_sent_without_removing_the_main_keyboard(self):
        screen = SimpleNamespace()
        message = SimpleNamespace(answer=AsyncMock(return_value=screen))
        section_keyboard = bot.InlineKeyboardMarkup(inline_keyboard=[])

        result = await bot.answer_inline_screen(message, "Поиск", section_keyboard)

        self.assertIs(result, screen)
        message.answer.assert_awaited_once()

    async def test_empty_scheduler_does_not_contact_wb(self):
        submit = AsyncMock()
        verification = Mock()
        with (
            patch.object(bot.db, "get_allowed_users", return_value=[{"telegram_id": 1}]),
            patch.object(bot.db, "get_articles", return_value=[{"auto_check": 0}]),
            patch.object(bot.position_queue, "submit", submit),
            patch.object(bot, "_verify_current_wb_session_sync", verification),
        ):
            await bot.scheduled_parse()

        submit.assert_not_awaited()
        verification.assert_not_called()

    async def test_scheduler_pauses_while_wb_authorization_is_active(self):
        submit = AsyncMock()
        get_users = Mock(return_value=[{"telegram_id": 1}])
        job = bot.WbSessionJob(phone="79991234567", chat_id=1)
        bot._wb_session_jobs[1] = job
        try:
            with (
                patch.object(bot.db, "get_allowed_users", get_users),
                patch.object(bot.position_queue, "submit", submit),
            ):
                await bot.scheduled_parse()
        finally:
            bot._wb_session_jobs.pop(1, None)

        get_users.assert_not_called()
        submit.assert_not_awaited()

    async def test_position_parser_stops_batch_on_first_antibot_response(self):
        blocked = {
            "query": "first",
            "promo_pos": None,
            "organic_pos": None,
            "is_advertised": False,
            "preset_id": None,
            "tokens": [],
            "error": True,
            "error_state": "antibot",
            "status_code": 498,
            "error_message": "blocked",
        }
        fetch = Mock(return_value=blocked)
        with (
            patch.object(proxy_positions.config, "WB_PROXIES", ["proxy"]),
            patch.object(proxy_positions, "_token_cache", {"proxy": {"token": "ok"}}),
            patch.object(proxy_positions, "_wb_session", {"cookies": {}}),
            patch.object(proxy_positions, "_get_wbaas_token", return_value="token"),
            patch.object(proxy_positions, "_fetch_keyword_sync", fetch),
        ):
            result = await proxy_positions.get_positions(123, ["first", "second"])

        fetch.assert_called_once()
        self.assertEqual(result["first"]["error_state"], "antibot")
        self.assertEqual(result["second"]["error_state"], "antibot")

    async def test_position_parser_stops_when_retry_hits_antibot(self):
        incomplete = {
            "query": "first", "promo_pos": None, "organic_pos": None,
            "is_advertised": False, "preset_id": None, "tokens": [],
            "error": True, "error_state": None, "status_code": None,
            "error_message": "",
        }
        blocked = {
            **incomplete,
            "error_state": "antibot",
            "status_code": 498,
            "error_message": "blocked on retry",
        }
        fetch = Mock(side_effect=(incomplete, blocked))
        with (
            patch.object(proxy_positions.config, "WB_PROXIES", ["proxy"]),
            patch.object(proxy_positions, "_token_cache", {"proxy": {"token": "ok"}}),
            patch.object(proxy_positions, "_wb_session", {"cookies": {}}),
            patch.object(proxy_positions, "_get_wbaas_token", return_value="token"),
            patch.object(proxy_positions, "_fetch_keyword_sync", fetch),
            patch.object(proxy_positions.asyncio, "sleep", AsyncMock()),
        ):
            result = await proxy_positions.get_positions(123, ["first", "second"])

        self.assertEqual(fetch.call_count, 2)
        self.assertEqual(result["second"]["error_state"], "antibot")

    async def test_position_parser_retries_network_error_once_then_stops(self):
        network_error = {
            "query": "first", "promo_pos": None, "organic_pos": None,
            "is_advertised": False, "preset_id": None, "tokens": [],
            "error": True, "error_state": "network_error", "status_code": 503,
            "error_message": "temporary failure",
        }
        fetch = Mock(side_effect=(network_error, network_error))
        with (
            patch.object(proxy_positions.config, "WB_PROXIES", ["proxy"]),
            patch.object(proxy_positions, "_token_cache", {"proxy": {"token": "ok"}}),
            patch.object(proxy_positions, "_wb_session", {"cookies": {}}),
            patch.object(proxy_positions, "_get_wbaas_token", return_value="token"),
            patch.object(proxy_positions, "_fetch_keyword_sync", fetch),
            patch.object(proxy_positions.asyncio, "sleep", AsyncMock()),
        ):
            result = await proxy_positions.get_positions(123, ["first", "second"])

        self.assertEqual(fetch.call_count, 2)
        self.assertEqual(result["first"]["error_state"], "network_error")
        self.assertEqual(result["second"]["error_state"], "network_error")

    def test_failed_positions_are_not_saved_and_are_labeled_antibot(self):
        article = {"id": 10, "sku": "123"}
        queries = [{"id": 20, "query": "query"}]
        positions = {
            "query": {
                "promo_pos": None,
                "organic_pos": None,
                "is_advertised": False,
                "error": True,
                "error_state": "antibot",
                "status_code": 498,
            }
        }

        with patch.object(bot.db, "save_result") as save_result:
            bot._save_evirma_positions(1, article, queries, positions)

        save_result.assert_not_called()
        rendered = bot._format_evirma_results("123", ["query"], positions)
        self.assertIn("ERR", rendered)
        self.assertIn("A-B", rendered)
        self.assertIn("временно отклонил", rendered)


if __name__ == "__main__":
    unittest.main()
