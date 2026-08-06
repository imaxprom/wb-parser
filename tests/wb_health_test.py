import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

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
        worker.wb_session = Mock()
        worker.wb_session.get.return_value = SimpleNamespace(
            status_code=498,
            text="Подозрительная активность",
        )

        with self.assertRaises(wb_health.WbAntibotError):
            worker._request_wb_batch(["123"])

        worker.refresh_auth.assert_not_called()
        self.assertEqual(wb_health.get_access_health()["state"], "antibot")

    def test_position_search_classifies_401_as_expired_auth(self):
        session = Mock()
        session.get.return_value = SimpleNamespace(status_code=401)

        data, error = proxy_positions._search_sync({}, {}, session=session)

        self.assertEqual(data, {})
        self.assertEqual(error["error_state"], "auth_expired")
        self.assertEqual(error["status_code"], 401)


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

    async def test_pre_parse_498_is_not_treated_as_expired_auth(self):
        refresh = AsyncMock()
        notify = AsyncMock()
        with (
            patch.object(bot, "_verify_current_wb_session_sync", return_value=(498, 0)),
            patch.object(bot, "_refresh_wb_session_from_saved_state", refresh),
            patch.object(bot, "_notify_owner_wb_session_problem", notify),
        ):
            result = await bot.ensure_wb_session_for_parse()

        self.assertFalse(result)
        refresh.assert_not_awaited()
        notify.assert_awaited_once()
        self.assertEqual(notify.await_args.kwargs["category"], "antibot")
        self.assertEqual(wb_health.get_access_health()["state"], "antibot")

    async def test_position_queue_returns_classified_error_during_cooldown(self):
        wb_health.record_antibot(498, "blocked", "test")
        queue = queue_worker.PositionQueue(pause=0)
        get_positions = AsyncMock()

        with patch.object(queue_worker._positions_module, "get_positions", get_positions):
            await queue.start()
            try:
                future = await queue.submit(1, 123, ["query"], label="123")
                result = await asyncio.wait_for(future, timeout=1)
            finally:
                await queue.stop()

        get_positions.assert_not_awaited()
        self.assertTrue(result["query"]["error"])
        self.assertEqual(result["query"]["error_state"], "antibot")
        self.assertEqual(result["query"]["status_code"], 498)

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
        record = Mock()
        with (
            patch.object(proxy_positions.config, "WB_PROXIES", ["proxy"]),
            patch.object(proxy_positions, "_token_cache", {"proxy": {"token": "ok"}}),
            patch.object(proxy_positions, "_wb_session", {"cookies": {}}),
            patch.object(proxy_positions, "_get_wbaas_token", return_value="token"),
            patch.object(proxy_positions, "_fetch_keyword_sync", fetch),
            patch.object(proxy_positions, "record_antibot", record),
        ):
            result = await proxy_positions.get_positions(123, ["first", "second"])

        fetch.assert_called_once()
        record.assert_called_once_with(498, "blocked", "position_parser")
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
        record = Mock()
        with (
            patch.object(proxy_positions.config, "WB_PROXIES", ["proxy"]),
            patch.object(proxy_positions, "_token_cache", {"proxy": {"token": "ok"}}),
            patch.object(proxy_positions, "_wb_session", {"cookies": {}}),
            patch.object(proxy_positions, "_get_wbaas_token", return_value="token"),
            patch.object(proxy_positions, "_fetch_keyword_sync", fetch),
            patch.object(proxy_positions, "record_antibot", record),
            patch.object(proxy_positions.asyncio, "sleep", AsyncMock()),
        ):
            result = await proxy_positions.get_positions(123, ["first", "second"])

        self.assertEqual(fetch.call_count, 2)
        record.assert_called_once_with(498, "blocked on retry", "position_parser")
        self.assertEqual(result["second"]["error_state"], "antibot")

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
        self.assertIn("антибот-защиту", rendered)
        self.assertIn("авторизация не слетела", rendered)


if __name__ == "__main__":
    unittest.main()
