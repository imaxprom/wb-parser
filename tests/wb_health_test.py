import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import wb_health
import bot
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


if __name__ == "__main__":
    unittest.main()
