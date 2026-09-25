import asyncio
from concurrent.futures import ThreadPoolExecutor
import fcntl
import json
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import bot
import proxy_positions as positions
import wb_health
import wb_search_recovery as recovery


OK = {"promo_pos": 4, "organic_pos": 5, "is_advertised": False, "error": False}
BLOCKED = {"promo_pos": None, "organic_pos": None, "is_advertised": False,
           "error": True, "error_state": "antibot", "status_code": 498}


class SearchRecoveryTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.session = self.directory / "wb_session.json"
        self.session.write_text(json.dumps({"saved_at": 1}))
        patches = [patch.object(recovery.config, "DATA_DIR", str(self.directory)),
                   patch.object(positions.config, "WB_PROXIES", []),
                   patch.object(positions, "_WB_SESSION_FILE", str(self.session)),
                   patch.object(positions, "_WBAAS_CACHE", str(self.directory / "tokens.json")),
                   patch.object(positions, "_wb_session", {}),
                   patch.object(positions, "_token_cache", {})]
        for item in patches:
            item.start()
            self.addCleanup(item.stop)

    def renewed(self):
        self.session.write_text(json.dumps({"saved_at": 2}))
        return {"state": "refreshed", "verification": {"state": "healthy", "status": 200}}

    def test_batch_resumes_failed_keyword_and_keeps_completed_work(self):
        with patch.object(positions, "_fetch_keyword_sync", side_effect=[OK, dict(BLOCKED), OK, OK]) as fetch, \
                patch.object(positions.curl_requests, "Session") as clients, \
                patch.object(recovery, "_refresh", side_effect=self.renewed) as refresh:
            result = asyncio.run(positions.get_positions(123, ["first", "second", "third"]))
        self.assertEqual([c.args[1] for c in fetch.call_args_list], ["first", "second", "second", "third"])
        self.assertTrue(all(not item["error"] for item in result.values()))
        self.assertEqual(clients.call_count, 2)
        self.assertEqual(clients.return_value.close.call_count, 2)
        refresh.assert_called_once()

    def test_rejected_retry_stops_batch_and_next_batch_without_network_or_login(self):
        with patch.object(positions, "_fetch_keyword_sync", side_effect=[dict(BLOCKED), dict(BLOCKED)]) as fetch, \
                patch.object(positions.curl_requests, "Session"), \
                patch.object(recovery, "_refresh", side_effect=self.renewed) as refresh:
            first = asyncio.run(positions.get_positions(123, ["a", "b"]))
            # A separate event loop models a new request: cooldown lives on disk.
            second = asyncio.run(positions.get_positions(123, ["c"]))
        self.assertEqual(fetch.call_count, 2)
        refresh.assert_called_once()
        self.assertTrue(first["b"]["error"] and second["c"]["error"])
        self.assertGreater(second["c"]["retry_after"], 0)

    def test_only_one_recovery_allowed_even_if_another_keyword_fails(self):
        with patch.object(positions, "_fetch_keyword_sync", side_effect=[dict(BLOCKED), OK, dict(BLOCKED)]) as fetch, \
                patch.object(positions.curl_requests, "Session"), \
                patch.object(recovery, "_refresh", side_effect=self.renewed) as refresh:
            result = asyncio.run(positions.get_positions(123, ["a", "b", "c"]))
        refresh.assert_called_once()
        self.assertEqual(fetch.call_count, 3)
        self.assertFalse(result["a"]["error"])
        self.assertTrue(result["b"]["error"] and result["c"]["error"])

    def test_rate_limit_and_server_pause_never_start_login(self):
        for error in [{**BLOCKED, "retry_after": 10000},
                      {**BLOCKED, "error_state": "rate_limited", "status_code": 429, "retry_after": 20000}]:
            with patch.object(recovery, "_refresh") as refresh:
                self.assertFalse(recovery.recover(error, 1))
                refresh.assert_not_called()
            self.assertGreaterEqual(recovery.cooldown_error(1)["retry_after"], error["retry_after"] - 1)
        # Even an externally replaced session must respect a server pause.
        self.session.write_text(json.dumps({"saved_at": 3}))
        self.assertGreater(recovery.cooldown_error(3)["retry_after"], 19000)

    def test_candidate_retry_after_cannot_be_shortened_by_backoff(self):
        with patch.object(recovery, "_refresh", return_value={"state": "candidate_rejected",
                "verification": {"state": "rate_limited", "retry_after": 30000}}):
            self.assertFalse(recovery.recover(BLOCKED, 1))
        self.assertGreater(recovery.cooldown_error(1)["retry_after"], 29990)

    def test_sms_requirement_is_reported_and_manual_login_clears_pause(self):
        with patch.object(recovery, "_refresh", return_value={"state": "login_required"}) as refresh:
            self.assertFalse(recovery.recover(BLOCKED, 1))
            self.assertFalse(recovery.recover(BLOCKED, 1))
            refresh.assert_called_once()
        error = recovery.cooldown_error(1)
        self.assertEqual(error["recovery_reason"], "login_required")
        self.assertGreater(error["retry_after"], 21000)
        notice = bot._evirma_error_notice([{"a": {**error, "error": True}}])
        self.assertIn("Обновить WB-сессию", notice)
        self.session.write_text(json.dumps({"saved_at": 2}))
        self.assertIsNone(recovery.cooldown_error(2))

    def test_concurrent_recovery_uses_one_browser(self):
        def renew_slowly():
            time.sleep(0.03)
            return self.renewed()
        with patch.object(recovery, "_refresh", side_effect=renew_slowly) as refresh, \
                ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(recovery.recover, BLOCKED, 1) for _ in range(2)]
            self.assertEqual([future.result(timeout=3) for future in futures], [True, True])
        refresh.assert_called_once()

    def test_failed_browser_does_not_overwrite_saved_session(self):
        with patch.object(recovery, "_refresh", side_effect=RuntimeError("private-value")):
            self.assertFalse(recovery.recover(BLOCKED, 1))
        self.assertEqual(recovery.session_generation(), 1)
        self.assertNotIn("private-value", recovery._state_path().read_text())

    def test_unverified_renewal_does_not_resume_batch(self):
        with patch.object(recovery, "_refresh", return_value={"state": "refreshed"}):
            self.assertFalse(recovery.recover(BLOCKED, 1))

    def test_existing_browser_login_prevents_another_browser(self):
        with (self.directory / "wb_session_refresh.lock").open("a+") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            with patch.object(recovery, "_refresh") as refresh:
                self.assertFalse(recovery.recover(BLOCKED, 1))
                refresh.assert_not_called()
        self.assertEqual(recovery.cooldown_error(1)["recovery_reason"], "login_in_progress")

    def test_interactive_login_takes_shared_browser_lock(self):
        def login(*args):
            with (self.directory / "wb_session_refresh.lock").open("a+") as lock:
                with self.assertRaises(BlockingIOError):
                    fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return {"ok": True}
        with patch.object(bot, "_run_wb_session_login_unlocked", side_effect=login):
            self.assertEqual(bot._run_wb_session_login_sync("", None, None), {"ok": True})

    def test_connection_closes_if_fetch_raises(self):
        with patch.object(positions, "_fetch_keyword_sync", side_effect=RuntimeError), \
                patch.object(positions.curl_requests, "Session") as client:
            with self.assertRaises(RuntimeError):
                asyncio.run(positions.get_positions(123, ["a"]))
        client.return_value.close.assert_called_once()

    def test_auth_response_preserves_http_date_retry_after(self):
        response = SimpleNamespace(status_code=401, headers={"Retry-After": "Thu, 01 Jan 1970 01:00:00 GMT"})
        with patch.object(wb_health.time, "time", return_value=1000), \
                patch.object(positions.curl_requests, "get", return_value=response):
            _, error = positions._search_sync({}, {})
        self.assertEqual(error["retry_after"], 2600)

    def test_unavailable_server_with_retry_after_is_not_retried(self):
        response = SimpleNamespace(status_code=503, headers={"Retry-After": "3600"})
        with patch.object(positions.curl_requests, "Session") as client, \
                patch.object(recovery, "_refresh") as refresh:
            client.return_value.get.return_value = response
            result = asyncio.run(positions.get_positions(123, ["a", "b"]))
        client.return_value.get.assert_called_once()
        refresh.assert_not_called()
        self.assertGreater(result["b"]["retry_after"], 3590)


if __name__ == "__main__":
    unittest.main()
