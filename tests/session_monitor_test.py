import asyncio
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import proxy_positions
from scripts import wb_session_monitor as monitor


class SessionMonitorTest(unittest.TestCase):
    def test_html_challenge_with_200_is_not_success(self):
        response = SimpleNamespace(status_code=200, headers={}, text="Подозрительная активность")
        with patch.object(monitor.positions.curl_requests, "get", return_value=response), patch.object(monitor.positions, "_build_headers", return_value={}):
            self.assertEqual(monitor.probe(reload=False)["state"], "antibot")

    def test_empty_catalog_is_not_success(self):
        response = SimpleNamespace(status_code=200, headers={}, text="{}", json=lambda: {"products": []})
        with patch.object(monitor.positions.curl_requests, "get", return_value=response), patch.object(monitor.positions, "_build_headers", return_value={}):
            self.assertEqual(monitor.probe(reload=False)["state"], "invalid_response")

    def test_retry_after_is_respected_and_backoff_is_capped(self):
        self.assertEqual(monitor.delay_for({"state": "rate_limited", "retry_after": 1800}, 1, 300), 1800)
        self.assertEqual(monitor.delay_for({"state": "antibot"}, 1, 300), 900)
        self.assertEqual(monitor.delay_for({"state": "antibot"}, 1000, 300), 7200)

    def test_session_metadata_does_not_contain_credentials(self):
        secret = "private-token"
        data = {"saved_at": 1, "localStorage": {"wbx__tokenData": json.dumps({"token": secret, "phone": secret})}, "cookies": {"x_wbaas_token": secret}}
        with patch.object(monitor, "read_json", return_value=data):
            self.assertNotIn(secret, json.dumps(monitor.session_metadata()))

    def _cycle(self, initial, result, refresh=None):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        directory = Path(temporary.name)
        state = {"start_at": 100, "end_at": 1000, "interval": 300, "next_at": 100,
                 "failures": 1, "recover_pending": True, **initial}
        monitor.atomic_json(directory / "state.json", state)
        def complete_probe():
            # The current iteration runs; its next loop finishes the experiment.
            state_time[0] = 500
            return result
        state_time = [200]
        real_write = monitor.atomic_json
        def finish_iteration(path, data):
            real_write(path, data)
            state_time[0] = 1001
        with patch.object(monitor.time, "time", side_effect=lambda: state_time[0]), patch.object(monitor, "probe", side_effect=complete_probe), patch.object(monitor, "session_metadata", return_value={}), patch.object(monitor, "production_load", return_value={}), patch.object(monitor, "refresh_subprocess", return_value=refresh) as renew, patch.object(monitor, "atomic_json", side_effect=finish_iteration), patch.object(monitor.config, "DATA_DIR", str(directory)), patch.object(monitor, "report"):
            monitor.run(directory, 12, 300)
        return renew, monitor.read_json(directory / "state.json")

    def test_natural_recovery_does_not_renew_session(self):
        renew, state = self._cycle({}, {"state": "healthy"})
        renew.assert_not_called()
        self.assertFalse(state["recover_pending"])

    def test_sms_requirement_backs_off_six_hours(self):
        renew, state = self._cycle({}, {"state": "antibot"}, {"state": "login_required"})
        renew.assert_called_once_with()
        self.assertEqual(state["next_at"], 500 + 21600)

    def test_first_antibot_failure_waits_before_refresh(self):
        renew, state = self._cycle({"recover_pending": False, "failures": 0}, {"state": "antibot"})
        renew.assert_not_called()
        self.assertEqual(state["next_at"], 1400)

    def test_existing_auth_cache_is_reloaded_at_batch_boundary(self):
        with patch.object(proxy_positions, "_wb_session", {"saved_at": 1}), patch.object(proxy_positions, "_token_cache", {"__direct__": {}}), patch.object(proxy_positions, "_load_wb_session") as session, patch.object(proxy_positions, "_load_token_cache") as cookies, patch.object(proxy_positions.curl_requests, "Session"), patch.object(proxy_positions.config, "WB_PROXIES", []):
            asyncio.run(proxy_positions.get_positions(1, []))
        session.assert_called_once_with()
        cookies.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
