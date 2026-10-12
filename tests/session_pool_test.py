import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

import bot
import config
import proxy_positions as positions
import wb_search_pacing as pacing
import wb_search_recovery as recovery
import wb_sessions as pool
from cart_stock_worker import CartStockWorker
from wb_session_runtime import _save_session


def bundle(name):
    return {"session": {"saved_at": 10, "cookies": {"x_wbaas_token": "cookie-" + name, "_wbauid": name},
        "localStorage": {"wbx__tokenData": json.dumps({"token": "e30.eyJ1c2VyIjoxfQ." + name}),
                         "wbx__sessionID": name, "session-pow-token": "pow-" + name}},
        "browser": {"cookies": [], "origins": []}, "login": {"cookies": [], "origins": []}}


AUTH = {"error_state": "auth_expired", "status_code": 401}


class SessionPoolTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        for p in (patch.object(config, "DATA_DIR", str(self.directory)),
                  patch.object(config, "WB_PROXIES", []),
                  patch.object(positions, "_wb_session", positions.SessionContext())):
            p.start(); self.addCleanup(p.stop)
        pool.initialize()
        self.a = pool.create()
        pool.publish(self.a, bundle("a"))
        self.b = pool.create()
        pool.publish(self.b, bundle("b"))

    def probe_ok(self, value, **kwargs):
        result = {"state": "healthy", "status": 200, "products": 1}
        pool.record_health(value, result)
        return result

    def test_active_bundle_does_not_change_while_adding_reserve(self):
        before = pool.snapshot()
        self.assertEqual(before["id"], self.a)
        self.assertEqual(before["session"]["cookies"]["x_wbaas_token"], "cookie-a")
        self.assertEqual(pool.snapshot(self.b)["session"]["cookies"]["x_wbaas_token"], "cookie-b")
        with self.assertRaises(ValueError):
            pool.publish(self.b, bundle("a"))
        self.assertEqual(pool.snapshot(), before)

    def test_max_three_slots_and_active_cannot_be_deleted(self):
        pool.create()
        with self.assertRaises(ValueError): pool.create()
        with self.assertRaises(ValueError): pool.remove(self.a)
        pool.remove(self.b)
        self.assertIsNone(pool.snapshot(self.b)["id"])

    def test_stale_or_cancelled_publication_cannot_replace_valid_login(self):
        before = pool.snapshot()
        with self.assertRaises(ValueError):
            pool.publish(self.a, bundle("new"), expected_revision="old")
        with self.assertRaises(ValueError):
            pool.publish(self.a, bundle("new"), cancelled=lambda: True)
        self.assertEqual(pool.snapshot(), before)

    def test_failed_login_does_not_publish_or_change_working_files(self):
        before = pool.snapshot()
        ctx, page = Mock(), Mock()
        raw = bundle("new")["session"]
        ctx.cookies.return_value = [{"name": k, "value": v} for k, v in raw["cookies"].items()]
        page.evaluate.return_value = raw["localStorage"]
        ctx.storage_state.return_value = {"cookies": [], "origins": []}
        with patch.object(pool, "probe", return_value={"state": "invalid_response"}):
            with self.assertRaises(RuntimeError): _save_session(ctx, page, session_id=self.a)
        self.assertEqual(pool.snapshot(), before)

    def test_401_switches_once_and_no_browser_is_started(self):
        before = pool.generation()
        with patch.object(pool, "probe", side_effect=self.probe_ok) as probe, patch.object(recovery, "_refresh") as refresh:
            self.assertTrue(recovery.recover(AUTH, before))
            self.assertTrue(recovery.recover(AUTH, before))
        self.assertEqual(pool.snapshot()["id"], self.b)
        probe.assert_called_once()
        refresh.assert_not_called()

    def test_parallel_failures_cause_only_one_switch(self):
        before = pool.generation()
        def verify(value, **kwargs):
            time.sleep(.03)
            return self.probe_ok(value)
        with patch.object(pool, "probe", side_effect=verify) as probe, patch.object(recovery, "_refresh") as refresh:
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = [executor.submit(recovery.recover, AUTH, before) for _ in range(3)]
                self.assertEqual([f.result(timeout=3) for f in futures], [True] * 3)
        self.assertEqual(probe.call_count, 1)
        refresh.assert_not_called()

    def test_old_response_cannot_mark_new_active_session_unhealthy(self):
        old = pool.generation()
        pool.activate(self.b)
        with patch.object(pool, "probe") as probe:
            self.assertTrue(recovery.recover({**AUTH, "session_generation": old}, pool.generation()))
        probe.assert_not_called()
        self.assertEqual(next(r for r in pool.list_sessions() if r["active"])["state"], "healthy")

    def test_rate_limit_never_rotates_or_refreshes(self):
        before = pool.generation()
        with patch.object(pool, "probe") as probe, patch.object(recovery, "_refresh") as refresh:
            self.assertFalse(recovery.recover({"error_state": "rate_limited", "status_code": 429, "retry_after": 600}, before))
        self.assertEqual(pool.generation(), before)
        self.assertGreater(recovery.cooldown_error(before)["retry_after"], 590)
        probe.assert_not_called(); refresh.assert_not_called()

    def test_rate_pause_cannot_expire_a_fraction_of_a_millisecond_early(self):
        recovery._save({"session_saved_at": pool.generation(), "server_delay": True, "retry_at": 100.0005})
        with patch.object(recovery.time, "time", return_value=100):
            pause = recovery.cooldown_error(pool.generation())
        self.assertEqual(pause["retry_after"], 1)

    def test_429_during_failover_survives_successful_candidate(self):
        before = pool.generation()
        def verify(value, **kwargs):
            recovery.recover({"error_state": "rate_limited", "status_code": 429, "retry_after": 800}, before, allow_refresh=False)
            return self.probe_ok(value)
        with patch.object(pool, "probe", side_effect=verify):
            self.assertFalse(recovery.recover(AUTH, before))
        self.assertGreater(recovery.cooldown_error(pool.generation())["retry_after"], 790)

    def test_shared_antibot_failure_checks_only_one_reserve(self):
        c = pool.create(); pool.publish(c, bundle("c"))
        with patch.object(pool, "probe", return_value={"state": "antibot", "status": 498}) as probe, \
                patch.object(recovery, "_refresh", return_value={"state": "refresh_error"}):
            self.assertFalse(recovery.recover({"error_state": "antibot", "status_code": 498}, pool.generation()))
        probe.assert_called_once()

    def test_busy_reserve_cannot_block_failover_forever(self):
        with pool.lock("slot_" + self.b):
            with patch.object(recovery, "_refresh", return_value={"state": "refresh_error"}), patch.object(pool, "probe") as probe:
                self.assertFalse(recovery.recover(AUTH, pool.generation()))
        probe.assert_not_called()

    def test_reserved_sms_lock_does_not_block_working_session(self):
        with pool.lock("slot_" + self.b):
            positions._load_wb_session()
            self.assertEqual(positions._build_headers("__direct__")["deviceid"], "a")

    def test_healthy_reserve_publication_wakes_waiting_authorization(self):
        before = pool.generation()
        pool.record_health(pool.snapshot(), {"state": "login_required"})
        recovery._save({"session_saved_at": before, "retry_at": time.time() + 21600, "last_result": "login_required"})
        pool.publish(self.b, bundle("b2"))
        self.assertEqual(pool.snapshot()["id"], self.b)
        self.assertIsNone(recovery.cooldown_error(pool.generation()))

    def test_local_authorization_wait_is_retryable_by_existing_website_queue(self):
        recovery._save({"session_saved_at": pool.generation(), "retry_at": time.time() + 100,
                        "last_result": "login_required", "error_state": "auth_expired", "status_code": 401})
        error = recovery.cooldown_error(pool.generation())
        self.assertEqual(error["error_state"], "parser_busy")
        self.assertTrue(error["local_pause"])
        self.assertEqual(error["access_error_state"], "auth_expired")
        self.assertLessEqual(error["retry_after"], 15)
        self.assertGreater(recovery._read(recovery._state_path())["retry_at"], time.time() + 80)

    def test_credential_files_private_and_events_contain_no_tokens(self):
        for path in pool.root().rglob("*.json"):
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        pool.activate(self.b)
        self.assertNotIn("cookie-b", json.dumps(pool.pending_events()))

    def test_request_headers_keep_actual_generation_after_switch(self):
        positions._load_wb_session()
        headers = positions._build_headers("__direct__")
        old = headers.generation
        pool.activate(self.b)
        response = SimpleNamespace(status_code=401, headers={}, json=lambda: {})
        with patch.object(positions.curl_requests, "get", return_value=response):
            _, error = positions._search_unpaced_sync(headers.copy(), {})
        self.assertEqual(error["session_generation"], old)
        self.assertEqual(headers["deviceid"], "a")

    def test_website_thread_mapping_does_not_mix_sessions(self):
        ready = threading.Event(); release = threading.Event()
        def first():
            positions._load_wb_session(); ready.set(); release.wait(2)
            return positions._wb_session.get("pool_id"), positions._build_headers("__direct__")["deviceid"]
        with ThreadPoolExecutor(max_workers=1) as executor:
            f = executor.submit(first)
            self.assertTrue(ready.wait(2))
            pool.activate(self.b); positions._load_wb_session(); release.set()
            self.assertEqual(f.result(timeout=2), (self.a, "a"))
        self.assertEqual(positions._build_headers("__direct__")["deviceid"], "b")

    def test_reserve_probe_does_not_replace_active_process_credentials(self):
        positions._load_wb_session()
        response = SimpleNamespace(status_code=200, headers={}, json=lambda: {"products": [{"id": 123}]})
        with patch.object(positions.curl_requests, "get", return_value=response) as request:
            self.assertEqual(pool.probe(pool.snapshot(self.b))["state"], "healthy")
        self.assertEqual(request.call_args.kwargs["headers"]["deviceid"], "b")
        self.assertEqual(positions._build_headers("__direct__")["deviceid"], "a")

    def test_emergency_probe_does_not_deadlock_on_bot_priority(self):
        response = SimpleNamespace(status_code=200, headers={}, json=lambda: {"products": [{"id": 123}]})
        with pacing.client_scope("bot"), pacing.bot_priority(), patch.object(positions.curl_requests, "get", return_value=response):
            self.assertEqual(pool.probe(pool.snapshot(self.b), emergency=True)["state"], "healthy")

    def test_emergency_probe_respects_global_rate_pause(self):
        recovery._save({"session_saved_at": pool.generation(), "server_delay": True, "retry_at": time.time() + 100})
        with patch.object(positions.curl_requests, "get") as request:
            self.assertEqual(pool.probe(pool.snapshot(self.b), emergency=True)["state"], "paused")
        request.assert_not_called()

    def test_cart_health_is_separate_from_search(self):
        pool.record_health(pool.snapshot(), {"state": "auth_expired", "status": 401}, scope="cart")
        active = next(r for r in pool.list_sessions() if r["active"])
        self.assertEqual(active["state"], "healthy")
        self.assertEqual(active["cart_state"], "auth_expired")

    def test_cart_switch_requires_a_successful_card_response(self):
        worker = CartStockWorker.__new__(CartStockWorker)
        previous = pool.snapshot()
        with patch.object(worker, "_browser_card_request", return_value=(200, '{"products":[]}')), \
                patch.object(worker, "refresh_auth", side_effect=RuntimeError("refresh failed")):
            with self.assertRaises(RuntimeError):
                worker._recover_cart_session(["123"], previous)
        self.assertEqual(pool.snapshot()["id"], self.a)
        self.assertEqual(next(r for r in pool.list_sessions() if r["active"])["state"], "healthy")

    def test_cart_switch_uses_card_validator_without_search_pause(self):
        worker = CartStockWorker.__new__(CartStockWorker)
        with patch.object(worker, "_browser_card_request", return_value=(200, '{"products":[{"id":123}]}')), \
                patch.object(worker, "refresh_auth") as refresh:
            self.assertTrue(worker._recover_cart_session(["123"], pool.snapshot()))
        self.assertEqual(pool.snapshot()["id"], self.b)
        self.assertIsNone(recovery.cooldown_error(pool.generation()))
        refresh.assert_not_called()

    def test_maintenance_can_restore_reserve_while_bot_waits_for_auth(self):
        before = pool.generation()
        pool.record_health(pool.snapshot(), {"state": "login_required"})
        with pool.connection() as conn:
            conn.execute("UPDATE sessions SET checked_at=1 WHERE id=?", (self.b,))
        recovery._save({"session_saved_at": before, "last_result": "login_required",
                        "retry_at": time.time() + 3600, "error_state": "auth_expired"})
        with pacing.client_scope("bot"), pacing.bot_priority(), patch.object(pool, "probe", side_effect=self.probe_ok):
            pool.maintain()
        self.assertEqual(pool.snapshot()["id"], self.b)
        self.assertIsNone(recovery.cooldown_error(pool.generation()))

    def test_maintenance_does_not_probe_during_global_rate_pause(self):
        with pool.connection() as conn:
            conn.execute("UPDATE sessions SET checked_at=1")
        recovery._save({"session_saved_at": pool.generation(), "server_delay": True,
                        "retry_at": time.time() + 3600, "last_result": "server_pause"})
        with patch.object(pool, "probe") as probe:
            pool.maintain()
        probe.assert_not_called()

    def test_empty_managed_pool_does_not_send_anonymous_requests(self):
        with pool.connection() as conn:
            conn.execute("DELETE FROM settings WHERE key='active'")
        with patch.object(positions.curl_requests, "get") as request:
            result = asyncio.run(positions.get_positions(123, ["query"]))
        self.assertEqual(result["query"]["recovery_reason"], "login_required")
        request.assert_not_called()

    def test_worker_recreates_browser_when_generation_changes(self):
        worker = CartStockWorker.__new__(CartStockWorker)
        worker.browser_page = Mock()
        worker.browser_page.is_closed.return_value = False
        old = pool.snapshot()
        worker._browser_version = (old["id"], old["revision"], old["session"]["saved_at"])
        worker._auth_snapshot = old
        self.assertIs(worker._ensure_browser_page(), worker.browser_page)
        pool.activate(self.b)
        worker._auth_snapshot = pool.snapshot()
        with patch.object(worker, "close_browser"), patch("cart_stock_worker.sync_playwright") as playwright:
            page = worker._ensure_browser_page()
            new_context = playwright.return_value.start.return_value.chromium.launch.return_value.new_context
            self.assertEqual(new_context.call_args.kwargs["storage_state"], pool.snapshot()["browser"])
            self.assertIs(page, new_context.return_value.new_page.return_value)

    def test_owner_only_actions_and_stale_callback(self):
        callback = SimpleNamespace(from_user=SimpleNamespace(id=99), data="wbslot_delete_" + self.b,
                                   answer=AsyncMock(), message=Mock())
        with patch.object(bot.db, "is_owner", return_value=False):
            asyncio.run(bot.wb_session_action(callback, AsyncMock()))
        self.assertIsNotNone(pool.snapshot(self.b)["id"])
        callback.answer.assert_awaited_once()

    def test_menu_shows_stale_health_without_exposing_credentials(self):
        with pool.connection() as db:
            db.execute("UPDATE sessions SET checked_at=1 WHERE id=?", (self.b,))
        text, keyboard = bot._build_wb_sessions_view()
        self.assertIn("давно не проверялась", text)
        self.assertNotIn("cookie", text)
        self.assertTrue(any(b.callback_data == "wb_session_add" for row in keyboard.inline_keyboard for b in row))


class MigrationTest(unittest.TestCase):
    def test_existing_session_migrates_once_without_new_login(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(config, "DATA_DIR", directory):
            before = bundle("legacy")["session"]
            Path(directory, "wb_session.json").write_text(json.dumps(before))
            pool.initialize(); pool.initialize()
            self.assertEqual(len(pool.list_sessions()), 1)
            self.assertEqual(pool.generation(), before["saved_at"])
            self.assertEqual(pool.snapshot()["session"]["cookies"], before["cookies"])


if __name__ == "__main__":
    unittest.main()
