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

    def test_more_than_three_slots_preserve_active_and_unique_names(self):
        before = pool.snapshot()
        added = [pool.create() for _ in range(10)]
        rows = pool.list_sessions()
        self.assertEqual(len(rows), 12)
        self.assertEqual(len({r["name"] for r in rows}), 12)
        self.assertEqual(pool.snapshot(), before)
        fourth = added[1]
        pool.bind_phone(fourth, "79991234567")
        pool.publish(fourth, bundle("fourth"))
        self.assertEqual(pool.snapshot(fourth)["phone"], "79991234567")
        self.assertEqual(pool.snapshot(), before)
        with self.assertRaises(ValueError): pool.remove(self.a)
        pool.remove(self.b)
        self.assertIsNone(pool.snapshot(self.b)["id"])
        replacement = pool.create()
        self.assertNotEqual(replacement, self.b)
        self.assertEqual(pool.snapshot(replacement)["name"], "Сессия 2")

    def test_concurrent_additions_keep_unique_slot_names(self):
        with ThreadPoolExecutor(max_workers=4) as executor:
            added = list(executor.map(lambda _: pool.create(), range(12)))
        self.assertEqual(len(set(added)), 12)
        self.assertEqual(len({r["name"] for r in pool.list_sessions()}), 14)

    def test_fourth_account_can_take_over_and_failover_probe_count_stays_bounded(self):
        for number in range(3, 13):
            sid = pool.create()
            pool.publish(sid, bundle(str(number)))
            if number == 4:
                fourth = sid
        with pool.connection() as db:
            db.execute("UPDATE sessions SET success_at=1")
            db.execute("UPDATE sessions SET success_at=2 WHERE id=?", (fourth,))
        with patch.object(pool, "probe", side_effect=self.probe_ok) as probe:
            result = pool.failover(AUTH, pool.generation())
        self.assertEqual(result["state"], "switched")
        self.assertEqual(pool.snapshot()["id"], fourth)
        probe.assert_called_once()
        with patch.object(pool, "probe", return_value={"state": "network_error"}) as probe:
            self.assertIsNone(pool.failover(AUTH, pool.generation()))
        self.assertEqual(probe.call_count, 2)

    def test_add_button_creates_fourth_slot_and_starts_its_login(self):
        pool.create()
        before = pool.snapshot()
        callback = SimpleNamespace(from_user=SimpleNamespace(id=42), data="wb_session_add", answer=AsyncMock())
        state = AsyncMock()
        with patch.object(bot.db, "is_owner", return_value=True), \
             patch.dict(bot._wb_session_jobs, {}, clear=True), \
             patch.object(bot, "_begin_wb_login", new_callable=AsyncMock) as login:
            asyncio.run(bot.wb_sessions_menu(callback, state))
        rows = pool.list_sessions()
        self.assertEqual(len(rows), 4)
        login.assert_awaited_once_with(callback, state, rows[-1]["id"])
        self.assertEqual(pool.snapshot(), before)

    def test_large_pool_menu_paginates_and_always_allows_adding(self):
        for _ in range(23):
            pool.create()
        rows = pool.list_sessions()
        for index, row in enumerate(rows):
            pool.bind_phone(row["id"], str(79990000000 + index))
        rows = pool.list_sessions()
        seen = []
        for page, expected in enumerate((10, 10, 5)):
            text, keyboard = bot._build_wb_sessions_view(page)
            self.assertLess(len(text), 4096)
            self.assertIn("Сессий: 25", text)
            self.assertIn(f"Страница {page + 1} из 3", text)
            session_rows = [r for r in keyboard.inline_keyboard if r[0].callback_data.startswith("wbslot_check_")]
            self.assertEqual(len(session_rows), expected)
            for row, source in zip(session_rows, rows[page * 10:]):
                self.assertEqual(len(row), 3)
                self.assertIn(bot._wb_session_label(source), row[0].text)
                seen.append(row[0].callback_data.removeprefix("wbslot_check_"))
            callbacks = {b.callback_data for row in keyboard.inline_keyboard for b in row}
            self.assertIn("wb_session_add", callbacks)
            if page:
                self.assertIn(f"wb_sessions_page_{page - 1}", callbacks)
                self.assertIn(f"wb_sessions_page_{page}", callbacks)
            if page < 2:
                self.assertIn(f"wb_sessions_page_{page + 1}", callbacks)
        self.assertEqual(seen, [r["id"] for r in rows])
        self.assertEqual(bot._build_wb_sessions_view(999)[0], bot._build_wb_sessions_view(2)[0])
        self.assertEqual(bot._build_wb_sessions_view(-1)[0], bot._build_wb_sessions_view(0)[0])

    def test_page_callbacks_and_delete_menu_preserve_owner_access(self):
        for _ in range(23):
            pool.create()
        callback = SimpleNamespace(from_user=SimpleNamespace(id=42), data="wb_sessions_page_1", answer=AsyncMock(),
                                   message=SimpleNamespace(edit_text=AsyncMock()))
        state = AsyncMock()
        for data in ("wb_sessions_page_1", "wb_session_delete_page_1", "wb_session_add"):
            callback.data = data
            with patch.object(bot.db, "is_owner", return_value=False):
                asyncio.run(bot.wb_sessions_menu(callback, state))
        callback.message.edit_text.assert_not_awaited()
        state.clear.assert_not_awaited()
        self.assertEqual(len(pool.list_sessions()), 25)
        with patch.object(bot.db, "is_owner", return_value=True):
            callback.data = "wb_sessions_page_1"
            asyncio.run(bot.wb_sessions_menu(callback, state))
            self.assertIn("Страница 2 из 3", callback.message.edit_text.call_args.args[0])
            deleted = []
            for page in range(3):
                callback.data = f"wb_session_delete_page_{page}"
                asyncio.run(bot.wb_sessions_menu(callback, state))
                keyboard = callback.message.edit_text.call_args.kwargs["reply_markup"]
                ids = [b.callback_data.removeprefix("wbslot_delete_") for row in keyboard.inline_keyboard
                       for b in row if b.callback_data.startswith("wbslot_delete_")]
                self.assertLessEqual(len(ids), 10)
                deleted.extend(ids)
            self.assertEqual(deleted, [r["id"] for r in pool.list_sessions() if not r["active"]])

    def test_slot_action_retains_page_and_clamps_after_last_page_deletion(self):
        for _ in range(9):
            pool.create()
        sid = pool.list_sessions()[-1]["id"]
        pool.publish(sid, bundle("last-page"))
        callback = SimpleNamespace(from_user=SimpleNamespace(id=42), data="wbslot_check_" + sid, answer=AsyncMock(),
                                   message=SimpleNamespace(edit_text=AsyncMock()))
        with patch.object(bot.db, "is_owner", return_value=True), patch.object(pool, "probe", side_effect=self.probe_ok):
            asyncio.run(bot.wb_session_action(callback, AsyncMock()))
            self.assertIn("Страница 2 из 2", callback.message.edit_text.call_args.args[0])
            callback.data = "wbslot_delete_" + sid
            asyncio.run(bot.wb_session_action(callback, AsyncMock()))
        self.assertEqual(len(pool.list_sessions()), 10)
        self.assertNotIn("Страница 2", callback.message.edit_text.call_args.args[0])

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

    def test_498_reserve_rechecked_after_one_minute_not_healthy_check_interval(self):
        value = pool.snapshot(self.b)
        with patch.object(pool.time, "time", return_value=100000):
            pool.record_health(value, {"state": "antibot", "status": 498})
        with patch.object(pacing, "bot_priority_active", return_value=False), \
                patch.object(pool, "probe", side_effect=self.probe_ok) as probe:
            with patch.object(pool.time, "time", return_value=100059):
                pool.maintain()
                probe.assert_not_called()
            with patch.object(pool.time, "time", return_value=100060):
                pool.maintain()
                probe.assert_called_once()
                self.assertEqual(probe.call_args.args[0]["id"], self.b)
            with patch.object(pool.time, "time", return_value=100120):
                pool.maintain()
                probe.assert_called_once()
        self.assertEqual(pool.snapshot()["id"], self.a)
        row = next(r for r in pool.list_sessions() if r["id"] == self.b)
        self.assertEqual((row["state"], row["retry_at"]), ("healthy", 0))

    def test_repeated_498_uses_fixed_minute_but_other_failures_keep_backoff(self):
        value = pool.snapshot(self.b)
        with patch.object(pool.time, "time", return_value=100000):
            for status, expected in ((498, 60), (498, 60), (403, 3600)):
                pool.record_health(value, {"state": "antibot", "status": status})
                row = next(r for r in pool.list_sessions() if r["id"] == self.b)
                self.assertEqual(row["retry_at"], 100000 + expected)

    def test_498_reserve_not_selected_until_its_minute_expires(self):
        with patch.object(pool.time, "time", return_value=100000):
            pool.record_health(pool.snapshot(self.b), {"state": "antibot", "status": 498})
        with pool.lock("switch"), patch.object(pool, "probe", side_effect=self.probe_ok) as probe:
            with patch.object(pool.time, "time", return_value=100059):
                self.assertIsNone(pool.failover(AUTH, pool.generation()))
                probe.assert_not_called()
            with patch.object(pool.time, "time", return_value=100060):
                self.assertEqual(pool.failover(AUTH, pool.generation())["state"], "switched")
        self.assertEqual(pool.snapshot()["id"], self.b)

    def test_due_498_reserve_still_yields_to_bot_and_server_pause(self):
        with patch.object(pool.time, "time", return_value=100000):
            pool.record_health(pool.snapshot(self.b), {"state": "antibot", "status": 498})
        with patch.object(pool.time, "time", return_value=100060), patch.object(pool, "probe") as probe:
            with patch.object(pacing, "bot_priority_active", return_value=True):
                pool.maintain()
            recovery._save({"session_saved_at": pool.generation(), "server_delay": True, "retry_at": 100600})
            with patch.object(pacing, "bot_priority_active", return_value=False):
                pool.maintain()
            probe.assert_not_called()

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

    def test_phone_is_bound_once_and_survives_refresh_failure_and_restart(self):
        phone = pool.bind_phone(self.a, "+7 (999) 123-45-67")
        self.assertEqual(phone, "79991234567")
        pool.publish(self.a, bundle("renewed"))
        before = pool.snapshot(self.a)
        with self.assertRaises(ValueError):
            pool.bind_phone(self.a, "79991234568")
        with self.assertRaises(ValueError):
            pool.publish(self.a, bundle("cancelled"), cancelled=lambda: True)
        pool.initialize()
        self.assertEqual(pool.snapshot(self.a), before)
        self.assertEqual(next(r for r in pool.list_sessions() if r["id"] == self.a)["phone"], phone)

    def test_phone_extraction_ignores_contacts_encrypted_and_ambiguous_profiles(self):
        value = bundle("profile")
        storage = value["session"]["localStorage"]
        storage["wb_delivery-points"] = json.dumps({"phone": "79991234568"})
        storage["user-info-current"] = json.dumps({"phone": "+7 (999) 123-45-67"})
        storage["wbx__tokenData"] = json.dumps({"phone": "encrypted79991234568"})
        self.assertEqual(pool.phone_from_bundle(value), "79991234567")
        storage["user-info-other"] = json.dumps({"phone": "79991234568"})
        self.assertIsNone(pool.phone_from_bundle(value))
        storage["user-info-current"] = json.dumps({"phone": "+7 999 ***-45-67"})
        del storage["user-info-other"]
        self.assertIsNone(pool.phone_from_bundle(value))

    def test_publication_cannot_change_bound_account(self):
        pool.bind_phone(self.a, "79991234567")
        before = pool.snapshot(self.a)
        candidate = bundle("wrong-account")
        candidate["session"]["localStorage"]["user-info-current"] = json.dumps({"phone": "79991234568"})
        with self.assertRaises(ValueError):
            pool.publish(self.a, candidate)
        self.assertEqual(pool.snapshot(self.a), before)

    def test_v1_pool_backfills_phone_without_changing_authorization(self):
        candidate = bundle("profile")
        candidate["session"]["localStorage"]["user-info-current"] = json.dumps({"phone": "79991234567"})
        pool.publish(self.a, candidate)
        before = pool.snapshot(self.a)
        with pool.connection() as db:
            db.execute("ALTER TABLE sessions DROP COLUMN phone")
        pool.atomic_json(pool.root() / "ready.json", {"version": 1})
        with patch.object(pool, "probe") as probe:
            pool.initialize(); pool.initialize()
        probe.assert_not_called()
        self.assertEqual(pool.snapshot(self.a), before)
        self.assertEqual(pool.snapshot(self.a)["phone"], "79991234567")

    def test_menu_identifies_phone_and_keeps_check_separate_from_renewal(self):
        pool.bind_phone(self.a, "79991234567")
        text, keyboard = bot._build_wb_sessions_view()
        phone = "+7 999 123-45-67"
        self.assertIn(phone, text)
        buttons = [b for row in keyboard.inline_keyboard for b in row]
        check = next(b for b in buttons if b.callback_data == "wbslot_check_" + self.a)
        self.assertIn(phone, check.text)
        pool.record_health(pool.snapshot(self.a), {"state": "auth_expired", "status": 401})
        _, keyboard = bot._build_wb_sessions_view()
        buttons = [b for row in keyboard.inline_keyboard for b in row]
        check = next(b for b in buttons if b.callback_data == "wbslot_check_" + self.a)
        self.assertIn(phone, check.text)
        login = next(b for b in buttons if b.callback_data == "wbslot_login_" + self.a)
        self.assertEqual(login.text, "Обновить")

    def test_saved_phone_button_starts_code_flow_without_phone_prompt_or_duplicate_job(self):
        pool.bind_phone(self.a, "79991234567")
        state = AsyncMock()
        callback = SimpleNamespace(from_user=SimpleNamespace(id=42), data="wbslot_login_" + self.a,
            answer=AsyncMock(), message=SimpleNamespace(chat=SimpleNamespace(id=56), answer=AsyncMock(), edit_text=AsyncMock()))
        async def scenario():
            with patch.dict(bot._wb_session_jobs, {}, clear=True), patch.object(bot.db, "is_owner", return_value=True), \
                 patch.object(bot, "_run_wb_session_job", new_callable=AsyncMock) as run:
                await bot.wb_session_action(callback, state)
                job = bot._wb_session_jobs[42]
                self.assertEqual((job.session_id, job.phone), (self.a, "79991234567"))
                await job.task
                await bot.wb_session_action(callback, state)
                run.assert_awaited_once_with(42, job)
        asyncio.run(scenario())
        state.set_state.assert_awaited_once_with(bot.UpdateWbSession.waiting_code)
        callback.message.edit_text.assert_not_awaited()

    def test_new_phone_is_saved_before_login_and_cancellation_keeps_binding(self):
        slot = pool.create()
        state = AsyncMock()
        state.get_data.return_value = {"session_id": slot}
        message = SimpleNamespace(from_user=SimpleNamespace(id=42), chat=SimpleNamespace(id=56),
                                  text="8 999 123 45 67", answer=AsyncMock())
        callback = SimpleNamespace(from_user=message.from_user, answer=AsyncMock(), message=SimpleNamespace(edit_text=AsyncMock()))
        async def scenario():
            with patch.dict(bot._wb_session_jobs, {}, clear=True), patch.object(bot.db, "is_owner", return_value=True), \
                 patch.object(bot, "_run_wb_session_job", new_callable=AsyncMock):
                await bot._begin_wb_login(callback, state, slot)
                state.set_state.assert_awaited_with(bot.UpdateWbSession.waiting_phone)
                await bot.wb_session_phone_process(message, state)
                job = bot._wb_session_jobs[42]
                self.assertEqual(pool.snapshot(slot)["phone"], "79991234567")
                await job.task
                await bot.wb_session_cancel(callback, state)
                self.assertTrue(job.cancel_event.is_set())
                self.assertEqual(pool.snapshot(slot)["phone"], "79991234567")
        asyncio.run(scenario())

    def test_old_phone_prompt_cannot_rebind_an_existing_session(self):
        pool.bind_phone(self.a, "79991234567")
        state = AsyncMock()
        state.get_data.return_value = {"session_id": self.a}
        message = SimpleNamespace(from_user=SimpleNamespace(id=42), chat=SimpleNamespace(id=56),
                                  text="79991234568", answer=AsyncMock())
        with patch.object(bot.db, "is_owner", return_value=True), patch.object(bot, "_run_wb_session_job") as run:
            asyncio.run(bot.wb_session_phone_process(message, state))
        run.assert_not_called()
        self.assertEqual(pool.snapshot(self.a)["phone"], "79991234567")

    def test_manual_renewal_uses_fresh_browser_and_submits_saved_phone(self):
        phone = pool.bind_phone(self.a, "79991234567")
        job = bot.WbSessionJob(phone=phone, chat_id=1, session_id=self.a, code="123456")
        job.code_event.set()
        with patch("playwright.sync_api.sync_playwright") as playwright, \
             patch.object(bot, "_start_wb_virtual_display", return_value=(None, ":123")), \
             patch.object(bot, "_stop_wb_virtual_display"), \
             patch.object(bot, "_wb_context_has_auth_tokens", return_value=(True, {})), \
             patch.object(bot, "_wb_saved_login_action", return_value=None), \
             patch.object(bot, "_safe_body_text", return_value=""), \
             patch.object(bot, "_visible_button_by_text") as get_code, \
             patch.object(bot, "_save_wb_session_from_context", return_value={"ok": True}):
            browser = playwright.return_value.__enter__.return_value.chromium.launch.return_value
            page = browser.new_context.return_value.new_page.return_value
            page.url = "https://www.wildberries.ru/lk"
            phone_input = page.locator.return_value.first
            phone_input.get_attribute.return_value = "wb-phone-number"
            phone_input.input_value.return_value = phone
            result = bot._run_wb_session_login_sync(phone, job, Mock())
        self.assertTrue(result["ok"])
        self.assertNotIn("storage_state", browser.new_context.call_args.kwargs)
        phone_input.fill.assert_called_once_with("9991234567")
        get_code.return_value.click.assert_called_once()
        browser.close.assert_called_once()

    def test_non_owner_cannot_request_code_from_saved_phone_button(self):
        pool.bind_phone(self.a, "79991234567")
        callback = SimpleNamespace(from_user=SimpleNamespace(id=42), data="wbslot_login_" + self.a, answer=AsyncMock())
        with patch.object(bot.db, "is_owner", return_value=False), patch.object(bot, "_start_wb_phone_login") as start:
            asyncio.run(bot.wb_session_action(callback, AsyncMock()))
        start.assert_not_called()


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
