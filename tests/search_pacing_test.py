import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import multiprocessing
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import proxy_positions as positions
import wb_search_pacing as pacing
import wb_search_recovery as recovery


def paced_worker(directory, output):
    pacing.config.DATA_DIR = directory
    for _ in range(3):
        with pacing.request_slot():
            start = time.time()
            time.sleep(.01)
            output.put((start, time.time()))


class SearchPacingTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.directory = Path(temp.name)
        for item in (patch.object(pacing.config, "DATA_DIR", str(self.directory)),
                     patch.object(positions.config, "WB_PROXIES", []),
                     patch.object(positions, "_WB_SESSION_FILE", str(self.directory / "wb_session.json")),
                     patch.object(positions, "_WBAAS_CACHE", str(self.directory / "tokens.json"))):
            item.start()
            self.addCleanup(item.stop)
        (self.directory / "wb_session.json").write_text(json.dumps({"saved_at": 1}))
        self.configure()

    def configure(self, **extra):
        value = {"enabled": True, "name": "test", "gap_ms": 0, **extra}
        (self.directory / "wb_search_pacing.json").write_text(json.dumps(value))

    def test_different_processes_share_request_slot_and_response_gap(self):
        self.configure(gap_ms=30)
        ctx = multiprocessing.get_context("spawn")
        output = ctx.Queue()
        children = [ctx.Process(target=paced_worker, args=(str(self.directory), output)) for _ in range(2)]
        try:
            for child in children: child.start()
            events = sorted(output.get(timeout=15) for _ in range(6))
            for child in children:
                child.join(timeout=5)
                self.assertEqual(child.exitcode, 0)
            for previous, current in zip(events, events[1:]):
                self.assertGreaterEqual(current[0] - previous[1], .025)
        finally:
            for child in children:
                if child.is_alive(): child.terminate(); child.join()
            output.close()

    def test_batch_pause_applies_across_individual_calls(self):
        self.configure(gap_ms=10, batch_size=2, batch_pause_ms=50)
        starts = []
        for _ in range(3):
            with pacing.request_slot(): starts.append(time.time())
        self.assertGreaterEqual(starts[1] - starts[0], .009)
        self.assertGreaterEqual(starts[2] - starts[1], .049)

    def test_client_profiles_propagate_to_request_threads_and_expire_independently(self):
        self.configure(gap_ms=50, profiles={
            "bot": {"name": "bot_batch", "batch_size": 4, "batch_pause_ms": 5000},
            "benchmark": {"name": "expired", "gap_ms": 0, "experiment": True, "expires_at": 1}})
        async def check():
            with pacing.client_scope("bot"):
                return await asyncio.to_thread(pacing.policy)
        bot = asyncio.run(check())
        self.assertEqual((bot["scope"],bot["batch_size"],bot["gap_ms"]),("bot",4,50))
        with pacing.client_scope("benchmark"):
            self.assertEqual(pacing.policy()["gap_ms"],1500)
        with pacing.client_scope("rpc"):
            self.assertEqual(pacing.policy()["gap_ms"],50)
            self.assertFalse(pacing.policy()["experiment"])

    def test_rpc_between_bot_requests_does_not_reset_bot_batch_count(self):
        self.configure(profiles={"bot": {"name": "batch", "batch_size": 2, "batch_pause_ms": 80}})
        with pacing.client_scope("bot"):
            with pacing.request_slot(): pass
            with pacing.request_slot(): last_bot = time.time()
        with pacing.client_scope("rpc"):
            with pacing.request_slot(): rpc_at = time.time()
        with pacing.client_scope("bot"):
            with pacing.request_slot(): next_bot = time.time()
        self.assertLess(rpc_at-last_bot,.06)
        self.assertGreaterEqual(next_bot-last_bot,.075)
        state=json.loads((self.directory/'wb_search_pacing_state.json').read_text())
        self.assertEqual(state['clients']['bot']['count'],3)
        self.assertEqual(state['clients']['rpc']['count'],1)

    def test_bot_wait_releases_global_slot_for_rpc(self):
        self.configure(profiles={"bot": {"name": "slow_bot", "gap_ms": 300}})
        first = threading.Event()
        def bot():
            with pacing.client_scope('bot'):
                with pacing.request_slot(): start=time.time()
                first.set()
                with pacing.request_slot(): return start,time.time()
        with ThreadPoolExecutor(max_workers=1) as pool:
            future=pool.submit(bot)
            self.assertTrue(first.wait(2))
            time.sleep(.03)
            with pacing.client_scope('rpc'):
                with pacing.request_slot(): rpc_at=time.time()
            start,finish=future.result(timeout=3)
        self.assertLess(rpc_at-start,.2)
        self.assertGreaterEqual(finish-rpc_at,.29)

    def test_existing_pause_blocks_network_and_is_not_extended(self):
        recovery._save({"session_saved_at": 1, "failures": 1, "retry_at": time.time() + 60,
                        "status_code": 429, "error_state": "rate_limited", "server_delay": True})
        before = recovery._read(recovery._state_path())
        with patch.object(positions.curl_requests, "get") as request:
            _, error = positions._search_sync({}, {})
        request.assert_not_called()
        self.assertTrue(error["local_pause"])
        recovery.recover(error, 1)
        self.assertEqual(recovery._read(recovery._state_path()), before)

    def test_rate_limit_is_published_before_request_slot_is_released(self):
        self.configure(experiment=True, expires_at=time.time() + 120)
        with patch.object(positions.curl_requests, "Session") as client:
            client.return_value.get.return_value = SimpleNamespace(status_code=429, headers={})
            result = asyncio.run(positions.get_positions(123, ["a", "b"]))
            client.return_value.get.assert_called_once()
        state = recovery._read(recovery._state_path())
        self.assertEqual(state["failures"], 1)
        self.assertEqual(state["experiment_429_count"], 1)
        self.assertGreater(result["a"]["retry_after"], 55)
        with patch.object(positions.curl_requests, "get") as request:
            _, error = positions._search_sync({}, {})
        request.assert_not_called()
        self.assertTrue(error["local_pause"])
        self.assertEqual(state, recovery._read(recovery._state_path()))

    def test_experiment_backoff_survives_success_and_honors_retry_after(self):
        self.configure(experiment=True, expires_at=200000)
        error = {"error_state": "rate_limited", "status_code": 429}
        for now, delay, count in [(100000, 60, 1), (100061, 120, 2), (100182, 300, 3)]:
            with patch.object(recovery.time, "time", return_value=now):
                recovery.record_success(1, now)
                recovery.recover(error, 1)
                state = recovery._read(recovery._state_path())
                self.assertEqual(state["retry_at"], now + delay)
                self.assertEqual(state["experiment_429_count"], count)
        with patch.object(recovery.time, "time", return_value=100483):
            recovery.record_success(1, 100483)
            recovery.recover({**error, "retry_after": 1800}, 1)
            self.assertEqual(recovery.cooldown_error(1)["retry_after"], 1800)

    def test_expired_lease_keeps_conservative_pacing_and_normal_backoff(self):
        self.configure(experiment=True, expires_at=1, gap_ms=50)
        self.assertEqual(pacing.policy()["name"], "fallback")
        self.assertEqual(pacing.policy()["gap_ms"], 1500)
        recovery.recover({"error_state": "rate_limited", "status_code": 429}, 1)
        self.assertGreater(recovery.cooldown_error(1)["retry_after"], 295)

    def test_audit_preserves_regime_and_excludes_local_wait_from_latency(self):
        self.configure(name="stage_a", gap_ms=20)
        response = SimpleNamespace(status_code=200, headers={}, json=lambda: {"products": []})
        with patch.object(positions.curl_requests, "get", return_value=response):
            positions._search_sync({}, {})
            positions._search_sync({}, {})
        events = [json.loads(line) for path in (self.directory / "wb_search_requests").glob("*.jsonl")
                  for line in path.read_text().splitlines()]
        self.assertEqual(events[0]["pacing"]["name"], "stage_a")
        self.assertGreaterEqual(events[1]["started_at"] - events[0]["at"], .019)
        self.assertLess(events[1]["elapsed_ms"], 20)
