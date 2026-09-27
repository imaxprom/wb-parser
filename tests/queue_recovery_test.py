import asyncio
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

import queue_worker as queues
import wb_search_recovery as recovery


OK = {"error": False, "promo_pos": 12, "organic_pos": 14, "is_advertised": False}


def paused(deadline, **values):
    return {"error": True, "error_state": "rate_limited", "status_code": 429,
            "promo_pos": None, "organic_pos": None, "is_advertised": False,
            "retry_at": deadline, "recovery_reason": "server_pause",
            "retry_session_saved_at": 1, **values}


class QueueRecoveryTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.session = self.directory / "wb_session.json"
        self.session.write_text(json.dumps({"saved_at": 1}))
        for item in [patch.object(recovery.config, "DATA_DIR", str(self.directory)),
                     patch.object(queues, "PAUSE_POLL_SECONDS", .01)]:
            item.start()
            self.addCleanup(item.stop)
        self.queue = queues.PositionQueue(pause=0)
        await self.queue.start()
        self.addAsyncCleanup(self.queue.stop)

    async def test_resume_at_deadline_without_scheduler_and_preserve_successes(self):
        deadline = time.time() + .08
        calls = []
        async def fetch(article, keywords):
            calls.append((time.time(), keywords))
            if len(calls) == 1:
                return {"a": OK, "b": paused(deadline), "c": paused(deadline)}
            return {kw: OK for kw in keywords}
        callback = AsyncMock()
        with patch.object(queues._positions_module, "get_positions", side_effect=fetch):
            future = await self.queue.submit(1, 123, ["a", "b", "c"], on_pause=callback)
            result = await asyncio.wait_for(future, timeout=1)
        self.assertEqual([call[1] for call in calls], [["a", "b", "c"], ["b", "c"]])
        self.assertGreaterEqual(calls[1][0], deadline)
        self.assertLess(calls[1][0] - deadline, .2)
        self.assertTrue(all(not r["error"] for r in result.values()))
        callback.assert_awaited_once_with(deadline)

    async def test_same_pending_article_shares_work_and_cancellation_is_isolated(self):
        started, release = asyncio.Event(), asyncio.Event()
        async def fetch(*args):
            started.set()
            await release.wait()
            return {"a": OK}
        with patch.object(queues._positions_module, "get_positions", side_effect=fetch) as mock:
            first = await self.queue.submit(1, 123, ["a"])
            await asyncio.wait_for(started.wait(), timeout=1)
            second = await self.queue.submit(1, 123, ["a"])
            first.cancel()
            release.set()
            result = await asyncio.wait_for(second, timeout=1)
        self.assertEqual(result, {"a": OK})
        mock.assert_awaited_once()

    async def test_confirmed_sms_requirement_finishes_instead_of_waiting(self):
        error = paused(time.time() + 21600, recovery_reason="login_required")
        with patch.object(queues._positions_module, "get_positions", AsyncMock(return_value={"a": error})) as fetch:
            result = await asyncio.wait_for(await self.queue.submit(1, 123, ["a"]), timeout=1)
        self.assertEqual(result["a"]["recovery_reason"], "login_required")
        fetch.assert_awaited_once()

    async def test_new_global_deadline_is_respected(self):
        short = time.time() + .02
        long = time.time() + .1
        recovery._save({"session_saved_at": 1, "retry_at": long, "server_delay": True})
        calls = []
        async def fetch(*args):
            calls.append(time.time())
            return {"a": paused(short)} if len(calls) == 1 else {"a": OK}
        with patch.object(queues._positions_module, "get_positions", side_effect=fetch):
            await asyncio.wait_for(await self.queue.submit(1, 123, ["a"]), timeout=1)
        self.assertGreaterEqual(calls[1], long)

    async def test_external_login_wakes_session_pause_without_waiting_old_deadline(self):
        deadline = time.time() + 3600
        recovery._save({"session_saved_at": 1, "retry_at": deadline, "server_delay": False})
        ready = asyncio.Event()
        async def notify(_): ready.set()
        with patch.object(queues._positions_module, "get_positions", AsyncMock(side_effect=[{"a": paused(deadline)}, {"a": OK}])):
            future = await self.queue.submit(1, 123, ["a"], on_pause=notify)
            await asyncio.wait_for(ready.wait(), timeout=1)
            self.session.write_text(json.dumps({"saved_at": 2}))
            result = await asyncio.wait_for(future, timeout=1)
        self.assertFalse(result["a"]["error"])

    async def test_external_login_does_not_skip_rate_limit(self):
        deadline = time.time() + .1
        recovery._save({"session_saved_at": 1, "retry_at": deadline, "server_delay": True})
        calls = []
        async def fetch(*args):
            calls.append(time.time())
            return {"a": paused(deadline)} if len(calls) == 1 else {"a": OK}
        async def notify(_): self.session.write_text(json.dumps({"saved_at": 2}))
        with patch.object(queues._positions_module, "get_positions", side_effect=fetch):
            await asyncio.wait_for(await self.queue.submit(1, 123, ["a"], on_pause=notify), timeout=1)
        self.assertGreaterEqual(calls[1], deadline)

    async def test_repeated_rejections_have_bounded_retries(self):
        async def fetch(*args): return {"a": paused(time.time() + .02)}
        with patch.object(queues._positions_module, "get_positions", side_effect=fetch) as mock:
            result = await asyncio.wait_for(await self.queue.submit(1, 123, ["a"]), timeout=1)
        self.assertEqual(mock.await_count, 1 + queues.MAX_PAUSE_RETRIES)
        self.assertTrue(result["a"]["automatic_retries_exhausted"])

    async def test_failed_status_message_does_not_lose_task(self):
        with patch.object(queues._positions_module, "get_positions", AsyncMock(side_effect=[{"a": paused(time.time() + .02)}, {"a": OK}])):
            callback = AsyncMock(side_effect=RuntimeError("message unavailable"))
            result = await asyncio.wait_for(await self.queue.submit(1, 123, ["a"], on_pause=callback), timeout=1)
        self.assertFalse(result["a"]["error"])

    async def test_stop_resolves_active_and_queued_waiters(self):
        ready = asyncio.Event()
        async def notify(_): ready.set()
        with patch.object(queues._positions_module, "get_positions", AsyncMock(return_value={"a": paused(time.time() + 3600)})):
            active = await self.queue.submit(1, 123, ["a"], on_pause=notify)
            queued = await self.queue.submit(2, 456, ["b"])
            await asyncio.wait_for(ready.wait(), timeout=1)
            await self.queue.stop()
            await asyncio.sleep(0)
            self.assertTrue(active.cancelled())
            self.assertTrue(queued.cancelled())
            self.assertFalse(self.queue._tasks)

    async def test_missing_keyword_is_an_error_not_a_missing_product(self):
        with patch.object(queues._positions_module, "get_positions", AsyncMock(return_value={})):
            result = await asyncio.wait_for(await self.queue.submit(1, 123, ["a"]), timeout=1)
        self.assertTrue(result["a"]["error"])
