import asyncio
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

import config
import db
import queue_worker as queues
import wb_search_recovery as recovery
from wb_job_store import JobStore


OK = {"error": False, "promo_pos": 3, "organic_pos": 4, "is_advertised": False}


class DurableQueueTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        tmp = tempfile.TemporaryDirectory(); self.addCleanup(tmp.cleanup)
        self.directory = Path(tmp.name)
        for p in (patch.object(config, "DATA_DIR", str(self.directory)),
                  patch.object(queues, "PAUSE_POLL_SECONDS", .01)):
            p.start(); self.addCleanup(p.stop)
        (self.directory / "wb_session.json").write_text(json.dumps({"saved_at": 1}))
        self.instances = []

    async def asyncTearDown(self):
        for queue in self.instances:
            if queue._worker_task and not queue._worker_task.done():
                queue._worker_task.cancel()
                try: await queue._worker_task
                except asyncio.CancelledError: pass
                queue._worker_task = None
            await queue.stop()

    async def start_queue(self, callback=None):
        queue = queues.PositionQueue(pause=0, durable=True)
        queue.on_restored_complete = callback
        self.instances.append(queue)
        await queue.start()
        return queue

    async def crash(self, queue):
        queue._worker_task.cancel()
        try: await queue._worker_task
        except asyncio.CancelledError: pass
        queue._worker_task = None
        await queue.stop()

    async def test_crash_keeps_completed_keys_and_only_restarts_unfinished_key(self):
        progressed = asyncio.Event()
        async def first_fetch(article, keywords):
            await queues._positions_module._keyword_callback.get()("a", OK)
            progressed.set()
            await asyncio.sleep(100)
        first = await self.start_queue()
        with patch.object(queues._positions_module, "get_positions", side_effect=first_fetch):
            await first.submit(1, 123, ["a", "b"])
            await asyncio.wait_for(progressed.wait(), 1)
            await self.crash(first)
        delivered = AsyncMock()
        with patch.object(queues._positions_module, "get_positions", AsyncMock(return_value={"b": OK})) as fetch:
            second = await self.start_queue(delivered)
            restored = next(iter(second._tasks.values())).future
            result = await asyncio.wait_for(restored, 1)
        fetch.assert_awaited_once_with(123, ["b"])
        self.assertEqual(result["a"]["promo_pos"], 3)
        self.assertEqual(result["a"]["_job_id"], result["b"]["_job_id"])
        self.assertEqual(second._store.counts(), {"done": 1})
        delivered.assert_awaited_once()

    async def test_login_required_is_saved_and_resumes_after_new_session(self):
        waiting = asyncio.Event(); calls = []
        async def fetch(article, keywords):
            calls.append(keywords)
            if len(calls) == 1:
                return {"a": {"error": True, "error_state": "auth_expired", "recovery_reason": "login_required",
                              "retry_at": time.time() + 3600, "retry_session_saved_at": 1}}
            return {"a": OK}
        async def notify(_): waiting.set()
        queue = await self.start_queue()
        with patch.object(queues._positions_module, "get_positions", side_effect=fetch):
            future = await queue.submit(1, 123, ["a"], on_pause=notify)
            await asyncio.wait_for(waiting.wait(), 1)
            self.assertFalse(future.done())
            self.assertEqual(queue._store.counts(), {"waiting_login": 1})
            (self.directory / "wb_session.json").write_text(json.dumps({"saved_at": 2}))
            result = await asyncio.wait_for(future, 1)
        self.assertFalse(result["a"]["error"])

    async def test_restarted_queue_obeys_saved_rate_limit(self):
        store = JobStore(); job = store.add(1, 123, ["a"], "123")
        deadline = time.time() + .12
        with store.connect() as conn:
            conn.execute("UPDATE jobs SET retry_at=?,generation=1,state='waiting' WHERE id=?", (deadline, job))
        recovery._save({"session_saved_at": 1, "server_delay": True, "retry_at": deadline})
        calls = []
        async def fetch(*args): calls.append(time.time()); return {"a": OK}
        with patch.object(queues._positions_module, "get_positions", side_effect=fetch):
            queue = await self.start_queue()
            await asyncio.wait_for(next(iter(queue._tasks.values())).future, 1)
        self.assertGreaterEqual(calls[0], deadline)

    async def test_duplicate_submission_has_one_durable_job(self):
        event = asyncio.Event()
        async def fetch(*args): await event.wait(); return {"a": OK}
        queue = await self.start_queue()
        with patch.object(queues._positions_module, "get_positions", side_effect=fetch) as mocked:
            a = await queue.submit(1, 123, ["a"])
            b = await queue.submit(1, 123, ["a"])
            event.set(); await asyncio.gather(a, b)
        self.assertEqual(mocked.call_count, 1)
        self.assertEqual(queue._store.counts(), {"done": 1})

    async def test_completed_job_is_reapplied_and_delivered_without_wb_requests(self):
        store = JobStore(); job = store.add(1, 123, ["a"], "123")
        with store.connect() as conn:
            conn.execute("UPDATE jobs SET result=?,state='completed' WHERE id=?", (json.dumps({"a": {**OK, "_job_id": job}}), job))
        delivered = AsyncMock()
        with patch.object(queues._positions_module, "get_positions", AsyncMock()) as fetch:
            queue = await self.start_queue(delivered)
            await asyncio.wait_for(next(iter(queue._tasks.values())).future, 1)
        fetch.assert_not_awaited(); delivered.assert_awaited_once()

    async def test_second_worker_cannot_claim_same_queue(self):
        first = await self.start_queue()
        second = queues.PositionQueue(durable=True)
        with self.assertRaises(BlockingIOError): await second.start()
        self.assertTrue(first._running)


class IdempotentHistoryTest(unittest.TestCase):
    def test_replayed_completion_does_not_duplicate_history(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "user.db"
            with sqlite3.connect(path) as conn:
                conn.execute("CREATE TABLE results(article_id INTEGER, query_id INTEGER, position INTEGER, page INTEGER, total_found INTEGER)")
            with patch.object(db, "_get_user_db_path", return_value=str(path)):
                db.save_result(1, 1, 1, 8, 1, job_key="job-a")
                db.save_result(1, 1, 1, 8, 1, job_key="job-a")
                db.save_result(1, 1, 1, 9, 1, job_key="job-b")
            with sqlite3.connect(path) as conn:
                self.assertEqual(conn.execute("SELECT position FROM results").fetchall(), [(8,), (9,)])

    def test_real_process_kill_preserves_only_completed_keys(self):
        first_code = '''
import asyncio
from pathlib import Path
import config, queue_worker as q
async def run():
    queue=q.PositionQueue(pause=0,durable=True)
    async def fetch(article,keywords):
        await q._positions_module._keyword_callback.get()('a',{'error':False,'promo_pos':3})
        Path(config.DATA_DIR,'ready').write_text('ready')
        await asyncio.sleep(60)
    q._positions_module.get_positions=fetch
    await queue.start()
    await (await queue.submit(1,123,['a','b']))
asyncio.run(run())
'''
        second_code = '''
import asyncio,json
import queue_worker as q
async def run():
    calls=[]
    async def fetch(article,keywords):
        calls.append(keywords)
        return {k:{'error':False,'promo_pos':4} for k in keywords}
    q._positions_module.get_positions=fetch
    queue=q.PositionQueue(pause=0,durable=True)
    await queue.start()
    value=await next(iter(queue._tasks.values())).future
    await queue.stop()
    print(json.dumps({'calls':calls,'keys':list(value),'counts':queue._store.counts()}))
asyncio.run(run())
'''
        with tempfile.TemporaryDirectory() as directory:
            env = {**os.environ, "WB_PARSER_DATA_DIR": directory}
            child = subprocess.Popen([sys.executable, "-c", first_code], env=env,
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                deadline = time.monotonic() + 8
                while not Path(directory, "ready").exists() and time.monotonic() < deadline:
                    time.sleep(.03)
                self.assertTrue(Path(directory, "ready").exists())
            finally:
                child.kill(); child.wait(timeout=3)
            second = subprocess.run([sys.executable, "-c", second_code], env=env,
                                    capture_output=True, text=True, timeout=8, check=True)
            result = json.loads(second.stdout)
            self.assertEqual(result, {"calls": [["b"]], "keys": ["a", "b"], "counts": {"done": 1}})


if __name__ == "__main__":
    unittest.main()
