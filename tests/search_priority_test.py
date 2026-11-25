import asyncio
import json
import multiprocessing
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

import wb_search_pacing as pacing


def bot_pair(directory, in_flight, finish_current, output):
    pacing.config.DATA_DIR=directory
    with pacing.client_scope('bot'), pacing.bot_priority():
        with pacing.request_slot():
            output.put(('bot1',time.monotonic()))
            in_flight.set()
            if not finish_current.wait(10):raise TimeoutError()
        with pacing.request_slot():output.put(('bot2',time.monotonic()))


def rpc_once(directory, entering, output):
    pacing.config.DATA_DIR=directory
    with pacing.client_scope('rpc'):
        entering.set()
        with pacing.request_slot():output.put(('rpc',time.monotonic()))


def idle_rpc(directory, admitted, release):
    pacing.config.DATA_DIR=directory
    with pacing.client_scope('rpc'), pacing.request_slot():
        admitted.set()
        release.wait(10)


def waiting_bot(directory, entering, entered):
    pacing.config.DATA_DIR=directory
    with pacing.client_scope('bot'):
        entering.set()
        with pacing.request_slot():entered.set()


def stop_process(process):
    if process.is_alive():process.terminate()
    process.join(timeout=5)


class SearchPriorityTest(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        self.root=Path(temp.name)
        patcher=patch.object(pacing.config,'DATA_DIR',str(self.root));patcher.start();self.addCleanup(patcher.stop)
        (self.root/'wb_search_pacing.json').write_text(json.dumps({'enabled':True,'name':'test','gap_ms':0}))
        self.ctx=multiprocessing.get_context('spawn')

    def start(self,target,*args):
        process=self.ctx.Process(target=target,args=(str(self.root),*args))
        process.start();self.addCleanup(stop_process,process)
        return process

    def assert_rpc_preempts_between_bot_requests(self):
        in_flight=self.ctx.Event();finish=self.ctx.Event();entering=self.ctx.Event();out=self.ctx.Queue()
        bot=self.start(bot_pair,in_flight,finish,out)
        self.assertTrue(in_flight.wait(5))
        self.assertFalse(pacing.bot_priority_active(), 'Telegram has no whole-job priority')
        rpc=self.start(rpc_once,entering,out)
        self.assertTrue(entering.wait(5))
        until=time.monotonic()+3
        while not pacing.bot_priority_active() and time.monotonic()<until:time.sleep(.005)
        self.assertTrue(pacing.bot_priority_active(), 'RPC reserves before the current HTTP finishes')
        finish.set()
        events=sorted([out.get(timeout=5) for _ in range(3)],key=lambda e:e[1])
        for process in (bot,rpc):process.join(5);self.assertEqual(process.exitcode,0)
        self.assertEqual([e[0] for e in events],['bot1','rpc','bot2'])
        self.assertFalse(pacing.bot_priority_active())
        out.close()

    def test_rpc_preempts_after_current_http_without_waiting_for_whole_bot_job(self):
        self.assert_rpc_preempts_between_bot_requests()

    def test_priority_still_applies_if_optional_pacing_is_disabled(self):
        (self.root/'wb_search_pacing.json').write_text('{}')
        self.assert_rpc_preempts_between_bot_requests()

    def test_crashed_rpc_releases_bot_without_stale_deadline(self):
        admitted=self.ctx.Event();release=self.ctx.Event();entering=self.ctx.Event();entered=self.ctx.Event()
        rpc=self.start(idle_rpc,admitted,release)
        self.assertTrue(admitted.wait(5))
        self.assertTrue(pacing.bot_priority_active())
        bot=self.start(waiting_bot,entering,entered)
        self.assertTrue(entering.wait(5))
        self.assertFalse(entered.wait(.15))
        rpc.terminate();rpc.join(5)
        self.assertTrue(entered.wait(3))
        bot.join(5);self.assertEqual(bot.exitcode,0)
        self.assertFalse(pacing.bot_priority_active())

    def test_multiple_rpc_reservations_and_bot_job_cancellation(self):
        with pacing.client_scope('rpc'):
            first=pacing.reserve_bot_priority();second=pacing.reserve_bot_priority()
            self.assertTrue(pacing.bot_priority_active())
            first.close();self.assertTrue(pacing.bot_priority_active())
            second.close();self.assertFalse(pacing.bot_priority_active())
        async def check():
            started=asyncio.Event()
            @pacing.bot_priority_job
            async def job():
                self.assertEqual(pacing.client_name(),'bot')
                started.set();await asyncio.sleep(100)
            task=asyncio.create_task(job());await started.wait()
            self.assertFalse(pacing.bot_priority_active())
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):await task
            self.assertFalse(pacing.bot_priority_active())
        asyncio.run(check())

    def test_rpc_error_releases_reservation(self):
        with pacing.client_scope('rpc'):
            with self.assertRaisesRegex(RuntimeError, 'request failed'):
                with pacing.request_slot():
                    self.assertTrue(pacing.bot_priority_active())
                    raise RuntimeError('request failed')
        self.assertFalse(pacing.bot_priority_active())
