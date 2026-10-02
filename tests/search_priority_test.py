import asyncio
import json
import multiprocessing
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

import wb_search_pacing as pacing


def rpc_pair(directory, in_flight, finish_current, output):
    pacing.config.DATA_DIR=directory
    with pacing.client_scope('rpc'):
        with pacing.request_slot():
            output.put(('rpc1',time.monotonic()))
            in_flight.set()
            if not finish_current.wait(10):raise TimeoutError()
        with pacing.request_slot():output.put(('rpc2',time.monotonic()))


def bot_check(directory, claimed, output):
    pacing.config.DATA_DIR=directory
    with pacing.client_scope('bot'), pacing.bot_priority():
        claimed.set()
        for i in range(3):
            with pacing.request_slot():output.put(('bot'+str(i),time.monotonic()))
            time.sleep(.04)
        output.put(('bot_done',time.monotonic()))


def idle_bot(directory, claimed, release):
    pacing.config.DATA_DIR=directory
    with pacing.client_scope('bot'), pacing.bot_priority():
        claimed.set()
        release.wait(10)


def waiting_rpc(directory, entering, entered):
    pacing.config.DATA_DIR=directory
    with pacing.client_scope('rpc'):
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

    def test_bot_preempts_after_current_http_and_keeps_whole_job_priority(self):
        in_flight=self.ctx.Event();finish=self.ctx.Event();claimed=self.ctx.Event();out=self.ctx.Queue()
        rpc=self.start(rpc_pair,in_flight,finish,out)
        self.assertTrue(in_flight.wait(5))
        bot=self.start(bot_check,claimed,out)
        self.assertTrue(claimed.wait(5))
        self.assertTrue(rpc.is_alive())
        self.assertTrue(pacing.bot_priority_active())
        finish.set()
        events=sorted([out.get(timeout=5) for _ in range(6)],key=lambda e:e[1])
        for process in (bot,rpc):process.join(5);self.assertEqual(process.exitcode,0)
        self.assertEqual([e[0] for e in events],['rpc1','bot0','bot1','bot2','bot_done','rpc2'])
        self.assertFalse(pacing.bot_priority_active())
        out.close()

    def test_crashed_bot_releases_website_without_stale_deadline(self):
        claimed=self.ctx.Event();release=self.ctx.Event();entering=self.ctx.Event();entered=self.ctx.Event()
        bot=self.start(idle_bot,claimed,release)
        self.assertTrue(claimed.wait(5))
        rpc=self.start(waiting_rpc,entering,entered)
        self.assertTrue(entering.wait(5))
        self.assertFalse(entered.wait(.15))
        bot.terminate();bot.join(5)
        self.assertTrue(entered.wait(3))
        rpc.join(5);self.assertEqual(rpc.exitcode,0)
        self.assertFalse(pacing.bot_priority_active())

    def test_priority_still_applies_if_optional_pacing_is_disabled(self):
        (self.root/'wb_search_pacing.json').write_text('{}')
        claimed=self.ctx.Event();release=self.ctx.Event();entering=self.ctx.Event();entered=self.ctx.Event()
        bot=self.start(idle_bot,claimed,release)
        self.assertTrue(claimed.wait(5))
        rpc=self.start(waiting_rpc,entering,entered)
        self.assertTrue(entering.wait(5));self.assertFalse(entered.wait(.15))
        release.set();self.assertTrue(entered.wait(3))
        for process in (bot,rpc):process.join(5);self.assertEqual(process.exitcode,0)

    def test_multiple_bot_reservations_and_job_cancellation(self):
        with pacing.client_scope('bot'):
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
            self.assertTrue(pacing.bot_priority_active())
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):await task
            self.assertFalse(pacing.bot_priority_active())
        asyncio.run(check())
