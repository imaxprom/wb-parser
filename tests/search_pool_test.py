"""Independent QA. Temporary synthetic registry only; no HTTP or production."""
import concurrent.futures
from contextlib import contextmanager
import json
import multiprocessing
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, os.environ.get('WB_PARSER_ROOT', str(Path.cwd())))
import config
import wb_sessions as sessions
import wb_search_pool as pool
import wb_search_pacing as pacing
import wb_search_recovery as recovery


def gate_worker(directory, sid, start, out):
    config.DATA_DIR = directory
    start.wait(10)
    for _ in range(3):
        with pacing.client_scope('maintenance' if sid == 'D' else 'rpc'):
            with pacing.request_slot(session_id=sid):
                begun = time.monotonic()
                time.sleep(.03)
                out.put((sid, begun, time.monotonic()))


class ParallelTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        p = patch.object(config, 'DATA_DIR', self.temp.name)
        p.start(); self.addCleanup(p.stop)
        self.config = {'enabled': True, 'working': ['A', 'B', 'C'], 'reserve': ['D']}
        sessions.atomic_json(self.root / 'wb_search_pool.json', self.config)
        sessions.atomic_json(self.root / 'wb_search_pacing.json', {'enabled': True, 'name': 'qa', 'gap_ms': 0})
        sessions.initialize()
        self.ids = []
        for index in range(4):
            sid = sessions.create()
            sessions.publish(sid, self.bundle(index))
            self.ids.append(sid)
        self.config = {'enabled': True, 'working': self.ids[:3], 'reserve': self.ids[3:]}
        sessions.atomic_json(self.root / 'wb_search_pool.json', self.config)
        (self.root / 'search_pool').mkdir()

    @staticmethod
    def bundle(index):
        return {'session': {'saved_at': index+10, 'cookies': {'_wbauid': str(index), 'x_wbaas_token': 'synthetic-'+str(index)},
                            'localStorage': {'wbx__tokenData': json.dumps({'token': 'e30.eyJ1c2VyIjoxfQ.'+str(index)})}},
                'browser': {}, 'login': {}}

    def test_process_account_serialization_global_cap_and_gap(self):
        ctx = multiprocessing.get_context('spawn')
        start, out = ctx.Event(), ctx.Queue()
        processes = [ctx.Process(target=gate_worker,args=(self.temp.name,s,start,out)) for s in ['A','A','B','C','D']]
        try:
            for p in processes: p.start()
            start.set()
            events = [out.get(timeout=20) for _ in range(15)]
            for p in processes: p.join(5); self.assertEqual(p.exitcode,0)
            timeline = sorted([(start,1) for _,start,_ in events]+[(end,-1) for _,_,end in events])
            active = peak = 0
            for _, delta in timeline: active += delta; peak=max(peak,active)
            self.assertEqual(peak,3)
            gaps=[]
            for sid in ['A','B','C','D']:
                own = sorted((start,end) for s,start,end in events if s==sid)
                for before,after in zip(own,own[1:]):
                    gaps.append(after[0]-before[1]);self.assertGreaterEqual(gaps[-1],.05)
            print('QA gate metrics: peak=3, minimum_response_gap_ms='+str(round(min(gaps)*1000,3)),flush=True)
        finally:
            for p in processes:
                if p.is_alive(): p.terminate(); p.join(5)
            out.close()

    def test_legacy_pool_gap_and_no_untracked_completion(self):
        sessions.atomic_json(self.root / 'wb_search_pacing.json', {})
        for first,second in [(None,None), (None,'A'), ('A',None)]:
            with pacing.request_slot(session_id=first): ended=time.monotonic()
            with pacing.request_slot(session_id=second): begun=time.monotonic()
            self.assertGreaterEqual(begun-ended,.05,(first,second))

    def test_reserve_contention_only_one_failed_lane_gets_it(self):
        for sid in self.ids[:2]: sessions.record_health(sessions.snapshot(sid),{'state':'auth_expired','status':401})
        with concurrent.futures.ThreadPoolExecutor(2) as executor:
            selected=list(executor.map(lambda index:pool.select(self.config,index),[0,1]))
        self.assertEqual([r['id'] for r in selected if r],[self.ids[3]])
        self.assertEqual(len([r for r in selected if r]),1)

    def test_transport_health_does_not_consume_reserve(self):
        sessions.record_health(sessions.snapshot(self.ids[0]),{'state':'network_error','status':None})
        selected=pool.select(self.config,0)
        self.assertNotEqual(selected and selected['id'],self.ids[3])

    def test_actual_account_failure_retries_once_on_reserve(self):
        calls=[]
        def fetch(selected):
            calls.append(selected['id'])
            return {'error':True,'error_state':'auth_expired','status_code':401} if len(calls)==1 else {'ok':True}
        self.assertTrue(pool.execute(fetch).get('ok'))
        self.assertEqual(calls,[self.ids[0],self.ids[3]])
        self.assertEqual(sessions.snapshot()['id'],self.ids[0],'Pool does not change active Telegram account')

    def test_429_and_network_do_not_switch_account(self):
        for error in [{'error_state':'rate_limited','status_code':429}, {'error_state':'network_error'}, {'error_state':'auth_expired','status_code':401,'retry_after':15}]:
            calls=[]
            result=pool.execute(lambda selected: calls.append(selected['id']) or {'error':True,**error})
            self.assertEqual(len(calls),1)
            self.assertEqual(result['error_state'],error['error_state'])

    def test_global_server_pause_blocks_all_accounts_and_recovery(self):
        recovery.recover({'error_state':'rate_limited','status_code':429},sessions.generation(),allow_refresh=False)
        before=recovery._read(recovery._state_path())
        for sid in self.ids:
            for emergency in [False,True]:
                with self.assertRaises(pacing.SearchPaused):
                    with pacing.request_slot(session_id=sid,recovery=emergency): self.fail('HTTP admitted')
        self.assertEqual(recovery._read(recovery._state_path()),before)

    def test_bot_priority_blocks_pool_until_release(self):
        entered=threading.Event()
        def worker():
            with pacing.client_scope('rpc'),pacing.request_slot(session_id=self.ids[0]): entered.set()
        with concurrent.futures.ThreadPoolExecutor(1) as executor:
            with pacing.client_scope('bot'),pacing.bot_priority():
                future=executor.submit(worker)
                self.assertFalse(entered.wait(.15))
            future.result(3)
            self.assertTrue(entered.is_set())

    def test_snapshot_is_immutable_and_stale_revision_is_ignored(self):
        old=sessions.snapshot(self.ids[0])
        new=sessions.publish(self.ids[0],self.bundle(20))
        sessions.record_health(old,{'state':'auth_expired','status':401})
        row=next(r for r in sessions.list_sessions() if r['id']==self.ids[0])
        self.assertEqual(row['state'],'healthy')
        self.assertEqual(row['revision'],new['revision'])
        self.assertNotEqual(old['revision'],new['revision'])

    def test_health_write_revision_race(self):
        old=sessions.snapshot(self.ids[0]); original=sessions.connection; injected=False
        @contextmanager
        def racing_connection():
            nonlocal injected
            with original() as db:
                class Wrapper:
                    def execute(_,sql,params=()):
                        nonlocal injected
                        cursor=db.execute(sql,params)
                        if sql.startswith('SELECT * FROM sessions WHERE id=? AND revision=?') and not injected:
                            row=cursor.fetchone(); injected=True
                            with patch.object(sessions,'connection',original): sessions.publish(self.ids[0],self.bundle(30))
                            class Cursor:
                                def fetchone(self): return row
                            return Cursor()
                        return cursor
                    def __getattr__(_,name): return getattr(db,name)
                yield Wrapper()
        with patch.object(sessions,'connection',racing_connection):
            sessions.record_health(old,{'state':'auth_expired','status':401})
        row=next(r for r in sessions.list_sessions() if r['id']==self.ids[0])
        self.assertEqual(row['state'],'healthy','Stale result changed freshly published credentials')

    def test_probe_old_success_cannot_clear_newer_failure(self):
        import proxy_positions as parser
        value=sessions.snapshot(self.ids[0])
        @contextmanager
        def slot(**kwargs):
            yield None
            # The next admitted account request has already failed before the
            # old probe publishes its result after leaving the HTTP gate.
            sessions.record_health(value,{'state':'auth_expired','status':401})
        with patch.object(pacing,'request_slot',slot), patch.object(parser,'_search_unpaced_sync',return_value=({'products':[{'id':1}]},None)):
            sessions.probe(value)
        row=next(r for r in sessions.list_sessions() if r['id']==self.ids[0])
        self.assertEqual(row['state'],'auth_expired')

    def test_pool_skips_unavailable_lane_without_consuming_reserve(self):
        sessions.record_health(sessions.snapshot(self.ids[0]),{'state':'network_error'})
        used=[]
        result=pool.execute(lambda selected: used.append(selected['id']) or {'ok':True})
        self.assertEqual(used,[self.ids[1]])
        self.assertTrue(result['ok'])

    def test_native_http_and_actual_probe_share_account_gate(self):
        import proxy_positions as parser
        value=sessions.snapshot(self.ids[0]); events=[]; start=threading.Barrier(2)
        def fake(*args,**kwargs):
            begun=time.monotonic();time.sleep(.025);events.append((begun,time.monotonic()))
            return {'products':[{'id':1}]},None
        def native():
            start.wait(3)
            return parser._search_sync(parser._build_headers('__direct__',session_data=value['session']),{})
        def probe(): start.wait(3);return sessions.probe(value)
        with patch.object(parser,'_search_unpaced_sync',side_effect=fake), concurrent.futures.ThreadPoolExecutor(2) as executor:
            tasks=[executor.submit(native),executor.submit(probe)]
            for task in tasks: task.result(5)
        events.sort();self.assertEqual(len(events),2)
        self.assertGreaterEqual(events[1][0]-events[0][1],.05)

    def test_malformed_configuration_fails_closed(self):
        for working,reserve in [(None,['D']),(['A','B','C'],None),(['A','A','C'],['D']),(['A','B','C'],['../D'])]:
            sessions.atomic_json(self.root/'wb_search_pool.json',{'enabled':True,'working':working,'reserve':reserve})
            self.assertIsNone(pool.settings())


if __name__=='__main__': unittest.main(verbosity=2)
