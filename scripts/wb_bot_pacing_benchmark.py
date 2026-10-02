"""Compare complete bot queue workloads, without Telegram sends or result writes."""
import asyncio
import collections
import hashlib
import json
import logging
import os
from pathlib import Path
import sqlite3
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config
import wb_search_pacing as pacing
import wb_search_recovery as recovery
from queue_worker import PositionQueue
from scripts.wb_pacing_experiment import Recorder, atomic, pause_seconds, read

logging.disable(logging.CRITICAL)  # Only the explicit safe JSON metadata below.


def workload():
    with sqlite3.connect('file:' + str(Path(config.DB_PATH).resolve()) + '?mode=ro', uri=True) as db:
        owner = db.execute('SELECT folder_name FROM allowed_users WHERE is_owner=1 ORDER BY id LIMIT 1').fetchone()
    if not owner:
        raise RuntimeError('No owner workload')
    path = Path(config.DATA_DIR) / 'users' / owner[0] / 'user.db'
    jobs = []
    with sqlite3.connect('file:' + str(path.resolve()) + '?mode=ro', uri=True) as db:
        for aid, sku in db.execute('SELECT id,sku FROM articles ORDER BY id').fetchall():
            if not str(sku).isdigit():
                continue
            keys = [r[0] for r in db.execute('SELECT query FROM queries WHERE article_id=? ORDER BY sort_order,id LIMIT 6', (aid,))]
            if len(keys) == 6:
                jobs.append((int(sku), keys))
            if len(jobs) == 5:
                break
    if len(jobs) != 5:
        raise RuntimeError('Five six-keyword articles required for this comparison')
    return jobs


class MeasuredQueue(PositionQueue):
    def __init__(self):
        super().__init__()
        self.measurements = []

    async def _execute(self, task):
        started = time.time()
        try:
            return await super()._execute(task)
        finally:
            self.measurements.append({'slot': len(self.measurements)+1,
                'queue_wait_seconds': started-task.submitted_at,
                'processing_seconds': time.time()-started})


async def run_jobs(jobs, queue, row, recorder):
    for index, (sku, keys) in enumerate(jobs):
        async def paused(deadline, slot=index+1):
            row['pause_notifications'].append({'at':time.time(),'slot':slot,'retry_at':deadline})
        future = await queue.submit(1, sku, keys, label='benchmark', on_pause=paused)
        result = await future
        good = sum(not result.get(k, {}).get('error', True) for k in keys)
        row['completed_keywords'] += good
        row['attempted_articles'] += 1
        row['completed_articles'] += int(good == len(keys))
        recorder.poll()
        print(json.dumps({'event':'article_complete','run':row['name'],'slot':index+1,
            'successful_keywords':good,'elapsed_seconds':round(time.time()-row['start_at'],2)}),flush=True)
        if good != len(keys):
            row['outcome'] = 'incomplete'
            break  # Same stopping rule as the main bot's bulk handler.


def summarize(row, recorder, now):
    stop = row.get('end_at',now)
    requests = [r for r in recorder.requests if row['start_at']<=r['started_at']<stop]
    own = [r for r in requests if r['pid']==os.getpid()]
    others = [r for r in requests if r['pid']!=os.getpid()]
    return {**row,'elapsed_seconds':stop-row['start_at'],
        'shared_cooldown_seconds':pause_seconds(recorder.events,row['start_at'],stop),
        'own_http':dict(collections.Counter(str(r['status']) for r in own)),
        'other_http':dict(collections.Counter(str(r['status']) for r in others)),
        'other_programs':dict(collections.Counter(r['program'] for r in others))}


def save(directory, rows):
    atomic(directory/'results.json',rows)
    lines=['# Время обработки одинакового набора через очередь бота','',
           '5 товаров × 6 ключей; время включает очередь и блокировки. Сайт: 50 мс в обоих вариантах.',
           'Статус running означает незавершённую попытку; неполные результаты нельзя считать выигрышем скорости.','',
           '| Попытка | Режим | Готовых ключей | Полное время, с | Из него cooldown, с | HTTP 200 / 429 теста | HTTP 200 / 429 других клиентов | Статус |',
           '|---|---|---:|---:|---:|---:|---:|---|']
    for r in rows:
        a,b=r['own_http'],r['other_http']
        lines.append(f"| {r['name']} | {r['mode']} | {r['completed_keywords']}/30 | {r['elapsed_seconds']:.2f} | {r['shared_cooldown_seconds']:.2f} | {a.get('200',0)} / {a.get('429',0)} | {b.get('200',0)} / {b.get('429',0)} | {r['outcome']} |")
    (directory/'TABLE.md').write_text('\n'.join(lines)+'\n')


async def main():
    jobs=workload()
    directory=Path(config.DATA_DIR)/'bot-pacing-20261002'
    directory.mkdir(exist_ok=True)
    if (directory/'state.json').exists():
        raise RuntimeError('Existing benchmark; refusing duplicate run')
    control=Path(config.DATA_DIR)/'wb_search_pacing.json'
    original=read(control)
    atomic(directory/'original_policy.json',original)
    stable={'enabled':True,'name':'site_serial_50','gap_ms':50,'batch_size':1,'batch_pause_ms':0,
            'experiment':False,'profiles':{'bot':{'name':'bot_batch4','gap_ms':50,'batch_size':4,'batch_pause_ms':5000}}}
    fingerprint=hashlib.sha256(json.dumps(jobs,ensure_ascii=False).encode()).hexdigest()
    state={'status':'running','started_at':time.time(),'pid':os.getpid(),'workload_sha256':fingerprint,
           'articles':5,'keywords':30,'order':['batch','serial50','serial50','batch']}
    atomic(directory/'state.json',state)
    atomic(control,stable)
    recorder=Recorder(directory)
    recorder.poll()
    raw_rows=[]
    try:
        for index, mode in enumerate(state['order']):
            before=time.time()
            while recovery.cooldown_error(recovery.session_generation()):
                if time.time()-before>1200:
                    raise TimeoutError('Existing cooldown prevented the next comparison run')
                atomic(directory/'state.json',{**state,'phase':'waiting_for_existing_cooldown','at':time.time()})
                print(json.dumps({'event':'waiting_before_run','run':index+1,'seconds':round(time.time()-before)}),flush=True)
                await asyncio.sleep(30)
            name=f'bot_{index+1}_{mode}'
            profile={'name':name,'gap_ms':50,'batch_size':4 if mode=='batch' else 1,
                     'batch_pause_ms':5000 if mode=='batch' else 0,'experiment':False}
            atomic(control,{**stable,'profiles':{**stable['profiles'],'benchmark':{**profile,'expires_at':time.time()+120}}})
            row={'name':name,'mode':mode,'start_at':time.time(),'pre_run_wait_seconds':time.time()-before,
                 'completed_keywords':0,'attempted_articles':0,'completed_articles':0,'pause_notifications':[],
                 'outcome':'running','policy':profile}
            with pacing.client_scope('benchmark'):
                queue=MeasuredQueue()
                await queue.start()
                job=asyncio.create_task(run_jobs(jobs,queue,row,recorder))
                next_message=0
                try:
                    while not job.done():
                        now=time.time()
                        if now-row['start_at']>1200:
                            row['outcome']='timeout'
                            job.cancel()
                            break
                        atomic(control,{**stable,'profiles':{**stable['profiles'],'benchmark':{**profile,'expires_at':now+120}}})
                        recorder.poll()
                        save(directory,[summarize(r,recorder,now) for r in raw_rows]+[summarize(row,recorder,now)])
                        atomic(directory/'state.json',{**state,'phase':name,'at':now})
                        if now>=next_message:
                            current=summarize(row,recorder,now)
                            print(json.dumps({'event':'progress','run':name,'elapsed':round(current['elapsed_seconds'],1),
                                  'completed_keywords':row['completed_keywords'],'own_http':current['own_http'],
                                  'other_http':current['other_http'],'cooldown_seconds':round(current['shared_cooldown_seconds'],1)}),flush=True)
                            next_message=now+45
                        await asyncio.wait([job],timeout=5)
                    if row['outcome']!='timeout':
                        await job
                        if row['outcome']=='running':row['outcome']='complete'
                    else:
                        await asyncio.gather(job,return_exceptions=True)
                finally:
                    row['end_at']=time.time()
                    await queue.stop()
                    row['articles_timing']=queue.measurements
            recorder.poll()
            raw_rows.append(row)
            save(directory,[summarize(r,recorder,time.time()) for r in raw_rows])
            print(json.dumps({'event':'run_complete',**summarize(row,recorder,time.time())}),flush=True)
        atomic(directory/'state.json',{**state,'status':'complete','ended_at':time.time()})
        print(json.dumps({'event':'complete','runs':len(raw_rows)}),flush=True)
    except BaseException as exc:
        atomic(directory/'state.json',{**state,'status':'interrupted','ended_at':time.time(),
                                      'error_type':type(exc).__name__})
        raise
    finally:
        # Keep the requested website setting; do not select the bot's mode
        # automatically before the user reviews these end-to-end results.
        atomic(control,stable)


if __name__=='__main__':
    asyncio.run(main())
