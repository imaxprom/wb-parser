"""Durable bot jobs; completed keywords survive process/service restarts."""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time
import uuid

import config


class JobStore:
    def __init__(self):
        self.path = Path(config.DATA_DIR) / "position_queue.db"
        with self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY, work_key TEXT NOT NULL, uid INTEGER NOT NULL,
                    nm_id INTEGER NOT NULL, keywords TEXT NOT NULL, label TEXT NOT NULL,
                    result TEXT NOT NULL DEFAULT '{}', state TEXT NOT NULL DEFAULT 'queued',
                    retry_at REAL NOT NULL DEFAULT 0, generation REAL NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL, updated_at REAL NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS pending_work ON jobs(work_key)
                    WHERE state NOT IN ('done','cancelled');
            """)
        os.chmod(self.path, 0o600)

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=15)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def add(self, uid, nm_id, keywords, label):
        key = hashlib.sha256(json.dumps([uid, nm_id, keywords], ensure_ascii=False).encode()).hexdigest()
        now = time.time()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT id FROM jobs WHERE work_key=? AND state NOT IN ('done','cancelled')", (key,)).fetchone()
            if row:
                return row[0]
            job_id = uuid.uuid4().hex
            db.execute("INSERT INTO jobs(id,work_key,uid,nm_id,keywords,label,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                       (job_id, key, uid, nm_id, json.dumps(keywords, ensure_ascii=False), label, now, now))
            return job_id

    def pending(self):
        with self.connect() as db:
            return [{**dict(row), "keywords": json.loads(row["keywords"]), "result": json.loads(row["result"])}
                    for row in db.execute("SELECT * FROM jobs WHERE state NOT IN ('done','cancelled') ORDER BY created_at")]

    def save(self, task, state="running", generation=0):
        with self.connect() as db:
            db.execute("UPDATE jobs SET result=?,state=?,retry_at=?,generation=?,updated_at=? WHERE id=?",
                       (json.dumps(task.result, ensure_ascii=False), state, task.retry_at, generation, time.time(), task.job_id))

    def finish(self, job_id):
        with self.connect() as db:
            db.execute("UPDATE jobs SET state='done',updated_at=? WHERE id=?", (time.time(), job_id))

    def counts(self):
        with self.connect() as db:
            return {r[0]: r[1] for r in db.execute("SELECT state,count(*) FROM jobs GROUP BY state")}
