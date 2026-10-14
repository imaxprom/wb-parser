"""
Global position-checking queue with fair round-robin scheduling.
Only one article is checked at a time; HTTP pacing controls request delays.
"""

import asyncio
import logging
import fcntl
import os
from pathlib import Path
import time
from contextlib import nullcontext
from dataclasses import dataclass, field
from collections import defaultdict

import config
import wb_search_pacing
import wb_search_recovery
from wb_job_store import JobStore

logger = logging.getLogger(__name__)

# Dynamic import based on PARSE_MODE
if config.PARSE_MODE == "proxy":
    import proxy_positions as _positions_module
    logger.info("Queue worker using PROXY mode")
else:
    import chrome_positions as _positions_module
    logger.info("Queue worker using CHROME mode")

PAUSE_BETWEEN_TASKS = 0.0  # HTTP pacing already applies across article boundaries
MAX_PAUSE_RETRIES = 3
PAUSE_POLL_SECONDS = 2


@dataclass
class Task:
    uid: int
    nm_id: int
    keywords: list[str]
    future: asyncio.Future
    submitted_at: float = field(default_factory=time.time)
    label: str = ""  # e.g. SKU for logging
    pause_callbacks: list = field(default_factory=list)
    retry_at: float = 0
    priority_handle: object = field(default=None, repr=False)
    job_id: str | None = None
    result: dict = field(default_factory=dict)
    restored: bool = False
    retry_generation: float = 0


class PositionQueue:
    """Fair round-robin queue for position checks.

    - Tasks from different users are interleaved (round-robin)
    - No additional article delay by default; HTTP pacing controls request rate
    - Thread-safe via asyncio.Lock
    """

    def __init__(self, pause: float = PAUSE_BETWEEN_TASKS, *, durable=False):
        self._user_queues: dict[int, list[Task]] = defaultdict(list)
        self._lock = asyncio.Lock()
        self._event = asyncio.Event()
        self._running = False
        self._worker_task: asyncio.Task | None = None
        self._last_uid: int | None = None
        self._pause = pause
        self._total_processed = 0
        self._tasks: dict[tuple, Task] = {}
        self._durable = durable
        self._store = None
        self._process_lock = None
        self.on_progress = None
        self.on_restored_complete = None
        self.authorize = None

    async def start(self):
        """Start the background worker."""
        if not self._running:
            if self._durable:
                fd = os.open(Path(config.DATA_DIR) / "position_queue.lock", os.O_RDWR | os.O_CREAT, 0o600)
                self._process_lock = os.fdopen(fd, "a+")
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BaseException:
                    self._process_lock.close(); self._process_lock = None
                    raise
                self._store = JobStore()
                for row in self._store.pending():
                    task = Task(uid=row["uid"], nm_id=row["nm_id"], keywords=row["keywords"],
                                future=asyncio.get_running_loop().create_future(), label=row["label"],
                                submitted_at=row["created_at"], job_id=row["id"], result=row["result"],
                                restored=True, retry_at=row["retry_at"], retry_generation=row["generation"])
                    self._tasks[(task.uid, task.nm_id, tuple(task.keywords))] = task
                    self._user_queues[task.uid].append(task)
            self._running = True
            self._worker_task = asyncio.create_task(self._worker())
            logger.info("PositionQueue worker started (pause=%.1fs)", self._pause)

    async def stop(self):
        """Stop the background worker gracefully."""
        self._running = False
        self._event.set()
        if self._worker_task:
            try:
                await asyncio.wait_for(self._worker_task, timeout=10)
            except asyncio.TimeoutError:
                self._worker_task.cancel()
            self._worker_task = None
        for task in self._tasks.values():
            self._release_priority(task)
            if not task.future.done():
                task.future.cancel()
        self._tasks.clear()
        self._user_queues.clear()
        if self._process_lock:
            self._process_lock.close()
            self._process_lock = None
        logger.info("PositionQueue worker stopped")

    async def submit(self, uid: int, nm_id: int, keywords: list[str],
                     label: str = "", on_pause=None) -> asyncio.Future:
        """Submit a position check task. Returns a Future with the result dict."""
        loop = asyncio.get_running_loop()
        key = (uid, nm_id, tuple(keywords))
        async with self._lock:
            task = self._tasks.get(key)
            if task is None or task.future.done():
                task = Task(uid=uid, nm_id=nm_id, keywords=list(keywords),
                            future=loop.create_future(), label=label or str(nm_id),
                            priority_handle=wb_search_pacing.reserve_bot_priority())
                if self._store:
                    task.job_id = self._store.add(uid, nm_id, keywords, task.label)
                self._tasks[key] = task
                self._user_queues[uid].append(task)
            if on_pause is not None:
                task.pause_callbacks.append(on_pause)
        self._event.set()
        logger.info(
            "Task queued: uid=%s %s (%d keywords) | queue depth: %d",
            uid, task.label, len(keywords), self.pending_count
        )
        if on_pause is not None and task.retry_at > time.time():
            await self._notify_pause(task, [on_pause])
        # Cancelling one waiting screen must not cancel another subscriber.
        return asyncio.shield(task.future)

    async def _notify_pause(self, task, callbacks=None):
        for callback in list(task.pause_callbacks if callbacks is None else callbacks):
            try:
                await asyncio.wait_for(callback(task.retry_at), timeout=5)
            except Exception as exc:
                logger.warning("Pause status update failed: %s", type(exc).__name__)

    async def _wait_for_retry(self, task, generation):
        """Wait locally; this loop does not issue any WB requests."""
        while self._running:
            current = wb_search_recovery.session_generation()
            pause = wb_search_recovery.cooldown_error(current)
            if current != generation and pause is None:
                return True  # External login cleared this session's failure.
            deadline = task.retry_at
            if pause:
                deadline = max(deadline, pause.get("retry_at") or time.time() + pause["retry_after"])
            if deadline <= time.time():
                return True
            if deadline != task.retry_at:
                task.retry_at = deadline
                await self._notify_pause(task)
            self._event.clear()
            try:
                await asyncio.wait_for(self._event.wait(), timeout=min(PAUSE_POLL_SECONDS, max(.01, deadline - time.time())))
            except asyncio.TimeoutError:
                pass
        return False

    async def _progress(self, task, kw, item):
        value = dict(item)
        if task.job_id:
            value["_job_id"] = task.job_id
        task.result[kw] = value
        if self._store:
            self._store.save(task)
        if not value.get("error") and self.on_progress:
            await self.on_progress(task, kw, value)

    async def _execute(self, task):
        if self.authorize and not self.authorize(task.uid):
            task.result = {kw: {"error": True, "error_state": "access_revoked", "promo_pos": None,
                                "organic_pos": None, "is_advertised": False} for kw in task.keywords}
            return task.result
        result = task.result
        # Reapply idempotent history writes after a crash between the two DBs.
        if self.on_progress:
            for kw, value in list(result.items()):
                if not value.get("error"):
                    await self.on_progress(task, kw, value)
        pending = [kw for kw in task.keywords if kw not in result or result[kw].get("error")]
        retries = 0
        if pending and task.retry_at > time.time():
            if not await self._wait_for_retry(task, task.retry_generation):
                raise asyncio.CancelledError
        while pending and self._running:
            task.retry_at = 0
            callback_var = getattr(_positions_module, "_keyword_callback", None)
            token = callback_var.set(lambda kw, item: self._progress(task, kw, item)) if callback_var else None
            try:
                batch = await _positions_module.get_positions(task.nm_id, pending)
            finally:
                if callback_var:
                    callback_var.reset(token)
            for kw in pending:
                await self._progress(task, kw, batch.get(kw, {"error": True, "error_state": "invalid_response",
                    "promo_pos": None, "organic_pos": None, "is_advertised": False}))
            pending = [kw for kw in task.keywords if result.get(kw, {}).get("error")]
            errors = [result[kw] for kw in pending]
            if not errors:
                break
            login_required = any(e.get("recovery_reason") == "login_required" for e in errors)
            if login_required and not self._store:
                break
            deadline = max((e.get("retry_at") or time.time() + (e.get("retry_after") or 0) for e in errors), default=0)
            transient = any(e.get("error_state") in ("network_error", "auth_expired", "antibot", "rate_limited") for e in errors)
            if not any(e.get("retry_at") or e.get("retry_after") for e in errors):
                if not self._store or not transient:
                    break
                deadline = time.time() + min(60 * 2 ** min(retries, 6), 3600)
            if retries >= MAX_PAUSE_RETRIES and not self._store:
                for kw in pending:
                    result[kw] = {**result[kw], "automatic_retries_exhausted": True}
                break
            task.retry_at = max(time.time(), deadline)
            generation = next((e["retry_session_saved_at"] for e in errors if e.get("retry_session_saved_at") is not None),
                              wb_search_recovery.session_generation())
            task.retry_generation = generation
            if self._store:
                self._store.save(task, "waiting_login" if login_required else "waiting", generation)
            logger.info("Position task paused: article=%s retry_at=%.3f remaining_keys=%d attempt=%d",
                        task.nm_id, task.retry_at, len(pending), retries + 1)
            await self._notify_pause(task)
            if not await self._wait_for_retry(task, generation):
                break
            retries += 1
            logger.info("Position task resuming: article=%s remaining_keys=%d", task.nm_id, len(pending))
        task.retry_at = 0
        if not self._running:
            raise asyncio.CancelledError
        return result

    @property
    def pending_count(self) -> int:
        """Total pending tasks across all users."""
        return sum(len(q) for q in self._user_queues.values())

    @staticmethod
    def _release_priority(task):
        if task.priority_handle is not None:
            task.priority_handle.close()
            task.priority_handle = None

    def pending_for_user(self, uid: int) -> int:
        """Pending tasks for a specific user."""
        return len(self._user_queues.get(uid, []))

    def queue_info(self, uid: int) -> tuple[int, float]:
        """Estimate (position, wait_seconds) for next task of this user.

        With round-robin, the user waits for one task per other active user
        before their next task runs.
        """
        other_count = sum(
            1 for u, q in self._user_queues.items()
            if u != uid and q
        )
        # Position = other users ahead + 1
        position = other_count + 1
        # Estimated wait: each task ~ pause + 2 sec execution
        est_seconds = other_count * (self._pause + 2.0)
        return (position, est_seconds)

    async def _pick_next(self) -> Task | None:
        """Pick next task using fair round-robin across users."""
        async with self._lock:
            active_uids = sorted(
                uid for uid, tasks in self._user_queues.items() if tasks
            )
            if not active_uids:
                return None

            # Round-robin: pick next user after last served
            if self._last_uid in active_uids:
                idx = active_uids.index(self._last_uid)
                next_idx = (idx + 1) % len(active_uids)
            else:
                next_idx = 0

            uid = active_uids[next_idx]
            self._last_uid = uid

            task = self._user_queues[uid].pop(0)
            if not self._user_queues[uid]:
                del self._user_queues[uid]
            return task

    async def _worker(self):
        """Main worker loop: picks tasks, respects rate limits."""
        last_run = 0.0

        while self._running:
            task = await self._pick_next()

            if task is None:
                self._event.clear()
                await self._event.wait()
                continue

            # Rate limiting: pause between tasks
            elapsed = time.time() - last_run
            if elapsed < self._pause and last_run > 0:
                wait = self._pause - elapsed
                await asyncio.sleep(wait)

            # Execute
            logger.info(
                "Processing: uid=%s %s (%d keywords) | pending: %d",
                task.uid, task.label, len(task.keywords), self.pending_count
            )
            retry_task = False
            try:
                context = wb_search_pacing.client_scope("bot") if self._durable or task.priority_handle is not None else nullcontext()
                with context:
                    if task.priority_handle is None:
                        task.priority_handle = wb_search_pacing.reserve_bot_priority()
                    result = await self._execute(task)
                if self._store:
                    self._store.save(task, "completed")
                if task.restored and self.on_restored_complete:
                    await self.on_restored_complete(task, result)
                if self._store:
                    self._store.finish(task.job_id)
                if not task.future.cancelled():
                    task.future.set_result(result)
                logger.info("Done: %s (%.1fs since submit)",
                            task.label, time.time() - task.submitted_at)
            except asyncio.CancelledError:
                if self._running:
                    raise
                break
            except Exception as e:
                logger.error("Task failed: %s — %s", task.label, e)
                if self._store:
                    task.retry_at = time.time() + 60
                    task.retry_generation = wb_search_recovery.session_generation()
                    self._store.save(task, "waiting", task.retry_generation)
                    self._user_queues[task.uid].append(task)
                    self._event.set()
                    retry_task = True
                elif not task.future.cancelled():
                    task.future.set_exception(e)
            finally:
                self._release_priority(task)
                if retry_task:
                    continue
                self._tasks.pop((task.uid, task.nm_id, tuple(task.keywords)), None)
                if not task.future.done():
                    task.future.cancel()

            last_run = time.time()
            self._total_processed += 1

        logger.info("PositionQueue stopped (total processed: %d)",
                     self._total_processed)


# ── Global singleton ──
position_queue = PositionQueue(durable=True)
