"""
Global position-checking queue with fair round-robin scheduling.
Only one article is checked at a time, with a short pause between articles.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from collections import defaultdict

import config
import wb_search_recovery

logger = logging.getLogger(__name__)

# Dynamic import based on PARSE_MODE
if config.PARSE_MODE == "proxy":
    import proxy_positions as _positions_module
    logger.info("Queue worker using PROXY mode")
else:
    import chrome_positions as _positions_module
    logger.info("Queue worker using CHROME mode")

PAUSE_BETWEEN_TASKS = 3.0  # seconds between article checks
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


class PositionQueue:
    """Fair round-robin queue for position checks.

    - Tasks from different users are interleaved (round-robin)
    - Rate limit: minimum PAUSE_BETWEEN_TASKS between article checks
    - Thread-safe via asyncio.Lock
    """

    def __init__(self, pause: float = PAUSE_BETWEEN_TASKS):
        self._user_queues: dict[int, list[Task]] = defaultdict(list)
        self._lock = asyncio.Lock()
        self._event = asyncio.Event()
        self._running = False
        self._worker_task: asyncio.Task | None = None
        self._last_uid: int | None = None
        self._pause = pause
        self._total_processed = 0
        self._tasks: dict[tuple, Task] = {}

    async def start(self):
        """Start the background worker."""
        if not self._running:
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
            if not task.future.done():
                task.future.cancel()
        self._tasks.clear()
        self._user_queues.clear()
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
                            future=loop.create_future(), label=label or str(nm_id))
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

    async def _execute(self, task):
        result = {}
        pending = list(task.keywords)
        retries = 0
        while pending and self._running:
            task.retry_at = 0
            batch = await _positions_module.get_positions(task.nm_id, pending)
            for kw in pending:
                result[kw] = batch.get(kw, {"error": True, "error_state": "invalid_response",
                    "promo_pos": None, "organic_pos": None, "is_advertised": False})
            pending = [kw for kw in task.keywords if result.get(kw, {}).get("error")]
            errors = [result[kw] for kw in pending]
            if not errors or any(e.get("recovery_reason") == "login_required" for e in errors):
                break
            deadline = max((e.get("retry_at") or time.time() + (e.get("retry_after") or 0) for e in errors), default=0)
            if not any(e.get("retry_at") or e.get("retry_after") for e in errors):
                break
            if retries >= MAX_PAUSE_RETRIES:
                for kw in pending:
                    result[kw] = {**result[kw], "automatic_retries_exhausted": True}
                break
            task.retry_at = max(time.time(), deadline)
            generation = next((e["retry_session_saved_at"] for e in errors if e.get("retry_session_saved_at") is not None),
                              wb_search_recovery.session_generation())
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
            try:
                result = await self._execute(task)
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
                if not task.future.cancelled():
                    task.future.set_exception(e)
            finally:
                self._tasks.pop((task.uid, task.nm_id, tuple(task.keywords)), None)
                if not task.future.done():
                    task.future.cancel()

            last_run = time.time()
            self._total_processed += 1

        logger.info("PositionQueue stopped (total processed: %d)",
                     self._total_processed)


# ── Global singleton ──
position_queue = PositionQueue()
