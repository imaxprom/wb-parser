"""
Global position-checking queue with fair round-robin scheduling.
Ensures concurrent requests from multiple users don't cause WB rate limits.

Based on rate limit tests (2026-04-02):
- Burst up to 50 requests: OK
- Between articles: 3 sec pause needed
- On 429: retry after 5s (handled by chrome_positions)
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from collections import defaultdict

import config

logger = logging.getLogger(__name__)

# Dynamic import based on PARSE_MODE
if config.PARSE_MODE == "proxy":
    import proxy_positions as _positions_module
    logger.info("Queue worker using PROXY mode")
else:
    import chrome_positions as _positions_module
    logger.info("Queue worker using CHROME mode")

PAUSE_BETWEEN_TASKS = 3.0  # seconds between chrome_positions calls


@dataclass
class Task:
    uid: int
    nm_id: int
    keywords: list[str]
    future: asyncio.Future
    submitted_at: float = field(default_factory=time.time)
    label: str = ""  # e.g. SKU for logging


class PositionQueue:
    """Fair round-robin queue for Chrome position checks.

    - Tasks from different users are interleaved (round-robin)
    - Rate limit: minimum PAUSE_BETWEEN_TASKS between Chrome calls
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
        logger.info("PositionQueue worker stopped")

    async def submit(self, uid: int, nm_id: int, keywords: list[str],
                     label: str = "") -> asyncio.Future:
        """Submit a position check task. Returns a Future with the result dict."""
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        task = Task(uid=uid, nm_id=nm_id, keywords=keywords,
                    future=future, label=label or str(nm_id))
        async with self._lock:
            self._user_queues[uid].append(task)
        self._event.set()
        logger.info(
            "Task queued: uid=%s %s (%d keywords) | queue depth: %d",
            uid, task.label, len(keywords), self.pending_count
        )
        return future

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
                result = await _positions_module.get_positions(
                    task.nm_id, task.keywords
                )
                if not task.future.cancelled():
                    task.future.set_result(result)
                logger.info("Done: %s (%.1fs since submit)",
                            task.label, time.time() - task.submitted_at)
            except Exception as e:
                logger.error("Task failed: %s — %s", task.label, e)
                if not task.future.cancelled():
                    task.future.set_exception(e)

            last_run = time.time()
            self._total_processed += 1

        logger.info("PositionQueue stopped (total processed: %d)",
                     self._total_processed)


# ── Global singleton ──
position_queue = PositionQueue()
