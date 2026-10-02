"""Per-request metadata shared by bot and short-lived search RPC processes."""

from datetime import datetime, timezone
import itertools
import json
import logging
import os
from pathlib import Path
import sys
import time

import config

logger = logging.getLogger(__name__)
_sequence = itertools.count(1)


def record(*, started_at, elapsed_ms, status, generation, response_id,
           retry_after, params, has_proxy):
    # Never accept headers, response bodies, query strings or command arguments.
    def number(name):
        try:
            return int(params.get(name))
        except (TypeError, ValueError):
            return None

    now = time.time()
    program = Path(getattr(sys.modules.get("__main__"), "__file__", "interactive")).name
    event = {"at": now, "started_at": started_at, "elapsed_ms": elapsed_ms,
             "pid": os.getpid(), "request_number": next(_sequence),
             "program": program[:80], "status": status, "session_saved_at": generation,
             "response_id": response_id, "retry_after": retry_after,
             "page": number("page"), "dest": number("dest"),
             "no_promo": params.get("ab_testid") == "no_promo",
             "has_proxy": bool(has_proxy)}
    try:
        directory = Path(config.DATA_DIR) / "wb_search_requests"
        directory.mkdir(mode=0o700, exist_ok=True)
        path = directory / (datetime.fromtimestamp(now, timezone.utc).strftime("%Y-%m-%d") + ".jsonl")
        # One O_APPEND write per event prevents interleaving between processes.
        fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
        try:
            os.write(fd, (json.dumps(event) + "\n").encode())
        finally:
            os.close(fd)
    except OSError as exc:
        logger.warning("WB search request audit unavailable: %s", type(exc).__name__)
