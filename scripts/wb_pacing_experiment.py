"""Run six ten-active-minute pacing trials on existing bot/RPC traffic.

No synthetic WB requests. Server/local cooldowns are excluded from active time.
"""
import collections
import csv
import datetime
import json
import os
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config


def read(path, default=None):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {} if default is None else default


def atomic(path, value):
    temp = path.with_suffix(f".tmp.{os.getpid()}")
    temp.write_text(json.dumps(value, indent=2))
    os.chmod(temp, 0o600)
    os.replace(temp, path)


def pause_seconds(events, start, end):
    """Integrate effective cooldown until replaced by the next state event."""
    total = 0.0
    events = sorted(events, key=lambda e: e["at"])
    for index, event in enumerate(events):
        following = events[index + 1]["at"] if index + 1 < len(events) else end
        left = max(start, event["at"])
        right = min(end, following, event.get("retry_at", 0))
        total += max(0, right - left)
    return total


def select_best(rows):
    candidates = [r for r in rows if r["successes"] >= 50]
    if not candidates:
        return rows[0]
    # Prefer a regime with no rejections; otherwise minimize rejection rate.
    return min(candidates, key=lambda r: (r["errors"] / max(1, r["requests"]), -r["successes_per_wall_minute"]))


class Recorder:
    def __init__(self, directory):
        self.root = Path(config.DATA_DIR)
        self.directory = directory
        self.offsets = {}
        self.requests = []
        self.events = []

    def poll(self):
        sources = [(p, "requests.jsonl", self.requests) for p in (self.root / "wb_search_requests").glob("*.jsonl")]
        sources.append((self.root / "wb_search_recovery_events.jsonl", "recovery_events.jsonl", self.events))
        for path, target, records in sources:
            if not path.exists():
                continue
            with path.open() as handle:
                handle.seek(self.offsets.get(str(path), 0))
                while True:
                    before = handle.tell()
                    line = handle.readline()
                    if not line or not line.endswith("\n"):
                        handle.seek(before)
                        break
                    record = json.loads(line)
                    records.append(record)
                    with (self.directory / target).open("a") as output:
                        output.write(line)
                self.offsets[str(path)] = handle.tell()


def summarize(policy, start, end, recorder):
    records = [r for r in recorder.requests if start <= r["started_at"] < end
               and r.get("pacing", {}).get("name") == policy["name"]]
    paused = pause_seconds(recorder.events, start, end)
    statuses = collections.Counter(str(r["status"]) for r in records)
    good = statuses.get("200", 0)
    return {"name": policy["name"], "policy": policy, "start_at": start, "end_at": end,
            "wall_seconds": end - start, "paused_seconds": paused,
            "active_seconds": end - start - paused, "requests": len(records),
            "successes": good, "errors": len(records) - good, "rate_limits": statuses.get("429", 0),
            "statuses": dict(statuses), "successes_per_wall_minute": good * 60 / max(1, end - start)}


def save_table(directory, rows):
    fields = ("name", "gap_ms", "batch_size", "batch_pause_ms", "active_minutes", "pause_minutes", "wall_minutes", "requests", "http_200", "http_429", "other_errors", "successes_per_wall_minute")
    with (directory / "results.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for r in rows:
            writer.writerow({"name": r["name"], **{k: r["policy"].get(k, 0) for k in fields[1:4]},
                             "active_minutes": round(r["active_seconds"] / 60, 3),
                             "pause_minutes": round(r["paused_seconds"] / 60, 3),
                             "wall_minutes": round(r["wall_seconds"] / 60, 3),
                             "requests": r["requests"], "http_200": r["successes"], "http_429": r["rate_limits"],
                             "other_errors": r["errors"] - r["rate_limits"],
                             "successes_per_wall_minute": round(r["successes_per_wall_minute"], 2)})
    atomic(directory / "results.json", rows)
    lines = ["# Результаты эксперимента", "", "Последняя строка может быть текущим незавершённым этапом.", "",
             "| Режим | Пауза после ответа, мс | Пачка / пауза между пачками, мс | Активно, мин | Исключённые паузы, мин | HTTP 200 | HTTP 429 | Успешных / мин с учётом простоя |",
             "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for r in rows:
        p = r["policy"]
        lines.append(f"| {r['name']} | {p['gap_ms']} | {p.get('batch_size', 1)} / {p.get('batch_pause_ms', 0)} | {r['active_seconds']/60:.2f} | {r['paused_seconds']/60:.2f} | {r['successes']} | {r['rate_limits']} | {r['successes_per_wall_minute']:.2f} |")
    (directory / "TABLE.md").write_text("\n".join(lines) + "\n")


def main():
    root = Path(config.DATA_DIR)
    directory = root / "pacing-experiment-20261002"
    directory.mkdir(exist_ok=True)
    if (directory / "controller_state.json").exists():
        raise RuntimeError("An experiment already exists; do not overwrite it")
    control = root / "wb_search_pacing.json"
    stages = [{"name": "serial_1500", "gap_ms": 1500},
              {"name": "serial_50", "gap_ms": 50},
              {"name": "serial_750", "gap_ms": 750},
              {"name": "serial_250", "gap_ms": 250},
              {"name": "batch4", "gap_ms": 50, "batch_size": 4, "batch_pause_ms": 5000}]
    recorder = Recorder(directory)
    recorder.poll()
    rows = []
    overall_start = time.time()
    next_message = 0
    try:
        for index in range(6):
            if index == 5:
                winner = select_best(rows)
                base = {**winner["policy"], "name": "confirm_" + winner["name"]}
            else:
                base = stages[index]
            policy = {"enabled": True, "batch_size": 1, "batch_pause_ms": 0, **base, "experiment": True}
            start = time.time()
            atomic(control, {**policy, "expires_at": time.time() + 120})
            print(json.dumps({"event": "stage_start", "at": start, "policy": policy}), flush=True)
            next_lease = 0
            while True:
                now = time.time()
                recorder.poll()
                current = summarize(policy, start, now, recorder)
                if now >= next_lease:
                    atomic(control, {**policy, "expires_at": now + 120})
                    save_table(directory, rows + [current])
                    atomic(directory / "controller_state.json", {"status": "running", "start_at": overall_start,
                           "at": now, "stage": index + 1, "policy": policy,
                           "total_active_seconds": sum(r["active_seconds"] for r in rows) + current["active_seconds"],
                           "total_paused_seconds": sum(r["paused_seconds"] for r in rows) + current["paused_seconds"]})
                    next_lease = now + 10
                if now >= next_message:
                    recovery = read(root / "wb_search_recovery.json")
                    print(json.dumps({"event": "progress", "at": now, "stage": index + 1,
                          "name": policy["name"], "active_seconds": round(current["active_seconds"], 1),
                          "paused_seconds": round(current["paused_seconds"], 1), "requests": current["requests"],
                          "statuses": current["statuses"], "pause_remaining": max(0, round(recovery.get("retry_at", 0) - now))}), flush=True)
                    next_message = now + 45
                if current["active_seconds"] >= 600:
                    rows.append(current)
                    save_table(directory, rows)
                    print(json.dumps({"event": "stage_complete", **current}), flush=True)
                    break
                recovery = read(root / "wb_search_recovery.json")
                time.sleep(2 if recovery.get("retry_at", 0) > now else min(2, 600 - current["active_seconds"]))
        # Wait only for an in-flight response to be audited; no new WB requests.
        final_end = time.time()
        atomic(control, {**select_best(rows)["policy"], "experiment": False})
        time.sleep(11)
        recorder.poll()
        rows = [summarize(r["policy"], r["start_at"], r["end_at"], recorder) for r in rows]
        winner = select_best(rows)
        final_policy = {**winner["policy"], "experiment": False}
        atomic(control, final_policy)
        save_table(directory, rows)
        final = {"status": "complete", "start_at": overall_start, "end_at": final_end,
                 "total_active_seconds": sum(r["active_seconds"] for r in rows),
                 "total_paused_seconds": sum(r["paused_seconds"] for r in rows), "selected_policy": final_policy}
        atomic(directory / "controller_state.json", final)
        print(json.dumps({"event": "complete", **final}), flush=True)
    except BaseException:
        atomic(control, {"enabled": True, "name": "fallback", "gap_ms": 1500, "experiment": False})
        raise


if __name__ == "__main__":
    main()
