from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import proxy_positions as positions
import wb_search_audit as audit


class SearchAuditTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        patched = patch.object(audit.config, "DATA_DIR", str(self.directory))
        patched.start()
        self.addCleanup(patched.stop)

    def events(self):
        return [json.loads(line) for p in (self.directory / "wb_search_requests").glob("*.jsonl")
                for line in p.read_text().splitlines()]

    def request(self, response):
        with patch.object(positions.curl_requests, "get", return_value=response):
            return positions._search_sync({"Authorization": "private-header"},
                {"query": "private-query", "page": "2", "dest": "-951305", "ab_testid": "no_promo"},
                proxy_url="http://private-proxy")

    def test_success_and_failure_are_recorded_without_secrets_or_info_logging(self):
        with patch.object(positions.logger, "info"):
            self.request(SimpleNamespace(status_code=200, headers={}, json=lambda: {"products": []}))
            _, error = self.request(SimpleNamespace(status_code=429, headers={"Retry-After": "123"}))
        events = self.events()
        self.assertEqual([e["status"] for e in events], [200, 429])
        self.assertEqual(events[1]["response_id"], error["response_id"])
        self.assertEqual(events[1]["retry_after"], 123)
        self.assertEqual(events[0]["page"], 2)
        self.assertEqual(events[0]["dest"], -951305)
        self.assertTrue(events[0]["no_promo"] and events[0]["has_proxy"])
        self.assertNotIn("private", json.dumps(events))
        self.assertLess(events[0]["request_number"], events[1]["request_number"])
        self.assertLessEqual(events[0]["started_at"], events[0]["at"])

    def test_network_failure_and_invalid_json_are_not_double_counted(self):
        with patch.object(positions.curl_requests, "get", side_effect=TimeoutError("private-error")):
            positions._search_sync({}, {})
        def invalid_json():
            raise ValueError("private-body")
        self.request(SimpleNamespace(status_code=200, headers={}, json=invalid_json))
        self.assertEqual([e["status"] for e in self.events()], [None, 200])
        self.assertNotIn("private", json.dumps(self.events()))

    def test_audit_disk_failure_does_not_turn_successful_search_into_error(self):
        with patch.object(audit.os, "open", side_effect=OSError("private-path")):
            data, error = self.request(SimpleNamespace(status_code=200, headers={}, json=lambda: {"ok": True}))
        self.assertIsNone(error)
        self.assertEqual(data, {"ok": True})

    def test_concurrent_writes_remain_separate_json_events(self):
        def write(_):
            audit.record(started_at=1, elapsed_ms=2, status=200, generation=3,
                         response_id=None, retry_after=0, params={}, has_proxy=False)
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(write, range(100)))
        events = self.events()
        self.assertEqual(len(events), 100)
        self.assertEqual(len({e["request_number"] for e in events}), 100)
