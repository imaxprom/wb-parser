import unittest
from scripts.wb_pacing_experiment import pause_seconds, select_best


class PacingExperimentTest(unittest.TestCase):
    def test_pause_excludes_only_effective_part_and_handles_early_success(self):
        events = [{"at": 10, "retry_at": 70}, {"at": 90, "retry_at": 210},
                  {"at": 200, "retry_at": 0}]
        self.assertEqual(pause_seconds(events, 0, 240), 170)
        self.assertEqual(pause_seconds(events, 30, 100), 50)

    def test_extending_pause_does_not_double_count(self):
        events = [{"at": 10, "retry_at": 70}, {"at": 20, "retry_at": 100}]
        self.assertEqual(pause_seconds(events, 0, 120), 90)

    def test_best_requires_traffic_and_prefers_no_errors(self):
        rows = [{"name": "idle", "successes": 1, "requests": 1, "errors": 0, "successes_per_wall_minute": 1},
                {"name": "fast", "successes": 1000, "requests": 1001, "errors": 1, "successes_per_wall_minute": 80},
                {"name": "steady", "successes": 300, "requests": 300, "errors": 0, "successes_per_wall_minute": 30}]
        self.assertEqual(select_best(rows)["name"], "steady")
