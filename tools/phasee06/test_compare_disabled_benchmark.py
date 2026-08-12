from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import compare_disabled_benchmark


class DisabledBenchmarkCompareTest(unittest.TestCase):
    def write_rows(self, path: Path, values: list[float]) -> None:
        rows = [
            {
                "case": "h2d",
                "event_id": index + 1,
                "status": "complete",
                "warmup": False,
                "actual_latency_us": value,
                "client_wall_us": value + 10,
            }
            for index, value in enumerate(values)
        ]
        path.write_text(json.dumps({"events": rows}), encoding="utf-8")

    def test_passes_under_threshold(self) -> None:
        with tempfile.TemporaryDirectory(prefix="phasee06_disabled_") as directory:
            root = Path(directory)
            baseline = root / "baseline.json"
            candidate = root / "candidate.json"
            self.write_rows(baseline, [100, 100, 100, 100, 100])
            self.write_rows(candidate, [102, 102, 102, 102, 102])
            report, errors = compare_disabled_benchmark.compare(baseline, candidate, 3.0, 5)
            self.assertEqual(errors, [])
            self.assertEqual(report["status"], "PASS")

    def test_fails_over_threshold(self) -> None:
        with tempfile.TemporaryDirectory(prefix="phasee06_disabled_") as directory:
            root = Path(directory)
            baseline = root / "baseline.json"
            candidate = root / "candidate.json"
            self.write_rows(baseline, [100, 100, 100, 100, 100])
            self.write_rows(candidate, [104, 104, 104, 104, 104])
            report, errors = compare_disabled_benchmark.compare(baseline, candidate, 3.0, 5)
            self.assertTrue(errors)
            self.assertEqual(report["status"], "FAIL")


if __name__ == "__main__":
    unittest.main()
