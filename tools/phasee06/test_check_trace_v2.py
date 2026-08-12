from __future__ import annotations

import argparse
import csv
import json
import tempfile
import unittest
from pathlib import Path

import check_trace_v2


class TraceV2CheckerTest(unittest.TestCase):
    def write_fixture(self, root: Path, *, include_outcome: bool = True) -> None:
        context = {
            "trace_schema_version": "2",
            "event_id": "9",
            "plan_id": "4",
            "src_object_id": "1",
            "dst_object_id": "2",
            "src_medium": "ssd",
            "dst_medium": "ddr",
            "direction": "ssd_to_ddr",
            "operation": "copy",
            "requested_mode": "auto",
            "bytes": "4096",
            "planned_path": "ssd_to_ddr",
            "planned_engine": "ssd_buffered_pread",
        }

        def trace_event(name: str, timestamp: int, role: str, phase: str = "X") -> dict:
            event = {
                "name": name,
                "cat": "uflow.transfer",
                "ph": phase,
                "pid": 1,
                "tid": 1,
                "ts": timestamp,
                "args": {**context, "stage_role": role},
            }
            if phase == "X":
                event["dur"] = 1.0
            else:
                event["s"] = "t"
            return event

        events = [
            trace_event("transfer.submit", 1, "submit", "i"),
            trace_event("transfer.queue_wait", 2, "queue_wait"),
            trace_event("transfer.worker_execute", 3, "worker_execute"),
            trace_event("ssd.read_wait", 4, "ssd_io"),
        ]
        outcome = {
            **context,
            "stage_role": "outcome",
            "status": "complete",
            "bytes_done": "4096",
            "actual_path": "ssd_to_ddr",
            "actual_engine": "ssd_buffered_pread",
            "actual_latency_us": "10.0",
            "actual_bandwidth_gib_s": "0.38",
            "fallback_used": "false",
            "fallback_reason": "",
            "error_code": "",
            "error_message": "",
            "ssd_io_bytes": "4096",
        }
        if include_outcome:
            event = trace_event("transfer.outcome", 5, "outcome", "i")
            event["args"] = outcome
            events.append(event)

        transfer = {
            "event_id": 9,
            "schema_version": 2,
            **{key: int(value) if value.isdigit() else value for key, value in context.items() if key not in {"trace_schema_version", "event_id"}},
            **{
                key: (
                    int(value)
                    if value.isdigit()
                    else float(value)
                    if key in {"actual_latency_us", "actual_bandwidth_gib_s"}
                    else value
                )
                for key, value in outcome.items()
                if key not in context and key != "stage_role"
            },
            "outcome_count": 1 if include_outcome else 0,
            "context_consistent": True,
            "stage_durations_us": {
                "queue_wait": 1.0,
                "worker_execute": 1.0,
                "ssd_io": 1.0,
            },
        }
        summary = {
            "schema_version": 2,
            "run_id": "checker-test",
            "mode": "full",
            "categories": "transfer",
            "event_count": len(events),
            "dropped_events": 0,
            "complete": include_outcome,
            "incomplete_event_ids": [] if include_outcome else [9],
            "duplicate_outcome_event_ids": [],
            "context_mismatch_event_ids": [],
            "stages": [],
            "transfers": [transfer],
            "path_summary": [],
        }
        (root / "trace_events.json").write_text(
            json.dumps({"uflowTraceSchemaVersion": 2, "traceEvents": events}),
            encoding="utf-8",
        )
        (root / "trace_summary.json").write_text(json.dumps(summary), encoding="utf-8")
        (root / "trace_summary.csv").write_text(
            "category,name,count,total_us,avg_us,max_us,bytes\n", encoding="utf-8"
        )
        with (root / "transfer_summary.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            columns = list(check_trace_v2.TRANSFER_REQUIRED_COLUMNS)
            writer.writerow(columns)
            row = []
            for column in columns:
                if column == "event_id":
                    value = 9
                elif column == "schema_version":
                    value = 2
                elif column.startswith("stage_") and column.endswith("_us"):
                    role = column[len("stage_") : -len("_us")]
                    value = transfer["stage_durations_us"].get(role, "")
                else:
                    value = transfer.get(column, "")
                row.append(value if value is not None else "")
            writer.writerow(row)
        (root / "summary.md").write_text("# fixture\n", encoding="utf-8")

    def arguments(self, root: Path) -> argparse.Namespace:
        return argparse.Namespace(
            artifact_dir=root,
            poll_events=None,
            case="unit",
            report=None,
        )

    def test_valid_fixture_passes(self) -> None:
        with tempfile.TemporaryDirectory(prefix="phasee06_checker_") as directory:
            root = Path(directory)
            self.write_fixture(root)
            report, errors = check_trace_v2.check(self.arguments(root))
            self.assertEqual(errors, [])
            self.assertEqual(report["status"], "PASS")

    def test_numeric_stage_csv_formatting_is_equivalent(self) -> None:
        self.assertTrue(check_trace_v2.equal_value("1.000", 1.0, "stage_ssd_io_us"))

    def test_missing_outcome_fails_explicitly(self) -> None:
        with tempfile.TemporaryDirectory(prefix="phasee06_checker_") as directory:
            root = Path(directory)
            self.write_fixture(root, include_outcome=False)
            report, errors = check_trace_v2.check(self.arguments(root))
            self.assertTrue(errors)
            self.assertEqual(report["status"], "FAIL")
            self.assertTrue(any("0 terminal outcomes" in error for error in errors))

    def test_dropped_events_fail_explicitly(self) -> None:
        with tempfile.TemporaryDirectory(prefix="phasee06_checker_") as directory:
            root = Path(directory)
            self.write_fixture(root)
            summary_path = root / "trace_summary.json"
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            summary["dropped_events"] = 2
            summary["complete"] = False
            summary_path.write_text(json.dumps(summary), encoding="utf-8")
            report, errors = check_trace_v2.check(self.arguments(root))
            self.assertEqual(report["status"], "FAIL")
            self.assertTrue(any("2 dropped event" in error for error in errors))

    def test_transfer_csv_mismatch_fails_explicitly(self) -> None:
        with tempfile.TemporaryDirectory(prefix="phasee06_checker_") as directory:
            root = Path(directory)
            self.write_fixture(root)
            csv_path = root / "transfer_summary.csv"
            with csv_path.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.reader(handle))
            status_index = rows[0].index("status")
            rows[1][status_index] = "failed"
            with csv_path.open("w", newline="", encoding="utf-8") as handle:
                csv.writer(handle).writerows(rows)
            report, errors = check_trace_v2.check(self.arguments(root))
            self.assertEqual(report["status"], "FAIL")
            self.assertTrue(any("CSV mismatch status" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
