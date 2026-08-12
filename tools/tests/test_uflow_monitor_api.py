from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path
from typing import Any


MODULE_PATH = Path(__file__).resolve().parents[1] / "uflow_monitor_api.py"
SPEC = importlib.util.spec_from_file_location("uflow_monitor_api", MODULE_PATH)
assert SPEC and SPEC.loader
monitor = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(monitor)


def complete_event(event_id: int, *, fallback: bool = False) -> dict[str, str]:
    return {
        "status": "ok",
        "event_status": "complete",
        "event_id": str(event_id),
        "plan_id": str(event_id - 10_000),
        "src_object_id": "100",
        "dst_object_id": "101",
        "src_medium": "ssd",
        "dst_medium": "hbm",
        "direction": "h2d",
        "operation": "copy",
        "requested_mode": "auto",
        "nbytes": "67108864",
        "planned_path": "ssd_to_hbm_direct",
        "planned_engine": "ssd_hbm_auto_file_mmap_acl_direct",
        "actual_path": "ssd_to_hbm_via_ddr" if fallback else "ssd_to_hbm_direct",
        "actual_engine": "ssd_hbm_relay_ddr" if fallback else "file_mmap_acl_direct",
        "actual_latency_us": "100.0" if fallback else "80.0",
        "actual_bandwidth_gib_s": "1.25" if fallback else "1.50",
        "fallback_used": "true" if fallback else "false",
        "fallback_reason": "validation failpoint" if fallback else "",
        "ssd_read_bytes": "67108864",
        "ssd_write_bytes": "0",
        "ssd_io_submit_us": "5.0",
        "ssd_io_wait_us": "20.0",
        "error_code": "",
        "error_message": "",
    }


class MonitorApiUnitTests(unittest.TestCase):
    def test_model_objects_use_medium_neutral_backing_path(self) -> None:
        raw = {
            "objects": (
                "100:ddr:user:Created:1024:1024:1024:uint8:ddr:host:0:/dev/shm/ddr100,"
                "101:ssd:user:Created:2048:4096:2048:uint8:ssd:ssd:local0:/data/ssd101"
            )
        }
        objects = monitor.parse_model_objects(raw)
        self.assertEqual(objects[0]["backing_path"], "/dev/shm/ddr100")
        self.assertEqual(objects[0]["ddr_path"], "/dev/shm/ddr100")
        self.assertEqual(objects[1]["target"], "ssd:local0")
        self.assertEqual(objects[1]["backing_path"], "/data/ssd101")
        self.assertEqual(objects[1]["ddr_path"], "")

    def test_transfer_query_is_bounded_and_rejects_invalid_filters(self) -> None:
        self.assertEqual(monitor.parse_transfer_query({})["limit"], 50)
        parsed = monitor.parse_transfer_query(
            {
                "limit": ["200"],
                "before_event_id": ["30010"],
                "status": ["complete"],
                "path": ["ssd_to_hbm_direct"],
                "engine": ["file_mmap_acl_direct"],
                "client_id": ["7"],
            }
        )
        self.assertEqual(parsed["before_event_id"], 30010)
        self.assertEqual(parsed["client_id"], 7)
        for query in (
            {"limit": ["-1"]},
            {"limit": ["201"]},
            {"status": ["mystery"]},
            {"unknown": ["value"]},
        ):
            with self.assertRaises(ValueError):
                monitor.parse_transfer_query(query)

    def test_partial_result_skips_disappeared_event(self) -> None:
        def fake(req: dict[str, Any]) -> dict[str, str]:
            if req["op"] == "ListTransferEvents":
                return {
                    "status": "ok",
                    "event_ids": "30002,30001",
                    "has_more": "false",
                    "next_before_event_id": "",
                }
            if req["event_id"] == 30002:
                return complete_event(30002)
            return {"status": "error", "detail": "transfer event not found"}

        window = monitor.fetch_transfer_window(fake, {"limit": 2})
        self.assertEqual(window["returned"], 1)
        self.assertEqual(window["skipped_event_ids"], [30001])
        self.assertEqual(window["transfers"][0]["status"], "complete")

    def test_summary_groups_paths_and_aggregates_fallback_and_ssd_io(self) -> None:
        transfers = [
            monitor.normalize_transfer_event(complete_event(30001)),
            monitor.normalize_transfer_event(complete_event(30002, fallback=True)),
        ]
        summary = monitor.summarize_transfers(
            {
                "transfers": transfers,
                "window_limit": 200,
                "has_more": False,
                "skipped_event_ids": [],
            },
            generated_at=123,
        )
        self.assertEqual(summary["sample_size"], 2)
        self.assertEqual(summary["generated_at"], 123)
        self.assertEqual(summary["fallback_count"], 1)
        self.assertEqual(summary["fallback_rate"], 0.5)
        self.assertEqual(summary["ssd_io"]["read_bytes"], 2 * 67108864)
        self.assertEqual(summary["ssd_io"]["read_ops"], 2)
        self.assertEqual(summary["ssd_io"]["total_us"], 50.0)
        self.assertEqual(len(summary["groups"]), 2)

    def test_transfer_window_success_invalid_offline_and_partial(self) -> None:
        def success(req: dict[str, Any]) -> dict[str, str]:
            if req["op"] == "ListTransferEvents":
                return {
                    "status": "ok",
                    "event_ids": "30002,30001",
                    "has_more": "true",
                    "next_before_event_id": "30001",
                }
            if req["event_id"] == 30001:
                return {"status": "error", "detail": "transfer event not found"}
            return complete_event(30002, fallback=True)

        window = monitor.fetch_transfer_window(
            success, monitor.parse_transfer_query({"limit": ["2"], "status": ["complete"]})
        )
        self.assertEqual(window["returned"], 1)
        self.assertEqual(window["skipped_event_ids"], [30001])

        def offline(_req: dict[str, Any]) -> dict[str, str]:
            raise monitor.DaemonUnavailable("daemon socket missing")

        with self.assertRaisesRegex(monitor.DaemonUnavailable, "socket missing"):
            monitor.fetch_transfer_window(offline, {"limit": 50})

    def test_observability_remains_compatible_with_v1_daemon(self) -> None:
        def should_not_be_called(_req: dict[str, Any]) -> dict[str, str]:
            raise AssertionError("v1 compatibility must not issue v2 event requests")

        summary = monitor.observability_transfer_summary(
            should_not_be_called,
            {"status": "ok", "ops": "GetStats,GetTraceStatus"},
        )
        self.assertFalse(summary["available"])


if __name__ == "__main__":
    unittest.main()
