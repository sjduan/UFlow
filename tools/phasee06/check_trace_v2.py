#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 2
ARTIFACT_NAMES = (
    "trace_events.json",
    "trace_summary.json",
    "trace_summary.csv",
    "transfer_summary.csv",
    "summary.md",
)
COMMON_KEYS = (
    "trace_schema_version",
    "event_id",
    "plan_id",
    "src_object_id",
    "dst_object_id",
    "src_medium",
    "dst_medium",
    "direction",
    "operation",
    "requested_mode",
    "bytes",
    "planned_path",
    "planned_engine",
)
OUTCOME_KEYS = (
    "status",
    "bytes_done",
    "actual_path",
    "actual_engine",
    "actual_latency_us",
    "actual_bandwidth_gib_s",
    "fallback_used",
    "fallback_reason",
    "error_code",
    "error_message",
)
NUMERIC_COMPARE_KEYS = {
    "schema_version",
    "plan_id",
    "src_object_id",
    "dst_object_id",
    "bytes",
    "bytes_done",
    "actual_latency_us",
    "actual_bandwidth_gib_s",
    "channel_lane_id",
    "channel_device_id",
    "channel_chunk_bytes",
    "channel_chunk_count",
    "channel_pinned_footprint_bytes",
    "channel_chunks_transferred",
    "channel_queue_wait_us",
    "channel_worker_execute_us",
    "channel_lane_wait_us",
    "channel_cpu_copy_us",
    "channel_acl_copy_us",
    "channel_acl_submit_us",
    "channel_acl_wait_us",
    "channel_wall_us",
    "channel_overlap_ratio",
    "channel_stream_create_count",
    "channel_event_reuse_count",
    "channel_event_record_count",
    "channel_event_wait_count",
    "ssd_io_submit_us",
    "ssd_io_wait_us",
    "ssd_io_bytes",
    "ssd_io_bandwidth_gib_s",
    "ssd_read_bytes",
    "ssd_write_bytes",
    "relay_stage_count",
    "relay_ddr_hbm_us",
    "relay_total_us",
    "direct_setup_us",
    "direct_register_us",
    "direct_fadvise_us",
    "direct_readahead_us",
    "direct_madvise_hugepage_us",
    "direct_madvise_willneed_us",
    "direct_madvise_populate_us",
    "direct_pretouch_us",
    "direct_mlock_us",
    "direct_acl_us",
    "direct_total_us",
    "sum_of_span_durations_us",
    "span_minus_actual_latency_us",
    "outcome_count",
}

TRACE_SUMMARY_CSV_COLUMNS = (
    "category",
    "name",
    "count",
    "total_us",
    "avg_us",
    "max_us",
    "bytes",
)

TRANSFER_REQUIRED_COLUMNS = (
    "event_id",
    "schema_version",
    *COMMON_KEYS[2:],
    *OUTCOME_KEYS,
    "channel_device_id",
    "stage_queue_wait_us",
    "stage_worker_execute_us",
    "stage_cpu_copy_us",
    "stage_acl_submit_us",
    "stage_acl_wait_us",
    "stage_ssd_io_us",
    "stage_relay_stage_us",
    "stage_direct_setup_us",
    "sum_of_span_durations_us",
    "span_minus_actual_latency_us",
    "outcome_count",
    "context_consistent",
)


def load_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def event_id_from_args(args: dict[str, Any]) -> int | None:
    try:
        value = int(args.get("event_id", 0))
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def equal_value(left: Any, right: Any, key: str) -> bool:
    if key in NUMERIC_COMPARE_KEYS or (key.startswith("stage_") and key.endswith("_us")):
        try:
            left_number = float(left)
            right_number = float(right)
        except (TypeError, ValueError):
            return left is None and right is None
        if not math.isfinite(left_number) or not math.isfinite(right_number):
            return False
        tolerance = max(0.01, abs(right_number) * 1e-4)
        return abs(left_number - right_number) <= tolerance
    if isinstance(left, bool) or isinstance(right, bool):
        return str(left).lower() == str(right).lower()
    return str(left if left is not None else "") == str(right if right is not None else "")


def required_roles(outcome: dict[str, Any]) -> set[str]:
    if outcome.get("status") != "complete":
        return {"outcome"}
    planned_path = str(outcome.get("planned_path", ""))
    actual_path = str(outcome.get("actual_path", ""))
    roles = {"outcome"}
    if planned_path == "direct_ref" or actual_path == "direct_ref":
        return roles
    roles.update({"queue_wait", "worker_execute"})
    if actual_path == "memfd_pinned_hbm_channel":
        roles.update({"lane_acquire", "lane_busy", "cpu_copy", "acl_submit", "acl_wait"})
    elif "direct_mmap" in actual_path or "direct_thp" in actual_path:
        roles.update({"lane_acquire", "lane_busy", "acl_submit", "acl_wait"})
    elif actual_path in {"ssd_to_ddr", "ddr_to_ssd"}:
        roles.add("ssd_io")
    elif "via_ddr" in actual_path:
        roles.update({"ssd_io", "relay_stage"})
        if planned_path in {"ssd_to_hbm_direct", "hbm_to_ssd_direct"}:
            roles.add("direct_setup")
    elif actual_path in {"ssd_to_hbm_direct", "hbm_to_ssd_direct"}:
        roles.update({"direct_setup", "acl_wait"})
    elif actual_path == "hbm_to_hbm":
        roles.add("acl_copy")
    elif actual_path == "ddr_to_ddr":
        roles.add("cpu_copy")
    return roles


def load_poll_events(path: Path | None) -> dict[int, dict[str, Any]]:
    if path is None:
        return {}
    payload = load_json(path)
    if isinstance(payload, dict):
        payload = payload.get("events", payload.get("transfers", []))
    if not isinstance(payload, list):
        raise ValueError("PollEvent snapshot must be a list or contain an events list")
    rows: dict[int, dict[str, Any]] = {}
    for row in payload:
        if not isinstance(row, dict):
            continue
        try:
            event_id = int(row.get("event_id", 0))
        except (TypeError, ValueError):
            continue
        if event_id > 0:
            rows[event_id] = row
    return rows


def check(args: argparse.Namespace) -> tuple[dict[str, Any], list[str]]:
    artifact_dir = args.artifact_dir.resolve()
    errors: list[str] = []
    for name in ARTIFACT_NAMES:
        if not (artifact_dir / name).is_file():
            errors.append(f"missing artifact: {name}")
    if errors:
        return {
            "schema_version": None,
            "artifact_dir": str(artifact_dir),
            "artifacts": {name: str(artifact_dir / name) for name in ARTIFACT_NAMES},
        }, errors

    raw = load_json(artifact_dir / "trace_events.json")
    summary = load_json(artifact_dir / "trace_summary.json")
    raw_events = raw.get("traceEvents", [])
    if raw.get("uflowTraceSchemaVersion") != SCHEMA_VERSION:
        errors.append(f"raw trace schema is {raw.get('uflowTraceSchemaVersion')!r}, expected 2")
    if summary.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"summary schema is {summary.get('schema_version')!r}, expected 2")
    if not isinstance(raw_events, list):
        errors.append("traceEvents is not a list")
        raw_events = []
    if summary.get("event_count") != len(raw_events):
        errors.append(
            f"event_count mismatch: summary={summary.get('event_count')} raw={len(raw_events)}"
        )

    timestamps: list[float] = []
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for index, event in enumerate(raw_events):
        if not isinstance(event, dict):
            errors.append(f"raw event {index} is not an object")
            continue
        try:
            timestamp = float(event.get("ts"))
        except (TypeError, ValueError):
            errors.append(f"raw event {index} has invalid timestamp")
            continue
        if timestamp < 0:
            errors.append(f"raw event {index} has negative timestamp")
        timestamps.append(timestamp)
        if event.get("ph") == "X":
            try:
                duration = float(event.get("dur"))
            except (TypeError, ValueError):
                errors.append(f"raw duration event {index} has invalid duration")
            else:
                if duration < 0:
                    errors.append(f"raw duration event {index} has negative duration")
        event_args = event.get("args", {})
        if not isinstance(event_args, dict):
            errors.append(f"raw event {index} args is not an object")
            continue
        event_id = event_id_from_args(event_args)
        if event_id is not None:
            grouped[event_id].append(event)
    if timestamps != sorted(timestamps):
        errors.append("raw trace timestamps are not monotonic")

    summary_transfers = {
        int(row["event_id"]): row
        for row in summary.get("transfers", [])
        if isinstance(row, dict) and row.get("event_id") is not None
    }
    poll_events = load_poll_events(args.poll_events)
    event_reports: list[dict[str, Any]] = []
    raw_outcomes: dict[int, dict[str, Any]] = {}
    for event_id, events in sorted(grouped.items()):
        outcomes = [event for event in events if event.get("name") == "transfer.outcome"]
        roles = {
            str(event.get("args", {}).get("stage_role"))
            for event in events
            if event.get("args", {}).get("stage_role")
        }
        context_mismatches: list[str] = []
        for key in COMMON_KEYS:
            values = {
                str(event.get("args", {}).get(key))
                for event in events
                if key in event.get("args", {})
            }
            if len(values) != 1:
                context_mismatches.append(f"{key}={sorted(values)}")
        if context_mismatches:
            errors.append(f"event {event_id} context mismatch/missing: {context_mismatches}")
        for event in events:
            event_args = event.get("args", {})
            if event_args.get("trace_schema_version") != "2":
                errors.append(
                    f"event {event_id} has unknown schema {event_args.get('trace_schema_version')!r}"
                )
            if not event_args.get("stage_role"):
                errors.append(f"event {event_id} stage {event.get('name')} has no stage_role")
        if len(outcomes) != 1:
            errors.append(f"event {event_id} has {len(outcomes)} terminal outcomes")
            outcome_args: dict[str, Any] = {}
        else:
            outcome_event = outcomes[0]
            outcome_args = outcome_event.get("args", {})
            raw_outcomes[event_id] = outcome_args
            if outcome_event.get("ph") != "i":
                errors.append(f"event {event_id} outcome phase is {outcome_event.get('ph')!r}, expected i")
            missing_outcome = [key for key in OUTCOME_KEYS if key not in outcome_args]
            if missing_outcome:
                errors.append(f"event {event_id} outcome missing fields: {missing_outcome}")
            needed = required_roles(outcome_args)
            missing_roles = sorted(needed - roles)
            if missing_roles:
                errors.append(f"event {event_id} missing required stage roles: {missing_roles}")
            if (
                outcome_args.get("direct_candidate")
                or outcome_args.get("actual_path") in {"ssd_to_hbm_direct", "hbm_to_ssd_direct"}
            ) and outcome_args.get("direct_kind") != "logical":
                errors.append(f"event {event_id} SSD-HBM direct attempt is not labelled logical")
            planned_direct = outcome_args.get("planned_path") in {
                "ssd_to_hbm_direct",
                "hbm_to_ssd_direct",
            }
            actual_relay = "via_ddr" in str(outcome_args.get("actual_path", ""))
            if planned_direct and actual_relay:
                if outcome_args.get("fallback_used") != "true" or not outcome_args.get(
                    "fallback_reason"
                ):
                    errors.append(f"event {event_id} runtime fallback is not fully described")

        summary_row = summary_transfers.get(event_id)
        if summary_row is None:
            errors.append(f"event {event_id} missing from trace_summary.json transfers")
        elif outcome_args:
            summary_keys = (*COMMON_KEYS[2:],) + tuple(
                key
                for key in outcome_args
                if key not in {"trace_schema_version", "event_id", "stage_role"}
            )
            for key in dict.fromkeys(summary_keys):
                if not equal_value(summary_row.get(key), outcome_args.get(key), key):
                    errors.append(
                        f"event {event_id} summary mismatch {key}: "
                        f"summary={summary_row.get(key)!r} outcome={outcome_args.get(key)!r}"
                    )

        poll_row = poll_events.get(event_id)
        if poll_row is not None and outcome_args:
            for key, poll_value in poll_row.items():
                outcome_key = "status" if key == "event_status" else key
                if outcome_key not in outcome_args:
                    continue
                if not equal_value(outcome_args.get(outcome_key), poll_value, outcome_key):
                    errors.append(
                        f"event {event_id} PollEvent mismatch {outcome_key}: "
                        f"poll={poll_value!r} outcome={outcome_args.get(outcome_key)!r}"
                    )

        event_reports.append(
            {
                "event_id": event_id,
                "status": outcome_args.get("status"),
                "planned_path": outcome_args.get("planned_path"),
                "actual_path": outcome_args.get("actual_path"),
                "required_stages": sorted(required_roles(outcome_args)) if outcome_args else [],
                "observed_stages": sorted(roles),
                "missing_stages": sorted(required_roles(outcome_args) - roles)
                if outcome_args
                else [],
                "outcome_count": len(outcomes),
            }
        )

    if set(summary_transfers) != set(grouped):
        errors.append(
            "transfer id set mismatch: "
            f"summary={sorted(summary_transfers)} raw={sorted(grouped)}"
        )
    expected_incomplete = sorted(
        event_id for event_id, events in grouped.items() if not any(e.get("name") == "transfer.outcome" for e in events)
    )
    if summary.get("incomplete_event_ids") != expected_incomplete:
        errors.append(
            "incomplete_event_ids mismatch: "
            f"summary={summary.get('incomplete_event_ids')} expected={expected_incomplete}"
        )
    dropped = int(summary.get("dropped_events", 0))
    if dropped > 0:
        errors.append(f"trace reports {dropped} dropped event(s)")
    expected_complete = dropped == 0 and not expected_incomplete and all(
        report["outcome_count"] == 1 for report in event_reports
    )
    if bool(summary.get("complete")) != expected_complete:
        errors.append(
            f"summary complete={summary.get('complete')!r}, expected {expected_complete}"
        )

    with (artifact_dir / "trace_summary.csv").open(newline="", encoding="utf-8") as handle:
        stage_csv_rows = list(csv.reader(handle))
    if not stage_csv_rows:
        errors.append("trace_summary.csv is empty")
    else:
        if tuple(stage_csv_rows[0]) != TRACE_SUMMARY_CSV_COLUMNS:
            errors.append(
                "trace_summary.csv header changed: "
                f"actual={stage_csv_rows[0]} expected={list(TRACE_SUMMARY_CSV_COLUMNS)}"
            )
        stage_width = len(stage_csv_rows[0])
        for index, row in enumerate(stage_csv_rows[1:], 2):
            if len(row) != stage_width:
                errors.append(
                    f"trace_summary.csv row {index} has {len(row)} columns, expected {stage_width}"
                )

    transfer_csv_path = artifact_dir / "transfer_summary.csv"
    with transfer_csv_path.open(newline="", encoding="utf-8") as handle:
        csv_rows = list(csv.reader(handle))
    if not csv_rows:
        errors.append("transfer_summary.csv is empty")
    else:
        width = len(csv_rows[0])
        if len(set(csv_rows[0])) != width:
            errors.append("transfer_summary.csv has duplicate columns")
        missing_columns = sorted(set(TRANSFER_REQUIRED_COLUMNS) - set(csv_rows[0]))
        if missing_columns:
            errors.append(f"transfer_summary.csv missing columns: {missing_columns}")
        for index, row in enumerate(csv_rows[1:], 2):
            if len(row) != width:
                errors.append(
                    f"transfer_summary.csv row {index} has {len(row)} columns, expected {width}"
                )
        if len(csv_rows) - 1 != len(summary_transfers):
            errors.append(
                "transfer_summary.csv row count mismatch: "
                f"csv={len(csv_rows) - 1} summary={len(summary_transfers)}"
            )
        header = csv_rows[0]
        csv_by_event: dict[int, dict[str, str]] = {}
        for row in csv_rows[1:]:
            if len(row) != width:
                continue
            csv_row = dict(zip(header, row))
            try:
                csv_event_id = int(csv_row.get("event_id", "0"))
            except ValueError:
                errors.append(f"transfer_summary.csv has invalid event_id {csv_row.get('event_id')!r}")
                continue
            if csv_event_id in csv_by_event:
                errors.append(f"transfer_summary.csv duplicates event_id {csv_event_id}")
            csv_by_event[csv_event_id] = csv_row
        if set(csv_by_event) != set(summary_transfers):
            errors.append(
                "transfer_summary.csv event id set mismatch: "
                f"csv={sorted(csv_by_event)} summary={sorted(summary_transfers)}"
            )
        for event_id, csv_row in csv_by_event.items():
            summary_row = summary_transfers.get(event_id)
            if summary_row is None:
                continue
            for column in header:
                if column.startswith("stage_") and column.endswith("_us"):
                    role = column[len("stage_") : -len("_us")]
                    expected = summary_row.get("stage_durations_us", {}).get(role)
                else:
                    expected = summary_row.get(column)
                actual: Any = csv_row[column] if csv_row[column] != "" else None
                if not equal_value(actual, expected, column):
                    errors.append(
                        f"event {event_id} CSV mismatch {column}: "
                        f"csv={actual!r} summary={expected!r}"
                    )

    report = {
        "schema_version": summary.get("schema_version"),
        "case": args.case,
        "status": "PASS" if not errors else "FAIL",
        "reason": "" if not errors else f"{len(errors)} validation error(s)",
        "event_count": len(raw_events),
        "transfer_count": len(summary_transfers),
        "incomplete_count": len(expected_incomplete),
        "dropped_events": dropped,
        "events": event_reports,
        "event_vs_trace_mismatches": errors,
        "artifacts": {name: str(artifact_dir / name) for name in ARTIFACT_NAMES},
    }
    return report, errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate UFlow PhaseE-06 Trace schema v2 artifacts")
    parser.add_argument("artifact_dir", type=Path)
    parser.add_argument("--poll-events", type=Path, help="JSON list of sampled PollEvent payloads")
    parser.add_argument("--case", default="trace_v2")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    try:
        report, errors = check(args)
    except Exception as exc:  # noqa: BLE001
        report = {
            "schema_version": None,
            "case": args.case,
            "status": "FAIL",
            "reason": f"checker exception: {exc}",
            "event_vs_trace_mismatches": [str(exc)],
        }
        errors = [str(exc)]
    output = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(output, encoding="utf-8")
    sys.stdout.write(output)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
