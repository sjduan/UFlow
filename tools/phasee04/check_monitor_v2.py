#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


ARTIFACTS = (
    "trace_events.json",
    "trace_summary.json",
    "trace_summary.csv",
    "transfer_summary.csv",
    "summary.md",
)
REQUIRED_TRANSFER_FIELDS = {
    "event_id",
    "plan_id",
    "status",
    "src_object_id",
    "dst_object_id",
    "src_medium",
    "dst_medium",
    "direction",
    "operation",
    "requested_mode",
    "nbytes",
    "planned_path",
    "planned_engine",
    "actual_path",
    "actual_engine",
    "actual_latency_us",
    "actual_bandwidth_gib_s",
    "fallback_used",
    "fallback_reason",
    "error",
}


def request_json(
    base_url: str,
    path: str,
    *,
    body: dict[str, Any] | None = None,
    expected_status: int = 200,
) -> dict[str, Any]:
    data = None
    headers: dict[str, str] = {}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(base_url.rstrip("/") + path, data=data, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            status = response.status
            payload = json.loads(response.read())
    except urllib.error.HTTPError as exc:
        status = exc.code
        payload = json.loads(exc.read())
    if status != expected_status:
        raise AssertionError(f"{path}: HTTP {status}, expected {expected_status}: {payload}")
    return payload


def save_json(output_dir: Path, name: str, payload: Any) -> None:
    (output_dir / name).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def assert_regular_matrix(transfers: list[dict[str, Any]]) -> None:
    def exists(src: str, dst: str, *, mode: str | None = None, path_fragment: str = "") -> bool:
        return any(
            row.get("src_medium") == src
            and row.get("dst_medium") == dst
            and (mode is None or row.get("requested_mode") == mode)
            and (not path_fragment or path_fragment in str(row.get("actual_path", "")))
            for row in transfers
        )

    for src, dst in (("ddr", "hbm"), ("hbm", "ddr")):
        for mode in ("auto", "direct_async", "pinned_async"):
            assert exists(src, dst, mode=mode), f"missing {src}->{dst} mode={mode}"
    for src, dst in (("ssd", "ddr"), ("ddr", "ssd")):
        assert exists(src, dst), f"missing {src}->{dst}"
    for src, dst in (("ssd", "hbm"), ("hbm", "ssd")):
        assert exists(src, dst, mode="relay", path_fragment="via_ddr"), f"missing relay {src}->{dst}"
        logical = [
            row
            for row in transfers
            if row.get("src_medium") == src
            and row.get("dst_medium") == dst
            and "direct" in str(row.get("actual_path", ""))
        ]
        assert logical, f"missing logical direct {src}->{dst}"
        assert all(row.get("direct_kind") == "logical" for row in logical), logical


def assert_fallback_matrix(transfers: list[dict[str, Any]]) -> None:
    fallback = [row for row in transfers if row.get("fallback_used") is True]
    assert fallback, "missing runtime fallback transfer"
    for row in fallback:
        assert "direct" in str(row.get("planned_path", "")), row
        assert "via_ddr" in str(row.get("actual_path", "")), row
        assert row.get("fallback_reason"), row
        assert row.get("status") == "complete", row
    failed = [row for row in transfers if row.get("status") == "failed"]
    assert failed, "missing explicit direct failure"
    assert all(row.get("fallback_used") is False for row in failed), failed


def validate_backing_paths(base_url: str, output_dir: Path) -> dict[str, Any]:
    create = request_json(
        base_url,
        "/v1/command",
        body={
            "op": "CreateDataObject",
            "model_id": "phasee04-monitor",
            "name": "monitor.check.ssd.1m",
            "role": "user",
            "hint": "mandatory:ssd",
            "target": "ssd:local0",
            "nbytes": 1048576,
            "shape": "1048576",
            "dtype": "uint8",
        },
    )
    response = create["response"]
    object_id = response["object_id"]
    lease_id = response["lease_id"]
    objects = request_json(base_url, "/v1/objects?model_id=phasee04-monitor")
    matching = [row for row in objects["objects"] if row["object_id"] == object_id]
    assert len(matching) == 1, objects
    item = matching[0]
    assert item["placement"] == "ssd", item
    assert item["backing_path"], item
    assert item["ddr_path"] == "", item
    request_json(base_url, "/v1/command", body={"op": "CloseLease", "lease_id": lease_id})
    request_json(base_url, "/v1/command", body={"op": "ReleaseDataObject", "object_id": object_id})
    result = {"create": create, "objects_while_live": objects, "validated_object": item}
    save_json(output_dir, "object_backing_path_check.json", result)
    return result


def download_trace_artifacts(
    base_url: str, run_id: str, output_dir: Path
) -> dict[str, Any]:
    download_dir = output_dir / "trace_download"
    download_dir.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {}
    for filename in ARTIFACTS:
        query = urllib.parse.urlencode({"run_id": run_id, "file": filename})
        url = base_url.rstrip("/") + "/v1/trace/artifact?" + query
        with urllib.request.urlopen(url, timeout=30) as response:
            data = response.read()
            assert response.status == 200
        (download_dir / filename).write_bytes(data)
        report[filename] = {"status": 200, "bytes": len(data)}
    save_json(output_dir, "trace_artifact_downloads.json", report)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate PhaseE-04 Monitor v2 HTTP contract")
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--profile", choices=("regular", "fallback"), required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    snapshots: dict[str, Any] = {}
    for name, path in (
        ("health", "/healthz"),
        ("config", "/v1/config"),
        ("stats", "/v1/stats"),
        ("observability", "/v1/observability"),
        ("trace_status", "/v1/trace/status"),
        ("transfers", "/v1/transfers?limit=200"),
        ("transfers_summary", "/v1/transfers/summary?limit=200"),
    ):
        snapshots[name] = request_json(args.base_url, path)
        save_json(output_dir, f"{name}.json", snapshots[name])

    request_json(args.base_url, "/v1/transfers?limit=0", expected_status=400)
    request_json(args.base_url, "/v1/transfers?limit=201", expected_status=400)
    request_json(args.base_url, "/v1/transfers?status=unknown", expected_status=400)

    transfer_payload = snapshots["transfers"]
    transfers = transfer_payload["transfers"]
    assert transfer_payload["returned"] == len(transfers)
    assert 0 < len(transfers) <= 200
    assert transfer_payload["skipped_event_ids"] == []
    assert [row["event_id"] for row in transfers] == sorted(
        [row["event_id"] for row in transfers], reverse=True
    )
    for row in transfers:
        missing = REQUIRED_TRANSFER_FIELDS - set(row)
        assert not missing, (row.get("event_id"), missing)

    summary = snapshots["transfers_summary"]["summary"]
    assert summary["sample_size"] == len(transfers), summary
    assert summary["window_limit"] == 200, summary
    assert snapshots["observability"]["transfer_summary"]["available"] is True
    assert snapshots["trace_status"]["trace"]["trace_enabled"] is False
    assert snapshots["trace_status"]["trace"]["trace_flush_count"] >= 1

    if args.profile == "regular":
        assert_regular_matrix(transfers)
    else:
        assert_fallback_matrix(transfers)

    validate_backing_paths(args.base_url, output_dir)
    downloads = download_trace_artifacts(args.base_url, args.run_id, output_dir)
    report = {
        "status": "PASS",
        "profile": args.profile,
        "run_id": args.run_id,
        "transfer_count": len(transfers),
        "summary_group_count": len(summary["groups"]),
        "fallback_count": summary["fallback_count"],
        "trace_artifacts": downloads,
        "invalid_filter_checks": 3,
    }
    save_json(output_dir, "monitor_v2_check.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001 - CLI validation boundary.
        print(json.dumps({"status": "FAIL", "error": str(exc)}, indent=2), file=sys.stderr)
        raise
