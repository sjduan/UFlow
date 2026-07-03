#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


GIB = 1024**3


RESULT_RE = re.compile(r"UFLOW_E05_HOTPATH_RESULT\s+(.*)$")


@dataclass
class Case:
    run_dir: Path
    selection: str
    label: str
    directions: str
    transfers: int
    bytes_per_transfer: int
    total_bytes: int
    daemon_bandwidth_gib_s: float
    client_effective_bandwidth_gib_s: float
    daemon_wall_ms: float
    client_wall_ms: float
    cpu_copy_us: float
    acl_submit_us: float
    acl_wait_us: float
    chunks: int
    pipeline_overlap: bool
    h2d_lanes: int
    d2h_lanes: int
    pinned_total: int
    lane_wait_count: int
    env: dict[str, str]


def parse_kv_tail(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in text.strip().split():
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        out[key] = value
    return out


def read_env(run_dir: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    path = run_dir / "environment.txt"
    if not path.exists():
        return env
    for raw in path.read_text(errors="ignore").splitlines():
        if "=" in raw:
            key, value = raw.split("=", 1)
            env[key.strip()] = value.strip()
    return env


def as_bool(value: str) -> bool:
    return value.lower() in {"1", "true", "yes"}


def selection_from_env(env: dict[str, str], directions: str) -> str:
    direct = as_bool(env.get("d2h_direct_ddr", "0"))
    registered = as_bool(env.get("d2h_register_ddr", "0"))
    h2d_cap = int(env.get("h2d_max_lanes", "0") or "0")
    pinned_max = int(env.get("pinned_max", "0") or "0")
    if directions == "h2d":
        if h2d_cap or pinned_max:
            return "h2d_pinned_budget_cap"
        return "h2d_pinned"
    if directions == "d2h":
        if direct and registered:
            return "d2h_direct_registered_v1"
        if direct:
            return "d2h_direct_ddr"
        return "d2h_pinned"
    if directions == "d2h,h2d":
        if direct:
            return "bidir_h2d_pinned_d2h_direct"
        return "bidir_h2d_d2h_pinned"
    return directions.replace(",", "_")


def parse_cases(input_root: Path) -> list[Case]:
    cases: list[Case] = []
    for run_dir in sorted(input_root.glob("phasee05_hotpath_*")):
        stdout = run_dir / "hotpath.stdout"
        if not stdout.exists():
            continue
        env = read_env(run_dir)
        for line in stdout.read_text(errors="ignore").splitlines():
            match = RESULT_RE.search(line)
            if not match:
                continue
            kv = parse_kv_tail(match.group(1))
            directions = kv["directions"]
            cases.append(
                Case(
                    run_dir=run_dir,
                    selection=selection_from_env(env, directions),
                    label=kv["label"],
                    directions=directions,
                    transfers=int(kv["transfers"]),
                    bytes_per_transfer=int(kv["bytes_per_transfer"]),
                    total_bytes=int(kv["total_bytes"]),
                    daemon_bandwidth_gib_s=float(kv["daemon_bandwidth_gib_s"]),
                    client_effective_bandwidth_gib_s=float(kv["client_effective_bandwidth_gib_s"]),
                    daemon_wall_ms=float(kv["daemon_wall_ms"]),
                    client_wall_ms=float(kv["client_wall_ms"]),
                    cpu_copy_us=float(kv["cpu_copy_us"]),
                    acl_submit_us=float(kv["acl_submit_us"]),
                    acl_wait_us=float(kv["acl_wait_us"]),
                    chunks=int(kv["chunks"]),
                    pipeline_overlap=as_bool(kv["pipeline_overlap"]),
                    h2d_lanes=int(kv["h2d_lanes"]),
                    d2h_lanes=int(kv["d2h_lanes"]),
                    pinned_total=int(kv["pinned_total"]),
                    lane_wait_count=int(kv["lane_wait_count"]),
                    env=env,
                )
            )
    return cases


def tid_for_selection(selection: str) -> int:
    order = {
        "h2d_pinned": 100,
        "h2d_pinned_budget_cap": 140,
        "d2h_pinned": 200,
        "d2h_direct_ddr": 300,
        "d2h_direct_registered_v1": 400,
        "bidir_h2d_pinned_d2h_direct": 500,
        "bidir_h2d_d2h_pinned": 600,
    }
    return order.get(selection, 900)


def add_x(events: list[dict[str, Any]], name: str, cat: str, pid: int, tid: int, ts: float, dur: float, args: dict[str, Any]) -> None:
    events.append(
        {
            "name": name,
            "cat": cat,
            "ph": "X",
            "pid": pid,
            "tid": tid,
            "ts": round(ts, 3),
            "dur": round(max(dur, 1.0), 3),
            "args": args,
        }
    )


def add_counter(events: list[dict[str, Any]], name: str, pid: int, tid: int, ts: float, value: float, args: dict[str, Any]) -> None:
    payload = dict(args)
    payload[name] = value
    events.append({"name": name, "cat": "uflow.counter", "ph": "C", "pid": pid, "tid": tid, "ts": round(ts, 3), "args": payload})


def add_metadata(events: list[dict[str, Any]], pid: int, tid: int, name: str) -> None:
    events.append({"name": "thread_name", "ph": "M", "pid": pid, "tid": tid, "args": {"name": name}})


def bytes_gib(nbytes: int) -> float:
    return nbytes / GIB


def stage_plan(case: Case, direction: str) -> list[tuple[str, float]]:
    per_transfer = max(case.transfers, 1)
    cpu = case.cpu_copy_us / per_transfer
    submit = case.acl_submit_us / per_transfer
    wait = case.acl_wait_us / per_transfer
    wall = case.daemon_wall_ms * 1000.0

    if case.selection == "d2h_direct_registered_v1":
        stages = [
            ("direct_d2h.acl_submit", submit),
            ("direct_d2h.acl_wait_or_register_penalty", wait),
        ]
    elif case.selection == "d2h_direct_ddr" or (case.selection == "bidir_h2d_pinned_d2h_direct" and direction == "d2h"):
        stages = [
            ("direct_d2h.acl_submit", submit),
            ("direct_d2h.acl_wait", wait),
        ]
    elif direction == "d2h":
        stages = [
            ("chunk.acl_submit.d2h", submit),
            ("chunk.acl_wait.d2h", wait),
            ("chunk.cpu_copy.pinned_to_memfd", cpu),
        ]
    else:
        stages = [
            ("chunk.cpu_copy.memfd_to_pinned", cpu),
            ("chunk.acl_submit.h2d", submit),
            ("chunk.acl_wait.h2d", wait),
        ]

    used = sum(duration for _, duration in stages)
    residual = max(wall - used, 1.0)
    stages.append(("stage.other_or_overlap_gap", residual))
    return stages


def transfer_directions(case: Case) -> list[str]:
    if case.directions == "d2h,h2d":
        half = max(case.transfers // 2, 1)
        return ["h2d"] * half + ["d2h"] * half
    return [case.directions] * case.transfers


def build_trace(cases: list[Case]) -> dict[str, Any]:
    events: list[dict[str, Any]] = []
    pid = 1
    add_metadata(events, pid, 1, "case daemon wall")
    add_metadata(events, pid, 2, "case client wall")
    add_metadata(events, pid, 3, "aggregate stage work")

    for selection in sorted({case.selection for case in cases}):
        base = tid_for_selection(selection)
        add_metadata(events, pid, base, f"{selection}: transfer wall")
        add_metadata(events, pid, base + 1, f"{selection}: inferred stage bars")

    ts = 0.0
    gap = 70_000.0
    for case_idx, case in enumerate(cases):
        base_tid = tid_for_selection(case.selection)
        wall_us = case.daemon_wall_ms * 1000.0
        client_us = case.client_wall_ms * 1000.0
        common = {
            "run_dir": case.run_dir.name,
            "label": case.label,
            "selection": case.selection,
            "directions": case.directions,
            "transfers": case.transfers,
            "bytes_gib": round(bytes_gib(case.total_bytes), 3),
            "daemon_bandwidth_gib_s": case.daemon_bandwidth_gib_s,
            "client_effective_bandwidth_gib_s": case.client_effective_bandwidth_gib_s,
            "pipeline_overlap": case.pipeline_overlap,
            "source": "PhaseE-05 hotpath stdout",
            "timing_kind": "inferred_from_aggregate_stats",
        }
        add_x(events, f"{case.selection}.{case.label}.daemon_wall", "uflow.case", pid, 1, ts, wall_us, common)
        add_x(events, f"{case.selection}.{case.label}.client_wall", "uflow.case", pid, 2, ts, client_us, common)
        add_counter(events, "bandwidth_gib_s", pid, 3, ts, case.daemon_bandwidth_gib_s, common)
        add_counter(events, "pinned_total_bytes", pid, 3, ts, case.pinned_total, common)

        aggregate_ts = ts
        aggregate_stages = [
            ("aggregate.cpu_copy_sum", case.cpu_copy_us),
            ("aggregate.acl_submit_sum", case.acl_submit_us),
            ("aggregate.acl_wait_sum", case.acl_wait_us),
        ]
        for stage_name, duration in aggregate_stages:
            add_x(
                events,
                stage_name,
                "uflow.aggregate",
                pid,
                3,
                aggregate_ts,
                duration,
                {**common, "note": "sum across concurrent transfers; may exceed daemon wall"},
            )
            aggregate_ts += max(duration, 1.0)

        directions = transfer_directions(case)
        for idx, direction in enumerate(directions):
            lane_tid = base_tid + 10 + idx
            add_metadata(events, pid, lane_tid, f"{case.selection}:{case.label}:transfer{idx}:{direction}")
            transfer_start = ts
            add_x(
                events,
                f"transfer.{direction}.wall",
                "uflow.transfer",
                pid,
                lane_tid,
                transfer_start,
                wall_us,
                {**common, "transfer_index": idx, "direction": direction},
            )
            stage_ts = transfer_start
            for stage_name, duration in stage_plan(case, direction):
                add_x(
                    events,
                    stage_name,
                    "uflow.stage",
                    pid,
                    lane_tid + 1000,
                    stage_ts,
                    duration,
                    {
                        **common,
                        "transfer_index": idx,
                        "direction": direction,
                        "stage_duration_us": round(duration, 3),
                    },
                )
                stage_ts += max(duration, 1.0)

        ts += max(client_us, wall_us, aggregate_ts - ts) + gap

    return {
        "displayTimeUnit": "ms",
        "metadata": {
            "title": "UFlow PhaseE-05 selection performance analysis",
            "created_by": "PhaseE-06 inferred trace generator",
            "warning": "This trace is inferred from PhaseE-05 aggregate logs. It is suitable for option comparison, not exact per-chunk timestamp validation.",
        },
        "traceEvents": events,
    }


def write_summary(cases: list[Case], path: Path) -> None:
    fields = [
        "selection",
        "label",
        "directions",
        "transfers",
        "total_gib",
        "daemon_bandwidth_gib_s",
        "client_effective_bandwidth_gib_s",
        "daemon_wall_ms",
        "client_wall_ms",
        "cpu_copy_us",
        "acl_submit_us",
        "acl_wait_us",
        "chunks",
        "pipeline_overlap",
        "h2d_lanes",
        "d2h_lanes",
        "pinned_total",
        "lane_wait_count",
        "run_dir",
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for case in cases:
            writer.writerow(
                {
                    "selection": case.selection,
                    "label": case.label,
                    "directions": case.directions,
                    "transfers": case.transfers,
                    "total_gib": f"{bytes_gib(case.total_bytes):.3f}",
                    "daemon_bandwidth_gib_s": f"{case.daemon_bandwidth_gib_s:.3f}",
                    "client_effective_bandwidth_gib_s": f"{case.client_effective_bandwidth_gib_s:.3f}",
                    "daemon_wall_ms": f"{case.daemon_wall_ms:.3f}",
                    "client_wall_ms": f"{case.client_wall_ms:.3f}",
                    "cpu_copy_us": f"{case.cpu_copy_us:.3f}",
                    "acl_submit_us": f"{case.acl_submit_us:.3f}",
                    "acl_wait_us": f"{case.acl_wait_us:.3f}",
                    "chunks": case.chunks,
                    "pipeline_overlap": case.pipeline_overlap,
                    "h2d_lanes": case.h2d_lanes,
                    "d2h_lanes": case.d2h_lanes,
                    "pinned_total": case.pinned_total,
                    "lane_wait_count": case.lane_wait_count,
                    "run_dir": case.run_dir.name,
                }
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Chrome trace JSON from PhaseE-05 UFlow hotpath logs.")
    parser.add_argument("--input-root", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    input_root = Path(args.input_root)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    cases = parse_cases(input_root)
    if not cases:
        raise SystemExit(f"no PhaseE-05 hotpath cases found under {input_root}")

    trace = build_trace(cases)
    trace_path = output_dir / "phasee05_selection_analysis_chrome_trace.json"
    trace_path.write_text(json.dumps(trace, indent=2), encoding="utf-8")
    write_summary(cases, output_dir / "phasee05_selection_summary.csv")

    readme = output_dir / "README.md"
    readme.write_text(
        "\n".join(
            [
                "# PhaseE-06 inferred trace from PhaseE-05 logs",
                "",
                "Open `phasee05_selection_analysis_chrome_trace.json` in `chrome://tracing` or https://ui.perfetto.dev.",
                "",
                "This is an inferred trace generated from PhaseE-05 aggregate benchmark logs.",
                "It compares H2D pinned, D2H pinned, D2H direct DDR, D2H registered V1, bidirectional, and budget-cap selections.",
                "It is not a replacement for the future daemon-native per-stage trace.",
                "",
                f"Cases: {len(cases)}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    print(trace_path)


if __name__ == "__main__":
    main()
