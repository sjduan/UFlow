from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from pathlib import Path


def _parse_kv_line(line: str, prefix: str) -> dict[str, str] | None:
    if prefix not in line:
        return None
    payload = line.split(prefix, 1)[1].strip()
    out: dict[str, str] = {}
    for item in payload.split():
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        out[key] = value
    return out


def _as_int(value: str | None, default: int = 0) -> int:
    try:
        return int(value or default)
    except ValueError:
        return default


def _as_float(value: str | None, default: float = 0.0) -> float:
    try:
        return float(value or default)
    except ValueError:
        return default


def _parse_transfers(lines: list[str]) -> list[dict[str, str]]:
    transfers: list[dict[str, str]] = []
    for line in lines:
        item = _parse_kv_line(line, "[uflow-a08] transfer_timeline ")
        if item is not None:
            transfers.append(item)
    return transfers


def _parse_compute_spans(lines: list[str]) -> list[dict[str, str]]:
    starts: dict[str, int] = {}
    spans: list[dict[str, str]] = []
    for line in lines:
        start = re.search(r"\[uflow-a08\] prefill_layer_start layer=(\d+) ts_ns=(\d+)", line)
        if start:
            starts[f"prefill.layer{start.group(1)}"] = int(start.group(2))
            continue
        done = re.search(r"\[uflow-a08\] prefill_layer_done layer=(\d+) ts_ns=(\d+)", line)
        if done:
            label = f"prefill.layer{done.group(1)}"
            start_ns = starts.pop(label, 0)
            if start_ns:
                spans.append({"lane": "PyPTO layer task", "label": label, "start_ns": str(start_ns), "end_ns": done.group(2)})
            continue
        decode_start = re.search(r"\[uflow-a08\] decode_compute_start ts_ns=(\d+)", line)
        if decode_start:
            starts["decode.compute"] = int(decode_start.group(1))
            continue
        decode_done = re.search(r"\[uflow-a08\] decode_compute_done ts_ns=(\d+)", line)
        if decode_done:
            start_ns = starts.pop("decode.compute", 0)
            if start_ns:
                spans.append({"lane": "PyPTO layer task", "label": "decode.compute", "start_ns": str(start_ns), "end_ns": decode_done.group(1)})
    return spans


def _write_csv(path: Path, transfers: list[dict[str, str]]) -> None:
    fields = [
        "stage",
        "label",
        "nbytes",
        "mode",
        "event_id",
        "plan_id",
        "status",
        "completion_kind",
        "actual_engine",
        "actual_path",
        "submitted_at_ns",
        "started_at_ns",
        "completed_at_ns",
        "actual_latency_us",
        "actual_bandwidth_gib_s",
        "submit_wall_us",
        "export_wait_us",
    ]
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for item in transfers:
            writer.writerow({field: item.get(field, "") for field in fields})


def _stage_summary(transfers: list[dict[str, str]]) -> list[dict[str, str]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for item in transfers:
        grouped[item.get("stage", "unknown")].append(item)
    rows: list[dict[str, str]] = []
    for stage, items in sorted(grouped.items()):
        bytes_done = sum(_as_int(item.get("nbytes")) for item in items)
        starts = [_as_int(item.get("started_at_ns")) for item in items if _as_int(item.get("started_at_ns"))]
        ends = [_as_int(item.get("completed_at_ns")) for item in items if _as_int(item.get("completed_at_ns"))]
        wall_ms = ((max(ends) - min(starts)) / 1_000_000.0) if starts and ends else 0.0
        sum_latency_ms = sum(_as_float(item.get("actual_latency_us")) for item in items) / 1000.0
        stage_effective_bw = (bytes_done / (wall_ms / 1000.0) / (1024.0**3)) if wall_ms > 0 else 0.0
        copy_bw = (bytes_done / (sum_latency_ms / 1000.0) / (1024.0**3)) if sum_latency_ms > 0 else 0.0
        engines = Counter(item.get("actual_engine", "") for item in items)
        paths = Counter(item.get("actual_path", "") for item in items)
        rows.append(
            {
                "stage": stage,
                "events": str(len(items)),
                "bytes": str(bytes_done),
                "wall_ms": f"{wall_ms:.3f}",
                "sum_latency_ms": f"{sum_latency_ms:.3f}",
                "stage_effective_bw_gib_s": f"{stage_effective_bw:.3f}",
                "copy_bw_gib_s": f"{copy_bw:.3f}",
                "engines": dict(engines).__repr__(),
                "paths": dict(paths).__repr__(),
            }
        )
    return rows


def _event_spans(transfers: list[dict[str, str]], compute_spans: list[dict[str, str]]) -> list[dict[str, str]]:
    spans: list[dict[str, str]] = []
    for item in transfers:
        label = f"{item.get('stage', '')}.{item.get('label', '')}"
        submitted = _as_int(item.get("submitted_at_ns"))
        started = _as_int(item.get("started_at_ns"))
        completed = _as_int(item.get("completed_at_ns"))
        printed = _as_int(item.get("printed_at_ns"))
        if submitted and completed:
            spans.append({"lane": "UFlow daemon", "label": label, "start_ns": str(submitted), "end_ns": str(completed)})
        if started and completed:
            spans.append({"lane": "ACL stream", "label": item.get("actual_engine", ""), "start_ns": str(started), "end_ns": str(completed)})
        if printed:
            spans.append({"lane": "pypto-serving", "label": f"event {item.get('event_id', '')}", "start_ns": str(printed), "end_ns": str(printed + 500_000)})
    spans.extend(compute_spans)
    return spans


def _trace_ts_us(ns: int, base_ns: int) -> float:
    return (ns - base_ns) / 1000.0


def _trace_dur_us(start_ns: int, end_ns: int) -> float:
    return max((end_ns - start_ns) / 1000.0, 0.001)


def _write_trace_json(path: Path, rows: list[dict[str, str]], spans: list[dict[str, str]], transfers: list[dict[str, str]]) -> None:
    lane_threads = {
        "pypto-serving": 10,
        "UFlow daemon": 20,
        "ACL stream": 30,
        "PyPTO layer task": 40,
    }
    lane_categories = {
        "pypto-serving": "serving",
        "UFlow daemon": "uflow,transfer",
        "ACL stream": "acl,transfer",
        "PyPTO layer task": "pypto,compute",
    }
    times: list[int] = []
    for span in spans:
        start = _as_int(span.get("start_ns"))
        end = _as_int(span.get("end_ns"))
        if start:
            times.append(start)
        if end:
            times.append(end)
    base_ns = min(times) if times else 0
    trace_events: list[dict[str, object]] = []
    pid = 1
    trace_events.append({"name": "process_name", "ph": "M", "pid": pid, "tid": 0, "args": {"name": "PhaseA-08 UFlow Direct Serving"}})
    for lane, tid in lane_threads.items():
        trace_events.append({"name": "thread_name", "ph": "M", "pid": pid, "tid": tid, "args": {"name": lane}})

    for span in spans:
        lane = span.get("lane", "")
        tid = lane_threads.get(lane)
        if tid is None:
            continue
        start = _as_int(span.get("start_ns"))
        end = max(_as_int(span.get("end_ns")), start + 1)
        if not start:
            continue
        trace_events.append(
            {
                "name": span.get("label", lane),
                "cat": lane_categories.get(lane, lane),
                "ph": "X",
                "pid": pid,
                "tid": tid,
                "ts": _trace_ts_us(start, base_ns),
                "dur": _trace_dur_us(start, end),
                "args": {"start_ns": start, "end_ns": end},
            }
        )

    for item in transfers:
        event_id = item.get("event_id", "")
        label = f"{item.get('stage', '')}.{item.get('label', '')}".strip(".")
        printed = _as_int(item.get("printed_at_ns"))
        submitted = _as_int(item.get("submitted_at_ns"))
        started = _as_int(item.get("started_at_ns"))
        completed = _as_int(item.get("completed_at_ns"))
        submit_wall_ns = int(_as_float(item.get("submit_wall_us")) * 1000.0)
        export_wait_ns = int(_as_float(item.get("export_wait_us")) * 1000.0)

        if printed and submit_wall_ns > 0:
            submit_start = max(printed - export_wait_ns - submit_wall_ns, 1)
            trace_events.append(
                {
                    "name": f"SubmitTransfer {label}",
                    "cat": "serving,uflow",
                    "ph": "X",
                    "pid": pid,
                    "tid": lane_threads["pypto-serving"],
                    "ts": _trace_ts_us(submit_start, base_ns),
                    "dur": _trace_dur_us(submit_start, submit_start + submit_wall_ns),
                    "args": {"event_id": event_id, "plan_id": item.get("plan_id", ""), "mode": item.get("mode", "")},
                }
            )
        if printed and export_wait_ns > 0:
            export_start = max(printed - export_wait_ns, 1)
            trace_events.append(
                {
                    "name": f"ExportCompletionProxy {label}",
                    "cat": "serving,event",
                    "ph": "X",
                    "pid": pid,
                    "tid": lane_threads["pypto-serving"],
                    "ts": _trace_ts_us(export_start, base_ns),
                    "dur": _trace_dur_us(export_start, printed),
                    "args": {"event_id": event_id, "completion_kind": item.get("completion_kind", "")},
                }
            )
        if submitted and started:
            trace_events.append(
                {
                    "name": f"TransferQueued {label}",
                    "cat": "uflow,queue",
                    "ph": "X",
                    "pid": pid,
                    "tid": lane_threads["UFlow daemon"],
                    "ts": _trace_ts_us(submitted, base_ns),
                    "dur": _trace_dur_us(submitted, started),
                    "args": {"event_id": event_id, "plan_id": item.get("plan_id", "")},
                }
            )
        if submitted and completed:
            trace_events.append(
                {
                    "name": "TransferEventComplete",
                    "cat": "uflow,event",
                    "ph": "i",
                    "s": "t",
                    "pid": pid,
                    "tid": lane_threads["UFlow daemon"],
                    "ts": _trace_ts_us(completed, base_ns),
                    "args": {
                        "event_id": event_id,
                        "plan_id": item.get("plan_id", ""),
                        "stage": item.get("stage", ""),
                        "label": item.get("label", ""),
                        "nbytes": _as_int(item.get("nbytes")),
                        "actual_engine": item.get("actual_engine", ""),
                        "actual_path": item.get("actual_path", ""),
                        "actual_latency_us": _as_float(item.get("actual_latency_us")),
                        "actual_bandwidth_gib_s": _as_float(item.get("actual_bandwidth_gib_s")),
                    },
                }
            )

    trace = {
        "traceEvents": trace_events,
        "displayTimeUnit": "ms",
        "metadata": {
            "title": "PhaseA-08 UFlow direct serving timeline",
            "format": "Chrome Trace Event JSON",
            "viewer": "Open in https://ui.perfetto.dev or chrome://tracing",
            "base_time_ns": base_ns,
            "transfer_count": len(transfers),
            "summary": rows,
            "contract": {
                "serving_path": "PlanTransfer -> SubmitTransfer -> TransferEvent -> completion proxy event",
                "ddr_hbm_route": "UFlow daemon memfd + THP/pre-touch + aclrtMemcpyAsync",
                "default_mode": "auto",
                "expected_engine": "acl_direct_async_thp",
            },
        },
    }
    path.write_text(json.dumps(trace, indent=2), encoding="utf-8")


def _write_summary(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w") as fh:
        fh.write("# PhaseA-08 Transfer Performance Summary\n\n")
        fh.write("| stage | events | bytes | wall_ms | sum_latency_ms | stage_effective_bw_gib_s | copy_bw_gib_s | engines | paths |\n")
        fh.write("|---|---:|---:|---:|---:|---:|---:|---|---|\n")
        for row in rows:
            fh.write(
                f"| {row['stage']} | {row['events']} | {row['bytes']} | {row['wall_ms']} | "
                f"{row['sum_latency_ms']} | {row['stage_effective_bw_gib_s']} | {row['copy_bw_gib_s']} | "
                f"`{row['engines']}` | `{row['paths']}` |\n"
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate PhaseA-08 transfer timeline report from qwen.stdout.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--stdout", default="qwen.stdout")
    args = parser.parse_args()
    run_dir = Path(args.run_dir)
    lines = (run_dir / args.stdout).read_text(errors="replace").splitlines()
    transfers = _parse_transfers(lines)
    compute_spans = _parse_compute_spans(lines)
    rows = _stage_summary(transfers)
    spans = _event_spans(transfers, compute_spans)
    _write_csv(run_dir / "transfer_timeline.csv", transfers)
    _write_summary(run_dir / "performance_summary.md", rows)
    _write_trace_json(run_dir / "phasea08_trace_events.json", rows, spans, transfers)
    print(run_dir / "transfer_timeline.csv")
    print(run_dir / "performance_summary.md")
    print(run_dir / "phasea08_trace_events.json")


if __name__ == "__main__":
    main()
