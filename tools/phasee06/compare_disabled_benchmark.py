#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any


METRICS = ("actual_latency_us", "client_wall_us")


def load_cases(path: Path) -> dict[str, list[dict[str, Any]]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("events", payload) if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        raise ValueError(f"{path}: expected event list")
    cases: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if not isinstance(row, dict) or row.get("warmup"):
            continue
        if row.get("status") != "complete":
            raise ValueError(f"{path}: event {row.get('event_id')} is not complete")
        cases[str(row["case"])].append(row)
    if not cases:
        raise ValueError(f"{path}: no measured events")
    return dict(cases)


def compare(
    baseline_path: Path,
    candidate_path: Path,
    threshold_pct: float,
    minimum_repeat: int,
) -> tuple[dict[str, Any], list[str]]:
    baseline = load_cases(baseline_path)
    candidate = load_cases(candidate_path)
    errors: list[str] = []
    if set(baseline) != set(candidate):
        errors.append(
            f"case set mismatch: baseline={sorted(baseline)} candidate={sorted(candidate)}"
        )
    case_rows: list[dict[str, Any]] = []
    latency_overheads: list[float] = []
    for case in sorted(set(baseline) & set(candidate)):
        base_rows = baseline[case]
        candidate_rows = candidate[case]
        if len(base_rows) < minimum_repeat or len(candidate_rows) < minimum_repeat:
            errors.append(
                f"{case}: repeats baseline={len(base_rows)} candidate={len(candidate_rows)}, "
                f"minimum={minimum_repeat}"
            )
        metrics: dict[str, Any] = {}
        for metric in METRICS:
            base_values = [float(row[metric]) for row in base_rows]
            candidate_values = [float(row[metric]) for row in candidate_rows]
            if not all(math.isfinite(value) and value >= 0 for value in base_values + candidate_values):
                errors.append(f"{case}: invalid {metric} sample")
                continue
            base_p50 = statistics.median(base_values)
            candidate_p50 = statistics.median(candidate_values)
            overhead = (
                (candidate_p50 - base_p50) / base_p50 * 100.0 if base_p50 > 0 else math.inf
            )
            metrics[metric] = {
                "baseline_p50": base_p50,
                "candidate_p50": candidate_p50,
                "overhead_pct": overhead,
            }
            if metric == "actual_latency_us" and math.isfinite(overhead):
                latency_overheads.append(overhead)
        case_rows.append(
            {
                "case": case,
                "baseline_repeat": len(base_rows),
                "candidate_repeat": len(candidate_rows),
                "metrics": metrics,
            }
        )
    matrix_overhead = statistics.median(latency_overheads) if latency_overheads else math.inf
    if not math.isfinite(matrix_overhead) or matrix_overhead > threshold_pct:
        errors.append(
            f"matrix actual_latency p50 overhead {matrix_overhead:.3f}% exceeds "
            f"{threshold_pct:.3f}%"
        )
    report = {
        "status": "PASS" if not errors else "FAIL",
        "threshold_pct": threshold_pct,
        "minimum_repeat": minimum_repeat,
        "matrix_actual_latency_p50_overhead_pct": matrix_overhead,
        "baseline": str(baseline_path.resolve()),
        "candidate": str(candidate_path.resolve()),
        "cases": case_rows,
        "errors": errors,
    }
    return report, errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare Trace v2 disabled fast-path overhead")
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--threshold-pct", type=float, default=3.0)
    parser.add_argument("--minimum-repeat", type=int, default=5)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    try:
        report, errors = compare(
            args.baseline,
            args.candidate,
            args.threshold_pct,
            args.minimum_repeat,
        )
    except Exception as exc:  # noqa: BLE001
        report = {"status": "FAIL", "errors": [str(exc)]}
        errors = [str(exc)]
    output = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(output, encoding="utf-8")
    print(output, end="")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
