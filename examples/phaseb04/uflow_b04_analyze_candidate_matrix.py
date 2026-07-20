from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from statistics import mean
from typing import Any


def read_rows(run_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for candidate_dir in sorted(path for path in run_dir.iterdir() if path.is_dir()):
        summary = candidate_dir / "candidate_summary.csv"
        if not summary.exists():
            continue
        candidate = candidate_dir.name
        with summary.open(newline="") as f:
            for row in csv.DictReader(f):
                row = dict(row)
                row["candidate_dir"] = candidate
                row["bytes_int"] = int(row["bytes"])
                row["bandwidth"] = float(row["actual_bandwidth_gib_s"])
                row["latency_us_float"] = float(row["actual_latency_us"])
                row["direct_acl_us_float"] = float(row.get("direct_acl_us") or 0)
                row["relay_total_us_float"] = float(row.get("relay_total_us") or 0)
                rows.append(row)
    return rows


def ratio_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int, str], dict[str, dict[str, Any]]] = {}
    for row in rows:
        key = (row["candidate_dir"], row["bytes_int"], row["direction"])
        grouped.setdefault(key, {})[row["mode"]] = row
    out: list[dict[str, Any]] = []
    for (candidate, nbytes, direction), modes in sorted(grouped.items(), key=lambda item: (item[0][1], item[0][2], item[0][0])):
        direct = modes.get("ssd_hbm_direct")
        if direct is None:
            auto = modes.get("auto")
            if auto is not None and "direct" in auto.get("actual_path", ""):
                direct = auto
        relay = modes.get("relay")
        if direct is None:
            continue
        relay_bw = float(relay["bandwidth"]) if relay is not None else 0.0
        direct_bw = float(direct["bandwidth"])
        out.append(
            {
                "candidate": candidate,
                "direction": direction,
                "bytes": nbytes,
                "direct_path": direct["actual_path"],
                "direct_engine": direct["actual_engine"],
                "direct_bw_gib_s": direct_bw,
                "relay_bw_gib_s": relay_bw,
                "direct_over_relay": direct_bw / relay_bw if relay_bw > 0 else 0.0,
                "direct_latency_us": float(direct["latency_us_float"]),
                "direct_acl_us": float(direct["direct_acl_us_float"]),
                "relay_total_us": float(relay["relay_total_us_float"]) if relay is not None else 0.0,
                "direct_kind": direct.get("direct_kind", ""),
                "direct_candidate": direct.get("direct_candidate", ""),
            }
        )
    return out


def best_rows(ratios: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[int, str], list[dict[str, Any]]] = {}
    for row in ratios:
        grouped.setdefault((int(row["bytes"]), row["direction"]), []).append(row)
    out: list[dict[str, Any]] = []
    for (nbytes, direction), items in sorted(grouped.items()):
        best = max(items, key=lambda row: float(row["direct_bw_gib_s"]))
        out.append(dict(best))
    return out


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_summary(path: Path, ratios: list[dict[str, Any]], best: list[dict[str, Any]]) -> None:
    lines = ["# PhaseB-04 Candidate Matrix Analysis", ""]
    if not ratios:
        lines.append("No candidate rows found.")
        path.write_text("\n".join(lines) + "\n")
        return
    for direction in sorted({row["direction"] for row in ratios}):
        subset = [row for row in ratios if row["direction"] == direction]
        ratios_only = [float(row["direct_over_relay"]) for row in subset if float(row["relay_bw_gib_s"]) > 0]
        lines.append(f"## {direction}")
        lines.append("")
        lines.append(f"- candidate rows: `{len(subset)}`")
        if ratios_only:
            lines.append(f"- average direct/relay ratio: `{mean(ratios_only):.3f}`")
        winners = [row for row in best if row["direction"] == direction]
        for row in winners:
            lines.append(
                f"- `{row['bytes']}` bytes best: `{row['candidate']}` "
                f"direct={float(row['direct_bw_gib_s']):.3f} GiB/s, "
                f"relay={float(row['relay_bw_gib_s']):.3f} GiB/s, "
                f"ratio={float(row['direct_over_relay']):.3f}"
            )
        lines.append("")
    path.write_text("\n".join(lines) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dir", type=Path)
    args = parser.parse_args()
    rows = read_rows(args.run_dir)
    ratios = ratio_rows(rows)
    best = best_rows(ratios)
    write_csv(args.run_dir / "direct_vs_relay_ratio.csv", ratios)
    write_csv(args.run_dir / "best_direct_by_size.csv", best)
    (args.run_dir / "direct_vs_relay_ratio.json").write_text(json.dumps(ratios, indent=2, sort_keys=True) + "\n")
    write_summary(args.run_dir / "analysis_summary.md", ratios, best)
    print(args.run_dir / "analysis_summary.md")


if __name__ == "__main__":
    main()
