#!/usr/bin/env python3
"""Prove that the deterministic PhaseC-02 payload did not enter Host files."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--seed-id", type=int, required=True)
    parser.add_argument("--nbytes", type=int, default=4096)
    return parser.parse_args()


def payload(seed_id: int, nbytes: int) -> bytes:
    return bytes((seed_id + index * 29) & 0xFF for index in range(nbytes))


def scan_file(path: Path, needles: list[tuple[str, bytes]]) -> tuple[str, dict[str, int]]:
    digest = hashlib.sha256()
    counts = {name: 0 for name, _ in needles}
    overlap = max(len(needle) for _, needle in needles) - 1
    previous = b""
    with path.open("rb") as stream:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            window = previous + chunk
            for name, needle in needles:
                cursor = 0
                while True:
                    found = window.find(needle, cursor)
                    if found < 0:
                        break
                    counts[name] += 1
                    cursor = found + 1
            previous = window[-overlap:] if overlap else b""
    return digest.hexdigest(), counts


def main() -> int:
    args = parse_args()
    run_root = Path(args.run_root).resolve()
    output = Path(args.output).resolve()
    raw = payload(args.seed_id, args.nbytes)
    chunks = [
        ("full_payload", raw),
        ("canary_offset_1024_len_512", raw[1024:1536]),
    ]
    files: list[dict[str, object]] = []
    totals = {name: 0 for name, _ in chunks}
    control_plane_hits = 0
    simulator_data_plane_hits = 0
    unexpected_hits = 0
    for root, _, names in os.walk(run_root):
        for name in sorted(names):
            path = Path(root, name)
            if path.resolve() == output or not path.is_file():
                continue
            try:
                sha256, counts = scan_file(path, chunks)
            except (OSError, PermissionError):
                continue
            relative = path.relative_to(run_root)
            hit_count = sum(counts.values())
            is_simulator_data_plane = (
                len(relative.parts) >= 2
                and relative.parts[0] == "fm"
                and relative.suffix == ".ring"
            )
            is_control_plane = relative.as_posix() in {
                "evidence/bridge_frames.capture",
                "evidence/daemon_control_frames.capture",
            }
            for key, count in counts.items():
                totals[key] += count
            if is_simulator_data_plane:
                simulator_data_plane_hits += hit_count
            elif is_control_plane:
                control_plane_hits += hit_count
            else:
                unexpected_hits += hit_count
            files.append(
                {
                    "path": str(relative),
                    "size_bytes": path.stat().st_size,
                    "sha256": sha256,
                    "matches": counts,
                    "classification": (
                        "simulator_data_plane_backing"
                        if is_simulator_data_plane
                        else "host_control_plane"
                        if is_control_plane
                        else "host_regular_artifact"
                    ),
                }
            )
    result = {
        "schema": "uflow-phasec02-payload-path-assertion-v1",
        "status": "pass" if control_plane_hits == 0 and unexpected_hits == 0 else "fail",
        "run_root": str(run_root),
        "seed_id": args.seed_id,
        "nbytes": args.nbytes,
        "payload_sha256": hashlib.sha256(raw).hexdigest(),
        "canaries": {
            name: {"bytes": len(value), "sha256": hashlib.sha256(value).hexdigest()}
            for name, value in chunks
        },
        "files_scanned": len(files),
        "total_matches": totals,
        "control_plane_hit_count": control_plane_hits,
        "simulator_data_plane_hit_count": simulator_data_plane_hits,
        "unexpected_hit_count": unexpected_hits,
        "files": files,
        "assertion": (
            "Host bridge/control and ordinary artifacts contain no payload canary; "
            "ub-sim FM ring matches are classified as the simulator fabric data plane"
        ),
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(
        json.dumps(
            {
                key: result[key]
                for key in (
                    "status",
                    "files_scanned",
                    "control_plane_hit_count",
                    "simulator_data_plane_hit_count",
                    "unexpected_hit_count",
                )
            }
        )
    )
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
