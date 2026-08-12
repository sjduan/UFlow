#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path


def process_command_lines() -> list[tuple[int, str]]:
    processes: list[tuple[int, str]] = []
    for entry in Path("/proc").glob("[0-9]*"):
        try:
            pid = int(entry.name)
            if pid == os.getpid():
                continue
            command = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode(
                "utf-8", "replace"
            )
        except (OSError, ValueError):
            continue
        if command:
            processes.append((pid, command))
    return processes


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify PhaseE-06 scoped remote cleanup")
    parser.add_argument("--run-id", action="append", required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()

    run_ids = args.run_id
    expected_removed = [
        path
        for run_id in run_ids
        for path in (
            Path(f"/tmp/{run_id}.sock"),
            Path(f"/dev/shm/{run_id}_ddr"),
            Path(f"/data/{run_id}_ssd"),
        )
    ]
    remaining_paths = [str(path) for path in expected_removed if path.exists()]
    live_processes = [
        {"pid": pid, "command": command}
        for pid, command in process_command_lines()
        if "check_remote_cleanup.py" not in command
        and any(run_id in command for run_id in run_ids)
    ]
    report = {
        "status": "PASS" if not remaining_paths and not live_processes else "FAIL",
        "checked_run_ids": run_ids,
        "remaining_paths": remaining_paths,
        "live_test_processes": live_processes,
    }
    output = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(output, encoding="utf-8")
    print(output, end="")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
