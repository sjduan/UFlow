#!/usr/bin/env python3
"""Fail-closed ub_sim baseline inspection and validation helper.

The tool is intentionally transport-neutral: it operates on an ub_sim checkout
inside the selected validation environment and records machine-readable gate
state.  It never resets, cleans, or updates an existing canonical checkout.
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import shlex
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


SCHEMA_VERSION = 1
DEFAULT_BRANCH_REF = "origin/master"
TOP_LEVEL_REQUIRED = (
    "guest-linux/kernel_ub",
    "mem_service",
    "vendor/obmm",
    "vendor/pto-isa",
    "vendor/pypto",
    "vendor/pypto-lib",
    "vendor/qemu_8.2.0_ub",
    "vendor/simpler",
)
SMOKE_SCRIPTS = {
    "two": "guest-linux/aarch64/scripts/run_ub_dual_node_mem_service_obmm_provider_conformance.sh",
    "four": "guest-linux/aarch64/scripts/run_ub_four_node_smoke.sh",
    "eight": "guest-linux/aarch64/scripts/run_ub_eight_node_smoke.sh",
}


class GuardFailure(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_command(
    argv: Iterable[str],
    *,
    cwd: Path | None = None,
    check: bool = True,
    env: dict[str, str] | None = None,
    log_path: Path | None = None,
    raw_log_path: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    command = list(argv)
    started_at = utc_now()
    started = time.monotonic()
    result = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    record = {
        "argv": command,
        "cwd": str(cwd) if cwd else None,
        "started_at": started_at,
        "duration_seconds": round(time.monotonic() - started, 6),
        "exit_code": result.returncode,
        "output": result.stdout,
    }
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    if raw_log_path:
        raw_log_path.parent.mkdir(parents=True, exist_ok=True)
        raw_log_path.write_text(result.stdout, encoding="utf-8")
    if check and result.returncode != 0:
        raise GuardFailure(
            f"command failed ({result.returncode}): {shlex.join(command)}\n{result.stdout}"
        )
    return result


def git(repo: Path, *args: str, check: bool = True) -> str:
    return run_command(["git", "-C", str(repo), *args], check=check).stdout.strip()


def require_repo(repo: Path) -> Path:
    resolved = repo.resolve()
    if not resolved.is_dir():
        raise GuardFailure(f"ub_sim repo not found: {resolved}")
    if git(resolved, "rev-parse", "--is-inside-work-tree") != "true":
        raise GuardFailure(f"not a Git worktree: {resolved}")
    return resolved


def load_state(output_dir: Path) -> dict[str, Any]:
    state_path = output_dir / "baseline.json"
    if state_path.is_file():
        return json.loads(state_path.read_text(encoding="utf-8"))
    return {
        "schema_version": SCHEMA_VERSION,
        "created_at": utc_now(),
        "gates": {},
        "commands": [],
    }


def save_state(output_dir: Path, state: dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    state["schema_version"] = SCHEMA_VERSION
    state["updated_at"] = utc_now()
    (output_dir / "baseline.json").write_text(
        json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def parse_lock(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise GuardFailure(f"invalid lock line in {path}: {line}")
        key, value = line.split("=", 1)
        values[key] = value
    required = {"lock_version", "version", "revision"}
    missing = sorted(required - values.keys())
    if missing:
        raise GuardFailure(f"missing lock keys in {path}: {','.join(missing)}")
    return values


def tree_gitlink(repo: Path, revision: str, path: str) -> str:
    output = git(repo, "ls-tree", revision, path)
    fields = output.split()
    if len(fields) < 3 or fields[0] != "160000" or fields[1] != "commit":
        raise GuardFailure(f"missing gitlink {path} at {revision}")
    return fields[2]


def actual_submodule_head(repo: Path, path: str) -> str:
    submodule = repo / path
    if not submodule.is_dir():
        raise GuardFailure(f"submodule checkout missing: {path}")
    return git(submodule, "rev-parse", "HEAD")


def inspect(args: argparse.Namespace) -> int:
    repo = require_repo(args.repo)
    output_dir = args.output_dir.resolve()
    if args.offline and args.fetch:
        raise GuardFailure("--offline and --fetch cannot be used together")
    if args.fetch:
        run_command(["git", "-C", str(repo), "fetch", args.remote, "--prune"])
    candidate_ref = args.candidate_sha or args.candidate_ref
    head = git(repo, "rev-parse", "HEAD")
    candidate = git(repo, "rev-parse", candidate_ref)
    dirty = bool(git(repo, "status", "--porcelain", "--untracked-files=all"))
    dependencies = {
        path: tree_gitlink(repo, candidate_ref, path) for path in TOP_LEVEL_REQUIRED
    }
    state = load_state(output_dir)
    state.update(
        {
            "environment": {
                "host": socket.gethostname(),
                "user": os.environ.get("USER") or getpass.getuser(),
                "workspace_root": str(repo.parent.parent),
            },
            "canonical_repo": str(repo),
            "canonical_sha": head,
            "canonical_dirty": dirty,
            "remote": args.remote,
            "candidate_ref": candidate_ref,
            "source_mode": "offline" if args.offline else "online",
            "upstream_candidate_sha": candidate,
            "upstream_dependencies": dependencies,
        }
    )
    state["gates"]["U0"] = {"status": "pass", "checked_at": utc_now()}
    save_state(output_dir, state)
    print(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def stage(args: argparse.Namespace) -> int:
    source_repo = require_repo(args.source_repo)
    stage_dir = args.stage_dir.resolve()
    if stage_dir.exists():
        raise GuardFailure(f"stage directory already exists: {stage_dir}")
    remote_url = git(source_repo, "remote", "get-url", args.remote)
    candidate = git(source_repo, "rev-parse", args.candidate_ref)
    run_command(["git", "clone", "--no-checkout", remote_url, str(stage_dir)])
    run_command(["git", "-C", str(stage_dir), "checkout", "--detach", candidate])
    run_command(["git", "-C", str(stage_dir), "submodule", "sync", "--recursive"])
    run_command(["git", "-C", str(stage_dir), "submodule", "update", "--init"])
    print(f"stage_repo={stage_dir}")
    print(f"candidate_sha={candidate}")
    return 0


def check_sources(repo: Path) -> dict[str, Any]:
    dirty = git(repo, "status", "--porcelain", "--untracked-files=no")
    if dirty:
        raise GuardFailure(f"candidate tracked tree is dirty:\n{dirty}")
    dependencies: dict[str, dict[str, str]] = {}
    for path in TOP_LEVEL_REQUIRED:
        expected = tree_gitlink(repo, "HEAD", path)
        actual = actual_submodule_head(repo, path)
        if expected != actual:
            raise GuardFailure(
                f"submodule mismatch for {path}: expected {expected}, got {actual}"
            )
        dependencies[path] = {"expected": expected, "actual": actual}

    lock_file = repo / "guest-linux/aarch64/mem_service.lock"
    lock = parse_lock(lock_file)
    mem_head = actual_submodule_head(repo, "mem_service")
    mem_version = (repo / "mem_service/VERSION").read_text(encoding="utf-8").strip()
    mem_gitlink = tree_gitlink(repo, "HEAD", "mem_service")
    if not (lock["revision"] == mem_head == mem_gitlink):
        raise GuardFailure(
            "mem_service gitlink/lock/HEAD mismatch: "
            f"gitlink={mem_gitlink} lock={lock['revision']} head={mem_head}"
        )
    if lock["version"] != mem_version:
        raise GuardFailure(
            f"mem_service VERSION mismatch: lock={lock['version']} file={mem_version}"
        )
    return {"dependencies": dependencies, "mem_service_lock": lock}


def check(args: argparse.Namespace) -> int:
    repo = require_repo(args.repo)
    output_dir = args.output_dir.resolve()
    state = load_state(output_dir)
    try:
        source_result = check_sources(repo)
        verifier = repo / "guest-linux/aarch64/scripts/verify_mem_service_source.py"
        run_command(
            [
                sys.executable,
                str(verifier),
                "--mem-service-root",
                str(repo / "mem_service"),
                "--lock-file",
                str(repo / "guest-linux/aarch64/mem_service.lock"),
            ],
            cwd=repo,
            log_path=output_dir / "commands.jsonl",
        )
        state["candidate_sha"] = git(repo, "rev-parse", "HEAD")
        state["source"] = source_result
        state["gates"]["U1"] = {"status": "pass", "checked_at": utc_now()}
    except GuardFailure as error:
        state["gates"]["U1"] = {
            "status": "fail",
            "checked_at": utc_now(),
            "reason": str(error),
        }
        state["status"] = "latest-unreachable"
        save_state(output_dir, state)
        raise

    if args.run_tests:
        test_commands = [
            ["cargo", "fmt", "--all", "--", "--check"],
            ["cargo", "test", "--workspace"],
            [
                sys.executable,
                "-m",
                "unittest",
                "discover",
                "guest-linux/aarch64/tests",
            ],
        ]
        try:
            for command in test_commands:
                run_command(
                    command,
                    cwd=repo,
                    log_path=output_dir / "commands.jsonl",
                )
            state["gates"]["U2"] = {"status": "pass", "checked_at": utc_now()}
        except GuardFailure as error:
            state["gates"]["U2"] = {
                "status": "fail",
                "checked_at": utc_now(),
                "reason": str(error),
            }
            state["status"] = "latest-failed"
            save_state(output_dir, state)
            raise

    save_state(output_dir, state)
    print(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def qemu_pids() -> set[int]:
    result = run_command(["pgrep", "-f", "qemu-system-aarch64"], check=False)
    if result.returncode not in (0, 1):
        raise GuardFailure(f"pgrep failed: {result.stdout}")
    return {int(line) for line in result.stdout.splitlines() if line.strip()}


def smoke(args: argparse.Namespace) -> int:
    repo = require_repo(args.repo)
    output_dir = args.output_dir.resolve()
    state = load_state(output_dir)
    script = repo / SMOKE_SCRIPTS[args.tier]
    if not script.is_file():
        raise GuardFailure(f"smoke entrypoint missing: {script}")
    before = qemu_pids()
    environment = os.environ.copy()
    environment["RUN_ID"] = args.run_id
    try:
        run_command(
            [str(script)],
            cwd=repo / "guest-linux/aarch64",
            env=environment,
            log_path=output_dir / "commands.jsonl",
            raw_log_path=output_dir / f"smoke_{args.tier}.log",
        )
        after = qemu_pids()
        leaked = sorted(after - before)
        if leaked:
            raise GuardFailure(f"new QEMU processes remain after smoke: {leaked}")
        gate = "U3" if args.tier == "two" else "U4"
        state["gates"][gate] = {
            "status": "pass",
            "checked_at": utc_now(),
            "tier": args.tier,
        }
    except GuardFailure as error:
        gate = "U3" if args.tier == "two" else "U4"
        state["gates"][gate] = {
            "status": "fail",
            "checked_at": utc_now(),
            "tier": args.tier,
            "reason": str(error),
        }
        state["status"] = "latest-failed"
        save_state(output_dir, state)
        raise
    save_state(output_dir, state)
    print(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def record(args: argparse.Namespace) -> int:
    """Record a gate backed by existing, non-empty evidence files."""
    output_dir = args.output_dir.resolve()
    evidence = [path.resolve() for path in args.evidence]
    missing = [str(path) for path in evidence if not path.is_file()]
    empty = [str(path) for path in evidence if path.is_file() and path.stat().st_size == 0]
    if missing:
        raise GuardFailure(f"evidence files missing: {', '.join(missing)}")
    if empty:
        raise GuardFailure(f"evidence files empty: {', '.join(empty)}")

    state = load_state(output_dir)
    gate: dict[str, Any] = {
        "status": args.status,
        "checked_at": utc_now(),
        "evidence": [str(path) for path in evidence],
    }
    if args.reason:
        gate["reason"] = args.reason
    existing_gate = state["gates"].get(args.gate, {})
    existing_gate.update(gate)
    state["gates"][args.gate] = existing_gate
    if args.status == "fail":
        state["status"] = "latest-failed"
    save_state(output_dir, state)
    print(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if args.status == "pass" else 1


def report(args: argparse.Namespace) -> int:
    output_dir = args.output_dir.resolve()
    state = load_state(output_dir)
    gates = state.get("gates", {})
    required = ["U0", "U1", "U2", "U3"]
    if args.require_u4:
        required.append("U4")
    ready = all(gates.get(name, {}).get("status") == "pass" for name in required)
    repaired = bool(args.repaired)
    state["ready"] = ready
    state["required_gates"] = required
    state["latest"] = bool(args.latest) and state.get("source_mode") != "offline"
    state["repaired"] = repaired
    if ready and repaired and state["latest"]:
        state["status"] = "latest-repaired-ready"
    elif ready and state["latest"]:
        state["status"] = "latest-ready"
    elif ready:
        state["status"] = "last-known-good"
    elif state.get("status") not in {"latest-unreachable", "latest-failed"}:
        state["status"] = "latest-testing"
    save_state(output_dir, state)
    summary_lines = [
        "# ub-sim baseline summary",
        "",
        f"- status: `{state['status']}`",
        f"- upstream candidate: `{state.get('upstream_candidate_sha', 'unknown')}`",
        f"- validated candidate: `{state.get('candidate_sha', 'unknown')}`",
        f"- latest: `{str(state['latest']).lower()}`",
        f"- repaired: `{str(repaired).lower()}`",
        f"- ready: `{str(ready).lower()}`",
        f"- required gates: `{','.join(required)}`",
        "",
        "## Gates",
        "",
    ]
    for name in ("U0", "U1", "U2", "U3", "U4"):
        gate = gates.get(name)
        if gate:
            summary_lines.append(f"- {name}: `{gate.get('status', 'unknown')}`")
    summary_lines.extend(
        [
            "",
            "`baseline.json` is the machine-readable source of truth for this report.",
            "",
        ]
    )
    (output_dir / "summary.md").write_text(
        "\n".join(summary_lines), encoding="utf-8"
    )
    print(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if ready else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    inspect_parser = subparsers.add_parser("inspect")
    inspect_parser.add_argument("--repo", type=Path, required=True)
    inspect_parser.add_argument("--output-dir", type=Path, required=True)
    inspect_parser.add_argument("--remote", default="origin")
    inspect_parser.add_argument("--candidate-ref", default=DEFAULT_BRANCH_REF)
    inspect_parser.add_argument("--candidate-sha")
    inspect_parser.add_argument("--fetch", action="store_true")
    inspect_parser.add_argument("--offline", action="store_true")
    inspect_parser.set_defaults(func=inspect)

    stage_parser = subparsers.add_parser("stage")
    stage_parser.add_argument("--source-repo", type=Path, required=True)
    stage_parser.add_argument("--stage-dir", type=Path, required=True)
    stage_parser.add_argument("--remote", default="origin")
    stage_parser.add_argument("--candidate-ref", default=DEFAULT_BRANCH_REF)
    stage_parser.set_defaults(func=stage)

    check_parser = subparsers.add_parser("check")
    check_parser.add_argument("--repo", type=Path, required=True)
    check_parser.add_argument("--output-dir", type=Path, required=True)
    check_parser.add_argument("--run-tests", action="store_true")
    check_parser.set_defaults(func=check)

    smoke_parser = subparsers.add_parser("smoke")
    smoke_parser.add_argument("--repo", type=Path, required=True)
    smoke_parser.add_argument("--output-dir", type=Path, required=True)
    smoke_parser.add_argument("--tier", choices=sorted(SMOKE_SCRIPTS), required=True)
    smoke_parser.add_argument("--run-id", required=True)
    smoke_parser.set_defaults(func=smoke)

    record_parser = subparsers.add_parser("record")
    record_parser.add_argument("--output-dir", type=Path, required=True)
    record_parser.add_argument("--gate", choices=["U0", "U1", "U2", "U3", "U4"], required=True)
    record_parser.add_argument("--status", choices=["pass", "fail"], required=True)
    record_parser.add_argument("--evidence", type=Path, action="append", required=True)
    record_parser.add_argument("--reason")
    record_parser.set_defaults(func=record)

    report_parser = subparsers.add_parser("report")
    report_parser.add_argument("--output-dir", type=Path, required=True)
    report_parser.add_argument("--latest", action="store_true")
    report_parser.add_argument("--repaired", action="store_true")
    report_parser.add_argument("--require-u4", action="store_true")
    report_parser.set_defaults(func=report)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        return args.func(args)
    except GuardFailure as error:
        print(f"ub_sim_baseline_guard: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
