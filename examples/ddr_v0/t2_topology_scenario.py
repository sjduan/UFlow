#!/usr/bin/env python3
"""T2 discovery/guard against a real T1 daemon and real independent owners.

The Rust helper consumes the same T1 wire operation as the SDK, not fixtures.
No T3 submit/copy or zero-side-effect guarantee is claimed here.
"""
from __future__ import annotations

import argparse
import json
import multiprocessing
import os
import subprocess
import time
from pathlib import Path

from t1_external_ddr_smoke import (
    ExternalBackingHandle, ExternalDdrClient, ExternalDdrError, SIZE,
    control_request, must_fail, owner_process, response,
)


def wait_broker(path: str, daemon: subprocess.Popen) -> None:
    deadline = time.monotonic() + 15
    while not Path(path + ".external-ddr").exists():
        if daemon.poll() is not None:
            raise RuntimeError(f"daemon exited {daemon.returncode}")
        if time.monotonic() > deadline:
            raise TimeoutError("broker did not start")
        time.sleep(0.02)


def command(helper: subprocess.Popen, text: str, *, ok: bool = True) -> dict:
    assert helper.stdin is not None and helper.stdout is not None
    helper.stdin.write(text + "\n")
    helper.stdin.flush()
    line = helper.stdout.readline()
    if not line:
        raise RuntimeError(f"discovery helper stopped ({helper.poll()}) after {text}")
    result = dict(item.split("=", 1) for item in line.rstrip("\n").split(";"))
    assert result.get("status") == ("ok" if ok else "error"), (text, result)
    return result


def number(row: dict, name: str) -> int:
    return int(row[name])


def run(args) -> dict:
    root = Path(args.run_dir)
    if root.exists():
        raise RuntimeError(f"refusing to overwrite existing scenario directory {root}")
    (root / "socket").mkdir(parents=True)
    (root / "results").mkdir()
    control = str(root / "socket" / "u.sock")
    if len(os.fsencode(control + ".external-ddr")) >= 108:
        raise RuntimeError("Unix socket path too long")
    details: dict = {"phases": [], "status": "running", "processes": [], "daemon_exits": []}
    env = os.environ.copy()
    env.update({"UF_EXTERNAL_DDR_HOST_ID": args.host_id, "UF_HBM_STARTUP_PROBE_BYTES": "0",
                "UF_EXTERNAL_DDR_OWNER_MAX_BYTES": str(16 * 1024 * 1024),
                "UF_EXTERNAL_DDR_TOTAL_MAX_BYTES": str(64 * 1024 * 1024)})
    daemons: list[subprocess.Popen] = []
    logs = []
    owners = []
    helper = None
    cleanup_errors = []

    def start_daemon() -> subprocess.Popen:
        log = open(root / "results" / f"daemon-{len(daemons)}.log", "w", encoding="utf-8")
        logs.append(log)
        proc = subprocess.Popen([args.daemon, "--socket", control, "--startup-probe-bytes", "0"],
                                env=env, stdout=log, stderr=subprocess.STDOUT)
        daemons.append(proc)
        wait_broker(control, proc)
        return proc

    def stop_daemon(proc: subprocess.Popen) -> None:
        assert control_request(control, op="ShutdownDaemon")["status"] == "ok"
        proc.wait(timeout=15)
        assert proc.returncode == 0
        details["daemon_exits"].append(proc.returncode)
        assert not Path(control).exists() and not Path(control + ".external-ddr").exists()

    ctx = multiprocessing.get_context("spawn")

    def start_owner(name: str, seed: int):
        parent, child = ctx.Pipe()
        proc = ctx.Process(target=owner_process, args=(control, name, seed, child), name=name)
        proc.start()
        child.close()
        owners.append((proc, parent))
        record = response(parent, "registered")
        details["processes"].append({"name": name, "pid": proc.pid})
        return proc, parent, record[1]

    try:
        daemon = start_daemon()
        client = ExternalDdrClient(control)
        helper = subprocess.Popen([args.helper, control, "2000"], stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE, stderr=open(root / "results" / "helper.log", "w"),
                                  text=True, bufsize=1)
        initial = command(helper, "snapshot")
        assert initial["host_id"] == args.host_id and number(initial, "committed_bytes") == 0
        assert number(initial, "nodes") > 0 and number(initial, "nodes_measured") > 0
        assert initial["page_unknown"] == initial["technology_unknown"] == "true"
        details["phases"].append({"empty": initial})

        a, pa, ha = start_owner("owner-a", 11)
        b, pb, hb = start_owner("owner-b", 29)
        before = command(helper, "capture " + str(ha["object_id"]))
        assert number(before, "epoch") > number(initial, "epoch")
        assert number(before, "committed_bytes") == 2 * SIZE and number(before, "retained_count") == 2
        assert before["placement_counts_one"] == before["numa_unknown"] == "true"
        assert number(before, "pools") == 2 and number(before, "pool_retained_sum") == 2 * SIZE
        assert number(before, "pool_advisory_sum") <= number(before, "advisory_budget")
        assert number(before, "pool_advisory_sum") + number(before, "committed_bytes") <= number(before, "budget_limit")
        assert number(before, "pool_reservable_sum") == 0  # not an atomic reservation service
        assert number(before, "unknown_map") == 4 and number(before, "unsupported_copy") == 4
        assert number(before, "supported_copy") == 0
        command(helper, "verify ok")
        rejected = {}
        for mode in ("host", "boot", "broker", "owner", "pool", "resource", "object",
                     "epoch", "topology", "capability", "realm", "range"):
            rejected[mode] = command(helper, "verify " + mode, ok=False)["detail"]
        details["phases"].append({"registered": before, "forged_guard_rejections": rejected})

        pa.send(("import", hb, 29))
        pb.send(("import", ha, 11))
        response(pa, "imported")
        response(pb, "imported")
        stale_views = command(helper, "verify ok", ok=False)
        assert "stale broker" in stale_views["detail"]
        mapped = command(helper, "snapshot")
        assert number(mapped, "active_views") == 2 and number(mapped, "committed_bytes") == 2 * SIZE
        assert number(mapped, "retained_count") == 2 and mapped["placement_counts_one"] == "true"
        assert number(mapped, "topology_generation") > number(before, "topology_generation")
        # A read-only broker connection closes per SDK request. Disconnect does
        # not destroy either already-committed view.
        assert client.snapshot()["active_views"] == 2
        assert command(helper, "snapshot")["active_views"] == "2"
        details["phases"].append({"views": mapped, "old_guard": stale_views, "disconnected_views_survive": True})

        command(helper, "capture " + str(hb["object_id"]))
        pa.send(("exit",))
        a.join(15)
        assert a.exitcode == 0
        pb.send(("owner_exited", ha, 11))
        response(pb, "owner_exit_ok")  # existing view still reads the dead owner's payload
        dead_reject = command(helper, "verify ok", ok=False)
        assert "stale broker" in dead_reject["detail"]
        after_exit = command(helper, "snapshot")
        assert f'{ha["object_id"]}:owner_exited:false' in after_exit["backings"]
        assert number(after_exit, "committed_bytes") == 2 * SIZE
        details["phases"].append({"owner_exit": after_exit, "stale_guard": dead_reject, "live_view_read": True})

        pb.send(("release_view",))
        response(pb, "view_released")
        after_unmap = command(helper, "snapshot")
        assert number(after_unmap, "committed_bytes") == SIZE
        assert f'{ha["object_id"]}:owner_exited:true' in after_unmap["backings"]
        command(helper, "capture " + str(hb["object_id"]))
        pb.send(("revoke",))
        response(pb, "revoked")
        revoke_reject = command(helper, "verify ok", ok=False)
        assert "stale broker" in revoke_reject["detail"]
        after_revoke = command(helper, "snapshot")
        assert number(after_revoke, "committed_bytes") == 0
        details["phases"].append({"unmapped": after_unmap, "revoked": after_revoke, "stale_guard": revoke_reject})

        c, pc, hc = start_owner("owner-c", 47)
        old = ExternalBackingHandle(**hc)
        old_capture = command(helper, "capture " + str(hc["object_id"]))
        time.sleep(2.1)
        expired = command(helper, "verify ok", ok=False)
        assert "stale snapshot" in expired["detail"]
        fresh = command(helper, "capture " + str(hc["object_id"]))
        command(helper, "verify ok")
        stop_daemon(daemon)
        daemon = start_daemon()
        restart_error = command(helper, "verify ok", ok=False)
        assert "stale broker" in restart_error["detail"]
        after_restart = command(helper, "snapshot")
        assert after_restart["broker_instance"] != fresh["broker_instance"]
        stale_handle_error = must_fail(lambda: client.acquire(old, offset_bytes=0, nbytes=4), ExternalDdrError)
        assert number(after_restart, "committed_bytes") == 0
        details["phases"].append({"freshness_expired": expired, "restart_guard": restart_error,
                                  "restart_snapshot": after_restart, "old_handle_error": stale_handle_error,
                                  "before_restart": fresh, "old_capture": old_capture})
        pb.send(("exit",))
        pc.send(("exit",))
        b.join(15)
        c.join(15)
        assert b.exitcode == c.exitcode == 0
        stop_daemon(daemon)
        details["status"] = "pass"
        return details
    finally:
        for proc, pipe in owners:
            if proc.is_alive():
                try:
                    pipe.send(("exit",))
                except (BrokenPipeError, OSError, EOFError):
                    pass
        for proc, pipe in owners:
            proc.join(15)
            if proc.is_alive() or proc.exitcode != 0:
                cleanup_errors.append(f"owner pid={proc.pid} exit={proc.exitcode}")
            pipe.close()
        for daemon in daemons:
            if daemon.poll() is None:
                try:
                    stop_daemon(daemon)
                except Exception as exc:
                    cleanup_errors.append(f"daemon pid={daemon.pid}: {exc}")
        if helper is not None:
            if helper.poll() is None:
                try:
                    assert helper.stdin is not None
                    helper.stdin.write("quit\n")
                    helper.stdin.flush()
                    helper.wait(timeout=10)
                except (OSError, subprocess.TimeoutExpired) as exc:
                    cleanup_errors.append(f"helper pid={helper.pid}: {exc}")
            details["helper_exit_code"] = helper.poll()
            if helper.returncode != 0:
                cleanup_errors.append(f"helper exit={helper.returncode}")
        for log in logs:
            log.close()
        details["socket_removed"] = not Path(control).exists() and not Path(control + ".external-ddr").exists()
        details["cleanup_errors"] = cleanup_errors
        (root / "results" / "t2-scenario.json").write_text(json.dumps(details, indent=2), encoding="utf-8")
        if cleanup_errors or not details["socket_removed"]:
            raise RuntimeError(f"cleanup failed: {cleanup_errors}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--daemon", required=True)
    parser.add_argument("--helper", required=True)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--host-id", required=True)
    args = parser.parse_args()
    result = run(args)
    print(json.dumps(result, indent=2))
    print("UFLOW_DDR_V0_T2_DISCOVERY_GUARD_PASS")


if __name__ == "__main__":
    main()
