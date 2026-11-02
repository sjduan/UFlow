#!/usr/bin/env python3
"""Real Linux two-owner external DDR smoke; does not request an NPU allocation."""
from __future__ import annotations

import argparse
import array
import dataclasses
import fcntl
import json
import mmap
import multiprocessing
import os
import socket
import subprocess
import sys
import time
import traceback
from pathlib import Path

SDK_DIR = Path(__file__).resolve().parents[2] / "sdk" / "python" / "uflow"
sys.path.insert(0, str(SDK_DIR))
import external_ddr  # noqa: E402
from external_ddr import ExternalBackingHandle, ExternalDdrClient, ExternalDdrError  # noqa: E402

SIZE = 65536
OFFSET = 123
VIEW_BYTES = 8192
CHILD_OFFSET = 13
CHILD_BYTES = 1024


def payload(seed: int) -> bytes:
    data = bytearray((i * 17 + seed) % 251 for i in range(SIZE))
    data[37:42] = bytes([seed, 1, 2, 3, 4])
    return bytes(data)


def new_memfd(name: str, size: int, *, seal: bool = True) -> int:
    fd = os.memfd_create(name, os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
    os.ftruncate(fd, size)
    if seal:
        fcntl.fcntl(fd, fcntl.F_ADD_SEALS, fcntl.F_SEAL_GROW | fcntl.F_SEAL_SHRINK)
    return fd


def must_fail(fn, *types) -> str:
    try:
        fn()
    except types as exc:
        return str(exc)
    raise AssertionError("operation unexpectedly succeeded")


def quiet_snapshot(client: ExternalDdrClient, expected: tuple[int, int, int], timeout: float = 4.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = client.snapshot()
        actual = (result["committed_bytes"], result["backing_fds"], result["active_views"])
        if actual == expected:
            return result
        time.sleep(0.02)
    raise AssertionError(f"counters did not recover: expected={expected} actual={actual}")


def stable_fd_count(pid: int, timeout: float = 4.0) -> int:
    deadline = time.monotonic() + timeout
    previous = None
    repeats = 0
    while time.monotonic() < deadline:
        count = len(os.listdir(f"/proc/{pid}/fd"))
        repeats = repeats + 1 if count == previous else 0
        if repeats >= 3:
            return count
        previous = count
        time.sleep(0.05)
    raise AssertionError(f"daemon FD count did not settle: {previous}")


def device_fd_sample(pid: int) -> dict:
    manager = []
    numbered = []
    other = []
    for entry in Path(f"/proc/{pid}/fd").iterdir():
        try:
            target = os.readlink(entry)
        except FileNotFoundError:
            continue
        if target == "/dev/davinci_manager":
            manager.append(target)
        elif target.startswith("/dev/davinci") and target[len("/dev/davinci"):].isdigit():
            numbered.append(target)
        elif target.startswith("/dev/ascend"):
            other.append(target)
    assert not numbered, numbered
    return {"manager": manager, "numbered_npu": numbered, "other_ascend": other}


def abort_provisional(client: ExternalDdrClient, request: dict, fd: int | None = None) -> dict:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(4)
        sock.connect(client.socket_path)
        data = client._encode(request)
        if fd is None:
            sock.sendall(data)
        else:
            sent = sock.sendmsg([data], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", [fd]))])
            sock.sendall(data[sent:])
        reply, ancillary, flags, _ = sock.recvmsg(65536, socket.CMSG_SPACE(16 * array.array("i").itemsize))
        assert not flags & socket.MSG_CTRUNC
        received = []
        for level, kind, raw in ancillary:
            if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
                fds = array.array("i")
                fds.frombytes(raw[:len(raw) - len(raw) % fds.itemsize])
                received.extend(fds)
        for received_fd in received:
            os.close(received_fd)
        fields = client._decode(reply.split(b"\n", 1)[0])
        assert fields.get("status") == "ok", fields
        snapshot = client.snapshot()
        assert snapshot["pending_handoffs"] >= 1
        if request["op"] == "RegisterExternalBacking":
            row = next(r for r in snapshot["backings"] if r["object_id"] == int(fields["object_id"]))
            assert row["state"] == "pending"
            pending = ExternalBackingHandle.from_response(fields)
            must_fail(lambda: client.acquire(pending, offset_bytes=0, nbytes=1), ExternalDdrError)
        if request["op"] == "AcquireExternalRead":
            assert len(received) == 1
        # Deliberately close without the acknowledgement: the broker must roll back.
        return fields


def owner_process(control: str, label: str, seed: int, pipe) -> None:
    fd = -1
    mapped = None
    child = None
    client = ExternalDdrClient(control)
    try:
        fd = os.memfd_create(f"{label}-owned", os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
        os.ftruncate(fd, SIZE)
        mapped = mmap.mmap(fd, SIZE, access=mmap.ACCESS_WRITE)
        before = bytes((i * 17 + seed) % 251 for i in range(SIZE))
        mapped[:] = before  # writes every page in this owner process
        mapped[37:42] = bytes([seed, 1, 2, 3, 4])
        assert mapped[:37] == before[:37] and mapped[42:] == before[42:]
        expected = payload(seed)
        assert mapped[:] == expected
        fcntl.fcntl(fd, fcntl.F_ADD_SEALS, fcntl.F_SEAL_GROW | fcntl.F_SEAL_SHRINK)
        handle = client.register(fd, name=f"{label}-object", owner_name=label, size_bytes=SIZE)
        assert mapped[:] == expected, "registration changed owner data"
        pipe.send(("registered", dataclasses.asdict(handle), os.getpid()))
        while True:
            command, *args = pipe.recv()
            if command == "import":
                other = ExternalBackingHandle(**args[0])
                other_seed = args[1]
                root = client.acquire(other, offset_bytes=OFFSET, nbytes=VIEW_BYTES)
                assert root.read() == payload(other_seed)[OFFSET:OFFSET + VIEW_BYTES]
                assert root.source_address_domain == other.owner_incarnation
                assert root.source_address_domain != root.accessor_address_domain
                must_fail(lambda: client.revoke(other), ExternalDdrError)
                must_fail(lambda: root.read(VIEW_BYTES, 1), ValueError)
                must_fail(lambda: root.read(-1, 1), ValueError)
                must_fail(lambda: root._mapping.__setitem__(0, 9), TypeError)
                must_fail(lambda: client._request("AcquireExternalRead", **other.identity_fields(), access_mode="read", offset_bytes=SIZE - 2, nbytes=3), ExternalDdrError)
                child = root.retain(CHILD_OFFSET, CHILD_BYTES)
                root.release()
                root.release()
                assert child.read() == payload(other_seed)[OFFSET + CHILD_OFFSET:OFFSET + CHILD_OFFSET + CHILD_BYTES]
                assert mapped[:] == expected
                pipe.send(("imported", child.view_id, child.lease_id))
            elif command == "faults":
                snap = client.snapshot()
                baseline = (snap["committed_bytes"], snap["backing_fds"], snap["active_views"])
                duplicate = must_fail(lambda: client.register(fd, name="duplicate", owner_name=label, size_bytes=SIZE), ExternalDdrError)
                unsealed_fd = new_memfd("unsealed", 4096, seal=False)
                try:
                    unsealed = must_fail(lambda: client.register(unsealed_fd, name="unsealed", owner_name=label, size_bytes=4096), ExternalDdrError)
                finally:
                    os.close(unsealed_fd)
                large_fd = new_memfd("quota", 16 * 1024 * 1024 + 1)
                try:
                    quota = must_fail(lambda: client.register(large_fd, name="quota", owner_name=label, size_bytes=16 * 1024 * 1024 + 1), ExternalDdrError)
                finally:
                    os.close(large_fd)
                extra_fd = new_memfd("rollback", 4096)
                try:
                    abort_provisional(client, {"op": "RegisterExternalBacking", "name": "rollback", "owner_name": label, "size_bytes": 4096}, extra_fd)
                finally:
                    os.close(extra_fd)
                quiet_snapshot(client, baseline)
                abort_provisional(client, {"op": "AcquireExternalRead", **handle.identity_fields(), "access_mode": "read", "offset_bytes": 2, "nbytes": 8})
                quiet_snapshot(client, baseline)
                original_mmap = external_ddr.mmap.mmap
                try:
                    def fail_mapping(*args, **kwargs):
                        raise OSError("injected mmap failure")
                    external_ddr.mmap.mmap = fail_mapping
                    mapping_failure = must_fail(lambda: client.acquire(handle, offset_bytes=2, nbytes=8), OSError)
                finally:
                    external_ddr.mmap.mmap = original_mmap
                quiet_snapshot(client, baseline)
                assert mapped[:] == expected
                pipe.send(("faults_ok", duplicate, unsealed, quota, baseline, mapping_failure))
            elif command == "owner_exited":
                old = ExternalBackingHandle(**args[0])
                other_seed = args[1]
                assert child is not None
                assert child.read() == payload(other_seed)[OFFSET + CHILD_OFFSET:OFFSET + CHILD_OFFSET + CHILD_BYTES]
                error = must_fail(lambda: client.acquire(old, offset_bytes=0, nbytes=16), ExternalDdrError)
                snap = client.snapshot()
                row = next(r for r in snap["backings"] if r["object_id"] == old.object_id)
                assert row["state"] == "owner_exited" and row["owner_alive"] is False and row["reclaimed"] is False
                pipe.send(("owner_exit_ok", error, snap))
            elif command == "pending_owner_exit":
                other = ExternalBackingHandle(**args[0])
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                    sock.settimeout(4)
                    sock.connect(client.socket_path)
                    sock.sendall(client._encode({"op": "AcquireExternalRead", **other.identity_fields(),
                                                 "access_mode": "read", "offset_bytes": 5, "nbytes": 32}))
                    reply, ancillary, flags, _ = sock.recvmsg(65536, socket.CMSG_SPACE(16 * array.array("i").itemsize))
                    assert not flags & socket.MSG_CTRUNC
                    fields = client._decode(reply.split(b"\n", 1)[0])
                    assert fields.get("status") == "ok"
                    fds = []
                    for level, kind, raw in ancillary:
                        if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
                            received = array.array("i")
                            received.frombytes(raw[:len(raw) - len(raw) % received.itemsize])
                            fds.extend(received)
                    assert len(fds) == 1
                    try:
                        pipe.send(("pending_open",))
                        assert pipe.recv() == ("finish_pending",)
                        sock.sendall(b"OK\n")
                        assert sock.recv(64) == b"aborted\n"
                    finally:
                        for received_fd in fds:
                            os.close(received_fd)
                pipe.send(("pending_aborted",))
            elif command == "restart":
                old = ExternalBackingHandle(**args[0])
                new = ExternalBackingHandle(**args[1])
                new_seed = args[2]
                assert old.owner_name == new.owner_name
                assert old.owner_incarnation != new.owner_incarnation
                assert old.pool_generation != new.pool_generation
                must_fail(lambda: client.acquire(old, offset_bytes=0, nbytes=16), ExternalDdrError)
                stale = dataclasses.replace(new, resource_generation=new.resource_generation + 1)
                must_fail(lambda: client.acquire(stale, offset_bytes=0, nbytes=16), ExternalDdrError)
                fresh = client.acquire(new, offset_bytes=11, nbytes=23)
                assert fresh.read() == payload(new_seed)[11:34]
                fresh.release()
                pipe.send(("restart_ok",))
            elif command == "release_view":
                if child is not None:
                    child.release()
                    child.release()
                    child = None
                pipe.send(("view_released",))
            elif command == "revoke":
                client.revoke(handle)
                client.revoke(handle)
                pipe.send(("revoked",))
            elif command == "exit":
                break
            else:
                raise AssertionError(f"unknown command {command}")
    except BaseException:
        pipe.send(("error", traceback.format_exc()))
        raise
    finally:
        if child is not None:
            child.release()
        if mapped is not None:
            mapped.close()
        if fd >= 0:
            os.close(fd)
        pipe.close()


def control_request(path: str, **fields) -> dict[str, str]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(5)
        sock.connect(path)
        sock.sendall(ExternalDdrClient._encode(fields))
        data = bytearray()
        while b"\n" not in data:
            chunk = sock.recv(65536)
            if not chunk:
                raise RuntimeError("control connection closed")
            data.extend(chunk)
        return ExternalDdrClient._decode(data.split(b"\n", 1)[0])


def response(pipe, expected: str):
    if not pipe.poll(20):
        raise TimeoutError(f"owner did not report {expected}")
    value = pipe.recv()
    if value[0] == "error":
        raise RuntimeError(value[1])
    assert value[0] == expected, value
    return value


def run(args) -> dict:
    socket_path = str(Path(args.run_dir) / "socket" / "u.sock")
    result_path = Path(args.run_dir) / "results"
    result_path.mkdir(parents=True, exist_ok=True)
    daemon_log = open(result_path / "daemon.log", "w", encoding="utf-8")
    env = os.environ.copy()
    env.update({"UF_EXTERNAL_DDR_HOST_ID": args.host_id, "UF_HBM_STARTUP_PROBE_BYTES": "0",
                "UF_EXTERNAL_DDR_OWNER_MAX_BYTES": str(16 * 1024 * 1024),
                "UF_EXTERNAL_DDR_TOTAL_MAX_BYTES": str(64 * 1024 * 1024)})
    daemon = subprocess.Popen([args.daemon, "--socket", socket_path, "--startup-probe-bytes", "0"],
                              env=env, stdout=daemon_log, stderr=subprocess.STDOUT)
    processes = []
    pipes = []
    details = {"daemon_pid": daemon.pid, "socket": socket_path, "size_per_owner": SIZE, "phases": []}
    try:
        deadline = time.monotonic() + 15
        while not Path(socket_path + ".external-ddr").exists():
            if daemon.poll() is not None:
                raise RuntimeError(f"daemon exited early: {daemon.returncode}")
            if time.monotonic() >= deadline:
                raise TimeoutError("broker socket did not appear")
            time.sleep(0.02)
        mappings = Path(f"/proc/{daemon.pid}/maps").read_text(encoding="utf-8")
        loaded = sorted({line.split()[-1] for line in mappings.splitlines()
                         if "/" in line and ("libuf_acl_shim.so" in line or "libascendcl.so" in line)})
        assert any("libuf_acl_shim.so" in path for path in loaded)
        assert any("libascendcl.so" in path for path in loaded)
        details["loaded_libraries"] = loaded
        details["device_fd_samples"] = {"startup": device_fd_sample(daemon.pid)}
        client = ExternalDdrClient(socket_path)
        assert client.snapshot()["session_pidfds"] == 0
        daemon_initial_fds = stable_fd_count(daemon.pid)
        ctx = multiprocessing.get_context("spawn")

        def start_owner(name, seed):
            parent, child_pipe = ctx.Pipe()
            proc = ctx.Process(target=owner_process, args=(socket_path, name, seed, child_pipe), name=name)
            proc.start()
            child_pipe.close()
            processes.append(proc)
            pipes.append(parent)
            msg = response(parent, "registered")
            return proc, parent, msg[1], msg[2]

        a, pa, ha, pid_a = start_owner("owner-a", 11)
        b, pb, hb, pid_b = start_owner("owner-b", 29)
        assert pid_a != pid_b and pid_a != daemon.pid and pid_b != daemon.pid
        before_views = quiet_snapshot(client, (SIZE * 2, 2, 0))
        assert all(row["placement_count"] == 1 for row in before_views["backings"])
        pa.send(("import", hb, 29))
        pb.send(("import", ha, 11))
        imported_a = response(pa, "imported")
        imported_b = response(pb, "imported")
        snap = quiet_snapshot(client, (SIZE * 2, 2, 2))
        assert len([r for r in snap["backings"] if not r["reclaimed"]]) == 2
        assert all(row["placement_count"] == 1 for row in snap["backings"])
        stats = control_request(socket_path, op="GetStats")
        assert stats["ddr_objects"] == "2" and int(stats["ddr_actual_bytes"]) == SIZE * 2
        assert int(stats["ddr_total_committed_bytes"]) == SIZE * 2
        assert int(stats["external_ddr_committed_bytes"]) == SIZE * 2
        assert stats["hbm_probe_attempted"] == "false"
        details["device_fd_samples"]["bidirectional"] = device_fd_sample(daemon.pid)
        assert control_request(socket_path, op="OpenDataObject", object_id=ha["object_id"], client_id=1)["status"] == "error"
        assert control_request(socket_path, op="MarkDirty", object_id=ha["object_id"])["status"] == "error"
        assert control_request(socket_path, op="PlanTransfer", src_object_id=ha["object_id"], dst_object_id=hb["object_id"])["status"] == "error"
        assert control_request(socket_path, op="ReleaseDataObject", object_id=ha["object_id"], execution_mode="simulated")["status"] == "error"
        for _, view_id, lease_id in (imported_a, imported_b):
            assert control_request(socket_path, op="CloseLease", lease_id=lease_id)["status"] == "error"
            assert control_request(socket_path, op="RevokeAccessBinding", access_binding_id=view_id)["status"] == "error"
            assert control_request(socket_path, op="ChecksumAccessBinding", access_binding_id=view_id)["status"] == "error"
        description = control_request(socket_path, op="DescribeObject", object_id=ha["object_id"])
        assert description["address_kind"] == "scm_rights_fd" and description["domain"] == "process"
        assert quiet_snapshot(client, (SIZE * 2, 2, 2))["owner_leases"] == 2
        details["phases"].append({"bidirectional": "pass", "snapshot": snap, "stats": {k: stats[k] for k in ("ddr_objects", "ddr_actual_bytes", "hbm_probe_attempted")}, "owner_pids": [pid_a, pid_b]})

        before_fds = stable_fd_count(daemon.pid)
        pa.send(("faults",))
        fault = response(pa, "faults_ok")
        after = quiet_snapshot(client, (SIZE * 2, 2, 2))
        after_fds = stable_fd_count(daemon.pid)
        assert after_fds == before_fds, (before_fds, after_fds)
        details["phases"].append({"rollback": "pass", "duplicate": fault[1], "unsealed": fault[2], "quota": fault[3], "mapping_failure": fault[5], "daemon_fd_count_before": before_fds, "daemon_fd_count_after": after_fds, "snapshot": after})

        pb.send(("pending_owner_exit", ha))
        response(pb, "pending_open")
        pa.send(("release_view",))
        response(pa, "view_released")
        pa.send(("exit",))
        a.join(15)
        assert a.exitcode == 0, f"owner A exit={a.exitcode}"
        pb.send(("finish_pending",))
        response(pb, "pending_aborted")
        pb.send(("owner_exited", ha, 11))
        exit_result = response(pb, "owner_exit_ok")
        assert exit_result[2]["committed_bytes"] == SIZE * 2
        details["phases"].append({"owner_exit": "pass", "new_import_error": exit_result[1], "snapshot": exit_result[2]})

        a2, pa2, ha2, pid_a2 = start_owner("owner-a", 47)
        assert pid_a2 not in (pid_a, pid_b)
        pb.send(("restart", ha, ha2, 47))
        response(pb, "restart_ok")
        details["phases"].append({"restart": "pass", "old_incarnation": ha["owner_incarnation"], "new_incarnation": ha2["owner_incarnation"], "old_generation": ha["resource_generation"], "new_generation": ha2["resource_generation"]})
        pb.send(("release_view",))
        response(pb, "view_released")
        quiet_snapshot(client, (SIZE * 2, 2, 0))
        pb.send(("revoke",))
        response(pb, "revoked")
        pa2.send(("revoke",))
        response(pa2, "revoked")
        final = quiet_snapshot(client, (0, 0, 0))
        stats = control_request(socket_path, op="GetStats")
        assert stats["ddr_objects"] == "0" and stats["ddr_actual_bytes"] == "0"
        pb.send(("exit",))
        pa2.send(("exit",))
        b.join(15)
        a2.join(15)
        assert b.exitcode == 0 and a2.exitcode == 0
        final = client.snapshot()  # pidfd-confirmed exit, never TTL/disconnect
        assert final["session_pidfds"] == 0 and final["owner_leases"] == 0
        stats = control_request(socket_path, op="GetStats")
        assert stats["active_clients"] == "0" and stats["active_leases"] == "0"
        daemon_final_fds = stable_fd_count(daemon.pid)
        assert daemon_final_fds == daemon_initial_fds, (daemon_initial_fds, daemon_final_fds)
        details["device_fd_samples"]["after_cleanup"] = device_fd_sample(daemon.pid)
        details["phases"].append({"cleanup": "pass", "snapshot": final, "ddr_objects": stats["ddr_objects"],
                                  "active_clients": stats["active_clients"], "active_leases": stats["active_leases"],
                                  "daemon_fd_count_initial": daemon_initial_fds, "daemon_fd_count_final": daemon_final_fds})
        if args.sdk_package_smoke:
            sys.path.insert(0, str(SDK_DIR.parent))
            from uflow import ExternalDdrClient as ExportedClient, UFlowClient
            import torch
            with UFlowClient(enabled=True, socket_path=socket_path, device_id=0,
                             acl_lib_path=str(Path(args.run_dir) / "no-acl-client-library")) as sdk:
                facade = sdk.external_ddr()
                assert isinstance(facade, ExportedClient)
                assert facade.snapshot()["committed_bytes"] == 0
            legacy_stats = control_request(socket_path, op="GetStats")
            details["sdk_facade"] = {"status": "pass", "acl_client_initialized": False,
                                      "legacy_clients_after_close": int(legacy_stats["active_clients"]),
                                      "python_executable": sys.executable, "python_version": sys.version.split()[0],
                                      "torch_version": torch.__version__, "torch_path": torch.__file__,
                                      "note": "existing generic RegisterClient has no unregister; daemon shutdown clears it"}
        details["status"] = "pass"
        return details
    finally:
        for proc, pipe in zip(processes, pipes):
            if proc.is_alive():
                try:
                    pipe.send(("exit",))
                except (BrokenPipeError, EOFError, OSError):
                    pass
        for proc in processes:
            proc.join(15)
            if proc.is_alive():
                details.setdefault("cleanup_errors", []).append(f"owner process still alive: {proc.pid}")
            elif proc.exitcode != 0:
                details.setdefault("cleanup_errors", []).append(f"owner process exit {proc.pid}: {proc.exitcode}")
        if daemon.poll() is None:
            try:
                control_request(socket_path, op="ShutdownDaemon")
                daemon.wait(timeout=15)
            except (OSError, subprocess.TimeoutExpired) as exc:
                details.setdefault("cleanup_errors", []).append(f"daemon not stopped gracefully: {exc}")
        details["daemon_exit_code"] = daemon.poll()
        details["socket_removed"] = not Path(socket_path).exists() and not Path(socket_path + ".external-ddr").exists()
        daemon_log.close()
        (result_path / "t1-smoke.json").write_text(json.dumps(details, indent=2), encoding="utf-8")
        if details.get("cleanup_errors") or details["daemon_exit_code"] != 0 or not details["socket_removed"]:
            raise RuntimeError(f"cleanup failed: {details}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--daemon", required=True)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--host-id", required=True)
    parser.add_argument("--sdk-package-smoke", action="store_true")
    args = parser.parse_args()
    result = run(args)
    print(json.dumps(result, indent=2))
    print("UFLOW_DDR_V0_T1_PASS")


if __name__ == "__main__":
    main()
