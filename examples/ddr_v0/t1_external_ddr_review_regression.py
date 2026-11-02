#!/usr/bin/env python3
"""Regression for first-request rollback and broker-restart view identity.

Runs only task-owned daemons, real memfds and real Unix/SCM_RIGHTS peers.
It never force-kills processes; failures preserve logs and report cleanup issues.
"""
from __future__ import annotations

import argparse
import array
import dataclasses
import json
import mmap
import multiprocessing
import os
import socket
import subprocess
import time
import traceback
from pathlib import Path

import t1_external_ddr_smoke as smoke
from t1_external_ddr_smoke import (
    ExternalBackingHandle, ExternalDdrClient, ExternalDdrError, abort_provisional,
    control_request, external_ddr, must_fail, new_memfd, response, stable_fd_count,
)

SIZE = 4096


class Harness:
    def __init__(self, args, name):
        self.args = args
        self.root = Path(args.run_dir) / name
        self.socket = str(self.root / "socket" / "u.sock")
        # Linux sockaddr_un.sun_path has 108 bytes including the trailing NUL.
        # Reject before daemon start so no half-created control socket is left.
        if len(os.fsencode(self.socket + ".external-ddr")) >= 108:
            raise ValueError("run-dir is too long for Unix socket; use a shorter task-owned path")
        (self.root / "socket").mkdir(parents=True, exist_ok=False)
        self.client = ExternalDdrClient(self.socket)
        self.proc = None
        self.log = None
        self.exits = []
        self.pids = []

    def start(self):
        assert self.proc is None
        env = os.environ.copy()
        env.update({"UF_EXTERNAL_DDR_HOST_ID": self.args.host_id, "UF_HBM_STARTUP_PROBE_BYTES": "0",
                    "UF_EXTERNAL_DDR_OWNER_MAX_BYTES": "8192", "UF_EXTERNAL_DDR_TOTAL_MAX_BYTES": "65536"})
        self.log = (self.root / f"daemon-{len(self.exits)}.log").open("w", encoding="utf-8")
        self.proc = subprocess.Popen([self.args.daemon, "--socket", self.socket, "--startup-probe-bytes", "0"],
                                     env=env, stdout=self.log, stderr=subprocess.STDOUT)
        self.pids.append(self.proc.pid)
        deadline = time.monotonic() + 15
        while not Path(self.client.socket_path).exists():
            if self.proc.poll() is not None:
                raise RuntimeError(f"daemon failed to start: {self.proc.returncode}")
            if time.monotonic() > deadline:
                raise TimeoutError("broker not ready")
            time.sleep(0.02)
        assert control_request(self.socket, op="GetStats")["hbm_probe_attempted"] == "false"
        smoke.device_fd_sample(self.proc.pid)
        return self.client

    def stop(self):
        if self.proc is None:
            return
        proc = self.proc
        try:
            if proc.poll() is None:
                assert control_request(self.socket, op="ShutdownDaemon")["status"] == "ok"
                proc.wait(timeout=15)
            assert proc.returncode == 0, proc.returncode
            assert not Path(self.socket).exists() and not Path(self.client.socket_path).exists()
            assert not Path(f"/proc/{proc.pid}").exists()
            self.exits.append(proc.returncode)
        finally:
            self.log.close()
            self.proc = None


def owner_worker(control, pipe):
    fd = new_memfd("review-owner", SIZE)
    try:
        os.pwrite(fd, b"O" * SIZE, 0)
        client = ExternalDdrClient(control)
        handle = client.register(fd, name="review-owner", owner_name="review-owner", size_bytes=SIZE)
        pipe.send(("registered", dataclasses.asdict(handle), os.getpid()))
        while True:
            cmd = pipe.recv()
            if cmd == "revoke":
                client.revoke(handle)
                pipe.send(("revoked",))
            elif cmd == "exit":
                break
            else:
                raise AssertionError(cmd)
    except BaseException:
        pipe.send(("error", traceback.format_exc()))
        raise
    finally:
        os.close(fd)
        pipe.close()


def open_pending(client, fields, send_fd=None, map_fd=False):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(4)
    try:
        sock.connect(client.socket_path)
        raw = client._encode(fields)
        if send_fd is None:
            sock.sendall(raw)
        else:
            n = sock.sendmsg([raw], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", [send_fd]))])
            sock.sendall(raw[n:])
        data, ancillary, flags, _ = sock.recvmsg(65536, socket.CMSG_SPACE(16 * array.array("i").itemsize))
        assert not flags & socket.MSG_CTRUNC
        fds = []
        for level, kind, raw in ancillary:
            if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
                arr = array.array("i")
                arr.frombytes(raw[:len(raw) - len(raw) % arr.itemsize])
                fds.extend(arr)
        while b"\n" not in data:
            data += sock.recv(65536)
        reply = client._decode(data.split(b"\n", 1)[0])
        assert reply.get("status") == "ok", reply
        assert len(fds) == int(map_fd), fds
        mapping = None
        try:
            if map_fd:
                mapping = mmap.mmap(fds[0], int(reply["size_bytes"]), access=mmap.ACCESS_READ)
        finally:
            for fd in fds:
                os.close(fd)
        return sock, reply, mapping
    except BaseException:
        sock.close()
        raise


def wait_snapshot(client, predicate, timeout=4):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        snap = client.snapshot()
        if predicate(snap):
            return snap
        time.sleep(0.02)
    raise AssertionError(f"snapshot timeout: {snap}")


def counters(h):
    snap = h.client.snapshot()
    stats = control_request(h.socket, op="GetStats")
    keys = ("committed_bytes", "backing_fds", "session_pidfds", "owner_leases", "active_views", "pending_handoffs")
    return {**{key: snap[key] for key in keys}, "active_clients": int(stats["active_clients"]),
            "active_leases": int(stats["active_leases"]), "daemon_fds": stable_fd_count(h.proc.pid)}


def restored(h, expected):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        actual = counters(h)
        if actual == expected:
            return actual
        time.sleep(0.05)
    raise AssertionError(f"live-process rollback mismatch: expected={expected}, actual={actual}")


def accessor_worker(control, handle_dict, pipe):
    client = ExternalDdrClient(control)
    handle = ExternalBackingHandle(**handle_dict)
    sockets = []
    maps = []
    view = None
    try:
        request = {"op": "AcquireExternalRead", **handle.identity_fields(), "access_mode": "read",
                   "offset_bytes": 7, "nbytes": 31}
        for _ in range(2):
            sock, fields, mapping = open_pending(client, request, map_fd=True)
            sockets.append(sock)
            maps.append(mapping)
            if len(sockets) == 2:
                second_fields = fields
        before = client.snapshot()
        assert before["pending_handoffs"] == 2
        sockets[0].close()
        maps[0].close()
        wait_snapshot(client, lambda s: s["pending_handoffs"] == 1 and s["active_views"] == 0)
        sockets[1].sendall(b"OK\n")
        assert sockets[1].recv(64) == b"committed\n"
        sockets[1].close()
        view = external_ddr.ExternalReadView(client, second_fields, maps[1])
        assert view.read() == b"O" * 31
        pipe.send(("view_ready", view.view_id, view.accessor_address_domain, os.getpid()))
        assert pipe.recv() == "check_after_revoke"
        assert view.read() == b"O" * 31
        view.release()
        view.release()
        pipe.send(("released",))
        assert pipe.recv() == "exit"
    except BaseException:
        pipe.send(("error", traceback.format_exc()))
        raise
    finally:
        for sock in sockets:
            sock.close()
        for mapping in maps:
            if not mapping.closed:
                mapping.close()
        pipe.close()


def first_failure_and_concurrency(args):
    h = Harness(args, "first-failure")
    ctx = multiprocessing.get_context("spawn")
    processes = []
    pipes = []
    details = {"cases": {}}
    fds = []
    try:
        client = h.start()
        empty = counters(h)
        fd = new_memfd("quota-first", 16384)
        fds.append(fd)
        details["cases"]["first_quota"] = must_fail(
            lambda: client.register(fd, name="quota", owner_name="new", size_bytes=16384), ExternalDdrError)
        details["empty_baseline"] = restored(h, empty)
        parent, child = ctx.Pipe()
        owner = ctx.Process(target=owner_worker, args=(h.socket, child))
        owner.start()
        child.close()
        processes.append(owner)
        pipes.append(parent)
        _, handle_dict, owner_pid = response(parent, "registered")
        handle = ExternalBackingHandle(**handle_dict)
        base = counters(h)
        assert base == {"committed_bytes": SIZE, "backing_fds": 1, "session_pidfds": 1,
                        "owner_leases": 1, "active_views": 0, "pending_handoffs": 0,
                        "active_clients": 1, "active_leases": 1, "daemon_fds": base["daemon_fds"]}
        details["owner_pid"] = owner_pid
        details["live_owner_baseline"] = base
        details["cases"]["first_invalid_object"] = must_fail(
            lambda: client._request("AcquireExternalRead", object_id=999999), ExternalDdrError)
        restored(h, base)
        details["cases"]["first_oob"] = must_fail(
            lambda: client._request("AcquireExternalRead", **handle.identity_fields(), access_mode="read",
                                    offset_bytes=SIZE - 2, nbytes=3), ExternalDdrError)
        restored(h, base)
        fd = new_memfd("no-ack-first", SIZE)
        fds.append(fd)
        abort_provisional(client, {"op": "RegisterExternalBacking", "name": "noack",
                                   "owner_name": "new", "size_bytes": SIZE}, fd)
        details["cases"]["first_register_no_ack"] = restored(h, base)
        abort_provisional(client, {"op": "AcquireExternalRead", **handle.identity_fields(),
                                   "access_mode": "read", "offset_bytes": 3, "nbytes": 17})
        details["cases"]["first_acquire_no_ack"] = restored(h, base)
        original_mmap = external_ddr.mmap.mmap
        try:
            def fail_map(*args, **kwargs):
                raise OSError("injected first mmap failure")
            external_ddr.mmap.mmap = fail_map
            details["cases"]["first_mmap_failure"] = must_fail(
                lambda: client.acquire(handle, offset_bytes=3, nbytes=17), OSError)
        finally:
            external_ddr.mmap.mmap = original_mmap
        details["after_first_failures"] = restored(h, base)
        assert owner.is_alive() and os.getpid() != owner_pid

        fd1, fd2 = new_memfd("pending-1", SIZE), new_memfd("pending-2", SIZE)
        fds.extend([fd1, fd2])
        request = {"op": "RegisterExternalBacking", "name": "pending", "owner_name": "caller", "size_bytes": SIZE}
        s1, f1, _ = open_pending(client, request, send_fd=fd1)
        s2, f2, _ = open_pending(client, request, send_fd=fd2)
        try:
            assert f1["owner_incarnation"] == f2["owner_incarnation"]
            assert f1["pool_generation"] == f2["pool_generation"]
            pending = client.snapshot()
            assert pending["session_pidfds"] == 2 and pending["pending_handoffs"] == 2
            s1.close()
            remaining = wait_snapshot(client, lambda s: s["pending_handoffs"] == 1)
            assert remaining["session_pidfds"] == 2
            s2.sendall(b"OK\n")
            assert s2.recv(64) == b"committed\n"
        finally:
            s1.close()
            s2.close()
        concurrent = ExternalBackingHandle.from_response(f2)
        fd3 = new_memfd("stable-success", SIZE)
        fds.append(fd3)
        stable = client.register(fd3, name="stable", owner_name="caller", size_bytes=SIZE)
        assert stable.owner_incarnation == concurrent.owner_incarnation
        assert stable.pool_generation == concurrent.pool_generation
        must_fail(lambda: client.register(fd, name="quota2", owner_name="caller", size_bytes=16384), ExternalDdrError)
        client.revoke(concurrent)
        client.revoke(concurrent)
        client.revoke(stable)
        assert client.snapshot()["session_pidfds"] == 2
        details["cases"]["concurrent_registration_session"] = "pass"

        accessor_pipe, accessor_child = ctx.Pipe()
        accessor = ctx.Process(target=accessor_worker, args=(h.socket, handle_dict, accessor_child))
        accessor.start()
        accessor_child.close()
        processes.append(accessor)
        pipes.append(accessor_pipe)
        ready = response(accessor_pipe, "view_ready")
        before_revoke = client.snapshot()
        assert before_revoke["active_views"] == 1 and before_revoke["session_pidfds"] == 3
        parent.send("revoke")
        response(parent, "revoked")
        held = client.snapshot()
        assert held["committed_bytes"] == SIZE and held["backing_fds"] == 1 and held["active_views"] == 1
        accessor_pipe.send("check_after_revoke")
        response(accessor_pipe, "released")
        after_release = wait_snapshot(client, lambda s: s["committed_bytes"] == 0 and s["active_views"] == 0)
        assert after_release["backing_fds"] == 0
        accessor_pipe.send("exit")
        parent.send("exit")
        for proc in processes:
            proc.join(15)
            assert proc.exitcode == 0 and not proc.is_alive(), (proc.pid, proc.exitcode)
        final = client.snapshot()
        assert final["session_pidfds"] == 1 and final["owner_leases"] == 0
        details["cases"]["concurrent_first_accessor_and_revoke"] = {
            "status": "pass", "accessor_pid": ready[3], "held_after_revoke": held,
            "after_release": after_release, "final_live_caller_session_pidfds": final["session_pidfds"]}
        details["process_alive_during_first_failures"] = True
        return details
    finally:
        for fd in fds:
            os.close(fd)
        cleanup_errors = []
        for proc, pipe in zip(processes, pipes):
            if proc.is_alive():
                try:
                    pipe.send("exit")
                except (OSError, BrokenPipeError):
                    pass
                proc.join(15)
            if proc.is_alive() or proc.exitcode != 0:
                cleanup_errors.append(f"worker cleanup failed: pid={proc.pid}, exit={proc.exitcode}")
            pipe.close()
        try:
            h.stop()
        finally:
            details["daemon_exit_codes"] = h.exits
            details["daemon_pids"] = h.pids
        if cleanup_errors:
            raise RuntimeError("; ".join(cleanup_errors))


def register_payload(client, label, data):
    fd = new_memfd(label, SIZE)
    os.pwrite(fd, data * SIZE, 0)
    return fd, client.register(fd, name=label, owner_name=label, size_bytes=SIZE)


def restart_collision(args):
    h = Harness(args, "restart")
    fds = []
    details = {}
    old_view = fresh = None
    try:
        client = h.start()
        fd, old_handle = register_payload(client, "old", b"A")
        fds.append(fd)
        old_view = client.acquire(old_handle, offset_bytes=0, nbytes=16)
        assert old_view.read() == b"A" * 16
        h.stop()
        h.start()
        fd, new_handle = register_payload(client, "new", b"B")
        fds.append(fd)
        fresh = client.acquire(new_handle, offset_bytes=0, nbytes=16)
        assert old_view.view_id == fresh.view_id and old_view.lease_id == fresh.lease_id
        assert old_handle.broker_instance != new_handle.broker_instance
        assert old_view.read() == b"A" * 16 and fresh.read() == b"B" * 16
        before = counters(h)
        details["stale_retain"] = must_fail(lambda: old_view.retain(0, 8), ExternalDdrError)
        details["stale_release"] = must_fail(old_view.release, ExternalDdrError)
        assert old_view._mapping.closed
        details["stale_release_repeat"] = must_fail(old_view.release, ExternalDdrError)
        must_fail(old_view.read, ExternalDdrError)
        details["stale_compensation"] = must_fail(
            lambda: client._request("ReleaseExternalView", **old_view._identity.identity_fields()), ExternalDdrError)
        must_fail(lambda: client._request("ReleaseExternalView", view_id=fresh.view_id), ExternalDdrError)
        for key, value in (("lease_id", fresh.lease_id + 1), ("object_id", new_handle.object_id + 1),
                           ("resource_generation", new_handle.resource_generation + 1),
                           ("accessor_address_domain", "invalid")):
            forged = fresh._identity.identity_fields()
            forged[key] = value
            must_fail(lambda: client._request("ReleaseExternalView", **forged), ExternalDdrError)
        assert counters(h) == before
        uncertain = client.acquire(new_handle, offset_bytes=20, nbytes=8)
        original_request = client._request
        try:
            def lose_release(op, **values):
                if op == "ReleaseExternalView":
                    raise OSError("injected unknown release result")
                return original_request(op, **values)
            client._request = lose_release
            details["injected_unknown_release"] = must_fail(uncertain.release, OSError)
            assert uncertain._mapping.closed
            must_fail(uncertain.read, ExternalDdrError)
        finally:
            client._request = original_request
        assert client.snapshot()["active_views"] == 2
        uncertain.release()
        uncertain.release()
        assert client.snapshot()["active_views"] == 1
        client.revoke(new_handle)
        held = client.snapshot()
        assert held["committed_bytes"] == SIZE and held["backing_fds"] == 1 and held["active_views"] == 1
        assert fresh.read() == b"B" * 16
        fresh.release()
        fresh.release()
        released = client.snapshot()
        assert released["committed_bytes"] == 0 and released["backing_fds"] == 0 and released["active_views"] == 0
        details.update({"view_id_collision": fresh.view_id, "lease_id_collision": fresh.lease_id,
                        "before": before, "held_after_revoke": held, "released": released})
        return details
    finally:
        if old_view is not None and not old_view._mapping.closed:
            old_view._mapping.close()
        if fresh is not None and not fresh._mapping.closed:
            fresh.release()
        for fd in fds:
            os.close(fd)
        h.stop()
        details["daemon_exit_codes"] = h.exits
        details["daemon_pids"] = h.pids


def compensation_restart(args, mode):
    h = Harness(args, "compensation-" + mode)
    fds = []
    details = {}
    fresh = None
    client = h.client
    original_mmap = external_ddr.mmap.mmap
    original_class = external_ddr.ExternalReadView
    original_request = client._request
    try:
        h.start()
        fd, old = register_payload(client, "old", b"A")
        fds.append(fd)

        def restart_and_create():
            nonlocal fresh
            h.stop()
            h.start()
            fd2, new = register_payload(client, "new", b"B")
            fds.append(fd2)
            fresh = client.acquire(new, offset_bytes=0, nbytes=16)
            details["new_handle"] = dataclasses.asdict(new)
            details["new_view_id"] = fresh.view_id
            return new

        def intercept_release(op, **values):
            if op == "ReleaseExternalView":
                details["compensation_identity"] = values.copy()
                if mode == "mmap":
                    external_ddr.mmap.mmap = original_mmap
                    restart_and_create()
                try:
                    return original_request(op, **values)
                except ExternalDdrError as exc:
                    details["stale_compensation_error"] = str(exc)
                    raise
            return original_request(op, **values)

        client._request = intercept_release
        if mode == "mmap":
            def fail_map(*args, **kwargs):
                raise OSError("injected mmap failure before ACK")
            external_ddr.mmap.mmap = fail_map
            details["injected_error"] = must_fail(lambda: client.acquire(old, offset_bytes=0, nbytes=16), OSError)
        else:
            def fail_constructor(_client, _fields, _mapping):
                external_ddr.ExternalReadView = original_class
                restart_and_create()
                raise ValueError("injected construction failure after ACK")
            external_ddr.ExternalReadView = fail_constructor
            details["injected_error"] = must_fail(lambda: client.acquire(old, offset_bytes=0, nbytes=16), ValueError)
        external_ddr.mmap.mmap = original_mmap
        external_ddr.ExternalReadView = original_class
        client._request = original_request
        assert fresh is not None and details["compensation_identity"]["view_id"] == fresh.view_id
        assert details["compensation_identity"]["broker_instance"] == old.broker_instance
        assert details["compensation_identity"]["broker_instance"] != details["new_handle"]["broker_instance"]
        assert "stale" in details["stale_compensation_error"]
        assert fresh.read() == b"B" * 16
        before = client.snapshot()
        assert before["active_views"] == 1 and before["committed_bytes"] == SIZE
        client.revoke(ExternalBackingHandle(**details["new_handle"]))
        held = client.snapshot()
        assert held["active_views"] == 1 and held["committed_bytes"] == SIZE
        fresh.release()
        assert client.snapshot()["committed_bytes"] == 0
        details["fresh_view_retained_after_stale_compensation"] = True
        return details
    finally:
        external_ddr.mmap.mmap = original_mmap
        external_ddr.ExternalReadView = original_class
        client._request = original_request
        if fresh is not None and not fresh._mapping.closed:
            fresh.release()
        for fd in fds:
            os.close(fd)
        h.stop()
        details["daemon_exit_codes"] = h.exits
        details["daemon_pids"] = h.pids


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--daemon", required=True)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--host-id", required=True)
    args = parser.parse_args()
    result_dir = Path(args.run_dir) / "results"
    result_dir.mkdir(parents=True, exist_ok=True)
    result = {"status": "running", "cases": {}}
    try:
        result["cases"]["first_failure_and_concurrency"] = first_failure_and_concurrency(args)
        result["cases"]["restart_collision"] = restart_collision(args)
        result["cases"]["mmap_compensation_restart"] = compensation_restart(args, "mmap")
        result["cases"]["constructor_compensation_restart"] = compensation_restart(args, "constructor")
        result["status"] = "pass"
    except BaseException:
        result["status"] = "fail"
        result["error"] = traceback.format_exc()
        raise
    finally:
        (result_dir / "review-regression.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))
    print("UFLOW_DDR_T1_REVIEW_REGRESSION_PASS")


if __name__ == "__main__":
    main()
