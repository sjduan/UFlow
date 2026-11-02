#!/usr/bin/env python3
"""Exercise production-binary fail-closed gates without accessing the fixed root.

This is a run-owned negative fixture, not production bootstrap or recovery.
It never calls a test-binary child entry point or requests NPU allocation.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import socket
import subprocess
import time


def request(path: str, op: str) -> bytes:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
        conn.settimeout(5)
        conn.connect(path)
        conn.sendall(f"op={op}\n".encode("ascii"))
        data = bytearray()
        while b"\n" not in data:
            chunk = conn.recv(8192)
            if not chunk:
                raise RuntimeError("control socket closed before reply")
            data.extend(chunk)
        return bytes(data.split(b"\n", 1)[0])


def run(daemon: Path, root: Path) -> None:
    parent = root.parent.resolve(strict=True)
    if not parent.is_relative_to(Path("/workspace/uflow/runs")):
        raise ValueError("fixture must be in a task-owned run directory")
    root.mkdir(mode=0o700)  # deliberately refuses any existing path
    control = str(root / "control.sock")
    if len(os.fsencode(control + ".external-ddr")) >= 108:
        raise ValueError("fixture socket path too long")
    env = dict(os.environ)
    # Invalid host configuration must be rejected before even looking at the
    # fixed production root. Never run a valid bootstrap from this fixture.
    env.update(UF_EXTERNAL_DDR_HOST_ID="", UF_EXTERNAL_DDR_OWNER_MAX_BYTES="8192",
               UF_EXTERNAL_DDR_TOTAL_MAX_BYTES="8192", UF_HBM_STARTUP_PROBE_BYTES="0",
               UF_DDR_WITNESS_HOLDER_ROLE="broker", UF_DDR_WITNESS_ROOT=str(root),
               UF_DDR_WITNESS_ENDPOINT=control)
    initial = subprocess.run([str(daemon), "--socket", control, "--initialize-external-ddr-authority",
                              "--external-ddr-witness-policy", str(root / "unused-policy")],
                             env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             timeout=15, check=False)
    (root / "bootstrap.log").write_bytes(initial.stdout)
    if initial.returncode == 0 or b"invalid external DDR authority configuration" not in initial.stdout:
        raise AssertionError("invalid-config bootstrap did not fail closed")
    if Path(control).exists() or Path(control + ".external-ddr").exists():
        raise AssertionError("bootstrap created a listener")
    with (root / "daemon.log").open("xb") as log:
        proc = subprocess.Popen([str(daemon), "--socket", control, "--execution-mode", "simulated",
                                 "--startup-probe-bytes", "0"], env=env, stdin=subprocess.DEVNULL,
                                stdout=log, stderr=subprocess.STDOUT, close_fds=True)
        try:
            deadline = time.monotonic() + 15
            while not Path(control).exists():
                if proc.poll() is not None:
                    raise RuntimeError(f"daemon exited before control startup: {proc.returncode}")
                if time.monotonic() > deadline:
                    raise TimeoutError(f"daemon control startup timeout, pid={proc.pid}")
                time.sleep(0.02)
            if Path(control + ".external-ddr").exists():
                raise AssertionError("production exposed an external-DDR broker")
            try:
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
                    conn.settimeout(1)
                    conn.connect(control + ".external-ddr")
            except (FileNotFoundError, ConnectionRefusedError):
                pass
            else:
                raise AssertionError("production unexpectedly accepted external-DDR connection")
            response = request(control, "RegisterExternalBacking")
            if b"status=error" not in response:
                raise AssertionError(f"registration was not rejected: {response!r}")
            shutdown = request(control, "ShutdownDaemon")
            if b"status=ok" not in shutdown:
                raise AssertionError(f"graceful shutdown rejected: {shutdown!r}")
            if proc.wait(timeout=15) != 0:
                raise RuntimeError(f"daemon did not exit normally: {proc.returncode}")
        finally:
            if proc.poll() is None and Path(control).exists():
                try:
                    request(control, "ShutdownDaemon")
                    proc.wait(timeout=15)
                except (OSError, RuntimeError, subprocess.TimeoutExpired):
                    print(f"run-owned daemon requires attention, pid={proc.pid}", flush=True)
    log_data = (root / "daemon.log").read_bytes()
    if b"invalid external DDR authority configuration" not in log_data:
        raise AssertionError("production open did not report invalid configuration")
    if Path(control).exists() or Path(control + ".external-ddr").exists():
        raise AssertionError("run-owned sockets remain after graceful stop")
    print(f"production fail-closed: invalid-config bootstrap={initial.returncode}, daemon=0, registration=rejected, fixture-env=ignored, logs={root}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--daemon", required=True, type=Path)
    parser.add_argument("--run-dir", required=True, type=Path)
    args = parser.parse_args()
    run(args.daemon.resolve(strict=True), args.run_dir)


if __name__ == "__main__":
    main()
