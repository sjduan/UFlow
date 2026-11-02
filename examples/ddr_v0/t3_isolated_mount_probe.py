#!/usr/bin/env python3
"""Exercise the real fixed-root binary in a private, task-owned mount namespace.

This is an isolation probe, not production authorization or evidence for the
G-owned root. It mounts a newly-created run directory over /workspace/uflow
only inside an unshared, private mount namespace. Before any daemon is run,
the child verifies both the new namespace and the mounted directory's inode
against the parent's already-open run-owned directory descriptor. No alternate
root option or evidence path is added to the production binary.
"""
from __future__ import annotations

import argparse
import array
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time

BASE = Path("/workspace/uflow/runs")
VIRTUAL_RUN = Path("/workspace/uflow/runs/isolated-probe")
VIRTUAL_AUTHORITY = Path("/workspace/uflow/authority/external-ddr-v0")
FILES = ("bootstrap", "authority.lock", "state", "recovery.journal", "recovery.head", "witness.trust")


def digest(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def write_synced(path: Path, content: bytes) -> None:
    # Only used on the task-owned, verified private mount fixture.
    with path.open("r+b") as output:
        output.seek(0)
        output.write(content)
        output.truncate()
        output.flush()
        os.fsync(output.fileno())


def response(path: str, request: bytes, fd: int | None = None) -> bytes:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
        conn.settimeout(5)
        conn.connect(path)
        if fd is None:
            conn.sendall(request)
        else:
            sent = conn.sendmsg([request], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", [fd]))])
            if sent <= 0:
                raise RuntimeError("SCM_RIGHTS request did not send")
            conn.sendall(request[sent:])
        data = bytearray()
        while b"\n" not in data:
            chunk = conn.recv(8192)
            if not chunk or len(data) + len(chunk) > 8192:
                raise RuntimeError("missing or oversized daemon reply")
            data.extend(chunk)
        return bytes(data.split(b"\n", 1)[0])


def identity(path: Path) -> tuple[int, int]:
    info = path.stat(follow_symlinks=False)
    return info.st_dev, info.st_ino


def inner(nsroot: Path, fd: int, parent_mnt: int, binary_sha: str) -> None:
    # An explicit self-check prevents executing the fixed-root binary on the
    # real namespace if unshare or mount failed or the wrong source was used.
    source = nsroot.resolve(strict=True)
    if source != nsroot or not source.is_relative_to(BASE):
        raise RuntimeError("namespace source is not a canonical run-owned directory")
    expected = os.fstat(fd)
    if identity(source) != (expected.st_dev, expected.st_ino):
        raise RuntimeError("namespace source descriptor changed")
    actual_parent_mnt = os.stat(f"/proc/{os.getppid()}/ns/mnt").st_ino
    if actual_parent_mnt != parent_mnt or os.stat("/proc/self/ns/mnt").st_ino == actual_parent_mnt:
        raise RuntimeError("mount namespace was not isolated")
    mountinfo = Path("/proc/self/mountinfo").read_text()
    if any(field.startswith("shared:") for line in mountinfo.splitlines()
           for field in line.split(" - ", 1)[0].split()[6:]):
        raise RuntimeError("mount namespace still has shared propagation")
    authority_identity = os.stat("authority", dir_fd=fd, follow_symlinks=False)
    subprocess.run(["mount", "--bind", str(source), "/workspace/uflow"], check=True)
    if identity(Path("/workspace/uflow")) != (expected.st_dev, expected.st_ino):
        raise RuntimeError("run-owned directory is not mounted at the virtual fixed root")
    if identity(VIRTUAL_AUTHORITY.parent) != (authority_identity.st_dev, authority_identity.st_ino):
        raise RuntimeError("virtual authority parent is not the run-owned directory")
    if VIRTUAL_AUTHORITY.exists():
        raise RuntimeError("isolated authority unexpectedly exists")

    binary = VIRTUAL_RUN / "uf-daemon"
    if digest(binary) != binary_sha:
        raise RuntimeError("isolated daemon copy differs from tested binary")
    endpoint = str(VIRTUAL_RUN / "control.sock")
    env = dict(os.environ)
    env.update(UF_EXTERNAL_DDR_HOST_ID="isolated-probe-host",
               UF_EXTERNAL_DDR_OWNER_MAX_BYTES="8192", UF_EXTERNAL_DDR_TOTAL_MAX_BYTES="8192",
               UF_HBM_STARTUP_PROBE_BYTES="0",
               LD_LIBRARY_PATH=f"{VIRTUAL_RUN}/shim:/usr/local/Ascend/cann-9.0.0/lib64")
    # This synthetic policy only exercises bootstrap/anchors. It does not
    # establish a real witness or authorize T1/recovery in this probe.
    artifact = {"path": str(binary), "sha256": binary_sha}
    policy = {"schema": "uflow-ddr-deployment-policy-v1", "roles": [
        {"role": role, "executable": artifact, "entrypoint": artifact,
         "argv": [str(binary)], "environment": {}}
        for role in ("supervisor", "launcher", "interpreter", "broker", "owner", "accessor")
    ]}
    policy_path = VIRTUAL_RUN / "synthetic-policy.json"
    policy_path.write_text(json.dumps(policy, sort_keys=True, separators=(",", ":")) + "\n", encoding="ascii")
    missing_policy = subprocess.run([str(binary), "--socket", endpoint, "--initialize-external-ddr-authority"],
                                    env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=20)
    untrusted_selector = subprocess.run([str(binary), "--socket", endpoint, "--external-ddr-witness-policy", str(policy_path)],
                                        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=20)
    if missing_policy.returncode == 0 or untrusted_selector.returncode == 0 or VIRTUAL_AUTHORITY.exists():
        raise AssertionError("missing policy or ordinary caller selected deployment trust")
    bootstrap = [str(binary), "--socket", endpoint, "--initialize-external-ddr-authority",
                 "--external-ddr-witness-policy", str(policy_path)]
    def bootstrap_once(name: str) -> int:
        with (VIRTUAL_RUN / name).open("xb") as log:
            proc = subprocess.Popen(bootstrap, env=env, stdin=subprocess.DEVNULL,
                                    stdout=log, stderr=subprocess.STDOUT, close_fds=True)
            try:
                return proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                # Preserve the run-owned process and its evidence; do not
                # kill it to manufacture a successful cleanup.
                raise TimeoutError(f"bootstrap remains running: pid={proc.pid}") from None
    result = bootstrap_once("bootstrap.log")
    if result != 0:
        raise RuntimeError(f"isolated bootstrap failed: exit={result}")
    before = {name: digest(VIRTUAL_AUTHORITY / name) for name in FILES}
    state = json.loads((VIRTUAL_AUTHORITY / "state").read_bytes())
    trust = (VIRTUAL_AUTHORITY / "witness.trust").read_bytes()
    if (state.get("mode") != "clean" or state.get("generation") != "none"
            or state.get("closure") != "closed" or state.get("pending") is not None
            or state.get("trust_sequence") != 1 or state.get("trust_offset") != len(trust)
            or state.get("trust_sha256") != hashlib.sha256(trust).hexdigest()
            or json.loads(trust).get("type") != "DEPLOY"):
        raise RuntimeError("bootstrap does not contain independently anchored pre-trust")
    again = bootstrap_once("repeat-bootstrap.log")
    if again == 0 or before != {name: digest(VIRTUAL_AUTHORITY / name) for name in FILES}:
        raise RuntimeError("repeat bootstrap did not reject without modification")

    def reject_unbound_gate(name: str, expected_hashes: dict[str, str]) -> int:
        # Real production binary, task-owned fixed-root mount. The current
        # parent is not a trusted witness; sending R alone is never authority.
        command = [str(binary), "--socket", endpoint, "--execution-mode", "simulated",
                   "--startup-probe-bytes", "0", "--bound-external-ddr-gate"]
        parent, child = socket.socketpair()
        parent.settimeout(20)
        try:
            with (VIRTUAL_RUN / name).open("xb") as log:
                gated = subprocess.Popen(command, env=env, stdin=child.fileno(), stdout=log,
                                         stderr=subprocess.STDOUT, close_fds=True)
                child.close()
                try:
                    message = bytearray()
                    while not message.endswith(b"\n"):
                        piece = parent.recv(64)
                        if not piece or len(message) + len(piece) > 64:
                            raise RuntimeError("gated daemon did not report ready")
                        message.extend(piece)
                    if bytes(message) != b"UFLOW-READY-1\n" or Path(endpoint).exists() \
                            or expected_hashes != {n: digest(VIRTUAL_AUTHORITY / n) for n in FILES}:
                        raise AssertionError("gate caused a side effect before release")
                    parent.sendall(b"R")
                    rc = gated.wait(timeout=20)
                    if rc == 0 or Path(endpoint).exists() or Path(endpoint + ".external-ddr").exists() \
                            or expected_hashes != {n: digest(VIRTUAL_AUTHORITY / n) for n in FILES}:
                        raise AssertionError("unbound or forged gate opened a listener or changed authority")
                    return rc
                finally:
                    parent.close()
                    if gated.poll() is None:
                        gated.wait(timeout=20)
        finally:
            parent.close()
            child.close()

    genesis_gate_exit = reject_unbound_gate("genesis-gate.log", before)
    invalid_gate = subprocess.run([str(binary), "--socket", endpoint, "--bound-external-ddr-gate"],
                                  env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, timeout=20)
    if invalid_gate.returncode == 0 or before != {name: digest(VIRTUAL_AUTHORITY / name) for name in FILES}:
        raise AssertionError("non-socket gate did not fail closed")

    # Read-only evidence inspection cannot make genesis into a sealed witness,
    # choose its own evidence policy, or create any metadata.
    inspect = [str(binary), "--socket", endpoint, "--inspect-external-ddr-evidence"]
    genesis_inspect = subprocess.run(inspect, env=env, stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT, timeout=20)
    inspect_selector = subprocess.run(inspect + ["--external-ddr-witness-policy", str(policy_path)],
                                      env=env, stdout=subprocess.PIPE,
                                      stderr=subprocess.STDOUT, timeout=20)
    if genesis_inspect.returncode == 0 or inspect_selector.returncode == 0 \
            or before != {name: digest(VIRTUAL_AUTHORITY / name) for name in FILES}:
        raise AssertionError("genesis inspection or caller-selected evidence must fail read-only")

    with (VIRTUAL_RUN / "daemon.log").open("xb") as log:
        proc = subprocess.Popen([str(binary), "--socket", endpoint, "--execution-mode", "simulated",
                                 "--startup-probe-bytes", "0"], env=env, stdin=subprocess.DEVNULL,
                                stdout=log, stderr=subprocess.STDOUT, close_fds=True)
        try:
            deadline = time.monotonic() + 20
            external = endpoint + ".external-ddr"
            while not Path(external).exists():
                if proc.poll() is not None:
                    raise RuntimeError(f"daemon exited before external listener: {proc.returncode}")
                if time.monotonic() > deadline:
                    raise TimeoutError(f"daemon external listener timeout, pid={proc.pid}")
                time.sleep(0.02)
            lockfd = os.open(VIRTUAL_AUTHORITY / "authority.lock", os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
            try:
                try:
                    fcntl.flock(lockfd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    pass
                else:
                    raise AssertionError("running broker did not hold the fixed lock")
                locked_inspect = subprocess.run(inspect, env=env, stdout=subprocess.PIPE,
                                                stderr=subprocess.STDOUT, timeout=20)
                if locked_inspect.returncode == 0:
                    raise AssertionError("evidence inspection ignored the running broker lock")
                memfd = os.memfd_create("isolated-probe", os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
                try:
                    os.ftruncate(memfd, 4096)
                    rejected = response(external, b"op=RegisterExternalBacking\n", memfd)
                finally:
                    os.close(memfd)
                if b"status=error" not in rejected or b"trusted witness" not in rejected:
                    raise AssertionError(f"untrusted production registration was not gated: {rejected!r}")
                shutdown = response(endpoint, b"op=ShutdownDaemon\n")
                if b"status=ok" not in shutdown:
                    raise AssertionError(f"shutdown rejected: {shutdown!r}")
                if proc.wait(timeout=20) != 0:
                    raise RuntimeError(f"daemon did not exit normally: {proc.returncode}")
                fcntl.flock(lockfd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                os.close(lockfd)
        finally:
            if proc.poll() is None and Path(endpoint).exists():
                try:
                    response(endpoint, b"op=ShutdownDaemon\n")
                    proc.wait(timeout=20)
                except (OSError, RuntimeError, subprocess.TimeoutExpired):
                    print(f"run-owned daemon requires attention: pid={proc.pid}", flush=True)
    after = {name: digest(VIRTUAL_AUTHORITY / name) for name in FILES}
    if before != after or Path(endpoint).exists() or Path(external).exists():
        raise AssertionError("unused authority changed or sockets remained after stop")

    # Inject a *synthetic*, structurally valid BIND into this task-owned root.
    # The fake process births and evidence are deliberately not proof; a real
    # binary must keep the lock, report unknown and reject registration even
    # when its state/chain/head appear consistent.
    manifest_lines = (VIRTUAL_AUTHORITY / "bootstrap").read_text(encoding="ascii").splitlines()[1:]
    manifest = dict(line.split("=", 1) for line in manifest_lines)
    original_head = (VIRTUAL_AUTHORITY / "recovery.head").read_text(encoding="ascii")
    policy_hash = hashlib.sha256(canonical(policy)).hexdigest()
    fake_birth = lambda pid: {"pid": pid, "uid": os.geteuid(), "gid": os.getegid(),
                              "pidns_dev": 1, "pidns_ino": 2, "starttime_ticks": pid + 100}
    binding = {"generation": "synthetic-g1", "previous_seal": "0" * 64,
               "policy_sha256": policy_hash, "host": manifest["host"], "boot": manifest["boot"],
               "domain": manifest["domain"], "revision": int(manifest["revision"]),
               "owner_limit": int(manifest["owner_limit"]), "total_limit": int(manifest["total_limit"]),
               "endpoint": manifest["endpoint"], "initial_head": original_head,
               "journal": list(identity(VIRTUAL_AUTHORITY / "recovery.journal")),
               "journal_head": list(identity(VIRTUAL_AUTHORITY / "recovery.head")),
               "broker": "synthetic-b1", "broker_birth": fake_birth(999991), "witness_birth": fake_birth(999992),
               "evidence_dir": str(VIRTUAL_RUN / "synthetic-evidence"), "evidence_dir_id": [999999, 1],
               "evidence_journal": [999999, 2], "evidence_head": [999999, 3],
               "evidence_initial_sha256": "1" * 64}
    frame = {"schema": "uflow-ddr-trust-chain-v1", "sequence": 2, "previous": json.loads(trust)["checksum"],
             "type": "BIND", "payload": binding}
    frame["checksum"] = hashlib.sha256(canonical(frame)).hexdigest()
    synthetic_trust = trust + canonical(frame) + b"\n"
    synthetic_state = dict(state)
    synthetic_state.update(trust_sequence=2, trust_offset=len(synthetic_trust),
                           trust_sha256=hashlib.sha256(synthetic_trust).hexdigest(),
                           generation="synthetic-g1", closure="bound")
    write_synced(VIRTUAL_AUTHORITY / "witness.trust", synthetic_trust)
    write_synced(VIRTUAL_AUTHORITY / "state", canonical(synthetic_state) + b"\n")
    synthetic_before = {name: digest(VIRTUAL_AUTHORITY / name) for name in FILES}
    with (VIRTUAL_RUN / "synthetic-bound-daemon.log").open("xb") as log:
        synthetic_proc = subprocess.Popen([str(binary), "--socket", endpoint, "--execution-mode", "simulated",
                                           "--startup-probe-bytes", "0"], env=env, stdin=subprocess.DEVNULL,
                                          stdout=log, stderr=subprocess.STDOUT, close_fds=True)
        try:
            deadline = time.monotonic() + 20
            while not Path(external).exists():
                if synthetic_proc.poll() is not None:
                    raise RuntimeError(f"synthetic quarantine daemon exited: {synthetic_proc.returncode}")
                if time.monotonic() > deadline:
                    raise TimeoutError(f"synthetic quarantine listener timeout, pid={synthetic_proc.pid}")
                time.sleep(0.02)
            lockfd = os.open(VIRTUAL_AUTHORITY / "authority.lock", os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
            try:
                try:
                    fcntl.flock(lockfd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    pass
                else:
                    raise AssertionError("synthetic quarantine did not retain authority lock")
                synthetic_stats = response(endpoint, b"op=GetStats\n")
                if b"external_ddr_committed_bytes=unknown" not in synthetic_stats:
                    raise AssertionError(f"synthetic generation reported known commitments: {synthetic_stats!r}")
                memfd = os.memfd_create("synthetic-quarantine", os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
                try:
                    os.ftruncate(memfd, 4096)
                    synthetic_registration = response(external, b"op=RegisterExternalBacking\n", memfd)
                finally:
                    os.close(memfd)
                if b"status=error" not in synthetic_registration:
                    raise AssertionError("synthetic generation authorized a registration")
                shutdown = response(endpoint, b"op=ShutdownDaemon\n")
                if b"status=ok" not in shutdown or synthetic_proc.wait(timeout=20) != 0:
                    raise RuntimeError("synthetic quarantine daemon did not stop normally")
                fcntl.flock(lockfd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                os.close(lockfd)
        finally:
            if synthetic_proc.poll() is None and Path(endpoint).exists():
                try:
                    response(endpoint, b"op=ShutdownDaemon\n")
                    synthetic_proc.wait(timeout=20)
                except (OSError, RuntimeError, subprocess.TimeoutExpired):
                    print(f"run-owned synthetic daemon requires attention: pid={synthetic_proc.pid}", flush=True)
    synthetic_after = {name: digest(VIRTUAL_AUTHORITY / name) for name in FILES}
    if synthetic_before != synthetic_after or Path(endpoint).exists() or Path(external).exists():
        raise AssertionError("synthetic quarantine changed evidence or left sockets")
    fake_inspect = subprocess.run(inspect, env=env, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, timeout=20)
    if fake_inspect.returncode == 0 \
            or synthetic_before != {name: digest(VIRTUAL_AUTHORITY / name) for name in FILES}:
        raise AssertionError("unsealed fake BIND inspection did not fail read-only")
    fake_gate_exit = reject_unbound_gate("fake-bind-gate.log", synthetic_before)

    # Deliberate task-owned fault injection, not a real dirty generation. An
    # unsupported/legacy state must not lose the v4 anchor or expose an
    # external listener. The ordinary control service reports unknown stats.
    with (VIRTUAL_AUTHORITY / "state").open("r+b") as state:
        state.write(b"dirty\n")
        state.truncate()
        state.flush()
        os.fsync(state.fileno())
    injected_before = {name: digest(VIRTUAL_AUTHORITY / name) for name in FILES}
    with (VIRTUAL_RUN / "quarantine-daemon.log").open("xb") as log:
        dirty_proc = subprocess.Popen([str(binary), "--socket", endpoint, "--execution-mode", "simulated",
                                       "--startup-probe-bytes", "0"], env=env, stdin=subprocess.DEVNULL,
                                      stdout=log, stderr=subprocess.STDOUT, close_fds=True)
        try:
            deadline = time.monotonic() + 20
            while not Path(endpoint).exists():
                if dirty_proc.poll() is not None:
                    raise RuntimeError(f"daemon exited before control listener: {dirty_proc.returncode}")
                if time.monotonic() > deadline:
                    raise TimeoutError(f"control listener timeout, pid={dirty_proc.pid}")
                time.sleep(0.02)
            if Path(external).exists():
                raise AssertionError("unsupported v4 state exposed external listener")
            lockfd = os.open(VIRTUAL_AUTHORITY / "authority.lock", os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
            try:
                fcntl.flock(lockfd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                dirty_stats = response(endpoint, b"op=GetStats\n")
                if b"external_ddr_committed_bytes=unknown" not in dirty_stats:
                    raise AssertionError(f"stats falsely reported known commitments: {dirty_stats!r}")
                dirty_registration = response(endpoint, b"op=RegisterExternalBacking\n")
                if b"status=error" not in dirty_registration:
                    raise AssertionError(f"unsupported state registration not rejected: {dirty_registration!r}")
                shutdown = response(endpoint, b"op=ShutdownDaemon\n")
                if b"status=ok" not in shutdown or dirty_proc.wait(timeout=20) != 0:
                    raise RuntimeError("diagnostic daemon did not stop normally")
            finally:
                os.close(lockfd)
        finally:
            if dirty_proc.poll() is None and Path(endpoint).exists():
                try:
                    response(endpoint, b"op=ShutdownDaemon\n")
                    dirty_proc.wait(timeout=20)
                except (OSError, RuntimeError, subprocess.TimeoutExpired):
                    print(f"run-owned quarantine daemon requires attention: pid={dirty_proc.pid}", flush=True)
    injected_after = {name: digest(VIRTUAL_AUTHORITY / name) for name in FILES}
    if (injected_before != injected_after or Path(endpoint).exists() or Path(external).exists()
            or (VIRTUAL_AUTHORITY / "state").read_bytes() != b"dirty\n"):
        raise AssertionError("quarantine diagnostic changed injected dirty evidence or left sockets")
    data = {"namespace_inode": os.stat("/proc/self/ns/mnt").st_ino,
            "parent_namespace_inode": actual_parent_mnt,
            "source_identity": [expected.st_dev, expected.st_ino],
            "authority_parent_identity": [authority_identity.st_dev, authority_identity.st_ino],
            "daemon_sha256": binary_sha, "bootstrap_exit": result,
            "missing_policy_exit": missing_policy.returncode,
            "ordinary_policy_selector_exit": untrusted_selector.returncode,
            "genesis_inspection_exit": genesis_inspect.returncode,
            "inspection_policy_selector_exit": inspect_selector.returncode,
            "locked_inspection_exit": locked_inspect.returncode,
            "unsealed_fake_binding_inspection_exit": fake_inspect.returncode,
            "genesis_gate_exit": genesis_gate_exit,
            "non_socket_gate_exit": invalid_gate.returncode,
            "fake_binding_gate_exit": fake_gate_exit,
            "repeat_bootstrap_exit": again, "broker_pid": proc.pid,
            "lock_contended_then_released": True, "registration_reply": rejected.decode("utf-8"),
            "daemon_exit": proc.returncode, "authority_hashes_before": before,
            "authority_hashes_after": after,
            "synthetic_generation": "fake BIND and births; no real witness or recovery authorization",
            "synthetic_broker_pid": synthetic_proc.pid, "synthetic_broker_exit": synthetic_proc.returncode,
            "synthetic_lock_contended_then_released": True, "synthetic_stats_committed": "unknown",
            "synthetic_registration_reply": synthetic_registration.decode("utf-8"),
            "synthetic_hashes_before": synthetic_before, "synthetic_hashes_after": synthetic_after,
            "fault_injection": "run-owned state set dirty over synthetic BIND, not a genuine recovery history",
            "quarantine_broker_pid": dirty_proc.pid, "quarantine_broker_exit": dirty_proc.returncode,
            "unsupported_state_external_listener": False,
            "quarantine_registration_reply": dirty_registration.decode("utf-8"),
            "quarantine_stats_committed": "unknown",
            "quarantine_hashes_before": injected_before, "quarantine_hashes_after": injected_after}
    (VIRTUAL_RUN / "result.json").write_text(json.dumps(data, sort_keys=True, indent=2) + "\n")
    print(json.dumps(data, sort_keys=True), flush=True)


def outer(run_dir: Path, binary: Path, shim: Path) -> None:
    parent = run_dir.parent.resolve(strict=True)
    if not parent.is_relative_to(BASE) or parent == BASE or run_dir.parent != parent:
        raise ValueError("probe must use a canonical task-owned run directory")
    binary = binary.resolve(strict=True)
    shim = shim.resolve(strict=True)
    if not binary.is_relative_to(parent) or not shim.is_relative_to(parent):
        raise ValueError("daemon and shim must belong to the same task run")
    run_dir.mkdir(mode=0o700)  # never reuse or overwrite an earlier probe
    nsroot = run_dir / "nsroot"
    nsroot.mkdir(mode=0o700)
    (nsroot / "authority").mkdir(mode=0o700)
    (nsroot / "runs").mkdir(mode=0o700)
    local_run = nsroot / "runs" / "isolated-probe"
    local_run.mkdir(mode=0o700)
    (local_run / "shim").mkdir(mode=0o700)
    shutil.copyfile(binary, local_run / "uf-daemon")
    os.chmod(local_run / "uf-daemon", 0o700)
    shutil.copyfile(shim, local_run / "shim" / "libuf_acl_shim.so")
    os.chmod(local_run / "shim" / "libuf_acl_shim.so", 0o700)
    binary_sha = digest(binary)
    fd = os.open(nsroot, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        # Prove the guard rejects a direct call in the outer namespace before
        # starting the child. This must fail before it can invoke mount.
        try:
            inner(nsroot, fd, os.stat(f"/proc/{os.getppid()}/ns/mnt").st_ino, binary_sha)
        except RuntimeError as error:
            if str(error) != "mount namespace was not isolated":
                raise
        else:
            raise AssertionError("outer-namespace guard unexpectedly accepted")
        command = ["unshare", "--mount", "--propagation", "private", sys.executable,
                   str(Path(__file__).resolve(strict=True)), "--inside", str(nsroot),
                   "--root-fd", str(fd), "--parent-mnt", str(os.stat("/proc/self/ns/mnt").st_ino),
                   "--binary-sha", binary_sha]
        with (run_dir / "outer.log").open("xb") as log:
            proc = subprocess.Popen(command, pass_fds=(fd,), stdin=subprocess.DEVNULL,
                                    stdout=log, stderr=subprocess.STDOUT)
            try:
                rc = proc.wait(timeout=120)
            except subprocess.TimeoutExpired:
                raise TimeoutError(f"isolated probe remains running: pid={proc.pid}; preserved at {run_dir}") from None
    finally:
        os.close(fd)
    sys.stdout.buffer.write((run_dir / "outer.log").read_bytes())
    if rc != 0:
        raise RuntimeError(f"isolated mount probe failed: exit={rc}; preserved at {run_dir}")
    print(f"isolated mount probe: exit=0; results={run_dir}/nsroot/runs/isolated-probe", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path)
    parser.add_argument("--daemon", type=Path)
    parser.add_argument("--shim", type=Path)
    parser.add_argument("--inside", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--root-fd", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--parent-mnt", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--binary-sha", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.inside is not None:
        if args.root_fd is None or args.parent_mnt is None or args.binary_sha is None:
            parser.error("incomplete isolation proof")
        inner(args.inside, args.root_fd, args.parent_mnt, args.binary_sha)
    else:
        if args.run_dir is None or args.daemon is None or args.shim is None:
            parser.error("--run-dir, --daemon and --shim are required")
        outer(args.run_dir, args.daemon, args.shim)


if __name__ == "__main__":
    main()
