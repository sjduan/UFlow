#!/usr/bin/env python3
"""Real ordinary-peer authority lifecycle in a task-owned private mount.

This never uses the real fixed authority root: a new run-owned directory is
bind-mounted over /workspace/uflow only after checking the private namespace,
its propagation and the already-open source inode. No witness or recovery API
is used. A dirty ending is deliberately retained in the isolated fixture.
"""
from __future__ import annotations

import argparse
import dataclasses
import fcntl
import hashlib
import json
import os
from pathlib import Path
import select
import shutil
import socket
import subprocess
import sys
import time
import traceback

BASE = Path("/workspace/uflow/runs")
RUN = Path("/workspace/uflow/runs/normal-probe")
ROOT = Path("/workspace/uflow/authority/external-ddr-v0")
FILES = ("bootstrap", "authority.lock", "state", "recovery.journal", "recovery.head", "witness.trust")
SIZE = 4096


def digest(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def identity(path: Path) -> tuple[int, int]:
    st = path.stat(follow_symlinks=False)
    return st.st_dev, st.st_ino


def control(path: str, request: bytes) -> bytes:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
        conn.settimeout(10)
        conn.connect(path)
        conn.sendall(request)
        result = bytearray()
        while not result.endswith(b"\n"):
            part = conn.recv(65536)
            if not part or len(result) + len(part) > 1_048_576:
                raise RuntimeError("incomplete control response")
            result.extend(part)
        return bytes(result)


def peer(endpoint: str, seed: int) -> None:
    # This is the existing SDK, used from ordinary independent processes. No
    # witness parent, test-only daemon entry, root override or torch stub.
    sys.path.insert(0, str(RUN))
    from external_ddr import ExternalBackingHandle, ExternalDdrClient, ExternalDdrError

    client = ExternalDdrClient(endpoint)
    fd = os.memfd_create(f"ordinary-{seed}", os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
    view = None
    try:
        os.ftruncate(fd, SIZE)
        os.pwrite(fd, bytes([seed]) * SIZE, 0)
        fcntl.fcntl(fd, fcntl.F_ADD_SEALS, fcntl.F_SEAL_GROW | fcntl.F_SEAL_SHRINK)
        handle = client.register(fd, name=f"object-{seed}", owner_name=f"owner-{seed}", size_bytes=SIZE)
        print(json.dumps({"event": "registered", "pid": os.getpid(), "handle": dataclasses.asdict(handle)}), flush=True)
        for line in sys.stdin:
            req = json.loads(line)
            op = req["op"]
            if op == "acquire":
                other = ExternalBackingHandle(**req["handle"])
                root = client.acquire(other, offset_bytes=17, nbytes=128)
                assert root.read() == bytes([req["seed"]]) * 128
                view = root.retain(7, 32)
                root.release()
                assert view.read() == bytes([req["seed"]]) * 32
                value = {"event": "acquired", "identity": view._identity.identity_fields()}
            elif op == "check_old":
                old = req["identity"]
                for operation, extra in (("RetainExternalView", {"offset_bytes": 0, "nbytes": 1}),
                                         ("ReleaseExternalView", {})):
                    try:
                        client._request(operation, **old, **extra)
                    except ExternalDdrError:
                        pass
                    else:
                        raise AssertionError(f"old tuple accepted by {operation}")
                assert view is not None and view.read() == bytes([req["seed"]]) * 32
                value = {"event": "old_rejected"}
            elif op == "check_live":
                assert view is not None and view.read() == bytes([req["seed"]]) * 32
                value = {"event": "live"}
            elif op == "release":
                assert view is not None
                view.release()
                view = None
                value = {"event": "released"}
            elif op == "revoke":
                client.revoke(handle)
                value = {"event": "revoked"}
            elif op == "reject_dirty":
                for action in (
                    lambda: client.acquire(ExternalBackingHandle(**req["handle"]), offset_bytes=0, nbytes=1),
                    lambda: client.register(fd, name="again", owner_name="again", size_bytes=SIZE),
                ):
                    try:
                        action()
                    except ExternalDdrError:
                        pass
                    else:
                        raise AssertionError("dirty authority accepted new work")
                value = {"event": "dirty_rejected"}
            elif op == "discard":
                # A quarantined broker cannot acknowledge a release. Unmap
                # locally without asserting that the authority is clean.
                if view is not None:
                    view._mapping.close()
                    view = None
                value = {"event": "discarded"}
            elif op == "exit":
                assert view is None
                print(json.dumps({"event": "exit"}), flush=True)
                return
            else:
                raise ValueError(f"unknown peer operation {op}")
            print(json.dumps(value), flush=True)
        raise RuntimeError("peer input closed without explicit exit")
    except BaseException:
        print(json.dumps({"event": "error", "detail": traceback.format_exc()}), flush=True)
        raise
    finally:
        if view is not None and not view._mapping.closed:
            view._mapping.close()
        os.close(fd)


def rejected_peer(endpoint: str) -> None:
    # A separate ordinary SDK process proves registration cannot even send its
    # FD when the untrusted bootstrap prevented creation of the broker socket.
    sys.path.insert(0, str(RUN))
    from external_ddr import ExternalDdrClient

    fd = os.memfd_create("rejected-restart", os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
    try:
        os.ftruncate(fd, SIZE)
        fcntl.fcntl(fd, fcntl.F_ADD_SEALS, fcntl.F_SEAL_GROW | fcntl.F_SEAL_SHRINK)
        try:
            ExternalDdrClient(endpoint).register(fd, name="rejected", owner_name="rejected", size_bytes=SIZE)
        except FileNotFoundError as error:
            print(json.dumps({"event": "registration_rejected_before_fd_send", "pid": os.getpid(),
                              "errno": error.errno}), flush=True)
        else:
            raise AssertionError("replacement bootstrap accepted a new registration")
    finally:
        os.close(fd)


class Peer:
    def __init__(self, endpoint: str, seed: int, name: str):
        self.log = (RUN / f"{name}.log").open("x", encoding="utf-8")
        self.proc = subprocess.Popen([sys.executable, str(RUN / "probe.py"), "--peer", endpoint, str(seed)],
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.log,
                                     text=True, bufsize=1, close_fds=True)
        self.initial = self.receive("registered")

    def receive(self, event: str) -> dict:
        assert self.proc.stdout is not None
        if not select.select([self.proc.stdout], [], [], 30)[0]:
            raise TimeoutError(f"peer {self.proc.pid} did not report {event}")
        line = self.proc.stdout.readline()
        if not line:
            raise RuntimeError(f"peer {self.proc.pid} closed stdout, exit={self.proc.poll()}")
        data = json.loads(line)
        if data.get("event") != event:
            raise RuntimeError(f"peer {self.proc.pid}: {data}")
        return data

    def command(self, op: str, event: str | None = None, **kwargs) -> dict:
        assert self.proc.stdin is not None
        self.proc.stdin.write(json.dumps({"op": op, **kwargs}) + "\n")
        self.proc.stdin.flush()
        return self.receive(event or op)

    def exit(self) -> None:
        self.command("exit")
        rc = self.proc.wait(timeout=20)
        self.log.close()
        if rc != 0 or Path(f"/proc/{self.proc.pid}").exists():
            raise RuntimeError(f"peer cleanup failed: pid={self.proc.pid} exit={rc}")


def require_isolated_namespace(outer_mnt_fd: int, outer_pid: int) -> int:
    """Read-only guard; the anchor is outer itself, never outer's parent.

    outer pins its live namespace before spawning unshare (which execs this
    child). Keeping that FD open prevents namespace inode reuse. A different
    host/container namespace of outer's parent is not evidence of unshare.
    """
    if not os.readlink(f"/proc/self/fd/{outer_mnt_fd}").startswith("mnt:["):
        raise RuntimeError("outer mount namespace descriptor required")
    pinned = os.fstat(outer_mnt_fd)
    current = os.stat("/proc/self/ns/mnt")
    outer = os.stat(f"/proc/{outer_pid}/ns/mnt")
    anchor = (pinned.st_dev, pinned.st_ino)
    if (os.getppid() != outer_pid or (outer.st_dev, outer.st_ino) != anchor
            or (current.st_dev, current.st_ino) == anchor):
        raise RuntimeError("mount namespace was not isolated")
    mountinfo = Path("/proc/self/mountinfo").read_text()
    if any(field.startswith(("shared:", "master:", "propagate_from:"))
           for line in mountinfo.splitlines()
           for field in line.split(" - ", 1)[0].split()[6:]):
        raise RuntimeError("mount propagation is not private")
    return pinned.st_ino


def inner(nsroot: Path, fd: int, outer_mnt_fd: int, outer_pid: int, binary_sha: str, replace_bootstrap: bool,
          legacy_binary_sha: str | None) -> None:
    # No path lookup, mount or bootstrap until the inherited isolation proof
    # passes. Do not call this side-effecting function as an outer negative test.
    outer_mnt = require_isolated_namespace(outer_mnt_fd, outer_pid)
    source = nsroot.resolve(strict=True)
    if source != nsroot or not source.is_relative_to(BASE):
        raise RuntimeError("namespace source is not canonical and task-owned")
    expected = os.fstat(fd)
    if identity(source) != (expected.st_dev, expected.st_ino):
        raise RuntimeError("namespace source descriptor changed")
    expected_authority_parent = os.stat("authority", dir_fd=fd, follow_symlinks=False)
    subprocess.run(["mount", "--bind", str(source), "/workspace/uflow"], check=True)
    if identity(Path("/workspace/uflow")) != (expected.st_dev, expected.st_ino):
        raise RuntimeError("wrong private mount source")
    if identity(ROOT.parent) != (expected_authority_parent.st_dev, expected_authority_parent.st_ino):
        raise RuntimeError("wrong authority parent")
    if ROOT.exists() or digest(RUN / "uf-daemon") != binary_sha:
        raise RuntimeError("unexpected fixture root or binary")
    if legacy_binary_sha and digest(RUN / "legacy-daemon") != legacy_binary_sha:
        raise RuntimeError("unexpected legacy binary")

    sys.path.insert(0, str(RUN))
    from external_ddr import ExternalDdrClient

    endpoint = str(RUN / "u.sock")
    candidate_binary = str(RUN / "uf-daemon")
    binary = str(RUN / "legacy-daemon") if legacy_binary_sha else candidate_binary
    env = dict(os.environ, UF_EXTERNAL_DDR_HOST_ID="isolated-normal-host",
               UF_EXTERNAL_DDR_OWNER_MAX_BYTES="8192", UF_EXTERNAL_DDR_TOTAL_MAX_BYTES="16384",
               UF_HBM_STARTUP_PROBE_BYTES="0", PYTHONDONTWRITEBYTECODE="1",
               LD_LIBRARY_PATH=f"{RUN}/shim:/usr/local/Ascend/cann-9.0.0/lib64")
    os.environ.update(env)
    bootstrap = subprocess.run([binary, "--socket", endpoint, "--initialize-external-ddr-authority"], env=env,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=20)
    if bootstrap.returncode:
        raise RuntimeError(f"fixture bootstrap failed: {bootstrap.stdout!r}")
    expected_version = "v5" if legacy_binary_sha else "v6"
    assert (ROOT / "bootstrap").read_text().startswith(f"uflow-ddr-authority-{expected_version}\n")
    assert (ROOT / "witness.trust").read_bytes() == b"uflow-ddr-normal-authority-v1\n"
    original = {name: digest(ROOT / name) for name in FILES}
    repeat = subprocess.run([binary, "--socket", endpoint, "--initialize-external-ddr-authority"], env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=20)
    assert repeat.returncode != 0 and original == {name: digest(ROOT / name) for name in FILES}
    disabled = {}
    for flag in ("--begin-external-ddr-generation", "--seal-external-ddr-generation", "--bound-external-ddr-gate"):
        attempt = subprocess.run([binary, "--socket", endpoint, flag], env=env, stdin=subprocess.DEVNULL,
                                 stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=20)
        assert attempt.returncode != 0 and original == {name: digest(ROOT / name) for name in FILES}
        disabled[flag] = attempt.returncode
    result = {"namespace_inode": os.stat("/proc/self/ns/mnt").st_ino,
              "parent_namespace_inode": outer_mnt, "source_identity": [expected.st_dev, expected.st_ino],
              "binary_sha256": binary_sha, "legacy_binary_sha256": legacy_binary_sha,
              "bootstrap_exit": bootstrap.returncode,
              "repeat_bootstrap_exit": repeat.returncode, "experimental_exits": disabled,
              "daemons": [], "peers": []}
    daemon = None
    daemon_log = None
    peers: list[Peer] = []

    def start(name: str, expect_external: bool = True) -> subprocess.Popen:
        nonlocal daemon, daemon_log
        daemon_log = (RUN / f"{name}.log").open("x", encoding="utf-8")
        daemon = subprocess.Popen([binary, "--socket", endpoint, "--execution-mode", "simulated",
                                   "--startup-probe-bytes", "0"], env=env, stdin=subprocess.DEVNULL,
                                  stdout=daemon_log, stderr=subprocess.STDOUT, close_fds=True)
        deadline = time.monotonic() + 20
        listener = Path(endpoint + ".external-ddr") if expect_external else Path(endpoint)
        while not listener.exists():
            if daemon.poll() is not None:
                raise RuntimeError(f"daemon {name} exited early: {daemon.returncode}")
            if time.monotonic() > deadline:
                raise TimeoutError(f"daemon {name} listener missing, pid={daemon.pid}")
            time.sleep(0.02)
        result["daemons"].append({"name": name, "pid": daemon.pid})
        return daemon

    def stop(expect_clean: bool = True) -> None:
        nonlocal daemon, daemon_log
        assert daemon is not None and daemon_log is not None
        if b"status=ok" not in control(endpoint, b"op=ShutdownDaemon\n"):
            raise RuntimeError("shutdown rejected")
        rc = daemon.wait(timeout=30)
        daemon_log.close()
        result["daemons"][-1]["exit"] = rc
        if (rc == 0) != expect_clean or Path(f"/proc/{daemon.pid}").exists() or Path(endpoint).exists() or Path(endpoint + ".external-ddr").exists():
            raise RuntimeError(f"daemon cleanup failed: pid={daemon.pid}, exit={rc}")
        daemon = None
        daemon_log = None

    def add_peer(seed: int, name: str) -> Peer:
        item = Peer(endpoint, seed, name)
        peers.append(item)
        result["peers"].append({"name": name, "pid": item.proc.pid})
        return item

    try:
        first = start("first")
        lock = os.open(ROOT / "authority.lock", os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
        try:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                pass
            else:
                raise AssertionError("first broker did not hold fixed lock")
            hashes = {name: digest(ROOT / name) for name in FILES}
            socket_id = identity(Path(endpoint))
            contender = subprocess.run([binary, "--socket", endpoint, "--execution-mode", "simulated",
                                        "--startup-probe-bytes", "0"], env=env, stdin=subprocess.DEVNULL,
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=20)
            if contender.returncode == 0 or identity(Path(endpoint)) != socket_id or hashes != {n: digest(ROOT / n) for n in FILES}:
                raise AssertionError("second broker disturbed the winner")
            assert first.poll() is None
            result["second_broker_exit"] = contender.returncode
        finally:
            os.close(lock)
        a, b = add_peer(11, "first-a"), add_peer(29, "first-b")
        old = a.command("acquire", "acquired", handle=b.initial["handle"], seed=29)["identity"]
        b.command("acquire", "acquired", handle=a.initial["handle"], seed=11)
        snapshot = ExternalDdrClient(endpoint).snapshot()
        assert snapshot["committed_bytes"] == 2 * SIZE and snapshot["active_views"] == 2
        a.command("release", "released")
        b.command("release", "released")
        a.command("revoke", "revoked")
        b.command("revoke", "revoked")
        a.exit()
        b.exit()
        peers.clear()
        stop()
        first_state = json.loads((ROOT / "state").read_text())
        assert first_state["mode"] == "clean" and first_state["closure"] == "normal"
        result["first_clean_head"] = first_state["journal_head"]

        if replace_bootstrap or legacy_binary_sha:
            # This negative runs in a separate task-owned mount. Keep both the
            # real old inode and the replacement when applicable; never repair
            # or reuse it. Legacy mode starts with a real v5 daemon/history.
            path = ROOT / "bootstrap"
            old_identity = identity(path)
            if replace_bootstrap:
                original_bytes = path.read_bytes()
                old_mode = path.stat().st_mode
                other_files = {name: {"identity": identity(ROOT / name), "sha256": digest(ROOT / name)}
                               for name in FILES if name != "bootstrap"}
                os.rename(path, ROOT / "bootstrap.test-original")
                replacement = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
                with os.fdopen(replacement, "wb") as file:
                    file.write(original_bytes)
                    file.flush()
                    os.fsync(file.fileno())
                directory_fd = os.open(ROOT, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
                assert path.read_bytes() == original_bytes == (ROOT / "bootstrap.test-original").read_bytes()
                assert path.stat().st_mode == old_mode
                assert other_files == {name: {"identity": identity(ROOT / name), "sha256": digest(ROOT / name)}
                                       for name in FILES if name != "bootstrap"}
            new_identity = identity(path)
            assert (new_identity != old_identity) == replace_bootstrap
            binary = candidate_binary
            expected_error = ("authority bootstrap inode mismatch" if replace_bootstrap
                              else "legacy authority lacks bootstrap inode anchor")
            evidence_files = (*FILES, "bootstrap.test-original") if replace_bootstrap else FILES

            def evidence() -> dict:
                return {name: {"identity": identity(ROOT / name), "mode": (ROOT / name).stat().st_mode,
                               "sha256": digest(ROOT / name)} for name in evidence_files}

            before = evidence()
            rejected = []
            for attempt in range(2):
                start(f"replaced-{attempt}", expect_external=False)
                assert expected_error in (RUN / f"replaced-{attempt}.log").read_text()
                assert not Path(endpoint + ".external-ddr").exists()
                stats = control(endpoint, b"op=GetStats\n")
                assert b"external_ddr_committed_bytes=unknown" in stats
                for field in (b"direct_transfer_submitted_jobs=0", b"transfer_plans=0", b"transfer_events=0"):
                    assert field in stats
                child = subprocess.run([sys.executable, str(RUN / "probe.py"), "--reject-peer", endpoint],
                                       env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE, timeout=30, check=True, text=True)
                event = json.loads(child.stdout)
                assert event["event"] == "registration_rejected_before_fd_send"
                assert not Path(f"/proc/{event['pid']}").exists()
                assert not Path(endpoint + ".external-ddr").exists()
                stop()
                assert expected_error in (RUN / f"replaced-{attempt}.log").read_text()
                assert evidence() == before
                rejected.append({"peer": event, "daemon_exit": result["daemons"][-1]["exit"],
                                 "stats": stats.decode()})
            result.update(status="pass", scenario=("offline same-byte bootstrap inode replacement" if replace_bootstrap
                                                    else "real legacy v5 non-genesis clean history"),
                          original_identity=old_identity, replacement_identity=new_identity,
                          evidence_before=before, evidence_after=evidence(), rejections=rejected,
                          note="no external listener or FD send, no transfer jobs; evidence retained without repair")
            (RUN / "result.json").write_text(json.dumps(result, sort_keys=True, indent=2) + "\n", encoding="utf-8")
            print(json.dumps(result, sort_keys=True), flush=True)
            return

        start("second")
        a, b = add_peer(11, "second-a"), add_peer(29, "second-b")
        fresh = a.command("acquire", "acquired", handle=b.initial["handle"], seed=29)["identity"]
        b.command("acquire", "acquired", handle=a.initial["handle"], seed=11)
        assert all(old[k] == fresh[k] for k in ("object_id", "view_id", "lease_id")), (old, fresh)
        assert old["broker_instance"] != fresh["broker_instance"]
        a.command("check_old", "old_rejected", identity=old, seed=29)
        result["clean_restart_collision"] = {"old": old, "fresh": fresh}
        # A genuine normal shutdown with live registered backings and mmap
        # cannot declare clean. The peer mapping remains usable after exit.
        stop(expect_clean=False)
        dirty = json.loads((ROOT / "state").read_text())
        assert dirty["mode"] == "dirty"
        a.command("check_live", "live", seed=29)
        before = {name: digest(ROOT / name) for name in FILES}
        start("diagnostic")
        stats = control(endpoint, b"op=GetStats\n")
        assert b"external_ddr_committed_bytes=unknown" in stats
        a.command("reject_dirty", "dirty_rejected", handle=b.initial["handle"])
        a.command("check_live", "live", seed=29)
        stop()
        after = {name: digest(ROOT / name) for name in FILES}
        assert before == after
        a.command("discard", "discarded")
        b.command("discard", "discarded")
        a.exit()
        b.exit()
        peers.clear()
        result.update(status="pass", dirty_state="quarantined; no dirty recovery",
                      diagnostic_stats="unknown", dirty_hashes_before=before, dirty_hashes_after=after)
        (RUN / "result.json").write_text(json.dumps(result, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(result, sort_keys=True), flush=True)
    finally:
        # Best-effort cooperative cleanup only; never force-kill a participant
        # or erase the isolated authority evidence to manufacture success.
        if daemon is not None and daemon.poll() is None and Path(endpoint).exists():
            try:
                control(endpoint, b"op=ShutdownDaemon\n")
                daemon.wait(timeout=20)
            except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
                print(f"daemon requires attention: pid={daemon.pid} error={error}", flush=True)
        if daemon_log is not None:
            daemon_log.close()
        for item in peers:
            if item.proc.poll() is None:
                try:
                    item.command("discard", "discarded")
                    item.exit()
                except (OSError, RuntimeError, TimeoutError, subprocess.TimeoutExpired) as error:
                    print(f"peer requires attention: pid={item.proc.pid} error={error}", flush=True)


def outer(run_dir: Path, binary: Path, shim: Path, replace_bootstrap: bool,
          legacy_binary: Path | None) -> None:
    parent = run_dir.parent.resolve(strict=True)
    if parent == BASE or not parent.is_relative_to(BASE) or run_dir.parent != parent:
        raise ValueError("run directory must have a canonical task-owned parent")
    binary, shim = binary.resolve(strict=True), shim.resolve(strict=True)
    if not binary.is_relative_to(parent) or not shim.is_relative_to(parent):
        raise ValueError("daemon and shim must be task-owned")
    if legacy_binary:
        legacy_binary = legacy_binary.resolve(strict=True)
        if not legacy_binary.is_relative_to(parent) or replace_bootstrap:
            raise ValueError("legacy daemon must be task-owned and use its own scenario")
    sdk = Path(__file__).resolve().parents[2] / "sdk/python/uflow/external_ddr.py"
    run_dir.mkdir(mode=0o700)
    nsroot = run_dir / "nsroot"
    nsroot.mkdir(mode=0o700)
    (nsroot / "authority").mkdir(mode=0o700)
    (nsroot / "runs").mkdir(mode=0o700)
    local = nsroot / "runs/normal-probe"
    local.mkdir(mode=0o700)
    (local / "shim").mkdir(mode=0o700)
    shutil.copyfile(binary, local / "uf-daemon")
    os.chmod(local / "uf-daemon", 0o700)
    if legacy_binary:
        shutil.copyfile(legacy_binary, local / "legacy-daemon")
        os.chmod(local / "legacy-daemon", 0o700)
    shutil.copyfile(shim, local / "shim/libuf_acl_shim.so")
    shutil.copyfile(sdk, local / "external_ddr.py")
    shutil.copyfile(Path(__file__).resolve(strict=True), local / "probe.py")
    binary_sha = digest(binary)
    outer_mnt_fd = os.open("/proc/self/ns/mnt", os.O_RDONLY | os.O_CLOEXEC)
    try:
        workspace_before = identity(Path("/workspace/uflow"))
        mounts_before = Path("/proc/self/mountinfo").read_bytes()
        outer_identity = os.fstat(outer_mnt_fd)
        outer_parent_mnt = os.stat(f"/proc/{os.getppid()}/ns/mnt").st_ino
        fd = os.open(nsroot, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
        try:
            command = ["unshare", "--mount", "--propagation", "private", sys.executable,
                       str(Path(__file__).resolve(strict=True)), "--inside", str(nsroot),
                       "--root-fd", str(fd), "--outer-mnt-fd", str(outer_mnt_fd),
                       "--outer-pid", str(os.getpid()), "--binary-sha", binary_sha]
            if replace_bootstrap:
                command.append("--replace-bootstrap-before-restart")
            if legacy_binary:
                command.extend(["--legacy-binary-sha", digest(legacy_binary)])
            with (run_dir / "outer.log").open("xb") as log:
                proc = subprocess.Popen(command, pass_fds=(fd, outer_mnt_fd), stdin=subprocess.DEVNULL,
                                        stdout=log, stderr=subprocess.STDOUT)
                try:
                    rc = proc.wait(timeout=180)
                except subprocess.TimeoutExpired:
                    raise TimeoutError(f"probe still running: pid={proc.pid}; preserve {run_dir}") from None
        finally:
            os.close(fd)
        workspace_after = identity(Path("/workspace/uflow"))
        mounts_after = Path("/proc/self/mountinfo").read_bytes()
        (run_dir / "mountinfo.before").write_bytes(mounts_before)
        (run_dir / "mountinfo.after").write_bytes(mounts_after)
        (run_dir / "isolation.json").write_text(json.dumps({
            "outer_pid": os.getpid(), "outer_parent_pid": os.getppid(),
            "outer_namespace_inode": outer_identity.st_ino,
            "outer_parent_namespace_inode": outer_parent_mnt,
            "workspace_before": workspace_before, "workspace_after": workspace_after,
            "mountinfo_unchanged": mounts_before == mounts_after, "child_exit": rc,
        }, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        if workspace_before != workspace_after or mounts_before != mounts_after:
            raise RuntimeError(f"outer workspace mount changed; preserve {run_dir}")
    finally:
        os.close(outer_mnt_fd)
    sys.stdout.buffer.write((run_dir / "outer.log").read_bytes())
    if rc:
        raise RuntimeError(f"isolated probe failed: exit={rc}; preserve {run_dir}")
    print(f"isolated normal authority probe: exit=0; results={run_dir}/nsroot/runs/normal-probe", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path)
    parser.add_argument("--daemon", type=Path)
    parser.add_argument("--shim", type=Path)
    parser.add_argument("--inside", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--root-fd", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--outer-mnt-fd", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--outer-pid", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--binary-sha", help=argparse.SUPPRESS)
    parser.add_argument("--peer", nargs=2, help=argparse.SUPPRESS)
    parser.add_argument("--reject-peer", help=argparse.SUPPRESS)
    parser.add_argument("--replace-bootstrap-before-restart", action="store_true")
    parser.add_argument("--legacy-daemon", type=Path)
    parser.add_argument("--legacy-binary-sha", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.peer:
        peer(args.peer[0], int(args.peer[1]))
    elif args.reject_peer:
        rejected_peer(args.reject_peer)
    elif args.inside is not None:
        if args.root_fd is None or args.outer_mnt_fd is None or args.outer_pid is None or args.binary_sha is None:
            parser.error("incomplete isolation proof")
        inner(args.inside, args.root_fd, args.outer_mnt_fd, args.outer_pid, args.binary_sha,
              args.replace_bootstrap_before_restart, args.legacy_binary_sha)
    else:
        if args.run_dir is None or args.daemon is None or args.shim is None:
            parser.error("--run-dir, --daemon and --shim are required")
        outer(args.run_dir, args.daemon, args.shim, args.replace_bootstrap_before_restart, args.legacy_daemon)


if __name__ == "__main__":
    main()
