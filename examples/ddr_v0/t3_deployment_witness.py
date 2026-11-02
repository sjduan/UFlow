#!/usr/bin/env python3
"""Direct-parent witness and explicit fixed-root BIND deployment (WIP).

The trusted-generation operation starts a fixed-root binder and a real gated
broker, then seals only a completed no-admission lifecycle. Resource-bearing
SEAL, recovery and admission remain fail-closed. The deployment must pin this
exact entrypoint/interpreter and both roles in its immutable policy. A readable
log alone is never authority or proof that all obligations were covered.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import select
import stat
import subprocess
import sys
import time

HEAD_SCHEMA = "uflow-ddr-evidence-head-v1"
FRAME_SCHEMA = "uflow-ddr-witness-v2"
ZERO = "0" * 64
MAX_LOG = 16 * 1024 * 1024
MAX_FRAME = 8192
RUNS = Path("/workspace/uflow/runs")


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def digest_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def token(value: str) -> bool:
    return bool(value) and len(value) <= 96 and value.isascii() and all(c.isalnum() or c in "._-" for c in value)


def birth(pid: int) -> dict:
    text = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    start, end = text.find("("), text.rfind(")")
    if start < 0 or end <= start or int(text[:start].strip()) != pid:
        raise ValueError("invalid process stat")
    fields = text[end + 1:].split()
    if len(fields) <= 19 or len(fields[0]) != 1:
        raise ValueError("short process stat")
    credentials = {}
    for line in Path(f"/proc/{pid}/status").read_text(encoding="utf-8").splitlines():
        if line.startswith(("Uid:", "Gid:")):
            name, value = line.split(":", 1)
            parts = value.split()
            if name in credentials or len(parts) != 4:
                raise ValueError("invalid process credentials")
            credentials[name] = int(parts[1])
    if set(credentials) != {"Uid", "Gid"}:
        raise ValueError("missing process credentials")
    namespace = os.stat(f"/proc/{pid}/ns/pid")
    return {"pid": pid, "uid": credentials["Uid"], "gid": credentials["Gid"],
            "pidns_dev": namespace.st_dev, "pidns_ino": namespace.st_ino,
            "starttime_ticks": int(fields[19])}


def exited(fd: int) -> bool:
    poller = select.poll()
    poller.register(fd, select.POLLIN | select.POLLERR | select.POLLHUP)
    return bool(poller.poll(0))


def write_all(fd: int, data: bytes) -> None:
    offset = 0
    while offset < len(data):
        written = os.write(fd, data[offset:])
        if written <= 0:
            raise OSError("short witness write")
        offset += written


def initial_environment() -> dict[str, str]:
    # CPython can coerce locale and modify os.environ. Deployment policy pins
    # the actual exec environment rather than that mutated Python mapping.
    raw = Path("/proc/self/environ").read_bytes()
    if len(raw) > 64 * 1024 or (raw and not raw.endswith(b"\0")):
        raise ValueError("invalid initial environment")
    environment: dict[str, str] = {}
    for item in raw.split(b"\0"):
        if not item:
            continue
        name, separator, value = item.partition(b"=")
        if not separator or not name:
            raise ValueError("invalid initial environment entry")
        key, text = name.decode("ascii"), value.decode("ascii")
        if key in environment:
            raise ValueError("duplicate initial environment entry")
        environment[key] = text
    return environment


class Witness:
    """A single real parent and single durable writer; not a recovery proof."""

    def __init__(self, directory: Path, generation: str, broker: str) -> None:
        if not token(generation) or not token(broker) or directory.resolve(strict=True) != directory \
                or not directory.is_relative_to(RUNS):
            raise ValueError("invalid generation or evidence directory")
        meta = directory.stat(follow_symlinks=False)
        if not stat.S_ISDIR(meta.st_mode) or meta.st_uid != os.geteuid() or meta.st_mode & 0o077:
            raise ValueError("evidence directory must be private and owned")
        self.directory = directory
        self._directory_id = (meta.st_dev, meta.st_ino)
        self.generation = generation
        self.boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        self._journal_path = directory / "witness.journal"
        self._head_path = directory / "witness.head"
        self._journal = os.open(self._journal_path, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
        try:
            self._head_fd = os.open(self._head_path, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
        except BaseException:
            os.close(self._journal)
            raise
        self._journal_id = self._identity(self._journal)
        self._head_id = self._identity(self._head_fd)
        self._head = {"schema": HEAD_SCHEMA, "seq": 0, "offset": 0, "checksum": ZERO}
        self._children: dict[int, dict] = {}
        self._absent_resources: set[tuple[int, int]] = set()
        self._poisoned = False
        self._sealed = False
        try:
            os.fsync(self._journal)
            self._write_head(self._head)
            directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            self._record({"type": "witness_start", "run_id": generation, "broker": broker, "boot_id": self.boot,
                          "witness_birth": birth(os.getpid()),
                          "witness_code_sha256": digest_file(os.path.realpath(__file__)),
                          "interpreter_sha256": digest_file(os.path.realpath(sys.executable))})
        except BaseException:
            self._poisoned = True
            self.close()
            raise

    @staticmethod
    def _identity(fd: int) -> tuple[int, int]:
        meta = os.fstat(fd)
        return meta.st_dev, meta.st_ino

    def _write_head(self, head: dict) -> None:
        os.ftruncate(self._head_fd, 0)
        os.lseek(self._head_fd, 0, os.SEEK_SET)
        write_all(self._head_fd, canonical(head) + b"\n")
        os.fsync(self._head_fd)

    def _record(self, event: dict) -> None:
        if self._poisoned or self._sealed:
            raise RuntimeError("witness evidence unavailable")
        try:
            directory = self.directory.stat(follow_symlinks=False)
            if not stat.S_ISDIR(directory.st_mode) or directory.st_uid != os.geteuid() \
                    or directory.st_mode & 0o077 \
                    or (directory.st_dev, directory.st_ino) != self._directory_id:
                raise RuntimeError("witness directory identity changed")
            for path, fd, identity in ((self._journal_path, self._journal, self._journal_id),
                                       (self._head_path, self._head_fd, self._head_id)):
                entry, held = path.stat(follow_symlinks=False), os.fstat(fd)
                if (entry.st_dev, entry.st_ino) != identity or (held.st_dev, held.st_ino) != identity \
                        or entry.st_nlink != 1 or held.st_nlink != 1 or not stat.S_ISREG(held.st_mode) \
                        or held.st_uid != os.geteuid() or held.st_mode & 0o077:
                    raise RuntimeError("witness inode changed")
            expected = canonical(self._head) + b"\n"
            if os.fstat(self._journal).st_size != self._head["offset"] \
                    or os.pread(self._head_fd, len(expected) + 1, 0) != expected:
                raise RuntimeError("witness durable head changed")
            data = os.pread(self._journal, self._head["offset"] + 1, 0)
            if len(data) != self._head["offset"]:
                raise RuntimeError("witness journal changed while reading")
            previous = ZERO
            lines = data.splitlines(keepends=True)
            for seq, line in enumerate(lines, 1):
                if not line.endswith(b"\n") or len(line) > MAX_FRAME:
                    raise RuntimeError("invalid witness frame")
                frame = json.loads(line)
                if set(frame) != {"schema", "seq", "previous", "event", "checksum"} \
                        or canonical(frame) + b"\n" != line:
                    raise RuntimeError("noncanonical witness frame")
                checksum = frame.pop("checksum")
                if frame["schema"] != FRAME_SCHEMA or frame["seq"] != seq \
                        or frame["previous"] != previous \
                        or hashlib.sha256(canonical(frame)).hexdigest() != checksum:
                    raise RuntimeError("witness chain changed")
                previous = checksum
            if len(lines) != self._head["seq"] or previous != self._head["checksum"]:
                raise RuntimeError("witness journal does not match durable head")
            sequence = self._head["seq"] + 1
            material = {"schema": FRAME_SCHEMA, "seq": sequence, "previous": self._head["checksum"], "event": event}
            checksum = hashlib.sha256(canonical(material)).hexdigest()
            frame = canonical({**material, "checksum": checksum}) + b"\n"
            if len(frame) > MAX_FRAME or self._head["offset"] + len(frame) > MAX_LOG:
                raise RuntimeError("witness capacity exhausted")
            target = {"schema": HEAD_SCHEMA, "seq": sequence,
                      "offset": self._head["offset"] + len(frame), "checksum": checksum}
            os.lseek(self._journal, self._head["offset"], os.SEEK_SET)
            write_all(self._journal, frame)
            os.fsync(self._journal)
            self._write_head(target)
            self._head = target
        except BaseException:
            self._poisoned = True
            raise

    def spawn_gated(self, role: str, argv: list[str], env: dict[str, str], *,
                    control_fd: int | None = None) -> int:
        if self._poisoned or self._sealed or not token(role) or not argv \
                or not os.path.isabs(argv[0]) or os.path.realpath(argv[0]) != argv[0] \
                or not all(isinstance(item, str) and "\x00" not in item for item in argv) \
                or not isinstance(env, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in env.items()) \
                or (control_fd is not None and (type(control_fd) is not int or control_fd < 3)):
            raise ValueError("invalid witness child launch")
        if control_fd is not None and not stat.S_ISSOCK(os.fstat(control_fd).st_mode):
            raise ValueError("inherited ready control must be a socket")

        if (not self._children and role != "broker") or (self._children and role == "broker"):
            raise ValueError("one broker must be the first bound child")
        launch = f"{self.generation}-{len(self._children) + 1}"
        if not token(launch):
            raise ValueError("invalid or exhausted launch identity")
        read_fd, write_fd = os.pipe2(os.O_CLOEXEC)
        process = None
        pidfd = None
        try:
            process = subprocess.Popen([os.path.realpath(sys.executable), os.path.realpath(__file__),
                                        "_gate", str(read_fd), *argv], close_fds=True,
                                       pass_fds=(read_fd,), env=env,
                                       stdin=control_fd if control_fd is not None else subprocess.DEVNULL,
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            os.close(read_fd)
            read_fd = -1
            pidfd = os.pidfd_open(process.pid, 0)
            first, second = birth(process.pid), birth(process.pid)
            if first != second or exited(pidfd):
                raise RuntimeError("child did not remain live at gate")
            self._record({"type": "child_birth", "run_id": self.generation, "launch_id": launch,
                          "boot_id": self.boot, "role": role, "birth": first,
                          "executable_sha256": digest_file(argv[0]),
                          "argv_sha256": hashlib.sha256(canonical(argv)).hexdigest(), "pidfd_opened": True})
            self._children[process.pid] = {"process": process, "pidfd": pidfd, "birth": first,
                                           "launch": launch, "role": role, "gate": write_fd, "status": None,
                                           "resources": set()}
            pidfd = None
            write_fd = -1
            return process.pid
        except BaseException:
            self._poisoned = True
            if write_fd >= 0:
                os.close(write_fd)
                write_fd = -1
            if process is not None:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass  # unresolved, never kill to make a clean record
            raise
        finally:
            if read_fd >= 0:
                os.close(read_fd)
            if write_fd >= 0:
                os.close(write_fd)
            if pidfd is not None:
                os.close(pidfd)

    def release_gate(self, pid: int) -> None:
        child = self._children[pid]
        if self._poisoned or child["gate"] is None or exited(child["pidfd"]) \
                or birth(pid) != child["birth"]:
            self._poisoned = True
            raise RuntimeError("child or gate is no longer valid")
        try:
            write_all(child["gate"], b"R")
        except BaseException:
            self._poisoned = True
            raise
        finally:
            os.close(child["gate"])
            child["gate"] = None

    def wait(self, pid: int, timeout: float) -> int:
        child = self._children[pid]
        if self._poisoned or child["gate"] is not None or child["status"] is not None or timeout < 0:
            raise RuntimeError("invalid witness wait")
        poller = select.poll()
        poller.register(child["pidfd"], select.POLLIN | select.POLLERR | select.POLLHUP)
        if not poller.poll(int(timeout * 1000)):
            self._poisoned = True
            raise TimeoutError("child did not exit; no evidence seal")
        try:
            waited, status = os.waitpid(pid, 0)
            if waited != pid:
                raise RuntimeError("wrong child wait")
            child["process"].returncode = os.waitstatus_to_exitcode(status)
            child["status"] = status
            os.close(child["pidfd"])
            child["pidfd"] = None
            self._record({"type": "child_wait", "run_id": self.generation,
                          "launch_id": child["launch"], "boot_id": self.boot,
                          "role": child["role"], "birth": child["birth"],
                          "raw_wait_status": status,
                          "normal_exit_zero": os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0})
            return status
        except BaseException:
            self._poisoned = True
            raise

    @staticmethod
    def _resource(dev: int, ino: int) -> tuple[int, int]:
        if type(dev) is not int or type(ino) is not int or dev < 0 or ino <= 0:
            raise ValueError("invalid backing identity")
        return dev, ino

    @staticmethod
    def _observed(pid: int, dev: int, ino: int) -> tuple[bool, bool]:
        found = False
        for entry in Path(f"/proc/{pid}/fd").iterdir():
            try:
                item = entry.stat()
            except FileNotFoundError:  # the directory iterator's own fd can close
                continue
            found |= (item.st_dev, item.st_ino) == (dev, ino)
        mapped = False
        for line in Path(f"/proc/{pid}/maps").read_text().splitlines():
            fields = line.split(None, 5)
            if len(fields) >= 5:
                major, minor = fields[3].split(":")
                mapped |= int(fields[4]) == ino and getattr(os, "makedev")(int(major, 16), int(minor, 16)) == dev
        return found, mapped

    def record_child_backing(self, pid: int, dev: int, ino: int) -> None:
        """Observe a live direct child's backing without receiving its FD.

        These observations still need reconciliation against the complete
        authoritative journal. They do not by themselves prove zero or allow
        recovery, and never scan arbitrary non-witness children.
        """
        if self._poisoned or self._sealed:
            raise RuntimeError("witness evidence unavailable")
        try:
            identity = self._resource(dev, ino)
            child = self._children[pid]
            if identity in child["resources"] or child["gate"] is not None \
                    or child["status"] is not None or exited(child["pidfd"]) or birth(pid) != child["birth"]:
                raise RuntimeError("child is not stably live for backing observation")
            found, mapped = self._observed(pid, dev, ino)
            if not found or exited(child["pidfd"]) or birth(pid) != child["birth"]:
                raise RuntimeError("child did not stably hold backing FD")
            self._record({"type": "child_resource_observed", "run_id": self.generation,
                          "boot_id": self.boot, "launch_id": child["launch"], "role": child["role"],
                          "birth": child["birth"], "dev": dev, "ino": ino,
                          "fd_observed": True, "mapping_observed": mapped})
            if exited(child["pidfd"]) or birth(pid) != child["birth"]:
                raise RuntimeError("child changed while syncing backing observation")
            child["resources"].add(identity)
        except BaseException:
            self._poisoned = True
            raise

    def record_no_backing(self, dev: int, ino: int) -> None:
        """Record a live witness self-observation, not a global FD scan."""
        if self._poisoned or self._sealed:
            raise RuntimeError("witness evidence unavailable")
        try:
            identity = self._resource(dev, ino)
            if identity in self._absent_resources or any(self._observed(os.getpid(), dev, ino)):
                raise RuntimeError("witness holds or already recorded this backing")
            self._record({"type": "witness_no_backing", "run_id": self.generation,
                          "boot_id": self.boot, "dev": dev, "ino": ino})
            self._absent_resources.add(identity)
        except BaseException:
            self._poisoned = True
            raise

    def seal(self) -> dict:
        if self._poisoned or self._sealed or not self._children \
                or any(child["status"] is None or child["status"] != 0 for child in self._children.values()):
            raise RuntimeError("witness has incomplete or nonzero waits")
        self._record({"type": "witness_seal", "run_id": self.generation,
                      "children": len(self._children), "all_normal_zero": True})
        self._sealed = True
        return dict(self._head)

    def close(self) -> None:
        for child in self._children.values():
            for key in ("gate", "pidfd"):
                if child[key] is not None:
                    os.close(child[key])
                    child[key] = None
        for name in ("_journal", "_head_fd"):
            fd = getattr(self, name, -1)
            if fd >= 0:
                os.close(fd)
                setattr(self, name, -1)


def trusted_generation(daemon: str, endpoint: str, directory: Path, generation: str, broker: str) -> int:
    """Explicit deployment, fixed root; a verified BIND still cannot admit.

    Only a completed no-work lifecycle may be sealed; no quota is recovered.
    """
    import socket
    if not os.path.isabs(daemon) or os.path.realpath(daemon) != daemon or not token(broker):
        raise ValueError("invalid deployment executable or broker identity")
    env = initial_environment()
    witness = Witness(directory, generation, broker)
    binder_parent, binder_child = socket.socketpair()
    binder_parent.settimeout(15)
    child_parent = None
    binder = None
    seal_parent = None
    seal_child = None
    sealer = None

    def receive_exact(channel: socket.socket, message: bytes) -> None:
        data = bytearray()
        while len(data) < len(message):
            part = channel.recv(len(message) - len(data))
            if not part:
                raise RuntimeError("deployment channel closed")
            data.extend(part)
        if bytes(data) != message:
            raise RuntimeError("unexpected deployment acknowledgement")

    try:
        # A separate binder opens the fixed root and owns its own lock. The
        # witness and broker never receive that lock's open-file description.
        logfd = os.open(directory / "deployment-binder.log", os.O_WRONLY | os.O_CREAT | os.O_EXCL
                        | getattr(os, "O_CLOEXEC") | getattr(os, "O_NOFOLLOW"), 0o600)
        try:
            binder = subprocess.Popen([daemon, "--socket", endpoint, "--begin-external-ddr-generation"],
                                      env=env, stdin=binder_child.fileno(), stdout=logfd, stderr=logfd,
                                      close_fds=True)
        finally:
            os.close(logfd)
            binder_child.close()
        receive_exact(binder_parent, b"UFLOW-READY-1\n")
        receive_exact(binder_parent, b"UFLOW-BINDER-LOCKED\n")
        child_parent, child_socket = socket.socketpair()
        child_parent.settimeout(15)
        try:
            command = [daemon, "--socket", endpoint, "--execution-mode", "simulated",
                       "--startup-probe-bytes", "0", "--bound-external-ddr-gate"]
            pid = witness.spawn_gated("broker", command, env, control_fd=child_socket.fileno())
        finally:
            child_socket.close()
        witness.release_gate(pid)  # exec; the real daemon then reaches its own ready gate
        receive_exact(child_parent, b"UFLOW-READY-1\n")
        proposal = {"generation": generation, "broker": broker, "evidence_dir": str(directory)}
        binder_parent.sendall(b"R" + canonical(proposal) + b"\n")
        receive_exact(binder_parent, b"UFLOW-BINDER-RELEASED\n")
        if binder.wait(timeout=15) != 0:
            raise RuntimeError("trusted deployment binder failed")
        child_parent.sendall(b"R")
        receive_exact(child_parent, b"UFLOW-BOUND-QUARANTINED-1\n")
        print("UFLOW-BOUND-QUARANTINED", flush=True)
        child_parent.close()
        if witness.wait(pid, 300) != 0:
            raise RuntimeError("bound broker did not exit normally")
        witness.seal()
        # The broker has actually exited and released its own lock. The
        # sealer must open the same fixed root independently; it reads all
        # remaining evidence and its final head from the anchored BIND.
        seal_parent, seal_child = socket.socketpair()
        seal_parent.settimeout(15)
        logfd = os.open(directory / "deployment-sealer.log", os.O_WRONLY | os.O_CREAT | os.O_EXCL
                        | getattr(os, "O_CLOEXEC") | getattr(os, "O_NOFOLLOW"), 0o600)
        try:
            sealer = subprocess.Popen([daemon, "--socket", endpoint, "--seal-external-ddr-generation"],
                                      env=env, stdin=seal_child.fileno(), stdout=logfd, stderr=logfd,
                                      close_fds=True)
        finally:
            os.close(logfd)
            seal_child.close()
        receive_exact(seal_parent, b"UFLOW-READY-1\n")
        receive_exact(seal_parent, b"UFLOW-BINDER-LOCKED\n")
        seal_parent.sendall(b"R")
        receive_exact(seal_parent, b"UFLOW-SEAL-RELEASED\n")
        if sealer.wait(timeout=15) != 0:
            raise RuntimeError("trusted no-work sealer failed")
        print("no-work trust SEAL installed; admission and recovery remain disabled", flush=True)
        return 0
    finally:
        if child_parent is not None:
            child_parent.close()
        binder_parent.close()
        binder_child.close()
        if seal_parent is not None:
            seal_parent.close()
        if seal_child is not None:
            seal_child.close()
        witness.close()
        if binder is not None and binder.poll() is None:
            binder.wait(timeout=15)
        if sealer is not None and sealer.poll() is None:
            sealer.wait(timeout=15)


def main() -> int:
    if len(sys.argv) >= 4 and sys.argv[1] == "_gate":
        fd = int(sys.argv[2])
        try:
            signal = os.read(fd, 1)
        finally:
            os.close(fd)
        if signal != b"R":
            return 125
        os.execve(sys.argv[3], sys.argv[3:], initial_environment())
    if len(sys.argv) == 7 and sys.argv[1] == "--trusted-generation":
        return trusted_generation(sys.argv[2], sys.argv[3], Path(sys.argv[4]), sys.argv[5], sys.argv[6])
    if len(sys.argv) == 2 and sys.argv[1] == "_normal-child":
        return 0
    if len(sys.argv) == 3 and sys.argv[1] == "_resource-child":
        # Isolated writer selftest only; this is not a broker or business copy.
        import mmap
        directory = Path(sys.argv[2])
        fd = getattr(os, "memfd_create")("witness-observation-fixture", getattr(os, "MFD_CLOEXEC"))
        try:
            os.ftruncate(fd, 4096)
            with mmap.mmap(fd, 4096) as mapping:
                info = os.fstat(fd)
                temporary = directory / "test-resource.tmp"
                temporary.write_text(f"{info.st_dev} {info.st_ino}\n")
                temporary.replace(directory / "test-resource")
                deadline = time.monotonic() + 10
                while not (directory / "test-resource-finish").exists():
                    if time.monotonic() > deadline:
                        return 1
                    time.sleep(0.01)
            return 0
        finally:
            os.close(fd)
    if len(sys.argv) == 3 and sys.argv[1] == "_hold-child":
        # Isolated live-preflight test only, not an external DDR broker.
        marker = Path(sys.argv[2]) / "test-child-finish"
        deadline = time.monotonic() + 15
        while not marker.exists():
            if time.monotonic() > deadline:
                return 1
            time.sleep(0.01)
        return 0
    if len(sys.argv) == 5 and sys.argv[1] == "_livecase":
        # The child is a harmless independently exec'd program, not a UFlow
        # broker. The parent remains alive for the Rust pidfd/policy preflight.
        directory = Path(sys.argv[2])
        witness = Witness(directory, sys.argv[3], sys.argv[4])
        try:
            command = [os.path.realpath(sys.executable), os.path.realpath(__file__),
                       "_hold-child", str(directory)]
            pid = witness.spawn_gated("broker", command, {})
            witness.release_gate(pid)
            expected = b"\0".join(item.encode() for item in command) + b"\0"
            deadline = time.monotonic() + 5
            while Path(f"/proc/{pid}/cmdline").read_bytes() != expected:
                if time.monotonic() > deadline:
                    raise TimeoutError("test child did not exec its pinned program")
                time.sleep(0.01)
            print("READY", flush=True)
            sys.stdin.buffer.read(1)
            (directory / "test-child-finish").write_bytes(b"finish\n")
            if witness.wait(pid, 5) != 0:
                return 1
            witness.seal()
            return 0
        finally:
            witness.close()
    if (len(sys.argv) == 7 and sys.argv[1] == "_bindcase") or \
            (len(sys.argv) == 8 and sys.argv[1] == "_production_bindcase"):
        # Isolated multi-process experiment only. The ordinary production
        # binary has no alternate root or arbitrary-FD deployment option.
        import fcntl
        import socket
        production = sys.argv[1] == "_production_bindcase"
        authority, directory = Path(sys.argv[2]), Path(sys.argv[3])
        daemon = ""
        if production:
            binary, daemon, endpoint, generation = sys.argv[4:8]
            if not Path(daemon).is_relative_to(RUNS) or os.path.realpath(daemon) != daemon:
                raise ValueError("production binary must be a canonical task-owned copy")
        else:
            binary, endpoint, generation = sys.argv[4:7]
        if not authority.is_relative_to(RUNS) or authority.resolve(strict=True) != authority \
                or not directory.is_relative_to(RUNS) or directory.resolve(strict=True) != directory:
            raise ValueError("bindcase uses only canonical task-owned roots")
        close_on_exec, nofollow = getattr(os, "O_CLOEXEC"), getattr(os, "O_NOFOLLOW")
        flock = getattr(fcntl, "flock")
        lock_exclusive = getattr(fcntl, "LOCK_EX") | getattr(fcntl, "LOCK_NB")
        witness = Witness(directory, generation, "ready-broker")
        binder = None
        binder_parent, binder_child = socket.socketpair()
        binder_parent.settimeout(10)
        parent = None

        def receive_exact(channel: socket.socket, size: int) -> bytes:
            result = bytearray()
            while len(result) < size:
                data = channel.recv(size - len(result))
                if not data:
                    raise RuntimeError("bindcase control closed before acknowledgement")
                result.extend(data)
            return bytes(result)

        def require_busy() -> None:
            contender = os.open(authority / "authority.lock", os.O_RDONLY | close_on_exec | nofollow)
            try:
                try:
                    flock(contender, lock_exclusive)
                except BlockingIOError:
                    return
                raise RuntimeError("bindcase authority lock was unexpectedly available")
            finally:
                os.close(contender)

        try:
            params = {"root": str(authority), "evidence": str(directory), "generation": generation,
                      "broker": "ready-broker", "boot": witness.boot, "endpoint": endpoint}
            # The binder takes its own fresh lock before the broker exists;
            # neither witness nor broker inherits that lock description.
            binder_env = {"LD_LIBRARY_PATH": os.environ["LD_LIBRARY_PATH"], "UF_TEST_BINDER_CONTROL": "1"}
            logfd = os.open(directory / "test-broker.log", os.O_WRONLY | os.O_CREAT | os.O_EXCL | close_on_exec, 0o600)
            try:
                binder = subprocess.Popen([binary, "--exact", "ddr_managed_authority::tests::ready_binding_binder", "--nocapture"],
                                          env=binder_env, close_fds=True, stdin=binder_child.fileno(),
                                          stdout=logfd, stderr=logfd)
            finally:
                os.close(logfd)
                binder_child.close()
            binder_parent.sendall(canonical(params) + b"\n")
            if receive_exact(binder_parent, len(b"UFLOW-BINDER-LOCKED\n")) != b"UFLOW-BINDER-LOCKED\n":
                raise RuntimeError("bindcase binder did not lock")
            require_busy()
            parent, child = socket.socketpair()
            parent.settimeout(10)
            try:
                if production:
                    command = [daemon, "--socket", endpoint, "--execution-mode", "simulated",
                               "--startup-probe-bytes", "0", "--bound-external-ddr-gate"]
                    child_env = {"LD_LIBRARY_PATH": os.environ["LD_LIBRARY_PATH"],
                                 "UF_EXTERNAL_DDR_HOST_ID": "host", "UF_EXTERNAL_DDR_OWNER_MAX_BYTES": "16",
                                 "UF_EXTERNAL_DDR_TOTAL_MAX_BYTES": "64"}
                else:
                    command = [binary, "--exact", "ddr_managed_authority::tests::ready_binding_child", "--nocapture"]
                    child_env = {"LD_LIBRARY_PATH": os.environ["LD_LIBRARY_PATH"], "UF_TEST_READY_BINDING_CHILD": "1"}
                pid = witness.spawn_gated("broker", command, child_env, control_fd=child.fileno())
            finally:
                child.close()
            witness.release_gate(pid)  # exec only; the Rust child has a second ready gate
            if not production:
                parent.sendall(canonical(params) + b"\n")
            if receive_exact(parent, len(b"UFLOW-READY-1\n")) != b"UFLOW-READY-1\n":
                raise RuntimeError("bindcase child did not reach ready gate")
            require_busy()
            binder_parent.sendall(b"Q" if generation == "ready-uncommitted" else b"C")
            if receive_exact(binder_parent, len(b"UFLOW-BINDER-RELEASED\n")) != b"UFLOW-BINDER-RELEASED\n":
                raise RuntimeError("bindcase binder did not release")
            if binder.wait(timeout=10) != 0:
                raise RuntimeError("bindcase binder failed")
            if generation == "ready-uncommitted":
                # Negative fixture: even an authentic parent's release byte is
                # insufficient unless the independently anchored BIND exists.
                parent.sendall(b"R")
                try:
                    unexpected = parent.recv(64)
                except ConnectionResetError:
                    unexpected = b""
                if unexpected:
                    raise RuntimeError("uncommitted binding was acknowledged")
                parent.close()
                if witness.wait(pid, 10) == 0:
                    raise RuntimeError("uncommitted binding child exited successfully")
                try:
                    witness.seal()
                except RuntimeError:
                    pass
                else:
                    raise RuntimeError("uncommitted binding generated a normal seal")
                print("bindcase: uncommitted release refused before any admission", flush=True)
                return 0
            # In the real gap a third process can obtain the unlocked inode,
            # but the committed BIND must leave it quarantined and unchanged.
            gap_env = {"LD_LIBRARY_PATH": os.environ["LD_LIBRARY_PATH"],
                       "UF_TEST_BOUND_CONTENDER_PARAMS": canonical(params).decode("ascii")}
            gap = subprocess.run([binary, "--exact", "ddr_managed_authority::tests::ready_binding_gap_contender", "--nocapture"],
                                 env=gap_env, close_fds=True, stdin=subprocess.DEVNULL,
                                 stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=10)
            if gap.returncode != 0:
                raise RuntimeError(f"bindcase gap contender failed: {gap.stdout.decode(errors='replace')}")
            parent.sendall(b"R")
            acknowledgement = b"UFLOW-BOUND-QUARANTINED-1\n" if production else b"UFLOW-BOUND-1\n"
            if receive_exact(parent, len(acknowledgement)) != acknowledgement:
                raise RuntimeError("bindcase child did not verify committed BIND")
            require_busy()  # broker independently acquired the original lock inode
            if production:
                deadline = time.monotonic() + 10
                while not Path(endpoint).exists() or not Path(endpoint + ".external-ddr").exists():
                    if time.monotonic() >= deadline or witness._children[pid]["process"].poll() is not None:
                        raise RuntimeError("production quarantine listener did not start")
                    time.sleep(0.01)
                def control(request: bytes) -> bytes:
                    with socket.socket(getattr(socket, "AF_UNIX"), socket.SOCK_STREAM) as client:
                        client.settimeout(5)
                        client.connect(endpoint)
                        client.sendall(request)
                        reply = bytearray()
                        while b"\n" not in reply and len(reply) < 8192:
                            part = client.recv(8192)
                            if not part:
                                break
                            reply.extend(part)
                        return bytes(reply)
                if b"external_ddr_committed_bytes=unknown" not in control(b"op=GetStats\n"):
                    raise RuntimeError("production quarantine reported a zero catalog")
                if b"status=ok" not in control(b"op=ShutdownDaemon\n"):
                    raise RuntimeError("production quarantine shutdown failed")
            else:
                parent.sendall(b"S")
            parent.close()
            if witness.wait(pid, 10) != 0:
                raise RuntimeError("bindcase broker helper did not exit normally")
            witness.seal()
            print(f"bindcase: real witness={os.getpid()} child={pid}, BIND anchored; independent lock acquired; production={production}", flush=True)
            return 0
        finally:
            if parent is not None:
                parent.close()
            binder_parent.close()
            binder_child.close()
            witness.close()
            if binder is not None and binder.poll() is None:
                binder.wait(timeout=10)
    if len(sys.argv) == 5 and sys.argv[1] == "_selftest":
        # This verifies the evidence primitive only, not authority or recovery.
        directory = Path(sys.argv[2])
        witness = Witness(directory, sys.argv[3], sys.argv[4])
        try:
            command = [os.path.realpath(sys.executable), os.path.realpath(__file__), "_resource-child", str(directory)]
            pid = witness.spawn_gated("broker", command, {})
            witness.release_gate(pid)
            info_path = directory / "test-resource"
            deadline = time.monotonic() + 5
            while not info_path.exists():
                if time.monotonic() > deadline:
                    raise TimeoutError("test child did not hold its backing")
                time.sleep(0.01)
            dev, ino = (int(part) for part in info_path.read_text().split())
            witness.record_child_backing(pid, dev, ino)
            witness.record_no_backing(dev, ino)
            (directory / "test-resource-finish").write_bytes(b"finish\n")
            if witness.wait(pid, 5) != 0:
                return 1
            witness.seal()
            return 0
        finally:
            witness.close()
    raise ValueError("not a deployment CLI")


if __name__ == "__main__":
    sys.exit(main())
