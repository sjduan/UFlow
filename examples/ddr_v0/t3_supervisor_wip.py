#!/usr/bin/env python3
"""WIP direct-parent witness primitive for isolated DDR recovery fixtures.

This is not a trusted deployment witness, recovery CLI, or authorization to
clear quarantine. The production bootstrap does not pin this program/run/head
and the broker does not yet log complete obligations. The supervisor never
accepts backing FDs and launches independent children with a pre-exec gate.
Only actual waitpid statuses are recorded; child output cannot claim exit 0.
"""
from __future__ import annotations

import argparse
from collections.abc import Callable
import hashlib
import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

SCHEMA = "uflow-ddr-witness-fixture-v1"
GENESIS = "0" * 64
MAX_LOG = 16 * 1024 * 1024
MAX_FRAME = 8192
FIXTURE_ROOT: Path | None = None


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def _write_all(fd: int, data: bytes) -> None:
    offset = 0
    while offset < len(data):
        written = os.write(fd, data[offset:])
        if written <= 0:
            raise OSError("short write in witness journal")
        offset += written


def _digest_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _birth(pid: int) -> dict:
    stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    begin, end = stat.find("("), stat.rfind(")")
    if begin < 0 or end <= begin or int(stat[:begin].strip()) != pid:
        raise ValueError("invalid /proc stat identity")
    fields = stat[end + 1:].split()
    if len(fields) <= 19 or len(fields[0]) != 1:
        raise ValueError("short /proc stat")
    values = {}
    for line in Path(f"/proc/{pid}/status").read_text(encoding="utf-8").splitlines():
        if line.startswith(("Uid:", "Gid:")):
            name, raw = line.split(":", 1)
            if name in values or len(raw.split()) != 4:
                raise ValueError("invalid /proc credentials")
            values[name] = int(raw.split()[1])
    if set(values) != {"Uid", "Gid"}:
        raise ValueError("missing /proc credentials")
    namespace = os.stat(f"/proc/{pid}/ns/pid")
    return {"pid": pid, "uid": values["Uid"], "gid": values["Gid"],
            "pidns_dev": namespace.st_dev, "pidns_ino": namespace.st_ino,
            "starttime_ticks": int(fields[19])}


def _normal(status: int) -> bool:
    return os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0


def _pidfd_exited(pidfd: int) -> bool:
    poller = select.poll()
    poller.register(pidfd, select.POLLIN | select.POLLERR | select.POLLHUP)
    return bool(poller.poll(0))


class Witness:
    """Single writer; direct parent only. No production trust is established."""

    def __init__(self, directory: Path, run_id: str, require_fixture_binding: bool = False) -> None:
        if not run_id or not all(c.isalnum() or c in "._-" for c in run_id):
            raise ValueError("invalid witness run id")
        self.directory = directory
        self.run_id = run_id
        self.journal_path = directory / "witness.journal"
        self.head_path = directory / "witness.head"
        self._journal = os.open(self.journal_path, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
        try:
            self._head_fd = os.open(self.head_path, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
        except BaseException:
            os.close(self._journal)
            raise
        self._head = {"schema": SCHEMA, "seq": 0, "offset": 0, "checksum": GENESIS}
        journal_meta = os.fstat(self._journal)
        head_meta = os.fstat(self._head_fd)
        self._journal_identity = (journal_meta.st_dev, journal_meta.st_ino)
        self._head_identity = (head_meta.st_dev, head_meta.st_ino)
        self._children: dict[int, dict] = {}
        self._next_launch = 1
        self._require_fixture_binding = require_fixture_binding
        self._fixture_binding_check: Callable[[], None] | None = None
        self._fixture_child_sha256: str | None = None
        self._fixture_seal_callback: Callable[[], None] | None = None
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
            witness_birth = _birth(os.getpid())
            self.boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
            interpreter = os.path.realpath(sys.executable)
            self._record({"type": "witness_start", "run_id": run_id,
                          "boot_id": self.boot_id,
                          "witness_birth": witness_birth, "witness_code_sha256": _digest_file(__file__),
                          "interpreter_sha256": _digest_file(interpreter)})
        except BaseException:
            self._poisoned = True
            self.close()
            raise

    def _write_head(self, head: dict) -> None:
        data = _canonical(head) + b"\n"
        os.ftruncate(self._head_fd, 0)
        os.lseek(self._head_fd, 0, os.SEEK_SET)
        _write_all(self._head_fd, data)
        os.fsync(self._head_fd)

    def _record(self, event: dict) -> None:
        if self._poisoned or self._sealed:
            raise RuntimeError("witness journal unavailable")
        try:
            for path, fd, identity in ((self.journal_path, self._journal, self._journal_identity),
                                       (self.head_path, self._head_fd, self._head_identity)):
                entry, held = os.stat(path, follow_symlinks=False), os.fstat(fd)
                if (entry.st_dev, entry.st_ino) != identity or (held.st_dev, held.st_ino) != identity \
                        or entry.st_nlink != 1 or held.st_nlink != 1:
                    raise RuntimeError("witness file was replaced or unlinked")
            if os.fstat(self._journal).st_size != self._head["offset"]:
                raise RuntimeError("witness journal length changed")
            expected_head = _canonical(self._head) + b"\n"
            if os.pread(self._head_fd, len(expected_head) + 1, 0) != expected_head:
                raise RuntimeError("witness durable head changed")
            verify_fixture_structure(self.directory)
        except BaseException:
            self._poisoned = True
            raise
        sequence = self._head["seq"] + 1
        material = {"schema": SCHEMA, "seq": sequence, "previous": self._head["checksum"], "event": event}
        digest = hashlib.sha256(_canonical(material)).hexdigest()
        frame = _canonical({**material, "checksum": digest}) + b"\n"
        if len(frame) > MAX_FRAME or self._head["offset"] + len(frame) > MAX_LOG:
            self._poisoned = True
            raise RuntimeError("witness journal limit reached")
        next_head = {"schema": SCHEMA, "seq": sequence, "offset": self._head["offset"] + len(frame), "checksum": digest}
        try:
            os.lseek(self._journal, self._head["offset"], os.SEEK_SET)
            _write_all(self._journal, frame)
            os.fsync(self._journal)
            self._write_head(next_head)
        except BaseException:
            self._poisoned = True
            raise
        self._head = next_head

    def spawn(self, role: str, argv: list[str], *, env: dict[str, str] | None = None,
              control: bool = False) -> int:
        if self._poisoned or self._sealed or not role or not argv or not os.path.isabs(argv[0]):
            raise ValueError("invalid or unavailable witness launch")
        if self._require_fixture_binding:
            if self._fixture_binding_check is None:
                raise RuntimeError("fixture witness has not been deployment-bound")
            try:
                self._fixture_binding_check()
            except BaseException:
                self._poisoned = True
                raise
        if not all(c.isalnum() or c in "._-" for c in role):
            raise ValueError("invalid role")
        executable = os.path.realpath(argv[0])
        code_hash = _digest_file(executable)
        if self._require_fixture_binding and code_hash != self._fixture_child_sha256:
            self._poisoned = True
            raise ValueError("child executable differs from fixture deployment pin")
        command_hash = hashlib.sha256(_canonical(argv)).hexdigest()
        launch_id = f"{self.run_id}-{self._next_launch}"
        self._next_launch += 1
        read_fd, write_fd = os.pipe2(os.O_CLOEXEC)
        process = None
        pidfd = None
        try:
            process = subprocess.Popen([sys.executable, __file__, "_gate", str(read_fd), *argv],
                                       pass_fds=(read_fd,), close_fds=True,
                                       env=env,
                                       stdin=subprocess.PIPE if control else subprocess.DEVNULL,
                                       stdout=subprocess.PIPE if control else subprocess.DEVNULL,
                                       stderr=None if control else subprocess.DEVNULL)
            os.close(read_fd)
            read_fd = -1
            # While the gate is closed, the supervisor is the only parent and
            # will not reap the process. The pidfd remains held until waitpid.
            pidfd = os.pidfd_open(process.pid, 0)
            if _pidfd_exited(pidfd):
                raise RuntimeError("child died before birth was pinned")
            first, second = _birth(process.pid), _birth(process.pid)
            if first != second or _pidfd_exited(pidfd):
                raise RuntimeError("child birth changed before gate")
            self._record({"type": "child_birth", "run_id": self.run_id, "launch_id": launch_id,
                          "boot_id": self.boot_id, "role": role, "birth": first,
                          "executable_sha256": code_hash, "argv_sha256": command_hash,
                          "pidfd_opened": True})
            self._children[process.pid] = {"process": process, "pidfd": pidfd, "birth": first,
                                           "launch_id": launch_id, "role": role, "status": None}
            pidfd = None
            if _pidfd_exited(self._children[process.pid]["pidfd"]):
                raise RuntimeError("child died before ready gate")
            _write_all(write_fd, b"R")
            return process.pid
        except BaseException:
            self._poisoned = True
            # A child whose gate is not released cannot obtain managed FDs.
            # Close the gate, then reap that run-owned child normally.
            if write_fd >= 0:
                os.close(write_fd)
                write_fd = -1
            if process is not None:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass  # report unresolved; never kill to hide it
            raise
        finally:
            if read_fd >= 0:
                os.close(read_fd)
            if write_fd >= 0:
                os.close(write_fd)
            if pidfd is not None:
                os.close(pidfd)

    def wait(self, pid: int, timeout: float | None = None) -> int:
        child = self._children.get(pid)
        if child is None or child["status"] is not None:
            raise ValueError("unknown or already waited child")
        if timeout is not None:
            if timeout < 0:
                raise ValueError("negative wait timeout")
            poller = select.poll()
            poller.register(child["pidfd"], select.POLLIN | select.POLLERR | select.POLLHUP)
            if not poller.poll(int(timeout * 1000)):
                self._poisoned = True
                raise TimeoutError("child wait timed out; roster cannot be trusted")
        try:
            waited_pid, status = os.waitpid(pid, 0)
        except ChildProcessError:
            self._poisoned = True
            raise RuntimeError("ECHILD: witness cannot prove exit status") from None
        if waited_pid != pid:
            self._poisoned = True
            raise RuntimeError("waitpid returned wrong child")
        child["process"].returncode = os.waitstatus_to_exitcode(status)
        child["status"] = status
        os.close(child["pidfd"])
        child["pidfd"] = None
        self._record({"type": "child_wait", "run_id": self.run_id, "launch_id": child["launch_id"],
                      "boot_id": self.boot_id, "role": child["role"], "birth": child["birth"],
                      "raw_wait_status": status, "normal_exit_zero": _normal(status)})
        return status

    def record_no_backing(self, dev: int, ino: int) -> None:
        """Record a live self-observation; the broker journal must match it.

        This does not scan other processes and is not an authorization. The
        inode is compared to the actual register intent in the fixture test.
        """
        for entry in Path("/proc/self/fd").iterdir():
            try:
                meta = entry.stat()
            except FileNotFoundError:  # the directory iterator's own fd
                continue
            if (meta.st_dev, meta.st_ino) == (dev, ino):
                raise RuntimeError("witness unexpectedly holds backing FD")
        for mapping in Path("/proc/self/maps").read_text().splitlines():
            fields = mapping.split(None, 5)
            if len(fields) >= 5:
                major, minor = fields[3].split(":")
                if int(fields[4]) == ino and getattr(os, "makedev")(int(major, 16), int(minor, 16)) == dev:
                    raise RuntimeError("witness unexpectedly maps backing")
        self._record({"type": "witness_no_backing", "run_id": self.run_id,
                      "boot_id": self.boot_id, "dev": dev, "ino": ino})

    def record_child_backing(self, pid: int, dev: int, ino: int) -> None:
        """Observe a live direct child's FD/map, without receiving the FD."""
        child = self._children[pid]
        if child["status"] is not None or _pidfd_exited(child["pidfd"]) \
                or _birth(pid) != child["birth"]:
            raise RuntimeError("child birth not live during resource observation")
        found = False
        for entry in Path(f"/proc/{pid}/fd").iterdir():
            try:
                meta = entry.stat()
            except FileNotFoundError:
                continue
            found |= (meta.st_dev, meta.st_ino) == (dev, ino)
        mapped = False
        for mapping in Path(f"/proc/{pid}/maps").read_text().splitlines():
            fields = mapping.split(None, 5)
            if len(fields) >= 5:
                major, minor = fields[3].split(":")
                mapped |= int(fields[4]) == ino and getattr(os, "makedev")(int(major, 16), int(minor, 16)) == dev
        if not found or _pidfd_exited(child["pidfd"]) or _birth(pid) != child["birth"]:
            raise RuntimeError("child did not stably hold backing FD")
        self._record({"type": "child_resource_observed", "run_id": self.run_id,
                      "boot_id": self.boot_id, "launch_id": child["launch_id"],
                      "role": child["role"], "birth": child["birth"],
                      "dev": dev, "ino": ino, "fd_observed": True, "mapping_observed": mapped})

    def seal(self) -> dict:
        if self._poisoned or self._sealed or any(child["status"] is None for child in self._children.values()):
            raise RuntimeError("witness cannot seal incomplete roster")
        self._record({"type": "witness_seal", "run_id": self.run_id,
                      "children": len(self._children),
                      "all_normal_zero": all(_normal(c["status"]) for c in self._children.values())})
        self._sealed = True
        if self._require_fixture_binding:
            try:
                if self._fixture_seal_callback is None:
                    raise RuntimeError("fixture seal was not deployment-bound")
                self._fixture_seal_callback()
            except BaseException:
                self._poisoned = True
                raise
        return dict(self._head)

    def close(self) -> None:
        for name in ("_journal", "_head_fd"):
            fd = getattr(self, name, -1)
            if fd >= 0:
                os.close(fd)
                setattr(self, name, -1)
        for child in self._children.values():
            if child["pidfd"] is not None:
                os.close(child["pidfd"])
                child["pidfd"] = None


def verify_fixture_structure(directory: Path) -> list[dict]:
    """Validate bytes in a fixture only; not a production trust verifier."""
    if (directory / "witness.head").stat().st_size > 4096 \
            or (directory / "witness.journal").stat().st_size > MAX_LOG:
        raise ValueError("witness file size limit exceeded")
    head_bytes = (directory / "witness.head").read_bytes()
    head = json.loads(head_bytes)
    if _canonical(head) + b"\n" != head_bytes or set(head) != {"schema", "seq", "offset", "checksum"}:
        raise ValueError("noncanonical witness head")
    data = (directory / "witness.journal").read_bytes()
    if head["schema"] != SCHEMA or len(data) != head["offset"] or len(data) > MAX_LOG:
        raise ValueError("witness exact head mismatch")
    previous = GENESIS
    events = []
    for sequence, line in enumerate(data.splitlines(keepends=True), 1):
        if not line.endswith(b"\n") or len(line) > MAX_FRAME:
            raise ValueError("invalid witness frame length")
        frame = json.loads(line)
        if _canonical(frame) + b"\n" != line or set(frame) != {"schema", "seq", "previous", "event", "checksum"}:
            raise ValueError("noncanonical witness frame")
        checksum = frame.pop("checksum")
        if frame["schema"] != SCHEMA or frame["seq"] != sequence or frame["previous"] != previous:
            raise ValueError("witness sequence or chain mismatch")
        if hashlib.sha256(_canonical(frame)).hexdigest() != checksum:
            raise ValueError("witness checksum mismatch")
        previous = checksum
        events.append(frame["event"])
    if len(events) != head["seq"] or previous != head["checksum"]:
        raise ValueError("witness final head mismatch")
    return events


def verify_fixture_roster(events: list[dict], run_id: str, witness_code_sha256: str,
                          witness_birth: dict, boot_id: str, required_births: list[dict]) -> None:
    """Strict fixture assertion only: bootstrap has not pinned these inputs.

    A caller cannot use this function to authorize production recovery. The
    future deployment verifier must obtain the expected run/code/head from the
    fixed bootstrap, and join the roster with a complete broker obligation log.
    """
    if len(events) < 2 or events[0].get("type") != "witness_start" \
            or events[-1].get("type") != "witness_seal":
        raise ValueError("missing witness start or seal")
    start, seal = events[0], events[-1]
    if set(start) != {"type", "run_id", "boot_id", "witness_birth", "witness_code_sha256", "interpreter_sha256"} \
            or set(seal) != {"type", "run_id", "children", "all_normal_zero"} \
            or start.get("run_id") != run_id or seal.get("run_id") != run_id \
            or start.get("witness_code_sha256") != witness_code_sha256 \
            or start.get("witness_birth") != witness_birth or start.get("boot_id") != boot_id:
        raise ValueError("witness run or code mismatch")
    births: dict[bytes, dict] = {}
    waits: dict[bytes, int] = {}
    active_pids: dict[int, bytes] = {}
    launch_ids: set[str] = set()
    for item in events[1:-1]:
        kind = item.get("type")
        if kind == "witness_no_backing":
            if set(item) != {"type", "run_id", "boot_id", "dev", "ino"} \
                    or item["run_id"] != run_id or item["boot_id"] != boot_id \
                    or type(item["dev"]) is not int or item["dev"] < 0 \
                    or type(item["ino"]) is not int or item["ino"] <= 0:
                raise ValueError("invalid witness resource observation")
            continue
        if kind == "child_resource_observed":
            birth = item.get("birth")
            key = _canonical(birth)
            if set(item) != {"type", "run_id", "boot_id", "launch_id", "role", "birth", "dev", "ino", "fd_observed", "mapping_observed"} \
                    or item["run_id"] != run_id or item["boot_id"] != boot_id \
                    or key not in births or key in waits or item["role"] != births[key]["role"] \
                    or item["launch_id"] != births[key]["launch_id"] \
                    or item["fd_observed"] is not True or type(item["mapping_observed"]) is not bool \
                    or type(item["dev"]) is not int or item["dev"] < 0 \
                    or type(item["ino"]) is not int or item["ino"] <= 0:
                raise ValueError("invalid live child resource observation")
            continue
        if kind not in ("child_birth", "child_wait"):
            raise ValueError("unknown witness roster event")
        birth = item.get("birth")
        if not isinstance(birth, dict) or set(birth) != {"pid", "uid", "gid", "pidns_dev", "pidns_ino", "starttime_ticks"} \
                or any(type(value) is not int or value < 0 for value in birth.values()) \
                or birth["pid"] == 0 or birth["pidns_ino"] == 0 or birth["starttime_ticks"] == 0:
            raise ValueError("invalid child birth")
        key = _canonical(birth)
        if item.get("run_id") != run_id or item.get("boot_id") != boot_id \
                or not isinstance(item.get("launch_id"), str) \
                or not item["launch_id"].startswith(run_id + "-"):
            raise ValueError("child launch generation mismatch")
        if kind == "child_birth":
            if key in births or birth["pid"] in active_pids \
                    or item["launch_id"] in launch_ids \
                    or set(item) != {"type", "run_id", "launch_id", "boot_id", "role", "birth", "executable_sha256", "argv_sha256", "pidfd_opened"} \
                    or item["pidfd_opened"] is not True:
                raise ValueError("duplicate or malformed child birth")
            if not isinstance(item["role"], str) or not item["role"] \
                    or any(not (c.isalnum() or c in "._-") for c in item["role"]) \
                    or any(not isinstance(item[field], str) or len(item[field]) != 64 \
                           or any(c not in "0123456789abcdef" for c in item[field]) \
                           for field in ("executable_sha256", "argv_sha256")):
                raise ValueError("invalid child launch binding")
            births[key] = item
            launch_ids.add(item["launch_id"])
            active_pids[birth["pid"]] = key
        else:
            if key not in births or key in waits or set(item) != {"type", "run_id", "launch_id", "boot_id", "role", "birth", "raw_wait_status", "normal_exit_zero"} \
                    or item["role"] != births[key]["role"] \
                    or item["launch_id"] != births[key]["launch_id"]:
                raise ValueError("unknown or duplicate child wait")
            status = item["raw_wait_status"]
            if type(status) is not int or not 0 <= status <= 0xffff \
                    or not (os.WIFEXITED(status) or os.WIFSIGNALED(status)) \
                    or type(item["normal_exit_zero"]) is not bool \
                    or item["normal_exit_zero"] != _normal(status):
                raise ValueError("invalid kernel wait status")
            waits[key] = status
            if active_pids.pop(birth["pid"], None) != key:
                raise ValueError("child PID birth mismatch")
    if set(births) != set(waits) or active_pids or type(seal.get("children")) is not int \
            or seal.get("children") != len(births) \
            or type(seal.get("all_normal_zero")) is not bool \
            or seal["all_normal_zero"] != all(_normal(status) for status in waits.values()):
        raise ValueError("incomplete or inconsistent sealed roster")
    if any(not _normal(status) for status in waits.values()):
        raise ValueError("nonzero or signalled child exit")
    if any(_canonical(birth) not in waits for birth in required_births):
        raise ValueError("potential holder missing from roster")


class FixtureTests(unittest.TestCase):
    def setUp(self) -> None:
        assert FIXTURE_ROOT is not None
        self.root = Path(tempfile.mkdtemp(prefix="witness-", dir=FIXTURE_ROOT))

    def test_actual_direct_child_wait_and_nonzero_rejection(self) -> None:
        witness = Witness(self.root, "fixture-run")
        try:
            zero = witness.spawn("owner", [sys.executable, __file__, "_exit", "0"])
            nonzero = witness.spawn("accessor", [sys.executable, __file__, "_exit", "7"])
            with self.assertRaises(RuntimeError):
                witness.seal()
            status0, status7 = witness.wait(zero), witness.wait(nonzero)
            self.assertTrue(_normal(status0))
            self.assertFalse(_normal(status7))
            self.assertEqual(os.WEXITSTATUS(status7), 7)
            sealed = witness.seal()
        finally:
            witness.close()
        events = verify_fixture_structure(self.root)
        self.assertEqual([item["type"] for item in events],
                         ["witness_start", "child_birth", "child_birth", "child_wait", "child_wait", "witness_seal"])
        self.assertEqual(events[-1]["children"], 2)
        self.assertFalse(events[-1]["all_normal_zero"])
        self.assertEqual(sealed["seq"], len(events))
        self.assertEqual(events[1]["birth"], events[3]["birth"])
        self.assertEqual(events[2]["birth"], events[4]["birth"])
        self.assertFalse(_normal(15))  # synthetic signal wait status, not a real crash test
        start = events[0]
        with self.assertRaises(ValueError):
            verify_fixture_roster(events, "fixture-run", start["witness_code_sha256"],
                                  start["witness_birth"], start["boot_id"], [events[1]["birth"]])

    def test_normal_roster_and_synthetic_missing_or_forged_holder_rejection(self) -> None:
        witness = Witness(self.root, "complete-run")
        try:
            a = witness.spawn("owner", [sys.executable, __file__, "_exit", "0"])
            b = witness.spawn("accessor", [sys.executable, __file__, "_exit", "0"])
            witness.wait(a)
            witness.wait(b)
            witness.seal()
        finally:
            witness.close()
        events = verify_fixture_structure(self.root)
        start = events[0]
        births = [events[1]["birth"], events[2]["birth"]]
        expected = ("complete-run", start["witness_code_sha256"], start["witness_birth"], start["boot_id"])
        verify_fixture_roster(events, *expected, births)
        wrong_birth = dict(births[0])
        wrong_birth["starttime_ticks"] += 1
        with self.assertRaises(ValueError):
            verify_fixture_roster(events, *expected, [wrong_birth])
        with self.assertRaises(ValueError):
            verify_fixture_roster(events, "other-run", *expected[1:], births)
        with self.assertRaises(ValueError):
            verify_fixture_roster(events, expected[0], "0" * 64, *expected[2:], births)
        for field in ("starttime_ticks", "pidns_ino", "pidns_dev"):
            wrong_birth = dict(births[0])
            wrong_birth[field] += 1
            with self.assertRaises(ValueError):
                verify_fixture_roster(events, *expected, [wrong_birth])
        wrong_generation = json.loads(json.dumps(events))
        wrong_generation[1]["boot_id"] = "another-boot"
        with self.assertRaises(ValueError):
            verify_fixture_roster(wrong_generation, *expected, births)
        wrong_launch = json.loads(json.dumps(events))
        wrong_launch[3]["launch_id"] = wrong_launch[2]["launch_id"]
        with self.assertRaises(ValueError):
            verify_fixture_roster(wrong_launch, *expected, births)
        # These mutations are synthetic verifier inputs, not real waitpid data.
        missing = [item for index, item in enumerate(events) if index != 3]
        with self.assertRaises(ValueError):
            verify_fixture_roster(missing, *expected, births)
        forged = json.loads(json.dumps(events))
        forged[3]["raw_wait_status"] = 15
        with self.assertRaises(ValueError):
            verify_fixture_roster(forged, *expected, births)

    def test_complete_tail_loss_does_not_match_exact_head(self) -> None:
        witness = Witness(self.root, "tail-loss")
        pid = witness.spawn("owner", [sys.executable, __file__, "_exit", "0"])
        witness.wait(pid)
        witness.seal()
        witness.close()
        data = (self.root / "witness.journal").read_bytes()
        frames = data.splitlines(keepends=True)
        (self.root / "witness.journal").write_bytes(b"".join(frames[:-1]))
        with self.assertRaises(ValueError):
            verify_fixture_structure(self.root)

    def test_real_signal_status_is_recorded_but_not_accepted(self) -> None:
        witness = Witness(self.root, "signal-run")
        try:
            pid = witness.spawn("worker", [sys.executable, __file__, "_signal"])
            status = witness.wait(pid, timeout=5)
            self.assertTrue(os.WIFSIGNALED(status))
            self.assertEqual(os.WTERMSIG(status), signal.SIGTERM)
            witness.seal()
        finally:
            witness.close()
        events = verify_fixture_structure(self.root)
        start = events[0]
        with self.assertRaises(ValueError):
            verify_fixture_roster(events, start["run_id"], start["witness_code_sha256"],
                                  start["witness_birth"], start["boot_id"], [events[1]["birth"]])

    def test_timeout_and_echild_never_seal(self) -> None:
        timeout_dir = self.root / "timeout"
        timeout_dir.mkdir()
        witness = Witness(timeout_dir, "timeout-run")
        try:
            pid = witness.spawn("worker", [sys.executable, __file__, "_sleep"])
            with self.assertRaises(TimeoutError):
                witness.wait(pid, timeout=0.01)
            with self.assertRaises(RuntimeError):
                witness.seal()
            # This fixture child exits naturally; reap it, never kill it.
            waited, status = os.waitpid(pid, 0)
            self.assertEqual(waited, pid)
            self.assertTrue(_normal(status))
            witness._children[pid]["process"].returncode = 0
        finally:
            witness.close()
        self.assertNotEqual(verify_fixture_structure(timeout_dir)[-1]["type"], "witness_seal")

        echild_dir = self.root / "echild"
        echild_dir.mkdir()
        witness = Witness(echild_dir, "echild-run")
        try:
            pid = witness.spawn("owner", [sys.executable, __file__, "_exit", "0"])
            waited, status = os.waitpid(pid, 0)
            self.assertEqual(waited, pid)
            self.assertTrue(_normal(status))
            witness._children[pid]["process"].returncode = 0
            with self.assertRaisesRegex(RuntimeError, "ECHILD"):
                witness.wait(pid)
            with self.assertRaises(RuntimeError):
                witness.seal()
        finally:
            witness.close()
        self.assertNotEqual(verify_fixture_structure(echild_dir)[-1]["type"], "witness_seal")

    def test_unstable_birth_does_not_open_gate_or_seal(self) -> None:
        witness = Witness(self.root, "unstable-run")
        original = _birth
        observations = 0

        def unstable(pid: int) -> dict:
            nonlocal observations
            result = original(pid)
            observations += 1
            if observations == 2:
                result["starttime_ticks"] += 1
            return result

        try:
            with mock.patch.object(sys.modules[__name__], "_birth", side_effect=unstable):
                with self.assertRaisesRegex(RuntimeError, "birth changed"):
                    witness.spawn("owner", [sys.executable, __file__, "_exit", "0"])
            with self.assertRaises(RuntimeError):
                witness.seal()
        finally:
            witness.close()
        self.assertEqual([event["type"] for event in verify_fixture_structure(self.root)],
                         ["witness_start"])


def main() -> None:
    global FIXTURE_ROOT
    if len(sys.argv) >= 3 and sys.argv[1] == "_gate":
        gate = int(sys.argv[2])
        allowed = os.read(gate, 1)
        os.close(gate)
        if allowed != b"R":
            raise SystemExit(2)
        os.execvpe(sys.argv[3], sys.argv[3:], os.environ)
    if len(sys.argv) == 3 and sys.argv[1] == "_exit":
        raise SystemExit(int(sys.argv[2]))
    if len(sys.argv) == 2 and sys.argv[1] == "_signal":
        os.kill(os.getpid(), signal.SIGTERM)
        raise SystemExit(3)
    if len(sys.argv) == 2 and sys.argv[1] == "_sleep":
        time.sleep(0.5)
        raise SystemExit(0)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test-run-dir", required=True, type=Path)
    args = parser.parse_args()
    root = args.self_test_run_dir.resolve(strict=True)
    if not root.is_dir():
        raise SystemExit("fixture directory must exist")
    FIXTURE_ROOT = root
    unittest.main(argv=[sys.argv[0]], verbosity=2)


if __name__ == "__main__":
    main()
