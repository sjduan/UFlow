#!/usr/bin/env python3
"""Isolated real-process checks for the v1 witness evidence writer.

The tests use their own run directory and do not touch any authority root.
They are not production BIND/SEAL or recovery acceptance tests.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import time
import unittest

import t3_deployment_witness as witness_module

ROOT: Path | None = None
PYTHON = os.path.realpath(sys.executable)


class WitnessTests(unittest.TestCase):
    def setUp(self) -> None:
        assert ROOT is not None
        self.directory = Path(tempfile.mkdtemp(prefix="writer-", dir=ROOT))
        self.witness = witness_module.Witness(self.directory, "real-writer", "real-broker")

    def tearDown(self) -> None:
        self.witness.close()

    def spawn_code(self, code: str, *args: str) -> int:
        return self.witness.spawn_gated("broker", [PYTHON, "-c", code, *args], {})

    def events(self) -> list[dict]:
        return [json.loads(line)["event"] for line in
                (self.directory / "witness.journal").read_bytes().splitlines()]

    def test_closed_gate_incomplete_seal_and_real_normal_wait(self) -> None:
        marker = self.directory / "child-ran"
        pid = self.spawn_code("import pathlib,sys;pathlib.Path(sys.argv[1]).write_text('ran')", str(marker))
        self.assertFalse(marker.exists())
        with self.assertRaises(RuntimeError):
            self.witness.seal()
        self.witness.release_gate(pid)
        self.assertEqual(self.witness.wait(pid, 5), 0)
        self.assertTrue(marker.exists())
        self.witness.seal()
        events = self.events()
        self.assertEqual([event["type"] for event in events],
                         ["witness_start", "child_birth", "child_wait", "witness_seal"])
        self.assertEqual(events[2]["raw_wait_status"], 0)
        self.assertTrue(events[2]["normal_exit_zero"])
        for name in ("witness.journal", "witness.head"):
            self.assertEqual(stat.S_IMODE((self.directory / name).stat().st_mode), 0o600)

    def test_gate_does_not_inject_interpreter_locale_into_child_environment(self) -> None:
        pid = self.spawn_code("import pathlib;assert pathlib.Path('/proc/self/environ').read_bytes()==b''")
        self.witness.release_gate(pid)
        self.assertEqual(self.witness.wait(pid, 5), 0)
        self.witness.seal()

    def test_real_child_backing_and_witness_absence_are_observed_before_wait(self) -> None:
        script = str(Path(witness_module.__file__).resolve())
        pid = self.witness.spawn_gated("broker", [PYTHON, script, "_resource-child", str(self.directory)], {})
        finish = self.directory / "test-resource-finish"
        try:
            self.witness.release_gate(pid)
            info = self.directory / "test-resource"
            deadline = time.monotonic() + 5
            while not info.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            dev, ino = (int(part) for part in info.read_text().split())
            self.witness.record_child_backing(pid, dev, ino)
            self.witness.record_no_backing(dev, ino)
            finish.write_bytes(b"finish\n")
            self.assertEqual(self.witness.wait(pid, 5), 0)
            self.witness.seal()
            events = self.events()
            self.assertEqual([item["type"] for item in events],
                             ["witness_start", "child_birth", "child_resource_observed",
                              "witness_no_backing", "child_wait", "witness_seal"])
            self.assertTrue(events[2]["fd_observed"])
            self.assertTrue(events[2]["mapping_observed"])
            self.assertEqual((events[2]["dev"], events[2]["ino"]), (dev, ino))
            self.assertEqual((events[3]["dev"], events[3]["ino"]), (dev, ino))
        finally:
            finish.write_bytes(b"finish\n")
            self.witness._children[pid]["process"].wait(timeout=10)

    def test_witness_held_backing_cannot_be_reported_absent(self) -> None:
        fd = getattr(os, "memfd_create")("witness-held-negative", getattr(os, "MFD_CLOEXEC"))
        try:
            info = os.fstat(fd)
            with self.assertRaises(RuntimeError):
                self.witness.record_no_backing(info.st_dev, info.st_ino)
            with self.assertRaises(ValueError):
                self.spawn_code("pass")
            self.assertEqual([item["type"] for item in self.events()], ["witness_start"])
        finally:
            os.close(fd)

    def test_authority_fd_is_not_a_child_launch_option_and_evidence_stays_private(self) -> None:
        backing = getattr(os, "memfd_create")("wrong-inherited-fd", getattr(os, "MFD_CLOEXEC"))
        lock_path = self.directory / "fixture.lock"
        lock = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
        command = [PYTHON, "-c", "pass"]
        try:
            with self.assertRaises(TypeError):
                self.witness.spawn_gated("broker", command, {}, authority_fd=backing)
            with self.assertRaises(ValueError):
                self.witness.spawn_gated("broker", command, {}, control_fd=lock)
            with self.assertRaises(TypeError):
                self.witness.spawn_gated("broker", command, {}, authority_fd=lock)
            for evidence_fd in (self.witness._journal, self.witness._head_fd):
                with self.assertRaises(TypeError):
                    self.witness.spawn_gated("broker", command, {}, authority_fd=evidence_fd)
                with self.assertRaises(TypeError):
                    self.witness.spawn_gated("broker", command, {}, stderr=evidence_fd)
            # Reopening the same inode yields another fd number; denial must
            # follow the pinned identity, not only the original descriptor.
            alias = os.open(self.witness._journal_path, os.O_RDWR | getattr(os, "O_CLOEXEC"))
            try:
                self.assertNotEqual(alias, self.witness._journal)
                with self.assertRaises(TypeError):
                    self.witness.spawn_gated("broker", command, {}, authority_fd=alias)
                with self.assertRaises(TypeError):
                    self.witness.spawn_gated("broker", command, {}, stderr=alias)
            finally:
                os.close(alias)
            self.assertEqual([item["type"] for item in self.events()], ["witness_start"])
        finally:
            os.close(backing)
            os.close(lock)

    def test_actual_nonzero_and_signal_exits_cannot_seal(self) -> None:
        pid = self.spawn_code("import sys;sys.exit(7)")
        self.witness.release_gate(pid)
        status = self.witness.wait(pid, 5)
        self.assertTrue(os.WIFEXITED(status))
        self.assertEqual(os.WEXITSTATUS(status), 7)
        self.assertFalse(self.events()[-1]["normal_exit_zero"])
        with self.assertRaises(RuntimeError):
            self.witness.seal()

        other = Path(tempfile.mkdtemp(prefix="signal-", dir=ROOT))
        w = witness_module.Witness(other, "signal-writer", "signal-broker")
        try:
            pid = w.spawn_gated("broker", [PYTHON, "-c", "import os,signal;os.kill(os.getpid(),signal.SIGTERM)"], {})
            w.release_gate(pid)
            status = w.wait(pid, 5)
            self.assertTrue(os.WIFSIGNALED(status))
            with self.assertRaises(RuntimeError):
                w.seal()
        finally:
            w.close()

    def test_echild_cannot_be_invented_into_normal_wait(self) -> None:
        pid = self.spawn_code("pass")
        self.witness.release_gate(pid)
        waited, status = os.waitpid(pid, 0)
        self.assertEqual(waited, pid)
        self.assertEqual(status, 0)
        self.witness._children[pid]["process"].returncode = 0
        with self.assertRaises(ChildProcessError):
            self.witness.wait(pid, 5)
        with self.assertRaises(RuntimeError):
            self.witness.seal()
        self.assertNotEqual(self.events()[-1]["type"], "witness_seal")

    def test_same_inode_same_length_corruption_poison_before_release(self) -> None:
        path = self.directory / "witness.journal"
        before = path.stat().st_ino
        data = bytearray(path.read_bytes())
        data[0] ^= 1
        path.write_bytes(data)
        self.assertEqual(path.stat().st_ino, before)
        marker = self.directory / "should-not-run"
        with self.assertRaises(Exception):
            self.spawn_code("import pathlib,sys;pathlib.Path(sys.argv[1]).write_text('ran')", str(marker))
        self.assertFalse(marker.exists())
        with self.assertRaises(RuntimeError):
            self.witness.seal()

    def test_authority_like_flock_fd_not_inherited_and_contender_blocked(self) -> None:
        lock_path = self.directory / "unrelated-test.lock"
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            info = os.fstat(fd)
            code = (
                "import os,fcntl,sys;fd=int(sys.argv[1]);d=int(sys.argv[2]);i=int(sys.argv[3]);"
                "p=sys.argv[4];"
                "\ntry:\n s=os.fstat(fd); assert (s.st_dev,s.st_ino)!=(d,i)\n"
                "except OSError: pass\n"
                "f=os.open(p,os.O_RDONLY|os.O_CLOEXEC)\n"
                "try:\n fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)\n"
                "except BlockingIOError: pass\n"
                "else: raise RuntimeError('lock was not exclusive')\n"
                "os.close(f)\n"
            )
            pid = self.spawn_code(code, str(fd), str(info.st_dev), str(info.st_ino), str(lock_path))
            self.witness.release_gate(pid)
            self.assertEqual(self.witness.wait(pid, 5), 0)
            self.witness.seal()
        finally:
            os.close(fd)


def main() -> None:
    global ROOT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    args = parser.parse_args()
    path = args.run_dir.resolve(strict=True)
    if path != args.run_dir or not path.is_relative_to(Path("/workspace/uflow/runs")):
        raise ValueError("test root must be an existing canonical run-owned directory")
    ROOT = path
    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(WitnessTests))
    if not result.wasSuccessful():
        raise SystemExit(1)


if __name__ == "__main__":
    main()
