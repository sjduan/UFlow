#!/usr/bin/env python3
"""Read-only isolation regressions; no mount or authority bootstrap is allowed.

Run with Python on Linux. The real direct-docker-exec and shell-parent lifecycle
commands are documented in t3_normal_authority.md; these tests must run first.
"""
from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest import mock

import t3_normal_authority_probe as probe


def namespace_stat(inode: int) -> SimpleNamespace:
    return SimpleNamespace(st_dev=4, st_ino=inode)


class IsolationGuardTests(unittest.TestCase):
    def test_real_outer_is_rejected_without_any_side_effect(self) -> None:
        before = Path("/proc/self/mountinfo").read_bytes()
        workspace = probe.identity(Path("/workspace/uflow"))
        fd = os.open("/proc/self/ns/mnt", os.O_RDONLY | os.O_CLOEXEC)
        try:
            with mock.patch.object(probe.subprocess, "run", side_effect=AssertionError("no subprocess")), \
                    mock.patch.object(probe.subprocess, "Popen", side_effect=AssertionError("no worker")):
                with self.assertRaisesRegex(RuntimeError, "mount namespace was not isolated"):
                    probe.require_isolated_namespace(fd, os.getpid())
            self.assertEqual(Path("/proc/self/mountinfo").read_bytes(), before)
            self.assertEqual(probe.identity(Path("/workspace/uflow")), workspace)
        finally:
            os.close(fd)

    def check_guard(self, *, current: int = 20, outer: int = 10, ppid: int = 123,
                    mountinfo: str = "1 0 0:1 / / rw - tmpfs tmpfs rw\n",
                    link: str = "mnt:[10]") -> int:
        with mock.patch.object(probe.os, "readlink", return_value=link), \
                mock.patch.object(probe.os, "fstat", return_value=namespace_stat(10)), \
                mock.patch.object(probe.os, "stat", side_effect=[namespace_stat(current), namespace_stat(outer)]), \
                mock.patch.object(probe.os, "getppid", return_value=ppid), \
                mock.patch.object(probe.Path, "read_text", return_value=mountinfo), \
                mock.patch.object(probe.subprocess, "run", side_effect=AssertionError("no mount/bootstrap")), \
                mock.patch.object(probe.subprocess, "Popen", side_effect=AssertionError("no child")):
            return probe.require_isolated_namespace(7, 123)

    def test_explicit_child_with_private_mounts_passes_read_only_guard(self) -> None:
        self.assertEqual(self.check_guard(), 10)

    def test_same_namespace_refused_even_when_parent_matches(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "mount namespace was not isolated"):
            self.check_guard(current=10)

    def test_outer_namespace_pin_mismatch_refused(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "mount namespace was not isolated"):
            self.check_guard(outer=11)

    def test_unrelated_parent_refused(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "mount namespace was not isolated"):
            self.check_guard(ppid=124)

    def test_non_mount_descriptor_refused(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "outer mount namespace descriptor required"):
            self.check_guard(link="/some/directory")

    def test_nonprivate_propagation_refused(self) -> None:
        for tag in ("shared:1", "master:1", "propagate_from:1"):
            with self.subTest(tag=tag), self.assertRaisesRegex(RuntimeError, "mount propagation is not private"):
                self.check_guard(mountinfo=f"1 0 0:1 / / rw {tag} - tmpfs tmpfs rw\n")

    def test_inner_guard_precedes_paths_mount_and_bootstrap(self) -> None:
        # This is not an outer invocation of a mount-capable inner negative:
        # replace the guard with an unconditional exception and forbid all I/O.
        with mock.patch.object(probe, "require_isolated_namespace", side_effect=RuntimeError("reject before I/O")), \
                mock.patch.object(probe.Path, "resolve", side_effect=AssertionError("no source lookup")) as resolve, \
                mock.patch.object(probe.subprocess, "run", side_effect=AssertionError("no mount/bootstrap")) as run, \
                mock.patch.object(probe.subprocess, "Popen", side_effect=AssertionError("no child")) as popen:
            with self.assertRaisesRegex(RuntimeError, "reject before I/O"):
                probe.inner(Path("/not-opened"), -1, -1, -1, "not-used", False, None)
            resolve.assert_not_called()
            run.assert_not_called()
            popen.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
