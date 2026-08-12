#!/usr/bin/env python3
from __future__ import annotations

import os
import unittest
from pathlib import Path

from tools.phasee06.check_remote_cleanup import ancestor_pids, process_command_lines


class CheckRemoteCleanupTests(unittest.TestCase):
    def test_ancestor_chain_contains_self_and_parent(self) -> None:
        ancestors = ancestor_pids()
        self.assertIn(os.getpid(), ancestors)
        if os.getppid() > 0 and Path(f"/proc/{os.getpid()}/status").exists():
            self.assertIn(os.getppid(), ancestors)

    def test_process_scan_excludes_ancestor_chain(self) -> None:
        ancestors = ancestor_pids()
        scanned_pids = {pid for pid, _command in process_command_lines(ancestors)}
        self.assertTrue(ancestors.isdisjoint(scanned_pids))


if __name__ == "__main__":
    unittest.main()
