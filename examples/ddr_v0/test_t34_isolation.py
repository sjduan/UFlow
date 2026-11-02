"""Read-only guard-order regression for the new public harness."""
import os
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import t34_public_probe as probe


class PublicIsolationTests(unittest.TestCase):
    def test_public_inner_rejects_outer_before_paths_mount_or_bootstrap(self):
        fd = os.open('/proc/self/ns/mnt', os.O_RDONLY | os.O_CLOEXEC)
        try:
            args = SimpleNamespace(outer_mnt_fd=fd, outer_pid=os.getppid(), inside=None)
            with patch.object(probe.Path, 'resolve', side_effect=AssertionError('path lookup')), \
                 patch.object(probe.subprocess, 'run', side_effect=AssertionError('mount/bootstrap')):
                with self.assertRaises(RuntimeError):
                    probe.inner(args)
        finally:
            os.close(fd)


if __name__ == '__main__':
    unittest.main(verbosity=2)
