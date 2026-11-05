"""Portable regression fixtures for role separation and failed-check evidence."""
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch
import uuid

import host_guard as guard


class RoleGuardTests(unittest.TestCase):
    def setUp(self):
        # mkdir's default mode avoids Python 3.13's Windows 0700 DACL behavior.
        # Remove only these fixture-owned paths; never recursively scan/delete.
        self.root = Path(os.environ['TMPDIR']) / ('guard-fixture-' + uuid.uuid4().hex)
        self.root.mkdir()
        self.addCleanup(self.cleanup_fixture)
        self.fence = str((self.root / 'api_fence.so').resolve())
        self.env = patch.dict(os.environ, UFLOW_POOL_TEST_FENCE=self.fence,
                              UFLOW_POOL_TEST_INVENTORY_ROOT=str(self.root))
        self.env.start()
        self.addCleanup(self.env.stop)
        self.hash = patch.object(guard, 'sha', return_value='fence-hash')
        self.hash.start()
        self.addCleanup(self.hash.stop)

    def cleanup_fixture(self):
        (self.root / 'fixture-provider-123.json').unlink(missing_ok=True)
        self.root.rmdir()

    def value(self):
        return {'pid': 123, 'LD_PRELOAD': self.fence, 'exe_sha256': 'exe', 'fds': {},
                'loaded_hashes': {self.fence: 'fence-hash', '/fixed/libuf_acl_shim.so': guard.SHIM_SHA,
                                  '/fixed/_task_interface.so': guard.TASK_SHA}}

    @staticmethod
    def fd(target, major, inherit=False, char=True):
        return {'target': target, 'char': char, 'rdev': [major, 0], 'inheritable': inherit}

    def test_only_daemon_accepts_exact_control_baseline(self):
        value = self.value()
        value['fds'] = {str(i): self.fd('/dev/davinci_manager', 235) for i in range(3)}
        value['fds']['8'] = self.fd('/dev/hisi_hdc', 509, True)
        guard.judge(value, 'daemon')
        for role in ('provider', 'client', 'child', 'tracker'):
            with self.subTest(role=role), self.assertRaises(AssertionError):
                guard.judge(value, role)
        value['fds'].pop('0')
        with self.assertRaises(AssertionError):
            guard.judge(value, 'daemon')

    def test_provider_single_inherited_hdc_is_rejected(self):
        value = self.value()
        guard.judge(value, 'provider')
        value['fds']['6'] = self.fd('/dev/hisi_hdc', 509, True)
        with self.assertRaises(AssertionError):
            guard.judge(value, 'provider')

    def test_alias_numbered_device_or_wrong_flags_do_not_match_baseline(self):
        for target, major in (('/dev/davinci0', 236), ('/dev/alias', 235), ('/dev/devmm_svm', 1)):
            value = self.value()
            value['fds']['7'] = self.fd(target, major)
            with self.subTest(target=target), self.assertRaises(AssertionError):
                guard.judge(value, 'client')
        value = self.value()
        value['LD_PRELOAD'] = ''
        with self.assertRaises(AssertionError):
            guard.judge(value, 'client')

    def test_inventory_is_saved_before_provider_rejection(self):
        value = self.value()
        value['fds']['6'] = self.fd('/dev/hisi_hdc', 509, True)
        with patch.object(guard, 'inventory', return_value=value), self.assertRaises(AssertionError):
            guard.save_guard(123, 'provider', 'fixture')
        self.assertEqual(json.loads((self.root / 'fixture-provider-123.json').read_text()), value)


if __name__ == '__main__':
    unittest.main()
