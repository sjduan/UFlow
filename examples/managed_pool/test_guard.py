"""Actual A metadata/FD and explicitly synthetic B235 offline guard fixtures."""
import copy
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch
import uuid

import host_guard as guard

FIXTURE = json.loads((Path(__file__).parent / 'fixtures/a-kernel-and-fds.json').read_text())


class RoleGuardTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(os.environ['TMPDIR']) / ('guard-fixture-' + uuid.uuid4().hex)
        self.root.mkdir()
        self.addCleanup(self.cleanup_fixture)
        self.fence = str((self.root / 'api_fence.so').resolve())
        for context in (patch.dict(os.environ, UFLOW_POOL_TEST_FENCE=self.fence,
                                   UFLOW_POOL_TEST_INVENTORY_ROOT=str(self.root)),
                        patch.object(guard, 'sha', return_value='fence-hash'),
                        patch.object(guard, '_host_identity', None)):
            context.start()
            self.addCleanup(context.stop)

    def cleanup_fixture(self):
        (self.root / 'fixture-provider-123.json').unlink(missing_ok=True)
        self.root.rmdir()

    def value(self, daemon=False):
        kernel = copy.deepcopy(FIXTURE['kernel'])
        kernel['source_kind'] = FIXTURE['source_kind']
        return {'pid': 123, 'LD_PRELOAD': self.fence, 'exe_sha256': 'exe',
                'fds': copy.deepcopy(FIXTURE['daemon_fds'] if daemon else FIXTURE['provider_fds']),
                'device_calibration': {'before': kernel, 'after': copy.deepcopy(kernel)},
                'loaded_hashes': {self.fence: 'fence-hash', '/fixed/libuf_acl_shim.so': guard.SHIM_SHA,
                                  '/fixed/_task_interface.so': guard.TASK_SHA}}

    def both(self, value, mutate):
        for raw in value['device_calibration'].values():
            mutate(raw)

    def test_actual_A_kernel_and_original_FD_positive_and_role_separation(self):
        value = self.value(True)
        self.assertEqual(value['device_calibration']['before']['nodes']['/dev/davinci_manager']['rdev'], [234, 0])
        guard.judge(value, 'daemon')
        guard.judge(self.value(), 'provider')
        for role in ('provider', 'client', 'child', 'tracker'):
            with self.subTest(role=role), self.assertRaises(AssertionError):
                guard.judge(value, role)

    def test_explicit_synthetic_B235_layout_not_live_B(self):
        value = self.value(True)
        def synthetic(raw):
            raw['source_kind'] = 'offline-synthetic B235 compatibility; no live B sampling'
            raw['proc_devices'] = raw['proc_devices'].replace('234 hisi_davinci_intf', '235 hisi_davinci_intf').replace('235 ipmidev', '234 ipmidev')
            for path, node in raw['nodes'].items():
                node['st_dev'], node['st_ino'] = 25, 1001 if path.endswith('manager') else 1002
                if path.endswith('manager'):
                    node['rdev'], node['sysfs_path'] = [235, 0], '/sys/dev/char/235:0'
                    node['sysfs_uevent'] = node['sysfs_uevent'].replace('MAJOR=234', 'MAJOR=235')
        self.both(value, synthetic)
        for fd in value['fds'].values():
            if fd['target'] in guard._CONTROL_NODES:
                fd['dev'], fd['inode'] = 25, 1001 if fd['target'].endswith('manager') else 1002
                if fd['target'].endswith('manager'):
                    fd['rdev'] = [235, 0]
        guard.judge(value, 'daemon')

    def test_missing_wrong_driver_type_path_or_sysfs_rejected(self):
        changes = [lambda r: r['nodes'].pop('/dev/davinci_manager'),
                   lambda r: r.update(proc_devices=r['proc_devices'].replace('234 hisi_davinci_intf', '234 ipmidev')),
                   lambda r: r['nodes']['/dev/davinci_manager'].update(is_char=False),
                   lambda r: r['nodes']['/dev/davinci_manager'].update(is_symlink=True),
                   lambda r: r['nodes']['/dev/davinci_manager'].update(canonical_path='/dev/alias'),
                   lambda r: r['nodes']['/dev/davinci_manager'].update(sysfs_exists=False),
                   lambda r: r['nodes']['/dev/davinci_manager'].update(sysfs_realpath='/sys/devices/other'),
                   lambda r: r['nodes']['/dev/davinci_manager'].update(sysfs_uevent='MAJOR=235\nMINOR=0\nDEVNAME=davinci_manager\n'),
                   lambda r: r['nodes']['/dev/hisi_hdc'].update(rdev=[509, 1]),
                   lambda r: r.update(errors=['fixture missing-node OSError'])]
        for index, change in enumerate(changes):
            value = self.value(True)
            self.both(value, change)
            with self.subTest(index=index), self.assertRaises(AssertionError):
                guard.judge(value, 'daemon')

    def test_exact_FD_identity_count_access_and_flags_required(self):
        changes = [lambda v: v['fds'].pop('3'),
                   lambda v: v['fds'].update(extra=copy.deepcopy(v['fds']['3'])),
                   lambda v: v['fds']['3'].update(inode=999), lambda v: v['fds']['3'].update(dev=999),
                   lambda v: v['fds']['3'].update(rdev=[235, 0]),
                   lambda v: v['fds']['3'].update(target='/dev/manager-alias'),
                   lambda v: v['fds']['3'].update(mode=0o100666),
                   lambda v: v['fds']['3'].update(flags=655360),
                   lambda v: v['fds']['6'].update(flags=131074),
                   lambda v: v['fds']['3'].update(inheritable=True),
                   lambda v: v['fds']['3'].update(cloexec=False)]
        for index, change in enumerate(changes):
            value = self.value(True)
            change(value)
            with self.subTest(index=index), self.assertRaises(AssertionError):
                guard.judge(value, 'daemon')

    def test_all_registered_Ascend_major_aliases_rejected_for_every_role(self):
        for major in (234, 508, 509, 510, 511):
            for role in ('daemon', 'provider', 'client', 'child', 'tracker'):
                value = self.value(role == 'daemon')
                fd = copy.deepcopy(FIXTURE['daemon_fds']['6'])
                fd.update(target='/dev/unexpected-alias', rdev=[major, 0])
                value['fds']['alias'] = fd
                with self.subTest(major=major, role=role), self.assertRaises(AssertionError):
                    guard.judge(value, role)

    def test_A_ipmidev235_not_falsely_classified_as_Ascend(self):
        value = self.value()
        fd = copy.deepcopy(FIXTURE['daemon_fds']['6'])
        fd.update(target='/dev/ipmi0', rdev=[235, 0])
        value['fds']['ipmi'] = fd
        guard.judge(value, 'provider')

    def test_related_identity_change_during_or_between_inventories_rejected(self):
        value = self.value(True)
        value['device_calibration']['after']['nodes']['/dev/hisi_hdc']['st_ino'] += 1
        with self.assertRaises(AssertionError):
            guard.judge(value, 'daemon')
        guard.judge(self.value(), 'provider')
        value = self.value()
        self.both(value, lambda r: r['nodes']['/dev/hisi_hdc'].update(st_ino=999))
        with self.assertRaises(AssertionError):
            guard.judge(value, 'provider')

    def test_failed_calibration_and_original_FDs_saved_before_judge(self):
        value = self.value(True)
        value['device_calibration']['before']['nodes'].pop('/dev/hisi_hdc')
        with patch.object(guard, 'inventory', return_value=value), self.assertRaises(AssertionError):
            guard.save_guard(123, 'provider', 'fixture')
        self.assertEqual(json.loads((self.root / 'fixture-provider-123.json').read_text()), value)


if __name__ == '__main__':
    unittest.main()
