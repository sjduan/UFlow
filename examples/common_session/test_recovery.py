"""CPU-only metadata recovery regressions; Linux tests use real FLOCK/socket FDs."""
from contextlib import contextmanager
import copy
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

import session_adopt as adopt
import session_owner
from session_owner import Owner, birth
from session_protocol import GROUP, POOL_BYTES, request, save


def sample_manifest():
    value = {k: {} for k in adopt.MANIFEST_KEYS}
    value.update(schema=1, group=GROUP, manifest_version=1, state='ready', host_alias='serving-b-sj',
        container='openeuler-2403-DS', partitions={'O1': [0], 'M1': [1, 2, 3, 4]},
        pool_descriptor={'pool_id': 'original', 'pool_epoch': 'epoch', 'provider_session': 'provider',
                         'nbytes': POOL_BYTES, 'canonical': {'body_hex': b'original'.hex(), 'dev': 1, 'inode': 2}},
        owner_births={'101': 1001, '102': 1002, '103': 1003},
        controller_birth={'pid': 100, 'starttime_ticks': 1000},
        provider_loaded={'pid': 102, 'parent_pid': 101}, bootstrap_client='bootstrap')
    return value


def sample_snapshot(manifest):
    lines = {'O1': {'clients': [], 'retired': False},
             'M1': {'clients': [f'old-{i}' for i in range(16)], 'retired': False}}
    pool = {**manifest['pool_descriptor'], 'state': 'Allocated', 'frozen': False,
            'clients': [{'client_session': s, 'state': 'Unmapped', 'children': []}
                        for s in ['bootstrap', *lines['M1']['clients']]], 'ranges': {'leases': []}}
    return {'budget_bytes': POOL_BYTES, 'charged_bytes': POOL_BYTES, 'creation_reserved_bytes': 0,
            'operation_records': 312, 'pools': [pool]}, lines


@contextmanager
def adoption_fixture():
    with tempfile.TemporaryDirectory(prefix='uflow-metadata-test-') as temp:
        public = Path(temp)
        manifest = sample_manifest()
        old = public / 'session002'
        (old / 'state').mkdir(parents=True)
        secret = old / 'state/admin.token'
        secret.write_text('SYNTHETIC_TEST_ONLY_CAPABILITY')
        secret.chmod(0o600)
        manifest.update(owner_root=str(old), endpoint=str(public / 'u.sock'),
                        control_endpoint=str(public / 'control.sock'))
        save(public / 'manifest.json', manifest)
        snapshot, lines = sample_snapshot(manifest)
        capsule = {'schema': 1, 'group': GROUP, 'mode': 'adopt-existing', 'decision_id': 'synthetic-host-test',
            'old_manifest_sha256': adopt.sha(public / 'manifest.json'),
            'old_controller_birth': manifest['controller_birth'], 'original_owner_births': manifest['owner_births'],
            'old_control_identity': {'dev': 1, 'inode': 3, 'uid': os.geteuid(), 'mode': 0o140600},
            'original_snapshot': snapshot, 'lines': lines,
            'reservations': {'schema': 1, 'group': GROUP, 'reservations': []},
            'new_owner_root': str(public / 'metadata-synthetic'), 'helper_commit': '1' * 40,
            'helper_files': {n: adopt.sha(Path(adopt.__file__).parent / n) for n in adopt.HELPER_FILES},
            'parent_safety': {}, 'closed_tasks': [{'pid': p, 'birth': 1} for p in (200, 201, 202)]}
        path = public / 'capsule.json'
        class RecordedSystem:
            def __init__(self):
                self.calls = []
                self.value = copy.deepcopy(snapshot)

            def verify(self, *args):
                self.calls.append('verify')

            def snapshot(self, *args):
                self.calls.append('GetManagedPoolSnapshot')
                return copy.deepcopy(self.value)

        system = RecordedSystem()
        def run():
            save(path, capsule, exclusive=False)
            return adopt.AdoptedOwner.adopt(path, adopt.sha(path), system=system)
        with patch.object(adopt, 'PUBLIC', public), patch.object(adopt, 'OLD_MANIFEST_SHA', capsule['old_manifest_sha256']):
            yield SimpleNamespace(public=public, manifest=manifest, capsule=capsule, system=system, run=run, path=path)


class OwnerTreeTests(unittest.TestCase):
    """Recorded original bootstrap ancestry, never actual UF service creation."""

    @contextmanager
    def tree(self, *, adopted=False, tracker=True):
        daemon, provider, helper = 927616, 927677, 928450
        births = {daemon: 1296287235, provider: 1296287237, helper: 1296287255}
        rows = {daemon: {'children': [provider], 'exe_sha256': session_owner.DAEMON_SHA},
                provider: {'children': [helper] if tracker else [], 'exe_sha256': session_owner.PYTHON_SHA},
                helper: {'children': [], 'exe_sha256': session_owner.PYTHON_SHA}}
        owner = Owner.__new__(Owner)
        if adopted:
            owner.original_daemon_pid = daemon
            owner.owners = dict(births)
        else:
            owner.daemon = SimpleNamespace(pid=daemon)
            owner.owners = {}
        owner.evidence = lambda *args: None
        fake_guard = ModuleType('host_guard')
        fake_guard.inventory = lambda pid: copy.deepcopy(rows[pid])
        fake_guard.judge = lambda value, role: None
        with tempfile.TemporaryDirectory(prefix='uflow-owner-tree-') as temp:
            symbols = Path(temp) / 'symbols.json'
            names = [f'synthetic_fence_symbol_{n}' for n in range(79)]
            symbols.write_text(json.dumps(names))
            owner.loaded = {'pid': provider, 'test_fence_bindings': {n: 'synthetic-fence' for n in names}}
            with patch.dict(sys.modules, {'host_guard': fake_guard}), \
                    patch.dict(os.environ, {'UFLOW_POOL_TEST_SYMBOLS': str(symbols), 'UFLOW_POOL_TEST_FENCE': 'synthetic-fence'}), \
                    patch.object(session_owner, 'birth', side_effect=lambda pid: births.get(pid)):
                yield owner, rows, births

    def test_original_bootstrap_allows_recorded_two_to_three_and_keeps_existing_birth_pins(self):
        with self.tree(tracker=False) as (owner, rows, births):
            owner.verify_owners('before-create-recorded')
            self.assertEqual(owner.owners, {927616: 1296287235, 927677: 1296287237})
            rows[927677]['children'] = [928450]
            owner.verify_owners('allocated-recorded')
            self.assertEqual(owner.owners, births)
            births[927677] += 1
            with self.assertRaisesRegex(ValueError, 'birth changed'):
                owner.verify_owners('existing-birth-drift')

    def test_adopted_original_three_cannot_add_remove_or_change_birth(self):
        for case in ('unchanged', 'added', 'missing', 'birth-drift'):
            with self.subTest(case=case), self.tree(adopted=True) as (owner, rows, births):
                original = dict(owner.owners)
                if case == 'added':
                    rows[927616]['children'].append(999001)
                    rows[999001] = {'children': [], 'exe_sha256': session_owner.PYTHON_SHA}
                    births[999001] = 999002
                elif case == 'missing':
                    rows[927677]['children'] = []
                elif case == 'birth-drift':
                    births[928450] += 1
                if case == 'unchanged':
                    owner.verify_owners('adopted-recorded')
                else:
                    with self.assertRaises(ValueError): owner.verify_owners('adopted-drift')
                self.assertEqual(owner.owners, original)


class AdoptionTests(unittest.TestCase):
    def test_adopt_preserves_all_histories_and_forbids_constructor_bootstrap_and_Popen(self):
        with adoption_fixture() as f, patch.object(Owner, '__init__', side_effect=AssertionError('constructor')), \
                patch.object(Owner, 'bootstrap', side_effect=AssertionError('bootstrap')), \
                patch.object(subprocess, 'Popen', side_effect=AssertionError('new UF service')):
            original = (f.public / 'manifest.json').read_bytes()
            owner = f.run()
            self.assertEqual(f.system.calls, ['verify', 'GetManagedPoolSnapshot'])
            self.assertEqual(owner.lines, f.capsule['lines'])
            self.assertEqual(owner.owners, {101: 1001, 102: 1002, 103: 1003})
            self.assertFalse(hasattr(owner, 'daemon'))
            self.assertEqual(set(owner.manifest), adopt.MANIFEST_KEYS)
            for key in adopt.MANIFEST_KEYS - {'manifest_version', 'owner_root', 'helper_root', 'controller_birth', 'frozen_inputs'}:
                self.assertEqual(owner.manifest[key], f.manifest[key])
            self.assertEqual(owner.manifest['manifest_version'], 2)
            self.assertEqual((f.public / 'manifest.json').read_bytes(), original)
            self.assertFalse((f.public / 'manifest-v2.json').exists())
            self.assertNotIn(owner._admin, json.dumps(owner.manifest))
            self.assertEqual(list(owner.root.iterdir()), [])

    def test_capsule_and_catalog_drift_rejected_before_new_root(self):
        for case in ('line-missing', 'retired', 'missing-client', 'mapped', 'child', 'range', 'counter',
                     'new-pool', 'reservation', 'helper', 'owner', 'unknown-field'):
            with self.subTest(case=case), adoption_fixture() as f:
                c = f.capsule
                if case == 'line-missing': c['lines']['M1']['clients'].pop()
                elif case == 'retired': c['lines']['O1']['retired'] = True
                elif case == 'missing-client': c['original_snapshot']['pools'][0]['clients'].pop()
                elif case == 'mapped': c['original_snapshot']['pools'][0]['clients'][0]['state'] = 'Mapped'
                elif case == 'child': c['original_snapshot']['pools'][0]['clients'][0]['children'] = [{'unknown': False}]
                elif case == 'range': c['original_snapshot']['pools'][0]['ranges']['leases'] = [{'client_session': 'bootstrap', 'state': 'Granted'}]
                elif case == 'counter': f.system.value['operation_records'] += 1
                elif case == 'new-pool': f.system.value['pools'][0]['pool_epoch'] = 'new'
                elif case == 'reservation': c['reservations']['reservations'] = [{}]
                elif case == 'helper': c['helper_files']['session_owner.py'] = '0' * 64
                elif case == 'owner': c['original_owner_births']['101'] += 1
                else: c['extra'] = 'not accepted'
                with self.assertRaises((ValueError, KeyError)):
                    f.run()
                self.assertFalse(Path(c['new_owner_root']).exists())

    def test_capsule_seal_and_original_manifest_bytes_are_required(self):
        with adoption_fixture() as f:
            save(f.path, f.capsule)
            with self.assertRaises(ValueError):
                adopt.AdoptedOwner.adopt(f.path, '0' * 64, system=f.system)
            (f.public / 'manifest.json').write_bytes(b'{}')
            with self.assertRaises(ValueError): f.run()

    def test_linux_system_rejects_live_old_controller_before_any_snapshot(self):
        m = sample_manifest()
        with patch.object(adopt, 'current_host', return_value=m['host']), \
                patch.object(adopt, 'birth', return_value=1000):
            with self.assertRaisesRegex(ValueError, 'still owns'):
                adopt.ExistingSystem().verify({}, m)

    def test_missing_original_frozen_SDK_is_rejected_without_import_or_UF_request(self):
        m = sample_manifest()
        m['helper_root'] = '/synthetic/pool/examples/common_session'
        with patch.object(adopt, 'current_host', return_value=m['host']), \
                patch.object(adopt, 'birth', return_value=None):
            with self.assertRaisesRegex(ValueError, 'SDK/Python'):
                adopt.ExistingSystem().verify({'closed_tasks': []}, m)

    def test_original_admin_capability_is_only_read_from_private_same_host_state(self):
        with adoption_fixture() as f:
            secret = f.public / 'session002/state/admin.token'
            secret.chmod(0o644)
            with self.assertRaisesRegex(ValueError, 'private admin'):
                f.run()
            self.assertFalse(Path(f.capsule['new_owner_root']).exists())

    def test_frozen_parent_proof_rejects_changed_birth_child_pipe_and_binary(self):
        import session_parent_pins
        m = sample_manifest()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            helper = root / 'examples/common_session'
            helper.mkdir(parents=True)
            m['helper_root'] = str(helper)
            (root / 'proof.txt').write_text('synthetic source pin')
            pins = {'proof.txt': adopt.sha(root / 'proof.txt')}
            rows = {str(p): {'pid': p, 'birth': 900 + p, 'ppid': p - 1,
                    'exe_sha256': session_owner.DAEMON_SHA if p == 101 else session_owner.PYTHON_SHA,
                    'argv': ['binary', '--host-pool'] if p == 101 else ['python'],
                    'children': [p + 1] if p in (101, 102) else [], 'fds': {}}
                    for p in (100, 101, 102, 103)}
            proof = {'source_files': pins, 'original_processes': rows, 'no_controller_parent_guard': True}
            with patch.object(session_parent_pins, 'SOURCE_PINS', pins):
                adopt.check_parent_safety(proof, m, observe=lambda p: copy.deepcopy(rows[str(p)]))
                for case in ('birth', 'binary', 'child', 'pipe', 'parent'):
                    data = copy.deepcopy(proof)
                    live = copy.deepcopy(rows)
                    if case == 'birth': live['101']['birth'] += 1
                    elif case == 'binary': live['102']['exe_sha256'] = 'bad'
                    elif case == 'child': live['103']['children'] = [999]
                    elif case == 'parent': live['102']['ppid'] = 100
                    else:
                        data['original_processes']['100']['fds']['8'] = {'target': 'pipe:[123]'}
                        data['original_processes']['101']['fds']['0'] = {'target': 'pipe:[123]'}
                    with self.subTest(case=case), self.assertRaises(ValueError):
                        adopt.check_parent_safety(data, m, observe=lambda p: live[str(p)])

    @unittest.skipUnless(sys.platform == 'linux', 'Linux Unix peer credentials required')
    def test_future_publication_binds_real_socket_and_describes_actual_v2(self):
        with adoption_fixture() as f:
            previous = Path.cwd()
            os.chdir(f.public)
            try:
                with socket.socket(socket.AF_UNIX) as old:
                    old.bind('control.sock')
                    info = Path('control.sock').lstat()
                    f.capsule['old_control_identity'] = {'dev': info.st_dev, 'inode': info.st_ino,
                                                       'uid': info.st_uid, 'mode': info.st_mode}
                owner = f.run()
                owner.control = 'control.sock'  # Short relative name in this synthetic TMPDIR.
                owner.snapshot = lambda _: (f.system.value, f.system.value['pools'][0])
                owner.finalize = lambda: None
                errors = []
                def run():
                    try: owner.publish_and_serve()
                    except Exception as error: errors.append(repr(error))
                worker = threading.Thread(target=run)
                worker.start()
                try:
                    deadline = time.monotonic() + 3
                    while not (f.public / 'manifest-v2.json').exists():
                        if time.monotonic() > deadline: self.fail(str(errors))
                        time.sleep(.005)
                    result = request({'control_endpoint': 'control.sock'}, 'describe')
                    self.assertEqual(result['manifest'], owner.manifest)
                    self.assertEqual(result['lines'], f.capsule['lines'])
                    self.assertEqual(adopt.read_json(f.public / 'manifest.json'), f.manifest)
                    self.assertEqual(adopt.read_json(f.public / 'manifest-v2.json'), owner.manifest)
                    self.assertEqual(set(result['manifest']), adopt.MANIFEST_KEYS)
                finally:
                    (owner.root / 'finalize.request').touch()
                    worker.join(3)
                self.assertFalse(worker.is_alive())
                self.assertEqual(errors, [])
                self.assertEqual((owner.root / 'previous-control.sock').lstat().st_ino, info.st_ino)
                with self.assertRaises(ValueError): f.run()
            finally:
                os.chdir(previous)

    def test_nonchild_finalizer_uses_real_ACK_contract_and_never_waits_for_fictional_child(self):
        for case in ('success', 'destroy-unknown', 'backing-live', 'shutdown-wrong', 'line-live'):
            with self.subTest(case=case), adoption_fixture() as f:
                owner = f.run()
                calls = []
                owner.snapshot = lambda _: (f.system.value, f.system.value['pools'][0])
                owner.verify_owners = lambda _: calls.append('verify-original-births')
                for line in owner.lines.values(): line['retired'] = True
                if case == 'line-live': owner.lines['M1']['retired'] = False
                def destroy(*args, **kwargs):
                    calls.append('Destroy-ACK')
                    if case == 'destroy-unknown': raise RuntimeError('synthetic Unknown')
                    return {'released': True, 'synthetic': True}
                def closed(*args):
                    calls.append('FD-VMA-closed')
                    if case == 'backing-live': raise RuntimeError('synthetic backing remains')
                    return 'original'
                def shutdown(*args, **kwargs):
                    calls.append('Shutdown-ACK')
                    return {'stopping': case != 'shutdown-wrong'}
                package, api = ModuleType('uflow'), ModuleType('uflow.managed_pool')
                api.destroy_pool, api.shutdown_pool_service = destroy, shutdown
                for forbidden in ('create_pool', 'ManagedPoolClient'):
                    setattr(api, forbidden, lambda *a, **k: (_ for _ in ()).throw(AssertionError('new pool/client')))
                clean = {'charged_bytes': 0, 'creation_reserved_bytes': 0,
                         'normal_shutdown': True, 'provider_exited': True}
                save(owner.state / 'clean.json', clean)
                with patch.dict(sys.modules, {'uflow': package, 'uflow.managed_pool': api}), \
                        patch.object(adopt, 'check_backing_closed', side_effect=closed), \
                        patch.object(adopt, 'birth', return_value=None), \
                        patch.object(subprocess, 'Popen', side_effect=AssertionError('no service start')):
                    if case == 'success':
                        owner.finalize()
                        value = adopt.read_json(f.public / 'finalized.json')
                        self.assertIsNone(value['daemon_exit_code'])
                        self.assertEqual(value['A_retained_historical_bytes'], POOL_BYTES)
                        self.assertEqual(calls, ['verify-original-births', 'Destroy-ACK', 'FD-VMA-closed', 'Shutdown-ACK'])
                    else:
                        with self.assertRaises((ValueError, RuntimeError)): owner.finalize()
                        self.assertFalse((f.public / 'finalized.json').exists())
                        if case in ('line-live', 'destroy-unknown', 'backing-live'):
                            self.assertNotIn('Shutdown-ACK', calls)


@unittest.skipUnless(sys.platform == 'linux', 'real Linux proc FD/FLOCK/Unix peer credentials required')
class LinuxReservationTests(unittest.TestCase):
    def setUp(self):
        import fcntl
        self.temp = tempfile.TemporaryDirectory(prefix='uflow-reserve-test-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.owner = Owner.__new__(Owner)
        self.owner.public, self.owner.root = self.root, self.root
        self.owner.counter = 0
        save(self.root / 'reservations.json', {'schema': 1, 'group': GROUP, 'reservations': []})
        self.lock_path = self.root / 'physical-10.lock'
        self.lock = self.lock_path.open('w+')
        self.addCleanup(self.lock.close)
        fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # Redirect only the test lock name. /proc and real kernel FLOCK remain real.
        original = Path
        def paths(*args):
            p = original(*args)
            return self.lock_path if str(p) == '/workspace/uflow/locks/run02-npu-10.lock' else p
        self.patcher = patch.object(session_owner, 'Path', paths)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)
        self.row = {'physical_device': 10, 'board': 5, 'chip': 0, 'bdf': '0000:89:00.0',
                    'execution_domain': 'synthetic-domain-10', 'lock_path': '/workspace/uflow/locks/run02-npu-10.lock',
                    'lock_holder_pid': os.getpid(), 'lock_holder_birth': birth(os.getpid()),
                    'evidence_path': 'synthetic-host-only-lock-evidence'}

    def test_socket_last_fd_does_not_replace_registry_target(self):
        with socket.socketpair()[0] as _unused:
            # Keep both real endpoints alive; no save spy and no fake ACK.
            with socket.socketpair()[0] as last:
                with self.assertRaises(OSError) as error:
                    Path('/proc', str(os.getpid()), 'fd', str(last.fileno())).open('w')
                self.assertEqual(error.exception.errno, 6)
                self.owner.reserve('O1', [self.row])
                saved = json.loads((self.root / 'reservations.json').read_text())
                self.assertEqual(saved['reservations'], [{'line': 'O1', **self.row}])
                self.assertIsNotNone(last.getsockname())
        self.owner.reserve('O1', [])
        self.assertEqual(json.loads((self.root / 'reservations.json').read_text())['reservations'], [])

    def test_no_lock_wrong_birth_device_overlap_domain_overlap_do_not_write(self):
        import fcntl
        before = (self.root / 'reservations.json').read_bytes()
        changed = {**self.row, 'lock_holder_birth': self.row['lock_holder_birth'] + 1}
        with self.assertRaises(ValueError): self.owner.reserve('O1', [changed])
        fcntl.flock(self.lock, fcntl.LOCK_UN)
        with self.assertRaises(ValueError): self.owner.reserve('O1', [self.row])
        self.assertEqual((self.root / 'reservations.json').read_bytes(), before)
        fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        for row in ({'line': 'M1', **self.row},
                    {'line': 'M1', **self.row, 'physical_device': 12}):
            save(self.root / 'reservations.json', {'schema': 1, 'group': GROUP, 'reservations': [row]}, exclusive=False)
            old = (self.root / 'reservations.json').read_bytes()
            with self.assertRaises(ValueError): self.owner.reserve('O1', [self.row])
            self.assertEqual((self.root / 'reservations.json').read_bytes(), old)

    def test_actual_control_request_socket_roundtrip_and_empty_release(self):
        # Task TMPDIR can exceed AF_UNIX's108-byte limit. This synthetic socket
        # is relative to its own temporary directory, never the live p/.
        previous = Path.cwd()
        os.chdir(self.root)
        self.addCleanup(os.chdir, previous)
        self.owner.control = 'control.sock'
        self.owner.lines = {'O1': {'clients': [], 'retired': False}, 'M1': {'clients': [], 'retired': False}}
        self.owner.snapshot = lambda _: ({'charged_bytes': POOL_BYTES}, {})
        self.owner.finalize = lambda: None  # Stop this synthetic CPU server; never an actual UF finalizer.
        errors = []
        def serve():
            try: self.owner.serve()
            except Exception as error: errors.append(repr(error))
        thread = threading.Thread(target=serve)
        thread.start()
        try:
            deadline = time.monotonic() + 3
            while not Path(self.owner.control).exists():
                if time.monotonic() > deadline: self.fail('synthetic server did not bind')
                time.sleep(.005)
            manifest = {'control_endpoint': self.owner.control}
            result = request(manifest, 'reserve-devices', line='O1', reservations=[self.row])
            self.assertIs(result['pool_destroyed'], False)
            self.assertEqual(len(json.loads((self.root / 'reservations.json').read_text())['reservations']), 1)
            request(manifest, 'reserve-devices', line='O1', reservations=[])
            self.assertEqual(json.loads((self.root / 'reservations.json').read_text())['reservations'], [])
        finally:
            (self.root / 'finalize.request').touch()
            thread.join(timeout=3)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertFalse(Path(self.owner.control).exists())

    def test_actual_kernel_backing_FD_and_VMA_each_block_closure(self):
        import ctypes
        import mmap
        libc = ctypes.CDLL(None, use_errno=True)
        libc.mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_long]
        libc.mmap.restype = ctypes.c_void_p
        libc.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
        pid = os.getpid()
        born = birth(pid)
        fd = os.memfd_create('uflow-synthetic-closure')
        os.ftruncate(fd, 4096)
        info = os.fstat(fd)
        canonical = {'body_hex': b'uflow-not-a-live-shm-name'.hex(), 'dev': info.st_dev, 'inode': info.st_ino}
        mapping = None
        try:
            with self.assertRaisesRegex(RuntimeError, 'FD'):
                adopt.check_backing_closed(canonical, {pid: born})
            mapping = libc.mmap(None, 4096, mmap.PROT_READ | mmap.PROT_WRITE, mmap.MAP_SHARED, fd, 0)
            self.assertNotEqual(mapping, ctypes.c_void_p(-1).value)
            os.close(fd)
            fd = None
            with self.assertRaisesRegex(RuntimeError, 'VMA'):
                adopt.check_backing_closed(canonical, {pid: born})
            self.assertEqual(libc.munmap(mapping, 4096), 0)
            mapping = None
            self.assertEqual(adopt.check_backing_closed(canonical, {pid: born}), 'uflow-not-a-live-shm-name')
            with self.assertRaisesRegex(RuntimeError, 'disappeared'):
                adopt.check_backing_closed(canonical, {pid: born + 1})
        finally:
            if mapping is not None: libc.munmap(mapping, 4096)
            if fd is not None: os.close(fd)


if __name__ == '__main__':
    unittest.main()
