"""Sealed, metadata-only continuation of an existing O1/M1 Catalog.

This entry point never stops the old controller. A manager-reviewed capsule
must be sealed out of band, and the old birth must already be absent. It does
not construct Owner, bootstrap, start UF services or mint an admin capability.
"""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import stat
import sys
import time

from session_owner import DAEMON_SHA, PYTHON_SHA, Owner, birth
from session_protocol import GROUP, PARTITIONS, POOL_BYTES, check_retired, pool_record, save

OLD_MANIFEST_SHA = 'f4f69367a7c2a38b61262469b9b996bf4e26caebc27a9de21ba00636749a8b72'
PUBLIC = Path('/workspace/uflow/runs/phasef-parallel-20261106/p')
MANIFEST_KEYS = set(('schema group manifest_version state host_alias container host endpoint '
    'control_endpoint coordination_root owner_root helper_root pool_descriptor allocations partitions '
    'frozen_inputs provider_loaded owner_births controller_birth bootstrap_client bootstrap_unmapped '
    'capacity coordination protocol').split())
HELPER_FILES = ('resources.py', 'session_protocol.py', 'session_owner.py', 'session_adopt.py',
                'session_parent_pins.py', 'sessionctl.py')
SDK_PINS = {
    'managed_pool.py': '366f3ea883cec08efd62f23f9ebf2afa8546c02a777ff25fdf753fee1debff0e',
    '_managed_pool_wire.py': 'c61fdf8f9c19261b9386ae5c64e1ac8e2b81cf41238ee9c8f3c24c7cab59015e',
    '__init__.py': 'bb529fe27ed36eb0a4e5ced2e739411e270bcf13383001b601ca517064f6f328',
}
SNAPSHOT_KEYS = set(('schema scope revision charged_bytes creation_reserved_bytes budget_bytes '
    'operation_records control_counts native_jobs_executed payload_rpc_bytes pools').split())
U64_MAX = (1 << 64) - 1


def same_resource_snapshot(expected, observed):
    """Allow only completed snapshot-query bookkeeping; every resource stays exact."""
    for value in (expected, observed):
        if type(value) is not dict or set(value) != SNAPSHOT_KEYS:
            raise ValueError('exact frozen11-field snapshot required')
        for key in SNAPSHOT_KEYS - {'scope', 'control_counts', 'pools'}:
            if type(value[key]) is not int or not 0 <= value[key] <= U64_MAX:
                raise ValueError('strict uint64 snapshot counters required')
        if value['schema'] != 1 or type(value['scope']) is not str or type(value['pools']) is not list:
            raise ValueError('frozen snapshot resource types changed')
        counts = value['control_counts']
        if type(counts) is not dict or 'GetManagedPoolSnapshot' not in counts or any(
                type(k) is not str or type(n) is not int or not 0 <= n <= U64_MAX for k, n in counts.items()):
            raise ValueError('strict uint64 control counters required')
    prior, current = expected['control_counts'], observed['control_counts']
    if set(prior) != set(current) or any(current[k] != prior[k] for k in prior if k != 'GetManagedPoolSnapshot'):
        raise ValueError('non-snapshot UF control or counter shape changed')
    delta = current['GetManagedPoolSnapshot'] - prior['GetManagedPoolSnapshot']
    if delta < 1 or observed['revision'] - expected['revision'] != 2 * delta:
        raise ValueError('snapshot query progress/revision is not fully explained')
    # JSON bytes preserve nested bool/int/float distinctions which Python ==
    # collapses. Object key order is immaterial; array order remains exact.
    def resource_bytes(value):
        try:
            return json.dumps({k: value[k] for k in SNAPSHOT_KEYS - {'revision', 'control_counts'}},
                              sort_keys=True, separators=(',', ':'), allow_nan=False)
        except (ValueError, TypeError) as error:
            raise ValueError('invalid frozen resource JSON') from error
    if resource_bytes(expected) != resource_bytes(observed):
        raise ValueError('original full resource state changed')
    return {'snapshot_query_delta': delta, 'revision_delta': observed['revision'] - expected['revision']}


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read_json(path, limit=2 << 20):
    from session_protocol import _unique
    path = Path(path)
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_size > limit or info.st_uid != os.geteuid():
        raise ValueError('bounded same-UID regular JSON required')
    return json.loads(path.read_bytes(), object_pairs_hook=_unique,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError('non-finite JSON')))


def exact(value, fields):
    if type(value) is not dict or set(value) != set(fields.split()):
        raise ValueError('unexpected sealed capsule fields')


def current_host():
    return {'hostname': socket.gethostname(),
            'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
            'pid_namespace': os.readlink('/proc/self/ns/pid'),
            'mount_namespace': os.readlink('/proc/self/ns/mnt')}


def proc(pid):
    base = Path('/proc', str(pid))
    first = birth(pid)
    fields = (base / 'stat').read_text().rpartition(')')[2].split()
    fds = {}
    for fd in (base / 'fd').iterdir():
        try:
            info = fd.stat()
            fds[fd.name] = {'target': os.readlink(fd), 'dev': info.st_dev, 'inode': info.st_ino}
        except FileNotFoundError:
            continue
    result = {'pid': pid, 'birth': first, 'ppid': int(fields[1]),
              'exe_sha256': sha(base / 'exe'),
              'argv': (base / 'cmdline').read_bytes().decode().rstrip('\0').split('\0'),
              'children': [int(p) for p in (base / 'task' / str(pid) / 'children').read_text().split()],
              'fds': fds}
    if first is None or birth(pid) != first:
        raise ValueError('process identity raced')
    return result


def check_closed(snapshot, lines, manifest):
    if set(lines) != set(PARTITIONS):
        raise ValueError('line history shape changed')
    sessions = [manifest['bootstrap_client']]
    for line in PARTITIONS:
        exact(lines[line], 'clients retired')
        if lines[line]['retired'] is not False or type(lines[line]['clients']) is not list:
            raise ValueError('handover requires both original unretired histories')
        sessions.extend(lines[line]['clients'])
    if len(sessions) != 17 or len(set(sessions)) != 17:
        raise ValueError('all original17 registrations must remain distinct')
    pool = pool_record(snapshot, manifest['pool_descriptor'])
    if set(sessions) != {c['client_session'] for c in pool['clients']} or len(pool['clients']) != 17:
        raise ValueError('unaccounted or missing actual UF client')
    check_retired(pool, sessions)
    if pool['ranges']['leases'] or type(snapshot['operation_records']) is not int or snapshot['operation_records'] != 312:
        raise ValueError('live ranges or unexpected original counter')


def validate_capsule(capsule, manifest):
    exact(capsule, 'schema group mode decision_id old_manifest_sha256 old_controller_birth '
          'old_control_identity original_owner_births original_snapshot lines reservations '
          'new_owner_root helper_commit helper_files parent_safety closed_tasks')
    if (type(capsule['schema']) is not int or capsule['schema'] != 1 or capsule['group'] != GROUP
            or capsule['mode'] != 'adopt-existing' or type(capsule['decision_id']) is not str
            or not capsule['decision_id'] or capsule['old_manifest_sha256'] != OLD_MANIFEST_SHA):
        raise ValueError('manager capsule identity mismatch')
    if set(manifest) != MANIFEST_KEYS or manifest['manifest_version'] != 1 or manifest['schema'] != 1:
        raise ValueError('only exact original schema1 manifest may be adopted')
    if (manifest['group'] != GROUP or manifest['state'] != 'ready'
            or manifest['host_alias'] != 'serving-b-sj' or manifest['container'] != 'openeuler-2403-DS'
            or manifest['partitions'] != PARTITIONS or manifest['pool_descriptor']['nbytes'] != POOL_BYTES
            or capsule['old_controller_birth'] != manifest['controller_birth']
            or capsule['original_owner_births'] != manifest['owner_births']
            or len(manifest['owner_births']) != 3):
        raise ValueError('original pool/owner identity mismatch')
    if (capsule['reservations'] != {'schema': 1, 'group': GROUP, 'reservations': []}
            or len(capsule['helper_commit']) != 40
            or any(c not in '0123456789abcdef' for c in capsule['helper_commit'])
            or set(capsule['helper_files']) != set(HELPER_FILES)):
        raise ValueError('empty reservations and exact new source seal required')
    exact(capsule['old_control_identity'], 'dev inode uid mode')
    check_closed(capsule['original_snapshot'], capsule['lines'], manifest)
    if type(capsule['closed_tasks']) is not list or len(capsule['closed_tasks']) < 3 or len(capsule['closed_tasks']) > 8:
        raise ValueError('explicit bounded original failed-task births required')
    for row in capsule['closed_tasks']:
        exact(row, 'pid birth')
        if any(type(row[k]) is not int or row[k] <= 0 for k in row):
            raise ValueError('invalid closed-task birth')


def check_parent_safety(proof, manifest, *, observe=proc):
    """Review source pins AND original FD topology, not a PID-death drain claim."""
    exact(proof, 'source_files original_processes no_controller_parent_guard')
    if proof['no_controller_parent_guard'] is not True:
        raise ValueError('parent-exit safety has not been established')
    # These pins are generated from the frozen accepted source, never a new core.
    from session_parent_pins import SOURCE_PINS
    old_pool = Path(manifest['helper_root']).parents[1]
    if proof['source_files'] != SOURCE_PINS:
        raise ValueError('parent-safety source closure changed')
    for name, expected in SOURCE_PINS.items():
        if sha(old_pool / name) != expected:
            raise ValueError('actual original parent-safety source differs')
    original = proof['original_processes']
    controller = manifest['controller_birth']['pid']
    owners = {int(k): v for k, v in manifest['owner_births'].items()}
    if set(original) != {str(controller), *map(str, owners)}:
        raise ValueError('parent-safety process closure incomplete')
    if (original[str(controller)]['birth'] != manifest['controller_birth']['starttime_ticks']
            or original[str(controller)]['exe_sha256'] != PYTHON_SHA):
        raise ValueError('original metadata controller proof identity changed')
    loaded = manifest['provider_loaded']
    daemon, provider = loaded['parent_pid'], loaded['pid']
    trackers = set(owners) - {daemon, provider}
    if len(trackers) != 1:
        raise ValueError('expected exact daemon/provider/tracker births')
    tracker = trackers.pop()
    if (original[str(daemon)]['ppid'] != controller or original[str(provider)]['ppid'] != daemon
            or original[str(tracker)]['ppid'] != provider):
        raise ValueError('original UF process ancestry differs')
    controller_pipes = {fd['target'] for fd in original[str(controller)]['fds'].values()
                        if fd['target'].startswith('pipe:[')}
    for pid, born in owners.items():
        old, now = original[str(pid)], observe(pid)
        if old['birth'] != born or now['birth'] != born or old['exe_sha256'] != now['exe_sha256']:
            raise ValueError('original UF process/binary changed')
        if now['argv'] != old['argv']:
            raise ValueError('original UF argv changed')
        if now['exe_sha256'] != (DAEMON_SHA if pid == daemon else PYTHON_SHA):
            raise ValueError('UF executable differs from frozen closure')
        if controller_pipes & {fd['target'] for fd in old['fds'].values()}:
            raise ValueError('UF owner depends on old controller pipe')
        if (pid == daemon and '--host-pool' not in now['argv']
                or pid == provider and now['ppid'] != daemon
                or pid == tracker and now['ppid'] != provider):
            raise ValueError('UF host-only mode/ancestry changed')
        expected_children = [provider] if pid == daemon else [tracker] if pid == provider else []
        if sorted(now['children']) != expected_children:
            raise ValueError('unaccounted original UF child')


class ExistingSystem:
    """Read-only UF adapter; it exposes no allocation/bootstrap/service start API."""
    def snapshot(self, manifest):
        from uflow.managed_pool import _exchange
        value, fds = _exchange(manifest['endpoint'], 'GetManagedPoolSnapshot',
            {k: manifest['pool_descriptor'][k] for k in ('pool_id', 'pool_epoch', 'provider_session')})
        if fds:
            for fd in fds:
                os.close(fd)
            raise ValueError('unexpected FD from metadata snapshot')
        return value

    def verify(self, capsule, manifest):
        if current_host() != manifest['host']:
            raise ValueError('original host/boot/namespaces changed')
        if birth(manifest['controller_birth']['pid']) == manifest['controller_birth']['starttime_ticks']:
            raise ValueError('old metadata controller still owns the boundary')
        for row in capsule['closed_tasks']:
            if birth(row['pid']) == row['birth']:
                raise ValueError('original failed task remains alive')
        from importlib.machinery import PathFinder
        sdk = Path(manifest['helper_root']).parents[1] / 'sdk/python/uflow'
        spec = PathFinder.find_spec('uflow', sys.path)
        if (spec is None or spec.submodule_search_locations is None
                or list(spec.submodule_search_locations) != [str(sdk)]
                or any(sha(sdk / name) != expected for name, expected in SDK_PINS.items())
                or sha('/proc/self/exe') != PYTHON_SHA):
            raise ValueError('original frozen SDK/Python import closure changed')
        for name in ('uflow', 'uflow.managed_pool', 'uflow._managed_pool_wire'):
            module = sys.modules.get(name)
            if module is not None and Path(module.__file__).resolve().parent != sdk:
                raise ValueError('already loaded SDK comes from another source')
        check_parent_safety(capsule['parent_safety'], manifest)
        if read_json(PUBLIC / 'reservations.json') != capsule['reservations']:
            raise ValueError('reservation registry changed')
        # Original coordination locks and known physical lock *files* only;
        # no device open, driver call, card query or payload access.
        import fcntl
        from contextlib import ExitStack
        with ExitStack() as stack:
            paths = [Path(manifest['coordination_root']) / n for n in
                     ('host-job-0.lock', 'host-job-1.lock', 'measurement.lock')]
            paths += [Path(f'/workspace/uflow/locks/run02-npu-{d}.lock') for d in (10, 12, 14)]
            for path in paths:
                fd = stack.enter_context(path.open('r+'))
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        canonical = manifest['pool_descriptor']['canonical']
        name = bytes.fromhex(canonical['body_hex']).decode()
        if Path(name).name != name:
            raise ValueError('unsafe original backing name')
        info = Path('/dev/shm', name).stat()
        if (info.st_dev, info.st_ino, info.st_size) != (canonical['dev'], canonical['inode'], POOL_BYTES):
            raise ValueError('original backing identity changed')


def check_backing_closed(canonical, owners):
    name = bytes.fromhex(canonical['body_hex']).decode()
    if Path(name).name != name or Path('/dev/shm', name).exists():
        raise RuntimeError('original backing remains after actual Destroy ACK')
    for pid, born in owners.items():
        if birth(pid) != born:
            raise RuntimeError('UF owner disappeared before normal Shutdown; no death-as-drain')
        base = Path('/proc', str(pid))
        for fd in (base / 'fd').iterdir():
            try:
                info = fd.stat()
            except FileNotFoundError:
                continue
            if (info.st_dev, info.st_ino) == (canonical['dev'], canonical['inode']):
                raise RuntimeError('original owner still holds backing FD')
        for line in (base / 'maps').read_text().splitlines():
            fields = line.split(None, 5)
            major, minor = (int(x, 16) for x in fields[3].split(':'))
            if (os.makedev(major, minor), int(fields[4])) == (canonical['dev'], canonical['inode']):
                raise RuntimeError('original owner still maps backing VMA')
        if birth(pid) != born:
            raise RuntimeError('UF owner changed during closure observation')
    return name


class AdoptedOwner(Owner):
    @classmethod
    def adopt(cls, capsule_path, capsule_sha256, *, system=None):
        # No Owner.__init__, no bootstrap and no subprocess construction.
        if sha(capsule_path) != capsule_sha256:
            raise ValueError('manager capsule seal mismatch')
        capsule = read_json(capsule_path)
        manifest_path = PUBLIC / 'manifest.json'
        if sha(manifest_path) != OLD_MANIFEST_SHA:
            raise ValueError('old public manifest bytes changed')
        manifest = read_json(manifest_path)
        validate_capsule(capsule, manifest)
        helper = Path(__file__).resolve().parent
        if {name: sha(helper / name) for name in HELPER_FILES} != capsule['helper_files']:
            raise ValueError('new helper source seal differs')
        root = Path(capsule['new_owner_root'])
        if (root.parent != PUBLIC or not root.name.startswith('metadata-') or root.exists()
                or Path(manifest['owner_root']).parent != PUBLIC
                or manifest['endpoint'] != str(PUBLIC / 'u.sock')
                or manifest['control_endpoint'] != str(PUBLIC / 'control.sock')
                or (PUBLIC / 'manifest-v2.json').exists()):
            raise ValueError('exact original domain and fresh metadata root required')
        system = ExistingSystem() if system is None else system
        system.verify(capsule, manifest)
        observed = system.snapshot(manifest)
        check_closed(observed, capsule['lines'], manifest)
        same_resource_snapshot(capsule['original_snapshot'], observed)
        owner = cls.__new__(cls)
        owner.root, owner.public = root, PUBLIC
        owner.endpoint, owner.control = manifest['endpoint'], manifest['control_endpoint']
        owner.state = Path(manifest['owner_root']) / 'state'
        owner.lines = copy.deepcopy(capsule['lines'])
        owner.descriptor = manifest['pool_descriptor']
        owner.loaded, owner.inputs = manifest['provider_loaded'], manifest['frozen_inputs']
        owner.original_daemon_pid = owner.loaded['parent_pid']
        owner.bootstrap_client = manifest['bootstrap_client']
        owner.owners = {int(k): v for k, v in manifest['owner_births'].items()}
        owner.counter = 0
        owner.original_manifest, owner.capsule, owner.capsule_sha256 = manifest, capsule, capsule_sha256
        owner.system = system
        owner._previous_snapshot = copy.deepcopy(observed)
        # This same-host capability never enters a manifest/evidence/env/argv.
        secret_path = owner.state / 'admin.token'
        secret_stat = secret_path.lstat()
        if (not stat.S_ISREG(secret_stat.st_mode) or secret_stat.st_uid != os.geteuid()
                or stat.S_IMODE(secret_stat.st_mode) != 0o600 or secret_stat.st_size > 256):
            raise ValueError('original private admin capability identity invalid')
        owner._admin = secret_path.read_text()
        root.mkdir(mode=0o700)
        owner.manifest = copy.deepcopy(manifest)
        owner.manifest.update(manifest_version=2, owner_root=str(root), helper_root=str(helper),
                              controller_birth={'pid': os.getpid(), 'starttime_ticks': birth(os.getpid())})
        owner.manifest['frozen_inputs']['metadata_handover'] = {
            'schema': 1, 'data_service_commit': capsule['helper_commit'], 'helper_files': capsule['helper_files'],
            'predecessor_manifest_sha256': OLD_MANIFEST_SHA, 'capsule_sha256': capsule_sha256,
            'old_controller_birth': manifest['controller_birth']}
        return owner

    def publish_and_serve(self):
        """Future approved action only. Bind before publishing the new manifest."""
        import fcntl
        with (self.public / 'metadata-handover.lock').open('a+') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.system.verify(self.capsule, self.original_manifest)
            observed = self.system.snapshot(self.original_manifest)
            check_closed(observed, self.lines, self.original_manifest)
            same_resource_snapshot(self._previous_snapshot, observed)
            self._previous_snapshot = copy.deepcopy(observed)
            control = Path(self.control)
            info = control.lstat()
            expected = self.capsule['old_control_identity']
            if (not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.geteuid()
                    or {'dev': info.st_dev, 'inode': info.st_ino, 'uid': info.st_uid, 'mode': info.st_mode} != expected):
                raise ValueError('only exact stale metadata socket may be archived')
            archived = self.root / 'previous-control.sock'
            if archived.exists():
                raise ValueError('stale control archive already exists')
            control.rename(archived)  # Never u.sock or any wildcard cleanup.
            self.serve(publish=lambda: save(self.public / 'manifest-v2.json', self.manifest))

    def finalize(self):
        """A non-child daemon has no observable wait status: never invent zero."""
        from uflow.managed_pool import destroy_pool, shutdown_pool_service
        from session_protocol import check_final
        _, pool = self.snapshot('before-finalize')
        check_final(pool, self.lines, self.bootstrap_client)
        self.verify_owners('before-finalize')
        destroyed = destroy_pool(self.endpoint, self.descriptor, admin_token=self._admin)
        self.evidence('actual-destroy', destroyed)
        canonical = self.descriptor['canonical']
        name = check_backing_closed(canonical, self.owners)
        stopped = shutdown_pool_service(self.endpoint, admin_token=self._admin)
        if type(stopped) is not dict or set(stopped) != {'stopping'} or stopped['stopping'] is not True:
            raise RuntimeError('actual Shutdown ACK unconfirmed; retain original domain')
        self.evidence('actual-shutdown-ack', stopped)
        deadline = time.monotonic() + 25
        while any(birth(pid) == born for pid, born in self.owners.items()):
            if time.monotonic() > deadline:
                raise RuntimeError('original UF birth remains after Shutdown ACK')
            time.sleep(.05)
        clean = read_json(self.state / 'clean.json')
        if (clean['charged_bytes'] or clean['creation_reserved_bytes'] or clean['normal_shutdown'] is not True
                or clean['provider_exited'] is not True or Path(self.endpoint).exists()
                or Path('/dev/shm', name).exists()):
            raise RuntimeError('actual original-domain normal cleanup not proven')
        save(self.public / 'finalized.json', {'destroy': destroyed, 'shutdown': stopped, 'clean': clean,
            'original_owner_births_gone': self.owners, 'backing_absent': name,
            'daemon_exit_code': None, 'daemon_exit_code_observation': 'not a Popen child; unavailable',
            'B_charged_bytes': 0, 'A_retained_historical_bytes': POOL_BYTES, 'A_queried': False})


CURRENT_MANIFEST_SHA = 'cd76b0e88c33d25a43df44ac4487f3ce761ebc88d5351e2c5c0ed466b7a74c78'
FINAL_MODE = 'finalize-existing-v2'
O1_SESSION = '06f1879e-8532-4461-89ca-053a4b9cfb93'
O1_UNMAP = '08557172e85b4ea6a1ec0a6c7aaffa78'
FINAL_CLOSED_TASKS = [{'pid': p, 'birth': b} for p, b in (
    (3295917, 1299094309), (3285540, 1299093916), (3276350, 1299093567),
    (3332878, 1299095738), (3332902, 1299095739))]


def typed_equal(left, right):
    """Compare sealed JSON without Python's bool/int/float equivalence."""
    return json.dumps(left, sort_keys=True, allow_nan=False, separators=(',', ':')) == json.dumps(
        right, sort_keys=True, allow_nan=False, separators=(',', ':'))


def check_finalization_closed(snapshot, lines, manifest):
    if type(lines) is not dict or set(lines) != set(PARTITIONS):
        raise ValueError('exact finalization line histories required')
    for line, retired, count in (('O1', False, 1), ('M1', True, 30)):
        exact(lines[line], 'clients retired')
        clients = lines[line]['clients']
        if (lines[line]['retired'] is not retired or type(clients) is not list or len(clients) != count
                or any(type(s) is not str or not s for s in clients)):
            raise ValueError('current32 history/retirement flags changed')
    if lines['O1']['clients'] != [O1_SESSION]:
        raise ValueError('original O1 session changed')
    sessions = [manifest['bootstrap_client'], *lines['O1']['clients'], *lines['M1']['clients']]
    if len(set(sessions)) != 32:
        raise ValueError('distinct bootstrap and32 sessions required')
    pool = pool_record(snapshot, manifest['pool_descriptor'])
    if not typed_equal({k: pool[k] for k in manifest['pool_descriptor']}, manifest['pool_descriptor']):
        raise ValueError('typed pool identity changed')
    if (pool['frozen'] is not False or len(pool['clients']) != 32
            or set(sessions) != {c['client_session'] for c in pool['clients']}):
        raise ValueError('full32 actual client rows required')
    for client in pool['clients']:
        exact(client, 'access access_generation birth children client_session closing state')
        exact(client['birth'], 'gid pid pid_namespace_dev pid_namespace_ino starttime_ticks uid')
        if (client['state'] != 'Unmapped' or client['children'] != [] or client['closing'] is not True
                or client['access'] not in ('r', 'rw') or type(client['access_generation']) is not int
                or client['access_generation'] <= 0
                or any(type(n) is not int or n < 0 for n in client['birth'].values())
                or any(client['birth'][k] <= 0 for k in ('pid', 'pid_namespace_dev', 'pid_namespace_ino', 'starttime_ticks'))):
            raise ValueError('typed Unmapped client/birth/children changed')
    check_retired(pool, sessions)
    if pool['ranges']['leases'] or type(snapshot['operation_records']) is not int or snapshot['operation_records'] != 1362:
        raise ValueError('finalization requires closed original R1362 Catalog')
    # Exercise the existing full typed11-field validation, without a UF call.
    advanced = copy.deepcopy(snapshot)
    advanced['control_counts']['GetManagedPoolSnapshot'] += 1
    advanced['revision'] += 2
    same_resource_snapshot(snapshot, advanced)


def validate_finalization_capsule(capsule, origin, current):
    exact(capsule, 'schema group mode decision_id origin_manifest_sha256 current_manifest_sha256 '
          'old_controller_birth old_control_identity original_owner_births original_snapshot lines '
          'reservations new_owner_root new_manifest_path helper_commit helper_files parent_safety '
          'closed_tasks history_evidence O1_close_evidence origin_source_proof new_source_proof state_root budget')
    if (type(capsule['schema']) is not int or capsule['schema'] != 1 or capsule['group'] != GROUP
            or capsule['mode'] != FINAL_MODE or type(capsule['decision_id']) is not str or not capsule['decision_id']
            or capsule['origin_manifest_sha256'] != OLD_MANIFEST_SHA
            or capsule['current_manifest_sha256'] != CURRENT_MANIFEST_SHA):
        raise ValueError('explicit current-v2 finalization capsule required')
    for manifest, version in ((origin, 1), (current, 2)):
        if (type(manifest) is not dict or set(manifest) != MANIFEST_KEYS
                or type(manifest['schema']) is not int or manifest['schema'] != 1
                or type(manifest['manifest_version']) is not int or manifest['manifest_version'] != version):
            raise ValueError('exact24-field v1/v2 lineage required')
    mutable = {'manifest_version', 'owner_root', 'helper_root', 'controller_birth', 'frozen_inputs'}
    if (not typed_equal({k: origin[k] for k in MANIFEST_KEYS - mutable},
                        {k: current[k] for k in MANIFEST_KEYS - mutable})
            or not typed_equal({k: v for k, v in current['frozen_inputs'].items() if k != 'metadata_handover'}, origin['frozen_inputs'])
            or current['frozen_inputs']['metadata_handover']['predecessor_manifest_sha256'] != OLD_MANIFEST_SHA
            or not typed_equal(current['controller_birth'], {'pid': 98781, 'starttime_ticks': 1298738805})
            or not typed_equal(capsule['old_controller_birth'], current['controller_birth'])
            or not typed_equal(capsule['original_owner_births'], origin['owner_births'])
            or origin['host_alias'] != 'serving-b-sj' or origin['container'] != 'openeuler-2403-DS'
            or origin['state'] != 'ready' or not typed_equal(origin['partitions'], PARTITIONS)):
        raise ValueError('original lineage/host/owner/immutable manifest changed')
    if (capsule['state_root'] != str(Path(origin['owner_root']) / 'state')
            or capsule['state_root'] == str(Path(current['owner_root']) / 'state')
            or Path(origin['owner_root']).parent != PUBLIC
            or current['owner_root'] != str(PUBLIC / 'metadata-v2-001')
            or current['endpoint'] != str(PUBLIC / 'u.sock') or current['control_endpoint'] != str(PUBLIC / 'control.sock')):
        raise ValueError('state must come from sealed v1 daemon root, not v2 metadata root')
    if (not re.fullmatch(r'[0-9a-f]{40}', capsule['helper_commit'])
            or set(capsule['helper_files']) != set(HELPER_FILES)
            or any(type(v) is not str or not re.fullmatch(r'[0-9a-f]{64}', v) for v in capsule['helper_files'].values())
            or not typed_equal(capsule['closed_tasks'], FINAL_CLOSED_TASKS)):
        raise ValueError('exact new source and original closed-task seals required')
    for field, prefix, suffix in (('new_owner_root', 'metadata-finalize-', ''),
                                  ('new_manifest_path', 'manifest-finalization-', '.json')):
        path = Path(capsule[field])
        pattern = re.escape(prefix) + r'[a-zA-Z0-9-]+' + re.escape(suffix)
        if path.parent != PUBLIC or not re.fullmatch(pattern, path.name):
            raise ValueError('fresh bounded finalization paths required')
    exact(capsule['old_control_identity'], 'dev inode uid mode')
    identity = capsule['old_control_identity']
    if (any(type(n) is not int or n < 0 for n in identity.values())
            or identity['uid'] != os.geteuid() or not stat.S_ISSOCK(identity['mode'])):
        raise ValueError('exact same-UID stale socket identity required')
    for field in ('history_evidence', 'O1_close_evidence', 'origin_source_proof', 'new_source_proof'):
        exact(capsule[field], 'path sha256')
        if type(capsule[field]['path']) is not str or not re.fullmatch(r'[0-9a-f]{64}', capsule[field]['sha256']):
            raise ValueError('exact bounded evidence seal required')
    expected_budget = {'anchor': 1362, 'finalizer_records': 2, 'terminal_records': 256,
                       'admission_limit': 4096, 'cleanup_limit': 8192}
    if not typed_equal(capsule['budget'], expected_budget):
        raise ValueError('original cost/finalizer/terminal limits changed')
    reservations = capsule['reservations']
    exact(reservations, 'schema group reservations')
    if (type(reservations['schema']) is not int or reservations['schema'] != 1 or reservations['group'] != GROUP
            or type(reservations['reservations']) is not list or len(reservations['reservations']) != 1):
        raise ValueError('old O1 reservation must be preserved until actual ACK')
    row = reservations['reservations'][0]
    exact(row, 'line physical_device board chip bdf execution_domain lock_path lock_holder_pid lock_holder_birth evidence_path')
    if (row['line'] != 'O1' or any(type(row[k]) is not int for k in ('physical_device', 'board', 'chip', 'lock_holder_pid', 'lock_holder_birth'))
            or (row['physical_device'], row['board'], row['chip'], row['lock_holder_pid'], row['lock_holder_birth']) != (10, 5, 0, 3285540, 1299093916)
            or row['bdf'] != '0000:89:00.0' or row['execution_domain'] != 'B/0000:89:00.0/physical-10'
            or row['lock_path'] != '/workspace/uflow/locks/run02-npu-10.lock'
            or row['evidence_path'] != '/workspace/uflow/runs/phasef-parallel-20261106/o1/campaign002/calibration002/resource-before.json'):
        raise ValueError('exact original O1 reservation changed')
    check_finalization_closed(capsule['original_snapshot'], capsule['lines'], current)


def check_finalization_state(capsule, origin, daemon):
    """Only the sealed v1 state named by the actual original daemon is usable."""
    args = daemon['argv']
    if (capsule['state_root'] != str(Path(origin['owner_root']) / 'state')
            or daemon['pid'] != origin['provider_loaded']['parent_pid']
            or daemon['birth'] != origin['owner_births'][str(daemon['pid'])]
            or daemon['exe_sha256'] != DAEMON_SHA or args.count('--pool-state-root') != 1
            or args.index('--pool-state-root') + 1 >= len(args)
            or args[args.index('--pool-state-root') + 1] != capsule['state_root']):
        raise ValueError('real original daemon does not name sealed v1 state')


def check_finalization_close(close):
    if (close['operation_id'] != O1_UNMAP or close['ACK']['unmapped'] is not True
            or close['ACK']['unknown_content_invalid_not_committed'] is not False
            or close['actual_native_close_complete'] is not True or close['actual_SDK_view_closed'] is not True
            or not typed_equal(close['pool_no_VMA_FD'], {'pid': 3295917, 'fds': [], 'maps': []})
            or type(close['pid']) is not int or close['pid'] != 3295917
            or type(close['birth']) is not int or close['birth'] != 1299094309):
        raise ValueError('original real O1 close/Unmapped evidence changed')


class FinalizationSystem(ExistingSystem):
    """Read-only validation for the explicit, finalization-only current32 mode."""
    def verify(self, capsule, manifest):
        origin = read_json(PUBLIC / 'manifest.json')
        if sha(PUBLIC / 'manifest.json') != OLD_MANIFEST_SHA or sha(PUBLIC / 'manifest-v2.json') != CURRENT_MANIFEST_SHA:
            raise ValueError('sealed v1/v2 lineage changed')
        if current_host() != manifest['host'] or birth(98781) is not None:
            raise ValueError('host changed or original metadata PID is occupied')
        for row in capsule['closed_tasks']:
            if birth(row['pid']) == row['birth']:
                raise ValueError('original O1 task still alive')
        from importlib.machinery import PathFinder
        sdk = Path(origin['helper_root']).parents[1] / 'sdk/python/uflow'
        spec = PathFinder.find_spec('uflow', sys.path)
        if (spec is None or list(spec.submodule_search_locations or []) != [str(sdk)]
                or any(sha(sdk / name) != expected for name, expected in SDK_PINS.items())
                or sha('/proc/self/exe') != PYTHON_SHA):
            raise ValueError('original SDK/Python loader closure changed')
        for name in ('uflow', 'uflow.managed_pool', 'uflow._managed_pool_wire'):
            module = sys.modules.get(name)
            if module is not None and Path(module.__file__).resolve().parent != sdk:
                raise ValueError('already loaded SDK comes from another source')
        check_parent_safety(capsule['parent_safety'], origin)
        daemon = proc(origin['provider_loaded']['parent_pid'])
        check_finalization_state(capsule, origin, daemon)
        state = Path(capsule['state_root'])
        if state.is_symlink() or not state.is_dir() or state.stat().st_uid != os.geteuid():
            raise ValueError('original state directory identity invalid')
        for pid, born in origin['owner_births'].items():
            status = Path('/proc', pid, 'status').read_text().splitlines()
            if birth(int(pid)) != born or next(v for v in status if v.startswith('Uid:')).split()[1:] != [str(os.geteuid())] * 4:
                raise ValueError('original UF birth/UID changed')
        for field in ('history_evidence', 'O1_close_evidence', 'origin_source_proof', 'new_source_proof'):
            row = capsule[field]
            if sha(row['path']) != row['sha256']:
                raise ValueError('actual evidence/source seal changed')
        history = capsule['history_evidence']
        if (Path(history['path']) != Path(manifest['owner_root']) / '0114-line-state.json'
                or not typed_equal(read_json(history['path']), capsule['lines'])):
            raise ValueError('last actually persisted exact histories changed')
        close_path = PUBLIC.parent / 'o1/campaign002/calibration002/consumer/cooperative-close/close-success.json'
        close = read_json(capsule['O1_close_evidence']['path'])
        if Path(capsule['O1_close_evidence']['path']) != close_path:
            raise ValueError('original O1 close receipt path changed')
        check_finalization_close(close)
        old_proof = capsule['origin_source_proof']
        if (old_proof['path'] != str(PUBLIC.parent / 'o1/reserve-recovery-final003/git-deployment.json')
                or old_proof['sha256'] != '38238c4db4292597df12d5abe97a98c4d63c8c57f5224ac3c4c88e2326784d51'):
            raise ValueError('accepted original task source closure changed')
        proof = read_json(capsule['new_source_proof']['path'])
        base = Path(__file__).resolve().parents[2]
        if proof['commit'] != capsule['helper_commit'] or proof['root'] != str(base):
            raise ValueError('new helper source commit/root mismatch')
        for name, row in proof['files'].items():
            path = base / name
            if Path(name).is_absolute() or '..' in Path(name).parts:
                raise ValueError('source path escapes bounded tree')
            data = os.readlink(path).encode() if row['mode'] == '120000' else path.read_bytes()
            if (len(data) != row['bytes'] or hashlib.sha256(data).hexdigest() != row['sha256']
                    or (row['mode'] != '120000' and bool(path.stat().st_mode & 0o111) != (row['mode'] == '100755'))):
                raise ValueError('new actual Git source content/mode drift')
        if read_json(PUBLIC / 'reservations.json') != capsule['reservations']:
            raise ValueError('original reservation registry changed')
        import fcntl
        from contextlib import ExitStack
        with ExitStack() as stack:
            paths = [Path(manifest['coordination_root']) / n for n in ('host-job-0.lock', 'host-job-1.lock', 'measurement.lock')]
            paths += [Path(f'/workspace/uflow/locks/run02-npu-{d}.lock') for d in (10, 12, 14, 8, 9)]
            for path in paths:
                handle = stack.enter_context(path.open('r+'))
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        canonical = manifest['pool_descriptor']['canonical']
        name = bytes.fromhex(canonical['body_hex']).decode()
        if Path(name).name != name:
            raise ValueError('unsafe backing name')
        info = Path('/dev/shm', name).stat()
        if (info.st_dev, info.st_ino, info.st_size) != (canonical['dev'], canonical['inode'], POOL_BYTES):
            raise ValueError('original physical backing changed')


class FinalizationOwner(AdoptedOwner):
    @classmethod
    def adopt_finalization(cls, capsule_path, capsule_sha256, *, system=None):
        if sha(capsule_path) != capsule_sha256:
            raise ValueError('finalization capsule seal mismatch')
        capsule = read_json(capsule_path)
        if sha(PUBLIC / 'manifest.json') != OLD_MANIFEST_SHA or sha(PUBLIC / 'manifest-v2.json') != CURRENT_MANIFEST_SHA:
            raise ValueError('actual sealed v1/v2 manifest mismatch')
        origin, current = read_json(PUBLIC / 'manifest.json'), read_json(PUBLIC / 'manifest-v2.json')
        validate_finalization_capsule(capsule, origin, current)
        helper = Path(__file__).resolve().parent
        if {n: sha(helper / n) for n in HELPER_FILES} != capsule['helper_files']:
            raise ValueError('new runtime helper source differs')
        root, publication = Path(capsule['new_owner_root']), Path(capsule['new_manifest_path'])
        if root.exists() or publication.exists() or (PUBLIC / 'finalized.json').exists():
            raise ValueError('new finalization root/publication must be unused')
        system = FinalizationSystem() if system is None else system
        system.verify(capsule, current)
        observed = system.snapshot(current)
        check_finalization_closed(observed, capsule['lines'], current)
        same_resource_snapshot(capsule['original_snapshot'], observed)
        owner = cls.__new__(cls)
        owner.root, owner.public = root, PUBLIC
        owner.endpoint, owner.control = current['endpoint'], current['control_endpoint']
        owner.state = Path(capsule['state_root'])
        owner.lines, owner.descriptor = copy.deepcopy(capsule['lines']), current['pool_descriptor']
        owner.loaded, owner.inputs = current['provider_loaded'], current['frozen_inputs']
        owner.original_daemon_pid = owner.loaded['parent_pid']
        owner.bootstrap_client = current['bootstrap_client']
        owner.owners, owner.counter = {int(k): v for k, v in current['owner_births'].items()}, 0
        owner.original_manifest, owner.capsule, owner.capsule_sha256 = current, capsule, capsule_sha256
        owner.system, owner._previous_snapshot = system, copy.deepcopy(observed)
        secret = owner.state / 'admin.token'
        info = secret.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600 or not 0 < info.st_size <= 256:
            raise ValueError('original private capability identity invalid')
        owner._admin = secret.read_text()  # Internal same-B owner use only; never evidence/env/argv.
        root.mkdir(mode=0o700)
        owner.manifest = copy.deepcopy(current)
        owner.manifest.update(owner_root=str(root), helper_root=str(helper),
                              controller_birth={'pid': os.getpid(), 'starttime_ticks': birth(os.getpid())})
        owner.manifest['frozen_inputs']['metadata_finalization'] = {
            'schema': 1, 'mode': FINAL_MODE, 'data_service_commit': capsule['helper_commit'],
            'helper_files': capsule['helper_files'], 'origin_manifest_sha256': OLD_MANIFEST_SHA,
            'predecessor_manifest_sha256': CURRENT_MANIFEST_SHA, 'capsule_sha256': capsule_sha256,
            'old_controller_birth': current['controller_birth']}
        owner.publication = publication
        return owner

    def handle(self, body):
        if body.get('op') == 'register-client' or (body.get('op') != 'describe' and (
                body.get('line') != 'O1' or body.get('op') == 'reserve-devices' and body.get('reservations') != [])):
            raise ValueError('finalization-only owner permits no new client/reservation/M1 mutation')
        return super().handle(body)

    def finalize(self):
        observed, _ = self.snapshot('finalization-budget-and-resource-check')
        same_resource_snapshot(self._previous_snapshot, observed)
        if (observed['operation_records'] != 1362 or observed['operation_records'] + 2 >= 4096
                or observed['operation_records'] + 2 + 256 >= 8192):
            raise ValueError('original R1362/finalizer2/terminal256 limits no longer hold')
        super().finalize()

    def publish_and_serve(self):
        import fcntl
        with (self.public / 'metadata-handover.lock').open('a+') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.system.verify(self.capsule, self.original_manifest)
            observed = self.system.snapshot(self.original_manifest)
            check_finalization_closed(observed, self.lines, self.original_manifest)
            same_resource_snapshot(self._previous_snapshot, observed)
            self._previous_snapshot = copy.deepcopy(observed)
            info = Path(self.control).lstat()
            if not typed_equal({'dev': info.st_dev, 'inode': info.st_ino, 'uid': info.st_uid, 'mode': info.st_mode},
                               self.capsule['old_control_identity']):
                raise ValueError('only exact stale original control socket may be archived')
            archived = self.root / 'previous-control.sock'
            if archived.exists():
                raise ValueError('stale socket archive already exists')
            Path(self.control).rename(archived)
            self.serve(publish=lambda: save(self.publication, self.manifest))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--capsule', type=Path, required=True)
    parser.add_argument('--capsule-sha256', required=True)
    parser.add_argument('--mode', choices=('adopt-existing', FINAL_MODE), default='adopt-existing')
    args = parser.parse_args()
    if args.mode == FINAL_MODE:
        FinalizationOwner.adopt_finalization(args.capsule, args.capsule_sha256).publish_and_serve()
    else:
        AdoptedOwner.adopt(args.capsule, args.capsule_sha256).publish_and_serve()


if __name__ == '__main__':
    main()
