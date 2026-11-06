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
        if observed != capsule['original_snapshot']:
            raise ValueError('original full Catalog state changed; no handover')
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
            if self.system.snapshot(self.original_manifest) != self.capsule['original_snapshot']:
                raise ValueError('Catalog changed before metadata publication')
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--capsule', type=Path, required=True)
    parser.add_argument('--capsule-sha256', required=True)
    args = parser.parse_args()
    AdoptedOwner.adopt(args.capsule, args.capsule_sha256).publish_and_serve()


if __name__ == '__main__':
    main()
