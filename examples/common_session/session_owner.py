"""E-only lifecycle owner for one real host pool shared by O1 and M1.

The service waits for explicit bounded metadata commands. It never probes for
free devices, runs arbitrary commands, or infers native completion from death.
Failures retain the same Catalog/backing and all evidence.
"""
import argparse
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import sys
import time

from session_protocol import (
    BLOCK_BYTES, GROUP, GUARD_BYTES, PARTITIONS, PAYLOAD_BYTES, POOL_BYTES,
    check_final, check_retired, pool_record, receive, save,
)

DAEMON_SHA = 'd4a84f6be5923fb57b8180f0aec67288b75060c0fe41993a517f59efff4ad09b'
FENCE_SHA = 'babe1a791fa82551a5cfdcec83eceeb104ca3c5d6ff1b81cacdc0e42e48c9c9e'
PYTHON_SHA = '1195c76f45c7cf16bf776a572cc85e074669c6f1a17d80d83fa8b7a7e3f80a87'


def birth(pid):
    base = Path('/proc', str(pid))
    try:
        return int((base / 'stat').read_text().rpartition(')')[2].split()[19])
    except FileNotFoundError:
        return None


class Owner:
    def __init__(self, args):
        from host_guard import sha
        self.root = args.root.resolve()
        self.root.mkdir(mode=0o700)
        self.public = self.root.parent
        self.daemon_path = args.daemon.resolve()
        if sha(self.daemon_path) != DAEMON_SHA or sha(self.daemon_path.parent / 'api_fence.so') != FENCE_SHA:
            raise ValueError('daemon/fence differs from accepted binary closure')
        self.endpoint = str(self.public / 'u.sock')
        self.control = str(self.public / 'control.sock')
        if max(len(p.encode()) for p in (self.endpoint, self.control)) >= 108:
            raise ValueError('Unix socket path too long')
        if Path(self.endpoint).exists() or Path(self.control).exists() or (self.public / 'manifest.json').exists():
            raise ValueError('existing session: inspect original responsibilities; no replacement')
        self.state = self.root / 'state'
        self.lines = {line: {'clients': [], 'retired': False} for line in PARTITIONS}
        self.counter = 0
        self.descriptor = None
        self.owners = {}
        self.inputs = json.loads(args.inputs.read_text())
        self.log = (self.root / 'daemon.log').open('xb')
        argv = [str(self.daemon_path), '--host-pool', '--socket', self.endpoint,
                '--pool-state-root', str(self.state), '--pool-provider-python', sys.executable,
                '--pool-budget-bytes', str(POOL_BYTES)]
        save(self.root / 'command.json', {'argv': argv, 'admin_in_argv_env': False})
        self.daemon = subprocess.Popen(argv, stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 45
        while not (self.state / 'state.json').exists() or not Path(self.endpoint).exists():
            if self.daemon.poll() is not None or time.monotonic() >= deadline:
                raise RuntimeError('startup failed: original domain/processes retained')
            time.sleep(.05)
        initial = json.loads((self.state / 'state.json').read_text())
        if (initial['budget_bytes'], initial['charged_bytes'], initial['creation_reserved_bytes'], initial['pools']) != (
                POOL_BYTES, 0, 0, []):
            raise ValueError('new host Catalog is not empty and hard-capped')
        self.loaded = initial['provider_loaded']
        if (self.loaded['parent_pid'] != self.daemon.pid
                or self.loaded['simpler_revision'] != '508becb93e182a3035a14e3f9226929dbe994346'
                or self.loaded['simpler_sha256'] != 'ee1e5b64d5f836950892fc3d6e986a3eda8d106b7c92f27f8420589184073f96'
                or self.loaded['task_interface_sha256'] != 'd7945a85cc49aedeed14006f1c7d2077807f6fb5e9565fe21cfa6cb9e694769d'):
            raise ValueError('loaded provider/runtime differs from frozen stack')
        # Never serialize/copy this private capability or put it in argv/env.
        self._admin = (self.state / 'admin.token').read_text()
        self.verify_owners('before-create')

    def evidence(self, label, value):
        self.counter += 1
        save(self.root / f'{self.counter:04d}-{label}.json', value)

    def snapshot(self, label):
        from uflow.managed_pool import _exchange
        value, fds = _exchange(self.endpoint, 'GetManagedPoolSnapshot',
            {k: self.descriptor[k] for k in ('pool_id', 'pool_epoch', 'provider_session')})
        if fds:
            raise RuntimeError('metadata snapshot returned unexpected FDs')
        self.evidence(label, value)
        pool = pool_record(value, self.descriptor)
        return value, pool

    def verify_owners(self, label):
        from host_guard import inventory, judge
        pending = [self.daemon.pid]
        current = {}
        while pending:
            pid = pending.pop()
            if pid in current or len(current) >= 8:
                raise ValueError('unexpected host owner tree')
            value = inventory(pid)
            self.evidence(f'{label}-inventory-{pid}', value)
            role = 'daemon' if pid == self.daemon.pid else 'provider' if pid == self.loaded['pid'] else 'tracker'
            judge(value, role)
            if value['exe_sha256'] != (DAEMON_SHA if role == 'daemon' else PYTHON_SHA):
                raise ValueError('host executable identity changed')
            current[pid] = birth(pid)
            pending.extend(value['children'])
        if any(v is None or k in self.owners and self.owners[k] != v for k, v in current.items()):
            raise ValueError('host owner process birth changed')
        self.owners = current
        bindings = self.loaded['test_fence_bindings']
        symbols = json.loads(Path(os.environ['UFLOW_POOL_TEST_SYMBOLS']).read_text())
        if set(bindings) != set(symbols) or len(bindings) != 79 or set(bindings.values()) != {os.environ['UFLOW_POOL_TEST_FENCE']}:
            raise ValueError('actual provider fence bindings changed')
        self.evidence(f'{label}-births', current)

    def bootstrap(self):
        from host_guard import inventory, judge
        from uflow.managed_pool import create_pool, ManagedPoolClient, ManagedPoolError, ManagedPoolUnknown
        self.descriptor = create_pool(self.endpoint, admin_token=self._admin)
        self.snapshot('created')
        self.verify_owners('allocated')
        try:
            create_pool(self.endpoint, admin_token=self._admin)
        except ManagedPoolUnknown:
            raise RuntimeError('second allocation outcome Unknown: retain same Catalog')
        except ManagedPoolError as error:
            if not error.response or error.response['code'] != 'Rejected':
                raise
            self.evidence('hard-cap-rejection', error.response)
        else:
            raise RuntimeError('Catalog admitted another physical pool')
        client = ManagedPoolClient.register(self.endpoint, self.descriptor['pool_id'], self.descriptor['pool_epoch'])
        self.bootstrap_client = client._descriptor['client_session']
        before = inventory(os.getpid())
        self.evidence('initializer-inventory', before)
        judge(before, 'client')
        allocations = []
        for index in range(5):
            block = client.allocate_range(BLOCK_BYTES, 65536)
            if block.nbytes != BLOCK_BYTES or block.offset % 65536 or any(
                    block.offset < r['block']['offset'] + BLOCK_BYTES and r['block']['offset'] < block.offset + BLOCK_BYTES
                    for r in allocations):
                raise ValueError('real UF allocation overlap/size/alignment')
            contents = {}
            for name, offset, length, byte in (('guard', 0, GUARD_BYTES, 0x91 + index),
                                              ('payload', GUARD_BYTES, PAYLOAD_BYTES, 0x41 + index)):
                lease = client.acquire_range(block, 'w', block.generation, byte_offset=offset, nbytes=length)
                data = bytes([byte]) * length
                client.view.buffer[lease.offset:lease.offset + length] = data
                if client.view.buffer[lease.offset:lease.offset + length] != data:
                    raise ValueError('full initializer byte verification failed')
                commit = client.commit_write(lease, f'{GROUP}-{index}-{name}-initial', length)
                read = client.acquire_range(block, 'r', block.generation, byte_offset=offset, nbytes=length)
                actual = client.view.buffer[read.offset:read.offset + read.nbytes]
                if read.content_version != commit['content_version'] or actual != data:
                    raise ValueError('real R lease/version/full bytes differ after commit')
                client.release(read)
                contents[name] = {'offset': block.offset + offset, 'byte_offset_in_block': offset, 'nbytes': length,
                                  'sha256': hashlib.sha256(actual).hexdigest(), 'byte_value': byte,
                                  'initial_commit': commit}
            allocations.append({'index': index, 'line': 'O1' if index == 0 else 'M1',
                                'block': asdict(block), **contents})
        client.begin_unregister()
        client.view.close()
        unmapped = client.ack_unmapped(native_drained=True)
        self.evidence('initializer-actually-unmapped', {'client_session': self.bootstrap_client, 'ack': unmapped})
        snapshot, pool = self.snapshot('manifest-ready')
        check_retired(pool, [self.bootstrap_client])
        if pool['ranges']['leases']:
            raise ValueError('bootstrap still holds leases')
        self.verify_owners('ready')
        coord = self.public / 'coordination'
        coord.mkdir()
        for name in ('host-job-0.lock', 'host-job-1.lock', 'measurement.lock'):
            (coord / name).touch(exist_ok=False)
        manifest = {'schema': 1, 'group': GROUP, 'manifest_version': 1, 'state': 'ready',
            'host_alias': 'serving-b-sj', 'container': 'openeuler-2403-DS',
            'host': {'hostname': socket.gethostname(), 'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                     'pid_namespace': os.readlink('/proc/self/ns/pid'), 'mount_namespace': os.readlink('/proc/self/ns/mnt')},
            'endpoint': self.endpoint, 'control_endpoint': self.control, 'coordination_root': str(coord),
            'owner_root': str(self.root),
            'helper_root': str(Path(__file__).resolve().parent), 'pool_descriptor': self.descriptor,
            'allocations': allocations, 'partitions': PARTITIONS, 'frozen_inputs': self.inputs,
            'provider_loaded': {k: v for k, v in self.loaded.items() if k != 'test_process_guard'},
            'owner_births': {str(k): v for k, v in self.owners.items()},
            'controller_birth': {'pid': os.getpid(), 'starttime_ticks': birth(os.getpid())},
            'bootstrap_client': self.bootstrap_client, 'bootstrap_unmapped': unmapped,
            'capacity': {'B_budget_bytes': POOL_BYTES, 'B_charged_bytes': snapshot['charged_bytes'],
                         'B_creation_reserved_bytes': 0, 'allocated_range_bytes': 5 * BLOCK_BYTES,
                         'unassigned_still_charged_bytes': POOL_BYTES - 5 * BLOCK_BYTES,
                         'A_retained_historical_bytes': POOL_BYTES, 'A_queried': False,
                         'task_total_responsibility_bytes': 2 * POOL_BYTES},
            'coordination': {'host_heavy_job_slots': 2, 'measurement_max_seconds': 120,
                             'windows_acquired_before_leases_or_directory_locks': True,
                             'physical_reservations_file': str(self.public / 'reservations.json')},
            'protocol': {'register_client': 'request(manifest, "register-client", line="M1", client_session=actual_UF_session)',
                         'retire': 'Close/drain all native/children/views; real SDK unmap ACK; then request line-retired with all registered sessions. No new clients after retirement.',
                         'finalize': 'E alone runs sessionctl finalize after BOTH real retirements. UF rechecks all obligations; actual provider close/unlink, daemon FD close and normal owner exit required.',
                         'failure': 'Unknown remains charged in same Catalog. Never replace pool or infer native drain from PID death.',
                         'authority': 'UF is range/lease/version/budget authority; directory/control metadata grants no access.'}}
        self.manifest = manifest
        save(self.public / 'manifest.json', manifest)
        save(self.public / 'reservations.json', {'schema': 1, 'group': GROUP, 'reservations': []})

    def handle(self, body):
        if body.get('schema') != 1 or type(body.get('schema')) is not int or body.get('group') != GROUP:
            raise ValueError('control schema/group mismatch')
        op = body.get('op')
        expected = {'describe': set(), 'register-client': {'line', 'client_session'},
                    'line-retired': {'line', 'client_sessions'}, 'reserve-devices': {'line', 'reservations'}}
        if op not in expected or set(body) != {'schema', 'group', 'op'} | expected[op]:
            raise ValueError('unknown command or control fields')
        snapshot, pool = self.snapshot('control-' + op)
        if op == 'describe':
            return {'manifest': self.manifest, 'lines': self.lines, 'snapshot': snapshot}
        line = body['line']
        if line not in self.lines or self.lines[line]['retired']:
            raise ValueError('unknown or already retired line')
        if op == 'register-client':
            session = body['client_session']
            rows = {c['client_session']: c for c in pool['clients']}
            if type(session) is not str or session not in rows or session == self.bootstrap_client:
                raise ValueError('must register an actual UF consumer client')
            if any(session in self.lines[other]['clients'] for other in self.lines if other != line):
                raise ValueError('client already belongs to the other line')
            if session not in self.lines[line]['clients']:
                self.lines[line]['clients'].append(session)
        elif op == 'line-retired':
            sessions = body['client_sessions']
            if type(sessions) is not list or any(type(s) is not str for s in sessions) or len(sessions) != len(set(sessions)):
                raise ValueError('unique exact client sessions required')
            if set(sessions) != set(self.lines[line]['clients']):
                raise ValueError('retirement must include every registered line client')
            check_retired(pool, sessions)
            self.lines[line]['retired'] = True
        else:
            self.reserve(line, body['reservations'])
        self.evidence('line-state', self.lines)
        return {'lines': self.lines, 'B_charged_bytes': snapshot['charged_bytes'], 'pool_destroyed': False}

    def reserve(self, line, entries):
        if type(entries) is not list or len(entries) > (1 if line == 'O1' else 4):
            raise ValueError('device reservation count exceeds line bound')
        path = self.public / 'reservations.json'
        current = json.loads(path.read_text())
        others = [r for r in current['reservations'] if r['line'] != line]
        occupied = {r['physical_device'] for r in others}
        domains = {r['execution_domain'] for r in others}
        ids = set()
        for row in entries:
            if set(row) != {'physical_device', 'board', 'chip', 'bdf', 'execution_domain', 'lock_path', 'lock_holder_pid', 'lock_holder_birth', 'evidence_path'}:
                raise ValueError('full actual mapping/lock evidence required')
            device = row['physical_device']
            if type(device) is not int or device not in range(16) or device in ids or device in occupied:
                raise ValueError('physical device overlaps another reservation')
            if any(type(row[k]) is not str or not row[k] for k in ('bdf', 'execution_domain', 'evidence_path')) or row['execution_domain'] in domains:
                raise ValueError('execution domain missing or already reserved')
            if device == 11 and line == 'M1':
                raise ValueError('M1 must avoid O1 board sibling11')
            if row['lock_path'] != f'/workspace/uflow/locks/run02-npu-{device}.lock' or birth(row['lock_holder_pid']) != row['lock_holder_birth']:
                raise ValueError('reservation holder/lock identity mismatch')
            # The actual holder must possess the same lock inode; the registry is
            # metadata only, and never substitutes for its flock/FD/health check.
            wanted = Path(row['lock_path']).stat()
            held = False
            for path in Path('/proc', str(row['lock_holder_pid']), 'fd').iterdir():
                try:
                    actual = path.stat()
                    details = Path('/proc', str(row['lock_holder_pid']), 'fdinfo', path.name).read_text()
                except FileNotFoundError:
                    continue
                if (actual.st_ino, actual.st_dev) == (wanted.st_ino, wanted.st_dev) and any(
                        'FLOCK  ADVISORY  WRITE' in entry and str(row['lock_holder_pid']) in entry.split()
                        for entry in details.splitlines() if entry.startswith('lock:')):
                    held = True
            if not held:
                raise ValueError('reservation holder lacks the actual FLOCK write lock')
            ids.add(device)
            domains.add(row['execution_domain'])
        other = [r for r in current['reservations'] if r['line'] != line]
        save(path, {**current, 'reservations': other + [{'line': line, **r} for r in entries]}, exclusive=False)

    def finalize(self):
        from uflow.managed_pool import destroy_pool, shutdown_pool_service
        _, pool = self.snapshot('before-finalize')
        check_final(pool, self.lines, self.bootstrap_client)
        self.verify_owners('before-finalize')
        result = destroy_pool(self.endpoint, self.descriptor, admin_token=self._admin)
        self.evidence('actual-destroy', result)
        canonical = self.descriptor['canonical']
        name = bytes.fromhex(canonical['body_hex']).decode()
        if Path(name).name != name or Path('/dev/shm', name).exists():
            raise RuntimeError('provider has not really unlinked original backing')
        for pid in self.owners:
            for fd in Path('/proc', str(pid), 'fd').iterdir():
                try:
                    info = fd.stat()
                except FileNotFoundError:
                    continue
                if (info.st_dev, info.st_ino) == (canonical['dev'], canonical['inode']):
                    raise RuntimeError('original owner still holds physical pool FD')
        stopped = shutdown_pool_service(self.endpoint, admin_token=self._admin)
        if self.daemon.wait(timeout=25) != 0:
            raise RuntimeError('normal host daemon shutdown failed')
        deadline = time.monotonic() + 20
        while any(birth(pid) == value for pid, value in self.owners.items()):
            if time.monotonic() > deadline:
                raise RuntimeError('host owner birth remains after actual shutdown')
            time.sleep(.05)
        clean = json.loads((self.state / 'clean.json').read_text())
        if (clean['charged_bytes'] or clean['creation_reserved_bytes'] or not clean['normal_shutdown']
                or not clean['provider_exited'] or Path(self.endpoint).exists()):
            raise RuntimeError('actual host shutdown/zero budget not proven')
        self.log.close()
        save(self.public / 'finalized.json', {'destroy': result, 'shutdown': stopped, 'clean': clean,
             'original_owner_births_gone': self.owners, 'backing_absent': name,
             'B_charged_bytes': 0, 'A_retained_historical_bytes': POOL_BYTES, 'A_queried': False})

    def serve(self):
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(self.control)
            os.chmod(self.control, 0o600)
            server.listen(8)
            server.settimeout(1)
            while True:
                if (self.root / 'finalize.request').exists():
                    self.finalize()
                    break
                try:
                    connection, _ = server.accept()
                except socket.timeout:
                    continue
                with connection:
                    connection.settimeout(5)
                    try:
                        _, uid, _ = struct.unpack('3i', connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
                        if uid != os.geteuid():
                            raise ValueError('same trusted UID required')
                        body = receive(connection)
                        value = {'status': 'ok', 'result': self.handle(body)}
                    except Exception as error:
                        value = {'status': 'error', 'detail': repr(error)}
                        self.evidence('control-error', value)
                    connection.sendall(json.dumps(value).encode() + b'\n')
        Path(self.control).unlink()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--daemon', type=Path, required=True)
    parser.add_argument('--inputs', type=Path, required=True)
    args = parser.parse_args()
    owner = Owner(args)
    owner.bootstrap()
    owner.serve()


if __name__ == '__main__':
    main()
