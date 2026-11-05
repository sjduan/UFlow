"""Real CPU/Simpler pool acceptance; fresh task roots, no device/native jobs."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

from client_peer import read_line
from host_guard import BUFFER_SHA, binding_proof, save_guard
from uflow.managed_pool import (ManagedPoolError, ManagedPoolUnknown, create_pool,
                                destroy_pool, query_pool_operation, shutdown_pool_service)

U, M = 65536, 1024 * 1024


class Probe:
    def __init__(self, args):
        self.args, self.peers, self.events = args, [], []
        self.root = Path(args.root)
        self.root.mkdir(mode=0o700, parents=False, exist_ok=False)
        self.state = self.root / 'state'
        self.endpoint = str(self.root / 'u.sock')
        assert len(self.endpoint.encode()) < 108
        self.event_file = (self.root / 'events.jsonl').open('x')
        self.daemon_log = (self.root / 'daemon.log').open('xb')
        self.inventory_root = self.root / 'inventories'
        self.inventory_root.mkdir(mode=0o700)
        os.environ['UFLOW_POOL_TEST_INVENTORY_ROOT'] = str(self.inventory_root)
        self.record('controller-fence-bindings', binding_proof())
        env = dict(os.environ)
        if args.case == 'release-fault':
            env['UFLOW_POOL_TEST_HOLD_RELEASE_ONCE'] = '1'
        if args.case == 'reply-loss':
            env['UFLOW_POOL_TEST_DROP_REPLY_ONCE'] = 'CommitPoolWrite'
        if args.case == 'control-timeout':
            env['UFLOW_POOL_TEST_DELAY_SECOND_CREATE'] = '1'
        self.daemon = subprocess.Popen([args.daemon, '--host-pool', '--socket', self.endpoint,
                                       '--pool-state-root', str(self.state), '--pool-provider-python', sys.executable],
                                      env=env, stdout=self.daemon_log, stderr=subprocess.STDOUT)
        self.record('daemon-start', {'pid': self.daemon.pid, 'binary_sha256': hashlib.sha256(Path(args.daemon).read_bytes()).hexdigest()})
        deadline = time.monotonic() + 30
        while not Path(self.endpoint).exists():
            if self.daemon.poll() is not None or time.monotonic() > deadline:
                raise RuntimeError('host-only startup failed/bounded timeout; inspect daemon.log')
            time.sleep(0.05)
        self.token = (self.state / 'admin.token').read_text()
        state = json.loads((self.state / 'state.json').read_text())
        self.owned = {self.daemon.pid, state['provider_loaded']['pid']}
        self.provider_pid = state['provider_loaded']['pid']
        assert state['provider_loaded']['simpler_sha256'] == BUFFER_SHA
        assert len(state['provider_loaded']['test_fence_bindings']) == 79
        assert len(state['provider_loaded']['test_daemon_fence_bindings']) == 79
        self.check_processes('initial')

    def check_processes(self, phase):
        roles = {self.daemon.pid: 'daemon', self.provider_pid: 'provider'}
        for name, process, _ in self.peers:
            if process.poll() is None:
                roles[process.pid] = 'client'
        for parent in list(roles):
            path = Path('/proc') / str(parent) / 'task' / str(parent) / 'children'
            for pid in map(int, path.read_text().split()):
                roles.setdefault(pid, 'tracker' if parent == self.provider_pid else 'child')
        for pid, role in roles.items():
            self.owned.add(pid)
            proof = self.record('process-guard-' + phase, save_guard(pid, role, phase))
            if role == 'daemon':
                assert proof['exe_sha256'] == hashlib.sha256(Path(self.args.daemon).read_bytes()).hexdigest()

    def record(self, name, value):
        event = {'index': len(self.events), 'monotonic_ns': time.monotonic_ns(), 'event': name, 'value': value}
        self.events.append(event)
        self.event_file.write(json.dumps(event, separators=(',', ':')) + '\n')
        self.event_file.flush()
        return value

    def peer(self, descriptor, name, access='rw'):
        log = (self.root / (name + '.stderr')).open('xb')
        process = subprocess.Popen([sys.executable, '-u', str(Path(__file__).with_name('client_peer.py')),
                                    '--endpoint', self.endpoint, '--pool-id', descriptor['pool_id'],
                                    '--epoch', descriptor['pool_epoch'], '--access', access],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log, text=True, bufsize=1)
        self.peers.append((name, process, log))
        ready = self.record(name + '-ready', read_line(process.stdout))
        assert ready['ready'] is True and ready['dev'] == descriptor['canonical']['dev']
        assert ready['inode'] == descriptor['canonical']['inode']
        self.check_processes('ready-' + name)
        return name, process, log

    def call(self, peer, op, *, error=None, **values):
        name, process, _ = peer
        request = {'op': op, **values}
        process.stdin.write(json.dumps(request) + '\n')
        process.stdin.flush()
        result = self.record(name + ':' + op, read_line(process.stdout, 30))
        if error:
            assert not result['ok'] and result['error_type'] in error, result
            return result
        assert result['ok'], result
        if op == 'child':
            self.owned.add(result['result']['birth']['pid'])
            self.check_processes('held-child-' + name)
        return result['result']

    def reject(self, name, action, unknown=False):
        try:
            action()
        except ManagedPoolError as error:
            assert isinstance(error, ManagedPoolUnknown) == unknown
            return self.record(name, {'type': type(error).__name__, 'operation_id': error.operation_id, 'detail': str(error)})
        raise AssertionError('expected explicit rejection: ' + name)

    def snapshot(self, peer, name, charge):
        snapshot = self.call(peer, 'snapshot')
        self.check_processes('snapshot-' + name)
        assert snapshot['charged_bytes'] == charge and snapshot['creation_reserved_bytes'] == 0
        assert snapshot['native_jobs_executed'] == 0 and snapshot['payload_rpc_bytes'] == 0
        self.record(name, snapshot)
        return snapshot

    def acquire(self, peer, block, key, mode, offset=0, nbytes=None, **values):
        return self.call(peer, 'acquire', block=block, key=key, mode=mode, offset=offset, nbytes=nbytes, **values)

    def write(self, peer, block, key, seed):
        self.acquire(peer, block, key, 'w')
        proof = self.call(peer, 'write', key=key, seed=seed)
        self.call(peer, 'commit', key=key, content_id=proof['sha256'])
        return proof

    def verify(self, peer, block, key, seed):
        self.acquire(peer, block, key, 'r')
        proof = self.call(peer, 'verify', key=key, seed=seed)
        self.call(peer, 'release', key=key)
        return proof

    def close_peer(self, peer, native_drained=False):
        self.call(peer, 'begin')
        self.call(peer, 'close')
        self.call(peer, 'ack', native_drained=native_drained)

    def exit_peer(self, peer):
        self.call(peer, 'exit')
        assert peer[1].wait(timeout=15) == 0
        peer[1].stdin.close()
        peer[1].stdout.close()
        peer[2].close()

    def normal(self, pool):
        a, b, c = [self.peer(pool, name) for name in ('a', 'b', 'c')]
        layout = []
        for index in range(5):
            layout.append((self.call(a, 'allocate', nbytes=2*M), 130 + index, 'guard'))
            layout.append((self.call(a, 'allocate', nbytes=20*M), 20 + index, 'payload'))
        layout.append((self.call(a, 'allocate', nbytes=18*M), 140, 'guard'))
        block = layout[1][0]
        assert [item[0]['offset'] for item in layout if item[2] == 'payload'] == [2*M, 24*M, 46*M, 68*M, 90*M]
        self.acquire(a, block, 'a-held', 'w', 0, 2*U)
        self.acquire(b, block, 'b-held', 'w', 2*U, 2*U)
        before = self.snapshot(a, 'ab-held-before-c', 128*M)
        held = {lease['lease_id'] for lease in before['pools'][0]['ranges']['leases']}
        self.acquire(c, block, 'c-write', 'w', 4*U, U)
        for mode in ('r', 'w'):
            self.acquire(c, block, 'overlap', mode, U, 2*U, error=('ManagedPoolError',))
        self.acquire(c, block, 'writing-unreadable', 'r', 4*U, U, error=('ManagedPoolError',))
        c_proof = self.call(c, 'write', key='c-write', seed=77)
        self.call(c, 'commit', key='c-write', content_id=c_proof['sha256'])
        r1 = self.acquire(c, block, 'r1', 'r', 4*U, U)
        r2 = self.acquire(b, block, 'r2', 'r', 4*U, U)
        assert r1['content_version'] == r2['content_version'] > 0
        self.call(c, 'verify', key='r1', seed=77)
        self.acquire(c, block, 'adjacent', 'w', 5*U, U)
        adjacent = self.call(c, 'write', key='adjacent', seed=78)
        self.call(c, 'commit', key='adjacent', content_id=adjacent['sha256'])
        self.call(b, 'verify', key='r2', seed=77)
        after = self.snapshot(c, 'ab-still-held-after-c-actual-write-read', 128*M)
        live = {lease['lease_id']: lease for lease in after['pools'][0]['ranges']['leases']}
        assert held <= live.keys() and all(live[key]['state'] == 'Writing' for key in held)
        self.call(a, 'identity')
        self.call(b, 'identity')
        self.call(c, 'release', key='r1')
        self.call(b, 'release', key='r2')
        self.call(a, 'release', key='a-held')  # These writers never wrote any byte.
        self.call(b, 'release', key='b-held')
        self.acquire(a, block, 'stale-gen', 'w', generation=2, error=('ManagedPoolError',))
        stale = dict(block, pool_epoch=str(uuid.uuid4()))
        self.acquire(a, stale, 'stale-epoch', 'w', error=('ManagedPoolError',))
        self.acquire(a, block, 'bounds', 'w', block['nbytes'], U, error=('ValueError',))
        expected = []
        for index, (item, seed, kind) in enumerate(layout):
            expected.append(self.write(a, item, 'all-' + str(index), seed))
        for index, (item, seed, _) in enumerate(layout):
            assert self.verify(b, item, 'read-' + str(index), seed) == expected[index]
        self.reject('destroy-with-live-views-rejected', lambda: destroy_pool(self.endpoint, pool, admin_token=self.token))
        extra = create_pool(self.endpoint, admin_token=self.token, nbytes=128*M)
        self.snapshot(a, 'two-resources-full-budget', 256*M)
        self.reject('global-budget-overflow', lambda: create_pool(self.endpoint, admin_token=self.token, nbytes=U))
        self.record('extra-resource-destroy', destroy_pool(self.endpoint, extra, admin_token=self.token))
        self.snapshot(a, 'single-resource-not-charged-per-map-or-range', 128*M)
        self.acquire(c, block, 'uncertain', 'w', 7*U, U)
        self.call(c, 'unknown', key='uncertain')
        self.call(c, 'release', key='uncertain', error=('ManagedPoolUnknown', 'ManagedPoolError', 'RuntimeError'))
        self.acquire(a, block, 'unknown-read', 'r', 7*U, U, error=('ManagedPoolError',))
        self.call(c, 'child')
        self.call(c, 'detach_live', error=('ManagedPoolError',))
        self.call(c, 'close', error=('RuntimeError',))
        self.call(c, 'drain_child')
        self.call(c, 'export')
        close_failure = self.call(c, 'close', error=('BufferError',))
        assert close_failure['view_closed'] is False
        self.call(c, 'identity')
        self.snapshot(a, 'real-export-close-failure-obligations-retained', 128*M)
        self.call(c, 'drop_export')
        self.close_peer(c, native_drained=True)
        self.acquire(a, block, 'invalid-after-explicit-drain', 'r', 7*U, U, error=('ManagedPoolError',))
        expected[1] = self.write(a, block, 'repair-full-block', layout[1][1])
        self.exit_peer(c)
        self.close_peer(b)
        self.close_peer(a)
        snap = self.snapshot(a, 'all-original-clients-unmapped-pool-still-owned', 128*M)
        assert all(client['state'] == 'Unmapped' and not client['children'] for client in snap['pools'][0]['clients'])
        assert not snap['pools'][0]['ranges']['leases']
        d = self.peer(pool, 'd', 'r')
        self.acquire(d, block, 'readonly-write', 'w', error=('ManagedPoolError',))
        for index, (item, seed, _) in enumerate(layout):
            assert self.verify(d, item, 'persisted-' + str(index), seed) == expected[index]
        assert sum(proof['nbytes'] for proof in expected) == 128*M
        self.record('full-payload-and-all-guards', {'blocks': expected, 'verified_bytes': 128*M})
        self.close_peer(d)
        self.exit_peer(d)
        for peer in (b, a):
            self.exit_peer(peer)

    def fault(self, pool):
        peer = self.peer(pool, 'fault-client')
        block = self.call(peer, 'allocate', nbytes=U)
        self.acquire(peer, block, 'write', 'w')
        proof = self.call(peer, 'write', key='write', seed=33)
        if self.args.case == 'reply-loss':
            operation = str(uuid.uuid4())
            lost = self.call(peer, 'commit', key='write', content_id=proof['sha256'], operation_id=operation,
                             error=('ManagedPoolUnknown',))
            assert lost['operation_id'] == operation and 'Active' in lost['local_lease_states'].values()
            queried = self.call(peer, 'query', operation_id=operation)
            assert queried['state'] == 'Completed'
            self.call(peer, 'commit', key='write', content_id=proof['sha256'], operation_id=operation)
        else:
            self.call(peer, 'commit', key='write', content_id=proof['sha256'])
        assert self.verify(peer, block, 'read', 33) == proof
        self.close_peer(peer)
        self.exit_peer(peer)
        if self.args.case == 'release-fault':
            first = self.reject('real-provider-close-failed', lambda: destroy_pool(self.endpoint, pool, admin_token=self.token), True)
            queried = query_pool_operation(self.endpoint, first['operation_id'])
            assert queried['state'] == 'Unknown'
            state = json.loads((self.state / 'state.json').read_text())
            assert state['charged_bytes'] == pool['nbytes'] and state['pools'][0]['state'] == 'ReleaseUnknown'
            self.record('close-unknown-still-charged', state)

    def timeout(self, pool):
        peer = self.peer(pool, 'fault-client')
        block = self.call(peer, 'allocate', nbytes=U)
        operation = str(uuid.uuid4())
        timed_out = self.reject('actual-create-control-timeout', lambda: create_pool(
            self.endpoint, admin_token=self.token, nbytes=8*M, operation_id=operation, timeout=0.05), True)
        assert timed_out['operation_id'] == operation
        pending = query_pool_operation(self.endpoint, operation)
        assert pending['state'] == 'Pending'
        self.record('provider-wait-still-pending-before-unrelated-payload', pending)
        proof = self.write(peer, block, 'during-provider-wait', 44)
        assert self.verify(peer, block, 'read-during-provider-wait', 44) == proof
        pending = query_pool_operation(self.endpoint, operation)
        assert pending['state'] == 'Pending', 'probe must demonstrate overlap with the actual executor wait'
        snapshot = self.call(peer, 'snapshot')
        assert snapshot['charged_bytes'] == snapshot['creation_reserved_bytes'] == 8*M
        self.record('provider-wait-did-not-hold-catalog', {'pending': pending, 'snapshot': snapshot})
        deadline = time.monotonic() + 10
        while True:
            queried = query_pool_operation(self.endpoint, operation)
            if queried['state'] == 'Completed':
                break
            assert queried['state'] == 'Pending' and time.monotonic() < deadline
            time.sleep(0.05)
        extra = queried['response']['result']
        self.record('timeout-resource-eventually-confirmed', extra)
        self.record('timeout-extra-resource-destroy', destroy_pool(self.endpoint, extra, admin_token=self.token))
        self.close_peer(peer)
        self.exit_peer(peer)

    def finish(self, pool):
        self.check_processes('before-destroy')
        self.record('real-primary-resource-destroy', destroy_pool(self.endpoint, pool, admin_token=self.token))
        state = json.loads((self.state / 'state.json').read_text())
        assert state['charged_bytes'] == state['creation_reserved_bytes'] == 0
        loaded = state['provider_loaded']
        self.record('provider-loaded', loaded)
        self.check_processes('before-shutdown')
        self.record('shutdown-control', shutdown_pool_service(self.endpoint, admin_token=self.token))
        assert self.daemon.wait(timeout=30) == 0
        self.daemon_log.close()
        clean = json.loads((self.state / 'clean.json').read_text())
        assert clean['normal_shutdown'] is True and clean['provider_exited'] is True
        assert clean['charged_bytes'] == clean['creation_reserved_bytes'] == 0
        assert not Path(self.endpoint).exists() and not (self.state / 'provider.sock').exists()
        with (self.state / 'authority.lock').open('rb') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(lock, fcntl.LOCK_UN)
        for name, process, _ in self.peers:
            assert process.poll() == 0, name
        # Poll only owned OS process existence after confirmed clean shutdown;
        # this is not a native completion or range-release mechanism.
        deadline = time.monotonic() + 5
        while any((Path('/proc') / str(pid)).exists() for pid in self.owned) and time.monotonic() < deadline:
            time.sleep(0.05)
        assert not any((Path('/proc') / str(pid)).exists() for pid in self.owned)
        logs = [self.root / 'daemon.log', self.state / 'provider.stderr',
                *self.root.glob('*.stderr')]
        assert not any(b'UFLOW_P1_FORBIDDEN_API:' in path.read_bytes() for path in logs)
        self.record('normal-postcheck', {'daemon_exit': 0, 'provider_exited': True,
                                       'client_exits': {name: process.returncode for name, process, _ in self.peers},
                                       'lock_free': True, 'sockets_absent': True, 'charged_bytes': 0,
                                       'owned_provider_daemon_tracker_pids_absent': sorted(self.owned)})
        (self.root / 'result.json').write_text(json.dumps({'passed': True, 'case': self.args.case,
            'scope': 'real host/Simpler pool only; no model or native jobs', 'events': len(self.events)}, indent=2))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--daemon', required=True)
    parser.add_argument('--root', required=True)
    parser.add_argument('--case', choices=('normal', 'release-fault', 'reply-loss', 'control-timeout'), required=True)
    args = parser.parse_args()
    probe = Probe(args)
    try:
        pool = create_pool(probe.endpoint, admin_token=probe.token, nbytes=128*M if args.case == 'normal' else 8*M)
        probe.record('pool-created', pool)
        provider_pid = json.loads((probe.state / 'state.json').read_text())['provider_loaded']['pid']
        children = Path('/proc') / str(provider_pid) / 'task' / str(provider_pid) / 'children'
        probe.owned.update(int(pid) for pid in children.read_text().split())
        probe.record('owned-pids-after-real-create', sorted(probe.owned))
        if args.case == 'normal':
            probe.normal(pool)
        elif args.case == 'control-timeout':
            probe.timeout(pool)
        else:
            probe.fault(pool)
        probe.finish(pool)
    except BaseException as error:
        probe.record('FAILED-responsibilities-preserved', {'type': type(error).__name__, 'detail': str(error),
            'daemon_pid': probe.daemon.pid, 'daemon_exit': probe.daemon.poll(),
            'client_pids': {name: process.pid for name, process, _ in probe.peers},
            'note': 'No forced teardown, no automatic restart or unknown-account clearing'})
        raise
    finally:
        probe.event_file.close()


if __name__ == '__main__':
    main()
