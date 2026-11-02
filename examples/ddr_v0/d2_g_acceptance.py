#!/usr/bin/env python3
"""Explicit normal-only G deployment acceptance; never dirty-inject or recover.

Bootstrap is opt-in and only for a literally absent root. Existing deployments
require the previously recorded bootstrap digest and exact frozen configuration.
All clients/handlers/workers must drain before each clean stop. Unknown state
is preserved, not repaired. Use only with authorized deployment ownership.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import select
import stat
import subprocess
import sys
import time

import t3_normal_authority_probe as wire

ROOT = Path('/workspace/uflow/authority/external-ddr-v0')
ENDPOINT = '/workspace/uflow/runs/external-ddr-v0/u.sock'
OWNER_LIMIT = 256 * 1024 * 1024
TOTAL_LIMIT = 512 * 1024 * 1024
ZERO_KEYS = ('committed_bytes', 'backing_fds', 'owner_leases', 'active_views',
             'pending_handoffs', 'in_flight')


def digest(path):
    with path.open('rb') as file:
        return hashlib.file_digest(file, 'sha256').hexdigest()


def metadata(path):
    value = path.lstat()
    return {'device': value.st_dev, 'inode': value.st_ino, 'mode': oct(value.st_mode),
            'uid': value.st_uid, 'links': value.st_nlink}


def snapshot_root():
    return {name: {'identity': metadata(ROOT / name), 'sha256': digest(ROOT / name)}
            for name in wire.FILES}


def control(op, **values):
    data = (';'.join(f'{key}={value}' for key, value in {'op': op, **values}.items()) + '\n').encode()
    response = wire.control(ENDPOINT, data).decode()
    fields = dict(part.split('=', 1) for part in response.strip().split(';') if '=' in part)
    assert fields.get('status') == 'ok', fields
    return fields


class Peer:
    def __init__(self, args, output, seed, legacy=False):
        self.log = (output / f'ordinary-{seed}.log').open('x')
        command = [str(args.python), str(Path(__file__).with_name('d2_owner_peer.py')),
                   '--endpoint', ENDPOINT, '--seed', str(seed)]
        if legacy:
            command += ['--legacy-sdk', str(args.legacy_sdk)]
        self.proc = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=self.log, text=True)
        self.start = self.receive()

    def send(self, op, **values):
        self.proc.stdin.write(json.dumps({'op': op, **values}) + '\n')
        self.proc.stdin.flush()

    def receive(self):
        if not select.select([self.proc.stdout], [], [], 90)[0]:
            raise TimeoutError(f'ordinary peer {self.proc.pid}')
        line = self.proc.stdout.readline()
        if not line:
            raise RuntimeError(f'ordinary peer exited {self.proc.pid}: {self.proc.poll()}')
        return json.loads(line)

    def command(self, op, **values):
        self.send(op, **values)
        result = self.receive()
        assert result.get('ok'), result
        return result['value']

    def close(self):
        if self.proc.poll() is None:
            self.command('cleanup')
            self.proc.stdin.close()
        assert self.proc.wait(timeout=30) == 0
        self.log.close()


def same_counts(before, after):
    for key in ZERO_KEYS:
        assert before[key] == after[key], (key, before[key], after[key])


def ordinary(args, output, client, old):
    output.mkdir(mode=0o700)
    peers, record = [], {}
    try:
        # A uses the real archived T1 six-op SDK; B uses normal uflow package.
        a = Peer(args, output, 11, legacy=True); peers.append(a)
        b = Peer(args, output, 29); peers.append(b)
        record['owners'] = [a.start, b.start]
        record['a_reads_b'] = a.command('read', handle=b.start['handle'], seed=29)
        record['b_reads_a'] = b.command('read', handle=a.start['handle'], seed=11)
        if old:
            fresh = record['a_reads_b']['tuple']
            previous = old['a_reads_b']['tuple']
            assert all(fresh[key] == previous[key] for key in ('object_id', 'view_id', 'lease_id'))
            assert fresh['broker_instance'] != previous['broker_instance']
            before = client.snapshot()
            record['old_legacy_sdk'] = a.command('old', tuple=previous,
                                               fields=old['a_reads_b']['fields'], seed=29)
            record['old_light_sdk'] = b.command('old', tuple=old['b_reads_a']['tuple'],
                                              fields=old['b_reads_a']['fields'], seed=11)
            after = client.snapshot()
            same_counts(before, after)
            record['collision'] = {'old': previous, 'fresh': fresh,
                                   'before': before, 'after': after}
        # Four genuine owner processes compete for the SAME host cap. Large
        # memfds are sparse (8KiB initialized), not a second pool or simulated cap.
        c = Peer(args, output, 41); peers.append(c)
        d = Peer(args, output, 59); peers.append(d)
        record['budget_peers'] = [c.start, d.start]
        a.command('register', size=OWNER_LIMIT - 8192)
        b.command('register', size=TOTAL_LIMIT - OWNER_LIMIT - 3 * 8192 - 4096)
        full_minus_one = client.snapshot()
        assert full_minus_one['committed_bytes'] == TOTAL_LIMIT - 4096
        a.send('register', size=4096)
        owner_denied = a.receive()
        assert not owner_denied['ok'] and 'quota exceeded' in owner_denied['error'], owner_denied
        same_counts(full_minus_one, client.snapshot())
        at = time.monotonic() + 0.1
        c.send('register', size=4096, at=at)
        d.send('register', size=4096, at=at)
        replies = [c.receive(), d.receive()]
        assert sum(bool(reply['ok']) for reply in replies) == 1, replies
        assert 'quota exceeded' in next(reply['error'] for reply in replies if not reply['ok'])
        full = client.snapshot()
        assert full['committed_bytes'] == TOTAL_LIMIT and full['pending_handoffs'] == 0
        winner = c if replies[0]['ok'] else d
        winner.command('revoke_last')
        same_counts(full_minus_one, client.snapshot())
        a.command('revoke_last'); b.command('revoke_last')
        record['budget'] = {'before_race': full_minus_one, 'owner_rejection': owner_denied,
                            'race': replies, 'at_cap': full, 'rolled_back': client.snapshot()}
    finally:
        errors = []
        # All cross-owner views must be legally released before ANY owner
        # exits. A revoked owner exiting while another accessor still retains
        # its mapping is correctly treated as unknown exposure, not clean.
        for peer in peers:
            try:
                peer.command('release_views')
            except Exception as error:
                errors.append(repr(error))
        for peer in reversed(peers):
            try:
                peer.close()
            except Exception as error:
                errors.append(repr(error))
        record['participants'] = [{'pid': peer.proc.pid, 'exit': peer.proc.poll()} for peer in peers]
        record['cleanup_errors'] = errors
        (output / 'result.json').write_text(json.dumps(record, indent=2) + '\n')
        if errors:
            raise RuntimeError(errors)
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-dir', required=True, type=Path)
    parser.add_argument('--daemon', required=True, type=Path)
    parser.add_argument('--shim', required=True, type=Path)
    parser.add_argument('--python', required=True, type=Path)
    parser.add_argument('--legacy-sdk', required=True, type=Path)
    parser.add_argument('--host-id', required=True)
    parser.add_argument('--bootstrap-if-absent', action='store_true')
    parser.add_argument('--expected-bootstrap-sha256')
    args = parser.parse_args()
    parent = args.run_dir.parent.resolve(strict=True)
    assert parent != wire.BASE and parent.is_relative_to(wire.BASE)
    assert args.run_dir.parent == parent
    args.run_dir.mkdir(mode=0o700)
    output = args.run_dir
    env = dict(os.environ, UF_EXTERNAL_DDR_HOST_ID=args.host_id,
               UF_EXTERNAL_DDR_OWNER_MAX_BYTES=str(OWNER_LIMIT),
               UF_EXTERNAL_DDR_TOTAL_MAX_BYTES=str(TOTAL_LIMIT), UF_HBM_STARTUP_PROBE_BYTES='0',
               LD_LIBRARY_PATH=f'{args.shim.parent}:/usr/local/Ascend/cann-9.0.0/lib64',
               PYTHONDONTWRITEBYTECODE='1')
    report = {'schema': 'uflow-ddr-d2-normal-v1', 'endpoint': ENDPOINT,
              'root': str(ROOT), 'host': args.host_id,
              'boot': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
              'owner_limit': OWNER_LIMIT, 'total_limit': TOTAL_LIMIT,
              'daemon_sha256': digest(args.daemon), 'shim_sha256': digest(args.shim),
              'daemons': [], 'commands': [], 'generations': []}
    initial_mounts = Path('/proc/self/mountinfo').read_bytes()
    initial_workspace = metadata(Path('/workspace/uflow'))
    daemon = None
    try:
        exists = os.path.lexists(ROOT)
        report['root_initially_exists'] = exists
        (output / 'initial.json').write_text(json.dumps(report, indent=2))
        endpoint_dir = Path(ENDPOINT).parent
        if exists:
            assert not args.bootstrap_if_absent, 'existing root must not be bootstrapped'
            assert args.expected_bootstrap_sha256 and digest(ROOT / 'bootstrap') == args.expected_bootstrap_sha256
            state = json.loads((ROOT / 'state').read_text())
            assert state['mode'] == 'clean' and state['closure'] in ('normal', 'genesis')
        else:
            assert args.bootstrap_if_absent and not args.expected_bootstrap_sha256
            if not os.path.lexists(ROOT.parent):
                ROOT.parent.mkdir(mode=0o700)
                fd = os.open(ROOT.parent.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            parent_stat = ROOT.parent.lstat()
            assert stat.S_ISDIR(parent_stat.st_mode) and parent_stat.st_uid == os.geteuid()
            assert not parent_stat.st_mode & 0o022
            assert ROOT.parent.resolve() == ROOT.parent
        if not os.path.lexists(endpoint_dir):
            endpoint_dir.mkdir(mode=0o700)
        endpoint_stat = endpoint_dir.lstat()
        assert endpoint_dir.resolve() == endpoint_dir and stat.S_ISDIR(endpoint_stat.st_mode)
        assert endpoint_stat.st_uid == os.geteuid() and not endpoint_stat.st_mode & 0o077
        assert not os.path.lexists(ENDPOINT) and not os.path.lexists(ENDPOINT + '.external-ddr')
        if not exists:
            command = [str(args.daemon), '--socket', ENDPOINT, '--initialize-external-ddr-authority']
            attempt = subprocess.run(command, env=env, capture_output=True, timeout=30)
            (output / 'bootstrap.log').write_bytes(attempt.stdout + attempt.stderr)
            report['commands'].append({'argv': command, 'exit': attempt.returncode})
            assert attempt.returncode == 0, attempt.stderr
        manifest = (ROOT / 'bootstrap').read_text()
        assert manifest.startswith('uflow-ddr-authority-v6\n')
        fields = dict(line.split('=', 1) for line in manifest.splitlines()[1:])
        assert fields['host'] == args.host_id and fields['boot'] == report['boot']
        assert fields['endpoint'] == ENDPOINT and fields['revision'] == '1'
        assert int(fields['owner_limit']) == OWNER_LIMIT and int(fields['total_limit']) == TOTAL_LIMIT
        report['bootstrap'] = manifest
        pinned = snapshot_root()
        report['after_bootstrap'] = pinned
        from uflow import ExternalDdrClient
        client = ExternalDdrClient(ENDPOINT, timeout=30)
        old = None
        for generation in range(2):
            logpath = output / f'daemon-{generation}.log'
            with logpath.open('x') as log:
                command = [str(args.daemon), '--socket', ENDPOINT, '--execution-mode', 'simulated',
                           '--startup-probe-bytes', '0']
                daemon = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
                row = {'pid': daemon.pid, 'argv': command}
                report['daemons'].append(row)
                deadline = time.monotonic() + 30
                while not Path(ENDPOINT + '.external-ddr').exists():
                    assert daemon.poll() is None, logpath.read_text()
                    assert time.monotonic() < deadline
                    time.sleep(0.02)
                lock = os.open(ROOT / 'authority.lock', os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
                try:
                    try:
                        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError:
                        pass
                    else:
                        raise AssertionError('broker did not hold original lock')
                    before = snapshot_root()
                    socket_id = metadata(Path(ENDPOINT))
                    contender = subprocess.run(command, env=env, capture_output=True, timeout=30)
                    (output / f'contender-{generation}.log').write_bytes(contender.stdout + contender.stderr)
                    assert contender.returncode != 0 and daemon.poll() is None
                    assert snapshot_root() == before and metadata(Path(ENDPOINT)) == socket_id
                    row['second_broker_exit'] = contender.returncode
                    row['lock_held'] = metadata(ROOT / 'authority.lock')
                finally:
                    os.close(lock)
                control('StartTrace', run_id=f'd2-{generation}', mode='timeline', include_args='true',
                        output_dir=str(output / f'trace-{generation}'))
                group = ordinary(args, output / f'ordinary-{generation}', client, old)
                old = group
                for key in ZERO_KEYS:
                    assert client.snapshot()[key] == 0, client.snapshot()
                # Reuse the already SE-approved real business clients/entry.
                command = [str(args.python), str(Path(__file__).with_name('t34_public_probe.py')),
                           '--endpoint', ENDPOINT, '--output', str(output / f'public-{generation}'),
                           '--cases', 'normal', 'large', 'cancel', 'failure']
                with (output / f'public-{generation}.log').open('x') as business_log:
                    public = subprocess.run(command, env=env, stdout=business_log, stderr=subprocess.STDOUT, timeout=240)
                report['commands'].append({'argv': command, 'exit': public.returncode})
                assert public.returncode == 0
                zero = client.snapshot()
                assert all(zero[key] == 0 for key in ZERO_KEYS), zero
                stats = control('GetStats')
                for key in ('external_ddr_committed_bytes', 'external_unfinalized_workers',
                            'external_cleanup_unconfirmed'):
                    assert stats[key] == '0', (key, stats)
                trace = control('ExportTrace')
                assert trace['trace_flushed'] == 'true'
                row['zero_snapshot'] = zero; row['stats'] = stats; row['trace'] = trace
                control('ShutdownDaemon')
                row['exit'] = daemon.wait(timeout=30)
                assert row['exit'] == 0 and 'external DDR authority drained and clean' in logpath.read_text()
                assert not os.path.lexists(ENDPOINT) and not os.path.lexists(ENDPOINT + '.external-ddr')
                daemon = None
                state = json.loads((ROOT / 'state').read_text())
                assert state['mode'] == 'clean' and state['closure'] == 'normal'
                current = snapshot_root()
                assert all(current[name]['identity'] == pinned[name]['identity'] for name in wire.FILES)
                assert current['bootstrap']['sha256'] == pinned['bootstrap']['sha256']
                lock = os.open(ROOT / 'authority.lock', os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    row['original_lock_released'] = True
                finally:
                    os.close(lock)
                row['clean_state'] = state
                report['generations'].append({'root_files': current})
        final_workspace = metadata(Path('/workspace/uflow'))
        # Provisioning the absent authority directory legitimately adds one
        # subdirectory link; it must not replace the workspace inode/mount.
        assert all(final_workspace[key] == initial_workspace[key]
                   for key in ('device', 'inode', 'mode', 'uid'))
        assert Path('/proc/self/mountinfo').read_bytes() == initial_mounts
        report['mountinfo_unchanged'] = True
        report['final_root'] = snapshot_root()
        report['status'] = 'pass'
    finally:
        if daemon is not None and daemon.poll() is None:
            # Never mask failed cleanup or turn a live-holder stop into success.
            try:
                control('ShutdownDaemon')
                report['exception_shutdown_exit'] = daemon.wait(timeout=30)
            except Exception as error:
                report['cleanup_error'] = repr(error)
        (output / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'status': report['status'], 'result': str(output / 'result.json')}))


if __name__ == '__main__':
    main()
