#!/usr/bin/env python3
"""Public DDR/standard-library SDK scenarios. Normal group never dirties G.

Use --endpoint with an already deployed broker for reusable ordinary business
scenarios, or --run-dir/--daemon/--shim for an isolated private namespace and
clean-restart scenario. The outer mount guard is the already reviewed T3.3 one.
"""
from __future__ import annotations
import argparse
import dataclasses
import fcntl
import importlib.util
import json
import mmap
import os
from pathlib import Path
import select
import shutil
import subprocess
import sys
import time
import traceback

import t3_normal_authority_probe as isolation

RUN = Path('/workspace/uflow/runs/public-probe')
ROOT = Path('/workspace/uflow/authority/external-ddr-v0')
SRC_OFFSET, DST_OFFSET, COUNT = 31, 173, 4096


def fill(fd, size, value):
    chunk = bytes([value]) * 65536
    for offset in range(0, size, len(chunk)):
        assert os.pwrite(fd, chunk[:min(len(chunk), size - offset)], offset) == min(len(chunk), size - offset)


def exact_target(fd, size, seed, payload=None):
    # Full deterministic byte comparison, not a digest or sampled neighbours.
    for offset in range(0, size, 65536):
        length = min(65536, size - offset)
        expected = bytearray([seed]) * length
        if payload is not None:
            a, b = max(offset, DST_OFFSET), min(offset + length, DST_OFFSET + COUNT)
            if a < b:
                expected[a - offset:b - offset] = payload[a - DST_OFFSET:b - DST_OFFSET]
        assert os.pread(fd, length, offset) == expected, (offset, length)


def worker(endpoint, seed):
    from uflow import ExternalBackingHandle, ExternalDdrClient, ExternalDdrError
    assert sys.prefix != sys.base_prefix, 'clean venv required for the light SDK scenario'
    assert all(importlib.util.find_spec(name) is None for name in ('torch', 'torch_npu', 'serving'))
    assert not any(name in sys.modules for name in ('uflow.client', 'uflow.objects', 'uflow.transfer', 'torch'))
    client = ExternalDdrClient(endpoint, timeout=30)
    def memfd(size, value, write_seal=False):
        fd = os.memfd_create('public-ddr', os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
        os.ftruncate(fd, size)
        fill(fd, size, value)
        fcntl.fcntl(fd, fcntl.F_ADD_SEALS, fcntl.F_SEAL_GROW | fcntl.F_SEAL_SHRINK |
                    (fcntl.F_SEAL_WRITE if write_seal else 0))
        return fd
    def rejected(call):
        try:
            call()
        except (ExternalDdrError, ValueError) as error:
            return str(error)
        raise AssertionError('negative unexpectedly accepted')
    anchor_fd = memfd(8192, seed, True)
    anchor = client.register(anchor_fd, name=f'anchor-{seed}', owner_name=f'owner-{seed}', size_bytes=8192)
    handles, fds = [anchor], [anchor_fd]
    targets = {}
    print(json.dumps({'pid': os.getpid(), 'handle': dataclasses.asdict(anchor), 'python': sys.executable,
                      'prefix': sys.prefix, 'base_prefix': sys.base_prefix, 'torch_absent': True}), flush=True)
    try:
        for line in sys.stdin:
            req = json.loads(line)
            try:
                op = req['op']
                if op == 'read':
                    other = ExternalBackingHandle(**req['source'])
                    with client.acquire(other, offset_bytes=17, nbytes=128) as root:
                        before = client.poll(root.event_id, event_token=root.event_token)
                        assert before['mapped_bytes'] == '128' and before['copied_bytes'] == '0'
                        assert before['accessed_bytes'] == '0' and 'not-read-complete' in before['phase']
                        try:
                            root._mapping[17] = 0
                        except (TypeError, OSError):
                            pass
                        else:
                            raise AssertionError('read-only mapping was writable')
                        write_denied = rejected(lambda: client._view_request('AcquireExternalRead',
                            **other.identity_fields(), access_mode='write', offset_bytes=0, nbytes=1))
                        assert 'only read access' in write_denied
                        assert root.read() == bytes([req['seed']]) * 128
                        view = root.retain(7, 32)
                        assert view.read() == bytes([req['seed']]) * 32
                        read = client.poll(root.event_id, event_token=root.event_token)
                        assert read['accessed_bytes'] == '128' and read['copied_bytes'] == '0'
                        old = view._identity.identity_fields()
                        view.release()
                    value = {'map_before': before, 'map_after_read': read, 'old_tuple': old, 'write_denied': write_denied}
                elif op == 'stage':
                    size = req['size']
                    fd = memfd(size, seed)
                    handle = client.stage(fd, name=f'target-{len(handles)}', owner_name=f'owner-{seed}', size_bytes=size)
                    handles.append(handle)
                    fds.append(fd)
                    targets[handle.object_id] = (fd, size)
                    value = dataclasses.asdict(handle)
                elif op == 'foreign':
                    source, target = ExternalBackingHandle(**req['source']), ExternalBackingHandle(**req['target'])
                    value = {'rejected': rejected(lambda: client.plan(source, target, src_offset_bytes=SRC_OFFSET,
                                              dst_offset_bytes=DST_OFFSET, nbytes=COUNT))}
                elif op == 'copy':
                    source, target = ExternalBackingHandle(**req['source']), ExternalBackingHandle(**req['target'])
                    fd, size = targets[target.object_id]
                    ranges = dict(src_offset_bytes=SRC_OFFSET, dst_offset_bytes=DST_OFFSET, nbytes=COUNT)
                    estimate = client.estimate(source, target, **ranges)
                    assert estimate['cost_version'] == 'unknown' and estimate['automatic_ranking'] == 'disabled'
                    negatives = [rejected(lambda: client.plan(source, target, **{**ranges, 'mode': 'auto'})),
                                 rejected(lambda: client.plan(source, target, **{**ranges, 'src_offset_bytes': 8191})),
                                 rejected(lambda: client.plan(source, target, **{**ranges, 'dst_offset_bytes': size - 1}))]
                    changed = dataclasses.replace(source, resource_generation=source.resource_generation + 1)
                    negatives.append(rejected(lambda: client.plan(changed, target, **ranges)))
                    plan = client.plan(source, target, **ranges)
                    assert plan['cost_version'] == 'unknown' and plan['source_identity_json']
                    # Explicit stale invalidation changes epoch, then Submit must
                    # do no worker/retains/write. The business target stays exact.
                    with client.acquire(source, offset_bytes=0, nbytes=1):
                        pass
                    before_stale = client.snapshot()
                    stale = rejected(lambda: client.submit(int(plan['plan_id']), plan_token=plan['plan_token']))
                    exact_target(fd, size, seed)
                    after_stale = client.snapshot()
                    for key in ('committed_bytes', 'backing_fds', 'active_views', 'pending_handoffs', 'in_flight'):
                        assert before_stale[key] == after_stale[key], key
                    assert after_stale['in_flight'] == 0
                    plan = client.plan(source, target, **ranges)
                    writable = mmap.mmap(fd, size, access=mmap.ACCESS_WRITE) if req.get('failure') else None
                    negatives.append(rejected(lambda: client.submit(int(plan['plan_id']), plan_token='old-wrong-plan-token')))
                    submitted = client.submit(int(plan['plan_id']), plan_token=plan['plan_token'])
                    event_id = int(submitted['event_id'])
                    event_token = submitted['event_token']
                    negatives.append(rejected(lambda: client.cancel(event_id, event_token='old-wrong-event-token')))
                    # The unrelated owner must not be able to reuse this plan.
                    if req.get('cancel'):
                        cancel = client.cancel(event_id, event_token=event_token)
                        assert cancel['cancel_requested'] == 'true'
                    polled = client.poll(event_id, event_token=event_token)
                    done = client.wait(event_id, event_token=event_token, timeout_ms=20_000)
                    if writable is not None:
                        writable.close()
                    assert done['drained'] == 'true' and done['actual_engine'] == 'memfd-pread-pwrite'
                    if req.get('cancel') or req.get('failure'):
                        assert done['event_status'] in ('failed', 'cancelled')
                        rejected(lambda: client.acquire(target))
                        snapshot = client.snapshot()
                        row = next(row for row in snapshot['backings'] if row['object_id'] == target.object_id)
                        assert row['state'] != 'active' and row['reclaimed']
                    else:
                        assert done['event_status'] == 'complete' and done['copied_bytes'] == str(COUNT)
                        assert done['planned_path'] == done['actual_path'] == 'external_ddr_copy'
                        assert done['queue_seconds'] != 'unknown' and done['execute_seconds'] != 'unknown'
                        exact_target(fd, size, seed, bytes([req['seed']]) * COUNT)
                        with client.acquire(target) as view:
                            assert view.read(DST_OFFSET, COUNT) == bytes([req['seed']]) * COUNT
                    value = {'estimate': estimate, 'plan': plan, 'submitted': submitted, 'poll': polled,
                             'done': done, 'stale': stale, 'negative_errors': negatives,
                             'target_size': size, 'target_complement_bytes': size - COUNT,
                             'exact_full_target': not (req.get('cancel') or req.get('failure'))}
                elif op == 'old':
                    value = {'retain': rejected(lambda: client._request('RetainExternalView', **req['tuple'], offset_bytes=0, nbytes=1)),
                             'release': rejected(lambda: client._request('ReleaseExternalView', **req['tuple']))}
                elif op == 'cleanup':
                    for handle in reversed(handles):
                        client.revoke(handle)
                    for fd in fds:
                        os.close(fd)
                    handles.clear()
                    fds.clear()
                    print(json.dumps({'ok': True, 'value': {'cleaned': True}}), flush=True)
                    return
                else:
                    raise ValueError(op)
                print(json.dumps({'ok': True, 'value': value}), flush=True)
            except Exception:
                print(json.dumps({'ok': False, 'error': traceback.format_exc()}), flush=True)
    finally:
        for handle in reversed(handles):
            client.revoke(handle)
        for fd in fds:
            os.close(fd)


class Peer:
    def __init__(self, endpoint, seed, python, output):
        self.log = output.open('x')
        script = RUN / 't34_public_probe.py'
        if not script.is_file():
            script = Path(__file__).resolve()
        self.proc = subprocess.Popen([python, str(script), '--peer', endpoint, str(seed)],
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.log, text=True)
        self.start = self.receive()
    def receive(self):
        assert self.proc.stdout
        if not select.select([self.proc.stdout], [], [], 90)[0]:
            raise TimeoutError(f'peer timeout {self.proc.pid}')
        line = self.proc.stdout.readline()
        if not line:
            raise RuntimeError(f'peer exited {self.proc.pid}: {self.proc.poll()}')
        return json.loads(line)
    def command(self, op, **args):
        self.proc.stdin.write(json.dumps({'op': op, **args}) + '\n')
        self.proc.stdin.flush()
        reply = self.receive()
        if not reply['ok']:
            raise RuntimeError(reply['error'])
        return reply['value']
    def close(self):
        if self.proc.poll() is None:
            self.command('cleanup')
            self.proc.stdin.close()
            assert self.proc.wait(timeout=30) == 0
        self.log.close()


def clients(endpoint, output, python, old=None, cases=None):
    output.mkdir(mode=0o700)
    peers = []
    result = {}
    try:
        a = Peer(endpoint, 37, python, output / 'owner-a.log'); peers.append(a)
        b = Peer(endpoint, 83, python, output / 'owner-b.log'); peers.append(b)
        result['owners'] = [a.start, b.start]
        result['a_reads_b'] = a.command('read', source=b.start['handle'], seed=83)
        result['b_reads_a'] = b.command('read', source=a.start['handle'], seed=37)
        if old:
            result['old_tuple_rejected'] = a.command('old', tuple=old)
        result['copies'] = []
        selected = set(cases or ('normal', 'large', 'cancel', 'failure'))
        for name, size, cancel, failure in (('normal', 12288, False, False), ('large', 20 * 1024 * 1024, False, False),
                                            ('cancel', 12288, True, False), ('failure', 12288, False, True)):
            if name not in selected:
                continue
            target = b.command('stage', size=size)
            result.setdefault('foreign_rejections', []).append(a.command('foreign', source=a.start['handle'], target=target))
            result['copies'].append(b.command('copy', source=a.start['handle'], target=target, seed=37,
                                             cancel=cancel, failure=failure))
    finally:
        errors = []
        for peer in reversed(peers):
            try:
                peer.close()
            except Exception:
                errors.append(traceback.format_exc())
        result['pids'] = [peer.proc.pid for peer in peers]
        result['cleanup_errors'] = errors
        (output / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
        if errors:
            raise RuntimeError(errors)
    assert all(not Path(f'/proc/{pid}').exists() for pid in result['pids'])
    return result


def inner(args):
    isolation.require_isolated_namespace(args.outer_mnt_fd, args.outer_pid)
    nsroot = args.inside.resolve(strict=True)
    assert nsroot == args.inside and nsroot.is_relative_to(isolation.BASE)
    source = os.fstat(args.root_fd)
    assert isolation.identity(nsroot) == (source.st_dev, source.st_ino)
    subprocess.run(['mount', '--bind', str(nsroot), '/workspace/uflow'], check=True)
    assert isolation.identity(Path('/workspace/uflow')) == (source.st_dev, source.st_ino)
    assert not ROOT.exists() and isolation.digest(RUN / 'uf-daemon') == args.binary_sha
    env = dict(os.environ, UF_EXTERNAL_DDR_HOST_ID='isolated-public-host',
               UF_EXTERNAL_DDR_OWNER_MAX_BYTES=str(96 * 1024 * 1024),
               UF_EXTERNAL_DDR_TOTAL_MAX_BYTES=str(256 * 1024 * 1024),
               UF_HBM_STARTUP_PROBE_BYTES='0', PYTHONDONTWRITEBYTECODE='1',
               PYTHONPATH=str(RUN / 'sdk'), LD_LIBRARY_PATH=f'{RUN}/shim:/usr/local/Ascend/cann-9.0.0/lib64')
    os.environ.update(env)
    endpoint = str(RUN / 'u.sock')
    binary = str(RUN / 'uf-daemon')
    python = str(RUN / 'venv/bin/python')
    bootstrap = subprocess.run([binary, '--socket', endpoint, '--initialize-external-ddr-authority'],
                               env=env, capture_output=True, timeout=30)
    assert bootstrap.returncode == 0, bootstrap.stderr
    results = {'bootstrap_exit': bootstrap.returncode, 'binary_sha256': args.binary_sha,
               'namespace_inode': os.stat('/proc/self/ns/mnt').st_ino, 'daemons': []}
    old = None
    for generation in range(2):
        with (RUN / f'daemon-{generation}.log').open('x') as log:
            daemon = subprocess.Popen([binary, '--socket', endpoint, '--execution-mode', 'simulated',
                                       '--startup-probe-bytes', '0'], env=env, stdout=log, stderr=subprocess.STDOUT)
            results['daemons'].append({'pid': daemon.pid})
            try:
                deadline = time.monotonic() + 20
                while not Path(endpoint + '.external-ddr').exists():
                    assert daemon.poll() is None
                    assert time.monotonic() < deadline
                    time.sleep(0.02)
                assert b'status=ok' in isolation.control(endpoint, f'op=StartTrace;run_id=public-{generation};mode=timeline;include_args=true;output_dir={RUN}/trace-{generation}\n'.encode())
                result = clients(endpoint, RUN / f'clients-{generation}', python, old)
                old = result['a_reads_b']['old_tuple']
                stats = isolation.control(endpoint, b'op=GetStats\n').decode()
                (RUN / f'stats-{generation}.txt').write_text(stats)
                expected_copied = sum(int(copy['done']['copied_bytes']) for copy in result['copies'])
                parsed = dict(item.split('=', 1) for item in stats.strip().split(';') if '=' in item)
                assert int(parsed['external_copied_bytes']) == expected_copied
                assert int(parsed['external_mapped_bytes']) > 0
                assert parsed['external_ddr_committed_bytes'] == '0'
                exported = isolation.control(endpoint, b'op=ExportTrace\n').decode()
                fields = dict(item.split('=', 1) for item in exported.strip().split(';') if '=' in item)
                assert fields['status'] == 'ok' and fields['trace_flushed'] == 'true'
                trace = Path(fields['trace_artifact']).read_text()
                assert 'transfer.external_terminal' in trace and 'copied_bytes' in trace and 'mapped_bytes' in trace
            finally:
                if daemon.poll() is None:
                    isolation.control(endpoint, b'op=ShutdownDaemon\n')
                rc = daemon.wait(timeout=30)
                results['daemons'][-1]['exit'] = rc
            assert rc == 0 and not Path(endpoint).exists() and not Path(endpoint + '.external-ddr').exists()
            assert 'external DDR authority drained and clean' in (RUN / f'daemon-{generation}.log').read_text()
    results['root_files'] = {name: {'identity': isolation.identity(ROOT / name), 'sha256': isolation.digest(ROOT / name)} for name in isolation.FILES}
    (RUN / 'result.json').write_text(json.dumps(results, indent=2) + '\n')
    print(json.dumps(results), flush=True)


def outer(args):
    parent = args.run_dir.parent.resolve(strict=True)
    assert parent != isolation.BASE and parent.is_relative_to(isolation.BASE) and args.run_dir.parent == parent
    binary, shim = args.daemon.resolve(strict=True), args.shim.resolve(strict=True)
    assert binary.is_relative_to(parent) and shim.is_relative_to(parent)
    args.run_dir.mkdir(mode=0o700)
    nsroot = args.run_dir / 'nsroot'; nsroot.mkdir(mode=0o700)
    (nsroot / 'authority').mkdir(mode=0o700); (nsroot / 'runs').mkdir(mode=0o700)
    local = nsroot / 'runs/public-probe'; local.mkdir(mode=0o700)
    (local / 'shim').mkdir(mode=0o700)
    shutil.copyfile(binary, local / 'uf-daemon'); os.chmod(local / 'uf-daemon', 0o700)
    shutil.copyfile(shim, local / 'shim/libuf_acl_shim.so')
    source = Path(__file__).resolve()
    shutil.copyfile(source, local / source.name)
    shutil.copyfile(source.with_name('t3_normal_authority_probe.py'), local / 't3_normal_authority_probe.py')
    shutil.copytree(source.parents[2] / 'sdk/python/uflow', local / 'sdk/uflow', ignore=shutil.ignore_patterns('__pycache__'))
    subprocess.run([sys.executable, '-m', 'venv', '--without-pip', str(local / 'venv')], check=True)
    nsfd = os.open('/proc/self/ns/mnt', os.O_RDONLY | os.O_CLOEXEC)
    rootfd = os.open(nsroot, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    before, mounts = isolation.identity(Path('/workspace/uflow')), Path('/proc/self/mountinfo').read_bytes()
    try:
        cmd = ['unshare', '--mount', '--propagation', 'private', sys.executable, str(source),
               '--inside', str(nsroot), '--root-fd', str(rootfd), '--outer-mnt-fd', str(nsfd),
               '--outer-pid', str(os.getpid()), '--binary-sha', isolation.digest(binary)]
        with (args.run_dir / 'outer.log').open('x') as log:
            proc = subprocess.Popen(cmd, pass_fds=(nsfd, rootfd), stdout=log, stderr=subprocess.STDOUT)
            rc = proc.wait(timeout=300)
        after, mounts_after = isolation.identity(Path('/workspace/uflow')), Path('/proc/self/mountinfo').read_bytes()
        (args.run_dir / 'isolation.json').write_text(json.dumps({'outer_pid': os.getpid(), 'child_pid': proc.pid,
            'outer_namespace': os.fstat(nsfd).st_ino, 'before': before, 'after': after,
            'mountinfo_unchanged': mounts == mounts_after, 'exit': rc}, indent=2) + '\n')
        assert before == after and mounts == mounts_after
        assert rc == 0, (args.run_dir / 'outer.log').read_text()
        print(f'public normal/clean-restart exit=0: {args.run_dir}')
    finally:
        os.close(nsfd); os.close(rootfd)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ('run-dir', 'daemon', 'shim', 'inside'):
        parser.add_argument('--' + flag, type=Path)
    for flag in ('root-fd', 'outer-mnt-fd', 'outer-pid'):
        parser.add_argument('--' + flag, type=int)
    parser.add_argument('--binary-sha')
    parser.add_argument('--peer', nargs=2)
    parser.add_argument('--endpoint')
    parser.add_argument('--cases', nargs='+', choices=('normal', 'large', 'cancel', 'failure'))
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    if args.cases and not args.endpoint:
        parser.error('--cases selects deployed endpoint clients only; isolated normal runs the full suite')
    if args.peer:
        worker(args.peer[0], int(args.peer[1]))
    elif args.endpoint:
        if args.output is None:
            parser.error('--output required')
        clients(args.endpoint, args.output, sys.executable, cases=args.cases)
    elif args.inside:
        if any(v is None for v in (args.root_fd, args.outer_mnt_fd, args.outer_pid, args.binary_sha)):
            parser.error('incomplete isolation proof')
        inner(args)
    else:
        if any(v is None for v in (args.run_dir, args.daemon, args.shim)):
            parser.error('--run-dir/--daemon/--shim required')
        outer(args)


if __name__ == '__main__':
    main()
