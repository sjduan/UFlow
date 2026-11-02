#!/usr/bin/env python3
"""Real private DDR regression; NEVER points at the actual G authority.

Run with --run-dir (new task-owned directory), --daemon, --shim and --delay-so.
The existing outer-namespace/inode guard is mandatory before bootstrap/preload.
The production daemon/Rust/SDK are unchanged; the test-only preload delays one
real sysfs read for 5.3 seconds, not the clock value or a synthetic response.
"""
from __future__ import annotations
import argparse
import dataclasses
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import sys
import time
from types import SimpleNamespace

import t3_normal_authority_probe as guard
import t35_calibrate as c

V = Path('/workspace/uflow/runs/t35-regression')
ROOT = Path('/workspace/uflow/authority/external-ddr-v0')
ENDPOINT = str(V / 'u.sock')
ZERO = ('committed_bytes', 'backing_fds', 'owner_leases', 'active_views', 'pending_handoffs', 'in_flight')


def journal():
    data = (ROOT / 'recovery.journal').read_bytes()
    frames, offset, previous = [], 0, 0
    def crc64(buffer):
        crc = 0
        for byte in buffer:
            crc ^= byte << 56
            for _ in range(8):
                crc = ((crc << 1) ^ (0x42f0e1eba9ea3693 if crc & (1 << 63) else 0)) & ((1 << 64) - 1)
        return crc
    while offset < len(data):
        magic, version, kind, sequence, prev, size = struct.unpack_from('>4sHHQQI', data, offset)
        assert magic == b'UDRJ' and version == 1 and sequence == len(frames) + 1 and prev == previous
        payload = data[offset+28:offset+28+size]
        checksum = struct.unpack_from('>Q', data, offset+28+size)[0]
        assert checksum == crc64(data[offset:offset+28+size])
        assert data[offset+36+size:offset+40+size] == b'JEND'
        fields = dict(item.split('=', 1) for item in payload.decode().strip().split(';'))
        assert payload.decode() == ';'.join(k+'='+v for k,v in sorted(fields.items()))+'\n'
        frames.append(dict(sequence=sequence, kind=kind, fields=fields))
        previous, offset = checksum, offset + size + 40
    head = dict(line.split('=', 1) for line in (ROOT / 'recovery.head').read_text().splitlines()[1:])
    assert offset == int(head['offset']) and len(frames) == int(head['sequence'])
    assert f'{previous:016x}' == head['checksum']
    return frames


def control(op, **values):
    raw = guard.control(ENDPOINT, (';'.join(f'{k}={v}' for k,v in {'op': op, **values}.items())+'\n').encode())
    response = dict(item.split('=', 1) for item in raw.decode().strip().split(';') if '=' in item)
    assert response['status'] == 'ok', response
    return response


def exercise(rootfd, outerfd, inner_pid):
    # Only run under the already-guarded driver's inherited private-root FD.
    expected = os.fstat(rootfd)
    assert guard.identity(Path('/workspace/uflow')) == (expected.st_dev, expected.st_ino)
    assert os.getppid() == inner_pid
    assert os.stat('/proc/self/ns/mnt').st_ino != os.fstat(outerfd).st_ino
    from uflow import ExternalBackingHandle, ExternalDdrClient, ExternalDdrError
    assert sys.prefix != sys.base_prefix
    client = ExternalDdrClient(ENDPOINT, timeout=90)
    out = V / 'results'; out.mkdir(mode=0o700)
    nbytes = 4096
    srcsize, dstsize = nbytes+c.SRC_OFFSET+c.SUFFIX, nbytes+c.DST_OFFSET+c.SUFFIX
    source_fd, target_fd = c.memfd('delay-source', srcsize, 37, True), c.memfd('delay-target', dstsize, 83, False)
    handles, peers = [], []
    result = {'kind': 'real-private-production-regression', 'full_matrix': False}
    try:
        source = client.register(source_fd, name='delay-source', owner_name='delay-owner', size_bytes=srcsize)
        handles.append(source)
        target = client.stage(target_fd, name='delay-target', owner_name='delay-owner', size_bytes=dstsize)
        handles.append(target)
        baseline = client.snapshot()
        (V / 'delay.arm').write_text('4\n')
        started = time.monotonic()
        try:
            client.plan(source, target, src_offset_bytes=c.SRC_OFFSET, dst_offset_bytes=c.DST_OFFSET, nbytes=nbytes)
        except ExternalDdrError as error:
            assert str(error) == c.FRESHNESS_REJECTION, error
            rejection = str(error)
        else:
            raise AssertionError('production Plan accepted >5 second observation')
        elapsed = time.monotonic()-started
        assert elapsed > 5 and not (V / 'delay.arm').exists()
        assert os.pread(target_fd, dstsize, 0) == bytes([83])*dstsize
        try:
            view = client.acquire(target, offset_bytes=c.DST_OFFSET, nbytes=nbytes)
        except ExternalDdrError as error:
            not_ready = str(error)
        else:
            view.release(); raise AssertionError('business target became Ready')
        negative = client.snapshot()
        for key in ZERO: assert baseline[key] == negative[key], (key, baseline, negative)
        assert next(b for b in negative['backings'] if b['object_id'] == target.object_id)['state'] == 'pending'
        records = journal()
        work = [r['fields'] for r in records if r['fields']['type'] == 'work_intent']
        assert not any(r['target_id'] == str(target.object_id) for r in work)
        probes = [r for r in work if r['purpose'] == 'probe_only']
        assert len(probes) == 1, probes
        probe = probes[0]
        life = [r['fields']['type'] for r in records if r['fields'].get('work_id') == probe['work_id']]
        assert 'work_drained' in life and 'work_verified' in life
        assert any(r['fields']['type'] == 'backing_close_done' and r['fields'].get('object_id') == probe['target_id'] for r in records)
        assert next(b for b in negative['backings'] if str(b['object_id']) == probe['target_id'])['reclaimed']
        result['delayed_plan'] = dict(error=rejection, seconds=elapsed, target=dataclasses.asdict(target),
                                      not_ready_error=not_ready, full_target_unchanged=True, business_work_count=0,
                                      probe=probe, probe_lifecycle=life, before=baseline, after=negative)
        c.save_json(out / 'delayed-plan.json', result['delayed_plan'])
        # A second one-shot delayed Plan now exercises the repaired real tool:
        # one rejection + new full Plan/guard, exactly one business Submit.
        (V / 'delay.arm').write_text('4\n')
        context = dict(concurrency=1, bytes=nbytes, round=0, requester=0, pid=os.getpid(), sample_id='real-delay-retry')
        plan, submitted, done, ready, plan_seconds, failures = c.copy_attempts(
            client, source, target, nbytes, context, lambda row: c.append_json(out/'attempts.jsonl', row))
        assert len(failures) == 1 and failures[0]['error'] == c.FRESHNESS_REJECTION
        c.exact(target_fd, dstsize, nbytes, 83)
        with client.acquire(target, offset_bytes=c.DST_OFFSET, nbytes=nbytes) as view:
            c.consume(view, nbytes)
        records = journal()
        business = [r for r in records if r['fields']['type']=='work_intent' and r['fields']['target_id']==str(target.object_id)]
        assert len(business) == 1
        result['real_retry'] = dict(plan=plan, submitted=submitted, done=done, failures=failures,
                                    full_payload_and_complement_exact=True, business_work_count=len(business))
        for h in reversed(handles): client.revoke(h)
        handles.clear()
        os.close(target_fd); target_fd = None
        os.close(source_fd); source_fd = None
        assert all(client.snapshot()[k] == 0 for k in ZERO)
        # One true c2 round; no fitting, no changed production settings/preload
        # arm, and no complete-matrix/performance claim.
        args = SimpleNamespace(output=out, endpoint=ENDPOINT)
        cpus = sorted(os.sched_getaffinity(0))[:3]; assert len(cpus) == 3
        source_peer = c.Peer(args, cpus[0], 'source'); peers.append(source_peer)
        nbytes = 1048576
        src = source_peer.command('source', bytes=nbytes)
        targets = []
        for i in range(2):
            p = c.Peer(args, cpus[i+1], f'target{i}'); targets.append(p); peers.append(p)
        prepared = [p.command('prepare', bytes=nbytes, source=src['handle'], round=0) for p in targets]
        at = time.monotonic()+.02
        for p in targets: p.send('map', bytes=nbytes, at=at, source_pid=source_peer.proc.pid)
        mapped = [p.receive() for p in targets]
        at = time.monotonic()+.02
        for i,p in enumerate(targets):
            p.send('copy', bytes=nbytes, at=at, concurrency=2, round=0, requester=i, sample_id=f'real-c2-q{i}')
        copied = [p.receive() for p in targets]
        consumed = [p.command('consume_copy', bytes=nbytes) for p in targets]
        rows = []
        for i in range(2):
            row = dict(concurrency=2, bytes=nbytes, round=0, requester=i, preparation=prepared[i],
                       **mapped[i], **copied[i], **consumed[i])
            assert row['full_payload_and_complement_exact']
            assert row['map_read']['crc32'] == row['copy_read']['crc32']
            assert row['done']['copied_bytes'] == str(nbytes) and row['done']['drained'] == 'true'
            records = journal()
            target_id = str(prepared[i]['target']['object_id'])
            assert len([r for r in records if r['fields']['type']=='work_intent' and r['fields']['target_id']==target_id]) == 1
            c.append_json(out / 'samples.jsonl', row); rows.append(row)
        result['c2_rows'] = rows
        result['participant_pids'] = [p.proc.pid for p in peers]
        for p in reversed(peers): p.close()
        peers.clear()
        # Real CLI stop negative: request the unchanged ten-round matrix, let
        # actual work complete, then stop new rounds. Do not fit partial rows.
        stopped = V/'stopped-calibration'
        stop_file = V/'calibration.stop'
        command = [sys.executable,str(Path(c.__file__)), '--endpoint',ENDPOINT,'--output',str(stopped),
                   '--source-commit',(V/'source-commit.txt').read_text().strip(),
                   '--deployment-metadata',str(V/'deployment.json'), '--soft-deadline-seconds','60',
                   '--stop-file',str(stop_file)]
        with (out/'cooperative-stop.log').open('x') as log:
            calibration = subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT)
            deadline = time.monotonic()+30
            while not (stopped/'samples.jsonl').exists():
                assert calibration.poll() is None and time.monotonic() < deadline
                time.sleep(.01)
            stop_file.write_text('private cooperative-stop negative\n')
            rc = calibration.wait(timeout=120)
        assert rc != 0 and not (stopped/'calibration.json').exists()
        status = json.loads((stopped/'run-status.json').read_text())
        cleanup = json.loads((stopped/'cleanup.json').read_text())
        assert status['status'] == 'failed' and not status['matrix_complete'] and status['cleanup_confirmed']
        assert cleanup['all_recorded_pids_exited'] and not cleanup['errors']
        assert (stopped/'deployment-input.json').read_bytes() == (V/'deployment.json').read_bytes()
        stopped_rows = [json.loads(line) for line in (stopped/'samples.jsonl').read_text().splitlines()]
        assert stopped_rows and all(row['full_payload_and_complement_exact'] for row in stopped_rows)
        result['participant_pids'] += [p['pid'] for p in cleanup['participants']] + [calibration.pid]
        result['cooperative_stop'] = dict(exit=rc, command=command, raw_samples=len(stopped_rows),
                                          status=status, cleanup=cleanup, full_matrix=False)
        copied_bytes = 4096 + sum(row['bytes'] for row in rows+stopped_rows)
        mapped_bytes = 4096 + 2*sum(row['bytes'] for row in rows+stopped_rows)
        result['expected_stats'] = dict(external_copied_bytes=str(copied_bytes),
                                        external_mapped_bytes=str(mapped_bytes),
                                        external_accessed_bytes=str(2*mapped_bytes))
    finally:
        errors = []
        for p in reversed(peers):
            try: p.close()
            except Exception as error: errors.append(repr(error))
        for h in reversed(handles):
            try: client.revoke(h)
            except Exception as error: errors.append(repr(error))
        for fd in (source_fd, target_fd):
            if fd is not None: os.close(fd)
        result['cleanup_errors'] = errors
        result['zero'] = client.snapshot()
        c.save_json(out / 'exercise.json', result)
    assert not errors and all(result['zero'][k] == 0 for k in ZERO)
    assert all(not Path(f'/proc/{pid}').exists() for pid in result['participant_pids'])
    print('REAL_DELAY_REJECTION_RETRY_AND_C2_PASS', flush=True)


def inner(args):
    guard.require_isolated_namespace(args.nsfd, args.outer_pid)
    expected = os.fstat(args.rootfd)
    assert guard.identity(args.nsroot) == (expected.st_dev, expected.st_ino)
    subprocess.run(['mount', '--bind', str(args.nsroot), '/workspace/uflow'], check=True)
    assert guard.identity(Path('/workspace/uflow')) == (expected.st_dev, expected.st_ino)
    assert not os.path.lexists(ROOT)
    env = dict(os.environ, UF_EXTERNAL_DDR_HOST_ID='t35-private-regression',
               UF_EXTERNAL_DDR_OWNER_MAX_BYTES='268435456', UF_EXTERNAL_DDR_TOTAL_MAX_BYTES='536870912',
               UF_HBM_STARTUP_PROBE_BYTES='0', LD_LIBRARY_PATH=str(V/'shim')+':/usr/local/Ascend/cann-9.0.0/lib64',
               PYTHONPATH=str(V/'src/sdk/python'), PYTHONDONTWRITEBYTECODE='1')
    binary = V/'uf-daemon'
    report = {'daemon_sha256': guard.digest(binary), 'test_preload_sha256': guard.digest(V/'delay.so')}
    init = subprocess.run([str(binary),'--socket',ENDPOINT,'--initialize-external-ddr-authority'],
                          env=env,capture_output=True)
    (V/'bootstrap.log').write_bytes(init.stdout+init.stderr); assert init.returncode == 0
    pinned = {name: guard.identity(ROOT/name) for name in guard.FILES}
    daemon = None
    try:
        with (V/'daemon.log').open('x') as log:
            daemon_env = dict(env, LD_PRELOAD=str(V/'delay.so'), UF_T35_PRIVATE_DELAY_ARM=str(V/'delay.arm'))
            daemon = subprocess.Popen([str(binary),'--socket',ENDPOINT,'--execution-mode','simulated',
                                       '--startup-probe-bytes','0'],env=daemon_env,stdout=log,stderr=subprocess.STDOUT)
            report['daemon_pid'] = daemon.pid
            deadline = time.monotonic()+30
            while not Path(ENDPOINT+'.external-ddr').exists():
                assert daemon.poll() is None and time.monotonic() < deadline; time.sleep(.02)
            libs = {line.split()[-1] for line in Path(f'/proc/{daemon.pid}/maps').read_text().splitlines()
                    if any(s in line for s in ('libuf_acl_shim','libascendcl','delay.so'))}
            report['loaded_libraries'] = {path: guard.digest(Path(path)) for path in libs}
            c.save_json(V/'deployment.json',dict(endpoint=ENDPOINT,source_commit=(V/'source-commit.txt').read_text().strip(),
                                                daemon_sha256=report['daemon_sha256'],daemon_pid=daemon.pid,
                                                loaded_libraries=report['loaded_libraries'],pinned_identities=pinned,
                                                purpose='guarded private functional regression; NOT actual G/performance matrix'))
            report['trace_start'] = control('StartTrace',run_id='t35-private',mode='timeline',include_args='true',output_dir=str(V/'trace'))
            run = subprocess.Popen([str(V/'venv/bin/python'),str(V/'src/examples/ddr_v0/test_t35_private_regression.py'),
                                    'exercise',str(args.rootfd),str(args.nsfd),str(os.getpid())],
                                   env=env,pass_fds=(args.rootfd,args.nsfd))
            report['exercise_pid'] = run.pid
            report['exercise_exit'] = run.wait(timeout=600)
            assert report['exercise_exit'] == 0
            report['stats'] = control('GetStats')
            expected_stats = json.loads((V/'results/exercise.json').read_text())['expected_stats']
            for key,value in expected_stats.items(): assert report['stats'][key] == value, (key,report['stats'][key],value)
            report['expected_stats'] = expected_stats
            for key in ('external_ddr_committed_bytes','external_unfinalized_workers','external_cleanup_unconfirmed'):
                assert report['stats'][key] == '0'
            assert report['stats']['external_cost_version'] == 'unknown'
            assert report['stats']['external_automatic_ranking'] == 'disabled'
            report['trace_export'] = control('ExportTrace')
            assert report['trace_export']['trace_flushed'] == 'true'
            trace = json.loads(Path(report['trace_export']['trace_artifact']).read_text())['traceEvents']
            submitted = [e['args'] for e in trace if e['name']=='transfer.external_submit']
            terminal = [e['args'] for e in trace if e['name']=='transfer.external_terminal']
            assert sum(int(e['copied_bytes']) for e in terminal) == int(expected_stats['external_copied_bytes'])
            assert {e['event_id'] for e in submitted} == {e['event_id'] for e in terminal}
            assert len(submitted) == len({e['event_id'] for e in submitted})
            report['trace_reconciliation'] = dict(submits=len(submitted),terminals=len(terminal),
                                                   copied_bytes=sum(int(e['copied_bytes']) for e in terminal))
    finally:
        if daemon and daemon.poll() is None:
            report['shutdown'] = control('ShutdownDaemon')
            report['daemon_exit'] = daemon.wait(timeout=90)
        c.save_json(V/'result.json', report)
    assert report['daemon_exit'] == 0
    assert json.loads((ROOT/'state').read_text())['mode'] == 'clean'
    assert json.loads((ROOT/'state').read_text())['closure'] == 'normal'
    assert pinned == {name: guard.identity(ROOT/name) for name in guard.FILES}
    assert not Path(ENDPOINT).exists() and not Path(ENDPOINT+'.external-ddr').exists()
    assert not Path(f'/proc/{daemon.pid}').exists()
    fd = os.open(ROOT/'authority.lock', os.O_RDONLY|os.O_CLOEXEC|os.O_NOFOLLOW)
    fcntl.flock(fd, fcntl.LOCK_EX|fcntl.LOCK_NB); os.close(fd)
    c.save_json(V/'clean-audit.json', dict(pinned_identities=pinned, lock_free=True, sockets_absent=True,
                                          daemon_exited=True, state=json.loads((ROOT/'state').read_text()),
                                          journal_frames=len(journal())))
    print('PRIVATE_PRODUCTION_CLEAN_PASS', flush=True)


def outer(args):
    run = args.run_dir.resolve()
    assert run.is_relative_to(Path('/workspace/uflow/runs')) and run == args.run_dir
    run.mkdir(mode=0o700)
    nsroot = run/'nsroot'; nsroot.mkdir(mode=0o700)
    (nsroot/'authority').mkdir(mode=0o700); (nsroot/'runs').mkdir(mode=0o700)
    clone = nsroot/V.relative_to('/workspace/uflow'); clone.mkdir(mode=0o700)
    src = Path(__file__).resolve().parents[2]
    for name in ('examples/ddr_v0','sdk/python'):
        shutil.copytree(src/name, clone/'src'/name)
    assert args.source_commit and len(args.source_commit) == 40
    (clone/'source-commit.txt').write_text(args.source_commit+'\n')
    (clone/'shim').mkdir(mode=0o700)
    for source,dest in ((args.daemon,clone/'uf-daemon'),(args.shim,clone/'shim/libuf_acl_shim.so'),(args.delay_so,clone/'delay.so')):
        shutil.copy2(source,dest)
    os.chmod(clone/'uf-daemon',0o700)
    subprocess.run(['python3','-m','venv','--without-pip',str(clone/'venv')],check=True)
    nsfd = os.open('/proc/self/ns/mnt',os.O_RDONLY|os.O_CLOEXEC)
    rootfd = os.open(nsroot,os.O_RDONLY|os.O_DIRECTORY|os.O_CLOEXEC|os.O_NOFOLLOW)
    before = Path('/proc/self/mountinfo').read_bytes(); original = guard.identity(Path('/workspace/uflow'))
    cmd = [sys.executable,str(Path(__file__).resolve()),'inner','--nsroot',str(nsroot),
           '--nsfd',str(nsfd),'--rootfd',str(rootfd),'--outer-pid',str(os.getpid())]
    try:
        with (run/'negative-direct.log').open('x') as log:
            negative = subprocess.Popen(cmd,pass_fds=(nsfd,rootfd),stdout=log,stderr=subprocess.STDOUT)
            assert negative.wait(timeout=30) != 0
        assert 'mount namespace was not isolated' in (run/'negative-direct.log').read_text()
        with (run/'outer.log').open('x') as log:
            proc = subprocess.Popen(['unshare','--mount','--propagation','private',*cmd],
                                    pass_fds=(nsfd,rootfd),stdout=log,stderr=subprocess.STDOUT)
            rc = proc.wait(timeout=900)  # Timeout preserves process; never kill.
        same = before == Path('/proc/self/mountinfo').read_bytes() and original == guard.identity(Path('/workspace/uflow'))
        c.save_json(run/'isolation.json', dict(exit=rc, outer_mounts_and_identity_unchanged=same,
                                              negative_direct_exit=negative.returncode, outer_namespace=os.fstat(nsfd).st_ino))
        assert rc == 0 and same, (run/'outer.log').read_text()
    finally:
        os.close(rootfd); os.close(nsfd)
    print('GUARDED_PRIVATE_REGRESSION_PASS', flush=True)


def main():
    if len(sys.argv) > 1 and sys.argv[1] == 'exercise':
        assert len(sys.argv) == 5, 'inherited guarded private-root and namespace descriptors required'
        return exercise(*map(int, sys.argv[2:]))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('outer','inner'))
    parser.add_argument('--run-dir',type=Path)
    parser.add_argument('--source-commit')
    parser.add_argument('--daemon',type=Path); parser.add_argument('--shim',type=Path); parser.add_argument('--delay-so',type=Path)
    parser.add_argument('--nsroot',type=Path); parser.add_argument('--nsfd',type=int)
    parser.add_argument('--rootfd',type=int); parser.add_argument('--outer-pid',type=int)
    args = parser.parse_args()
    return outer(args) if args.mode == 'outer' else inner(args)


if __name__ == '__main__':
    main()
