#!/usr/bin/env python3
"""Public-client cost samples; no authority initialization, repair or daemon control.

Run against an explicitly deployed endpoint. The deployment driver owns clean
start/stop. Each requester consumes the same N bytes twice with C zlib.crc32,
without a Python N-byte slice/allocation. Exact byte validation is separate.
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
import resource
import select
import signal
import subprocess
import sys
import time
import traceback
import zlib

from t35_cost_model import TRAIN_SIZES, HELD_OUT, summarize

CHUNK = 65536
SRC_OFFSET, DST_OFFSET = 31, 173
SUFFIX = 4096
FRESHNESS_REJECTION = 'clock changed or observation exceeded freshness limit'
# Only explicit pre-admission rejections whose production call sites have
# been checked. In particular, arbitrary "replan" and foreign/consumed plans
# are NOT permission to replay a business operation.
STALE_REJECTIONS = frozenset((
    # wire_error replaces internal semicolons with commas.
    'broker or NUMA inventory changed during observation, retry',
    'stale snapshot or realm mismatch',
    'stale broker, epoch, generation, topology or resources',
    'snapshot expired during guard',
    'stale epoch or shutdown, explicitly replan',
    'broker or physical topology changed during probe, explicitly replan',
))


def public_evidence(value):
    """Keep full backing/event identities, never reusable capability tokens."""
    if isinstance(value, dict):
        return {k: public_evidence(v) for k, v in value.items()
                if 'token' not in k.lower() and k.lower() != 'proof'}
    if isinstance(value, (list, tuple)):
        return [public_evidence(v) for v in value]
    return value


def save_json(path, value):
    with path.open('x') as file:
        json.dump(public_evidence(value), file, indent=2)
        file.write('\n'); file.flush(); os.fsync(file.fileno())


def append_json(path, value):
    with path.open('a') as file:
        file.write(json.dumps(public_evidence(value)) + '\n')
        file.flush(); os.fsync(file.fileno())


def retry_rejection(error, phase, plan):
    from uflow import ExternalDdrError
    if not isinstance(error, ExternalDdrError):
        return False
    if phase == 'plan' and plan is None and str(error) == FRESHNESS_REJECTION:
        return True
    return phase in ('plan', 'submit') and str(error) in STALE_REJECTIONS


def copy_attempts(client, source, target, nbytes, context, record, pause=0):
    """At most four Plans for ONE requester; never replay a returned Submit.

    Plan can run and drain private probes. Only Submit admits business work.
    The callback syncs each failure before a retry or any exception escapes.
    """
    failures = []
    for attempt in range(1, 5):
        plan, submitted, phase = None, None, 'plan'
        started = time.perf_counter_ns()
        timings = {}
        try:
            plan = client.plan(source, target, src_offset_bytes=SRC_OFFSET,
                               dst_offset_bytes=DST_OFFSET, nbytes=nbytes)
            timings['plan_seconds'] = (time.perf_counter_ns() - started) / 1e9
            phase, started = 'submit', time.perf_counter_ns()
            submitted = client.submit(int(plan['plan_id']), plan_token=plan['plan_token'])
            timings['submit_seconds'] = (time.perf_counter_ns() - started) / 1e9
            # A successful Submit is never inside the replan branch.
        except Exception as error:
            retryable = retry_rejection(error, phase, plan)
            row = {**context, 'kind': 'failed_attempt', 'attempt': attempt,
                   'failed_phase': phase, 'failed_phase_seconds': (time.perf_counter_ns() - started) / 1e9,
                   'phase_timings': timings, 'error': str(error), 'error_type': type(error).__name__,
                   'source': dataclasses.asdict(source), 'target': dataclasses.asdict(target),
                   'plan_id': None if plan is None else plan.get('plan_id'),
                   'retryable_rejection': retryable, 'will_retry': retryable and attempt < 4,
                   'recorded_at_unix_ns': time.time_ns()}
            record(row); failures.append(row)
            if not retryable:
                raise
            if attempt == 4:
                raise RuntimeError('bounded explicit pre-admission replan exhausted (4 attempts)') from error
            time.sleep(pause)
            continue
        phase, wait_start = 'wait', time.perf_counter_ns()
        try:
            done = client.wait(int(submitted['event_id']), event_token=submitted['event_token'], timeout_ms=30000)
            timings['wait_seconds'] = (time.perf_counter_ns() - wait_start) / 1e9
            ready = (time.perf_counter_ns() - started) / 1e9
            assert done['event_status'] == 'complete' and done['drained'] == 'true', public_evidence(done)
            assert done['copied_bytes'] == str(nbytes)
        except Exception as error:
            record({**context, 'kind': 'failed_attempt', 'attempt': attempt,
                    'failed_phase': phase, 'failed_phase_seconds': (time.perf_counter_ns() - wait_start) / 1e9,
                    'phase_timings': timings, 'error': str(error), 'error_type': type(error).__name__,
                    'source': dataclasses.asdict(source), 'target': dataclasses.asdict(target),
                    'plan_id': plan.get('plan_id'), 'event_id': submitted.get('event_id'),
                    'retryable_rejection': False, 'will_retry': False, 'recorded_at_unix_ns': time.time_ns()})
            # Cancellation requests are not drain. Keep the result separate
            # from the original failure, and require a real terminal Wait.
            cleanup = {**context, 'kind': 'event_cleanup', 'attempt': attempt,
                       'event_id': submitted.get('event_id'), 'target': dataclasses.asdict(target)}
            cleanup_started = time.perf_counter_ns()
            try:
                client.cancel(int(submitted['event_id']), event_token=submitted['event_token'])
            except Exception as cleanup_error:
                record({**cleanup, 'kind': 'failed_attempt', 'failed_phase': 'cancel',
                        'failed_phase_seconds': (time.perf_counter_ns()-cleanup_started)/1e9,
                        'error': str(cleanup_error), 'error_type': type(cleanup_error).__name__,
                        'retryable_rejection': False, 'will_retry': False})
            # Even an unknown Cancel result does not authorize abandoning a
            # known admitted event. Independently try its real terminal Wait.
            cleanup_started = time.perf_counter_ns()
            try:
                drained = client.wait(int(submitted['event_id']), event_token=submitted['event_token'], timeout_ms=30000)
                assert drained['drained'] == 'true', public_evidence(drained)
                cleanup['drained'] = True
            except Exception as cleanup_error:
                cleanup.update(drained=False, error=str(cleanup_error))
                record({**cleanup, 'kind': 'failed_attempt', 'failed_phase': 'cleanup_wait',
                        'failed_phase_seconds': (time.perf_counter_ns()-cleanup_started)/1e9,
                        'error_type': type(cleanup_error).__name__,
                        'retryable_rejection': False, 'will_retry': False})
            record(cleanup)
            raise
        record({**context, 'kind': 'copy_complete', 'attempt': attempt,
                'failed_attempts': len(failures), 'plan_id': plan.get('plan_id'),
                'event_id': submitted.get('event_id'), 'phase_timings': timings,
                'target': dataclasses.asdict(target), 'recorded_at_unix_ns': time.time_ns()})
        return plan, submitted, done, ready, timings['plan_seconds'], failures


class StopRequested(RuntimeError):
    pass


class CooperativeStop:
    def __init__(self, seconds, stop_file=None):
        self.deadline = time.monotonic() + seconds
        self.stop_file, self.signal = stop_file, None
    def request(self, signum, frame):
        self.signal = signum
    def checkpoint(self):
        if self.signal is not None or time.monotonic() >= self.deadline or (
                self.stop_file is not None and self.stop_file.exists()):
            raise StopRequested('cooperative stop before new round/group; matrix incomplete')


def offline_report(samples, metadata):
    report = summarize(samples, metadata)
    now = time.time_ns()
    valid = metadata['sampled_at_unix_ns'] <= now < metadata['expires_at_unix_ns']
    report.update(analyzed_at_unix_ns=now, evidence_validity='within-policy-ttl' if valid else 'unknown-expired-or-clock-changed')
    if not valid:
        for profile in report['profiles']:
            profile.update(ranking_validated=False, validity='unknown',
                           reason='original analysis TTL expired or clock changed; historical statistics only; never installed')
    return report


def numa(name):
    return [line for line in Path('/proc/self/numa_maps').read_text().splitlines()
            if 'memfd:' + name in line]


def placement(fd, size, name):
    # Full read faults existing backing pages into this temporary observation
    # mapping; not an allocation or inference from sysfs topology.
    readonly = os.open(f'/proc/self/fd/{fd}', os.O_RDONLY | os.O_CLOEXEC)
    try:
        with mmap.mmap(readonly, size, access=mmap.ACCESS_READ) as mapping:
            zlib.crc32(mapping)
            return numa(name)
    finally:
        os.close(readonly)


def memfd(name, size, seed, immutable):
    fd = os.memfd_create(name, os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
    os.ftruncate(fd, size)
    chunk = bytes([seed]) * CHUNK
    for offset in range(0, size, CHUNK):
        data = chunk[:min(CHUNK, size - offset)]
        assert os.pwrite(fd, data, offset) == len(data)
    fcntl.fcntl(fd, fcntl.F_ADD_SEALS, fcntl.F_SEAL_GROW | fcntl.F_SEAL_SHRINK |
                (fcntl.F_SEAL_WRITE if immutable else 0))
    return fd


def consume(view, nbytes):
    offset = view.allowed_offset_bytes
    with memoryview(view._mapping)[offset:offset + nbytes] as buffer:
        before = resource.getrusage(resource.RUSAGE_SELF)
        t = time.perf_counter_ns()
        first_crc = zlib.crc32(buffer)
        first = (time.perf_counter_ns() - t) / 1e9
        after_first = resource.getrusage(resource.RUSAGE_SELF)
        t = time.perf_counter_ns()
        reuse_crc = zlib.crc32(buffer)
        reuse = (time.perf_counter_ns() - t) / 1e9
        after = resource.getrusage(resource.RUSAGE_SELF)
    assert first_crc == reuse_crc
    # Public telemetry outside local read timing. No replacement success data.
    view._client._request('RecordExternalAccess', event_id=view.event_id,
                          event_token=view.event_token, accessed_bytes=2 * nbytes)
    return {'first_seconds': first, 'reuse_seconds': reuse, 'crc32': first_crc,
            'first_minor_faults': after_first.ru_minflt - before.ru_minflt,
            'first_major_faults': after_first.ru_majflt - before.ru_majflt,
            'reuse_minor_faults': after.ru_minflt - after_first.ru_minflt,
            'reuse_major_faults': after.ru_majflt - after_first.ru_majflt}


def exact(fd, size, nbytes, seed):
    """Full source payload AND all known target neighbours; bounded byte compare."""
    started = time.perf_counter_ns()
    for offset in range(0, size, CHUNK):
        count = min(CHUNK, size - offset)
        expected = bytearray([seed]) * count
        a, b = max(offset, DST_OFFSET), min(offset + count, DST_OFFSET + nbytes)
        if a < b:
            expected[a-offset:b-offset] = bytes([37]) * (b-a)
        assert os.pread(fd, count, offset) == expected, (offset, count)
    return (time.perf_counter_ns() - started) / 1e9


def worker(args):
    from uflow import ExternalBackingHandle, ExternalDdrClient
    assert sys.prefix != sys.base_prefix
    assert all(importlib.util.find_spec(n) is None for n in ('torch', 'torch_npu', 'serving'))
    assert not any(n in sys.modules for n in ('torch', 'uflow.client', 'uflow.objects'))
    os.sched_setaffinity(0, {args.cpu})
    client = ExternalDdrClient(args.endpoint, timeout=90)
    handles, fds, views = [], [], []
    source, target, target_fd, size, target_name = None, None, None, 0, ''
    copy_row = {}
    def cleanup_resources():
        errors = []
        for view in list(views):
            try:
                view.release(); views.remove(view)
            except Exception:
                errors.append(traceback.format_exc())
        for handle in reversed(list(handles)):
            try:
                client.revoke(handle); handles.remove(handle)
            except Exception:
                errors.append(traceback.format_exc())
        for fd in list(fds):
            try:
                os.close(fd); fds.remove(fd)
            except Exception:
                errors.append(traceback.format_exc())
        if errors:
            raise RuntimeError(errors)
    def emit(value):
        print(json.dumps(public_evidence(value)), flush=True)
    emit({'pid': os.getpid(), 'affinity': sorted(os.sched_getaffinity(0)),
          'python': sys.executable, 'torch_absent': True})
    try:
        for line in sys.stdin:
            req = json.loads(line)
            try:
                op = req['op']
                if op == 'source':
                    name = f't35-S-{os.getpid()}'
                    size = req['bytes'] + SRC_OFFSET + SUFFIX
                    fd = memfd(name, size, 37, True); fds.append(fd)
                    handle = client.register(fd, name=name, owner_name='t35-source', size_bytes=size)
                    handles.append(handle)
                    value = {'handle': dataclasses.asdict(handle), 'numa': placement(fd, size, name)}
                elif op == 'prepare':
                    if not handles:
                        anchor_fd = memfd(f't35-anchor-{os.getpid()}', 4096, 83, True)
                        fds.append(anchor_fd)
                        handles.append(client.register(anchor_fd, name='t35-anchor',
                                                       owner_name='t35-target', size_bytes=4096))
                    source = ExternalBackingHandle(**req['source'])
                    # Directed private regression may enlarge only the target
                    # complement. Normal calibration never supplies this key.
                    extra = req.get('target_extra_bytes', 0)
                    assert isinstance(extra, int) and 0 <= extra <= 64 * 1024 * 1024
                    size = req['bytes'] + DST_OFFSET + SUFFIX + extra
                    target_name = f't35-T-{os.getpid()}-{req["round"]}'
                    target_fd = memfd(target_name, size, 83, False); fds.append(target_fd)
                    target = client.stage(target_fd, name=target_name, owner_name='t35-target', size_bytes=size)
                    handles.append(target)
                    value = {'target': dataclasses.asdict(target),
                             'target_numa_before': placement(target_fd, size, target_name)}
                elif op == 'map':
                    while time.monotonic() < req['at']:
                        time.sleep(.0001)
                    t = time.perf_counter_ns()
                    view = client.acquire(source, offset_bytes=SRC_OFFSET, nbytes=req['bytes'])
                    views.append(view)
                    setup = (time.perf_counter_ns() - t) / 1e9
                    read = consume(view, req['bytes'])
                    observed = numa(f't35-S-{req["source_pid"]}')
                    event = client.poll(view.event_id, event_token=view.event_token)
                    assert event['mapped_bytes'] == str(req['bytes']) and event['copied_bytes'] == '0'
                    assert event['accessed_bytes'] == str(2 * req['bytes'])
                    view.release(); views.remove(view)
                    value = {'map_setup_seconds': setup, 'map_first_seconds': read['first_seconds'],
                             'map_reuse_seconds': read['reuse_seconds'], 'map_read': read,
                             'map_total_seconds': setup + read['first_seconds'] + read['reuse_seconds'],
                             'source_numa_at_requester': observed, 'map_event': event}
                elif op == 'copy':
                    while time.monotonic() < req['at']:
                        time.sleep(.0001)
                    context = {k: req[k] for k in ('concurrency', 'bytes', 'round', 'requester')}
                    context.update(pid=os.getpid(), sample_id=req['sample_id'])
                    plan, submitted, done, ready, plan_seconds, attempts = copy_attempts(
                        client, source, target, req['bytes'], context,
                        lambda row: append_json(args.attempt_log, row), .001 * (args.cpu + 1))
                    assert plan['automatic_ranking'] == 'disabled' and plan['cost_version'] == 'unknown'
                    queue, execute = float(done['queue_seconds']), float(done['execute_seconds'])
                    residual = ready - queue - execute
                    assert residual >= 0, (ready, queue, execute)
                    copy_row = {'copy_ready_seconds': ready, 'copy_residual_seconds': residual,
                                'copy_execute_seconds': execute, 'copy_queue_seconds': queue,
                                'probe_plan_seconds': plan_seconds, 'replan_attempts': attempts,
                                'sample_id': req['sample_id'], 'attempt_log': str(args.attempt_log),
                                'plan': plan, 'submitted': submitted, 'done': done,
                                'ordinary_io': {'source_read_bytes': req['bytes'], 'target_write_bytes': req['bytes'],
                                                'copy_buffer_bytes': CHUNK, 'neighbour_snapshot_bytes': 0,
                                                'payload_readback_bytes': 0},
                                'probe_io': {'private_target_charged_bytes': size,
                                             'prefill_write_bytes': size, 'primitive_source_read_bytes': req['bytes'],
                                             'primitive_target_write_bytes': req['bytes'],
                                             'validation_target_read_bytes': size,
                                             'validation_source_read_bytes': req['bytes'],
                                             'validation_buffers_bytes': 2 * CHUNK,
                                             'verification_slots': 1,
                                             'provenance': 'successful exact-range broker probe; counts from reviewed loops, not syscall tracing',
                                             'component_times_seconds': None}}
                    value = copy_row
                elif op == 'consume_copy':
                    t = time.perf_counter_ns()
                    view = client.acquire(target, offset_bytes=DST_OFFSET, nbytes=req['bytes'])
                    views.append(view)
                    setup = (time.perf_counter_ns() - t) / 1e9
                    read = consume(view, req['bytes'])
                    observed = numa(target_name)
                    view.release(); views.remove(view)
                    # Separate exact full-target validation, NOT a normal-copy
                    # buffer/capacity requirement or an invented probe timer.
                    validation = exact(target_fd, size, req['bytes'], 83)
                    value = {'copy_target_map_setup_seconds': setup,
                             'copy_setup_seconds': copy_row['copy_residual_seconds'] + setup,
                             'copy_first_seconds': read['first_seconds'],
                             'copy_reuse_seconds': read['reuse_seconds'], 'copy_read': read,
                             'copy_total_seconds': copy_row['copy_ready_seconds'] + setup + read['first_seconds'] + read['reuse_seconds'],
                             'target_numa_after': observed,
                             'explicit_validation_seconds': validation,
                             'explicit_validation_read_bytes': size,
                             'explicit_validation_buffer_bound_bytes': 3 * CHUNK,
                             'full_payload_and_complement_exact': True}
                    client.revoke(target); handles.remove(target)
                    os.close(target_fd); fds.remove(target_fd)
                elif op == 'cleanup':
                    cleanup_resources()
                    emit({'ok': True, 'value': {'cleaned': True}})
                    return
                else:
                    raise ValueError(op)
                emit({'ok': True, 'value': value})
            except Exception:
                emit({'ok': False, 'error': traceback.format_exc()})
    finally:
        cleanup_resources()


class Peer:
    def __init__(self, args, cpu, label):
        self.pending = False
        self.attempt_log = args.output / f'{label}.attempts.jsonl'
        self.log = (args.output / f'{label}.log').open('x')
        self.proc = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), '--worker',
                                      '--endpoint', args.endpoint, '--cpu', str(cpu),
                                      '--attempt-log', str(self.attempt_log)],
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.log, text=True)
        try:
            self.start = self.receive(raw=True)
        except BaseException:
            self.proc.stdin.close()
            self.proc.wait(timeout=180)
            self.log.close()
            raise
    def send(self, op, **values):
        assert not self.pending, 'one outstanding command per peer'
        self.proc.stdin.write(json.dumps({'op': op, **values}) + '\n'); self.proc.stdin.flush()
        self.pending = True
    def receive(self, raw=False):
        if not select.select([self.proc.stdout], [], [], 720)[0]:
            raise TimeoutError(self.proc.pid)
        line = self.proc.stdout.readline()
        if not line:
            raise RuntimeError(f'peer exited {self.proc.pid}: {self.proc.poll()}')
        result = json.loads(line)
        self.pending = False
        if raw:
            return result
        if not result['ok']:
            raise RuntimeError(result['error'])
        return result['value']
    def command(self, op, **values):
        self.send(op, **values); return self.receive()
    def close(self):
        # An already-issued copy may still run while its sibling failed.
        # Consume its real result before asking for cleanup, NEVER replay it.
        error = None
        try:
            if self.proc.poll() is None:
                if self.pending:
                    try:
                        self.receive()
                    except RuntimeError:
                        # A returned failed command is not a failed cleanup.
                        # Timeout/invalid transport is not a cleanup ACK.
                        if self.pending:
                            raise
                result = self.command('cleanup')
                assert result['cleaned'] is True
        except Exception as failure:
            error = failure
        finally:
            # Even a rejected cleanup/unknown response must deliver EOF and
            # wait for the worker's own finally. No terminate/kill shortcut.
            if not self.proc.stdin.closed:
                self.proc.stdin.close()
            try:
                exited = self.proc.wait(timeout=180)
            finally:
                self.log.close()
        if error is not None:
            raise error
        assert exited == 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--endpoint', required=True)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--source-commit')
    parser.add_argument('--deployment-metadata', type=Path)
    parser.add_argument('--worker', action='store_true')
    parser.add_argument('--cpu', type=int)
    parser.add_argument('--rounds', type=int, default=10)
    parser.add_argument('--attempt-log', type=Path)
    parser.add_argument('--soft-deadline-seconds', type=float,
                        help='Approved finite measurement budget; stops new rounds, never kills peers')
    parser.add_argument('--stop-file', type=Path, help='Driver creates this to request cooperative stop')
    args = parser.parse_args()
    if args.worker:
        return worker(args)
    assert args.rounds >= 10 and args.source_commit and args.deployment_metadata
    assert args.soft_deadline_seconds is not None and 0 < args.soft_deadline_seconds < float('inf')
    args.output.mkdir(mode=0o700)
    cpus = sorted(os.sched_getaffinity(0))[:3]
    assert len(cpus) == 3
    started = time.time_ns()
    deployment_bytes = args.deployment_metadata.read_bytes()
    metadata = {'version': 't35-offline-v1', 'source_commit': args.source_commit,
                'deployment': json.loads(deployment_bytes),
                'sampled_at_unix_ns': started, 'expires_at_unix_ns': started + 3600 * 10**9,
                'validity_policy': 'one hour is a conservative analysis TTL, not measured physical stability; never installed',
                'endpoint': args.endpoint, 'kernel': os.uname().release,
                'page_bytes': os.sysconf('SC_PAGE_SIZE'), 'python': sys.version,
                'units': {'time': 'seconds', 'size': 'bytes', 'bandwidth': 'bytes/second'},
                'condition': 'source and target backing fully populated before samples; new requester PTE per read; '
                             'no drop_caches or cold-storage claim; CPU-cache uncontrolled; reuse immediately follows first CRC32 read',
                'workload': 'full N-byte C zlib.crc32 first + one full reuse on both paths; no Python full-size read slice; '
                            'exact full target byte verification separately timed after consumption; preparation/register/revoke excluded',
                'concurrency': '1/2 real requester processes, synchronized map/copy issue; broker serializes probe discovery; '
                               'actual queue/execute and bounded stale replans retained, not a claim of two simultaneously executing workers',
                'fit': 'per-size p50 least squares execute=a+N/B; other setup/queue and read B use medians of training groups only',
                'numa': 'actual per-backing /proc/<pid>/numa_maps Nn page counts and affinity in every sample; '
                        'no mbind/locality guarantee; broker may allocate private-probe pages elsewhere',
                'cpu_nodes': {str(cpu): [p.name for p in Path(f'/sys/devices/system/cpu/cpu{cpu}').glob('node*')] for cpu in cpus}}
    metadata['cooperative_stop'] = {'soft_deadline_seconds': args.soft_deadline_seconds,
                                    'stop_file': None if args.stop_file is None else str(args.stop_file),
                                    'policy': 'finish issued round, stop new rounds/groups, incomplete is failure; driver owns broker stop'}
    # Original input + start/expiry are durable BEFORE any measurement/peer.
    with (args.output / 'deployment-input.json').open('xb') as file:
        file.write(deployment_bytes); file.flush(); os.fsync(file.fileno())
    save_json(args.output / 'metadata.json', metadata)
    stop = CooperativeStop(args.soft_deadline_seconds, args.stop_file)
    previous_handlers = {sig: signal.signal(sig, stop.request) for sig in (signal.SIGTERM, signal.SIGINT)}
    peers, samples, participants, errors = [], [], [], []
    from uflow import ExternalDdrClient
    client = ExternalDdrClient(args.endpoint, timeout=90)
    outcome = {'status': 'incomplete', 'matrix_complete': False}
    try:
        for concurrency in (1, 2):
            for nbytes in (*TRAIN_SIZES, HELD_OUT):
                stop.checkpoint()
                source = Peer(args, cpus[0], f'source-c{concurrency}-n{nbytes}'); peers.append(source)
                participants.append(source.start)
                src = source.command('source', bytes=nbytes)
                targets = []
                for i in range(concurrency):
                    p = Peer(args, cpus[i+1], f'target{i}-c{concurrency}-n{nbytes}')
                    targets.append(p); peers.append(p); participants.append(p.start)
                for iteration in range(-2, args.rounds):
                    stop.checkpoint()
                    prepared = [p.command('prepare', bytes=nbytes, source=src['handle'], round=iteration) for p in targets]
                    at = time.monotonic() + .02
                    for p in targets:
                        p.send('map', bytes=nbytes, at=at, source_pid=source.proc.pid)
                    mapped = [p.receive() for p in targets]
                    at = time.monotonic() + .02
                    for index, p in enumerate(targets):
                        p.send('copy', bytes=nbytes, at=at, concurrency=concurrency,
                               round=iteration, requester=index,
                               sample_id=f'c{concurrency}-n{nbytes}-r{iteration}-q{index}')
                    copied = [p.receive() for p in targets]
                    consumed = [p.command('consume_copy', bytes=nbytes) for p in targets]
                    for index, p in enumerate(targets):
                        assert mapped[index]['map_read']['crc32'] == consumed[index]['copy_read']['crc32']
                        row = {'bytes': nbytes, 'concurrency': concurrency, 'round': iteration,
                               'recorded_at_unix_ns': time.time_ns(),
                               'requester': index, 'pid': p.proc.pid, 'warmup': iteration < 0,
                               'source': src, 'preparation': prepared[index],
                               **mapped[index], **copied[index], **consumed[index]}
                        samples.append(row)
                        append_json(args.output / 'samples.jsonl', row)
                for p in targets:
                    p.close(); peers.remove(p)
                source.close(); peers.remove(source)
                snapshot = client.snapshot()
                assert all(snapshot[k] == 0 for k in ('committed_bytes', 'active_views', 'pending_handoffs', 'in_flight'))
                print(f'completed c={concurrency} bytes={nbytes}', flush=True)
        outcome['matrix_complete'] = True
    except BaseException:
        outcome['error'] = traceback.format_exc()
        raise
    finally:
        for p in reversed(peers):
            try:
                p.close()
            except Exception:
                errors.append(traceback.format_exc())
        try:
            snapshot = client.snapshot()
            assert all(snapshot[k] == 0 for k in ('committed_bytes', 'backing_fds', 'owner_leases',
                                                'active_views', 'pending_handoffs', 'in_flight'))
        except Exception:
            snapshot = None
            errors.append(traceback.format_exc())
        exited = all(not Path(f'/proc/{p["pid"]}').exists() for p in participants)
        if not exited:
            errors.append('recorded participants remain alive; no force kill attempted')
        cleanup = {'participants': participants, 'errors': errors, 'snapshot': snapshot,
                   'all_recorded_pids_exited': exited}
        save_json(args.output / 'cleanup.json', cleanup)
        outcome.update(status='complete-clean-awaiting-report' if outcome['matrix_complete'] and not errors else 'failed',
                       cleanup_confirmed=not errors, raw_samples=len(samples))
        if not outcome['matrix_complete'] or errors:
            save_json(args.output / 'run-status.json', outcome)
        for sig, handler in previous_handlers.items():
            signal.signal(sig, handler)
        if errors:
            raise RuntimeError(errors)
    # No profile/pass artifact until the complete matrix AND real cleanup.
    # Re-evaluate original TTL after cleanup, at actual report creation.
    try:
        report = offline_report(samples, metadata)
        report['status'] = 'pass'
        save_json(args.output / 'calibration.json', report)
    except BaseException:
        outcome.update(status='failed', error=traceback.format_exc())
        save_json(args.output / 'run-status.json', outcome)
        raise
    outcome['status'] = 'pass'
    save_json(args.output / 'run-status.json', outcome)


if __name__ == '__main__':
    main()
