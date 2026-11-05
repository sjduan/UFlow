"""Independent stdlib mmap client for the managed-pool host contract probe."""
import argparse
from dataclasses import asdict
import hashlib
import json
import mmap
import os
import selectors
import subprocess
import sys

from uflow.managed_pool import ManagedPoolClient, PoolBlock, process_birth


def pattern(seed, offset, length):
    period = bytes((seed + offset + i) % 251 for i in range(251))
    return (period * ((length + 250) // 251))[:length]


def bytes_io(mapping, offset, length, seed, write=False):
    digest = hashlib.sha256()
    for start in range(offset, offset + length, 65536):
        size = min(65536, offset + length - start)
        expected = pattern(seed, start, size)
        if write:
            mapping[start:start + size] = expected
        actual = mapping[start:start + size]
        if actual != expected:
            raise AssertionError('payload/guard mismatch at ' + str(start))
        digest.update(actual)
    return {'offset': offset, 'nbytes': length, 'seed': seed, 'sha256': digest.hexdigest()}


def emit(value):
    raw = json.dumps(value, separators=(',', ':'))
    assert len(raw.encode()) < 65536
    print(raw, flush=True)


def read_line(stream, timeout=15):
    with selectors.DefaultSelector() as selector:
        selector.register(stream, selectors.EVENT_READ)
        if not selector.select(timeout):
            raise TimeoutError('bounded peer handshake expired; responsibilities retained')
    line = stream.readline()
    if not line or len(line.encode()) > 65536:
        raise RuntimeError('missing/oversized peer handshake')
    return json.loads(line)


def child_main(fd, nbytes):
    actual = os.fstat(fd)
    view = mmap.mmap(fd, nbytes, flags=mmap.MAP_SHARED, prot=mmap.PROT_READ | mmap.PROT_WRITE)
    emit({'birth': process_birth(os.getpid()), 'dev': actual.st_dev, 'inode': actual.st_ino,
          'nbytes': actual.st_size, 'mapped': True})
    command = read_line(sys.stdin, 120)
    assert command == {'close': True}
    view.close()
    os.close(fd)
    emit({'unmapped': True, 'fd_closed': True, 'native_jobs': 0})


def main(args):
    client = ManagedPoolClient.register(args.endpoint, args.pool_id, args.epoch, args.access)
    leases, exported, child, child_birth = {}, None, None, None
    actual = os.fstat(client.view.fileno())
    emit({'ready': True, 'birth': process_birth(os.getpid()), 'descriptor': client.view.descriptor,
          'dev': actual.st_dev, 'inode': actual.st_ino})
    for line in sys.stdin:
        request = json.loads(line)
        op = request['op']
        try:
            if op == 'allocate':
                result = asdict(client.allocate_range(request['nbytes']))
            elif op == 'acquire':
                block = PoolBlock.from_dict(request['block'])
                lease = client.acquire_range(block, request['mode'], request.get('generation', block.generation),
                                             request.get('offset', 0), request.get('nbytes'))
                leases[request['key']] = lease
                result = asdict(lease)
            elif op in ('write', 'verify'):
                lease = leases[request['key']]
                assert lease.mode == ('w' if op == 'write' else 'r')
                result = bytes_io(client.view.buffer, lease.offset, lease.nbytes, request['seed'], op == 'write')
            elif op == 'commit':
                lease = leases[request['key']]
                result = client.commit_write(lease, request['content_id'], lease.nbytes, request.get('operation_id'))
            elif op == 'release':
                result = client.release(leases[request['key']], request.get('operation_id'))
            elif op == 'unknown':
                result = client.report_unknown(leases[request['key']], reason='host probe explicit ambiguous CPU outcome')
            elif op == 'query':
                result = client.query_operation(request['operation_id'])
            elif op == 'snapshot':
                result = client.snapshot()
            elif op == 'export':
                assert exported is None
                exported = memoryview(client.view.buffer)
                result = {'exported': True}
            elif op == 'drop_export':
                exported.release()
                exported = None
                result = {'export_released': True}
            elif op == 'child':
                assert child is None
                fd = client.view.fileno()
                child = subprocess.Popen([sys.executable, '-u', __file__, '--child-fd', str(fd),
                                          '--nbytes', str(client.view.descriptor['nbytes'])],
                                         pass_fds=(fd,), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                         text=True, bufsize=1)
                result = read_line(child.stdout)
                assert result['dev'] == actual.st_dev and result['inode'] == actual.st_ino
                child_birth = result['birth']
                client.attach_child(child.pid, child_birth)
                result['attached'] = True
            elif op == 'detach_live':
                result = client.detach_child(child.pid, child_birth, native_drained=True)
            elif op == 'drain_child':
                child.stdin.write('{"close":true}\n')
                child.stdin.flush()
                result = read_line(child.stdout)
                assert result == {'unmapped': True, 'fd_closed': True, 'native_jobs': 0}
                assert child.wait(timeout=15) == 0
                result['detached'] = client.detach_child(child.pid, child_birth, native_drained=True)
                child.stdin.close()
                child.stdout.close()
                child = None
            elif op == 'begin':
                result = client.begin_unregister()
            elif op == 'close':
                client.view.close()
                result = {'view_closed': client.view.closed}
            elif op == 'ack':
                result = client.ack_unmapped(native_drained=request.get('native_drained', False))
            elif op == 'identity':
                current = os.fstat(client.view.fileno())
                result = {'dev': current.st_dev, 'inode': current.st_ino, 'view_closed': client.view.closed}
            elif op == 'exit':
                assert client._unmapped and child is None and exported is None
                emit({'ok': True, 'result': {'exiting_clean': True}})
                return
            else:
                raise ValueError('unknown peer command')
            emit({'ok': True, 'result': result})
        except Exception as error:
            emit({'ok': False, 'error_type': type(error).__name__, 'detail': str(error),
                  'operation_id': getattr(error, 'operation_id', None),
                  'view_closed': client.view.closed, 'children': list(client._children),
                  'local_lease_states': client._leases})
    raise RuntimeError('controller disappeared; no implicit unmap acknowledgement')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--endpoint')
    parser.add_argument('--pool-id')
    parser.add_argument('--epoch')
    parser.add_argument('--access', default='rw')
    parser.add_argument('--child-fd', type=int)
    parser.add_argument('--nbytes', type=int)
    arguments = parser.parse_args()
    if arguments.child_fd is not None:
        child_main(arguments.child_fd, arguments.nbytes)
    else:
        main(arguments)
