"""UFlow-owned Simpler executor: real backend handles only, never resource authority.

This adapter alone imports the frozen runtime. It does not create a Worker,
initialize a device, maintain quotas/range locks, or copy any payload to clients.
"""
import hashlib
import json
import os
from pathlib import Path
import socket
import sys
import time

from ._managed_pool_wire import fields, integer, opaque, peercred, receive, send

SIMPLER_REVISION = '508becb93e182a3035a14e3f9226929dbe994346'


def descriptor(buffer):
    buffer.to_descriptor()  # Actual frozen Buffer ABI validator; no native object/VA on the wire.
    info = os.fstat(buffer.shm._fd)  # Delimited CPython SharedMemory provider boundary.
    return {'kind': 'posix_shm_simpler', 'owner_instance_id': bytes(buffer.identity.owner_instance_id).hex(),
            'buffer_id': int(buffer.identity.buffer_id), 'generation': int(buffer.identity.generation),
            'address_space': int(buffer.address_space), 'access': int(buffer.access),
            'backend_kind': int(buffer.backend_kind), 'nbytes': int(buffer.nbytes),
            'body_hex': bytes(buffer.body).hex(), 'owner_worker_path_id': int(buffer.owner_worker_path_id),
            'dev': info.st_dev, 'inode': info.st_ino, 'native_descriptor': 'validated-Simpler-Buffer-ABI-v1'}


def serve(endpoint, parent_pid, provider_session, log_path):
    # Actual owner-side API, not a torch/memfd surrogate or a mirrored KV pool.
    from simpler.buffer import create_host_shared_buffer, mint_owner_instance_id
    import simpler.buffer as simpler_buffer

    owner = mint_owner_instance_id()
    handles, release_results, held_exports = {}, {}, {}
    hold_once = os.environ.get('UFLOW_POOL_TEST_HOLD_RELEASE_ONCE') == '1'
    source = Path(simpler_buffer.__file__).resolve()
    loaded = {'provider_session': provider_session, 'pid': os.getpid(), 'parent_pid': parent_pid,
              'simpler_revision': SIMPLER_REVISION, 'simpler_module': str(source),
              'simpler_sha256': hashlib.sha256(source.read_bytes()).hexdigest(),
              'task_interface_module': str(Path(sys.modules['_task_interface'].__file__).resolve()),
              'task_interface_sha256': hashlib.sha256(Path(sys.modules['_task_interface'].__file__).read_bytes()).hexdigest(),
              'adapter': 'simpler508-cpython-owner-fd-v1', 'test_hold_release_once': hold_once}
    def event(value):
        with Path(log_path).open('a') as stream:
            stream.write(json.dumps(value, sort_keys=True)+'\n')
            stream.flush()
            os.fsync(stream.fileno())
    event({'event': 'loaded', **loaded})
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
        server.bind(endpoint)
        os.chmod(endpoint, 0o600)
        server.listen(8)
        while True:
            with server.accept()[0] as connection:
                connection.settimeout(5)
                sent_fd = None
                incoming = []
                try:
                    pid, uid, _ = peercred(connection)
                    if pid != parent_pid or uid != os.geteuid():
                        raise ValueError('provider accepts only its owning daemon')
                    request, incoming = receive(connection)
                    if incoming:
                        raise ValueError('provider does not accept client backing FDs')
                    fields(request, ('schema', 'op', 'provider_session'), ('resource_id', 'nbytes', 'buffer_id'))
                    if type(request['schema']) is not int or request['schema'] != 1 or request['provider_session'] != provider_session:
                        raise ValueError('provider protocol/session mismatch')
                    op = request['op']
                    if op == 'identify':
                        result = loaded
                    elif op == 'create':
                        key, size = opaque(request['resource_id']), integer(request['nbytes'], positive=True)
                        buffer_id = integer(request['buffer_id'], positive=True)
                        if size > 256*1024*1024 or len(handles) >= 16 and key not in handles:
                            raise ValueError('bounded provider executor capacity')
                        if key in release_results:
                            raise ValueError('backend identity already released')
                        if key not in handles:
                            if os.environ.get('UFLOW_POOL_TEST_DELAY_SECOND_CREATE') == '1' and len(handles) == 1:
                                event({'event': 'test_second_create_delay', 'seconds': 2})
                                time.sleep(2)  # Test-only executor wait; no Catalog or payload work here.
                            handles[key] = create_host_shared_buffer(size, owner, buffer_id, generation=1)
                        buffer = handles[key]
                        if buffer.nbytes != size or buffer.identity.buffer_id != buffer_id:
                            raise ValueError('backend create retry changed identity')
                        result = descriptor(buffer)
                        sent_fd = buffer.shm._fd
                    elif op == 'close':
                        key = opaque(request['resource_id'])
                        if key in release_results:
                            result = release_results[key]
                        else:
                            buffer = handles[key]
                            name = bytes(buffer.body).decode()
                            before = os.fstat(buffer.shm._fd)
                            if hold_once and key not in held_exports:
                                held_exports[key] = memoryview(buffer.shm.buf)
                                hold_once = False
                            elif key in held_exports:
                                held_exports.pop(key).release()
                            buffer.close()  # Real close and unlink; failure retains the handle.
                            result = {'closed': bool(buffer.closed and buffer.shm is None),
                                      'unlinked': bool(buffer.unlinked), 'name_absent': not (Path('/dev/shm')/name).exists(),
                                      'dev': before.st_dev, 'inode': before.st_ino}
                            if not all(result[k] for k in ('closed', 'unlinked', 'name_absent')):
                                raise RuntimeError('provider close not confirmed')
                            release_results[key] = result
                            del handles[key]
                    elif op == 'shutdown':
                        if handles:
                            raise RuntimeError('provider still owns backing; shutdown forbidden')
                        send(connection, {'schema': 1, 'status': 'ok', 'result': {'drained': True}})
                        event({'event': 'provider_stopped', 'owned_buffers': 0})
                        break
                    else:
                        raise ValueError('unknown provider operation')
                    event({'event': op, 'resource_id': request.get('resource_id'), 'result': result})
                    send(connection, {'schema': 1, 'status': 'ok', 'result': result}, sent_fd)
                except Exception as error:
                    note = {'schema': 1, 'status': 'error', 'detail': str(error), 'error_type': type(error).__name__}
                    event({'event': 'provider_error', **note})
                    try:
                        send(connection, note)
                    except OSError:
                        pass
                finally:
                    for fd in incoming:
                        os.close(fd)
    Path(endpoint).unlink()  # Only after this executor has no owner handles.
    # CPython's owner-side tracker normally receives EOF on interpreter exit.
    # Close its pipe and wait here so provider exit also confirms its own helper
    # has drained. This sends no signal and is never used with live buffers.
    from multiprocessing import resource_tracker
    resource_tracker._resource_tracker._stop()
    event({'event': 'resource_tracker_stopped_after_zero_owners', 'owned_buffers': 0})


if __name__ == '__main__':
    if len(sys.argv) != 5:
        raise SystemExit('pool_provider ENDPOINT PARENT_PID SESSION LOG')
    serve(sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4])
