"""Strict managed-pool control SDK; direct mmap bytes stay on the client data plane.

Completion is cooperative: callers keep a lease through actual native completion
and result cleanup. A timeout/PID observation never releases that responsibility.
No tensor, provider or device library is imported by this module.
"""
from dataclasses import asdict, dataclass
import mmap
import os
import socket
import stat
import uuid
import weakref

from ._managed_pool_wire import fields, integer, opaque, process_birth, receive, send


class ManagedPoolError(RuntimeError):
    def __init__(self, detail, *, operation_id, response=None, client=None):
        super().__init__(detail)
        self.operation_id, self.response, self.client = operation_id, response, client


class ManagedPoolUnknown(ManagedPoolError):
    """Unconfirmed outcome: retain the client/lease and query this operation ID."""


def _operation(value):
    return opaque(value) if value is not None else str(uuid.uuid4())


def _exchange(endpoint, op, values, operation_id=None, timeout=5.0, *, client=None, expect_fd=False):
    operation_id = _operation(operation_id)
    fds = []
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.settimeout(timeout)
            sock.connect(endpoint)
            send(sock, {'schema': 1, 'op': op, 'operation_id': operation_id, **values})
            response, fds = receive(sock)
        fields(response, ('schema', 'status', 'operation_id'), ('result', 'code', 'detail'))
        if type(response['schema']) is not int or response['schema'] != 1 or response['operation_id'] != operation_id:
            raise ValueError('managed response version/operation mismatch')
        if response['status'] == 'error':
            fields(response, ('schema', 'status', 'operation_id', 'code', 'detail'), ('result',))
            if fds or response['code'] not in ('Rejected', 'Unknown'):
                raise ValueError('invalid managed error response/FD')
            error_type = ManagedPoolUnknown if response['code'] == 'Unknown' else ManagedPoolError
            raise error_type(opaque(response['detail']), operation_id=operation_id, response=response, client=client)
        fields(response, ('schema', 'status', 'operation_id', 'result'))
        if response['status'] != 'ok' or len(fds) != int(expect_fd):
            raise ValueError('managed success/FD count mismatch')
        retained = fds
        fds = []
        return response['result'], retained
    except ManagedPoolError:
        raise
    except Exception as error:
        raise ManagedPoolUnknown(str(error), operation_id=operation_id, client=client) from error
    finally:
        for fd in fds:
            os.close(fd)


POOL_FIELDS = ('schema', 'pool_id', 'pool_epoch', 'provider_session', 'provider_kind', 'nbytes',
               'resource_id', 'placement_id', 'binding_id', 'resource_generation', 'canonical', 'state')


def _pool_descriptor(value, *, registered=False):
    extra = ('client_session', 'access_generation', 'access') if registered else ()
    fields(value, (*POOL_FIELDS, *extra))
    for name in ('pool_id', 'pool_epoch', 'provider_session', 'state'):
        opaque(value[name])
    for name in ('schema', 'nbytes', 'resource_id', 'placement_id', 'binding_id', 'resource_generation'):
        integer(value[name], positive=True)
    if value['schema'] != 1 or value['provider_kind'] != 'posix_shm_simpler' or value['resource_generation'] != 1:
        raise ValueError('managed pool provider/version mismatch')
    if value['canonical'] is not None:
        canonical = value['canonical']
        fields(canonical, ('kind', 'owner_instance_id', 'buffer_id', 'generation', 'address_space', 'access',
                           'backend_kind', 'nbytes', 'body_hex', 'owner_worker_path_id', 'dev', 'inode', 'native_descriptor'))
        if (canonical['kind'] != 'posix_shm_simpler' or canonical['nbytes'] != value['nbytes']
                or canonical['buffer_id'] != value['resource_id'] or canonical['generation'] != 1
                or canonical['address_space'] != 0 or canonical['backend_kind'] != 1 or canonical['access'] != 2):
            raise ValueError('canonical Simpler provider identity mismatch')
        for key in ('buffer_id', 'generation', 'nbytes', 'inode'):
            integer(canonical[key], positive=True)
        for key in ('dev', 'owner_worker_path_id', 'address_space', 'access', 'backend_kind'):
            integer(canonical[key])
        nonce = bytes.fromhex(canonical['owner_instance_id'])
        if len(nonce) != 8 or not any(nonce) or not bytes.fromhex(canonical['body_hex']):
            raise ValueError('canonical owner/body identity')
    if registered:
        opaque(value['client_session'])
        integer(value['access_generation'], positive=True)
        if value['access'] not in ('r', 'rw'):
            raise ValueError('managed access mode')
        if value['canonical'] is None or value['state'] != 'Allocated':
            raise ValueError('registration requires an actual allocated backing')
    return value


def _queried_result(value, target, client=None):
    try:
        fields(value, ('state', 'response'))
        state, response = value['state'], value['response']
        if state == 'NotFound' and response is None:
            return value
        if state not in ('Completed', 'Rejected', 'Unknown', 'Pending'):
            raise ValueError('unknown operation state')
        fields(response, ('schema', 'status', 'operation_id'), ('result', 'code', 'detail'))
        if type(response['schema']) is not int or response['schema'] != 1 or response['operation_id'] != target:
            raise ValueError('query did not return the exact original operation')
        if state == 'Completed':
            fields(response, ('schema', 'status', 'operation_id', 'result'))
            if response['status'] != 'ok' or type(response['result']) is not dict:
                raise ValueError('completed operation result malformed')
        else:
            fields(response, ('schema', 'status', 'operation_id', 'code', 'detail'), ('result',))
            expected = 'Rejected' if state == 'Rejected' else 'Unknown'
            if response['status'] != 'error' or response['code'] != expected:
                raise ValueError('unconfirmed operation falsely represented as success')
        return value
    except (ValueError, KeyError, TypeError) as error:
        raise ManagedPoolUnknown(str(error), operation_id=target, client=client) from error


@dataclass(frozen=True)
class PoolBlock:
    pool_id: str
    pool_epoch: str
    provider_session: str
    block_id: int
    generation: int
    offset: int
    nbytes: int

    def __post_init__(self):
        for name in ('pool_id', 'pool_epoch', 'provider_session'):
            opaque(getattr(self, name))
        for name in ('block_id', 'generation', 'nbytes'):
            integer(getattr(self, name), positive=True)
        integer(self.offset)
        if self.offset + self.nbytes > (1 << 64) - 1:
            raise ValueError('allocation overflow')

    @classmethod
    def from_dict(cls, value):
        fields(value, cls.__dataclass_fields__)
        return cls(**value)


@dataclass(frozen=True)
class PoolLease:
    pool_id: str
    pool_epoch: str
    provider_session: str
    access_generation: int
    lease_id: str
    lease_generation: int
    client_session: str
    block_id: int
    generation: int
    offset: int
    nbytes: int
    mode: str
    content_version: int
    content_id: str

    def __post_init__(self):
        for name in ('pool_id', 'pool_epoch', 'provider_session', 'lease_id', 'client_session'):
            opaque(getattr(self, name))
        for name in ('access_generation', 'lease_generation', 'block_id', 'generation', 'nbytes'):
            integer(getattr(self, name), positive=True)
        integer(self.offset)
        integer(self.content_version)
        if self.mode not in ('r', 'w') or type(self.content_id) is not str:
            raise ValueError('lease mode/content identity')
        if self.mode == 'r' and (self.content_version == 0 or not self.content_id):
            raise ValueError('read lease requires committed content identity')
        if self.mode == 'w' and (self.content_version != 0 or self.content_id):
            raise ValueError('write lease cannot claim committed content')
        if self.offset + self.nbytes > (1 << 64) - 1:
            raise ValueError('lease range overflow')

    @classmethod
    def from_dict(cls, value):
        fields(value, cls.__dataclass_fields__)
        return cls(**value)


class ManagedPoolView:
    """One mapped FD access; close failures preserve server and local obligations."""
    def __init__(self, client, descriptor, fd):
        import fcntl  # Linux adapter boundary; normal SDK import stays portable stdlib.

        self.client, self.descriptor, self._fd = client, descriptor, fd
        self._mapping, self._map_closed, self._fd_closed = None, False, False
        self.close_error = None
        canonical = descriptor['canonical']
        actual = os.fstat(fd)
        flags = fcntl.fcntl(fd, fcntl.F_GETFL)
        expected_flags = os.O_RDWR if descriptor['access'] == 'rw' else os.O_RDONLY
        if (not stat.S_ISREG(actual.st_mode) or actual.st_size != descriptor['nbytes']
                or actual.st_dev != canonical['dev'] or actual.st_ino != canonical['inode']
                or flags & os.O_ACCMODE != expected_flags or flags & os.O_APPEND):
            raise ValueError('actual managed FD/descriptor/access mismatch')
        protection = mmap.PROT_READ | (mmap.PROT_WRITE if descriptor['access'] == 'rw' else 0)
        self._mapping = mmap.mmap(fd, descriptor['nbytes'], flags=mmap.MAP_SHARED, prot=protection)

    @property
    def buffer(self):
        if self.closed or self._mapping is None:
            raise RuntimeError('managed view is closed/unavailable')
        return self._mapping

    @property
    def closed(self):
        return self._map_closed and self._fd_closed

    def fileno(self):
        if self._fd_closed:
            raise RuntimeError('managed FD closed')
        return self._fd

    def close(self):
        if self.closed:
            return
        if self.client._children:
            raise RuntimeError('registered child obligations remain; drain and detach first')
        try:
            if not self._map_closed:
                if self._mapping is not None:
                    self._mapping.close()  # BufferError from a real derived view must propagate.
                self._map_closed = True
            if not self._fd_closed:
                os.close(self._fd)
                self._fd_closed = True
        except Exception as error:
            self.close_error = repr(error)
            try:
                self.client.report_unknown(reason='mapped view/FD close failed; obligation retained')
            except Exception:
                pass  # Never mask the actual close failure or claim a confirmed unmap.
            raise


class ManagedPoolClient:
    _registrations = weakref.WeakValueDictionary()

    @classmethod
    def register(cls, endpoint, pool_id, expected_epoch, access='rw', *, operation_id=None, timeout=5.0):
        opaque(pool_id), opaque(expected_epoch)
        if access not in ('r', 'rw'):
            raise ValueError('access must be r or rw')
        operation_id = _operation(operation_id)
        key = (os.getpid(), endpoint, operation_id)
        previous = cls._registrations.get(key)
        if previous is not None:
            if previous._requested_registration != (pool_id, expected_epoch, access):
                raise ManagedPoolError('operation_id reused with different registration', operation_id=operation_id)
            if previous._registered and not previous.view.closed:
                return previous
            raise ManagedPoolUnknown('query/reconcile the original registration; no second mapping',
                                     operation_id=operation_id, client=previous)
        descriptor = describe_pool(endpoint, pool_id, expected_epoch, timeout=timeout)
        if descriptor['state'] != 'Allocated':
            raise ManagedPoolError('pool not allocated/admitting', operation_id=operation_id)
        client = cls()
        client.endpoint, client.timeout, client._descriptor = endpoint, timeout, descriptor
        client._registered, client._stopping, client._unmapped = False, False, False
        client._children, client._child_operations, client._leases, client.view = {}, {}, {}, None
        client.registration_operation_id, client._ack_operation_id = operation_id, None
        client._requested_registration = (pool_id, expected_epoch, access)
        cls._registrations[key] = client
        fds = []
        try:
            values = {'pool_id': pool_id, 'pool_epoch': expected_epoch,
                      'provider_session': descriptor['provider_session'], 'access': access}
            result, fds = _exchange(endpoint, 'RegisterPoolClient', values, operation_id, timeout,
                                    client=client, expect_fd=True)
            _pool_descriptor(result, registered=True)
            if any(result[name] != descriptor[name] for name in POOL_FIELDS):
                raise ValueError('registration changed trusted pool descriptor')
            client._descriptor = result
            fd = fds.pop()
            # Retain the local object even if fstat/map/ACK fails. The exception
            # carries this client; no destructor silently clears access state.
            view = ManagedPoolView.__new__(ManagedPoolView)
            client.view = view
            ManagedPoolView.__init__(view, client, result, fd)
            client._ack_operation_id = _operation(None)
            canonical = result['canonical']
            ack = client._call('AckPoolMapping', {'dev': canonical['dev'], 'inode': canonical['inode'],
                              'nbytes': result['nbytes'], 'access': access}, client._ack_operation_id)
            fields(ack, ('mapped',))
            if ack['mapped'] is not True:
                raise ValueError('mapping ACK unconfirmed')
            client._registered = True
            return client
        except ManagedPoolError:
            raise
        except Exception as error:
            raise ManagedPoolUnknown(str(error), operation_id=operation_id, client=client) from error
        finally:
            for fd in fds:
                os.close(fd)

    def _base(self):
        return {name: self._descriptor[name] for name in ('pool_id', 'pool_epoch', 'provider_session',
                                                         'client_session', 'access_generation')}

    def _call(self, op, values=None, operation_id=None):
        result, _ = _exchange(self.endpoint, op, {**self._base(), **(values or {})}, operation_id,
                              self.timeout, client=self)
        return result

    def _admit(self):
        if not self._registered or self._stopping or self.view.closed:
            raise RuntimeError('managed access is not accepting new work')

    def _lease(self, lease):
        if type(lease) is not PoolLease or any(getattr(lease, key) != self._descriptor[key]
                                              for key in ('pool_id', 'pool_epoch', 'provider_session', 'client_session', 'access_generation')):
            raise ValueError('foreign/stale full lease identity')
        return asdict(lease)

    def allocate_range(self, nbytes, alignment=65536, *, operation_id=None):
        self._admit()
        operation_id = _operation(operation_id)
        result = self._call('AllocatePoolRange', {'nbytes': integer(nbytes, positive=True),
                            'alignment': integer(alignment, positive=True)}, operation_id)
        try:
            block = PoolBlock.from_dict(result)
            if any(getattr(block, key) != self._descriptor[key] for key in ('pool_id', 'pool_epoch', 'provider_session')):
                raise ValueError('allocation changed trusted pool identity')
            return block
        except ValueError as error:
            raise ManagedPoolUnknown(str(error), operation_id=operation_id, client=self) from error

    def acquire_range(self, block, mode, expected_generation, byte_offset=0, nbytes=None, *, operation_id=None):
        self._admit()
        if type(block) is not PoolBlock or mode not in ('r', 'w'):
            raise ValueError('typed block and r/w mode required')
        integer(byte_offset)
        length = block.nbytes-byte_offset if nbytes is None else integer(nbytes, positive=True)
        if length <= 0 or byte_offset+length > block.nbytes:
            raise ValueError('subrange outside allocation')
        operation_id = _operation(operation_id)
        result = self._call('AcquirePoolRange', {'block': asdict(block), 'mode': mode,
                            'expected_generation': integer(expected_generation, positive=True),
                            'byte_offset': byte_offset, 'nbytes': length}, operation_id)
        try:
            lease = PoolLease.from_dict(result)
            self._lease(lease)
            if (lease.offset != block.offset+byte_offset or lease.nbytes != length or lease.mode != mode
                    or lease.block_id != block.block_id or lease.generation != expected_generation
                    or lease.generation != block.generation):
                raise ValueError('returned lease changed requested range/mode')
        except ValueError as error:
            raise ManagedPoolUnknown(str(error), operation_id=operation_id, client=self) from error
        self._leases[lease.lease_id] = 'Active'
        return lease

    def commit_write(self, lease, content_id, nbytes, operation_id=None):
        operation_id = _operation(operation_id)
        result = self._call('CommitPoolWrite', {'lease': self._lease(lease), 'content_id': opaque(content_id),
                            'nbytes': integer(nbytes, positive=True)}, operation_id)
        if (type(result) is not dict or set(result) != {'committed', 'content_version', 'content_id', 'lease_id'}
                or result['committed'] is not True or result['lease_id'] != lease.lease_id
                or result['content_id'] != content_id or type(result['content_version']) is not int or result['content_version'] <= 0):
            raise ManagedPoolUnknown('commit ACK not confirmed; query operation', operation_id=operation_id, client=self)
        self._leases[lease.lease_id] = 'Closed'
        return result

    def release(self, lease, operation_id=None):
        # A W release is the caller's explicit assertion that writing never
        # started. An in-flight/ambiguous write must instead report_unknown.
        if self._leases.get(lease.lease_id) == 'Unknown':
            raise RuntimeError('unknown lease cannot be normally released')
        operation_id = _operation(operation_id)
        result = self._call('ReleasePoolLease', {'lease': self._lease(lease),
                            'write_not_started': lease.mode == 'w'}, operation_id)
        if type(result) is not dict or set(result) != {'released', 'lease_id'} or result['released'] is not True or result['lease_id'] != lease.lease_id:
            raise ManagedPoolUnknown('release ACK not confirmed; query operation', operation_id=operation_id, client=self)
        self._leases[lease.lease_id] = 'Closed'
        return result

    def query_operation(self, operation_id):
        result, _ = _exchange(self.endpoint, 'QueryPoolOperation', {'target_operation_id': opaque(operation_id)},
                              timeout=self.timeout, client=self)
        _queried_result(result, operation_id, self)
        response = result.get('response')
        if result.get('state') == 'Completed' and response and response.get('status') == 'ok':
            value = response['result']
            if operation_id == self._ack_operation_id and value.get('mapped'):
                fields(value, ('mapped',))
                if value['mapped'] is not True:
                    raise ManagedPoolUnknown('queried mapping ACK malformed', operation_id=operation_id, client=self)
                self._registered = True
            terminal = value.get('lease_id') and ('committed' in value or 'released' in value)
            if terminal:
                try:
                    if 'committed' in value:
                        fields(value, ('committed', 'lease_id', 'content_version', 'content_id'))
                        integer(value['content_version'], positive=True)
                        opaque(value['content_id'])
                        if value['committed'] is not True:
                            raise ValueError('queried commit not confirmed')
                    else:
                        fields(value, ('released', 'lease_id'))
                        if value['released'] is not True:
                            raise ValueError('queried release not confirmed')
                except (ValueError, TypeError) as error:
                    raise ManagedPoolUnknown(str(error), operation_id=operation_id, client=self) from error
                self._leases[value['lease_id']] = 'Closed'
            if operation_id in self._child_operations:
                action, pid, birth = self._child_operations[operation_id]
                expected = {'attached': True} if action == 'attach' else {'detached': True, 'native_drain_attested': True}
                if value != expected:
                    raise ManagedPoolUnknown('queried child ACK malformed', operation_id=operation_id, client=self)
                if action == 'attach':
                    self._children[pid] = birth
                else:
                    self._children.pop(pid, None)
                del self._child_operations[operation_id]
        return result

    def snapshot(self):
        return self._call('GetManagedPoolSnapshot')

    def attach_child(self, pid, birth, *, operation_id=None):
        self._admit()
        operation_id = _operation(operation_id)
        try:
            result = self._call('AttachPoolChild', {'pid': integer(pid, positive=True), 'birth': birth}, operation_id)
            if result != {'attached': True} or type(result.get('attached')) is not bool:
                raise ManagedPoolUnknown('child attach ACK not confirmed', operation_id=operation_id, client=self)
        except ManagedPoolUnknown:
            self._children[pid] = birth
            self._child_operations[operation_id] = ('attach', pid, birth)
            raise
        self._children[pid] = birth
        return result

    def detach_child(self, pid, birth, *, native_drained, operation_id=None):
        if native_drained is not True:
            raise ValueError('actual native drain must be explicitly confirmed')
        operation_id = _operation(operation_id)
        try:
            result = self._call('DetachPoolChild', {'pid': integer(pid, positive=True), 'birth': birth,
                                'native_drained': True}, operation_id)
            if result != {'detached': True, 'native_drain_attested': True} or any(type(v) is not bool for v in result.values()):
                raise ManagedPoolUnknown('child detach ACK not confirmed', operation_id=operation_id, client=self)
        except ManagedPoolUnknown:
            self._child_operations[operation_id] = ('detach', pid, birth)
            raise
        self._children.pop(pid, None)
        return result

    def report_unknown(self, lease=None, *, reason, operation_id=None):
        values = {'reason': opaque(reason)}
        if lease is not None:
            values['lease'] = self._lease(lease)
            self._leases[lease.lease_id] = 'Unknown'
        return self._call('ReportPoolUnknown', values, operation_id)

    def begin_unregister(self, *, operation_id=None):
        self._stopping = True
        return self._call('BeginPoolUnregister', operation_id=operation_id)

    def ack_unmapped(self, *, native_drained=False, operation_id=None):
        if not self._stopping or self._children or self.view is None or not self.view.closed:
            raise RuntimeError('explicit stop, drained children and successful local unmap/FD close required')
        if type(native_drained) is not bool:
            raise ValueError('native_drained must be bool')
        operation_id = _operation(operation_id)
        result = self._call('AckPoolUnmapped', {'unmapped': True, 'fd_closed': True,
                            'native_drained': native_drained}, operation_id)
        if (type(result) is not dict or set(result) != {'unmapped', 'unknown_content_invalid_not_committed'}
                or result['unmapped'] is not True or type(result['unknown_content_invalid_not_committed']) is not bool):
            raise ManagedPoolUnknown('unmap ACK not confirmed; query operation', operation_id=operation_id, client=self)
        self._registered, self._unmapped = False, True
        if native_drained:
            self._leases = {key: 'Closed' for key in self._leases}
        return result


def create_pool(endpoint, *, admin_token, nbytes=134217728, operation_id=None, timeout=10.0):
    operation_id = _operation(operation_id)
    result, _ = _exchange(endpoint, 'CreateManagedPool', {'admin_token': opaque(admin_token),
                          'nbytes': integer(nbytes, positive=True)}, operation_id, timeout)
    try:
        return _pool_descriptor(result)
    except (ValueError, KeyError, TypeError) as error:
        raise ManagedPoolUnknown(str(error), operation_id=operation_id) from error


def describe_pool(endpoint, pool_id, expected_epoch, *, timeout=5.0):
    operation_id = _operation(None)
    result, _ = _exchange(endpoint, 'DescribeManagedPool', {'pool_id': opaque(pool_id),
                          'pool_epoch': opaque(expected_epoch)}, operation_id, timeout=timeout)
    try:
        _pool_descriptor(result)
        if result['pool_id'] != pool_id or result['pool_epoch'] != expected_epoch:
            raise ValueError('description changed expected pool identity')
        return result
    except (ValueError, KeyError, TypeError) as error:
        raise ManagedPoolUnknown(str(error), operation_id=operation_id) from error


def destroy_pool(endpoint, descriptor, *, admin_token, operation_id=None, timeout=10.0):
    operation_id = _operation(operation_id)
    result = _exchange(endpoint, 'DestroyManagedPool', {**{key: descriptor[key] for key in ('pool_id', 'pool_epoch', 'provider_session')},
                       'admin_token': opaque(admin_token)}, operation_id, timeout)[0]
    try:
        fields(result, ('released', 'provider_release', 'daemon_fd_closed'))
        released = result['provider_release']
        fields(released, ('closed', 'unlinked', 'name_absent', 'dev', 'inode'))
        if (result['released'] is not True or result['daemon_fd_closed'] is not True
                or not all(released[key] is True for key in ('closed', 'unlinked', 'name_absent'))
                or released['dev'] != descriptor['canonical']['dev'] or released['inode'] != descriptor['canonical']['inode']):
            raise ValueError('physical provider release identity/result unconfirmed')
        return result
    except (ValueError, KeyError, TypeError) as error:
        raise ManagedPoolUnknown(str(error), operation_id=operation_id) from error


def shutdown_pool_service(endpoint, *, admin_token, operation_id=None, timeout=10.0):
    return _exchange(endpoint, 'ShutdownManagedPool', {'admin_token': opaque(admin_token)}, operation_id, timeout)[0]


def query_pool_operation(endpoint, operation_id, *, timeout=5.0):
    result = _exchange(endpoint, 'QueryPoolOperation', {'target_operation_id': opaque(operation_id)}, timeout=timeout)[0]
    return _queried_result(result, operation_id)


__all__ = ['ManagedPoolClient', 'ManagedPoolView', 'PoolBlock', 'PoolLease', 'ManagedPoolError', 'ManagedPoolUnknown',
           'create_pool', 'describe_pool', 'destroy_pool', 'shutdown_pool_service', 'query_pool_operation', 'process_birth']
