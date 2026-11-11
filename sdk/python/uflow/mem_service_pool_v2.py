"""Explicit import only: mem_service remote views, never POSIX FDs or mmap.

UFlow remains the range/commit authority. This object only sends bounded control
and pattern IDs to the Guest data plane; it never contains a foreign pointer.
"""
import os
import socket
import uuid
from ._managed_pool_wire import fields, integer, opaque, receive, send


class PoolV2Error(RuntimeError):
    def __init__(self, detail, operation_id, response=None):
        super().__init__(detail)
        self.operation_id, self.response = operation_id, response


class PoolV2Unknown(PoolV2Error):
    """Retain operation/client/lease; timeout or process death is not an ACK."""


def exchange(endpoint, op, values, *, operation_id=None):
    operation = opaque(operation_id or str(uuid.uuid4()))
    fds = []
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(130)
            connection.connect(endpoint)
            send(connection, {'schema': 2, 'op': op, 'operation_id': operation, **values})
            response, fds = receive(connection)
        fields(response, ('schema', 'status', 'operation_id'), ('result', 'code', 'detail'))
        if response['schema'] != 2 or type(response['schema']) is not int or response['operation_id'] != operation or fds:
            raise ValueError('v2 version/correlation/FD mismatch')
        if response['status'] == 'error':
            error = PoolV2Error if response.get('code') == 'Rejected' else PoolV2Unknown
            raise error(response.get('detail', 'unconfirmed outcome'), operation, response)
        if response['status'] != 'ok' or 'result' not in response:
            raise ValueError('invalid terminal response')
        return response['result']
    except PoolV2Error:
        raise
    except Exception as error:
        raise PoolV2Unknown(str(error), operation) from error
    finally:
        for fd in fds:
            os.close(fd)


def pool_descriptor(value):
    fields(value, ('schema','pool_id','pool_epoch','provider_session','provider_kind','nbytes',
                   'resource_id','placement_id','binding_id','resource_generation','canonical','state'))
    for name in ('pool_id', 'pool_epoch', 'provider_session'):
        opaque(value[name])
    for name in ('nbytes', 'resource_generation'):
        integer(value[name], positive=True)
    if value['schema'] != 2 or value['provider_kind'] != 'mem_service_obmm':
        raise ValueError('explicit mem_service schema2 descriptor required')
    if value['nbytes'] > 128 << 20:
        raise ValueError('B pool budget exceeded')
    receipt=value['canonical']
    if (receipt.get('schema')!=2 or receipt.get('phase')!='completed' or
            receipt.get('key')!=value['pool_id'] or receipt.get('generation')!=value['resource_generation'] or
            receipt.get('nbytes')!=value['nbytes']):
        raise ValueError('lower backing receipt differs from Catalog descriptor')
    return value


def access_binding(value, pool, node, access):
    fields(value, ('schema', 'backend', 'realm', 'node', 'boot_id', 'incarnation',
                   'pool_id', 'pool_epoch', 'provider_session', 'client_session',
                   'access_generation', 'rights', 'offset', 'nbytes', 'receipt'))
    for key in ('pool_id', 'pool_epoch', 'provider_session', 'nbytes'):
        if value[key] != pool[key]:
            raise ValueError('binding/pool identity mismatch')
    for key in ('realm', 'boot_id', 'client_session'):
        opaque(value[key])
    if not value['realm'].startswith('simulated:'):
        raise ValueError('explicit simulated execution realm required')
    for key in ('incarnation', 'access_generation'):
        integer(value[key], positive=True)
    if (value['schema'], value['backend'], value['node'], value['rights'], value['offset']) != (2, 'mem_service_obmm', node, access, 0):
        raise ValueError('backend/realm/node/rights/range mismatch')
    if value['receipt'].get('mapped') is not True or value['receipt'].get('holder') is not True:
        raise ValueError('real mapped holder receipt required')
    if (value['receipt'].get('client_session')!=value['client_session'] or
            value['receipt'].get('nbytes')!=pool['nbytes'] or
            value['receipt'].get('generation')!=pool['resource_generation'] or value['receipt'].get('pins')!=0):
        raise ValueError('mapping receipt identity/bounds mismatch')
    integer(value['receipt']['guest_pid'],positive=True)
    return value


class MemServicePoolClient:
    def __init__(self, endpoint, pool, node, access='rw', *, transport=exchange):
        self.endpoint, self.pool = endpoint, pool_descriptor(pool.copy())
        if node not in ('nodeA', 'nodeB') or access not in ('r', 'rw'):
            raise ValueError('logical node and explicit rights required')
        self.transport, self.closed, self.unknown = transport, False, {}
        registered = self._call('RegisterPoolClient', node=node, access=access)
        self.binding = access_binding(registered['access_binding_v2'], self.pool, node, access)
        if registered['client_session'] != self.binding['client_session'] or registered['access_generation'] != self.binding['access_generation']:
            raise ValueError('registration/binding identity mismatch')

    def _call(self, op, **values):
        if self.closed:
            raise ValueError('client is unmapped')
        base = {key: self.pool[key] for key in ('pool_id', 'pool_epoch', 'provider_session')}
        if hasattr(self, 'binding'):
            base.update({key: self.binding[key] for key in ('client_session', 'access_generation')})
            base['access_binding_v2']=self.binding
        try:
            return self.transport(self.endpoint, op, {**base, **values})
        except PoolV2Unknown as error:
            self.unknown[error.operation_id] = {'op': op, 'values': values}
            error.client = self  # Including an uncertain constructor/map.
            raise

    def allocate(self, size, alignment=65536):
        return self._call('AllocatePoolRange', nbytes=integer(size, positive=True), alignment=alignment)

    def acquire(self, block, mode, offset=0, nbytes=None):
        if mode not in ('r', 'w') or mode == 'w' and self.binding['rights'] != 'rw':
            raise ValueError('lease mode/rights mismatch')
        return self._call('AcquirePoolRange', block=block, mode=mode,
                          expected_generation=block['generation'], byte_offset=integer(offset),
                          nbytes=integer(block['nbytes']-offset if nbytes is None else nbytes, positive=True))

    def execute_pattern(self, lease, pattern_id):
        """Guest generates/verifies all bytes; IDs only, no user payload upload."""
        integer(pattern_id)
        if lease['client_session'] != self.binding['client_session']:
            raise ValueError('lease belongs to a different context')
        return self._call('ExecutePoolRange', lease=lease, pattern_id=pattern_id)

    def release(self, lease, *, write_not_started=False):
        return self._call('ReleasePoolLease', lease=lease, write_not_started=write_not_started)

    def snapshot(self):
        return self._call('GetManagedPoolSnapshot')

    def query(self, operation_id):
        # Queries never discard the original unknown context on their own.
        return self.transport(self.endpoint, 'QueryPoolOperation', {'target_operation_id': opaque(operation_id)})

    def close(self):
        result = self._call('BeginPoolUnregister')
        if result.get('unmapped') is not True:
            raise PoolV2Unknown('real lower unmap missing', 'close-unconfirmed', result)
        self.closed = True
        return result
