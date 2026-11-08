#!/usr/bin/env python3
"""C02 pool-v2 serial bridge and lifecycle-only lower wire relay.

Host parses metadata but never a Guest pointer. Public mem_service control wire
is relayed only for managed lifecycle/directory ops, never GET/PUT/materialize.
There is one host lower authority, not a second simulation Catalog.
"""
import json
import os
from pathlib import Path
import socket
import struct
import threading
import time
from uflow._pool_bridge_v2 import Frame, POOL, metadata

LOWER = struct.Struct('<IHHQIIIIIIQ')  # Frozen public native little-endian aarch64 ABI.


def notify_failure():
    descriptor = os.environ.get('UFLOW_ENV_CLOSE_FD')
    if descriptor is None:
        raise RuntimeError('accepted independent environment monitor required')
    try:
        os.write(int(descriptor), b'!')
    except BlockingIOError:
        pass  # Already pending notice; never wait in an exceptional boundary.


def exact(connection, size):
    output = bytearray()
    while len(output) < size:
        data = connection.recv(size-len(output))
        if not data:
            raise RuntimeError('unconfirmed control EOF')
        output.extend(data)
    return bytes(output)


def lower_request(data):
    if len(data) < LOWER.size:
        raise ValueError('truncated lower header')
    magic, version, length, request, op, flags, count, digest, status, error, stamp = LOWER.unpack_from(data)
    if (magic, version, length, flags, status, error) != (0x4d535643, 1, 48, 0, 0, 0):
        raise ValueError('lower request header/version/status mismatch')
    if op not in (1, 2, 3) and not 0x70 <= op <= 0x7d:
        raise ValueError('lower relay forbids payload/recovery/reference operations')
    if not request or count > 4096 or len(data) != 48 + count:
        raise ValueError('lower control length/identity bound')
    value = 2166136261 if count else 0
    for byte in data[48:]:
        value = ((value ^ byte) * 16777619) & 0xffffffff
    if value != digest:
        raise ValueError('lower control checksum')
    return request, op


class GuestNode:
    def __init__(self, channel, identity, lower_socket, event, realm):
        self.channel, self.identity, self.lower_socket, self.event = channel, identity, lower_socket, event
        self.realm = realm
        self.pending, self.lock, self.command_lock = {}, threading.Lock(), threading.Lock()
        self.started, self.drained, self.stop = threading.Event(), threading.Event(), threading.Event()
        self.failure = None
        self.thread = threading.Thread(target=self.pump, name='pool-'+identity['node'])

    def send(self, line):
        with self.lock:
            self.channel.send(line)

    def relay(self, identifier, data):
        request, op = lower_request(data)
        with socket.socket(socket.AF_UNIX) as lower:
            lower.settimeout(10)
            lower.connect(self.lower_socket)
            lower.sendall(data)
            header = exact(lower, LOWER.size)
            fields = LOWER.unpack(header)
            if fields[:3] != (0x4d535643, 1, 48) or fields[3] != request or fields[4] != op or fields[6] > 4096:
                raise ValueError('lower reply identity/length mismatch')
            response = header + exact(lower, fields[6])
        for offset in range(0, len(response), 1024):
            self.send('wire %s %s' % (identifier, response[offset:offset+1024].hex()))
        self.event({'event': 'lower-control', 'node': self.identity['node'], 'operation': op,
                    'request': request, 'request_bytes': len(data), 'response_bytes': len(response),
                    'status':fields[8],'error_code':fields[9],
                    'payload_plane': False})

    def pump(self):
        try:
            while not self.stop.is_set():
                try:
                    line = self.channel.line(time.monotonic()+.25)
                except (socket.timeout, TimeoutError):
                    continue
                if line.startswith('UFPOOL_RPC '):
                    _, identifier, encoded = line.split(' ', 2)
                    self.relay(int(identifier), bytes.fromhex(encoded))
                elif line.startswith('UFPOOL_REPLY '):
                    _, request, correlation, encoded = line.split(' ', 3)
                    key = (int(request), int(correlation))
                    if key not in self.pending:
                        raise ValueError('unsolicited/replayed response')
                    receipt = metadata(bytes.fromhex(encoded))
                    item = self.pending[key]
                    item['response'] = receipt
                    item['ready'].set()
                elif line == 'UFPOOL_STARTED':
                    self.started.set()
                elif line == 'UFPOOL_WORKERS_CLOSED':
                    self.drained.set()
                elif line == 'UFPOOL_FAILURE_NOTICE':
                    notify_failure()  # Immediately, before reading/serializing diagnostics.
                elif line.startswith(('UFPOOL_FATAL', 'UFLOW_ENV_FAIL')):
                    raise RuntimeError('Guest reported failure before diagnostics: '+line)
                # Kernel/boot lines remain in serial raw logs; no parsing as ACK.
        except BaseException as error:
            notify_failure()  # Must precede event serialization or cleanup.
            self.failure = error
            for item in self.pending.values():
                item['ready'].set()
            self.event({'event': 'guest-bridge-failure', 'node': self.identity['node'], 'detail': str(error)})

    def exchange(self, frame):
        # Serial transport lock is per node and held for one bounded operation;
        # no pool/global lock is held while A/B application leases stay alive.
        with self.command_lock:
            expected = self.identity
            if frame.realm != self.realm:
                raise ValueError('execution realm mismatch')
            if (frame.node, frame.boot, frame.session, frame.generation) != (
                    expected['node'], expected['boot_id'], expected['agent_session'], expected['incarnation']):
                raise ValueError('actual Guest boot/session/incarnation mismatch')
            body = metadata(frame.body)
            if len(frame.body) > 1800 or frame.kind != POOL or body.get('schema') != 2:
                raise ValueError('pool command/schema bound')
            item = {'ready': threading.Event()}
            key = (frame.request, frame.correlation)
            self.pending[key] = item
            try:
                self.send('v2 %d %d %s' % (*key, frame.body.hex()))
                if not item['ready'].wait(120) or self.failure:
                    raise RuntimeError('same Guest operation outcome unknown') from self.failure
                receipt = item['response']
                self.event({'event': 'guest-completion', 'node': frame.node, 'request': frame.request,
                            'correlation': frame.correlation, 'op': body['op'], 'receipt': receipt,
                            'host_monotonic_ns': time.monotonic_ns(), 'clock_domain': 'B-host'})
                return frame.completion(receipt)
            finally:
                self.pending.pop(key, None)

    def prepare_close(self):
        self.send('prepare-quit')
        if not self.drained.wait(10) or self.failure:
            raise RuntimeError('actual Guest worker/client drain not confirmed')
        self.stop.set()
        self.thread.join(1)
        if self.thread.is_alive():
            raise RuntimeError('bridge thread did not stop before handing Channel to QMP helper')


class BridgeServer:
    def __init__(self, path, node):
        self.path, self.node = Path(path), node
        self.listener = socket.socket(socket.AF_UNIX)
        self.listener.bind(str(self.path))
        os.chmod(self.path, 0o600)
        self.listener.listen(16)
        self.listener.settimeout(.1)
        self.stop = threading.Event()
        self.failure = None
        self.thread = threading.Thread(target=self.serve, name='pool-endpoint-'+node.identity['node'])

    def serve(self):
        try:
            while not self.stop.is_set():
                try:
                    connection, _ = self.listener.accept()
                except socket.timeout:
                    continue
                with connection:
                    connection.settimeout(130)
                    count, = struct.unpack('!I', exact(connection, 4))
                    if not 68 <= count <= 65536+68+4*65535:
                        raise ValueError('bridge frame size limit')
                    request = Frame.decode(exact(connection, count))
                    response = self.node.exchange(request).encode()
                    connection.sendall(struct.pack('!I', len(response))+response)
        except BaseException as error:
            notify_failure()
            self.failure = error

    def close(self):
        self.stop.set()
        self.thread.join(1)
        if self.thread.is_alive():
            raise RuntimeError('endpoint active at shutdown')
        self.listener.close()
        self.path.unlink()
