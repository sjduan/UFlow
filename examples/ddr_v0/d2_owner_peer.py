#!/usr/bin/env python3
"""Ordinary cooperative owner. No authority paths, witness, or daemon controls.

--legacy-sdk selects an actual archived pre-T3 external_ddr.py for six-op
compatibility, not the light-package test. Otherwise use the normal uflow entry.
Fault injection is restricted to replaying a previously observed old response
into the SDK compensation branches; every compensating request uses the real
broker socket. It never supplies business success or capability evidence.
"""
from __future__ import annotations

import argparse
import dataclasses
import fcntl
import importlib
import json
import os
from pathlib import Path
import sys
import time
import traceback


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--endpoint', required=True)
    parser.add_argument('--seed', type=int, required=True)
    parser.add_argument('--legacy-sdk', type=Path)
    args = parser.parse_args()
    if args.legacy_sdk:
        sys.path.insert(0, str(args.legacy_sdk.parent))
        sdk = importlib.import_module(args.legacy_sdk.stem)
    else:
        from uflow import ExternalDdrClient
        sdk = importlib.import_module(ExternalDdrClient.__module__)
    client = sdk.ExternalDdrClient(args.endpoint, timeout=30)
    handles, fds, views = [], [], []

    def register(size):
        fd = os.memfd_create('ordinary-owner', os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
        os.ftruncate(fd, size)
        os.pwrite(fd, bytes([args.seed]) * min(size, 8192), 0)
        fcntl.fcntl(fd, fcntl.F_ADD_SEALS,
                    fcntl.F_SEAL_GROW | fcntl.F_SEAL_SHRINK | fcntl.F_SEAL_WRITE)
        try:
            handle = client.register(fd, name='ordinary', owner_name='ordinary', size_bytes=size)
        except BaseException:
            os.close(fd)
            raise
        handles.append(handle)
        fds.append(fd)
        return dataclasses.asdict(handle)

    def denied(action):
        try:
            action()
        except sdk.ExternalDdrError as error:
            return str(error)
        raise AssertionError('old identity unexpectedly accepted')

    def emit(value):
        print(json.dumps(value), flush=True)

    emit({'pid': os.getpid(), 'sdk': sdk.__file__, 'handle': register(8192)})
    try:
        for line in sys.stdin:
            req = json.loads(line)
            try:
                op = req['op']
                if op == 'read':
                    accepted = {field.name for field in dataclasses.fields(sdk.ExternalBackingHandle)}
                    handle = sdk.ExternalBackingHandle(**{key: value for key, value in req['handle'].items()
                                                         if key in accepted})
                    original = client._exchange
                    captured = {}

                    def observe(*a, **kw):
                        result = original(*a, **kw)
                        captured.update(result[0])
                        return result

                    client._exchange = observe
                    try:
                        view = client.acquire(handle, offset_bytes=17, nbytes=128)
                    finally:
                        client._exchange = original
                    views.append(view)
                    assert view.read() == bytes([req['seed']]) * 128
                    child = view.retain(7, 32)
                    assert child.read() == bytes([req['seed']]) * 32
                    child.release()
                    try:
                        view._mapping[17] = 0
                    except (TypeError, OSError):
                        pass
                    else:
                        raise AssertionError('mapping was writable')
                    value = {'tuple': view._identity.identity_fields(), 'fields': captured,
                             'read': True, 'readonly': True, 'retain_release': True}
                elif op == 'old':
                    old = req['tuple']
                    value = {
                        'retain': denied(lambda: client._request('RetainExternalView', **old,
                                                               offset_bytes=0, nbytes=1)),
                        'release': denied(lambda: client._request('ReleaseExternalView', **old)),
                        'revoke': denied(lambda: client._request('RevokeExternalBacking',
                                     **sdk.ExternalBackingHandle.from_response(req['fields']).identity_fields())),
                    }
                    # Exercise BOTH actual SDK compensation branches with a
                    # delayed old response. This is negative transport injection,
                    # not an authority/FD ACK or normal-business simulation.
                    releases = []
                    original_request, original_exchange = client._request, client._exchange
                    original_socket, original_class = sdk.socket.socket, sdk.ExternalReadView

                    def record_request(operation, **fields):
                        if operation == 'ReleaseExternalView':
                            try:
                                return original_request(operation, **fields)
                            except sdk.ExternalDdrError as error:
                                releases.append({'identity': fields, 'error': str(error)})
                                raise
                        return original_request(operation, **fields)

                    class DelayedResponse:
                        def __enter__(self):
                            return self
                        def __exit__(self, *unused):
                            return False
                        def settimeout(self, unused):
                            pass
                        def connect(self, unused):
                            # Compensation connections are real, not intercepted.
                            sdk.socket.socket = original_socket
                        def sendall(self, unused):
                            pass
                        def recvmsg(self, *unused):
                            return client._encode(req['fields']), [], 0, None

                    client._request = record_request
                    try:
                        sdk.socket.socket = lambda *a, **k: DelayedResponse()
                        try:
                            client._exchange('AcquireExternalRead', expect_fd=True, provisional=True)
                        except sdk.ExternalDdrError as error:
                            value['exchange_injection'] = str(error)
                        else:
                            raise AssertionError('missing FD injection did not fail')
                        sdk.socket.socket = original_socket
                        readonly = os.open(f'/proc/self/fd/{fds[0]}', os.O_RDONLY | os.O_CLOEXEC)
                        try:
                            mapping = sdk.mmap.mmap(readonly, 8192, access=sdk.mmap.ACCESS_READ)
                        finally:
                            os.close(readonly)
                        def delayed_exchange(*a, **k):
                            client._exchange = original_exchange
                            return dict(req['fields']), mapping

                        client._exchange = delayed_exchange

                        def fail_constructor(*unused):
                            raise ValueError('injected old-response constructor failure')

                        sdk.ExternalReadView = fail_constructor
                        try:
                            client._view_request('AcquireExternalRead')
                        except ValueError as error:
                            value['constructor_injection'] = str(error)
                        else:
                            raise AssertionError('constructor injection did not fail')
                        assert mapping.closed
                        assert len(releases) == 2, releases
                        assert all(row['identity'] == old for row in releases)
                        value['real_compensation_rejections'] = releases
                    finally:
                        client._request, client._exchange = original_request, original_exchange
                        sdk.socket.socket, sdk.ExternalReadView = original_socket, original_class
                    assert views[0].read() == bytes([req['seed']]) * 128
                elif op == 'register':
                    if 'at' in req:
                        while time.monotonic() < req['at']:
                            time.sleep(0.001)
                    value = register(req['size'])
                elif op == 'revoke_last':
                    client.revoke(handles.pop())
                    os.close(fds.pop())
                    value = {'revoked': True}
                elif op == 'release_views':
                    for view in views:
                        view.release()
                    views.clear()
                    value = {'released': True}
                elif op == 'cleanup':
                    for view in views:
                        view.release()
                    views.clear()
                    for handle in reversed(handles):
                        client.revoke(handle)
                    handles.clear()
                    for fd in fds:
                        os.close(fd)
                    fds.clear()
                    emit({'ok': True, 'value': {'cleaned': True}})
                    return
                else:
                    raise ValueError(op)
                emit({'ok': True, 'value': value})
            except Exception:
                emit({'ok': False, 'error': traceback.format_exc()})
    finally:
        for view in views:
            view.release()
        for handle in reversed(handles):
            client.revoke(handle)
        for fd in fds:
            os.close(fd)


if __name__ == '__main__':
    main()
