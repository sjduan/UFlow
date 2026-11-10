#!/usr/bin/env python3
"""C02 pool-v2 serial bridge and lifecycle-only lower wire relay.

Host parses metadata but never a Guest pointer. Public mem_service control wire
is relayed only for managed lifecycle/directory ops, never GET/PUT/materialize.
There is one host lower authority, not a second simulation Catalog.
"""
import json
import hashlib
import os
from pathlib import Path
import re
import socket
import struct
import threading
import time
from uflow._pool_bridge_v2 import Frame, POOL, metadata

LOWER = struct.Struct('<IHHQIIIIIIQ')  # Frozen public native little-endian aarch64 ABI.


class NoticeOnce:
    """One nonblocking supervisor notice per C process or injected node scope."""
    def __init__(self,callback):
        self.callback,self.sent,self.lock=callback,False,threading.Lock()

    def __call__(self):
        with self.lock:
            if self.sent:return False
            self.sent=True
            self.callback()
            return True


def _supervisor_notice():
    descriptor = os.environ.get('UFLOW_ENV_CLOSE_FD')
    if descriptor is None:
        raise RuntimeError('accepted independent environment monitor required')
    try:
        os.write(int(descriptor), b'!')
    except BlockingIOError:
        pass  # Already pending notice; never wait in an exceptional boundary.


_process_notice=NoticeOnce(_supervisor_notice)


def notify_failure():
    return _process_notice()


class GuestFatalPanic(RuntimeError):
    def __init__(self,node,line):
        self.serial_line=line[:1024]
        self.serial_line_sha256=hashlib.sha256(line.encode('utf-8')).hexdigest()
        super().__init__('Guest '+node+' fatal kernel panic; pending operations remain Unknown: '+self.serial_line)


def fatal_kernel_panic(line):
    # Match a complete kernel console record, not quoted payload, a warning,
    # RCU stall or worker text containing the words "kernel panic".
    return re.fullmatch(r'(?:\[\s*[0-9]+(?:\.[0-9]+)?\]\s*)?Kernel panic - not syncing:\s+\S.*',line) is not None


def exact(connection, size, deadline=None, capture=None):
    output = bytearray()
    while len(output) < size:
        if deadline is not None:
            remaining = deadline-time.monotonic()
            if remaining <= 0:raise TimeoutError('whole lower frame deadline')
            connection.settimeout(remaining)
        data = connection.recv(size-len(output))
        if not data:
            raise RuntimeError('unconfirmed control EOF')
        output.extend(data)
        if capture is not None:capture.extend(data)
    return bytes(output)


def lower_metadata(data):
    """The frozen public key=value grammar, including repeated provider rows."""
    value, providers = {}, []
    for line in data.decode('utf-8').splitlines():
        if not line or '=' not in line or any(ord(c)<32 for c in line):
            raise ValueError('malformed public lower metadata')
        key, item = line.split('=', 1)
        if not key or len(key)>64 or len(item)>4096:raise ValueError('lower metadata bound')
        if key == 'provider':
            fields=item.split(' ')
            if len(fields)!=5:raise ValueError('public provider row shape')
            row={'node':fields[0]}
            for field in fields[1:]:
                name, raw=field.split('=',1)
                if name in row or not raw.isdecimal():raise ValueError('provider row identity')
                row[name]=int(raw)
            if set(row)!={'node','incarnation','readiness_generation','capabilities','age_ms'}:
                raise ValueError('provider row fields')
            providers.append(row)
        else:
            if key in value:raise ValueError('duplicate lower metadata field')
            value[key]=item
    if providers:value['providers']=providers
    return value


def lower_response(data, request, op):
    if len(data)<48:raise ValueError('truncated lower response')
    fields=LOWER.unpack_from(data)
    if fields[:3]!=(0x4d535643,1,48) or fields[3]!=request or fields[4]!=op or fields[5] or fields[6]>4096 or len(data)!=48+fields[6]:
        raise ValueError('lower response identity/length')
    if fields[8]>10 or fields[9]>10:raise ValueError('lower response status bound')
    value=2166136261 if fields[6] else 0
    for byte in data[48:]:value=((value^byte)*16777619)&0xffffffff
    if fields[7]!=value:raise ValueError('lower response checksum')
    metadata=lower_metadata(data[48:]) if fields[6] else {}
    if (fields[8]==0)!=(metadata.get('status')=='ok'):
        raise ValueError('lower header/body status mismatch')
    return fields,metadata


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


def worker_idle_reply(identity,worker,origin,op,fields,public,reply):
    # Frozen worker deliberately polls an empty work queue and continues on
    # NOT_FOUND. This read-only result is neither allocation completion nor a
    # no-side-effect/cleanup witness. All other errors remain Unknown.
    return (worker is not None and origin['role']=='worker' and
        (origin['pid'],origin['birth'])==(worker['pid'],worker['birth']) and op==0x7c and
        fields[8:10]==(1,1) and public=={'node_id':identity['node'],
            'incarnation':str(identity['incarnation']),'after_generation':'0'} and
        reply=={'status':'not_found','reason':'not_found','key':''})


class GuestNode:
    def __init__(self, channel, identity, lower_socket, event, realm, *, notify=notify_failure, lower_timeout=10, managed_control=None, publication_book=None, managed_write_control=None):
        self.channel, self.identity, self.lower_socket, self.event = channel, identity, lower_socket, event
        self.realm = realm
        self.managed_control,self.publication_book=managed_control,publication_book
        self.managed_write_control=managed_write_control
        self.notify,self.lower_timeout=NoticeOnce(notify),lower_timeout
        self.root_pid,self.root_birth=0,0
        self.worker=None
        self.directory=None
        self.refreshes={}
        self.log_bytes=0
        self.pending, self.lock, self.command_lock = {}, threading.Lock(), threading.Lock()
        self.started, self.drained, self.stop = threading.Event(), threading.Event(), threading.Event()
        self.failure = None
        self.first_failure = None
        self.failure_lock = threading.RLock()
        self.thread = threading.Thread(target=self.pump, name='pool-'+identity['node'])

    def fail(self,error,cause='guest-control-failure'):
        """Keep the first cause and wake every waiter before diagnostic IO."""
        with self.failure_lock:
            if self.failure is not None:return False
            observed=time.monotonic_ns()
            try:
                self.notify()  # First exceptional side effect; no logging first.
            finally:
                self.failure=error
                self.first_failure={'event':'guest-bridge-failure','node':self.identity['node'],
                    'boot_id':self.identity['boot_id'],'agent_session':self.identity['agent_session'],
                    'incarnation':self.identity['incarnation'],'cause':cause,'detail':str(error)[:2048],
                    'host_monotonic_ns':observed,'pending_outcome':'Unknown','responsibility_retained':True,
                    'SDK_ACK_or_side_effect_absence_inferred':False}
                if isinstance(error,GuestFatalPanic):
                    self.first_failure.update(serial_line=error.serial_line,serial_line_sha256=error.serial_line_sha256)
                for item in list(self.pending.values()):item['ready'].set()
        self.event(dict(self.first_failure))
        return True

    def send(self, line):
        with self.lock:
            self.channel.send(line)

    def relay(self, identifier, data, origin):
        request, op = lower_request(data)
        public=lower_metadata(data[48:]) if len(data)>48 else {}
        if origin['role']=='root':
            if origin['pid']!=self.root_pid or not origin['control_request'] or (origin['control_request'],origin['correlation']) not in self.pending:
                raise ValueError('root SDK process/control identity mismatch')
            if self.root_birth and self.root_birth!=origin['birth']:raise ValueError('root process birth changed')
            self.root_birth=origin['birth']
        elif origin['role']=='worker':
            if not self.worker or (origin['pid'],origin['birth'])!=(self.worker['pid'],self.worker['birth']):
                raise ValueError('worker process birth mismatch')
            if op in (0x77,0x79,0x7c) and (public.get('node_id'),public.get('incarnation'))!=(self.identity['node'],str(self.identity['incarnation'])):
                raise ValueError('worker lower identity mismatch')
            if op==0x77 and public.get('readiness_generation')!='1':raise ValueError('wrong worker readiness generation')
        elif origin['role']!='client':raise ValueError('unowned lower origin')
        response=b'';captured=bytearray()
        try:
            deadline=time.monotonic()+self.lower_timeout
            with socket.socket(socket.AF_UNIX) as lower:
                lower.settimeout(self.lower_timeout)
                lower.connect(self.lower_socket)
                lower.settimeout(max(.001,deadline-time.monotonic()))
                lower.sendall(data)
                header=exact(lower,48,deadline,captured)
                fields=LOWER.unpack(header)
                if fields[6]>4096:raise ValueError('lower response length bound')
                response=header+exact(lower,fields[6],deadline,captured)
            fields,reply=lower_response(response,request,op)
            idle=worker_idle_reply(self.identity,self.worker,origin,op,fields,public,reply)
            if (fields[8] or fields[9]) and not idle:self.notify()  # No generic INTERNAL/NOT_FOUND waiver.
        except BaseException as error:
            self.notify()
            self.event({'event':'lower-control-unconfirmed','node':self.identity['node'],'origin':origin,
                'boot_id':self.identity['boot_id'],'incarnation':self.identity['incarnation'],
                'operation':op,'request':request,'relay_id':identifier,'request_bytes':len(data),
                'request_checksum':LOWER.unpack_from(data)[7],'request_metadata':public,
                'request_hex':data.hex(),'partial_response_hex':bytes(captured).hex(),'partial_response_bytes':len(captured),
                'host_monotonic_ns':time.monotonic_ns(),'detail':str(error),
                'outcome':'Unknown','side_effect_absence_inferred':False})
            raise
        evidence={'event':'lower-control','node':self.identity['node'],'boot_id':self.identity['boot_id'],
            'incarnation':self.identity['incarnation'],'origin':origin,'relay_id':identifier,'operation':op,
            'request':request,'request_bytes':len(data),'response_bytes':len(response),
            'request_checksum':LOWER.unpack_from(data)[7],'response_checksum':fields[7],
            'request_hex':data.hex(),'response_hex':response.hex(),'request_metadata':public,'response_metadata':reply,
            'status':fields[8],'error_code':fields[9],'reason':reply.get('reason',''),
            'outcome':'idle' if idle else ('completed' if not fields[8] and not fields[9] else 'Unknown'),
            'expected_worker_idle_poll':idle,
            'side_effect_absence_inferred':False,'payload_plane':False}
        evidence['host_monotonic_ns']=time.monotonic_ns()
        self.event(evidence)  # Complete legal metadata is retained before forwarding to a failing child.
        if not fields[8] and not fields[9]:
            if origin['role']=='worker' and op==0x77:
                self.refreshes[request]=evidence
                while len(self.refreshes)>64:self.refreshes.pop(next(iter(self.refreshes)))
            if origin['role']=='root' and op==0x78:self.directory=evidence
        if self.publication_book is not None and not fields[8] and not fields[9]:
            self.publication_book.observe_worker_publication(self.identity,origin,op,public,reply)
        for offset in range(0, len(response), 1024):
            self.send('wire %s %s' % (identifier, response[offset:offset+1024].hex()))

    def pump(self):
        try:
            while not self.stop.is_set():
                try:
                    line = self.channel.line(time.monotonic()+.25)
                except (socket.timeout, TimeoutError):
                    continue
                if fatal_kernel_panic(line):
                    self.fail(GuestFatalPanic(self.identity['node'],line),'guest-kernel-panic')
                    return
                if line.startswith('UFPOOL_RPC '):
                    _, identifier,role,pid,birth,control,correlation,encoded=line.split(' ',7)
                    self.relay(int(identifier),bytes.fromhex(encoded),{'role':role,'pid':int(pid),'birth':int(birth),
                        'control_request':int(control),'correlation':int(correlation)})
                elif line.startswith('UFPOOL_WORKER_START '):
                    _,pid,birth=line.split(' ')
                    if self.worker:raise ValueError('worker replacement forbidden')
                    self.worker={'pid':int(pid),'birth':int(birth)}
                    self.event({'event':'worker-start','node':self.identity['node'],**self.worker})
                elif line.startswith('UFPOOL_WORKER_REFRESH '):
                    _,pid,birth,count,lease,request=line.split(' ')
                    if int(request) not in self.refreshes or (int(pid),int(birth))!=(self.worker['pid'],self.worker['birth']):
                        raise ValueError('worker refresh receipt lacks actual lower completion')
                    self.event({'event':'worker-refresh','node':self.identity['node'],'pid':int(pid),'birth':int(birth),
                        'count':int(count),'lease_ms':int(lease),'request':int(request)})
                elif line.startswith('UFPOOL_WORKER_LOG '):
                    _,pid,birth,stream,encoded=line.split(' ',4)
                    if not self.worker or (int(pid),int(birth))!=(self.worker['pid'],self.worker['birth']) or stream not in ('stdout','stderr'):
                        raise ValueError('worker log ownership')
                    data=bytes.fromhex(encoded)
                    self.log_bytes+=len(data)
                    if len(data)>512 or self.log_bytes>256*1024:raise ValueError('worker log bound')
                    self.event({'event':'worker-log','node':self.identity['node'],'pid':int(pid),'birth':int(birth),
                        'stream':stream,'bytes':len(data),'hex':encoded})
                elif line.startswith('UFPOOL_WORKER_EXIT '):
                    self.fail(RuntimeError('actual worker exited; original context retained'),'worker-exit')
                    self.event({'event':'worker-exit','node':self.identity['node'],'raw':line})
                    return
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
                    self.notify()  # Immediately, before reading/serializing diagnostics.
                elif line.startswith(('UFPOOL_FATAL', 'UFLOW_ENV_FAIL')):
                    raise RuntimeError('Guest reported failure before diagnostics: '+line)
                # Kernel/boot lines remain in serial raw logs; no parsing as ACK.
        except BaseException as error:
            self.fail(error)

    def exchange(self, frame, timeout=120, *, auth=None):
        body=metadata(frame.body)
        if any(k.startswith('write_') for k in body):
            if self.managed_write_control is None:
                raise ValueError('production-not-ready: no accepted managed W control/profile')
            return self.managed_write_control.execute(self,frame,auth,timeout)
        if any(k.startswith('cache_') for k in body):
            if self.managed_control is None:
                raise ValueError('production-not-ready: no accepted managed native control/profile')
            return self.managed_control.execute(self,frame,auth,timeout)
        with self.command_lock:
            return self._exchange_locked(frame,timeout)

    def _exchange_locked(self,frame,timeout):
        # Caller owns command_lock for the entire begin/read/end transaction.
        if self.failure:
            self.notify()
            raise RuntimeError('same Guest context already failed; operation remains unknown') from self.failure
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
        try:
            with self.failure_lock:
                if self.failure:raise RuntimeError('same Guest context already failed; operation remains unknown') from self.failure
                self.pending[key] = item
            # Registration is the admission point. Never hold failure_lock
            # across serial IO: a blocked send must not delay receiver-side
            # panic notice or waking this already in-flight Unknown request.
            self.send('v2 %d %d %s' % (*key, frame.body.hex()))
            if not item['ready'].wait(timeout) or self.failure:
                raise RuntimeError('same Guest operation outcome unknown') from self.failure
            receipt = item['response']
            if receipt.get('phase') != 'completed':
                self.notify()
            if body['op']=='bootstrap_prepare' and receipt.get('canary_prepared') is True:
                self.root_pid=receipt['guest_pid']
            self.event({'event': 'guest-completion', 'node': frame.node, 'request': frame.request,
                        'correlation': frame.correlation, 'op': body['op'], 'receipt': receipt,
                        'host_monotonic_ns': time.monotonic_ns(), 'clock_domain': 'B-host'})
            return frame.completion(receipt)
        except BaseException:
            self.notify()  # Before removing pending ownership or any diagnostics.
            raise
        finally:
            with self.failure_lock:self.pending.pop(key, None)

    def prepare_close(self):
        self.send('prepare-quit')
        if not self.drained.wait(10) or self.failure:
            raise RuntimeError('actual Guest worker/client drain not confirmed')
        self.stop.set()
        self.thread.join(1)
        if self.thread.is_alive():
            raise RuntimeError('bridge thread did not stop before handing Channel to QMP helper')


class BridgeServer:
    def __init__(self, path, node, daemon_auth):
        self.daemon_auth=daemon_auth
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
                    from daemon_peer_auth import PeerRejected
                    try:authenticated=self.daemon_auth.verify(connection)
                    except PeerRejected:continue # Untrusted peers cannot poison the actual Guest.
                    connection.settimeout(130)
                    count, = struct.unpack('!I', exact(connection, 4))
                    if not 68 <= count <= 65536+68+4*65535:
                        raise ValueError('bridge frame size limit')
                    request = Frame.decode(exact(connection, count))
                    response = self.node.exchange(request,auth=authenticated).encode()
                    connection.sendall(struct.pack('!I', len(response))+response)
        except BaseException as error:
            self.node.fail(error,'bridge-endpoint-failure')
            self.failure = self.node.failure

    def close(self):
        self.stop.set()
        self.thread.join(1)
        if self.thread.is_alive():
            raise RuntimeError('endpoint active at shutdown')
        self.listener.close()
        self.path.unlink()
