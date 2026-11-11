#!/usr/bin/env python3
"""Bounded two-Guest QMP/serial control under uflow_env_runner.

The caller supplies already prepared per-node boot arguments from the shared
boot helper and an installed-SDK agent overlay. This layer transfers control
messages only. It never decodes Guest addresses or unlinks shared FM backing.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import stat
import struct
import subprocess
import time
import hashlib
import threading
import types
from contextlib import contextmanager

def _verified_peer_module(name, pin):
    path = Path(__file__).resolve().parent / name
    if path.resolve() != path or not path.is_file() or not 0 < path.stat().st_size <= 128 << 10:
        raise ValueError('ordinary same-release module required')
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != pin:
        raise ValueError('managed group dependency source drift')
    module = types.ModuleType('_managed_group_' + name[:-3]); module.__file__ = str(path)
    exec(compile(raw, str(path), 'exec'), module.__dict__)
    return module

codec = _verified_peer_module('uflow_managed_cache_codec_v2.py', '7cfbdc76f1ab8d696ae3349b6bf5510d5b8e6b4f047f50ae90e579b72c4e62db')
write_codec = _verified_peer_module('uflow_managed_write_codec_v1.py', 'e5627711dc76a5441a7c957733983bff10dfd7d38fe1cf787b3b535e0de05c08')

def validate_managed_selection(config):
    selected = config.get('managed_write_profile')
    if (type(selected) is not dict or set(selected) != {'manifest','manifest_sha256','validator_sha256'} or
        selected['validator_sha256'] != '77f382eb31ba8d0ae0013da1816877876d41dae6684696688e19e1c6cdec858f'):
        raise ValueError('explicit reviewed managed-cache profile required')
    module = _verified_peer_module('uflow_managed_qemu_profile_v2.py', '77f382eb31ba8d0ae0013da1816877876d41dae6684696688e19e1c6cdec858f')
    result = module.validate(selected['manifest'], selected['manifest_sha256'], config['qemu_binary'])
    if (result['managed_write'] != module.CONTROL or result['runtime_wrapper']['path'] != str(Path(__file__).resolve()) or
        result['runtime_wrapper']['sha256'] != hashlib.sha256(Path(__file__).read_bytes()).hexdigest() or
        result['runtime_R_codec']['sha256'] != '7cfbdc76f1ab8d696ae3349b6bf5510d5b8e6b4f047f50ae90e579b72c4e62db' or
        result['runtime_W_codec']['sha256'] != 'e5627711dc76a5441a7c957733983bff10dfd7d38fe1cf787b3b535e0de05c08'):
        raise ValueError('actual selected profile/wrapper/codec closure mismatch')
    return result


def notify_failure():
    descriptor = os.environ.get('UFLOW_ENV_CLOSE_FD')
    if descriptor is None:
        raise RuntimeError('independent environment runner is required')
    os.write(int(descriptor), b'!')


def save(path, value):
    with path.open('x') as stream:
        json.dump(value, stream, indent=2)


class Channel:
    def __init__(self, path, deadline):
        self.sock = socket.socket(socket.AF_UNIX)
        self.sock.settimeout(min(1, max(.01, deadline - time.monotonic())))
        while True:
            try:
                self.sock.connect(str(path))
                break
            except (FileNotFoundError, ConnectionRefusedError):
                if time.monotonic() >= deadline:
                    self.sock.close()
                    raise TimeoutError('Guest control endpoint deadline')
                time.sleep(.05)
        self.pending = b''

    def line(self, deadline):
        while b'\n' not in self.pending:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('Guest control reply deadline')
            self.sock.settimeout(remaining)
            data = self.sock.recv(4096)
            if not data:
                raise RuntimeError('Guest control EOF')
            self.pending += data
            if len(self.pending) > 65536:
                raise RuntimeError('Guest control reply exceeded64KiB')
        line, self.pending = self.pending.split(b'\n', 1)
        return line.decode(errors='replace').strip()

    def send(self, text):
        if len(text.encode()) > 4096 or '\n' in text:
            raise ValueError('bounded single control message required')
        self.sock.sendall(text.encode() + b'\n')

    def close(self):
        self.sock.close()

    def verify_peer(self, pid):
        actual_pid, uid, _ = struct.unpack('3i', self.sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
        if (actual_pid, uid) != (pid, os.geteuid()):
            raise RuntimeError('Guest control endpoint peer identity mismatch')


class QMP(Channel):
    def __init__(self, path, deadline):
        self._control_lock = threading.RLock()
        self._managed_pid = self._managed_birth = None
        self._outstanding = None
        self._managed_unknown = False
        super().__init__(path, deadline)
        if 'QMP' not in self._json(self.line(deadline)):
            raise RuntimeError('QMP greeting missing')
        self.sequence = 0
        self.command('qmp_capabilities')

    @staticmethod
    def _json(value):
        def unique(pairs):
            result = {}
            for key, item in pairs:
                if key in result: raise ValueError('duplicate QMP reply member')
                result[key] = item
            return result
        def reject(value): raise ValueError('nonfinite QMP reply')
        return json.loads(value, object_pairs_hook=unique, parse_constant=reject)

    @contextmanager
    def _locked(self, deadline):
        if not self._control_lock.acquire(timeout=max(0, deadline-time.monotonic())):
            raise TimeoutError('bounded QMP metadata serialization deadline')
        try: yield
        finally: self._control_lock.release()

    def _request(self, name, deadline, arguments=None):
        self.sequence += 1
        request = {'execute': name, 'id': self.sequence}
        if arguments is not None: request['arguments'] = arguments
        self.send(json.dumps(request, separators=(',', ':')))
        while True:
            response = self._json(self.line(deadline))
            if type(response.get('id')) is int and response['id'] == self.sequence:
                if 'error' in response:
                    raise RuntimeError('QMP operation rejected: ' + str(response['error'])[:512])
                if 'return' not in response: raise ValueError('QMP reply lacks result')
                return response['return']

    def command(self, name):
        if name not in ('qmp_capabilities','query-status','quit','system_powerdown'):
            raise ValueError('QMP command outside lifecycle control scope')
        deadline = time.monotonic() + 5
        with self._locked(deadline):
            if name in ('quit','system_powerdown'):
                self.assert_no_managed_grants()
            return self._request(name, deadline)

    def bind_managed_peer(self, pid, birth):
        if type(pid) is not int or pid <= 0 or type(birth) is not int or birth <= 0:
            raise ValueError('exact task QEMU peer PID/birth required')
        self._managed_pid, self._managed_birth = pid, birth
        self._check_managed_peer()

    def _check_managed_peer(self):
        if self._managed_pid is None: raise ValueError('unbound managed QMP peer')
        self.verify_peer(self._managed_pid)
        path = Path('/proc') / str(self._managed_pid)
        with (path/'stat').open('rb') as stream: raw = stream.read(8193)
        if len(raw)>8192 or int(raw.rpartition(b')')[2].split()[19]) != self._managed_birth:
            raise RuntimeError('managed QEMU peer birth changed')
        if any(os.readlink(path/'ns'/key) != os.readlink(Path('/proc/self/ns')/key) for key in ('pid','mnt','user')):
            raise RuntimeError('managed QEMU peer namespace differs')

    def close(self):
        self.assert_no_managed_grants()
        super().close()

    def abort_transport(self):
        # Transport disposal after failure is not a grant/drain acknowledgment.
        super().close()

    def assert_no_managed_grants(self):
        if self._managed_unknown or self._outstanding is not None:
            raise RuntimeError('normal close forbidden with live or uncertain managed grant')

    def managed_cache(self, wire):
        return self._managed_control(wire, codec, 'R')

    def managed_write(self, wire):
        return self._managed_control(wire, write_codec, 'W')

    def _managed_control(self, wire, codec, mode):
        command = codec.decode(wire)
        deadline = time.monotonic() + 5
        with self._locked(deadline):
            try:
                self._check_managed_peer()
                if self._managed_unknown: raise RuntimeError('uncertain managed control cannot resume normal IO')
                if command['operation']=='begin' and self._outstanding is not None:
                    raise RuntimeError('one managed execution per owning QEMU channel')
                result = self._request('qom-set', deadline, {'path':codec.OBJECT_PATH,'property':codec.PROPERTY,'value':wire})
                if result != {}: raise ValueError('unexpected managed qom-set acknowledgment')
                value = self._request('qom-get', deadline, {'path':codec.OBJECT_PATH,'property':codec.PROPERTY})
                value = codec.validate_reply(wire, value, self._managed_pid)
                self._check_managed_peer()
                identity=(mode,command['control_generation'],command['authority_sha256'],command['correlation_sha256'])
                if value['code']=='ok' and command['operation']=='begin': self._outstanding=identity
                if value['code']=='ok' and command['operation']=='end':
                    if self._outstanding not in (None, identity): raise ValueError('end belongs to another operation')
                    self._outstanding=None
                if mode=='W' and (value['uncertain'] or value['code']=='unknown'):
                    self._managed_unknown=True
                    notify_failure()
                return value
            except BaseException:
                self._managed_unknown=True
                notify_failure()
                raise


class ManagedSerialChannel(Channel):
    def __init__(self, path, deadline, qmp, pid, birth):
        super().__init__(path, deadline)
        self._managed_qmp=qmp
        qmp.bind_managed_peer(pid, birth)

    def managed_cache(self, wire):
        return self._managed_qmp.managed_cache(wire)

    def managed_write(self, wire):
        return self._managed_qmp.managed_write(wire)


def validate(config):
    validate_managed_selection(config)
    root = Path(config['root']).resolve()
    output = Path(config['output']).resolve()
    if not output.is_relative_to(root / 'runs') or not output.is_dir():
        raise ValueError('existing task-owned run directory required')
    if len(config['nodes']) != 2 or {n['id'] for n in config['nodes']} != {'nodeA', 'nodeB'}:
        raise ValueError('exactly two distinct logical nodes required')
    for node in config['nodes']:
        if type(node['memory_mib']) is not int or not 256 <= node['memory_mib'] <= 8192:
            raise ValueError('per-Guest memory budget exceeded')
        if type(node['vcpus']) is not int or not 1 <= node['vcpus'] <= 4:
            raise ValueError('per-Guest vCPU budget exceeded')
        argv = node['boot_argv']
        if type(argv) is not list or not argv or any(type(x) is not str for x in argv):
            raise ValueError('explicit shared-helper boot argv required')
        if any(x not in ('-initrd', '-append', '-drive') for x in argv[::2]) or len(argv) % 2:
            raise ValueError('only shared boot engine arguments may extend QEMU')
        if len(os.fsencode(output / (node['id'] + '.serial'))) >= 108:
            raise ValueError('task socket path exceeds AF_UNIX limit')
        for suffix in ('.qmp', '.serial', '.serial.log', '.qemu.log'):
            if (output / (node['id'] + suffix)).exists():
                raise ValueError('Guest output already exists; reconcile before any relaunch')
    if sum(n['memory_mib'] for n in config['nodes']) > 16384:
        raise ValueError('aggregate Guest memory budget exceeded')
    if not 0 < config.get('boot_seconds', 180) <= 180:
        raise ValueError('boot deadline must not exceed180 seconds')
    if 'UFLOW_ENV_CLOSE_FD' not in os.environ:
        raise ValueError('run through the independent environment supervisor')
    return root, output


def run(config, control=None):
    root, output = validate(config)
    fm = output / 'fm'
    fm.mkdir()
    guests, channels, records = [], [], []
    deadline = time.monotonic() + config.get('boot_seconds', 180)
    try:
        for node in config['nodes']:
            name = node['id']
            qmp = output / (name + '.qmp')
            serial = output / (name + '.serial')
            env = {**os.environ, 'UB_FM_NODE_ID': name, 'UB_FM_TOPOLOGY_FILE': config['topology'],
                   'UB_FM_SHARED_DIR': str(fm), 'UB_SIM_ENTITY_COUNT': '2',
                   'UB_FM_ENTITY_PLAN_FILE': config['entity_plan'], 'UB_SIM_EXPERIMENTAL_FEATURES': 'gsva',
                   'GSVA_COH_UB_LINK_TX': '0'}
            argv = [config['qemu_binary'], '-M', 'virt,gic-version=3,its=on,ummu=on,ub-cluster-mode=on',
                    '-cpu', 'cortex-a57', '-m', str(node['memory_mib']) + 'M', '-smp', str(node['vcpus']),
                    '-nodefaults', '-nographic', '-global', 'ubc.uflow-managed-cache-v1=on',
                    '-global', 'ubc.uflow-managed-write-v1=on', '-kernel', config['kernel'],
                    '-qmp', 'unix:' + str(qmp) + ',server=on,wait=off',
                    '-chardev', 'socket,id=ser0,path=' + str(serial) + ',server=on,wait=off,logfile=' +
                    str(output / (name + '.serial.log')), '-serial', 'chardev:ser0', *node['boot_argv']]
            log = (output / (name + '.qemu.log')).open('xb')
            child = subprocess.Popen(argv, env=env, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)
            log.close()
            birth = int(Path('/proc', str(child.pid), 'stat').read_text().rpartition(')')[2].split()[19])
            guests.append(child)
            records.append({'node': name, 'pid': child.pid, 'birth': birth, 'argv': argv})
        save(output / 'guest-starts.json', records)
        identities = []
        for node, child in zip(config['nodes'], guests):
            qmp = QMP(output / (node['id'] + '.qmp'), deadline)
            serial = ManagedSerialChannel(output / (node['id'] + '.serial'), deadline, qmp, child.pid,
                next(record['birth'] for record in records if record['pid']==child.pid))
            channels.extend([qmp, serial])
            qmp.verify_peer(child.pid)
            serial.verify_peer(child.pid)
            serial.send('identity')
            while True:
                line = serial.line(deadline)
                if line.startswith('UFLOW_ENV_IDENTITY '):
                    identity = json.loads(line.partition(' ')[2])
                    if identity['node'] != node['id']:
                        raise RuntimeError('Guest logical identity mismatch')
                    identities.append(identity)
                    break
                if child.poll() is not None:
                    raise RuntimeError('Guest exited before identity')
            save(output / (node['id'] + '-qmp.json'), qmp.command('query-status'))
            serial.send('sdk-check')
            step_deadline = time.monotonic() + 30
            while True:
                line = serial.line(step_deadline)
                if line == 'UFLOW_ENV_SDK_OK':
                    break
                if line.startswith('UFLOW_ENV_FAIL'):
                    raise RuntimeError(line)
        if identities[0]['boot_id'] == identities[1]['boot_id']:
            raise RuntimeError('logical nodes unexpectedly share Guest boot identity')
        save(output / 'guest-identities.json', identities)
        links, link_deadline = {}, time.monotonic() + 30
        while len(links) != 2:
            for node in ('nodeA', 'nodeB'):
                status_path = fm / (node + '_ubcdev0__1.status')
                if status_path.is_file():
                    if status_path.stat().st_size > 65536:
                        raise RuntimeError('FM status exceeded bounded control metadata')
                    text = status_path.read_text()
                    if 'state=READY' in text.splitlines():
                        links[node] = text
            if len(links) == 2:
                break
            if time.monotonic() >= link_deadline:
                raise TimeoutError('both logical FM links did not become READY')
            time.sleep(.05)
        save(output / 'fm-links-ready.json', links)
        endpoints = []
        for node in config['nodes']:
            for suffix in ('.qmp', '.serial'):
                path = output / (node['id'] + suffix)
                info = path.lstat()
                endpoints.append({'path': str(path), 'dev': info.st_dev, 'inode': info.st_ino, 'uid': info.st_uid})
        save(output / 'control-endpoints-active.json', endpoints)
        carrier = []
        total = 0
        from uflow_guest_audit import file_identity_aliases
        for path in fm.rglob('*'):
            info = path.lstat()
            if stat.S_ISREG(info.st_mode) or stat.S_ISFIFO(info.st_mode) or stat.S_ISSOCK(info.st_mode):
                total += info.st_size
                row = {'path': str(path), 'dev': info.st_dev, 'inode': info.st_ino,
                                'uid': info.st_uid, 'file_type': stat.S_IFMT(info.st_mode),
                                'bytes': info.st_size, 'actual_blocks_bytes': info.st_blocks * 512}
                row['kernel_inode_aliases'] = file_identity_aliases(path, row)
                carrier.append(row)
        if total > 4 << 30:
            raise RuntimeError('aggregate FM carrier exceeds4GiB')
        save(output / 'fm-carrier-active.json', {'files': carrier, 'total_bytes': total,
             'not_private_SHM': True, 'business_pool_allocated_by_E0': False})
        if control is not None:
            control({node['id']: channels[index * 2 + 1] for index, node in enumerate(config['nodes'])}, identities)
        if config.get('controlled_hang_after_ready', False):
            notify_failure()
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            while True:
                signal.pause()
        for qmp in channels[::2]:
            qmp.assert_no_managed_grants()
        for channel in channels[1::2]:
            channel.send('quit')
        for channel in channels[1::2]:
            close_deadline = time.monotonic() + 10
            while channel.line(close_deadline) != 'UFLOW_ENV_DAEMON_CLOSED':
                pass
        save(output / 'guest-services-drained.json', {'both_daemon_wait_markers_received': True,
             'logical_allocation_ACK_inferred': False, 'Guest_kernel_poweroff_used': False})
        qmp_closes = []
        for index, child in enumerate(guests):
            try:
                qmp_closes.append({'reply_received': True, 'reply': channels[index * 2].command('quit')})
            except RuntimeError as error:
                if str(error) != 'Guest control EOF' or child.wait(timeout=5) != 0:
                    raise
                qmp_closes.append({'reply_received': False, 'EOF_after_quit_send': True, 'actual_child_wait': 0})
        statuses = [child.wait(timeout=10) for child in guests]
        if statuses != [0, 0]:
            raise RuntimeError('Guest normal exit did not return zero')
        for node in config['nodes']:
            log_path = output / (node['id'] + '.serial.log')
            if log_path.stat().st_size > 64 << 20:
                raise RuntimeError('single Guest serial log exceeded64MiB')
            text = log_path.read_text(errors='replace')
            if 'Kernel panic - not syncing' in text or 'Internal error: Oops:' in text:
                raise RuntimeError('Guest kernel fault observed in supported close path')
        save(output / 'guest-normal-exit.json', {'actual_wait_codes': statuses, 'identities': identities,
             'guest_daemon_close_markers_received': True, 'logical_allocation_ACK_inferred': False,
             'termination_mode': 'service-drain-then-QMP-quit', 'QMP_close_observations': qmp_closes,
             'Guest_kernel_poweroff_used': False, 'FM_cleanup_pending_exact_holder_audit': True})
    except BaseException as error:
        notify_failure()
        try:
            save(output / 'guest-first-failure.json', {'type': type(error).__name__, 'message': str(error)[:2048],
                 'independent_monitor_notified_before_this_diagnostic': True})
        except OSError:
            pass
        raise
    finally:
        for channel in channels:
            if isinstance(channel, QMP):
                channel.abort_transport()
            else:
                channel.close()
        # External monitor handles escalation. This layer never waits without a
        # deadline or deletes FM to hide an unobserved Guest allocation state.


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('config', type=Path)
    run(json.loads(parser.parse_args().config.read_bytes()))


if __name__ == '__main__':
    main()
