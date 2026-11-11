"""Private bounded JSON/FD transport shared by the managed SDK and provider."""
import array
import json
import os
from pathlib import Path
import socket
import struct

MAX_FRAME = 65536
U64_MAX = (1 << 64) - 1


def integer(value, *, positive=False):
    if type(value) is not int or not (int(positive) <= value <= U64_MAX):
        raise ValueError('positive u64 required' if positive else 'nonnegative u64 required')
    return value


def opaque(value):
    if type(value) is not str or not value or len(value.encode()) > 256 or any(ord(c) < 32 for c in value):
        raise ValueError('bounded nonempty identity required')
    return value


def fields(value, required, optional=()):
    if type(value) is not dict or not set(required) <= value.keys() or value.keys() - set(required) - set(optional):
        raise ValueError('unknown/missing managed control field')
    return value


def _object(pairs):
    if len(pairs) > 128 or len({key for key, _ in pairs}) != len(pairs):
        raise ValueError('duplicate JSON key or object limit')
    return dict(pairs)


def _no_float(value):
    raise ValueError('control JSON floats/constants are forbidden')


def _integer_literal(value):
    return integer(int(value))


def decode(raw):
    if not raw or len(raw) > MAX_FRAME:
        raise ValueError('managed control frame size')
    return json.loads(raw, object_pairs_hook=_object, parse_int=_integer_literal,
                      parse_float=_no_float, parse_constant=_no_float)


def send(sock, value, fd=None):
    raw = json.dumps(value, separators=(',', ':'), sort_keys=True, allow_nan=False).encode() + b'\n'
    if len(raw) > MAX_FRAME:
        raise ValueError('managed control frame size')
    if fd is None:
        sock.sendall(raw)
    else:
        rights = array.array('i', [fd])
        count = sock.sendmsg([raw], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, rights)])
        if count <= 0:
            raise OSError('no managed frame sent')
        sock.sendall(raw[count:])


def receive(sock):
    fds = []
    try:
        raw, ancillary, flags, _ = sock.recvmsg(MAX_FRAME + 1, socket.CMSG_SPACE(4 * array.array('i').itemsize),
                                               getattr(socket, 'MSG_CMSG_CLOEXEC', 0))
        for level, kind, data in ancillary:
            if level != socket.SOL_SOCKET or kind != socket.SCM_RIGHTS:
                raise ValueError('unexpected managed ancillary message')
            rights = array.array('i')
            rights.frombytes(data[:len(data) - len(data) % rights.itemsize])
            fds.extend(rights)
        if flags & (socket.MSG_CTRUNC | socket.MSG_TRUNC) or len(fds) > 1:
            raise ValueError('truncated/extra managed FD')
        while b'\n' not in raw and len(raw) <= MAX_FRAME:
            part = sock.recv(min(4096, MAX_FRAME + 1 - len(raw)))
            if not part:
                raise ValueError('incomplete managed response')
            raw += part
        if len(raw) > MAX_FRAME or not raw.endswith(b'\n') or raw.count(b'\n') != 1:
            raise ValueError('one bounded managed control frame required')
        value = decode(raw)
        for fd in fds:
            os.set_inheritable(fd, False)
        return value, fds
    except BaseException:
        for fd in fds:
            os.close(fd)
        raise


def peercred(sock):
    return struct.unpack('3i', sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize('3i')))


def process_birth(pid):
    integer(pid, positive=True)
    base = Path('/proc') / str(pid)
    raw = (base / 'stat').read_text()
    if int(raw.split('(', 1)[0]) != pid:
        raise ValueError('process identity changed')
    ticks = int(raw[raw.rindex(')') + 1:].split()[19])
    namespace = (base / 'ns/pid').stat()
    lines = (base / 'status').read_text().splitlines()
    uid = int(next(row for row in lines if row.startswith('Uid:')).split()[2])
    gid = int(next(row for row in lines if row.startswith('Gid:')).split()[2])
    return {'pid': pid, 'uid': uid, 'gid': gid, 'starttime_ticks': ticks,
            'pid_namespace_dev': namespace.st_dev, 'pid_namespace_ino': namespace.st_ino}
