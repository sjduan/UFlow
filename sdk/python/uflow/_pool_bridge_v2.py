"""Pool extension to the C02 envelope; metadata only, never tensor bytes."""
from dataclasses import dataclass
import json
import struct

MAGIC = b'UFSIMB01'
HEADER = struct.Struct('!8sHHHHQQQIiiQ4H')
MAX_BODY = 65536
POOL, COMPLETION = 14, 10


def checksum(data):
    value = 0xcbf29ce484222325
    for byte in data:
        value = ((value ^ byte) * 0x100000001b3) & ((1 << 64) - 1)
    return value


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result or len(result) >= 64:
            raise ValueError('duplicate or excessive control keys')
        result[key] = value
    return result


def metadata(data):
    """Flat metadata, bounded tokens; reject byte/payload/VA escape hatches."""
    if len(data) > MAX_BODY:
        raise ValueError('control body limit')
    value = json.loads(data, object_pairs_hook=_unique)
    if type(value) is not dict:
        raise ValueError('control object required')
    for key, item in value.items():
        if key in ('payload', 'data', 'bytes', 'pointer', 'address', 'base', 'descriptor_hex'):
            raise ValueError('payload or raw address is not control metadata')
        if type(item) is int and not 0 <= item < 1 << 64:
            raise ValueError('control integer outside u64')
        if type(item) not in (int, bool, str):
            raise ValueError('flat scalar control fields required')
        if type(item) is str and (len(item.encode()) > 256 or any(ord(c) < 32 for c in item)):
            raise ValueError('bounded control token required')
    return value


@dataclass(frozen=True)
class Frame:
    request: int
    correlation: int
    realm: str
    node: str
    boot: str
    session: str
    generation: int
    body: bytes
    kind: int = POOL
    status: int = 0
    error: int = 0

    def encode(self):
        for number in (self.request, self.correlation, self.generation):
            if type(number) is not int or not 0 < number < 1 << 64:
                raise ValueError('positive envelope identity required')
        strings = [item.encode() for item in (self.realm, self.node, self.boot, self.session)]
        if any(not item or len(item) > 65535 or b'\0' in item for item in strings):
            raise ValueError('invalid envelope identity')
        if self.kind not in (POOL, COMPLETION) or self.status not in (0, 1) or self.error < 0:
            raise ValueError('pool envelope kind/status')
        metadata(self.body)
        header = HEADER.pack(MAGIC, 1, 1, self.kind, 0, self.request, self.correlation,
                             self.generation, len(self.body), self.status, self.error,
                             checksum(self.body), *map(len, strings))
        return header + b''.join(strings) + self.body

    @classmethod
    def decode(cls, data):
        if len(data) < HEADER.size:
            raise ValueError('truncated bridge header')
        magic, major, minor, kind, flags, request, correlation, generation, length, status, error, digest, *sizes = HEADER.unpack_from(data)
        if (magic, major, minor, flags) != (MAGIC, 1, 1, 0) or length > MAX_BODY:
            raise ValueError('incompatible bridge envelope')
        if len(data) != HEADER.size + sum(sizes) + length:
            raise ValueError('bridge length mismatch')
        position, strings = HEADER.size, []
        for size in sizes:
            strings.append(data[position:position+size].decode())
            position += size
        body = data[position:]
        if checksum(body) != digest:
            raise ValueError('bridge body checksum mismatch')
        result = cls(request, correlation, *strings, generation, body, kind, status, error)
        result.encode()  # The exact same bounds apply in both directions.
        return result

    def completion(self, body):
        return Frame(self.request, self.correlation, self.realm, self.node, self.boot,
                     self.session, self.generation, json.dumps(body, separators=(',', ':')).encode(), COMPLETION)

    def matches(self, reply):
        return (reply.request, reply.correlation, reply.realm, reply.node, reply.boot,
                reply.session, reply.generation, reply.kind) == (
                    self.request, self.correlation, self.realm, self.node, self.boot,
                    self.session, self.generation, COMPLETION)
