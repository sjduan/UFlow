"""Bounded, metadata-only control for the one O1/M1 host session.

UFlow remains the resource/lease authority. This protocol does not transport
payloads, grant ranges, execute commands, or export the admin capability.
"""
import json
from pathlib import Path
import socket

GROUP = 'phasef-parallel-20261106'
POOL_BYTES = 128 << 20
GUARD_BYTES = 2 << 20
PAYLOAD_BYTES = 20 << 20
BLOCK_BYTES = GUARD_BYTES + PAYLOAD_BYTES
PARTITIONS = {'O1': [0], 'M1': [1, 2, 3, 4]}
LIMIT = 65536


def save(path, value, *, exclusive=True):
    path = Path(path)
    with path.open('x' if exclusive else 'w', encoding='utf-8', newline='\n') as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write('\n')


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate control field')
        result[key] = value
    return result


def receive(sock):
    data = bytearray()
    while not data.endswith(b'\n'):
        part = sock.recv(min(4096, LIMIT + 1 - len(data)))
        if not part:
            raise ValueError('incomplete control frame')
        data.extend(part)
        if len(data) > LIMIT or b'\n' in data[:-1]:
            raise ValueError('control frame too large or repeated')
    value = json.loads(data, object_pairs_hook=_unique)
    if type(value) is not dict:
        raise ValueError('control object required')
    return value


def request(manifest, op, **values):
    """No retry after a lost response: describe and inspect the original state."""
    if type(manifest) is not dict:
        manifest = json.loads(Path(manifest).read_text(encoding='utf-8'))
    body = {'schema': 1, 'group': GROUP, 'op': op, **values}
    data = json.dumps(body).encode() + b'\n'
    if len(data) > LIMIT:
        raise ValueError('oversized control request')
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(35)
        sock.connect(manifest['control_endpoint'])
        sock.sendall(data)
        result = receive(sock)
    if result.get('status') != 'ok':
        raise RuntimeError(result)
    return result['result']


def pool_record(snapshot, descriptor):
    """Only the original single Catalog and physical backing may be used."""
    if any(type(snapshot.get(k)) is not int for k in (
            'budget_bytes', 'charged_bytes', 'creation_reserved_bytes')):
        raise ValueError('invalid exact budget counters')
    if (snapshot['budget_bytes'] != POOL_BYTES or snapshot['charged_bytes'] != POOL_BYTES
            or snapshot['creation_reserved_bytes'] != 0 or len(snapshot['pools']) != 1):
        raise ValueError('single physical pool/cap invariant failed')
    pool = snapshot['pools'][0]
    if any(pool.get(k) != v for k, v in descriptor.items()):
        raise ValueError('physical pool descriptor changed')
    if (pool['state'] != 'Allocated' or pool['frozen']
            or any(c['state'] == 'Unknown' or any(x['unknown'] for x in c['children'])
                   for c in pool['clients'])
            or any(lease['state'] == 'Unknown' for lease in pool['ranges']['leases'])):
        raise ValueError('Unknown/frozen responsibility: retain the original domain')
    return pool


def check_retired(pool, sessions):
    """Actual UF unmap/drain evidence, never a PID/timeout-based retirement."""
    rows = {c['client_session']: c for c in pool['clients']}
    if any(s not in rows for s in sessions):
        raise ValueError('retirement names an unobserved client')
    for session in sessions:
        row = rows[session]
        if row['state'] != 'Unmapped' or row['children']:
            raise ValueError('client/child responsibility remains')
    if any(lease['client_session'] in sessions for lease in pool['ranges']['leases']):
        raise ValueError('line range responsibility remains')


def check_final(pool, lines, bootstrap):
    if any(not lines[line]['retired'] for line in PARTITIONS):
        raise ValueError('both lines must actually retire before final Destroy')
    sessions = {bootstrap}
    for line in PARTITIONS:
        sessions.update(lines[line]['clients'])
    if sessions != {c['client_session'] for c in pool['clients']}:
        raise ValueError('unaccounted client remains; no final Destroy')
    check_retired(pool, sessions)
    if pool['ranges']['leases']:
        raise ValueError('range responsibility remains; no final Destroy')
