"""Independent bootstrap and guard expectations. No simulated data-plane ACKs."""
import json
from uflow._pool_bridge_v2 import Frame

CANARY_BYTES, CANARY_VISIBLE = 2 << 20, 4096


def canary_checksum(seed):
    value = 1469598103934665603
    for index in range(CANARY_VISIBLE):
        value = ((value ^ ((seed + index*29) & 255))*1099511628211) & ((1 << 64)-1)
    return value


def bootstrap_nodes(nodes, realm, event, *, control=None):
    if set(nodes) != {'nodeA', 'nodeB'}:
        raise ValueError('exactly two bound Guest nodes required')
    sequence = 0

    def call(node, body):
        nonlocal sequence
        if control is not None:return control.call(node.identity['node'],body)
        sequence += 1
        identity = node.identity
        frame = Frame(sequence, sequence, realm, identity['node'], identity['boot_id'],
                      identity['agent_session'], identity['incarnation'],
                      json.dumps({'schema': 2, **body}, separators=(',', ':')).encode())
        reply = node.exchange(frame)
        if not frame.matches(reply):
            raise RuntimeError('canary completion identity mismatch')
        receipt = json.loads(reply.body)
        if receipt.get('schema') != 2 or receipt.get('phase') != 'completed':
            raise RuntimeError('canary operation unconfirmed; original endpoint retained')
        return receipt

    prepared, ready = {}, {}
    # Both exports exist before the first verify; no first-client/peer deadlock.
    for name, seed in (('nodeA', 17), ('nodeB', 29)):
        event({'event': 'bootstrap-responsibility', 'node': name, 'bytes': CANARY_BYTES,
               'state': 'reserved-before-real-prepare', 'B_cap_bytes': 128 << 20})
        receipt = call(nodes[name], {'op': 'bootstrap_prepare', 'seed': seed})
        descriptor = receipt.get('canary_descriptor', '')
        if (receipt.get('canary_prepared') is not True or receipt.get('canary_bytes') != CANARY_BYTES or
                receipt.get('visible_bytes') != CANARY_VISIBLE or receipt.get('checksum') != canary_checksum(seed) or
                receipt.get('seed') != seed or type(receipt.get('guest_pid')) is not int or receipt['guest_pid'] <= 0 or
                not descriptor or len(descriptor) > 384 or len(descriptor) % 2 or
                any(c not in '0123456789abcdef' for c in descriptor)):
            raise RuntimeError('actual canary preparation receipt invalid')
        prepared[name] = receipt
        event({'event': 'bootstrap-prepared', 'node': name, 'receipt': receipt})
    for name in ('nodeA', 'nodeB'):
        peer = prepared['nodeB' if name == 'nodeA' else 'nodeA']
        receipt = call(nodes[name], {'op': 'bootstrap_verify',
            'peer_canary_descriptor': peer['canary_descriptor'], 'checksum': peer['checksum']})
        if (receipt.get('canary_ready') is not True or receipt.get('verified_checksum') != peer['checksum'] or
                receipt.get('guest_pid') != prepared[name]['guest_pid']):
            raise RuntimeError('actual same-context peer verification not confirmed')
        ready[name] = receipt
        event({'event': 'bootstrap-verified', 'node': name, 'receipt': receipt})
    return {name: {'canary': prepared[name], 'ready': ready[name]} for name in nodes}


def guard_intervals(capacity, written):
    """Complement of independent planned writes, never inferred from snapshot."""
    if capacity <= 0:
        raise ValueError('positive capacity required')
    end, guards = 0, []
    for begin, stop in sorted(written):
        if not 0 <= begin < stop <= capacity or begin < end:
            raise ValueError('planned write bounds/overlap')
        if end < begin:
            guards.append((end, begin))
        end = stop
    if end < capacity:
        guards.append((end, capacity))
    return guards


def guard_chunks(guards, segments, maximum):
    """Snapshot may split a read at content versions, never remove guard bytes."""
    if maximum <= 0 or any(s['offset'] < 0 or s['nbytes'] <= 0 for s in segments):
        raise ValueError('positive bounded guard chunks and segments required')
    ordered = sorted(segments, key=lambda s: s['offset'])
    chunks = []
    for begin, end in guards:
        cursor = begin
        for segment in ordered:
            start, stop = segment['offset'], segment['offset'] + segment['nbytes']
            if stop <= begin or start >= end:
                continue
            left, right = max(start, begin), min(stop, end)
            if left != cursor or segment['state'] != 'Committed' or segment['content_version'] <= 0:
                raise RuntimeError('snapshot gap/overlap/uncommitted guard')
            while left < right:
                count = min(maximum, right-left)
                chunks.append((left, count))
                left += count
            cursor = right
        if cursor != end:
            raise RuntimeError('snapshot omitted expected guard bytes')
    if sum(n for _, n in chunks) != sum(end-begin for begin, end in guards):
        raise RuntimeError('independent guard coverage mismatch')
    return chunks
