#!/usr/bin/env python3
"""Data-only wire codec for the explicit Catalog-managed read-cache model.

This codec supplies no authority. C must authenticate its actual Catalog daemon
and mint/retain a real range-lease obligation before sending a begin command.
No payload or token_value is decoded, returned or carried by this control.
"""
import hashlib
import json
import re
import struct

FEATURE = 'uflow-managed-lease-cache-v1'
PROPERTY = 'uflow-managed-cache-control-v1'
OBJECT_PATH = '/machine'
WIRE = struct.Struct('!4sB3xQ32s32s32s9Q9I')
assert WIRE.size == 220
OPS = {'probe': 1, 'begin': 2, 'end': 3, 'status': 4}
FIELDS64 = ('map_id', 'lease_epoch', 'export_mem_id', 'remote_uba', 'size', 'segment_id', 'epoch', 'offset', 'length')
FIELDS32 = ('backing_token_id', 'home_cna', 'gsva_token_id', 'cache_policy', 'p_tag', 'access_flags', 'owner_node', 'node_count', 'segment_flags')
HASHES = ('authority_sha256', 'correlation_sha256', 'descriptor_sha256')


def check(flag, why):
    if not flag: raise ValueError(why)


def descriptor_metadata(opaque_hex):
    """Fixed MSOB-v2 public slices only. Offset92 is never decoded or returned."""
    check(type(opaque_hex) is str and re.fullmatch('[0-9a-f]{192}', opaque_hex), 'exact opaque MSOB-v2 hex required')
    def number(at, size):
        check(at + size <= 92, 'secret descriptor slice forbidden')
        return int(opaque_hex[at * 2:(at + size) * 2], 16)
    check((number(0, 4), number(4, 4), number(8, 4), number(12, 4)) == (0x4d534f42, 2, 96, 0), 'fixed MSOB-v2 ABI required')
    row = {name: number(at, size) for name, at, size in (
        ('export_mem_id', 16, 8), ('remote_uba', 24, 8), ('size', 32, 8), ('backing_token_id', 40, 4), ('home_cna', 44, 4),
        ('segment_id', 48, 8), ('epoch', 56, 8), ('segment_flags', 64, 4), ('owner_node', 68, 4), ('node_count', 72, 4),
        ('cache_policy', 76, 4), ('p_tag', 80, 4), ('access_flags', 84, 4), ('gsva_token_id', 88, 4))}
    check(all(row[key] > 0 for key in ('export_mem_id', 'remote_uba', 'size', 'backing_token_id', 'home_cna', 'segment_id', 'epoch', 'gsva_token_id')),
          'native identity contains zero')
    check(row['segment_flags'] == 7 and row['cache_policy'] == 1 and 2 <= row['node_count'] <= 8 and
          row['owner_node'] < row['node_count'] and row['access_flags'] & 1, 'unsupported native descriptor geometry/permissions')
    row['descriptor_sha256'] = hashlib.sha256(opaque_hex.encode('ascii')).hexdigest()
    return row


def encode(command):
    required = {'version', 'operation', 'control_generation', *HASHES, *FIELDS64, *FIELDS32}
    check(type(command) is dict and set(command) == required and type(command['version']) is int and command['version'] == 1 and command['operation'] in OPS,
          'exact versioned metadata command required')
    for name in ('control_generation', *FIELDS64):
        check(type(command[name]) is int and 0 <= command[name] < 1 << 64, 'bad uint64: ' + name)
    for name in FIELDS32:
        check(type(command[name]) is int and 0 <= command[name] < 1 << 32, 'bad uint32: ' + name)
    for name in HASHES:
        check(type(command[name]) is str and re.fullmatch('[0-9a-f]{64}', command[name]), 'bad digest: ' + name)
    check(command['size'] > 0 and command['length'] > 0 and command['offset'] <= command['size'] and
          command['length'] <= command['size'] - command['offset'] and command['remote_uba'] <= ((1 << 64) - 1) - command['size'],
          'nonempty exact native range required')
    check(command['segment_flags'] == 7 and command['cache_policy'] == 1 and command['access_flags'] & 1 and
          2 <= command['node_count'] <= 8 and command['owner_node'] < command['node_count'] and
          all(command[name] > 0 for name in ('export_mem_id', 'segment_id', 'epoch', 'backing_token_id', 'home_cna', 'gsva_token_id')),
          'unsupported native identity')
    if command['operation'] in ('begin', 'end'):
        check(command['control_generation'] > 0 and command['map_id'] > 0 and command['lease_epoch'] > 0 and
              all(command[name] != '0' * 64 for name in HASHES), 'real native alias and one-use authority required')
    body = WIRE.pack(b'UMC1', OPS[command['operation']], command['control_generation'],
                     *(bytes.fromhex(command[name]) for name in HASHES), *(command[name] for name in FIELDS64),
                     *(command[name] for name in FIELDS32))
    return 'umc1:' + body.hex()


def decode(wire):
    check(type(wire) is str and re.fullmatch('umc1:[0-9a-f]{440}', wire), 'exact bounded canonical control wire required')
    body = bytes.fromhex(wire[5:]); check(body[5:8] == b'\0' * 3, 'reserved control bytes must be zero')
    values = WIRE.unpack(body); check(values[0] == b'UMC1' and values[1] in OPS.values(), 'unknown control version/opcode')
    command = {'version': 1, 'operation': next(k for k, v in OPS.items() if v == values[1]), 'control_generation': values[2]}
    command.update({name: value.hex() for name, value in zip(HASHES, values[3:6])})
    command.update(zip(FIELDS64, values[6:15])); command.update(zip(FIELDS32, values[15:24]))
    check(encode(command) == wire, 'noncanonical wire')
    return command


def request_sha256(wire):
    decode(wire)
    return hashlib.sha256(bytes.fromhex(wire[5:])).hexdigest()


COUNTERS = ('read_callbacks', 'cache_hits', 'remote_fill_calls', 'remote_fill_requested_bytes',
            'remote_fill_successful_bytes', 'evictions', 'invalidations', 'late_fill_discards', 'remote_failures')


def _u64_text(value):
    check(type(value) is str and re.fullmatch('0|[1-9][0-9]{0,19}', value) and int(value) < 1 << 64,
          'canonical unsigned uint64 decimal string required')
    return int(value)


def validate_reply(wire, value, expected_pid):
    """Strict reply schema. Booleans/floats are never integer counters/aliases."""
    command = decode(wire)
    def unique(pairs):
        result = {}
        for key, item in pairs:
            check(key not in result, 'duplicate reply member'); result[key] = item
        return result
    def invalid_constant(value): raise ValueError('nonfinite JSON rejected')
    if type(value) is str:
        try: body = value.encode('ascii')
        except UnicodeError as error: raise ValueError('ASCII reply required') from error
        check(0 < len(body) <= 4096, 'bounded ASCII metadata reply required')
        value = json.loads(body, object_pairs_hook=unique, parse_constant=invalid_constant)
    else:
        check(type(value) is dict, 'typed metadata reply required')
        try: body = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode('ascii')
        except (UnicodeError, ValueError) as error: raise ValueError('finite ASCII reply required') from error
        check(0 < len(body) <= 4096, 'bounded ASCII metadata reply required')
    required = {'version', 'feature', 'pid', 'operation', 'request_sha256', 'control_generation', *HASHES,
                'code', 'active', 'in_flight', 'cache_pages', 'native_alias', 'counters', 'scope'}
    check(type(value) is dict and set(value) == required and type(value['version']) is int and value['version'] == 1 and
          value['feature'] == FEATURE and value['scope'] == 'UFlow-Catalog-managed-only' and
          type(expected_pid) is int and expected_pid > 0 and type(value['pid']) is int and value['pid'] == expected_pid and
          value['operation'] == command['operation'] and value['request_sha256'] == request_sha256(wire) and
          value['control_generation'] == str(command['control_generation']) and all(value[name] == command[name] for name in HASHES),
          'reply schema/source/authority/correlation mismatch')
    check(value['code'] in ('ok', 'busy', 'denied', 'disabled', 'replay', 'full') and type(value['active']) is bool and
          type(value['in_flight']) is int and 0 <= value['in_flight'] <= 1 and
          type(value['cache_pages']) is int and 0 <= value['cache_pages'] <= 2048, 'reply state types/bounds')
    counters = value['counters']
    check(type(counters) is dict and set(counters) == {*COUNTERS, 'saturated'} and type(counters['saturated']) is bool,
          'complete typed counters required')
    for key in COUNTERS: _u64_text(counters[key])
    alias = value['native_alias']
    if alias is not None:
        check(type(alias) is dict and set(alias) == {*FIELDS64, *FIELDS32, 'descriptor_sha256'}, 'exact public native alias schema required')
        for key in FIELDS64:
            actual = _u64_text(alias[key])
            if key in ('map_id', 'lease_epoch') and command['operation'] == 'probe':
                check(actual > 0, 'probe must return real native map/token epoch')
            else: check(actual == command[key], 'native alias mismatch: ' + key)
        for key in FIELDS32:
            check(type(alias[key]) is int and 0 <= alias[key] < 1 << 32 and alias[key] == command[key], 'native alias uint32 mismatch: ' + key)
        check(alias['descriptor_sha256'] == command['descriptor_sha256'], 'opaque native identity digest differs')
    if value['code'] == 'ok':
        check(alias is not None, 'successful reply requires real bound native alias')
    if value['code'] == 'ok' and command['operation'] == 'begin':
        check(value['active'] is True and value['in_flight'] == value['cache_pages'] == 0, 'begin must start empty')
    if value['code'] == 'ok' and command['operation'] == 'end':
        check(value['active'] is False and value['in_flight'] == value['cache_pages'] == 0, 'end ACK requires zero retained entries/fills')
    return value
