"""Retirement, responsibility and bounded-control regressions (no device imports)."""
import copy
import json
import unittest

from session_protocol import GROUP, POOL_BYTES, check_final, check_retired, pool_record, receive
from session_owner import Owner


def fixture():
    descriptor = {'pool_id': 'real-original', 'pool_epoch': 'epoch', 'state': 'Allocated'}
    pool = {**descriptor, 'frozen': False, 'clients': [
        {'client_session': s, 'state': 'Unmapped', 'children': []} for s in ('bootstrap', 'e', 'f')],
        'ranges': {'leases': []}}
    snapshot = {'budget_bytes': POOL_BYTES, 'charged_bytes': POOL_BYTES,
                'creation_reserved_bytes': 0, 'pools': [pool]}
    lines = {'O1': {'clients': ['e'], 'retired': True}, 'M1': {'clients': ['f'], 'retired': True}}
    return descriptor, pool, snapshot, lines


class RetirementTests(unittest.TestCase):
    def test_destroy_blocked_for_live_other_line(self):
        _, pool, _, lines = fixture()
        lines['M1']['retired'] = False
        with self.assertRaises(ValueError):
            check_final(pool, lines, 'bootstrap')

    def test_every_actual_obligation_blocks_final_destroy(self):
        for kind in ('mapped', 'unknown', 'child', 'range', 'unaccounted'):
            with self.subTest(kind=kind):
                _, pool, _, lines = fixture()
                if kind in ('mapped', 'unknown'):
                    pool['clients'][2]['state'] = kind.capitalize()
                elif kind == 'child':
                    pool['clients'][2]['children'] = [{'pid': 123}]
                elif kind == 'range':
                    pool['ranges']['leases'] = [{'client_session': 'f'}]
                else:
                    pool['clients'].append({'client_session': 'unlisted', 'state': 'Unmapped', 'children': []})
                with self.assertRaises(ValueError):
                    check_final(pool, lines, 'bootstrap')

    def test_real_unmap_allows_line_retirement_and_final(self):
        _, pool, _, lines = fixture()
        check_retired(pool, ['e'])
        check_final(pool, lines, 'bootstrap')

    def test_descriptor_unknown_and_budget_drift_rejected(self):
        descriptor, _, original, _ = fixture()
        pool_record(original, descriptor)
        for kind in ('epoch', 'hidden-pool', 'charge', 'reserve', 'frozen', 'unknown', 'unknown-child', 'unknown-lease', 'bool'):
            value = copy.deepcopy(original)
            pool = value['pools'][0]
            if kind == 'epoch':
                pool['pool_epoch'] = 'new-epoch'
            elif kind == 'hidden-pool':
                value['pools'].append(copy.deepcopy(pool))
            elif kind == 'charge':
                value['charged_bytes'] = 0
            elif kind == 'reserve':
                value['creation_reserved_bytes'] = POOL_BYTES
            elif kind == 'frozen':
                pool['frozen'] = True
            elif kind == 'unknown':
                pool['clients'][0]['state'] = 'Unknown'
            elif kind == 'unknown-child':
                pool['clients'][0]['children'] = [{'unknown': True}]
            elif kind == 'unknown-lease':
                pool['ranges']['leases'] = [{'state': 'Unknown'}]
            else:
                value['creation_reserved_bytes'] = False
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                pool_record(value, descriptor)

    def test_unobserved_session_is_not_retirement(self):
        with self.assertRaises(ValueError):
            check_retired(fixture()[1], ['unseen'])


class ControlTests(unittest.TestCase):
    def test_duplicate_fields_and_multiple_frames_rejected(self):
        for data in (b'{"op":"describe","op":"exec"}\n', b'{}\n{}\n', b'[]\n', b'x' * 65537):
            # In-memory receive source avoids a blocking oversized socket send.
            class Input:
                def __init__(self, data):
                    self.data = data

                def recv(self, count):
                    result, self.data = self.data[:count], self.data[count:]
                    return result
            with self.subTest(data=data[:40]), self.assertRaises((ValueError, json.JSONDecodeError)):
                receive(Input(data))

    def test_no_exec_admin_or_unknown_command_surface(self):
        owner = Owner.__new__(Owner)
        for body in ({'op': 'exec', 'command': 'anything'}, {'op': 'finalize'},
                     {'op': 'describe', 'admin_token': 'never'}, {'op': 'describe', 'schema': True}):
            with self.subTest(body=body), self.assertRaises(ValueError):
                owner.handle({'schema': 1, 'group': GROUP, **body})

    def test_registered_client_cannot_cross_lines_or_retire_incomplete(self):
        owner = Owner.__new__(Owner)
        _, pool, snapshot, lines = fixture()
        owner.lines = {k: {**v, 'retired': False} for k, v in lines.items()}
        owner.bootstrap_client = 'bootstrap'
        owner.snapshot = lambda _: (snapshot, pool)
        owner.evidence = lambda *args: None
        base = {'schema': 1, 'group': GROUP}
        for body in ({'op': 'register-client', 'line': 'M1', 'client_session': 'e'},
                     {'op': 'line-retired', 'line': 'M1', 'client_sessions': []},
                     {'op': 'line-retired', 'line': 'M1', 'client_sessions': ['f', 'f']}):
            with self.subTest(body=body), self.assertRaises(ValueError):
                owner.handle({**base, **body})
        owner.handle({**base, 'op': 'line-retired', 'line': 'O1', 'client_sessions': ['e']})
        self.assertTrue(owner.lines['O1']['retired'])
        self.assertFalse(owner.lines['M1']['retired'])
        with self.assertRaises(ValueError):
            owner.handle({**base, 'op': 'register-client', 'line': 'O1', 'client_session': 'e'})


if __name__ == '__main__':
    unittest.main()
