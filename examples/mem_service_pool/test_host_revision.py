"""Portable contracts and real owned CPU subprocesses; no OBMM result."""
import json
import hashlib
from pathlib import Path
import subprocess
import sys
import time
import unittest

sys.path.insert(0, str(Path(__file__).parents[2]/'tools/phasec'))
from pool_peer_io import PeerIO
from pool_matrix_contract import (bootstrap_nodes, canary_checksum, guard_intervals,
                                  guard_chunks, CANARY_BYTES, CANARY_VISIBLE)
from run_mem_service_pool_matrix import Peer


class PeerDeadlineTests(unittest.TestCase):
    def run_child(self, action, error, timeout=.5, limit=None, actual_peer=False):
        code = 'import os,sys,time\nsys.stdin.buffer.readline()\n'+action+'\ntime.sleep(60)\n'
        child = subprocess.Popen([sys.executable, '-u', '-c', code], stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)
        notices = []
        io = PeerIO(child, lambda: notices.append(time.monotonic()))
        if limit:
            io.MAX_REPLY = limit
        start = time.monotonic()
        elapsed = None
        try:
            with self.assertRaises(error):
                if actual_peer:
                    peer=Peer.__new__(Peer);peer.child=child;peer.io=io
                    peer.call('real-process-test')
                else:
                    io.transaction({'op': 'real-process-test'}, timeout=timeout)
            elapsed = time.monotonic()-start
            self.assertLess(elapsed, 2)
            self.assertEqual(len(notices), 1)
            self.assertIsNone(child.poll())  # Failure notification did not await child exit.
            self.assertLessEqual(len(io.diagnostic().encode()), 4096)
            with self.assertRaisesRegex(RuntimeError, 'original context'):
                io.transaction({'op': 'forbidden-retry'}, timeout=timeout)
        finally:
            if child.poll() is None:
                child.terminate()
            waited=child.wait(timeout=5)
            io.reaped()
            self.assertTrue(all(not t.is_alive() for t in [*io.workers, io.stderr_worker]))
            self.assertTrue(all(s.closed for s in (child.stdin, child.stdout, child.stderr)))
            print(json.dumps({'event':'owned-process-deadline-regression','child_pid':child.pid,
                'fixture_sha256':hashlib.sha256(code.encode()).hexdigest(),
                'actual_Peer_call':actual_peer,'reply_deadline_seconds':125 if actual_peer else timeout,
                'failure_elapsed_seconds':elapsed,'notice_count':len(notices),'child_wait':waited,
                'all_owned_processes_and_pipe_threads_reaped':True,'all_pipe_handles_closed':True,
                'platform':sys.platform,'Linux_or_Guest_result':False}))

    def test_stdout_eof_does_not_wait_on_open_stderr_or_child(self):
        self.run_child('os.close(1)', RuntimeError, actual_peer=True)

    def test_single_byte_without_newline_obeys_whole_frame_deadline(self):
        self.run_child("os.write(1,b'{')", TimeoutError)

    def test_slow_partial_frame_does_not_reset_deadline(self):
        self.run_child("os.write(1,b'{');time.sleep(.15);os.write(1,b' ')", TimeoutError)

    def test_oversized_frame_fails_before_newline(self):
        self.run_child("os.write(1,b'x'*2048)", RuntimeError, limit=1024)

    def test_malformed_complete_frame_notifies_before_diagnostic(self):
        self.run_child("os.write(2,b'diagnostic');os.write(1,b'not-json\\n')", ValueError)

    def test_real_success_and_all_pipe_workers_reap(self):
        child = subprocess.Popen([sys.executable, '-u', '-c',
            "import sys;sys.stdin.buffer.readline();print('{\"status\":\"ok\",\"result\":17}')"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)
        notices = []
        io = PeerIO(child, lambda: notices.append(True))
        try:
            self.assertEqual(io.transaction({'op': 'success'}, timeout=5)['result'], 17)
            self.assertEqual(child.wait(timeout=5), 0)
            io.reaped()
            self.assertFalse(notices)
        finally:
            if child.poll() is None:
                child.terminate();child.wait(timeout=5)
            io.reaped()


class GuardCoverageTests(unittest.TestCase):
    def segment(self, start, count, version=1):
        return {'offset': start, 'nbytes': count, 'state': 'Committed', 'content_version': version}

    def test_complement_exact_boundaries_and_total_bytes(self):
        guards = guard_intervals(100, [(10, 30), (50, 70), (80, 85)])
        self.assertEqual(guards, [(0, 10), (30, 50), (70, 80), (85, 100)])
        chunks = guard_chunks(guards, [self.segment(0, 100)], 7)
        self.assertEqual(sum(n for _, n in chunks), 55)
        self.assertEqual({i for off, n in chunks for i in range(off, off+n)},
                         set(range(100))-set(range(10, 30))-set(range(50, 70))-set(range(80, 85)))

    def test_merged_widened_written_segment_cannot_hide_corrupt_guard(self):
        guards = guard_intervals(100, [(10, 30)])
        for segments in ([self.segment(0, 100)],
                         [self.segment(0, 5), self.segment(5, 30, 2), self.segment(35, 65)]):
            chunks = guard_chunks(guards, segments, 16)
            data = bytearray(100);data[5] = 1;data[34] = 1
            checked = {i for off, n in chunks for i in range(off, off+n)}
            self.assertTrue({5, 34} <= checked)
            self.assertFalse(all(data[i] == 0 for i in checked))
            self.assertEqual(len(checked), 80)

    def test_snapshot_versions_split_reads_without_changing_expected_coverage(self):
        guards = [(0, 100)]
        self.assertEqual(guard_chunks(guards, [self.segment(0, 40), self.segment(40, 60, 2)], 64),
                         [(0, 40), (40, 60)])

    def test_snapshot_missing_overlapping_or_uncommitted_guard_is_rejected(self):
        for segments in ([self.segment(1, 99)], [self.segment(0, 60), self.segment(50, 50)],
                         [self.segment(0, 100, 0)]):
            with self.assertRaises(RuntimeError):
                guard_chunks([(0, 100)], segments, 16)

    def test_invalid_planned_bounds_rejected(self):
        for written in ([(-1, 3)], [(0, 101)], [(10, 10)], [(10, 30), (29, 40)]):
            with self.assertRaises(ValueError):
                guard_intervals(100, written)


class BootstrapContractTests(unittest.TestCase):
    def nodes(self, history, mutation=None):
        class RecordedNode:
            def __init__(self, name):
                self.identity = {'node': name, 'boot_id': 'boot-'+name,
                                 'agent_session': 'agent-'+name, 'incarnation': 7}

            def exchange(self, frame):
                req = json.loads(frame.body);name = self.identity['node']
                history.append((name, req['op']))
                if req['op'] == 'bootstrap_prepare':
                    receipt = {'schema': 2, 'phase': 'completed', 'canary_prepared': True,
                        'canary_descriptor': 'ab'*180, 'checksum': canary_checksum(req['seed']),
                        'canary_bytes': CANARY_BYTES, 'visible_bytes': CANARY_VISIBLE,
                        'seed': req['seed'], 'guest_pid': 123}
                else:
                    receipt = {'schema': 2, 'phase': 'completed', 'canary_ready': True,
                               'verified_checksum': req['checksum'], 'guest_pid': 123}
                if mutation:
                    mutation(req, receipt)
                return frame.completion(receipt)
        return {name: RecordedNode(name) for name in ('nodeA', 'nodeB')}

    def test_both_prepares_precede_any_peer_verify_and_business_creation(self):
        history, events = [], []
        result = bootstrap_nodes(self.nodes(history), 'simulated:contract', events.append)
        self.assertEqual(history, [('nodeA', 'bootstrap_prepare'), ('nodeB', 'bootstrap_prepare'),
                                   ('nodeA', 'bootstrap_verify'), ('nodeB', 'bootstrap_verify')])
        self.assertEqual(sum(result[n]['canary']['canary_bytes'] for n in result), 4 << 20)
        self.assertLessEqual((64 << 20)+(4 << 20), 128 << 20)
        self.assertEqual(sum(e['event'] == 'bootstrap-responsibility' for e in events), 2)

    def test_wrong_canary_size_or_checksum_prevents_ready(self):
        for field, value in (('canary_bytes', 1), ('checksum', 0), ('canary_descriptor', 'x')):
            history = []
            def mutate(req, receipt):
                if req['op'] == 'bootstrap_prepare':
                    receipt[field] = value
            with self.assertRaises(RuntimeError):
                bootstrap_nodes(self.nodes(history, mutate), 'simulated:contract', lambda e: None)
            self.assertFalse(any(op == 'bootstrap_verify' for _, op in history))

    def test_other_endpoint_pid_or_missing_ready_cannot_certify_same_context(self):
        for field, value in (('guest_pid', 999), ('canary_ready', False), ('verified_checksum', 1)):
            history = []
            def mutate(req, receipt):
                if req['op'] == 'bootstrap_verify':
                    receipt[field] = value
            with self.assertRaises(RuntimeError):
                bootstrap_nodes(self.nodes(history, mutate), 'simulated:contract', lambda e: None)
            self.assertEqual(history[-1], ('nodeA', 'bootstrap_verify'))


if __name__ == '__main__':
    unittest.main()
