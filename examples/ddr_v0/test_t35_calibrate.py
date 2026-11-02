"""Focused UNIT tests. Scripted responses below are NOT hardware evidence."""
import dataclasses
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'sdk/python'))
from uflow import ExternalBackingHandle, ExternalDdrError
import t35_calibrate as c
import t35_cooperative_run as supervisor


def handle(number):
    return ExternalBackingHandle('host', 'boot', 'broker', 'owner', 'owner-name',
                                 3, 7, number, number + 100, number + 200, number + 300, 16384,
                                 'staging' if number == 2 else 'immutable')


class ScriptedClient:
    def __init__(self, failures=(), phase='plan'):
        self.failures, self.phase = list(failures), phase
        self.calls = {'plan': 0, 'submit': 0, 'wait': 0, 'cancel': 0}
    def step(self, name):
        self.calls[name] += 1
        if name == self.phase and self.failures:
            raise self.failures.pop(0)
    def plan(self, *args, **kwargs):
        self.step('plan')
        return {'plan_id': '1', 'plan_token': 'usable-plan-secret', 'automatic_ranking': 'disabled', 'cost_version': 'unknown'}
    def submit(self, *args, **kwargs):
        self.step('submit')
        return {'event_id': '2', 'event_token': 'usable-event-secret'}
    def wait(self, *args, **kwargs):
        self.step('wait')
        return {'event_status': 'complete', 'drained': 'true', 'copied_bytes': '4096'}
    def cancel(self, *args, **kwargs):
        self.step('cancel')
        return {'drained': 'false'}


def run_copy(client, path, requester=0):
    context = {'concurrency': 2, 'bytes': 4096, 'round': 4, 'requester': requester,
               'sample_id': f'c2-n4096-r4-q{requester}', 'pid': os.getpid()}
    return c.copy_attempts(client, handle(1), handle(2), 4096, context,
                           lambda row: c.append_json(path, row))


def rows(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


class RetryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'attempts.jsonl'

    def test_exact_plan_once_then_one_business_submit(self):
        client = ScriptedClient([ExternalDdrError(c.FRESHNESS_REJECTION)])
        result = run_copy(client, self.path)
        self.assertEqual(client.calls, dict(plan=2, submit=1, wait=1, cancel=0))
        log = rows(self.path)
        self.assertEqual([r['kind'] for r in log], ['failed_attempt', 'copy_complete'])
        failed = log[0]
        self.assertEqual(failed['target'], dataclasses.asdict(handle(2)))
        self.assertEqual(failed['error'], c.FRESHNESS_REJECTION)
        self.assertEqual(failed['failed_phase'], 'plan')
        self.assertGreaterEqual(failed['failed_phase_seconds'], 0)
        self.assertEqual(failed['sample_id'], log[1]['sample_id'])
        self.assertEqual(len(result[-1]), 1)
        self.assertNotIn('usable-', self.path.read_text())

    def test_four_rejections_persist_before_every_retry_no_submit(self):
        class Checked(ScriptedClient):
            def plan(client, *args, **kwargs):
                if client.calls['plan']:
                    self.assertEqual(len(rows(self.path)), client.calls['plan'])
                return super(Checked, client).plan(*args, **kwargs)
        client = Checked([ExternalDdrError(c.FRESHNESS_REJECTION) for _ in range(4)])
        with self.assertRaisesRegex(RuntimeError, '4 attempts'):
            run_copy(client, self.path)
        log = rows(self.path)
        self.assertEqual([r['attempt'] for r in log], [1, 2, 3, 4])
        self.assertEqual([r['will_retry'] for r in log], [True, True, True, False])
        self.assertEqual(client.calls['submit'], 0)

    def test_same_text_submit_and_wait_are_not_replanned(self):
        for phase in ('submit', 'wait'):
            with self.subTest(phase=phase):
                path = self.path.with_name(phase)
                client = ScriptedClient([ExternalDdrError(c.FRESHNESS_REJECTION)], phase)
                with self.assertRaises(ExternalDdrError):
                    run_copy(client, path)
                self.assertEqual(client.calls['plan'], 1)
                self.assertEqual(client.calls['submit'], 1)
                self.assertFalse(rows(path)[0]['will_retry'])
                if phase == 'wait':
                    self.assertEqual(client.calls['cancel'], 1)
                    self.assertTrue(rows(path)[1]['drained'])

    def test_unknown_transport_and_nonexact_messages_not_retried(self):
        errors = [RuntimeError(c.FRESHNESS_REJECTION), TimeoutError(c.FRESHNESS_REJECTION),
                  ConnectionError('disconnect'), ExternalDdrError('invalid broker response'),
                  ExternalDdrError(c.FRESHNESS_REJECTION + ' suffix'),
                  ExternalDdrError('prefix ' + c.FRESHNESS_REJECTION),
                  ExternalDdrError('clock freshness timeout'),
                  ExternalDdrError('foreign or consumed plan; explicitly replan'),
                  ExternalDdrError('foreign or consumed plan, explicitly replan'),
                  ExternalDdrError('permission denied'), ExternalDdrError('resource range out of bounds'),
                  ExternalDdrError('authority poisoned'), ExternalDdrError('stale resource identity'),
                  ExternalDdrError('unknown; please replan')]
        for i, error in enumerate(errors):
            with self.subTest(error=repr(error)):
                client = ScriptedClient([error])
                with self.assertRaises(type(error)):
                    run_copy(client, self.path.with_name(str(i)))
                self.assertEqual(client.calls['plan'], 1)
                self.assertEqual(client.calls['submit'], 0)

    def test_safe_stale_plan_and_submit_rejections(self):
        for phase in ('plan', 'submit'):
            for i, message in enumerate(c.STALE_REJECTIONS):
                with self.subTest(phase=phase, message=message):
                    client = ScriptedClient([ExternalDdrError(message)], phase)
                    run_copy(client, self.path.with_name(f'{phase}{i}'))
                    self.assertEqual(client.calls['plan'], 2)
                    self.assertEqual(client.calls['submit'], 1 if phase == 'plan' else 2)
                    self.assertEqual(client.calls['wait'], 1)

    def test_successful_other_requester_not_replayed(self):
        good, bad = ScriptedClient(), ScriptedClient([ExternalDdrError(c.FRESHNESS_REJECTION)])
        run_copy(good, self.path, 0)
        run_copy(bad, self.path.with_name('other'), 1)
        self.assertEqual(good.calls['submit'], 1)
        self.assertEqual(bad.calls['submit'], 1)

    def test_cancel_failure_never_replans_and_still_requires_real_wait(self):
        client = ScriptedClient([ExternalDdrError(c.FRESHNESS_REJECTION)], 'wait')
        client.cancel = lambda *a, **k: (_ for _ in ()).throw(ExternalDdrError(c.FRESHNESS_REJECTION))
        with self.assertRaises(ExternalDdrError):
            run_copy(client, self.path)
        self.assertEqual(client.calls['plan'], 1)
        self.assertEqual(rows(self.path)[1]['failed_phase'], 'cancel')
        self.assertFalse(rows(self.path)[1]['will_retry'])
        self.assertTrue(rows(self.path)[-1]['drained'])
        self.assertEqual(client.calls['wait'], 2)

    def test_tokens_removed_recursively_but_full_identity_preserved(self):
        value = {'plan_token': 'a', 'event_token': 'b', 'proof': 'c',
                 'nested': [{'plan_id': 5, 'target': dataclasses.asdict(handle(2))}]}
        self.assertEqual(c.public_evidence(value), {'nested': value['nested']})

    def test_peer_close_consumes_pending_result_then_cleanup_never_resends_copy(self):
        peer = c.Peer.__new__(c.Peer)
        peer.pending = True
        peer.proc = SimpleNamespace(poll=lambda: None, wait=lambda timeout: 0,
                                    stdin=SimpleNamespace(closed=False, close=lambda: None))
        peer.log = SimpleNamespace(close=lambda: None)
        actions = []
        def receive():
            actions.append('finish-copy'); peer.pending = False
            return {'drained': 'true'}
        peer.receive = receive
        peer.command = lambda op: actions.append(op) or {'cleaned': True}
        peer.close()
        self.assertEqual(actions, ['finish-copy', 'cleanup'])

    def test_peer_cleanup_failure_still_delivers_eof_and_waits(self):
        peer = c.Peer.__new__(c.Peer)
        peer.pending = False
        actions = []
        peer.proc = SimpleNamespace(poll=lambda: None, wait=lambda timeout: actions.append('wait') or 1,
                                    stdin=SimpleNamespace(closed=False, close=lambda: actions.append('eof')))
        peer.log = SimpleNamespace(close=lambda: actions.append('log-close'))
        def rejected(op):
            actions.append(op)
            raise RuntimeError('cleanup explicitly rejected')
        peer.command = rejected
        with self.assertRaisesRegex(RuntimeError, 'cleanup explicitly rejected'):
            peer.close()
        self.assertEqual(actions, ['cleanup', 'eof', 'wait', 'log-close'])


class LifecycleTests(unittest.TestCase):
    def test_expired_metadata_not_refreshed_by_report(self):
        metadata = {'sampled_at_unix_ns': 1, 'expires_at_unix_ns': 2}
        with patch.object(c, 'summarize', return_value={'profiles': [{'ranking_validated': True}],
                                                     'automatic_ranking': 'disabled'}):
            report = c.offline_report([], metadata)
        self.assertEqual(metadata, {'sampled_at_unix_ns': 1, 'expires_at_unix_ns': 2})
        self.assertEqual(report['evidence_validity'], 'unknown-expired-or-clock-changed')
        self.assertFalse(report['profiles'][0]['ranking_validated'])
        self.assertEqual(report['profiles'][0]['validity'], 'unknown')

    def test_stop_file_deadline_and_signal_stop_at_checkpoint(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'stop'
            stop = c.CooperativeStop(30, path)
            stop.checkpoint()
            path.touch()
            with self.assertRaises(c.StopRequested): stop.checkpoint()
            with self.assertRaises(c.StopRequested): c.CooperativeStop(0).checkpoint()
            stop = c.CooperativeStop(30)
            stop.request(15, None)
            with self.assertRaises(c.StopRequested): stop.checkpoint()

    def test_failure_cli_nonzero_original_metadata_cleanup_and_no_profile(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / 'output'
            env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1')
            result = subprocess.run([sys.executable, __file__, '--fixture-main', str(out)],
                                    env=env, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0, result.stdout)
            self.assertIn('4 attempts', result.stderr)
            self.assertFalse((out / 'calibration.json').exists())
            self.assertEqual(json.loads((out / 'run-status.json').read_text())['status'], 'failed')
            self.assertTrue(json.loads((out / 'cleanup.json').read_text())['all_recorded_pids_exited'])
            self.assertEqual(len(rows(out / 'attempts.jsonl')), 4)
            metadata = json.loads((out / 'metadata.json').read_text())
            self.assertEqual(metadata['expires_at_unix_ns'] - metadata['sampled_at_unix_ns'], 3600 * 10**9)
            self.assertEqual((out / 'deployment-input.json').read_bytes(), b'{"fixture":"UNIT ONLY"}\n')
            self.assertEqual((out / 'closed.json').read_text(), '["target0-c1-n4096", "source-c1-n4096"]')
            second = subprocess.run([sys.executable, __file__, '--fixture-main', str(out)],
                                    env=env, capture_output=True, text=True)
            self.assertNotEqual(second.returncode, 0)
            self.assertIn('FileExistsError', second.stderr)

    def test_supervisor_requests_stop_and_waits_for_normal_child_exit(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); stop = root / 'stop'
            child = root / 'child.py'
            child.write_text('from pathlib import Path\nimport sys,time\np=Path(sys.argv[1])\n'
                             'while not p.exists(): time.sleep(.01)\ntime.sleep(.05)\nsys.exit(1)\n')
            rc = supervisor.supervise([sys.executable, str(child), str(stop)], .1, 3,
                                      stop, root / 'log', root / 'result')
            self.assertEqual(rc, 1)
            result = json.loads((root / 'result').read_text())
            self.assertTrue(result['stop_requested'])
            self.assertFalse(result['process_still_running'])
            self.assertEqual(result['exit'], 1)

    def test_supervisor_deadline_preserves_process_and_reports_failure(self):
        # Fake still-live process: no kill/terminate method exists. No real
        # holder is left behind by this UNIT timeout-path test.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake = SimpleNamespace(pid=999999999, poll=lambda: None)
            with patch.object(supervisor.subprocess, 'Popen', return_value=fake):
                rc = supervisor.supervise(['UNIT-ONLY'], .05, .05, root/'stop', root/'log', root/'result')
            self.assertEqual(rc, 2)
            result = json.loads((root/'result').read_text())
            self.assertTrue(result['process_still_running'])
            self.assertEqual(result['status'], 'failed')


def fixture_main(output):
    """A real failing CLI lifecycle with mocked transport, explicitly UNIT only."""
    output = Path(output)
    deployment = output.parent / 'deployment.json'
    deployment.write_bytes(b'{"fixture":"UNIT ONLY"}\n')
    closed = []
    client = ScriptedClient([ExternalDdrError(c.FRESHNESS_REJECTION) for _ in range(4)])
    class Peer:
        def __init__(self, args, cpu, label):
            assert (output/'metadata.json').exists() and (output/'deployment-input.json').exists()
            # No real PID: participants are mocked, not hardware lifecycle data.
            self.start = {'pid': 999999999}
            self.proc = SimpleNamespace(pid=999999999)
            self.label, self.req = label, None
        def command(self, op, **values):
            if op == 'source': return {'handle': dataclasses.asdict(handle(1))}
            if op == 'prepare': return {'target': dataclasses.asdict(handle(2))}
            raise AssertionError(op)
        def send(self, op, **values): self.req = op
        def receive(self):
            if self.req == 'map': return {}
            return run_copy(client, output / 'attempts.jsonl')
        def close(self):
            closed.append(self.label)
            (output / 'closed.json').write_text(json.dumps(closed))
    def snapshot(*args):
        return dict.fromkeys(('committed_bytes', 'backing_fds', 'owner_leases', 'active_views', 'pending_handoffs', 'in_flight'), 0)
    argv = ['calibrate', '--endpoint', 'UNIT-ONLY', '--output', str(output), '--source-commit', 'unit-fixture',
            '--deployment-metadata', str(deployment), '--soft-deadline-seconds', '30']
    with patch('sys.argv', argv), patch.object(c, 'Peer', Peer), patch('uflow.ExternalDdrClient.snapshot', snapshot):
        c.main()


if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == '--fixture-main':
        fixture_main(sys.argv[2])
    else:
        unittest.main()
