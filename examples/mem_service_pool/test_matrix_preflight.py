import copy
from types import SimpleNamespace
import unittest
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'tools/phasec'))
from pool_matrix_preflight import worker_serial_preflight


class SerialPreflightTests(unittest.TestCase):
    def run_probe(self,change=None,clock=None):
        sample={'schema':2,'phase':'completed','worker_pid':12,'worker_birth':34,'worker_alive':True,'worker_ready':True,
                'worker_refresh_count':5,'lease_ms':10000,'refresh_age_ms':4}
        readiness={node:{'worker':copy.deepcopy(sample)} for node in ('nodeA','nodeB')};calls=[];events=[]
        def call(node,body):
            calls.append((node,body));value=copy.deepcopy(sample)
            if change:change(value)
            return value
        startup=SimpleNamespace(control=SimpleNamespace(call=call));ticks=iter(clock or [10,20,30,40])
        result=worker_serial_preflight(startup,readiness,events.append,clock=lambda:next(ticks))
        return result,calls,events
    def test_completed_guest_receipts_and_host_rtt_before_business(self):
        result,calls,events=self.run_probe();self.assertEqual(set(result),{'nodeA','nodeB'})
        self.assertEqual(len(calls),2);self.assertTrue(all(v['body_bytes']==1496 for v in result.values()))
        self.assertFalse(events[0]['Guest_acceptance_proved']);self.assertTrue(events[1]['Guest_acceptance_proved'])
    def test_wrong_worker_identity_is_not_accepted(self):
        for key,value in [('worker_pid',13),('worker_birth',35),('worker_alive',False),('worker_ready',False)]:
            with self.subTest(key=key),self.assertRaises(RuntimeError):self.run_probe(lambda r:r.update({key:value}))
    def test_stale_missing_refresh_and_lease_changes_fail_before_business(self):
        for key,value in [('worker_refresh_count',0),('worker_refresh_count',4),('lease_ms',11000),('refresh_age_ms',5000),('refresh_age_ms',-1)]:
            with self.subTest(key=key),self.assertRaises(RuntimeError):self.run_probe(lambda r:r.update({key:value}))
    def test_host_slow_roundtrip_cannot_be_relabelled_guest_fresh(self):
        with self.assertRaises(RuntimeError):self.run_probe(clock=[10,5_000_000_010])
    def test_unknown_completion_is_not_guest_acceptance(self):
        with self.assertRaises(RuntimeError):self.run_probe(lambda r:r.update(phase='unknown'))
