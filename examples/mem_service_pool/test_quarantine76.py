"""Semantic CPU evidence only; never invoke the production verifier."""
import base64,copy,json,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parents[2]/'tools/phasec'))
import verify_quarantine76_pool as q
class Quarantine76(unittest.TestCase):
    def setUp(self):
        d=Path(__file__).parent/'reconciliation';b=(d/'quarantine76-assessment.json').read_bytes();self.assertEqual(q.sha(b),q.ASSESSMENT_SHA);self.a=json.loads(b)
        f=json.loads((d/'quarantine76-cpu-documents.json').read_bytes());self.assertEqual(f['marker'],'CPU-semantic-fixture-not-original-byte-proof')
        self.raw={n:base64.b64decode(v) for n,v in f['files_base64'].items()}
    def derive(self):return q.documents(self.a,self.raw)
    def change(self,n,fn):
        v=json.loads(self.raw[n]);fn(v);self.raw[n]=json.dumps(v).encode()
    def test_full76_no_refund_or_backing_invention(self):
        p=self.derive();self.assertEqual([c['object_id'] for c in p['retained_canaries']],[100,101,103,104,105,106])
        self.assertEqual((p['charged_bytes'],p['reserved_bytes']),(12<<20,64<<20));self.assertIsNone(p['uncertain_business_reservation']['pool']['canonical'])
        self.assertFalse(p['cancel64_repeated']);self.assertFalse(p['SDK_ACK_Unmapped_or_canary_close_claimed'])
    def test_missing_and_extra_bound_record(self):
        self.raw.pop('runs/c033/lower.store')
        with self.assertRaises(ValueError):self.derive()
        self.setUp();self.raw['extra']=b'{}'
        with self.assertRaises(ValueError):self.derive()
    def test_ledger_capacity_charge_retained_and_reservation_not_hidden(self):
        for key,value in [('charged_bytes',8<<20),('retained_bytes',12<<20),('active_bytes',0),('creation_reserved_bytes',0),('budget_bytes',256<<20)]:
            self.setUp();self.change('runs/c033/state/state.json',lambda s:s.update({key:value}))
            with self.assertRaises(ValueError):self.derive()
    def test_Unknown_not_relabelled_released_or_no_backing(self):
        for key,value in [('state','Released'),('canonical',{}),('release_result',{'ok':True}),('pool_epoch','wrong')]:
            self.setUp();self.change('runs/c033/state/state.json',lambda s:s['pools'][0].update({key:value}))
            with self.assertRaises(ValueError):self.derive()
    def test_original_reservation_key_and_operation_must_survive(self):
        self.change('runs/c033/state/state.json',lambda s:s['resource_ledger'].update(reservations={'new-key':64<<20}))
        with self.assertRaises(ValueError):self.derive()
        self.setUp();self.change('runs/c033/state/state.json',lambda s:s.update(operations=[]))
        with self.assertRaises(ValueError):self.derive()
    def test_incomplete_predecessor_drift_or_second_cancel(self):
        for key,value in [('cancel64_repeated',True),('successor_phase','Retained8Pending')]:
            self.setUp();self.change('implementation/responsibility-c013/successor-c027-v1/result.json',lambda s:s.update({key:value}))
            with self.assertRaises(ValueError):self.derive()
    def test_boot_receipt_canary_size_and_actual_charge_identity(self):
        for key,value in [('boot_id','reused'),('incarnation',1)]:
            self.setUp();self.change('runs/c033/guest-identities.json',lambda s:s[0].update({key:value}))
            with self.assertRaises(ValueError):self.derive()
        self.setUp();self.change('runs/c033/state/state.json',lambda s:s['resource_ledger']['resources'][0].update(nbytes=1))
        with self.assertRaises(ValueError):self.derive()
    def test_live_or_inconclusive_failure_audit_not_closed(self):
        for key,value in [('remaining',{'71':8}),('process_tree_empty',False),('task_resources_closed',False),('evidence_errors',['denied'])]:
            self.setUp();self.change('runs/c033/monitor/result.json',lambda s:s.update({key:value}))
            with self.assertRaises(ValueError):self.derive()
    def test_FD_denied_unlink_or_fake_ACK_never_become_proof(self):
        for fn in [lambda s:s['holder_audit'].update(denied=[1]),lambda s:s.update(shared_FM_unlinked=True),lambda s:s.update(logical_allocation_ACK_inferred=True)]:
            self.setUp();self.change('runs/c033/post-close-audit.json',fn)
            with self.assertRaises(ValueError):self.derive()
    def test_KVM_host_device_or_arbitrary_QEMU_flag_rejected(self):
        for flag in ['-enable-kvm','-device','vfio-pci,host=0000:01:00.0','-drive']:
            self.setUp();self.change('runs/c033/guest-starts.json',lambda s:s[0]['argv'].append(flag))
            with self.assertRaises(ValueError):self.derive()
    def test_full_predecessor_c033_snapshot_and_native_work(self):
        self.change('runs/c033/state/state.json',lambda s:s['reconciliation'].update(proof_sha256='wrong'))
        with self.assertRaises(ValueError):self.derive()
        self.setUp();self.change('runs/c033/state/state.json',lambda s:s.update(native_jobs_executed=1))
        with self.assertRaises(ValueError):self.derive()
