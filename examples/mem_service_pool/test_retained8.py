"""Portable semantic fixtures only; never invoke production retained8 verify."""
import base64,copy,json
from pathlib import Path
import sys,unittest
from unittest.mock import MagicMock,patch
from types import SimpleNamespace
sys.path.insert(0,str(Path(__file__).parents[2]/'tools/phasec'))
from verify_retained8_pool import documents,store_family,nonlive,ASSESSMENT_SHA,sha
import verify_retained8_pool as verifier

class Retained8Semantics(unittest.TestCase):
    def setUp(self):
        p=Path(__file__).parent/'reconciliation'
        a=(p/'retained8-assessment.json').read_bytes();self.assertEqual(sha(a),ASSESSMENT_SHA);self.a=json.loads(a)
        f=json.loads((p/'retained8-cpu-documents.json').read_bytes());self.assertEqual(f['marker'],'CPU-semantic-fixture-not-original-byte-proof')
        self.raw={n:base64.b64decode(v) for n,v in f['files_base64'].items()}
        self.old=json.loads(self.raw['implementation/responsibility-c013/result.json'])['verified_proof']
    def derive(self):return documents(self.a,self.raw,self.old)
    def change(self,n,mutate):
        v=json.loads(self.raw[n]);mutate(v);self.raw[n]=json.dumps(v,separators=(',',':')).encode()
    def test_exact_semantics_retains_all_four_without_cancel_or_ack(self):
        p=self.derive();self.assertEqual([r['object_id'] for r in p['retained_canaries']],[100,101,103,104])
        self.assertEqual(p['charged_bytes'],8<<20);self.assertEqual(p['reserved_bytes'],0)
        self.assertIs(p['cancel64_repeated'],False);self.assertIs(p['SDK_ACK_Unmapped_or_canary_close_claimed'],False)
    def test_missing_or_extra_evidence(self):
        self.raw.pop('runs/c027/provider.json')
        with self.assertRaises(ValueError):self.derive()
        self.setUp();self.raw['extra']=b'{}'
        with self.assertRaises(ValueError):self.derive()
    def test_wrong_predecessor_phase_or_proof(self):
        for n,key,value in [('implementation/responsibility-c013/intent.json','phase','partial'),
                ('implementation/responsibility-c013/result.json','proof_sha256','drift'),
                ('implementation/responsibility-c013/result.json','cancelled_uncreated_bytes',0)]:
            self.setUp();self.change(n,lambda v:v.update({key:value}))
            with self.assertRaises(ValueError):self.derive()
    def test_business_or_reserved_or_native_work_not_hidden(self):
        for key,value in [('pools',[{}]),('operations',[{}]),('active_bytes',1),('creation_reserved_bytes',0),('native_jobs_executed',1)]:
            self.setUp();self.change('runs/c027/state/state.json',lambda v:v.update({key:value}))
            with self.assertRaises(ValueError):self.derive()
    def test_wrong_boot_and_session(self):
        self.change('runs/c027/guest-identities.json',lambda v:v[0].update(boot_id='wrong-boot'))
        with self.assertRaises(ValueError):self.derive()
        self.setUp();self.change('runs/c027/provider.json',lambda v:v.update(session='wrong-session'))
        with self.assertRaises(ValueError):self.derive()
    def test_wrong_actual_checksum_or_descriptor(self):
        name='runs/c027/pool-events.jsonl'
        for key,value in [('checksum',1),('canary_descriptor','00')]:
            self.setUp();events=[json.loads(s) for s in self.raw[name].splitlines()]
            e=next(e for e in events if e['event']=='bootstrap-prepared');e['receipt'][key]=value
            self.raw[name]=('\n'.join(json.dumps(e) for e in events)+'\n').encode()
            with self.assertRaises(ValueError):self.derive()
    def test_missing_serial_receipt_and_serial_boot_drift(self):
        n='runs/c027/nodeA.serial.log';self.raw[n]=b'no actual serial\n'
        with self.assertRaises(ValueError):self.derive()
    def test_new_business_wire_or_extra_event(self):
        n='runs/c027/pool-events.jsonl';self.raw[n]+=b'{"event":"E1-one-allocation"}\n'
        with self.assertRaises(ValueError):self.derive()
    def test_no_store_family_even_not_found_or_incomplete(self):
        store_family([])
        for names in [['lower.store'],['lower.store.journal'],['lower.store.managed'],['lower.store.next']]:
            with self.assertRaises(ValueError):store_family(names)
    def test_live_birth_rejected_reused_pid_not_ack(self):
        with self.assertRaises(ValueError):nonlive({'71':123},lambda pid:123)
        nonlive({'71':123},lambda pid:124)
        self.assertIs(self.derive()['SDK_ACK_Unmapped_or_canary_close_claimed'],False)
    def test_unclosed_physical_tree_or_inconclusive_old_audit(self):
        self.change('runs/c027/monitor/result.json',lambda v:v.update(remaining={'71':123}))
        with self.assertRaises(ValueError):self.derive()
    def test_fresh_holder_audit_fd_vma_denied_and_deadline_fail_closed(self):
        process=MagicMock();process.name='71';root=MagicMock();root.iterdir.return_value=[process]
        scan=MagicMock();scan.__enter__.return_value=[]
        with patch.object(verifier,'Path',return_value=root),patch.object(verifier.os,'scandir',return_value=scan),patch.object(verifier,'proc_read',return_value=b''):
            self.assertEqual(verifier.holder_audit({(2,3)})['holders'],[])
            scan.__enter__.return_value=[SimpleNamespace(stat=lambda:SimpleNamespace(st_dev=2,st_ino=3))]
            with self.assertRaises(ValueError):verifier.holder_audit({(2,3)})
            scan.__enter__.return_value=[]
            with patch.object(verifier.os,'makedev',return_value=2,create=True),patch.object(verifier,'proc_read',return_value=b'1000-2000 r--p 0 00:02 3 /old-fm\n'):
                with self.assertRaises(ValueError):verifier.holder_audit({(2,3)})
            with patch.object(verifier,'proc_read',side_effect=PermissionError()):
                with self.assertRaises(PermissionError):verifier.holder_audit({(2,3)})
            with patch.object(verifier.time,'monotonic',side_effect=[0,16]):
                with self.assertRaises(ValueError):verifier.holder_audit({(2,3)})
        self.setUp();self.change('runs/c027/post-close-audit.json',lambda v:v['holder_audit'].update(denied=[{'pid':71}]))
        with self.assertRaises(ValueError):self.derive()
if __name__=='__main__':unittest.main()
