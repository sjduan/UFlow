"""Consumer trust/lifecycle tests; no future production artifacts fabricated."""
from contextlib import contextmanager
import copy
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).parents[2]/'tools/phasec'))
import profile_metadata_gate as gate
import profile_metadata_completion as completion
import qemu_profile_gate as qemu

class ProfileMetadataConsumerTests(unittest.TestCase):
    def test_fixed_raw_metadata_anchor_cannot_be_caller_descriptor(self):
        a=gate.metadata_anchors();self.assertEqual(a['producer_commit'],'6aaf99a2e52dbf6cd82b39b833a570a096bd1e47')
        with self.assertRaisesRegex(ValueError,'anchor drift'):gate.metadata_anchors(b'{"ready":true}')
    def test_host_or_caller_identity_rejects_before_loading_any_source(self):
        with patch.object(gate,'metadata_producer',side_effect=AssertionError('must not execute')):
            for identity in ({'sha256':'3c34'},qemu._identity('/cpu/fixture','0'*64,{},qemu._HOST)):
                with self.subTest(identity=type(identity)),self.assertRaisesRegex(ValueError,'production-validated'):gate.require_profile_metadata_ready(identity)
    def test_all_dependency_modes_and_refs_precede_first_exec(self):
        calls=[]
        def bound(row,*args):
            calls.append(row['path'])
            if len(calls)==3:raise ValueError('release document drift')
            return b'{}'
        with patch.object(gate,'ap_bound',side_effect=bound),patch.object(gate,'ap_fresh_modules',side_effect=AssertionError('no exec')):
            with self.assertRaisesRegex(ValueError,'document drift'):
                with gate.metadata_producer():pass
        self.assertEqual(len(calls),3)
    def test_wrong_actual_profile_rejects_before_historical_audit(self):
        a=gate.metadata_anchors();p=types.SimpleNamespace(validate_profile=lambda:{'qemu_binary_sha256':'0'*64},
            historical_inventory=lambda:(_ for _ in ()).throw(AssertionError('no historical evaluation')))
        @contextmanager
        def producer():yield a,p
        with patch.object(gate,'_require_production_identity'),patch.object(gate,'metadata_producer',producer):
            with self.assertRaisesRegex(ValueError,'metadata profile differs'):gate.require_profile_metadata_ready(types.SimpleNamespace(sha256=a['qemu_binary_sha256']))
    def test_future_case_requires_actual_reviewed_context_before_IO(self):
        with patch.object(completion,'ap_bound',side_effect=AssertionError('no evidence IO')):
            with self.assertRaisesRegex(ValueError,'context references'):completion.runtime_case(Path('/cpu'),None,gate.metadata_anchors())
    def test_failed_original_exit_is_preserved_and_physical_closure_independent(self):
        r={'actual_task_exit_code':1,'process_tree_empty':True,'task_resources_closed':True,'remaining':[],
            'evidence_errors':[],'isolation_error':None}
        completion.require_closed(r);self.assertEqual(r['actual_task_exit_code'],1)
        for k,v in [('process_tree_empty',False),('task_resources_closed',False),('remaining',[1]),('evidence_errors',['lost']),('isolation_error','denied'),('process_tree_empty',1)]:
            changed=copy.deepcopy(r);changed[k]=v
            with self.subTest(field=k),self.assertRaisesRegex(ValueError,'physical closure'):completion.require_closed(changed)

if __name__=='__main__':unittest.main()
