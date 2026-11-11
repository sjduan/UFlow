"""Production admission must reject before caller-selected original IO."""
import json,sys,unittest,os,tempfile
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).parents[2]/'tools/phasec'))
import c101_execution_adapter as adapter
from accepted_c101_v4 import v4_anchors,v4_policy
from managed_gate_v4 import runtime_admission,require_runtime

class ProductionGateTests(unittest.TestCase):
    @unittest.skipUnless(sys.platform=='linux','actual production source root')
    def test_real_reviewed_eleven_MiB_CPU_metadata_fixture_fits_source_gate(self):
        path=Path(__file__).resolve().parent/'reconciliation/closed-c039-cpu-documents.json'
        self.assertGreater(path.stat().st_size,8<<20)
        raw=adapter.c101_source_file(path,{'bytes':path.stat().st_size,'mode':'100644'})
        self.assertEqual(len(raw),11633943)
    @unittest.skipUnless(sys.platform=='linux','actual POSIX source identity')
    def test_empty_tracked_source_is_allowed_but_changed_bytes_mode_and_alias_are_not(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve();path=root/'__init__.py';path.write_bytes(b'');path.chmod(0o644);row={'bytes':0,'mode':'100644'}
            with patch.object(adapter,'AP_ROOT',root):
                self.assertEqual(adapter.c101_source_file(path,row),b'')
                path.write_bytes(b'x')
                with self.assertRaises(ValueError):adapter.c101_source_file(path,row)
                path.write_bytes(b'');path.chmod(0o600)
                with self.assertRaises(ValueError):adapter.c101_source_file(path,row)
                path.chmod(0o644);alias=root/'alias.py';alias.symlink_to(path)
                with self.assertRaises(ValueError):adapter.c101_source_file(alias,row)
    def test_old_context_ready_flags_and_wrong_namespaces_never_read_caller_data(self):
        a=v4_anchors();policy=v4_policy()
        with patch.object(adapter,'ap_read',side_effect=AssertionError('unexpected original IO')):
            for path in ['/missing','/workspace/uflow/runs/mem-service-shared-pool-20261108/runs/c101/state/state.json']:
                with self.assertRaisesRegex(ValueError,'before|no descriptor'):
                    adapter.c101_manifest_gate(path,'a'*64,policy)
            with self.assertRaises(ValueError):adapter.c101_manifest_gate(Path(policy['config_directory'])/'execution.json','A'*64,policy)
        for config in [{'terminal_c101_execution':{'ready':True}},{'terminal_c101_execution':{'path':'/missing','sha256':'a'*64}}]:
            with self.assertRaises(ValueError):runtime_admission(config,{})
        with self.assertRaises(ValueError):require_runtime({'ready':True,'accepted_context':a['accepted_context']})
    def test_corrected_context_and_immutable_case_bind_current_three_resources(self):
        a=v4_anchors();policy=v4_policy();root=Path(__file__).parent/'accepted_c101_v4';case=json.loads((root/'case-template.json').read_bytes())
        self.assertEqual(case['accepted_context'],a['accepted_context']);self.assertIn('/recovery-release-v2/',case['accepted_context']['path'])
        self.assertEqual(case['accepted_candidate'],a['code_and_data']['terminal_candidate']);self.assertEqual(case['resource_ids'],[119,120,121]);self.assertEqual(case['future_canary_ids'],[122,123]);self.assertEqual(case['future_business_id'],124)
        self.assertFalse(set(case)&{'HOST_fixture_only','production_producer_context','production_execution_package','CPU_fixture_only_not_E_accepted_proof'})
        self.assertEqual(policy['phases'],{'terminal-only':'c103','successor':'c104','postclose-audit':'c105'})
    def test_postclose_failure_never_appends_a_normal_registration(self):
        import managed_metadata_v4 as audit
        with patch.object(audit,'runtime_case',side_effect=ValueError('original Guest FAIL')),patch.object(Path,'mkdir',side_effect=AssertionError('no output or registry before validation')):
            with self.assertRaisesRegex(ValueError,'original Guest FAIL'):audit.complete({}, {}, {'metadata_producer':type('P',(),{'ev':None,'md':None})()}, {},Path('/missing'))
    def test_fixed_guard_closeout_is_an_independent_compiled_anchor(self):
        import accepted_c101_v4 as inputs
        root=Path(inputs.__file__).parents[2]/'examples/mem_service_pool/accepted_c101_v4'
        original=(root/'anchors.json').read_bytes();closeout=(root/'host-closeout.json').read_bytes()
        a=json.loads(original);c=inputs.v4_host_closeout(a,closeout)
        self.assertEqual(c['pending'],[]);self.assertFalse(c['new_terminal_claim_Guest_authorized'])
        for raw in [closeout+b' ',original]:
            with self.assertRaises(ValueError):inputs.v4_host_closeout(a,raw)
        a['future_W_ELF']['sha256']='0'*64
        with self.assertRaises(ValueError):inputs.v4_host_closeout(a,closeout)
    def test_old_R_context_and_forged_readiness_cannot_construct_W_or_owning_routes(self):
        from managed_gate_v3 import _RuntimeAdmission as Old, _RUNTIME as OLD_SEAL
        from managed_write_control import production_controller
        from managed_write_interface_v1 import production_binding
        from managed_native_routes_v4 import production_routes
        for admission in [None,{'ready':True},Old({}, {}, {}, {}, OLD_SEAL)]:
            with self.assertRaisesRegex(ValueError,'production-not-ready'):production_controller(admission=admission)
            with self.assertRaisesRegex(ValueError,'production-not-ready'):production_binding(object(),object(),object(),admission=admission)
            with self.assertRaisesRegex(ValueError,'production-not-ready'):production_routes(admission,None,None,None)
if __name__=='__main__':unittest.main()
