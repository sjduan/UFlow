"""C trust-boundary tests using pinned metadata, never a production permit."""
import copy
import hashlib
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).parents[2]/'tools/phasec'))
sys.path.insert(0,str(Path(__file__).parents[2]/'sdk/python'))
import accepted_producers as ap
import terminal_current_adapter as adapter
import qemu_profile_gate as qemu

class AcceptedProducerTests(unittest.TestCase):
    def setUp(self):
        self.pins=ap.accepted_anchors()
        self.recorded=json.loads((Path(__file__).parent/'current_terminal_production/accepted-terminal-candidate.json').read_bytes())
    def test_raw_external_anchors_and_whole_actual_typed_result(self):
        a=self.pins['terminal'];self.assertIs(ap.ap_check_terminal_result(self.recorded,a,self.recorded),self.recorded)
        raw=ap.ap_canonical(self.pins)
        with self.assertRaisesRegex(ValueError,'anchor bytes'):ap.accepted_anchors(raw)
    def test_any_current_backing_protected_history_or_false_fact_change_refused(self):
        mutations=[('status','PASS'),('assessment_sha256','0'*64),('protected_historical_bytes',0),
            ('current_eligible_capacity_bytes',0),('journal_A_complete',True),('normal_native_or_SDK_release_ACK',True),
            ('authority_to_mutate_ledger',True),('eligible_rows',[]),('prior_chain',{}),('source_runtime_closure',{}),
            ('physical_closure',{}),('v1_metadata_verdict','PASS')]
        for key,value in mutations:
            changed=copy.deepcopy(self.recorded);changed[key]=value
            with self.subTest(key=key),self.assertRaises(ValueError):ap.ap_check_terminal_result(changed,self.pins['terminal'],self.recorded)
    def test_all_module_bytes_and_modes_precede_first_source_exec(self):
        rows={'uflow_fixture_a':{'path':'/cpu/a'},'uflow_fixture_b':{'path':'/cpu/b'}};modes={k:'0755' for k in rows};calls=[]
        def read(row,limit,mode):
            calls.append((row['path'],mode))
            if row['path']=='/cpu/b':raise ValueError('bad second dependency')
            return b'raise AssertionError("must not execute first source")'
        with patch.object(ap,'ap_bound',side_effect=read),self.assertRaisesRegex(ValueError,'second dependency'):
            with ap.ap_fresh_modules(rows,modes,tuple(rows)):raise AssertionError('not yielded')
        self.assertEqual(calls,[('/cpu/a',0o755),('/cpu/b',0o755)])
        self.assertNotIn('uflow_fixture_a',sys.modules)
    def test_cached_modules_are_replaced_fresh_and_restored(self):
        old=types.ModuleType('uflow_fixture');old.value='stale';row={'path':'/cpu/source'}
        with patch.dict(sys.modules,{'uflow_fixture':old}),patch.object(ap,'ap_bound',return_value=b'value="fresh"'):
            with ap.ap_fresh_modules({'uflow_fixture':row},{'uflow_fixture':'0755'},('uflow_fixture',)) as modules:
                self.assertEqual(modules['uflow_fixture'].value,'fresh');self.assertIsNot(modules['uflow_fixture'],old)
            self.assertIs(sys.modules['uflow_fixture'],old)
    def test_wrong_execution_namespace_rejects_before_file_or_proc_reads(self):
        with patch.object(adapter,'ap_bound',side_effect=AssertionError('no file IO')):
            with self.assertRaisesRegex(ValueError,'execution namespace'):adapter.current_execution_context({'execution_manifest':{'path':'/tmp/fixture/execution.json'}},self.pins)
    def test_actual_nested_qemu_closure_shapes_and_exact_selection(self):
        anchor=self.pins['profile'];expected=anchor['expected_actual_result']
        refs=[row for key in ('source_closure','object_closure','link_closure') for row in qemu._closure_refs(expected[key])]
        self.assertEqual(len(refs),12)
        self.assertTrue(all(set(row)=={'path','bytes','sha256'} for row in refs))
        self.assertEqual(qemu.binding(),qemu._selection(anchor))
        with patch.object(qemu,'ap_bound',side_effect=AssertionError('unreviewed selection must not read')):
            with self.assertRaisesRegex(ValueError,'exact accepted'):qemu.launch_qemu_gate(qemu.ROOT,{'qemu_profile':{'ready':True},'qemu_binary':anchor['binary']['path']})
    def test_current_successor_cannot_fall_back_to_legacy_before_launch_gate(self):
        import os
        import run_mem_service_pool_matrix as driver
        config={'output':'/cpu/runs/1','terminal_current_execution':{'path':'CPU fixture only'}}
        with patch.dict(os.environ,{'UFLOW_ENV_CLOSE_FD':'5'}),patch.object(Path,'is_dir',return_value=True), \
             patch.object(driver,'launch_gate',side_effect=AssertionError('must reject before producer/launch IO')):
            with self.assertRaisesRegex(ValueError,'no legacy fallback'):driver.matrix(Path('/cpu'),config,{'source':'/cpu/source'})

if __name__=='__main__':unittest.main()
