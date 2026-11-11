"""Exact final E type/call shapes; no accepted context or original authority."""
import copy,json,sys,types,unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).parents[2]/'tools/phasec'))
from managed_interface_v2 import HostTypedAPIs,production_binding,_host_wrapper_snapshot
from managed_cache_control import strict_reply,codec
import test_managed_cache_control as control_fixture

class InterfaceTests(unittest.TestCase):
    def setUp(self):self.f=control_fixture.ControlTests();self.f.setUp()
    def test_actual_native_aliases_and_all_counters_use_exact_final_types(self):
        self.f.execute();reply=self.f.controller.audit[-1]['reply'];c=self.f.c
        self.assertEqual(set(reply['native_alias']),{*c.FIELDS64,*c.FIELDS32,'descriptor_sha256'})
        self.assertTrue(all(type(reply['native_alias'][k]) is str for k in c.FIELDS64));self.assertTrue(all(type(reply['native_alias'][k]) is int for k in c.FIELDS32))
        self.assertEqual(set(reply['counters']),{*c.COUNTERS,'saturated'});self.assertTrue(all(type(reply['counters'][k]) is str for k in c.COUNTERS));self.assertIs(type(reply['counters']['saturated']),bool)
    def test_old_HOST_alias_shape_noncanonical_text_counter_types_and_scope_reject(self):
        for alter in [lambda r:r['native_alias'].update(map_id=102),lambda r:r['native_alias'].update(offset='065536'),lambda r:r['native_alias'].update(length=20971520),
                      lambda r:r['native_alias'].update(home_cna=True),lambda r:r['counters'].update(cache_hits=0),lambda r:r['counters'].update(cache_hits='18446744073709551616'),
                      lambda r:r['counters'].update(saturated=0),lambda r:r.update(scope='unmanaged'),lambda r:r['counters'].pop('remote_failures')]:
            self.f.setUp();self.f.bad=lambda op,r:alter(r)
            with self.assertRaises(ValueError):self.f.execute()
            self.assertNotIn('SDK-six-passes-unchanged-seam',self.f.events)
    def test_actual_wrapper_source_snapshot_exposes_one_wire_dict_API(self):
        wrapper=_host_wrapper_snapshot();seen=[]
        class Q:
            def managed_cache(self,wire):seen.append(wire);return {'source_defined_dict_return':True}
        serial=wrapper.ManagedSerialChannel.__new__(wrapper.ManagedSerialChannel);serial._managed_qmp=Q()
        self.assertEqual(serial.managed_cache('bounded-metadata-wire'),{'source_defined_dict_return':True});self.assertEqual(seen,['bounded-metadata-wire'])
    def test_terminal_metadata_and_ownership_argument_order_and_companion_alias(self):
        seen=[]
        def evaluate(context,external_sha):seen.append(('evaluate',context,external_sha));return 'host-only'
        def validate_candidate(candidate,external_sha):return None
        def registration(request,sha,helper,context,context_sha):seen.append(('registration',request,sha,helper,context,context_sha));return 'host-only'
        def append(registry,head,entry):return None
        def history(registry,head):return None
        def union(plan,sha,context,context_sha):return None
        def owned(descriptor,run,helper,starts=None,capture=None):return None
        terminal=types.SimpleNamespace(evaluate=evaluate,validate_candidate=validate_candidate)
        metadata=types.SimpleNamespace(validate_registration=registration,append_registration=append,registry_history=history,validate_closed_union=union,companion_audit=union)
        apis=HostTypedAPIs(terminal,metadata,types.SimpleNamespace(validate_descriptor=owned));self.assertEqual(apis._host_dispatch('evaluate','context','external'),'host-only')
        self.assertEqual(apis._host_dispatch('validate_registration','request','sha','helper','context','context-sha'),'host-only');self.assertEqual(seen[-1],('registration','request','sha','helper','context','context-sha'))
        with self.assertRaises(ValueError):apis._host_dispatch('validate_registration','request','sha')
        apis.source_only=True
        with self.assertRaisesRegex(ValueError,'source-only'):apis._host_dispatch('evaluate','must-not-read','0'*64)
    def test_production_factory_refuses_even_valid_proposal_before_source_or_original_IO(self):
        with patch.object(Path,'read_bytes',side_effect=AssertionError('no source or original IO')):
            for value in [{'manager_accepted':True},'7cfbdc76f1ab8d696ae3349b6bf5510d5b8e6b4f047f50ae90e579b72c4e62db']:
                with self.assertRaisesRegex(ValueError,'production-not-ready'):production_binding(value)
if __name__=='__main__':unittest.main()
