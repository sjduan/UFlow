import sys,unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).parents[2]/'tools/phasec'))
from pool_c101_execution_draft import prepare,resolve_production,CONSUMED

class CurrentPreparationTests(unittest.TestCase):
    def receipt(self):return {'kind':'CExplicitWAuthorityHostV1','pass':True,'production_W_enabled':False,'source_commit':'HOST-new-source','source_manifest_sha256':'11'*32,'daemon_sha256':'22'*32}
    def test_current_history_and_future_graph_are_distinct_nonexecutable_data(self):
        with patch.object(Path,'open',side_effect=AssertionError('preparation must not open authority')):
            v=prepare(self.receipt())
        self.assertEqual(v['current']['eligible_object_ids'],[119,120,121]);self.assertEqual(v['prospective_objects']['bootstrap'],[122,123]);self.assertEqual(v['prospective_objects']['business'],124)
        self.assertEqual(len(v['consumed_ancestors']),3);self.assertEqual(v['consumed_ancestors'][-1]['claim_sha256'],CONSUMED[-1]['claim_sha256'])
        self.assertEqual(v['observed_failed_domain']['source_generation'],'071');self.assertEqual(v['future_repaired_domain']['C_source_commit'],'HOST-new-source');self.assertIsNone(v['future_repaired_domain']['accepted_E_current_c101_context'])
        self.assertFalse(v['executable']);self.assertFalse(v['production_ready']);self.assertFalse(v['terminal_or_claim_or_Guest_executed'])
        v['consumed_ancestors'].clear();self.assertEqual(len(CONSUMED),3)
    def test_early_production_gate_does_not_read_caller_or_original_paths(self):
        with patch.object(Path,'open',side_effect=AssertionError('no original IO')):
            with self.assertRaisesRegex(ValueError,'production-not-ready'):resolve_production('/missing',ready=True,sha256='00'*32)
        for key,value in [('pass',False),('production_W_enabled',True),('kind','arbitrary')]:
            r=self.receipt();r[key]=value
            with self.assertRaises(ValueError):prepare(r)

if __name__=='__main__':unittest.main()
