"""Preparation is data, never permission; missing context fails before IO."""
import sys,unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).parents[2]/'tools/phasec'))
from pool_execution_draft import prepare,resolve_production
class DraftTests(unittest.TestCase):
    def test_actual_image_receipt_builds_explicit_unresolved_plan(self):
        r={'kind':'CProductionImageHostBuildV1','pass':True,**{k:'pin-or-own-path' for k in ['image','image_sha256','agent_sha256','launcher_sha256','SDK_manifest_sha256','worker_sha256','daemon','daemon_sha256']}}
        v=prepare(r);self.assertFalse(v['executable']);self.assertFalse(v['production_ready']);self.assertTrue(all(x is None for x in v['unresolved'].values()));self.assertEqual(v['unchanged_guards']['verification_passes'],6);self.assertEqual(v['prospective_objects']['bootstrap'],[119,120])
    def test_no_caller_ready_boolean_hash_or_file_can_resolve_context(self):
        with patch.object(Path,'read_bytes',side_effect=AssertionError('must not read')):
            for value in [True,{'ready':True,'sha256':'0'*64},Path('/original/must-not-read')]:
                with self.assertRaisesRegex(ValueError,'production-not-ready'):resolve_production(value)
if __name__=='__main__':unittest.main()
