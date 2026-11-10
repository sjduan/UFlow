"""New current case cannot reach old producer/lower/authority via flags."""
import os,sys,unittest
from pathlib import Path
from unittest.mock import patch
sys.path[:0]=[str(Path(__file__).parents[2]/'tools/phasec'),str(Path(__file__).parents[2]/'sdk/python')]
import run_mem_service_pool_matrix as driver
from pool_access_cost_model import model
class ChainGateTests(unittest.TestCase):
    def test_future_case_rejected_before_old_producer_or_lower_launch(self):
        with patch.dict(os.environ,{'UFLOW_ENV_CLOSE_FD':'5'}),patch.object(Path,'is_dir',return_value=True),patch.object(driver,'launch_gate',side_effect=AssertionError('no producer IO')):
            with self.assertRaisesRegex(ValueError,'production-not-ready'):driver.matrix(Path('/cpu'),{'output':'/cpu/runs/1','terminal_case_execution':{'ready':True}},{'source':'/cpu/source'})
    def test_unchanged_SDK_alone_exceeds_deadline_even_if_callback_cost_is_zero(self):
        v=model(20<<20);self.assertEqual(v['minimum_scalar_read_calls'],4*(20<<20));self.assertGreater(v['unchanged_SDK_two_pass_floor_seconds'],2000)
        self.assertGreater(v['C_only_eight_byte_callback_loads_still_scalar_SDK_estimated_seconds'],120)
        self.assertFalse(v['isolated_benchmark']);self.assertFalse(v['SDK_or_visibility_hash_byte_guards_removed'])
if __name__=='__main__':unittest.main()
