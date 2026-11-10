"""Synthetic pure observations cannot substitute a production E context."""
import copy,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parents[2]/'tools/phasec'))
from result_ownership import validate_observation
class OwnershipTests(unittest.TestCase):
    def setUp(self):
        self.monitor={'actual_task_exit_code':1,'process_tree_empty':True,'task_resources_closed':True,'remaining':{},'evidence_errors':[],'isolation_error':None,
            'owned_births':{'10':100,'11':110,'12':120},'supervisor_pid':9,'supervisor_birth':90,'task_pid':10,'task_birth':100}
        self.owner={'pid':10,'birth':100};self.starts=[{'node':'nodeA','pid':11,'birth':110},{'node':'nodeB','pid':12,'birth':120}]
    def validate(self,m=None,t=None):
        m=self.monitor if m is None else m;t={'processes':{**m,'supervisor_actual_exit_code':1}} if t is None else t
        return validate_observation(t,m,self.owner,self.starts)
    def test_truthful_failed_exit_and_actual_birth_union_remain_distinct_from_ACK(self):
        v=self.validate();self.assertEqual(v['actual_task_exit_code'],1);self.assertEqual(len(v['observed_births']),4)
        self.assertFalse(v['ownership_history_reconstructed']);self.assertFalse(v['SDK_ACK_or_normal_release_inferred'])
    def test_top_monitor_disagreement_refused(self):
        top={'processes':{**self.monitor,'actual_task_exit_code':0,'supervisor_actual_exit_code':0}}
        with self.assertRaisesRegex(ValueError,'disagree'):self.validate(t=top)
    def test_unobserved_owner_or_replaced_birth_refused(self):
        m=copy.deepcopy(self.monitor);m['owned_births']['12']=999
        with self.assertRaisesRegex(ValueError,'outside real'):self.validate(m=m)
    def test_nonclosed_or_untyped_monitor_cannot_become_observation(self):
        for key,value in [('task_resources_closed',False),('process_tree_empty',1),('remaining',{'1':'live'}),('isolation_error','denied')]:
            m=copy.deepcopy(self.monitor);m[key]=value
            with self.subTest(key=key),self.assertRaisesRegex(ValueError,'physical closure'):self.validate(m=m)
if __name__=='__main__':unittest.main()
