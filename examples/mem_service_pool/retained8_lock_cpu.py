"""Actual isolated Linux flock/fdinfo proof; never open E0/real lineage locks."""
import argparse,contextlib,fcntl,json,os
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).parents[2]/'tools/phasec'))
from verify_retained8_pool import LOCK_NAMES,ROOT,admission_record,birth,held_file_lock

def main():
 p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);a=p.parse_args()
 if 'UFLOW_ENV_CLOSE_FD' not in os.environ:raise RuntimeError('admitted isolated B build required')
 a.output.mkdir();owner=os.getpid();stamp=birth(owner);records=[]
 with contextlib.ExitStack() as stack:
  files=[]
  for name in LOCK_NAMES:
   path=a.output/name;f=stack.enter_context(path.open('x+b'));fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB);files.append(f)
   row={'pid':owner,'birth':stamp,'job':'c900','kind':'guest','state':'held'}
   f.write(json.dumps(row).encode());f.flush();records.append(row)
   assert held_file_lock(owner,path),'actual owned FLOCK absent from canonical FD fdinfo'
  admission_record(records,{owner:stamp},ROOT/'runs/c900/state')
  fcntl.flock(files[0],fcntl.LOCK_UN);assert not held_file_lock(owner,a.output/LOCK_NAMES[0])
  fcntl.flock(files[0],fcntl.LOCK_EX|fcntl.LOCK_NB)
  with (a.output/LOCK_NAMES[0]).open('rb') as second:
   try:fcntl.flock(second,fcntl.LOCK_EX|fcntl.LOCK_NB)
   except BlockingIOError:pass
   else:raise AssertionError('independent flock acquired while owner still held it')
 result={'pass':True,'actual_kernel_flock_fdinfo_and_canonical_inode':True,'exclusive_three_fixture_locks':True,
  'released_lock_no_longer_proves_admission':True,'parallel_fd_rejected':True,'real_coordination_or_lineage_opened':False,
  'production_guest_admission_run':False,'fixture_authority_is_semantic_only':True}
 (a.output/'results.json').write_text(json.dumps(result,indent=2));print(json.dumps(result))
if __name__=='__main__':main()
