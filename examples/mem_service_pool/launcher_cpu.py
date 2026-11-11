"""Actual task launcher/process-group/wait test, no namespaces/Guest/device."""
import argparse,json,os,signal,subprocess,sys,time
from pathlib import Path

def main():
 p=argparse.ArgumentParser();p.add_argument('--launcher',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
 if 'UFLOW_ENV_CLOSE_FD' not in os.environ:raise RuntimeError('admitted build required')
 a.output.mkdir();results=[]
 cases={
  'normal':"print('actual-agent-normal',flush=True)",
  'exit7':"print('UFPOOL_FAILURE_NOTICE',flush=True);raise SystemExit(7)",
  'signal':"import os,signal;os.kill(os.getpid(),signal.SIGTERM)",
  'descendant-ignore-term':"import os,signal,time;pid=os.fork();\nif not pid:\n signal.signal(signal.SIGTERM,signal.SIG_IGN);print('owned-grandchild='+str(os.getpid()),flush=True);time.sleep(10)\nelse:\n time.sleep(.05);raise SystemExit(9)",
 }
 for name,code in cases.items():
  start=time.monotonic();result=subprocess.run([a.launcher,'100','500','150',sys.executable,'-c',code],
       stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=4)
  elapsed=time.monotonic()-start;text=result.stdout.decode();(a.output/(name+'.stdout')).write_bytes(result.stdout)
  (a.output/(name+'.stderr')).write_bytes(result.stderr)
  assert result.returncode==(0 if name=='normal' else 1) and 'children_empty=1 logical_ACK_inferred=0' in text
  assert elapsed<3 and ('UFPOOL_FAILURE_NOTICE' in text)==(name!='normal')
  if name!='normal':assert text.index('UFPOOL_FAILURE_NOTICE')<text.index('UFPOOL_LAUNCHER_CLOSED')
  if 'owned-grandchild=' in text:
   pid=int(text.split('owned-grandchild=')[1].splitlines()[0]);assert not Path('/proc',str(pid)).exists()
  results.append({'case':name,'pass':True,'exit':result.returncode,'elapsed_seconds':elapsed})
 value={'pass':True,'actual_process_groups_and_subreaper':True,'cases':results,
        'host_fixture_not_real_PID1_or_QMP':True,'finite_handoff_then_exit_verified':True}
 (a.output/'results.json').write_text(json.dumps(value,indent=2));print(json.dumps(value))
if __name__=='__main__':main()
