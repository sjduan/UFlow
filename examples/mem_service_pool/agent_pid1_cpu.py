"""Actual new agent + PID1 logic + SDK worker drain, explicit CPU platform.
The journal bytes are a labelled metadata fixture, not an official Guest journal.
"""
import argparse,ctypes,hashlib,json,os,sys,time
from pathlib import Path
from startup_cpu_probe import Harness,birth
from pool_journal_capture import capture
def main():
 p=argparse.ArgumentParser()
 for n in ['agent','worker','launcher','controller','scratch','output']:p.add_argument('--'+n,type=Path,required=True)
 a=p.parse_args();assert 'UFLOW_ENV_CLOSE_FD' in os.environ and ctypes.CDLL(None).prctl(36,1,0,0,0)==0
 a.scratch.mkdir();a.output.mkdir();h=Harness(a,0);spawn=h.spawn;owned={};fixture=b'HOST synthetic journal metadata; not a Guest SDK journal\n'
 def launched(argv,**kwargs):
  if Path(argv[0])==a.agent:
   name=str(argv[2]);cwd=h.path/('pid1-'+name);cwd.mkdir();(cwd/'worker.conf.state').write_bytes(fixture)
   kwargs['cwd']=cwd;argv=[a.launcher,'100','500','150',*argv]
   child=spawn(argv,**kwargs);owned[name]=child;return child
  return spawn(argv,**kwargs)
 h.spawn=launched
 try:
  startup=h.start();startup.run();startup.stop_infrastructure()
  for node in h.nodes.values():node.prepare_close()
  summaries={}
  for name,node in h.nodes.items():
   node.send('quit');deadline=time.monotonic()+4
   while True:
    try:node.channel.line(deadline)
    except RuntimeError as error:
     if 'EOF' not in str(error):raise
     break
   child=owned[name];assert child.wait(timeout=1)==0
   raw=(h.output/(name+'.serial.log')).read_bytes();summary,reconstructed=capture(raw.decode());assert summary['complete'] and reconstructed==fixture
   assert raw.count(b'UFLOW_ENV_DAEMON_CLOSED\n')==1 and b'UFPOOL_FAILURE_NOTICE' not in raw
   assert raw.index(b'UFPOOL_WORKERS_CLOSED')<raw.index(b'UFPOOL_JOURNAL_BEGIN')<raw.index(b'UFPOOL_JOURNAL_END')<raw.index(b'UFPOOL_LAUNCHER_CLOSED')<raw.index(b'UFLOW_ENV_DAEMON_CLOSED')
   assert birth(child.pid) is None
   summaries[name]={'actual_launcher_exit':0,'actual_agent_worker_SDK_drain':True,'journal_fixture_sha256':hashlib.sha256(fixture).hexdigest(),'journal':summary,'serial_sha256':hashlib.sha256(raw).hexdigest()}
  result={'pass':True,'actual_new_agent_PID1_logic_and_neutral_SDK':True,'CPU_worker_platform_and_journal_are_explicit_fixtures':True,'actual_Guest_official_worker_journal_or_QMP':False,'nodes':summaries}
  (a.output/'results.json').write_text(json.dumps(result,indent=2));print(json.dumps(result))
 finally:h.close()
if __name__=='__main__':main()
