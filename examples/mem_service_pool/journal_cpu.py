"""Actual Linux child/launcher/file tests; metadata fixture, no Guest or SDK."""
import argparse,hashlib,json,os,subprocess,sys,time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'tools/phasec'))
from pool_journal_capture import capture,MAX_BYTES
def main():
 p=argparse.ArgumentParser();p.add_argument('--launcher',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
 if 'UFLOW_ENV_CLOSE_FD' not in os.environ:raise RuntimeError('admitted build required')
 a.output.mkdir();results=[]
 for name in ['normal','failure','missing','symlink','fifo','hardlink','oversized','blocked-serial']:
  cwd=a.output/name;cwd.mkdir();path=cwd/'worker.conf.state';raw=b'CPU-METADATA-JOURNAL\nsegment=123\nphase=export-unknown\n'
  if name=='symlink':(cwd/'secret-fixture').write_bytes(b'NEVER-READ');path.symlink_to(cwd/'secret-fixture')
  elif name=='fifo':os.mkfifo(path)
  elif name=='hardlink':path.write_bytes(raw);os.link(path,cwd/'alias')
  elif name not in ['missing']:
   if name in ['oversized','blocked-serial']:raw=b'M'*(MAX_BYTES+99)
   path.write_bytes(raw)
  code="raise SystemExit("+('0' if name=='normal' else '7')+")"
  begin=time.monotonic();child=subprocess.Popen([str(a.launcher),'100','300','200',sys.executable,'-c',code],cwd=cwd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
  if name=='blocked-serial':child.wait(timeout=3)
  stdout,stderr=child.communicate(timeout=3);elapsed=time.monotonic()-begin
  (cwd/'serial.log').write_bytes(stdout);(cwd/'stderr.log').write_bytes(stderr)
  info,reconstructed=capture(stdout.decode());assert child.returncode==(0 if name=='normal' else 1) and elapsed<3
  if name!='normal':assert stdout.index(b'UFPOOL_FAILURE_NOTICE')<stdout.index(b'UFPOOL_JOURNAL_')
  if name!='blocked-serial':assert b'logical_ACK_inferred=0' in stdout
  if name in ['normal','failure']:assert info['complete'] and reconstructed==raw and info['sha256']==hashlib.sha256(raw).hexdigest()
  elif name=='oversized':assert not info['complete'] and info['status']=='truncated' and reconstructed==raw[:MAX_BYTES]
  elif name=='blocked-serial':assert not info['complete']
  else:assert not info['complete'] and not reconstructed
  if name=='symlink':assert b'NEVER-READ' not in stdout
  results.append({'case':name,'pass':True,'exit':child.returncode,'elapsed':elapsed,'capture':info})
 result={'pass':True,'fixed_known_metadata_file_only':True,'notice_before_diagnosis':True,'deadline_not_extended':True,'actual_Linux_children_and_files':True,'cases':results}
 (a.output/'results.json').write_text(json.dumps(result,indent=2));print(json.dumps(result))
if __name__=='__main__':main()
