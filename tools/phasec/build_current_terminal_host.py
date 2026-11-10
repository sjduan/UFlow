"""Only affected Rust daemon/tests; reuse unchanged accepted C/image/SDK closure."""
import argparse,hashlib,json,os,shutil,subprocess,sys
from pathlib import Path
ROOT='/workspace/uflow/runs/mem-service-shared-pool-20261108'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main(root,source,name):
 assert str(root)==ROOT and 'UFLOW_ENV_CLOSE_FD' in os.environ
 assert name=='build-host049';out=root/'implementation'/name;out.mkdir(exist_ok=False)
 before=root/'implementation/source-host048';old=json.loads((before/'.uflow-source.json').read_bytes());current=json.loads((source/'.uflow-source.json').read_bytes())
 assert old['commit']=='6403bdafc8d2cd6708ed5a21edcdd1a1b91624ca'
 changes={'crates/uf-daemon/src/main.rs','crates/uf-daemon/src/pool_terminal_domain.rs','crates/uf-daemon/src/ddr_resources.rs',
  'crates/uf-daemon/src/mem_service_pool.rs','tools/phasec/mem_service_pool_bridge.py','examples/mem_service_pool/test_guest_panic.py','examples/mem_service_pool/matrix_entry_cpu.py'}
 changed=set()
 for rel,row in current['files'].items():
  p=source/rel;assert not p.is_symlink() and sha(p)==row['sha256'] and p.stat().st_mode&0o777==int(row['mode'][-3:],8)
  if rel in old['files']:
   assert sha(before/rel)==old['files'][rel]['sha256']
   if row!=old['files'][rel]:changed.add(rel)
  else:assert rel.startswith(('examples/mem_service_pool/current_terminal_host/','crates/uf-daemon/src/pool_terminal_domain/current')) or rel=='tools/phasec/build_current_terminal_host.py'
 assert changed==changes
 base=root/'implementation/build-host047';cbase=root/'implementation/build-host046';receipt=json.loads((base/'build-receipt.json').read_bytes())
 assert sha(base/'build-receipt.json')=='08f61e09795cf1aaf7a933d223c1f05ce3bf47cd4c43091b446fb3c3bc5bc09a'
 for key,pin in [('image','image_sha256'),('fence_shim','fence_shim_sha256'),('daemon','daemon_sha256')]:assert sha(Path(receipt[key]))==receipt[pin]
 for path,row in receipt['official_worker']['source_and_headers_link_inputs'].items():assert sha(Path(path))==row['sha256']
 for rel,pin in receipt['installed_sdk_manifest'].items():assert sha(root/'env/mem-service-sdk'/rel)==pin
 assert len(receipt['installed_sdk_manifest'])==53 and len(receipt['official_worker']['source_and_headers_link_inputs'])==233
 binaries=[cbase/'cpu-agent',cbase/'cpu-worker',cbase/'pool-agent',Path(receipt['image']),Path(receipt['fence_shim']),Path(receipt['daemon']),Path(receipt['official_worker']['ELF'])]
 pins={str(p):sha(p) for p in binaries};state=root/'runs/c064/state/state.json';state_pin=sha(state)
 assert state_pin=='163f7500836dc4fd199d34f83a0e08453ab05255f8b15a57338052df4601d5b0'
 cargo=shutil.which('cargo') or '/root/.cargo/bin/cargo';target=root/'implementation/cargo-host049';assert not target.exists()
 env={**os.environ,'PATH':str(Path(cargo).parent)+os.pathsep+os.environ.get('PATH',''),'CARGO_INCREMENTAL':'0','CARGO_BUILD_JOBS':'4',
  'UF_ACL_SHIM_LIB_DIR':str(cbase/'fence'),'LD_LIBRARY_PATH':str(cbase/'fence'),'PYTHONPATH':str(source/'sdk/python'),'PYTHONDONTWRITEBYTECODE':'1'}
 def run(argv,label,timeout=600):
  with (out/(label+'.log')).open('xb') as log:subprocess.run(list(map(str,argv)),cwd=source,env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=timeout)
 run([cargo,'build','--locked','-p','uf-daemon','--target-dir',target,'-j','4'],'affected-daemon-build',1200)
 launch=out/'uf-daemon'
 with (target/'debug/uf-daemon').open('rb') as src,launch.open('xb') as dst:shutil.copyfileobj(src,dst);dst.flush();os.fsync(dst.fileno())
 launch.chmod(0o755);assert launch.stat().st_nlink==1
 run([cargo,'test','--locked','-p','uf-daemon','--target-dir',target,'-j','4','--no-run','--message-format=json'],'test-artifacts',1200)
 rows=[json.loads(line) for line in (out/'test-artifacts.log').read_text().splitlines() if line.startswith('{')]
 tests=[Path(v['executable']) for v in rows if v.get('reason')=='compiler-artifact' and v.get('target',{}).get('name')=='uf-daemon' and v.get('profile',{}).get('test') and v.get('executable')];assert len(tests)==1;rust=tests[0]
 run([rust,'pool_terminal_domain::','--nocapture','--test-threads=4'],'affected-terminal-tests')
 run([sys.executable,'-B','-m','unittest','discover','-s',source/'examples/mem_service_pool','-p','test_guest_panic.py'],'blocked-send-and-panic-tests')
 # Bare production entries always fail closed, including arbitrary descriptors.
 for index,argv in enumerate([[launch,'--pool-terminal-domain-loss'],[launch,'--pool-terminal-current-manifest',root/'must-not-read-case',sha(source/'.uflow-source.json')]]):
  r=subprocess.run(list(map(str,argv)),env=env,capture_output=True,timeout=10);assert r.returncode==1 and b'production-not-ready' in r.stderr
  (out/('production-entry-%d.json'%index)).write_text(json.dumps({'actual_exit':r.returncode,'stderr':r.stderr.decode(),'original_IO':False},indent=2))
 for panic,short,label in [(False,'v049','current-normal-matrix'),(True,'w049','current-panic-Unknown-matrix')]:
  run([sys.executable,'-B',source/'examples/mem_service_pool/matrix_entry_cpu.py','--current-terminal-domain',*(['--panic-on-first-write'] if panic else []),
   '--agent',cbase/'cpu-agent','--worker',cbase/'cpu-worker','--controller',root/'env/mem-service-sdk/bin/linqu_mem_service',
   '--scratch',root/short,'--output',out/label,'--rust-tests',rust,'--source',source,'--image',cbase/'pool-agent','--shim',cbase/'fence/libuf_acl_shim.so'],label)
 assert sha(state)==state_pin
 for path,pin in pins.items():assert sha(Path(path))==pin
 result={'schema':1,'pass':True,'source_commit':current['commit'],'source_manifest_sha256':sha(source/'.uflow-source.json'),
  'compiled_only':'affected uf-daemon and terminal Rust tests','runtime_ELF':str(launch),'runtime_ELF_sha256':sha(launch),'runtime_ELF_nlink':1,
  'Rust_test_ELF':str(rust),'Rust_test_ELF_sha256':sha(rust),'C_image_SDK_worker_reused_unchanged':pins,
  'SDK53_worker233_unchanged':True,'production_v2_ready':False,'accepted_E_v2_pins':None,
  'real_c064_state_sha256_before_after':state_pin,'new_Guest_or_original_ledger_mutation':False,
  'normal_current_CPU_matrix':json.loads((out/'current-normal-matrix/results.json').read_bytes()),
  'panic_current_CPU_matrix':json.loads((out/'current-panic-Unknown-matrix/results.json').read_bytes())}
 (out/'validation-receipt.json').write_text(json.dumps(result,indent=2));print(json.dumps(result))
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--source',type=Path,required=True);p.add_argument('--output',required=True);a=p.parse_args();main(a.root,a.source,a.output)
