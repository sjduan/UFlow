"""Only affected Rust daemon/tests; reuse unchanged accepted C/image/SDK closure."""
import argparse,hashlib,json,os,shutil,subprocess,sys
from pathlib import Path
ROOT='/workspace/uflow/runs/mem-service-shared-pool-20261108'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main(root,source,name):
 assert str(root)==ROOT and 'UFLOW_ENV_CLOSE_FD' in os.environ
 assert name in ('build-host049','build-host050','build-host051','build-host052');resume=name!='build-host049';dispatch_fix=name in ('build-host051','build-host052');runtime_fix=name=='build-host052';out=root/'implementation'/name;out.mkdir(exist_ok=False)
 before=root/'implementation/source-host048';old=json.loads((before/'.uflow-source.json').read_bytes());current=json.loads((source/'.uflow-source.json').read_bytes())
 assert old['commit']=='6403bdafc8d2cd6708ed5a21edcdd1a1b91624ca'
 changes={'crates/uf-daemon/src/main.rs','crates/uf-daemon/src/pool_terminal_domain.rs','crates/uf-daemon/src/ddr_resources.rs',
  'crates/uf-daemon/src/mem_service_pool.rs','tools/phasec/mem_service_pool_bridge.py','examples/mem_service_pool/test_guest_panic.py','examples/mem_service_pool/matrix_entry_cpu.py'}
 changed=set()
 if runtime_fix:changes.add('tools/phasec/run_mem_service_pool_matrix.py')
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
 compile_source=source
 if resume:
  job=json.loads((root/'runs/cb070/result.json').read_bytes())['processes'];assert job['actual_task_exit_code']==1 and job['process_tree_empty'] and job['task_resources_closed'] and not job['remaining'] and not job['evidence_errors']
  compile_source=root/'implementation/source-host049';compiled=json.loads((compile_source/'.uflow-source.json').read_bytes())
  assert compiled['commit']=='ac08d5dcd0230112efd5a0322f0c829b471747f4' and set(compiled['files'])==set(current['files'])
  for rel,row in compiled['files'].items():
   assert sha(compile_source/rel)==row['sha256']
   if dispatch_fix and rel=='crates/uf-daemon/src/main.rs':
    old_main=(compile_source/rel).read_bytes();new_main=(source/rel).read_bytes()
    marker=b'v=="--pool-terminal-execution-check") {'
    assert old_main.count(marker)==1 and new_main==old_main.replace(marker,b'v=="--pool-terminal-execution-check"||v=="--pool-terminal-current-manifest") {')
   elif runtime_fix and rel=='tools/phasec/run_mem_service_pool_matrix.py':pass
   elif rel!='tools/phasec/build_current_terminal_host.py':assert current['files'][rel]==row
  if dispatch_fix and not runtime_fix:compile_source=source
 cargo=shutil.which('cargo') or '/root/.cargo/bin/cargo';target=root/'implementation/cargo-host049';assert target.exists()==resume
 env={**os.environ,'PATH':str(Path(cargo).parent)+os.pathsep+os.environ.get('PATH',''),'CARGO_INCREMENTAL':'0','CARGO_BUILD_JOBS':'4',
  'UF_ACL_SHIM_LIB_DIR':str(cbase/'fence'),'LD_LIBRARY_PATH':str(cbase/'fence'),'PYTHONPATH':str(source/'sdk/python'),'PYTHONDONTWRITEBYTECODE':'1'}
 def run(argv,label,timeout=600):
  with (out/(label+'.log')).open('xb') as log:subprocess.run(list(map(str,argv)),cwd=compile_source,env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=timeout)
 if not resume or (dispatch_fix and not runtime_fix):run([cargo,'build','--locked','-p','uf-daemon','--target-dir',target,'-j','4'],'affected-daemon-build',1200)
 else:
  previous=root/'implementation'/('build-host051' if runtime_fix else 'build-host049')
  assert b'Finished `dev` profile' in (previous/'affected-daemon-build.log').read_bytes()
  assert sha(target/'debug/uf-daemon')==sha(previous/'uf-daemon')
  if runtime_fix:
   reused=json.loads((root/'implementation/source-host051/.uflow-source.json').read_bytes())
   for rel,row in reused['files'].items():
    if rel not in ('tools/phasec/build_current_terminal_host.py','tools/phasec/run_mem_service_pool_matrix.py'):assert current['files'][rel]==row and sha(root/'implementation/source-host051'/rel)==row['sha256']
  (out/'production-build-reuse.json').write_text(json.dumps({'compiled_source_commit':reused['commit'] if runtime_fix else compiled['commit'],'original_job':'cb073' if runtime_fix else 'cb070','original_job_exit':1,
   'actual_daemon_build_completed':True,'repeated_daemon_build':False,'reused_daemon_sha256':sha(previous/'uf-daemon')},indent=2))
 launch=out/'uf-daemon'
 with (target/'debug/uf-daemon').open('rb') as src,launch.open('xb') as dst:shutil.copyfileobj(src,dst);dst.flush();os.fsync(dst.fileno())
 launch.chmod(0o755);assert launch.stat().st_nlink==1
 if dispatch_fix:
  # Only production main's early dispatch changed. Its test harness and every
  # reachable tested module/fixture are unchanged; do not repeat 31 old cases.
  previous=root/'implementation/build-host050'
  assert b'test result: ok. 31 passed; 0 failed; 0 ignored;' in (previous/'affected-terminal-tests.log').read_bytes()
  for file in ('test-artifacts.log','affected-terminal-tests.log'):shutil.copyfile(previous/file,out/file)
 else:run([cargo,'test','--locked','-p','uf-daemon','--target-dir',target,'-j','4','--no-run','--message-format=json'],'test-artifacts',1200)
 rows=[json.loads(line) for line in (out/'test-artifacts.log').read_text().splitlines() if line.startswith('{')]
 tests=[Path(v['executable']) for v in rows if v.get('reason')=='compiler-artifact' and v.get('target',{}).get('name')=='uf-daemon' and v.get('profile',{}).get('test') and v.get('executable')];assert len(tests)==1;rust=tests[0]
 test_pin=sha(rust)
 if not dispatch_fix:run([rust,'pool_terminal_domain::','--nocapture','--test-threads=4'],'affected-terminal-tests')
 run([sys.executable,'-B','-m','unittest','discover','-s',source/'examples/mem_service_pool','-p','test_guest_panic.py'],'blocked-send-and-panic-tests')
 # Bare production entries always fail closed, including arbitrary descriptors.
 for index,argv in enumerate([[launch,'--pool-terminal-domain-loss'],[launch,'--pool-terminal-current-manifest',root/'must-not-read-case',sha(source/'.uflow-source.json')]]):
  r=subprocess.run(list(map(str,argv)),env=env,capture_output=True,timeout=10);assert r.returncode==1 and b'production-not-ready' in r.stderr
  (out/('production-entry-%d.json'%index)).write_text(json.dumps({'actual_exit':r.returncode,'stderr':r.stderr.decode(),'original_IO':False},indent=2))
 for panic,short,label in [(False,'v052' if runtime_fix else ('v051' if dispatch_fix else ('v050' if resume else 'v049')),'current-normal-matrix'),(True,'w052' if runtime_fix else ('w051' if dispatch_fix else ('w050' if resume else 'w049')),'current-panic-Unknown-matrix')]:
  run([sys.executable,'-B',source/'examples/mem_service_pool/matrix_entry_cpu.py','--current-terminal-domain',*(['--panic-on-first-write'] if panic else []),
   '--agent',cbase/'cpu-agent','--worker',cbase/'cpu-worker','--controller',root/'env/mem-service-sdk/bin/linqu_mem_service',
   '--scratch',root/short,'--output',out/label,'--rust-tests',rust,'--source',source,'--image',cbase/'pool-agent','--shim',cbase/'fence/libuf_acl_shim.so'],label)
 assert sha(state)==state_pin
 for path,pin in pins.items():assert sha(Path(path))==pin
 result={'schema':1,'pass':True,'source_commit':current['commit'],'source_manifest_sha256':sha(source/'.uflow-source.json'),
  'compiled_only':'none; exact production051/test049 reused' if runtime_fix else ('production uf-daemon dispatcher; unchanged terminal31 reused' if dispatch_fix else ('affected terminal Rust tests; production daemon reused' if resume else 'affected uf-daemon and terminal Rust tests')),
  'compiled_source_commit':reused['commit'] if runtime_fix else (current['commit'] if dispatch_fix else (compiled['commit'] if resume else current['commit'])),
  'terminal_Rust31_source_commit':compiled['commit'] if resume else current['commit'],
  'terminal_Rust31_reused_without_repeat':dispatch_fix,'production_dispatch_fix_recompiled':dispatch_fix and not runtime_fix,'runtime_ELF':str(launch),'runtime_ELF_sha256':sha(launch),'runtime_ELF_nlink':1,
  'Rust_test_ELF':str(rust),'Rust_test_ELF_sha256':sha(rust),'C_image_SDK_worker_reused_unchanged':pins,
  'SDK53_worker233_unchanged':True,'production_v2_ready':False,'accepted_E_v2_pins':None,
  'real_c064_state_sha256_before_after':state_pin,'new_Guest_or_original_ledger_mutation':False,
  'normal_current_CPU_matrix':json.loads((out/'current-normal-matrix/results.json').read_bytes()),
  'panic_current_CPU_matrix':json.loads((out/'current-panic-Unknown-matrix/results.json').read_bytes())}
 assert sha(rust)==test_pin
 (out/'validation-receipt.json').write_text(json.dumps(result,indent=2));print(json.dumps(result))
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--source',type=Path,required=True);p.add_argument('--output',required=True);a=p.parse_args();main(a.root,a.source,a.output)
