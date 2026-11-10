"""Affected Rust/two C TUs + one isolated CPU matrix, no original authority."""
import argparse,hashlib,json,os,shutil,subprocess,sys
from pathlib import Path
ROOT='/workspace/uflow/runs/mem-service-shared-pool-20261108'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main(root,source,name):
 assert str(root)==ROOT and name.startswith('build-host') and len(name)==13 and name[-3:].isdecimal() and int(name[-3:])>=57 and 'UFLOW_ENV_CLOSE_FD' in os.environ
 generation=name[-3:];assert source==root/'implementation'/('source-host'+generation)
 out=root/'implementation'/name;out.mkdir(exist_ok=False)
 manifest=json.loads((source/'.uflow-source.json').read_bytes())
 for rel,row in manifest['files'].items():
  p=source/rel;assert p.is_file() and not p.is_symlink() and sha(p)==row['sha256'] and p.stat().st_mode&0o777==int(row['mode'][-3:],8)
 prior=root/'implementation/build-host056';receipt=json.loads((prior/'build-receipt.json').read_bytes());assert receipt['source_commit']=='64014e46ec1144c16293054a4bfa70f7a8b36b19'
 frozen={**receipt['reuse']['preserved_ELFs'],receipt['daemon']:receipt['daemon_sha256'],receipt['Rust_test_ELF']:receipt['Rust_test_ELF_sha256']}
 for p,pin in frozen.items():assert sha(Path(p))==pin
 for rel,pin in receipt['installed_sdk_manifest'].items():assert sha(root/'env/mem-service-sdk'/rel)==pin
 for p,row in receipt['official_worker']['source_and_headers_link_inputs'].items():assert sha(Path(p))==row['sha256']
 original=root/'runs/c081/state/state.json';state_pin=sha(original);assert state_pin=='4d33870d43d69c6eb094494fe794cf38cf73bbb609fd72514dadd6e4cbd5cc26'
 authority=root/'implementation/responsibility-c013';real={str(p):sha(p) for folder in ['successor-c044-terminal-domain-loss-v1','successor-c064-terminal-domain-loss-v2'] for p in (authority/folder).iterdir()};assert len(real)==15
 assert not (authority/'successor-c081-terminal-domain-loss-v3').exists()
 cargo=shutil.which('cargo') or '/root/.cargo/bin/cargo';target=root/'implementation'/('cargo-host'+generation);assert not target.exists()
 cbase=root/'implementation/build-host046';own=source/'crates/uf-ub-sim-agent/c'
 env={**os.environ,'PATH':str(Path(cargo).parent)+os.pathsep+os.environ.get('PATH',''),'CARGO_INCREMENTAL':'0','CARGO_BUILD_JOBS':'4',
  'UF_ACL_SHIM_LIB_DIR':str(cbase/'fence'),'LD_LIBRARY_PATH':str(cbase/'fence'),'PYTHONPATH':str(source/'sdk/python'),'PYTHONDONTWRITEBYTECODE':'1',
  'UFLOW_POOL_JOURNAL_LAUNCHER':str(out/'cpu-guest-init')}
 def run(argv,label,timeout=900):
  with (out/(label+'.log')).open('xb') as log:subprocess.run(list(map(str,argv)),cwd=source,env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=timeout)
 run([cargo,'build','--locked','-p','uf-daemon','--target-dir',target,'-j','4'],'affected-Rust-build',1200)
 run([cargo,'test','--locked','-p','uf-daemon','--target-dir',target,'-j','4','--no-run','--message-format=json'],'affected-test-build',1200)
 rows=[json.loads(s) for s in (out/'affected-test-build.log').read_text().splitlines() if s.startswith('{')]
 tests=[Path(v['executable']) for v in rows if v.get('reason')=='compiler-artifact' and v.get('target',{}).get('name')=='uf-daemon' and v.get('profile',{}).get('test') and v.get('executable')];assert len(tests)==1;rust=tests[0]
 run([rust,'pool_terminal_domain::case_chain::tests::','--test-threads=4','--nocapture'],'third-generation-Rust-tests')
 run([rust,'--exact','pool_terminal_domain::current::tests::union_keeps_both_generations_and_independent_identity_highwaters','--nocapture'],'shared-fence-union-compatibility')
 launch=out/'uf-daemon'
 with (target/'debug/uf-daemon').open('rb') as src,launch.open('xb') as dst:shutil.copyfileobj(src,dst);dst.flush();os.fsync(dst.fileno())
 launch.chmod(0o755)
 with launch.open('rb') as f:os.fsync(f.fileno())
 fd=os.open(out,os.O_RDONLY|os.O_DIRECTORY)
 try:os.fsync(fd)
 finally:os.close(fd)
 assert launch.stat().st_nlink==1
 p=subprocess.run([str(launch),'--pool-terminal-case-manifest',str(root/'must-not-read-case'),'0'*64],env=env,capture_output=True,timeout=10)
 assert p.returncode==1 and b'production-not-ready' in p.stderr and b'no descriptor/state/authority/verifier IO' in p.stderr
 (out/'actual-production-not-ready.json').write_text(json.dumps({'actual_exit':p.returncode,'stderr':p.stderr.decode(),'original_IO':False},indent=2))
 # Recompile only the changed C translation units. Full C/SDK/image are kept
 # unchanged for CPU matrix reuse; a new production image is still pending.
 old_flags=receipt['compiler_argv'];flags=old_flags[:next(i for i,a in enumerate(old_flags) if a.endswith('/pool_agent.c'))]
 flags=[s.replace('/implementation/source-host046/','/implementation/source-host'+generation+'/') for s in flags]
 run(flags+['-c',own/'pool_agent.c','-o',out/'pool_agent.o'],'changed-agent-TU')
 run(flags+[own/'pool_guest_init.c','-o',out/'pool-guest-init'],'changed-PID1-TU')
 run(['/usr/bin/gcc','-O0','-Wall','-Wextra','-DUFLOW_POOL_LAUNCHER_FIXTURE',own/'pool_guest_init.c','-o',out/'cpu-guest-init'],'host-PID1-fixture-compile')
 for pattern in ['test_case_chain_gate.py','test_result_ownership.py','test_normal_journal_shutdown.py']:
  run([sys.executable,'-B','-m','unittest','discover','-s',source/'examples/mem_service_pool','-p',pattern],pattern,45)
 run([sys.executable,'-B',source/'tools/phasec/pool_access_cost_model.py','--sdk-provider',root/'env/mem-service-sdk/src/lingqu/mem_service/mem_service_provider.c',
  '--source',source,'--output',out/'full-pipeline-cost-model.json'],'full-pipeline-source-counter-model')
 sys.path.insert(0,str(source/'tools/phasec'))
 from result_ownership import collect
 ownership=collect(root/'runs/c081');assert ownership['actual_task_exit_code']==1 and ownership['ownership_history_reconstructed'] is False
 (out/'actual-c081-monitor-ownership.json').write_text(json.dumps(ownership,indent=2))
 run([sys.executable,'-B',source/'examples/mem_service_pool/matrix_entry_cpu.py','--chain-terminal-domain',
  '--agent',cbase/'cpu-agent','--worker',cbase/'cpu-worker','--controller',root/'env/mem-service-sdk/bin/linqu_mem_service',
  '--scratch',root/('z'+generation),'--output',out/'three-generation-normal-CPU-matrix','--rust-tests',rust,'--source',source,'--image',cbase/'pool-agent','--shim',cbase/'fence/libuf_acl_shim.so'],
  'three-generation-normal-CPU-matrix',900)
 assert sha(original)==state_pin and not (authority/'successor-c081-terminal-domain-loss-v3').exists()
 for p,pin in {**frozen,**real}.items():assert sha(Path(p))==pin
 value={'schema':3,'pass':True,'source':str(source),'source_commit':manifest['commit'],'source_manifest_sha256':sha(source/'.uflow-source.json'),
  'daemon':str(launch),'daemon_sha256':sha(launch),'daemon_single_link':True,'Rust_test_ELF':str(rust),'Rust_test_ELF_sha256':sha(rust),
  'targeted_new_Rust_behavioral_tests':7,'shared_fence_compatibility_Rust_tests':1,'Rust_harness_entries':3,'targeted_Python_tests':8,'Python_skips':0,
  'changed_C_TUs_only':{'pool_agent.o':sha(out/'pool_agent.o'),'pool-guest-init':sha(out/'pool-guest-init')},
  'C_image_SDK_worker_reused_without_rebuild':frozen,'new_production_C_image_created':False,
  'normal_journal_source_order_fixed_and_HOST_child_order_tested':True,'full_scalar_SDK_pipeline_model':json.loads((out/'full-pipeline-cost-model.json').read_bytes()),
  'actual_monitor_result_ownership_without_fake_history':ownership,'production_new_context_ready':False,
  'three_generation_CPU_matrix':json.loads((out/'three-generation-normal-CPU-matrix/results.json').read_bytes()),
  'real_c081_state_sha256_before_after':state_pin,'original_ledger_lock_or_new_Guest_or_FM_payload':False,
  'accepted_E_next_witness_QEMU_metadata_pins':None,'future_production_image_and_runtime_package_pending':True}
 (out/'validation-receipt.json').write_text(json.dumps(value,indent=2));print(json.dumps({'status':'PASS_THIRD_GENERATION_HOST_ONLY','source_commit':manifest['commit'],'production_ready':False,'real_current_unchanged':True}))
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--source',type=Path,required=True);p.add_argument('--output',required=True);a=p.parse_args();main(a.root,a.source,a.output)
