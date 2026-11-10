"""Runtime-only third-generation CPU driver correction; no rebuild/retest loop."""
import argparse,hashlib,json,os,subprocess,sys
from pathlib import Path
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main(root,source,name):
 assert str(root)=='/workspace/uflow/runs/mem-service-shared-pool-20261108' and name=='build-host060' and 'UFLOW_ENV_CLOSE_FD' in os.environ
 out=root/'implementation'/name;out.mkdir(exist_ok=False);old=root/'implementation/source-host059';m=json.loads((old/'.uflow-source.json').read_bytes());new=json.loads((source/'.uflow-source.json').read_bytes())
 allowed={'tools/phasec/run_mem_service_pool_matrix.py','examples/mem_service_pool/matrix_entry_cpu.py'};added={'tools/phasec/resume_case_chain_cpu_host.py'}
 assert set(new['files'])-set(m['files'])==added and not set(m['files'])-set(new['files'])
 for rel,row in m['files'].items():
  assert sha(old/rel)==row['sha256']
  if rel not in allowed:assert new['files'][rel]==row and sha(source/rel)==row['sha256']
 previous=root/'implementation/build-host059';target=root/'implementation/cargo-host059/debug';rust=target/'deps/uf_daemon-0f10ee78cd381af7'
 assert b'test result: ok. 11 passed; 0 failed;' in (previous/'third-generation-Rust-tests.log').read_bytes()
 for pattern,n in [('test_case_chain_gate.py',2),('test_result_ownership.py',4),('test_normal_journal_shutdown.py',2)]:assert ('Ran %d tests'%n) in (previous/(pattern+'.log')).read_text() and '\nOK\n' in (previous/(pattern+'.log')).read_text()
 assert b'test result: ok. 1 passed;' in (previous/'shared-fence-union-compatibility.log').read_bytes()
 state=root/'runs/c081/state/state.json';pin=sha(state);assert pin=='4d33870d43d69c6eb094494fe794cf38cf73bbb609fd72514dadd6e4cbd5cc26'
 before={str(p):sha(p) for p in [rust,target/'uf-daemon',previous/'pool_agent.o',previous/'pool-guest-init']}
 cbase=root/'implementation/build-host046';env={**os.environ,'PYTHONPATH':str(source/'sdk/python'),'PYTHONDONTWRITEBYTECODE':'1','LD_LIBRARY_PATH':str(cbase/'fence')}
 with (out/'three-generation-normal-CPU-matrix.log').open('xb') as log:
  subprocess.run(list(map(str,[sys.executable,'-B',source/'examples/mem_service_pool/matrix_entry_cpu.py','--chain-terminal-domain',
   '--agent',cbase/'cpu-agent','--worker',cbase/'cpu-worker','--controller',root/'env/mem-service-sdk/bin/linqu_mem_service',
   '--scratch',root/'z060','--output',out/'three-generation-normal-CPU-matrix','--rust-tests',rust,'--source',source,'--image',cbase/'pool-agent','--shim',cbase/'fence/libuf_acl_shim.so'])),
   cwd=source,env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=900)
 assert sha(state)==pin and not (root/'implementation/responsibility-c013/successor-c081-terminal-domain-loss-v3').exists()
 for p,v in before.items():assert sha(Path(p))==v
 result={'pass':True,'source_commit':new['commit'],'source_manifest_sha256':sha(source/'.uflow-source.json'),'compiled_source_commit':m['commit'],
  'actual_completed_Rust11_and_shared1_and_Python8_reused':True,'new_Cargo_C_SDK_image_build':False,'reused_ELFs':before,
  'production_new_context_ready':False,'real_c081_sha256_before_after':pin,'new_Guest_or_original_lock_or_ledger':False,
  'three_generation_CPU_matrix':json.loads((out/'three-generation-normal-CPU-matrix/results.json').read_bytes())}
 (out/'validation-receipt.json').write_text(json.dumps(result,indent=2));print(json.dumps({'status':'PASS_THIRD_GENERATION_CPU_DRIVER_NO_REBUILD','source_commit':new['commit'],'actual_current_unchanged':True}))
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--source',type=Path,required=True);p.add_argument('--output',required=True);a=p.parse_args();main(a.root,a.source,a.output)
