"""Affected Rust, real Linux SO_PEERCRED tests, HOST control, CPU regression.
No original lock/ledger, Guest, QMP or device; immutable prior results reused.
"""
import argparse,hashlib,json,os,shutil,subprocess,sys
from pathlib import Path
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main(root,source,name,reuse=False):
 assert str(root)=='/workspace/uflow/runs/mem-service-shared-pool-20261108' and name.startswith('build-host') and name[-3:].isdigit() and len(name)==13 and 'UFLOW_ENV_CLOSE_FD' in os.environ
 generation=name[-3:];assert source==root/'implementation'/('source-host'+generation)
 out=root/'implementation'/name;out.mkdir(exist_ok=False);manifest=json.loads((source/'.uflow-source.json').read_bytes())
 reused=None
 if reuse:
  assert int(generation)>62
  closed=json.loads((root/'runs/cb091/result.json').read_bytes())['processes'];assert closed['actual_task_exit_code']==0 and closed['process_tree_empty'] and closed['task_resources_closed'] and not closed['remaining'] and not closed['evidence_errors']
  reused=json.loads((root/'implementation/build-host062/validation-receipt.json').read_bytes());assert reused['pass']
  old=root/'implementation/source-host062';previous=json.loads((old/'.uflow-source.json').read_bytes());assert previous['commit']==reused['source_commit']
  allowed={'crates/uf-daemon/src/managed_read_cache.rs','crates/uf-daemon/src/managed_read_cache/tests.rs','crates/uf-daemon/src/pool_ranges.rs','tools/phasec/managed_cache_control.py','examples/mem_service_pool/test_managed_cache_control.py','tools/phasec/build_managed_cache_host.py'}
  assert set(previous['files'])==set(manifest['files'])
  for rel,row in previous['files'].items():
   assert sha(old/rel)==row['sha256']
   if rel not in allowed:assert manifest['files'][rel]==row
 for rel,row in manifest['files'].items():
  p=source/rel;assert p.is_file() and not p.is_symlink() and sha(p)==row['sha256'] and p.stat().st_mode&0o777==int(row['mode'][-3:],8)
 prior=root/'implementation/build-host056';receipt=json.loads((prior/'build-receipt.json').read_bytes())
 frozen={**receipt['reuse']['preserved_ELFs'],receipt['daemon']:receipt['daemon_sha256'],receipt['Rust_test_ELF']:receipt['Rust_test_ELF_sha256']}
 for p,pin in frozen.items():assert sha(Path(p))==pin
 for rel,pin in receipt['installed_sdk_manifest'].items():assert sha(root/'env/mem-service-sdk'/rel)==pin
 for p,row in receipt['official_worker']['source_and_headers_link_inputs'].items():assert sha(Path(p))==row['sha256']
 original=root/'runs/c081/state/state.json';state_pin=sha(original);assert state_pin=='4d33870d43d69c6eb094494fe794cf38cf73bbb609fd72514dadd6e4cbd5cc26'
 authority=root/'implementation/responsibility-c013';real={str(p):sha(p) for folder in ['successor-c044-terminal-domain-loss-v1','successor-c064-terminal-domain-loss-v2'] for p in (authority/folder).iterdir()};assert len(real)==15
 assert not (authority/'successor-c081-terminal-domain-loss-v3').exists()
 cbase=root/'implementation/build-host046';cargo=shutil.which('cargo') or '/root/.cargo/bin/cargo';target=root/'implementation'/('cargo-host'+generation);assert not target.exists()
 env={**os.environ,'PATH':str(Path(cargo).parent)+os.pathsep+os.environ.get('PATH',''),'CARGO_INCREMENTAL':'0','CARGO_BUILD_JOBS':'4',
      'UF_ACL_SHIM_LIB_DIR':str(cbase/'fence'),'LD_LIBRARY_PATH':str(cbase/'fence'),'PYTHONPATH':str(source/'sdk/python'),'PYTHONDONTWRITEBYTECODE':'1'}
 def run(argv,label,timeout=60):
  with (out/(label+'.log')).open('xb') as log:subprocess.run(list(map(str,argv)),cwd=source,env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=timeout)
 run([cargo,'build','--locked','-p','uf-daemon','--target-dir',target,'-j','4'],'affected-Rust-build',1200)
 run([cargo,'test','--locked','-p','uf-daemon','--target-dir',target,'-j','4','--no-run','--message-format=json'],'affected-test-build',1200)
 rows=[json.loads(s) for s in (out/'affected-test-build.log').read_text().splitlines() if s.startswith('{')]
 tests=[Path(v['executable']) for v in rows if v.get('reason')=='compiler-artifact' and v.get('target',{}).get('name')=='uf-daemon' and v.get('profile',{}).get('test') and v.get('executable')];assert len(tests)==1;rust=tests[0]
 for scope in ['managed_read_cache::tests::','mem_service_pool::tests::','pool_ranges::tests::','managed_pool::tests::confirmed_unmap_closes_catalog_unknown_and_keeps_content_invalid']:
  run([rust,scope,'--test-threads=4','--nocapture'],scope.replace(':','_'))
 for pattern in (['test_managed_cache_control.py'] if reuse else ['test_daemon_peer_auth.py','test_managed_cache_control.py','test_case_chain_gate.py','test_guest_panic.py']):
  run([sys.executable,'-B','-m','unittest','discover','-s',source/'examples/mem_service_pool','-p',pattern],pattern)
 if not reuse:
  run([sys.executable,'-B',source/'examples/mem_service_pool/matrix_entry_cpu.py','--chain-terminal-domain',
   '--agent',cbase/'cpu-agent','--worker',cbase/'cpu-worker','--controller',root/'env/mem-service-sdk/bin/linqu_mem_service',
   '--scratch',root/('z'+generation),'--output',out/'authenticated-three-generation-CPU-matrix','--rust-tests',rust,'--source',source,'--image',cbase/'pool-agent','--shim',cbase/'fence/libuf_acl_shim.so'],
   'authenticated-three-generation-CPU-matrix',900)
 assert sha(original)==state_pin and not (authority/'successor-c081-terminal-domain-loss-v3').exists()
 for p,pin in {**frozen,**real}.items():assert sha(Path(p))==pin
 value={'pass':True,'source_commit':manifest['commit'],'source_manifest_sha256':sha(source/'.uflow-source.json'),
        'daemon':str(target/'debug/uf-daemon'),'daemon_sha256':sha(target/'debug/uf-daemon'),'Rust_test_ELF':str(rust),'Rust_test_ELF_sha256':sha(rust),
        'original_c081_sha256_before_after':state_pin,'production_cache_or_terminal_context_ready':False,'new_Guest_QMP_original_lock_ledger':False,
        'actual_Linux_SO_PEERCRED_owned_CPU_children':True,'HOST_native_controls_only_transport_doubles':True,
        'matrix':reused['matrix'] if reuse else json.loads((out/'authenticated-three-generation-CPU-matrix/results.json').read_bytes()),
        'completed_authenticated_CPU_matrix_and_Linux_peer_tests_reused_from':'build-host062' if reuse else None,
        'prior_third_generation11_shared1_Python8_two_C_TUs_cost_model_ownership_reused_from':'build-host059 + CPU build-host061; same immutable named evidence'}
 (out/'validation-receipt.json').write_text(json.dumps(value,indent=2));print(json.dumps({'status':'PASS_AUTHENTICATED_MANAGED_CACHE_HOST','source_commit':manifest['commit']}))
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--source',type=Path,required=True);p.add_argument('--output',required=True);p.add_argument('--reuse-authenticated-host062',action='store_true');a=p.parse_args();main(a.root,a.source,a.output,a.reuse_authenticated_host062)
