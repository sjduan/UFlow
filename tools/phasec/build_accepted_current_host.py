"""Affected production Rust/adapter checks plus fresh actual E read-only APIs.

No original lock/transaction, Guest, pool or metadata registration is executed.
Reuse the accepted C/image/SDK/31Rust/130Python/mapping evidence transparently.
"""
import argparse,hashlib,json,os,shutil,subprocess,sys
from pathlib import Path
ROOT='/workspace/uflow/runs/mem-service-shared-pool-20261108'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main(root,source,name):
 assert str(root)==ROOT and name=='build-host054' and 'UFLOW_ENV_CLOSE_FD' in os.environ
 out=root/'implementation'/name;out.mkdir(exist_ok=False)
 old_source=root/'implementation/source-host053';old=json.loads((old_source/'.uflow-source.json').read_bytes());current=json.loads((source/'.uflow-source.json').read_bytes())
 assert old['commit']=='5852c400c741cd327a85b7d176e3f54599a16344' and len(old['files'])==259
 changed={'crates/uf-daemon/src/main.rs','crates/uf-daemon/src/pool_service.rs','crates/uf-daemon/src/pool_terminal_domain/current.rs',
  'crates/uf-daemon/src/pool_terminal_domain/current/tests.rs','examples/mem_service_pool/test_qemu_profile.py',
  'tools/phasec/qemu_profile_gate.py','tools/phasec/run_mem_service_pool_matrix.py','tools/phasec/pool_run_metadata.py'}
 actual=set()
 for rel,row in current['files'].items():
  p=source/rel;assert not p.is_symlink() and sha(p)==row['sha256'] and p.stat().st_mode&0o777==int(row['mode'][-3:],8)
  if rel in old['files']:
   assert sha(old_source/rel)==old['files'][rel]['sha256']
   if row!=old['files'][rel]:actual.add(rel)
  else:assert rel.startswith('examples/mem_service_pool/current_terminal_production/') or rel in {
   'tools/phasec/accepted_producers.py','tools/phasec/terminal_current_adapter.py','tools/phasec/profile_metadata_gate.py',
   'tools/phasec/build_accepted_current_host.py','examples/mem_service_pool/test_accepted_producers.py'}
 assert actual==changed
 previous=root/'implementation/build-host053';reuse=json.loads((previous/'validation-receipt.json').read_bytes());assert reuse['pass']
 assert sha(previous/'validation-receipt.json')=='3955d66e08c7def8038e8d55efe6cf207c64b07e20c942cd4a36aa707f18cf28'
 pins=reuse['reused_ELFs']
 for p,pin in pins.items():assert sha(Path(p))==pin
 base=root/'implementation/build-host047';receipt=json.loads((base/'build-receipt.json').read_bytes());cbase=root/'implementation/build-host046'
 for rel,pin in receipt['installed_sdk_manifest'].items():assert sha(root/'env/mem-service-sdk'/rel)==pin
 for p,row in receipt['official_worker']['source_and_headers_link_inputs'].items():assert sha(Path(p))==row['sha256']
 original=root/'runs/c064/state/state.json';state_pin=sha(original);assert state_pin=='163f7500836dc4fd199d34f83a0e08453ab05255f8b15a57338052df4601d5b0'
 assert not (root/'implementation/responsibility-c013/successor-c064-terminal-domain-loss-v2').exists()
 cargo=shutil.which('cargo') or '/root/.cargo/bin/cargo';target=root/'implementation/cargo-host054';assert not target.exists()
 env={**os.environ,'PATH':str(Path(cargo).parent)+os.pathsep+os.environ.get('PATH',''),'CARGO_INCREMENTAL':'0','CARGO_BUILD_JOBS':'4',
  'UF_ACL_SHIM_LIB_DIR':str(cbase/'fence'),'LD_LIBRARY_PATH':str(cbase/'fence'),'PYTHONPATH':str(source/'sdk/python'),'PYTHONDONTWRITEBYTECODE':'1'}
 def run(argv,label,timeout=600):
  with (out/(label+'.log')).open('xb') as log:subprocess.run(list(map(str,argv)),cwd=source,env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=timeout)
 run([cargo,'build','--locked','-p','uf-daemon','--target-dir',target,'-j','4'],'affected-production-build',1200)
 launch=out/'uf-daemon'
 with (target/'debug/uf-daemon').open('rb') as src,launch.open('xb') as dst:shutil.copyfileobj(src,dst);dst.flush();os.fsync(dst.fileno())
 launch.chmod(0o755);assert launch.stat().st_nlink==1
 with launch.open('rb') as f:os.fsync(f.fileno())
 descriptor=os.open(out,os.O_RDONLY|os.O_DIRECTORY)
 try:os.fsync(descriptor)
 finally:os.close(descriptor)
 run([cargo,'test','--locked','-p','uf-daemon','--target-dir',target,'-j','4','--no-run','--message-format=json'],'test-artifacts',1200)
 rows=[json.loads(s) for s in (out/'test-artifacts.log').read_text().splitlines() if s.startswith('{')]
 tests=[Path(v['executable']) for v in rows if v.get('reason')=='compiler-artifact' and v.get('target',{}).get('name')=='uf-daemon' and v.get('profile',{}).get('test') and v.get('executable')];assert len(tests)==1;rust=tests[0]
 run([rust,'current_production_','--nocapture','--test-threads=1'],'new-current-production-tests')
 run([rust,'--exact','pool_terminal_domain::current::tests::production_v2_refuses_before_any_descriptor_state_lock_or_verifier_io','--nocapture'],'current-early-binding-rejection')
 for pattern in ('test_accepted_producers.py','test_qemu_profile.py'):
  run([sys.executable,'-B','-m','unittest','discover','-s',source/'examples/mem_service_pool','-p',pattern],pattern)
 for index,args in enumerate([[launch,'--pool-terminal-domain-loss'],[launch,'--pool-terminal-current-manifest',root/'must-not-read-case','0'*64]]):
  p=subprocess.run(list(map(str,args)),env=env,capture_output=True,timeout=10)
  (out/('early-CLI-%d.json'%index)).write_text(json.dumps({'actual_exit':p.returncode,'stderr':p.stderr.decode(),'read_original_state_or_lock':False},indent=2))
  assert p.returncode==1 and b'production-not-ready' in p.stderr
 # Explicit read-only mode returns no permit/authority and never acquires the
 # original flock. Consumption invokes the same fresh predicate under flock.
 p=subprocess.run([str(launch),'--pool-terminal-current-verify'],env=env,capture_output=True,timeout=120)
 (out/'actual-E-terminal.stderr').write_bytes(p.stderr);assert p.returncode==0,p.stderr.decode()
 proof=json.loads(p.stdout);(out/'actual-E-terminal.json').write_bytes(p.stdout)
 assert proof['validated_candidate']['assessment_sha256']=='427e165598dee9a64ead152a97e5ba33bf2e3c24748aa1c7a6e5fe5c52388c5c'
 sys.path.insert(0,str(source/'tools/phasec'))
 from qemu_profile_gate import binding,launch_qemu_gate
 from accepted_producers import accepted_anchors,AP_SHA
 anchor=accepted_anchors();profile=launch_qemu_gate(root,{'qemu_profile':binding(),'qemu_binary':anchor['profile']['binary']['path']})
 (out/'actual-E-QEMU-profile.json').write_text(json.dumps(profile.metadata(),indent=2))
 assert profile.sha256=='3c34d92dc970359036a969bd109a61086e23e5d3b0d908c9143ee62beb006779'
 assert sha(original)==state_pin and not (root/'implementation/responsibility-c013/successor-c064-terminal-domain-loss-v2').exists()
 for p,pin in pins.items():assert sha(Path(p))==pin
 result={**receipt,'source':str(source),'source_commit':current['commit'],'source_manifest_sha256':sha(source/'.uflow-source.json'),
  'daemon':str(launch),'daemon_sha256':sha(launch),'runtime_ELF_nlink':1,'compiled_only':'affected uf-daemon + targeted production tests',
  'accepted_producer_anchor_sha256':AP_SHA,'actual_E_current_assessment_sha256':proof['validated_candidate']['assessment_sha256'],
  'actual_QEMU_profile_identity':profile.metadata(),'current_terminal_production_bound':True,'new_QEMU_profile_production_bound':True,
  'runtime_execution_package_review_pending':True,'metadata_profile_release_binding_pending':True,
  'real_c064_state_sha256_before_after':state_pin,'original_lock_or_ledger_transaction_or_new_Guest':False,
  'Rust_test_ELF':str(rust),'Rust_test_ELF_sha256':sha(rust),
  'reuse':{'old_31Rust_130Python_and_mapping_matrix_evidence_from':reuse['source_commit'],'new_C_image_SDK_build':False,
          'base_build047_receipt_sha256':sha(base/'build-receipt.json'),'preserved_ELFs':pins}}
 (out/'build-receipt.json').write_text(json.dumps(result,indent=2));print(json.dumps({'status':'PASS_ACCEPTED_PRODUCTION_ADAPTER_HOST_READONLY','source_commit':current['commit'],'runtime_ELF_sha256':sha(launch),'state_sha256_unchanged':state_pin}))
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--source',type=Path,required=True);p.add_argument('--output',required=True);a=p.parse_args();main(a.root,a.source,a.output)
