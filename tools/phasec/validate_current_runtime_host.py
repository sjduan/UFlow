"""Python-only current runtime preflight/isolation, no Guest/ledger/Cargo."""
import argparse,hashlib,json,os,subprocess,sys
from pathlib import Path
ROOT='/workspace/uflow/runs/mem-service-shared-pool-20261108'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main(root,source,name):
 assert str(root)==ROOT and name=='build-host055' and 'UFLOW_ENV_CLOSE_FD' in os.environ
 out=root/'implementation'/name;out.mkdir(exist_ok=False)
 old_source=root/'implementation/source-host054';old=json.loads((old_source/'.uflow-source.json').read_bytes());current=json.loads((source/'.uflow-source.json').read_bytes())
 assert old['commit']=='ed76d08ea25c8a5b923a65c76067a19f3bf383f5' and len(old['files'])==268
 changes={'tools/phasec/run_mem_service_pool_matrix.py','tools/phasec/profile_metadata_gate.py','examples/mem_service_pool/current_terminal_production/.gitattributes'}
 actual=set()
 for rel,row in current['files'].items():
  p=source/rel;assert not p.is_symlink() and sha(p)==row['sha256'] and p.stat().st_mode&0o777==int(row['mode'][-3:],8)
  if rel in old['files']:
   assert sha(old_source/rel)==old['files'][rel]['sha256']
   if row!=old['files'][rel]:actual.add(rel)
  else:assert rel in {'tools/phasec/current_fm_isolation.py','tools/phasec/validate_current_runtime_host.py',
   'examples/mem_service_pool/current_terminal_production/historical62-identities.json','examples/mem_service_pool/test_current_fm_isolation.py'}
 assert actual==changes
 for rel in old['files']:
  if rel.startswith('crates/') and Path(rel).suffix in ('.rs','.c','.h'):
   text=(old_source/rel).read_text();assert not any(Path(p).name in text and ('include_str!' in text or 'include_bytes!' in text or '#include' in text) for p in changes)
 previous=root/'implementation/build-host054';receipt=json.loads((previous/'build-receipt.json').read_bytes())
 assert receipt['source_commit']==old['commit'] and receipt['daemon_sha256']=='735c48fe10c9c0801874cc545654ce626e4b60d893f0b2c67333b68e16d821a7'
 all_pins={**receipt['reuse']['preserved_ELFs'],receipt['daemon']:receipt['daemon_sha256'],receipt['Rust_test_ELF']:receipt['Rust_test_ELF_sha256']}
 for p,pin in all_pins.items():assert sha(Path(p))==pin
 state=root/'runs/c064/state/state.json';state_pin=sha(state);assert state_pin=='163f7500836dc4fd199d34f83a0e08453ab05255f8b15a57338052df4601d5b0'
 sys.path.insert(0,str(source/'tools/phasec'))
 from current_fm_isolation import historical62
 from qemu_profile_gate import binding,launch_qemu_gate
 from accepted_producers import accepted_anchors
 from profile_metadata_gate import require_profile_metadata_ready
 aliases,historical=historical62();assert historical['historical_files']==62 and len(aliases)>=62
 profile=launch_qemu_gate(root,{'qemu_profile':binding(),'qemu_binary':accepted_anchors()['profile']['binary']['path']})
 try:require_profile_metadata_ready(profile)
 except ValueError as error:assert 'metadata not-ready' in str(error)
 else:raise AssertionError('unaccepted future metadata API was promoted')
 env={**os.environ,'PYTHONPATH':str(source/'sdk/python'),'PYTHONDONTWRITEBYTECODE':'1'}
 with (out/'current-FM-isolation-tests.log').open('xb') as log:subprocess.run([sys.executable,'-B','-m','unittest','discover','-s',str(source/'examples/mem_service_pool'),'-p','test_current_fm_isolation.py'],check=True,env=env,cwd=source,stdout=log,stderr=subprocess.STDOUT,timeout=30)
 contexts={}
 for job,phase,negative in [('c079','terminal-only',False),('c083','successor',False),('c084','terminal-only',True)]:
  p=json.loads((root/'runs'/job/'result.json').read_bytes())['processes'];v=json.loads((root/'runs'/job/'context-check.json').read_bytes())
  assert p['actual_task_exit_code']==0 and p['process_tree_empty'] and p['task_resources_closed'] and not p['remaining'] and not p['evidence_errors']
  assert v['actual_exit']==(1 if negative else 0) and v['phase']==phase and v['negative_hardlink_case']==negative
  contexts[job]=v
 for p,pin in all_pins.items():assert sha(Path(p))==pin
 assert sha(state)==state_pin and not (root/'implementation/responsibility-c013/successor-c064-terminal-domain-loss-v2').exists()
 value={**receipt,'source':str(source),'source_commit':current['commit'],'source_manifest_sha256':sha(source/'.uflow-source.json'),
  'compiled_source_commit':old['commit'],'compiled_generation':'host054','new_Cargo_or_C_image_SDK_build':False,
  'runtime_preflight_historical62':historical,'actual_current_E0_context_checks':contexts,
  'production_terminal_and_private_QEMU_bound':True,'profile_metadata_producer_binding_ready':False,
  'profile_metadata_rejection_occurs_before_lower_authority_token_or_Guest':True,
  'runtime_execution_package_review_pending':True,'original_lock_ledger_or_new_Guest':False,'real_c064_state_sha256_before_after':state_pin}
 (out/'build-receipt.json').write_text(json.dumps(value,indent=2));print(json.dumps({'status':'PASS_RUNTIME_PREFLIGHT_HISTORICAL62_NO_CARGO','source_commit':current['commit'],'metadata_dependency':'NOT_READY_ACTUAL_E_RELEASE_PENDING','original_current_unchanged':True}))
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--source',type=Path,required=True);p.add_argument('--output',required=True);a=p.parse_args();main(a.root,a.source,a.output)
