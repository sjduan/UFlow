"""Runtime-only profile/collector HOST checks; exact accepted Rust/C reuse."""
import argparse,hashlib,json,os,subprocess,sys
from pathlib import Path
ROOT='/workspace/uflow/runs/mem-service-shared-pool-20261108'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main(root,source,name):
 assert str(root)==ROOT and name=='build-host053' and 'UFLOW_ENV_CLOSE_FD' in os.environ
 out=root/'implementation'/name;out.mkdir(exist_ok=False)
 old_source=root/'implementation/source-host052';old=json.loads((old_source/'.uflow-source.json').read_bytes());current=json.loads((source/'.uflow-source.json').read_bytes())
 assert old['commit']=='588cb9bf3a1422d782b10a6040a83413e10745a2' and len(old['files'])==255
 changed={'tools/phasec/collect_owned_qemu_metadata.py','tools/phasec/run_mem_service_pool_matrix.py',
  'examples/mem_service_pool/matrix_entry_cpu.py','examples/mem_service_pool/test_owned_qemu_metadata.py'}
 added={'tools/phasec/qemu_profile_gate.py','tools/phasec/validate_qemu_profile_host.py','examples/mem_service_pool/test_qemu_profile.py','examples/mem_service_pool/current_terminal_host/QEMU-PROFILE.md'}
 assert set(current['files'])==set(old['files'])|added;actual=set()
 for rel,row in current['files'].items():
  p=source/rel;assert not p.is_symlink() and sha(p)==row['sha256'] and p.stat().st_mode&0o777==int(row['mode'][-3:],8)
  if rel in old['files']:
   assert sha(old_source/rel)==old['files'][rel]['sha256']
   if row!=old['files'][rel]:actual.add(rel)
 assert actual==changed
 for rel in old['files']:
  if rel.startswith('crates/') and Path(rel).suffix in ('.rs','.h','.c'):
   text=(old_source/rel).read_text();assert not any(Path(p).name in text and ('include_str!' in text or 'include_bytes!' in text or '#include' in text) for p in changed|added)
 base=root/'implementation/build-host052';receipt=json.loads((base/'validation-receipt.json').read_bytes());assert receipt['pass']
 pins=dict(receipt['C_image_SDK_worker_reused_unchanged']);pins[receipt['runtime_ELF']]=receipt['runtime_ELF_sha256'];pins[receipt['Rust_test_ELF']]=receipt['Rust_test_ELF_sha256']
 for path,pin in pins.items():assert sha(Path(path))==pin
 build=json.loads((root/'implementation/build-host047/build-receipt.json').read_bytes())
 assert len(build['installed_sdk_manifest'])==53 and len(build['official_worker']['source_and_headers_link_inputs'])==233
 for rel,pin in build['installed_sdk_manifest'].items():assert sha(root/'env/mem-service-sdk'/rel)==pin
 for p,row in build['official_worker']['source_and_headers_link_inputs'].items():assert sha(Path(p))==row['sha256']
 state=root/'runs/c064/state/state.json';state_pin=sha(state);assert state_pin=='163f7500836dc4fd199d34f83a0e08453ab05255f8b15a57338052df4601d5b0'
 job=json.loads((root/'runs/cb074/result.json').read_bytes())['processes'];assert job['actual_task_exit_code']==0 and job['process_tree_empty'] and job['task_resources_closed'] and not job['remaining'] and not job['evidence_errors']
 sys.path.insert(0,str(source/'tools/phasec'))
 from qemu_profile_gate import launch_qemu_gate,capture_identity,LEGACY_BINARY
 legacy=launch_qemu_gate(root,{'qemu_binary':str(LEGACY_BINARY)});assert capture_identity({'qemu_binary':str(LEGACY_BINARY)},legacy) is legacy
 (out/'actual-old-QEMU-readonly.json').write_text(json.dumps(legacy.metadata(),indent=2))
 try:launch_qemu_gate(root,{'qemu_profile':{'ready':True},'qemu_binary':str(LEGACY_BINARY)})
 except ValueError as error:assert 'not-ready' in str(error)
 else:raise AssertionError('new production QEMU profile unexpectedly admitted')
 cbase=root/'implementation/build-host046';env={**os.environ,'PYTHONPATH':str(source/'sdk/python'),'PYTHONDONTWRITEBYTECODE':'1','LD_LIBRARY_PATH':str(cbase/'fence')}
 def run(argv,label,timeout=600):
  with (out/(label+'.log')).open('xb') as log:subprocess.run(list(map(str,argv)),env=env,cwd=source,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=timeout)
 run([sys.executable,'-B','-m','unittest','discover','-s',source/'examples/mem_service_pool','-p','test_*.py'],'python-pool-contracts')
 for panic,short,label in [(False,'v053','current-normal-matrix'),(True,'w053','current-panic-Unknown-matrix')]:
  run([sys.executable,'-B',source/'examples/mem_service_pool/matrix_entry_cpu.py','--current-terminal-domain',*(['--panic-on-first-write'] if panic else []),
   '--agent',cbase/'cpu-agent','--worker',cbase/'cpu-worker','--controller',root/'env/mem-service-sdk/bin/linqu_mem_service',
   '--scratch',root/short,'--output',out/label,'--rust-tests',receipt['Rust_test_ELF'],'--source',source,'--image',cbase/'pool-agent','--shim',cbase/'fence/libuf_acl_shim.so'],label)
 assert sha(state)==state_pin
 for p,pin in pins.items():assert sha(Path(p))==pin
 result={'schema':1,'pass':True,'source_commit':current['commit'],'source_manifest_sha256':sha(source/'.uflow-source.json'),
  'new_Cargo_or_C_image_SDK_build':False,'reused_current_consumer_receipt_sha256':sha(base/'validation-receipt.json'),
  'compiled_production_source_commit':receipt['compiled_source_commit'],'compiled_terminal31_source_commit':receipt['terminal_Rust31_source_commit'],
  'reused_ELFs':pins,'runtime_ELF':receipt['runtime_ELF'],'runtime_ELF_sha256':receipt['runtime_ELF_sha256'],
  'Rust_test_ELF':receipt['Rust_test_ELF'],'Rust_test_ELF_sha256':receipt['Rust_test_ELF_sha256'],
  'SDK53_worker233_unchanged':True,'new_QEMU_profile_production_ready':False,'new_QEMU_E_accepted_pins':None,
  'actual_original_QEMU_readonly_identity':legacy.metadata(),'kernel_profile_unchanged':True,'production_current_terminal_v2_ready':False,
  'real_c064_state_sha256_before_after':state_pin,'new_Guest_or_original_ledger_mutation':False,
  'normal_current_CPU_matrix':json.loads((out/'current-normal-matrix/results.json').read_bytes()),
  'panic_current_CPU_matrix':json.loads((out/'current-panic-Unknown-matrix/results.json').read_bytes())}
 (out/'validation-receipt.json').write_text(json.dumps(result,indent=2));print(json.dumps(result))
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--source',type=Path,required=True);p.add_argument('--output',required=True);a=p.parse_args();main(a.root,a.source,a.output)
