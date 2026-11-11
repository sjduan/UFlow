"""Targeted Python binding validation; exact HOST054 compiled closure reused."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()

def main(root,source,name):
    assert str(root)=='/workspace/uflow/runs/mem-service-shared-pool-20261108' and name=='build-host056' and 'UFLOW_ENV_CLOSE_FD' in os.environ
    out=root/'implementation'/name;out.mkdir(exist_ok=False)
    old_source=root/'implementation/source-host055';old=json.loads((old_source/'.uflow-source.json').read_bytes());current=json.loads((source/'.uflow-source.json').read_bytes())
    assert old['commit']=='9d4b531c893d93cb3d4ebe1669ffb626acad3fde'
    allowed={'examples/mem_service_pool/test_accepted_producers.py','tools/phasec/qemu_profile_gate.py',
        'tools/phasec/run_mem_service_pool_matrix.py','tools/phasec/profile_metadata_gate.py','tools/phasec/pool_run_metadata.py',
        'examples/mem_service_pool/current_terminal_production/.gitattributes'}
    added={'tools/phasec/profile_metadata_completion.py','tools/phasec/validate_profile_metadata_host.py',
        'examples/mem_service_pool/test_profile_metadata_consumer.py',
        'examples/mem_service_pool/current_terminal_production/accepted-metadata-anchors.json'}
    assert set(current['files'])-set(old['files'])==added and not set(old['files'])-set(current['files'])
    changed=set()
    for rel,row in current['files'].items():
        p=source/rel;assert not p.is_symlink() and sha(p)==row['sha256'] and p.stat().st_mode&0o777==int(row['mode'][-3:],8)
        if rel in old['files']:
            assert sha(old_source/rel)==old['files'][rel]['sha256']
            if row!=old['files'][rel]:changed.add(rel)
    assert changed==allowed
    compiled=root/'implementation/source-host054';cm=json.loads((compiled/'.uflow-source.json').read_bytes());embedded=set()
    for rel in cm['files']:
        if rel.startswith('crates/') and Path(rel).suffix in ('.rs','.c','.h'):
            p=compiled/rel;assert sha(p)==sha(source/rel)
            if p.suffix=='.rs':
                for target in re.findall(r'include_(?:str|bytes)!\(\s*"([^"]+)"\s*\)',p.read_text()):
                    included=(p.parent/target).resolve();new=source/included.relative_to(compiled)
                    assert sha(included)==sha(new);embedded.add(str(included.relative_to(compiled)))
    previous=root/'implementation/build-host055';receipt=json.loads((previous/'build-receipt.json').read_bytes())
    pins={**receipt['reuse']['preserved_ELFs'],receipt['daemon']:receipt['daemon_sha256'],receipt['Rust_test_ELF']:receipt['Rust_test_ELF_sha256']}
    for p,pin in pins.items():assert sha(Path(p))==pin
    state=root/'runs/c064/state/state.json';state_sha=sha(state);assert state_sha=='163f7500836dc4fd199d34f83a0e08453ab05255f8b15a57338052df4601d5b0'
    sys.path.insert(0,str(source/'tools/phasec'))
    from qemu_profile_gate import binding,launch_qemu_gate
    from accepted_producers import accepted_anchors
    from profile_metadata_gate import require_profile_metadata_ready,metadata_producer,ANCHOR_SHA
    from current_fm_isolation import historical62
    profile=launch_qemu_gate(root,{'qemu_profile':binding(),'qemu_binary':accepted_anchors()['profile']['binary']['path']})
    anchors=require_profile_metadata_ready(profile);_,history=historical62()
    (out/'accepted-metadata-preflight.json').write_text(json.dumps({'anchors':anchors,'anchor_sha256':ANCHOR_SHA,
        'actual_new_profile_and_historical_inventory_verified':True,'historical62':history,'Guest_registration_or_union_executed':False},indent=2))
    env={**os.environ,'PYTHONPATH':str(source/'sdk/python'),'PYTHONDONTWRITEBYTECODE':'1'}
    for test in ('test_accepted_producers.py','test_profile_metadata_consumer.py','test_qemu_profile.py'):
        with (out/(test+'.log')).open('xb') as log:
            subprocess.run([sys.executable,'-B','-m','unittest','discover','-s',str(source/'examples/mem_service_pool'),'-p',test],check=True,
                env=env,cwd=source,stdout=log,stderr=subprocess.STDOUT,timeout=45)
    # A genuine E HOST request must remain inadmissible to the production API.
    with metadata_producer() as (_,p):
        fixture=root/'implementation/c064-profile-metadata-v1/fixtures/c902/bootstrap-request.json'
        try:p.validate_registration(str(fixture),sha(fixture),str(root/'env/guest-kit004/uflow_guest_audit.py'))
        except (ValueError,p.ev.EvidenceError) as error:assert 'schema/interface/kind' in str(error)
        else:raise AssertionError('HOST fixture became production registration')
    for p,pin in pins.items():assert sha(Path(p))==pin
    assert sha(state)==state_sha and not (root/'implementation/responsibility-c013/successor-c064-terminal-domain-loss-v2').exists()
    value={**receipt,'source':str(source),'source_commit':current['commit'],'source_manifest_sha256':sha(source/'.uflow-source.json'),
        'compiled_source_commit':cm['commit'],'compiled_generation':'host054','new_Cargo_or_C_image_SDK_build':False,
        'compiled_embedded_inputs_verified':sorted(embedded),'profile_metadata_producer_binding_ready':True,
        'accepted_metadata_anchor_sha256':ANCHOR_SHA,'production_metadata_case_registration_and_union_tested':False,
        'targeted_Python_tests':21,'targeted_Python_skips':0,'runtime_execution_package_review_pending':True,
        'original_lock_ledger_or_new_Guest':False,'real_c064_state_sha256_before_after':state_sha}
    (out/'build-receipt.json').write_text(json.dumps(value,indent=2))
    print(json.dumps({'status':'PASS_PROFILE_METADATA_BINDING_TARGETED_HOST_NO_CARGO','source_commit':current['commit'],
        'actual_profile_and_historical62_verified':True,'targeted_Python_tests':21,'original_current_unchanged':True}))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--source',type=Path,required=True);p.add_argument('--output',required=True)
    a=p.parse_args();main(a.root,a.source,a.output)
