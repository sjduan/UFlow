"""New production-bound daemon, affected HOST tests, real read-only E APIs.
No original authority lock/write, Guest, real QMP, device or normal registry.
"""
import argparse,hashlib,json,os,shutil,subprocess,sys
from pathlib import Path
from accepted_c081_v3 import v3_anchors,v3_sources,v3_context,v3_terminal_proof,v3_policy

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main(root,source,name):
    generation=name[-3:];assert str(root)=='/workspace/uflow/runs/mem-service-shared-pool-20261108' and name.startswith('build-host') and generation.isdigit() and len(name)==13 and source==root/'implementation'/('source-host'+generation)
    assert 'UFLOW_ENV_CLOSE_FD' in os.environ
    out=root/'implementation'/name;out.mkdir(exist_ok=False)
    manifest=json.loads((source/'.uflow-source.json').read_bytes())
    for rel,row in manifest['files'].items():
        path=source/rel;assert path.is_file() and not path.is_symlink() and sha(path)==row['sha256'] and path.stat().st_mode&0o777==int(row['mode'][-3:],8)
    state=root/'runs/c081/state/state.json';state_pin=sha(state);assert state_pin==v3_policy()['state_sha256']
    authority=root/'implementation/responsibility-c013';assert not (authority/v3_policy()['successor']).exists()
    real={str(p):sha(p) for n in ['successor-c044-terminal-domain-loss-v1','successor-c064-terminal-domain-loss-v2'] for p in (authority/n).iterdir()};assert len(real)==15
    policy=v3_policy();assert not Path(policy['config_directory']).exists()
    for job in policy['phases'].values():assert not (root/'runs'/job).exists()
    image=json.loads((root/'implementation/build-host066/build-receipt.json').read_bytes());assert image['pass'] and image['image_sha256']=='5c00fbbd5359ce8bf44876f8a610c579a7eef9cefe62c56c8009c49ba284539f'
    assert sha(Path(image['image']))==image['image_sha256']
    for rel,pin in image['installed_sdk_manifest'].items():assert sha(root/'env/mem-service-sdk'/rel)==pin
    for path,row in image['official_worker_reused']['source_and_headers_link_inputs'].items():assert sha(Path(path))==row['sha256']
    cbase=root/'implementation/build-host046';base=json.loads((cbase/'build-receipt.json').read_bytes())
    cargo=shutil.which('cargo') or '/root/.cargo/bin/cargo';target=root/'implementation'/('cargo-host'+generation);assert not target.exists()
    env={**os.environ,'PATH':str(Path(cargo).parent)+os.pathsep+os.environ.get('PATH',''),'CARGO_INCREMENTAL':'0','CARGO_BUILD_JOBS':'4',
        'UF_ACL_SHIM_LIB_DIR':str(cbase/'fence'),'LD_LIBRARY_PATH':str(cbase/'fence'),'PYTHONPATH':str(source/'sdk/python'),'PYTHONDONTWRITEBYTECODE':'1'}
    commands=[]
    def run(argv,label,timeout=90):
        commands.append({'argv':list(map(str,argv)),'label':label})
        with (out/(label+'.log')).open('xb') as log:subprocess.run(list(map(str,argv)),cwd=source,env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=timeout)
    run([cargo,'build','--locked','-p','uf-daemon','--target-dir',target,'-j','4'],'production-Rust-build',1200)
    run([cargo,'test','--locked','-p','uf-daemon','--target-dir',target,'-j','4','--no-run','--message-format=json'],'production-test-build',1200)
    rows=[json.loads(s) for s in (out/'production-test-build.log').read_text().splitlines() if s.startswith('{')]
    tests=[Path(v['executable']) for v in rows if v.get('reason')=='compiler-artifact' and v.get('target',{}).get('name')=='uf-daemon' and v.get('profile',{}).get('test') and v.get('executable')];assert len(tests)==1;rust=tests[0]
    for scope in ['pool_terminal_domain::case_chain::','managed_read_cache::tests::','mem_service_pool::tests::','pool_ranges::tests::','managed_pool::tests::confirmed_unmap_closes_catalog_unknown_and_keeps_content_invalid']:
        run([rust,scope,'--test-threads=4','--nocapture'],scope.replace(':','_'))
    for pattern in ['test_daemon_peer_auth.py','test_managed_cache_control.py','test_managed_interface_v2.py','test_managed_wrapper_connection.py','test_case_chain_gate.py','test_c081_production_gate.py']:
        run([sys.executable,'-B','-m','unittest','discover','-s',source/'examples/mem_service_pool','-p',pattern],pattern)
    # The actual source-defined E APIs are read-only here, not HOST substitutes.
    a=v3_anchors();modules=v3_sources(a);anchor,profile,recorded=v3_context(a,modules);proof=v3_terminal_proof(a)
    assert proof['validated_candidate']==recorded
    (out/'actual-accepted-v6-readonly-proof.json').write_text(json.dumps(proof,indent=2))
    daemon=out/'uf-daemon'
    with (target/'debug/uf-daemon').open('rb') as src,daemon.open('xb') as dst:shutil.copyfileobj(src,dst);dst.flush();os.fsync(dst.fileno())
    daemon.chmod(0o755);assert daemon.stat().st_nlink==1
    run([daemon,'--pool-terminal-case-verify'],'actual-compiled-v6-readonly-verify',90)
    assert json.loads((out/'actual-compiled-v6-readonly-verify.log').read_bytes())==proof
    run([sys.executable,'-B',source/'examples/mem_service_pool/matrix_entry_cpu.py','--chain-terminal-domain',
        '--agent',root/'implementation/build-host066/cpu-agent','--worker',cbase/'cpu-worker','--controller',root/'env/mem-service-sdk/bin/linqu_mem_service',
        '--scratch',root/('z'+generation),'--output',out/'authenticated-three-generation-CPU-matrix','--rust-tests',rust,'--source',source,'--image',cbase/'pool-agent','--shim',cbase/'fence/libuf_acl_shim.so'],
        'authenticated-three-generation-CPU-matrix',900)
    assert sha(state)==state_pin and not (authority/policy['successor']).exists()
    for path,pin in real.items():assert sha(Path(path))==pin
    for rel,pin in image['installed_sdk_manifest'].items():assert sha(root/'env/mem-service-sdk'/rel)==pin
    value={**base,'kind':'CManagedProductionDaemonHostBuildV3','pass':True,'source':str(source),'source_commit':manifest['commit'],
        'source_manifest_sha256':sha(source/'.uflow-source.json'),'daemon':str(daemon),'daemon_sha256':sha(daemon),'daemon_bytes':daemon.stat().st_size,
        'daemon_single_link':True,'managed_cache_enabled':True,'accepted_context':a['accepted_context'],'consumer_anchor_sha256':proof['consumer_anchor_sha256'],
        'image':image['image'],'image_sha256':image['image_sha256'],'installed_sdk_manifest':image['installed_sdk_manifest'],
        'official_worker':image['official_worker_reused'],'Rust_test_ELF':str(rust),'Rust_test_ELF_sha256':sha(rust),'commands':commands,
        'actual_E_v6_APIs_and_new_compiled_readonly_verify':True,'original_c081_sha256_before_after':state_pin,'new_Guest_real_QMP_authority_write_device':False,
        'matrix':json.loads((out/'authenticated-three-generation-CPU-matrix/results.json').read_bytes())}
    (out/'build-receipt.json').write_text(json.dumps(value,indent=2));print(json.dumps({'pass':True,'daemon_sha256':value['daemon_sha256'],'source_commit':manifest['commit']}))
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--source',type=Path,required=True);p.add_argument('--output',required=True);a=p.parse_args();main(a.root,a.source,a.output)
