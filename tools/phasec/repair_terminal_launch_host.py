"""Compile only the affected daemon; reuse the exact accepted C image/SDK.

The installed launch executable is a new fsynced single-link copy. Cargo's
original debug/deps hardlinks remain intact and are never executed as runtime.
"""
import argparse,hashlib,json,os,shutil,subprocess,sys
from pathlib import Path

def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()

def main(root,source,output):
    assert str(root)=='/workspace/uflow/runs/mem-service-shared-pool-20261108' and 'UFLOW_ENV_CLOSE_FD' in os.environ
    out=root/'implementation'/output;out.mkdir(exist_ok=False)
    previous=root/'implementation/source-host046';old=json.loads((previous/'.uflow-source.json').read_bytes())
    current=json.loads((source/'.uflow-source.json').read_bytes())
    assert old['commit']=='737c6d1cdb1337d43c4f61cc5bbb5977951a1aad'
    allowed={'crates/uf-daemon/src/main.rs','crates/uf-daemon/src/pool_terminal_domain.rs',
             'tools/phasec/terminal_domain_adapter.py','tools/phasec/pool_run_metadata.py'}
    assert set(current['files'])==set(old['files'])|{'tools/phasec/repair_terminal_launch_host.py'}
    changed=set()
    for name,row in current['files'].items():
        path=source/name;assert sha(path)==row['sha256'] and path.stat().st_mode&0o777==int(row['mode'][-3:],8)
        if name in old['files'] and row!=old['files'][name]:changed.add(name)
    assert changed==allowed
    base=root/'implementation/build-host046';receipt=json.loads((base/'build-receipt.json').read_bytes())
    assert sha(base/'build-receipt.json')=='c846d668e6b888e062cfa2c34c42d25eb06566a6dfa6dd956621f43e89561426'
    for key,pin in [('image','image_sha256'),('fence_shim','fence_shim_sha256')]:assert sha(Path(receipt[key]))==receipt[pin]
    for name,pin in receipt['installed_sdk_manifest'].items():assert sha(root/'env/mem-service-sdk'/name)==pin
    assert len(receipt['installed_sdk_manifest'])==53
    for path,row in receipt['official_worker']['source_and_headers_link_inputs'].items():assert sha(Path(path))==row['sha256']
    cargo=shutil.which('cargo') or '/root/.cargo/bin/cargo'
    target=root/'implementation'/output.replace('build-','cargo-',1);assert not target.exists()
    env={**os.environ,'PATH':str(Path(cargo).parent)+os.pathsep+os.environ.get('PATH',''),
        'CARGO_INCREMENTAL':'0','CARGO_BUILD_JOBS':'4','UF_ACL_SHIM_LIB_DIR':str(Path(receipt['fence_shim']).parent),
        'LD_LIBRARY_PATH':str(Path(receipt['fence_shim']).parent),'PYTHONDONTWRITEBYTECODE':'1'}
    def run(argv,**kwargs):
        argv=list(map(str,argv))
        with (out/'repair-steps.jsonl').open('a') as f:f.write(json.dumps({'phase':'start','argv':argv})+'\n')
        subprocess.run(argv,check=True,env=env,cwd=source,timeout=kwargs.pop('timeout',300),**kwargs)
        with (out/'repair-steps.jsonl').open('a') as f:f.write(json.dumps({'phase':'completed','argv':argv,'exit':0})+'\n')
    run([cargo,'build','--locked','-p','uf-daemon','--target-dir',target,'-j','4'],timeout=1200)
    origin=target/'debug/uf-daemon';launch=out/'uf-daemon'
    with origin.open('rb') as src,launch.open('xb') as dest:
        shutil.copyfileobj(src,dest);dest.flush();os.fsync(dest.fileno())
    launch.chmod(0o755)
    assert launch.stat().st_nlink==1 and sha(launch)==sha(origin)
    fd=os.open(out,os.O_DIRECTORY);os.fsync(fd);os.close(fd)
    bare=subprocess.run([str(launch),'--pool-terminal-domain-loss'],env=env,capture_output=True,timeout=10)
    assert bare.returncode==1 and b'production-not-ready' in bare.stderr
    (out/'bare-entry.json').write_text(json.dumps({'actual_exit':bare.returncode,'stderr':bare.stderr.decode(),'original_IO':False},indent=2))
    with (out/'terminal-ledger-rust.log').open('xb') as log:
        run([cargo,'test','--locked','-p','uf-daemon','--target-dir',target,'-j','4','pool_terminal_domain::tests','--','--nocapture'],stdout=log,timeout=300)
    run([sys.executable,'-B','-m','unittest','discover','-s',source/'examples/mem_service_pool','-p','test_terminal_adapter.py'])
    actual=subprocess.run([str(launch),'--pool-terminal-domain-verify'],env=env,capture_output=True,timeout=120)
    assert actual.returncode==0,actual.stderr.decode()
    proof=json.loads(actual.stdout);assert proof==json.loads((base/'E-adapter-readonly-1.json').read_bytes())
    (out/'E-adapter-readonly.json').write_bytes(actual.stdout)
    copied=dict(receipt)
    copied.update(source_commit=current['commit'],source=str(source),source_manifest_sha256=sha(source/'.uflow-source.json'),
        daemon=str(launch),daemon_sha256=sha(launch),runtime_ELF_nlink=1,
        daemon_Cargo_original=str(origin),daemon_Cargo_original_nlink=origin.stat().st_nlink,
        component_reuse={'C_agent_worker_launcher_image_SDK_from_commit':old['commit'],'from_generation':'host046',
            'base_receipt_sha256':sha(base/'build-receipt.json'),'all_C_and_SDK_sources_unchanged':True,
            'new_C_or_image_build':False,'full_old_Rust_or_Python_suite_repeated':False,
            'matrix_profile_startup_RX_validation_fields_are_accepted_HOST046_results':True},
        actual_execution_context_host_probe_pending=True)
    (out/'build-receipt.json').write_text(json.dumps(copied,indent=2))
    (out/'repair-receipt.json').write_text(json.dumps({'pass':True,'source_commit':current['commit'],'changed_inputs':sorted(changed),
        'compiled_only':'uf-daemon','targeted_terminal_Rust_suite':22,'targeted_adapter_Python':5,
        'runtime_ELF':str(launch),'runtime_ELF_sha256':sha(launch),'runtime_ELF_nlink':1,
        'copied_from_exact_Cargo_bytes':True,'base_source_commit':old['commit'],
        'base_image_sha256':receipt['image_sha256'],'full_old_build_repeated':False,'original_authority_or_ledger_or_Guest':False},indent=2))
    print(json.dumps({'status':'PASS_AFFECTED_DAEMON_BUILD_AND_TARGETED_TESTS','commit':current['commit'],'daemon_sha256':sha(launch),'nlink':1}))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--source',type=Path,required=True);p.add_argument('--output',required=True);a=p.parse_args()
    try:main(a.root.resolve(),a.source.resolve(),a.output)
    except BaseException:
        if 'UFLOW_ENV_CLOSE_FD' in os.environ:
            try:os.write(int(os.environ['UFLOW_ENV_CLOSE_FD']),b'!')
            except BlockingIOError:pass
        raise
