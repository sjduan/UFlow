"""Python-only HOST validation over the exact accepted HOST047 ELF closure.

No compiler, real Guest, original authority mutation or SDK drain is run.
The c064 replay is metadata-only, retaining the original FAIL/Unknown.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = '/workspace/uflow/runs/mem-service-shared-pool-20261108'
BASE = '2c7a15341ad5251708930e04076585761e5a92db'
CHANGED = {'tools/phasec/mem_service_pool_bridge.py', 'tools/phasec/pool_run_metadata.py',
           'examples/mem_service_pool/matrix_entry_cpu.py'}
ADDED = {'tools/phasec/validate_panic_host.py', 'examples/mem_service_pool/test_guest_panic.py'}


def sha(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda:f.read(1<<20),b''):h.update(chunk)
    return h.hexdigest()


def main(root, source, name):
    assert str(root)==ROOT and 'UFLOW_ENV_CLOSE_FD' in os.environ and name=='build-host048'
    out=root/'implementation'/name;out.mkdir(exist_ok=False)
    old_source=root/'implementation/source-host047'
    before=json.loads((old_source/'.uflow-source.json').read_bytes())
    current=json.loads((source/'.uflow-source.json').read_bytes())
    assert before['commit']==BASE and len(before['files'])==239
    assert set(current['files'])==set(before['files'])|ADDED
    changed=set()
    for rel,row in current['files'].items():
        path=source/rel
        assert not path.is_symlink() and sha(path)==row['sha256'] and path.stat().st_mode&0o777==int(row['mode'][-3:],8)
        if rel in before['files']:
            assert sha(old_source/rel)==before['files'][rel]['sha256']
            if row!=before['files'][rel]:changed.add(rel)
    assert changed==CHANGED
    for rel in before['files']:
        if rel.startswith('crates/') and Path(rel).suffix in ('.rs','.h','.c'):
            text=(old_source/rel).read_text()
            assert not any(Path(p).name in text and ('include_str!' in text or 'include_bytes!' in text or '#include' in text) for p in CHANGED|ADDED)
    base=root/'implementation/build-host047'; cbase=root/'implementation/build-host046'
    assert sha(base/'build-receipt.json')=='08f61e09795cf1aaf7a933d223c1f05ce3bf47cd4c43091b446fb3c3bc5bc09a'
    receipt=json.loads((base/'build-receipt.json').read_bytes())
    for key,pin in [('image','image_sha256'),('fence_shim','fence_shim_sha256'),('daemon','daemon_sha256')]:assert sha(Path(receipt[key]))==receipt[pin]
    assert len(receipt['installed_sdk_manifest'])==53
    sdk=root/'env/mem-service-sdk'
    for rel,pin in receipt['installed_sdk_manifest'].items():assert sha(sdk/rel)==pin
    inputs=receipt['official_worker']['source_and_headers_link_inputs'];assert len(inputs)==233
    for path,row in inputs.items():assert sha(Path(path))==row['sha256']
    rust=root/'implementation/cargo-host047/debug/deps/uf_daemon-0f10ee78cd381af7'
    binaries=[rust,base/'uf-daemon',cbase/'cpu-agent',cbase/'cpu-worker',cbase/'pool-agent',
              Path(receipt['image']),Path(receipt['fence_shim']),Path(receipt['official_worker']['ELF'])]
    pins={str(p):sha(p) for p in binaries}
    state=root/'runs/c064/state/state.json';state_sha='163f7500836dc4fd199d34f83a0e08453ab05255f8b15a57338052df4601d5b0'
    assert sha(state)==state_sha
    for job,exit_code in [('c063',0),('c064',1),('c065',1)]:
        p=json.loads((root/'runs'/job/'result.json').read_bytes())['processes']
        assert p['actual_task_exit_code']==exit_code and p['process_tree_empty'] and p['task_resources_closed'] and not p['remaining'] and not p['evidence_errors']
    env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1','PYTHONPATH':str(source/'sdk/python'),
         'LD_LIBRARY_PATH':str(cbase/'fence')}
    def run(argv, label, timeout=300):
        with (out/(label+'.log')).open('xb') as log:
            subprocess.run(list(map(str,argv)),cwd=source,env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=timeout)
    run([sys.executable,'-B','-m','unittest','discover','-s',source/'examples/mem_service_pool','-p','test_*.py'],'python-pool-contracts')
    fixture=source/'examples/mem_service_pool/matrix_entry_cpu.py'
    for panic, short, label in [(False,'p048','normal-matrix'),(True,'f048','panic-Unknown-matrix')]:
        run([sys.executable,'-B',fixture,'--terminal-domain',*(['--panic-on-first-write'] if panic else []),
             '--agent',cbase/'cpu-agent','--worker',cbase/'cpu-worker','--controller',sdk/'bin/linqu_mem_service',
             '--scratch',root/short,'--output',out/label,'--rust-tests',rust,'--source',source,
             '--image',cbase/'pool-agent','--shim',cbase/'fence/libuf_acl_shim.so'],label,600)
    # Exercise actual c064 serial capture + strict unchanged E admission. A new
    # immutable namespace stores both node diagnostics, never a fake registry.
    sys.path.insert(0,str(source/'tools/phasec'))
    from pool_run_metadata import complete_run
    directory=root/'implementation/qemu-domain-v1/consumer-phase-c/host048-c064-journal-diag-v1'
    try:complete_run(root/'runs/c064',directory)
    except Exception as error:
        assert 'complete actual worker serial journal required' in str(error),repr(error)
    else:raise AssertionError('missing nodeA journal admitted')
    diagnostics=json.loads((directory/'journal-completeness.json').read_bytes())
    assert set(p.name for p in directory.iterdir())=={'journal-completeness.json'}
    assert diagnostics['nodes']['nodeA']['status']=='missing-serial-records'
    assert not diagnostics['nodes']['nodeA']['complete'] and diagnostics['nodes']['nodeB']['complete']
    assert diagnostics['nodes']['nodeB']['bytes']==1024 and not diagnostics['all_serial_captures_complete']
    (out/'actual-c064-journal-completeness.json').write_bytes((directory/'journal-completeness.json').read_bytes())
    assert sha(state)==state_sha
    for path,pin in pins.items():assert sha(Path(path))==pin
    result={'schema':1,'pass':True,'validation_source_commit':current['commit'],
       'compiled_source_commit':BASE,'new_compile_or_Guest_or_original_ledger_mutation':False,
       'source_manifest_sha256':sha(source/'.uflow-source.json'),'changed_runtime_Python':sorted(CHANGED),'added':sorted(ADDED),
       'reused_binary_sha256':pins,'SDK53_and_worker233_unchanged':True,'c064_state_sha256_before_after':state_sha,
       'normal_matrix':json.loads((out/'normal-matrix/results.json').read_bytes()),
       'panic_Unknown_matrix':json.loads((out/'panic-Unknown-matrix/results.json').read_bytes()),
       'actual_c064_metadata_admission':'REJECTED_INCOMPLETE_AS_REQUIRED','per_node_diagnostics':diagnostics,
       'old_c064_c065_FAIL_retained':True,'new52_responsibility_retained':True}
    (out/'validation-receipt.json').write_text(json.dumps(result,indent=2));print(json.dumps(result))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--source',type=Path,required=True);p.add_argument('--output',required=True)
    a=p.parse_args();main(a.root,a.source,a.output)
