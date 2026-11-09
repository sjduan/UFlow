"""Targeted HOST validation with explicitly reused HOST041 compiled outputs.

No compiler, Cargo, Guest or production terminal verifier is run. The old
failed job stays failed. A new source/receipt names the actual two Python
changes and hashes every unchanged source/SDK/worker/ELF input before reuse.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import traceback

ROOT='/workspace/uflow/runs/mem-service-shared-pool-20261108'
COMPILED_COMMIT='786dbef80119298ae5a7a36fad23f10fff6b0986'
RUNTIME_CHANGES={'tools/phasec/run_mem_service_pool_matrix.py','examples/mem_service_pool/matrix_entry_cpu.py'}
NEW_FILE='tools/phasec/reuse_host_pool_cpu.py'


def sha(path):
    h=hashlib.sha256()
    with path.open('rb') as stream:
        for part in iter(lambda:stream.read(1<<20),b''):h.update(part)
    return h.hexdigest()


def main(root,source,name):
    if str(root)!=ROOT or 'UFLOW_ENV_CLOSE_FD' not in os.environ or not source.is_relative_to(root/'implementation') or not re.fullmatch(r'build-host\d{3}',name):
        raise RuntimeError('admitted own task HOST scope required')
    out=root/'implementation'/name;out.mkdir(exist_ok=False)
    old_source=root/'implementation/source-host041';old=root/'implementation/build-host041'
    before=json.loads((old_source/'.uflow-source.json').read_bytes());current=json.loads((source/'.uflow-source.json').read_bytes())
    if before['commit']!=COMPILED_COMMIT or len(before['files'])!=232 or set(current['files'])!=set(before['files'])|{NEW_FILE}:
        raise RuntimeError('exact previous compiled source and one new runtime-only validation script required')
    changed=[];unchanged={}
    for rel,record in before['files'].items():
        actual=current['files'][rel]
        for base,entry in ((old_source,record),(source,actual)):
            p=base/rel
            if p.is_symlink() or not p.is_file() or sha(p)!=entry['sha256'] or p.stat().st_mode&0o777!=int(entry['mode'][-3:],8):
                raise RuntimeError('source/mode drift: '+rel)
        if actual!=record:
            if rel not in RUNTIME_CHANGES:raise RuntimeError('compiled input change requires a new compile: '+rel)
            changed.append(rel)
        else:unchanged[rel]=record['sha256']
    if set(changed)!=RUNTIME_CHANGES:raise RuntimeError('only the two reviewed Python matrix files may change')
    for rel in before['files']:
        if rel.startswith('crates/') and Path(rel).suffix in ('.rs','.c','.h'):
            text=(old_source/rel).read_text()
            if any(Path(name).name in text and ('include_str!' in text or 'include_bytes!' in text or '#include' in text) for name in RUNTIME_CHANGES):
                raise RuntimeError('changed Python may be embedded in a compiled input: '+rel)
    status=json.loads((root/'runs/cb051/result.json').read_bytes())['processes']
    if status['actual_task_exit_code']!=1 or not status['process_tree_empty'] or not status['task_resources_closed'] or status['remaining'] or status['evidence_errors']:
        raise RuntimeError('reuse requires original failed job already closed')
    baseline=json.loads((root/'implementation/build-host039/build-receipt.json').read_bytes())
    sdk=root/'env/mem-service-sdk';sdk_actual={p.relative_to(sdk).as_posix():sha(p) for p in sdk.rglob('*') if p.is_file()}
    if len(sdk_actual)!=53 or sdk_actual!=baseline['installed_sdk_manifest']:raise RuntimeError('frozen SDK53 drift')
    worker=json.loads((old/'official-worker/receipt.json').read_bytes())
    if len(worker['source_and_headers_link_inputs'])!=233:raise RuntimeError('worker233 closure')
    for path,row in worker['source_and_headers_link_inputs'].items():
        p=Path(path)
        if p.is_symlink() or sha(p)!=row['sha256']:raise RuntimeError('official worker compile/link input drift: '+path)
    if sha(Path(worker['ELF']))!=worker['ELF_sha256']:raise RuntimeError('worker ELF drift')
    artifacts=[json.loads(s) for s in (old/'test-artifacts.jsonl').read_text().splitlines()]
    tests=[Path(v['executable']) for v in artifacts if v.get('reason')=='compiler-artifact' and v.get('target',{}).get('name')=='uf-daemon' and v.get('profile',{}).get('test') and v.get('executable')]
    if len(tests)!=1:raise RuntimeError('exact existing Rust test ELF required')
    rust=tests[0]
    if not rust.is_relative_to(root/'implementation/cargo-host041/debug/deps'):raise RuntimeError('fixed compiled test output path')
    binaries=[old/'cpu-agent',old/'cpu-worker',old/'pool-agent',old/'pool-guest-init',old/'cpu-guest-init',
              root/'implementation/cargo-host041/debug/uf-daemon',rust,Path(worker['ELF']),old/'fence/libuf_acl_shim.so']
    binary_pins={str(p):{'bytes':p.stat().st_size,'sha256':sha(p)} for p in binaries}
    for p in binaries:
        if p.is_symlink() or p.read_bytes()[:4]!=b'\x7fELF':raise RuntimeError('existing ordinary ELF only')
    steps=out/'reuse-steps.jsonl'
    env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1','PYTHONPATH':str(source/'sdk/python'),
         'LD_LIBRARY_PATH':str(old/'fence'),'CARGO_INCREMENTAL':'0'}
    def run(argv,**kwargs):
        argv=list(map(str,argv))
        with steps.open('a') as f:f.write(json.dumps({'phase':'start','argv':argv})+'\n')
        try:subprocess.run(argv,check=True,timeout=kwargs.pop('timeout',120),env=kwargs.pop('env',env),**kwargs)
        except BaseException:
            try:os.write(int(os.environ['UFLOW_ENV_CLOSE_FD']),b'!')
            except BlockingIOError:pass
            (out/'failure.json').write_text(json.dumps({'argv':argv,'traceback':traceback.format_exc()},indent=2));raise
        with steps.open('a') as f:f.write(json.dumps({'phase':'completed','argv':argv,'exit':0})+'\n')
    generation=source.name.removeprefix('source-host');fixture=source/'examples/mem_service_pool'
    for terminal,prefix,output in [(False,'m','matrix-entry-cpu'),(True,'t','terminal-staged-matrix-cpu')]:
        run([sys.executable,'-B',fixture/'matrix_entry_cpu.py',*(['--terminal-domain'] if terminal else []),
            '--agent',old/'cpu-agent','--worker',old/'cpu-worker','--controller',sdk/'bin/linqu_mem_service',
            '--scratch',root/(prefix+generation),'--output',out/output,'--rust-tests',rust,'--source',source,
            '--image',old/'pool-agent','--shim',old/'fence/libuf_acl_shim.so'],timeout=600)
    short=root/('e'+generation);short.mkdir(exist_ok=False)
    for case in ('managed_creation_and_external_memfd_share_catalog_capacity_and_close_witness',
                 'independent_owner_processes_compete_for_one_catalog_budget','t1_registration_and_clean_drain_use_the_fixture_authority'):
        run([rust,'--exact','external_ddr::managed_tests::'+case,'--nocapture','--test-threads=1'],
            env={**env,'CARGO_TARGET_DIR':str(short)},cwd=source)
    for directory,pattern in [('mem_service_pool','test_*.py'),('managed_pool','test_sdk.py'),('common_session','test_*.py'),('ddr_v0','test_t34_isolation.py')]:
        run([sys.executable,'-B','-m','unittest','discover','-s',source/'examples'/directory,'-p',pattern],cwd=source)
    for path,row in binary_pins.items():
        if sha(Path(path))!=row['sha256']:raise RuntimeError('reused ELF changed during validation')
    receipt={'schema':1,'pass':True,'validation_source_commit':current['commit'],'validation_source_manifest_sha256':sha(source/'.uflow-source.json'),
        'compiled_from_commit':COMPILED_COMMIT,'compiled_from_generation':'host041','original_cb051_exit_unchanged':1,
        'new_compilation_or_Cargo_invoked':False,'unchanged_source_files':unchanged,'changed_runtime_only_sources':changed,
        'reused_ELFs':binary_pins,'SDK53_unchanged':sdk_actual,'worker233_receipt_sha256':sha(old/'official-worker/receipt.json'),
        'matrix_entry_cpu':json.loads((out/'matrix-entry-cpu/results.json').read_bytes()),
        'terminal_staged_matrix_cpu':json.loads((out/'terminal-staged-matrix-cpu/results.json').read_bytes()),
        'production_ready':False,'E_accepted_pins_and_real_adapter_missing':True,'production_lock_or_ledger_or_verifier_or_new_Guest_called':False,
        'source':str(source),'output':str(out)}
    (out/'reuse-receipt.json').write_text(json.dumps(receipt,indent=2));print(json.dumps(receipt,indent=2))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--source',type=Path,required=True);p.add_argument('--output',required=True)
    a=p.parse_args()
    try:main(a.root.resolve(),a.source.resolve(),a.output)
    except BaseException:
        if 'UFLOW_ENV_CLOSE_FD' in os.environ:
            try:os.write(int(os.environ['UFLOW_ENV_CLOSE_FD']),b'!')
            except BlockingIOError:pass
        raise
