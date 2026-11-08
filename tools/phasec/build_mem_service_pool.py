#!/usr/bin/env python3
"""Build only in the approved B job; installed neutral SDK + frozen platform TU.

This script has no SSH/upload path. Invoke under E0 runner kind=build, max4workers.
It does not start Guest, daemon, pool or device and never changes lower sources.
"""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

PIN = '546985e26bce60798fa3f88329c3fbed8350c798'
OBMM = '53011eed10716b422d2ac29199f68b55f7c5bdc5'
SUPERVISOR_ROOT = '/workspace/uflow/runs/mem-service-shared-pool-20261108'


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build(root, source, name):
    if str(root) != SUPERVISOR_ROOT or 'UFLOW_ENV_CLOSE_FD' not in os.environ:
        raise RuntimeError('explicit task root and accepted independent runner required')
    if not source.is_relative_to(root / 'implementation'):
        raise ValueError('C task source root only')
    sdk = root / 'env/mem-service-sdk'
    sdk_before = {str(p.relative_to(sdk)):digest(p) for p in sdk.rglob('*') if p.is_file()}
    if len(sdk_before)!=53:raise RuntimeError('exact E0 SDK53 layout required')
    lower = root / 'src/ub_sim/mem_service'
    out = root / 'implementation' / name
    out.mkdir(exist_ok=False)
    manifest=json.loads((source/'.uflow-source.json').read_bytes())
    if manifest['base']!='8eac5c22446e935d71598271c1d1bdf340cda237':
        raise RuntimeError('required C02+D1+M1 base missing')
    for rel,row in manifest['files'].items():
        path=source/rel
        if not path.resolve().is_relative_to(source) or path.is_symlink() or digest(path)!=row['sha256']:
            raise RuntimeError('exact delivered source mismatch: '+rel)
    def run(argv, **kwargs):
        subprocess.run(list(map(str, argv)), check=True, timeout=kwargs.pop('timeout', 120), **kwargs)
    def git(path, *argv):
        return subprocess.check_output(['git', '-C', str(path), *argv]).decode().strip()
    if git(lower, 'rev-parse', 'HEAD') != PIN or git(lower, 'status', '--porcelain', '-uno'):
        raise RuntimeError('frozen mem_service pin/clean check failed')
    if git(lower / 'vendor/obmm', 'rev-parse', 'HEAD') != OBMM:
        raise RuntimeError('frozen libobmm pin mismatch')
    inputs = []
    for name in ('mem_service_client.c','mem_service_wire_client.c','mem_service_provider.c','mem_service_mapping_owner.c'):
        path = sdk / 'src/lingqu/mem_service' / name
        blob = subprocess.check_output(['git','-C',str(lower),'show', PIN+':components/mem_service/'+name])
        if hashlib.sha256(blob).hexdigest() != digest(path):
            raise RuntimeError('installed SDK/Git mismatch: '+name)
        inputs.append(path)
    inputs.extend([lower / 'components/mem_service/providers/mem_service_provider_obmm.c',
                   lower / 'vendor/obmm/src/libobmm/libobmm.c', lower / 'common/obmm_vendor_adaptor_sim.c'])
    own = source / 'crates/uf-ub-sim-agent/c'
    agent_inputs = [own / name for name in ('pool_agent.c','pool_client.c','pool_platform_obmm.c')]
    includes = [sdk/'include/lingqu/mem_service',root/'env/kernel-uapi/include',own,lower,
                lower/'libs/obmm_queue',lower/'vendor/obmm/src/libobmm',
                lower/'kernel_ub/include/uapi',lower/'kernel_ub/include']
    # CPU facade contract with the real neutral SDK and explicit endpoint doubles.
    # Compiled/run only inside this admitted B build; never labelled real OBMM.
    platform_test=out/'platform-contract'
    run(['/usr/bin/gcc','-O0','-Wall','-Wextra']+['-I'+str(p) for p in includes]+[
        source/'examples/mem_service_pool/test_platform.c',sdk/'src/lingqu/mem_service/mem_service_provider.c',
        '-o',platform_test])
    run([platform_test])
    compiler = ['/usr/local/bin/aarch64-linux-gnu-gcc','-O2','-Wall','-Wextra','-static','-pthread','-D__EXPORTED_HEADERS__']
    command = compiler + ['-I'+str(p) for p in includes] + [str(p) for p in agent_inputs+inputs] + ['-o',str(out/'pool-agent')]
    run(command)
    binary = (out/'pool-agent').read_bytes()
    if binary[:4] != b'\x7fELF' or int.from_bytes(binary[18:20],'little') != 183:
        raise RuntimeError('actual aarch64 ELF required')
    shim = out/'fence';shim.mkdir()
    run(['/usr/bin/gcc','-shared','-fPIC',source/'examples/managed_pool/api_fence.c','-o',shim/'libuf_acl_shim.so'])
    env = {**os.environ,'CARGO_BUILD_JOBS':'4','UF_ACL_SHIM_LIB_DIR':str(shim),
           'LD_LIBRARY_PATH':str(shim),'PYTHONDONTWRITEBYTECODE':'1','PYTHONPATH':str(source/'sdk/python')}
    # No libascend/libnpu is linked into this explicitly fail-closed host artifact.
    run(['cargo','build','--locked','-p','uf-daemon','--target-dir',out/'cargo','-j','4'],cwd=source,env=env,timeout=1800)
    run(['cargo','test','--locked','-p','uf-core','--target-dir',out/'cargo','-j','4'],cwd=source,env=env,timeout=300)
    for group in ('ddr_resources::tests','pool_ranges::tests','managed_pool::tests','mem_service_pool::tests'):
        run(['cargo','test','--locked','-p','uf-daemon','--target-dir',out/'cargo','-j','4',group],cwd=source,env=env,timeout=300)
    run([sys.executable,'-B','-m','unittest','discover','-s',source/'examples/mem_service_pool','-p','test_*.py'],env=env)
    run([sys.executable,'-B','-m','unittest','discover','-s',source/'examples/managed_pool','-p','test_sdk.py'],env=env)
    run([sys.executable,'-B','-m','unittest','discover','-s',source/'examples/common_session','-p','test_*.py'],env=env)
    run([sys.executable,'-B','-m','unittest','discover','-s',source/'examples/ddr_v0','-p','test_t34_isolation.py'],env=env)
    base=root/'src/ub_sim/guest-linux/aarch64/out/initramfs.cpio.gz'
    stage=out/'stage';stage.mkdir()
    with gzip.open(base,'rb') as archive:
        run(['cpio','-idm','--quiet','--no-absolute-filenames'],cwd=stage,input=archive.read())
    for name,p in {'bin/run_app':root/'env/guest-kit004/uflow_env_guest_entry.sh',
                   'bin/uflow_env_payload':out/'pool-agent'}.items():
        shutil.copy2(p,stage/name);(stage/name).chmod(0o755)
    # Keep standard linqu_init --resume and existing official managed worker.
    if not (stage/'bin/linqu_mem_service_provider_obmm').is_file():
        raise RuntimeError('official standard managed worker absent')
    members=['.']+[str(p.relative_to(stage)) for p in stage.rglob('*')]
    if len(members)>10000:
        raise RuntimeError('bounded standard image overlay enumeration exceeded')
    packed=out/'overlay.cpio'
    with packed.open('xb') as f:
        run(['cpio','-o','-H','newc','--quiet','--null'],cwd=stage,input=('\0'.join(members)+'\0').encode(),stdout=f)
    image=out/'pool-agent.cpio.gz'
    with packed.open('rb') as src,gzip.open(image,'xb',compresslevel=6) as dest:
        shutil.copyfileobj(src,dest)
    daemon=out/'cargo/debug/uf-daemon'
    receipt={'schema':2,'source_commit':manifest['commit'],'source_manifest_sha256':digest(source/'.uflow-source.json'),
             'compiler_argv':command,'agent_sha256':digest(out/'pool-agent'),
             'image':str(image),'image_sha256':digest(image),'base_sha256':digest(base),
             'source_inputs':{str(p):{'bytes':p.stat().st_size,'sha256':digest(p)} for p in agent_inputs+inputs},
             'fence_shim':str(shim/'libuf_acl_shim.so'),'fence_shim_sha256':digest(shim/'libuf_acl_shim.so'),
             'daemon':str(daemon),'daemon_sha256':digest(daemon),'installed_sdk_manifest':sdk_before,
             'source':str(source),'no_business_runtime_executed':True,
             'sdk53_unchanged':sdk_before=={str(p.relative_to(sdk)):digest(p) for p in sdk.rglob('*') if p.is_file()},
             'frozen_mem_service_unchanged':not git(lower,'status','--porcelain','-uno')}
    if not receipt['sdk53_unchanged'] or not receipt['frozen_mem_service_unchanged']:
        raise RuntimeError('frozen source/install drift after build')
    (out/'build-receipt.json').write_text(json.dumps(receipt,indent=2))
    print(json.dumps(receipt,indent=2))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True)
    p.add_argument('--source',type=Path,required=True);p.add_argument('--output',default='build001')
    a=p.parse_args()
    try:build(a.root.resolve(),a.source.resolve(),a.output)
    except BaseException:
        if 'UFLOW_ENV_CLOSE_FD' in os.environ:
            try:os.write(int(os.environ['UFLOW_ENV_CLOSE_FD']),b'!')
            except BlockingIOError:pass
        raise
