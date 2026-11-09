#!/usr/bin/env python3
"""Build only in the approved B job; installed neutral SDK + frozen platform TU.

This script has no SSH/upload path. Invoke under E0 runner kind=build, max4workers.
It runs isolated CPU metadata/ordinary-process fixtures, never Guest, business
backing, OBMM or device, and never changes lower sources or the c013 authority.
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
import traceback

PIN = '546985e26bce60798fa3f88329c3fbed8350c798'
OBMM = '53011eed10716b422d2ac29199f68b55f7c5bdc5'
SUPERVISOR_ROOT = '/workspace/uflow/runs/mem-service-shared-pool-20261108'


def failure_notice():
    if 'UFLOW_ENV_CLOSE_FD' in os.environ:
        try:os.write(int(os.environ['UFLOW_ENV_CLOSE_FD']),b'!')
        except BlockingIOError:pass


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
        argv=list(map(str,argv))
        with (out/'build-steps.jsonl').open('a') as log:
            log.write(json.dumps({'phase':'start','argv':argv})+'\n')
        try:
            result=subprocess.run(argv, check=True, timeout=kwargs.pop('timeout', 120), **kwargs)
        except BaseException:
            failure_notice()  # Before diagnostics; the runner may stop draining stdout on notice.
            (out/'failure.json').write_text(json.dumps({'argv':argv,'traceback':traceback.format_exc()},indent=2))
            raise
        with (out/'build-steps.jsonl').open('a') as log:
            log.write(json.dumps({'phase':'completed','argv':argv,'exit':result.returncode})+'\n')
    def git(path, *argv):
        return subprocess.check_output(['git', '-C', str(path), *argv]).decode().strip()
    generation=name.removeprefix('build-host')
    build_config=json.loads((root/'implementation'/('config-host'+generation)/'build.json').read_bytes())
    if build_config['kind']!='build' or not build_config['job'].startswith('cb'):raise RuntimeError('own admitted build probe root required')
    run([sys.executable,'-B',source/'examples/mem_service_pool/profile_consumer_cpu.py','--root',root,'--source',source,
         '--output',out/'profile-consumer-cpu','--probe-root',root/'runs'/build_config['job']/'profile-consumer-probes'],timeout=180)
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
    neutral_inputs=list(inputs)
    inputs.extend([lower / 'components/mem_service/providers/mem_service_provider_obmm.c',
                   lower / 'vendor/obmm/src/libobmm/libobmm.c', lower / 'common/obmm_vendor_adaptor_sim.c'])
    platform=lower.parent;gva=platform/'guest-linux/aarch64/libs/gva_manager'
    gva_inputs=[gva/'gva_manager.c',gva/'gva_manager.h']
    for path in gva_inputs:
        rel=path.relative_to(platform).as_posix()
        if hashlib.sha256(subprocess.check_output(['git','-C',str(platform),'show',
                'abeec108a1607feb37218d0d89987e37c579d567:'+rel])).hexdigest()!=digest(path):
            raise RuntimeError('locked platform GVA library changed')
    inputs.append(gva/'gva_manager.c')
    own = source / 'crates/uf-ub-sim-agent/c'
    common_inputs=[own/name for name in ('pool_agent.c','pool_client.c','pool_worker.c','pool_agent_io.c')]
    agent_inputs = common_inputs+[own/'pool_platform_obmm.c']
    includes = [sdk/'include/lingqu/mem_service',root/'env/kernel-uapi/include',own,lower,gva,
                lower/'libs/obmm_queue',lower/'vendor/obmm/src/libobmm',
                lower/'kernel_ub/include/uapi',lower/'kernel_ub/include']
    # CPU facade contract with the real neutral SDK and explicit endpoint doubles.
    # Compiled/run only inside this admitted B build; never labelled real OBMM.
    platform_test=out/'platform-contract'
    run(['/usr/bin/gcc','-O0','-Wall','-Wextra']+['-I'+str(p) for p in includes]+[
        source/'examples/mem_service_pool/test_platform.c',sdk/'src/lingqu/mem_service/mem_service_provider.c',
        gva/'gva_manager.c','-Wl,--wrap=open','-Wl,--wrap=close','-Wl,--wrap=ioctl',
        '-o',platform_test])
    run([platform_test])
    compiler = ['/usr/local/bin/aarch64-linux-gnu-gcc','-O2','-Wall','-Wextra','-static','-pthread','-D__EXPORTED_HEADERS__']
    command = compiler + ['-I'+str(p) for p in includes] + [str(p) for p in agent_inputs+inputs] + ['-o',str(out/'pool-agent')]
    run(command)
    launcher=out/'pool-guest-init';launcher_fixture=out/'cpu-guest-init'
    run(compiler+[own/'pool_guest_init.c','-o',launcher])
    run(['/usr/bin/gcc','-O0','-Wall','-Wextra','-DUFLOW_POOL_LAUNCHER_FIXTURE',
         own/'pool_guest_init.c','-o',launcher_fixture])
    from build_pool_worker import build_worker
    official_worker,worker_receipt=build_worker(root,out,run)
    binary = (out/'pool-agent').read_bytes()
    if binary[:4] != b'\x7fELF' or int.from_bytes(binary[18:20],'little') != 183:
        raise RuntimeError('actual aarch64 ELF required')
    fixture=source/'examples/mem_service_pool'
    fixture_inputs=common_inputs+[fixture/'fixture_platform.c']+neutral_inputs
    fixture_agent=out/'cpu-agent';fixture_worker=out/'cpu-worker'
    neutral_includes=['-I'+str(sdk/'include/lingqu/mem_service'),'-I'+str(own)]
    run(['/usr/bin/gcc','-O0','-g','-Wall','-Wextra','-pthread','-DUFLOW_POOL_CPU_FIXTURE']+neutral_includes+
        fixture_inputs+['-o',fixture_agent])
    run(['/usr/bin/gcc','-O0','-g','-Wall','-Wextra','-pthread']+neutral_includes+
        [fixture/'fixture_worker.c']+neutral_inputs+['-o',fixture_worker])
    fixture_output=out/'startup-cpu'
    # Short exclusive Unix socket paths, outside both old authority and sources.
    scratch=root/('v'+out.name.removeprefix('build-host'))
    run([sys.executable,'-B',fixture/'startup_cpu_probe.py','--agent',fixture_agent,'--worker',fixture_worker,
         '--controller',sdk/'bin/linqu_mem_service','--scratch',scratch,'--output',fixture_output],timeout=180)
    shim = out/'fence';shim.mkdir()
    run(['/usr/bin/gcc','-shared','-fPIC',source/'examples/managed_pool/api_fence.c','-o',shim/'libuf_acl_shim.so'])
    cargo=shutil.which('cargo') or '/root/.cargo/bin/cargo'
    if not Path(cargo).is_file():raise RuntimeError('existing Cargo toolchain not found; no implicit install')
    # Core's real Unix-socket fixtures use CARGO_TARGET_DIR.parent(). Keep that
    # parent short enough for Linux SUN_LEN, independently of delivered sources.
    cargo_target=out.parent/out.name.replace('build-','cargo-',1)
    if cargo_target.exists():raise RuntimeError('fresh per-generation Cargo output required')
    env = {**os.environ,'PATH':str(Path(cargo).parent)+os.pathsep+os.environ.get('PATH',''),
           'CARGO_BUILD_JOBS':'4','CARGO_TARGET_DIR':str(cargo_target),'UF_ACL_SHIM_LIB_DIR':str(shim),
           'LD_LIBRARY_PATH':str(shim),'PYTHONDONTWRITEBYTECODE':'1','PYTHONPATH':str(source/'sdk/python')}
    # No libascend/libnpu is linked into this explicitly fail-closed host artifact.
    run([cargo,'build','--locked','-p','uf-daemon','--target-dir',cargo_target,'-j','4'],cwd=source,env=env,timeout=1800)
    run([cargo,'test','--locked','-p','uf-core','--target-dir',cargo_target,'-j','4'],cwd=source,env=env,timeout=300)
    for group in ('ddr_resources::tests','pool_ranges::tests','managed_pool::tests','mem_service_pool::tests','pool_reconciliation::tests','pool_retained8::tests','pool_quarantine76::tests'):
        run([cargo,'test','--locked','-p','uf-daemon','--target-dir',cargo_target,'-j','4',group],cwd=source,env=env,timeout=300)
    # Real Catalog/memfd/independent owner closure regressions, no driver query.
    # The fixture appends .external-ddr to its already nonce-qualified socket.
    # A compact generation label leaves that suffix within Linux SUN_LEN.
    short=root/('x'+format(int(out.name.removeprefix('build-host')),'x'));short.mkdir(exist_ok=False)
    if len(str(short/'br-4194304-1794135978000000000.sock.external-ddr').encode())>=108:
        raise RuntimeError('short E10 fixture path exceeds its full socket suffix budget')
    e10_env={**env,'CARGO_TARGET_DIR':str(short)}
    test_manifest=out/'test-artifacts.jsonl'
    with test_manifest.open('xb') as f:
        run([cargo,'test','--locked','--no-run','--message-format=json','-p','uf-daemon','--target-dir',cargo_target,'-j','4'],
            cwd=source,env=env,stdout=f,timeout=120)
    test_bins=[Path(row['executable']) for row in (json.loads(line) for line in test_manifest.read_text().splitlines())
               if row.get('reason')=='compiler-artifact' and row.get('target',{}).get('name')=='uf-daemon' and
               row.get('profile',{}).get('test') and row.get('executable')]
    if len(test_bins)!=1:raise RuntimeError('exact compiled Rust test executable required')
    run([sys.executable,'-B',fixture/'staged_catalog_cpu.py','--agent',fixture_agent,'--worker',fixture_worker,
         '--controller',sdk/'bin/linqu_mem_service','--scratch',root/('z'+out.name.removeprefix('build-host')),
         '--output',out/'staged-catalog-cpu','--rust-tests',test_bins[0]],env=env,timeout=60)
    run([sys.executable,'-B',fixture/'matrix_entry_cpu.py','--agent',fixture_agent,'--worker',fixture_worker,
         '--controller',sdk/'bin/linqu_mem_service','--scratch',root/('m'+out.name.removeprefix('build-host')),
         '--output',out/'matrix-entry-cpu','--rust-tests',test_bins[0],'--source',source,
         '--image',out/'pool-agent','--shim',shim/'libuf_acl_shim.so'],env=env,timeout=600)
    run([sys.executable,'-B',fixture/'launcher_cpu.py','--launcher',launcher_fixture,
         '--output',out/'launcher-cpu'],env=env,timeout=30)
    run([sys.executable,'-B',fixture/'journal_cpu.py','--launcher',launcher_fixture,
         '--output',out/'journal-cpu'],env=env,timeout=40)
    run([sys.executable,'-B',fixture/'retained8_lock_cpu.py','--output',out/'retained8-admission-cpu'],env=env,timeout=30)
    for case in ('managed_creation_and_external_memfd_share_catalog_capacity_and_close_witness',
                 'independent_owner_processes_compete_for_one_catalog_budget','t1_registration_and_clean_drain_use_the_fixture_authority'):
        run([test_bins[0],'--exact','external_ddr::managed_tests::'+case,
             '--nocapture','--test-threads=1'],cwd=source,env=e10_env,timeout=120)
    run([sys.executable,'-B',fixture/'original_reconciliation_cpu.py'],env=env,timeout=120)
    run([sys.executable,'-B','-m','unittest','discover','-s',source/'examples/mem_service_pool','-p','test_*.py'],env=env)
    run([sys.executable,'-B','-m','unittest','discover','-s',source/'examples/managed_pool','-p','test_sdk.py'],env=env)
    run([sys.executable,'-B','-m','unittest','discover','-s',source/'examples/common_session','-p','test_*.py'],env=env)
    run([sys.executable,'-B','-m','unittest','discover','-s',source/'examples/ddr_v0','-p','test_t34_isolation.py'],env=env)
    base=root/'src/ub_sim/guest-linux/aarch64/out/initramfs.cpio.gz'
    stage=out/'stage';stage.mkdir()
    with gzip.open(base,'rb') as archive:
        run(['cpio','-idm','--quiet','--no-absolute-filenames'],cwd=stage,input=archive.read())
    for name,p in {'bin/run_app':root/'env/guest-kit004/uflow_env_guest_entry.sh',
                   'bin/uflow_env_payload':launcher,
                   'bin/uflow_pool_agent':out/'pool-agent',
                   'bin/linqu_mem_service_provider_obmm':official_worker}.items():
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
    daemon=cargo_target/'debug/uf-daemon'
    receipt={'schema':2,'source_commit':manifest['commit'],'source_manifest_sha256':digest(source/'.uflow-source.json'),
             'actual_kernel_profile_consumer':json.loads((out/'profile-consumer-cpu/results.json').read_bytes()),
             'compiler_argv':command,'agent_sha256':digest(out/'pool-agent'),
             'image':str(image),'image_sha256':digest(image),'base_sha256':digest(base),
             'platform_gva_inputs':{str(p):digest(p) for p in gva_inputs},
             'launcher':{'path':str(launcher),'sha256':digest(launcher),'source_sha256':digest(own/'pool_guest_init.c'),
                 'image_member_sha256':digest(stage/'bin/uflow_env_payload'),
                 'host_tests':json.loads((out/'launcher-cpu/results.json').read_bytes()),
                 'bounded_journal_CPU':json.loads((out/'journal-cpu/results.json').read_bytes()),
                 'PID1_handoff_ms':30000,'unbounded_park':False},
             'matrix_entry_cpu':json.loads((out/'matrix-entry-cpu/results.json').read_bytes()),
             'retained8_admission_cpu':json.loads((out/'retained8-admission-cpu/results.json').read_bytes()),
             'source_inputs':{str(p):{'bytes':p.stat().st_size,'sha256':digest(p)} for p in agent_inputs+inputs},
             'startup_cpu':{'results':json.loads((fixture_output/'results.json').read_bytes()),
                 'source_inputs':{str(p):digest(p) for p in fixture_inputs+[fixture/'fixture_worker.c',fixture/'startup_cpu_probe.py']},
                 'agent':str(fixture_agent),'agent_sha256':digest(fixture_agent),
                 'worker':str(fixture_worker),'worker_sha256':digest(fixture_worker),
                 'canary_endpoint_double':True,'actual_processes_directory_SDK':True,'business_backing':False},
             'fence_shim':str(shim/'libuf_acl_shim.so'),'fence_shim_sha256':digest(shim/'libuf_acl_shim.so'),
             'daemon':str(daemon),'daemon_sha256':digest(daemon),'installed_sdk_manifest':sdk_before,
             'source':str(source),'no_business_runtime_executed':True,
             'official_worker':{**worker_receipt,'image_member_sha256':digest(stage/'bin/linqu_mem_service_provider_obmm')},
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
        failure_notice()
        failure=a.root/'implementation'/a.output/'failure.json'
        if failure.parent.is_dir() and not failure.exists():
            failure.write_text(json.dumps({'traceback':traceback.format_exc()},indent=2))
        raise
