"""Affected production ARM64 agent/PID1 image; existing SDK/worker immutable.
Run under C's admitted HOST build. No Guest/QMP/OBMM/authority mutation.
"""
import argparse,gzip,hashlib,json,os,re,shlex,shutil,stat,subprocess,sys
from pathlib import Path
from pool_image import overlay,read_gzip,parse,inventory
from pool_execution_draft import prepare,encode
ROOT='/workspace/uflow/runs/mem-service-shared-pool-20261108'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def closure(argv,cwd):
 compiler=argv[0];sources=[Path(p) for p in argv if p.endswith('.c')];deps=set(sources);options=[];i=1
 while i<len(argv):
  a=argv[i]
  if a=='-o':i+=2;continue
  if not a.endswith('.c') and not a.startswith('-Wl,') and a!='-save-temps=obj':options.append(a)
  i+=1
 for src in sources:
  raw=subprocess.check_output([compiler,'-M',*options,str(src)],cwd=cwd,timeout=30).decode().replace('\\\n',' ')
  deps.update(Path(p).resolve() for p in shlex.split(raw.split(':',1)[1]))
 for entry in re.findall(r'^LOAD\s+([^\n]+)',(cwd/'agent.link.map').read_text(),re.M):
  if entry.strip()=='linker stubs':continue
  p=Path(entry.strip());deps.add(p.resolve() if p.is_absolute() else (cwd/p).resolve())
 deps.add(Path(compiler).resolve())
 for program in ['cc1','as','collect2','ld']:
  name=subprocess.check_output([compiler,'-print-prog-name='+program],timeout=10).decode().strip();p=name if Path(name).is_absolute() else shutil.which(name);assert p;deps.add(Path(p).resolve())
 assert len(deps)<1024;records={}
 for p in sorted(deps):
  assert p.is_file() and p.stat().st_size<=128<<20;records[str(p)]={'bytes':p.stat().st_size,'sha256':sha(p),'compiler_generated_intermediate':p.is_relative_to(cwd)}
 return records
def main(root,source,name,reuse=False):
 assert str(root)==ROOT and 'UFLOW_ENV_CLOSE_FD' in os.environ and re.fullmatch('build-host[0-9]{3}',name)
 generation=name[-3:];assert int(generation)>=64 and source==root/'implementation'/('source-host'+generation)
 out=root/'implementation'/name;out.mkdir(exist_ok=False);manifest=json.loads((source/'.uflow-source.json').read_bytes())
 for rel,row in manifest['files'].items():
  p=source/rel;assert p.is_file() and not p.is_symlink() and sha(p)==row['sha256'] and p.stat().st_mode&0o777==int(row['mode'][-3:],8)
 old=root/'implementation/source-host063';old_manifest=json.loads((old/'.uflow-source.json').read_bytes());assert old_manifest['commit']=='b451a468cef53d7a032aceabf8bf1d6a4ce6082f'
 # No Rust/core/SDK modification in this image-only step; no new Cargo build.
 for rel,row in old_manifest['files'].items():
  if rel.startswith(('crates/','sdk/')):assert manifest['files'][rel]==row and sha(old/rel)==row['sha256']
 baseline=json.loads((root/'implementation/build-host046/build-receipt.json').read_bytes());previous=json.loads((root/'implementation/build-host056/build-receipt.json').read_bytes())
 rust=json.loads((root/'implementation/build-host063/validation-receipt.json').read_bytes());assert rust['pass'] and rust['source_commit']==old_manifest['commit']
 state=root/'runs/c081/state/state.json';pin=sha(state);assert pin=='4d33870d43d69c6eb094494fe794cf38cf73bbb609fd72514dadd6e4cbd5cc26'
 authority=root/'implementation/responsibility-c013';assert not (authority/'successor-c081-terminal-domain-loss-v3').exists()
 claimed={str(p):sha(p) for folder in ['successor-c044-terminal-domain-loss-v1','successor-c064-terminal-domain-loss-v2'] for p in (authority/folder).iterdir()};assert len(claimed)==15
 sdk=root/'env/mem-service-sdk';sdk_pins=baseline['installed_sdk_manifest'];assert len(sdk_pins)==53
 for rel,pin_sdk in sdk_pins.items():assert sha(sdk/rel)==pin_sdk
 worker=baseline['official_worker'];assert len(worker['source_and_headers_link_inputs'])==233
 for p,row in worker['source_and_headers_link_inputs'].items():assert sha(Path(p))==row['sha256']
 worker_elf=Path(worker['ELF']);assert sha(worker_elf)==worker['ELF_sha256']
 frozen={**previous['reuse']['preserved_ELFs'],baseline['image']:baseline['image_sha256'],rust['daemon']:rust['daemon_sha256'],str(worker_elf):worker['ELF_sha256']}
 for p,pin_old in frozen.items():assert sha(Path(p))==pin_old
 env={**os.environ,'PYTHONPATH':str(source/'sdk/python'),'PYTHONDONTWRITEBYTECODE':'1','UFLOW_POOL_JOURNAL_LAUNCHER':str(out/'cpu-guest-init')}
 commands=[]
 def run(argv,label,timeout=120,**kwargs):
  argv=list(map(str,argv));commands.append({'label':label,'argv':argv})
  with (out/(label+'.log')).open('xb') as log:subprocess.run(argv,cwd=kwargs.pop('cwd',out),env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=timeout,**kwargs)
 own=source/'crates/uf-ub-sim-agent/c';argv=[]
 link_source=source;link_out=out;link_commit=manifest['commit']
 if reuse:
  assert int(generation)>64
  closed=json.loads((root/'runs/cb093/result.json').read_bytes())['processes'];assert closed['actual_task_exit_code']==1 and closed['process_tree_empty'] and closed['task_resources_closed'] and not closed['remaining'] and not closed['evidence_errors']
  failure=(root/'runs/cb093/command-00.log').read_text();assert "FileNotFoundError: [Errno 2] No such file or directory: '/usr/local/bin/aarch64-linux-gnu-readelf'" in failure
  link_source=root/'implementation/source-host064';link_out=root/'implementation/build-host064';linked=json.loads((link_source/'.uflow-source.json').read_bytes());link_commit=linked['commit']
  assert set(linked['files'])==set(manifest['files'])
  for rel,row in linked['files'].items():
   assert sha(link_source/rel)==row['sha256']
   if rel!='tools/phasec/build_pool_production_image.py':assert manifest['files'][rel]==row
  for name_link in ['pool-agent','pool-guest-init']:
   src=link_out/name_link;assert src.is_file() and src.read_bytes()[:4]==b'\x7fELF';shutil.copyfile(src,out/name_link);(out/name_link).chmod(0o755);assert sha(src)==sha(out/name_link)
 for value in baseline['compiler_argv']:
  if '/implementation/source-host046/' in value:value=value.replace(str(root/'implementation/source-host046'),str(link_source))
  elif value==str(root/'implementation/build-host046/pool-agent'):value=str(link_out/'pool-agent')
  argv.append(value)
 argv.extend(['-save-temps=obj','-Wl,-Map='+str(link_out/'agent.link.map')])
 if not reuse:run(argv,'production-agent-link')
 compiler=argv[:argv.index('-I'+str(sdk/'include/lingqu/mem_service'))];assert compiler==['/usr/local/bin/aarch64-linux-gnu-gcc','-O2','-Wall','-Wextra','-static','-pthread','-D__EXPORTED_HEADERS__']
 if not reuse:run([*compiler,own/'pool_guest_init.c','-o',out/'pool-guest-init'],'production-PID1-link')
 readelf=shutil.which('readelf');assert readelf,'existing generic ELF inspection tool required; no implicit install'
 for p in [out/'pool-agent',out/'pool-guest-init',worker_elf]:
  data=p.read_bytes();assert data[:4]==b'\x7fELF' and int.from_bytes(data[18:20],'little')==183
  header=subprocess.check_output([readelf,'-l',str(p)],timeout=10);assert b'INTERP' not in header
 inputs=closure(argv,link_out)
 # PID1 shares the same target toolchain. Obtain its complete source/header set.
 dep=subprocess.check_output([*compiler,'-M',str(link_source/'crates/uf-ub-sim-agent/c/pool_guest_init.c')],timeout=30).decode().replace('\\\n',' ')
 for value in shlex.split(dep.split(':',1)[1]):
  p=Path(value).resolve();inputs[str(p)]={'bytes':p.stat().st_size,'sha256':sha(p),'compiler_generated_intermediate':False}
 neutral=[sdk/'src/lingqu/mem_service'/n for n in ['mem_service_client.c','mem_service_wire_client.c','mem_service_provider.c','mem_service_mapping_owner.c']]
 common=[own/n for n in ['pool_agent.c','pool_client.c','pool_worker.c','pool_agent_io.c']]
 run(['/usr/bin/gcc','-O0','-Wall','-Wextra','-pthread','-DUFLOW_POOL_CPU_FIXTURE','-I'+str(sdk/'include/lingqu/mem_service'),'-I'+str(own),*common,source/'examples/mem_service_pool/fixture_platform.c',*neutral,'-o',out/'cpu-agent'],'CPU-agent-link')
 run(['/usr/bin/gcc','-O0','-Wall','-Wextra','-DUFLOW_POOL_LAUNCHER_FIXTURE',own/'pool_guest_init.c','-o',out/'cpu-guest-init'],'CPU-PID1-link')
 for script in ['launcher_cpu.py','journal_cpu.py']:
  run([sys.executable,'-B',source/'examples/mem_service_pool'/script,'--launcher',out/'cpu-guest-init','--output',out/script.removesuffix('.py')],script,60)
 for pattern in ['test_normal_journal_shutdown.py','test_pool_image.py','test_execution_draft.py']:
  run([sys.executable,'-B','-m','unittest','discover','-s',source/'examples/mem_service_pool','-p',pattern],pattern,30)
 run([sys.executable,'-B',source/'examples/mem_service_pool/agent_pid1_cpu.py','--agent',out/'cpu-agent','--worker',root/'implementation/build-host046/cpu-worker',
      '--launcher',out/'cpu-guest-init','--controller',sdk/'bin/linqu_mem_service','--scratch',root/('i'+generation),'--output',out/'actual-agent-PID1-SDK-normal-close'],'actual-agent-PID1-SDK-normal-close',90)
 base=root/'src/ub_sim/guest-linux/aarch64/out/initramfs.cpio.gz';assert sha(base)==baseline['base_sha256']
 entry=root/'env/guest-kit004/uflow_env_guest_entry.sh';old_image={e.name:e for e in parse(read_gzip(Path(baseline['image'])))}
 assert hashlib.sha256(old_image['bin/run_app'].data).hexdigest()==sha(entry) and hashlib.sha256(old_image['bin/linqu_mem_service_provider_obmm'].data).hexdigest()==sha(worker_elf)
 base_raw=read_gzip(base);base_rows={e.name:e for e in parse(base_raw)};assert stat.S_ISDIR(base_rows['bin'].fields[1])
 replacements={'bin/run_app':entry.read_bytes(),'bin/uflow_env_payload':(out/'pool-guest-init').read_bytes(),'bin/uflow_pool_agent':(out/'pool-agent').read_bytes(),'bin/linqu_mem_service_provider_obmm':worker_elf.read_bytes()}
 raw=overlay(base_raw,replacements);image=out/'pool-agent.cpio.gz'
 with image.open('xb') as f,gzip.GzipFile(fileobj=f,mode='wb',filename='',mtime=0,compresslevel=6) as zipped:zipped.write(raw)
 assert read_gzip(image)==raw;after=inventory(raw);before=inventory(base_raw)
 assert set(after)==set(before)|set(replacements)
 for name_before,row in before.items():
  if name_before not in replacements:assert after[name_before]==row
 (out/'image-members.json').write_text(json.dumps(after,indent=2));(out/'compiler-inputs.json').write_text(json.dumps(inputs,indent=2))
 run(['gzip','-t',image],'actual-gzip-integrity')
 run(['cpio','-it','--quiet'],'actual-cpio-list',input=raw)
 # Make a genuine single-link launch candidate without mutating old Cargo ELF.
 daemon=out/'uf-daemon'
 with Path(rust['daemon']).open('rb') as src,daemon.open('xb') as dst:shutil.copyfileobj(src,dst);dst.flush();os.fsync(dst.fileno())
 daemon.chmod(0o755);assert daemon.stat().st_nlink==1 and sha(daemon)==rust['daemon_sha256']
 for rel,pin_sdk in sdk_pins.items():assert sha(sdk/rel)==pin_sdk
 for p,row in worker['source_and_headers_link_inputs'].items():assert sha(Path(p))==row['sha256']
 for p,pin_old in {**frozen,**claimed}.items():assert sha(Path(p))==pin_old
 assert sha(state)==pin and not (authority/'successor-c081-terminal-domain-loss-v3').exists()
 receipt={'kind':'CProductionImageHostBuildV1','pass':True,'source_commit':manifest['commit'],'source_manifest_sha256':sha(source/'.uflow-source.json'),'source_files':len(manifest['files']),
  'image':str(image),'image_sha256':sha(image),'image_bytes':image.stat().st_size,'image_members':len(after),'base_sha256':sha(base),'unrelated_base_members_byte_identical':True,
  'agent_sha256':sha(out/'pool-agent'),'launcher_sha256':sha(out/'pool-guest-init'),'ARM_compiled_source_commit':link_commit,'ARM_links_reused_from':str(link_out) if reuse else None,'ARM_compiler_argv':argv,'readelf':readelf,'worker_sha256':sha(worker_elf),'worker_inputs':233,'SDK_files':53,
  'SDK_manifest_sha256':hashlib.sha256(json.dumps(sdk_pins,sort_keys=True,separators=(',',':')).encode()).hexdigest(),'installed_sdk_manifest':sdk_pins,
  'official_worker_reused':worker,'agent_PID1_compiler_closure':str(out/'compiler-inputs.json'),'compiler_input_count':len(inputs),'compiler_inputs_sha256':sha(out/'compiler-inputs.json'),
  'daemon':str(daemon),'daemon_sha256':sha(daemon),'daemon_single_link':True,'daemon_compiled_source_commit':rust['source_commit'],'no_new_Cargo_or_worker_build':True,
  'new_agent_PID1_SDK_CPU':json.loads((out/'actual-agent-PID1-SDK-normal-close/results.json').read_bytes()),'finite_exit':json.loads((out/'launcher_cpu/results.json').read_bytes()),'journal':json.loads((out/'journal_cpu/results.json').read_bytes()),
  'commands':commands,'original_c081_sha256_before_after':pin,'actual_Guest_QMP_original_lock_ledger_device':False,'production_controller_ready':False}
 (out/'build-receipt.json').write_text(json.dumps(receipt,indent=2));draft=prepare(receipt);(out/'execution-preparation.json').write_bytes(encode(draft))
 print(json.dumps({'pass':True,'image_sha256':receipt['image_sha256'],'image_bytes':receipt['image_bytes'],'source_commit':manifest['commit'],'production_runtime_ready':False}))
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--source',type=Path,required=True);p.add_argument('--output',required=True);p.add_argument('--reuse-linked-host064',action='store_true');a=p.parse_args();main(a.root,a.source,a.output,a.reuse_linked_host064)
