"""Full actual matrix callback through Rust/C/SDK and real CPU mmap bytes.
QEMU, old import, canaries, platform, worker and Image are isolated CPU doubles.
No production verifier/lineage/Guest/OBMM/device is executed.
"""
import argparse,ctypes,hashlib,json,os,subprocess,sys,time,types
from pathlib import Path
from unittest.mock import patch
from startup_cpu_probe import Harness,Channel,birth
from mem_service_pool_bridge import GuestNode,BridgeServer
from uflow.mem_service_pool_v2 import exchange,PoolV2Error,PoolV2Unknown
import run_mem_service_pool_matrix as driver

def main():
 p=argparse.ArgumentParser()
 for k in ('agent','worker','controller','scratch','output','rust-tests','source','image','shim'):p.add_argument('--'+k,type=Path,required=True)
 a=p.parse_args()
 if 'UFLOW_ENV_CLOSE_FD' not in os.environ:raise RuntimeError('admitted build required')
 if ctypes.CDLL(None).prctl(36,1,0,0,0):raise RuntimeError('subreaper required')
 a.scratch.mkdir();a.output.mkdir();h=Harness(a,0,lower=False);servers=[];catalog=[];observed=[];states=[]
 out=a.scratch/'runs/1';out.mkdir(parents=True);env=a.scratch/'env';env.mkdir()
 (env/'mem-service-sdk').symlink_to(a.controller.parent.parent,target_is_directory=True)
 sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
 daemon=a.scratch/'catalog-fixture';daemon.symlink_to(a.image)
 build={'source':str(a.source),'image':str(a.image),'image_sha256':sha(a.image),'daemon':str(daemon),'daemon_sha256':sha(a.image),
  'fence_shim':str(a.shim),'fence_shim_sha256':sha(a.shim),'source_manifest_sha256':sha(a.source/'.uflow-source.json'),'installed_sdk_manifest':{}}
 config={'output':str(out),'nodes':[{'id':n,'boot_argv':['-initrd',str(a.image)]} for n in ('nodeA','nodeB')]}
 real_popen=subprocess.Popen;real_exchange=exchange;gate=[];memory=a.scratch/'cpu-memory.data'
 def popen(argv,**kwargs):
  assert gate,'Image gate must precede first subprocess'
  if argv[0]==str(daemon):
   control=h.path/'rust.json';control.write_text(json.dumps({'provider_config':str(out/'provider.json'),'state':str(out/'state'),'endpoint':str(out/'u.sock')}))
   kwargs['env']={**kwargs['env'],'UFLOW_RECON_CATALOG_CPU_CONFIG':str(control)}
   argv=[str(a.rust_tests),'--exact','pool_c039_prestart::tests::staged_c039_prestart_fixture','--nocapture','--test-threads=1']
   child=real_popen(argv,**kwargs);catalog.append(child)
  else:child=real_popen(argv,**kwargs)
  h.children.append((child,birth(child.pid)));return child
 def node(*args,**kwargs):
  n=GuestNode(*args,**kwargs,notify=h.notice);h.nodes[n.identity['node']]=n;return n
 def bridge(*args):
  s=BridgeServer(*args);servers.append(s);return s
 def state():return json.loads((out/'state/state.json').read_bytes())
 def total(s):return s['charged_bytes']+s['creation_reserved_bytes']
 def group_run(config,control):
  initial=state();assert initial['charged_bytes']==initial['retained_bytes']==12<<20 and initial['creation_reserved_bytes']==68<<20
  assert initial['retained_reserved_bytes']==64<<20 and total(initial)==80<<20
  record=json.loads((out/'state/fixture-lineage/successor-c039-prestart-v1/result.json').read_bytes())
  assert record['resource_ledger']['charged_bytes']==12<<20 and record['resource_ledger']['reserved_bytes']==64<<20
  inherited=json.loads((out/'state/prestart-inherited80.json').read_bytes());resulting=json.loads((out/'state/prestart-resulting76.json').read_bytes())
  assert inherited['resource_ledger']['charged_bytes']==12<<20 and inherited['resource_ledger']['reserved_bytes']==68<<20
  assert resulting['resource_ledger']['charged_bytes']==12<<20 and resulting['resource_ledger']['reserved_bytes']==64<<20
  assert initial['resource_ledger']['retired_unstarted_object_ids']==[108,109]
  states.extend([{'point':'inherited-prestart-baseline','total':80<<20},{'point':'proved-two-unstarted-reservations-cancelled','total':76<<20},{'point':'reserved-new-canaries','total':total(initial)}])
  channels={};identities=[]
  for name in ('nodeA','nodeB'):
   raw=(h.output/(name+'.serial.log')).open('xb');h.raw.append(raw)
   err=(h.output/(name+'.stderr')).open('xb');h.raw.append(err)
   child=popen([str(a.agent),str(h.path/name),name,'cpu-matrix-'+name,'metadata-only-no-OBMM'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=err,
    env={**os.environ,'UFLOW_POOL_FIXTURE_WORKER':str(a.worker),'UFLOW_POOL_FIXTURE_MODE':'matrix-data','UFLOW_POOL_FIXTURE_MEMORY':str(memory)})
   c=Channel(child,raw);line=c.line(time.monotonic()+5);assert line.startswith('UFLOW_ENV_IDENTITY ')
   identity=json.loads(line.split(' ',1)[1]);assert identity['engine']=='cpu-fixture';channels[name]=c;identities.append(identity)
  control(channels,identities)
  final=state();assert final['charged_bytes']==final['retained_bytes']==12<<20 and final['active_bytes']==0
  assert final['creation_reserved_bytes']==final['retained_reserved_bytes']==64<<20 and final['active_reserved_bytes']==0
  assert catalog[0].wait(timeout=5)==0;states.append({'point':'current-closed','total':total(final)})
 def routed(endpoint,method,values,**kwargs):
  observed.append({'endpoint':str(endpoint),'method':method,'nbytes':values.get('nbytes')})
  assert str(endpoint)==str(out/'u.sock'),'Catalog endpoint shadowed by Guest bridge'
  if method=='CreateManagedPool' and values['nbytes']==48<<20:
   s=state();assert s['charged_bytes']==16<<20 and s['creation_reserved_bytes']==64<<20 and s['pools']==[]
   history=real_exchange(endpoint,'QueryPoolOperation',{'target_operation_id':'45282a1e-2287-4be6-97fa-0e7313c75818'})
   assert history['state']=='Unknown' and history['reserved_bytes']==64<<20 and history['pool']['canonical'] is None
   for fields in [dict(values),{'pool_id':history['pool']['pool_id'],'pool_epoch':history['pool']['pool_epoch'],'provider_session':history['pool']['provider_session'],'node':'nodeA','access':'rw'}]:
    try:real_exchange(endpoint,'CreateManagedPool' if 'nbytes' in fields else 'RegisterPoolClient',fields,operation_id='45282a1e-2287-4be6-97fa-0e7313c75818')
    except PoolV2Unknown:raise
    except PoolV2Error as e:assert e.response['code']=='Rejected'
    else:raise AssertionError('old Unknown accessible or operation replayed')
  result=real_exchange(endpoint,method,values,**kwargs)
  if method=='CreateManagedPool':
   s=state();assert total(s)==128<<20 and s['retained_reserved_bytes']==64<<20;states.append({'point':'business-allocated','total':total(s)})
  return result
 def capability(*args):gate.append(True);return {'CPU_synthetic_capability_seam':True,'runtime_tested':False}
 try:
  with patch.dict(sys.modules,{'uflow_guest_group':types.SimpleNamespace(run=group_run)}),patch.object(driver.subprocess,'Popen',popen), \
      patch.object(driver,'GuestNode',node),patch.object(driver,'BridgeServer',bridge),patch.object(driver,'exchange',routed), \
      patch.object(driver,'notify_failure',h.notice),patch.object(driver,'launch_gate',capability), \
      patch.object(driver,'verify_new_fm',lambda *a:{'CPU_fake_QEMU_no_FM':True,'independent':True}):
   driver.matrix(a.scratch,config,build)
  events=[json.loads(s) for s in (out/'pool-events.jsonl').read_text().splitlines()]
  assert [s['total'] for s in states]==[80<<20,76<<20,80<<20,128<<20,76<<20]
  directions=[e for e in events if e['event']=='E2-direction'];assert [e['nbytes'] for e in directions[:2]]==[20<<20,20<<20]
  assert {e['writer']['node'] for e in directions[:2]}=={'nodeA','nodeB'}
  barrier=next(e for e in events if e['event']=='E3-real-lifetime-barrier');assert len(set(barrier['independent_host_pids']))==3
  assert any(e['event']=='E4-two-layer-release' for e in events) and not memory.exists()
  calls=[e for e in events if e['event']=='lower-control']
  allocated=[e for e in calls if e['operation']==0x70 and e['origin']['role']=='root']
  assert len(allocated)==1 and allocated[0]['response_metadata']['live_refs']=='0'
  assert not any(e['operation']==0x72 and e['origin']['role']=='root' for e in calls),'ALLOCATE does not create a holder to RELEASE'
  assert any(e['operation']==0x73 and e['origin']['role']=='root' and e['status']==0 for e in calls)
  original=json.loads((out/'E1-E4.json').read_bytes());assert original['capacity']==48<<20 and original['final_B_total_bytes']==76<<20
  result={'pass':True,'actual_matrix_and_nested_callback':True,'actual_Rust_C_SDK_sockets':True,'actual_CPU_shared_mapping_all_bytes':True,
   'budget_sequence':states,'strict_prestart_cancel_before_new_reservation':True,'both20MiB_directions':True,'independent_full_guards':True,'three_client_lifetime_barrier':True,
   'negative64_and_new_live_Unknown_guard':True,'historical_Unknown_query_and_retry_denied':True,'normal_current_drain_keeps_reserved64':True,
   'SDK_allocate_has_no_holder_and_no_spurious_owner_RELEASE':True,
   'CPU_import_canary_worker_platform_QEMU_Image_gate_doubles':True,'real_Guest_or_OBMM_or_production_lineage':False,'actual_capacity':48<<20}
  (a.output/'results.json').write_text(json.dumps(result,indent=2));print(json.dumps(result))
 finally:
  for s in servers:s.stop.set();s.thread.join(1);s.listener.close()
  h.close()
if __name__=='__main__':main()
