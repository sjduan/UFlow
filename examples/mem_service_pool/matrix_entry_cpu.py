"""Full actual matrix callback through Rust/C/SDK and real CPU mmap bytes.
QEMU, old import, canaries, platform, worker and Image are isolated CPU doubles.
No production verifier/lineage/Guest/OBMM/device is executed.
"""
import argparse,copy,ctypes,hashlib,json,os,subprocess,sys,time,types
from pathlib import Path
from unittest.mock import patch
from startup_cpu_probe import Harness,Channel,birth
from mem_service_pool_bridge import GuestNode,BridgeServer
from uflow.mem_service_pool_v2 import exchange,PoolV2Error,PoolV2Unknown
import run_mem_service_pool_matrix as driver

def main():
 p=argparse.ArgumentParser()
 p.add_argument('--terminal-domain',action='store_true')
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
   if a.terminal_domain:
    value=json.loads(control.read_bytes());value['terminal_origin']=str(out/'terminal-origin');control.write_text(json.dumps(value))
    kwargs['env']={**kwargs['env'],'UFLOW_TERMINAL_STAGED_CPU_CONFIG':str(control)}
    commit=real_popen([str(a.rust_tests),'--exact','pool_terminal_domain::tests::terminal_commit_process','--nocapture','--test-threads=1'],**kwargs)
    stamp=birth(commit.pid);h.children.append((commit,stamp));assert commit.wait(timeout=10)==0
    (out/'terminal-only-process.json').write_text(json.dumps({'pid':commit.pid,'birth':stamp,'actual_exit':0,'source_state_preserved128':True,'durable_terminal_B_bytes':76<<20,'new_service_started':False}))
    argv=[str(a.rust_tests),'--exact','pool_terminal_domain::tests::staged_terminal_service_process','--nocapture','--test-threads=1']
   else:
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
  if a.terminal_domain:
   records=out/'terminal-origin/lineage/successor-c044-terminal-domain-loss-v1'
   inherited=json.loads((records/'before128.json').read_bytes());resulting=json.loads((records/'resulting76.json').read_bytes())
   assert inherited['charged_bytes']==64<<20 and inherited['reserved_bytes']==64<<20
   assert resulting['B_conservative_bytes']==76<<20 and initial['reconciliation']['terminal_bootstrap_phase']=='AwaitingNewGuestReadiness'
   admin=(out/'state/admin.token').read_text()
   try:real_exchange(out/'u.sock','CreateManagedPool',{'admin_token':admin,'nbytes':48<<20})
   except PoolV2Unknown:raise
   except PoolV2Error as e:assert e.response['code']=='Rejected'
   else:raise AssertionError('business admitted before actual Guest identities/worker readiness')
  else:
   record=json.loads((out/'state/fixture-lineage/successor-c039-prestart-v1/result.json').read_bytes())
   assert record['resource_ledger']['charged_bytes']==12<<20 and record['resource_ledger']['reserved_bytes']==64<<20
   inherited=json.loads((out/'state/prestart-inherited80.json').read_bytes());resulting=json.loads((out/'state/prestart-resulting76.json').read_bytes())
   assert inherited['resource_ledger']['charged_bytes']==12<<20 and inherited['resource_ledger']['reserved_bytes']==68<<20
   assert resulting['resource_ledger']['charged_bytes']==12<<20 and resulting['resource_ledger']['reserved_bytes']==64<<20
  assert initial['resource_ledger']['retired_unstarted_object_ids']==[108,109]
  states.extend([{'point':'original128-terminal-source' if a.terminal_domain else 'inherited-prestart-baseline','total':(128 if a.terminal_domain else 80)<<20},
                 {'point':'typed-terminal76-committed-in-prior-process' if a.terminal_domain else 'proved-two-unstarted-reservations-cancelled','total':76<<20},
                 {'point':'reserved-new-canaries-before-Guest-identity','total':total(initial)}])
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
  if a.terminal_domain and method=='ActivateManagedBootstrap':
   prepared=out/'prepared-provider.json';good=json.loads(prepared.read_bytes());bad=copy.deepcopy(good)
   old=json.loads((out/'terminal-origin/original-state.json').read_bytes())['bootstrap_readiness']['nodes']['nodeA']
   node=next(v for v in bad['nodes'] if v['node']=='nodeA')
   for key in ('boot_id','agent_session','incarnation'):node[key]=old[key]
   for key in ('registration','directory'):
    node['startup'][key]['boot_id']=old['boot_id'];node['startup'][key]['incarnation']=old['incarnation']
   node['startup']['worker']['incarnation']=old['incarnation'];prepared.write_text(json.dumps(bad))
   try:real_exchange(endpoint,method,values,operation_id='CPU-old-c044-boot-fence')
   except PoolV2Unknown:raise
   except PoolV2Error as e:assert e.response['code']=='Rejected' and 'terminal-domain' in e.response['detail']
   else:raise AssertionError('old Guest boot identity entered staged fresh namespace')
   assert not any(n.failure for n in h.nodes.values()),'old identity must be fenced before any Guest RPC'
   prepared.write_text(json.dumps(good))
  if method=='CreateManagedPool' and values['nbytes']==48<<20:
   s=state();assert s['charged_bytes']==16<<20 and s['creation_reserved_bytes']==64<<20 and s['pools']==[]
   if a.terminal_domain:
    old=json.loads((out/'terminal-origin/original-state.json').read_bytes())
    history={'pool':old['quarantined_pools'][0]}
    try:real_exchange(endpoint,'QueryPoolOperation',{'target_operation_id':'45282a1e-2287-4be6-97fa-0e7313c75818'})
    except PoolV2Unknown:raise
    except PoolV2Error as e:assert e.response['code']=='Rejected'
    else:raise AssertionError('terminal historical operation entered new service context')
    assert s['reconciliation']['terminal_bootstrap_phase']=='ActiveNewNamespace'
   else:
    history=real_exchange(endpoint,'QueryPoolOperation',{'target_operation_id':'45282a1e-2287-4be6-97fa-0e7313c75818'})
    assert history['state']=='Unknown' and history['reserved_bytes']==64<<20 and history['pool']['canonical'] is None
   for fields in [dict(values),{'pool_id':history['pool']['pool_id'],'pool_epoch':history['pool']['pool_epoch'],'provider_session':history['pool']['provider_session'],'node':'nodeA','access':'rw'}]:
    try:real_exchange(endpoint,'CreateManagedPool' if 'nbytes' in fields else 'RegisterPoolClient',fields,operation_id='45282a1e-2287-4be6-97fa-0e7313c75818')
    except PoolV2Unknown:raise
    except PoolV2Error as e:assert e.response['code']=='Rejected'
    else:raise AssertionError('old Unknown accessible or operation replayed')
  result=real_exchange(endpoint,method,values,**kwargs)
  if method=='CreateManagedPool':
   if a.terminal_domain:assert result['resource_generation']==1 and result['resource_id']>=115,'fresh namespace uses real native generation1'
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
  assert [s['total'] for s in states]==[(128 if a.terminal_domain else 80)<<20,76<<20,80<<20,128<<20,76<<20]
  directions=[e for e in events if e['event']=='E2-direction'];assert [e['nbytes'] for e in directions[:2]]==[20<<20,20<<20]
  assert {e['writer']['node'] for e in directions[:2]}=={'nodeA','nodeB'}
  barrier=next(e for e in events if e['event']=='E3-real-lifetime-barrier');assert len(set(barrier['independent_host_pids']))==3
  assert any(e['event']=='E4-two-layer-release' for e in events) and not memory.exists()
  calls=[e for e in events if e['event']=='lower-control']
  preflight=[e for e in events if e['event']=='stage-worker-serial-preflight-completed']
  assert len(preflight)==2 and all(e['Guest_acceptance_proved'] and e['body_bytes']==1496 for e in preflight)
  business=next(i for i,e in enumerate(events) if e['event']=='stage-business48-create-start')
  assert all(events.index(e)<business for e in preflight)
  diagnostic=next(e for e in events if e['event']=='stage-first-cross-node-read4KiB-completed')
  assert diagnostic['nbytes']==4096 and diagnostic['read']['receipt']['all_bytes'] and diagnostic['read']['receipt']['visible']
  assert events.index(diagnostic)<next(i for i,e in enumerate(events) if e['event']=='stage-full-direction-start')
  allocated=[e for e in calls if e['operation']==0x70 and e['origin']['role']=='root']
  assert len(allocated)==1 and allocated[0]['response_metadata']['live_refs']=='0'
  assert not any(e['operation']==0x72 and e['origin']['role']=='root' for e in calls),'ALLOCATE does not create a holder to RELEASE'
  assert any(e['operation']==0x73 and e['origin']['role']=='root' and e['status']==0 for e in calls)
  original=json.loads((out/'E1-E4.json').read_bytes());assert original['capacity']==48<<20 and original['final_B_total_bytes']==76<<20
  result={'pass':True,'actual_matrix_and_nested_callback':True,'actual_Rust_C_SDK_sockets':True,'actual_CPU_shared_mapping_all_bytes':True,
   'budget_sequence':states,'strict_prestart_cancel_before_new_reservation':not a.terminal_domain,'terminal_commit_then_separate_service_process':a.terminal_domain,
   'reserve4_before_real_Guest_identity_then_activate_before48':a.terminal_domain,'new_native_generation_one_allowed':a.terminal_domain,
   'old_c044_boot_fenced_before_actual_activation_RPC':a.terminal_domain,
   'large_serial_preflight_before48':True,'first_cross_node_4KiB_before_full_directions':True,
   'both20MiB_directions':True,'independent_full_guards':True,'three_client_lifetime_barrier':True,
   'negative64_and_new_live_Unknown_guard':True,'historical_Unknown_query_and_retry_denied':True,'normal_current_drain_keeps_reserved64':True,
   'SDK_allocate_has_no_holder_and_no_spurious_owner_RELEASE':True,
   'CPU_import_canary_worker_platform_QEMU_Image_gate_doubles':True,'real_Guest_or_OBMM_or_production_lineage':False,'actual_capacity':48<<20}
  (a.output/'results.json').write_text(json.dumps(result,indent=2));print(json.dumps(result))
 finally:
  for s in servers:s.stop.set();s.thread.join(1);s.listener.close()
  h.close()
if __name__=='__main__':main()
