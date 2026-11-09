"""Actual matrix() and its callback with real C agent/SDK/Rust socket service.

Only the QEMU seam, canaries/worker, cfg(test) original import and the first
business creation are explicit CPU doubles. No actual c013/c027 authority reads.
"""
import argparse,ctypes,json,os,signal,subprocess,sys,time,types
from pathlib import Path
from unittest.mock import patch
from startup_cpu_probe import Harness,Channel,birth
from mem_service_pool_bridge import GuestNode,BridgeServer
from uflow.mem_service_pool_v2 import exchange
import run_mem_service_pool_matrix as driver

class ReachedBusinessGate(Exception):pass
def main():
 p=argparse.ArgumentParser()
 for key in ('agent','worker','controller','scratch','output','rust-tests','source','image','shim'):p.add_argument('--'+key,type=Path,required=True)
 a=p.parse_args()
 if 'UFLOW_ENV_CLOSE_FD' not in os.environ:raise RuntimeError('admitted build required')
 if ctypes.CDLL(None).prctl(36,1,0,0,0):raise RuntimeError('subreaper required')
 a.scratch.mkdir();a.output.mkdir();h=Harness(a,0,lower=False);servers=[];children=[];observed=[]
 out=a.scratch/'runs/1';out.mkdir(parents=True);env=a.scratch/'env';env.mkdir()
 (env/'mem-service-sdk').symlink_to(a.controller.parent.parent,target_is_directory=True)
 import hashlib
 sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
 build={'source':str(a.source),'image':str(a.image),'image_sha256':sha(a.image),
   'daemon':'CPU-TEST-ONLY-RUST-SERVICE','daemon_sha256':sha(a.image),'fence_shim':str(a.shim),'fence_shim_sha256':sha(a.shim),
   'source_manifest_sha256':sha(a.source/'.uflow-source.json'),'installed_sdk_manifest':{}}
 # matrix checks the actual binary binding before it starts anything.
 daemon=a.scratch/'catalog-fixture';daemon.symlink_to(a.image);build['daemon']=str(daemon)
 config={'output':str(out),'nodes':[{'id':n,'boot_argv':['-initrd',str(a.image)]} for n in ('nodeA','nodeB')]}
 real_popen=subprocess.Popen;real_exchange=exchange;catalog=[]
 def popen(argv,**kwargs):
  if argv[0]==str(daemon):
   control=h.path/'rust.json';control.write_text(json.dumps({'provider_config':str(out/'provider.json'),'state':str(out/'state'),'endpoint':str(out/'u.sock')}))
   kwargs['env']={**kwargs['env'],'UFLOW_RECON_CATALOG_CPU_CONFIG':str(control)}
   argv=[str(a.rust_tests),'--exact','pool_reconciliation::tests::staged_catalog_fixture','--nocapture','--test-threads=1']
   child=real_popen(argv,**kwargs);catalog.append(child)
  else:child=real_popen(argv,**kwargs)
  h.children.append((child,birth(child.pid)));return child
 def node(*args,**kwargs):
  n=GuestNode(*args,**kwargs,notify=h.notice);h.nodes[n.identity['node']]=n;return n
 def bridge(*args):
  server=BridgeServer(*args);servers.append(server);return server
 def group_run(config,control):
  channels={};identities=[]
  for name in ('nodeA','nodeB'):
   raw=(h.output/(name+'.serial.log')).open('xb');h.raw.append(raw)
   err=(h.output/(name+'.stderr')).open('xb');h.raw.append(err)
   child=popen([str(a.agent),str(h.path/name),name,'cpu-matrix-'+name,'metadata-only-no-OBMM'],
    stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=err,
    env={**os.environ,'UFLOW_POOL_FIXTURE_WORKER':str(a.worker)})
   c=Channel(child,raw);line=c.line(time.monotonic()+5);assert line.startswith('UFLOW_ENV_IDENTITY ')
   identity=json.loads(line.split(' ',1)[1]);assert identity['engine']=='cpu-fixture'
   channels[name]=c;identities.append(identity)
  return control(channels,identities)
 def routed(endpoint,method,values,**kwargs):
  observed.append({'endpoint':str(endpoint),'method':method})
  assert str(endpoint)==str(out/'u.sock'),'Catalog control was routed to a Guest bridge'
  if method=='CreateManagedPool':
   state=json.loads((out/'state/state.json').read_bytes())
   assert state['charged_bytes']==8<<20 and state['retained_bytes']==4<<20 and state['creation_reserved_bytes']==0
   assert state['pools']==[]
   closed=real_exchange(endpoint,'ShutdownManagedPool',values={'admin_token':values['admin_token']})
   assert closed['charged_bytes']==closed['retained_bytes']==4<<20 and closed['active_bytes']==closed['reserved_bytes']==0
   assert catalog[0].wait(timeout=5)==0
   for s in servers:s.close()
   for n in h.nodes.values():n.prepare_close()
   raise ReachedBusinessGate()
  return real_exchange(endpoint,method,values,**kwargs)
 try:
  with patch.dict(sys.modules,{'uflow_guest_group':types.SimpleNamespace(run=group_run)}), \
       patch.object(driver.subprocess,'Popen',popen),patch.object(driver,'GuestNode',node), \
       patch.object(driver,'BridgeServer',bridge),patch.object(driver,'exchange',routed),patch.object(driver,'notify_failure',h.notice):
   try:driver.matrix(a.scratch,config,build)
   except ReachedBusinessGate:pass
   else:raise AssertionError('actual matrix callback did not reach controlled business gate')
  assert [v['method'] for v in observed]==['ActivateManagedBootstrap','CreateManagedPool']
  events=[json.loads(s) for s in (out/'pool-events.jsonl').read_text().splitlines()]
  assert len([e for e in events if e['event']=='worker-platform-bootstrap'])==2
  assert any(e['event']=='managed-admission-ready' for e in events)
  result={'pass':True,'actual_matrix_function_and_callback':True,'actual_Rust_C_SDK_sockets':True,
   'Catalog_endpoint_after_both_bridges':observed,'actual_canary_activation_and_normal_shutdown':True,
   'CPU_import_canary_worker_QEMU_doubles':True,'business_call_intercepted_before_allocation':True,
   'real_original_authorities_read_or_modified':False}
  (a.output/'results.json').write_text(json.dumps(result,indent=2));print(json.dumps(result))
 finally:
  for s in servers:
   s.stop.set();s.thread.join(1);s.listener.close()
  h.close()
if __name__=='__main__':main()
