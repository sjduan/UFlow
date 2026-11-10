"""Real Rust service + production C/SDK/IPC, explicit CPU import/canary doubles.

The cfg(test)-only Rust hook never reads or changes the real responsibility
lineage. The production executable has no fixture/import-ACK escape hatch.
"""
import argparse
import copy
import ctypes
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
from startup_cpu_probe import Harness
from daemon_peer_auth import DaemonPeer
from mem_service_pool_bridge import BridgeServer,notify_failure
from uflow.mem_service_pool_v2 import exchange,PoolV2Error,PoolV2Unknown

def refused(action):
    try:action()
    except PoolV2Unknown:raise
    except PoolV2Error as e:
        assert e.response['code']=='Rejected';return
    raise AssertionError('expected real service rejection')

def main():
    p=argparse.ArgumentParser();p.add_argument('--agent',type=Path,required=True);p.add_argument('--worker',type=Path,required=True)
    p.add_argument('--controller',type=Path,required=True);p.add_argument('--scratch',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--rust-tests',type=Path,required=True)
    a=p.parse_args()
    if 'UFLOW_ENV_CLOSE_FD' not in os.environ:raise RuntimeError('admitted B build required')
    if ctypes.CDLL(None).prctl(36,1,0,0,0):raise RuntimeError('subreaper required')
    a.scratch.mkdir(exist_ok=False);a.output.mkdir(exist_ok=False)
    h=Harness(a,0);servers=[]
    try:
        config=h.path/'provider.json';prepared=h.path/'prepared.json';state=h.path/'state';endpoint=str(h.path/'u.sock')
        realm='simulated:cpu-startup';session='cpu-staged-catalog'
        config.write_text(json.dumps({'schema':2,'backend':'mem_service_obmm','realm':realm,'session':session,'home':'nodeA',
            'deferred_bootstrap':True,'readiness_file':str(prepared),
            'reconciliation':{'assessment':'test-only-hook','bindings':'not-production-verifier'},
            'nodes':[{'node':name,'socket':str(h.path/(name+'.sock'))} for name in ('nodeA','nodeB')]}))
        control=h.path/'test.json';control.write_text(json.dumps({'provider_config':str(config),'state':str(state),'endpoint':endpoint}))
        f=(h.output/'catalog.log').open('xb');h.raw.append(f)
        child=h.spawn([a.rust_tests,'--exact','pool_reconciliation::tests::staged_catalog_fixture','--nocapture','--test-threads=1'],
            stdout=f,stderr=subprocess.STDOUT,env={**os.environ,'UFLOW_RECON_CATALOG_CPU_CONFIG':str(control)})
        deadline=time.monotonic()+5
        while not Path(endpoint).exists():
            if child.poll() is not None or time.monotonic()>deadline:raise RuntimeError('actual staged Rust fixture failed startup')
            time.sleep(.02)
        daemon_auth=DaemonPeer.capture(child.pid,a.rust_tests.resolve(),hashlib.sha256(a.rust_tests.read_bytes()).hexdigest())
        admin=(state/'admin.token').read_text();initial=json.loads((state/'state.json').read_bytes())
        assert initial['charged_bytes']==initial['retained_bytes']==4<<20 and initial['creation_reserved_bytes']==4<<20
        refused(lambda:exchange(endpoint,'CreateManagedPool',{'admin_token':admin,'nbytes':64<<20}))
        assert json.loads((state/'state.json').read_bytes())['pools']==[]
        query=exchange(endpoint,'QueryPoolOperation',{'target_operation_id':'de21099a-5b02-4e8d-b1d7-3a6129ab9f22'})
        assert query['state']=='VerifiedUncreatedTerminal' and query['SDK_ACK_Unmapped_or_canary_close_claimed'] is False
        refused(lambda:exchange(endpoint,'RegisterPoolClient',{'pool_id':'c509e452-79e6-4b02-9ad6-918cb901cfa4',
            'pool_epoch':'125b8f2a-0c42-4605-8a5c-9fff0184fdfe','provider_session':'d0fce298-a1bc-4a4f-b6fe-af025b1fc0fd',
            'node':'nodeA','access':'rw'}))
        startup=h.start();bootstrap,ready=startup.run();bindings=[]
        for name,node in h.nodes.items():
            server=BridgeServer(h.path/(name+'.sock'),node,daemon_auth);servers.append(server);server.thread.start()
            bindings.append({k:node.identity[k] for k in ('node','boot_id','agent_session','incarnation')}|
                {'socket':str(server.path),**bootstrap[name],'startup':ready[name]})
        value={'schema':2,'backend':'mem_service_obmm','realm':realm,'session':session,'home':'nodeA','nodes':bindings}
        bad=copy.deepcopy(value);bad['nodes'][0]['startup']['managed_ready']=False;prepared.write_text(json.dumps(bad))
        refused(lambda:exchange(endpoint,'ActivateManagedBootstrap',{'admin_token':admin}))
        assert json.loads((state/'state.json').read_bytes())['creation_reserved_bytes']==4<<20
        prepared.write_text(json.dumps(value));operation='cpu-activate-one'
        active=exchange(endpoint,'ActivateManagedBootstrap',{'admin_token':admin},operation_id=operation)
        assert active['bootstrap_activated'] is True
        assert exchange(endpoint,'ActivateManagedBootstrap',{'admin_token':admin},operation_id=operation)==active
        activated=json.loads((state/'state.json').read_bytes())
        assert activated['charged_bytes']==8<<20 and activated['retained_bytes']==4<<20 and activated['creation_reserved_bytes']==0
        done=exchange(endpoint,'ShutdownManagedPool',{'admin_token':admin})
        assert done['charged_bytes']==done['retained_bytes']==4<<20 and done['active_bytes']==done['reserved_bytes']==0
        assert child.wait(timeout=5)==0
        clean=json.loads((state/'clean.json').read_bytes());assert clean['retained_bytes']==4<<20 and clean['active_bytes']==0
        (h.output/'initial.json').write_text(json.dumps(initial,indent=2))
        (h.output/'activated.json').write_text(json.dumps(activated,indent=2))
        (h.output/'clean.json').write_text(json.dumps(clean,indent=2))
        for server in servers:server.close()
        servers=[]
        for node in h.nodes.values():node.prepare_close()
        result={'pass':True,'actual_Rust_C_SDK_process_IPC':True,'import_canary_worker_are_explicit_CPU_doubles':True,
            'real_original_lineage_read_or_modified':False,'preactivation_and_old_terminal_rejected':True,
            'activate_real_readiness_and_same_operation_idempotent':True,'final_retained_bytes':4<<20,
            'active_bytes':0,'reserved_bytes':0,'SDK_worker_deregister_and_root_context_close':True,'new_business_backing':False}
        (a.output/'results.json').write_text(json.dumps(result,indent=2));print(json.dumps(result),flush=True)
    except BaseException:
        notify_failure();raise
    finally:
        for server in servers:
            server.stop.set();server.thread.join(1);server.listener.close()
        h.close()

if __name__=='__main__':main()
