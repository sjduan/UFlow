#!/usr/bin/env python3
"""Real E1-E4 driver. Prepared for approved B execution, never fixture PASS.

Run under E0 kind=guest. No SSH, device query, transport workaround or SDK copy.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
import uuid
from mem_service_pool_bridge import GuestNode, BridgeServer, notify_failure
from uflow.mem_service_pool_v2 import exchange, PoolV2Error, PoolV2Unknown
from pool_peer_io import PeerIO
from pool_matrix_contract import guard_intervals, guard_chunks, CANARY_BYTES
from pool_startup import Startup

U,M=65536,1024*1024


def expected(seed,offset,n):
    h=1469598103934665603
    for i in range(offset,offset+n):
        byte=((i*1315423911+seed*2654435761)^((i>>7)*17)^(i>>19))&255
        h=((h^byte)*1099511628211)&((1<<64)-1)
    return '%016x'%h


class Peer:
    def __init__(self,source,endpoint,node,pool,env,access='rw'):
        self.node=node
        self.child=subprocess.Popen([sys.executable,'-B',str(source/'examples/mem_service_pool/client_peer.py'),endpoint,node],
            stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,bufsize=0,env=env)
        self.io=PeerIO(self.child,notify_failure)
        self.binding=self.call('register',pool=pool,access=access)

    def call(self,op,expect_error=False,expect_unknown=False,**values):
        result=self.io.transaction({'op':op,**values})
        if type(result) is not dict or result.get('status') not in ('ok','error'):
            notify_failure()
            raise RuntimeError('invalid independent client reply')
        if expect_error:
            if result['status']!='error' or result.get('class')!=('PoolV2Unknown' if expect_unknown else 'PoolV2Error'):
                notify_failure()
                raise RuntimeError('expected known rejection; got '+str(result))
            return result
        if result['status']!='ok':
            notify_failure()
            raise RuntimeError(str(result)+'; bounded stderr: '+self.io.diagnostic())
        return result['result']

    def close(self):
        result=self.call('close');self.child.stdin.close()
        if self.child.wait(timeout=10)!=0:raise RuntimeError('client exit after unmap failed')
        self.io.reaped()
        return result


def wait_socket(path,child):
    deadline=time.monotonic()+15
    while not Path(path).exists():
        if child.poll() is not None or time.monotonic()>=deadline:
            raise RuntimeError('private daemon did not become reachable')
        time.sleep(.05)


def matrix(root,config,build):
    if 'UFLOW_ENV_CLOSE_FD' not in os.environ:raise RuntimeError('accepted E0 supervisor required')
    out=Path(config['output']);source=Path(build['source'])
    if not out.is_relative_to(root/'runs') or not out.is_dir():raise ValueError('admitted unique Guest job required')
    for path,digest in ((Path(build['image']),build['image_sha256']),
                        (Path(build['daemon']),build['daemon_sha256']),
                        (Path(build['fence_shim']),build['fence_shim_sha256']),
                        (source/'.uflow-source.json',build['source_manifest_sha256'])):
        if hashlib.sha256(path.read_bytes()).hexdigest()!=digest:raise RuntimeError('source/binary binding drift')
    for rel,digest in build['installed_sdk_manifest'].items():
        if hashlib.sha256((root/'env/mem-service-sdk'/rel).read_bytes()).hexdigest()!=digest:
            raise RuntimeError('fixed SDK install changed before run')
    for node in config['nodes']:
        argv=node['boot_argv']
        if argv[argv.index('-initrd')+1]!=build['image']:raise RuntimeError('Guest is not using the source-bound image')
    sys.path.insert(0,str(root/'env/guest-kit004'))
    from uflow_guest_group import run
    lock=threading.Lock()
    log=(out/'pool-events.jsonl').open('x')
    def event(value):
        with lock:
            if value.get('event')=='worker-log':
                with (out/('worker-'+value['node']+'.'+value['stream']+'.log')).open('ab') as raw:
                    raw.write(bytes.fromhex(value['hex']))
            log.write(json.dumps({'host_monotonic_ns':time.monotonic_ns(),**value})+'\n');log.flush()
    lower_socket=str(out/'l.sock')
    lower_config=out/'lower.conf'
    lower_config.write_text('listen=unix:'+lower_socket+'\nstore='+str(out/'lower.store')+
        '\nrequired_provider=nodeA\nrequired_provider=nodeB\nprovider_lease_ms=10000\nallocation_home_provider=nodeA\n')
    lower_binary=root/'env/mem-service-sdk/bin/linqu_mem_service'
    lower_log=(out/'lower.log').open('xb')
    lower=subprocess.Popen([str(lower_binary),'serve','--config',str(lower_config)],stdout=lower_log,stderr=subprocess.STDOUT)
    wait_socket(lower_socket,lower)
    # One authority is already carrying the old responsibility before any new
    # Guest/bootstrap. It reserves both new canaries before their real prepare.
    realm='simulated:mem-service:'+str(uuid.uuid4());session=str(uuid.uuid4())
    provider_config=out/'provider.json';prepared_config=out/'prepared-provider.json'
    endpoint=str(out/'u.sock');state=out/'state'
    provider_config.write_text(json.dumps({'schema':2,'backend':'mem_service_obmm','realm':realm,'session':session,
        'home':'nodeA','deferred_bootstrap':True,'readiness_file':str(prepared_config),
        'nodes':[{'node':name,'socket':str(out/('a.sock' if name=='nodeA' else 'b.sock'))} for name in ('nodeA','nodeB')],
        'reconciliation':{'assessment':str(source/'examples/mem_service_pool/reconciliation/assessment.json'),
                          'bindings':str(source/'examples/mem_service_pool/reconciliation/bindings.json'),
                          'retained8_assessment':str(source/'examples/mem_service_pool/reconciliation/retained8-assessment.json')}},indent=2))
    env={**os.environ,'PYTHONPATH':str(source/'sdk/python'),'PYTHONDONTWRITEBYTECODE':'1',
         'LD_LIBRARY_PATH':str(Path(build['fence_shim']).parent)}
    daemon_log=(out/'catalog.log').open('xb')
    daemon=subprocess.Popen([build['daemon'],'--host-pool','--socket',endpoint,'--pool-state-root',str(state),
        '--pool-budget-bytes',str(128*M),'--pool-mem-service-config',str(provider_config)],env=env,
        stdout=daemon_log,stderr=subprocess.STDOUT)
    wait_socket(endpoint,daemon)
    imported=json.loads((state/'state.json').read_bytes())
    if imported['charged_bytes']!=8*M or imported['retained_bytes']!=8*M or imported['creation_reserved_bytes']!=4*M or imported['reconciliation']['phase']!='VerifiedUncreatedTerminal' or imported['reconciliation']['successor_phase']!='Retained8':
        raise RuntimeError('single-ledger retained8 succession and new canary reservation not established; no Guest')
    event({'event':'retained8-succession-established-before-Guest','charged_bytes':8*M,'retained_bytes':8*M,
        'current_canary_reserved_bytes':4*M,'predecessor_cancel64_repeated':False,
        'proof_sha256':imported['reconciliation']['proof_sha256'],'original_SDK_close_claimed':False})
    admin=(state/'admin.token').read_text()
    def callback(channels,identities):
        nodes={}
        servers=[]
        peers=[]
        pool=None
        try:
            for identity in identities:
                name=identity['node']
                nodes[name]=GuestNode(channels[name],identity,lower_socket,event,realm)
                nodes[name].thread.start()
                nodes[name].send('start')
            for node in nodes.values():
                if not node.started.wait(10) or node.failure:raise RuntimeError('Guest agent start failed')
            startup=Startup(nodes,realm,event)
            bootstrap,readiness=startup.run()
            bootstrap_bytes=len(bootstrap)*CANARY_BYTES
            bindings=[]
            for name,node in nodes.items():
                bridge_endpoint=out/('a.sock' if name=='nodeA' else 'b.sock')
                server=BridgeServer(bridge_endpoint,node);servers.append(server);server.thread.start()
                identity=node.identity
                bindings.append({'node':name,'boot_id':identity['boot_id'],'agent_session':identity['agent_session'],
                                 'incarnation':identity['incarnation'],'socket':str(bridge_endpoint),**bootstrap[name],
                                 'startup':readiness[name]})
            prepared_config.write_text(json.dumps({'schema':2,'backend':'mem_service_obmm','realm':realm,'session':session,
                                                   'home':'nodeA','nodes':bindings},indent=2))
            activated=exchange(endpoint,'ActivateManagedBootstrap',{'admin_token':admin})
            if activated['bootstrap_activated'] is not True:raise RuntimeError('actual current bootstrap not committed into one ledger')
            pool=exchange(endpoint,'CreateManagedPool',{'admin_token':admin,'nbytes':64*M})
            base={key:pool[key] for key in ('pool_id','pool_epoch','provider_session')}
            event({'event':'E1-one-allocation','pool':pool,'provider':json.loads(prepared_config.read_text())})
            # Real server negatives, not local SDK-validator substitutes.
            for field,value in [('pool_epoch','stale-epoch'),('provider_session','stale-provider')]:
                try:exchange(endpoint,'RegisterPoolClient',{**base,field:value,'node':'nodeB','access':'rw'})
                except PoolV2Unknown:raise
                except PoolV2Error as error:event({'event':'E1-negative','field':field,'outcome':type(error).__name__})
                else:raise RuntimeError('identity negative unexpectedly accepted')
            peers=[Peer(source,endpoint,node,pool,env) for node in ('nodeA','nodeB','nodeA')]
            if len({p.binding['receipt']['guest_pid'] for p in peers if p.node=='nodeA'})!=2:
                raise RuntimeError('A/C are not independent Guest processes')
            a,b,c=peers
            block=a.call('allocate',size=64*M)
            for field,value in [('backend','posix_shm_simpler'),('realm','physical:wrong'),('node','nodeB'),
                                 ('incarnation',0),('rights','r'),('offset',U)]:
                stale={**a.binding,field:value}
                a.call('raw',expect_error=True,method='GetManagedPoolSnapshot',values={'access_binding_v2':stale})
                event({'event':'E1-real-binding-negative','field':field,'actual_pool':pool['pool_id'],'known_rejected':True})
            for change in ({'expected_generation':2},{'byte_offset':(1<<64)-65536},{'nbytes':65*M}):
                values={'block':block,'mode':'w','expected_generation':block['generation'],'byte_offset':0,'nbytes':U,**change}
                a.call('raw',expect_error=True,method='AcquirePoolRange',values=values)
            for change in ({'node':'unbound'},{'access':'write'}):
                try:exchange(endpoint,'RegisterPoolClient',{**base,'node':'nodeB','access':'rw',**change})
                except PoolV2Unknown:raise
                except PoolV2Error:event({'event':'E1-negative','fields':change,'known_rejected':True})
                else:raise RuntimeError('node/rights negative unexpectedly accepted')
            # Initialize and prove the entire backing in <=20MiB pieces; no payload on Host.
            for off in range(0,64*M,16*M):
                lease=a.call('acquire',block=block,mode='w',offset=off,nbytes=16*M)
                a.call('execute',lease=lease,pattern_id=0)
            cases=[(a,b,U,20*M,17),(b,a,24*M+U,20*M,29),(a,b,48*M+U,2*U,41)]
            written=[]
            for writer,reader,off,n,seed in cases:
                started=time.monotonic_ns()
                lease=writer.call('acquire',block=block,mode='w',offset=off,nbytes=n)
                done=writer.call('execute',lease=lease,pattern_id=seed)
                read=reader.call('acquire',block=block,mode='r',offset=off,nbytes=n)
                verified=reader.call('execute',lease=read,pattern_id=seed)
                digest=expected(seed,off,n)
                if done['receipt']['checksum']!=digest or verified['receipt']['checksum']!=digest:
                    raise RuntimeError('independently computed full-range hash mismatch')
                reader.call('release',lease=read)
                written.append((off,off+n))
                event({'event':'E2-direction','writer':writer.binding,'reader':reader.binding,'offset':off,'nbytes':n,
                       'pattern_id':seed,'write':done,'read':verified,'expected_checksum':digest,
                       'start_host_ns':started,'end_host_ns':time.monotonic_ns(),'clock_domain':'B-host'})
            readonly=Peer(source,endpoint,'nodeB',pool,env,access='r')
            ro_lease=readonly.call('acquire',block=block,mode='r',offset=U,nbytes=20*M)
            readonly.call('execute',lease=ro_lease,pattern_id=17)
            readonly.call('release',lease=ro_lease)
            readonly.call('raw',expect_error=True,method='AcquirePoolRange',values={
                'block':block,'mode':'w','expected_generation':block['generation'],'byte_offset':0,'nbytes':U})
            event({'event':'E1-real-readonly-mapping','binding':readonly.binding,'close':readonly.close()})
            # Define all expected guard bytes before consulting returned segmentation.
            planned_written=[(U,U+20*M),(24*M+U,44*M+U),(48*M+U,48*M+3*U)]
            if written!=planned_written:raise RuntimeError('executed write intervals differ from independent plan')
            guards=guard_intervals(64*M,planned_written)
            event({'event':'E2-expected-guard-coverage','pool_bounds':[0,64*M],
                   'written':planned_written,'guards':guards,'nbytes':sum(end-begin for begin,end in guards)})
            snapshot=a.call('snapshot')
            segments=snapshot['pools'][0]['ranges']['allocations'][0]['segments']
            verified_guards=0
            for begin,count in guard_chunks(guards,segments,16*M):
                lease=b.call('acquire',block=block,mode='r',offset=begin,nbytes=count)
                result=b.call('execute',lease=lease,pattern_id=0);b.call('release',lease=lease)
                digest=expected(0,begin,count)
                if result['receipt']['checksum']!=digest:raise RuntimeError('independent guard checksum mismatch')
                verified_guards+=count
                event({'event':'E2-guard','offset':begin,'nbytes':count,'expected_checksum':digest,'result':result})
            if verified_guards!=sum(end-begin for begin,end in guards):raise RuntimeError('guard verification incomplete')
            # Same allocation: A and B keep real leases while C completes disjoint I/O.
            la=a.call('acquire',block=block,mode='w',offset=52*M,nbytes=2*U)
            lb=b.call('acquire',block=block,mode='w',offset=52*M+2*U,nbytes=2*U)
            before=c.call('snapshot')
            for mode in ('r','w'):
                c.call('acquire',expect_error=True,block=block,mode=mode,offset=52*M+U,nbytes=2*U)
            lc=c.call('acquire',block=block,mode='w',offset=52*M+4*U,nbytes=U)
            done=c.call('execute',lease=lc,pattern_id=73)
            rc=c.call('acquire',block=block,mode='r',offset=52*M+4*U,nbytes=U)
            same=a.call('acquire',block=block,mode='r',offset=52*M+4*U,nbytes=U)
            c.call('execute',lease=rc,pattern_id=73)
            after=c.call('snapshot')
            tokens={lease['lease_id'] for lease in after['pools'][0]['ranges']['leases']}
            if not {la['lease_id'],lb['lease_id']}<=tokens:raise RuntimeError('A/B released before C completed')
            event({'event':'E3-real-lifetime-barrier','before':before,'C_write':done,'after':after,
                   'independent_host_pids':[p.child.pid for p in peers],
                   'clock_domain':'B-host','cross_guest_clock_subtraction':False})
            c.call('release',lease=rc);a.call('release',lease=same)
            a.call('release',lease=la,write_not_started=True);b.call('release',lease=lb,write_not_started=True)
            uncertain=c.call('acquire',block=block,mode='w',offset=52*M+8*U,nbytes=U)
            c.call('raw',method='ReportPoolUnknown',values={'lease':uncertain,
                'reason':'controlled client-reported uncertainty; no injected device or provider fault'})
            held=c.call('snapshot')
            if not held['pools'][0]['frozen'] or held['charged_bytes']!=64*M+bootstrap_bytes+8*M:
                raise RuntimeError('original pool and budget not retained by uncertainty')
            b.call('acquire',expect_error=True,expect_unknown=True,block=block,mode='r',offset=U,nbytes=U)
            try:exchange(endpoint,'CreateManagedPool',{'admin_token':admin,'nbytes':64*M})
            except PoolV2Unknown:pass
            else:raise RuntimeError('replacement pool admitted while original obligation unknown')
            event({'event':'E4-explicit-client-uncertainty','snapshot':held,'injection_kind':'control-only, no actual device fault',
                   'original_pool':pool['pool_id'],'replacement_pool_not_created':True})
            event({'event':'E4-first-client-unmapped','receipt':c.close()})
            reconciled=a.call('snapshot')
            if reconciled['pools'][0]['frozen'] or reconciled['charged_bytes']!=64*M+bootstrap_bytes+8*M:
                raise RuntimeError('actual same-context unmap did not reconcile only this client')
            invalid=[s for s in reconciled['pools'][0]['ranges']['allocations'][0]['segments'] if s['state']=='Invalid']
            if not any(s['offset']==52*M+8*U and s['nbytes']==U for s in invalid):
                raise RuntimeError('uncertain content became committed instead of Invalid')
            if any(l['state']!='Closed' for l in reconciled['catalog_range_leases']
                   if l['client_session']==c.binding['client_session']):
                raise RuntimeError('Catalog lease remained open after same-context lower unmap')
            check=b.call('acquire',block=block,mode='r',offset=U,nbytes=20*M)
            b.call('execute',lease=check,pattern_id=17);b.call('release',lease=check)
            a.close();b.close()
            retired=exchange(endpoint,'DestroyManagedPool',{**base,'admin_token':admin})
            final=exchange(endpoint,'GetManagedPoolSnapshot',base)
            if final['charged_bytes']!=bootstrap_bytes+8*M or final['creation_reserved_bytes']:
                raise RuntimeError('business retire did not leave exactly the two owned canaries')
            shutdown=exchange(endpoint,'ShutdownManagedPool',{'admin_token':admin})
            if shutdown['charged_bytes']!=8*M or shutdown['retained_bytes']!=8*M or shutdown['active_bytes']!=0 or shutdown['bootstrap_closed'] is not True:
                raise RuntimeError('canary checked-close/final ledger not confirmed')
            if daemon.wait(timeout=10)!=0:raise RuntimeError('Catalog normal shutdown failed')
            event({'event':'E4-two-layer-release','retired':retired,'catalog_before_canary_close':final,'shutdown':shutdown})
            for server in servers:server.close()
            for node in nodes.values():node.prepare_close()
            (out/'E1-E4.json').write_text(json.dumps({'state':'passed','real_Guest_mapping':True,
                'pool_id':pool['pool_id'],'data_bytes_on_control_bridge':0,'capacity':64*M,
                'bootstrap_bytes':bootstrap_bytes,'retained_predecessor_canary_bytes':8*M,
                'peak_B_responsibility':64*M+bootstrap_bytes+8*M,'final_B_charged_bytes':8*M,
                'A_historical_bytes_unqueried':128*M,'global_responsibility_zero_claimed':False},indent=2))
        except BaseException:
            notify_failure()  # Never perform potentially blocking state capture before notice.
            event({'event':'matrix-failure','original_pool':pool,'same_pool_retained':True,
                   'unknown_not_reconciled_by_process_death':True})
            raise
    try:
        run(config,control=callback)
        lower.send_signal(signal.SIGTERM)
        if lower.wait(timeout=10)!=0:raise RuntimeError('lower metadata daemon shutdown failed')
        event({'event':'normal-control-daemons-reaped','lower_pid':lower.pid,'lower_wait':0})
    except BaseException:
        notify_failure()
        # External supervisor owns bounded TERM/KILL/wait for this exact subtree.
        raise
    finally:
        log.close()


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('config',type=Path);p.add_argument('build_receipt',type=Path)
    args=p.parse_args()
    try:matrix(Path('/workspace/uflow/runs/mem-service-shared-pool-20261108'),
        json.loads(args.config.read_bytes()),json.loads(args.build_receipt.read_bytes()))
    except BaseException:
        notify_failure()
        raise
