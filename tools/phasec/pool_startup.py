"""Actual bootstrap -> public directory register -> worker refresh admission.

The optional in-process Host provider registry's data_plane_ready is logged
unchanged. Managed allocations use the frozen directory gate; Guest channel
readiness is independently established by each endpoint's real canary verify.
"""
import json
import time
from uflow._pool_bridge_v2 import Frame


class NotReady(RuntimeError):
    pass


class Control:
    def __init__(self, nodes, realm, *, timeout=120):
        self.nodes,self.realm,self.timeout=nodes,realm,timeout
        self.sequence=0
        self.last={}

    def call(self, name, body):
        self.sequence+=1
        if self.sequence>=1<<32:raise RuntimeError('bootstrap sequence space exhausted')
        node=self.nodes[name];identity=node.identity
        frame=Frame(self.sequence,self.sequence,self.realm,name,identity['boot_id'],identity['agent_session'],
                    identity['incarnation'],json.dumps({'schema':2,**body},separators=(',',':')).encode())
        reply=node.exchange(frame,timeout=self.timeout)
        if not frame.matches(reply):raise RuntimeError('startup completion identity mismatch')
        receipt=json.loads(reply.body)
        if receipt.get('schema')!=2 or receipt.get('phase')!='completed':
            raise RuntimeError('startup operation unconfirmed; original context retained; bounded receipt: '+
                               json.dumps(receipt,separators=(',',':'))[:2048])
        self.last[(name,body['op'])]=frame.request
        return receipt


def validate_readiness(nodes, registrations, workers, statuses, directory_evidence, refresh_evidence, now_ns):
    if set(nodes)!={'nodeA','nodeB'}:raise NotReady('two bound nodes required')
    epochs=set()
    for name,node in nodes.items():
        identity=node.identity;registration=registrations[name];worker=workers[name];status=statuses[name]
        expected=(name,identity['boot_id'],identity['incarnation'],1,1)
        for value in (registration,status):
            actual=tuple(value.get(k) for k in ('node','boot_id','incarnation','readiness_generation','capabilities'))
            if actual!=expected or value.get('directory_registered') is not True or value.get('channel_ready') is not True:
                raise NotReady('registration/channel boot identity not established')
        if (worker.get('worker_alive') is not True or worker.get('worker_ready') is not True or
                worker.get('worker_refresh_count',0)<1 or worker.get('incarnation')!=identity['incarnation'] or
                worker.get('readiness_generation')!=1 or worker.get('capabilities')!=1 or
                not node.worker or (worker.get('worker_pid'),worker.get('worker_birth'))!=(node.worker['pid'],node.worker['birth'])):
            raise NotReady('real worker identity/liveness/refresh not established')
        directory=directory_evidence[name]
        if not directory or directory['origin']['role']!='root' or directory['operation']!=0x78 or directory['status']:
            raise NotReady('actual SDK directory status completion missing')
        meta=directory['response_metadata']
        try:
            lease=int(meta['lease_ms']);epoch=int(meta['directory_epoch'])
            if lease<100 or epoch<=0 or meta['provider_required_count']!='2' or meta['provider_active_count']!='2' or meta['provider_directory_ready']!='1':
                raise NotReady('managed directory is not ready')
            if status['required_count']!=2 or status['active_count']!=2 or status['directory_ready'] is not True or status['directory_epoch']!=epoch or status['lease_ms']!=lease:
                raise NotReady('SDK completion disagrees with raw directory reply')
            if status['host_registry_data_plane_ready']!=(meta['data_plane_ready']=='1'):
                raise NotReady('optional Host registry flag was relabelled')
            providers=meta.get('providers',[])
            if len(providers)!=2 or {p['node'] for p in providers}!=set(nodes):raise NotReady('missing/duplicate/foreign provider')
            for provider in providers:
                wanted=nodes[provider['node']].identity
                if (provider['incarnation']!=wanted['incarnation'] or provider['readiness_generation']!=1 or
                    provider['capabilities']!=1 or not 0<=provider['age_ms']<lease//2):
                    raise NotReady('stale/expired/wrong-incarnation directory provider')
            if worker['lease_ms']!=lease or not 0<=worker['refresh_age_ms']<lease//2:
                raise NotReady('worker heartbeat too old for admission')
            refreshes=[r for r in refresh_evidence[name] if r['origin']['role']=='worker' and
                (r['origin']['pid'],r['origin']['birth'])==(worker['worker_pid'],worker['worker_birth']) and
                r['request_metadata'].get('node_id')==name and r['request_metadata'].get('incarnation')==str(identity['incarnation']) and
                r['request_metadata'].get('readiness_generation')=='1' and r['status']==0 and r['operation']==0x77]
            if not refreshes or now_ns-max(r['host_monotonic_ns'] for r in refreshes)>=lease*500000:
                raise NotReady('no fresh completion from the actual worker process')
            if now_ns-directory['host_monotonic_ns']>=lease*500000:raise NotReady('directory observation expired')
            epochs.add(epoch)
        except (KeyError,TypeError,ValueError):
            raise NotReady('malformed public readiness evidence') from None
    if len(epochs)!=1:raise NotReady('directory changed during admission observations')
    return next(iter(epochs))


class Startup:
    def __init__(self,nodes,realm,event,*,timeout=120):
        if set(nodes)!={'nodeA','nodeB'}:raise ValueError('two actual node bindings required')
        self.nodes,self.event=nodes,event
        self.control=Control(nodes,realm,timeout=timeout)
        self.registrations={}
        self.bootstrap=None

    def bootstrap_and_register(self):
        from pool_matrix_contract import bootstrap_nodes
        self.bootstrap=bootstrap_nodes(self.nodes,self.control.realm,self.event,control=self.control)
        for name in self.nodes:
            receipt=self.control.call(name,{'op':'worker_bootstrap'})
            diagnostic=receipt.get('diagnostic',{})
            if (receipt.get('worker_bootstrap_ready') is not True or diagnostic.get('stage')!='ready' or
                    diagnostic.get('errno')!=0 or diagnostic.get('node_id')!=(0 if name=='nodeA' else 1) or
                    diagnostic.get('node_count')!=2 or diagnostic.get('backing_allocation_attempted') is not False):
                raise NotReady('platform worker preflight receipt invalid')
            self.event({'event':'worker-platform-bootstrap','node':name,'receipt':receipt})
            receipt=self.control.call(name,{'op':'provider_register'})
            self.registrations[name]=receipt
            self.event({'event':'directory-registered-after-canary','node':name,'receipt':receipt})
        return self.bootstrap

    def start_workers(self):
        for name in self.nodes:
            receipt=self.control.call(name,{'op':'worker_start'})
            node=self.nodes[name]
            if receipt.get('worker_started') is not True or not node.worker or (
                receipt.get('worker_pid'),receipt.get('worker_birth'))!=(node.worker['pid'],node.worker['birth']):
                raise NotReady('worker start receipt does not match real PID/birth')

    def check_once(self):
        workers,statuses,directories,refreshes={},{},{},{}
        for name,node in self.nodes.items():
            workers[name]=self.control.call(name,{'op':'worker_state'})
            statuses[name]=self.control.call(name,{'op':'provider_status'})
            directories[name]=node.directory
            if not directories[name] or directories[name]['origin']['control_request']!=self.control.last[(name,'provider_status')]:
                raise NotReady('directory completion correlation mismatch')
            refreshes[name]=list(node.refreshes.values())
        epoch=validate_readiness(self.nodes,self.registrations,workers,statuses,directories,refreshes,time.monotonic_ns())
        proof={name:{'registration':self.registrations[name],'worker':workers[name],'directory':statuses[name],
                     'directory_epoch':epoch,'managed_ready':True} for name in self.nodes}
        self.event({'event':'managed-admission-ready','nodes':proof,'generic_ready_exit_not_used':True})
        return proof

    def wait_ready(self,seconds=15):
        deadline=time.monotonic()+seconds
        while True:
            try:return self.check_once()
            except NotReady:
                if time.monotonic()>=deadline:raise
                time.sleep(.05)

    def run(self):
        self.bootstrap_and_register();self.start_workers()
        return self.bootstrap,self.wait_ready()

    def stop_infrastructure(self):
        # Only after actual business/holder/map/pin obligations have drained.
        for name in self.nodes:
            stopped=self.control.call(name,{'op':'worker_stop'})
            if stopped.get('worker_stopped') is not True or stopped.get('directory_deregistered') is not True:
                raise RuntimeError('worker SDK close unconfirmed')
        for name in self.nodes:
            receipt=self.control.call(name,{'op':'provider_drain'})
            if receipt.get('directory_drained') is not True:raise RuntimeError('directory drain unconfirmed')
        for name in self.nodes:
            receipt=self.control.call(name,{'op':'bootstrap_close'})
            if receipt.get('canary_closed') is not True:raise RuntimeError('canary checked-close unconfirmed')
