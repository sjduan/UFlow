"""C's authenticated managed-read orchestration. Metadata only, no data path.

The concrete E codec is pinned, but its final alias schema/profile/wrapper has
not yet been accepted. Production construction deliberately fails before IO.
Private HOST controls exercise the actual state machine, never certify QEMU.
"""
from dataclasses import replace
import hashlib,json,re,time,types
from pathlib import Path
from daemon_peer_auth import AuthenticatedPeer
from uflow._pool_bridge_v2 import metadata,POOL

CODEC_SHA='84e32ea2325a36b870351a9a000690c0bf32cbf754330da94b316efeaf805284'
def check(ok,why):
    if not ok:raise ValueError(why)
def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode('utf-8')
def sha(value):return hashlib.sha256(value).hexdigest()
def codec():
    path=Path(__file__).parents[2]/'examples/mem_service_pool/managed_cache_contract/v1/uflow_managed_cache_codec_v1.py'
    raw=path.read_bytes();check(sha(raw)==CODEC_SHA,'external E codec pin mismatch')
    module=types.ModuleType('_pinned_managed_codec');exec(compile(raw,str(path),'exec'),module.__dict__);return module
def production_controller(*args,**kwargs):
    raise ValueError('production-not-ready: accepted new native alias schema/profile/wrapper/current witness required before control IO')

READ={'schema','op','client_session','offset','nbytes','lease_id','content_version','pattern_id'}
STRINGS={'cache_pool_id','cache_pool_epoch','cache_provider_session','cache_backing_realm','cache_backing_provider',
         'cache_backing_resource','cache_mode','cache_content_id','cache_operation_id','cache_frame_sha256','cache_authority_sha256'}
NUMBERS={'cache_version','cache_resource_id','cache_resource_generation','cache_placement_id','cache_binding_id',
         'cache_backing_generation','cache_access_generation','cache_lease_generation','cache_block_id','cache_block_generation','cache_control_generation'}
def authority(frame):
    body=metadata(frame.body);check(frame.kind==POOL and frame.status==0 and frame.error==0 and len(frame.body)<=1800,'managed frame bound/type')
    check(set(body)==READ|STRINGS|NUMBERS,'exact server-minted authority schema required')
    check(all(type(body[k]) is str and body[k] for k in STRINGS),'typed authority tokens required')
    check(all(type(body[k]) is int and body[k]>0 for k in NUMBERS),'typed positive authority generations required')
    check(body['schema']==2 and type(body['schema']) is int and body['cache_version']==1 and body['op']=='read' and body['cache_mode']=='r','only actual managed read')
    check(all(type(body[k]) is int and 0<=body[k]<1<<64 for k in ('offset','nbytes','content_version','pattern_id')) and body['nbytes']>0 and body['content_version']>0,'typed read range/content required')
    check(all(type(body[k]) is str and body[k] for k in ('client_session','lease_id')),'actual client/range lease required')
    check(body['cache_backing_provider']=='mem_service_obmm' and body['cache_backing_realm']==frame.realm and body['cache_backing_generation']==body['cache_resource_generation'],'backing authority mismatch')
    check(frame.request==frame.correlation==body['cache_control_generation'],'one-use actual frame correlation required')
    actual={'request':frame.request,'correlation':frame.correlation,'realm':frame.realm,'node':frame.node,'boot':frame.boot,'session':frame.session,'generation':frame.generation}
    check(body['cache_frame_sha256']==sha(canonical(actual)),'actual bound frame mismatch')
    claimed=body.pop('cache_authority_sha256');check(re.fullmatch('[0-9a-f]{64}',claimed) and claimed==sha(canonical(body)),'server authority digest mismatch')
    body['cache_authority_sha256']=claimed;return body

class PublicationBook:
    """Only successful already-authenticated worker relay calls feed this book."""
    def __init__(self):self._rows={};self._codec=codec()
    def observe_worker_publication(self,identity,origin,op,request,reply):
        if op!=0x7a or origin['role']!='worker' or reply.get('status')!='ok':return
        # Origin PID/birth and complete lower wire have already been checked by
        # GuestNode.relay. Do not publish a naked address or decode offset92.
        key=request.get('key');generation=request.get('generation');incarnation=request.get('incarnation')
        check(key and request.get('node_id')==identity['node'] and incarnation==str(identity['incarnation']) and
              generation and generation.isdecimal() and int(generation)>0 and reply.get('key')==key and
              reply.get('generation')==generation and reply.get('descriptor_hex')==request.get('descriptor_hex'),'actual worker publication identity mismatch')
        public=self._codec.descriptor_metadata(reply['descriptor_hex'])
        row={'public':public,'home':identity['node'],'incarnation':identity['incarnation'],'generation':int(generation),'key':key}
        index=(key,int(generation));check(index not in self._rows or self._rows[index]==row,'publication identity drift');check(index in self._rows or len(self._rows)<64,'bounded publication registry');self._rows[index]=row
    def binding(self,body):
        row=self._rows.get((body['cache_pool_id'],body['cache_resource_generation']));check(row is not None,'actual native worker publication missing')
        check(body['cache_backing_resource']==f"{row['home']}:{row['incarnation']}:{row['key']}" and
              body['offset']+body['nbytes']<=row['public']['size'],'actual backing/range publication mismatch');return row

def strict_reply(c,wire,value,pid,alias):
    # E's interface-v1 validator is intentionally supplemented: Python bool
    # and float equality must not masquerade as integer counters/version.
    reply=c.validate_reply(wire,value,pid);command=c.decode(wire)
    keys={'version','feature','pid','operation','request_sha256','control_generation',*c.HASHES,'code','active','in_flight','cache_pages','native_alias','counters'}
    check(type(reply) is dict and set(reply)==keys,'exact native reply schema required')
    check(type(reply['version']) is int and reply['version']==1 and type(reply['pid']) is int and reply['pid']==pid and type(reply['active']) is bool,'strict native reply types')
    check(all(type(reply[k]) is int and 0<=reply[k]<1<<64 for k in ('in_flight','cache_pages')),'strict native outstanding counters')
    check(type(reply['counters']) is dict and len(reply['counters'])<=32 and all(type(k) is str and type(v) is int and 0<=v<1<<64 for k,v in reply['counters'].items()),'strict bounded native counters')
    check(canonical(reply['native_alias'])==canonical(alias),'full independently resolved native alias mismatch')
    check(reply['code']=='ok','native denial/busy is not an ACK')
    if command['operation'] in ('begin','end'):
        check(reply['active'] is (command['operation']=='begin') and reply['in_flight']==0 and reply['cache_pages']==0,'empty begin or drained end ACK required')
    return reply

class _HostController:
    """HOST-only seam. Final E alias resolver and QMP adapter are not invented."""
    def __init__(self,publication,bindings):
        self.publication,self.bindings=publication,bindings;self.c=codec();self.highwater={};self.live={};self.audit=[]
    def _control(self,binding,command,deadline,auth):
        auth.recheck();check(time.monotonic()<deadline,'bounded managed control deadline')
        binding['recheck']() # Actual production implementation must verify PID/birth/ELF/profile, both peers.
        wire=self.c.encode(command)
        reply=binding['control'](wire,min(deadline,time.monotonic()+5))
        check(time.monotonic()<deadline,'native control reply arrived after operation deadline')
        auth.recheck();binding['recheck']()
        result=strict_reply(self.c,wire,reply,binding['pid'],binding['alias'])
        self.audit.append({'operation':command['operation'],'request_sha256':self.c.request_sha256(wire),'reply':result})
        if len(self.audit)>1024:self.audit.pop(0)
        return result
    def execute(self,node,frame,auth,timeout=120):
        check(type(auth) is AuthenticatedPeer,'actual Catalog SO_PEERCRED authentication required');auth.recheck()
        deadline=time.monotonic()+min(timeout,120);body=authority(frame);generation=body['cache_control_generation'];key=(frame.realm,frame.node,frame.boot,frame.session,frame.generation)
        check(node.command_lock.acquire(timeout=max(0,deadline-time.monotonic())),'same node control lock deadline')
        try:
            check(generation>self.highwater.get(key,0) and key not in self.live,'stale/live managed control generation')
            row=self.publication.binding(body);peers=self.bindings[(body['cache_pool_id'],frame.node)]
            # These fixtures stand in for the final E-defined complete alias
            # schema. Production never accepts a HOST binding/ready flag.
            check(set(peers)=={'home','importer'},'both actual home and importer native bindings required')
            for binding in peers.values():
                check(type(binding['alias']) is dict and set(binding['alias'])==set(row['public'])|{'map_id','lease_epoch'} and
                      all(type(binding['alias'].get(k)) is type(v) and binding['alias'][k]==v for k,v in row['public'].items()),'complete home/importer public descriptor binding required')
            base={'version':1,'control_generation':generation,'authority_sha256':body['cache_authority_sha256'],'correlation_sha256':body['cache_frame_sha256'],
                  **row['public'],'offset':body['offset'],'length':body['nbytes'],'map_id':0,'lease_epoch':0}
            self.highwater[key]=generation;self.live[key]='BeginPending';end=None;ended=False
            try:
                probes={}
                for role,binding in peers.items():probes[role]=self._control(binding,{**base,'operation':'probe'},deadline,auth)
                imp=peers['importer'];alias=probes['importer']['native_alias']
                # HOST schema only. The actual native alias names must be
                # supplied by E and accepted before production construction.
                check(type(alias.get('map_id')) is int and alias['map_id']>0 and type(alias.get('lease_epoch')) is int and alias['lease_epoch']>0,'actual nonzero native map/lease alias required')
                native={**base,'map_id':alias['map_id'],'lease_epoch':alias['lease_epoch']}
                end={**native,'operation':'end'}
                self._control(imp,{**native,'operation':'begin'},deadline-5,auth);self.live[key]='Reading'
                read=replace(frame,body=canonical({k:v for k,v in body.items() if k in READ}))
                response=node._exchange_locked(read,max(0,deadline-time.monotonic()-5))
                receipt=metadata(response.body)
                check(response.status==0 and receipt.get('phase')=='completed' and receipt.get('all_bytes') is True and receipt.get('visible') is True and type(receipt.get('pins')) is int and receipt['pins']==0 and
                      all(receipt.get(k)==body[k] and type(receipt.get(k)) is int for k in ('offset','nbytes','pattern_id')),'actual unchanged Guest SDK read did not complete')
                self.live[key]='Revoking';ended=True;self._control(imp,end,deadline,auth)
                transcript=sha(canonical(self.audit[-4:]));self.live.pop(key)
                receipt.update({'cache_version':1,'cache_control_generation':generation,'cache_authority_sha256':body['cache_authority_sha256'],'cache_frame_sha256':body['cache_frame_sha256'],
                                'cache_begin_confirmed':True,'cache_end_confirmed':True,'cache_active':False,'cache_in_flight':0,'cache_pages':0,'cache_transcript_sha256':transcript})
                return frame.completion(receipt)
            except BaseException:
                node.notify() # Before diagnostics and any bounded best-effort revoke.
                self.live[key]='Unknown'
                if end is not None and not ended and time.monotonic()<deadline:
                    try:self._control(peers['importer'],end,deadline,auth)
                    except BaseException:pass
                raise
        finally:node.command_lock.release()

def _host_fixture_controller(publication,bindings):return _HostController(publication,bindings)
