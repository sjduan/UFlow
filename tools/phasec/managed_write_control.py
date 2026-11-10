"""Exclusive Catalog W orchestration, distinct from committed-content R.

The private HOST constructor can use E's pinned stable W codec/wrapper or an
explicit normalized double. Production still needs the final profile/context.
The protocol normalizes native metadata; it never carries data bytes.
No production constructor accepts a caller's protocol or readiness flag.
"""
from dataclasses import replace
import re,time
from daemon_peer_auth import AuthenticatedPeer
from managed_cache_control import canonical,sha,check,native_u64
from uflow._pool_bridge_v2 import POOL,metadata

SDK={'schema','op','client_session','offset','nbytes','lease_id','content_version','pattern_id'}
STRINGS={'write_pool_id','write_pool_epoch','write_provider_session','write_backing_realm','write_backing_provider',
         'write_backing_resource','write_mode','write_rights','write_operation_id','write_frame_sha256','write_authority_sha256'}
NUMBERS={'write_version','write_resource_id','write_resource_generation','write_placement_id','write_binding_id',
         'write_backing_generation','write_access_generation','write_lease_generation','write_block_id','write_block_generation','write_control_generation'}

def authority(frame):
    b=metadata(frame.body)
    check(frame.kind==POOL and frame.status==0 and frame.error==0 and len(frame.body)<=1800 and set(b)==SDK|STRINGS|NUMBERS,'exact bounded server W authority required')
    check(all(type(b[k]) is str and b[k] for k in STRINGS|{'client_session','lease_id'}),'typed W identities required')
    check(all(type(b[k]) is int and 0<b[k]<1<<64 for k in NUMBERS),'typed W generations required')
    check(type(b['schema']) is int and b['schema']==2 and b['op']=='write' and b['write_version']==1 and b['write_mode']=='w' and b['write_rights']=='rw','distinct W/RW authority required')
    check(all(type(b[k]) is int and 0<=b[k]<1<<64 for k in ('offset','nbytes','content_version','pattern_id')) and b['nbytes']>0 and b['offset']+b['nbytes']<1<<64 and b['content_version']==0,'exact W range without invented prior content required')
    check(b['write_backing_provider']=='mem_service_obmm' and b['write_backing_realm']==frame.realm and b['write_backing_generation']==b['write_resource_generation'],'W backing identity mismatch')
    check(frame.request==frame.correlation==b['write_control_generation'],'one-use W frame generation required')
    actual={'request':frame.request,'correlation':frame.correlation,'realm':frame.realm,'node':frame.node,'boot':frame.boot,'session':frame.session,'generation':frame.generation}
    check(b['write_frame_sha256']==sha(canonical(actual)),'actual W frame mismatch')
    claimed=b.pop('write_authority_sha256');check(re.fullmatch('[0-9a-f]{64}',claimed) and claimed==sha(canonical(b)),'W server authority digest mismatch')
    b['write_authority_sha256']=claimed;return b

def production_controller(publication=None,*args,admission=None,native_routes=None,event=None,**kwargs):
    from managed_gate_v4 import require_runtime
    admission=require_runtime(admission)  # Before caller/channel interpretation.
    from managed_native_routes_v4 import require_routes
    from managed_write_interface_v1 import _NativeProtocol
    from accepted_producers import ap_bound
    import types
    routes=require_routes(native_routes,admission,publication);check(not args and not kwargs,'exact admitted W constructor required')
    ref=admission.anchors['code_and_data']['write_codec'];raw=ap_bound(ref,128<<10,0o644)
    codec=types.ModuleType('_accepted_c1e_W_codec');codec.__file__=ref['path'];exec(compile(raw,codec.__file__,'exec'),codec.__dict__)
    controller=_WriteController(publication,routes.write,_NativeProtocol(codec));controller.event=event;return controller

class _WriteController:
    def __init__(self,publication,bindings,protocol):
        self.publication,self.bindings,self.protocol=publication,bindings,protocol
        self.highwater={};self.live={};self.audit=[]
    def _control(self,binding,body,public,op,deadline,auth,alias,trace):
        auth.recheck();binding['recheck']();check(time.monotonic()<deadline,'bounded W control deadline')
        actual=binding.get('requires_full_five_seconds') is True
        if actual:check(deadline-time.monotonic()>=5.05,'original W deadline cannot fit five-second native control')
        control_deadline=deadline if actual else min(deadline,time.monotonic()+5)
        # Only the finalized adapter will select E's real operation/version
        # and validate its complete wire reply before normalization here.
        wire=self.protocol.encode(body,public,op,alias)
        check(type(wire) is str and 0<len(wire)<=2048 and wire.isascii(),'bounded W metadata wire required')
        raw=binding['control'](wire,control_deadline)
        check(time.monotonic()<control_deadline,'native W metadata response is late')
        auth.recheck();binding['recheck']()
        reply=self.protocol.validate_reply(wire,raw,binding['pid'])
        check(type(reply['version']) is int and reply['version']==self.protocol.VERSION and reply['mode']=='w' and reply['operation']==op and type(reply['pid']) is int and reply['pid']==binding['pid'] and
              reply['control_generation']==str(body['write_control_generation']) and reply['authority_sha256']==body['write_authority_sha256'] and
              reply['correlation_sha256']==body['write_frame_sha256'] and reply['request_sha256']==self.protocol.request_sha256(wire) and reply['code']=='ok','full authenticated W control correlation required')
        native=reply['native_alias']
        check(set(native)=={*self.protocol.FIELDS64,*self.protocol.FIELDS32,'descriptor_sha256'} and
              all(type(native.get(k)) is (str if k in self.protocol.FIELDS64 else type(v)) and native.get(k)==(str(v) if k in self.protocol.FIELDS64 else v) for k,v in public.items()) and
              all(native_u64(native[k])>0 for k in ('map_id','lease_epoch')) and native['offset']==str(body['offset']) and native['length']==str(body['nbytes']),'full native W publication/alias/range required')
        if alias is not None:check(native==alias,'W native alias/epoch changed around IO')
        if op in ('begin','flush','end'):
            check(reply['active'] is (op!='end') and all(type(reply[k]) is int and reply[k]==0 for k in ('in_flight','dirty_bytes')),'zero dirty/IO native W ACK required')
        if op in ('begin','end'):check(type(reply['cache_pages']) is int and reply['cache_pages']==0,'empty begin or drained W end required')
        if op in ('flush','end'):check(reply['flush_confirmed'] is True,'actual strict-native W flush ACK required')
        record={'operation':op,'request_sha256':self.protocol.request_sha256(wire),'reply':reply};trace.append(record);self.audit.append(record)
        if len(self.audit)>1024:self.audit.pop(0)
        return reply
    def execute(self,node,frame,auth,timeout=120):
        check(type(auth) is AuthenticatedPeer,'actual Catalog SO_PEERCRED W authentication required');auth.recheck()
        deadline=time.monotonic()+min(timeout,120);body=authority(frame);generation=body['write_control_generation'];key=(frame.realm,frame.node,frame.boot,frame.session,frame.generation)
        check(node.command_lock.acquire(timeout=max(0,deadline-time.monotonic())),'whole W node command lock deadline')
        try:
            check(generation>self.highwater.get(key,0) and key not in self.live,'stale/live/Unknown W operation')
            row=self.publication._rows.get((body['write_pool_id'],body['write_resource_generation']))
            check(row is not None and body['write_backing_resource']==f"{row['home']}:{row['incarnation']}:{row['key']}" and body['offset']+body['nbytes']<=row['public']['size'],'actual W native worker publication missing/mismatched')
            peers=self.bindings[(body['write_pool_id'],frame.node)];check(set(peers)=={'home','importer'},'actual two-peer W provenance required')
            for binding in peers.values():check(type(binding['pid']) is int and binding['pid']>0,'actual owning W native peer required')
            self.highwater[key]=generation;self.live[key]='BeginPending';trace=[];alias=None;ended=False
            try:
                for role,binding in peers.items():
                    result=self._control(binding,body,row['public'],'probe',deadline,auth,None,trace)
                    if role=='importer':alias=result['native_alias']
                imp=peers['importer'];self._control(imp,body,row['public'],'begin',deadline-10.10,auth,alias,trace);self.live[key]='Writing'
                response=node._exchange_locked(replace(frame,body=canonical({k:v for k,v in body.items() if k in SDK})),max(0,deadline-time.monotonic()-10.10))
                r=metadata(response.body)
                check(response.status==0 and r.get('phase')=='completed' and r.get('all_bytes') is True and r.get('visible') is True and type(r.get('pins')) is int and r['pins']==0 and
                      type(r.get('checksum')) is str and r['checksum'] and all(type(r.get(k)) is int and r[k]==body[k] for k in ('offset','nbytes','pattern_id')),'unchanged Guest W stores/verification/publication did not complete')
                # Flush keeps the exclusive W grant active. It is not end ACK.
                self.live[key]='Flushing';self._control(imp,body,row['public'],'flush',deadline-5.05,auth,alias,trace)
                self.live[key]='Revoking';ended=True;self._control(imp,body,row['public'],'end',deadline,auth,alias,trace)
                check([x['operation'] for x in trace]==['probe','probe','begin','flush','end'],'one complete W native transcript required')
                r.update({'write_version':1,'write_control_generation':generation,'write_authority_sha256':body['write_authority_sha256'],'write_frame_sha256':body['write_frame_sha256'],
                          'write_begin_confirmed':True,'write_end_confirmed':True,'write_flush_confirmed':True,'write_active':False,'write_in_flight':0,'write_pages':0,'write_dirty_bytes':0,'write_transcript_sha256':sha(canonical(trace))})
                self.live.pop(key);return frame.completion(r)
            except BaseException:
                node.notify();self.live[key]='Unknown'
                if alias is not None and not ended and time.monotonic()<deadline:
                    try:self._control(peers['importer'],body,row['public'],'end',deadline,auth,alias,[])
                    except BaseException:pass
                raise
        finally:node.command_lock.release()
    def close(self):check(not self.live,'live/Unknown W obligations prevent normal close')

def _host_fixture_controller(publication,bindings,protocol):return _WriteController(publication,bindings,protocol)
