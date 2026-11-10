"""HOST-only control transport doubles; no QMP, Guest or native cache proof."""
import copy,hashlib,json,sys,threading,time,types,unittest
from unittest.mock import patch
from dataclasses import replace
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parents[2]/'tools/phasec'))
from managed_cache_control import (authority,canonical,sha,codec,strict_reply,PublicationBook,production_controller,_host_fixture_controller)
from daemon_peer_auth import AuthenticatedPeer
from uflow._pool_bridge_v2 import Frame,metadata

class Owner:
    alive=True
    def recheck(self):
        if not self.alive:raise ValueError('actual daemon died; no ACK')
class Node:
    def __init__(self,events):self.command_lock=threading.Lock();self.events=events;self.fail_read=False;self.seen=[]
    def notify(self):self.events.append('notice')
    def _exchange_locked(self,frame,timeout):
        assert self.command_lock.locked() and 0<timeout<=115
        body=metadata(frame.body);assert not any(k.startswith('cache_') for k in body);self.seen.append(frame);self.events.append('SDK-six-passes-unchanged-seam')
        if self.fail_read:raise EOFError('HOST injected SDK read EOF')
        return frame.completion({'schema':2,'phase':'completed','all_bytes':True,'visible':True,'pins':0,'offset':body['offset'],'nbytes':body['nbytes'],'pattern_id':body['pattern_id'],'checksum':'actual-guest-receipt-seam'})
class ControlTests(unittest.TestCase):
    def setUp(self):
        self.c=codec();self.events=[];self.owner=Owner();self.auth=AuthenticatedPeer(self.owner);self.node=Node(self.events);self.bad=None
        raw=bytearray(96)
        for at,size,value in [(0,4,0x4d534f42),(4,4,2),(8,4,96),(16,8,11),(24,8,0x100000),(32,8,48<<20),(40,4,21),(44,4,1),(48,8,31),(56,8,41),(64,4,7),(68,4,0),(72,4,2),(76,4,1),(80,4,51),(84,4,3),(88,4,61)]:raw[at:at+size]=value.to_bytes(size,'big')
        encoded=raw.hex();self.book=PublicationBook()
        self.book.observe_worker_publication({'node':'nodeA','incarnation':7},{'role':'worker','pid':123,'birth':456},0x7a,{'key':'pool','generation':'1','incarnation':'7','node_id':'nodeA','descriptor_hex':encoded},{'status':'ok','key':'pool','generation':'1','descriptor_hex':encoded})
        public=self.c.descriptor_metadata(encoded);bindings={}
        for role,pid,map_id in [('home',111,101),('importer',222,102)]:
            alias={**public,'map_id':map_id,'lease_epoch':71}
            def control(wire,deadline,role=role,pid=pid,alias=alias):
                command=self.c.decode(wire);op=command['operation'];self.events.append(role+'-'+op);self.assertLessEqual(deadline,time.monotonic()+5.01)
                reply={'version':1,'feature':self.c.FEATURE,'pid':pid,'operation':op,'request_sha256':self.c.request_sha256(wire),'control_generation':str(command['control_generation']),
                       **{k:command[k] for k in self.c.HASHES},'code':'ok','active':op=='begin','in_flight':0,'cache_pages':0,'native_alias':copy.deepcopy(alias),'counters':{'fills':0,'hits':0}}
                if self.bad:self.bad(op,reply)
                return json.dumps(reply)
            bindings[role]={'pid':pid,'alias':alias,'control':control,'recheck':lambda:self.events.append('actual-QEMU-profile-recheck-seam')}
        self.bindings=bindings;self.controller=_host_fixture_controller(self.book,{('pool','nodeB'):bindings})
        body={'schema':2,'op':'read','client_session':'client','offset':65536,'nbytes':20<<20,'lease_id':'lease','content_version':1,'pattern_id':17,
              'cache_version':1,'cache_pool_id':'pool','cache_pool_epoch':'epoch','cache_provider_session':'provider','cache_resource_id':118,'cache_resource_generation':1,'cache_placement_id':10004,'cache_binding_id':40004,
              'cache_backing_realm':'realm','cache_backing_provider':'mem_service_obmm','cache_backing_resource':'nodeA:7:pool','cache_backing_generation':1,'cache_access_generation':50004,'cache_lease_generation':1020,'cache_block_id':1,'cache_block_generation':1,
              'cache_mode':'r','cache_content_id':'committed-content','cache_operation_id':'operation','cache_control_generation':4294967296}
        self.frame=Frame(4294967296,4294967296,'realm','nodeB','boot-b','session-b',8,b'')
        frame={'request':self.frame.request,'correlation':self.frame.correlation,'realm':self.frame.realm,'node':self.frame.node,'boot':self.frame.boot,'session':self.frame.session,'generation':self.frame.generation}
        body['cache_frame_sha256']=sha(canonical(frame));body['cache_authority_sha256']=sha(canonical(body));self.frame=replace(self.frame,body=canonical(body))
    def execute(self):return self.controller.execute(self.node,self.frame,self.auth)
    def test_actual_orchestration_orders_begin_before_SDK_and_end_before_completion(self):
        receipt=metadata(self.execute().body);self.assertTrue(receipt['cache_end_confirmed']);self.assertFalse(self.controller.live)
        self.assertLess(self.events.index('importer-begin'),self.events.index('SDK-six-passes-unchanged-seam'));self.assertLess(self.events.index('SDK-six-passes-unchanged-seam'),self.events.index('importer-end'))
        self.assertFalse(self.node.command_lock.locked());self.assertEqual(metadata(self.node.seen[0].body)['nbytes'],20<<20)
        with self.assertRaisesRegex(ValueError,'stale'):self.execute()
    def test_forwarded_flag_and_cross_frame_digest_never_authorize(self):
        with self.assertRaisesRegex(ValueError,'SO_PEERCRED'):self.controller.execute(self.node,self.frame,{'authorized':True})
        for frame in [replace(self.frame,boot='old'),replace(self.frame,node='nodeA'),replace(self.frame,correlation=self.frame.correlation+1)]:
            with self.assertRaises(ValueError):self.controller.execute(self.node,frame,self.auth)
        self.assertEqual(self.events,[])
    def test_forged_authority_and_extra_nested_fields_reject_before_control(self):
        for key,value in [('client_session','other'),('cache_lease_generation',1021),('cache_content_id','other'),('cache_version',True)]:
            body=json.loads(self.frame.body);body[key]=value
            with self.assertRaises(ValueError):authority(replace(self.frame,body=canonical(body)))
        self.assertEqual(self.events,[])
    def test_wrong_native_alias_or_boolean_version_or_false_float_counters_reject(self):
        for key,value in [('version',True),('pid',True),('in_flight',False),('cache_pages',0.0),('native_alias',{'map_id':102})]:
            self.setUp();self.bad=lambda op,r,key=key,value=value:r.update({key:value})
            with self.assertRaises(ValueError):self.execute()
            self.assertIn('Unknown',self.controller.live.values());self.assertNotIn('SDK-six-passes-unchanged-seam',self.events)
    def test_begin_ack_drop_never_reads_and_notice_precedes_bounded_end(self):
        def bad(op,r):
            if op=='begin':raise EOFError('begin ACK lost')
        self.bad=bad
        with self.assertRaises(EOFError):self.execute()
        self.assertNotIn('SDK-six-passes-unchanged-seam',self.events);self.assertLess(self.events.index('notice'),self.events.index('importer-end'));self.assertIn('Unknown',self.controller.live.values())
    def test_end_ack_drop_and_inflight_retains_Unknown_no_completion(self):
        for field,value in [('code','busy'),('active',True),('in_flight',1),('cache_pages',1)]:
            self.setUp();self.bad=lambda op,r:r.update({field:value}) if op=='end' else None
            with self.assertRaises(ValueError):self.execute()
            self.assertIn('Unknown',self.controller.live.values());self.assertIn('notice',self.events)
    def test_read_EOF_revokes_bounded_but_never_relabels_original_Unknown(self):
        self.node.fail_read=True
        with self.assertRaises(EOFError):self.execute()
        self.assertLess(self.events.index('notice'),self.events.index('importer-end'));self.assertIn('Unknown',self.controller.live.values())
    def test_daemon_death_after_begin_never_becomes_end_ACK(self):
        def bad(op,r):
            if op=='begin':self.owner.alive=False
        self.bad=bad
        with self.assertRaisesRegex(ValueError,'died'):self.execute()
        self.assertNotIn('SDK-six-passes-unchanged-seam',self.events);self.assertIn('Unknown',self.controller.live.values())
    def test_both_peer_bindings_and_actual_publication_are_required(self):
        self.bindings['home']['alias']['home_cna']+=1
        with self.assertRaisesRegex(ValueError,'home/importer'):self.execute()
        self.assertEqual(self.events,[])
    def test_production_construction_still_rejects_before_channel_IO(self):
        with self.assertRaisesRegex(ValueError,'production-not-ready'):production_controller(self.book,self.bindings)
        self.assertEqual(self.events,[])
    def test_same_node_lock_covers_the_whole_begin_read_end_transaction(self):
        entered=threading.Event();resume=threading.Event();completed=[];original=self.node._exchange_locked
        def read(frame,timeout):
            entered.set();self.assertTrue(resume.wait(2));return original(frame,timeout)
        self.node._exchange_locked=read
        def execute():
            try:completed.append(self.execute())
            except BaseException as error:completed.append(error)
        thread=threading.Thread(target=execute);thread.start()
        try:
            self.assertTrue(entered.wait(2));self.assertFalse(self.node.command_lock.acquire(timeout=.02));self.assertNotIn('importer-end',self.events)
        finally:resume.set();thread.join(3)
        self.assertFalse(thread.is_alive());self.assertEqual(len(completed),1);self.assertIsInstance(completed[0],Frame);self.assertFalse(self.controller.live)
    def test_transcript_filters_interleaved_other_node_audit_by_full_authority(self):
        def interleave(op,r):
            if op=='begin':
                other=copy.deepcopy(r);other['authority_sha256']='00'*32;self.controller.audit.append({'operation':'begin','reply':other,'request_sha256':'00'*32})
        self.bad=interleave;receipt=metadata(self.execute().body)
        trace=[r for r in self.controller.audit if r['reply']['authority_sha256']==receipt['cache_authority_sha256']]
        self.assertEqual(receipt['cache_transcript_sha256'],sha(canonical(trace)));self.assertEqual(len(trace),4)
    def test_reply_after_five_second_metadata_deadline_is_Unknown(self):
        clock=[time.monotonic()]
        def late(op,r):clock[0]+=6
        self.bad=late
        with patch('managed_cache_control.time',types.SimpleNamespace(monotonic=lambda:clock[0])):
            with self.assertRaisesRegex(ValueError,'metadata deadline'):self.execute()
        self.assertIn('Unknown',self.controller.live.values());self.assertIn('notice',self.events)

if __name__=='__main__':unittest.main()
