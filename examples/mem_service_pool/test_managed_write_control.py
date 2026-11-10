"""HOST orchestration with explicitly normalized doubles, not an E wire claim."""
import copy,json,sys,threading,time,types,unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).parents[2]/'tools/phasec'))
from daemon_peer_auth import AuthenticatedPeer
from managed_cache_control import canonical,sha
from managed_write_control import authority,production_controller,_host_fixture_controller
from uflow._pool_bridge_v2 import metadata
import test_managed_cache_control as read_fixture

class HostProtocol:
    """Internal normalized metadata double; no invented external E W ABI."""
    VERSION=1
    def __init__(self,c):self.FIELDS64,self.FIELDS32=c.FIELDS64,c.FIELDS32
    def encode(self,body,public,op,alias):
        return canonical({'HOST_ONLY_NOT_E_NATIVE_WIRE':True,'op':op,'generation':body['write_control_generation'],'authority':body['write_authority_sha256'],'correlation':body['write_frame_sha256']}).decode()
    def validate_reply(self,wire,value,pid):return value

class WriteControlTests(unittest.TestCase):
    def setUp(self):
        f=read_fixture.ControlTests();f.setUp();self.f=f;self.node=f.node;self.auth=f.auth;self.events=f.events;self.bad=None
        old=metadata(f.frame.body);b={k.replace('cache_','write_') if k.startswith('cache_') else k:v for k,v in old.items()}
        b.update(op='write',content_version=0,write_mode='w',write_rights='rw');b.pop('write_content_id');b.pop('write_authority_sha256');b['write_authority_sha256']=sha(canonical(b));self.frame=replace(f.frame,body=canonical(b));self.protocol=HostProtocol(f.c)
        for role,binding in f.bindings.items():
            def control(wire,deadline,role=role,binding=binding):
                self.assertTrue(self.node.command_lock.locked());q=json.loads(wire);op=q['op'];self.events.append(role+'-W-'+op)
                r={'version':1,'mode':'w','operation':op,'pid':binding['pid'],'control_generation':str(q['generation']),'authority_sha256':q['authority'],'correlation_sha256':q['correlation'],'request_sha256':sha(wire.encode()),'code':'ok','active':op=='begin','in_flight':0,'cache_pages':0,'dirty_bytes':0,'flush_confirmed':op=='end','native_alias':copy.deepcopy(binding['alias'])}
                if self.bad:self.bad(op,r)
                return r
            binding['control']=control
        self.controller=_host_fixture_controller(f.book,{('pool','nodeB'):f.bindings},self.protocol)
    def execute(self):return self.controller.execute(self.node,self.frame,self.auth)
    def test_W_controls_cover_unchanged_stores_and_verification_before_exact_flush_end(self):
        receipt=metadata(self.execute().body);self.assertTrue(receipt['write_flush_confirmed']);self.assertNotIn('cache_version',receipt);self.assertFalse(self.controller.live)
        self.assertLess(self.events.index('importer-W-begin'),self.events.index('SDK-six-passes-unchanged-seam'));self.assertLess(self.events.index('SDK-six-passes-unchanged-seam'),self.events.index('importer-W-end'))
        forwarded=metadata(self.node.seen[0].body);self.assertEqual(forwarded['op'],'write');self.assertEqual(forwarded['content_version'],0);self.assertFalse(any(k.startswith('write_') for k in forwarded))
        with self.assertRaisesRegex(ValueError,'stale'):self.execute()
    def test_read_authority_and_forged_frame_cannot_select_W_mode(self):
        for frame in [self.f.frame,replace(self.frame,boot='old'),replace(self.frame,correlation=9)]:
            with self.assertRaises(ValueError):self.controller.execute(self.node,frame,self.auth)
        for k,v in [('write_rights','ro'),('content_version',1),('write_version',True),('nbytes',0)]:
            b=metadata(self.frame.body);b[k]=v;b.pop('write_authority_sha256');b['write_authority_sha256']=sha(canonical(b))
            with self.assertRaises(ValueError):authority(replace(self.frame,body=canonical(b)))
        self.assertFalse(self.events)
    def test_full_native_alias_epoch_and_peer_are_checked_around_control(self):
        for key,value in [('home_cna',999),('map_id','0'),('lease_epoch','0'),('offset','65537')]:
            self.setUp();self.f.bindings['home']['alias'][key]=value
            with self.assertRaises(ValueError):self.execute()
            self.assertNotIn('SDK-six-passes-unchanged-seam',self.events);self.assertIn('Unknown',self.controller.live.values())
        with self.assertRaisesRegex(ValueError,'SO_PEERCRED'):self.controller.execute(self.node,self.frame,{'approved':True})
    def test_partial_dirty_failed_or_wrong_typed_end_retains_sticky_obligation(self):
        for k,v in [('dirty_bytes',1),('dirty_bytes',False),('cache_pages',1),('in_flight',1),('flush_confirmed',False),('active',True),('version',True),('code','busy')]:
            self.setUp();self.bad=lambda op,r,k=k,v=v:r.update({k:v}) if op=='end' else None
            with self.assertRaises(ValueError):self.execute()
            self.assertIn('Unknown',self.controller.live.values());self.assertIn('notice',self.events)
            with self.assertRaises(ValueError):self.controller.close()
            with self.assertRaisesRegex(ValueError,'stale'):self.execute()
    def test_native_epoch_changes_during_execution_cannot_end_normally(self):
        original=self.node._exchange_locked
        def execute(frame,timeout):
            r=original(frame,timeout);self.f.bindings['importer']['alias']['lease_epoch']='72';return r
        self.node._exchange_locked=execute
        with self.assertRaisesRegex(ValueError,'alias/epoch'):self.execute()
        self.assertIn('Unknown',self.controller.live.values())
    def test_native_begin_or_sdk_failure_notifies_before_best_effort_flush(self):
        def bad(op,r):
            if op=='begin':raise EOFError('HOST begin ACK lost')
        self.bad=bad
        with self.assertRaises(EOFError):self.execute()
        self.assertLess(self.events.index('notice'),self.events.index('importer-W-end'));self.assertIn('Unknown',self.controller.live.values())
        self.setUp();self.node.fail_read=True
        with self.assertRaises(EOFError):self.execute()
        self.assertLess(self.events.index('notice'),self.events.index('importer-W-end'));self.assertIn('Unknown',self.controller.live.values())
    def test_response_after_metadata_deadline_and_owner_death_do_not_release(self):
        clock=[time.monotonic()];self.bad=lambda op,r:clock.__setitem__(0,clock[0]+6)
        with patch('managed_write_control.time',types.SimpleNamespace(monotonic=lambda:clock[0])):
            with self.assertRaisesRegex(ValueError,'late'):self.execute()
        self.assertIn('Unknown',self.controller.live.values())
        self.setUp()
        def dead(op,r):
            if op=='begin':self.f.owner.alive=False
        self.bad=dead
        with self.assertRaisesRegex(ValueError,'died'):self.execute()
        self.assertNotIn('SDK-six-passes-unchanged-seam',self.events);self.assertIn('Unknown',self.controller.live.values())
    def test_same_node_lock_spans_W_and_cannot_overlap_other_commands(self):
        entered=threading.Event();resume=threading.Event();results=[];original=self.node._exchange_locked
        def execute(frame,timeout):entered.set();self.assertTrue(resume.wait(2));return original(frame,timeout)
        self.node._exchange_locked=execute
        def run():
            try:results.append(self.execute())
            except BaseException as e:results.append(e)
        t=threading.Thread(target=run);t.start()
        try:self.assertTrue(entered.wait(2));self.assertFalse(self.node.command_lock.acquire(timeout=.02));self.assertNotIn('importer-W-end',self.events)
        finally:resume.set();t.join(3)
        self.assertFalse(t.is_alive());self.assertEqual(len(results),1);self.assertFalse(self.controller.live)
    def test_production_old_R_context_or_caller_readiness_never_opens_W_controls(self):
        with self.assertRaisesRegex(ValueError,'production-not-ready'):production_controller(self.f.book,self.f.bindings,self.protocol,ready=True)
        self.assertFalse(self.events)

if __name__=='__main__':unittest.main()
