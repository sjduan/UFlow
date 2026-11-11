"""Actual stable E W wrapper/codec + real Linux owning endpoint peers.

Endpoints synthesize metadata only. This is not QEMU/native data performance.
"""
import copy,hashlib,json,sys,tempfile,subprocess,time,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parents[2]/'tools/phasec'))
from daemon_peer_auth import DaemonPeer
from managed_write_interface_v1 import _host_snapshot,_host_native_protocol,_host_serial_binding,SNAPSHOT,production_binding
from managed_write_control import _host_fixture_controller
from uflow._pool_bridge_v2 import metadata
import test_managed_write_control as fixture

SERVER=r'''
import hashlib,json,os,socket,sys,types
from pathlib import Path
path,source,map_id,mode=sys.argv[1:];map_id=int(map_id);raw=Path(source).read_bytes();assert hashlib.sha256(raw).hexdigest()=='e5627711dc76a5441a7c957733983bff10dfd7d38fe1cf787b3b535e0de05c08'
c=types.ModuleType('actual_W_codec');exec(compile(raw,source,'exec'),c.__dict__)
server=socket.socket(socket.AF_UNIX);server.bind(path);server.listen(1);print('ready',flush=True)
s,_=server.accept();s.sendall(b'{"QMP":{}}\r\n');pending=b'';wire=None;active=False
while True:
 while b'\n' not in pending:
  part=s.recv(8192)
  if not part:sys.exit(0)
  pending+=part
 line,_,pending=pending.partition(b'\n');q=json.loads(line);name=q['execute'];a=q.get('arguments',{})
 if name=='qmp_capabilities':result={}
 elif name=='qom-set':
  assert a['path']=='/machine' and a['property']=='uflow-managed-write-control-v1' and set(a)=={'path','property','value'}
  wire=a['value'];command=c.decode(wire);op=command['operation']
  if mode=='drop-flush' and op=='flush':s.close();sys.exit(0)
  if op=='begin':active=True
  if op=='end' and mode!='busy-end':active=False
  if op=='flush' and mode=='flush-is-end':active=False
  result={}
 elif name=='qom-get':
  assert a=={'path':'/machine','property':'uflow-managed-write-control-v1'};command=c.decode(wire);op=command['operation']
  native={k:command[k] for k in (*c.FIELDS64,*c.FIELDS32)};native.update(map_id=map_id,lease_epoch=71)
  alias={k:str(v) if k in c.FIELDS64 else v for k,v in native.items()};alias['descriptor_sha256']=command['descriptor_sha256']
  result={'version':1,'feature':c.FEATURE,'pid':os.getpid(),'operation':op,'request_sha256':c.request_sha256(wire),'control_generation':str(command['control_generation']),**{k:command[k] for k in c.HASHES},
          'code':'busy' if mode=='busy-end' and op=='end' else 'ok','active':active,'in_flight':0,'cache_pages':2 if op=='flush' else 0,'native_alias':alias,'scope':'UFlow-Catalog-exclusive-W-with-native-RW-verification',
          'counters':{**{k:'0' for k in c.COUNTERS},'saturated':False},'uncertain':False,'dirty_bytes':0,'write_in_flight':0,'write_counters':{k:'0' for k in ('cpu_callbacks','accepted_bytes','strict_flush_calls','completed_bytes','failures')}}
  if mode=='wrong-alias':result['native_alias']['home_cna']+=1
  if mode=='store-inflight' and op=='end':result['write_in_flight']=1
  if mode=='wrong-authority':result['authority_sha256']='00'*32
  result=json.dumps(result)
 else:raise AssertionError('non-allowlisted native metadata command')
 s.sendall((json.dumps({'return':result,'id':q['id']})+'\r\n').encode())
'''

class WriteInterfaceTests(unittest.TestCase):
    def test_actual_stable_codec_examples_and_default_production_gate(self):
        c,wrapper=_host_snapshot();rows=json.loads((SNAPSHOT/'actual-synthetic-examples.json').read_bytes())
        for row in rows:
            self.assertEqual(c.encode(c.decode(row['wire'])),row['wire']);c.validate_reply(row['wire'],row['reply'],row['reply']['pid'])
        with self.assertRaisesRegex(ValueError,'production-not-ready'):production_binding(wrapper,object(),ready=True)

@unittest.skipUnless(sys.platform=='linux','actual Linux UID/namespace/pidfd endpoint fixture')
class WriteWrapperConnectionTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='uf-W-qmp-');self.c,self.wrapper=_host_snapshot();self.notices=[];self.wrapper.notify_failure=lambda:self.notices.append('notice');self.children=[];self.qmps=[];self.peers=[]
        self.exe=Path(sys.executable).resolve();self.pin=hashlib.sha256(self.exe.read_bytes()).hexdigest();self.f=fixture.WriteControlTests();self.f.setUp()
    def binding(self,role,mode='ok'):
        path=Path(self.tmp.name)/(role+'.sock');child=subprocess.Popen([str(self.exe),'-B','-c',SERVER,str(path),str(SNAPSHOT/'uflow_managed_write_codec_v1.py'),'101' if role=='home' else '102',mode],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        self.children.append(child);self.assertEqual(child.stdout.readline(),b'ready\n');peer=DaemonPeer.capture(child.pid,self.exe,self.pin);self.peers.append(peer)
        qmp=self.wrapper.QMP(path,time.monotonic()+5);self.qmps.append(qmp);qmp.bind_managed_peer(child.pid,peer._expected['birth'])
        channel=self.wrapper.ManagedSerialChannel.__new__(self.wrapper.ManagedSerialChannel);channel._managed_qmp=qmp
        return _host_serial_binding(self.wrapper,channel,peer)
    def controller(self,mode='ok'):
        return _host_fixture_controller(self.f.f.book,{('pool','nodeB'):{'home':self.binding('home'),'importer':self.binding('importer',mode)}},_host_native_protocol(self.c))
    def tearDown(self):
        for qmp in self.qmps:
            try:qmp.close()
            except RuntimeError:qmp.abort_transport() # Transport death never impersonates native end.
        for peer in self.peers:peer.close()
        for child in self.children:
            try:child.wait(timeout=2)
            except subprocess.TimeoutExpired:
                child.terminate()
                try:child.wait(timeout=2)
                except subprocess.TimeoutExpired:child.kill();child.wait(timeout=2)
            child.stdout.close();child.stderr.close()
        self.tmp.cleanup()
    def execute(self,c):return c.execute(self.f.node,self.f.frame,self.f.auth)
    def test_actual_E_dict_API_umw1_twopeer_begin_SDK_flush_end_and_clear(self):
        controller=self.controller();r=metadata(self.execute(controller).body)
        self.assertTrue(r['write_flush_confirmed']);self.assertFalse(controller.live);self.assertEqual([x['operation'] for x in controller.audit],['probe','probe','begin','flush','end']);self.assertFalse(self.notices)
        flush=controller.audit[-2]['reply'];self.assertTrue(flush['active']);self.assertEqual(flush['cache_pages'],2);self.assertFalse(controller.audit[-1]['reply']['active'])
        for qmp in self.qmps:qmp.assert_no_managed_grants()
    def test_wrong_alias_and_authority_are_rejected_before_any_SDK_stores(self):
        for mode in ['wrong-alias','wrong-authority']:
            with self.subTest(mode=mode):
                # Distinct Unix endpoint names per case, retaining actual peers.
                self.tmp.cleanup();self.tmp=tempfile.TemporaryDirectory(prefix='uf-W-qmp-negative-')
                controller=self.controller(mode)
                with self.assertRaises(ValueError):self.execute(controller)
                self.assertIn('Unknown',controller.live.values());self.assertFalse(self.f.node.seen);self.assertTrue(self.notices)
    def test_real_wrapper_lost_flush_is_unknown_and_blocks_normal_close(self):
        controller=self.controller('drop-flush')
        with self.assertRaises((ValueError,RuntimeError,OSError)):self.execute(controller)
        self.assertIn('Unknown',controller.live.values());self.assertEqual(len(self.f.node.seen),1)
        with self.assertRaises(RuntimeError):self.qmps[-1].assert_no_managed_grants()
    def test_flush_is_not_end_and_write_IO_must_be_zero_at_end(self):
        for mode in ['flush-is-end','store-inflight','busy-end']:
            with self.subTest(mode=mode):
                self.tmp.cleanup();self.tmp=tempfile.TemporaryDirectory(prefix='uf-W-qmp-negative-')
                controller=self.controller(mode)
                with self.assertRaises((ValueError,RuntimeError)):self.execute(controller)
                self.assertIn('Unknown',controller.live.values())
                with self.assertRaises(RuntimeError):self.qmps[-1].close()
    def test_new_wrapper_blocks_R_begin_during_live_W_on_same_real_QMP_peer(self):
        controller=self.controller();original=self.f.node._exchange_locked
        def sdk(frame,timeout):
            begin=controller.audit[-1]['reply']['native_reply'];command=self.c.decode(_host_native_protocol(self.c).encode(metadata(self.f.frame.body),self.f.f.book._rows[('pool',1)]['public'],'begin',begin['native_alias']))
            command['control_generation']+=1
            with self.assertRaisesRegex(RuntimeError,'one managed'):self.qmps[-1].managed_cache(self.wrapper.codec.encode(command))
            return original(frame,timeout)
        self.f.node._exchange_locked=sdk
        with self.assertRaises(RuntimeError):self.execute(controller)
        self.assertIn('Unknown',controller.live.values());self.assertTrue(self.notices)

if __name__=='__main__':unittest.main()
