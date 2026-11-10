"""Actual E wrapper + C control over owned Linux Unix QMP endpoint doubles.
No QEMU, Guest, ledger or payload. Kernel SO_PEERCRED/birth checks are real.
"""
import hashlib,json,os,subprocess,sys,tempfile,time,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parents[2]/'tools/phasec'))
from daemon_peer_auth import DaemonPeer
from managed_interface_v2 import _host_wrapper_snapshot,_host_managed_serial_binding,SNAPSHOT
from managed_cache_control import _host_fixture_controller
from uflow._pool_bridge_v2 import metadata
import test_managed_cache_control as control_fixture

SERVER=r'''
import hashlib,json,os,socket,sys,types
from pathlib import Path
path,source,map_id,mode=sys.argv[1:];map_id=int(map_id)
raw=Path(source).read_bytes();assert hashlib.sha256(raw).hexdigest()=='7cfbdc76f1ab8d696ae3349b6bf5510d5b8e6b4f047f50ae90e579b72c4e62db'
c=types.ModuleType('actual_codec');exec(compile(raw,source,'exec'),c.__dict__)
server=socket.socket(socket.AF_UNIX);server.bind(path);server.listen(1);print('ready',flush=True)
s,_=server.accept();s.sendall(b'{"QMP":{}}\r\n');pending=b'';wire=None;active=False
while True:
 while b'\n' not in pending:
  part=s.recv(8192)
  if not part:sys.exit(0)
  pending+=part
 line,_,pending=pending.partition(b'\n');q=json.loads(line);name=q['execute'];args=q.get('arguments',{})
 if name=='qmp_capabilities':result={}
 elif name=='qom-set':
  assert args['path']=='/machine' and args['property']=='uflow-managed-cache-control-v1' and set(args)=={'path','property','value'}
  wire=args['value'];command=c.decode(wire);op=command['operation']
  if mode=='drop-end' and op=='end':s.close();sys.exit(0)
  if op=='begin':active=True
  if op=='end' and mode!='busy-end':active=False
  result={}
 elif name=='qom-get':
  assert args=={'path':'/machine','property':'uflow-managed-cache-control-v1'}
  command=c.decode(wire);op=command['operation'];native={k:command[k] for k in (*c.FIELDS64,*c.FIELDS32)};native.update(map_id=map_id,lease_epoch=71)
  alias={k:str(v) if k in c.FIELDS64 else v for k,v in native.items()};alias['descriptor_sha256']=command['descriptor_sha256']
  result={'version':1,'feature':c.FEATURE,'pid':os.getpid(),'operation':op,'request_sha256':c.request_sha256(wire),'control_generation':str(command['control_generation']),**{k:command[k] for k in c.HASHES},
          'code':'busy' if mode=='busy-end' and op=='end' else 'ok','active':active,'in_flight':0,'cache_pages':0,'native_alias':alias,'scope':'UFlow-Catalog-managed-only','counters':{**{k:'0' for k in c.COUNTERS},'saturated':False}}
  if mode=='wrong-alias':result['native_alias']['home_cna']+=1
  result=json.dumps(result)
 else:raise AssertionError('C sent non-allowlisted command')
 s.sendall((json.dumps({'return':result,'id':q['id']})+'\r\n').encode())
'''

@unittest.skipUnless(sys.platform=='linux','actual Linux peer credential fixture')
class WrapperConnectionTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='uf-v2-qmp-');self.wrapper=_host_wrapper_snapshot();self.notices=[];self.wrapper.notify_failure=lambda:self.notices.append('notice');self.children=[];self.qmps=[];self.peers=[]
        self.exe=Path(sys.executable).resolve();self.exe_pin=hashlib.sha256(self.exe.read_bytes()).hexdigest();self.f=control_fixture.ControlTests();self.f.setUp()
    def binding(self,role,mode='ok'):
        path=Path(self.tmp.name)/(role+'.sock');child=subprocess.Popen([str(self.exe),'-B','-c',SERVER,str(path),str(SNAPSHOT/'uflow_managed_cache_codec_v2.py'),'101' if role=='home' else '102',mode],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        self.children.append(child);self.assertEqual(child.stdout.readline(),b'ready\n');peer=DaemonPeer.capture(child.pid,self.exe,self.exe_pin);self.peers.append(peer)
        qmp=self.wrapper.QMP(path,time.monotonic()+5);self.qmps.append(qmp);qmp.bind_managed_peer(child.pid,peer._expected['birth'])
        channel=self.wrapper.ManagedSerialChannel.__new__(self.wrapper.ManagedSerialChannel);channel._managed_qmp=qmp
        return _host_managed_serial_binding(self.wrapper,channel,peer)
    def controller(self,mode='ok'):
        return _host_fixture_controller(self.f.book,{('pool','nodeB'):{'home':self.binding('home'),'importer':self.binding('importer',mode)}})
    def tearDown(self):
        for qmp in self.qmps:
            try:qmp.close()
            except RuntimeError:qmp.abort_transport() # Disposal is not a revoke ACK.
        for peer in self.peers:peer.close()
        for child in self.children:
            try:child.wait(timeout=2)
            except subprocess.TimeoutExpired:
                child.terminate()
                try:child.wait(timeout=2)
                except subprocess.TimeoutExpired:child.kill();child.wait(timeout=2)
            child.stdout.close();child.stderr.close()
        self.tmp.cleanup()
    def test_C_begin_read_end_calls_actual_wrapper_one_wire_dict_API(self):
        controller=self.controller();r=metadata(controller.execute(self.f.node,self.f.frame,self.f.auth).body)
        self.assertTrue(r['cache_end_confirmed']);self.assertFalse(controller.live);self.assertEqual([x['operation'] for x in controller.audit],['probe','probe','begin','end']);self.assertFalse(self.notices)
        for qmp in self.qmps:qmp.assert_no_managed_grants()
    def test_real_wrapper_rejects_full_alias_mismatch_before_SDK_read(self):
        controller=self.controller('wrong-alias')
        with self.assertRaises(ValueError):controller.execute(self.f.node,self.f.frame,self.f.auth)
        self.assertIn('Unknown',controller.live.values());self.assertFalse(self.f.node.seen);self.assertTrue(self.notices)
    def test_real_wrapper_EOF_at_end_is_Unknown_and_never_normal_close(self):
        controller=self.controller('drop-end')
        with self.assertRaises((ValueError,RuntimeError,OSError)):controller.execute(self.f.node,self.f.frame,self.f.auth)
        self.assertIn('Unknown',controller.live.values());self.assertEqual(len(self.f.node.seen),1)
        with self.assertRaises(RuntimeError):self.qmps[-1].assert_no_managed_grants()
    def test_real_wrapper_busy_end_never_mints_end_ACK(self):
        controller=self.controller('busy-end')
        with self.assertRaises(ValueError):controller.execute(self.f.node,self.f.frame,self.f.auth)
        self.assertIn('Unknown',controller.live.values())
        with self.assertRaises(RuntimeError):self.qmps[-1].close()
if __name__=='__main__':unittest.main()
