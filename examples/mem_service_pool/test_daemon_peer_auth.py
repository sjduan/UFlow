"""Real Linux peer credentials/birth/exe with owned CPU children only."""
import copy,hashlib,os,socket,subprocess,sys,tempfile,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parents[2]/'tools/phasec'))
from daemon_peer_auth import DaemonPeer,PeerRejected

class DaemonPeerTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='uf-peer-auth-');self.path=str(Path(self.tmp.name)/'socket')
        self.server=socket.socket(socket.AF_UNIX);self.server.bind(self.path);self.server.listen(4);self.server.settimeout(3);self.children=[];self.auth=[]
        self.exe=Path(sys.executable).resolve();self.pin=hashlib.sha256(self.exe.read_bytes()).hexdigest()
    def child(self):
        script="import socket,sys,time;print('ready',flush=True);sys.stdin.readline();s=socket.socket(socket.AF_UNIX);s.connect(sys.argv[1]);s.sendall(b'metadata only');sys.stdin.readline()"
        p=subprocess.Popen([str(self.exe),'-B','-c',script,self.path],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        self.children.append(p);self.assertEqual(p.stdout.readline(),b'ready\n');return p
    def capture(self,p):
        a=DaemonPeer.capture(p.pid,self.exe,self.pin);self.auth.append(a);return a
    def connect(self,p):p.stdin.write(b'go\n');p.stdin.flush();return self.server.accept()[0]
    def tearDown(self):
        for a in self.auth:a.close()
        for p in self.children:
            if p.poll() is None:
                try:p.stdin.write(b'done\ndone\n');p.stdin.flush()
                except BrokenPipeError:pass
            try:p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.terminate()
                try:p.wait(timeout=2)
                except subprocess.TimeoutExpired:p.kill();p.wait(timeout=2)
            for f in [p.stdin,p.stdout,p.stderr]:f.close()
        self.server.close();self.tmp.cleanup()
    def test_actual_direct_child_credentials_and_code_identity_pass(self):
        p=self.child();a=self.capture(p)
        with self.connect(p) as c:a.verify(c).recheck()
    def test_different_same_UID_process_cannot_supply_Catalog_control(self):
        owner=self.child();a=self.capture(owner);attacker=self.child()
        with self.connect(attacker) as c:
            with self.assertRaisesRegex(PeerRejected,'non-Catalog'):a.verify(c)
    def test_birth_replacement_and_process_death_never_become_revoke_ACK(self):
        p=self.child();a=self.capture(p)
        with self.connect(p) as c:
            before=copy.deepcopy(a._expected);a._expected['birth']+=1
            with self.assertRaisesRegex(PeerRejected,'changed'):a.verify(c)
            a._expected=before;p.stdin.write(b'done\n');p.stdin.flush();p.wait(timeout=3)
            with self.assertRaisesRegex(PeerRejected,'died'):a.verify(c)
    def test_wrong_ELF_hash_rejects_at_initial_capture(self):
        p=self.child()
        with self.assertRaisesRegex(PeerRejected,'code pin'):DaemonPeer.capture(p.pid,self.exe,'0'*64)

if __name__=='__main__':unittest.main()
