"""Actual ordinary-file identities in a private CPU namespace, no real FM."""
import json,os,stat,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).parents[2]/'tools/phasec'))
import verify_quarantine76_pool as q
class IndependentFiles(unittest.TestCase):
    def setUp(self):
        allowed=Path(os.environ.get('UFLOW_POOL_CPU_TMP_ROOT',tempfile.gettempdir())).resolve()
        self.tmp=tempfile.TemporaryDirectory(dir=allowed);self.root=Path(self.tmp.name).resolve();self.assertEqual(self.root.parent,allowed);self.addCleanup(self.tmp.cleanup)
        self.a={'files_requiring_fresh_exact_byte_validation':{}};self.old=[]
        for job in q.OLD_JOBS:
            out=self.root/'runs'/job;(out/'fm').mkdir(parents=True);files=[]
            for i in range(10):
                p=out/'fm'/str(i);p.write_bytes(b'CPU-ordinary-file');self.old.append(p);files.append(self.row(p))
            raw=json.dumps({'files':files}).encode();(out/'fm-carrier-active.json').write_bytes(raw)
            self.a['files_requiring_fresh_exact_byte_validation']['runs/'+job+'/fm-carrier-active.json']={'bytes':len(raw),'sha256':q.sha(raw)}
        self.out=self.root/'runs/c900';(self.out/'fm').mkdir(parents=True);self.current=[]
        for i in range(10):
            p=self.out/'fm'/str(i);p.write_bytes(b'fresh-CPU-file');self.current.append(p)
        self.assessment=self.root/'assessment.json';self.assessment.write_text(json.dumps(self.a));self.write_current()
    def row(self,p):
        m=p.lstat();return {'path':str(p),'dev':m.st_dev,'inode':m.st_ino,'uid':m.st_uid,'file_type':stat.S_IFMT(m.st_mode),'kernel_inode_aliases':[[m.st_dev,m.st_ino]]}
    def write_current(self,rows=None):(self.out/'fm-carrier-active.json').write_text(json.dumps({'files':rows if rows is not None else [self.row(p) for p in self.current]}))
    def verify(self,out=None):
        with patch.object(q,'ROOT',self.root),patch.object(q,'ASSESSMENT_SHA',q.sha(self.assessment.read_bytes())):
            return q.verify_new_fm(out or self.out,self.assessment)
    def test_thirty_old_and_ten_new_actual_inodes(self):self.assertTrue(self.verify()['independent'])
    def test_hardlink_to_old_inode_or_claimed_old_alias_rejected(self):
        rows=[self.row(p) for p in self.current];rows[0]['kernel_inode_aliases'].append([self.old[0].stat().st_dev,self.old[0].stat().st_ino]);self.write_current(rows)
        with self.assertRaises(ValueError):self.verify()
        if os.name=='nt':
            # Windows sandbox disallows CreateHardLink; exercise identity drift
            # here. The B Linux suite executes the actual hardlink branch.
            rows=[self.row(p) for p in self.current];rows[0]=self.row(self.old[0]);rows[0]['path']=str(self.current[0]);self.write_current(rows)
        else:
            self.current[0].unlink();os.link(self.old[0],self.current[0]);self.write_current()
        with self.assertRaises(ValueError):self.verify()
    def test_extra_or_missing_FM_never_ignored(self):
        (self.out/'fm/extra').write_bytes(b'extra')
        with self.assertRaises(ValueError):self.verify()
        (self.out/'fm/extra').unlink();self.write_current([self.row(p) for p in self.current[:-1]])
        with self.assertRaises(ValueError):self.verify()
    def test_old_inventory_hash_drift_and_old_namespace_reuse(self):
        with self.assertRaises(ValueError):self.verify(self.root/'runs/c033')
        p=self.root/'runs/c033/fm-carrier-active.json';p.write_bytes(p.read_bytes()+b' ')
        with self.assertRaises(ValueError):self.verify()
