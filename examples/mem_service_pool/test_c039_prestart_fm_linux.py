"""Actual typed new FM versus all four old namespaces in a private fixture."""
import json,os,unittest
from unittest.mock import patch
from pathlib import Path
import test_quarantine76_fm as base
import test_quarantine76_fm_linux as typed
import verify_c039_prestart as q
@unittest.skipUnless(os.name=='posix','actual forty-file inode/block and FIFO fixtures require POSIX')
class FourOldNamespaces(unittest.TestCase):
    row=base.IndependentFiles.row
    write_current=base.IndependentFiles.write_current
    test_actual_fifo_and_socket_with_regular_carriers=typed.TypedFiles.test_actual_fifo_and_socket_with_regular_carriers
    test_type_owner_symlink_and_alias_drift_remain_rejected=typed.TypedFiles.test_type_owner_symlink_and_alias_drift_remain_rejected
    test_extra_or_missing_FM_never_ignored=base.IndependentFiles.test_extra_or_missing_FM_never_ignored
    def setUp(self):
        base.IndependentFiles.setUp(self);out=self.root/'runs/c039';(out/'fm').mkdir(parents=True);rows=[]
        for i in range(10):
            p=out/'fm'/str(i);p.write_bytes(b'old-c039-fixture');self.old.append(p);rows.append(self.row(p))
        raw=json.dumps({'files':rows}).encode();(out/'fm-carrier-active.json').write_bytes(raw)
        self.a['files_requiring_fresh_exact_byte_validation']['runs/c039/fm-carrier-active.json']={'bytes':len(raw),'sha256':q.sha(raw)}
        self.a['FM40']=[{'path':str(p),'actual_blocks_bytes':p.stat().st_blocks*512} for p in self.old];self.assessment.write_text(json.dumps(self.a))
    def verify(self,out=None):
        with patch.object(q,'ROOT',self.root),patch.object(q,'SHA',q.sha(self.assessment.read_bytes())):
            return q.verify_new_fm(out or self.out,self.assessment)
    def test_fourth_context_inode_alias_is_never_reused(self):
        rows=[self.row(p) for p in self.current];old=self.row(self.root/'runs/c039/fm/0');rows[0]['kernel_inode_aliases'].append([old['dev'],old['inode']]);self.write_current(rows)
        with self.assertRaises(ValueError):self.verify()
if __name__=='__main__':unittest.main()
