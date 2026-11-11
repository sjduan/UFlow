"""Metadata-only HOST fixtures; do not prove real carrier release or contents."""
import copy
import os
from pathlib import Path
import stat
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).parents[2]/'tools/phasec'))
from current_fm_isolation import _validate_new_rows

@unittest.skipUnless(hasattr(os,'geteuid'),'POSIX carrier ownership fixture')
class CurrentFMTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.out=Path(self.temp.name).resolve();(self.out/'fm').mkdir();self.rows=[]
        for i in range(10):
            p=self.out/'fm'/('cpu-metadata-%d'%i);p.write_bytes(b'CPU fixture metadata only')
            m=p.lstat();self.rows.append({'path':str(p),'dev':m.st_dev,'inode':m.st_ino,'uid':m.st_uid,'file_type':stat.S_IFMT(m.st_mode),'kernel_inode_aliases':[[m.st_dev,m.st_ino]]})
    def tearDown(self):self.temp.cleanup()
    def test_exact_new_ten_are_disjoint_without_payload_reads(self):self.assertEqual(len(_validate_new_rows(self.out,self.rows,{(999,999)})),10)
    def test_any_historical_alias_overlap_refused(self):
        with self.assertRaisesRegex(ValueError,'overlap'):_validate_new_rows(self.out,self.rows,{tuple(self.rows[0]['kernel_inode_aliases'][0])})
    def test_replaced_or_duplicate_carrier_refused(self):
        rows=copy.deepcopy(self.rows);rows[0]['inode']+=1
        with self.assertRaises(ValueError):_validate_new_rows(self.out,rows,set())
        with self.assertRaises(ValueError):_validate_new_rows(self.out,self.rows[:-1]+[self.rows[0]],set())
    def test_unknown_new_extra_not_admitted_as_carrier(self):
        (self.out/'fm'/'unknown-metadata-extra').write_bytes(b'fixture')
        with self.assertRaisesRegex(ValueError,'unknown extra'):_validate_new_rows(self.out,self.rows,set())

if __name__=='__main__':unittest.main()
