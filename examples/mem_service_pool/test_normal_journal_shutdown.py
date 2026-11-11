"""Actual compiled PID1 HOST fixture; no Guest or SDK drain permit."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).parents[2]/'tools/phasec'))
from pool_journal_capture import capture

class NormalJournalShutdownTests(unittest.TestCase):
    def run_launcher(self,exitcode):
        binary=os.environ['UFLOW_POOL_JOURNAL_LAUNCHER']
        with tempfile.TemporaryDirectory(prefix='uf-journal-order-') as tmp:
            Path(tmp,'worker.conf.state').write_bytes(b'HOST synthetic journal metadata only\n'*32)
            p=subprocess.run([binary,'20','20','100',sys.executable,'-B','-c','raise SystemExit(%d)'%exitcode],cwd=tmp,capture_output=True,timeout=3)
        return p,p.stdout.decode('ascii')
    def test_normal_real_child_exit_precedes_journal_and_QMP_close_marker(self):
        p,text=self.run_launcher(0);self.assertEqual(p.returncode,0)
        summary,_=capture(text);self.assertTrue(summary['complete']);self.assertNotIn('UFPOOL_FAILURE_NOTICE',text)
        self.assertLess(text.index('UFPOOL_JOURNAL_BEGIN'),text.index('UFPOOL_JOURNAL_END'))
        self.assertLess(text.index('UFPOOL_JOURNAL_END'),text.index('UFPOOL_LAUNCHER_CLOSED'))
        self.assertLess(text.index('UFPOOL_LAUNCHER_CLOSED'),text.index('UFLOW_ENV_DAEMON_CLOSED'))
    def test_failed_real_child_notifies_before_diagnostics_and_never_mints_normal_marker(self):
        p,text=self.run_launcher(1);self.assertEqual(p.returncode,1)
        self.assertLess(text.index('UFPOOL_FAILURE_NOTICE'),text.index('UFPOOL_JOURNAL_BEGIN'))
        self.assertNotIn('UFLOW_ENV_DAEMON_CLOSED',text)

if __name__=='__main__':unittest.main()
