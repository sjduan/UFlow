import hashlib,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parents[2]/'tools/phasec'))
from pool_journal_capture import capture
class JournalSerial(unittest.TestCase):
    def serial(self):
        return 'UFPOOL_JOURNAL_BEGIN source=worker.conf.state dev=1 inode=2 file_bytes=1 max_bytes=65536\nUFPOOL_JOURNAL_CHUNK offset=0 hex=41\nUFPOOL_JOURNAL_END status=complete errno=0 bytes=1 fnv64=44bd6ad473cd62a6 complete=1\n'
    def test_reassembled_bytes_hash_and_no_ACK(self):
        r,b=capture(self.serial());self.assertTrue(r['complete']);self.assertEqual(b,b'A');self.assertEqual(r['sha256'],hashlib.sha256(b'A').hexdigest());self.assertFalse(r['SDK_ACK_or_replay_claimed'])
    def test_missing_duplicate_truncated_offset_or_hash_cannot_claim_complete(self):
        s=self.serial()
        for value in ['',s.split('UFPOOL_JOURNAL_END')[0],s+s,s.replace('offset=0','offset=1'),s.replace('44bd6ad473cd62a6','0'*16),s.replace('file_bytes=1','file_bytes=2')]:
            self.assertFalse(capture(value)[0]['complete'])
