"""Real POSIX carrier types used by frozen E0; never open FIFO/ring payload."""
import os,socket,stat,unittest
from unittest.mock import patch
import test_quarantine76_fm as fixtures

@unittest.skipUnless(os.name=='posix','actual FIFO/socket identity fixtures require POSIX')
class TypedFiles(unittest.TestCase):
    setUp=fixtures.IndependentFiles.setUp
    row=fixtures.IndependentFiles.row
    write_current=fixtures.IndependentFiles.write_current
    verify=fixtures.IndependentFiles.verify
    def test_actual_fifo_and_socket_with_regular_carriers(self):
        fifo=self.current[0];fifo.unlink();os.mkfifo(fifo,0o600)
        endpoint=self.current[1];endpoint.unlink()
        sock=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);self.addCleanup(sock.close)
        # Linux directory FD keeps the socket address below sockaddr_un's limit
        # even when the task-specific temporary root has a long absolute path.
        fd=os.open(endpoint.parent,os.O_DIRECTORY)
        try:sock.bind('/proc/self/fd/'+str(fd)+'/'+endpoint.name)
        finally:os.close(fd)
        self.write_current();self.assertTrue(self.verify()['independent'])
        rows=[self.row(p) for p in self.current];rows[0]['file_type']=stat.S_IFREG;self.write_current(rows)
        with self.assertRaises(ValueError):self.verify()

    def test_type_owner_symlink_and_alias_drift_remain_rejected(self):
        rows=[self.row(p) for p in self.current];rows[0]['uid']=os.geteuid()+1;self.write_current(rows)
        with self.assertRaises(ValueError):self.verify()
        self.write_current()
        with patch('verify_quarantine76_pool.os.geteuid',return_value=os.geteuid()+1):
            with self.assertRaises(ValueError):self.verify()
        self.current[0].unlink();self.current[0].symlink_to(self.current[2]);self.write_current()
        with self.assertRaises(ValueError):self.verify()

if __name__=='__main__':unittest.main()
