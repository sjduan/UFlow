"""CPU transport failure contracts; never a Guest or release witness."""
import hashlib
import io
import json
from pathlib import Path
import queue
import sys
import threading
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[2] / 'tools/phasec'))
from mem_service_pool_bridge import GuestNode, GuestFatalPanic, NoticeOnce, fatal_kernel_panic
from pool_run_metadata import journal_diagnostics
from uflow._pool_bridge_v2 import Frame

# c064/nodeA.serial.log line6034, immutable source2c7a1534; literal console record.
PANIC = '[   13.867342] Kernel panic - not syncing: Oops: Fatal exception in interrupt'
IDENTITY = dict(node='nodeA', boot_id='cpu-boot', agent_session='cpu-session', incarnation=7)


class Serial:
    def __init__(self): self.rows = queue.Queue(); self.sent = threading.Event()
    def line(self, deadline):
        try: return self.rows.get(timeout=max(.001, deadline-time.monotonic()))
        except queue.Empty: raise TimeoutError()
    def send(self, line): self.sent.set()


class PanicTests(unittest.TestCase):
    def test_actual_fatal_record_and_nonfatal_or_quoted_text(self):
        self.assertTrue(fatal_kernel_panic(PANIC))
        self.assertTrue(fatal_kernel_panic('Kernel panic - not syncing: fatal'))
        for line in ('[ 1.0] WARNING: CPU: 0', '[ 2.0] rcu: INFO: rcu_sched detected stalls',
                     'UFPOOL_WORKER_LOG 89 847 stdout '+PANIC.encode().hex(),
                     '[ 3.0] Oops: Fatal exception in interrupt', 'worker: '+PANIC,
                     '---[ end Kernel panic - not syncing: fatal ]---', PANIC+'\ntrailing'):
            with self.subTest(line=line): self.assertFalse(fatal_kernel_panic(line))

    def test_notice_concurrent_once(self):
        calls=[]; notice=NoticeOnce(lambda: calls.append(True))
        threads=[threading.Thread(target=notice) for _ in range(24)]
        for t in threads: t.start()
        for t in threads: t.join(1); self.assertFalse(t.is_alive())
        self.assertEqual(calls,[True])

    def test_panic_wakes_waiter_before_diagnostic_io_and_preserves_first_cause(self):
        channel=Serial(); order=[]; entered=threading.Event(); release=threading.Event(); errors=[]
        def event(row):
            order.append(row['event']); entered.set(); release.wait(2)
        node=GuestNode(channel,IDENTITY,'unused',event,'simulated:cpu',notify=lambda:order.append('notice'))
        frame=Frame(17,19,'simulated:cpu','nodeA','cpu-boot','cpu-session',7,b'{"schema":2,"op":"write"}')
        def exchange():
            try: node.exchange(frame,timeout=120)
            except RuntimeError as error: errors.append(error)
        waiter=threading.Thread(target=exchange); waiter.start()
        self.assertTrue(channel.sent.wait(1)); node.thread.start()
        channel.rows.put(PANIC); self.assertTrue(entered.wait(1))
        waiter.join(1)
        try:
            self.assertFalse(waiter.is_alive(),'waiter must wake while diagnostic IO is still blocked')
            self.assertEqual(len(errors),1); self.assertIsInstance(errors[0].__cause__,GuestFatalPanic)
            self.assertFalse(node.fail(RuntimeError('later endpoint EOF'),'bridge-endpoint-failure'))
            self.assertEqual(node.first_failure['cause'],'guest-kernel-panic')
            self.assertEqual(node.first_failure['serial_line_sha256'],hashlib.sha256(PANIC.encode()).hexdigest())
            self.assertEqual(node.first_failure['pending_outcome'],'Unknown')
            self.assertTrue(node.first_failure['responsibility_retained'])
            self.assertEqual(order,['notice','guest-bridge-failure'])
        finally: release.set(); node.thread.join(1)

    def test_existing_channel_reassembles_fragmented_interleaved_records(self):
        # Exercise the actual CPU serial parser used by the matrix, including a
        # split panic prefix and a complete worker control record in one read.
        from startup_cpu_probe import Channel
        class FD:
            def fileno(self): return 987
        process=type('P',(),dict(stdout=FD(),stdin=FD()))()
        chunks=iter([b'[ 1.0] boot ok\nUFPOOL_WORKER_START 89 847\n[   13.',
                     b'867342] Kernel pan',b'ic - not syncing: Oops: Fatal exception in interrupt\n'])
        with patch('startup_cpu_probe.os.set_blocking'), patch('startup_cpu_probe.select.select',return_value=([process.stdout],[],[])), \
             patch('startup_cpu_probe.os.read',side_effect=lambda *a:next(chunks)):
            channel=Channel(process,io.BytesIO())
            self.assertFalse(fatal_kernel_panic(channel.line(time.monotonic()+1)))
            self.assertEqual(channel.line(time.monotonic()+1),'UFPOOL_WORKER_START 89 847')
            self.assertTrue(fatal_kernel_panic(channel.line(time.monotonic()+1)))

    def test_blocked_send_cannot_delay_panic_notice_or_pending_wake(self):
        entered=threading.Event();release=threading.Event();noticed=threading.Event();errors=[];events=[]
        class BlockedSerial(Serial):
            def send(self,line):
                entered.set()
                if not release.wait(2):raise TimeoutError('bounded blocked-send fixture')
        channel=BlockedSerial();node=GuestNode(channel,IDENTITY,'unused',events.append,'simulated:cpu',notify=noticed.set)
        frame=Frame(17,19,'simulated:cpu','nodeA','cpu-boot','cpu-session',7,b'{"schema":2,"op":"write"}')
        def exchange():
            try:node.exchange(frame)
            except RuntimeError as error:errors.append(error)
        caller=threading.Thread(target=exchange);caller.start()
        self.assertTrue(entered.wait(1));node.thread.start();channel.rows.put(PANIC)
        try:
            self.assertTrue(noticed.wait(.5),'panic notice must precede unblocking send')
            node.thread.join(.5);self.assertFalse(node.thread.is_alive())
            self.assertTrue(next(iter(node.pending.values()))['ready'].is_set())
            self.assertEqual(node.first_failure['cause'],'guest-kernel-panic')
            self.assertEqual(len(events),1)
        finally:release.set();caller.join(1);node.stop.set();node.thread.join(1)
        self.assertFalse(caller.is_alive());self.assertEqual(len(errors),1)
        self.assertIsInstance(errors[0].__cause__,GuestFatalPanic)
        with self.assertRaisesRegex(RuntimeError,'already failed'):node.exchange(frame)
        self.assertFalse(node.pending)


class JournalTests(unittest.TestCase):
    def diagnostic(self, values):
        def read(path, maximum):
            value=values[path.name]
            if isinstance(value,BaseException):raise value
            return value,{}
        return journal_diagnostics(Path('/cpu'),read)[0]

    def test_both_nodes_reported_and_no_registration_claim(self):
        result=self.diagnostic({'nodeA.serial.log':b'panic\n','nodeB.serial.log':FileNotFoundError()})
        self.assertEqual(result['nodes']['nodeA']['status'],'missing-serial-records')
        self.assertEqual(result['nodes']['nodeB']['status'],'missing-serial-file')
        self.assertFalse(result['all_serial_captures_complete'])
        self.assertFalse(result['E_registration_or_publication_proven'])
        self.assertFalse(result['ledger_mutation'])

    def test_truncated_invalid_and_non_ascii_are_distinct(self):
        begin=b'UFPOOL_JOURNAL_BEGIN source=worker.conf.state max_bytes=65536 file_bytes=1\n'
        cases=[(begin,'incomplete-serial-records'),(begin+b'UFPOOL_JOURNAL_CHUNK offset=2 hex=00\n','invalid-serial-records'),
               (b'\xff','non-ascii-serial'),(PermissionError('denied'),'unavailable-serial-file')]
        for data,status in cases:
            result=self.diagnostic({'nodeA.serial.log':data,'nodeB.serial.log':b''})
            self.assertEqual(result['nodes']['nodeA']['status'],status)

    def test_complete_transport_capture_is_not_publication_or_ACK(self):
        text=b'UFPOOL_JOURNAL_BEGIN source=worker.conf.state max_bytes=65536 file_bytes=0\nUFPOOL_JOURNAL_END bytes=0 fnv64=14650fb0739d0383 status=complete complete=1 errno=0\n'
        result=self.diagnostic({'nodeA.serial.log':text,'nodeB.serial.log':text})
        self.assertTrue(result['all_serial_captures_complete'])
        self.assertFalse(result['E_registration_or_publication_proven'])


if __name__=='__main__': unittest.main()
