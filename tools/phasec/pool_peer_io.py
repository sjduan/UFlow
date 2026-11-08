"""Bounded whole-frame process IO, including partial lines and open stderr.

Blocking pipe operations live in one disposable transaction thread. Its caller
keeps the deadline; the admitted supervisor owns abnormal process termination.
No diagnostic read occurs on the failure path before the monitor notice.
"""
from collections import deque
import json
import queue
import threading
import time


class PeerIO:
    MAX_REQUEST = 65536
    MAX_REPLY = 1024 * 1024

    def __init__(self, child, notify):
        self.child, self.notify = child, notify
        self.failed = False
        self.stderr = deque(maxlen=4)
        self.workers = []
        self.stderr_worker = threading.Thread(target=self._stderr, name='pool-peer-stderr', daemon=True)
        self.stderr_worker.start()

    def _stderr(self):
        # This can wait on the child; the request/failure path never joins it.
        while True:
            chunk = self.child.stderr.read(1024)
            if not chunk:
                break
            self.stderr.append(chunk)

    def diagnostic(self):
        return b''.join(list(self.stderr)).decode('utf-8', errors='replace')[-4096:]

    def transaction(self, value, timeout=125):
        if self.failed:
            raise RuntimeError('failed peer retains original context; no new transaction')
        deadline = time.monotonic() + timeout
        result = queue.Queue(maxsize=1)

        def transfer():
            try:
                request = json.dumps(value, separators=(',', ':')).encode() + b'\n'
                if len(request) > self.MAX_REQUEST:
                    raise ValueError('peer request frame limit')
                remaining = memoryview(request)
                while remaining:
                    count = self.child.stdin.write(remaining)
                    if not count:
                        raise RuntimeError('peer stdin EOF')
                    remaining = remaining[count:]
                self.child.stdin.flush()
                line = self.child.stdout.readline(self.MAX_REPLY + 1)
                if not line:
                    raise RuntimeError('independent client EOF')
                if len(line) > self.MAX_REPLY or not line.endswith(b'\n'):
                    raise RuntimeError('peer reply incomplete or exceeds frame limit')
                result.put_nowait((True, json.loads(line)))
            except BaseException as error:
                result.put_nowait((False, error))

        worker = threading.Thread(target=transfer, name='pool-peer-transaction', daemon=True)
        self.workers.append(worker)
        worker.start()
        try:
            try:
                ok, reply = result.get(timeout=max(0, deadline-time.monotonic()))
            except queue.Empty:
                raise TimeoutError('independent client whole-frame deadline') from None
            if time.monotonic() > deadline:
                raise TimeoutError('independent client whole-frame deadline')
            if not ok:
                raise reply
            return reply
        except BaseException:
            self.failed = True
            self.notify()  # First action at the known failure boundary.
            raise

    def reaped(self, timeout=2):
        """Only after actual child wait; verify pumps exited without unbounded join."""
        if self.child.poll() is None:
            raise RuntimeError('cannot drain IO before actual child reap')
        deadline = time.monotonic() + timeout
        for worker in [*self.workers, self.stderr_worker]:
            worker.join(timeout=max(0, deadline-time.monotonic()))
            if worker.is_alive():
                raise RuntimeError('peer pipe worker still retained after reap')
        for stream in (self.child.stdin, self.child.stdout, self.child.stderr):
            stream.close()
