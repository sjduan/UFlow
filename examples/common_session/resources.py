"""Cooperative task-local bounded host slots and measurement windows.

These locks do not authorize device use or clear UF obligations. Callers acquire
windows before UF leases/directory locks; native work must finish inside 120s.
"""
from contextlib import contextmanager
import json
from pathlib import Path
import time


@contextmanager
def host_job(manifest, *, timeout=60):
    import fcntl
    manifest = _manifest(manifest)
    root = Path(manifest['coordination_root'])
    deadline = time.monotonic() + min(float(timeout), 60)
    held = None
    while held is None:
        for index in range(2):
            stream = (root / f'host-job-{index}.lock').open('a+')
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                stream.close()
            else:
                held = stream
                break
        if held is None:
            if time.monotonic() >= deadline:
                raise TimeoutError('two heavy host jobs already active')
            time.sleep(.05)
    try:
        yield
    finally:
        fcntl.flock(held, fcntl.LOCK_UN)
        held.close()


def _manifest(value):
    return value if type(value) is dict else json.loads(Path(value).read_text())


@contextmanager
def measurement_window(manifest, *, timeout=60):
    import fcntl
    root = Path(_manifest(manifest)['coordination_root'])
    stream = (root / 'measurement.lock').open('a+')
    deadline = time.monotonic() + min(float(timeout), 60)
    try:
        while True:
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError('measurement window unavailable')
                time.sleep(.05)
        start = time.monotonic()
        yield {'start_monotonic': start, 'max_seconds': 120}
        if time.monotonic() - start > 120:
            raise RuntimeError('measurement window exceeded 120s; preserve contended evidence')
    finally:
        fcntl.flock(stream, fcntl.LOCK_UN)
        stream.close()
