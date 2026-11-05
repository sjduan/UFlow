"""Fresh proof of every fence binding and intercepted positive init controls."""
import ctypes
import json
import os
from pathlib import Path
import sys

from host_guard import BUFFER_SHA, binding_proof, inventory, judge, sha

mode, shim = sys.argv[1:]
before = inventory(os.getpid())
held = None
if mode in ('shim', 'positive-uf'):
    held = ctypes.CDLL(shim, mode=os.RTLD_NOW | os.RTLD_GLOBAL)
else:
    import simpler.buffer as buffer
    assert sha(buffer.__file__) == BUFFER_SHA
after = inventory(os.getpid())
proof = {'mode': mode, 'before': before, 'after': after, 'bindings': binding_proof(),
         'no_Buffer_or_Worker_created': True}
print(json.dumps(proof), flush=True)  # Preserve all data before any assertion/exit86.
judge(after, 'daemon' if held is not None else 'provider')
if mode == 'positive-uf':
    ctypes.CDLL(None).uf_acl_backend_init(None, None)
    raise AssertionError('fenced UF init returned')
if mode == 'positive-acl':
    ctypes.CDLL(None).aclInit(None)
    raise AssertionError('fenced ACL init returned')
assert mode in ('shim', 'simpler') and Path('/proc/self').exists()
