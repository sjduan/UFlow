"""Fixed E6aaf metadata release; fresh preflight before any new authority.

Actual final cases require the completed E0 result and ownership history, so
registration/exact old62-plus-new union runs only in a later post-close job.
"""
from qemu_profile_gate import _require_production_identity
from contextlib import contextmanager
import hashlib
from pathlib import Path
from accepted_producers import (ap_bound,ap_decode,ap_canonical,ap_fresh_modules,ap_require,accepted_anchors)

ANCHOR_SHA='a29d115c5c843e41567bd8891fcb6710b7af8b1378c1c9d61987cb7ee8040904'
ORDER=('uflow_qemu_evidence','uflow_qemu_metadata','uflow_qemu_profile','uflow_profile_metadata_v2')

def metadata_anchors(raw=None):
    if raw is None:raw=(Path(__file__).resolve().parents[2]/'examples/mem_service_pool/current_terminal_production/accepted-metadata-anchors.json').read_bytes()
    ap_require(hashlib.sha256(raw).hexdigest()==ANCHOR_SHA,'fixed external metadata anchor drift')
    return ap_decode(raw)

@contextmanager
def metadata_producer():
    a=metadata_anchors()
    for key in ('release_manifest','documentation','interface_reference','historical_baseline'):ap_bound(a[key],256<<10)
    with ap_fresh_modules(a['modules'],a['module_modes'],ORDER) as modules:
        yield a,modules['uflow_profile_metadata_v2']
    for key in ('release_manifest','documentation','interface_reference','historical_baseline'):ap_bound(a[key],256<<10)

def require_profile_metadata_ready(qemu_identity):
    _require_production_identity(qemu_identity)
    with metadata_producer() as (a,p):
        profile=p.validate_profile()
        ap_require(qemu_identity.sha256==a['qemu_binary_sha256']==profile['qemu_binary_sha256'] and
            profile['writer_source']['sha256']==a['writer_source_sha256'] and
            ap_canonical({k:v for k,v in profile.items() if k not in ('writer_source','unchanged_writer_functions')})==
            ap_canonical(accepted_anchors()['profile']['expected_actual_result']),'actual accepted metadata profile differs')
        inventory=p.historical_inventory()
        ap_require(len(inventory['files'])==62 and len(inventory['births'])==117 and
            hashlib.sha256(ap_canonical(inventory)).hexdigest()==a['historical_inventory_canonical_sha256'],
            'accepted historical62/117 inventory drift')
    return a

def complete_profile_run(run,directory,qemu_identity,*,runtime_context=None):
    require_profile_metadata_ready(qemu_identity)
    from profile_metadata_completion import complete
    return complete(run,directory,runtime_context)
