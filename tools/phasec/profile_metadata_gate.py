"""New-QEMU normal registration/exact-old62-plus-new union producer seam.

The accepted old v1 API is deliberately not called with a new profile. E owns
the separate profile-aware release; bind its actual manager-accepted raw pins
before the original-ledger/runtime technical review can authorize execution.
"""
from qemu_profile_gate import _require_production_identity

_ACCEPTED_METADATA=None

def complete_profile_run(run,directory,qemu_identity):
    _require_production_identity(qemu_identity)
    if _ACCEPTED_METADATA is None:
        raise ValueError('profile-aware metadata not-ready: actual accepted E registration and historical62-plus-new exact-union API/pins required; old56fa/writer/registry never substituted')
    raise ValueError('profile-aware metadata release binding must implement the accepted actual E API before execution')
