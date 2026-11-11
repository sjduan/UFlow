"""UFlow SDK exports.

External DDR is a standard-library-only consumer. Tensor/runtime integrations
are imported on first use, preserving the public names without requiring torch
merely to connect to the external-memory broker.
"""
from importlib import import_module

_EXPORTS = {
    "client": ("MANDATORY_DDR_HINT", "MANDATORY_HBM_HINT", "MANDATORY_SSD_HINT",
               "UFlowClient", "env_flag", "env_int"),
    "idl": ("DataHandle", "DataObject", "DataPlacement", "ResourceBinding", "AccessBinding",
            "TransferCost", "TransferEvent", "TransferPlan", "TransferRequest"),
    "external_ddr": ("ExternalBackingHandle", "ExternalDdrClient", "ExternalDdrError", "ExternalReadView"),
    "managed_pool": ("ManagedPoolClient", "ManagedPoolView", "PoolBlock", "PoolLease", "ManagedPoolError", "ManagedPoolUnknown"),
    "objects": ("DdrBuffer", "DdrObject", "HandleNotLocallyAddressable", "HbmObject", "ManagedBuffer",
                "SimulatedAccessView", "SimulatedObject", "SsdBuffer", "SsdObject"),
    "transfer": ("AclEventHandle", "AclStreamHandle", "TransferCompletionEventHandle"),
    "event_pipeline": ("AsyncLayerWeightPipeline", "DecodeWeightResidency", "LayerReloadState", "LayerWeightPlacement"),
}
__all__ = [name for names in _EXPORTS.values() for name in names]
_MODULES = {name: module for module, names in _EXPORTS.items() for name in names}


def __getattr__(name):
    module = _MODULES.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(f".{module}", __name__), name)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))
