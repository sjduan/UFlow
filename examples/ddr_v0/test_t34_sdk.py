"""Normal-package import regression, run with the repository SDK on PYTHONPATH.

--tensor-compat exercises the unchanged exports with a REAL installed torch.
Without it this must run in a clean venv: no torch/torch_npu/serving or stubs.
"""
import importlib.util
import sys

import uflow

if '--tensor-compat' in sys.argv:
    import torch
    for name in uflow.__all__:
        assert getattr(uflow, name) is not None, name
    from uflow.client import UFlowClient
    from uflow.objects import DdrObject
    assert uflow.UFlowClient is UFlowClient
    assert uflow.DdrObject is DdrObject
    assert torch.tensor([1, 2, 3]).sum().item() == 6
    print('tensor compatibility: all legacy exports resolved against real torch', torch.__version__)
else:
    assert sys.prefix != sys.base_prefix, 'a clean venv is required'
    for name in ('torch', 'torch_npu', 'serving'):
        assert importlib.util.find_spec(name) is None, name
    from uflow import ExternalDdrClient, ExternalBackingHandle, ExternalReadView, ExternalDdrError
    assert uflow.ExternalDdrClient is ExternalDdrClient
    assert 'UFlowClient' in dir(uflow) and 'DdrObject' in uflow.__all__
    for name in ('torch', 'torch_npu', 'serving', 'uflow.client', 'uflow.objects', 'uflow.event_pipeline', 'uflow.acl'):
        assert name not in sys.modules, name
    try:
        uflow.no_such_export
    except AttributeError:
        pass
    else:
        raise AssertionError('unknown export accepted')
    print('light package: clean venv, standard-library external DDR exports, no tensor/runtime modules loaded')
