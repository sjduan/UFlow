from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import torch


REPO_ROOT = Path(__file__).resolve().parents[2]
SDK_ROOT = REPO_ROOT / "sdk" / "python"
if str(SDK_ROOT) not in sys.path:
    sys.path.insert(0, str(SDK_ROOT))

PYPTO_RUNTIME_ROOT = Path(os.environ.get("PYPTO_RUNTIME_ROOT", "/home/sj/git/pypto/runtime"))
if str(PYPTO_RUNTIME_ROOT) not in sys.path:
    sys.path.insert(0, str(PYPTO_RUNTIME_ROOT))

from uflow import UFlowClient  # noqa: E402

from simpler.task_interface import ArgDirection as D  # noqa: E402
from simpler.task_interface import CallConfig  # noqa: E402
from simpler.worker import Worker  # noqa: E402
from simpler_setup import SceneTestCase, TaskArgsBuilder, Tensor, scene_test  # noqa: E402
from simpler_setup.scene_test import _build_chip_task_args, _compare_outputs  # noqa: E402


KERNEL_ROOT = PYPTO_RUNTIME_ROOT / "tests" / "st" / "a2a3" / "host_build_graph" / "vector_example" / "kernels"


@scene_test(level=2, runtime="host_build_graph")
class _VectorExternalEventProbe(SceneTestCase):
    RTOL = 1e-5
    ATOL = 1e-5

    CALLABLE = {
        "orchestration": {
            "source": str(KERNEL_ROOT / "orchestration" / "example_orch.cpp"),
            "function_name": "build_example_graph",
            "signature": [D.IN, D.IN, D.OUT],
        },
        "incores": [
            {
                "func_id": 0,
                "source": str(KERNEL_ROOT / "aiv" / "kernel_add.cpp"),
                "core_type": "aiv",
                "signature": [D.IN, D.IN, D.OUT],
            },
            {
                "func_id": 1,
                "source": str(KERNEL_ROOT / "aiv" / "kernel_add_scalar.cpp"),
                "core_type": "aiv",
                "signature": [D.IN, D.OUT],
            },
            {
                "func_id": 2,
                "source": str(KERNEL_ROOT / "aiv" / "kernel_mul.cpp"),
                "core_type": "aiv",
                "signature": [D.IN, D.IN, D.OUT],
            },
        ],
    }

    def generate_args(self, params):
        size = int(params.get("size", 128 * 128))
        return TaskArgsBuilder(
            Tensor("a", torch.full((size,), 2.0, dtype=torch.float32)),
            Tensor("b", torch.full((size,), 3.0, dtype=torch.float32)),
            Tensor("f", torch.zeros(size, dtype=torch.float32)),
        )

    def compute_golden(self, args, params) -> None:
        args.f[:] = (args.a + args.b + 1) * (args.a + args.b + 2)


def run_probe(args: argparse.Namespace) -> None:
    os.environ.setdefault("UF_ENABLE", "1")
    client = UFlowClient.from_env(
        device_id=args.device,
        client_role="phasea08-pypto-event-probe",
        model_id="phasea08-pypto-event-probe",
    )
    wait_event = None
    done_event = None
    worker = None
    registered = False
    cid = -1
    try:
        probe = _VectorExternalEventProbe()
        callable_obj = probe.build_callable(args.platform)

        worker = Worker(level=2, device_id=args.device, platform=args.platform, runtime="host_build_graph")
        worker.init()
        cid = worker.register(callable_obj)
        registered = True

        wait_event = client.create_event_handle()
        done_event = client.create_event_handle()
        if wait_event.raw_handle == 0 or done_event.raw_handle == 0:
            raise AssertionError("UFlow ACL event raw handle must be non-zero")

        client.record_event_handle(wait_event)
        client.synchronize_event_handle(wait_event)

        test_args = probe.generate_args({"size": args.elements})
        chip_args, output_names = _build_chip_task_args(test_args, probe.CALLABLE["orchestration"]["signature"])
        golden_args = test_args.clone()
        probe.compute_golden(golden_args, {"size": args.elements})

        config = CallConfig()
        config.block_dim = args.block_dim
        config.aicpu_thread_num = args.aicpu_thread_num
        config.external_wait_event = wait_event.raw_handle
        config.external_record_event = done_event.raw_handle

        timing = worker.run(cid, chip_args, config=config)
        client.synchronize_event_handle(done_event)
        _compare_outputs(test_args, golden_args, output_names, probe.RTOL, probe.ATOL)

        host_wall_us = getattr(timing, "host_wall_us", 0) if timing is not None else 0
        device_wall_us = getattr(timing, "device_wall_us", 0) if timing is not None else 0
        print(
            "PYPTO_EXTERNAL_EVENT_SMOKE_PASS "
            f"device={args.device} platform={args.platform} elements={args.elements} "
            f"wait_event_id={wait_event.event_id} wait_raw=0x{wait_event.raw_handle:x} "
            f"record_event_id={done_event.event_id} record_raw=0x{done_event.raw_handle:x} "
            f"host_wall_us={host_wall_us} device_wall_us={device_wall_us}",
            flush=True,
        )
    finally:
        if worker is not None and registered:
            worker.unregister(cid)
        if worker is not None:
            worker.close()
        if done_event is not None:
            client.destroy_event_handle(done_event)
        if wait_event is not None:
            client.destroy_event_handle(wait_event)
        client.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--device", type=int, default=int(os.environ.get("UF_TARGET_DEVICE", "0")))
    parser.add_argument("--platform", default=os.environ.get("UF_PYPTO_PLATFORM", "a2a3"))
    parser.add_argument("--elements", type=int, default=int(os.environ.get("UF_PYPTO_EVENT_ELEMENTS", str(128 * 128))))
    parser.add_argument("--block-dim", type=int, default=int(os.environ.get("UF_PYPTO_EVENT_BLOCK_DIM", "3")))
    parser.add_argument("--aicpu-thread-num", type=int, default=int(os.environ.get("UF_PYPTO_EVENT_AICPU_THREADS", "3")))
    return parser.parse_args()


def main() -> None:
    run_probe(parse_args())


if __name__ == "__main__":
    main()
