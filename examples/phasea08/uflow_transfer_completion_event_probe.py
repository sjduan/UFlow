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
if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

PYPTO_RUNTIME_ROOT = Path(os.environ.get("PYPTO_RUNTIME_ROOT", "/home/sj/git/pypto/runtime"))
if str(PYPTO_RUNTIME_ROOT) not in sys.path:
    sys.path.insert(0, str(PYPTO_RUNTIME_ROOT))

from uflow import UFlowClient  # noqa: E402
from pypto_external_event_smoke import _VectorExternalEventProbe  # noqa: E402
from pypto_external_event_smoke import _build_chip_task_args, _compare_outputs  # noqa: E402
from simpler.task_interface import CallConfig  # noqa: E402
from simpler.worker import Worker  # noqa: E402


def _pattern_tensor(nbytes: int, *, offset: int = 0) -> torch.Tensor:
    return (torch.arange(nbytes, dtype=torch.uint8) + int(offset)) % 251


def _write_ddr(ddr, payload: bytes) -> None:
    view = ddr.as_memoryview()
    try:
        view[: len(payload)] = payload
    finally:
        del view


def run_probe(args: argparse.Namespace) -> None:
    os.environ.setdefault("UF_ENABLE", "1")
    client = UFlowClient.from_env(
        device_id=args.device,
        client_role="phasea08-transfer-completion-event",
        model_id="phasea08-transfer-completion-event",
    )
    src_ddr = None
    dst_hbm = None
    completion_event = None
    compute_done_event = None
    worker = None
    registered = False
    cid = -1
    try:
        payload = _pattern_tensor(args.transfer_bytes, offset=67)
        src_ddr = client.allocate_ddr(
            name="phasea08.gatec.reload.src_ddr",
            role="weight",
            nbytes=args.transfer_bytes,
            target=args.ddr_target,
            immutable=True,
            mark_ready=False,
        )
        _write_ddr(src_ddr, payload.numpy().tobytes())
        client.mark_ready(src_ddr)
        dst_hbm = client.allocate(
            name="phasea08.gatec.reload.dst_hbm",
            role="weight",
            nbytes=args.transfer_bytes,
            shape=(args.transfer_bytes,),
            dtype=torch.uint8,
            immutable=True,
            mark_ready=False,
        )

        plan = client.plan_transfer(
            src=src_ddr,
            dst=dst_hbm,
            nbytes=args.transfer_bytes,
            mode=args.transfer_mode,
            wait_policy="return_immediately",
        )
        completion_event = client.submit_transfer_hotpath_event(
            src=src_ddr,
            dst=dst_hbm,
            nbytes=args.transfer_bytes,
            mode=args.transfer_mode,
            completion_export=os.environ.get("UF_A08_COMPLETION_EXPORT", "auto"),
            timeout_ms=args.transfer_timeout_ms,
            plan=plan,
        )
        transfer_event = client.poll_event(completion_event.transfer_event_id)
        if args.transfer_mode in {"auto", "direct_async"} and transfer_event.actual_engine != "acl_direct_async_thp":
            raise AssertionError(
                f"expected direct engine acl_direct_async_thp for mode={args.transfer_mode}, "
                f"got {transfer_event.actual_engine}"
            )

        probe = _VectorExternalEventProbe()
        callable_obj = probe.build_callable(args.platform)
        worker = Worker(level=2, device_id=args.device, platform=args.platform, runtime="host_build_graph")
        worker.init()
        cid = worker.register(callable_obj)
        registered = True

        compute_done_event = client.create_event_handle()
        test_args = probe.generate_args({"size": args.elements})
        chip_args, output_names = _build_chip_task_args(test_args, probe.CALLABLE["orchestration"]["signature"])
        golden_args = test_args.clone()
        probe.compute_golden(golden_args, {"size": args.elements})

        config = CallConfig()
        config.block_dim = args.block_dim
        config.aicpu_thread_num = args.aicpu_thread_num
        config.external_wait_event = completion_event.raw_handle
        config.external_record_event = compute_done_event.raw_handle

        timing = worker.run(cid, chip_args, config=config)
        client.synchronize_event_handle(compute_done_event)
        if completion_event.export_kind not in {"host_proxy_blocking", "daemon_transfer_event_proxy_blocking"}:
            client.wait_completion_event_proxy(completion_event, timeout_s=args.transfer_timeout_ms / 1000.0)
        _compare_outputs(test_args, golden_args, output_names, probe.RTOL, probe.ATOL)

        out = torch.empty((args.transfer_bytes,), dtype=torch.uint8)
        client.copy_from_device(dst_hbm, out)
        if not torch.equal(payload, out):
            raise AssertionError("DDR->HBM reload payload mismatch after completion event probe")

        host_wall_us = getattr(timing, "host_wall_us", 0) if timing is not None else 0
        device_wall_us = getattr(timing, "device_wall_us", 0) if timing is not None else 0
        print(
            "UFLOW_A08_GATE_C_TRANSFER_COMPLETION_EVENT_PROXY_PASS "
            f"device={args.device} platform={args.platform} "
            "bridge=daemon_hotpath "
            f"transfer_event_id={transfer_event.event_id} "
            f"completion_event_id={completion_event.event_id} "
            f"completion_raw=0x{completion_event.raw_handle:x} export_kind={completion_event.export_kind} "
            f"source_completion={completion_event.source_completion_kind} compute_done_raw=0x{compute_done_event.raw_handle:x} "
            f"path={plan.path} engine={plan.engine} actual_engine={transfer_event.actual_engine} "
            f"actual_path={transfer_event.actual_path} "
            f"transfer_bytes={args.transfer_bytes} "
            f"transfer_latency_us={transfer_event.actual_latency_us:.3f} "
            f"transfer_bandwidth_gib_s={transfer_event.actual_bandwidth_gib_s:.3f} "
            f"host_wall_us={host_wall_us} device_wall_us={device_wall_us}",
            flush=True,
        )
    finally:
        if worker is not None and registered:
            worker.unregister(cid)
        if worker is not None:
            worker.close()
        if compute_done_event is not None:
            client.destroy_event_handle(compute_done_event)
        if completion_event is not None:
            client.destroy_event_handle(completion_event)
        if dst_hbm is not None:
            dst_hbm.release()
        if src_ddr is not None:
            src_ddr.release()
        client.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe UFlow TransferEvent -> PyPTO external wait event bridge.")
    parser.add_argument("--device", type=int, default=int(os.environ.get("UF_TARGET_DEVICE", "0")))
    parser.add_argument("--platform", default=os.environ.get("UF_PYPTO_PLATFORM", "a2a3"))
    parser.add_argument("--elements", type=int, default=int(os.environ.get("UF_PYPTO_EVENT_ELEMENTS", str(128 * 128))))
    parser.add_argument("--transfer-bytes", type=int, default=int(os.environ.get("UF_A08_GATEC_TRANSFER_BYTES", str(1 << 20))))
    parser.add_argument("--transfer-mode", default=os.environ.get("UF_A08_GATEC_TRANSFER_MODE", "auto"))
    parser.add_argument("--transfer-timeout-ms", type=int, default=int(os.environ.get("UF_A08_GATEC_TIMEOUT_MS", "120000")))
    parser.add_argument("--ddr-target", default=os.environ.get("UF_DDR_TARGET", "host:0"))
    parser.add_argument("--block-dim", type=int, default=int(os.environ.get("UF_PYPTO_EVENT_BLOCK_DIM", "3")))
    parser.add_argument("--aicpu-thread-num", type=int, default=int(os.environ.get("UF_PYPTO_EVENT_AICPU_THREADS", "3")))
    return parser.parse_args()


def main() -> None:
    run_probe(parse_args())


if __name__ == "__main__":
    main()
