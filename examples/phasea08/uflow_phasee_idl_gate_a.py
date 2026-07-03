from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import torch


REPO_ROOT = Path(__file__).resolve().parents[2]
SDK_ROOT = REPO_ROOT / "sdk" / "python"
PHASEE01_ROOT = REPO_ROOT / "examples" / "phasee01"
for path in (SDK_ROOT, PHASEE01_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from uflow import DdrBuffer, TransferEvent, TransferPlan, UFlowClient  # noqa: E402
import uflow_e01_unified_transfer_smoke as e01  # noqa: E402


def _acl_lib() -> str:
    return os.environ.get("UF_ACL_LIB", "/home/sj/git/data-service/build/lib/libuf_acl_shim.so")


def _client(*, socket_path: str, device: int, role: str, model_id: str = "phasea08-gate-a") -> UFlowClient:
    return UFlowClient(
        enabled=True,
        socket_path=socket_path,
        device_id=device,
        acl_lib_path=_acl_lib(),
        client_role=role,
        model_id=model_id,
    )


def _pattern_bytes(nbytes: int, *, offset: int = 0) -> bytes:
    return ((torch.arange(nbytes, dtype=torch.uint8) + int(offset)) % 251).numpy().tobytes()


def _write_ddr(ddr: DdrBuffer, payload: bytes, *, offset: int = 0) -> None:
    view = ddr.as_memoryview()
    try:
        view[offset : offset + len(payload)] = payload
    finally:
        del view


def _read_ddr(ddr: DdrBuffer, nbytes: int, *, offset: int = 0) -> bytes:
    view = ddr.as_memoryview()
    try:
        return bytes(view[offset : offset + nbytes])
    finally:
        del view


def _assert_plan(plan: TransferPlan, *, label: str, nbytes: int) -> None:
    if plan.plan_id <= 0:
        raise AssertionError(f"{label}: invalid plan_id={plan.plan_id}")
    if plan.nbytes != nbytes:
        raise AssertionError(f"{label}: plan nbytes={plan.nbytes}, expected={nbytes}")
    for field_name in ("path", "engine", "completion_kind", "wait_policy"):
        if not getattr(plan, field_name):
            raise AssertionError(f"{label}: missing plan.{field_name}")
    if not plan.cost.explanation:
        raise AssertionError(f"{label}: missing transfer cost explanation")
    if plan.cost.effort < 0:
        raise AssertionError(f"{label}: negative transfer effort={plan.cost.effort}")


def _assert_event(event: TransferEvent, *, label: str, nbytes: int, expect_complete: bool) -> None:
    if event.event_id <= 0:
        raise AssertionError(f"{label}: invalid event_id={event.event_id}")
    if not event.completion_kind:
        raise AssertionError(f"{label}: missing event.completion_kind")
    if expect_complete:
        if event.status != "complete":
            raise AssertionError(f"{label}: status={event.status}, expected complete: {event}")
        if event.bytes_done != nbytes:
            raise AssertionError(f"{label}: bytes_done={event.bytes_done}, expected={nbytes}")
        if not event.actual_engine or not event.actual_path:
            raise AssertionError(f"{label}: missing actual engine/path: {event}")
    elif event.status == "complete":
        raise AssertionError(f"{label}: expected non-complete event, got {event}")


def _plan_submit_wait(
    client: UFlowClient,
    *,
    src: DdrBuffer,
    dst: DdrBuffer,
    nbytes: int,
    mode: str,
    label: str,
) -> TransferEvent:
    cost = client.estimate_cost(src=src, dst=dst, nbytes=nbytes, mode=mode)
    plan = client.plan_transfer(src=src, dst=dst, nbytes=nbytes, mode=mode)
    _assert_plan(plan, label=label, nbytes=nbytes)
    if abs(cost.effort - plan.cost.effort) > 1e-6:
        raise AssertionError(f"{label}: EstimateCost/PlanTransfer effort mismatch")
    event = client.submit_transfer(plan)
    if client._transfer_handles:  # noqa: SLF001 - Gate A intentionally checks legacy handle bypass.
        raise AssertionError(f"{label}: daemon TransferEvent path unexpectedly created legacy TransferHandle")
    polled = client.poll_event(event)
    if polled.status not in {"running", "complete", "failed", "cancelled"}:
        raise AssertionError(f"{label}: unexpected PollEvent status={polled.status}")
    final = client.wait_event(event)
    _assert_event(final, label=label, nbytes=nbytes, expect_complete=True)
    print(
        f"UFLOW_A08_GATE_A_TRANSFER_PASS label={label} "
        f"plan_id={plan.plan_id} event_id={final.event_id} path={plan.path} "
        f"engine={final.actual_engine} completion={final.completion_kind} "
        f"latency_us={final.actual_latency_us:.3f} bandwidth_gib_s={final.actual_bandwidth_gib_s:.3f} "
        f"fallback_used={str(final.fallback_used).lower()} fallback_reason={final.fallback_reason}",
        flush=True,
    )
    return final


def ddr_mark_dirty_ready_probe(args: argparse.Namespace) -> None:
    client = _client(socket_path=args.socket, device=args.src_device, role="phasea08-gatea-dirty-ready")
    src = None
    dst = None
    try:
        payload = _pattern_bytes(args.bytes, offset=41)
        src = client.allocate_ddr(
            name="phasea08.gatea.dirty_ready.src",
            role="user",
            nbytes=args.bytes,
            target=args.ddr_target,
            mark_ready=False,
        )
        dst = client.allocate_ddr(
            name="phasea08.gatea.dirty_ready.dst",
            role="user",
            nbytes=args.bytes,
            target=args.ddr_dst_target,
            mark_ready=False,
        )
        _write_ddr(src, payload)
        client.mark_dirty(src)
        obj, placement = client.describe_object(src.object_id)
        if obj.state != "Modified":
            raise AssertionError(f"MarkDirty did not set state=Modified, got {obj.state}")
        if placement.medium != "ddr":
            raise AssertionError(f"expected DDR placement, got {placement}")
        _plan_submit_wait(
            client,
            src=src,
            dst=dst,
            nbytes=args.bytes,
            mode=args.transfer_mode,
            label="ddr_mark_dirty_to_ddr",
        )
        if _read_ddr(dst, args.bytes) != payload:
            raise AssertionError("MarkDirty DDR->DDR payload mismatch")
        client.mark_ready(src)
        obj, _ = client.describe_object(src.object_id)
        if obj.state != "Ready":
            raise AssertionError(f"MarkReady did not set state=Ready, got {obj.state}")
        print(
            "UFLOW_A08_GATE_A_MARK_DIRTY_READY_PASS "
            f"object_id={src.object_id} src_placement={src.placement_id} dst_object_id={dst.object_id}",
            flush=True,
        )
    finally:
        if dst is not None:
            dst.release()
        if src is not None:
            src.release()
        client.close()


def not_ready_negative_probe(args: argparse.Namespace) -> None:
    client = _client(socket_path=args.socket, device=args.src_device, role="phasea08-gatea-not-ready")
    src = None
    dst = None
    try:
        src = client.allocate_ddr(
            name="phasea08.gatea.not_ready.src",
            role="user",
            nbytes=args.bytes,
            target=args.ddr_target,
            mark_ready=False,
        )
        dst = client.allocate_ddr(
            name="phasea08.gatea.not_ready.dst",
            role="user",
            nbytes=args.bytes,
            target=args.ddr_dst_target,
            mark_ready=False,
        )
        plan = client.plan_transfer(src=src, dst=dst, nbytes=args.bytes, mode=args.transfer_mode)
        _assert_plan(plan, label="not_ready_negative", nbytes=args.bytes)
        event = client.submit_transfer(plan)
        final = client.wait_event(event)
        if final.status != "failed":
            raise AssertionError(f"not-ready source should fail, got {final}")
        if "not ready" not in final.error_message:
            raise AssertionError(f"not-ready failure did not explain readiness: {final.error_message}")
        print(
            "UFLOW_A08_GATE_A_NOT_READY_NEGATIVE_PASS "
            f"event_id={final.event_id} status={final.status} error={final.error_message!r}",
            flush=True,
        )
    finally:
        if dst is not None:
            dst.release()
        if src is not None:
            src.release()
        client.close()


def cancel_event_probe(args: argparse.Namespace) -> None:
    client = _client(socket_path=args.socket, device=args.src_device, role="phasea08-gatea-cancel")
    ddr = None
    try:
        payload = _pattern_bytes(args.bytes, offset=53)
        ddr = client.allocate_ddr(
            name="phasea08.gatea.cancel.direct_ref",
            role="user",
            nbytes=args.bytes,
            target=args.ddr_target,
            mark_ready=False,
        )
        _write_ddr(ddr, payload)
        client.mark_ready(ddr)
        plan = client.plan_transfer(src=ddr, dst=ddr, nbytes=args.bytes, mode=args.transfer_mode)
        _assert_plan(plan, label="cancel_direct_ref", nbytes=args.bytes)
        if plan.path != "direct_ref":
            raise AssertionError(f"cancel probe expected direct_ref plan, got path={plan.path}")
        event = client.submit_transfer(plan)
        cancelled = client.cancel_event(event)
        _assert_event(cancelled, label="cancel_direct_ref", nbytes=args.bytes, expect_complete=True)
        print(
            "UFLOW_A08_GATE_A_CANCEL_EVENT_API_PASS "
            f"event_id={cancelled.event_id} status={cancelled.status} path={plan.path}",
            flush=True,
        )
    finally:
        if ddr is not None:
            ddr.release()
        client.close()


def run_matrix(args: argparse.Namespace) -> None:
    if args.require_cross_device and args.dst_socket == "" and args.src_device == args.dst_device:
        raise AssertionError("--require-cross-device requires a different --dst-device or --dst-socket")
    e01.run_all(args)
    print(
        "UFLOW_A08_GATE_A_PHASEE_MATRIX_PASS "
        f"src_device={args.src_device} dst_device={args.dst_device} bytes={args.bytes}",
        flush=True,
    )


def run_extras(args: argparse.Namespace) -> None:
    ddr_mark_dirty_ready_probe(args)
    not_ready_negative_probe(args)
    cancel_event_probe(args)
    print(
        "UFLOW_A08_GATE_A_IDL_EXTRAS_PASS "
        f"src_device={args.src_device} bytes={args.bytes}",
        flush=True,
    )


def run_all(args: argparse.Namespace) -> None:
    run_matrix(args)
    run_extras(args)
    print(
        "UFLOW_A08_GATE_A_PASS "
        f"src_device={args.src_device} dst_device={args.dst_device} bytes={args.bytes}",
        flush=True,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PhaseA-08 Gate A smoke for PhaseE UFlow IDL transfer APIs.")
    sub = parser.add_subparsers(dest="mode", required=True)
    for name in ("all", "matrix", "extras", "mark-dirty-ready", "not-ready-negative", "cancel-event"):
        p = sub.add_parser(name)
        p.add_argument("--socket", default=os.environ.get("UF_SOCKET", "/tmp/uflow_e01_src.sock"))
        p.add_argument("--dst-socket", default=os.environ.get("UF_DST_SOCKET", ""))
        p.add_argument("--src-device", type=int, default=int(os.environ.get("UF_SRC_DEVICE", os.environ.get("UF_TARGET_DEVICE", "0"))))
        p.add_argument("--dst-device", type=int, default=int(os.environ.get("UF_DST_DEVICE", os.environ.get("UF_SRC_DEVICE", "0"))))
        p.add_argument("--bytes", type=int, default=int(os.environ.get("UF_TEST_BYTES", str(1 << 20))))
        p.add_argument("--ddr-target", default=os.environ.get("UF_DDR_TARGET", "host:0"))
        p.add_argument("--ddr-dst-target", default=os.environ.get("UF_DDR_DST_TARGET", os.environ.get("UF_DDR_TARGET", "host:0")))
        p.add_argument("--transfer-mode", default=os.environ.get("UF_E01_TRANSFER_MODE", "auto"))
        p.add_argument("--overlay-offset", type=int, default=1024)
        p.add_argument("--overlay-bytes", type=int, default=4096)
        p.add_argument("--object-id", type=int, default=0)
        p.add_argument("--require-cross-device", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.mode == "all":
        run_all(args)
    elif args.mode == "matrix":
        run_matrix(args)
    elif args.mode == "extras":
        run_extras(args)
    elif args.mode == "mark-dirty-ready":
        ddr_mark_dirty_ready_probe(args)
    elif args.mode == "not-ready-negative":
        not_ready_negative_probe(args)
    elif args.mode == "cancel-event":
        cancel_event_probe(args)
    else:
        raise ValueError(args.mode)


if __name__ == "__main__":
    main()
