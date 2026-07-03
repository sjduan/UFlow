from __future__ import annotations

import argparse
import ctypes
import os
import subprocess
import sys
from pathlib import Path

import torch


REPO_ROOT = Path(__file__).resolve().parents[2]
SDK_ROOT = REPO_ROOT / "sdk" / "python"
if str(SDK_ROOT) not in sys.path:
    sys.path.insert(0, str(SDK_ROOT))

from uflow import DdrBuffer, ManagedBuffer, TransferEvent, TransferPlan, UFlowClient  # noqa: E402


def _acl_lib() -> str:
    return os.environ.get("UF_ACL_LIB", "/home/sj/git/data-service/build/lib/libuf_acl_shim.so")


def _client(*, socket_path: str, device: int, role: str, model_id: str = "phasee01") -> UFlowClient:
    return UFlowClient(
        enabled=True,
        socket_path=socket_path,
        device_id=device,
        acl_lib_path=_acl_lib(),
        client_role=role,
        model_id=model_id,
    )


def _pattern_tensor(nbytes: int, *, offset: int = 0) -> torch.Tensor:
    return (torch.arange(nbytes, dtype=torch.uint8) + int(offset)) % 251


def _pattern_bytes(nbytes: int, *, offset: int = 0) -> bytes:
    return _pattern_tensor(nbytes, offset=offset).numpy().tobytes()


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


def _copy_ddr_bytes(src: DdrBuffer, dst: DdrBuffer, nbytes: int) -> None:
    src_view = src.as_memoryview()
    dst_view = dst.as_memoryview()
    try:
        dst_view[:nbytes] = src_view[:nbytes]
    finally:
        del dst_view
        del src_view


def _print_plan(prefix: str, plan: TransferPlan, event: TransferEvent) -> None:
    print(
        f"{prefix}_PLAN "
        f"plan_id={plan.plan_id} event_id={event.event_id} path={plan.path} "
        f"engine={plan.engine} completion={plan.completion_kind} status={event.status} "
        f"effort={plan.cost.effort:.3f} latency_us={event.actual_latency_us:.3f} "
        f"bandwidth_gib_s={event.actual_bandwidth_gib_s:.3f} "
        f"fallback_used={event.fallback_used} fallback_reason={event.fallback_reason}",
        flush=True,
    )


def _execute(client: UFlowClient, src: ManagedBuffer | DdrBuffer, dst: ManagedBuffer | DdrBuffer, *, nbytes: int, mode: str, prefix: str) -> TransferEvent:
    cost = client.estimate_cost(src=src, dst=dst, nbytes=nbytes, mode=mode)
    plan = client.plan_transfer(src=src, dst=dst, nbytes=nbytes, mode=mode)
    if abs(plan.cost.effort - cost.effort) > 1e-6:
        raise AssertionError(f"estimate/plan effort mismatch: {cost.effort} vs {plan.cost.effort}")
    event = client.submit_transfer(plan)
    event = client.wait_event(event)
    if event.status != "complete":
        raise AssertionError(f"transfer {prefix} did not complete: {event}")
    if event.bytes_done != nbytes:
        raise AssertionError(f"transfer {prefix} bytes_done={event.bytes_done}, expected={nbytes}")
    _print_plan(prefix, plan, event)
    return event


def hbm_object(args: argparse.Namespace) -> None:
    client = _client(socket_path=args.socket, device=args.src_device, role="phasee01-hbm-object")
    hbm = None
    try:
        original = _pattern_tensor(args.bytes)
        hbm = client.allocate(
            name="phasee01.hbm.object",
            role="user",
            nbytes=args.bytes,
            shape=(args.bytes,),
            dtype=torch.uint8,
            mark_ready=False,
        )
        client.copy_to_device(hbm, original)
        client.mark_ready(hbm)
        obj, placement = client.describe_object(hbm.object_id)
        out = torch.empty((args.bytes,), dtype=torch.uint8)
        client.copy_from_device(hbm, out)
        if not torch.equal(original, out):
            raise AssertionError("HBM object roundtrip mismatch")
        print(
            "UFLOW_E01_HBM_OBJECT_PASS "
            f"object_id={obj.object_id} placement_id={placement.placement_id} "
            f"medium={placement.medium} target={placement.target} address_kind={placement.address_kind}",
            flush=True,
        )
    finally:
        if hbm is not None:
            hbm.release()
        client.close()


def ddr_child(args: argparse.Namespace) -> None:
    client = _client(socket_path=args.socket, device=args.src_device, role="phasee01-ddr-child")
    ddr = client.open_ddr(
        object_id=args.object_id,
        target=args.ddr_target,
        allowed_offset_bytes=args.overlay_offset,
        allowed_bytes=args.overlay_bytes,
    )
    try:
        expected = _pattern_bytes(args.overlay_bytes, offset=args.overlay_offset)
        actual = _read_ddr(ddr, args.overlay_bytes)
        if actual != expected:
            raise AssertionError("child did not observe parent DDR pattern")
        overlay = _pattern_bytes(args.overlay_bytes, offset=37)
        _write_ddr(ddr, overlay)
        client.mark_modified(ddr, offset_bytes=args.overlay_offset, nbytes=args.overlay_bytes)
        print(
            "UFLOW_E01_DDR_CHILD_OVERLAY_PASS "
            f"object_id={ddr.object_id} lease_id={ddr.lease_id} offset={args.overlay_offset} bytes={args.overlay_bytes}",
            flush=True,
        )
    finally:
        ddr.release(release_object=False)
        client.close()


def ddr_object(args: argparse.Namespace) -> None:
    client = _client(socket_path=args.socket, device=args.src_device, role="phasee01-ddr-parent")
    ddr = None
    try:
        ddr = client.allocate_ddr(
            name="phasee01.ddr.object",
            role="user",
            nbytes=args.bytes,
            target=args.ddr_target,
            mark_ready=False,
        )
        _write_ddr(ddr, _pattern_bytes(args.bytes))
        client.mark_ready(ddr)
        child_cmd = [
            sys.executable,
            str(Path(__file__).resolve()),
            "ddr-child",
            "--socket",
            args.socket,
            "--src-device",
            str(args.src_device),
            "--ddr-target",
            args.ddr_target,
            "--object-id",
            str(ddr.object_id),
            "--overlay-offset",
            str(args.overlay_offset),
            "--overlay-bytes",
            str(args.overlay_bytes),
        ]
        subprocess.run(child_cmd, check=True)
        overlay = _pattern_bytes(args.overlay_bytes, offset=37)
        actual = _read_ddr(ddr, args.overlay_bytes, offset=args.overlay_offset)
        if actual != overlay:
            raise AssertionError("parent did not observe child DDR overlay")
        obj, placement = client.describe_object(ddr.object_id)
        print(
            "UFLOW_E01_DDR_OBJECT_PASS "
            f"object_id={obj.object_id} placement_id={placement.placement_id} "
            f"medium={placement.medium} target={placement.target} path={ddr.path}",
            flush=True,
        )
    finally:
        if ddr is not None:
            ddr.release()
        client.close()


def hbm_to_ddr(args: argparse.Namespace) -> None:
    client = _client(socket_path=args.socket, device=args.src_device, role="phasee01-hbm-ddr")
    src = None
    dst = None
    try:
        original = _pattern_tensor(args.bytes, offset=3)
        src = client.allocate(name="phasee01.hbm_to_ddr.src", nbytes=args.bytes, shape=(args.bytes,), dtype=torch.uint8)
        client.copy_to_device(src, original)
        dst = client.allocate_ddr(name="phasee01.hbm_to_ddr.dst", nbytes=args.bytes, target=args.ddr_target, mark_ready=False)
        event = _execute(client, src, dst, nbytes=args.bytes, mode=args.transfer_mode, prefix="UFLOW_E01_HBM_TO_DDR")
        if _read_ddr(dst, args.bytes) != original.numpy().tobytes():
            raise AssertionError("HBM to DDR data mismatch")
        print(
            "UFLOW_E01_HBM_TO_DDR_PASS "
            f"event_id={event.event_id} bytes={args.bytes} fallback_used={event.fallback_used}",
            flush=True,
        )
    finally:
        if dst is not None:
            dst.release()
        if src is not None:
            src.release()
        client.close()


def ddr_to_hbm(args: argparse.Namespace) -> None:
    client = _client(socket_path=args.socket, device=args.src_device, role="phasee01-ddr-hbm")
    src = None
    dst = None
    try:
        payload = _pattern_bytes(args.bytes, offset=7)
        src = client.allocate_ddr(name="phasee01.ddr_to_hbm.src", nbytes=args.bytes, target=args.ddr_target, mark_ready=False)
        _write_ddr(src, payload)
        client.mark_ready(src)
        dst = client.allocate(name="phasee01.ddr_to_hbm.dst", nbytes=args.bytes, shape=(args.bytes,), dtype=torch.uint8, mark_ready=False)
        event = _execute(client, src, dst, nbytes=args.bytes, mode=args.transfer_mode, prefix="UFLOW_E01_DDR_TO_HBM")
        out = torch.empty((args.bytes,), dtype=torch.uint8)
        client.copy_from_device(dst, out)
        if out.numpy().tobytes() != payload:
            raise AssertionError("DDR to HBM data mismatch")
        print(
            "UFLOW_E01_DDR_TO_HBM_PASS "
            f"event_id={event.event_id} bytes={args.bytes} fallback_used={event.fallback_used}",
            flush=True,
        )
    finally:
        if dst is not None:
            dst.release()
        if src is not None:
            src.release()
        client.close()


def hbm_to_hbm_same(args: argparse.Namespace) -> None:
    client = _client(socket_path=args.socket, device=args.src_device, role="phasee01-hbm-hbm-same")
    src = None
    dst = None
    try:
        original = _pattern_tensor(args.bytes, offset=11)
        src = client.allocate(name="phasee01.hbm_same.src", nbytes=args.bytes, shape=(args.bytes,), dtype=torch.uint8)
        dst = client.allocate(name="phasee01.hbm_same.dst", nbytes=args.bytes, shape=(args.bytes,), dtype=torch.uint8, mark_ready=False)
        client.copy_to_device(src, original)
        event = _execute(client, src, dst, nbytes=args.bytes, mode=args.transfer_mode, prefix="UFLOW_E01_HBM_TO_HBM_SAME_DEVICE")
        out = torch.empty((args.bytes,), dtype=torch.uint8)
        client.copy_from_device(dst, out)
        if not torch.equal(original, out):
            raise AssertionError("same-device HBM to HBM data mismatch")
        print(
            "UFLOW_E01_HBM_TO_HBM_SAME_DEVICE_PASS "
            f"event_id={event.event_id} bytes={args.bytes} engine={event.actual_engine}",
            flush=True,
        )
    finally:
        if dst is not None:
            dst.release()
        if src is not None:
            src.release()
        client.close()


def hbm_to_hbm_cross(args: argparse.Namespace) -> None:
    dst_socket = args.dst_socket or args.socket
    src_client = _client(socket_path=args.socket, device=args.src_device, role="phasee01-hbm-cross-src")
    dst_client = _client(socket_path=dst_socket, device=args.dst_device, role="phasee01-hbm-cross-dst")
    src_hbm = None
    dst_hbm = None
    try:
        original = _pattern_tensor(args.bytes, offset=19)
        src_hbm = src_client.allocate(name="phasee01.hbm_cross.src_hbm", nbytes=args.bytes, shape=(args.bytes,), dtype=torch.uint8)
        src_client.copy_to_device(src_hbm, original)
        dst_hbm = dst_client.allocate(name="phasee01.hbm_cross.dst_hbm", nbytes=args.bytes, shape=(args.bytes,), dtype=torch.uint8, mark_ready=False)
        event = _execute(src_client, src_hbm, dst_hbm, nbytes=args.bytes, mode=args.transfer_mode, prefix="UFLOW_E01_HBM_TO_HBM_CROSS_DEVICE")
        out = torch.empty((args.bytes,), dtype=torch.uint8)
        dst_client.copy_from_device(dst_hbm, out)
        if not torch.equal(original, out):
            raise AssertionError("cross-device HBM to HBM data mismatch")
        print(
            "UFLOW_E01_HBM_TO_HBM_CROSS_DEVICE_PASS "
            f"src_device={args.src_device} dst_device={args.dst_device} bytes={args.bytes} "
            f"event_id={event.event_id} engine={event.actual_engine} "
            f"fallback_used={str(event.fallback_used).lower()} fallback_reason={event.fallback_reason}",
            flush=True,
        )
    finally:
        if dst_hbm is not None:
            dst_hbm.release()
        dst_client.close()
        if src_hbm is not None:
            src_hbm.release()
        src_client.close()


def ddr_to_ddr(args: argparse.Namespace) -> None:
    client = _client(socket_path=args.socket, device=args.src_device, role="phasee01-ddr-ddr")
    src = None
    dst = None
    try:
        payload = _pattern_bytes(args.bytes, offset=23)
        src = client.allocate_ddr(name="phasee01.ddr_to_ddr.src", nbytes=args.bytes, target=args.ddr_target, mark_ready=False)
        dst = client.allocate_ddr(name="phasee01.ddr_to_ddr.dst", nbytes=args.bytes, target=args.ddr_dst_target, mark_ready=False)
        _write_ddr(src, payload)
        client.mark_ready(src)
        event = _execute(client, src, dst, nbytes=args.bytes, mode=args.transfer_mode, prefix="UFLOW_E01_DDR_TO_DDR")
        if _read_ddr(dst, args.bytes) != payload:
            raise AssertionError("DDR to DDR data mismatch")
        print(
            "UFLOW_E01_DDR_TO_DDR_PASS "
            f"event_id={event.event_id} bytes={args.bytes} src_target={args.ddr_target} dst_target={args.ddr_dst_target}",
            flush=True,
        )
    finally:
        if dst is not None:
            dst.release()
        if src is not None:
            src.release()
        client.close()


def run_all(args: argparse.Namespace) -> None:
    hbm_object(args)
    ddr_object(args)
    hbm_to_ddr(args)
    ddr_to_hbm(args)
    hbm_to_hbm_same(args)
    hbm_to_hbm_cross(args)
    ddr_to_ddr(args)
    print(
        "UFLOW_E01_UNIFIED_TRANSFER_SMOKE_PASS "
        f"src_device={args.src_device} dst_device={args.dst_device} bytes={args.bytes}",
        flush=True,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="mode", required=True)
    names = [
        "all",
        "hbm-object",
        "ddr-object",
        "ddr-child",
        "hbm-to-ddr",
        "ddr-to-hbm",
        "hbm-to-hbm-same",
        "hbm-to-hbm-cross",
        "ddr-to-ddr",
    ]
    for name in names:
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.mode == "all":
        run_all(args)
    elif args.mode == "hbm-object":
        hbm_object(args)
    elif args.mode == "ddr-object":
        ddr_object(args)
    elif args.mode == "ddr-child":
        ddr_child(args)
    elif args.mode == "hbm-to-ddr":
        hbm_to_ddr(args)
    elif args.mode == "ddr-to-hbm":
        ddr_to_hbm(args)
    elif args.mode == "hbm-to-hbm-same":
        hbm_to_hbm_same(args)
    elif args.mode == "hbm-to-hbm-cross":
        hbm_to_hbm_cross(args)
    elif args.mode == "ddr-to-ddr":
        ddr_to_ddr(args)
    else:
        raise ValueError(args.mode)


if __name__ == "__main__":
    main()
