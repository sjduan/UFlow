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

from uflow import DdrBuffer, ManagedBuffer, UFlowClient  # noqa: E402


def _acl_lib() -> str:
    return os.environ.get("UF_ACL_LIB", "/home/sj/git/data-service/build/lib/libuf_acl_shim.so")


def _client(*, socket_path: str, device: int, role: str) -> UFlowClient:
    return UFlowClient(
        enabled=True,
        socket_path=socket_path,
        device_id=device,
        acl_lib_path=_acl_lib(),
        client_role=role,
        model_id="phasee02",
    )


def _pattern(nbytes: int, *, offset: int = 0) -> torch.Tensor:
    return (torch.arange(nbytes, dtype=torch.uint8) + int(offset)) % 251


def _read_ddr(ddr: DdrBuffer, nbytes: int) -> bytes:
    view = ddr.as_memoryview()
    try:
        return bytes(view[:nbytes])
    finally:
        del view


def _transfer(client: UFlowClient, src: ManagedBuffer | DdrBuffer, dst: ManagedBuffer | DdrBuffer, *, nbytes: int, mode: str):
    plan = client.plan_transfer(src=src, dst=dst, nbytes=nbytes, mode=mode)
    event = client.submit_transfer(plan)
    return plan, client.wait_event(event)


def mark_dirty_ready_roundtrip(args: argparse.Namespace) -> None:
    client = _client(socket_path=args.socket, device=args.device, role="phasee02-dirty-ready")
    src = None
    dst = None
    try:
        payload = _pattern(args.bytes, offset=31)
        src = client.allocate(
            name="phasee02.dirty_ready.src",
            nbytes=args.bytes,
            shape=(args.bytes,),
            dtype=torch.uint8,
            mark_ready=False,
        )
        client.copy_to_device(src, payload)
        client.mark_dirty(src, offset_bytes=0, nbytes=args.bytes)
        client.mark_ready(src)
        dst = client.allocate_ddr(
            name="phasee02.dirty_ready.dst",
            nbytes=args.bytes,
            target=args.ddr_target,
            mark_ready=False,
        )
        plan, event = _transfer(client, src, dst, nbytes=args.bytes, mode=args.transfer_mode)
        if event.status != "complete":
            raise AssertionError(f"dirty/ready transfer failed: {event}")
        if event.actual_path != "hbm_to_ddr" or event.fallback_used:
            raise AssertionError(f"unexpected transfer actual path: plan={plan} event={event}")
        if _read_ddr(dst, args.bytes) != payload.numpy().tobytes():
            raise AssertionError("dirty/ready HBM->DDR payload mismatch")
        print(
            "UFLOW_E02_MARK_DIRTY_READY_PASS "
            f"event_id={event.event_id} engine={event.actual_engine} bytes={args.bytes}",
            flush=True,
        )
    finally:
        if dst is not None:
            dst.release()
        if src is not None:
            src.release()
        client.close()


def not_ready_rejected(args: argparse.Namespace) -> None:
    client = _client(socket_path=args.socket, device=args.device, role="phasee02-not-ready")
    src = None
    dst = None
    try:
        src = client.allocate(
            name="phasee02.not_ready.src",
            nbytes=args.bytes,
            shape=(args.bytes,),
            dtype=torch.uint8,
            mark_ready=False,
        )
        dst = client.allocate_ddr(
            name="phasee02.not_ready.dst",
            nbytes=args.bytes,
            target=args.ddr_target,
            mark_ready=False,
        )
        _, event = _transfer(client, src, dst, nbytes=args.bytes, mode=args.transfer_mode)
        if event.status != "failed":
            raise AssertionError(f"not-ready source should fail, got {event.status}: {event}")
        if "not ready" not in event.error_message:
            raise AssertionError(f"not-ready failure did not explain state: {event.error_message}")
        print(
            "UFLOW_E02_NOT_READY_REJECTED_PASS "
            f"event_id={event.event_id} error={event.error_message}",
            flush=True,
        )
    finally:
        if dst is not None:
            dst.release()
        if src is not None:
            src.release()
        client.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket", default=os.environ.get("UF_SOCKET", "/tmp/uflow_e02.sock"))
    parser.add_argument("--device", type=int, default=int(os.environ.get("UF_TARGET_DEVICE", "0")))
    parser.add_argument("--bytes", type=int, default=int(os.environ.get("UF_TEST_BYTES", str(1 << 20))))
    parser.add_argument("--ddr-target", default=os.environ.get("UF_DDR_TARGET", "host:0"))
    parser.add_argument("--transfer-mode", default=os.environ.get("UF_E02_TRANSFER_MODE", "auto"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    mark_dirty_ready_roundtrip(args)
    not_ready_rejected(args)
    print(
        "UFLOW_E02_SERVICE_OWNED_TRANSFER_SMOKE_PASS "
        f"device={args.device} bytes={args.bytes}",
        flush=True,
    )


if __name__ == "__main__":
    main()
