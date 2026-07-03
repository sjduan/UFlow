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

from uflow import MANDATORY_DDR_HINT, UFlowClient  # noqa: E402


def _client(device: int, role: str) -> UFlowClient:
    return UFlowClient.from_env(device_id=device, client_role=role, model_id="phasea08-gate-a")


def _pattern(nbytes: int, *, offset: int = 0) -> bytes:
    return bytes(((idx + offset) % 251 for idx in range(nbytes)))


def _write_ddr(ddr, payload: bytes, *, offset: int = 0) -> None:
    view = ddr.as_memoryview()
    try:
        view[offset : offset + len(payload)] = payload
    finally:
        del view


def _read_ddr(ddr, nbytes: int, *, offset: int = 0) -> bytes:
    view = ddr.as_memoryview()
    try:
        return bytes(view[offset : offset + nbytes])
    finally:
        del view


def ddr_child(args: argparse.Namespace) -> None:
    client = _client(args.device, "phasea08-ddr-child")
    ddr = client.open_ddr(
        object_id=args.object_id,
        target=args.ddr_target,
        allowed_offset_bytes=args.overlay_offset,
        allowed_bytes=args.overlay_bytes,
    )
    try:
        before = _read_ddr(ddr, args.overlay_bytes)
        expected_before = _pattern(args.overlay_bytes, offset=args.overlay_offset)
        if before != expected_before:
            raise AssertionError("child did not observe parent DDR pattern")
        overlay = _pattern(args.overlay_bytes, offset=17)
        _write_ddr(ddr, overlay)
        client.mark_modified(ddr, offset_bytes=args.overlay_offset, nbytes=args.overlay_bytes)
        print(
            "DDR_CHILD_OVERLAY_PASS "
            f"object_id={ddr.object_id} lease_id={ddr.lease_id} offset={args.overlay_offset} bytes={args.overlay_bytes}",
            flush=True,
        )
    finally:
        ddr.release(release_object=False)
        client.close()


def ddr_sharing(args: argparse.Namespace) -> None:
    parent = _client(args.device, "phasea08-ddr-parent")
    ddr = parent.allocate_ddr(
        name="phasea08.shared.ddr",
        role="user",
        nbytes=args.bytes,
        target=args.ddr_target,
        immutable=False,
        mark_ready=False,
    )
    try:
        _write_ddr(ddr, _pattern(args.bytes))
        parent.mark_ready(ddr)
        child_cmd = [
            sys.executable,
            str(Path(__file__).resolve()),
            "ddr-child",
            "--device",
            str(args.device),
            "--object-id",
            str(ddr.object_id),
            "--ddr-target",
            args.ddr_target,
            "--overlay-offset",
            str(args.overlay_offset),
            "--overlay-bytes",
            str(args.overlay_bytes),
        ]
        subprocess.run(child_cmd, check=True)
        overlay = _pattern(args.overlay_bytes, offset=17)
        actual = _read_ddr(ddr, args.overlay_bytes, offset=args.overlay_offset)
        if actual != overlay:
            raise AssertionError("parent did not observe child DDR overlay")
        stats = parent.stats()
        print(
            "DDR_LEASE_SHARING_SMOKE_PASS "
            f"object_id={ddr.object_id} parent_lease={ddr.lease_id} "
            f"active_leases={stats.get('active_leases', 'unknown')} path={ddr.path}",
            flush=True,
        )
    finally:
        ddr.release(release_object=True)
        parent.close()


def _make_host_tensor(nbytes: int) -> torch.Tensor:
    return torch.arange(nbytes, dtype=torch.uint8) % 251


def _transfer_roundtrip(args: argparse.Namespace, mode: str, *, use_async: bool) -> dict[str, object]:
    client = _client(args.device, f"phasea08-transfer-{mode}")
    src_hbm = None
    ddr = None
    dst_hbm = None
    try:
        original = _make_host_tensor(args.bytes).contiguous()
        src_hbm = client.allocate(
            name=f"phasea08.{mode}.src",
            role="user",
            nbytes=args.bytes,
            shape=(args.bytes,),
            dtype=torch.uint8,
            immutable=False,
        )
        client.copy_to_device(src_hbm, original)
        ddr = client.allocate_ddr(
            name=f"phasea08.{mode}.ddr",
            role="user",
            nbytes=args.bytes,
            target=args.ddr_target,
            immutable=False,
            mark_ready=False,
        )
        if use_async:
            offload = client.copy_device_to_ddr_async(src_hbm, ddr, mode=mode)
            offload.wait()
            offload_stats = offload.stats_dict()
        else:
            client.copy_device_to_ddr(src_hbm, ddr, mode=mode)
            offload_stats = {"mode": mode, "direction": "device_to_ddr"}
        client.mark_ready(ddr)
        dst_hbm = client.allocate(
            name=f"phasea08.{mode}.dst",
            role="user",
            nbytes=args.bytes,
            shape=(args.bytes,),
            dtype=torch.uint8,
            immutable=False,
            mark_ready=False,
        )
        if use_async:
            reload = client.copy_ddr_to_device_async(ddr, dst_hbm, mode=mode)
            reload.wait()
            reload_stats = reload.stats_dict()
        else:
            client.copy_ddr_to_device(ddr, dst_hbm, mode=mode)
            reload_stats = {"mode": mode, "direction": "ddr_to_device"}
        client.mark_ready(dst_hbm)
        out = torch.empty((args.bytes,), dtype=torch.uint8)
        client.copy_from_device(dst_hbm, out)
        if not torch.equal(original, out):
            raise AssertionError(f"roundtrip mismatch for mode {mode}")
        marker = f"UFLOW_{mode.upper()}_TRANSFER_SMOKE_PASS"
        print(f"{marker} bytes={args.bytes} offload={offload_stats} reload={reload_stats}", flush=True)
        return {"offload": offload_stats, "reload": reload_stats}
    finally:
        if dst_hbm is not None:
            dst_hbm.release()
        if ddr is not None:
            ddr.release()
        if src_hbm is not None:
            src_hbm.release()
        client.close()


def transfer_modes(args: argparse.Namespace) -> None:
    _transfer_roundtrip(args, "sync", use_async=False)
    _transfer_roundtrip(args, "pinned_sync", use_async=False)
    print("PINNED_POOL_SMOKE_PASS mode=pinned_sync", flush=True)
    _transfer_roundtrip(args, "async", use_async=True)
    _transfer_roundtrip(args, "pinned_async", use_async=True)


def prepared_transfer(args: argparse.Namespace) -> None:
    client = _client(args.device, "phasea08-prepared")
    src_hbm = None
    ddr = None
    dst_hbm = None
    try:
        original = _make_host_tensor(args.bytes).contiguous()
        src_hbm = client.allocate(
            name="phasea08.prepared.src",
            role="user",
            nbytes=args.bytes,
            shape=(args.bytes,),
            dtype=torch.uint8,
        )
        client.copy_to_device(src_hbm, original)
        ddr = client.allocate_ddr(
            name="phasea08.prepared.ddr",
            role="user",
            nbytes=args.bytes,
            target=args.ddr_target,
            mark_ready=False,
        )
        path = client.prepare_transfer(src=src_hbm, dst=ddr, mode="pinned_async")
        offload = path.offload_async()
        offload.wait()
        client.mark_ready(ddr)
        path.close()

        dst_hbm = client.allocate(
            name="phasea08.prepared.dst",
            role="user",
            nbytes=args.bytes,
            shape=(args.bytes,),
            dtype=torch.uint8,
            mark_ready=False,
        )
        path = client.prepare_transfer(src=ddr, dst=dst_hbm, mode="pinned_async")
        reload = path.reload_async()
        reload.wait()
        client.mark_ready(dst_hbm)
        path.close()

        out = torch.empty((args.bytes,), dtype=torch.uint8)
        client.copy_from_device(dst_hbm, out)
        if not torch.equal(original, out):
            raise AssertionError("prepared transfer mismatch")
        print(
            "UFLOW_PREPARED_TRANSFER_SMOKE_PASS "
            f"offload={offload.stats_dict()} reload={reload.stats_dict()}",
            flush=True,
        )
    finally:
        if dst_hbm is not None:
            dst_hbm.release()
        if ddr is not None:
            ddr.release()
        if src_hbm is not None:
            src_hbm.release()
        client.close()


def stream_event(args: argparse.Namespace) -> None:
    client = _client(args.device, "phasea08-stream-event")
    hbm = None
    stream_h2d = None
    stream_wait = None
    event_h2d = None
    event_waited = None
    in_chunk = None
    try:
        hbm = client.allocate(
            name="phasea08.stream_event.hbm",
            role="user",
            nbytes=args.bytes,
            shape=(args.bytes,),
            dtype=torch.uint8,
            immutable=False,
            mark_ready=False,
        )
        pool = client.ensure_pinned_pool()
        if args.bytes > pool.chunk_bytes:
            raise ValueError(
                f"--bytes={args.bytes} exceeds pinned chunk bytes={pool.chunk_bytes}; "
                "increase UF_PINNED_CHUNK_BYTES or reduce --bytes"
            )
        in_chunk = pool.acquire()
        stream_h2d = client.create_stream_handle()
        stream_wait = client.create_stream_handle()
        event_h2d = client.create_event_handle()
        event_waited = client.create_event_handle()
        if event_h2d.raw_handle == 0 or event_waited.raw_handle == 0:
            raise AssertionError("UFlow ACL event raw handle must be non-zero")

        original = _make_host_tensor(args.bytes).contiguous()
        actual = torch.empty((args.bytes,), dtype=torch.uint8)
        ctypes.memmove(in_chunk.ptr, original.data_ptr(), args.bytes)
        expected_bytes = original.numpy().tobytes()
        input_prefix = ctypes.string_at(in_chunk.ptr, min(args.bytes, 16))
        if input_prefix != expected_bytes[: len(input_prefix)]:
            raise AssertionError(
                "pinned input copy mismatch "
                f"expected_prefix={list(expected_bytes[:16])} actual_prefix={list(input_prefix)}"
            )

        client.copy_host_ptr_to_device_async(
            hbm,
            in_chunk.ptr,
            args.bytes,
            stream=stream_h2d,
            event=event_h2d,
        )
        client.stream_wait_event_handle(stream_wait, event_h2d)
        client.record_event_handle(event_waited, stream_wait)
        client.synchronize_event_handle(event_waited)
        client.copy_from_device(hbm, actual)
        if not torch.equal(original, actual):
            raise AssertionError("stream/event pinned H2D async check mismatch")
        client.mark_ready(hbm)
        print(
            "UFLOW_STREAM_EVENT_PINNED_SMOKE_PASS "
            f"bytes={args.bytes} h2d_event_id={event_h2d.event_id} "
            f"h2d_raw=0x{event_h2d.raw_handle:x} wait_event_id={event_waited.event_id} "
            f"wait_raw=0x{event_waited.raw_handle:x} pinned_allocations={pool.allocations}",
            flush=True,
        )
    finally:
        if event_waited is not None:
            client.destroy_event_handle(event_waited)
        if event_h2d is not None:
            client.destroy_event_handle(event_h2d)
        if stream_wait is not None:
            client.destroy_stream_handle(stream_wait)
        if stream_h2d is not None:
            client.destroy_stream_handle(stream_h2d)
        if in_chunk is not None:
            client.ensure_pinned_pool().release(in_chunk)
        if hbm is not None:
            hbm.release()
        client.close()


def numa_preflight(args: argparse.Namespace) -> None:
    client = _client(args.device, "phasea08-preflight")
    try:
        stats = client.stats()
        safe = int(stats.get("ddr_safe_allocatable_bytes", "0"))
        root = stats.get("ddr_root", "")
        node = stats.get("ddr_numa_node", "")
        failed = False
        try:
            too_large = max(safe + 1, args.bytes * 1024)
            obj = client.allocate_ddr(
                name="phasea08.preflight.too_large",
                role="user",
                nbytes=too_large,
                target=args.ddr_target,
                mark_ready=False,
            )
            obj.release()
        except RuntimeError:
            failed = True
        if safe > 0 and not failed:
            raise AssertionError("oversized DDR preflight request unexpectedly succeeded")
        print(
            "NUMA_PREFLIGHT_SMOKE_PASS "
            f"root={root} node={node} safe_allocatable={safe} oversized_failed={failed}",
            flush=True,
        )
    finally:
        client.close()


def run_all(args: argparse.Namespace) -> None:
    ddr_sharing(args)
    transfer_modes(args)
    stream_event(args)
    prepared_transfer(args)
    numa_preflight(args)
    print("UFLOW_GATE_A_SMOKE_PASS", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="mode", required=True)
    for name in ["all", "ddr-sharing", "transfer", "stream-event", "prepared-transfer", "numa-preflight", "ddr-child"]:
        p = sub.add_parser(name)
        p.add_argument("--device", type=int, default=int(os.environ.get("UF_TARGET_DEVICE", "0")))
        p.add_argument("--bytes", type=int, default=int(os.environ.get("UF_TEST_BYTES", str(1 << 20))))
        p.add_argument("--ddr-target", default=os.environ.get("UF_DDR_TARGET", "host:0"))
        p.add_argument("--overlay-offset", type=int, default=1024)
        p.add_argument("--overlay-bytes", type=int, default=4096)
        p.add_argument("--object-id", type=int, default=0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    os.environ.setdefault("UF_ENABLE", "1")
    if args.mode == "all":
        run_all(args)
    elif args.mode == "ddr-sharing":
        ddr_sharing(args)
    elif args.mode == "transfer":
        transfer_modes(args)
    elif args.mode == "stream-event":
        stream_event(args)
    elif args.mode == "prepared-transfer":
        prepared_transfer(args)
    elif args.mode == "numa-preflight":
        numa_preflight(args)
    elif args.mode == "ddr-child":
        ddr_child(args)
    else:
        raise ValueError(args.mode)


if __name__ == "__main__":
    main()
