from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SDK_ROOT = REPO_ROOT / "sdk" / "python"
if str(SDK_ROOT) not in sys.path:
    sys.path.insert(0, str(SDK_ROOT))

from uflow import DdrBuffer, TransferEvent, UFlowClient  # noqa: E402


def _acl_lib() -> str:
    return os.environ.get("UF_ACL_LIB", "/home/sj/git/data-service/build/lib/libuf_acl_shim.so")


def _client(args: argparse.Namespace) -> UFlowClient:
    return UFlowClient(
        enabled=True,
        socket_path=args.socket,
        device_id=args.device,
        acl_lib_path=_acl_lib(),
        client_role="phasea08-h0-hotpath",
        model_id="phasea08-h0-hotpath",
    )


def _pattern(nbytes: int) -> bytes:
    return bytes((idx % 251 for idx in range(nbytes)))


def _write_ddr(ddr: DdrBuffer, payload: bytes) -> None:
    view = ddr.as_memoryview()
    try:
        view[: len(payload)] = payload
    finally:
        del view


def _read_ddr(ddr: DdrBuffer, nbytes: int) -> bytes:
    view = ddr.as_memoryview()
    try:
        return bytes(view[:nbytes])
    finally:
        del view


def _wait_and_assert(client: UFlowClient, event: TransferEvent, *, direction: str, nbytes: int, mode: str) -> TransferEvent:
    final = client.wait_event(event, timeout_ms=180_000)
    if final.status != "complete":
        raise AssertionError(f"{direction}: transfer failed: {final}")
    if final.bytes_done != nbytes:
        raise AssertionError(f"{direction}: bytes_done={final.bytes_done}, expected={nbytes}")
    if mode in {"auto", "direct_async"}:
        if final.actual_engine != "acl_direct_async_thp":
            raise AssertionError(f"{direction}: unexpected direct engine={final.actual_engine}")
        expected_path = "ddr_hbm_direct_thp" if direction == "h2d" else "hbm_ddr_direct_thp"
        if final.actual_path != expected_path:
            raise AssertionError(f"{direction}: unexpected direct path={final.actual_path}, expected={expected_path}")
    elif mode == "pinned_async":
        if final.actual_engine != "acl_pinned_async_channel":
            raise AssertionError(f"{direction}: unexpected pinned engine={final.actual_engine}")
        if final.actual_path != "memfd_pinned_hbm_channel":
            raise AssertionError(f"{direction}: unexpected pinned path={final.actual_path}")
        if final.channel_pinned_footprint_bytes <= 0:
            raise AssertionError(f"{direction}: missing pinned footprint")
        if final.channel_cpu_copy_us <= 0:
            raise AssertionError(f"{direction}: missing pinned cpu copy timing")
    elif mode == "pinned_sync":
        if final.actual_engine != "acl_pinned_sync_channel":
            raise AssertionError(f"{direction}: unexpected pinned sync engine={final.actual_engine}")
        if final.actual_path != "memfd_pinned_hbm_channel":
            raise AssertionError(f"{direction}: unexpected pinned path={final.actual_path}")
    else:
        raise AssertionError(f"{direction}: unsupported H0 mode={mode!r}; use auto, direct_async, pinned_async, or pinned_sync")
    if mode.startswith("pinned"):
        if final.channel_direction != direction:
            raise AssertionError(f"{direction}: channel_direction={final.channel_direction}")
        if final.channel_lane_id <= 0:
            raise AssertionError(f"{direction}: missing channel lane id")
        if final.channel_chunk_count != 2:
            raise AssertionError(f"{direction}: channel_chunk_count={final.channel_chunk_count}, expected=2")
        if final.channel_chunks_transferred <= 0:
            raise AssertionError(f"{direction}: missing chunks_transferred")
        if final.channel_acl_copy_us <= 0:
            raise AssertionError(f"{direction}: missing ACL copy timing")
    elif final.actual_latency_us <= 0 or final.actual_bandwidth_gib_s <= 0:
        raise AssertionError(
            f"{direction}: missing direct timing latency={final.actual_latency_us} "
            f"bandwidth={final.actual_bandwidth_gib_s}"
        )
    print(
        "UFLOW_A08_H0_TRANSFER_PASS "
        f"direction={direction} mode={mode} event_id={final.event_id} engine={final.actual_engine} "
        f"path={final.actual_path} "
        f"lane_id={final.channel_lane_id} chunk_bytes={final.channel_chunk_bytes} "
        f"chunks={final.channel_chunks_transferred} pinned={final.channel_pinned_footprint_bytes} "
        f"cpu_copy_us={final.channel_cpu_copy_us:.3f} acl_copy_us={final.channel_acl_copy_us:.3f} "
        f"latency_us={final.actual_latency_us:.3f} bandwidth_gib_s={final.actual_bandwidth_gib_s:.3f} "
        f"pipeline_overlap={str(final.channel_pipeline_overlap).lower()}",
        flush=True,
    )
    return final


def run(args: argparse.Namespace) -> None:
    client = _client(args)
    src_ddr = None
    hbm = None
    dst_ddr = None
    try:
        payload = _pattern(args.bytes)
        src_ddr = client.allocate_ddr(
            name="phasea08.h0.src_ddr",
            role="weight",
            nbytes=args.bytes,
            target=args.ddr_target,
            immutable=True,
            mark_ready=False,
        )
        _write_ddr(src_ddr, payload)
        client.mark_ready(src_ddr)
        hbm = client.allocate(
            name="phasea08.h0.hbm",
            role="weight",
            nbytes=args.bytes,
            hint="mandatory:hbm",
            target=f"npu:{args.device}",
            immutable=True,
            mark_ready=False,
        )
        h2d_plan = client.plan_transfer(src=src_ddr, dst=hbm, nbytes=args.bytes, mode=args.mode)
        h2d_event = client.submit_transfer(h2d_plan)
        _wait_and_assert(client, h2d_event, direction="h2d", nbytes=args.bytes, mode=args.mode)

        dst_ddr = client.allocate_ddr(
            name="phasea08.h0.dst_ddr",
            role="weight",
            nbytes=args.bytes,
            target=args.ddr_target,
            immutable=False,
            mark_ready=False,
        )
        d2h_plan = client.plan_transfer(src=hbm, dst=dst_ddr, nbytes=args.bytes, mode=args.mode)
        d2h_event = client.submit_transfer(d2h_plan)
        _wait_and_assert(client, d2h_event, direction="d2h", nbytes=args.bytes, mode=args.mode)

        actual = _read_ddr(dst_ddr, args.bytes)
        if actual != payload:
            raise AssertionError("DDR->HBM->DDR payload mismatch")

        stats = client.stats()
        if args.mode.startswith("pinned") and (
            int(stats.get("h2d_lane_count", "0")) < 1 or int(stats.get("d2h_lane_count", "0")) < 1
        ):
            raise AssertionError(f"missing channel stats: {stats}")
        print(
            "UFLOW_A08_H0_HOTPATH_PASS "
            f"mode={args.mode} bytes={args.bytes} h2d_lanes={stats.get('h2d_lane_count')} "
            f"d2h_lanes={stats.get('d2h_lane_count')} pinned_total={stats.get('pinned_total_bytes')} "
            f"chunk_h2d={stats.get('chunk_bytes_h2d')} chunk_d2h={stats.get('chunk_bytes_d2h')} "
            f"acquires={stats.get('transfer_channel_acquires')}",
            flush=True,
        )
    finally:
        if dst_ddr is not None:
            dst_ddr.release()
        if hbm is not None:
            hbm.release()
        if src_ddr is not None:
            src_ddr.release()
        client.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PhaseA-08 Gate H0 daemon hotpath smoke.")
    parser.add_argument("--socket", default=os.environ.get("UF_SOCKET", "/tmp/uflow_a08_h0.sock"))
    parser.add_argument("--device", type=int, default=int(os.environ.get("UF_A08_DEVICE", "7")))
    parser.add_argument("--bytes", type=int, default=int(os.environ.get("UF_A08_H0_BYTES", str(32 << 20))))
    parser.add_argument("--mode", default=os.environ.get("UF_A08_H0_MODE", "auto"))
    parser.add_argument("--ddr-target", default=os.environ.get("UF_DDR_TARGET", "host:0"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.mode = args.mode.lower()
    run(args)


if __name__ == "__main__":
    main()
