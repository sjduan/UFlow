from __future__ import annotations

import argparse
import os
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SDK_ROOT = REPO_ROOT / "sdk" / "python"
if str(SDK_ROOT) not in sys.path:
    sys.path.insert(0, str(SDK_ROOT))

from uflow import DdrBuffer, ManagedBuffer, TransferEvent, TransferPlan, UFlowClient  # noqa: E402


MIB = 1024 * 1024
GIB = 1024 * 1024 * 1024


@dataclass
class TransferPair:
    lane_index: int
    src: DdrBuffer
    dst: ManagedBuffer
    plan: TransferPlan | None = None
    event: TransferEvent | None = None
    final: TransferEvent | None = None


def _acl_lib() -> str:
    return os.environ.get("UF_ACL_LIB", "/home/sj/git/data-service/build/lib/libuf_acl_shim.so")


def _parse_size(value: str) -> int:
    text = value.strip().lower()
    scale = 1
    if text.endswith("gib"):
        scale = GIB
        text = text[:-3]
    elif text.endswith("gb") or text.endswith("g"):
        scale = GIB
        text = text.rstrip("gb")
    elif text.endswith("mib"):
        scale = MIB
        text = text[:-3]
    elif text.endswith("mb") or text.endswith("m"):
        scale = MIB
        text = text.rstrip("mb")
    return int(float(text) * scale)


def _parse_size_list(value: str) -> list[int]:
    return [_parse_size(item) for item in value.split(",") if item.strip()]


def _client(args: argparse.Namespace) -> UFlowClient:
    return UFlowClient(
        enabled=True,
        socket_path=args.socket,
        device_id=args.device,
        acl_lib_path=_acl_lib(),
        client_role="phasea08-h2d-bandwidth",
        model_id="phasea08-h2d-bandwidth",
    )


def _fill_ddr(ddr: DdrBuffer, nbytes: int, *, lane_index: int) -> None:
    view = ddr.as_memoryview()
    block_len = min(8 * MIB, nbytes)
    block = bytes(((idx + lane_index * 17) % 251 for idx in range(block_len)))
    try:
        for offset in range(0, nbytes, block_len):
            chunk = min(block_len, nbytes - offset)
            view[offset : offset + chunk] = block[:chunk]
    finally:
        del view


def _make_pairs(client: UFlowClient, args: argparse.Namespace, *, nbytes: int, lanes: int) -> list[TransferPair]:
    pairs: list[TransferPair] = []
    for lane in range(lanes):
        src = client.allocate_ddr(
            name=f"phasea08.h2d_bw.size{nbytes}.lane{lane}.src",
            role="weight",
            nbytes=nbytes,
            target=args.ddr_target,
            immutable=True,
            mark_ready=False,
        )
        if not args.skip_fill:
            _fill_ddr(src, nbytes, lane_index=lane)
        client.mark_ready(src)
        dst = client.allocate(
            name=f"phasea08.h2d_bw.size{nbytes}.lane{lane}.hbm",
            role="weight",
            nbytes=nbytes,
            hint="mandatory:hbm",
            target=f"npu:{args.device}",
            immutable=False,
            mark_ready=False,
        )
        pairs.append(TransferPair(lane_index=lane, src=src, dst=dst))
    return pairs


def _release_pairs(pairs: list[TransferPair]) -> None:
    for pair in reversed(pairs):
        try:
            pair.dst.release()
        except Exception:
            pass
        try:
            pair.src.release()
        except Exception:
            pass


def _format_size(nbytes: int) -> str:
    if nbytes % GIB == 0:
        return f"{nbytes // GIB}GiB"
    if nbytes % MIB == 0:
        return f"{nbytes // MIB}MiB"
    return str(nbytes)


def _bandwidth_gib(total_bytes: int, seconds: float) -> float:
    return total_bytes / max(seconds, 1e-12) / GIB


def _sum(events: list[TransferEvent], attr: str) -> float:
    return float(sum(float(getattr(event, attr)) for event in events))


def _run_case(client: UFlowClient, args: argparse.Namespace, *, nbytes: int, lanes: int) -> None:
    pairs = _make_pairs(client, args, nbytes=nbytes, lanes=lanes)
    total_bytes = nbytes * lanes
    for pair in pairs:
        pair.plan = client.plan_transfer(src=pair.src, dst=pair.dst, nbytes=nbytes, mode=args.mode)

    start_barrier = threading.Barrier(len(pairs) + 1)
    submit_errors: list[BaseException] = []

    def submit_pair(pair: TransferPair) -> None:
        try:
            assert pair.plan is not None
            start_barrier.wait()
            pair.event = client.submit_transfer(pair.plan)
        except BaseException as exc:  # keep worker exceptions visible in main thread
            submit_errors.append(exc)

    threads = [threading.Thread(target=submit_pair, args=(pair,), daemon=True) for pair in pairs]
    for thread in threads:
        thread.start()
    submitted_at = time.perf_counter()
    try:
        start_barrier.wait()
        for thread in threads:
            thread.join(timeout=10.0)
            if thread.is_alive():
                raise TimeoutError("timed out waiting for submit thread")
        if submit_errors:
            raise submit_errors[0]
        for pair in pairs:
            assert pair.event is not None
            pair.final = client.wait_event(pair.event, timeout_ms=args.timeout_ms)
            if pair.final.status != "complete":
                raise RuntimeError(f"transfer failed lane={pair.lane_index}: {pair.final}")
            if pair.final.bytes_done != nbytes:
                raise RuntimeError(f"bytes_done mismatch lane={pair.lane_index}: {pair.final.bytes_done} != {nbytes}")
        wall_s = time.perf_counter() - submitted_at
        finals = [pair.final for pair in pairs if pair.final is not None]
        daemon_start_ns = min(event.started_at_ns for event in finals)
        daemon_end_ns = max(event.completed_at_ns for event in finals)
        daemon_wall_s = max((daemon_end_ns - daemon_start_ns) / 1_000_000_000.0, 1e-12)
        cpu_us = _sum(finals, "channel_cpu_copy_us")
        acl_submit_us = _sum(finals, "channel_acl_submit_us")
        acl_wait_us = _sum(finals, "channel_acl_wait_us")
        event_record_count = sum(int(event.channel_event_record_count) for event in finals)
        event_wait_count = sum(int(event.channel_event_wait_count) for event in finals)
        stream_create_count = sum(int(event.channel_stream_create_count) for event in finals)
        chunk_count = sum(int(event.channel_chunks_transferred) for event in finals)
        overlap_ratio = sum(float(event.channel_overlap_ratio) for event in finals) / max(len(finals), 1)
        stats = client.stats()
        print(
            "UFLOW_A08_H2D_BW_RESULT "
            f"size={_format_size(nbytes)} bytes={nbytes} lanes={lanes} mode={args.mode} "
            f"daemon_bandwidth_gib_s={_bandwidth_gib(total_bytes, daemon_wall_s):.3f} "
            f"client_effective_bandwidth_gib_s={_bandwidth_gib(total_bytes, wall_s):.3f} "
            f"daemon_wall_ms={daemon_wall_s * 1000.0:.3f} client_wall_ms={wall_s * 1000.0:.3f} "
            f"cpu_copy_us={cpu_us:.3f} acl_submit_us={acl_submit_us:.3f} acl_wait_us={acl_wait_us:.3f} "
            f"overlap_ratio={overlap_ratio:.6f} pipeline_overlap={all(e.channel_pipeline_overlap for e in finals)} "
            f"stream_create_count={stream_create_count} event_record_count={event_record_count} "
            f"event_wait_count={event_wait_count} chunks={chunk_count} "
            f"h2d_lanes={stats.get('h2d_lane_count')} h2d_busy={stats.get('h2d_busy_lanes')} "
            f"pinned_total={stats.get('pinned_total_bytes')} lane_wait_count={stats.get('lane_wait_count')}",
            flush=True,
        )
    finally:
        _release_pairs(pairs)


def run(args: argparse.Namespace) -> None:
    sizes = _parse_size_list(args.sizes)
    lanes_list = [int(item) for item in args.lanes.split(",") if item.strip()]
    client = _client(args)
    try:
        for nbytes in sizes:
            for lanes in lanes_list:
                _run_case(client, args, nbytes=nbytes, lanes=lanes)
    finally:
        client.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PhaseA-08 H2D bandwidth saturation benchmark.")
    parser.add_argument("--socket", default=os.environ.get("UF_SOCKET", "/tmp/uflow_h2d_bw.sock"))
    parser.add_argument("--device", type=int, default=int(os.environ.get("UF_A08_DEVICE", "7")))
    parser.add_argument("--sizes", default=os.environ.get("UF_H2D_BW_SIZES", "256MiB,512MiB,1GiB"))
    parser.add_argument("--lanes", default=os.environ.get("UF_H2D_BW_LANES", "1,2"))
    parser.add_argument("--mode", default=os.environ.get("UF_H2D_BW_MODE", "auto"))
    parser.add_argument("--ddr-target", default=os.environ.get("UF_DDR_TARGET", "host:0"))
    parser.add_argument("--timeout-ms", type=int, default=int(os.environ.get("UF_H2D_BW_TIMEOUT_MS", "300000")))
    parser.add_argument("--skip-fill", action="store_true", default=os.environ.get("UF_H2D_BW_SKIP_FILL", "0") in {"1", "true", "yes"})
    return parser.parse_args()


def main() -> None:
    run(parse_args())


if __name__ == "__main__":
    main()
