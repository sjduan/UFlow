from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SDK_ROOT = REPO_ROOT / "sdk" / "python"
if str(SDK_ROOT) not in sys.path:
    sys.path.insert(0, str(SDK_ROOT))

from uflow import DdrBuffer, ManagedBuffer, TransferEvent, UFlowClient  # noqa: E402


MIB = 1024 * 1024
GIB = 1024 * 1024 * 1024


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


def _parse_sizes(value: str) -> list[int]:
    return [_parse_size(item) for item in value.split(",") if item.strip()]


def _format_size(nbytes: int) -> str:
    if nbytes % GIB == 0:
        return f"{nbytes // GIB}GiB"
    if nbytes % MIB == 0:
        return f"{nbytes // MIB}MiB"
    return str(nbytes)


def _bandwidth_gib(nbytes: int, us: float) -> float:
    return nbytes / max(us / 1_000_000.0, 1e-12) / GIB


def _client(args: argparse.Namespace) -> UFlowClient:
    return UFlowClient(
        enabled=True,
        socket_path=args.socket,
        device_id=args.device,
        acl_lib_path=_acl_lib(),
        client_role="phasee08-low-overhead",
        model_id="phasee08-low-overhead",
    )


def _fill_ddr(ddr: DdrBuffer, nbytes: int, *, seed: int) -> None:
    view = ddr.as_memoryview()
    block_len = min(8 * MIB, nbytes)
    block = bytes(((idx + seed) % 251 for idx in range(block_len)))
    try:
        for offset in range(0, nbytes, block_len):
            chunk = min(block_len, nbytes - offset)
            view[offset : offset + chunk] = block[:chunk]
    finally:
        del view


def _release_all(items: list[ManagedBuffer | DdrBuffer]) -> None:
    for item in reversed(items):
        try:
            item.release()
        except Exception:
            pass


def _event_row(*, label: str, direction: str, nbytes: int, iteration: int, event: TransferEvent, client_wall_us: float) -> dict[str, object]:
    return {
        "label": label,
        "direction": direction,
        "size": _format_size(nbytes),
        "bytes": nbytes,
        "iteration": iteration,
        "status": event.status,
        "actual_engine": event.actual_engine,
        "actual_path": event.actual_path,
        "event_latency_us": f"{event.actual_latency_us:.3f}",
        "event_bandwidth_gib_s": f"{event.actual_bandwidth_gib_s:.3f}",
        "channel_wall_us": f"{event.channel_wall_us:.3f}",
        "channel_bandwidth_gib_s": f"{_bandwidth_gib(nbytes, event.channel_wall_us):.3f}",
        "client_wall_us": f"{client_wall_us:.3f}",
        "queue_wait_us": f"{event.channel_queue_wait_us:.3f}",
        "worker_execute_us": f"{event.channel_worker_execute_us:.3f}",
        "lane_wait_us": f"{event.channel_lane_wait_us:.3f}",
        "acl_submit_us": f"{event.channel_acl_submit_us:.3f}",
        "acl_wait_us": f"{event.channel_acl_wait_us:.3f}",
        "stream_create_count": event.channel_stream_create_count,
        "event_reuse_count": event.channel_event_reuse_count,
        "event_record_count": event.channel_event_record_count,
        "event_wait_count": event.channel_event_wait_count,
        "lane_id": event.channel_lane_id,
        "device_id": event.channel_device_id,
    }


def _run_h2d(client: UFlowClient, args: argparse.Namespace, *, nbytes: int) -> list[dict[str, object]]:
    owned: list[ManagedBuffer | DdrBuffer] = []
    try:
        src = client.allocate_ddr(
            name=f"phasee08.h2d.{nbytes}.src",
            role="user",
            nbytes=nbytes,
            target=args.ddr_target,
            immutable=True,
            mark_ready=False,
        )
        owned.append(src)
        if not args.skip_fill:
            _fill_ddr(src, nbytes, seed=17)
        client.mark_ready(src)
        dst = client.allocate(
            name=f"phasee08.h2d.{nbytes}.dst",
            role="user",
            nbytes=nbytes,
            target=f"npu:{args.device}",
            immutable=False,
            mark_ready=False,
        )
        owned.append(dst)
        plan = client.plan_transfer(src=src, dst=dst, nbytes=nbytes, mode=args.mode)
        rows: list[dict[str, object]] = []
        for iteration in range(args.repeats):
            started = time.perf_counter()
            event = client.submit_transfer(plan)
            final = client.wait_event(event, timeout_ms=args.timeout_ms)
            client_wall_us = (time.perf_counter() - started) * 1_000_000.0
            if final.status != "complete":
                raise RuntimeError(f"H2D transfer failed size={nbytes} iteration={iteration}: {final}")
            rows.append(_event_row(label=f"h2d_{_format_size(nbytes)}", direction="h2d", nbytes=nbytes, iteration=iteration, event=final, client_wall_us=client_wall_us))
        return rows
    finally:
        _release_all(owned)


def _run_d2h(client: UFlowClient, args: argparse.Namespace, *, nbytes: int) -> list[dict[str, object]]:
    owned: list[ManagedBuffer | DdrBuffer] = []
    try:
        preload = client.allocate_ddr(
            name=f"phasee08.d2h.{nbytes}.preload",
            role="user",
            nbytes=nbytes,
            target=args.ddr_target,
            immutable=True,
            mark_ready=False,
        )
        owned.append(preload)
        if not args.skip_fill:
            _fill_ddr(preload, nbytes, seed=29)
        client.mark_ready(preload)
        src = client.allocate(
            name=f"phasee08.d2h.{nbytes}.src",
            role="user",
            nbytes=nbytes,
            target=f"npu:{args.device}",
            immutable=False,
            mark_ready=False,
        )
        owned.append(src)
        setup = client.transfer_sync(src=preload, dst=src, nbytes=nbytes, mode=args.mode)
        if setup.status != "complete":
            raise RuntimeError(f"D2H preload failed size={nbytes}: {setup}")
        dst = client.allocate_ddr(
            name=f"phasee08.d2h.{nbytes}.dst",
            role="user",
            nbytes=nbytes,
            target=args.ddr_target,
            immutable=False,
            mark_ready=False,
        )
        owned.append(dst)
        plan = client.plan_transfer(src=src, dst=dst, nbytes=nbytes, mode=args.mode)
        rows: list[dict[str, object]] = []
        for iteration in range(args.repeats):
            started = time.perf_counter()
            event = client.submit_transfer(plan)
            final = client.wait_event(event, timeout_ms=args.timeout_ms)
            client_wall_us = (time.perf_counter() - started) * 1_000_000.0
            if final.status != "complete":
                raise RuntimeError(f"D2H transfer failed size={nbytes} iteration={iteration}: {final}")
            rows.append(_event_row(label=f"d2h_{_format_size(nbytes)}", direction="d2h", nbytes=nbytes, iteration=iteration, event=final, client_wall_us=client_wall_us))
        return rows
    finally:
        _release_all(owned)


def _write_outputs(args: argparse.Namespace, rows: list[dict[str, object]], stats: dict[str, str]) -> None:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "direct_executor_summary.csv"
    if rows:
        with csv_path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
    summary = output_dir / "summary.md"
    with summary.open("w") as f:
        f.write("# PhaseE-08 Low-Overhead Direct Executor\n\n")
        f.write(f"device: {args.device}\n\n")
        f.write(f"mode: {args.mode}\n\n")
        f.write(f"repeats: {args.repeats}\n\n")
        f.write("## Direct executor stats\n\n")
        for key in sorted(stats):
            if key.startswith("direct_"):
                f.write(f"- {key}: {stats[key]}\n")
        f.write("\n## Result rows\n\n")
        for row in rows:
            f.write(
                f"- {row['direction']} {row['size']} iter={row['iteration']} "
                f"channel={row['channel_bandwidth_gib_s']}GiB/s event={row['event_bandwidth_gib_s']}GiB/s "
                f"stream_create={row['stream_create_count']} event_reuse={row['event_reuse_count']} "
                f"queue_wait_us={row['queue_wait_us']}\n"
            )
    print(f"UFLOW_E08_OUTPUT csv={csv_path} summary={summary}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PhaseE-08 low-overhead direct executor benchmark.")
    parser.add_argument("--socket", default=os.environ.get("UF_SOCKET", "/tmp/uflow_phasee08.sock"))
    parser.add_argument("--device", type=int, default=int(os.environ.get("UF_TARGET_DEVICE", "7")))
    parser.add_argument("--sizes", default=os.environ.get("UF_E08_SIZES", "64MiB,256MiB,1GiB,2GiB"))
    parser.add_argument("--directions", default=os.environ.get("UF_E08_DIRECTIONS", "h2d,d2h"))
    parser.add_argument("--repeats", type=int, default=int(os.environ.get("UF_E08_REPEATS", "3")))
    parser.add_argument("--mode", default=os.environ.get("UF_E08_MODE", "auto"))
    parser.add_argument("--ddr-target", default=os.environ.get("UF_DDR_TARGET", "host:0"))
    parser.add_argument("--timeout-ms", type=int, default=int(os.environ.get("UF_E08_TIMEOUT_MS", "300000")))
    parser.add_argument("--output-dir", default=os.environ.get("UF_E08_OUTPUT_DIR", "/tmp/proj_output/phasee08_low_overhead_executor"))
    parser.add_argument("--skip-fill", action="store_true", default=os.environ.get("UF_E08_SKIP_FILL", "0").lower() in {"1", "true", "yes", "on"})
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows: list[dict[str, object]] = []
    sizes = _parse_sizes(args.sizes)
    directions = {item.strip().lower() for item in args.directions.split(",") if item.strip()}
    client = _client(args)
    try:
        for nbytes in sizes:
            if "h2d" in directions:
                rows.extend(_run_h2d(client, args, nbytes=nbytes))
            if "d2h" in directions:
                rows.extend(_run_d2h(client, args, nbytes=nbytes))
        stats = client.stats()
        _write_outputs(args, rows, stats)
        print(
            "UFLOW_E08_LOW_OVERHEAD_PASS "
            f"sizes={args.sizes} directions={args.directions} repeats={args.repeats} mode={args.mode}",
            flush=True,
        )
    finally:
        client.close()


if __name__ == "__main__":
    main()
