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
class TransferSpec:
    name: str
    direction: str
    src: ManagedBuffer | DdrBuffer
    dst: ManagedBuffer | DdrBuffer
    nbytes: int
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


def _format_size(nbytes: int) -> str:
    if nbytes % GIB == 0:
        return f"{nbytes // GIB}GiB"
    if nbytes % MIB == 0:
        return f"{nbytes // MIB}MiB"
    return str(nbytes)


def _bandwidth_gib(total_bytes: int, seconds: float) -> float:
    return total_bytes / max(seconds, 1e-12) / GIB


def _client(args: argparse.Namespace) -> UFlowClient:
    return UFlowClient(
        enabled=True,
        socket_path=args.socket,
        device_id=args.device,
        acl_lib_path=_acl_lib(),
        client_role="phasee05-hotpath",
        model_id="phasee05-hotpath",
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


def _setup_h2d_specs(client: UFlowClient, args: argparse.Namespace, *, nbytes: int, lanes: int) -> tuple[list[TransferSpec], list[ManagedBuffer | DdrBuffer]]:
    specs: list[TransferSpec] = []
    owned: list[ManagedBuffer | DdrBuffer] = []
    for lane in range(lanes):
        src = client.allocate_ddr(
            name=f"phasee05.h2d.size{nbytes}.lane{lane}.src",
            role="weight",
            nbytes=nbytes,
            target=args.ddr_target,
            immutable=True,
            mark_ready=False,
        )
        owned.append(src)
        if not args.skip_fill:
            _fill_ddr(src, nbytes, seed=lane * 17)
        client.mark_ready(src)
        dst = client.allocate(
            name=f"phasee05.h2d.size{nbytes}.lane{lane}.hbm",
            role="weight",
            nbytes=nbytes,
            hint="mandatory:hbm",
            target=f"npu:{args.device}",
            immutable=False,
            mark_ready=False,
        )
        owned.append(dst)
        specs.append(TransferSpec(name=f"h2d-{lane}", direction="h2d", src=src, dst=dst, nbytes=nbytes))
    return specs, owned


def _setup_d2h_specs(client: UFlowClient, args: argparse.Namespace, *, nbytes: int, lanes: int) -> tuple[list[TransferSpec], list[ManagedBuffer | DdrBuffer]]:
    specs: list[TransferSpec] = []
    owned: list[ManagedBuffer | DdrBuffer] = []
    for lane in range(lanes):
        seed = lane * 23
        preload = client.allocate_ddr(
            name=f"phasee05.d2h.size{nbytes}.lane{lane}.preload",
            role="weight",
            nbytes=nbytes,
            target=args.ddr_target,
            immutable=True,
            mark_ready=False,
        )
        owned.append(preload)
        if not args.skip_fill:
            _fill_ddr(preload, nbytes, seed=seed)
        client.mark_ready(preload)
        src = client.allocate(
            name=f"phasee05.d2h.size{nbytes}.lane{lane}.hbm",
            role="weight",
            nbytes=nbytes,
            hint="mandatory:hbm",
            target=f"npu:{args.device}",
            immutable=False,
            mark_ready=False,
        )
        owned.append(src)
        setup_plan = client.plan_transfer(src=preload, dst=src, nbytes=nbytes, mode=args.mode)
        setup_event = client.submit_transfer(setup_plan)
        setup_final = client.wait_event(setup_event, timeout_ms=args.timeout_ms)
        if setup_final.status != "complete":
            raise RuntimeError(f"D2H preload failed lane={lane}: {setup_final}")
        dst = client.allocate_ddr(
            name=f"phasee05.d2h.size{nbytes}.lane{lane}.dst",
            role="weight",
            nbytes=nbytes,
            target=args.ddr_target,
            immutable=False,
            mark_ready=False,
        )
        owned.append(dst)
        specs.append(TransferSpec(name=f"d2h-{lane}", direction="d2h", src=src, dst=dst, nbytes=nbytes))
    return specs, owned


def _sum(events: list[TransferEvent], attr: str) -> float:
    return float(sum(float(getattr(event, attr)) for event in events))


def _run_specs(client: UFlowClient, args: argparse.Namespace, *, label: str, specs: list[TransferSpec]) -> None:
    for spec in specs:
        spec.plan = client.plan_transfer(src=spec.src, dst=spec.dst, nbytes=spec.nbytes, mode=args.mode)

    barrier = threading.Barrier(len(specs) + 1)
    errors: list[BaseException] = []

    def submit(spec: TransferSpec) -> None:
        try:
            assert spec.plan is not None
            barrier.wait()
            spec.event = client.submit_transfer(spec.plan)
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=submit, args=(spec,), daemon=True) for spec in specs]
    for thread in threads:
        thread.start()
    submitted_at = time.perf_counter()
    barrier.wait()
    for thread in threads:
        thread.join(timeout=20.0)
        if thread.is_alive():
            raise TimeoutError("timed out waiting for submit thread")
    if errors:
        raise errors[0]

    for spec in specs:
        assert spec.event is not None
        spec.final = client.wait_event(spec.event, timeout_ms=args.timeout_ms)
        if spec.final.status != "complete":
            raise RuntimeError(f"transfer failed {spec.name}: {spec.final}")
        if spec.final.bytes_done != spec.nbytes:
            raise RuntimeError(f"bytes_done mismatch {spec.name}: {spec.final.bytes_done} != {spec.nbytes}")

    wall_s = time.perf_counter() - submitted_at
    finals = [spec.final for spec in specs if spec.final is not None]
    actual_engines = "+".join(sorted(set(str(event.actual_engine) for event in finals)))
    actual_paths = "+".join(sorted(set(str(event.actual_path) for event in finals)))
    daemon_start_ns = min(event.started_at_ns for event in finals)
    daemon_end_ns = max(event.completed_at_ns for event in finals)
    daemon_wall_s = max((daemon_end_ns - daemon_start_ns) / 1_000_000_000.0, 1e-12)
    total_bytes = sum(spec.nbytes for spec in specs)
    channel_wall_us = _sum(finals, "channel_wall_us")
    channel_wall_s = max(channel_wall_us / 1_000_000.0, 1e-12)
    stats = client.stats()
    directions = ",".join(sorted(set(spec.direction for spec in specs)))
    print(
        "UFLOW_E05_HOTPATH_RESULT "
        f"label={label} directions={directions} transfers={len(specs)} "
        f"bytes_per_transfer={specs[0].nbytes if specs else 0} total_bytes={total_bytes} mode={args.mode} "
        f"actual_engines={actual_engines} actual_paths={actual_paths} "
        f"daemon_bandwidth_gib_s={_bandwidth_gib(total_bytes, daemon_wall_s):.3f} "
        f"channel_bandwidth_gib_s={_bandwidth_gib(total_bytes, channel_wall_s):.3f} "
        f"client_effective_bandwidth_gib_s={_bandwidth_gib(total_bytes, wall_s):.3f} "
        f"daemon_wall_ms={daemon_wall_s * 1000.0:.3f} channel_wall_ms={channel_wall_s * 1000.0:.3f} client_wall_ms={wall_s * 1000.0:.3f} "
        f"cpu_copy_us={_sum(finals, 'channel_cpu_copy_us'):.3f} "
        f"acl_submit_us={_sum(finals, 'channel_acl_submit_us'):.3f} "
        f"acl_wait_us={_sum(finals, 'channel_acl_wait_us'):.3f} "
        f"event_record_count={sum(int(event.channel_event_record_count) for event in finals)} "
        f"event_wait_count={sum(int(event.channel_event_wait_count) for event in finals)} "
        f"chunks={sum(int(event.channel_chunks_transferred) for event in finals)} "
        f"pipeline_overlap={all(event.channel_pipeline_overlap for event in finals)} "
        f"h2d_lanes={stats.get('h2d_lane_count')} d2h_lanes={stats.get('d2h_lane_count')} "
        f"h2d_busy={stats.get('h2d_busy_lanes')} d2h_busy={stats.get('d2h_busy_lanes')} "
        f"pinned_total={stats.get('pinned_total_bytes')} pinned_idle={stats.get('pinned_idle_bytes')} "
        f"lane_wait_count={stats.get('lane_wait_count')} reaped_lanes={stats.get('pinned_idle_reaped_lanes')}",
        flush=True,
    )


def _run_case(client: UFlowClient, args: argparse.Namespace, *, direction: str, nbytes: int, lanes: int) -> None:
    owned: list[ManagedBuffer | DdrBuffer] = []
    try:
        if direction == "h2d":
            specs, case_owned = _setup_h2d_specs(client, args, nbytes=nbytes, lanes=lanes)
            owned.extend(case_owned)
        elif direction == "d2h":
            specs, case_owned = _setup_d2h_specs(client, args, nbytes=nbytes, lanes=lanes)
            owned.extend(case_owned)
        elif direction == "bidir":
            h2d_specs, h2d_owned = _setup_h2d_specs(client, args, nbytes=nbytes, lanes=lanes)
            d2h_specs, d2h_owned = _setup_d2h_specs(client, args, nbytes=nbytes, lanes=lanes)
            specs = h2d_specs + d2h_specs
            owned.extend(h2d_owned)
            owned.extend(d2h_owned)
        else:
            raise ValueError(f"unsupported direction: {direction}")
        _run_specs(client, args, label=f"{direction}_{_format_size(nbytes)}_{lanes}lanes", specs=specs)
    finally:
        _release_all(owned)


def run(args: argparse.Namespace) -> None:
    sizes = _parse_size_list(args.sizes)
    lanes_list = [int(item) for item in args.lanes.split(",") if item.strip()]
    directions = [item.strip().lower() for item in args.directions.split(",") if item.strip()]
    client = _client(args)
    try:
        for nbytes in sizes:
            for direction in directions:
                for lanes in lanes_list:
                    _run_case(client, args, direction=direction, nbytes=nbytes, lanes=lanes)
    finally:
        client.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PhaseE-05 UFlow daemon hot path saturation benchmark.")
    parser.add_argument("--socket", default=os.environ.get("UF_SOCKET", "/tmp/uflow_e05_hotpath.sock"))
    parser.add_argument("--device", type=int, default=int(os.environ.get("UF_E05_DEVICE", "7")))
    parser.add_argument("--sizes", default=os.environ.get("UF_E05_SIZES", "1GiB"))
    parser.add_argument("--lanes", default=os.environ.get("UF_E05_LANES", "1,2,4,6,8"))
    parser.add_argument("--directions", default=os.environ.get("UF_E05_DIRECTIONS", "h2d,d2h,bidir"))
    parser.add_argument("--mode", default=os.environ.get("UF_E05_MODE", "auto"))
    parser.add_argument("--ddr-target", default=os.environ.get("UF_DDR_TARGET", "host:0"))
    parser.add_argument("--timeout-ms", type=int, default=int(os.environ.get("UF_E05_TIMEOUT_MS", "300000")))
    parser.add_argument("--skip-fill", action="store_true", default=os.environ.get("UF_E05_SKIP_FILL", "0") in {"1", "true", "yes"})
    return parser.parse_args()


def main() -> None:
    run(parse_args())


if __name__ == "__main__":
    main()
