from __future__ import annotations

import argparse
import ctypes
import csv
import json
import mmap
import os
import statistics
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
SDK_ROOT = REPO_ROOT / "sdk" / "python"
if str(SDK_ROOT) not in sys.path:
    sys.path.insert(0, str(SDK_ROOT))

from uflow.client import _AclClient, _UfAclHbmBlock, _UfAclHostMemory  # noqa: E402


PROBE_BYTES = 4096


def now_us() -> float:
    return time.perf_counter_ns() / 1000.0


def gib_s(nbytes: int, elapsed_us: float) -> float:
    if elapsed_us <= 0:
        return 0.0
    return (float(nbytes) / (1024.0**3)) / (elapsed_us / 1_000_000.0)


def parse_sizes(text: str) -> list[int]:
    out: list[int] = []
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        suffix = part[-1].lower()
        if suffix in {"k", "m", "g"}:
            scale = {"k": 1024, "m": 1024**2, "g": 1024**3}[suffix]
            out.append(int(float(part[:-1]) * scale))
        else:
            out.append(int(float(part)))
    return out


def pattern_bytes(nbytes: int, *, offset: int = 0) -> bytes:
    base = bytes(((idx + offset) % 251 for idx in range(251)))
    return (base * ((int(nbytes) + len(base) - 1) // len(base)))[: int(nbytes)]


def pattern_slice(start: int, nbytes: int, *, offset: int = 0) -> bytes:
    return bytes((((start + idx) + offset) % 251 for idx in range(int(nbytes))))


def ptr_from_buffer(buf: Any) -> int:
    return ctypes.addressof(ctypes.c_char.from_buffer(buf))


def memcopy(dst: int, src: int, nbytes: int) -> None:
    ctypes.memmove(ctypes.c_void_p(int(dst)), ctypes.c_void_p(int(src)), int(nbytes))


def memwrite(ptr: int, payload: bytes, *, offset: int = 0) -> None:
    ctypes.memmove(ctypes.c_void_p(int(ptr) + int(offset)), payload, len(payload))


def memread(ptr: int, nbytes: int, *, offset: int = 0) -> bytes:
    return ctypes.string_at(int(ptr) + int(offset), int(nbytes))


def memzero(ptr: int, nbytes: int) -> None:
    ctypes.memset(ctypes.c_void_p(int(ptr)), 0, int(nbytes))


def acl_lib_path() -> str:
    return os.environ.get("UF_ACL_LIB", "/home/sj/git/data-service/build/lib/libuf_acl_shim.so")


@dataclass
class MemfdRegion:
    nbytes: int
    fd: int
    mapped: mmap.mmap
    ptr: int

    @classmethod
    def create(cls, nbytes: int, name: str) -> "MemfdRegion":
        if not hasattr(os, "memfd_create"):
            raise RuntimeError("os.memfd_create is not available")
        fd = os.memfd_create(name, flags=0)
        os.ftruncate(fd, nbytes)
        mapped = mmap.mmap(fd, nbytes)
        return cls(nbytes=nbytes, fd=fd, mapped=mapped, ptr=ptr_from_buffer(mapped))

    def close(self) -> None:
        try:
            self.mapped.close()
        finally:
            os.close(self.fd)


@dataclass
class HbmRegion:
    acl: _AclClient
    block: _UfAclHbmBlock

    @property
    def device_ptr(self) -> int:
        return int(self.block.service_device_ptr)

    def close(self) -> None:
        if self.block.raw_handle_id:
            self.acl.free_physical(self.block)
            self.block.raw_handle_id = 0


@dataclass
class PinnedPool:
    acl: _AclClient
    chunks: list[_UfAclHostMemory]

    @classmethod
    def create(cls, acl: _AclClient, chunk_bytes: int, chunk_count: int) -> "PinnedPool":
        return cls(acl=acl, chunks=[acl.malloc_host(chunk_bytes) for _ in range(chunk_count)])

    def ptr(self, idx: int) -> int:
        return int(self.chunks[idx].host_ptr)

    def close(self) -> None:
        for chunk in self.chunks:
            self.acl.free_host(chunk)


class JsonlWriter:
    def __init__(self, run_dir: Path) -> None:
        self.rows: list[dict[str, Any]] = []
        self.fp = open(run_dir / "chunk_results.jsonl", "w", encoding="utf-8")

    def add(self, row: dict[str, Any]) -> None:
        row.setdefault("ts_ns", time.time_ns())
        self.rows.append(row)
        self.fp.write(json.dumps(row, sort_keys=True) + "\n")
        self.fp.flush()

    def close(self) -> None:
        self.fp.close()


def fill_region_by_chunk(region: MemfdRegion, chunk_bytes: int, *, seed: int) -> None:
    pos = 0
    while pos < region.nbytes:
        nbytes = min(chunk_bytes, region.nbytes - pos)
        payload = pattern_bytes(nbytes, offset=(seed + pos // max(1, chunk_bytes)) % 251)
        memwrite(region.ptr, payload, offset=pos)
        pos += nbytes


def verify_region_by_chunk(region: MemfdRegion, chunk_bytes: int, *, seed: int) -> None:
    pos = 0
    while pos < region.nbytes:
        nbytes = min(chunk_bytes, region.nbytes - pos)
        expected_head = pattern_bytes(min(PROBE_BYTES, nbytes), offset=(seed + pos // max(1, chunk_bytes)) % 251)
        actual_head = memread(region.ptr, len(expected_head), offset=pos)
        if actual_head != expected_head:
            raise AssertionError(f"verify failed at chunk_offset={pos}")
        if nbytes > PROBE_BYTES * 2:
            tail_n = min(PROBE_BYTES, nbytes)
            expected_tail = pattern_slice(nbytes - tail_n, tail_n, offset=(seed + pos // max(1, chunk_bytes)) % 251)
            actual_tail = memread(region.ptr, tail_n, offset=pos + nbytes - tail_n)
            if actual_tail != expected_tail:
                raise AssertionError(f"tail verify failed at chunk_offset={pos}")
        pos += nbytes


def verify_hbm_by_chunk(acl: _AclClient, hbm: HbmRegion, object_bytes: int, chunk_bytes: int, *, seed: int) -> None:
    sample = MemfdRegion.create(PROBE_BYTES, f"uflow_e03_chunk_hbm_sample_{os.getpid()}")
    try:
        pos = 0
        while pos < object_bytes:
            nbytes = min(chunk_bytes, object_bytes - pos)
            sample_n = min(PROBE_BYTES, nbytes)
            acl.d2h(sample.ptr, hbm.device_ptr, sample_n, offset=pos)
            expected_head = pattern_bytes(sample_n, offset=(seed + pos // max(1, chunk_bytes)) % 251)
            actual_head = memread(sample.ptr, sample_n)
            if actual_head != expected_head:
                raise AssertionError(f"HBM head verify failed at chunk_offset={pos}")
            if nbytes > PROBE_BYTES * 2:
                tail_n = min(PROBE_BYTES, nbytes)
                acl.d2h(sample.ptr, hbm.device_ptr, tail_n, offset=pos + nbytes - tail_n)
                expected_tail = pattern_slice(nbytes - tail_n, tail_n, offset=(seed + pos // max(1, chunk_bytes)) % 251)
                actual_tail = memread(sample.ptr, tail_n)
                if actual_tail != expected_tail:
                    raise AssertionError(f"HBM tail verify failed at chunk_offset={pos}")
            pos += nbytes
    finally:
        sample.close()


def init_hbm_from_memfd_direct(acl: _AclClient, hbm: HbmRegion, src: MemfdRegion, nbytes: int) -> None:
    acl.h2d(hbm.device_ptr, src.ptr, nbytes)


def h2d_full_size(acl: _AclClient, hbm: HbmRegion, src: MemfdRegion, pool: PinnedPool, nbytes: int) -> tuple[float, dict[str, float]]:
    stream = acl.create_stream()
    event = acl.create_event()
    t0 = now_us()
    copy0 = now_us()
    memcopy(pool.ptr(0), src.ptr, nbytes)
    copy_us = now_us() - copy0
    submit0 = now_us()
    acl.h2d_async(hbm.device_ptr, pool.ptr(0), nbytes, stream_id=stream, event_id=event)
    submit_us = now_us() - submit0
    acl.synchronize_event(event)
    acl.synchronize_stream(stream)
    total = now_us() - t0
    acl.destroy_event(event)
    acl.destroy_stream(stream)
    return total, {"cpu_copy_us": copy_us, "acl_submit_us": submit_us}


def d2h_full_size(acl: _AclClient, hbm: HbmRegion, dst: MemfdRegion, pool: PinnedPool, nbytes: int) -> tuple[float, dict[str, float]]:
    stream = acl.create_stream()
    event = acl.create_event()
    t0 = now_us()
    submit0 = now_us()
    acl.d2h_async(pool.ptr(0), hbm.device_ptr, nbytes, stream_id=stream, event_id=event)
    submit_us = now_us() - submit0
    acl.synchronize_event(event)
    acl.synchronize_stream(stream)
    copy0 = now_us()
    memcopy(dst.ptr, pool.ptr(0), nbytes)
    copy_us = now_us() - copy0
    total = now_us() - t0
    acl.destroy_event(event)
    acl.destroy_stream(stream)
    return total, {"cpu_copy_us": copy_us, "acl_submit_us": submit_us}


def h2d_chunked(
    acl: _AclClient,
    hbm: HbmRegion,
    src: MemfdRegion,
    pool: PinnedPool,
    object_bytes: int,
    chunk_bytes: int,
) -> tuple[float, dict[str, float]]:
    streams = [acl.create_stream() for _ in pool.chunks]
    events = [acl.create_event() for _ in pool.chunks]
    pending = [False for _ in pool.chunks]
    wait_us = 0.0
    cpu_copy_us = 0.0
    submit_us = 0.0
    t0 = now_us()
    try:
        chunk_idx = 0
        pos = 0
        while pos < object_bytes:
            nbytes = min(chunk_bytes, object_bytes - pos)
            slot = chunk_idx % len(pool.chunks)
            if pending[slot]:
                w0 = now_us()
                acl.synchronize_event(events[slot])
                acl.synchronize_stream(streams[slot])
                wait_us += now_us() - w0
                pending[slot] = False
            c0 = now_us()
            memcopy(pool.ptr(slot), src.ptr + pos, nbytes)
            cpu_copy_us += now_us() - c0
            s0 = now_us()
            acl.h2d_async(hbm.device_ptr, pool.ptr(slot), nbytes, offset=pos, stream_id=streams[slot], event_id=events[slot])
            submit_us += now_us() - s0
            pending[slot] = True
            pos += nbytes
            chunk_idx += 1
        for slot, is_pending in enumerate(pending):
            if is_pending:
                w0 = now_us()
                acl.synchronize_event(events[slot])
                acl.synchronize_stream(streams[slot])
                wait_us += now_us() - w0
        total = now_us() - t0
        return total, {"cpu_copy_us": cpu_copy_us, "acl_submit_us": submit_us, "wait_us": wait_us}
    finally:
        for event in events:
            acl.destroy_event(event)
        for stream in streams:
            acl.destroy_stream(stream)


def d2h_chunked(
    acl: _AclClient,
    hbm: HbmRegion,
    dst: MemfdRegion,
    pool: PinnedPool,
    object_bytes: int,
    chunk_bytes: int,
) -> tuple[float, dict[str, float]]:
    streams = [acl.create_stream() for _ in pool.chunks]
    events = [acl.create_event() for _ in pool.chunks]
    pending: list[tuple[bool, int, int]] = [(False, 0, 0) for _ in pool.chunks]
    wait_us = 0.0
    cpu_copy_us = 0.0
    submit_us = 0.0
    t0 = now_us()
    try:
        chunk_idx = 0
        pos = 0
        while pos < object_bytes:
            nbytes = min(chunk_bytes, object_bytes - pos)
            slot = chunk_idx % len(pool.chunks)
            was_pending, old_pos, old_nbytes = pending[slot]
            if was_pending:
                w0 = now_us()
                acl.synchronize_event(events[slot])
                acl.synchronize_stream(streams[slot])
                wait_us += now_us() - w0
                c0 = now_us()
                memcopy(dst.ptr + old_pos, pool.ptr(slot), old_nbytes)
                cpu_copy_us += now_us() - c0
            s0 = now_us()
            acl.d2h_async(pool.ptr(slot), hbm.device_ptr, nbytes, offset=pos, stream_id=streams[slot], event_id=events[slot])
            submit_us += now_us() - s0
            pending[slot] = (True, pos, nbytes)
            pos += nbytes
            chunk_idx += 1
        for slot, (is_pending, old_pos, old_nbytes) in enumerate(pending):
            if is_pending:
                w0 = now_us()
                acl.synchronize_event(events[slot])
                acl.synchronize_stream(streams[slot])
                wait_us += now_us() - w0
                c0 = now_us()
                memcopy(dst.ptr + old_pos, pool.ptr(slot), old_nbytes)
                cpu_copy_us += now_us() - c0
        total = now_us() - t0
        return total, {"cpu_copy_us": cpu_copy_us, "acl_submit_us": submit_us, "wait_us": wait_us}
    finally:
        for event in events:
            acl.destroy_event(event)
        for stream in streams:
            acl.destroy_stream(stream)


def run_one(
    *,
    acl: _AclClient,
    writer: JsonlWriter,
    device: int,
    object_bytes: int,
    chunk_bytes: int,
    chunk_count: int,
    mode: str,
    op: str,
    repeat_idx: int,
    warmup: bool,
    run_dir: Path,
) -> None:
    src = dst = None
    hbm = None
    pool = None
    try:
        src = MemfdRegion.create(object_bytes, f"uflow_e03_chunk_src_{os.getpid()}")
        dst = MemfdRegion.create(object_bytes, f"uflow_e03_chunk_dst_{os.getpid()}")
        fill_region_by_chunk(src, min(chunk_bytes, object_bytes), seed=37)
        memzero(dst.ptr, object_bytes)
        hbm = HbmRegion(acl=acl, block=acl.alloc_physical(object_bytes))
        effective_chunk = object_bytes if mode == "full_size" else min(chunk_bytes, object_bytes)
        effective_count = 1 if mode == "full_size" else chunk_count
        pool = PinnedPool.create(acl, effective_chunk, effective_count)
        if op == "h2d":
            acl.h2d(hbm.device_ptr, dst.ptr, object_bytes)
            if mode == "full_size":
                elapsed, extra = h2d_full_size(acl, hbm, src, pool, object_bytes)
            else:
                elapsed, extra = h2d_chunked(acl, hbm, src, pool, object_bytes, effective_chunk)
            verify_hbm_by_chunk(acl, hbm, object_bytes, min(chunk_bytes, object_bytes), seed=37)
        elif op == "d2h":
            init_hbm_from_memfd_direct(acl, hbm, src, object_bytes)
            if mode == "full_size":
                elapsed, extra = d2h_full_size(acl, hbm, dst, pool, object_bytes)
            else:
                elapsed, extra = d2h_chunked(acl, hbm, dst, pool, object_bytes, effective_chunk)
            verify_region_by_chunk(dst, min(chunk_bytes, object_bytes), seed=37)
        else:
            raise ValueError(f"unknown op {op}")
        writer.add(
            {
                "status": "pass",
                "device_id": device,
                "object_bytes": object_bytes,
                "chunk_bytes": effective_chunk,
                "chunk_count": effective_count,
                "pinned_footprint_bytes": effective_chunk * effective_count,
                "mode": mode,
                "op": op,
                "repeat_idx": repeat_idx,
                "warmup": warmup,
                "latency_us": elapsed,
                "bandwidth_gib_s": gib_s(object_bytes, elapsed),
                **extra,
            }
        )
    except Exception as exc:
        writer.add(
            {
                "status": "fail",
                "reason": repr(exc),
                "device_id": device,
                "object_bytes": object_bytes,
                "chunk_bytes": chunk_bytes,
                "chunk_count": chunk_count,
                "mode": mode,
                "op": op,
                "repeat_idx": repeat_idx,
                "warmup": warmup,
            }
        )
    finally:
        if pool is not None:
            pool.close()
        if hbm is not None:
            hbm.close()
        if src is not None:
            src.close()
        if dst is not None:
            dst.close()


def write_aggregate(run_dir: Path, rows: list[dict[str, Any]]) -> None:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in rows:
        if row.get("op") not in {"h2d", "d2h"} or row.get("warmup") or row.get("status") != "pass":
            continue
        key = (
            row.get("mode"),
            int(row.get("object_bytes", 0)),
            int(row.get("chunk_bytes", 0)),
            int(row.get("chunk_count", 0)),
            row.get("op"),
        )
        groups.setdefault(key, []).append(row)
    fields = [
        "mode",
        "object_bytes",
        "chunk_bytes",
        "chunk_count",
        "op",
        "success_count",
        "fail_count",
        "mean_us",
        "median_us",
        "p90_us",
        "p95_us",
        "mean_bandwidth_gib_s",
        "pinned_footprint_bytes",
        "mean_cpu_copy_us",
        "mean_acl_submit_us",
        "mean_wait_us",
    ]
    fail_counts: dict[tuple[Any, ...], int] = {}
    for row in rows:
        if row.get("op") not in {"h2d", "d2h"} or row.get("warmup") or row.get("status") == "pass":
            continue
        key = (
            row.get("mode"),
            int(row.get("object_bytes", 0)),
            int(row.get("chunk_bytes", 0)),
            int(row.get("chunk_count", 0)),
            row.get("op"),
        )
        fail_counts[key] = fail_counts.get(key, 0) + 1
    with open(run_dir / "chunk_aggregate.csv", "w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        for key in sorted(set(groups) | set(fail_counts), key=lambda x: (x[1], x[4], x[0], x[2], x[3])):
            values = [float(row["latency_us"]) for row in groups.get(key, [])]
            count = len(values)
            values_sorted = sorted(values)
            if count:
                mean_us = statistics.fmean(values)
                median_us = statistics.median(values)
                p90_us = values_sorted[min(count - 1, int((count - 1) * 0.90))]
                p95_us = values_sorted[min(count - 1, int((count - 1) * 0.95))]
                mean_bw = gib_s(key[1], mean_us)
                mean_cpu = statistics.fmean(float(row.get("cpu_copy_us", 0.0) or 0.0) for row in groups[key])
                mean_submit = statistics.fmean(float(row.get("acl_submit_us", 0.0) or 0.0) for row in groups[key])
                mean_wait = statistics.fmean(float(row.get("wait_us", 0.0) or 0.0) for row in groups[key])
                footprint = int(groups[key][0].get("pinned_footprint_bytes", 0))
            else:
                mean_us = median_us = p90_us = p95_us = mean_bw = mean_cpu = mean_submit = mean_wait = 0.0
                footprint = 0
            writer.writerow(
                {
                    "mode": key[0],
                    "object_bytes": key[1],
                    "chunk_bytes": key[2],
                    "chunk_count": key[3],
                    "op": key[4],
                    "success_count": count,
                    "fail_count": fail_counts.get(key, 0),
                    "mean_us": f"{mean_us:.3f}",
                    "median_us": f"{median_us:.3f}",
                    "p90_us": f"{p90_us:.3f}",
                    "p95_us": f"{p95_us:.3f}",
                    "mean_bandwidth_gib_s": f"{mean_bw:.6f}",
                    "pinned_footprint_bytes": footprint,
                    "mean_cpu_copy_us": f"{mean_cpu:.3f}",
                    "mean_acl_submit_us": f"{mean_submit:.3f}",
                    "mean_wait_us": f"{mean_wait:.3f}",
                }
            )


def write_recommendation(run_dir: Path, rows: list[dict[str, Any]]) -> None:
    aggregates = list(csv.DictReader(open(run_dir / "chunk_aggregate.csv", encoding="utf-8")))
    best_by_object_op: dict[tuple[int, str], dict[str, Any]] = {}
    default_rows: list[dict[str, Any]] = []
    for row in aggregates:
        if int(row["success_count"]) <= 0:
            continue
        key = (int(row["object_bytes"]), row["op"])
        bw = float(row["mean_bandwidth_gib_s"])
        if key not in best_by_object_op or bw > float(best_by_object_op[key]["mean_bandwidth_gib_s"]):
            best_by_object_op[key] = row
        if row["mode"] == "chunked" and int(row["chunk_bytes"]) == 64 * 1024 * 1024 and int(row["chunk_count"]) == 2:
            default_rows.append(row)
    default_ratios: list[float] = []
    for row in default_rows:
        key = (int(row["object_bytes"]), row["op"])
        best = best_by_object_op.get(key)
        if best and float(best["mean_bandwidth_gib_s"]) > 0:
            default_ratios.append(float(row["mean_bandwidth_gib_s"]) / float(best["mean_bandwidth_gib_s"]))
    recommendation = {
        "default": "2x64MiB ping-pong",
        "default_min_ratio_to_best": min(default_ratios) if default_ratios else 0.0,
        "default_avg_ratio_to_best": statistics.fmean(default_ratios) if default_ratios else 0.0,
        "decision": "keep_2x64MiB_default" if default_ratios and min(default_ratios) >= 0.95 else "needs_review",
        "best_by_object_op": {f"{key[0]}:{key[1]}": value for key, value in best_by_object_op.items()},
    }
    with open(run_dir / "chunk_recommendation.json", "w", encoding="utf-8") as fp:
        json.dump(recommendation, fp, indent=2, sort_keys=True)


def bench_main(args: argparse.Namespace) -> None:
    args.run_dir.mkdir(parents=True, exist_ok=True)
    acl = _AclClient(acl_lib_path(), args.device)
    acl.backend_init()
    object_sizes = parse_sizes(args.object_sizes)
    chunk_sizes = parse_sizes(args.chunk_sizes)
    chunk_counts = [int(item) for item in args.chunk_counts.split(",") if item.strip()]
    modes = [item.strip() for item in args.modes.split(",") if item.strip()]
    ops = [item.strip() for item in args.ops.split(",") if item.strip()]
    total_iters = max(0, args.warmups) + max(1, args.repeats)
    writer = JsonlWriter(args.run_dir)
    try:
        writer.add(
            {
                "status": "pass",
                "op": "run_metadata",
                "device": args.device,
                "object_sizes": object_sizes,
                "chunk_sizes": chunk_sizes,
                "chunk_counts": chunk_counts,
                "modes": modes,
                "ops": ops,
                "warmups": args.warmups,
                "repeats": args.repeats,
                "acl_lib": acl_lib_path(),
            }
        )
        for object_bytes in object_sizes:
            for mode in modes:
                mode_chunk_sizes = [object_bytes] if mode == "full_size" else [size for size in chunk_sizes if size <= object_bytes]
                mode_chunk_counts = [1] if mode == "full_size" else chunk_counts
                for chunk_bytes in mode_chunk_sizes:
                    for chunk_count in mode_chunk_counts:
                        for op in ops:
                            print(
                                "UFLOW_E03_CHUNK_START "
                                f"mode={mode} object={object_bytes} chunk={chunk_bytes} count={chunk_count} op={op}",
                                flush=True,
                            )
                            for iter_idx in range(total_iters):
                                warmup = iter_idx < args.warmups
                                repeat_idx = -1 if warmup else iter_idx - args.warmups
                                run_one(
                                    acl=acl,
                                    writer=writer,
                                    device=args.device,
                                    object_bytes=object_bytes,
                                    chunk_bytes=chunk_bytes,
                                    chunk_count=chunk_count,
                                    mode=mode,
                                    op=op,
                                    repeat_idx=repeat_idx,
                                    warmup=warmup,
                                    run_dir=args.run_dir,
                                )
        write_aggregate(args.run_dir, writer.rows)
        write_recommendation(args.run_dir, writer.rows)
        print(f"UFLOW_E03_CHUNK_PIPELINE_BENCH_PASS run_dir={args.run_dir}", flush=True)
    finally:
        writer.close()
        try:
            acl.backend_finalize()
        except Exception:
            pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--device", type=int, default=int(os.environ.get("UF_TARGET_DEVICE", "0")))
    parser.add_argument("--object-sizes", default=os.environ.get("UF_E03_CHUNK_OBJECT_SIZES", "64m,128m,256m,512m,1g"))
    parser.add_argument("--chunk-sizes", default=os.environ.get("UF_E03_CHUNK_SIZES", "8m,16m,32m,64m,128m"))
    parser.add_argument("--chunk-counts", default=os.environ.get("UF_E03_CHUNK_COUNTS", "1,2,3"))
    parser.add_argument("--modes", default=os.environ.get("UF_E03_CHUNK_MODES", "full_size,chunked"))
    parser.add_argument("--ops", default=os.environ.get("UF_E03_CHUNK_OPS", "h2d,d2h"))
    parser.add_argument("--warmups", type=int, default=int(os.environ.get("UF_E03_CHUNK_WARMUPS", "1")))
    parser.add_argument("--repeats", type=int, default=int(os.environ.get("UF_E03_CHUNK_REPEATS", "3")))
    parser.add_argument("--run-dir", type=Path, default=Path(os.environ.get("UF_E03_CHUNK_RUN_DIR", "/tmp/proj_output/phasee03_chunk_pipeline")))
    return parser.parse_args()


def main() -> None:
    bench_main(parse_args())


if __name__ == "__main__":
    main()
