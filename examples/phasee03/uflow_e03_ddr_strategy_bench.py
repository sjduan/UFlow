from __future__ import annotations

import argparse
import ctypes
import ctypes.util
import json
import mmap
import multiprocessing as mp
import os
import resource
import subprocess
import sys
import time
import statistics
from dataclasses import dataclass, field
from multiprocessing import shared_memory
from pathlib import Path
from typing import Any, Callable


REPO_ROOT = Path(__file__).resolve().parents[2]
SDK_ROOT = REPO_ROOT / "sdk" / "python"
if str(SDK_ROOT) not in sys.path:
    sys.path.insert(0, str(SDK_ROOT))

from uflow.client import _AclClient, _UfAclHbmBlock, _UfAclHostMemory, _UfAclHostRegisterInfo  # noqa: E402


DEFAULT_STRATEGIES = [
    "tmpfs_mmap_shared",
    "file_mmap_shared",
    "memfd_shared",
    "posix_shm_shared",
    "anonymous_shared_fork_only",
    "locked_mmap_shared",
    "thp_mmap_shared",
    "hugetlb_mmap_shared",
    "numa_mmap_shared",
    "acl_malloc_host_pinned",
    "malloc_registered_host",
    "registered_tmpfs_mmap_shared",
    "registered_memfd_shared",
    "registered_posix_shm_shared",
    "registered_tmpfs_mmap_v2_shared",
    "mmap_plus_pinned_staging",
    "memfd_plus_pinned_staging",
]

FOCUSED_SIZE_SWEEP_STRATEGIES = [
    "memfd_shared",
    "memfd_plus_pinned_staging",
    "registered_memfd_shared",
]

MEASURED_OPS = {
    "ddr_to_ddr_memcpy",
    "h2d_sync",
    "d2h_sync",
    "h2d_async",
    "d2h_async",
}

PROBE_BYTES = 4096
PROT_READ = 0x1
PROT_WRITE = 0x2
MAP_SHARED = 0x01
MAP_ANONYMOUS = 0x20
MAP_HUGETLB = 0x40000
MADV_HUGEPAGE = 14

LIBC = ctypes.CDLL(None, use_errno=True)
LIBC.mlock.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
LIBC.mlock.restype = ctypes.c_int
LIBC.munlock.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
LIBC.munlock.restype = ctypes.c_int
LIBC.madvise.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int]
LIBC.madvise.restype = ctypes.c_int
LIBC.mmap.argtypes = [
    ctypes.c_void_p,
    ctypes.c_size_t,
    ctypes.c_int,
    ctypes.c_int,
    ctypes.c_int,
    ctypes.c_long,
]
LIBC.mmap.restype = ctypes.c_void_p
LIBC.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
LIBC.munmap.restype = ctypes.c_int
LIBC.posix_memalign.argtypes = [ctypes.POINTER(ctypes.c_void_p), ctypes.c_size_t, ctypes.c_size_t]
LIBC.posix_memalign.restype = ctypes.c_int
LIBC.free.argtypes = [ctypes.c_void_p]
LIBC.free.restype = None


class SkipStrategy(RuntimeError):
    pass


def now_us() -> float:
    return time.perf_counter_ns() / 1000.0


def timed_us(fn: Callable[[], Any]) -> tuple[Any, float]:
    start = now_us()
    value = fn()
    return value, now_us() - start


def gib_s(nbytes: int, elapsed_us: float) -> float:
    if elapsed_us <= 0:
        return 0.0
    return (float(nbytes) / (1024.0**3)) / (elapsed_us / 1_000_000.0)


def pattern_bytes(nbytes: int, *, offset: int = 0) -> bytes:
    base = bytes(((idx + offset) % 251 for idx in range(251)))
    return (base * ((int(nbytes) + len(base) - 1) // len(base)))[: int(nbytes)]


def ptr_from_buffer(buf: Any) -> int:
    return ctypes.addressof(ctypes.c_char.from_buffer(buf))


def memzero(ptr: int, nbytes: int) -> None:
    ctypes.memset(ctypes.c_void_p(int(ptr)), 0, int(nbytes))


def memwrite(ptr: int, payload: bytes, *, offset: int = 0) -> None:
    ctypes.memmove(ctypes.c_void_p(int(ptr) + int(offset)), payload, len(payload))


def memread(ptr: int, nbytes: int, *, offset: int = 0) -> bytes:
    return ctypes.string_at(int(ptr) + int(offset), int(nbytes))


def parse_sizes(text: str) -> list[int]:
    sizes = []
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        suffix = part[-1].lower()
        if suffix in {"k", "m", "g"}:
            scale = {"k": 1024, "m": 1024**2, "g": 1024**3}[suffix]
            sizes.append(int(float(part[:-1]) * scale))
        else:
            sizes.append(int(float(part)))
    return sizes


def acl_lib_path() -> str:
    return os.environ.get("UF_ACL_LIB", "/home/sj/git/data-service/build/lib/libuf_acl_shim.so")


@dataclass
class HostRegion:
    strategy: str
    nbytes: int
    ptr: int
    close_fn: Callable[[], None]
    share_kind: str
    path: str = ""
    fd: int | None = None
    shm_name: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    register_info: _UfAclHostRegisterInfo | None = None
    pinned_host: _UfAclHostMemory | None = None
    alloc_us: float = 0.0
    register_us: float = 0.0

    def close(self) -> None:
        self.close_fn()

    def fill(self, offset: int = 0) -> bytes:
        payload = pattern_bytes(self.nbytes, offset=offset)
        memwrite(self.ptr, payload)
        return payload


@dataclass
class LocalHbmRegion:
    acl: _AclClient
    block: _UfAclHbmBlock

    @property
    def device_ptr(self) -> int:
        return int(self.block.service_device_ptr)

    def release(self) -> None:
        if self.block.raw_handle_id:
            self.acl.free_physical(self.block)
            self.block.raw_handle_id = 0


def make_tmpfs_mmap(strategy: str, nbytes: int, run_dir: Path, suffix: str, *, root: Path | None = None) -> HostRegion:
    root = root or Path("/dev/shm")
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"uflow_e03_{os.getpid()}_{strategy}_{suffix}"
    file_obj = open(path, "w+b")
    file_obj.truncate(nbytes)
    mapped = mmap.mmap(file_obj.fileno(), nbytes)
    ptr = ptr_from_buffer(mapped)

    def close() -> None:
        try:
            mapped.close()
        finally:
            try:
                file_obj.close()
            finally:
                try:
                    path.unlink()
                except FileNotFoundError:
                    pass

    return HostRegion(strategy, nbytes, ptr, close, "path", path=str(path), metadata={"root": str(root)})


def make_file_mmap(strategy: str, nbytes: int, run_dir: Path, suffix: str) -> HostRegion:
    return make_tmpfs_mmap(strategy, nbytes, run_dir, suffix, root=run_dir / "file_mmap")


def make_memfd(strategy: str, nbytes: int, run_dir: Path, suffix: str) -> HostRegion:
    if not hasattr(os, "memfd_create"):
        raise SkipStrategy("os.memfd_create is not available")
    fd = os.memfd_create(f"uflow_e03_{strategy}_{suffix}", flags=0)
    os.ftruncate(fd, nbytes)
    mapped = mmap.mmap(fd, nbytes)
    ptr = ptr_from_buffer(mapped)

    def close() -> None:
        try:
            mapped.close()
        finally:
            os.close(fd)

    return HostRegion(strategy, nbytes, ptr, close, "fd", fd=fd)


def make_posix_shm(strategy: str, nbytes: int, run_dir: Path, suffix: str) -> HostRegion:
    shm = shared_memory.SharedMemory(create=True, size=nbytes, name=None)
    ptr = ptr_from_buffer(shm.buf)

    def close() -> None:
        try:
            shm.close()
        finally:
            try:
                shm.unlink()
            except FileNotFoundError:
                pass

    return HostRegion(strategy, nbytes, ptr, close, "posix_shm", shm_name=shm.name)


def make_anonymous(strategy: str, nbytes: int, run_dir: Path, suffix: str) -> HostRegion:
    mapped = mmap.mmap(-1, nbytes)
    ptr = ptr_from_buffer(mapped)

    def close() -> None:
        mapped.close()

    return HostRegion(strategy, nbytes, ptr, close, "fork_only")


def make_locked_mmap(strategy: str, nbytes: int, run_dir: Path, suffix: str) -> HostRegion:
    soft, hard = resource.getrlimit(resource.RLIMIT_MEMLOCK)
    if soft != resource.RLIM_INFINITY and soft < nbytes:
        raise SkipStrategy(f"RLIMIT_MEMLOCK soft limit {soft} < nbytes {nbytes}")
    region = make_tmpfs_mmap(strategy, nbytes, run_dir, suffix)
    rc = LIBC.mlock(ctypes.c_void_p(region.ptr), ctypes.c_size_t(nbytes))
    if rc != 0:
        err = ctypes.get_errno()
        region.close()
        raise SkipStrategy(f"mlock failed errno={err}")
    old_close = region.close_fn

    def close() -> None:
        try:
            LIBC.munlock(ctypes.c_void_p(region.ptr), ctypes.c_size_t(nbytes))
        finally:
            old_close()

    region.close_fn = close
    region.metadata["mlock"] = "1"
    return region


def make_thp_mmap(strategy: str, nbytes: int, run_dir: Path, suffix: str) -> HostRegion:
    region = make_tmpfs_mmap(strategy, nbytes, run_dir, suffix)
    rc = LIBC.madvise(ctypes.c_void_p(region.ptr), ctypes.c_size_t(nbytes), ctypes.c_int(MADV_HUGEPAGE))
    region.metadata["madvise_hugepage_rc"] = rc
    if rc != 0:
        region.metadata["madvise_hugepage_errno"] = ctypes.get_errno()
    return region


def make_hugetlb(strategy: str, nbytes: int, run_dir: Path, suffix: str) -> HostRegion:
    ptr = LIBC.mmap(
        None,
        ctypes.c_size_t(nbytes),
        PROT_READ | PROT_WRITE,
        MAP_SHARED | MAP_ANONYMOUS | MAP_HUGETLB,
        -1,
        0,
    )
    map_failed = ctypes.c_void_p(-1).value
    if ptr is None or int(ptr) == int(map_failed):
        err = ctypes.get_errno()
        raise SkipStrategy(f"MAP_HUGETLB mmap failed errno={err}")

    def close() -> None:
        LIBC.munmap(ctypes.c_void_p(int(ptr)), ctypes.c_size_t(nbytes))

    return HostRegion(strategy, nbytes, int(ptr), close, "fork_only")


def make_numa_mmap(strategy: str, nbytes: int, run_dir: Path, suffix: str) -> HostRegion:
    region = make_tmpfs_mmap(strategy, nbytes, run_dir, suffix)
    region.metadata["numa_policy"] = "first_touch_only"
    region.metadata["numa_note"] = "PhaseE-03 prototype records NUMA as a cost tag; hard binding is deferred to libnuma/mbind backend."
    return region


def make_pinned_host(strategy: str, nbytes: int, run_dir: Path, suffix: str, acl: _AclClient) -> HostRegion:
    host = acl.malloc_host(nbytes)

    def close() -> None:
        acl.free_host(host)

    return HostRegion(strategy, nbytes, int(host.host_ptr), close, "not_shareable", pinned_host=host)


def make_aligned_heap(strategy: str, nbytes: int, run_dir: Path, suffix: str) -> HostRegion:
    alignment = max(4096, mmap.PAGESIZE)
    out = ctypes.c_void_p()
    rc = LIBC.posix_memalign(ctypes.byref(out), ctypes.c_size_t(alignment), ctypes.c_size_t(nbytes))
    if rc != 0 or out.value is None:
        raise SkipStrategy(f"posix_memalign failed rc={rc}")
    ptr = int(out.value)

    def close() -> None:
        LIBC.free(ctypes.c_void_p(ptr))

    return HostRegion(strategy, nbytes, ptr, close, "not_shareable", metadata={"alignment": alignment})


def register_region(region: HostRegion, acl: _AclClient, device: int, *, use_v2: bool = False) -> HostRegion:
    info, elapsed = timed_us(lambda: acl.host_register(region.ptr, region.nbytes, device_id=device, use_v2=use_v2))
    region.register_info = info
    region.register_us = elapsed
    region.metadata["registered_device_ptr"] = f"0x{int(info.device_ptr):x}" if info.device_ptr else "0x0"
    region.metadata["register_api"] = "aclrtHostRegisterV2" if use_v2 else "aclrtHostRegister"
    old_close = region.close_fn

    def close() -> None:
        try:
            if region.register_info is not None and region.register_info.registered_host_id:
                acl.host_unregister(region.register_info)
        finally:
            old_close()

    region.close_fn = close
    return region


def make_staging_region(
    strategy: str,
    nbytes: int,
    run_dir: Path,
    suffix: str,
    acl: _AclClient,
    *,
    backing: str = "tmpfs",
) -> HostRegion:
    if backing == "memfd":
        region = make_memfd(strategy, nbytes, run_dir, suffix)
    else:
        region = make_tmpfs_mmap(strategy, nbytes, run_dir, suffix)
    pinned = acl.malloc_host(nbytes)
    region.pinned_host = pinned
    region.metadata["staging_ptr"] = f"0x{int(pinned.host_ptr):x}"
    region.metadata["staging_backing"] = backing
    old_close = region.close_fn

    def close() -> None:
        try:
            acl.free_host(pinned)
        finally:
            old_close()

    region.close_fn = close
    return region


def create_region(strategy: str, nbytes: int, run_dir: Path, suffix: str, acl: _AclClient, device: int) -> HostRegion:
    start = now_us()
    if strategy == "tmpfs_mmap_shared":
        region = make_tmpfs_mmap(strategy, nbytes, run_dir, suffix)
    elif strategy == "file_mmap_shared":
        region = make_file_mmap(strategy, nbytes, run_dir, suffix)
    elif strategy == "memfd_shared":
        region = make_memfd(strategy, nbytes, run_dir, suffix)
    elif strategy == "posix_shm_shared":
        region = make_posix_shm(strategy, nbytes, run_dir, suffix)
    elif strategy == "anonymous_shared_fork_only":
        region = make_anonymous(strategy, nbytes, run_dir, suffix)
    elif strategy == "locked_mmap_shared":
        region = make_locked_mmap(strategy, nbytes, run_dir, suffix)
    elif strategy == "thp_mmap_shared":
        region = make_thp_mmap(strategy, nbytes, run_dir, suffix)
    elif strategy == "hugetlb_mmap_shared":
        region = make_hugetlb(strategy, nbytes, run_dir, suffix)
    elif strategy == "numa_mmap_shared":
        region = make_numa_mmap(strategy, nbytes, run_dir, suffix)
    elif strategy == "acl_malloc_host_pinned":
        region = make_pinned_host(strategy, nbytes, run_dir, suffix, acl)
    elif strategy == "malloc_registered_host":
        region = register_region(make_aligned_heap(strategy, nbytes, run_dir, suffix), acl, device)
    elif strategy == "registered_tmpfs_mmap_shared":
        region = register_region(make_tmpfs_mmap(strategy, nbytes, run_dir, suffix), acl, device)
    elif strategy == "registered_memfd_shared":
        region = register_region(make_memfd(strategy, nbytes, run_dir, suffix), acl, device)
    elif strategy == "registered_posix_shm_shared":
        region = register_region(make_posix_shm(strategy, nbytes, run_dir, suffix), acl, device)
    elif strategy == "registered_tmpfs_mmap_v2_shared":
        region = register_region(make_tmpfs_mmap(strategy, nbytes, run_dir, suffix), acl, device, use_v2=True)
    elif strategy == "mmap_plus_pinned_staging":
        region = make_staging_region(strategy, nbytes, run_dir, suffix, acl)
    elif strategy == "memfd_plus_pinned_staging":
        region = make_staging_region(strategy, nbytes, run_dir, suffix, acl, backing="memfd")
    else:
        raise SkipStrategy(f"unknown strategy {strategy}")
    region.alloc_us = now_us() - start
    return region


def path_share_child(args: argparse.Namespace) -> None:
    if args.kind == "path":
        file_obj = open(args.path, "r+b")
        mapped = mmap.mmap(file_obj.fileno(), args.bytes)
        close_fn = lambda: (mapped.close(), file_obj.close())
    elif args.kind == "fd":
        mapped = mmap.mmap(args.fd, args.bytes)
        close_fn = lambda: mapped.close()
    elif args.kind == "posix_shm":
        shm = shared_memory.SharedMemory(name=args.name)
        mapped = shm.buf
        close_fn = lambda: shm.close()
    else:
        raise SystemExit(f"unsupported child kind {args.kind}")
    try:
        ptr = ptr_from_buffer(mapped)
        expected = pattern_bytes(min(PROBE_BYTES, args.bytes), offset=args.pattern_offset)
        actual = memread(ptr, len(expected))
        if actual != expected:
            raise AssertionError(f"share child observed unexpected prefix for {args.kind}")
        overlay = pattern_bytes(args.overlay_bytes, offset=args.overlay_pattern_offset)
        memwrite(ptr, overlay, offset=args.overlay_offset)
        print("UFLOW_E03_SHARE_CHILD_PASS", flush=True)
    finally:
        close_fn()


def fork_share_worker(ptr: int, nbytes: int, overlay_offset: int, overlay_bytes: int, pattern_offset: int, overlay_pattern_offset: int, queue: Any) -> None:
    try:
        expected = pattern_bytes(min(PROBE_BYTES, nbytes), offset=pattern_offset)
        actual = memread(ptr, len(expected))
        if actual != expected:
            queue.put(("fail", "fork child observed unexpected prefix"))
            return
        overlay = pattern_bytes(overlay_bytes, offset=overlay_pattern_offset)
        memwrite(ptr, overlay, offset=overlay_offset)
        queue.put(("ok", ""))
    except Exception as exc:  # pragma: no cover - child diagnostic path
        queue.put(("fail", repr(exc)))


def run_share_probe(region: HostRegion, script: Path, pattern_offset: int, run_dir: Path) -> dict[str, Any]:
    overlay_bytes = min(4096, max(1, region.nbytes // 4))
    overlay_offset = max(0, region.nbytes // 2 - overlay_bytes // 2)
    overlay_pattern_offset = 93
    result: dict[str, Any] = {
        "status": "skip",
        "reason": "",
        "share_kind": region.share_kind,
        "overlay_offset": overlay_offset,
        "overlay_bytes": overlay_bytes,
    }
    try:
        if region.share_kind == "path":
            cmd = [
                sys.executable,
                str(script),
                "share-child",
                "--kind",
                "path",
                "--path",
                region.path,
                "--bytes",
                str(region.nbytes),
                "--overlay-offset",
                str(overlay_offset),
                "--overlay-bytes",
                str(overlay_bytes),
                "--pattern-offset",
                str(pattern_offset),
                "--overlay-pattern-offset",
                str(overlay_pattern_offset),
            ]
            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        elif region.share_kind == "fd" and region.fd is not None:
            cmd = [
                sys.executable,
                str(script),
                "share-child",
                "--kind",
                "fd",
                "--fd",
                str(region.fd),
                "--bytes",
                str(region.nbytes),
                "--overlay-offset",
                str(overlay_offset),
                "--overlay-bytes",
                str(overlay_bytes),
                "--pattern-offset",
                str(pattern_offset),
                "--overlay-pattern-offset",
                str(overlay_pattern_offset),
            ]
            subprocess.run(cmd, check=True, pass_fds=(region.fd,), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        elif region.share_kind == "posix_shm":
            cmd = [
                sys.executable,
                str(script),
                "share-child",
                "--kind",
                "posix_shm",
                "--name",
                region.shm_name,
                "--bytes",
                str(region.nbytes),
                "--overlay-offset",
                str(overlay_offset),
                "--overlay-bytes",
                str(overlay_bytes),
                "--pattern-offset",
                str(pattern_offset),
                "--overlay-pattern-offset",
                str(overlay_pattern_offset),
            ]
            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        elif region.share_kind == "fork_only":
            ctx = mp.get_context("fork")
            queue = ctx.Queue()
            proc = ctx.Process(
                target=fork_share_worker,
                args=(region.ptr, region.nbytes, overlay_offset, overlay_bytes, pattern_offset, overlay_pattern_offset, queue),
            )
            proc.start()
            proc.join(10)
            if proc.exitcode != 0:
                raise RuntimeError(f"fork child exitcode={proc.exitcode}")
            status, message = queue.get(timeout=2)
            if status != "ok":
                raise RuntimeError(message)
        else:
            result["reason"] = "not_shareable"
            return result
        expected_overlay = pattern_bytes(overlay_bytes, offset=overlay_pattern_offset)
        actual_overlay = memread(region.ptr, overlay_bytes, offset=overlay_offset)
        if actual_overlay != expected_overlay:
            raise AssertionError("parent did not observe child overlay")
        result["status"] = "pass"
        return result
    except Exception as exc:
        result["status"] = "fail"
        result["reason"] = repr(exc)
        return result


class ResultWriter:
    def __init__(self, run_dir: Path) -> None:
        self.run_dir = run_dir
        self.rows: list[dict[str, Any]] = []
        self.jsonl = open(run_dir / "results.jsonl", "w", encoding="utf-8")

    def add(self, row: dict[str, Any]) -> None:
        row.setdefault("ts_ns", time.time_ns())
        self.rows.append(row)
        self.jsonl.write(json.dumps(row, sort_keys=True) + "\n")
        self.jsonl.flush()

    def close(self) -> None:
        self.jsonl.close()

    def write_summary(self) -> None:
        fields = [
            "strategy",
            "size",
            "device_id",
            "op",
            "repeat_idx",
            "warmup",
            "status",
            "latency_us",
            "bandwidth_gib_s",
            "alloc_us",
            "register_us",
            "share_kind",
            "reason",
        ]
        with open(self.run_dir / "summary.csv", "w", encoding="utf-8") as f:
            f.write(",".join(fields) + "\n")
            for row in self.rows:
                values = []
                for field_name in fields:
                    value = row.get(field_name, "")
                    text = str(value).replace("\n", " ").replace(",", ";")
                    values.append(text)
                f.write(",".join(values) + "\n")

    def write_setup_summary(self) -> None:
        fields = [
            "strategy",
            "size",
            "device_id",
            "share_kind",
            "alloc_us",
            "register_us",
            "pinned_pool_allocated",
            "metadata",
        ]
        with open(self.run_dir / "setup_summary.csv", "w", encoding="utf-8") as f:
            f.write(",".join(fields) + "\n")
            for row in self.rows:
                if row.get("op") != "allocate" or row.get("status") != "pass":
                    continue
                values = []
                for field_name in fields:
                    value = row.get(field_name, "")
                    if field_name == "metadata":
                        value = json.dumps(value, sort_keys=True)
                    text = str(value).replace("\n", " ").replace(",", ";")
                    values.append(text)
                f.write(",".join(values) + "\n")

    def write_aggregate(self) -> None:
        groups: dict[tuple[str, int, str], list[float]] = {}
        failures: dict[tuple[str, int, str], int] = {}
        for row in self.rows:
            if row.get("op") not in MEASURED_OPS or bool(row.get("warmup", False)):
                continue
            key = (str(row.get("strategy", "")), int(row.get("size", 0) or 0), str(row.get("op", "")))
            if row.get("status") == "pass":
                groups.setdefault(key, []).append(float(row.get("latency_us", 0.0) or 0.0))
            else:
                failures[key] = failures.get(key, 0) + 1

        fields = [
            "strategy",
            "size",
            "op",
            "success_count",
            "fail_count",
            "mean_us",
            "median_us",
            "p90_us",
            "p95_us",
            "min_us",
            "max_us",
            "stddev_us",
            "mean_bandwidth_gib_s",
        ]
        with open(self.run_dir / "aggregate.csv", "w", encoding="utf-8") as f:
            f.write(",".join(fields) + "\n")
            all_keys = sorted(set(groups) | set(failures), key=lambda x: (x[1], x[0], x[2]))
            for key in all_keys:
                values = sorted(groups.get(key, []))
                count = len(values)
                if count:
                    mean_us = statistics.fmean(values)
                    median_us = statistics.median(values)
                    p90_us = values[min(count - 1, int((count - 1) * 0.90))]
                    p95_us = values[min(count - 1, int((count - 1) * 0.95))]
                    min_us = values[0]
                    max_us = values[-1]
                    stddev_us = statistics.pstdev(values) if count > 1 else 0.0
                    bw = gib_s(key[1], mean_us)
                else:
                    mean_us = median_us = p90_us = p95_us = min_us = max_us = stddev_us = bw = 0.0
                row_values = [
                    key[0],
                    key[1],
                    key[2],
                    count,
                    failures.get(key, 0),
                    f"{mean_us:.3f}",
                    f"{median_us:.3f}",
                    f"{p90_us:.3f}",
                    f"{p95_us:.3f}",
                    f"{min_us:.3f}",
                    f"{max_us:.3f}",
                    f"{stddev_us:.3f}",
                    f"{bw:.6f}",
                ]
                f.write(",".join(str(item) for item in row_values) + "\n")


def record_fail(writer: ResultWriter, strategy: str, size: int, op: str, exc: Exception, **extra: Any) -> None:
    writer.add(
        {
            "strategy": strategy,
            "size": size,
            "op": op,
            "status": "fail",
            "reason": repr(exc),
            **extra,
        }
    )


def is_pinned_staging(region: HostRegion) -> bool:
    return region.strategy in {"mmap_plus_pinned_staging", "memfd_plus_pinned_staging"} and region.pinned_host is not None


def h2d_sync(acl: _AclClient, hbm: Any, region: HostRegion, nbytes: int) -> float:
    if is_pinned_staging(region):
        start = now_us()
        ctypes.memmove(ctypes.c_void_p(int(region.pinned_host.host_ptr)), ctypes.c_void_p(region.ptr), nbytes)
        acl.h2d(hbm.device_ptr, int(region.pinned_host.host_ptr), nbytes)
        return now_us() - start
    _, elapsed = timed_us(lambda: acl.h2d(hbm.device_ptr, region.ptr, nbytes))
    return elapsed


def d2h_sync(acl: _AclClient, hbm: Any, region: HostRegion, nbytes: int) -> float:
    if is_pinned_staging(region):
        start = now_us()
        acl.d2h(int(region.pinned_host.host_ptr), hbm.device_ptr, nbytes)
        ctypes.memmove(ctypes.c_void_p(region.ptr), ctypes.c_void_p(int(region.pinned_host.host_ptr)), nbytes)
        return now_us() - start
    _, elapsed = timed_us(lambda: acl.d2h(region.ptr, hbm.device_ptr, nbytes))
    return elapsed


def h2d_async(acl: _AclClient, hbm: Any, region: HostRegion, nbytes: int) -> float:
    stream = acl.create_stream()
    event = acl.create_event()
    try:
        start = now_us()
        if is_pinned_staging(region):
            ctypes.memmove(ctypes.c_void_p(int(region.pinned_host.host_ptr)), ctypes.c_void_p(region.ptr), nbytes)
            host_ptr = int(region.pinned_host.host_ptr)
        else:
            host_ptr = region.ptr
        acl.h2d_async(hbm.device_ptr, host_ptr, nbytes, stream_id=stream, event_id=event)
        acl.synchronize_event(event)
        acl.synchronize_stream(stream)
        return now_us() - start
    finally:
        acl.destroy_event(event)
        acl.destroy_stream(stream)


def d2h_async(acl: _AclClient, hbm: Any, region: HostRegion, nbytes: int) -> float:
    stream = acl.create_stream()
    event = acl.create_event()
    try:
        start = now_us()
        if is_pinned_staging(region):
            host_ptr = int(region.pinned_host.host_ptr)
        else:
            host_ptr = region.ptr
        acl.d2h_async(host_ptr, hbm.device_ptr, nbytes, stream_id=stream, event_id=event)
        acl.synchronize_event(event)
        acl.synchronize_stream(stream)
        if is_pinned_staging(region):
            ctypes.memmove(ctypes.c_void_p(region.ptr), ctypes.c_void_p(int(region.pinned_host.host_ptr)), nbytes)
        return now_us() - start
    finally:
        acl.destroy_event(event)
        acl.destroy_stream(stream)


def bench_strategy_size(
    *,
    strategy: str,
    size: int,
    args: argparse.Namespace,
    acl: _AclClient,
    writer: ResultWriter,
) -> None:
    hbm = None
    src = None
    dst = None
    pattern_offset = 17
    try:
        hbm = LocalHbmRegion(acl=acl, block=acl.alloc_physical(size))
        src = create_region(strategy, size, args.run_dir, f"src_{size}", acl, args.device)
        dst = create_region(strategy, size, args.run_dir, f"dst_{size}", acl, args.device)
        payload = src.fill(offset=pattern_offset)
        memzero(dst.ptr, size)
        writer.add(
            {
                "strategy": strategy,
                "size": size,
                "device_id": args.device,
                "op": "allocate",
                "status": "pass",
                "alloc_us": src.alloc_us,
                "register_us": src.register_us,
                "share_kind": src.share_kind,
                "pinned_pool_allocated": bool(src.pinned_host is not None),
                "metadata": src.metadata,
            }
        )
        share = run_share_probe(src, Path(__file__).resolve(), pattern_offset, args.run_dir)
        writer.add(
            {
                "strategy": strategy,
                "size": size,
                "device_id": args.device,
                "op": "share_probe",
                "status": share["status"],
                "reason": share.get("reason", ""),
                "latency_us": share.get("latency_us", ""),
                "share_kind": share["share_kind"],
            }
        )
        # Restore source payload after the share overlay.
        memwrite(src.ptr, payload)

        total_iters = max(0, int(args.warmups)) + max(1, int(args.repeats))

        for iter_idx in range(total_iters):
            warmup = iter_idx < int(args.warmups)
            repeat_idx = -1 if warmup else iter_idx - int(args.warmups)
            try:
                memwrite(src.ptr, payload)
                memzero(dst.ptr, size)
                _, ddr_copy_us = timed_us(lambda: ctypes.memmove(ctypes.c_void_p(dst.ptr), ctypes.c_void_p(src.ptr), size))
                if memread(dst.ptr, min(size, PROBE_BYTES)) != payload[: min(size, PROBE_BYTES)]:
                    raise AssertionError("DDR-DDR memcpy verification failed")
                writer.add(
                    {
                        "strategy": strategy,
                        "size": size,
                        "device_id": args.device,
                        "op": "ddr_to_ddr_memcpy",
                        "status": "pass",
                        "repeat_idx": repeat_idx,
                        "warmup": warmup,
                        "latency_us": ddr_copy_us,
                        "bandwidth_gib_s": gib_s(size, ddr_copy_us),
                        "share_kind": src.share_kind,
                        "verification_bytes": min(size, PROBE_BYTES),
                    }
                )
            except Exception as exc:
                record_fail(
                    writer,
                    strategy,
                    size,
                    "ddr_to_ddr_memcpy",
                    exc,
                    repeat_idx=repeat_idx,
                    warmup=warmup,
                    device_id=args.device,
                    share_kind=src.share_kind,
                )

        for op_name, op_fn in [
            ("h2d_sync", h2d_sync),
            ("d2h_sync", d2h_sync),
            ("h2d_async", h2d_async),
            ("d2h_async", d2h_async),
        ]:
            if op_name.startswith("d2h"):
                memwrite(src.ptr, payload)
                acl.h2d(hbm.device_ptr, src.ptr, size)
            for iter_idx in range(total_iters):
                warmup = iter_idx < int(args.warmups)
                repeat_idx = -1 if warmup else iter_idx - int(args.warmups)
                try:
                    if op_name.startswith("h2d"):
                        memwrite(src.ptr, payload)
                        elapsed = op_fn(acl, hbm, src, size)
                        verify = bytearray(min(size, PROBE_BYTES))
                        verify_buf = (ctypes.c_char * len(verify)).from_buffer(verify)
                        acl.d2h(ctypes.addressof(verify_buf), hbm.device_ptr, len(verify))
                        if bytes(verify) != payload[: len(verify)]:
                            raise AssertionError(f"{op_name} verification failed")
                    else:
                        memzero(dst.ptr, size)
                        elapsed = op_fn(acl, hbm, dst, size)
                        if memread(dst.ptr, min(size, PROBE_BYTES)) != payload[: min(size, PROBE_BYTES)]:
                            raise AssertionError(f"{op_name} verification failed")
                    writer.add(
                        {
                            "strategy": strategy,
                            "size": size,
                            "device_id": args.device,
                            "op": op_name,
                            "status": "pass",
                            "repeat_idx": repeat_idx,
                            "warmup": warmup,
                            "latency_us": elapsed,
                            "bandwidth_gib_s": gib_s(size, elapsed),
                            "share_kind": src.share_kind,
                            "register_us": src.register_us,
                            "verification_bytes": min(size, PROBE_BYTES),
                        }
                    )
                except Exception as exc:
                    record_fail(
                        writer,
                        strategy,
                        size,
                        op_name,
                        exc,
                        repeat_idx=repeat_idx,
                        warmup=warmup,
                        device_id=args.device,
                        share_kind=src.share_kind,
                        register_us=src.register_us,
                    )
    except SkipStrategy as exc:
        writer.add(
            {
                "strategy": strategy,
                "size": size,
                "device_id": args.device,
                "op": "strategy",
                "status": "skip",
                "reason": str(exc),
            }
        )
    except Exception as exc:
        record_fail(writer, strategy, size, "strategy", exc, device_id=args.device)
    finally:
        for region in [dst, src]:
            if region is not None:
                try:
                    region.close()
                except Exception as exc:
                    writer.add(
                        {
                            "strategy": strategy,
                            "size": size,
                            "device_id": args.device,
                            "op": "cleanup",
                            "status": "fail",
                            "reason": repr(exc),
                        }
                    )
        if hbm is not None:
            hbm.release()


def summarize_recommendation(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_strategy: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_strategy.setdefault(str(row.get("strategy", "")), []).append(row)

    def passed(strategy: str, op: str) -> bool:
        strategy_rows = [row for row in by_strategy.get(strategy, []) if row.get("op") == op]
        return bool(strategy_rows) and all(row.get("status") == "pass" for row in strategy_rows)

    def avg_bw(strategy: str, ops: set[str]) -> float:
        values = [
            float(row.get("bandwidth_gib_s", 0.0) or 0.0)
            for row in by_strategy.get(strategy, [])
            if row.get("op") in ops and row.get("status") == "pass"
            and not bool(row.get("warmup", False))
        ]
        return sum(values) / len(values) if values else 0.0

    independent_shared = [
        strategy
        for strategy in by_strategy
        if any(row.get("op") == "share_probe" and row.get("status") == "pass" and row.get("share_kind") in {"path", "fd", "posix_shm"} for row in by_strategy[strategy])
    ]
    direct_dma_candidates = [
        strategy
        for strategy in independent_shared
        if passed(strategy, "h2d_sync") and passed(strategy, "d2h_sync") and passed(strategy, "h2d_async") and passed(strategy, "d2h_async")
    ]
    registered_candidates = [strategy for strategy in direct_dma_candidates if strategy.startswith("registered_")]
    preferred_shared_order = ["memfd_shared", "tmpfs_mmap_shared", "posix_shm_shared", "file_mmap_shared"]
    default_shared_ddr = next((item for item in preferred_shared_order if item in independent_shared), "")
    if not default_shared_ddr:
        default_shared_ddr = independent_shared[0] if independent_shared else "tmpfs_mmap_shared"

    no_bounce_candidates = [
        item
        for item in direct_dma_candidates
        if item
        in {
            "memfd_shared",
            "tmpfs_mmap_shared",
            "posix_shm_shared",
            "registered_memfd_shared",
            "registered_tmpfs_mmap_shared",
            "registered_posix_shm_shared",
        }
    ]
    if no_bounce_candidates:
        fastest_no_bounce = max(
            no_bounce_candidates,
            key=lambda item: avg_bw(item, {"h2d_sync", "d2h_sync", "h2d_async", "d2h_async"}),
        )
        memfd_direct_bw = avg_bw("memfd_shared", {"h2d_sync", "d2h_sync", "h2d_async", "d2h_async"})
        best_no_bounce_bw = avg_bw(fastest_no_bounce, {"h2d_sync", "d2h_sync", "h2d_async", "d2h_async"})
        if "memfd_shared" in no_bounce_candidates and memfd_direct_bw >= best_no_bounce_bw * 0.80:
            optional_no_bounce = "memfd_shared"
        else:
            optional_no_bounce = fastest_no_bounce
    else:
        optional_no_bounce = ""

    pinned_bw = avg_bw("acl_malloc_host_pinned", {"h2d_async", "d2h_async", "h2d_sync", "d2h_sync"})
    mmap_staging_bw = avg_bw("mmap_plus_pinned_staging", {"h2d_async", "d2h_async", "h2d_sync", "d2h_sync"})
    memfd_staging_bw = avg_bw("memfd_plus_pinned_staging", {"h2d_async", "d2h_async", "h2d_sync", "d2h_sync"})
    staging_bw = max(mmap_staging_bw, memfd_staging_bw)
    staging_name = "memfd_plus_pinned_staging" if memfd_staging_bw >= mmap_staging_bw else "mmap_plus_pinned_staging"
    no_bounce_bw = avg_bw(optional_no_bounce, {"h2d_async", "d2h_async", "h2d_sync", "d2h_sync"}) if optional_no_bounce else 0.0
    if staging_bw > 0 and staging_bw >= no_bounce_bw * 1.25:
        default_transfer = "shared_ddr_plus_pinned_staging"
        fallback = "direct_shared_ddr"
    elif optional_no_bounce.startswith("registered_"):
        default_transfer = "direct_registered_shared_ddr"
        fallback = staging_name
    else:
        default_transfer = "direct_shared_ddr"
        fallback = staging_name

    return {
        "default_shared_ddr": default_shared_ddr,
        "default_transfer_path": default_transfer,
        "fallback_path": fallback,
        "optional_no_bounce_path": optional_no_bounce,
        "independent_shared_pass": sorted(independent_shared),
        "direct_dma_candidates": sorted(direct_dma_candidates),
        "registered_candidates": sorted(registered_candidates),
        "acl_malloc_host_pinned_avg_gib_s": pinned_bw,
        "mmap_plus_pinned_staging_avg_gib_s": mmap_staging_bw,
        "memfd_plus_pinned_staging_avg_gib_s": memfd_staging_bw,
        "best_pinned_staging_avg_gib_s": staging_bw,
        "optional_no_bounce_avg_gib_s": no_bounce_bw,
        "registration_policy": "available_with_aclrtHostRegister_v1_but_not_default_until_it_beats_unregistered_or_pinned_staging",
    }


def bench_main(args: argparse.Namespace) -> None:
    args.run_dir.mkdir(parents=True, exist_ok=True)
    strategies = args.strategies or DEFAULT_STRATEGIES
    sizes = parse_sizes(args.sizes)
    acl = _AclClient(acl_lib_path(), args.device)
    acl.backend_init()
    writer = ResultWriter(args.run_dir)
    try:
        writer.add(
            {
                "op": "run_metadata",
                "status": "pass",
                "device": args.device,
                "socket": args.socket,
                "sizes": sizes,
                "strategies": strategies,
                "warmups": args.warmups,
                "repeats": args.repeats,
                "acl_lib": acl_lib_path(),
                "hbm_va_owner": "benchmark_process_service_owned_physical_hbm",
            }
        )
        for size in sizes:
            for strategy in strategies:
                print(f"UFLOW_E03_BENCH_START strategy={strategy} size={size}", flush=True)
                bench_strategy_size(
                    strategy=strategy,
                    size=size,
                    args=args,
                    acl=acl,
                    writer=writer,
                )
        recommendation = summarize_recommendation(writer.rows)
        with open(args.run_dir / "recommendation.json", "w", encoding="utf-8") as f:
            json.dump(recommendation, f, indent=2, sort_keys=True)
        writer.add({"op": "recommendation", "status": "pass", **recommendation})
        writer.write_setup_summary()
        writer.write_aggregate()
        print(
            "UFLOW_E03_RECOMMENDATION "
            f"default_shared_ddr={recommendation['default_shared_ddr']} "
            f"default_transfer_path={recommendation['default_transfer_path']} "
            f"fallback_path={recommendation['fallback_path']}",
            flush=True,
        )
        print(
            "UFLOW_E03_DDR_STRATEGY_BENCH_PASS "
            f"run_dir={args.run_dir} device={args.device} sizes={','.join(str(s) for s in sizes)}",
            flush=True,
        )
    finally:
        writer.write_summary()
        writer.write_setup_summary()
        writer.write_aggregate()
        writer.close()
        try:
            acl.backend_finalize()
        except Exception:
            pass


def parse_args() -> argparse.Namespace:
    argv = sys.argv[1:]
    if not argv or argv[0] not in {"bench", "share-child"}:
        argv = ["bench", *argv]

    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd")

    bench = sub.add_parser("bench")
    bench.add_argument("--socket", default=os.environ.get("UF_SOCKET", "/tmp/uflow_e03.sock"))
    bench.add_argument("--device", type=int, default=int(os.environ.get("UF_TARGET_DEVICE", "0")))
    bench.add_argument("--sizes", default=os.environ.get("UF_E03_SIZES", "4k,64k,1m,16m"))
    bench.add_argument("--run-dir", type=Path, default=Path(os.environ.get("UF_E03_RUN_DIR", "/tmp/proj_output/phasee03")))
    bench.add_argument("--strategies", default="", help="Comma-separated strategy subset; default runs the PhaseE-03 matrix.")
    bench.add_argument("--warmups", type=int, default=int(os.environ.get("UF_E03_WARMUPS", "0")))
    bench.add_argument("--repeats", type=int, default=int(os.environ.get("UF_E03_REPEATS", "1")))
    bench.add_argument(
        "--focused-size-sweep",
        action="store_true",
        help="Use PhaseE-03 focused DDR sweep defaults: memfd, memfd+pinned staging, registered memfd.",
    )

    child = sub.add_parser("share-child")
    child.add_argument("--kind", required=True, choices=["path", "fd", "posix_shm"])
    child.add_argument("--path", default="")
    child.add_argument("--fd", type=int, default=-1)
    child.add_argument("--name", default="")
    child.add_argument("--bytes", type=int, required=True)
    child.add_argument("--overlay-offset", type=int, required=True)
    child.add_argument("--overlay-bytes", type=int, required=True)
    child.add_argument("--pattern-offset", type=int, required=True)
    child.add_argument("--overlay-pattern-offset", type=int, required=True)

    args = parser.parse_args(argv)
    if args.cmd == "bench":
        if args.focused_size_sweep and not args.strategies:
            args.strategies = FOCUSED_SIZE_SWEEP_STRATEGIES
        elif isinstance(args.strategies, str) and args.strategies:
            args.strategies = [item.strip() for item in args.strategies.split(",") if item.strip()]
        else:
            args.strategies = []
    return args


def main() -> None:
    args = parse_args()
    if args.cmd == "share-child":
        path_share_child(args)
    else:
        bench_main(args)


if __name__ == "__main__":
    main()
