from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import numpy as np


REPO_ROOT = Path(__file__).resolve().parents[2]
SDK_ROOT = REPO_ROOT / "sdk" / "python"
if str(SDK_ROOT) not in sys.path:
    sys.path.insert(0, str(SDK_ROOT))

from uflow import DdrObject, SsdObject, TransferEvent, UFlowClient  # noqa: E402


DEFAULT_SIZES = "4MiB,16MiB,32MiB,64MiB,128MiB,256MiB,512MiB,1024MiB,2048MiB"


def parse_size(text: str) -> int:
    value = text.strip()
    lower = value.lower()
    scale = 1
    for suffix, factor in (("gib", 1024**3), ("gb", 1024**3), ("mib", 1024**2), ("mb", 1024**2), ("kib", 1024), ("kb", 1024)):
        if lower.endswith(suffix):
            scale = factor
            value = value[: -len(suffix)]
            break
    return int(value) * scale


def parse_sizes(text: str) -> list[int]:
    return [parse_size(item) for item in text.split(",") if item.strip()]


def pattern_chunk(nbytes: int, seed: int, absolute_offset: int = 0) -> bytes:
    values = (np.arange(nbytes, dtype=np.uint32) + int(seed) + int(absolute_offset)) % 251
    return values.astype(np.uint8).tobytes()


def write_file_pattern(path: str, nbytes: int, seed: int, *, offset: int = 0, chunk_bytes: int) -> None:
    with open(path, "r+b", buffering=0) as f:
        done = 0
        while done < nbytes:
            chunk = min(chunk_bytes, nbytes - done)
            f.seek(offset + done)
            f.write(pattern_chunk(chunk, seed, absolute_offset=done))
            done += chunk


def verify_file_pattern(path: str, nbytes: int, seed: int, *, offset: int = 0, chunk_bytes: int) -> None:
    with open(path, "rb", buffering=0) as f:
        done = 0
        while done < nbytes:
            chunk = min(chunk_bytes, nbytes - done)
            f.seek(offset + done)
            actual = f.read(chunk)
            expected = pattern_chunk(chunk, seed, absolute_offset=done)
            if actual != expected:
                raise AssertionError(f"file pattern mismatch at offset={offset + done}")
            done += chunk


def write_ddr_pattern(ddr: DdrObject, nbytes: int, seed: int, *, offset: int = 0, chunk_bytes: int) -> None:
    view = ddr.as_memoryview()
    try:
        done = 0
        while done < nbytes:
            chunk = min(chunk_bytes, nbytes - done)
            view[offset + done : offset + done + chunk] = pattern_chunk(chunk, seed, absolute_offset=done)
            done += chunk
    finally:
        del view


def verify_ddr_pattern(ddr: DdrObject, nbytes: int, seed: int, *, offset: int = 0, chunk_bytes: int) -> None:
    view = ddr.as_memoryview()
    try:
        done = 0
        while done < nbytes:
            chunk = min(chunk_bytes, nbytes - done)
            actual = bytes(view[offset + done : offset + done + chunk])
            expected = pattern_chunk(chunk, seed, absolute_offset=done)
            if actual != expected:
                raise AssertionError(f"DDR pattern mismatch at offset={offset + done}")
            done += chunk
    finally:
        del view


def client(args: argparse.Namespace, role: str) -> UFlowClient:
    return UFlowClient(
        enabled=True,
        socket_path=args.socket,
        device_id=args.device,
        acl_lib_path=os.environ.get("UF_ACL_LIB", "/home/sj/git/data-service/build/lib/libuf_acl_shim.so"),
        client_role=role,
        model_id="phaseb02",
    )


def wait_transfer(uf: UFlowClient, src, dst, *, nbytes: int, mode: str = "auto", src_offset: int = 0, dst_offset: int = 0) -> TransferEvent:
    cost = uf.estimate_cost(src=src, dst=dst, nbytes=nbytes, mode=mode, src_offset_bytes=src_offset, dst_offset_bytes=dst_offset)
    plan = uf.plan_transfer(src=src, dst=dst, nbytes=nbytes, mode=mode, src_offset_bytes=src_offset, dst_offset_bytes=dst_offset)
    if plan.cost.effort != cost.effort:
        raise AssertionError("EstimateCost and PlanTransfer returned different effort")
    event = uf.submit_transfer(plan)
    final = uf.wait_event(event, timeout_ms=300_000)
    if final.status != "complete":
        raise AssertionError(f"transfer failed: {final}")
    if final.bytes_done != nbytes:
        raise AssertionError(f"bytes_done={final.bytes_done}, expected={nbytes}")
    if final.ssd_io_bytes != nbytes:
        raise AssertionError(f"ssd_io_bytes={final.ssd_io_bytes}, expected={nbytes}")
    return final


def exercise_size(args: argparse.Namespace, nbytes: int, index: int) -> None:
    uf = client(args, "phaseb02")
    ssd_src: SsdObject | None = None
    ddr_dst: DdrObject | None = None
    ddr_src: DdrObject | None = None
    ssd_dst: SsdObject | None = None
    try:
        ssd_src = uf.allocate_ssd(name=f"phaseb02.ssd.src.{index}", nbytes=nbytes, mark_ready=False)
        ddr_dst = uf.allocate_ddr(name=f"phaseb02.ddr.dst.{index}", nbytes=nbytes, mark_ready=False)
        write_file_pattern(ssd_src.path, nbytes, seed=11 + index, chunk_bytes=args.chunk_bytes)
        uf.mark_ready(ssd_src)
        event = wait_transfer(uf, ssd_src, ddr_dst, nbytes=nbytes)
        if event.actual_path != "ssd_to_ddr" or event.actual_engine != "ssd_buffered_pread":
            raise AssertionError(f"unexpected SSD->DDR event path/engine: {event}")
        verify_ddr_pattern(ddr_dst, nbytes, seed=11 + index, chunk_bytes=args.chunk_bytes)

        ddr_src = uf.allocate_ddr(name=f"phaseb02.ddr.src.{index}", nbytes=nbytes, mark_ready=False)
        ssd_dst = uf.allocate_ssd(name=f"phaseb02.ssd.dst.{index}", nbytes=nbytes, mark_ready=False)
        write_ddr_pattern(ddr_src, nbytes, seed=71 + index, chunk_bytes=args.chunk_bytes)
        uf.mark_ready(ddr_src)
        event = wait_transfer(uf, ddr_src, ssd_dst, nbytes=nbytes)
        if event.actual_path != "ddr_to_ssd" or event.actual_engine != "ssd_buffered_pwrite":
            raise AssertionError(f"unexpected DDR->SSD event path/engine: {event}")
        verify_file_pattern(ssd_dst.path, nbytes, seed=71 + index, chunk_bytes=args.chunk_bytes)

        stats = uf.stats()
        if int(stats.get("ssd_read_bytes", "0")) < nbytes or int(stats.get("ssd_write_bytes", "0")) < nbytes:
            raise AssertionError(f"SSD stats did not increase as expected: {stats}")
        print(
            "UFLOW_B02_SSD_DDR_PASS "
            f"bytes={nbytes} ssd_to_ddr_bw={event.ssd_io_bandwidth_gib_s:.3f} "
            f"read_bytes={stats.get('ssd_read_bytes')} write_bytes={stats.get('ssd_write_bytes')}",
            flush=True,
        )
    finally:
        for obj in (ssd_dst, ddr_src, ddr_dst, ssd_src):
            if obj is not None:
                obj.release()
        uf.close()


def offset_case(args: argparse.Namespace) -> None:
    uf = client(args, "phaseb02-offset")
    ssd = None
    ddr = None
    try:
        nbytes = 3 * 1024 * 1024 + 123
        src_offset = 4096
        dst_offset = 8192
        ssd = uf.allocate_ssd(name="phaseb02.offset.ssd", nbytes=nbytes + src_offset, mark_ready=False)
        ddr = uf.allocate_ddr(name="phaseb02.offset.ddr", nbytes=nbytes + dst_offset, mark_ready=False)
        write_file_pattern(ssd.path, nbytes, seed=203, offset=src_offset, chunk_bytes=args.chunk_bytes)
        uf.mark_ready(ssd)
        event = wait_transfer(uf, ssd, ddr, nbytes=nbytes, src_offset=src_offset, dst_offset=dst_offset)
        if event.ssd_read_bytes != nbytes:
            raise AssertionError(f"offset SSD read bytes mismatch: {event}")
        verify_ddr_pattern(ddr, nbytes, seed=203, offset=dst_offset, chunk_bytes=args.chunk_bytes)
        print("UFLOW_B02_OFFSET_PASS", flush=True)
    finally:
        if ddr is not None:
            ddr.release()
        if ssd is not None:
            ssd.release()
        uf.close()


def unsupported_mode_case(args: argparse.Namespace) -> None:
    uf = client(args, "phaseb02-unsupported")
    ssd = None
    ddr = None
    try:
        ssd = uf.allocate_ssd(name="phaseb02.unsupported.ssd", nbytes=4096)
        ddr = uf.allocate_ddr(name="phaseb02.unsupported.ddr", nbytes=4096)
        try:
            uf.plan_transfer(src=ssd, dst=ddr, nbytes=4096, mode="ssd_mmap")
        except Exception as exc:  # noqa: BLE001
            print(f"UFLOW_B02_UNSUPPORTED_MODE_PASS detail={exc}", flush=True)
            return
        raise AssertionError("mode=ssd_mmap unexpectedly planned successfully")
    finally:
        if ddr is not None:
            ddr.release()
        if ssd is not None:
            ssd.release()
        uf.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket", default=os.environ.get("UF_SOCKET", "/tmp/uflow.sock"))
    parser.add_argument("--device", type=int, default=int(os.environ.get("UF_TARGET_DEVICE", "0")))
    parser.add_argument("--sizes", default=DEFAULT_SIZES)
    parser.add_argument("--chunk-bytes", type=int, default=int(os.environ.get("UF_B02_TEST_CHUNK_BYTES", str(4 * 1024 * 1024))))
    args = parser.parse_args()
    for index, nbytes in enumerate(parse_sizes(args.sizes)):
        exercise_size(args, nbytes, index)
    offset_case(args)
    unsupported_mode_case(args)
    print("UFLOW_B02_ALL_PASS", flush=True)


if __name__ == "__main__":
    main()
