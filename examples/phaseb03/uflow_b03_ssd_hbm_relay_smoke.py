from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import numpy as np
import torch


REPO_ROOT = Path(__file__).resolve().parents[2]
SDK_ROOT = REPO_ROOT / "sdk" / "python"
if str(SDK_ROOT) not in sys.path:
    sys.path.insert(0, str(SDK_ROOT))

from uflow import HbmObject, SsdObject, TransferEvent, UFlowClient  # noqa: E402


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


def pattern_array(nbytes: int, seed: int, absolute_offset: int = 0) -> np.ndarray:
    values = (np.arange(nbytes, dtype=np.uint32) + int(seed) + int(absolute_offset)) % 251
    return values.astype(np.uint8)


def write_file_pattern(path: str, nbytes: int, seed: int, *, chunk_bytes: int) -> None:
    with open(path, "r+b", buffering=0) as f:
        done = 0
        while done < nbytes:
            chunk = min(chunk_bytes, nbytes - done)
            f.seek(done)
            f.write(pattern_array(chunk, seed, done).tobytes())
            done += chunk


def verify_file_pattern(path: str, nbytes: int, seed: int, *, chunk_bytes: int) -> None:
    with open(path, "rb", buffering=0) as f:
        done = 0
        while done < nbytes:
            chunk = min(chunk_bytes, nbytes - done)
            f.seek(done)
            actual = f.read(chunk)
            expected = pattern_array(chunk, seed, done).tobytes()
            if actual != expected:
                raise AssertionError(f"file pattern mismatch at offset={done}")
            done += chunk


def write_hbm_pattern(uf: UFlowClient, hbm: HbmObject, nbytes: int, seed: int, *, chunk_bytes: int) -> None:
    done = 0
    while done < nbytes:
        chunk = min(chunk_bytes, nbytes - done)
        arr = pattern_array(chunk, seed, done)
        tensor = torch.from_numpy(arr)
        uf.copy_to_device(hbm, tensor, offset_bytes=done)
        done += chunk


def verify_hbm_pattern(uf: UFlowClient, hbm: HbmObject, nbytes: int, seed: int, *, chunk_bytes: int) -> None:
    done = 0
    while done < nbytes:
        chunk = min(chunk_bytes, nbytes - done)
        tensor = torch.empty((chunk,), dtype=torch.uint8)
        uf.copy_from_device(hbm, tensor, offset_bytes=done)
        actual = tensor.numpy().tobytes()
        expected = pattern_array(chunk, seed, done).tobytes()
        if actual != expected:
            raise AssertionError(f"HBM pattern mismatch at offset={done}")
        done += chunk


def client(args: argparse.Namespace, role: str) -> UFlowClient:
    return UFlowClient(
        enabled=True,
        socket_path=args.socket,
        device_id=args.device,
        acl_lib_path=os.environ.get("UF_ACL_LIB", "/home/sj/git/data-service/build/lib/libuf_acl_shim.so"),
        client_role=role,
        model_id="phaseb03",
    )


def wait_transfer(uf: UFlowClient, src, dst, *, nbytes: int, mode: str) -> TransferEvent:
    plan = uf.plan_transfer(src=src, dst=dst, nbytes=nbytes, mode=mode)
    event = uf.submit_transfer(plan)
    final = uf.wait_event(event, timeout_ms=600_000)
    if final.status != "complete":
        raise AssertionError(f"transfer failed: {final}")
    if final.bytes_done != nbytes or final.ssd_io_bytes != nbytes:
        raise AssertionError(f"transfer bytes mismatch: {final}")
    if final.relay_stage_count != 2:
        raise AssertionError(f"relay_stage_count mismatch: {final}")
    if final.relay_ddr_hbm_us <= 0 or final.relay_total_us <= 0:
        raise AssertionError(f"relay timing missing: {final}")
    return final


def exercise_size(args: argparse.Namespace, nbytes: int, index: int) -> None:
    uf = client(args, "phaseb03")
    ssd_src: SsdObject | None = None
    hbm_dst: HbmObject | None = None
    hbm_src: HbmObject | None = None
    ssd_dst: SsdObject | None = None
    try:
        ssd_src = uf.allocate_ssd(name=f"phaseb03.ssd.src.{index}", nbytes=nbytes, mark_ready=False)
        hbm_dst = uf.allocate(name=f"phaseb03.hbm.dst.{index}", nbytes=nbytes, shape=(nbytes,), dtype=torch.uint8, mark_ready=False)
        write_file_pattern(ssd_src.path, nbytes, seed=19 + index, chunk_bytes=args.chunk_bytes)
        uf.mark_ready(ssd_src)
        event = wait_transfer(uf, ssd_src, hbm_dst, nbytes=nbytes, mode="auto")
        if event.actual_path != "ssd_to_hbm_via_ddr" or event.actual_engine != "ssd_hbm_relay_ddr":
            raise AssertionError(f"unexpected SSD->HBM event path/engine: {event}")
        verify_hbm_pattern(uf, hbm_dst, nbytes, seed=19 + index, chunk_bytes=args.chunk_bytes)

        hbm_src = uf.allocate(name=f"phaseb03.hbm.src.{index}", nbytes=nbytes, shape=(nbytes,), dtype=torch.uint8, mark_ready=False)
        ssd_dst = uf.allocate_ssd(name=f"phaseb03.ssd.dst.{index}", nbytes=nbytes, mark_ready=False)
        write_hbm_pattern(uf, hbm_src, nbytes, seed=113 + index, chunk_bytes=args.chunk_bytes)
        uf.mark_ready(hbm_src)
        event = wait_transfer(uf, hbm_src, ssd_dst, nbytes=nbytes, mode="relay")
        if event.actual_path != "hbm_to_ssd_via_ddr" or event.actual_engine != "ssd_hbm_relay_ddr":
            raise AssertionError(f"unexpected HBM->SSD event path/engine: {event}")
        verify_file_pattern(ssd_dst.path, nbytes, seed=113 + index, chunk_bytes=args.chunk_bytes)
        stats = uf.stats()
        if int(stats.get("ssd_read_bytes", "0")) < nbytes or int(stats.get("ssd_write_bytes", "0")) < nbytes:
            raise AssertionError(f"SSD relay stats did not increase as expected: {stats}")
        print(
            "UFLOW_B03_SSD_HBM_RELAY_PASS "
            f"bytes={nbytes} ssd_io_bw={event.ssd_io_bandwidth_gib_s:.3f} "
            f"relay_ddr_hbm_us={event.relay_ddr_hbm_us:.3f} relay_total_us={event.relay_total_us:.3f}",
            flush=True,
        )
    finally:
        for obj in (ssd_dst, hbm_src, hbm_dst, ssd_src):
            if obj is not None:
                obj.release()
        uf.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket", default=os.environ.get("UF_SOCKET", "/tmp/uflow.sock"))
    parser.add_argument("--device", type=int, default=int(os.environ.get("UF_TARGET_DEVICE", "0")))
    parser.add_argument("--sizes", default=DEFAULT_SIZES)
    parser.add_argument("--chunk-bytes", type=int, default=int(os.environ.get("UF_B03_TEST_CHUNK_BYTES", str(16 * 1024 * 1024))))
    args = parser.parse_args()
    for index, nbytes in enumerate(parse_sizes(args.sizes)):
        exercise_size(args, nbytes, index)
    print("UFLOW_B03_ALL_PASS", flush=True)


if __name__ == "__main__":
    main()
