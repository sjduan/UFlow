from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from pathlib import Path
from typing import Any

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
        tensor = torch.from_numpy(pattern_array(chunk, seed, done))
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
        model_id="phaseb04",
    )


def event_row(direction: str, nbytes: int, mode: str, event: TransferEvent) -> dict[str, Any]:
    return {
        "direction": direction,
        "bytes": nbytes,
        "mode": mode,
        "status": event.status,
        "actual_path": event.actual_path,
        "actual_engine": event.actual_engine,
        "actual_bandwidth_gib_s": event.actual_bandwidth_gib_s,
        "actual_latency_us": event.actual_latency_us,
        "ssd_io_bandwidth_gib_s": event.ssd_io_bandwidth_gib_s,
        "ssd_io_wait_us": event.ssd_io_wait_us,
        "relay_stage_count": event.relay_stage_count,
        "relay_ddr_hbm_us": event.relay_ddr_hbm_us,
        "relay_total_us": event.relay_total_us,
        "direct_candidate": event.direct_candidate,
        "direct_kind": event.direct_kind,
        "direct_setup_us": event.direct_setup_us,
        "direct_register_us": event.direct_register_us,
        "direct_fadvise_us": event.direct_fadvise_us,
        "direct_readahead_us": event.direct_readahead_us,
        "direct_madvise_hugepage_us": event.direct_madvise_hugepage_us,
        "direct_madvise_willneed_us": event.direct_madvise_willneed_us,
        "direct_madvise_populate_us": event.direct_madvise_populate_us,
        "direct_pretouch_us": event.direct_pretouch_us,
        "direct_mlock_us": event.direct_mlock_us,
        "direct_acl_us": event.direct_acl_us,
        "direct_total_us": event.direct_total_us,
        "fallback_used": event.fallback_used,
        "fallback_reason": event.fallback_reason,
        "error_code": event.error_code,
        "error_message": event.error_message,
    }


def wait_transfer(uf: UFlowClient, src, dst, *, nbytes: int, mode: str) -> TransferEvent:
    plan = uf.plan_transfer(src=src, dst=dst, nbytes=nbytes, mode=mode)
    event = uf.submit_transfer(plan)
    final = uf.wait_event(event, timeout_ms=900_000)
    if final.status != "complete":
        raise AssertionError(f"transfer failed: {final}")
    if final.bytes_done != nbytes:
        raise AssertionError(f"transfer bytes mismatch: {final}")
    return final


def exercise_size(args: argparse.Namespace, nbytes: int, index: int, mode: str) -> list[dict[str, Any]]:
    uf = client(args, f"phaseb04-{mode}")
    ssd_src: SsdObject | None = None
    hbm_dst: HbmObject | None = None
    hbm_src: HbmObject | None = None
    ssd_dst: SsdObject | None = None
    rows: list[dict[str, Any]] = []
    try:
        ssd_src = uf.allocate_ssd(name=f"phaseb04.{mode}.ssd.src.{index}", nbytes=nbytes, mark_ready=False)
        hbm_dst = uf.allocate(name=f"phaseb04.{mode}.hbm.dst.{index}", nbytes=nbytes, shape=(nbytes,), dtype=torch.uint8, mark_ready=False)
        write_file_pattern(ssd_src.path, nbytes, seed=31 + index, chunk_bytes=args.chunk_bytes)
        uf.mark_ready(ssd_src)
        event = wait_transfer(uf, ssd_src, hbm_dst, nbytes=nbytes, mode=mode)
        verify_hbm_pattern(uf, hbm_dst, nbytes, seed=31 + index, chunk_bytes=args.chunk_bytes)
        rows.append(event_row("ssd_to_hbm", nbytes, mode, event))

        hbm_src = uf.allocate(name=f"phaseb04.{mode}.hbm.src.{index}", nbytes=nbytes, shape=(nbytes,), dtype=torch.uint8, mark_ready=False)
        ssd_dst = uf.allocate_ssd(name=f"phaseb04.{mode}.ssd.dst.{index}", nbytes=nbytes, mark_ready=False)
        write_hbm_pattern(uf, hbm_src, nbytes, seed=131 + index, chunk_bytes=args.chunk_bytes)
        uf.mark_ready(hbm_src)
        event = wait_transfer(uf, hbm_src, ssd_dst, nbytes=nbytes, mode=mode)
        verify_file_pattern(ssd_dst.path, nbytes, seed=131 + index, chunk_bytes=args.chunk_bytes)
        rows.append(event_row("hbm_to_ssd", nbytes, mode, event))
        print(
            "UFLOW_B04_CANDIDATE_PASS "
            f"mode={mode} bytes={nbytes} "
            f"ssd_to_hbm_path={rows[-2]['actual_path']} ssd_to_hbm_bw={rows[-2]['actual_bandwidth_gib_s']:.3f} "
            f"hbm_to_ssd_path={rows[-1]['actual_path']} hbm_to_ssd_bw={rows[-1]['actual_bandwidth_gib_s']:.3f}",
            flush=True,
        )
        return rows
    finally:
        for obj in (ssd_dst, hbm_src, hbm_dst, ssd_src):
            if obj is not None:
                obj.release()
        uf.close()


def write_outputs(output_dir: Path, rows: list[dict[str, Any]], capabilities: dict[str, str]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "capability_probe.json").write_text(json.dumps(capabilities, indent=2, sort_keys=True) + "\n")
    (output_dir / "candidate_summary.json").write_text(json.dumps(rows, indent=2, sort_keys=True) + "\n")
    if rows:
        with (output_dir / "candidate_summary.csv").open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
    lines = ["# PhaseB-04 SSD-HBM Direct Candidate Summary", ""]
    lines.append(f"- direct enabled: `{capabilities.get('ssd_hbm_direct_enabled', '')}`")
    lines.append(f"- configured candidate: `{capabilities.get('ssd_hbm_direct_candidate', '')}`")
    lines.append(f"- status: `{capabilities.get('ssd_hbm_direct_status', '')}`")
    lines.append("")
    for row in rows:
        lines.append(
            f"- `{row['mode']}` `{row['direction']}` `{row['bytes']}` bytes: "
            f"path=`{row['actual_path']}`, engine=`{row['actual_engine']}`, "
            f"bw={row['actual_bandwidth_gib_s']:.3f} GiB/s, "
            f"direct_acl_us={row['direct_acl_us']:.3f}, relay_total_us={row['relay_total_us']:.3f}"
        )
    (output_dir / "summary.md").write_text("\n".join(lines) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket", default=os.environ.get("UF_SOCKET", "/tmp/uflow.sock"))
    parser.add_argument("--device", type=int, default=int(os.environ.get("UF_TARGET_DEVICE", "0")))
    parser.add_argument("--sizes", default=DEFAULT_SIZES)
    parser.add_argument("--chunk-bytes", type=int, default=int(os.environ.get("UF_B04_TEST_CHUNK_BYTES", str(16 * 1024 * 1024))))
    parser.add_argument("--mode", default="ssd_hbm_direct", choices=("auto", "ssd_hbm_direct", "relay"))
    parser.add_argument("--include-relay-baseline", action="store_true")
    parser.add_argument("--output-dir", default=os.environ.get("UF_B04_OUTPUT_DIR", "/tmp/proj_output/phaseb04_ssd_hbm_direct"))
    args = parser.parse_args()

    uf = client(args, "phaseb04-capability")
    try:
        capabilities = uf.capabilities()
    finally:
        uf.close()

    rows: list[dict[str, Any]] = []
    for mode in ([args.mode, "relay"] if args.include_relay_baseline and args.mode != "relay" else [args.mode]):
        for index, nbytes in enumerate(parse_sizes(args.sizes)):
            rows.extend(exercise_size(args, nbytes, index, mode))
    write_outputs(Path(args.output_dir), rows, capabilities)
    print(f"UFLOW_B04_ALL_PASS output_dir={args.output_dir}", flush=True)


if __name__ == "__main__":
    main()
