#!/usr/bin/env python3
from __future__ import annotations

import argparse
import faulthandler
import os
import sys
from pathlib import Path

import pypto.language as pl
import torch
from pypto.ir.distributed_compiled_program import DistributedCompiledProgram, DistributedConfig
from pypto.runtime import DistributedWorker, RunConfig


REPO_ROOT = Path(__file__).resolve().parents[2]
SDK_ROOT = REPO_ROOT / "sdk" / "python"
if str(SDK_ROOT) not in sys.path:
    sys.path.insert(0, str(SDK_ROOT))

from uflow import MANDATORY_HBM_HINT, UFlowClient  # noqa: E402


TILE = 128


def _log(message: str) -> None:
    print(f"[phasea09-g1] {message}", flush=True)


@pl.jit.incore
def write_kv(prompt: pl.Tensor, kv: pl.Out[pl.Tensor]):
    tile_p = pl.load(prompt, [0, 0], [TILE, TILE])
    tile_kv = pl.add(tile_p, tile_p)
    return pl.store(tile_kv, [0, 0], kv)


@pl.jit
def prefill_chip(prompt: pl.Tensor, kv: pl.Out[pl.Tensor]):
    out_kv = write_kv(prompt, kv)
    return out_kv


@pl.jit.host
def prefill(prompt: pl.Tensor, kv: pl.Out[pl.Tensor]):
    out_kv = prefill_chip(prompt, kv)
    return out_kv


@pl.jit.incore
def read_kv(token: pl.Tensor, kv: pl.Tensor, logits: pl.Out[pl.Tensor]):
    tile_t = pl.load(token, [0, 0], [TILE, TILE])
    tile_kv = pl.load(kv, [0, 0], [TILE, TILE])
    tile_o = pl.add(tile_t, tile_kv)
    return pl.store(tile_o, [0, 0], logits)


@pl.jit
def decode_chip(token: pl.Tensor, kv: pl.Tensor, logits: pl.Out[pl.Tensor]):
    out = read_kv(token, kv, logits)
    return out


@pl.jit.host
def decode(token: pl.Tensor, kv: pl.Tensor, logits: pl.Out[pl.Tensor]):
    out = decode_chip(token, kv, logits)
    return out


def run(args: argparse.Namespace) -> None:
    if args.dump_after_s > 0:
        faulthandler.dump_traceback_later(args.dump_after_s, repeat=True)
    os.environ.setdefault("UF_ENABLE", "1")
    client = None
    hbm = None
    try:
        host_prompt = torch.full((TILE, TILE), 2.0, dtype=torch.float32).share_memory_()
        host_token = torch.zeros((TILE, TILE), dtype=torch.float32).share_memory_()
        host_logits = torch.zeros((TILE, TILE), dtype=torch.float32).share_memory_()
        kv_sample = torch.zeros((TILE, TILE), dtype=torch.float32)

        dc = DistributedConfig(
            device_ids=[args.device],
            num_sub_workers=0,
            block_dim=args.block_dim,
            aicpu_thread_num=args.aicpu_thread_num,
        )
        cfg = RunConfig(platform=args.platform, device_id=args.device, distributed_config=dc)
        _log("compile prefill")
        prefill_c = prefill.compile(host_prompt, kv_sample, config=cfg)
        _log("compile decode")
        decode_c = decode.compile(host_token, kv_sample, host_logits, config=cfg)
        if not isinstance(prefill_c, DistributedCompiledProgram):
            raise TypeError(f"prefill did not compile to DistributedCompiledProgram: {type(prefill_c).__name__}")
        if not isinstance(decode_c, DistributedCompiledProgram):
            raise TypeError(f"decode did not compile to DistributedCompiledProgram: {type(decode_c).__name__}")

        _log("start distributed worker")
        with DistributedWorker([prefill_c, decode_c]) as worker:
            _log("connect uflow")
            client = UFlowClient.from_env(
                device_id=args.device,
                client_role="phasea09-l3-device-tensor-probe",
                model_id="phasea09-l3-device-tensor-probe",
            )
            _log("allocate hbm object")
            hbm = client.allocate(
                name="phasea09.probe.kv_cache",
                role="kvcache",
                nbytes=int(kv_sample.nbytes),
                hint=MANDATORY_HBM_HINT,
                target=f"npu:{args.device}",
                shape=tuple(kv_sample.shape),
                dtype=kv_sample.dtype,
                immutable=False,
            )
            _log("import hbm object in worker")
            kv_cache = worker.import_hbm_object(
                object_id=int(hbm.object_id),
                nbytes=int(kv_sample.nbytes),
                shape=tuple(kv_sample.shape),
                dtype=kv_sample.dtype,
                device_id=args.device,
                model_id="phasea09-l3-device-tensor-probe",
                name=hbm.name,
                role=hbm.role,
                target=f"npu:{args.device}",
            )
            _log("run prefill")
            worker.run(prefill_c, host_prompt, kv_cache)
            for step in range(args.decode_steps):
                _log(f"run decode step={step}")
                host_token.fill_(float(step))
                host_logits.zero_()
                worker.run(decode_c, host_token, kv_cache, host_logits)
                expected = torch.full((TILE, TILE), float(step) + 4.0, dtype=torch.float32)
                torch.testing.assert_close(host_logits, expected, rtol=1e-5, atol=1e-5)
            worker.free_imported_hbm_tensor(kv_cache)

        print(
            "UFLOW_PHASEA09_L3_DEVICE_TENSOR_PROBE_PASS "
            f"device={args.device} platform={args.platform} worker_ptr=0x{kv_cache.data_ptr:x} "
            f"bytes={kv_cache.nbytes} decode_steps={args.decode_steps}",
            flush=True,
        )
    finally:
        if hbm is not None:
            hbm.release()
        if client is not None:
            client.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--device", type=int, default=int(os.environ.get("UF_TARGET_DEVICE", "0")))
    parser.add_argument("--platform", default=os.environ.get("UF_PYPTO_PLATFORM", "a2a3"))
    parser.add_argument("--block-dim", type=int, default=int(os.environ.get("UF_PYPTO_BLOCK_DIM", "3")))
    parser.add_argument("--aicpu-thread-num", type=int, default=int(os.environ.get("UF_PYPTO_AICPU_THREAD_NUM", "4")))
    parser.add_argument("--decode-steps", type=int, default=int(os.environ.get("UF_A09_PROBE_DECODE_STEPS", "3")))
    parser.add_argument("--dump-after-s", type=int, default=int(os.environ.get("UF_A09_PROBE_DUMP_AFTER_S", "60")))
    return parser.parse_args()


def main() -> None:
    run(parse_args())


if __name__ == "__main__":
    main()
