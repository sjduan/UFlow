from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

import torch


REPO_ROOT = Path(__file__).resolve().parents[2]
SDK_ROOT = REPO_ROOT / "sdk" / "python"
if str(SDK_ROOT) not in sys.path:
    sys.path.insert(0, str(SDK_ROOT))

from uflow import UFlowClient  # noqa: E402


def parse_size(text: str) -> int:
    value = text.strip()
    lower = value.lower()
    for suffix, scale in (
        ("gib", 1024**3),
        ("gb", 1024**3),
        ("mib", 1024**2),
        ("mb", 1024**2),
        ("kib", 1024),
        ("kb", 1024),
    ):
        if lower.endswith(suffix):
            return int(value[: -len(suffix)]) * scale
    return int(value)


def parse_sizes(text: str) -> list[int]:
    return [parse_size(item) for item in text.split(",") if item.strip()]


def new_client(args: argparse.Namespace, role: str) -> UFlowClient:
    return UFlowClient(
        enabled=True,
        socket_path=args.socket,
        device_id=args.device,
        acl_lib_path=os.environ.get(
            "UF_ACL_LIB", "/home/sj/git/data-service/build/lib/libuf_acl_shim.so"
        ),
        client_role=role,
        model_id="phasee06-trace-v2",
    )


def allocate(uf: UFlowClient, medium: str, name: str, nbytes: int, *, ready: bool):
    if medium == "hbm":
        return uf.allocate(
            name=name,
            nbytes=nbytes,
            shape=(nbytes,),
            dtype=torch.uint8,
            mark_ready=ready,
        )
    if medium == "ddr":
        return uf.allocate_ddr(name=name, nbytes=nbytes, mark_ready=ready)
    if medium == "ssd":
        return uf.allocate_ssd(name=name, nbytes=nbytes, mark_ready=ready)
    raise ValueError(f"unknown medium {medium}")


def transfer_row(
    case: str,
    size: int,
    mode: str,
    iteration: int,
    warmup: bool,
    plan,
    event,
    client_wall_us: float,
) -> dict[str, Any]:
    row = asdict(event)
    row.update(
        {
            "case": case,
            "size": size,
            "mode": mode,
            "iteration": iteration,
            "warmup": warmup,
            "requested_mode": getattr(plan, "requested_mode", mode),
            "planned_path": plan.path,
            "planned_engine": plan.engine,
            "client_wall_us": client_wall_us,
        }
    )
    return row


def run_pair(
    uf: UFlowClient,
    rows: list[dict[str, Any]],
    *,
    case: str,
    src_medium: str,
    dst_medium: str,
    size: int,
    mode: str,
    warmup: int,
    repeat: int,
    expected_status: str = "complete",
) -> None:
    src = None
    dst = None
    try:
        src = allocate(uf, src_medium, f"{case}.src.{size}", size, ready=True)
        dst = allocate(uf, dst_medium, f"{case}.dst.{size}", size, ready=False)
        for iteration in range(warmup + repeat):
            plan = uf.plan_transfer(src=src, dst=dst, nbytes=size, mode=mode)
            if getattr(plan, "requested_mode", mode) != mode:
                raise AssertionError(
                    f"{case}: requested_mode={getattr(plan, 'requested_mode', mode)!r}, "
                    f"expected {mode!r}"
                )
            client_started = time.perf_counter_ns()
            submitted = uf.submit_transfer(plan)
            final = uf.wait_event(submitted, timeout_ms=900_000)
            client_wall_us = (time.perf_counter_ns() - client_started) / 1_000.0
            if final.status != expected_status:
                raise AssertionError(
                    f"{case}: event {final.event_id} status={final.status}, "
                    f"expected={expected_status}, error={final.error_message}"
                )
            if expected_status == "complete" and final.bytes_done != size:
                raise AssertionError(
                    f"{case}: event {final.event_id} bytes_done={final.bytes_done}, expected={size}"
                )
            row = transfer_row(
                case,
                size,
                mode,
                iteration,
                iteration < warmup,
                plan,
                final,
                client_wall_us,
            )
            rows.append(row)
            print(
                "UFLOW_E06_CASE "
                f"case={case} event_id={final.event_id} size={size} mode={mode} "
                f"status={final.status} planned_path={plan.path} "
                f"actual_path={final.actual_path} actual_engine={final.actual_engine} "
                f"fallback={final.fallback_used}",
                flush=True,
            )
    finally:
        if dst is not None:
            dst.release()
        if src is not None:
            src.release()


def run_regular(args: argparse.Namespace, uf: UFlowClient, rows: list[dict[str, Any]]) -> None:
    for size in parse_sizes(args.sizes):
        for mode, label in (
            ("auto", "direct_auto"),
            ("direct_async", "direct_explicit"),
            ("pinned_async", "pinned"),
        ):
            run_pair(
                uf,
                rows,
                case=f"hbm_ddr_{label}_h2d_{size}",
                src_medium="ddr",
                dst_medium="hbm",
                size=size,
                mode=mode,
                warmup=args.warmup,
                repeat=args.repeat,
            )
            run_pair(
                uf,
                rows,
                case=f"hbm_ddr_{label}_d2h_{size}",
                src_medium="hbm",
                dst_medium="ddr",
                size=size,
                mode=mode,
                warmup=args.warmup,
                repeat=args.repeat,
            )

        run_pair(
            uf,
            rows,
            case=f"ssd_ddr_read_{size}",
            src_medium="ssd",
            dst_medium="ddr",
            size=size,
            mode="auto",
            warmup=args.warmup,
            repeat=args.repeat,
        )
        run_pair(
            uf,
            rows,
            case=f"ssd_ddr_write_{size}",
            src_medium="ddr",
            dst_medium="ssd",
            size=size,
            mode="auto",
            warmup=args.warmup,
            repeat=args.repeat,
        )
        run_pair(
            uf,
            rows,
            case=f"ssd_hbm_relay_h2d_{size}",
            src_medium="ssd",
            dst_medium="hbm",
            size=size,
            mode="relay",
            warmup=args.warmup,
            repeat=args.repeat,
        )
        run_pair(
            uf,
            rows,
            case=f"ssd_hbm_relay_d2h_{size}",
            src_medium="hbm",
            dst_medium="ssd",
            size=size,
            mode="relay",
            warmup=args.warmup,
            repeat=args.repeat,
        )

    for size in parse_sizes(args.direct_sizes):
        for mode, label in (("ssd_hbm_direct", "explicit"), ("auto", "auto")):
            run_pair(
                uf,
                rows,
                case=f"ssd_hbm_direct_{label}_h2d_{size}",
                src_medium="ssd",
                dst_medium="hbm",
                size=size,
                mode=mode,
                warmup=args.warmup,
                repeat=args.repeat,
            )
            run_pair(
                uf,
                rows,
                case=f"ssd_hbm_direct_{label}_d2h_{size}",
                src_medium="hbm",
                dst_medium="ssd",
                size=size,
                mode=mode,
                warmup=args.warmup,
                repeat=args.repeat,
            )


def run_fallback(args: argparse.Namespace, uf: UFlowClient, rows: list[dict[str, Any]]) -> None:
    size = parse_sizes(args.direct_sizes)[-1]
    for src_medium, dst_medium, direction in (
        ("ssd", "hbm", "h2d"),
        ("hbm", "ssd", "d2h"),
    ):
        run_pair(
            uf,
            rows,
            case=f"runtime_fallback_{direction}_{size}",
            src_medium=src_medium,
            dst_medium=dst_medium,
            size=size,
            mode="auto",
            warmup=args.warmup,
            repeat=args.repeat,
        )
        run_pair(
            uf,
            rows,
            case=f"explicit_direct_failure_{direction}_{size}",
            src_medium=src_medium,
            dst_medium=dst_medium,
            size=size,
            mode="ssd_hbm_direct",
            warmup=0,
            repeat=1,
            expected_status="failed",
        )


def wait_for_jobs(uf: UFlowClient, completed_before: int, expected_delta: int) -> None:
    deadline = time.monotonic() + 900
    while time.monotonic() < deadline:
        stats = uf.stats()
        completed = int(stats.get("direct_transfer_completed_jobs", "0"))
        if completed >= completed_before + expected_delta:
            return
        time.sleep(0.05)
    raise TimeoutError("timed out waiting for transfer worker drain")


def run_cancel(args: argparse.Namespace, uf: UFlowClient, rows: list[dict[str, Any]]) -> None:
    size = parse_sizes(args.direct_sizes)[-1]
    src1 = dst1 = src2 = dst2 = None
    try:
        src1 = allocate(uf, "ssd", "cancel.blocker.src", size, ready=True)
        dst1 = allocate(uf, "hbm", "cancel.blocker.dst", size, ready=False)
        src2 = allocate(uf, "ssd", "cancel.target.src", size, ready=True)
        dst2 = allocate(uf, "hbm", "cancel.target.dst", size, ready=False)
        completed_before = int(uf.stats().get("direct_transfer_completed_jobs", "0"))
        plan1 = uf.plan_transfer(src=src1, dst=dst1, nbytes=size, mode="relay")
        plan2 = uf.plan_transfer(src=src2, dst=dst2, nbytes=size, mode="relay")
        event1 = uf.submit_transfer(plan1)
        event2 = uf.submit_transfer(plan2)
        cancelled = uf.cancel_event(event2)
        if cancelled.status != "cancelled":
            raise AssertionError(f"cancel returned status={cancelled.status}")
        final1 = uf.wait_event(event1, timeout_ms=900_000)
        if final1.status != "complete":
            raise AssertionError(f"cancel blocker failed: {final1}")
        wait_for_jobs(uf, completed_before, 2)
        final2 = uf.poll_event(event2)
        if final2.status != "cancelled":
            raise AssertionError(f"cancel target ended status={final2.status}")
        rows.append(
            transfer_row("cancel_blocker", size, "relay", 0, False, plan1, final1, 0.0)
        )
        rows.append(
            transfer_row("cancel_target", size, "relay", 0, False, plan2, final2, 0.0)
        )
    finally:
        for obj in (dst2, src2, dst1, src1):
            if obj is not None:
                obj.release()


def run_incomplete(args: argparse.Namespace, uf: UFlowClient, rows: list[dict[str, Any]]) -> None:
    size = parse_sizes(args.direct_sizes)[-1]
    src = dst = None
    try:
        src = allocate(uf, "ssd", "incomplete.src", size, ready=True)
        dst = allocate(uf, "hbm", "incomplete.dst", size, ready=False)
        plan = uf.plan_transfer(src=src, dst=dst, nbytes=size, mode="relay")
        submitted = uf.submit_transfer(plan)
        stop = uf._request(op="StopTrace", flush=1)
        args.trace_stopped = True
        final = uf.wait_event(submitted, timeout_ms=900_000)
        rows.append(
            transfer_row("trace_stop_incomplete", size, "relay", 0, False, plan, final, 0.0)
        )
        args.trace_control["stop"] = stop
    finally:
        if dst is not None:
            dst.release()
        if src is not None:
            src.release()


def run_drop(args: argparse.Namespace, uf: UFlowClient, rows: list[dict[str, Any]]) -> None:
    size = parse_sizes(args.sizes)[0]
    run_pair(
        uf,
        rows,
        case=f"bounded_drop_h2d_{size}",
        src_medium="ddr",
        dst_medium="hbm",
        size=size,
        mode="pinned_async",
        warmup=0,
        repeat=1,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket", default=os.environ.get("UF_SOCKET", "/tmp/uflow.sock"))
    parser.add_argument("--device", type=int, default=int(os.environ.get("UF_TARGET_DEVICE", "0")))
    parser.add_argument(
        "--profile",
        choices=("regular", "fallback", "cancel", "incomplete", "drop"),
        default="regular",
    )
    parser.add_argument("--sizes", default="64MiB,256MiB")
    parser.add_argument("--direct-sizes", default="256MiB")
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--trace-run-id", required=True)
    parser.add_argument("--trace-root", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--max-events", type=int, default=200_000)
    parser.add_argument("--trace-enabled", choices=("0", "1"), default="1")
    args = parser.parse_args()
    args.trace_stopped = False
    args.trace_control = {}

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    uf = new_client(args, f"phasee06-{args.profile}")
    rows: list[dict[str, Any]] = []
    try:
        if args.trace_enabled == "1":
            start = uf._request(
                op="StartTrace",
                run_id=args.trace_run_id,
                mode="full",
                max_events=args.max_events,
                output_dir=args.trace_root,
                include_args=1,
            )
            args.trace_control["start"] = start
        if args.profile == "regular":
            run_regular(args, uf, rows)
        elif args.profile == "fallback":
            run_fallback(args, uf, rows)
        elif args.profile == "cancel":
            run_cancel(args, uf, rows)
        elif args.profile == "incomplete":
            run_incomplete(args, uf, rows)
        else:
            run_drop(args, uf, rows)
    finally:
        if args.trace_enabled == "1" and not args.trace_stopped:
            try:
                args.trace_control["stop"] = uf._request(op="StopTrace", flush=1)
            except Exception as exc:  # noqa: BLE001
                args.trace_control["stop_error"] = str(exc)
        uf.close()
        (output_dir / "poll_events.json").write_text(
            json.dumps({"events": rows}, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        (output_dir / "trace_control.json").write_text(
            json.dumps(args.trace_control, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    print(
        f"UFLOW_E06_PROFILE_PASS profile={args.profile} transfers={len(rows)} "
        f"artifact_dir={Path(args.trace_root) / args.trace_run_id}",
        flush=True,
    )


if __name__ == "__main__":
    main()
