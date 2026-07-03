#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import sys
import time
from typing import Any


DEFAULT_SOCKET = "/tmp/uflow_phasee04.sock"
DEFAULT_MODEL_ID = "phasee04-monitor"


def encode_kv(kv: dict[str, Any]) -> bytes:
    parts: list[str] = []
    for key, value in kv.items():
        text = str(value)
        if ";" in text or "=" in text:
            raise ValueError(f"UFlow value for {key!r} cannot contain ';' or '='")
        parts.append(f"{key}={text}")
    return (";".join(parts) + "\n").encode()


def decode_kv(line: bytes) -> dict[str, str]:
    out: dict[str, str] = {}
    for part in line.decode(errors="replace").strip().split(";"):
        if not part or "=" not in part:
            continue
        key, value = part.split("=", 1)
        out[key] = value
    return out


def request(socket_path: str, kv: dict[str, Any], timeout_s: float = 5.0) -> dict[str, str]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout_s)
        sock.connect(socket_path)
        sock.sendall(encode_kv(kv))
        with sock.makefile("rb") as reader:
            line = reader.readline()
    if not line:
        raise RuntimeError("empty daemon response")
    response = decode_kv(line)
    if response.get("status") != "ok":
        raise RuntimeError(response.get("detail", f"daemon error: {response}"))
    return response


def parse_size(value: str) -> int:
    text = value.strip().lower()
    units = {
        "k": 1024,
        "kb": 1024,
        "kib": 1024,
        "m": 1024 * 1024,
        "mb": 1024 * 1024,
        "mib": 1024 * 1024,
        "g": 1024 * 1024 * 1024,
        "gb": 1024 * 1024 * 1024,
        "gib": 1024 * 1024 * 1024,
    }
    for suffix, multiplier in sorted(units.items(), key=lambda item: len(item[0]), reverse=True):
        if text.endswith(suffix):
            return int(float(text[: -len(suffix)]) * multiplier)
    return int(text)


def print_event(name: str, payload: dict[str, Any]) -> None:
    print(json.dumps({"event": name, **payload}, ensure_ascii=False), flush=True)


def cleanup(socket_path: str, object_id: str, lease_id: str) -> None:
    if lease_id:
        try:
            request(socket_path, {"op": "CloseLease", "lease_id": lease_id})
            print_event("lease_closed", {"lease_id": lease_id})
        except Exception as exc:  # noqa: BLE001 - cleanup should be best effort.
            print_event("lease_close_failed", {"lease_id": lease_id, "error": str(exc)})
    if object_id:
        try:
            request(socket_path, {"op": "ReleaseDataObject", "object_id": object_id})
            print_event("object_released", {"object_id": object_id})
        except Exception as exc:  # noqa: BLE001 - cleanup should be best effort.
            print_event("object_release_failed", {"object_id": object_id, "error": str(exc)})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a PhaseE-04 demo DDR object for the UFlow monitor")
    parser.add_argument("--socket", default=os.environ.get("UF_SOCKET", DEFAULT_SOCKET))
    parser.add_argument("--model-id", default=os.environ.get("UFLOW_MONITOR_MODEL_ID", DEFAULT_MODEL_ID))
    parser.add_argument("--name", default="demo_ddr_256m")
    parser.add_argument("--bytes", dest="bytes_text", default="256MiB")
    parser.add_argument("--target", default=os.environ.get("UF_DDR_TARGET", "host:0"))
    parser.add_argument("--dtype", default="uint8")
    parser.add_argument("--hold", action="store_true", help="Keep the object alive until Ctrl-C")
    parser.add_argument("--immutable", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    size_bytes = parse_size(args.bytes_text)
    client = request(
        args.socket,
        {
            "op": "RegisterClient",
            "role": "phasee04-seed",
            "os_pid": os.getpid(),
            "bare_tgid": os.getpid(),
            "device_id": 0,
        },
    )
    client_id = client["client_id"]
    request(args.socket, {"op": "RegisterModel", "model_id": args.model_id})
    created = request(
        args.socket,
        {
            "op": "CreateDataObject",
            "client_id": client_id,
            "model_id": args.model_id,
            "name": args.name,
            "role": "user",
            "hint": "mandatory:ddr",
            "target": args.target,
            "nbytes": size_bytes,
            "shape": str(size_bytes),
            "dtype": args.dtype,
            "immutable": "1" if args.immutable else "0",
        },
    )
    object_id = created.get("object_id", "")
    lease_id = created.get("lease_id", "")
    print_event(
        "ddr_object_seeded",
        {
            "client_id": client_id,
            "object_id": object_id,
            "lease_id": lease_id,
            "model_id": args.model_id,
            "name": args.name,
            "bytes": size_bytes,
            "target": args.target,
            "ddr_path": created.get("ddr_path", ""),
            "hold": args.hold,
        },
    )

    should_stop = False

    def stop(_signum: int, _frame: object) -> None:
        nonlocal should_stop
        should_stop = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    if args.hold:
        print_event("holding", {"object_id": object_id, "message": "send SIGINT or SIGTERM to release"})
        while not should_stop:
            time.sleep(0.5)
    cleanup(args.socket, object_id, lease_id)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001 - command-line boundary.
        print(json.dumps({"event": "seed_failed", "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        sys.exit(1)
