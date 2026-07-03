#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import socket
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse


DEFAULT_SOCKET = "/tmp/uflow_phasee04.sock"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8082
DEFAULT_MODEL_ID = "phasee04-monitor"
MAX_COMMAND_BODY_BYTES = 64 * 1024
CONTROL_OPS = {
    "RegisterClient",
    "RegisterModel",
    "CreateDataObject",
    "OpenDataObject",
    "ReleaseDataObject",
    "CloseLease",
    "DescribeObject",
    "GetStats",
    "GetModelObjects",
    "MarkReady",
    "MarkModified",
    "MarkDirty",
    "EstimateCost",
    "PlanTransfer",
    "SubmitTransfer",
    "PollEvent",
    "WaitEvent",
    "CancelEvent",
    "GetTraceStatus",
    "StartTrace",
    "StopTrace",
    "FlushTrace",
    "ClearTrace",
    "ExportTrace",
    "GetCapabilities",
    "ShutdownDaemon",
}
TRACE_ARTIFACT_FILES = {
    "trace_events.json": "application/json",
    "trace_summary.json": "application/json",
    "trace_summary.csv": "text/csv",
    "summary.md": "text/markdown",
}


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


def coerce_value(value: str) -> Any:
    if value == "":
        return ""
    if value in {"true", "false"}:
        return value == "true"
    if value.isdigit():
        try:
            return int(value)
        except ValueError:
            return value
    try:
        if "." in value and value.replace(".", "", 1).isdigit():
            return float(value)
    except ValueError:
        return value
    return value


def coerce_dict(kv: dict[str, str]) -> dict[str, Any]:
    return {key: coerce_value(value) for key, value in kv.items()}


def normalize_command_value(key: str, value: Any) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float, str)):
        text = str(value)
        if ";" in text or "=" in text:
            raise ValueError(f"UFlow command value for {key!r} cannot contain ';' or '='")
        return text
    raise ValueError(f"UFlow command value for {key!r} must be a string, number, or boolean")


def normalize_command(payload: Any) -> dict[str, str]:
    if isinstance(payload, dict) and "command" in payload and isinstance(payload["command"], dict):
        payload = payload["command"]
    if not isinstance(payload, dict):
        raise ValueError("command body must be a JSON object")
    command = {str(key): normalize_command_value(str(key), value) for key, value in payload.items()}
    op = command.get("op", "")
    if not op:
        raise ValueError("command requires op")
    if op not in CONTROL_OPS:
        raise ValueError(f"unsupported op {op}")
    return command


def request_daemon(socket_path: str, req: dict[str, Any], timeout_s: float = 2.0) -> dict[str, str]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout_s)
        sock.connect(socket_path)
        sock.sendall(encode_kv(req))
        with sock.makefile("rb") as reader:
            line = reader.readline()
    if not line:
        raise RuntimeError("empty daemon response")
    return decode_kv(line)


def parse_model_objects(kv: dict[str, str]) -> list[dict[str, Any]]:
    raw = kv.get("objects", "")
    if not raw:
        return []
    objects: list[dict[str, Any]] = []
    for item in raw.split(","):
        parts = item.split(":", 9)
        if len(parts) != 10:
            continue
        (
            object_id,
            name,
            role,
            state,
            requested_bytes,
            actual_bytes,
            shape,
            dtype,
            placement,
            address_tail,
        ) = parts
        tail_parts = address_tail.split(":", 2)
        if len(tail_parts) >= 2 and tail_parts[0] in {"host", "npu"}:
            target = f"{tail_parts[0]}:{tail_parts[1]}"
            ddr_path = tail_parts[2] if len(tail_parts) == 3 else ""
        else:
            target = address_tail
            ddr_path = ""
        objects.append(
            {
                "object_id": int(object_id) if object_id.isdigit() else object_id,
                "name": name,
                "role": role,
                "state": state,
                "requested_bytes": int(requested_bytes) if requested_bytes.isdigit() else requested_bytes,
                "actual_bytes": int(actual_bytes) if actual_bytes.isdigit() else actual_bytes,
                "shape": shape,
                "dtype": dtype,
                "placement": placement,
                "target": target,
                "ddr_path": ddr_path,
            }
        )
    return objects


class MonitorApiHandler(BaseHTTPRequestHandler):
    server_version = "UFlowMonitorAPI/0.1"

    def log_message(self, fmt: str, *args: Any) -> None:
        print(
            json.dumps(
                {
                    "event": "monitor_api_access",
                    "client": self.client_address[0],
                    "message": fmt % args,
                },
                separators=(",", ":"),
            ),
            flush=True,
        )

    @property
    def config(self) -> dict[str, Any]:
        return self.server.config  # type: ignore[attr-defined]

    def end_headers(self) -> None:
        origin = self.headers.get("Origin", "")
        if origin.startswith("http://127.0.0.1") or origin.startswith("http://localhost"):
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        super().end_headers()

    def send_json(self, status: int, payload: dict[str, Any]) -> None:
        data = json.dumps(payload, ensure_ascii=False, indent=2).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.end_headers()

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/healthz":
                self.handle_healthz()
            elif parsed.path == "/v1/config":
                self.handle_config()
            elif parsed.path == "/v1/stats":
                self.handle_stats()
            elif parsed.path == "/v1/observability":
                self.handle_observability()
            elif parsed.path == "/v1/capabilities":
                self.handle_capabilities()
            elif parsed.path == "/v1/trace/status":
                self.handle_trace_status()
            elif parsed.path == "/v1/trace/artifact":
                query = parse_qs(parsed.query)
                run_id = query.get("run_id", [""])[0]
                filename = query.get("file", ["trace_events.json"])[0]
                self.handle_trace_artifact(run_id, filename)
            elif parsed.path == "/v1/objects":
                query = parse_qs(parsed.query)
                model_id = query.get("model_id", [DEFAULT_MODEL_ID])[0]
                self.handle_objects(model_id)
            else:
                self.send_json(404, {"ok": False, "error": "not found", "path": parsed.path})
        except Exception as exc:  # noqa: BLE001 - this is the HTTP boundary.
            self.send_json(
                500,
                {
                    "ok": False,
                    "error": str(exc),
                    "ts": int(time.time()),
                },
            )

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/v1/command":
                self.handle_command()
            elif parsed.path == "/v1/trace/start":
                self.handle_trace_start()
            elif parsed.path == "/v1/trace/stop":
                self.handle_trace_stop()
            elif parsed.path == "/v1/trace/flush":
                self.handle_trace_flush()
            else:
                self.send_json(404, {"ok": False, "error": "not found", "path": parsed.path})
        except ValueError as exc:
            self.send_json(
                400,
                {
                    "ok": False,
                    "error": str(exc),
                    "ts": int(time.time()),
                },
            )
        except Exception as exc:  # noqa: BLE001 - this is the HTTP boundary.
            self.send_json(
                500,
                {
                    "ok": False,
                    "error": str(exc),
                    "ts": int(time.time()),
                },
            )

    def daemon_request(self, req: dict[str, Any]) -> dict[str, str]:
        return request_daemon(str(self.config["daemon_socket"]), req, float(self.config["timeout_s"]))

    def read_json_body(self) -> Any:
        raw_len = self.headers.get("Content-Length", "0")
        try:
            length = int(raw_len)
        except ValueError as exc:
            raise ValueError("invalid Content-Length") from exc
        if length <= 0:
            raise ValueError("empty request body")
        if length > MAX_COMMAND_BODY_BYTES:
            raise ValueError("request body too large")
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def read_optional_json_body(self) -> Any:
        raw_len = self.headers.get("Content-Length", "0")
        try:
            length = int(raw_len)
        except ValueError as exc:
            raise ValueError("invalid Content-Length") from exc
        if length <= 0:
            return {}
        if length > MAX_COMMAND_BODY_BYTES:
            raise ValueError("request body too large")
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def maybe_add_monitor_client(self, command: dict[str, str]) -> dict[str, Any]:
        if command.get("op") != "CreateDataObject" or command.get("client_id"):
            return {}
        device_id = command.get("device_id", command.get("target", "0").removeprefix("npu:"))
        if not str(device_id).isdigit():
            device_id = "0"
        client = self.daemon_request(
            {
                "op": "RegisterClient",
                "role": "monitor-ui",
                "os_pid": os.getpid(),
                "bare_tgid": os.getpid(),
                "device_id": device_id,
            }
        )
        if client.get("status") != "ok":
            raise RuntimeError(client.get("detail", "RegisterClient failed"))
        command["client_id"] = client["client_id"]
        return {"auto_client_id": coerce_value(client["client_id"])}

    def handle_healthz(self) -> None:
        started = time.time()
        try:
            stats_raw = self.daemon_request({"op": "GetStats"})
            daemon_ok = stats_raw.get("status") == "ok"
            status_code = 200 if daemon_ok else 503
            payload = {
                "ok": daemon_ok,
                "monitor_api": "ok",
                "daemon_reachable": daemon_ok,
                "daemon_socket": self.config["daemon_socket"],
                "latency_ms": round((time.time() - started) * 1000.0, 3),
                "hbm_available": coerce_value(stats_raw.get("hbm_available", "false")),
                "hbm_probe_error": stats_raw.get("hbm_probe_error", ""),
                "detail": stats_raw.get("detail", ""),
                "ts": int(time.time()),
            }
            self.send_json(status_code, payload)
        except Exception as exc:  # noqa: BLE001 - daemon may be down.
            self.send_json(
                503,
                {
                    "ok": False,
                    "monitor_api": "ok",
                    "daemon_reachable": False,
                    "daemon_socket": self.config["daemon_socket"],
                    "error": str(exc),
                    "ts": int(time.time()),
                },
            )

    def handle_config(self) -> None:
        self.send_json(
            200,
            {
                "ok": True,
                "config": self.config,
                "ts": int(time.time()),
            },
        )

    def handle_stats(self) -> None:
        stats_raw = self.daemon_request({"op": "GetStats"})
        status_ok = stats_raw.get("status") == "ok"
        payload = {
            "ok": status_ok,
            "stats": coerce_dict(stats_raw),
            "ts": int(time.time()),
        }
        self.send_json(200 if status_ok else 503, payload)

    def handle_observability(self) -> None:
        started = time.time()
        stats_raw = self.daemon_request({"op": "GetStats"})
        trace_raw = self.daemon_request({"op": "GetTraceStatus"})
        capabilities_raw = self.daemon_request({"op": "GetCapabilities"})
        status_ok = (
            stats_raw.get("status") == "ok"
            and trace_raw.get("status") == "ok"
            and capabilities_raw.get("status") == "ok"
        )
        self.send_json(
            200 if status_ok else 503,
            {
                "ok": status_ok,
                "monitor_api": "ok",
                "daemon_reachable": stats_raw.get("status") == "ok",
                "daemon_socket": self.config["daemon_socket"],
                "latency_ms": round((time.time() - started) * 1000.0, 3),
                "stats": coerce_dict(stats_raw),
                "trace": coerce_dict(trace_raw),
                "capabilities": coerce_dict(capabilities_raw),
                "trace_root": self.config["trace_root"],
                "ts": int(time.time()),
            },
        )

    def handle_capabilities(self) -> None:
        raw = self.daemon_request({"op": "GetCapabilities"})
        status_ok = raw.get("status") == "ok"
        self.send_json(
            200 if status_ok else 503,
            {
                "ok": status_ok,
                "capabilities": coerce_dict(raw),
                "ts": int(time.time()),
            },
        )

    def handle_trace_status(self) -> None:
        raw = self.daemon_request({"op": "GetTraceStatus"})
        status_ok = raw.get("status") == "ok"
        self.send_json(
            200 if status_ok else 503,
            {
                "ok": status_ok,
                "trace": coerce_dict(raw),
                "trace_root": self.config["trace_root"],
                "ts": int(time.time()),
            },
        )

    def handle_trace_start(self) -> None:
        payload = self.read_optional_json_body()
        if not isinstance(payload, dict):
            raise ValueError("trace start body must be a JSON object")
        command = {str(key): normalize_command_value(str(key), value) for key, value in payload.items()}
        command["op"] = "StartTrace"
        command.setdefault("output_dir", str(self.config["trace_root"]))
        raw = self.daemon_request(command)
        status_ok = raw.get("status") == "ok"
        self.send_json(
            200 if status_ok else 400,
            {
                "ok": status_ok,
                "command": command,
                "response": coerce_dict(raw),
                "ts": int(time.time()),
            },
        )

    def handle_trace_stop(self) -> None:
        payload = self.read_optional_json_body()
        if not isinstance(payload, dict):
            raise ValueError("trace stop body must be a JSON object")
        command = {str(key): normalize_command_value(str(key), value) for key, value in payload.items()}
        command["op"] = "StopTrace"
        command.setdefault("flush", "1")
        raw = self.daemon_request(command)
        status_ok = raw.get("status") == "ok"
        self.send_json(
            200 if status_ok else 400,
            {
                "ok": status_ok,
                "command": command,
                "response": coerce_dict(raw),
                "ts": int(time.time()),
            },
        )

    def handle_trace_flush(self) -> None:
        payload = self.read_optional_json_body()
        if payload and not isinstance(payload, dict):
            raise ValueError("trace flush body must be a JSON object")
        raw = self.daemon_request({"op": "FlushTrace"})
        status_ok = raw.get("status") == "ok"
        self.send_json(
            200 if status_ok else 400,
            {
                "ok": status_ok,
                "response": coerce_dict(raw),
                "ts": int(time.time()),
            },
        )

    def handle_trace_artifact(self, run_id: str, filename: str) -> None:
        if not run_id:
            raise ValueError("run_id is required")
        if "/" in run_id or "\\" in run_id or run_id in {".", ".."}:
            raise ValueError("invalid run_id")
        if filename not in TRACE_ARTIFACT_FILES:
            raise ValueError(f"unsupported trace artifact {filename}")
        root = Path(str(self.config["trace_root"])).resolve()
        path = (root / run_id / filename).resolve()
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise ValueError("trace artifact escapes trace root") from exc
        if not path.exists():
            self.send_json(404, {"ok": False, "error": "trace artifact not found", "path": str(path)})
            return
        data = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", f"{TRACE_ARTIFACT_FILES[filename]}; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def handle_objects(self, model_id: str) -> None:
        raw = self.daemon_request({"op": "GetModelObjects", "model_id": model_id})
        status_ok = raw.get("status") == "ok"
        self.send_json(
            200 if status_ok else 503,
            {
                "ok": status_ok,
                "model_id": model_id,
                "object_count": coerce_value(raw.get("object_count", "0")),
                "objects": parse_model_objects(raw),
                "detail": raw.get("detail", ""),
                "ts": int(time.time()),
            },
        )

    def handle_command(self) -> None:
        command = normalize_command(self.read_json_body())
        extra = self.maybe_add_monitor_client(command)
        raw = self.daemon_request(command)
        status_ok = raw.get("status") == "ok"
        status_code = 200 if status_ok else 400
        self.send_json(
            status_code,
            {
                "ok": status_ok,
                "command": command,
                "response": coerce_dict(raw),
                **extra,
                "ts": int(time.time()),
            },
        )


def build_config(args: argparse.Namespace) -> dict[str, Any]:
    api_base = os.environ.get("UFLOW_API_BASE", f"http://{args.host}:{args.port}")
    ui_url = os.environ.get("UFLOW_UI_URL", "http://127.0.0.1:3000")
    return {
        "daemon_socket": args.socket,
        "monitor_host": args.host,
        "monitor_port": args.port,
        "api_base": api_base,
        "ui_url": ui_url,
        "tunnel_path": os.environ.get("UFLOW_TUNNEL_PATH", "local -> host1 -> host2 container"),
        "default_model_id": os.environ.get("UFLOW_MONITOR_MODEL_ID", DEFAULT_MODEL_ID),
        "trace_root": os.environ.get("UF_TRACE_OUTPUT_DIR", os.environ.get("UF_TRACE_ROOT", "/tmp/uflow_traces")),
        "timeout_s": args.timeout,
        "poll_interval_ms": int(os.environ.get("UFLOW_MONITOR_POLL_MS", "2000")),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="UFlow PhaseE-04 monitor API")
    parser.add_argument("--socket", default=os.environ.get("UF_SOCKET", DEFAULT_SOCKET))
    parser.add_argument("--host", default=os.environ.get("UF_MONITOR_HOST", DEFAULT_HOST))
    parser.add_argument("--port", type=int, default=int(os.environ.get("UF_MONITOR_PORT", str(DEFAULT_PORT))))
    parser.add_argument("--timeout", type=float, default=float(os.environ.get("UF_MONITOR_TIMEOUT_S", "2.0")))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    server = ThreadingHTTPServer((args.host, args.port), MonitorApiHandler)
    server.config = build_config(args)  # type: ignore[attr-defined]
    print(
        json.dumps(
            {
                "event": "monitor_api_started",
                "host": args.host,
                "port": args.port,
                "daemon_socket": args.socket,
            },
            separators=(",", ":"),
        ),
        flush=True,
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
