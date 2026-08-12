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
DEFAULT_TRANSFER_LIMIT = 50
MAX_TRANSFER_LIMIT = 200
TRANSFER_STATUSES = {"pending", "running", "complete", "failed", "cancelled"}
TRANSFER_FIELDS = (
    "event_id",
    "plan_id",
    "status",
    "src_object_id",
    "dst_object_id",
    "src_medium",
    "dst_medium",
    "direction",
    "operation",
    "requested_mode",
    "nbytes",
    "planned_path",
    "planned_engine",
    "actual_path",
    "actual_engine",
    "actual_latency_us",
    "actual_bandwidth_gib_s",
    "fallback_used",
    "fallback_reason",
    "error",
)
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
    "ListTransferEvents",
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


class DaemonUnavailable(RuntimeError):
    """The daemon socket is unavailable or cannot serve the requested read op."""
TRACE_ARTIFACT_FILES = {
    "trace_events.json": "application/json",
    "trace_summary.json": "application/json",
    "trace_summary.csv": "text/csv",
    "transfer_summary.csv": "text/csv",
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
        if len(tail_parts) >= 2 and tail_parts[0] in {"host", "npu", "ssd"}:
            target = f"{tail_parts[0]}:{tail_parts[1]}"
            backing_path = tail_parts[2] if len(tail_parts) == 3 else ""
        else:
            target = address_tail
            backing_path = ""
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
                "backing_path": backing_path,
                "ddr_path": backing_path if placement == "ddr" else "",
            }
        )
    return objects


def _single_query_value(query: dict[str, list[str]], key: str) -> str:
    values = query.get(key, [])
    if len(values) > 1:
        raise ValueError(f"{key} must be specified at most once")
    return values[0] if values else ""


def _positive_query_int(
    query: dict[str, list[str]],
    key: str,
    *,
    default: int | None = None,
    maximum: int | None = None,
) -> int | None:
    raw = _single_query_value(query, key)
    if not raw:
        return default
    if not raw.isdigit() or int(raw) <= 0:
        raise ValueError(f"{key} must be a positive integer")
    value = int(raw)
    if maximum is not None and value > maximum:
        raise ValueError(f"{key} must be at most {maximum}")
    return value


def parse_transfer_query(
    query: dict[str, list[str]], *, summary: bool = False
) -> dict[str, Any]:
    allowed = {"limit"} if summary else {
        "limit",
        "before_event_id",
        "status",
        "path",
        "engine",
        "client_id",
    }
    unknown = sorted(set(query) - allowed)
    if unknown:
        raise ValueError(f"unsupported transfer filter {unknown[0]}")
    limit = _positive_query_int(
        query,
        "limit",
        default=MAX_TRANSFER_LIMIT if summary else DEFAULT_TRANSFER_LIMIT,
        maximum=MAX_TRANSFER_LIMIT,
    )
    parsed: dict[str, Any] = {"limit": limit}
    if summary:
        return parsed

    before_event_id = _positive_query_int(query, "before_event_id")
    client_id = _positive_query_int(query, "client_id")
    status = _single_query_value(query, "status")
    if status and status not in TRANSFER_STATUSES:
        raise ValueError(f"unsupported transfer status {status}")
    for key in ("path", "engine"):
        value = _single_query_value(query, key)
        if len(value) > 256 or ";" in value or "=" in value:
            raise ValueError(f"invalid {key} filter")
        parsed[key] = value
    parsed.update(
        {
            "before_event_id": before_event_id,
            "status": status,
            "client_id": client_id,
        }
    )
    return parsed


def normalize_transfer_event(raw: dict[str, str]) -> dict[str, Any]:
    record = coerce_dict(raw)
    record["status"] = record.get("event_status") or None
    error_message = str(record.get("error_message") or "")
    error_code = str(record.get("error_code") or "")
    record["error"] = error_message or error_code or None
    for field in TRANSFER_FIELDS:
        record.setdefault(field, None)
    return record


def fetch_transfer_window(
    daemon_request: Any, filters: dict[str, Any]
) -> dict[str, Any]:
    list_request: dict[str, Any] = {
        "op": "ListTransferEvents",
        "limit": filters["limit"],
    }
    mapping = {
        "before_event_id": "before_event_id",
        "status": "status",
        "path": "actual_path",
        "engine": "actual_engine",
        "client_id": "client_id",
    }
    for source, target in mapping.items():
        value = filters.get(source)
        if value not in (None, ""):
            list_request[target] = value

    listed = daemon_request(list_request)
    if listed.get("status") != "ok":
        raise DaemonUnavailable(listed.get("detail", "ListTransferEvents failed"))
    raw_ids = listed.get("event_ids", "")
    event_ids: list[int] = []
    for raw_id in raw_ids.split(","):
        if not raw_id:
            continue
        if not raw_id.isdigit():
            raise RuntimeError(f"invalid event id from daemon: {raw_id!r}")
        event_ids.append(int(raw_id))

    transfers: list[dict[str, Any]] = []
    skipped_event_ids: list[int] = []
    for event_id in event_ids:
        event = daemon_request({"op": "PollEvent", "event_id": event_id})
        if event.get("status") != "ok":
            if "not found" in event.get("detail", "").lower():
                skipped_event_ids.append(event_id)
                continue
            raise DaemonUnavailable(event.get("detail", f"PollEvent {event_id} failed"))
        transfers.append(normalize_transfer_event(event))

    next_before_raw = listed.get("next_before_event_id", "")
    next_before_event_id = int(next_before_raw) if next_before_raw.isdigit() else None
    return {
        "transfers": transfers,
        "returned": len(transfers),
        "listed": len(event_ids),
        "has_more": listed.get("has_more") == "true",
        "next_before_event_id": next_before_event_id,
        "skipped_event_ids": skipped_event_ids,
        "window_limit": int(filters["limit"]),
        "filters": {key: value for key, value in filters.items() if value not in (None, "")},
    }


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _number(value: Any) -> float | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number


def summarize_transfers(window: dict[str, Any], generated_at: int | None = None) -> dict[str, Any]:
    groups: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    fallback_count = 0
    ssd_read_bytes = 0
    ssd_write_bytes = 0
    ssd_read_ops = 0
    ssd_write_ops = 0
    ssd_io_submit_us = 0.0
    ssd_io_wait_us = 0.0

    for transfer in window["transfers"]:
        key = tuple(
            str(transfer.get(name) or "unknown")
            for name in ("actual_path", "actual_engine", "direction", "status")
        )
        group = groups.setdefault(
            key,
            {
                "actual_path": key[0],
                "actual_engine": key[1],
                "direction": key[2],
                "status": key[3],
                "count": 0,
                "bytes": 0,
                "latencies": [],
                "bandwidths": [],
            },
        )
        group["count"] += 1
        group["bytes"] += int(_number(transfer.get("nbytes")) or _number(transfer.get("bytes_done")) or 0)
        latency = _number(transfer.get("actual_latency_us"))
        bandwidth = _number(transfer.get("actual_bandwidth_gib_s"))
        if latency is not None:
            group["latencies"].append(latency)
        if bandwidth is not None:
            group["bandwidths"].append(bandwidth)
        if transfer.get("fallback_used") is True:
            fallback_count += 1

        read_bytes = int(_number(transfer.get("ssd_read_bytes")) or 0)
        write_bytes = int(_number(transfer.get("ssd_write_bytes")) or 0)
        ssd_read_bytes += read_bytes
        ssd_write_bytes += write_bytes
        ssd_read_ops += int(read_bytes > 0)
        ssd_write_ops += int(write_bytes > 0)
        ssd_io_submit_us += _number(transfer.get("ssd_io_submit_us")) or 0.0
        ssd_io_wait_us += _number(transfer.get("ssd_io_wait_us")) or 0.0

    group_rows: list[dict[str, Any]] = []
    for key in sorted(groups):
        group = groups[key]
        latencies = group.pop("latencies")
        bandwidths = group.pop("bandwidths")
        group.update(
            {
                "p50_latency_us": percentile(latencies, 0.50),
                "p95_latency_us": percentile(latencies, 0.95),
                "p50_bandwidth_gib_s": percentile(bandwidths, 0.50),
                "p95_bandwidth_gib_s": percentile(bandwidths, 0.95),
            }
        )
        group_rows.append(group)

    sample_size = len(window["transfers"])
    return {
        "sample_size": sample_size,
        "window_limit": window["window_limit"],
        "generated_at": int(time.time()) if generated_at is None else generated_at,
        "groups": group_rows,
        "fallback_count": fallback_count,
        "fallback_rate": fallback_count / sample_size if sample_size else 0.0,
        "ssd_io": {
            "read_bytes": ssd_read_bytes,
            "write_bytes": ssd_write_bytes,
            "read_ops": ssd_read_ops,
            "write_ops": ssd_write_ops,
            "submit_us": ssd_io_submit_us,
            "wait_us": ssd_io_wait_us,
            "total_us": ssd_io_submit_us + ssd_io_wait_us,
        },
        "has_more": window["has_more"],
        "skipped_event_ids": window["skipped_event_ids"],
    }


def observability_transfer_summary(
    daemon_request: Any, capabilities_raw: dict[str, str]
) -> dict[str, Any]:
    capability_ops = capabilities_raw.get("ops", "").split(",")
    if "ListTransferEvents" not in capability_ops:
        return {
            "available": False,
            "reason": "ListTransferEvents is not advertised by this daemon",
        }
    try:
        window = fetch_transfer_window(daemon_request, {"limit": DEFAULT_TRANSFER_LIMIT})
        return {"available": True, **summarize_transfers(window)}
    except Exception as exc:  # noqa: BLE001 - this section is optional for v1 daemons.
        return {"available": False, "reason": str(exc)}


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
                query = parse_qs(parsed.query, keep_blank_values=True)
                model_id = query.get("model_id", [DEFAULT_MODEL_ID])[0]
                self.handle_objects(model_id)
            elif parsed.path == "/v1/transfers":
                query = parse_qs(parsed.query, keep_blank_values=True)
                self.handle_transfers(query)
            elif parsed.path == "/v1/transfers/summary":
                query = parse_qs(parsed.query, keep_blank_values=True)
                self.handle_transfer_summary(query)
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
        except DaemonUnavailable as exc:
            self.send_json(
                503,
                {
                    "ok": False,
                    "error": str(exc),
                    "daemon_reachable": False,
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
        try:
            return request_daemon(
                str(self.config["daemon_socket"]), req, float(self.config["timeout_s"])
            )
        except (OSError, TimeoutError, RuntimeError) as exc:
            raise DaemonUnavailable(str(exc)) from exc

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
        transfer_summary = observability_transfer_summary(
            self.daemon_request, capabilities_raw
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
                "transfer_summary": transfer_summary,
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

    def handle_transfers(self, query: dict[str, list[str]]) -> None:
        filters = parse_transfer_query(query)
        window = fetch_transfer_window(self.daemon_request, filters)
        self.send_json(
            200,
            {
                "ok": True,
                **window,
                "ts": int(time.time()),
            },
        )

    def handle_transfer_summary(self, query: dict[str, list[str]]) -> None:
        filters = parse_transfer_query(query, summary=True)
        window = fetch_transfer_window(self.daemon_request, filters)
        self.send_json(
            200,
            {
                "ok": True,
                "summary": summarize_transfers(window),
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
        "tunnel_path": os.environ.get(
            "UFLOW_TUNNEL_PATH", "local -> ssh serving-b -> openeuler-2403-DS"
        ),
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
