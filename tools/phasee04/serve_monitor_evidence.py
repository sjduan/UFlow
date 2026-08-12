#!/usr/bin/env python3
"""Serve saved Monitor v2 evidence or a minimal v1 fixture for visual QA."""

from __future__ import annotations

import argparse
import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


TRACE_ARTIFACTS = {
    "summary.md": "text/markdown; charset=utf-8",
    "trace_events.json": "application/json; charset=utf-8",
    "trace_summary.csv": "text/csv; charset=utf-8",
    "trace_summary.json": "application/json; charset=utf-8",
    "transfer_summary.csv": "text/csv; charset=utf-8",
}


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path}")
    return payload


class EvidenceSource:
    def __init__(self, run_dir: Path | None, *, legacy_v1: bool = False) -> None:
        self.run_dir = run_dir
        self.legacy_v1 = legacy_v1
        if not legacy_v1:
            if run_dir is None or not run_dir.is_dir():
                raise ValueError("--run-dir must name a saved Monitor v2 result directory")
            for filename in (
                "config.json",
                "health.json",
                "stats.json",
                "trace_status.json",
                "transfers.json",
                "transfers_summary.json",
                "object_backing_path_check.json",
            ):
                if not (run_dir / filename).is_file():
                    raise ValueError(f"missing evidence file: {run_dir / filename}")

    def json_payload(self, path: str) -> tuple[int, dict[str, Any]]:
        if self.legacy_v1:
            return self._legacy_payload(path)
        assert self.run_dir is not None
        files = {
            "/healthz": "health.json",
            "/v1/config": "config.json",
            "/v1/stats": "stats.json",
            "/v1/trace/status": "trace_status.json",
            "/v1/transfers": "transfers.json",
            "/v1/transfers/summary": "transfers_summary.json",
        }
        if path == "/v1/objects":
            objects = load_json(self.run_dir / "object_backing_path_check.json")
            return HTTPStatus.OK, objects["objects_while_live"]
        filename = files.get(path)
        if filename is None:
            return HTTPStatus.NOT_FOUND, {"ok": False, "error": "fixture endpoint not found"}
        return HTTPStatus.OK, load_json(self.run_dir / filename)

    def artifact(self, filename: str) -> tuple[str, bytes] | None:
        if self.legacy_v1 or filename not in TRACE_ARTIFACTS or self.run_dir is None:
            return None
        path = self.run_dir / "trace_download" / filename
        if not path.is_file():
            return None
        return TRACE_ARTIFACTS[filename], path.read_bytes()

    @staticmethod
    def _legacy_payload(path: str) -> tuple[int, dict[str, Any]]:
        payloads: dict[str, dict[str, Any]] = {
            "/healthz": {
                "ok": True,
                "monitor_api": "ok",
                "daemon_reachable": True,
                "hbm_available": True,
                "detail": "v1 compatibility fixture",
                "ts": 1786536000,
            },
            "/v1/config": {
                "ok": True,
                "config": {
                    "daemon_socket": "/tmp/uflow_phasee04_v1.sock",
                    "default_model_id": "phasee04-monitor",
                    "poll_interval_ms": 2000,
                    "tunnel_path": "local -> ssh serving-b -> openeuler-2403-DS",
                },
                "ts": 1786536000,
            },
            "/v1/stats": {
                "ok": True,
                "stats": {
                    "ddr_backend_mode": "memfd",
                    "ddr_objects": 1,
                    "ddr_requested_bytes": 268435456,
                    "ddr_committed_bytes": 268435456,
                    "ddr_safe_allocatable_bytes": 1073741824,
                    "ddr_root": "/dev/shm/uflow_phasee04_v1",
                },
                "ts": 1786536000,
            },
            "/v1/trace/status": {
                "ok": True,
                "trace": {"trace_enabled": False},
                "ts": 1786536000,
            },
            "/v1/objects": {
                "ok": True,
                "model_id": "phasee04-monitor",
                "object_count": 1,
                "objects": [
                    {
                        "object_id": 100,
                        "name": "v1.ddr.256m",
                        "role": "user",
                        "placement": "ddr",
                        "target": "host:0",
                        "requested_bytes": 268435456,
                        "state": "Created",
                        "ddr_path": "/dev/shm/uflow_phasee04_v1/object_100",
                    }
                ],
                "ts": 1786536000,
            },
        }
        if path in ("/v1/transfers", "/v1/transfers/summary"):
            return HTTPStatus.NOT_FOUND, {
                "ok": False,
                "error": "Monitor v2 transfer endpoint unavailable on v1 daemon",
            }
        payload = payloads.get(path)
        if payload is None:
            return HTTPStatus.NOT_FOUND, {"ok": False, "error": "fixture endpoint not found"}
        return HTTPStatus.OK, payload


def handler_factory(source: EvidenceSource) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def end_headers(self) -> None:
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Cache-Control", "no-store")
            super().end_headers()

        def do_GET(self) -> None:  # noqa: N802 - stdlib handler contract
            parsed = urlparse(self.path)
            if parsed.path == "/v1/trace/artifact":
                params = dict(
                    part.split("=", 1) for part in parsed.query.split("&") if "=" in part
                )
                artifact = source.artifact(params.get("file", ""))
                if artifact is None:
                    self._send_json(
                        HTTPStatus.NOT_FOUND,
                        {"ok": False, "error": "artifact unavailable"},
                    )
                    return
                content_type, body = artifact
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            status, payload = source.json_payload(parsed.path)
            self._send_json(status, payload)

        def log_message(self, _format: str, *_args: object) -> None:
            return

        def _send_json(self, status: int, payload: dict[str, Any]) -> None:
            body = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return Handler


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path)
    parser.add_argument("--legacy-v1", action="store_true")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    if args.legacy_v1 == (args.run_dir is not None):
        parser.error("choose exactly one of --run-dir or --legacy-v1")
    source = EvidenceSource(args.run_dir, legacy_v1=args.legacy_v1)
    server = ThreadingHTTPServer((args.host, args.port), handler_factory(source))
    print(f"serving Monitor evidence on http://{args.host}:{args.port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
