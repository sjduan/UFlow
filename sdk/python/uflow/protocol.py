from __future__ import annotations

import socket
from typing import Any


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


class SocketClient:
    def __init__(self, socket_path: str) -> None:
        self.socket_path = socket_path

    def request(self, **kv: Any) -> dict[str, str]:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.connect(self.socket_path)
            sock.sendall(encode_kv(kv))
            chunks: list[bytes] = []
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                chunks.append(chunk)
                if b"\n" in chunk:
                    break
        resp = decode_kv(b"".join(chunks).split(b"\n", 1)[0])
        if resp.get("status") != "ok":
            raise RuntimeError(f"UFlow request {kv.get('op')} failed: {resp.get('detail', resp)}")
        return resp
