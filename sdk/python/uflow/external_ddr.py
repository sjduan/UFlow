"""Cooperative, same-UID external memfd registration and read-only views.

The backing owner creates and populates the memfd before registration. The broker
never allocates or zeroes it. A view is a mapping, not a copy or a new placement.
"""
from __future__ import annotations

import array
import json
import mmap
import os
import socket
from dataclasses import dataclass
from typing import Any


class ExternalDdrError(RuntimeError):
    pass


@dataclass(frozen=True)
class ExternalBackingHandle:
    host_id: str
    boot_id: str
    broker_instance: str
    owner_incarnation: str
    owner_name: str
    pool_generation: int
    resource_generation: int
    object_id: int
    placement_id: int
    binding_id: int
    owner_lease_id: int
    size_bytes: int

    @classmethod
    def from_response(cls, fields: dict[str, str]) -> "ExternalBackingHandle":
        return cls(
            host_id=fields["host_id"], boot_id=fields["boot_id"],
            broker_instance=fields["broker_instance"], owner_incarnation=fields["owner_incarnation"],
            owner_name=fields["owner_name"], pool_generation=int(fields["pool_generation"]),
            resource_generation=int(fields["resource_generation"]), object_id=int(fields["object_id"]),
            placement_id=int(fields["placement_id"]), binding_id=int(fields["binding_id"]),
            owner_lease_id=int(fields["owner_lease_id"]), size_bytes=int(fields["size_bytes"]),
        )

    def identity_fields(self) -> dict[str, Any]:
        return {
            "host_id": self.host_id, "boot_id": self.boot_id,
            "broker_instance": self.broker_instance, "owner_incarnation": self.owner_incarnation,
            "pool_generation": self.pool_generation, "resource_generation": self.resource_generation,
            "object_id": self.object_id,
        }


@dataclass(frozen=True)
class _ViewIdentity:
    backing: ExternalBackingHandle
    view_id: int
    lease_id: int
    accessor_address_domain: str

    @classmethod
    def from_response(cls, fields: dict[str, str]) -> "_ViewIdentity":
        return cls(ExternalBackingHandle.from_response(fields), int(fields["view_id"]),
                   int(fields["lease_id"]), fields["accessor_address_domain"])

    def identity_fields(self) -> dict[str, Any]:
        return {**self.backing.identity_fields(), "view_id": self.view_id,
                "lease_id": self.lease_id, "accessor_address_domain": self.accessor_address_domain}


class ExternalReadView:
    def __init__(self, client: "ExternalDdrClient", fields: dict[str, str], mapping: mmap.mmap) -> None:
        self._client = client
        self._mapping = mapping
        self._identity = _ViewIdentity.from_response(fields)
        self.view_id = self._identity.view_id
        self.lease_id = self._identity.lease_id
        self.object_id = int(fields["object_id"])
        self.placement_id = int(fields["placement_id"])
        self.allowed_offset_bytes = int(fields["allowed_offset_bytes"])
        self.allowed_bytes = int(fields["allowed_bytes"])
        self.source_address_domain = fields["source_address_domain"]
        self.accessor_address_domain = fields["accessor_address_domain"]
        self._release_confirmed = False

    def read(self, offset_bytes: int = 0, nbytes: int | None = None) -> bytes:
        if self._mapping.closed:
            raise ExternalDdrError("view already released")
        offset = int(offset_bytes)
        size = self.allowed_bytes - offset if nbytes is None else int(nbytes)
        if offset < 0 or size < 0 or offset + size > self.allowed_bytes:
            raise ValueError("read range out of bounds")
        start = self.allowed_offset_bytes + offset
        return self._mapping[start:start + size]

    def retain(self, offset_bytes: int = 0, nbytes: int | None = None) -> "ExternalReadView":
        if self._mapping.closed:
            raise ExternalDdrError("view already released")
        offset = int(offset_bytes)
        size = self.allowed_bytes - offset if nbytes is None else int(nbytes)
        if offset < 0 or size <= 0 or offset + size > self.allowed_bytes:
            raise ValueError("derived view range out of bounds")
        return self._client._view_request(
            "RetainExternalView", **self._identity.identity_fields(), offset_bytes=offset, nbytes=size,
        )

    def release(self) -> None:
        if self._release_confirmed:
            return
        # Unmap first. Do not advertise a release if a live Python buffer prevents it.
        if not self._mapping.closed:
            self._mapping.close()
        # A rejected/unknown remote result is surfaced and may be retried; the
        # local mapping is already closed and cannot be used again.
        self._client._request("ReleaseExternalView", **self._identity.identity_fields())
        self._release_confirmed = True

    def __enter__(self) -> "ExternalReadView":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.release()


class ExternalDdrClient:
    """Use the daemon's authenticated ``<control socket>.external-ddr`` endpoint."""

    def __init__(self, control_socket: str, *, timeout: float = 5.0) -> None:
        self.socket_path = control_socket + ".external-ddr"
        self.timeout = timeout

    @staticmethod
    def _encode(fields: dict[str, Any]) -> bytes:
        parts = []
        for key, value in fields.items():
            text = str(value)
            if any(c in text for c in ";=\n\r"):
                raise ValueError(f"invalid wire value for {key}")
            parts.append(f"{key}={text}")
        return (";".join(parts) + "\n").encode()

    @staticmethod
    def _decode(data: bytes) -> dict[str, str]:
        result: dict[str, str] = {}
        for item in data.decode("utf-8").rstrip("\n").split(";"):
            if "=" in item:
                key, value = item.split("=", 1)
                result[key] = value
        return result

    def _exchange(self, op: str, *, send_fd: int | None = None, expect_fd: bool = False,
                  provisional: bool = False, **values: Any) -> tuple[dict[str, str], mmap.mmap | None]:
        fields = {"op": op, **values}
        payload = self._encode(fields)
        mapping: mmap.mmap | None = None
        received_fds: list[int] = []
        response: dict[str, str] | None = None
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.settimeout(self.timeout)
            sock.connect(self.socket_path)
            if send_fd is None:
                sock.sendall(payload)
            else:
                fd_array = array.array("i", [int(send_fd)])
                sent = sock.sendmsg([payload], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, fd_array)])
                if sent <= 0:
                    raise ExternalDdrError("failed to send registration FD")
                sock.sendall(payload[sent:])
            try:
                first, ancillary, flags, _ = sock.recvmsg(65536, socket.CMSG_SPACE(16 * array.array("i").itemsize))
                for level, kind, data in ancillary:
                    if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
                        items = array.array("i")
                        items.frombytes(data[:len(data) - len(data) % items.itemsize])
                        received_fds.extend(items)
                if flags & socket.MSG_CTRUNC:
                    raise ExternalDdrError("truncated received FDs")
                data = bytearray(first)
                while b"\n" not in data:
                    chunk = sock.recv(65536)
                    if not chunk or len(data) + len(chunk) > 1_048_576:
                        raise ExternalDdrError("invalid broker response")
                    data.extend(chunk)
                line, extra = bytes(data).split(b"\n", 1)
                if extra:
                    raise ExternalDdrError("unexpected data after broker response")
                response = self._decode(line)
                if response.get("status") != "ok":
                    raise ExternalDdrError(response.get("detail", "broker request failed"))
                if len(received_fds) != (1 if expect_fd else 0):
                    raise ExternalDdrError("unexpected number of FDs")
                if expect_fd:
                    import fcntl
                    fd = received_fds[0]
                    if fcntl.fcntl(fd, fcntl.F_GETFL) & os.O_ACCMODE != os.O_RDONLY:
                        raise ExternalDdrError("broker did not supply a read-only FD")
                    if os.fstat(fd).st_size != int(response["size_bytes"]):
                        raise ExternalDdrError("backing size mismatch")
                    mapping = mmap.mmap(fd, int(response["size_bytes"]), access=mmap.ACCESS_READ)
                if provisional:
                    sock.sendall(b"OK\n")
                    confirmation = bytearray()
                    while not confirmation.endswith(b"\n"):
                        chunk = sock.recv(64)
                        if not chunk or len(confirmation) + len(chunk) > 64:
                            raise ExternalDdrError("broker did not commit")
                        confirmation.extend(chunk)
                    if confirmation != b"committed\n":
                        raise ExternalDdrError("unexpected broker confirmation")
                return response, mapping
            except BaseException:
                if mapping is not None:
                    mapping.close()
                # A lost final confirmation is ambiguous: the broker may have
                # committed. Best-effort compensation uses the returned IDs;
                # if the broker is unreachable the caller must treat it as unknown.
                if provisional and response and response.get("status") == "ok":
                    try:
                        if op == "RegisterExternalBacking":
                            self._request("RevokeExternalBacking", **ExternalBackingHandle.from_response(response).identity_fields())
                        elif "view_id" in response:
                            self._request("ReleaseExternalView", **_ViewIdentity.from_response(response).identity_fields())
                    except Exception:
                        pass
                raise
            finally:
                for fd in received_fds:
                    os.close(fd)

    def _request(self, op: str, **values: Any) -> dict[str, str]:
        return self._exchange(op, **values)[0]

    def register(self, fd: int, *, name: str, owner_name: str, size_bytes: int) -> ExternalBackingHandle:
        fields, _ = self._exchange(
            "RegisterExternalBacking", send_fd=fd, provisional=True,
            name=name, owner_name=owner_name, size_bytes=int(size_bytes),
        )
        return ExternalBackingHandle.from_response(fields)

    def _view_request(self, op: str, **values: Any) -> ExternalReadView:
        fields, mapping = self._exchange(op, expect_fd=True, provisional=True, **values)
        assert mapping is not None
        try:
            return ExternalReadView(self, fields, mapping)
        except BaseException:
            mapping.close()
            try:
                self._request("ReleaseExternalView", **_ViewIdentity.from_response(fields).identity_fields())
            except Exception:
                # Preserve the original error; compensation is best effort.
                pass
            raise

    def acquire(self, handle: ExternalBackingHandle, *, offset_bytes: int = 0,
                nbytes: int | None = None) -> ExternalReadView:
        offset = int(offset_bytes)
        size = handle.size_bytes - offset if nbytes is None else int(nbytes)
        if offset < 0 or size <= 0 or offset + size > handle.size_bytes:
            raise ValueError("view range out of bounds")
        return self._view_request(
            "AcquireExternalRead", **handle.identity_fields(),
            access_mode="read", offset_bytes=offset, nbytes=size,
        )

    def revoke(self, handle: ExternalBackingHandle) -> None:
        self._request("RevokeExternalBacking", **handle.identity_fields())

    def snapshot(self) -> dict[str, Any]:
        response = self._request("GetExternalDdrSnapshot")
        projection = json.loads(response["projection_json"])
        for key in ("committed_bytes", "backing_fds", "session_pidfds", "owner_leases", "active_views", "pending_handoffs", "in_flight"):
            projection[key] = int(response[key])
        return projection
