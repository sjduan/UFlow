from __future__ import annotations

import mmap
import os
import threading
from pathlib import Path
from typing import Any

import torch

from .acl import _AclClient
from .idl import DataObject, DataPlacement, TransferCost, TransferEvent, TransferPlan, TransferRequest
from .objects import DdrBuffer, DdrObject, HbmObject, ManagedBuffer, SsdObject
from .protocol import SocketClient
from .transfer import AclEventHandle, AclStreamHandle, TransferCompletionEventHandle

MANDATORY_HBM_HINT = "mandatory:hbm"
MANDATORY_DDR_HINT = "mandatory:ddr"
MANDATORY_SSD_HINT = "mandatory:ssd"
TRANSFER_MODES = {
    "auto",
    "sync",
    "pinned_sync",
    "async",
    "pinned_async",
    "direct_async",
    "buffered",
    "relay",
    "ssd_hbm_direct",
    "ssd_mmap",
    "ssd_odirect",
}


def env_flag(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


class UFlowClient:
    def __init__(
        self,
        *,
        enabled: bool,
        socket_path: str,
        device_id: int,
        acl_lib_path: str,
        client_role: str = "user",
        model_id: str = "",
    ) -> None:
        self.enabled = bool(enabled)
        self.socket_path = socket_path
        self.device_id = int(device_id)
        self.acl_lib_path = acl_lib_path
        self.client_role = client_role
        self.model_id = model_id
        self._socket: SocketClient | None = None
        self._acl: _AclClient | None = None
        self._client_id: int | None = None
        self._objects: dict[int, HbmObject] = {}
        self._ddr_objects: dict[int, DdrObject] = {}
        self._ssd_objects: dict[int, SsdObject] = {}
        self._completion_event_threads: dict[int, threading.Thread] = {}
        self._completion_event_errors: dict[int, BaseException] = {}
        if self.enabled:
            self._connect()

    @classmethod
    def from_env(
        cls,
        *,
        device_id: int,
        model_id: str = "",
        client_role: str = "user",
    ) -> "UFlowClient":
        enabled = env_flag("UF_ENABLE", False)
        socket_path = os.environ.get("UF_SOCKET", "/tmp/uflow.sock")
        target_device = int(os.environ.get("UF_TARGET_DEVICE", str(device_id)))
        acl_lib_path = os.environ.get("UF_ACL_LIB", "/home/sj/git/data-service/build/lib/libuf_acl_shim.so")
        return cls(
            enabled=enabled,
            socket_path=socket_path,
            device_id=target_device,
            acl_lib_path=acl_lib_path,
            client_role=client_role,
            model_id=model_id,
        )

    @property
    def active(self) -> bool:
        return self.enabled and self._socket is not None and self._client_id is not None

    @property
    def client_id(self) -> int | None:
        return self._client_id

    def _connect(self) -> None:
        if not Path(self.socket_path).exists():
            raise FileNotFoundError(f"UF_SOCKET does not exist: {self.socket_path}")
        require_acl = env_flag("UF_REQUIRE_ACL", False)
        if Path(self.acl_lib_path).exists():
            try:
                self._acl = _AclClient(self.acl_lib_path, self.device_id)
            except Exception:
                if require_acl:
                    raise
                self._acl = None
        elif require_acl:
            raise FileNotFoundError(f"UF_ACL_LIB does not exist: {self.acl_lib_path}")
        self._socket = SocketClient(self.socket_path)
        reg = self._request(
            op="RegisterClient",
            role=self.client_role,
            device_id=self.device_id,
            os_pid=os.getpid(),
            bare_tgid=self._acl.bare_tgid() if self._acl is not None else 0,
        )
        self._client_id = int(reg["client_id"])
        if self.model_id:
            self._request(op="RegisterModel", model_id=self.model_id, client_id=self._client_id)

    def _request(self, **kv: Any) -> dict[str, str]:
        if self._socket is None:
            raise RuntimeError("UFlow client is not connected")
        return self._socket.request(**kv)

    @staticmethod
    def _shape_text(shape: tuple[int, ...]) -> str:
        return "x".join(str(int(dim)) for dim in shape)

    @staticmethod
    def tensor_nbytes(shape: tuple[int, ...], dtype: torch.dtype) -> int:
        element_size = torch.empty((), dtype=dtype).element_size()
        count = 1
        for dim in shape:
            count *= int(dim)
        return count * element_size

    def _resolve_target(self, target: str | None) -> str:
        if target is None or target == "":
            return f"npu:{self.device_id}"
        if not target.startswith("npu:"):
            raise ValueError("UFlow HBM target must use npu:<device_id>")
        return target

    @staticmethod
    def _resolve_ddr_target(target: str | None) -> str:
        if target is None or target == "":
            target = os.environ.get("UF_DDR_TARGET", "host:0")
        if not target.startswith("host:"):
            raise ValueError("UFlow DDR target must use host:<numa_id>")
        int(target.split(":", 1)[1])
        return target

    @staticmethod
    def _resolve_ssd_target(target: str | None) -> str:
        if target is None or target == "":
            target = os.environ.get("UF_SSD_TARGET", "ssd:local0")
        if target != "ssd:local0":
            raise ValueError("UFlow SSD target must use ssd:local0")
        return target

    @staticmethod
    def _normalize_transfer_mode(mode: str | None) -> str:
        actual = mode or os.environ.get("UF_DDR_TRANSFER_MODE", "auto")
        if actual not in TRANSFER_MODES:
            raise ValueError(f"unknown UFlow transfer mode {actual!r}; expected one of {sorted(TRANSFER_MODES)}")
        return actual

    def capabilities(self) -> dict[str, str]:
        return self._request(op="GetCapabilities")

    def create_event_handle(self) -> AclEventHandle:
        if self._acl is None:
            raise RuntimeError("UFlow client ACL bridge is not initialized")
        event_id = self._acl.create_event()
        return AclEventHandle(event_id=event_id, raw_handle=self._acl.event_handle(event_id))

    def create_stream_handle(self) -> AclStreamHandle:
        if self._acl is None:
            raise RuntimeError("UFlow client ACL bridge is not initialized")
        return AclStreamHandle(stream_id=self._acl.create_stream())

    @staticmethod
    def _event_id(event: AclEventHandle | TransferCompletionEventHandle | int) -> int:
        return event.event_id if isinstance(event, (AclEventHandle, TransferCompletionEventHandle)) else int(event)

    @staticmethod
    def _stream_id(stream: AclStreamHandle | int) -> int:
        return stream.stream_id if isinstance(stream, AclStreamHandle) else int(stream)

    def destroy_event_handle(self, event: AclEventHandle | TransferCompletionEventHandle | int) -> None:
        if self._acl is None:
            raise RuntimeError("UFlow client ACL bridge is not initialized")
        self._acl.destroy_event(self._event_id(event))

    def destroy_stream_handle(self, stream: AclStreamHandle | int) -> None:
        if self._acl is None:
            raise RuntimeError("UFlow client ACL bridge is not initialized")
        self._acl.destroy_stream(self._stream_id(stream))

    def synchronize_event_handle(self, event: AclEventHandle | TransferCompletionEventHandle | int) -> None:
        if self._acl is None:
            raise RuntimeError("UFlow client ACL bridge is not initialized")
        self._acl.synchronize_event(self._event_id(event))

    def synchronize_stream_handle(self, stream: AclStreamHandle | int) -> None:
        if self._acl is None:
            raise RuntimeError("UFlow client ACL bridge is not initialized")
        self._acl.synchronize_stream(self._stream_id(stream))

    def record_event_handle(self, event: AclEventHandle | TransferCompletionEventHandle | int, stream: AclStreamHandle | int = 0) -> None:
        if self._acl is None:
            raise RuntimeError("UFlow client ACL bridge is not initialized")
        self._acl.record_event(self._event_id(event), self._stream_id(stream))

    def stream_wait_event_handle(self, stream: AclStreamHandle | int, event: AclEventHandle | TransferCompletionEventHandle | int) -> None:
        if self._acl is None:
            raise RuntimeError("UFlow client ACL bridge is not initialized")
        self._acl.stream_wait_event(self._stream_id(stream), self._event_id(event))

    def export_completion_event_proxy(
        self,
        event: TransferEvent | int,
        *,
        timeout_ms: int = 120_000,
        record_stream: AclStreamHandle | int = 0,
        wait_before_return: bool = False,
    ) -> TransferCompletionEventHandle:
        if self._acl is None:
            raise RuntimeError("UFlow client ACL bridge is not initialized")
        transfer_event_id = event.event_id if isinstance(event, TransferEvent) else int(event)
        source_completion_kind = event.completion_kind if isinstance(event, TransferEvent) else ""
        acl_event = self.create_event_handle()
        stream_id = self._stream_id(record_stream)
        if wait_before_return:
            final = self.wait_event(transfer_event_id, timeout_ms=timeout_ms)
            if final.status != "complete":
                raise RuntimeError(f"transfer event {transfer_event_id} ended with status={final.status}: {final.error_message}")
            self.record_event_handle(acl_event, stream_id)
            return TransferCompletionEventHandle(acl_event.event_id, acl_event.raw_handle, transfer_event_id, "host_proxy_blocking", source_completion_kind)

        def _wait_and_record() -> None:
            try:
                final = self.wait_event(transfer_event_id, timeout_ms=timeout_ms)
                if final.status != "complete":
                    raise RuntimeError(f"transfer event {transfer_event_id} ended with status={final.status}: {final.error_message}")
                self.record_event_handle(acl_event, stream_id)
            except BaseException as exc:  # noqa: BLE001
                self._completion_event_errors[acl_event.event_id] = exc

        thread = threading.Thread(target=_wait_and_record, name=f"uflow-transfer-event-proxy-{transfer_event_id}", daemon=True)
        self._completion_event_threads[acl_event.event_id] = thread
        thread.start()
        return TransferCompletionEventHandle(acl_event.event_id, acl_event.raw_handle, transfer_event_id, "host_proxy", source_completion_kind)

    def wait_completion_event_proxy(
        self,
        event: TransferCompletionEventHandle | int,
        *,
        timeout_s: float | None = None,
        synchronize_acl_event: bool = True,
    ) -> None:
        event_id = self._event_id(event)
        thread = self._completion_event_threads.get(event_id)
        if thread is not None:
            thread.join(timeout=timeout_s)
            if thread.is_alive():
                raise TimeoutError(f"timed out waiting for completion event proxy thread {event_id}")
            self._completion_event_threads.pop(event_id, None)
        error = self._completion_event_errors.pop(event_id, None)
        if error is not None:
            raise RuntimeError(f"completion event proxy {event_id} failed") from error
        if synchronize_acl_event:
            self.synchronize_event_handle(event_id)

    def submit_transfer_hotpath_event(
        self,
        src: HbmObject | DdrObject,
        dst: HbmObject | DdrObject,
        *,
        nbytes: int | None = None,
        mode: str | None = None,
        completion_export: str = "auto",
        timeout_ms: int = 120_000,
        plan: TransferPlan | None = None,
    ) -> TransferCompletionEventHandle:
        export_kind = (completion_export or "auto").lower()
        if export_kind not in {"auto", "host_proxy", "host_proxy_blocking"}:
            raise ValueError(
                "completion_export must be one of 'auto', 'host_proxy', or 'host_proxy_blocking'"
            )
        if plan is None:
            plan = self.plan_transfer(src=src, dst=dst, nbytes=nbytes, mode=mode, wait_policy="return_immediately")
        event = self.submit_transfer(plan)
        wait_before_return = env_flag("UF_A08_COMPLETION_PROXY_BLOCKING", export_kind != "host_proxy")
        if export_kind == "host_proxy":
            wait_before_return = False
        elif export_kind == "host_proxy_blocking":
            wait_before_return = True
        return self.export_completion_event_proxy(
            event,
            timeout_ms=timeout_ms,
            wait_before_return=wait_before_return,
        )

    def _map_hbm_response(
        self,
        *,
        resp: dict[str, str],
        name: str,
        role: str,
        shape: tuple[int, ...],
        dtype: torch.dtype,
        owner: bool,
        offset_bytes: int = 0,
    ) -> HbmObject:
        if self._acl is None:
            raise RuntimeError("UFlow client ACL bridge is not initialized")
        mapped = self._acl.import_and_map(shareable=int(resp["shareable"]), actual_bytes=int(resp["actual_bytes"]))
        obj = HbmObject(
            client=self,
            object_id=int(resp["object_id"]),
            placement_id=int(resp.get("placement_id", resp["object_id"])),
            lease_id=int(resp["lease_id"]),
            name=name,
            role=role,
            shape=tuple(int(dim) for dim in shape),
            dtype=dtype,
            requested_bytes=int(resp.get("requested_bytes", resp["actual_bytes"])),
            actual_bytes=int(resp["actual_bytes"]),
            shareable=int(resp["shareable"]),
            mapped=mapped,
            offset_bytes=int(offset_bytes),
            owner=owner,
        )
        self._objects[obj.lease_id] = obj
        return obj

    def allocate(
        self,
        *,
        name: str,
        role: str = "user",
        nbytes: int | None = None,
        hint: str = MANDATORY_HBM_HINT,
        target: str | None = None,
        shape: tuple[int, ...] | None = None,
        dtype: torch.dtype | None = None,
        immutable: bool = False,
        mark_ready: bool = True,
    ) -> HbmObject:
        if not self.active:
            raise RuntimeError("UFlow client is disabled")
        if self._acl is None:
            raise RuntimeError("UFlow HBM allocation requires an initialized ACL bridge")
        if hint != MANDATORY_HBM_HINT:
            raise ValueError(f"UFlow HBM allocation requires hint={MANDATORY_HBM_HINT!r}")
        dtype = dtype or torch.uint8
        if shape is None:
            if nbytes is None:
                raise ValueError("UFlow allocate requires nbytes when shape is omitted")
            shape = (int(nbytes),)
            dtype = torch.uint8
        shape = tuple(int(dim) for dim in shape)
        if nbytes is None:
            nbytes = self.tensor_nbytes(shape, dtype)
        resp = self._request(
            op="CreateDataObject",
            client_id=self._client_id,
            model_id=self.model_id,
            name=name,
            role=role,
            hint=hint,
            target=self._resolve_target(target),
            shape=self._shape_text(shape),
            dtype=str(dtype).replace("torch.", ""),
            nbytes=int(nbytes),
            immutable=1 if immutable else 0,
        )
        obj = self._map_hbm_response(resp=resp, name=name, role=role, shape=shape, dtype=dtype, owner=resp.get("existing", "0") != "1")
        if mark_ready:
            self.mark_ready(obj)
        return obj

    def upload(self, *, name: str, host_tensor: torch.Tensor, role: str = "user", target: str | None = None, immutable: bool = False) -> HbmObject:
        if host_tensor.device.type != "cpu":
            host_tensor = host_tensor.detach().cpu()
        if not host_tensor.is_contiguous():
            host_tensor = host_tensor.contiguous()
        obj = self.allocate(
            name=name,
            role=role,
            nbytes=int(host_tensor.nbytes),
            target=target,
            shape=tuple(int(dim) for dim in host_tensor.shape),
            dtype=host_tensor.dtype,
            immutable=immutable,
            mark_ready=False,
        )
        self.copy_to_device(obj, host_tensor)
        self.mark_ready(obj)
        return obj

    def _map_ddr_response(
        self,
        *,
        resp: dict[str, str],
        name: str,
        role: str,
        shape: tuple[int, ...],
        dtype: torch.dtype,
        owner: bool,
        offset_bytes: int = 0,
        visible_bytes: int | None = None,
    ) -> DdrObject:
        path = resp["ddr_path"]
        file_obj = open(path, "r+b")
        mapped = mmap.mmap(file_obj.fileno(), int(resp["actual_bytes"]))
        obj = DdrObject(
            client=self,
            object_id=int(resp["object_id"]),
            placement_id=int(resp.get("placement_id", resp["object_id"])),
            lease_id=int(resp["lease_id"]),
            name=name,
            role=role,
            shape=tuple(int(dim) for dim in shape),
            dtype=dtype,
            requested_bytes=int(resp.get("requested_bytes", visible_bytes or resp["actual_bytes"])),
            actual_bytes=int(resp["actual_bytes"]),
            path=path,
            offset_bytes=int(offset_bytes),
            visible_bytes=visible_bytes,
            owner=owner,
            _file=file_obj,
            _mmap=mapped,
        )
        self._ddr_objects[obj.lease_id] = obj
        return obj

    def allocate_ddr(
        self,
        *,
        name: str,
        role: str = "user",
        nbytes: int | None = None,
        target: str | None = None,
        shape: tuple[int, ...] | None = None,
        dtype: torch.dtype | None = None,
        immutable: bool = False,
        mark_ready: bool = True,
    ) -> DdrObject:
        if not self.active:
            raise RuntimeError("UFlow client is disabled")
        dtype = dtype or torch.uint8
        if shape is None:
            if nbytes is None:
                raise ValueError("UFlow allocate_ddr requires nbytes when shape is omitted")
            shape = (int(nbytes),)
            dtype = torch.uint8
        shape = tuple(int(dim) for dim in shape)
        if nbytes is None:
            nbytes = self.tensor_nbytes(shape, dtype)
        resp = self._request(
            op="CreateDataObject",
            client_id=self._client_id,
            model_id=self.model_id,
            name=name,
            role=role,
            hint=MANDATORY_DDR_HINT,
            target=self._resolve_ddr_target(target),
            shape=self._shape_text(shape),
            dtype=str(dtype).replace("torch.", ""),
            nbytes=int(nbytes),
            immutable=1 if immutable else 0,
        )
        obj = self._map_ddr_response(resp=resp, name=name, role=role, shape=shape, dtype=dtype, owner=resp.get("existing", "0") != "1")
        if mark_ready:
            self.mark_ready(obj)
        return obj

    def _map_ssd_response(
        self,
        *,
        resp: dict[str, str],
        name: str,
        role: str,
        shape: tuple[int, ...],
        dtype: torch.dtype,
        owner: bool,
        offset_bytes: int = 0,
        visible_bytes: int | None = None,
    ) -> SsdObject:
        obj = SsdObject(
            client=self,
            object_id=int(resp["object_id"]),
            placement_id=int(resp.get("placement_id", resp["object_id"])),
            lease_id=int(resp["lease_id"]),
            name=name,
            role=role,
            shape=tuple(int(dim) for dim in shape),
            dtype=dtype,
            requested_bytes=int(resp.get("requested_bytes", visible_bytes or resp["actual_bytes"])),
            actual_bytes=int(resp["actual_bytes"]),
            path=resp["ssd_path"],
            offset_bytes=int(resp.get("allowed_offset_bytes", resp.get("ssd_offset_bytes", offset_bytes))),
            visible_bytes=visible_bytes,
            alignment_bytes=int(resp.get("alignment_bytes", "4096")),
            io_mode=resp.get("ssd_io_mode", "buffered"),
            owner=owner,
        )
        self._ssd_objects[obj.lease_id] = obj
        return obj

    def allocate_ssd(
        self,
        *,
        name: str,
        role: str = "user",
        nbytes: int | None = None,
        target: str | None = None,
        shape: tuple[int, ...] | None = None,
        dtype: torch.dtype | None = None,
        immutable: bool = False,
        mark_ready: bool = True,
    ) -> SsdObject:
        if not self.active:
            raise RuntimeError("UFlow client is disabled")
        dtype = dtype or torch.uint8
        if shape is None:
            if nbytes is None:
                raise ValueError("UFlow allocate_ssd requires nbytes when shape is omitted")
            shape = (int(nbytes),)
            dtype = torch.uint8
        shape = tuple(int(dim) for dim in shape)
        if nbytes is None:
            nbytes = self.tensor_nbytes(shape, dtype)
        resp = self._request(
            op="CreateDataObject",
            client_id=self._client_id,
            model_id=self.model_id,
            name=name,
            role=role,
            hint=MANDATORY_SSD_HINT,
            target=self._resolve_ssd_target(target),
            shape=self._shape_text(shape),
            dtype=str(dtype).replace("torch.", ""),
            nbytes=int(nbytes),
            immutable=1 if immutable else 0,
        )
        obj = self._map_ssd_response(
            resp=resp,
            name=name,
            role=role,
            shape=shape,
            dtype=dtype,
            owner=resp.get("existing", "0") != "1",
        )
        if mark_ready:
            self.mark_ready(obj)
        return obj

    def open(
        self,
        *,
        object_id: int,
        name: str = "",
        role: str = "user",
        target: str | None = None,
        shape: tuple[int, ...] | None = None,
        dtype: torch.dtype | None = None,
        allowed_offset_bytes: int = 0,
        allowed_bytes: int = 0,
    ) -> HbmObject:
        if not self.active:
            raise RuntimeError("UFlow client is disabled")
        if self._acl is None:
            raise RuntimeError("UFlow HBM open requires an initialized ACL bridge")
        resp = self._request(
            op="OpenDataObject",
            client_id=self._client_id,
            object_id=int(object_id),
            target=self._resolve_target(target),
            allowed_offset_bytes=int(allowed_offset_bytes),
            allowed_bytes=int(allowed_bytes),
        )
        visible = int(allowed_bytes) if int(allowed_bytes) > 0 else int(resp.get("requested_bytes", resp["actual_bytes"]))
        dtype = dtype or torch.uint8
        shape = (visible,) if shape is None else tuple(int(dim) for dim in shape)
        return self._map_hbm_response(resp=resp, name=name or f"object.{object_id}", role=role, shape=shape, dtype=dtype, owner=False, offset_bytes=allowed_offset_bytes)

    def open_ddr(
        self,
        *,
        object_id: int,
        name: str = "",
        role: str = "user",
        target: str | None = None,
        shape: tuple[int, ...] | None = None,
        dtype: torch.dtype | None = None,
        allowed_offset_bytes: int = 0,
        allowed_bytes: int = 0,
    ) -> DdrObject:
        if not self.active:
            raise RuntimeError("UFlow client is disabled")
        resp = self._request(
            op="OpenDataObject",
            client_id=self._client_id,
            object_id=int(object_id),
            target=self._resolve_ddr_target(target),
            allowed_offset_bytes=int(allowed_offset_bytes),
            allowed_bytes=int(allowed_bytes),
        )
        requested_bytes = int(resp.get("requested_bytes", allowed_bytes or resp["actual_bytes"]))
        offset_bytes = int(resp.get("allowed_offset_bytes", allowed_offset_bytes))
        visible_bytes = int(resp.get("allowed_bytes", allowed_bytes))
        if visible_bytes <= 0:
            visible_bytes = max(requested_bytes - offset_bytes, 0)
        dtype = dtype or torch.uint8
        shape = (visible_bytes,) if shape is None else tuple(int(dim) for dim in shape)
        return self._map_ddr_response(
            resp=resp,
            name=name or f"ddr.{object_id}",
            role=role,
            shape=shape,
            dtype=dtype,
            owner=False,
            offset_bytes=offset_bytes,
            visible_bytes=visible_bytes,
        )

    def open_ssd(
        self,
        *,
        object_id: int,
        name: str = "",
        role: str = "user",
        target: str | None = None,
        shape: tuple[int, ...] | None = None,
        dtype: torch.dtype | None = None,
        allowed_offset_bytes: int = 0,
        allowed_bytes: int = 0,
    ) -> SsdObject:
        if not self.active:
            raise RuntimeError("UFlow client is disabled")
        resp = self._request(
            op="OpenDataObject",
            client_id=self._client_id,
            object_id=int(object_id),
            target=self._resolve_ssd_target(target),
            allowed_offset_bytes=int(allowed_offset_bytes),
            allowed_bytes=int(allowed_bytes),
        )
        requested_bytes = int(resp.get("requested_bytes", allowed_bytes or resp["actual_bytes"]))
        offset_bytes = int(resp.get("allowed_offset_bytes", allowed_offset_bytes))
        visible_bytes = int(resp.get("allowed_bytes", allowed_bytes))
        if visible_bytes <= 0:
            visible_bytes = max(requested_bytes - offset_bytes, 0)
        dtype = dtype or torch.uint8
        shape = (visible_bytes,) if shape is None else tuple(int(dim) for dim in shape)
        return self._map_ssd_response(
            resp=resp,
            name=name or f"ssd.{object_id}",
            role=role,
            shape=shape,
            dtype=dtype,
            owner=False,
            offset_bytes=offset_bytes,
            visible_bytes=visible_bytes,
        )

    def copy_to_device(self, managed: HbmObject, host_tensor: torch.Tensor, *, offset_bytes: int = 0) -> None:
        if host_tensor.device.type != "cpu":
            raise ValueError("copy_to_device source must be a CPU tensor")
        if not host_tensor.is_contiguous():
            raise ValueError("copy_to_device source must be contiguous")
        if int(offset_bytes) < 0 or int(offset_bytes) + int(host_tensor.nbytes) > managed.requested_bytes:
            raise ValueError("source tensor is larger than HBM object")
        if self._acl is None:
            raise RuntimeError("UFlow client ACL bridge is not initialized")
        self._acl.h2d(managed.device_ptr + int(offset_bytes), host_tensor.data_ptr(), int(host_tensor.nbytes))

    def copy_from_device(self, managed: HbmObject, host_tensor: torch.Tensor, *, offset_bytes: int = 0) -> None:
        if host_tensor.device.type != "cpu":
            raise ValueError("copy_from_device destination must be a CPU tensor")
        if not host_tensor.is_contiguous():
            raise ValueError("copy_from_device destination must be contiguous")
        if int(offset_bytes) < 0 or int(offset_bytes) + int(host_tensor.nbytes) > managed.requested_bytes:
            raise ValueError("destination tensor is larger than HBM object")
        if self._acl is None:
            raise RuntimeError("UFlow client ACL bridge is not initialized")
        self._acl.d2h(host_tensor.data_ptr(), managed.device_ptr + int(offset_bytes), int(host_tensor.nbytes))

    def copy_device_to_device(self, src: HbmObject, dst: HbmObject, *, nbytes: int | None = None) -> None:
        if self._acl is None:
            raise RuntimeError("UFlow client ACL bridge is not initialized")
        nbytes = int(src.requested_bytes if nbytes is None else nbytes)
        self._acl.d2d(dst.device_ptr, src.device_ptr, nbytes)

    def describe_object(self, object_id: int) -> tuple[DataObject, DataPlacement]:
        resp = self._request(op="DescribeObject", client_id=self._client_id, object_id=int(object_id))
        return DataObject.from_response(resp), DataPlacement.from_response(resp)

    def estimate_cost(
        self,
        *,
        src: HbmObject | DdrObject | SsdObject,
        dst: HbmObject | DdrObject | SsdObject,
        nbytes: int | None = None,
        mode: str | None = None,
        src_offset_bytes: int = 0,
        dst_offset_bytes: int = 0,
    ) -> TransferCost:
        actual_mode = self._normalize_transfer_mode(mode)
        resp = self._request(
            op="EstimateCost",
            client_id=self._client_id,
            src_object_id=src.object_id,
            dst_object_id=dst.object_id,
            src_placement_id=src.placement_id,
            dst_placement_id=dst.placement_id,
            src_offset_bytes=int(src_offset_bytes),
            dst_offset_bytes=int(dst_offset_bytes),
            nbytes=int(min(src.requested_bytes, dst.requested_bytes) if nbytes is None else nbytes),
            mode=actual_mode,
        )
        return TransferCost.from_response(resp)

    def plan_transfer(
        self,
        *,
        src: HbmObject | DdrObject | SsdObject,
        dst: HbmObject | DdrObject | SsdObject,
        nbytes: int | None = None,
        operation: str = "copy",
        mode: str | None = None,
        wait_policy: str = "return_immediately",
        src_offset_bytes: int = 0,
        dst_offset_bytes: int = 0,
    ) -> TransferPlan:
        actual_mode = self._normalize_transfer_mode(mode)
        request = TransferRequest(
            src_object_id=src.object_id,
            dst_object_id=dst.object_id,
            nbytes=int(min(src.requested_bytes, dst.requested_bytes) if nbytes is None else nbytes),
            operation=operation,
            wait_policy=wait_policy,
            mode=actual_mode,
            src_offset_bytes=int(src_offset_bytes),
            dst_offset_bytes=int(dst_offset_bytes),
        )
        resp = self._request(
            op="PlanTransfer",
            client_id=self._client_id,
            src_placement_id=src.placement_id,
            dst_placement_id=dst.placement_id,
            **request.as_kv(),
        )
        return TransferPlan.from_response(resp)

    def submit_transfer(self, plan: TransferPlan, *, wait_policy: str | None = None) -> TransferEvent:
        resp = self._request(op="SubmitTransfer", client_id=self._client_id, plan_id=plan.plan_id, wait_policy=wait_policy or plan.wait_policy)
        event = TransferEvent.from_response(resp)
        if (wait_policy or plan.wait_policy) == "wait_complete":
            return self.wait_event(event.event_id)
        return event

    def poll_event(self, event: TransferEvent | int) -> TransferEvent:
        event_id = event.event_id if isinstance(event, TransferEvent) else int(event)
        return TransferEvent.from_response(self._request(op="PollEvent", client_id=self._client_id, event_id=event_id))

    def wait_event(self, event: TransferEvent | int, *, timeout_ms: int = 120_000) -> TransferEvent:
        event_id = event.event_id if isinstance(event, TransferEvent) else int(event)
        return TransferEvent.from_response(self._request(op="WaitEvent", client_id=self._client_id, event_id=event_id, timeout_ms=int(timeout_ms)))

    def cancel_event(self, event: TransferEvent | int) -> TransferEvent:
        event_id = event.event_id if isinstance(event, TransferEvent) else int(event)
        return TransferEvent.from_response(self._request(op="CancelEvent", client_id=self._client_id, event_id=event_id))

    def transfer_sync(self, *, src: HbmObject | DdrObject | SsdObject, dst: HbmObject | DdrObject | SsdObject, nbytes: int | None = None, mode: str | None = None) -> TransferEvent:
        plan = self.plan_transfer(src=src, dst=dst, nbytes=nbytes, mode=mode, wait_policy="return_immediately")
        event = self.submit_transfer(plan)
        return self.wait_event(event)

    def mark_ready(self, obj: HbmObject | DdrObject | SsdObject) -> None:
        if self.active:
            self._request(op="MarkReady", client_id=self._client_id, object_id=obj.object_id, lease_id=obj.lease_id)

    def mark_modified(self, obj: HbmObject | DdrObject | SsdObject, *, offset_bytes: int = 0, nbytes: int | None = None) -> None:
        if self.active:
            self._request(
                op="MarkModified",
                client_id=self._client_id,
                object_id=obj.object_id,
                lease_id=obj.lease_id,
                modified_offset_bytes=int(offset_bytes),
                modified_bytes=int(obj.requested_bytes if nbytes is None else nbytes),
            )

    def mark_dirty(self, obj: HbmObject | DdrObject | SsdObject, *, offset_bytes: int = 0, nbytes: int | None = None) -> None:
        if self.active:
            self._request(
                op="MarkDirty",
                client_id=self._client_id,
                object_id=obj.object_id,
                lease_id=obj.lease_id,
                modified_offset_bytes=int(offset_bytes),
                modified_bytes=int(obj.requested_bytes if nbytes is None else nbytes),
            )

    def stats(self, role_filter: str | None = None) -> dict[str, str]:
        if not self.active:
            return {}
        req: dict[str, Any] = {"op": "GetStats"}
        if role_filter:
            req["role_filter"] = role_filter
        return self._request(**req)

    def get_model_objects(self, model_id: str | None = None) -> dict[str, str]:
        if not self.active:
            return {}
        return self._request(op="GetModelObjects", model_id=model_id or self.model_id)

    def _release_hbm_object(self, obj: HbmObject, *, release_object: bool | None = None) -> None:
        if self._acl is not None:
            self._acl.unmap_and_release(obj.mapped)
        if self.active and self._client_id is not None:
            self._request(op="CloseLease", client_id=self._client_id, lease_id=obj.lease_id)
            should_release_object = obj.owner if release_object is None else bool(release_object)
            if should_release_object:
                self._request(op="ReleaseDataObject", client_id=self._client_id, object_id=obj.object_id)
        self._objects.pop(obj.lease_id, None)

    def _release_ddr_object(self, obj: DdrObject, *, release_object: bool | None = None) -> None:
        obj.close_mapping()
        if self.active and self._client_id is not None:
            self._request(op="CloseLease", client_id=self._client_id, lease_id=obj.lease_id)
            should_release_object = obj.owner if release_object is None else bool(release_object)
            if should_release_object:
                self._request(op="ReleaseDataObject", client_id=self._client_id, object_id=obj.object_id)
        self._ddr_objects.pop(obj.lease_id, None)

    def _release_ssd_object(self, obj: SsdObject, *, release_object: bool | None = None) -> None:
        if self.active and self._client_id is not None:
            self._request(op="CloseLease", client_id=self._client_id, lease_id=obj.lease_id)
            should_release_object = obj.owner if release_object is None else bool(release_object)
            if should_release_object:
                self._request(op="ReleaseDataObject", client_id=self._client_id, object_id=obj.object_id)
        self._ssd_objects.pop(obj.lease_id, None)

    def close(self) -> None:
        for event_id, thread in list(self._completion_event_threads.items()):
            thread.join(timeout=1.0)
            self._completion_event_threads.pop(event_id, None)
            self._completion_event_errors.pop(event_id, None)
        for obj in list(self._objects.values()):
            obj.release(release_object=False)
        for obj in list(self._ddr_objects.values()):
            obj.release(release_object=False)
        for obj in list(self._ssd_objects.values()):
            obj.release(release_object=False)
        self._socket = None
        self._acl = None
        self._client_id = None

    def __enter__(self) -> "UFlowClient":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()
