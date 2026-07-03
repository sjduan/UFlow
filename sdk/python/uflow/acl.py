from __future__ import annotations

import ctypes

class _UfAclStatus(ctypes.Structure):
    _fields_ = [
        ("code", ctypes.c_int32),
        ("message", ctypes.c_char * 256),
    ]


class _UfAclInitOptions(ctypes.Structure):
    _fields_ = [
        ("device_id", ctypes.c_int32),
        ("flags", ctypes.c_uint32),
    ]


class _UfAclHbmAllocRequest(ctypes.Structure):
    _fields_ = [
        ("device_id", ctypes.c_int32),
        ("requested_bytes", ctypes.c_uint64),
        ("alignment", ctypes.c_uint64),
        ("memory_type", ctypes.c_uint32),
    ]


class _UfAclHbmBlock(ctypes.Structure):
    _fields_ = [
        ("raw_handle_id", ctypes.c_uint64),
        ("shareable_handle_payload", ctypes.c_uint64 * 8),
        ("shareable_handle_bytes", ctypes.c_uint64),
        ("requested_bytes", ctypes.c_uint64),
        ("actual_bytes", ctypes.c_uint64),
        ("granularity", ctypes.c_uint64),
        ("service_mapping_id", ctypes.c_uint64),
        ("service_device_ptr", ctypes.c_void_p),
        ("device_id", ctypes.c_int32),
    ]


class _UfAclClientImportRequest(ctypes.Structure):
    _fields_ = [
        ("device_id", ctypes.c_int32),
        ("shareable_handle_payload", ctypes.c_uint64 * 8),
        ("shareable_handle_bytes", ctypes.c_uint64),
        ("actual_bytes", ctypes.c_uint64),
    ]


class _UfAclMappedMemory(ctypes.Structure):
    _fields_ = [
        ("imported_handle_id", ctypes.c_uint64),
        ("device_ptr", ctypes.c_void_p),
        ("actual_bytes", ctypes.c_uint64),
        ("device_id", ctypes.c_int32),
    ]


class _UfAclHostMemory(ctypes.Structure):
    _fields_ = [
        ("host_handle_id", ctypes.c_uint64),
        ("host_ptr", ctypes.c_void_p),
        ("bytes", ctypes.c_uint64),
    ]


class _UfAclHostRegisterRequest(ctypes.Structure):
    _fields_ = [
        ("device_id", ctypes.c_int32),
        ("host_ptr", ctypes.c_void_p),
        ("bytes", ctypes.c_uint64),
        ("flags", ctypes.c_uint32),
        ("use_v2", ctypes.c_uint32),
    ]


class _UfAclHostRegisterInfo(ctypes.Structure):
    _fields_ = [
        ("registered_host_id", ctypes.c_uint64),
        ("host_ptr", ctypes.c_void_p),
        ("device_ptr", ctypes.c_void_p),
        ("bytes", ctypes.c_uint64),
        ("device_id", ctypes.c_int32),
        ("use_v2", ctypes.c_uint32),
    ]


class _AclClient:
    def __init__(self, lib_path: str, device_id: int) -> None:
        self.lib_path = lib_path
        self.device_id = int(device_id)
        self.lib = ctypes.CDLL(lib_path)
        self._bind()
        self._client_init()

    def _bind(self) -> None:
        lib = self.lib
        lib.uf_acl_backend_init.argtypes = [
            ctypes.POINTER(_UfAclInitOptions),
            ctypes.POINTER(_UfAclStatus),
        ]
        lib.uf_acl_backend_init.restype = ctypes.c_int
        lib.uf_acl_backend_finalize.argtypes = [ctypes.POINTER(_UfAclStatus)]
        lib.uf_acl_backend_finalize.restype = ctypes.c_int
        lib.uf_acl_alloc_physical.argtypes = [
            ctypes.POINTER(_UfAclHbmAllocRequest),
            ctypes.POINTER(_UfAclHbmBlock),
            ctypes.POINTER(_UfAclStatus),
        ]
        lib.uf_acl_alloc_physical.restype = ctypes.c_int
        lib.uf_acl_free_physical.argtypes = [
            ctypes.c_uint64,
            ctypes.POINTER(_UfAclStatus),
        ]
        lib.uf_acl_free_physical.restype = ctypes.c_int
        lib.uf_acl_client_init.argtypes = [ctypes.c_int32, ctypes.POINTER(_UfAclStatus)]
        lib.uf_acl_client_init.restype = ctypes.c_int
        lib.uf_acl_client_get_bare_tgid.argtypes = [
            ctypes.POINTER(ctypes.c_int64),
            ctypes.POINTER(_UfAclStatus),
        ]
        lib.uf_acl_client_get_bare_tgid.restype = ctypes.c_int
        lib.uf_acl_import_and_map.argtypes = [
            ctypes.POINTER(_UfAclClientImportRequest),
            ctypes.POINTER(_UfAclMappedMemory),
            ctypes.POINTER(_UfAclStatus),
        ]
        lib.uf_acl_import_and_map.restype = ctypes.c_int
        lib.uf_acl_h2d.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.POINTER(_UfAclStatus),
        ]
        lib.uf_acl_h2d.restype = ctypes.c_int
        lib.uf_acl_d2h.argtypes = [
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.POINTER(_UfAclStatus),
        ]
        lib.uf_acl_d2h.restype = ctypes.c_int
        lib.uf_acl_d2d.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.POINTER(_UfAclStatus),
        ]
        lib.uf_acl_d2d.restype = ctypes.c_int
        lib.uf_acl_malloc_host.argtypes = [
            ctypes.c_uint64,
            ctypes.POINTER(_UfAclHostMemory),
            ctypes.POINTER(_UfAclStatus),
        ]
        lib.uf_acl_malloc_host.restype = ctypes.c_int
        lib.uf_acl_free_host.argtypes = [
            ctypes.POINTER(_UfAclHostMemory),
            ctypes.POINTER(_UfAclStatus),
        ]
        lib.uf_acl_free_host.restype = ctypes.c_int
        lib.uf_acl_host_register.argtypes = [
            ctypes.POINTER(_UfAclHostRegisterRequest),
            ctypes.POINTER(_UfAclHostRegisterInfo),
            ctypes.POINTER(_UfAclStatus),
        ]
        lib.uf_acl_host_register.restype = ctypes.c_int
        lib.uf_acl_host_get_device_pointer.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.POINTER(_UfAclStatus),
        ]
        lib.uf_acl_host_get_device_pointer.restype = ctypes.c_int
        lib.uf_acl_host_unregister.argtypes = [
            ctypes.POINTER(_UfAclHostRegisterInfo),
            ctypes.POINTER(_UfAclStatus),
        ]
        lib.uf_acl_host_unregister.restype = ctypes.c_int
        lib.uf_acl_create_stream.argtypes = [ctypes.POINTER(ctypes.c_uint64), ctypes.POINTER(_UfAclStatus)]
        lib.uf_acl_create_stream.restype = ctypes.c_int
        lib.uf_acl_destroy_stream.argtypes = [ctypes.c_uint64, ctypes.POINTER(_UfAclStatus)]
        lib.uf_acl_destroy_stream.restype = ctypes.c_int
        lib.uf_acl_create_event.argtypes = [ctypes.POINTER(ctypes.c_uint64), ctypes.POINTER(_UfAclStatus)]
        lib.uf_acl_create_event.restype = ctypes.c_int
        lib.uf_acl_destroy_event.argtypes = [ctypes.c_uint64, ctypes.POINTER(_UfAclStatus)]
        lib.uf_acl_destroy_event.restype = ctypes.c_int
        lib.uf_acl_get_event_handle.argtypes = [
            ctypes.c_uint64,
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(_UfAclStatus),
        ]
        lib.uf_acl_get_event_handle.restype = ctypes.c_int
        lib.uf_acl_record_event.argtypes = [ctypes.c_uint64, ctypes.c_uint64, ctypes.POINTER(_UfAclStatus)]
        lib.uf_acl_record_event.restype = ctypes.c_int
        lib.uf_acl_stream_wait_event.argtypes = [ctypes.c_uint64, ctypes.c_uint64, ctypes.POINTER(_UfAclStatus)]
        lib.uf_acl_stream_wait_event.restype = ctypes.c_int
        lib.uf_acl_synchronize_stream.argtypes = [ctypes.c_uint64, ctypes.POINTER(_UfAclStatus)]
        lib.uf_acl_synchronize_stream.restype = ctypes.c_int
        lib.uf_acl_synchronize_event.argtypes = [ctypes.c_uint64, ctypes.POINTER(_UfAclStatus)]
        lib.uf_acl_synchronize_event.restype = ctypes.c_int
        lib.uf_acl_h2d_async.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.POINTER(_UfAclStatus),
        ]
        lib.uf_acl_h2d_async.restype = ctypes.c_int
        lib.uf_acl_d2h_async.argtypes = [
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.POINTER(_UfAclStatus),
        ]
        lib.uf_acl_d2h_async.restype = ctypes.c_int
        lib.uf_acl_unmap_and_release.argtypes = [
            ctypes.POINTER(_UfAclMappedMemory),
            ctypes.POINTER(_UfAclStatus),
        ]
        lib.uf_acl_unmap_and_release.restype = ctypes.c_int

    @staticmethod
    def _check(rc: int, status: _UfAclStatus, what: str) -> None:
        if rc == 0:
            return
        message = bytes(status.message).split(b"\0", 1)[0].decode(errors="replace")
        raise RuntimeError(f"{what} failed: {message}")

    def _client_init(self) -> None:
        status = _UfAclStatus()
        rc = self.lib.uf_acl_client_init(self.device_id, ctypes.byref(status))
        self._check(rc, status, "uf_acl_client_init")

    def backend_init(self) -> None:
        status = _UfAclStatus()
        opts = _UfAclInitOptions()
        opts.device_id = self.device_id
        opts.flags = 0
        rc = self.lib.uf_acl_backend_init(ctypes.byref(opts), ctypes.byref(status))
        self._check(rc, status, "uf_acl_backend_init")

    def backend_finalize(self) -> None:
        status = _UfAclStatus()
        rc = self.lib.uf_acl_backend_finalize(ctypes.byref(status))
        self._check(rc, status, "uf_acl_backend_finalize")

    def alloc_physical(self, nbytes: int, *, alignment: int = 0, memory_type: int = 0) -> _UfAclHbmBlock:
        self.backend_init()
        status = _UfAclStatus()
        req = _UfAclHbmAllocRequest()
        req.device_id = self.device_id
        req.requested_bytes = int(nbytes)
        req.alignment = int(alignment)
        req.memory_type = int(memory_type)
        block = _UfAclHbmBlock()
        rc = self.lib.uf_acl_alloc_physical(ctypes.byref(req), ctypes.byref(block), ctypes.byref(status))
        self._check(rc, status, "uf_acl_alloc_physical")
        return block

    def free_physical(self, block: _UfAclHbmBlock) -> None:
        status = _UfAclStatus()
        rc = self.lib.uf_acl_free_physical(ctypes.c_uint64(int(block.raw_handle_id)), ctypes.byref(status))
        self._check(rc, status, "uf_acl_free_physical")

    def bare_tgid(self) -> int:
        status = _UfAclStatus()
        out = ctypes.c_int64(-1)
        rc = self.lib.uf_acl_client_get_bare_tgid(ctypes.byref(out), ctypes.byref(status))
        self._check(rc, status, "uf_acl_client_get_bare_tgid")
        return int(out.value)

    def import_and_map(self, *, shareable: int, actual_bytes: int) -> _UfAclMappedMemory:
        status = _UfAclStatus()
        req = _UfAclClientImportRequest()
        req.device_id = self.device_id
        req.shareable_handle_payload[0] = int(shareable)
        req.shareable_handle_bytes = ctypes.sizeof(ctypes.c_uint64)
        req.actual_bytes = int(actual_bytes)
        mapped = _UfAclMappedMemory()
        rc = self.lib.uf_acl_import_and_map(ctypes.byref(req), ctypes.byref(mapped), ctypes.byref(status))
        self._check(rc, status, "uf_acl_import_and_map")
        return mapped

    def h2d(self, device_ptr: int, host_ptr: int, nbytes: int, offset: int = 0) -> None:
        self._client_init()
        status = _UfAclStatus()
        rc = self.lib.uf_acl_h2d(
            ctypes.c_void_p(int(device_ptr)),
            ctypes.c_uint64(int(offset)),
            ctypes.c_void_p(int(host_ptr)),
            ctypes.c_uint64(int(nbytes)),
            ctypes.byref(status),
        )
        self._check(rc, status, "uf_acl_h2d")

    def d2h(self, host_ptr: int, device_ptr: int, nbytes: int, offset: int = 0) -> None:
        self._client_init()
        status = _UfAclStatus()
        rc = self.lib.uf_acl_d2h(
            ctypes.c_void_p(int(host_ptr)),
            ctypes.c_void_p(int(device_ptr)),
            ctypes.c_uint64(int(offset)),
            ctypes.c_uint64(int(nbytes)),
            ctypes.byref(status),
        )
        self._check(rc, status, "uf_acl_d2h")

    def d2d(
        self,
        dst_device_ptr: int,
        src_device_ptr: int,
        nbytes: int,
        *,
        dst_offset: int = 0,
        src_offset: int = 0,
    ) -> None:
        self._client_init()
        status = _UfAclStatus()
        rc = self.lib.uf_acl_d2d(
            ctypes.c_void_p(int(dst_device_ptr)),
            ctypes.c_uint64(int(dst_offset)),
            ctypes.c_void_p(int(src_device_ptr)),
            ctypes.c_uint64(int(src_offset)),
            ctypes.c_uint64(int(nbytes)),
            ctypes.byref(status),
        )
        self._check(rc, status, "uf_acl_d2d")

    def malloc_host(self, nbytes: int) -> _UfAclHostMemory:
        status = _UfAclStatus()
        host = _UfAclHostMemory()
        rc = self.lib.uf_acl_malloc_host(ctypes.c_uint64(int(nbytes)), ctypes.byref(host), ctypes.byref(status))
        self._check(rc, status, "uf_acl_malloc_host")
        return host

    def free_host(self, host: _UfAclHostMemory) -> None:
        status = _UfAclStatus()
        rc = self.lib.uf_acl_free_host(ctypes.byref(host), ctypes.byref(status))
        self._check(rc, status, "uf_acl_free_host")

    def host_register(
        self,
        host_ptr: int,
        nbytes: int,
        *,
        flags: int = 0,
        use_v2: bool = False,
        device_id: int | None = None,
    ) -> _UfAclHostRegisterInfo:
        status = _UfAclStatus()
        req = _UfAclHostRegisterRequest()
        req.device_id = self.device_id if device_id is None else int(device_id)
        req.host_ptr = int(host_ptr)
        req.bytes = int(nbytes)
        req.flags = int(flags)
        req.use_v2 = 1 if use_v2 else 0
        info = _UfAclHostRegisterInfo()
        rc = self.lib.uf_acl_host_register(ctypes.byref(req), ctypes.byref(info), ctypes.byref(status))
        self._check(rc, status, "uf_acl_host_register")
        return info

    def host_get_device_pointer(self, host_ptr: int) -> int:
        status = _UfAclStatus()
        out = ctypes.c_void_p()
        rc = self.lib.uf_acl_host_get_device_pointer(
            ctypes.c_void_p(int(host_ptr)),
            ctypes.byref(out),
            ctypes.byref(status),
        )
        self._check(rc, status, "uf_acl_host_get_device_pointer")
        return 0 if out.value is None else int(out.value)

    def host_unregister(self, info: _UfAclHostRegisterInfo) -> None:
        status = _UfAclStatus()
        rc = self.lib.uf_acl_host_unregister(ctypes.byref(info), ctypes.byref(status))
        self._check(rc, status, "uf_acl_host_unregister")

    def create_stream(self) -> int:
        status = _UfAclStatus()
        out = ctypes.c_uint64(0)
        rc = self.lib.uf_acl_create_stream(ctypes.byref(out), ctypes.byref(status))
        self._check(rc, status, "uf_acl_create_stream")
        return int(out.value)

    def destroy_stream(self, stream_id: int) -> None:
        status = _UfAclStatus()
        rc = self.lib.uf_acl_destroy_stream(ctypes.c_uint64(int(stream_id)), ctypes.byref(status))
        self._check(rc, status, "uf_acl_destroy_stream")

    def create_event(self) -> int:
        status = _UfAclStatus()
        out = ctypes.c_uint64(0)
        rc = self.lib.uf_acl_create_event(ctypes.byref(out), ctypes.byref(status))
        self._check(rc, status, "uf_acl_create_event")
        return int(out.value)

    def destroy_event(self, event_id: int) -> None:
        status = _UfAclStatus()
        rc = self.lib.uf_acl_destroy_event(ctypes.c_uint64(int(event_id)), ctypes.byref(status))
        self._check(rc, status, "uf_acl_destroy_event")

    def event_handle(self, event_id: int) -> int:
        status = _UfAclStatus()
        out = ctypes.c_uint64(0)
        rc = self.lib.uf_acl_get_event_handle(
            ctypes.c_uint64(int(event_id)),
            ctypes.byref(out),
            ctypes.byref(status),
        )
        self._check(rc, status, "uf_acl_get_event_handle")
        return int(out.value)

    def record_event(self, event_id: int, stream_id: int = 0) -> None:
        status = _UfAclStatus()
        rc = self.lib.uf_acl_record_event(
            ctypes.c_uint64(int(event_id)),
            ctypes.c_uint64(int(stream_id)),
            ctypes.byref(status),
        )
        self._check(rc, status, "uf_acl_record_event")

    def stream_wait_event(self, stream_id: int, event_id: int) -> None:
        status = _UfAclStatus()
        rc = self.lib.uf_acl_stream_wait_event(
            ctypes.c_uint64(int(stream_id)),
            ctypes.c_uint64(int(event_id)),
            ctypes.byref(status),
        )
        self._check(rc, status, "uf_acl_stream_wait_event")

    def synchronize_stream(self, stream_id: int) -> None:
        status = _UfAclStatus()
        rc = self.lib.uf_acl_synchronize_stream(ctypes.c_uint64(int(stream_id)), ctypes.byref(status))
        self._check(rc, status, "uf_acl_synchronize_stream")

    def synchronize_event(self, event_id: int) -> None:
        status = _UfAclStatus()
        rc = self.lib.uf_acl_synchronize_event(ctypes.c_uint64(int(event_id)), ctypes.byref(status))
        self._check(rc, status, "uf_acl_synchronize_event")

    def h2d_async(
        self,
        device_ptr: int,
        host_ptr: int,
        nbytes: int,
        *,
        offset: int = 0,
        stream_id: int,
        event_id: int,
    ) -> None:
        self._client_init()
        status = _UfAclStatus()
        rc = self.lib.uf_acl_h2d_async(
            ctypes.c_void_p(int(device_ptr)),
            ctypes.c_uint64(int(offset)),
            ctypes.c_void_p(int(host_ptr)),
            ctypes.c_uint64(int(nbytes)),
            ctypes.c_uint64(int(stream_id)),
            ctypes.c_uint64(int(event_id)),
            ctypes.byref(status),
        )
        self._check(rc, status, "uf_acl_h2d_async")

    def d2h_async(
        self,
        host_ptr: int,
        device_ptr: int,
        nbytes: int,
        *,
        offset: int = 0,
        stream_id: int,
        event_id: int,
    ) -> None:
        self._client_init()
        status = _UfAclStatus()
        rc = self.lib.uf_acl_d2h_async(
            ctypes.c_void_p(int(host_ptr)),
            ctypes.c_void_p(int(device_ptr)),
            ctypes.c_uint64(int(offset)),
            ctypes.c_uint64(int(nbytes)),
            ctypes.c_uint64(int(stream_id)),
            ctypes.c_uint64(int(event_id)),
            ctypes.byref(status),
        )
        self._check(rc, status, "uf_acl_d2h_async")

    def unmap_and_release(self, mapped: _UfAclMappedMemory) -> None:
        status = _UfAclStatus()
        rc = self.lib.uf_acl_unmap_and_release(ctypes.byref(mapped), ctypes.byref(status))
        self._check(rc, status, "uf_acl_unmap_and_release")


