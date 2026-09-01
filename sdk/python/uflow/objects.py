from __future__ import annotations

import ctypes
import mmap
from dataclasses import dataclass
from typing import Any

import torch

from .acl import _UfAclMappedMemory


class HandleNotLocallyAddressable(RuntimeError):
    pass


@dataclass
class SimulatedObject:
    client: Any
    object_id: int
    placement_id: int
    binding_id: int
    name: str
    role: str
    requested_bytes: int
    node_id: str
    domain: str
    provider_id: str
    provider_session_id: str
    execution_realm_id: str
    fidelity: str
    addressability: str
    seed_checksum: int = 0
    released: bool = False

    @property
    def placement(self) -> str:
        return "ddr"

    @property
    def data_ptr(self) -> int:
        raise HandleNotLocallyAddressable(
            "simulated provider-only handle cannot be dereferenced on the Host"
        )

    def as_memoryview(self) -> memoryview:
        raise HandleNotLocallyAddressable(
            "simulated provider-only handle cannot be mapped on the Host"
        )

    def release(self) -> None:
        if not self.released:
            self.client.release_simulated(self)
            self.released = True


@dataclass
class SimulatedAccessView:
    client: Any
    object_id: int
    placement_id: int
    source_binding_id: int
    access_binding_id: int
    accessor_node_id: str
    provider_completion_id: str
    accessor_provider_id: str
    accessor_provider_session_id: str
    accessor_resource_id: str
    placement_count: int
    remote_map_created_placement: bool
    plan_id: int
    parent_event_id: int
    import_step_event_id: int
    checksum_step_event_id: int = 0
    checksum_completion_id: str = ""
    parent_terminal_count: int = 0
    access_mode: str = "read"
    transport_kind: str = "obmm_gsva"
    state: str = "Ready"

    @property
    def data_ptr(self) -> int:
        raise HandleNotLocallyAddressable(
            "remote AccessBinding is provider-only and cannot be dereferenced on the Host"
        )

    def checksum(self, *, offset_bytes: int = 0, nbytes: int | None = None) -> int:
        return self.client.checksum_access(
            self,
            offset_bytes=offset_bytes,
            nbytes=nbytes,
        )

    def revoke(self) -> None:
        self.client.revoke_access(self)
        self.state = "Stale"


@dataclass
class HbmObject:
    client: Any
    object_id: int
    placement_id: int
    lease_id: int
    name: str
    role: str
    shape: tuple[int, ...]
    dtype: torch.dtype
    requested_bytes: int
    actual_bytes: int
    shareable: int
    mapped: _UfAclMappedMemory
    offset_bytes: int = 0
    owner: bool = True
    released: bool = False

    @property
    def placement(self) -> str:
        return "hbm"

    @property
    def device_ptr(self) -> int:
        if self.mapped.device_ptr is None:
            return 0
        return int(self.mapped.device_ptr) + int(self.offset_bytes)

    @property
    def data_ptr(self) -> int:
        return self.device_ptr

    def release(self, *, release_object: bool | None = None) -> None:
        if self.released:
            return
        self.client._release_hbm_object(self, release_object=release_object)
        self.released = True


@dataclass
class DdrObject:
    client: Any
    object_id: int
    placement_id: int
    lease_id: int
    name: str
    role: str
    shape: tuple[int, ...]
    dtype: torch.dtype
    requested_bytes: int
    actual_bytes: int
    path: str
    offset_bytes: int = 0
    visible_bytes: int | None = None
    owner: bool = True
    released: bool = False
    _file: Any | None = None
    _mmap: mmap.mmap | None = None

    @property
    def placement(self) -> str:
        return "ddr"

    @property
    def data_ptr(self) -> int:
        if self._mmap is None:
            return 0
        return ctypes.addressof(ctypes.c_char.from_buffer(self._mmap)) + int(self.offset_bytes)

    @property
    def nbytes(self) -> int:
        if self.visible_bytes is not None:
            return int(self.visible_bytes)
        return int(self.requested_bytes) - int(self.offset_bytes)

    def as_memoryview(self) -> memoryview:
        if self._mmap is None:
            raise RuntimeError("DDR object is not mapped")
        return memoryview(self._mmap)[self.offset_bytes : self.offset_bytes + self.nbytes]

    def close_mapping(self) -> None:
        if self._mmap is not None:
            self._mmap.close()
            self._mmap = None
        if self._file is not None:
            self._file.close()
            self._file = None

    def release(self, *, release_object: bool | None = None) -> None:
        if self.released:
            return
        self.client._release_ddr_object(self, release_object=release_object)
        self.released = True


ManagedBuffer = HbmObject
DdrBuffer = DdrObject


@dataclass
class SsdObject:
    client: Any
    object_id: int
    placement_id: int
    lease_id: int
    name: str
    role: str
    shape: tuple[int, ...]
    dtype: torch.dtype
    requested_bytes: int
    actual_bytes: int
    path: str
    offset_bytes: int = 0
    visible_bytes: int | None = None
    alignment_bytes: int = 4096
    io_mode: str = "buffered"
    owner: bool = True
    released: bool = False

    @property
    def placement(self) -> str:
        return "ssd"

    @property
    def nbytes(self) -> int:
        if self.visible_bytes is not None:
            return int(self.visible_bytes)
        return int(self.requested_bytes) - int(self.offset_bytes)

    def release(self, *, release_object: bool | None = None) -> None:
        if self.released:
            return
        self.client._release_ssd_object(self, release_object=release_object)
        self.released = True


SsdBuffer = SsdObject
