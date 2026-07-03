from __future__ import annotations

import ctypes
import mmap
from dataclasses import dataclass
from typing import Any

import torch

from .acl import _UfAclMappedMemory


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
