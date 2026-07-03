from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AclEventHandle:
    event_id: int
    raw_handle: int


@dataclass(frozen=True)
class AclStreamHandle:
    stream_id: int


@dataclass(frozen=True)
class TransferCompletionEventHandle:
    event_id: int
    raw_handle: int
    transfer_event_id: int
    export_kind: str
    source_completion_kind: str
