from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DataObject:
    object_id: int
    namespace: str
    name: str
    role: str
    size_bytes: int
    consistency: str
    state: str

    @classmethod
    def from_response(cls, resp: dict[str, str]) -> "DataObject":
        return cls(
            object_id=int(resp["object_id"]),
            namespace=resp.get("namespace", ""),
            name=resp.get("name", ""),
            role=resp.get("role", "user"),
            size_bytes=int(resp.get("size_bytes", resp.get("requested_bytes", "0"))),
            consistency=resp.get("consistency", "single_writer"),
            state=resp.get("state", ""),
        )


@dataclass(frozen=True)
class DataPlacement:
    placement_id: int
    object_id: int
    medium: str
    target: str
    domain: str
    address_kind: str
    offset_bytes: int
    nbytes: int
    state: str

    @classmethod
    def from_response(cls, resp: dict[str, str]) -> "DataPlacement":
        return cls(
            placement_id=int(resp["placement_id"]),
            object_id=int(resp["object_id"]),
            medium=resp.get("medium", resp.get("placement", "")),
            target=resp.get("target", ""),
            domain=resp.get("domain", ""),
            address_kind=resp.get("address_kind", ""),
            offset_bytes=int(resp.get("offset_bytes", resp.get("allowed_offset_bytes", "0"))),
            nbytes=int(resp.get("nbytes", resp.get("requested_bytes", "0"))),
            state=resp.get("state", ""),
        )


@dataclass(frozen=True)
class DataHandle:
    handle_id: int
    object_id: int
    placement_id: int
    lease_id: int
    access_mode: str
    address_domain: str
    runtime_descriptor: str = ""


@dataclass(frozen=True)
class TransferCost:
    effort: float
    estimated_latency_us: float
    estimated_bandwidth_gib_s: float
    setup_cost_us: float
    hop_count: int
    fallback_used: bool
    fallback_reason: str
    explanation: str

    @classmethod
    def from_response(cls, resp: dict[str, str]) -> "TransferCost":
        return cls(
            effort=float(resp.get("effort", "0")),
            estimated_latency_us=float(resp.get("estimated_latency_us", "0")),
            estimated_bandwidth_gib_s=float(resp.get("estimated_bandwidth_gib_s", "0")),
            setup_cost_us=float(resp.get("setup_cost_us", "0")),
            hop_count=int(resp.get("hop_count", "0")),
            fallback_used=_truthy(resp.get("fallback_used", "0")),
            fallback_reason=resp.get("fallback_reason", ""),
            explanation=resp.get("explanation", ""),
        )


@dataclass(frozen=True)
class TransferRequest:
    src_object_id: int
    dst_object_id: int
    nbytes: int
    operation: str = "copy"
    wait_policy: str = "return_immediately"
    mode: str = "auto"
    request_id: int = 0
    src_offset_bytes: int = 0
    dst_offset_bytes: int = 0

    def as_kv(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "src_object_id": self.src_object_id,
            "dst_object_id": self.dst_object_id,
            "operation": self.operation,
            "wait_policy": self.wait_policy,
            "mode": self.mode,
            "src_offset_bytes": self.src_offset_bytes,
            "dst_offset_bytes": self.dst_offset_bytes,
            "nbytes": self.nbytes,
        }


@dataclass(frozen=True)
class TransferPlan:
    plan_id: int
    request_id: int
    src_object_id: int
    src_placement_id: int
    dst_object_id: int
    dst_placement_id: int
    operation: str
    path: str
    engine: str
    completion_kind: str
    wait_policy: str
    src_offset_bytes: int
    dst_offset_bytes: int
    nbytes: int
    cost: TransferCost

    @classmethod
    def from_response(cls, resp: dict[str, str]) -> "TransferPlan":
        return cls(
            plan_id=int(resp["plan_id"]),
            request_id=int(resp.get("request_id", "0")),
            src_object_id=int(resp["src_object_id"]),
            src_placement_id=int(resp["src_placement_id"]),
            dst_object_id=int(resp["dst_object_id"]),
            dst_placement_id=int(resp["dst_placement_id"]),
            operation=resp.get("operation", "copy"),
            path=resp.get("path", ""),
            engine=resp.get("engine", ""),
            completion_kind=resp.get("completion_kind", ""),
            wait_policy=resp.get("wait_policy", "return_immediately"),
            src_offset_bytes=int(resp.get("src_offset_bytes", "0")),
            dst_offset_bytes=int(resp.get("dst_offset_bytes", "0")),
            nbytes=int(resp.get("nbytes", "0")),
            cost=TransferCost.from_response(resp),
        )


@dataclass
class TransferEvent:
    event_id: int
    plan_id: int
    status: str
    completion_kind: str
    bytes_done: int
    submitted_at_ns: int
    started_at_ns: int
    completed_at_ns: int
    actual_latency_us: float
    actual_bandwidth_gib_s: float
    actual_engine: str
    actual_path: str
    fallback_used: bool
    fallback_reason: str
    error_code: str
    error_message: str
    channel_direction: str = ""
    channel_lane_id: int = 0
    channel_device_id: int = 0
    channel_chunk_bytes: int = 0
    channel_chunk_count: int = 0
    channel_pinned_footprint_bytes: int = 0
    channel_lane_wait_us: float = 0.0
    channel_chunks_transferred: int = 0
    channel_cpu_copy_us: float = 0.0
    channel_acl_copy_us: float = 0.0
    channel_acl_submit_us: float = 0.0
    channel_acl_wait_us: float = 0.0
    channel_wall_us: float = 0.0
    channel_queue_wait_us: float = 0.0
    channel_worker_execute_us: float = 0.0
    channel_overlap_ratio: float = 0.0
    channel_stream_create_count: int = 0
    channel_event_reuse_count: int = 0
    channel_event_record_count: int = 0
    channel_event_wait_count: int = 0
    channel_pipeline_overlap: bool = False
    ssd_io_submit_us: float = 0.0
    ssd_io_wait_us: float = 0.0
    ssd_io_bytes: int = 0
    ssd_io_bandwidth_gib_s: float = 0.0
    ssd_read_bytes: int = 0
    ssd_write_bytes: int = 0
    relay_stage_count: int = 0
    relay_ddr_hbm_us: float = 0.0
    relay_total_us: float = 0.0
    direct_candidate: str = ""
    direct_kind: str = ""
    direct_setup_us: float = 0.0
    direct_register_us: float = 0.0
    direct_fadvise_us: float = 0.0
    direct_readahead_us: float = 0.0
    direct_madvise_hugepage_us: float = 0.0
    direct_madvise_willneed_us: float = 0.0
    direct_madvise_populate_us: float = 0.0
    direct_pretouch_us: float = 0.0
    direct_mlock_us: float = 0.0
    direct_acl_us: float = 0.0
    direct_total_us: float = 0.0

    @classmethod
    def from_response(cls, resp: dict[str, str]) -> "TransferEvent":
        return cls(
            event_id=int(resp["event_id"]),
            plan_id=int(resp["plan_id"]),
            status=resp.get("event_status", resp.get("status", "")),
            completion_kind=resp.get("completion_kind", ""),
            bytes_done=int(resp.get("bytes_done", "0")),
            submitted_at_ns=int(resp.get("submitted_at_ns", "0")),
            started_at_ns=int(resp.get("started_at_ns", "0")),
            completed_at_ns=int(resp.get("completed_at_ns", "0")),
            actual_latency_us=float(resp.get("actual_latency_us", "0")),
            actual_bandwidth_gib_s=float(resp.get("actual_bandwidth_gib_s", "0")),
            actual_engine=resp.get("actual_engine", ""),
            actual_path=resp.get("actual_path", ""),
            fallback_used=_truthy(resp.get("fallback_used", "0")),
            fallback_reason=resp.get("fallback_reason", ""),
            error_code=resp.get("error_code", ""),
            error_message=resp.get("error_message", ""),
            channel_direction=resp.get("channel_direction", ""),
            channel_lane_id=int(resp.get("channel_lane_id", "0")),
            channel_device_id=int(resp.get("channel_device_id", "0")),
            channel_chunk_bytes=int(resp.get("channel_chunk_bytes", "0")),
            channel_chunk_count=int(resp.get("channel_chunk_count", "0")),
            channel_pinned_footprint_bytes=int(resp.get("channel_pinned_footprint_bytes", "0")),
            channel_lane_wait_us=float(resp.get("channel_lane_wait_us", "0")),
            channel_chunks_transferred=int(resp.get("channel_chunks_transferred", "0")),
            channel_cpu_copy_us=float(resp.get("channel_cpu_copy_us", "0")),
            channel_acl_copy_us=float(resp.get("channel_acl_copy_us", "0")),
            channel_acl_submit_us=float(resp.get("channel_acl_submit_us", "0")),
            channel_acl_wait_us=float(resp.get("channel_acl_wait_us", "0")),
            channel_wall_us=float(resp.get("channel_wall_us", "0")),
            channel_queue_wait_us=float(resp.get("channel_queue_wait_us", "0")),
            channel_worker_execute_us=float(resp.get("channel_worker_execute_us", "0")),
            channel_overlap_ratio=float(resp.get("channel_overlap_ratio", "0")),
            channel_stream_create_count=int(resp.get("channel_stream_create_count", "0")),
            channel_event_reuse_count=int(resp.get("channel_event_reuse_count", "0")),
            channel_event_record_count=int(resp.get("channel_event_record_count", "0")),
            channel_event_wait_count=int(resp.get("channel_event_wait_count", "0")),
            channel_pipeline_overlap=_truthy(resp.get("channel_pipeline_overlap", "0")),
            ssd_io_submit_us=float(resp.get("ssd_io_submit_us", "0")),
            ssd_io_wait_us=float(resp.get("ssd_io_wait_us", "0")),
            ssd_io_bytes=int(resp.get("ssd_io_bytes", "0")),
            ssd_io_bandwidth_gib_s=float(resp.get("ssd_io_bandwidth_gib_s", "0")),
            ssd_read_bytes=int(resp.get("ssd_read_bytes", "0")),
            ssd_write_bytes=int(resp.get("ssd_write_bytes", "0")),
            relay_stage_count=int(resp.get("relay_stage_count", "0")),
            relay_ddr_hbm_us=float(resp.get("relay_ddr_hbm_us", "0")),
            relay_total_us=float(resp.get("relay_total_us", "0")),
            direct_candidate=resp.get("direct_candidate", ""),
            direct_kind=resp.get("direct_kind", ""),
            direct_setup_us=float(resp.get("direct_setup_us", "0")),
            direct_register_us=float(resp.get("direct_register_us", "0")),
            direct_fadvise_us=float(resp.get("direct_fadvise_us", "0")),
            direct_readahead_us=float(resp.get("direct_readahead_us", "0")),
            direct_madvise_hugepage_us=float(resp.get("direct_madvise_hugepage_us", "0")),
            direct_madvise_willneed_us=float(resp.get("direct_madvise_willneed_us", "0")),
            direct_madvise_populate_us=float(resp.get("direct_madvise_populate_us", "0")),
            direct_pretouch_us=float(resp.get("direct_pretouch_us", "0")),
            direct_mlock_us=float(resp.get("direct_mlock_us", "0")),
            direct_acl_us=float(resp.get("direct_acl_us", "0")),
            direct_total_us=float(resp.get("direct_total_us", "0")),
        )


def _truthy(value: str) -> bool:
    return value in {"1", "true", "True", "yes", "YES", "on"}
