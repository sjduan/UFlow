#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Medium {
    Hbm,
    Ddr,
    Ssd,
    UbShmem,
    Dfs,
}

impl Medium {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Hbm => "hbm",
            Self::Ddr => "ddr",
            Self::Ssd => "ssd",
            Self::UbShmem => "ub_shmem",
            Self::Dfs => "dfs",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AddressKind {
    DevicePtr,
    MmapPath,
    HostPtr,
    ShareableHandle,
    FilePathOffset,
    Virtual,
    UbAddress,
    BlockDescriptor,
}

impl AddressKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::DevicePtr => "device_ptr",
            Self::MmapPath => "mmap_path",
            Self::HostPtr => "host_ptr",
            Self::ShareableHandle => "shareable_handle",
            Self::FilePathOffset => "file_path_offset",
            Self::Virtual => "virtual",
            Self::UbAddress => "ub_address",
            Self::BlockDescriptor => "block_descriptor",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompletionKind {
    Immediate,
    DaemonEvent,
    AclEvent,
    ThreadEvent,
    UbCompletion,
    BlockCompletion,
}

impl CompletionKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Immediate => "immediate",
            Self::DaemonEvent => "daemon_event",
            Self::AclEvent => "acl_event",
            Self::ThreadEvent => "thread_event",
            Self::UbCompletion => "ub_completion",
            Self::BlockCompletion => "block_completion",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WaitPolicy {
    ReturnImmediately,
    WaitComplete,
}

impl WaitPolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ReturnImmediately => "return_immediately",
            Self::WaitComplete => "wait_complete",
        }
    }
}

#[derive(Clone, Debug)]
pub struct DataObject {
    pub object_id: u64,
    pub namespace: String,
    pub name: String,
    pub role: String,
    pub size_bytes: u64,
    pub consistency: String,
    pub content_version: u64,
    pub owner: String,
    pub shape: String,
    pub dtype: String,
    pub state: String,
}

#[derive(Clone, Debug)]
pub struct DataPlacement {
    pub placement_id: u64,
    pub object_id: u64,
    pub medium: String,
    pub node_id: String,
    pub device_id: Option<i32>,
    pub domain: String,
    pub address_kind: String,
    pub provider_id: String,
    pub execution_mode: ExecutionMode,
    pub execution_realm_id: String,
    pub requested_fidelity: Fidelity,
    pub actual_fidelity: Fidelity,
    pub addressability: Addressability,
    pub topology_generation: u64,
    pub capability_generation: u64,
    pub offset_bytes: u64,
    pub nbytes: u64,
    pub state: String,
}

#[derive(Clone, Debug)]
pub struct ResourceBinding {
    pub binding_id: u64,
    pub placement_id: u64,
    pub provider_id: String,
    pub provider_session_id: String,
    pub provider_resource_id: String,
    pub binding_generation: u64,
    pub opaque_descriptor: Vec<u8>,
    pub allowed_offset_bytes: u64,
    pub allowed_bytes: u64,
    pub limitations: Vec<String>,
    pub state: String,
}

#[derive(Clone, Debug)]
pub struct AccessBinding {
    pub access_binding_id: u64,
    pub placement_id: u64,
    pub source_binding_id: u64,
    pub accessor_node_id: String,
    pub accessor_device_id: Option<String>,
    pub accessor_provider_id: String,
    pub accessor_provider_session_id: String,
    pub accessor_resource_id: String,
    pub access_binding_generation: u64,
    pub source_binding_generation: u64,
    pub access_kind: AccessKind,
    pub transport_kind: TransportKind,
    pub addressability: Addressability,
    pub access_mode: String,
    pub allowed_offset_bytes: u64,
    pub allowed_bytes: u64,
    pub topology_generation: u64,
    pub capability_generation: u64,
    pub opaque_token: Vec<u8>,
    pub state: String,
}

pub type DataObjectRecord = DataObject;
pub type PlacementRecord = DataPlacement;
pub type ResourceBindingRecord = ResourceBinding;
pub type AccessBindingRecord = AccessBinding;

#[derive(Clone, Debug)]
pub struct DataHandle {
    pub handle_id: u64,
    pub object_id: u64,
    pub placement_id: u64,
    pub binding_id: u64,
    pub access_binding_id: Option<u64>,
    pub lease_id: u64,
    pub object_version: u64,
    pub binding_generation: u64,
    pub access_binding_generation: Option<u64>,
    pub access_mode: String,
    pub address_domain: String,
    pub execution_mode: ExecutionMode,
    pub execution_realm_id: String,
    pub provider_id: String,
    pub addressability: Addressability,
    pub access_kind: AccessKind,
    pub transport_kind: TransportKind,
    pub host_dereference: bool,
    pub runtime_descriptor: String,
}

#[derive(Clone, Debug)]
pub struct PlanStep {
    pub step_id: u64,
    pub dependencies: Vec<u64>,
    pub provider_id: String,
    pub operation: String,
    pub src_placement_id: Option<u64>,
    pub dst_placement_id: Option<u64>,
    pub access_binding_id: Option<u64>,
    pub offset_bytes: u64,
    pub nbytes: u64,
    pub expected_completion_kind: String,
    pub fallback_policy: String,
    pub provider_completion_id: Option<String>,
    pub completion_state: ProviderCompletionState,
}

#[derive(Clone, Debug)]
pub struct TransferRequest {
    pub request_id: u64,
    pub src_placement_id: u64,
    pub dst_placement_id: u64,
    pub operation: String,
    pub offset_bytes: u64,
    pub nbytes: u64,
    pub wait_policy: String,
}

#[derive(Clone, Debug)]
pub struct TransferCost {
    pub effort: f64,
    pub estimated_latency_us: f64,
    pub estimated_bandwidth_gib_s: f64,
    pub setup_cost_us: f64,
    pub hop_count: u32,
    pub contention_score: f64,
    pub reliability_score: f64,
    pub explanation: String,
}

#[derive(Clone, Debug)]
pub struct TransferPlan {
    pub plan_id: u64,
    pub request_id: u64,
    pub src_placement_id: u64,
    pub dst_placement_id: u64,
    pub path: String,
    pub engine: String,
    pub completion_kind: String,
    pub topology_generation: u64,
    pub capability_generation: u64,
    pub execution_realm_id: String,
    pub steps: Vec<PlanStep>,
    pub cost: TransferCost,
    pub fallback_used: bool,
    pub fallback_reason: String,
}

#[derive(Clone, Debug)]
pub struct TransferEvent {
    pub event_id: u64,
    pub plan_id: u64,
    pub parent_event_id: Option<u64>,
    pub attempt_id: u64,
    pub step_id: Option<u64>,
    pub provider_id: String,
    pub provider_session_id: String,
    pub status: String,
    pub completion_kind: String,
    pub submitted_at_ns: u128,
    pub started_at_ns: u128,
    pub completed_at_ns: u128,
    pub bytes_done: u64,
    pub checksum_verdict: String,
    pub planned_path: String,
    pub actual_path: String,
    pub topology_generation: u64,
    pub capability_generation: u64,
    pub execution_mode: ExecutionMode,
    pub fidelity: Fidelity,
    pub error_code: String,
    pub error_message: String,
}
use crate::provider::{
    AccessKind, Addressability, ExecutionMode, Fidelity, ProviderCompletionState, TransportKind,
};
