use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

pub const PROVIDER_API_MAJOR: u16 = 1;
pub const PROVIDER_API_MINOR: u16 = 0;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionMode {
    Physical,
    Simulated,
}

impl ExecutionMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Physical => "physical",
            Self::Simulated => "simulated",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderKind {
    Local,
    Remote,
    Simulator,
    Mock,
}

impl ProviderKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Remote => "remote",
            Self::Simulator => "simulator",
            Self::Mock => "mock",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fidelity {
    Physical,
    Functional,
    Semantic,
}

impl Fidelity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Physical => "physical",
            Self::Functional => "functional",
            Self::Semantic => "semantic",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Addressability {
    LocallyAddressable,
    ProviderOnly,
    RemoteMapping,
    BlockDescriptor,
}

impl Addressability {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LocallyAddressable => "locally_addressable",
            Self::ProviderOnly => "provider_only",
            Self::RemoteMapping => "remote_mapping",
            Self::BlockDescriptor => "block_descriptor",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccessKind {
    Local,
    RemoteMap,
}

impl AccessKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::RemoteMap => "remote_map",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportKind {
    Local,
    UbShmem,
    ObmmGsva,
    File,
}

impl TransportKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::UbShmem => "ub_shmem",
            Self::ObmmGsva => "obmm_gsva",
            Self::File => "file",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderHealth {
    Ready,
    Degraded,
    NotReady,
    Unavailable,
}

impl ProviderHealth {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Degraded => "degraded",
            Self::NotReady => "not_ready",
            Self::Unavailable => "unavailable",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderIdentity {
    pub provider_api_major: u16,
    pub provider_api_minor: u16,
    pub provider_id: String,
    pub provider_kind: ProviderKind,
    pub adapter_revision: String,
    pub external_platform_revision: String,
    pub external_protocol_revision: String,
    pub execution_realm_id: String,
    pub node_id: String,
    pub session_id: String,
    pub boot_id: String,
    pub session_generation: u64,
    pub fidelity: Fidelity,
    pub limitations: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderCapabilities {
    pub supported_operations: BTreeSet<ProviderOperationKind>,
    pub max_transfer_bytes: u64,
    pub alignment_bytes: u64,
    pub queue_depth: u32,
    pub addressability: Addressability,
    pub limitations: Vec<String>,
}

impl ProviderCapabilities {
    pub fn supports(&self, operation: ProviderOperationKind) -> bool {
        self.supported_operations.contains(&operation)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ProviderOperationKind {
    Allocate,
    Release,
    Publish,
    Import,
    Revoke,
    Checksum,
    Copy,
    Read,
    Write,
}

impl ProviderOperationKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allocate => "allocate",
            Self::Release => "release",
            Self::Publish => "publish",
            Self::Import => "import",
            Self::Revoke => "revoke",
            Self::Checksum => "checksum",
            Self::Copy => "copy",
            Self::Read => "read",
            Self::Write => "write",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderResourceRef {
    pub provider_resource_id: String,
    pub resource_generation: u64,
    pub size_bytes: u64,
    pub allowed_offset_bytes: u64,
    pub allowed_bytes: u64,
    pub addressability: Addressability,
    pub opaque_token: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderDescriptor {
    pub schema_major: u16,
    pub schema_minor: u16,
    pub provider_id: String,
    pub provider_session_id: String,
    pub resource_generation: u64,
    pub allowed_offset_bytes: u64,
    pub allowed_bytes: u64,
    pub access_mode: String,
    pub opaque_bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderAccessRef {
    pub accessor_provider_id: String,
    pub accessor_provider_session_id: String,
    pub accessor_resource_id: String,
    pub access_generation: u64,
    pub source_resource_generation: u64,
    pub allowed_offset_bytes: u64,
    pub allowed_bytes: u64,
    pub access_mode: String,
    pub addressability: Addressability,
    pub opaque_token: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProviderOperation {
    Allocate {
        size_bytes: u64,
        alignment_bytes: u64,
        medium: String,
        node_id: String,
    },
    Release {
        resource: ProviderResourceRef,
    },
    InitializeDeterministic {
        resource: ProviderResourceRef,
        offset_bytes: u64,
        nbytes: u64,
        seed_id: u64,
    },
    Publish {
        resource: ProviderResourceRef,
        offset_bytes: u64,
        nbytes: u64,
        access_mode: String,
    },
    Import {
        descriptor: ProviderDescriptor,
        accessor_node_id: String,
        offset_bytes: u64,
        nbytes: u64,
        access_mode: String,
    },
    Revoke {
        access: ProviderAccessRef,
    },
    ChecksumResource {
        resource: ProviderResourceRef,
        offset_bytes: u64,
        nbytes: u64,
    },
    ChecksumAccess {
        access: ProviderAccessRef,
        offset_bytes: u64,
        nbytes: u64,
    },
    Copy {
        source: ProviderResourceRef,
        destination: ProviderResourceRef,
        source_offset_bytes: u64,
        destination_offset_bytes: u64,
        nbytes: u64,
    },
}

impl ProviderOperation {
    pub fn kind(&self) -> ProviderOperationKind {
        match self {
            Self::Allocate { .. } => ProviderOperationKind::Allocate,
            Self::Release { .. } => ProviderOperationKind::Release,
            Self::InitializeDeterministic { .. } => ProviderOperationKind::Write,
            Self::Publish { .. } => ProviderOperationKind::Publish,
            Self::Import { .. } => ProviderOperationKind::Import,
            Self::Revoke { .. } => ProviderOperationKind::Revoke,
            Self::ChecksumResource { .. } | Self::ChecksumAccess { .. } => {
                ProviderOperationKind::Checksum
            }
            Self::Copy { .. } => ProviderOperationKind::Copy,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderCompletionState {
    Submitted,
    Running,
    Complete,
    Failed,
    Canceled,
}

impl ProviderCompletionState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Complete | Self::Failed | Self::Canceled)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Submitted => "submitted",
            Self::Running => "running",
            Self::Complete => "complete",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProviderCompletionResult {
    Unit,
    Resource(ProviderResourceRef),
    Descriptor(ProviderDescriptor),
    Access(ProviderAccessRef),
    Checksum { algorithm: String, value: u64 },
    Bytes { completed_bytes: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderSubmission {
    pub provider_completion_id: String,
    pub initial_state: ProviderCompletionState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderCompletionSnapshot {
    pub provider_completion_id: String,
    pub state: ProviderCompletionState,
    pub result: Option<ProviderCompletionResult>,
    pub error: Option<ProviderError>,
}

impl ProviderCompletionSnapshot {
    pub fn validate(&self) -> Result<(), ProviderError> {
        if !self.state.is_terminal() && (self.result.is_some() || self.error.is_some()) {
            return Err(ProviderError::new(
                ProviderErrorCode::ProviderProtocolError,
                "non-terminal completion carries a terminal result",
            ));
        }
        match self.state {
            ProviderCompletionState::Complete if self.result.is_none() || self.error.is_some() => {
                Err(ProviderError::new(
                    ProviderErrorCode::ProviderProtocolError,
                    "complete completion must carry exactly one result",
                ))
            }
            ProviderCompletionState::Failed if self.error.is_none() || self.result.is_some() => {
                Err(ProviderError::new(
                    ProviderErrorCode::ProviderProtocolError,
                    "failed completion must carry exactly one error",
                ))
            }
            ProviderCompletionState::Canceled if self.result.is_some() => Err(ProviderError::new(
                ProviderErrorCode::ProviderProtocolError,
                "canceled completion cannot carry a result",
            )),
            _ => Ok(()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderErrorCode {
    ProviderUnavailable,
    ProviderNotReady,
    ProviderVersionMismatch,
    ProviderOperationUnsupported,
    ProviderSessionRestarted,
    ProviderProtocolError,
    ProviderResourceNotFound,
    ProviderResourceStale,
    HandleNotLocallyAddressable,
    DescriptorMalformed,
    AccessDenied,
    RangeOutOfBounds,
    ChecksumMismatch,
    QueueFull,
    Timeout,
    CancelUnsupported,
    CrossRealmTransferUnsupported,
    InternalProviderAdapterError,
}

impl ProviderErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProviderUnavailable => "ProviderUnavailable",
            Self::ProviderNotReady => "ProviderNotReady",
            Self::ProviderVersionMismatch => "ProviderVersionMismatch",
            Self::ProviderOperationUnsupported => "ProviderOperationUnsupported",
            Self::ProviderSessionRestarted => "ProviderSessionRestarted",
            Self::ProviderProtocolError => "ProviderProtocolError",
            Self::ProviderResourceNotFound => "ProviderResourceNotFound",
            Self::ProviderResourceStale => "ProviderResourceStale",
            Self::HandleNotLocallyAddressable => "HandleNotLocallyAddressable",
            Self::DescriptorMalformed => "DescriptorMalformed",
            Self::AccessDenied => "AccessDenied",
            Self::RangeOutOfBounds => "RangeOutOfBounds",
            Self::ChecksumMismatch => "ChecksumMismatch",
            Self::QueueFull => "QueueFull",
            Self::Timeout => "Timeout",
            Self::CancelUnsupported => "CancelUnsupported",
            Self::CrossRealmTransferUnsupported => "CrossRealmTransferUnsupported",
            Self::InternalProviderAdapterError => "InternalProviderAdapterError",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderError {
    pub code: ProviderErrorCode,
    pub detail: String,
    pub external_code: Option<i64>,
}

impl ProviderError {
    pub fn new(code: ProviderErrorCode, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
            external_code: None,
        }
    }
}

impl fmt::Display for ProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code.as_str(), self.detail)
    }
}

impl std::error::Error for ProviderError {}

pub trait DataProvider: Send + Sync {
    fn identity(&self) -> ProviderIdentity;
    fn capabilities(&self) -> ProviderCapabilities;
    fn health(&self) -> ProviderHealth;
    fn submit_operation(
        &self,
        operation: ProviderOperation,
    ) -> Result<ProviderSubmission, ProviderError>;
    fn poll_completion(
        &self,
        provider_completion_id: &str,
    ) -> Result<ProviderCompletionSnapshot, ProviderError>;
    fn wait_completion(
        &self,
        provider_completion_id: &str,
        timeout_ms: u64,
    ) -> Result<ProviderCompletionSnapshot, ProviderError>;
    fn cancel_completion(&self, provider_completion_id: &str) -> Result<(), ProviderError>;
    fn read_counters(&self) -> BTreeMap<String, u64>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_result_is_only_valid_on_complete() {
        let invalid = ProviderCompletionSnapshot {
            provider_completion_id: "c1".to_string(),
            state: ProviderCompletionState::Running,
            result: Some(ProviderCompletionResult::Unit),
            error: None,
        };
        assert_eq!(
            invalid.validate().unwrap_err().code,
            ProviderErrorCode::ProviderProtocolError
        );
    }

    #[test]
    fn operation_kind_is_typed() {
        let operation = ProviderOperation::Allocate {
            size_bytes: 4096,
            alignment_bytes: 4096,
            medium: "ddr".to_string(),
            node_id: "nodeA".to_string(),
        };
        assert_eq!(operation.kind(), ProviderOperationKind::Allocate);
    }
}
