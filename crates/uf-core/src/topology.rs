use crate::provider::{Addressability, Fidelity, ProviderOperationKind};
use std::collections::BTreeSet;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopologyNode {
    pub node_id: String,
    pub domain: String,
    pub state: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopologyDevice {
    pub device_id: String,
    pub node_id: String,
    pub medium: String,
    pub state: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopologyLink {
    pub link_id: String,
    pub source_node_id: String,
    pub destination_node_id: String,
    pub kind: String,
    pub state: String,
    pub bandwidth_bytes_per_second: u64,
    pub latency_ns: u64,
    pub hop_count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopologySnapshot {
    pub snapshot_id: String,
    pub generation: u64,
    pub observed_at_ns: u128,
    pub execution_realm_id: String,
    pub immutable_for_run: bool,
    pub nodes: Vec<TopologyNode>,
    pub devices: Vec<TopologyDevice>,
    pub links: Vec<TopologyLink>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderCapabilityContribution {
    pub provider_id: String,
    pub node_id: String,
    pub supported_operations: BTreeSet<ProviderOperationKind>,
    pub addressability: Addressability,
    pub fidelity: Fidelity,
    pub limitations: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapabilitySnapshot {
    pub snapshot_id: String,
    pub generation: u64,
    pub observed_at_ns: u128,
    pub execution_realm_id: String,
    pub immutable_for_run: bool,
    pub contributions: Vec<ProviderCapabilityContribution>,
}

impl TopologySnapshot {
    pub fn validate_static_phasec02(&self) -> Result<(), String> {
        if self.generation != 1 || !self.immutable_for_run {
            return Err("PhaseC-02 topology must be immutable generation=1".to_string());
        }
        if self.snapshot_id.is_empty() || self.execution_realm_id.is_empty() {
            return Err("snapshot_id and execution_realm_id are required".to_string());
        }
        Ok(())
    }
}

impl CapabilitySnapshot {
    pub fn validate_static_phasec02(&self) -> Result<(), String> {
        if self.generation != 1 || !self.immutable_for_run {
            return Err("PhaseC-02 capability snapshot must be immutable generation=1".to_string());
        }
        if self.snapshot_id.is_empty() || self.execution_realm_id.is_empty() {
            return Err("snapshot_id and execution_realm_id are required".to_string());
        }
        Ok(())
    }
}
