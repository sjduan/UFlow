use crate::acl_backend::HbmAllocation;
use crate::providers::{
    LegacyLocalProvider, MockBridgeUbSimProvider, MockProvider, ProviderRegistry, UbSimProvider,
};

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use uf_core::idl::{
    AccessBindingRecord, DataObjectRecord, PlacementRecord, ResourceBindingRecord, TransferEvent,
    TransferPlan,
};
use uf_core::provider::{Addressability, ExecutionMode, Fidelity};

#[derive(Clone, Debug)]
pub struct Block {
    pub block_id: u64,
    pub allocation: HbmAllocation,
    pub state: String,
    pub object_id: u64,
    pub dynamic: bool,
}

impl Block {
    pub fn new(block_id: u64, allocation: HbmAllocation, dynamic: bool) -> Self {
        Self {
            block_id,
            allocation,
            state: "Ready".to_string(),
            object_id: 0,
            dynamic,
        }
    }

    pub fn can_hold(&self, requested_bytes: u64) -> bool {
        self.state == "Ready"
            && self.object_id == 0
            && self.allocation.actual_bytes >= requested_bytes
    }

    pub fn can_hold_on(&self, requested_bytes: u64, device_id: i32) -> bool {
        self.can_hold(requested_bytes) && self.allocation.device_id == device_id
    }
}

#[derive(Clone, Debug)]
pub struct Client {
    pub client_id: u64,
    pub role: String,
    pub os_pid: i64,
    pub bare_tgid: i64,
    pub device_id: i32,
}

#[derive(Clone, Debug)]
pub struct Object {
    pub object_id: u64,
    pub placement_id: u64,
    pub block_id: u64,
    pub placement: String,
    pub target: String,
    pub ddr_path: String,
    pub ddr_fd: i32,
    pub ddr_service_ptr: u64,
    pub ddr_service_len: u64,
    pub requested_bytes: u64,
    pub actual_bytes: u64,
    pub state: String,
    pub creator_client_id: u64,
    pub modified_offset_bytes: u64,
    pub modified_bytes: u64,
    pub model_id: String,
    pub name: String,
    pub role: String,
    pub shape: String,
    pub dtype: String,
    pub immutable: bool,
    pub ddr_fast_profile: String,
    pub ddr_madvise_hugepage: bool,
    pub ddr_pretouched: bool,
    pub ddr_prepare_us: f64,
    pub ddr_madvise_us: f64,
    pub ddr_pretouch_us: f64,
    pub ddr_fallback_reason: String,
    pub ssd_path: String,
    pub ssd_offset_bytes: u64,
    pub ssd_alignment_bytes: u64,
    pub ssd_io_mode: String,
    pub ssd_file_owned: bool,
    pub ssd_preallocated: bool,
}

#[derive(Clone, Debug)]
pub struct Lease {
    pub lease_id: u64,
    pub object_id: u64,
    pub block_id: u64,
    pub client_id: u64,
    pub state: String,
    pub allowed_offset_bytes: u64,
    pub allowed_bytes: u64,
}

#[derive(Clone, Debug, Default)]
pub struct TransferPlanRecord {
    pub plan_id: u64,
    pub request_id: u64,
    pub client_id: u64,
    pub src_object_id: u64,
    pub src_placement_id: u64,
    pub src_medium: String,
    pub dst_object_id: u64,
    pub dst_placement_id: u64,
    pub dst_medium: String,
    pub operation: String,
    pub requested_mode: String,
    pub path: String,
    pub engine: String,
    pub completion_kind: String,
    pub wait_policy: String,
    pub src_offset_bytes: u64,
    pub dst_offset_bytes: u64,
    pub nbytes: u64,
    pub effort: f64,
    pub estimated_latency_us: f64,
    pub estimated_bandwidth_gib_s: f64,
    pub setup_cost_us: f64,
    pub hop_count: u32,
    pub fallback_used: bool,
    pub fallback_reason: String,
    pub explanation: String,
}

#[derive(Clone, Debug, Default)]
pub struct TransferEventRecord {
    pub event_id: u64,
    pub plan_id: u64,
    pub client_id: u64,
    pub status: String,
    pub completion_kind: String,
    pub submitted_at_ns: u128,
    pub started_at_ns: u128,
    pub completed_at_ns: u128,
    pub bytes_done: u64,
    pub actual_latency_us: f64,
    pub actual_bandwidth_gib_s: f64,
    pub actual_engine: String,
    pub actual_path: String,
    pub fallback_used: bool,
    pub fallback_reason: String,
    pub error_code: String,
    pub error_message: String,
    pub channel_direction: String,
    pub channel_lane_id: u64,
    pub channel_device_id: i32,
    pub channel_chunk_bytes: u64,
    pub channel_chunk_count: u64,
    pub channel_pinned_footprint_bytes: u64,
    pub channel_lane_wait_us: f64,
    pub channel_chunks_transferred: u64,
    pub channel_cpu_copy_us: f64,
    pub channel_acl_copy_us: f64,
    pub channel_acl_submit_us: f64,
    pub channel_acl_wait_us: f64,
    pub channel_wall_us: f64,
    pub channel_queue_wait_us: f64,
    pub channel_worker_execute_us: f64,
    pub channel_overlap_ratio: f64,
    pub channel_stream_create_count: u64,
    pub channel_event_reuse_count: u64,
    pub channel_event_record_count: u64,
    pub channel_event_wait_count: u64,
    pub channel_pipeline_overlap: bool,
    pub ssd_io_submit_us: f64,
    pub ssd_io_wait_us: f64,
    pub ssd_io_bytes: u64,
    pub ssd_io_bandwidth_gib_s: f64,
    pub ssd_read_bytes: u64,
    pub ssd_write_bytes: u64,
    pub relay_stage_count: u64,
    pub relay_ddr_hbm_us: f64,
    pub relay_total_us: f64,
    pub direct_candidate: String,
    pub direct_kind: String,
    pub direct_setup_us: f64,
    pub direct_register_us: f64,
    pub direct_fadvise_us: f64,
    pub direct_readahead_us: f64,
    pub direct_madvise_hugepage_us: f64,
    pub direct_madvise_willneed_us: f64,
    pub direct_madvise_populate_us: f64,
    pub direct_pretouch_us: f64,
    pub direct_mlock_us: f64,
    pub direct_acl_us: f64,
    pub direct_total_us: f64,
}

pub struct Catalog {
    pub device: i32,
    pub hbm: HbmRuntimeStatus,
    pub shutdown_requested: bool,
    pub blocks: Vec<Block>,
    pub clients: HashMap<u64, Client>,
    pub objects: HashMap<u64, Object>,
    pub data_objects: HashMap<u64, DataObjectRecord>,
    pub placements: HashMap<u64, PlacementRecord>,
    pub resource_bindings: HashMap<u64, ResourceBindingRecord>,
    pub access_bindings: HashMap<u64, AccessBindingRecord>,
    pub providers: ProviderRegistry,
    pub leases: HashMap<u64, Lease>,
    pub transfer_plans: HashMap<u64, TransferPlanRecord>,
    pub transfer_events: HashMap<u64, TransferEventRecord>,
    pub typed_transfer_plans: HashMap<u64, TransferPlan>,
    pub typed_transfer_events: HashMap<u64, TransferEvent>,
    pub simulated_object_plan_ids: HashMap<u64, u64>,
    pub simulated_access_plan_ids: HashMap<u64, u64>,
    pub simulated_plan_parent_event_ids: HashMap<u64, u64>,
    pub ssd_read_bytes: u64,
    pub ssd_write_bytes: u64,
    pub ssd_read_ops: u64,
    pub ssd_write_ops: u64,
    pub next_block_id: u64,
    pub next_client_id: u64,
    pub next_object_id: u64,
    pub next_placement_id: u64,
    pub next_resource_binding_id: u64,
    pub next_access_binding_id: u64,
    pub next_lease_id: u64,
    pub next_transfer_plan_id: u64,
    pub next_transfer_event_id: u64,
}

pub type SharedCatalog = Arc<(Mutex<Catalog>, Condvar)>;

#[derive(Clone, Debug, Default)]
pub struct HbmRuntimeStatus {
    pub available: bool,
    pub probe_attempted: bool,
    pub probe_bytes: u64,
    pub probe_actual_bytes: u64,
    pub probe_error: String,
    pub last_error: String,
}

impl Catalog {
    pub fn new(device: i32, blocks: Vec<Block>) -> Self {
        let next_block_id = blocks.len() as u64 + 1;
        let mut providers = ProviderRegistry::new();
        providers
            .register(Arc::new(LegacyLocalProvider::new(
                "local_acl",
                "hbm",
                Addressability::LocallyAddressable,
            )))
            .expect("register local_acl provider");
        providers
            .register(Arc::new(LegacyLocalProvider::new(
                "local_ddr",
                "ddr",
                Addressability::LocallyAddressable,
            )))
            .expect("register local_ddr provider");
        providers
            .register(Arc::new(LegacyLocalProvider::new(
                "local_ssd",
                "ssd",
                Addressability::BlockDescriptor,
            )))
            .expect("register local_ssd provider");
        providers
            .register(Arc::new(MockProvider::new(
                "mock",
                "simulated:mock",
                "mock-node",
            )))
            .expect("register mock provider");
        providers
            .register(Arc::new(MockBridgeUbSimProvider::new()))
            .expect("register ub_sim_mock provider");
        match UbSimProvider::from_env() {
            Ok(Some(provider)) => providers
                .register(Arc::new(provider))
                .expect("register ub_sim provider"),
            Ok(None) => {}
            Err(error) => eprintln!(
                "{{\"event\":\"provider_unavailable\",\"provider_id\":\"ub_sim\",\"error_code\":\"{}\",\"detail\":\"{}\"}}",
                error.code.as_str(),
                error.detail.replace('"', "'")
            ),
        }
        Self {
            device,
            hbm: HbmRuntimeStatus::default(),
            shutdown_requested: false,
            blocks,
            clients: HashMap::new(),
            objects: HashMap::new(),
            data_objects: HashMap::new(),
            placements: HashMap::new(),
            resource_bindings: HashMap::new(),
            access_bindings: HashMap::new(),
            providers,
            leases: HashMap::new(),
            transfer_plans: HashMap::new(),
            transfer_events: HashMap::new(),
            typed_transfer_plans: HashMap::new(),
            typed_transfer_events: HashMap::new(),
            simulated_object_plan_ids: HashMap::new(),
            simulated_access_plan_ids: HashMap::new(),
            simulated_plan_parent_event_ids: HashMap::new(),
            ssd_read_bytes: 0,
            ssd_write_bytes: 0,
            ssd_read_ops: 0,
            ssd_write_ops: 0,
            next_block_id,
            next_client_id: 1,
            next_object_id: 100,
            next_placement_id: 10_000,
            next_resource_binding_id: 40_000,
            next_access_binding_id: 50_000,
            next_lease_id: 1000,
            next_transfer_plan_id: 20_000,
            next_transfer_event_id: 30_000,
        }
    }

    pub fn take_next_block_id(&mut self) -> u64 {
        let block_id = self.next_block_id;
        self.next_block_id += 1;
        block_id
    }

    pub fn take_next_client_id(&mut self) -> u64 {
        let client_id = self.next_client_id;
        self.next_client_id += 1;
        client_id
    }

    pub fn take_next_object_id(&mut self) -> u64 {
        let object_id = self.next_object_id;
        self.next_object_id += 1;
        object_id
    }

    pub fn take_next_placement_id(&mut self) -> u64 {
        let placement_id = self.next_placement_id;
        self.next_placement_id += 1;
        placement_id
    }

    pub fn take_next_lease_id(&mut self) -> u64 {
        let lease_id = self.next_lease_id;
        self.next_lease_id += 1;
        lease_id
    }

    pub fn take_next_resource_binding_id(&mut self) -> u64 {
        let binding_id = self.next_resource_binding_id;
        self.next_resource_binding_id += 1;
        binding_id
    }

    pub fn take_next_access_binding_id(&mut self) -> u64 {
        let access_binding_id = self.next_access_binding_id;
        self.next_access_binding_id += 1;
        access_binding_id
    }

    pub fn record_legacy_object(&mut self, object: &Object) {
        let (provider_id, addressability, address_kind, domain, node_id, device_id) =
            match object.placement.as_str() {
                "hbm" => (
                    "local_acl",
                    Addressability::LocallyAddressable,
                    "shareable_handle",
                    "local_node",
                    "local".to_string(),
                    object
                        .target
                        .strip_prefix("npu:")
                        .and_then(|value| value.parse::<i32>().ok()),
                ),
                "ddr" => (
                    "local_ddr",
                    Addressability::LocallyAddressable,
                    "mmap_path",
                    "numa_node",
                    object.target.clone(),
                    None,
                ),
                "ssd" => (
                    "local_ssd",
                    Addressability::BlockDescriptor,
                    "file_path_offset",
                    "local_ssd",
                    "local".to_string(),
                    None,
                ),
                _ => (
                    "legacy_unknown",
                    Addressability::ProviderOnly,
                    "virtual",
                    "local_node",
                    "local".to_string(),
                    None,
                ),
            };
        let consistency = if object.immutable {
            "immutable"
        } else {
            "single_writer"
        };
        self.data_objects.insert(
            object.object_id,
            DataObjectRecord {
                object_id: object.object_id,
                namespace: object.model_id.clone(),
                name: object.name.clone(),
                role: object.role.clone(),
                size_bytes: object.requested_bytes,
                consistency: consistency.to_string(),
                content_version: 1,
                owner: object.creator_client_id.to_string(),
                shape: object.shape.clone(),
                dtype: object.dtype.clone(),
                state: object.state.clone(),
            },
        );
        self.placements.insert(
            object.placement_id,
            PlacementRecord {
                placement_id: object.placement_id,
                object_id: object.object_id,
                medium: object.placement.clone(),
                node_id,
                device_id,
                domain: domain.to_string(),
                address_kind: address_kind.to_string(),
                provider_id: provider_id.to_string(),
                execution_mode: ExecutionMode::Physical,
                execution_realm_id: "physical:local".to_string(),
                requested_fidelity: Fidelity::Physical,
                actual_fidelity: Fidelity::Physical,
                addressability,
                topology_generation: 1,
                capability_generation: 1,
                offset_bytes: 0,
                nbytes: object.requested_bytes,
                state: object.state.clone(),
            },
        );
        let existing_binding_id = self
            .resource_bindings
            .values()
            .find(|binding| binding.placement_id == object.placement_id)
            .map(|binding| binding.binding_id);
        let binding_id =
            existing_binding_id.unwrap_or_else(|| self.take_next_resource_binding_id());
        let (provider_resource_id, descriptor) = match object.placement.as_str() {
            "hbm" => (format!("block:{}", object.block_id), Vec::new()),
            "ddr" => (
                format!("ddr:{}", object.object_id),
                object.ddr_path.as_bytes().to_vec(),
            ),
            "ssd" => (
                format!("ssd:{}:{}", object.object_id, object.ssd_offset_bytes),
                object.ssd_path.as_bytes().to_vec(),
            ),
            _ => (format!("legacy:{}", object.object_id), Vec::new()),
        };
        self.resource_bindings.insert(
            binding_id,
            ResourceBindingRecord {
                binding_id,
                placement_id: object.placement_id,
                provider_id: provider_id.to_string(),
                provider_session_id: format!("{}:process", provider_id),
                provider_resource_id,
                binding_generation: 1,
                opaque_descriptor: descriptor,
                allowed_offset_bytes: 0,
                allowed_bytes: object.requested_bytes,
                limitations: vec!["legacy-compatibility-adapter".to_string()],
                state: object.state.clone(),
            },
        );
    }

    pub fn mark_object_released(&mut self, object_id: u64) {
        if let Some(object) = self.data_objects.get_mut(&object_id) {
            object.state = "Released".to_string();
        }
        let placement_ids = self
            .placements
            .values_mut()
            .filter(|placement| placement.object_id == object_id)
            .map(|placement| {
                placement.state = "Released".to_string();
                placement.placement_id
            })
            .collect::<Vec<_>>();
        for binding in self.resource_bindings.values_mut().filter(|binding| {
            placement_ids
                .iter()
                .any(|placement_id| *placement_id == binding.placement_id)
        }) {
            binding.state = "Released".to_string();
        }
        for access in self.access_bindings.values_mut().filter(|access| {
            placement_ids
                .iter()
                .any(|placement_id| *placement_id == access.placement_id)
        }) {
            access.state = "Stale".to_string();
        }
    }

    pub fn take_next_transfer_plan_id(&mut self) -> u64 {
        let plan_id = self.next_transfer_plan_id;
        self.next_transfer_plan_id += 1;
        plan_id
    }

    pub fn take_next_transfer_event_id(&mut self) -> u64 {
        let event_id = self.next_transfer_event_id;
        self.next_transfer_event_id += 1;
        event_id
    }

    pub fn find_object_by_placement_id(&self, placement_id: u64) -> Option<Object> {
        self.objects
            .values()
            .find(|object| object.placement_id == placement_id)
            .cloned()
    }

    pub fn find_reusable_object(
        &self,
        model_id: &str,
        name: &str,
        role: &str,
        placement: &str,
        target: &str,
        requested_bytes: u64,
        immutable: bool,
    ) -> Option<Object> {
        if model_id.is_empty() || name.is_empty() || role.is_empty() || !immutable {
            return None;
        }
        self.objects
            .values()
            .find(|object| {
                object.immutable
                    && object.model_id == model_id
                    && object.name == name
                    && object.role == role
                    && object.placement == placement
                    && (target.is_empty() || object.target == target)
                    && object.requested_bytes == requested_bytes
                    && object.state != "Released"
            })
            .cloned()
    }

    pub fn make_lease(
        &mut self,
        client_id: u64,
        object: &Object,
        offset: u64,
        bytes: u64,
    ) -> Lease {
        let lease_id = self.take_next_lease_id();
        let lease = Lease {
            lease_id,
            object_id: object.object_id,
            block_id: object.block_id,
            client_id,
            state: "Active".to_string(),
            allowed_offset_bytes: offset,
            allowed_bytes: bytes,
        };
        self.leases.insert(lease_id, lease.clone());
        lease
    }
}
