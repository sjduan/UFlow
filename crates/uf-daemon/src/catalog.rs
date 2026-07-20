use crate::acl_backend::HbmAllocation;

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};

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

#[derive(Clone, Debug)]
pub struct TransferPlanRecord {
    pub plan_id: u64,
    pub request_id: u64,
    pub client_id: u64,
    pub src_object_id: u64,
    pub src_placement_id: u64,
    pub dst_object_id: u64,
    pub dst_placement_id: u64,
    pub operation: String,
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

#[derive(Clone, Debug)]
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
    pub leases: HashMap<u64, Lease>,
    pub transfer_plans: HashMap<u64, TransferPlanRecord>,
    pub transfer_events: HashMap<u64, TransferEventRecord>,
    pub ssd_read_bytes: u64,
    pub ssd_write_bytes: u64,
    pub ssd_read_ops: u64,
    pub ssd_write_ops: u64,
    pub next_block_id: u64,
    pub next_client_id: u64,
    pub next_object_id: u64,
    pub next_placement_id: u64,
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
        Self {
            device,
            hbm: HbmRuntimeStatus::default(),
            shutdown_requested: false,
            blocks,
            clients: HashMap::new(),
            objects: HashMap::new(),
            leases: HashMap::new(),
            transfer_plans: HashMap::new(),
            transfer_events: HashMap::new(),
            ssd_read_bytes: 0,
            ssd_write_bytes: 0,
            ssd_read_ops: 0,
            ssd_write_ops: 0,
            next_block_id,
            next_client_id: 1,
            next_object_id: 100,
            next_placement_id: 10_000,
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
