use crate::acl_backend::{HbmBackend, HbmMemInfo, NpuHbmAclBackend};
use crate::catalog::SharedCatalog;
use crate::common::{sanitize_kv_value, DDR_PLACEMENT, HBM_PLACEMENT};
use crate::ddr_backend::{ddr_committed_bytes, ddr_info_with_committed};
use crate::direct_transfer::{direct_lane_manager, direct_transfer_executor};
use crate::transfer_channel::transfer_channel_manager;
use uf_core::{get, ok, Kv};

use std::env;

#[derive(Default)]
struct BucketStats {
    count: usize,
    requested: u64,
    actual: u64,
}

#[derive(Default)]
struct StatsSnapshot {
    device: i32,
    hbm_available_flag: bool,
    hbm_probe_attempted: bool,
    hbm_probe_bytes: u64,
    hbm_probe_actual_bytes: u64,
    hbm_probe_error: String,
    hbm_last_error: String,
    block_count: usize,
    free_blocks: usize,
    leased_blocks: usize,
    object_count: usize,
    requested_total: u64,
    actual_total: u64,
    hbm: BucketStats,
    ddr: BucketStats,
    weight: BucketStats,
    kvcache: BucketStats,
    user: BucketStats,
    filtered_count: usize,
    filtered_actual: u64,
    active_clients: usize,
    active_leases: usize,
    transfer_plans: usize,
    transfer_events: usize,
    ddr_committed: u64,
    ddr_fast_thp_pretouched_count: usize,
    ddr_fast_fallback_count: usize,
    ddr_prepare_us_total: f64,
}

pub(crate) fn get_stats(req: &Kv, shared: &SharedCatalog) -> Kv {
    let role_filter = get(req, "role_filter").to_string();
    let stats_node = env::var("UF_DDR_NUMA_NODE")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0);

    let snapshot = {
        let (lock, _) = &**shared;
        let st = lock.lock().unwrap();
        let mut snapshot = StatsSnapshot {
            device: st.device,
            hbm_available_flag: st.hbm.available,
            hbm_probe_attempted: st.hbm.probe_attempted,
            hbm_probe_bytes: st.hbm.probe_bytes,
            hbm_probe_actual_bytes: st.hbm.probe_actual_bytes,
            hbm_probe_error: st.hbm.probe_error.clone(),
            hbm_last_error: st.hbm.last_error.clone(),
            block_count: st.blocks.len(),
            free_blocks: st
                .blocks
                .iter()
                .filter(|block| block.object_id == 0)
                .count(),
            object_count: st.objects.len(),
            active_clients: st.clients.len(),
            active_leases: st
                .leases
                .values()
                .filter(|lease| lease.state == "Active")
                .count(),
            transfer_plans: st.transfer_plans.len(),
            transfer_events: st.transfer_events.len(),
            ddr_committed: ddr_committed_bytes(&st, Some(stats_node)),
            ..StatsSnapshot::default()
        };
        snapshot.leased_blocks = snapshot.block_count.saturating_sub(snapshot.free_blocks);

        for object in st.objects.values() {
            snapshot.requested_total += object.requested_bytes;
            snapshot.actual_total += object.actual_bytes;
            let placement_bucket = if object.placement == HBM_PLACEMENT {
                Some(&mut snapshot.hbm)
            } else if object.placement == DDR_PLACEMENT {
                Some(&mut snapshot.ddr)
            } else {
                None
            };
            if let Some(bucket) = placement_bucket {
                bucket.count += 1;
                bucket.requested += object.requested_bytes;
                bucket.actual += object.actual_bytes;
            }
            if object.placement == DDR_PLACEMENT {
                if object.ddr_fast_profile == "thp_pretouched" {
                    snapshot.ddr_fast_thp_pretouched_count += 1;
                }
                if !object.ddr_fallback_reason.is_empty() {
                    snapshot.ddr_fast_fallback_count += 1;
                }
                snapshot.ddr_prepare_us_total += object.ddr_prepare_us;
            }
            let role_bucket = match object.role.as_str() {
                "weight" => Some(&mut snapshot.weight),
                "kvcache" => Some(&mut snapshot.kvcache),
                "user" => Some(&mut snapshot.user),
                _ => None,
            };
            if let Some(bucket) = role_bucket {
                bucket.count += 1;
                bucket.requested += object.requested_bytes;
                bucket.actual += object.actual_bytes;
            }
            if role_filter.is_empty() || object.role == role_filter {
                snapshot.filtered_count += 1;
                snapshot.filtered_actual += object.actual_bytes;
            }
        }
        snapshot
    };

    let (mem, hbm_mem_info_error) = if snapshot.hbm_probe_attempted {
        match NpuHbmAclBackend::default().mem_info(snapshot.device) {
            Ok(mem) => (mem, String::new()),
            Err(e) => (HbmMemInfo::default(), e),
        }
    } else {
        (
            HbmMemInfo::default(),
            "hbm startup probe skipped; mem_info unavailable".to_string(),
        )
    };
    let hbm_available = snapshot.hbm_available_flag && hbm_mem_info_error.is_empty();
    let ddr = ddr_info_with_committed(stats_node, snapshot.ddr_committed);
    let ddr_backend_mode = if ddr.root.starts_with("/dev/shm") {
        "tmpfs_mmap"
    } else {
        "file_mmap"
    };
    let channel = transfer_channel_manager().stats();
    let direct_executor = direct_transfer_executor().stats();
    let direct_lanes = direct_lane_manager().stats();

    ok(&[
        ("device_id", snapshot.device.to_string()),
        (
            "hbm_available",
            if hbm_available { "true" } else { "false" }.to_string(),
        ),
        (
            "hbm_probe_attempted",
            if snapshot.hbm_probe_attempted {
                "true"
            } else {
                "false"
            }
            .to_string(),
        ),
        ("hbm_probe_bytes", snapshot.hbm_probe_bytes.to_string()),
        (
            "hbm_probe_actual_bytes",
            snapshot.hbm_probe_actual_bytes.to_string(),
        ),
        (
            "hbm_probe_error",
            sanitize_kv_value(&snapshot.hbm_probe_error),
        ),
        (
            "hbm_last_error",
            sanitize_kv_value(&snapshot.hbm_last_error),
        ),
        ("hbm_mem_info_error", sanitize_kv_value(&hbm_mem_info_error)),
        ("hbm_free_bytes", mem.free_bytes.to_string()),
        ("hbm_total_bytes", mem.total_bytes.to_string()),
        ("block_count", snapshot.block_count.to_string()),
        ("free_blocks", snapshot.free_blocks.to_string()),
        ("leased_blocks", snapshot.leased_blocks.to_string()),
        ("object_count", snapshot.object_count.to_string()),
        ("requested_bytes", snapshot.requested_total.to_string()),
        ("actual_bytes", snapshot.actual_total.to_string()),
        ("hbm_objects", snapshot.hbm.count.to_string()),
        ("hbm_requested_bytes", snapshot.hbm.requested.to_string()),
        ("hbm_actual_bytes", snapshot.hbm.actual.to_string()),
        ("ddr_objects", snapshot.ddr.count.to_string()),
        ("ddr_requested_bytes", snapshot.ddr.requested.to_string()),
        ("ddr_actual_bytes", snapshot.ddr.actual.to_string()),
        ("ddr_root", ddr.root.to_string_lossy().to_string()),
        ("ddr_numa_node", ddr.node.to_string()),
        ("ddr_fs_total_bytes", ddr.fs_total.to_string()),
        ("ddr_fs_free_bytes", ddr.fs_free.to_string()),
        ("ddr_fs_available_bytes", ddr.fs_available.to_string()),
        ("ddr_cgroup_limit_bytes", ddr.cgroup_limit.to_string()),
        ("ddr_cgroup_current_bytes", ddr.cgroup_current.to_string()),
        ("ddr_numa_mem_free_bytes", ddr.numa_free.to_string()),
        ("ddr_committed_bytes", ddr.committed.to_string()),
        (
            "ddr_fast_thp_pretouched_count",
            snapshot.ddr_fast_thp_pretouched_count.to_string(),
        ),
        (
            "ddr_fast_fallback_count",
            snapshot.ddr_fast_fallback_count.to_string(),
        ),
        (
            "ddr_prepare_us_total",
            format!("{:.3}", snapshot.ddr_prepare_us_total),
        ),
        (
            "ddr_safe_allocatable_bytes",
            ddr.safe_allocatable.to_string(),
        ),
        ("ddr_backend_mode", ddr_backend_mode.to_string()),
        ("weight_objects", snapshot.weight.count.to_string()),
        (
            "weight_requested_bytes",
            snapshot.weight.requested.to_string(),
        ),
        ("weight_actual_bytes", snapshot.weight.actual.to_string()),
        ("kvcache_objects", snapshot.kvcache.count.to_string()),
        (
            "kvcache_requested_bytes",
            snapshot.kvcache.requested.to_string(),
        ),
        ("kvcache_actual_bytes", snapshot.kvcache.actual.to_string()),
        ("user_objects", snapshot.user.count.to_string()),
        ("user_requested_bytes", snapshot.user.requested.to_string()),
        ("user_actual_bytes", snapshot.user.actual.to_string()),
        ("filtered_objects", snapshot.filtered_count.to_string()),
        (
            "filtered_actual_bytes",
            snapshot.filtered_actual.to_string(),
        ),
        ("active_clients", snapshot.active_clients.to_string()),
        ("active_leases", snapshot.active_leases.to_string()),
        ("transfer_plans", snapshot.transfer_plans.to_string()),
        ("transfer_events", snapshot.transfer_events.to_string()),
        ("pinned_total_bytes", channel.pinned_total_bytes.to_string()),
        ("pinned_used_bytes", channel.pinned_used_bytes.to_string()),
        ("pinned_idle_bytes", channel.pinned_idle_bytes.to_string()),
        ("h2d_lane_count", channel.h2d_lane_count.to_string()),
        ("d2h_lane_count", channel.d2h_lane_count.to_string()),
        ("h2d_busy_lanes", channel.h2d_busy_lanes.to_string()),
        ("d2h_busy_lanes", channel.d2h_busy_lanes.to_string()),
        ("lane_wait_count", channel.lane_wait_count.to_string()),
        ("budget_wait_us", format!("{:.3}", channel.budget_wait_us)),
        ("chunk_bytes_h2d", channel.chunk_bytes_h2d.to_string()),
        ("chunk_bytes_d2h", channel.chunk_bytes_d2h.to_string()),
        ("pinned_chunk_count", channel.chunk_count.to_string()),
        ("h2d_max_lanes", channel.h2d_max_lanes.to_string()),
        ("d2h_max_lanes", channel.d2h_max_lanes.to_string()),
        ("pinned_idle_ttl_ms", channel.idle_ttl_ms.to_string()),
        (
            "pinned_idle_reaped_lanes",
            channel.idle_reaped_lanes.to_string(),
        ),
        (
            "pinned_idle_reaped_bytes",
            channel.idle_reaped_bytes.to_string(),
        ),
        (
            "transfer_channel_acquires",
            channel.total_acquires.to_string(),
        ),
        (
            "direct_transfer_workers",
            direct_executor.worker_count.to_string(),
        ),
        (
            "direct_transfer_queue_depth",
            direct_executor.queue_depth.to_string(),
        ),
        (
            "direct_transfer_queue_depth_high_watermark",
            direct_executor.queue_depth_high_watermark.to_string(),
        ),
        (
            "direct_transfer_submitted_jobs",
            direct_executor.submitted_jobs.to_string(),
        ),
        (
            "direct_transfer_completed_jobs",
            direct_executor.completed_jobs.to_string(),
        ),
        (
            "direct_transfer_queue_wait_count",
            direct_executor.queue_wait_count.to_string(),
        ),
        (
            "direct_transfer_queue_wait_us_total",
            format!("{:.3}", direct_executor.queue_wait_us_total),
        ),
        (
            "direct_transfer_worker_execute_us_total",
            format!("{:.3}", direct_executor.worker_execute_us_total),
        ),
        ("direct_lane_count", direct_lanes.lane_count.to_string()),
        (
            "direct_h2d_lane_count",
            direct_lanes.h2d_lane_count.to_string(),
        ),
        (
            "direct_d2h_lane_count",
            direct_lanes.d2h_lane_count.to_string(),
        ),
        (
            "direct_h2d_busy_lanes",
            direct_lanes.h2d_busy_lanes.to_string(),
        ),
        (
            "direct_d2h_busy_lanes",
            direct_lanes.d2h_busy_lanes.to_string(),
        ),
        (
            "direct_lane_acquires",
            direct_lanes.total_acquires.to_string(),
        ),
        (
            "direct_lane_wait_count",
            direct_lanes.lane_wait_count.to_string(),
        ),
        (
            "direct_lane_wait_us_total",
            format!("{:.3}", direct_lanes.lane_wait_us_total),
        ),
        (
            "direct_stream_create_count",
            direct_lanes.stream_create_count.to_string(),
        ),
        (
            "direct_event_create_count",
            direct_lanes.event_create_count.to_string(),
        ),
        (
            "direct_event_reuse_count",
            direct_lanes.event_reuse_count.to_string(),
        ),
        (
            "direct_h2d_max_lanes",
            direct_lanes.h2d_max_lanes.to_string(),
        ),
        (
            "direct_d2h_max_lanes",
            direct_lanes.d2h_max_lanes.to_string(),
        ),
        ("direct_idle_ttl_ms", direct_lanes.idle_ttl_ms.to_string()),
        (
            "direct_idle_reaped_lanes",
            direct_lanes.idle_reaped_lanes.to_string(),
        ),
    ])
}
