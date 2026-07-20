use crate::acl_backend::HbmExport;
use crate::catalog::{Catalog, Lease, Object, TransferEventRecord, TransferPlanRecord};
use uf_core::{get, get_u64, ok, Kv};

use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) const HBM_PLACEMENT: &str = "hbm";
pub(crate) const DDR_PLACEMENT: &str = "ddr";
pub(crate) const SSD_PLACEMENT: &str = "ssd";

pub(crate) fn now_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

pub(crate) fn get_bool(req: &Kv, key: &str) -> bool {
    matches!(get(req, key), "1" | "true" | "True" | "yes" | "YES")
}

pub(crate) fn get_bool_default(req: &Kv, key: &str, default: bool) -> bool {
    let value = get(req, key);
    if value.is_empty() {
        return default;
    }
    matches!(value, "1" | "true" | "True" | "yes" | "YES" | "on")
}

pub(crate) fn env_bool(key: &str, default: bool) -> bool {
    match env::var(key) {
        Ok(value) => matches!(value.as_str(), "1" | "true" | "True" | "yes" | "YES" | "on"),
        Err(_) => default,
    }
}

pub(crate) fn sanitize_kv_value(value: &str) -> String {
    value.replace(';', ",").replace('=', ":").replace('\n', " ")
}

pub(crate) fn requested_bytes(req: &Kv) -> u64 {
    for key in ["nbytes", "requested_bytes", "size_bytes", "bytes"] {
        let value = get_u64(req, key);
        if value != 0 {
            return value;
        }
    }
    0
}

pub(crate) fn role_from_req(req: &Kv) -> String {
    let role = get(req, "role");
    if !role.is_empty() {
        return role.to_string();
    }
    let object_type = get(req, "object_type");
    if !object_type.is_empty() {
        return object_type.to_string();
    }
    "user".to_string()
}

pub(crate) fn validate_hbm_create_request(req: &Kv, daemon_device: i32) -> Result<i32, String> {
    let hint = get(req, "hint");
    if hint != "mandatory:hbm" {
        return Err("hint must be mandatory:hbm".to_string());
    }
    parse_hbm_target(req, daemon_device)
}

pub(crate) fn is_ddr_request(req: &Kv) -> bool {
    get(req, "hint") == "mandatory:ddr" || get(req, "target").starts_with("host:")
}

pub(crate) fn is_ssd_request(req: &Kv) -> bool {
    get(req, "hint") == "mandatory:ssd" || get(req, "target").starts_with("ssd:")
}

pub(crate) fn parse_ddr_target(req: &Kv) -> Result<u32, String> {
    let hint = get(req, "hint");
    if !hint.is_empty() && hint != "mandatory:ddr" {
        return Err("hint must be mandatory:ddr".to_string());
    }
    let mut target = get(req, "target").to_string();
    if target.is_empty() {
        target = format!(
            "host:{}",
            env::var("UF_DDR_NUMA_NODE").unwrap_or_else(|_| "0".to_string())
        );
    }
    let node = target
        .strip_prefix("host:")
        .and_then(|text| text.parse::<u32>().ok())
        .ok_or_else(|| "DDR target must use host:<numa_id>".to_string())?;
    Ok(node)
}

pub(crate) fn parse_hbm_target(req: &Kv, daemon_device: i32) -> Result<i32, String> {
    let target = get(req, "target");
    if target.is_empty() {
        return Ok(daemon_device);
    }
    let device = target
        .strip_prefix("npu:")
        .and_then(|text| text.parse::<i32>().ok())
        .ok_or_else(|| "target must use npu:<device_id>".to_string())?;
    Ok(device)
}

pub(crate) fn parse_ssd_target(req: &Kv) -> Result<String, String> {
    let hint = get(req, "hint");
    if !hint.is_empty() && hint != "mandatory:ssd" {
        return Err("hint must be mandatory:ssd".to_string());
    }
    let target = if get(req, "target").is_empty() {
        "ssd:local0".to_string()
    } else {
        get(req, "target").to_string()
    };
    if target != "ssd:local0" {
        return Err("SSD target must use ssd:local0".to_string());
    }
    Ok(target)
}

pub(crate) fn validate_target_matches_object(req: &Kv, object: &Object) -> Result<(), String> {
    let target = get(req, "target");
    if target.is_empty() {
        return Ok(());
    }
    if !target.starts_with("npu:") {
        return Err("target must use npu:<device_id>".to_string());
    }
    if target != object.target {
        return Err(format!(
            "target {} does not match object target {}",
            target, object.target
        ));
    }
    Ok(())
}

pub(crate) fn create_lease_response(
    object: &Object,
    lease: &Lease,
    export: HbmExport,
    requested_bytes: u64,
    existing: bool,
) -> Kv {
    let items = vec![
        ("object_id", object.object_id.to_string()),
        ("placement_id", object.placement_id.to_string()),
        ("lease_id", lease.lease_id.to_string()),
        ("placement", HBM_PLACEMENT.to_string()),
        ("target", object.target.clone()),
        ("address_kind", "shareable_handle".to_string()),
        ("device_id", export.device_id.to_string()),
        ("actual_bytes", export.actual_bytes.to_string()),
        ("requested_bytes", requested_bytes.to_string()),
        ("shareable", export.shareable.to_string()),
        ("existing", if existing { "1" } else { "0" }.to_string()),
    ];
    ok(&items)
}

pub(crate) fn object_by_request(
    req: &Kv,
    st: &Catalog,
    object_key: &str,
    placement_key: &str,
) -> Option<Object> {
    let object_id = get_u64(req, object_key);
    if object_id != 0 {
        return st.objects.get(&object_id).cloned();
    }
    let placement_id = get_u64(req, placement_key);
    if placement_id != 0 {
        return st.find_object_by_placement_id(placement_id);
    }
    None
}

pub(crate) fn address_kind(object: &Object) -> &'static str {
    if object.placement == HBM_PLACEMENT {
        "shareable_handle"
    } else if object.placement == DDR_PLACEMENT {
        "mmap_path"
    } else if object.placement == SSD_PLACEMENT {
        "file_path_offset"
    } else {
        "virtual"
    }
}

pub(crate) fn transfer_plan_payload(plan: &TransferPlanRecord) -> Vec<(&'static str, String)> {
    vec![
        ("plan_id", plan.plan_id.to_string()),
        ("request_id", plan.request_id.to_string()),
        ("client_id", plan.client_id.to_string()),
        ("src_object_id", plan.src_object_id.to_string()),
        ("src_placement_id", plan.src_placement_id.to_string()),
        ("dst_object_id", plan.dst_object_id.to_string()),
        ("dst_placement_id", plan.dst_placement_id.to_string()),
        ("operation", plan.operation.clone()),
        ("path", plan.path.clone()),
        ("engine", plan.engine.clone()),
        ("completion_kind", plan.completion_kind.clone()),
        ("wait_policy", plan.wait_policy.clone()),
        ("src_offset_bytes", plan.src_offset_bytes.to_string()),
        ("dst_offset_bytes", plan.dst_offset_bytes.to_string()),
        ("nbytes", plan.nbytes.to_string()),
        ("effort", format!("{:.3}", plan.effort)),
        (
            "estimated_latency_us",
            format!("{:.3}", plan.estimated_latency_us),
        ),
        (
            "estimated_bandwidth_gib_s",
            format!("{:.3}", plan.estimated_bandwidth_gib_s),
        ),
        ("setup_cost_us", format!("{:.3}", plan.setup_cost_us)),
        ("hop_count", plan.hop_count.to_string()),
        (
            "fallback_used",
            if plan.fallback_used { "true" } else { "false" }.to_string(),
        ),
        ("fallback_reason", plan.fallback_reason.clone()),
        ("explanation", plan.explanation.clone()),
    ]
}

pub(crate) fn event_payload(event: &TransferEventRecord) -> Vec<(&'static str, String)> {
    vec![
        ("event_id", event.event_id.to_string()),
        ("plan_id", event.plan_id.to_string()),
        ("client_id", event.client_id.to_string()),
        ("event_status", event.status.clone()),
        ("completion_kind", event.completion_kind.clone()),
        ("submitted_at_ns", event.submitted_at_ns.to_string()),
        ("started_at_ns", event.started_at_ns.to_string()),
        ("completed_at_ns", event.completed_at_ns.to_string()),
        ("bytes_done", event.bytes_done.to_string()),
        (
            "actual_latency_us",
            format!("{:.3}", event.actual_latency_us),
        ),
        (
            "actual_bandwidth_gib_s",
            format!("{:.3}", event.actual_bandwidth_gib_s),
        ),
        ("actual_engine", event.actual_engine.clone()),
        ("actual_path", event.actual_path.clone()),
        ("channel_direction", event.channel_direction.clone()),
        ("channel_lane_id", event.channel_lane_id.to_string()),
        ("channel_device_id", event.channel_device_id.to_string()),
        ("channel_chunk_bytes", event.channel_chunk_bytes.to_string()),
        ("channel_chunk_count", event.channel_chunk_count.to_string()),
        (
            "channel_pinned_footprint_bytes",
            event.channel_pinned_footprint_bytes.to_string(),
        ),
        (
            "channel_lane_wait_us",
            format!("{:.3}", event.channel_lane_wait_us),
        ),
        (
            "channel_chunks_transferred",
            event.channel_chunks_transferred.to_string(),
        ),
        (
            "channel_cpu_copy_us",
            format!("{:.3}", event.channel_cpu_copy_us),
        ),
        (
            "channel_acl_copy_us",
            format!("{:.3}", event.channel_acl_copy_us),
        ),
        (
            "channel_acl_submit_us",
            format!("{:.3}", event.channel_acl_submit_us),
        ),
        (
            "channel_acl_wait_us",
            format!("{:.3}", event.channel_acl_wait_us),
        ),
        ("channel_wall_us", format!("{:.3}", event.channel_wall_us)),
        (
            "channel_queue_wait_us",
            format!("{:.3}", event.channel_queue_wait_us),
        ),
        (
            "channel_worker_execute_us",
            format!("{:.3}", event.channel_worker_execute_us),
        ),
        (
            "channel_overlap_ratio",
            format!("{:.6}", event.channel_overlap_ratio),
        ),
        (
            "channel_stream_create_count",
            event.channel_stream_create_count.to_string(),
        ),
        (
            "channel_event_reuse_count",
            event.channel_event_reuse_count.to_string(),
        ),
        (
            "channel_event_record_count",
            event.channel_event_record_count.to_string(),
        ),
        (
            "channel_event_wait_count",
            event.channel_event_wait_count.to_string(),
        ),
        (
            "channel_pipeline_overlap",
            if event.channel_pipeline_overlap {
                "true"
            } else {
                "false"
            }
            .to_string(),
        ),
        ("ssd_io_submit_us", format!("{:.3}", event.ssd_io_submit_us)),
        ("ssd_io_wait_us", format!("{:.3}", event.ssd_io_wait_us)),
        ("ssd_io_bytes", event.ssd_io_bytes.to_string()),
        (
            "ssd_io_bandwidth_gib_s",
            format!("{:.3}", event.ssd_io_bandwidth_gib_s),
        ),
        ("ssd_read_bytes", event.ssd_read_bytes.to_string()),
        ("ssd_write_bytes", event.ssd_write_bytes.to_string()),
        ("relay_stage_count", event.relay_stage_count.to_string()),
        ("relay_ddr_hbm_us", format!("{:.3}", event.relay_ddr_hbm_us)),
        ("relay_total_us", format!("{:.3}", event.relay_total_us)),
        ("direct_candidate", event.direct_candidate.clone()),
        ("direct_kind", event.direct_kind.clone()),
        ("direct_setup_us", format!("{:.3}", event.direct_setup_us)),
        (
            "direct_register_us",
            format!("{:.3}", event.direct_register_us),
        ),
        (
            "direct_fadvise_us",
            format!("{:.3}", event.direct_fadvise_us),
        ),
        (
            "direct_readahead_us",
            format!("{:.3}", event.direct_readahead_us),
        ),
        (
            "direct_madvise_hugepage_us",
            format!("{:.3}", event.direct_madvise_hugepage_us),
        ),
        (
            "direct_madvise_willneed_us",
            format!("{:.3}", event.direct_madvise_willneed_us),
        ),
        (
            "direct_madvise_populate_us",
            format!("{:.3}", event.direct_madvise_populate_us),
        ),
        (
            "direct_pretouch_us",
            format!("{:.3}", event.direct_pretouch_us),
        ),
        ("direct_mlock_us", format!("{:.3}", event.direct_mlock_us)),
        ("direct_acl_us", format!("{:.3}", event.direct_acl_us)),
        ("direct_total_us", format!("{:.3}", event.direct_total_us)),
        (
            "fallback_used",
            if event.fallback_used { "true" } else { "false" }.to_string(),
        ),
        ("fallback_reason", event.fallback_reason.clone()),
        ("error_code", event.error_code.clone()),
        ("error_message", event.error_message.clone()),
    ]
}
