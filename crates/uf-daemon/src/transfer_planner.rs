use crate::catalog::{Object, SharedCatalog, TransferPlanRecord};
use crate::common::{object_by_request, transfer_plan_payload, DDR_PLACEMENT, HBM_PLACEMENT};
use crate::trace::{span, TraceCategory};
use uf_core::{err, get, get_u64, ok, Kv};

struct TransferClassification {
    path: String,
    engine: String,
    completion_kind: String,
    effort: f64,
    estimated_latency_us: f64,
    estimated_bandwidth_gib_s: f64,
    setup_cost_us: f64,
    hop_count: u32,
    fallback_used: bool,
    fallback_reason: String,
    explanation: String,
}

fn classify_transfer(
    src: &Object,
    dst: &Object,
    nbytes: u64,
    mode: &str,
) -> TransferClassification {
    let mode = if mode.is_empty() { "auto" } else { mode };
    if src.placement_id == dst.placement_id || src.object_id == dst.object_id {
        return TransferClassification {
            path: "direct_ref".to_string(),
            engine: "descriptor".to_string(),
            completion_kind: "immediate".to_string(),
            effort: 0.0,
            estimated_latency_us: 1.0,
            estimated_bandwidth_gib_s: 0.0,
            setup_cost_us: 1.0,
            hop_count: 0,
            fallback_used: false,
            fallback_reason: String::new(),
            explanation: "same placement/object no data movement required".to_string(),
        };
    }

    let mib = (nbytes as f64) / (1024.0 * 1024.0);
    match (src.placement.as_str(), dst.placement.as_str()) {
        (HBM_PLACEMENT, DDR_PLACEMENT) => {
            let (engine, explanation, effort, bandwidth, setup_cost) = if mode == "auto"
                || mode.contains("direct")
            {
                (
                    "acl_direct_async_thp",
                    "HBM to host DDR uses daemon service-owned HBM VA and THP/pre-touch mmap DDR direct ACL D2H",
                    1.0,
                    40.0,
                    10.0,
                )
            } else if mode.contains("pinned") && mode.contains("async") {
                (
                    "acl_pinned_async_channel",
                    "HBM to host DDR uses daemon pinned staging channel fallback",
                    1.5,
                    16.0,
                    30.0,
                )
            } else if mode.contains("pinned") {
                (
                    "acl_pinned_sync_channel",
                    "HBM to host DDR uses daemon pinned staging channel fallback",
                    1.8,
                    12.0,
                    30.0,
                )
            } else {
                (
                    "acl_sync",
                    "HBM to host DDR uses synchronous ACL D2H",
                    2.0,
                    20.0,
                    10.0,
                )
            };
            TransferClassification {
                path: "hbm_to_ddr".to_string(),
                engine: engine.to_string(),
                completion_kind: "thread_event".to_string(),
                effort,
                estimated_latency_us: 40.0 + mib * (1024.0 / bandwidth),
                estimated_bandwidth_gib_s: bandwidth,
                setup_cost_us: setup_cost,
                hop_count: 1,
                fallback_used: false,
                fallback_reason: String::new(),
                explanation: explanation.to_string(),
            }
        }
        (DDR_PLACEMENT, HBM_PLACEMENT) => {
            let (engine, explanation, effort, bandwidth, setup_cost) =
                if mode == "auto" || mode.contains("direct") {
                    (
                        "acl_direct_async_thp",
                        "Host DDR to HBM uses THP/pre-touch mmap DDR direct ACL H2D",
                        0.8,
                        54.0,
                        10.0,
                    )
                } else if mode.contains("pinned") && mode.contains("async") {
                    (
                        "acl_pinned_async_channel",
                        "Host DDR to HBM uses daemon pinned staging channel fallback",
                        1.0,
                        24.0,
                        30.0,
                    )
                } else if mode.contains("pinned") {
                    (
                        "acl_pinned_sync_channel",
                        "Host DDR to HBM uses daemon pinned staging channel fallback",
                        1.3,
                        16.0,
                        30.0,
                    )
                } else {
                    (
                        "acl_sync",
                        "Host DDR to HBM uses synchronous ACL H2D",
                        1.5,
                        24.0,
                        10.0,
                    )
                };
            TransferClassification {
                path: "ddr_to_hbm".to_string(),
                engine: engine.to_string(),
                completion_kind: "thread_event".to_string(),
                effort,
                estimated_latency_us: 35.0 + mib * (1024.0 / bandwidth),
                estimated_bandwidth_gib_s: bandwidth,
                setup_cost_us: setup_cost,
                hop_count: 1,
                fallback_used: false,
                fallback_reason: String::new(),
                explanation: explanation.to_string(),
            }
        }
        (HBM_PLACEMENT, HBM_PLACEMENT) => {
            let same_target = src.target == dst.target;
            TransferClassification {
                path: "hbm_to_hbm".to_string(),
                engine: if mode.contains("async") {
                    "acl_d2d_async"
                } else if same_target {
                    "acl_d2d"
                } else {
                    "acl_d2d_peer"
                }
                .to_string(),
                completion_kind: "thread_event".to_string(),
                effort: if same_target { 0.8 } else { 4.0 },
                estimated_latency_us: if same_target { 40.0 + mib * 6.0 } else { 180.0 + mib * 36.0 },
                estimated_bandwidth_gib_s: if same_target { 40.0 } else { 24.0 },
                setup_cost_us: if same_target { 15.0 } else { 40.0 },
                hop_count: 1,
                fallback_used: false,
                fallback_reason: String::new(),
                explanation: if same_target {
                    "Same-device HBM to HBM uses daemon service-owned VA and ACL device-to-device copy"
                } else {
                    "Cross-device HBM to HBM uses one daemon process, peer access, and ACL device-to-device copy"
                }
                .to_string(),
            }
        }
        (DDR_PLACEMENT, DDR_PLACEMENT) => {
            let same_target = src.target == dst.target;
            TransferClassification {
                path: "ddr_to_ddr".to_string(),
                engine: "cpu_memcpy".to_string(),
                completion_kind: "thread_event".to_string(),
                effort: if same_target { 0.5 } else { 1.2 },
                estimated_latency_us: if same_target {
                    20.0 + mib * 4.0
                } else {
                    50.0 + mib * 8.0
                },
                estimated_bandwidth_gib_s: if same_target { 30.0 } else { 16.0 },
                setup_cost_us: 10.0,
                hop_count: if same_target { 0 } else { 1 },
                fallback_used: false,
                fallback_reason: String::new(),
                explanation: if same_target {
                    "Same host DDR domain uses mmap-backed CPU memcpy"
                } else {
                    "Different host DDR domains use CPU memcpy with higher modeled cost"
                }
                .to_string(),
            }
        }
        _ => TransferClassification {
            path: "unsupported".to_string(),
            engine: "none".to_string(),
            completion_kind: "immediate".to_string(),
            effort: f64::INFINITY,
            estimated_latency_us: 0.0,
            estimated_bandwidth_gib_s: 0.0,
            setup_cost_us: 0.0,
            hop_count: 0,
            fallback_used: false,
            fallback_reason: "unsupported_medium_pair".to_string(),
            explanation: "Unsupported PhaseE-01 local transfer medium pair".to_string(),
        },
    }
}

pub(crate) fn estimate_cost(req: &Kv, shared: &SharedCatalog) -> Kv {
    let (lock, _) = &**shared;
    let st = lock.lock().unwrap();
    let src = match object_by_request(req, &st, "src_object_id", "src_placement_id") {
        Some(object) => object,
        None => return err("source object not found"),
    };
    let dst = match object_by_request(req, &st, "dst_object_id", "dst_placement_id") {
        Some(object) => object,
        None => return err("destination object not found"),
    };
    let mut nbytes = get_u64(req, "nbytes");
    if nbytes == 0 {
        nbytes = src.requested_bytes.min(dst.requested_bytes);
    }
    if nbytes > src.requested_bytes || nbytes > dst.requested_bytes {
        return err("transfer size exceeds source or destination object");
    }
    let mode = get(req, "mode");
    let class = classify_transfer(
        &src,
        &dst,
        nbytes,
        if mode.is_empty() { "auto" } else { mode },
    );
    if class.path == "unsupported" {
        return err(class.explanation);
    }
    ok(&[
        ("src_object_id", src.object_id.to_string()),
        ("src_placement_id", src.placement_id.to_string()),
        ("dst_object_id", dst.object_id.to_string()),
        ("dst_placement_id", dst.placement_id.to_string()),
        ("path", class.path),
        ("engine", class.engine),
        ("completion_kind", class.completion_kind),
        ("nbytes", nbytes.to_string()),
        ("effort", format!("{:.3}", class.effort)),
        (
            "estimated_latency_us",
            format!("{:.3}", class.estimated_latency_us),
        ),
        (
            "estimated_bandwidth_gib_s",
            format!("{:.3}", class.estimated_bandwidth_gib_s),
        ),
        ("setup_cost_us", format!("{:.3}", class.setup_cost_us)),
        ("hop_count", class.hop_count.to_string()),
        (
            "fallback_used",
            if class.fallback_used { "true" } else { "false" }.to_string(),
        ),
        ("fallback_reason", class.fallback_reason),
        ("explanation", class.explanation),
    ])
}

pub(crate) fn plan_transfer(req: &Kv, shared: &SharedCatalog) -> Kv {
    let _trace = span(
        TraceCategory::Transfer,
        "transfer.plan",
        vec![
            ("client_id", get_u64(req, "client_id").to_string()),
            ("src_object_id", get_u64(req, "src_object_id").to_string()),
            ("dst_object_id", get_u64(req, "dst_object_id").to_string()),
            ("bytes", get_u64(req, "nbytes").to_string()),
            ("mode", get(req, "mode").to_string()),
        ],
    );
    let (lock, _) = &**shared;
    let mut st = lock.lock().unwrap();
    let client_id = get_u64(req, "client_id");
    if client_id != 0 && !st.clients.contains_key(&client_id) {
        return err("client not found");
    }
    let src = match object_by_request(req, &st, "src_object_id", "src_placement_id") {
        Some(object) => object,
        None => return err("source object not found"),
    };
    let dst = match object_by_request(req, &st, "dst_object_id", "dst_placement_id") {
        Some(object) => object,
        None => return err("destination object not found"),
    };
    let mut nbytes = get_u64(req, "nbytes");
    if nbytes == 0 {
        nbytes = src.requested_bytes.min(dst.requested_bytes);
    }
    if nbytes > src.requested_bytes || nbytes > dst.requested_bytes {
        return err("transfer size exceeds source or destination object");
    }
    let request_id = {
        let id = get_u64(req, "request_id");
        if id == 0 {
            st.take_next_transfer_plan_id()
        } else {
            id
        }
    };
    let plan_id = st.take_next_transfer_plan_id();
    let operation = if get(req, "operation").is_empty() {
        "copy"
    } else {
        get(req, "operation")
    }
    .to_string();
    let wait_policy = if get(req, "wait_policy").is_empty() {
        "return_immediately"
    } else {
        get(req, "wait_policy")
    }
    .to_string();
    let mode = if get(req, "mode").is_empty() {
        "auto"
    } else {
        get(req, "mode")
    };
    let class = classify_transfer(&src, &dst, nbytes, mode);
    if class.path == "unsupported" {
        return err(class.explanation);
    }
    let plan = TransferPlanRecord {
        plan_id,
        request_id,
        client_id,
        src_object_id: src.object_id,
        src_placement_id: src.placement_id,
        dst_object_id: dst.object_id,
        dst_placement_id: dst.placement_id,
        operation,
        path: class.path,
        engine: class.engine,
        completion_kind: class.completion_kind,
        wait_policy,
        nbytes,
        effort: class.effort,
        estimated_latency_us: class.estimated_latency_us,
        estimated_bandwidth_gib_s: class.estimated_bandwidth_gib_s,
        setup_cost_us: class.setup_cost_us,
        hop_count: class.hop_count,
        fallback_used: class.fallback_used,
        fallback_reason: class.fallback_reason,
        explanation: class.explanation,
    };
    st.transfer_plans.insert(plan_id, plan.clone());
    eprintln!(
        "{{\"event\":\"transfer_planned\",\"plan_id\":{},\"client_id\":{},\"path\":\"{}\",\"engine\":\"{}\",\"nbytes\":{},\"fallback_used\":{}}}",
        plan.plan_id, plan.client_id, plan.path, plan.engine, plan.nbytes, plan.fallback_used
    );
    ok(&transfer_plan_payload(&plan))
}
