use crate::capabilities;
use crate::catalog::SharedCatalog;
use crate::object_service;
use crate::stats_service;
use crate::trace::{span, TraceCategory};
use crate::trace_api;
use crate::transfer_executor;
use crate::transfer_planner;
use uf_core::{err, get, get_u64, Kv};

fn unsupported_removed_op(op: &str) -> Kv {
    err(format!(
        "unsupported op {}; legacy buffer/client-local transfer API has been removed",
        op
    ))
}

pub fn handle_request(req: Kv, shared: &SharedCatalog) -> Kv {
    let op = get(&req, "op").to_string();
    let _trace = span(
        TraceCategory::Control,
        "command.handle",
        vec![
            ("op", op.clone()),
            ("client_id", get_u64(&req, "client_id").to_string()),
            ("model_id", get(&req, "model_id").to_string()),
        ],
    );
    match op.as_str() {
        "RegisterClient" => object_service::register_client(&req, shared),
        "RegisterModel" => object_service::register_model(&req),
        "CreateDataObject" => object_service::create_data_object(&req, shared),
        "OpenDataObject" => object_service::open_data_object(&req, shared),
        "ReleaseDataObject" => object_service::release_data_object(&req, shared),
        "CloseLease" => object_service::close_lease(&req, shared),
        "DescribeObject" => object_service::describe_object(&req, shared),
        "MarkReady" => object_service::mark_ready(&req, shared),
        "MarkModified" | "MarkDirty" => object_service::mark_dirty(&req, shared),
        "GetModelObjects" => object_service::get_model_objects(&req, shared),
        "EstimateCost" => transfer_planner::estimate_cost(&req, shared),
        "PlanTransfer" => transfer_planner::plan_transfer(&req, shared),
        "SubmitTransfer" => transfer_executor::submit_transfer(&req, shared),
        "PollEvent" => transfer_executor::poll_event(&req, shared),
        "WaitEvent" => transfer_executor::wait_event(&req, shared),
        "CancelEvent" => transfer_executor::cancel_event(&req, shared),
        "GetStats" => stats_service::get_stats(&req, shared),
        "GetTraceStatus" => trace_api::get_trace_status(),
        "StartTrace" => trace_api::start_trace(&req),
        "StopTrace" => trace_api::stop_trace(&req),
        "FlushTrace" => trace_api::flush_trace(),
        "ClearTrace" => trace_api::clear_trace(),
        "ExportTrace" => trace_api::export_trace(&req),
        "GetCapabilities" => capabilities::get_capabilities(),
        "ShutdownDaemon" => object_service::shutdown_daemon(shared),
        "CreateBuffer"
        | "OpenBuffer"
        | "ReleaseBuffer"
        | "UploadData"
        | "GetDeviceHandle"
        | "CompleteTransferEvent"
        | "WaitObjectEvent" => unsupported_removed_op(&op),
        _ => err(format!("unknown op {}", op)),
    }
}
