use crate::common::{get_bool_default, sanitize_kv_value};
use crate::trace::{TraceConfig, TraceFlushResult};
use uf_core::{err, get, get_u64, ok, Kv};

use std::path::PathBuf;

fn trace_config_from_request(req: &Kv) -> TraceConfig {
    let mut config = TraceConfig::from_env();
    let run_id = get(req, "run_id");
    if !run_id.is_empty() {
        config.run_id = run_id.to_string();
    }
    let mode = get(req, "mode");
    if !mode.is_empty() {
        config.mode = mode.to_string();
    }
    let categories = get(req, "categories");
    if !categories.is_empty() {
        config.categories = categories.to_string();
    }
    let max_events = get_u64(req, "max_events");
    if max_events > 0 {
        config.max_events = max_events as usize;
    }
    let output_dir = get(req, "output_dir");
    if !output_dir.is_empty() {
        config.output_dir = PathBuf::from(output_dir);
    }
    config.include_args = get_bool_default(req, "include_args", config.include_args);
    config
}

fn trace_flush_payload(result: TraceFlushResult) -> Vec<(&'static str, String)> {
    vec![
        (
            "trace_flushed",
            if result.flushed { "true" } else { "false" }.to_string(),
        ),
        ("trace_run_id", result.run_id),
        ("trace_output_dir", result.output_dir),
        ("trace_event_count", result.event_count.to_string()),
        ("trace_dropped_events", result.dropped_events.to_string()),
        ("trace_error", sanitize_kv_value(&result.error)),
    ]
}

pub(crate) fn get_trace_status() -> Kv {
    ok(&crate::trace::collector().status_payload())
}

pub(crate) fn start_trace(req: &Kv) -> Kv {
    match crate::trace::collector().start(trace_config_from_request(req)) {
        Ok(result) => {
            let mut payload = trace_flush_payload(result);
            payload.extend(crate::trace::collector().status_payload());
            ok(&payload)
        }
        Err(e) => err(e),
    }
}

pub(crate) fn stop_trace(req: &Kv) -> Kv {
    let flush = get_bool_default(req, "flush", true);
    let result = crate::trace::collector().stop(flush);
    let mut payload = trace_flush_payload(result);
    payload.extend(crate::trace::collector().status_payload());
    ok(&payload)
}

pub(crate) fn flush_trace() -> Kv {
    let result = crate::trace::collector().flush();
    let mut payload = trace_flush_payload(result);
    payload.extend(crate::trace::collector().status_payload());
    ok(&payload)
}

pub(crate) fn clear_trace() -> Kv {
    let result = crate::trace::collector().clear();
    let mut payload = trace_flush_payload(result);
    payload.extend(crate::trace::collector().status_payload());
    ok(&payload)
}

pub(crate) fn export_trace(req: &Kv) -> Kv {
    let format = if get(req, "format").is_empty() {
        "chrome"
    } else {
        get(req, "format")
    };
    let result = crate::trace::collector().flush();
    let file = match format {
        "summary" => "trace_summary.json",
        "csv" => "trace_summary.csv",
        "md" => "summary.md",
        _ => "trace_events.json",
    };
    let mut payload = trace_flush_payload(result.clone());
    payload.push(("trace_format", format.to_string()));
    payload.push((
        "trace_artifact",
        if result.output_dir.is_empty() {
            String::new()
        } else {
            PathBuf::from(&result.output_dir)
                .join(file)
                .to_string_lossy()
                .to_string()
        },
    ));
    payload.extend(crate::trace::collector().status_payload());
    ok(&payload)
}
