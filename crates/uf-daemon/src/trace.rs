use std::collections::{HashMap, VecDeque};
use std::env;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_MAX_EVENTS: usize = 200_000;
const DEFAULT_OUTPUT_DIR: &str = "/tmp/uflow_traces";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TraceCategory {
    Control,
    Object,
    Transfer,
    Channel,
    Chunk,
    Acl,
    Backend,
    Counter,
}

impl TraceCategory {
    pub fn as_str(self) -> &'static str {
        match self {
            TraceCategory::Control => "control",
            TraceCategory::Object => "object",
            TraceCategory::Transfer => "transfer",
            TraceCategory::Channel => "channel",
            TraceCategory::Chunk => "chunk",
            TraceCategory::Acl => "acl",
            TraceCategory::Backend => "backend",
            TraceCategory::Counter => "counter",
        }
    }

    pub fn chrome_cat(self) -> &'static str {
        match self {
            TraceCategory::Control => "uflow.control",
            TraceCategory::Object => "uflow.object",
            TraceCategory::Transfer => "uflow.transfer",
            TraceCategory::Channel => "uflow.channel",
            TraceCategory::Chunk => "uflow.chunk",
            TraceCategory::Acl => "uflow.acl",
            TraceCategory::Backend => "uflow.backend",
            TraceCategory::Counter => "uflow.counter",
        }
    }

    fn bit(self) -> u64 {
        1u64 << (self as u8)
    }

    fn from_str(value: &str) -> Option<Self> {
        match value.trim() {
            "control" | "uflow.control" => Some(TraceCategory::Control),
            "object" | "uflow.object" => Some(TraceCategory::Object),
            "transfer" | "uflow.transfer" => Some(TraceCategory::Transfer),
            "channel" | "uflow.channel" => Some(TraceCategory::Channel),
            "chunk" | "uflow.chunk" => Some(TraceCategory::Chunk),
            "acl" | "uflow.acl" => Some(TraceCategory::Acl),
            "backend" | "uflow.backend" => Some(TraceCategory::Backend),
            "counter" | "uflow.counter" => Some(TraceCategory::Counter),
            _ => None,
        }
    }
}

const ALL_CATEGORIES: [TraceCategory; 8] = [
    TraceCategory::Control,
    TraceCategory::Object,
    TraceCategory::Transfer,
    TraceCategory::Channel,
    TraceCategory::Chunk,
    TraceCategory::Acl,
    TraceCategory::Backend,
    TraceCategory::Counter,
];

#[derive(Clone, Debug)]
pub struct TraceConfig {
    pub run_id: String,
    pub mode: String,
    pub categories: String,
    pub max_events: usize,
    pub output_dir: PathBuf,
    pub include_args: bool,
}

impl TraceConfig {
    pub fn from_env() -> Self {
        let mode = env::var("UF_TRACE_MODE").unwrap_or_else(|_| "hot".to_string());
        let categories = env::var("UF_TRACE_CATEGORIES").unwrap_or_default();
        let run_id = env::var("UF_TRACE_RUN_ID").unwrap_or_else(|_| default_run_id());
        let output_dir = env::var("UF_TRACE_OUTPUT_DIR")
            .or_else(|_| env::var("UF_TRACE_ROOT"))
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(DEFAULT_OUTPUT_DIR));
        let max_events = env::var("UF_TRACE_MAX_EVENTS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_MAX_EVENTS);
        let include_args = env_bool("UF_TRACE_INCLUDE_ARGS", true);
        Self {
            run_id,
            mode,
            categories,
            max_events,
            output_dir,
            include_args,
        }
    }

    fn category_mask(&self) -> u64 {
        if !self.categories.trim().is_empty() {
            return mask_from_categories(&self.categories);
        }
        mask_from_mode(&self.mode)
    }

    fn sanitized(mut self) -> Self {
        if self.run_id.is_empty() {
            self.run_id = default_run_id();
        }
        self.run_id = sanitize_path_component(&self.run_id);
        if self.mode.is_empty() {
            self.mode = "hot".to_string();
        }
        if self.max_events == 0 {
            self.max_events = DEFAULT_MAX_EVENTS;
        }
        self
    }
}

#[derive(Clone, Debug)]
struct TraceEvent {
    name: String,
    category: TraceCategory,
    ts_us: u128,
    dur_us: f64,
    pid: u32,
    tid: u64,
    args: Vec<(String, String)>,
}

#[derive(Default)]
struct TraceState {
    config: Option<TraceConfig>,
    events: VecDeque<TraceEvent>,
    started_at_us: u128,
    stopped_at_us: u128,
    flush_count: u64,
    last_flush_dir: String,
    last_flush_error: String,
}

#[derive(Clone, Debug, Default)]
pub struct TraceFlushResult {
    pub flushed: bool,
    pub run_id: String,
    pub output_dir: String,
    pub event_count: usize,
    pub dropped_events: u64,
    pub error: String,
}

pub struct TraceCollector {
    enabled: AtomicBool,
    category_mask: AtomicU64,
    dropped_events: AtomicU64,
    state: Mutex<TraceState>,
}

static TRACE_COLLECTOR: OnceLock<TraceCollector> = OnceLock::new();

pub fn collector() -> &'static TraceCollector {
    TRACE_COLLECTOR.get_or_init(TraceCollector::new)
}

pub fn span(
    category: TraceCategory,
    name: &'static str,
    args: Vec<(&'static str, String)>,
) -> TraceSpan {
    collector().span(category, name, args)
}

pub fn record_duration_us(
    category: TraceCategory,
    name: &'static str,
    dur_us: f64,
    args: Vec<(&'static str, String)>,
) {
    collector().record_duration_us(category, name, dur_us, args);
}

impl TraceCollector {
    fn new() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            category_mask: AtomicU64::new(0),
            dropped_events: AtomicU64::new(0),
            state: Mutex::new(TraceState::default()),
        }
    }

    pub fn init_from_env(&self) {
        if env_bool("UF_TRACE_ENABLE", false) {
            let _ = self.start(TraceConfig::from_env());
        }
    }

    pub fn start(&self, config: TraceConfig) -> Result<TraceFlushResult, String> {
        let config = config.sanitized();
        let mask = config.category_mask();
        if mask == 0 {
            return Err("trace categories are empty or invalid".to_string());
        }
        self.enabled.store(false, Ordering::Release);
        self.dropped_events.store(0, Ordering::Relaxed);
        self.category_mask.store(mask, Ordering::Release);
        let mut state = self.state.lock().unwrap();
        state.events.clear();
        state.started_at_us = now_us();
        state.stopped_at_us = 0;
        state.flush_count = 0;
        state.last_flush_dir.clear();
        state.last_flush_error.clear();
        state.config = Some(config.clone());
        self.enabled.store(true, Ordering::Release);
        Ok(TraceFlushResult {
            flushed: false,
            run_id: config.run_id,
            output_dir: config.output_dir.to_string_lossy().to_string(),
            event_count: 0,
            dropped_events: 0,
            error: String::new(),
        })
    }

    pub fn stop(&self, flush: bool) -> TraceFlushResult {
        self.enabled.store(false, Ordering::Release);
        {
            let mut state = self.state.lock().unwrap();
            state.stopped_at_us = now_us();
        }
        if flush {
            self.flush()
        } else {
            let status = self.status_payload();
            TraceFlushResult {
                flushed: false,
                run_id: value_from_payload(&status, "trace_run_id"),
                output_dir: value_from_payload(&status, "trace_output_dir"),
                event_count: value_from_payload(&status, "trace_buffered_events")
                    .parse()
                    .unwrap_or(0),
                dropped_events: self.dropped_events.load(Ordering::Relaxed),
                error: String::new(),
            }
        }
    }

    pub fn clear(&self) -> TraceFlushResult {
        self.enabled.store(false, Ordering::Release);
        self.category_mask.store(0, Ordering::Release);
        self.dropped_events.store(0, Ordering::Relaxed);
        let mut state = self.state.lock().unwrap();
        let run_id = state
            .config
            .as_ref()
            .map(|config| config.run_id.clone())
            .unwrap_or_default();
        let output_dir = state
            .config
            .as_ref()
            .map(|config| config.output_dir.to_string_lossy().to_string())
            .unwrap_or_default();
        state.events.clear();
        state.config = None;
        state.last_flush_dir.clear();
        state.last_flush_error.clear();
        TraceFlushResult {
            flushed: false,
            run_id,
            output_dir,
            event_count: 0,
            dropped_events: 0,
            error: String::new(),
        }
    }

    pub fn flush(&self) -> TraceFlushResult {
        let mut state = self.state.lock().unwrap();
        let Some(config) = state.config.clone() else {
            return TraceFlushResult {
                flushed: false,
                error: "trace is not configured".to_string(),
                ..TraceFlushResult::default()
            };
        };
        let events = state.events.iter().cloned().collect::<Vec<_>>();
        let dropped = self.dropped_events.load(Ordering::Relaxed);
        let run_dir = config.output_dir.join(&config.run_id);
        let result = write_trace_files(&run_dir, &config, &events, dropped);
        match result {
            Ok(()) => {
                state.flush_count += 1;
                state.last_flush_dir = run_dir.to_string_lossy().to_string();
                state.last_flush_error.clear();
                TraceFlushResult {
                    flushed: true,
                    run_id: config.run_id,
                    output_dir: state.last_flush_dir.clone(),
                    event_count: events.len(),
                    dropped_events: dropped,
                    error: String::new(),
                }
            }
            Err(error) => {
                state.last_flush_error = error.clone();
                TraceFlushResult {
                    flushed: false,
                    run_id: config.run_id,
                    output_dir: run_dir.to_string_lossy().to_string(),
                    event_count: events.len(),
                    dropped_events: dropped,
                    error,
                }
            }
        }
    }

    pub fn status_payload(&self) -> Vec<(&'static str, String)> {
        let enabled = self.enabled.load(Ordering::Acquire);
        let mask = self.category_mask.load(Ordering::Acquire);
        let dropped = self.dropped_events.load(Ordering::Relaxed);
        let state = self.state.lock().unwrap();
        let (run_id, mode, categories, max_events, output_dir, include_args) =
            if let Some(config) = &state.config {
                (
                    config.run_id.clone(),
                    config.mode.clone(),
                    categories_from_mask(mask),
                    config.max_events,
                    config.output_dir.to_string_lossy().to_string(),
                    config.include_args,
                )
            } else {
                (
                    String::new(),
                    String::new(),
                    categories_from_mask(mask),
                    0,
                    String::new(),
                    true,
                )
            };
        vec![
            ("trace_enabled", bool_text(enabled)),
            ("trace_run_id", run_id),
            ("trace_mode", mode),
            ("trace_categories", categories),
            ("trace_max_events", max_events.to_string()),
            ("trace_buffered_events", state.events.len().to_string()),
            ("trace_dropped_events", dropped.to_string()),
            ("trace_output_dir", output_dir),
            ("trace_last_flush_dir", state.last_flush_dir.clone()),
            (
                "trace_last_flush_error",
                sanitize_kv(&state.last_flush_error),
            ),
            ("trace_flush_count", state.flush_count.to_string()),
            ("trace_include_args", bool_text(include_args)),
            ("trace_started_at_us", state.started_at_us.to_string()),
            ("trace_stopped_at_us", state.stopped_at_us.to_string()),
        ]
    }

    pub fn span(
        &self,
        category: TraceCategory,
        name: &'static str,
        args: Vec<(&'static str, String)>,
    ) -> TraceSpan {
        if !self.category_enabled(category) {
            return TraceSpan::inactive();
        }
        TraceSpan {
            active: true,
            category,
            name,
            started: Instant::now(),
            ts_us: now_us(),
            args: args
                .into_iter()
                .map(|(key, value)| (key.to_string(), value))
                .collect(),
        }
    }

    pub fn record_duration_us(
        &self,
        category: TraceCategory,
        name: &'static str,
        dur_us: f64,
        args: Vec<(&'static str, String)>,
    ) {
        if !self.category_enabled(category) {
            return;
        }
        self.record_event(TraceEvent {
            name: name.to_string(),
            category,
            ts_us: now_us(),
            dur_us,
            pid: std::process::id(),
            tid: numeric_thread_id(),
            args: args
                .into_iter()
                .map(|(key, value)| (key.to_string(), value))
                .collect(),
        });
    }

    fn category_enabled(&self, category: TraceCategory) -> bool {
        self.enabled.load(Ordering::Acquire)
            && (self.category_mask.load(Ordering::Acquire) & category.bit()) != 0
    }

    fn record_event(&self, event: TraceEvent) {
        let Ok(mut state) = self.state.try_lock() else {
            self.dropped_events.fetch_add(1, Ordering::Relaxed);
            return;
        };
        let Some(config) = &state.config else {
            self.dropped_events.fetch_add(1, Ordering::Relaxed);
            return;
        };
        if state.events.len() >= config.max_events {
            self.dropped_events.fetch_add(1, Ordering::Relaxed);
            return;
        }
        state.events.push_back(event);
    }
}

pub struct TraceSpan {
    active: bool,
    category: TraceCategory,
    name: &'static str,
    started: Instant,
    ts_us: u128,
    args: Vec<(String, String)>,
}

impl TraceSpan {
    fn inactive() -> Self {
        Self {
            active: false,
            category: TraceCategory::Counter,
            name: "",
            started: Instant::now(),
            ts_us: 0,
            args: Vec::new(),
        }
    }
}

impl Drop for TraceSpan {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        collector().record_event(TraceEvent {
            name: self.name.to_string(),
            category: self.category,
            ts_us: self.ts_us,
            dur_us: self.started.elapsed().as_secs_f64() * 1_000_000.0,
            pid: std::process::id(),
            tid: numeric_thread_id(),
            args: std::mem::take(&mut self.args),
        });
    }
}

fn write_trace_files(
    run_dir: &PathBuf,
    config: &TraceConfig,
    events: &[TraceEvent],
    dropped_events: u64,
) -> Result<(), String> {
    fs::create_dir_all(run_dir).map_err(|e| format!("create trace output dir failed: {}", e))?;
    fs::write(
        run_dir.join("trace_events.json"),
        chrome_trace_json(events, config.include_args),
    )
    .map_err(|e| format!("write trace_events.json failed: {}", e))?;
    fs::write(
        run_dir.join("trace_summary.json"),
        summary_json(config, events, dropped_events),
    )
    .map_err(|e| format!("write trace_summary.json failed: {}", e))?;
    fs::write(run_dir.join("trace_summary.csv"), summary_csv(events))
        .map_err(|e| format!("write trace_summary.csv failed: {}", e))?;
    fs::write(
        run_dir.join("summary.md"),
        summary_md(config, events, dropped_events),
    )
    .map_err(|e| format!("write summary.md failed: {}", e))?;
    Ok(())
}

fn chrome_trace_json(events: &[TraceEvent], include_args: bool) -> String {
    let mut out = String::from("{\"displayTimeUnit\":\"ms\",\"traceEvents\":[");
    for (idx, event) in events.iter().enumerate() {
        if idx != 0 {
            out.push(',');
        }
        out.push_str(&format!(
            "{{\"name\":\"{}\",\"cat\":\"{}\",\"ph\":\"X\",\"pid\":{},\"tid\":{},\"ts\":{},\"dur\":{:.3},\"args\":",
            json_escape(&event.name),
            event.category.chrome_cat(),
            event.pid,
            event.tid,
            event.ts_us,
            event.dur_us.max(0.0)
        ));
        if include_args {
            out.push_str(&args_json(&event.args));
        } else {
            out.push_str("{}");
        }
        out.push('}');
    }
    out.push_str("]}");
    out
}

fn summary_json(config: &TraceConfig, events: &[TraceEvent], dropped_events: u64) -> String {
    let rows = aggregate_rows(events);
    let total_dur = events.iter().map(|event| event.dur_us).sum::<f64>();
    let mut out = String::new();
    out.push_str("{\n");
    out.push_str(&format!(
        "  \"run_id\": \"{}\",\n",
        json_escape(&config.run_id)
    ));
    out.push_str(&format!("  \"mode\": \"{}\",\n", json_escape(&config.mode)));
    out.push_str(&format!(
        "  \"categories\": \"{}\",\n",
        json_escape(&categories_from_mask(config.category_mask()))
    ));
    out.push_str(&format!("  \"event_count\": {},\n", events.len()));
    out.push_str(&format!("  \"dropped_events\": {},\n", dropped_events));
    out.push_str(&format!("  \"total_duration_us\": {:.3},\n", total_dur));
    out.push_str("  \"stages\": [\n");
    for (idx, row) in rows.iter().enumerate() {
        if idx != 0 {
            out.push_str(",\n");
        }
        out.push_str(&format!(
            "    {{\"category\":\"{}\",\"name\":\"{}\",\"count\":{},\"total_us\":{:.3},\"avg_us\":{:.3},\"max_us\":{:.3},\"bytes\":{}}}",
            json_escape(&row.category),
            json_escape(&row.name),
            row.count,
            row.total_us,
            row.avg_us(),
            row.max_us,
            row.bytes
        ));
    }
    out.push_str("\n  ]\n");
    out.push_str("}\n");
    out
}

fn summary_csv(events: &[TraceEvent]) -> String {
    let mut out = String::from("category,name,count,total_us,avg_us,max_us,bytes\n");
    for row in aggregate_rows(events) {
        out.push_str(&format!(
            "{},{},{},{:.3},{:.3},{:.3},{}\n",
            csv_escape(&row.category),
            csv_escape(&row.name),
            row.count,
            row.total_us,
            row.avg_us(),
            row.max_us,
            row.bytes
        ));
    }
    out
}

fn summary_md(config: &TraceConfig, events: &[TraceEvent], dropped_events: u64) -> String {
    let mut out = String::new();
    out.push_str("# UFlow Trace Summary\n\n");
    out.push_str(&format!("- run_id: `{}`\n", config.run_id));
    out.push_str(&format!("- mode: `{}`\n", config.mode));
    out.push_str(&format!("- events: `{}`\n", events.len()));
    out.push_str(&format!("- dropped_events: `{}`\n\n", dropped_events));
    out.push_str("| category | stage | count | total us | avg us | max us | bytes |\n");
    out.push_str("|---|---:|---:|---:|---:|---:|---:|\n");
    for row in aggregate_rows(events).into_iter().take(40) {
        out.push_str(&format!(
            "| {} | {} | {} | {:.3} | {:.3} | {:.3} | {} |\n",
            row.category,
            row.name,
            row.count,
            row.total_us,
            row.avg_us(),
            row.max_us,
            row.bytes
        ));
    }
    out
}

#[derive(Default)]
struct AggregateRow {
    category: String,
    name: String,
    count: u64,
    total_us: f64,
    max_us: f64,
    bytes: u64,
}

impl AggregateRow {
    fn avg_us(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.total_us / self.count as f64
        }
    }
}

fn aggregate_rows(events: &[TraceEvent]) -> Vec<AggregateRow> {
    let mut map: HashMap<(String, String), AggregateRow> = HashMap::new();
    for event in events {
        let key = (event.category.as_str().to_string(), event.name.clone());
        let row = map.entry(key.clone()).or_insert_with(|| AggregateRow {
            category: key.0,
            name: key.1,
            ..AggregateRow::default()
        });
        row.count += 1;
        row.total_us += event.dur_us;
        row.max_us = row.max_us.max(event.dur_us);
        row.bytes = row.bytes.saturating_add(arg_u64(&event.args, "bytes"));
    }
    let mut rows = map.into_values().collect::<Vec<_>>();
    rows.sort_by(|a, b| {
        b.total_us
            .partial_cmp(&a.total_us)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.name.cmp(&b.name))
    });
    rows
}

fn mask_from_mode(mode: &str) -> u64 {
    match mode {
        "summary" => {
            TraceCategory::Control.bit()
                | TraceCategory::Object.bit()
                | TraceCategory::Transfer.bit()
                | TraceCategory::Channel.bit()
                | TraceCategory::Backend.bit()
                | TraceCategory::Counter.bit()
        }
        "full" => mask_from_categories("control,object,transfer,channel,chunk,acl,backend,counter"),
        _ => {
            TraceCategory::Control.bit()
                | TraceCategory::Transfer.bit()
                | TraceCategory::Channel.bit()
                | TraceCategory::Chunk.bit()
                | TraceCategory::Acl.bit()
                | TraceCategory::Backend.bit()
        }
    }
}

fn mask_from_categories(categories: &str) -> u64 {
    categories
        .split(',')
        .filter_map(TraceCategory::from_str)
        .fold(0u64, |mask, category| mask | category.bit())
}

fn categories_from_mask(mask: u64) -> String {
    ALL_CATEGORIES
        .iter()
        .copied()
        .filter(|category| (mask & category.bit()) != 0)
        .map(TraceCategory::as_str)
        .collect::<Vec<_>>()
        .join(",")
}

fn args_json(args: &[(String, String)]) -> String {
    let mut out = String::from("{");
    for (idx, (key, value)) in args.iter().enumerate() {
        if idx != 0 {
            out.push(',');
        }
        out.push_str(&format!(
            "\"{}\":\"{}\"",
            json_escape(key),
            json_escape(value)
        ));
    }
    out.push('}');
    out
}

fn arg_u64(args: &[(String, String)], key: &str) -> u64 {
    args.iter()
        .find(|(name, _)| name == key)
        .and_then(|(_, value)| value.parse::<u64>().ok())
        .unwrap_or(0)
}

fn default_run_id() -> String {
    format!("uflow_trace_{}", now_us())
}

fn now_us() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
}

fn numeric_thread_id() -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    thread::current().id().hash(&mut hasher);
    hasher.finish()
}

fn env_bool(key: &str, default: bool) -> bool {
    match env::var(key) {
        Ok(value) => matches!(value.as_str(), "1" | "true" | "True" | "yes" | "YES" | "on"),
        Err(_) => default,
    }
}

fn bool_text(value: bool) -> String {
    if value {
        "true".to_string()
    } else {
        "false".to_string()
    }
}

fn value_from_payload(payload: &[(&'static str, String)], key: &str) -> String {
    payload
        .iter()
        .find(|(name, _)| *name == key)
        .map(|(_, value)| value.clone())
        .unwrap_or_default()
}

fn sanitize_path_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn sanitize_kv(value: &str) -> String {
    value.replace(';', ",").replace('=', ":").replace('\n', " ")
}

fn json_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}
