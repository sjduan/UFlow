use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_MAX_EVENTS: usize = 200_000;
const DEFAULT_OUTPUT_DIR: &str = "/tmp/uflow_traces";
const TRACE_SCHEMA_VERSION: u64 = 2;

static ATOMIC_WRITE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static ACTIVE_TRANSFER_CONTEXT: RefCell<Option<TransferTraceContext>> = const { RefCell::new(None) };
}

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TracePhase {
    Complete,
    Instant,
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
    phase: TracePhase,
}

#[derive(Clone, Debug, Default)]
pub struct TransferTraceContext {
    pub plan_id: u64,
    pub event_id: Option<u64>,
    pub src_object_id: u64,
    pub dst_object_id: u64,
    pub src_medium: String,
    pub dst_medium: String,
    pub direction: String,
    pub operation: String,
    pub requested_mode: String,
    pub bytes: u64,
    pub planned_path: String,
    pub planned_engine: String,
}

impl TransferTraceContext {
    pub fn args(&self) -> Vec<(&'static str, String)> {
        let mut args = vec![
            ("trace_schema_version", TRACE_SCHEMA_VERSION.to_string()),
            ("plan_id", self.plan_id.to_string()),
            ("src_object_id", self.src_object_id.to_string()),
            ("dst_object_id", self.dst_object_id.to_string()),
            ("src_medium", self.src_medium.clone()),
            ("dst_medium", self.dst_medium.clone()),
            ("direction", self.direction.clone()),
            ("operation", self.operation.clone()),
            ("requested_mode", self.requested_mode.clone()),
            ("bytes", self.bytes.to_string()),
            ("planned_path", self.planned_path.clone()),
            ("planned_engine", self.planned_engine.clone()),
        ];
        if let Some(event_id) = self.event_id {
            args.insert(2, ("event_id", event_id.to_string()));
        }
        args
    }
}

pub fn transfer_direction(src_medium: &str, dst_medium: &str) -> String {
    match (src_medium, dst_medium) {
        ("ddr" | "ssd", "hbm") => "h2d".to_string(),
        ("hbm", "ddr" | "ssd") => "d2h".to_string(),
        _ => format!("{}_to_{}", src_medium, dst_medium),
    }
}

pub struct TransferTraceScope {
    previous: Option<TransferTraceContext>,
    active: bool,
}

impl TransferTraceScope {
    fn inactive() -> Self {
        Self {
            previous: None,
            active: false,
        }
    }
}

impl Drop for TransferTraceScope {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let previous = self.previous.take();
        ACTIVE_TRANSFER_CONTEXT.with(|slot| {
            slot.replace(previous);
        });
    }
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
    plan_contexts: HashMap<u64, TransferTraceContext>,
    event_contexts: HashMap<u64, TransferTraceContext>,
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

pub fn is_enabled() -> bool {
    collector().enabled.load(Ordering::Acquire)
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

pub fn record_instant(
    category: TraceCategory,
    name: &'static str,
    args: Vec<(&'static str, String)>,
) {
    collector().record_instant(category, name, args);
}

pub fn register_plan_context(context: &TransferTraceContext) {
    collector().register_context(context);
}

pub fn register_event_context(context: &TransferTraceContext) {
    collector().register_context(context);
}

pub fn enter_transfer_context(context: &TransferTraceContext) -> TransferTraceScope {
    if !collector().enabled.load(Ordering::Acquire) {
        return TransferTraceScope::inactive();
    }
    let previous = ACTIVE_TRANSFER_CONTEXT.with(|slot| slot.replace(Some(context.clone())));
    TransferTraceScope {
        previous,
        active: true,
    }
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
        let mut state = self
            .state
            .lock()
            .map_err(|_| "trace state lock poisoned".to_string())?;
        state.events.clear();
        state.started_at_us = now_us();
        state.stopped_at_us = 0;
        state.flush_count = 0;
        state.last_flush_dir.clear();
        state.last_flush_error.clear();
        state.plan_contexts.clear();
        state.event_contexts.clear();
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
        state.plan_contexts.clear();
        state.event_contexts.clear();
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
        let (config, events) = {
            let state = self.state.lock().unwrap();
            let Some(config) = state.config.clone() else {
                return TraceFlushResult {
                    flushed: false,
                    error: "trace is not configured".to_string(),
                    ..TraceFlushResult::default()
                };
            };
            let events = state.events.iter().cloned().collect::<Vec<_>>();
            (config, events)
        };
        let dropped = self.dropped_events.load(Ordering::Relaxed);
        let run_dir = config.output_dir.join(&config.run_id);
        let result = write_trace_files(&run_dir, &config, &events, dropped);
        let mut state = self.state.lock().unwrap();
        let same_run = state
            .config
            .as_ref()
            .map(|current| {
                current.run_id == config.run_id && current.output_dir == config.output_dir
            })
            .unwrap_or(false);
        match result {
            Ok(()) => {
                if same_run {
                    state.flush_count += 1;
                    state.last_flush_dir = run_dir.to_string_lossy().to_string();
                    state.last_flush_error.clear();
                }
                TraceFlushResult {
                    flushed: true,
                    run_id: config.run_id,
                    output_dir: run_dir.to_string_lossy().to_string(),
                    event_count: events.len(),
                    dropped_events: dropped,
                    error: String::new(),
                }
            }
            Err(error) => {
                if same_run {
                    state.last_flush_error = error.clone();
                }
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
            phase: TracePhase::Complete,
        });
    }

    pub fn record_instant(
        &self,
        category: TraceCategory,
        name: &'static str,
        args: Vec<(&'static str, String)>,
    ) {
        if !self.category_enabled(category) {
            return;
        }
        self.record_event(TraceEvent {
            name: name.to_string(),
            category,
            ts_us: now_us(),
            dur_us: 0.0,
            pid: std::process::id(),
            tid: numeric_thread_id(),
            args: args
                .into_iter()
                .map(|(key, value)| (key.to_string(), value))
                .collect(),
            phase: TracePhase::Instant,
        });
    }

    fn register_context(&self, context: &TransferTraceContext) {
        if !self.enabled.load(Ordering::Acquire) || context.plan_id == 0 {
            return;
        }
        let Ok(mut state) = self.state.lock() else {
            self.dropped_events.fetch_add(1, Ordering::Relaxed);
            return;
        };
        if state.config.is_none() {
            return;
        }
        state.plan_contexts.insert(context.plan_id, context.clone());
        if let Some(event_id) = context.event_id {
            state.event_contexts.insert(event_id, context.clone());
        }
    }

    fn category_enabled(&self, category: TraceCategory) -> bool {
        self.enabled.load(Ordering::Acquire)
            && (self.category_mask.load(Ordering::Acquire) & category.bit()) != 0
    }

    fn record_event(&self, mut event: TraceEvent) {
        let Ok(mut state) = self.state.lock() else {
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
        enrich_transfer_event(&state, &mut event);
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
            phase: TracePhase::Complete,
        });
    }
}

fn enrich_transfer_event(state: &TraceState, event: &mut TraceEvent) {
    let active = ACTIVE_TRANSFER_CONTEXT.with(|slot| slot.borrow().clone());
    let event_id = arg_optional_u64(&event.args, "event_id");
    let plan_id = arg_optional_u64(&event.args, "plan_id");
    let context = active
        .or_else(|| event_id.and_then(|id| state.event_contexts.get(&id).cloned()))
        .or_else(|| plan_id.and_then(|id| state.plan_contexts.get(&id).cloned()));

    if let Some(context) = context {
        for (key, value) in context.args() {
            if key == "bytes" {
                normalize_transfer_bytes(&mut event.args, &value);
            } else {
                push_arg_if_missing(&mut event.args, key, value);
            }
        }
    }
    if event.name.starts_with("transfer.")
        || event.name.starts_with("channel.")
        || event.name.starts_with("direct_lane.")
        || event.name.starts_with("chunk.")
        || event.name.starts_with("relay.")
        || event.name.starts_with("ssd.")
        || event.name.starts_with("ssd_hbm.")
        || event.name.starts_with("direct_h2d.")
        || event.name.starts_with("direct_d2h.")
        || event.name.starts_with("acl_copy.")
    {
        push_arg_if_missing(
            &mut event.args,
            "trace_schema_version",
            TRACE_SCHEMA_VERSION.to_string(),
        );
        if let Some(role) = stage_role_for_name(&event.name) {
            push_arg_if_missing(&mut event.args, "stage_role", role.to_string());
        }
    }
}

fn normalize_transfer_bytes(args: &mut Vec<(String, String)>, transfer_bytes: &str) {
    if let Some((_, value)) = args.iter_mut().find(|(name, _)| name == "bytes") {
        if value != transfer_bytes {
            let stage_bytes = value.clone();
            *value = transfer_bytes.to_string();
            push_arg_if_missing(args, "stage_bytes", stage_bytes);
        }
    } else {
        args.push(("bytes".to_string(), transfer_bytes.to_string()));
    }
}

fn push_arg_if_missing(args: &mut Vec<(String, String)>, key: &str, value: String) {
    if !args.iter().any(|(name, _)| name == key) {
        args.push((key.to_string(), value));
    }
}

fn stage_role_for_name(name: &str) -> Option<&'static str> {
    if name == "transfer.outcome" {
        Some("outcome")
    } else if name == "transfer.plan_outcome" {
        Some("plan_outcome")
    } else if name == "transfer.queue_wait" {
        Some("queue_wait")
    } else if name == "transfer.worker_execute" {
        Some("worker_execute")
    } else if name.ends_with("lane_busy") || name.ends_with(".busy") {
        Some("lane_busy")
    } else if name.ends_with(".acquire") {
        Some("lane_acquire")
    } else if name == "direct_lane.create" {
        Some("lane_acquire")
    } else if name.ends_with("lane_create") || name.ends_with("lane.create") {
        Some("lane_create")
    } else if name.ends_with(".release") {
        Some("lane_release")
    } else if name.ends_with(".reap") {
        Some("lane_reap")
    } else if name.contains("cpu_copy") || name == "transfer.copy.ddr_to_ddr" {
        Some("cpu_copy")
    } else if name.contains("acl_submit") {
        Some("acl_submit")
    } else if name.contains("acl_wait") {
        Some("acl_wait")
    } else if name.contains("acl_sync") || name.starts_with("acl_copy.") {
        Some("acl_copy")
    } else if name.starts_with("relay.") {
        Some("relay_stage")
    } else if name.starts_with("ssd.read")
        || name.starts_with("ssd.write")
        || matches!(
            name,
            "transfer.copy.ssd_to_ddr" | "transfer.copy.ddr_to_ssd"
        )
    {
        Some("ssd_io")
    } else if name.starts_with("ssd_hbm.direct_") || name.starts_with("direct_d2h.register") {
        Some("direct_setup")
    } else if name == "transfer.submit" || name == "transfer.enqueue" {
        Some("submit")
    } else if name == "transfer.execute" {
        Some("execute")
    } else if name == "transfer.complete" {
        Some("complete")
    } else if name == "transfer.wait_client" {
        Some("wait_client")
    } else {
        None
    }
}

fn write_trace_files(
    run_dir: &PathBuf,
    config: &TraceConfig,
    events: &[TraceEvent],
    dropped_events: u64,
) -> Result<(), String> {
    fs::create_dir_all(run_dir).map_err(|e| format!("create trace output dir failed: {}", e))?;
    atomic_write(
        &run_dir.join("trace_events.json"),
        &chrome_trace_json(events, config.include_args),
    )?;
    atomic_write(
        &run_dir.join("trace_summary.json"),
        &summary_json(config, events, dropped_events),
    )?;
    atomic_write(&run_dir.join("trace_summary.csv"), &summary_csv(events))?;
    atomic_write(
        &run_dir.join("transfer_summary.csv"),
        &transfer_summary_csv(events),
    )?;
    atomic_write(
        &run_dir.join("summary.md"),
        &summary_md(config, events, dropped_events),
    )?;
    Ok(())
}

fn atomic_write(path: &Path, contents: &str) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| format!("trace artifact has no parent: {}", path.display()))?;
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("trace_artifact");
    let sequence = ATOMIC_WRITE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temp = parent.join(format!(".{}.{}.{}.tmp", name, std::process::id(), sequence));
    fs::write(&temp, contents)
        .map_err(|e| format!("write temporary {} failed: {}", path.display(), e))?;
    if let Err(error) = fs::rename(&temp, path) {
        let _ = fs::remove_file(&temp);
        return Err(format!(
            "publish trace artifact {} failed: {}",
            path.display(),
            error
        ));
    }
    Ok(())
}

fn chrome_trace_json(events: &[TraceEvent], include_args: bool) -> String {
    let mut out = format!(
        "{{\"displayTimeUnit\":\"ms\",\"uflowTraceSchemaVersion\":{},\"traceEvents\":[",
        TRACE_SCHEMA_VERSION
    );
    let mut ordered = events.iter().collect::<Vec<_>>();
    ordered.sort_by(|a, b| a.ts_us.cmp(&b.ts_us).then_with(|| a.tid.cmp(&b.tid)));
    for (idx, event) in ordered.into_iter().enumerate() {
        if idx != 0 {
            out.push(',');
        }
        match event.phase {
            TracePhase::Complete => out.push_str(&format!(
                "{{\"name\":\"{}\",\"cat\":\"{}\",\"ph\":\"X\",\"pid\":{},\"tid\":{},\"ts\":{},\"dur\":{:.3},\"args\":",
                json_escape(&event.name),
                event.category.chrome_cat(),
                event.pid,
                event.tid,
                event.ts_us,
                event.dur_us.max(0.0)
            )),
            TracePhase::Instant => out.push_str(&format!(
                "{{\"name\":\"{}\",\"cat\":\"{}\",\"ph\":\"i\",\"s\":\"t\",\"pid\":{},\"tid\":{},\"ts\":{},\"args\":",
                json_escape(&event.name),
                event.category.chrome_cat(),
                event.pid,
                event.tid,
                event.ts_us
            )),
        }
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
    let transfers = aggregate_transfers(events);
    let incomplete = transfers
        .iter()
        .filter(|row| row.outcome_count == 0)
        .map(|row| row.event_id)
        .collect::<Vec<_>>();
    let duplicate = transfers
        .iter()
        .filter(|row| row.outcome_count > 1)
        .map(|row| row.event_id)
        .collect::<Vec<_>>();
    let context_mismatch = transfers
        .iter()
        .filter(|row| !row.context_mismatches.is_empty() || !row.schema_valid)
        .map(|row| row.event_id)
        .collect::<Vec<_>>();
    let path_rows = aggregate_paths(&transfers);
    let complete = dropped_events == 0
        && incomplete.is_empty()
        && duplicate.is_empty()
        && context_mismatch.is_empty();
    let total_dur = events.iter().map(|event| event.dur_us).sum::<f64>();
    let mut out = String::new();
    out.push_str("{\n");
    out.push_str(&format!(
        "  \"schema_version\": {},\n",
        TRACE_SCHEMA_VERSION
    ));
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
    out.push_str(&format!("  \"complete\": {},\n", complete));
    out.push_str(&format!(
        "  \"raw_args_exported\": {},\n",
        config.include_args
    ));
    out.push_str(&format!(
        "  \"incomplete_event_ids\": {},\n",
        json_u64_array(&incomplete)
    ));
    out.push_str(&format!(
        "  \"duplicate_outcome_event_ids\": {},\n",
        json_u64_array(&duplicate)
    ));
    out.push_str(&format!(
        "  \"context_mismatch_event_ids\": {},\n",
        json_u64_array(&context_mismatch)
    ));
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
    out.push_str("\n  ],\n");
    out.push_str("  \"transfers\": [\n");
    for (idx, transfer) in transfers.iter().enumerate() {
        if idx != 0 {
            out.push_str(",\n");
        }
        out.push_str("    ");
        out.push_str(&transfer_json(transfer));
    }
    out.push_str("\n  ],\n");
    out.push_str("  \"path_summary\": [\n");
    for (idx, path) in path_rows.iter().enumerate() {
        if idx != 0 {
            out.push_str(",\n");
        }
        out.push_str("    ");
        out.push_str(&path_summary_json(path));
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

fn transfer_summary_csv(events: &[TraceEvent]) -> String {
    let transfers = aggregate_transfers(events);
    let mut out = TRANSFER_CSV_COLUMNS.join(",");
    out.push('\n');
    for transfer in &transfers {
        for (idx, column) in TRANSFER_CSV_COLUMNS.iter().enumerate() {
            if idx != 0 {
                out.push(',');
            }
            out.push_str(&csv_escape(&transfer_column_value(transfer, column)));
        }
        out.push('\n');
    }
    out
}

fn summary_md(config: &TraceConfig, events: &[TraceEvent], dropped_events: u64) -> String {
    let transfers = aggregate_transfers(events);
    let incomplete = transfers
        .iter()
        .filter(|row| row.outcome_count == 0)
        .map(|row| row.event_id.to_string())
        .collect::<Vec<_>>();
    let invalid = transfers
        .iter()
        .filter(|row| {
            row.outcome_count > 1 || !row.context_mismatches.is_empty() || !row.schema_valid
        })
        .map(|row| row.event_id.to_string())
        .collect::<Vec<_>>();
    let complete = dropped_events == 0 && incomplete.is_empty() && invalid.is_empty();
    let mut out = String::new();
    out.push_str("# UFlow Trace Summary\n\n");
    out.push_str(&format!("- schema_version: `{}`\n", TRACE_SCHEMA_VERSION));
    out.push_str(&format!("- run_id: `{}`\n", config.run_id));
    out.push_str(&format!("- mode: `{}`\n", config.mode));
    out.push_str(&format!("- events: `{}`\n", events.len()));
    out.push_str(&format!("- transfers: `{}`\n", transfers.len()));
    out.push_str(&format!("- complete: `{}`\n", complete));
    out.push_str(&format!("- raw_args_exported: `{}`\n", config.include_args));
    out.push_str(&format!("- dropped_events: `{}`\n", dropped_events));
    out.push_str(&format!(
        "- incomplete_event_ids: `{}`\n\n",
        if incomplete.is_empty() {
            "none".to_string()
        } else {
            incomplete.join(",")
        }
    ));
    out.push_str(&format!(
        "- invalid_or_duplicate_event_ids: `{}`\n\n",
        if invalid.is_empty() {
            "none".to_string()
        } else {
            invalid.join(",")
        }
    ));
    out.push_str("## Transfers\n\n");
    out.push_str("| event | direction | planned path | actual path | engine | status | bytes | latency us | fallback | span sum us |\n");
    out.push_str("|---:|---|---|---|---|---|---:|---:|---|---:|\n");
    for transfer in transfers.iter().take(100) {
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {:.3} |\n",
            transfer.event_id,
            md_escape(transfer_value(transfer, "direction")),
            md_escape(transfer_value(transfer, "planned_path")),
            md_escape(transfer_value(transfer, "actual_path")),
            md_escape(transfer_value(transfer, "actual_engine")),
            md_escape(transfer_value(transfer, "status")),
            transfer_value(transfer, "bytes_done"),
            transfer_value(transfer, "actual_latency_us"),
            transfer_value(transfer, "fallback_used"),
            transfer.sum_of_span_durations_us
        ));
    }
    out.push_str("\n## Stage compatibility summary\n\n");
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

const TRANSFER_CONTEXT_KEYS: [&str; 12] = [
    "trace_schema_version",
    "plan_id",
    "src_object_id",
    "dst_object_id",
    "src_medium",
    "dst_medium",
    "direction",
    "operation",
    "requested_mode",
    "bytes",
    "planned_path",
    "planned_engine",
];

const TRANSFER_STRING_KEYS: [&str; 16] = [
    "src_medium",
    "dst_medium",
    "direction",
    "operation",
    "requested_mode",
    "planned_path",
    "planned_engine",
    "status",
    "actual_path",
    "actual_engine",
    "fallback_reason",
    "error_code",
    "error_message",
    "channel_direction",
    "direct_candidate",
    "direct_kind",
];

const TRANSFER_U64_KEYS: [&str; 19] = [
    "plan_id",
    "src_object_id",
    "dst_object_id",
    "bytes",
    "bytes_done",
    "channel_lane_id",
    "channel_device_id",
    "channel_chunk_bytes",
    "channel_chunk_count",
    "channel_pinned_footprint_bytes",
    "channel_chunks_transferred",
    "channel_stream_create_count",
    "channel_event_reuse_count",
    "channel_event_record_count",
    "channel_event_wait_count",
    "ssd_io_bytes",
    "ssd_read_bytes",
    "ssd_write_bytes",
    "relay_stage_count",
];

const TRANSFER_F64_KEYS: [&str; 24] = [
    "actual_latency_us",
    "actual_bandwidth_gib_s",
    "channel_lane_wait_us",
    "channel_cpu_copy_us",
    "channel_acl_copy_us",
    "channel_acl_submit_us",
    "channel_acl_wait_us",
    "channel_wall_us",
    "channel_queue_wait_us",
    "channel_worker_execute_us",
    "channel_overlap_ratio",
    "ssd_io_submit_us",
    "ssd_io_wait_us",
    "ssd_io_bandwidth_gib_s",
    "relay_ddr_hbm_us",
    "relay_total_us",
    "direct_setup_us",
    "direct_register_us",
    "direct_fadvise_us",
    "direct_readahead_us",
    "direct_madvise_hugepage_us",
    "direct_madvise_willneed_us",
    "direct_madvise_populate_us",
    "direct_pretouch_us",
];

const TRANSFER_EXTRA_F64_KEYS: [&str; 3] = ["direct_mlock_us", "direct_acl_us", "direct_total_us"];

const TRANSFER_BOOL_KEYS: [&str; 2] = ["fallback_used", "channel_pipeline_overlap"];

const TRANSFER_CSV_COLUMNS: &[&str] = &[
    "event_id",
    "schema_version",
    "plan_id",
    "src_object_id",
    "dst_object_id",
    "src_medium",
    "dst_medium",
    "direction",
    "operation",
    "requested_mode",
    "bytes",
    "planned_path",
    "planned_engine",
    "status",
    "bytes_done",
    "actual_path",
    "actual_engine",
    "actual_latency_us",
    "actual_bandwidth_gib_s",
    "fallback_used",
    "fallback_reason",
    "error_code",
    "error_message",
    "channel_direction",
    "channel_lane_id",
    "channel_device_id",
    "channel_chunk_bytes",
    "channel_chunk_count",
    "channel_pinned_footprint_bytes",
    "channel_lane_wait_us",
    "channel_chunks_transferred",
    "channel_cpu_copy_us",
    "channel_acl_copy_us",
    "channel_acl_submit_us",
    "channel_acl_wait_us",
    "channel_wall_us",
    "channel_queue_wait_us",
    "channel_worker_execute_us",
    "channel_overlap_ratio",
    "channel_stream_create_count",
    "channel_event_reuse_count",
    "channel_event_record_count",
    "channel_event_wait_count",
    "channel_pipeline_overlap",
    "ssd_io_submit_us",
    "ssd_io_wait_us",
    "ssd_io_bytes",
    "ssd_io_bandwidth_gib_s",
    "ssd_read_bytes",
    "ssd_write_bytes",
    "relay_stage_count",
    "relay_ddr_hbm_us",
    "relay_total_us",
    "direct_candidate",
    "direct_kind",
    "direct_setup_us",
    "direct_register_us",
    "direct_fadvise_us",
    "direct_readahead_us",
    "direct_madvise_hugepage_us",
    "direct_madvise_willneed_us",
    "direct_madvise_populate_us",
    "direct_pretouch_us",
    "direct_mlock_us",
    "direct_acl_us",
    "direct_total_us",
    "stage_queue_wait_us",
    "stage_worker_execute_us",
    "stage_lane_acquire_us",
    "stage_lane_busy_us",
    "stage_lane_release_us",
    "stage_cpu_copy_us",
    "stage_acl_submit_us",
    "stage_acl_wait_us",
    "stage_acl_copy_us",
    "stage_ssd_io_us",
    "stage_relay_stage_us",
    "stage_direct_setup_us",
    "sum_of_span_durations_us",
    "span_minus_actual_latency_us",
    "outcome_count",
    "context_consistent",
];

#[derive(Default)]
struct TransferRow {
    event_id: u64,
    values: HashMap<String, String>,
    stage_durations_us: HashMap<String, f64>,
    sum_of_span_durations_us: f64,
    outcome_count: u64,
    context_mismatches: HashSet<String>,
    schema_valid: bool,
}

fn aggregate_transfers(events: &[TraceEvent]) -> Vec<TransferRow> {
    let mut rows: HashMap<u64, TransferRow> = HashMap::new();
    for event in events {
        let Some(event_id) = arg_optional_u64(&event.args, "event_id") else {
            continue;
        };
        if event_id == 0 {
            continue;
        }
        let row = rows.entry(event_id).or_insert_with(|| TransferRow {
            event_id,
            schema_valid: true,
            ..TransferRow::default()
        });
        for (key, value) in &event.args {
            if TRANSFER_CONTEXT_KEYS.contains(&key.as_str()) {
                if let Some(existing) = row.values.get(key) {
                    if existing != value {
                        row.context_mismatches.insert(key.clone());
                    }
                } else {
                    row.values.insert(key.clone(), value.clone());
                }
            } else if event.name == "transfer.outcome" {
                row.values.insert(key.clone(), value.clone());
            }
        }
        if let Some(version) = arg_value(&event.args, "trace_schema_version") {
            if version != TRACE_SCHEMA_VERSION.to_string() {
                row.schema_valid = false;
            }
        } else {
            row.schema_valid = false;
        }
        if event.name == "transfer.outcome" {
            row.outcome_count += 1;
        }
        if event.phase == TracePhase::Complete {
            row.sum_of_span_durations_us += event.dur_us.max(0.0);
            if let Some(role) = arg_value(&event.args, "stage_role") {
                *row.stage_durations_us.entry(role.to_string()).or_default() +=
                    event.dur_us.max(0.0);
            }
        }
    }
    let mut rows = rows.into_values().collect::<Vec<_>>();
    rows.sort_by_key(|row| row.event_id);
    rows
}

fn transfer_json(row: &TransferRow) -> String {
    let mut out = String::from("{");
    let mut first = true;
    push_json_number(
        &mut out,
        &mut first,
        "event_id",
        Some(row.event_id.to_string()),
    );
    push_json_number(
        &mut out,
        &mut first,
        "schema_version",
        Some(TRACE_SCHEMA_VERSION.to_string()),
    );
    for key in TRANSFER_U64_KEYS {
        push_json_number(&mut out, &mut first, key, row.values.get(key).cloned());
    }
    for key in TRANSFER_F64_KEYS
        .iter()
        .chain(TRANSFER_EXTRA_F64_KEYS.iter())
    {
        push_json_number(&mut out, &mut first, key, row.values.get(*key).cloned());
    }
    for key in TRANSFER_BOOL_KEYS {
        push_json_bool(&mut out, &mut first, key, row.values.get(key).cloned());
    }
    for key in TRANSFER_STRING_KEYS {
        push_json_string(&mut out, &mut first, key, row.values.get(key));
    }
    push_json_number(
        &mut out,
        &mut first,
        "sum_of_span_durations_us",
        Some(format!("{:.3}", row.sum_of_span_durations_us)),
    );
    let delta = row
        .values
        .get("actual_latency_us")
        .and_then(|value| value.parse::<f64>().ok())
        .map(|latency| format!("{:.3}", row.sum_of_span_durations_us - latency));
    push_json_number(&mut out, &mut first, "span_minus_actual_latency_us", delta);
    push_json_number(
        &mut out,
        &mut first,
        "outcome_count",
        Some(row.outcome_count.to_string()),
    );
    push_json_bool(
        &mut out,
        &mut first,
        "context_consistent",
        Some((row.context_mismatches.is_empty() && row.schema_valid).to_string()),
    );
    if !first {
        out.push(',');
    }
    out.push_str("\"stage_durations_us\":{");
    let mut stages = row.stage_durations_us.iter().collect::<Vec<_>>();
    stages.sort_by(|a, b| a.0.cmp(b.0));
    for (idx, (role, duration)) in stages.iter().enumerate() {
        if idx != 0 {
            out.push(',');
        }
        out.push_str(&format!("\"{}\":{:.3}", json_escape(role), duration));
    }
    out.push_str("}}");
    out
}

fn push_json_separator(out: &mut String, first: &mut bool) {
    if *first {
        *first = false;
    } else {
        out.push(',');
    }
}

fn push_json_number(out: &mut String, first: &mut bool, key: &str, value: Option<String>) {
    push_json_separator(out, first);
    out.push_str(&format!("\"{}\":", json_escape(key)));
    match value.filter(|value| value.parse::<f64>().map(|v| v.is_finite()).unwrap_or(false)) {
        Some(value) => out.push_str(&value),
        None => out.push_str("null"),
    }
}

fn push_json_bool(out: &mut String, first: &mut bool, key: &str, value: Option<String>) {
    push_json_separator(out, first);
    out.push_str(&format!("\"{}\":", json_escape(key)));
    match value.as_deref() {
        Some("true") => out.push_str("true"),
        Some("false") => out.push_str("false"),
        _ => out.push_str("null"),
    }
}

fn push_json_string(out: &mut String, first: &mut bool, key: &str, value: Option<&String>) {
    push_json_separator(out, first);
    out.push_str(&format!("\"{}\":", json_escape(key)));
    match value {
        Some(value) => out.push_str(&format!("\"{}\"", json_escape(value))),
        None => out.push_str("null"),
    }
}

#[derive(Default)]
struct PathSummaryRow {
    direction: String,
    actual_path: String,
    actual_engine: String,
    status: String,
    count: u64,
    bytes: u64,
    fallback_count: u64,
    latencies_us: Vec<f64>,
    bandwidths_gib_s: Vec<f64>,
}

fn aggregate_paths(transfers: &[TransferRow]) -> Vec<PathSummaryRow> {
    let mut paths: HashMap<(String, String, String, String), PathSummaryRow> = HashMap::new();
    for transfer in transfers.iter().filter(|row| row.outcome_count == 1) {
        let key = (
            transfer_value(transfer, "direction").to_string(),
            transfer_value(transfer, "actual_path").to_string(),
            transfer_value(transfer, "actual_engine").to_string(),
            transfer_value(transfer, "status").to_string(),
        );
        let row = paths.entry(key.clone()).or_insert_with(|| PathSummaryRow {
            direction: key.0,
            actual_path: key.1,
            actual_engine: key.2,
            status: key.3,
            ..PathSummaryRow::default()
        });
        row.count += 1;
        row.bytes = row
            .bytes
            .saturating_add(parse_transfer_u64(transfer, "bytes_done"));
        if transfer_value(transfer, "fallback_used") == "true" {
            row.fallback_count += 1;
        }
        if let Some(value) = parse_transfer_f64(transfer, "actual_latency_us") {
            row.latencies_us.push(value);
        }
        if let Some(value) = parse_transfer_f64(transfer, "actual_bandwidth_gib_s") {
            row.bandwidths_gib_s.push(value);
        }
    }
    let mut rows = paths.into_values().collect::<Vec<_>>();
    rows.sort_by(|a, b| {
        a.direction
            .cmp(&b.direction)
            .then_with(|| a.actual_path.cmp(&b.actual_path))
            .then_with(|| a.actual_engine.cmp(&b.actual_engine))
            .then_with(|| a.status.cmp(&b.status))
    });
    rows
}

fn path_summary_json(row: &PathSummaryRow) -> String {
    let latency_p50 = percentile(&row.latencies_us, 0.50);
    let latency_p95 = percentile(&row.latencies_us, 0.95);
    let bandwidth_p50 = percentile(&row.bandwidths_gib_s, 0.50);
    let bandwidth_p95 = percentile(&row.bandwidths_gib_s, 0.95);
    format!(
        "{{\"direction\":\"{}\",\"actual_path\":\"{}\",\"actual_engine\":\"{}\",\"status\":\"{}\",\"count\":{},\"bytes\":{},\"fallback_count\":{},\"latency_us_p50\":{},\"latency_us_p95\":{},\"bandwidth_gib_s_p50\":{},\"bandwidth_gib_s_p95\":{}}}",
        json_escape(&row.direction),
        json_escape(&row.actual_path),
        json_escape(&row.actual_engine),
        json_escape(&row.status),
        row.count,
        row.bytes,
        row.fallback_count,
        json_optional_f64(latency_p50),
        json_optional_f64(latency_p95),
        json_optional_f64(bandwidth_p50),
        json_optional_f64(bandwidth_p95)
    )
}

fn percentile(values: &[f64], quantile: f64) -> Option<f64> {
    let mut sorted = values
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    if sorted.is_empty() {
        return None;
    }
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let rank = (quantile * sorted.len() as f64).ceil().max(1.0) as usize - 1;
    sorted.get(rank.min(sorted.len() - 1)).copied()
}

fn json_optional_f64(value: Option<f64>) -> String {
    value
        .filter(|value| value.is_finite())
        .map(|value| format!("{:.6}", value))
        .unwrap_or_else(|| "null".to_string())
}

fn json_u64_array(values: &[u64]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn transfer_value<'a>(row: &'a TransferRow, key: &str) -> &'a str {
    row.values.get(key).map(String::as_str).unwrap_or("")
}

fn transfer_column_value(row: &TransferRow, column: &str) -> String {
    match column {
        "event_id" => row.event_id.to_string(),
        "schema_version" => TRACE_SCHEMA_VERSION.to_string(),
        "sum_of_span_durations_us" => format!("{:.3}", row.sum_of_span_durations_us),
        "span_minus_actual_latency_us" => parse_transfer_f64(row, "actual_latency_us")
            .map(|latency| format!("{:.3}", row.sum_of_span_durations_us - latency))
            .unwrap_or_default(),
        "outcome_count" => row.outcome_count.to_string(),
        "context_consistent" => (row.context_mismatches.is_empty() && row.schema_valid).to_string(),
        _ if column.starts_with("stage_") && column.ends_with("_us") => {
            let role = &column[6..column.len() - 3];
            row.stage_durations_us
                .get(role)
                .map(|value| format!("{:.3}", value))
                .unwrap_or_default()
        }
        _ => transfer_value(row, column).to_string(),
    }
}

fn parse_transfer_u64(row: &TransferRow, key: &str) -> u64 {
    transfer_value(row, key).parse::<u64>().unwrap_or(0)
}

fn parse_transfer_f64(row: &TransferRow, key: &str) -> Option<f64> {
    transfer_value(row, key)
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
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
        let event_bytes = arg_optional_u64(&event.args, "stage_bytes")
            .unwrap_or_else(|| arg_u64(&event.args, "bytes"));
        row.bytes = row.bytes.saturating_add(event_bytes);
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

fn arg_value<'a>(args: &'a [(String, String)], key: &str) -> Option<&'a str> {
    args.iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.as_str())
}

fn arg_optional_u64(args: &[(String, String)], key: &str) -> Option<u64> {
    arg_value(args, key).and_then(|value| value.parse::<u64>().ok())
}

fn arg_u64(args: &[(String, String)], key: &str) -> u64 {
    arg_optional_u64(args, key).unwrap_or(0)
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
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            '\u{08}' => escaped.push_str("\\b"),
            '\u{0c}' => escaped.push_str("\\f"),
            ch if ch <= '\u{1f}' => escaped.push_str(&format!("\\u{:04x}", ch as u32)),
            ch => escaped.push(ch),
        }
    }
    escaped
}

fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\r') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn md_escape(value: &str) -> String {
    value
        .replace('|', "\\|")
        .replace('\r', " ")
        .replace('\n', "<br>")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(output_dir: PathBuf, include_args: bool) -> TraceConfig {
        TraceConfig {
            run_id: "trace-v2-test".to_string(),
            mode: "full".to_string(),
            categories: String::new(),
            max_events: 64,
            output_dir,
            include_args,
        }
    }

    fn context(event_id: u64) -> TransferTraceContext {
        TransferTraceContext {
            plan_id: 7,
            event_id: Some(event_id),
            src_object_id: 11,
            dst_object_id: 12,
            src_medium: "ssd".to_string(),
            dst_medium: "hbm".to_string(),
            direction: "h2d".to_string(),
            operation: "copy".to_string(),
            requested_mode: "auto".to_string(),
            bytes: 4096,
            planned_path: "ssd_to_hbm_direct".to_string(),
            planned_engine: "ssd_hbm_auto_file_mmap_acl_direct".to_string(),
        }
    }

    fn event(name: &str, phase: TracePhase, dur_us: f64, args: Vec<(&str, String)>) -> TraceEvent {
        TraceEvent {
            name: name.to_string(),
            category: TraceCategory::Transfer,
            ts_us: 123,
            dur_us,
            pid: 1,
            tid: 2,
            args: args
                .into_iter()
                .map(|(key, value)| (key.to_string(), value))
                .collect(),
            phase,
        }
    }

    fn synthetic_events(event_id: u64) -> Vec<TraceEvent> {
        let context = context(event_id);
        let mut stage_args = context.args();
        stage_args.push(("stage_role", "direct_setup".to_string()));
        let mut outcome_args = context.args();
        outcome_args.extend([
            ("stage_role", "outcome".to_string()),
            ("status", "complete".to_string()),
            ("bytes_done", "4096".to_string()),
            ("actual_path", "ssd_to_hbm_via_ddr".to_string()),
            ("actual_engine", "ssd_hbm_relay_ddr".to_string()),
            ("actual_latency_us", "125.5".to_string()),
            ("actual_bandwidth_gib_s", "0.0304".to_string()),
            ("fallback_used", "true".to_string()),
            (
                "fallback_reason",
                "candidate failed retry relay".to_string(),
            ),
            ("error_code", String::new()),
            ("error_message", String::new()),
            ("direct_candidate", "file_mmap_acl_direct".to_string()),
            ("direct_kind", "logical".to_string()),
        ]);
        vec![
            event(
                "ssd_hbm.direct_pretouch",
                TracePhase::Complete,
                40.0,
                stage_args,
            ),
            event("transfer.outcome", TracePhase::Instant, 0.0, outcome_args),
        ]
    }

    #[test]
    fn enriches_transfer_context_and_stage_role() {
        let context = context(91);
        let mut state = TraceState::default();
        state.event_contexts.insert(91, context);
        let mut trace_event = event(
            "channel.acquire",
            TracePhase::Complete,
            3.0,
            vec![
                ("event_id", "91".to_string()),
                ("bytes", "1024".to_string()),
            ],
        );
        enrich_transfer_event(&state, &mut trace_event);
        assert_eq!(arg_value(&trace_event.args, "plan_id"), Some("7"));
        assert_eq!(arg_value(&trace_event.args, "bytes"), Some("4096"));
        assert_eq!(arg_value(&trace_event.args, "stage_bytes"), Some("1024"));
        assert_eq!(
            arg_value(&trace_event.args, "trace_schema_version"),
            Some("2")
        );
        assert_eq!(
            arg_value(&trace_event.args, "stage_role"),
            Some("lane_acquire")
        );
    }

    #[test]
    fn emits_real_instant_and_hides_raw_args_without_losing_summary() {
        let events = synthetic_events(101);
        let raw = chrome_trace_json(&events, false);
        assert!(raw.contains("\"uflowTraceSchemaVersion\":2"));
        assert!(raw.contains("\"ph\":\"i\""));
        assert!(raw.contains("\"args\":{}"));
        assert!(!raw.contains("candidate failed"));

        let summary = summary_json(&config(PathBuf::new(), false), &events, 0);
        assert!(summary.contains("\"raw_args_exported\": false"));
        assert!(summary.contains("\"actual_path\":\"ssd_to_hbm_via_ddr\""));
        assert!(summary.contains("\"complete\": true"));
        assert!(summary.contains("\"direct_setup\":40.000"));
    }

    #[test]
    fn marks_missing_duplicate_and_dropped_traces_incomplete() {
        let context = context(202);
        let mut args = context.args();
        args.push(("stage_role", "ssd_io".to_string()));
        let missing = vec![event("ssd.read_wait", TracePhase::Complete, 7.0, args)];
        let missing_summary = summary_json(&config(PathBuf::new(), true), &missing, 0);
        assert!(missing_summary.contains("\"complete\": false"));
        assert!(missing_summary.contains("\"incomplete_event_ids\": [202]"));

        let mut duplicate = synthetic_events(203);
        duplicate.push(duplicate[1].clone());
        let duplicate_summary = summary_json(&config(PathBuf::new(), true), &duplicate, 0);
        assert!(duplicate_summary.contains("\"duplicate_outcome_event_ids\": [203]"));
        assert!(duplicate_summary.contains("\"complete\": false"));

        let dropped_summary =
            summary_json(&config(PathBuf::new(), true), &synthetic_events(204), 1);
        assert!(dropped_summary.contains("\"dropped_events\": 1"));
        assert!(dropped_summary.contains("\"complete\": false"));
    }

    #[test]
    fn escapes_json_csv_and_writes_five_atomic_artifacts() {
        assert_eq!(json_escape("a\t\"b\\c\n"), "a\\t\\\"b\\\\c\\n");
        assert_eq!(csv_escape("a,\"b\"\n"), "\"a,\"\"b\"\"\n\"");

        let root = env::temp_dir().join(format!(
            "phasee06_trace_test_{}_{}",
            std::process::id(),
            now_us()
        ));
        let run_dir = root.join("trace-v2-test");
        let cfg = config(root.clone(), true);
        write_trace_files(&run_dir, &cfg, &synthetic_events(301), 0).unwrap();
        for name in [
            "trace_events.json",
            "trace_summary.json",
            "trace_summary.csv",
            "transfer_summary.csv",
            "summary.md",
        ] {
            assert!(run_dir.join(name).is_file(), "missing {}", name);
        }
        let leftovers = fs::read_dir(&run_dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(".tmp"))
            .count();
        assert_eq!(leftovers, 0);
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn atomic_write_failure_does_not_publish_or_leave_temp_file() {
        let root = env::temp_dir().join(format!(
            "phasee06_atomic_failure_{}_{}",
            std::process::id(),
            now_us()
        ));
        fs::create_dir_all(root.join("artifact.json")).unwrap();
        let result = atomic_write(&root.join("artifact.json"), "not published");
        assert!(result.is_err());
        assert!(root.join("artifact.json").is_dir());
        let leftovers = fs::read_dir(&root)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(".tmp"))
            .count();
        assert_eq!(leftovers, 0);
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn transfer_csv_has_stable_columns_and_nullable_fields() {
        let csv = transfer_summary_csv(&synthetic_events(401));
        let lines = csv.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0].split(',').count(), lines[1].split(',').count());
        assert!(lines[0].contains("stage_direct_setup_us"));
        assert!(lines[1].contains("candidate failed"));

        let json = summary_json(&config(PathBuf::new(), true), &synthetic_events(401), 0);
        assert!(json.contains("\"channel_lane_id\":null"));
        assert!(json.contains("\"direct_kind\":\"logical\""));
    }
}
