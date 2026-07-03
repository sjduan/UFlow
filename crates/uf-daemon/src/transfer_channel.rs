use crate::acl_backend::{
    create_event, create_stream, malloc_pinned_host, AclEvent, AclStream, PinnedHostAllocation,
};
use crate::trace::{record_duration_us, span, TraceCategory};

use std::env;
use std::sync::{Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

const DEFAULT_H2D_CHUNK_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_D2H_CHUNK_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_CHUNK_COUNT: usize = 2;
const DEFAULT_IDLE_TTL_MS: u64 = 30_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferDirection {
    H2D,
    D2H,
}

impl TransferDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            TransferDirection::H2D => "h2d",
            TransferDirection::D2H => "d2h",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PinnedChunkView {
    pub ptr: *mut uf_acl_sys::CVoid,
    pub bytes: u64,
    pub event_id: u64,
}

#[derive(Clone, Debug)]
pub struct LaneLeaseInfo {
    pub lane_id: u64,
    pub direction: TransferDirection,
    pub active_device_id: i32,
    pub chunk_bytes: u64,
    pub chunk_count: usize,
    pub pinned_footprint_bytes: u64,
    pub lane_wait_us: f64,
    pub stream_id: u64,
    pub chunks: Vec<PinnedChunkView>,
}

#[derive(Clone, Debug, Default)]
pub struct TransferChannelRunStats {
    pub direction: String,
    pub lane_id: u64,
    pub active_device_id: i32,
    pub chunk_bytes: u64,
    pub chunk_count: usize,
    pub pinned_footprint_bytes: u64,
    pub lane_wait_us: f64,
    pub chunks_transferred: u64,
    pub cpu_copy_us: f64,
    pub acl_copy_us: f64,
    pub acl_submit_us: f64,
    pub acl_wait_us: f64,
    pub wall_us: f64,
    pub queue_wait_us: f64,
    pub worker_execute_us: f64,
    pub overlap_ratio: f64,
    pub stream_create_count: u64,
    pub event_reuse_count: u64,
    pub event_record_count: u64,
    pub event_wait_count: u64,
    pub pipeline_overlap: bool,
}

#[derive(Clone, Debug, Default)]
pub struct TransferChannelStats {
    pub pinned_total_bytes: u64,
    pub pinned_used_bytes: u64,
    pub pinned_idle_bytes: u64,
    pub h2d_lane_count: usize,
    pub d2h_lane_count: usize,
    pub h2d_busy_lanes: usize,
    pub d2h_busy_lanes: usize,
    pub lane_wait_count: u64,
    pub budget_wait_us: f64,
    pub chunk_bytes_h2d: u64,
    pub chunk_bytes_d2h: u64,
    pub chunk_count: usize,
    pub total_acquires: u64,
    pub h2d_max_lanes: usize,
    pub d2h_max_lanes: usize,
    pub idle_ttl_ms: u64,
    pub idle_reaped_lanes: u64,
    pub idle_reaped_bytes: u64,
}

struct TransferLane {
    lane_id: u64,
    direction: TransferDirection,
    active_device_id: Option<i32>,
    chunk_bytes: u64,
    stream: AclStream,
    events: Vec<AclEvent>,
    chunks: Vec<PinnedHostAllocation>,
    busy: bool,
    idle_since: Option<Instant>,
    acquire_count: u64,
}

impl TransferLane {
    fn footprint_bytes(&self) -> u64 {
        self.chunks.iter().map(PinnedHostAllocation::bytes).sum()
    }
}

#[derive(Default)]
struct ManagerState {
    next_lane_id: u64,
    lanes: Vec<TransferLane>,
    lane_wait_count: u64,
    budget_wait_us: f64,
    total_acquires: u64,
    idle_reaped_lanes: u64,
    idle_reaped_bytes: u64,
}

pub struct TransferChannelManager {
    state: Mutex<ManagerState>,
    cv: Condvar,
}

static TRANSFER_CHANNEL_MANAGER: OnceLock<TransferChannelManager> = OnceLock::new();

pub fn transfer_channel_manager() -> &'static TransferChannelManager {
    TRANSFER_CHANNEL_MANAGER.get_or_init(TransferChannelManager::new)
}

impl TransferChannelManager {
    fn new() -> Self {
        Self {
            state: Mutex::new(ManagerState {
                next_lane_id: 1,
                lanes: Vec::new(),
                lane_wait_count: 0,
                budget_wait_us: 0.0,
                total_acquires: 0,
                idle_reaped_lanes: 0,
                idle_reaped_bytes: 0,
            }),
            cv: Condvar::new(),
        }
    }

    pub fn with_lane<T>(
        &self,
        direction: TransferDirection,
        active_device_id: i32,
        bytes_hint: u64,
        f: impl FnOnce(&LaneLeaseInfo) -> Result<T, String>,
    ) -> Result<T, String> {
        let lease = self.acquire(direction, active_device_id, bytes_hint)?;
        let lane_id = lease.lane_id;
        let busy = span(
            TraceCategory::Channel,
            "channel.lane_busy",
            vec![
                ("lane_id", lease.lane_id.to_string()),
                ("direction", lease.direction.as_str().to_string()),
                ("device_id", lease.active_device_id.to_string()),
                ("bytes", bytes_hint.to_string()),
                ("chunk_bytes", lease.chunk_bytes.to_string()),
                ("chunk_count", lease.chunk_count.to_string()),
            ],
        );
        let result = f(&lease);
        drop(busy);
        self.release(lane_id);
        result
    }

    fn acquire(
        &self,
        direction: TransferDirection,
        active_device_id: i32,
        bytes_hint: u64,
    ) -> Result<LaneLeaseInfo, String> {
        let started = Instant::now();
        let chunk_bytes = chunk_bytes_for(direction).min(bytes_hint.max(1)).max(1);
        let chunk_count = chunk_count();
        let mut state = self.state.lock().unwrap();
        loop {
            if let Some(index) = state.lanes.iter().position(|lane| {
                !lane.busy && lane.direction == direction && lane.chunk_bytes == chunk_bytes
            }) {
                let wait_us = started.elapsed().as_secs_f64() * 1_000_000.0;
                state.total_acquires += 1;
                if wait_us > 1.0 {
                    state.lane_wait_count += 1;
                    state.budget_wait_us += wait_us;
                }
                let lane = &mut state.lanes[index];
                lane.busy = true;
                lane.active_device_id = Some(active_device_id);
                lane.idle_since = None;
                lane.acquire_count += 1;
                record_duration_us(
                    TraceCategory::Channel,
                    "channel.acquire",
                    wait_us,
                    vec![
                        ("lane_id", lane.lane_id.to_string()),
                        ("direction", lane.direction.as_str().to_string()),
                        ("device_id", active_device_id.to_string()),
                        ("chunk_bytes", lane.chunk_bytes.to_string()),
                        ("reused", "true".to_string()),
                    ],
                );
                return Ok(lane_lease_info(lane, active_device_id, wait_us));
            }

            let required = chunk_bytes
                .checked_mul(chunk_count as u64)
                .ok_or_else(|| "pinned lane footprint overflow".to_string())?;
            reap_expired_lanes(&mut state, Instant::now());
            let max_lanes = max_lanes_for(direction);
            if max_lanes > 0 && direction_lane_count(&state, direction) >= max_lanes {
                if reap_one_idle_lane(&mut state, Some(direction)) {
                    continue;
                }
                state = self.cv.wait(state).unwrap();
                continue;
            }

            let budget = max_pinned_bytes();
            let total = pinned_total_bytes(&state);
            if budget == 0 || total + required <= budget {
                let lane_id = state.next_lane_id;
                state.next_lane_id += 1;
                let mut chunks = Vec::with_capacity(chunk_count);
                let stream = create_stream()?;
                let mut events = Vec::with_capacity(chunk_count);
                for _ in 0..chunk_count {
                    chunks.push(malloc_pinned_host(chunk_bytes)?);
                    events.push(create_event()?);
                }
                let lane = TransferLane {
                    lane_id,
                    direction,
                    active_device_id: Some(active_device_id),
                    chunk_bytes,
                    stream,
                    events,
                    chunks,
                    busy: true,
                    idle_since: None,
                    acquire_count: 1,
                };
                state.total_acquires += 1;
                let wait_us = started.elapsed().as_secs_f64() * 1_000_000.0;
                if wait_us > 1.0 {
                    state.lane_wait_count += 1;
                    state.budget_wait_us += wait_us;
                }
                let info = lane_lease_info(&lane, active_device_id, wait_us);
                record_duration_us(
                    TraceCategory::Channel,
                    "channel.lane_create",
                    wait_us,
                    vec![
                        ("lane_id", lane.lane_id.to_string()),
                        ("direction", lane.direction.as_str().to_string()),
                        ("device_id", active_device_id.to_string()),
                        ("chunk_bytes", lane.chunk_bytes.to_string()),
                        ("chunk_count", lane.chunks.len().to_string()),
                        ("pinned_footprint_bytes", lane.footprint_bytes().to_string()),
                    ],
                );
                record_duration_us(
                    TraceCategory::Channel,
                    "channel.acquire",
                    wait_us,
                    vec![
                        ("lane_id", lane.lane_id.to_string()),
                        ("direction", lane.direction.as_str().to_string()),
                        ("device_id", active_device_id.to_string()),
                        ("chunk_bytes", lane.chunk_bytes.to_string()),
                        ("reused", "false".to_string()),
                    ],
                );
                eprintln!(
                    "{{\"event\":\"transfer_lane_created\",\"lane_id\":{},\"direction\":\"{}\",\"device_id\":{},\"chunk_bytes\":{},\"chunk_count\":{},\"pinned_footprint_bytes\":{}}}",
                    lane.lane_id,
                    lane.direction.as_str(),
                    active_device_id,
                    lane.chunk_bytes,
                    lane.chunks.len(),
                    lane.footprint_bytes()
                );
                state.lanes.push(lane);
                return Ok(info);
            }

            if required > budget {
                return Err(format!(
                    "pinned lane requires {} bytes but UF_PINNED_MAX_BYTES={}",
                    required, budget
                ));
            }
            if reap_one_idle_lane(&mut state, None) {
                continue;
            }
            state = self.cv.wait(state).unwrap();
        }
    }

    fn release(&self, lane_id: u64) {
        let mut state = self.state.lock().unwrap();
        if let Some(lane) = state.lanes.iter_mut().find(|lane| lane.lane_id == lane_id) {
            record_duration_us(
                TraceCategory::Channel,
                "channel.release",
                0.0,
                vec![
                    ("lane_id", lane.lane_id.to_string()),
                    ("direction", lane.direction.as_str().to_string()),
                    (
                        "device_id",
                        lane.active_device_id.unwrap_or_default().to_string(),
                    ),
                    ("pinned_footprint_bytes", lane.footprint_bytes().to_string()),
                ],
            );
            lane.busy = false;
            lane.active_device_id = None;
            lane.idle_since = Some(Instant::now());
        }
        self.cv.notify_all();
    }

    pub fn stats(&self) -> TransferChannelStats {
        let mut state = self.state.lock().unwrap();
        reap_expired_lanes(&mut state, Instant::now());
        let mut out = TransferChannelStats {
            pinned_total_bytes: pinned_total_bytes(&state),
            pinned_used_bytes: 0,
            pinned_idle_bytes: 0,
            h2d_lane_count: 0,
            d2h_lane_count: 0,
            h2d_busy_lanes: 0,
            d2h_busy_lanes: 0,
            lane_wait_count: state.lane_wait_count,
            budget_wait_us: state.budget_wait_us,
            chunk_bytes_h2d: chunk_bytes_for(TransferDirection::H2D),
            chunk_bytes_d2h: chunk_bytes_for(TransferDirection::D2H),
            chunk_count: chunk_count(),
            total_acquires: state.total_acquires,
            h2d_max_lanes: max_lanes_for(TransferDirection::H2D),
            d2h_max_lanes: max_lanes_for(TransferDirection::D2H),
            idle_ttl_ms: idle_ttl_ms(),
            idle_reaped_lanes: state.idle_reaped_lanes,
            idle_reaped_bytes: state.idle_reaped_bytes,
        };
        for lane in &state.lanes {
            match lane.direction {
                TransferDirection::H2D => {
                    out.h2d_lane_count += 1;
                    if lane.busy {
                        out.h2d_busy_lanes += 1;
                    }
                }
                TransferDirection::D2H => {
                    out.d2h_lane_count += 1;
                    if lane.busy {
                        out.d2h_busy_lanes += 1;
                    }
                }
            }
            if lane.busy {
                out.pinned_used_bytes += lane.footprint_bytes();
            } else {
                out.pinned_idle_bytes += lane.footprint_bytes();
            }
        }
        out
    }
}

fn lane_lease_info(lane: &TransferLane, active_device_id: i32, lane_wait_us: f64) -> LaneLeaseInfo {
    let chunks = lane
        .chunks
        .iter()
        .zip(lane.events.iter())
        .map(|(chunk, event)| PinnedChunkView {
            ptr: chunk.ptr(),
            bytes: chunk.bytes(),
            event_id: event.id(),
        })
        .collect::<Vec<_>>();
    LaneLeaseInfo {
        lane_id: lane.lane_id,
        direction: lane.direction,
        active_device_id,
        chunk_bytes: lane.chunk_bytes,
        chunk_count: chunks.len(),
        pinned_footprint_bytes: chunks.iter().map(|chunk| chunk.bytes).sum(),
        lane_wait_us,
        stream_id: lane.stream.id(),
        chunks,
    }
}

fn pinned_total_bytes(state: &ManagerState) -> u64 {
    state.lanes.iter().map(TransferLane::footprint_bytes).sum()
}

fn chunk_count() -> usize {
    env::var("UF_PINNED_CHUNK_COUNT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_CHUNK_COUNT)
}

fn chunk_bytes_for(direction: TransferDirection) -> u64 {
    let direction_key = match direction {
        TransferDirection::H2D => "UF_H2D_PINNED_CHUNK_BYTES",
        TransferDirection::D2H => "UF_D2H_PINNED_CHUNK_BYTES",
    };
    env_u64(direction_key)
        .or_else(|| env_u64("UF_PINNED_CHUNK_BYTES"))
        .or_else(|| env_u64("UF_DDR_COPY_CHUNK_BYTES"))
        .unwrap_or(match direction {
            TransferDirection::H2D => DEFAULT_H2D_CHUNK_BYTES,
            TransferDirection::D2H => DEFAULT_D2H_CHUNK_BYTES,
        })
        .max(1)
}

fn max_pinned_bytes() -> u64 {
    env_u64("UF_PINNED_MAX_BYTES").unwrap_or(0)
}

fn max_lanes_for(direction: TransferDirection) -> usize {
    let key = match direction {
        TransferDirection::H2D => "UF_H2D_MAX_LANES",
        TransferDirection::D2H => "UF_D2H_MAX_LANES",
    };
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0)
}

fn idle_ttl_ms() -> u64 {
    env::var("UF_PINNED_IDLE_TTL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_IDLE_TTL_MS)
}

fn idle_ttl() -> Option<Duration> {
    let ms = idle_ttl_ms();
    if ms == 0 {
        None
    } else {
        Some(Duration::from_millis(ms))
    }
}

fn direction_lane_count(state: &ManagerState, direction: TransferDirection) -> usize {
    state
        .lanes
        .iter()
        .filter(|lane| lane.direction == direction)
        .count()
}

fn reap_expired_lanes(state: &mut ManagerState, now: Instant) {
    let Some(ttl) = idle_ttl() else {
        return;
    };
    let mut index = 0usize;
    while index < state.lanes.len() {
        let expired = {
            let lane = &state.lanes[index];
            !lane.busy
                && lane
                    .idle_since
                    .map(|idle_since| now.saturating_duration_since(idle_since) >= ttl)
                    .unwrap_or(false)
        };
        if expired {
            reap_lane_at(state, index, "idle_ttl");
        } else {
            index += 1;
        }
    }
}

fn reap_one_idle_lane(state: &mut ManagerState, direction: Option<TransferDirection>) -> bool {
    let index = state.lanes.iter().position(|lane| {
        !lane.busy
            && direction
                .map(|direction| lane.direction == direction)
                .unwrap_or(true)
    });
    if let Some(index) = index {
        reap_lane_at(state, index, "capacity_or_budget");
        true
    } else {
        false
    }
}

fn reap_lane_at(state: &mut ManagerState, index: usize, reason: &str) {
    let lane_id = state.lanes[index].lane_id;
    let direction = state.lanes[index].direction.as_str();
    let footprint = state.lanes[index].footprint_bytes();
    state.lanes.remove(index);
    state.idle_reaped_lanes += 1;
    state.idle_reaped_bytes += footprint;
    record_duration_us(
        TraceCategory::Channel,
        "channel.reap",
        0.0,
        vec![
            ("lane_id", lane_id.to_string()),
            ("direction", direction.to_string()),
            ("reason", reason.to_string()),
            ("pinned_footprint_bytes", footprint.to_string()),
        ],
    );
    eprintln!(
        "{{\"event\":\"transfer_lane_reaped\",\"lane_id\":{},\"direction\":\"{}\",\"reason\":\"{}\",\"pinned_footprint_bytes\":{}}}",
        lane_id, direction, reason, footprint
    );
}

fn env_u64(key: &str) -> Option<u64> {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
}
