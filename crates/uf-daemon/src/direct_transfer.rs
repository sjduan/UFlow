use crate::acl_backend::{create_event, create_stream, AclEvent, AclStream};
use crate::trace::{record_duration_us, span, TraceCategory};
use crate::transfer_channel::TransferDirection;

use std::env;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_DIRECT_IDLE_TTL_MS: u64 = 30_000;

type TransferJob = Box<dyn FnOnce(f64) + Send + 'static>;

struct QueuedTransferJob {
    event_id: u64,
    plan_id: u64,
    enqueued_at: Instant,
    job: TransferJob,
}

#[derive(Clone, Debug, Default)]
pub struct DirectTransferExecutorStats {
    pub worker_count: usize,
    pub queue_depth: usize,
    pub queue_depth_high_watermark: usize,
    pub submitted_jobs: u64,
    pub completed_jobs: u64,
    pub queue_wait_count: u64,
    pub queue_wait_us_total: f64,
    pub worker_execute_us_total: f64,
}

#[derive(Default)]
struct ExecutorStatsState {
    submitted_jobs: u64,
    completed_jobs: u64,
    queue_wait_count: u64,
    queue_wait_us_total: f64,
    worker_execute_us_total: f64,
    queue_depth_high_watermark: usize,
}

pub struct DirectTransferExecutor {
    sender: mpsc::Sender<QueuedTransferJob>,
    stats: Arc<Mutex<ExecutorStatsState>>,
    queue_depth: Arc<AtomicUsize>,
    worker_count: usize,
}

static DIRECT_TRANSFER_EXECUTOR: OnceLock<DirectTransferExecutor> = OnceLock::new();

pub fn direct_transfer_executor() -> &'static DirectTransferExecutor {
    DIRECT_TRANSFER_EXECUTOR.get_or_init(DirectTransferExecutor::new)
}

impl DirectTransferExecutor {
    fn new() -> Self {
        let worker_count = env_usize("UF_TRANSFER_WORKERS", 1).max(1);
        let (sender, receiver) = mpsc::channel::<QueuedTransferJob>();
        let receiver = Arc::new(Mutex::new(receiver));
        let stats = Arc::new(Mutex::new(ExecutorStatsState::default()));
        let queue_depth = Arc::new(AtomicUsize::new(0));
        for worker_id in 0..worker_count {
            let receiver = Arc::clone(&receiver);
            let stats = Arc::clone(&stats);
            let queue_depth = Arc::clone(&queue_depth);
            let _ = thread::Builder::new()
                .name(format!("uf-direct-transfer-worker-{}", worker_id))
                .spawn(move || worker_loop(worker_id, receiver, stats, queue_depth));
        }
        Self {
            sender,
            stats,
            queue_depth,
            worker_count,
        }
    }

    pub fn submit(&self, event_id: u64, plan_id: u64, job: TransferJob) -> Result<(), String> {
        let depth = self.queue_depth.fetch_add(1, Ordering::SeqCst) + 1;
        {
            let mut stats = self.stats.lock().unwrap();
            stats.submitted_jobs += 1;
            stats.queue_depth_high_watermark = stats.queue_depth_high_watermark.max(depth);
        }
        let _trace = span(
            TraceCategory::Transfer,
            "transfer.enqueue",
            vec![
                ("event_id", event_id.to_string()),
                ("plan_id", plan_id.to_string()),
                ("queue_depth", depth.to_string()),
            ],
        );
        self.sender
            .send(QueuedTransferJob {
                event_id,
                plan_id,
                enqueued_at: Instant::now(),
                job,
            })
            .map_err(|e| {
                self.queue_depth.fetch_sub(1, Ordering::SeqCst);
                format!("direct transfer executor enqueue failed: {}", e)
            })
    }

    pub fn stats(&self) -> DirectTransferExecutorStats {
        let stats = self.stats.lock().unwrap();
        DirectTransferExecutorStats {
            worker_count: self.worker_count,
            queue_depth: self.queue_depth.load(Ordering::SeqCst),
            queue_depth_high_watermark: stats.queue_depth_high_watermark,
            submitted_jobs: stats.submitted_jobs,
            completed_jobs: stats.completed_jobs,
            queue_wait_count: stats.queue_wait_count,
            queue_wait_us_total: stats.queue_wait_us_total,
            worker_execute_us_total: stats.worker_execute_us_total,
        }
    }
}

fn worker_loop(
    worker_id: usize,
    receiver: Arc<Mutex<mpsc::Receiver<QueuedTransferJob>>>,
    stats: Arc<Mutex<ExecutorStatsState>>,
    queue_depth: Arc<AtomicUsize>,
) {
    loop {
        let recv_result = {
            let receiver = receiver.lock().unwrap();
            receiver.recv()
        };
        let Ok(queued) = recv_result else {
            break;
        };
        queue_depth.fetch_sub(1, Ordering::SeqCst);
        let queue_wait_us = queued.enqueued_at.elapsed().as_secs_f64() * 1_000_000.0;
        record_duration_us(
            TraceCategory::Transfer,
            "transfer.queue_wait",
            queue_wait_us,
            vec![
                ("event_id", queued.event_id.to_string()),
                ("plan_id", queued.plan_id.to_string()),
                ("worker_id", worker_id.to_string()),
            ],
        );
        let execute_started = Instant::now();
        {
            let _trace = span(
                TraceCategory::Transfer,
                "transfer.worker_execute",
                vec![
                    ("event_id", queued.event_id.to_string()),
                    ("plan_id", queued.plan_id.to_string()),
                    ("worker_id", worker_id.to_string()),
                ],
            );
            (queued.job)(queue_wait_us);
        }
        let worker_execute_us = execute_started.elapsed().as_secs_f64() * 1_000_000.0;
        let mut stats = stats.lock().unwrap();
        stats.completed_jobs += 1;
        if queue_wait_us > 1.0 {
            stats.queue_wait_count += 1;
            stats.queue_wait_us_total += queue_wait_us;
        }
        stats.worker_execute_us_total += worker_execute_us;
    }
}

#[derive(Clone, Debug)]
pub struct DirectLaneLeaseInfo {
    pub lane_id: u64,
    pub direction: TransferDirection,
    pub active_device_id: i32,
    pub lane_wait_us: f64,
    pub stream_id: u64,
    pub event_id: u64,
    pub stream_create_count: u64,
    pub event_reuse_count: u64,
}

#[derive(Clone, Debug, Default)]
pub struct DirectLaneStats {
    pub lane_count: usize,
    pub h2d_lane_count: usize,
    pub d2h_lane_count: usize,
    pub h2d_busy_lanes: usize,
    pub d2h_busy_lanes: usize,
    pub total_acquires: u64,
    pub lane_wait_count: u64,
    pub lane_wait_us_total: f64,
    pub stream_create_count: u64,
    pub event_create_count: u64,
    pub event_reuse_count: u64,
    pub h2d_max_lanes: usize,
    pub d2h_max_lanes: usize,
    pub idle_ttl_ms: u64,
    pub idle_reaped_lanes: u64,
}

struct DirectLane {
    lane_id: u64,
    direction: TransferDirection,
    device_id: i32,
    stream: AclStream,
    event: AclEvent,
    busy: bool,
    idle_since: Option<Instant>,
    acquire_count: u64,
}

#[derive(Default)]
struct DirectLaneManagerState {
    next_lane_id: u64,
    lanes: Vec<DirectLane>,
    total_acquires: u64,
    lane_wait_count: u64,
    lane_wait_us_total: f64,
    stream_create_count: u64,
    event_create_count: u64,
    event_reuse_count: u64,
    idle_reaped_lanes: u64,
}

pub struct DirectLaneManager {
    state: Mutex<DirectLaneManagerState>,
    cv: Condvar,
}

static DIRECT_LANE_MANAGER: OnceLock<DirectLaneManager> = OnceLock::new();

pub fn direct_lane_manager() -> &'static DirectLaneManager {
    DIRECT_LANE_MANAGER.get_or_init(DirectLaneManager::new)
}

impl DirectLaneManager {
    fn new() -> Self {
        Self {
            state: Mutex::new(DirectLaneManagerState {
                next_lane_id: 1,
                ..DirectLaneManagerState::default()
            }),
            cv: Condvar::new(),
        }
    }

    pub fn with_lane<T>(
        &self,
        direction: TransferDirection,
        active_device_id: i32,
        f: impl FnOnce(&DirectLaneLeaseInfo) -> Result<T, String>,
    ) -> Result<T, String> {
        let lease = self.acquire(direction, active_device_id)?;
        let lane_id = lease.lane_id;
        let busy = span(
            TraceCategory::Channel,
            "direct_lane.busy",
            vec![
                ("lane_id", lease.lane_id.to_string()),
                ("direction", lease.direction.as_str().to_string()),
                ("device_id", lease.active_device_id.to_string()),
                ("stream_id", lease.stream_id.to_string()),
                ("acl_event_id", lease.event_id.to_string()),
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
    ) -> Result<DirectLaneLeaseInfo, String> {
        let started = Instant::now();
        let mut state = self.state.lock().unwrap();
        loop {
            reap_expired_lanes(&mut state, Instant::now());
            if let Some(index) = state.lanes.iter().position(|lane| {
                !lane.busy && lane.direction == direction && lane.device_id == active_device_id
            }) {
                let wait_us = started.elapsed().as_secs_f64() * 1_000_000.0;
                state.total_acquires += 1;
                if wait_us > 1.0 {
                    state.lane_wait_count += 1;
                    state.lane_wait_us_total += wait_us;
                }
                let (lane_id, lane_direction, stream_id, event_id, event_reuse_count) = {
                    let lane = &mut state.lanes[index];
                    lane.busy = true;
                    lane.idle_since = None;
                    lane.acquire_count += 1;
                    (
                        lane.lane_id,
                        lane.direction,
                        lane.stream.id(),
                        lane.event.id(),
                        lane.acquire_count.saturating_sub(1),
                    )
                };
                state.event_reuse_count += u64::from(event_reuse_count > 0);
                record_duration_us(
                    TraceCategory::Channel,
                    "direct_lane.acquire",
                    wait_us,
                    vec![
                        ("lane_id", lane_id.to_string()),
                        ("direction", lane_direction.as_str().to_string()),
                        ("device_id", active_device_id.to_string()),
                        ("reused", "true".to_string()),
                    ],
                );
                return Ok(DirectLaneLeaseInfo {
                    lane_id,
                    direction: lane_direction,
                    active_device_id,
                    lane_wait_us: wait_us,
                    stream_id,
                    event_id,
                    stream_create_count: 0,
                    event_reuse_count,
                });
            }

            let max_lanes = max_lanes_for(direction);
            if max_lanes > 0 && direction_lane_count(&state, direction) >= max_lanes {
                if reap_one_idle_lane(&mut state, Some(direction)) {
                    continue;
                }
                state = self.cv.wait(state).unwrap();
                continue;
            }

            let lane_id = state.next_lane_id;
            state.next_lane_id += 1;
            let stream = create_stream()?;
            let event = create_event()?;
            let stream_id = stream.id();
            let event_id = event.id();
            let lane = DirectLane {
                lane_id,
                direction,
                device_id: active_device_id,
                stream,
                event,
                busy: true,
                idle_since: None,
                acquire_count: 1,
            };
            state.lanes.push(lane);
            state.total_acquires += 1;
            state.stream_create_count += 1;
            state.event_create_count += 1;
            let wait_us = started.elapsed().as_secs_f64() * 1_000_000.0;
            if wait_us > 1.0 {
                state.lane_wait_count += 1;
                state.lane_wait_us_total += wait_us;
            }
            record_duration_us(
                TraceCategory::Channel,
                "direct_lane.create",
                wait_us,
                vec![
                    ("lane_id", lane_id.to_string()),
                    ("direction", direction.as_str().to_string()),
                    ("device_id", active_device_id.to_string()),
                    ("stream_id", stream_id.to_string()),
                    ("acl_event_id", event_id.to_string()),
                ],
            );
            return Ok(DirectLaneLeaseInfo {
                lane_id,
                direction,
                active_device_id,
                lane_wait_us: wait_us,
                stream_id,
                event_id,
                stream_create_count: 1,
                event_reuse_count: 0,
            });
        }
    }

    fn release(&self, lane_id: u64) {
        let mut state = self.state.lock().unwrap();
        if let Some(lane) = state.lanes.iter_mut().find(|lane| lane.lane_id == lane_id) {
            lane.busy = false;
            lane.idle_since = Some(Instant::now());
            let _trace = span(
                TraceCategory::Channel,
                "direct_lane.release",
                vec![
                    ("lane_id", lane.lane_id.to_string()),
                    ("direction", lane.direction.as_str().to_string()),
                    ("device_id", lane.device_id.to_string()),
                ],
            );
        }
        self.cv.notify_all();
    }

    pub fn stats(&self) -> DirectLaneStats {
        let mut state = self.state.lock().unwrap();
        reap_expired_lanes(&mut state, Instant::now());
        let mut stats = DirectLaneStats {
            lane_count: state.lanes.len(),
            total_acquires: state.total_acquires,
            lane_wait_count: state.lane_wait_count,
            lane_wait_us_total: state.lane_wait_us_total,
            stream_create_count: state.stream_create_count,
            event_create_count: state.event_create_count,
            event_reuse_count: state.event_reuse_count,
            h2d_max_lanes: max_lanes_for(TransferDirection::H2D),
            d2h_max_lanes: max_lanes_for(TransferDirection::D2H),
            idle_ttl_ms: idle_ttl_ms(),
            idle_reaped_lanes: state.idle_reaped_lanes,
            ..DirectLaneStats::default()
        };
        for lane in &state.lanes {
            match lane.direction {
                TransferDirection::H2D => {
                    stats.h2d_lane_count += 1;
                    if lane.busy {
                        stats.h2d_busy_lanes += 1;
                    }
                }
                TransferDirection::D2H => {
                    stats.d2h_lane_count += 1;
                    if lane.busy {
                        stats.d2h_busy_lanes += 1;
                    }
                }
            }
        }
        stats
    }
}

fn reap_expired_lanes(state: &mut DirectLaneManagerState, now: Instant) {
    let ttl = idle_ttl_ms();
    if ttl == 0 {
        return;
    }
    let ttl = Duration::from_millis(ttl);
    let before = state.lanes.len();
    state.lanes.retain(|lane| {
        if lane.busy {
            return true;
        }
        match lane.idle_since {
            Some(idle_since) => now.saturating_duration_since(idle_since) < ttl,
            None => true,
        }
    });
    state.idle_reaped_lanes += (before - state.lanes.len()) as u64;
}

fn reap_one_idle_lane(
    state: &mut DirectLaneManagerState,
    direction: Option<TransferDirection>,
) -> bool {
    let Some(index) = state.lanes.iter().position(|lane| {
        !lane.busy
            && direction
                .map(|value| lane.direction == value)
                .unwrap_or(true)
    }) else {
        return false;
    };
    state.lanes.swap_remove(index);
    state.idle_reaped_lanes += 1;
    true
}

fn direction_lane_count(state: &DirectLaneManagerState, direction: TransferDirection) -> usize {
    state
        .lanes
        .iter()
        .filter(|lane| lane.direction == direction)
        .count()
}

fn max_lanes_for(direction: TransferDirection) -> usize {
    match direction {
        TransferDirection::H2D => env_usize("UF_DIRECT_H2D_MAX_LANES", 1),
        TransferDirection::D2H => env_usize("UF_DIRECT_D2H_MAX_LANES", 1),
    }
}

fn idle_ttl_ms() -> u64 {
    env_u64("UF_DIRECT_IDLE_TTL_MS", DEFAULT_DIRECT_IDLE_TTL_MS)
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}
