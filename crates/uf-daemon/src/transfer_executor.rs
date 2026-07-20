use crate::acl_backend::{
    d2d_on_devices, d2h_async_on_stream, d2h_on_device, h2d_async_on_stream, h2d_on_device,
    register_host, synchronize_event, HbmAllocation,
};
use crate::catalog::{Catalog, Object, SharedCatalog, TransferEventRecord, TransferPlanRecord};
use crate::common::{env_bool, event_payload, now_ns, HBM_PLACEMENT};
use crate::ddr_backend::with_ddr_mapping;
use crate::direct_transfer::{direct_lane_manager, direct_transfer_executor};
use crate::ssd_backend::{read_ssd_to_ptr, ssd_chunk_bytes, write_ptr_to_ssd, SsdIoStats};
use crate::ssd_direct::{copy_hbm_to_ssd_direct, copy_ssd_to_hbm_direct, SsdHbmDirectStats};
use crate::trace::{span, TraceCategory};
use crate::transfer_channel::{
    transfer_channel_manager, TransferChannelRunStats, TransferDirection,
};
use uf_core::{err, get_u64, ok, Kv};

use std::ptr;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone)]
struct RuntimeObject {
    object: Object,
    hbm: Option<HbmAllocation>,
}

#[derive(Clone)]
struct TransferWork {
    event_id: u64,
    plan: TransferPlanRecord,
    src: RuntimeObject,
    dst: RuntimeObject,
}

struct ActualTransfer {
    bytes_done: u64,
    actual_engine: String,
    actual_path: String,
    fallback_used: bool,
    fallback_reason: String,
    channel: Option<TransferChannelRunStats>,
    ssd: SsdIoStats,
    direct: SsdHbmDirectStats,
    relay_stage_count: u64,
    relay_ddr_hbm_us: f64,
    relay_total_us: f64,
}

impl ActualTransfer {
    fn new(
        bytes_done: u64,
        engine: String,
        path: String,
        fallback_used: bool,
        fallback_reason: String,
    ) -> Self {
        Self {
            bytes_done,
            actual_engine: engine,
            actual_path: path,
            fallback_used,
            fallback_reason,
            channel: None,
            ssd: SsdIoStats::default(),
            direct: SsdHbmDirectStats::default(),
            relay_stage_count: 0,
            relay_ddr_hbm_us: 0.0,
            relay_total_us: 0.0,
        }
    }
}

fn runtime_object(st: &Catalog, object: Object) -> Result<RuntimeObject, String> {
    if object.placement == HBM_PLACEMENT {
        let block = st
            .blocks
            .iter()
            .find(|block| block.block_id == object.block_id)
            .ok_or_else(|| "HBM block not found".to_string())?;
        if block.allocation.service_device_ptr == 0 {
            return Err("HBM service_device_ptr is not mapped".to_string());
        }
        Ok(RuntimeObject {
            object,
            hbm: Some(block.allocation.clone()),
        })
    } else {
        Ok(RuntimeObject { object, hbm: None })
    }
}

unsafe fn host_memcpy(dst: *mut u8, src: *const u8, bytes: u64) {
    libc::memcpy(
        dst as *mut libc::c_void,
        src as *const libc::c_void,
        bytes as usize,
    );
}

fn ddr_fast_profile(object: &RuntimeObject) -> &str {
    if object.object.placement == crate::common::DDR_PLACEMENT {
        object.object.ddr_fast_profile.as_str()
    } else {
        ""
    }
}

fn direct_engine_name(ddr: &RuntimeObject) -> String {
    if ddr_fast_profile(ddr) == "thp_pretouched" {
        "acl_direct_async_thp".to_string()
    } else {
        "acl_direct_async".to_string()
    }
}

fn direct_path_name(direction: &str, ddr: &RuntimeObject) -> String {
    let suffix = if ddr_fast_profile(ddr) == "thp_pretouched" {
        "direct_thp"
    } else {
        "direct_mmap"
    };
    match direction {
        "h2d" => format!("ddr_hbm_{}", suffix),
        "d2h" => format!("hbm_ddr_{}", suffix),
        _ => suffix.to_string(),
    }
}

fn engine_requests_direct(engine: &str) -> bool {
    engine.contains("direct")
}

fn direct_candidate_from_engine(engine: &str) -> &str {
    engine
        .strip_prefix("ssd_hbm_auto_")
        .or_else(|| engine.strip_prefix("ssd_hbm_"))
        .unwrap_or("file_mmap_acl_direct")
}

fn engine_requests_auto_ssd_hbm(engine: &str) -> bool {
    engine.starts_with("ssd_hbm_auto_")
}

fn auto_direct_fallback_reason(candidate: &str, reason: &str) -> String {
    format!(
        "auto SSD-HBM direct candidate {} failed, fell back to SSD-DDR-HBM relay: {}",
        candidate, reason
    )
}

fn ssd_to_hbm_relay_actual_path() -> String {
    "ssd_to_hbm_via_ddr".to_string()
}

fn hbm_to_ssd_relay_actual_path() -> String {
    "hbm_to_ssd_via_ddr".to_string()
}

fn ssd_hbm_relay_actual_engine() -> String {
    "ssd_hbm_relay_ddr".to_string()
}

fn ensure_transfer_bounds(
    src: &RuntimeObject,
    dst: &RuntimeObject,
    src_offset: u64,
    dst_offset: u64,
    nbytes: u64,
) -> Result<(), String> {
    if nbytes == 0 {
        return Err("transfer nbytes must be positive".to_string());
    }
    if src_offset.checked_add(nbytes).unwrap_or(u64::MAX) > src.object.requested_bytes
        || dst_offset.checked_add(nbytes).unwrap_or(u64::MAX) > dst.object.requested_bytes
    {
        return Err("transfer size exceeds source or destination object".to_string());
    }
    if !matches!(src.object.state.as_str(), "Ready" | "Modified") {
        return Err(format!(
            "source object {} is not ready, state={}",
            src.object.object_id, src.object.state
        ));
    }
    Ok(())
}

fn copy_hbm_to_ddr_direct(
    event_id: u64,
    src: &RuntimeObject,
    dst: &RuntimeObject,
    nbytes: u64,
) -> Result<TransferChannelRunStats, String> {
    let src_hbm = src
        .hbm
        .as_ref()
        .ok_or_else(|| "source is not HBM".to_string())?;
    direct_lane_manager().with_lane(TransferDirection::D2H, src_hbm.device_id, |lane| {
        let mut stats = TransferChannelRunStats {
            direction: lane.direction.as_str().to_string(),
            lane_id: lane.lane_id,
            active_device_id: lane.active_device_id,
            chunk_bytes: nbytes,
            chunk_count: 1,
            stream_create_count: lane.stream_create_count,
            event_reuse_count: lane.event_reuse_count,
            lane_wait_us: lane.lane_wait_us,
            pipeline_overlap: false,
            ..TransferChannelRunStats::default()
        };
        with_ddr_mapping(&dst.object, |dst_ptr, dst_len| {
            if nbytes as usize > dst_len {
                return Err("transfer size exceeds mapped DDR destination".to_string());
            }
            let _registered = if env_bool("UF_D2H_REGISTER_DDR", false) {
                let _register_trace = span(
                    TraceCategory::Backend,
                    "direct_d2h.register_host",
                    vec![
                        ("event_id", event_id.to_string()),
                        ("device_id", src_hbm.device_id.to_string()),
                        ("bytes", nbytes.to_string()),
                    ],
                );
                Some(register_host(
                    src_hbm.device_id,
                    dst_ptr as *mut _,
                    nbytes,
                    env_bool("UF_DDR_REGISTER_USE_V2", false),
                )?)
            } else {
                None
            };
            let wall_started = Instant::now();
            let submit_started = Instant::now();
            {
                let _submit_trace = span(
                    TraceCategory::Acl,
                    "direct_d2h.acl_submit",
                    vec![
                        ("event_id", event_id.to_string()),
                        ("device_id", src_hbm.device_id.to_string()),
                        ("stream_id", lane.stream_id.to_string()),
                        ("acl_event_id", lane.event_id.to_string()),
                        ("bytes", nbytes.to_string()),
                        ("ddr_fast_profile", ddr_fast_profile(dst).to_string()),
                    ],
                );
                d2h_async_on_stream(
                    src_hbm.device_id,
                    dst_ptr as *mut _,
                    src_hbm.service_device_ptr,
                    0,
                    nbytes,
                    lane.stream_id,
                    lane.event_id,
                )?;
            }
            stats.acl_submit_us += submit_started.elapsed().as_secs_f64() * 1_000_000.0;
            stats.event_record_count += 1;
            let wait_started = Instant::now();
            {
                let _wait_trace = span(
                    TraceCategory::Acl,
                    "direct_d2h.acl_wait",
                    vec![
                        ("event_id", event_id.to_string()),
                        ("device_id", src_hbm.device_id.to_string()),
                        ("acl_event_id", lane.event_id.to_string()),
                        ("bytes", nbytes.to_string()),
                        ("ddr_fast_profile", ddr_fast_profile(dst).to_string()),
                    ],
                );
                synchronize_event(lane.event_id)?;
            }
            stats.acl_wait_us += wait_started.elapsed().as_secs_f64() * 1_000_000.0;
            stats.event_wait_count += 1;
            stats.chunks_transferred = 1;
            stats.acl_copy_us = stats.acl_submit_us + stats.acl_wait_us;
            stats.wall_us = wall_started.elapsed().as_secs_f64() * 1_000_000.0;
            Ok(())
        })?;
        Ok(stats)
    })
}

fn copy_hbm_to_ddr(
    event_id: u64,
    src: &RuntimeObject,
    dst: &RuntimeObject,
    nbytes: u64,
    use_async: bool,
    engine: &str,
) -> Result<TransferChannelRunStats, String> {
    if use_async && engine_requests_direct(engine) {
        return copy_hbm_to_ddr_direct(event_id, src, dst, nbytes);
    }
    let src_hbm = src
        .hbm
        .as_ref()
        .ok_or_else(|| "source is not HBM".to_string())?;
    transfer_channel_manager().with_lane(
        TransferDirection::D2H,
        src_hbm.device_id,
        nbytes,
        |lane| {
            let mut stats = TransferChannelRunStats {
                direction: lane.direction.as_str().to_string(),
                lane_id: lane.lane_id,
                active_device_id: lane.active_device_id,
                chunk_bytes: lane.chunk_bytes,
                chunk_count: lane.chunk_count,
                pinned_footprint_bytes: lane.pinned_footprint_bytes,
                lane_wait_us: lane.lane_wait_us,
                pipeline_overlap: false,
                ..TransferChannelRunStats::default()
            };
            let wall_started = Instant::now();
            with_ddr_mapping(&dst.object, |dst_ptr, dst_len| {
                if nbytes as usize > dst_len {
                    return Err("transfer size exceeds mapped DDR destination".to_string());
                }
                let mut offset = 0u64;
                if use_async {
                    let mut inflight = vec![(false, 0u64, 0u64); lane.chunks.len()];
                    while offset < nbytes {
                        let chunk_index = (stats.chunks_transferred as usize) % lane.chunks.len();
                        let pinned = &lane.chunks[chunk_index];
                        let (was_inflight, copy_offset, copy_bytes) = inflight[chunk_index];
                        if was_inflight {
                            let wait_started = Instant::now();
                            {
                                let _wait_trace = span(
                                    TraceCategory::Acl,
                                    "chunk.acl_wait.d2h",
                                    vec![
                                        ("event_id", event_id.to_string()),
                                        ("lane_id", lane.lane_id.to_string()),
                                        ("chunk_index", chunk_index.to_string()),
                                        ("acl_event_id", pinned.event_id.to_string()),
                                        ("bytes", copy_bytes.to_string()),
                                    ],
                                );
                                synchronize_event(pinned.event_id)?;
                            }
                            stats.acl_wait_us += wait_started.elapsed().as_secs_f64() * 1_000_000.0;
                            stats.event_wait_count += 1;
                            let cpu_started = Instant::now();
                            {
                                let _copy_trace = span(
                                    TraceCategory::Chunk,
                                    "chunk.cpu_copy.pinned_to_memfd",
                                    vec![
                                        ("event_id", event_id.to_string()),
                                        ("lane_id", lane.lane_id.to_string()),
                                        ("chunk_index", chunk_index.to_string()),
                                        ("bytes", copy_bytes.to_string()),
                                    ],
                                );
                                unsafe {
                                    host_memcpy(
                                        dst_ptr.add(copy_offset as usize),
                                        pinned.ptr as *const u8,
                                        copy_bytes,
                                    );
                                }
                            }
                            stats.cpu_copy_us += cpu_started.elapsed().as_secs_f64() * 1_000_000.0;
                            inflight[chunk_index] = (false, 0, 0);
                        }

                        let chunk = (nbytes - offset).min(lane.chunk_bytes);
                        let submit_started = Instant::now();
                        {
                            let _submit_trace = span(
                                TraceCategory::Acl,
                                "chunk.acl_submit.d2h",
                                vec![
                                    ("event_id", event_id.to_string()),
                                    ("lane_id", lane.lane_id.to_string()),
                                    ("chunk_index", chunk_index.to_string()),
                                    ("stream_id", lane.stream_id.to_string()),
                                    ("acl_event_id", pinned.event_id.to_string()),
                                    ("offset", offset.to_string()),
                                    ("bytes", chunk.to_string()),
                                ],
                            );
                            d2h_async_on_stream(
                                src_hbm.device_id,
                                pinned.ptr,
                                src_hbm.service_device_ptr,
                                offset,
                                chunk,
                                lane.stream_id,
                                pinned.event_id,
                            )?;
                        }
                        stats.acl_submit_us += submit_started.elapsed().as_secs_f64() * 1_000_000.0;
                        stats.event_record_count += 1;
                        inflight[chunk_index] = (true, offset, chunk);
                        stats.chunks_transferred += 1;
                        offset += chunk;
                    }
                    for (idx, pinned) in lane.chunks.iter().enumerate() {
                        let (was_inflight, copy_offset, copy_bytes) = inflight[idx];
                        if was_inflight {
                            let wait_started = Instant::now();
                            {
                                let _wait_trace = span(
                                    TraceCategory::Acl,
                                    "chunk.acl_wait.d2h",
                                    vec![
                                        ("event_id", event_id.to_string()),
                                        ("lane_id", lane.lane_id.to_string()),
                                        ("chunk_index", idx.to_string()),
                                        ("acl_event_id", pinned.event_id.to_string()),
                                        ("bytes", copy_bytes.to_string()),
                                    ],
                                );
                                synchronize_event(pinned.event_id)?;
                            }
                            stats.acl_wait_us += wait_started.elapsed().as_secs_f64() * 1_000_000.0;
                            stats.event_wait_count += 1;
                            let cpu_started = Instant::now();
                            {
                                let _copy_trace = span(
                                    TraceCategory::Chunk,
                                    "chunk.cpu_copy.pinned_to_memfd",
                                    vec![
                                        ("event_id", event_id.to_string()),
                                        ("lane_id", lane.lane_id.to_string()),
                                        ("chunk_index", idx.to_string()),
                                        ("bytes", copy_bytes.to_string()),
                                    ],
                                );
                                unsafe {
                                    host_memcpy(
                                        dst_ptr.add(copy_offset as usize),
                                        pinned.ptr as *const u8,
                                        copy_bytes,
                                    );
                                }
                            }
                            stats.cpu_copy_us += cpu_started.elapsed().as_secs_f64() * 1_000_000.0;
                        }
                    }
                    stats.acl_copy_us = stats.acl_submit_us + stats.acl_wait_us;
                    stats.pipeline_overlap = stats.chunks_transferred > 1;
                } else {
                    while offset < nbytes {
                        let chunk = (nbytes - offset).min(lane.chunk_bytes);
                        let pinned =
                            &lane.chunks[(stats.chunks_transferred as usize) % lane.chunks.len()];
                        let acl_started = Instant::now();
                        {
                            let _acl_trace = span(
                                TraceCategory::Acl,
                                "chunk.acl_sync.d2h",
                                vec![
                                    ("event_id", event_id.to_string()),
                                    ("lane_id", lane.lane_id.to_string()),
                                    ("chunk_index", stats.chunks_transferred.to_string()),
                                    ("offset", offset.to_string()),
                                    ("bytes", chunk.to_string()),
                                ],
                            );
                            d2h_on_device(
                                src_hbm.device_id,
                                pinned.ptr,
                                src_hbm.service_device_ptr,
                                offset,
                                chunk,
                                false,
                            )?;
                        }
                        let acl_us = acl_started.elapsed().as_secs_f64() * 1_000_000.0;
                        stats.acl_copy_us += acl_us;
                        stats.acl_wait_us += acl_us;
                        let cpu_started = Instant::now();
                        {
                            let _copy_trace = span(
                                TraceCategory::Chunk,
                                "chunk.cpu_copy.pinned_to_memfd",
                                vec![
                                    ("event_id", event_id.to_string()),
                                    ("lane_id", lane.lane_id.to_string()),
                                    ("chunk_index", stats.chunks_transferred.to_string()),
                                    ("bytes", chunk.to_string()),
                                ],
                            );
                            unsafe {
                                host_memcpy(
                                    dst_ptr.add(offset as usize),
                                    pinned.ptr as *const u8,
                                    chunk,
                                );
                            }
                        }
                        stats.cpu_copy_us += cpu_started.elapsed().as_secs_f64() * 1_000_000.0;
                        stats.chunks_transferred += 1;
                        offset += chunk;
                    }
                }
                Ok(())
            })?;
            stats.wall_us = wall_started.elapsed().as_secs_f64() * 1_000_000.0;
            let serial_us = stats.cpu_copy_us + stats.acl_copy_us;
            if serial_us > 0.0 {
                stats.overlap_ratio = (1.0 - (stats.wall_us / serial_us)).clamp(0.0, 1.0);
            }
            stats.stream_create_count = 1;
            Ok(stats)
        },
    )
}

fn copy_ddr_to_hbm_direct(
    event_id: u64,
    src: &RuntimeObject,
    dst: &RuntimeObject,
    nbytes: u64,
) -> Result<TransferChannelRunStats, String> {
    let dst_hbm = dst
        .hbm
        .as_ref()
        .ok_or_else(|| "destination is not HBM".to_string())?;
    direct_lane_manager().with_lane(TransferDirection::H2D, dst_hbm.device_id, |lane| {
        let mut stats = TransferChannelRunStats {
            direction: lane.direction.as_str().to_string(),
            lane_id: lane.lane_id,
            active_device_id: lane.active_device_id,
            chunk_bytes: nbytes,
            chunk_count: 1,
            stream_create_count: lane.stream_create_count,
            event_reuse_count: lane.event_reuse_count,
            lane_wait_us: lane.lane_wait_us,
            pipeline_overlap: false,
            ..TransferChannelRunStats::default()
        };
        with_ddr_mapping(&src.object, |src_ptr, src_len| {
            if nbytes as usize > src_len {
                return Err("transfer size exceeds mapped DDR source".to_string());
            }
            let wall_started = Instant::now();
            let submit_started = Instant::now();
            {
                let _submit_trace = span(
                    TraceCategory::Acl,
                    "direct_h2d.acl_submit",
                    vec![
                        ("event_id", event_id.to_string()),
                        ("device_id", dst_hbm.device_id.to_string()),
                        ("stream_id", lane.stream_id.to_string()),
                        ("acl_event_id", lane.event_id.to_string()),
                        ("bytes", nbytes.to_string()),
                        ("ddr_fast_profile", ddr_fast_profile(src).to_string()),
                    ],
                );
                h2d_async_on_stream(
                    dst_hbm.device_id,
                    dst_hbm.service_device_ptr,
                    0,
                    src_ptr as *const _,
                    nbytes,
                    lane.stream_id,
                    lane.event_id,
                )?;
            }
            stats.acl_submit_us += submit_started.elapsed().as_secs_f64() * 1_000_000.0;
            stats.event_record_count += 1;
            let wait_started = Instant::now();
            {
                let _wait_trace = span(
                    TraceCategory::Acl,
                    "direct_h2d.acl_wait",
                    vec![
                        ("event_id", event_id.to_string()),
                        ("device_id", dst_hbm.device_id.to_string()),
                        ("acl_event_id", lane.event_id.to_string()),
                        ("bytes", nbytes.to_string()),
                        ("ddr_fast_profile", ddr_fast_profile(src).to_string()),
                    ],
                );
                synchronize_event(lane.event_id)?;
            }
            stats.acl_wait_us += wait_started.elapsed().as_secs_f64() * 1_000_000.0;
            stats.event_wait_count += 1;
            stats.chunks_transferred = 1;
            stats.acl_copy_us = stats.acl_submit_us + stats.acl_wait_us;
            stats.wall_us = wall_started.elapsed().as_secs_f64() * 1_000_000.0;
            Ok(())
        })?;
        Ok(stats)
    })
}

fn copy_ddr_to_hbm(
    event_id: u64,
    src: &RuntimeObject,
    dst: &RuntimeObject,
    nbytes: u64,
    use_async: bool,
    engine: &str,
) -> Result<TransferChannelRunStats, String> {
    let dst_hbm = dst
        .hbm
        .as_ref()
        .ok_or_else(|| "destination is not HBM".to_string())?;
    if use_async && engine_requests_direct(engine) {
        return copy_ddr_to_hbm_direct(event_id, src, dst, nbytes);
    }
    transfer_channel_manager().with_lane(
        TransferDirection::H2D,
        dst_hbm.device_id,
        nbytes,
        |lane| {
            let mut stats = TransferChannelRunStats {
                direction: lane.direction.as_str().to_string(),
                lane_id: lane.lane_id,
                active_device_id: lane.active_device_id,
                chunk_bytes: lane.chunk_bytes,
                chunk_count: lane.chunk_count,
                pinned_footprint_bytes: lane.pinned_footprint_bytes,
                lane_wait_us: lane.lane_wait_us,
                pipeline_overlap: false,
                ..TransferChannelRunStats::default()
            };
            let wall_started = Instant::now();
            with_ddr_mapping(&src.object, |src_ptr, src_len| {
                if nbytes as usize > src_len {
                    return Err("transfer size exceeds mapped DDR source".to_string());
                }
                let mut offset = 0u64;
                if use_async {
                    let mut inflight = vec![false; lane.chunks.len()];
                    while offset < nbytes {
                        let chunk_index = (stats.chunks_transferred as usize) % lane.chunks.len();
                        let pinned = &lane.chunks[chunk_index];
                        if inflight[chunk_index] {
                            let wait_started = Instant::now();
                            {
                                let _wait_trace = span(
                                    TraceCategory::Acl,
                                    "chunk.acl_wait.h2d",
                                    vec![
                                        ("event_id", event_id.to_string()),
                                        ("lane_id", lane.lane_id.to_string()),
                                        ("chunk_index", chunk_index.to_string()),
                                        ("acl_event_id", pinned.event_id.to_string()),
                                    ],
                                );
                                synchronize_event(pinned.event_id)?;
                            }
                            stats.acl_wait_us += wait_started.elapsed().as_secs_f64() * 1_000_000.0;
                            stats.event_wait_count += 1;
                            inflight[chunk_index] = false;
                        }

                        let chunk = (nbytes - offset).min(lane.chunk_bytes);
                        let cpu_started = Instant::now();
                        {
                            let _copy_trace = span(
                                TraceCategory::Chunk,
                                "chunk.cpu_copy.memfd_to_pinned",
                                vec![
                                    ("event_id", event_id.to_string()),
                                    ("lane_id", lane.lane_id.to_string()),
                                    ("chunk_index", chunk_index.to_string()),
                                    ("offset", offset.to_string()),
                                    ("bytes", chunk.to_string()),
                                ],
                            );
                            unsafe {
                                host_memcpy(
                                    pinned.ptr as *mut u8,
                                    src_ptr.add(offset as usize),
                                    chunk,
                                );
                            }
                        }
                        stats.cpu_copy_us += cpu_started.elapsed().as_secs_f64() * 1_000_000.0;

                        let submit_started = Instant::now();
                        {
                            let _submit_trace = span(
                                TraceCategory::Acl,
                                "chunk.acl_submit.h2d",
                                vec![
                                    ("event_id", event_id.to_string()),
                                    ("lane_id", lane.lane_id.to_string()),
                                    ("chunk_index", chunk_index.to_string()),
                                    ("stream_id", lane.stream_id.to_string()),
                                    ("acl_event_id", pinned.event_id.to_string()),
                                    ("offset", offset.to_string()),
                                    ("bytes", chunk.to_string()),
                                ],
                            );
                            h2d_async_on_stream(
                                dst_hbm.device_id,
                                dst_hbm.service_device_ptr,
                                offset,
                                pinned.ptr as *const _,
                                chunk,
                                lane.stream_id,
                                pinned.event_id,
                            )?;
                        }
                        stats.acl_submit_us += submit_started.elapsed().as_secs_f64() * 1_000_000.0;
                        stats.event_record_count += 1;
                        inflight[chunk_index] = true;
                        stats.chunks_transferred += 1;
                        offset += chunk;
                    }
                    for (idx, pinned) in lane.chunks.iter().enumerate() {
                        if inflight[idx] {
                            let wait_started = Instant::now();
                            {
                                let _wait_trace = span(
                                    TraceCategory::Acl,
                                    "chunk.acl_wait.h2d",
                                    vec![
                                        ("event_id", event_id.to_string()),
                                        ("lane_id", lane.lane_id.to_string()),
                                        ("chunk_index", idx.to_string()),
                                        ("acl_event_id", pinned.event_id.to_string()),
                                    ],
                                );
                                synchronize_event(pinned.event_id)?;
                            }
                            stats.acl_wait_us += wait_started.elapsed().as_secs_f64() * 1_000_000.0;
                            stats.event_wait_count += 1;
                        }
                    }
                    stats.acl_copy_us = stats.acl_submit_us + stats.acl_wait_us;
                    stats.pipeline_overlap = stats.chunks_transferred > 1;
                } else {
                    while offset < nbytes {
                        let chunk = (nbytes - offset).min(lane.chunk_bytes);
                        let pinned =
                            &lane.chunks[(stats.chunks_transferred as usize) % lane.chunks.len()];
                        let cpu_started = Instant::now();
                        {
                            let _copy_trace = span(
                                TraceCategory::Chunk,
                                "chunk.cpu_copy.memfd_to_pinned",
                                vec![
                                    ("event_id", event_id.to_string()),
                                    ("lane_id", lane.lane_id.to_string()),
                                    ("chunk_index", stats.chunks_transferred.to_string()),
                                    ("offset", offset.to_string()),
                                    ("bytes", chunk.to_string()),
                                ],
                            );
                            unsafe {
                                host_memcpy(
                                    pinned.ptr as *mut u8,
                                    src_ptr.add(offset as usize),
                                    chunk,
                                );
                            }
                        }
                        stats.cpu_copy_us += cpu_started.elapsed().as_secs_f64() * 1_000_000.0;
                        let acl_started = Instant::now();
                        {
                            let _acl_trace = span(
                                TraceCategory::Acl,
                                "chunk.acl_sync.h2d",
                                vec![
                                    ("event_id", event_id.to_string()),
                                    ("lane_id", lane.lane_id.to_string()),
                                    ("chunk_index", stats.chunks_transferred.to_string()),
                                    ("offset", offset.to_string()),
                                    ("bytes", chunk.to_string()),
                                ],
                            );
                            h2d_on_device(
                                dst_hbm.device_id,
                                dst_hbm.service_device_ptr,
                                offset,
                                pinned.ptr as *const _,
                                chunk,
                                false,
                            )?;
                        }
                        let acl_us = acl_started.elapsed().as_secs_f64() * 1_000_000.0;
                        stats.acl_copy_us += acl_us;
                        stats.acl_wait_us += acl_us;
                        stats.chunks_transferred += 1;
                        offset += chunk;
                    }
                }
                Ok(())
            })?;
            stats.wall_us = wall_started.elapsed().as_secs_f64() * 1_000_000.0;
            let serial_us = stats.cpu_copy_us + stats.acl_copy_us;
            if serial_us > 0.0 {
                stats.overlap_ratio = (1.0 - (stats.wall_us / serial_us)).clamp(0.0, 1.0);
            }
            stats.stream_create_count = 1;
            Ok(stats)
        },
    )
}

fn copy_hbm_to_hbm(
    event_id: u64,
    src: &RuntimeObject,
    dst: &RuntimeObject,
    nbytes: u64,
    use_async: bool,
) -> Result<(), String> {
    let src_hbm = src
        .hbm
        .as_ref()
        .ok_or_else(|| "source is not HBM".to_string())?;
    let dst_hbm = dst
        .hbm
        .as_ref()
        .ok_or_else(|| "destination is not HBM".to_string())?;
    let _trace = span(
        TraceCategory::Acl,
        "acl_copy.hbm_to_hbm",
        vec![
            ("event_id", event_id.to_string()),
            ("src_device_id", src_hbm.device_id.to_string()),
            ("dst_device_id", dst_hbm.device_id.to_string()),
            ("bytes", nbytes.to_string()),
            (
                "async",
                if use_async { "true" } else { "false" }.to_string(),
            ),
        ],
    );
    d2d_on_devices(
        dst_hbm.device_id,
        dst_hbm.service_device_ptr,
        0,
        src_hbm.device_id,
        src_hbm.service_device_ptr,
        0,
        nbytes,
        use_async,
    )
}

fn copy_ddr_to_ddr(
    event_id: u64,
    src: &RuntimeObject,
    dst: &RuntimeObject,
    nbytes: u64,
) -> Result<(), String> {
    let _trace = span(
        TraceCategory::Transfer,
        "transfer.copy.ddr_to_ddr",
        vec![
            ("event_id", event_id.to_string()),
            ("bytes", nbytes.to_string()),
            ("src_object_id", src.object.object_id.to_string()),
            ("dst_object_id", dst.object.object_id.to_string()),
        ],
    );
    with_ddr_mapping(&src.object, |src_ptr, src_len| {
        if nbytes as usize > src_len {
            return Err("transfer size exceeds mapped DDR source".to_string());
        }
        with_ddr_mapping(&dst.object, |dst_ptr, dst_len| {
            if nbytes as usize > dst_len {
                return Err("transfer size exceeds mapped DDR destination".to_string());
            }
            unsafe {
                ptr::copy_nonoverlapping(src_ptr, dst_ptr, nbytes as usize);
            }
            Ok(())
        })
    })
}

fn copy_ssd_to_ddr(
    event_id: u64,
    src: &RuntimeObject,
    dst: &RuntimeObject,
    src_offset: u64,
    dst_offset: u64,
    nbytes: u64,
) -> Result<SsdIoStats, String> {
    let _trace = span(
        TraceCategory::Transfer,
        "transfer.copy.ssd_to_ddr",
        vec![
            ("event_id", event_id.to_string()),
            ("bytes", nbytes.to_string()),
            ("src_object_id", src.object.object_id.to_string()),
            ("dst_object_id", dst.object.object_id.to_string()),
            ("src_offset", src_offset.to_string()),
            ("dst_offset", dst_offset.to_string()),
        ],
    );
    with_ddr_mapping(&dst.object, |dst_ptr, dst_len| {
        if dst_offset.checked_add(nbytes).unwrap_or(u64::MAX) > dst_len as u64 {
            return Err("transfer size exceeds mapped DDR destination".to_string());
        }
        let _io_trace = span(
            TraceCategory::Transfer,
            "ssd.read_wait",
            vec![
                ("event_id", event_id.to_string()),
                ("bytes", nbytes.to_string()),
                ("src_offset", src_offset.to_string()),
            ],
        );
        read_ssd_to_ptr(
            &src.object,
            src_offset,
            unsafe { dst_ptr.add(dst_offset as usize) },
            nbytes,
        )
    })
}

fn copy_ddr_to_ssd(
    event_id: u64,
    src: &RuntimeObject,
    dst: &RuntimeObject,
    src_offset: u64,
    dst_offset: u64,
    nbytes: u64,
) -> Result<SsdIoStats, String> {
    let _trace = span(
        TraceCategory::Transfer,
        "transfer.copy.ddr_to_ssd",
        vec![
            ("event_id", event_id.to_string()),
            ("bytes", nbytes.to_string()),
            ("src_object_id", src.object.object_id.to_string()),
            ("dst_object_id", dst.object.object_id.to_string()),
            ("src_offset", src_offset.to_string()),
            ("dst_offset", dst_offset.to_string()),
        ],
    );
    with_ddr_mapping(&src.object, |src_ptr, src_len| {
        if src_offset.checked_add(nbytes).unwrap_or(u64::MAX) > src_len as u64 {
            return Err("transfer size exceeds mapped DDR source".to_string());
        }
        let _io_trace = span(
            TraceCategory::Transfer,
            "ssd.write_wait",
            vec![
                ("event_id", event_id.to_string()),
                ("bytes", nbytes.to_string()),
                ("dst_offset", dst_offset.to_string()),
            ],
        );
        write_ptr_to_ssd(
            &dst.object,
            dst_offset,
            unsafe { src_ptr.add(src_offset as usize) } as *const u8,
            nbytes,
        )
    })
}

fn copy_ssd_to_hbm_via_ddr(
    event_id: u64,
    src: &RuntimeObject,
    dst: &RuntimeObject,
    src_offset: u64,
    dst_offset: u64,
    nbytes: u64,
) -> Result<(SsdIoStats, f64, f64), String> {
    let dst_hbm = dst
        .hbm
        .as_ref()
        .ok_or_else(|| "destination is not HBM".to_string())?;
    let relay_started = Instant::now();
    let chunk_capacity = ssd_chunk_bytes().min(nbytes.max(1));
    let mut staging = vec![0u8; chunk_capacity as usize];
    let mut ssd = SsdIoStats::default();
    let mut acl_us = 0.0;
    let mut offset = 0u64;
    while offset < nbytes {
        let chunk = (nbytes - offset).min(chunk_capacity);
        let read_stats = {
            let _trace = span(
                TraceCategory::Transfer,
                "relay.ssd_to_ddr",
                vec![
                    ("event_id", event_id.to_string()),
                    ("offset", offset.to_string()),
                    ("bytes", chunk.to_string()),
                ],
            );
            read_ssd_to_ptr(
                &src.object,
                src_offset + offset,
                staging.as_mut_ptr(),
                chunk,
            )?
        };
        ssd.merge(&read_stats);
        let acl_started = Instant::now();
        {
            let _trace = span(
                TraceCategory::Acl,
                "relay.ddr_to_hbm",
                vec![
                    ("event_id", event_id.to_string()),
                    ("device_id", dst_hbm.device_id.to_string()),
                    ("offset", offset.to_string()),
                    ("bytes", chunk.to_string()),
                ],
            );
            h2d_on_device(
                dst_hbm.device_id,
                dst_hbm.service_device_ptr,
                dst_offset + offset,
                staging.as_ptr() as *const _,
                chunk,
                true,
            )?;
        }
        acl_us += acl_started.elapsed().as_secs_f64() * 1_000_000.0;
        offset += chunk;
    }
    Ok((
        ssd,
        acl_us,
        relay_started.elapsed().as_secs_f64() * 1_000_000.0,
    ))
}

fn copy_hbm_to_ssd_via_ddr(
    event_id: u64,
    src: &RuntimeObject,
    dst: &RuntimeObject,
    src_offset: u64,
    dst_offset: u64,
    nbytes: u64,
) -> Result<(SsdIoStats, f64, f64), String> {
    let src_hbm = src
        .hbm
        .as_ref()
        .ok_or_else(|| "source is not HBM".to_string())?;
    let relay_started = Instant::now();
    let chunk_capacity = ssd_chunk_bytes().min(nbytes.max(1));
    let mut staging = vec![0u8; chunk_capacity as usize];
    let mut ssd = SsdIoStats::default();
    let mut acl_us = 0.0;
    let mut offset = 0u64;
    while offset < nbytes {
        let chunk = (nbytes - offset).min(chunk_capacity);
        let acl_started = Instant::now();
        {
            let _trace = span(
                TraceCategory::Acl,
                "relay.hbm_to_ddr",
                vec![
                    ("event_id", event_id.to_string()),
                    ("device_id", src_hbm.device_id.to_string()),
                    ("offset", offset.to_string()),
                    ("bytes", chunk.to_string()),
                ],
            );
            d2h_on_device(
                src_hbm.device_id,
                staging.as_mut_ptr() as *mut _,
                src_hbm.service_device_ptr,
                src_offset + offset,
                chunk,
                true,
            )?;
        }
        acl_us += acl_started.elapsed().as_secs_f64() * 1_000_000.0;
        let write_stats = {
            let _trace = span(
                TraceCategory::Transfer,
                "relay.ddr_to_ssd",
                vec![
                    ("event_id", event_id.to_string()),
                    ("offset", offset.to_string()),
                    ("bytes", chunk.to_string()),
                ],
            );
            write_ptr_to_ssd(&dst.object, dst_offset + offset, staging.as_ptr(), chunk)?
        };
        ssd.merge(&write_stats);
        offset += chunk;
    }
    Ok((
        ssd,
        acl_us,
        relay_started.elapsed().as_secs_f64() * 1_000_000.0,
    ))
}

fn execute_transfer_work(work: &TransferWork) -> Result<ActualTransfer, String> {
    let _trace = span(
        TraceCategory::Transfer,
        "transfer.execute",
        vec![
            ("event_id", work.event_id.to_string()),
            ("plan_id", work.plan.plan_id.to_string()),
            ("path", work.plan.path.clone()),
            ("engine", work.plan.engine.clone()),
            ("bytes", work.plan.nbytes.to_string()),
        ],
    );
    ensure_transfer_bounds(
        &work.src,
        &work.dst,
        work.plan.src_offset_bytes,
        work.plan.dst_offset_bytes,
        work.plan.nbytes,
    )?;
    let use_async = work.plan.engine.contains("async");
    let mut channel = None;
    let mut ssd = SsdIoStats::default();
    let mut direct = SsdHbmDirectStats::default();
    let mut relay_stage_count = 0;
    let mut relay_ddr_hbm_us = 0.0;
    let mut relay_total_us = 0.0;
    let mut fallback_used = work.plan.fallback_used;
    let mut fallback_reason = work.plan.fallback_reason.clone();
    let mut actual_engine_override: Option<String> = None;
    let mut actual_path_override: Option<String> = None;
    match work.plan.path.as_str() {
        "direct_ref" => {}
        "hbm_to_ddr" => {
            channel = Some(copy_hbm_to_ddr(
                work.event_id,
                &work.src,
                &work.dst,
                work.plan.nbytes,
                use_async,
                &work.plan.engine,
            )?);
        }
        "ddr_to_hbm" => {
            channel = Some(copy_ddr_to_hbm(
                work.event_id,
                &work.src,
                &work.dst,
                work.plan.nbytes,
                use_async,
                &work.plan.engine,
            )?);
        }
        "hbm_to_hbm" => copy_hbm_to_hbm(
            work.event_id,
            &work.src,
            &work.dst,
            work.plan.nbytes,
            use_async,
        )?,
        "ddr_to_ddr" => copy_ddr_to_ddr(work.event_id, &work.src, &work.dst, work.plan.nbytes)?,
        "ssd_to_ddr" => {
            ssd = copy_ssd_to_ddr(
                work.event_id,
                &work.src,
                &work.dst,
                work.plan.src_offset_bytes,
                work.plan.dst_offset_bytes,
                work.plan.nbytes,
            )?;
        }
        "ddr_to_ssd" => {
            ssd = copy_ddr_to_ssd(
                work.event_id,
                &work.src,
                &work.dst,
                work.plan.src_offset_bytes,
                work.plan.dst_offset_bytes,
                work.plan.nbytes,
            )?;
        }
        "ssd_to_hbm_via_ddr" => {
            let (stats, acl_us, total_us) = copy_ssd_to_hbm_via_ddr(
                work.event_id,
                &work.src,
                &work.dst,
                work.plan.src_offset_bytes,
                work.plan.dst_offset_bytes,
                work.plan.nbytes,
            )?;
            ssd = stats;
            relay_stage_count = 2;
            relay_ddr_hbm_us = acl_us;
            relay_total_us = total_us;
        }
        "hbm_to_ssd_via_ddr" => {
            let (stats, acl_us, total_us) = copy_hbm_to_ssd_via_ddr(
                work.event_id,
                &work.src,
                &work.dst,
                work.plan.src_offset_bytes,
                work.plan.dst_offset_bytes,
                work.plan.nbytes,
            )?;
            ssd = stats;
            relay_stage_count = 2;
            relay_ddr_hbm_us = acl_us;
            relay_total_us = total_us;
        }
        "ssd_to_hbm_direct" => {
            let dst_hbm = work
                .dst
                .hbm
                .as_ref()
                .ok_or_else(|| "destination is not HBM".to_string())?;
            let candidate = direct_candidate_from_engine(&work.plan.engine);
            match copy_ssd_to_hbm_direct(
                work.event_id,
                &work.src.object,
                dst_hbm.device_id,
                dst_hbm.service_device_ptr,
                work.plan.src_offset_bytes,
                work.plan.dst_offset_bytes,
                work.plan.nbytes,
                candidate,
            ) {
                Ok(stats) => {
                    direct = stats;
                    ssd.read_bytes = direct.read_bytes;
                    ssd.bytes = direct.bytes;
                }
                Err(reason) if engine_requests_auto_ssd_hbm(&work.plan.engine) => {
                    let (stats, acl_us, total_us) = copy_ssd_to_hbm_via_ddr(
                        work.event_id,
                        &work.src,
                        &work.dst,
                        work.plan.src_offset_bytes,
                        work.plan.dst_offset_bytes,
                        work.plan.nbytes,
                    )?;
                    ssd = stats;
                    relay_stage_count = 2;
                    relay_ddr_hbm_us = acl_us;
                    relay_total_us = total_us;
                    fallback_used = true;
                    fallback_reason = auto_direct_fallback_reason(candidate, &reason);
                    actual_engine_override = Some(ssd_hbm_relay_actual_engine());
                    actual_path_override = Some(ssd_to_hbm_relay_actual_path());
                }
                Err(reason) => return Err(reason),
            }
        }
        "hbm_to_ssd_direct" => {
            let src_hbm = work
                .src
                .hbm
                .as_ref()
                .ok_or_else(|| "source is not HBM".to_string())?;
            let candidate = direct_candidate_from_engine(&work.plan.engine);
            match copy_hbm_to_ssd_direct(
                work.event_id,
                src_hbm.device_id,
                src_hbm.service_device_ptr,
                &work.dst.object,
                work.plan.src_offset_bytes,
                work.plan.dst_offset_bytes,
                work.plan.nbytes,
                candidate,
            ) {
                Ok(stats) => {
                    direct = stats;
                    ssd.write_bytes = direct.write_bytes;
                    ssd.bytes = direct.bytes;
                }
                Err(reason) if engine_requests_auto_ssd_hbm(&work.plan.engine) => {
                    let (stats, acl_us, total_us) = copy_hbm_to_ssd_via_ddr(
                        work.event_id,
                        &work.src,
                        &work.dst,
                        work.plan.src_offset_bytes,
                        work.plan.dst_offset_bytes,
                        work.plan.nbytes,
                    )?;
                    ssd = stats;
                    relay_stage_count = 2;
                    relay_ddr_hbm_us = acl_us;
                    relay_total_us = total_us;
                    fallback_used = true;
                    fallback_reason = auto_direct_fallback_reason(candidate, &reason);
                    actual_engine_override = Some(ssd_hbm_relay_actual_engine());
                    actual_path_override = Some(hbm_to_ssd_relay_actual_path());
                }
                Err(reason) => return Err(reason),
            }
        }
        other => return Err(format!("unsupported transfer path {}", other)),
    }
    let direct_channel = channel
        .as_ref()
        .map(|channel| {
            (channel.direction == "h2d" || channel.direction == "d2h")
                && channel.pinned_footprint_bytes == 0
                && channel.event_record_count > 0
        })
        .unwrap_or(false);
    let actual_engine = if let Some(engine) = actual_engine_override {
        engine
    } else if direct_channel {
        let ddr = if work.plan.path == "ddr_to_hbm" {
            &work.src
        } else {
            &work.dst
        };
        direct_engine_name(ddr)
    } else if channel.is_some() && work.plan.engine.contains("pinned") {
        if use_async {
            "acl_pinned_async_channel".to_string()
        } else {
            "acl_pinned_sync_channel".to_string()
        }
    } else {
        work.plan.engine.clone()
    };
    let actual_path = if let Some(path) = actual_path_override {
        path
    } else if direct_channel {
        let ddr = if work.plan.path == "ddr_to_hbm" {
            &work.src
        } else {
            &work.dst
        };
        direct_path_name(
            channel
                .as_ref()
                .map(|stats| stats.direction.as_str())
                .unwrap_or(""),
            ddr,
        )
    } else if channel.is_some() {
        "memfd_pinned_hbm_channel".to_string()
    } else {
        work.plan.path.clone()
    };
    let mut actual = ActualTransfer::new(
        work.plan.nbytes,
        actual_engine,
        actual_path,
        fallback_used,
        fallback_reason,
    );
    actual.channel = channel;
    actual.ssd = ssd;
    actual.direct = direct;
    actual.relay_stage_count = relay_stage_count;
    actual.relay_ddr_hbm_us = relay_ddr_hbm_us;
    actual.relay_total_us = relay_total_us;
    Ok(actual)
}

fn finish_transfer_event(
    shared: &SharedCatalog,
    event_id: u64,
    result: Result<ActualTransfer, String>,
) -> Kv {
    let _trace = span(
        TraceCategory::Transfer,
        "transfer.complete",
        vec![("event_id", event_id.to_string())],
    );
    let (lock, cv) = &**shared;
    let mut st = lock.lock().unwrap();
    let plan = {
        let event = match st.transfer_events.get(&event_id) {
            Some(event) => event,
            None => return err("transfer event not found"),
        };
        match st.transfer_plans.get(&event.plan_id).cloned() {
            Some(plan) => plan,
            None => return err("transfer plan not found"),
        }
    };
    let mut mark_dst_ready = false;
    let mut ssd_read_delta = 0u64;
    let mut ssd_write_delta = 0u64;
    let payload = {
        let event = st.transfer_events.get_mut(&event_id).unwrap();
        event.completed_at_ns = now_ns();
        match result {
            Ok(actual) => {
                event.status = "complete".to_string();
                event.bytes_done = actual.bytes_done;
                event.actual_engine = actual.actual_engine;
                event.actual_path = actual.actual_path;
                event.fallback_used = actual.fallback_used;
                event.fallback_reason = actual.fallback_reason;
                event.ssd_io_submit_us = actual.ssd.submit_us;
                event.ssd_io_wait_us = actual.ssd.wait_us;
                event.ssd_io_bytes = actual.ssd.bytes;
                event.ssd_io_bandwidth_gib_s = actual.ssd.bandwidth_gib_s();
                event.ssd_read_bytes = actual.ssd.read_bytes;
                event.ssd_write_bytes = actual.ssd.write_bytes;
                event.relay_stage_count = actual.relay_stage_count;
                event.relay_ddr_hbm_us = actual.relay_ddr_hbm_us;
                event.relay_total_us = actual.relay_total_us;
                event.direct_candidate = actual.direct.candidate_name;
                event.direct_kind = actual.direct.direct_kind;
                event.direct_setup_us = actual.direct.setup_us;
                event.direct_register_us = actual.direct.register_us;
                event.direct_fadvise_us = actual.direct.fadvise_us;
                event.direct_readahead_us = actual.direct.readahead_us;
                event.direct_madvise_hugepage_us = actual.direct.madvise_hugepage_us;
                event.direct_madvise_willneed_us = actual.direct.madvise_willneed_us;
                event.direct_madvise_populate_us = actual.direct.madvise_populate_us;
                event.direct_pretouch_us = actual.direct.pretouch_us;
                event.direct_mlock_us = actual.direct.mlock_us;
                event.direct_acl_us = actual.direct.acl_us;
                event.direct_total_us = actual.direct.total_us;
                ssd_read_delta = actual.ssd.read_bytes;
                ssd_write_delta = actual.ssd.write_bytes;
                if let Some(channel) = actual.channel {
                    event.channel_direction = channel.direction;
                    event.channel_lane_id = channel.lane_id;
                    event.channel_device_id = channel.active_device_id;
                    event.channel_chunk_bytes = channel.chunk_bytes;
                    event.channel_chunk_count = channel.chunk_count as u64;
                    event.channel_pinned_footprint_bytes = channel.pinned_footprint_bytes;
                    event.channel_lane_wait_us = channel.lane_wait_us;
                    event.channel_chunks_transferred = channel.chunks_transferred;
                    event.channel_cpu_copy_us = channel.cpu_copy_us;
                    event.channel_acl_copy_us = channel.acl_copy_us;
                    event.channel_acl_submit_us = channel.acl_submit_us;
                    event.channel_acl_wait_us = channel.acl_wait_us;
                    event.channel_wall_us = channel.wall_us;
                    event.channel_queue_wait_us = channel.queue_wait_us;
                    event.channel_worker_execute_us = channel.worker_execute_us;
                    event.channel_overlap_ratio = channel.overlap_ratio;
                    event.channel_stream_create_count = channel.stream_create_count;
                    event.channel_event_reuse_count = channel.event_reuse_count;
                    event.channel_event_record_count = channel.event_record_count;
                    event.channel_event_wait_count = channel.event_wait_count;
                    event.channel_pipeline_overlap = channel.pipeline_overlap;
                }
                event.error_code.clear();
                event.error_message.clear();
                mark_dst_ready = true;
            }
            Err(message) => {
                event.status = "failed".to_string();
                event.bytes_done = 0;
                event.actual_engine = plan.engine.clone();
                event.actual_path = plan.path.clone();
                event.fallback_used = plan.fallback_used;
                event.fallback_reason = plan.fallback_reason.clone();
                event.error_code = "TransferExecutorError".to_string();
                event.error_message = message.replace(';', ",").replace('=', ":");
            }
        }
        let latency_us = if event.completed_at_ns > event.started_at_ns {
            (event.completed_at_ns - event.started_at_ns) as f64 / 1000.0
        } else {
            0.0
        };
        event.actual_latency_us = latency_us;
        event.actual_bandwidth_gib_s = if latency_us > 0.0 && event.bytes_done > 0 {
            (event.bytes_done as f64) / (latency_us / 1_000_000.0) / (1024.0 * 1024.0 * 1024.0)
        } else {
            0.0
        };
        eprintln!(
            "{{\"event\":\"transfer_completed\",\"event_id\":{},\"plan_id\":{},\"status\":\"{}\",\"bytes_done\":{},\"latency_us\":{:.3},\"fallback_used\":{},\"actual_engine\":\"{}\"}}",
            event.event_id,
            event.plan_id,
            event.status,
            event.bytes_done,
            event.actual_latency_us,
            event.fallback_used,
            event.actual_engine
        );
        event_payload(event)
    };
    if ssd_read_delta > 0 {
        st.ssd_read_bytes += ssd_read_delta;
        st.ssd_read_ops += 1;
    }
    if ssd_write_delta > 0 {
        st.ssd_write_bytes += ssd_write_delta;
        st.ssd_write_ops += 1;
    }
    if mark_dst_ready {
        if let Some(dst) = st.objects.get_mut(&plan.dst_object_id) {
            dst.state = "Ready".to_string();
        }
    }
    cv.notify_all();
    ok(&payload)
}

pub(crate) fn submit_transfer(req: &Kv, shared: &SharedCatalog) -> Kv {
    let (lock, cv) = &**shared;
    let mut st = lock.lock().unwrap();
    let plan_id = get_u64(req, "plan_id");
    let plan = match st.transfer_plans.get(&plan_id).cloned() {
        Some(plan) => plan,
        None => return err("transfer plan not found"),
    };
    let work = if plan.completion_kind == "immediate" {
        None
    } else {
        let src_object = match st.objects.get(&plan.src_object_id).cloned() {
            Some(object) => object,
            None => return err("source object not found"),
        };
        let dst_object = match st.objects.get(&plan.dst_object_id).cloned() {
            Some(object) => object,
            None => return err("destination object not found"),
        };
        let src = match runtime_object(&st, src_object) {
            Ok(object) => object,
            Err(e) => return err(e),
        };
        let dst = match runtime_object(&st, dst_object) {
            Ok(object) => object,
            Err(e) => return err(e),
        };
        Some((src, dst))
    };
    let event_id = st.take_next_transfer_event_id();
    let now = now_ns();
    let immediate = plan.completion_kind == "immediate";
    let event = TransferEventRecord {
        event_id,
        plan_id,
        client_id: plan.client_id,
        status: if immediate { "complete" } else { "running" }.to_string(),
        completion_kind: plan.completion_kind.clone(),
        submitted_at_ns: now,
        started_at_ns: now,
        completed_at_ns: if immediate { now } else { 0 },
        bytes_done: if immediate { plan.nbytes } else { 0 },
        actual_latency_us: 0.0,
        actual_bandwidth_gib_s: 0.0,
        actual_engine: plan.engine.clone(),
        actual_path: plan.path.clone(),
        fallback_used: plan.fallback_used,
        fallback_reason: plan.fallback_reason.clone(),
        error_code: String::new(),
        error_message: String::new(),
        channel_direction: String::new(),
        channel_lane_id: 0,
        channel_device_id: 0,
        channel_chunk_bytes: 0,
        channel_chunk_count: 0,
        channel_pinned_footprint_bytes: 0,
        channel_lane_wait_us: 0.0,
        channel_chunks_transferred: 0,
        channel_cpu_copy_us: 0.0,
        channel_acl_copy_us: 0.0,
        channel_acl_submit_us: 0.0,
        channel_acl_wait_us: 0.0,
        channel_wall_us: 0.0,
        channel_queue_wait_us: 0.0,
        channel_worker_execute_us: 0.0,
        channel_overlap_ratio: 0.0,
        channel_stream_create_count: 0,
        channel_event_reuse_count: 0,
        channel_event_record_count: 0,
        channel_event_wait_count: 0,
        channel_pipeline_overlap: false,
        ssd_io_submit_us: 0.0,
        ssd_io_wait_us: 0.0,
        ssd_io_bytes: 0,
        ssd_io_bandwidth_gib_s: 0.0,
        ssd_read_bytes: 0,
        ssd_write_bytes: 0,
        relay_stage_count: 0,
        relay_ddr_hbm_us: 0.0,
        relay_total_us: 0.0,
        direct_candidate: String::new(),
        direct_kind: String::new(),
        direct_setup_us: 0.0,
        direct_register_us: 0.0,
        direct_fadvise_us: 0.0,
        direct_readahead_us: 0.0,
        direct_madvise_hugepage_us: 0.0,
        direct_madvise_willneed_us: 0.0,
        direct_madvise_populate_us: 0.0,
        direct_pretouch_us: 0.0,
        direct_mlock_us: 0.0,
        direct_acl_us: 0.0,
        direct_total_us: 0.0,
    };
    st.transfer_events.insert(event_id, event.clone());
    eprintln!(
        "{{\"event\":\"transfer_submitted\",\"event_id\":{},\"plan_id\":{},\"status\":\"{}\",\"completion_kind\":\"{}\"}}",
        event.event_id, event.plan_id, event.status, event.completion_kind
    );
    cv.notify_all();
    let response_payload = event_payload(&event);
    drop(st);
    if let Some((src, dst)) = work {
        let work = TransferWork {
            event_id,
            plan: plan.clone(),
            src,
            dst,
        };
        let shared_for_worker = Arc::clone(shared);
        let submit_result = direct_transfer_executor().submit(
            event_id,
            plan.plan_id,
            Box::new(move |queue_wait_us| {
                let worker_started = Instant::now();
                let result = execute_transfer_work(&work);
                let worker_execute_us = worker_started.elapsed().as_secs_f64() * 1_000_000.0;
                let mut result = result;
                if let Ok(actual) = &mut result {
                    if let Some(channel) = &mut actual.channel {
                        channel.queue_wait_us = queue_wait_us;
                        channel.worker_execute_us = worker_execute_us;
                    }
                }
                let _ = finish_transfer_event(&shared_for_worker, work.event_id, result);
            }),
        );
        if let Err(error) = submit_result {
            let _ = finish_transfer_event(shared, event_id, Err(error));
        }
    }
    ok(&response_payload)
}

pub(crate) fn poll_event(req: &Kv, shared: &SharedCatalog) -> Kv {
    let (lock, _) = &**shared;
    let st = lock.lock().unwrap();
    let event_id = get_u64(req, "event_id");
    let event = match st.transfer_events.get(&event_id) {
        Some(event) => event,
        None => return err("transfer event not found"),
    };
    ok(&event_payload(event))
}

pub(crate) fn wait_event(req: &Kv, shared: &SharedCatalog) -> Kv {
    let event_id = get_u64(req, "event_id");
    let timeout_ms = get_u64(req, "timeout_ms");
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    let (lock, cv) = &**shared;
    let mut st = lock.lock().unwrap();
    loop {
        if let Some(event) = st.transfer_events.get(&event_id) {
            if !matches!(event.status.as_str(), "pending" | "running") {
                return ok(&event_payload(event));
            }
        } else {
            return err("transfer event not found");
        }
        let now = Instant::now();
        if timeout_ms == 0 || now >= deadline {
            return err("wait timeout");
        }
        let wait = deadline.saturating_duration_since(now);
        let (new_st, _) = cv.wait_timeout(st, wait).unwrap();
        st = new_st;
    }
}

pub(crate) fn cancel_event(req: &Kv, shared: &SharedCatalog) -> Kv {
    let (lock, cv) = &**shared;
    let mut st = lock.lock().unwrap();
    let event_id = get_u64(req, "event_id");
    let event = match st.transfer_events.get_mut(&event_id) {
        Some(event) => event,
        None => return err("transfer event not found"),
    };
    if matches!(event.status.as_str(), "complete" | "failed" | "cancelled") {
        return ok(&event_payload(event));
    }
    event.status = "cancelled".to_string();
    event.completed_at_ns = now_ns();
    event.error_code = "cancelled".to_string();
    event.error_message = "cancelled by client".to_string();
    let payload = event_payload(event);
    cv.notify_all();
    ok(&payload)
}
