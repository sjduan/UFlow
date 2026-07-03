mod acl_backend;
mod capabilities;
mod catalog;
mod common;
mod ddr_backend;
mod direct_transfer;
mod object_service;
mod protocol;
mod router;
mod stats_service;
mod trace;
mod trace_api;
mod transfer_channel;
mod transfer_executor;
mod transfer_planner;

use acl_backend::{HbmBackend, HbmMemInfo, NpuHbmAclBackend};
use catalog::{Catalog, HbmRuntimeStatus, SharedCatalog};
use protocol::handle_request;
use trace::{span, TraceCategory};
use uf_core::{decode, encode};

use std::env;
use std::fs;
use std::io::{BufRead, BufReader, ErrorKind, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

#[derive(Clone)]
struct Args {
    device: i32,
    socket: String,
    block_bytes: u64,
    block_count: usize,
    startup_probe_bytes: u64,
    accept_poll_us: u64,
}

fn parse_args() -> Args {
    let mut args = Args {
        device: env::var("UF_TARGET_DEVICE")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(0),
        socket: "/tmp/uf_phasea03.sock".to_string(),
        block_bytes: 1 << 20,
        block_count: 0,
        startup_probe_bytes: env::var("UF_HBM_STARTUP_PROBE_BYTES")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(1 << 20),
        accept_poll_us: env::var("UF_DAEMON_ACCEPT_POLL_US")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(1_000),
    };
    args.accept_poll_us = args.accept_poll_us.max(100);
    let mut iter = env::args().skip(1);
    while let Some(key) = iter.next() {
        let mut value = || {
            iter.next()
                .unwrap_or_else(|| panic!("missing value for {}", key))
        };
        match key.as_str() {
            "--device" => args.device = value().parse().expect("invalid --device"),
            "--socket" => args.socket = value(),
            "--block-bytes" => args.block_bytes = value().parse().expect("invalid --block-bytes"),
            "--block-count" => args.block_count = value().parse().expect("invalid --block-count"),
            "--startup-probe-bytes" => {
                args.startup_probe_bytes = value().parse().expect("invalid --startup-probe-bytes")
            }
            "--accept-poll-us" => {
                args.accept_poll_us = value()
                    .parse::<u64>()
                    .expect("invalid --accept-poll-us")
                    .max(100)
            }
            _ => panic!("unknown arg: {}", key),
        }
    }
    args
}

fn startup_hbm_probe(args: &Args) -> HbmRuntimeStatus {
    let _trace = span(
        TraceCategory::Backend,
        "backend.startup_hbm_probe",
        vec![
            ("device_id", args.device.to_string()),
            ("bytes", args.startup_probe_bytes.to_string()),
        ],
    );
    let mut status = HbmRuntimeStatus {
        probe_attempted: args.startup_probe_bytes > 0,
        probe_bytes: args.startup_probe_bytes,
        ..HbmRuntimeStatus::default()
    };
    if args.startup_probe_bytes == 0 {
        eprintln!(
            "{{\"event\":\"daemon_hbm_probe_skipped\",\"device_id\":{},\"reason\":\"startup_probe_bytes_is_zero\"}}",
            args.device
        );
        return status;
    }
    let mut backend = NpuHbmAclBackend::default();
    if let Err(e) = backend.init(args.device) {
        status.probe_error = e.clone();
        status.last_error = e.clone();
        eprintln!(
            "{{\"event\":\"daemon_hbm_probe_failed\",\"device_id\":{},\"stage\":\"init\",\"requested_bytes\":{},\"error\":\"{}\"}}",
            args.device,
            args.startup_probe_bytes,
            json_escape(&e)
        );
        return status;
    }
    let granularity = match backend.allocation_granularity(args.device) {
        Ok(granularity) => granularity,
        Err(e) => {
            status.probe_error = e.clone();
            status.last_error = e.clone();
            eprintln!(
                "{{\"event\":\"daemon_hbm_probe_failed\",\"device_id\":{},\"stage\":\"granularity\",\"requested_bytes\":{},\"error\":\"{}\"}}",
                args.device,
                args.startup_probe_bytes,
                json_escape(&e)
            );
            return status;
        }
    };
    let mem = backend
        .mem_info(args.device)
        .unwrap_or_else(|_| HbmMemInfo::default());
    eprintln!(
        "{{\"event\":\"daemon_hbm_probe_start\",\"device_id\":{},\"requested_bytes\":{},\"granularity\":{},\"hbm_free\":{},\"hbm_total\":{}}}",
        args.device, args.startup_probe_bytes, granularity, mem.free_bytes, mem.total_bytes
    );

    let allocation = match backend.allocate(args.device, args.startup_probe_bytes) {
        Ok(allocation) => allocation,
        Err(e) => {
            status.probe_error = e.clone();
            status.last_error = e.clone();
            eprintln!(
                "{{\"event\":\"daemon_hbm_probe_failed\",\"device_id\":{},\"stage\":\"allocate\",\"requested_bytes\":{},\"error\":\"{}\"}}",
                args.device,
                args.startup_probe_bytes,
                json_escape(&e)
            );
            return status;
        }
    };
    status.probe_actual_bytes = allocation.actual_bytes;
    eprintln!(
        "{{\"event\":\"daemon_hbm_probe_allocated\",\"device_id\":{},\"raw_handle_id\":{},\"requested_bytes\":{},\"actual_bytes\":{},\"granularity\":{},\"service_mapping_id\":{},\"service_device_ptr\":\"0x{:x}\"}}",
        allocation.device_id,
        allocation.raw_handle_id,
        allocation.requested_bytes,
        allocation.actual_bytes,
        allocation.granularity,
        allocation.service_mapping_id,
        allocation.service_device_ptr
    );
    if let Err(e) = backend.free(allocation) {
        status.probe_error = e.clone();
        status.last_error = e.clone();
        eprintln!(
            "{{\"event\":\"daemon_hbm_probe_failed\",\"device_id\":{},\"stage\":\"free\",\"requested_bytes\":{},\"actual_bytes\":{},\"error\":\"{}\"}}",
            args.device,
            args.startup_probe_bytes,
            status.probe_actual_bytes,
            json_escape(&e)
        );
        return status;
    }
    status.available = true;
    eprintln!(
        "{{\"event\":\"daemon_hbm_probe_released\",\"device_id\":{},\"requested_bytes\":{},\"actual_bytes\":{}}}",
        args.device, args.startup_probe_bytes, status.probe_actual_bytes
    );
    status
}

fn startup_catalog(args: &Args) -> Catalog {
    let mut catalog = Catalog::new(args.device, Vec::new());
    catalog.hbm = startup_hbm_probe(args);
    if args.block_count > 0 {
        eprintln!(
            "{{\"event\":\"daemon_prealloc_ignored\",\"device_id\":{},\"requested_block_count\":{},\"block_bytes\":{},\"reason\":\"phasee04_lazy_allocation\"}}",
            args.device,
            args.block_count,
            args.block_bytes
        );
    }
    catalog
}

fn json_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

fn handle_client(stream: UnixStream, shared: SharedCatalog) {
    let peer = stream.try_clone().expect("clone stream");
    let mut reader = BufReader::new(peer);
    let mut writer = stream;
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {
                let req = decode(&line);
                let resp = handle_request(req, &shared);
                let encoded = encode(&resp);
                if writer.write_all(encoded.as_bytes()).is_err() {
                    break;
                }
            }
            Err(_) => break,
        }
    }
}

fn cleanup_blocks(shared: &SharedCatalog) {
    let blocks = {
        let (lock, _) = &**shared;
        let st = lock.lock().unwrap();
        st.blocks.clone()
    };
    let mut backend = NpuHbmAclBackend::default();
    for block in blocks {
        let _ = backend.free(block.allocation);
    }
    let _ = backend.finalize();
}

fn shutdown_requested(shared: &SharedCatalog) -> bool {
    let (lock, _) = &**shared;
    let st = lock.lock().unwrap();
    st.shutdown_requested
}

fn main() {
    trace::collector().init_from_env();
    let args = parse_args();
    let catalog = startup_catalog(&args);
    let _ = fs::remove_file(&args.socket);
    let listener = UnixListener::bind(&args.socket).unwrap_or_else(|e| {
        panic!("bind {} failed: {}", args.socket, e);
    });
    listener
        .set_nonblocking(true)
        .expect("set listener nonblocking");
    eprintln!(
        "{{\"event\":\"daemon_started\",\"device_id\":{},\"socket\":\"{}\",\"block_count\":0,\"startup_probe_bytes\":{},\"accept_poll_us\":{},\"hbm_available\":{},\"hbm_probe_error\":\"{}\"}}",
        args.device,
        args.socket,
        args.startup_probe_bytes,
        args.accept_poll_us,
        catalog.hbm.available,
        json_escape(&catalog.hbm.probe_error)
    );
    let shared: SharedCatalog = Arc::new((Mutex::new(catalog), Condvar::new()));
    loop {
        if shutdown_requested(&shared) {
            eprintln!(
                "{{\"event\":\"daemon_shutdown_begin\",\"socket\":\"{}\"}}",
                args.socket
            );
            break;
        }
        match listener.accept() {
            Ok((stream, _addr)) => {
                let shared_catalog = Arc::clone(&shared);
                thread::spawn(move || handle_client(stream, shared_catalog));
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_micros(args.accept_poll_us));
            }
            Err(e) => {
                eprintln!("accept error: {}", e);
                break;
            }
        }
    }
    cleanup_blocks(&shared);
    let _ = fs::remove_file(&args.socket);
    eprintln!("{{\"event\":\"daemon_shutdown_complete\"}}");
}
