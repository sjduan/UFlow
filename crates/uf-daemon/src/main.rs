mod acl_backend;
mod capabilities;
mod catalog;
mod common;
mod ddr_authority;
mod ddr_backend;
mod ddr_copy;
mod ddr_cost;
mod ddr_deployment_policy;
mod ddr_managed_authority;
mod ddr_process_identity;
mod ddr_ready_gate;
mod ddr_recovery_audit;
mod ddr_recovery_codec;
mod ddr_recovery_coverage;
mod ddr_recovery_durable;
#[cfg(test)]
mod ddr_recovery_fixture;
mod ddr_recovery_journal;
mod ddr_resources;
mod ddr_trust_history;
mod ddr_witness_active;
mod ddr_witness_evidence;
mod direct_transfer;
mod external_ddr;
mod managed_pool;
mod mem_service_pool;
mod pool_reconciliation;
mod pool_retained8;
mod pool_quarantine76;
mod pool_c039_prestart;
mod pool_terminal_domain;
mod object_service;
mod pool_provider;
mod pool_ranges;
mod pool_service;
mod pool_wire;
mod protocol;
mod providers;
mod router;
mod sim_object_service;
mod ssd_backend;
mod ssd_direct;
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
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, ErrorKind, Read, Write};
use std::os::fd::FromRawFd;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

#[derive(Clone)]
struct Args {
    host_pool: bool,
    pool_state_root: String,
    pool_provider_python: String,
    pool_mem_service_config: String,
    pool_budget_bytes: u64,
    device: i32,
    socket: String,
    execution_mode: String,
    block_bytes: u64,
    block_count: usize,
    startup_probe_bytes: u64,
    accept_poll_us: u64,
    initialize_external_ddr_authority: bool,
    inspect_external_ddr_evidence: bool,
    bound_external_ddr_gate: bool,
    begin_external_ddr_generation: bool,
    seal_external_ddr_generation: bool,
    external_ddr_witness_policy: Option<String>,
}

fn parse_args() -> Args {
    let mut args = Args {
        host_pool: false,
        pool_state_root: String::new(),
        pool_provider_python: String::new(),
        pool_mem_service_config: String::new(),
        pool_budget_bytes: 256 * 1024 * 1024,
        device: env::var("UF_TARGET_DEVICE")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(0),
        socket: "/tmp/uf_phasea03.sock".to_string(),
        execution_mode: env::var("UF_EXECUTION_MODE").unwrap_or_else(|_| "physical".to_string()),
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
        initialize_external_ddr_authority: false,
        inspect_external_ddr_evidence: false,
        bound_external_ddr_gate: false,
        begin_external_ddr_generation: false,
        seal_external_ddr_generation: false,
        external_ddr_witness_policy: None,
    };
    args.accept_poll_us = args.accept_poll_us.max(100);
    let mut iter = env::args().skip(1);
    while let Some(key) = iter.next() {
        let mut value = || {
            iter.next()
                .unwrap_or_else(|| panic!("missing value for {}", key))
        };
        match key.as_str() {
            "--host-pool" => args.host_pool = true,
            "--pool-state-root" => args.pool_state_root = value(),
            "--pool-provider-python" => args.pool_provider_python = value(),
            "--pool-mem-service-config" => args.pool_mem_service_config = value(),
            "--pool-budget-bytes" => {
                args.pool_budget_bytes = value().parse().expect("invalid pool budget")
            }
            "--device" => args.device = value().parse().expect("invalid --device"),
            "--socket" => args.socket = value(),
            "--execution-mode" => args.execution_mode = value(),
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
            "--initialize-external-ddr-authority" => args.initialize_external_ddr_authority = true,
            "--inspect-external-ddr-evidence" => args.inspect_external_ddr_evidence = true,
            "--bound-external-ddr-gate" => args.bound_external_ddr_gate = true,
            "--begin-external-ddr-generation" => args.begin_external_ddr_generation = true,
            "--seal-external-ddr-generation" => args.seal_external_ddr_generation = true,
            "--external-ddr-witness-policy" => {
                if args.external_ddr_witness_policy.replace(value()).is_some() {
                    panic!("duplicate --external-ddr-witness-policy");
                }
            }
            _ => panic!("unknown arg: {}", key),
        }
    }
    if !matches!(
        args.execution_mode.as_str(),
        "physical" | "mixed" | "simulated"
    ) {
        panic!(
            "invalid --execution-mode {}; expected physical, mixed, or simulated",
            args.execution_mode
        );
    }
    if args.external_ddr_witness_policy.is_some() && !args.initialize_external_ddr_authority {
        panic!("witness policy is only allowed for explicit authority initialization");
    }
    if args.initialize_external_ddr_authority && args.inspect_external_ddr_evidence {
        panic!("authority initialization and evidence inspection are separate operations");
    }
    if args.bound_external_ddr_gate
        && (args.initialize_external_ddr_authority || args.inspect_external_ddr_evidence)
    {
        panic!("bound broker gate is separate from authority deployment operations");
    }
    if args.begin_external_ddr_generation
        && (args.initialize_external_ddr_authority
            || args.inspect_external_ddr_evidence
            || args.bound_external_ddr_gate
            || args.seal_external_ddr_generation)
    {
        panic!("generation deployment must be a separate operation");
    }
    if args.seal_external_ddr_generation
        && (args.initialize_external_ddr_authority
            || args.inspect_external_ddr_evidence
            || args.bound_external_ddr_gate)
    {
        panic!("generation seal must be a separate operation");
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
    if args.execution_mode == "simulated" {
        status.probe_attempted = false;
        eprintln!(
            "{{\"event\":\"daemon_hbm_probe_skipped\",\"device_id\":{},\"reason\":\"simulated_execution_mode\"}}",
            args.device
        );
        return status;
    }
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
    // A persistent idle control connection must not keep shutdown from joining
    // this handler forever. Preserve partial frames across timeouts, however:
    // a timeout is not a request boundary and must not discard input bytes.
    if stream
        .set_read_timeout(Some(Duration::from_millis(100)))
        .and_then(|_| stream.set_write_timeout(Some(Duration::from_secs(5))))
        .is_err()
    {
        eprintln!("control handler could not configure socket timeouts");
        return;
    }
    let peer = stream.try_clone().expect("clone stream");
    let mut reader = BufReader::new(peer);
    let mut writer = stream;
    let mut line = String::new();
    loop {
        if shutdown_requested(&shared) {
            break;
        }
        match reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {
                if shutdown_requested(&shared) {
                    break;
                }
                capture_control_frame(b'Q', line.as_bytes());
                let req = decode(&line);
                let resp = handle_request(req, &shared);
                let encoded = encode(&resp);
                capture_control_frame(b'R', encoded.as_bytes());
                if writer.write_all(encoded.as_bytes()).is_err() {
                    break;
                }
                line.clear();
            }
            Err(e)
                if matches!(
                    e.kind(),
                    ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::Interrupted
                ) => {}
            Err(_) => break,
        }
    }
}

/// Reap completed handlers without losing failures; at shutdown join every
/// accepted handler before the external broker may record a drain/checkpoint.
fn join_control_handlers(handlers: &mut Vec<thread::JoinHandle<()>>, all: bool) -> bool {
    let mut joined = true;
    let mut active = Vec::new();
    for handler in handlers.drain(..) {
        if all || handler.is_finished() {
            joined &= handler.join().is_ok();
        } else {
            active.push(handler);
        }
    }
    *handlers = active;
    joined
}

fn capture_control_frame(direction: u8, payload: &[u8]) {
    let path = match env::var("UF_DAEMON_CONTROL_CAPTURE") {
        Ok(path) if !path.is_empty() => path,
        _ => return,
    };
    let mut record = Vec::with_capacity(payload.len() + 12);
    record.extend_from_slice(b"UFCTL01");
    record.push(direction);
    record.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    record.extend_from_slice(payload);
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = file.write_all(&record);
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

/// Only the direct-parent control socket is inherited, never the authority
/// lock or caller-selected evidence. The parent is checked against the pinned
/// deployment policy and BIND after the broker has independently locked.
fn parent_ready_gate() -> std::io::Result<ddr_ready_gate::ParentReadyGate> {
    let parent = ddr_process_identity::observe(
        unsafe { libc::getppid() },
        unsafe { libc::geteuid() },
        unsafe { libc::getegid() },
    )?;
    // This mode owns stdin. SO_PEERCRED and parent identity checks reject an
    // unrelated descriptor or process before any managed side effect.
    let stream = unsafe { UnixStream::from_raw_fd(0) };
    ddr_ready_gate::ParentReadyGate::arrive(stream, parent, Duration::from_secs(15))
}

/// Explicit trusted deployment operation. A live pinned witness proposes a
/// new generation only after the binder has independently acquired the fixed
/// root lock and the child has reached its gate. No alternate root, expected
/// manifest, recovery input or trust policy is accepted from this channel.
fn begin_bound_generation(endpoint: &str) -> std::io::Result<()> {
    use ddr_recovery_coverage::{fields, parse_line, s};
    let mut gate = parent_ready_gate()?;
    let parent = gate.parent().clone();
    let external = external_ddr::ExternalDdrState::new();
    let mut authority = external.open_binding_authority(endpoint)?;
    gate.announce_locked()?;
    let mut control = gate.wait_release()?;
    let mut raw = Vec::new();
    let mut input = BufReader::new(control.try_clone()?).take(4097);
    input.read_until(b'\n', &mut raw)?;
    if raw.len() > 4096 {
        return Err(std::io::Error::new(
            ErrorKind::InvalidData,
            "binding proposal too large",
        ));
    }
    let proposal = parse_line(&raw)?;
    fields(&proposal, &["broker", "evidence_dir", "generation"])?;
    let generation = s(&proposal, "generation")?;
    let evidence = std::path::Path::new(s(&proposal, "evidence_dir")?);
    let live = authority.prepare_live_binding(generation, s(&proposal, "broker")?, evidence)?;
    if live.generation().witness != parent {
        return Err(std::io::Error::new(
            ErrorKind::PermissionDenied,
            "binding proposer is not the pinned witness parent",
        ));
    }
    authority.commit_live_binding(generation, &live)?;
    // Closing this independent description before acknowledgement is part of
    // the handoff. A successor can obtain the lock but cannot admit while the
    // bound generation is unsealed; the gated broker must acquire it itself.
    drop(authority);
    control.write_all(b"UFLOW-BINDER-RELEASED\n")
}

/// Evidence and final head are derived solely from the existing locked BIND;
/// its original live witness must be this sealer's direct parent.
fn seal_bound_generation(endpoint: &str) -> std::io::Result<()> {
    let mut gate = parent_ready_gate()?;
    let parent = gate.parent().clone();
    let external = external_ddr::ExternalDdrState::new();
    let mut authority = external.open_binding_authority(endpoint)?;
    gate.announce_locked()?;
    let mut control = gate.wait_release()?;
    authority.seal_bound_quarantine(&parent)?;
    drop(authority);
    control.write_all(b"UFLOW-SEAL-RELEASED\n")
}

fn main() {
    // Private explicit administrative modes run before every backend/trace/
    // Catalog startup. Neither missing endpoints nor a human flag mint ACKs.
    let private_args=std::env::args().skip(1).collect::<Vec<_>>();
    if private_args.iter().any(|v|v=="--pool-terminal-domain-loss"||v=="--pool-shutdown-report"||v=="--pool-terminal-domain-verify"||v=="--pool-terminal-execution-check") {
        let report=match private_args.as_slice() {
            [mode] if mode=="--pool-terminal-domain-loss"=>pool_terminal_domain::production_terminal_only(),
            [mode] if mode=="--pool-terminal-domain-verify"=>pool_terminal_domain::production_verify_readonly(),
            [mode] if mode=="--pool-shutdown-report"=>pool_terminal_domain::production_shutdown_report(),
            [mode,manifest,path,pin,sha] if mode=="--pool-terminal-domain-loss"&&manifest=="--execution-manifest"&&pin=="--execution-manifest-sha256"=>
                pool_terminal_domain::production_terminal_manifest(std::path::Path::new(path),sha),
            [mode,manifest,path,pin,sha,phase_flag,phase] if mode=="--pool-terminal-execution-check"&&manifest=="--execution-manifest"&&pin=="--execution-manifest-sha256"&&phase_flag=="--phase"=>
                pool_terminal_domain::production_execution_check(std::path::Path::new(path),sha,phase),
            _=>Err("production-not-ready: private mode requires its exact read-only flag or resolved execution manifest; no original action".into())
        };
        match report {Ok(value)=>println!("{}",value),Err(error)=>{eprintln!("{error}");std::process::exit(1);}}
        return;
    }
    trace::collector().init_from_env();
    let args = parse_args();
    if args.host_pool {
        if args.pool_state_root.is_empty()
            || (args.pool_provider_python.is_empty() == args.pool_mem_service_config.is_empty())
            || args.block_count != 0
            || args.initialize_external_ddr_authority
            || args.inspect_external_ddr_evidence
            || args.bound_external_ddr_gate
            || args.begin_external_ddr_generation
            || args.seal_external_ddr_generation
        {
            eprintln!("host-pool is a separate explicit mode; fresh task state/provider required");
            std::process::exit(1);
        }
        if let Err(error) = pool_service::run(
            &args.socket,
            std::path::Path::new(&args.pool_state_root),
            &args.pool_provider_python,
            args.pool_budget_bytes,
            if args.pool_mem_service_config.is_empty() { None }
            else { Some(std::path::Path::new(&args.pool_mem_service_config)) },
        ) {
            eprintln!("managed host pool stopped with unresolved state: {error}");
            std::process::exit(1);
        }
        return; // No startup_catalog, backend init/finalize, or E10 authority access.
    }
    // Multi-generation deployment/recovery remains an internal experiment.
    // Ordinary brokers never need a witness; no command-line path may mutate
    // the trust chain or turn a dirty root into available capacity.
    if args.bound_external_ddr_gate
        || args.begin_external_ddr_generation
        || args.seal_external_ddr_generation
    {
        eprintln!("experimental external DDR generation operations are disabled");
        std::process::exit(1);
    }
    if args.inspect_external_ddr_evidence {
        let external = external_ddr::ExternalDdrState::new();
        match external.inspect_authority_evidence(&args.socket) {
            Ok(report) => println!("{report}"),
            Err(e) => {
                eprintln!("external DDR evidence inspection failed: {}", e);
                std::process::exit(1);
            }
        }
        return;
    }
    if args.initialize_external_ddr_authority {
        let external = external_ddr::ExternalDdrState::new();
        let policy = args
            .external_ddr_witness_policy
            .as_deref()
            .map(std::path::Path::new);
        if let Err(e) = external.initialize_authority(&args.socket, policy) {
            eprintln!("external DDR authority initialization failed: {}", e);
            std::process::exit(1);
        }
        eprintln!("external DDR authority initialized for {}", args.socket);
        return;
    }
    if args.begin_external_ddr_generation {
        if let Err(e) = begin_bound_generation(&args.socket) {
            eprintln!("external DDR generation deployment rejected: {}", e);
            std::process::exit(1);
        }
        return;
    }
    if args.seal_external_ddr_generation {
        if let Err(e) = seal_bound_generation(&args.socket) {
            eprintln!("external DDR generation seal rejected: {}", e);
            std::process::exit(1);
        }
        return;
    }
    let mut bound_control = if args.bound_external_ddr_gate {
        match parent_ready_gate().and_then(|gate| gate.wait_release()) {
            Ok(stream) => Some(stream),
            Err(e) => {
                eprintln!("external DDR bound ready gate rejected: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        None
    };
    let mut catalog = startup_catalog(&args);
    if let Some(control) = bound_control.as_mut() {
        if let Err(e) = catalog
            .external_ddr
            .open_bound_quarantined_authority(&args.socket)
            .and_then(|()| control.write_all(b"UFLOW-BOUND-QUARANTINED-1\n"))
        {
            eprintln!("external DDR bound quarantine rejected: {}", e);
            std::process::exit(1);
        }
    } else if let Err(e) = catalog.external_ddr.open_authority(&args.socket) {
        eprintln!("external DDR broker disabled: {}", e);
    }
    // Never unlink a possibly live or foreign control socket at startup.
    let listener = UnixListener::bind(&args.socket).unwrap_or_else(|e| {
        panic!("bind {} failed: {}", args.socket, e);
    });
    let control_metadata = fs::symlink_metadata(&args.socket).expect("stat control socket");
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
    let has_external_authority = {
        let (lock, _) = &*shared;
        lock.lock().unwrap().external_ddr.has_authority()
    };
    let external_broker = if has_external_authority {
        match external_ddr::start(&args.socket, Arc::clone(&shared)) {
            Ok(broker) => Some(broker),
            Err(e) => {
                eprintln!("external DDR broker disabled: {}", e);
                shared.0.lock().unwrap().external_ddr.disable_authority();
                None
            }
        }
    } else {
        None
    };
    let mut control_handlers = Vec::new();
    let mut control_drained = true;
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
                match thread::Builder::new().spawn(move || handle_client(stream, shared_catalog)) {
                    Ok(handler) => control_handlers.push(handler),
                    Err(e) => eprintln!("control handler spawn failed: {}", e),
                }
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_micros(args.accept_poll_us));
            }
            Err(e) => {
                eprintln!("accept error: {}", e);
                break;
            }
        }
        control_drained &= join_control_handlers(&mut control_handlers, false);
    }
    {
        let (lock, cv) = &*shared;
        lock.lock().unwrap().shutdown_requested = true;
        cv.notify_all();
    }
    drop(listener);
    control_drained &= join_control_handlers(&mut control_handlers, true);
    let external_shutdown = match external_broker {
        Some(broker) => broker.stop(control_drained),
        None if control_drained => Ok(()),
        None => Err("control handler drain failed".to_owned()),
    };
    cleanup_blocks(&shared);
    if fs::symlink_metadata(&args.socket).is_ok_and(|meta| {
        meta.file_type().is_socket()
            && meta.dev() == control_metadata.dev()
            && meta.ino() == control_metadata.ino()
    }) {
        let _ = fs::remove_file(&args.socket);
    }
    if let Err(error) = external_shutdown {
        eprintln!(
            "{{\"event\":\"daemon_shutdown_failed\",\"error\":\"{}\"}}",
            json_escape(&error)
        );
        std::process::exit(1);
    }
    eprintln!("{{\"event\":\"daemon_shutdown_complete\"}}");
}

#[cfg(test)]
mod control_shutdown_tests {
    use super::*;
    use std::sync::mpsc;

    #[test]
    fn persistent_control_handlers_preserve_partial_frames_and_join_on_shutdown() {
        let shared: SharedCatalog =
            Arc::new((Mutex::new(Catalog::new(0, Vec::new())), Condvar::new()));
        let (mut client, server) = UnixStream::pair().unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let client_handler = {
            let shared = Arc::clone(&shared);
            thread::spawn(move || handle_client(server, shared))
        };
        let (_idle_client, idle_server) = UnixStream::pair().unwrap();
        let idle_handler = {
            let shared = Arc::clone(&shared);
            thread::spawn(move || handle_client(idle_server, shared))
        };

        client.write_all(b"op=Get").unwrap();
        thread::sleep(Duration::from_millis(250));
        client.write_all(b"Capabilities\n").unwrap();
        let mut response = String::new();
        BufReader::new(client.try_clone().unwrap())
            .read_line(&mut response)
            .unwrap();
        assert_eq!(uf_core::get(&decode(&response), "status"), "ok");

        let (mut stopper, stop_server) = UnixStream::pair().unwrap();
        stopper
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let stop_handler = {
            let shared = Arc::clone(&shared);
            thread::spawn(move || handle_client(stop_server, shared))
        };
        stopper.write_all(b"op=ShutdownDaemon\n").unwrap();
        response.clear();
        BufReader::new(stopper).read_line(&mut response).unwrap();
        assert_eq!(uf_core::get(&decode(&response), "status"), "ok");
        let mut handlers = vec![client_handler, idle_handler, stop_handler];
        assert!(join_control_handlers(&mut handlers, true));
        assert!(handlers.is_empty());
    }

    #[test]
    fn joining_waits_for_every_handler_and_remembers_panics() {
        let (release, blocked) = mpsc::channel::<()>();
        let mut handlers = vec![
            thread::spawn(|| panic!("intentional handler failure")),
            thread::spawn(move || blocked.recv().unwrap()),
        ];
        let (done, result) = mpsc::channel();
        let joiner = thread::spawn(move || {
            done.send(join_control_handlers(&mut handlers, true))
                .unwrap();
            assert!(handlers.is_empty());
        });
        assert!(result.recv_timeout(Duration::from_millis(100)).is_err());
        release.send(()).unwrap();
        assert!(!result.recv_timeout(Duration::from_secs(2)).unwrap());
        joiner.join().unwrap();
    }
}
