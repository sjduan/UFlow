//! Authenticated Linux adapter for the existing planner, executor and catalog
//! records. Discovery/probe I/O never holds the catalog mutex. No second ledger.
use super::*;
use crate::catalog::{TransferEventRecord, TransferPlanRecord};
use crate::common::{event_payload, now_ns, transfer_plan_payload};
use crate::trace::{record_instant, TraceCategory};
use std::time::Instant;
use uf_core::topology::ddr::diagnostic as diag;
use uf_core::topology::ddr::{CapabilityEvidence, DdrSnapshot};

#[derive(Clone, Debug)]
pub(crate) struct ExternalPlan {
    token: String,
    request: capability::CopyRequest,
    snapshot: DdrSnapshot,
    evidence: CapabilityEvidence,
    peer: Peer,
    submitted: Option<u64>,
    validation_wall_seconds: f64,
}

impl ExternalPlan {
    pub(crate) fn payload(&self) -> Vec<(&'static str, String)> {
        vec![
            ("plan_token", self.token.clone()),
            (
                "snapshot_version",
                format!(
                    "s-{}-{}-{}",
                    self.snapshot.epoch,
                    self.snapshot.topology_generation,
                    self.snapshot.capability_generation
                ),
            ),
            (
                "capability_version",
                String::from_utf8_lossy(&self.evidence.proof).into_owned(),
            ),
            ("cost_version", "unknown".into()),
            ("estimated_latency_us", "unknown".into()),
            ("estimated_bandwidth_gib_s", "unknown".into()),
            ("setup_cost_us", "unknown".into()),
            ("effort", "unknown".into()),
            ("automatic_ranking", "disabled".into()),
            (
                "validation_wall_seconds",
                self.validation_wall_seconds.to_string(),
            ),
            (
                "validation_target_bytes",
                get(&self.request.target, "size_bytes").into(),
            ),
            (
                "source_identity_json",
                serde_json::to_string(&self.request.source).unwrap(),
            ),
            (
                "target_identity_json",
                serde_json::to_string(&self.request.target).unwrap(),
            ),
        ]
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ExternalEvent {
    pub token: String,
    pub planned_path: String,
    pub planned_engine: String,
    pub snapshot_version: String,
    pub work_id: Option<u64>,
    pub view_id: Option<u64>,
    pub mapped_bytes: u64,
    pub accessed_bytes: u64,
    pub copied_bytes: u64,
    pub phase: String,
    pub drained: bool,
    pub cancel_requested: bool,
    pub execute_seconds: Option<f64>,
    pub queue_seconds: Option<f64>,
    pub validation_wall_seconds: f64,
}

impl ExternalEvent {
    pub(crate) fn payload(&self) -> Vec<(&'static str, String)> {
        vec![
            ("event_token", self.token.clone()),
            ("planned_path", self.planned_path.clone()),
            ("planned_engine", self.planned_engine.clone()),
            ("snapshot_version", self.snapshot_version.clone()),
            ("cost_version", "unknown".into()),
            ("automatic_ranking", "disabled".into()),
            ("mapped_bytes", self.mapped_bytes.to_string()),
            ("accessed_bytes", self.accessed_bytes.to_string()),
            (
                "accessed_provenance",
                "cooperative-sdk-read-report-not-capability-evidence".into(),
            ),
            ("copied_bytes", self.copied_bytes.to_string()),
            ("phase", self.phase.clone()),
            ("drained", self.drained.to_string()),
            ("cancel_requested", self.cancel_requested.to_string()),
            (
                "cleanup",
                if self.drained {
                    if self.view_id.is_some() {
                        "view-release-confirmed"
                    } else {
                        "worker-drained"
                    }
                } else {
                    "retained-until-real-drain"
                }
                .into(),
            ),
            (
                "execute_seconds",
                self.execute_seconds
                    .map_or("unknown".into(), |n| n.to_string()),
            ),
            (
                "queue_seconds",
                self.queue_seconds
                    .map_or("unknown".into(), |n| n.to_string()),
            ),
            (
                "validation_wall_seconds",
                self.validation_wall_seconds.to_string(),
            ),
            ("business_read_complete", "not-implied-by-map-ready".into()),
        ]
    }
}

pub(crate) fn is_transfer_op(op: &str) -> bool {
    matches!(
        op,
        "EstimateCost"
            | "PlanTransfer"
            | "SubmitTransfer"
            | "PollEvent"
            | "WaitEvent"
            | "CancelEvent"
            | "RecordExternalAccess"
    )
}

fn wire_error(error: String) -> Kv {
    err(error.replace(';', ",").replace('=', ":"))
}

fn diagnostic_identity(fields: &Kv) -> Kv {
    // No arbitrary extra caller keys, in particular no token/proof.
    [
        "host_id",
        "boot_id",
        "broker_instance",
        "owner_incarnation",
        "pool_generation",
        "resource_generation",
        "object_id",
        "placement_id",
        "binding_id",
        "owner_lease_id",
        "size_bytes",
        "purpose",
    ]
    .into_iter()
    .map(|key| (key.into(), get(fields, key).chars().take(128).collect()))
    .collect()
}

#[cfg(test)]
#[test]
fn diagnostics_never_copy_arbitrary_bearer_fields() {
    let fields = uf_core::kv(&[
        ("object_id", "42".into()),
        ("plan_token", "secret-plan".into()),
        ("event_token", "secret-event".into()),
        ("proof", "secret-proof".into()),
    ]);
    let safe = diagnostic_identity(&fields);
    assert_eq!(get(&safe, "object_id"), "42");
    assert!(!format!("{safe:?}").contains("secret"));
}

fn discovery(
    shared: &SharedCatalog,
) -> Result<Arc<std::sync::Mutex<uf_core::topology::ddr::DdrDiscovery>>, String> {
    shared
        .0
        .lock()
        .map_err(|_| "catalog poisoned")?
        .external_ddr
        .discovery
        .clone()
        .ok_or_else(|| "external discovery unavailable".into())
}

pub(crate) fn plan(req: &Kv, peer: Peer, shared: &SharedCatalog, save: bool) -> Kv {
    let result = plan_result(req, peer, shared, save);
    diag::note(format!(
        "plan.terminal ok={} plan_saved={} business_admitted=false",
        result.is_ok(),
        save && result.is_ok()
    ));
    result.unwrap_or_else(wire_error)
}

fn plan_result(req: &Kv, peer: Peer, shared: &SharedCatalog, save: bool) -> Result<Kv, String> {
    // No auto mode while costs are unknown. Never silently choose a fallback.
    if get(req, "mode") != "external_copy" || get(req, "operation") != "copy" {
        return Err(
            "explicit external_copy/copy required; unknown cost disables automatic ranking".into(),
        );
    }
    let request = capability::CopyRequest {
        source: serde_json::from_str(get(req, "source_json")).map_err(|e| e.to_string())?,
        target: serde_json::from_str(get(req, "target_json")).map_err(|e| e.to_string())?,
        source_offset: number(req, "src_offset_bytes")?,
        target_offset: number(req, "dst_offset_bytes")?,
        bytes: number(req, "nbytes")?,
    };
    // Only complete resource descriptors, never bearer plan/event tokens.
    diag::note(format!(
        "copy.identity peer_pid={} source={:?} target={:?} src_offset={} dst_offset={} bytes={}",
        peer.pid,
        diagnostic_identity(&request.source),
        diagnostic_identity(&request.target),
        request.source_offset,
        request.target_offset,
        request.bytes
    ));
    let observer = discovery(shared)?;
    let mut observer = {
        let _wait = diag::span("discovery.wait");
        observer.lock().map_err(|_| "discovery poisoned")?
    };
    let before = {
        let _span = diag::span("plan.before-snapshot");
        observer.snapshot()?
    };
    capability::with_state(shared, |e, st| {
        e.validate_copy_request(st, peer, &request, before.epoch)
    })?;
    if !save {
        return Ok(ok(&[
            ("path", "external_ddr_copy".into()),
            ("engine", "memfd-pread-pwrite".into()),
            ("nbytes", request.bytes.to_string()),
            ("cost_version", "unknown".into()),
            ("estimated_latency_us", "unknown".into()),
            ("automatic_ranking", "disabled".into()),
            ("capability", "requires-broker-probe-at-plan".into()),
            ("epoch", before.epoch.to_string()),
        ]));
    }
    // Bound metadata before performing a charged, real private probe.
    if shared.0.lock().unwrap().transfer_plans.len() >= 8192 {
        return Err("transfer plan metadata limit".into());
    }
    let started = Instant::now();
    let evidence = {
        let _span = diag::span("plan.probe");
        capability::probe(shared, &mut observer, &before, peer, &request)?
    };
    let validation_wall_seconds = started.elapsed().as_secs_f64();
    let snapshot = {
        let _span = diag::span("plan.post-snapshot");
        observer.snapshot()?
    };
    let mut st = shared.0.lock().map_err(|_| "catalog poisoned")?;
    // The receipt and complete descriptor are revalidated again atomically at Submit.
    let client_id = st
        .external_ddr
        .current_pid
        .get(&peer.pid)
        .copied()
        .ok_or("owner session missing")?;
    let plan_id = st.take_next_transfer_plan_id();
    let plan = TransferPlanRecord {
        plan_id,
        request_id: plan_id,
        client_id,
        src_object_id: number(&request.source, "object_id")?,
        dst_object_id: number(&request.target, "object_id")?,
        src_placement_id: number(&request.source, "placement_id")?,
        dst_placement_id: number(&request.target, "placement_id")?,
        src_medium: "ddr".into(),
        dst_medium: "ddr".into(),
        operation: "copy".into(),
        requested_mode: "external_copy".into(),
        path: "external_ddr_copy".into(),
        engine: "memfd-pread-pwrite".into(),
        completion_kind: "thread_event".into(),
        wait_policy: "return_immediately".into(),
        src_offset_bytes: request.source_offset,
        dst_offset_bytes: request.target_offset,
        nbytes: request.bytes,
        explanation: "explicit independent copy; exact broker probe; cost unknown; no fallback"
            .into(),
        external: Some(ExternalPlan {
            token: fresh_uuid()?,
            request,
            snapshot,
            evidence,
            peer,
            submitted: None,
            validation_wall_seconds,
        }),
        ..TransferPlanRecord::default()
    };
    let payload = transfer_plan_payload(&plan);
    st.transfer_plans.insert(plan_id, plan);
    drop(st);
    record_instant(
        TraceCategory::Transfer,
        "transfer.external_plan",
        payload.clone(),
    );
    Ok(ok(&payload))
}

pub(crate) fn event(req: &Kv, peer: Peer, shared: &SharedCatalog) -> Kv {
    let result = match get(req, "op") {
        "SubmitTransfer" => submit(req, peer, shared),
        "RecordExternalAccess" => report_access(req, peer, shared),
        _ => observe(req, peer, shared),
    };
    diag::note(format!(
        "event.terminal op={} ok={}",
        get(req, "op"),
        result.is_ok()
    ));
    result.unwrap_or_else(wire_error)
}

fn submit(req: &Kv, peer: Peer, shared: &SharedCatalog) -> Result<Kv, String> {
    let observer = discovery(shared)?;
    let mut observer = {
        let _wait = diag::span("discovery.wait");
        observer.lock().map_err(|_| "discovery poisoned")?
    };
    let plan_id = number(req, "plan_id")?;
    let plan = shared
        .0
        .lock()
        .unwrap()
        .transfer_plans
        .get(&plan_id)
        .cloned()
        .ok_or("plan missing")?;
    let external = plan.external.as_ref().ok_or("not external plan")?;
    if external.peer != peer
        || external.submitted.is_some()
        || get(req, "plan_token") != external.token
    {
        return Err("foreign or consumed plan; explicitly replan".into());
    }
    let submitted_at_ns = now_ns();
    // Guard outside mutex; receipt/identity/authority + retain/reserve/writer
    // admission inside the one existing catalog critical section.
    let work_id = capability::admit(
        shared,
        &mut observer,
        &external.snapshot,
        peer,
        &external.request,
        &external.evidence,
    )?;
    diag::note(format!(
        "business.admitted work_id={work_id} peer_pid={} target={:?}",
        peer.pid,
        diagnostic_identity(&external.request.target)
    ));
    let mut st = shared.0.lock().map_err(|_| "catalog poisoned")?;
    let event_id = st.take_next_transfer_event_id();
    st.transfer_plans
        .get_mut(&plan_id)
        .unwrap()
        .external
        .as_mut()
        .unwrap()
        .submitted = Some(event_id);
    let event = TransferEventRecord {
        event_id,
        plan_id,
        client_id: plan.client_id,
        status: "running".into(),
        completion_kind: "thread_event".into(),
        submitted_at_ns,
        actual_engine: plan.engine.clone(),
        actual_path: plan.path.clone(),
        external: Some(ExternalEvent {
            token: external.token.clone(),
            work_id: Some(work_id),
            phase: "execute".into(),
            planned_path: plan.path.clone(),
            planned_engine: plan.engine.clone(),
            snapshot_version: format!(
                "s-{}-{}-{}",
                external.snapshot.epoch,
                external.snapshot.topology_generation,
                external.snapshot.capability_generation
            ),
            validation_wall_seconds: external.validation_wall_seconds,
            ..ExternalEvent::default()
        }),
        ..TransferEventRecord::default()
    };
    let payload = event_payload(&event);
    st.transfer_events.insert(event_id, event);
    drop(st);
    record_instant(
        TraceCategory::Transfer,
        "transfer.external_submit",
        payload.clone(),
    );
    Ok(ok(&payload))
}

fn authorize_event(st: &Catalog, id: u64, peer: Peer) -> Result<(), String> {
    let event = st.transfer_events.get(&id).ok_or("event missing")?;
    if event.external.is_none() {
        return Err("not an external event".into());
    }
    let owner = st
        .external_ddr
        .sessions
        .get(&event.client_id)
        .ok_or("session missing")?;
    if !owner.alive
        || (owner.pid, owner.uid, owner.gid) != (peer.pid, peer.uid, peer.gid)
        || st.external_ddr.current_pid.get(&peer.pid) != Some(&event.client_id)
    {
        return Err("foreign event".into());
    }
    if ddr_process_identity::observe(peer.pid, peer.uid, peer.gid).map_err(|e| e.to_string())?
        != owner.birth
    {
        return Err("stale event peer incarnation".into());
    }
    Ok(())
}

fn observe(req: &Kv, peer: Peer, shared: &SharedCatalog) -> Result<Kv, String> {
    let id = number(req, "event_id")?;
    let wait = get(req, "op") == "WaitEvent";
    let timeout = if wait {
        number(req, "timeout_ms")?.min(30_000)
    } else {
        0
    };
    let deadline = Instant::now() + Duration::from_millis(timeout);
    loop {
        let mut st = shared.0.lock().map_err(|_| "catalog poisoned")?;
        authorize_event(&st, id, peer)?;
        if get(req, "event_token") != st.transfer_events[&id].external.as_ref().unwrap().token {
            return Err("stale complete event identity".into());
        }
        let work_id = st.transfer_events[&id].external.as_ref().unwrap().work_id;
        if get(req, "op") == "CancelEvent" && st.transfer_events[&id].completed_at_ns == 0 {
            if let Some(work) = work_id {
                st.external_ddr.cancel_copy_wip(work)?;
                let e = st
                    .transfer_events
                    .get_mut(&id)
                    .unwrap()
                    .external
                    .as_mut()
                    .unwrap();
                e.cancel_requested = true;
                e.phase = "cancel-requested-awaiting-drain".into();
            }
        }
        let join = work_id.is_some_and(|work| {
            st.external_ddr
                .copy_workers
                .get(&work)
                .is_some_and(JoinHandle::is_finished)
        });
        if join {
            // Remove the handle while holding the mutex: concurrent polls and
            // shutdown cannot both own/join it. Finalization remains under lock.
            let work = work_id.unwrap();
            let worker = st.external_ddr.copy_workers.remove(&work).unwrap();
            drop(st);
            let drained = worker
                .join()
                .map_err(|_| "copy worker panicked before drain".to_string());
            capability::with_state(shared, |e, st| {
                let result = match drained {
                    Ok(drained) => e.finish_copy_wip(st, drained).map(|_| ()),
                    Err(error) => Err(error),
                };
                if let Err(error) = &result {
                    if let Some(a) = &e.authority {
                        a.poison_live_admission();
                    }
                    if let Some(a) = &mut e.fixture_audit {
                        a.poison();
                    }
                    let event = st.transfer_events.get_mut(&id).unwrap();
                    event.status = "failed".into();
                    event.completed_at_ns = now_ns();
                    event.error_code = "drain-or-finalization-unconfirmed".into();
                    event.error_message = error.replace(';', ",").replace('=', ":");
                    event.external.as_mut().unwrap().phase =
                        "quarantined-retains-not-released".into();
                    record_instant(
                        TraceCategory::Transfer,
                        "transfer.external_finalization_failed",
                        event_payload(event),
                    );
                }
                result
            })?;
            st = shared.0.lock().unwrap();
        }
        let terminal = st.transfer_events[&id].completed_at_ns != 0;
        if !wait || terminal {
            let mut payload = event_payload(&st.transfer_events[&id]);
            if let Some(plan) = st.transfer_plans.get(&st.transfer_events[&id].plan_id) {
                payload.extend([
                    ("planned_path", plan.path.clone()),
                    ("planned_engine", plan.engine.clone()),
                    ("nbytes", plan.nbytes.to_string()),
                ]);
            }
            return Ok(ok(&payload));
        }
        if st.shutdown_requested || Instant::now() >= deadline {
            return Err("wait timeout or shutdown; work remains retained".into());
        }
        drop(st);
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// Called by the existing executor finalization, including shutdown join.
pub(super) fn complete(st: &mut Catalog, work_id: u64, done: &copy_admission::Completion) {
    for event in st.transfer_events.values_mut() {
        let Some(external) = &mut event.external else {
            continue;
        };
        if external.work_id != Some(work_id) {
            continue;
        }
        external.drained = true;
        external.copied_bytes = done.copied_bytes;
        external.execute_seconds = done.execute_seconds;
        external.queue_seconds = Some(done.queue_seconds);
        external.phase = if done.published {
            "ready"
        } else {
            "failed-non-ready"
        }
        .into();
        event.status = if done.published {
            "complete"
        } else if external.cancel_requested {
            "cancelled"
        } else {
            "failed"
        }
        .into();
        event.started_at_ns = done.started_at_ns;
        event.completed_at_ns = now_ns();
        event.bytes_done = done.copied_bytes;
        event.actual_latency_us =
            (event.completed_at_ns.saturating_sub(event.submitted_at_ns)) as f64 / 1000.0;
        if let Some(seconds) = done.execute_seconds.filter(|n| *n > 0.0) {
            event.actual_bandwidth_gib_s =
                done.copied_bytes as f64 / seconds / (1024.0 * 1024.0 * 1024.0);
        }
        event.error_code = if done.error.is_some() {
            event.status.clone()
        } else {
            String::new()
        };
        event.error_message = done
            .error
            .clone()
            .unwrap_or_default()
            .replace(';', ",")
            .replace('=', ":");
        record_instant(
            TraceCategory::Transfer,
            "transfer.external_terminal",
            event_payload(event),
        );
    }
}

pub(super) fn map_event(st: &mut Catalog, client_id: u64, view_id: u64, token: String) -> u64 {
    let id = st.take_next_transfer_event_id();
    let now = now_ns();
    let event = TransferEventRecord {
        event_id: id,
        client_id,
        status: "pending".into(),
        completion_kind: "map-ready".into(),
        submitted_at_ns: now,
        started_at_ns: now,
        actual_engine: "readonly-mmap".into(),
        actual_path: "external_ddr_map".into(),
        external: Some(ExternalEvent {
            token,
            view_id: Some(view_id),
            planned_path: "external_ddr_map".into(),
            planned_engine: "readonly-mmap".into(),
            phase: "awaiting-fd-mmap-ack".into(),
            ..ExternalEvent::default()
        }),
        ..TransferEventRecord::default()
    };
    record_instant(
        TraceCategory::Transfer,
        "transfer.external_map",
        event_payload(&event),
    );
    st.transfer_events.insert(id, event);
    id
}

pub(super) fn map_committed(st: &mut Catalog, view_id: u64, bytes: u64) {
    for event in st.transfer_events.values_mut() {
        if let Some(e) = &mut event.external {
            if e.view_id != Some(view_id) {
                continue;
            }
            e.mapped_bytes = bytes;
            e.phase = "map-ready-not-read-complete".into();
            event.status = "complete".into();
            event.completed_at_ns = now_ns();
            record_instant(
                TraceCategory::Transfer,
                "transfer.external_map_ready",
                event_payload(event),
            );
        }
    }
}

pub(super) fn map_closed(st: &mut Catalog, view_id: u64, confirmed: bool) {
    for event in st.transfer_events.values_mut() {
        if let Some(e) = &mut event.external {
            if e.view_id != Some(view_id) {
                continue;
            }
            e.drained = confirmed;
            e.phase = if confirmed {
                "view-release-confirmed"
            } else {
                "local-closed-not-proof-of-unmap"
            }
            .into();
            if event.completed_at_ns == 0 {
                event.status = "failed".into();
                event.completed_at_ns = now_ns();
                event.error_code = "unconfirmed-map-handoff".into();
            }
        }
    }
}

fn report_access(req: &Kv, peer: Peer, shared: &SharedCatalog) -> Result<Kv, String> {
    let id = number(req, "event_id")?;
    let bytes = number(req, "accessed_bytes")?;
    let mut st = shared.0.lock().map_err(|_| "catalog poisoned")?;
    authorize_event(&st, id, peer)?;
    let external = st.transfer_events[&id].external.as_ref().unwrap();
    if get(req, "event_token") != external.token {
        return Err("stale complete map event identity".into());
    }
    let view = st
        .external_ddr
        .views
        .get(&external.view_id.ok_or("not a map event")?)
        .ok_or("view missing")?;
    if view.pending || view.released {
        return Err("view not active".into());
    }
    if bytes < external.accessed_bytes {
        return Err("read report is cumulative".into());
    }
    st.transfer_events
        .get_mut(&id)
        .unwrap()
        .external
        .as_mut()
        .unwrap()
        .accessed_bytes = bytes;
    let payload = event_payload(&st.transfer_events[&id]);
    drop(st);
    record_instant(
        TraceCategory::Transfer,
        "transfer.external_access",
        payload.clone(),
    );
    Ok(ok(&payload))
}

pub(crate) fn stats(st: &Catalog) -> Vec<(&'static str, String)> {
    let events: Vec<_> = st
        .transfer_events
        .values()
        .filter_map(|e| e.external.as_ref())
        .collect();
    vec![
        (
            "external_mapped_bytes",
            events
                .iter()
                .map(|e| e.mapped_bytes as u128)
                .sum::<u128>()
                .to_string(),
        ),
        (
            "external_accessed_bytes",
            events
                .iter()
                .map(|e| e.accessed_bytes as u128)
                .sum::<u128>()
                .to_string(),
        ),
        (
            "external_copied_bytes",
            events
                .iter()
                .map(|e| e.copied_bytes as u128)
                .sum::<u128>()
                .to_string(),
        ),
        (
            "external_failed_events",
            st.transfer_events
                .values()
                .filter(|e| e.external.is_some() && e.status == "failed")
                .count()
                .to_string(),
        ),
        (
            "external_cancelled_events",
            st.transfer_events
                .values()
                .filter(|e| e.external.is_some() && e.status == "cancelled")
                .count()
                .to_string(),
        ),
        (
            "external_unfinalized_workers",
            st.external_ddr.copy_work.len().to_string(),
        ),
        (
            "external_cleanup_unconfirmed",
            events.iter().filter(|e| !e.drained).count().to_string(),
        ),
        ("external_event_scope", "current-broker-incarnation".into()),
        ("external_cost_version", "unknown".into()),
        ("external_automatic_ranking", "disabled".into()),
    ]
}
