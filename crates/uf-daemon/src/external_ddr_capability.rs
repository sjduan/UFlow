//! Private production seam for T3.4. No new wire operation or second catalog.
//! C discovery remains advisory; this broker rechecks and retains under its
//! existing catalog mutex. Only broker-owned, sealed, exact-validated probes
//! mint an opaque, short-lived receipt. Legacy probe fixtures cannot mint one.
#![allow(dead_code)]
use super::copy_admission::{Completion, CopyContract};
use super::*;
use std::ffi::CString;
use uf_core::topology::ddr::diagnostic as diag;

use std::time::Instant;
use uf_core::topology::ddr::{
    self, CapabilityEvidence, CapabilityEvidenceVerifier, DdrDiscovery, DdrSnapshot, Operation,
    Support,
};

const EXECUTOR: &str = "external-ddr-copy-v1-known-full-pattern-v1";
const RECEIPT_TTL: Duration = Duration::from_secs(5);

const MAX_RECEIPTS: usize = 512;

// Facts, not optimistic cleanup inference. Errors retain the last reached
// stage and explicitly distinguish unconfirmed cleanup from success.
#[derive(Default)]
struct ProbeStatus {
    stage: &'static str,
    registered: bool,
    started: bool,
    drained: bool,
    closed: bool,
}
impl Drop for ProbeStatus {
    fn drop(&mut self) {
        diag::note(format!(
            "probe.exit stage={} registered={} started={} drain_confirmed={} close_confirmed={}",
            self.stage, self.registered, self.started, self.drained, self.closed
        ));
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CopyRequest {
    pub source: Kv,
    pub target: Kv,
    pub source_offset: u64,
    pub target_offset: u64,
    pub bytes: u64,
}

pub(super) struct Receipt {
    evidence: CapabilityEvidence,
    request: CopyRequest,
    deadline: Instant,
}

fn same_evidence(a: &CapabilityEvidence, b: &CapabilityEvidence) -> bool {
    a.source == b.source
        && a.destination_address_domain == b.destination_address_domain
        && a.destination_pool_generation == b.destination_pool_generation
        && a.realm == b.realm
        && a.epoch == b.epoch
        && a.topology_generation == b.topology_generation
        && a.capability_generation == b.capability_generation
        && a.operation == b.operation
        && a.offset == b.offset
        && a.bytes == b.bytes
        && a.expires_at_unix_ns == b.expires_at_unix_ns
        && a.provenance == b.provenance
        && a.proof == b.proof
}

impl CapabilityEvidenceVerifier for ExternalDdrState {
    fn verify(&self, evidence: &CapabilityEvidence) -> bool {
        self.capability_receipts
            .get(&evidence.proof)
            .is_some_and(|r| {
                r.deadline > Instant::now()
                    && evidence.provenance == EXECUTOR
                    && evidence.epoch == self.epoch
                    && same_evidence(evidence, &r.evidence)
            })
    }
}

fn version(snapshot: &DdrSnapshot) -> String {
    format!(
        "s-{}-{}-{}",
        snapshot.epoch, snapshot.topology_generation, snapshot.capability_generation
    )
}

fn guarded(
    discovery: &mut DdrDiscovery,
    previous: &DdrSnapshot,
    req: &CopyRequest,
) -> Result<DdrSnapshot, String> {
    let row = previous
        .backings
        .iter()
        .find(|r| r.identity.object_id.to_string() == get(&req.source, "object_id"))
        .ok_or("source absent from snapshot")?;
    discovery.guard(previous, &row.identity, req.source_offset, req.bytes)
}

impl ExternalDdrState {
    pub(super) fn validate_copy_request(
        &mut self,
        st: &mut Catalog,
        peer: Peer,
        req: &CopyRequest,
        epoch: u64,
    ) -> Result<(), String> {
        self.refresh(st)?;
        if st.shutdown_requested || self.epoch != epoch {
            return Err("stale epoch or shutdown; explicitly replan".into());
        }
        let source = self
            .backings
            .get(&number(&req.source, "object_id")?)
            .ok_or("source missing")?;
        let target = self
            .backings
            .get(&number(&req.target, "object_id")?)
            .ok_or("target missing")?;
        for (fields, backing) in [(&req.source, source), (&req.target, target)] {
            // All descriptor fields, including placement/binding/lease/size and
            // permission-purpose, not just a colliding object number.
            let actual = self.descriptor(backing);
            if actual
                .iter()
                .any(|(key, value)| fields.get(key) != Some(value))
            {
                return Err("stale complete copy descriptor".into());
            }
            if self
                .sessions
                .get(&backing.owner_client_id)
                .is_none_or(|s| !s.alive)
                || st
                    .resource_bindings
                    .get(&backing.binding_id)
                    .is_none_or(|b| b.binding_generation != backing.resource_generation)
            {
                return Err("stale owner or binding version".into());
            }
        }
        let owner = &self.sessions[&target.owner_client_id];
        if (owner.pid, owner.uid, owner.gid) != (peer.pid, peer.uid, peer.gid)
            || self.current_pid.get(&peer.pid) != Some(&target.owner_client_id)
        {
            return Err("only the target owner may admit a write".into());
        }
        if source.state != "active"
            || source.purpose != BackingPurpose::Immutable
            || target.state != "pending"
            || target.purpose != BackingPurpose::Staging
            || target.handoff_pending
            || target.in_flight != 0
            || target.writer.is_none()
            || source.object_id == target.object_id
            || (source.dev, source.ino) == (target.dev, target.ino)
            || req.bytes == 0
            || req
                .source_offset
                .checked_add(req.bytes)
                .is_none_or(|n| n > source.bytes)
            || req
                .target_offset
                .checked_add(req.bytes)
                .is_none_or(|n| n > target.bytes)
        {
            return Err("invalid copy permission, lifecycle or range".into());
        }
        // T1's cooperative immutable registrations remain compatible. This
        // stricter copy route cannot issue exact evidence for a mutable source.
        let file = source.file.as_ref().ok_or("source file missing")?;
        let seals = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GET_SEALS) };
        if seals < 0 || seals & libc::F_SEAL_WRITE == 0 {
            return Err("exact copy capability requires write-sealed source".into());
        }
        if self.budget_committed_bytes() > self.total_limit {
            return Err("budget invariant violated".into());
        }
        Ok(())
    }
}

/// Future T3.4 caller supplies credentials obtained from its authenticated
/// channel, not fields from a client request. Writable FD handoff is cooperative.
/// The broker does not export a pending target's read FD.
pub(crate) fn stage(
    shared: &SharedCatalog,
    peer: Peer,
    fields: &Kv,
    file: File,
) -> Result<Kv, String> {
    with_state(shared, |external, st| {
        external.refresh(st)?;
        if st.shutdown_requested {
            return Err("shutdown".into());
        }
        external.before_admission()?;
        let client = external.session(st, peer)?;
        let reply = external.register(st, client, fields, file, BackingPurpose::Staging);
        match reply {
            Ok(reply) => {
                let undo = reply
                    .undo
                    .ok_or("staging registration has no transaction")?;
                if let Err(e) = external.commit(st, &undo) {
                    external.undo(st, undo);
                    return Err(e);
                }
                Ok(reply.fields)
            }
            Err(e) => {
                external.discard_uncommitted_session(st, client);
                Err(e)
            }
        }
    })
}

pub(super) fn with_state<T>(
    shared: &SharedCatalog,
    f: impl FnOnce(&mut ExternalDdrState, &mut Catalog) -> Result<T, String>,
) -> Result<T, String> {
    let mut st = {
        let _wait = diag::span("catalog.wait");
        shared.0.lock().map_err(|_| "catalog poisoned")?
    };
    let _hold = diag::span("catalog.hold");
    let mut external = mem::take(&mut st.external_ddr);
    let result = f(&mut external, &mut st);
    st.external_ddr = external;
    result
}

/// Join actual tracked worker outside the catalog lock. A request to cancel or
/// an arbitrary caller completion cannot substitute for this ownership token.
pub(crate) fn join(shared: &SharedCatalog, id: u64) -> Result<Completion, String> {
    let worker = with_state(shared, |e, _| {
        e.copy_workers
            .remove(&id)
            .ok_or_else(|| "no owned worker".into())
    })?;
    let joined = {
        let _span = diag::span("worker.join");
        worker.join()
    };
    match joined {
        Ok(drained) => with_state(shared, |e, st| e.finish_copy_wip(st, drained)),
        Err(_) => with_state(shared, |e, _| {
            if let Some(a) = e.authority.as_ref() {
                a.poison_live_admission();
            }
            if let Some(a) = e.fixture_audit.as_mut() {
                a.poison();
            }
            Err("copy worker panicked before drain".into())
        }),
    }
}

/// Runs the SAME ordinary primitive against a broker-only target with the same
/// size/range and destination pool as the proposed business target. This method
/// never writes the business target and never publishes the private probe.
pub(crate) fn probe(
    shared: &SharedCatalog,
    discovery: &mut DdrDiscovery,
    previous: &DdrSnapshot,
    peer: Peer,
    req: &CopyRequest,
) -> Result<CapabilityEvidence, String> {
    probe_with_hook(shared, discovery, previous, peer, req, || {})
}

pub(super) fn probe_with_hook(
    shared: &SharedCatalog,
    discovery: &mut DdrDiscovery,
    previous: &DdrSnapshot,
    peer: Peer,
    req: &CopyRequest,
    hook: impl FnOnce() + Send + 'static,
) -> Result<CapabilityEvidence, String> {
    let mut status = ProbeStatus {
        stage: "guard",
        ..ProbeStatus::default()
    };
    let observed = {
        let _span = diag::span("probe.guard");
        guarded(discovery, previous, req)?
    };
    let (id, probe_id) = with_state(shared, |e, st| {
        status.stage = "prepare-admission";
        let _prepare = diag::span("probe.prepare");
        e.validate_copy_request(st, peer, req, observed.epoch)?;
        if e.backings
            .values()
            .any(|b| b.purpose == BackingPurpose::ProbeOnly && b.charged)
        {
            return Err("probe verification slot busy".into());
        }
        let target_id = number(&req.target, "object_id")?;
        let size = e.backings[&target_id].bytes;

        e.before_admission()?;
        status.stage = "memfd-create";
        let name = CString::new("uflow-broker-private-probe").unwrap();
        let fd = unsafe {
            libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING)
        };
        if fd < 0 {
            return Err(diag::io_error(
                "probe.memfd-create",
                io::Error::last_os_error(),
            ));
        }
        let file = unsafe { File::from_raw_fd(fd) };
        status.stage = "set-len";
        file.set_len(size)
            .map_err(|e| diag::io_error("probe.set-len", e))?;
        status.stage = "add-seals";
        if unsafe {
            libc::fcntl(
                fd,
                libc::F_ADD_SEALS,
                libc::F_SEAL_GROW | libc::F_SEAL_SHRINK,
            )
        } != 0
        {
            return Err(diag::io_error(
                "probe.add-seals",
                io::Error::last_os_error(),
            ));
        }
        // register atomically reserves the full backing before worker prefill.
        let client = e.backings[&target_id].owner_client_id;
        let fields = uf_core::kv(&[
            ("name", "private-probe".into()),
            ("owner_name", "broker-probe".into()),
            ("size_bytes", size.to_string()),
        ]);
        let reply = e.register(st, client, &fields, file, BackingPurpose::ProbeOnly)?;
        status.registered = true;
        status.stage = "register-commit";
        let undo = reply.undo.ok_or("probe registration transaction missing")?;
        let probe_id = number(&reply.fields, "object_id")?;
        diag::note(format!("probe.registered object_id={probe_id}"));
        if let Err(error) = e.commit(st, &undo) {
            e.undo(st, undo);
            return Err(error);
        }
        let ticket = e.admit_copy_mode(
            st,
            &req.source,
            &reply.fields,
            e.epoch,
            req.source_offset,
            req.target_offset,
            req.bytes,
            BackingPurpose::ProbeOnly,
            Some(CopyContract {
                snapshot: version(&observed),
                evidence: EXECUTOR.into(),
                cost: "unknown".into(),
                probe: true,
            }),
        );
        let ticket = match ticket {
            Ok(t) => t,
            Err(error) => {
                let _ = e.retire(st, probe_id, "registration_rollback");
                e.collect_retired(st);
                return Err(error);
            }
        };
        let id = e.spawn_admitted_copy_with_hook(ticket, hook)?;
        status.started = true;
        diag::note(format!("probe.started work_id={id} object_id={probe_id}"));
        Ok((id, probe_id))
    })?;
    let done = {
        status.stage = "join-verify-finalize";
        let _span = diag::span("probe.join-verify-finalize");
        join(shared, id)
    };
    // A failed/panicked/unconfirmed worker retains the charged probe. Never
    // reclaim merely because join/finalization returned an error.
    let done = done?;
    status.drained = true;
    status.stage = "close";
    diag::note(format!("probe.terminal work_id={id} drained=true verified_bytes={} verify_seconds={} validated_target_bytes={} execute_seconds={:?} probe_completed={} published={} error={}", done.verified_bytes, done.validation_seconds, done.validated_target_bytes, done.execute_seconds, done.probe_completed, done.published, done.error.is_some()));
    with_state(shared, |e, st| {
        let _close = diag::span("probe.close");
        if e.backings.get(&probe_id).is_none_or(|b| b.in_flight != 0) {
            return Err("probe not drained".into());
        }
        e.retire(st, probe_id, "revoked")?;
        e.collect_retired(st);
        if e.backings[&probe_id].charged {
            return Err("probe close not confirmed".into());
        }
        diag::note(format!(
            "probe.cleanup object_id={probe_id} charged=false in_flight=0"
        ));
        Ok(())
    })?;
    status.closed = true;
    status.stage = "verify-result";
    if !done.probe_completed
        || done.published
        || done.error.is_some()
        || done.verified_bytes != req.bytes
    {
        return Err(done
            .error
            .unwrap_or_else(|| "probe did not validate".into()));
    }
    let snapshot = {
        status.stage = "post-snapshot";
        let _span = diag::span("probe.post-snapshot");
        discovery.snapshot()?
    };
    // Probe-owned catalog changes legitimately advance observer generations;
    // a physical/broker change does not. Do not rebase a measurement onto it.
    if snapshot.identity != observed.identity || snapshot.physical != observed.physical {
        return Err("broker or physical topology changed during probe; explicitly replan".into());
    }
    status.stage = "receipt";
    with_state(shared, |e, st| {
        e.validate_copy_request(st, peer, req, snapshot.epoch)?;
        e.before_admission()?;
        e.capability_receipts
            .retain(|_, r| r.deadline > Instant::now() && r.evidence.epoch == e.epoch);
        if e.capability_receipts.len() >= MAX_RECEIPTS {
            return Err("receipt metadata limit".into());
        }
        let source = snapshot
            .backings
            .iter()
            .find(|b| b.identity.object_id.to_string() == get(&req.source, "object_id"))
            .ok_or("source absent")?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_nanos();
        let evidence = CapabilityEvidence {
            source: source.identity.clone(),
            destination_address_domain: get(&req.target, "owner_incarnation").into(),
            destination_pool_generation: number(&req.target, "pool_generation")?,
            realm: snapshot.realm.clone(),
            epoch: snapshot.epoch,
            topology_generation: snapshot.topology_generation,
            capability_generation: snapshot.capability_generation,
            operation: Operation::Copy,
            offset: req.source_offset,
            bytes: req.bytes,
            expires_at_unix_ns: now + RECEIPT_TTL.as_nanos(),
            provenance: EXECUTOR.into(),
            proof: fresh_uuid()?.into_bytes(),
        };
        e.capability_receipts.insert(
            evidence.proof.clone(),
            Receipt {
                evidence: evidence.clone(),
                request: req.clone(),
                deadline: Instant::now() + RECEIPT_TTL,
            },
        );
        if let Err(error) = ddr::verify_capability_evidence(&snapshot, &evidence, e) {
            e.capability_receipts.remove(&evidence.proof);
            return Err(error);
        }
        Ok(evidence)
    })
}

/// Capability is source -> destination pool evidence, NOT blanket write access.
/// The exact business target, credentials, full tuples and range are bound and
/// atomically compared again with the retains/exclusive writer transaction.
pub(crate) fn admit(
    shared: &SharedCatalog,
    discovery: &mut DdrDiscovery,
    previous: &DdrSnapshot,
    peer: Peer,
    req: &CopyRequest,
    evidence: &CapabilityEvidence,
) -> Result<u64, String> {
    admit_with_hook(shared, discovery, previous, peer, req, evidence, || {})
}

pub(super) fn admit_with_hook(
    shared: &SharedCatalog,
    discovery: &mut DdrDiscovery,
    previous: &DdrSnapshot,
    peer: Peer,
    req: &CopyRequest,
    evidence: &CapabilityEvidence,
    hook: impl FnOnce() + Send + 'static,
) -> Result<u64, String> {
    let snapshot = {
        let _span = diag::span("submit.guard");
        guarded(discovery, previous, req)?
    };
    with_state(shared, |e, st| {
        e.validate_copy_request(st, peer, req, snapshot.epoch)?;
        if ddr::verify_capability_evidence(&snapshot, evidence, e)? != Support::Supported
            || e.capability_receipts
                .get(&evidence.proof)
                .is_none_or(|r| &r.request != req)
        {
            return Err("receipt does not authorize exact target/range".into());
        }
        let ticket = e.admit_copy_mode(
            st,
            &req.source,
            &req.target,
            snapshot.epoch,
            req.source_offset,
            req.target_offset,
            req.bytes,
            BackingPurpose::Staging,
            Some(CopyContract {
                snapshot: version(&snapshot),
                evidence: String::from_utf8(evidence.proof.clone()).map_err(|e| e.to_string())?,
                cost: "unknown".into(),
                probe: false,
            }),
        )?;
        e.capability_receipts.remove(&evidence.proof);
        e.spawn_admitted_copy_with_hook(ticket, hook)
    })
}
