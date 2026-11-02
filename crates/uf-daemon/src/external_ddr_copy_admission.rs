//! Private catalog compare-and-retain mechanics. Production callers enter via
//! external_ddr_capability's C/E verifier, including the authenticated public adapter.
//! Legacy explicit full-proof fixtures are retained but cannot mint receipts.
#![allow(dead_code)]

use super::*;
use crate::ddr_copy::{CopyCompletion, CopyFailure};
use std::sync::atomic::AtomicBool;

/// Test-only end-to-end guard-to-catalog seam. Production still needs a
/// long-lived observer, genuine capability receipt, cost version and API wiring.
#[cfg(test)]
pub(super) fn guarded_admit_wip(
    shared: &SharedCatalog,
    discovery: &mut uf_core::topology::ddr::DdrDiscovery,
    previous: &uf_core::topology::ddr::DdrSnapshot,
    source: &Kv,
    target: &Kv,
    source_offset: u64,
    target_offset: u64,
    bytes: u64,
    purpose: BackingPurpose,
) -> Result<Ticket, String> {
    use uf_core::topology::ddr::Backing as ObservedBacking;
    fn matches_snapshot(fields: &Kv, row: &ObservedBacking) -> bool {
        let identity = &row.identity;
        get(fields, "host_id") == identity.host_id
            && get(fields, "boot_id") == identity.boot_id
            && get(fields, "broker_instance") == identity.broker_instance
            && get(fields, "owner_incarnation") == identity.owner_incarnation
            && number(fields, "pool_generation").ok() == Some(identity.pool_generation)
            && number(fields, "object_id").ok() == Some(identity.object_id)
            && number(fields, "resource_generation").ok() == Some(identity.resource_generation)
            && number(fields, "placement_id").ok() == Some(row.placement_id)
            && number(fields, "binding_id").ok() == Some(row.binding_id)
            && number(fields, "size_bytes").ok() == Some(row.size_bytes)
    }
    let source_row = previous
        .backings
        .iter()
        .find(|row| matches_snapshot(source, row))
        .ok_or("source absent from snapshot")?;
    // guard() performs broker socket I/O; never hold the catalog mutex here.
    let guarded = discovery.guard(previous, &source_row.identity, source_offset, bytes)?;
    let target_row = guarded
        .backings
        .iter()
        .find(|row| matches_snapshot(target, row))
        .ok_or("target absent from guarded snapshot")?;
    if target_row.state != "pending"
        || target_row.reclaimed
        || !target_row.owner_alive
        || bytes == 0
        || target_offset
            .checked_add(bytes)
            .is_none_or(|end| end > target_row.size_bytes)
    {
        return Err("pending target is stale or out of range".into());
    }
    let (lock, _) = &**shared;
    let mut st = lock.lock().unwrap();
    let mut external = mem::take(&mut st.external_ddr);
    let result = external.admit_copy_wip(
        &mut st,
        source,
        target,
        guarded.epoch,
        source_offset,
        target_offset,
        bytes,
        purpose,
    );
    st.external_ddr = external;
    result
}

pub(super) struct Work {
    source_id: u64,
    target_id: u64,
    purpose: BackingPurpose,
    bytes: u64,
    cancelled: Arc<AtomicBool>,
    contract: Option<CopyContract>,
    source_offset: u64,
    target_offset: u64,
}

#[derive(Clone)]
pub(super) struct CopyContract {
    pub snapshot: String,
    pub evidence: String,
    pub cost: String,
    pub probe: bool,
}

pub(super) struct Ticket {
    queued_at: std::time::Instant,
    id: u64,
    source: File,
    target: File,
    source_offset: u64,
    target_offset: u64,
    bytes: u64,
    cancelled: Arc<AtomicBool>,
    contract: Option<CopyContract>,
}

/// A consumed executor ticket: the worker's two cloned FDs have been closed.
/// For threaded execution the caller must additionally join the worker before
/// passing this token to finish. The finalization records a durable drain.
pub(super) struct DrainedTicket {
    queue_seconds: f64,
    started_at_ns: u128,
    id: u64,
    result: Result<CopyCompletion, CopyFailure>,
}

impl Ticket {
    pub(super) fn id(&self) -> u64 {
        self.id
    }

    pub(super) fn execute(self) -> DrainedTicket {
        let queue_seconds = self.queued_at.elapsed().as_secs_f64();
        let started_at_ns = crate::common::now_ns();
        let Self {
            queued_at: _,
            id,
            source,
            target,
            source_offset,
            target_offset,
            bytes,
            cancelled,
            contract,
        } = self;
        let initialized = if contract.as_ref().is_some_and(|c| c.probe) {
            crate::ddr_copy::fill_probe(&target).map_err(|detail| CopyFailure {
                kind: crate::ddr_copy::CopyFailureKind::Io,
                bytes_copied: 0,
                bytes_verified: 0,
                detail,
            })
        } else {
            Ok(())
        };
        let primitive = if contract.is_some() {
            crate::ddr_copy::copy_range
        } else {
            crate::ddr_copy::copy_and_verify
        };
        let result = initialized.and_then(|()| {
            primitive(
                &source,
                &target,
                source_offset,
                target_offset,
                bytes,
                &cancelled,
            )
        });
        drop(source);
        drop(target);
        DrainedTicket {
            id,
            result,
            queue_seconds,
            started_at_ns,
        }
    }
}

#[derive(Clone)]
pub(crate) struct Completion {
    pub queue_seconds: f64,
    pub started_at_ns: u128,
    pub published: bool,
    pub probe_completed: bool,
    pub copied_bytes: u64,
    pub verified_bytes: u64,
    pub execute_seconds: Option<f64>,
    pub validation_seconds: f64,
    pub validated_target_bytes: u64,
    pub error: Option<String>,
}

impl ExternalDdrState {
    /// Under the catalog lock, durably admit and retain the worker handle
    /// before returning. Production cannot obtain an untracked Ticket here;
    /// this WIP path is not a public request API.
    pub(super) fn admit_and_spawn_wip(
        &mut self,
        st: &mut Catalog,
        source: &Kv,
        target: &Kv,
        epoch: u64,
        source_offset: u64,
        target_offset: u64,
        bytes: u64,
        purpose: BackingPurpose,
    ) -> Result<u64, String> {
        let ticket = self.admit_copy_locked(
            st,
            source,
            target,
            epoch,
            source_offset,
            target_offset,
            bytes,
            purpose,
        )?;
        self.spawn_admitted_copy_with_hook(ticket, || {})
    }

    pub(super) fn spawn_admitted_copy_with_hook(
        &mut self,
        ticket: Ticket,
        before_execute: impl FnOnce() + Send + 'static,
    ) -> Result<u64, String> {
        let id = ticket.id();
        if !self.copy_work.contains_key(&id) || self.copy_workers.contains_key(&id) {
            if let Some(audit) = self.fixture_audit.as_mut() {
                audit.poison();
            }
            if let Some(authority) = self.authority.as_ref() {
                authority.poison_live_admission();
            }
            return Err("copy worker does not match a unique admitted ticket".into());
        }
        let worker = match std::thread::Builder::new()
            .name(format!("uf-managed-ddr-{id}"))
            .spawn(move || {
                before_execute();
                ticket.execute()
            }) {
            Ok(worker) => worker,
            Err(error) => {
                // Do not invent WORK_DRAINED when spawn fails.
                if let Some(audit) = self.fixture_audit.as_mut() {
                    audit.poison();
                }
                if let Some(authority) = self.authority.as_ref() {
                    authority.poison_live_admission();
                }
                return Err(format!("managed copy worker spawn failed: {error}"));
            }
        };
        self.copy_workers.insert(id, worker);
        Ok(id)
    }

    #[cfg(test)]
    pub(super) fn admit_and_spawn_test_hook(
        &mut self,
        st: &mut Catalog,
        source: &Kv,
        target: &Kv,
        epoch: u64,
        source_offset: u64,
        target_offset: u64,
        bytes: u64,
        purpose: BackingPurpose,
        before_execute: impl FnOnce() + Send + 'static,
    ) -> Result<u64, String> {
        let ticket = self.admit_copy_locked(
            st,
            source,
            target,
            epoch,
            source_offset,
            target_offset,
            bytes,
            purpose,
        )?;
        self.spawn_admitted_copy_with_hook(ticket, before_execute)
    }

    /// Older test fixtures may manually execute and join the returned ticket.
    #[cfg(test)]
    pub(super) fn admit_copy_wip(
        &mut self,
        st: &mut Catalog,
        source: &Kv,
        target: &Kv,
        epoch: u64,
        source_offset: u64,
        target_offset: u64,
        bytes: u64,
        purpose: BackingPurpose,
    ) -> Result<Ticket, String> {
        self.admit_copy_locked(
            st,
            source,
            target,
            epoch,
            source_offset,
            target_offset,
            bytes,
            purpose,
        )
    }

    /// The caller must have checked a fresh C source guard outside the catalog
    /// lock. This method only compares the broker-side identity/epoch and
    /// atomically retains already-budgeted backings under the catalog lock.
    /// It is deliberately not wired to any broker or control API.
    fn admit_copy_locked(
        &mut self,
        st: &mut Catalog,
        source: &Kv,
        target: &Kv,
        epoch: u64,
        source_offset: u64,
        target_offset: u64,
        bytes: u64,
        purpose: BackingPurpose,
    ) -> Result<Ticket, String> {
        self.admit_copy_mode(
            st,
            source,
            target,
            epoch,
            source_offset,
            target_offset,
            bytes,
            purpose,
            None,
        )
    }

    pub(super) fn admit_copy_mode(
        &mut self,
        st: &mut Catalog,
        source: &Kv,
        target: &Kv,
        epoch: u64,
        source_offset: u64,
        target_offset: u64,
        bytes: u64,
        purpose: BackingPurpose,
        contract: Option<CopyContract>,
    ) -> Result<Ticket, String> {
        if contract.is_none() && !cfg!(test) {
            return Err("legacy fixture admission is not a production copy contract".into());
        }
        self.refresh(st)?;
        if st.shutdown_requested || epoch != self.epoch {
            return Err("stale epoch or shutdown; explicitly replan".into());
        }
        if self
            .copy_work
            .len()
            .checked_add(self.copy_results.len())
            .is_none_or(|n| n >= 8192)
        {
            return Err("copy work metadata limit reached".into());
        }
        if !matches!(purpose, BackingPurpose::Staging | BackingPurpose::ProbeOnly) {
            return Err("invalid copy purpose".into());
        }
        let source_id = number(source, "object_id")?;
        let target_id = number(target, "object_id")?;
        if source_id == target_id || bytes == 0 {
            return Err("copy requires distinct nonempty backings".into());
        }
        let source_backing = self.backings.get(&source_id).ok_or("source not found")?;
        let target_backing = self.backings.get(&target_id).ok_or("target not found")?;
        self.validate_descriptor(source, source_backing)?;
        self.validate_descriptor(target, target_backing)?;
        for (descriptor, backing) in [(source, source_backing), (target, target_backing)] {
            if number(descriptor, "placement_id")? != backing.placement_id
                || number(descriptor, "binding_id")? != backing.binding_id
                || st
                    .resource_bindings
                    .get(&backing.binding_id)
                    .is_none_or(|b| b.binding_generation != backing.resource_generation)
                || self
                    .sessions
                    .get(&backing.owner_client_id)
                    .is_none_or(|s| !s.alive)
            {
                return Err("stale placement, binding or owner".into());
            }
        }
        if source_backing.state != "active"
            || source_backing.purpose != BackingPurpose::Immutable
            || source_backing.file.is_none()
            || target_backing.state != "pending"
            || target_backing.handoff_pending
            || target_backing.purpose != purpose
            || target_backing.writer.is_none()
            || target_backing.in_flight != 0
            || target_backing.probe_completed
            || (source_backing.dev, source_backing.ino) == (target_backing.dev, target_backing.ino)
        {
            return Err("source or destination is not eligible for this copy".into());
        }
        if source_offset
            .checked_add(bytes)
            .is_none_or(|end| end > source_backing.bytes)
            || target_offset
                .checked_add(bytes)
                .is_none_or(|end| end > target_backing.bytes)
        {
            return Err("copy range out of bounds".into());
        }
        if self.committed_bytes() > self.total_limit {
            return Err("managed budget invariant violated".into());
        }
        let source_file = source_backing
            .file
            .as_ref()
            .unwrap()
            .try_clone()
            .map_err(|e| e.to_string())?;
        let target_file = target_backing
            .writer
            .as_ref()
            .unwrap()
            .try_clone()
            .map_err(|e| e.to_string())?;
        let source_refs = source_backing
            .in_flight
            .checked_add(1)
            .ok_or("source reference overflow")?;
        let target_refs = target_backing
            .in_flight
            .checked_add(1)
            .ok_or("target reference overflow")?;
        let source_generation = source_backing.resource_generation;
        let target_generation = target_backing.resource_generation;
        let id = self
            .next_copy_work_id
            .checked_add(1)
            .ok_or("work id overflow")?;
        let next_epoch = self.epoch.checked_add(1).ok_or("epoch overflow")?;
        self.before_admission()?;
        if self.fixture_audit.is_some() {
            use crate::ddr_recovery_audit::{EventKind, Purpose, WorkIdentity};
            // Explicitly bind the fixture's broker-thread job to its journaled
            // broker birth. Older tests without a broker birth retain the old
            // sentinel but cannot pass the coverage verifier.
            let worker_id = match self.fixture_audit.as_ref().unwrap().replay().broker_birth() {
                Some(b) => format!(
                    "bt-{id}-{}-{}-{}-{}",
                    b.pid, b.starttime_ticks, b.pid_namespace_dev, b.pid_namespace_ino
                ),
                None => format!("fixture-work-{id}"),
            };
            // Test-only sentinel versions explicitly do not establish a real
            // snapshot/receipt/cost binding or grant business capability.
            self.record_fixture_event(EventKind::WorkIntent(WorkIdentity {
                work_id: id,
                source_id,
                source_generation,
                target_id,
                target_generation,
                src_offset: source_offset,
                dst_offset: target_offset,
                bytes,
                snapshot_version: contract
                    .as_ref()
                    .map_or("fixture-unverified", |c| &c.snapshot)
                    .into(),
                evidence_version: contract
                    .as_ref()
                    .map_or("fixture-unverified", |c| &c.evidence)
                    .into(),
                cost_version: contract
                    .as_ref()
                    .map_or("fixture-unverified", |c| &c.cost)
                    .into(),
                worker_id,
                purpose: if purpose == BackingPurpose::ProbeOnly {
                    Purpose::ProbeOnly
                } else {
                    Purpose::Staging
                },
            }))?;
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        self.backings.get_mut(&source_id).unwrap().in_flight = source_refs;
        self.backings.get_mut(&target_id).unwrap().in_flight = target_refs;
        self.copy_work.insert(
            id,
            Work {
                source_id,
                target_id,
                purpose,
                bytes,
                cancelled: Arc::clone(&cancelled),
                contract: contract.clone(),
                source_offset,
                target_offset,
            },
        );
        self.next_copy_work_id = id;
        self.epoch = next_epoch;
        Ok(Ticket {
            queued_at: std::time::Instant::now(),
            id,
            source: source_file,
            target: target_file,
            source_offset,
            target_offset,
            bytes,
            cancelled,
            contract,
        })
    }

    /// A cancel request is not completion; retained references remain until
    /// finish_copy_wip is called after the real worker returns.
    pub(super) fn cancel_copy_wip(&mut self, id: u64) -> Result<(), String> {
        if self.copy_results.contains_key(&id) {
            return Ok(());
        }
        let cancelled = Arc::clone(&self.copy_work.get(&id).ok_or("unknown work")?.cancelled);
        let result = if self.fixture_audit.is_some() {
            self.record_fixture_cleanup(crate::ddr_recovery_audit::EventKind::CancelRequest {
                work_id: id,
            })
        } else {
            Ok(())
        };
        // Even a failed durable cancel record must not prevent stopping work.
        cancelled.store(true, Ordering::Release);
        result
    }

    /// Caller must pass the consumed executor ticket only after the worker has
    /// joined. An arbitrary result or a still-owned writable Ticket cannot be
    /// used to release the retained source/destination references.
    /// Probe completion here does NOT constitute a production capability proof.
    pub(super) fn finish_copy_wip(
        &mut self,
        st: &mut Catalog,
        drained: DrainedTicket,
    ) -> Result<Completion, String> {
        self.finish_copy_wip_with_hook(st, drained, || {})
    }

    /// Private seam; production entry uses only the no-op hook above. Tests
    /// exercise the real post-seal/pre-record failure window here.
    pub(super) fn finish_copy_wip_with_hook(
        &mut self,
        st: &mut Catalog,
        drained: DrainedTicket,
        before_publish_record: impl FnOnce(),
    ) -> Result<Completion, String> {
        let DrainedTicket {
            id,
            result,
            queue_seconds,
            started_at_ns,
        } = drained;
        if let Some(completed) = self.copy_results.get(&id) {
            return Ok(completed.clone());
        }
        self.refresh(st)?;
        let next_epoch = self.epoch.checked_add(1).ok_or("epoch overflow")?;
        let work = self.copy_work.get(&id).ok_or("unknown or completed work")?;
        let source_id = work.source_id;
        let target_id = work.target_id;
        let purpose = work.purpose;
        let wanted = work.bytes;
        let contract = work.contract.clone();
        let source_offset = work.source_offset;
        let target_offset = work.target_offset;
        let cancelled = work.cancelled.load(Ordering::Acquire);
        let source = self
            .backings
            .get(&source_id)
            .ok_or("retained source missing")?;
        let target = self
            .backings
            .get(&target_id)
            .ok_or("retained target missing")?;
        if source.in_flight == 0 || target.in_flight == 0 {
            return Err("retained reference invariant violated".into());
        }
        let valid_lifecycle = source.state == "active"
            && target.state == "pending"
            && target.purpose == purpose
            && self
                .sessions
                .get(&source.owner_client_id)
                .is_some_and(|s| s.alive)
            && self
                .sessions
                .get(&target.owner_client_id)
                .is_some_and(|s| s.alive);
        let execute_seconds = result.as_ref().ok().map(|c| c.execute_seconds);
        let mut validation_seconds = 0.0;
        let mut validated_target_bytes = 0;
        let (copied, mut verified, mut error, proof) = match result {
            Ok(value)
                if value.bytes_copied == wanted
                    && (contract.is_some() || value.bytes_verified == wanted) =>
            {
                (value.bytes_copied, value.bytes_verified, None, Some(value))
            }
            Ok(value) => (
                value.bytes_copied,
                value.bytes_verified,
                Some("incomplete executor result".into()),
                None,
            ),
            Err(value) => (
                value.bytes_copied,
                value.bytes_verified,
                Some(value.detail),
                None,
            ),
        };
        if cancelled {
            error = Some("cancelled after admission".into());
        }
        if !valid_lifecycle {
            error = Some("source or target retired during execution".into());
        }
        if self.fixture_audit.is_some() {
            // This private test seam receives a consumed ticket only after its
            // FD clones were closed; a threaded caller must also have joined.
            // Failed sync preserves runtime retains and poisons new work.
            self.record_fixture_cleanup(crate::ddr_recovery_audit::EventKind::WorkDrained {
                work_id: id,
            })?;
        }
        if error.is_none() && self.fixture_audit.is_some() {
            if contract.is_some() {
                self.record_fixture_cleanup(
                    crate::ddr_recovery_audit::EventKind::WorkIoCompleted {
                        work_id: id,
                        copied,
                    },
                )?;
            } else {
                self.record_fixture_cleanup(crate::ddr_recovery_audit::EventKind::WorkVerified {
                    work_id: id,
                    copied,
                    verified,
                })?;
            }
        }
        if error.is_none() && (purpose == BackingPurpose::Staging || contract.is_some()) {
            if self.fixture_audit.is_some() && purpose == BackingPurpose::Staging {
                self.record_fixture_cleanup(crate::ddr_recovery_audit::EventKind::PublishIntent {
                    work_id: id,
                })?;
            }
            let writer = self.backings[&target_id]
                .writer
                .as_ref()
                .ok_or("target writer missing")?;
            let seal_result = if unsafe {
                libc::fcntl(
                    writer.as_raw_fd(),
                    libc::F_ADD_SEALS,
                    libc::F_SEAL_WRITE | libc::F_SEAL_SEAL,
                )
            } != 0
            {
                Err(format!(
                    "target seal failed: {}",
                    io::Error::last_os_error()
                ))
            } else {
                let seals = unsafe { libc::fcntl(writer.as_raw_fd(), libc::F_GET_SEALS) };
                let required = libc::F_SEAL_GROW
                    | libc::F_SEAL_SHRINK
                    | libc::F_SEAL_WRITE
                    | libc::F_SEAL_SEAL;
                if seals < 0 || seals & required != required {
                    Err("target seal verification failed".to_string())
                } else {
                    Ok(seals as u32)
                }
            };
            match seal_result {
                Err(message) => {
                    if self.fixture_audit.is_some() && purpose == BackingPurpose::Staging {
                        self.record_fixture_cleanup(
                            crate::ddr_recovery_audit::EventKind::PublishAbort { work_id: id },
                        )?;
                    }
                    error = Some(message);
                }
                Ok(seals) => {
                    // Recheck lifecycle and exact payload/neighbours AFTER the
                    // real write seal, before publishing. The executor-owned
                    // pre-copy proof prevents a caller from supplying success.
                    self.refresh(st)?;
                    let source = self
                        .backings
                        .get(&source_id)
                        .ok_or("retained source missing")?;
                    let target = self
                        .backings
                        .get(&target_id)
                        .ok_or("retained target missing")?;
                    let still_valid = source.state == "active"
                        && target.state == "pending"
                        && self
                            .sessions
                            .get(&source.owner_client_id)
                            .is_some_and(|s| s.alive)
                        && self
                            .sessions
                            .get(&target.owner_client_id)
                            .is_some_and(|s| s.alive);
                    let check = if !still_valid {
                        Err("source or target retired before publish".to_string())
                    } else {
                        match (proof.as_ref(), source.file.as_ref(), target.file.as_ref()) {
                            (Some(proof), Some(source), Some(target)) => {
                                proof.verify_final(source, target).and_then(|()| {
                                    if contract.as_ref().is_some_and(|c| c.probe) {
                                        let started = std::time::Instant::now();
                                        let result = crate::ddr_copy::verify_probe(
                                            source,
                                            target,
                                            source_offset,
                                            target_offset,
                                            wanted,
                                        );
                                        validation_seconds = started.elapsed().as_secs_f64();
                                        result.map(|bytes| {
                                            validated_target_bytes = bytes;
                                        })
                                    } else {
                                        Ok(())
                                    }
                                })
                            }
                            _ => Err("missing final copy proof or retained file".to_string()),
                        }
                    };
                    if let Err(message) = check {
                        if self.fixture_audit.is_some() && purpose == BackingPurpose::Staging {
                            self.record_fixture_cleanup(
                                crate::ddr_recovery_audit::EventKind::PublishAbort { work_id: id },
                            )?;
                        }
                        error = Some(message);
                    } else {
                        // Drop the broker's controlled writer before the durable
                        // completion record; seals prohibit later writes.
                        self.backings.get_mut(&target_id).unwrap().writer = None;
                        if contract.as_ref().is_some_and(|c| c.probe) {
                            verified = wanted;
                            if self.fixture_audit.is_some() {
                                self.record_fixture_cleanup(
                                    crate::ddr_recovery_audit::EventKind::WorkVerified {
                                        work_id: id,
                                        copied,
                                        verified,
                                    },
                                )?;
                            }
                        }
                        if self.fixture_audit.is_some() && purpose == BackingPurpose::Staging {
                            before_publish_record();
                            self.record_fixture_cleanup(
                                crate::ddr_recovery_audit::EventKind::PublishDone {
                                    work_id: id,
                                    seals,
                                },
                            )?;
                        }
                    }
                }
            }
        }
        let mut published = false;
        let mut probe_completed = false;
        if error.is_none() {
            let target = self.backings.get_mut(&target_id).unwrap();
            if purpose == BackingPurpose::ProbeOnly {
                // Kept pending and reserved. Proof binding and neighbour checks
                // are not implemented; it must never publish as business data.
                target.probe_completed = true;
                probe_completed = true;
            } else {
                target.writer = None;
                target.purpose = BackingPurpose::Immutable;
                target.state = "active";
                if let Some(object) = st.objects.get_mut(&target_id) {
                    object.state = "Ready".into();
                    object.immutable = true;
                }
                if let Some(object) = st.data_objects.get_mut(&target_id) {
                    object.state = "Ready".into();
                }
                if let Some(placement) = st.placements.get_mut(&target.placement_id) {
                    placement.state = "Ready".into();
                }
                if let Some(binding) = st.resource_bindings.get_mut(&target.binding_id) {
                    binding.state = "Ready".into();
                    binding.limitations = vec![
                        "cooperative-immutable".into(),
                        "fd-range-not-os-isolated".into(),
                        "numa-unknown".into(),
                    ];
                }
                if let Some(lease) = st.leases.get_mut(&target.owner_lease_id) {
                    lease.state = "Active".into();
                }
                published = true;
            }
        } else {
            // Keep the projection within T2's existing tombstone vocabulary.
            let _ = self.retire(st, target_id, "registration_rollback");
        }
        self.copy_work.remove(&id);
        self.backings.get_mut(&source_id).unwrap().in_flight -= 1;
        self.backings.get_mut(&target_id).unwrap().in_flight -= 1;
        self.epoch = next_epoch;
        self.collect_retired(st);
        let completed = Completion {
            queue_seconds,
            started_at_ns,
            published,
            probe_completed,
            copied_bytes: copied,
            verified_bytes: verified,
            execute_seconds,
            validation_seconds,
            validated_target_bytes,
            error,
        };
        self.copy_results.insert(id, completed.clone());
        super::transfer::complete(st, id, &completed);
        Ok(completed)
    }
}

/// Join outside the catalog lock, then finalize actual returned tickets under
/// it. Unknown/untracked work is never treated as drained.
pub(super) fn stop_and_join_workers(shared: &SharedCatalog) -> Result<(), String> {
    let workers = {
        let mut st = shared.0.lock().unwrap();
        mem::take(&mut st.external_ddr.copy_workers)
    };
    let mut failure = None;
    for (id, worker) in workers {
        match worker.join() {
            Ok(drained) => {
                let mut st = shared.0.lock().unwrap();
                let mut external = mem::take(&mut st.external_ddr);
                let result = external.finish_copy_wip(&mut st, drained);
                if let Err(error) = result {
                    if let Some(audit) = external.fixture_audit.as_mut() {
                        audit.poison();
                    }
                    if let Some(authority) = external.authority.as_ref() {
                        authority.poison_live_admission();
                    }
                    failure.get_or_insert(format!("worker {id} finalization failed: {error}"));
                }
                st.external_ddr = external;
            }
            Err(_) => {
                let mut st = shared.0.lock().unwrap();
                if let Some(audit) = st.external_ddr.fixture_audit.as_mut() {
                    audit.poison();
                }
                if let Some(authority) = st.external_ddr.authority.as_ref() {
                    authority.poison_live_admission();
                }
                failure.get_or_insert(format!("worker {id} panicked before drain"));
            }
        }
    }
    let st = shared.0.lock().unwrap();
    if !st.external_ddr.copy_work.is_empty() || !st.external_ddr.copy_workers.is_empty() {
        failure.get_or_insert("untracked or unfinished copy work remains".into());
    }
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}
