//! Physical, cooperative external DDR registration and FD broker.
//! The catalog remains authoritative; an AccessBinding is not another placement.
use crate::catalog::{Catalog, Client, Object, SharedCatalog};
use crate::ddr_managed_authority::{DrainedCommitments, ManagedAuthority};
use crate::ddr_process_identity::{self, ProcessBirth};
use std::collections::HashMap;
use std::fs::{self, File, Permissions};
use std::io::{self, Read, Write};
use std::mem;
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uf_core::idl::AccessBindingRecord;
use uf_core::provider::{AccessKind, Addressability, TransportKind};
use uf_core::{decode, encode, err, get, ok, Kv};

const MAX_FRAME: usize = 8192;
const DEFAULT_OWNER_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_TOTAL_BYTES: u64 = 64 * 1024 * 1024;
// Tombstones preserve idempotency and stale-identity rejection for this daemon
// incarnation. Bound them rather than silently growing without limit.
const MAX_BACKING_RECORDS: usize = 512;
const MAX_VIEW_RECORDS: usize = 8192;
const MAX_SESSION_RECORDS: usize = 2048;

#[cfg(test)]
#[path = "external_ddr_tests.rs"]
mod managed_tests;

#[path = "external_ddr_copy_admission.rs"]
mod copy_admission;

#[path = "external_ddr_capability.rs"]
pub(crate) mod capability;

#[path = "external_ddr_transfer.rs"]
pub(crate) mod transfer;

fn identity_file(path: &str) -> String {
    fs::read_to_string(path)
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn fresh_uuid() -> Result<String, String> {
    let id = identity_file("/proc/sys/kernel/random/uuid");
    if id.len() == 36 && id.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-') {
        Ok(id)
    } else {
        Err("kernel random UUID unavailable".to_string())
    }
}

fn number(req: &Kv, key: &str) -> Result<u64, String> {
    get(req, key)
        .parse()
        .map_err(|_| format!("invalid {}", key))
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 96
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}

struct Session {
    pid: i32,
    uid: u32,
    gid: u32,
    birth: ProcessBirth,
    incarnation: String,
    pool_generation: u64,
    pidfd: Option<File>,
    alive: bool,
    // An identity is durable only after at least one handoff commits.
    committed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BackingPurpose {
    Immutable,
    Staging,
    ProbeOnly,
}

struct Backing {
    object_id: u64,
    placement_id: u64,
    binding_id: u64,
    owner_client_id: u64,
    owner_lease_id: u64,
    owner_name: String,
    resource_generation: u64,
    bytes: u64,
    dev: u64,
    ino: u64,
    state: &'static str,
    file: Option<File>,
    // The backing remains charged until its real close and durable close-done.
    charged: bool,
    /// Writable targets are cooperative handoffs; no read view is issued while pending.
    writer: Option<File>,
    purpose: BackingPurpose,
    handoff_pending: bool,
    in_flight: u64,
    probe_completed: bool,
}

struct View {
    object_id: u64,
    client_id: u64,
    lease_id: u64,
    offset: u64,
    bytes: u64,
    pending: bool,
    released: bool,
}

#[derive(Default)]
pub struct ExternalDdrState {
    host_id: String,
    boot_id: String,
    broker_instance: String,
    bound_quarantine_lifecycle: bool,
    epoch: u64,
    next_generation: u64,
    sessions: HashMap<u64, Session>,
    current_pid: HashMap<i32, u64>,
    backings: HashMap<u64, Backing>,
    views: HashMap<u64, View>,
    owner_limit: u64,
    total_limit: u64,
    config_error: Option<String>,
    authority: Option<ManagedAuthority>,
    /// Until this process has opened the fixed authority successfully, the
    /// inherited commitment count cannot be inferred from its empty catalog.
    authority_commitments_unknown: bool,
    /// Lifecycle writer over the fixed authority's pinned journal. The name
    /// is retained to avoid changing the many existing fixture call sites.
    fixture_audit: Option<crate::ddr_recovery_durable::DurableAudit>,
    /// A possible FD exposure or unresolved peer death permanently stops
    /// admissions for this live authority. Cleanup may still be recorded;
    /// neither cleanup nor an empty catalog clears this flag.
    uncertain_exposure: bool,
    /// Persist the real broker birth before the first resource intent.
    broker_birth_pending: bool,
    #[cfg(test)]
    after_close_before_done: Option<Box<dyn FnOnce() + Send>>,
    #[cfg(test)]
    after_handoff_send: Option<Box<dyn FnOnce() + Send>>,
    /// Experimental normal-drain path for an isolated pinned-journal fixture.
    #[cfg(test)]
    fixture_clean_checkpoint: bool,
    copy_work: HashMap<u64, copy_admission::Work>,
    /// All managed copy threads are owned until they actually return and join.
    copy_workers: HashMap<u64, JoinHandle<copy_admission::DrainedTicket>>,
    /// Independent reservations are not yet exposed by any production API;
    /// backing-attached reservations are already counted in charged backings.
    independent_reservations: HashMap<u64, u64>,
    copy_results: HashMap<u64, copy_admission::Completion>,
    next_copy_work_id: u64,
    capability_receipts: HashMap<Vec<u8>, capability::Receipt>,
    discovery: Option<Arc<std::sync::Mutex<uf_core::topology::ddr::DdrDiscovery>>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Peer {
    pid: i32,
    uid: u32,
    gid: u32,
}

enum Undo {
    View(u64),
    Registration(u64),
}

struct Reply {
    fields: Kv,
    fd: Option<File>,
    undo: Option<Undo>,
}

/// Only `Broker::stop` may construct this in production, after the listener,
/// accepted external/control handlers and all tracked copy workers joined.
/// It does not by itself prove that catalog obligations, independent
/// reservations and potential exposures are zero; production remains gated.
pub(crate) struct JoinedDrain {
    broker: String,
}

impl JoinedDrain {
    pub(crate) fn broker(&self) -> &str {
        &self.broker
    }
}

#[cfg(test)]
pub(crate) fn fixture_joined_drain(broker: &str) -> JoinedDrain {
    JoinedDrain {
        broker: broker.to_owned(),
    }
}

impl Reply {
    fn plain(fields: Kv) -> Self {
        Self {
            fields,
            fd: None,
            undo: None,
        }
    }
}

impl ExternalDdrState {
    pub fn new() -> Self {
        fn configured_limit(name: &str, default: u64) -> Result<u64, String> {
            match std::env::var(name) {
                Ok(value) => value
                    .parse::<u64>()
                    .ok()
                    .filter(|n| *n > 0)
                    .ok_or_else(|| format!("invalid {}", name)),
                Err(std::env::VarError::NotPresent) => Ok(default),
                Err(_) => Err(format!("invalid {}", name)),
            }
        }
        let owner = configured_limit("UF_EXTERNAL_DDR_OWNER_MAX_BYTES", DEFAULT_OWNER_BYTES);
        let total = configured_limit("UF_EXTERNAL_DDR_TOTAL_MAX_BYTES", DEFAULT_TOTAL_BYTES);
        let config_error = owner
            .as_ref()
            .err()
            .or_else(|| total.as_ref().err())
            .cloned();
        Self {
            // A container's /etc/machine-id can differ from the physical host.
            // Require a trusted host-scoped value instead of inventing one.
            host_id: std::env::var("UF_EXTERNAL_DDR_HOST_ID")
                .ok()
                .filter(|v| valid_name(v))
                .unwrap_or_default(),
            boot_id: identity_file("/proc/sys/kernel/random/boot_id"),
            broker_instance: fresh_uuid().unwrap_or_default(),
            next_generation: 1,
            owner_limit: owner.unwrap_or(0),
            total_limit: total.unwrap_or(0),
            config_error,
            authority_commitments_unknown: true,
            ..Self::default()
        }
    }

    fn authority_config_valid(&self) -> io::Result<()> {
        if self.config_error.is_some()
            || !valid_name(&self.host_id)
            || !valid_name(&self.boot_id)
            || self.owner_limit == 0
            || self.total_limit < self.owner_limit
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid external DDR authority configuration",
            ));
        }
        Ok(())
    }

    /// Called only by the explicit trusted deployment bootstrap operation.
    pub fn initialize_authority(
        &self,
        endpoint: &str,
        policy_path: Option<&std::path::Path>,
    ) -> io::Result<()> {
        self.authority_config_valid()?;
        if let Some(path) = policy_path {
            let policy = crate::ddr_deployment_policy::DeploymentPolicy::load(path)?;
            ManagedAuthority::initialize_production(
                &self.host_id,
                &self.boot_id,
                endpoint,
                self.owner_limit,
                self.total_limit,
                &policy,
            )
        } else {
            ManagedAuthority::initialize_normal_production(
                &self.host_id,
                &self.boot_id,
                endpoint,
                self.owner_limit,
                self.total_limit,
            )
        }
    }

    /// Trusted operator inspection is deliberately read-only. It obtains the
    /// fixed root and lock itself and cannot be given an evidence path.
    pub fn inspect_authority_evidence(&self, endpoint: &str) -> io::Result<String> {
        self.authority_config_valid()?;
        let authority = ManagedAuthority::open_production(
            &self.host_id,
            &self.boot_id,
            endpoint,
            self.owner_limit,
            self.total_limit,
        )?;
        let rosters = authority.inspect_sealed_evidence()?;
        let generations: Vec<_> = rosters
            .iter()
            .map(|roster| {
                serde_json::json!({
                    "generation": roster.generation,
                    "recorded_normal_waits": roster.normal_exits.len(),
                    "evidence_head_sha256": roster.final_head_sha256,
                })
            })
            .collect();
        Ok(serde_json::json!({
            "schema": "uflow-ddr-evidence-inspection-v1",
            "scope": "structural-pinned-roster-only-not-authorization-or-recovery",
            "generations": generations,
        })
        .to_string())
    }

    /// Must succeed before binding any listener that can accept external DDR.
    pub fn open_authority(&mut self, endpoint: &str) -> io::Result<()> {
        self.authority_commitments_unknown = true;
        self.authority_config_valid()?;
        let authority = ManagedAuthority::open_production(
            &self.host_id,
            &self.boot_id,
            endpoint,
            self.owner_limit,
            self.total_limit,
        )?;
        self.attach_lifecycle(authority)?;
        Ok(())
    }

    /// A gated broker reacquires the fixed root itself and verifies its live
    /// identity against the anchored BIND. Even a verified BIND remains
    /// quarantined until the remaining lifecycle and evidence paths are safe.
    pub fn open_bound_quarantined_authority(&mut self, endpoint: &str) -> io::Result<()> {
        self.authority_commitments_unknown = true;
        self.authority_config_valid()?;
        if self.authority.is_some() || self.fixture_audit.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "managed authority already attached",
            ));
        }
        let mut authority = ManagedAuthority::open_production(
            &self.host_id,
            &self.boot_id,
            endpoint,
            self.owner_limit,
            self.total_limit,
        )?;
        let generation = authority.verify_bound_self_from_anchor()?;
        if !authority.is_inherited_quarantine() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "verified bound authority must remain quarantined",
            ));
        }
        // Record the real bound birth before either listener starts. The
        // quarantine still rejects every external DDR operation, including
        // snapshots, before it can create a session or backing.
        let mut audit = authority.start_bound_quarantine_audit(&generation)?;
        audit.record(crate::ddr_recovery_audit::Event {
            domain: audit.domain().clone(),
            epoch: 0,
            kind: crate::ddr_recovery_audit::EventKind::BrokerBirth {
                birth: generation.broker_birth,
            },
        })?;
        authority.publish_bound_quarantine_head(&audit)?;
        self.broker_instance = generation.broker;
        self.fixture_audit = Some(audit);
        self.authority = Some(authority);
        self.bound_quarantine_lifecycle = true;
        Ok(())
    }

    /// Explicit deployment binder only. The root and expected config are
    /// fixed here; this does not expose a root or trust selector to the SDK.
    pub(crate) fn open_binding_authority(&self, endpoint: &str) -> io::Result<ManagedAuthority> {
        self.authority_config_valid()?;
        ManagedAuthority::open_production(
            &self.host_id,
            &self.boot_id,
            endpoint,
            self.owner_limit,
            self.total_limit,
        )
    }

    /// New commitments require both the authority lock and its pinned,
    /// semantically replayed writer. An inherited quarantine keeps the lock
    /// for error-only diagnostics without fabricating a fresh empty catalog.
    fn attach_lifecycle(&mut self, authority: ManagedAuthority) -> io::Result<()> {
        if self.authority.is_some() || self.fixture_audit.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "managed authority already attached",
            ));
        }
        if !authority.matches_runtime_config(
            &self.host_id,
            &self.boot_id,
            self.owner_limit,
            self.total_limit,
        ) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "managed authority and runtime configuration disagree",
            ));
        }
        if authority.is_inherited_quarantine() {
            self.authority = Some(authority);
            self.authority_commitments_unknown = false;
            return Ok(());
        }
        let audit = authority.start_lifecycle_audit(&self.broker_instance)?;
        if audit.domain().host != self.host_id
            || audit.domain().boot != self.boot_id
            || audit.domain().owner_limit != self.owner_limit
            || audit.domain().total_limit != self.total_limit
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "managed authority and runtime configuration disagree",
            ));
        }
        self.fixture_audit = Some(audit);
        self.authority = Some(authority);
        self.authority_commitments_unknown = false;
        self.broker_birth_pending = true;
        Ok(())
    }

    pub fn has_authority(&self) -> bool {
        self.authority.is_some()
    }

    pub fn disable_authority(&mut self) {
        self.fixture_audit = None;
        self.authority = None;
        self.authority_commitments_unknown = true;
        self.broker_birth_pending = false;
        self.bound_quarantine_lifecycle = false;
    }

    fn before_admission(&mut self) -> Result<(), String> {
        if self.uncertain_exposure {
            return Err("unresolved exposure; authority admission disabled".into());
        }
        if self
            .fixture_audit
            .as_ref()
            .is_some_and(|audit| audit.is_poisoned())
        {
            return Err("fixture audit writer poisoned; admission disabled".to_string());
        }
        // Legacy unit fixtures exercise raw mechanics without a journal;
        // every production admission requires the pinned lifecycle writer.
        if self.fixture_audit.is_none() && !cfg!(test) {
            return Err("durable lifecycle writer unavailable; admission disabled".into());
        }
        let result = self
            .authority
            .as_mut()
            .ok_or("managed authority unavailable")?
            .before_admission()
            .map_err(|e| format!("managed authority unavailable: {}", e));
        if result.is_err() {
            if let Some(audit) = self.fixture_audit.as_mut() {
                audit.poison();
            }
        }
        result?;
        if self.broker_birth_pending {
            let pid = std::process::id() as i32;
            let uid = unsafe { libc::geteuid() };
            let gid = unsafe { libc::getegid() };
            let observed = (|| -> io::Result<ProcessBirth> {
                let first = ddr_process_identity::observe(pid, uid, gid)?;
                if ddr_process_identity::observe(pid, uid, gid)? != first {
                    return Err(io::Error::other("broker birth changed during binding"));
                }
                Ok(first)
            })();
            let result = observed.and_then(|birth| {
                let audit = self
                    .fixture_audit
                    .as_mut()
                    .ok_or_else(|| io::Error::other("pinned lifecycle writer unavailable"))?;
                audit.record(crate::ddr_recovery_audit::Event {
                    domain: audit.domain().clone(),
                    epoch: 0,
                    kind: crate::ddr_recovery_audit::EventKind::BrokerBirth { birth },
                })
            });
            if let Err(error) = result {
                if let Some(audit) = self.fixture_audit.as_mut() {
                    audit.poison();
                }
                if let Some(authority) = self.authority.as_ref() {
                    authority.poison_live_admission();
                }
                return Err(format!("durable broker birth: {error}"));
            }
            self.broker_birth_pending = false;
        }
        Ok(())
    }

    fn record_fixture_event(
        &mut self,
        kind: crate::ddr_recovery_audit::EventKind,
    ) -> Result<(), String> {
        self.record_fixture_event_inner(kind, false)
    }

    fn record_fixture_event_inner(
        &mut self,
        kind: crate::ddr_recovery_audit::EventKind,
        cleanup: bool,
    ) -> Result<(), String> {
        if self.fixture_audit.is_some() {
            let domain = self.fixture_audit.as_ref().unwrap().domain();
            let valid = domain.host == self.host_id
                && domain.boot == self.boot_id
                && domain.broker == self.broker_instance
                && domain.owner_limit == self.owner_limit
                && domain.total_limit == self.total_limit
                && self.authority.as_ref().is_some_and(|authority| {
                    (if cleanup {
                        authority.cleanup_namespace_valid()
                    } else {
                        authority.admits_new_work()
                    }) && authority.matches_audit_domain(domain)
                });
            if !valid {
                self.fixture_audit.as_mut().unwrap().poison();
                if let Some(authority) = self.authority.as_ref() {
                    authority.poison_live_admission();
                }
                return Err("authority identity or namespace changed; audit poisoned".into());
            }
        }
        if let Some(audit) = self.fixture_audit.as_mut() {
            let event = crate::ddr_recovery_audit::Event {
                domain: audit.domain().clone(),
                epoch: self.epoch,
                kind,
            };
            if let Err(error) = audit.record(event) {
                if let Some(authority) = self.authority.as_ref() {
                    authority.poison_live_admission();
                }
                return Err(format!("fixture audit record: {error}"));
            }
        } else if !cfg!(test) {
            return Err("durable audit unavailable; admission disabled".into());
        }
        Ok(())
    }

    fn record_fixture_cleanup(
        &mut self,
        kind: crate::ddr_recovery_audit::EventKind,
    ) -> Result<(), String> {
        let result = self.record_fixture_event_inner(kind, true);
        if result.is_err() {
            if let Some(audit) = self.fixture_audit.as_mut() {
                audit.poison();
            }
        }
        result
    }

    /// Called after the external listener and all its handlers have joined.
    fn finish_shutdown(&mut self, st: &mut Catalog, joined: &JoinedDrain) -> Result<(), String> {
        if !st.shutdown_requested {
            return Err("catalog admission has not been stopped".into());
        }
        self.refresh(st)?;
        // A read-only inherited quarantine never opened a lifecycle writer or
        // accepted an external operation. Its diagnostic process can terminate
        // normally after actual joins without calling the domain clean.
        if !self.bound_quarantine_lifecycle
            && self
                .authority
                .as_ref()
                .is_some_and(ManagedAuthority::is_inherited_quarantine)
            && self.fixture_audit.is_none()
            && self.sessions.is_empty()
            && self.backings.is_empty()
            && self.views.is_empty()
            && self.copy_work.is_empty()
            && self.copy_workers.is_empty()
            && self.independent_reservations.is_empty()
            && joined.broker() == self.broker_instance
        {
            return Ok(());
        }
        if self.uncertain_exposure {
            return Err("unresolved exposure; authority must remain dirty".into());
        }
        if self.bound_quarantine_lifecycle {
            let audit = self
                .fixture_audit
                .as_mut()
                .ok_or("bound quarantine audit missing")?;
            let authority = self
                .authority
                .as_mut()
                .ok_or("bound quarantine authority missing")?;
            if audit.is_poisoned()
                || !authority.is_inherited_quarantine()
                || joined.broker() != self.broker_instance
                || audit.domain().broker != self.broker_instance
                || !self.sessions.is_empty()
                || !self.backings.is_empty()
                || !self.views.is_empty()
                || !self.copy_work.is_empty()
                || !self.copy_workers.is_empty()
                || !self.independent_reservations.is_empty()
            {
                audit.poison();
                return Err("bound quarantine did not actually drain".into());
            }
            let snapshot = audit.replay().observed_drain(0, 0);
            audit
                .record_joined_drain(joined, self.epoch, snapshot)
                .map_err(|e| format!("bound quarantine drain: {e}"))?;
            authority
                .publish_bound_quarantine_head(audit)
                .map_err(|e| format!("bound quarantine drain state: {e}"))?;
            let receipt = audit
                .checkpoint_after_drain(joined)
                .map_err(|e| format!("bound quarantine checkpoint: {e}"))?;
            if receipt.head() != audit.head()
                || receipt.domain() != audit.domain()
                || receipt.file_identities() != audit.file_identities()
            {
                audit.poison();
                return Err("bound quarantine checkpoint receipt mismatch".into());
            }
            authority
                .publish_bound_quarantine_head(audit)
                .map_err(|e| format!("bound quarantine checkpoint state: {e}"))?;
            // Semantic-zero lifecycle is not a SEAL or permission to admit.
            return Ok(());
        }
        if self.broker_birth_pending {
            // An unused clean authority must not acquire a fabricated dirty
            // generation merely because its listener stopped.
            if joined.broker() == self.broker_instance
                && self
                    .fixture_audit
                    .as_ref()
                    .is_some_and(|audit| !audit.is_poisoned())
                && self
                    .authority
                    .as_ref()
                    .is_some_and(ManagedAuthority::admits_new_work)
                && self.backings.is_empty()
                && self.views.is_empty()
                && self.copy_work.is_empty()
                && self.copy_workers.is_empty()
                && self.independent_reservations.is_empty()
            {
                return Ok(());
            }
            return Err("unstarted lifecycle has catalog obligations".into());
        }
        if self.fixture_audit.is_some() {
            let audit = self.fixture_audit.as_ref().unwrap();
            // Zero handlers comes from JoinedDrain; reservation count comes
            // from the one catalog-owned registry, not a placeholder.
            let snapshot = audit
                .replay()
                .observed_drain(0, self.independent_reservations.len() as u64);
            let charged = self.backings.values().filter(|b| b.charged).count() as u64;
            let active_views = self
                .views
                .values()
                .filter(|v| !v.released && !v.pending)
                .count() as u64;
            let pending = self
                .backings
                .values()
                .filter(|b| b.handoff_pending && b.state == "pending")
                .count()
                + self
                    .views
                    .values()
                    .filter(|v| !v.released && v.pending)
                    .count();
            if audit.is_poisoned()
                || joined.broker() != self.broker_instance
                || audit.domain().broker != self.broker_instance
                || !self.authority.as_ref().is_some_and(|authority| {
                    authority.admits_new_work() && authority.matches_audit_domain(audit.domain())
                })
                || snapshot.retained_backings != charged
                || audit.replay().charged_bytes() != self.committed_bytes()
                || snapshot.active_views != active_views
                || snapshot.pending_handoffs != pending as u64
                || snapshot.in_flight != self.copy_work.len() as u64
                || snapshot.pending_handoffs != 0
                || snapshot.in_flight != 0
            {
                return Err("catalog and audit have not verifiably drained".into());
            }
            self.fixture_audit
                .as_mut()
                .unwrap()
                .record_joined_drain(joined, self.epoch, snapshot.clone())
                .map_err(|e| format!("durable broker drain: {e}"))?;
            #[cfg(test)]
            let checkpoint_enabled = self.fixture_clean_checkpoint;
            #[cfg(not(test))]
            let checkpoint_enabled = true;
            if checkpoint_enabled {
                let seal = self
                    .fixture_audit
                    .as_mut()
                    .unwrap()
                    .checkpoint_after_drain(joined)
                    .map_err(|e| format!("semantic clean checkpoint: {e}"))?;
                let drained = DrainedCommitments {
                    accepting_stopped: true,
                    handlers: snapshot.handlers,
                    retained_backings: snapshot.retained_backings,
                    views: snapshot.active_views,
                    pending: snapshot.pending_handoffs,
                    reservations: snapshot.reservations,
                    in_flight: snapshot.in_flight,
                };
                return self
                    .authority
                    .as_mut()
                    .ok_or("managed authority unavailable")?
                    .clean_after_semantic_drain(&drained, seal)
                    .map_err(|e| format!("managed authority remains dirty: {e}"));
            }
            // The isolated fixture can intentionally leave a drained but
            // uncheckpointed generation dirty for recovery exercises.
            return Err("durable broker drain recorded; authority remains dirty".into());
        }
        if !cfg!(test) {
            return Err("durable audit unavailable; authority remains dirty".into());
        }
        let drained = DrainedCommitments {
            accepting_stopped: true,
            handlers: 0,
            retained_backings: self.backings.values().filter(|b| b.charged).count() as u64,
            views: self.views.values().filter(|v| !v.released).count() as u64,
            pending: self
                .backings
                .values()
                .filter(|b| b.state == "pending")
                .count() as u64,
            reservations: self.independent_reservations.len() as u64,
            in_flight: self.copy_work.len() as u64,
        };
        self.authority
            .as_mut()
            .ok_or("managed authority unavailable")?
            .clean_after_drain(&drained)
            .map_err(|e| format!("managed authority remains dirty: {}", e))
    }

    pub fn contains(&self, object_id: u64) -> bool {
        self.backings.contains_key(&object_id)
    }

    pub fn configured(&self) -> bool {
        !self.uncertain_exposure
            && !self.host_id.is_empty()
            && !self.boot_id.is_empty()
            && !self.broker_instance.is_empty()
            && self
                .fixture_audit
                .as_ref()
                .map_or(cfg!(test), |audit| !audit.is_poisoned())
            && self
                .authority
                .as_ref()
                .is_some_and(ManagedAuthority::admits_new_work)
    }

    pub fn pidfd_observed(&self) -> bool {
        !self.sessions.is_empty()
    }

    pub fn committed_bytes(&self) -> u64 {
        self.backings
            .values()
            .filter(|b| b.charged)
            .map(|b| b.bytes)
            .try_fold(0u64, |sum, n| sum.checked_add(n))
            .unwrap_or(u64::MAX)
    }

    /// Backing reservations are already charged above. Only independent
    /// reservations add budget here; the lifecycle replay's charged_bytes is
    /// specifically the backing ledger and must retain that meaning.
    fn budget_committed_bytes(&self) -> u64 {
        self.independent_reservations
            .values()
            .try_fold(self.committed_bytes(), |sum, n| sum.checked_add(*n))
            .unwrap_or(u64::MAX)
    }

    /// Failed authority startup or inherited quarantine cannot establish the
    /// commitment count. An empty local catalog must not be reported as zero.
    pub fn commitments_unknown(&self) -> bool {
        self.uncertain_exposure
            || self.authority_commitments_unknown
            || self
                .authority
                .as_ref()
                .is_some_and(|authority| !authority.admits_new_work())
            || self
                .fixture_audit
                .as_ref()
                .is_some_and(|audit| audit.is_poisoned())
    }

    pub fn owns_lease(&self, lease_id: u64) -> bool {
        self.backings.values().any(|b| b.owner_lease_id == lease_id)
            || self.views.values().any(|v| v.lease_id == lease_id)
    }

    pub fn owns_view(&self, view_id: u64) -> bool {
        self.views.contains_key(&view_id)
    }

    fn generation(&mut self) -> u64 {
        let result = self.next_generation;
        self.next_generation += 1;
        result
    }

    fn session(&mut self, st: &mut Catalog, peer: Peer) -> Result<u64, String> {
        if let Some(id) = self.current_pid.get(&peer.pid).copied() {
            if let Some(session) = self.sessions.get(&id) {
                let birth = ddr_process_identity::observe(peer.pid, peer.uid, peer.gid)
                    .map_err(|e| format!("observe peer birth: {e}"))?;
                if session.alive
                    && session.uid == peer.uid
                    && session.gid == peer.gid
                    && session.birth == birth
                    && !Self::session_dead(session)?
                {
                    return Ok(id);
                }
            }
            // refresh() handles confirmed exits; never attach a new incarnation
            // to a different process simply because its numeric PID matches.
            return Err("peer birth no longer matches the live session".to_string());
        }
        if self.sessions.len() >= MAX_SESSION_RECORDS {
            return Err("external DDR session metadata limit reached".to_string());
        }
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, peer.pid, 0) } as i32;
        if fd < 0 {
            return Err(format!("pidfd_open failed: {}", io::Error::last_os_error()));
        }
        let pidfd = unsafe { File::from_raw_fd(fd) };
        let first = ddr_process_identity::observe(peer.pid, peer.uid, peer.gid)
            .map_err(|e| format!("observe peer birth: {e}"))?;
        let second = ddr_process_identity::observe(peer.pid, peer.uid, peer.gid)
            .map_err(|e| format!("recheck peer birth: {e}"))?;
        if first != second || Self::pidfd_dead(&pidfd)? {
            return Err("peer birth changed or exited during binding".to_string());
        }
        let incarnation = fresh_uuid()?;
        let pool_generation = self.generation();
        let client_id = st.take_next_client_id();
        st.clients.insert(
            client_id,
            Client {
                client_id,
                role: "external_ddr".to_string(),
                os_pid: peer.pid as i64,
                bare_tgid: 0,
                device_id: st.device,
            },
        );
        self.sessions.insert(
            client_id,
            Session {
                pid: peer.pid,
                uid: peer.uid,
                gid: peer.gid,
                birth: first,
                incarnation,
                pool_generation,
                pidfd: Some(pidfd),
                alive: true,
                committed: false,
            },
        );
        self.current_pid.insert(peer.pid, client_id);
        self.epoch += 1;
        Ok(client_id)
    }

    // The catalog lock serializes this check with other requests and handoffs.
    // Never detach an established session or one with a concurrent pending
    // registration/view. Rolled-back records for an uncommitted session can be
    // discarded without reusing their IDs; committed tombstones remain intact.
    fn discard_uncommitted_session(&mut self, st: &mut Catalog, client_id: u64) {
        let Some(session) = self.sessions.get(&client_id) else {
            return;
        };
        if session.committed
            || self.backings.values().any(|b| {
                b.owner_client_id == client_id
                    && (b.state == "active" || b.state == "pending" || b.charged)
            })
            || self
                .views
                .values()
                .any(|v| v.client_id == client_id && !v.released)
        {
            return;
        }
        let views: Vec<u64> = self
            .views
            .iter()
            .filter(|(_, v)| v.client_id == client_id)
            .map(|(id, _)| *id)
            .collect();
        for id in views {
            if let Some(view) = self.views.remove(&id) {
                st.access_bindings.remove(&id);
                st.leases.remove(&view.lease_id);
            }
        }
        let backings: Vec<u64> = self
            .backings
            .values()
            .filter(|b| b.owner_client_id == client_id)
            .map(|b| b.object_id)
            .collect();
        for id in backings {
            if let Some(backing) = self.backings.remove(&id) {
                st.objects.remove(&id);
                st.data_objects.remove(&id);
                st.placements.remove(&backing.placement_id);
                st.resource_bindings.remove(&backing.binding_id);
                st.leases.remove(&backing.owner_lease_id);
            }
        }
        if let Some(session) = self.sessions.remove(&client_id) {
            if self.current_pid.get(&session.pid) == Some(&client_id) {
                self.current_pid.remove(&session.pid);
            }
            st.clients.remove(&client_id);
            self.epoch += 1;
        }
    }

    fn session_dead(session: &Session) -> Result<bool, String> {
        let pidfd = session.pidfd.as_ref().ok_or("live session has no pidfd")?;
        Self::pidfd_dead(pidfd)
    }

    fn pidfd_dead(pidfd: &File) -> Result<bool, String> {
        let mut p = libc::pollfd {
            fd: pidfd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let rc = unsafe { libc::poll(&mut p, 1, 0) };
        if rc < 0 {
            return Err(format!("pidfd poll failed: {}", io::Error::last_os_error()));
        }
        if rc > 0 && p.revents & libc::POLLNVAL != 0 {
            return Err("pidfd poll reported invalid descriptor".to_string());
        }
        Ok(rc > 0 && p.revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) != 0)
    }

    fn close_view(&mut self, st: &mut Catalog, view_id: u64) {
        if let Some(view) = self.views.get_mut(&view_id) {
            if view.released {
                return;
            }
            view.released = true;
            view.pending = false;
            transfer::map_closed(st, view_id, false);
            if let Some(lease) = st.leases.get_mut(&view.lease_id) {
                lease.state = "Closed".to_string();
            }
            if let Some(access) = st.access_bindings.get_mut(&view_id) {
                access.state = "Released".to_string();
            }
            self.epoch += 1;
        }
    }

    fn release_view(&mut self, st: &mut Catalog, view_id: u64) -> Result<(), String> {
        let view = self.views.get(&view_id).ok_or("view not found")?;
        if self.fixture_audit.is_some() {
            use crate::ddr_recovery_audit::EventKind;
            if view.released {
                return if self.fixture_audit.as_ref().unwrap().is_poisoned() {
                    Err("fixture audit poisoned; release is not durably confirmed".into())
                } else {
                    Ok(())
                };
            }
            if view.pending {
                return Err("cannot durably release an uncommitted view".into());
            }
            let lease_id = view.lease_id;
            let intent =
                self.record_fixture_cleanup(EventKind::ViewCloseIntent { view_id, lease_id });
            // On an audit failure still allow local safety cleanup; never
            // confirm success or admit new work after the writer is poisoned.
            self.close_view(st, view_id);
            intent?;
            self.record_fixture_cleanup(EventKind::ViewCloseDone { view_id, lease_id })?;
            transfer::map_closed(st, view_id, true);
            return Ok(());
        }
        let _ = view;
        self.close_view(st, view_id);
        transfer::map_closed(st, view_id, true);
        Ok(())
    }

    fn retire(
        &mut self,
        st: &mut Catalog,
        object_id: u64,
        state: &'static str,
    ) -> Result<(), String> {
        let Some(backing) = self.backings.get(&object_id) else {
            return Ok(());
        };
        if backing.state != "active" && backing.state != "pending" {
            if self
                .fixture_audit
                .as_ref()
                .is_some_and(|audit| audit.is_poisoned())
            {
                return Err("fixture audit poisoned; revoke is not durably confirmed".into());
            }
            return Ok(());
        }
        let audit_result = {
            use crate::ddr_recovery_audit::EventKind;
            let generation = backing.resource_generation;
            let kind = if backing.handoff_pending {
                EventKind::RegisterAbort {
                    object_id,
                    resource_generation: generation,
                }
            } else {
                EventKind::Revoke {
                    object_id,
                    resource_generation: generation,
                }
            };
            self.record_fixture_cleanup(kind)
        };
        if let Some(backing) = self.backings.get_mut(&object_id) {
            backing.state = state;
            if let Some(lease) = st.leases.get_mut(&backing.owner_lease_id) {
                lease.state = "Closed".to_string();
            }
            if let Some(object) = st.objects.get_mut(&object_id) {
                object.state = "Retiring".to_string();
            }
            if let Some(object) = st.data_objects.get_mut(&object_id) {
                object.state = "Retiring".to_string();
            }
            if let Some(placement) = st.placements.get_mut(&backing.placement_id) {
                placement.state = "Retiring".to_string();
            }
            if let Some(binding) = st.resource_bindings.get_mut(&backing.binding_id) {
                binding.state = "Retiring".to_string();
            }
            self.epoch += 1;
        }
        audit_result
    }

    fn collect_retired(&mut self, st: &mut Catalog) {
        let ready: Vec<u64> = self
            .backings
            .values()
            .filter(|b| {
                b.state != "active"
                    && b.state != "pending"
                    && b.charged
                    && b.in_flight == 0
                    && !self
                        .views
                        .values()
                        .any(|v| v.object_id == b.object_id && !v.released)
            })
            .map(|b| b.object_id)
            .collect();
        for id in ready {
            {
                use crate::ddr_recovery_audit::EventKind;
                let generation = self.backings[&id].resource_generation;
                if self.fixture_audit.as_ref().is_some_and(|audit| {
                    audit.is_poisoned() || !audit.replay().backing_can_close(id, generation)
                }) {
                    continue;
                }
                if self
                    .record_fixture_cleanup(EventKind::BackingCloseIntent {
                        object_id: id,
                        resource_generation: generation,
                    })
                    .is_err()
                {
                    continue;
                }
            }
            if let Some(backing) = self.backings.get_mut(&id) {
                backing.file = None;
                backing.writer = None;
            }
            #[cfg(test)]
            if let Some(hook) = self.after_close_before_done.take() {
                hook();
            }
            {
                use crate::ddr_recovery_audit::EventKind;
                let generation = self.backings[&id].resource_generation;
                if self
                    .record_fixture_cleanup(EventKind::BackingCloseDone {
                        object_id: id,
                        resource_generation: generation,
                    })
                    .is_err()
                {
                    continue;
                }
            }
            self.backings.get_mut(&id).unwrap().charged = false;
            st.ddr_resources.release_confirmed(id);
            st.objects.remove(&id);
            st.mark_object_released(id);
            self.epoch += 1;
        }
    }

    fn refresh(&mut self, st: &mut Catalog) -> Result<(), String> {
        let mut dead = Vec::new();
        for (id, session) in &self.sessions {
            if session.alive && Self::session_dead(session)? {
                dead.push(*id);
            }
        }
        for id in dead {
            {
                use crate::ddr_recovery_audit::EventKind;
                let birth = self.sessions[&id].birth.clone();
                if self
                    .fixture_audit
                    .as_ref()
                    .is_some_and(|audit| audit.replay().unresolved_holders().contains(&birth))
                {
                    // This pidfd observation is not a normal-exit witness.
                    let _ = self.record_fixture_cleanup(EventKind::PeerTerminated { birth });
                    self.uncertain_exposure = true;
                    if let Some(authority) = self.authority.as_ref() {
                        authority.poison_live_admission();
                    }
                }
            }
            if let Some(session) = self.sessions.get_mut(&id) {
                session.alive = false;
                session.pidfd = None;
                if self.current_pid.get(&session.pid) == Some(&id) {
                    self.current_pid.remove(&session.pid);
                }
            }
            st.clients.remove(&id);
            self.epoch += 1;
            let owned: Vec<u64> = self
                .backings
                .values()
                .filter(|b| {
                    b.owner_client_id == id && (b.state == "active" || b.state == "pending")
                })
                .map(|b| b.object_id)
                .collect();
            for object_id in owned {
                let _ = self.retire(st, object_id, "owner_exited");
            }
            let held: Vec<u64> = self
                .views
                .iter()
                .filter(|(_, v)| v.client_id == id && !v.released)
                .map(|(id, _)| *id)
                .collect();
            for view_id in held {
                if self.fixture_audit.is_some() {
                    let lease_id = self.views[&view_id].lease_id;
                    let _ = self.record_fixture_cleanup(
                        crate::ddr_recovery_audit::EventKind::ViewPeerTerminated {
                            view_id,
                            lease_id,
                        },
                    );
                }
                self.close_view(st, view_id);
            }
        }
        self.collect_retired(st);
        Ok(())
    }

    fn descriptor(&self, backing: &Backing) -> Kv {
        let owner = &self.sessions[&backing.owner_client_id];
        ok(&[
            ("host_id", self.host_id.clone()),
            ("boot_id", self.boot_id.clone()),
            ("broker_instance", self.broker_instance.clone()),
            ("owner_incarnation", owner.incarnation.clone()),
            ("owner_name", backing.owner_name.clone()),
            ("pool_generation", owner.pool_generation.to_string()),
            (
                "resource_generation",
                backing.resource_generation.to_string(),
            ),
            ("object_id", backing.object_id.to_string()),
            ("placement_id", backing.placement_id.to_string()),
            ("binding_id", backing.binding_id.to_string()),
            ("owner_lease_id", backing.owner_lease_id.to_string()),
            ("size_bytes", backing.bytes.to_string()),
            (
                "purpose",
                match backing.purpose {
                    BackingPurpose::Immutable => "immutable",
                    BackingPurpose::Staging => "staging",
                    BackingPurpose::ProbeOnly => "probe_only",
                }
                .to_string(),
            ),
        ])
    }

    fn validate_descriptor(&self, req: &Kv, backing: &Backing) -> Result<(), String> {
        let owner = &self.sessions[&backing.owner_client_id];
        if get(req, "host_id") != self.host_id
            || get(req, "boot_id") != self.boot_id
            || get(req, "broker_instance") != self.broker_instance
            || get(req, "owner_incarnation") != owner.incarnation
            || number(req, "pool_generation")? != owner.pool_generation
            || number(req, "resource_generation")? != backing.resource_generation
        {
            return Err("stale external DDR identity or generation".to_string());
        }
        Ok(())
    }

    fn validate_view_identity(&self, req: &Kv, peer: Peer) -> Result<(), String> {
        // Reject an old broker before even looking up a colliding numeric ID.
        if get(req, "host_id") != self.host_id
            || get(req, "boot_id") != self.boot_id
            || get(req, "broker_instance") != self.broker_instance
        {
            return Err("stale external DDR view identity".to_string());
        }
        let view = self
            .views
            .get(&number(req, "view_id")?)
            .ok_or("view not found")?;
        let backing = self
            .backings
            .get(&view.object_id)
            .ok_or("backing not found")?;
        self.validate_descriptor(req, backing)?;
        let accessor = self
            .sessions
            .get(&view.client_id)
            .ok_or("accessor not found")?;
        if number(req, "object_id")? != view.object_id
            || number(req, "lease_id")? != view.lease_id
            || get(req, "accessor_address_domain") != accessor.incarnation
            || !accessor.alive
            || accessor.pid != peer.pid
            || accessor.uid != peer.uid
            || accessor.gid != peer.gid
            || self.current_pid.get(&peer.pid) != Some(&view.client_id)
        {
            return Err("stale or foreign external DDR view identity".to_string());
        }
        Ok(())
    }

    fn register(
        &mut self,
        st: &mut Catalog,
        client_id: u64,
        req: &Kv,
        file: File,
        purpose: BackingPurpose,
    ) -> Result<Reply, String> {
        if self.host_id.is_empty() || self.boot_id.is_empty() || self.broker_instance.is_empty() {
            return Err("host, boot, or broker identity unavailable".to_string());
        }
        if self.backings.len() >= MAX_BACKING_RECORDS {
            return Err("external DDR backing metadata limit reached".to_string());
        }
        let name = get(req, "name");
        let owner_name = get(req, "owner_name");
        if !valid_name(name) || !valid_name(owner_name) {
            return Err("invalid name or owner_name".to_string());
        }
        let bytes = number(req, "size_bytes")?;
        if bytes == 0 {
            return Err("size_bytes must be positive".to_string());
        }
        let mut stat: libc::stat = unsafe { mem::zeroed() };
        if unsafe { libc::fstat(file.as_raw_fd(), &mut stat) } != 0 {
            return Err("fstat failed".to_string());
        }
        if stat.st_mode & libc::S_IFMT != libc::S_IFREG
            || stat.st_size < 0
            || stat.st_size as u64 != bytes
        {
            return Err("FD must be one complete regular backing of size_bytes".to_string());
        }
        let path = format!("/proc/self/fd/{}", file.as_raw_fd());
        let target = fs::read_link(&path).map_err(|e| format!("inspect FD: {}", e))?;
        if !target.to_string_lossy().starts_with("/memfd:") {
            return Err("FD is not a memfd".to_string());
        }
        let seals = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GET_SEALS) };
        if seals < 0
            || seals & (libc::F_SEAL_GROW | libc::F_SEAL_SHRINK)
                != libc::F_SEAL_GROW | libc::F_SEAL_SHRINK
        {
            return Err("memfd must be sealed against grow and shrink".to_string());
        }
        if purpose != BackingPurpose::Immutable {
            let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL) };
            if flags < 0
                || flags & libc::O_ACCMODE != libc::O_RDWR
                || flags & libc::O_APPEND != 0
                || seals & (libc::F_SEAL_WRITE | libc::F_SEAL_SEAL) != 0
            {
                return Err("staging backing must be writable and sealable".to_string());
            }
            if !self.backings.values().any(|b| {
                b.owner_client_id == client_id
                    && b.state == "active"
                    && b.purpose == BackingPurpose::Immutable
                    && b.file.is_some()
            }) {
                return Err("staging owner requires an active T1 address-domain anchor".to_string());
            }
        }
        if self
            .backings
            .values()
            .any(|b| b.charged && b.dev == stat.st_dev as u64 && b.ino == stat.st_ino as u64)
        {
            return Err("backing already registered".to_string());
        }
        let total = self.budget_committed_bytes();
        let owner_total: u64 = self
            .backings
            .values()
            .filter(|b| b.charged && b.owner_client_id == client_id)
            .map(|b| b.bytes)
            .sum();
        if total
            .checked_add(bytes)
            .is_none_or(|v| v > self.total_limit)
            || owner_total
                .checked_add(bytes)
                .is_none_or(|v| v > self.owner_limit)
        {
            return Err("external DDR quota exceeded".to_string());
        }
        // Reopen read-only: the daemon never calls the create-path pre-touch, which writes zero.
        let readonly = File::open(&path).map_err(|e| format!("open read-only backing: {}", e))?;
        let generation = self.generation();
        let object_id = st.take_next_object_id();
        let placement_id = st.take_next_placement_id();
        st.ddr_resources.admit_existing(
            object_id,
            stat.st_dev as u64,
            stat.st_ino as u64,
            bytes,
        )?;
        let owner = &self.sessions[&client_id];
        let object = Object {
            object_id,
            placement_id,
            block_id: 0,
            placement: "ddr".to_string(),
            target: "host:unknown".to_string(),
            ddr_path: String::new(),
            ddr_fd: -1,
            ddr_service_ptr: 0,
            ddr_service_len: 0,
            requested_bytes: bytes,
            actual_bytes: bytes,
            state: "Creating".to_string(),
            creator_client_id: client_id,
            modified_offset_bytes: 0,
            modified_bytes: 0,
            model_id: String::new(),
            name: name.to_string(),
            role: "user".to_string(),
            shape: String::new(),
            dtype: String::new(),
            immutable: purpose == BackingPurpose::Immutable,
            ddr_fast_profile: "external_memfd".to_string(),
            ddr_madvise_hugepage: false,
            ddr_pretouched: false,
            ddr_prepare_us: 0.0,
            ddr_madvise_us: 0.0,
            ddr_pretouch_us: 0.0,
            ddr_fallback_reason: String::new(),
            ssd_path: String::new(),
            ssd_offset_bytes: 0,
            ssd_alignment_bytes: 0,
            ssd_io_mode: String::new(),
            ssd_file_owned: false,
            ssd_preallocated: false,
        };
        st.objects.insert(object_id, object.clone());
        st.record_legacy_object(&object);
        if let Some(record) = st.data_objects.get_mut(&object_id) {
            record.owner = owner.incarnation.clone();
        }
        if let Some(placement) = st.placements.get_mut(&placement_id) {
            placement.node_id = self.host_id.clone();
            placement.domain = "process".to_string();
            placement.address_kind = "scm_rights_fd".to_string();
            placement.topology_generation = self.epoch + 1;
            placement.capability_generation = 0;
        }
        let binding_id = st
            .resource_bindings
            .values()
            .find(|b| b.placement_id == placement_id)
            .expect("legacy binding inserted")
            .binding_id;
        if let Some(binding) = st.resource_bindings.get_mut(&binding_id) {
            binding.provider_session_id = owner.incarnation.clone();
            binding.provider_resource_id = format!("external:{}:{}", object_id, generation);
            binding.binding_generation = generation;
            binding.opaque_descriptor.clear();
            binding.limitations = vec![
                if purpose == BackingPurpose::Immutable {
                    "cooperative-immutable"
                } else {
                    "exclusive-writer-pending"
                }
                .to_string(),
                "fd-range-not-os-isolated".to_string(),
                "numa-unknown".to_string(),
            ];
        }
        let lease = st.make_lease(client_id, &object, 0, bytes);
        if let Some(record) = st.leases.get_mut(&lease.lease_id) {
            record.state = "Pending".to_string();
        }
        let backing = Backing {
            object_id,
            placement_id,
            binding_id,
            owner_client_id: client_id,
            owner_lease_id: lease.lease_id,
            owner_name: owner_name.to_string(),
            resource_generation: generation,
            bytes,
            dev: stat.st_dev as u64,
            ino: stat.st_ino as u64,
            state: "pending",
            file: Some(readonly),
            charged: true,
            writer: if purpose == BackingPurpose::Immutable {
                None
            } else {
                Some(file)
            },
            purpose,
            handoff_pending: true,
            in_flight: 0,
            probe_completed: false,
        };
        let fields = self.descriptor(&backing);
        {
            use crate::ddr_recovery_audit::{BackingIdentity, EventKind, Purpose};
            let owner = &self.sessions[&client_id];
            let identity = BackingIdentity {
                owner: owner.birth.clone(),
                owner_incarnation: owner.incarnation.clone(),
                pool_generation: owner.pool_generation,
                object_id,
                resource_generation: generation,
                placement_id,
                binding_id,
                binding_generation: generation,
                owner_lease_id: lease.lease_id,
                reservation_id: object_id,
                bytes,
                memfd_dev: stat.st_dev as u64,
                memfd_ino: stat.st_ino as u64,
                memfd_seals: seals,
                purpose: match purpose {
                    BackingPurpose::Immutable => Purpose::Immutable,
                    BackingPurpose::Staging => Purpose::Staging,
                    BackingPurpose::ProbeOnly => Purpose::ProbeOnly,
                },
            };
            // Catalog entries above are provisional under this mutex. Nothing
            // may publish them or send a successful descriptor until this
            // fixture's intent and exact head have been synced.
            if let Err(error) = self.record_fixture_event(EventKind::RegisterIntent(identity)) {
                st.objects.remove(&object_id);
                st.data_objects.remove(&object_id);
                st.placements.remove(&placement_id);
                st.resource_bindings.remove(&binding_id);
                st.leases.remove(&lease.lease_id);
                return Err(error);
            }
        }
        self.backings.insert(object_id, backing);
        self.epoch += 1;
        Ok(Reply {
            fields,
            fd: None,
            undo: Some(Undo::Registration(object_id)),
        })
    }

    fn grant_view(
        &mut self,
        st: &mut Catalog,
        client_id: u64,
        object_id: u64,
        offset: u64,
        bytes: u64,
        parent_view_id: Option<u64>,
    ) -> Result<Reply, String> {
        if self.views.len() >= MAX_VIEW_RECORDS {
            return Err("external DDR view metadata limit reached".to_string());
        }
        let backing = self.backings.get(&object_id).ok_or("backing not found")?;
        if backing.state != "active" || backing.file.is_none() {
            return Err("backing is not accepting imports".to_string());
        }
        if bytes == 0
            || offset
                .checked_add(bytes)
                .is_none_or(|end| end > backing.bytes)
        {
            return Err("view range out of bounds".to_string());
        }
        let fd = backing
            .file
            .as_ref()
            .unwrap()
            .try_clone()
            .map_err(|e| format!("duplicate FD: {}", e))?;
        let mut fields = self.descriptor(backing);
        let placement_id = backing.placement_id;
        let source_binding_id = backing.binding_id;
        let source_generation = backing.resource_generation;
        let owner_incarnation = self.sessions[&backing.owner_client_id].incarnation.clone();
        let accessor_incarnation = self.sessions[&client_id].incarnation.clone();
        let object = st
            .objects
            .get(&object_id)
            .ok_or("catalog object not found")?
            .clone();
        let lease = st.make_lease(client_id, &object, offset, bytes);
        if let Some(record) = st.leases.get_mut(&lease.lease_id) {
            record.state = "Pending".to_string();
        }
        let view_id = st.take_next_access_binding_id();
        st.access_bindings.insert(
            view_id,
            AccessBindingRecord {
                access_binding_id: view_id,
                placement_id,
                source_binding_id,
                accessor_node_id: self.host_id.clone(),
                accessor_device_id: None,
                accessor_provider_id: "local_ddr".to_string(),
                accessor_provider_session_id: accessor_incarnation.clone(),
                accessor_resource_id: format!("fd-view:{}", view_id),
                access_binding_generation: view_id,
                source_binding_generation: source_generation,
                access_kind: AccessKind::Local,
                transport_kind: TransportKind::Local,
                addressability: Addressability::LocallyAddressable,
                access_mode: "read".to_string(),
                allowed_offset_bytes: offset,
                allowed_bytes: bytes,
                topology_generation: self.epoch + 1,
                capability_generation: 0,
                opaque_token: Vec::new(),
                state: "Creating".to_string(),
            },
        );
        {
            use crate::ddr_recovery_audit::{EventKind, ViewIdentity};
            let accessor = &self.sessions[&client_id];
            let identity = ViewIdentity {
                object_id,
                resource_generation: source_generation,
                accessor: accessor.birth.clone(),
                accessor_incarnation: accessor.incarnation.clone(),
                client_id,
                view_id,
                lease_id: lease.lease_id,
                handoff_id: view_id,
                parent_view_id,
                offset,
                bytes,
            };
            // Catalog entries are still provisional under the catalog mutex;
            // no Reply containing an FD may escape before intent + head sync.
            if let Err(error) = self.record_fixture_event(EventKind::ViewIntent(identity)) {
                st.access_bindings.remove(&view_id);
                st.leases.remove(&lease.lease_id);
                return Err(error);
            }
        }
        self.views.insert(
            view_id,
            View {
                object_id,
                client_id,
                lease_id: lease.lease_id,
                offset,
                bytes,
                pending: true,
                released: false,
            },
        );
        self.epoch += 1;
        fields.insert("view_id".to_string(), view_id.to_string());
        fields.insert("lease_id".to_string(), lease.lease_id.to_string());
        fields.insert("allowed_offset_bytes".to_string(), offset.to_string());
        fields.insert("allowed_bytes".to_string(), bytes.to_string());
        fields.insert("access_mode".to_string(), "read".to_string());
        fields.insert("source_address_domain".to_string(), owner_incarnation);
        fields.insert("accessor_address_domain".to_string(), accessor_incarnation);
        let event_token = format!("{}-view-{view_id}", self.broker_instance);
        let event_id = transfer::map_event(st, client_id, view_id, event_token.clone());
        fields.insert("event_id".into(), event_id.to_string());
        fields.insert("event_token".into(), event_token);
        Ok(Reply {
            fields,
            fd: Some(fd),
            undo: Some(Undo::View(view_id)),
        })
    }

    fn snapshot(&self, st: &Catalog) -> Kv {
        let mut entries: Vec<&Backing> = self.backings.values().collect();
        entries.sort_by_key(|b| b.object_id);
        let mut rows = Vec::new();
        for b in entries {
            let s = &self.sessions[&b.owner_client_id];
            let placement_count = st
                .placements
                .values()
                .filter(|p| p.object_id == b.object_id)
                .count();
            rows.push(format!("{{\"object_id\":{},\"placement_id\":{},\"placement_count\":{},\"binding_id\":{},\"owner_name\":\"{}\",\"owner_incarnation\":\"{}\",\"owner_pid\":{},\"owner_alive\":{},\"pool_generation\":{},\"resource_generation\":{},\"size_bytes\":{},\"state\":\"{}\",\"reclaimed\":{},\"numa_node\":null}}",
                b.object_id, b.placement_id, placement_count, b.binding_id, b.owner_name, s.incarnation, s.pid, s.alive,
                s.pool_generation, b.resource_generation, b.bytes, b.state, !b.charged));
        }
        let committed: u64 = self
            .backings
            .values()
            .filter(|b| b.charged)
            .map(|b| b.bytes)
            .sum();
        let active_views = self
            .views
            .values()
            .filter(|v| !v.released && !v.pending)
            .count();
        let held = self.backings.values().filter(|b| b.file.is_some()).count();
        let pidfds = self.sessions.values().filter(|s| s.pidfd.is_some()).count();
        let owner_leases = self
            .backings
            .values()
            .filter(|b| b.state == "active")
            .count();
        let pending_handoffs = self
            .backings
            .values()
            .filter(|b| b.handoff_pending && b.state == "pending")
            .count()
            + self
                .views
                .values()
                .filter(|v| !v.released && v.pending)
                .count();
        let observed_at_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let projection = format!("{{\"schema\":\"uflow-external-ddr-snapshot-v1\",\"host_id\":\"{}\",\"boot_id\":\"{}\",\"broker_instance\":\"{}\",\"lifecycle_epoch\":{},\"observed_at_unix_ns\":{},\"backings\":[{}]}}",
            self.host_id, self.boot_id, self.broker_instance, self.epoch, observed_at_ns, rows.join(","));
        ok(&[
            ("projection_json", projection),
            ("lifecycle_epoch", self.epoch.to_string()),
            ("committed_bytes", committed.to_string()),
            ("backing_fds", held.to_string()),
            ("session_pidfds", pidfds.to_string()),
            ("owner_leases", owner_leases.to_string()),
            ("active_views", active_views.to_string()),
            ("pending_handoffs", pending_handoffs.to_string()),
            ("in_flight", self.copy_work.len().to_string()),
        ])
    }

    fn handle(
        &mut self,
        st: &mut Catalog,
        peer: Peer,
        req: &Kv,
        incoming: Vec<File>,
    ) -> Result<Reply, String> {
        if self
            .authority
            .as_ref()
            .is_some_and(ManagedAuthority::is_inherited_quarantine)
        {
            // Even snapshot must not report the freshly empty in-memory
            // catalog as evidence of zero inherited obligations.
            return Err("authority quarantined; external DDR state unknown".into());
        }
        self.refresh(st)?;
        let op = get(req, "op");
        let registration = matches!(
            op,
            "RegisterExternalBacking" | "RegisterExternalStaging" | "RegisterExternalProbe"
        );
        if registration && incoming.len() != 1 {
            return Err("register requires exactly one FD".to_string());
        }
        if !registration && !incoming.is_empty() {
            return Err("unexpected FD".to_string());
        }
        if op == "GetExternalDdrSnapshot" {
            if self.uncertain_exposure {
                return Err("unresolved exposure; external DDR state unknown".into());
            }
            return Ok(Reply::plain(self.snapshot(st)));
        }
        // Probe targets are broker-owned; only staging is an owner FD handoff.
        if op == "RegisterExternalProbe" && !cfg!(test) {
            return Err("probe targets are private to the broker".to_string());
        }
        if matches!(
            op,
            "RegisterExternalBacking"
                | "RegisterExternalStaging"
                | "RegisterExternalProbe"
                | "AcquireExternalRead"
                | "RetainExternalView"
        ) {
            if st.shutdown_requested {
                return Err("daemon shutdown in progress".to_string());
            }
            self.before_admission()?;
        }
        if op == "RetainExternalView" || op == "ReleaseExternalView" {
            self.validate_view_identity(req, peer)?;
        }
        let client_id = self.session(st, peer)?;
        let result = (|| -> Result<Reply, String> {
            match op {
                "RegisterExternalBacking" => self.register(
                    st,
                    client_id,
                    req,
                    incoming.into_iter().next().unwrap(),
                    BackingPurpose::Immutable,
                ),
                "RegisterExternalStaging" | "RegisterExternalProbe" => {
                    let purpose = if op == "RegisterExternalProbe" {
                        BackingPurpose::ProbeOnly
                    } else {
                        BackingPurpose::Staging
                    };
                    self.register(
                        st,
                        client_id,
                        req,
                        incoming.into_iter().next().unwrap(),
                        purpose,
                    )
                }
                "AcquireExternalRead" => {
                    let object_id = number(req, "object_id")?;
                    let backing = self.backings.get(&object_id).ok_or("backing not found")?;
                    self.validate_descriptor(req, backing)?;
                    if get(req, "access_mode") != "read" {
                        return Err("only read access is supported".to_string());
                    }
                    self.grant_view(
                        st,
                        client_id,
                        object_id,
                        number(req, "offset_bytes")?,
                        number(req, "nbytes")?,
                        None,
                    )
                }
                "RetainExternalView" => {
                    let view_id = number(req, "view_id")?;
                    let view = self.views.get(&view_id).ok_or("view not found")?;
                    if view.client_id != client_id || view.released || view.pending {
                        return Err("view is not active for this process".to_string());
                    }
                    let relative = number(req, "offset_bytes")?;
                    let bytes = number(req, "nbytes")?;
                    if bytes == 0
                        || relative
                            .checked_add(bytes)
                            .is_none_or(|end| end > view.bytes)
                    {
                        return Err("derived view range out of bounds".to_string());
                    }
                    let object_id = view.object_id;
                    let offset = view
                        .offset
                        .checked_add(relative)
                        .ok_or("view offset overflow")?;
                    self.grant_view(st, client_id, object_id, offset, bytes, Some(view_id))
                }
                "ReleaseExternalView" => {
                    let view_id = number(req, "view_id")?;
                    let view = self.views.get(&view_id).ok_or("view not found")?;
                    if view.client_id != client_id {
                        return Err("view belongs to another process".to_string());
                    }
                    self.release_view(st, view_id)?;
                    self.collect_retired(st);
                    if self
                        .fixture_audit
                        .as_ref()
                        .is_some_and(|audit| audit.is_poisoned())
                    {
                        return Err("audit poisoned; close is not durably confirmed".into());
                    }
                    Ok(Reply::plain(ok(&[])))
                }
                "RevokeExternalBacking" => {
                    let object_id = number(req, "object_id")?;
                    let backing = self.backings.get(&object_id).ok_or("backing not found")?;
                    self.validate_descriptor(req, backing)?;
                    if backing.owner_client_id != client_id {
                        return Err("only the owner may revoke".to_string());
                    }
                    let result = self.retire(st, object_id, "revoked");
                    self.collect_retired(st);
                    result?;
                    if self
                        .fixture_audit
                        .as_ref()
                        .is_some_and(|audit| audit.is_poisoned())
                    {
                        return Err("audit poisoned; revoke is not durably confirmed".into());
                    }
                    Ok(Reply::plain(ok(&[])))
                }
                _ => Err("unknown external DDR operation".to_string()),
            }
        })();
        // A provisional reply keeps its session until commit/undo. On other
        // results only detach if no concurrent handoff needs this identity.
        if result.as_ref().map_or(true, |reply| reply.undo.is_none()) {
            self.collect_retired(st);
            self.discard_uncommitted_session(st, client_id);
        }
        result
    }

    fn undo(&mut self, st: &mut Catalog, action: Undo) {
        let client_id = match &action {
            Undo::View(id) => self.views.get(id).map(|view| view.client_id),
            Undo::Registration(id) => self.backings.get(id).map(|backing| backing.owner_client_id),
        };
        if let Undo::View(id) = &action {
            if let Some(view) = self.views.get(id).filter(|view| !view.released) {
                use crate::ddr_recovery_audit::EventKind;
                let kind = if view.pending {
                    EventKind::ViewAbort {
                        view_id: *id,
                        lease_id: view.lease_id,
                    }
                } else {
                    EventKind::ViewCommitUnconfirmed {
                        view_id: *id,
                        lease_id: view.lease_id,
                    }
                };
                // An error/partial send or lost confirmation cannot prove that
                // the peer never obtained the FD. Do not erase the exposure.
                if self.fixture_audit.is_some() {
                    if self.record_fixture_cleanup(kind).is_err() {
                        self.fixture_audit.as_mut().unwrap().poison();
                    }
                    // Once an FD may have been sent, no later in-memory
                    // rollback or cleanup can restore admission/clean.
                    self.uncertain_exposure = true;
                    if let Some(authority) = self.authority.as_ref() {
                        authority.poison_live_admission();
                    }
                }
            }
        }
        match action {
            Undo::View(id) => self.close_view(st, id),
            Undo::Registration(id) => {
                let _ = self.retire(st, id, "registration_rollback");
            }
        }
        self.collect_retired(st);
        if let Some(client_id) = client_id {
            self.discard_uncommitted_session(st, client_id);
        }
    }

    fn commit(&mut self, st: &mut Catalog, action: &Undo) -> Result<(), String> {
        self.refresh(st)?;
        self.before_admission()?;
        let client_id = match action {
            Undo::Registration(id) => {
                let backing = self.backings.get(id).ok_or("registration vanished")?;
                if backing.state != "pending" || !self.sessions[&backing.owner_client_id].alive {
                    return Err("registration owner is no longer alive".to_string());
                }
                if !backing.handoff_pending {
                    return Err("registration handoff already completed".to_string());
                }
                self.record_fixture_event(crate::ddr_recovery_audit::EventKind::RegisterCommit {
                    object_id: *id,
                    resource_generation: backing.resource_generation,
                })?;
                let backing = self.backings.get_mut(id).expect("verified registration");
                backing.handoff_pending = false;
                if backing.purpose == BackingPurpose::Immutable {
                    backing.state = "active";
                    if let Some(object) = st.objects.get_mut(id) {
                        object.state = "Ready".to_string();
                    }
                    if let Some(object) = st.data_objects.get_mut(id) {
                        object.state = "Ready".to_string();
                    }
                    if let Some(placement) = st.placements.get_mut(&backing.placement_id) {
                        placement.state = "Ready".to_string();
                    }
                    if let Some(binding) = st.resource_bindings.get_mut(&backing.binding_id) {
                        binding.state = "Ready".to_string();
                    }
                    if let Some(lease) = st.leases.get_mut(&backing.owner_lease_id) {
                        lease.state = "Active".to_string();
                    }
                }
                backing.owner_client_id
            }
            Undo::View(id) => {
                let view = self.views.get(id).ok_or("view vanished")?;
                if view.released
                    || !view.pending
                    || !self.sessions[&view.client_id].alive
                    || self
                        .backings
                        .get(&view.object_id)
                        .is_none_or(|b| b.state != "active")
                {
                    return Err("view or owner is no longer active".to_string());
                }
                self.record_fixture_event(crate::ddr_recovery_audit::EventKind::ViewCommit {
                    view_id: *id,
                    lease_id: view.lease_id,
                })?;
                let view = self.views.get_mut(id).expect("verified view");
                view.pending = false;
                transfer::map_committed(st, *id, view.bytes);
                if let Some(lease) = st.leases.get_mut(&view.lease_id) {
                    lease.state = "Active".to_string();
                }
                if let Some(access) = st.access_bindings.get_mut(id) {
                    access.state = "Ready".to_string();
                }
                view.client_id
            }
        };
        self.sessions
            .get_mut(&client_id)
            .expect("handoff session")
            .committed = true;
        self.epoch += 1;
        Ok(())
    }
}

pub(crate) fn peer_credentials(stream: &UnixStream) -> io::Result<Peer> {
    let mut cred: libc::ucred = unsafe { mem::zeroed() };
    let mut len = mem::size_of::<libc::ucred>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            &mut cred as *mut _ as *mut _,
            &mut len,
        )
    };
    if rc != 0 || len as usize != mem::size_of::<libc::ucred>() {
        return Err(io::Error::last_os_error());
    }
    if cred.uid != unsafe { libc::geteuid() } || cred.pid <= 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "peer UID denied",
        ));
    }
    Ok(Peer {
        pid: cred.pid,
        uid: cred.uid,
        gid: cred.gid,
    })
}

fn receive(stream: &mut UnixStream) -> io::Result<(String, Vec<File>)> {
    let mut data = [0u8; MAX_FRAME];
    let mut control = [0usize; 64];
    let mut iov = libc::iovec {
        iov_base: data.as_mut_ptr() as *mut _,
        iov_len: data.len(),
    };
    let mut msg: libc::msghdr = unsafe { mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr() as *mut _;
    msg.msg_controllen = mem::size_of_val(&control);
    let count = unsafe { libc::recvmsg(stream.as_raw_fd(), &mut msg, libc::MSG_CMSG_CLOEXEC) };
    if count <= 0 {
        return Err(io::Error::last_os_error());
    }
    let mut files = Vec::new();
    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS {
                let header = libc::CMSG_LEN(0) as usize;
                let payload = ((*cmsg).cmsg_len as usize).saturating_sub(header);
                let count = payload / mem::size_of::<RawFd>();
                for index in 0..count {
                    let raw = std::ptr::read_unaligned(
                        (libc::CMSG_DATA(cmsg) as *const RawFd).add(index),
                    );
                    files.push(File::from_raw_fd(raw));
                }
            }
            cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
        }
    }
    if msg.msg_flags & libc::MSG_CTRUNC != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "truncated ancillary data",
        ));
    }
    let mut bytes = data[..count as usize].to_vec();
    while !bytes.contains(&b'\n') && bytes.len() < MAX_FRAME {
        let mut more = [0u8; 1024];
        let n = stream.read(&mut more)?;
        if n == 0 {
            break;
        }
        bytes.extend_from_slice(&more[..n]);
    }
    if bytes.len() > MAX_FRAME
        || !bytes.ends_with(b"\n")
        || bytes[..bytes.len() - 1].contains(&b'\n')
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid request frame",
        ));
    }
    let line = String::from_utf8(bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid UTF-8"))?;
    Ok((line, files))
}

fn send_reply(stream: &mut UnixStream, fields: &Kv, fd: Option<&File>) -> io::Result<()> {
    let data = encode(fields).into_bytes();
    if let Some(fd) = fd {
        let mut control = [0usize; 8];
        let mut iov = libc::iovec {
            iov_base: data.as_ptr() as *mut _,
            iov_len: data.len(),
        };
        let mut msg: libc::msghdr = unsafe { mem::zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = control.as_mut_ptr() as *mut _;
        msg.msg_controllen = unsafe { libc::CMSG_SPACE(mem::size_of::<RawFd>() as u32) as usize };
        unsafe {
            let cmsg = libc::CMSG_FIRSTHDR(&msg);
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len = libc::CMSG_LEN(mem::size_of::<RawFd>() as u32) as usize;
            std::ptr::write_unaligned(libc::CMSG_DATA(cmsg) as *mut RawFd, fd.as_raw_fd());
        }
        let sent = unsafe { libc::sendmsg(stream.as_raw_fd(), &msg, libc::MSG_NOSIGNAL) };
        if sent <= 0 {
            return Err(io::Error::last_os_error());
        }
        stream.write_all(&data[sent as usize..])
    } else {
        stream.write_all(&data)
    }
}

fn serve(mut stream: UnixStream, shared: SharedCatalog) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    let peer = match peer_credentials(&stream) {
        Ok(peer) => peer,
        Err(_) => return,
    };
    let (line, incoming) = match receive(&mut stream) {
        Ok(v) => v,
        Err(_) => return,
    };
    let req = decode(&line);
    // Declared before every request lock: flush only after all guards drop.
    let _diagnostics = uf_core::topology::ddr::diagnostic::Scope::new(format!(
        "op={} peer_pid={} uid={} gid={}",
        get(&req, "op"),
        peer.pid,
        peer.uid,
        peer.gid
    ));
    if transfer::is_transfer_op(get(&req, "op")) {
        let fields = if incoming.is_empty() {
            crate::router::handle_external_request(&req, peer, &shared)
        } else {
            err("unexpected FD")
        };
        let _ = send_reply(&mut stream, &fields, None);
        return;
    }
    let mut reply = {
        let (lock, _) = &*shared;
        let mut st = {
            let _wait = uf_core::topology::ddr::diagnostic::span("catalog.wait");
            lock.lock().unwrap()
        };
        let _hold = uf_core::topology::ddr::diagnostic::span("catalog.hold");
        let mut external = mem::take(&mut st.external_ddr);
        let result = external.handle(&mut st, peer, &req, incoming);
        st.external_ddr = external;
        match result {
            Ok(reply) => reply,
            Err(message) => Reply::plain(err(message.replace(';', ",").replace('=', ":"))),
        }
    };
    let sent = send_reply(&mut stream, &reply.fields, reply.fd.as_ref()).is_ok();
    // sendmsg transfers its own reference. Close the handler's duplicate before
    // an ACK can make the view releasable or a concurrent revoke reclaim quota.
    // A failed/partial send is still treated as a possible external exposure.
    drop(reply.fd.take());
    #[cfg(test)]
    {
        let hook = shared
            .0
            .lock()
            .unwrap()
            .external_ddr
            .after_handoff_send
            .take();
        if let Some(hook) = hook {
            hook();
        }
    }
    let committed = if sent && reply.undo.is_some() {
        // A registration or new view is provisional until the recipient has
        // received its descriptor/FD and successfully created its mapping.
        let mut ack = [0u8; 3];
        if stream.read_exact(&mut ack).is_ok() && &ack == b"OK\n" {
            let accepted = {
                let (lock, _) = &*shared;
                let mut st = lock.lock().unwrap();
                let mut external = mem::take(&mut st.external_ddr);
                let accepted = external
                    .commit(&mut st, reply.undo.as_ref().unwrap())
                    .is_ok();
                st.external_ddr = external;
                accepted
            };
            if accepted {
                stream.write_all(b"committed\n").is_ok()
            } else {
                let _ = stream.write_all(b"aborted\n");
                false
            }
        } else {
            false
        }
    } else {
        sent
    };
    if !committed {
        if let Some(undo) = reply.undo.take() {
            let (lock, _) = &*shared;
            let mut st = lock.lock().unwrap();
            let mut external = mem::take(&mut st.external_ddr);
            external.undo(&mut st, undo);
            st.external_ddr = external;
        }
    }
}

pub struct Broker {
    stop: Arc<AtomicBool>,
    worker: JoinHandle<bool>,
    path: String,
    path_dev: u64,
    path_ino: u64,
    shared: SharedCatalog,
}

impl Broker {
    pub fn stop(self, control_drained: bool) -> Result<(), String> {
        self.stop.store(true, Ordering::Release);
        let joined = self.worker.join().unwrap_or(false);
        // Never hold the catalog mutex while waiting for a copy to finish.
        let copies_drained = copy_admission::stop_and_join_workers(&self.shared);
        let result = if joined && control_drained && copies_drained.is_ok() {
            let (lock, _) = &*self.shared;
            let mut st = lock.lock().unwrap();
            let mut external = mem::take(&mut st.external_ddr);
            let proof = JoinedDrain {
                broker: external.broker_instance.clone(),
            };
            let result = external.finish_shutdown(&mut st, &proof);
            if result.is_err() {
                if let Some(authority) = external.authority.as_ref() {
                    authority.poison_live_admission();
                }
            }
            st.external_ddr = external;
            match &result {
                Ok(()) if st.external_ddr.bound_quarantine_lifecycle => {
                    eprintln!("external DDR quarantine lifecycle drained; BIND awaits SEAL")
                }
                Ok(())
                    if st
                        .external_ddr
                        .authority
                        .as_ref()
                        .is_some_and(ManagedAuthority::is_inherited_quarantine) =>
                {
                    eprintln!(
                        "external DDR quarantine diagnostics stopped; authority remains unknown"
                    )
                }
                Ok(()) => eprintln!("external DDR authority drained and clean"),
                Err(e) => eprintln!("external DDR shutdown left authority quarantined: {}", e),
            }
            result
        } else {
            let error = format!(
                "control, external DDR handler or copy worker drain failed; authority remains dirty: {:?}",
                copies_drained.err()
            );
            eprintln!("{error}");
            if let Some(authority) = self
                .shared
                .0
                .lock()
                .unwrap()
                .external_ddr
                .authority
                .as_ref()
            {
                authority.poison_live_admission();
            }
            Err(error)
        };
        if fs::symlink_metadata(&self.path).is_ok_and(|meta| {
            meta.file_type().is_socket()
                && meta.dev() == self.path_dev
                && meta.ino() == self.path_ino
        }) {
            let _ = fs::remove_file(self.path);
        }
        result
    }
}

pub fn start(control_socket: &str, shared: SharedCatalog) -> io::Result<Broker> {
    {
        let (lock, _) = &*shared;
        if !lock.lock().unwrap().external_ddr.has_authority() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "managed authority unavailable",
            ));
        }
    }
    let path = format!("{}.external-ddr", control_socket);
    let listener = UnixListener::bind(&path)?;
    fs::set_permissions(&path, Permissions::from_mode(0o600))?;
    listener.set_nonblocking(true)?;
    let metadata = fs::symlink_metadata(&path)?;
    let discovery =
        uf_core::topology::ddr::DdrDiscovery::new(uf_core::topology::ddr::DiscoveryConfig {
            realm: "external-ddr".into(),
            control_socket: control_socket.into(),
            max_age: Duration::from_secs(5),
            host_budget_limit_bytes: shared.0.lock().unwrap().external_ddr.total_limit,
        })
        .map_err(io::Error::other)?;
    shared.0.lock().unwrap().external_ddr.discovery =
        Some(Arc::new(std::sync::Mutex::new(discovery)));
    let stop = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&stop);
    let worker_shared = Arc::clone(&shared);
    let worker = thread::spawn(move || {
        let mut handlers: Vec<JoinHandle<()>> = Vec::new();
        let mut drained = true;
        while !flag.load(Ordering::Acquire) {
            match listener.accept() {
                Ok((stream, _)) => {
                    let shared = Arc::clone(&worker_shared);
                    match thread::Builder::new().spawn(move || serve(stream, shared)) {
                        Ok(handler) => handlers.push(handler),
                        Err(e) => eprintln!("external DDR handler spawn failed: {}", e),
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5))
                }
                Err(_) => break,
            }
            let mut active = Vec::new();
            for handler in handlers.drain(..) {
                if handler.is_finished() {
                    drained &= handler.join().is_ok();
                } else {
                    active.push(handler);
                }
            }
            handlers = active;
        }
        drop(listener);
        for handler in handlers {
            drained &= handler.join().is_ok();
        }
        drained
    });
    Ok(Broker {
        stop,
        worker,
        path,
        path_dev: metadata.dev(),
        path_ino: metadata.ino(),
        shared,
    })
}
