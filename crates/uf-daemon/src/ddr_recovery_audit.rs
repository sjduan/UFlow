//! WIP in-memory recovery-obligation replay rules.
//!
//! This is not a second sellable catalog, a durable event codec, a trusted
//! witness verifier or a recovery operation. Only a trusted writer which has
//! already established the actual ordering of FD sends/closes and worker joins
//! can produce valid events. In particular an abort never proves an FD was not
//! delivered. Production admission remains disabled while the journal codec,
//! lifecycle barriers and trusted witness are not wired together.
#![allow(dead_code)]

use crate::ddr_process_identity::ProcessBirth;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Domain {
    pub host: String,
    pub boot: String,
    pub domain: String,
    pub cap_revision: u64,
    pub broker: String,
    pub owner_limit: u64,
    pub total_limit: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Purpose {
    Immutable,
    Staging,
    ProbeOnly,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BackingIdentity {
    pub owner: ProcessBirth,
    pub owner_incarnation: String,
    pub pool_generation: u64,
    pub object_id: u64,
    pub resource_generation: u64,
    pub placement_id: u64,
    pub binding_id: u64,
    pub binding_generation: u64,
    pub owner_lease_id: u64,
    pub reservation_id: u64,
    pub bytes: u64,
    pub memfd_dev: u64,
    pub memfd_ino: u64,
    pub memfd_seals: i32,
    pub purpose: Purpose,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ViewIdentity {
    pub object_id: u64,
    pub resource_generation: u64,
    pub accessor: ProcessBirth,
    pub accessor_incarnation: String,
    pub client_id: u64,
    pub view_id: u64,
    pub lease_id: u64,
    pub handoff_id: u64,
    pub parent_view_id: Option<u64>,
    pub offset: u64,
    pub bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WorkIdentity {
    pub work_id: u64,
    pub source_id: u64,
    pub source_generation: u64,
    pub target_id: u64,
    pub target_generation: u64,
    pub src_offset: u64,
    pub dst_offset: u64,
    pub bytes: u64,
    pub snapshot_version: String,
    pub evidence_version: String,
    pub cost_version: String,
    pub worker_id: String,
    pub purpose: Purpose,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DrainSnapshot {
    pub handlers: u64,
    pub pending_handoffs: u64,
    pub retained_backings: u64,
    pub active_views: u64,
    pub reservations: u64,
    pub in_flight: u64,
    pub potential_holders: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum EventKind {
    /// A kernel-observed broker incarnation, not an exit or recovery proof.
    BrokerBirth {
        birth: ProcessBirth,
    },
    RegisterIntent(BackingIdentity),
    RegisterCommit {
        object_id: u64,
        resource_generation: u64,
    },
    RegisterAbort {
        object_id: u64,
        resource_generation: u64,
    },
    Revoke {
        object_id: u64,
        resource_generation: u64,
    },
    BackingCloseIntent {
        object_id: u64,
        resource_generation: u64,
    },
    BackingCloseDone {
        object_id: u64,
        resource_generation: u64,
    },
    ViewIntent(ViewIdentity),
    ViewCommit {
        view_id: u64,
        lease_id: u64,
    },
    ViewAbort {
        view_id: u64,
        lease_id: u64,
    },
    // ACK was received and the view committed, but delivery of the final
    // confirmation failed. The accessor may still have the FD or mapping.
    ViewCommitUnconfirmed {
        view_id: u64,
        lease_id: u64,
    },
    ViewNotSent {
        view_id: u64,
        lease_id: u64,
    },
    // A pidfd observation is not a trusted normal-exit witness.
    ViewPeerTerminated {
        view_id: u64,
        lease_id: u64,
    },
    ViewCloseIntent {
        view_id: u64,
        lease_id: u64,
    },
    ViewCloseDone {
        view_id: u64,
        lease_id: u64,
    },
    WorkIntent(WorkIdentity),
    CancelRequest {
        work_id: u64,
    },
    WorkDrained {
        work_id: u64,
    },
    /// Real I/O completion, not exact byte/probe verification.
    WorkIoCompleted {
        work_id: u64,
        copied: u64,
    },
    WorkVerified {
        work_id: u64,
        copied: u64,
        verified: u64,
    },
    PublishIntent {
        work_id: u64,
    },
    PublishDone {
        work_id: u64,
        seals: u32,
    },
    PublishAbort {
        work_id: u64,
    },
    PeerTerminated {
        birth: ProcessBirth,
    },
    BrokerDrained(DrainSnapshot),
    /// A checkpoint is valid only immediately after a verified, zero-obligation
    /// broker drain; it is not evidence that an unknown dirty generation ended.
    CleanCheckpoint(DrainSnapshot),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Event {
    pub domain: Domain,
    pub epoch: u64,
    pub kind: EventKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BackingState {
    Provisional,
    Active,
    Pending,
    Publishing,
    Retired,
    Closing,
    Closed,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ViewState {
    Intent,
    Active,
    PossiblyExposed,
    Closing,
    Closed,
    NotSent,
}

#[derive(Clone, PartialEq, Eq)]
struct Backing {
    identity: BackingIdentity,
    state: BackingState,
    publishing_work: Option<u64>,
}
#[derive(Clone, PartialEq, Eq)]
struct View {
    identity: ViewIdentity,
    state: ViewState,
}
#[derive(Clone, PartialEq, Eq)]
struct Work {
    identity: WorkIdentity,
    cancelled: bool,
    drained: bool,
    verified: bool,
    io_completed: bool,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct AuditReplay {
    domain: Domain,
    epoch: u64,
    broker_birth: Option<ProcessBirth>,
    backings: BTreeMap<u64, Backing>,
    views: BTreeMap<u64, View>,
    works: BTreeMap<u64, Work>,
    terminated: Vec<ProcessBirth>,
    drained: bool,
    drained_snapshot: Option<DrainSnapshot>,
    checkpointed: bool,
}

fn valid_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 96
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}

fn valid_birth(birth: &ProcessBirth) -> bool {
    birth.pid > 0 && birth.pid_namespace_ino != 0 && birth.starttime_ticks != 0
}

fn range(offset: u64, bytes: u64, size: u64) -> bool {
    bytes != 0 && offset.checked_add(bytes).is_some_and(|end| end <= size)
}

impl AuditReplay {
    pub(crate) fn new(domain: Domain) -> Result<Self, &'static str> {
        if !valid_name(&domain.host)
            || !valid_name(&domain.boot)
            || !valid_name(&domain.domain)
            || !valid_name(&domain.broker)
            || domain.cap_revision == 0
            || domain.owner_limit == 0
            || domain.total_limit < domain.owner_limit
        {
            return Err("invalid audit domain");
        }
        Ok(Self {
            domain,
            epoch: 0,
            broker_birth: None,
            backings: BTreeMap::new(),
            views: BTreeMap::new(),
            works: BTreeMap::new(),
            terminated: Vec::new(),
            drained: false,
            drained_snapshot: None,
            checkpointed: false,
        })
    }

    /// This replay covers one broker incarnation after its latest clean
    /// checkpoint. Calling code must separately validate framing, event codec,
    /// checkpoint boundaries and the complete journal's broker transitions.
    pub(crate) fn apply(&mut self, event: Event) -> Result<(), &'static str> {
        if event.domain != self.domain
            || event.epoch < self.epoch
            || self.checkpointed
            || (self.drained && !matches!(event.kind, EventKind::CleanCheckpoint(_)))
        {
            return Err("audit domain or epoch mismatch");
        }
        match event.kind {
            EventKind::BrokerBirth { birth } => {
                if !valid_birth(&birth)
                    || self.broker_birth.is_some()
                    || event.epoch != 0
                    || !self.backings.is_empty()
                    || !self.views.is_empty()
                    || !self.works.is_empty()
                    || !self.terminated.is_empty()
                {
                    return Err("invalid or late broker birth");
                }
                self.broker_birth = Some(birth);
            }
            EventKind::RegisterIntent(identity) => {
                if !valid_birth(&identity.owner)
                    || !valid_name(&identity.owner_incarnation)
                    || identity.pool_generation == 0
                    || identity.object_id == 0
                    || identity.resource_generation == 0
                    || identity.placement_id == 0
                    || identity.binding_id == 0
                    || identity.binding_generation == 0
                    || identity.owner_lease_id == 0
                    || identity.reservation_id == 0
                    || identity.bytes == 0
                    || identity.memfd_ino == 0
                    || self.backings.contains_key(&identity.object_id)
                    || self.terminated.contains(&identity.owner)
                {
                    return Err("invalid or duplicate backing intent");
                }
                if self.backings.values().any(|b| {
                    b.identity.reservation_id == identity.reservation_id
                        || b.identity.placement_id == identity.placement_id
                        || b.identity.binding_id == identity.binding_id
                        || b.identity.owner_lease_id == identity.owner_lease_id
                        || (b.state != BackingState::Closed
                            && b.identity.memfd_dev == identity.memfd_dev
                            && b.identity.memfd_ino == identity.memfd_ino)
                }) {
                    return Err("backing identity or reservation reused");
                }
                if self.backings.values().any(|b| {
                    (b.identity.owner_incarnation == identity.owner_incarnation
                        && (b.identity.owner != identity.owner
                            || b.identity.pool_generation != identity.pool_generation))
                        || (b.identity.owner_incarnation != identity.owner_incarnation
                            && b.identity.pool_generation == identity.pool_generation)
                }) {
                    return Err("owner incarnation or pool identity mismatch");
                }
                let total = self
                    .charged_bytes()
                    .checked_add(identity.bytes)
                    .ok_or("total overflow")?;
                let owner_total = self
                    .owner_charged(&identity.owner_incarnation)
                    .checked_add(identity.bytes)
                    .ok_or("owner overflow")?;
                if total > self.domain.total_limit || owner_total > self.domain.owner_limit {
                    return Err("audit budget exceeded");
                }
                self.backings.insert(
                    identity.object_id,
                    Backing {
                        identity,
                        state: BackingState::Provisional,
                        publishing_work: None,
                    },
                );
            }
            EventKind::RegisterCommit {
                object_id,
                resource_generation,
            } => {
                let owner = &self.backing(object_id, resource_generation)?.identity.owner;
                if self.terminated.contains(owner) {
                    return Err("registration owner terminated");
                }
                let backing = self.backing_mut(object_id, resource_generation)?;
                if backing.state != BackingState::Provisional {
                    return Err("invalid registration commit");
                }
                backing.state = if backing.identity.purpose == Purpose::Immutable {
                    BackingState::Active
                } else {
                    BackingState::Pending
                };
            }
            EventKind::RegisterAbort {
                object_id,
                resource_generation,
            } => {
                let backing = self.backing_mut(object_id, resource_generation)?;
                if backing.state != BackingState::Provisional {
                    return Err("invalid registration abort");
                }
                backing.state = BackingState::Retired;
            }
            EventKind::Revoke {
                object_id,
                resource_generation,
            } => {
                let backing = self.backing_mut(object_id, resource_generation)?;
                if !matches!(backing.state, BackingState::Active | BackingState::Pending) {
                    return Err("invalid revoke");
                }
                backing.state = BackingState::Retired;
            }
            EventKind::BackingCloseIntent {
                object_id,
                resource_generation,
            } => {
                let backing = self.backing_mut(object_id, resource_generation)?;
                if backing.state != BackingState::Retired {
                    return Err("invalid backing close intent");
                }
                backing.state = BackingState::Closing;
            }
            EventKind::BackingCloseDone {
                object_id,
                resource_generation,
            } => {
                if self.views.values().any(|v| {
                    v.identity.object_id == object_id
                        && !matches!(v.state, ViewState::Closed | ViewState::NotSent)
                }) || self.works.values().any(|w| {
                    !w.drained
                        && (w.identity.source_id == object_id || w.identity.target_id == object_id)
                }) {
                    return Err("backing has unresolved views or work");
                }
                let backing = self.backing_mut(object_id, resource_generation)?;
                if backing.state != BackingState::Closing {
                    return Err("missing backing close intent");
                }
                backing.state = BackingState::Closed;
            }
            EventKind::ViewIntent(identity) => {
                if !valid_birth(&identity.accessor)
                    || !valid_name(&identity.accessor_incarnation)
                    || identity.client_id == 0
                    || identity.view_id == 0
                    || identity.lease_id == 0
                    || identity.handoff_id == 0
                    || self.views.contains_key(&identity.view_id)
                    || self.views.values().any(|v| {
                        v.identity.lease_id == identity.lease_id
                            || v.identity.handoff_id == identity.handoff_id
                    })
                    || self
                        .backings
                        .values()
                        .any(|b| b.identity.owner_lease_id == identity.lease_id)
                {
                    return Err("invalid or duplicate view intent");
                }
                if self.views.values().any(|v| {
                    (v.identity.client_id == identity.client_id
                        && (v.identity.accessor != identity.accessor
                            || v.identity.accessor_incarnation != identity.accessor_incarnation))
                        || (v.identity.accessor_incarnation == identity.accessor_incarnation
                            && (v.identity.accessor != identity.accessor
                                || v.identity.client_id != identity.client_id))
                }) {
                    return Err("accessor incarnation or client identity mismatch");
                }
                let backing = self.backing(identity.object_id, identity.resource_generation)?;
                if backing.state != BackingState::Active
                    || self.terminated.contains(&backing.identity.owner)
                    || self.terminated.contains(&identity.accessor)
                    || !range(identity.offset, identity.bytes, backing.identity.bytes)
                {
                    return Err("view source or range invalid");
                }
                if let Some(parent_id) = identity.parent_view_id {
                    let parent = self.views.get(&parent_id).ok_or("missing parent view")?;
                    let v = &parent.identity;
                    if parent.state != ViewState::Active
                        || v.object_id != identity.object_id
                        || v.resource_generation != identity.resource_generation
                        || v.accessor != identity.accessor
                        || v.client_id != identity.client_id
                        || v.accessor_incarnation != identity.accessor_incarnation
                        || identity.offset < v.offset
                        || identity
                            .offset
                            .checked_add(identity.bytes)
                            .is_none_or(|end| end > v.offset + v.bytes)
                    {
                        return Err("invalid parent view");
                    }
                }
                self.views.insert(
                    identity.view_id,
                    View {
                        identity,
                        state: ViewState::Intent,
                    },
                );
            }
            EventKind::ViewCommit { view_id, lease_id } => {
                let view = self.views.get(&view_id).ok_or("unknown view")?;
                let backing =
                    self.backing(view.identity.object_id, view.identity.resource_generation)?;
                if view.identity.lease_id != lease_id
                    || backing.state != BackingState::Active
                    || self.terminated.contains(&backing.identity.owner)
                    || self.terminated.contains(&view.identity.accessor)
                {
                    return Err("view source retired or lease mismatched");
                }
                let view = self.view_mut(view_id, lease_id)?;
                if view.state != ViewState::Intent {
                    return Err("invalid view commit");
                }
                view.state = ViewState::Active;
            }
            EventKind::ViewAbort { view_id, lease_id } => {
                let view = self.view_mut(view_id, lease_id)?;
                if view.state != ViewState::Intent {
                    return Err("invalid view abort");
                }
                // A failed send/ACK/undo may still have delivered an FD.
                view.state = ViewState::PossiblyExposed;
            }
            EventKind::ViewCommitUnconfirmed { view_id, lease_id } => {
                let view = self.view_mut(view_id, lease_id)?;
                if view.state != ViewState::Active {
                    return Err("view was not committed");
                }
                // An unsuccessful confirmation write does not revoke the FD.
                view.state = ViewState::PossiblyExposed;
            }
            EventKind::ViewNotSent { view_id, lease_id } => {
                let view = self.view_mut(view_id, lease_id)?;
                if view.state != ViewState::Intent {
                    return Err("not-sent proof is too late");
                }
                view.state = ViewState::NotSent;
            }
            EventKind::ViewPeerTerminated { view_id, lease_id } => {
                let accessor = &self
                    .views
                    .get(&view_id)
                    .ok_or("unknown view")?
                    .identity
                    .accessor;
                if !self.terminated.contains(accessor) {
                    return Err("view accessor termination was not observed");
                }
                let view = self.view_mut(view_id, lease_id)?;
                if !matches!(view.state, ViewState::Intent | ViewState::Active) {
                    return Err("terminated view is not live");
                }
                view.state = ViewState::PossiblyExposed;
            }
            EventKind::ViewCloseIntent { view_id, lease_id } => {
                let view = self.view_mut(view_id, lease_id)?;
                if view.state != ViewState::Active {
                    return Err("view has no committed release");
                }
                view.state = ViewState::Closing;
            }
            EventKind::ViewCloseDone { view_id, lease_id } => {
                let view = self.view_mut(view_id, lease_id)?;
                if view.state != ViewState::Closing {
                    return Err("missing view close intent");
                }
                view.state = ViewState::Closed;
            }
            EventKind::WorkIntent(identity) => {
                if identity.work_id == 0
                    || self.works.contains_key(&identity.work_id)
                    || !valid_name(&identity.worker_id)
                    || !valid_name(&identity.snapshot_version)
                    || !valid_name(&identity.evidence_version)
                    || !valid_name(&identity.cost_version)
                    || !matches!(identity.purpose, Purpose::Staging | Purpose::ProbeOnly)
                    || identity.source_id == identity.target_id
                {
                    return Err("invalid work identity");
                }
                let source = self.backing(identity.source_id, identity.source_generation)?;
                let target = self.backing(identity.target_id, identity.target_generation)?;
                if source.state != BackingState::Active || target.state != BackingState::Pending
                    || target.identity.purpose != identity.purpose
                    || self.terminated.contains(&source.identity.owner)
                    || self.terminated.contains(&target.identity.owner)
                    || (source.identity.memfd_dev, source.identity.memfd_ino) == (target.identity.memfd_dev, target.identity.memfd_ino)
                    || !range(identity.src_offset, identity.bytes, source.identity.bytes)
                    || !range(identity.dst_offset, identity.bytes, target.identity.bytes)
                    // A v0 target is single-use even after an earlier worker
                    // drained; retries require a fresh staging backing.
                    || self.works.values().any(|w| w.identity.target_id == identity.target_id)
                {
                    return Err("invalid source, target, range or exclusive writer");
                }
                self.works.insert(
                    identity.work_id,
                    Work {
                        identity,
                        cancelled: false,
                        drained: false,
                        verified: false,
                        io_completed: false,
                    },
                );
            }
            EventKind::CancelRequest { work_id } => {
                let work = self.works.get_mut(&work_id).ok_or("unknown work")?;
                work.cancelled = true; // idempotent; never itself drains.
            }
            EventKind::WorkDrained { work_id } => {
                let work = self.works.get_mut(&work_id).ok_or("unknown work")?;
                if work.drained {
                    return Err("duplicate work drain");
                }
                work.drained = true;
            }
            EventKind::WorkIoCompleted { work_id, copied } => {
                let work = self.works.get(&work_id).ok_or("unknown work")?;
                let source =
                    self.backing(work.identity.source_id, work.identity.source_generation)?;
                let target =
                    self.backing(work.identity.target_id, work.identity.target_generation)?;
                if !work.drained
                    || work.cancelled
                    || work.io_completed
                    || copied != work.identity.bytes
                    || source.state != BackingState::Active
                    || target.state != BackingState::Pending
                    || self.terminated.contains(&source.identity.owner)
                    || self.terminated.contains(&target.identity.owner)
                {
                    return Err("work is not eligible for I/O completion");
                }
                self.works.get_mut(&work_id).unwrap().io_completed = true;
            }
            EventKind::WorkVerified {
                work_id,
                copied,
                verified,
            } => {
                let work = self.works.get(&work_id).ok_or("unknown work")?;
                let source =
                    self.backing(work.identity.source_id, work.identity.source_generation)?;
                let target =
                    self.backing(work.identity.target_id, work.identity.target_generation)?;
                if !work.drained
                    || work.cancelled
                    || work.verified
                    || copied != work.identity.bytes
                    || verified != work.identity.bytes
                    || source.state != BackingState::Active
                    || target.state != BackingState::Pending
                    || self.terminated.contains(&source.identity.owner)
                    || self.terminated.contains(&target.identity.owner)
                {
                    return Err("work is not eligible for verified completion");
                }
                self.works.get_mut(&work_id).unwrap().verified = true;
            }
            EventKind::PublishIntent { work_id } => {
                let work = self.works.get(&work_id).ok_or("unknown work")?;
                if !work.drained
                    || !(work.verified || work.io_completed)
                    || work.cancelled
                    || work.identity.purpose != Purpose::Staging
                {
                    return Err("work cannot publish");
                }
                let source =
                    self.backing(work.identity.source_id, work.identity.source_generation)?;
                let target =
                    self.backing(work.identity.target_id, work.identity.target_generation)?;
                if source.state != BackingState::Active
                    || target.state != BackingState::Pending
                    || self.terminated.contains(&source.identity.owner)
                    || self.terminated.contains(&target.identity.owner)
                {
                    return Err("publication identities became stale");
                }
                let target_id = work.identity.target_id;
                let target_generation = work.identity.target_generation;
                let target = self.backing_mut(target_id, target_generation)?;
                target.state = BackingState::Publishing;
                target.publishing_work = Some(work_id);
            }
            EventKind::PublishDone { work_id, seals } => {
                let work = self.works.get(&work_id).ok_or("unknown work")?;
                if !work.drained
                    || !(work.verified || work.io_completed)
                    || work.cancelled
                    || work.identity.purpose != Purpose::Staging
                {
                    return Err("work cannot finish publication");
                }
                let required = (libc::F_SEAL_GROW
                    | libc::F_SEAL_SHRINK
                    | libc::F_SEAL_WRITE
                    | libc::F_SEAL_SEAL) as u32;
                if seals & required != required {
                    return Err("published target is not fully sealed");
                }
                let source =
                    self.backing(work.identity.source_id, work.identity.source_generation)?;
                let target =
                    self.backing(work.identity.target_id, work.identity.target_generation)?;
                if source.state != BackingState::Active
                    || target.state != BackingState::Publishing
                    || target.publishing_work != Some(work_id)
                    || self.terminated.contains(&source.identity.owner)
                    || self.terminated.contains(&target.identity.owner)
                {
                    return Err("publication identities became stale");
                }
                let target_id = work.identity.target_id;
                let target_generation = work.identity.target_generation;
                let target = self.backing_mut(target_id, target_generation)?;
                target.state = BackingState::Active;
                target.publishing_work = None;
            }
            EventKind::PublishAbort { work_id } => {
                let work = self.works.get(&work_id).ok_or("unknown work")?;
                let target_id = work.identity.target_id;
                let target_generation = work.identity.target_generation;
                let target = self.backing_mut(target_id, target_generation)?;
                if target.state != BackingState::Publishing
                    || target.publishing_work != Some(work_id)
                {
                    return Err("publication intent missing or belongs to another work");
                }
                target.state = BackingState::Pending;
                target.publishing_work = None;
            }
            EventKind::PeerTerminated { birth } => {
                let known_owner = self.backings.values().any(|b| b.identity.owner == birth);
                let known_accessor = self.views.values().any(|v| v.identity.accessor == birth);
                if !known_owner && !known_accessor {
                    return Err("unknown terminated peer birth");
                }
                if !self.terminated.contains(&birth) {
                    self.terminated.push(birth);
                }
                // A pidfd termination observation is not a normal exit proof,
                // and does not erase a potential holder or free a backing.
            }
            EventKind::BrokerDrained(snapshot) => {
                let expected = self.observed_drain(snapshot.handlers, snapshot.reservations);
                if snapshot != expected
                    || snapshot.handlers != 0
                    || snapshot.pending_handoffs != 0
                    || snapshot.in_flight != 0
                {
                    return Err("broker drain counts mismatch or unfinished work");
                }
                self.drained = true;
                self.drained_snapshot = Some(snapshot);
            }
            EventKind::CleanCheckpoint(snapshot) => {
                let expected = self.observed_drain(0, 0);
                if !self.drained
                    || self.drained_snapshot.as_ref() != Some(&snapshot)
                    || snapshot != expected
                    || snapshot.handlers != 0
                    || snapshot.pending_handoffs != 0
                    || snapshot.retained_backings != 0
                    || snapshot.active_views != 0
                    || snapshot.reservations != 0
                    || snapshot.in_flight != 0
                    || snapshot.potential_holders != 0
                {
                    return Err("clean checkpoint requires a verified zero-obligation drain");
                }
                self.checkpointed = true;
            }
        }
        self.epoch = event.epoch;
        Ok(())
    }

    fn backing(&self, object_id: u64, generation: u64) -> Result<&Backing, &'static str> {
        let backing = self.backings.get(&object_id).ok_or("unknown backing")?;
        if backing.identity.resource_generation != generation {
            return Err("backing generation mismatch");
        }
        Ok(backing)
    }
    fn backing_mut(
        &mut self,
        object_id: u64,
        generation: u64,
    ) -> Result<&mut Backing, &'static str> {
        let backing = self.backings.get_mut(&object_id).ok_or("unknown backing")?;
        if backing.identity.resource_generation != generation {
            return Err("backing generation mismatch");
        }
        Ok(backing)
    }
    fn view_mut(&mut self, view_id: u64, lease_id: u64) -> Result<&mut View, &'static str> {
        let view = self.views.get_mut(&view_id).ok_or("unknown view")?;
        if view.identity.lease_id != lease_id {
            return Err("view lease mismatch");
        }
        Ok(view)
    }

    pub(crate) fn charged_bytes(&self) -> u64 {
        self.backings
            .values()
            .filter(|b| b.state != BackingState::Closed)
            .map(|b| b.identity.bytes)
            .sum()
    }
    fn owner_charged(&self, owner: &str) -> u64 {
        self.backings
            .values()
            .filter(|b| b.state != BackingState::Closed && b.identity.owner_incarnation == owner)
            .map(|b| b.identity.bytes)
            .sum()
    }
    pub(crate) fn unresolved_holders(&self) -> Vec<ProcessBirth> {
        // This WIP projection is not proof of the full holder set:
        // This is not a roster: even with a broker birth record it omits the
        // complete historical holder set and process-worker coverage.
        let mut holders = Vec::new();
        for backing in self
            .backings
            .values()
            .filter(|b| b.state != BackingState::Closed)
        {
            if !holders.contains(&backing.identity.owner) {
                holders.push(backing.identity.owner.clone());
            }
        }
        for view in self
            .views
            .values()
            .filter(|v| !matches!(v.state, ViewState::Closed | ViewState::NotSent))
        {
            if !holders.contains(&view.identity.accessor) {
                holders.push(view.identity.accessor.clone());
            }
        }
        // A pidfd proves termination, not a trusted normal exit. Keep each
        // observed peer in the recovery set after local references are closed.
        for birth in &self.terminated {
            if !holders.contains(birth) {
                holders.push(birth.clone());
            }
        }
        holders
    }
    pub(crate) fn active_work(&self) -> usize {
        self.works.values().filter(|w| !w.drained).count()
    }
    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }
    pub(crate) fn broker_birth(&self) -> Option<&ProcessBirth> {
        self.broker_birth.as_ref()
    }
    pub(crate) fn is_checkpointed(&self) -> bool {
        self.checkpointed
    }

    /// Resolve a *copy* of the isolated fixture replay, never the live catalog.
    /// CoverageReport is constructed only by the pinned witness verifier: a
    /// caller cannot supply exit booleans or a list of PIDs. Every outstanding
    /// identity must have a matching verified full birth and every work item
    /// must already be drained. This is not a production recovery authority.
    #[cfg(test)]
    pub(crate) fn resolved_fixture_drain(
        &self,
        coverage: &crate::ddr_recovery_coverage::CoverageReport,
    ) -> Result<DrainSnapshot, &'static str> {
        let mut covered_replay = Self::new(self.domain.clone())?;
        for event in coverage.events() {
            covered_replay.apply(event.clone())?;
        }
        if covered_replay != *self {
            return Err("verified coverage belongs to a different replay");
        }
        let input = self
            .drained_snapshot
            .as_ref()
            .ok_or("missing broker drain")?;
        if !self.drained
            || self.checkpointed
            || input.handlers != 0
            || input.pending_handoffs != 0
            || input.reservations != 0
            || input.in_flight != 0
            || self.works.values().any(|work| !work.drained)
            || self
                .broker_birth
                .as_ref()
                .is_none_or(|birth| !coverage.has_normal_exit(birth))
        {
            return Err("recovery requires verified broker exit and actual drain");
        }
        let mut resolved = self.clone();
        for view in resolved.views.values_mut() {
            if !coverage.has_normal_exit(&view.identity.accessor) {
                return Err("view holder lacks verified normal exit");
            }
            if !matches!(view.state, ViewState::Closed | ViewState::NotSent) {
                view.state = ViewState::Closed;
            }
        }
        for backing in resolved.backings.values_mut() {
            if !coverage.has_normal_exit(&backing.identity.owner) {
                return Err("backing owner lacks verified normal exit");
            }
            if backing.state != BackingState::Closed {
                backing.state = BackingState::Closed;
            }
        }
        if resolved
            .terminated
            .iter()
            .any(|birth| !coverage.has_normal_exit(birth))
        {
            return Err("terminated peer lacks verified normal exit");
        }
        resolved.terminated.clear();
        let after = resolved.observed_drain(input.handlers, input.reservations);
        if after.handlers != 0
            || after.pending_handoffs != 0
            || after.retained_backings != 0
            || after.active_views != 0
            || after.reservations != 0
            || after.in_flight != 0
            || after.potential_holders != 0
        {
            return Err("recovery left unresolved obligations");
        }
        Ok(after)
    }
    /// A runtime view rollback is not proof that an FD was never delivered.
    /// Keep the backing charged if the durable replay still has an unresolved
    /// view or undrained worker, even when the runtime catalog is empty.
    pub(crate) fn backing_can_close(&self, object_id: u64, resource_generation: u64) -> bool {
        self.backing(object_id, resource_generation)
            .is_ok_and(|backing| backing.state == BackingState::Retired)
            && !self.views.values().any(|view| {
                view.identity.object_id == object_id
                    && !matches!(view.state, ViewState::Closed | ViewState::NotSent)
            })
            && !self.works.values().any(|work| {
                !work.drained
                    && (work.identity.source_id == object_id
                        || work.identity.target_id == object_id)
            })
    }
    #[cfg(test)]
    pub(crate) fn view_is_possibly_exposed(&self, view_id: u64) -> bool {
        self.views
            .get(&view_id)
            .is_some_and(|view| view.state == ViewState::PossiblyExposed)
    }
    /// The caller must obtain handler and independent-reservation counts from
    /// the real catalog after joining workers/handlers; they are not known to
    /// this WIP replay. This function does not itself prove drain or clean.
    pub(crate) fn observed_drain(&self, handlers: u64, reservations: u64) -> DrainSnapshot {
        DrainSnapshot {
            handlers,
            pending_handoffs: self
                .backings
                .values()
                .filter(|b| b.state == BackingState::Provisional)
                .count() as u64
                + self
                    .views
                    .values()
                    .filter(|v| v.state == ViewState::Intent)
                    .count() as u64,
            retained_backings: self
                .backings
                .values()
                .filter(|b| b.state != BackingState::Closed)
                .count() as u64,
            active_views: self
                .views
                .values()
                .filter(|v| matches!(v.state, ViewState::Active | ViewState::Closing))
                .count() as u64,
            // A reservation attached to a backing is counted once above;
            // the caller supplies any independent pending reservations.
            reservations,
            in_flight: self.active_work() as u64,
            potential_holders: self.unresolved_holders().len() as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn birth(pid: i32, starttime: u64) -> ProcessBirth {
        ProcessBirth {
            pid,
            uid: 1000,
            gid: 1000,
            pid_namespace_dev: 1,
            pid_namespace_ino: 2,
            starttime_ticks: starttime,
        }
    }
    fn domain() -> Domain {
        Domain {
            host: "host".into(),
            boot: "boot".into(),
            domain: "external-ddr-v0".into(),
            cap_revision: 1,
            broker: "broker-a".into(),
            owner_limit: 8192,
            total_limit: 12288,
        }
    }
    fn backing(id: u64, owner: &str, purpose: Purpose) -> BackingIdentity {
        let owner_id = match owner {
            "a" => 1,
            "b" => 2,
            "c" => 3,
            _ => 4,
        };
        BackingIdentity {
            owner: birth(owner_id + 10, owner_id as u64 + 100),
            owner_incarnation: owner.into(),
            pool_generation: owner_id as u64,
            object_id: id,
            resource_generation: id + 1,
            placement_id: id + 10,
            binding_id: id + 20,
            binding_generation: id + 30,
            owner_lease_id: id + 40,
            reservation_id: id + 50,
            bytes: 4096,
            memfd_dev: 1,
            memfd_ino: id + 1000,
            memfd_seals: 6,
            purpose,
        }
    }
    fn view() -> ViewIdentity {
        ViewIdentity {
            object_id: 1,
            resource_generation: 2,
            accessor: birth(99, 999),
            accessor_incarnation: "accessor".into(),
            client_id: 100,
            view_id: 200,
            lease_id: 300,
            handoff_id: 400,
            parent_view_id: None,
            offset: 17,
            bytes: 19,
        }
    }
    fn event(kind: EventKind) -> Event {
        Event {
            domain: domain(),
            epoch: 1,
            kind,
        }
    }
    fn register(audit: &mut AuditReplay, identity: BackingIdentity) {
        let object_id = identity.object_id;
        let resource_generation = identity.resource_generation;
        audit
            .apply(event(EventKind::RegisterIntent(identity)))
            .unwrap();
        audit
            .apply(event(EventKind::RegisterCommit {
                object_id,
                resource_generation,
            }))
            .unwrap();
    }

    #[test]
    fn broker_birth_must_be_valid_unique_and_precede_resources() {
        let first = Event {
            domain: domain(),
            epoch: 0,
            kind: EventKind::BrokerBirth {
                birth: birth(44, 55),
            },
        };
        let mut audit = AuditReplay::new(domain()).unwrap();
        audit.apply(first.clone()).unwrap();
        assert!(audit.apply(first.clone()).is_err());
        register(&mut audit, backing(1, "a", Purpose::Immutable));
        let mut later = AuditReplay::new(domain()).unwrap();
        register(&mut later, backing(1, "a", Purpose::Immutable));
        assert!(later.apply(first).is_err());
        let mut invalid = AuditReplay::new(domain()).unwrap();
        let bad = Event {
            domain: domain(),
            epoch: 0,
            kind: EventKind::BrokerBirth {
                birth: birth(44, 0),
            },
        };
        assert!(invalid.apply(bad).is_err());
    }

    #[test]
    fn failed_ack_retains_a_possible_holder_until_real_resolution() {
        let mut audit = AuditReplay::new(domain()).unwrap();
        register(&mut audit, backing(1, "a", Purpose::Immutable));
        audit.apply(event(EventKind::ViewIntent(view()))).unwrap();
        audit
            .apply(event(EventKind::ViewAbort {
                view_id: 200,
                lease_id: 300,
            }))
            .unwrap();
        assert_eq!(
            audit.unresolved_holders(),
            vec![birth(11, 101), birth(99, 999)]
        );
        assert!(audit
            .apply(event(EventKind::ViewNotSent {
                view_id: 200,
                lease_id: 300
            }))
            .is_err());
        assert!(audit
            .apply(event(EventKind::ViewCloseDone {
                view_id: 200,
                lease_id: 300
            }))
            .is_err());
        audit
            .apply(event(EventKind::Revoke {
                object_id: 1,
                resource_generation: 2,
            }))
            .unwrap();
        audit
            .apply(event(EventKind::BackingCloseIntent {
                object_id: 1,
                resource_generation: 2,
            }))
            .unwrap();
        assert!(audit
            .apply(event(EventKind::BackingCloseDone {
                object_id: 1,
                resource_generation: 2
            }))
            .is_err());
        assert_eq!(audit.charged_bytes(), 4096);
    }

    #[test]
    fn peer_death_is_not_normal_exit_and_nonzero_drain_does_not_clear_holders() {
        let mut audit = AuditReplay::new(domain()).unwrap();
        register(&mut audit, backing(1, "a", Purpose::Immutable));
        audit.apply(event(EventKind::ViewIntent(view()))).unwrap();
        audit
            .apply(event(EventKind::ViewAbort {
                view_id: 200,
                lease_id: 300,
            }))
            .unwrap();
        audit
            .apply(event(EventKind::PeerTerminated {
                birth: birth(99, 999),
            }))
            .unwrap();
        assert_eq!(
            audit.unresolved_holders(),
            vec![birth(11, 101), birth(99, 999)]
        );
        let mut false_drain = audit.observed_drain(0, 0);
        false_drain.potential_holders = 0;
        assert!(audit
            .apply(event(EventKind::BrokerDrained(false_drain)))
            .is_err());
        let handlers_not_drained = audit.observed_drain(1, 0);
        assert!(audit
            .apply(event(EventKind::BrokerDrained(handlers_not_drained)))
            .is_err());
        let mut with_reservation = audit.clone();
        let reservation_not_drained = with_reservation.observed_drain(0, 1);
        with_reservation
            .apply(event(EventKind::BrokerDrained(reservation_not_drained)))
            .unwrap();
        let forged_zero = with_reservation.observed_drain(0, 0);
        assert!(with_reservation
            .apply(event(EventKind::CleanCheckpoint(forged_zero)))
            .is_err());
        let actual = audit.observed_drain(0, 0);
        assert_eq!(actual.retained_backings, 1);
        assert_eq!(actual.potential_holders, 2);
        audit
            .apply(event(EventKind::BrokerDrained(actual)))
            .unwrap();
        assert!(audit
            .apply(event(EventKind::Revoke {
                object_id: 1,
                resource_generation: 2
            }))
            .is_err());
        assert_eq!(audit.charged_bytes(), 4096);
    }

    #[test]
    fn terminated_live_view_remains_a_possible_exposure_and_blocks_reuse() {
        let mut audit = AuditReplay::new(domain()).unwrap();
        register(&mut audit, backing(1, "a", Purpose::Immutable));
        audit.apply(event(EventKind::ViewIntent(view()))).unwrap();
        audit
            .apply(event(EventKind::ViewCommit {
                view_id: 200,
                lease_id: 300,
            }))
            .unwrap();
        assert!(audit
            .apply(event(EventKind::ViewPeerTerminated {
                view_id: 200,
                lease_id: 300
            }))
            .is_err());
        audit
            .apply(event(EventKind::PeerTerminated {
                birth: birth(99, 999),
            }))
            .unwrap();
        audit
            .apply(event(EventKind::ViewPeerTerminated {
                view_id: 200,
                lease_id: 300,
            }))
            .unwrap();
        audit
            .apply(event(EventKind::Revoke {
                object_id: 1,
                resource_generation: 2,
            }))
            .unwrap();
        assert!(!audit.backing_can_close(1, 2));
        assert_eq!(audit.charged_bytes(), 4096);
        assert!(audit.unresolved_holders().contains(&birth(99, 999)));
    }

    #[test]
    fn closed_local_backing_does_not_erase_terminated_peer_evidence() {
        let mut audit = AuditReplay::new(domain()).unwrap();
        register(&mut audit, backing(1, "a", Purpose::Immutable));
        let owner = birth(11, 101);
        audit
            .apply(event(EventKind::PeerTerminated {
                birth: owner.clone(),
            }))
            .unwrap();
        audit
            .apply(event(EventKind::Revoke {
                object_id: 1,
                resource_generation: 2,
            }))
            .unwrap();
        audit
            .apply(event(EventKind::BackingCloseIntent {
                object_id: 1,
                resource_generation: 2,
            }))
            .unwrap();
        audit
            .apply(event(EventKind::BackingCloseDone {
                object_id: 1,
                resource_generation: 2,
            }))
            .unwrap();
        assert_eq!(audit.charged_bytes(), 0);
        assert_eq!(audit.unresolved_holders(), vec![owner]);
        assert_eq!(audit.observed_drain(0, 0).potential_holders, 1);
    }

    #[test]
    fn terminated_birth_cannot_receive_or_commit_new_work() {
        let mut audit = AuditReplay::new(domain()).unwrap();
        register(&mut audit, backing(1, "a", Purpose::Immutable));
        audit.apply(event(EventKind::ViewIntent(view()))).unwrap();
        audit
            .apply(event(EventKind::PeerTerminated {
                birth: birth(11, 101),
            }))
            .unwrap();
        assert!(audit
            .apply(event(EventKind::ViewCommit {
                view_id: 200,
                lease_id: 300
            }))
            .is_err());
        let mut next_view = view();
        next_view.view_id = 201;
        next_view.lease_id = 301;
        next_view.handoff_id = 401;
        assert!(audit
            .apply(event(EventKind::ViewIntent(next_view)))
            .is_err());
        assert!(audit
            .apply(event(EventKind::RegisterIntent(backing(
                2,
                "a",
                Purpose::Staging
            ))))
            .is_err());
        assert_eq!(audit.charged_bytes(), 4096);
    }

    #[test]
    fn budget_and_identity_reject_duplicate_and_over_capacity() {
        let mut audit = AuditReplay::new(domain()).unwrap();
        register(&mut audit, backing(1, "a", Purpose::Immutable));
        register(&mut audit, backing(2, "a", Purpose::Staging));
        assert_eq!(audit.charged_bytes(), 8192);
        assert!(audit
            .apply(event(EventKind::RegisterIntent(backing(
                3,
                "a",
                Purpose::ProbeOnly
            ))))
            .is_err());
        let mut dup = backing(3, "b", Purpose::Staging);
        dup.memfd_ino = backing(1, "a", Purpose::Immutable).memfd_ino;
        assert!(audit.apply(event(EventKind::RegisterIntent(dup))).is_err());
        register(&mut audit, backing(3, "b", Purpose::Staging));
        assert_eq!(audit.charged_bytes(), 12288);
        assert!(audit
            .apply(event(EventKind::RegisterIntent(backing(
                4,
                "c",
                Purpose::ProbeOnly
            ))))
            .is_err());
    }

    #[test]
    fn cancel_never_releases_backings_before_worker_drains() {
        let mut audit = AuditReplay::new(domain()).unwrap();
        register(&mut audit, backing(1, "a", Purpose::Immutable));
        register(&mut audit, backing(2, "b", Purpose::Staging));
        let work = WorkIdentity {
            work_id: 1,
            source_id: 1,
            source_generation: 2,
            target_id: 2,
            target_generation: 3,
            src_offset: 17,
            dst_offset: 23,
            bytes: 19,
            snapshot_version: "snapshot-v1".into(),
            evidence_version: "receipt-v1".into(),
            cost_version: "cost-v1".into(),
            worker_id: "worker-1".into(),
            purpose: Purpose::Staging,
        };
        audit.apply(event(EventKind::WorkIntent(work))).unwrap();
        audit
            .apply(event(EventKind::CancelRequest { work_id: 1 }))
            .unwrap();
        audit
            .apply(event(EventKind::CancelRequest { work_id: 1 }))
            .unwrap();
        audit
            .apply(event(EventKind::Revoke {
                object_id: 2,
                resource_generation: 3,
            }))
            .unwrap();
        audit
            .apply(event(EventKind::BackingCloseIntent {
                object_id: 2,
                resource_generation: 3,
            }))
            .unwrap();
        assert!(audit
            .apply(event(EventKind::BackingCloseDone {
                object_id: 2,
                resource_generation: 3
            }))
            .is_err());
        assert_eq!(audit.charged_bytes(), 8192);
        assert_eq!(audit.active_work(), 1);
        audit
            .apply(event(EventKind::WorkDrained { work_id: 1 }))
            .unwrap();
        audit
            .apply(event(EventKind::BackingCloseDone {
                object_id: 2,
                resource_generation: 3,
            }))
            .unwrap();
        assert_eq!(audit.charged_bytes(), 4096);
        assert_eq!(audit.active_work(), 0);
    }

    #[test]
    fn publication_requires_drained_verified_staging_and_actual_seal_record() {
        let mut audit = AuditReplay::new(domain()).unwrap();
        register(&mut audit, backing(1, "a", Purpose::Immutable));
        register(&mut audit, backing(2, "b", Purpose::Staging));
        let work = WorkIdentity {
            work_id: 1,
            source_id: 1,
            source_generation: 2,
            target_id: 2,
            target_generation: 3,
            src_offset: 17,
            dst_offset: 23,
            bytes: 19,
            snapshot_version: "snapshot-v1".into(),
            evidence_version: "receipt-v1".into(),
            cost_version: "cost-v1".into(),
            worker_id: "worker-1".into(),
            purpose: Purpose::Staging,
        };
        audit
            .apply(event(EventKind::WorkIntent(work.clone())))
            .unwrap();
        assert!(audit
            .apply(event(EventKind::WorkVerified {
                work_id: 1,
                copied: 19,
                verified: 19
            }))
            .is_err());
        assert!(audit
            .apply(event(EventKind::PublishIntent { work_id: 1 }))
            .is_err());
        audit
            .apply(event(EventKind::WorkDrained { work_id: 1 }))
            .unwrap();
        let mut reuse = work;
        reuse.work_id = 2;
        assert!(audit.apply(event(EventKind::WorkIntent(reuse))).is_err());
        assert!(audit
            .apply(event(EventKind::WorkVerified {
                work_id: 1,
                copied: 19,
                verified: 18
            }))
            .is_err());
        audit
            .apply(event(EventKind::WorkVerified {
                work_id: 1,
                copied: 19,
                verified: 19,
            }))
            .unwrap();
        assert!(audit
            .apply(event(EventKind::PublishDone {
                work_id: 1,
                seals: 15
            }))
            .is_err());
        audit
            .apply(event(EventKind::PublishIntent { work_id: 1 }))
            .unwrap();
        assert!(audit
            .apply(event(EventKind::PublishDone {
                work_id: 1,
                seals: 6
            }))
            .is_err());
        audit
            .apply(event(EventKind::PublishDone {
                work_id: 1,
                seals: 15,
            }))
            .unwrap();
        assert!(audit
            .apply(event(EventKind::PublishDone {
                work_id: 1,
                seals: 15
            }))
            .is_err());
        let mut target_view = view();
        target_view.object_id = 2;
        target_view.resource_generation = 3;
        audit
            .apply(event(EventKind::ViewIntent(target_view)))
            .unwrap();
    }

    #[test]
    fn cancelled_or_probe_work_cannot_publish_and_abort_can_retire() {
        for purpose in [Purpose::Staging, Purpose::ProbeOnly] {
            let mut audit = AuditReplay::new(domain()).unwrap();
            register(&mut audit, backing(1, "a", Purpose::Immutable));
            register(&mut audit, backing(2, "b", purpose));
            let work = WorkIdentity {
                work_id: 1,
                source_id: 1,
                source_generation: 2,
                target_id: 2,
                target_generation: 3,
                src_offset: 17,
                dst_offset: 23,
                bytes: 19,
                snapshot_version: "snapshot-v1".into(),
                evidence_version: "receipt-v1".into(),
                cost_version: "cost-v1".into(),
                worker_id: "worker-1".into(),
                purpose,
            };
            audit.apply(event(EventKind::WorkIntent(work))).unwrap();
            if purpose == Purpose::Staging {
                audit
                    .apply(event(EventKind::CancelRequest { work_id: 1 }))
                    .unwrap();
            }
            audit
                .apply(event(EventKind::WorkDrained { work_id: 1 }))
                .unwrap();
            let verified = audit.apply(event(EventKind::WorkVerified {
                work_id: 1,
                copied: 19,
                verified: 19,
            }));
            assert_eq!(verified.is_ok(), purpose == Purpose::ProbeOnly);
            assert!(audit
                .apply(event(EventKind::PublishIntent { work_id: 1 }))
                .is_err());
        }
        let mut audit = AuditReplay::new(domain()).unwrap();
        register(&mut audit, backing(1, "a", Purpose::Immutable));
        register(&mut audit, backing(2, "b", Purpose::Staging));
        let work = WorkIdentity {
            work_id: 3,
            source_id: 1,
            source_generation: 2,
            target_id: 2,
            target_generation: 3,
            src_offset: 17,
            dst_offset: 23,
            bytes: 19,
            snapshot_version: "snapshot-v1".into(),
            evidence_version: "receipt-v1".into(),
            cost_version: "cost-v1".into(),
            worker_id: "worker-3".into(),
            purpose: Purpose::Staging,
        };
        audit.apply(event(EventKind::WorkIntent(work))).unwrap();
        audit
            .apply(event(EventKind::WorkDrained { work_id: 3 }))
            .unwrap();
        audit
            .apply(event(EventKind::WorkVerified {
                work_id: 3,
                copied: 19,
                verified: 19,
            }))
            .unwrap();
        audit
            .apply(event(EventKind::PublishIntent { work_id: 3 }))
            .unwrap();
        audit
            .apply(event(EventKind::PublishAbort { work_id: 3 }))
            .unwrap();
        audit
            .apply(event(EventKind::Revoke {
                object_id: 2,
                resource_generation: 3,
            }))
            .unwrap();
        audit
            .apply(event(EventKind::BackingCloseIntent {
                object_id: 2,
                resource_generation: 3,
            }))
            .unwrap();
        audit
            .apply(event(EventKind::BackingCloseDone {
                object_id: 2,
                resource_generation: 3,
            }))
            .unwrap();
        assert_eq!(audit.charged_bytes(), 4096);
    }

    #[test]
    fn stale_source_cannot_finish_publication_but_can_abort_safely() {
        let mut audit = AuditReplay::new(domain()).unwrap();
        register(&mut audit, backing(1, "a", Purpose::Immutable));
        register(&mut audit, backing(2, "b", Purpose::Staging));
        let work = WorkIdentity {
            work_id: 1,
            source_id: 1,
            source_generation: 2,
            target_id: 2,
            target_generation: 3,
            src_offset: 17,
            dst_offset: 23,
            bytes: 19,
            snapshot_version: "snapshot-v1".into(),
            evidence_version: "receipt-v1".into(),
            cost_version: "cost-v1".into(),
            worker_id: "worker-1".into(),
            purpose: Purpose::Staging,
        };
        audit.apply(event(EventKind::WorkIntent(work))).unwrap();
        audit
            .apply(event(EventKind::WorkDrained { work_id: 1 }))
            .unwrap();
        audit
            .apply(event(EventKind::WorkVerified {
                work_id: 1,
                copied: 19,
                verified: 19,
            }))
            .unwrap();
        audit
            .apply(event(EventKind::PublishIntent { work_id: 1 }))
            .unwrap();
        audit
            .apply(event(EventKind::Revoke {
                object_id: 1,
                resource_generation: 2,
            }))
            .unwrap();
        assert!(audit
            .apply(event(EventKind::PublishDone {
                work_id: 1,
                seals: 15
            }))
            .is_err());
        audit
            .apply(event(EventKind::PublishAbort { work_id: 1 }))
            .unwrap();
        audit
            .apply(event(EventKind::Revoke {
                object_id: 2,
                resource_generation: 3,
            }))
            .unwrap();
        audit
            .apply(event(EventKind::BackingCloseIntent {
                object_id: 2,
                resource_generation: 3,
            }))
            .unwrap();
        audit
            .apply(event(EventKind::BackingCloseDone {
                object_id: 2,
                resource_generation: 3,
            }))
            .unwrap();
    }

    #[test]
    fn scope_generation_and_unsent_or_close_order_are_checked() {
        let mut audit = AuditReplay::new(domain()).unwrap();
        register(&mut audit, backing(1, "a", Purpose::Immutable));
        let mut invalid = view();
        invalid.resource_generation = 3;
        assert!(audit.apply(event(EventKind::ViewIntent(invalid))).is_err());
        let mut foreign = event(EventKind::ViewIntent(view()));
        foreign.domain.broker = "broker-b".into();
        assert!(audit.apply(foreign).is_err());
        audit.apply(event(EventKind::ViewIntent(view()))).unwrap();
        assert!(audit
            .apply(event(EventKind::ViewCloseDone {
                view_id: 200,
                lease_id: 300
            }))
            .is_err());
        audit
            .apply(event(EventKind::ViewNotSent {
                view_id: 200,
                lease_id: 300,
            }))
            .unwrap();
        assert_eq!(audit.unresolved_holders(), vec![birth(11, 101)]);
    }

    #[test]
    fn derived_view_survives_parent_release_and_requires_its_own_close() {
        let mut audit = AuditReplay::new(domain()).unwrap();
        register(&mut audit, backing(1, "a", Purpose::Immutable));
        audit.apply(event(EventKind::ViewIntent(view()))).unwrap();
        audit
            .apply(event(EventKind::ViewCommit {
                view_id: 200,
                lease_id: 300,
            }))
            .unwrap();
        let mut child = view();
        child.view_id = 201;
        child.lease_id = 301;
        child.handoff_id = 401;
        child.parent_view_id = Some(200);
        child.offset = 20;
        child.bytes = 5;
        audit.apply(event(EventKind::ViewIntent(child))).unwrap();
        audit
            .apply(event(EventKind::ViewCommit {
                view_id: 201,
                lease_id: 301,
            }))
            .unwrap();
        audit
            .apply(event(EventKind::ViewCloseIntent {
                view_id: 200,
                lease_id: 300,
            }))
            .unwrap();
        audit
            .apply(event(EventKind::ViewCloseDone {
                view_id: 200,
                lease_id: 300,
            }))
            .unwrap();
        audit
            .apply(event(EventKind::Revoke {
                object_id: 1,
                resource_generation: 2,
            }))
            .unwrap();
        audit
            .apply(event(EventKind::BackingCloseIntent {
                object_id: 1,
                resource_generation: 2,
            }))
            .unwrap();
        assert!(audit
            .apply(event(EventKind::BackingCloseDone {
                object_id: 1,
                resource_generation: 2
            }))
            .is_err());
        audit
            .apply(event(EventKind::ViewCloseIntent {
                view_id: 201,
                lease_id: 301,
            }))
            .unwrap();
        audit
            .apply(event(EventKind::ViewCloseDone {
                view_id: 201,
                lease_id: 301,
            }))
            .unwrap();
        audit
            .apply(event(EventKind::BackingCloseDone {
                object_id: 1,
                resource_generation: 2,
            }))
            .unwrap();
        assert_eq!(audit.charged_bytes(), 0);
        assert!(audit.unresolved_holders().is_empty());
    }
}
