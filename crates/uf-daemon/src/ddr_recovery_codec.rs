//! WIP canonical codec for the subset of recovery obligations currently
//! modeled by ddr_recovery_audit. Unsupported record types fail closed.
//! Some isolated fixture events are wired; production admission, clean/recovery,
//! and witnesses remain unavailable.
#![allow(dead_code)]

use crate::ddr_process_identity::ProcessBirth;
use crate::ddr_recovery_audit::{
    BackingIdentity, Domain, DrainSnapshot, Event, EventKind, Purpose, ViewIdentity, WorkIdentity,
};
use crate::ddr_recovery_journal::{Frame, Kind};
use std::collections::BTreeMap;
use std::fmt::Display;

const SCHEMA: &str = "uflow-ddr-audit-event-v1";
type Fields = BTreeMap<String, String>;
type Result<T> = std::result::Result<T, &'static str>;

fn token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 96
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}

fn put(fields: &mut Fields, key: &str, value: impl Display) {
    assert!(fields.insert(key.to_string(), value.to_string()).is_none());
}

fn put_birth(fields: &mut Fields, prefix: &str, birth: &ProcessBirth) {
    put(fields, &format!("{prefix}_pid"), birth.pid);
    put(fields, &format!("{prefix}_uid"), birth.uid);
    put(fields, &format!("{prefix}_gid"), birth.gid);
    put(
        fields,
        &format!("{prefix}_pidns_dev"),
        birth.pid_namespace_dev,
    );
    put(
        fields,
        &format!("{prefix}_pidns_ino"),
        birth.pid_namespace_ino,
    );
    put(
        fields,
        &format!("{prefix}_starttime"),
        birth.starttime_ticks,
    );
}

fn purpose_name(purpose: Purpose) -> &'static str {
    match purpose {
        Purpose::Immutable => "immutable",
        Purpose::Staging => "staging",
        Purpose::ProbeOnly => "probe_only",
    }
}

fn kind_name(kind: &EventKind) -> (&'static str, Kind) {
    match kind {
        EventKind::BrokerBirth { .. } => ("broker_birth", Kind::BrokerBirth),
        EventKind::RegisterIntent(_) => ("register_intent", Kind::SessionRegisterIntent),
        EventKind::RegisterCommit { .. } => ("register_commit", Kind::RegisterCommit),
        EventKind::RegisterAbort { .. } => ("register_abort", Kind::Abort),
        EventKind::Revoke { .. } => ("revoke", Kind::Retire),
        EventKind::BackingCloseIntent { .. } => ("backing_close_intent", Kind::CloseIntent),
        EventKind::BackingCloseDone { .. } => ("backing_close_done", Kind::CloseDone),
        EventKind::ViewIntent(_) => ("view_intent", Kind::ViewIntent),
        EventKind::ViewCommit { .. } => ("view_commit", Kind::ViewCommit),
        EventKind::ViewAbort { .. } => ("view_abort", Kind::Abort),
        EventKind::ViewCommitUnconfirmed { .. } => ("view_commit_unconfirmed", Kind::Abort),
        EventKind::ViewNotSent { .. } => ("view_not_sent", Kind::NotSent),
        EventKind::ViewPeerTerminated { .. } => ("view_peer_terminated", Kind::PeerTerminated),
        EventKind::ViewCloseIntent { .. } => ("view_close_intent", Kind::CloseIntent),
        EventKind::ViewCloseDone { .. } => ("view_close_done", Kind::CloseDone),
        EventKind::WorkIntent(_) => ("work_intent", Kind::CopyIntent),
        EventKind::CancelRequest { .. } => ("cancel_request", Kind::CancelRequest),
        EventKind::WorkDrained { .. } => ("work_drained", Kind::WorkDrained),
        EventKind::WorkIoCompleted { .. } => ("work_io_completed", Kind::WorkIoCompleted),
        EventKind::WorkVerified { .. } => ("work_verified", Kind::WorkVerified),
        EventKind::PublishIntent { .. } => ("publish_intent", Kind::PublishIntent),
        EventKind::PublishDone { .. } => ("publish_done", Kind::PublishDone),
        EventKind::PublishAbort { .. } => ("publish_abort", Kind::Abort),
        EventKind::PeerTerminated { .. } => ("peer_terminated", Kind::PeerTerminated),
        EventKind::BrokerDrained(_) => ("broker_drained", Kind::BrokerDrained),
        EventKind::CleanCheckpoint(_) => ("clean_checkpoint", Kind::CleanCheckpoint),
    }
}

fn encode_fields(fields: &Fields) -> Result<Vec<u8>> {
    if fields.is_empty() || fields.iter().any(|(k, v)| !token(k) || !token(v)) {
        return Err("invalid audit event token");
    }
    let mut text = String::new();
    for (index, (key, value)) in fields.iter().enumerate() {
        if index != 0 {
            text.push(';');
        }
        text.push_str(key);
        text.push('=');
        text.push_str(value);
    }
    text.push('\n');
    if text.len() > 8192 {
        return Err("audit event exceeds frame limit");
    }
    Ok(text.into_bytes())
}

pub(crate) fn encode(event: &Event) -> Result<(Kind, Vec<u8>)> {
    let mut fields = Fields::new();
    let (name, kind) = kind_name(&event.kind);
    put(&mut fields, "schema", SCHEMA);
    put(&mut fields, "type", name);
    put(&mut fields, "host", &event.domain.host);
    put(&mut fields, "boot", &event.domain.boot);
    put(&mut fields, "domain", &event.domain.domain);
    put(&mut fields, "broker", &event.domain.broker);
    put(&mut fields, "cap_revision", event.domain.cap_revision);
    put(&mut fields, "owner_limit", event.domain.owner_limit);
    put(&mut fields, "total_limit", event.domain.total_limit);
    put(&mut fields, "epoch", event.epoch);
    match &event.kind {
        EventKind::BrokerBirth { birth } => put_birth(&mut fields, "broker_birth", birth),
        EventKind::RegisterIntent(b) => {
            put_birth(&mut fields, "owner", &b.owner);
            put(&mut fields, "owner_incarnation", &b.owner_incarnation);
            put(&mut fields, "pool_generation", b.pool_generation);
            put(&mut fields, "object_id", b.object_id);
            put(&mut fields, "resource_generation", b.resource_generation);
            put(&mut fields, "placement_id", b.placement_id);
            put(&mut fields, "binding_id", b.binding_id);
            put(&mut fields, "binding_generation", b.binding_generation);
            put(&mut fields, "owner_lease_id", b.owner_lease_id);
            put(&mut fields, "reservation_id", b.reservation_id);
            put(&mut fields, "bytes", b.bytes);
            put(&mut fields, "memfd_dev", b.memfd_dev);
            put(&mut fields, "memfd_ino", b.memfd_ino);
            put(&mut fields, "memfd_seals", b.memfd_seals);
            put(&mut fields, "purpose", purpose_name(b.purpose));
        }
        EventKind::RegisterCommit {
            object_id,
            resource_generation,
        }
        | EventKind::RegisterAbort {
            object_id,
            resource_generation,
        }
        | EventKind::Revoke {
            object_id,
            resource_generation,
        }
        | EventKind::BackingCloseIntent {
            object_id,
            resource_generation,
        }
        | EventKind::BackingCloseDone {
            object_id,
            resource_generation,
        } => {
            put(&mut fields, "object_id", object_id);
            put(&mut fields, "resource_generation", resource_generation);
        }
        EventKind::ViewIntent(v) => {
            put_birth(&mut fields, "accessor", &v.accessor);
            put(&mut fields, "object_id", v.object_id);
            put(&mut fields, "resource_generation", v.resource_generation);
            put(&mut fields, "accessor_incarnation", &v.accessor_incarnation);
            put(&mut fields, "client_id", v.client_id);
            put(&mut fields, "view_id", v.view_id);
            put(&mut fields, "lease_id", v.lease_id);
            put(&mut fields, "handoff_id", v.handoff_id);
            put(&mut fields, "parent_view_id", v.parent_view_id.unwrap_or(0));
            put(&mut fields, "offset", v.offset);
            put(&mut fields, "bytes", v.bytes);
        }
        EventKind::ViewCommit { view_id, lease_id }
        | EventKind::ViewAbort { view_id, lease_id }
        | EventKind::ViewCommitUnconfirmed { view_id, lease_id }
        | EventKind::ViewNotSent { view_id, lease_id }
        | EventKind::ViewPeerTerminated { view_id, lease_id }
        | EventKind::ViewCloseIntent { view_id, lease_id }
        | EventKind::ViewCloseDone { view_id, lease_id } => {
            put(&mut fields, "view_id", view_id);
            put(&mut fields, "lease_id", lease_id);
        }
        EventKind::WorkIntent(w) => {
            put(&mut fields, "work_id", w.work_id);
            put(&mut fields, "source_id", w.source_id);
            put(&mut fields, "source_generation", w.source_generation);
            put(&mut fields, "target_id", w.target_id);
            put(&mut fields, "target_generation", w.target_generation);
            put(&mut fields, "src_offset", w.src_offset);
            put(&mut fields, "dst_offset", w.dst_offset);
            put(&mut fields, "bytes", w.bytes);
            put(&mut fields, "snapshot_version", &w.snapshot_version);
            put(&mut fields, "evidence_version", &w.evidence_version);
            put(&mut fields, "cost_version", &w.cost_version);
            put(&mut fields, "worker_id", &w.worker_id);
            put(&mut fields, "purpose", purpose_name(w.purpose));
        }
        EventKind::CancelRequest { work_id }
        | EventKind::WorkDrained { work_id }
        | EventKind::PublishIntent { work_id }
        | EventKind::PublishAbort { work_id } => {
            put(&mut fields, "work_id", work_id);
        }
        EventKind::PublishDone { work_id, seals } => {
            put(&mut fields, "work_id", work_id);
            put(&mut fields, "seals", seals);
        }
        EventKind::WorkIoCompleted { work_id, copied } => {
            put(&mut fields, "work_id", work_id);
            put(&mut fields, "copied", copied);
        }
        EventKind::WorkVerified {
            work_id,
            copied,
            verified,
        } => {
            put(&mut fields, "work_id", work_id);
            put(&mut fields, "copied", copied);
            put(&mut fields, "verified", verified);
        }
        EventKind::PeerTerminated { birth } => put_birth(&mut fields, "peer", birth),
        EventKind::BrokerDrained(snapshot) | EventKind::CleanCheckpoint(snapshot) => {
            put(&mut fields, "handlers", snapshot.handlers);
            put(&mut fields, "pending_handoffs", snapshot.pending_handoffs);
            put(&mut fields, "retained_backings", snapshot.retained_backings);
            put(&mut fields, "active_views", snapshot.active_views);
            put(&mut fields, "reservations", snapshot.reservations);
            put(&mut fields, "in_flight", snapshot.in_flight);
            put(&mut fields, "potential_holders", snapshot.potential_holders);
        }
    }
    let payload = encode_fields(&fields)?;
    // Do not write a payload the strict decoder cannot faithfully reconstruct
    // (for example a negative PID or an ambiguous Some(0) parent identifier).
    let check = Frame {
        kind,
        sequence: 1,
        previous: 0,
        payload: payload.clone(),
        checksum: 0,
    };
    if decode(&check)? != *event {
        return Err("non-roundtripping audit event");
    }
    Ok((kind, payload))
}

fn parse_fields(payload: &[u8]) -> Result<Fields> {
    if payload.len() > 8192 {
        return Err("audit event exceeds frame limit");
    }
    let text = std::str::from_utf8(payload).map_err(|_| "audit event is not UTF-8")?;
    let body = text
        .strip_suffix('\n')
        .ok_or("audit event missing newline")?;
    let mut fields = Fields::new();
    for part in body.split(';') {
        let (key, value) = part.split_once('=').ok_or("malformed audit field")?;
        if !token(key) || !token(value) || fields.insert(key.to_owned(), value.to_owned()).is_some()
        {
            return Err("invalid or duplicate audit field");
        }
    }
    if encode_fields(&fields)? != payload {
        return Err("noncanonical audit event");
    }
    Ok(fields)
}

fn take(fields: &mut Fields, key: &str) -> Result<String> {
    fields.remove(key).ok_or("missing audit field")
}
fn take_u64(fields: &mut Fields, key: &str) -> Result<u64> {
    let value = take(fields, key)?;
    let n = value.parse::<u64>().map_err(|_| "invalid audit integer")?;
    if n.to_string() != value {
        return Err("noncanonical audit integer");
    }
    Ok(n)
}
fn take_u32(fields: &mut Fields, key: &str) -> Result<u32> {
    u32::try_from(take_u64(fields, key)?).map_err(|_| "audit integer exceeds u32")
}
fn take_i32(fields: &mut Fields, key: &str) -> Result<i32> {
    i32::try_from(take_u64(fields, key)?).map_err(|_| "audit integer exceeds i32")
}
fn take_birth(fields: &mut Fields, prefix: &str) -> Result<ProcessBirth> {
    Ok(ProcessBirth {
        pid: take_i32(fields, &format!("{prefix}_pid"))?,
        uid: take_u32(fields, &format!("{prefix}_uid"))?,
        gid: take_u32(fields, &format!("{prefix}_gid"))?,
        pid_namespace_dev: take_u64(fields, &format!("{prefix}_pidns_dev"))?,
        pid_namespace_ino: take_u64(fields, &format!("{prefix}_pidns_ino"))?,
        starttime_ticks: take_u64(fields, &format!("{prefix}_starttime"))?,
    })
}
fn take_purpose(fields: &mut Fields) -> Result<Purpose> {
    match take(fields, "purpose")?.as_str() {
        "immutable" => Ok(Purpose::Immutable),
        "staging" => Ok(Purpose::Staging),
        "probe_only" => Ok(Purpose::ProbeOnly),
        _ => Err("unknown audit purpose"),
    }
}

pub(crate) fn decode(frame: &Frame) -> Result<Event> {
    let mut fields = parse_fields(&frame.payload)?;
    if take(&mut fields, "schema")? != SCHEMA {
        return Err("unknown audit schema");
    }
    let name = take(&mut fields, "type")?;
    let domain = Domain {
        host: take(&mut fields, "host")?,
        boot: take(&mut fields, "boot")?,
        domain: take(&mut fields, "domain")?,
        broker: take(&mut fields, "broker")?,
        cap_revision: take_u64(&mut fields, "cap_revision")?,
        owner_limit: take_u64(&mut fields, "owner_limit")?,
        total_limit: take_u64(&mut fields, "total_limit")?,
    };
    let epoch = take_u64(&mut fields, "epoch")?;
    let kind = match name.as_str() {
        "broker_birth" => EventKind::BrokerBirth {
            birth: take_birth(&mut fields, "broker_birth")?,
        },
        "register_intent" => EventKind::RegisterIntent(BackingIdentity {
            owner: take_birth(&mut fields, "owner")?,
            owner_incarnation: take(&mut fields, "owner_incarnation")?,
            pool_generation: take_u64(&mut fields, "pool_generation")?,
            object_id: take_u64(&mut fields, "object_id")?,
            resource_generation: take_u64(&mut fields, "resource_generation")?,
            placement_id: take_u64(&mut fields, "placement_id")?,
            binding_id: take_u64(&mut fields, "binding_id")?,
            binding_generation: take_u64(&mut fields, "binding_generation")?,
            owner_lease_id: take_u64(&mut fields, "owner_lease_id")?,
            reservation_id: take_u64(&mut fields, "reservation_id")?,
            bytes: take_u64(&mut fields, "bytes")?,
            memfd_dev: take_u64(&mut fields, "memfd_dev")?,
            memfd_ino: take_u64(&mut fields, "memfd_ino")?,
            memfd_seals: take_i32(&mut fields, "memfd_seals")?,
            purpose: take_purpose(&mut fields)?,
        }),
        "register_commit"
        | "register_abort"
        | "revoke"
        | "backing_close_intent"
        | "backing_close_done" => {
            let object_id = take_u64(&mut fields, "object_id")?;
            let resource_generation = take_u64(&mut fields, "resource_generation")?;
            match name.as_str() {
                "register_commit" => EventKind::RegisterCommit {
                    object_id,
                    resource_generation,
                },
                "register_abort" => EventKind::RegisterAbort {
                    object_id,
                    resource_generation,
                },
                "revoke" => EventKind::Revoke {
                    object_id,
                    resource_generation,
                },
                "backing_close_intent" => EventKind::BackingCloseIntent {
                    object_id,
                    resource_generation,
                },
                _ => EventKind::BackingCloseDone {
                    object_id,
                    resource_generation,
                },
            }
        }
        "view_intent" => {
            let accessor = take_birth(&mut fields, "accessor")?;
            let object_id = take_u64(&mut fields, "object_id")?;
            let resource_generation = take_u64(&mut fields, "resource_generation")?;
            let accessor_incarnation = take(&mut fields, "accessor_incarnation")?;
            let client_id = take_u64(&mut fields, "client_id")?;
            let view_id = take_u64(&mut fields, "view_id")?;
            let lease_id = take_u64(&mut fields, "lease_id")?;
            let handoff_id = take_u64(&mut fields, "handoff_id")?;
            let parent = take_u64(&mut fields, "parent_view_id")?;
            let offset = take_u64(&mut fields, "offset")?;
            let bytes = take_u64(&mut fields, "bytes")?;
            EventKind::ViewIntent(ViewIdentity {
                object_id,
                resource_generation,
                accessor,
                accessor_incarnation,
                client_id,
                view_id,
                lease_id,
                handoff_id,
                parent_view_id: if parent == 0 { None } else { Some(parent) },
                offset,
                bytes,
            })
        }
        "view_commit"
        | "view_abort"
        | "view_commit_unconfirmed"
        | "view_not_sent"
        | "view_peer_terminated"
        | "view_close_intent"
        | "view_close_done" => {
            let view_id = take_u64(&mut fields, "view_id")?;
            let lease_id = take_u64(&mut fields, "lease_id")?;
            match name.as_str() {
                "view_commit" => EventKind::ViewCommit { view_id, lease_id },
                "view_abort" => EventKind::ViewAbort { view_id, lease_id },
                "view_commit_unconfirmed" => EventKind::ViewCommitUnconfirmed { view_id, lease_id },
                "view_not_sent" => EventKind::ViewNotSent { view_id, lease_id },
                "view_peer_terminated" => EventKind::ViewPeerTerminated { view_id, lease_id },
                "view_close_intent" => EventKind::ViewCloseIntent { view_id, lease_id },
                _ => EventKind::ViewCloseDone { view_id, lease_id },
            }
        }
        "work_intent" => EventKind::WorkIntent(WorkIdentity {
            work_id: take_u64(&mut fields, "work_id")?,
            source_id: take_u64(&mut fields, "source_id")?,
            source_generation: take_u64(&mut fields, "source_generation")?,
            target_id: take_u64(&mut fields, "target_id")?,
            target_generation: take_u64(&mut fields, "target_generation")?,
            src_offset: take_u64(&mut fields, "src_offset")?,
            dst_offset: take_u64(&mut fields, "dst_offset")?,
            bytes: take_u64(&mut fields, "bytes")?,
            snapshot_version: take(&mut fields, "snapshot_version")?,
            evidence_version: take(&mut fields, "evidence_version")?,
            cost_version: take(&mut fields, "cost_version")?,
            worker_id: take(&mut fields, "worker_id")?,
            purpose: take_purpose(&mut fields)?,
        }),
        "cancel_request" => EventKind::CancelRequest {
            work_id: take_u64(&mut fields, "work_id")?,
        },
        "work_drained" => EventKind::WorkDrained {
            work_id: take_u64(&mut fields, "work_id")?,
        },
        "work_io_completed" => EventKind::WorkIoCompleted {
            work_id: take_u64(&mut fields, "work_id")?,
            copied: take_u64(&mut fields, "copied")?,
        },
        "work_verified" => EventKind::WorkVerified {
            work_id: take_u64(&mut fields, "work_id")?,
            copied: take_u64(&mut fields, "copied")?,
            verified: take_u64(&mut fields, "verified")?,
        },
        "publish_intent" => EventKind::PublishIntent {
            work_id: take_u64(&mut fields, "work_id")?,
        },
        "publish_done" => EventKind::PublishDone {
            work_id: take_u64(&mut fields, "work_id")?,
            seals: take_u32(&mut fields, "seals")?,
        },
        "publish_abort" => EventKind::PublishAbort {
            work_id: take_u64(&mut fields, "work_id")?,
        },
        "peer_terminated" => EventKind::PeerTerminated {
            birth: take_birth(&mut fields, "peer")?,
        },
        "broker_drained" | "clean_checkpoint" => {
            let snapshot = DrainSnapshot {
                handlers: take_u64(&mut fields, "handlers")?,
                pending_handoffs: take_u64(&mut fields, "pending_handoffs")?,
                retained_backings: take_u64(&mut fields, "retained_backings")?,
                active_views: take_u64(&mut fields, "active_views")?,
                reservations: take_u64(&mut fields, "reservations")?,
                in_flight: take_u64(&mut fields, "in_flight")?,
                potential_holders: take_u64(&mut fields, "potential_holders")?,
            };
            if name == "broker_drained" {
                EventKind::BrokerDrained(snapshot)
            } else {
                EventKind::CleanCheckpoint(snapshot)
            }
        }
        _ => return Err("unsupported audit event type"),
    };
    if !fields.is_empty() || kind_name(&kind).1 != frame.kind {
        return Err("unexpected audit fields or frame type mismatch");
    }
    Ok(Event {
        domain,
        epoch,
        kind,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddr_recovery_audit::AuditReplay;
    use crate::ddr_recovery_journal::{AnchoredJournal, Mode};
    use std::fs::{self, File, OpenOptions};
    use std::io::Write;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn birth(pid: i32) -> ProcessBirth {
        ProcessBirth {
            pid,
            uid: 1000,
            gid: 1000,
            pid_namespace_dev: 1,
            pid_namespace_ino: 2,
            starttime_ticks: 123,
        }
    }
    fn domain() -> Domain {
        Domain {
            host: "host".into(),
            boot: "boot".into(),
            domain: "ddr".into(),
            cap_revision: 1,
            broker: "broker".into(),
            owner_limit: 8192,
            total_limit: 16384,
        }
    }
    fn event(kind: EventKind) -> Event {
        Event {
            domain: domain(),
            epoch: 1,
            kind,
        }
    }
    fn frame(event: &Event) -> Frame {
        let (kind, payload) = encode(event).unwrap();
        Frame {
            kind,
            payload,
            sequence: 1,
            previous: 0,
            checksum: 0,
        }
    }
    fn backing() -> BackingIdentity {
        BackingIdentity {
            owner: birth(10),
            owner_incarnation: "owner".into(),
            pool_generation: 1,
            object_id: 1,
            resource_generation: 2,
            placement_id: 3,
            binding_id: 4,
            binding_generation: 5,
            owner_lease_id: 6,
            reservation_id: 7,
            bytes: 4096,
            memfd_dev: 1,
            memfd_ino: 100,
            memfd_seals: 6,
            purpose: Purpose::Immutable,
        }
    }
    fn view() -> ViewIdentity {
        ViewIdentity {
            object_id: 1,
            resource_generation: 2,
            accessor: birth(20),
            accessor_incarnation: "accessor".into(),
            client_id: 8,
            view_id: 9,
            lease_id: 10,
            handoff_id: 11,
            parent_view_id: None,
            offset: 17,
            bytes: 19,
        }
    }

    #[test]
    fn all_modeled_records_roundtrip_with_frame_type_bound() {
        let work = WorkIdentity {
            work_id: 1,
            source_id: 1,
            source_generation: 2,
            target_id: 2,
            target_generation: 3,
            src_offset: 17,
            dst_offset: 19,
            bytes: 10,
            snapshot_version: "s1".into(),
            evidence_version: "e1".into(),
            cost_version: "c1".into(),
            worker_id: "w1".into(),
            purpose: Purpose::Staging,
        };
        let records = vec![
            EventKind::RegisterIntent(backing()),
            EventKind::RegisterCommit {
                object_id: 1,
                resource_generation: 2,
            },
            EventKind::RegisterAbort {
                object_id: 1,
                resource_generation: 2,
            },
            EventKind::Revoke {
                object_id: 1,
                resource_generation: 2,
            },
            EventKind::BackingCloseIntent {
                object_id: 1,
                resource_generation: 2,
            },
            EventKind::BackingCloseDone {
                object_id: 1,
                resource_generation: 2,
            },
            EventKind::ViewIntent(view()),
            EventKind::ViewCommit {
                view_id: 9,
                lease_id: 10,
            },
            EventKind::ViewAbort {
                view_id: 9,
                lease_id: 10,
            },
            EventKind::ViewCommitUnconfirmed {
                view_id: 9,
                lease_id: 10,
            },
            EventKind::ViewNotSent {
                view_id: 9,
                lease_id: 10,
            },
            EventKind::ViewCloseIntent {
                view_id: 9,
                lease_id: 10,
            },
            EventKind::ViewCloseDone {
                view_id: 9,
                lease_id: 10,
            },
            EventKind::WorkIntent(work),
            EventKind::CancelRequest { work_id: 1 },
            EventKind::WorkDrained { work_id: 1 },
            EventKind::WorkVerified {
                work_id: 1,
                copied: 10,
                verified: 10,
            },
            EventKind::PublishIntent { work_id: 1 },
            EventKind::PublishDone {
                work_id: 1,
                seals: 15,
            },
            EventKind::PublishAbort { work_id: 1 },
            EventKind::PeerTerminated { birth: birth(20) },
            EventKind::BrokerDrained(DrainSnapshot {
                handlers: 0,
                pending_handoffs: 0,
                retained_backings: 1,
                active_views: 1,
                reservations: 0,
                in_flight: 0,
                potential_holders: 2,
            }),
        ];
        for kind in records {
            let original = event(kind);
            let encoded = frame(&original);
            assert_eq!(decode(&encoded).unwrap(), original);
            let mut mismatched = encoded;
            mismatched.kind = Kind::RecoveryAudit;
            assert!(decode(&mismatched).is_err());
        }
    }

    #[test]
    fn rejects_missing_extra_duplicate_noncanonical_and_unknown_fields() {
        let original = frame(&event(EventKind::RegisterIntent(backing())));
        let text = std::str::from_utf8(&original.payload).unwrap();
        let mut duplicate = original.clone();
        duplicate.payload = format!("{};host=host\n", text.trim_end()).into_bytes();
        assert!(decode(&duplicate).is_err());
        let mut missing = original.clone();
        missing.payload = text.replace("host=host;", "").into_bytes();
        assert!(decode(&missing).is_err());
        let mut extra = original.clone();
        extra.payload = text.replace("schema=", "other=field;schema=").into_bytes();
        assert!(decode(&extra).is_err());
        let mut bad_number = original.clone();
        bad_number.payload = text.replace("object_id=1;", "object_id=01;").into_bytes();
        assert!(decode(&bad_number).is_err());
        let mut unknown = original.clone();
        unknown.payload = text
            .replace("type=register_intent", "type=not_implemented")
            .into_bytes();
        assert!(decode(&unknown).is_err());
    }

    #[test]
    fn decoded_obligations_replay_without_losing_unacked_exposure() {
        let records = vec![
            event(EventKind::RegisterIntent(backing())),
            event(EventKind::RegisterCommit {
                object_id: 1,
                resource_generation: 2,
            }),
            event(EventKind::ViewIntent(view())),
            event(EventKind::ViewAbort {
                view_id: 9,
                lease_id: 10,
            }),
        ];
        let mut replay = AuditReplay::new(domain()).unwrap();
        for record in records {
            replay.apply(decode(&frame(&record)).unwrap()).unwrap();
        }
        assert_eq!(replay.charged_bytes(), 4096);
        assert_eq!(replay.unresolved_holders(), vec![birth(10), birth(20)]);
    }

    #[test]
    fn durable_frames_roundtrip_to_replay_after_reopen() {
        let base = std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target"));
        let base = fs::canonicalize(base).unwrap();
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = base.join(format!("recovery-codec-{}-{nonce}", std::process::id()));
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let journal_path = root.join("journal");
        let state_path = root.join("state");
        let journal = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&journal_path)
            .unwrap();
        let mut state = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&state_path)
            .unwrap();
        state
            .write_all(AnchoredJournal::genesis_state().as_bytes())
            .unwrap();
        journal.sync_all().unwrap();
        state.sync_all().unwrap();
        File::open(&root).unwrap().sync_all().unwrap();
        let j = journal.metadata().unwrap();
        let s = state.metadata().unwrap();
        let journal_id = (j.dev(), j.ino());
        let state_id = (s.dev(), s.ino());
        drop(journal);
        drop(state);
        let mut log =
            AnchoredJournal::open(&journal_path, &state_path, journal_id, state_id).unwrap();
        let records = vec![
            event(EventKind::RegisterIntent(backing())),
            event(EventKind::RegisterCommit {
                object_id: 1,
                resource_generation: 2,
            }),
            event(EventKind::ViewIntent(view())),
            event(EventKind::ViewAbort {
                view_id: 9,
                lease_id: 10,
            }),
        ];
        for record in &records {
            let (kind, payload) = encode(record).unwrap();
            log.append_dirty(&record.domain.broker, kind, &payload)
                .unwrap();
        }
        drop(log);
        let mut reopened =
            AnchoredJournal::open(&journal_path, &state_path, journal_id, state_id).unwrap();
        assert_eq!(reopened.head().mode, Mode::Dirty);
        let frames = reopened.verify().unwrap();
        assert_eq!(frames.len(), records.len());
        let mut replay = AuditReplay::new(domain()).unwrap();
        for (index, frame) in frames.iter().enumerate() {
            let decoded = decode(frame).unwrap();
            assert_eq!(decoded, records[index]);
            replay.apply(decoded).unwrap();
        }
        assert_eq!(replay.charged_bytes(), 4096);
        assert_eq!(replay.unresolved_holders(), vec![birth(10), birth(20)]);
    }
}
