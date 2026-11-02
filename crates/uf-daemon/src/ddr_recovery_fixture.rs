//! Test-only audited recovery transaction. This is not an SDK operation and
//! cannot open or mutate the production authority root. Authorization always
//! comes from the one pinned coverage verifier under the authority lock.
use crate::ddr_managed_authority::ManagedAuthority;
use crate::ddr_recovery_audit::{AuditReplay, EventKind};
use crate::ddr_recovery_codec;
use crate::ddr_recovery_coverage::{self, CoverageReport};
use crate::ddr_recovery_journal::{Head, Kind, Mode};
use serde_json::{json, Value};
use std::io;

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 96
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}

fn encode(value: &Value) -> io::Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec(value).map_err(|e| invalid(e.to_string()))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn head_value(head: &Head) -> Value {
    json!({
        "mode": match head.mode { Mode::Dirty => "dirty", Mode::Clean => "clean" },
        "broker": head.broker,
        "sequence": head.sequence,
        "offset": head.offset,
        "checksum": format!("{:016x}", head.checksum),
        "clean_sequence": head.clean_sequence,
    })
}

pub(crate) fn marker(recovery_id: &str, input: &Head, audit_sha256: &str) -> String {
    format!(
        "uflow-ddr-fixture-recovery-pending-v1\nid={recovery_id}\ninput_sequence={}\ninput_checksum={:016x}\naudit_sha256={audit_sha256}\n",
        input.sequence, input.checksum
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RecoveryResult {
    pub recovery_id: String,
    pub input_head: Head,
    pub witness_head_sha256: String,
    pub audit_sha256: String,
    pub clean_head: Head,
    pub resolutions: Vec<Value>,
}

pub(crate) struct Transaction {
    pub audit: Vec<u8>,
    pub commit: Vec<u8>,
    pub checkpoint: Vec<u8>,
}

/// Derive one auditable resolution per original intent; sequence+payload hash
/// refers to the full immutable identity in the retained, anchored journal.
/// All subject exits, resource coverage and actual drains come solely from the
/// existing verifier, never from a caller-supplied list or boolean.
fn prepare(report: &CoverageReport, recovery_id: &str) -> io::Result<(Transaction, Vec<Value>)> {
    if !token(recovery_id) || report.events().len() as u64 != report.input_head().sequence {
        return Err(invalid("invalid recovery identity or input sequence"));
    }
    let last = report
        .events()
        .last()
        .ok_or_else(|| invalid("empty recovery"))?;
    let EventKind::BrokerDrained(snapshot) = &last.kind else {
        return Err(invalid("recovery requires final broker drain"));
    };
    if snapshot.handlers != 0
        || snapshot.pending_handoffs != 0
        || snapshot.reservations != 0
        || snapshot.in_flight != 0
    {
        return Err(invalid("unfinished handler, handoff, reservation or work"));
    }
    let domain = &last.domain;
    // Recompute the after-counts from semantic replay and the verifier's
    // typed, full-birth normal-exit evidence; do not manufacture zeroes.
    let mut replay = AuditReplay::new(domain.clone()).map_err(invalid)?;
    for event in report.events() {
        replay.apply(event.clone()).map_err(invalid)?;
    }
    let after = replay.resolved_fixture_drain(report).map_err(invalid)?;
    let mut resolutions = Vec::new();
    let mut work_count = 0usize;
    for (index, event) in report.events().iter().enumerate() {
        let (kind, resolution) = match &event.kind {
            EventKind::RegisterIntent(_) => ("backing", "covered_holder_exit_zero"),
            EventKind::ViewIntent(_) => ("potential_exposure", "covered_holder_exit_zero"),
            EventKind::WorkIntent(_) => {
                work_count += 1;
                ("work", "verified_worker_joined")
            }
            _ => continue,
        };
        let (_, identity) = ddr_recovery_codec::encode(event).map_err(invalid)?;
        resolutions.push(json!({
            "intent_sequence": index + 1,
            "kind": kind,
            "identity_sha256": ddr_recovery_coverage::digest(&identity),
            "resolution": resolution,
        }));
    }
    if report.subjects().is_empty() || work_count != report.work_drains().len() {
        return Err(invalid("incomplete subject or work resolution"));
    }
    let audit = encode(&json!({
        "schema": "uflow-ddr-fixture-recovery-audit-v1",
        "recovery_id": recovery_id,
        "domain": domain.domain,
        "host": domain.host,
        "boot": domain.boot,
        "cap_revision": domain.cap_revision,
        "broker": domain.broker,
        "owner_limit": domain.owner_limit,
        "total_limit": domain.total_limit,
        "input_head": head_value(report.input_head()),
        "witness_head_sha256": report.witness_head_sha256(),
        "subjects": report.subjects(),
        "work_drains": report.work_drains(),
        "broker_drain": report.broker_drain(),
        "before": {
            "handlers": snapshot.handlers,
            "pending_handoffs": snapshot.pending_handoffs,
            "retained_backings": snapshot.retained_backings,
            "active_views": snapshot.active_views,
            "reservations": snapshot.reservations,
            "in_flight": snapshot.in_flight,
            "potential_holders": snapshot.potential_holders,
        },
        "resolutions": resolutions,
        "after": {
            "handlers": after.handlers,
            "pending_handoffs": after.pending_handoffs,
            "retained_backings": after.retained_backings,
            "active_views": after.active_views,
            "reservations": after.reservations,
            "in_flight": after.in_flight,
            "potential_holders": after.potential_holders,
        },
    }))?;
    let audit_sha256 = ddr_recovery_coverage::digest(&audit);
    let commit = encode(&json!({
        "schema": "uflow-ddr-fixture-recovery-commit-v1",
        "recovery_id": recovery_id,
        "input_head": head_value(report.input_head()),
        "audit_sha256": audit_sha256,
    }))?;
    let checkpoint = encode(&json!({
        "schema": "uflow-ddr-fixture-recovery-clean-v1",
        "recovery_id": recovery_id,
        "audit_sha256": audit_sha256,
        "commit_sha256": ddr_recovery_coverage::digest(&commit),
    }))?;
    Ok((
        Transaction {
            audit,
            commit,
            checkpoint,
        },
        resolutions,
    ))
}

pub(crate) fn transaction(report: &CoverageReport, recovery_id: &str) -> io::Result<Transaction> {
    prepare(report, recovery_id).map(|(transaction, _)| transaction)
}

/// Strict readback: re-run the single verifier over the original prefix, then
/// reconstruct every byte of all three records. The recovery id is obtained
/// from the pinned record; an optional requested id may only narrow the match.
pub(crate) fn verify_sealed(
    authority: &ManagedAuthority,
    requested_id: Option<&str>,
) -> io::Result<RecoveryResult> {
    let report = ddr_recovery_coverage::verify_recovered(authority)?;
    let (mut journal, _, domain) = authority.fixture_recovery_material()?;
    let head = journal.head().clone();
    let frames = journal.verify()?;
    if head.mode != Mode::Clean || frames.len() < 4 {
        return Err(invalid("recovery is not sealed"));
    }
    let split = usize::try_from(report.input_head().sequence)
        .map_err(|_| invalid("recovery input sequence overflow"))?;
    let end = split
        .checked_add(3)
        .filter(|end| split > 0 && *end <= frames.len())
        .ok_or_else(|| invalid("incomplete recovery suffix"))?;
    let suffix = &frames[split..end];
    if suffix[0].kind != Kind::RecoveryAudit
        || suffix[1].kind != Kind::RecoveryCommit
        || suffix[2].kind != Kind::CleanCheckpoint
    {
        return Err(invalid("invalid recovery suffix"));
    }
    let audit: Value = serde_json::from_slice(&suffix[0].payload)
        .map_err(|e| invalid(format!("invalid recovery audit: {e}")))?;
    let recovery_id = audit
        .get("recovery_id")
        .and_then(Value::as_str)
        .filter(|value| token(value))
        .ok_or_else(|| invalid("invalid sealed recovery id"))?;
    if requested_id.is_some_and(|requested| requested != recovery_id) {
        return Err(invalid("recovery id does not match sealed transaction"));
    }
    let (expected, resolutions) = prepare(&report, recovery_id)?;
    if suffix[0].payload != expected.audit
        || suffix[1].payload != expected.commit
        || suffix[2].payload != expected.checkpoint
    {
        return Err(invalid(
            "recovery transaction or resolutions do not match evidence",
        ));
    }
    // Keep the original recovery checkpoint identity even if a later normal
    // generation has advanced the head. Replay all of that later history.
    let checkpoint = &suffix[2];
    let clean_head = Head {
        mode: Mode::Clean,
        broker: report.input_head().broker.clone(),
        sequence: checkpoint.sequence,
        offset: frames[..end].iter().map(|frame| frame.encoded_len()).sum(),
        checksum: checkpoint.checksum,
        clean_sequence: checkpoint.sequence,
    };
    crate::ddr_recovery_durable::verify_fixture_recovery_history(
        &mut journal,
        report.input_head(),
        &clean_head,
        &domain,
        &report,
    )?;
    Ok(RecoveryResult {
        recovery_id: recovery_id.into(),
        input_head: report.input_head().clone(),
        witness_head_sha256: report.witness_head_sha256().into(),
        audit_sha256: ddr_recovery_coverage::digest(&expected.audit),
        clean_head,
        resolutions,
    })
}
