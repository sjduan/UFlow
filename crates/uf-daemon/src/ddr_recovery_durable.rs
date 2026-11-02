//! Framed, fsync-anchored lifecycle records and obligation replay. Normal
//! zero-drain checkpoints are used by the fixed-root adapter; they do not
//! verify trusted witnesses or authorize recovery of an inherited dirty domain.
#![allow(dead_code)]

use crate::ddr_recovery_audit::{AuditReplay, Domain, DrainSnapshot, Event, EventKind};
use crate::ddr_recovery_codec;
use crate::ddr_recovery_journal::{AnchoredJournal, Frame, Head, Kind, Mode};
use crate::external_ddr::JoinedDrain;
use std::collections::HashSet;
use std::io;

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// Only this module can construct the checkpoint passed to the journal. It is
/// made after replay accepts the checkpoint; it is not a substitute for the
/// caller's actual stop/join, catalog reconciliation or witness evidence.
pub(crate) struct SemanticCheckpoint {
    broker: String,
    payload: Vec<u8>,
}

impl SemanticCheckpoint {
    pub(crate) fn broker(&self) -> &str {
        &self.broker
    }

    pub(crate) fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// A non-cloneable receipt for a synced semantic zero checkpoint. Only the
/// durable writer may construct it, after the actual JoinedDrain and replay.
/// It names the exact pinned files and head that the authority must recheck.
pub(crate) struct SemanticClean {
    domain: Domain,
    head: Head,
    file_identities: ((u64, u64), (u64, u64)),
}

impl SemanticClean {
    pub(crate) fn domain(&self) -> &Domain {
        &self.domain
    }

    pub(crate) fn head(&self) -> &Head {
        &self.head
    }

    pub(crate) fn file_identities(&self) -> ((u64, u64), (u64, u64)) {
        self.file_identities
    }
}

pub(crate) struct DurableAudit {
    journal: AnchoredJournal,
    replay: AuditReplay,
    broker: String,
    domain: Domain,
    poisoned: bool,
}

/// A recovery boundary is usable only if its original evidence and exact
/// transaction were verified under the same pinned authority lock. The only
/// constructor for now is isolated-test-only; production remains fail-closed.
#[derive(Clone)]
struct VerifiedRecovery {
    input_head: Head,
    prefix: Vec<Frame>,
    suffix: Vec<Frame>,
    file_identities: ((u64, u64), (u64, u64)),
    #[cfg(test)]
    coverage: Option<crate::ddr_recovery_coverage::CoverageReport>,
}

#[cfg(test)]
impl VerifiedRecovery {
    fn from_fixture(
        authority: &crate::ddr_managed_authority::ManagedAuthority,
    ) -> io::Result<Self> {
        let result = crate::ddr_recovery_fixture::verify_sealed(authority, None)?;
        let mut journal = authority.fixture_pinned_journal()?;
        let file_identities = journal.file_identities();
        let frames = journal.verify()?;
        let split = usize::try_from(result.input_head.sequence)
            .map_err(|_| invalid("recovery input sequence overflow"))?;
        if journal.head().mode != Mode::Clean
            || split == 0
            || split.checked_add(3).is_none_or(|end| end > frames.len())
            || result.clean_head.sequence != (split + 3) as u64
        {
            return Err(invalid("verified recovery no longer matches its journal"));
        }
        Ok(Self {
            input_head: result.input_head,
            prefix: frames[..split].to_vec(),
            suffix: frames[split..split + 3].to_vec(),
            file_identities,
            coverage: Some(crate::ddr_recovery_coverage::verify_recovered(authority)?),
        })
    }
}

/// Only called after the fixture verifier has reconstructed and matched all
/// recovery bytes from the pinned evidence. Validate the entire later history
/// too; this helper alone does not establish evidence or authorize production.
#[cfg(test)]
pub(crate) fn verify_fixture_recovery_history(
    journal: &mut AnchoredJournal,
    input_head: &Head,
    clean_head: &Head,
    expected: &Domain,
    coverage: &crate::ddr_recovery_coverage::CoverageReport,
) -> io::Result<()> {
    let frames = journal.verify()?;
    let split = usize::try_from(input_head.sequence)
        .map_err(|_| invalid("recovery input sequence overflow"))?;
    let end = split
        .checked_add(3)
        .filter(|end| split > 0 && *end <= frames.len())
        .ok_or_else(|| invalid("invalid recovery transaction boundary"))?;
    let checkpoint = &frames[end - 1];
    let offset = frames[..end].iter().map(Frame::encoded_len).sum::<u64>();
    if clean_head.mode != Mode::Clean
        || clean_head.broker != input_head.broker
        || clean_head.sequence != checkpoint.sequence
        || clean_head.offset != offset
        || clean_head.checksum != checkpoint.checksum
        || clean_head.clean_sequence != checkpoint.sequence
    {
        return Err(invalid("verified recovery checkpoint identity mismatch"));
    }
    let proof = VerifiedRecovery {
        input_head: input_head.clone(),
        prefix: frames[..split].to_vec(),
        suffix: frames[split..end].to_vec(),
        file_identities: journal.file_identities(),
        coverage: Some(coverage.clone()),
    };
    replay_history(
        &frames,
        journal.head(),
        expected,
        std::slice::from_ref(&proof),
    )?;
    Ok(())
}

/// Exercise semantic replay independently of the outer byte-match check.
/// These mutated frames never touch disk: their in-memory proof is changed to
/// match each forged suffix, so rejecting them requires semantic validation.
#[cfg(test)]
pub(crate) fn fixture_replay_rejects_forged_semantics(
    authority: &crate::ddr_managed_authority::ManagedAuthority,
) -> io::Result<usize> {
    use crate::ddr_recovery_coverage::{canonical, parse_line};
    let proof = VerifiedRecovery::from_fixture(authority)?;
    let (mut journal, _, domain) = authority.fixture_recovery_material()?;
    let frames = journal.verify()?;
    let head = journal.head().clone();
    replay_history(&frames, &head, &domain, std::slice::from_ref(&proof))?;
    let split = usize::try_from(proof.input_head.sequence)
        .map_err(|_| invalid("input sequence overflow"))?;
    let cases = [
        "missing-resolution",
        "resolution-identity",
        "resolution-type",
        "forged-before",
        "forged-after",
        "audit-input",
        "commit-input",
        "commit-hash",
        "checkpoint-audit",
        "checkpoint-commit",
    ];
    for case in cases {
        let mut altered = frames.clone();
        let position = match case {
            "commit-input" | "commit-hash" => split + 1,
            "checkpoint-audit" | "checkpoint-commit" => split + 2,
            _ => split,
        };
        let mut value = parse_line(&altered[position].payload)?;
        match case {
            "missing-resolution" => {
                value["resolutions"]
                    .as_array_mut()
                    .ok_or_else(|| invalid("missing resolutions"))?
                    .pop()
                    .ok_or_else(|| invalid("fixture has no resolution"))?;
            }
            "resolution-identity" => {
                value["resolutions"][0]["identity_sha256"] = "0".repeat(64).into()
            }
            "resolution-type" => value["resolutions"][0]["resolution"] = "UNKNOWN".into(),
            "forged-before" => value["before"]["retained_backings"] = 0.into(),
            "forged-after" => value["after"]["retained_backings"] = 1.into(),
            "audit-input" => value["input_head"]["checksum"] = "0000000000000000".into(),
            "commit-input" => value["input_head"]["offset"] = 0.into(),
            "commit-hash" => value["audit_sha256"] = "0".repeat(64).into(),
            "checkpoint-audit" => value["audit_sha256"] = "0".repeat(64).into(),
            "checkpoint-commit" => value["commit_sha256"] = "0".repeat(64).into(),
            _ => unreachable!(),
        }
        let mut encoded = canonical(&value)?;
        encoded.push(b'\n');
        if encoded == altered[position].payload {
            return Err(invalid("semantic rejection mutation did not change input"));
        }
        altered[position].payload = encoded;
        let mut forged = proof.clone();
        forged.suffix = altered[split..split + 3].to_vec();
        let error = replay_history(&altered, &head, &domain, &[forged])
            .err()
            .ok_or_else(|| invalid("forged semantic recovery accepted"))?;
        if !error
            .to_string()
            .contains("recovery semantic resolutions or transaction mismatch")
        {
            return Err(invalid(
                "forged recovery was not rejected by semantic replay",
            ));
        }
    }
    let mut forged = proof;
    forged.input_head.offset += 1;
    if replay_history(&frames, &head, &domain, &[forged]).is_ok() {
        return Err(invalid("wrong recovery input offset accepted"));
    }
    Ok(cases.len() + 1)
}

impl DurableAudit {
    /// A new writer can start only at genesis or after every previous normal
    /// checkpoint has been semantically replayed. Dirty recovery is separate.
    pub(crate) fn start(journal: AnchoredJournal, domain: Domain) -> io::Result<Self> {
        Self::start_inner(journal, domain, &[])
    }

    fn start_inner(
        mut journal: AnchoredJournal,
        domain: Domain,
        recoveries: &[VerifiedRecovery],
    ) -> io::Result<Self> {
        if journal.head().mode != Mode::Clean {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "dirty recovery journal requires trusted recovery",
            ));
        }
        let frames = journal.verify()?;
        if recoveries
            .iter()
            .any(|proof| proof.file_identities != journal.file_identities())
        {
            return Err(invalid("recovery proof belongs to different journal files"));
        }
        let (_, brokers) = replay_history(&frames, journal.head(), &domain, recoveries)?;
        if brokers.contains(&domain.broker) {
            return Err(invalid("broker incarnation was already used"));
        }
        let replay = AuditReplay::new(domain.clone()).map_err(invalid)?;
        Ok(Self {
            journal,
            replay,
            broker: domain.broker.clone(),
            domain,
            poisoned: false,
        })
    }

    #[cfg(test)]
    pub(crate) fn start_after_fixture_recovery(
        authority: &crate::ddr_managed_authority::ManagedAuthority,
        domain: Domain,
    ) -> io::Result<Self> {
        let recovery = VerifiedRecovery::from_fixture(authority)?;
        let (journal, _, _) = authority.fixture_recovery_material()?;
        if journal.file_identities() != recovery.file_identities {
            return Err(invalid(
                "verified recovery belongs to different journal files",
            ));
        }
        Self::start_inner(journal, domain, std::slice::from_ref(&recovery))
    }

    /// The caller must hold the authority lock and catalog serialization lock.
    /// It may perform the protected effect only after this returns success.
    /// Candidate replay is validated without mutating live state; the frame and
    /// exact dirty head are synced before publishing the candidate. The caller
    /// still must wire every lifecycle transition and actual close/drain proof.
    pub(crate) fn record(&mut self, event: Event) -> io::Result<()> {
        if matches!(
            event.kind,
            EventKind::CleanCheckpoint(_) | EventKind::BrokerDrained(_)
        ) {
            self.poisoned = true;
            return Err(invalid("drain and checkpoint require the joined path"));
        }
        self.record_transition(event)
    }

    fn record_transition(&mut self, event: Event) -> io::Result<()> {
        if self.poisoned {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "durable audit poisoned",
            ));
        }
        let mut candidate = self.replay.clone();
        if let Err(error) = candidate.apply(event.clone()) {
            self.poisoned = true;
            return Err(invalid(error));
        }
        let (kind, payload) = match ddr_recovery_codec::encode(&event) {
            Ok(encoded) => encoded,
            Err(error) => {
                self.poisoned = true;
                return Err(invalid(error));
            }
        };
        if let Err(error) = self.journal.append_dirty(&self.broker, kind, &payload) {
            self.poisoned = true;
            return Err(error);
        }
        self.replay = candidate;
        Ok(())
    }

    /// A real joined listener/handler result is necessary but not sufficient:
    /// the caller must first reconcile this snapshot with the locked catalog.
    pub(crate) fn record_joined_drain(
        &mut self,
        joined: &JoinedDrain,
        epoch: u64,
        snapshot: DrainSnapshot,
    ) -> io::Result<()> {
        if joined.broker() != self.broker {
            self.poisoned = true;
            return Err(invalid("joined drain belongs to another broker"));
        }
        self.record_transition(Event {
            domain: self.domain.clone(),
            epoch,
            kind: EventKind::BrokerDrained(snapshot),
        })
    }

    /// This is usable only after a real broker stop and joined handlers/workers
    /// have been recorded as BrokerDrained and reconciled against the catalog.
    /// It must never be called to clear an inherited dirty/quarantined journal.
    pub(crate) fn checkpoint_after_drain(
        &mut self,
        joined: &JoinedDrain,
    ) -> io::Result<SemanticClean> {
        if self.poisoned {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "durable audit poisoned",
            ));
        }
        if joined.broker() != self.broker {
            self.poisoned = true;
            return Err(invalid("joined checkpoint belongs to another broker"));
        }
        let event = Event {
            domain: self.domain.clone(),
            epoch: self.replay.epoch(),
            kind: EventKind::CleanCheckpoint(self.replay.observed_drain(0, 0)),
        };
        let mut candidate = self.replay.clone();
        candidate.apply(event.clone()).map_err(invalid)?;
        let (kind, payload) = ddr_recovery_codec::encode(&event).map_err(invalid)?;
        if kind != Kind::CleanCheckpoint {
            return Err(invalid("unexpected checkpoint kind"));
        }
        let checkpoint = SemanticCheckpoint {
            broker: self.broker.clone(),
            payload,
        };
        let head = match self.journal.append_clean_after_drain(&checkpoint) {
            Ok(head) => head,
            Err(error) => {
                self.poisoned = true;
                return Err(error);
            }
        };
        self.replay = candidate;
        Ok(SemanticClean {
            domain: self.domain.clone(),
            head,
            file_identities: self.journal.file_identities(),
        })
    }

    pub(crate) fn replay(&self) -> &AuditReplay {
        &self.replay
    }
    pub(crate) fn domain(&self) -> &Domain {
        &self.domain
    }
    pub(crate) fn head(&self) -> &Head {
        self.journal.head()
    }
    pub(crate) fn file_identities(&self) -> ((u64, u64), (u64, u64)) {
        self.journal.file_identities()
    }
    pub(crate) fn is_poisoned(&self) -> bool {
        self.poisoned
    }
    /// A lifecycle transition failed after a prior durable intent. The caller
    /// cannot safely infer whether the peer saw an FD, so stop new admissions.
    pub(crate) fn poison(&mut self) {
        self.poisoned = true;
    }
}

/// Validate all history rather than trusting a syntactically clean head.
/// Cap/domain changes remain forbidden without audited reconfiguration. A
/// recovery boundary requires an exact evidence-verified capability; no such
/// capability is currently constructible by the production entry point.
fn replay_history(
    frames: &[Frame],
    head: &Head,
    expected: &Domain,
    recoveries: &[VerifiedRecovery],
) -> io::Result<(Option<AuditReplay>, HashSet<String>)> {
    let mut replay: Option<AuditReplay> = None;
    let mut current: Option<Domain> = None;
    let mut brokers = HashSet::new();
    let mut latest_clean = 0;
    let mut last_broker = String::from("none");
    let mut ended_clean = true;
    let mut index = 0usize;
    let mut used_recoveries = HashSet::new();
    while index < frames.len() {
        let frame = &frames[index];
        if frame.kind == Kind::RecoveryAudit {
            let mut matches = recoveries
                .iter()
                .filter(|proof| proof.input_head.sequence == index as u64);
            let proof = matches
                .next()
                .ok_or_else(|| invalid("recovery requires verified evidence"))?;
            if matches.next().is_some() || !used_recoveries.insert(index as u64) {
                return Err(invalid("ambiguous recovery evidence"));
            }
            let end = index
                .checked_add(3)
                .ok_or_else(|| invalid("recovery suffix overflow"))?;
            let suffix = frames
                .get(index..end)
                .ok_or_else(|| invalid("incomplete recovery suffix"))?;
            let input = &proof.input_head;
            let last = frames.get(index.wrapping_sub(1));
            let input_offset = frames[..index].iter().try_fold(0u64, |offset, frame| {
                offset
                    .checked_add(frame.encoded_len())
                    .ok_or_else(|| invalid("recovery input offset overflow"))
            })?;
            if input.mode != Mode::Dirty
                || input.sequence != index as u64
                || input.offset != input_offset
                || input.clean_sequence != latest_clean
                || last.is_none_or(|last| {
                    last.kind != Kind::BrokerDrained
                        || last.sequence != input.sequence
                        || last.checksum != input.checksum
                })
                || current.as_ref().is_none_or(|d| d.broker != input.broker)
                || replay.as_ref().is_none_or(AuditReplay::is_checkpointed)
                || ended_clean
                || frames[..index] != proof.prefix
                || suffix != proof.suffix
                || suffix[0].kind != Kind::RecoveryAudit
                || suffix[1].kind != Kind::RecoveryCommit
                || suffix[2].kind != Kind::CleanCheckpoint
            {
                return Err(invalid(
                    "recovery boundary does not match verified input and audit",
                ));
            }
            // Exact-frame equality alone is not a semantic recovery replay.
            // Re-resolve the *current* replay using the independently pinned
            // coverage, then reconstruct each resolution, zero count, input
            // head and both chained hashes before accepting the boundary.
            #[cfg(test)]
            {
                let coverage = proof
                    .coverage
                    .as_ref()
                    .ok_or_else(|| invalid("recovery lacks verified coverage"))?;
                if coverage.input_head() != input {
                    return Err(invalid("recovery coverage has a different input head"));
                }
                replay
                    .as_ref()
                    .ok_or_else(|| invalid("missing recovery replay"))?
                    .resolved_fixture_drain(coverage)
                    .map_err(invalid)?;
                let audit = crate::ddr_recovery_coverage::parse_line(&suffix[0].payload)?;
                let id = crate::ddr_recovery_coverage::s(&audit, "recovery_id")?;
                let expected = crate::ddr_recovery_fixture::transaction(coverage, id)?;
                if suffix[0].payload != expected.audit
                    || suffix[1].payload != expected.commit
                    || suffix[2].payload != expected.checkpoint
                {
                    return Err(invalid(
                        "recovery semantic resolutions or transaction mismatch",
                    ));
                }
                last_broker = input.broker.clone();
                latest_clean = suffix[2].sequence;
                ended_clean = true;
                replay = None;
                current = None;
                index = end;
                continue;
            }
            #[cfg(not(test))]
            return Err(invalid(
                "production recovery evidence verifier not yet available",
            ));
        }
        let event = ddr_recovery_codec::decode(frame).map_err(invalid)?;
        let d = &event.domain;
        if d.host != expected.host
            || d.boot != expected.boot
            || d.domain != expected.domain
            || d.cap_revision != expected.cap_revision
            || d.owner_limit != expected.owner_limit
            || d.total_limit != expected.total_limit
        {
            return Err(invalid("audit history domain or budget mismatch"));
        }
        if replay.as_ref().is_none_or(AuditReplay::is_checkpointed) {
            if !recoveries.is_empty() && !matches!(event.kind, EventKind::BrokerBirth { .. }) {
                return Err(invalid("verified recovery history lacks broker birth"));
            }
            if !brokers.insert(d.broker.clone()) {
                return Err(invalid("audit history reused a broker incarnation"));
            }
            current = Some(d.clone());
            replay = Some(AuditReplay::new(d.clone()).map_err(invalid)?);
        } else if current.as_ref() != Some(d) {
            return Err(invalid("broker changed without a clean checkpoint"));
        }
        let checkpoint = matches!(event.kind, EventKind::CleanCheckpoint(_));
        last_broker = d.broker.clone();
        replay.as_mut().unwrap().apply(event).map_err(invalid)?;
        ended_clean = checkpoint;
        if checkpoint {
            latest_clean = frame.sequence;
        }
        index += 1;
    }
    if used_recoveries.len() != recoveries.len()
        || latest_clean != head.clean_sequence
        || last_broker != head.broker
        || (head.mode == Mode::Clean && !ended_clean)
        || (head.mode == Mode::Dirty && ended_clean)
    {
        return Err(invalid("audit history and durable head disagree"));
    }
    Ok((replay, brokers))
}

/// Strictly decode and replay all modeled incarnations for diagnostics. This
/// does not prove trusted exits or authorize dirty recovery.
pub(crate) fn inspect(mut journal: AnchoredJournal, domain: Domain) -> io::Result<AuditReplay> {
    if journal.head().sequence != 0 && journal.head().broker != domain.broker {
        return Err(invalid("head broker does not match audit domain"));
    }
    let frames = journal.verify()?;
    let (replay, _) = replay_history(&frames, journal.head(), &domain, &[])?;
    replay.map_or_else(|| AuditReplay::new(domain).map_err(invalid), Ok)
}

/// A deployed normal restart is not authorized by a clean-looking head alone.
/// Replay every incarnation and require an actual broker-birth record followed
/// by a legally drained zero checkpoint for each non-genesis generation.
pub(crate) fn verify_normal_restart(
    journal: &mut AnchoredJournal,
    domain: &Domain,
) -> io::Result<Vec<Frame>> {
    if journal.head().mode != Mode::Clean {
        return Err(invalid("normal restart requires a clean head"));
    }
    let frames = journal.verify()?;
    let mut at_birth = true;
    for frame in &frames {
        let event = ddr_recovery_codec::decode(frame).map_err(invalid)?;
        if at_birth && !matches!(event.kind, EventKind::BrokerBirth { .. }) {
            return Err(invalid("normal generation lacks broker birth"));
        }
        at_birth = matches!(event.kind, EventKind::CleanCheckpoint(_));
    }
    if !at_birth {
        return Err(invalid("normal generation lacks final checkpoint"));
    }
    replay_history(&frames, journal.head(), domain, &[])?;
    Ok(frames)
}

/// Read-only, non-authorizing semantic inspection of the versioned normal
/// generation history. A syntactically valid SEAL is not trusted witness
/// evidence. Recovery boundaries and configuration transitions remain closed.
/// Each generation starts with its exact bound broker birth and every prior
/// normal generation must end at a replayed zero checkpoint.
pub(crate) fn inspect_versioned(
    mut journal: AnchoredJournal,
    history: &crate::ddr_trust_history::TrustHistory,
) -> io::Result<()> {
    let original_head = journal.head().clone();
    let frames = journal.verify()?;
    let mut cursor = Head::parse(&AnchoredJournal::genesis_state())?;
    if history.generations.is_empty() {
        if !frames.is_empty() || cursor != original_head || history.config.is_some() {
            return Err(invalid(
                "empty trust history does not match journal genesis",
            ));
        }
        return Ok(());
    }
    let config = history
        .config
        .as_ref()
        .ok_or_else(|| invalid("versioned history lacks config"))?;
    if journal.file_identities() != (config.journal, config.journal_head) {
        return Err(invalid("versioned journal identity mismatch"));
    }
    let mut index = 0usize;
    for (position, generation) in history.generations.iter().enumerate() {
        if cursor != generation.initial_head || cursor.mode != Mode::Clean {
            return Err(invalid("versioned generation has wrong input head"));
        }
        let is_last = position + 1 == history.generations.len();
        let limit = match &generation.final_head {
            Some(head) if generation.seal_checksum.is_some() => head.sequence,
            None if is_last && generation.seal_checksum.is_none() => original_head.sequence,
            _ => return Err(invalid("incomplete versioned generation boundary")),
        };
        if limit < cursor.sequence || limit > original_head.sequence {
            return Err(invalid("versioned generation sequence out of range"));
        }
        let mut replay: Option<AuditReplay> = None;
        while cursor.sequence < limit {
            let frame = frames
                .get(index)
                .ok_or_else(|| invalid("missing versioned journal frame"))?;
            if frame.sequence != cursor.sequence + 1
                || frame.previous != cursor.checksum
                || matches!(frame.kind, Kind::RecoveryAudit | Kind::RecoveryCommit)
            {
                return Err(invalid("unsupported versioned recovery boundary"));
            }
            let event = ddr_recovery_codec::decode(frame).map_err(invalid)?;
            let domain = &event.domain;
            if domain.host != config.host
                || domain.boot != config.boot
                || domain.domain != config.domain
                || domain.cap_revision != config.revision
                || domain.owner_limit != config.owner_limit
                || domain.total_limit != config.total_limit
                || domain.broker != generation.broker
            {
                return Err(invalid("versioned journal generation domain mismatch"));
            }
            if replay.is_none() {
                if !matches!(&event.kind, EventKind::BrokerBirth { birth } if *birth == generation.broker_birth)
                {
                    return Err(invalid("versioned generation lacks bound broker birth"));
                }
                replay = Some(AuditReplay::new(domain.clone()).map_err(invalid)?);
            }
            let checkpoint = matches!(event.kind, EventKind::CleanCheckpoint(_));
            replay.as_mut().unwrap().apply(event).map_err(invalid)?;
            cursor.sequence = frame.sequence;
            cursor.offset = cursor
                .offset
                .checked_add(frame.encoded_len())
                .ok_or_else(|| invalid("journal offset overflow"))?;
            cursor.checksum = frame.checksum;
            cursor.broker = generation.broker.clone();
            cursor.mode = if checkpoint { Mode::Clean } else { Mode::Dirty };
            if checkpoint {
                cursor.clean_sequence = frame.sequence;
            }
            index += 1;
        }
        if let Some(final_head) = &generation.final_head {
            if &cursor != final_head
                || cursor.mode != Mode::Clean
                || replay.as_ref().is_none_or(|state| !state.is_checkpointed())
            {
                return Err(invalid("sealed generation lacks semantic zero checkpoint"));
            }
        }
    }
    if cursor != original_head || index != frames.len() || journal.verify()? != frames {
        return Err(invalid("versioned journal changed or has unbound history"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddr_process_identity::ProcessBirth;
    use crate::ddr_recovery_audit::{BackingIdentity, EventKind, Purpose, ViewIdentity};
    use std::fs::{self, File, OpenOptions};
    use std::io::Write;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct Fixture {
        root: PathBuf,
        journal: PathBuf,
        state: PathBuf,
        j: (u64, u64),
        s: (u64, u64),
    }
    impl Fixture {
        fn new() -> Self {
            let base = std::env::var_os("CARGO_TARGET_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target"));
            let base = fs::canonicalize(base).unwrap();
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let root = base.join(format!("durable-audit-{}-{nonce}", std::process::id()));
            fs::create_dir(&root).unwrap();
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
            let journal = root.join("journal");
            let state = root.join("state");
            let j = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&journal)
                .unwrap();
            let mut s = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&state)
                .unwrap();
            s.write_all(AnchoredJournal::genesis_state().as_bytes())
                .unwrap();
            j.sync_all().unwrap();
            s.sync_all().unwrap();
            File::open(&root).unwrap().sync_all().unwrap();
            let jm = j.metadata().unwrap();
            let sm = s.metadata().unwrap();
            Self {
                root,
                journal,
                state,
                j: (jm.dev(), jm.ino()),
                s: (sm.dev(), sm.ino()),
            }
        }
        fn open(&self) -> io::Result<AnchoredJournal> {
            AnchoredJournal::open(&self.journal, &self.state, self.j, self.s)
        }
    }
    fn birth(pid: i32) -> ProcessBirth {
        ProcessBirth {
            pid,
            uid: 1000,
            gid: 1000,
            pid_namespace_dev: 1,
            pid_namespace_ino: 2,
            starttime_ticks: 100,
        }
    }
    fn domain() -> Domain {
        Domain {
            host: "host".into(),
            boot: "boot".into(),
            domain: "ddr".into(),
            cap_revision: 1,
            broker: "broker".into(),
            owner_limit: 4096,
            total_limit: 4096,
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
    fn event(kind: EventKind) -> Event {
        Event {
            domain: domain(),
            epoch: 1,
            kind,
        }
    }

    #[test]
    fn invalid_transition_poison_preserves_existing_obligations_and_dirty_log() {
        let fixture = Fixture::new();
        let mut audit = DurableAudit::start(fixture.open().unwrap(), domain()).unwrap();
        audit
            .record(event(EventKind::RegisterIntent(backing())))
            .unwrap();
        audit
            .record(event(EventKind::RegisterCommit {
                object_id: 1,
                resource_generation: 2,
            }))
            .unwrap();
        audit.record(event(EventKind::ViewIntent(view()))).unwrap();
        audit
            .record(event(EventKind::ViewAbort {
                view_id: 9,
                lease_id: 10,
            }))
            .unwrap();
        assert_eq!(
            audit.replay().unresolved_holders(),
            vec![birth(10), birth(20)]
        );
        let len = fs::metadata(&fixture.journal).unwrap().len();
        let mut duplicate = backing();
        duplicate.object_id = 2;
        duplicate.memfd_ino = 101;
        assert!(audit
            .record(event(EventKind::RegisterIntent(duplicate)))
            .is_err());
        assert!(audit.is_poisoned());
        assert!(audit
            .record(event(EventKind::ViewAbort {
                view_id: 9,
                lease_id: 10,
            }))
            .is_err());
        assert_eq!(fs::metadata(&fixture.journal).unwrap().len(), len);
        assert_eq!(audit.replay().charged_bytes(), 4096);
        drop(audit);
        assert!(DurableAudit::start(fixture.open().unwrap(), domain()).is_err());
        let replay = inspect(fixture.open().unwrap(), domain()).unwrap();
        assert_eq!(replay.charged_bytes(), 4096);
        assert_eq!(replay.unresolved_holders(), vec![birth(10), birth(20)]);
    }

    #[test]
    fn changed_journal_poisons_live_writer_without_publishing_candidate() {
        let fixture = Fixture::new();
        let mut audit = DurableAudit::start(fixture.open().unwrap(), domain()).unwrap();
        audit
            .record(event(EventKind::RegisterIntent(backing())))
            .unwrap();
        fs::rename(&fixture.journal, fixture.root.join("old-journal")).unwrap();
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&fixture.journal)
            .unwrap();
        assert!(audit
            .record(event(EventKind::RegisterCommit {
                object_id: 1,
                resource_generation: 2
            }))
            .is_err());
        assert_eq!(audit.replay().charged_bytes(), 4096);
        fs::remove_file(&fixture.journal).unwrap();
        fs::rename(fixture.root.join("old-journal"), &fixture.journal).unwrap();
        assert!(audit
            .record(event(EventKind::RegisterCommit {
                object_id: 1,
                resource_generation: 2
            }))
            .is_err());
        drop(audit);
        let replay = inspect(fixture.open().unwrap(), domain()).unwrap();
        assert_eq!(replay.charged_bytes(), 4096);
    }

    #[test]
    fn normal_zero_checkpoint_replays_all_generations_and_rejects_reused_broker() {
        let fixture = Fixture::new();
        let mut first = DurableAudit::start(fixture.open().unwrap(), domain()).unwrap();
        let joined = crate::external_ddr::fixture_joined_drain("broker");
        first
            .record(event(EventKind::RegisterIntent(backing())))
            .unwrap();
        first
            .record(event(EventKind::RegisterCommit {
                object_id: 1,
                resource_generation: 2,
            }))
            .unwrap();
        first
            .record(event(EventKind::Revoke {
                object_id: 1,
                resource_generation: 2,
            }))
            .unwrap();
        first
            .record(event(EventKind::BackingCloseIntent {
                object_id: 1,
                resource_generation: 2,
            }))
            .unwrap();
        first
            .record(event(EventKind::BackingCloseDone {
                object_id: 1,
                resource_generation: 2,
            }))
            .unwrap();
        // Zero references alone are not a broker drain.
        assert!(first.checkpoint_after_drain(&joined).is_err());
        let snapshot = first.replay().observed_drain(0, 0);
        first.record_joined_drain(&joined, 1, snapshot).unwrap();
        first.checkpoint_after_drain(&joined).unwrap();
        drop(first);
        assert_eq!(fixture.open().unwrap().head().mode, Mode::Clean);
        assert!(DurableAudit::start(fixture.open().unwrap(), domain()).is_err());

        let mut next_domain = domain();
        next_domain.broker = "next-broker".into();
        let mut wrong = next_domain.clone();
        wrong.total_limit += 1;
        assert!(DurableAudit::start(fixture.open().unwrap(), wrong).is_err());
        let mut next = DurableAudit::start(fixture.open().unwrap(), next_domain.clone()).unwrap();
        let next_event = |kind| Event {
            domain: next_domain.clone(),
            epoch: 1,
            kind,
        };
        next.record(next_event(EventKind::RegisterIntent(backing())))
            .unwrap();
        assert_eq!(next.replay().charged_bytes(), 4096);
        drop(next);
        assert!(DurableAudit::start(fixture.open().unwrap(), domain()).is_err());
        assert_eq!(
            inspect(fixture.open().unwrap(), next_domain)
                .unwrap()
                .charged_bytes(),
            4096
        );
    }

    #[test]
    fn versioned_inspection_replays_two_normal_generations_without_authorizing() {
        use crate::ddr_trust_history::{Generation, TrustConfig, TrustHistory};
        let fixture = Fixture::new();
        let genesis = fixture.open().unwrap().head().clone();
        let mut first = DurableAudit::start(fixture.open().unwrap(), domain()).unwrap();
        first
            .record(Event {
                domain: domain(),
                epoch: 0,
                kind: EventKind::BrokerBirth { birth: birth(100) },
            })
            .unwrap();
        let joined = crate::external_ddr::fixture_joined_drain("broker");
        let snapshot = first.replay().observed_drain(0, 0);
        first.record_joined_drain(&joined, 1, snapshot).unwrap();
        first.checkpoint_after_drain(&joined).unwrap();
        drop(first);
        let first_head = fixture.open().unwrap().head().clone();
        let mut next_domain = domain();
        next_domain.broker = "next-broker".into();
        let mut second = DurableAudit::start(fixture.open().unwrap(), next_domain.clone()).unwrap();
        second
            .record(Event {
                domain: next_domain.clone(),
                epoch: 0,
                kind: EventKind::BrokerBirth { birth: birth(101) },
            })
            .unwrap();
        let joined = crate::external_ddr::fixture_joined_drain("next-broker");
        let snapshot = second.replay().observed_drain(0, 0);
        second.record_joined_drain(&joined, 1, snapshot).unwrap();
        second.checkpoint_after_drain(&joined).unwrap();
        drop(second);
        let second_head = fixture.open().unwrap().head().clone();

        // Synthetic trust records only: actual witness evidence remains an
        // independent prerequisite that this inspection deliberately lacks.
        let generation =
            |name: &str, broker: &str, b: ProcessBirth, initial: Head, final_head: Head| {
                Generation {
                    generation: name.into(),
                    broker: broker.into(),
                    broker_birth: b,
                    witness: birth(200),
                    bind_sequence: 2,
                    bind_checksum: "1".repeat(64),
                    initial_head: initial,
                    seal_checksum: Some("2".repeat(64)),
                    final_head: Some(final_head),
                    evidence: None,
                }
            };
        let first_gen = generation("g1", "broker", birth(100), genesis, first_head.clone());
        let second_gen = generation("g2", "next-broker", birth(101), first_head, second_head);
        let config = TrustConfig {
            host: "host".into(),
            boot: "boot".into(),
            domain: "ddr".into(),
            revision: 1,
            owner_limit: 4096,
            total_limit: 4096,
            endpoint: "synthetic".into(),
            journal: fixture.j,
            journal_head: fixture.s,
        };
        let make = |a: Generation, b: Generation, config: TrustConfig| TrustHistory {
            policy_digest: "3".repeat(64),
            generations: vec![a, b],
            config: Some(config),
        };
        assert!(inspect_versioned(
            fixture.open().unwrap(),
            &make(first_gen.clone(), second_gen.clone(), config.clone())
        )
        .is_ok());
        for fault in [
            "first-birth",
            "second-birth",
            "domain",
            "inode",
            "boundary",
            "seal",
            "broker",
        ] {
            let mut a = first_gen.clone();
            let mut b = second_gen.clone();
            let mut altered = config.clone();
            match fault {
                "first-birth" => a.broker_birth = birth(999),
                "second-birth" => b.broker_birth = birth(999),
                "domain" => altered.total_limit += 1,
                "inode" => altered.journal.1 += 1,
                "boundary" => b.initial_head.offset += 1,
                "seal" => a.seal_checksum = None,
                "broker" => b.broker = "unbound".into(),
                _ => unreachable!(),
            }
            assert!(
                inspect_versioned(fixture.open().unwrap(), &make(a, b, altered)).is_err(),
                "fault={fault}"
            );
        }

        let missing_birth = Fixture::new();
        let initial = missing_birth.open().unwrap().head().clone();
        let mut writer = DurableAudit::start(missing_birth.open().unwrap(), domain()).unwrap();
        writer
            .record(event(EventKind::RegisterIntent(backing())))
            .unwrap();
        drop(writer);
        let missing_history = TrustHistory {
            policy_digest: "3".repeat(64),
            generations: vec![Generation {
                generation: "g1".into(),
                broker: "broker".into(),
                broker_birth: birth(100),
                witness: birth(200),
                bind_sequence: 2,
                bind_checksum: "1".repeat(64),
                initial_head: initial,
                seal_checksum: None,
                final_head: None,
                evidence: None,
            }],
            config: Some(TrustConfig {
                journal: missing_birth.j,
                journal_head: missing_birth.s,
                ..config
            }),
        };
        assert!(inspect_versioned(missing_birth.open().unwrap(), &missing_history).is_err());
    }

    #[test]
    fn synthetic_recovery_boundaries_cannot_authorize_without_coverage() {
        // This deliberately constructs fake VerifiedRecovery capabilities in
        // this cfg(test) module. Production still has no such constructor.
        fn push(
            frames: &mut Vec<Frame>,
            head: &mut Head,
            broker: &str,
            kind: Kind,
            payload: Vec<u8>,
        ) {
            let sequence = head.sequence + 1;
            let bytes = Frame::encode_fixture(kind, sequence, head.checksum, &payload).unwrap();
            let checksum =
                u64::from_be_bytes(bytes[bytes.len() - 12..bytes.len() - 4].try_into().unwrap());
            frames.push(Frame {
                kind,
                sequence,
                previous: head.checksum,
                payload,
                checksum,
            });
            head.sequence = sequence;
            head.offset += bytes.len() as u64;
            head.checksum = checksum;
            head.broker = broker.into();
            head.mode = Mode::Dirty;
        }
        let mut frames = Vec::new();
        let mut head = Head::parse(&AnchoredJournal::genesis_state()).unwrap();
        let mut inputs = Vec::new();
        for (index, broker) in ["broker", "next-broker"].iter().enumerate() {
            let mut d = domain();
            d.broker = (*broker).into();
            let zero = AuditReplay::new(d.clone()).unwrap().observed_drain(0, 0);
            for event in [
                Event {
                    domain: d.clone(),
                    epoch: 0,
                    kind: EventKind::BrokerBirth {
                        birth: birth(100 + index as i32),
                    },
                },
                Event {
                    domain: d.clone(),
                    epoch: 1,
                    kind: EventKind::BrokerDrained(zero),
                },
            ] {
                let (kind, payload) = ddr_recovery_codec::encode(&event).unwrap();
                push(&mut frames, &mut head, broker, kind, payload);
            }
            inputs.push(head.clone());
            for kind in [
                Kind::RecoveryAudit,
                Kind::RecoveryCommit,
                Kind::CleanCheckpoint,
            ] {
                push(
                    &mut frames,
                    &mut head,
                    broker,
                    kind,
                    b"synthetic-only".to_vec(),
                );
            }
            head.mode = Mode::Clean;
            head.clean_sequence = head.sequence;
        }
        let proof = |input: &Head| {
            let split = input.sequence as usize;
            VerifiedRecovery {
                input_head: input.clone(),
                prefix: frames[..split].to_vec(),
                suffix: frames[split..split + 3].to_vec(),
                file_identities: ((1, 2), (1, 3)),
                coverage: None,
            }
        };
        let proofs = vec![proof(&inputs[0]), proof(&inputs[1])];
        assert!(replay_history(&frames, &head, &domain(), &proofs).is_err());
        assert!(replay_history(&frames, &head, &domain(), &[]).is_err());
        assert!(replay_history(&frames, &head, &domain(), &proofs[..1]).is_err());
        let mut duplicate = vec![proof(&inputs[0]), proof(&inputs[1]), proof(&inputs[1])];
        assert!(replay_history(&frames, &head, &domain(), &duplicate).is_err());
        duplicate.pop();
        duplicate[1].input_head.clean_sequence = 0;
        assert!(replay_history(&frames, &head, &domain(), &duplicate).is_err());
        let mut bad = vec![proof(&inputs[0]), proof(&inputs[1])];
        bad[0].suffix[0].payload.push(0);
        assert!(replay_history(&frames, &head, &domain(), &bad).is_err());
        let mut bad = vec![proof(&inputs[0]), proof(&inputs[1])];
        bad[1].prefix[0].payload.push(0);
        assert!(replay_history(&frames, &head, &domain(), &bad).is_err());
    }

    #[test]
    fn generic_drain_and_other_broker_join_cannot_seal_the_journal() {
        let fixture = Fixture::new();
        let mut audit = DurableAudit::start(fixture.open().unwrap(), domain()).unwrap();
        audit
            .record(event(EventKind::RegisterIntent(backing())))
            .unwrap();
        let before = fs::read(&fixture.journal).unwrap();
        let snapshot = audit.replay().observed_drain(0, 0);
        assert!(audit
            .record(event(EventKind::BrokerDrained(snapshot)))
            .is_err());
        assert!(audit.is_poisoned());
        assert_eq!(fs::read(&fixture.journal).unwrap(), before);
        drop(audit);

        let fixture = Fixture::new();
        let mut audit = DurableAudit::start(fixture.open().unwrap(), domain()).unwrap();
        audit
            .record(event(EventKind::RegisterIntent(backing())))
            .unwrap();
        let before = fs::read(&fixture.journal).unwrap();
        let snapshot = audit.replay().observed_drain(0, 0);
        let other = crate::external_ddr::fixture_joined_drain("another-broker");
        assert!(audit.record_joined_drain(&other, 1, snapshot).is_err());
        assert!(audit.is_poisoned());
        assert!(audit.checkpoint_after_drain(&other).is_err());
        assert_eq!(fs::read(&fixture.journal).unwrap(), before);
        drop(audit);
        assert_eq!(fixture.open().unwrap().head().mode, Mode::Dirty);
    }

    #[test]
    fn live_or_ambiguous_holders_cannot_be_checkpointed_or_reopened_clean() {
        let fixture = Fixture::new();
        let mut audit = DurableAudit::start(fixture.open().unwrap(), domain()).unwrap();
        let joined = crate::external_ddr::fixture_joined_drain("broker");
        audit
            .record(event(EventKind::RegisterIntent(backing())))
            .unwrap();
        audit
            .record(event(EventKind::RegisterCommit {
                object_id: 1,
                resource_generation: 2,
            }))
            .unwrap();
        audit.record(event(EventKind::ViewIntent(view()))).unwrap();
        audit
            .record(event(EventKind::ViewAbort {
                view_id: 9,
                lease_id: 10,
            }))
            .unwrap();
        let before = fs::metadata(&fixture.journal).unwrap().len();
        assert!(audit.checkpoint_after_drain(&joined).is_err());
        assert_eq!(before, fs::metadata(&fixture.journal).unwrap().len());
        let snapshot = audit.replay().observed_drain(0, 0);
        audit.record_joined_drain(&joined, 1, snapshot).unwrap();
        assert!(audit.checkpoint_after_drain(&joined).is_err());
        drop(audit);
        let journal = fixture.open().unwrap();
        assert_eq!(journal.head().mode, Mode::Dirty);
        let mut next = domain();
        next.broker = "next-broker".into();
        assert!(DurableAudit::start(journal, next).is_err());
    }

    #[test]
    fn syntactically_clean_head_cannot_hide_a_nonzero_drain() {
        let fixture = Fixture::new();
        let mut journal = fixture.open().unwrap();
        let register = event(EventKind::RegisterIntent(backing()));
        let (kind, bytes) = ddr_recovery_codec::encode(&register).unwrap();
        journal.append_dirty("broker", kind, &bytes).unwrap();
        let mut replay = AuditReplay::new(domain()).unwrap();
        replay.apply(register).unwrap();
        // Complete the handoff before the actual nonzero drain.
        let commit = event(EventKind::RegisterCommit {
            object_id: 1,
            resource_generation: 2,
        });
        let (kind, bytes) = ddr_recovery_codec::encode(&commit).unwrap();
        journal.append_dirty("broker", kind, &bytes).unwrap();
        replay.apply(commit).unwrap();
        let drained = event(EventKind::BrokerDrained(replay.observed_drain(0, 0)));
        let (kind, bytes) = ddr_recovery_codec::encode(&drained).unwrap();
        journal.append_dirty("broker", kind, &bytes).unwrap();
        let forged = event(EventKind::CleanCheckpoint(
            crate::ddr_recovery_audit::DrainSnapshot {
                handlers: 0,
                pending_handoffs: 0,
                retained_backings: 0,
                active_views: 0,
                reservations: 0,
                in_flight: 0,
                potential_holders: 0,
            },
        ));
        let (_, bytes) = ddr_recovery_codec::encode(&forged).unwrap();
        journal
            .fixture_append_framing_clean("broker", &bytes)
            .unwrap();
        drop(journal);
        let mut next = domain();
        next.broker = "next-broker".into();
        assert!(DurableAudit::start(fixture.open().unwrap(), next).is_err());
    }
}
