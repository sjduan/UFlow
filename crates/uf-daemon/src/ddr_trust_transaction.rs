//! Crash-conservative trust append mechanism under the original authority lock.
//!
//! The internal live-BIND primitive checks live identities and the original
//! pinned authority, but does not prove the broker is held at a ready gate.
//! It is not a deployment CLI and cannot authorize admission or recovery.
//! SEAL and recovery are not wired. Never expose an arbitrary-frame CLI or
//! treat a syntactically valid frame as authorization.
#![allow(dead_code)]

use super::*;
use crate::ddr_recovery_coverage::{canonical, digest, n, parse_line, s};
use crate::ddr_trust_history::{verify, TrustAnchor};
use serde_json::{json, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    BeforePending,
    PendingTruncated,
    PendingWritten,
    PendingSynced,
    TrustPartial,
    TrustWritten,
    TrustSynced,
    FinalTruncated,
    FinalWritten,
    FinalSynced,
    FinalReadback,
}

// Constructed only after the specific preflight below; test fixtures also
// construct synthetic records to exercise interrupted-write boundaries.
struct PreparedTrustAppend {
    frame: Vec<u8>,
}

fn encode_state(value: &Value) -> io::Result<String> {
    let mut bytes = canonical(value)?;
    bytes.push(b'\n');
    if bytes.len() as u64 > MAX_CONFIG_BYTES {
        return Err(invalid("versioned state exceeds limit"));
    }
    String::from_utf8(bytes).map_err(|_| invalid("invalid versioned state"))
}

fn trust_has_capacity(sequence: u64, existing: usize, append: usize) -> bool {
    sequence < crate::ddr_trust_history::MAX_FRAMES as u64
        && existing
            .checked_add(append)
            .is_some_and(|len| len as u64 <= MAX_TRUST_BYTES)
}

impl ManagedAuthority {
    /// Prepare a live BIND candidate using only the locked authority's pinned
    /// policy/config/journal and an initial witness stream. The supplied
    /// directory is a *proposal before BIND*, never a recovery-time trust
    /// selector. This is read-only and does not prove a ready gate or authorize
    /// any listener/admission; a trusted launcher must still perform both.
    pub(crate) fn prepare_live_binding(
        &self,
        generation_id: &str,
        broker: &str,
        evidence_dir: &Path,
    ) -> io::Result<crate::ddr_witness_evidence::LiveBindingGuard> {
        use crate::ddr_recovery_coverage::birth;
        use crate::ddr_recovery_journal::Mode;
        use crate::ddr_trust_history::{EvidenceBinding, Generation, TrustConfig};
        if !self.bootstrap.deployed
            || self.admission_poisoned.get()
            || !name_ok(generation_id)
            || !name_ok(broker)
        {
            return Err(invalid("live binding preflight unavailable"));
        }
        if let Err(error) = self.ensure_namespace() {
            self.admission_poisoned.set(true);
            return Err(error);
        }
        let state = parse_line(
            self.deployed_state
                .as_ref()
                .ok_or_else(|| invalid("missing deployed state"))?
                .as_bytes(),
        )?;
        if s(&state, "mode")? != "clean"
            || !state["pending"].is_null()
            || !matches!(s(&state, "closure")?, "closed" | "sealed")
        {
            return Err(invalid("live binding requires a closed, clean authority"));
        }
        if !evidence_dir.is_absolute()
            || !evidence_dir.starts_with("/workspace/uflow/runs/")
            || fs::canonicalize(evidence_dir)? != evidence_dir
        {
            return Err(invalid("invalid proposed witness evidence directory"));
        }
        let directory = private_dir(evidence_dir)?;
        let journal_file = private_file(&evidence_dir.join("witness.journal"), false)?;
        let head_file = private_file(&evidence_dir.join("witness.head"), false)?;
        let journal_meta = journal_file.metadata()?;
        let head_meta = head_file.metadata()?;
        let journal = read_bounded_with_limit(&journal_file, MAX_TRUST_BYTES)?;
        let head = read_bounded_with_limit(&head_file, MAX_CONFIG_BYTES)?;
        let lines: Vec<_> = journal
            .as_bytes()
            .split_inclusive(|byte| *byte == b'\n')
            .collect();
        if lines.len() != 2 {
            return Err(invalid("initial witness must contain two frames"));
        }
        let start = parse_line(lines[0])?;
        let child = parse_line(lines[1])?;
        let trust = read_bounded_with_limit(
            &private_file(&self.root.join(WITNESS_TRUST_FILE), false)?,
            MAX_TRUST_BYTES,
        )?;
        let anchor = TrustAnchor {
            sequence: n(&state, "trust_sequence")?,
            offset: n(&state, "trust_offset")?,
            sha256: s(&state, "trust_sha256")?.to_owned(),
        };
        let history = verify(trust.as_bytes(), &anchor)?;
        let first = trust
            .as_bytes()
            .split_inclusive(|byte| *byte == b'\n')
            .next()
            .ok_or_else(|| invalid("missing fixed deployment policy"))?;
        let policy = crate::ddr_deployment_policy::DeploymentPolicy::from_initial_chain(first)?;
        if policy.policy_digest()? != history.policy_digest {
            return Err(invalid("pinned policy mismatch"));
        }
        let config = TrustConfig {
            host: self.bootstrap.host.clone(),
            boot: self.bootstrap.boot.clone(),
            domain: DOMAIN.into(),
            revision: self.bootstrap.revision,
            owner_limit: self.bootstrap.owner_limit,
            total_limit: self.bootstrap.total_limit,
            endpoint: self.bootstrap.endpoint.clone(),
            journal: (self.bootstrap.journal_dev, self.bootstrap.journal_ino),
            journal_head: (
                self.bootstrap.journal_head_dev,
                self.bootstrap.journal_head_ino,
            ),
        };
        let journal = self.open_journal()?;
        let initial_head = journal.head().clone();
        // Do not even propose a later generation over an unchecked historical
        // witness or a syntactically clean but semantically invalid journal.
        crate::ddr_recovery_durable::inspect_versioned(journal, &history)?;
        if !history.generations.is_empty() {
            crate::ddr_witness_evidence::verify_sealed_history(&policy, &history)?;
        }
        if initial_head.mode != Mode::Clean || initial_head.clean_sequence != initial_head.sequence
        {
            return Err(invalid("proposed live binding does not start clean"));
        }
        let generation = Generation {
            generation: generation_id.into(),
            broker: broker.into(),
            broker_birth: birth(&child["event"]["birth"])?,
            witness: birth(&start["event"]["witness_birth"])?,
            bind_sequence: 0,
            bind_checksum: String::new(),
            initial_head,
            seal_checksum: None,
            final_head: None,
            evidence: Some(EvidenceBinding {
                directory: evidence_dir
                    .to_str()
                    .ok_or_else(|| invalid("invalid evidence path"))?
                    .into(),
                directory_id: (directory.dev(), directory.ino()),
                journal_id: (journal_meta.dev(), journal_meta.ino()),
                head_id: (head_meta.dev(), head_meta.ino()),
                initial_head_sha256: digest(head.as_bytes()),
                final_head: None,
            }),
        };
        let result =
            crate::ddr_witness_evidence::verify_live_binding(&policy, &config, &generation)?;
        if let Err(error) = self.ensure_namespace() {
            self.admission_poisoned.set(true);
            return Err(error);
        }
        result.recheck()?;
        Ok(result)
    }

    /// The ready-gated broker's independent post-release check. This confirms
    /// the exact live pair and immutable BIND under its own authority lock,
    /// but still does not authorize admission or prove resource coverage.
    pub(crate) fn verify_committed_live_binding_for_self(
        &self,
        live: &crate::ddr_witness_evidence::LiveBindingGuard,
    ) -> io::Result<()> {
        let checked = (|| {
            if !self.bootstrap.deployed || self.admission_poisoned.get() {
                return Err(invalid("committed live binding unavailable"));
            }
            self.ensure_namespace()?;
            live.recheck()?;
            let expected = live.generation();
            if expected.broker_birth.pid != std::process::id() as i32 {
                return Err(invalid("committed binding is not for this broker process"));
            }
            let state = parse_line(
                self.deployed_state
                    .as_ref()
                    .ok_or_else(|| invalid("missing deployed state"))?
                    .as_bytes(),
            )?;
            if s(&state, "closure")? != "bound"
                || s(&state, "mode")? != "clean"
                || !state["pending"].is_null()
            {
                return Err(invalid("committed live binding is not a clean bound state"));
            }
            let anchor = TrustAnchor {
                sequence: n(&state, "trust_sequence")?,
                offset: n(&state, "trust_offset")?,
                sha256: s(&state, "trust_sha256")?.to_owned(),
            };
            let trust = read_bounded_with_limit(
                &private_file(&self.root.join(WITNESS_TRUST_FILE), false)?,
                MAX_TRUST_BYTES,
            )?;
            let history = verify(trust.as_bytes(), &anchor)?;
            let actual = history
                .generations
                .last()
                .ok_or_else(|| invalid("committed generation missing"))?;
            let mut expected_frame = expected.clone();
            expected_frame.bind_sequence = actual.bind_sequence;
            expected_frame.bind_checksum = actual.bind_checksum.clone();
            if history.config.as_ref() != Some(live.config())
                || history.policy_digest != live.policy_digest()?
                || actual != &expected_frame
                || actual.bind_sequence != anchor.sequence
                || self.open_journal()?.head() != &expected.initial_head
            {
                return Err(invalid("committed BIND differs from the gated live pair"));
            }
            self.ensure_namespace()?;
            live.recheck()
        })();
        if checked.is_err() {
            self.admission_poisoned.set(true);
        }
        checked
    }

    /// Reconstruct the live-pair guard exclusively from the independently
    /// anchored BIND after the broker has acquired its own lock description.
    /// This is a verification primitive, not permission to admit work: the
    /// deployed state remains quarantined until the remaining lifecycle and
    /// recovery invariants are implemented and verified.
    pub(crate) fn verify_bound_self_from_anchor(
        &self,
    ) -> io::Result<crate::ddr_trust_history::Generation> {
        let checked = (|| {
            if !self.bootstrap.deployed || self.admission_poisoned.get() {
                return Err(invalid("anchored live binding unavailable"));
            }
            self.ensure_namespace()?;
            let state = parse_line(
                self.deployed_state
                    .as_ref()
                    .ok_or_else(|| invalid("missing deployed state"))?
                    .as_bytes(),
            )?;
            if s(&state, "closure")? != "bound"
                || s(&state, "mode")? != "clean"
                || !state["pending"].is_null()
            {
                return Err(invalid("authority has no clean bound generation"));
            }
            let trust = read_bounded_with_limit(
                &private_file(&self.root.join(WITNESS_TRUST_FILE), false)?,
                MAX_TRUST_BYTES,
            )?;
            let anchor = TrustAnchor {
                sequence: n(&state, "trust_sequence")?,
                offset: n(&state, "trust_offset")?,
                sha256: s(&state, "trust_sha256")?.to_owned(),
            };
            let history = verify(trust.as_bytes(), &anchor)?;
            let generation = history
                .generations
                .last()
                .ok_or_else(|| invalid("missing bound generation"))?;
            if generation.seal_checksum.is_some() || generation.bind_sequence != anchor.sequence {
                return Err(invalid("last generation is not the active BIND"));
            }
            let config = history
                .config
                .as_ref()
                .ok_or_else(|| invalid("missing bound configuration"))?;
            let first = trust
                .as_bytes()
                .split_inclusive(|byte| *byte == b'\n')
                .next()
                .ok_or_else(|| invalid("missing fixed deployment policy"))?;
            let policy = crate::ddr_deployment_policy::DeploymentPolicy::from_initial_chain(first)?;
            let live =
                crate::ddr_witness_evidence::verify_live_binding(&policy, config, generation)?;
            self.verify_committed_live_binding_for_self(&live)?;
            Ok(generation.clone())
        })();
        if checked.is_err() {
            self.admission_poisoned.set(true);
        }
        checked
    }

    /// Start only the no-admission quarantine lifecycle for the live, pinned
    /// pair. Recheck all retained generations under this broker's own lock.
    pub(crate) fn start_bound_quarantine_audit(
        &self,
        generation: &crate::ddr_trust_history::Generation,
    ) -> io::Result<crate::ddr_recovery_durable::DurableAudit> {
        if &self.verify_bound_self_from_anchor()? != generation {
            return Err(invalid("bound generation changed before audit startup"));
        }
        let state = parse_line(self.deployed_state.as_ref().unwrap().as_bytes())?;
        let trust = read_bounded_with_limit(
            &private_file(&self.root.join(WITNESS_TRUST_FILE), false)?,
            MAX_TRUST_BYTES,
        )?;
        let anchor = TrustAnchor {
            sequence: n(&state, "trust_sequence")?,
            offset: n(&state, "trust_offset")?,
            sha256: s(&state, "trust_sha256")?.into(),
        };
        let mut history = verify(trust.as_bytes(), &anchor)?;
        crate::ddr_recovery_durable::inspect_versioned(self.open_journal()?, &history)?;
        if history.generations.pop().as_ref() != Some(generation) {
            return Err(invalid("bound generation changed while opening audit"));
        }
        if !history.generations.is_empty() {
            let initial = trust
                .as_bytes()
                .split_inclusive(|b| *b == b'\n')
                .next()
                .ok_or_else(|| invalid("missing deployment policy"))?;
            let policy =
                crate::ddr_deployment_policy::DeploymentPolicy::from_initial_chain(initial)?;
            crate::ddr_witness_evidence::verify_sealed_history(&policy, &history)?;
        }
        let domain = crate::ddr_recovery_audit::Domain {
            host: self.bootstrap.host.clone(),
            boot: self.bootstrap.boot.clone(),
            domain: DOMAIN.into(),
            cap_revision: self.bootstrap.revision,
            broker: generation.broker.clone(),
            owner_limit: self.bootstrap.owner_limit,
            total_limit: self.bootstrap.total_limit,
        };
        let audit = crate::ddr_recovery_durable::DurableAudit::start(self.open_journal()?, domain)?;
        self.verify_bound_self_from_anchor()?;
        Ok(audit)
    }

    /// Publish one synced no-work frame in the independently pinned state.
    /// Crashes between journal and state writes make reopen invalid, never
    /// silently clean. Only birth/drain/checkpoint are accepted; quarantine
    /// and all production admission restrictions remain in force.
    pub(crate) fn publish_bound_quarantine_head(
        &mut self,
        audit: &crate::ddr_recovery_durable::DurableAudit,
    ) -> io::Result<()> {
        use crate::ddr_recovery_journal::{Head, Kind, Mode};
        let result = (|| {
            if !self.bootstrap.deployed
                || !self.quarantined
                || self.admission_poisoned.get()
                || audit.is_poisoned()
                || !self.matches_audit_domain(audit.domain())
            {
                return Err(invalid("bound quarantine writer unavailable"));
            }
            self.ensure_static_namespace()?;
            let old_text = self
                .deployed_state
                .as_ref()
                .ok_or_else(|| invalid("missing bound state"))?
                .clone();
            let state_file = private_file(&self.root.join(STATE_FILE), false)?;
            let state_meta = state_file.metadata()?;
            if (state_meta.dev(), state_meta.ino())
                != (self.bootstrap.state_dev, self.bootstrap.state_ino)
                || read_bounded(&state_file)? != old_text
            {
                return Err(invalid("bound state changed during journal append"));
            }
            let mut state = parse_line(old_text.as_bytes())?;
            if s(&state, "closure")? != "bound" || !state["pending"].is_null() {
                return Err(invalid("bound lifecycle state unavailable"));
            }
            let previous = Head::parse(s(&state, "journal_head")?)?;
            let trust_file = private_file(&self.root.join(WITNESS_TRUST_FILE), false)?;
            let meta = trust_file.metadata()?;
            if (meta.dev(), meta.ino())
                != (
                    self.bootstrap.witness_trust_dev,
                    self.bootstrap.witness_trust_ino,
                )
            {
                return Err(invalid("bound trust inode changed"));
            }
            let trust = read_bounded_with_limit(&trust_file, MAX_TRUST_BYTES)?;
            if !verify_deployment_state(&self.bootstrap, trust.as_bytes(), &old_text, &previous)? {
                return Err(invalid("bound lifecycle lost quarantine"));
            }
            let anchor = TrustAnchor {
                sequence: n(&state, "trust_sequence")?,
                offset: n(&state, "trust_offset")?,
                sha256: s(&state, "trust_sha256")?.into(),
            };
            let history = verify(trust.as_bytes(), &anchor)?;
            let generation = history
                .generations
                .last()
                .ok_or_else(|| invalid("missing bound generation"))?;
            let mut journal = self.open_journal()?;
            let frames = journal.verify()?;
            let head = journal.head().clone();
            let ordinal = head
                .sequence
                .checked_sub(generation.initial_head.sequence)
                .ok_or_else(|| invalid("bound lifecycle sequence moved backwards"))?;
            let expected = match ordinal {
                1 => Kind::BrokerBirth,
                2 => Kind::BrokerDrained,
                3 => Kind::CleanCheckpoint,
                _ => return Err(invalid("bound quarantine may not record resource work")),
            };
            if generation.seal_checksum.is_some()
                || generation.broker_birth.pid != std::process::id() as i32
                || generation.broker != audit.domain().broker
                || head.broker != generation.broker
                || previous.sequence.checked_add(1) != Some(head.sequence)
                || audit.head() != &head
                || audit.file_identities() != journal.file_identities()
                || frames.last().is_none_or(|f| f.kind != expected)
                || (ordinal == 3) != (head.mode == Mode::Clean)
            {
                return Err(invalid("bound quarantine journal transition mismatch"));
            }
            crate::ddr_recovery_durable::inspect_versioned(self.open_journal()?, &history)?;
            state["mode"] = if head.mode == Mode::Clean {
                "clean"
            } else {
                "dirty"
            }
            .into();
            state["journal_head"] = head.encode().into();
            let final_text = encode_state(&state)?;
            if !verify_deployment_state(&self.bootstrap, trust.as_bytes(), &final_text, &head)? {
                return Err(invalid("bound lifecycle unexpectedly authorized"));
            }
            self.ensure_static_namespace()?;
            if read_bounded(&private_file(&self.root.join(STATE_FILE), false)?)? != old_text
                || read_bounded_with_limit(
                    &private_file(&self.root.join(WITNESS_TRUST_FILE), false)?,
                    MAX_TRUST_BYTES,
                )? != trust
                || self.open_journal()?.head() != &head
            {
                return Err(invalid(
                    "bound lifecycle namespace changed before state publish",
                ));
            }
            self.write_transaction_state(&final_text, false, &mut |_| Ok(()))?;
            self.deployed_state = Some(final_text);
            self.ensure_namespace()
        })();
        if result.is_err() {
            self.admission_poisoned.set(true);
        }
        result
    }

    /// Seal only the pinned no-admission lifecycle after its real broker has
    /// exited and the original live witness has durably recorded its wait.
    /// No evidence path, holder assertion, final head or frame is caller input.
    pub(crate) fn seal_bound_quarantine(
        &mut self,
        parent: &crate::ddr_process_identity::ProcessBirth,
    ) -> io::Result<()> {
        use crate::ddr_recovery_journal::{Kind, Mode};
        use crate::ddr_trust_history::TrustHistory;
        if !self.bootstrap.deployed || !self.quarantined || self.admission_poisoned.get() {
            return Err(invalid("bound seal writer unavailable"));
        }
        self.ensure_namespace()?;
        let state = parse_line(self.deployed_state.as_ref().unwrap().as_bytes())?;
        if s(&state, "mode")? != "clean"
            || s(&state, "closure")? != "bound"
            || !state["pending"].is_null()
        {
            return Err(invalid("bound generation has no completed clean lifecycle"));
        }
        let trust = read_bounded_with_limit(
            &private_file(&self.root.join(WITNESS_TRUST_FILE), false)?,
            MAX_TRUST_BYTES,
        )?;
        let anchor = TrustAnchor {
            sequence: n(&state, "trust_sequence")?,
            offset: n(&state, "trust_offset")?,
            sha256: s(&state, "trust_sha256")?.into(),
        };
        let mut history = verify(trust.as_bytes(), &anchor)?;
        let bound = history
            .generations
            .last()
            .ok_or_else(|| invalid("missing bound generation"))?
            .clone();
        if bound.seal_checksum.is_some() || bound.bind_sequence != anchor.sequence {
            return Err(invalid("active BIND is not the trust anchor"));
        }
        let config = history
            .config
            .as_ref()
            .ok_or_else(|| invalid("missing bound configuration"))?
            .clone();
        let first = trust
            .as_bytes()
            .split_inclusive(|b| *b == b'\n')
            .next()
            .ok_or_else(|| invalid("missing deployment policy"))?;
        let policy = crate::ddr_deployment_policy::DeploymentPolicy::from_initial_chain(first)?;
        let mut journal = self.open_journal()?;
        let head = journal.head().clone();
        let frames = journal.verify()?;
        if head.mode != Mode::Clean || head.broker != bound.broker {
            return Err(invalid("bound journal is not a clean broker checkpoint"));
        }
        // This first production seal supports only the currently hard-gated
        // no-work path. Do not infer holder coverage for any resource history.
        for generation in &history.generations {
            let final_sequence = generation
                .final_head
                .as_ref()
                .map_or(head.sequence, |h| h.sequence);
            let start = usize::try_from(generation.initial_head.sequence)
                .map_err(|_| invalid("generation input sequence overflow"))?;
            let end = usize::try_from(final_sequence)
                .map_err(|_| invalid("generation final sequence overflow"))?;
            if end
                != start
                    .checked_add(3)
                    .ok_or_else(|| invalid("generation sequence overflow"))?
                || frames.get(start..end).is_none_or(|slice| {
                    slice.iter().map(|frame| frame.kind).ne([
                        Kind::BrokerBirth,
                        Kind::BrokerDrained,
                        Kind::CleanCheckpoint,
                    ])
                })
            {
                return Err(invalid(
                    "resource-bearing history requires full holder coverage",
                ));
            }
        }
        let prior = TrustHistory {
            policy_digest: history.policy_digest.clone(),
            config: history.config.clone(),
            generations: history.generations[..history.generations.len() - 1].to_vec(),
        };
        if !prior.generations.is_empty() {
            crate::ddr_witness_evidence::verify_sealed_history(&policy, &prior)?;
        }
        let guard =
            crate::ddr_witness_evidence::verify_live_seal(&policy, &config, &bound, &head, parent)?;
        *history.generations.last_mut().unwrap() = guard.generation().clone();
        crate::ddr_recovery_durable::inspect_versioned(self.open_journal()?, &history)?;
        self.ensure_namespace()?;
        guard.recheck()?;
        let final_evidence = guard
            .generation()
            .evidence
            .as_ref()
            .unwrap()
            .final_head
            .as_ref()
            .unwrap();
        let birth = &bound.witness;
        let payload = json!({
            "generation": bound.generation, "bind_sequence": bound.bind_sequence,
            "bind_checksum": bound.bind_checksum,
            "witness_birth": {"pid": birth.pid, "uid": birth.uid, "gid": birth.gid,
                "pidns_dev": birth.pid_namespace_dev, "pidns_ino": birth.pid_namespace_ino,
                "starttime_ticks": birth.starttime_ticks},
            "evidence_final_head": final_evidence,
            "evidence_final_sha256": digest(final_evidence.as_bytes()),
            "broker_final_head": head.encode(),
        });
        let mut frame = json!({"schema": "uflow-ddr-trust-chain-v1",
            "sequence": anchor.sequence.checked_add(1).ok_or_else(|| invalid("trust sequence overflow"))?,
            "previous": bound.bind_checksum, "type": "SEAL", "payload": payload});
        frame["checksum"] = digest(&canonical(&frame)?).into();
        let mut encoded = canonical(&frame)?;
        encoded.push(b'\n');
        self.commit_prepared_trust(
            &bound.generation,
            &PreparedTrustAppend { frame: encoded },
            |_| guard.recheck(),
        )?;
        let result = self.inspect_sealed_evidence().and_then(|_| guard.recheck());
        if result.is_err() {
            self.admission_poisoned.set(true);
        }
        result
    }

    /// Produce and append an internal BIND from a real live-role guard and
    /// this already-locked fixed authority. The guard cannot prove a broker's
    /// execution is still held at its ready gate. This primitive deliberately
    /// leaves the result quarantined; it must not be exposed as a production
    /// launch until a trusted gate and lock handoff are actually verified.
    pub(crate) fn commit_live_binding(
        &mut self,
        transaction_id: &str,
        live: &crate::ddr_witness_evidence::LiveBindingGuard,
    ) -> io::Result<()> {
        use crate::ddr_recovery_journal::Mode;
        use crate::ddr_trust_history::TrustConfig;
        if !self.bootstrap.deployed || self.admission_poisoned.get() || !name_ok(transaction_id) {
            return Err(invalid("live binding writer unavailable"));
        }
        if let Err(error) = self.ensure_namespace() {
            self.admission_poisoned.set(true);
            return Err(error);
        }
        let state = self
            .deployed_state
            .as_ref()
            .ok_or_else(|| invalid("missing deployed state"))?;
        let state = parse_line(state.as_bytes())?;
        if s(&state, "mode")? != "clean" || !state["pending"].is_null() {
            return Err(invalid(
                "live binding requires clean state without pending transaction",
            ));
        }
        let trust_file = private_file(&self.root.join(WITNESS_TRUST_FILE), false)?;
        let trust = read_bounded_with_limit(&trust_file, MAX_TRUST_BYTES)?;
        let anchor = TrustAnchor {
            sequence: n(&state, "trust_sequence")?,
            offset: n(&state, "trust_offset")?,
            sha256: s(&state, "trust_sha256")?.to_owned(),
        };
        let history = verify(trust.as_bytes(), &anchor)?;
        let initial = trust
            .as_bytes()
            .split_inclusive(|byte| *byte == b'\n')
            .next()
            .ok_or_else(|| invalid("missing deployment policy"))?;
        let policy = crate::ddr_deployment_policy::DeploymentPolicy::from_initial_chain(initial)?;
        if live.policy_digest()? != history.policy_digest
            || policy.policy_digest()? != history.policy_digest
        {
            return Err(invalid(
                "live witness policy is not the pinned deployment policy",
            ));
        }
        let config = TrustConfig {
            host: self.bootstrap.host.clone(),
            boot: self.bootstrap.boot.clone(),
            domain: DOMAIN.into(),
            revision: self.bootstrap.revision,
            owner_limit: self.bootstrap.owner_limit,
            total_limit: self.bootstrap.total_limit,
            endpoint: self.bootstrap.endpoint.clone(),
            journal: (self.bootstrap.journal_dev, self.bootstrap.journal_ino),
            journal_head: (
                self.bootstrap.journal_head_dev,
                self.bootstrap.journal_head_ino,
            ),
        };
        if live.config() != &config {
            return Err(invalid(
                "live binding does not match fixed authority config",
            ));
        }
        let generation = live.generation();
        let evidence = generation
            .evidence
            .as_ref()
            .ok_or_else(|| invalid("missing live evidence"))?;
        let mut journal = self.open_journal()?;
        if journal.head().mode != Mode::Clean
            || journal.head().clean_sequence != journal.head().sequence
            || generation.initial_head != *journal.head()
        {
            return Err(invalid(
                "live binding requires the exact semantic-zero input head",
            ));
        }
        let predecessor = if history.generations.is_empty() {
            if s(&state, "closure")? != "closed" || !journal.verify()?.is_empty() {
                return Err(invalid(
                    "genesis binding requires the original empty journal",
                ));
            }
            "0".repeat(64)
        } else {
            if s(&state, "closure")? != "sealed" {
                return Err(invalid("previous generation is awaiting seal"));
            }
            crate::ddr_recovery_durable::inspect_versioned(journal, &history)?;
            crate::ddr_witness_evidence::verify_sealed_history(&policy, &history)?;
            history
                .generations
                .last()
                .and_then(|previous| previous.seal_checksum.clone())
                .ok_or_else(|| invalid("missing previous generation seal"))?
        };
        live.recheck()?;
        let last = trust
            .as_bytes()
            .split_inclusive(|byte| *byte == b'\n')
            .last()
            .ok_or_else(|| invalid("missing prior trust record"))?;
        let last = parse_line(last)?;
        let birth = |birth: &crate::ddr_process_identity::ProcessBirth| {
            json!({
                "pid": birth.pid, "uid": birth.uid, "gid": birth.gid,
                "pidns_dev": birth.pid_namespace_dev, "pidns_ino": birth.pid_namespace_ino,
                "starttime_ticks": birth.starttime_ticks,
            })
        };
        let payload = json!({
            "generation": generation.generation, "previous_seal": predecessor,
            "policy_sha256": history.policy_digest, "host": config.host, "boot": config.boot,
            "domain": config.domain, "revision": config.revision, "owner_limit": config.owner_limit,
            "total_limit": config.total_limit, "endpoint": config.endpoint,
            "initial_head": generation.initial_head.encode(), "journal": [config.journal.0, config.journal.1],
            "journal_head": [config.journal_head.0, config.journal_head.1], "broker": generation.broker,
            "broker_birth": birth(&generation.broker_birth), "witness_birth": birth(&generation.witness),
            "evidence_dir": evidence.directory, "evidence_dir_id": [evidence.directory_id.0, evidence.directory_id.1],
            "evidence_journal": [evidence.journal_id.0, evidence.journal_id.1],
            "evidence_head": [evidence.head_id.0, evidence.head_id.1],
            "evidence_initial_sha256": evidence.initial_head_sha256,
        });
        let mut frame = json!({"schema": "uflow-ddr-trust-chain-v1",
            "sequence": anchor.sequence.checked_add(1).ok_or_else(|| invalid("trust sequence overflow"))?,
            "previous": s(&last, "checksum")?, "type": "BIND", "payload": payload});
        frame["checksum"] = digest(&canonical(&frame)?).into();
        let mut encoded = canonical(&frame)?;
        encoded.push(b'\n');
        let prepared = PreparedTrustAppend { frame: encoded };
        // A failure after pending is persisted poisons the writer and leaves
        // the intermediate evidence untouched for a later operator to inspect.
        self.commit_prepared_trust(transaction_id, &prepared, |_| live.recheck())
    }

    fn ensure_transaction_namespace(
        &self,
        pending: &str,
        trust: &[u8],
        journal_head: &crate::ddr_recovery_journal::Head,
    ) -> io::Result<()> {
        self.ensure_static_namespace()?;
        let state = private_file(&self.root.join(STATE_FILE), false)?;
        let state_meta = state.metadata()?;
        let trust_file = private_file(&self.root.join(WITNESS_TRUST_FILE), false)?;
        let trust_meta = trust_file.metadata()?;
        if (state_meta.dev(), state_meta.ino())
            != (self.bootstrap.state_dev, self.bootstrap.state_ino)
            || read_bounded(&state)? != pending
            || (trust_meta.dev(), trust_meta.ino())
                != (
                    self.bootstrap.witness_trust_dev,
                    self.bootstrap.witness_trust_ino,
                )
            || read_bounded_with_limit(&trust_file, MAX_TRUST_BYTES)?.as_bytes() != trust
            || self.open_journal()?.head() != journal_head
        {
            return Err(invalid("pending trust transaction namespace changed"));
        }
        Ok(())
    }

    fn write_transaction_state(
        &self,
        text: &str,
        pending: bool,
        hook: &mut impl FnMut(Stage) -> io::Result<()>,
    ) -> io::Result<()> {
        let mut file = private_file(&self.root.join(STATE_FILE), true)?;
        let meta = file.metadata()?;
        if (meta.dev(), meta.ino()) != (self.bootstrap.state_dev, self.bootstrap.state_ino) {
            return Err(invalid("versioned state inode changed before transaction"));
        }
        file.set_len(0)?;
        hook(if pending {
            Stage::PendingTruncated
        } else {
            Stage::FinalTruncated
        })?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(text.as_bytes())?;
        hook(if pending {
            Stage::PendingWritten
        } else {
            Stage::FinalWritten
        })?;
        file.sync_all()?;
        hook(if pending {
            Stage::PendingSynced
        } else {
            Stage::FinalSynced
        })?;
        File::open(&self.root)?.sync_all()?;
        let readback = private_file(&self.root.join(STATE_FILE), false)?;
        let meta = readback.metadata()?;
        if (meta.dev(), meta.ino()) != (self.bootstrap.state_dev, self.bootstrap.state_ino)
            || read_bounded(&readback)? != text
        {
            return Err(invalid("versioned state transaction readback mismatch"));
        }
        Ok(())
    }

    /// A complete structural preflight happens before pending is persisted.
    /// A real live-role guard and semantic preflight are required for a BIND;
    /// SEAL still has no production constructor. Any new generation remains
    /// quarantined and therefore cannot authorize admission or recovery.
    fn commit_prepared_trust(
        &mut self,
        transaction_id: &str,
        prepared: &PreparedTrustAppend,
        mut hook: impl FnMut(Stage) -> io::Result<()>,
    ) -> io::Result<()> {
        if !self.bootstrap.deployed || self.admission_poisoned.get() {
            return Err(invalid("versioned trust writer unavailable"));
        }
        if let Err(error) = self.ensure_namespace() {
            self.admission_poisoned.set(true);
            return Err(error);
        }
        if !name_ok(transaction_id) {
            return Err(invalid("invalid trust transaction identity"));
        }
        let previous_state = self
            .deployed_state
            .as_ref()
            .ok_or_else(|| invalid("missing versioned state"))?
            .clone();
        let mut old_state = parse_line(previous_state.as_bytes())?;
        if !old_state["pending"].is_null() {
            return Err(invalid("another trust transaction remains pending"));
        }
        let journal = self.open_journal()?;
        let journal_head = journal.head().clone();
        let mut trust = private_file(&self.root.join(WITNESS_TRUST_FILE), true)?;
        let trust_meta = trust.metadata()?;
        if (trust_meta.dev(), trust_meta.ino())
            != (
                self.bootstrap.witness_trust_dev,
                self.bootstrap.witness_trust_ino,
            )
        {
            self.admission_poisoned.set(true);
            return Err(invalid("versioned trust inode changed"));
        }
        let old_trust = read_bounded_with_limit(&trust, MAX_TRUST_BYTES)?.into_bytes();
        let old_anchor = TrustAnchor {
            sequence: n(&old_state, "trust_sequence")?,
            offset: n(&old_state, "trust_offset")?,
            sha256: s(&old_state, "trust_sha256")?.to_owned(),
        };
        verify(&old_trust, &old_anchor)?;
        if !trust_has_capacity(old_anchor.sequence, old_trust.len(), prepared.frame.len()) {
            self.admission_poisoned.set(true); // stop admitting and drain, never erase history
            return Err(invalid("versioned trust capacity exhausted"));
        }
        let proposal = parse_line(&prepared.frame)?;
        let kind = s(&proposal, "type")?;
        if !matches!(kind, "BIND" | "SEAL") {
            return Err(invalid("unsupported trust transaction type"));
        }
        let mut next_trust = old_trust.clone();
        next_trust.extend_from_slice(&prepared.frame);
        let next_anchor = TrustAnchor {
            sequence: old_anchor
                .sequence
                .checked_add(1)
                .ok_or_else(|| invalid("trust sequence overflow"))?,
            offset: next_trust.len() as u64,
            sha256: digest(&next_trust),
        };
        let history = verify(&next_trust, &next_anchor)?;
        let generation = history
            .generations
            .last()
            .ok_or_else(|| invalid("trust transaction has no generation"))?;
        if (kind == "BIND" && generation.seal_checksum.is_some())
            || (kind == "SEAL" && generation.seal_checksum.is_none())
        {
            return Err(invalid(
                "trust transaction does not match generation closure",
            ));
        }
        let target_generation = generation.generation.clone();
        let target_closure = if kind == "BIND" { "bound" } else { "sealed" };
        let mut final_state = old_state.clone();
        final_state["trust_sequence"] = next_anchor.sequence.into();
        final_state["trust_offset"] = next_anchor.offset.into();
        final_state["trust_sha256"] = next_anchor.sha256.clone().into();
        final_state["generation"] = target_generation.clone().into();
        final_state["closure"] = target_closure.into();
        final_state["pending"] = Value::Null;
        let final_text = encode_state(&final_state)?;
        if !verify_deployment_state(&self.bootstrap, &next_trust, &final_text, &journal_head)? {
            return Err(invalid("trust transaction must remain quarantined"));
        }
        // Bind pending to the precise original and target trust heads, current
        // config and original resource head. A crash is never resolved by guess.
        old_state["pending"] = json!({"schema": "uflow-ddr-trust-pending-v1", "id": transaction_id,
            "type": kind, "input_sequence": old_anchor.sequence, "input_offset": old_anchor.offset,
            "input_sha256": old_anchor.sha256, "target_sequence": next_anchor.sequence,
            "target_offset": next_anchor.offset, "target_sha256": next_anchor.sha256,
            "input_state_sha256": digest(previous_state.as_bytes()),
            "input_journal_head": journal_head.encode(), "config_sha256": s(&final_state, "config_sha256")?,
            "target_generation": target_generation, "target_closure": target_closure});
        let pending_text = encode_state(&old_state)?;
        hook(Stage::BeforePending)?;
        let result = (|| -> io::Result<()> {
            self.write_transaction_state(&pending_text, true, &mut hook)?;
            self.ensure_transaction_namespace(&pending_text, &old_trust, &journal_head)?;
            if trust.metadata()?.len() != old_anchor.offset {
                return Err(invalid("trust or journal changed before append"));
            }
            trust.seek(SeekFrom::Start(old_anchor.offset))?;
            let split = prepared.frame.len() / 2;
            trust.write_all(&prepared.frame[..split])?;
            hook(Stage::TrustPartial)?;
            trust.write_all(&prepared.frame[split..])?;
            hook(Stage::TrustWritten)?;
            trust.sync_all()?;
            hook(Stage::TrustSynced)?;
            self.ensure_transaction_namespace(&pending_text, &next_trust, &journal_head)?;
            self.write_transaction_state(&final_text, false, &mut hook)?;
            hook(Stage::FinalReadback)?;
            let reread = private_file(&self.root.join(WITNESS_TRUST_FILE), false)?;
            let bytes = read_bounded_with_limit(&reread, MAX_TRUST_BYTES)?;
            let state = read_bounded(&private_file(&self.root.join(STATE_FILE), false)?)?;
            if bytes.as_bytes() != next_trust
                || state != final_text
                || !verify_deployment_state(
                    &self.bootstrap,
                    bytes.as_bytes(),
                    &state,
                    self.open_journal()?.head(),
                )?
            {
                return Err(invalid("trust transaction final readback mismatch"));
            }
            self.deployed_state = Some(final_text);
            self.quarantined = true;
            self.dirty = true;
            self.ensure_namespace()?;
            Ok(())
        })();
        if result.is_err() {
            self.admission_poisoned.set(true);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddr_recovery_coverage::{canonical, digest, parse_line, s};

    const ENDPOINT: &str = "/workspace/uflow/runs/t_855f763c-r49/a.sock";

    fn setup() -> (PathBuf, ManagedAuthority, PreparedTrustAppend) {
        let (root, _, policy) = super::super::tests::deployment_fixture();
        super::super::tests::initialize_deployed_fixture(&root, &policy);
        // Another test may fork before the initializer's CLOEXEC lock closes.
        // Wait only in this fixture; production and contention probes remain
        // nonblocking and still acquire a fresh lock description.
        let authority = super::super::after_test_lock_release(|| {
            ManagedAuthority::open_at(&root, "host", "boot", ENDPOINT, 16, 64)
        })
        .unwrap();
        let raw = fs::read(root.join(WITNESS_TRUST_FILE)).unwrap();
        let deploy = parse_line(&raw).unwrap();
        let birth = |pid: u64| {
            json!({"pid": pid, "uid": 1, "gid": 1,
            "pidns_dev": 1, "pidns_ino": 2, "starttime_ticks": pid + 10})
        };
        let payload = json!({"generation": "g1", "previous_seal": "0".repeat(64),
            "policy_sha256": policy.policy_digest().unwrap(), "host": "host", "boot": "boot", "domain": DOMAIN,
            "revision": 1, "owner_limit": 16, "total_limit": 64, "endpoint": ENDPOINT,
            "initial_head": crate::ddr_recovery_journal::AnchoredJournal::genesis_state(),
            "journal": [authority.bootstrap.journal_dev, authority.bootstrap.journal_ino],
            "journal_head": [authority.bootstrap.journal_head_dev, authority.bootstrap.journal_head_ino],
            "broker": "b1", "broker_birth": birth(100), "witness_birth": birth(101),
            "evidence_dir": "/workspace/uflow/runs/synthetic/evidence", "evidence_dir_id": [999999, 1],
            "evidence_journal": [999999, 2], "evidence_head": [999999, 3], "evidence_initial_sha256": "1".repeat(64)});
        let mut frame = json!({"schema": "uflow-ddr-trust-chain-v1", "sequence": 2,
            "previous": s(&deploy, "checksum").unwrap(), "type": "BIND", "payload": payload});
        frame["checksum"] = digest(&canonical(&frame).unwrap()).into();
        let mut bytes = canonical(&frame).unwrap();
        bytes.push(b'\n');
        (root, authority, PreparedTrustAppend { frame: bytes })
    }

    fn bytes(root: &Path) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        (
            fs::read(root.join(STATE_FILE)).unwrap(),
            fs::read(root.join(WITNESS_TRUST_FILE)).unwrap(),
            fs::read(root.join(JOURNAL_HEAD_FILE)).unwrap(),
        )
    }

    #[test]
    fn trust_capacity_stops_at_frame_and_byte_limits_without_overflow() {
        let frames = crate::ddr_trust_history::MAX_FRAMES as u64;
        assert!(trust_has_capacity(
            frames - 1,
            MAX_TRUST_BYTES as usize - 1,
            1
        ));
        assert!(!trust_has_capacity(frames, 1, 1));
        assert!(!trust_has_capacity(frames - 1, MAX_TRUST_BYTES as usize, 1));
        assert!(!trust_has_capacity(1, usize::MAX, 1));
    }

    #[test]
    fn synthetic_bound_transaction_syncs_anchor_but_never_authorizes() {
        let (root, mut authority, prepared) = setup();
        let before = bytes(&root);
        let trust_inode = fs::metadata(root.join(WITNESS_TRUST_FILE)).unwrap().ino();
        let state_inode = fs::metadata(root.join(STATE_FILE)).unwrap().ino();
        authority
            .commit_prepared_trust("tx1", &prepared, |_| Ok(()))
            .unwrap();
        let after = bytes(&root);
        assert!(after.1.starts_with(&before.1));
        assert_eq!(after.2, before.2);
        assert_eq!(
            fs::metadata(root.join(WITNESS_TRUST_FILE)).unwrap().ino(),
            trust_inode
        );
        assert_eq!(
            fs::metadata(root.join(STATE_FILE)).unwrap().ino(),
            state_inode
        );
        let state = parse_line(&after.0).unwrap();
        assert!(state["pending"].is_null());
        assert_eq!(s(&state, "closure").unwrap(), "bound");
        assert!(authority.is_inherited_quarantine());
        assert!(authority.before_admission().is_err());
        let contender = private_file(&root.join(LOCK_FILE), true).unwrap();
        assert_ne!(
            unsafe { libc::flock(contender.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0
        );
        drop(contender);
        drop(authority);
        let reopened = super::super::after_test_lock_release(|| {
            ManagedAuthority::open_at(&root, "host", "boot", ENDPOINT, 16, 64)
        })
        .unwrap();
        assert!(reopened.is_inherited_quarantine());
        assert!(!reopened.admits_new_work());
        assert_eq!(bytes(&root), after);
    }

    #[test]
    fn trust_transaction_fault_windows_keep_quarantine_and_evidence() {
        for stop in [
            Stage::BeforePending,
            Stage::PendingTruncated,
            Stage::PendingWritten,
            Stage::PendingSynced,
            Stage::TrustPartial,
            Stage::TrustWritten,
            Stage::TrustSynced,
            Stage::FinalTruncated,
            Stage::FinalWritten,
            Stage::FinalSynced,
            Stage::FinalReadback,
        ] {
            let (root, mut authority, prepared) = setup();
            let before = bytes(&root);
            let mut hit = false;
            let result = authority.commit_prepared_trust("tx1", &prepared, |stage| {
                if stage == stop {
                    hit = true;
                    Err(io::Error::other("injected trust transaction fault"))
                } else {
                    Ok(())
                }
            });
            assert!(result.is_err() && hit, "stage={stop:?}");
            let after = bytes(&root);
            assert_eq!(after.2, before.2, "resource head changed at {stop:?}");
            if stop == Stage::BeforePending {
                assert_eq!(after, before);
                assert!(!authority.admission_poisoned.get());
            } else {
                assert!(authority.admission_poisoned.get());
                assert!(authority.before_admission().is_err());
            }
            drop(authority);
            let reopened = super::super::after_test_lock_release(|| {
                ManagedAuthority::open_at(&root, "host", "boot", ENDPOINT, 16, 64)
            });
            if stop == Stage::BeforePending {
                assert!(!reopened.unwrap().is_inherited_quarantine());
            } else if let Ok(reopened) = reopened {
                // A fully written final state can be read after a reported
                // failure, but never grants admission or silently repairs it.
                assert!(reopened.is_inherited_quarantine(), "stage={stop:?}");
                assert!(!reopened.admits_new_work());
            }
            assert_eq!(bytes(&root), after, "reopen changed evidence at {stop:?}");
        }
    }

    #[test]
    fn bad_proposal_is_rejected_before_pending_without_writes() {
        let (root, mut authority, prepared) = setup();
        let before = bytes(&root);
        let mut frame = parse_line(&prepared.frame).unwrap();
        frame["payload"]["host"] = "wrong-host".into();
        frame.as_object_mut().unwrap().remove("checksum");
        frame["checksum"] = digest(&canonical(&frame).unwrap()).into();
        let mut bad = canonical(&frame).unwrap();
        bad.push(b'\n');
        assert!(authority
            .commit_prepared_trust("tx1", &PreparedTrustAppend { frame: bad }, |_| Ok(()))
            .is_err());
        assert!(!authority.admission_poisoned.get());
        assert_eq!(bytes(&root), before);
    }

    #[test]
    fn pending_transaction_rechecks_pinned_files_before_append() {
        for file in [
            MANIFEST_FILE,
            STATE_FILE,
            WITNESS_TRUST_FILE,
            JOURNAL_HEAD_FILE,
        ] {
            let (root, mut authority, prepared) = setup();
            let original_trust_len = fs::metadata(root.join(WITNESS_TRUST_FILE)).unwrap().len();
            let mut changed = false;
            let result = authority.commit_prepared_trust("tx1", &prepared, |stage| {
                if stage == Stage::PendingSynced {
                    let path = root.join(file);
                    let mut data = fs::read(&path)?;
                    data[0] ^= 1; // same inode and size; do not simulate replacement
                    fs::write(path, data)?;
                    changed = true;
                }
                Ok(())
            });
            assert!(changed && result.is_err(), "file={file}");
            assert!(authority.admission_poisoned.get());
            assert_eq!(
                fs::metadata(root.join(WITNESS_TRUST_FILE)).unwrap().len(),
                original_trust_len
            );
            let saved = bytes(&root);
            let manifest = fs::read(root.join(MANIFEST_FILE)).unwrap();
            drop(authority);
            assert!(
                super::super::after_test_lock_release(|| ManagedAuthority::open_at(
                    &root, "host", "boot", ENDPOINT, 16, 64
                ))
                .is_err()
            );
            assert_eq!(
                bytes(&root),
                saved,
                "reopen changed data after {file} mutation"
            );
            assert_eq!(fs::read(root.join(MANIFEST_FILE)).unwrap(), manifest);
        }
    }
}
