//! Read-only structural validation of versioned witness history.
//!
//! This is deliberately NOT an authorization capability. It does not inspect
//! live processes, verify evidence contents, replay resource obligations, or
//! commit a BIND/SEAL. Production still accepts only the initial DEPLOY frame.
//! The anchor must come from independently persisted authority state, never be
//! derived from the bytes being checked. Audit-backed revision transitions and
//! dirty-to-recovery transitions are intentionally rejected until wired.
#![allow(dead_code)]

use crate::ddr_deployment_policy::DeploymentPolicy;
use crate::ddr_process_identity::ProcessBirth;
use crate::ddr_recovery_coverage::{
    birth, birth_key, canonical, digest, ensure, fields, identity, invalid, n, parse_line, s, token,
};
use crate::ddr_recovery_journal::{AnchoredJournal, Head, Mode};
use serde_json::Value;
use std::collections::BTreeSet;
use std::io;

const SCHEMA: &str = "uflow-ddr-trust-chain-v1";
const ZERO: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const MAX_CHAIN: usize = 16 * 1024 * 1024;
const MAX_FRAME: usize = 128 * 1024;
pub(crate) const MAX_FRAMES: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrustAnchor {
    pub sequence: u64,
    pub offset: u64,
    pub sha256: String,
}

/// Exact evidence identities retained from the immutable BIND and SEAL. These
/// values only identify bytes to inspect; they are not themselves a proof of
/// an actual witness, normal wait, a semantic-zero checkpoint or recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EvidenceBinding {
    pub directory: String,
    pub directory_id: (u64, u64),
    pub journal_id: (u64, u64),
    pub head_id: (u64, u64),
    pub initial_head_sha256: String,
    pub final_head: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Generation {
    pub generation: String,
    pub broker: String,
    pub broker_birth: ProcessBirth,
    pub witness: ProcessBirth,
    pub bind_sequence: u64,
    pub bind_checksum: String,
    pub initial_head: Head,
    pub seal_checksum: Option<String>,
    pub final_head: Option<Head>,
    // None exists only in older synthetic inspection tests. A generation
    // parsed from the independently anchored trust chain always contains it.
    pub evidence: Option<EvidenceBinding>,
}

#[derive(Debug)]
pub(crate) struct TrustHistory {
    pub policy_digest: String,
    pub generations: Vec<Generation>,
    pub config: Option<TrustConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrustConfig {
    pub host: String,
    pub boot: String,
    pub domain: String,
    pub revision: u64,
    pub owner_limit: u64,
    pub total_limit: u64,
    pub endpoint: String,
    pub journal: (u64, u64),
    pub journal_head: (u64, u64),
}

fn hash_ok(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn valid_endpoint(value: &str) -> bool {
    value.starts_with("/workspace/uflow/runs/")
        && value.len() + ".external-ddr".len() < 108
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"/._-".contains(&b))
        && !value.split('/').any(|part| part == "." || part == "..")
}

fn valid_evidence_dir(value: &str) -> bool {
    value.starts_with("/workspace/uflow/runs/")
        && value.len() <= 1024
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"/._-".contains(&b))
        && !value
            .split('/')
            .skip(1)
            .any(|part| part.is_empty() || part == "." || part == "..")
}

fn pinned_id(value: &Value, key: &str) -> io::Result<(u64, u64)> {
    let id = identity(value, key)?;
    ensure(id.1 > 0, "invalid pinned history inode")?;
    Ok(id)
}

fn evidence_head(text: &str) -> io::Result<()> {
    let value = parse_line(text.as_bytes())?;
    fields(&value, &["schema", "seq", "offset", "checksum"])?;
    ensure(
        s(&value, "schema")? == "uflow-ddr-evidence-head-v1"
            && n(&value, "seq")? > 0
            && n(&value, "offset")? > 0
            && hash_ok(s(&value, "checksum")?),
        "invalid sealed evidence head",
    )
}

/// Validate the complete chain against an independently supplied anchor.
/// Inputs are syntactic/identity checks only, not proof of actual births,
/// witness waits, resource drain, clean semantics, or durable transaction.
pub(crate) fn verify(raw: &[u8], anchor: &TrustAnchor) -> io::Result<TrustHistory> {
    ensure(
        !raw.is_empty()
            && raw.len() <= MAX_CHAIN
            && raw.len() as u64 == anchor.offset
            && hash_ok(&anchor.sha256)
            && digest(raw) == anchor.sha256,
        "trust bytes do not match independent anchor",
    )?;
    let mut lines = raw.split_inclusive(|b| *b == b'\n');
    let first = lines
        .next()
        .ok_or_else(|| invalid("missing deployment frame"))?;
    ensure(first.len() <= MAX_FRAME, "deployment frame too large")?;
    let policy = DeploymentPolicy::from_initial_chain(first)?;
    let policy_digest = policy.policy_digest()?;
    let deploy = parse_line(first)?;
    let mut previous = s(&deploy, "checksum")?.to_owned();
    let mut sequence = 1u64;
    let mut predecessor_seal = ZERO.to_string();
    let mut generation_names = BTreeSet::new();
    let mut brokers = BTreeSet::new();
    let mut births = BTreeSet::new();
    let mut evidence_directories = BTreeSet::new();
    let mut evidence_paths = BTreeSet::new();
    let mut evidence_files = BTreeSet::new();
    let mut generation_config: Option<TrustConfig> = None;
    let mut generations: Vec<Generation> = Vec::new();

    for line in lines {
        ensure(
            line.len() <= MAX_FRAME && sequence < MAX_FRAMES as u64,
            "trust history capacity exceeded",
        )?;
        let mut frame = parse_line(line)?;
        fields(
            &frame,
            &[
                "schema", "sequence", "previous", "type", "payload", "checksum",
            ],
        )?;
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| invalid("trust sequence overflow"))?;
        ensure(
            s(&frame, "schema")? == SCHEMA
                && n(&frame, "sequence")? == sequence
                && s(&frame, "previous")? == previous,
            "trust history sequence or previous checksum mismatch",
        )?;
        let checksum = s(&frame, "checksum")?.to_owned();
        let kind = s(&frame, "type")?.to_owned();
        let payload = frame["payload"].clone();
        frame.as_object_mut().unwrap().remove("checksum");
        ensure(
            hash_ok(&checksum) && digest(&canonical(&frame)?) == checksum,
            "trust frame checksum mismatch",
        )?;
        match kind.as_str() {
            "BIND" => {
                fields(
                    &payload,
                    &[
                        "generation",
                        "previous_seal",
                        "policy_sha256",
                        "host",
                        "boot",
                        "domain",
                        "revision",
                        "owner_limit",
                        "total_limit",
                        "endpoint",
                        "initial_head",
                        "journal",
                        "journal_head",
                        "broker",
                        "broker_birth",
                        "witness_birth",
                        "evidence_dir",
                        "evidence_dir_id",
                        "evidence_journal",
                        "evidence_head",
                        "evidence_initial_sha256",
                    ],
                )?;
                ensure(
                    generations.last().is_none_or(|g| g.seal_checksum.is_some()),
                    "previous generation is not sealed",
                )?;
                let generation = s(&payload, "generation")?;
                let broker = s(&payload, "broker")?;
                let host = s(&payload, "host")?;
                let boot = s(&payload, "boot")?;
                let domain = s(&payload, "domain")?;
                let endpoint = s(&payload, "endpoint")?;
                let revision = n(&payload, "revision")?;
                let owner = n(&payload, "owner_limit")?;
                let total = n(&payload, "total_limit")?;
                ensure(
                    token(generation)
                        && token(broker)
                        && token(host)
                        && token(boot)
                        && domain == "external-ddr-v0"
                        && revision > 0
                        && owner > 0
                        && total >= owner
                        && valid_endpoint(endpoint)
                        && s(&payload, "policy_sha256")? == policy_digest
                        && s(&payload, "previous_seal")? == predecessor_seal
                        && generation_names.insert(generation.to_owned())
                        && brokers.insert(broker.to_owned()),
                    "invalid or reused generation binding",
                )?;
                let journal = pinned_id(&payload, "journal")?;
                let journal_head = pinned_id(&payload, "journal_head")?;
                let dir = pinned_id(&payload, "evidence_dir_id")?;
                let evidence = pinned_id(&payload, "evidence_journal")?;
                let evidence_head = pinned_id(&payload, "evidence_head")?;
                let evidence_path = s(&payload, "evidence_dir")?;
                ensure(
                    valid_evidence_dir(evidence_path)
                        && evidence_paths.insert(evidence_path.to_owned())
                        && hash_ok(s(&payload, "evidence_initial_sha256")?)
                        && evidence != journal
                        && evidence != journal_head
                        && evidence_head != journal
                        && evidence_head != journal_head
                        && evidence_directories.insert(dir)
                        && evidence_files.insert(evidence)
                        && evidence_files.insert(evidence_head),
                    "invalid or reused evidence binding",
                )?;
                let broker_birth = birth(&payload["broker_birth"])?;
                let witness = birth(&payload["witness_birth"])?;
                ensure(
                    births.insert(birth_key(&broker_birth)) && births.insert(birth_key(&witness)),
                    "broker or witness birth reused",
                )?;
                let initial_head = Head::parse(s(&payload, "initial_head")?)?;
                ensure(
                    initial_head.mode == Mode::Clean,
                    "generation does not start at a clean head",
                )?;
                if let Some(last) = generations.last() {
                    let prior = last
                        .final_head
                        .as_ref()
                        .ok_or_else(|| invalid("previous seal has no broker head"))?;
                    ensure(
                        prior.mode == Mode::Clean && *prior == initial_head,
                        "recovery or changed clean head needs semantic proof",
                    )?;
                } else {
                    ensure(
                        initial_head.encode() == AnchoredJournal::genesis_state(),
                        "first generation must start at genesis",
                    )?;
                }
                let config = TrustConfig {
                    host: host.to_owned(),
                    boot: boot.to_owned(),
                    domain: domain.to_owned(),
                    revision,
                    owner_limit: owner,
                    total_limit: total,
                    endpoint: endpoint.to_owned(),
                    journal,
                    journal_head,
                };
                if let Some(previous_config) = &generation_config {
                    ensure(
                        previous_config == &config,
                        "configuration transition needs audited proof",
                    )?;
                } else {
                    generation_config = Some(config);
                }
                generations.push(Generation {
                    generation: generation.to_owned(),
                    broker: broker.to_owned(),
                    broker_birth,
                    witness,
                    bind_sequence: sequence,
                    bind_checksum: checksum.clone(),
                    initial_head,
                    seal_checksum: None,
                    final_head: None,
                    evidence: Some(EvidenceBinding {
                        directory: evidence_path.to_owned(),
                        directory_id: dir,
                        journal_id: evidence,
                        head_id: evidence_head,
                        initial_head_sha256: s(&payload, "evidence_initial_sha256")?.to_owned(),
                        final_head: None,
                    }),
                });
            }
            "SEAL" => {
                fields(
                    &payload,
                    &[
                        "generation",
                        "bind_sequence",
                        "bind_checksum",
                        "witness_birth",
                        "evidence_final_head",
                        "evidence_final_sha256",
                        "broker_final_head",
                    ],
                )?;
                let active = generations
                    .last_mut()
                    .ok_or_else(|| invalid("seal without bind"))?;
                let final_evidence = s(&payload, "evidence_final_head")?;
                evidence_head(final_evidence)?;
                let final_head = Head::parse(s(&payload, "broker_final_head")?)?;
                ensure(
                    active.seal_checksum.is_none()
                        && s(&payload, "generation")? == active.generation
                        && n(&payload, "bind_sequence")? == active.bind_sequence
                        && s(&payload, "bind_checksum")? == active.bind_checksum
                        && birth(&payload["witness_birth"])? == active.witness
                        && s(&payload, "evidence_final_sha256")?
                            == digest(final_evidence.as_bytes())
                        && final_head.broker == active.broker
                        && final_head.sequence > active.initial_head.sequence,
                    "seal does not match original generation binding",
                )?;
                active.seal_checksum = Some(checksum.clone());
                active.final_head = Some(final_head);
                active
                    .evidence
                    .as_mut()
                    .ok_or_else(|| invalid("seal without evidence binding"))?
                    .final_head = Some(final_evidence.to_owned());
                predecessor_seal = checksum.clone();
            }
            _ => return Err(invalid("unknown trust record type")),
        }
        previous = checksum;
    }
    ensure(
        sequence == anchor.sequence,
        "trust anchor sequence mismatch",
    )?;
    Ok(TrustHistory {
        policy_digest,
        generations,
        config: generation_config,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    // These are deliberately synthetic codec inputs, not process/witness or
    // journal evidence; only the production bootstrap probe exercises a real
    // binary and real lock. No synthetic input is able to authorize work.
    fn genesis() -> (Vec<u8>, String) {
        let target = std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target"));
        let base = fs::canonicalize(target).unwrap();
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = base.join(format!("trust-history-{}-{nonce}", std::process::id()));
        fs::create_dir(&dir).unwrap();
        let artifact = dir.join("synthetic-artifact");
        fs::write(&artifact, b"codec fixture only\n").unwrap();
        let file = json!({"path": artifact.to_str().unwrap(), "sha256": digest(&fs::read(&artifact).unwrap())});
        let roles: Vec<_> = [
            "supervisor",
            "launcher",
            "interpreter",
            "broker",
            "owner",
            "accessor",
        ]
        .iter()
        .map(|role| {
            json!({"role": role, "executable": file, "entrypoint": file,
                "argv": [artifact.to_str().unwrap()], "environment": {}})
        })
        .collect();
        let value = json!({"schema": "uflow-ddr-deployment-policy-v1", "roles": roles});
        let mut encoded = canonical(&value).unwrap();
        encoded.push(b'\n');
        let path = dir.join("policy");
        fs::write(&path, encoded).unwrap();
        let policy = DeploymentPolicy::load(&path).unwrap();
        (
            policy.initial_chain().unwrap(),
            policy.policy_digest().unwrap(),
        )
    }

    fn anchor(raw: &[u8]) -> TrustAnchor {
        TrustAnchor {
            sequence: raw.iter().filter(|b| **b == b'\n').count() as u64,
            offset: raw.len() as u64,
            sha256: digest(raw),
        }
    }

    fn append(raw: &mut Vec<u8>, kind: &str, payload: Value) -> (u64, String) {
        let last = raw.split_inclusive(|b| *b == b'\n').last().unwrap();
        let last = parse_line(last).unwrap();
        let sequence = n(&last, "sequence").unwrap() + 1;
        let previous = s(&last, "checksum").unwrap();
        let mut frame = json!({"schema": SCHEMA, "sequence": sequence, "previous": previous,
            "type": kind, "payload": payload});
        let checksum = digest(&canonical(&frame).unwrap());
        frame["checksum"] = checksum.clone().into();
        raw.extend(canonical(&frame).unwrap());
        raw.push(b'\n');
        (sequence, checksum)
    }

    fn birth_value(pid: u64) -> Value {
        json!({"pid": pid, "uid": 10, "gid": 10, "pidns_dev": 1, "pidns_ino": 2, "starttime_ticks": pid + 100})
    }

    fn bind(
        generation: &str,
        broker: &str,
        witness_pid: u64,
        broker_pid: u64,
        previous_seal: &str,
        policy: &str,
        head: &str,
    ) -> Value {
        json!({"generation": generation, "previous_seal": previous_seal, "policy_sha256": policy,
            "host": "host", "boot": "boot", "domain": "external-ddr-v0", "revision": 1,
            "owner_limit": 16, "total_limit": 64, "endpoint": "/workspace/uflow/runs/x/a.sock",
            "initial_head": head, "journal": [1, 2], "journal_head": [1, 3],
            "broker": broker, "broker_birth": birth_value(broker_pid), "witness_birth": birth_value(witness_pid),
            "evidence_dir": format!("/workspace/uflow/runs/x/evidence-{witness_pid}"), "evidence_dir_id": [1, 100 + witness_pid],
            "evidence_journal": [1, 200 + witness_pid], "evidence_head": [1, 300 + witness_pid], "evidence_initial_sha256": "1".repeat(64)})
    }

    fn seal(
        generation: &str,
        broker: &str,
        witness_pid: u64,
        bind_seq: u64,
        bind_hash: &str,
        seq: u64,
    ) -> Value {
        let mut evidence = canonical(&json!({"schema": "uflow-ddr-evidence-head-v1", "seq": 1,
            "offset": 40, "checksum": "2".repeat(64)}))
        .unwrap();
        evidence.push(b'\n');
        let evidence = String::from_utf8(evidence).unwrap();
        let head = Head {
            mode: Mode::Clean,
            broker: broker.into(),
            sequence: seq,
            offset: seq * 64,
            checksum: seq,
            clean_sequence: seq,
        }
        .encode();
        json!({"generation": generation, "bind_sequence": bind_seq, "bind_checksum": bind_hash,
            "witness_birth": birth_value(witness_pid), "evidence_final_sha256": digest(evidence.as_bytes()),
            "evidence_final_head": evidence, "broker_final_head": head})
    }

    #[test]
    fn synthetic_two_generation_history_retains_prior_frames_and_anchor_detects_loss() {
        let (mut raw, policy) = genesis();
        let (seq1, hash1) = append(
            &mut raw,
            "BIND",
            bind(
                "g1",
                "b1",
                10,
                11,
                ZERO,
                &policy,
                &AnchoredJournal::genesis_state(),
            ),
        );
        let end1 = seal("g1", "b1", 10, seq1, &hash1, 1);
        let head1 = end1["broker_final_head"].as_str().unwrap().to_owned();
        let (_, seal1) = append(&mut raw, "SEAL", end1);
        let (seq2, hash2) = append(
            &mut raw,
            "BIND",
            bind("g2", "b2", 20, 21, &seal1, &policy, &head1),
        );
        append(&mut raw, "SEAL", seal("g2", "b2", 20, seq2, &hash2, 2));
        let expected = anchor(&raw);
        let history = verify(&raw, &expected).unwrap();
        assert_eq!(history.generations.len(), 2);
        assert_eq!(history.generations[0].bind_checksum, hash1);
        assert_eq!(
            history.generations[0].seal_checksum.as_deref(),
            Some(seal1.as_str())
        );
        assert_eq!(history.generations[1].initial_head.encode(), head1);
        assert!(history.generations[1].seal_checksum.is_some());
        let split = raw.iter().rposition(|b| *b == b'\n').unwrap();
        let earlier = raw[..split].iter().rposition(|b| *b == b'\n').unwrap() + 1;
        assert!(verify(&raw[..earlier], &expected).is_err()); // whole final frame lost
        let mut changed = raw.clone();
        let pos = changed.windows(2).position(|w| w == b"g1").unwrap();
        changed[pos + 1] = b'9';
        assert!(verify(&changed, &expected).is_err());
        assert!(verify(&changed, &anchor(&changed)).is_err()); // reanchoring cannot repair frame chain
    }

    #[test]
    fn synthetic_history_rejects_reuse_unsealed_revision_and_wrong_seal() {
        let (start, policy) = genesis();
        let bind1 = bind(
            "g1",
            "b1",
            10,
            11,
            ZERO,
            &policy,
            &AnchoredJournal::genesis_state(),
        );
        let mut unsealed = start.clone();
        append(&mut unsealed, "BIND", bind1.clone());
        append(
            &mut unsealed,
            "BIND",
            bind(
                "g2",
                "b2",
                20,
                21,
                ZERO,
                &policy,
                &AnchoredJournal::genesis_state(),
            ),
        );
        assert!(verify(&unsealed, &anchor(&unsealed)).is_err());

        let mut base = start;
        let (seq, checksum) = append(&mut base, "BIND", bind1);
        let mut wrong = seal("g1", "b1", 10, seq, &checksum, 1);
        wrong["witness_birth"] = birth_value(30);
        let mut wrong_history = base.clone();
        append(&mut wrong_history, "SEAL", wrong);
        assert!(verify(&wrong_history, &anchor(&wrong_history)).is_err());
        let end = seal("g1", "b1", 10, seq, &checksum, 1);
        let clean = end["broker_final_head"].as_str().unwrap().to_owned();
        let (_, seal1) = append(&mut base, "SEAL", end);
        for fault in [
            "generation",
            "broker",
            "birth",
            "previous",
            "revision",
            "cap",
            "head",
            "evidence-path",
            "evidence-inode",
        ] {
            let mut candidate = base.clone();
            let mut next = bind("g2", "b2", 20, 21, &seal1, &policy, &clean);
            match fault {
                "generation" => next["generation"] = "g1".into(),
                "broker" => next["broker"] = "b1".into(),
                "birth" => next["witness_birth"] = birth_value(10),
                "previous" => next["previous_seal"] = ZERO.into(),
                "revision" => next["revision"] = 2.into(),
                "cap" => next["total_limit"] = 65.into(),
                "head" => next["initial_head"] = AnchoredJournal::genesis_state().into(),
                "evidence-path" => {
                    next["evidence_dir"] = "/workspace/uflow/runs/x/evidence-10".into()
                }
                "evidence-inode" => next["evidence_journal"] = json!([1, 210]),
                _ => unreachable!(),
            }
            append(&mut candidate, "BIND", next);
            assert!(
                verify(&candidate, &anchor(&candidate)).is_err(),
                "fault={fault}"
            );
        }
    }
}
