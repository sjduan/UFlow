//! Read-only verification of evidence referenced by versioned trust history.
//!
//! The trust anchor, policy and every BIND/SEAL must first be read under the
//! original authority lock. This module cannot install a frame, admit work,
//! recover quota or create a semantic-zero proof. It validates a witness's
//! complete pinned stream and real-wait records against the deployment policy;
//! a separate live preflight and journal/holder coverage are still required.
#![allow(dead_code)]

use crate::ddr_deployment_policy::{DeploymentPolicy, RoleContext};
use crate::ddr_process_identity::ProcessBirth;
use crate::ddr_recovery_coverage::{
    birth, birth_key, canonical, digest, ensure, fields, invalid, n, parse_line, private_file, s,
    token, MAX_WITNESS,
};
use crate::ddr_trust_history::{Generation, TrustConfig, TrustHistory};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

const HEAD_SCHEMA: &str = "uflow-ddr-evidence-head-v1";
// v2 adds the broker identity to the witness's pinned start frame. Older
// streams cannot prove this binding and must remain fail-closed.
const FRAME_SCHEMA: &str = "uflow-ddr-witness-v2";
const ZERO: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const MAX_FRAME: usize = 8192;

#[derive(Clone)]
struct Child {
    birth: ProcessBirth,
    role: String,
    launch: String,
    waited: bool,
    resources: BTreeSet<(u64, u64)>,
}

/// These are structural/pinned-writer results, not a recovery capability.
/// The individual records must still be reconciled with the resource journal.
pub(crate) struct VerifiedRoster {
    pub generation: String,
    pub witness: ProcessBirth,
    pub normal_exits: Vec<ProcessBirth>,
    pub events: Vec<Value>,
    pub final_head_sha256: String,
}

fn check_dir(path: &Path, identity: (u64, u64)) -> io::Result<()> {
    ensure(
        path.is_absolute()
            && path.starts_with("/workspace/uflow/runs/")
            && fs::canonicalize(path)? == path,
        "invalid bound evidence directory",
    )?;
    let meta = fs::symlink_metadata(path)?;
    ensure(
        meta.is_dir()
            && meta.uid() == unsafe { libc::geteuid() }
            && meta.mode() & 0o077 == 0
            && (meta.dev(), meta.ino()) == identity,
        "bound evidence directory identity changed",
    )
}

fn head_bytes(sequence: u64, offset: usize, checksum: &str) -> io::Result<Vec<u8>> {
    let mut result = canonical(&json!({
        "schema": HEAD_SCHEMA,
        "seq": sequence,
        "offset": offset,
        "checksum": checksum,
    }))?;
    result.push(b'\n');
    Ok(result)
}

/// Held pidfds and an exact initial evidence snapshot for a live pair. It
/// does NOT prove the broker is still held at a pre-admission ready gate.
/// This guard is only one precondition for BIND; it does not hold the
/// authority lock, prove semantic zero or grant admission. The deployment
/// transaction must recheck it while holding the original authority lock.
pub(crate) struct LiveBindingGuard {
    policy: DeploymentPolicy,
    config: TrustConfig,
    generation: Generation,
    witness_fd: File,
    broker_fd: File,
    head: Vec<u8>,
    journal: Vec<u8>,
}

/// A sealed evidence snapshot belonging to the original still-live pinned
/// witness. This needs a separate semantic journal proof and never grants
/// admission or recovery.
pub(crate) struct LiveSealGuard {
    policy: DeploymentPolicy,
    config: TrustConfig,
    generation: Generation,
    witness_fd: File,
    journal: Vec<u8>,
    head: Vec<u8>,
}

impl LiveSealGuard {
    pub(crate) fn generation(&self) -> &Generation {
        &self.generation
    }
    pub(crate) fn recheck(&self) -> io::Result<()> {
        let binding = self
            .generation
            .evidence
            .as_ref()
            .ok_or_else(|| invalid("missing seal binding"))?;
        let directory = Path::new(&binding.directory);
        check_dir(directory, binding.directory_id)?;
        ensure(
            private_file(&directory.join("witness.head"), binding.head_id, 4096)? == self.head
                && private_file(
                    &directory.join("witness.journal"),
                    binding.journal_id,
                    MAX_WITNESS,
                )? == self.journal,
            "sealed witness evidence changed during transaction",
        )?;
        crate::ddr_witness_active::ensure_live(&self.witness_fd)?;
        let context = RoleContext {
            generation: &self.generation.generation,
            broker: &self.generation.broker,
            endpoint: &self.config.endpoint,
            evidence_dir: &binding.directory,
        };
        let live =
            self.policy
                .verify_live_role("supervisor", &self.generation.witness, &context)?;
        crate::ddr_witness_active::ensure_live(&live)?;
        let roster = verify_sealed_generation(&self.policy, &self.config, &self.generation)?;
        ensure(
            roster.normal_exits.contains(&self.generation.broker_birth),
            "bound broker wait missing",
        )?;
        crate::ddr_witness_active::ensure_live(&self.witness_fd)
    }
}

/// The caller chooses neither witness nor evidence: both come from BIND.
/// The final resource head is read from the fixed locked authority separately.
pub(crate) fn verify_live_seal(
    policy: &DeploymentPolicy,
    config: &TrustConfig,
    bound: &Generation,
    final_head: &crate::ddr_recovery_journal::Head,
    parent: &ProcessBirth,
) -> io::Result<LiveSealGuard> {
    ensure(
        bound.seal_checksum.is_none() && bound.final_head.is_none() && &bound.witness == parent,
        "seal proposer is not the original bound witness",
    )?;
    let binding = bound
        .evidence
        .as_ref()
        .ok_or_else(|| invalid("missing seal binding"))?;
    let directory = Path::new(&binding.directory);
    check_dir(directory, binding.directory_id)?;
    let head = private_file(&directory.join("witness.head"), binding.head_id, 4096)?;
    let journal = private_file(
        &directory.join("witness.journal"),
        binding.journal_id,
        MAX_WITNESS,
    )?;
    let context = RoleContext {
        generation: &bound.generation,
        broker: &bound.broker,
        endpoint: &config.endpoint,
        evidence_dir: &binding.directory,
    };
    let witness_fd = policy.verify_live_role("supervisor", &bound.witness, &context)?;
    let mut generation = bound.clone();
    generation.seal_checksum = Some(ZERO.into()); // internal verifier marker, never a trust frame
    generation.final_head = Some(final_head.clone());
    generation.evidence.as_mut().unwrap().final_head =
        Some(String::from_utf8(head.clone()).map_err(|_| invalid("non-UTF8 sealed witness head"))?);
    let guard = LiveSealGuard {
        policy: policy.clone(),
        config: config.clone(),
        generation,
        witness_fd,
        journal,
        head,
    };
    guard.recheck()?;
    Ok(guard)
}

impl LiveBindingGuard {
    pub(crate) fn generation(&self) -> &Generation {
        &self.generation
    }

    pub(crate) fn config(&self) -> &TrustConfig {
        &self.config
    }

    pub(crate) fn policy_digest(&self) -> io::Result<String> {
        self.policy.policy_digest()
    }

    pub(crate) fn recheck(&self) -> io::Result<()> {
        let binding = self
            .generation
            .evidence
            .as_ref()
            .ok_or_else(|| invalid("missing live evidence binding"))?;
        let directory = Path::new(&binding.directory);
        check_dir(directory, binding.directory_id)?;
        ensure(
            private_file(&directory.join("witness.head"), binding.head_id, 4096)? == self.head
                && private_file(
                    &directory.join("witness.journal"),
                    binding.journal_id,
                    MAX_WITNESS,
                )? == self.journal,
            "live witness evidence changed before binding",
        )?;
        crate::ddr_witness_active::ensure_live(&self.witness_fd)?;
        crate::ddr_witness_active::ensure_live(&self.broker_fd)?;
        let context = RoleContext {
            generation: &self.generation.generation,
            broker: &self.generation.broker,
            endpoint: &self.config.endpoint,
            evidence_dir: &binding.directory,
        };
        let witness =
            self.policy
                .verify_live_role("supervisor", &self.generation.witness, &context)?;
        let broker =
            self.policy
                .verify_live_role("broker", &self.generation.broker_birth, &context)?;
        ensure(
            crate::ddr_witness_active::parent_pid(self.generation.broker_birth.pid)?
                == self.generation.witness.pid,
            "broker is not the original witness child",
        )?;
        crate::ddr_witness_active::ensure_live(&witness)?;
        crate::ddr_witness_active::ensure_live(&broker)?;
        crate::ddr_witness_active::ensure_live(&self.witness_fd)?;
        crate::ddr_witness_active::ensure_live(&self.broker_fd)?;
        check_dir(directory, binding.directory_id)?;
        ensure(
            private_file(&directory.join("witness.head"), binding.head_id, 4096)? == self.head
                && private_file(
                    &directory.join("witness.journal"),
                    binding.journal_id,
                    MAX_WITNESS,
                )? == self.journal,
            "live witness evidence changed during binding check",
        )
    }
}

/// Validate a real witness and its live child, including the complete
/// two-frame initial roster, policy/executable/argv/environment, stable pidfds
/// and the direct-parent relation. Callers must still obtain the authoritative
/// inputs and semantic-zero head under the original lock; this accepts no
/// recovery and is deliberately not wired to a production launch path.
pub(crate) fn verify_live_binding(
    policy: &DeploymentPolicy,
    config: &TrustConfig,
    generation: &Generation,
) -> io::Result<LiveBindingGuard> {
    use crate::ddr_recovery_journal::Mode;
    ensure(
        generation.seal_checksum.is_none()
            && generation.final_head.is_none()
            && generation.initial_head.mode == Mode::Clean,
        "live binding is not an unsealed clean-start generation",
    )?;
    let binding = generation
        .evidence
        .as_ref()
        .ok_or_else(|| invalid("missing live evidence binding"))?;
    ensure(
        binding.final_head.is_none(),
        "live binding already has a final evidence head",
    )?;
    let directory = Path::new(&binding.directory);
    check_dir(directory, binding.directory_id)?;
    let head = private_file(&directory.join("witness.head"), binding.head_id, 4096)?;
    let journal = private_file(
        &directory.join("witness.journal"),
        binding.journal_id,
        MAX_WITNESS,
    )?;
    ensure(
        digest(&head) == binding.initial_head_sha256,
        "initial live evidence head mismatch",
    )?;
    let lines: Vec<&[u8]> = journal.split_inclusive(|byte| *byte == b'\n').collect();
    ensure(
        lines.len() == 2 && lines.iter().all(|line| line.len() <= MAX_FRAME),
        "live witness roster is not at the initial gate",
    )?;
    let mut previous = ZERO.to_owned();
    let mut events = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        let mut frame = parse_line(line)?;
        fields(&frame, &["schema", "seq", "previous", "event", "checksum"])?;
        ensure(
            s(&frame, "schema")? == FRAME_SCHEMA
                && n(&frame, "seq")? == (index + 1) as u64
                && s(&frame, "previous")? == previous,
            "invalid initial witness chain",
        )?;
        let checksum = s(&frame, "checksum")?.to_owned();
        let event = frame["event"].clone();
        frame.as_object_mut().unwrap().remove("checksum");
        ensure(
            digest(&canonical(&frame)?) == checksum,
            "invalid initial witness checksum",
        )?;
        previous = checksum;
        events.push(event);
    }
    ensure(
        head == head_bytes(2, journal.len(), &previous)?,
        "initial witness head and journal differ",
    )?;
    let start = &events[0];
    fields(
        start,
        &[
            "type",
            "run_id",
            "broker",
            "boot_id",
            "witness_birth",
            "witness_code_sha256",
            "interpreter_sha256",
        ],
    )?;
    let child = &events[1];
    fields(
        child,
        &[
            "type",
            "run_id",
            "launch_id",
            "boot_id",
            "role",
            "birth",
            "executable_sha256",
            "argv_sha256",
            "pidfd_opened",
        ],
    )?;
    let context = RoleContext {
        generation: &generation.generation,
        broker: &generation.broker,
        endpoint: &config.endpoint,
        evidence_dir: &binding.directory,
    };
    let supervisor = policy.role_evidence("supervisor", &context)?;
    let broker_role = policy.role_evidence("broker", &context)?;
    let launch = s(child, "launch_id")?;
    ensure(
        s(start, "type")? == "witness_start"
            && s(start, "run_id")? == generation.generation
            && s(start, "broker")? == generation.broker
            && s(start, "boot_id")? == config.boot
            && birth(&start["witness_birth"])? == generation.witness
            && s(start, "witness_code_sha256")? == supervisor.entrypoint_sha256
            && s(start, "interpreter_sha256")? == supervisor.executable_sha256
            && s(child, "type")? == "child_birth"
            && s(child, "run_id")? == generation.generation
            && s(child, "boot_id")? == config.boot
            && s(child, "role")? == "broker"
            && token(launch)
            && launch.starts_with(&format!("{}-", generation.generation))
            && child["pidfd_opened"] == Value::Bool(true)
            && birth(&child["birth"])? == generation.broker_birth
            && s(child, "executable_sha256")? == broker_role.executable_sha256
            && s(child, "argv_sha256")? == broker_role.argv_sha256,
        "initial live witness birth or policy mismatch",
    )?;
    let witness_fd = policy.verify_live_role("supervisor", &generation.witness, &context)?;
    let broker_fd = policy.verify_live_role("broker", &generation.broker_birth, &context)?;
    let guard = LiveBindingGuard {
        policy: policy.clone(),
        config: config.clone(),
        generation: generation.clone(),
        witness_fd,
        broker_fd,
        head,
        journal,
    };
    guard.recheck()?;
    Ok(guard)
}

/// Verify all retained evidence generations, never only the latest one. A
/// missing or unsealed earlier generation is an error; this routine cannot be
/// used for a still-active generation or to turn a dirty journal clean.
pub(crate) fn verify_sealed_history(
    policy: &DeploymentPolicy,
    history: &TrustHistory,
) -> io::Result<Vec<VerifiedRoster>> {
    ensure(
        policy.policy_digest()? == history.policy_digest && !history.generations.is_empty(),
        "policy or generation history mismatch",
    )?;
    let config = history
        .config
        .as_ref()
        .ok_or_else(|| invalid("missing versioned evidence configuration"))?;
    history
        .generations
        .iter()
        .map(|generation| verify_sealed_generation(policy, config, generation))
        .collect()
}

fn verify_sealed_generation(
    policy: &DeploymentPolicy,
    config: &TrustConfig,
    generation: &Generation,
) -> io::Result<VerifiedRoster> {
    ensure(
        generation.seal_checksum.is_some() && generation.final_head.is_some(),
        "generation has not been sealed",
    )?;
    let binding = generation
        .evidence
        .as_ref()
        .ok_or_else(|| invalid("generation has no evidence binding"))?;
    let expected_head = binding
        .final_head
        .as_ref()
        .ok_or_else(|| invalid("generation has no sealed evidence head"))?;
    let directory = Path::new(&binding.directory);
    check_dir(directory, binding.directory_id)?;
    let head_raw = private_file(&directory.join("witness.head"), binding.head_id, 4096)?;
    let journal_raw = private_file(
        &directory.join("witness.journal"),
        binding.journal_id,
        MAX_WITNESS,
    )?;
    ensure(
        head_raw == expected_head.as_bytes(),
        "pinned sealed evidence head changed",
    )?;
    let head = parse_line(&head_raw)?;
    fields(&head, &["schema", "seq", "offset", "checksum"])?;
    ensure(
        s(&head, "schema")? == HEAD_SCHEMA && n(&head, "offset")? == journal_raw.len() as u64,
        "evidence head does not match journal length",
    )?;
    let context = RoleContext {
        generation: &generation.generation,
        broker: &generation.broker,
        endpoint: &config.endpoint,
        evidence_dir: &binding.directory,
    };
    let supervisor = policy.role_evidence("supervisor", &context)?;
    let mut previous = ZERO.to_owned();
    let mut sequence = 0u64;
    let mut offset = 0usize;
    let mut children = BTreeMap::<String, Child>::new();
    let mut active_pids = BTreeMap::<i32, String>::new();
    let mut launches = BTreeSet::new();
    let mut witness_absent = BTreeSet::new();
    let mut events = Vec::new();
    let mut saw_initial = false;
    let mut saw_seal = false;
    for line in journal_raw.split_inclusive(|byte| *byte == b'\n') {
        ensure(
            !saw_seal && line.len() <= MAX_FRAME,
            "bad evidence frame boundary",
        )?;
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| invalid("evidence sequence overflow"))?;
        offset = offset
            .checked_add(line.len())
            .ok_or_else(|| invalid("evidence offset overflow"))?;
        let mut frame = parse_line(line)?;
        fields(&frame, &["schema", "seq", "previous", "event", "checksum"])?;
        ensure(
            s(&frame, "schema")? == FRAME_SCHEMA
                && n(&frame, "seq")? == sequence
                && s(&frame, "previous")? == previous,
            "evidence sequence or predecessor mismatch",
        )?;
        let checksum = s(&frame, "checksum")?.to_owned();
        let event = frame["event"].clone();
        frame.as_object_mut().unwrap().remove("checksum");
        ensure(
            digest(&canonical(&frame)?) == checksum,
            "evidence checksum mismatch",
        )?;
        let kind = s(&event, "type")?;
        ensure(
            s(&event, "run_id")? == generation.generation,
            "evidence generation mismatch",
        )?;
        match kind {
            "witness_start" if sequence == 1 => {
                fields(
                    &event,
                    &[
                        "type",
                        "run_id",
                        "broker",
                        "boot_id",
                        "witness_birth",
                        "witness_code_sha256",
                        "interpreter_sha256",
                    ],
                )?;
                ensure(
                    s(&event, "boot_id")? == config.boot
                        && s(&event, "broker")? == generation.broker
                        && birth(&event["witness_birth"])? == generation.witness
                        && s(&event, "witness_code_sha256")? == supervisor.entrypoint_sha256
                        && s(&event, "interpreter_sha256")? == supervisor.executable_sha256,
                    "witness start differs from binding or policy",
                )?;
            }
            "child_birth" if sequence > 1 => {
                fields(
                    &event,
                    &[
                        "type",
                        "run_id",
                        "launch_id",
                        "boot_id",
                        "role",
                        "birth",
                        "executable_sha256",
                        "argv_sha256",
                        "pidfd_opened",
                    ],
                )?;
                let actual = birth(&event["birth"])?;
                let key = birth_key(&actual);
                let role = s(&event, "role")?;
                let launch = s(&event, "launch_id")?;
                let expected = policy.role_evidence(role, &context)?;
                ensure(
                    s(&event, "boot_id")? == config.boot
                        && token(role)
                        && token(launch)
                        && launch.starts_with(&format!("{}-", generation.generation))
                        && event["pidfd_opened"] == Value::Bool(true)
                        && s(&event, "executable_sha256")? == expected.executable_sha256
                        && s(&event, "argv_sha256")? == expected.argv_sha256
                        && !children.contains_key(&key)
                        && !active_pids.contains_key(&actual.pid)
                        && launches.insert(launch.to_owned())
                        && ((sequence == 2
                            && role == "broker"
                            && actual == generation.broker_birth)
                            || (sequence > 2 && role != "broker")),
                    "invalid witness child birth or policy",
                )?;
                active_pids.insert(actual.pid, key.clone());
                children.insert(
                    key,
                    Child {
                        birth: actual,
                        role: role.into(),
                        launch: launch.into(),
                        waited: false,
                        resources: BTreeSet::new(),
                    },
                );
            }
            "child_resource_observed" if sequence > 2 => {
                fields(
                    &event,
                    &[
                        "type",
                        "run_id",
                        "boot_id",
                        "launch_id",
                        "role",
                        "birth",
                        "dev",
                        "ino",
                        "fd_observed",
                        "mapping_observed",
                    ],
                )?;
                let actual = birth(&event["birth"])?;
                let key = birth_key(&actual);
                let resource = (n(&event, "dev")?, n(&event, "ino")?);
                let child = children
                    .get_mut(&key)
                    .ok_or_else(|| invalid("unknown observed child"))?;
                ensure(
                    s(&event, "boot_id")? == config.boot
                        && child.birth == actual
                        && !child.waited
                        && active_pids.get(&actual.pid) == Some(&key)
                        && s(&event, "role")? == child.role
                        && s(&event, "launch_id")? == child.launch
                        && resource.1 > 0
                        && event["fd_observed"] == Value::Bool(true)
                        && event["mapping_observed"].is_boolean()
                        && child.resources.insert(resource),
                    "invalid or duplicate child backing observation",
                )?;
            }
            "witness_no_backing" if sequence > 2 => {
                fields(&event, &["type", "run_id", "boot_id", "dev", "ino"])?;
                let resource = (n(&event, "dev")?, n(&event, "ino")?);
                ensure(
                    s(&event, "boot_id")? == config.boot
                        && resource.1 > 0
                        && witness_absent.insert(resource),
                    "invalid or duplicate witness self-observation",
                )?;
            }
            "child_wait" if sequence > 2 => {
                fields(
                    &event,
                    &[
                        "type",
                        "run_id",
                        "launch_id",
                        "boot_id",
                        "role",
                        "birth",
                        "raw_wait_status",
                        "normal_exit_zero",
                    ],
                )?;
                let actual = birth(&event["birth"])?;
                let key = birth_key(&actual);
                let child = children
                    .get_mut(&key)
                    .ok_or_else(|| invalid("wait for unknown child"))?;
                ensure(
                    s(&event, "boot_id")? == config.boot
                        && !child.waited
                        && child.birth == actual
                        && s(&event, "role")? == child.role
                        && s(&event, "launch_id")? == child.launch
                        && n(&event, "raw_wait_status")? == 0
                        && event["normal_exit_zero"] == Value::Bool(true)
                        && active_pids.remove(&actual.pid).as_ref() == Some(&key),
                    "child wait does not prove normal exit",
                )?;
                child.waited = true;
            }
            "witness_seal" if sequence > 2 => {
                fields(&event, &["type", "run_id", "children", "all_normal_zero"])?;
                ensure(
                    active_pids.is_empty()
                        && !children.is_empty()
                        && children.values().all(|child| child.waited)
                        && n(&event, "children")? == children.len() as u64
                        && event["all_normal_zero"] == Value::Bool(true),
                    "incomplete or abnormal witness seal",
                )?;
                saw_seal = true;
            }
            _ => return Err(invalid("unexpected or out-of-order witness event")),
        }
        if sequence == 2 {
            let initial = head_bytes(sequence, offset, &checksum)?;
            ensure(
                digest(&initial) == binding.initial_head_sha256,
                "initial evidence head does not match BIND",
            )?;
            saw_initial = true;
        }
        previous = checksum;
        events.push(event);
    }
    ensure(
        saw_initial
            && saw_seal
            && sequence == n(&head, "seq")?
            && offset == journal_raw.len()
            && previous == s(&head, "checksum")?,
        "sealed witness history incomplete",
    )?;
    // Re-read the pinned names after parsing so a concurrent normal writer or
    // inode replacement cannot silently change the evidence snapshot.
    check_dir(directory, binding.directory_id)?;
    ensure(
        private_file(&directory.join("witness.head"), binding.head_id, 4096)? == head_raw
            && private_file(
                &directory.join("witness.journal"),
                binding.journal_id,
                MAX_WITNESS,
            )? == journal_raw,
        "witness evidence changed during verification",
    )?;
    Ok(VerifiedRoster {
        generation: generation.generation.clone(),
        witness: generation.witness.clone(),
        normal_exits: children.into_values().map(|child| child.birth).collect(),
        events,
        final_head_sha256: digest(&head_raw),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddr_recovery_journal::{AnchoredJournal, Head};
    use crate::ddr_trust_history::{EvidenceBinding, TrustAnchor};
    use std::io::{BufRead, BufReader, Write};
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::process::{Command, Stdio};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn fixtures() -> (DeploymentPolicy, TrustConfig, Vec<Generation>, PathBuf) {
        let target = PathBuf::from(std::env::var_os("CARGO_TARGET_DIR").expect("task target"));
        let target = fs::canonicalize(target).unwrap();
        assert!(target.starts_with("/workspace/uflow/runs/"));
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = target.join(format!(
            "real-witness-evidence-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let python = fs::canonicalize("/usr/bin/python3").unwrap();
        let script = fs::canonicalize(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../examples/ddr_v0/t3_deployment_witness.py"),
        )
        .unwrap();
        let py = python.to_str().unwrap();
        let source = script.to_str().unwrap();
        let executable = json!({"path": py, "sha256": digest(&fs::read(&python).unwrap())});
        let entrypoint = json!({"path": source, "sha256": digest(&fs::read(&script).unwrap())});
        let mut roles = Vec::new();
        for role in [
            "supervisor",
            "launcher",
            "interpreter",
            "broker",
            "owner",
            "accessor",
        ] {
            let argv = if role == "supervisor" {
                json!([
                    py,
                    source,
                    "_selftest",
                    "{uflow:evidence_dir}",
                    "{uflow:generation}",
                    "{uflow:broker}"
                ])
            } else {
                json!([py, source, "_resource-child", "{uflow:evidence_dir}"])
            };
            roles.push(
                json!({"role": role, "executable": executable, "entrypoint": entrypoint,
                "argv": argv, "environment": {}}),
            );
        }
        let mut encoded =
            canonical(&json!({"schema": "uflow-ddr-deployment-policy-v1", "roles": roles}))
                .unwrap();
        encoded.push(b'\n');
        let policy_file = root.join("policy.json");
        fs::write(&policy_file, encoded).unwrap();
        let policy = DeploymentPolicy::load(&policy_file).unwrap();
        let mut generations = Vec::new();
        let mut boot = String::new();
        for id in ["real-witness-g1", "real-witness-g2"] {
            let dir = root.join(id);
            fs::create_dir(&dir).unwrap();
            fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).unwrap();
            let child = Command::new(&python)
                .arg(&script)
                .arg("_selftest")
                .arg(&dir)
                .arg(id)
                .arg(format!("broker-{id}"))
                .env_clear()
                .output()
                .unwrap();
            assert!(
                child.status.success(),
                "real witness process: {:?}",
                child.status
            );
            let raw = fs::read(dir.join("witness.journal")).unwrap();
            let frames: Vec<_> = raw.split_inclusive(|byte| *byte == b'\n').collect();
            assert_eq!(frames.len(), 6);
            let start = parse_line(frames[0]).unwrap();
            let broker = parse_line(frames[1]).unwrap();
            let observed = parse_line(frames[2]).unwrap();
            let absent = parse_line(frames[3]).unwrap();
            assert_eq!(
                s(&observed["event"], "type").unwrap(),
                "child_resource_observed"
            );
            assert_eq!(observed["event"]["fd_observed"], Value::Bool(true));
            assert_eq!(observed["event"]["mapping_observed"], Value::Bool(true));
            assert_eq!(s(&absent["event"], "type").unwrap(), "witness_no_backing");
            assert_eq!(observed["event"]["dev"], absent["event"]["dev"]);
            assert_eq!(observed["event"]["ino"], absent["event"]["ino"]);
            let actual_wait = parse_line(frames[4]).unwrap();
            assert_eq!(n(&actual_wait["event"], "raw_wait_status").unwrap(), 0);
            boot = s(&start["event"], "boot_id").unwrap().to_owned();
            let witness = birth(&start["event"]["witness_birth"]).unwrap();
            let broker_birth = birth(&broker["event"]["birth"]).unwrap();
            let initial = head_bytes(
                2,
                frames[0].len() + frames[1].len(),
                s(&broker, "checksum").unwrap(),
            )
            .unwrap();
            let identity = |path: &Path| {
                let meta = fs::metadata(path).unwrap();
                (meta.dev(), meta.ino())
            };
            let evidence = EvidenceBinding {
                directory: dir.to_str().unwrap().into(),
                directory_id: identity(&dir),
                journal_id: identity(&dir.join("witness.journal")),
                head_id: identity(&dir.join("witness.head")),
                initial_head_sha256: digest(&initial),
                final_head: Some(fs::read_to_string(dir.join("witness.head")).unwrap()),
            };
            eprintln!("real evidence primitive generation={id} witness_pid={} broker_child_pid={} wait_status=0 directory={}",
                witness.pid, broker_birth.pid, dir.display());
            generations.push(Generation {
                generation: id.into(),
                broker: format!("broker-{id}"),
                broker_birth,
                witness,
                bind_sequence: 2,
                bind_checksum: "1".repeat(64),
                initial_head: Head::parse(&AnchoredJournal::genesis_state()).unwrap(),
                seal_checksum: Some("2".repeat(64)),
                final_head: Some(Head::parse(&AnchoredJournal::genesis_state()).unwrap()),
                evidence: Some(evidence),
            });
        }
        let config = TrustConfig {
            host: "host".into(),
            boot,
            domain: "external-ddr-v0".into(),
            revision: 1,
            owner_limit: 16,
            total_limit: 64,
            endpoint: "/workspace/uflow/runs/x/a.sock".into(),
            journal: (1, 2),
            journal_head: (1, 3),
        };
        (policy, config, generations, root)
    }

    fn history(
        policy: &DeploymentPolicy,
        config: &TrustConfig,
        generations: &[Generation],
    ) -> TrustHistory {
        TrustHistory {
            policy_digest: policy.policy_digest().unwrap(),
            config: Some(config.clone()),
            generations: generations.to_vec(),
        }
    }

    #[test]
    fn real_live_pair_is_bound_to_policy_parent_birth_pidfds_and_evidence() {
        if std::env::var_os("UFLOW_WITNESS_RUST_FIXTURE").as_deref() != Some("1".as_ref()) {
            eprintln!("skipping isolated live-witness test: fixture env not set");
            return;
        }
        let target =
            fs::canonicalize(PathBuf::from(std::env::var_os("CARGO_TARGET_DIR").unwrap())).unwrap();
        assert!(target.starts_with("/workspace/uflow/runs/"));
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = target.join(format!("live-evidence-{}-{nonce}", std::process::id()));
        fs::create_dir(&dir).unwrap();
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).unwrap();
        let python = fs::canonicalize("/usr/bin/python3").unwrap();
        let script = fs::canonicalize(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../examples/ddr_v0/t3_deployment_witness.py"),
        )
        .unwrap();
        let py = python.to_str().unwrap();
        let source = script.to_str().unwrap();
        let executable = json!({"path": py, "sha256": digest(&fs::read(&python).unwrap())});
        let entrypoint = json!({"path": source, "sha256": digest(&fs::read(&script).unwrap())});
        let roles: Vec<_> = [
            "supervisor",
            "launcher",
            "interpreter",
            "broker",
            "owner",
            "accessor",
        ]
        .into_iter()
        .map(|role| {
            let args = if role == "supervisor" {
                json!([
                    py,
                    source,
                    "_livecase",
                    "{uflow:evidence_dir}",
                    "{uflow:generation}",
                    "{uflow:broker}"
                ])
            } else {
                json!([py, source, "_hold-child", "{uflow:evidence_dir}"])
            };
            json!({"role": role, "executable": executable, "entrypoint": entrypoint,
                    "argv": args, "environment": {}})
        })
        .collect();
        let mut policy_data =
            canonical(&json!({"schema": "uflow-ddr-deployment-policy-v1", "roles": roles}))
                .unwrap();
        policy_data.push(b'\n');
        let policy_path = dir.join("test-policy.json");
        fs::write(&policy_path, policy_data).unwrap();
        let policy = DeploymentPolicy::load(&policy_path).unwrap();
        let mut witness = Command::new(&python)
            .arg(&script)
            .arg("_livecase")
            .arg(&dir)
            .arg("live-g1")
            .arg("live-b1")
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut ready = String::new();
        BufReader::new(witness.stdout.take().unwrap())
            .read_line(&mut ready)
            .unwrap();
        assert_eq!(ready, "READY\n");
        let journal_path = dir.join("witness.journal");
        let head_path = dir.join("witness.head");
        let raw = fs::read(&journal_path).unwrap();
        let head = fs::read(&head_path).unwrap();
        let frames: Vec<_> = raw.split_inclusive(|byte| *byte == b'\n').collect();
        assert_eq!(frames.len(), 2);
        let start = parse_line(frames[0]).unwrap();
        let broker = parse_line(frames[1]).unwrap();
        let witness_birth = birth(&start["event"]["witness_birth"]).unwrap();
        let broker_birth = birth(&broker["event"]["birth"]).unwrap();
        assert_eq!(witness_birth.pid, witness.id() as i32);
        let id = |p: &Path| {
            let m = fs::metadata(p).unwrap();
            (m.dev(), m.ino())
        };
        let root = dir.join("authority");
        let boot = s(&start["event"], "boot_id").unwrap();
        let endpoint = "/workspace/uflow/runs/x/a.sock";
        let mut authority = crate::ddr_managed_authority::ManagedAuthority::deployed_live_fixture(
            &root, boot, endpoint, &policy,
        )
        .unwrap();
        let config = TrustConfig {
            host: "host".into(),
            boot: boot.into(),
            domain: "external-ddr-v0".into(),
            revision: 1,
            owner_limit: 16,
            total_limit: 64,
            endpoint: endpoint.into(),
            journal: id(&root.join("recovery.journal")),
            journal_head: id(&root.join("recovery.head")),
        };
        let generation = Generation {
            generation: "live-g1".into(),
            broker: "live-b1".into(),
            broker_birth,
            witness: witness_birth,
            bind_sequence: 2,
            bind_checksum: "1".repeat(64),
            initial_head: Head::parse(&AnchoredJournal::genesis_state()).unwrap(),
            seal_checksum: None,
            final_head: None,
            evidence: Some(EvidenceBinding {
                directory: dir.to_str().unwrap().into(),
                directory_id: id(&dir),
                journal_id: id(&journal_path),
                head_id: id(&head_path),
                initial_head_sha256: digest(&head),
                final_head: None,
            }),
        };
        let guard = verify_live_binding(&policy, &config, &generation).unwrap();
        guard.recheck().unwrap();
        eprintln!(
            "live witness preflight witness_pid={} broker_child_pid={} directory={}",
            generation.witness.pid,
            generation.broker_birth.pid,
            dir.display()
        );
        let mut wrong = generation.clone();
        wrong.broker_birth.starttime_ticks += 1;
        assert!(verify_live_binding(&policy, &config, &wrong).is_err());
        let mut wrong = generation.clone();
        wrong.evidence.as_mut().unwrap().initial_head_sha256 = "0".repeat(64);
        assert!(verify_live_binding(&policy, &config, &wrong).is_err());
        let mut changed = head.clone();
        changed[0] ^= 1;
        fs::write(&head_path, changed).unwrap();
        assert!(guard.recheck().is_err());
        let before = (
            fs::read(root.join("state")).unwrap(),
            fs::read(root.join("witness.trust")).unwrap(),
        );
        assert!(authority
            .commit_live_binding("bad-live-bind", &guard)
            .is_err());
        assert_eq!(
            before,
            (
                fs::read(root.join("state")).unwrap(),
                fs::read(root.join("witness.trust")).unwrap()
            )
        );
        fs::write(&head_path, &head).unwrap();
        guard.recheck().unwrap();

        // The ordinary pending/append/readback mechanism binds actual live
        // child and witness processes, but this is not a ready-gated launch.
        authority
            .commit_live_binding("live-bind-tx", &guard)
            .unwrap();
        assert!(authority.is_inherited_quarantine());
        assert!(!authority.admits_new_work());
        assert!(authority.before_admission().is_err());
        let contending_lock = fs::File::open(root.join("authority.lock")).unwrap();
        assert_ne!(
            unsafe { libc::flock(contending_lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0
        );
        let state = parse_line(&fs::read(root.join("state")).unwrap()).unwrap();
        let anchor = TrustAnchor {
            sequence: n(&state, "trust_sequence").unwrap(),
            offset: n(&state, "trust_offset").unwrap(),
            sha256: s(&state, "trust_sha256").unwrap().into(),
        };
        let verified = crate::ddr_trust_history::verify(
            &fs::read(root.join("witness.trust")).unwrap(),
            &anchor,
        )
        .unwrap();
        assert_eq!(verified.generations.len(), 1);
        assert_eq!(verified.generations[0].witness, generation.witness);
        assert_eq!(
            verified.generations[0].broker_birth,
            generation.broker_birth
        );
        assert!(verified.generations[0].seal_checksum.is_none());
        eprintln!(
            "isolated authority committed real live BIND (admission closed), root={}",
            root.display()
        );

        // An independently launched process has matching code and argv but is
        // not the witness's child. Re-anchor its synthetic birth in the test
        // stream so the real parent check, not a checksum error, rejects it.
        let mut foreign = Command::new(&python)
            .arg(&script)
            .arg("_hold-child")
            .arg(&dir)
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let foreign_birth = crate::ddr_process_identity::observe(
            foreign.id() as i32,
            unsafe { libc::geteuid() },
            unsafe { libc::getegid() },
        )
        .unwrap();
        let mut frame = parse_line(frames[1]).unwrap();
        frame["event"]["birth"] = json!({"pid": foreign_birth.pid, "uid": foreign_birth.uid,
            "gid": foreign_birth.gid, "pidns_dev": foreign_birth.pid_namespace_dev,
            "pidns_ino": foreign_birth.pid_namespace_ino, "starttime_ticks": foreign_birth.starttime_ticks});
        frame.as_object_mut().unwrap().remove("checksum");
        let checksum = digest(&canonical(&frame).unwrap());
        frame["checksum"] = checksum.clone().into();
        let mut forged = frames[0].to_vec();
        forged.extend(canonical(&frame).unwrap());
        forged.push(b'\n');
        let forged_head = head_bytes(2, forged.len(), &checksum).unwrap();
        fs::write(&journal_path, &forged).unwrap();
        fs::write(&head_path, &forged_head).unwrap();
        let mut foreign_generation = generation.clone();
        foreign_generation.broker_birth = foreign_birth;
        foreign_generation
            .evidence
            .as_mut()
            .unwrap()
            .initial_head_sha256 = digest(&forged_head);
        let error = verify_live_binding(&policy, &config, &foreign_generation)
            .err()
            .expect("foreign parent rejected");
        assert!(
            error.to_string().contains("not the original witness child"),
            "{error}"
        );
        fs::write(&journal_path, &raw).unwrap();
        fs::write(&head_path, &head).unwrap();
        guard.recheck().unwrap();
        witness.stdin.take().unwrap().write_all(b"x").unwrap();
        assert!(witness.wait().unwrap().success());
        assert!(foreign.wait().unwrap().success());
        assert!(guard.recheck().is_err());
        drop(authority);
        let mut reopened =
            crate::ddr_managed_authority::ManagedAuthority::reopen_deployed_live_fixture(
                &root, boot, endpoint,
            )
            .unwrap();
        assert!(reopened.is_inherited_quarantine());
        assert!(!reopened.admits_new_work());
        assert!(reopened.before_admission().is_err());
    }

    #[test]
    fn real_two_process_witness_streams_and_historical_tamper_are_checked() {
        // The witness/child processes and kernel wait statuses are real. The
        // surrounding BIND/SEAL and resource journal here are test fixtures;
        // this is NOT a production generation or recovery acceptance test.
        if std::env::var_os("UFLOW_WITNESS_RUST_FIXTURE").as_deref() != Some("1".as_ref()) {
            eprintln!("skipping isolated real-witness evidence test: fixture env not set");
            return;
        }
        let (policy, config, generations, root) = fixtures();
        assert_ne!(generations[0].witness, generations[1].witness);
        let reports =
            verify_sealed_history(&policy, &history(&policy, &config, &generations)).unwrap();
        assert_eq!(reports.len(), 2);
        assert!(reports.iter().all(|report| report.normal_exits.len() == 1));
        for fault in [
            "initial-head",
            "witness",
            "broker",
            "broker-label",
            "unsealed",
            "directory",
            "journal",
            "head",
        ] {
            let mut altered = generations.clone();
            match fault {
                "initial-head" => {
                    altered[0].evidence.as_mut().unwrap().initial_head_sha256 = "0".repeat(64)
                }
                "witness" => altered[0].witness.starttime_ticks += 1,
                "broker" => altered[0].broker_birth.starttime_ticks += 1,
                "broker-label" => altered[0].broker = "other-broker".into(),
                "unsealed" => altered[0].seal_checksum = None,
                "directory" => altered[0].evidence.as_mut().unwrap().directory_id.1 += 1,
                "journal" => altered[0].evidence.as_mut().unwrap().journal_id.1 += 1,
                "head" => altered[0].evidence.as_mut().unwrap().head_id.1 += 1,
                _ => unreachable!(),
            }
            assert!(
                verify_sealed_history(&policy, &history(&policy, &config, &altered)).is_err(),
                "fault={fault}"
            );
        }
        let path = root.join("real-witness-g1/witness.journal");
        let original = fs::read(&path).unwrap();
        let last = original[..original.len() - 1]
            .iter()
            .rposition(|byte| *byte == b'\n')
            .unwrap()
            + 1;
        fs::write(&path, &original[..last]).unwrap();
        assert!(verify_sealed_history(&policy, &history(&policy, &config, &generations)).is_err());
        fs::write(&path, &original).unwrap();
        assert_eq!(
            verify_sealed_history(&policy, &history(&policy, &config, &generations))
                .unwrap()
                .len(),
            2
        );

        // Adversarial parser inputs, not claimed real kernel statuses: repair
        // every frame hash and both test anchors so semantic checks rather than
        // a broken checksum must reject them.
        let head_path = root.join("real-witness-g1/witness.head");
        let original_head = fs::read(&head_path).unwrap();
        for fault in [
            "signal-wait",
            "wait-birth",
            "broker-argv",
            "witness-code",
            "duplicate-wait",
            "unknown-observation",
            "no-fd-observation",
            "duplicate-observation",
            "observation-after-wait",
            "duplicate-witness-absence",
        ] {
            let mut frames: Vec<Value> = original
                .split_inclusive(|byte| *byte == b'\n')
                .map(|raw| parse_line(raw).unwrap())
                .collect();
            match fault {
                "signal-wait" => frames[4]["event"]["raw_wait_status"] = 15.into(),
                "wait-birth" => frames[4]["event"]["birth"]["starttime_ticks"] = 1.into(),
                "broker-argv" => frames[1]["event"]["argv_sha256"] = "0".repeat(64).into(),
                "witness-code" => frames[0]["event"]["witness_code_sha256"] = "0".repeat(64).into(),
                "duplicate-wait" => frames.insert(5, frames[4].clone()),
                "unknown-observation" => frames[2]["event"]["birth"]["starttime_ticks"] = 1.into(),
                "no-fd-observation" => frames[2]["event"]["fd_observed"] = false.into(),
                "duplicate-observation" => frames.insert(3, frames[2].clone()),
                "observation-after-wait" => {
                    let observation = frames.remove(2);
                    frames.insert(4, observation);
                }
                "duplicate-witness-absence" => frames.insert(4, frames[3].clone()),
                _ => unreachable!(),
            }
            let mut raw = Vec::new();
            let mut previous = ZERO.to_owned();
            let mut initial = Vec::new();
            for (index, frame) in frames.iter_mut().enumerate() {
                frame["seq"] = ((index + 1) as u64).into();
                frame["previous"] = previous.clone().into();
                frame.as_object_mut().unwrap().remove("checksum");
                let checksum = digest(&canonical(frame).unwrap());
                frame["checksum"] = checksum.clone().into();
                raw.extend(canonical(frame).unwrap());
                raw.push(b'\n');
                if index == 1 {
                    initial = head_bytes(2, raw.len(), &checksum).unwrap();
                }
                previous = checksum;
            }
            let rewritten_head = head_bytes(frames.len() as u64, raw.len(), &previous).unwrap();
            fs::write(&path, &raw).unwrap();
            fs::write(&head_path, &rewritten_head).unwrap();
            let mut altered = generations.clone();
            let binding = altered[0].evidence.as_mut().unwrap();
            binding.initial_head_sha256 = digest(&initial);
            binding.final_head = Some(String::from_utf8(rewritten_head).unwrap());
            assert!(
                verify_sealed_history(&policy, &history(&policy, &config, &altered)).is_err(),
                "fault={fault}"
            );
            fs::write(&path, &original).unwrap();
            fs::write(&head_path, &original_head).unwrap();
        }
        assert!(verify_sealed_history(&policy, &history(&policy, &config, &generations)).is_ok());
    }
}
