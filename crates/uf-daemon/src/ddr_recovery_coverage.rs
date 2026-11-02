//! Fixture-only, read-only coverage verification. This is not recovery or clean.
//! All inputs come from a locked, dirty ManagedAuthority and its pinned trust
//! slot; there is no caller-supplied holder set, evidence path or force flag.
//! The v2 pin is genesis-only. A clean restart needs a new binding protocol.
#![allow(dead_code)]

use crate::ddr_managed_authority::ManagedAuthority;
use crate::ddr_process_identity::ProcessBirth;
use crate::ddr_recovery_audit::{AuditReplay, Event, EventKind};
use crate::ddr_recovery_codec;
use crate::ddr_recovery_journal::{Head, Kind, Mode};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{self, Read};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

pub(crate) const GENESIS_HEAD: &[u8] = b"uflow-ddr-recovery-head-v1\nmode=clean\nbroker=none\nsequence=0\noffset=0\nchecksum=0000000000000000\nclean_sequence=0\n";
pub(crate) const BINDING: &str = "uflow-ddr-witness-binding-fixture-v2";
pub(crate) const WITNESS: &str = "uflow-ddr-witness-fixture-v1";
pub(crate) const MAX_WITNESS: usize = 16 * 1024 * 1024;

#[link(name = "crypto")]
unsafe extern "C" {
    fn SHA256(data: *const u8, len: usize, md: *mut u8) -> *mut u8;
}
pub(crate) fn digest(data: &[u8]) -> String {
    let mut md = [0u8; 32];
    // OpenSSL's one-shot API writes exactly 32 bytes into the caller's buffer.
    let result = unsafe { SHA256(data.as_ptr(), data.len(), md.as_mut_ptr()) };
    assert!(!result.is_null());
    md.iter().map(|v| format!("{v:02x}")).collect()
}
pub(crate) fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
pub(crate) fn ensure(condition: bool, message: &'static str) -> io::Result<()> {
    if condition {
        Ok(())
    } else {
        Err(invalid(message))
    }
}
fn obj(v: &Value) -> io::Result<&Map<String, Value>> {
    v.as_object().ok_or_else(|| invalid("expected object"))
}
pub(crate) fn fields(v: &Value, keys: &[&str]) -> io::Result<()> {
    let o = obj(v)?;
    ensure(
        o.len() == keys.len() && keys.iter().all(|k| o.contains_key(*k)),
        "unexpected JSON fields",
    )
}
pub(crate) fn s<'a>(v: &'a Value, key: &str) -> io::Result<&'a str> {
    v.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| invalid(format!("invalid string {key}")))
}
pub(crate) fn n(v: &Value, key: &str) -> io::Result<u64> {
    v.get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| invalid(format!("invalid integer {key}")))
}
pub(crate) fn token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 96
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
}
pub(crate) fn canonical(v: &Value) -> io::Result<Vec<u8>> {
    serde_json::to_vec(v).map_err(|e| invalid(e.to_string()))
}
pub(crate) fn parse_line(raw: &[u8]) -> io::Result<Value> {
    ensure(
        raw.last() == Some(&b'\n') && raw.is_ascii(),
        "noncanonical JSON line",
    )?;
    let value: Value =
        serde_json::from_slice(&raw[..raw.len() - 1]).map_err(|e| invalid(e.to_string()))?;
    let mut encoded = canonical(&value)?;
    encoded.push(b'\n');
    ensure(encoded == raw, "noncanonical or duplicate JSON")?;
    Ok(value)
}
pub(crate) fn birth(v: &Value) -> io::Result<ProcessBirth> {
    fields(
        v,
        &[
            "pid",
            "uid",
            "gid",
            "pidns_dev",
            "pidns_ino",
            "starttime_ticks",
        ],
    )?;
    let pid = i32::try_from(n(v, "pid")?).map_err(|_| invalid("invalid pid"))?;
    let uid = u32::try_from(n(v, "uid")?).map_err(|_| invalid("invalid uid"))?;
    let gid = u32::try_from(n(v, "gid")?).map_err(|_| invalid("invalid gid"))?;
    let value = ProcessBirth {
        pid,
        uid,
        gid,
        pid_namespace_dev: n(v, "pidns_dev")?,
        pid_namespace_ino: n(v, "pidns_ino")?,
        starttime_ticks: n(v, "starttime_ticks")?,
    };
    ensure(
        value.pid > 0 && value.pid_namespace_ino > 0 && value.starttime_ticks > 0,
        "invalid process birth",
    )?;
    Ok(value)
}
pub(crate) fn birth_key(b: &ProcessBirth) -> String {
    format!(
        "{}:{}:{}:{}:{}:{}",
        b.pid, b.uid, b.gid, b.pid_namespace_dev, b.pid_namespace_ino, b.starttime_ticks
    )
}
pub(crate) fn identity(v: &Value, key: &str) -> io::Result<(u64, u64)> {
    let a = v
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("invalid pinned identity"))?;
    ensure(a.len() == 2, "invalid pinned identity")?;
    Ok((
        a[0].as_u64().ok_or_else(|| invalid("invalid dev"))?,
        a[1].as_u64().ok_or_else(|| invalid("invalid inode"))?,
    ))
}
pub(crate) fn private_file(path: &Path, expected: (u64, u64), limit: usize) -> io::Result<Vec<u8>> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    let meta = file.metadata()?;
    ensure(
        meta.is_file()
            && meta.uid() == unsafe { libc::geteuid() }
            && meta.mode() & 0o077 == 0
            && meta.nlink() == 1
            && (meta.dev(), meta.ino()) == expected
            && meta.len() <= limit as u64,
        "pinned witness file changed",
    )?;
    let mut data = Vec::new();
    (&mut file).take(limit as u64 + 1).read_to_end(&mut data)?;
    let after = file.metadata()?;
    let entry = fs::symlink_metadata(path)?;
    ensure(
        data.len() as u64 == meta.len()
            && data.len() <= limit
            && after.nlink() == 1
            && (entry.dev(), entry.ino()) == expected
            && (after.dev(), after.ino()) == expected
            && after.len() == meta.len(),
        "pinned witness file changed during read",
    )?;
    Ok(data)
}
pub(crate) fn validate_code(binding: &Value, path_key: &str, hash_key: &str) -> io::Result<()> {
    let path = PathBuf::from(s(binding, path_key)?);
    ensure(
        path.is_absolute() && fs::canonicalize(&path)? == path,
        "noncanonical pinned code path",
    )?;
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&path)?;
    let meta = file.metadata()?;
    let entry = fs::symlink_metadata(&path)?;
    ensure(
        meta.is_file()
            && meta.len() <= 512 * 1024 * 1024
            && (entry.dev(), entry.ino(), entry.len()) == (meta.dev(), meta.ino(), meta.len()),
        "invalid pinned code file",
    )?;
    let mut bytes = Vec::new();
    (&mut file).take(meta.len() + 1).read_to_end(&mut bytes)?;
    let held_after = file.metadata()?;
    let after = fs::symlink_metadata(&path)?;
    ensure(
        (meta.dev(), meta.ino(), meta.len())
            == (held_after.dev(), held_after.ino(), held_after.len())
            && (meta.dev(), meta.ino(), meta.len()) == (after.dev(), after.ino(), after.len())
            && fs::canonicalize(&path)? == path
            && bytes.len() as u64 == meta.len()
            && digest(&bytes) == s(binding, hash_key)?,
        "pinned executable or script changed",
    )
}

#[derive(Debug, Clone)]
pub(crate) struct CoverageReport {
    input_head: Head,
    witness_head_sha256: String,
    subjects: Vec<String>,
    normal_exits: Vec<ProcessBirth>,
    work_drains: Vec<String>,
    broker_drain: String,
    events: Vec<Event>,
}
impl CoverageReport {
    pub(crate) fn input_head(&self) -> &Head {
        &self.input_head
    }
    pub(crate) fn witness_head_sha256(&self) -> &str {
        &self.witness_head_sha256
    }
    pub(crate) fn subjects(&self) -> &[String] {
        &self.subjects
    }
    /// This set is populated only after the complete pinned roster, raw wait
    /// statuses, role/resource coverage and journal have all been checked.
    pub(crate) fn has_normal_exit(&self, birth: &ProcessBirth) -> bool {
        self.normal_exits.contains(birth)
    }
    pub(crate) fn work_drains(&self) -> &[String] {
        &self.work_drains
    }
    pub(crate) fn events(&self) -> &[Event] {
        &self.events
    }
    pub(crate) fn broker_drain(&self) -> &str {
        &self.broker_drain
    }
}
#[derive(Default)]
struct Subject {
    broker: bool,
    owner: bool,
    accessor: bool,
    resources: BTreeSet<(u64, u64)>,
}
fn subject<'a>(subjects: &'a mut BTreeMap<String, Subject>, b: &ProcessBirth) -> &'a mut Subject {
    subjects.entry(birth_key(b)).or_default()
}

pub(crate) fn verify(authority: &ManagedAuthority) -> io::Result<CoverageReport> {
    verify_inner(authority, false)
}

/// Re-verify the same pinned evidence against the original genesis prefix of
/// one recovery transaction. Later generations still need semantic replay.
/// No caller can choose the prefix or evidence.
pub(crate) fn verify_recovered(authority: &ManagedAuthority) -> io::Result<CoverageReport> {
    verify_inner(authority, true)
}

fn verify_inner(authority: &ManagedAuthority, recovered: bool) -> io::Result<CoverageReport> {
    let (mut journal, binding_raw, mut domain) = if recovered {
        authority.fixture_recovery_material()?
    } else {
        authority.fixture_coverage_material()?
    };
    let full_head = journal.head().clone();
    let full_frames = journal.verify()?;
    let binding = parse_line(binding_raw.as_bytes())?;
    let frames = if recovered {
        ensure(
            full_head.mode == Mode::Clean
                && full_frames.len() >= 4
                && full_head.clean_sequence == full_head.sequence,
            "incomplete recovery transaction",
        )?;
        // This v2 binding covers only genesis and exactly one recovery. Find
        // its retained transaction, not an arbitrary caller-selected prefix.
        let mut positions = full_frames
            .iter()
            .enumerate()
            .filter(|(_, frame)| frame.kind == Kind::RecoveryAudit)
            .map(|(index, _)| index);
        let split = positions
            .next()
            .ok_or_else(|| invalid("missing recovery transaction"))?;
        ensure(
            positions.next().is_none()
                && split > 0
                && split
                    .checked_add(3)
                    .is_some_and(|end| end <= full_frames.len()),
            "ambiguous or incomplete recovery transaction",
        )?;
        ensure(
            full_frames[split - 1].kind == Kind::BrokerDrained
                && full_frames[split].kind == Kind::RecoveryAudit
                && full_frames[split + 1].kind == Kind::RecoveryCommit
                && full_frames[split + 2].kind == Kind::CleanCheckpoint,
            "invalid recovery transaction suffix",
        )?;
        // A later normal generation may own the current head. The historical
        // broker comes from the independently pinned binding and is checked
        // against the replayed journal below.
        domain.broker = s(&binding, "broker_instance")?.to_owned();
        full_frames[..split].to_vec()
    } else {
        full_frames.clone()
    };
    let input_head = if recovered {
        let last = frames
            .last()
            .ok_or_else(|| invalid("empty recovery input"))?;
        Head {
            mode: Mode::Dirty,
            broker: domain.broker.clone(),
            sequence: last.sequence,
            offset: frames.iter().map(|frame| frame.encoded_len()).sum(),
            checksum: last.checksum,
            clean_sequence: 0,
        }
    } else {
        full_head.clone()
    };
    ensure(
        input_head.mode == Mode::Dirty && input_head.clean_sequence == 0 && input_head.sequence > 0,
        "fixture verifier accepts only a dirty genesis generation",
    )?;
    fields(
        &binding,
        &[
            "schema",
            "run_id",
            "host",
            "domain",
            "boot",
            "cap_revision",
            "broker_instance",
            "lifecycle_epoch",
            "broker_journal",
            "broker_head",
            "initial_head_sha256",
            "witness_birth",
            "witness_code_sha256",
            "witness_code_path",
            "interpreter_path",
            "interpreter_sha256",
            "pin_code_path",
            "pin_code_sha256",
            "child_executable_path",
            "child_executable_sha256",
            "sealed_head_sha256",
            "evidence_dir",
            "evidence_journal",
            "evidence_head",
        ],
    )?;
    let run = s(&binding, "run_id")?;
    ensure(
        s(&binding, "schema")? == BINDING
            && token(run)
            && s(&binding, "host")? == domain.host
            && s(&binding, "domain")? == domain.domain
            && s(&binding, "boot")? == domain.boot
            && n(&binding, "cap_revision")? == domain.cap_revision
            && s(&binding, "broker_instance")? == domain.broker
            && n(&binding, "lifecycle_epoch")? == 0
            && s(&binding, "initial_head_sha256")? == digest(GENESIS_HEAD),
        "pinned witness generation mismatch",
    )?;
    // Journal and head inode pins are also checked by ManagedAuthority; the
    // trust values themselves must match those exact bootstrap-pinned files.
    authority.fixture_verify_binding_journal_ids(
        identity(&binding, "broker_journal")?,
        identity(&binding, "broker_head")?,
    )?;
    for (p, h) in [
        ("witness_code_path", "witness_code_sha256"),
        ("interpreter_path", "interpreter_sha256"),
        ("pin_code_path", "pin_code_sha256"),
        ("child_executable_path", "child_executable_sha256"),
    ] {
        validate_code(&binding, p, h)?;
    }
    let witness_birth = birth(
        binding
            .get("witness_birth")
            .ok_or_else(|| invalid("missing witness birth"))?,
    )?;
    let evidence = PathBuf::from(s(&binding, "evidence_dir")?);
    ensure(
        evidence.starts_with("/workspace/uflow/runs/") && fs::canonicalize(&evidence)? == evidence,
        "invalid pinned evidence directory",
    )?;
    let dir = fs::symlink_metadata(&evidence)?;
    ensure(
        dir.is_dir() && dir.uid() == unsafe { libc::geteuid() } && dir.mode() & 0o077 == 0,
        "invalid evidence directory",
    )?;
    let evidence_id = identity(&binding, "evidence_journal")?;
    let witness_head_id = identity(&binding, "evidence_head")?;
    let head_raw = private_file(&evidence.join("witness.head"), witness_head_id, 4096)?;
    let roster_raw = private_file(&evidence.join("witness.journal"), evidence_id, MAX_WITNESS)?;
    ensure(
        s(&binding, "sealed_head_sha256")? == digest(&head_raw),
        "witness seal anchor mismatch",
    )?;
    let head = parse_line(&head_raw)?;
    fields(&head, &["schema", "seq", "offset", "checksum"])?;
    ensure(
        s(&head, "schema")? == WITNESS && n(&head, "offset")? == roster_raw.len() as u64,
        "witness head mismatch",
    )?;
    let mut previous = "0".repeat(64);
    let mut events = Vec::new();
    for (i, raw) in roster_raw.split_inclusive(|b| *b == b'\n').enumerate() {
        ensure(raw.len() <= 8192, "witness frame too large")?;
        let mut frame = parse_line(raw)?;
        fields(&frame, &["schema", "seq", "previous", "event", "checksum"])?;
        ensure(
            s(&frame, "schema")? == WITNESS
                && n(&frame, "seq")? == (i + 1) as u64
                && s(&frame, "previous")? == previous,
            "witness chain mismatch",
        )?;
        let checksum = s(&frame, "checksum")?.to_owned();
        let event = frame
            .get("event")
            .ok_or_else(|| invalid("missing witness event"))?
            .clone();
        frame.as_object_mut().unwrap().remove("checksum");
        ensure(
            digest(&canonical(&frame)?) == checksum,
            "witness checksum mismatch",
        )?;
        previous = checksum;
        events.push(event);
    }
    ensure(
        events.len() as u64 == n(&head, "seq")?
            && previous == s(&head, "checksum")?
            && events.len() >= 2,
        "witness sealed head mismatch",
    )?;
    let start = &events[0];
    fields(
        start,
        &[
            "type",
            "run_id",
            "boot_id",
            "witness_birth",
            "witness_code_sha256",
            "interpreter_sha256",
        ],
    )?;
    ensure(
        s(start, "type")? == "witness_start"
            && s(start, "run_id")? == run
            && s(start, "boot_id")? == domain.boot
            && birth(&start["witness_birth"])? == witness_birth
            && s(start, "witness_code_sha256")? == s(&binding, "witness_code_sha256")?
            && s(start, "interpreter_sha256")? == s(&binding, "interpreter_sha256")?,
        "witness start mismatch",
    )?;
    let seal = events.last().unwrap();
    fields(seal, &["type", "run_id", "children", "all_normal_zero"])?;
    ensure(
        s(seal, "type")? == "witness_seal"
            && s(seal, "run_id")? == run
            && seal["all_normal_zero"] == Value::Bool(true),
        "missing sealed normal roster",
    )?;

    let mut replay = AuditReplay::new(domain.clone()).map_err(invalid)?;
    let mut subjects: BTreeMap<String, Subject> = BTreeMap::new();
    let mut backings: BTreeMap<u64, (u64, u64)> = BTreeMap::new();
    let mut broker_birth: Option<ProcessBirth> = None;
    let mut work: BTreeMap<u64, (String, bool)> = BTreeMap::new();
    let mut drain = false;
    let mut events_replayed = Vec::new();
    for (index, frame) in frames.iter().enumerate() {
        let event = ddr_recovery_codec::decode(frame).map_err(invalid)?;
        if index == 0 {
            ensure(
                matches!(event.kind, EventKind::BrokerBirth { .. })
                    && event.epoch == n(&binding, "lifecycle_epoch")?,
                "missing initial broker birth",
            )?;
        }
        events_replayed.push(event.clone());
        match &event.kind {
            EventKind::BrokerBirth { birth } => {
                subject(&mut subjects, birth).broker = true;
                broker_birth = Some(birth.clone());
            }
            EventKind::RegisterIntent(b) => {
                backings.insert(b.object_id, (b.memfd_dev, b.memfd_ino));
                let entry = subject(&mut subjects, &b.owner);
                entry.owner = true;
                entry.resources.insert((b.memfd_dev, b.memfd_ino));
            }
            EventKind::ViewIntent(v) => {
                let resource = *backings
                    .get(&v.object_id)
                    .ok_or_else(|| invalid("view without backing"))?;
                let entry = subject(&mut subjects, &v.accessor);
                entry.accessor = true;
                entry.resources.insert(resource);
            }
            EventKind::WorkIntent(w) => {
                let b = broker_birth
                    .as_ref()
                    .ok_or_else(|| invalid("work without broker birth"))?;
                let expected = format!(
                    "bt-{}-{}-{}-{}-{}",
                    w.work_id, b.pid, b.starttime_ticks, b.pid_namespace_dev, b.pid_namespace_ino
                );
                ensure(
                    w.worker_id == expected,
                    "worker job is not bound to broker birth",
                )?;
                ensure(
                    work.insert(w.work_id, (w.worker_id.clone(), false))
                        .is_none(),
                    "duplicate worker",
                )?;
            }
            EventKind::WorkDrained { work_id } => {
                let item = work
                    .get_mut(work_id)
                    .ok_or_else(|| invalid("unknown work drain"))?;
                ensure(!item.1, "duplicate work drain")?;
                item.1 = true;
            }
            EventKind::BrokerDrained(_) => {
                drain = true;
            }
            EventKind::CleanCheckpoint(_) => {
                return Err(invalid("non-genesis recovery not supported"))
            }
            _ => {}
        }
        replay.apply(event).map_err(invalid)?;
    }
    ensure(
        drain && work.values().all(|item| item.1) && replay.active_work() == 0,
        "missing broker/handler or worker drain",
    )?;
    let b = broker_birth.ok_or_else(|| invalid("missing broker birth"))?;
    for resource in backings.values() {
        subject(&mut subjects, &b).resources.insert(*resource);
    }

    #[derive(Clone)]
    struct Child {
        role: String,
        launch: String,
        birth: ProcessBirth,
        waited: bool,
        resources: BTreeSet<(u64, u64)>,
    }
    let mut children: BTreeMap<String, Child> = BTreeMap::new();
    let mut active_pids = BTreeMap::new();
    let mut launch_ids = BTreeSet::new();
    let mut witness_absent = BTreeSet::new();
    for item in &events[1..events.len() - 1] {
        let kind = s(item, "type")?;
        match kind {
            "child_birth" => {
                fields(
                    item,
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
                let b = birth(&item["birth"])?;
                let key = birth_key(&b);
                let launch = s(item, "launch_id")?;
                let role = s(item, "role")?;
                ensure(
                    s(item, "run_id")? == run
                        && s(item, "boot_id")? == domain.boot
                        && launch.starts_with(&format!("{run}-"))
                        && token(launch)
                        && token(role)
                        && item["pidfd_opened"] == Value::Bool(true)
                        && s(item, "executable_sha256")? == s(&binding, "child_executable_sha256")?
                        && s(item, "argv_sha256")?.len() == 64
                        && s(item, "argv_sha256")?
                            .bytes()
                            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
                        && !children.contains_key(&key)
                        && !active_pids.contains_key(&b.pid)
                        && launch_ids.insert(launch.to_owned()),
                    "invalid or duplicate child birth",
                )?;
                active_pids.insert(b.pid, key.clone());
                children.insert(
                    key,
                    Child {
                        role: role.into(),
                        launch: launch.into(),
                        birth: b,
                        waited: false,
                        resources: BTreeSet::new(),
                    },
                );
            }
            "child_wait" => {
                fields(
                    item,
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
                let b = birth(&item["birth"])?;
                let key = birth_key(&b);
                let child = children
                    .get_mut(&key)
                    .ok_or_else(|| invalid("unknown waited child"))?;
                ensure(
                    s(item, "run_id")? == run
                        && s(item, "boot_id")? == domain.boot
                        && s(item, "role")? == child.role
                        && s(item, "launch_id")? == child.launch
                        && !child.waited
                        && n(item, "raw_wait_status")? == 0
                        && item["normal_exit_zero"] == Value::Bool(true)
                        && active_pids.remove(&b.pid).as_ref() == Some(&key),
                    "child did not have a matching normal wait",
                )?;
                child.waited = true;
            }
            "child_resource_observed" => {
                fields(
                    item,
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
                let b = birth(&item["birth"])?;
                let key = birth_key(&b);
                let child = children
                    .get_mut(&key)
                    .ok_or_else(|| invalid("unknown resource holder"))?;
                let resource = (n(item, "dev")?, n(item, "ino")?);
                ensure(
                    s(item, "run_id")? == run
                        && s(item, "boot_id")? == domain.boot
                        && s(item, "role")? == child.role
                        && s(item, "launch_id")? == child.launch
                        && !child.waited
                        && resource.1 > 0
                        && item["fd_observed"] == Value::Bool(true)
                        && item["mapping_observed"].is_boolean()
                        && child.resources.insert(resource),
                    "invalid live resource observation",
                )?;
            }
            "witness_no_backing" => {
                fields(item, &["type", "run_id", "boot_id", "dev", "ino"])?;
                let resource = (n(item, "dev")?, n(item, "ino")?);
                ensure(
                    s(item, "run_id")? == run
                        && s(item, "boot_id")? == domain.boot
                        && resource.1 > 0
                        && witness_absent.insert(resource),
                    "invalid witness no-backing observation",
                )?;
            }
            _ => return Err(invalid("unknown witness event")),
        }
    }
    ensure(
        active_pids.is_empty()
            && children.values().all(|v| v.waited)
            && n(seal, "children")? == children.len() as u64
            && children.len() == subjects.len(),
        "missing or extra child",
    )?;
    ensure(
        witness_absent == backings.values().copied().collect(),
        "witness no-backing coverage mismatch",
    )?;
    let mut conclusions = Vec::new();
    let mut normal_exits = Vec::new();
    for (key, expected) in subjects {
        let child = children
            .get(&key)
            .ok_or_else(|| invalid("journal holder missing from roster"))?;
        let role_ok = match child.role.as_str() {
            "broker" => expected.broker && !expected.owner && !expected.accessor,
            "owner" => expected.owner && !expected.broker && !expected.accessor,
            "accessor" | "worker" => expected.accessor && !expected.broker && !expected.owner,
            _ => false,
        };
        ensure(
            role_ok && child.resources == expected.resources && child.waited,
            "roster role or backing coverage mismatch",
        )?;
        normal_exits.push(child.birth.clone());
        conclusions.push(format!(
            "{}:{} resources={:?} exit=0",
            child.role,
            birth_key(&child.birth),
            child.resources
        ));
    }
    // Reject changed files and head even if a live writer raced the first read.
    ensure(
        private_file(&evidence.join("witness.head"), witness_head_id, 4096)? == head_raw
            && private_file(&evidence.join("witness.journal"), evidence_id, MAX_WITNESS)?
                == roster_raw,
        "witness evidence changed during verification",
    )?;
    let (mut rechecked, binding_again, mut domain_again) = if recovered {
        authority.fixture_recovery_material()?
    } else {
        authority.fixture_coverage_material()?
    };
    if recovered {
        domain_again.broker = s(&binding, "broker_instance")?.to_owned();
    }
    ensure(
        binding_again == binding_raw
            && domain_again == domain
            && rechecked.head() == &full_head
            && rechecked.verify()? == full_frames,
        "pinned authority changed during verification",
    )?;
    Ok(CoverageReport {
        input_head,
        witness_head_sha256: digest(&head_raw),
        subjects: conclusions,
        normal_exits,
        work_drains: work
            .into_values()
            .map(|(id, _)| format!("{id}:joined"))
            .collect(),
        broker_drain: "joined listener/handlers and drained workers".into(),
        events: events_replayed,
    })
}
