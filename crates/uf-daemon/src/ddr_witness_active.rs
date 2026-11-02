//! Read-only live witness checks for the existing genesis-only v2 binding.
//!
//! This is an intermediate primitive, not production authorization. In
//! particular, v2 pins a single child executable, cannot retain generations,
//! and the fixed-root deployment has no supported way to install the binding.
//! Production admission and recovery stay closed until those gaps are solved.
#![allow(dead_code)]

use crate::ddr_managed_authority::ManagedAuthority;
use crate::ddr_process_identity::{self, ProcessBirth};
use crate::ddr_recovery_codec;
use crate::ddr_recovery_coverage::{
    birth, birth_key, canonical, digest, ensure, fields, identity, invalid, n, parse_line,
    private_file, s, token, validate_code, BINDING, GENESIS_HEAD, MAX_WITNESS, WITNESS,
};
use crate::ddr_recovery_journal::Mode;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;

const BINDING_FIELDS: &[&str] = &[
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
];

pub(crate) fn live_pidfd(b: &ProcessBirth) -> io::Result<File> {
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, b.pid, 0) } as i32;
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // The successful syscall returns a new owned descriptor.
    let fd = unsafe { File::from_raw_fd(fd) };
    ensure_live(&fd)?;
    ensure(
        ddr_process_identity::observe(b.pid, b.uid, b.gid)? == *b,
        "live birth changed",
    )?;
    Ok(fd)
}

pub(crate) fn ensure_live(fd: &File) -> io::Result<()> {
    let mut pollfd = libc::pollfd {
        fd: fd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let result = unsafe { libc::poll(&mut pollfd, 1, 0) };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    ensure(result == 0, "witness or child is no longer live")
}

pub(crate) fn parent_pid(pid: i32) -> io::Result<i32> {
    let status = fs::read_to_string(format!("/proc/{pid}/status"))?;
    let mut values = status.lines().filter_map(|line| line.strip_prefix("PPid:"));
    let value = values.next().ok_or_else(|| invalid("missing parent PID"))?;
    ensure(values.next().is_none(), "duplicate parent PID")?;
    value
        .trim()
        .parse()
        .map_err(|_| invalid("invalid parent PID"))
}

fn check_live_child(
    birth: &ProcessBirth,
    witness: &ProcessBirth,
    expected_executable: &str,
) -> io::Result<File> {
    let fd = live_pidfd(birth)?;
    ensure(
        parent_pid(birth.pid)? == witness.pid,
        "process is not a direct witness child",
    )?;
    ensure(
        fs::canonicalize(format!("/proc/{}/exe", birth.pid))? == PathBuf::from(expected_executable),
        "live child executable differs from deployment binding",
    )?;
    ensure_live(&fd)?;
    Ok(fd)
}

fn hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

#[derive(Clone)]
struct Child {
    birth: ProcessBirth,
    role: String,
    launch: String,
    waited: bool,
}

/// Check the original pinned witness, its live direct-parent relationship and
/// a specific broker/peer birth. The caller cannot choose an evidence path.
/// This v2-only check must NOT alone unlock production admission or recovery.
pub(crate) fn verify_fixture_active_parent(
    authority: &ManagedAuthority,
    broker: &str,
    peer: Option<&ProcessBirth>,
) -> io::Result<()> {
    let (mut journal, binding_raw, domain) = authority.fixture_recovery_material()?;
    let full_head = journal.head().clone();
    let frames = journal.verify()?;
    let binding = parse_line(binding_raw.as_bytes())?;
    fields(&binding, BINDING_FIELDS)?;
    let run = s(&binding, "run_id")?;
    ensure(
        s(&binding, "schema")? == BINDING
            && token(run)
            && token(broker)
            && s(&binding, "host")? == domain.host
            && s(&binding, "boot")? == domain.boot
            && s(&binding, "domain")? == domain.domain
            && n(&binding, "cap_revision")? == domain.cap_revision
            && s(&binding, "broker_instance")? == broker
            && n(&binding, "lifecycle_epoch")? == 0
            && s(&binding, "initial_head_sha256")? == digest(GENESIS_HEAD)
            && s(&binding, "sealed_head_sha256")? == "pending",
        "invalid active witness binding",
    )?;
    authority.fixture_verify_binding_journal_ids(
        identity(&binding, "broker_journal")?,
        identity(&binding, "broker_head")?,
    )?;
    ensure(
        (full_head.sequence == 0 && full_head.mode == Mode::Clean)
            || (full_head.sequence > 0
                && full_head.mode == Mode::Dirty
                && full_head.clean_sequence == 0
                && full_head.broker == broker),
        "active witness does not match genesis generation",
    )?;
    for frame in &frames {
        let event = ddr_recovery_codec::decode(frame).map_err(invalid)?;
        ensure(
            event.domain.host == domain.host
                && event.domain.boot == domain.boot
                && event.domain.domain == domain.domain
                && event.domain.cap_revision == domain.cap_revision
                && event.domain.owner_limit == domain.owner_limit
                && event.domain.total_limit == domain.total_limit
                && event.domain.broker == broker,
            "active witness journal generation mismatch",
        )?;
    }
    for (path, hash) in [
        ("witness_code_path", "witness_code_sha256"),
        ("interpreter_path", "interpreter_sha256"),
        ("pin_code_path", "pin_code_sha256"),
        ("child_executable_path", "child_executable_sha256"),
    ] {
        validate_code(&binding, path, hash)?;
    }
    let witness = birth(&binding["witness_birth"])?;
    let witness_fd = live_pidfd(&witness)?;
    let self_birth = ddr_process_identity::observe(
        std::process::id() as i32,
        unsafe { libc::geteuid() },
        unsafe { libc::getegid() },
    )?;
    if let Some(first) = frames.first() {
        let event = ddr_recovery_codec::decode(first).map_err(invalid)?;
        ensure(
            matches!(event.kind, crate::ddr_recovery_audit::EventKind::BrokerBirth { birth } if birth == self_birth),
            "active journal does not begin with this broker birth",
        )?;
    }
    let executable = fs::canonicalize(format!("/proc/{}/exe", witness.pid))?;
    ensure(
        executable == PathBuf::from(s(&binding, "interpreter_path")?),
        "witness interpreter mismatch",
    )?;
    let command = fs::read(format!("/proc/{}/cmdline", witness.pid))?;
    let args: Vec<&[u8]> = command.split(|byte| *byte == 0).collect();
    let script = args
        .get(1)
        .ok_or_else(|| invalid("witness has no script argument"))?;
    let script =
        std::str::from_utf8(script).map_err(|_| invalid("invalid witness script argument"))?;
    ensure(
        fs::canonicalize(script)? == PathBuf::from(s(&binding, "pin_code_path")?),
        "witness launcher mismatch",
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
    let witness_journal_id = identity(&binding, "evidence_journal")?;
    let witness_head_id = identity(&binding, "evidence_head")?;
    let head = private_file(&evidence.join("witness.head"), witness_head_id, 4096)?;
    let raw = private_file(
        &evidence.join("witness.journal"),
        witness_journal_id,
        MAX_WITNESS,
    )?;
    let head_value = parse_line(&head)?;
    fields(&head_value, &["schema", "seq", "offset", "checksum"])?;
    ensure(
        s(&head_value, "schema")? == WITNESS && n(&head_value, "offset")? == raw.len() as u64,
        "active witness head mismatch",
    )?;
    let mut previous = "0".repeat(64);
    let mut events = Vec::new();
    for (index, line) in raw.split_inclusive(|b| *b == b'\n').enumerate() {
        ensure(line.len() <= 8192, "active witness frame too large")?;
        let mut frame = parse_line(line)?;
        fields(&frame, &["schema", "seq", "previous", "event", "checksum"])?;
        ensure(
            s(&frame, "schema")? == WITNESS
                && n(&frame, "seq")? == (index + 1) as u64
                && s(&frame, "previous")? == previous,
            "active witness chain mismatch",
        )?;
        let checksum = s(&frame, "checksum")?.to_owned();
        let event = frame
            .get("event")
            .ok_or_else(|| invalid("missing active witness event"))?
            .clone();
        frame
            .as_object_mut()
            .ok_or_else(|| invalid("invalid witness frame"))?
            .remove("checksum");
        ensure(
            hex_digest(&checksum) && digest(&canonical(&frame)?) == checksum,
            "active witness checksum mismatch",
        )?;
        previous = checksum;
        events.push(event);
    }
    ensure(
        !events.is_empty()
            && events.len() as u64 == n(&head_value, "seq")?
            && previous == s(&head_value, "checksum")?,
        "active witness head changed",
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
            && birth(&start["witness_birth"])? == witness
            && s(start, "witness_code_sha256")? == s(&binding, "witness_code_sha256")?
            && s(start, "interpreter_sha256")? == s(&binding, "interpreter_sha256")?,
        "active witness start mismatch",
    )?;

    let mut children = BTreeMap::<String, Child>::new();
    let mut active_pids = BTreeMap::new();
    let mut launches = BTreeSet::new();
    for event in &events[1..] {
        ensure(
            s(event, "run_id")? == run && s(event, "boot_id")? == domain.boot,
            "active witness event generation mismatch",
        )?;
        match s(event, "type")? {
            "child_birth" => {
                fields(
                    event,
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
                let child_birth = birth(&event["birth"])?;
                let key = birth_key(&child_birth);
                let role = s(event, "role")?;
                let launch = s(event, "launch_id")?;
                ensure(
                    token(role)
                        && token(launch)
                        && launch.starts_with(&format!("{run}-"))
                        && event["pidfd_opened"] == Value::Bool(true)
                        && s(event, "executable_sha256")?
                            == s(&binding, "child_executable_sha256")?
                        && hex_digest(s(event, "argv_sha256")?)
                        && !children.contains_key(&key)
                        && !active_pids.contains_key(&child_birth.pid)
                        && launches.insert(launch.to_owned()),
                    "invalid active child birth",
                )?;
                active_pids.insert(child_birth.pid, key.clone());
                children.insert(
                    key,
                    Child {
                        birth: child_birth,
                        role: role.into(),
                        launch: launch.into(),
                        waited: false,
                    },
                );
            }
            "child_wait" => {
                fields(
                    event,
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
                let child_birth = birth(&event["birth"])?;
                let key = birth_key(&child_birth);
                let child = children
                    .get_mut(&key)
                    .ok_or_else(|| invalid("unknown waited child"))?;
                ensure(
                    !child.waited
                        && s(event, "role")? == child.role
                        && s(event, "launch_id")? == child.launch
                        && n(event, "raw_wait_status")? == 0
                        && event["normal_exit_zero"] == Value::Bool(true)
                        && active_pids.remove(&child_birth.pid).as_ref() == Some(&key),
                    "invalid active child wait",
                )?;
                child.waited = true;
            }
            "child_resource_observed" => {
                fields(
                    event,
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
                let child = children
                    .get(&birth_key(&birth(&event["birth"])?))
                    .ok_or_else(|| invalid("unknown observed child"))?;
                ensure(
                    !child.waited
                        && s(event, "role")? == child.role
                        && s(event, "launch_id")? == child.launch
                        && n(event, "ino")? > 0
                        && event["fd_observed"] == Value::Bool(true)
                        && event["mapping_observed"].is_boolean(),
                    "invalid active resource observation",
                )?;
                let _ = n(event, "dev")?;
            }
            "witness_no_backing" => {
                fields(event, &["type", "run_id", "boot_id", "dev", "ino"])?;
                ensure(n(event, "ino")? > 0, "invalid active witness resource")?;
                let _ = n(event, "dev")?;
            }
            _ => return Err(invalid("unexpected active witness event")),
        }
    }
    let broker_child = children
        .get(&birth_key(&self_birth))
        .ok_or_else(|| invalid("broker missing from witness roster"))?;
    ensure(
        !broker_child.waited && broker_child.role == "broker" && broker_child.birth == self_birth,
        "broker witness birth mismatch",
    )?;
    let child_executable = s(&binding, "child_executable_path")?;
    let broker_fd = check_live_child(&self_birth, &witness, child_executable)?;
    let peer_fd = if let Some(peer) = peer {
        let child = children
            .get(&birth_key(peer))
            .ok_or_else(|| invalid("peer missing from witness roster"))?;
        ensure(
            !child.waited
                && child.birth == *peer
                && matches!(child.role.as_str(), "owner" | "accessor" | "worker")
                && peer.pid != self_birth.pid,
            "peer witness birth or role mismatch",
        )?;
        Some(check_live_child(peer, &witness, child_executable)?)
    } else {
        None
    };
    ensure(
        private_file(&evidence.join("witness.head"), witness_head_id, 4096)? == head
            && private_file(
                &evidence.join("witness.journal"),
                witness_journal_id,
                MAX_WITNESS,
            )? == raw,
        "active witness changed during check",
    )?;
    let (mut again, binding_again, domain_again) = authority.fixture_recovery_material()?;
    ensure(
        binding_again == binding_raw
            && domain_again == domain
            && again.head() == &full_head
            && again.verify()? == frames,
        "active authority changed during check",
    )?;
    ensure_live(&witness_fd)?;
    ensure_live(&broker_fd)?;
    if let Some(fd) = peer_fd.as_ref() {
        ensure_live(fd)?;
    }
    ensure(
        ddr_process_identity::observe(witness.pid, witness.uid, witness.gid)? == witness
            && parent_pid(self_birth.pid)? == witness.pid,
        "witness relationship changed",
    )?;
    if let Some(peer) = peer {
        ensure(
            ddr_process_identity::observe(peer.pid, peer.uid, peer.gid)? == *peer
                && parent_pid(peer.pid)? == witness.pid
                && fs::canonicalize(format!("/proc/{}/exe", peer.pid))?
                    == PathBuf::from(child_executable),
            "peer relationship changed",
        )?;
    }
    Ok(())
}
