//! Persistent namespace primitive for the T3 managed external-DDR authority.
//!
//! The normal T1 lifecycle uses this fixed-root lock and dirty barrier.
//! Staged targets, probes and business copy remain separately gated; this
//! adapter does not provide dirty recovery or a global OS quota ledger.

#![allow(dead_code)] // WIP: integration and deployment entry points are pending.

use std::cell::Cell;
use std::collections::HashSet;
use std::ffi::CString;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

#[path = "ddr_trust_transaction.rs"]
mod trust_transaction;

const MANAGED_ROOT: &str = "/workspace/uflow/authority/external-ddr-v0";
const DOMAIN: &str = "external-ddr-v0";
// v3 is isolated-fixture-only. v4/v5 lack a bootstrap inode anchor and are
// parsed only to reject them without modifying their existing history.
const FORMAT: &str = "uflow-ddr-authority-v3";
const DEPLOYMENT_FORMAT: &str = "uflow-ddr-authority-v4";
const NORMAL_FORMAT: &str = "uflow-ddr-authority-v5";
const PINNED_NORMAL_FORMAT: &str = "uflow-ddr-authority-v6";
const PINNED_DEPLOYMENT_FORMAT: &str = "uflow-ddr-authority-v7";
const NORMAL_TRUST: &[u8] = b"uflow-ddr-normal-authority-v1\n";
const LOCK_FILE: &str = "authority.lock";
const MANIFEST_FILE: &str = "bootstrap";
const PENDING_MANIFEST_FILE: &str = "bootstrap.pending";
const STATE_FILE: &str = "state";
const JOURNAL_FILE: &str = "recovery.journal";
const JOURNAL_HEAD_FILE: &str = "recovery.head";
const WITNESS_TRUST_FILE: &str = "witness.trust";
const MAX_CONFIG_BYTES: u64 = 4096;
const MAX_TRUST_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StateWriteStage {
    BeforeTruncate,
    AfterTruncate,
    AfterPartialWrite,
    BeforeSync,
    AfterSync,
    BeforeDirectorySync,
    AfterDirectorySync,
    BeforeReadbackOpen,
    BeforeReadbackMetadata,
    BeforeReadbackRead,
    AfterReadback,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RestartStage {
    BeforeJournalSync,
    AfterJournalSync,
    BeforeHeadSync,
    AfterHeadSync,
    BeforeStateSync,
    AfterStateSync,
    BeforeTrustSync,
    BeforeBootstrapSync,
    BeforeDirectorySync,
    AfterDirectorySync,
    BeforeFinalVerification,
    AfterFinalVerification,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BootstrapStage {
    RootCreated,
    LockCreated,
    StateCreated,
    JournalCreated,
    HeadCreated,
    WitnessCreated,
    ComponentsSynced,
    DirectoriesSynced,
    ManifestSynced,
    Published,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FixtureRecoveryFault {
    Capacity,
    PendingBeforeTruncate,
    PendingAfterTruncate,
    PendingBeforeSync,
    PendingAfterSync,
    PendingAfterDirectorySync,
    AfterPending,
    Journal(
        crate::ddr_recovery_journal::Kind,
        crate::ddr_recovery_journal::WriteStage,
    ),
    FinalBeforeTruncate,
    FinalAfterTruncate,
    FinalBeforeSync,
    FinalAfterSync,
    FinalAfterDirectorySync,
    FinalReadback,
    SealedReadback,
}

#[cfg(test)]
impl FixtureRecoveryFault {
    fn pending_stage(self) -> Option<StateWriteStage> {
        match self {
            Self::PendingBeforeTruncate => Some(StateWriteStage::BeforeTruncate),
            Self::PendingAfterTruncate => Some(StateWriteStage::AfterTruncate),
            Self::PendingBeforeSync => Some(StateWriteStage::BeforeSync),
            Self::PendingAfterSync => Some(StateWriteStage::AfterSync),
            Self::PendingAfterDirectorySync => Some(StateWriteStage::AfterDirectorySync),
            _ => None,
        }
    }

    fn state_stage(self) -> Option<StateWriteStage> {
        match self {
            Self::FinalBeforeTruncate => Some(StateWriteStage::BeforeTruncate),
            Self::FinalAfterTruncate => Some(StateWriteStage::AfterTruncate),
            Self::FinalBeforeSync => Some(StateWriteStage::BeforeSync),
            Self::FinalAfterSync => Some(StateWriteStage::AfterSync),
            Self::FinalAfterDirectorySync => Some(StateWriteStage::AfterDirectorySync),
            _ => None,
        }
    }
}

// Only tests expecting a released lock may wait. In the multithreaded Rust
// test binary, another test can fork while this test closes an O_CLOEXEC FD;
// that child temporarily inherits the locked open-file description until exec.
// Production acquisition and tests asserting real contention stay nonblocking.
#[cfg(test)]
fn after_test_lock_release<T>(mut action: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        match action() {
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock
                    && std::time::Instant::now() < deadline =>
            {
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            result => return result,
        }
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn name_ok(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 96
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}

fn endpoint_ok(endpoint: &str) -> bool {
    endpoint.starts_with("/workspace/uflow/runs/")
        && endpoint.len() + ".external-ddr".len() < 108
        && endpoint
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"/._-".contains(&b))
        && !endpoint.split('/').any(|part| part == "." || part == "..")
}

fn require_current_boot(expected: &str) -> io::Result<()> {
    let observed = fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
    if !name_ok(expected) || observed.trim() != expected {
        return Err(invalid("authority boot does not match the running kernel"));
    }
    Ok(())
}

fn private_dir(path: &Path) -> io::Result<fs::Metadata> {
    let meta = fs::symlink_metadata(path)?;
    if !meta.is_dir() || meta.uid() != unsafe { libc::geteuid() } || meta.mode() & 0o077 != 0 {
        return Err(invalid("authority directory is not private and owned"));
    }
    Ok(meta)
}

fn private_file(path: &Path, writable: bool) -> io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(writable)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    let meta = file.metadata()?;
    if !meta.is_file()
        || meta.uid() != unsafe { libc::geteuid() }
        || meta.mode() & 0o077 != 0
        || meta.nlink() != 1
    {
        return Err(invalid(
            "authority file is not private, owned and singly linked",
        ));
    }
    Ok(file)
}

fn create_private(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
}

fn read_bounded(file: &File) -> io::Result<String> {
    read_bounded_with_limit(file, MAX_CONFIG_BYTES)
}

fn read_bounded_with_limit(mut file: &File, limit: u64) -> io::Result<String> {
    let length = file.metadata()?.len();
    if length > limit {
        return Err(invalid("authority file exceeds maximum size"));
    }
    let mut bytes = Vec::new();
    (&mut file).take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 != length || file.metadata()?.len() != length {
        return Err(invalid("authority file changed or exceeded maximum size"));
    }
    String::from_utf8(bytes).map_err(|_| invalid("authority file is not UTF-8"))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Bootstrap {
    deployed: bool,
    normal_only: bool,
    host: String,
    boot: String,
    endpoint: String,
    owner_limit: u64,
    total_limit: u64,
    revision: u64,
    dir_dev: u64,
    dir_ino: u64,
    lock_dev: u64,
    lock_ino: u64,
    state_dev: u64,
    state_ino: u64,
    journal_dev: u64,
    journal_ino: u64,
    journal_head_dev: u64,
    journal_head_ino: u64,
    witness_trust_dev: u64,
    witness_trust_ino: u64,
    // Encoded into immutable config and independently hashed by state.
    // None identifies v3 fixtures or legacy v4/v5 with insufficient evidence.
    manifest_identity: Option<(u64, u64)>,
}

impl Bootstrap {
    fn encode(&self) -> String {
        let format = if self.normal_only && self.manifest_identity.is_some() {
            PINNED_NORMAL_FORMAT
        } else if self.normal_only {
            NORMAL_FORMAT
        } else if self.deployed && self.manifest_identity.is_some() {
            PINNED_DEPLOYMENT_FORMAT
        } else if self.deployed {
            DEPLOYMENT_FORMAT
        } else {
            FORMAT
        };
        let mut text = format!(
            "{format}\ndomain={DOMAIN}\nhost={}\nboot={}\nendpoint={}\nowner_limit={}\ntotal_limit={}\nrevision={}\ndir_dev={}\ndir_ino={}\nlock_dev={}\nlock_ino={}\nstate_dev={}\nstate_ino={}\njournal_dev={}\njournal_ino={}\njournal_head_dev={}\njournal_head_ino={}\nwitness_trust_dev={}\nwitness_trust_ino={}\n",
            self.host, self.boot, self.endpoint, self.owner_limit, self.total_limit,
            self.revision, self.dir_dev, self.dir_ino, self.lock_dev, self.lock_ino,
            self.state_dev, self.state_ino, self.journal_dev, self.journal_ino,
            self.journal_head_dev, self.journal_head_ino, self.witness_trust_dev,
            self.witness_trust_ino,
        );
        if let Some((dev, ino)) = self.manifest_identity {
            text.push_str(&format!("bootstrap_dev={dev}\nbootstrap_ino={ino}\n"));
        }
        text
    }

    fn parse(text: &str) -> io::Result<Self> {
        let mut lines = text.lines();
        let (deployed, normal_only, has_manifest_identity) = match lines.next() {
            Some(FORMAT) => (false, false, false),
            Some(DEPLOYMENT_FORMAT) => (true, false, false),
            Some(NORMAL_FORMAT) => (true, true, false),
            Some(PINNED_NORMAL_FORMAT) => (true, true, true),
            Some(PINNED_DEPLOYMENT_FORMAT) => (true, false, true),
            _ => return Err(invalid("invalid authority format")),
        };
        if lines.next() != Some("domain=external-ddr-v0") {
            return Err(invalid("invalid authority format or domain"));
        }
        fn field<'a>(lines: &mut impl Iterator<Item = &'a str>, key: &str) -> io::Result<&'a str> {
            let line = lines
                .next()
                .ok_or_else(|| invalid("missing authority field"))?;
            line.strip_prefix(key)
                .ok_or_else(|| invalid("invalid authority field"))
        }
        fn num(value: &str) -> io::Result<u64> {
            value
                .parse()
                .map_err(|_| invalid("invalid authority integer"))
        }
        let value = Self {
            deployed,
            normal_only,
            host: field(&mut lines, "host=")?.to_owned(),
            boot: field(&mut lines, "boot=")?.to_owned(),
            endpoint: field(&mut lines, "endpoint=")?.to_owned(),
            owner_limit: num(field(&mut lines, "owner_limit=")?)?,
            total_limit: num(field(&mut lines, "total_limit=")?)?,
            revision: num(field(&mut lines, "revision=")?)?,
            dir_dev: num(field(&mut lines, "dir_dev=")?)?,
            dir_ino: num(field(&mut lines, "dir_ino=")?)?,
            lock_dev: num(field(&mut lines, "lock_dev=")?)?,
            lock_ino: num(field(&mut lines, "lock_ino=")?)?,
            state_dev: num(field(&mut lines, "state_dev=")?)?,
            state_ino: num(field(&mut lines, "state_ino=")?)?,
            journal_dev: num(field(&mut lines, "journal_dev=")?)?,
            journal_ino: num(field(&mut lines, "journal_ino=")?)?,
            journal_head_dev: num(field(&mut lines, "journal_head_dev=")?)?,
            journal_head_ino: num(field(&mut lines, "journal_head_ino=")?)?,
            witness_trust_dev: num(field(&mut lines, "witness_trust_dev=")?)?,
            witness_trust_ino: num(field(&mut lines, "witness_trust_ino=")?)?,
            manifest_identity: if has_manifest_identity {
                Some((
                    num(field(&mut lines, "bootstrap_dev=")?)?,
                    num(field(&mut lines, "bootstrap_ino=")?)?,
                ))
            } else {
                None
            },
        };
        if lines.next().is_some()
            || text != value.encode()
            || !name_ok(&value.host)
            || !name_ok(&value.boot)
            || !endpoint_ok(&value.endpoint)
            || value.owner_limit == 0
            || value.total_limit < value.owner_limit
            || value.revision == 0
        {
            return Err(invalid("invalid or noncanonical authority bootstrap"));
        }
        Ok(value)
    }
}

/// Genesis anchors the initial journal head, immutable config and exact trust
/// bytes in the distinct pinned state inode. This is not an anti-rollback
/// guarantee against a hostile same-UID writer.
fn deployment_genesis_state(bootstrap: &Bootstrap, trust: &[u8]) -> io::Result<String> {
    use crate::ddr_deployment_policy::DeploymentPolicy;
    use crate::ddr_recovery_coverage::{canonical, digest};
    let (policy_sha256, trust_sequence, closure) = if bootstrap.normal_only {
        if trust != NORMAL_TRUST {
            return Err(invalid("invalid normal authority marker"));
        }
        (digest(trust), 0, "normal")
    } else {
        let policy = DeploymentPolicy::from_initial_chain_structural(trust)?;
        (policy.policy_digest()?, 1, "closed")
    };
    let value = serde_json::json!({
        "schema": "uflow-ddr-authority-state-v1",
        "mode": "clean",
        "policy_sha256": policy_sha256,
        "trust_sequence": trust_sequence,
        "trust_offset": trust.len(),
        "trust_sha256": digest(trust),
        "generation": "none",
        "closure": closure,
        "revision": bootstrap.revision,
        "config_sha256": digest(bootstrap.encode().as_bytes()),
        "journal_head": crate::ddr_recovery_journal::AnchoredJournal::genesis_state(),
        "pending": null,
    });
    let mut bytes = canonical(&value)?;
    bytes.push(b'\n');
    String::from_utf8(bytes).map_err(|_| invalid("invalid deployment state"))
}

fn verify_deployment_genesis(bootstrap: &Bootstrap, trust: &[u8], state: &str) -> io::Result<()> {
    use crate::ddr_recovery_coverage::{fields, n, parse_line, s};
    use crate::ddr_trust_history::{verify, TrustAnchor};
    let value = parse_line(state.as_bytes())?;
    fields(
        &value,
        &[
            "schema",
            "mode",
            "policy_sha256",
            "trust_sequence",
            "trust_offset",
            "trust_sha256",
            "generation",
            "closure",
            "revision",
            "config_sha256",
            "journal_head",
            "pending",
        ],
    )?;
    if bootstrap.normal_only {
        if state != deployment_genesis_state(bootstrap, trust)? {
            return Err(invalid("invalid normal authority genesis"));
        }
        return Ok(());
    }
    // The independent anchor comes from the already-pinned state, not from
    // the untrusted trust bytes. Only genesis is usable by this WIP.
    let anchor = TrustAnchor {
        sequence: n(&value, "trust_sequence")?,
        offset: n(&value, "trust_offset")?,
        sha256: s(&value, "trust_sha256")?.to_owned(),
    };
    let history = verify(trust, &anchor)?;
    if !history.generations.is_empty() || state != deployment_genesis_state(bootstrap, trust)? {
        return Err(invalid("unsupported versioned deployment state"));
    }
    Ok(())
}

/// Validate the deployed anchor. Witness-bound generations remain quarantined.
/// The separate normal lifecycle never clears an inherited dirty marker: its
/// clean state pins the last semantic checkpoint, while its dirty state keeps
/// that checkpoint as a crash-conservative baseline until a joined drain.
fn verify_deployment_state(
    bootstrap: &Bootstrap,
    trust: &[u8],
    state: &str,
    journal_head: &crate::ddr_recovery_journal::Head,
) -> io::Result<bool> {
    use crate::ddr_recovery_coverage::{digest, fields, n, parse_line, s};
    use crate::ddr_recovery_journal::Mode;
    use crate::ddr_trust_history::{verify, TrustAnchor, TrustConfig};
    let value = parse_line(state.as_bytes())?;
    fields(
        &value,
        &[
            "schema",
            "mode",
            "policy_sha256",
            "trust_sequence",
            "trust_offset",
            "trust_sha256",
            "generation",
            "closure",
            "revision",
            "config_sha256",
            "journal_head",
            "pending",
        ],
    )?;
    if s(&value, "schema")? != "uflow-ddr-authority-state-v1"
        || !value["pending"].is_null()
        || n(&value, "revision")? != bootstrap.revision
        || s(&value, "config_sha256")? != digest(bootstrap.encode().as_bytes())
    {
        return Err(invalid("invalid or pending versioned authority state"));
    }
    // A normal-only root has no witness policy. A policy-backed genesis/normal
    // closure keeps its anchored DEPLOY bytes without requiring witness
    // executables to be present to serve ordinary cooperative clients.
    let closure = s(&value, "closure")?;
    if closure == "normal" || closure == "closed" {
        let mut expected = parse_line(deployment_genesis_state(bootstrap, trust)?.as_bytes())?;
        if closure == "closed" {
            if bootstrap.normal_only
                || value != expected
                || journal_head.encode()
                    != crate::ddr_recovery_journal::AnchoredJournal::genesis_state()
            {
                return Err(invalid("invalid closed deployment genesis"));
            }
            return Ok(false);
        }
        expected["closure"] = "normal".into();
        expected["mode"] = value["mode"].clone();
        expected["journal_head"] = value["journal_head"].clone();
        let checkpoint = crate::ddr_recovery_journal::Head::parse(s(&value, "journal_head")?)?;
        if value != expected
            || checkpoint.mode != Mode::Clean
            || checkpoint.clean_sequence != checkpoint.sequence
        {
            return Err(invalid("invalid normal authority anchor"));
        }
        return match s(&value, "mode")? {
            "clean" if journal_head == &checkpoint => Ok(false),
            "dirty" if journal_head.sequence >= checkpoint.sequence => Ok(true),
            _ => Err(invalid("normal authority and journal head disagree")),
        };
    }
    if bootstrap.normal_only {
        return Err(invalid(
            "normal-only authority cannot enter witness lifecycle",
        ));
    }
    let anchor = TrustAnchor {
        sequence: n(&value, "trust_sequence")?,
        offset: n(&value, "trust_offset")?,
        sha256: s(&value, "trust_sha256")?.to_owned(),
    };
    let history = verify(trust, &anchor)?;
    if s(&value, "policy_sha256")? != history.policy_digest {
        return Err(invalid("versioned state policy mismatch"));
    }
    let last = history
        .generations
        .last()
        .ok_or_else(|| invalid("missing witness generation"))?;
    let expected_mode = match journal_head.mode {
        Mode::Clean => "clean",
        Mode::Dirty => "dirty",
    };
    if s(&value, "journal_head")? != journal_head.encode() || s(&value, "mode")? != expected_mode {
        return Err(invalid("versioned state and journal head differ"));
    }
    let expected_config = TrustConfig {
        host: bootstrap.host.clone(),
        boot: bootstrap.boot.clone(),
        domain: DOMAIN.into(),
        revision: bootstrap.revision,
        owner_limit: bootstrap.owner_limit,
        total_limit: bootstrap.total_limit,
        endpoint: bootstrap.endpoint.clone(),
        journal: (bootstrap.journal_dev, bootstrap.journal_ino),
        journal_head: (bootstrap.journal_head_dev, bootstrap.journal_head_ino),
    };
    if history.config.as_ref() != Some(&expected_config)
        || s(&value, "generation")? != last.generation
    {
        return Err(invalid("versioned binding and fixed authority disagree"));
    }
    match &last.final_head {
        Some(final_head) if closure == "sealed" && final_head == journal_head => {}
        None if matches!(closure, "bound" | "closing")
            && (journal_head == &last.initial_head
                || (journal_head.sequence > last.initial_head.sequence
                    && journal_head.broker == last.broker)) => {}
        _ => {
            return Err(invalid(
                "versioned generation closure or journal head mismatch",
            ))
        }
    }
    Ok(true)
}

/// In-process poison is keyed by the original directory identity and fixed
/// path, and survives dropping/reopening an authority or reconstructing a catalog.
/// It is deliberately not a cross-process restart interlock.
struct ProcessPoison {
    local: Cell<bool>,
    identity: Option<(u32, u64, u64)>,
    path: Option<(u32, PathBuf)>,
}

fn process_poisoned_domains() -> &'static Mutex<HashSet<(u32, u64, u64)>> {
    static DOMAINS: OnceLock<Mutex<HashSet<(u32, u64, u64)>>> = OnceLock::new();
    DOMAINS.get_or_init(|| Mutex::new(HashSet::new()))
}

fn process_poisoned_paths() -> &'static Mutex<HashSet<(u32, PathBuf)>> {
    static PATHS: OnceLock<Mutex<HashSet<(u32, PathBuf)>>> = OnceLock::new();
    PATHS.get_or_init(|| Mutex::new(HashSet::new()))
}

impl ProcessPoison {
    fn new(root: &Path, identity: Option<(u64, u64)>, deployed: bool) -> Self {
        Self {
            local: Cell::new(false),
            identity: if deployed {
                identity.map(|(dev, ino)| (std::process::id(), dev, ino))
            } else {
                None
            },
            path: deployed.then(|| (std::process::id(), root.to_owned())),
        }
    }

    fn get(&self) -> bool {
        self.local.get()
            || self.identity.is_some_and(|identity| {
                process_poisoned_domains()
                    .lock()
                    .map_or(true, |domains| domains.contains(&identity))
            })
            || self.path.as_ref().is_some_and(|path| {
                process_poisoned_paths()
                    .lock()
                    .map_or(true, |paths| paths.contains(path))
            })
    }

    fn set(&self, poisoned: bool) {
        if poisoned {
            self.local.set(true);
            if let (Some(identity), Ok(mut domains)) =
                (self.identity, process_poisoned_domains().lock())
            {
                domains.insert(identity);
            }
            if let (Some(path), Ok(mut paths)) = (&self.path, process_poisoned_paths().lock()) {
                paths.insert(path.clone());
            }
        }
    }
}

/// Kept private to the daemon: this lock must outlive every handler and worker.
pub(crate) struct ManagedAuthority {
    root: PathBuf,
    lock: File,
    bootstrap: Bootstrap,
    bootstrapped_text: String,
    // New deployed formats derive this from the independently state-anchored
    // config. Only isolated v3 fixtures use the identity observed at open.
    bootstrap_identity: (u64, u64),
    deployed_state: Option<String>,
    /// A dirty state observed on opening means unknown legacy commitments.
    quarantined: bool,
    /// A dirty state written by this live authority is distinct from quarantine.
    dirty: bool,
    /// Separates this fixture transaction from a later dirty generation.
    recovery_marker: Option<String>,
    /// Once a live namespace/admission barrier fails, restoring the path or
    /// bytes must not re-enable this authority or its clean shortcut.
    admission_poisoned: ProcessPoison,
}

/// The caller must derive these counts from the authoritative catalog only
/// after it has stopped accepting work and joined all handlers and workers.
#[derive(Debug, Default)]
pub(crate) struct DrainedCommitments {
    pub accepting_stopped: bool,
    pub handlers: u64,
    pub retained_backings: u64,
    pub views: u64,
    pub pending: u64,
    pub reservations: u64,
    pub in_flight: u64,
}

impl DrainedCommitments {
    fn is_empty(&self) -> bool {
        self.accepting_stopped
            && self.handlers == 0
            && self.retained_backings == 0
            && self.views == 0
            && self.pending == 0
            && self.reservations == 0
            && self.in_flight == 0
    }
}

impl ManagedAuthority {
    /// Task-isolated test authority only; never accepts a root override in the
    /// compiled production deployment or startup paths.
    #[cfg(test)]
    pub(crate) fn deployed_live_fixture(
        root: &Path,
        boot: &str,
        endpoint: &str,
        policy: &crate::ddr_deployment_policy::DeploymentPolicy,
    ) -> io::Result<Self> {
        require_current_boot(boot)?;
        initialize_with_policy_at_inner(
            root,
            "host",
            boot,
            endpoint,
            16,
            64,
            Some(policy),
            |_| Ok(()),
        )?;
        Self::open_at(root, "host", boot, endpoint, 16, 64)
    }

    #[cfg(test)]
    pub(crate) fn reopen_deployed_live_fixture(
        root: &Path,
        boot: &str,
        endpoint: &str,
    ) -> io::Result<Self> {
        require_current_boot(boot)?;
        after_test_lock_release(|| Self::open_at(root, "host", boot, endpoint, 16, 64))
    }

    #[cfg(test)]
    pub(crate) fn initialize_fixture(root: &Path, endpoint: &str) -> io::Result<()> {
        initialize_at(root, "fixture-host", "fixture-boot", endpoint, 8192, 8192)
    }

    #[cfg(test)]
    pub(crate) fn open_fixture(root: &Path, endpoint: &str) -> io::Result<Self> {
        Self::open_at(root, "fixture-host", "fixture-boot", endpoint, 8192, 8192)
    }

    #[cfg(test)]
    pub(crate) fn open_fixture_after_release(root: &Path, endpoint: &str) -> io::Result<Self> {
        after_test_lock_release(|| Self::open_fixture(root, endpoint))
    }

    /// Real-boot variant exclusively for the pinned, cross-process fixture.
    /// Neither helper is compiled into the production daemon.
    #[cfg(test)]
    pub(crate) fn initialize_boot_fixture(
        root: &Path,
        endpoint: &str,
        boot: &str,
    ) -> io::Result<()> {
        require_current_boot(boot)?;
        initialize_at(root, "fixture-host", boot, endpoint, 8192, 8192)
    }

    #[cfg(test)]
    pub(crate) fn open_boot_fixture(root: &Path, endpoint: &str, boot: &str) -> io::Result<Self> {
        require_current_boot(boot)?;
        after_test_lock_release(|| Self::open_at(root, "fixture-host", boot, endpoint, 8192, 8192))
    }

    /// Trusted deployment only; an existing or partially initialized root is
    /// never overwritten. The parent must already be securely provisioned.
    pub(crate) fn initialize_production(
        host: &str,
        boot: &str,
        endpoint: &str,
        owner_limit: u64,
        total_limit: u64,
        policy: &crate::ddr_deployment_policy::DeploymentPolicy,
    ) -> io::Result<()> {
        require_current_boot(boot)?;
        initialize_with_policy_at_inner(
            Path::new(MANAGED_ROOT),
            host,
            boot,
            endpoint,
            owner_limit,
            total_limit,
            Some(policy),
            |_| Ok(()),
        )
    }

    /// Ordinary bootstrap uses the same fixed root and requires no witness.
    pub(crate) fn initialize_normal_production(
        host: &str,
        boot: &str,
        endpoint: &str,
        owner_limit: u64,
        total_limit: u64,
    ) -> io::Result<()> {
        require_current_boot(boot)?;
        initialize_at_mode_inner(
            Path::new(MANAGED_ROOT),
            host,
            boot,
            endpoint,
            owner_limit,
            total_limit,
            None,
            true,
            |_| Ok(()),
        )
    }

    /// Ordinary production startup cannot select another authority namespace.
    pub(crate) fn open_production(
        host: &str,
        boot: &str,
        endpoint: &str,
        owner_limit: u64,
        total_limit: u64,
    ) -> io::Result<Self> {
        require_current_boot(boot)?;
        let authority = Self::open_at(
            Path::new(MANAGED_ROOT),
            host,
            boot,
            endpoint,
            owner_limit,
            total_limit,
        )?;
        if !authority.bootstrap.deployed {
            return Err(invalid(
                "legacy fixture authority is not a deployed authority",
            ));
        }
        Ok(authority)
    }

    fn open_at(
        root: &Path,
        host: &str,
        boot: &str,
        endpoint: &str,
        owner_limit: u64,
        total_limit: u64,
    ) -> io::Result<Self> {
        // Remember failures before the lock/state can be opened, even when
        // the directory is temporarily missing. Mere lock contention is not
        // a storage failure and must not poison the lock holder.
        let identity = fs::symlink_metadata(root)
            .ok()
            .filter(|m| m.is_dir())
            .map(|m| (m.dev(), m.ino()));
        let poison = ProcessPoison::new(root, identity, true);
        let result = (|| {
            if !root.is_absolute() || fs::canonicalize(root)? != root {
                return Err(invalid("authority root must be the canonical fixed path"));
            }
            private_dir(root)?;
            let lock = private_file(&root.join(LOCK_FILE), true)?;
            if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
                return Err(io::Error::last_os_error());
            }
            Self::open_at_held_lock(root, host, boot, endpoint, owner_limit, total_limit, lock)
        })();
        if let Err(error) = &result {
            if error.kind() != io::ErrorKind::WouldBlock {
                poison.set(true);
            }
        }
        result
    }

    /// Revalidate all pinned metadata using the description newly locked by
    /// `open_at`. The inherited-FD caller below is only an OS primitive test;
    /// production must never inherit the deployment actor's flock description.
    fn open_at_held_lock(
        root: &Path,
        host: &str,
        boot: &str,
        endpoint: &str,
        owner_limit: u64,
        total_limit: u64,
        lock: File,
    ) -> io::Result<Self> {
        Self::open_at_held_lock_with_hook(
            root,
            host,
            boot,
            endpoint,
            owner_limit,
            total_limit,
            lock,
            |_| Ok(()),
        )
    }

    fn open_at_held_lock_with_hook(
        root: &Path,
        host: &str,
        boot: &str,
        endpoint: &str,
        owner_limit: u64,
        total_limit: u64,
        lock: File,
        hook: impl FnMut(RestartStage) -> io::Result<()>,
    ) -> io::Result<Self> {
        // The root identity is available before parsing state and constructing
        // an authority. An early read/validation failure must still poison a
        // later reconstruction in this same process. A different process does
        // not inherit this in-memory record and must independently verify C/R.
        let dir = private_dir(root)?;
        let poison = ProcessPoison::new(root, Some((dir.dev(), dir.ino())), true);
        let result = Self::open_at_held_lock_checked(
            root,
            host,
            boot,
            endpoint,
            owner_limit,
            total_limit,
            lock,
            hook,
        );
        if result.is_err() {
            poison.set(true);
        }
        result
    }

    fn open_at_held_lock_checked(
        root: &Path,
        host: &str,
        boot: &str,
        endpoint: &str,
        owner_limit: u64,
        total_limit: u64,
        lock: File,
        hook: impl FnMut(RestartStage) -> io::Result<()>,
    ) -> io::Result<Self> {
        if !root.is_absolute() || fs::canonicalize(root)? != root {
            return Err(invalid("authority root must be the canonical fixed path"));
        }
        let dir = private_dir(root)?;
        let named_lock = private_file(&root.join(LOCK_FILE), false)?.metadata()?;
        let held = lock.metadata()?;
        if !held.is_file()
            || held.uid() != unsafe { libc::geteuid() }
            || held.mode() & 0o077 != 0
            || held.nlink() != 1
            || (held.dev(), held.ino()) != (named_lock.dev(), named_lock.ino())
        {
            return Err(invalid("held lock does not match the pinned namespace"));
        }
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let bootstrap_file = private_file(&root.join(MANIFEST_FILE), false)?;
        let bootstrap_meta = bootstrap_file.metadata()?;
        let text = read_bounded(&bootstrap_file)?;
        let bootstrap = Bootstrap::parse(&text)?;
        let observed_bootstrap = (bootstrap_meta.dev(), bootstrap_meta.ino());
        if bootstrap.deployed && bootstrap.manifest_identity.is_none() {
            return Err(invalid("legacy authority lacks bootstrap inode anchor"));
        }
        if bootstrap
            .manifest_identity
            .is_some_and(|identity| identity != observed_bootstrap)
        {
            return Err(invalid("authority bootstrap inode mismatch"));
        }
        let lock_meta = lock.metadata()?;
        if (bootstrap.dir_dev, bootstrap.dir_ino) != (dir.dev(), dir.ino())
            || (bootstrap.lock_dev, bootstrap.lock_ino) != (lock_meta.dev(), lock_meta.ino())
            || bootstrap.host != host
            || bootstrap.boot != boot
            || bootstrap.endpoint != endpoint
            || bootstrap.owner_limit != owner_limit
            || bootstrap.total_limit != total_limit
        {
            return Err(invalid(
                "authority identity, endpoint, quota or inode mismatch",
            ));
        }
        let state_file = private_file(&root.join(STATE_FILE), false)?;
        let state_meta = state_file.metadata()?;
        if (state_meta.dev(), state_meta.ino()) != (bootstrap.state_dev, bootstrap.state_ino) {
            return Err(invalid("authority state inode mismatch"));
        }
        let state = read_bounded(&state_file)?;
        let (quarantined, recovery_marker) = match state.as_str() {
            _ if bootstrap.deployed => (false, None), // verified below before use
            "clean\n" => (false, None),
            "dirty\n" => (true, None),
            #[cfg(test)]
            text if text.starts_with("uflow-ddr-fixture-recovery-pending-v1\n") => {
                (true, Some(state.clone()))
            }
            _ => return Err(invalid("authority state missing or corrupt")),
        };
        let deployed_state = bootstrap.deployed.then(|| state.clone());
        let process_poison = ProcessPoison::new(
            root,
            Some((bootstrap.dir_dev, bootstrap.dir_ino)),
            bootstrap.deployed,
        );
        let mut result = Self {
            root: root.to_owned(),
            lock,
            bootstrap,
            bootstrapped_text: text,
            bootstrap_identity: observed_bootstrap,
            deployed_state,
            quarantined,
            dirty: quarantined,
            recovery_marker,
            admission_poisoned: process_poison,
        };
        if result.bootstrap.deployed {
            let witness = private_file(&result.root.join(WITNESS_TRUST_FILE), false)?;
            let meta = witness.metadata()?;
            if (meta.dev(), meta.ino())
                != (
                    result.bootstrap.witness_trust_dev,
                    result.bootstrap.witness_trust_ino,
                )
            {
                return Err(invalid("witness trust inode changed"));
            }
            let trust = read_bounded_with_limit(&witness, MAX_TRUST_BYTES)?;
            let journal = result.open_journal()?;
            let inherited = verify_deployment_state(
                &result.bootstrap,
                trust.as_bytes(),
                &state,
                journal.head(),
            )?;
            let normal = crate::ddr_recovery_coverage::s(
                &crate::ddr_recovery_coverage::parse_line(state.as_bytes())?,
                "closure",
            )? == "normal";
            if inherited && !normal {
                use crate::ddr_recovery_coverage::{n, parse_line, s};
                use crate::ddr_trust_history::TrustAnchor;
                let value = parse_line(state.as_bytes())?;
                let anchor = TrustAnchor {
                    sequence: n(&value, "trust_sequence")?,
                    offset: n(&value, "trust_offset")?,
                    sha256: s(&value, "trust_sha256")?.to_owned(),
                };
                let history = crate::ddr_trust_history::verify(trust.as_bytes(), &anchor)?;
                // Even a semantically consistent history remains quarantined;
                // this does not validate actual witness evidence or recovery.
                crate::ddr_recovery_durable::inspect_versioned(journal, &history)?;
            }
            result.quarantined = inherited;
            result.dirty = inherited;
        }
        result.ensure_namespace()?;
        if result.bootstrap.deployed && !result.quarantined && !result.admission_poisoned.get() {
            if let Err(error) = result.confirm_normal_restart(hook) {
                result.admission_poisoned.set(true);
                return Err(error);
            }
        }
        #[cfg(test)]
        if !result.dirty {
            let mut journal = result.open_journal()?;
            if journal.verify()?.iter().any(|frame| {
                matches!(
                    frame.kind,
                    crate::ddr_recovery_journal::Kind::RecoveryAudit
                        | crate::ddr_recovery_journal::Kind::RecoveryCommit
                )
            }) {
                // A syntactically clean head is insufficient: replay the
                // pinned original evidence and exact sealed transaction.
                crate::ddr_recovery_fixture::verify_sealed(&result, None)?;
                journal.fixture_sync_recovery()?;
                private_file(&result.root.join(STATE_FILE), true)?.sync_all()?;
            }
        }
        Ok(result)
    }

    /// Independently confirm the last normal clean on every deployed startup.
    /// This is not a dirty recovery operation: no state is created or changed.
    /// The same exclusive lock is held throughout replay, real sync barriers,
    /// and the final fresh namespace/content/replay verification.
    fn confirm_normal_restart(
        &self,
        mut hook: impl FnMut(RestartStage) -> io::Result<()>,
    ) -> io::Result<()> {
        self.ensure_namespace()?;
        let mut journal = self.open_journal()?;
        let original_head = journal.head().clone();
        let domain = crate::ddr_recovery_audit::Domain {
            host: self.bootstrap.host.clone(),
            boot: self.bootstrap.boot.clone(),
            domain: DOMAIN.into(),
            cap_revision: self.bootstrap.revision,
            broker: original_head.broker.clone(),
            owner_limit: self.bootstrap.owner_limit,
            total_limit: self.bootstrap.total_limit,
        };
        let original_frames =
            crate::ddr_recovery_durable::verify_normal_restart(&mut journal, &domain)?;
        let pinned = |name: &str, expected: (u64, u64)| -> io::Result<File> {
            let file = private_file(&self.root.join(name), true)?;
            let meta = file.metadata()?;
            if (meta.dev(), meta.ino()) != expected {
                return Err(invalid("restart sync file inode changed"));
            }
            Ok(file)
        };
        let journal_file = pinned(
            JOURNAL_FILE,
            (self.bootstrap.journal_dev, self.bootstrap.journal_ino),
        )?;
        let head_file = pinned(
            JOURNAL_HEAD_FILE,
            (
                self.bootstrap.journal_head_dev,
                self.bootstrap.journal_head_ino,
            ),
        )?;
        let state_file = pinned(
            STATE_FILE,
            (self.bootstrap.state_dev, self.bootstrap.state_ino),
        )?;
        let trust_file = pinned(
            WITNESS_TRUST_FILE,
            (
                self.bootstrap.witness_trust_dev,
                self.bootstrap.witness_trust_ino,
            ),
        )?;
        let bootstrap_file = pinned(MANIFEST_FILE, self.bootstrap_identity)?;
        let directory = File::open(&self.root)?;
        let dir_meta = directory.metadata()?;
        if (dir_meta.dev(), dir_meta.ino()) != (self.bootstrap.dir_dev, self.bootstrap.dir_ino) {
            return Err(invalid("restart sync directory changed"));
        }
        hook(RestartStage::BeforeJournalSync)?;
        journal_file.sync_all()?;
        hook(RestartStage::AfterJournalSync)?;
        hook(RestartStage::BeforeHeadSync)?;
        head_file.sync_all()?;
        hook(RestartStage::AfterHeadSync)?;
        hook(RestartStage::BeforeStateSync)?;
        state_file.sync_all()?;
        hook(RestartStage::AfterStateSync)?;
        hook(RestartStage::BeforeTrustSync)?;
        trust_file.sync_all()?;
        hook(RestartStage::BeforeBootstrapSync)?;
        bootstrap_file.sync_all()?;
        hook(RestartStage::BeforeDirectorySync)?;
        directory.sync_all()?;
        hook(RestartStage::AfterDirectorySync)?;
        hook(RestartStage::BeforeFinalVerification)?;
        self.ensure_namespace()?;
        let mut reread = self.open_journal()?;
        if reread.head() != &original_head
            || crate::ddr_recovery_durable::verify_normal_restart(&mut reread, &domain)?
                != original_frames
        {
            return Err(invalid(
                "normal restart history changed during confirmation",
            ));
        }
        self.ensure_namespace()?;
        hook(RestartStage::AfterFinalVerification)?;
        Ok(())
    }

    fn ensure_static_namespace(&self) -> io::Result<()> {
        let dir = private_dir(&self.root)?;
        let lock = private_file(&self.root.join(LOCK_FILE), false)?;
        let lock_meta = lock.metadata()?;
        let held = self.lock.metadata()?;
        let bootstrap = private_file(&self.root.join(MANIFEST_FILE), false)?;
        let bootstrap_meta = bootstrap.metadata()?;
        if (dir.dev(), dir.ino()) != (self.bootstrap.dir_dev, self.bootstrap.dir_ino)
            || (lock_meta.dev(), lock_meta.ino())
                != (self.bootstrap.lock_dev, self.bootstrap.lock_ino)
            || (held.dev(), held.ino()) != (self.bootstrap.lock_dev, self.bootstrap.lock_ino)
            || held.nlink() != 1
            || (bootstrap_meta.dev(), bootstrap_meta.ino()) != self.bootstrap_identity
            || read_bounded(&bootstrap)? != self.bootstrapped_text
        {
            return Err(invalid("authority namespace changed"));
        }
        Ok(())
    }

    /// Validate the pinned entries again before every new admission. A replaced
    /// or missing entry stops new work but does not release this locked inode.
    fn ensure_namespace(&self) -> io::Result<()> {
        self.ensure_static_namespace()?;
        let state_file = private_file(&self.root.join(STATE_FILE), false)?;
        let state_meta = state_file.metadata()?;
        if (state_meta.dev(), state_meta.ino())
            != (self.bootstrap.state_dev, self.bootstrap.state_ino)
        {
            return Err(invalid("authority state inode changed"));
        }
        let state = read_bounded(&state_file)?;
        let journal = self.open_journal()?;
        let witness_trust = private_file(&self.root.join(WITNESS_TRUST_FILE), false)?;
        let witness_meta = witness_trust.metadata()?;
        if (witness_meta.dev(), witness_meta.ino())
            != (
                self.bootstrap.witness_trust_dev,
                self.bootstrap.witness_trust_ino,
            )
        {
            return Err(invalid("witness trust inode changed"));
        }
        if self.bootstrap.deployed {
            if self.deployed_state.as_deref() != Some(state.as_str()) {
                return Err(invalid("versioned authority durable state changed"));
            }
            let trust = read_bounded_with_limit(&witness_trust, MAX_TRUST_BYTES)?;
            let inherited =
                verify_deployment_state(&self.bootstrap, trust.as_bytes(), &state, journal.head())?;
            let normal = crate::ddr_recovery_coverage::s(
                &crate::ddr_recovery_coverage::parse_line(state.as_bytes())?,
                "closure",
            )? == "normal";
            let expected = self.quarantined || (normal && self.dirty);
            if inherited != expected {
                return Err(invalid("versioned quarantine state changed"));
            }
        } else {
            if !self.dirty && journal.head().mode != crate::ddr_recovery_journal::Mode::Clean {
                return Err(invalid("clean authority has a dirty recovery head"));
            }
            let expected = self.recovery_marker.as_deref().unwrap_or(if self.dirty {
                "dirty\n"
            } else {
                "clean\n"
            });
            if state != expected {
                return Err(invalid("authority durable state changed"));
            }
        }
        Ok(())
    }

    /// The lock and bootstrap pin the recovery files. Framing and inode checks
    /// alone do not validate semantic replay or authorize dirty recovery.
    fn open_journal(&self) -> io::Result<crate::ddr_recovery_journal::AnchoredJournal> {
        crate::ddr_recovery_journal::AnchoredJournal::open(
            &self.root.join(JOURNAL_FILE),
            &self.root.join(JOURNAL_HEAD_FILE),
            (self.bootstrap.journal_dev, self.bootstrap.journal_ino),
            (
                self.bootstrap.journal_head_dev,
                self.bootstrap.journal_head_ino,
            ),
        )
    }

    /// Read-only deployment inspection. The original fixed lock and state
    /// select the complete trust history and every evidence inode; the caller
    /// cannot provide an evidence path or an expected manifest. Success is
    /// only a pinned-roster inspection, NOT authorization or recovery.
    pub(crate) fn inspect_sealed_evidence(
        &self,
    ) -> io::Result<Vec<crate::ddr_witness_evidence::VerifiedRoster>> {
        if !self.bootstrap.deployed || self.admission_poisoned.get() {
            return Err(invalid("versioned deployment inspection unavailable"));
        }
        self.ensure_namespace()?;
        let state = self
            .deployed_state
            .as_ref()
            .ok_or_else(|| invalid("missing versioned deployment state"))?;
        let state = crate::ddr_recovery_coverage::parse_line(state.as_bytes())?;
        let trust = private_file(&self.root.join(WITNESS_TRUST_FILE), false)?;
        let trust = read_bounded_with_limit(&trust, MAX_TRUST_BYTES)?;
        let anchor = crate::ddr_trust_history::TrustAnchor {
            sequence: crate::ddr_recovery_coverage::n(&state, "trust_sequence")?,
            offset: crate::ddr_recovery_coverage::n(&state, "trust_offset")?,
            sha256: crate::ddr_recovery_coverage::s(&state, "trust_sha256")?.to_owned(),
        };
        let history = crate::ddr_trust_history::verify(trust.as_bytes(), &anchor)?;
        let initial = trust
            .as_bytes()
            .split_inclusive(|byte| *byte == b'\n')
            .next()
            .ok_or_else(|| invalid("missing deployment policy"))?;
        let policy = crate::ddr_deployment_policy::DeploymentPolicy::from_initial_chain(initial)?;
        crate::ddr_recovery_durable::inspect_versioned(self.open_journal()?, &history)?;
        let rosters = crate::ddr_witness_evidence::verify_sealed_history(&policy, &history)?;
        self.ensure_namespace()?;
        Ok(rosters)
    }

    /// An inherited dirty generation can be held for diagnostics but cannot
    /// supply a live writer or authorize new commitments.
    pub(crate) fn is_inherited_quarantine(&self) -> bool {
        self.quarantined
    }

    pub(crate) fn poison_live_admission(&self) {
        self.admission_poisoned.set(true);
    }

    /// Poison denies new effects, but already-held objects may still follow a
    /// valid pinned cleanup path. This is not permission to clear quarantine.
    pub(crate) fn cleanup_namespace_valid(&self) -> bool {
        !self.quarantined && self.ensure_namespace().is_ok()
    }

    pub(crate) fn matches_runtime_config(
        &self,
        host: &str,
        boot: &str,
        owner_limit: u64,
        total_limit: u64,
    ) -> bool {
        self.bootstrap.host == host
            && self.bootstrap.boot == boot
            && self.bootstrap.owner_limit == owner_limit
            && self.bootstrap.total_limit == total_limit
    }

    /// Create the only live lifecycle writer from this lock's pinned files.
    /// An inherited dirty generation, unsupported recovery suffix or budget
    /// revision is rejected by the state check or strict normal-history replay.
    /// Creating a writer is read-only; it does not by itself authorize work.
    pub(crate) fn start_lifecycle_audit(
        &self,
        broker: &str,
    ) -> io::Result<crate::ddr_recovery_durable::DurableAudit> {
        if self.quarantined || self.dirty || self.admission_poisoned.get() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "dirty or poisoned authority requires trusted recovery",
            ));
        }
        let result = (|| {
            self.ensure_namespace()?;
            let domain = crate::ddr_recovery_audit::Domain {
                host: self.bootstrap.host.clone(),
                boot: self.bootstrap.boot.clone(),
                domain: DOMAIN.into(),
                cap_revision: self.bootstrap.revision,
                broker: broker.to_owned(),
                owner_limit: self.bootstrap.owner_limit,
                total_limit: self.bootstrap.total_limit,
            };
            crate::ddr_recovery_durable::DurableAudit::start(self.open_journal()?, domain)
        })();
        if result.is_err() {
            self.admission_poisoned.set(true);
        }
        result
    }

    #[cfg(test)]
    pub(crate) fn fixture_pinned_journal(
        &self,
    ) -> io::Result<crate::ddr_recovery_journal::AnchoredJournal> {
        self.ensure_namespace()?;
        self.open_journal()
    }

    /// Read-only pinned material; no caller selects an evidence path, holder
    /// list or exit assertion. A production recovery entry is not enabled.
    pub(crate) fn fixture_coverage_material(
        &self,
    ) -> io::Result<(
        crate::ddr_recovery_journal::AnchoredJournal,
        String,
        crate::ddr_recovery_audit::Domain,
    )> {
        if !self.quarantined {
            return Err(invalid("coverage requires an inherited dirty fixture"));
        }
        self.fixture_recovery_material()
    }

    /// Internal readback seam for a fully sealed recovery transaction. It
    /// supplies no evidence selection and cannot itself authorize any write.
    pub(crate) fn fixture_recovery_material(
        &self,
    ) -> io::Result<(
        crate::ddr_recovery_journal::AnchoredJournal,
        String,
        crate::ddr_recovery_audit::Domain,
    )> {
        self.ensure_namespace()?;
        let journal = self.open_journal()?;
        let binding = read_bounded(&private_file(&self.root.join(WITNESS_TRUST_FILE), false)?)?;
        let domain = crate::ddr_recovery_audit::Domain {
            host: self.bootstrap.host.clone(),
            boot: self.bootstrap.boot.clone(),
            domain: DOMAIN.into(),
            cap_revision: self.bootstrap.revision,
            broker: journal.head().broker.clone(),
            owner_limit: self.bootstrap.owner_limit,
            total_limit: self.bootstrap.total_limit,
        };
        Ok((journal, binding, domain))
    }

    #[cfg(test)]
    pub(crate) fn verify_fixture_coverage(
        &self,
    ) -> io::Result<crate::ddr_recovery_coverage::CoverageReport> {
        crate::ddr_recovery_coverage::verify(self)
    }

    /// The one isolated deployment recovery entry. A caller cannot supply
    /// evidence paths, holders, drain/exit assertions or force. Acquiring this
    /// exact lock fails while any old or quarantined daemon still holds it.
    #[cfg(test)]
    pub(crate) fn recover_quarantined_fixture(
        root: &Path,
        endpoint: &str,
        boot: &str,
        recovery_id: &str,
    ) -> io::Result<crate::ddr_recovery_fixture::RecoveryResult> {
        require_current_boot(boot)?;
        if !root.starts_with("/workspace/uflow/runs/") {
            return Err(invalid("recovery is restricted to isolated run fixtures"));
        }
        let mut authority = Self::open_at(root, "fixture-host", boot, endpoint, 8192, 8192)?;
        authority.recover_fixture_locked(recovery_id)
    }

    #[cfg(test)]
    pub(crate) fn recover_fixture_locked(
        &mut self,
        recovery_id: &str,
    ) -> io::Result<crate::ddr_recovery_fixture::RecoveryResult> {
        self.recover_fixture_locked_impl(recovery_id, None)
    }

    /// Test-only injected errors exercise real transaction boundaries; a
    /// failpoint can never authorize recovery or supply evidence.
    #[cfg(test)]
    pub(crate) fn recover_fixture_fault_locked(
        &mut self,
        recovery_id: &str,
        fault: FixtureRecoveryFault,
    ) -> io::Result<crate::ddr_recovery_fixture::RecoveryResult> {
        self.recover_fixture_locked_impl(recovery_id, Some(fault))
    }

    /// Every failed validation of persisted recovery materials poisons the
    /// current locked instance, even before a transaction marker exists or
    /// after the marker has been cleared. Input-only rejections do not.
    #[cfg(test)]
    fn recovery_checked<T>(&self, result: io::Result<T>) -> io::Result<T> {
        if result.is_err() {
            self.admission_poisoned.set(true);
        }
        result
    }

    #[cfg(test)]
    fn recover_fixture_locked_impl(
        &mut self,
        recovery_id: &str,
        fault: Option<FixtureRecoveryFault>,
    ) -> io::Result<crate::ddr_recovery_fixture::RecoveryResult> {
        use crate::ddr_recovery_journal::{Kind, Mode};
        if self.admission_poisoned.get() {
            return Err(invalid("poisoned authority cannot recover"));
        }
        self.recovery_checked(self.ensure_namespace())?;
        let mut journal = self.recovery_checked(self.open_journal())?;
        if journal.head().mode == Mode::Clean {
            // An interrupted transaction may only finish when its entire
            // original input, evidence, audit, commit and checkpoint match.
            // Verify ALL persisted material before treating a wrong caller id
            // as an ordinary read-only rejection, so a wrong id cannot mask a
            // corrupt or missing witness and bypass the poison barrier.
            let result =
                self.recovery_checked(crate::ddr_recovery_fixture::verify_sealed(self, None))?;
            if result.recovery_id != recovery_id {
                return Err(invalid("recovery id does not match sealed transaction"));
            }
            if self.dirty {
                let expected = crate::ddr_recovery_fixture::marker(
                    recovery_id,
                    &result.input_head,
                    &result.audit_sha256,
                );
                if self.recovery_marker.as_deref() != Some(expected.as_str()) {
                    return self.recovery_checked(Err(invalid(
                        "dirty generation is not this recovery transaction",
                    )));
                }
                self.recovery_checked(journal.fixture_sync_recovery())?;
                if let Err(error) = self.write_state("clean\n") {
                    self.admission_poisoned.set(true);
                    return Err(error);
                }
                self.dirty = false;
                self.quarantined = false;
                self.recovery_marker = None;
            }
            let readback = if fault == Some(FixtureRecoveryFault::SealedReadback) {
                Err(io::Error::from_raw_os_error(libc::EIO))
            } else {
                crate::ddr_recovery_fixture::verify_sealed(self, None)
            };
            return self.recovery_checked(readback.and_then(|readback| {
                if readback == result {
                    Ok(readback)
                } else {
                    Err(invalid("sealed recovery changed during readback"))
                }
            }));
        }
        if !self.quarantined {
            return Err(invalid(
                "recovery requires an inherited quarantined fixture",
            ));
        }
        if self.recovery_marker.is_some() {
            return self.recovery_checked(Err(invalid(
                "incomplete recovery transaction remains quarantined",
            )));
        }
        // Verifier replays the exact pinned journal and proves trusted roster,
        // normal waits and joined broker/worker drains before any write.
        let report = self.recovery_checked(self.verify_fixture_coverage())?;
        // A bad caller ID is not a material failure, but it must not mask a
        // missing/corrupt witness or prevent this instance being poisoned.
        if !name_ok(recovery_id) {
            return Err(invalid("invalid recovery identity"));
        }
        let transaction = self.recovery_checked(crate::ddr_recovery_fixture::transaction(
            &report,
            recovery_id,
        ))?;
        if journal.head() != report.input_head() {
            return self
                .recovery_checked(Err(invalid("recovery input changed before transaction")));
        }
        if fault == Some(FixtureRecoveryFault::Capacity) {
            journal.fixture_exhaust_recovery_capacity();
        }
        if let Err(error) = journal.fixture_recovery_room(&[
            &transaction.audit,
            &transaction.commit,
            &transaction.checkpoint,
        ]) {
            self.admission_poisoned.set(true);
            return Err(error);
        }
        let marker = crate::ddr_recovery_fixture::marker(
            recovery_id,
            report.input_head(),
            &crate::ddr_recovery_coverage::digest(&transaction.audit),
        );
        let marker_failure = fault.and_then(FixtureRecoveryFault::pending_stage);
        if let Err(error) = self.write_state_inner(&marker, |stage| {
            if Some(stage) == marker_failure {
                Err(io::Error::from_raw_os_error(libc::EIO))
            } else {
                Ok(())
            }
        }) {
            self.admission_poisoned.set(true);
            return Err(error);
        }
        self.recovery_marker = Some(marker);
        if fault == Some(FixtureRecoveryFault::AfterPending) {
            self.admission_poisoned.set(true);
            return Err(io::Error::from_raw_os_error(libc::ENOSPC));
        }
        let broker = report.input_head().broker.as_str();
        for (kind, payload) in [
            (Kind::RecoveryAudit, &transaction.audit),
            (Kind::RecoveryCommit, &transaction.commit),
            (Kind::CleanCheckpoint, &transaction.checkpoint),
        ] {
            let injection = match fault {
                Some(FixtureRecoveryFault::Journal(target, stage)) if target == kind => Some(stage),
                _ => None,
            };
            if let Err(error) =
                journal.fixture_append_recovery_fault(broker, kind, payload, injection)
            {
                self.admission_poisoned.set(true);
                return Err(error);
            }
        }
        let result = self.recovery_checked(crate::ddr_recovery_fixture::verify_sealed(
            self,
            Some(recovery_id),
        ))?;
        let state_failure = fault.and_then(FixtureRecoveryFault::state_stage);
        if let Err(error) = self.write_state_inner("clean\n", |stage| {
            if Some(stage) == state_failure {
                Err(io::Error::from_raw_os_error(libc::EIO))
            } else {
                Ok(())
            }
        }) {
            self.admission_poisoned.set(true);
            return Err(error);
        }
        self.dirty = false;
        self.quarantined = false;
        self.recovery_marker = None;
        let readback = if fault == Some(FixtureRecoveryFault::FinalReadback) {
            Err(io::Error::from_raw_os_error(libc::EIO))
        } else {
            crate::ddr_recovery_fixture::verify_sealed(self, Some(recovery_id))
        };
        let readback = match readback {
            Ok(readback) => readback,
            Err(error) => {
                self.admission_poisoned.set(true);
                return Err(error);
            }
        };
        if result != readback {
            self.admission_poisoned.set(true);
            return Err(invalid("sealed recovery changed during readback"));
        }
        Ok(readback)
    }

    pub(crate) fn fixture_verify_binding_journal_ids(
        &self,
        journal: (u64, u64),
        head: (u64, u64),
    ) -> io::Result<()> {
        self.ensure_namespace()?;
        if journal != (self.bootstrap.journal_dev, self.bootstrap.journal_ino)
            || head
                != (
                    self.bootstrap.journal_head_dev,
                    self.bootstrap.journal_head_ino,
                )
        {
            return Err(invalid("witness binding names different journal files"));
        }
        Ok(())
    }

    /// Must succeed before allocating any managed commitment or delivering FD.
    /// A dirty state inherited from another broker is always quarantined.
    pub(crate) fn before_admission(&mut self) -> io::Result<()> {
        self.before_admission_inner(|_| Ok(()))
    }

    fn before_admission_inner(
        &mut self,
        hook: impl FnMut(StateWriteStage) -> io::Result<()>,
    ) -> io::Result<()> {
        self.before_admission_with_faults(hook, |_| Ok(()))
    }

    fn before_admission_with_faults(
        &mut self,
        mut hook: impl FnMut(StateWriteStage) -> io::Result<()>,
        mut retry_hook: impl FnMut(StateWriteStage) -> io::Result<()>,
    ) -> io::Result<()> {
        if self.admission_poisoned.get() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "authority admission poisoned",
            ));
        }
        if let Err(error) = self.ensure_namespace() {
            self.admission_poisoned.set(true);
            return Err(error);
        }
        if self.quarantined {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "authority quarantined",
            ));
        }
        if !self.dirty {
            let written = if self.bootstrap.deployed {
                self.write_normal_deployment_state_inner(true, None, &mut hook)
            } else {
                self.write_state_inner("dirty\n", &mut hook)
            };
            if let Err(error) = written {
                // No commitment has been exposed. If the failed operation
                // left clean-looking bytes, attempt to persist dirty under
                // the original lock. A failed retry never grants admission.
                let retry = if self.bootstrap.deployed {
                    self.ensure_static_namespace().and_then(|_| {
                        self.write_normal_deployment_state_inner(true, None, &mut retry_hook)
                    })
                } else {
                    Ok(())
                };
                self.admission_poisoned.set(true);
                if let Err(retry_error) = retry {
                    return Err(io::Error::other(format!(
                        "state write failed: {error}; dirty retry failed: {retry_error}"
                    )));
                }
                if self.bootstrap.deployed {
                    self.dirty = true;
                }
                return Err(error);
            }
            self.dirty = true;
            if let Err(error) = self.ensure_namespace() {
                self.admission_poisoned.set(true);
                return Err(error);
            }
        }
        Ok(())
    }

    pub(crate) fn admits_new_work(&self) -> bool {
        if self.quarantined || self.admission_poisoned.get() {
            return false;
        }
        if self.ensure_namespace().is_err() {
            self.admission_poisoned.set(true);
            return false;
        }
        true
    }

    pub(crate) fn matches_audit_domain(&self, domain: &crate::ddr_recovery_audit::Domain) -> bool {
        domain.domain == DOMAIN
            && domain.host == self.bootstrap.host
            && domain.boot == self.bootstrap.boot
            && domain.cap_revision == self.bootstrap.revision
            && domain.owner_limit == self.bootstrap.owner_limit
            && domain.total_limit == self.bootstrap.total_limit
    }

    /// Finish an ordinary live generation only with a non-cloneable receipt
    /// from the same pinned, semantically replayed writer. This is not a dirty
    /// recovery entry and can never clear an inherited quarantine.
    pub(crate) fn clean_after_semantic_drain(
        &mut self,
        drained: &DrainedCommitments,
        seal: crate::ddr_recovery_durable::SemanticClean,
    ) -> io::Result<()> {
        self.clean_after_semantic_drain_inner(drained, seal, |_| Ok(()))
    }

    fn clean_after_semantic_drain_inner(
        &mut self,
        drained: &DrainedCommitments,
        seal: crate::ddr_recovery_durable::SemanticClean,
        hook: impl FnMut(StateWriteStage) -> io::Result<()>,
    ) -> io::Result<()> {
        self.clean_after_semantic_drain_with_faults(drained, seal, hook, |_| Ok(()), || Ok(()))
    }

    fn clean_after_semantic_drain_with_faults(
        &mut self,
        drained: &DrainedCommitments,
        seal: crate::ddr_recovery_durable::SemanticClean,
        mut hook: impl FnMut(StateWriteStage) -> io::Result<()>,
        mut rollback_hook: impl FnMut(StateWriteStage) -> io::Result<()>,
        mut final_hook: impl FnMut() -> io::Result<()>,
    ) -> io::Result<()> {
        use crate::ddr_recovery_audit::EventKind;
        use crate::ddr_recovery_journal::Mode;
        if self.admission_poisoned.get() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "authority admission poisoned",
            ));
        }
        if let Err(error) = self.ensure_namespace() {
            self.admission_poisoned.set(true);
            return Err(error);
        }
        if self.quarantined || !self.dirty || !drained.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "authority is not a live, verifiably drained generation",
            ));
        }
        let checked = (|| -> io::Result<()> {
            let mut journal = self.open_journal()?;
            let frames = journal.verify()?;
            if !self.matches_audit_domain(seal.domain())
                || journal.file_identities() != seal.file_identities()
                || journal.head() != seal.head()
                || seal.head().mode != Mode::Clean
                || seal.head().broker != seal.domain().broker
            {
                return Err(invalid(
                    "semantic checkpoint does not match the pinned authority",
                ));
            }
            let last = frames
                .last()
                .ok_or_else(|| invalid("missing semantic checkpoint"))?;
            let event = crate::ddr_recovery_codec::decode(last).map_err(invalid)?;
            let EventKind::CleanCheckpoint(snapshot) = event.kind else {
                return Err(invalid("last frame is not a semantic checkpoint"));
            };
            if event.domain != *seal.domain()
                || snapshot.handlers != 0
                || snapshot.pending_handoffs != 0
                || snapshot.retained_backings != 0
                || snapshot.active_views != 0
                || snapshot.reservations != 0
                || snapshot.in_flight != 0
                || snapshot.potential_holders != 0
            {
                return Err(invalid(
                    "semantic checkpoint is not the verified zero drain",
                ));
            }
            Ok(())
        })();
        if let Err(error) = checked {
            self.admission_poisoned.set(true);
            return Err(error);
        }
        let old_dirty = self.deployed_state.clone();
        let written = if self.bootstrap.deployed {
            self.write_normal_deployment_state_inner(false, Some(seal.head()), &mut hook)
        } else {
            self.write_state_inner("clean\n", &mut hook)
        };
        if let Err(error) = written {
            return Err(self.clean_publish_failure(
                "write_or_sync",
                error,
                old_dirty.as_deref(),
                &mut rollback_hook,
            ));
        }
        self.dirty = false;
        if let Err(error) = self.ensure_namespace().and_then(|_| final_hook()) {
            return Err(self.clean_publish_failure(
                "final_namespace",
                error,
                old_dirty.as_deref(),
                &mut rollback_hook,
            ));
        }
        Ok(())
    }

    fn clean_publish_failure(
        &self,
        phase: &'static str,
        primary: io::Error,
        old_dirty: Option<&str>,
        rollback_hook: impl FnMut(StateWriteStage) -> io::Result<()>,
    ) -> io::Error {
        // A failed clean sync or final check may already have exposed complete
        // clean bytes. Attempt one rollback under the same lock; never suppress
        // its error or represent an unsuccessful retry as durable quarantine.
        self.admission_poisoned.set(true);
        let rollback = if self.bootstrap.deployed {
            self.ensure_static_namespace().and_then(|_| {
                self.write_pinned_state_inner(
                    old_dirty.ok_or_else(|| invalid("missing prior dirty state"))?,
                    rollback_hook,
                )
            })
        } else {
            Err(invalid("fixture has no deployed dirty rollback"))
        };
        let rollback = match rollback {
            Ok(()) => "dirty_write_sync_readback_succeeded".to_owned(),
            Err(error) => format!("unknown: {error}"),
        };
        io::Error::other(format!(
            "clean_unconfirmed phase={phase}; primary={primary}; rollback={rollback}; live_authority=poisoned"
        ))
    }

    /// The caller must stop admission and drain real catalog references first.
    /// A newly opened dirty authority can never make itself clean this way.
    pub(crate) fn clean_after_drain(&mut self, drained: &DrainedCommitments) -> io::Result<()> {
        // The legacy fixture marker has no proof that the pinned journal is
        // the lifecycle writer. Never enable this shortcut in production.
        if !cfg!(test) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "semantic checkpoint is not wired to production authority",
            ));
        }
        if self.admission_poisoned.get() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "authority admission poisoned",
            ));
        }
        if let Err(error) = self.ensure_namespace() {
            self.admission_poisoned.set(true);
            return Err(error);
        }
        if self.quarantined || !drained.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "authority not verifiably drained",
            ));
        }
        if self.open_journal()?.head().mode != crate::ddr_recovery_journal::Mode::Clean {
            return Err(invalid("recovery journal has no clean checkpoint"));
        }
        if self.dirty {
            if let Err(error) = self.write_state("clean\n") {
                self.admission_poisoned.set(true);
                return Err(error);
            }
            self.dirty = false;
            if let Err(error) = self.ensure_namespace() {
                self.admission_poisoned.set(true);
                return Err(error);
            }
        }
        Ok(())
    }

    /// Trusted deployment may switch the run-owned endpoint or budget only
    /// after stopping and draining the locked, clean authority. A partial
    /// write makes bootstrap invalid rather than inventing a second domain.
    /// The caller must persist an independent audit record.
    pub(crate) fn reconfigure_after_drain(
        &mut self,
        drained: &DrainedCommitments,
        endpoint: &str,
        owner_limit: u64,
        total_limit: u64,
    ) -> io::Result<u64> {
        // Until an independently durable revision audit is implemented this
        // fixture helper is not a production reconfiguration operation.
        if !cfg!(test) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "audited authority reconfiguration is not available",
            ));
        }
        self.ensure_namespace()?;
        if self.admission_poisoned.get()
            || self.quarantined
            || self.dirty
            || !drained.is_empty()
            || !endpoint_ok(endpoint)
            || owner_limit == 0
            || total_limit < owner_limit
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "authority cannot be reconfigured",
            ));
        }
        let mut updated = self.bootstrap.clone();
        updated.revision = updated
            .revision
            .checked_add(1)
            .ok_or_else(|| invalid("revision overflow"))?;
        updated.endpoint = endpoint.to_owned();
        updated.owner_limit = owner_limit;
        updated.total_limit = total_limit;
        let text = updated.encode();
        let mut file = private_file(&self.root.join(MANIFEST_FILE), true)?;
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
        File::open(&self.root)?.sync_all()?;
        self.bootstrap = updated;
        self.bootstrapped_text = text;
        self.ensure_namespace()?;
        Ok(self.bootstrap.revision)
    }

    fn write_state(&self, value: &str) -> io::Result<()> {
        self.write_state_inner(value, |_| Ok(()))
    }

    fn write_state_inner(
        &self,
        value: &str,
        hook: impl FnMut(StateWriteStage) -> io::Result<()>,
    ) -> io::Result<()> {
        // A v3 clean/dirty write would erase the deployed independent anchors.
        // Deployed transitions use the versioned writer instead.
        if self.bootstrap.deployed {
            return Err(invalid(
                "versioned authority state writer not yet available",
            ));
        }
        self.write_pinned_state_inner(value, hook)
    }

    /// Normal work and the experimental witness transaction use mutually
    /// exclusive closure modes. The old clean head remains independently
    /// anchored while dirty, including a crash after the journal checkpoint.
    fn write_normal_deployment_state_inner(
        &mut self,
        dirty: bool,
        checkpoint: Option<&crate::ddr_recovery_journal::Head>,
        hook: impl FnMut(StateWriteStage) -> io::Result<()>,
    ) -> io::Result<()> {
        use crate::ddr_recovery_coverage::{canonical, parse_line, s};
        use crate::ddr_recovery_journal::Mode;
        if self.quarantined || self.admission_poisoned.get() {
            return Err(invalid("normal state writer is quarantined"));
        }
        let mut state = parse_line(
            self.deployed_state
                .as_ref()
                .ok_or_else(|| invalid("missing deployed state"))?
                .as_bytes(),
        )?;
        let closure = s(&state, "closure")?;
        if (dirty
            && (self.dirty || !matches!(closure, "closed" | "normal") || checkpoint.is_some()))
            || (!dirty && (!self.dirty || closure != "normal" || checkpoint.is_none()))
        {
            return Err(invalid("invalid normal authority transition"));
        }
        if let Some(head) = checkpoint {
            if head.mode != Mode::Clean || self.open_journal()?.head() != head {
                return Err(invalid("normal checkpoint is not the pinned clean head"));
            }
            state["journal_head"] = head.encode().into();
        }
        state["mode"] = if dirty { "dirty" } else { "clean" }.into();
        state["closure"] = "normal".into();
        let mut bytes = canonical(&state)?;
        bytes.push(b'\n');
        if bytes.len() as u64 > MAX_CONFIG_BYTES {
            return Err(invalid("normal authority state exceeds limit"));
        }
        let text =
            String::from_utf8(bytes).map_err(|_| invalid("invalid normal authority state"))?;
        self.write_pinned_state_inner(&text, hook)?;
        self.deployed_state = Some(text);
        Ok(())
    }

    fn write_pinned_state_inner(
        &self,
        value: &str,
        mut hook: impl FnMut(StateWriteStage) -> io::Result<()>,
    ) -> io::Result<()> {
        let mut state = private_file(&self.root.join(STATE_FILE), true)?;
        let meta = state.metadata()?;
        if (meta.dev(), meta.ino()) != (self.bootstrap.state_dev, self.bootstrap.state_ino) {
            return Err(invalid("authority state inode changed before write"));
        }
        // Truncate-first deliberately makes a crash mid-update invalid, never
        // implicitly clean. The caller must not mutate the catalog on failure.
        hook(StateWriteStage::BeforeTruncate)?;
        state.set_len(0)?;
        hook(StateWriteStage::AfterTruncate)?;
        state.seek(SeekFrom::Start(0))?;
        let bytes = value.as_bytes();
        let split = bytes.len() / 2;
        state.write_all(&bytes[..split])?;
        hook(StateWriteStage::AfterPartialWrite)?;
        state.write_all(&bytes[split..])?;
        hook(StateWriteStage::BeforeSync)?;
        state.sync_all()?;
        hook(StateWriteStage::AfterSync)?;
        hook(StateWriteStage::BeforeDirectorySync)?;
        File::open(&self.root)?.sync_all()?;
        hook(StateWriteStage::AfterDirectorySync)?;
        hook(StateWriteStage::BeforeReadbackOpen)?;
        let readback = private_file(&self.root.join(STATE_FILE), false)?;
        hook(StateWriteStage::BeforeReadbackMetadata)?;
        let meta = readback.metadata()?;
        hook(StateWriteStage::BeforeReadbackRead)?;
        if (meta.dev(), meta.ino()) != (self.bootstrap.state_dev, self.bootstrap.state_ino)
            || read_bounded(&readback)? != value
        {
            return Err(invalid("authority state readback mismatch"));
        }
        hook(StateWriteStage::AfterReadback)?;
        Ok(())
    }
}

fn initialize_at(
    root: &Path,
    host: &str,
    boot: &str,
    endpoint: &str,
    owner_limit: u64,
    total_limit: u64,
) -> io::Result<()> {
    initialize_at_inner(root, host, boot, endpoint, owner_limit, total_limit, |_| {
        Ok(())
    })
}

fn initialize_at_inner(
    root: &Path,
    host: &str,
    boot: &str,
    endpoint: &str,
    owner_limit: u64,
    total_limit: u64,
    hook: impl FnMut(BootstrapStage) -> io::Result<()>,
) -> io::Result<()> {
    initialize_with_policy_at_inner(
        root,
        host,
        boot,
        endpoint,
        owner_limit,
        total_limit,
        None,
        hook,
    )
}

fn initialize_with_policy_at_inner(
    root: &Path,
    host: &str,
    boot: &str,
    endpoint: &str,
    owner_limit: u64,
    total_limit: u64,
    policy: Option<&crate::ddr_deployment_policy::DeploymentPolicy>,
    hook: impl FnMut(BootstrapStage) -> io::Result<()>,
) -> io::Result<()> {
    initialize_at_mode_inner(
        root,
        host,
        boot,
        endpoint,
        owner_limit,
        total_limit,
        policy,
        false,
        hook,
    )
}

fn initialize_at_mode_inner(
    root: &Path,
    host: &str,
    boot: &str,
    endpoint: &str,
    owner_limit: u64,
    total_limit: u64,
    policy: Option<&crate::ddr_deployment_policy::DeploymentPolicy>,
    normal_only: bool,
    mut hook: impl FnMut(BootstrapStage) -> io::Result<()>,
) -> io::Result<()> {
    if !root.is_absolute()
        || !name_ok(host)
        || !name_ok(boot)
        || !endpoint_ok(endpoint)
        || owner_limit == 0
        || total_limit < owner_limit
    {
        return Err(invalid("invalid authority initialization"));
    }
    // Validate the complete preselected policy and all referenced artifacts
    // before creating any part of a new root.
    if normal_only && policy.is_some() {
        return Err(invalid("normal authority cannot select a witness policy"));
    }
    let initial_trust = if normal_only {
        Some(NORMAL_TRUST.to_vec())
    } else if let Some(policy) = policy {
        let bytes = policy.initial_chain()?;
        crate::ddr_deployment_policy::DeploymentPolicy::from_initial_chain(&bytes)?;
        Some(bytes)
    } else {
        None
    };
    let parent = root
        .parent()
        .ok_or_else(|| invalid("authority has no parent"))?;
    if fs::canonicalize(parent)? != parent {
        return Err(invalid("authority parent must be canonical"));
    }
    let parent_meta = fs::metadata(parent)?;
    if !parent_meta.is_dir()
        || parent_meta.uid() != unsafe { libc::geteuid() }
        || parent_meta.mode() & 0o022 != 0
    {
        return Err(invalid("authority parent is not trusted"));
    }
    // No recursive create, no removal or reuse: an interrupted bootstrap is
    // explicitly left for inspection and cannot be mistaken for a clean root.
    fs::DirBuilder::new().mode(0o700).create(root)?;
    hook(BootstrapStage::RootCreated)?;
    let dir = private_dir(root)?;
    let lock = create_private(&root.join(LOCK_FILE))?;
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let lock_meta = lock.metadata()?;
    hook(BootstrapStage::LockCreated)?;
    let mut state = create_private(&root.join(STATE_FILE))?;
    let state_meta = state.metadata()?;
    hook(BootstrapStage::StateCreated)?;
    let journal = create_private(&root.join(JOURNAL_FILE))?;
    let journal_meta = journal.metadata()?;
    hook(BootstrapStage::JournalCreated)?;
    let mut journal_head = create_private(&root.join(JOURNAL_HEAD_FILE))?;
    let journal_head_meta = journal_head.metadata()?;
    hook(BootstrapStage::HeadCreated)?;
    // Policy-backed and normal formats respectively pin DEPLOY or the
    // no-witness marker. Only isolated v3 fixtures have an empty slot.
    let mut witness_trust = create_private(&root.join(WITNESS_TRUST_FILE))?;
    let witness_trust_meta = witness_trust.metadata()?;
    hook(BootstrapStage::WitnessCreated)?;
    // Obtain the eventual bootstrap inode before computing the independently
    // pinned state config digest. No final name is visible at this point.
    let mut manifest = create_private(&root.join(PENDING_MANIFEST_FILE))?;
    let manifest_meta = manifest.metadata()?;
    let bootstrap = Bootstrap {
        deployed: initial_trust.is_some(),
        normal_only,
        host: host.to_owned(),
        boot: boot.to_owned(),
        endpoint: endpoint.to_owned(),
        owner_limit,
        total_limit,
        revision: 1,
        dir_dev: dir.dev(),
        dir_ino: dir.ino(),
        lock_dev: lock_meta.dev(),
        lock_ino: lock_meta.ino(),
        state_dev: state_meta.dev(),
        state_ino: state_meta.ino(),
        journal_dev: journal_meta.dev(),
        journal_ino: journal_meta.ino(),
        journal_head_dev: journal_head_meta.dev(),
        journal_head_ino: journal_head_meta.ino(),
        witness_trust_dev: witness_trust_meta.dev(),
        witness_trust_ino: witness_trust_meta.ino(),
        manifest_identity: initial_trust
            .as_ref()
            .map(|_| (manifest_meta.dev(), manifest_meta.ino())),
    };
    let initial_state = if let Some(trust) = initial_trust.as_ref() {
        witness_trust.write_all(trust)?;
        witness_trust.sync_all()?;
        deployment_genesis_state(&bootstrap, trust)?
    } else {
        "clean\n".to_owned()
    };
    state.write_all(initial_state.as_bytes())?;
    state.sync_all()?;
    journal_head
        .write_all(crate::ddr_recovery_journal::AnchoredJournal::genesis_state().as_bytes())?;
    journal.sync_all()?;
    journal_head.sync_all()?;
    witness_trust.sync_all()?;
    lock.sync_all()?;
    hook(BootstrapStage::ComponentsSynced)?;

    // Sync all genesis directory entries before publishing a valid bootstrap.
    // An interrupted root is never implicitly repaired or replaced.
    let open_directory = |path: &Path| {
        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
    };
    let root_dir = open_directory(root)?;
    let parent_dir = open_directory(parent)?;
    root_dir.sync_all()?;
    parent_dir.sync_all()?;
    hook(BootstrapStage::DirectoriesSynced)?;

    // Verify the exact synced genesis before publishing the final name.
    let trust_readback = read_bounded_with_limit(
        &private_file(&root.join(WITNESS_TRUST_FILE), false)?,
        MAX_TRUST_BYTES,
    )?;
    if read_bounded(&private_file(&root.join(STATE_FILE), false)?)? != initial_state
        || trust_readback.as_bytes() != initial_trust.as_deref().unwrap_or(b"")
    {
        return Err(invalid("initial authority state or witness slot changed"));
    }
    if bootstrap.deployed {
        verify_deployment_genesis(&bootstrap, trust_readback.as_bytes(), &initial_state)?;
    }
    let mut genesis = crate::ddr_recovery_journal::AnchoredJournal::open(
        &root.join(JOURNAL_FILE),
        &root.join(JOURNAL_HEAD_FILE),
        (bootstrap.journal_dev, bootstrap.journal_ino),
        (bootstrap.journal_head_dev, bootstrap.journal_head_ino),
    )?;
    if genesis.head().sequence != 0 || !genesis.verify()?.is_empty() {
        return Err(invalid("initial authority journal is not genesis"));
    }

    // The final name appears atomically only after the staged file is synced;
    // never replace an existing entry, including one created concurrently.
    let manifest_text = bootstrap.encode();
    manifest.write_all(manifest_text.as_bytes())?;
    manifest.sync_all()?;
    hook(BootstrapStage::ManifestSynced)?;
    let staged = private_file(&root.join(PENDING_MANIFEST_FILE), false)?;
    let staged_meta = staged.metadata()?;
    if (staged_meta.dev(), staged_meta.ino()) != (manifest_meta.dev(), manifest_meta.ino())
        || read_bounded(&staged)? != manifest_text
    {
        return Err(invalid("staged bootstrap changed before publication"));
    }
    let pending = CString::new(PENDING_MANIFEST_FILE).unwrap();
    let final_name = CString::new(MANIFEST_FILE).unwrap();
    if unsafe {
        libc::renameat2(
            root_dir.as_raw_fd(),
            pending.as_ptr(),
            root_dir.as_raw_fd(),
            final_name.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    hook(BootstrapStage::Published)?;
    root_dir.sync_all()?;
    parent_dir.sync_all()?;
    let published = private_file(&root.join(MANIFEST_FILE), false)?;
    let published_meta = published.metadata()?;
    if (published_meta.dev(), published_meta.ino()) != (manifest_meta.dev(), manifest_meta.ino())
        || read_bounded(&published)? != manifest_text
    {
        return Err(invalid("published bootstrap changed"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader};
    use std::os::fd::FromRawFd;
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::UnixStream;
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};
    use std::time::{SystemTime, UNIX_EPOCH};

    const ENDPOINT: &str = "/workspace/uflow/runs/t_855f763c-r49/a.sock";
    fn new_root() -> PathBuf {
        let base = std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target"));
        let base = fs::canonicalize(base).unwrap();
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        base.join(format!("managed-authority-{}-{nonce}", std::process::id()))
    }
    fn init(root: &Path) {
        initialize_at(root, "host", "boot", ENDPOINT, 16, 64).unwrap();
    }
    fn open(root: &Path) -> io::Result<ManagedAuthority> {
        after_test_lock_release(|| {
            ManagedAuthority::open_at(root, "host", "boot", ENDPOINT, 16, 64)
        })
    }
    fn drained() -> DrainedCommitments {
        DrainedCommitments {
            accepting_stopped: true,
            ..DrainedCommitments::default()
        }
    }

    fn snapshot(root: &Path) -> Vec<(std::ffi::OsString, u64, u64, u32, Vec<u8>)> {
        let mut entries: Vec<_> = fs::read_dir(root)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                let meta = fs::symlink_metadata(entry.path()).unwrap();
                assert!(meta.is_file());
                (
                    entry.file_name(),
                    meta.dev(),
                    meta.ino(),
                    meta.mode(),
                    fs::read(entry.path()).unwrap(),
                )
            })
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
    }

    pub(super) fn deployment_fixture() -> (
        PathBuf,
        PathBuf,
        crate::ddr_deployment_policy::DeploymentPolicy,
    ) {
        let parent = new_root();
        fs::DirBuilder::new().mode(0o700).create(&parent).unwrap();
        let artifact = parent.join("fixture-artifact");
        fs::write(
            &artifact,
            b"fixture role artifact, not executable evidence\n",
        )
        .unwrap();
        let hash = crate::ddr_recovery_coverage::digest(&fs::read(&artifact).unwrap());
        let item = serde_json::json!({"path": artifact.to_str().unwrap(), "sha256": hash});
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
            serde_json::json!({
                "role": role, "executable": item, "entrypoint": item,
                "argv": [artifact.to_str().unwrap()], "environment": {},
            })
        })
        .collect();
        let value = serde_json::json!({"schema": "uflow-ddr-deployment-policy-v1", "roles": roles});
        let policy_file = parent.join("policy.json");
        let mut bytes = crate::ddr_recovery_coverage::canonical(&value).unwrap();
        bytes.push(b'\n');
        fs::write(&policy_file, bytes).unwrap();
        let policy = crate::ddr_deployment_policy::DeploymentPolicy::load(&policy_file).unwrap();
        (parent.join("authority"), artifact, policy)
    }

    pub(super) fn initialize_deployed_fixture(
        root: &Path,
        policy: &crate::ddr_deployment_policy::DeploymentPolicy,
    ) {
        initialize_with_policy_at_inner(
            root,
            "host",
            "boot",
            ENDPOINT,
            16,
            64,
            Some(policy),
            |_| Ok(()),
        )
        .unwrap();
    }

    #[test]
    fn deployment_genesis_publishes_and_pins_nonempty_pretrust() {
        let (root, _, policy) = deployment_fixture();
        let mut stages = Vec::new();
        initialize_with_policy_at_inner(
            &root,
            "host",
            "boot",
            ENDPOINT,
            16,
            64,
            Some(&policy),
            |stage| {
                if stage != BootstrapStage::Published {
                    assert!(!root.join(MANIFEST_FILE).exists());
                }
                if stage == BootstrapStage::ComponentsSynced {
                    let trust = fs::read(root.join(WITNESS_TRUST_FILE)).unwrap();
                    assert!(!trust.is_empty());
                    crate::ddr_deployment_policy::DeploymentPolicy::from_initial_chain(&trust)
                        .unwrap();
                    assert!(fs::read(root.join(STATE_FILE)).unwrap().starts_with(b"{"));
                }
                stages.push(stage);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(stages.last(), Some(&BootstrapStage::Published));
        let authority = open(&root).unwrap();
        assert!(authority.bootstrap.deployed);
        assert_eq!(authority.open_journal().unwrap().head().sequence, 0);
        let before = snapshot(&root);
        assert!(authority.admits_new_work());
        assert_eq!(snapshot(&root), before);
        drop(authority);
        assert!(initialize_with_policy_at_inner(
            &root,
            "host",
            "boot",
            ENDPOINT,
            16,
            64,
            Some(&policy),
            |_| Ok(())
        )
        .is_err());
        assert_eq!(snapshot(&root), before);
    }

    #[test]
    fn deployed_normal_dirty_and_joined_clean_allow_same_root_restart() {
        use crate::ddr_recovery_audit::{Event, EventKind};
        let (root, _, policy) = deployment_fixture();
        initialize_deployed_fixture(&root, &policy);
        let mut authority = open(&root).unwrap();
        let mut audit = authority.start_lifecycle_audit("normal-broker-1").unwrap();
        authority.before_admission().unwrap();
        assert!(authority.admits_new_work());
        let state: serde_json::Value =
            serde_json::from_slice(&fs::read(root.join(STATE_FILE)).unwrap()).unwrap();
        assert_eq!(state["mode"], "dirty");
        assert_eq!(state["closure"], "normal");
        assert!(
            open(&root).is_err(),
            "second broker must not acquire the lock"
        );
        let birth = crate::ddr_process_identity::observe(
            std::process::id() as i32,
            unsafe { libc::geteuid() },
            unsafe { libc::getegid() },
        )
        .unwrap();
        audit
            .record(Event {
                domain: audit.domain().clone(),
                epoch: 0,
                kind: EventKind::BrokerBirth { birth },
            })
            .unwrap();
        let joined = crate::external_ddr::fixture_joined_drain("normal-broker-1");
        audit
            .record_joined_drain(&joined, 0, audit.replay().observed_drain(0, 0))
            .unwrap();
        let clean = audit.checkpoint_after_drain(&joined).unwrap();
        authority
            .clean_after_semantic_drain(&drained(), clean)
            .unwrap();
        let state: serde_json::Value =
            serde_json::from_slice(&fs::read(root.join(STATE_FILE)).unwrap()).unwrap();
        assert_eq!(state["mode"], "clean");
        drop(audit);
        drop(authority);
        let mut reopened = open(&root).unwrap();
        assert!(!reopened.is_inherited_quarantine());
        let _next = reopened.start_lifecycle_audit("normal-broker-2").unwrap();
        reopened.before_admission().unwrap();
        assert!(reopened.admits_new_work());
    }

    #[test]
    fn normal_bootstrap_has_no_witness_policy_and_is_not_reused() {
        let parent = new_root();
        fs::DirBuilder::new().mode(0o700).create(&parent).unwrap();
        let root = parent.join("authority");
        initialize_at_mode_inner(&root, "host", "boot", ENDPOINT, 16, 64, None, true, |_| {
            Ok(())
        })
        .unwrap();
        let before = snapshot(&root);
        let authority = open(&root).unwrap();
        assert!(authority.bootstrap.normal_only);
        assert_eq!(
            fs::read(root.join(WITNESS_TRUST_FILE)).unwrap(),
            NORMAL_TRUST
        );
        assert!(authority.admits_new_work());
        drop(authority);
        assert!(initialize_at_mode_inner(
            &root,
            "host",
            "boot",
            ENDPOINT,
            16,
            64,
            None,
            true,
            |_| Ok(())
        )
        .is_err());
        assert_eq!(snapshot(&root), before);
    }

    #[test]
    fn normal_restart_requires_each_real_sync_and_final_verification() {
        let stages = [
            RestartStage::BeforeJournalSync,
            RestartStage::AfterJournalSync,
            RestartStage::BeforeHeadSync,
            RestartStage::AfterHeadSync,
            RestartStage::BeforeStateSync,
            RestartStage::AfterStateSync,
            RestartStage::BeforeTrustSync,
            RestartStage::BeforeBootstrapSync,
            RestartStage::BeforeDirectorySync,
            RestartStage::AfterDirectorySync,
            RestartStage::BeforeFinalVerification,
            RestartStage::AfterFinalVerification,
        ];
        for policy_backed in [false, true] {
            for fault in stages {
                let root = new_normal_deployment_fixture(policy_backed);
                normal_clean_fixture(&root, "prior-normal-broker");
                let before = snapshot(&root);
                let lock = private_file(&root.join(LOCK_FILE), true).unwrap();
                assert_eq!(
                    unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
                    0
                );
                let mut seen = Vec::new();
                let failed = ManagedAuthority::open_at_held_lock_with_hook(
                    &root,
                    "host",
                    "boot",
                    ENDPOINT,
                    16,
                    64,
                    lock,
                    |stage| {
                        seen.push(stage);
                        if stage == fault {
                            Err(io::Error::from_raw_os_error(libc::EIO))
                        } else {
                            Ok(())
                        }
                    },
                );
                assert!(failed.is_err(), "restart stage {fault:?} must deny");
                assert_eq!(seen.last(), Some(&fault));
                println!("restart-fault policy_backed={policy_backed} fault={fault:?} reached={seen:?} input={root:?} old-process=poisoned");
                assert_eq!(
                    snapshot(&root),
                    before,
                    "restart must not rewrite authority"
                );
                let reopened = open(&root).unwrap();
                assert!(
                    !reopened.admits_new_work(),
                    "same process must stay poisoned"
                );
                drop(reopened);
                run_restart_child(&root, "sync-fails");
                run_restart_child(&root, "sync-fails");
                run_restart_child(&root, "dirty-fails");
                assert_eq!(
                    snapshot(&root),
                    before,
                    "failed barriers cannot change authority"
                );
                run_restart_child(&root, "admit");
                run_restart_child(&root, "deny");
            }
        }
    }

    #[test]
    fn normal_restart_child() {
        let Some(path) = std::env::var_os("UFLOW_RESTART_TEST_ROOT") else {
            return;
        };
        let root = PathBuf::from(path);
        match std::env::var("UFLOW_RESTART_TEST_MODE").unwrap().as_str() {
            "sync-fails" => {
                let lock = private_file(&root.join(LOCK_FILE), true).unwrap();
                assert_eq!(
                    unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
                    0
                );
                assert!(ManagedAuthority::open_at_held_lock_with_hook(
                    &root,
                    "host",
                    "boot",
                    ENDPOINT,
                    16,
                    64,
                    lock,
                    |stage| {
                        if stage == RestartStage::BeforeStateSync {
                            Err(io::Error::from_raw_os_error(libc::EIO))
                        } else {
                            Ok(())
                        }
                    },
                )
                .is_err());
                let mut again = open(&root).unwrap();
                assert!(!again.admits_new_work());
                assert!(again.before_admission().is_err());
            }
            "dirty-fails" => {
                let before = snapshot(&root);
                let mut authority = open(&root).unwrap();
                assert!(authority
                    .before_admission_with_faults(
                        |stage| if stage == StateWriteStage::BeforeTruncate {
                            Err(io::Error::from_raw_os_error(libc::EIO))
                        } else {
                            Ok(())
                        },
                        |stage| if stage == StateWriteStage::BeforeTruncate {
                            Err(io::Error::from_raw_os_error(libc::ENOSPC))
                        } else {
                            Ok(())
                        },
                    )
                    .is_err());
                assert!(!authority.admits_new_work());
                assert_eq!(snapshot(&root), before);
                drop(authority);
                let mut again = open(&root).unwrap();
                assert!(!again.admits_new_work());
                assert!(again.before_admission().is_err());
                assert_eq!(snapshot(&root), before);
            }
            "admit" => {
                let mut reopened = open(&root).unwrap();
                assert!(reopened.admits_new_work());
                reopened.before_admission().unwrap();
                assert!(reopened.admits_new_work());
            }
            "deny" => {
                if let Ok(mut reopened) = open(&root) {
                    assert!(!reopened.admits_new_work());
                    assert!(reopened.before_admission().is_err());
                }
            }
            "reject-missing-anchor" | "reject-bootstrap-inode" | "reject-config-anchor" => {
                let error = match open(&root) {
                    Ok(_) => panic!("invalid pinned bootstrap was opened"),
                    Err(error) => error,
                };
                let expected = match std::env::var("UFLOW_RESTART_TEST_MODE").unwrap().as_str() {
                    "reject-missing-anchor" => "legacy authority lacks bootstrap inode anchor",
                    "reject-bootstrap-inode" => "authority bootstrap inode mismatch",
                    _ => "invalid or pending versioned authority state",
                };
                assert!(error.to_string().contains(expected), "{error}");
            }
            other => panic!("invalid restart child mode: {other}"),
        }
    }

    fn run_restart_child(root: &Path, mode: &str) {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "ddr_managed_authority::tests::normal_restart_child",
                "--exact",
                "--nocapture",
            ])
            .env("UFLOW_RESTART_TEST_ROOT", root)
            .env("UFLOW_RESTART_TEST_MODE", mode)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "mode={mode} stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
    }

    fn new_normal_deployment_fixture(policy_backed: bool) -> PathBuf {
        if policy_backed {
            let (root, _, policy) = deployment_fixture();
            initialize_deployed_fixture(&root, &policy);
            root
        } else {
            let parent = new_root();
            fs::DirBuilder::new().mode(0o700).create(&parent).unwrap();
            let root = parent.join("authority");
            initialize_at_mode_inner(&root, "host", "boot", ENDPOINT, 16, 64, None, true, |_| {
                Ok(())
            })
            .unwrap();
            root
        }
    }

    // A non-genesis clean candidate, with fixture JoinedDrain rather than a
    // claim that this unit test exercised real daemon shutdown.
    fn normal_clean_fixture(root: &Path, broker: &str) {
        use crate::ddr_recovery_audit::{Event, EventKind};
        let mut authority = open(root).unwrap();
        let mut audit = authority.start_lifecycle_audit(broker).unwrap();
        authority.before_admission().unwrap();
        let birth = crate::ddr_process_identity::observe(
            std::process::id() as i32,
            unsafe { libc::geteuid() },
            unsafe { libc::getegid() },
        )
        .unwrap();
        audit
            .record(Event {
                domain: audit.domain().clone(),
                epoch: 0,
                kind: EventKind::BrokerBirth { birth },
            })
            .unwrap();
        let joined = crate::external_ddr::fixture_joined_drain(broker);
        audit
            .record_joined_drain(&joined, 0, audit.replay().observed_drain(0, 0))
            .unwrap();
        let clean = audit.checkpoint_after_drain(&joined).unwrap();
        authority
            .clean_after_semantic_drain(&drained(), clean)
            .unwrap();
    }

    #[test]
    fn legacy_v4_v5_without_bootstrap_anchor_are_preserved_and_rejected() {
        use crate::ddr_recovery_coverage::{canonical, digest};
        for policy_backed in [false, true] {
            for history in ["genesis", "normal-clean", "dirty"] {
                let root = new_normal_deployment_fixture(policy_backed);
                if history != "genesis" {
                    normal_clean_fixture(&root, "legacy-fixture-clean");
                }
                if history == "dirty" {
                    let mut authority = open(&root).unwrap();
                    authority.before_admission().unwrap();
                }
                // Construct canonical old-format fixture inputs, including a
                // matching old config digest and the existing actual history.
                // This is not a migration path and is confined to the test.
                let manifest_path = root.join(MANIFEST_FILE);
                let mut old =
                    Bootstrap::parse(&fs::read_to_string(&manifest_path).unwrap()).unwrap();
                old.manifest_identity = None;
                let old_text = old.encode();
                assert!(old_text.starts_with(if policy_backed {
                    DEPLOYMENT_FORMAT
                } else {
                    NORMAL_FORMAT
                }));
                fs::write(&manifest_path, &old_text).unwrap();
                let state_path = root.join(STATE_FILE);
                let mut state: serde_json::Value =
                    serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
                state["config_sha256"] = digest(old_text.as_bytes()).into();
                let mut bytes = canonical(&state).unwrap();
                bytes.push(b'\n');
                fs::write(&state_path, &bytes).unwrap();
                let trust = fs::read(root.join(WITNESS_TRUST_FILE)).unwrap();
                let journal = crate::ddr_recovery_journal::AnchoredJournal::open(
                    &root.join(JOURNAL_FILE),
                    &root.join(JOURNAL_HEAD_FILE),
                    (old.journal_dev, old.journal_ino),
                    (old.journal_head_dev, old.journal_head_ino),
                )
                .unwrap();
                assert_eq!(
                    verify_deployment_state(
                        &old,
                        &trust,
                        std::str::from_utf8(&bytes).unwrap(),
                        journal.head()
                    )
                    .unwrap(),
                    history == "dirty"
                );
                drop(journal);
                let before = snapshot(&root);
                run_restart_child(&root, "reject-missing-anchor");
                run_restart_child(&root, "reject-missing-anchor");
                assert_eq!(snapshot(&root), before, "legacy history={history}");
                println!("legacy-preserved policy_backed={policy_backed} history={history}");
            }
        }
    }

    #[test]
    fn bootstrap_identity_is_independently_anchored_across_processes() {
        for policy_backed in [false, true] {
            let root = new_normal_deployment_fixture(policy_backed);
            normal_clean_fixture(&root, "before-bootstrap-offline-replacement");
            let path = root.join(MANIFEST_FILE);
            let original = fs::read(&path).unwrap();
            let old_meta = fs::metadata(&path).unwrap();
            fs::rename(&path, root.join("bootstrap.test-original")).unwrap();
            let mut replacement = create_private(&path).unwrap();
            replacement.write_all(&original).unwrap();
            replacement.sync_all().unwrap();
            File::open(&root).unwrap().sync_all().unwrap();
            let new_meta = replacement.metadata().unwrap();
            assert_ne!(
                (old_meta.dev(), old_meta.ino()),
                (new_meta.dev(), new_meta.ino())
            );
            assert_eq!(fs::read(&path).unwrap(), original);
            let replaced = snapshot(&root);
            run_restart_child(&root, "reject-bootstrap-inode");
            run_restart_child(&root, "reject-bootstrap-inode");
            assert_eq!(snapshot(&root), replaced);
            assert!(open(&root).is_err());
            // Restoration occurs only in this isolated test, to prove that
            // the already-poisoned process cannot self-clear after repair.
            fs::remove_file(&path).unwrap();
            fs::rename(root.join("bootstrap.test-original"), &path).unwrap();
            let mut same_process = open(&root).unwrap();
            assert!(!same_process.admits_new_work());
            assert!(same_process.before_admission().is_err());
            drop(same_process);
            run_restart_child(&root, "admit");
        }
    }

    #[test]
    fn rewritten_bootstrap_identity_cannot_bypass_pinned_state_digest() {
        for policy_backed in [false, true] {
            let root = new_normal_deployment_fixture(policy_backed);
            normal_clean_fixture(&root, "before-changed-config");
            let path = root.join(MANIFEST_FILE);
            let mut config = Bootstrap::parse(&fs::read_to_string(&path).unwrap()).unwrap();
            fs::rename(&path, root.join("bootstrap.test-original")).unwrap();
            let mut replacement = create_private(&path).unwrap();
            let meta = replacement.metadata().unwrap();
            config.manifest_identity = Some((meta.dev(), meta.ino()));
            replacement.write_all(config.encode().as_bytes()).unwrap();
            replacement.sync_all().unwrap();
            File::open(&root).unwrap().sync_all().unwrap();
            let before = snapshot(&root);
            run_restart_child(&root, "reject-config-anchor");
            assert_eq!(snapshot(&root), before);
        }
    }

    #[test]
    fn pinned_bootstrap_requires_canonical_complete_identity_and_version() {
        for policy_backed in [false, true] {
            let root = new_normal_deployment_fixture(policy_backed);
            let original = fs::read_to_string(root.join(MANIFEST_FILE)).unwrap();
            let value = Bootstrap::parse(&original).unwrap();
            let (dev, ino) = value.manifest_identity.unwrap();
            let dev_line = format!("bootstrap_dev={dev}\n");
            let ino_line = format!("bootstrap_ino={ino}\n");
            let legacy = if policy_backed {
                DEPLOYMENT_FORMAT
            } else {
                NORMAL_FORMAT
            };
            let version = if policy_backed {
                PINNED_DEPLOYMENT_FORMAT
            } else {
                PINNED_NORMAL_FORMAT
            };
            for invalid_text in [
                original.replace(&dev_line, ""),
                original.replace(&ino_line, ""),
                original.replace(&dev_line, &format!("bootstrap_dev=0{dev}\n")),
                original.replace(&ino_line, &format!("{ino_line}{ino_line}")),
                original.replacen(version, legacy, 1),
            ] {
                assert!(Bootstrap::parse(&invalid_text).is_err());
            }
        }
    }

    #[test]
    fn early_invalid_state_poison_survives_restoration_and_object_reopen() {
        let parent = new_root();
        fs::DirBuilder::new().mode(0o700).create(&parent).unwrap();
        let root = parent.join("authority");
        initialize_at_mode_inner(&root, "host", "boot", ENDPOINT, 16, 64, None, true, |_| {
            Ok(())
        })
        .unwrap();
        normal_clean_fixture(&root, "before-early-failure");
        let before = snapshot(&root);
        let state = fs::read(root.join(STATE_FILE)).unwrap();
        fs::write(root.join(STATE_FILE), b"partial\n").unwrap();
        assert!(open(&root).is_err());
        fs::write(root.join(STATE_FILE), state).unwrap();
        assert_eq!(snapshot(&root), before);
        let mut reopened = open(&root).unwrap();
        assert!(!reopened.admits_new_work());
        assert!(reopened.start_lifecycle_audit("same-process").is_err());
        assert!(reopened.before_admission().is_err());
        drop(reopened);
        // Restoration is fixture-only; an independent process must perform
        // every real restart barrier before it can write dirty.
        run_restart_child(&root, "sync-fails");
        run_restart_child(&root, "dirty-fails");
        assert_eq!(snapshot(&root), before);
        run_restart_child(&root, "admit");
        run_restart_child(&root, "deny");
    }

    #[test]
    fn missing_lock_open_failure_poison_survives_restoring_original_inode() {
        let root = new_normal_deployment_fixture(false);
        normal_clean_fixture(&root, "before-missing-lock");
        let before = snapshot(&root);
        fs::rename(root.join(LOCK_FILE), root.join("lock.test-original")).unwrap();
        assert!(open(&root).is_err());
        fs::rename(root.join("lock.test-original"), root.join(LOCK_FILE)).unwrap();
        assert_eq!(snapshot(&root), before);
        let mut reopened = open(&root).unwrap();
        assert!(!reopened.admits_new_work());
        assert!(reopened.before_admission().is_err());
    }

    #[test]
    fn missing_root_open_failure_poison_survives_restoring_original_directory() {
        let root = new_normal_deployment_fixture(false);
        normal_clean_fixture(&root, "before-missing-root");
        let before = snapshot(&root);
        let parked = root.with_file_name("authority.test-original");
        fs::rename(&root, &parked).unwrap();
        assert!(open(&root).is_err());
        fs::rename(&parked, &root).unwrap();
        assert_eq!(snapshot(&root), before);
        let mut reopened = open(&root).unwrap();
        assert!(!reopened.admits_new_work());
        assert!(reopened.before_admission().is_err());
    }

    #[test]
    fn audit_start_namespace_failure_poison_survives_restore_and_reopen() {
        let root = new_normal_deployment_fixture(false);
        normal_clean_fixture(&root, "before-audit-open-failure");
        let mut authority = open(&root).unwrap();
        let before = snapshot(&root);
        let head = fs::read(root.join(JOURNAL_HEAD_FILE)).unwrap();
        fs::write(root.join(JOURNAL_HEAD_FILE), b"invalid-head\n").unwrap();
        assert!(authority.start_lifecycle_audit("new-broker").is_err());
        fs::write(root.join(JOURNAL_HEAD_FILE), head).unwrap();
        assert_eq!(snapshot(&root), before);
        assert!(!authority.admits_new_work());
        assert!(authority.before_admission().is_err());
        drop(authority);
        let mut reopened = open(&root).unwrap();
        assert!(!reopened.admits_new_work());
        assert!(reopened.before_admission().is_err());
    }

    #[test]
    fn identical_bootstrap_replacement_during_confirmation_is_not_accepted() {
        let parent = new_root();
        fs::DirBuilder::new().mode(0o700).create(&parent).unwrap();
        let root = parent.join("authority");
        initialize_at_mode_inner(&root, "host", "boot", ENDPOINT, 16, 64, None, true, |_| {
            Ok(())
        })
        .unwrap();
        normal_clean_fixture(&root, "before-bootstrap-replacement");
        let old = fs::read(root.join(MANIFEST_FILE)).unwrap();
        let lock = private_file(&root.join(LOCK_FILE), true).unwrap();
        assert_eq!(
            unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0
        );
        let failed = ManagedAuthority::open_at_held_lock_with_hook(
            &root,
            "host",
            "boot",
            ENDPOINT,
            16,
            64,
            lock,
            |stage| {
                if stage == RestartStage::BeforeFinalVerification {
                    fs::rename(
                        root.join(MANIFEST_FILE),
                        root.join("bootstrap.test-original"),
                    )?;
                    let mut replacement = create_private(&root.join(MANIFEST_FILE))?;
                    replacement.write_all(&old)?;
                    replacement.sync_all()?;
                }
                Ok(())
            },
        );
        assert!(
            failed.is_err(),
            "same-byte inode replacement passed final check"
        );
        // Keep the renamed original as evidence, then restore it only in this
        // isolated test so we can verify sticky in-process poison.
        fs::remove_file(root.join(MANIFEST_FILE)).unwrap();
        fs::rename(
            root.join("bootstrap.test-original"),
            root.join(MANIFEST_FILE),
        )
        .unwrap();
        let mut again = open(&root).unwrap();
        assert!(!again.admits_new_work());
        assert!(again.before_admission().is_err());
    }

    #[test]
    fn dirty_double_faults_follow_actual_persisted_state_on_new_process_restart() {
        let stages = [
            StateWriteStage::BeforeTruncate,
            StateWriteStage::AfterTruncate,
            StateWriteStage::AfterPartialWrite,
            StateWriteStage::BeforeSync,
            StateWriteStage::AfterSync,
            StateWriteStage::BeforeDirectorySync,
            StateWriteStage::AfterDirectorySync,
            StateWriteStage::BeforeReadbackOpen,
            StateWriteStage::BeforeReadbackMetadata,
            StateWriteStage::BeforeReadbackRead,
            StateWriteStage::AfterReadback,
        ];
        for policy_backed in [false, true] {
            for primary in stages {
                for rollback in stages {
                    let root = new_normal_deployment_fixture(policy_backed);
                    normal_clean_fixture(&root, "previous-clean-broker");
                    let before = snapshot(&root);
                    let mut authority = open(&root).unwrap();
                    let error = authority
                        .before_admission_with_faults(
                            |stage| {
                                if stage == primary {
                                    Err(io::Error::from_raw_os_error(libc::EIO))
                                } else {
                                    Ok(())
                                }
                            },
                            |stage| {
                                if stage == rollback {
                                    Err(io::Error::from_raw_os_error(libc::ENOSPC))
                                } else {
                                    Ok(())
                                }
                            },
                        )
                        .unwrap_err()
                        .to_string();
                    assert!(error.contains("dirty retry failed"), "{error}");
                    assert!(!authority.admits_new_work());
                    assert!(authority.before_admission().is_err());
                    drop(authority);
                    let after = snapshot(&root);
                    let unchanged = after == before;
                    assert_eq!(
                        unchanged,
                        primary == StateWriteStage::BeforeTruncate
                            && rollback == StateWriteStage::BeforeTruncate,
                        "policy_backed={policy_backed} primary={primary:?} rollback={rollback:?}"
                    );
                    println!("dirty-fault policy_backed={policy_backed} primary={primary:?} rollback={rollback:?} unchanged={unchanged} state_sha256={} root={root:?} result={error}",
                        crate::ddr_recovery_coverage::digest(&fs::read(root.join(STATE_FILE)).unwrap()));
                    if unchanged {
                        let mut again = open(&root).unwrap();
                        assert!(!again.admits_new_work());
                        assert!(again.before_admission().is_err());
                        drop(again);
                        run_restart_child(&root, "sync-fails");
                        run_restart_child(&root, "dirty-fails");
                        assert_eq!(snapshot(&root), before);
                        run_restart_child(&root, "admit");
                    } else {
                        run_restart_child(&root, "deny");
                        run_restart_child(&root, "deny");
                        assert_eq!(
                            snapshot(&root),
                            after,
                            "policy_backed={policy_backed} primary={primary:?} rollback={rollback:?}"
                        );
                    }
                    run_restart_child(&root, "deny");
                }
            }
        }
    }

    #[test]
    fn clean_double_faults_poison_live_and_require_independent_restart_confirmation() {
        use crate::ddr_recovery_audit::{Event, EventKind};
        let rollback_faults = [
            StateWriteStage::BeforeTruncate,
            StateWriteStage::AfterTruncate,
            StateWriteStage::AfterPartialWrite,
            StateWriteStage::BeforeSync,
            StateWriteStage::AfterSync,
            StateWriteStage::BeforeDirectorySync,
            StateWriteStage::AfterDirectorySync,
            StateWriteStage::BeforeReadbackOpen,
            StateWriteStage::BeforeReadbackMetadata,
            StateWriteStage::BeforeReadbackRead,
            StateWriteStage::AfterReadback,
        ];
        let primary_faults = [
            Some(StateWriteStage::BeforeTruncate),
            Some(StateWriteStage::AfterTruncate),
            Some(StateWriteStage::AfterPartialWrite),
            Some(StateWriteStage::BeforeSync),
            Some(StateWriteStage::AfterSync),
            Some(StateWriteStage::BeforeDirectorySync),
            Some(StateWriteStage::AfterDirectorySync),
            Some(StateWriteStage::BeforeReadbackOpen),
            Some(StateWriteStage::BeforeReadbackMetadata),
            Some(StateWriteStage::BeforeReadbackRead),
            Some(StateWriteStage::AfterReadback),
            None, // final namespace verification after a complete clean write
        ];
        for policy_backed in [false, true] {
            for primary in primary_faults {
                let final_failure = primary.is_none();
                for rollback_failure in rollback_faults {
                    let root = new_normal_deployment_fixture(policy_backed);
                    let mut authority = open(&root).unwrap();
                    let mut audit = authority
                        .start_lifecycle_audit("double-fault-broker")
                        .unwrap();
                    authority.before_admission().unwrap();
                    let birth = crate::ddr_process_identity::observe(
                        std::process::id() as i32,
                        unsafe { libc::geteuid() },
                        unsafe { libc::getegid() },
                    )
                    .unwrap();
                    audit
                        .record(Event {
                            domain: audit.domain().clone(),
                            epoch: 0,
                            kind: EventKind::BrokerBirth { birth },
                        })
                        .unwrap();
                    let joined = crate::external_ddr::fixture_joined_drain("double-fault-broker");
                    audit
                        .record_joined_drain(&joined, 0, audit.replay().observed_drain(0, 0))
                        .unwrap();
                    let seal = audit.checkpoint_after_drain(&joined).unwrap();
                    let error = authority
                        .clean_after_semantic_drain_with_faults(
                            &drained(),
                            seal,
                            |stage| {
                                if Some(stage) == primary {
                                    Err(io::Error::from_raw_os_error(libc::EIO))
                                } else {
                                    Ok(())
                                }
                            },
                            |stage| {
                                if stage == rollback_failure {
                                    Err(io::Error::from_raw_os_error(libc::ENOSPC))
                                } else {
                                    Ok(())
                                }
                            },
                            || {
                                if final_failure {
                                    Err(io::Error::from_raw_os_error(libc::EIO))
                                } else {
                                    Ok(())
                                }
                            },
                        )
                        .unwrap_err()
                        .to_string();
                    assert!(error.contains(if final_failure {
                        "phase=final_namespace"
                    } else {
                        "phase=write_or_sync"
                    }));
                    assert!(error.contains("rollback=unknown:"), "{error}");
                    assert!(!authority.admits_new_work());
                    assert!(authority.before_admission().is_err());
                    drop((audit, authority));
                    let before = snapshot(&root);
                    let complete_clean_candidate = rollback_failure
                        == StateWriteStage::BeforeTruncate
                        && (final_failure
                            || matches!(
                                primary,
                                Some(
                                    StateWriteStage::BeforeSync
                                        | StateWriteStage::AfterSync
                                        | StateWriteStage::BeforeDirectorySync
                                        | StateWriteStage::AfterDirectorySync
                                        | StateWriteStage::BeforeReadbackOpen
                                        | StateWriteStage::BeforeReadbackMetadata
                                        | StateWriteStage::BeforeReadbackRead
                                        | StateWriteStage::AfterReadback
                                )
                            ));
                    println!("clean-fault policy_backed={policy_backed} primary={primary:?} rollback={rollback_failure:?} candidate={complete_clean_candidate} result={error}");
                    if complete_clean_candidate {
                        let reopened = open(&root).unwrap();
                        assert!(!reopened.admits_new_work(), "same process cannot unpoison");
                        drop(reopened);
                        run_restart_child(&root, "sync-fails");
                        run_restart_child(&root, "dirty-fails");
                        assert_eq!(snapshot(&root), before);
                        run_restart_child(&root, "admit");
                        assert!(fs::read_to_string(root.join(STATE_FILE))
                            .unwrap()
                            .contains("\"mode\":\"dirty\""));
                        run_restart_child(&root, "deny");
                    } else {
                        run_restart_child(&root, "deny");
                        assert_eq!(
                            snapshot(&root),
                            before,
                            "unknown or dirty cannot be repaired"
                        );
                        run_restart_child(&root, "deny");
                        assert_eq!(snapshot(&root), before);
                    }
                }
            }
        }
    }

    #[test]
    fn changed_state_during_readback_never_publishes_clean() {
        use crate::ddr_recovery_audit::{Event, EventKind};
        let root = new_normal_deployment_fixture(false);
        let mut authority = open(&root).unwrap();
        let mut audit = authority.start_lifecycle_audit("readback-broker").unwrap();
        authority.before_admission().unwrap();
        let birth = crate::ddr_process_identity::observe(
            std::process::id() as i32,
            unsafe { libc::geteuid() },
            unsafe { libc::getegid() },
        )
        .unwrap();
        audit
            .record(Event {
                domain: audit.domain().clone(),
                epoch: 0,
                kind: EventKind::BrokerBirth { birth },
            })
            .unwrap();
        let joined = crate::external_ddr::fixture_joined_drain("readback-broker");
        audit
            .record_joined_drain(&joined, 0, audit.replay().observed_drain(0, 0))
            .unwrap();
        let seal = audit.checkpoint_after_drain(&joined).unwrap();
        let error = authority
            .clean_after_semantic_drain_inner(&drained(), seal, |stage| {
                if stage == StateWriteStage::BeforeReadbackRead {
                    fs::write(root.join(STATE_FILE), b"mismatched readback\n")?;
                }
                Ok(())
            })
            .unwrap_err()
            .to_string();
        assert!(error.contains("readback mismatch"), "{error}");
        assert!(
            error.contains("rollback=dirty_write_sync_readback_succeeded"),
            "{error}"
        );
        assert!(!authority.admits_new_work());
        drop((audit, authority));
        let before = snapshot(&root);
        run_restart_child(&root, "deny");
        run_restart_child(&root, "deny");
        assert_eq!(snapshot(&root), before);
    }

    #[test]
    fn diagnostic_prior_clean_and_failed_dirty_attempt_leave_identical_restart_inputs() {
        use crate::ddr_recovery_audit::{Event, EventKind};
        let parent = new_root();
        fs::DirBuilder::new().mode(0o700).create(&parent).unwrap();
        let root = parent.join("authority");
        initialize_at_mode_inner(&root, "host", "boot", ENDPOINT, 16, 64, None, true, |_| {
            Ok(())
        })
        .unwrap();
        let mut previous = open(&root).unwrap();
        let mut audit = previous.start_lifecycle_audit("prior-broker").unwrap();
        previous.before_admission().unwrap();
        let birth = crate::ddr_process_identity::observe(
            std::process::id() as i32,
            unsafe { libc::geteuid() },
            unsafe { libc::getegid() },
        )
        .unwrap();
        audit
            .record(Event {
                domain: audit.domain().clone(),
                epoch: 0,
                kind: EventKind::BrokerBirth { birth },
            })
            .unwrap();
        let joined = crate::external_ddr::fixture_joined_drain("prior-broker");
        audit
            .record_joined_drain(&joined, 0, audit.replay().observed_drain(0, 0))
            .unwrap();
        let seal = audit.checkpoint_after_drain(&joined).unwrap();
        previous
            .clean_after_semantic_drain(&drained(), seal)
            .unwrap();
        drop((audit, previous));
        let mut failed = open(&root).unwrap();
        let before = snapshot(&root);
        let error = failed
            .before_admission_with_faults(
                |stage| {
                    if stage == StateWriteStage::BeforeTruncate {
                        Err(io::Error::from_raw_os_error(libc::EIO))
                    } else {
                        Ok(())
                    }
                },
                |stage| {
                    if stage == StateWriteStage::BeforeTruncate {
                        Err(io::Error::from_raw_os_error(libc::ENOSPC))
                    } else {
                        Ok(())
                    }
                },
            )
            .unwrap_err()
            .to_string();
        assert!(error.contains("dirty retry failed"), "{error}");
        assert!(!failed.admits_new_work());
        assert_eq!(
            snapshot(&root),
            before,
            "both failures occurred before any state change"
        );
        drop(failed);
        let reopened = open(&root).unwrap();
        assert!(!reopened.admits_new_work(), "old process remains poisoned");
        drop(reopened);
        // Diagnostic observation, not an assertion that this B contract
        // authorizes arbitrary admission failures to be cleared on restart.
        run_restart_child(&root, "admit");
    }

    #[test]
    fn normal_state_write_and_sync_faults_poison_and_preserve_dirty_on_retry() {
        use crate::ddr_recovery_audit::{Event, EventKind};
        for clean_write in [false, true] {
            for stage in [
                StateWriteStage::BeforeTruncate,
                StateWriteStage::AfterTruncate,
                StateWriteStage::AfterPartialWrite,
                StateWriteStage::BeforeSync,
                StateWriteStage::AfterSync,
                StateWriteStage::BeforeDirectorySync,
                StateWriteStage::AfterDirectorySync,
                StateWriteStage::BeforeReadbackOpen,
                StateWriteStage::BeforeReadbackMetadata,
                StateWriteStage::BeforeReadbackRead,
                StateWriteStage::AfterReadback,
            ] {
                let parent = new_root();
                fs::DirBuilder::new().mode(0o700).create(&parent).unwrap();
                let root = parent.join("authority");
                initialize_at_mode_inner(
                    &root,
                    "host",
                    "boot",
                    ENDPOINT,
                    16,
                    64,
                    None,
                    true,
                    |_| Ok(()),
                )
                .unwrap();
                let mut authority = open(&root).unwrap();
                let mut audit = authority.start_lifecycle_audit("fault-broker").unwrap();
                let inject = |actual| {
                    if actual == stage {
                        Err(io::Error::from_raw_os_error(libc::EIO))
                    } else {
                        Ok(())
                    }
                };
                if clean_write {
                    authority.before_admission().unwrap();
                    let birth = crate::ddr_process_identity::observe(
                        std::process::id() as i32,
                        unsafe { libc::geteuid() },
                        unsafe { libc::getegid() },
                    )
                    .unwrap();
                    audit
                        .record(Event {
                            domain: audit.domain().clone(),
                            epoch: 0,
                            kind: EventKind::BrokerBirth { birth },
                        })
                        .unwrap();
                    let joined = crate::external_ddr::fixture_joined_drain("fault-broker");
                    audit
                        .record_joined_drain(&joined, 0, audit.replay().observed_drain(0, 0))
                        .unwrap();
                    let seal = audit.checkpoint_after_drain(&joined).unwrap();
                    assert!(authority
                        .clean_after_semantic_drain_inner(&drained(), seal, inject)
                        .is_err());
                } else {
                    assert!(authority.before_admission_inner(inject).is_err());
                    assert_eq!(authority.open_journal().unwrap().head().sequence, 0);
                }
                assert!(!authority.admits_new_work(), "stage={stage:?}");
                assert!(authority.before_admission().is_err());
                let state: serde_json::Value =
                    serde_json::from_slice(&fs::read(root.join(STATE_FILE)).unwrap()).unwrap();
                assert_eq!(
                    state["mode"], "dirty",
                    "stage={stage:?} clean={clean_write}"
                );
                drop(audit);
                drop(authority);
                let mut reopened = open(&root).unwrap();
                assert!(
                    reopened.is_inherited_quarantine(),
                    "stage={stage:?} clean={clean_write}"
                );
                assert!(reopened.before_admission().is_err());
            }
        }
    }

    #[test]
    fn policy_backed_ordinary_use_does_not_require_witness_artifact() {
        let (root, artifact, policy) = deployment_fixture();
        initialize_deployed_fixture(&root, &policy);
        fs::remove_file(&artifact).unwrap();
        let trust = fs::read(root.join(WITNESS_TRUST_FILE)).unwrap();
        assert!(
            crate::ddr_deployment_policy::DeploymentPolicy::from_initial_chain(&trust).is_err()
        );
        let mut authority = open(&root).unwrap();
        assert!(authority.admits_new_work());
        let _audit = authority.start_lifecycle_audit("ordinary-broker").unwrap();
        authority.before_admission().unwrap();
        assert!(authority.admits_new_work());
    }

    #[test]
    fn deployed_dirty_restart_stays_quarantined_even_after_unpublished_checkpoint() {
        use crate::ddr_recovery_audit::{Event, EventKind};
        for stop_after in 0..=3 {
            let (root, _, policy) = deployment_fixture();
            initialize_deployed_fixture(&root, &policy);
            let mut authority = open(&root).unwrap();
            let mut audit = authority
                .start_lifecycle_audit("interrupted-broker")
                .unwrap();
            authority.before_admission().unwrap();
            if stop_after >= 1 {
                let birth = crate::ddr_process_identity::observe(
                    std::process::id() as i32,
                    unsafe { libc::geteuid() },
                    unsafe { libc::getegid() },
                )
                .unwrap();
                audit
                    .record(Event {
                        domain: audit.domain().clone(),
                        epoch: 0,
                        kind: EventKind::BrokerBirth { birth },
                    })
                    .unwrap();
            }
            let joined = crate::external_ddr::fixture_joined_drain("interrupted-broker");
            if stop_after >= 2 {
                audit
                    .record_joined_drain(&joined, 0, audit.replay().observed_drain(0, 0))
                    .unwrap();
            }
            if stop_after >= 3 {
                let _unpublished = audit.checkpoint_after_drain(&joined).unwrap();
            }
            drop(audit);
            drop(authority);
            let before = snapshot(&root);
            let mut reopened = open(&root).unwrap();
            assert!(
                reopened.is_inherited_quarantine(),
                "stop_after={stop_after}"
            );
            assert!(!reopened.admits_new_work());
            assert!(reopened.before_admission().is_err());
            assert_eq!(snapshot(&root), before, "reopen must not repair or resell");
        }
    }

    #[test]
    fn deployment_genesis_refuses_changed_policy_artifact_and_incomplete_publication() {
        let (root, artifact, policy) = deployment_fixture();
        fs::write(&artifact, b"changed after trusted policy was loaded\n").unwrap();
        assert!(initialize_with_policy_at_inner(
            &root,
            "host",
            "boot",
            ENDPOINT,
            16,
            64,
            Some(&policy),
            |_| Ok(())
        )
        .is_err());
        assert!(!root.exists());
        for stop in [
            BootstrapStage::WitnessCreated,
            BootstrapStage::ComponentsSynced,
            BootstrapStage::DirectoriesSynced,
            BootstrapStage::ManifestSynced,
        ] {
            let (root, _, policy) = deployment_fixture();
            let result = initialize_with_policy_at_inner(
                &root,
                "host",
                "boot",
                ENDPOINT,
                16,
                64,
                Some(&policy),
                |stage| {
                    if stage == stop {
                        Err(io::Error::other("injected bootstrap interruption"))
                    } else {
                        Ok(())
                    }
                },
            );
            assert!(result.is_err());
            assert!(!root.join(MANIFEST_FILE).exists());
            let before = snapshot(&root);
            assert!(open(&root).is_err());
            assert!(initialize_with_policy_at_inner(
                &root,
                "host",
                "boot",
                ENDPOINT,
                16,
                64,
                Some(&policy),
                |_| Ok(())
            )
            .is_err());
            assert_eq!(snapshot(&root), before);
        }
    }

    #[test]
    fn deployment_genesis_refuses_trust_state_and_history_tampering() {
        for fault in [
            "empty-trust",
            "extra-frame",
            "state",
            "trust-anchor",
            "pending",
            "config",
            "journal",
        ] {
            let (root, _, policy) = deployment_fixture();
            initialize_deployed_fixture(&root, &policy);
            match fault {
                "empty-trust" => fs::write(root.join(WITNESS_TRUST_FILE), b"").unwrap(),
                "extra-frame" => OpenOptions::new()
                    .append(true)
                    .open(root.join(WITNESS_TRUST_FILE))
                    .unwrap()
                    .write_all(b"{}\n")
                    .unwrap(),
                "state" => fs::write(root.join(STATE_FILE), b"clean\n").unwrap(),
                "trust-anchor" | "pending" => {
                    let path = root.join(STATE_FILE);
                    let mut state: serde_json::Value =
                        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
                    if fault == "trust-anchor" {
                        state["trust_sha256"] = "0".repeat(64).into();
                    } else {
                        state["pending"] = serde_json::json!({"type": "BIND", "id": "interrupted"});
                    }
                    let mut text = crate::ddr_recovery_coverage::canonical(&state).unwrap();
                    text.push(b'\n');
                    fs::write(path, text).unwrap();
                }

                "config" => {
                    let path = root.join(MANIFEST_FILE);
                    let text = fs::read_to_string(&path)
                        .unwrap()
                        .replace("revision=1\n", "revision=2\n");
                    fs::write(path, text).unwrap();
                }
                "journal" => {
                    fs::write(root.join(JOURNAL_FILE), b"unexpected complete tail").unwrap()
                }
                _ => unreachable!(),
            }
            let before = snapshot(&root);
            assert!(open(&root).is_err(), "fault={fault}");
            assert_eq!(snapshot(&root), before, "fault={fault}");
        }
    }

    #[test]
    fn deployment_policy_rejects_missing_roles_wrong_digest_and_noncanonical_inputs() {
        let (root, _, _) = deployment_fixture();
        let path = root.parent().unwrap().join("policy.json");
        let original = fs::read(&path).unwrap();
        for fault in [
            "missing-role",
            "duplicate-role",
            "bad-digest",
            "unknown-field",
            "bad-argv",
            "noncanonical",
        ] {
            let mut policy: serde_json::Value = serde_json::from_slice(&original).unwrap();
            match fault {
                "missing-role" => {
                    policy["roles"].as_array_mut().unwrap().pop();
                }
                "duplicate-role" => {
                    policy["roles"][1]["role"] = policy["roles"][0]["role"].clone();
                }
                "bad-digest" => {
                    policy["roles"][0]["entrypoint"]["sha256"] = "0".repeat(64).into();
                }
                "unknown-field" => {
                    policy["roles"][0]["unexpected"] = true.into();
                }
                "bad-argv" => {
                    policy["roles"][0]["argv"] = serde_json::json!([]);
                }
                "noncanonical" => {}
                _ => unreachable!(),
            }
            let mut bytes = crate::ddr_recovery_coverage::canonical(&policy).unwrap();
            if fault == "noncanonical" {
                bytes.insert(1, b' ');
            }
            bytes.push(b'\n');
            fs::write(&path, bytes).unwrap();
            assert!(
                crate::ddr_deployment_policy::DeploymentPolicy::load(&path).is_err(),
                "fault={fault}"
            );
            assert!(!root.exists());
        }
    }

    #[test]
    fn synthetic_versioned_binding_is_diagnostic_only_and_pinned_to_same_authority() {
        use crate::ddr_recovery_coverage::{canonical, digest, parse_line, s};
        for fault in ["none", "host", "journal", "closure", "pending"] {
            let (root, _, policy) = deployment_fixture();
            initialize_deployed_fixture(&root, &policy);
            let bootstrap =
                Bootstrap::parse(&fs::read_to_string(root.join(MANIFEST_FILE)).unwrap()).unwrap();
            let mut trust = fs::read(root.join(WITNESS_TRUST_FILE)).unwrap();
            let deploy = parse_line(&trust).unwrap();
            let birth = |pid: u64| {
                serde_json::json!({"pid": pid, "uid": 1, "gid": 1,
                "pidns_dev": 1, "pidns_ino": 2, "starttime_ticks": pid + 10})
            };
            let payload = serde_json::json!({"generation": "g1", "previous_seal": "0".repeat(64),
                "policy_sha256": policy.policy_digest().unwrap(), "host": if fault == "host" { "wrong" } else { "host" },
                "boot": "boot", "domain": DOMAIN, "revision": 1, "owner_limit": 16, "total_limit": 64,
                "endpoint": ENDPOINT, "initial_head": crate::ddr_recovery_journal::AnchoredJournal::genesis_state(),
                "journal": [bootstrap.journal_dev, if fault == "journal" { bootstrap.journal_ino + 1 } else { bootstrap.journal_ino }],
                "journal_head": [bootstrap.journal_head_dev, bootstrap.journal_head_ino], "broker": "b1",
                "broker_birth": birth(100), "witness_birth": birth(101),
                "evidence_dir": "/workspace/uflow/runs/synthetic/evidence", "evidence_dir_id": [999999, 1],
                "evidence_journal": [999999, 2], "evidence_head": [999999, 3], "evidence_initial_sha256": "1".repeat(64)});
            let mut frame = serde_json::json!({"schema": "uflow-ddr-trust-chain-v1", "sequence": 2,
                "previous": s(&deploy, "checksum").unwrap(), "type": "BIND", "payload": payload});
            frame["checksum"] = digest(&canonical(&frame).unwrap()).into();
            trust.extend(canonical(&frame).unwrap());
            trust.push(b'\n');
            let state_path = root.join(STATE_FILE);
            let mut state: serde_json::Value =
                serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
            state["trust_sequence"] = 2.into();
            state["trust_offset"] = (trust.len() as u64).into();
            state["trust_sha256"] = digest(&trust).into();
            state["generation"] = "g1".into();
            state["closure"] = if fault == "closure" {
                "sealed"
            } else {
                "bound"
            }
            .into();
            if fault == "pending" {
                state["pending"] = serde_json::json!({"type": "BIND", "id": "interrupted"});
            }
            let mut encoded = canonical(&state).unwrap();
            encoded.push(b'\n');
            // Synthetic records are confined to this task-owned fixture; they
            // are not real birth evidence and never authorize admission.
            fs::write(root.join(WITNESS_TRUST_FILE), &trust).unwrap();
            fs::write(&state_path, &encoded).unwrap();
            let before = snapshot(&root);
            if fault == "none" {
                let mut authority = open(&root).unwrap();
                assert!(authority.is_inherited_quarantine());
                assert!(authority.before_admission().is_err());
                assert!(!authority.admits_new_work());
                let other = private_file(&root.join(LOCK_FILE), true).unwrap();
                assert_ne!(
                    unsafe { libc::flock(other.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
                    0
                );
                assert_eq!(snapshot(&root), before);
            } else {
                assert!(open(&root).is_err(), "fault={fault}");
                assert_eq!(snapshot(&root), before, "fault={fault}");
            }
        }
    }

    #[test]
    fn bootstrap_interrupted_before_publication_is_preserved_and_never_opened() {
        for stop in [
            BootstrapStage::RootCreated,
            BootstrapStage::LockCreated,
            BootstrapStage::StateCreated,
            BootstrapStage::JournalCreated,
            BootstrapStage::HeadCreated,
            BootstrapStage::WitnessCreated,
            BootstrapStage::ComponentsSynced,
            BootstrapStage::DirectoriesSynced,
            BootstrapStage::ManifestSynced,
        ] {
            let root = new_root();
            let mut reached = false;
            let result = initialize_at_inner(&root, "host", "boot", ENDPOINT, 16, 64, |stage| {
                if stage == stop {
                    reached = true;
                    return Err(io::Error::other("injected bootstrap interruption"));
                }
                Ok(())
            });
            assert!(result.is_err() && reached, "stage {stop:?}");
            assert!(!root.join(MANIFEST_FILE).exists(), "stage {stop:?}");
            let before = snapshot(&root);
            assert!(open(&root).is_err(), "stage {stop:?}");
            assert!(initialize_at(&root, "host", "boot", ENDPOINT, 16, 64).is_err());
            assert_eq!(snapshot(&root), before, "stage {stop:?}");
        }
    }

    #[test]
    fn bootstrap_publication_is_last_and_noreplace() {
        let root = new_root();
        let mut stages = Vec::new();
        initialize_at_inner(&root, "host", "boot", ENDPOINT, 16, 64, |stage| {
            stages.push(stage);
            if stage != BootstrapStage::Published {
                assert!(!root.join(MANIFEST_FILE).exists());
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(stages.last(), Some(&BootstrapStage::Published));
        assert!(!root.join(PENDING_MANIFEST_FILE).exists());
        assert_eq!(fs::metadata(&root).unwrap().mode() & 0o777, 0o700);
        for entry in fs::read_dir(&root).unwrap() {
            assert_eq!(entry.unwrap().metadata().unwrap().mode() & 0o777, 0o600);
        }
        let guard = open(&root).unwrap();
        assert_eq!(guard.open_journal().unwrap().head().sequence, 0);
        drop(guard);

        let occupied = new_root();
        let result = initialize_at_inner(&occupied, "host", "boot", ENDPOINT, 16, 64, |stage| {
            if stage == BootstrapStage::ManifestSynced {
                let mut intruder = create_private(&occupied.join(MANIFEST_FILE))?;
                intruder.write_all(b"unknown existing bootstrap\n")?;
                intruder.sync_all()?;
            }
            Ok(())
        });
        assert!(result.is_err());
        let before = snapshot(&occupied);
        assert_eq!(
            fs::read(occupied.join(MANIFEST_FILE)).unwrap(),
            b"unknown existing bootstrap\n"
        );
        assert!(open(&occupied).is_err());
        assert!(initialize_at(&occupied, "host", "boot", ENDPOINT, 16, 64).is_err());
        assert_eq!(snapshot(&occupied), before);
    }

    #[test]
    fn staged_bootstrap_replacement_before_publication_is_preserved_and_rejected() {
        for policy_backed in [false, true] {
            let (root, _, policy) = deployment_fixture();
            let result = initialize_at_mode_inner(
                &root,
                "host",
                "boot",
                ENDPOINT,
                16,
                64,
                policy_backed.then_some(&policy),
                !policy_backed,
                |stage| {
                    if stage == BootstrapStage::ManifestSynced {
                        let path = root.join(PENDING_MANIFEST_FILE);
                        let bytes = fs::read(&path)?;
                        fs::rename(&path, root.join("bootstrap.pending.test-original"))?;
                        let mut replacement = create_private(&path)?;
                        replacement.write_all(&bytes)?;
                        replacement.sync_all()?;
                    }
                    Ok(())
                },
            );
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("staged bootstrap changed"));
            assert!(!root.join(MANIFEST_FILE).exists());
            let before = snapshot(&root);
            assert!(open(&root).is_err());
            assert!(initialize_at_mode_inner(
                &root,
                "host",
                "boot",
                ENDPOINT,
                16,
                64,
                policy_backed.then_some(&policy),
                !policy_backed,
                |_| Ok(())
            )
            .is_err());
            assert_eq!(snapshot(&root), before);
        }
    }

    #[test]
    fn existing_or_symlinked_root_is_not_reused_or_modified() {
        let root = new_root();
        fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
        let mut sentinel = create_private(&root.join("unknown")).unwrap();
        sentinel.write_all(b"retain me\n").unwrap();
        sentinel.sync_all().unwrap();
        let before = snapshot(&root);
        assert!(initialize_at(&root, "host", "boot", ENDPOINT, 16, 64).is_err());
        assert_eq!(snapshot(&root), before);
        let link = new_root();
        std::os::unix::fs::symlink(&root, &link).unwrap();
        assert!(initialize_at(&link, "host", "boot", ENDPOINT, 16, 64).is_err());
        assert!(ManagedAuthority::open_at(&link, "host", "boot", ENDPOINT, 16, 64).is_err());
        assert_eq!(fs::read_link(link).unwrap(), root);
        assert_eq!(snapshot(&root), before);
    }

    #[test]
    fn audit_domain_must_match_bootstrap_and_revision() {
        let root = new_root();
        init(&root);
        let authority = open(&root).unwrap();
        let mut domain = crate::ddr_recovery_audit::Domain {
            host: "host".into(),
            boot: "boot".into(),
            domain: "external-ddr-v0".into(),
            cap_revision: 1,
            broker: "fixture-broker".into(),
            owner_limit: 16,
            total_limit: 64,
        };
        assert!(authority.matches_audit_domain(&domain));
        domain.cap_revision += 1;
        assert!(!authority.matches_audit_domain(&domain));
        domain.cap_revision -= 1;
        domain.domain = "another-domain".into();
        assert!(!authority.matches_audit_domain(&domain));
    }
    #[test]
    fn child_open_helper() {
        let Some(root) = std::env::var_os("UF_TEST_MANAGED_ROOT") else {
            return;
        };
        let result = ManagedAuthority::open_at(Path::new(&root), "host", "boot", ENDPOINT, 16, 64);
        assert_eq!(result.err().unwrap().kind(), io::ErrorKind::WouldBlock);
    }

    fn ready_binding_guard(
        authority: &ManagedAuthority,
        params: &serde_json::Value,
    ) -> crate::ddr_witness_evidence::LiveBindingGuard {
        authority
            .prepare_live_binding(
                params["generation"].as_str().unwrap(),
                params["broker"].as_str().unwrap(),
                Path::new(params["evidence"].as_str().unwrap()),
            )
            .unwrap()
    }

    #[test]
    fn ready_binding_binder() {
        if std::env::var_os("UF_TEST_BINDER_CONTROL").is_none() {
            return;
        }
        let mut control = unsafe { UnixStream::from_raw_fd(0) };
        control
            .set_read_timeout(Some(std::time::Duration::from_secs(10)))
            .unwrap();
        let mut line = String::new();
        BufReader::new(control.try_clone().unwrap())
            .read_line(&mut line)
            .unwrap();
        let params: serde_json::Value = serde_json::from_str(&line).unwrap();
        let root = Path::new(params["root"].as_str().unwrap());
        assert!(root.starts_with("/workspace/uflow/runs/"));
        // The binder opens and locks its own description, before the witness
        // launches the broker. No root lock FD is inherited by either child.
        let mut authority = ManagedAuthority::open_at(
            root,
            "host",
            params["boot"].as_str().unwrap(),
            params["endpoint"].as_str().unwrap(),
            16,
            64,
        )
        .unwrap();
        control.write_all(b"UFLOW-BINDER-LOCKED\n").unwrap();
        let mut command = [0u8; 1];
        control.read_exact(&mut command).unwrap();
        if command == *b"Q" {
            drop(authority);
            control.write_all(b"UFLOW-BINDER-RELEASED\n").unwrap();
            return;
        }
        assert_eq!(&command, b"C");
        let evidence = Path::new(params["evidence"].as_str().unwrap());
        let generation = params["generation"].as_str().unwrap();
        let broker = params["broker"].as_str().unwrap();
        let state_before = fs::read(root.join(STATE_FILE)).unwrap();
        let trust_before = fs::read(root.join(WITNESS_TRUST_FILE)).unwrap();
        assert!(authority
            .prepare_live_binding("other-generation", broker, evidence)
            .is_err());
        assert!(authority
            .prepare_live_binding(generation, "other-broker", evidence)
            .is_err());
        assert!(authority
            .prepare_live_binding(generation, broker, &evidence.join(".."))
            .is_err());
        assert!(!authority.admission_poisoned.get());
        assert_eq!(fs::read(root.join(STATE_FILE)).unwrap(), state_before);
        assert_eq!(
            fs::read(root.join(WITNESS_TRUST_FILE)).unwrap(),
            trust_before
        );
        let live = ready_binding_guard(&authority, &params);
        authority
            .commit_live_binding("ready-binding-tx", &live)
            .unwrap();
        assert!(!authority.admits_new_work());
        // A binder holding its own lock must not pass the broker-only check.
        assert!(authority
            .verify_committed_live_binding_for_self(&live)
            .is_err());
        assert!(authority.admission_poisoned.get());
        drop(authority); // release before notifying the witness
        control.write_all(b"UFLOW-BINDER-RELEASED\n").unwrap();
    }

    #[test]
    fn ready_binding_gap_contender() {
        let Ok(text) = std::env::var("UF_TEST_BOUND_CONTENDER_PARAMS") else {
            return;
        };
        let params: serde_json::Value = serde_json::from_str(&text).unwrap();
        let root = Path::new(params["root"].as_str().unwrap());
        assert!(root.starts_with("/workspace/uflow/runs/"));
        let before = snapshot(root);
        let mut authority = ManagedAuthority::open_at(
            root,
            "host",
            params["boot"].as_str().unwrap(),
            params["endpoint"].as_str().unwrap(),
            16,
            64,
        )
        .unwrap();
        assert!(authority.is_inherited_quarantine());
        assert!(!authority.admits_new_work());
        assert!(authority.before_admission().is_err());
        // A syntactically valid bound generation never authorizes a third
        // process just because the lock was briefly available.
        assert!(authority.verify_bound_self_from_anchor().is_err());
        drop(authority);
        assert_eq!(snapshot(root), before);
    }

    #[test]
    fn ready_binding_child() {
        if std::env::var_os("UF_TEST_READY_BINDING_CHILD").is_none() {
            return;
        }
        let mut control = unsafe { UnixStream::from_raw_fd(0) };
        control
            .set_read_timeout(Some(std::time::Duration::from_secs(10)))
            .unwrap();
        let mut line = String::new();
        BufReader::new(control.try_clone().unwrap())
            .read_line(&mut line)
            .unwrap();
        let params: serde_json::Value = serde_json::from_str(&line).unwrap();
        let root = Path::new(params["root"].as_str().unwrap());
        assert!(root.starts_with("/workspace/uflow/runs/"));
        let boot = params["boot"].as_str().unwrap();
        let endpoint = params["endpoint"].as_str().unwrap();
        let pinned_lock = fs::metadata(root.join(LOCK_FILE)).unwrap();
        for entry in fs::read_dir("/proc/self/fd").unwrap() {
            let entry = entry.unwrap();
            if let Ok(meta) = fs::metadata(entry.path()) {
                assert_ne!(
                    (meta.dev(), meta.ino()),
                    (pinned_lock.dev(), pinned_lock.ino()),
                    "broker inherited the deployer's authority lock"
                );
            }
        }
        let parent = crate::ddr_process_identity::observe(
            unsafe { libc::getppid() },
            unsafe { libc::geteuid() },
            unsafe { libc::getegid() },
        )
        .unwrap();
        let gate = crate::ddr_ready_gate::ParentReadyGate::arrive(
            control,
            parent,
            std::time::Duration::from_secs(10),
        )
        .unwrap();
        // No listener or protected effect can occur while waiting here.
        control = gate.wait_release().unwrap();
        // Independently reacquire the original lock inode, then derive policy,
        // witness, birth and initial head only from its anchored BIND.
        let mut bound = ManagedAuthority::open_at(root, "host", boot, endpoint, 16, 64).unwrap();
        let generation = bound.verify_bound_self_from_anchor().unwrap();
        assert!(!bound.admits_new_work());
        assert!(bound.before_admission().is_err());
        let mut audit = bound.start_bound_quarantine_audit(&generation).unwrap();
        audit
            .record(crate::ddr_recovery_audit::Event {
                domain: audit.domain().clone(),
                epoch: 0,
                kind: crate::ddr_recovery_audit::EventKind::BrokerBirth {
                    birth: generation.broker_birth,
                },
            })
            .unwrap();
        bound.publish_bound_quarantine_head(&audit).unwrap();
        assert!(bound.is_inherited_quarantine());
        control.write_all(b"UFLOW-BOUND-1\n").unwrap();
        let mut stop = [0u8; 1];
        control.read_exact(&mut stop).unwrap();
        assert_eq!(&stop, b"S");
        // Unit fixture token only: verifies the three synced state transitions,
        // not real listener/handler joins or production SEAL authorization.
        let joined = crate::external_ddr::fixture_joined_drain(&generation.broker);
        let snapshot = audit.replay().observed_drain(0, 0);
        audit.record_joined_drain(&joined, 0, snapshot).unwrap();
        bound.publish_bound_quarantine_head(&audit).unwrap();
        let receipt = audit.checkpoint_after_drain(&joined).unwrap();
        assert_eq!(receipt.head(), audit.head());
        bound.publish_bound_quarantine_head(&audit).unwrap();
        assert!(bound.is_inherited_quarantine());
        assert!(!bound.admits_new_work());
    }

    #[test]
    fn witness_ready_binding_and_original_lock_handoff_in_isolated_authority() {
        if std::env::var_os("UFLOW_WITNESS_RUST_FIXTURE").as_deref() != Some("1".as_ref()) {
            return;
        }
        use crate::ddr_recovery_coverage::{canonical, digest};
        let parent = new_root();
        assert!(parent.starts_with("/workspace/uflow/runs/"));
        fs::DirBuilder::new().mode(0o700).create(&parent).unwrap();
        let root = parent.join("authority");
        let evidence = parent.join("evidence");
        fs::DirBuilder::new().mode(0o700).create(&evidence).unwrap();
        let python = fs::canonicalize("/usr/bin/python3").unwrap();
        let script = fs::canonicalize(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../examples/ddr_v0/t3_deployment_witness.py"),
        )
        .unwrap();
        let binary = fs::canonicalize(std::env::current_exe().unwrap()).unwrap();
        let endpoint = "/workspace/uflow/runs/t_5211f680/ready.sock";
        let boot = fs::read_to_string("/proc/sys/kernel/random/boot_id").unwrap();
        let boot = boot.trim();
        let library_path = std::env::var("LD_LIBRARY_PATH").unwrap();
        let py = python.to_str().unwrap();
        let source = script.to_str().unwrap();
        let exe = binary.to_str().unwrap();
        let supervisor_exe =
            serde_json::json!({"path": py, "sha256": digest(&fs::read(&python).unwrap())});
        let supervisor_entry =
            serde_json::json!({"path": source, "sha256": digest(&fs::read(&script).unwrap())});
        let broker_exe =
            serde_json::json!({"path": exe, "sha256": digest(&fs::read(&binary).unwrap())});
        let roles: Vec<_> = ["supervisor", "launcher", "interpreter", "broker", "owner", "accessor"].iter().map(|role| {
            if *role == "supervisor" {
                serde_json::json!({"role": role, "executable": supervisor_exe, "entrypoint": supervisor_entry,
                    "argv": [py, source, "_bindcase", root.to_str().unwrap(), "{uflow:evidence_dir}", exe, "{uflow:endpoint}", "{uflow:generation}"],
                    "environment": {"LD_LIBRARY_PATH": library_path}})
            } else {
                serde_json::json!({"role": role, "executable": broker_exe, "entrypoint": broker_exe,
                    "argv": [exe, "--exact", "ddr_managed_authority::tests::ready_binding_child", "--nocapture"],
                    "environment": {"LD_LIBRARY_PATH": library_path, "UF_TEST_READY_BINDING_CHILD": "1"}})
            }
        }).collect();
        let mut policy_bytes = canonical(
            &serde_json::json!({"schema": "uflow-ddr-deployment-policy-v1", "roles": roles}),
        )
        .unwrap();
        policy_bytes.push(b'\n');
        let policy_path = parent.join("policy.json");
        fs::write(&policy_path, policy_bytes).unwrap();
        let policy = crate::ddr_deployment_policy::DeploymentPolicy::load(&policy_path).unwrap();
        initialize_with_policy_at_inner(
            &root,
            "host",
            boot,
            endpoint,
            16,
            64,
            Some(&policy),
            |_| Ok(()),
        )
        .unwrap();
        let before = snapshot(&root);
        let negative_evidence = parent.join("evidence-uncommitted");
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&negative_evidence)
            .unwrap();
        let denied = Command::new(&python)
            .arg(&script)
            .arg("_bindcase")
            .arg(&root)
            .arg(&negative_evidence)
            .arg(&binary)
            .arg(endpoint)
            .arg("ready-uncommitted")
            .env_clear()
            .env("LD_LIBRARY_PATH", &library_path)
            .output()
            .unwrap();
        assert!(
            denied.status.success(),
            "uncommitted case stdout={} stderr={} child={}",
            String::from_utf8_lossy(&denied.stdout),
            String::from_utf8_lossy(&denied.stderr),
            fs::read_to_string(negative_evidence.join("test-broker.log")).unwrap_or_default()
        );
        assert!(String::from_utf8_lossy(&denied.stdout).contains("uncommitted release refused"));
        assert_eq!(snapshot(&root), before);
        let result = Command::new(&python)
            .arg(&script)
            .arg("_bindcase")
            .arg(&root)
            .arg(&evidence)
            .arg(&binary)
            .arg(endpoint)
            .arg("ready-g1")
            .env_clear()
            .env("LD_LIBRARY_PATH", &library_path)
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "supervisor stdout={} stderr={} child={}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr),
            fs::read_to_string(evidence.join("test-broker.log")).unwrap_or_default()
        );
        eprintln!("{}", String::from_utf8_lossy(&result.stdout));
        let mut reopened = after_test_lock_release(|| {
            ManagedAuthority::open_at(&root, "host", boot, endpoint, 16, 64)
        })
        .unwrap();
        assert!(reopened.is_inherited_quarantine());
        assert!(!reopened.admits_new_work());
        assert!(reopened.before_admission().is_err());
        let journal = reopened.open_journal().unwrap();
        assert_eq!(journal.head().sequence, 3);
        assert_eq!(
            journal.head().mode,
            crate::ddr_recovery_journal::Mode::Clean
        );
        let trust = fs::read(root.join(WITNESS_TRUST_FILE)).unwrap();
        assert!(String::from_utf8(trust)
            .unwrap()
            .contains("\"type\":\"BIND\""));
    }

    #[test]
    fn inherited_lock_child() {
        let Some(root) = std::env::var_os("UF_TEST_INHERITED_LOCK_ROOT") else {
            return;
        };
        let fd: i32 = std::env::var("UF_TEST_INHERITED_LOCK_FD")
            .unwrap()
            .parse()
            .unwrap();
        // Only the isolated parent test passes this descriptor via exec; it
        // is not an ordinary daemon flag or production authority root option.
        let inherited = unsafe { File::from_raw_fd(fd) };
        let authority = ManagedAuthority::open_at_held_lock(
            Path::new(&root),
            "host",
            "boot",
            ENDPOINT,
            16,
            64,
            inherited,
        )
        .unwrap();
        // Only check lock/namespace here. This inherited-OFD test helper is
        // never a production authority or permission to start a lifecycle.
        authority.ensure_namespace().unwrap();
        println!("UF_TEST_LOCK_HANDOFF_READY");
        io::stdout().flush().unwrap();
        let mut release = [0u8; 1];
        io::stdin().read_exact(&mut release).unwrap();
        assert_eq!(&release, b"R");
        authority.ensure_namespace().unwrap();
    }

    #[test]
    fn isolated_inherited_original_lock_remains_exclusive_after_parent_closes() {
        let (root, _, policy) = deployment_fixture();
        initialize_deployed_fixture(&root, &policy);
        let authority = open(&root).unwrap();
        let before = snapshot(&root);
        // A separately opened FD to the same inode is not the original open
        // file description and must fail while the original lock is held.
        let wrong = private_file(&root.join(LOCK_FILE), true).unwrap();
        let error =
            ManagedAuthority::open_at_held_lock(&root, "host", "boot", ENDPOINT, 16, 64, wrong)
                .err()
                .expect("independent file description must not inherit the lock");
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        let different_inode = private_file(&root.join(JOURNAL_FILE), false).unwrap();
        assert!(ManagedAuthority::open_at_held_lock(
            &root,
            "host",
            "boot",
            ENDPOINT,
            16,
            64,
            different_inode
        )
        .is_err());

        let shared = authority.lock.try_clone().unwrap();
        let fd = shared.as_raw_fd();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "ddr_managed_authority::tests::inherited_lock_child",
                "--nocapture",
            ])
            .env("UF_TEST_INHERITED_LOCK_ROOT", &root)
            .env("UF_TEST_INHERITED_LOCK_FD", fd.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        // The child receives this single duplicate of the original open-file
        // description at exec; no lock path is reopened as a replacement.
        unsafe {
            command.pre_exec(move || {
                let flags = libc::fcntl(fd, libc::F_GETFD);
                if flags < 0 || libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = command.spawn().unwrap();
        let mut reader = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        let mut ready = false;
        while reader.read_line(&mut line).unwrap() != 0 {
            if line.contains("UF_TEST_LOCK_HANDOFF_READY") {
                ready = true;
                break;
            }
            line.clear();
        }
        assert!(ready, "inherited-lock child did not attach");
        let contender = private_file(&root.join(LOCK_FILE), true).unwrap();
        assert_ne!(
            unsafe { libc::flock(contender.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0
        );
        drop(authority);
        drop(shared);
        // The parent has closed every local duplicate; the child alone still
        // holds the original locked description, so there is no unlock gap.
        assert_ne!(
            unsafe { libc::flock(contender.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0
        );
        child.stdin.take().unwrap().write_all(b"R").unwrap();
        assert!(child.wait().unwrap().success());
        assert_eq!(
            unsafe { libc::flock(contender.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0
        );
        assert_eq!(snapshot(&root), before);
    }

    #[test]
    fn exclusive_across_processes_and_requires_exact_configuration() {
        let root = new_root();
        init(&root);
        assert!(initialize_at(&root, "host", "boot", ENDPOINT, 16, 64).is_err());
        assert!(ManagedAuthority::open_at(&root, "alias", "boot", ENDPOINT, 16, 64).is_err());
        assert!(ManagedAuthority::open_at(&root, "host", "boot", ENDPOINT, 16, 65).is_err());
        assert!(ManagedAuthority::open_at(&root, "host", "otherboot", ENDPOINT, 16, 64).is_err());
        assert!(ManagedAuthority::open_at(
            &root,
            "host",
            "boot",
            "/workspace/uflow/runs/other.sock",
            16,
            64
        )
        .is_err());
        let guard = open(&root).unwrap();
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "ddr_managed_authority::tests::child_open_helper",
                "--nocapture",
            ])
            .env("UF_TEST_MANAGED_ROOT", &root)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
        drop(guard);
        assert!(open(&root).is_ok());
    }

    #[test]
    fn witness_python_can_pin_a_real_rust_genesis_fixture_before_launch() {
        // The cross-language subprocess is intentionally opt-in because its
        // fixture policy permits writes only under /workspace/uflow/runs/.
        if std::env::var_os("UFLOW_WITNESS_RUST_FIXTURE").as_deref() != Some("1".as_ref()) {
            return;
        }
        let root = new_root();
        let boot = fs::read_to_string("/proc/sys/kernel/random/boot_id").unwrap();
        let boot = boot.trim();
        initialize_at(&root, "fixture-host", boot, ENDPOINT, 8192, 8192).unwrap();
        let script = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../examples/ddr_v0/t3_witness_pin_wip.py");
        let output = Command::new("python3")
            .arg(script)
            .arg("--rust-genesis-fixture")
            .arg(&root)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("not resource coverage"));
        let authority =
            ManagedAuthority::open_at(&root, "fixture-host", boot, ENDPOINT, 8192, 8192).unwrap();
        assert!(authority.admits_new_work());
        assert!(
            read_bounded(&private_file(&root.join(WITNESS_TRUST_FILE), false).unwrap())
                .unwrap()
                .contains("uflow-ddr-witness-binding-fixture-v2")
        );
        assert_eq!(fs::read(root.join(STATE_FILE)).unwrap(), b"clean\n");
        // The real Rust bootstrap and witness inode were exercised, but none
        // of these subprocesses acquired backing; no recovery proof exists.
    }

    #[test]
    fn dirty_restart_is_quarantined_and_clean_drain_can_restart() {
        let root = new_root();
        init(&root);
        let mut authority = open(&root).unwrap();
        authority.before_admission().unwrap();
        assert!(authority
            .clean_after_drain(&DrainedCommitments {
                accepting_stopped: true,
                views: 1,
                ..Default::default()
            })
            .is_err());
        drop(authority); // unknown legacy references; lock release is not drain proof
        let mut restarted = open(&root).unwrap();
        assert!(restarted.before_admission().is_err());
        assert!(restarted.clean_after_drain(&drained()).is_err());

        let root2 = new_root();
        init(&root2);
        let mut clean = open(&root2).unwrap();
        clean.before_admission().unwrap();
        clean.clean_after_drain(&drained()).unwrap();
        drop(clean);
        let mut restarted = open(&root2).unwrap();
        restarted.before_admission().unwrap();
        restarted.clean_after_drain(&drained()).unwrap();
    }

    fn fixture_semantic_seal(
        authority: &mut ManagedAuthority,
    ) -> crate::ddr_recovery_durable::SemanticClean {
        let mut audit = authority.start_lifecycle_audit("fixture-broker").unwrap();
        authority.before_admission().unwrap();
        let joined = crate::external_ddr::fixture_joined_drain("fixture-broker");
        let observed = audit.replay().observed_drain(0, 0);
        audit.record_joined_drain(&joined, 0, observed).unwrap();
        audit.checkpoint_after_drain(&joined).unwrap()
    }

    #[test]
    fn semantic_clean_receipt_is_bound_to_the_live_locked_authority() {
        let root = new_root();
        init(&root);
        let mut authority = open(&root).unwrap();
        let seal = fixture_semantic_seal(&mut authority);
        authority
            .clean_after_semantic_drain(&drained(), seal)
            .unwrap();
        assert_eq!(fs::read(root.join(STATE_FILE)).unwrap(), b"clean\n");
        drop(authority);
        assert!(open(&root).is_ok());

        let source = new_root();
        let foreign = new_root();
        init(&source);
        init(&foreign);
        let mut source_authority = open(&source).unwrap();
        let mut foreign_authority = open(&foreign).unwrap();
        let seal = fixture_semantic_seal(&mut source_authority);
        foreign_authority.before_admission().unwrap();
        let before = snapshot(&foreign);
        assert!(foreign_authority
            .clean_after_semantic_drain(&drained(), seal)
            .is_err());
        assert_eq!(snapshot(&foreign), before);
        assert!(!foreign_authority.admits_new_work());
        assert_eq!(fs::read(source.join(STATE_FILE)).unwrap(), b"dirty\n");

        let root = new_root();
        init(&root);
        let mut authority = open(&root).unwrap();
        let seal = fixture_semantic_seal(&mut authority);
        drop(authority);
        let mut quarantined = open(&root).unwrap();
        assert!(quarantined
            .clean_after_semantic_drain(&drained(), seal)
            .is_err());
        assert_eq!(fs::read(root.join(STATE_FILE)).unwrap(), b"dirty\n");

        let root = new_root();
        init(&root);
        let mut authority = open(&root).unwrap();
        let seal = fixture_semantic_seal(&mut authority);
        let before = snapshot(&root);
        assert!(authority
            .clean_after_semantic_drain(
                &DrainedCommitments {
                    accepting_stopped: true,
                    views: 1,
                    ..Default::default()
                },
                seal,
            )
            .is_err());
        assert_eq!(snapshot(&root), before);
        assert_eq!(fs::read(root.join(STATE_FILE)).unwrap(), b"dirty\n");

        let root = new_root();
        init(&root);
        let mut authority = open(&root).unwrap();
        let seal = fixture_semantic_seal(&mut authority);
        let head = root.join(JOURNAL_HEAD_FILE);
        let original_head = fs::read(&head).unwrap();
        fs::write(&head, b"corrupt\n").unwrap();
        assert!(authority
            .clean_after_semantic_drain(&drained(), seal)
            .is_err());
        assert_eq!(fs::read(root.join(STATE_FILE)).unwrap(), b"dirty\n");
        fs::write(&head, original_head).unwrap();
        assert!(!authority.admits_new_work());
        drop(authority);
        assert!(open(&root).unwrap().before_admission().is_err());
    }
    #[test]
    fn copied_manifest_and_replaced_lock_never_admit() {
        let root = new_root();
        init(&root);
        let other = new_root();
        fs::create_dir(&other).unwrap();
        fs::set_permissions(&other, fs::Permissions::from_mode(0o700)).unwrap();
        for name in [
            LOCK_FILE,
            MANIFEST_FILE,
            STATE_FILE,
            JOURNAL_FILE,
            JOURNAL_HEAD_FILE,
            WITNESS_TRUST_FILE,
        ] {
            fs::copy(root.join(name), other.join(name)).unwrap();
        }
        assert!(open(&other).is_err());
        let mut authority = open(&root).unwrap();
        fs::rename(root.join(LOCK_FILE), root.join("old-lock")).unwrap();
        create_private(&root.join(LOCK_FILE)).unwrap();
        assert!(authority.before_admission().is_err());
    }
    #[test]
    fn corrupt_state_or_bootstrap_fails_closed() {
        let root = new_root();
        init(&root);
        fs::write(root.join(STATE_FILE), b"").unwrap();
        assert!(open(&root).is_err());
        fs::write(root.join(STATE_FILE), b"clean\n").unwrap();
        let mut authority = open(&root).unwrap();
        fs::write(root.join(MANIFEST_FILE), b"garbage\n").unwrap();
        assert!(authority.before_admission().is_err());
    }

    #[test]
    fn restored_namespace_does_not_unpoison_a_live_authority_or_release_its_lock() {
        let root = new_root();
        init(&root);
        let mut authority = open(&root).unwrap();
        authority.before_admission().unwrap();
        // Simulate a corrupted pinned state after admission, then restore its
        // contents, not its identity. The live failure remains irreversible.
        fs::write(root.join(STATE_FILE), b"corrupt\n").unwrap();
        assert!(!authority.admits_new_work());
        fs::write(root.join(STATE_FILE), b"dirty\n").unwrap();
        assert!(!authority.admits_new_work());
        assert!(authority.before_admission().is_err());
        assert!(authority.clean_after_drain(&drained()).is_err());
        assert!(authority
            .reconfigure_after_drain(&drained(), ENDPOINT, 16, 64)
            .is_err());
        assert_eq!(
            ManagedAuthority::open_at(&root, "host", "boot", ENDPOINT, 16, 64)
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        drop(authority);
        let mut reopened = open(&root).unwrap();
        assert!(reopened.before_admission().is_err());
        assert!(reopened.clean_after_drain(&drained()).is_err());
        assert_eq!(fs::read(root.join(STATE_FILE)).unwrap(), b"dirty\n");
    }

    #[test]
    fn copied_clean_state_cannot_clear_a_dirty_authority() {
        let root = new_root();
        init(&root);
        let mut authority = open(&root).unwrap();
        authority.before_admission().unwrap();
        fs::rename(root.join(STATE_FILE), root.join("old-state")).unwrap();
        let mut replacement = create_private(&root.join(STATE_FILE)).unwrap();
        replacement.write_all(b"clean\n").unwrap();
        drop(replacement);
        assert!(authority.before_admission().is_err());
        drop(authority);
        assert!(open(&root).is_err());
    }

    #[test]
    fn pinned_journal_head_and_witness_trust_replacement_rejects_admission_and_reopen() {
        for name in [JOURNAL_FILE, JOURNAL_HEAD_FILE, WITNESS_TRUST_FILE] {
            let root = new_root();
            init(&root);
            let mut authority = open(&root).unwrap();
            fs::rename(root.join(name), root.join(format!("old-{name}"))).unwrap();
            let mut replacement = create_private(&root.join(name)).unwrap();
            if name == JOURNAL_HEAD_FILE {
                replacement
                    .write_all(
                        crate::ddr_recovery_journal::AnchoredJournal::genesis_state().as_bytes(),
                    )
                    .unwrap();
            }
            replacement.sync_all().unwrap();
            assert!(authority.before_admission().is_err());
            drop(authority);
            assert!(open(&root).is_err());
        }
    }

    #[test]
    fn dirty_journal_cannot_be_cleared_by_marker_or_empty_catalog() {
        let root = new_root();
        init(&root);
        let mut authority = open(&root).unwrap();
        authority.before_admission().unwrap();
        let mut journal = authority.open_journal().unwrap();
        journal
            .append_dirty(
                "fixture-broker",
                crate::ddr_recovery_journal::Kind::ViewIntent,
                b"fixture-only",
            )
            .unwrap();
        assert!(authority.clean_after_drain(&drained()).is_err());
        drop((journal, authority));
        let mut restarted = open(&root).unwrap();
        assert!(restarted.before_admission().is_err());
        assert!(restarted.clean_after_drain(&drained()).is_err());
        drop(restarted);
        // A clean marker alone cannot override the independently pinned head.
        fs::write(root.join(STATE_FILE), b"clean\n").unwrap();
        assert!(open(&root).is_err());
    }

    #[test]
    fn reconfiguration_requires_exclusive_clean_drained_authority() {
        let root = new_root();
        init(&root);
        let mut authority = open(&root).unwrap();
        let next = "/workspace/uflow/runs/t_855f763c-i/b.sock";
        assert!(authority
            .reconfigure_after_drain(&DrainedCommitments::default(), next, 32, 128)
            .is_err());
        authority.before_admission().unwrap();
        assert!(authority
            .reconfigure_after_drain(&drained(), next, 32, 128)
            .is_err());
        authority.clean_after_drain(&drained()).unwrap();
        assert_eq!(
            authority
                .reconfigure_after_drain(&drained(), next, 32, 128)
                .unwrap(),
            2
        );
        drop(authority);
        assert!(open(&root).is_err());
        let mut next_authority =
            ManagedAuthority::open_at(&root, "host", "boot", next, 32, 128).unwrap();
        assert_eq!(next_authority.bootstrap.revision, 2);
        next_authority.before_admission().unwrap();
        next_authority.clean_after_drain(&drained()).unwrap();
    }
}
