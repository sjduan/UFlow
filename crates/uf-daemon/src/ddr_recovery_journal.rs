//! WIP recovery-only, exact-head-anchored append log.
//!
//! Valid frames alone do not prove that all resource obligations or FD holders
//! were recorded, that a holder exited normally, or that recovery is safe.
//! A clean-head primitive below is only for a semantically verified normal
//! drain; it cannot authorize recovery from an inherited dirty generation.
//! Production admission remains gated until the enclosing authority pins these
//! files and completes the intent/close/witness protocol.
#![allow(dead_code)]

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

const FRAME_MAGIC: &[u8; 4] = b"UDRJ";
const END_MAGIC: &[u8; 4] = b"JEND";
const FORMAT_VERSION: u16 = 1;
const STATE_VERSION: &str = "uflow-ddr-recovery-head-v1";
const MAX_PAYLOAD: usize = 8192;
const MAX_JOURNAL: u64 = 64 * 1024 * 1024;
const MAX_STATE: u64 = 4096;
const HEADER_LEN: usize = 28;
const TRAILER_LEN: usize = 12;
// CRC-64/ECMA-182, init=0, xorout=0, refin/refout=false. This is a
// corruption/continuity checksum, NOT authentication against hostile writers.
const CRC_POLY: u64 = 0x42f0_e1eb_a9ea_3693;
const GENESIS: u64 = 0;

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn io_failure(stage: &'static str, error: io::Error) -> io::Error {
    uf_core::topology::ddr::diagnostic::note(format!(
        "journal.io stage={stage} kind={:?} raw_os_error={:?}",
        error.kind(),
        error.raw_os_error()
    ));
    error // preserve type, errno and the original poison/rollback behaviour
}

// Same ECMA polynomial/init/reflection/byte order as the original bit loop.
// Only the arithmetic changes: every caller still reads and verifies the
// entire journal, chain and head at every existing barrier. No content cache.
const fn crc_table() -> [u64; 256] {
    let mut table = [0; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = (i as u64) << 56;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & (1 << 63) != 0 {
                (crc << 1) ^ CRC_POLY
            } else {
                crc << 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}
// A static immutable table, not a const aggregate re-materialized at each
// debug-build indexing operation. This stores ONLY polynomial constants.
static CRC_TABLE: [u64; 256] = crc_table();

fn crc64(bytes: &[u8]) -> u64 {
    let mut crc = 0u64;
    for &byte in bytes {
        crc = (crc << 8) ^ CRC_TABLE[((crc >> 56) as u8 ^ byte) as usize];
    }
    crc
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u16)]
pub(crate) enum Kind {
    SessionRegisterIntent = 1,
    RegisterCommit = 2,
    ViewIntent = 3,
    ViewCommit = 4,
    Abort = 5,
    CloseIntent = 6,
    CloseDone = 7,
    PeerTerminated = 8,
    ReserveIntent = 9,
    CopyIntent = 10,
    CancelRequest = 11,
    WorkDrained = 12,
    PublishIntent = 13,
    PublishDone = 14,
    BrokerDrained = 15,
    CleanCheckpoint = 16,
    RecoveryAudit = 17,
    RecoveryCommit = 18,
    NotSent = 19,
    Retire = 20,
    WorkVerified = 21,
    BrokerBirth = 22,
    WorkIoCompleted = 23,
}

impl Kind {
    fn decode(value: u16) -> io::Result<Self> {
        Ok(match value {
            1 => Self::SessionRegisterIntent,
            2 => Self::RegisterCommit,
            3 => Self::ViewIntent,
            4 => Self::ViewCommit,
            5 => Self::Abort,
            6 => Self::CloseIntent,
            7 => Self::CloseDone,
            8 => Self::PeerTerminated,
            9 => Self::ReserveIntent,
            10 => Self::CopyIntent,
            11 => Self::CancelRequest,
            12 => Self::WorkDrained,
            13 => Self::PublishIntent,
            14 => Self::PublishDone,
            15 => Self::BrokerDrained,
            16 => Self::CleanCheckpoint,
            17 => Self::RecoveryAudit,
            18 => Self::RecoveryCommit,
            19 => Self::NotSent,
            20 => Self::Retire,
            21 => Self::WorkVerified,
            22 => Self::BrokerBirth,
            23 => Self::WorkIoCompleted,
            _ => return Err(invalid("unknown recovery event type")),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Frame {
    pub kind: Kind,
    pub sequence: u64,
    pub previous: u64,
    pub payload: Vec<u8>,
    pub checksum: u64,
}

impl Frame {
    pub(crate) fn encoded_len(&self) -> u64 {
        (HEADER_LEN + self.payload.len() + TRAILER_LEN) as u64
    }

    fn encode(kind: Kind, sequence: u64, previous: u64, payload: &[u8]) -> io::Result<Vec<u8>> {
        if sequence == 0 || payload.is_empty() || payload.len() > MAX_PAYLOAD {
            return Err(invalid("invalid recovery frame sequence or payload length"));
        }
        let mut bytes = Vec::with_capacity(HEADER_LEN + payload.len() + TRAILER_LEN);
        bytes.extend_from_slice(FRAME_MAGIC);
        bytes.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
        bytes.extend_from_slice(&(kind as u16).to_be_bytes());
        bytes.extend_from_slice(&sequence.to_be_bytes());
        bytes.extend_from_slice(&previous.to_be_bytes());
        bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        bytes.extend_from_slice(payload);
        let checksum = crc64(&bytes);
        bytes.extend_from_slice(&checksum.to_be_bytes());
        bytes.extend_from_slice(END_MAGIC);
        Ok(bytes)
    }

    #[cfg(test)]
    pub(crate) fn encode_fixture(
        kind: Kind,
        sequence: u64,
        previous: u64,
        payload: &[u8],
    ) -> io::Result<Vec<u8>> {
        Self::encode(kind, sequence, previous, payload)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
    Clean,
    Dirty,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Head {
    pub mode: Mode,
    pub broker: String,
    pub sequence: u64,
    pub offset: u64,
    pub checksum: u64,
    pub clean_sequence: u64,
}

impl Head {
    fn genesis() -> Self {
        Self {
            mode: Mode::Clean,
            broker: "none".into(),
            sequence: 0,
            offset: 0,
            checksum: GENESIS,
            clean_sequence: 0,
        }
    }

    pub(crate) fn encode(&self) -> String {
        let mode = match self.mode {
            Mode::Clean => "clean",
            Mode::Dirty => "dirty",
        };
        format!(
            "{STATE_VERSION}\nmode={mode}\nbroker={}\nsequence={}\noffset={}\nchecksum={:016x}\nclean_sequence={}\n",
            self.broker, self.sequence, self.offset, self.checksum, self.clean_sequence
        )
    }

    pub(crate) fn parse(text: &str) -> io::Result<Self> {
        fn field<'a>(
            lines: &mut impl Iterator<Item = &'a str>,
            prefix: &str,
        ) -> io::Result<&'a str> {
            lines
                .next()
                .and_then(|line| line.strip_prefix(prefix))
                .ok_or_else(|| invalid("missing or malformed recovery head field"))
        }
        fn number(value: &str) -> io::Result<u64> {
            value
                .parse()
                .map_err(|_| invalid("invalid recovery head integer"))
        }
        let mut lines = text.lines();
        if lines.next() != Some(STATE_VERSION) {
            return Err(invalid("invalid recovery head version"));
        }
        let mode = match field(&mut lines, "mode=")? {
            "clean" => Mode::Clean,
            "dirty" => Mode::Dirty,
            _ => return Err(invalid("invalid recovery head mode")),
        };
        let broker = field(&mut lines, "broker=")?.to_string();
        let sequence = number(field(&mut lines, "sequence=")?)?;
        let offset = number(field(&mut lines, "offset=")?)?;
        let checksum_text = field(&mut lines, "checksum=")?;
        let checksum = u64::from_str_radix(checksum_text, 16)
            .map_err(|_| invalid("invalid recovery head checksum"))?;
        let clean_sequence = number(field(&mut lines, "clean_sequence=")?)?;
        let head = Self {
            mode,
            broker,
            sequence,
            offset,
            checksum,
            clean_sequence,
        };
        if lines.next().is_some()
            || head.encode() != text
            || !valid_broker(&head.broker)
            || head.offset > MAX_JOURNAL
            || head.clean_sequence > head.sequence
            || (head.sequence == 0 && head != Self::genesis())
            || (head.sequence > 0 && head.offset == 0)
            || (head.sequence > 0 && head.broker == "none")
            || (head.mode == Mode::Clean && head.clean_sequence != head.sequence)
        {
            return Err(invalid("noncanonical or inconsistent recovery head"));
        }
        Ok(head)
    }
}

fn valid_broker(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 96
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}

fn parse_frames(bytes: &[u8], head: &Head) -> io::Result<Vec<Frame>> {
    uf_core::topology::ddr::diagnostic::note(format!(
        "journal.scan sequence={} offset={} checksum={:016x} mode={:?}",
        head.sequence, head.offset, head.checksum, head.mode
    ));
    // Collection only, never logger I/O while the enclosing catalog is held.
    struct Scan<'a>(&'a [u8], u64, std::time::Instant);
    impl Drop for Scan<'_> {
        fn drop(&mut self) {
            uf_core::topology::ddr::diagnostic::scan(self.0.len(), self.1, self.2);
        }
    }
    let _scan = Scan(bytes, head.sequence, std::time::Instant::now());
    if bytes.len() as u64 != head.offset || head.offset > MAX_JOURNAL {
        return Err(invalid("journal length does not match durable head"));
    }
    let mut frames = Vec::new();
    let mut offset = 0usize;
    let mut previous = GENESIS;
    let mut sequence = 0u64;
    while offset < bytes.len() {
        let remaining = &bytes[offset..];
        if remaining.len() < HEADER_LEN + TRAILER_LEN || &remaining[..4] != FRAME_MAGIC {
            return Err(invalid("truncated or invalid recovery frame"));
        }
        let version = u16::from_be_bytes(remaining[4..6].try_into().unwrap());
        let kind = Kind::decode(u16::from_be_bytes(remaining[6..8].try_into().unwrap()))?;
        let frame_sequence = u64::from_be_bytes(remaining[8..16].try_into().unwrap());
        let frame_previous = u64::from_be_bytes(remaining[16..24].try_into().unwrap());
        let len = u32::from_be_bytes(remaining[24..28].try_into().unwrap()) as usize;
        if version != FORMAT_VERSION || len == 0 || len > MAX_PAYLOAD {
            return Err(invalid("invalid recovery frame version or length"));
        }
        let total = HEADER_LEN
            .checked_add(len)
            .and_then(|n| n.checked_add(TRAILER_LEN))
            .ok_or_else(|| invalid("recovery frame size overflow"))?;
        if total > remaining.len() {
            return Err(invalid("truncated recovery frame payload"));
        }
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| invalid("recovery sequence overflow"))?;
        if frame_sequence != sequence || frame_previous != previous {
            return Err(invalid("recovery frame sequence or chain mismatch"));
        }
        let checksum_offset = HEADER_LEN + len;
        let checksum = u64::from_be_bytes(
            remaining[checksum_offset..checksum_offset + 8]
                .try_into()
                .unwrap(),
        );
        if &remaining[checksum_offset + 8..total] != END_MAGIC
            || checksum != crc64(&remaining[..checksum_offset])
        {
            return Err(invalid("recovery frame checksum or terminator mismatch"));
        }
        frames.push(Frame {
            kind,
            sequence,
            previous,
            payload: remaining[HEADER_LEN..checksum_offset].to_vec(),
            checksum,
        });
        previous = checksum;
        offset += total;
    }
    if sequence != head.sequence || previous != head.checksum {
        return Err(invalid("journal frames do not match durable head"));
    }
    if head.clean_sequence != 0
        && frames
            .get((head.clean_sequence - 1) as usize)
            .is_none_or(|frame| frame.kind != Kind::CleanCheckpoint)
    {
        return Err(invalid("referenced clean checkpoint is absent"));
    }
    // A clean state must point at an actual clean checkpoint. A genesis clean
    // state is the only exception; no semantic obligation replay is done here.
    if head.mode == Mode::Clean
        && sequence != 0
        && frames
            .last()
            .is_none_or(|frame| frame.kind != Kind::CleanCheckpoint)
    {
        return Err(invalid("clean head is not a clean checkpoint"));
    }
    Ok(frames)
}

fn open_private(path: &Path, write: bool) -> io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(write)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|e| io_failure("open-private", e))?;
    let meta = file.metadata()?;
    if !meta.is_file()
        || meta.uid() != unsafe { libc::geteuid() }
        || meta.mode() & 0o077 != 0
        || meta.nlink() != 1
    {
        return Err(invalid(
            "recovery file must be private, owned and singly linked",
        ));
    }
    Ok(file)
}

fn identity(file: &File) -> io::Result<(u64, u64)> {
    let meta = file.metadata()?;
    Ok((meta.dev(), meta.ino()))
}

fn read_head(file: &mut File) -> io::Result<Head> {
    if file.metadata()?.len() > MAX_STATE {
        return Err(invalid("recovery head exceeds size limit"));
    }
    file.seek(SeekFrom::Start(0))?;
    let mut text = String::new();
    file.read_to_string(&mut text)
        .map_err(|e| io_failure("read-head", e))?;
    Head::parse(&text)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WriteStage {
    BeforeJournalWrite,
    AfterPartialJournalWrite,
    BeforeJournalSync,
    AfterJournalSync,
    AfterHeadTruncate,
    BeforeHeadSync,
    AfterHeadSync,
}

/// Both inodes must already be pinned by the enclosing bootstrap and guarded
/// by the one authority lock. No alternate-root production entry is exposed.
pub(crate) struct AnchoredJournal {
    journal_path: PathBuf,
    state_path: PathBuf,
    journal: File,
    state: File,
    journal_identity: (u64, u64),
    state_identity: (u64, u64),
    head: Head,
    poisoned: bool,
    #[cfg(test)]
    write_limit: u64,
}

impl AnchoredJournal {
    pub(crate) fn genesis_state() -> String {
        Head::genesis().encode()
    }

    pub(crate) fn open(
        journal_path: &Path,
        state_path: &Path,
        journal_identity: (u64, u64),
        state_identity: (u64, u64),
    ) -> io::Result<Self> {
        let mut journal = open_private(journal_path, true)?;
        let mut state = open_private(state_path, true)?;
        if identity(&journal)? != journal_identity || identity(&state)? != state_identity {
            return Err(invalid("recovery file inode mismatch"));
        }
        let head = read_head(&mut state)?;
        if journal.metadata()?.len() != head.offset || head.offset > MAX_JOURNAL {
            return Err(invalid("journal length does not match durable head"));
        }
        journal.seek(SeekFrom::Start(0))?;
        let mut bytes = Vec::with_capacity(head.offset as usize);
        (&mut journal)
            .take(head.offset + 1)
            .read_to_end(&mut bytes)
            .map_err(|e| io_failure("open-read-journal", e))?;
        parse_frames(&bytes, &head)?;
        Ok(Self {
            journal_path: journal_path.to_owned(),
            state_path: state_path.to_owned(),
            journal,
            state,
            journal_identity,
            state_identity,
            head,
            poisoned: false,
            #[cfg(test)]
            write_limit: MAX_JOURNAL,
        })
    }

    pub(crate) fn head(&self) -> &Head {
        &self.head
    }

    pub(crate) fn file_identities(&self) -> ((u64, u64), (u64, u64)) {
        (self.journal_identity, self.state_identity)
    }

    pub(crate) fn verify(&mut self) -> io::Result<Vec<Frame>> {
        if self.poisoned {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "recovery journal poisoned",
            ));
        }
        let result = self.verify_inner();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    fn verify_inner(&mut self) -> io::Result<Vec<Frame>> {
        let journal_entry = open_private(&self.journal_path, false)?;
        let state_entry = open_private(&self.state_path, false)?;
        if identity(&journal_entry)? != self.journal_identity
            || identity(&self.journal)? != self.journal_identity
            || identity(&state_entry)? != self.state_identity
            || identity(&self.state)? != self.state_identity
            || self.journal.metadata()?.nlink() != 1
            || self.state.metadata()?.nlink() != 1
        {
            return Err(invalid("pinned recovery file replaced or unlinked"));
        }
        if read_head(&mut self.state)? != self.head
            || self.journal.metadata()?.len() != self.head.offset
        {
            return Err(invalid("recovery head or journal changed"));
        }
        self.journal.seek(SeekFrom::Start(0))?;
        let mut bytes = Vec::with_capacity(self.head.offset as usize);
        (&mut self.journal)
            .take(self.head.offset + 1)
            .read_to_end(&mut bytes)
            .map_err(|e| io_failure("verify-read-journal", e))?;
        parse_frames(&bytes, &self.head)
    }

    /// Sync a complete frame, then sync its exact dirty head. The caller may
    /// cross its resource/FD/worker side-effect barrier only on success. Any
    /// write, sync, or identity error permanently poisons this live instance.
    /// Payload interpretation and transition validation belong to the unfinished
    /// obligation replay layer, not to this framing primitive.
    pub(crate) fn append_dirty(
        &mut self,
        broker: &str,
        kind: Kind,
        payload: &[u8],
    ) -> io::Result<Head> {
        if matches!(
            kind,
            Kind::CleanCheckpoint | Kind::RecoveryAudit | Kind::RecoveryCommit
        ) {
            return Err(invalid(
                "checkpoint or recovery requires its dedicated path",
            ));
        }
        self.append_inner(broker, kind, payload, false, false, |_| Ok(()))
    }

    /// The payload can only be produced by DurableAudit after semantic replay.
    /// The caller still has to prove actual handler/worker stop and join; this
    /// is not a dirty-recovery API and is not a witness authorization.
    pub(crate) fn append_clean_after_drain(
        &mut self,
        checkpoint: &crate::ddr_recovery_durable::SemanticCheckpoint,
    ) -> io::Result<Head> {
        self.append_inner(
            checkpoint.broker(),
            Kind::CleanCheckpoint,
            checkpoint.payload(),
            true,
            false,
            |_| Ok(()),
        )
    }

    /// Isolated framing/tamper tests deliberately bypass semantic replay. This
    /// helper is not compiled into a production daemon.
    #[cfg(test)]
    pub(crate) fn fixture_append_framing_clean(
        &mut self,
        broker: &str,
        payload: &[u8],
    ) -> io::Result<Head> {
        self.append_inner(broker, Kind::CleanCheckpoint, payload, true, false, |_| {
            Ok(())
        })
    }

    /// Fixture-only transaction helpers. They are not an authorization API;
    /// the locked recovery entry first verifies coverage and every resolution.
    #[cfg(test)]
    pub(crate) fn fixture_recovery_room(&mut self, payloads: &[&[u8]]) -> io::Result<()> {
        self.verify()?;
        let mut end = self.head.offset;
        for payload in payloads {
            if payload.is_empty() || payload.len() > MAX_PAYLOAD {
                return Err(invalid("recovery transaction payload exceeds frame limit"));
            }
            end = end
                .checked_add((HEADER_LEN + payload.len() + TRAILER_LEN) as u64)
                .ok_or_else(|| invalid("recovery transaction size overflow"))?;
        }
        if end > self.write_limit {
            return Err(invalid("recovery journal limit reached"));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn fixture_exhaust_recovery_capacity(&mut self) {
        self.write_limit = self.head.offset;
    }

    #[cfg(test)]
    pub(crate) fn fixture_append_recovery(
        &mut self,
        broker: &str,
        kind: Kind,
        payload: &[u8],
    ) -> io::Result<Head> {
        self.fixture_append_recovery_fault(broker, kind, payload, None)
    }

    /// A fault injected at an actual journal write/sync boundary. This helper
    /// exists only in test builds and can only turn an operation into error.
    #[cfg(test)]
    pub(crate) fn fixture_append_recovery_fault(
        &mut self,
        broker: &str,
        kind: Kind,
        payload: &[u8],
        fault: Option<WriteStage>,
    ) -> io::Result<Head> {
        let previous = match kind {
            Kind::RecoveryAudit => Kind::BrokerDrained,
            Kind::RecoveryCommit => Kind::RecoveryAudit,
            Kind::CleanCheckpoint => Kind::RecoveryCommit,
            _ => return Err(invalid("invalid recovery transaction record")),
        };
        let frames = self.verify()?;
        if self.head.mode != Mode::Dirty
            || self.head.broker != broker
            || frames.last().is_none_or(|f| f.kind != previous)
        {
            return Err(invalid("invalid recovery transaction ordering"));
        }
        let checkpoint = kind == Kind::CleanCheckpoint;
        self.append_inner(broker, kind, payload, checkpoint, checkpoint, |stage| {
            if Some(stage) == fault {
                Err(io::Error::from_raw_os_error(libc::ENOSPC))
            } else {
                Ok(())
            }
        })
    }

    #[cfg(test)]
    pub(crate) fn fixture_sync_recovery(&mut self) -> io::Result<()> {
        self.verify()?;
        if let Err(error) = self.journal.sync_all().and_then(|_| self.state.sync_all()) {
            self.poisoned = true;
            return Err(error);
        }
        self.verify()?;
        Ok(())
    }

    // The hook is private; production callers always use the no-op above.
    // Tests inject failures at real write/sync boundaries without replacing a
    // pinned production file or exposing a runtime failpoint.
    fn append_inner(
        &mut self,
        broker: &str,
        kind: Kind,
        payload: &[u8],
        clean_checkpoint: bool,
        recovery_checkpoint: bool,
        mut hook: impl FnMut(WriteStage) -> io::Result<()>,
    ) -> io::Result<Head> {
        if !valid_broker(broker) || broker == "none" {
            return Err(invalid("invalid broker incarnation"));
        }
        // A new broker cannot silently inherit the current dirty head.
        if self.head.mode == Mode::Dirty && self.head.broker != broker {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "dirty head belongs to another broker",
            ));
        }
        let frames = self.verify()?;
        if clean_checkpoint
            && (kind != Kind::CleanCheckpoint
                || self.head.mode != Mode::Dirty
                || self.head.broker != broker
                || frames.last().is_none_or(|f| {
                    f.kind
                        != if recovery_checkpoint {
                            Kind::RecoveryCommit
                        } else {
                            Kind::BrokerDrained
                        }
                }))
        {
            return Err(invalid("clean checkpoint is not after this broker's drain"));
        }
        let next_sequence = self
            .head
            .sequence
            .checked_add(1)
            .ok_or_else(|| invalid("recovery sequence overflow"))?;
        let frame = Frame::encode(kind, next_sequence, self.head.checksum, payload)?;
        #[cfg(test)]
        let limit = self.write_limit;
        #[cfg(not(test))]
        let limit = MAX_JOURNAL;
        let next_offset = match self
            .head
            .offset
            .checked_add(frame.len() as u64)
            .filter(|n| *n <= limit)
        {
            Some(offset) => offset,
            None => {
                self.poisoned = true;
                return Err(invalid("recovery journal limit reached"));
            }
        };
        let checksum_index = frame.len() - TRAILER_LEN;
        let checksum = u64::from_be_bytes(
            frame[checksum_index..checksum_index + 8]
                .try_into()
                .unwrap(),
        );
        let next = Head {
            mode: if clean_checkpoint {
                Mode::Clean
            } else {
                Mode::Dirty
            },
            broker: broker.to_owned(),
            sequence: next_sequence,
            offset: next_offset,
            checksum,
            clean_sequence: if clean_checkpoint {
                next_sequence
            } else {
                self.head.clean_sequence
            },
        };
        let result = (|| -> io::Result<()> {
            self.journal.seek(SeekFrom::Start(self.head.offset))?;
            hook(WriteStage::BeforeJournalWrite)?;
            self.journal
                .write_all(&frame[..HEADER_LEN])
                .map_err(|e| io_failure("write-header", e))?;
            hook(WriteStage::AfterPartialJournalWrite)?;
            self.journal
                .write_all(&frame[HEADER_LEN..])
                .map_err(|e| io_failure("write-payload", e))?;
            hook(WriteStage::BeforeJournalSync)?;
            self.journal
                .sync_all()
                .map_err(|e| io_failure("sync-journal", e))?;
            hook(WriteStage::AfterJournalSync)?;
            let text = next.encode();
            self.state
                .set_len(0)
                .map_err(|e| io_failure("truncate-head", e))?;
            hook(WriteStage::AfterHeadTruncate)?;
            self.state.seek(SeekFrom::Start(0))?;
            self.state
                .write_all(text.as_bytes())
                .map_err(|e| io_failure("write-head", e))?;
            hook(WriteStage::BeforeHeadSync)?;
            self.state
                .sync_all()
                .map_err(|e| io_failure("sync-head", e))?;
            hook(WriteStage::AfterHeadSync)?;
            Ok(())
        })();
        if let Err(error) = result {
            self.poisoned = true;
            return Err(error);
        }
        self.head = next.clone();
        // Check both namespace and exact on-disk head after the sync barrier.
        self.verify()?;
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct Fixture {
        root: PathBuf,
        journal: PathBuf,
        state: PathBuf,
        journal_identity: (u64, u64),
        state_identity: (u64, u64),
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
            let root = base.join(format!("recovery-journal-{}-{nonce}", std::process::id()));
            fs::create_dir(&root).unwrap();
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
            let journal = root.join("journal");
            let state = root.join("state");
            let mut j = OpenOptions::new()
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
            j.write_all(&[]).unwrap();
            j.sync_all().unwrap();
            s.write_all(AnchoredJournal::genesis_state().as_bytes())
                .unwrap();
            s.sync_all().unwrap();
            File::open(&root).unwrap().sync_all().unwrap();
            Self {
                root,
                journal,
                state,
                journal_identity: identity(&j).unwrap(),
                state_identity: identity(&s).unwrap(),
            }
        }
        fn open(&self) -> io::Result<AnchoredJournal> {
            AnchoredJournal::open(
                &self.journal,
                &self.state,
                self.journal_identity,
                self.state_identity,
            )
        }
    }

    #[test]
    fn crc_has_standard_ecma_check_vector() {
        assert_eq!(crc64(b"123456789"), 0x6c40_df5f_0b49_7347);
    }

    fn bitwise_reference(bytes: &[u8]) -> u64 {
        let mut crc = 0u64;
        for byte in bytes {
            crc ^= u64::from(*byte) << 56;
            for _ in 0..8 {
                crc = if crc & (1 << 63) != 0 {
                    (crc << 1) ^ CRC_POLY
                } else {
                    crc << 1
                };
            }
        }
        crc
    }

    #[test]
    fn table_crc_is_byte_exact_for_random_boundary_and_all_byte_vectors() {
        assert_eq!(crc64(b""), 0);
        for byte in 0..=255u8 {
            assert_eq!(crc64(&[byte]), bitwise_reference(&[byte]));
        }
        let mut random = 0x1234_5678_9abc_def0u64;
        for len in [
            0, 1, 2, 7, 8, 9, 27, 28, 39, 40, 255, 256, 257, 4095, 4096, 8192, 8220, 65536,
        ] {
            let bytes: Vec<u8> = (0..len)
                .map(|_| {
                    random ^= random << 13;
                    random ^= random >> 7;
                    random ^= random << 17;
                    random as u8
                })
                .collect();
            assert_eq!(crc64(&bytes), bitwise_reference(&bytes), "len={len}");
            for fill in [0, 0xff, 0x80, 1] {
                let bytes = vec![fill; len];
                assert_eq!(crc64(&bytes), bitwise_reference(&bytes));
            }
        }
    }

    /// Read-only archived framing regression, opt-in at an explicitly supplied
    /// task evidence directory. Never opens the fixed live authority root.
    #[test]
    #[ignore = "requires UF_DDR_CRC_ARCHIVE_DIR task-owned evidence"]
    fn archived_frames_are_byte_exact_and_corruption_still_rejected() {
        let root = PathBuf::from(std::env::var_os("UF_DDR_CRC_ARCHIVE_DIR").expect("archive dir"));
        let bytes = fs::read(root.join("recovery.journal")).unwrap();
        let head = Head::parse(&fs::read_to_string(root.join("recovery.head")).unwrap()).unwrap();
        let frames = parse_frames(&bytes, &head).unwrap();
        let mut offset = 0;
        for f in &frames {
            let end = offset + f.encoded_len() as usize;
            let encoded = Frame::encode(f.kind, f.sequence, f.previous, &f.payload).unwrap();
            assert_eq!(encoded, bytes[offset..end]);
            assert_eq!(
                f.checksum,
                bitwise_reference(&bytes[offset..end - TRAILER_LEN])
            );
            let mut corrupt = encoded[..encoded.len() - TRAILER_LEN].to_vec();
            corrupt[HEADER_LEN] ^= 1;
            assert_ne!(crc64(&corrupt), f.checksum);
            assert_eq!(crc64(&corrupt), bitwise_reference(&corrupt));
            offset = end;
        }
        assert_eq!(offset, bytes.len());
        for index in [0, bytes.len() / 2, bytes.len() - 1] {
            let mut corrupt = bytes.clone();
            corrupt[index] ^= 1;
            assert!(parse_frames(&corrupt, &head).is_err());
        }
        assert!(parse_frames(&bytes[..bytes.len() - 1], &head).is_err());
        let mut bad_head = head.clone();
        bad_head.checksum ^= 1;
        assert!(parse_frames(&bytes, &bad_head).is_err());
        println!(
            "archived_crc frames={} bytes={} every_frame_reference_and_encoding_equal=true",
            frames.len(),
            bytes.len()
        );
    }

    #[test]
    fn append_sync_reopen_and_dirty_broker_binding() {
        let fixture = Fixture::new();
        let mut log = fixture.open().unwrap();
        assert_eq!(log.head(), &Head::genesis());
        log.append_dirty("broker-a", Kind::SessionRegisterIntent, b"register=1")
            .unwrap();
        log.append_dirty("broker-a", Kind::ViewIntent, b"view=1")
            .unwrap();
        assert!(log
            .append_dirty("broker-b", Kind::ViewCommit, b"view=1")
            .is_err());
        let frames = log.verify().unwrap();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].kind, Kind::SessionRegisterIntent);
        assert_eq!(frames[1].previous, frames[0].checksum);
        assert_eq!(frames[1].payload, b"view=1");
        let expected = log.head().clone();
        drop(log);
        let mut reopened = fixture.open().unwrap();
        assert_eq!(reopened.head(), &expected);
        assert_eq!(reopened.verify().unwrap(), frames);
    }

    #[test]
    fn loss_of_complete_tail_and_extra_complete_tail_are_rejected() {
        let fixture = Fixture::new();
        let mut log = fixture.open().unwrap();
        let first = log
            .append_dirty("broker-a", Kind::ViewIntent, b"first")
            .unwrap();
        log.append_dirty("broker-a", Kind::ViewCommit, b"second")
            .unwrap();
        drop(log);
        let journal = open_private(&fixture.journal, true).unwrap();
        journal.set_len(first.offset).unwrap();
        journal.sync_all().unwrap();
        assert!(fixture.open().is_err());

        let fixture = Fixture::new();
        let mut log = fixture.open().unwrap();
        log.append_dirty("broker-a", Kind::ViewIntent, b"first")
            .unwrap();
        let old_head = log.head().clone();
        let frame = Frame::encode(
            Kind::ViewCommit,
            old_head.sequence + 1,
            old_head.checksum,
            b"second",
        )
        .unwrap();
        drop(log);
        let mut journal = open_private(&fixture.journal, true).unwrap();
        journal.seek(SeekFrom::End(0)).unwrap();
        journal.write_all(&frame).unwrap();
        journal.sync_all().unwrap();
        assert!(fixture.open().is_err());
    }

    #[test]
    fn corruption_truncation_sequence_and_unknown_kind_are_rejected() {
        let first = Frame::encode(Kind::ViewIntent, 1, GENESIS, b"intent").unwrap();
        let last = Frame::encode(
            Kind::ViewCommit,
            2,
            crc64(&first[..first.len() - TRAILER_LEN]),
            b"commit",
        )
        .unwrap();
        let mut bytes = first.clone();
        bytes.extend_from_slice(&last);
        let head = Head {
            mode: Mode::Dirty,
            broker: "broker-a".into(),
            sequence: 2,
            offset: bytes.len() as u64,
            checksum: u64::from_be_bytes(
                last[last.len() - TRAILER_LEN..last.len() - 4]
                    .try_into()
                    .unwrap(),
            ),
            clean_sequence: 0,
        };
        assert_eq!(parse_frames(&bytes, &head).unwrap().len(), 2);
        let mut corrupt = bytes.clone();
        corrupt[HEADER_LEN] ^= 1;
        assert!(parse_frames(&corrupt, &head).is_err());
        assert!(parse_frames(&bytes[..bytes.len() - 1], &head).is_err());
        let mut bad_seq = bytes.clone();
        bad_seq[8..16].copy_from_slice(&2u64.to_be_bytes());
        assert!(parse_frames(&bad_seq, &head).is_err());
        let mut bad_kind = bytes.clone();
        bad_kind[6..8].copy_from_slice(&65535u16.to_be_bytes());
        assert!(parse_frames(&bad_kind, &head).is_err());
        let mut bad_version = bytes.clone();
        bad_version[4..6].copy_from_slice(&2u16.to_be_bytes());
        assert!(parse_frames(&bad_version, &head).is_err());
    }

    #[test]
    fn replacement_or_changed_head_permanently_poison_live_instance() {
        let fixture = Fixture::new();
        let mut log = fixture.open().unwrap();
        log.append_dirty("broker-a", Kind::ViewIntent, b"intent")
            .unwrap();
        fs::rename(&fixture.state, fixture.root.join("old-state")).unwrap();
        fs::write(&fixture.state, AnchoredJournal::genesis_state()).unwrap();
        fs::set_permissions(&fixture.state, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(log
            .append_dirty("broker-a", Kind::ViewCommit, b"commit")
            .is_err());
        fs::remove_file(&fixture.state).unwrap();
        fs::rename(fixture.root.join("old-state"), &fixture.state).unwrap();
        assert!(log
            .append_dirty("broker-a", Kind::ViewCommit, b"commit")
            .is_err());
        assert!(fixture.open().is_ok());
    }

    #[test]
    fn journal_capacity_stops_writer_without_deleting_or_reusing_evidence() {
        let fixture = Fixture::new();
        let mut log = fixture.open().unwrap();
        log.append_dirty("broker-a", Kind::ViewIntent, b"intent")
            .unwrap();
        let before = fs::read(&fixture.journal).unwrap();
        let before_head = fs::read(&fixture.state).unwrap();
        // A smaller limit is a fixture failpoint, not a production limit.
        log.write_limit = log.head().offset;
        assert!(log
            .append_dirty("broker-a", Kind::ViewCommit, b"commit")
            .is_err());
        assert!(log.verify().is_err());
        log.write_limit = MAX_JOURNAL;
        assert!(log
            .append_dirty("broker-a", Kind::ViewCommit, b"commit")
            .is_err());
        assert_eq!(fs::read(&fixture.journal).unwrap(), before);
        assert_eq!(fs::read(&fixture.state).unwrap(), before_head);
        drop(log);
        let reopened = fixture.open().unwrap();
        assert_eq!(reopened.head().mode, Mode::Dirty);
    }

    #[test]
    fn noncanonical_head_bad_lengths_and_invented_clean_checkpoint_fail() {
        let fixture = Fixture::new();
        fs::write(&fixture.state, b"clean\n").unwrap();
        assert!(fixture.open().is_err());
        assert!(Frame::encode(Kind::ViewIntent, 0, GENESIS, b"x").is_err());
        assert!(Frame::encode(Kind::ViewIntent, 1, GENESIS, b"").is_err());
        assert!(Frame::encode(Kind::ViewIntent, 1, GENESIS, &vec![0; MAX_PAYLOAD + 1]).is_err());
        let frame = Frame::encode(Kind::ViewIntent, 1, GENESIS, b"x").unwrap();
        let fake_clean = Head {
            mode: Mode::Clean,
            broker: "broker-a".into(),
            sequence: 1,
            offset: frame.len() as u64,
            checksum: u64::from_be_bytes(
                frame[frame.len() - TRAILER_LEN..frame.len() - 4]
                    .try_into()
                    .unwrap(),
            ),
            clean_sequence: 1,
        };
        assert!(parse_frames(&frame, &fake_clean).is_err());
    }

    #[test]
    fn clean_head_requires_same_broker_and_immediately_preceding_drain() {
        let fixture = Fixture::new();
        let mut log = fixture.open().unwrap();
        assert!(log
            .fixture_append_framing_clean("broker-a", b"checkpoint")
            .is_err());
        log.append_dirty("broker-a", Kind::ViewIntent, b"intent")
            .unwrap();
        assert!(log
            .fixture_append_framing_clean("broker-a", b"checkpoint")
            .is_err());
        assert!(log
            .append_dirty("broker-a", Kind::CleanCheckpoint, b"checkpoint")
            .is_err());
        log.append_dirty("broker-a", Kind::BrokerDrained, b"drained")
            .unwrap();
        assert!(log
            .fixture_append_framing_clean("broker-b", b"checkpoint")
            .is_err());
        let clean = log
            .fixture_append_framing_clean("broker-a", b"checkpoint")
            .unwrap();
        assert_eq!(clean.mode, Mode::Clean);
        assert_eq!(clean.clean_sequence, clean.sequence);
        drop(log);
        assert_eq!(fixture.open().unwrap().head(), &clean);
        // The opaque payload is intentionally not semantic proof: the durable
        // replay layer must reject it when starting a new writer.
    }

    #[test]
    fn checkpoint_write_faults_poison_and_keep_the_exact_head_contract() {
        let cases = [
            (WriteStage::BeforeJournalWrite, Some(Mode::Dirty)),
            (WriteStage::AfterPartialJournalWrite, None),
            (WriteStage::BeforeJournalSync, None),
            (WriteStage::AfterJournalSync, None),
            (WriteStage::AfterHeadTruncate, None),
            // A live read can see a complete new head even when sync was not
            // confirmed. This test does not claim crash persistence.
            (WriteStage::BeforeHeadSync, Some(Mode::Clean)),
            (WriteStage::AfterHeadSync, Some(Mode::Clean)),
        ];
        for (stage, reopen_mode) in cases {
            let fixture = Fixture::new();
            let mut log = fixture.open().unwrap();
            log.append_dirty("broker-a", Kind::BrokerDrained, b"fixture-drain")
                .unwrap();
            let error = log
                .append_inner(
                    "broker-a",
                    Kind::CleanCheckpoint,
                    b"fixture-checkpoint",
                    true,
                    false,
                    |point| {
                        if point == stage {
                            Err(io::Error::from_raw_os_error(libc::ENOSPC))
                        } else {
                            Ok(())
                        }
                    },
                )
                .unwrap_err();
            assert_eq!(error.raw_os_error(), Some(libc::ENOSPC));
            assert!(log.verify().is_err());
            drop(log);
            match (fixture.open(), reopen_mode) {
                (Ok(reopened), Some(mode)) => assert_eq!(reopened.head().mode, mode),
                (Err(_), None) => (),
                _ => panic!("unexpected reopen after checkpoint fault {stage:?}"),
            }
        }
    }

    #[test]
    fn simulated_write_and_sync_failures_poison_and_never_reopen_as_false_clean() {
        let cases = [
            (WriteStage::BeforeJournalWrite, true, false),
            (WriteStage::AfterPartialJournalWrite, false, false),
            (WriteStage::BeforeJournalSync, false, false),
            (WriteStage::AfterJournalSync, false, false),
            (WriteStage::AfterHeadTruncate, false, false),
            (WriteStage::BeforeHeadSync, true, true),
            (WriteStage::AfterHeadSync, true, true),
        ];
        for (injection, may_reopen, must_be_dirty) in cases {
            let fixture = Fixture::new();
            let mut log = fixture.open().unwrap();
            // Simulates an ENOSPC/short-write or sync error at each explicit
            // boundary; this is not a claim of actual media failure testing.
            let failure = log.append_inner(
                "broker-a",
                Kind::ViewIntent,
                b"intent",
                false,
                false,
                |stage| {
                    if stage == injection {
                        Err(io::Error::from_raw_os_error(libc::ENOSPC))
                    } else {
                        Ok(())
                    }
                },
            );
            assert_eq!(failure.unwrap_err().raw_os_error(), Some(libc::ENOSPC));
            assert!(log
                .append_dirty("broker-a", Kind::ViewCommit, b"commit")
                .is_err());
            drop(log);
            match fixture.open() {
                Ok(reopened) => {
                    assert!(may_reopen, "unexpected valid reopen after {injection:?}");
                    if must_be_dirty {
                        assert_eq!(reopened.head().mode, Mode::Dirty, "{injection:?}");
                    } else {
                        assert_eq!(reopened.head(), &Head::genesis());
                    }
                }
                Err(_) => assert!(!may_reopen, "unexpected invalid reopen after {injection:?}"),
            }
        }
    }
}
