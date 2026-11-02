//! Private execution primitive for the external DDR path.
//!
//! This module does not admit work, retain resources, publish a destination, or
//! grant a capability. The broker must do all of those before calling it. In
//! particular it must retain both files through the worker's actual return and
//! discard an unsuccessful destination only after that return.

use std::fs::File;
use std::io::{self, ErrorKind};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

const CHUNK_BYTES: usize = 64 * 1024;
// Exact neighbouring-byte verification needs a pre-copy image. Limit this
// temporary proof buffer; it is not a managed/publishable DDR backing. Larger
// complements fail explicitly until an accounted streaming proof exists.
const MAX_NEIGHBOR_PROOF_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CopyFailureKind {
    InvalidRange,
    InvalidBacking,
    InvalidAccess,
    Cancelled,
    Io,
    Verification,
}

#[derive(Debug)]
pub(crate) struct CopyFailure {
    pub kind: CopyFailureKind,
    /// Actual bytes written before the failure, including a partial write.
    pub bytes_copied: u64,
    /// Bytes subsequently read back and compared to the source.
    pub bytes_verified: u64,
    pub detail: String,
}

#[derive(Debug)]
pub(crate) struct CopyCompletion {
    pub bytes_copied: u64,
    pub bytes_verified: u64,
    pub execute_seconds: f64,
    proof: Option<NeighborProof>,
    source_identity: (u64, u64, u64),
    target_identity: (u64, u64, u64),
}

#[derive(Debug)]
struct NeighborProof {
    source_offset: u64,
    target_offset: u64,
    bytes: u64,
    target_size: u64,
    left_len: usize,
    original_neighbors: Vec<u8>,
}

impl CopyCompletion {
    /// Recheck exact backing identity/size after seal. Only explicit legacy
    /// validation completions also own a pre-copy neighbour proof. An ordinary
    /// I/O completion is never represented as exact byte-validation evidence.
    pub(crate) fn verify_final(&self, source: &File, target: &File) -> Result<(), String> {
        let identity = |file: &File| -> Result<_, String> {
            let m = file.metadata().map_err(|e| e.to_string())?;
            Ok((m.dev(), m.ino(), m.len()))
        };
        if identity(source)? != self.source_identity || identity(target)? != self.target_identity {
            return Err("copy backing identity or size changed".into());
        }
        if let Some(proof) = &self.proof {
            proof.verify(source, target)?;
        }
        Ok(())
    }
}

/// Ordinary I/O completion, not byte-validation evidence. No complement
/// snapshot or payload readback. The broker validates its private probe later.
pub(crate) fn copy_range(
    source: &File,
    target: &File,
    source_offset: u64,
    target_offset: u64,
    nbytes: u64,
    cancelled: &AtomicBool,
) -> Result<CopyCompletion, CopyFailure> {
    copy_internal(
        source,
        target,
        source_offset,
        target_offset,
        nbytes,
        cancelled,
        false,
        |_| {},
    )
}

pub(crate) fn fill_probe(target: &File) -> Result<(), String> {
    let size = target.metadata().map_err(|e| e.to_string())?.len();
    let mut buffer = vec![0; CHUNK_BYTES];
    let mut offset = 0;
    while offset < size {
        let n = (size - offset).min(CHUNK_BYTES as u64) as usize;
        for (i, b) in buffer[..n].iter_mut().enumerate() {
            *b = ((offset + i as u64) % 251) as u8 ^ 0x5a;
        }
        target
            .write_all_at(&buffer[..n], offset)
            .map_err(|e| e.to_string())?;
        offset += n as u64;
    }
    Ok(())
}

/// Exact full target verification against a known positional pattern and the
/// retained immutable source, with two fixed-size buffers, never a digest.
pub(crate) fn verify_probe(
    source: &File,
    target: &File,
    source_offset: u64,
    target_offset: u64,
    bytes: u64,
) -> Result<u64, String> {
    let size = target.metadata().map_err(|e| e.to_string())?.len();
    let end = target_offset
        .checked_add(bytes)
        .filter(|n| *n <= size)
        .ok_or("invalid probe range")?;
    let mut actual = vec![0; CHUNK_BYTES];
    let mut expected = vec![0; CHUNK_BYTES];
    let mut offset = 0;
    while offset < size {
        let boundary = if offset < target_offset {
            target_offset
        } else if offset < end {
            end
        } else {
            size
        };
        let n = (boundary - offset).min(CHUNK_BYTES as u64) as usize;
        target
            .read_exact_at(&mut actual[..n], offset)
            .map_err(|e| e.to_string())?;
        if offset >= target_offset && offset < end {
            source
                .read_exact_at(&mut expected[..n], source_offset + offset - target_offset)
                .map_err(|e| e.to_string())?;
        } else {
            for (i, b) in expected[..n].iter_mut().enumerate() {
                *b = ((offset + i as u64) % 251) as u8 ^ 0x5a;
            }
        }
        if actual[..n] != expected[..n] {
            return Err("probe payload or full neighbours differ".into());
        }
        offset += n as u64;
    }
    Ok(size)
}

impl NeighborProof {
    fn capture(
        source_offset: u64,
        target: &File,
        target_offset: u64,
        bytes: u64,
        target_size: u64,
    ) -> Result<Self, CopyFailure> {
        let count = target_size - bytes;
        if count > MAX_NEIGHBOR_PROOF_BYTES {
            return Err(failure(
                CopyFailureKind::InvalidRange,
                0,
                0,
                "target neighbour proof exceeds bounded scratch limit",
            ));
        }
        let left_len = usize::try_from(target_offset).map_err(|_| {
            failure(
                CopyFailureKind::InvalidRange,
                0,
                0,
                "target offset does not fit memory",
            )
        })?;
        let count = usize::try_from(count).map_err(|_| {
            failure(
                CopyFailureKind::InvalidRange,
                0,
                0,
                "neighbour proof does not fit memory",
            )
        })?;
        let mut original_neighbors = Vec::new();
        original_neighbors.try_reserve_exact(count).map_err(|_| {
            failure(
                CopyFailureKind::Io,
                0,
                0,
                "neighbour proof allocation failed",
            )
        })?;
        original_neighbors.resize(count, 0);
        target
            .read_exact_at(&mut original_neighbors[..left_len], 0)
            .map_err(|e| failure(CopyFailureKind::Io, 0, 0, e))?;
        target
            .read_exact_at(&mut original_neighbors[left_len..], target_offset + bytes)
            .map_err(|e| failure(CopyFailureKind::Io, 0, 0, e))?;
        Ok(Self {
            source_offset,
            target_offset,
            bytes,
            target_size,
            left_len,
            original_neighbors,
        })
    }

    fn verify(&self, source: &File, target: &File) -> Result<(), String> {
        if target.metadata().map_err(|e| e.to_string())?.len() != self.target_size {
            return Err("target size changed during proof".into());
        }
        let mut buffer = vec![0; CHUNK_BYTES];
        for (offset, expected) in [
            (0, &self.original_neighbors[..self.left_len]),
            (
                self.target_offset + self.bytes,
                &self.original_neighbors[self.left_len..],
            ),
        ] {
            for (index, chunk) in expected.chunks(CHUNK_BYTES).enumerate() {
                let start = offset + (index * CHUNK_BYTES) as u64;
                target
                    .read_exact_at(&mut buffer[..chunk.len()], start)
                    .map_err(|e| e.to_string())?;
                if &buffer[..chunk.len()] != chunk {
                    return Err("target neighbours changed".into());
                }
            }
        }
        let mut source_buffer = vec![0; CHUNK_BYTES];
        let mut position = 0u64;
        while position < self.bytes {
            let count = (self.bytes - position).min(CHUNK_BYTES as u64) as usize;
            source
                .read_exact_at(&mut source_buffer[..count], self.source_offset + position)
                .map_err(|e| e.to_string())?;
            target
                .read_exact_at(&mut buffer[..count], self.target_offset + position)
                .map_err(|e| e.to_string())?;
            if source_buffer[..count] != buffer[..count] {
                return Err("target differs from source".into());
            }
            position += count as u64;
        }
        Ok(())
    }
}

fn failure(
    kind: CopyFailureKind,
    copied: u64,
    verified: u64,
    detail: impl ToString,
) -> CopyFailure {
    CopyFailure {
        kind,
        bytes_copied: copied,
        bytes_verified: verified,
        detail: detail.to_string(),
    }
}

fn validate_file_access(source: &File, target: &File) -> Result<(), CopyFailure> {
    let src = source
        .metadata()
        .map_err(|e| failure(CopyFailureKind::InvalidBacking, 0, 0, e))?;
    let dst = target
        .metadata()
        .map_err(|e| failure(CopyFailureKind::InvalidBacking, 0, 0, e))?;
    if !src.is_file() || !dst.is_file() || (src.dev(), src.ino()) == (dst.dev(), dst.ino()) {
        return Err(failure(
            CopyFailureKind::InvalidBacking,
            0,
            0,
            "source and target must be different regular backings",
        ));
    }
    let source_flags = unsafe { libc::fcntl(source.as_raw_fd(), libc::F_GETFL) };
    let target_flags = unsafe { libc::fcntl(target.as_raw_fd(), libc::F_GETFL) };
    if source_flags < 0
        || target_flags < 0
        || source_flags & libc::O_ACCMODE != libc::O_RDONLY
        || target_flags & libc::O_ACCMODE != libc::O_RDWR
        || target_flags & libc::O_APPEND != 0
    {
        return Err(failure(
            CopyFailureKind::InvalidAccess,
            0,
            0,
            "source must be read-only and target must be non-append read/write",
        ));
    }
    let source_seals = unsafe { libc::fcntl(source.as_raw_fd(), libc::F_GET_SEALS) };
    let target_seals = unsafe { libc::fcntl(target.as_raw_fd(), libc::F_GET_SEALS) };
    let size_seals = libc::F_SEAL_GROW | libc::F_SEAL_SHRINK;
    if source_seals < 0
        || target_seals < 0
        || source_seals & size_seals != size_seals
        || target_seals & size_seals != size_seals
        || target_seals & (libc::F_SEAL_WRITE | libc::F_SEAL_SEAL) != 0
    {
        return Err(failure(
            CopyFailureKind::InvalidBacking,
            0,
            0,
            "size-sealed memfds required; target must remain writable and sealable",
        ));
    }
    Ok(())
}

/// Legacy explicit full-proof fixture/validation path, NOT ordinary capacity.
/// Production capability and business workers both call copy_range instead.
pub(crate) fn copy_and_verify(
    source: &File,
    target: &File,
    source_offset: u64,
    target_offset: u64,
    nbytes: u64,
    cancelled: &AtomicBool,
) -> Result<CopyCompletion, CopyFailure> {
    copy_and_verify_with_progress(
        source,
        target,
        source_offset,
        target_offset,
        nbytes,
        cancelled,
        |_| {},
    )
}

fn copy_and_verify_with_progress(
    source: &File,
    target: &File,
    source_offset: u64,
    target_offset: u64,
    nbytes: u64,
    cancelled: &AtomicBool,
    progress: impl FnMut(u64),
) -> Result<CopyCompletion, CopyFailure> {
    copy_internal(
        source,
        target,
        source_offset,
        target_offset,
        nbytes,
        cancelled,
        true,
        progress,
    )
}

fn copy_internal(
    source: &File,
    target: &File,
    source_offset: u64,
    target_offset: u64,
    nbytes: u64,
    cancelled: &AtomicBool,
    explicit_proof: bool,
    mut progress: impl FnMut(u64),
) -> Result<CopyCompletion, CopyFailure> {
    let started = Instant::now();
    validate_file_access(source, target)?;
    let source_len = source
        .metadata()
        .map_err(|e| failure(CopyFailureKind::InvalidBacking, 0, 0, e))?
        .len();
    let target_len = target
        .metadata()
        .map_err(|e| failure(CopyFailureKind::InvalidBacking, 0, 0, e))?
        .len();
    if nbytes == 0
        || source_offset
            .checked_add(nbytes)
            .is_none_or(|end| end > source_len)
        || target_offset
            .checked_add(nbytes)
            .is_none_or(|end| end > target_len)
    {
        return Err(failure(
            CopyFailureKind::InvalidRange,
            0,
            0,
            "copy range exceeds source or target backing",
        ));
    }

    // Only the legacy explicit-validation fixture takes a complement snapshot.
    let proof = if explicit_proof {
        Some(NeighborProof::capture(
            source_offset,
            target,
            target_offset,
            nbytes,
            target_len,
        )?)
    } else {
        None
    };
    let mut input = vec![0u8; CHUNK_BYTES];
    let mut readback = vec![0u8; CHUNK_BYTES];
    let mut copied = 0u64;
    let mut verified = 0u64;
    while copied < nbytes {
        if cancelled.load(Ordering::Acquire) {
            return Err(failure(
                CopyFailureKind::Cancelled,
                copied,
                verified,
                "cancel requested",
            ));
        }
        let chunk = (nbytes - copied).min(CHUNK_BYTES as u64) as usize;
        source
            .read_exact_at(&mut input[..chunk], source_offset + copied)
            .map_err(|e| failure(CopyFailureKind::Io, copied, verified, e))?;
        let mut written = 0usize;
        while written < chunk {
            if cancelled.load(Ordering::Acquire) {
                return Err(failure(
                    CopyFailureKind::Cancelled,
                    copied,
                    verified,
                    "cancel requested",
                ));
            }
            match target.write_at(&input[written..chunk], target_offset + copied) {
                Ok(0) => {
                    return Err(failure(
                        CopyFailureKind::Io,
                        copied,
                        verified,
                        io::Error::new(ErrorKind::WriteZero, "target write returned zero"),
                    ));
                }
                Ok(count) => {
                    written += count;
                    copied += count as u64;
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(failure(CopyFailureKind::Io, copied, verified, e)),
            }
        }
        if explicit_proof {
            target
                .read_exact_at(
                    &mut readback[..chunk],
                    target_offset + copied - chunk as u64,
                )
                .map_err(|e| failure(CopyFailureKind::Io, copied, verified, e))?;
            if readback[..chunk] != input[..chunk] {
                return Err(failure(
                    CopyFailureKind::Verification,
                    copied,
                    verified,
                    "target readback differs from source",
                ));
            }
            verified += chunk as u64;
        }
        progress(copied);
    }
    // A cancel accepted while the last write/readback was running must not be
    // misreported as success. The authority still owns final publication.
    if cancelled.load(Ordering::Acquire) {
        return Err(failure(
            CopyFailureKind::Cancelled,
            copied,
            verified,
            "cancel requested",
        ));
    }
    if let Some(proof) = &proof {
        proof
            .verify(source, target)
            .map_err(|e| failure(CopyFailureKind::Verification, copied, verified, e))?;
    }
    if cancelled.load(Ordering::Acquire) {
        return Err(failure(
            CopyFailureKind::Cancelled,
            copied,
            verified,
            "cancel requested during proof",
        ));
    }
    Ok(CopyCompletion {
        bytes_copied: copied,
        bytes_verified: verified,
        execute_seconds: started.elapsed().as_secs_f64(),
        proof,
        source_identity: {
            let m = source
                .metadata()
                .map_err(|e| failure(CopyFailureKind::Io, copied, verified, e))?;
            (m.dev(), m.ino(), m.len())
        },
        target_identity: {
            let m = target
                .metadata()
                .map_err(|e| failure(CopyFailureKind::Io, copied, verified, e))?;
            (m.dev(), m.ino(), m.len())
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;
    use std::os::fd::FromRawFd;
    use std::sync::atomic::AtomicBool;

    fn memfd(name: &str, bytes: &[u8]) -> File {
        let name = CString::new(name).unwrap();
        let fd = unsafe {
            libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING)
        };
        assert!(fd >= 0);
        let file = unsafe { File::from_raw_fd(fd) };
        file.set_len(bytes.len() as u64).unwrap();
        file.write_all_at(bytes, 0).unwrap();
        assert_eq!(
            unsafe {
                libc::fcntl(
                    file.as_raw_fd(),
                    libc::F_ADD_SEALS,
                    libc::F_SEAL_GROW | libc::F_SEAL_SHRINK,
                )
            },
            0
        );
        file
    }

    fn readonly(file: &File) -> File {
        File::open(format!("/proc/self/fd/{}", file.as_raw_fd())).unwrap()
    }

    fn bytes(file: &File, size: usize) -> Vec<u8> {
        let mut result = vec![0; size];
        file.read_exact_at(&mut result, 0).unwrap();
        result
    }

    #[test]
    fn independent_copy_checks_nonzero_offsets_and_all_neighbours() {
        let source_payload: Vec<u8> = (0..150_000).map(|i| (i % 251) as u8).collect();
        let target_payload: Vec<u8> = (0..155_000).map(|i| ((i + 71) % 253) as u8).collect();
        let original_source = memfd("source", &source_payload);
        let source = readonly(&original_source);
        let target = memfd("target", &target_payload);
        let result =
            copy_and_verify(&source, &target, 313, 557, 100_111, &AtomicBool::new(false)).unwrap();
        assert_eq!(result.bytes_copied, 100_111);
        assert_eq!(result.bytes_verified, 100_111);
        let mut expected = target_payload;
        expected[557..557 + 100_111].copy_from_slice(&source_payload[313..313 + 100_111]);
        assert_eq!(bytes(&target, expected.len()), expected);
        assert_eq!(bytes(&source, source_payload.len()), source_payload);
    }

    #[test]
    fn neighbour_proof_rejects_mutation_during_and_after_worker() {
        let source_rw = memfd("proof-source", &vec![0xa5; 4096]);
        let source = readonly(&source_rw);
        let target = memfd("proof-target", &vec![0x5a; 4096]);
        let cancelled = AtomicBool::new(false);
        let error = copy_and_verify_with_progress(&source, &target, 17, 37, 19, &cancelled, |_| {
            target.write_all_at(&[0x11], 2).unwrap();
        })
        .unwrap_err();
        assert_eq!(error.kind, CopyFailureKind::Verification);
        assert_eq!((error.bytes_copied, error.bytes_verified), (19, 19));

        let target = memfd("proof-target-after", &vec![0x5a; 4096]);
        let completion = copy_and_verify(&source, &target, 17, 37, 19, &cancelled).unwrap();
        assert!(completion.verify_final(&source, &target).is_ok());
        target.write_all_at(&[0x11], 4095).unwrap();
        assert!(completion.verify_final(&source, &target).is_err());
        target.write_all_at(&[0x5a], 4095).unwrap();
        source_rw.write_all_at(&[0x12], 17).unwrap();
        assert!(completion.verify_final(&source, &target).is_err());
    }

    #[test]
    fn oversized_neighbour_proof_fails_before_any_copy() {
        let source_rw = memfd("proof-limit-source", &[0xa5; 8]);
        let source = readonly(&source_rw);
        let target = memfd(
            "proof-limit-target",
            &vec![0x5a; MAX_NEIGHBOR_PROOF_BYTES as usize + 2],
        );
        let error =
            copy_and_verify(&source, &target, 0, 0, 1, &AtomicBool::new(false)).unwrap_err();
        assert_eq!(error.kind, CopyFailureKind::InvalidRange);
        assert_eq!((error.bytes_copied, error.bytes_verified), (0, 0));
        let mut first = [0u8; 1];
        target.read_exact_at(&mut first, 0).unwrap();
        assert_eq!(first, [0x5a]);
    }

    #[test]
    fn invalid_range_alias_and_write_access_fail_before_writing() {
        let initial = vec![0x5a; 4096];
        let src_rw = memfd("source", &vec![0xa5; 4096]);
        let src = readonly(&src_rw);
        let dst = memfd("target", &initial);
        let cancel = AtomicBool::new(false);
        for (source, target, so, to, count, kind) in [
            (&src, &dst, 4095, 0, 2, CopyFailureKind::InvalidRange),
            (&src, &dst, 0, u64::MAX, 2, CopyFailureKind::InvalidRange),
            (&src, &dst, 0, 0, 0, CopyFailureKind::InvalidRange),
            (&src, &src_rw, 0, 0, 1, CopyFailureKind::InvalidBacking),
            (&src_rw, &dst, 0, 0, 1, CopyFailureKind::InvalidAccess),
        ] {
            let error = copy_and_verify(source, target, so, to, count, &cancel).unwrap_err();
            assert_eq!(error.kind, kind);
            assert_eq!(error.bytes_copied, 0);
        }
        assert_eq!(bytes(&dst, initial.len()), initial);
    }

    #[test]
    fn cancellation_reports_real_partial_work_and_never_reports_success() {
        let src_rw = memfd("source", &vec![0xa5; 150_000]);
        let source = readonly(&src_rw);
        let target = memfd("target", &vec![0x5a; 150_000]);
        let cancelled = AtomicBool::new(false);
        let error =
            copy_and_verify_with_progress(&source, &target, 1, 2, 100_000, &cancelled, |_| {
                cancelled.store(true, Ordering::Release);
            })
            .unwrap_err();
        assert_eq!(error.kind, CopyFailureKind::Cancelled);
        assert_eq!(error.bytes_copied, CHUNK_BYTES as u64);
        assert_eq!(error.bytes_verified, CHUNK_BYTES as u64);
        let actual = bytes(&target, 150_000);
        assert_eq!(&actual[..2], &[0x5a; 2]);
        assert!(actual[2..2 + CHUNK_BYTES].iter().all(|b| *b == 0xa5));
        assert!(actual[2 + CHUNK_BYTES..].iter().all(|b| *b == 0x5a));
    }
}
