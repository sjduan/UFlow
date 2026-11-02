//! Exclusive broker ownership for an explicitly configured managed DDR domain.
//!
//! This OS-backed lock prevents two new brokers configured for the same shared
//! authority directory and physical host from each claiming the same quota.
//! It is not itself a reservation, catalog, or proof of page residency. A
//! deployment must supply the same persistent, shared directory to every broker
//! in the domain, and must not unlink or replace the lock file while in use.
//! This primitive alone cannot establish the managed production domain; the
//! approved deployment does not permit an unmanaged T1 fallback within it.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

pub(crate) struct AuthorityLock {
    _file: File,
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl AuthorityLock {
    /// Acquire a nonblocking, process-shared lock. The directory is a trusted,
    /// persistent namespace shared by all configured brokers in this domain;
    /// it must not be a private temporary directory per daemon.
    pub(crate) fn acquire(directory: &Path, host_id: &str) -> io::Result<Self> {
        if !directory.is_absolute()
            || host_id.is_empty()
            || host_id.len() > 96
            || !host_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid authority directory or host ID",
            ));
        }
        let directory = fs::canonicalize(directory)?;
        let dir_meta = fs::metadata(&directory)?;
        if !dir_meta.is_dir()
            || dir_meta.uid() != unsafe { libc::geteuid() }
            || dir_meta.mode() & 0o022 != 0
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "authority directory must be owned by this UID and not group/other writable",
            ));
        }
        let path = directory.join(format!("uflow-external-ddr-{host_id}.lock"));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o077 != 0
            || metadata.nlink() != 1
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "authority lock must be a private, singly-linked regular file",
            ));
        }
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let lock = Self {
            _file: file,
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
        };
        lock.ensure_valid()?;
        Ok(lock)
    }

    /// Fail closed if the namespace entry no longer refers to the locked inode.
    /// This does not protect against hostile same-UID replacement after this
    /// check; the v0 domain is a cooperative, trusted configuration boundary.
    pub(crate) fn ensure_valid(&self) -> io::Result<()> {
        let entry = fs::symlink_metadata(&self.path)?;
        if !entry.is_file()
            || entry.dev() != self.device
            || entry.ino() != self.inode
            || entry.nlink() != 1
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "authority lock entry was replaced or unlinked",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::process::Command;

    fn directory(name: &str) -> PathBuf {
        let base = std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target"));
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = base.join(format!(
            "authority-test-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        directory
    }

    fn child(directory: &Path, expected: &str) {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "ddr_authority::tests::child_lock_helper",
                "--nocapture",
            ])
            .env("UF_DDR_TEST_LOCK_DIR", directory)
            .env("UF_DDR_TEST_LOCK_EXPECTED", expected)
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success()
                && stdout.contains("running 1 test")
                && stdout.contains("1 passed"),
            "child did not pass its one helper test: stdout={stdout}, stderr={}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn child_lock_helper() {
        let Some(dir) = std::env::var_os("UF_DDR_TEST_LOCK_DIR") else {
            return;
        };
        let expected = std::env::var("UF_DDR_TEST_LOCK_EXPECTED").unwrap();
        let result = AuthorityLock::acquire(Path::new(&dir), "physical-host");
        match expected.as_str() {
            "busy" => match result {
                Err(error) => assert_eq!(error.kind(), io::ErrorKind::WouldBlock),
                Ok(_) => panic!("second process acquired the same authority"),
            },
            "available" => assert!(result.is_ok(), "authority was not released"),
            _ => panic!("invalid child expectation"),
        }
    }

    #[test]
    fn same_managed_domain_excludes_an_independent_process_until_release() {
        let directory = directory("process");
        let lock = AuthorityLock::acquire(&directory, "physical-host").unwrap();
        lock.ensure_valid().unwrap();
        child(&directory, "busy");
        // A separate host identifier is a different explicit authority domain.
        let other = AuthorityLock::acquire(&directory, "other-host").unwrap();
        drop(other);
        drop(lock);
        child(&directory, "available");
    }

    #[test]
    fn different_configured_directories_do_not_establish_one_host_authority() {
        let first = directory("first-namespace");
        let second = directory("second-namespace");
        let _lock = AuthorityLock::acquire(&first, "physical-host").unwrap();
        // This deliberately succeeds. A caller-selected directory alone cannot
        // guarantee that two brokers on the same host share one budget.
        child(&second, "available");
    }

    #[test]
    fn rejects_untrusted_directory_and_detects_replaced_lock_inode() {
        let directory = directory("replacement");
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o777)).unwrap();
        assert!(AuthorityLock::acquire(&directory, "physical-host").is_err());
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        let lock = AuthorityLock::acquire(&directory, "physical-host").unwrap();
        fs::rename(&lock.path, directory.join("old-lock")).unwrap();
        File::create(&lock.path).unwrap();
        assert!(lock.ensure_valid().is_err());
    }
}
