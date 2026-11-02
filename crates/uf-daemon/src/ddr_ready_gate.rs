//! Private direct-parent ready-gate primitive for future trusted deployment.
//!
//! This module alone does not authorize a broker, install a BIND, or enable
//! admission. A deployment actor must durably commit the exact BIND while
//! holding the original lock, then release it before the gated broker opens
//! and independently locks the same inode. Never inherit the locked open-file
//! description. The broker must verify the parent against the pinned policy
//! and committed BIND before any protected effect. No alternate authority
//! root or evidence selector is exposed by this primitive.
#![allow(dead_code)]

use crate::ddr_process_identity::{self, ProcessBirth};
use crate::ddr_witness_active::{ensure_live, live_pidfd, parent_pid};
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::time::Duration;

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message)
}

/// The receiver waits before the first broker listener or protected effect.
/// The pidfd remains held through release so PID reuse or parent exit cannot
/// turn a closed/foreign channel into a successful deployment handoff.
pub(crate) struct ParentReadyGate {
    stream: UnixStream,
    parent: ProcessBirth,
    parent_fd: File,
}

impl ParentReadyGate {
    /// SO_PEERCRED is a channel check, not deployment-policy authentication.
    /// A caller using a provisional observed parent must compare it to the
    /// pinned live witness before committing or allowing any protected effect.
    pub(crate) fn arrive(
        mut stream: UnixStream,
        parent: ProcessBirth,
        timeout: Duration,
    ) -> io::Result<Self> {
        if timeout.is_zero() {
            return Err(invalid("ready gate requires a bounded timeout"));
        }
        let mut peer: libc::ucred = unsafe { std::mem::zeroed() };
        let mut size = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
        if unsafe {
            libc::getsockopt(
                stream.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                (&mut peer as *mut libc::ucred).cast(),
                &mut size,
            )
        } != 0
        {
            return Err(io::Error::last_os_error());
        }
        if size as usize != std::mem::size_of::<libc::ucred>()
            || peer.pid != parent.pid
            || peer.uid != parent.uid
            || peer.gid != parent.gid
            || parent_pid(std::process::id() as i32)? != parent.pid
        {
            return Err(invalid("ready gate peer is not the bound direct parent"));
        }
        let parent_fd = live_pidfd(&parent)?;
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;
        ensure_live(&parent_fd)?;
        stream.write_all(b"UFLOW-READY-1\n")?;
        Ok(Self {
            stream,
            parent,
            parent_fd,
        })
    }

    pub(crate) fn parent(&self) -> &ProcessBirth {
        &self.parent
    }

    /// A deployment binder calls this only while it owns the original lock.
    /// It does not transfer that lock description to either child or parent.
    pub(crate) fn announce_locked(&mut self) -> io::Result<()> {
        ensure_live(&self.parent_fd)?;
        self.stream.write_all(b"UFLOW-BINDER-LOCKED\n")
    }

    /// Only a still-live direct parent on the original peer channel can
    /// release. The caller must then re-read its anchored BIND before any
    /// effect; a byte on this channel is not itself proof of a transaction.
    pub(crate) fn wait_release(mut self) -> io::Result<UnixStream> {
        let mut command = [0u8; 1];
        self.stream.read_exact(&mut command)?;
        if command != *b"R" {
            return Err(invalid("ready gate received an invalid release"));
        }
        ensure_live(&self.parent_fd)?;
        if parent_pid(std::process::id() as i32)? != self.parent.pid
            || ddr_process_identity::observe(self.parent.pid, self.parent.uid, self.parent.gid)?
                != self.parent
        {
            return Err(invalid("ready gate parent changed before release"));
        }
        ensure_live(&self.parent_fd)?;
        Ok(self.stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::fd::FromRawFd;
    use std::os::unix::process::CommandExt;
    use std::path::PathBuf;
    use std::process::{Command, Stdio};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn ready_gate_child() {
        let Ok(mode) = std::env::var("UF_TEST_READY_GATE_MODE") else {
            return;
        };
        let fd: i32 = std::env::var("UF_TEST_READY_GATE_FD")
            .unwrap()
            .parse()
            .unwrap();
        let marker = PathBuf::from(std::env::var_os("UF_TEST_READY_GATE_MARKER").unwrap());
        let pid = unsafe { libc::getppid() };
        let mut parent = ddr_process_identity::observe(pid, unsafe { libc::geteuid() }, unsafe {
            libc::getegid()
        })
        .unwrap();
        if mode == "wrong-birth" {
            parent.starttime_ticks += 1;
        }
        // Test-only descriptor inheritance: production has no FD selector yet.
        let stream = unsafe { UnixStream::from_raw_fd(fd) };
        let gate = ParentReadyGate::arrive(stream, parent, Duration::from_secs(5));
        if mode == "wrong-birth" {
            assert!(gate.is_err());
            return;
        }
        let result = gate.unwrap().wait_release();
        if mode == "allow" {
            result.unwrap();
            fs::write(marker, b"effect-after-release\n").unwrap();
        } else {
            assert!(result.is_err());
            assert!(!marker.exists());
        }
    }

    #[test]
    fn real_direct_parent_gate_refuses_wrong_birth_invalid_release_and_eof() {
        let base = fs::canonicalize(std::env::var_os("CARGO_TARGET_DIR").unwrap()).unwrap();
        assert!(base.starts_with("/workspace/uflow/runs/"));
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = base.join(format!("ready-gate-{}-{nonce}", std::process::id()));
        fs::create_dir(&root).unwrap();
        for mode in ["allow", "wrong-birth", "invalid", "eof"] {
            let (mut parent, child_socket) = UnixStream::pair().unwrap();
            parent
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let fd = child_socket.as_raw_fd();
            let marker = root.join(mode);
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    "ddr_ready_gate::tests::ready_gate_child",
                    "--nocapture",
                ])
                .env("UF_TEST_READY_GATE_MODE", mode)
                .env("UF_TEST_READY_GATE_FD", fd.to_string())
                .env("UF_TEST_READY_GATE_MARKER", &marker)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            unsafe {
                command.pre_exec(move || {
                    let flags = libc::fcntl(fd, libc::F_GETFD);
                    if flags < 0 || libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0 {
                        return Err(io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
            let child = command.spawn().unwrap();
            drop(child_socket);
            if mode != "wrong-birth" {
                let mut ready = [0u8; 14];
                parent.read_exact(&mut ready).unwrap();
                assert_eq!(&ready, b"UFLOW-READY-1\n");
                assert!(!marker.exists());
                match mode {
                    "allow" => parent.write_all(b"R").unwrap(),
                    "invalid" => parent.write_all(b"X").unwrap(),
                    "eof" => drop(parent),
                    _ => unreachable!(),
                }
            }
            let result = child.wait_with_output().unwrap();
            assert!(
                result.status.success(),
                "mode={mode}: {}",
                String::from_utf8_lossy(&result.stderr)
            );
            assert_eq!(marker.exists(), mode == "allow", "mode={mode}");
        }
    }
}
