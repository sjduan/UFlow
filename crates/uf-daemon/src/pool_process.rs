//! Linux process identity support shared by managed pool providers.
use std::fs::File;
use std::os::fd::{AsRawFd, FromRawFd};

pub fn opaque_id() -> Result<String, String> {
    let id = std::fs::read_to_string("/proc/sys/kernel/random/uuid").map_err(|e| e.to_string())?;
    let id = id.trim();
    if id.len() != 36 || !id.bytes().all(|c| c.is_ascii_hexdigit() || c == b'-') {
        return Err("kernel identity unavailable".into());
    }
    Ok(id.to_owned())
}

pub fn pidfd(pid: i32) -> Result<File, String> {
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) } as i32;
    if fd < 0 {
        Err(std::io::Error::last_os_error().to_string())
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}
pub fn exited(file: &File) -> bool {
    let mut poll = libc::pollfd {
        fd: file.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    unsafe { libc::poll(&mut poll, 1, 0) > 0 }
}
