use crate::catalog::{Catalog, Object};
use crate::common::SSD_PLACEMENT;

use std::env;
use std::ffi::CString;
use std::fs::{self, File, OpenOptions};
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::slice;
use std::time::Instant;

#[derive(Clone, Debug)]
pub(crate) struct SsdCreateInfo {
    pub(crate) path: PathBuf,
    pub(crate) requested_bytes: u64,
    pub(crate) actual_bytes: u64,
    pub(crate) alignment_bytes: u64,
    pub(crate) io_mode: String,
    pub(crate) preallocated: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SsdInfo {
    pub(crate) root: PathBuf,
    pub(crate) fs_total: u64,
    pub(crate) fs_free: u64,
    pub(crate) fs_available: u64,
    pub(crate) committed: u64,
    pub(crate) safe_allocatable: u64,
    pub(crate) alignment_bytes: u64,
    pub(crate) io_mode: String,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SsdIoStats {
    pub(crate) submit_us: f64,
    pub(crate) wait_us: f64,
    pub(crate) bytes: u64,
    pub(crate) read_bytes: u64,
    pub(crate) write_bytes: u64,
}

impl SsdIoStats {
    pub(crate) fn merge(&mut self, other: &SsdIoStats) {
        self.submit_us += other.submit_us;
        self.wait_us += other.wait_us;
        self.bytes += other.bytes;
        self.read_bytes += other.read_bytes;
        self.write_bytes += other.write_bytes;
    }

    pub(crate) fn bandwidth_gib_s(&self) -> f64 {
        if self.wait_us <= 0.0 || self.bytes == 0 {
            0.0
        } else {
            (self.bytes as f64) / (self.wait_us / 1_000_000.0) / (1024.0 * 1024.0 * 1024.0)
        }
    }
}

fn env_bool(key: &str, default: bool) -> bool {
    match env::var(key) {
        Ok(value) => matches!(value.as_str(), "1" | "true" | "True" | "yes" | "YES" | "on"),
        Err(_) => default,
    }
}

pub(crate) fn ssd_root() -> PathBuf {
    PathBuf::from(env::var("UF_SSD_ROOT").unwrap_or_else(|_| "/data/uflow_ssd".to_string()))
}

pub(crate) fn ensure_ssd_root() -> Result<PathBuf, String> {
    let root = ssd_root();
    fs::create_dir_all(&root).map_err(|e| format!("create SSD root failed: {}", e))?;
    Ok(root)
}

pub(crate) fn ssd_io_mode() -> String {
    env::var("UF_SSD_IO_MODE").unwrap_or_else(|_| "buffered".to_string())
}

pub(crate) fn ssd_alignment_bytes() -> u64 {
    env::var("UF_SSD_ALIGN_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(4096)
}

pub(crate) fn ssd_chunk_bytes() -> u64 {
    env::var("UF_SSD_CHUNK_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(64 * 1024 * 1024)
}

pub(crate) fn ssd_path(object_id: u64) -> PathBuf {
    ssd_root().join(format!("uflow_ssd_object_{}", object_id))
}

pub(crate) fn align_up(value: u64, alignment: u64) -> u64 {
    if alignment <= 1 {
        return value;
    }
    let rem = value % alignment;
    if rem == 0 {
        value
    } else {
        value.saturating_add(alignment - rem)
    }
}

fn statvfs_bytes(root: &Path) -> (u64, u64, u64) {
    let path = root.to_string_lossy().to_string();
    let c_path = match CString::new(path) {
        Ok(path) => path,
        Err(_) => return (0, 0, 0),
    };
    let mut st = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), st.as_mut_ptr()) };
    if rc != 0 {
        return (0, 0, 0);
    }
    let st = unsafe { st.assume_init() };
    let frsize = if st.f_frsize == 0 {
        st.f_bsize
    } else {
        st.f_frsize
    } as u64;
    (
        st.f_blocks as u64 * frsize,
        st.f_bfree as u64 * frsize,
        st.f_bavail as u64 * frsize,
    )
}

pub(crate) fn ssd_committed_bytes(st: &Catalog) -> u64 {
    st.objects
        .values()
        .filter(|object| object.placement == SSD_PLACEMENT)
        .map(|object| object.actual_bytes)
        .sum()
}

pub(crate) fn ssd_info_with_committed(committed: u64) -> SsdInfo {
    let root = ssd_root();
    let alignment_bytes = ssd_alignment_bytes();
    let io_mode = ssd_io_mode();
    let (fs_total, fs_free, fs_available) = statvfs_bytes(&root);
    SsdInfo {
        root,
        fs_total,
        fs_free,
        fs_available,
        committed,
        safe_allocatable: fs_available,
        alignment_bytes,
        io_mode,
    }
}

pub(crate) fn ssd_info(st: &Catalog) -> SsdInfo {
    ssd_info_with_committed(ssd_committed_bytes(st))
}

pub(crate) fn create_ssd_file(
    object_id: u64,
    requested_bytes: u64,
) -> Result<SsdCreateInfo, String> {
    if requested_bytes == 0 {
        return Err("nbytes is required".to_string());
    }
    let _root = ensure_ssd_root()?;
    let alignment_bytes = ssd_alignment_bytes();
    let actual_bytes = align_up(requested_bytes, alignment_bytes);
    let path = ssd_path(object_id);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|e| format!("create SSD object file failed: {}", e))?;
    if let Err(e) = file.set_len(actual_bytes) {
        let _ = fs::remove_file(&path);
        return Err(format!("resize SSD object file failed: {}", e));
    }
    let mut preallocated = false;
    if env_bool("UF_SSD_PREALLOCATE", true) {
        match preallocate_file(&file, actual_bytes) {
            Ok(()) => preallocated = true,
            Err(e) => {
                let _ = fs::remove_file(&path);
                return Err(e);
            }
        }
    }
    Ok(SsdCreateInfo {
        path,
        requested_bytes,
        actual_bytes,
        alignment_bytes,
        io_mode: ssd_io_mode(),
        preallocated,
    })
}

fn preallocate_file(file: &File, bytes: u64) -> Result<(), String> {
    let rc = unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, bytes as libc::off_t) };
    if rc == 0 {
        return Ok(());
    }
    Err(format!("preallocate SSD object file failed errno={}", rc))
}

pub(crate) fn remove_ssd_file(object: &Object) -> Result<(), String> {
    if !object.ssd_file_owned || object.ssd_path.is_empty() {
        return Ok(());
    }
    fs::remove_file(&object.ssd_path).map_err(|e| format!("remove SSD object file failed: {}", e))
}

fn open_ssd_object(object: &Object, write: bool) -> Result<File, String> {
    if object.ssd_path.is_empty() {
        return Err("SSD object has empty path".to_string());
    }
    let mut options = OpenOptions::new();
    options.read(true).write(write);
    options
        .open(&object.ssd_path)
        .map_err(|e| format!("open SSD object failed: {}", e))
}

pub(crate) fn read_ssd_to_ptr(
    object: &Object,
    object_offset: u64,
    dst: *mut u8,
    bytes: u64,
) -> Result<SsdIoStats, String> {
    if bytes == 0 {
        return Ok(SsdIoStats::default());
    }
    if object_offset.checked_add(bytes).unwrap_or(u64::MAX) > object.actual_bytes {
        return Err("SSD read range out of bounds".to_string());
    }
    let submit_started = Instant::now();
    let file = open_ssd_object(object, false)?;
    let mut stats = SsdIoStats {
        submit_us: submit_started.elapsed().as_secs_f64() * 1_000_000.0,
        bytes,
        read_bytes: bytes,
        ..SsdIoStats::default()
    };
    let wait_started = Instant::now();
    let mut done = 0u64;
    let chunk_bytes = ssd_chunk_bytes();
    while done < bytes {
        let chunk = (bytes - done).min(chunk_bytes);
        let buf = unsafe { slice::from_raw_parts_mut(dst.add(done as usize), chunk as usize) };
        let mut filled = 0usize;
        while filled < buf.len() {
            let n = file
                .read_at(&mut buf[filled..], object_offset + done + filled as u64)
                .map_err(|e| format!("SSD pread failed: {}", e))?;
            if n == 0 {
                return Err("SSD pread reached EOF".to_string());
            }
            filled += n;
        }
        done += chunk;
    }
    stats.wait_us = wait_started.elapsed().as_secs_f64() * 1_000_000.0;
    Ok(stats)
}

pub(crate) fn write_ptr_to_ssd(
    object: &Object,
    object_offset: u64,
    src: *const u8,
    bytes: u64,
) -> Result<SsdIoStats, String> {
    if bytes == 0 {
        return Ok(SsdIoStats::default());
    }
    if object_offset.checked_add(bytes).unwrap_or(u64::MAX) > object.actual_bytes {
        return Err("SSD write range out of bounds".to_string());
    }
    let submit_started = Instant::now();
    let file = open_ssd_object(object, true)?;
    let mut stats = SsdIoStats {
        submit_us: submit_started.elapsed().as_secs_f64() * 1_000_000.0,
        bytes,
        write_bytes: bytes,
        ..SsdIoStats::default()
    };
    let wait_started = Instant::now();
    let mut done = 0u64;
    let chunk_bytes = ssd_chunk_bytes();
    while done < bytes {
        let chunk = (bytes - done).min(chunk_bytes);
        let buf = unsafe { slice::from_raw_parts(src.add(done as usize), chunk as usize) };
        let mut written = 0usize;
        while written < buf.len() {
            let n = file
                .write_at(&buf[written..], object_offset + done + written as u64)
                .map_err(|e| format!("SSD pwrite failed: {}", e))?;
            if n == 0 {
                return Err("SSD pwrite wrote zero bytes".to_string());
            }
            written += n;
        }
        done += chunk;
    }
    stats.wait_us = wait_started.elapsed().as_secs_f64() * 1_000_000.0;
    Ok(stats)
}
