use crate::catalog::{Catalog, Object};
use crate::common::DDR_PLACEMENT;

use std::env;
use std::ffi::CString;
use std::fs::{self, File, OpenOptions};
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::path::PathBuf;
use std::ptr;
use std::time::Instant;

const MADV_HUGEPAGE: i32 = 14;

pub(crate) fn ddr_root_for_node(node: u32) -> PathBuf {
    let node_key = format!("UF_DDR_ROOT_NODE{}", node);
    if let Ok(root) = env::var(&node_key) {
        return PathBuf::from(root);
    }
    PathBuf::from(env::var("UF_DDR_ROOT").unwrap_or_else(|_| "/dev/shm".to_string()))
}

pub(crate) fn ddr_path(object_id: u64, node: u32) -> PathBuf {
    ddr_root_for_node(node).join(format!("uflow_ddr_object_{}", object_id))
}

#[derive(Clone, Debug, Default)]
pub(crate) struct DdrPrepareInfo {
    pub(crate) fast_profile: String,
    pub(crate) service_ptr: u64,
    pub(crate) service_len: u64,
    pub(crate) madvise_hugepage: bool,
    pub(crate) pretouched: bool,
    pub(crate) madvise_us: f64,
    pub(crate) pretouch_us: f64,
    pub(crate) total_us: f64,
    pub(crate) fallback_reason: String,
}

fn env_bool(key: &str, default: bool) -> bool {
    match env::var(key) {
        Ok(value) => matches!(value.as_str(), "1" | "true" | "True" | "yes" | "YES" | "on"),
        Err(_) => default,
    }
}

pub(crate) fn ddr_use_memfd() -> bool {
    env_bool("UF_DDR_USE_MEMFD", true)
}

pub(crate) fn create_memfd_file(name: &str, len: u64) -> Result<File, String> {
    let c_name = CString::new(name).map_err(|_| "memfd name contains nul byte".to_string())?;
    let fd = unsafe { libc::syscall(libc::SYS_memfd_create, c_name.as_ptr(), libc::MFD_CLOEXEC) };
    if fd < 0 {
        return Err(format!(
            "memfd_create failed errno={}",
            std::io::Error::last_os_error()
                .raw_os_error()
                .unwrap_or_default()
        ));
    }
    let file = unsafe { File::from_raw_fd(fd as i32) };
    if let Err(e) = file.set_len(len) {
        return Err(format!("resize memfd DDR object failed: {}", e));
    }
    Ok(file)
}

pub(crate) fn proc_fd_path(fd: i32) -> String {
    format!("/proc/{}/fd/{}", std::process::id(), fd)
}

pub(crate) fn close_ddr_fd(fd: i32) -> Result<(), String> {
    if fd < 0 {
        return Ok(());
    }
    let rc = unsafe { libc::close(fd) };
    if rc != 0 {
        return Err(format!(
            "close DDR memfd failed errno={}",
            std::io::Error::last_os_error()
                .raw_os_error()
                .unwrap_or_default()
        ));
    }
    Ok(())
}

pub(crate) fn prepare_ddr_file(file: &File, len: u64) -> Result<DdrPrepareInfo, String> {
    let total_start = Instant::now();
    let use_hugepage = env_bool("UF_DDR_MADVISE_HUGEPAGE", true);
    let use_pretouch = env_bool("UF_DDR_PRETOUCH_ON_CREATE", true);
    let mut info = DdrPrepareInfo {
        fast_profile: if use_hugepage || use_pretouch {
            "preparing".to_string()
        } else {
            "mmap_lazy".to_string()
        },
        ..DdrPrepareInfo::default()
    };
    if len == 0 {
        info.fast_profile = "empty".to_string();
        return Ok(info);
    }
    if !use_hugepage && !use_pretouch {
        info.total_us = total_start.elapsed().as_secs_f64() * 1_000_000.0;
        return Ok(info);
    }
    let ptr = unsafe {
        libc::mmap(
            ptr::null_mut(),
            len as usize,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err("mmap DDR object for prepare failed".to_string());
    }
    info.service_ptr = ptr as u64;
    info.service_len = len;

    if use_hugepage {
        let madvise_start = Instant::now();
        let rc = unsafe { libc::madvise(ptr, len as usize, MADV_HUGEPAGE) };
        info.madvise_us = madvise_start.elapsed().as_secs_f64() * 1_000_000.0;
        if rc == 0 {
            info.madvise_hugepage = true;
        } else {
            let errno = std::io::Error::last_os_error()
                .raw_os_error()
                .unwrap_or_default();
            info.fallback_reason = format!("madvise_hugepage_failed_errno_{}", errno);
        }
    }

    if use_pretouch {
        let pretouch_start = Instant::now();
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        let page_size = if page_size > 0 {
            page_size as usize
        } else {
            4096
        };
        let base = ptr as *mut u8;
        let mut offset = 0usize;
        while offset < len as usize {
            unsafe {
                base.add(offset).write_volatile(0);
            }
            offset = offset.saturating_add(page_size);
        }
        unsafe {
            base.add((len - 1) as usize).write_volatile(0);
        }
        info.pretouched = true;
        info.pretouch_us = pretouch_start.elapsed().as_secs_f64() * 1_000_000.0;
    }

    info.total_us = total_start.elapsed().as_secs_f64() * 1_000_000.0;
    info.fast_profile = match (info.madvise_hugepage, info.pretouched) {
        (true, true) => "thp_pretouched",
        (true, false) => "thp_lazy",
        (false, true) => "pretouched",
        (false, false) => "mmap_lazy",
    }
    .to_string();
    Ok(info)
}

pub(crate) fn unmap_ddr_service_mapping(ptr: u64, len: u64) -> Result<(), String> {
    if ptr == 0 || len == 0 {
        return Ok(());
    }
    let rc = unsafe { libc::munmap(ptr as *mut libc::c_void, len as usize) };
    if rc != 0 {
        return Err(format!(
            "munmap DDR service mapping failed errno={}",
            std::io::Error::last_os_error()
                .raw_os_error()
                .unwrap_or_default()
        ));
    }
    Ok(())
}

fn read_u64_file(path: &str) -> Option<u64> {
    let text = fs::read_to_string(path).ok()?;
    let trimmed = text.trim();
    if trimmed == "max" {
        return Some(u64::MAX);
    }
    trimmed.parse::<u64>().ok()
}

fn cgroup_memory() -> (u64, u64) {
    let limit = read_u64_file("/sys/fs/cgroup/memory.max")
        .or_else(|| read_u64_file("/sys/fs/cgroup/memory/memory.limit_in_bytes"))
        .unwrap_or(u64::MAX);
    let current = read_u64_file("/sys/fs/cgroup/memory.current")
        .or_else(|| read_u64_file("/sys/fs/cgroup/memory/memory.usage_in_bytes"))
        .unwrap_or(0);
    (limit, current)
}

fn statvfs_bytes(root: &PathBuf) -> (u64, u64, u64) {
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

fn numa_mem_free_bytes(node: u32) -> u64 {
    let path = format!("/sys/devices/system/node/node{}/meminfo", node);
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(_) => return 0,
    };
    for line in text.lines() {
        if line.contains("MemFree:") {
            let parts = line.split_whitespace().collect::<Vec<_>>();
            for window in parts.windows(2) {
                if window[1] == "kB" {
                    if let Ok(kb) = window[0].parse::<u64>() {
                        return kb * 1024;
                    }
                }
            }
        }
    }
    0
}

pub(crate) fn ddr_committed_bytes(st: &Catalog, node: Option<u32>) -> u64 {
    st.objects
        .values()
        .filter(|object| object.placement == DDR_PLACEMENT)
        .filter(|object| {
            if let Some(node) = node {
                object.target == format!("host:{}", node)
            } else {
                true
            }
        })
        .map(|object| object.actual_bytes)
        .sum()
}

#[derive(Default)]
pub(crate) struct DdrInfo {
    pub(crate) node: u32,
    pub(crate) root: PathBuf,
    pub(crate) fs_total: u64,
    pub(crate) fs_free: u64,
    pub(crate) fs_available: u64,
    pub(crate) cgroup_limit: u64,
    pub(crate) cgroup_current: u64,
    pub(crate) numa_free: u64,
    pub(crate) committed: u64,
    pub(crate) safe_allocatable: u64,
}

pub(crate) fn ddr_info_with_committed(node: u32, committed: u64) -> DdrInfo {
    let root = ddr_root_for_node(node);
    let (fs_total, fs_free, fs_available) = statvfs_bytes(&root);
    let (cgroup_limit, cgroup_current) = cgroup_memory();
    let cgroup_remaining = if cgroup_limit == u64::MAX {
        u64::MAX
    } else {
        cgroup_limit.saturating_sub(cgroup_current)
    };
    let safety = env::var("UF_DDR_SAFETY_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(64 * 1024 * 1024);
    let raw_safe = fs_available.min(cgroup_remaining);
    DdrInfo {
        node,
        root,
        fs_total,
        fs_free,
        fs_available,
        cgroup_limit,
        cgroup_current,
        numa_free: numa_mem_free_bytes(node),
        committed,
        safe_allocatable: raw_safe.saturating_sub(safety),
    }
}

pub(crate) fn ddr_info(st: &Catalog, node: u32) -> DdrInfo {
    ddr_info_with_committed(node, ddr_committed_bytes(st, Some(node)))
}

pub(crate) fn with_ddr_mapping<T>(
    object: &Object,
    f: impl FnOnce(*mut u8, usize) -> Result<T, String>,
) -> Result<T, String> {
    if object.ddr_service_ptr != 0 && object.ddr_service_len > 0 {
        return f(
            object.ddr_service_ptr as *mut u8,
            object.ddr_service_len as usize,
        );
    }
    if object.ddr_path.is_empty() {
        return Err("DDR object has empty path".to_string());
    }
    let len = object.actual_bytes as usize;
    if len == 0 {
        return Err("DDR object size is zero".to_string());
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&object.ddr_path)
        .map_err(|e| format!("open DDR object failed: {}", e))?;
    let ptr = unsafe {
        libc::mmap(
            ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err("mmap DDR object failed".to_string());
    }
    let result = f(ptr as *mut u8, len);
    unsafe {
        libc::munmap(ptr, len);
    }
    result
}
