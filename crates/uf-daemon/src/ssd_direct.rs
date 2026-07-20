use crate::acl_backend::{d2h_on_device, h2d_on_device, register_host, RegisteredHostAllocation};
use crate::catalog::Object;
use crate::trace::{span, TraceCategory};

use std::env;
use std::fs::OpenOptions;
use std::os::fd::AsRawFd;
use std::ptr;
use std::time::Instant;

const DEFAULT_CANDIDATE: &str = "file_mmap_acl_direct";
const MIB: u64 = 1024 * 1024;
const MADV_POPULATE_READ: i32 = 22;
const MADV_POPULATE_WRITE: i32 = 23;

const IMPLEMENTED_CANDIDATES: &[&str] = &[
    "file_mmap_acl_direct",
    "file_mmap_thp_pretouch_acl_direct",
    "file_mmap_populate_acl_direct",
    "file_mmap_fadvise_acl_direct",
    "file_mmap_madvise_populate_acl_direct",
    "file_mmap_readahead_acl_direct",
    "file_mmap_readahead_populate_acl_direct",
    "file_mmap_readahead_madvise_populate_acl_direct",
    "file_mmap_readahead_madvise_willneed_acl_direct",
    "file_mmap_fadvise_readahead_acl_direct",
    "file_mmap_readahead_pretouch_acl_direct",
    "file_mmap_fadvise_readahead_pretouch_acl_direct",
    "file_mmap_fadvise_populate_acl_direct",
    "file_mmap_fadvise_thp_pretouch_acl_direct",
    "file_mmap_madvise_willneed_acl_direct",
    "file_mmap_mlock_acl_direct",
    "file_mmap_hostregister_v1",
    "file_mmap_hostregister_v2",
    "file_mmap_thp_pretouch_hostregister_v1",
    "file_mmap_thp_pretouch_hostregister_v2",
    "file_mmap_fadvise_hostregister_v2",
    "file_mmap_fadvise_populate_hostregister_v2",
    "file_mmap_fadvise_thp_pretouch_hostregister_v2",
];

const PLANNED_CANDIDATES: &[&str] = &[
    "odirect_aligned_staging_acl",
    "io_uring_pipeline",
    "nvme_p2p_driver_ioctl_probe",
    "aicpu_device_fs_probe",
    "ssu_lba_descriptor_mock",
];

#[derive(Clone, Debug, Default)]
pub(crate) struct SsdHbmDirectStats {
    pub(crate) candidate_name: String,
    pub(crate) direct_kind: String,
    pub(crate) setup_us: f64,
    pub(crate) register_us: f64,
    pub(crate) fadvise_us: f64,
    pub(crate) readahead_us: f64,
    pub(crate) madvise_hugepage_us: f64,
    pub(crate) madvise_willneed_us: f64,
    pub(crate) madvise_populate_us: f64,
    pub(crate) pretouch_us: f64,
    pub(crate) mlock_us: f64,
    pub(crate) acl_us: f64,
    pub(crate) total_us: f64,
    pub(crate) bytes: u64,
    pub(crate) read_bytes: u64,
    pub(crate) write_bytes: u64,
}

#[derive(Clone, Debug)]
struct Candidate {
    name: String,
    use_thp: bool,
    use_pretouch: bool,
    use_populate: bool,
    use_fadvise: bool,
    use_readahead: bool,
    use_madvise_populate: bool,
    use_madvise_willneed: bool,
    use_mlock: bool,
    register: bool,
    register_v2: bool,
}

struct MappedSsd {
    ptr: *mut u8,
    len: u64,
    mlock_ptr: *mut u8,
    mlock_len: u64,
}

impl Drop for MappedSsd {
    fn drop(&mut self) {
        if !self.mlock_ptr.is_null() && self.mlock_len > 0 {
            unsafe {
                let _ = libc::munlock(self.mlock_ptr as *mut libc::c_void, self.mlock_len as usize);
            }
        }
        if !self.ptr.is_null() && self.len > 0 {
            unsafe {
                let _ = libc::munmap(self.ptr as *mut libc::c_void, self.len as usize);
            }
        }
    }
}

pub(crate) fn direct_enabled() -> bool {
    env_bool("UF_SSD_DIRECT_ENABLE", true)
}

pub(crate) fn direct_auto_enabled() -> bool {
    env_bool("UF_SSD_DIRECT_AUTO", true)
}

pub(crate) fn configured_candidate_name() -> String {
    env::var("UF_SSD_HBM_DIRECT_CANDIDATE").unwrap_or_else(|_| DEFAULT_CANDIDATE.to_string())
}

pub(crate) fn implemented_candidates_csv() -> String {
    IMPLEMENTED_CANDIDATES.join(",")
}

pub(crate) fn planned_candidates_csv() -> String {
    PLANNED_CANDIDATES.join(",")
}

pub(crate) fn configured_candidate() -> Result<String, String> {
    if !direct_enabled() {
        return Err("ssd_hbm_direct_disabled_set_UF_SSD_DIRECT_ENABLE=1".to_string());
    }
    let name = configured_candidate_name();
    if !is_implemented_candidate(&name) {
        return Err(format!("ssd_hbm_direct_candidate_not_implemented:{}", name));
    }
    Ok(name)
}

pub(crate) fn auto_candidate(_ssd_to_hbm: bool, nbytes: u64) -> Result<Option<String>, String> {
    if !direct_enabled() || !direct_auto_enabled() {
        return Ok(None);
    }
    if env::var("UF_SSD_HBM_DIRECT_CANDIDATE").is_ok() {
        return configured_candidate().map(Some);
    }
    if nbytes < direct_min_bytes() {
        return Ok(None);
    }
    let candidate = "file_mmap_acl_direct";
    Ok(Some(candidate.to_string()))
}

pub(crate) fn candidate_explanation(name: &str) -> String {
    if is_implemented_candidate(name) {
        format!("logical direct candidate {} maps SSD file extent and copies directly between mmap VA and service-owned HBM VA", name)
    } else if PLANNED_CANDIDATES.contains(&name) {
        format!(
            "candidate {} is planned/probed only in PhaseB-04 and has no daemon data path yet",
            name
        )
    } else {
        format!("unknown SSD-HBM direct candidate {}", name)
    }
}

pub(crate) fn copy_ssd_to_hbm_direct(
    event_id: u64,
    src: &Object,
    device_id: i32,
    dst_device_ptr: u64,
    src_offset: u64,
    dst_offset: u64,
    nbytes: u64,
    candidate_name: &str,
) -> Result<SsdHbmDirectStats, String> {
    let candidate = parse_candidate(candidate_name)?;
    let total_started = Instant::now();
    let (mapping, _registered, mut stats) = map_and_prepare(
        event_id, src, device_id, src_offset, nbytes, false, &candidate,
    )?;
    let acl_started = Instant::now();
    {
        let _trace = span(
            TraceCategory::Acl,
            "ssd_hbm.direct_h2d_acl_wait",
            vec![
                ("event_id", event_id.to_string()),
                ("candidate", candidate.name.clone()),
                ("device_id", device_id.to_string()),
                ("src_offset", src_offset.to_string()),
                ("dst_offset", dst_offset.to_string()),
                ("bytes", nbytes.to_string()),
            ],
        );
        unsafe {
            h2d_on_device(
                device_id,
                dst_device_ptr,
                dst_offset,
                mapping.ptr.add(src_offset as usize) as *const _,
                nbytes,
                true,
            )?;
        }
    }
    stats.acl_us = acl_started.elapsed().as_secs_f64() * 1_000_000.0;
    stats.total_us = total_started.elapsed().as_secs_f64() * 1_000_000.0;
    stats.bytes = nbytes;
    stats.read_bytes = nbytes;
    Ok(stats)
}

pub(crate) fn copy_hbm_to_ssd_direct(
    event_id: u64,
    src_device_id: i32,
    src_device_ptr: u64,
    dst: &Object,
    src_offset: u64,
    dst_offset: u64,
    nbytes: u64,
    candidate_name: &str,
) -> Result<SsdHbmDirectStats, String> {
    let candidate = parse_candidate(candidate_name)?;
    let total_started = Instant::now();
    let (mapping, _registered, mut stats) = map_and_prepare(
        event_id,
        dst,
        src_device_id,
        dst_offset,
        nbytes,
        true,
        &candidate,
    )?;
    let acl_started = Instant::now();
    {
        let _trace = span(
            TraceCategory::Acl,
            "ssd_hbm.direct_d2h_acl_wait",
            vec![
                ("event_id", event_id.to_string()),
                ("candidate", candidate.name.clone()),
                ("device_id", src_device_id.to_string()),
                ("src_offset", src_offset.to_string()),
                ("dst_offset", dst_offset.to_string()),
                ("bytes", nbytes.to_string()),
            ],
        );
        unsafe {
            d2h_on_device(
                src_device_id,
                mapping.ptr.add(dst_offset as usize) as *mut _,
                src_device_ptr,
                src_offset,
                nbytes,
                true,
            )?;
        }
    }
    stats.acl_us = acl_started.elapsed().as_secs_f64() * 1_000_000.0;
    stats.total_us = total_started.elapsed().as_secs_f64() * 1_000_000.0;
    stats.bytes = nbytes;
    stats.write_bytes = nbytes;
    Ok(stats)
}

fn map_and_prepare(
    event_id: u64,
    object: &Object,
    device_id: i32,
    object_offset: u64,
    nbytes: u64,
    write: bool,
    candidate: &Candidate,
) -> Result<
    (
        MappedSsd,
        Option<RegisteredHostAllocation>,
        SsdHbmDirectStats,
    ),
    String,
> {
    if object.ssd_path.is_empty() {
        return Err("SSD object has empty path".to_string());
    }
    if object_offset.checked_add(nbytes).unwrap_or(u64::MAX) > object.actual_bytes {
        return Err("SSD direct range out of bounds".to_string());
    }
    let setup_started = Instant::now();
    let file = OpenOptions::new()
        .read(true)
        .write(write)
        .open(&object.ssd_path)
        .map_err(|e| format!("open SSD direct object failed: {}", e))?;
    let mut stats = SsdHbmDirectStats {
        candidate_name: candidate.name.clone(),
        direct_kind: "logical".to_string(),
        ..SsdHbmDirectStats::default()
    };
    if candidate.use_fadvise {
        let started = Instant::now();
        let _trace = span(
            TraceCategory::Backend,
            "ssd_hbm.direct_posix_fadvise",
            vec![
                ("event_id", event_id.to_string()),
                ("candidate", candidate.name.clone()),
                ("offset", object_offset.to_string()),
                ("bytes", nbytes.to_string()),
            ],
        );
        unsafe {
            let _ = libc::posix_fadvise(
                file.as_raw_fd(),
                object_offset as libc::off_t,
                nbytes as libc::off_t,
                libc::POSIX_FADV_SEQUENTIAL,
            );
            let _ = libc::posix_fadvise(
                file.as_raw_fd(),
                object_offset as libc::off_t,
                nbytes as libc::off_t,
                libc::POSIX_FADV_WILLNEED,
            );
        }
        stats.fadvise_us += started.elapsed().as_secs_f64() * 1_000_000.0;
    }
    if candidate.use_readahead {
        let started = Instant::now();
        let _trace = span(
            TraceCategory::Backend,
            "ssd_hbm.direct_readahead",
            vec![
                ("event_id", event_id.to_string()),
                ("candidate", candidate.name.clone()),
                ("offset", object_offset.to_string()),
                ("bytes", nbytes.to_string()),
            ],
        );
        unsafe {
            let _ = libc::readahead(
                file.as_raw_fd(),
                object_offset as libc::off64_t,
                nbytes as libc::size_t,
            );
        }
        stats.readahead_us += started.elapsed().as_secs_f64() * 1_000_000.0;
    }
    let prot = if write {
        libc::PROT_READ | libc::PROT_WRITE
    } else {
        libc::PROT_READ
    };
    let mut flags = libc::MAP_SHARED;
    if candidate.use_populate {
        flags |= libc::MAP_POPULATE;
    }
    let ptr = unsafe {
        libc::mmap(
            ptr::null_mut(),
            object.actual_bytes as usize,
            prot,
            flags,
            file.as_raw_fd(),
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err(format!(
            "mmap SSD direct object failed errno={}",
            std::io::Error::last_os_error()
        ));
    }
    let mapping = MappedSsd {
        ptr: ptr as *mut u8,
        len: object.actual_bytes,
        mlock_ptr: ptr::null_mut(),
        mlock_len: 0,
    };
    stats.setup_us = setup_started.elapsed().as_secs_f64() * 1_000_000.0;
    if candidate.use_thp {
        let started = Instant::now();
        let _trace = span(
            TraceCategory::Backend,
            "ssd_hbm.direct_madvise_hugepage",
            vec![
                ("event_id", event_id.to_string()),
                ("candidate", candidate.name.clone()),
                ("bytes", object.actual_bytes.to_string()),
            ],
        );
        unsafe {
            let _ = libc::madvise(
                mapping.ptr as *mut libc::c_void,
                object.actual_bytes as usize,
                libc::MADV_HUGEPAGE,
            );
        }
        stats.madvise_hugepage_us += started.elapsed().as_secs_f64() * 1_000_000.0;
    }
    if candidate.use_madvise_willneed {
        let started = Instant::now();
        let _trace = span(
            TraceCategory::Backend,
            "ssd_hbm.direct_madvise_willneed",
            vec![
                ("event_id", event_id.to_string()),
                ("candidate", candidate.name.clone()),
                ("offset", object_offset.to_string()),
                ("bytes", nbytes.to_string()),
            ],
        );
        unsafe {
            let _ = libc::madvise(
                mapping.ptr.add(object_offset as usize) as *mut libc::c_void,
                nbytes as usize,
                libc::MADV_WILLNEED,
            );
        }
        stats.madvise_willneed_us += started.elapsed().as_secs_f64() * 1_000_000.0;
    }
    if candidate.use_madvise_populate {
        let started = Instant::now();
        let advice = if write {
            MADV_POPULATE_WRITE
        } else {
            MADV_POPULATE_READ
        };
        let _trace = span(
            TraceCategory::Backend,
            "ssd_hbm.direct_madvise_populate",
            vec![
                ("event_id", event_id.to_string()),
                ("candidate", candidate.name.clone()),
                ("offset", object_offset.to_string()),
                ("bytes", nbytes.to_string()),
                ("write", write.to_string()),
                ("advice", advice.to_string()),
            ],
        );
        let rc = unsafe {
            libc::madvise(
                mapping.ptr.add(object_offset as usize) as *mut libc::c_void,
                nbytes as usize,
                advice,
            )
        };
        if rc != 0 {
            return Err(format!(
                "madvise_populate SSD direct mmap failed advice={} errno={}",
                advice,
                std::io::Error::last_os_error()
            ));
        }
        stats.madvise_populate_us += started.elapsed().as_secs_f64() * 1_000_000.0;
    }
    if candidate.use_pretouch {
        let pretouch_started = Instant::now();
        {
            let _trace = span(
                TraceCategory::Backend,
                "ssd_hbm.direct_pretouch",
                vec![
                    ("event_id", event_id.to_string()),
                    ("candidate", candidate.name.clone()),
                    ("offset", object_offset.to_string()),
                    ("bytes", nbytes.to_string()),
                ],
            );
            unsafe {
                pretouch(mapping.ptr.add(object_offset as usize), nbytes);
            }
        }
        stats.pretouch_us = pretouch_started.elapsed().as_secs_f64() * 1_000_000.0;
    }
    let mut mapping = mapping;
    if candidate.use_mlock {
        let started = Instant::now();
        let _trace = span(
            TraceCategory::Backend,
            "ssd_hbm.direct_mlock",
            vec![
                ("event_id", event_id.to_string()),
                ("candidate", candidate.name.clone()),
                ("offset", object_offset.to_string()),
                ("bytes", nbytes.to_string()),
            ],
        );
        let lock_ptr = unsafe { mapping.ptr.add(object_offset as usize) };
        let rc = unsafe { libc::mlock(lock_ptr as *mut libc::c_void, nbytes as usize) };
        if rc != 0 {
            return Err(format!(
                "mlock SSD direct mmap failed errno={}",
                std::io::Error::last_os_error()
            ));
        }
        mapping.mlock_ptr = lock_ptr;
        mapping.mlock_len = nbytes;
        stats.mlock_us += started.elapsed().as_secs_f64() * 1_000_000.0;
    }
    if candidate.register {
        let register_started = Instant::now();
        {
            let _trace = span(
                TraceCategory::Backend,
                "ssd_hbm.direct_host_register",
                vec![
                    ("event_id", event_id.to_string()),
                    ("candidate", candidate.name.clone()),
                    ("bytes", object.actual_bytes.to_string()),
                    ("use_v2", candidate.register_v2.to_string()),
                ],
            );
            let registered = register_host(
                device_id,
                mapping.ptr as *mut _,
                object.actual_bytes,
                candidate.register_v2,
            )?;
            stats.register_us = register_started.elapsed().as_secs_f64() * 1_000_000.0;
            return Ok((mapping, Some(registered), stats));
        }
    }
    Ok((mapping, None, stats))
}

fn parse_candidate(name: &str) -> Result<Candidate, String> {
    if !is_implemented_candidate(name) {
        return Err(format!(
            "SSD-HBM direct candidate is not implemented: {}",
            name
        ));
    }
    Ok(Candidate {
        name: name.to_string(),
        use_thp: name.contains("thp"),
        use_pretouch: name.contains("pretouch"),
        use_populate: name.contains("_populate_") && !name.contains("madvise_populate"),
        use_fadvise: name.contains("fadvise"),
        use_readahead: name.contains("readahead"),
        use_madvise_populate: name.contains("madvise_populate"),
        use_madvise_willneed: name.contains("madvise_willneed"),
        use_mlock: name.contains("mlock"),
        register: name.contains("hostregister"),
        register_v2: name.ends_with("_v2"),
    })
}

fn is_implemented_candidate(name: &str) -> bool {
    IMPLEMENTED_CANDIDATES.contains(&name)
}

unsafe fn pretouch(ptr: *mut u8, bytes: u64) {
    let mut offset = 0usize;
    let len = bytes as usize;
    while offset < len {
        ptr.add(offset).read_volatile();
        offset = offset.saturating_add(4096);
    }
    if len > 0 {
        ptr.add(len - 1).read_volatile();
    }
}

fn env_bool(key: &str, default: bool) -> bool {
    match env::var(key) {
        Ok(value) => matches!(value.as_str(), "1" | "true" | "True" | "yes" | "YES" | "on"),
        Err(_) => default,
    }
}

fn direct_min_bytes() -> u64 {
    env::var("UF_SSD_HBM_DIRECT_MIN_BYTES")
        .ok()
        .and_then(|value| parse_size_bytes(&value))
        .unwrap_or(256 * MIB)
}

fn parse_size_bytes(value: &str) -> Option<u64> {
    let text = value.trim();
    if text.is_empty() {
        return None;
    }
    let lower = text.to_ascii_lowercase();
    for (suffix, scale) in [
        ("gib", 1024_u64 * MIB),
        ("gb", 1024_u64 * MIB),
        ("mib", MIB),
        ("mb", MIB),
        ("kib", 1024_u64),
        ("kb", 1024_u64),
    ] {
        if let Some(number) = lower.strip_suffix(suffix) {
            return number.trim().parse::<u64>().ok().map(|n| n * scale);
        }
    }
    lower.parse::<u64>().ok()
}
