use super::{acl_check, acl_status};
use std::env;
use uf_acl_sys as acl;

pub struct PinnedHostAllocation {
    pub inner: acl::UfAclHostMemory,
}

// The allocation is owned by the daemon and is only shared through
// TransferChannelManager lane leases. The raw pointer refers to CANN pinned
// host memory whose lifetime is controlled by UfAclHostMemory/free_host.
unsafe impl Send for PinnedHostAllocation {}

impl PinnedHostAllocation {
    pub fn ptr(&self) -> *mut acl::CVoid {
        self.inner.host_ptr
    }

    pub fn bytes(&self) -> u64 {
        self.inner.bytes
    }
}

impl Drop for PinnedHostAllocation {
    fn drop(&mut self) {
        if self.inner.host_handle_id == 0 {
            return;
        }
        let mut status = acl_status();
        unsafe {
            let _ = acl::uf_acl_free_host(&mut self.inner, &mut status);
        }
    }
}

pub fn malloc_pinned_host(bytes: u64) -> Result<PinnedHostAllocation, String> {
    let mut status = acl_status();
    let mut host = acl::UfAclHostMemory::default();
    unsafe {
        acl_check(
            acl::uf_acl_malloc_host(bytes, &mut host, &mut status),
            &status,
            "malloc_host",
        )?;
    }
    Ok(PinnedHostAllocation { inner: host })
}

pub struct RegisteredHostAllocation {
    inner: acl::UfAclHostRegisterInfo,
}

unsafe impl Send for RegisteredHostAllocation {}

impl Drop for RegisteredHostAllocation {
    fn drop(&mut self) {
        if self.inner.registered_host_id == 0 && self.inner.host_ptr.is_null() {
            return;
        }
        let mut status = acl_status();
        unsafe {
            let _ = acl::uf_acl_host_unregister(&mut self.inner, &mut status);
        }
    }
}

pub fn register_host(
    device_id: i32,
    host_ptr: *mut acl::CVoid,
    bytes: u64,
    use_v2: bool,
) -> Result<RegisteredHostAllocation, String> {
    let mut status = acl_status();
    let flags = if use_v2 {
        env::var("UF_DDR_REGISTER_V2_FLAGS")
            .ok()
            .and_then(|value| {
                if let Some(hex) = value.strip_prefix("0x") {
                    u32::from_str_radix(hex, 16).ok()
                } else {
                    value.parse::<u32>().ok()
                }
            })
            .unwrap_or(0)
    } else {
        0
    };
    let req = acl::UfAclHostRegisterRequest {
        device_id,
        host_ptr,
        bytes,
        flags,
        use_v2: if use_v2 { 1 } else { 0 },
    };
    let mut info = acl::UfAclHostRegisterInfo::default();
    unsafe {
        acl_check(
            acl::uf_acl_host_register(&req, &mut info, &mut status),
            &status,
            "host_register",
        )?;
    }
    Ok(RegisteredHostAllocation { inner: info })
}
