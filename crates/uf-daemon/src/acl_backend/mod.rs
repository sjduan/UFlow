use uf_acl_sys as acl;

fn acl_status() -> acl::UfAclStatus {
    acl::UfAclStatus::default()
}

fn acl_check(rc: i32, st: &acl::UfAclStatus, what: &str) -> Result<(), String> {
    acl::check(rc, st, what)
}

mod device_copy;
mod hbm_memory;
mod host_memory;
mod runtime;

pub(crate) use self::device_copy::{
    d2d_on_devices, d2h_async_on_stream, d2h_on_device, h2d_async_on_stream, h2d_on_device,
};
pub(crate) use self::hbm_memory::{
    HbmAllocation, HbmBackend, HbmExport, HbmMemInfo, NpuHbmAclBackend,
};
pub(crate) use self::host_memory::{malloc_pinned_host, register_host, PinnedHostAllocation};
pub(crate) use self::runtime::{
    create_event, create_stream, synchronize_event, AclEvent, AclStream,
};
