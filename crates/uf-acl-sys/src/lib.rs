use std::os::raw::{c_char, c_int, c_void};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct UfAclStatus {
    pub code: i32,
    pub message: [c_char; 256],
}

impl Default for UfAclStatus {
    fn default() -> Self {
        Self {
            code: 0,
            message: [0; 256],
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct UfAclInitOptions {
    pub device_id: i32,
    pub flags: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct UfAclMemInfo {
    pub free_bytes: u64,
    pub total_bytes: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct UfAclHbmAllocRequest {
    pub device_id: i32,
    pub requested_bytes: u64,
    pub alignment: u64,
    pub memory_type: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct UfAclHbmBlock {
    pub raw_handle_id: u64,
    pub shareable_handle_payload: [u64; 8],
    pub shareable_handle_bytes: u64,
    pub requested_bytes: u64,
    pub actual_bytes: u64,
    pub granularity: u64,
    pub service_mapping_id: u64,
    pub service_device_ptr: *mut c_void,
    pub device_id: i32,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct UfAclHostMemory {
    pub host_handle_id: u64,
    pub host_ptr: *mut c_void,
    pub bytes: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct UfAclHostRegisterRequest {
    pub device_id: i32,
    pub host_ptr: *mut c_void,
    pub bytes: u64,
    pub flags: u32,
    pub use_v2: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct UfAclHostRegisterInfo {
    pub registered_host_id: u64,
    pub host_ptr: *mut c_void,
    pub device_ptr: *mut c_void,
    pub bytes: u64,
    pub device_id: i32,
    pub use_v2: u32,
}

extern "C" {
    pub fn uf_acl_backend_init(options: *const UfAclInitOptions, status: *mut UfAclStatus)
        -> c_int;
    pub fn uf_acl_backend_finalize(status: *mut UfAclStatus) -> c_int;
    pub fn uf_acl_get_mem_info(
        device_id: i32,
        out: *mut UfAclMemInfo,
        status: *mut UfAclStatus,
    ) -> c_int;
    pub fn uf_acl_get_allocation_granularity(
        device_id: i32,
        out: *mut u64,
        status: *mut UfAclStatus,
    ) -> c_int;
    pub fn uf_acl_alloc_physical(
        req: *const UfAclHbmAllocRequest,
        out: *mut UfAclHbmBlock,
        status: *mut UfAclStatus,
    ) -> c_int;
    pub fn uf_acl_export_shareable(
        raw_handle_id: u64,
        inout: *mut UfAclHbmBlock,
        status: *mut UfAclStatus,
    ) -> c_int;
    pub fn uf_acl_set_pid_access(
        raw_handle_id: u64,
        bare_tgid: i64,
        status: *mut UfAclStatus,
    ) -> c_int;
    pub fn uf_acl_free_physical(raw_handle_id: u64, status: *mut UfAclStatus) -> c_int;
    pub fn uf_acl_h2d_on_device(
        device_id: i32,
        device_ptr: *mut c_void,
        dst_offset_bytes: u64,
        host_src: *const c_void,
        bytes: u64,
        status: *mut UfAclStatus,
    ) -> c_int;
    pub fn uf_acl_d2h_on_device(
        device_id: i32,
        host_dst: *mut c_void,
        device_ptr: *const c_void,
        src_offset_bytes: u64,
        bytes: u64,
        status: *mut UfAclStatus,
    ) -> c_int;
    pub fn uf_acl_d2d_on_devices(
        dst_device_id: i32,
        dst_device_ptr: *mut c_void,
        dst_offset_bytes: u64,
        src_device_id: i32,
        src_device_ptr: *const c_void,
        src_offset_bytes: u64,
        bytes: u64,
        status: *mut UfAclStatus,
    ) -> c_int;
    pub fn uf_acl_h2d_async_wait_on_device(
        device_id: i32,
        device_ptr: *mut c_void,
        dst_offset_bytes: u64,
        host_src: *const c_void,
        bytes: u64,
        status: *mut UfAclStatus,
    ) -> c_int;
    pub fn uf_acl_d2h_async_wait_on_device(
        device_id: i32,
        host_dst: *mut c_void,
        device_ptr: *const c_void,
        src_offset_bytes: u64,
        bytes: u64,
        status: *mut UfAclStatus,
    ) -> c_int;
    pub fn uf_acl_d2d_async_wait_on_devices(
        dst_device_id: i32,
        dst_device_ptr: *mut c_void,
        dst_offset_bytes: u64,
        src_device_id: i32,
        src_device_ptr: *const c_void,
        src_offset_bytes: u64,
        bytes: u64,
        status: *mut UfAclStatus,
    ) -> c_int;
    pub fn uf_acl_malloc_host(
        bytes: u64,
        out: *mut UfAclHostMemory,
        status: *mut UfAclStatus,
    ) -> c_int;
    pub fn uf_acl_free_host(host: *mut UfAclHostMemory, status: *mut UfAclStatus) -> c_int;
    pub fn uf_acl_host_register(
        req: *const UfAclHostRegisterRequest,
        out: *mut UfAclHostRegisterInfo,
        status: *mut UfAclStatus,
    ) -> c_int;
    pub fn uf_acl_host_unregister(
        info: *mut UfAclHostRegisterInfo,
        status: *mut UfAclStatus,
    ) -> c_int;
    pub fn uf_acl_create_stream(out_stream_id: *mut u64, status: *mut UfAclStatus) -> c_int;
    pub fn uf_acl_destroy_stream(stream_id: u64, status: *mut UfAclStatus) -> c_int;
    pub fn uf_acl_create_event(out_event_id: *mut u64, status: *mut UfAclStatus) -> c_int;
    pub fn uf_acl_destroy_event(event_id: u64, status: *mut UfAclStatus) -> c_int;
    pub fn uf_acl_synchronize_event(event_id: u64, status: *mut UfAclStatus) -> c_int;
    pub fn uf_acl_h2d_async_on_device(
        device_id: i32,
        device_ptr: *mut c_void,
        dst_offset_bytes: u64,
        host_src: *const c_void,
        bytes: u64,
        stream_id: u64,
        event_id: u64,
        status: *mut UfAclStatus,
    ) -> c_int;
    pub fn uf_acl_d2h_async_on_device(
        device_id: i32,
        host_dst: *mut c_void,
        device_ptr: *const c_void,
        src_offset_bytes: u64,
        bytes: u64,
        stream_id: u64,
        event_id: u64,
        status: *mut UfAclStatus,
    ) -> c_int;
}

pub fn status_message(status: &UfAclStatus) -> String {
    let bytes = status
        .message
        .iter()
        .map(|c| *c as u8)
        .take_while(|b| *b != 0)
        .collect::<Vec<_>>();
    String::from_utf8_lossy(&bytes).into_owned()
}

pub fn check(rc: c_int, status: &UfAclStatus, what: &str) -> Result<(), String> {
    if rc == 0 {
        Ok(())
    } else {
        Err(format!("{} failed: {}", what, status_message(status)))
    }
}

#[allow(dead_code)]
pub type CVoid = c_void;
