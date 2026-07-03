use super::{acl_check, acl_status};
use uf_acl_sys as acl;

pub fn h2d_async_on_stream(
    device_id: i32,
    device_ptr: u64,
    dst_offset_bytes: u64,
    host_src: *const acl::CVoid,
    bytes: u64,
    stream_id: u64,
    event_id: u64,
) -> Result<(), String> {
    let mut status = acl_status();
    unsafe {
        acl_check(
            acl::uf_acl_h2d_async_on_device(
                device_id,
                device_ptr as *mut acl::CVoid,
                dst_offset_bytes,
                host_src,
                bytes,
                stream_id,
                event_id,
                &mut status,
            ),
            &status,
            "h2d_async_on_stream",
        )
    }
}

pub fn d2h_async_on_stream(
    device_id: i32,
    host_dst: *mut acl::CVoid,
    device_ptr: u64,
    src_offset_bytes: u64,
    bytes: u64,
    stream_id: u64,
    event_id: u64,
) -> Result<(), String> {
    let mut status = acl_status();
    unsafe {
        acl_check(
            acl::uf_acl_d2h_async_on_device(
                device_id,
                host_dst,
                device_ptr as *const acl::CVoid,
                src_offset_bytes,
                bytes,
                stream_id,
                event_id,
                &mut status,
            ),
            &status,
            "d2h_async_on_stream",
        )
    }
}

pub fn h2d_on_device(
    device_id: i32,
    device_ptr: u64,
    dst_offset_bytes: u64,
    host_src: *const acl::CVoid,
    bytes: u64,
    use_async: bool,
) -> Result<(), String> {
    let mut status = acl_status();
    unsafe {
        let rc = if use_async {
            acl::uf_acl_h2d_async_wait_on_device(
                device_id,
                device_ptr as *mut acl::CVoid,
                dst_offset_bytes,
                host_src,
                bytes,
                &mut status,
            )
        } else {
            acl::uf_acl_h2d_on_device(
                device_id,
                device_ptr as *mut acl::CVoid,
                dst_offset_bytes,
                host_src,
                bytes,
                &mut status,
            )
        };
        acl_check(rc, &status, "h2d_on_device")
    }
}

pub fn d2h_on_device(
    device_id: i32,
    host_dst: *mut acl::CVoid,
    device_ptr: u64,
    src_offset_bytes: u64,
    bytes: u64,
    use_async: bool,
) -> Result<(), String> {
    let mut status = acl_status();
    unsafe {
        let rc = if use_async {
            acl::uf_acl_d2h_async_wait_on_device(
                device_id,
                host_dst,
                device_ptr as *const acl::CVoid,
                src_offset_bytes,
                bytes,
                &mut status,
            )
        } else {
            acl::uf_acl_d2h_on_device(
                device_id,
                host_dst,
                device_ptr as *const acl::CVoid,
                src_offset_bytes,
                bytes,
                &mut status,
            )
        };
        acl_check(rc, &status, "d2h_on_device")
    }
}

pub fn d2d_on_devices(
    dst_device_id: i32,
    dst_device_ptr: u64,
    dst_offset_bytes: u64,
    src_device_id: i32,
    src_device_ptr: u64,
    src_offset_bytes: u64,
    bytes: u64,
    use_async: bool,
) -> Result<(), String> {
    let mut status = acl_status();
    unsafe {
        let rc = if use_async {
            acl::uf_acl_d2d_async_wait_on_devices(
                dst_device_id,
                dst_device_ptr as *mut acl::CVoid,
                dst_offset_bytes,
                src_device_id,
                src_device_ptr as *const acl::CVoid,
                src_offset_bytes,
                bytes,
                &mut status,
            )
        } else {
            acl::uf_acl_d2d_on_devices(
                dst_device_id,
                dst_device_ptr as *mut acl::CVoid,
                dst_offset_bytes,
                src_device_id,
                src_device_ptr as *const acl::CVoid,
                src_offset_bytes,
                bytes,
                &mut status,
            )
        };
        acl_check(rc, &status, "d2d_on_devices")
    }
}
