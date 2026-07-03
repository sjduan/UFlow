use super::{acl_check, acl_status};
use uf_acl_sys as acl;

pub struct AclStream {
    id: u64,
}

// The stream is owned by one daemon transfer lane. The numeric handle is only
// used through ACL shim calls while the lane lease is active.
unsafe impl Send for AclStream {}

impl AclStream {
    pub fn id(&self) -> u64 {
        self.id
    }
}

impl Drop for AclStream {
    fn drop(&mut self) {
        if self.id == 0 {
            return;
        }
        let mut status = acl_status();
        unsafe {
            let _ = acl::uf_acl_destroy_stream(self.id, &mut status);
        }
        self.id = 0;
    }
}

pub struct AclEvent {
    id: u64,
}

unsafe impl Send for AclEvent {}

impl AclEvent {
    pub fn id(&self) -> u64 {
        self.id
    }
}

impl Drop for AclEvent {
    fn drop(&mut self) {
        if self.id == 0 {
            return;
        }
        let mut status = acl_status();
        unsafe {
            let _ = acl::uf_acl_destroy_event(self.id, &mut status);
        }
        self.id = 0;
    }
}

pub fn create_stream() -> Result<AclStream, String> {
    let mut status = acl_status();
    let mut id = 0u64;
    unsafe {
        acl_check(
            acl::uf_acl_create_stream(&mut id, &mut status),
            &status,
            "create_stream",
        )?;
    }
    Ok(AclStream { id })
}

pub fn create_event() -> Result<AclEvent, String> {
    let mut status = acl_status();
    let mut id = 0u64;
    unsafe {
        acl_check(
            acl::uf_acl_create_event(&mut id, &mut status),
            &status,
            "create_event",
        )?;
    }
    Ok(AclEvent { id })
}

pub fn synchronize_event(event_id: u64) -> Result<(), String> {
    let mut status = acl_status();
    unsafe {
        acl_check(
            acl::uf_acl_synchronize_event(event_id, &mut status),
            &status,
            "synchronize_event",
        )
    }
}
