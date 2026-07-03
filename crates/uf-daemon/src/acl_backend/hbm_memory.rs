use super::{acl_check, acl_status};
use uf_acl_sys as acl;

#[derive(Clone, Copy, Debug, Default)]
pub struct HbmMemInfo {
    pub free_bytes: u64,
    pub total_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct HbmAllocation {
    pub raw_handle_id: u64,
    pub shareable: u64,
    pub requested_bytes: u64,
    pub actual_bytes: u64,
    pub granularity: u64,
    pub service_mapping_id: u64,
    pub service_device_ptr: u64,
    pub device_id: i32,
}

#[derive(Clone, Copy, Debug)]
pub struct HbmExport {
    pub shareable: u64,
    pub actual_bytes: u64,
    pub device_id: i32,
}

pub trait HbmBackend {
    fn init(&mut self, device_id: i32) -> Result<(), String>;
    fn mem_info(&self, device_id: i32) -> Result<HbmMemInfo, String>;
    fn allocation_granularity(&self, device_id: i32) -> Result<u64, String>;
    fn allocate(&mut self, device_id: i32, requested_bytes: u64) -> Result<HbmAllocation, String>;
    fn export_for_client(
        &mut self,
        allocation: &HbmAllocation,
        bare_tgid: i64,
    ) -> Result<HbmExport, String>;
    fn free(&mut self, allocation: HbmAllocation) -> Result<(), String>;
    fn finalize(&mut self) -> Result<(), String>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NpuHbmAclBackend;

impl HbmBackend for NpuHbmAclBackend {
    fn init(&mut self, device_id: i32) -> Result<(), String> {
        let mut status = acl_status();
        let opts = acl::UfAclInitOptions {
            device_id,
            flags: 0,
        };
        unsafe {
            acl_check(
                acl::uf_acl_backend_init(&opts, &mut status),
                &status,
                "backend_init",
            )
        }
    }

    fn mem_info(&self, device_id: i32) -> Result<HbmMemInfo, String> {
        let mut status = acl_status();
        let mut mem = acl::UfAclMemInfo::default();
        unsafe {
            acl_check(
                acl::uf_acl_get_mem_info(device_id, &mut mem, &mut status),
                &status,
                "get_mem_info",
            )?;
        }
        Ok(HbmMemInfo {
            free_bytes: mem.free_bytes,
            total_bytes: mem.total_bytes,
        })
    }

    fn allocation_granularity(&self, device_id: i32) -> Result<u64, String> {
        let mut status = acl_status();
        let mut granularity = 0u64;
        unsafe {
            acl_check(
                acl::uf_acl_get_allocation_granularity(device_id, &mut granularity, &mut status),
                &status,
                "get_granularity",
            )?;
        }
        Ok(granularity)
    }

    fn allocate(&mut self, device_id: i32, requested_bytes: u64) -> Result<HbmAllocation, String> {
        let mut status = acl_status();
        let req = acl::UfAclHbmAllocRequest {
            device_id,
            requested_bytes,
            alignment: 0,
            memory_type: 0,
        };
        let mut block = acl::UfAclHbmBlock::default();
        unsafe {
            acl_check(
                acl::uf_acl_alloc_physical(&req, &mut block, &mut status),
                &status,
                "alloc_physical",
            )?;
            acl_check(
                acl::uf_acl_export_shareable(block.raw_handle_id, &mut block, &mut status),
                &status,
                "export_shareable",
            )?;
        }
        Ok(HbmAllocation {
            raw_handle_id: block.raw_handle_id,
            shareable: block.shareable_handle_payload[0],
            requested_bytes: block.requested_bytes,
            actual_bytes: block.actual_bytes,
            granularity: block.granularity,
            service_mapping_id: block.service_mapping_id,
            service_device_ptr: block.service_device_ptr as u64,
            device_id: block.device_id,
        })
    }

    fn export_for_client(
        &mut self,
        allocation: &HbmAllocation,
        bare_tgid: i64,
    ) -> Result<HbmExport, String> {
        let mut status = acl_status();
        unsafe {
            acl_check(
                acl::uf_acl_set_pid_access(allocation.raw_handle_id, bare_tgid, &mut status),
                &status,
                "set_pid_access",
            )?;
        }
        Ok(HbmExport {
            shareable: allocation.shareable,
            actual_bytes: allocation.actual_bytes,
            device_id: allocation.device_id,
        })
    }

    fn free(&mut self, allocation: HbmAllocation) -> Result<(), String> {
        let mut status = acl_status();
        unsafe {
            acl_check(
                acl::uf_acl_free_physical(allocation.raw_handle_id, &mut status),
                &status,
                "free_physical",
            )
        }
    }

    fn finalize(&mut self) -> Result<(), String> {
        let mut status = acl_status();
        unsafe {
            acl_check(
                acl::uf_acl_backend_finalize(&mut status),
                &status,
                "backend_finalize",
            )
        }
    }
}
