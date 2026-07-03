use crate::acl_backend::{HbmBackend, NpuHbmAclBackend};
use crate::catalog::{Block, Client, Lease, Object, SharedCatalog};
use crate::common::{
    address_kind, create_lease_response, get_bool, is_ddr_request, object_by_request,
    parse_ddr_target, requested_bytes, role_from_req, sanitize_kv_value,
    validate_hbm_create_request, validate_target_matches_object, DDR_PLACEMENT, HBM_PLACEMENT,
};
use crate::ddr_backend::{
    close_ddr_fd, create_memfd_file, ddr_info, ddr_path, ddr_root_for_node, ddr_use_memfd,
    prepare_ddr_file, proc_fd_path, unmap_ddr_service_mapping,
};
use crate::trace::{record_duration_us, span, TraceCategory};
use uf_core::{err, get, get_i64, get_u64, ok, Kv};

use std::fs::{self, OpenOptions};
use std::os::fd::{AsRawFd, IntoRawFd};

pub(crate) fn create_data_object(req: &Kv, shared: &SharedCatalog) -> Kv {
    create_data_object_impl(req, shared)
}

fn create_ddr_lease_response(object: &Object, lease: &Lease, existing: bool) -> Kv {
    ok(&[
        ("object_id", object.object_id.to_string()),
        ("placement_id", object.placement_id.to_string()),
        ("lease_id", lease.lease_id.to_string()),
        ("placement", DDR_PLACEMENT.to_string()),
        ("target", object.target.clone()),
        ("address_kind", "mmap_path".to_string()),
        ("ddr_path", object.ddr_path.clone()),
        ("ddr_fast_profile", object.ddr_fast_profile.clone()),
        (
            "ddr_madvise_hugepage",
            if object.ddr_madvise_hugepage {
                "true"
            } else {
                "false"
            }
            .to_string(),
        ),
        (
            "ddr_pretouched",
            if object.ddr_pretouched {
                "true"
            } else {
                "false"
            }
            .to_string(),
        ),
        ("ddr_prepare_us", format!("{:.3}", object.ddr_prepare_us)),
        ("ddr_madvise_us", format!("{:.3}", object.ddr_madvise_us)),
        ("ddr_pretouch_us", format!("{:.3}", object.ddr_pretouch_us)),
        (
            "ddr_fallback_reason",
            sanitize_kv_value(&object.ddr_fallback_reason),
        ),
        ("actual_bytes", object.actual_bytes.to_string()),
        ("requested_bytes", object.requested_bytes.to_string()),
        (
            "allowed_offset_bytes",
            lease.allowed_offset_bytes.to_string(),
        ),
        ("allowed_bytes", lease.allowed_bytes.to_string()),
        ("existing", if existing { "1" } else { "0" }.to_string()),
    ])
}

fn create_ddr_data_object(req: &Kv, shared: &SharedCatalog) -> Kv {
    let _trace = span(
        TraceCategory::Object,
        "object.create",
        vec![
            ("placement", DDR_PLACEMENT.to_string()),
            ("target", get(req, "target").to_string()),
            ("bytes", requested_bytes(req).to_string()),
            ("name", get(req, "name").to_string()),
            ("role", role_from_req(req)),
        ],
    );
    let (lock, _) = &**shared;
    let mut st = lock.lock().unwrap();
    let client_id = get_u64(req, "client_id");
    let size_bytes = requested_bytes(req);
    if size_bytes == 0 {
        return err("nbytes is required");
    }
    if get(req, "hint") != "mandatory:ddr" {
        return err("hint must be mandatory:ddr");
    }
    let node = match parse_ddr_target(req) {
        Ok(node) => node,
        Err(e) => return err(e),
    };
    if !st.clients.contains_key(&client_id) {
        return err("client not found");
    }
    let model_id = get(req, "model_id").to_string();
    let name = get(req, "name").to_string();
    let role = role_from_req(req);
    let immutable = get_bool(req, "immutable");

    if let Some(object) = st.find_reusable_object(
        &model_id,
        &name,
        &role,
        DDR_PLACEMENT,
        &format!("host:{}", node),
        size_bytes,
        immutable,
    ) {
        record_duration_us(
            TraceCategory::Object,
            "object.reuse",
            0.0,
            vec![
                ("object_id", object.object_id.to_string()),
                ("placement", DDR_PLACEMENT.to_string()),
                ("bytes", object.requested_bytes.to_string()),
            ],
        );
        let lease = st.make_lease(client_id, &object, 0, object.requested_bytes);
        eprintln!(
            "{{\"event\":\"ddr_object_reused\",\"object_id\":{},\"lease_id\":{},\"client_id\":{},\"model_id\":\"{}\",\"name\":\"{}\",\"role\":\"{}\",\"path\":\"{}\"}}",
            object.object_id, lease.lease_id, client_id, object.model_id, object.name, object.role, object.ddr_path
        );
        return create_ddr_lease_response(&object, &lease, true);
    }

    let use_memfd = ddr_use_memfd();
    let root = ddr_root_for_node(node);
    if !use_memfd {
        if let Err(e) = fs::create_dir_all(&root) {
            return err(format!("create DDR root failed: {}", e));
        }
    }
    let info = ddr_info(&st, node);
    if size_bytes > info.safe_allocatable {
        return err(format!(
            "DDR preflight failed: requested={} safe_allocatable={} root={} node={}",
            size_bytes,
            info.safe_allocatable,
            info.root.to_string_lossy(),
            node
        ));
    }

    let object_id = st.take_next_object_id();
    let placement_id = st.take_next_placement_id();
    let path = ddr_path(object_id, node);
    let file = if use_memfd {
        match create_memfd_file(&format!("uflow_ddr_object_{}", object_id), size_bytes) {
            Ok(file) => file,
            Err(e) => return err(e),
        }
    } else {
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
        {
            Ok(file) => file,
            Err(e) => return err(format!("create DDR object file failed: {}", e)),
        };
        if let Err(e) = file.set_len(size_bytes) {
            let _ = fs::remove_file(&path);
            return err(format!("resize DDR object file failed: {}", e));
        }
        file
    };
    let ddr_fd = if use_memfd { file.as_raw_fd() } else { -1 };
    let ddr_path_string = if use_memfd {
        proc_fd_path(ddr_fd)
    } else {
        path.to_string_lossy().to_string()
    };
    let prepare_info = {
        let _prepare = span(
            TraceCategory::Backend,
            "backend.ddr_prepare_fast_profile",
            vec![
                ("object_id", object_id.to_string()),
                ("bytes", size_bytes.to_string()),
                ("target", format!("host:{}", node)),
            ],
        );
        match prepare_ddr_file(&file, size_bytes) {
            Ok(info) => info,
            Err(e) => {
                let _ = fs::remove_file(&path);
                return err(format!("prepare DDR object failed: {}", e));
            }
        }
    };
    let ddr_fd = if use_memfd { file.into_raw_fd() } else { -1 };
    if prepare_info.madvise_hugepage {
        record_duration_us(
            TraceCategory::Backend,
            "backend.ddr_madvise_hugepage",
            prepare_info.madvise_us,
            vec![
                ("object_id", object_id.to_string()),
                ("bytes", size_bytes.to_string()),
                ("target", format!("host:{}", node)),
            ],
        );
    }
    if prepare_info.pretouched {
        record_duration_us(
            TraceCategory::Backend,
            "backend.ddr_pretouch",
            prepare_info.pretouch_us,
            vec![
                ("object_id", object_id.to_string()),
                ("bytes", size_bytes.to_string()),
                ("target", format!("host:{}", node)),
            ],
        );
        eprintln!(
            "{{\"event\":\"ddr_object_pretouched\",\"object_id\":{},\"requested_bytes\":{},\"path\":\"{}\",\"ddr_fast_profile\":\"{}\",\"madvise_hugepage\":{},\"prepare_us\":{:.3}}}",
            object_id,
            size_bytes,
            ddr_path_string,
            prepare_info.fast_profile,
            if prepare_info.madvise_hugepage { "true" } else { "false" },
            prepare_info.total_us
        );
    }
    if !prepare_info.fallback_reason.is_empty() {
        eprintln!(
            "{{\"event\":\"ddr_fast_profile_fallback\",\"object_id\":{},\"requested_bytes\":{},\"reason\":\"{}\"}}",
            object_id,
            size_bytes,
            sanitize_kv_value(&prepare_info.fallback_reason)
        );
    }

    let object = Object {
        object_id,
        placement_id,
        block_id: 0,
        placement: DDR_PLACEMENT.to_string(),
        target: format!("host:{}", node),
        ddr_path: ddr_path_string,
        ddr_fd,
        ddr_service_ptr: prepare_info.service_ptr,
        ddr_service_len: prepare_info.service_len,
        requested_bytes: size_bytes,
        actual_bytes: size_bytes,
        state: "Created".to_string(),
        creator_client_id: client_id,
        modified_offset_bytes: 0,
        modified_bytes: 0,
        model_id,
        name,
        role,
        shape: get(req, "shape").to_string(),
        dtype: get(req, "dtype").to_string(),
        immutable,
        ddr_fast_profile: prepare_info.fast_profile.clone(),
        ddr_madvise_hugepage: prepare_info.madvise_hugepage,
        ddr_pretouched: prepare_info.pretouched,
        ddr_prepare_us: prepare_info.total_us,
        ddr_madvise_us: prepare_info.madvise_us,
        ddr_pretouch_us: prepare_info.pretouch_us,
        ddr_fallback_reason: prepare_info.fallback_reason.clone(),
    };
    st.objects.insert(object_id, object.clone());
    let lease = st.make_lease(client_id, &object, 0, size_bytes);
    eprintln!(
        "{{\"event\":\"ddr_object_created\",\"object_id\":{},\"lease_id\":{},\"client_id\":{},\"requested_bytes\":{},\"actual_bytes\":{},\"role\":\"{}\",\"name\":\"{}\",\"target\":\"host:{}\",\"path\":\"{}\"}}",
        object_id,
        lease.lease_id,
        client_id,
        size_bytes,
        size_bytes,
        object.role,
        object.name,
        node,
        object.ddr_path
    );
    create_ddr_lease_response(&object, &lease, false)
}

fn create_data_object_impl(req: &Kv, shared: &SharedCatalog) -> Kv {
    if is_ddr_request(req) {
        return create_ddr_data_object(req, shared);
    }
    let _trace = span(
        TraceCategory::Object,
        "object.create",
        vec![
            ("placement", HBM_PLACEMENT.to_string()),
            ("target", get(req, "target").to_string()),
            ("bytes", requested_bytes(req).to_string()),
            ("name", get(req, "name").to_string()),
            ("role", role_from_req(req)),
        ],
    );
    let (lock, _) = &**shared;
    let mut st = lock.lock().unwrap();
    let client_id = get_u64(req, "client_id");
    let size_bytes = requested_bytes(req);
    if size_bytes == 0 {
        return err("nbytes is required");
    }
    let target_device = match validate_hbm_create_request(req, st.device) {
        Ok(device) => device,
        Err(e) => return err(e),
    };
    let client = match st.clients.get(&client_id).cloned() {
        Some(client) => client,
        None => return err("client not found"),
    };
    let model_id = get(req, "model_id").to_string();
    let name = get(req, "name").to_string();
    let role = role_from_req(req);
    let immutable = get_bool(req, "immutable");

    if let Some(object) = st.find_reusable_object(
        &model_id,
        &name,
        &role,
        HBM_PLACEMENT,
        &format!("npu:{}", target_device),
        size_bytes,
        immutable,
    ) {
        record_duration_us(
            TraceCategory::Object,
            "object.reuse",
            0.0,
            vec![
                ("object_id", object.object_id.to_string()),
                ("placement", HBM_PLACEMENT.to_string()),
                ("bytes", object.requested_bytes.to_string()),
            ],
        );
        let block_pos = match st
            .blocks
            .iter()
            .position(|block| block.block_id == object.block_id)
        {
            Some(pos) => pos,
            None => return err("block not found"),
        };
        let mut backend = NpuHbmAclBackend::default();
        let block = st.blocks[block_pos].clone();
        let export = match backend.export_for_client(&block.allocation, client.bare_tgid) {
            Ok(export) => export,
            Err(e) => return err(e),
        };
        let lease = st.make_lease(client_id, &object, 0, object.requested_bytes);
        eprintln!(
            "{{\"event\":\"data_object_reused\",\"object_id\":{},\"lease_id\":{},\"client_id\":{},\"model_id\":\"{}\",\"name\":\"{}\",\"role\":\"{}\"}}",
            object.object_id, lease.lease_id, client_id, object.model_id, object.name, object.role
        );
        return create_lease_response(&object, &lease, export, object.requested_bytes, true);
    }

    let block_pos = match st
        .blocks
        .iter()
        .position(|block| block.can_hold_on(size_bytes, target_device))
    {
        Some(pos) => pos,
        None => {
            let block_id = st.take_next_block_id();
            let mut backend = NpuHbmAclBackend::default();
            let _alloc_trace = span(
                TraceCategory::Backend,
                "backend.hbm_alloc",
                vec![
                    ("block_id", block_id.to_string()),
                    ("device_id", target_device.to_string()),
                    ("bytes", size_bytes.to_string()),
                ],
            );
            let allocation = match backend.allocate(target_device, size_bytes) {
                Ok(allocation) => allocation,
                Err(e) => {
                    st.hbm.available = false;
                    st.hbm.last_error = e.clone();
                    return err(e);
                }
            };
            st.hbm.available = true;
            st.hbm.last_error.clear();
            eprintln!(
                "{{\"event\":\"block_allocated\",\"block_id\":{},\"raw_handle_id\":{},\"requested_bytes\":{},\"actual_bytes\":{},\"service_mapping_id\":{},\"service_device_ptr\":\"0x{:x}\",\"shareable\":{},\"dynamic\":true}}",
                block_id,
                allocation.raw_handle_id,
                allocation.requested_bytes,
                allocation.actual_bytes,
                allocation.service_mapping_id,
                allocation.service_device_ptr,
                allocation.shareable
            );
            st.blocks.push(Block::new(block_id, allocation, true));
            st.blocks.len() - 1
        }
    };

    let mut backend = NpuHbmAclBackend::default();
    let block = st.blocks[block_pos].clone();
    let export = match backend.export_for_client(&block.allocation, client.bare_tgid) {
        Ok(export) => export,
        Err(e) => return err(e),
    };

    let object_id = st.take_next_object_id();
    let placement_id = st.take_next_placement_id();
    let lease_id = st.take_next_lease_id();
    st.blocks[block_pos].state = "Leased".to_string();
    st.blocks[block_pos].object_id = object_id;
    let object = Object {
        object_id,
        placement_id,
        block_id: block.block_id,
        placement: HBM_PLACEMENT.to_string(),
        target: format!("npu:{}", block.allocation.device_id),
        ddr_path: String::new(),
        ddr_fd: -1,
        ddr_service_ptr: 0,
        ddr_service_len: 0,
        requested_bytes: size_bytes,
        actual_bytes: block.allocation.actual_bytes,
        state: "Created".to_string(),
        creator_client_id: client_id,
        modified_offset_bytes: 0,
        modified_bytes: 0,
        model_id,
        name,
        role,
        shape: get(req, "shape").to_string(),
        dtype: get(req, "dtype").to_string(),
        immutable,
        ddr_fast_profile: String::new(),
        ddr_madvise_hugepage: false,
        ddr_pretouched: false,
        ddr_prepare_us: 0.0,
        ddr_madvise_us: 0.0,
        ddr_pretouch_us: 0.0,
        ddr_fallback_reason: String::new(),
    };
    st.objects.insert(object_id, object.clone());
    let lease = Lease {
        lease_id,
        object_id,
        block_id: block.block_id,
        client_id,
        state: "Active".to_string(),
        allowed_offset_bytes: 0,
        allowed_bytes: size_bytes,
    };
    st.leases.insert(lease_id, lease.clone());
    eprintln!(
        "{{\"event\":\"data_object_created\",\"object_id\":{},\"lease_id\":{},\"client_id\":{},\"block_id\":{},\"requested_bytes\":{},\"actual_bytes\":{},\"role\":\"{}\",\"name\":\"{}\",\"dynamic\":{}}}",
        object_id,
        lease_id,
        client_id,
        block.block_id,
        size_bytes,
        block.allocation.actual_bytes,
        object.role,
        object.name,
        block.dynamic
    );
    create_lease_response(&object, &lease, export, size_bytes, false)
}

pub(crate) fn open_data_object(req: &Kv, shared: &SharedCatalog) -> Kv {
    let _trace = span(
        TraceCategory::Object,
        "object.open",
        vec![
            ("object_id", get_u64(req, "object_id").to_string()),
            ("placement_id", get_u64(req, "placement_id").to_string()),
            ("client_id", get_u64(req, "client_id").to_string()),
        ],
    );
    let (lock, _) = &**shared;
    let mut st = lock.lock().unwrap();
    let client_id = get_u64(req, "client_id");
    let object_id = get_u64(req, "object_id");
    let offset = get_u64(req, "allowed_offset_bytes");
    let mut bytes = get_u64(req, "allowed_bytes");
    let client = match st.clients.get(&client_id).cloned() {
        Some(client) => client,
        None => return err("client not found"),
    };
    let object = match st.objects.get(&object_id).cloned() {
        Some(object) => object,
        None => return err("object not found"),
    };
    if bytes == 0 {
        bytes = object.requested_bytes.saturating_sub(offset);
    }
    if offset.checked_add(bytes).unwrap_or(u64::MAX) > object.requested_bytes {
        return err("open range out of bounds");
    }
    if object.placement == DDR_PLACEMENT {
        if !get(req, "target").is_empty() || !get(req, "hint").is_empty() {
            match parse_ddr_target(req) {
                Ok(node) => {
                    if object.target != format!("host:{}", node) {
                        return err(format!(
                            "DDR target host:{} does not match object target {}",
                            node, object.target
                        ));
                    }
                }
                Err(e) => return err(e),
            }
        }
        let lease = st.make_lease(client_id, &object, offset, bytes);
        eprintln!(
            "{{\"event\":\"ddr_lease_granted\",\"object_id\":{},\"lease_id\":{},\"client_id\":{},\"offset\":{},\"bytes\":{},\"path\":\"{}\"}}",
            object_id, lease.lease_id, client_id, offset, bytes, object.ddr_path
        );
        return create_ddr_lease_response(&object, &lease, true);
    }
    if let Err(e) = validate_target_matches_object(req, &object) {
        return err(e);
    }
    let block_pos = match st
        .blocks
        .iter()
        .position(|block| block.block_id == object.block_id)
    {
        Some(pos) => pos,
        None => return err("block not found"),
    };
    let mut backend = NpuHbmAclBackend::default();
    let block = st.blocks[block_pos].clone();
    let export = match backend.export_for_client(&block.allocation, client.bare_tgid) {
        Ok(export) => export,
        Err(e) => return err(e),
    };
    let lease = st.make_lease(client_id, &object, offset, bytes);
    eprintln!(
        "{{\"event\":\"lease_granted\",\"object_id\":{},\"lease_id\":{},\"client_id\":{},\"offset\":{},\"bytes\":{}}}",
        object_id, lease.lease_id, client_id, offset, bytes
    );
    create_lease_response(&object, &lease, export, object.requested_bytes, true)
}

pub(crate) fn release_data_object(req: &Kv, shared: &SharedCatalog) -> Kv {
    let _trace = span(
        TraceCategory::Object,
        "object.release",
        vec![("object_id", get_u64(req, "object_id").to_string())],
    );
    let mut allocation_to_free = None;
    let mut ddr_path_to_remove = None;
    let mut ddr_mapping_to_unmap = None;
    let mut ddr_fd_to_close = -1;
    {
        let (lock, cv) = &**shared;
        let mut st = lock.lock().unwrap();
        let object_id = get_u64(req, "object_id");
        let mut object = match st.objects.remove(&object_id) {
            Some(object) => object,
            None => return err("object not found"),
        };
        object.state = "Released".to_string();
        for lease in st
            .leases
            .values_mut()
            .filter(|lease| lease.object_id == object_id)
        {
            if lease.state != "Closed" {
                lease.state = "Stale".to_string();
                eprintln!(
                    "{{\"event\":\"lease_stale\",\"object_id\":{},\"lease_id\":{},\"client_id\":{},\"block_id\":{},\"allowed_offset_bytes\":{},\"allowed_bytes\":{}}}",
                    object_id,
                    lease.lease_id,
                    lease.client_id,
                    lease.block_id,
                    lease.allowed_offset_bytes,
                    lease.allowed_bytes
                );
            }
        }
        if object.placement == DDR_PLACEMENT {
            if object.ddr_service_ptr != 0 && object.ddr_service_len > 0 {
                ddr_mapping_to_unmap = Some((object.ddr_service_ptr, object.ddr_service_len));
            }
            ddr_fd_to_close = object.ddr_fd;
            if !object.ddr_path.is_empty() {
                ddr_path_to_remove = Some(object.ddr_path.clone());
            }
        } else if let Some(pos) = st
            .blocks
            .iter()
            .position(|block| block.block_id == object.block_id)
        {
            if st.blocks[pos].dynamic {
                allocation_to_free = Some(st.blocks.remove(pos).allocation);
            } else {
                st.blocks[pos].state = "Ready".to_string();
                st.blocks[pos].object_id = 0;
            }
        }
        eprintln!(
            "{{\"event\":\"data_object_released\",\"object_id\":{},\"creator_client_id\":{},\"model_id\":\"{}\",\"name\":\"{}\",\"role\":\"{}\",\"placement\":\"{}\"}}",
            object_id, object.creator_client_id, object.model_id, object.name, object.role, object.placement
        );
        cv.notify_all();
    }
    if let Some(allocation) = allocation_to_free {
        let mut backend = NpuHbmAclBackend::default();
        let _free_trace = span(
            TraceCategory::Backend,
            "backend.hbm_free",
            vec![
                ("device_id", allocation.device_id.to_string()),
                ("bytes", allocation.actual_bytes.to_string()),
                ("raw_handle_id", allocation.raw_handle_id.to_string()),
            ],
        );
        if let Err(e) = backend.free(allocation) {
            return err(e);
        }
    }
    if let Some(path) = ddr_path_to_remove {
        if let Some((ptr, len)) = ddr_mapping_to_unmap {
            if let Err(e) = unmap_ddr_service_mapping(ptr, len) {
                return err(e);
            }
        }
        if ddr_fd_to_close >= 0 {
            if let Err(e) = close_ddr_fd(ddr_fd_to_close) {
                return err(e);
            }
        } else if let Err(e) = fs::remove_file(&path) {
            return err(format!("remove DDR object file failed: {}", e));
        }
    }
    ok(&[])
}

pub(crate) fn describe_object(req: &Kv, shared: &SharedCatalog) -> Kv {
    let (lock, _) = &**shared;
    let st = lock.lock().unwrap();
    let object = match object_by_request(req, &st, "object_id", "placement_id") {
        Some(object) => object,
        None => return err("object not found"),
    };
    ok(&[
        ("object_id", object.object_id.to_string()),
        ("placement_id", object.placement_id.to_string()),
        ("namespace", object.model_id.clone()),
        ("name", object.name.clone()),
        ("role", object.role.clone()),
        ("size_bytes", object.requested_bytes.to_string()),
        ("actual_bytes", object.actual_bytes.to_string()),
        ("state", object.state.clone()),
        ("medium", object.placement.clone()),
        ("target", object.target.clone()),
        ("address_kind", address_kind(&object).to_string()),
        (
            "domain",
            if object.placement == DDR_PLACEMENT {
                "numa_node"
            } else {
                "local_node"
            }
            .to_string(),
        ),
        ("offset_bytes", "0".to_string()),
        ("nbytes", object.requested_bytes.to_string()),
        ("ddr_path", object.ddr_path.clone()),
        ("ddr_fast_profile", object.ddr_fast_profile.clone()),
        (
            "ddr_madvise_hugepage",
            if object.ddr_madvise_hugepage {
                "true"
            } else {
                "false"
            }
            .to_string(),
        ),
        (
            "ddr_pretouched",
            if object.ddr_pretouched {
                "true"
            } else {
                "false"
            }
            .to_string(),
        ),
        ("ddr_prepare_us", format!("{:.3}", object.ddr_prepare_us)),
        ("ddr_madvise_us", format!("{:.3}", object.ddr_madvise_us)),
        ("ddr_pretouch_us", format!("{:.3}", object.ddr_pretouch_us)),
        (
            "ddr_fallback_reason",
            sanitize_kv_value(&object.ddr_fallback_reason),
        ),
        ("shape", object.shape.clone()),
        ("dtype", object.dtype.clone()),
        (
            "consistency",
            if object.immutable {
                "immutable"
            } else {
                "single_writer"
            }
            .to_string(),
        ),
    ])
}

pub(crate) fn register_client(req: &Kv, shared: &SharedCatalog) -> Kv {
    let (lock, _) = &**shared;
    let mut st = lock.lock().unwrap();
    let client_id = st.take_next_client_id();
    let client = Client {
        client_id,
        role: get(req, "role").to_string(),
        os_pid: get_i64(req, "os_pid"),
        bare_tgid: get_i64(req, "bare_tgid"),
        device_id: get(req, "device_id").parse().unwrap_or(st.device),
    };
    eprintln!(
        "{{\"event\":\"client_registered\",\"client_id\":{},\"role\":\"{}\",\"os_pid\":{},\"bare_tgid\":{},\"device_id\":{}}}",
        client.client_id, client.role, client.os_pid, client.bare_tgid, client.device_id
    );
    st.clients.insert(client_id, client);
    ok(&[("client_id", client_id.to_string())])
}

pub(crate) fn register_model(req: &Kv) -> Kv {
    let model_id = get(req, "model_id").to_string();
    if model_id.is_empty() {
        return err("model_id is required");
    }
    eprintln!(
        "{{\"event\":\"model_registered\",\"model_id\":\"{}\"}}",
        model_id
    );
    ok(&[("model_id", model_id)])
}

pub(crate) fn mark_ready(req: &Kv, shared: &SharedCatalog) -> Kv {
    let (lock, cv) = &**shared;
    let mut st = lock.lock().unwrap();
    let object_id = get_u64(req, "object_id");
    if let Some(object) = st.objects.get_mut(&object_id) {
        object.state = "Ready".to_string();
        record_duration_us(
            TraceCategory::Object,
            "object.mark_ready",
            0.0,
            vec![("object_id", object_id.to_string())],
        );
        eprintln!(
            "{{\"event\":\"data_object_ready\",\"object_id\":{}}}",
            object_id
        );
        cv.notify_all();
        ok(&[])
    } else {
        err("object not found")
    }
}

pub(crate) fn mark_dirty(req: &Kv, shared: &SharedCatalog) -> Kv {
    let (lock, cv) = &**shared;
    let mut st = lock.lock().unwrap();
    let object_id = get_u64(req, "object_id");
    let offset = get_u64(req, "modified_offset_bytes");
    let bytes = get_u64(req, "modified_bytes");
    if let Some(object) = st.objects.get_mut(&object_id) {
        if offset.checked_add(bytes).unwrap_or(u64::MAX) > object.requested_bytes {
            return err("modified range out of bounds");
        }
        object.state = "Modified".to_string();
        object.modified_offset_bytes = offset;
        object.modified_bytes = bytes;
        record_duration_us(
            TraceCategory::Object,
            "object.mark_dirty",
            0.0,
            vec![
                ("object_id", object_id.to_string()),
                ("offset", offset.to_string()),
                ("bytes", bytes.to_string()),
            ],
        );
        eprintln!(
            "{{\"event\":\"data_object_modified\",\"object_id\":{},\"offset\":{},\"bytes\":{}}}",
            object_id, offset, bytes
        );
        cv.notify_all();
        ok(&[])
    } else {
        err("object not found")
    }
}

pub(crate) fn close_lease(req: &Kv, shared: &SharedCatalog) -> Kv {
    let (lock, _) = &**shared;
    let mut st = lock.lock().unwrap();
    let lease_id = get_u64(req, "lease_id");
    if let Some(lease) = st.leases.get_mut(&lease_id) {
        lease.state = "Closed".to_string();
        eprintln!("{{\"event\":\"lease_closed\",\"lease_id\":{}}}", lease_id);
        ok(&[])
    } else {
        err("lease not found")
    }
}

pub(crate) fn get_model_objects(req: &Kv, shared: &SharedCatalog) -> Kv {
    let (lock, _) = &**shared;
    let st = lock.lock().unwrap();
    let model_id = get(req, "model_id");
    if model_id.is_empty() {
        return err("model_id is required");
    }
    let objects = st
        .objects
        .values()
        .filter(|object| object.model_id == model_id)
        .map(|object| {
            format!(
                "{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}",
                object.object_id,
                object.name,
                object.role,
                object.state,
                object.requested_bytes,
                object.actual_bytes,
                object.shape,
                object.dtype,
                object.placement,
                object.target,
                object.ddr_path
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    ok(&[
        ("model_id", model_id.to_string()),
        (
            "object_count",
            st.objects
                .values()
                .filter(|object| object.model_id == model_id)
                .count()
                .to_string(),
        ),
        ("objects", objects),
    ])
}

pub(crate) fn shutdown_daemon(shared: &SharedCatalog) -> Kv {
    let (lock, cv) = &**shared;
    let mut st = lock.lock().unwrap();
    st.shutdown_requested = true;
    eprintln!(
        "{{\"event\":\"daemon_shutdown_requested\",\"reason\":\"command\",\"active_clients\":{},\"active_leases\":{}}}",
        st.clients.len(),
        st.leases.values().filter(|lease| lease.state == "Active").count()
    );
    cv.notify_all();
    ok(&[("detail", "shutdown_requested".to_string())])
}
