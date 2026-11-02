use crate::catalog::SharedCatalog;
use crate::ssd_direct;
use uf_core::{ok, Kv};

const SOLID_OPS: &[&str] = &[
    "RegisterClient",
    "RegisterModel",
    "CreateDataObject",
    "OpenDataObject",
    "ReleaseDataObject",
    "CloseLease",
    "DescribeObject",
    "MarkReady",
    "MarkDirty",
    "MarkModified",
    "EstimateCost",
    "PlanTransfer",
    "SubmitTransfer",
    "ListTransferEvents",
    "PollEvent",
    "WaitEvent",
    "CancelEvent",
    "GetStats",
    "GetModelObjects",
    "GetTraceStatus",
    "StartTrace",
    "StopTrace",
    "FlushTrace",
    "ClearTrace",
    "ExportTrace",
    "GetCapabilities",
    "GetSimulatedValidation",
    "ShutdownDaemon",
];

pub(crate) fn get_capabilities(shared: &SharedCatalog) -> Kv {
    let direct_candidate = ssd_direct::configured_candidate_name();
    let direct_enabled = ssd_direct::direct_enabled();
    let direct_status = match ssd_direct::configured_candidate() {
        Ok(_) => "configured",
        Err(_) if direct_enabled => "candidate_unavailable",
        Err(_) => "disabled",
    };
    let (provider_ids, provider_health, external_configured, external_pidfd_observed) = {
        let (lock, _) = &**shared;
        let catalog = lock.lock().unwrap();
        let provider_ids = catalog
            .providers
            .identities()
            .iter()
            .map(|identity| identity.provider_id.clone())
            .collect::<Vec<_>>()
            .join(",");
        let provider_health = catalog
            .providers
            .health_summary()
            .iter()
            .map(|(provider_id, health)| format!("{}:{}", provider_id, health.as_str()))
            .collect::<Vec<_>>()
            .join(",");
        (
            provider_ids,
            provider_health,
            catalog.external_ddr.configured(),
            catalog.external_ddr.pidfd_observed(),
        )
    };
    ok(&[
        ("ops", SOLID_OPS.join(",")),
        ("placements", "hbm,ddr,ssd".to_string()),
        (
            "address_kinds",
            "shareable_handle,mmap_path,file_path_offset".to_string(),
        ),
        (
            "transfer_paths",
            "direct_ref,hbm_to_ddr,ddr_to_hbm,hbm_to_hbm,ddr_to_ddr,ssd_to_ddr,ddr_to_ssd,ssd_to_hbm_via_ddr,hbm_to_ssd_via_ddr,ssd_to_hbm_direct,hbm_to_ssd_direct".to_string(),
        ),
        (
            "planned_transfer_paths",
            "ssd_to_hbm_physical_direct,hbm_to_ssd_physical_direct,ssu_lba_to_hbm,hbm_to_ssu_lba".to_string(),
        ),
        (
            "transfer_modes",
            "auto,direct_async,sync,pinned,pinned_async,async,buffered,relay,ssd_hbm_direct".to_string(),
        ),
        (
            "ssd_hbm_direct_enabled",
            if direct_enabled { "true" } else { "false" }.to_string(),
        ),
        ("ssd_hbm_direct_status", direct_status.to_string()),
        ("ssd_hbm_direct_candidate", direct_candidate),
        (
            "ssd_hbm_direct_candidates",
            ssd_direct::implemented_candidates_csv(),
        ),
        (
            "ssd_hbm_direct_planned_candidates",
            ssd_direct::planned_candidates_csv(),
        ),
        (
            "ssd_hbm_direct_kind",
            if direct_enabled { "logical" } else { "" }.to_string(),
        ),
        ("trace", "1".to_string()),
        ("provider_api", "1.0".to_string()),
        ("providers", provider_ids),
        ("provider_health", provider_health),
        ("resource_binding", "1".to_string()),
        ("access_binding", "1".to_string()),
        ("execution_realms", "1".to_string()),
        ("external_ddr_broker_ops", "RegisterExternalBacking,AcquireExternalRead,RetainExternalView,ReleaseExternalView,RevokeExternalBacking,GetExternalDdrSnapshot".to_string()),
        ("external_ddr_transport", "same_uid_unix_scm_rights".to_string()),
        ("external_ddr_status", if external_configured { "configured" } else { "identity_unknown" }.to_string()),
        ("external_ddr_pidfd", if external_pidfd_observed { "observed_this_broker" } else { "unknown" }.to_string()),
        ("external_ddr_limits", "cooperative_immutable,fd_range_not_os_isolated,numa_unknown,copy_unsupported,bounded_lifetime_metadata".to_string()),
        ("legacy_ops_removed", "CreateBuffer,OpenBuffer,ReleaseBuffer,UploadData,GetDeviceHandle,CompleteTransferEvent,WaitObjectEvent".to_string()),
    ])
}
