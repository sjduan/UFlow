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
    "ShutdownDaemon",
];

pub(crate) fn get_capabilities() -> Kv {
    let direct_candidate = ssd_direct::configured_candidate_name();
    let direct_enabled = ssd_direct::direct_enabled();
    let direct_status = match ssd_direct::configured_candidate() {
        Ok(_) => "configured",
        Err(_) if direct_enabled => "candidate_unavailable",
        Err(_) => "disabled",
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
        ("legacy_ops_removed", "CreateBuffer,OpenBuffer,ReleaseBuffer,UploadData,GetDeviceHandle,CompleteTransferEvent,WaitObjectEvent".to_string()),
    ])
}
