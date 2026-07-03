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
    ok(&[
        ("ops", SOLID_OPS.join(",")),
        ("placements", "hbm,ddr".to_string()),
        ("address_kinds", "shareable_handle,mmap_path".to_string()),
        ("transfer_paths", "direct_ref,hbm_to_ddr,ddr_to_hbm,hbm_to_hbm,ddr_to_ddr".to_string()),
        (
            "transfer_modes",
            "auto,direct_async,sync,pinned,pinned_async,async".to_string(),
        ),
        ("trace", "1".to_string()),
        ("legacy_ops_removed", "CreateBuffer,OpenBuffer,ReleaseBuffer,UploadData,GetDeviceHandle,CompleteTransferEvent,WaitObjectEvent".to_string()),
    ])
}
