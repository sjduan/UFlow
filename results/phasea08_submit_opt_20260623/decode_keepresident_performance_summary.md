# PhaseA-08 Transfer Performance Summary

| stage | events | bytes | wall_ms | sum_latency_ms | stage_effective_bw_gib_s | copy_bw_gib_s | engines | paths |
|---|---:|---:|---:|---:|---:|---:|---|---|
| decode | 13 | 27982950400 | 566.588 | 469.851 | 45.997 | 55.467 | `{'acl_direct_async_thp': 13}` | `{'ddr_hbm_direct_thp': 13}` |
| prefill | 40 | 26425794560 | 441.159 | 10769.340 | 55.787 | 2.285 | `{'acl_direct_async_thp': 40}` | `{'ddr_hbm_direct_thp': 40}` |
