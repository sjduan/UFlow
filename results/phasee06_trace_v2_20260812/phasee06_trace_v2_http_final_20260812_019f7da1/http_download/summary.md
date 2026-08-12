# UFlow Trace Summary

- schema_version: `2`
- run_id: `phasee06_trace_v2_http_final_20260812_019f7da1`
- mode: `full`
- events: `440`
- transfers: `14`
- complete: `true`
- raw_args_exported: `true`
- dropped_events: `0`
- incomplete_event_ids: `none`

- invalid_or_duplicate_event_ids: `none`

## Transfers

| event | direction | planned path | actual path | engine | status | bytes | latency us | fallback | span sum us |
|---:|---|---|---|---|---|---:|---:|---|---:|
| 30000 | h2d | ddr_to_hbm | ddr_hbm_direct_thp | acl_direct_async_thp | complete | 4194304 | 1163.607 | false | 3691.603 |
| 30001 | d2h | hbm_to_ddr | hbm_ddr_direct_thp | acl_direct_async_thp | complete | 4194304 | 1034.592 | false | 3219.809 |
| 30002 | h2d | ddr_to_hbm | ddr_hbm_direct_thp | acl_direct_async_thp | complete | 4194304 | 290.262 | false | 984.761 |
| 30003 | d2h | hbm_to_ddr | hbm_ddr_direct_thp | acl_direct_async_thp | complete | 4194304 | 314.113 | false | 1112.635 |
| 30004 | h2d | ddr_to_hbm | memfd_pinned_hbm_channel | acl_pinned_async_channel | complete | 4194304 | 1702.289 | false | 6532.595 |
| 30005 | d2h | hbm_to_ddr | memfd_pinned_hbm_channel | acl_pinned_async_channel | complete | 4194304 | 1458.339 | false | 5597.267 |
| 30006 | ssd_to_ddr | ssd_to_ddr | ssd_to_ddr | ssd_buffered_pread | complete | 4194304 | 1121.176 | false | 4322.397 |
| 30007 | ddr_to_ssd | ddr_to_ssd | ddr_to_ssd | ssd_buffered_pwrite | complete | 4194304 | 2081.895 | false | 8129.332 |
| 30008 | h2d | ssd_to_hbm_via_ddr | ssd_to_hbm_via_ddr | ssd_hbm_relay_ddr | complete | 4194304 | 10376.033 | false | 32236.251 |
| 30009 | d2h | hbm_to_ssd_via_ddr | hbm_to_ssd_via_ddr | ssd_hbm_relay_ddr | complete | 4194304 | 6451.673 | false | 22305.530 |
| 30010 | h2d | ssd_to_hbm_direct | ssd_to_hbm_direct | ssd_hbm_file_mmap_acl_direct | complete | 268435456 | 83179.560 | false | 239181.596 |
| 30011 | d2h | hbm_to_ssd_direct | hbm_to_ssd_direct | ssd_hbm_file_mmap_acl_direct | complete | 268435456 | 152027.355 | false | 443713.050 |
| 30012 | h2d | ssd_to_hbm_direct | ssd_to_hbm_direct | ssd_hbm_auto_file_mmap_acl_direct | complete | 268435456 | 94872.796 | false | 274516.988 |
| 30013 | d2h | hbm_to_ssd_direct | hbm_to_ssd_direct | ssd_hbm_auto_file_mmap_acl_direct | complete | 268435456 | 153076.208 | false | 446789.635 |

## Stage compatibility summary

| category | stage | count | total us | avg us | max us | bytes |
|---|---:|---:|---:|---:|---:|---:|
| control | command.handle | 144 | 922496.776 | 6406.228 | 152018.705 | 0 |
| transfer | transfer.worker_execute | 14 | 509573.674 | 36398.120 | 153111.259 | 1115684864 |
| transfer | transfer.execute | 14 | 508276.222 | 36305.444 | 152993.865 | 1115684864 |
| object | object.release | 28 | 412297.971 | 14724.928 | 84116.478 | 0 |
| acl | ssd_hbm.direct_d2h_acl_wait | 2 | 280254.650 | 140127.325 | 140627.410 | 536870912 |
| acl | ssd_hbm.direct_h2d_acl_wait | 2 | 157578.241 | 78789.120 | 84769.764 | 536870912 |
| backend | backend.hbm_free | 12 | 148613.966 | 12384.497 | 35749.447 | 1107296256 |
| acl | relay.ddr_to_hbm | 1 | 8685.384 | 8685.384 | 8685.384 | 4194304 |
| object | object.create | 28 | 8399.512 | 299.983 | 896.856 | 2231369728 |
| transfer | ssd.write_wait | 2 | 5147.230 | 2573.615 | 3151.959 | 8388608 |
| backend | backend.hbm_alloc | 12 | 3619.257 | 301.605 | 481.430 | 1107296256 |
| transfer | relay.ddr_to_ssd | 1 | 3168.619 | 3168.619 | 3168.619 | 4194304 |
| acl | relay.hbm_to_ddr | 1 | 3019.403 | 3019.403 | 3019.403 | 4194304 |
| transfer | ssd.read_wait | 2 | 2450.510 | 1225.255 | 1401.277 | 8388608 |
| channel | channel.acquire | 2 | 2085.045 | 1042.523 | 1127.766 | 8388608 |
| channel | channel.lane_create | 2 | 2085.045 | 1042.523 | 1127.766 | 8388608 |
| transfer | transfer.copy.ddr_to_ssd | 1 | 2012.722 | 2012.722 | 2012.722 | 4194304 |
| channel | direct_lane.create | 2 | 1438.599 | 719.300 | 742.121 | 8388608 |
| transfer | relay.ssd_to_ddr | 1 | 1410.047 | 1410.047 | 1410.047 | 4194304 |
| channel | direct_lane.busy | 4 | 1056.703 | 264.176 | 328.623 | 16777216 |
| transfer | transfer.copy.ssd_to_ddr | 1 | 1055.923 | 1055.923 | 1055.923 | 4194304 |
| backend | backend.ddr_prepare_fast_profile | 8 | 952.949 | 119.119 | 162.256 | 33554432 |
| backend | backend.ddr_pretouch | 8 | 903.706 | 112.963 | 157.606 | 33554432 |
| channel | channel.lane_busy | 2 | 863.915 | 431.958 | 464.359 | 8388608 |
| transfer | transfer.plan | 14 | 587.075 | 41.934 | 59.052 | 1115684864 |
| acl | direct_h2d.acl_submit | 2 | 420.367 | 210.183 | 246.960 | 8388608 |
| acl | direct_d2h.acl_submit | 2 | 388.495 | 194.248 | 201.858 | 8388608 |
| chunk | chunk.cpu_copy.memfd_to_pinned | 1 | 264.271 | 264.271 | 264.271 | 4194304 |
| chunk | chunk.cpu_copy.pinned_to_memfd | 1 | 183.777 | 183.777 | 183.777 | 4194304 |
| acl | chunk.acl_wait.d2h | 1 | 162.047 | 162.047 | 162.047 | 4194304 |
| acl | chunk.acl_wait.h2d | 1 | 142.125 | 142.125 | 142.125 | 4194304 |
| backend | ssd_hbm.direct_setup | 4 | 135.526 | 33.882 | 34.611 | 1073741824 |
| transfer | transfer.queue_wait | 14 | 135.024 | 9.645 | 16.350 | 1115684864 |
| acl | direct_h2d.acl_wait | 2 | 104.804 | 52.402 | 69.343 | 8388608 |
| acl | direct_d2h.acl_wait | 2 | 88.544 | 44.272 | 50.572 | 8388608 |
| transfer | transfer.enqueue | 14 | 77.234 | 5.517 | 7.530 | 1115684864 |
| acl | chunk.acl_submit.h2d | 1 | 36.201 | 36.201 | 36.201 | 4194304 |
| acl | chunk.acl_submit.d2h | 1 | 29.311 | 29.311 | 29.311 | 4194304 |
| backend | backend.ddr_madvise_hugepage | 8 | 6.560 | 0.820 | 1.200 | 33554432 |
| channel | direct_lane.release | 4 | 1.991 | 0.498 | 0.970 | 16777216 |
