# UFlow Trace Summary

- schema_version: `2`
- run_id: `phasee06_trace_v2_http_stop_final_20260812_019f7da1`
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
| 30000 | h2d | ddr_to_hbm | ddr_hbm_direct_thp | acl_direct_async_thp | complete | 4194304 | 1742.711 | false | 5431.533 |
| 30001 | d2h | hbm_to_ddr | hbm_ddr_direct_thp | acl_direct_async_thp | complete | 4194304 | 1084.664 | false | 3386.749 |
| 30002 | h2d | ddr_to_hbm | ddr_hbm_direct_thp | acl_direct_async_thp | complete | 4194304 | 293.862 | false | 982.162 |
| 30003 | d2h | hbm_to_ddr | hbm_ddr_direct_thp | acl_direct_async_thp | complete | 4194304 | 296.212 | false | 1012.381 |
| 30004 | h2d | ddr_to_hbm | memfd_pinned_hbm_channel | acl_pinned_async_channel | complete | 4194304 | 1764.532 | false | 6720.783 |
| 30005 | d2h | hbm_to_ddr | memfd_pinned_hbm_channel | acl_pinned_async_channel | complete | 4194304 | 1462.770 | false | 5564.417 |
| 30006 | ssd_to_ddr | ssd_to_ddr | ssd_to_ddr | ssd_buffered_pread | complete | 4194304 | 1117.415 | false | 4297.816 |
| 30007 | ddr_to_ssd | ddr_to_ssd | ddr_to_ssd | ssd_buffered_pwrite | complete | 4194304 | 2093.246 | false | 8132.791 |
| 30008 | h2d | ssd_to_hbm_via_ddr | ssd_to_hbm_via_ddr | ssd_hbm_relay_ddr | complete | 4194304 | 3508.023 | false | 11700.744 |
| 30009 | d2h | hbm_to_ssd_via_ddr | hbm_to_ssd_via_ddr | ssd_hbm_relay_ddr | complete | 4194304 | 4321.346 | false | 14440.466 |
| 30010 | h2d | ssd_to_hbm_direct | ssd_to_hbm_direct | ssd_hbm_file_mmap_acl_direct | complete | 268435456 | 80045.992 | false | 225103.993 |
| 30011 | d2h | hbm_to_ssd_direct | hbm_to_ssd_direct | ssd_hbm_file_mmap_acl_direct | complete | 268435456 | 140706.654 | false | 402399.759 |
| 30012 | h2d | ssd_to_hbm_direct | ssd_to_hbm_direct | ssd_hbm_auto_file_mmap_acl_direct | complete | 268435456 | 80174.547 | false | 224908.167 |
| 30013 | d2h | hbm_to_ssd_direct | hbm_to_ssd_direct | ssd_hbm_auto_file_mmap_acl_direct | complete | 268435456 | 138650.720 | false | 397100.341 |

## Stage compatibility summary

| category | stage | count | total us | avg us | max us | bytes |
|---|---:|---:|---:|---:|---:|---:|
| control | command.handle | 144 | 737829.973 | 5123.819 | 139648.111 | 0 |
| transfer | transfer.worker_execute | 14 | 457495.391 | 32678.242 | 140721.534 | 1115684864 |
| transfer | transfer.execute | 14 | 456365.906 | 32597.565 | 140636.601 | 1115684864 |
| object | object.release | 28 | 280098.629 | 10003.522 | 42725.141 | 0 |
| acl | ssd_hbm.direct_d2h_acl_wait | 2 | 240852.115 | 120426.057 | 121013.752 | 536870912 |
| backend | backend.hbm_free | 12 | 148352.212 | 12362.684 | 35708.325 | 1107296256 |
| acl | ssd_hbm.direct_h2d_acl_wait | 2 | 129593.452 | 64796.726 | 65021.400 | 536870912 |
| object | object.create | 28 | 7810.509 | 278.947 | 544.522 | 2231369728 |
| transfer | ssd.write_wait | 2 | 3745.552 | 1872.776 | 1991.941 | 8388608 |
| backend | backend.hbm_alloc | 12 | 3598.006 | 299.834 | 507.721 | 1107296256 |
| transfer | ssd.read_wait | 2 | 2491.102 | 1245.551 | 1446.899 | 8388608 |
| acl | relay.hbm_to_ddr | 1 | 2305.544 | 2305.544 | 2305.544 | 4194304 |
| channel | channel.acquire | 2 | 2166.538 | 1083.269 | 1180.788 | 8388608 |
| channel | channel.lane_create | 2 | 2166.538 | 1083.269 | 1180.788 | 8388608 |
| channel | direct_lane.create | 2 | 2027.223 | 1013.611 | 1291.803 | 8388608 |
| transfer | transfer.copy.ddr_to_ssd | 1 | 2011.162 | 2011.162 | 2011.162 | 4194304 |
| acl | relay.ddr_to_hbm | 1 | 1811.903 | 1811.903 | 1811.903 | 4194304 |
| transfer | relay.ddr_to_ssd | 1 | 1759.921 | 1759.921 | 1759.921 | 4194304 |
| transfer | relay.ssd_to_ddr | 1 | 1453.869 | 1453.869 | 1453.869 | 4194304 |
| channel | direct_lane.busy | 4 | 1061.694 | 265.423 | 347.905 | 16777216 |
| transfer | transfer.copy.ssd_to_ddr | 1 | 1050.723 | 1050.723 | 1050.723 | 4194304 |
| backend | backend.ddr_prepare_fast_profile | 8 | 973.349 | 121.669 | 134.526 | 33554432 |
| backend | backend.ddr_pretouch | 8 | 921.706 | 115.213 | 129.486 | 33554432 |
| channel | channel.lane_busy | 2 | 814.302 | 407.151 | 456.198 | 8388608 |
| transfer | transfer.plan | 14 | 587.663 | 41.976 | 47.962 | 1115684864 |
| acl | direct_h2d.acl_submit | 2 | 429.468 | 214.734 | 259.801 | 8388608 |
| acl | direct_d2h.acl_submit | 2 | 375.335 | 187.668 | 189.088 | 8388608 |
| chunk | chunk.cpu_copy.memfd_to_pinned | 1 | 270.361 | 270.361 | 270.361 | 4194304 |
| chunk | chunk.cpu_copy.pinned_to_memfd | 1 | 168.367 | 168.367 | 168.367 | 4194304 |
| acl | chunk.acl_wait.d2h | 1 | 140.546 | 140.546 | 140.546 | 4194304 |
| acl | chunk.acl_wait.h2d | 1 | 125.295 | 125.295 | 125.295 | 4194304 |
| acl | direct_h2d.acl_wait | 2 | 108.504 | 54.252 | 73.353 | 8388608 |
| transfer | transfer.queue_wait | 14 | 101.038 | 7.217 | 10.431 | 1115684864 |
| acl | direct_d2h.acl_wait | 2 | 84.363 | 42.181 | 63.042 | 8388608 |
| transfer | transfer.enqueue | 14 | 82.015 | 5.858 | 17.541 | 1115684864 |
| backend | ssd_hbm.direct_setup | 4 | 61.442 | 15.361 | 16.870 | 1073741824 |
| acl | chunk.acl_submit.h2d | 1 | 33.132 | 33.132 | 33.132 | 4194304 |
| acl | chunk.acl_submit.d2h | 1 | 26.421 | 26.421 | 26.421 | 4194304 |
| backend | backend.ddr_madvise_hugepage | 8 | 6.440 | 0.805 | 1.130 | 33554432 |
| channel | direct_lane.acquire | 2 | 1.780 | 0.890 | 1.010 | 8388608 |
