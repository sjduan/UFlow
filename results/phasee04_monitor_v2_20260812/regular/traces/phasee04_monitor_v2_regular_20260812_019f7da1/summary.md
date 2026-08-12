# UFlow Trace Summary

- schema_version: `2`
- run_id: `phasee04_monitor_v2_regular_20260812_019f7da1`
- mode: `full`
- events: `841`
- transfers: `24`
- complete: `true`
- raw_args_exported: `true`
- dropped_events: `0`
- incomplete_event_ids: `none`

- invalid_or_duplicate_event_ids: `none`

## Transfers

| event | direction | planned path | actual path | engine | status | bytes | latency us | fallback | span sum us |
|---:|---|---|---|---|---|---:|---:|---|---:|
| 30000 | h2d | ddr_to_hbm | ddr_hbm_direct_thp | acl_direct_async_thp | complete | 67108864 | 2837.486 | false | 9736.547 |
| 30001 | d2h | hbm_to_ddr | hbm_ddr_direct_thp | acl_direct_async_thp | complete | 67108864 | 3050.235 | false | 10785.029 |
| 30002 | h2d | ddr_to_hbm | ddr_hbm_direct_thp | acl_direct_async_thp | complete | 67108864 | 1326.914 | false | 5073.199 |
| 30003 | d2h | hbm_to_ddr | hbm_ddr_direct_thp | acl_direct_async_thp | complete | 67108864 | 1867.776 | false | 7285.425 |
| 30004 | h2d | ddr_to_hbm | memfd_pinned_hbm_channel | acl_pinned_async_channel | complete | 67108864 | 6077.688 | false | 23977.147 |
| 30005 | d2h | hbm_to_ddr | memfd_pinned_hbm_channel | acl_pinned_async_channel | complete | 67108864 | 7739.236 | false | 30572.665 |
| 30006 | ssd_to_ddr | ssd_to_ddr | ssd_to_ddr | ssd_buffered_pread | complete | 67108864 | 16953.061 | false | 67563.586 |
| 30007 | ddr_to_ssd | ddr_to_ssd | ddr_to_ssd | ssd_buffered_pwrite | complete | 67108864 | 30659.009 | false | 122482.690 |
| 30008 | h2d | ssd_to_hbm_via_ddr | ssd_to_hbm_via_ddr | ssd_hbm_relay_ddr | complete | 67108864 | 21645.702 | false | 82128.379 |
| 30009 | d2h | hbm_to_ssd_via_ddr | hbm_to_ssd_via_ddr | ssd_hbm_relay_ddr | complete | 67108864 | 44162.580 | false | 170908.248 |
| 30010 | h2d | ddr_to_hbm | ddr_hbm_direct_thp | acl_direct_async_thp | complete | 268435456 | 4825.717 | false | 18982.973 |
| 30011 | d2h | hbm_to_ddr | hbm_ddr_direct_thp | acl_direct_async_thp | complete | 268435456 | 6714.004 | false | 26544.392 |
| 30012 | h2d | ddr_to_hbm | ddr_hbm_direct_thp | acl_direct_async_thp | complete | 268435456 | 4648.479 | false | 18303.756 |
| 30013 | d2h | hbm_to_ddr | hbm_ddr_direct_thp | acl_direct_async_thp | complete | 268435456 | 6617.800 | false | 26193.696 |
| 30014 | h2d | ddr_to_hbm | memfd_pinned_hbm_channel | acl_pinned_async_channel | complete | 268435456 | 41237.771 | false | 163795.883 |
| 30015 | d2h | hbm_to_ddr | memfd_pinned_hbm_channel | acl_pinned_async_channel | complete | 268435456 | 38804.891 | false | 154869.393 |
| 30016 | ssd_to_ddr | ssd_to_ddr | ssd_to_ddr | ssd_buffered_pread | complete | 268435456 | 90606.713 | false | 362240.954 |
| 30017 | ddr_to_ssd | ddr_to_ssd | ddr_to_ssd | ssd_buffered_pwrite | complete | 268435456 | 295706.543 | false | 1182664.932 |
| 30018 | h2d | ssd_to_hbm_via_ddr | ssd_to_hbm_via_ddr | ssd_hbm_relay_ddr | complete | 268435456 | 86446.013 | false | 333431.238 |
| 30019 | d2h | hbm_to_ssd_via_ddr | hbm_to_ssd_via_ddr | ssd_hbm_relay_ddr | complete | 268435456 | 178455.753 | false | 698255.363 |
| 30020 | h2d | ssd_to_hbm_direct | ssd_to_hbm_direct | ssd_hbm_file_mmap_acl_direct | complete | 268435456 | 96325.936 | false | 278708.841 |
| 30021 | d2h | hbm_to_ssd_direct | hbm_to_ssd_direct | ssd_hbm_file_mmap_acl_direct | complete | 268435456 | 153243.796 | false | 447252.820 |
| 30022 | h2d | ssd_to_hbm_direct | ssd_to_hbm_direct | ssd_hbm_auto_file_mmap_acl_direct | complete | 268435456 | 95783.744 | false | 277126.446 |
| 30023 | d2h | hbm_to_ssd_direct | hbm_to_ssd_direct | ssd_hbm_auto_file_mmap_acl_direct | complete | 268435456 | 153804.009 | false | 449105.675 |

## Stage compatibility summary

| category | stage | count | total us | avg us | max us | bytes |
|---|---:|---:|---:|---:|---:|---:|
| control | command.handle | 242 | 2563191.837 | 10591.702 | 294664.960 | 0 |
| transfer | transfer.worker_execute | 24 | 1390398.379 | 57933.266 | 295783.935 | 4429185024 |
| transfer | transfer.execute | 24 | 1387709.093 | 57821.212 | 295633.719 | 4429185024 |
| object | object.release | 48 | 1100621.348 | 22929.611 | 76709.106 | 0 |
| transfer | ssd.write_wait | 7 | 528246.029 | 75463.718 | 295593.678 | 671088640 |
| backend | backend.hbm_free | 20 | 506410.478 | 25320.524 | 37581.091 | 3758096384 |
| transfer | transfer.copy.ddr_to_ssd | 2 | 326216.085 | 163108.043 | 295625.899 | 335544320 |
| acl | ssd_hbm.direct_d2h_acl_wait | 2 | 282235.083 | 141117.541 | 141468.366 | 536870912 |
| transfer | relay.ddr_to_ssd | 5 | 202122.327 | 40424.465 | 42250.622 | 335544320 |
| transfer | ssd.read_wait | 7 | 199556.632 | 28508.090 | 90509.398 | 671088640 |
| acl | ssd_hbm.direct_h2d_acl_wait | 2 | 171540.602 | 85770.301 | 86014.956 | 536870912 |
| transfer | transfer.copy.ssd_to_ddr | 2 | 107391.667 | 53695.834 | 90528.109 | 335544320 |
| object | object.create | 48 | 94138.986 | 1961.229 | 9632.563 | 8858370048 |
| transfer | relay.ssd_to_ddr | 5 | 92254.779 | 18450.956 | 21635.731 | 335544320 |
| channel | channel.lane_busy | 4 | 88583.771 | 22145.943 | 41112.406 | 671088640 |
| backend | backend.ddr_prepare_fast_profile | 16 | 76839.472 | 4802.467 | 9409.824 | 2684354560 |
| backend | backend.ddr_pretouch | 16 | 76704.715 | 4794.045 | 9399.983 | 2684354560 |
| chunk | chunk.cpu_copy.memfd_to_pinned | 20 | 40708.118 | 2035.406 | 2609.066 | 335544320 |
| chunk | chunk.cpu_copy.pinned_to_memfd | 5 | 39511.360 | 7902.272 | 9316.059 | 335544320 |
| channel | direct_lane.busy | 8 | 28497.090 | 3562.136 | 6591.209 | 1342177280 |
| acl | relay.hbm_to_ddr | 5 | 19718.561 | 3943.712 | 4834.977 | 335544320 |
| acl | direct_d2h.acl_submit | 4 | 16405.188 | 4101.297 | 6523.466 | 671088640 |
| acl | relay.ddr_to_hbm | 5 | 15023.651 | 3004.730 | 3743.542 | 335544320 |
| acl | direct_h2d.acl_submit | 4 | 11503.598 | 2875.899 | 4589.397 | 671088640 |
| backend | backend.hbm_alloc | 20 | 9353.753 | 467.688 | 981.890 | 3758096384 |
| channel | channel.acquire | 4 | 4773.866 | 1193.467 | 3003.813 | 671088640 |
| channel | channel.lane_create | 2 | 4767.235 | 2383.618 | 3003.813 | 134217728 |
| acl | chunk.acl_wait.d2h | 5 | 3299.974 | 659.995 | 1618.435 | 335544320 |
| channel | direct_lane.create | 2 | 2548.674 | 1274.337 | 1368.326 | 134217728 |
| acl | chunk.acl_wait.h2d | 20 | 2111.903 | 105.595 | 662.357 | 4563402752 |
| acl | chunk.acl_submit.h2d | 20 | 1474.920 | 73.746 | 110.614 | 335544320 |
| transfer | transfer.plan | 24 | 1167.032 | 48.626 | 72.613 | 4429185024 |
| transfer | transfer.queue_wait | 24 | 320.946 | 13.373 | 18.211 | 4429185024 |
| acl | chunk.acl_submit.d2h | 5 | 303.342 | 60.668 | 100.574 | 335544320 |
| acl | direct_h2d.acl_wait | 4 | 233.751 | 58.438 | 86.544 | 671088640 |
| acl | direct_d2h.acl_wait | 4 | 185.328 | 46.332 | 50.332 | 671088640 |
| transfer | transfer.enqueue | 24 | 181.728 | 7.572 | 10.440 | 4429185024 |
| backend | ssd_hbm.direct_setup | 4 | 142.066 | 35.517 | 37.091 | 1073741824 |
| channel | direct_lane.acquire | 6 | 17.761 | 2.960 | 5.930 | 1207959552 |
| backend | backend.ddr_madvise_hugepage | 16 | 16.130 | 1.008 | 1.770 | 2684354560 |
