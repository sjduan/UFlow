# UFlow Trace Summary

- schema_version: `2`
- run_id: `phasee06_trace_v2_fallback_final_20260812_019f7da1`
- mode: `full`
- events: `396`
- transfers: `14`
- complete: `true`
- raw_args_exported: `true`
- dropped_events: `0`
- incomplete_event_ids: `none`

- invalid_or_duplicate_event_ids: `none`

## Transfers

| event | direction | planned path | actual path | engine | status | bytes | latency us | fallback | span sum us |
|---:|---|---|---|---|---|---:|---:|---|---:|
| 30000 | h2d | ssd_to_hbm_direct | ssd_to_hbm_via_ddr | ssd_hbm_relay_ddr | complete | 268435456 | 93195.278 | true | 360290.031 |
| 30001 | h2d | ssd_to_hbm_direct | ssd_to_hbm_via_ddr | ssd_hbm_relay_ddr | complete | 268435456 | 72366.069 | true | 275599.080 |
| 30002 | h2d | ssd_to_hbm_direct | ssd_to_hbm_via_ddr | ssd_hbm_relay_ddr | complete | 268435456 | 57911.010 | true | 220805.958 |
| 30003 | h2d | ssd_to_hbm_direct | ssd_to_hbm_via_ddr | ssd_hbm_relay_ddr | complete | 268435456 | 56262.303 | true | 214451.778 |
| 30004 | h2d | ssd_to_hbm_direct | ssd_to_hbm_via_ddr | ssd_hbm_relay_ddr | complete | 268435456 | 55963.071 | true | 213287.170 |
| 30005 | h2d | ssd_to_hbm_direct | ssd_to_hbm_via_ddr | ssd_hbm_relay_ddr | complete | 268435456 | 55820.664 | true | 212677.916 |
| 30006 | h2d | ssd_to_hbm_direct | ssd_to_hbm_direct | ssd_hbm_file_mmap_acl_direct | failed | 0 | 137.805 | false | 270.142 |
| 30007 | d2h | hbm_to_ssd_direct | hbm_to_ssd_via_ddr | ssd_hbm_relay_ddr | complete | 268435456 | 176428.740 | true | 690471.661 |
| 30008 | d2h | hbm_to_ssd_direct | hbm_to_ssd_via_ddr | ssd_hbm_relay_ddr | complete | 268435456 | 118092.132 | true | 457735.273 |
| 30009 | d2h | hbm_to_ssd_direct | hbm_to_ssd_via_ddr | ssd_hbm_relay_ddr | complete | 268435456 | 118776.810 | true | 460463.914 |
| 30010 | d2h | hbm_to_ssd_direct | hbm_to_ssd_via_ddr | ssd_hbm_relay_ddr | complete | 268435456 | 118917.926 | true | 461052.668 |
| 30011 | d2h | hbm_to_ssd_direct | hbm_to_ssd_via_ddr | ssd_hbm_relay_ddr | complete | 268435456 | 118957.538 | true | 461106.713 |
| 30012 | d2h | hbm_to_ssd_direct | hbm_to_ssd_via_ddr | ssd_hbm_relay_ddr | complete | 268435456 | 119000.229 | true | 461300.632 |
| 30013 | d2h | hbm_to_ssd_direct | hbm_to_ssd_direct | ssd_hbm_file_mmap_acl_direct | failed | 0 | 130.575 | false | 253.680 |

## Stage compatibility summary

| category | stage | count | total us | avg us | max us | bytes |
|---|---:|---:|---:|---:|---:|---:|
| control | command.handle | 70 | 1414095.955 | 20201.371 | 175366.396 | 0 |
| transfer | transfer.worker_execute | 14 | 1162614.987 | 83043.928 | 176458.181 | 3758096384 |
| transfer | transfer.execute | 14 | 1160917.321 | 82922.666 | 176343.647 | 3758096384 |
| transfer | relay.ddr_to_ssd | 24 | 683727.432 | 28488.643 | 41489.711 | 1610612736 |
| transfer | ssd.write_wait | 24 | 683490.885 | 28478.787 | 41480.321 | 1610612736 |
| transfer | relay.ssd_to_ddr | 24 | 324872.881 | 13536.370 | 22511.598 | 1610612736 |
| transfer | ssd.read_wait | 24 | 324605.677 | 13525.237 | 22495.097 | 1610612736 |
| object | object.release | 8 | 259039.657 | 32379.957 | 73223.654 | 0 |
| backend | backend.hbm_free | 4 | 145441.507 | 36360.377 | 37400.854 | 1073741824 |
| acl | relay.hbm_to_ddr | 24 | 84341.598 | 3514.233 | 4482.123 | 1610612736 |
| acl | relay.ddr_to_hbm | 24 | 64493.407 | 2687.225 | 5459.802 | 1610612736 |
| object | object.create | 8 | 3848.545 | 481.068 | 952.689 | 2147483648 |
| backend | backend.hbm_alloc | 4 | 1902.327 | 475.582 | 497.790 | 1073741824 |
| transfer | transfer.plan | 14 | 744.148 | 53.153 | 67.542 | 3758096384 |
| backend | ssd_hbm.direct_setup | 14 | 395.106 | 28.222 | 37.941 | 3758096384 |
| transfer | transfer.queue_wait | 14 | 211.459 | 15.104 | 16.991 | 3758096384 |
| transfer | transfer.enqueue | 14 | 95.863 | 6.847 | 7.570 | 3758096384 |
| object | object.mark_ready | 4 | 0.000 | 0.000 | 0.000 | 0 |
| transfer | ssd_hbm.direct_failpoint | 14 | 0.000 | 0.000 | 0.000 | 3758096384 |
| transfer | transfer.complete | 14 | 0.000 | 0.000 | 0.000 | 3758096384 |
| transfer | transfer.outcome | 14 | 0.000 | 0.000 | 0.000 | 3758096384 |
| transfer | transfer.plan_outcome | 14 | 0.000 | 0.000 | 0.000 | 3758096384 |
| transfer | transfer.submit | 14 | 0.000 | 0.000 | 0.000 | 3758096384 |
