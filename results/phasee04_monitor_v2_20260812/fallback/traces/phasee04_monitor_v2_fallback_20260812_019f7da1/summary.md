# UFlow Trace Summary

- schema_version: `2`
- run_id: `phasee04_monitor_v2_fallback_20260812_019f7da1`
- mode: `full`
- events: `138`
- transfers: `4`
- complete: `true`
- raw_args_exported: `true`
- dropped_events: `0`
- incomplete_event_ids: `none`

- invalid_or_duplicate_event_ids: `none`

## Transfers

| event | direction | planned path | actual path | engine | status | bytes | latency us | fallback | span sum us |
|---:|---|---|---|---|---|---:|---:|---|---:|
| 30000 | h2d | ssd_to_hbm_direct | ssd_to_hbm_via_ddr | ssd_hbm_relay_ddr | complete | 268435456 | 76017.859 | true | 292051.324 |
| 30001 | h2d | ssd_to_hbm_direct | ssd_to_hbm_direct | ssd_hbm_file_mmap_acl_direct | failed | 0 | 103.904 | false | 180.347 |
| 30002 | d2h | hbm_to_ssd_direct | hbm_to_ssd_via_ddr | ssd_hbm_relay_ddr | complete | 268435456 | 170693.887 | true | 666910.202 |
| 30003 | d2h | hbm_to_ssd_direct | hbm_to_ssd_direct | ssd_hbm_file_mmap_acl_direct | failed | 0 | 92.183 | false | 175.796 |

## Stage compatibility summary

| category | stage | count | total us | avg us | max us | bytes |
|---|---:|---:|---:|---:|---:|---:|
| control | command.handle | 42 | 453307.254 | 10793.030 | 169635.924 | 0 |
| transfer | transfer.worker_execute | 4 | 246982.136 | 61745.534 | 170726.088 | 1073741824 |
| transfer | transfer.execute | 4 | 246638.232 | 61659.558 | 170626.974 | 1073741824 |
| object | object.release | 8 | 203927.059 | 25490.882 | 41636.687 | 0 |
| transfer | relay.ddr_to_ssd | 4 | 155401.543 | 38850.386 | 39038.411 | 268435456 |
| transfer | ssd.write_wait | 4 | 155366.062 | 38841.516 | 39030.071 | 268435456 |
| backend | backend.hbm_free | 4 | 140881.422 | 35220.356 | 35355.851 | 1073741824 |
| transfer | relay.ssd_to_ddr | 4 | 64632.065 | 16158.016 | 17145.189 | 268435456 |
| transfer | ssd.read_wait | 4 | 64593.412 | 16148.353 | 17134.088 | 268435456 |
| acl | relay.hbm_to_ddr | 4 | 14768.083 | 3692.021 | 4695.271 | 268435456 |
| acl | relay.ddr_to_hbm | 4 | 10801.010 | 2700.253 | 3088.626 | 268435456 |
| object | object.create | 8 | 3961.799 | 495.225 | 938.569 | 2147483648 |
| backend | backend.hbm_alloc | 4 | 1942.259 | 485.565 | 488.110 | 1073741824 |
| transfer | transfer.plan | 4 | 214.859 | 53.715 | 72.153 | 1073741824 |
| backend | ssd_hbm.direct_setup | 4 | 75.783 | 18.946 | 26.481 | 1073741824 |
| transfer | transfer.queue_wait | 4 | 35.972 | 8.993 | 12.711 | 1073741824 |
| transfer | transfer.enqueue | 4 | 23.371 | 5.843 | 7.690 | 1073741824 |
| object | object.mark_ready | 4 | 0.000 | 0.000 | 0.000 | 0 |
| transfer | ssd_hbm.direct_failpoint | 4 | 0.000 | 0.000 | 0.000 | 1073741824 |
| transfer | transfer.complete | 4 | 0.000 | 0.000 | 0.000 | 1073741824 |
| transfer | transfer.outcome | 4 | 0.000 | 0.000 | 0.000 | 1073741824 |
| transfer | transfer.plan_outcome | 4 | 0.000 | 0.000 | 0.000 | 1073741824 |
| transfer | transfer.submit | 4 | 0.000 | 0.000 | 0.000 | 1073741824 |
