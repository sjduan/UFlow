# UFlow Trace Summary

- schema_version: `2`
- run_id: `phasee06_trace_v2_cancel_final_20260812_019f7da1`
- mode: `full`
- events: `81`
- transfers: `2`
- complete: `true`
- raw_args_exported: `true`
- dropped_events: `0`
- incomplete_event_ids: `none`

- invalid_or_duplicate_event_ids: `none`

## Transfers

| event | direction | planned path | actual path | engine | status | bytes | latency us | fallback | span sum us |
|---:|---|---|---|---|---|---:|---:|---|---:|
| 30000 | h2d | ssd_to_hbm_via_ddr | ssd_to_hbm_via_ddr | ssd_hbm_relay_ddr | complete | 268435456 | 76615.312 | false | 294316.153 |
| 30001 | h2d | ssd_to_hbm_via_ddr | ssd_to_hbm_via_ddr | ssd_hbm_relay_ddr | cancelled | 0 | 1071.784 | false | 364601.600 |

## Stage compatibility summary

| category | stage | count | total us | avg us | max us | bytes |
|---|---:|---:|---:|---:|---:|---:|
| control | command.handle | 25 | 192449.566 | 7697.983 | 73351.379 | 0 |
| transfer | transfer.worker_execute | 2 | 151609.828 | 75804.914 | 76641.843 | 536870912 |
| transfer | transfer.execute | 2 | 151485.514 | 75742.757 | 76547.640 | 536870912 |
| transfer | relay.ssd_to_ddr | 8 | 129720.996 | 16215.124 | 17597.917 | 536870912 |
| transfer | ssd.read_wait | 8 | 129653.144 | 16206.643 | 17588.376 | 536870912 |
| object | object.release | 4 | 114493.465 | 28623.366 | 36341.621 | 0 |
| transfer | transfer.queue_wait | 2 | 75557.589 | 37778.795 | 75547.819 | 536870912 |
| backend | backend.hbm_free | 2 | 71651.020 | 35825.510 | 36316.410 | 536870912 |
| acl | relay.ddr_to_hbm | 8 | 20883.712 | 2610.464 | 3475.401 | 536870912 |
| object | object.create | 4 | 1500.302 | 375.076 | 508.391 | 1073741824 |
| backend | backend.hbm_alloc | 2 | 843.945 | 421.972 | 461.689 | 536870912 |
| transfer | transfer.plan | 2 | 94.894 | 47.447 | 54.332 | 536870912 |
| transfer | transfer.enqueue | 2 | 6.970 | 3.485 | 5.930 | 536870912 |
| object | object.mark_ready | 2 | 0.000 | 0.000 | 0.000 | 0 |
| transfer | transfer.complete | 2 | 0.000 | 0.000 | 0.000 | 536870912 |
| transfer | transfer.outcome | 2 | 0.000 | 0.000 | 0.000 | 536870912 |
| transfer | transfer.plan_outcome | 2 | 0.000 | 0.000 | 0.000 | 536870912 |
| transfer | transfer.submit | 2 | 0.000 | 0.000 | 0.000 | 536870912 |
