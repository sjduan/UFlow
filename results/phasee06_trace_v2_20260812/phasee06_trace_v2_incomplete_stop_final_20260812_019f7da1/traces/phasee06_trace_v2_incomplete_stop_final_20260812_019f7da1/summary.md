# UFlow Trace Summary

- schema_version: `2`
- run_id: `phasee06_trace_v2_incomplete_stop_final_20260812_019f7da1`
- mode: `full`
- events: `14`
- transfers: `1`
- complete: `false`
- raw_args_exported: `true`
- dropped_events: `0`
- incomplete_event_ids: `30000`

- invalid_or_duplicate_event_ids: `none`

## Transfers

| event | direction | planned path | actual path | engine | status | bytes | latency us | fallback | span sum us |
|---:|---|---|---|---|---|---:|---:|---|---:|
| 30000 | h2d | ssd_to_hbm_via_ddr |  |  |  |  |  |  | 15.642 |

## Stage compatibility summary

| category | stage | count | total us | avg us | max us | bytes |
|---|---:|---:|---:|---:|---:|---:|
| control | command.handle | 5 | 1389.346 | 277.869 | 879.266 | 0 |
| object | object.create | 2 | 1229.110 | 614.555 | 876.695 | 536870912 |
| backend | backend.hbm_alloc | 1 | 830.894 | 830.894 | 830.894 | 268435456 |
| transfer | transfer.plan | 1 | 57.232 | 57.232 | 57.232 | 268435456 |
| transfer | transfer.queue_wait | 1 | 9.371 | 9.371 | 9.371 | 268435456 |
| transfer | transfer.enqueue | 1 | 6.271 | 6.271 | 6.271 | 268435456 |
| object | object.mark_ready | 1 | 0.000 | 0.000 | 0.000 | 0 |
| transfer | transfer.plan_outcome | 1 | 0.000 | 0.000 | 0.000 | 268435456 |
| transfer | transfer.submit | 1 | 0.000 | 0.000 | 0.000 | 268435456 |
