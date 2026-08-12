# UFlow Trace Summary

- schema_version: `2`
- run_id: `phasee06_trace_v2_drop_final_20260812_019f7da1`
- mode: `full`
- events: `8`
- transfers: `0`
- complete: `false`
- raw_args_exported: `true`
- dropped_events: `37`
- incomplete_event_ids: `none`

- invalid_or_duplicate_event_ids: `none`

## Transfers

| event | direction | planned path | actual path | engine | status | bytes | latency us | fallback | span sum us |
|---:|---|---|---|---|---|---:|---:|---|---:|

## Stage compatibility summary

| category | stage | count | total us | avg us | max us | bytes |
|---|---:|---:|---:|---:|---:|---:|
| control | command.handle | 2 | 2024.912 | 1012.456 | 2011.952 | 0 |
| object | object.create | 1 | 2007.572 | 2007.572 | 2007.572 | 67108864 |
| backend | backend.ddr_prepare_fast_profile | 1 | 1808.034 | 1808.034 | 1808.034 | 67108864 |
| backend | backend.ddr_pretouch | 1 | 1800.043 | 1800.043 | 1800.043 | 67108864 |
| backend | backend.hbm_alloc | 1 | 335.843 | 335.843 | 335.843 | 67108864 |
| backend | backend.ddr_madvise_hugepage | 1 | 1.070 | 1.070 | 1.070 | 67108864 |
| object | object.mark_ready | 1 | 0.000 | 0.000 | 0.000 | 0 |
