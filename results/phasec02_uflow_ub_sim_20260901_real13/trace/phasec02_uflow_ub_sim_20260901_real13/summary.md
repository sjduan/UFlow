# UFlow Trace Summary

- schema_version: `2`
- run_id: `phasec02_uflow_ub_sim_20260901_real13`
- mode: `full`
- events: `21`
- transfers: `0`
- complete: `true`
- raw_args_exported: `true`
- dropped_events: `0`
- incomplete_event_ids: `none`

- invalid_or_duplicate_event_ids: `none`

## Transfers

| event | direction | planned path | actual path | engine | status | bytes | latency us | fallback | span sum us |
|---:|---|---|---|---|---|---:|---:|---|---:|

## Stage compatibility summary

| category | stage | count | total us | avg us | max us | bytes |
|---|---:|---:|---:|---:|---:|---:|
| control | command.handle | 11 | 333261.767 | 30296.524 | 277501.995 | 0 |
| backend | provider.remote_map.checksum | 2 | 277496.555 | 138748.277 | 277491.334 | 8192 |
| backend | provider.remote_map.import | 1 | 26315.297 | 26315.297 | 26315.297 | 4096 |
| object | provider.data_object.create | 1 | 15502.575 | 15502.575 | 15502.575 | 4096 |
| backend | provider.data_object.release | 1 | 8924.976 | 8924.976 | 8924.976 | 0 |
| backend | provider.remote_map.revoke | 1 | 3394.259 | 3394.259 | 3394.259 | 0 |
| backend | provider.publish | 1 | 1281.292 | 1281.292 | 1281.292 | 4096 |
| backend | backend.startup_hbm_probe | 1 | 28.131 | 28.131 | 28.131 | 1048576 |
| backend | provider.data_object.ready | 1 | 0.000 | 0.000 | 0.000 | 0 |
| backend | provider.remote_map.ready | 1 | 0.000 | 0.000 | 0.000 | 0 |
