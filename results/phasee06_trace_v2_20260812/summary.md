# PhaseE-06 Trace v2 evidence index

Date: 2026-08-12
Status: PASS; E06-T1～T6 and G0～G10 closed
Final implementation commit: `5f47d22983fe6d932d892144da7bf3c881bf55e2`

## Environment

- `ssh serving-b` → `openeuler-2403-DS`
- Host: `liteserver-560t-1.novalocal`
- Container ID: `68527857ede227ab53199265eb6ff41e51314a5dba28f9b78d7c072a30a273dd`
- Image: `image-transfer/old2-openeuler-2403-ds:20260706_194906`
- Kernel: `5.10.0-182.0.0.95.r2220_157.hce2.aarch64`
- Device: 7, selected from the run-time `npu-smi` snapshot; all devices healthy and unused
- Isolated tree: `/tmp/phasee06_v2_stage_019f7da1`; remote dirty canonical repo was not modified

## Gate summary

| Gate | Result | Evidence |
|---|---|---|
| G0 | PASS | remote cargo fmt/check/test; 8 Rust tests; 7 Python tests; shell syntax |
| G1 | PASS | 120 baseline + 120 candidate measured transfers; no trace files; matrix p50 overhead `-0.083818%` vs 3% threshold |
| G2-G6 | PASS | regular matrix: 144 transfers, 3144 events, zero incomplete/drop/mismatch |
| G7 | PASS | fallback matrix: 14 transfers, 396 events; auto runtime fallback succeeds and explicit direct fails |
| G8 | PASS | unique failed/cancelled outcomes; incomplete and drop checkers fail as required |
| G9 | PASS (automated contract) | HTTP start/status/flush/stop; 440 status/artifact events; five HTTP downloads are byte-identical |
| G10 | PASS | no scoped process, socket, DDR root or SSD root remains |

## Canonical runs

- `phasee06_v2_disabled_baseline2_20260812_019f7da1`
- `phasee06_v2_disabled_candidate2_20260812_019f7da1`
- `phasee06_trace_v2_regular_final_20260812_019f7da1`
- `phasee06_trace_v2_fallback_final_20260812_019f7da1`
- `phasee06_trace_v2_cancel_final_20260812_019f7da1`
- `phasee06_trace_v2_incomplete_stop_final_20260812_019f7da1`
- `phasee06_trace_v2_drop_final_20260812_019f7da1`
- `phasee06_trace_v2_http_stop_final_20260812_019f7da1`

The earlier `http_final` and `incomplete_final` directories are retained as transparent diagnostic evidence. They were superseded by the `*_stop_final` reruns after fixing the post-stop late-span boundary.

Each trace-on run contains the five trace artifacts, PollEvent samples, checker output, control snapshots, daemon/API logs, and the live environment snapshot. Expected-negative `incomplete` and `drop` reports intentionally have checker status `FAIL`; their runner-level gate is PASS because the failure was detected and `summary.complete=false`.

Automated validation covers standard JSON parsing, Chrome Trace Event `X/i` phases, monotonic timestamps, per-event stage roles, PollEvent/raw/JSON/CSV consistency, and byte-identical HTTP downloads. No trace was uploaded to an external viewer; a manual Chrome/Perfetto screenshot is optional follow-up evidence, not a data-path blocker.
