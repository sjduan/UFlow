# PhaseE-04 Monitor v2 validation summary

Date: 2026-08-12

Status: implementation and serving-b functional validation pass; current-session browser screenshots pending because the Codex in-app browser blocked navigation to the loopback URL.

## Baseline and environment

- Local implementation commit: `3e932b16fca43750526b1bd6e2a64cbdd2fcdbdd` (implementation chain starts at `1f9749f`).
- Remote execution: `ssh serving-b` → `openeuler-2403-DS`.
- Host: `liteserver-560t-1.novalocal`.
- Kernel: `5.10.0-182.0.0.95.r2220_157.hce2.aarch64`.
- Selected device: `7`, chosen from the live `npu-smi info` snapshot.
- Canonical remote repo: `/home/sj/git/data-service@e854fa0`, dirty and deliberately untouched.
- Isolated validation tree: `/tmp/phasee04_v2_stage_019f7da1`.
- Release daemon: `/tmp/phasee04_v2_target_019f7da1/release/uf-daemon`.

## Automated results

| Gate | Result | Evidence |
|---|---|---|
| G0 format/build/unit contracts | PASS | `g0/g0_validation.txt`: Rust fmt/check/test, 10 daemon tests, 6 Monitor API tests, 10 Trace/cleanup tests, 3 UI contract tests, Python/JS/Bash syntax |
| G1 v1 compatibility | FUNCTIONAL PASS; screenshot pending | Monitor API v1 compatibility unit test plus UI N/A contract test |
| G2 HBM↔DDR | FUNCTIONAL PASS; screenshot pending | regular run: auto/direct_async/pinned_async, H2D+D2H, 64MiB+256MiB |
| G3 SSD↔DDR | FUNCTIONAL PASS; screenshot pending | regular run: buffered read/write, 64MiB+256MiB |
| G4 SSD↔HBM relay | FUNCTIONAL PASS; screenshot pending | regular run: explicit relay, H2D+D2H, two relay stages |
| G5 SSD↔HBM logical direct | FUNCTIONAL PASS; screenshot pending | regular run: explicit+auto, H2D+D2H, `direct_kind=logical`; UI label contract rejects physical wording |
| G6 runtime fallback | FUNCTIONAL PASS; screenshot pending | fallback run: planned direct→actual relay in both directions with validation-only failpoint and explicit reason |
| G7 Trace linkage | FUNCTIONAL PASS; screenshot pending | both profiles: start→transfer→stop/flush; five artifacts downloaded over HTTP; zero drop/mismatch |
| G8 environment/lifecycle/cleanup | FUNCTIONAL PASS; stale/offline screenshot pending | single-alias preflight, helper start/status/reconnect/stop-local, remote survives local disconnect, both cleanup reports PASS |

The task's visual evidence requirement remains open until the page is opened by the user and the current tab is explicitly shared with Codex; the in-app browser rejected direct loopback navigation and its policy forbids an automated workaround.

## Regular profile

- Run id: `phasee04_monitor_v2_regular_20260812_019f7da1`.
- Transfers: `24`, all complete.
- Trace events: `841`; dropped: `0`; incomplete transfers: `0`; event/trace mismatches: `0`.
- Summary groups: `12`; runtime fallback count: `0`.
- Covered paths:
  - DDR↔HBM `auto` and `direct_async` → `*_direct_thp`.
  - DDR↔HBM `pinned_async` → `memfd_pinned_hbm_channel`.
  - SSD↔DDR buffered read/write at 64MiB and 256MiB.
  - SSD↔HBM explicit relay in both directions, `relay_stage_count=2`.
  - SSD↔HBM explicit and automatic logical direct in both directions at 256MiB.
- All five trace artifacts returned HTTP 200.
- Cleanup: no remaining process, socket, DDR root, or SSD root.

## Fallback profile

- Run id: `phasee04_monitor_v2_fallback_20260812_019f7da1`.
- Transfers: `4`: two expected explicit-direct failures plus two successful auto runtime fallbacks.
- Successful fallback records: planned logical direct → actual relay, `fallback_used=true`, reason includes `validation_failpoint:ssd_direct_before_acl`.
- Trace events: `138`; dropped: `0`; incomplete transfers: `0`; event/trace mismatches: `0`.
- Summary groups: `4`; runtime fallback count: `2`.
- All five trace artifacts returned HTTP 200.
- Cleanup: no remaining process, socket, DDR root, or SSD root.

## Lifecycle result

The local helper used the required single alias and tunnel options. `start` and `status` returned healthy `/healthz`, `/v1/config`, `/v1/stats`, and `/v1/transfers?limit=1`. After the local UI/tunnel sessions were stopped, the remote API still returned HTTP 200 and the daemon remained reachable. `reconnect` restored the local API path. `stop-local` reported that the remote API and daemon were untouched. Final cleanup left port 8082 free and no matching test process.

## Evidence index

- `preflight_final.txt`: host/container/canonical-repo and final cleanup state.
- `g0/g0_validation.txt`: complete static/build/unit test log.
- `regular/`: API snapshots, bounded transfers and summary, object backing-path check, Trace v2 check, five downloaded trace artifacts, lifecycle and cleanup evidence.
- `fallback/`: runtime fallback equivalents and cleanup evidence.
