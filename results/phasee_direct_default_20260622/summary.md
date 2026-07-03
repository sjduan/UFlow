# PhaseE Direct DDR-HBM Default Path Summary

Date: 2026-06-22

Remote validation: 2号机 `openeuler-2403-DS`, NPU7.

## Standalone ACL Baseline

Run dir: `/tmp/proj_output/phasee03_direct_fastpath_matrix_20260622_232500`

Local CSV: `phasee03_direct_fastpath_matrix_20260622_232500/matrix_summary.csv`

| size | H2D `memfd_thp` | H2D baseline | H2D ratio | D2H `memfd_thp` | D2H baseline | D2H ratio |
|---:|---:|---:|---:|---:|---:|---:|
| 64MiB | 50.745 GiB/s | 53.338 GiB/s | 95.1% | 37.985 GiB/s | 38.824 GiB/s | 97.8% |
| 256MiB | 54.437 GiB/s | 55.481 GiB/s | 98.1% | 40.392 GiB/s | 40.768 GiB/s | 99.1% |
| 1GiB | 55.985 GiB/s | 56.418 GiB/s | 99.2% | 40.398 GiB/s | 40.951 GiB/s | 98.6% |
| 2GiB | 57.085 GiB/s | 57.186 GiB/s | 99.8% | 41.306 GiB/s | 41.364 GiB/s | 99.9% |

## Daemon Default Policy

Run dir: `/tmp/proj_output/phasee05_default_direct_matrix_20260622_234108`

Local CSV: `phasee05_default_direct_matrix_20260622_234108/daemon_matrix_summary.csv`

Default mode is now `auto -> acl_direct_async_thp`. Explicit `mode=pinned_async` remains the pinned staging fallback.

| size | direction | actual path | direct channel | pinned fallback |
|---:|---|---|---:|---:|
| 64MiB | H2D | `ddr_hbm_direct_thp` | 42.430 GiB/s | 12.884 GiB/s |
| 64MiB | D2H | `hbm_ddr_direct_thp` | 33.897 GiB/s | 12.389 GiB/s |
| 256MiB | H2D | `ddr_hbm_direct_thp` | 52.690 GiB/s | 16.506 GiB/s |
| 256MiB | D2H | `hbm_ddr_direct_thp` | 38.170 GiB/s | 14.937 GiB/s |
| 1GiB | H2D | `ddr_hbm_direct_thp` | 55.025 GiB/s | 19.211 GiB/s |
| 1GiB | D2H | `hbm_ddr_direct_thp` | 39.974 GiB/s | 15.809 GiB/s |
| 2GiB | H2D | `ddr_hbm_direct_thp` | 55.420 GiB/s | 19.560 GiB/s |
| 2GiB | D2H | `hbm_ddr_direct_thp` | 40.318 GiB/s | 15.993 GiB/s |

## Notes

- DDR object default is `memfd + daemon-owned mmap + MADV_HUGEPAGE + pre-touch`.
- HBM physical/VMM probe confirmed physical HBM is not the bottleneck.
- 64MiB D2H reaches 87.3% of pure ACL baseline on channel timing; 256MiB and larger meet the >=90% acceptance threshold.
