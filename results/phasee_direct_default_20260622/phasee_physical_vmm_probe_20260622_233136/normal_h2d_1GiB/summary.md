# PhaseE-03 h2d Memfd Fast Path Matrix

- device: NPU7
- direction: `h2d`
- hbm_kind: `normal`
- bytes: 1073741824 (1 GiB)
- warmups: 1
- repeats: 3
- finest trace: `h2d_stage_trace.json`

| selection | status | hot avg GiB/s | hot min GiB/s | hot max GiB/s | hot avg ms | register ms | warmup ms | flags | verified | note |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| `memfd_thp` | ok | 56.022 | 55.900 | 56.205 | 17.850 | 0.000 | 18.025 | `0x0` | true |  |
| `acl_malloc_host_dma_hbm` | ok | 56.423 | 56.317 | 56.548 | 17.723 | 0.000 | 17.695 | `0x0` | true |  |
