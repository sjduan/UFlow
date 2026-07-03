# PhaseE-03 h2d Memfd Fast Path Matrix

- device: NPU7
- direction: `h2d`
- hbm_kind: `physical`
- bytes: 1073741824 (1 GiB)
- warmups: 1
- repeats: 3
- finest trace: `h2d_stage_trace.json`

| selection | status | hot avg GiB/s | hot min GiB/s | hot max GiB/s | hot avg ms | register ms | warmup ms | flags | verified | note |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| `memfd_thp` | ok | 56.102 | 55.997 | 56.160 | 17.825 | 0.000 | 18.006 | `0x0` | true |  |
| `acl_malloc_host_dma_hbm` | ok | 56.442 | 56.373 | 56.541 | 17.717 | 0.000 | 17.672 | `0x0` | true |  |
