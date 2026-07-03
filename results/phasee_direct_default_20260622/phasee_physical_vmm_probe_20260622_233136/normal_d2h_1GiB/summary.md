# PhaseE-03 d2h Memfd Fast Path Matrix

- device: NPU7
- direction: `d2h`
- hbm_kind: `normal`
- bytes: 1073741824 (1 GiB)
- warmups: 1
- repeats: 3
- finest trace: `d2h_stage_trace.json`

| selection | status | hot avg GiB/s | hot min GiB/s | hot max GiB/s | hot avg ms | register ms | warmup ms | flags | verified | note |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| `memfd_thp` | ok | 40.430 | 40.401 | 40.451 | 24.734 | 0.000 | 25.093 | `0x0` | true |  |
| `acl_malloc_host_dma_hbm` | ok | 40.959 | 40.947 | 40.968 | 24.415 | 0.000 | 24.691 | `0x0` | true |  |
