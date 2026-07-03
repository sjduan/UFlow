# PhaseE-03 d2h Memfd Fast Path Matrix

- device: NPU7
- direction: `d2h`
- hbm_kind: `physical`
- bytes: 1073741824 (1 GiB)
- warmups: 1
- repeats: 3
- finest trace: `d2h_stage_trace.json`

| selection | status | hot avg GiB/s | hot min GiB/s | hot max GiB/s | hot avg ms | register ms | warmup ms | flags | verified | note |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| `memfd_thp` | ok | 40.450 | 40.429 | 40.462 | 24.722 | 0.000 | 25.111 | `0x0` | true |  |
| `acl_malloc_host_dma_hbm` | ok | 40.919 | 40.832 | 40.965 | 24.438 | 0.000 | 24.664 | `0x0` | true |  |
