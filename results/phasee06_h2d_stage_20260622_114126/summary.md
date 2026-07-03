# PhaseE-06 H2D Native Per-Stage Timestamp

- device: NPU7
- bytes: 2 GiB
- finest trace: `h2d_stage_trace.json`

| selection | status | hot GiB/s | wall ms | setup ms | CPU copy ms | ACL submit ms | ACL wait ms | register ms | unregister ms | chunks | verified |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `memfd_pinned_chunk_hbm` | ok | 18.037 | 110.885 | 966.705 | 99.826 | 5.539 | 4.266 | 0.000 | 0.000 | 128 | true |
| `memfd_registered_hbm` | ok | 7.771 | 257.351 | 1695.152 | 0.000 | 0.072 | 257.269 | 805.256 | 91.920 | 1 | true |
| `acl_malloc_host_dma_hbm` | ok | 56.206 | 35.584 | 207.884 | 0.000 | 0.081 | 35.487 | 0.000 | 0.000 | 1 | true |
