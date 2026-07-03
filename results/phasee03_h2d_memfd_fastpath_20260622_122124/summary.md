# PhaseE-03 H2D Memfd Fast Path Matrix

- device: NPU7
- bytes: 2147483648 (2 GiB)
- warmups: 1
- repeats: 3
- finest trace: `h2d_stage_trace.json`

| selection | status | hot avg GiB/s | hot min GiB/s | hot max GiB/s | hot avg ms | register ms | warmup ms | flags | verified | note |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| `memfd_direct` | ok | 21.540 | 20.373 | 22.333 | 92.850 | 0.000 | 827.080 | `0x0` | true |  |
| `memfd_pretouch` | ok | 24.782 | 23.358 | 25.690 | 80.705 | 0.000 | 42.741 | `0x0` | true |  |
| `memfd_mlock` | skipped | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | `0x0` | false | RLIMIT_MEMLOCK soft limit 67108864 < bytes 2147483648 |
| `memfd_thp` | ok | 54.696 | 52.749 | 55.787 | 36.566 | 0.000 | 35.938 | `0x0` | true |  |
| `memfd_v2_pinned` | ok | 19.779 | 11.933 | 30.144 | 101.116 | 0.020 | 857.108 | `0x10000000` | true |  |
| `memfd_v2_mapped` | ok | 17.364 | 12.752 | 21.695 | 115.181 | 1400.865 | 93.674 | `0x2` | true |  |
| `memfd_v2_mapped_pinned` | ok | 20.604 | 12.048 | 32.019 | 97.070 | 1355.946 | 67.578 | `0x10000002` | true |  |
| `memfd_mlock_v2_pinned` | skipped | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | `0x10000000` | false | RLIMIT_MEMLOCK soft limit 67108864 < bytes 2147483648 |
| `memfd_thp_v2_pinned` | ok | 36.216 | 30.895 | 40.492 | 55.225 | 0.020 | 54.183 | `0x10000000` | true |  |
| `hugetlb_memfd_v2_pinned` | skipped | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | `0x10000000` | false | mmap hugetlb memfd failed errno=12 |
| `acl_malloc_host_dma_hbm` | ok | 56.251 | 56.219 | 56.276 | 35.555 | 0.000 | 35.604 | `0x0` | true |  |
| `memfd_pinned_chunk_hbm` | ok | 7.752 | 7.475 | 8.046 | 258.009 | 0.000 | 1223.688 | `0x0` | true |  |
