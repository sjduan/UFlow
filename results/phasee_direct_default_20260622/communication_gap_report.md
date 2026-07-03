# UFlow DDR-HBM Communication Gap Analysis

Date: 2026-06-30

Scope: based on existing PhaseE-03 / PhaseE-05 / PhaseE-07 data only. No new benchmark was run.

## 1. 结论摘要

当前 UFlow 的 DDR-HBM 通信已经分成三层口径：

1. **ACL 理想上限**：`aclrtMallocHost <-> HBM` pure ACL async baseline。
2. **shared DDR 理想形态**：standalone `memfd + MADV_HUGEPAGE + pre-touch <-> HBM`。
3. **UFlow daemon 默认路径**：`CreateDataObject(ddr)` 生成 memfd，daemon 保存 service VA，`PlanTransfer/SubmitTransfer auto -> acl_direct_async_thp`。

核心结论：

- shared DDR 本身已经接近理想：standalone `memfd_thp` 在 1GiB/2GiB 上基本达到 ACL baseline 的 **98.6%-99.9%**。
- UFlow daemon 的 **channel 热路径** 在 256MiB 以上已经接近理想：H2D 为 **95.0%-97.5%**，D2H 为 **93.6%-97.6%**。
- 当前最大差距在小对象和服务调度口径：
  - 64MiB H2D channel 只有 baseline 的 **79.5%**。
  - 64MiB D2H channel 只有 baseline 的 **87.3%**。
  - daemon TransferEvent 口径每次 transfer 还有约 **1.5-2.1ms** 固定服务开销。
- pinned staging 已经不应作为默认通信路径：大对象 H2D 只有 baseline 的约 **34%**，D2H 约 **39%**。

## 2. Standalone shared DDR 距理想差距

理想 baseline 是 `acl_malloc_host_dma_hbm`。单位为 GiB/s。

| size | direction | baseline | `memfd_thp` | ratio | gap |
|---:|---|---:|---:|---:|---:|
| 64MiB | H2D | 53.338 | 50.745 | 95.1% | 4.9% |
| 64MiB | D2H | 38.824 | 37.985 | 97.8% | 2.2% |
| 256MiB | H2D | 55.481 | 54.437 | 98.1% | 1.9% |
| 256MiB | D2H | 40.768 | 40.392 | 99.1% | 0.9% |
| 1GiB | H2D | 56.418 | 55.985 | 99.2% | 0.8% |
| 1GiB | D2H | 40.951 | 40.398 | 98.6% | 1.4% |
| 2GiB | H2D | 57.186 | 57.085 | 99.8% | 0.2% |
| 2GiB | D2H | 41.364 | 41.306 | 99.9% | 0.1% |

判断：**通信介质选型已经对了**。真正的 shared DDR fast path 是 `memfd + THP + active service VA`，它不是普通 `/dev/shm` path file，也不是临时 mmap 后 madvise 再 unmap。

## 3. UFlow daemon channel 热路径距理想差距

这里看的是 daemon 内真正执行 direct ACL copy 的 channel 口径，排除 client 侧测试脚本的外层等待。

| size | direction | baseline | daemon channel | ratio | gap |
|---:|---|---:|---:|---:|---:|
| 64MiB | H2D | 53.338 | 42.430 | 79.5% | 20.5% |
| 64MiB | D2H | 38.824 | 33.897 | 87.3% | 12.7% |
| 256MiB | H2D | 55.481 | 52.690 | 95.0% | 5.0% |
| 256MiB | D2H | 40.768 | 38.170 | 93.6% | 6.4% |
| 1GiB | H2D | 56.418 | 55.025 | 97.5% | 2.5% |
| 1GiB | D2H | 40.951 | 39.974 | 97.6% | 2.4% |
| 2GiB | H2D | 57.186 | 55.420 | 96.9% | 3.1% |
| 2GiB | D2H | 41.364 | 40.318 | 97.5% | 2.5% |

判断：

- 256MiB 以上已经进入可接受区间。
- 64MiB 未完全达标，本质是固定开销被小数据量放大。
- 2GiB 的 H2D/D2H 仍有 2.5%-3.1% gap，属于 daemon 抽象层和计时口径引入的剩余损耗。

## 4. 固定开销分解

按 baseline 带宽反推理想耗时，再与 daemon channel / daemon wall 对比。

| size | direction | ideal hot | channel wall | channel extra | daemon wall | daemon event extra |
|---:|---|---:|---:|---:|---:|---:|
| 64MiB | H2D | 1.172ms | 1.473ms | +0.301ms | 3.544ms | +2.071ms |
| 64MiB | D2H | 1.610ms | 1.844ms | +0.234ms | 3.398ms | +1.554ms |
| 256MiB | H2D | 4.506ms | 4.745ms | +0.239ms | 6.684ms | +1.939ms |
| 256MiB | D2H | 6.132ms | 6.550ms | +0.418ms | 8.369ms | +1.819ms |
| 1GiB | H2D | 17.725ms | 18.174ms | +0.449ms | 20.162ms | +1.988ms |
| 1GiB | D2H | 24.419ms | 25.016ms | +0.597ms | 27.001ms | +1.985ms |
| 2GiB | H2D | 34.973ms | 36.088ms | +1.115ms | 38.217ms | +2.129ms |
| 2GiB | D2H | 48.351ms | 49.605ms | +1.254ms | 51.360ms | +1.755ms |

这说明现在有两类损耗：

1. **channel 内部损耗**：约 0.2-1.3ms，来自 daemon direct copy 实现层。
2. **TransferEvent 服务损耗**：约 1.5-2.1ms，来自提交、线程执行、catalog/event 更新、stats/trace 记账等服务框架成本。

64MiB 的真实 copy 只需要 1-2ms，所以固定服务成本看起来很重；1GiB/2GiB 时固定成本被摊薄，因此接近理想。

## 5. 差距具体在哪里

### 5.1 已解决的问题

以前慢的主要原因不是 HBM physical/VMM，也不是 CANN 不支持 direct async，而是 DDR object 形态不对：

- 普通 `/dev/shm` path file direct 路径只有约 5-8 GiB/s。
- 临时 mmap 上 `madvise(MADV_HUGEPAGE)` 后 unmap，transfer 时重新 mmap，不能保证 active VMA 继续走 fast path。
- 当前修正为 daemon 持有 memfd fd 和 service mmap VA，创建时完成 THP/pre-touch，transfer 时复用同一 active VA。

### 5.2 仍存在的 channel 内差距

当前 direct path 每次 transfer 都会：

- 创建 ACL stream。
- 创建 ACL event。
- submit async memcpy。
- synchronize event。
- 记录 stats/trace 字段。

这些动作在大对象上影响很小，但在 64MiB 上会直接吃掉 10%-20% 的可见带宽。

### 5.3 仍存在的 daemon 服务层差距

TransferEvent 口径比 channel 口径多约 1.5-2.1ms。这部分不是 HBM-DDR DMA 本身，而是 UFlow 作为服务的框架成本：

- `SubmitTransfer` 后创建/调度 transfer work。
- worker thread 执行。
- catalog/event 状态更新。
- event completion 写回。
- SDK/daemon protocol 往返的一部分时间。

如果未来 PyPTO 需要按 layer 切很细的 transfer，这个固定成本会影响 pipeline 粒度选择。

### 5.4 pinned staging 的差距

pinned staging fallback 大对象表现：

| size | H2D pinned / baseline | D2H pinned / baseline |
|---:|---:|---:|
| 1GiB | 34.1% | 38.6% |
| 2GiB | 34.2% | 38.7% |

原因很明确：它多了一跳 `memfd <-> pinned chunk` CPU copy。即使 ACL pinned DMA 很快，端到端仍被 CPU copy 和 chunk 调度压住。

## 6. 与理想状态的距离

如果把“理想状态”定义为 pure ACL baseline：

- shared DDR 数据结构层面：距离理想 **0.1%-2%**，基本达成。
- daemon 大对象 hot path：距离理想 **2.4%-6.4%**，基本可用。
- daemon 小对象 hot path：距离理想 **12.7%-20.5%**，需要优化固定开销。
- daemon TransferEvent 端到端：每次 transfer 多 **1.5-2.1ms**，是接 PyPTO layer pipeline 前最需要关注的部分。

## 7. 后续优化优先级

1. **persistent direct stream/event**：direct path 不要每次 transfer 创建 stream/event。
2. **persistent transfer worker**：减少 SubmitTransfer 到 executor 的线程调度和 event bookkeeping 成本。
3. **small-object batching**：64MiB 或更小对象不单独提交，尽量合并或在一个 command 内提交多段。
4. **trace 分层统计**：把 `protocol_submit`、`queue_wait`、`worker_execute`、`acl_submit`、`acl_wait`、`event_complete` 分开，确认 1.5-2.1ms 固定成本的主因。
5. **PyPTO pipeline 粒度选择**：大对象/整层权重适合当前 direct path；过细 layer chunk 会被固定成本吃掉，需要和 DAG 任务粒度一起设计。

## 8. 结论

当前通信链路的“物理路径”已经接近理想：`memfd_thp <-> HBM` 可以追齐 `aclrtMallocHost <-> HBM`。现在主要差距不在 DDR 类型，也不在 HBM physical/VMM，而在 UFlow 服务化之后每次 transfer 的固定控制成本和 direct path 内 stream/event 生命周期成本。

因此，下一步不是换 DDR strategy，而是把 direct transfer executor 做成 persistent / batched / event-driven 的低固定开销执行器。
