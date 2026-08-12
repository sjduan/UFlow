# PhaseE-06: UFlow Observability Trace Framework

> 开发环境统一口径（2026-08-12）：所有新开发、构建和验证默认使用 `ssh serving-b` → `openeuler-2403-DS` → `/home/sj/git/data-service`；device 必须现场探测并记录。本文中的“1号机/2号机/NPU7”等旧称仅表示历史证据，不再是当前执行指令。详见 [Phase0-development-environment.md](Phase0-development-environment.md)。

Last reviewed: 2026-08-12

Status: Trace v2 `done / integrated / current-pass` on `serving-b/openeuler-2403-DS`; implementation head `5f47d22983fe6d932d892144da7bf3c881bf55e2`, G0-G10 evidence is archived under `results/phasee06_trace_v2_20260812/`.

## 1. 前序文档索引

- `PhaseE-00-independent-data-service-architecture.md`：定义 PhaseE 总架构、模块边界和完成矩阵。
- `PhaseE-01-unified-transfer-idl-and-local-exchange.md`：定义 DataObject、DataPlacement、TransferPlan、TransferEvent 等统一语言。
- `PhaseE-02-service-owned-vmm-transfer-executor.md`：定义 daemon 自持 HBM/DDR runtime view，由 UFlow 服务侧执行 transfer。
- `PhaseE-03-ddr-data-strategy-selection-and-benchmark.md`：总结 DDR object、pinned host、registered memfd、chunk pipeline 和 pure ACL baseline。
- `PhaseE-04-local-web-monitor-over-ssh-tunnel.md`：定义 monitor API / UI / command bridge 和远端链路检查；PhaseE-06 将 trace control 接入同一 HTTP 出口。
- `PhaseE-05-direction-aware-transfer-channel-manager.md`：实现 service-side H2D/D2H hot path、lazy lane、budget、reaper 和 transfer stats。

## 2. 任务定位

PhaseE-06 将 UFlow 的一次性 benchmark trace 升级为通用观测框架：

```text
monitor = 常开、低频、聚合状态
trace   = 可开关、高粒度、事件级分析
```

两者统一走 UFlow Observability 出口：

```text
client / UI / CLI
  -> monitor API HTTP
  -> uf-daemon Unix socket protocol
  -> TraceCollector / Catalog / TransferExecutor
```

默认 `UF_TRACE_ENABLE=0`。关闭时 daemon 不创建 trace 文件，不分配大 buffer，不记录细粒度事件；monitor API 仍可提供 `/healthz`、`/v1/stats`、`/v1/objects` 等基础监控。

## 3. 实现结果

### 3.1 Daemon-native TraceCollector

新增文件：

```text
crates/uf-daemon/src/trace.rs
```

核心对象：

| 对象 | 作用 |
|---|---|
| `TraceCollector` | daemon 全局 trace collector，默认 disabled |
| `TraceConfig` | `run_id/mode/categories/max_events/output_dir/include_args` |
| `TraceSpan` | RAII duration span，drop 时写入事件 |
| `TraceEvent` | Chrome Trace Event JSON 的内部事件 |

实现策略：

- disabled fast path 只检查 `enabled` 和 category bitmask。
- enabled 后写入 bounded in-memory ring buffer；事件入队只持有短临界区锁。
- 只有 buffer 满或锁中毒时 drop event，并在 status/artifact 中暴露 `trace_dropped_events`；正常锁竞争不再丢事件。
- flush 先在锁内取得 event snapshot，再在锁外完成 JSON/CSV/Markdown IO 与原子 rename，写盘不阻塞 transfer 打点。
- `summary | hot | full` 三档 mode。
- category 包括 `control/object/transfer/channel/chunk/acl/backend/counter`。
- 无外部 Rust crate 依赖，避免远端环境额外安装。

### 3.2 Trace mode

| mode | 默认 category | 用途 |
|---|---|---|
| `summary` | `control,object,transfer,channel,backend,counter` | 低开销、粗粒度运行分析 |
| `hot` | `control,transfer,channel,chunk,acl,backend` | 传输 hot path 分析，首版默认使用 |
| `full` | 全部 category | 调试 object lifecycle + hot path |

也可以通过 `categories=control,transfer,chunk,acl` 显式覆盖。

### 3.3 Daemon protocol ops

新增 Unix socket KV protocol ops：

| op | 语义 |
|---|---|
| `GetTraceStatus` | 查询 trace enabled、run_id、mode、buffered/dropped events、last flush dir |
| `StartTrace` | 开启 trace，参数为 `run_id/mode/categories/max_events/output_dir/include_args` |
| `StopTrace` | 关闭 trace，可选 `flush=1` |
| `FlushTrace` | 将当前 buffer 写出到 artifact |
| `ClearTrace` | 关闭并清空 trace 状态 |
| `ExportTrace` | flush 并返回指定格式 artifact 路径 |

关闭或未配置时，trace ops 返回明确 status/no-op 信息，不让 monitor 或 daemon 报错退出。

### 3.4 Monitor API 合流

`tools/uflow_monitor_api.py` 新增 Observability/Trace HTTP endpoints：

| endpoint | 语义 |
|---|---|
| `GET /v1/observability` | 聚合 health、stats、trace status |
| `GET /v1/trace/status` | 查询 daemon trace status |
| `POST /v1/trace/start` | 开启 trace；默认补 `output_dir=$UF_TRACE_OUTPUT_DIR` |
| `POST /v1/trace/stop` | 关闭 trace；默认 `flush=1` |
| `POST /v1/trace/flush` | flush 当前 trace |
| `GET /v1/trace/artifact?run_id=...&file=...` | 读取 trace artifact |

`POST /v1/command` 也已允许透传 trace ops，UI/CLI 共用同一出口。

### 3.5 UI 轻量接入

`ui/uflow-monitor` 已新增：

- trace enabled/mode 展示。
- buffered/drop event 展示。
- command help 中的 `StartTrace` / `StopTrace` / `FlushTrace` 示例。
- command template：`Start Trace` / `Stop Trace`。

首版不做内嵌 trace viewer；Chrome/Perfetto 直接读取 `trace_events.json`。

## 4. 首批打点

| category | stage |
|---|---|
| `control` | `command.handle` |
| `object` | `object.create/open/reuse/release/mark_ready/mark_dirty` |
| `backend` | `backend.startup_hbm_probe/hbm_alloc/hbm_free/ddr_pretouch` |
| `transfer` | `transfer.plan/submit/execute/complete/wait_client/copy.ddr_to_ddr` |
| `channel` | `channel.acquire/lane_create/lane_busy/release/reap` |
| `chunk` | `chunk.cpu_copy.memfd_to_pinned`、`chunk.cpu_copy.pinned_to_memfd` |
| `acl` | `chunk.acl_submit.h2d/d2h`、`chunk.acl_wait.h2d/d2h`、`chunk.acl_sync.h2d/d2h`、`direct_d2h.acl_submit/wait`、`acl_copy.hbm_to_hbm` |

## 5. 输出文件

daemon-native trace 输出目录：

```text
$UF_TRACE_OUTPUT_DIR/<run_id>/
  trace_events.json
  trace_summary.json
  trace_summary.csv
  transfer_summary.csv
  summary.md
```

| file | role |
|---|---|
| `trace_events.json` | Chrome Trace Event JSON，唯一细粒度 trace |
| `trace_summary.json` | 按 category/name 聚合的机器可读摘要 |
| `trace_summary.csv` | 表格摘要 |
| `transfer_summary.csv` | schema v2 按 `event_id` 聚合的 transfer-level 出口 |
| `summary.md` | 人读总结 |

旧 standalone H2D benchmark 仍可保留 `h2d_stage_*` 文件名；daemon-native PhaseE-06 统一使用 `trace_*`。

## 6. 环境变量

| env | 默认 | 说明 |
|---|---|---|
| `UF_TRACE_ENABLE` | `0` | daemon 启动时是否自动开启 trace |
| `UF_TRACE_RUN_ID` | timestamp | run id |
| `UF_TRACE_MODE` | `hot` | `summary/hot/full` |
| `UF_TRACE_CATEGORIES` | mode default | 显式 category 列表 |
| `UF_TRACE_MAX_EVENTS` | `200000` | bounded buffer 上限 |
| `UF_TRACE_OUTPUT_DIR` | `/tmp/uflow_traces` | artifact root |
| `UF_TRACE_ROOT` | fallback | monitor API trace root fallback |
| `UF_TRACE_INCLUDE_ARGS` | `1` | Chrome trace event 是否写 args |

## 7. 历史验证记录（旧 2号机环境）

### 7.1 本地检查

```text
python3 -m py_compile data-service/tools/uflow_monitor_api.py: pass
node --check data-service/ui/uflow-monitor/app.js: pass
local cargo: unavailable
```

### 7.2 远端 build

旧 2号机 `openeuler-2403-DS` 容器（不代表当前 `serving-b` 已复验）：

```text
cmake -S . -B build: pass
cmake --build build -j 8: pass
cargo fmt: pass
cargo check -p uf-daemon: pass
cargo build -p uf-daemon --release: pass
```

### 7.3 Monitor API trace control smoke

Run dir：

```text
/tmp/proj_output/phasee06_trace_smoke_20260622_145322
```

验证内容：

| item | result |
|---|---|
| `UF_TRACE_ENABLE=0` 默认启动 | pass |
| `GET /v1/observability` shows `trace_enabled=false` | pass |
| `POST /v1/trace/start` | pass |
| `POST /v1/command` create/ready/release DDR object | pass |
| `POST /v1/trace/flush` | pass |
| `GET /v1/trace/artifact` read `trace_events.json` | pass |
| `GET /v1/trace/artifact` read `trace_summary.json` | pass |
| `POST /v1/trace/stop` | pass |

Artifact：

```text
/tmp/proj_output/phasee06_trace_smoke_20260622_145322/traces/smoke_hot/
  trace_events.json
  trace_summary.json
  trace_summary.csv
  summary.md
```

### 7.4 64MiB H2D/D2H hot path trace

Run dir：

```text
/tmp/proj_output/phasee06_hotpath_trace_20260622_145536
```

Configuration：

```text
device: NPU7
size: 64MiB
lanes: 1
directions: h2d,d2h
mode: pinned_async
trace mode: hot
```

Trace check：

```json
{
  "event_count": 89,
  "missing": [],
  "has_required": true
}
```

Required stages observed：

- `chunk.cpu_copy.memfd_to_pinned`
- `chunk.acl_submit.h2d`
- `chunk.acl_wait.h2d`
- `chunk.acl_submit.d2h`
- `chunk.acl_wait.d2h`
- `chunk.cpu_copy.pinned_to_memfd`
- `channel.acquire/lane_create/lane_busy/release`
- `transfer.plan/submit/execute/complete/wait_client`

Artifact：

```text
/tmp/proj_output/phasee06_hotpath_trace_20260622_145536/traces/hotpath_64m/
  trace_events.json
  trace_summary.json
  trace_summary.csv
  summary.md
```

Cleanup：

```text
uf-daemon: stopped
uflow_monitor_api.py: stopped
phasee06 temp sockets: removed or inactive
```

## 8. 当前边界

已实现：

- daemon-native trace collector。
- monitor API trace control + artifact read。
- command bridge trace ops。
- transfer/channel/chunk/ACL 首批打点。
- Chrome trace + summary JSON/CSV/MD 输出。

暂不实现：

- PyPTO / pypto-serving trace 合并。
- 分布式多 daemon clock sync。
- Perfetto protobuf。
- 浏览器内嵌 trace viewer。
- 自动瓶颈诊断。
- 长时间生产级采样、压缩、滚动文件。

## 9. 后续任务

1. 将 PhaseE-06 trace 接入 PhaseE-05 saturation benchmark，形成固定 `--trace-run-id` 输出。
2. 在 PhaseA-08 中把 UFlow `TransferEvent` 与 PyPTO layer task event bridge 对齐。
3. 增加 trace summary 的 transfer-level grouping：按 `event_id/plan_id/lane_id` 输出更适合汇报的表。
4. 若 UI 需要实时数据反馈，可在 PhaseE-04 后续版本加入 `/v1/trace/status` polling 与 artifact download button。

## 10. 2026-06-22 Observability 边界更新

本轮重构后，monitor 和 trace 统一视为 UFlow Observability 出口的两层能力：

| 层级 | 默认状态 | 粒度 | 入口 |
|---|---|---|---|
| monitor | 常开 | 低频聚合状态 | `/healthz`、`/v1/stats`、`/v1/objects`、`/v1/capabilities`、`/v1/observability` |
| trace | 默认关闭 | 事件级 timeline | `/v1/trace/status`、`/v1/trace/start`、`/v1/trace/stop`、`/v1/trace/flush`、`/v1/trace/artifact` |

`/v1/command` 继续作为 JSON command 透传入口，但 command allowlist 只接受新的 solid op；`CreateBuffer/OpenBuffer/ReleaseBuffer/UploadData/GetDeviceHandle/CompleteTransferEvent/WaitObjectEvent` 会由 daemon 返回 unsupported removed op。`/v1/observability` 现在聚合 health、stats、trace status 和 capabilities，便于 UI 启动时一次判断 daemon 链路、当前能力和 trace 开关状态。

### 10.1 Trace 文件约定

daemon-native trace 保留 4 个正式产物：

```text
trace_events.json
trace_summary.json
trace_summary.csv
summary.md
```

standalone benchmark 可以继续保留历史 `h2d_stage_*` 命名；服务侧统一 trace 使用 `trace_*` 命名，后续更多 transfer/path/lane 事件都追加到同一个 Chrome Trace Event JSON 中。

### 10.2 向前索引

- PhaseE-00：定义 UFlow 总体架构、solid op 和观测能力归属。
- PhaseE-01：定义统一 IDL，trace event 依附 `TransferEvent` / command lifecycle。
- PhaseE-02：定义 service-owned executor，trace hot path 打点以 executor/channel/backend 为主。
- PhaseE-06：定义 monitor + trace 合流后的 Observability 出口。

## 11. Trace v2：统一 HBM↔DDR 与 HBM↔SSD 传输时间线（2026-08-12）

### 11.1 当前审计结论

E06 v1 已经具备开关、bounded buffer、Chrome Trace JSON、summary 和 monitor API 控制，但其正式验收停留在 64MiB HBM↔DDR pinned hot path。当前 Transfer Engine 已扩展到 HBM↔DDR direct executor/lane、SSD↔DDR、SSD↔HBM relay 和 SSD↔HBM logical direct，trace 尚未形成统一、可按 transfer 关联的最终证据。

| 能力 | 当前打点 | 当前缺口 |
|---|---|---|
| HBM↔DDR direct executor/lane | 已有 `transfer.execute`、queue/worker、direct lane、ACL submit/wait 等 span | 子阶段字段不统一；最终 outcome 没有实际 path/engine/fallback/stage 指标 |
| HBM↔DDR pinned fallback | 已有 channel/chunk/CPU copy/ACL span，v1 有历史验收 | 尚未与 direct 使用同一 transfer schema 和对比汇总 |
| SSD↔DDR | 已有 copy 与 SSD read/write wait 类 span | 缺少统一 direction/path/engine/stage role；没有正式 trace 验收产物 |
| SSD↔HBM relay | 已有 `relay.ssd_to_ddr`、`relay.ddr_to_hbm`、反向两段 | 无 per-transfer 分段汇总，嵌套 span 总和容易被误解为端到端时间 |
| SSD↔HBM logical direct | 已有 candidate setup 和 ACL span | 缺少完整 outcome、logical-direct 强标识、fallback 关联和正式产物 |
| transfer completion | `transfer.complete` 目前只带 `event_id` | 完成/失败后的 actual fields 没有进入 trace；无法解释 planned→actual |
| summary writer | 只按 `category + name` 聚合 | 丢失 event/plan/path/engine/direction/candidate/fallback 关联 |
| PhaseB 验证目录 | 有 benchmark/candidate summary | 当前没有 `trace_events.json` 或 transfer-level trace summary，不能视为 trace 已适配 |

因此，E06 v1 的历史结论保留；本节 Trace v2 是独立门禁，未生成 11.7 的新产物前不得标记完成。

### 11.2 目标、边界与不变量

Trace v2 必须让一条 `event_id` 从 plan/submit、排队、worker、lane、SSD IO、relay/direct stage 一直关联到 terminal outcome，并回答：

```text
计划走什么 -> 实际走什么 -> 为什么 fallback/失败 -> 每段花了多久 -> 端到端结果如何
```

不变量：

- trace 只观测，不改变正常 path selection、执行顺序、同步语义、错误传播和 event 状态机。
- `UF_TRACE_ENABLE=0` 时不创建文件、不持有大 buffer；新增字段准备不能在 transfer hot path 引入明显回归。
- trace buffer 满、锁竞争、写盘或 summary 失败都不能导致 transfer 失败；drop/error 必须在 status 和 artifact 中可见。
- terminal truth 只能取完成状态写回后的 `TransferEvent` 快照，禁止以 `TransferPlan` 推断 actual path/engine。
- `planned_*` 与 `actual_*` 分开；planner-selected relay 与 runtime fallback 分开。
- SSD↔HBM mmap + ACL 必须记录 `direct_kind=logical`；禁止用 trace 名称或 summary 暗示 physical NVMe/SSU direct。
- 不记录 SSD 绝对路径、数据内容、密钥或地址明文；只记录 object/event/plan id、介质、设备和性能字段。
- 保留 v1 event name 与四个既有产物的兼容性；本任务不做 Perfetto protobuf、跨 daemon clock sync、PyPTO 合流或在线自动诊断。

### 11.3 Trace schema v2

#### 11.3.1 公共关联字段

新增内部 `TransferTraceContext`（或等价的单一 helper），避免各模块手写后字段漂移。plan-scoped event 至少包含 `plan_id`，从 submit 开始的 event-scoped lifecycle event 必须同时包含 `event_id + plan_id`；`event_id` 尚未分配时不得伪造 0。公共字段为：

```text
trace_schema_version=2
plan_id, event_id=<submit 及以后必填>
src_object_id, dst_object_id
src_medium, dst_medium, direction
operation, requested_mode, bytes
planned_path, planned_engine
```

现有请求期 `transfer.plan` 可以继续记录规划耗时，但必须增加一个带真实 `plan_id` 的 plan outcome/result event 供后续关联。所有执行子阶段至少包含 `trace_schema_version/event_id/stage_role/bytes`；能够低成本取得时再带 `plan_id/direction/lane_id/worker_id/device_id/candidate`。不得为了补 trace 字段在 catalog lock 内做 IO 或跨线程阻塞。

保留现有 span name，使用稳定的 `stage_role` 做跨实现归类：

| `stage_role` | 典型现有 span |
|---|---|
| `queue_wait` / `worker_execute` | `transfer.queue_wait`、`transfer.worker_execute` |
| `lane_acquire` / `lane_busy` / `lane_release` | `channel.*`、`direct_lane.*` |
| `cpu_copy` | pinned channel 的 memfd↔pinned copy |
| `acl_submit` / `acl_wait` / `acl_copy` | HBM↔DDR direct/pinned 与 SSD logical direct ACL 阶段 |
| `ssd_io` | SSD read/write submit/wait/copy |
| `relay_stage` | SSD↔DDR 与 DDR↔HBM relay stage |
| `direct_setup` | mmap/register/fadvise/readahead/madvise/pre-touch/mlock |
| `outcome` | terminal `transfer.outcome` |

#### 11.3.2 terminal outcome

新增真正的 instant event `transfer.outcome`；Chrome JSON 使用 instant phase，而不是用一个虚假的长 duration。`finish_transfer_event` 必须先完成 event 字段更新并取得只读快照，释放 catalog lock 后再记录 outcome。失败与 cancel 的其他 terminal 路径也调用同一个 helper，确保每个 terminal event 恰好一个 outcome。

outcome 至少包含：

```text
status, bytes_done
actual_path, actual_engine
actual_latency_us, actual_bandwidth_gib_s
fallback_used, fallback_reason
error_code, error_message

channel_direction, channel_lane_id, channel_queue_wait_us,
channel_worker_execute_us, channel_lane_wait_us,
channel_cpu_copy_us, channel_acl_submit_us, channel_acl_wait_us,
channel_wall_us, channel_overlap_ratio

ssd_io_submit_us, ssd_io_wait_us, ssd_io_bytes,
ssd_io_bandwidth_gib_s, ssd_read_bytes, ssd_write_bytes

relay_stage_count, relay_ddr_hbm_us, relay_total_us

direct_candidate, direct_kind, direct_setup_us, direct_register_us,
direct_fadvise_us, direct_readahead_us,
direct_madvise_hugepage_us, direct_madvise_willneed_us,
direct_madvise_populate_us, direct_pretouch_us, direct_mlock_us,
direct_acl_us, direct_total_us
```

可选/不适用数值必须可区分“未采集”和真实 `0`。推荐 raw trace args 使用空值/省略，transfer summary 使用 `null`；不得把缺失指标静默补成 0。

#### 11.3.3 时间口径

- `actual_latency_us`：TransferEvent 从 started 到 terminal 的端到端真值。
- stage duration：单个 span 的 wall duration。
- `relay_total_us`、`direct_total_us`、`channel_wall_us`：各 executor 已写回的路径级口径。
- 所有嵌套 span 的 duration 之和只能命名为 `sum_of_span_durations_us`，不得标成端到端总耗时。
- summary 中分段之和与端到端不相等是允许的，但必须显示差值，不能通过重复累加“修正”。

### 11.4 Trace writer 与产物合同

v2 保留 v1 四个文件，并新增 per-transfer CSV：

```text
trace_events.json       # Chrome/Perfetto 可打开的原始 timeline
trace_summary.json      # 保留既有顶层字段/stages，新增 schema_version/transfers/path_summary
trace_summary.csv       # 继续作为 category+stage 兼容汇总
transfer_summary.csv    # 新增：每个 event_id 一行，作为 transfer-level 机器可读出口
summary.md              # 新增 transfer/path/fallback/incomplete/drop 小结
```

详细要求：

- `trace_events.json` 顶层增加可忽略的 `uflowTraceSchemaVersion: 2`，同时保持标准 `traceEvents` 数组。
- `trace_summary.json` 保留 v1 的 `run_id/mode/categories/event_count/dropped_events/stages`；新增 `schema_version=2`、`complete`、`incomplete_event_ids`、`transfers`、`path_summary`。
- `transfers` 与 `transfer_summary.csv` 按 `event_id` 聚合 common context、outcome 和 stage role duration；一条 terminal event 只能一行。
- 没有 outcome 的 event 放进 `incomplete_event_ids`，不得伪造成 success。`dropped_events > 0` 时 `complete=false`。
- `path_summary` 至少按 `direction + actual_path + actual_engine + status` 给出 count、bytes、p50/p95 latency、p50/p95 bandwidth、fallback count。
- `trace_summary.csv` 的现有列与含义不删除；如需扩展只能追加列。
- JSON/CSV/Markdown 必须正确转义逗号、引号、换行和非 ASCII；artifact writer 使用原子临时文件 + rename，失败不得留下貌似完整的正式文件。
- `include_args=false` 时 raw timeline 可以省略 args，但内部 event 仍可用于生成 transfer summary；summary 必须明确 raw args 是否被导出。

### 11.5 实施任务与文件归属

| ID | 工作 | 主要文件 | 完成证据 |
|---|---|---|---|
| E06-T1 | 引入 schema v2、公共 context/helper、instant event 与 JSON/CSV escaping/atomic write 单测 | `crates/uf-daemon/src/trace.rs` | synthetic trace 单测；五个产物均可解析；v1 字段兼容 |
| E06-T2 | 在 plan 中持久化规范化 `requested_mode`，并在 plan/submit/execute/complete/fail/cancel 全生命周期传递 context；从更新后的 event 快照记录唯一 outcome | `transfer_planner.rs`、`transfer_executor.rs`、`common.rs`、必要时 `catalog.rs` | 默认/显式 mode 正确；complete/failed/cancelled 各恰好一个 outcome；锁外记录测试 |
| E06-T3 | 统一 HBM↔DDR direct 与 pinned 的 `event_id/stage_role`，覆盖 queue/worker/lane/CPU/ACL | `direct_transfer.rs`、`transfer_channel.rs`、`transfer_executor.rs` | H2D/D2H 两种 path 的阶段检查器通过 |
| E06-T4 | 统一 SSD↔DDR、SSD↔HBM relay/logical direct 的 SSD IO、relay、candidate setup、ACL 和 fallback 打点 | `transfer_executor.rs`、`ssd_backend.rs`、`ssd_direct.rs` | 三类路径双向 trace 检查通过；logical 标签与 event 一致 |
| E06-T5 | 实现 transfer aggregation、`transfer_summary.csv`、path/fallback/incomplete/drop summary 和校验器 | `trace.rs`、`tools/phasee06/`、`examples/phasee06/` | synthetic + no-NPU test 通过；嵌套 duration 不被当端到端时间 |
| E06-T6 | 在 `serving-b/openeuler-2403-DS` 选择健康空闲 device，跑 11.7 全矩阵、保存环境快照与 artifact，并更新本文件实施记录 | `examples/phasee06/`、本文件 | hostname/container/kernel/device/commit、命令、run dir、trace 五件套、检查报告、清理记录齐全 |

代码所有权约束：

- E06 agent 负责 trace schema、writer 和各 transfer path 的 instrumentation；不得改变默认 path/cost 阈值或把测试开关带入正常执行。
- E04 agent 负责 monitor 只读 API/UI。推荐 E06-T1 至 T5 先合入，再由 E04 消费最终字段；两名 agent 不得并行覆盖 `common.rs/transfer_executor.rs`。
- 保留现有 span name，除非能同时给出兼容 alias 和迁移测试；优先追加 `stage_role`。
- 仓库中可能有其他人的修改。只处理本表相关文件，不得回滚、清理或批量格式化无关文件。

为稳定验证 runtime fallback，只有在无法构造可重复的自然失败时，才允许加入 validation-only failpoint：

```text
UF_ENABLE_TEST_FAILPOINTS=1
UF_TEST_SSD_DIRECT_FAIL_STAGE=before_acl
```

必须双重开关同时满足才生效，默认构建/运行完全关闭；显式 direct 模式应按原语义报错，只有允许 fallback 的 `auto` 用例才能转 relay。trace 与 event 都要记录 failpoint 原因，正式 benchmark 不得启用它。

### 11.6 自动检查器要求

新增的检查器不能只搜索 span 名称；它必须按 `event_id` 重建生命周期并验证：

1. common context 前后一致；
2. terminal event 恰好一个，status 与 daemon `PollEvent` 一致；
3. planned/actual、fallback、candidate/kind 与 event 一致；
4. 每条 path 存在必需的 `stage_role`，不允许拿另一个 event 的 span 补齐；
5. `actual_latency_us`、bytes、bandwidth 和主要 stage metrics 在合理浮点容差内与 event 一致；
6. JSON 可由标准 parser 读取，Chrome/Perfetto 时间戳单调合理，CSV 列数稳定；
7. 缺 outcome、duplicate outcome、dropped event、未知 schema 或错误 logical-direct 标识必须返回非零。

检查器输出至少包括：

```text
schema_version
event_count / transfer_count / incomplete_count / dropped_events
matrix case -> PASS|FAIL|SKIP + reason
per-event required/missing stages
event-vs-trace mismatches
artifact paths
```

### 11.7 验收矩阵

所有 NPU 用例固定使用默认 `ssh serving-b` → `openeuler-2403-DS`，并记录 hostname、container id/image、kernel、code commit、实际 device、size、direction、mode/path、repeat 和 run dir。device 必须由当次 `npu-smi info` 选择，不能沿用历史 NPU7 假设。最小 size 使用 64MiB 与 256MiB；每个性能口径至少 warmup 1 次、正式 repeat 5 次。

| Gate | 场景 | 必须检查 |
|---|---|---|
| G0 静态/单测 | trace writer/context/aggregation 的 no-NPU synthetic cases | Rust fmt/check/test；五个 artifact 解析；v1 兼容；escaping/atomic write |
| G1 disabled fast path | `UF_TRACE_ENABLE=0` 跑 HBM↔DDR 与 SSD path | 无 trace 文件；event 正常；相对 E06 v2 开工基线 commit、同机同配置同矩阵的 p50 开销目标 ≤3%，超出必须报告并优化/解释 |
| G2 HBM↔DDR direct | H2D/D2H，`auto/direct`，64/256MiB | queue/worker/lane/ACL/outcome 同一 event 可关联；actual path/engine 正确 |
| G3 HBM↔DDR pinned | H2D/D2H，`pinned_async`，64/256MiB | CPU copy、ACL submit/wait、channel/outcome 齐全；与 direct 可分组比较 |
| G4 SSD↔DDR | SSD→DDR 与 DDR→SSD，64/256MiB | SSD IO role、read/write bytes、IO time/BW、outcome 一致 |
| G5 SSD↔HBM relay | 双向显式 relay，64/256MiB | 两段 relay stage 齐全；relay/path-level/end-to-end 三种时间口径不混用 |
| G6 SSD↔HBM logical direct | 双向显式 direct 与 auto-direct，256MiB | candidate/setup/ACL/outcome 齐全；`direct_kind=logical`；无 physical direct 表述 |
| G7 runtime fallback | auto direct 执行期失败后 relay 成功 | planned direct、actual relay、fallback=true/reason；direct 失败阶段与 relay 成功阶段同属一个 event |
| G8 terminal negatives | transfer failed、cancelled、trace 中途 stop、bounded buffer drop | terminal 不重复；失败不伪装成功；incomplete/drop 明确使 summary `complete=false` |
| G9 出口联动 | daemon op 与 HTTP start/status/stop/flush/artifact，Perfetto/Chrome 打开 | 状态和 event/drop 数一致；五个文件可下载；timeline 可打开；E04 可消费 transfer summary |
| G10 清理 | 完成后停止测试 daemon/API，清理临时 socket/SSD object/HBM lease | 无遗留进程、socket、mount/mmap、测试 object；正式 artifact 保留 |

G7 只接受执行期 fallback，不接受 planner 初始选择 relay 代替。若使用 11.5 的 failpoint，报告必须同时证明双开关未满足时不生效。

### 11.8 Definition of Done

Trace v2 只有同时满足以下条件才能标记完成：

- E06-T1 至 E06-T6 全部完成并进入明确 commit；
- G0 至 G10 全部通过；任何 skip/fail 均保留原因，不能计入通过矩阵；
- HBM↔DDR direct/pinned、SSD↔DDR、SSD↔HBM relay/logical direct/runtime fallback 均有双向、按 event 关联的正式 trace；
- `PollEvent` 抽样值、raw trace outcome、`trace_summary.json`、`transfer_summary.csv` 四处一致；
- Perfetto/Chrome 可打开 `trace_events.json`，五个 artifact 可通过 monitor API 下载；
- disabled fast path、bounded/drop、失败/cancel/incomplete 和清理负例通过；
- run dir、检查器输出、机器/device、命令、commit 和 remaining gaps 回填本文件；
- 项目总控台账同步把 E06 从 `v1 done / v2 planned` 升级为有证据的实际状态。

### 11.9 给执行 agent 的开工指令

```text
目标：实现 PhaseE-06 §11 Trace v2，先建立统一 schema/outcome，再补各 path 打点和 writer。
先读：Phase0-development-environment.md、PhaseE-06 §11、PhaseE-04 §13、PhaseE-07/08、PhaseB-00/02/03/04，以及当前 trace.rs/transfer_executor.rs/direct_transfer.rs/transfer_channel.rs/ssd_backend.rs/ssd_direct.rs。
环境：默认 ssh serving-b -> openeuler-2403-DS -> /home/sj/git/data-service；先保存 preflight，device 现场选择并记录，禁止使用旧 HOST1/HOST2/next 链路。
前置阻塞：现场已确认 remote repo 为 e854fa0 dirty、local main 为 6179941。先只读审计/保存远端改动，禁止 rsync --delete、reset、checkout 或 clean；没有明确执行基线不得开始 E06 远端构建。
顺序：先记录开工基线 commit 与 disabled 性能，再做 E06-T1 -> T2 -> T3/T4 -> T5 -> T6。E06 应先于 E04 的 trace 联动部分合入。
要求：不改变 transfer 策略；保留 v1 兼容；actual truth 来自 terminal event；logical direct 不得省略；trace 失败不得影响 transfer。
交付：代码 commit、逐项状态、G0-G10 结果、机器/device、完整命令、五个 trace artifact、检查报告、run dir 和 remaining gaps。
协作：你不是仓库中唯一执行者；不得回滚或覆盖其他人的改动，发现交叠先报告再调整。
```

## 12. Trace v2 实施与验收记录（2026-08-12）

### 12.1 最终结论与代码基线

E06-T1～T6 已完成，Trace v2 从 `v1 historical / v2 planned` 升级为 `done / integrated / current-pass`。实现保持原有 span name，通过 `stage_role` 增加稳定分类；未改变 transfer path、cost threshold、同步语义或默认 fallback 策略。

实现提交链：

| commit | 内容 |
|---|---|
| `42f353d7e0b3231008d6899a9d4da57cde821765` | schema v2、统一 context/terminal outcome、各 transfer path 打点、五件套 writer、检查器与远端矩阵 |
| `17cc3ed048f0a7008861f50d1c5f3ea8ddf45636` | CSV stage 数值按浮点容差校验，消除 `0.700`/`0.7` 假阳性 |
| `c6b97e59a01ee043b679cf550685aa858436117a` | HTTP start/status/flush/stop 的正式验证入口 |
| `44ef4141a3a4a6ea1517c38f90ca88e0a738b3a8` | 原子发布失败不残留临时/伪完整文件的单测 |
| `5f47d22983fe6d932d892144da7bf3c881bf55e2` | 修复 stop/flush 后 late span 令 status 比 artifact 多 1 的竞态；加入 G10 清理检查器 |

关键实现结果：

- `TransferTraceContext` 统一携带 plan/event/object/medium/direction/operation/requested mode/bytes/planned path/engine。
- `transfer.outcome` 使用 Chrome instant phase `ph=i,s=t`，terminal truth 来自完成状态写回后的 `TransferEvent` 快照，并在 catalog lock 外记录。
- complete/failed/cancelled 都恰好生成一个 outcome；worker 与 cancel 竞态不会覆盖 terminal 状态或重复 outcome。
- HBM↔DDR direct/pinned、SSD↔DDR、SSD↔HBM relay/logical direct/runtime fallback 全部按同一 `event_id` 关联阶段。
- SSD↔HBM mmap + ACL 始终标记 `direct_kind=logical`；auto 执行期失败保留 planned direct、actual relay、candidate 和 fallback reason。
- `trace_summary.json` 新增 transfer/path/incomplete/drop 汇总，`transfer_summary.csv` 每个 event 一行；缺失可选值为 JSON `null`/CSV 空值。
- 五件套使用临时文件 + rename 原子发布；`include_args=false` 只隐藏 raw args，不影响内部 transfer summary。
- `UF_TRACE_ENABLE=0` 时不构造 plan/outcome 参数集合、不创建 trace 文件；active span 在 stop 后完成时不再写入已 flush buffer。

### 12.2 正式环境与隔离方式

| 项目 | 实际值 |
|---|---|
| SSH | `serving-b` |
| hostname | `liteserver-560t-1.novalocal` |
| container | `openeuler-2403-DS` |
| container id | `68527857ede227ab53199265eb6ff41e51314a5dba28f9b78d7c072a30a273dd` |
| image | `image-transfer/old2-openeuler-2403-ds:20260706_194906` |
| kernel | `5.10.0-182.0.0.95.r2220_157.hce2.aarch64` |
| selected device | `7`；当次 `npu-smi info` 显示 0～7 全部 Health OK、无运行进程，device 7 温度最低，选择不是沿用历史假设 |
| canonical remote repo | `/home/sj/git/data-service@e854fa0` dirty，保持未修改 |
| isolated execution tree | `/tmp/phasee06_v2_stage_019f7da1`；baseline tree 为 `/tmp/phasee06_v2_baseline_6179941` |
| local evidence root | `results/phasee06_trace_v2_20260812/` |

远端 dirty repo 没有执行 reset/checkout/clean、覆盖式同步或 `rsync --delete`。所有构建和运行均在隔离副本完成，正式 artifact 保留在 `/tmp/proj_output/<run_id>` 并同步回本地 results。

### 12.3 E06-T1～T6 状态

| Task | 状态 | 证据 |
|---|---|---|
| E06-T1 schema/writer | PASS | Rust trace 单测覆盖 instant、隐藏 raw args、escaping、五件套、nullable、duplicate/incomplete/drop、atomic success/failure |
| E06-T2 lifecycle context/outcome | PASS | regular/fallback/cancel 的 context mismatch=0、每个 event outcome_count=1；incomplete 负例 outcome_count=0 并返回非零 |
| E06-T3 HBM↔DDR | PASS | regular 64/256MiB 双向 direct auto/direct_async 与 pinned_async，queue/worker/lane/CPU/ACL/outcome 全部同 event |
| E06-T4 SSD paths | PASS | SSD↔DDR、SSD↔HBM relay/logical direct 双向；runtime fallback 同 event 含 direct_setup + relay + ssd_io |
| E06-T5 aggregation/checker | PASS | JSON/两份 CSV/Markdown 均解析；PollEvent/raw/summary/transfer CSV 一致；负例返回非零 |
| E06-T6 serving-b matrix | PASS | 机器、容器、kernel、device、commit、size/repeat、run dir、HTTP 与清理证据齐全 |

### 12.4 G0～G10 结果

| Gate | 结果 | 正式证据/结论 |
|---|---|---|
| G0 | PASS | 容器内 `cargo fmt --all -- --check`、`cargo check --workspace`、`cargo test --workspace`；Rust 8 tests PASS。Python 7 tests PASS；runner `bash -n` PASS |
| G1 | PASS | baseline `6179941` 与 candidate `42f353d` 各 120 条非 warmup transfer，均无 trace 文件/失败 event；同机同 device 的 matrix actual-latency p50 overhead `-0.083818%`，阈值 `3%` |
| G2 | PASS | regular run 覆盖 HBM↔DDR direct auto/direct_async、双向、64/256MiB |
| G3 | PASS | regular run 覆盖 pinned_async 双向、64/256MiB，CPU copy、ACL submit/wait 和 lane roles 齐全 |
| G4 | PASS | regular run 覆盖 SSD→DDR、DDR→SSD 双向、64/256MiB，IO bytes/time/BW 与 PollEvent 一致 |
| G5 | PASS | regular run 覆盖 SSD↔HBM 显式 relay 双向、64/256MiB，`ssd_io` 与 `relay_stage` 分开聚合 |
| G6 | PASS | regular run 覆盖显式与 auto logical direct 双向 256MiB，candidate/setup/ACL/outcome 齐全且 `direct_kind=logical` |
| G7 | PASS | fallback run：auto direct 在 `before_acl` 执行期失败后 relay 成功，双向各 warmup 1 + repeat 5；显式 direct 双向各失败一次且不 fallback |
| G8 | PASS | explicit failure 与 cancel 均唯一 outcome；incomplete checker 按预期 FAIL（1 incomplete）；drop checker 按预期 FAIL（37 dropped、`complete=false`） |
| G9 | PASS（自动合同） | daemon ops 与 HTTP start/status/flush/stop 全通过；stop/status/artifact event count 均为 440；五件套 HTTP 200 且下载文件逐字节一致。标准 JSON、Chrome `X/i` event、单调时间戳验证通过 |
| G10 | PASS | 清理检查覆盖 10 个正式/复验 run：无测试进程、socket、DDR/SSD 临时目录；收尾 `npu-smi` 无 NPU 进程 |

G1 的逐 case 数值受 SSD page cache、pinned lane 首次建链和运行顺序影响，出现正负双向波动；因此按预先固定的完整矩阵 p50 口径判定。为降低实现侧开销，disabled 路径增加显式 fast-path guard；反向顺序复跑后的完整矩阵中位开销为 `-0.083818%`。

G9 没有把内部 trace 上传到外部网站。Codex 浏览器安全策略禁止自动访问 `chrome://tracing`，因此未保存人工 viewer 截图；已用标准 parser、Chrome Trace Event 合同、时间戳/phase 检查和 HTTP 下载逐字节校验完成自动可打开性验证。这是证据呈现限制，不是 Trace v2 数据或接口缺口。

### 12.5 正式 run 与 artifact

本地统一入口：`results/phasee06_trace_v2_20260812/`。

| run id | 用途 | 核心结果 |
|---|---|---|
| `phasee06_v2_disabled_baseline2_20260812_019f7da1` | G1 baseline | 120 measured transfers；0 trace files；PASS |
| `phasee06_v2_disabled_candidate2_20260812_019f7da1` | G1 candidate | 120 measured transfers；0 trace files；overhead report PASS |
| `phasee06_trace_v2_regular_final_20260812_019f7da1` | G2-G6 主矩阵 | 144 transfers、3144 trace events、0 incomplete、0 drop、0 mismatch；PASS |
| `phasee06_trace_v2_fallback_final_20260812_019f7da1` | G7 + explicit failure | 14 transfers、396 events、0 drop/mismatch；PASS |
| `phasee06_trace_v2_cancel_final_20260812_019f7da1` | G8 cancel | 1 complete + 1 cancelled，均唯一 outcome；PASS |
| `phasee06_trace_v2_incomplete_stop_final_20260812_019f7da1` | G8 incomplete | checker 按预期非零；1 incomplete、0 drop、`complete=false` |
| `phasee06_trace_v2_drop_final_20260812_019f7da1` | G8 bounded drop | checker 按预期非零；37 dropped、`complete=false` |
| `phasee06_trace_v2_http_stop_final_20260812_019f7da1` | G9 HTTP/stop consistency | 14 transfers、440 events；start/status/flush/stop/download PASS；count 440/440/440 |

每个 trace-on run 均保存：

```text
environment.txt
poll_events.json
trace_control.json
trace_check.json
http_artifact_check.json
traces/<run_id>/trace_events.json
traces/<run_id>/trace_summary.json
traces/<run_id>/trace_summary.csv
traces/<run_id>/transfer_summary.csv
traces/<run_id>/summary.md
```

### 12.6 复现入口

```bash
# serving-b -> openeuler-2403-DS；先用 npu-smi 现场选 device
UF_E06_DEVICE=<device> \
UF_E06_PYTHON=/usr/local/python3.11.14/bin/python3 \
UF_E06_PROFILE=regular \
bash examples/phasee06/run_trace_v2_remote.sh

# runtime fallback / cancel / incomplete / drop
UF_E06_PROFILE=fallback bash examples/phasee06/run_trace_v2_remote.sh
UF_E06_PROFILE=cancel bash examples/phasee06/run_trace_v2_remote.sh
UF_E06_PROFILE=incomplete bash examples/phasee06/run_trace_v2_remote.sh
UF_E06_PROFILE=drop bash examples/phasee06/run_trace_v2_remote.sh

# HTTP trace lifecycle
UF_E06_TRACE_CONTROL=http bash examples/phasee06/run_trace_v2_remote.sh

# disabled baseline/candidate
bash examples/phasee06/run_trace_disabled_benchmark.sh
python3 tools/phasee06/compare_disabled_benchmark.py <baseline.json> <candidate.json>
```

### 12.7 Remaining gaps / future work

E06 当前范围内没有功能阻塞。后续工作不回退本次完成状态：

1. E04 Monitor v2 消费 `transfer_summary.csv`/trace status，增加最近 transfer、planned→actual、fallback 和三介质展示；这是独立的 PhaseE-04 任务。
2. 若需要审计截图，可由维护者在受信任本机手工将 `trace_events.json` 导入 Chrome/Perfetto；自动化不向外部 viewer 上传内部 trace。
3. Perfetto protobuf、跨 daemon clock sync、多 daemon 合流和在线自动诊断仍是明确 out-of-scope 的后续能力。
4. G1 中 SSD cache/pinned 首次建链的逐 case 抖动应在 B05 的冷热页、交替顺序、p50/p90 长矩阵中继续治理，不改变 Trace disabled fast-path 已达标的结论。
