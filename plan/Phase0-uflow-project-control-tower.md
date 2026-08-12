# Phase0: UFlow 项目总控台账

> 开发环境统一口径（2026-08-12）：所有新开发、构建和验证默认使用 `ssh serving-b` → `openeuler-2403-DS` → `/home/sj/git/data-service`；device 必须现场探测并记录。本文中的“1号机/2号机/NPU7”等旧称仅表示历史证据，不再是当前执行指令。详见 [Phase0-development-environment.md](Phase0-development-environment.md)。

Last reviewed: 2026-08-12

Status: active; this document is the single project-level status and future-work outlet.

## 1. 文档定位

本文是 UFlow 项目的唯一总监控出口，用来持续回答：

1. 总纲要求什么；
2. 当前真正完成了什么；
3. 哪些结论只在历史基线或远端实验中成立；
4. 当前阻塞、风险和缺口是什么；
5. 下一步应该先做什么，以及完成门槛是什么。

范围总纲仍然是 `uflow_framework_scope.md`。PhaseA、PhaseB、PhaseE 文档负责保存设计细节、实验过程和阶段证据；它们不再各自充当全项目状态出口。若阶段文档与本文冲突，以本文的最新审计结论为项目状态口径，但不得删除阶段文档中的原始实验事实。

本文只维护项目状态和治理口径，不替代：

- `uflow_framework_scope.md` 的长期产品/架构范围；
- 各 Phase 文档的实施细节和实验记录；
- `CODE_WALKTHROUGH.md` 的当前源码导读；
- Git、测试产物和远端 run dir 所代表的原始证据。

### 1.1 当前默认开发环境

当前唯一默认执行环境是：

```text
ssh serving-b
  -> openeuler-2403-DS
  -> /home/sj/git/data-service
  -> device 由当次 npu-smi info 选择并记录
```

`serving-a/c/d` 只用于明确指定的多机、对照或故障任务。旧“1号机/2号机/NPU7”结果继续作为历史证据，但在 `serving-b` 生成新环境快照和新 run id 前不得升级为当前环境 `current-pass`。连接、迁移和证据要求以 `Phase0-development-environment.md` 为准。

## 2. 状态与证据规则

以后任何阶段不得只写一个模糊的 `Done`。总控状态固定拆成以下维度：

| 字段 | 可选值 | 含义 |
|---|---|---|
| `plan_status` | `done` / `active` / `planned` / `not-started` / `superseded` | 计划层面的进度 |
| `code_status` | `integrated` / `working-tree` / `historical-worktree` / `absent` | 代码是否位于当前稳定基线 |
| `validation_status` | `current-pass` / `historical-pass` / `doc-claimed` / `not-run` | 验证是否可复核、是否仍对应当前代码 |
| `scope_alignment` | `aligned` / `partial` / `stale` | 与 `uflow_framework_scope.md` 的一致程度 |
| `next_gate` | 明确的单一出口条件 | 进入下一状态前必须满足的条件 |

证据等级固定为：

| 等级 | 定义 |
|---|---|
| E3：源码与结果可复核 | 当前代码/工作树可定位，且本地存在结果或可重复验证记录 |
| E2：历史远端验收 | 文档记录了机器、基线、run dir 和通过结果，但当前环境未重跑，或代码基线已经前进 |
| E1：代码或文档单边存在 | 只有代码落点，或只有文档声明，缺少成套验证证据 |
| E0：规划 | 只有目标、接口草案或验收矩阵，尚未实施 |

降级规则：

- 上游仓库显著前进但未回归时，`current-pass` 自动降为 `historical-pass`。
- 实现只存在于 dirty working tree 时，不能标记为 `integrated`。
- 性能实验必须同时记录 size、方向、mode、基线、重复次数和结果目录，单次最好值不能升级默认策略。
- “direct” 必须区分 logical direct 与 physical direct。当前 SSD↔HBM mmap 路径属于 logical direct，不等于 NVMe/SSU 到 HBM 的物理直通。

## 3. 总纲目标与当前覆盖

`uflow_framework_scope.md` 定义的长期目标，是统一管理本机及跨节点 DDR、SSD/SSU、HBM、UB、DFS 等介质上的 DataObject、Placement、Handle、TransferPlan、TransferCost、Event 和一致性状态，并由 topology/capability/cost 驱动路径选择。

截至 2026-08-12，实际落地边界是：

| 总纲能力 | 当前状态 | 总控判断 |
|---|---|---|
| 本机 HBM/DDR 对象、lease、placement | 已落地并有历史远端验收 | 核心本机对象模型成立 |
| 本机 HBM↔DDR、HBM↔HBM、DDR↔DDR | 已落地并有远端验收 | daemon-owned transfer 主边界成立 |
| pypto-serving 权重/KVCache 接入 | A09 历史基线通过 | 当前 main 已前进，需重新适配和回归 |
| 本机 SSD DataObject 与 SSD↔DDR | 已进入 `data-service/main@6179941` 并有既有验收 | 代码已集成；PhaseB 计划与结果归档仍未纳入版本基线 |
| SSD↔HBM relay | 已进入 `data-service/main@6179941` 并有既有验收 | Trace v2 正式观测证据已在 `serving-b` 闭环；Monitor v2 待实施 |
| SSD↔HBM logical direct | `>=256MiB` direct-first 已有结果 | 不是物理 NVMe/SSU direct；策略仍需 B05 稳定性收口 |
| Prepare/Prefetch 与异步预取 | 仅 B06 计划 | 未实现 |
| 独立 daemon、SDK、monitor、trace | daemon/SDK 与 Trace v2 已落地 | Trace v2 已覆盖当前 HBM↔DDR direct/pinned 与 SSD 多路径；E04 Monitor v2 仍待实施 |
| Topology/Capability/Cost 动态闭环 | 仅静态/局部探测 | 未达到总纲目标 |
| ub-sim 多节点验证 | 未见 PhaseC 实施文档或执行证据 | 未开始 |
| Remote DDR / UB shmem / SSU block | 无 backend 与端到端证据 | 未开始 |
| Remote HBM direct/relay | 无 PhaseD 实施文档与执行证据 | 未开始 |
| namespace/tenant、鉴权、持久化 catalog、recovery | 长期项 | 未完成 |

项目的真实实施顺序不是字母顺序。当前依赖关系是：

```text
PhaseA 本机原型
  -> PhaseE 本机独立服务与 Transfer Engine 底座
     -> PhaseA-08/A09 serving 新路径
     -> PhaseB 本机 SSD 扩展
  -> PhaseC UB/多节点仿真（尚未开始）
  -> PhaseD 远端 HBM direct/relay（尚未开始）
```

PhaseE 已提前实施，不代表总纲中的 PhaseE 全部完成；目前完成的是本机独立服务首版，而不是跨节点、动态 cost、全局 catalog 意义上的完整 PhaseE。

## 4. 阶段总览

### 4.1 Phase 0：资料与项目治理

| 项目 | 状态 | 证据 | 缺口 / 下一门禁 |
|---|---|---|---|
| CANN ACL 手册索引 | `active` | `Phase0-CANN-acl-manual-reference.md` | 文档仍有 `Validation result: TODO`；后续新增 ACL 能力必须回填函数、页码、机器和结果 |
| 统一开发环境 | `active` | `Phase0-development-environment.md`、`memory/remote_connection.md` | 默认 serving-b 已冻结；各历史能力需按风险在新 host/kernel/device 上逐步复验 |
| 项目总控 | `active` | 本文 | 每次阶段状态变化、基线变化、验证或风险变化后更新 |
| 总纲同步 | `stale` | `uflow_framework_scope.md` 最后审阅为 2026-06-15 | 需回写已经回答的问题，例如 SSD↔HBM 当前 logical direct-first 结论，同时保留 physical direct 未决边界 |

### 4.2 Phase A：单机 UFlow 与 serving 接入

| 阶段 | 总控状态 | 证据等级 | 当前判断 | 下一门禁 |
|---|---|---:|---|---|
| A00 总体策略 | `done / maintained` | E2 | 完成矩阵覆盖 A02-A09；仍是 PhaseA 入口 | 持续同步 latest serving 与未支持项 |
| A01 旧 daemon 经验 | `done` | E1 | 设计经验提炼完成，不是实现验收 | 无独立实施门禁 |
| A02 ACL HBM C++ 验证 | `done` | E2 | normal/physical/shareable/overlay 主链路通过 | 异步 event、跨容器和异常恢复仍属扩展项 |
| A03 Rust daemon + ACL backend | `done` | E2 | daemon-owned HBM、lease、共享访问成立 | liveness/异常回收和跨容器仍需生产化 |
| A04 Qwen3-14B baseline | `done-historical` | E2 | 旧环境 baseline 通过 | PTOAS clean rebuild 兼容问题仍需独立闭环 |
| A05 managed weights/KVCache | `superseded-by-A06/A09` | E2 | L2 结果有效，旧 L3 失败结论已被 A09 的新 contract 替代 | 只保留历史证据 |
| A06 global adapter | `done-historical` | E2 | L2 adapter 与 `UF_ENABLE=0` fallback 通过 | 由 A09/latest main 路径继承，不再单独扩旧 L2 |
| A07 DDR offload/reload | `done-functional` | E2 | 同步 HBM→DDR→HBM roundtrip 通过 | 性能路径已由 E07/E08/A08 取代；new-pointer reload 仍未完成 |
| A08 daemon direct pipeline | `done-contract` | E3 | 正式传输只走 daemon Plan/Submit/Event；direct + fallback 已验证 | cross-process/device event handoff 与更细粒度 DAG overlap |
| A09 latest L3 worker adapter | `historical-pass / stale-baseline` | E2 | 在 `pypto-serving@0b0d8a0` 单卡通过 | 当前 `pypto-serving/main@49297c2` 已领先 62 commits；必须 rebase/重适配并重跑 G1-G6 |

Phase A 的主要未闭环项：

- current `pypto-serving/main` 的 UFlow 回归；
- multi-NPU / multi-sub-worker 的指定 worker/rank import routing；
- daemon-created 或跨进程可交付的真实 completion event；
- prefill/decode 两套权重布局的统一 view/layout planning；
- CPU/shared tensor 与 UFlow DDR shadow 的去重；
- HTTP serving 长稳态、异常退出、stale lease 和资源回收；
- new HBM pointer reload 后的 runtime descriptor/cache refresh。

### 4.3 Phase B：本机 SSD 多介质

| 阶段 | 总控状态 | 证据等级 | 当前判断 | 下一门禁 |
|---|---|---:|---|---|
| B00 SSD 架构 | `active` | E1 | Gate 0-4 已有实现路径，Gate 5/6 未闭环 | 以 B05/B06 完成作为 PhaseB 首轮收口 |
| B01 SSD DataObject | `integrated` | E2 | SSD object/backend 已进入 `main@6179941`，旧 2号机文档验收通过 | 在 `serving-b` 纳入自动回归，并把计划/结果归档绑定到 commit |
| B02 SSD↔DDR | `integrated` | E3 | 代码已集成；本地结果归档含 `UFLOW_B02_ALL_PASS`；E06 Trace v2 双向观测通过 | 自动回归与 E04 Monitor v2 观测验收 |
| B03 SSD↔HBM relay | `integrated` | E3 | 代码已集成；完整 size matrix 含 `UFLOW_B03_ALL_PASS`；E06 runtime fallback trace 通过 | fallback 自动回归与 E04 Monitor v2 观测验收 |
| B04 SSD↔HBM direct candidates | `integrated-policy-first-version` | E3 | 代码已集成；`auto` 对 `>=256MiB` 采用 `file_mmap_acl_direct`；256MiB+ 汇总优于 relay | 必须始终标注 logical direct；完成 B05 repeat 稳定性与 runtime fallback 收口 |
| B05 优化与策略 | `active-planning` | E0 | 指标与 benchmark 设计存在，尚无实施闭环 | 重复矩阵、p50/p90、冷热页、fallback、阈值升级规则全部通过 |
| B06 Prepare/Prefetch | `planned` | E0 | 只有 API/事件/验收草案 | 实现显式 prepare/prefetch，证明 prepared hit 与未准备 fallback |

PhaseB 的 SSD runtime、SDK、examples 与说明已通过 `ba78523`、`59fec14`、`6179941` 进入稳定代码基线；E06 Trace v2 又在 `5f47d22` 前后的提交链补齐了 SSD↔DDR、SSD↔HBM relay/logical direct/runtime fallback 的正式 event-level 观测。剩余治理风险转为：PhaseB 计划和大量结果目录仍是 untracked，代码 commit 与原始证据尚未形成一个可移植的归档索引；E04 Monitor v2 仍不能完整展示这些新路径。

### 4.4 Phase C：UB 超节点仿真

`plan/` 下当前没有 PhaseC 实施文档，代码审计也未发现 UB transport/ub-sim executor 的落地证据。

状态：`not-started`，证据等级 E0。

启动门禁：

1. 冻结 topology、capability、cost table 的最小 IDL；
2. 给出 ub-sim backend 与 executor 的明确代码边界；
3. 覆盖 remote DDR import、remote SSD/SSU block、异步 completion、failure injection；
4. 用至少两套 cost table 验证 a2a3 与 a5/UB-domain 的不同选路；
5. 所有路径选择必须输出可解释 effort 和 fallback 原因。

### 4.5 Phase D：远端 HBM 与 relay

`plan/` 下当前没有 PhaseD 实施文档，也没有 remote HBM direct/relay 的端到端证据。

状态：`not-started`，证据等级 E0。

启动依赖：PhaseC 的 topology/capability/remote DDR/async completion 基础先通过。

首轮必须同时验证：

- `direct_capability=true` 的远端 HBM 路径；
- `direct_capability=false` 时的最少跳数 relay；
- relay 中间 placement 的生命周期和失败清理；
- cost 变化、链路失败、容量不足时的重规划；
- dirty/ready/lease 在跨节点边界上的一致性。

### 4.6 Phase E：独立数据服务

| 阶段 | 总控状态 | 证据等级 | 当前判断 | 下一门禁 |
|---|---|---:|---|---|
| E00 总架构 | `active / partial` | E2 | 本机独立服务首版成立；完整成功标准尚未达到 | 关闭本节长期缺口 |
| E01 统一 IDL 与 local exchange | `done-local` | E2 | Object/Placement/Plan/Cost/Event 与本机交换矩阵通过 | 正式跨语言 wire IDL |
| E02 service-owned executor | `done-local` | E2 | daemon 持有 HBM/DDR runtime view，MarkDirty/Ready 与负例通过 | 真实跨进程 event 与更完整一致性 |
| E03 DDR 策略 benchmark | `done-local` | E3 | memfd + THP/pre-touch 结论形成并进入默认路径 | 拓扑/硬件变化后的自动重校准 |
| E04 monitor/lifecycle | `v1-historical / env-migration+v2-planned` | E2 / E0 | v1 在旧 machine2 环境通过；当前需先迁移 serving-b 单 alias，再实现三介质容量、最近传输、planned→actual、fallback 与 trace artifact 联动 | 实施 `PhaseE-04 §13` 的 E04-M0～M6，并通过 G0～G8 |
| E05 transfer channel | `done-fallback` | E2 | pinned channel manager 作为 fallback 成立 | topology-driven budget、并发/双卡补测 |
| E06 trace framework | `done / integrated / current-pass` | E3 | Trace schema v2、唯一 terminal outcome、HBM↔DDR/SSD 多路径分段、五件套与 per-transfer summary 已进入 `main@5f47d22`；serving-b G0～G10 已闭环 | 由 E04 Monitor v2 消费最终合同；后续只做 Perfetto protobuf/多 daemon clock 等扩展 |
| E07 direct default | `done-local` | E3 | memfd direct ACL async 成为 DDR↔HBM 默认 | 已由 E08 优化小对象固定开销 |
| E08 low-overhead executor | `done-first-version` | E3 | persistent worker/stream/event 与 size matrix 通过 | H2D+D2H 并发、idle reaper、64MiB event 口径继续补测 |

Phase E 距离总纲完整目标仍缺：

- E04 对当前 HBM↔DDR direct/pinned 与 SSD↔DDR、SSD↔HBM relay/logical direct/runtime fallback 的统一 monitor 展示；Trace v2 已闭环；
- 正式跨语言 RPC/IDL；
- 全局多 daemon placement planner 和持久化 catalog/recovery；
- namespace/tenant、鉴权和 share permission；
- TopologyProvider/CapabilityProvider 的真实动态发现；
- 运行时 stats 反馈到 CostModel 的自动校准闭环；
- ub-sim executor 与跨节点场景；
- 真实 UB/URMA/SSU/DFS backend；
- PyPTO DAG 的原生 transfer-event handoff；
- 多 daemon trace clock sync 和生产级 observability。

## 5. 当前关键路径

### P0-A：补齐当前多介质传输的 Observability（E06 已完成，剩余 E04）

实施顺序固定为：

1. E06 已用隔离树绕开并保护 `serving-b/openeuler-2403-DS` 的 remote `e854fa0` dirty repo；Trace v2 实现进入 `data-service/main@5f47d22`，G0～G10 证据位于 `results/phasee06_trace_v2_20260812/`。
2. 当前实施 `PhaseE-04 §13`：先完成 serving-b helper 迁移，再基于最终 event/trace 合同增加三介质容量、最近传输、planned→actual、fallback 和 artifact UI。
3. 在 `serving-b/openeuler-2403-DS` 现场选择健康 device，覆盖 Monitor 的 HBM↔DDR direct/pinned、SSD↔DDR、SSD↔HBM relay/logical direct/runtime fallback 双向矩阵。
4. 把 daemon `PollEvent`、raw trace、transfer summary、HTTP JSON 和 UI 抽样对齐到同一 event id。

退出条件：E06 G0～G10 已通过；待 E04 G0～G8 通过后关闭整个 P0-A。最终代码、环境快照、五件套 trace、API 快照、UI 截图、命令、清理记录和 commit 必须可复核；logical direct 无误标，planner relay 与 runtime fallback 无混淆。

### P0-B：恢复 serving 当前主线兼容性

1. 以当前 `pypto-serving/main@49297c2` 为基线重新定位 UFlow adapter 接入点。
2. 审计 62 个提交带来的 runner、KV cache、parallel serving、scheduler、worker 生命周期变化。
3. 重跑 A09 G1-G6：worker import、KV-only、weights-only、组合 decode、trace、disabled fallback。
4. 追加 HTTP serving 生命周期与至少单个长稳态回归。

退出条件：A09 从 `historical-pass` 恢复为 `current-pass`，且 UFlow disabled 路径与 upstream baseline 一致。

### P1-A：收拢 PhaseB 证据基线

1. 保持 `data-service/main@6179941` 之后的 SSD 代码基线可审查。
2. 把 PhaseB-00～06 计划、关键结果摘要和 result index 纳入版本管理或明确的外部归档。
3. 对 B01-B04 建立同一套可重复构建、功能、fallback、清理回归入口。
4. 将每个正式结果目录绑定到 commit、机器/device、配置和完整命令。

退出条件：PhaseB 不再依赖本机 untracked 文档/结果才能解释已有结论；任一维护者都能从明确 commit 与索引复现关键结果。

### P1-B：完成 PhaseB 策略闭环

1. 实施 B05，固定 repeat、p50/p90、冷热页和方向化 size matrix。
2. 只有重复稳定通过后才保留或调整 `256MiB` direct-first 阈值。
3. 实施 B06 Prepare/Prefetch，把 readahead/pre-touch 从 Submit 热路径移到显式准备阶段。
4. 保留 relay 作为可观测、可强制的 fallback；任何自动 fallback 都必须写入 event/trace。

退出条件：B00 Gate 5/6 关闭，PhaseB 可标记 `done-local-first-version`。

### P1-C：补齐本机服务生产性缺口

优先级顺序：

1. 扩大自动化 Rust/Python 静态检查与回归入口；E06 已新增 8 个 Rust/7 个 Python 合同测试，但项目其他模块仍主要依赖 examples/smoke。
2. stale lease、client crash、daemon restart、catalog recovery。
3. multi-NPU/multi-worker import routing。
4. PyPTO 原生 event handoff 与并发传输矩阵。
5. namespace/tenant/permission 最小模型。

### P2：启动 PhaseC，再进入 PhaseD

PhaseC 先完成仿真语义和故障闭环，PhaseD 再做远端 HBM。不得跳过仿真直接把本机静态 cost table 扩写成“跨节点已支持”。

## 6. 风险登记

| ID | 风险 | 等级 | 当前证据 | 缓解动作 |
|---|---|---|---|---|
| R1 | PhaseB 代码已集成，但计划与结果证据仍未进入统一版本/归档基线 | 中 | `main@6179941` 已含 SSD 代码；PhaseB plans/results 仍 untracked | P1-A 收拢证据基线并绑定 commit |
| R2 | A09 验收基线落后当前 serving main 62 commits | 高 | `0b0d8a0` vs `49297c2`，相关目录变化 1664+/532- | 重适配并重跑 G1-G6 |
| R3 | smoke/benchmark 多，自动 unit/integration regression 少 | 高 | E06 已有 Rust/Python 合同测试；其他模块仍主要依赖 examples 和远端脚本 | 把 E06 分层测试接入 CI，并扩展到其他模块 |
| R4 | logical direct 被误解为 physical SSD direct | 高 | B04 使用 file mmap + ACL copy | 文档、event、API 始终标注 logical direct |
| R5 | 总纲与现状时间失配 | 中 | 总纲 2026-06-15；B04 已回答部分旧开放问题 | 下一轮架构审阅同步总纲 |
| R6 | 静态 cost/capability 无法支撑跨节点决策 | 高 | 当前主要是本机静态规则/环境探测 | PhaseC 引入 topology/capability provider 与动态反馈 |
| R7 | client proxy event 不能代表完整 device-side overlap | 中高 | A08/E08 保留问题 | 设计跨进程 event handoff 并接 PyPTO DAG |
| R8 | 单机成功被外推为生产多机能力 | 高 | C/D 无实施文档和证据 | 状态表严格保持 `not-started` |
| R9 | 本机缺少 Cargo，无法现场复核 Rust | 中 | `cargo` 不在 PATH；Python compileall 通过 | 远端构建产物绑定 commit；补 CI |
| R10 | 文档和 run dir 多但缺统一索引 | 中 | 计划约 1.5 万行、结果目录分散 | 每次验证回填本文的基线、命令、结果路径 |
| R11 | Monitor 仍落后当前 HBM↔DDR direct 与 SSD 多路径，可能看不到 runtime fallback 或误报 direct | 中高 | E06 Trace v2 已通过双向矩阵并输出 planned→actual/fallback/logical-direct；E04 UI 仍无完整 transfer history | 直接实施 E04 §13，消费已冻结的 Trace v2/PollEvent 合同并完成 UI/API 一致性检查 |
| R12 | 大量历史性能/功能结论来自旧 2号机，可能被误当成 serving-b 当前能力 | 高 | 旧文档常写 NPU7、5.10 kernel、双跳链路；新主机/kernel/device 尚未逐项复验 | 所有新 run 强制保存 serving-b 环境快照；kernel/SSD/DMA/带宽结论优先重跑，未重跑保持 historical-pass |
| R13 | serving-b 容器 remote repo 落后本地主线且 dirty，覆盖式同步可能丢失远端证据 | 高 | remote `e854fa0` 仍保持原状；E06 在 `/tmp/phasee06_v2_stage_019f7da1` 隔离验证，没有修改 canonical repo | E04 继续采用无损隔离/明确 baseline；禁用 `--delete`、reset/checkout/clean，另行治理 canonical repo |

## 7. 本次审计事实

审计日期：2026-08-12（本次增量复审聚焦 PhaseB 集成状态与 E04/E06 Observability；其他阶段沿用 2026-07-20 审计结论）。

本次已读取：

- `uflow_framework_scope.md`；
- `Phase0-CANN-acl-manual-reference.md`；
- PhaseA-00 至 PhaseA-09；
- PhaseB-00 至 PhaseB-06；
- PhaseE-00 至 PhaseE-08；
- `data-service` 当前 Git 状态、关键源码落点、PhaseB/PhaseE 结果摘要；
- `pypto-serving` 与 `pypto-serving-phasea09` 的当前基线差异；
- 已有 `CODE_WALKTHROUGH.md` 与已接受的 `HANDOFF.md` 证据说明。

本次检查结果：

| 检查 | 结果 |
|---|---|
| Python SDK 与 PhaseB examples `compileall` | Pass |
| Rust `cargo fmt/check/test` | serving-b 容器 PASS；E06 最终为 8 个 Rust tests |
| PhaseB-02 本地结果摘要 | `UFLOW_B02_ALL_PASS` |
| PhaseB-03 本地结果摘要 | `UFLOW_B03_ALL_PASS` |
| PhaseB-04 final policy | 256MiB-2GiB `auto` 使用 logical direct；summary 记录 direct/relay 比值 |
| PhaseB 代码基线 | SSD runtime/SDK/examples/docs 已通过 `ba78523`、`59fec14`、`6179941` 进入 `data-service/main` |
| PhaseB 计划与结果 | 仍为 untracked；代码已集成不等于证据归档已收口 |
| Monitor 当前适配 | API 可透出部分新 stats/event 字段；UI 仍以 DDR-first 为主，无 recent transfers、SSD stage、planned→actual/fallback 完整视图 |
| Trace 当前适配 | Trace v2 已完成；regular 144 transfers/3144 events、fallback 14/396、cancel 2/81，均 0 drop/mismatch；incomplete/drop 负例按预期失败；五件套与 HTTP 出口通过 |
| serving-b 连通性 | `ssh serving-b` 成功；hostname `liteserver-560t-1.novalocal`，IP `192.169.0.85`，`openeuler-2403-DS` running |
| serving-b repo | `/home/sj/git/data-service@e854fa0`，触发 dubious ownership 且 working tree dirty；核心源码 SHA-256 抽查 16/16 等同 local `6179941`，但其余 artifacts/provenance 未收口；只读命令使用临时 `-c safe.directory=...` |
| A09 基线新鲜度 | 过时；current main 已领先 62 commits |
| PhaseC/PhaseD | 未发现实施文档或执行证据 |

## 8. 后续更新协议

任何项目进展先更新对应 Phase 文档或保留原始结果，再由项目总监在本文更新总控状态。一次合格更新至少包含：

```text
date:
workstream / phase:
baseline commit or worktree:
what changed:
plan_status:
code_status:
validation_status:
commands / machine / device:
result artifact or run dir:
regressions / failures:
remaining gap:
next gate:
owner:
```

状态升级必须有证据，失败结果不得从台账中消失。若同一能力在新基线上未复测，应保留历史通过事实，同时明确降级为 `historical-pass`。

每次更新本文时至少复查：

1. `data-service` Git 状态和最近提交；
2. `pypto-serving` / PyPTO 相关基线是否前进；
3. 新增或修改的 Phase 文档；
4. 新增 results/run dir；
5. 当前 P0/P1 门禁是否真正关闭；
6. 风险登记是否需要新增、升级、降级或关闭。

## 9. 变更日志

| 日期 | 变更 |
|---|---|
| 2026-07-20 | 创建项目总控台账；以 `uflow_framework_scope.md` 为总纲，完成 Phase0/A/B/E 基线审计，确认 PhaseC/D 未启动；登记 PhaseB 未稳定集成与 A09 基线过时为两个 P0 风险。 |
| 2026-08-12 | 复核 `data-service/main@6179941`，确认 PhaseB SSD 代码已经集成，风险转为计划/结果未归档；确认 Monitor/Trace 仅部分适配当前 HBM↔DDR direct 与 SSD 多路径；将 E06 Trace v2、E04 Monitor v2 登记为当前 P0-A，并分别链接到 E06 §11 与 E04 §13 的实施和验收矩阵。 |
| 2026-08-12 | 开发环境迁移到 abcd 四机体系；统一默认 `ssh serving-b -> openeuler-2403-DS -> /home/sj/git/data-service`，停止使用旧 HOST1/HOST2/next 双跳口径；旧 1/2号机结果保留为 historical evidence，新增 R12 防止跨机器误用性能结论。 |
| 2026-08-12 | 现场只读验证 serving-b 与目标容器；发现 remote repo 仍为 `e854fa0` dirty、落后 local `6179941`，且 Git 有 dubious ownership 检查。新增 R13，并把无损收口远端执行基线设为 E06/E04 开工前置门禁。 |
| 2026-08-12 | 完成 PhaseE-06 Trace v2：代码提交链至 `5f47d22`，在 serving-b 隔离树完成 G0～G10；regular/fallback/cancel/incomplete/drop/HTTP/disabled 证据同步至 `results/phasee06_trace_v2_20260812/`。P0-A 从“E06+E04 待实施”推进为“E06 完成，仅余 E04 Monitor v2”；R11 降为 Monitor 单侧风险。 |
