# PhaseC-00: UFlow × ub-sim 适配总路线

> 开发环境统一口径（2026-08-31）：UFlow 开发、构建和默认验证入口为 `ssh serving-b` → `openeuler-2403-DS`；容器执行用户为 `root`，workspace 根目录为 `/home/sj`，UFlow 仓库为 `/home/sj/git/data-service`，ub-sim 权威 checkout 为 `/home/sj/git/ub_sim`。`serving-a/c/d` 只在文档明确要求物理多机或故障验证时使用。ub-sim 的 2/4/8 个 logical node 不等同于 2/4/8 台 serving 物理机。详见 [Phase0-development-environment.md](Phase0-development-environment.md)。

Last reviewed: 2026-08-31

Status: `active-implementation`; C01 已以 `latest-repaired-ready / manual-active` 关闭首轮基线，C02 中立合同任务可以基于固定 repair candidate 开工；UFlow 的 UB provider/remote placement 尚未实现。

## 1. 文档定位

PhaseC-00 是 Phase C 的总设计和派工入口，负责回答：

1. UFlow 为什么以及如何接入 ub-sim；
2. UFlow 与 ub-sim 各自负责什么，边界在哪里；
3. Phase C 按什么顺序实施，前后任务的输入和出口是什么；
4. 数据共享、拓扑感知路由、异步传输和故障回退如何形成一条验收主线；
5. 什么证据可以把 Phase C 从“规划”升级为“完成”。

本文不直接实现某一条 transport，也不把 ub-sim 的既有能力表述成 UFlow 已支持。每个子任务必须在自己的 PhaseC-XX 文档中维护实现、命令、证据和未完成项。

一句话目标：

> 让 UFlow 在不依赖 simulator 私有语义的前提下，通过标准 Topology、Capability、Placement、TransferPlan 和 TransferEvent 接口驱动 ub-sim，先闭环跨节点数据共享与拓扑感知路由，再进入真实 UB 硬件适配。

## 2. 当前基线与启动判断

### 2.1 UFlow 当前具备的底座

Phase A/B/E 已经提供 Phase C 可复用的本机底座：

- `DataObject / DataPlacement / DataHandle / Lease` 对象与生命周期模型；
- HBM、DDR、SSD 本机 placement 和传输执行；
- `EstimateCost / PlanTransfer / SubmitTransfer / Poll/Wait/CancelEvent` 异步优先接口；
- 本机 direct、relay、runtime fallback 及 Monitor v2 / Trace v2 可观测合同；
- 独立 daemon 与 Python SDK。

尚未具备：

- ub-sim provider/executor；
- 跨节点稳定地址或 block descriptor；
- 动态 TopologyProvider / CapabilityProvider；
- remote DDR、remote SSD/SSU 的 UFlow 端到端实现；
- 跨节点 cost 反馈、故障重规划和数据一致性闭环。

### 2.2 ub-sim 当前可利用的能力

本地镜像为 `/Users/sean/Desktop/gitee/MemoryService/ub_sim`；正式开发与验证使用 `serving-b/openeuler-2403-DS:/home/sj/git/ub_sim`。从当前源码和历史报告可确认，ub-sim 已包含：

- 2/4/8 logical-node topology 与 QEMU/headless harness；
- GVA/GSVA、OBMM remote mapping、一致性和生命周期验证路径；
- UB-attached NPU/SSD、异步 load/completion、故障与延迟模型；
- 独立 `mem_service` 子模块、provider 合同和多类 smoke/matrix 脚本；
- Rust 行为模型、guest Linux、QEMU/kernel/UAPI 和模型 workload。

这些是 ub-sim 自身能力或历史证据，不是 UFlow 适配通过证据。Phase C 必须建立 UFlow 自己的对象身份、placement、路由决策、事件和 Trace 证据，禁止直接引用 simulator 成功结果冒充 UFlow 成功。

### 2.3 当前启动结论

原始 upstream 仍不能直接作为正式适配基线：

- 本地 ub-sim `HEAD=7aec390`，在 2026-08-31 刷新远端后落后 `origin/master=0fecb3b` 23 个提交；
- 当前 checkout 的 `mem_service` HEAD 与根仓 gitlink/lock 不一致；
- 最新 `origin/master` 锁定 `mem_service=4b13ec0`，但该对象当前无法从 `mem_service` 远端取得；
- 最新 root 还锁定不可获取的 `qemu@15951308`；因此 raw upstream 不是 reproducible/latest-ready。

PhaseC-01 已在不修改 canonical dirty checkout 的前提下建立隔离 repair candidate `c68cfb8ab139312cc9d41fb26f0ebcd3b00c7072`：显式修复 `mem_service/QEMU` gitlink 和 bridge ABI，U0-U4、2-node provider conformance 与 4-node canary 全部通过。该状态是 `latest-repaired-ready / manual-active`，允许 C02～C07 内部开发，但不能冒充 upstream plain `latest-ready` 或对外 release。证据见 [phasec01_ub_sim_20260831_01](../results/phasec01_ub_sim_20260831_01/)。

## 3. Phase C 架构边界

### 3.1 权威职责

| 层 | 权威职责 | Phase C 禁止事项 |
|---|---|---|
| UFlow daemon/core | object identity、placement、lease、consistency、plan、cost、event、fallback | core 中出现 QEMU、SIM_DEC、OBMM 私有控制命令或硬编码 topology |
| UFlow ub-sim adapter/provider | 把中立合同翻译为 ub-sim topology、mapping、block 和 completion 操作 | 绕过 UFlow catalog 私建第二套对象身份或生命周期 |
| ub-sim | 模拟 UB 节点、链路、GVA/GSVA、OBMM、SSD/SSU、异步完成和故障 | 决定 UFlow 的业务 placement policy，或成为生产 runtime 依赖 |
| serving / PyPTO | 表达数据用途、约束、deadline、prefetch/migrate 请求并消费 event | 直接选择 OBMM/QEMU/URMA transport，或持有跨节点裸指针 |

架构法则：

1. ub-sim 是 Phase C 的验证平台，不是 UFlow 生产核心依赖。
2. UFlow core 只依赖 transport-neutral contract；simulator 细节必须留在 adapter/provider。
3. 数据面可由 provider 直接传输，控制面不得代理大 payload。
4. 跨节点只传播稳定 handle/descriptor，不传播仅在某进程有效的裸 pointer。
5. 每次传输都必须产生可等待、可追踪且只有一个 terminal outcome 的 event。
6. `logical direct`、`simulated direct` 和真实物理 UB direct 必须分开标注。
7. Phase C 验证 remote DDR/SSD 与路由语义；remote HBM direct/relay 的正式实现归 Phase D。

### 3.2 目标调用链

```text
client / serving / PyPTO
  -> UFlow SDK
  -> UFlow daemon catalog + lease
  -> TopologyProvider + CapabilityProvider
  -> CostModel + TransferPlanner
  -> TransferExecutor
  -> UbSimProvider
  -> ub-sim topology / OBMM-GSVA / UB-SSD / completion
  -> TransferEvent + Trace v2 + Monitor v2
```

`UbSimProvider` 必须是可替换 provider。后续真实 UB/URMA/SSU provider 应复用同一上层请求、计划、事件和一致性合同。

## 4. Phase C 最小合同

PhaseC-02 会冻结正式 schema；本节先固定不可缺少的语义面。

### 4.1 TopologySnapshot

至少描述：

```text
snapshot_id / generation / observed_at
nodes[]
devices[]: HBM | DDR | SSD | SSU | UB_ENDPOINT
links[]: src / dst / kind / state / bandwidth / latency / hop
domains[]: local_node | ub_domain | supernode
```

要求：同一份 `TransferPlan` 必须引用明确的 `snapshot_id/generation`。拓扑变化后，旧计划不得静默继续执行。

### 4.2 CapabilitySnapshot

至少描述：

```text
remote_ddr_import
remote_block_read / remote_block_write
async_completion
checksum / fence / coherence profile
direct_hbm
max_transfer_bytes / alignment / queue_depth
fallbacks[]
```

capability 不得根据“脚本存在”推断，必须来自 provider 探测或明确的 scenario manifest。

### 4.3 跨节点 Placement 与 Handle

首期必须支持：

- `medium=UB_SHMEM`：remote DDR export/import 或 OBMM/GSVA mapping descriptor；
- `medium=SSU|SSD`：remote block/object descriptor；
- `domain=ub_domain|supernode`；
- `node_id/device_id/generation/range/access_mode`；
- 可验证 object/version/checksum，不允许只有地址没有对象身份。

### 4.4 Provider 与 Event

Provider 最小操作面：

```text
discover_topology
discover_capabilities
publish / import / revoke remote memory
submit block read / write
poll / wait / cancel completion
inject or report failure
read counters / health
```

每次执行必须记录：planned path、actual path、fallback reason、source/destination、topology generation、provider provenance、bytes、timestamps、terminal outcome 和 checksum verdict。

## 5. Phase C 任务路线

```text
C00 总路线
  -> C01 ub-sim latest-ready 基线
  -> C02 中立合同与 adapter 骨架
  -> C03 Topology/CapabilityProvider
  -> C04 remote DDR 数据共享
  -> C05 remote SSD/SSU block
  -> C06 topology-aware routing + CostModel
  -> C07 async completion + failure/fallback
  -> C08 2/4/8 logical-node UFlow E2E 验收
  -> Phase D remote HBM direct/relay
```

### C01：ub-sim 上游同步与内部自测

目标：持续给出一个 clean、可获取、可构建、可跑基础 matrix 的 `latest-ready` ub-sim 基线。

出口：根仓和所有 gitlink/lock 一致；轻量测试、2-node integrated smoke 和按变更影响选择的 4/8-node canary 通过；生成可机读 manifest。详细要求见 [PhaseC-01-ub-sim-upstream-sync-and-selftest.md](PhaseC-01-ub-sim-upstream-sync-and-selftest.md)。

### C02：UFlow—ub-sim 中立合同与 adapter 骨架

目标：冻结 Topology、Capability、remote placement、provider、completion 和 error schema；创建 simulator provider，但不实现完整数据路径。

出口：

- UFlow core 不依赖 ub-sim/QEMU/OBMM 头文件或命令；
- mock provider 与 ub-sim provider 使用同一 conformance suite；
- wire/schema 有 version 和 compatibility 负例；
- disabled provider 不影响现有本机路径。

### C03：TopologyProvider 与 CapabilityProvider

目标：把 ub-sim scenario、logical node/device/link 和运行时健康状态转换为 UFlow snapshot。

出口：

- 2/4/8 logical-node topology 可稳定发现；
- node/link/device/capability 有稳定 ID 和 generation；
- link down、device unavailable、snapshot stale 可观测且 fail closed；
- Monitor/Trace 可展示 topology generation 与 provider health。

### C04：remote DDR 数据共享

目标：用 UFlow DataObject/Placement/Handle 驱动 remote DDR publish/import/read/write/revoke，优先映射 ub-sim OBMM/GSVA 能力。

出口：

- 两个独立 client/daemon address domain 之间不传播裸 pointer；
- immutable share 与 single-writer 生命周期正确；
- range、permission、generation、stale handle、revoke 和 checksum 负例通过；
- 数据路径无 JSON/file/in-process payload 旁路。

### C05：remote SSD/SSU block

目标：把 UFlow SSD/SSU placement 和 async block transfer 映射到 ub-sim UB-attached SSD/Block 语义。

出口：

- remote block read/write 返回统一 TransferEvent；
- SSD→DDR 与 SSD→本地 HBM relay 可计划和执行；
- object/version/range/checksum/completion 一致；
- queue busy、timeout、device unavailable 和 partial failure 明确失败或回退。

### C06：拓扑感知路由与 CostModel

目标：让路径由 topology + capability + cost 决定，而不是由介质名或硬编码 node id 决定。

出口：

- 至少两套 cost profile 改变同一请求的最优路径；
- 多副本选择最近可用 placement；
- 路径输出 latency/bandwidth/setup/hop/contention/failure 等可解释项；
- plan 绑定 topology generation；实际观测可回写校准但不得污染确定性测试。

### C07：异步 completion、故障注入与重规划

目标：统一 remote memory、block、relay 的异步事件和 failure semantics。

出口：

- submit/poll/wait/cancel 和唯一 terminal outcome 通过；
- link down、node down、timeout、capacity exhausted、stale topology、checksum mismatch 覆盖；
- fallback 或 replan 有显式父子关系和原因；
- 失败后 lease、intermediate placement、queue 和 simulator process 无泄漏。

### C08：Phase C E2E 收口

目标：用同一 UFlow CLI/SDK 和同一 evidence schema 完成 2-node golden path，再扩到 4/8 logical nodes。

出口：

- 数据共享：remote DDR 与 remote SSD/SSU 各有一条无旁路闭环；
- 路由：multi-placement、cost 变化和 link failure 会产生预期路径切换；
- 可观测：UFlow event、Trace v2、Monitor v2 与 simulator counter 可按 event/object id 对齐；
- 2-node 必须每次通过，4/8-node 按 release gate 通过；无残留 daemon/QEMU/socket。

## 6. Golden 场景矩阵

| 场景 | 首要验证 | 最小规模 | 归属 |
|---|---|---:|---|
| remote DDR immutable share | Object/Handle 跨 address domain 与 checksum | 2 logical nodes | C04 |
| remote DDR single-writer | owner、dirty/ready、revoke、stale generation | 2 | C04 |
| remote SSD block → local DDR | block descriptor、async completion、range/checksum | 2 | C05 |
| remote SSD block → local HBM | relay plan、中间 placement、事件链 | 2 | C05 |
| 多副本最近路由 | topology/cost 驱动 placement 选择 | 4 | C06 |
| 两套 cost profile | 相同对象和请求产生不同但可解释的最优路径 | 4 | C06 |
| link/node down | replan/fallback/fail-closed | 4 | C07 |
| capacity exhausted/queue busy | 资源约束进入 planner 与事件 | 2/4 | C07 |
| scale-out canary | 相同合同扩到 8-node，不新增 simulator 私有 API | 8 | C08 |

性能不是 Phase C 首轮的主退出条件。首轮先证明语义、选路、事件、故障和证据正确；带宽/延迟数值只用于确定性 cost profile 与相对选路，不得外推为真实 UB 硬件性能。

## 7. 验收门禁

| Gate | 条件 | 证据 |
|---|---|---|
| C-G0 baseline | C01 `latest-ready=pass`；根仓、gitlink、lock、环境唯一 | baseline manifest、fetch/submodule/test logs |
| C-G1 contract | 中立 schema、version、mock/provider conformance 通过 | schema、unit/contract test |
| C-G2 discovery | 2/4/8 topology 和 capability 可发现、可失效 | snapshots、negative cases |
| C-G3 sharing | remote DDR publish/import/revoke 与数据校验通过 | object/placement/event/trace |
| C-G4 block | remote SSD/SSU async block 与 relay 通过 | plan/event/checksum/provider trace |
| C-G5 routing | 不同 topology/cost/capability 得到预期路由 | decision matrix、explanation |
| C-G6 failure | 故障注入、fallback/replan、清理通过 | failure matrix、cleanup manifest |
| C-G7 E2E | 2-node golden + 4/8-node canary 无旁路闭环 | immutable acceptance bundle |

任一 Gate 不得只写 `PASS`。必须绑定：UFlow commit、ub-sim commit、全部子模块 commit、scenario hash、机器/容器、命令、开始结束时间、exit code、raw log、summary 和 cleanup 结果。

## 8. 开发与验证环境

### 8.1 默认控制入口

```text
ssh serving-b
  -> openeuler-2403-DS
  -> user root
  -> workspace /home/sj
  -> /home/sj/git/data-service
  -> /home/sj/git/ub_sim
  -> actual device 现场探测并记录
```

ub-sim canonical checkout 固定为 `/home/sj/git/ub_sim`，但它当前包含历史产物和未跟踪文件，只作为来源与 last-known state。C01 candidate 必须在 `/home/sj` 下的明确隔离目录运行，例如 `/home/sj/phasec01_runs/<run_id>/ub_sim`；不能修改或覆盖 canonical dirty 仓库。

### 8.2 logical node 与 physical host

- `2/4/8 logical nodes`：ub-sim 场景规模，通常可以位于一台物理 host 上。
- `serving-a/b/c/d`：真实四机执行环境，只有 C08 明确要求物理多机对照、资源隔离或故障验证时才扩展。
- 不得用“ub-sim 4-node 通过”表述成“serving 四机通过”，反之亦然。

涉及 QEMU、模型 workload、多节点或高资源验证时必须在合适的远端环境执行；本地 Mac 仅允许文档、静态审计和明确轻量的合同检查。

## 9. 证据与状态更新

Phase C 结果建议统一写入：

```text
remote: /tmp/proj_output/<phasec_run_id>/
local:  data-service/results/<phasec_run_id>/
```

每个结果目录至少包含：

```text
baseline.json
environment.txt
topology.json
capabilities.json
commands.log
events.json
trace_events.json
checksums.json
cleanup.txt
summary.md
```

Phase C 状态升级规则：

- C00 文档建立只表示 `active-planning`，不表示 UB 适配已开始；
- C01 的 plain `latest-ready` 或带完整 patch/manifest/U0-U4 的 `latest-repaired-ready` 通过后，C02 才可进入实现；repaired candidate 不得用于 release；
- 只有 C04/C05 的真实 UFlow provider 通过，才可写“remote DDR/SSD supported”；
- 只有 C06/C07 通过，才可写“topology-aware routing supported”；
- 只有 C08 acceptance bundle 完整，Phase C 才可标记 `done-first-version`。

## 10. Phase C Definition of Done

Phase C 首版完成必须同时满足：

1. 有一个可复现、持续维护的 ub-sim latest-ready 基线；
2. UFlow core 保持 simulator/transport-neutral；
3. remote DDR 数据共享和 remote SSD/SSU block 由 UFlow 对象/placement/handle 驱动；
4. topology/capability/cost 会实质改变路径选择，并输出解释；
5. 所有远端操作统一进入 TransferEvent、Trace v2 和 Monitor v2；
6. 故障注入会触发显式 fallback/replan 或 fail closed，且无资源泄漏；
7. 2-node golden 与 4/8-node canary 有当前 revision、无旁路、可复核证据；
8. Phase D 可以只新增 remote HBM capability/executor，而不需要重写 Phase C 的对象、路由和事件合同。

## 11. 当前 Future Work

当前主门禁转为 PhaseC-02：基于 `c68cfb8` 冻结 transport-neutral topology/capability/placement/handle/TransferEvent 合同，确保后续数据共享与 topology-aware routing 不把 QEMU/OBMM 私有语义泄漏到 UFlow core。

并行维护项：上游仍需原子修复 `origin/master@0fecb3b` 的 `mem_service@4b13ec0`、`qemu@15951308` 可获取性并吸收 bridge compatibility alias；C01 guard 仍需接入每日/事件触发。在此之前每个 Phase C agent 必须绑定 `c68cfb8` 或归档 patch，不能自行选择“相近”子模块版本。
