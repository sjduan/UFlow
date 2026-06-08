# Data Service Framework Scope

Last reviewed: 2026-06-08

## 1. 背景

Data Service 的长期目标不是只做一个本机 allocator，而是面向未来超节点架构做统一数据管理。未来机器形态中，多个节点通过 UB 链路连接，不同节点上的 DDR、SSD、HBM(NPU) 可能都成为可寻址、可迁移、可异步访问的数据资源。

统一数据管理本身不是新概念。910 / a2a3 代际上，面向 NPU、CPU DDR、SSD、分布式文件系统的数据管理和缓存系统已经比较成熟；但它们大多运行在“协议分段、通道分段、地址空间分段”的机器假设上：HBM、Host DDR、远端 DDR、远端 SSD 往往需要经过不同 API、不同协议栈和显式搬运路径才能互通。

950 代际开始全面引入 UB 链路后，这个前提发生变化。`UBL128_serving.md` 中的 UBL128 HBD、SU/SO/UBG、UB Urma/uRPC、SSU LBA-direct KV 存储等设计，把 NPU、CPU、SSU 等资源放到更统一的 UB 数据平面中：热路径控制面可以走 uRPC over UB Urma，数据面可以走 Urma read/write、UB shmem import/export、UB block 异步 command。960 及后续代际可以视为这一 UB-native 数据平面的继续增强；本文不绑定 960 的具体硬件参数，而是保留“统一地址、直连/逻辑直连、异步完成、拓扑感知”的能力抽象。

核心诉求：

- 节点间 DDR/SSD 地址打平，允许通过 UB 链路访问远端节点数据。
- 同一节点内 DDR、SSD、HBM(NPU) 互通互用，支持快速、动态、异步读取。
- 节点间 HBM 是否直通取决于硬件能力；不支持直通时，由 Data Service 选择最少跳数的数据路径。
- 不同介质、不同 DDR、不同链路的访问速度不同，Data Service 必须把带宽、延迟、拥塞、容量、拓扑距离纳入数据放置和迁移决策。
- 功能验证优先通过 ub-sim 建立仿真闭环。

### 1.1 950 / 960 代际的新增硬件前提

根据 `UBL128_serving.md`、`linqu_data_system.md`、`sharded_tensor.md` 和 `runtime_async.md`，Data Service 需要建立在以下新前提上：

- **UBL128 HBD 是天然高带宽域**：一个 UBL128 由 8 台 PC16、128 颗 Ascend 950 NPU 组成，域内通过 SU 单层交换形成 full any-to-any 高带宽互联。
- **SO / UBG 提供跨 HBD any-to-any 数据平面**：跨 UBL128 的 NPU、CPU、SSU 通过 SO 网络互通，用于 KV 迁移、prefix 共享、prefill/decode 解耦和跨域数据交付。
- **UB Urma 提供可靠远端内存访问语义**：热路径 RPC 采用 uRPC over UB Urma，数据面采用 Urma read/write，避免 gRPC/TCP/IP 栈进入计算与 KV 热路径。
- **SSU 是 UB-attached 存储设备**：KV / prefix / 权重等热数据可以通过 `(UB_ADDRESS, LBA, length)` 或 SSU LBA 直接建模，而不是先落成 POSIX 文件再经 CPU 读取。
- **UB shmem 让远端内存可被导入为本地映射**：`ubmem export/import` 允许远端 DDR 或可发布区域在本地形成 mapped base，CPU load/store 或 TLOAD/TSTORE 可以按映射访问。
- **异步硬件引擎成为数据流动的常态**：SDMA、URMA、RoCE、CCU 等 completion 需要被 runtime 依赖系统感知，避免数据未完成就释放 buffer 或唤醒消费者。

### 1.2 与 910 / a2a3 代际的关键区别

| 维度 | 910 / a2a3 代际的常见形态 | 950 / 960 UB-native 形态 | 对 Data Service 的影响 |
|---|---|---|---|
| 地址空间 | HBM、Host DDR、远端内存、SSD 多为分离地址域 | UB 使 DDR/SSD/部分可发布内存具备统一寻址或 import mapping 语义 | DataHandle 必须能表达 global address、UB import、block descriptor，而不是只包装本地 ptr |
| 数据搬运 | Host↔Device DMA、RoCE/RDMA、文件 IO、runtime copy 各自为政 | SU/SO/服务器内 UB 形成统一数据平面，Urma read/write 和 uRPC 共栈但可用 QP 隔离 | Transfer Engine 需要统一规划 DMA、UB、block、relay，而不是写多个孤立 adapter |
| 存储访问 | 模型/缓存多经 CPU 文件系统或独立 KV/cache 服务 | SSU 可在 SO 上作为 LBA-direct 热数据面，KV bytes 可经 Urma 直接读写 | KV/prefix/热权重可绕开 POSIX FS 热路径，按 block/extent 管理 |
| 网络职责 | 控制面、数据面、运维面容易混用 | UBL128 文档明确 SU 承载域内 EP/DP，SO 承载 KV bytes + hot-path uRPC，DCN 承载外部/运维/POSIX FS | Data Service 必须把协议选择和网络选择绑定，避免热数据误走 DCN 或挤占 SU |
| 远端 HBM | 通常依赖显式中转或框架级 copy | 能否直通取决于硬件；不支持时用最少跳数 relay | Cost Model 需要抽象 direct capability 和 relay path |
| 同步模型 | 多数以调用返回或同步 copy 为边界 | URMA/SDMA/block 都是异步 completion，远端通知可用 counter/atomic | Data Service 的 event 必须和 PyPTO DAG / completion queue 对齐 |
| 设计重点 | 管理单机/小集群内存池、缓存和 copy | 管理跨介质、跨节点、跨 UB 域的数据位置和流动 | 创新点从“会分配内存”转向“会选择数据路径和生命周期” |

### 1.3 重构统一数据服务的意义

这次重构的价值不在于重新发明一个缓存系统，而在于把 950+ 硬件已经提供的 UB-native 能力变成可编程、可观测、可调度的数据服务：

- **把硬件直连能力变成软件可用的统一抽象**：上层通过 DataObject / DataPlacement / DataHandle 表达数据，不关心底层是 local HBM、UB import、SSU LBA，还是 relay path。
- **把异构协议差异收敛到 Transfer Engine**：DDR↔HBM DMA、UB shmem、Urma read/write、UB block、DFS read 统一变成可规划、可回退、可异步等待的 TransferPlan。
- **让 pypto-serving 从数据路径细节中解耦**：serving 只表达权重、KVCache、prefix、生命周期和 policy，不在业务代码里写死 CPU staging、NPU upload、远端读取或 SSU 路由。
- **让调度器可以基于数据位置调度计算**：prefill/decode 放置、KV owner 迁移、prefix 副本选择、远端 HBM relay 都可以由 cost model 参与决策。
- **为 960 及后续代际保留演进空间**：如果未来 HBM direct、SSU direct-to-HBM、UB multicast、跨域 coherence 等能力增强，只需要扩展 capability 和 backend，不重写上层接口。
- **形成 ub-sim 可验证的系统能力**：数据路径、异步 completion、故障注入、带宽/延迟模型都可以先在 ub-sim 中验证，再映射到真实硬件。

### 1.4 文档依据

本节判断主要来自以下顶层文档：

- `pypto_top_level_documents/UBL128_serving.md`：950 UBL128 HBD、PC16、SU/SO/DCN、UBG、Urma/uRPC、SSU LBA-direct KV、prefill/decode 解耦路径。
- `pypto_top_level_documents/linqu_data_system.md`：Lingqu shmem、block、DFS、DB 四类 UB 数据服务基础抽象。
- `pypto_top_level_documents/sharded_tensor.md`：UB `ubmem export/import`、远端 share 映射、direct load/store 或 TLOAD/TSTORE 访问。
- `pypto_top_level_documents/runtime_async.md`：SDMA、URMA、RoCE、CCU 等异步硬件引擎 completion 与 runtime DAG 生命周期的关系。
- `pypto_top_level_documents/pypto-runtime-arch-docs/02-logical-view/04-memory.md` 和 `11-machine-memory-model.md`：Memory Scope、Memory Region、IMemoryOps、GlobalAddress、a2a3/a5 平台透明边界。

## 2. 总体定位

Data Service 是 PyPTO/pypto-serving 之下、硬件/运行时之上的数据管理控制面和数据面组合。

它负责回答四类问题：

1. 数据在哪里：权重、KVCache、激活、checkpoint、文件块分别落在什么节点、设备、介质、offset。
2. 数据怎么访问：本地指针、UB import mapping、DMA copy、block read、DFS read、跨节点 relay，哪条路径可用。
3. 数据怎么移动：同步、异步、预取、回写、迁移、复制、失效、降级。
4. 数据是否值得移动：根据带宽、延迟、热点、容量、拓扑距离、数据生命周期和一致性需求决策。

一句话边界：

> Data Service 管“跨介质、跨设备、跨节点的数据位置、引用、移动和一致性”；PyPTO/Simpler 管“任务执行和 kernel 参数消费”；pypto-serving 管“模型请求、调度和业务语义”。

## 3. 需要覆盖的资源层

### 3.1 单节点内资源

单节点内要统一管理：

- CPU DDR：host tensor、metadata、staging buffer、CPU KV/weights fallback。
- Local SSD / NVMe：模型权重冷存、KV swap、checkpoint、请求缓存。
- NPU HBM：权重常驻、KVCache 常驻、kernel 输入输出、device workspace。
- DMA / copy engine：DDR↔HBM、SSD↔DDR、SSD↔HBM 可用路径。
- 共享内存 / mmap：同节点多进程间 CPU 内存共享。

### 3.2 超节点内资源

超节点内要统一管理：

- Remote DDR：通过 UB shmem/import 或 explicit open-share-memory API 访问。
- Remote SSD / UB-SSU：通过 `(UB_ADDRESS, LBA, length)` block descriptor 异步读写。
- Remote HBM：硬件支持直通时直接映射或远端 DMA；不支持时走 HBM→DDR→UB→DDR/HBM relay。
- UB 链路：作为跨节点主要数据路径，需要建模拓扑、带宽、延迟、拥塞和可达性。

### 3.3 集群/持久化资源

后续纳入：

- DFS：全局文件 namespace，模型、checkpoint、日志和数据集的持久层。
- Object / KV DB：小对象、metadata、lease、状态同步。
- 多副本持久缓存：热权重、公共 KV 前缀、共享 embedding cache。

## 4. 核心抽象

### 4.1 DataObject

DataObject 是上层看到的逻辑数据对象。

典型字段：

```text
object_id
object_type: weight | kv_cache | activation | file_block | checkpoint | metadata
shape / dtype / layout
size_bytes
lifetime: model | request | session | persistent | temporary
consistency: immutable | single_writer | replicated | transactional
owner / tenant / namespace
```

### 4.2 DataPlacement

DataPlacement 描述某份数据的一个物理副本或分片。

典型字段：

```text
placement_id
object_id
node_id
device_id
medium: DDR | SSD | HBM | UB_SHMEM | DFS
address_kind: local_ptr | device_ptr | ub_addr_lba | ub_import_base | file_path_offset
offset / nbytes
state: creating | ready | dirty | stale | evicting | failed
performance_class: latency_ns / bandwidth_gbps / hop_count
```

### 4.3 DataHandle

DataHandle 是应用和 runtime 传递的稳定引用，不等于裸指针。

它可以解析为：

- 本地 CPU pointer。
- 本地 NPU device pointer。
- `ContinuousTensor(child_memory=True)`。
- UB import 后的 local mapped base + offset。
- 远端 block descriptor。
- 异步 transfer handle。

原则：

- 跨进程、跨节点不直接暴露裸 pointer。
- pointer 只在明确的 address domain 内有效。
- 上层只持有 handle，Data Service 负责解析和路径选择。

### 4.4 TransferPlan

TransferPlan 是一次数据读取、迁移、同步或预取的执行计划。

典型字段：

```text
src_placement
dst_placement
operation: read | write | migrate | replicate | prefetch | evict | sync
path: direct | ub_import | dma | block | relay | dfs
hop_count
estimated_latency
estimated_bandwidth
async_event
fallback_path
```

## 5. 数据访问路径分类

### 5.1 本地最快路径

同一 address domain 内直接引用：

- HBM 内 kernel 直接读写 device ptr。
- Simpler `ContinuousTensor(child_memory=True)` pass-through。
- CPU 进程内普通 pointer。
- 同节点 shared memory/mmap。

目标：不复制，只传 descriptor。

### 5.2 本地跨介质路径

同节点不同介质之间：

- DDR↔HBM：DMA / worker copy。
- SSD↔DDR：async block/file read。
- SSD↔HBM：优先 direct storage-to-device；不支持时 SSD→DDR→HBM。

目标：异步化、可 overlap、可预取。

### 5.3 跨节点 UB shmem 路径

远端 DDR 或可发布内存：

- 优先 `ubmem export/import`。
- import 后本地得到 mapped base address。
- CPU 可 direct load/store。
- MTE/DMA 可用 `TLOAD/TSTORE` 或等价数据通道访问。

目标：把远端 DDR 变成本地可寻址映射，减少 per-access RPC。

### 5.4 跨节点 block 路径

远端 SSD / UB-SSU：

- 使用 `(UB_ADDRESS, LBA, length, flags)`。
- 异步提交到 device command ring。
- completion queue 通知 Data Service 和 runtime。
- 读结果直接落到目标 buffer，写完成后释放生产者依赖。

目标：绕过 CPU 中转，服务冷数据和持久化数据。

### 5.5 跨节点 HBM 路径

远端 HBM：

- 如果硬件支持 HBM 直通：直接 import/mapping 或远端 DMA。
- 如果不支持：选择 relay path。
- relay path 示例：
  - remote HBM → remote DDR → UB → local DDR → local HBM
  - remote HBM → remote DDR → UB → local HBM
  - remote HBM → peer HBM relay → local HBM

目标：硬件能力可插拔，路径选择以最少跳数和最低成本为准。

## 6. Data Service 模块范围

### 6.1 Metadata Service

负责：

- object registry。
- placement registry。
- topology registry。
- capability registry。
- lease / owner / lifetime。
- namespace / tenant 隔离。

### 6.2 Placement Manager

负责：

- 数据初始放置。
- 副本数量。
- 分片策略。
- 热点迁移。
- 冷热分层。
- eviction。
- policy 选择。

### 6.3 Transfer Engine

负责：

- 同步/异步 copy。
- DMA、UB、block、DFS、relay path 执行。
- event/poll/wait。
- retry/fallback。
- 带宽节流和队列管理。

### 6.4 Cost Model

负责：

- 记录介质特性：DDR、SSD、HBM。
- 记录链路特性：UB bandwidth/latency/hop。
- 记录历史统计：实际耗时、拥塞、失败率。
- 为 Placement Manager 和 Transfer Engine 提供 cost estimate。

### 6.5 Consistency Manager

负责：

- immutable 数据：权重、只读文件块。
- single-writer 数据：KVCache、临时 activation。
- replicated 数据：热缓存、副本。
- dirty/stale 状态。
- flush、invalidate、barrier、lease。

### 6.6 Runtime Adapter

负责和现有系统对接：

- pypto-serving allocator wrapper。
- Torch/NPU allocator hook。
- PyPTO/Simpler `ContinuousTensor(child_memory=True)`。
- ub-sim 仿真接口。
- DFS/block/shmem 底层适配。

## 7. 与 pypto-serving 的首期接口关系

pypto-serving 首期只需要看到一个较窄接口：

```python
with data_service.allocator_scope(
    placement="npu",
    tags={"weight", "kv_cache"},
    policy="prefer_local_hbm",
):
    engine.init_model(...)
    engine.generate(...)
```

首期接管：

- Qwen3 NPU 权重。
- Qwen3 NPU KVCache。

首期不强制接管：

- token ids。
- hidden states。
- logits。
- rope。
- 小型临时 buffer。

Data Service 对 serving 暴露的是 handle 和 tensor ref，不要求 serving 理解远端 DDR/SSD/HBM 路由细节。

## 8. 开放数据控制接口

除了 allocator scope 这种自动接管路径，Data Service 后续还需要开放一组显式数据控制接口。这些接口可以被 pypto-serving、PyPTO runtime、离线预热进程、调度器、运维工具或其它业务进程调用，用于手动控制数据共享和流动。

### 8.1 两类接口形态

第一类是自动路径：

```python
with data_service.allocator_scope(...):
    # torch allocation / model init / generate 自动进入 Data Service policy
```

特点：

- 面向 pypto-serving 的默认热路径。
- 上层只表达 placement/policy/tag。
- Data Service 自动决定申请、上传、复用、释放。

第二类是显式控制路径：

```python
handle = data_service.open("model://qwen3/layer.0.q_proj")
dst = data_service.allocate(size, medium="HBM", node="node0", device="npu0")
event = data_service.migrate(handle, dst, async_=True)
event.wait()
```

特点：

- 面向手动预热、迁移、同步、共享和调试。
- 调用方显式指定 object、目标位置、同步语义和策略约束。
- 适合其它进程参与数据生命周期管理。

### 8.2 数据对象管理接口

建议接口：

```text
create_object(type, shape, dtype, lifetime, consistency, namespace) -> DataHandle
open(uri_or_object_id) -> DataHandle
close(handle)
delete(object_id)
describe(handle) -> DataObject + placements
list(namespace, filters)
```

用途：

- 注册权重、KVCache、checkpoint、文件块、共享 activation。
- 让多个进程通过稳定 object id 访问同一份逻辑数据。
- 查询数据当前有哪些 placement 和副本状态。

### 8.3 显式放置和共享接口

建议接口：

```text
allocate(size, medium, node, device, policy) -> DataHandle
publish(handle, scope, mode) -> ShareHandle
import_share(share_handle, node, device, mode) -> DataHandle
pin(handle, reason)
unpin(handle)
evict(handle, target_medium=None)
```

语义：

- `publish` 把本地数据发布为可被其它节点或进程访问的共享区域。
- `import_share` 在目标节点建立本地可访问引用，优先走 UB import mapping。
- `pin/unpin` 控制数据是否允许被 eviction 或迁移。
- `evict` 将热介质上的数据下沉到 DDR/SSD/DFS。

### 8.4 迁移、复制和同步接口

建议接口：

```text
migrate(src, dst_hint, async=True, policy=None) -> TransferEvent
replicate(src, replica_policy, async=True) -> list[TransferEvent]
prefetch(src, dst_hint, deadline=None) -> TransferEvent
sync(src, dst=None, mode="flush" | "invalidate" | "barrier") -> TransferEvent
flush(handle, target=None) -> TransferEvent
invalidate(handle, placement=None)
wait(event)
poll(event) -> TransferStatus
cancel(event)
```

语义：

- `migrate` 移动主副本或改变 preferred placement。
- `replicate` 增加副本，常用于 immutable 权重或热点数据。
- `prefetch` 在数据即将被消费前异步搬运。
- `sync/flush/invalidate` 处理 dirty/stale 状态和一致性边界。
- 所有长操作默认返回 event，允许和 serving decode loop 或 PyPTO DAG overlap。

### 8.5 路径选择和策略约束接口

建议接口：

```text
plan_transfer(src, dst_hint, constraints) -> TransferPlan
estimate_cost(src, dst_hint) -> CostEstimate
set_policy(namespace_or_object, policy)
get_topology() -> TopologySnapshot
get_stats(scope=None) -> DataServiceStats
```

约束示例：

```text
max_hops=2
prefer_direct=True
avoid_ssd=True
deadline_us=500
allow_staging=True
require_consistent=True
```

用途：

- serving 可以提前问“权重从哪个副本读最快”。
- 调度器可以根据 topology/cost 决定请求放到哪个节点。
- 运维工具可以观察远端 DDR/SSD/HBM 的容量、拥塞和迁移队列。

### 8.6 接口使用场景

典型场景：

1. 模型预热进程把权重从 DFS/远端 SSD 预取到多个节点 HBM。
2. pypto-serving 在请求到来前预取公共 prefix KV。
3. 调度器把请求迁移到另一个节点前，同步或复制 KVCache。
4. 调试工具强制把某个 DataObject dump 到 DDR/SSD。
5. ub-sim 测试进程手动构造 remote DDR、remote SSD、remote HBM relay 场景。

### 8.7 权限和隔离

开放接口后需要明确安全边界：

- namespace / tenant 隔离。
- object owner 和 share permission。
- read-only / read-write share mode。
- lease 过期后的访问失效。
- 跨节点 import/export 必须由 Data Service registry 记录，不能依赖裸 pointer 私下流转。

## 9. 与 PyPTO/Simpler 的关系

PyPTO/Simpler 侧不应该承担全局数据管理，但需要提供几个关键能力：

- 接收 Data Service 生成的 child-memory tensor descriptor。
- 支持 host/device/remote mapped buffer 的参数描述。
- 支持异步 transfer completion 与 DAG dependency 对接。
- 在 ub-sim 中模拟 UB shmem、block、relay 路径。

已经能复用的概念：

- `ContinuousTensor(child_memory=True)`。
- `Worker.malloc/copy_to/copy_from/free`。
- runtime memory scope / region / manager / ops。
- sharded_tensor/open-share-memory 里的 UB export/import 设计。
- lingqu_block 异步 command + completion model。

## 10. 策略维度

Data Service 做 placement 和 transfer 时至少考虑：

- locality：local HBM > local DDR > remote DDR > local SSD > remote SSD，具体顺序由实测 cost 决定。
- capacity：HBM 容量小，优先放热权重、KV、即将消费的数据。
- mutability：immutable 权重适合多副本；KVCache 写多读多，需要清晰 owner。
- lifetime：model/request/session/persistent。
- bandwidth：DDR、SSD、HBM、UB 链路实际吞吐。
- latency：首 token、decode loop、batch prefetch 对延迟敏感度不同。
- hop count：跨节点 HBM 不直通时最少 relay。
- contention：多请求并发时避免压垮单条 UB 或单块 SSD。
- failure：远端节点不可达、链路降级、device OOM、transfer timeout。

## 11. ub-sim 验证范围

ub-sim 首期用于验证功能，而不是追求真实性能。

建议仿真能力：

- 多节点 topology。
- 每节点 DDR/SSD/HBM capacity。
- UB link latency/bandwidth/hop count。
- remote DDR import/export。
- remote SSD block async read/write。
- HBM direct 支持/不支持两种模式。
- relay path 选择。
- async completion queue。
- failure injection：link down、node down、timeout、capacity exhausted。

首批场景：

1. 本地 HBM 权重常驻。
2. 本地 HBM KVCache 常驻。
3. 远端 DDR 权重读取到本地 HBM。
4. 远端 SSD 权重 block read 到本地 HBM。
5. 远端 HBM 不直通时通过 DDR relay 到本地 HBM。
6. 多副本权重选择最近 placement。
7. KVCache owner 节点迁移或读取。
8. 手动 `migrate/replicate/sync/prefetch` 接口生成的 transfer plan 与执行事件。

## 12. 阶段边界

### Phase A: 单机 Data Service

- 管理本机 DDR/HBM。
- 接 pypto-serving 权重和 KVCache。
- 使用 in-process manager。
- 输出 allocation/transfer stats。

### Phase B: 单节点多介质

- 接本机 SSD。
- 支持 DDR/HBM/SSD 三层 placement。
- 支持异步 prefetch/evict。

### Phase C: UB 超节点仿真

- 接 ub-sim。
- 支持 remote DDR shmem。
- 支持 remote SSD block。
- 支持 topology-aware path selection。

### Phase D: 远端 HBM 与 relay

- 抽象 HBM direct capability。
- 支持 direct 和 relay 两套路径。
- 用最少跳数和 cost model 选路。

### Phase E: 独立服务化

- Data Service 作为独立进程。
- 支持 RPC API。
- 支持 lease、namespace、stats、recovery。

## 13. 当前开放问题

1. 超节点内地址打平是只打平 DDR/SSD，还是最终也把可支持的 HBM 纳入同一 global address model？
2. 远端 DDR 的一致性模型采用 OpenSHMEM 风格 barrier/put/get，还是额外提供 cache coherence/lease 语义？
3. KVCache 跨节点访问是 owner-read/write 模型，还是允许多节点并发读写？
4. 远端 HBM 不支持直通时，relay 节点是否由 Data Service 自主选择，还是由上层 scheduler 指定？
5. SSD→HBM 是否假设存在 direct path，还是首期统一经 DDR staging？
6. Data Service 的 metadata 存储首期用进程内 registry，还是直接接一个轻量 KV/DB？
7. ub-sim 验证应该先模拟语义正确性，还是从第一版就带 cost/latency 模型？
8. 开放接口首期采用 Python client、Unix socket RPC，还是直接定义跨语言 IDL？
9. 手动控制接口和自动 allocator policy 冲突时，以 pin/lease 为最高优先级，还是由 policy 决定？

## 14. 第一版范围结论

第一版 Data Service 的框架范围建议定为：

- 控制面：object、placement、topology、capability、lifetime、lease。
- 数据面：本地 DDR/HBM allocation，NPU child-memory ref，异步 transfer handle。
- 策略面：基于 locality、capacity、lifetime、hop_count 的简单 cost model。
- 仿真面：ub-sim 多节点 DDR/SSD/HBM topology 和 remote transfer 语义。
- 对接面：pypto-serving 只看 allocator scope 和 DataHandle；PyPTO/Simpler 只看可消费的 tensor descriptor。

这样既能服务当前权重/KVCache 接管，又不会把未来超节点、UB shmem、UB block、远端 HBM relay 的设计门关死。
