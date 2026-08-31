# PhaseC-02：UFlow × ub-sim 功能对接与仿真运行骨架

> 开发环境统一口径（2026-08-31）：实现、构建和验证固定在 `ssh serving-b` → `openeuler-2403-DS` 内执行；容器用户为 `root`，workspace 根目录为 `/home/sj`。当前内部可行 ub-sim 基线固定为 `/home/sj/git/ub_sim@c68cfb8ab139312cc9d41fb26f0ebcd3b00c7072`，UFlow 仓库入口为 `/home/sj/git/data-service`。实际开发必须使用 `/home/sj/phasec02_runs/<run_id>/` 隔离候选，不修改 `/home/sj/git/ub_sim.pre_phasec01_20260831` 历史备份，也不覆盖 dirty 的远端 data-service checkout。详见 [Phase0-development-environment.md](Phase0-development-environment.md) 与 [PhaseC-01-ub-sim-upstream-sync-and-selftest.md](PhaseC-01-ub-sim-upstream-sync-and-selftest.md)。

Last reviewed: 2026-08-31

Status: `ready-for-implementation / code-walk-complete / adapter-absent`

## 1. 任务定位

PhaseC-02 负责建立 UFlow 与 ub-sim 之间第一条可持续演进的功能对接骨架，使 UFlow 能在没有真实 UB/NPU/SSD 硬件时，仍以自己的 `DataObject / Placement / Handle / TransferPlan / TransferEvent` 合同驱动 ub-sim 做语义验证。

本任务不是把 `mem_service` 改名为 UFlow，也不是让 UFlow core 直接调用 QEMU/OBMM ioctl。两者定位不同：

- `mem_service` 是 ub-sim 为内存/对象仿真单独演进的服务，包含自己的 daemon、对象记录、模型适配和 provider；
- UFlow 是面向 LLM serving 的数据管理控制面，负责 serving 侧对象身份、placement、lease、路由、事件、Trace 和 Monitor；
- ub-sim 是 UFlow 的一种验证后端，不是 UFlow 的生产依赖，也不能反向决定 UFlow 的业务对象模型；
- mem-service 中已经验证的中立 provider/OBMM 能力可以作为 guest 数据面执行库复用，但 mem-service 的对象目录和业务 wire API 不成为 UFlow 的第二个 source of truth。

一句话目标：

> UFlow 保持唯一对象身份和 serving API，在 host 侧通过可替换 `UbSimProvider` 驱动每个 QEMU guest 内的轻量执行 agent；仿真对象使用有版本、范围、代际和校验信息的 opaque descriptor，不伪造真实硬件指针。

## 2. 本任务边界

### 2.1 C02 必须完成

1. 冻结 provider-neutral 的 topology、capability、placement、resource binding、handle、completion 和 error schema；
2. 把 UFlow 当前直接绑定 ACL/mmap/file 的实现隔离到 `LocalProvider`，保证既有本机路径不回归；
3. 建立 `UbSimProvider` host adapter、版本化 bridge 协议和 guest `uflow_sim_agent` 骨架；
4. 建立 2-node QEMU + 双 agent + 单 UFlow daemon 的可重复启动、ready、shutdown 和证据流程；
5. 证明模拟对象不会向 host client 暴露伪造的 HBM/GSVA/device pointer；
6. 通过 mock/local/ub-sim 共用的 provider conformance suite；
7. 通过一个 4 KiB deterministic seed/checksum 的内部 provider vertical slice，证明 UFlow 确实驱动了 guest 内 OBMM mapping，而不是只解析静态配置或匹配串口日志；
8. provider 未启用时，现有 HBM/DDR/SSD 创建、传输、Trace 和 Monitor 行为保持不变。

### 2.2 C02 明确不声称完成

- 不把 vertical slice 写成 `remote DDR supported`；完整 publish/import/revoke 生命周期属于 C04；
- 不实现 UB-attached SSD/SSU 正式数据路径；属于 C05；
- 不实现 topology-aware cost/routing；属于 C03/C06；
- 不完成 failure replan、全量 cancel 或 4/8-node acceptance；属于 C07/C08；
- 不宣称模拟 NPU 等价真实 NPU，也不输出物理 HBM/SSD 性能结论；
- 不让 host 控制通道承载正式 application payload。

C02 完成后的准确状态应为：

```text
UFlow ub-sim integration spine: available
UFlow provider contract: frozen v1
2-node control/descriptor/checksum conformance: pass
remote DDR/SSD/topology-aware routing: not yet supported
physical hardware fidelity: semantic simulation only
```

## 3. 代码走读结论

本次走读绑定：

```text
UFlow local:       data-service/main@ea3d37a
ub-sim canonical: /home/sj/git/ub_sim@c68cfb8ab139312cc9d41fb26f0ebcd3b00c7072
mem_service:       366a335275d14c277dc44d3a75070f35951a8fb4
QEMU UB:           3a247cbe1532438f27805efe2e124c4ab882528f
```

### 3.1 UFlow 当前实现

| 代码位置 | 当前事实 | 对接缺口 |
|---|---|---|
| `crates/uf-core/src/idl.rs` | 已有 `Medium`、`AddressKind`、`DataObject`、`DataPlacement`、`DataHandle`、`TransferPlan/Event` | 没有 provider identity、schema version、topology generation、object version、simulation truth label；`runtime_descriptor` 仍是无类型字符串 |
| `crates/uf-daemon/src/catalog.rs` | `Object` 同时保存对象身份、唯一 `placement_id` 和 HBM/DDR/SSD 物理字段 | 一个对象只能绑定一个实现；对象身份与 ACL allocation、mmap path、SSD file 耦合，无法表示多节点/多 placement |
| `crates/uf-daemon/src/main.rs` | daemon 启动时直接创建 `NpuHbmAclBackend` 并做 ACL HBM probe | 无 provider registry；simulation 模式即使没有物理 ACL 也会走硬件探测 |
| `crates/uf-daemon/src/object_service.rs` | HBM 直接 ACL allocate/export，DDR 直接 memfd/mmap，SSD 直接本地文件 | 没有将 allocation/release 委派给 provider；response 默认返回 client 可映射 pointer/path |
| `crates/uf-daemon/src/transfer_planner.rs` | 按 `(src.placement, dst.placement)` 静态 match，成本和路径硬编码 | 不读取 topology/capability/provider；无法区分 physical 和 simulated realization |
| `crates/uf-daemon/src/transfer_executor.rs` | 按 `plan.path` match 后直接操作 `HbmAllocation`、mmap pointer 和 file | 没有 provider completion 映射；remote/sim handle 无执行入口 |
| `sdk/python/uflow/client.py` | `allocate()` 要求 ACL bridge，并立即 import/map shareable HBM handle | 仿真对象无法也不应返回真实 `device_ptr`；需要 descriptor-first API |
| `uf-core/src/lib.rs` 与 Python `protocol.py` | client-daemon wire 是未版本化的 `key=value;...` 单行协议 | 现有兼容面可保留，但 provider bridge 必须独立版本化、校验并 fail closed |

核心判断：当前 UFlow 的上层名词已经接近目标，但 daemon 内部仍是“一个对象等于一个本机物理资源”。C02 的首要重构不是增加一个 `if sim`，而是把稳定对象身份、逻辑 placement 和 provider resource binding 分开。

### 3.2 ub-sim 当前实现

| 代码/脚本 | 当前事实 | 对接含义 |
|---|---|---|
| `run_ub_dual_node_apps.sh` | QEMU 由 kernel cmdline 选择固定 app，主要依赖 serial log 判定 PASS；QMP 只用于生命周期 | 现有 runner 适合一次性 smoke，不足以承载持久 UFlow provider；需要专用双向 bridge |
| QEMU 启动参数 | `-nodefaults -nographic`，默认 serial 写文件；没有 host 可连接的业务网络/agent 端口 | C02 不能假设 host 能 TCP 进入 guest；必须显式添加 transport |
| `mem_service_provider.h` | 已有 v2 provider API：probe、region register、transfer completion、remote mapping、publish/invalidate/wait visible | guest agent 可复用的成熟数据面合同，不应在 UFlow 重写 OBMM ioctl |
| `mem_service_provider_obmm.*` | OBMM provider 拥有 export/import、mmap、cache maintenance、range visibility | 可作为 `uflow_sim_agent` 内部 backend；opaque descriptor 可经 UFlow 控制面交换 |
| mem-service daemon wire v1 | 24 个 operation 主要面向对象/KV/artifact；未暴露通用 provider register/map/poll API | 不能直接把现有 mem-service daemon 当作 UFlow provider RPC endpoint |
| OBMM conformance | 已在两个 guest 中验证 register/export/map/publish/invalidate/wait/unmap/deregister 与 checksum 负例 | C02 vertical slice 应复用同一 provider 调用链，并由 UFlow 发起和归档 |
| `sim_ub_attached_npu_design.md` | NPU 是语义设备，明确不模拟真实 NPU microarchitecture、DMA/HBM | UFlow 不能把 guest RAM/OBMM backing 说成真实 HBM，也不能报告物理 HBM 带宽 |
| `sim-topology` YAML 与 QEMU topology INI | Rust scenario 表达 host/domain/service；QEMU INI 表达 node-scoped UB link，当前不是同一运行时 source | C02 manifest 必须同时固定两者和 hash；C03 再建立统一 snapshot/generation |

### 3.3 mem-service 与 UFlow 的复用边界

可以直接复用：

- `mem_service_provider.h` 的 region、mapping、completion 基本语义；
- OBMM provider 的 `/dev/obmm` 生命周期、descriptor 编解码、mapping 和 range visibility；
- provider conformance 的 checksum、越界、坏 descriptor、cleanup 断言；
- ub-sim 已有 guest kernel/QEMU/OBMM/GSVA/NPU/SSD 仿真设备。

不能直接复用为 UFlow source of truth：

- mem-service daemon 的对象 ID、record table、prefix/KV/object wire operation；
- mem-service 的模型固定 object kind/offset；
- Qwen3/DeepSeek adapter 和 runtime handoff policy；
- 串口 PASS 文本或静态 scenario 文件推断出的 provider readiness。

## 4. 架构方案选择

### 4.1 选择：host UFlow daemon + per-guest sim agent

```text
LLM serving / PyPTO / test client（host）
  -> UFlow Python/C SDK
  -> UFlow daemon（唯一 catalog / lease / plan / event / trace）
       -> LocalProvider（现有 ACL/mmap/file）
       -> UbSimProvider（host adapter）
            -> uflow-sim-bridge-v1 control sockets
                 -> nodeA guest: uflow_sim_agent
                      -> mem_service neutral provider API
                      -> OBMM / GSVA / semantic NPU / semantic SSD
                 -> nodeB guest: uflow_sim_agent
                      -> mem_service neutral provider API
                      -> OBMM / GSVA / semantic NPU / semantic SSD
  <- TransferEvent / Trace v2 / Monitor v2（明确 simulated）
```

UFlow daemon 保留全局业务语义，guest agent 只负责：

- 拥有 guest 进程内 region/mapping；
- 调用 mem-service neutral provider；
- 返回 opaque descriptor、capability、completion 和 checksum；
- 不创建 UFlow object_id，不决定 placement policy，不解析模型角色。

### 4.2 不采用的方案

#### 每个 guest 内运行完整 UFlow daemon

不采用。它会产生多个 catalog/object_id/lease source，host LLM serving 仍无法直接消费 guest pointer，还会把 Rust/ACL/Python 依赖塞入最小 initramfs。

#### UFlow 直接调用 mem-service daemon 对象 RPC

不采用。现有 wire 没有暴露通用 provider 操作，并且会形成 UFlow catalog 与 mem-service record table 两套对象真相。UFlow 只能复用其 provider library，不复用其业务对象服务作为中间层。

#### 只在 host 使用 `sim-services` Rust stub

只用于 unit/mock conformance，不作为 integrated acceptance。它不经过 QEMU guest、OBMM/SIM_DEC/GSVA，不能证明 UFlow 已在 ub-sim 上运行。

#### 复用 console serial 或解析 guest PASS 日志作为 RPC

不采用。console 输出没有消息边界、并发请求、版本、checksum 和错误恢复；串口日志只能作为辅助证据。

## 5. 没有真实硬件时的 DataObject 处理

### 5.1 对象身份与资源实现必须分层

```text
DataObject
  object_id / namespace / name / role / shape / dtype
  size / consistency / object_version / checksum / state

DataPlacement
  placement_id / object_id
  medium=hbm|ddr|ssd|ub_shmem|dfs
  node_id / device_id / domain
  execution_mode=physical|simulated
  fidelity=physical|functional|semantic
  provider_id / provider_instance
  topology_generation / state

ResourceBinding（daemon 内部，不是业务对象身份）
  binding_id / placement_id
  provider_resource_id / boot_id / generation
  addressability=local_mapped|provider_only|remote_mapping|block
  provider_backing / descriptor_version / opaque_descriptor
  range / access / expiry / state

DataHandle（client 可见的 lease 投影）
  object_id / placement_id / lease_id
  object_version / generation / allowed_range / access_mode
  address_domain / address_kind
  provider_id / descriptor_version / opaque_descriptor
```

一个 `DataObject` 可以有多个 `DataPlacement`；一个 placement 在某个 provider 上对应一个 resource binding。对象释放、agent 重启或 topology generation 改变后，旧 handle 必须 stale，不能重新指向新 region。

### 5.2 simulated HBM 的准确语义

ub-sim 的 semantic NPU 明确不模拟真实 HBM。C02 采用以下表达：

```text
medium=hbm                         # UFlow/serving 的逻辑 tier 意图
execution_mode=simulated
fidelity=semantic
provider_id=ub_sim
provider_backing=guest_ram_obmm_gsva
address_kind=provider_handle
host_dereference=false
physical_capacity_evidence=false
```

这表示“按 HBM placement 语义验证对象生命周期、路由和 NPU 可访问性”，不表示“已分配物理 HBM”。Monitor/Trace 必须显示 `HBM (simulated/semantic, guest RAM + OBMM/GSVA backing)`，禁止只显示 `HBM` 或 `physical direct`。

### 5.3 SDK 行为

- 保留现有 `HbmObject/DdrObject/SsdObject` 作为 physical/local convenience view；
- 新增 descriptor-first `ManagedDataObject`（或等价命名），持有 `DataObject + DataPlacement + DataHandle`；
- `create_data_object(medium=..., target=...)` 在 physical/simulated 模式使用相同业务参数；
- simulated placement 不初始化 ACL bridge，不执行 host mmap，不返回 `device_ptr=0` 冒充有效地址；
- 对 simulated object 调用 `.device_ptr`、`.data_ptr`、`as_memoryview()` 必须抛出稳定 `HandleNotLocallyAddressable`；
- 初始化/校验仿真 payload 使用 agent 内部 deterministic seed/checksum fixture，不让 JSON、串口、临时文件或 host memcpy 旁路正式 guest 数据面；
- 后续需要真实 serving tensor pointer 时，只能走 physical provider，或把对应 compute/runtime 移入拥有该 simulated mapping 的 guest 进程；C02 不制造跨 address-domain 裸指针。

## 6. Provider-neutral v1 合同

### 6.1 ProviderIdentity

```text
provider_api_major / provider_api_minor
provider_id / provider_instance
execution_mode
implementation / revision
session_id / boot_id
health / ready / degraded_reason
```

`boot_id` 每次 agent 启动变化，是 stale resource/handle 的硬边界；provider revision 必须包含 UFlow SHA、ub-sim SHA、mem_service SHA 和 QEMU SHA。

### 6.2 TopologySnapshot 与 CapabilitySnapshot

C02 冻结类型，C03 填充完整动态内容：

```text
TopologySnapshot:
  schema_version / snapshot_id / generation / observed_at
  nodes[] / devices[] / links[] / domains[]
  source_manifest_hash / qemu_topology_hash

CapabilitySnapshot:
  schema_version / provider_id / generation
  operations[] / address_kinds[] / completion_kinds[]
  remote_mapping / peer_transfer / block_io / checksum
  publish_invalidate / cancel / max_transfer_bytes / alignment / queue_depth
  fidelity / limitations[]
```

能力必须来自 agent handshake/probe 和实际 canary，不能从“文件/脚本存在”推断。OBMM 在 peer canary 前只能 `degraded`，不能 `ready`。

### 6.3 DataPlaneProvider 操作面

```text
identity()
probe()
discover_topology()
discover_capabilities()
allocate_resource(request)
release_resource(binding)
export_region(binding, range, access)
import_region(descriptor, range, access)
revoke_region(binding_or_export)
submit_transfer(request) -> provider_completion_id
submit_block_io(request) -> provider_completion_id
poll_completion(id)
wait_completion(id, timeout)
cancel_completion(id)
read_counters()
shutdown()
```

C02 必须实现 identity/probe、allocate/release、descriptor encode/decode 和内部 OBMM mapping vertical slice。未实现的操作必须以 capability=false 且 `ProviderOperationUnsupported` fail closed，不能落入 LocalProvider 或 CPU copy。

### 6.4 Completion 映射

```text
UFlow event_id (global)
  -> provider_id + provider_completion_id + provider_boot_id
  -> submitted | queued | running | complete | failed | canceled
  -> exactly one terminal outcome
```

规则：

- provider completion 只能更新对应 UFlow event；
- agent 重启导致 `boot_id` 不一致时事件终止为 `ProviderRestarted`；
- capability 未声明 cancel 时，`CancelEvent` 不能伪报 canceled；
- timeout 是明确 terminal failure 或仍在运行的 poll 状态，不能同时出现两个结论；
- planned/actual path、provider、execution_mode、fidelity 和 topology generation 全部进入 Trace v2 / Monitor v2。

### 6.5 稳定错误码

至少冻结：

```text
ProviderUnavailable
ProviderNotReady
ProviderVersionMismatch
ProviderOperationUnsupported
ProviderRestarted
TopologyStale
CapabilityStale
ResourceNotFound
ResourceStale
HandleNotLocallyAddressable
DescriptorMalformed
DescriptorVersionMismatch
AccessDenied
RangeOutOfBounds
ChecksumMismatch
QueueFull
Timeout
CancelUnsupported
InternalProviderError
```

错误 detail 可以扩展，但 code 不得依赖 QEMU/OBMM 私有日志字符串。

## 7. uflow-sim-bridge-v1

### 7.1 传输选择

首选 dedicated virtio-serial：每个 QEMU node 增加一个独立 chardev Unix socket 和一个命名 virtserialport，例如：

```text
host socket: /tmp/uflow-ubsim/<run_id>/nodeA.agent.sock
guest port:  /dev/virtio-ports/org.uflow.sim.nodeA
```

QMP 仍只负责 QEMU pause/resume/quit；console serial 仍只负责日志。若当前 guest kernel 缺少 virtio-console/virtio-serial，C02 必须补齐 kernel config/initramfs 驱动并保存 probe 证据，不能退回 console screen scraping 作为 acceptance path。

### 7.2 Frame

bridge 使用独立版本化 frame，不复用 UFlow client-daemon 单行 KV：

```text
magic="UFSB"
major=1 / minor
header_len / opcode / flags
request_id
payload_len
payload_checksum
payload=versioned TLV
```

要求：

- 明确 little-endian；
- header/payload 长度、checksum、required TLV、duplicate TLV 全部校验；
- major 不兼容、坏 checksum、超长 frame、未知 required field 立即拒绝；
- opaque provider descriptor 作为 byte string 透传，host adapter 不解析 OBMM 私有布局；
- 控制面允许传 identity、descriptor、completion 和 checksum，不允许传正式 payload bytes；
- test-only `FillPattern(seed)` 只在 conformance feature 下启用，并在能力中标记 `validation_only=true`。

### 7.3 Opcode 面

```text
HELLO / HEALTH / GET_CAPABILITIES / GET_TOPOLOGY
ALLOCATE_RESOURCE / RELEASE_RESOURCE
EXPORT_REGION / IMPORT_REGION / REVOKE_REGION
SUBMIT_TRANSFER / SUBMIT_BLOCK_IO
POLL / WAIT / CANCEL
FILL_PATTERN_TEST_ONLY / CHECKSUM_RANGE
SHUTDOWN
```

C02 不要求全部 opcode 实现数据功能，但 schema、capability gating、unsupported 负例和版本兼容测试必须齐全。

## 8. 启动与关闭方案

### 8.1 统一入口

实施后提供单一 orchestrator：

```text
data-service/tools/phasec/run_uflow_ub_sim.sh
```

目标 CLI：

```bash
tools/phasec/run_uflow_ub_sim.sh \
  --ub-sim-root /home/sj/phasec02_runs/<run_id>/ub_sim \
  --nodes 2 \
  --scenario scenarios/mvp_2host_single_domain.yaml \
  --topology vendor/ub_topology_two_node_v0.ini \
  --uflow-socket /tmp/uflow-ubsim/<run_id>/uflow.sock \
  --run-id <run_id> \
  --smoke contract
```

### 8.2 启动顺序

1. 用 C01 guard 校验 `ub_sim@c68cfb8` 和全部 gitlink/lock；
2. 记录 UFlow/ub-sim/mem_service/QEMU SHA、scenario/topology hash 和容器环境；
3. 构建 host `uf-daemon`、cross-build guest `uflow_sim_agent`；
4. 将 agent 和 bridge contract manifest 打包进 initramfs；
5. 启动 2 个 QEMU node，建立独立 QMP、console、agent socket；
6. guest init 通过 `linqu_uflow_sim_agent=1` 启动持久 agent，不运行一次性 PASS 后退出；
7. orchestrator 等待两个 agent `HELLO`，校验 node_id、boot_id、provider API、revision；
8. agent 内 OBMM provider 完成 device probe 和 peer canary，ready 前保持 degraded；
9. 生成只读 `provider_manifest.json`，再启动 UFlow daemon：

```text
uf-daemon
  --socket <uflow.sock>
  --provider-config <provider_manifest.json>
  --startup-probe-bytes 0
```

10. UFlow daemon 注册 `LocalProvider` 和 `UbSimProvider`；simulation 模式禁止 ACL startup probe；
11. conformance/client 只连接 UFlow socket，不直接连接 guest agent；
12. Trace/Monitor 显示两个 provider session 和 simulated truth label。

### 8.3 关闭顺序

```text
stop accepting new UFlow requests
  -> drain/cancel provider events according to capability
  -> release/revoke UFlow bindings
  -> request agent shutdown
  -> QMP quit remaining guests
  -> remove run-owned sockets/pid files
  -> verify no qemu/agent/uf-daemon process and no run socket remains
```

orchestrator 必须只清理 manifest 中属于本次 `run_id` 的资源，禁止 broad `pkill`、通配删除其他 run 或清理 canonical build cache。

## 9. 实施任务拆分

### C02-M0：基线与走读断言

交付：

- 固定 UFlow SHA 与 `ub_sim@c68cfb8`、mem_service `366a335`、QEMU `3a247cb`；
- 把本节代码走读结论转成 contract tests/注释，避免实现绕回具体 backend；
- 验证 virtio-serial guest 驱动能力并保存 probe；
- 建立 `phasec02_runs/<run_id>` manifest 和 cleanup trap。

### C02-M1：IDL、对象/placement/binding 分层

主要修改：

```text
crates/uf-core/src/idl.rs
crates/uf-daemon/src/catalog.rs
sdk/python/uflow/idl.py
sdk/python/uflow/objects.py
```

要求：

- 增加 schema/provider/execution_mode/fidelity/generation/version/checksum 字段；
- 引入独立 `PlacementRecord` 与 `ResourceBindingRecord`；
- 一个对象允许多个 placement；
- 保留 local v1 response compatibility；
- 禁止 provider descriptor 进入 object identity 或 placement policy。

### C02-M2：Provider trait、registry 与 LocalProvider

建议落点：

```text
crates/uf-daemon/src/providers/mod.rs
crates/uf-daemon/src/providers/registry.rs
crates/uf-daemon/src/providers/local.rs
crates/uf-daemon/src/providers/mock.rs
```

要求：

- object service/planner/executor 只依赖中立 provider trait；
- 现有 ACL/mmap/file 行为迁入 LocalProvider，不改变现有功能与性能默认值；
- provider disabled/unavailable 不影响不依赖它的 local paths；
- mock/local 共用 conformance suite。

### C02-M3：Bridge schema 与 host UbSimProvider

建议落点：

```text
crates/uf-core/src/provider_contract.rs
crates/uf-core/src/sim_bridge.rs
crates/uf-daemon/src/providers/ub_sim.rs
tests/phasec02_provider_contract.rs
```

要求：

- 实现 v1 frame/TLV、版本/长度/checksum/required-field 负例；
- per-node connection、request correlation、timeout、reconnect/boot_id 检查；
- host adapter 不 include/parse QEMU、OBMM、mem-service provider 私有头文件；
- 连接断开后 bindings stale，禁止透明改走 LocalProvider。

### C02-M4：guest uflow_sim_agent

ub-sim 候选中的建议落点：

```text
guest-linux/aarch64/apps/uflow_sim_agent/
guest-linux/aarch64/scripts/build_initramfs.sh
guest-linux/aarch64/initramfs/run_app
```

要求：

- agent 是轻量 C 进程，不包含 UFlow catalog/LLM placement policy；
- 链接 mem_service neutral provider 与 OBMM provider source；
- 实现 identity/probe、allocate/release、descriptor、checksum 和 test seed；
- region/mapping 属于 agent 进程，ownership/bounds/checksum fail closed；
- agent 不调用 mem-service 业务对象 daemon API，不使用模型固定 offsets；
- 每个资源绑定当前 boot_id，退出时完整 unmap/deregister。

若必须修改 mem_service，改动只能进入 neutral provider 层并单独提交、测试、更新 C01 lock；禁止在 ub-sim 复制 provider 源码形成 fork。

### C02-M5：QEMU runner 与 orchestrator

ub-sim：

```text
guest-linux/aarch64/scripts/run_ub_dual_node_apps.sh
guest-linux/aarch64/scripts/qemu_ub_common.sh
```

UFlow：

```text
tools/phasec/run_uflow_ub_sim.sh
examples/phasec02/uflow_c02_ub_sim_smoke.py
```

要求：

- 增加 dedicated virtio-serial agent port 和 per-node socket manifest；
- agent persistent ready 取代静态 cmdline app PASS 作为对接门禁；
- QMP/console/agent socket 分离；
- daemon 只在所有 required agent/canary ready 后启动；
- 任一步失败都清理本 run 资源并保存 raw log。

### C02-M6：descriptor-first SDK 与 truth label

要求：

- 新增 generic `create/open/describe_data_object` view；
- physical convenience API 保持兼容；
- simulated object 本地解引用稳定失败；
- capabilities 返回 provider、execution_mode、fidelity、limitations；
- Trace/Monitor 至少增加 provider_id、provider_revision、execution_mode、simulation_fidelity、provider_boot_id、topology_generation。

### C02-M7：conformance 与 vertical slice

统一 conformance 对 mock/local/ub-sim 执行：

1. identity/version/required capability；
2. allocate/release 与重复释放；
3. descriptor roundtrip、坏版本、坏 checksum、越界；
4. agent 内生成 4 KiB deterministic payload；
5. 通过 OBMM provider export/map/publish/wait/checksum；
6. descriptor 与 checksum 经 UFlow provider bridge，payload bytes 不经 host 控制面；
7. UFlow event/trace 关联 provider completion；
8. revoke/release 后旧 handle stale；
9. agent 重启 boot_id 改变，旧 binding fail closed；
10. cleanup 无进程/socket/mapping 泄漏。

vertical slice 只能标记 `provider_conformance`，不能加入正式 supported transfer path 列表。

### C02-M8：证据、文档与下游门禁

交付：

- source/contract/unit/2-node integrated/negative/cleanup 原始证据；
- 更新 C00、C02 和项目总控；
- 输出 C03/C04 可直接消费的 schema/manifest；
- 明确未完成 remote DDR/SSD/routing，不提前升级 Phase C 状态。

## 10. 验收矩阵

| Gate | 场景 | 必须结果 |
|---|---|---|
| C02-G0 | baseline/source | UFlow、ub-sim、8 个 gitlink、scenario/topology hash 可复现 |
| C02-G1 | IDL/schema | mock/local/ub-sim 使用同一 provider contract；major/minor 与兼容负例通过 |
| C02-G2 | local regression | provider 未启用时现有 HBM/DDR/SSD 与 Phase E contract tests 全过 |
| C02-G3 | bridge framing | 坏 magic/version/length/checksum/unknown required TLV fail closed |
| C02-G4 | 2-node startup | 双 QEMU、双 agent ready；node/boot/revision/capability 一致 |
| C02-G5 | simulated object | UFlow 创建 descriptor-only simulated placement；无伪 device_ptr/path |
| C02-G6 | OBMM vertical slice | 4 KiB seed/checksum 经 guest OBMM mapping 验证，payload 不经 control channel |
| C02-G7 | event/observability | provider completion 映射一个 UFlow terminal event；Trace/Monitor 明示 simulated |
| C02-G8 | stale/restart | revoke、release、agent restart、boot_id mismatch 全部拒绝旧 handle |
| C02-G9 | disabled/degraded | UbSimProvider down 不影响 local provider；required sim request 不静默 fallback |
| C02-G10 | cleanup | 无 uf-daemon/agent/QEMU/run socket 残留，证据 checksum 通过 |

## 11. 必测负例

- UFlow 请求 simulated HBM 后 SDK 返回/接受 host `device_ptr`；
- `medium=hbm` 但缺失 `execution_mode/fidelity/provider_backing`；
- agent 未完成 peer canary 就报告 ready；
- descriptor 来自旧 boot_id/generation；
- object range、lease range、provider region range 任一不一致；
- corrupt descriptor、payload checksum mismatch、重复 required TLV；
- provider capability=false 但 planner 仍选择该 path；
- provider 断开后自动改走 CPU memcpy/local file；
- control bridge 捕获到正式 payload bytes；
- UFlow event complete 后又被 failed/canceled 覆盖；
- QMP/console/agent socket 混用；
- 只根据 scenario/脚本存在就声称 topology/capability ready；
- 只看 guest log `PASS`，没有 UFlow object/handle/event 证据；
- 把 simulated latency/bandwidth 写成真实硬件性能。

## 12. 证据目录

```text
remote: /home/sj/phasec02_runs/<run_id>/evidence/
local:  data-service/results/<run_id>/
```

至少包含：

```text
baseline.json
environment.txt
source_manifest.json
scenario.yaml
scenario.sha256
qemu_topology.ini
qemu_topology.sha256
provider_contract.json
provider_manifest.json
agent_nodeA.log
agent_nodeB.log
qemu_nodeA.log
qemu_nodeB.log
uflow_daemon.log
provider_conformance.log
bridge_negative.log
events.json
trace_events.json
monitor_snapshot.json
cleanup.txt
SHA256SUMS
summary.md
```

每个 PASS 必须绑定 exit code、开始/结束时间、UFlow event_id、provider completion_id、boot_id、object/placement/binding/handle generation 与 checksum verdict。

## 13. Definition of Done

C02 关闭必须同时满足：

1. provider-neutral v1 类型和 bridge schema 已进入代码并有兼容测试；
2. UFlow 对象身份与 provider resource binding 已分离，一个对象可表达多个 placement；
3. LocalProvider 承接现有物理路径且回归通过；
4. UbSimProvider 与双 guest agent 能持久 handshake、probe、allocate/release；
5. simulated HBM 不暴露伪造本地 pointer，并明确 semantic fidelity；
6. UFlow 发起的 2-node OBMM vertical slice 通过 descriptor/checksum/cleanup；
7. stale descriptor、agent restart、provider down、坏 frame/越界/checksum 负例 fail closed；
8. Trace/Monitor 能区分 physical 与 simulated，并关联 provider revision/generation；
9. unified orchestrator 可一键启动和关闭，且只清理本 run；
10. 证据 bundle 可复核，状态只升级为 `integration-spine-ready`，不提前声称 remote DDR/SSD/routing supported。

## 14. C02 之后的接口

C02 向后续任务提供：

- C03：`TopologySnapshot/CapabilitySnapshot` schema、agent handshake、provider registry；
- C04：DataObject/Placement/Binding/Handle、OBMM descriptor 和 publish/import/revoke 操作面；
- C05：同一 provider/event 合同下的 block descriptor 与 completion；
- C06：带 provider、topology generation、capability 和真实 feedback 的 planner 输入；
- C07：provider error/cancel/restart/fallback 状态机；
- C08：统一 orchestrator、2/4/8-node manifest 与证据格式。

当前唯一 Next Gate：按 M0→M8 顺序实现并归档 2-node integration spine，先守住对象身份、仿真真实性和 provider 边界；在 C02-G0～G10 全部通过前，不启动 C03/C04 的 supported 状态升级。
