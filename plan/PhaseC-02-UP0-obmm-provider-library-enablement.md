# PhaseC-02-UP0：公共可安装 OBMM Provider Library 上游使能

> 开发环境统一口径（2026-08-31）：实现、构建和验证固定在 `ssh serving-b` → `openeuler-2403-DS` 内执行，容器为 `openeuler-2403-DS`、用户为 `root`、workspace 为 `/home/sj`。不得直接在 canonical `/home/sj/git/ub_sim` 或远端 dirty data-service checkout 上试改；使用隔离 candidate/build/run 目录。

Last reviewed: 2026-09-01

Status: `closed-internal / public-consumer-and-real-obmm-pass / G0-G8-pass / formal-archive-promoted`

## 0. 2026-09-01 最终闭环结果

UP0-M0～M4 与 C02 companion handoff 已完成实现、独立提交、正式归档和 exact-commit 验证：

```text
local source worktree:
  /Users/sean/Desktop/gitee/MemoryService/worktrees/phasec02-mem-service-up0
remote source copy:
  /home/sj/phasec02_up0_runs/phasec02_up0_20260831_impl01/mem_service
commits:
  mem_service@546caede8cbfe79c28c38e3659938e02f39bd953
  ub_sim@b624ee4318c082434ede1415e9e4d558ca47abe3
final integrated evidence:
  data-service/results/phasec02_uflow_ub_sim_20260901_real13/
promotion evidence:
  data-service/results/phasec02_promotion_20260901/
```

已新增 public API/ABI 1 合同、opaque/bounded/read-only descriptor、single-terminal completion、稳定错误、static library、public header、pkg-config 和 machine-readable version manifest。`obmm-provider-installed-consumer-smoke` 在 source tree 外仅使用安装 prefix，以 `-Wall -Wextra -Werror -static` 编译链接并输出：

```text
mem_service obmm-sdk-contract: status=ok api=1 abi=1 descriptor=fail-closed completion=single-terminal
```

同一安装产物随后被 UFlow Guest companion 消费，并在 exact-commit real13 完成 NodeA allocate/seed/publish、NodeB import-read-only/checksum、revoke/release；daemon RPC 未扩展，UFlow 未复制 private provider source，Host 未承载 application payload。

当前 Gate 状态：

| Gate | 状态 | 证据/说明 |
|---|---|---|
| G0～G5 | PASS | `provider_contract.json`、`up0_build.log`、real13 golden slice 与负例合同 |
| G8 | PASS | companion 只通过安装后的 header/library/pkg-config 构建 |
| G6 | PASS | mem_service `546caed`、ub_sim `b624ee4` 均 clean，并由 complete-history Git bundle 正式归档；gitlink/lock 同 SHA |
| G7 | PASS | exact commits 上 C01 U0～U4 全部通过，证据位于 `phasec02_promotion_20260901/c01/` |

因此 public surface 与内部 promotion 均已关闭；UP0 不再是 C02/C03 的 blocker，且没有转向 daemon RPC。

## 1. 任务定位

C02-UP0 只关闭一个 external enablement 缺口：把 mem_service 已有 neutral Provider/OBMM 能力交付成 Guest companion 可以正常安装、链接、版本检查和调用的公共 library。它不实现 UFlow adapter，不扩大 mem-service daemon RPC，也不把 mem-service object catalog 变成 UFlow Catalog。

当前经只读审计确认的基线：

```text
mem_service upstream: https://github.com/LL-mixed/mem_service
mem_service baseline: 366a335275d14c277dc44d3a75070f35951a8fb4
ub_sim baseline:      c68cfb8ab139312cc9d41fb26f0ebcd3b00c7072
ub_sim gitlink:       mem_service@366a335275d14c277dc44d3a75070f35951a8fb4
guest lock:           guest-linux/aarch64/mem_service.lock@366a335275d14c277dc44d3a75070f35951a8fb4
```

真实状态：public library、source-tree 外 consumer、Guest companion 和 C02-M4B～M9 真实 OBMM acceptance 已在 exact-commit real13 通过；G6/G7 已按内部正式归档口径关闭。该结果仍不是 upstream/release baseline。

## 2. 所有权与提交边界

执行 C02 的 worker 同时拥有 UP0，但必须按仓库形成三段独立、可审查提交：

1. **mem_service commit**：在 `https://github.com/LL-mixed/mem_service` 的隔离 clone、以 `366a335...` 为基线，新增公共安装/链接交付和测试；
2. **ub_sim commit**：在 ub_sim 隔离 candidate 中原子更新 mem_service gitlink 与 `guest-linux/aarch64/mem_service.lock`，并保存兼容性/自测结果；
3. **data-service commit**：C02 的 UbSimProvider/companion/bridge 集成，独立于上述 external commits。

不得把三者压成一个不可追溯 patch；不得先修改 canonical checkout 再补证据。mem_service commit 必须是 clean、可从配置的远端或正式归档地址获取的对象，ub_sim gitlink 和 lock 必须指向同一 exact SHA。

## 3. 冻结方案

唯一首选交付是公共、可安装的 in-process OBMM Provider library：

```text
public neutral headers
  + OBMM provider implementation library
  + install/export metadata
  + provider ABI/version manifest
  + link/runtime dependency contract
  + minimal consumer smoke
```

明确不做：

- 不新增或扩大 mem-service daemon 的 region/mapping/completion RPC；
- 不要求 UFlow Guest companion复制、include 或编译 private provider source；
- 不通过 console `PASS`、test app wrapper、Host memcpy 或 payload 文件冒充 provider control surface；
- 不修改 UFlow Object/Placement/Planner schema；这些属于 C02 主任务；
- 不承诺生产硬件 API、物理性能或 remote HBM。

若公共 library 经实现证据证明不可行，worker 必须将 UP0 标为 blocked 并上报项目总监；不得自行转向 daemon RPC。

## 4. Public Library 最小合同

安装后，独立 consumer 只能依赖 public headers、installed library 和声明的系统依赖，至少能完成：

```text
provider identity / API version / ABI version
probe / capabilities / health
provider session create/destroy
DDR region allocate/register/release
publish/export descriptor
remote import/map/unmap/revoke
checksum or bounded read/check operation
submit -> completion poll/wait
stable error mapping
```

Completion 合同必须与 C02 一致：所有 mutation/data/access operation 使用一次 `submit -> completion`；一次 submission 只有一个 completion ID 和一个 terminal outcome。即使调用内立即完成，也返回该 completion 的 terminal snapshot，不能再异步产生第二个 terminal completion。

Public descriptor 必须 opaque、versioned、bounded，不暴露要求 Host 解释的 Guest pointer。至少校验 protocol/ABI major、provider session、resource generation、range 和 permission。

## 5. 安装与消费合同

必须交付并验证：

- 可重复的 build/install target，支持安装到隔离 prefix，不写系统全局目录；
- public header 列表和 private header denylist；
- shared/static library 的明确选择、SONAME/ABI major 或等价版本机制；
- `pkg-config`、CMake package config 或等价稳定 link metadata；
- runtime dependencies、RPATH/loader 使用说明；
- `provider_version.json` 或等价 machine-readable manifest，记录 source SHA、API/ABI、features；
- 一个独立目录 consumer smoke，仅从 install prefix 编译和运行，构建命令不得引用 mem_service source tree。

## 6. 实施步骤

### UP0-M0：Inventory 与 API/ABI freeze

- 固定上述 baseline SHA；
- 列出现有 neutral header、OBMM provider source、private dependency 和 symbol；
- 产出 public/private 边界、导出 symbol、ABI/version 和 dependency decision；
- 证明 daemon RPC 不在方案中。

### UP0-M1：mem_service public library

- 重构或封装现有 OBMM provider，使 public consumer 只依赖安装产物；
- 增加安装/export/version manifest；
- 统一 submit/completion 和稳定错误；
- 保持既有 mem_service daemon/conformance 行为兼容。

### UP0-M2：isolated consumer tests

- clean build 后安装到 run-owned prefix；
- 在 mem_service source tree 之外构建 consumer；
- 跑 identity/probe/session/allocate/publish/import/checksum/revoke/release；
- 跑 version mismatch、bad descriptor、range、stale session 和 double-terminal 负例；
- 扫描 compile/link command，确认无 private source/include path 泄漏。

### UP0-M3：ub_sim 原子接入

- 在 ub_sim 隔离 candidate 中更新 mem_service gitlink 与 guest lock 到同一 SHA；
- 使用新的 public install artifact 构建现有 conformance 和 C02 companion canary；
- 按 C01 重新执行 U0～U4；任何失败先回到 candidate 修复；
- 形成独立 ub_sim commit，不覆盖 canonical `/home/sj/git/ub_sim`。

### UP0-M4：交接与 promotion decision

- 记录 mem_service/ub_sim commit、remote fetchability 和 tree clean；
- 输出 C02 可直接消费的 include/link/version/runtime contract；
- 由项目总监审查后再决定是否提升 canonical baseline；
- 未经审查不得把 candidate 路径写成 Phase C 默认基线。

## 7. Gate

| Gate | 必须结果 |
|---|---|
| UP0-G0 baseline | mem_service/ub_sim exact SHA、remote 和 candidate 路径记录完整，canonical untouched |
| UP0-G1 public boundary | public headers/symbols/dependencies/ABI/version 冻结；daemon RPC 不扩展 |
| UP0-G2 install | clean build 可安装到隔离 prefix；machine-readable manifest 可读取 |
| UP0-G3 external consumer | source tree 外 consumer 只用 install artifact 编译链接；无 private include/source path |
| UP0-G4 provider semantics | identity/session/allocate/publish/import/checksum/revoke/release 与统一 completion 正例通过 |
| UP0-G5 fail closed | ABI mismatch、bad descriptor/range、stale session、重复 terminal 全部稳定失败 |
| UP0-G6 upstream commits | mem_service commit clean/fetchable；ub_sim gitlink/lock 同 SHA、commit clean/fetchable |
| UP0-G7 C01 regression | 更新后的 candidate 重新通过 C01 U0～U4，证据绑定 exact commits |
| UP0-G8 handoff | C02 companion 可仅凭 install contract 构建；无 canonical source 修改或 payload 旁路 |

## 8. 证据与 Definition of Done

证据建议保存到：

```text
remote: /home/sj/phasec02_up0_runs/<run_id>/evidence/
local:  data-service/results/<run_id>/
```

至少包含：

```text
baseline.json
source_before_after.txt
public_api_inventory.md
public_private_boundary.json
exported_symbols.txt
provider_version.json
install_manifest.txt
consumer_build.log
consumer_link_map.txt
provider_positive_tests.log
provider_negative_tests.log
mem_service_commit.json
ub_sim_gitlink_lock.json
c01_rerun_summary.json
SHA256SUMS
summary.md
```

UP0 只有在以下条件同时成立时才可关闭：

1. mem_service 公共 OBMM Provider library 可安装、可版本检查、可被 source tree 外 consumer 使用；
2. public consumer 不依赖 private source/header，不经 daemon RPC，不解析 console；
3. Provider mutation/data/access operation 具有唯一 submit/completion/terminal 语义；
4. mem_service 形成 clean/fetchable commit；
5. ub_sim gitlink 与 guest lock 原子指向该 exact commit，并形成 clean/fetchable commit；
6. 更新后的 ub_sim candidate 重新通过 C01 U0～U4；
7. C02 Guest companion 的 build/link canary 只使用安装产物；
8. canonical source 未经 promotion decision 不被替换。

UP0 已完成并解除 C02-M4B～M9 的 external blocker；C02 也已独立关闭，但 Phase C 仍需 C03～C08。
