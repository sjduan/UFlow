# PhaseC-01: ub-sim 上游同步与 Latest-Ready 内部自测

> 开发环境统一口径（2026-08-31）：固定在 `ssh serving-b` → `openeuler-2403-DS` 内准备和验证候选基线；容器执行用户为 `root`，workspace 根目录为 `/home/sj`，UFlow 仓库为 `/home/sj/git/data-service`，ub-sim canonical checkout 为 `/home/sj/git/ub_sim`。candidate staging 和运行目录必须位于 `/home/sj` 下并由当次 manifest 记录。详见 [Phase0-development-environment.md](Phase0-development-environment.md)。

Last reviewed: 2026-08-31

Status: `done-first-run / latest-repaired-ready / manual-active`; 原始 upstream 仍有不可获取 gitlink，但显式下游修复候选已在 serving-b 通过 U0-U4。未接入每日 watcher，不能写成 plain `latest-ready` 或 release-ready。

## 1. 任务定位

PhaseC-01 是所有 Phase C 开发和验收的第一道常驻门禁。它不是“一次 git pull”，而是持续维护以下三个可独立回答的事实：

1. **Latest**：当前权威上游分支的最新根提交是什么；
2. **Reproducible**：该提交锁定的全部子模块、lock、artifact 输入是否可以精确获取；
3. **Ready**：该精确源码组合是否通过内部 source check、unit/contract test 和最小多节点 smoke。

只有三者同时成立，候选提交才能标记为 `latest-ready` 并交给 C02～C08 使用。

一句话目标：

> 任何 Phase C agent 开工前，都能拿到一个来源清晰、依赖完整、内部跑通、证据新鲜的 ub-sim 基线；如果最新上游坏了，则 fail closed 并保留最后一个已知可用基线，不在工作区里偷偷拼版本。

## 2. “实时保证”的准确含义

文档本身不能提供持续运行的监控进程。C01 实施后，“实时”定义为事件驱动加活跃期定时检查：

- 每个 Phase C 任务开工前强制检查；
- `ub_sim origin/master` 或关键子模块更新后立即检查；
- Phase C 活跃期间至少每日检查一次；
- 准备正式 acceptance/release bundle 前强制全量检查；
- 超过 freshness window 的旧结果自动降为 `last-known-good`，不能继续称为 latest-ready。

默认 freshness window：

```text
source/reachability check: 24 hours
unit/contract check:       24 hours
2-node integrated smoke:   72 hours
4/8-node canary:           按变更影响或 release gate
```

如果自动 watcher/CI 尚未实现，C01 只能标记 `manual-active`，不得声称“实时监控已经上线”。

## 3. 状态模型

| 状态 | 定义 | 是否允许 C02+ 使用 |
|---|---|---|
| `unknown` | 尚未 fetch 或证据过期 | 否 |
| `latest-unreachable` | 根分支或锁定依赖无法获取 | 否 |
| `latest-reproducible` | 根提交与所有依赖可精确 checkout，尚未验证 | 否 |
| `latest-testing` | candidate 正在跑门禁 | 否 |
| `latest-ready` | 所有强制门禁通过且证据未过期 | 是 |
| `latest-repaired-ready` | 以最新 upstream root 为基线，显式应用下游可审计修复后通过全部影响面门禁；修复尚未被 upstream 接收 | 允许 C02～C07 内部开发与回归；不允许对外 release，必须绑定 repair patch/SHA |
| `latest-failed` | 最新 candidate 可获取但测试失败 | 否；保留 last-known-good |
| `last-known-good` | 曾通过但已不是最新或 freshness 过期 | 只允许诊断/对照，不允许新的正式验收 |

核心规则：`latest` 不等于 `ready`。上游根仓虽然前进，但只要一个 gitlink 不可获取、lock 不一致或 smoke 失败，就必须发布 `latest-unreachable/latest-failed`，不能把旧 checkout 继续包装成最新版本。

## 4. 2026-08-31 当前审计快照

### 4.1 根仓

```text
local mirror:     /Users/sean/Desktop/gitee/MemoryService/ub_sim
remote canonical: serving-b/openeuler-2403-DS:/home/sj/git/ub_sim
remote:           https://github.com/LL-mixed/ub_sim.git
branch:           master
local HEAD:       7aec390
origin/master:    0fecb3b
divergence:       local behind 23, ahead 0
working tree:     M mem_service
```

### 4.2 mem_service 双重锁定

当前 local root `7aec390`：

```text
root gitlink:     17152cb487d7f975e9002fe3a6073897416f4624
lock revision:    17152cb487d7f975e9002fe3a6073897416f4624
submodule HEAD:   366a335275d14c277dc44d3a75070f35951a8fb4
result:           local checkout drift; source verifier must fail
```

最新 `origin/master=0fecb3b`：

```text
root gitlink:     4b13ec0eae2d1f91a1e203fe2644c9df1e0765f6
lock revision:    4b13ec0eae2d1f91a1e203fe2644c9df1e0765f6
mem_service origin/master: 366a335275d14c277dc44d3a75070f35951a8fb4
target object:    not present after explicit fetch
recursive fetch:  failed with "not our ref 4b13ec0..."
```

### 4.3 原始 upstream 结论

```text
latest known:        yes (0fecb3b after fetch)
latest checked out:  no
reproducible:        no
internal self-test:  not run, because source gate failed first
latest-ready:        no
status:              latest-unreachable
```

原始 `0fecb3b` 不是 UFlow 代码问题，也不能在工作区静默把 gitlink 换成相近 SHA 后继续叫 `latest-ready`。上游必须先完成以下任一动作：

1. 将 `mem_service@4b13ec0` 推送到 `https://github.com/LL-mixed/mem_service` 的可获取 ref；或
2. 在 ub_sim 上游提交一个新的 root revision，把 gitlink 与 `mem_service.lock` 原子地改到可获取且已验证的 commit。

### 4.4 2026-08-31 repaired-ready 闭环

为解除内部 Phase C 开发阻塞，同时保留上游事实，本次在隔离 checkout 建立了不同状态的 downstream repair：

```text
run root:          /home/sj/phasec01_runs/phasec01_ub_sim_20260831_01
canonical:         /home/sj/git/ub_sim@67c3ce5 (dirty, 未修改)
upstream root:     0fecb3b1037a424d7d285ed8c2792f08031e88ed
repair commit 1:   d78143e (reachable mem_service/QEMU gitlink + mem_service.lock)
validated commit:  c68cfb8ab139312cc9d41fb26f0ebcd3b00c7072
result:            latest-repaired-ready / manual-active
```

现场确认 raw upstream 除 `mem_service@4b13ec0` 外，`qemu@15951308` 也不可从对应子模块读取。repair 使用 `mem_service@366a335`、`qemu@3a247cb`，并在 `c68cfb8` 增加旧 QEMU 所需的 bridge ABI 兼容 alias。它不是静默拼版本：两次提交、完整 patch、依赖 manifest、失败 attempt 和最终日志全部归档。

门禁结果：

- U0/U1：八个顶层 gitlink 与 checkout HEAD 一致；`mem_service` gitlink/lock/VERSION/HEAD 一致；
- U2：Cargo workspace PASS（其中 sim-uapi 156 passed / 9 ignored），Python 304/304，post-alias sim-qemu 25/25，QEMU UB focused 22/22，guard 10/10；
- U3：QEMU、guest kernel/modules/UAPI/initramfs、24-app matrix 全部构建通过；2-node provider conformance 1/1；
- U4：4-node canary 1/1；本次不改变 node-count/full-mesh/routing/release 语义，因此不要求 8-node；
- cleanup：无 QEMU 进程或 candidate runtime socket 残留。

证据入口：[results/phasec01_ub_sim_20260831_01](../results/phasec01_ub_sim_20260831_01/)。C02～C07 内部开发必须绑定 `c68cfb8` 或归档的 `candidate_repair.patch`；对外 release 仍须等待 upstream 原子修复或接收该 repair。

## 5. Source of Truth 与版本规则

### 5.1 权威版本

默认权威源：

```text
ub_sim remote:  origin
ub_sim branch:  origin/master
candidate SHA:  fetch 后解析 origin/master，不使用缓存日期猜测
```

子模块的“正确版本”不是各自 branch tip，而是 candidate root commit 的 gitlink。禁止对所有子模块执行 `git submodule update --remote` 来追求所谓“每个仓都最新”；这会破坏根仓已经声明的可复现组合。

### 5.2 关键依赖登记

每次 candidate 必须记录并验证：

- `mem_service`：gitlink、`guest-linux/aarch64/mem_service.lock` revision、`VERSION`、实际 HEAD 四者一致；
- `vendor/qemu_8.2.0_ub`；
- `guest-linux/kernel_ub`；
- `vendor/obmm`；
- `vendor/simpler`；
- `vendor/pto-isa`；
- `vendor/pypto`；
- `vendor/pypto-lib`；
- `.gitmodules` 中以后新增的任何子模块。

### 5.3 原子更新规则

- 根 gitlink 和仓内 lock 必须在同一个可审计变更中更新；
- 被锁定提交必须已推送并可从声明 remote 获取；
- 下游不允许用本机只有的 commit、bundle 或 unadvertised object 作为正式基线；
- remote URL、branch、commit、fetch time、fetch exit code 全部进入 manifest；
- tag/release 如被采用，仍需保存解析后的 immutable commit SHA。

## 6. 非破坏式同步流程

### 6.1 保护 canonical checkout

同步开始先只读记录：

```bash
git -C <ub_sim_canonical> status --short --branch
git -C <ub_sim_canonical> remote -v
git -C <ub_sim_canonical> rev-parse HEAD
git -C <ub_sim_canonical> submodule status --recursive
```

权威 canonical checkout 是容器内 `/home/sj/git/ub_sim`。若它 dirty：

- 保存状态到 evidence；
- 不执行 pull、merge、reset、checkout、clean 或覆盖式 rsync；
- 在 `/home/sj/phasec01_runs/<run_id>/ub_sim` 创建隔离 candidate clone；
- canonical dirty 内容属于现有工作，不由 C01 自动修复。

### 6.2 Candidate 获取

参考流程：

```bash
phasec01_run_id=phasec01_ub_sim_YYYYMMDD_HHMMSS
phasec01_stage=/home/sj/phasec01_runs/${phasec01_run_id}
phasec01_remote=$(git -C <ub_sim_canonical> remote get-url origin)

git -C <ub_sim_canonical> fetch origin --prune
phasec01_candidate=$(git -C <ub_sim_canonical> rev-parse origin/master)

git clone --no-checkout "$phasec01_remote" "$phasec01_stage/ub_sim"
git -C "$phasec01_stage/ub_sim" checkout --detach "$phasec01_candidate"
git -C "$phasec01_stage/ub_sim" submodule sync --recursive
git -C "$phasec01_stage/ub_sim" submodule update --init --recursive
```

变量只用于本任务，不复用 `HOME/CODEX_HOME` 等系统变量。失败时保留 candidate SHA、失败 dependency 和 stderr；不得改写 dependency revision 后重试并继续声称验证了原 candidate。

## 7. 分层门禁

### U0：环境与来源

必须记录：

```text
ssh alias / hostname / IP
container name / image / id
container user=root
workspace root=/home/sj
canonical ub_sim=/home/sj/git/ub_sim
OS / kernel / arch
candidate remote / branch / SHA
start/end time
candidate workspace
available CPU/memory/disk
QEMU/KVM or emulation prerequisites
```

serving-b 是默认入口；如果实际 QEMU/guest 验证需换执行 host，必须在 manifest 中说明原因和目标，不能沿用 serving-b 的环境结论。

本次 openEuler 现场补齐的最小环境依赖为 `glib2-devel pixman-devel zlib-devel flex bison bc openssl-devel elfutils-libelf-devel rsync cpio`。QEMU configure 固定增加 `--python=/usr/bin/python3`，避开 `/usr/local` Python 的不完整 distlib；当前 guest Makefile 需要容器内 `aarch64-linux-gnu-gcc/ar` 兼容软链接，具体目标见 evidence 的 `environment_prerequisites.txt`。

### U1：源码与依赖可复现

强制条件：

1. candidate checkout clean；
2. `git submodule status --recursive` 不包含 `-`、`+`、`U`；
3. 每个 gitlink object 都可读取；
4. `mem_service` gitlink/lock/HEAD/VERSION 一致；
5. `verify_mem_service_source.py` 通过；
6. Cargo lock、scenario 和关键 artifact 输入可定位；
7. 不存在本机路径或未声明 override 替代锁定源码。

任一失败，状态立即为 `latest-unreachable`，后续构建和 workload 不再运行。

### U2：轻量 source/unit/contract

每个 candidate 强制执行：

```text
cargo fmt --all -- --check
cargo test --workspace
python contract tests selected from changed components
guest app/source layout and mem_service downstream contract
```

Python 测试选择必须覆盖本次 diff 触及的 runner、topology、QEMU contract、Memory Service contract 和 Web console；不能只固定跑一个与变更无关的小测试。输出完整命令、测试数、跳过数和 exit code。

### U3：构建与 2-node integrated smoke

每个会进入 Phase C 的 latest-ready candidate 至少完成：

1. guest app build/source verification；
2. 2 logical-node headless 启动；
3. Memory Service + OBMM provider conformance 或等价当前正式入口；
4. remote mapping/read-write/checksum/completion；
5. shutdown 和无残留 QEMU/socket/process。

首选仓内正式脚本，不人工拼 QEMU command。当前可审计入口包括：

```text
guest-linux/aarch64/scripts/run_ub_app_build_matrix.sh
guest-linux/aarch64/scripts/run_ub_dual_node_mem_service_obmm_provider_conformance.sh
```

如果上游重命名入口，C01 agent 必须根据 candidate README/AGENTS/脚本合同更新命令和本文，不能静默跳过。

### U4：4/8-node 影响面 canary

下列变更必须追加 4-node canary：

- topology/scenario、QEMU bridge、guest kernel/UAPI；
- OBMM/GSVA/coherence；
- Memory Service provider/readiness；
- multi-node launcher、RPC/port/runtime isolation；
- UB-attached SSD/NPU。

下列变更必须追加 8-node canary：

- node-count、full-mesh、routing 或 scale-out 语义改变；
- 4-node 不能覆盖的端口、queue、resource isolation；
- 正式 Phase C acceptance/release。

当前候选脚本包括 `run_ub_four_node_smoke.sh`、`run_ub_eight_node_smoke.sh` 及对应 focused matrix。实际选择必须写入 `change_impact.json`。

模型权重、完整 inference 和长矩阵不属于每次 C01 的固定 gate；只有候选变更触及模型 workload/runtime 或进入相应 release 时才执行，避免用高成本测试掩盖基础版本问题。

## 8. Change Impact 决策

| 变更范围 | 最低门禁 |
|---|---|
| docs only | U0-U1；文档引用/链接检查 |
| Web console only | U0-U2；console unit/API/visual smoke |
| Rust model/CLI/report | U0-U3 |
| scenario/topology/launcher | U0-U4（至少 4-node） |
| mem_service/OBMM/GSVA/QEMU/kernel/UAPI | U0-U4（2+4-node，必要时 8-node） |
| release/Phase C E2E | U0-U4 全部，含 8-node canary |

如果 diff 同时命中多个类别，采用最高门禁。门禁降级必须在 summary 中写明理由和审批人，不能由 agent 自行因为耗时而跳过。

## 9. 自动化交付要求

PhaseC-01 实施 agent 需要交付一个可重复入口，而不是每次手工复制命令。建议落点：

```text
data-service/tools/phasec/ub_sim_baseline_guard.py
data-service/tools/tests/test_phasec_ub_sim_baseline_guard.py
```

CLI 至少支持：

```text
inspect    只读发现 canonical 状态与最新 candidate
stage      在隔离目录获取 candidate 和全部 pinned dependency
check      执行 U0-U2
smoke      执行 U3/U4
record     将已有且非空的正式日志登记为 U2/U3/U4 证据
report     生成状态、manifest 和 summary
```

实现要求：

- 默认 fail closed；
- 每个外部命令保存 argv、cwd、开始结束时间和 exit code；
- 支持 `--candidate-sha` 复验历史 candidate；
- 支持 `--offline` 只核验 last-known-good，但结果不得标记 latest；
- 影响面要求 4-node 时，`report --require-u4` 必须 fail closed；
- 不提供自动 reset/clean canonical checkout 的功能；
- runner 被中断后仍执行有界 cleanup 并保存 partial evidence；
- 状态输出既有人读 `summary.md`，也有机器读 `baseline.json`。

活跃期定时/事件触发可以由 CI 或受控 automation 调用同一 CLI。调度器不能包含另一套隐式同步逻辑。

## 10. Evidence Bundle

结果路径：

```text
remote: /home/sj/phasec01_runs/<phasec01_run_id>/evidence/
local:  data-service/results/<phasec01_run_id>/
```

最低文件集：

```text
baseline.json
environment.txt
root_status_before.txt
root_refs.txt
root_diff_summary.txt
submodule_status.txt
dependency_manifest.json
change_impact.json
commands.jsonl
unit_contract.log
smoke.log
cleanup.txt
summary.md
```

`baseline.json` 至少包含：

```text
schema_version
checked_at / freshness_deadline
root_remote / branch / candidate_sha
canonical_sha / canonical_dirty
dependencies{name,url,gitlink,lock,actual,reachable}
gates{U0,U1,U2,U3,U4}
latest / reproducible / ready
status / failure_class / failure_reason
last_known_good_sha
evidence_dir
```

raw log 应 immutable；重试使用新的 `attempt_id`，不得覆盖失败结果。

## 11. 失败处理

| 失败 | 状态 | 动作 |
|---|---|---|
| GitHub/DNS/SSH 暂时不可达 | `unknown` 或 `latest-unreachable` | 保留网络错误；可使用 LKG 做诊断，不宣称 latest |
| root fetch 成功但 gitlink object 缺失 | `latest-unreachable` | 向上游报告缺失 SHA；禁止替换 revision |
| gitlink/lock/version 不一致 | `latest-unreachable` | 要求上游原子修复 |
| unit/contract 失败 | `latest-failed` | 保存最小失败证据；停止进入 smoke 或按诊断需要运行 targeted test |
| 2-node smoke 失败 | `latest-failed` | 保存 serial/QEMU/provider/cleanup；LKG 不升级 |
| cleanup 不完整 | `latest-failed` | 结果无效；只清理本轮明确 PID/socket/run dir |
| canonical dirty | 不改变 candidate 状态 | 只用隔离 clone；不处理用户工作区 |

不得采取：

- `git reset --hard`、`git clean -fdx` 或覆盖 canonical checkout；
- 把另一个可获取 submodule commit 写入工作区后继续宣称验证原 candidate；
- 在 smoke 失败时切回 simulator 私有 store/file/in-process helper 旁路；
- 只保留最终成功 attempt，删除之前失败证据；
- 把 historical report 当作 current candidate pass。

## 12. Agent 执行顺序

1. 读取 ub_sim candidate 自身的 `AGENTS.md`、README 和与变更范围有关的子目录说明；
2. 只读检查 canonical checkout 和 remote；
3. fetch root 并解析 candidate SHA；
4. 在隔离目录 checkout candidate；
5. 先跑 U1，依赖不可复现则立即 fail closed；
6. 根据 diff 生成 change impact，依次跑 U2/U3/U4；
7. 保存 source、test、smoke、failure 和 cleanup 证据；
8. 更新本文“当前审计快照”和项目总控，不修改历史事实；
9. 只有所有强制 Gate 通过才发布 `latest-ready`。

## 13. 验收标准

PhaseC-01 首版关闭必须同时满足：

1. baseline guard CLI 和 focused unit tests 已实现；
2. plain `latest-ready` 要求权威 root 与全部 pinned dependency 可从声明 remote 精确获取；`latest-repaired-ready` 必须另有不可变 repair SHA/patch、替换理由与完整门禁，且禁止用于对外 release；
3. clean isolated checkout 中 gitlink/lock/HEAD/VERSION 全部一致；
4. U2 轻量 source/unit/contract 通过；
5. U3 2-node integrated smoke 通过并完成 checksum/completion/cleanup；
6. 依据 candidate diff 完成所需 U4 canary；
7. evidence bundle 字段齐全，latest/reproducible/ready 三个判断可机器读取；
8. latest candidate 失败时 last-known-good 不被覆盖，Phase C 下游明确阻断；
9. 定时/开工前触发方式已经接入，或状态明确保持 `manual-active`。

## 14. 当前 Next Gate

内部主线：C02 可以基于 `c68cfb8ab139312cc9d41fb26f0ebcd3b00c7072` 开工，先冻结 UFlow/ub-sim 中立合同；每次开工前继续用 guard 检查 freshness，不得回退到 canonical dirty checkout。

上游/release 主线：请 ub_sim 上游原子修复 `mem_service@4b13ec0` 与 `qemu@15951308` 的可获取性或 gitlink，并吸收 bridge compatibility alias。新 upstream root 到达后重新执行 U0-U4，只有不依赖下游 patch 时才升级为 plain `latest-ready`。另需把 guard 接入每日/事件触发，才能从 `manual-active` 升级为持续门禁。
