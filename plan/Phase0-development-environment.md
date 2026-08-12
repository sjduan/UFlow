# Phase0: UFlow 统一开发与远端验证环境

Last reviewed: 2026-08-12

Status: active; this document is the single source of truth for UFlow development-machine naming and remote execution.

## 1. 当前唯一默认环境

从 2026-08-12 起，UFlow/Data Service 的新开发、构建、NPU 验证和远端 Monitor/Trace 验收默认使用：

```text
local workspace:  /Users/sean/Desktop/gitee/MemoryService
ssh alias:        serving-b
remote container: openeuler-2403-DS
remote repo:      /home/sj/git/data-service
model root:       /data/models
run root:         /tmp/proj_output/<run_id>
long-log root:    /home/sj/logs/<run_id>
```

标准进入方式：

```bash
ssh serving-b
docker exec -it openeuler-2403-DS bash
cd /home/sj/git/data-service
```

`serving-a`、`serving-c`、`serving-d` 是同一四机系统中的可用节点，但只有任务明确要求多机、对照或故障验证时才使用。任何文档未指定目标机时，一律解释为 `serving-b`。

连接/IP/SSH config 的权威来源：

- `/Users/sean/Desktop/gitee/Serving_proj/memory/remote_connection.md`
- `/Users/sean/Desktop/gitee/MemoryService/memory/remote_connection.md`

其它文档不得复制或重新发明 IP、key、ProxyCommand；优先使用已配置的 `ssh serving-a|b|c|d` alias。

### 1.1 2026-08-12 现场只读确认

当前已实际确认：

```text
ssh alias:       serving-b
hostname:        liteserver-560t-1.novalocal
private IP:      192.169.0.85
container:       openeuler-2403-DS (running)
container repo:  /home/sj/git/data-service
remote HEAD:     e854fa0003f719a2c451c07ba77a81ea1fa58eac
kernel:          5.10.0-182.0.0.95.r2220_157.hce2.aarch64
```

关键阻塞：容器内 remote repo 当前是 **dirty working tree**，包含多项 tracked 修改和未跟踪的 SSD 源码/build 产物；本地 `data-service/main` 已到 `6179941`。只读 SHA-256 抽查的 16 个核心 Rust/Python SSD 相关源码与本地 `6179941` 内容 16/16 一致，说明主要源码很可能已经被本地主线吸收；但 remote Git provenance、PhaseB examples/plans/results 和 build 产物仍未完整对账。在任何 E04/E06 或性能任务开始前，必须先保存清单并形成明确执行基线，禁止直接 rsync 覆盖、`git reset --hard`、`git checkout --` 或清理未跟踪文件。

容器中的仓库还会触发 Git `dubious ownership` 检查。只读审计使用单命令参数：

```bash
git -c safe.directory=/home/sj/git/data-service status --short
git -c safe.directory=/home/sj/git/data-service rev-parse HEAD
```

不要为了绕过检查擅自修改容器全局 Git config。后续若确需永久配置，应由环境维护者明确处理。

## 2. 开工前强制检查

每个执行 agent 在同步、构建或运行前必须先确认实际目标，不得依据旧文档假定主机或 NPU 编号：

```bash
ssh serving-b 'hostname; hostname -I; docker ps --format "table {{.Names}}\t{{.Status}}"'

ssh serving-b \
  "docker exec openeuler-2403-DS bash -lc '
    set -e
    cd /home/sj/git/data-service
    printf \"repo=%s\\n\" \"\$PWD\"
    git -c safe.directory=/home/sj/git/data-service status --short
    git -c safe.directory=/home/sj/git/data-service rev-parse HEAD
    uname -r
    python --version
    npu-smi info
  '"
```

验证记录至少保存：

```text
ssh_alias=serving-b
host_name / host_ip
container_name / container_id / image
remote_repo / git_commit / git_status
kernel
CANN/runtime version
npu-smi snapshot
selected device id
command / env / run_dir / exit_code
```

NPU7 只是旧环境中经常使用的历史 device。新任务必须先根据 `npu-smi info` 选择空闲且健康的 device，并把实际编号写入产物；禁止把 7 作为无法覆盖的默认值。

## 3. 四机使用边界

| SSH alias | 默认用途 | 使用规则 |
|---|---|---|
| `serving-b` | UFlow/Data Service 单机开发、构建、功能与性能验证 | 默认 |
| `serving-a` | 明确指定的对照、基础环境或四机控制任务 | 非默认，任务必须写明 |
| `serving-c` | PhaseC/多机扩展、故障或拓扑验证 | 未明确授权时不使用 |
| `serving-d` | PhaseC/多机扩展、故障或拓扑验证 | 未明确授权时不使用 |

单机任务不得为了“顺手验证”扩展到其它三台机器。跨机任务必须逐台记录 hostname、container、device、commit 和 clock/网络条件，不能只写“abcd 四机通过”。

## 4. 旧环境迁移口径

旧文档中的以下写法已经停止作为当前执行指令：

```text
1号机 / 2号机
local -> 1号机 -> 2号机
source ~/.bashrc; next "..."
UFLOW_HOST1_SSH / UFLOW_HOST2_SSH
UFLOW_HOST1_KEY / UFLOW_HOST2_KEY_ON_HOST1
machine2
```

统一迁移为：

```text
ssh serving-b
  -> docker exec openeuler-2403-DS
  -> /home/sj/git/data-service
```

历史实验中的“旧 2号机 NPU7”必须保留为历史事实，不得批量改写成“serving-b NPU7 已通过”。`serving-b` 虽承接了相关容器，但主机、kernel、device 状态和性能条件可能已经变化。状态口径为：

- 原 run dir、日期和数值仍是历史证据；
- 若新基线未在 `serving-b` 复跑，标为 `historical-pass`；
- 新的 pass 必须生成新的 run id 和环境快照；
- 涉及 kernel、NUMA、SSD、DMA、带宽或延迟的结论必须在 `serving-b` 重新测量，不能沿用旧机能力判断。

## 5. 常用远端操作

### 5.1 直接执行容器命令

```bash
ssh serving-b \
  "docker exec openeuler-2403-DS bash -lc '
    cd /home/sj/git/data-service
    git -c safe.directory=/home/sj/git/data-service rev-parse HEAD
    npu-smi info
  '"
```

### 5.2 本地端口转发

UFlow monitor API 在容器/宿主网络的 `127.0.0.1:8082` 监听时，本地默认使用：

```bash
ssh -N -L 18082:127.0.0.1:8082 serving-b
```

浏览器/API 使用 `http://127.0.0.1:18082`。不再建立“本地→1号机→2号机”的两段手工 tunnel；`serving-b` SSH alias 自身负责经 `serving-a` 的 ProxyCommand。

### 5.3 结果回传

结果优先写入容器内 `/tmp/proj_output/<run_id>`。回传前先确认目录与 run id，再从容器流式打包：

```bash
RUN_ID=phase_name_YYYYMMDD_HHMMSS
ssh serving-b \
  "docker exec openeuler-2403-DS tar -C /tmp/proj_output -czf - $RUN_ID" \
  > "/tmp/$RUN_ID.tgz"
```

不得使用宽泛 glob 拉取或删除 `/tmp/proj_output`；测试结束只清理能够确认属于本轮的进程、socket、object 和临时目录。

## 6. 文档与派工要求

从本文件生效后：

1. 新任务的环境栏必须写 `serving-b / openeuler-2403-DS / actual device`，不能只写“远端”或“2号机”。
2. 新命令使用 `ssh serving-b`，不得重新引入双跳变量或 `next` helper。
3. 历史章节保留旧环境标记；当前步骤、Future Work 和验收矩阵必须使用新环境。
4. 每次环境变化同时更新本文件、`memory/remote_connection.md` 和 `Phase0-uflow-project-control-tower.md`。
5. E06 Trace v2 先在默认机完成，再由 E04 Monitor v2 消费其合同；二者新验收都必须在 `serving-b` 留存环境快照。
