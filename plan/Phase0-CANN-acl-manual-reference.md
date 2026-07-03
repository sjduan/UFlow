# Phase0: CANN ACL Manual Reference Guide

Last reviewed: 2026-06-10

Source PDF: `${MEMORY_SERVICE_ROOT}/memory/CANN_manual.pdf`

## 1. 文档定位

这份 Phase0 文档用于“使能”后续 UFlow / HBM backend 开发中的 CANN ACL 文档查询能力。

我们后续在做本机 HBM 管理、跨进程 Device 内存共享、Host/Device 数据交互、Stream 同步、Event 等待、Context 管理时，都应优先回到这份 CANN manual 中确认 C/C++ API 的：

- 函数原型。
- 参数输入/输出方向。
- 返回值和错误码。
- 产品支持情况。
- 约束说明。
- 推荐调用流程。
- 相关数据类型。

本项目当前只关注 **C/C++ API**。PDF 中从第 3 章开始是 Python 应用开发，默认忽略。

## 2. PDF 基本信息

```text
title:      CANN 社区版 8.5.0 应用开发指南
version:    文档版本 01
date:       2026-05-20
pages:      3749
scope:      C/C++ 应用开发 + Python 应用开发 + API 参考
```

页码说明：

- 本文中的页码统一写作 **PDF page**，即 PDF 阅读器显示的实际页码。
- PDF 内部印刷页码与 PDF page 有偏移。例如 C/C++ 第 2 章从 PDF page 82 开始，但印刷页码显示为第 2 页。
- 查找时优先使用 PDF outline / bookmarks；如果阅读器页码和本文不同，以 PDF page 为准。

## 3. 只读 C/C++ 范围

需要阅读：

```text
2 应用开发 (C&C++)         PDF page 82 - 2262
2.13 acl API 参考          PDF page 383 - 2259
2.14 FAQ 案例集            PDF page 2260
2.15 附录                  PDF page 2261
```

默认忽略：

```text
3 应用开发 (Python)        PDF page 2263 起
```

检索时如果跳到第 3 章或 Python API，除非明确需要对照概念，否则不要作为实现依据。

## 4. 内容简介

### 4.1 C/C++ 应用开发主线

C/C++ 部分主要讲 ACL 应用的完整生命周期：

1. 准备环境。
2. 包含头文件、链接库。
3. `aclInit` 初始化。
4. 申请运行时资源：Device、Context、Stream、Event、Host/Device memory。
5. 进行模型推理、单算子调用、Kernel 调用或数据处理。
6. 通过 Stream/Event/Notify 做异步同步。
7. 释放 Stream、Event、内存、Device。
8. `aclFinalize` 去初始化。

对于 UFlow PhaseA，最相关的是：

- `2.3 接口概述与调用流程`
- `2.4 初始化与去初始化`
- `2.5 运行时管理`
- `2.9.1 内存二次分配管理`
- `2.13.6 运行时管理`
- `2.13.14 数据类型及其操作接口`

### 4.2 API 参考页的固定结构

每个 C/C++ API 页面通常按以下结构组织：

```text
产品支持情况
功能说明
函数原型
参数说明
返回值说明
约束说明
```

查函数时不要只看函数原型，必须同时看：

- 产品支持情况：确认 910 / A2 / A3 / 推理 / 训练系列是否支持。
- 约束说明：很多内存、跨进程共享、虚拟化、算力分组、RC 形态限制都写在这里。
- 参数说明：ACL 很多参数是预留或固定取值，不看这里容易误用。
- 数据类型：复杂结构体和 enum 往往在 `2.13.14`。

## 5. 快速定位方法

### 5.1 已知函数名时

优先做精确函数名搜索。

PDF 阅读器：

```text
Command+F / Ctrl+F -> 输入完整函数名，例如 aclrtMallocPhysical
```

本地命令行可以用内置 `pypdf` 快速定位：

```bash
${CODEX_RUNTIME_ROOT}/dependencies/python/bin/python3 - <<'PY'
from pypdf import PdfReader
fn = "aclrtMallocPhysical"
r = PdfReader("${MEMORY_SERVICE_ROOT}/memory/CANN_manual.pdf")
for i, page in enumerate(r.pages, start=1):
    text = page.extract_text() or ""
    if fn in text:
        print(i, text[text.find(fn)-120:text.find(fn)+300])
PY
```

建议搜索策略：

- 第一次搜索完整函数名。
- 如果命中太多，加上章节名或小节号，例如 `2.13.6.9.28 aclrtMallocPhysical`。
- 如果函数名相近，优先去 outline 中的 `2.13 acl API参考`，不要直接看样例里的调用片段。

### 5.2 已知能力、不知道函数名时

先查本文件的“能力索引表”，找到章节，再回 PDF。

查找顺序：

1. 先看概念章节：例如 Context/Stream/Event 先看 `2.5.1`。
2. 再看流程章节：例如数据传输先看 `2.5.3`，同步等待先看 `2.5.6`。
3. 最后看 API reference：例如内存管理看 `2.13.6.9`。
4. 如果涉及 enum/struct，再看 `2.13.14`。

### 5.3 已知错误码或类型时

错误码：

```text
2.13.14.1 aclError       PDF page 1961
```

常用 runtime 类型：

```text
aclrtContext             PDF page 1994
aclrtStream              PDF page 1994
aclrtEvent               PDF page 1994
aclrtMemcpyKind          PDF page 1990
aclrtPhysicalMemProp     PDF page 2030
aclrtDrvMemHandle        PDF page 2030
aclrtMemGranularityOptions PDF page 2031
aclrtAllocatorDesc       PDF page 2255
```

## 6. C/C++ 章节地图

| 主题 | PDF page | 用途 |
|---|---:|---|
| C/C++ 简介 | 82 | ACL C/C++ 应用开发总览 |
| 头文件和库文件说明 | 85 | 确认 include 和 link 依赖 |
| 接口调用流程 | 89 | 确认应用整体生命周期 |
| 初始化与去初始化 | 91 | `aclInit` / `aclFinalize` 使用方式 |
| 运行时概念 | 92 | Host、Device、Context、Stream、Event 概念 |
| 运行时资源申请与释放 | 97 | Device/Stream 申请释放顺序 |
| 数据传输 | 100 | Host/Device/Device 内拷贝流程 |
| Stream 管理 | 105 | 单 Stream、多 Stream 使用方式 |
| 同步等待 | 108 | Device/Stream/Event/Notify 同步模式 |
| 内存二次分配管理 | 311 | 大块内存池二次分配约束 |
| acl API 参考 | 383 | C/C++ API 总入口 |
| 同步&异步 API 说明 | 483 | 判断接口同步/异步语义 |
| API 头文件和库文件说明 | 483 | API reference 版 include/link 说明 |
| 数据类型及操作接口 | 1961 | enum、struct、handle、error |
| Python 应用开发 | 2263 | 默认忽略 |

## 7. PhaseA 重点能力索引

### 7.1 初始化 / 去初始化

| 能力 | 函数 | PDF page | 备注 |
|---|---|---:|---|
| 初始化 ACL | `aclInit` | 488 | 进程使用 ACL 前必须调用 |
| 去初始化 ACL | `aclFinalize` | 502 | 进程退出前调用 |
| 初始化引用释放 | `aclFinalizeReference` | 503 | 多组件/引用场景可查 |

推荐阅读顺序：

1. `2.4 初始化与去初始化`，PDF page 91。
2. `2.13.5.1 aclInit`，PDF page 488。
3. `2.13.5.2 aclFinalize`，PDF page 502。

### 7.2 Device 管理

| 能力 | 函数 | PDF page |
|---|---|---:|
| 设置当前 Device | `aclrtSetDevice` | 517 |
| 释放/重置 Device | `aclrtResetDevice` | 518 |
| 强制重置 Device | `aclrtResetDeviceForce` | 519 |
| 查询当前 Device | `aclrtGetDevice` | 521 |
| 查询运行模式 | `aclrtGetRunMode` | 521 |
| 查询 Device 数量 | `aclrtGetDeviceCount` | 523 |
| 查询 Device 状态 | `aclrtQueryDeviceStatus` | 525 |
| 查询 SoC 名称 | `aclrtGetSocName` | 526 |
| Device 间可访问性 | `aclrtDeviceCanAccessPeer` | 528 |
| 开启 P2P | `aclrtDeviceEnablePeerAccess` | 529 |
| Device 同步等待 | `aclrtSynchronizeDevice` | 534 |
| Device 同步等待带超时 | `aclrtSynchronizeDeviceWithTimeout` | 535 |
| 查询 Device capability | `aclrtGetDeviceCapability` | 537 |
| 查询拓扑 | `aclrtGetDevicesTopo` | 538 |
| 用户/逻辑/物理 Device ID 转换 | `aclrtGetLogicDevIdByUserDevId` 等 | 541-545 |

UFlow 重点：

- daemon 启动时需要 `aclrtGetDeviceCount` 建立本机设备表。
- 多卡和容器内可见卡场景，需要区分 user device id、logic device id、physical device id。
- HBM 跨 device 共享或拷贝前，先看 P2P capability 和 topology。

### 7.3 Context 管理

| 能力 | 函数 | PDF page |
|---|---|---:|
| 创建 Context | `aclrtCreateContext` | 547 |
| 销毁 Context | `aclrtDestroyContext` | 548 |
| 设置当前 Context | `aclrtSetCurrentContext` | 549 |
| 获取当前 Context | `aclrtGetCurrentContext` | 550 |
| 获取默认 Stream | `aclrtCtxGetCurrentDefaultStream` | 552 |
| 查询 Primary Context 状态 | `aclrtGetPrimaryCtxState` | 553 |

推荐原则：

- 简单 demo 可依赖 `aclrtSetDevice` 隐式创建默认 Context。
- UFlow daemon / HBM backend 更建议显式管理 Context，避免多线程行为依赖调度顺序。
- 不同 Context 的 Stream/Event 隔离，不能跨 Context 建立同步等待关系。

### 7.4 Stream 管理

| 能力 | 函数 | PDF page |
|---|---|---:|
| 创建 Stream | `aclrtCreateStream` | 555 |
| 创建 Stream V2 | `aclrtCreateStreamV2` | 557 |
| 配置 Stream | `aclrtCreateStreamWithConfig` | 559 |
| 销毁 Stream | `aclrtDestroyStream` | 562 |
| 强制销毁 Stream | `aclrtDestroyStreamForce` | 563 |
| 查询 Stream 状态 | `aclrtStreamQuery` | 567 |
| 同步等待 Stream | `aclrtSynchronizeStream` | 567 |
| 同步等待 Stream 带超时 | `aclrtSynchronizeStreamWithTimeout` | 568 |
| 获取 Stream ID | `aclrtStreamGetId` | 570 |
| 获取可用 Stream 数 | `aclrtGetStreamAvailableNum` | 571 |

UFlow 重点：

- 所有异步拷贝和 event record/wait 都需要绑定 stream。
- 如果 UFlow 管理异步 H2D/D2H 或 device-to-device copy，需要明确 stream ownership。
- 默认 Stream 可用 `NULL` 表示，但长期服务不建议依赖默认 Stream。

### 7.5 Event / Notify 同步等待

| 能力 | 函数 | PDF page |
|---|---|---:|
| 创建 Event | `aclrtCreateEvent` | 578 |
| 创建 Event with flag | `aclrtCreateEventWithFlag` | 579 |
| 销毁 Event | `aclrtDestroyEvent` | 583 |
| 记录 Event | `aclrtRecordEvent` | 584 |
| 重置 Event | `aclrtResetEvent` | 585 |
| 查询 Event 状态 | `aclrtQueryEventStatus` | 587 |
| 查询 Event wait 状态 | `aclrtQueryEventWaitStatus` | 588 |
| 同步等待 Event | `aclrtSynchronizeEvent` | 588 |
| 同步等待 Event 带超时 | `aclrtSynchronizeEventWithTimeout` | 589 |
| 统计 Event 间耗时 | `aclrtEventElapsedTime` | 590 |
| Stream 等待 Event | `aclrtStreamWaitEvent` | 591 |
| Stream 等待 Event 带超时 | `aclrtStreamWaitEventWithTimeout` | 592 |
| IPC 导出 Event handle | `aclrtIpcGetEventHandle` | 596 |
| IPC 打开 Event handle | `aclrtIpcOpenEventHandle` | 597 |
| 创建 Notify | `aclrtCreateNotify` | 598 |
| 记录 Notify | `aclrtRecordNotify` | 600 |
| 等待并重置 Notify | `aclrtWaitAndResetNotify` | 601 |

Event 与 Notify 的选择：

- 一个 Event record 可以通知一个或多个 Event wait，wait 后 Event 不会自动重置。
- Notify wait 完成后会自动重置，一个 notify record 更适合通知一个 wait。
- UFlow 后续做跨 stream 数据依赖时，优先用 Event；做一次性唤醒可评估 Notify。

### 7.6 常规内存申请 / 释放 / 拷贝

| 能力 | 函数 | PDF page |
|---|---|---:|
| Device malloc | `aclrtMalloc` | 619 |
| Device malloc 32B 对齐 | `aclrtMallocAlign32` | 620 |
| Cached device malloc | `aclrtMallocCached` | 622 |
| flush cached memory | `aclrtMemFlush` | 623 |
| invalidate cached memory | `aclrtMemInvalidate` | 623 |
| Device free | `aclrtFree` | 626 |
| Host malloc | `aclrtMallocHost` | 628 |
| Host free | `aclrtFreeHost` | 630 |
| memset | `aclrtMemset` | 632 |
| async memset | `aclrtMemsetAsync` | 633 |
| memcpy | `aclrtMemcpy` | 634 |
| async memcpy | `aclrtMemcpyAsync` | 635 |
| batch memcpy | `aclrtMemcpyBatch` | 638 |
| 2D memcpy | `aclrtMemcpy2d` | 641 |
| async 2D memcpy | `aclrtMemcpy2dAsync` | 643 |
| 获取 memcpy desc 大小 | `aclrtGetMemcpyDescSize` | 645 |
| desc-based async memcpy | `aclrtMemcpyAsyncWithDesc` | 647 |
| offset async memcpy | `aclrtMemcpyAsyncWithOffset` | 648 |
| 查询内存信息 | `aclrtGetMemInfo` | 689 |
| 检查内存类型 | `aclrtCheckMemType` | 691 |
| 查询内存使用 | `aclrtGetMemUsageInfo` | 692 |

使用提示：

- Host/Device 交互先看 `2.5.3 数据传输`，PDF page 100。
- `aclrtMemcpy` 是同步接口。
- `aclrtMemcpyAsync` 需要 stream，并通常配合 `aclrtSynchronizeStream` 或 Event。
- 对大块内存池二次分配，先看 `2.9.1 内存二次分配管理`，PDF page 311。

### 7.7 Physical HBM / VMM / 跨进程共享

这是 UFlow PhaseA 最关键的 ACL 能力区。

| 能力 | 函数 | PDF page |
|---|---|---:|
| 申请 physical memory | `aclrtMallocPhysical` | 649 |
| 释放 physical memory | `aclrtFreePhysical` | 651 |
| 预留虚拟地址 | `aclrtReserveMemAddress` | 652 |
| 释放虚拟地址 | `aclrtReleaseMemAddress` | 654 |
| map physical -> virtual | `aclrtMapMem` | 655 |
| unmap virtual memory | `aclrtUnmapMem` | 656 |
| 导出 shareable handle | `aclrtMemExportToShareableHandle` | 657 |
| 设置 PID 白名单 | `aclrtMemSetPidToShareableHandle` | 660 |
| 导入 shareable handle | `aclrtMemImportFromShareableHandle` | 661 |
| 导出 shareable handle V2 | `aclrtMemExportToShareableHandleV2` | 662 |
| 设置 PID 白名单 V2 | `aclrtMemSetPidToShareableHandleV2` | 664 |
| 导入 shareable handle V2 | `aclrtMemImportFromShareableHandleV2` | 665 |
| 获取 bare tgid | `aclrtDeviceGetBareTgid` | 666 |
| 查询 allocation granularity | `aclrtMemGetAllocationGranularity` | 667 |
| 设置访问权限 | `aclrtMemSetAccess` | 668 |
| 获取访问权限 | `aclrtMemGetAccess` | 669 |
| 保留 allocation handle | `aclrtMemRetainAllocationHandle` | 670 |
| 查询 pointer 属性 | `aclrtPointerGetAttributes` | 676 |

推荐先阅读：

1. `aclrtMallocPhysical`，PDF page 649-651。
2. `aclrtReserveMemAddress` / `aclrtMapMem`，PDF page 652-656。
3. `aclrtMemExportToShareableHandle`，PDF page 657-660。
4. `aclrtMemSetPidToShareableHandle`，PDF page 660-661。
5. `aclrtMemImportFromShareableHandle`，PDF page 661-662。
6. `aclrtDeviceGetBareTgid`，PDF page 666-667。
7. `aclrtMemGetAllocationGranularity`，PDF page 667-668。

重要约束：

- `aclrtMallocPhysical` 的 size 应先按 `aclrtMemGetAllocationGranularity` 返回的粒度对齐。
- `aclrtMapMem` 的 size 必须与 physical allocation size 一致，并满足最小粒度对齐。
- shareable handle 和 physical handle 一一对应，同一进程内不允许一对多或多对一。
- 启用 PID 白名单时，导出进程需要调用 `aclrtMemSetPidToShareableHandle`。
- 文档明确 Docker 场景下 `aclrtDeviceGetBareTgid` 获取的是物理机上的进程 ID。
- import 前必须确保导出方 physical memory 仍然存在。
- import/export 不支持在同一进程里混用，目标是跨进程。
- 所有相关进程都释放 handle 后，底层 physical memory 才能真正释放。
- 部分接口不支持虚拟化实例、算力分组或特定 RC 形态，必须看函数页约束。

### 7.8 Host register / 自定义 allocator

| 能力 | 函数 | PDF page |
|---|---|---:|
| 注册 Host 内存 | `aclrtHostRegister` | 677 |
| 注册 Host 内存 V2 | `aclrtHostRegisterV2` | 678 |
| 获取 Host 对应 Device pointer | `aclrtHostGetDevicePointer` | 679 |
| 取消 Host 注册 | `aclrtHostUnregister` | 680 |
| 注册 allocator | `aclrtAllocatorRegister` | 693 |
| 根据 stream 获取 allocator | `aclrtAllocatorGetByStream` | 695 |
| 注销 allocator | `aclrtAllocatorUnregister` | 696 |
| allocator desc | `aclrtAllocatorDesc` | 2255 |

UFlow 重点：

- 如果 daemon-owned HBM import/map 受限，可能需要研究 `aclrtAllocatorRegister` 作为 service-controlled in-process allocator 入口。
- allocator descriptor 相关 setter 在 `2.13.14.181`，PDF page 2255 起。

### 7.9 错误、异常、执行控制

| 能力 | 函数/章节 | PDF page |
|---|---|---:|
| callback / report | `aclrtLaunchCallback` 等 | 697 |
| 最近错误消息 | `aclGetRecentErrMsg` | 715 |
| 设置异常回调 | `aclrtSetExceptionInfoCallback` | 715 |
| 获取 task/stream/device/error 信息 | `aclrtGet*FromExceptionInfo` | 717-721 |
| last error | `aclrtPeekAtLastError` / `aclrtGetLastError` | 721-722 |
| verbose error | `aclrtGetErrorVerbose` | 725 |
| `aclError` 错误码 | `2.13.14.1 aclError` | 1961 |

## 8. 常见开发问题怎么查

### 8.1 我要做 Context 管理

查找顺序：

1. `2.5.1 概念说明`，PDF page 92。
2. `2.13.6.3 Context管理`，PDF page 546。
3. 重点函数：`aclrtCreateContext`、`aclrtSetCurrentContext`、`aclrtGetCurrentContext`、`aclrtDestroyContext`。

需要确认：

- 是否使用默认 Context。
- 是否多线程。
- Stream/Event 是否在同一 Context。

### 8.2 我要做 H2D / D2H / D2D 数据交互

查找顺序：

1. `2.5.3 数据传输`，PDF page 100。
2. `2.13.6.9 内存管理`，PDF page 618。
3. `aclrtMallocHost` / `aclrtMalloc` / `aclrtMemcpy` / `aclrtMemcpyAsync`。
4. `aclrtMemcpyKind`，PDF page 1990。

需要确认：

- 源地址和目标地址属于 Host 还是 Device。
- 是否需要异步 copy。
- 如果异步 copy，使用哪个 Stream，在哪里同步。

### 8.3 我要做 Stream 同步

查找顺序：

1. `2.5.4 Stream管理`，PDF page 105。
2. `2.5.6 同步等待`，PDF page 108。
3. `2.13.6.4 Stream管理`，PDF page 555。

常用链路：

```text
aclrtCreateStream
aclrtMemcpyAsync / kernel launch / model execute async
aclrtSynchronizeStream 或 aclrtStreamQuery
aclrtDestroyStream
```

### 8.4 我要做 Event 等待

查找顺序：

1. `2.5.6 同步等待`，PDF page 108。
2. `2.13.6.5 Event管理`，PDF page 578。

常用链路：

```text
aclrtCreateEvent
aclrtRecordEvent(event, producer_stream)
aclrtStreamWaitEvent(consumer_stream, event)
aclrtSynchronizeStream(consumer_stream)
aclrtDestroyEvent
```

### 8.5 我要做 daemon-owned HBM physical handle

查找顺序：

1. `2.13.6.9.28 aclrtMallocPhysical`，PDF page 649。
2. `2.13.6.9.41 aclrtMemGetAllocationGranularity`，PDF page 667。
3. `2.13.6.9.34 aclrtMemExportToShareableHandle`，PDF page 657。
4. `2.13.6.9.35 aclrtMemSetPidToShareableHandle`，PDF page 660。
5. `2.13.6.9.36 aclrtMemImportFromShareableHandle`，PDF page 661。
6. `2.13.6.9.30-33` VMM reserve/map/unmap/release，PDF page 652-657。

最小验证链路：

```text
consumer: aclrtDeviceGetBareTgid -> send pid to daemon
daemon:   aclrtMemGetAllocationGranularity
daemon:   aclrtMallocPhysical
daemon:   aclrtMemExportToShareableHandle
daemon:   aclrtMemSetPidToShareableHandle
consumer: aclrtMemImportFromShareableHandle
consumer: aclrtReserveMemAddress
consumer: aclrtMapMem
consumer: use virtual pointer
consumer: aclrtUnmapMem
consumer: aclrtReleaseMemAddress
consumer: aclrtFreePhysical(imported handle)
daemon:   aclrtFreePhysical(original handle)
```

这条链路是 PhaseA HBM daemon 必须优先跑通的能力。

## 9. 建议记录格式

后续每次基于 CANN manual 确认一个 ACL 能力时，建议在对应 PhaseA 文档里记录：

```text
Capability:
Manual location:
Functions:
Required data types:
Call order:
Important constraints:
Open questions:
Validation result:
```

示例：

```text
Capability: cross-process HBM shareable handle
Manual location: 2.13.6.9.34-36, PDF page 657-662
Functions: aclrtMallocPhysical, aclrtMemExportToShareableHandle, aclrtMemSetPidToShareableHandle, aclrtMemImportFromShareableHandle
Required data types: aclrtDrvMemHandle, aclrtPhysicalMemProp, aclrtMemHandleType
Call order: exporter alloc/export/set-pid, importer import/map/use/free
Important constraints: import before exporter free; all processes must free; Docker pid should come from aclrtDeviceGetBareTgid
Open questions: imported handle to PyTorch/PyPTO pointer handoff
Validation result: TODO
```

## 10. PhaseA 默认查阅原则

- 只使用 C/C++ API，默认忽略 Python 章节。
- 先查概念和流程，再查函数原型。
- 每个函数必须看约束说明。
- 使用 physical memory API 前必须先查 allocation granularity。
- 跨进程共享必须用 `aclrtDeviceGetBareTgid` 获取目标进程 ID，不自行猜 OS pid。
- 异步接口必须记录 stream 和同步点。
- Event/Notify/Stream 不能跨不兼容 Context 乱用。
- 若 ACL 函数计划被 Rust 调用，先设计 C ABI shim，再封装 Rust safe API。

