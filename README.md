# UFlow

UFlow is a unified data service for model serving and heterogeneous-memory workloads.

It provides a service-side control plane for data objects, memory placement, leases, and data transfer. The current working target is local-node HBM, DDR, and SSD management on Ascend NPU systems. The longer-term target is a unified data layer for supernode-style systems, where HBM, DDR, SSD/SSU, and remote memory can be represented through the same object, placement, and transfer language.

## Ecosystem

UFlow is designed to work with the broader PyPTO serving stack:

| Repository | Role |
|---|---|
| [LL-mixed/ub_sim](https://github.com/LL-mixed/ub_sim) | UB data-flow simulation and validation environment. |
| [hw-native-sys/pypto-serving](https://github.com/hw-native-sys/pypto-serving) | Model-serving framework that UFlow is designed to integrate with. |
| [hw-native-sys/pypto](https://github.com/hw-native-sys/pypto) | High-performance AI accelerator programming framework used by the PyPTO runtime stack. |

## Current Status

UFlow currently supports:

- Rust daemon runtime with catalog, object lifecycle, transfer planning, transfer execution, stats, and trace modules.
- C/C++ ACL shim for Ascend runtime integration.
- Python SDK for client-side object and transfer operations.
- DataObject, Placement, Lease, TransferPlan, and TransferEvent primitives.
- HBM object allocation and service-owned HBM runtime views.
- DDR object allocation backed by memfd or mmap-backed files.
- SSD objects backed by daemon-owned, preallocated local file extents.
- Service-side HBM <-> DDR data movement.
- Default direct DDR <-> HBM transfer path using daemon-owned runtime views.
- Explicit pinned staging fallback path for comparison and compatibility.
- HBM <-> HBM and DDR <-> DDR local exchange experiments.
- Buffered SSD <-> DDR transfers with offset and range support.
- SSD <-> HBM transfers through a DDR relay path.
- Logical SSD <-> HBM direct transfers using a file-backed mmap host view and ACL copy to or from the daemon-owned HBM view.
- Automatic SSD <-> HBM path selection: objects smaller than 256 MiB use the relay path, while objects of 256 MiB or larger use logical direct by default, with runtime fallback to relay.
- Monitor API and lightweight static web monitor.
- Trace and benchmark utilities for transfer-stage analysis.

For cooperative external-owner DDR registration, read-only sharing and
explicit DDR copies, see the [external DDR usage guide](examples/ddr_v0/README.md).

Performance artifacts and validation summaries are kept under `results/`.

The current SSD direct path is logical direct at the UFlow object layer. It still traverses Linux file-backed host memory/page cache and is not physical NVMe/SSU-to-NPU DMA. `io_uring`, `O_DIRECT` pipelines, NVMe P2P, AICPU filesystem access, and SSU/LBA descriptor paths remain future work.

## Architecture

At a high level, UFlow separates the user-facing data contract from backend-specific memory operations:

```text
Client / SDK / Monitor
        |
        v
UFlow daemon
  - object catalog
  - lease manager
  - placement metadata
  - transfer planner
  - transfer executor
  - stats and trace
        |
        v
Backends
  - HBM via ACL runtime
  - DDR via memfd / mmap
  - SSD via daemon-owned file extents
  - SSD logical direct via file mmap + ACL copy
  - pinned host memory fallback
```

Clients describe what data they need and where it should live. UFlow decides how the object is represented, how it is leased, and how transfers are executed and observed.

## Repository Layout

```text
crates/
  uf-core/        Shared IDL and core Rust types.
  uf-acl-sys/     Rust FFI bindings to the native ACL shim.
  uf-daemon/      UFlow daemon runtime.

native/
  acl_shim/       C/C++ ABI layer around CANN ACL runtime calls.

sdk/python/
  uflow/          Python client SDK and object wrappers.

examples/
  phasea*/        HBM/DDR and serving integration probes.
  phaseb*/        SSD object, transfer, and policy validation.
  phasee*/        Unified service and transfer-framework probes.

experiments/
  acl_hbm_smoke/  Standalone ACL validation and bandwidth probes.

tools/
  phasee04/       Monitor and tunnel helper scripts.
  uflow_monitor_api.py

ui/
  uflow-monitor/  Static monitor UI.

plan/
  Public architecture notes and reports.

results/
  Selected benchmark summaries and trace artifacts.
```

Detailed planning notes and raw run artifacts are maintained locally. Source commits include only selected documentation and validation results intended for publication.

## Build

The daemon depends on CANN ACL headers/libraries and a working Rust toolchain on the target machine.

```bash
cmake -S . -B build
cmake --build build -j

export LD_LIBRARY_PATH="$PWD/build/lib:${LD_LIBRARY_PATH:-}"
cargo build --release -p uf-daemon
```

Python SDK usage normally requires:

```bash
export PYTHONPATH="$PWD/sdk/python:${PYTHONPATH:-}"
export UF_ACL_LIB="$PWD/build/lib/libuf_acl_shim.so"
```

## Run

Start a local daemon:

```bash
./target/release/uf-daemon \
  --device "${UF_TARGET_DEVICE:-0}" \
  --socket "${UF_SOCKET:-/tmp/uflow.sock}"
```

Run a client smoke test against the same socket:

```bash
PYTHONPATH="$PWD/sdk/python" \
UF_SOCKET=/tmp/uflow.sock \
python3 examples/phasee01/uflow_e01_unified_transfer_smoke.py
```

Validate SSD object lifecycle without requiring an NPU transfer:

```bash
PYTHONPATH="$PWD/sdk/python" \
UF_SOCKET=/tmp/uflow.sock \
UF_SSD_ROOT=/data/uflow_ssd \
python3 examples/phaseb01/uflow_b01_ssd_object_smoke.py
```

The canonical remote development target is `serving-b` and its `openeuler-2403-DS` container. Connection details are defined in `memory/remote_connection.md` and `plan/Phase0-development-environment.md`.

```bash
export UFLOW_REMOTE_SSH=serving-b
export UFLOW_REMOTE_CONTAINER=openeuler-2403-DS
export UFLOW_REMOTE_WORKDIR=/home/sj/git/data-service

ssh "$UFLOW_REMOTE_SSH"
docker exec -it "$UFLOW_REMOTE_CONTAINER" bash
```

The old `UFLOW_HOST1_*` / `UFLOW_HOST2_*` two-hop convention is retired. The `serving-b` SSH alias already carries the ProxyCommand through `serving-a`. A local monitor tunnel should use:

```bash
ssh -N -L 18082:127.0.0.1:8082 serving-b
```

As of 2026-08-12, `/home/sj/git/data-service` in the remote container is a dirty `e854fa0` worktree while local `main` is `6179941`. Preserve and reconcile the remote changes before syncing; do not use destructive `rsync --delete`, reset, checkout, or clean commands.

## Development Notes

- `mode=auto` selects the current service-side default transfer strategy.
- `mode=pinned_async` selects the pinned staging fallback path.
- `mode=relay` forces SSD <-> HBM transfer through DDR staging.
- `mode=ssd_hbm_direct` explicitly selects the configured logical-direct candidate and reports candidate failures instead of silently hiding them.
- `UF_SSD_HBM_DIRECT_MIN_BYTES` controls the automatic logical-direct threshold; the default is `256MiB`.
- Trace is disabled by default; enable it only when detailed event-level diagnosis is needed.
- Generated run outputs should stay small and summary-oriented before being committed.
