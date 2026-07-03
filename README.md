# UFlow

UFlow is an experimental unified data service for model-serving and heterogeneous-memory workloads.

The current focus is local-node data management for Ascend NPU systems:

- manage HBM and DDR data objects through a service-side daemon;
- expose a small client SDK for object lifecycle, leases, transfer planning, and transfer events;
- move data through UFlow-owned runtime views instead of leaving all data movement invisible inside client processes;
- benchmark and optimize DDR <-> HBM hot paths for future PyPTO / pypto-serving integration.

The long-term direction is a unified data layer for future supernode-style systems, where HBM, DDR, SSD/SSU, and remote memory can be represented by the same object / placement / transfer language.

## Related Repositories

- [LL-mixed/ub_sim](https://github.com/LL-mixed/ub_sim): UB data-flow simulation and validation environment.
- [hw-native-sys/pypto-serving](https://github.com/hw-native-sys/pypto-serving): model-serving framework that UFlow is designed to integrate with.
- [hw-native-sys/pypto](https://github.com/hw-native-sys/pypto): high-performance AI accelerator programming framework used by the PyPTO runtime stack.

## Current Status

Implemented pieces include:

- Rust workspace for UFlow core types, ACL FFI bindings, and daemon runtime.
- C++ ACL shim for HBM allocation, shareable handles, host memory, stream/event, and memcpy operations.
- Python SDK for clients.
- DataObject / Placement / Lease lifecycle.
- TransferPlan / TransferEvent API.
- DDR object support based on memfd or mmap-backed files.
- Service-owned HBM and DDR views, allowing daemon-side HBM <-> DDR transfers.
- Default direct DDR <-> HBM path using THP/pre-touch DDR and ACL async memcpy.
- Pinned staging channel as an explicit fallback path.
- Monitor API and lightweight web UI.
- Trace and benchmark utilities for transfer-stage analysis.

Recent validated hot-path numbers on a local NPU test node:

| Direction | Size | Channel Bandwidth |
|---|---:|---:|
| DDR -> HBM | 2 GiB | about 57 GiB/s |
| HBM -> DDR | 2 GiB | about 41 GiB/s |

These numbers are machine and CANN-version dependent. See `results/` for captured benchmark summaries.

## Repository Layout

```text
crates/
  uf-core/        Shared IDL and core Rust types.
  uf-acl-sys/     Rust FFI bindings to the native ACL shim.
  uf-daemon/      UFlow daemon, catalog, transfer planner/executor, stats, trace.

native/
  acl_shim/       C/C++ ABI layer around CANN ACL runtime calls.

sdk/python/
  uflow/          Python client SDK and data object wrappers.

examples/
  phase*/         Smoke tests, benchmark drivers, and integration probes.

experiments/
  acl_hbm_smoke/  Standalone ACL validation and bandwidth probes.

tools/
  phasee04/       Local monitor/tunnel helper scripts.
  uflow_monitor_api.py

ui/
  uflow-monitor/  Static monitor UI.

plan/
  Public architecture notes and reports.

results/
  Selected benchmark summaries and trace artifacts.
```

Internal PhaseA / PhaseE planning documents are intentionally ignored in `.gitignore` and are not meant to be published from this repository.

## Build Sketch

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

## Runtime Sketch

Start a local daemon:

```bash
./target/release/uf-daemon \
  --device "${UF_TARGET_DEVICE:-0}" \
  --socket "${UF_SOCKET:-/tmp/uflow.sock}"
```

Run client examples with the same socket:

```bash
PYTHONPATH="$PWD/sdk/python" \
UF_SOCKET=/tmp/uflow.sock \
python3 examples/phasee01/uflow_e01_unified_transfer_smoke.py
```

Remote monitor helpers intentionally do not store personal hostnames, SSH users, or key paths. Configure them with environment variables such as:

```bash
export UFLOW_HOST1_SSH="<jump-user>@<jump-host>"
export UFLOW_HOST1_KEY="<local-private-key-path>"
export UFLOW_HOST2_SSH="<remote-user>@<remote-host>"
export UFLOW_HOST2_KEY_ON_HOST1="<private-key-path-on-jump-host>"
```

## Development Notes

- UFlow daemon owns the authoritative transfer path. Clients may receive handles or leases for compute access, but planned data movement should go through UFlow APIs when observability and scheduling matter.
- `mode=auto` selects the service-side default transfer strategy.
- `mode=pinned_async` keeps the pinned staging channel available as an explicit fallback and comparison path.
- Trace is disabled by default; enable it only when detailed event-level diagnosis is needed.
- Generated run outputs should stay small and summary-oriented before being committed.

## Safety

Do not commit private SSH keys, real hostnames, internal IP addresses, access tokens, or machine-local absolute paths. Use placeholders or environment variables in scripts and documentation.
