# UFlow A5 supernode development baseline

UFlow exposes one control plane for data objects, placements, resource and access bindings, leases, and transfer plans and completion events. This branch builds the standalone UFlow service for A5 development. The shared contract leaves data movement with the provider that owns the memory or storage.

The runtime includes local HBM through the CANN ACL shim, local DDR through Linux mappings, and local SSD through file extents. The managed range and operation accounting code remains available for future backends. The existing simulator adapter requires explicit selection and has its own source lock. See [provider and validation boundaries](docs/a5-supernode-native.md).

## Linux host checks

Use Python 3.11, a current stable Rust toolchain, and GCC. Select a new output path.

```sh
cargo fetch --locked
python3 -B tools/check_framework_host.py /tmp/uflow-a5-host-check
python3 -B tools/test_locked_source.py
```

The host check links an ACL fence, verifies the native and simulator CLI boundaries, and runs shared protocol and lifecycle tests. It starts no NPU or simulator job. The branch workflow runs the same host checks.

## Build for an A5 host

Use the A5 machine's installed CANN environment. Confirm the toolkit path before building.

```sh
export ASCEND_HOME_PATH=/usr/local/Ascend/cann-9.3.0
. "$ASCEND_HOME_PATH/set_env.sh"
cmake -S . -B build -DASCEND_HOME_PATH="$ASCEND_HOME_PATH"
cmake --build build --target uf_acl_shim -j4
UF_ACL_SHIM_LIB_DIR="$PWD/build/lib" cargo build --release --locked -p uf-daemon -j4
```

For a device run, use a fresh task socket and the assigned device. Prepare and build before submitting the device command. The queue supplies `TASK_DEVICE`.

```sh
case_dir=$(mktemp -d /tmp/uflow-a5.XXXXXX)
export LD_LIBRARY_PATH="$PWD/build/lib:${LD_LIBRARY_PATH:-}"
task-submit --device auto --device-num 1 --max-time 300 \
  --env "UF_SOCKET=$case_dir/u.sock" --env LD_LIBRARY_PATH --run \
  'exec ./target/release/uf-daemon --device "$TASK_DEVICE" --socket "$UF_SOCKET"'
```

The Python SDK is in `sdk/python`. Set `UF_SOCKET` to the matching daemon socket. For calls that need ACL, set `UF_ACL_LIB` to the built shim. The monitor defaults to the local `/tmp/uflow.sock` and accepts an explicit `UF_SOCKET`.

The local SSD object and DDR/SSD transfer probes are in `examples/host`. They use small host buffers by default and can use a task-owned daemon endpoint with `--socket`.

The published UFlow `main` and existing A3 task branches preserve the earlier A3 adapters, campaign scripts, and evidence. The A5 branch has its own build, host checks, and runtime instructions.
