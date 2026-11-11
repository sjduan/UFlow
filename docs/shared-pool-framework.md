# Shared DDR pool framework baseline

This candidate consolidates the accepted Simpler/A3 shared-pool implementation
and the mem_service CPU-Guest adapter. The delivery checks are **host only**.
Four-card regression belongs to the next A3 task, before sixteen-instance
expansion. A checkout or merge of this version does not imply those tests passed.

## Boundaries

| Layer | Source | Responsibility |
|---|---|---|
| Pool authority | `uf-daemon/{ddr_resources,managed_pool,pool_ranges,pool_service}.rs` | One budget, resource/generation identity, leases, committed content, operation outcomes and retained uncertainty |
| Provider | `pool_provider.rs`, Python `uflow.pool_provider`; `mem_service_pool.rs` and `uf-ub-sim-agent/c` | Actual backing, access handles and native completion; backend waits occur outside the Catalog mutex |
| Client | `sdk/python/uflow/{managed_pool,mem_service_pool_v2}.py` | Registered local mapped view or explicit simulated remote execution; metadata transport never relays KV payload |
| Serving connector | `sjduan/pypto-serving`, `serving/native_ddr/{managed_pool,managed_pool_sdk,shared_pool}.py` | Exact accepted SDK normalization, tensor views, guarded partitions and prefix-cache policy; UFlow remains the authority |

The Simpler provider owns one `create_host_shared_buffer` allocation. Multiple
independent serving instances receive registered views and disjoint partitions
of that allocation. They do not create separate pools behind one pool name.
The mem_service provider uses two logical CPU Guests, actual SDK allocations,
GSVA/OBMM imports and the UBLink data path. It does not claim NPU GM binding or
direct cross-host tensor addressability. Serving's tensor connector currently
uses the Simpler/POSIX mapping path; Guest execution has its own client API.

Unknown remains an obligation. A dead process, supervisor success, source hash,
QMP quit or completed host test cannot manufacture an unmap/native-drain ACK,
refund uncertain capacity or replace SDK data visibility.

## Source and dependency reconstruction

Install the standard-library-only SDK from this checkout:

```sh
python -m pip install --no-deps ./sdk/python
```

`dependencies/locked-sources.json` records public repository/base commits,
exact patches, complete resulting Git trees and changed-file SHA256 values.
The accepted PyPTO `8007f114`, Simpler `508becb9` and ub_sim `c1e73ed2`
commits were local-only at delivery. Their public bases and patches are the
fetchable source of truth; those local SHAs are provenance, not fetch commands.
The QEMU patch is flattened onto public `f2ef54f4` and matches all eleven
accepted managed-write profile sources, avoiding a dependency on private build
directories. See [dependency instructions](../dependencies/README.md).

The helper performs no download or build. It requires a clean exact base,
checks the patch checksum, applies to the worktree **and index**, verifies the
resulting tree, and preserves unrelated edits by refusing the operation.
Repeated `--check` is read-only apart from Git's normal object verification.
Prepare each locked submodule separately; a parent's gitlink is not evidence
that its nested working tree has the matching source.

## Build and host checks

For CPU checks on Linux with GCC, Cargo and Python 3.11:

```sh
cargo fetch --locked
python -B tools/check_framework_host.py /tmp/uflow-host-check-unique
python -B tools/test_locked_source.py
```

Use a new output directory. The check builds an ACL API fence, uses at most
four Cargo jobs and offline locked dependencies, builds the ordinary daemon,
tests the affected pool contracts/SDK, and compiles the experimental adapter
without executing historical recovery. Calling any fenced ACL function exits
the test process with code 86. These checks start no device, Simpler Worker or
Guest and make no claim about native performance.

For a real A3 deployment, build the existing native ACL shim with the selected
CANN environment, then build the same Rust source:

```sh
cmake -S . -B build -DASCEND_HOME_PATH="$ASCEND_HOME_PATH"
cmake --build build --target uf_acl_shim -j4
UF_ACL_SHIM_LIB_DIR="$PWD/build/lib" cargo build --locked -p uf-daemon -j4
```

The CPU fence must never be substituted for the deployment shim. Runtime library
paths and physical device reservations belong to that deployment's launcher.

## Entry points and lifecycle

The ordinary host-pool entry runs before ACL/HBM initialization. Choose exactly
one provider, use a fresh task root, and preserve failed roots:

```sh
uf-daemon --host-pool --socket "$CASE/pool.sock" \
  --pool-state-root "$CASE/state" --pool-provider-python "$PYTHON"
# Or select an explicit simulated mem_service configuration:
uf-daemon --host-pool --socket "$CASE/pool.sock" \
  --pool-state-root "$CASE/state" --pool-mem-service-config "$CASE/provider.json"
```

See [managed pool](../examples/managed_pool/README.md) for the control/view API.
`--pool-mem-service-config` requires schema 2, `mem_service_obmm`, an explicit
`simulated:` realm, unique nodeA/nodeB identities, absolute task endpoints and
the real readiness/canary/worker receipts. Placeholder receipts do not authorize
a pool. Admin capabilities stay private to the pool owner and are never sent
to serving clients or committed with configuration.

New native/serving launches must use the generic bounded supervisor delivered
in Serving under `pypto_serving/serving/native_ddr/task_supervisor.py`, or a
verified equivalent. It establishes ownership before opening the task gate,
receives failure notices, then applies finite close/TERM/KILL/reap to its own
proven descendants. Process closure and logical DDR closure are separate
results. No GDB/ptrace injection, broad process-name kill, driver unload, host
restart or device reset is part of the launcher contract.

## Experimental history

The normal daemon excludes task-bound reconciliation modules and refuses
`--pool-terminal-*`, `--pool-shutdown-report` and historical execution contexts
before reading their witnesses or invoking producers. The explicit feature is:

```sh
cargo build --locked -p uf-daemon --features experimental-reconciliation
```

Enabling it retains the original proof, identity, Unknown and authority checks.
It is for historical reproduction under its original contract, not a generic
restart, a current claim, or permission to mutate old responsibility. Recorded
fixtures under `examples/mem_service_pool` are cold experimental evidence, not
portable deployment configuration. Their historical paths/seals are deliberate.

## Evidence and remaining limits

Historical tested baselines: UFlow M1 `8eac5c22`, Serving M1 `1f96d877`,
UFlow Guest adapter `16f7ea6d`, ub_sim `c1e73ed2`. The actual two-Guest core
validation transferred 20 MiB A→B in 59.088300291 seconds and B→A in
62.506758941 seconds with complete byte checks. Those are historical results
on that exact stack, not measurements of this newly consolidated candidate.

An extra read-only client importing an already imported object in one Guest
failed with GSVA overlap/`KEY_MISMATCH`; its final normal drain was not proven.
Wait for ub_sim support/defined alias lifetime before a targeted retest.
Do not interpret core data PASS as complete multi-client, shutdown or recovery
coverage. Existing retained files, Unknown and conservative accounting remain
unchanged. Unvalidated C `32094cf` and six E WIP files are excluded.

The next A3 task must freeze both merged repository commits plus this dependency
lock, perform four-card serving regression under a legal device reservation,
and only then expand to sixteen instances. This delivery starts no such task.
