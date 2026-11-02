# Cooperative external DDR

This guide describes the external-owner DDR path on a single Linux host. An
application owns and populates a real `memfd` backing; the daemon registers the
backing and mediates cooperative read-only views and explicit copies. It does
not take ownership of an arbitrary `malloc` pointer or create the owner's data.
Registration passes a file descriptor using `SCM_RIGHTS`; a reader receives a
read-only FD and `mmap`. A mapping is not a copy, a new placement, or additional
capacity. Retained views and outstanding work have independent lifetimes.

## Requirements and trust boundary

- Use Linux with `memfd`, FD passing and `mmap`, a supported UFlow deployment,
  and the daemon control socket for that deployment. The light external DDR
  Python SDK uses the standard library and does not need `torch`, `torch_npu`
  or serving. The daemon still requires its real ACL shim and CANN libraries;
  simulated execution settings do not make it a CPU-only daemon.
- Peers cooperate under the same UID and honor the immutable-backing and
  exclusive-writer protocols. The FD covers the entire backing. Range checks
  and read-only mappings do not provide OS-enforced subrange isolation or
  security against malicious same-UID tenants.
- External DDR uses a managed authority with a fixed deployment root, trusted
  host identity, endpoint and limits. Only the authorized operator should
  initialize or operate it. A new socket or changed directory is not a way to
  bypass existing authority state. Do not delete a marker, clear history or
  substitute a new root to make a dirty domain appear clean.
- Discovery is observational and its budgets are advisory; see the
  [discovery documentation](../../crates/uf-core/src/topology/README.md).
  Unknown is not a zero cost or proof of reachability. The current explicit
  cost response reports `cost_version=unknown` and
  `automatic_ranking=disabled`; this is not a complete cost model or a claim
  of optimal ranking, validated p90, bandwidth or performance.

## Import the light SDK

From the repository root, the following checks the normal package entry in a
new, clean virtual environment. Set `VENV_DIR` to a new path first.

```sh
python3 -m venv --without-pip "$VENV_DIR"
PYTHONPATH="$PWD/sdk/python" "$VENV_DIR/bin/python" examples/ddr_v0/test_t34_sdk.py
```

Application code uses `from uflow import ExternalDdrClient`. Construct
`ExternalDdrClient(control_socket)`, passing the daemon's control socket, not
the `.external-ddr` suffix: the client appends it. A missing or quarantined
broker must be treated as unavailable, not as zero commitments.

## Register and share immutable data

The following is an owner-side fragment. `control_socket` is supplied by the
trusted deployment. Keep `fd` and the backing alive for the entire registration
and for any outstanding views or work.

```python
import fcntl
import os
from dataclasses import asdict
from uflow import ExternalDdrClient

client = ExternalDdrClient(control_socket)
payload = b"cooperative external DDR"
fd = os.memfd_create("owner-data", os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
os.ftruncate(fd, len(payload))
assert os.pwrite(fd, payload, 0) == len(payload)
fcntl.fcntl(fd, fcntl.F_ADD_SEALS,
            fcntl.F_SEAL_GROW | fcntl.F_SEAL_SHRINK | fcntl.F_SEAL_WRITE)
handle = client.register(fd, name="owner-data", owner_name="owner-a",
                         size_bytes=len(payload))
# Exchange asdict(handle) over your authenticated, cooperating application channel.
```

A second independent owner may register its own backing and both owners may
exchange the complete handle fields. The reader reconstructs the full handle,
not just an object number. For example, in the other owner's process:

```python
from uflow import ExternalBackingHandle, ExternalDdrClient

reader = ExternalDdrClient(control_socket)
other = ExternalBackingHandle(**received_handle_fields)
view = reader.acquire(other, offset_bytes=0, nbytes=other.size_bytes)
child = view.retain(0, other.size_bytes)
view.release()                  # closes this mapping before remote release
contents = child.read()        # retained view remains valid
child.release()                # release the derived mapping too
```

Handle exchange and both processes' lifetime coordination are the application's
responsibility. Do not modify immutable bytes while they may be read. Each owner
releases all its views, waits for work to actually drain, revokes its own backing
with `client.revoke(handle)`, and closes its own FD only when it is safe to do
so. `release()` unmaps first and reports an unconfirmed remote release as an
error; do not treat an ambiguous result as successful cleanup.

## Explicit copy, event and statistics

The requester must have an active immutable registration anchoring its pool.
The target owner creates a separate memfd, sizes and populates it, seals against
growth and shrink, then calls `client.stage(fd, name=..., owner_name=...,
size_bytes=...)`. A staging backing is an exclusive writer handoff: remove all
writable mappings before submission and do not write the FD until work has
actually drained. The source is an immutable registered handle, possibly shared
by the other owner. Preserve and exchange the complete handles.

The following fragment runs in the target/requester process with valid `source`
and `target` handles and matching ranges. It shows the public methods, not a
standalone provisioning script.

```python
client = ExternalDdrClient(control_socket, timeout=30.0)
ranges = dict(src_offset_bytes=31, dst_offset_bytes=173, nbytes=4096)
estimate = client.estimate(source, target, **ranges)
plan = client.plan(source, target, **ranges)
submitted = client.submit(int(plan["plan_id"]), plan_token=plan["plan_token"])
event_id = int(submitted["event_id"])
event_token = submitted["event_token"]
current = client.poll(event_id, event_token=event_token)
# To request cancellation instead: client.cancel(event_id, event_token=event_token)
done = client.wait(event_id, event_token=event_token, timeout_ms=20000)
if done["drained"] != "true" or done["event_status"] != "complete":
    raise RuntimeError("copy did not complete and drain")
with client.acquire(target, offset_bytes=173, nbytes=4096) as copied:
    copied_bytes = copied.read()
```

`estimate` is informational; `plan` performs an explicit probe and binds the
plan to observed identity and version. A stale submit is rejected and requires
a fresh plan; do not infer permission for arbitrary retries. A cancellation
acknowledgement is only a request, not proof that a worker has drained. A wait
can time out or fail: keep the backing, FD and other obligations alive until a
real drained terminal event is confirmed, or treat the state as unknown and
follow the deployment's conservative failure procedure. Failed or cancelled
work is not Ready. On success, compare the actual bytes and any untouched target
region against the expected source, rather than treating a plan or event as
proof of content.

Event responses include status, `drained`, mapped/accessed/copied bytes and
planned/actual path information. `client.snapshot()` provides the broker
projection and obligation counters; the daemon control `GetStats` operation and
trace facility expose aggregate mapped/accessed/copied and transfer information.
Do not count mapping or access as a copy, or a requested byte count as completed
work. For normal shutdown, stop admitting work, confirm all workers joined and
all views and backing obligations released, then request `ShutdownDaemon` on
the control socket and verify the daemon actually exits cleanly. A request or
socket disconnect alone is not a clean checkpoint.

## Reproducing the existing scenarios safely

The normal public scenario is [`t34_public_probe.py`](t34_public_probe.py).
Its outer mode creates a new run-owned private mount namespace and checks the
namespace and mounted directory identity before bootstrapping a private authority.
It starts real independent SDK peers, checks read-only sharing, real bytes,
copy events, statistics and clean restart, and preserves its results. Use a
separately provisioned sandbox with mount-namespace privileges and the real
shim/CANN dependencies. `RUN_PARENT` must be an existing canonical task-owned
parent under the script's `BASE`; the daemon and shim must both be files inside
that parent, and `public-check` must not exist. With those prerequisites:

```sh
python3 examples/ddr_v0/t34_public_probe.py \
  --run-dir "$RUN_PARENT/public-check" \
  --daemon "$RUN_PARENT/uf-daemon" \
  --shim "$RUN_PARENT/libuf_acl_shim.so"
```

Do not call an inner mode directly or run a fixed-root acceptance program
against an existing real authority. The private namespace must be verified
before any daemon or bootstrap is started; if isolation is unavailable, stop.
[`d2_g_acceptance.py`](d2_g_acceptance.py) contains fixed deployment defaults
and is not a safe generic demo. Do not invoke it against an existing domain or
assume that changing a socket isolates the authority. Fixture and regression
programs are not evidence of production recovery.

## Failure and scope

A normal clean restart is distinct from dirty or unknown authority state. On a
failure, stop new admissions; old broker/process identity is poisoned for new
work, while an already legal mapping can remain readable until its holder
unmaps it. Preserve the persistent domain and outstanding obligations; there is
no dirty-to-clean recovery procedure in this path. Do not infer clean state from
holder exit, a cancel reply, an empty catalog or a changed endpoint.

This guide describes the cooperative external DDR scope. Existing HBM, SSD,
serving and other repository features are separate from this path. This release
does not establish new HBM/SSD/serving, A5, multi-host, physical power-loss,
full recovery, full cost-model or performance acceptance.
