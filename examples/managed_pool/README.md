# UFlow managed DDR pool (host mode)

The host validation fence blocks all 79 selected UF/ACL entries, including the
real backend finalizer. It is test-only and is never a global serving setting.
The frozen vendor shim loader opens exactly three manager controls (235:0) and
one hdc control (509:0) in the daemon without an application initialization call.
The daemon guard verifies this complete role, type, count and inheritance
baseline. Providers, clients and their children must have no Ascend device FDs.
Do not interpret this as zero vendor driver interaction. Provider startup marks
bounded parent-observed nonstdio FDs CLOEXEC in the child before exec; it keeps
the parent's flags unchanged. Only async-signal-safe fcntl calls run after fork.
Inventories are saved before judging and record exe, loaded libraries, fence,
flags and LD_PRELOAD at startup, while resources are held and before cleanup.

UFlow owns a physical pool allocated by the frozen Simpler
`create_host_shared_buffer` factory. The provider process holds the actual Buffer;
the Rust Catalog owns resource identity, the single DDR budget and creation
reservations, client registrations, range leases, content versions and operation
outcomes. E10 and legacy DDR admission use this same physical resource ledger.
E10 retains its owner policy, memfd/seal/read-only adapter and durable witnesses.

`sdk/python/uflow/managed_pool.py` is the public control and mapped-view API. It
imports only the standard library. `_managed_pool_wire.py` implements bounded
strict JSON and FD transport. `pool_provider.py` is the only Simpler owner adapter;
it imports the backend after startup. The mem_service adapter is implemented
separately at this provider boundary in `mem_service_pool.rs` and
`uf-ub-sim-agent/c`; its simulated capabilities do not imply POSIX tensor
addressability. See the [framework baseline](../../docs/shared-pool-framework.md).

The explicit daemon mode starts before ACL/HBM or E10 authority initialization:

```sh
uf-daemon --host-pool --socket "$CASE/u.sock" \
  --pool-state-root "$CASE/state" --pool-provider-python "$PYTHON"
```

`CASE` must be a new, bounded task directory. `state` must not exist. Preserve
failed roots; restarting into a new root does not discharge old obligations.
Pool management uses the private `state/admin.token` capability. Serving clients
receive pool identity and registered FD access, never this capability. Do not
copy the token into source, logs or evidence bundles.

```python
from uflow.managed_pool import ManagedPoolClient

client = ManagedPoolClient.register(endpoint, pool_id, expected_epoch, access='rw')
block = client.allocate_range(20 * 1024 * 1024, alignment=65536)
lease = client.acquire_range(block, 'w', block.generation,
                             byte_offset=0, nbytes=65536)
# Write the mapped bytes in [lease.offset, lease.offset + lease.nbytes).
# Wait for actual CPU/native work AND its result/cleanup before committing.
client.commit_write(lease, content_id, lease.nbytes, operation_id)
```

All intervals are half-open. Only overlapping ranges with at least one writer
conflict; adjacent ranges and shared readers proceed concurrently, including
within one allocation. Writing/Unknown/Invalid content cannot be read. Allocation
generation and content version are separate. A write in a disjoint interval does
not invalidate an existing read lease elsewhere. A read covering multiple
content versions is rejected; publish a complete write to establish one version.
This cooperative API does not provide OS protection between RW mmap subranges.

Keep the registered view and lease through actual native completion. UFlow never
submits or polls native work. `release` of a writer explicitly asserts that no
write started. An uncertain write uses `report_unknown`; a timeout, lost reply
or dead PID is not permission to release, commit, reuse capacity or publish data.
Query the original operation ID. Lost registration ACKs retain the original
client object; do not create another mapping as a retry.

For shutdown, stop new work with `begin_unregister`, drain and detach registered
children, release derived tensors/views, and close the mmap and FD successfully
before `ack_unmapped`. Child detach requires its observed exit plus the caller's
explicit `native_drained=True` attestation. A real mmap `BufferError` keeps the
FD and server obligation. Unknown access can be reconciled only after actual
drain, all children detached and successful unmap/FD close, with explicit
`ack_unmapped(native_drained=True)`. Its unknown written content becomes Invalid,
never Committed. Client exit never automatically discharges unknown exposure.

Unregistering clients leaves the pool allocated and charged, available to another
registered reader. Only management `destroy_pool` after every view, child and
lease drains calls the actual provider close/unlink, confirms the daemon FD
close, and subtracts physical capacity. Release failure retains the charge and
explicit ReleaseUnknown state. A fresh operation may retry a known provider
close failure; query an ambiguous operation first. Durability failure freezes
control outcomes as Unknown and retains all obligations; recovery is not
implemented.

`host_probe.py` runs real independent-process tests against the actual provider.
It uses one 128MiB primary pool with five 20MiB payload allocations at
2/24/46/68/90MiB, all gaps and the 18MiB tail checked in full. An auxiliary
untouched pool tests the shared 256MiB budget; it is not a payload mirror.
The `normal`, `release-fault`, `reply-loss` and `control-timeout` cases require
distinct fresh roots. The negative cases inject real exported-view close failure,
drop one completed commit response, or delay a provider creation long enough for
an actual control timeout while other ranges progress. None executes a model or
native job. `test_sdk.py` is a separate portable unit suite, not provider proof.

The host probe preserves failure logs and unknown identities, never force-kills
processes or resets devices. A failed controller can leave task-owned live
responsibilities requiring explicit investigation. Normal success requires the
actual provider/tracker and daemon to exit, mapped clients to close, pool names
and sockets to disappear, the task lock to be free and the physical ledger zero.
