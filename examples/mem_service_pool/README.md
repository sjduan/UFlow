# mem_service pool v2

This opt-in backend extends the existing UFlow Catalog and `pool_ranges` with
one real managed allocation shared by two logical Guests on B. Legacy POSIX
pool clients keep their schema1 FD/seal contract. Import the new client
explicitly from `uflow.mem_service_pool_v2`; the existing three fenced SDK
files are unchanged.

The Host brokers metadata only. The Guest client obtains a legal mapping through
the installed neutral SDK, adopts the holder into a mapping owner, pins a bounded
range, accesses every byte, and publishes or invalidates/waits through the actual
provider. UFlow commits a content version only after that terminal receipt.
Host pointers, payload upload and whole-pool V2 reference SEAL are absent.

Only `pool_platform_obmm.c` includes the source-integrated OBMM provider header.
It links the unmodified frozen mem_service implementation in place, following
that repository's `integration-ub-sim.md` contract. This does not extend or
relabel its installed public SDK. The public managed worker supplies home
allocation services; it is not an application mapping substitute.

Before business creation, each Guest's dedicated root endpoint prepares and
exports a 2 MiB canary (4096 visible bytes). Both exports exist before either
peer verify. The Host exchanges only SDK-encoded opaque descriptors and
independently checked checksums. Every application endpoint then verifies the
live opposite Guest canary itself, refreshes the registry and binds only after
the official ready gate. Root readiness cannot certify another endpoint.

The two exports carry 4 MiB of infrastructure responsibility, charged once in
the same Catalog DDR ledger before admitting business allocations. The matrix
uses 64 MiB business + 4 MiB canaries = 68 MiB within the B 128 MiB cap; A's
historical 128 MiB remains unqueried. Temporary verification views and each
application mapping add no physical backing charge. A failed pre-Catalog
bootstrap is recorded as reserved/owned responsibility and fails the job;
no replacement pool or endpoint is created to hide an uncertain outcome.

Application owners unmap/release before their endpoint checked-close. After
business retire, the Catalog still charges the 4 MiB canary baseline. Shutdown
checked-closes both root endpoints and releases each charge only after its real
receipt, then requires zero contexts and zero ledger. The official checked-close
also owns canary export cleanup; a failure retains the original endpoint and
charge. Shared FM files are retained and audited, never unlinked as private SHM.

## Build and run

These are **B-only steps**, prepared for a separately approved execution. A local
portable test pass does not establish Linux compilation or any real mapping.
Use the accepted E0 runner with `kind=build` (at most four workers) for
`tools/phasec/build_mem_service_pool.py`. It checks the exact source manifest,
frozen lower Git pin, installed SDK copies and 53011ee libobmm, builds the Guest
agent and a fail-closed ACL-fenced Host daemon, and produces a source-bound
overlay of the standard legacy initramfs. The fence aborts any ACL entry; it
does not simulate provider data or mapping.

Run `tools/phasec/run_mem_service_pool_matrix.py CONFIG BUILD_RECEIPT` under the
accepted runner's `kind=guest`. Reuse the same Guest lock and both heavy slots.
The matrix creates one private official lower control daemon and one UFlow
Catalog, starts two official Guest managed workers and separate application
processes, and records:

- E1 actual backing/registration plus backend, identity, rights and bounds checks;
- E2 both directions, nonzero offsets, full-byte/hash verification up to 20MiB,
  readonly access and full unchanged guard coverage;
- E3 A/B leases alive while a third independent client completes disjoint I/O,
  overlapping RR, partial RW/WW rejection, adjacency and Writing-read rejection;
- E4 explicit client-reported uncertainty with the original pool/budget held,
  rejected replacement/admission, actual same-context unmap, invalidation of
  uncertain content, surviving clients, final lower retire and upper zero charge.

Guard intervals and their byte total are derived from fixed pool bounds and the
planned write complement. Returned segments only split reads at version
boundaries; a merged/widened segment cannot skip guards. Each guard byte is
checked in the Guest and its checksum is computed independently on the Host.

`PeerIO` limits the entire reply transaction, including incomplete lines, stdin
delivery and EOF, with bounded request/reply sizes. Failure notifies the external
monitor first; stderr is collected independently into a bounded buffer. The
supervisor's short close path owns abnormal process termination. After actual
normal wait, all pipe workers and handles must drain.

The uncertainty case is a **control-plane injection**, not a hardware or SDK
fault result. A separate E0 failure test already bounds Host processes. Any real
runtime failure stays failed/unknown; this driver never opens a replacement
pool to hide it, infers drain from death, or deletes shared FM backing.

For normal shutdown the Guest processes/managed workers drain first, the E0
helper performs QMP quit and actual wait, and the separate accepted audit checks
births, FD/VMA/socket/overlay/FM holders and locks. Guest poweroff is unsupported
on this fixed kernel. A historical A128MiB responsibility is not queried or
cleared. No physical NPU, model, performance or global-zero claim is made.

## Portable checks

Startup is ordered: both root prepare/peer verify, then public SDK directory
registration with the actual boot-bound incarnation/generation/capabilities,
then worker fork and real SDK refresh. The bridge binds refresh to SO_PEERCRED
PID/birth and retains full bounded public metadata requests and replies. Catalog
is created only after both directory entries and both fresh workers are proven.
Every new business admission rechecks the live directory/worker evidence;
cleanup remains possible without reopening admission. Normal shutdown requires
actual worker exit zero and SDK deregister, empty directory, then canary close.

The frozen status response's `data_plane_ready` includes an optional in-process
Host provider registry, which is empty when providers run externally. Its raw
value is retained as `host_registry_data_plane_ready`; it is never rewritten.
Managed readiness is separately derived from the public directory gate plus
actual Guest channel canaries and real worker refreshes. Generic `ready` exit
zero does not establish this condition. Any transport error or non-OK response,
including `store_write_uncertain`, preserves Unknown without an absence-of-side-
effects inference. Historical c013 responsibility is unaffected.

`startup_cpu_probe.py` executes the production C agent/client and installed SDK
against the installed metadata controller with ordinary CPU child workers and
an explicit canary endpoint double. It tests ordering, missing/expired entries,
wrong incarnation/generation, worker exit/missing/stale refresh, bounded logs,
actual SDK deregister/drain, and EOF/truncation/timeout/corrupt/error replies.
The fixture has a relay fence forbidding every business allocation/mapping
operation. It is run only by the B build job and proves no OBMM or Guest result.
Worker stdout/stderr use bounded nonblocking parent forwarding; failure notice
precedes diagnostic serialization and the external supervisor owns escalation.

With `PYTHONPATH=sdk/python`, run `python -B -m unittest discover -s
examples/mem_service_pool` and the existing `examples/managed_pool/test_sdk.py`.
Rust ledger/range/core tests, compilation, old POSIX/Simpler/E10 Linux regression
and the real Guest matrix remain separate evidence requirements. The prepared
execution packet explicitly marks all unexecuted checks pending.

`test_host_revision.py` executes real owned CPU subprocesses for the IO failure
cases and records their waits/pipe-thread closure. Its bootstrap transport is an
explicit recording fixture, not an OBMM result. `test_platform.c` is a separate
B build CPU contract: it uses the actual facade and neutral SDK with explicit
endpoint test doubles to check per-endpoint readiness and failed-close retention.
The Rust regression drives Catalog acquire → Unknown → the shared confirmed-unmap
boundary, checks Invalid content and all lease/access/client closure, and rejects
independently corrupted Catalog state. The existing `Service::catalog` outer
lease synchronization remains; immediate closure no longer depends on it.
