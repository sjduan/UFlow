# mem_service pool v2

## Current HOST candidate: closed-c033 quarantine76 and48 MiB matrix

The c033 run inherited retained8, started both Guests/workers and actually
reserved a64 MiB GSVA segment before export failed. Its frozen Image disabled
`CONFIG_UB_UMMU_SVA_SEPARATED_PAGES`; the corresponding kernel header function
returned -EINVAL. That is CreationUnknown, not proof of absence or release.
Real E1-E5 acceptance remains incomplete. c033/ca033 cannot be repeated.

The new candidate retains six historical canaries (typed12 MiB) and the original
attempted creation reservation (typed64 MiB). New4 MiB canaries and48 MiB business
give B peak128 MiB; A historical unqueried128 gives global256. A new64 MiB request
must be rejected. Normal current close ends charged=retained12, total reserved=
retained reserved64, active and new reserved0: B still76 MiB, never0/8/12 total.

Only the fixed closed c033 pure-CPU context may enter `pool_quarantine76`.
The pinned assessment binds33 original records, all six canaries and the full
original64 reservation key/pool/op/epoch/session. Its embedded verifier checks
complete original and retained8 lineage, exact old births, quiet controls,
bounded FD/VMA/FM holders, actual frozen E0 ancestor kernel FLOCK admissions
and a fresh pure QEMU namespace. After new FM creation a further check requires
all old30 identities retained and all new10 inode aliases disjoint, before any
canary work. No production verifier or actual lineage is run by HOST fixtures.

The original lock stays held for the whole Service. Only `successor-c033-v1`
is appended by fsynced no-replace intent/result; the inherited76 state is
fsynced before new reservations. Partial writes, drift, concurrency, re-entry
and restart reject. The old SDK/store/Guest is neither replayed nor revived.
CreationUnknown query returns original Unknown as inaccessible history. Old
pool/op, resources100–107 and all three generations of boot/realm/session
identities cannot be reused. Typed uncertainty blocks ordinary cancel/commit/
backing conversion/release; shutdown reports it. New live Unknown still refuses
admission through the default guard. No canonical backing or ACK is invented.

`kernel_profile_gate.py` runs before lower/Catalog/lineage/Guest. Its explicit
opt-in `kernel_profile` binds the reviewed `manifest`/`manifest_sha256` and
`validator`/`validator_sha256` at the fixed additional release. All four values
must equal the externally reviewed pins. The gate reads an actual regular
validator file, hashes it before compiling, and executes those exact bytes
without re-importing its path. The reviewed public API validates the actual
Image/embedded IKCONFIG, builtin delta, source Git object manifest/ls-tree,
compiler/build commands/recipe, objects and linked Matt/OBMM call evidence.
It consumes producer output keys `source_manifest`/`source_ls_tree`; the source
closure is kernel_commit/tree/tracked_objects/git_ls_tree_sha256/source_root.
No synthetic files array or ready flag is manufactured. Old default Image18f
is checked unchanged; selected `config.kernel` must be the same reviewed95165f
Image returned by the validator. Missing, malformed, duplicate, truncated,
oversized or mismatched profile/hash/path input rejects before runtime state.
`profile_consumer_cpu.py` exercises this actual release, private malformed
negative fixtures, and failed-gate/no-Popen/no-Catalog ordering on B. Host
capability success remains `runtime_tested=false` and grants no boot authority.

`matrix_entry_cpu.py` runs the complete actual matrix/callback through real
Rust/C/SDK sockets, independent client processes and a48 MiB CPU shared-file
mmap provider. Import/canaries/platform/worker/QEMU/Image are explicit doubles
linked only into CPU fixtures. It verifies76→80→128→76, two full20 MiB transfers
in opposite directions at nonzero offsets, every byte/hash, independent full
complement guards and A/B live leases while C completes real I/O. It also
checks identity negatives, negative64, new live Unknown refusal and actual
same-context current cleanup. It never claims real Guest/OBMM/64 MiB PASS.
The third probe starts44 MiB+128 KiB and E3 starts46 MiB, within48 MiB.

The private PID1 launcher captures only fixed `worker.conf.state` metadata,
owned regular/nofollow/nlink1, at most64 KiB and1s within the existing finite
handoff. Failure notice precedes diagnostics. Raw serial chunks can be
reconstructed, checked by FNV and SHA256, or are marked missing/error/truncated/
incomplete. Linux CPU tests cover symlink/hardlink/FIFO/oversize/blocked-output
and actual child cleanup. No arbitrary path, secret/FM/payload read, frozen
worker change, monitor extension or c033 lost-journal reconstruction occurs.

## Historical implementation notes through9ca

The following describes predecessor stages, their old64 MiB matrix and earlier
CPU fixtures. Current behavior and responsibility are defined above; old
snapshots are preserved as evidence, not current execution permission or PASS.

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
prepares 64 MiB business + 4 MiB current canaries + 8 MiB retained predecessor canaries
=76 MiB within the B 128 MiB cap; A's
historical 128 MiB remains unqueried. Temporary verification views and each
application mapping add no physical backing charge. A failed current
bootstrap is recorded as reserved/owned responsibility and fails the job;
no replacement pool or endpoint is created to hide an uncertain outcome.

Application owners unmap/release before their endpoint checked-close. After
business retire, the Catalog still charges the 12 MiB current/retained canary baseline. Shutdown
checked-closes both root endpoints and releases each charge only after its real
receipt, then requires zero current contexts and active/reserved capacity, while
the four predecessor UnknownCanaryClose entries retain 8 MiB. The official checked-close
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

Startup is ordered: both root prepare/peer verify, official platform GVA
aperture registry/empty inventory preflight, then public SDK directory
registration with the actual boot-bound incarnation/generation/capabilities,
then worker fork and real SDK refresh. The bridge binds refresh to SO_PEERCRED
PID/birth and retains full bounded public metadata requests and replies. Catalog
admits business only after both directory entries and both fresh workers are proven.
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

The historical c013 continuation started its single responsibility Catalog before any new
Guest/bootstrap. An embedded verifier checks the pinned manager assessment and
all original source/controller/SDK/image bindings, exact snapshot/store/journal/
events, IDs/actor, absence of business objects and nonlive exact births in place
on B. Original files are read only and are not exported by the verifier. The
Catalog restores 68MiB, fsyncs an intent, uses `cancel_uncreated` only for the
proved pre-dispatch 64MiB reservation, and fsyncs the result. Original Unknown
response/IDs remain terminal history. Two typed Provider canaries retain 4MiB
as `UnknownCanaryClose` in the same DdrResources and public resource snapshot.
No SDK close or Unmapped is claimed for them.

That historical run retained its two new, actually prepared canaries after a
worker startup failure, adding 4MiB without checked-close. The current matrix
requires a narrow conservative c027 successor: the embedded retained8 verifier
pins assessment b283ec29 and revalidates the original proof, fourteen exact
predecessor files, completed cancel64 history, both serial/boot/receipt sets and
empty store family. It also binds original FM metadata, checks both generations'
actual births, all control endpoints and bounded FD/VMA/FM holders. It never
replays descriptors or infers an SDK ACK. The original lineage lock is held
through the entire successor Service. A new exclusive append-only
`successor-c027-v1` contains fsynced intent/result; link-based publication refuses
overwrite. Existing successors, half writes, drift or restart are refused.
E0 already acquires the Guest execution lock before launching this matrix.
The verifier authenticates the fixed E0 ancestor command/source, exact birth,
current job/state root and its actual kernel FD-bound exclusive Guest and both
heavy locks; it does not try to reacquire an ancestor's lock or trust a caller
boolean. CPU tests use only three new fixture locks and never open real
coordination/lineage locks.

The four original BackingIds enter the same DdrResources as typed
UnknownCanaryClose objects100/101/103/104, charged/retained8 and reserved0.
No cancel64 is executed again. Resource IDs start above104; two generations'
old boot/incarnation/realm/session identities cannot be reused. The authority
then reserves the two current canaries before their real prepare.
After real bootstrap/register/worker readiness, authenticated
`ActivateManagedBootstrap` commits those reservations with actual boot-bound
backings. Business is refused until activation. Final normal close releases
only current resources: active0/reserved0/retained8MiB. The planned B peak is
76MiB under128, plus unqueried A historical128MiB gives204MiB. This succession
has only been exercised in isolated CPU fixtures; production restoration,
Guest/aperture/OBMM and E1-E5 require their separate actual execution evidence.
SDK3 and default POSIX zero-account close stay unchanged.

`matrix_entry_cpu.py` invokes the actual matrix and callback with real Rust/C/SDK
sockets, explicit import/canary/worker/QEMU doubles, and intercepts business
creation before allocation. It checks retained8 -> new4 reserved -> active12 ->
normal retained8, Catalog routing after both bridges, and old terminal identity
rejection. No production verifier or actual old lineage is opened by this test.
The task overlay PID1 launcher gives the Host monitor/QMP a finite30s handoff
window after bounded child-group TERM/KILL/wait; expiry may still panic PID1 if
the external handoff fails. CPU process tests do not certify real PID1/QMP.

The build now consumes the official frozen managed-worker Makefile in a private
output directory, with546/53011 and locked platform/GVA sources. It records the
expanded recipe, all compiler source/header/link inputs and ELF/image member
hashes, and overlays that binary. It never modifies the official source, SDK53,
base image, dependency versions or recipes.

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
