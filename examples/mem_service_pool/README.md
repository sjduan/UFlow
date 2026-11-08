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

With `PYTHONPATH=sdk/python`, run `python -B -m unittest discover -s
examples/mem_service_pool` and the existing `examples/managed_pool/test_sdk.py`.
Rust ledger/range/core tests, compilation, old POSIX/Simpler/E10 Linux regression
and the real Guest matrix remain separate evidence requirements. The prepared
execution packet explicitly marks all unexecuted checks pending.
