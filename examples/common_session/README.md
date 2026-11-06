# One O1/M1 host session

This example adds orchestration only. The accepted 9d daemon/provider/SDK remains
the sole Catalog and budget/range/lease/content authority. E launches the owner
once with a fresh private root and the accepted binaries, under the 79-symbol
host-only API fence. Native/model processes must not inherit that fence.

The owner creates one real 128MiB Simpler buffer and five actual UF 22MiB
allocations. Index0 belongs to O1; indices1-4 belong to M1. Each allocation has a
2MiB guard and 20MiB payload. The unused 18MiB is still physically charged.
The initializer commits and reads every byte under real W/R leases, then actually
unmaps/closes and acknowledges unregister. Its continued mapping is unnecessary.

`manifest.json` in the public root has the actual descriptor/epoch/dev/inode,
allocation identities and offsets, initial versions/hashes, loaded identities,
host namespaces and control paths. It contains no pointer or admin capability.
Consumers validate the exact descriptor themselves through the actual SDK and
register only the assigned allocation identities; they never allocate another
pool or substitute an independent host buffer.

Add the manifest `helper_root` to `sys.path`. After each actual UF registration:

```python
from session_protocol import request
request(manifest, 'register-client', line='M1', client_session=actual_session)
```

Before heavy build/tests use `resources.host_job(manifest)` (two global slots).
Before measurements use `resources.measurement_window(manifest)` with no UF lease
or directory lock held. Finish real native work within 120 seconds; preserve and
label contention, never interrupt another line's in-flight model. Device use
still requires fresh physical/board/chip/BDF/execution-domain mapping, full FD
visibility/health/HBM and the legal device flock. `reserve-devices` records that
full identity and the actual lock holder FD/birth. The metadata reservation is
not device authorization or proof of native completion. M1 avoids device11.

After the line's real native/result/children/views/FDs have drained and all SDK
unmap ACKs succeeded:

```python
request(manifest, 'line-retired', line='M1', client_sessions=all_registered_sessions)
```

Do not register or start new work for that line afterward. F sends E its real
retirement evidence. E alone runs the bounded local finalizer:

```sh
python sessionctl.py --manifest /exact/p/manifest.json finalize --owner-root /exact/p/session001
```

This only places a fixed finalization request in the original private owner root.
It cannot execute commands or name another pool. Both real line retirements,
every observed client Unmapped, no children/leases/Unknown and unchanged physical
identity are required. Real UF Destroy, provider close/unlink, original daemon
FD disappearance, normal UF shutdown and owner birth exit precede
`finalized.json`. Until then B remains charged128MiB and A's historical128MiB
remains conservatively charged and untouched. Errors preserve the original domain
and evidence; process exit/timeout alone never discharges native responsibility.
