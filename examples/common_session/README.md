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

## Sealed metadata continuation

`session_adopt.py` is a separate, explicitly approved `adopt-existing` mode.
It never calls `Owner.__init__`, bootstrap, pool creation/allocation, or service
startup. It must not be run as an automatic response to a failed claim.
The reserve bug fix keeps the registry path separate from each inspected FD;
real holder birth, FLOCK and physical/execution-domain exclusion remain required.

Before a future handover, the manager seals a bounded capsule containing the
original public manifest hash, all three UF process births, old controller birth
and socket inode/UID/type, all17 actual registered clients and complete line
histories, exact original snapshot/counter312, empty reservations, explicitly
closed failed-task births, new helper Git commit/file hashes, and parent-safety
proof. The latter binds the frozen original host-pool source and the actual
four-process ancestry/FD topology. The separate host-pool daemon loop does not
watch the metadata parent; provider authentication names the unchanged daemon,
and the tracker belongs to the unchanged provider. No controller pipe may be a
UF owner's lifetime dependency. This source/topology proof does not test stopping
the live owner and must precede any separately approved stop.

After the manager separately dispatches handover, the old metadata birth must
already be absent, the three original UF births and backing must still match,
all known task/coordination locks must be free, and the exact original Catalog
resource state must remain unchanged. Only the same-UID, exact stale `control.sock`
inode is archived to the fresh metadata root. `u.sock` is never renamed/removed.
The new server binds the original control address before exclusively publishing
`manifest-v2.json`; actual `describe` returns that new manifest. All24 top-level
fields remain; only manifest version, metadata root/helper identity, controller
birth and a sealed `frozen_inputs.metadata_handover` are updated. The original
`manifest.json`, bootstrap state, UF endpoint/births, descriptor/backing, capacity,
allocation/partition data and both unretired histories remain intact.

The capability is read internally from the original private state file, never
from a capsule, argument or environment and never serialized. The SDK and Python
import closure is checked against the original frozen files. Finalization of
this adopted controller still requires real line/native/UF closure, validated
Destroy ACK, original backing unlink and FD/VMA absence, real Shutdown ACK, all
original UF births gone and the original normal `clean.json`. Its daemon exit
code is explicitly unavailable because it is not a Popen child; no fake wait0.
Both the normal UF cost2 and A's untouched128MiB historical charge remain.

Snapshot observations use the frozen11-field schema. A successful read itself
advances `GetManagedPoolSnapshot` by N and revision by exactly2N (N>=1), without
adding cached operations. Adoption permits only that strict uint64/non-bool
progress; every other counter/key and the full typed resource JSON remain
exact. It preserves the capsule's original snapshot and independently retains
the first verified observation. Publication must positively advance from that
last observation, rejecting replay/rollback even when a query would still be
newer than the capsule. Other control or resource changes stop the handover.

Host tests may exercise this mode against synthetic/recorded metadata and owned
CPU sockets/FLOCK/memfd mappings. They do not authorize live adoption, a new
runtime decision, campaign retry, retire, Destroy or integration.
