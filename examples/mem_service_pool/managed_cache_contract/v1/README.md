# UFlow Catalog-managed range-read cache v1

This is the opt-in model selected by the manager's
`C-review-c081-boundary/managed-lease-cache-decision-v1.md`. A real executing
Catalog R lease and a retained server obligation are its authority. SDK
READ_ACQUIRE/FENCE, a caller boolean and a digest alone cannot mint a grant.
Unmanaged bare CPU/PTO stores are outside this model. Without an active grant,
the original strict uncached CPU-window path remains. All payload comes from
real Guest GSVA/UBLink/scatter-page I/O; no host payload/checksum relay.

## Source-defined metadata control

The new QEMU profile adds construction property `ubc.uflow-managed-cache-v1`
(default false), selected only by the new reviewed group/profile using exactly
`-global ubc.uflow-managed-cache-v1=on`. An enabled single UBC exposes the string
property `uflow-managed-cache-control-v1` as an alias on **`/machine`**. Old
profiles/05c2 lifecycle wrappers remain unchanged. Only the new wrapper may
send these exact qom-set/qom-get operations, never arbitrary QMP/QOM/memory/FD
commands. Set/get is one serialized transaction over the existing peer-verified
QMP channel, with the original five-second metadata deadline.

The concrete Python codec is `uflow_managed_cache_codec_v1.py`. Wire is exactly
`umc1:` plus **440 lowercase hex characters**, encoding this 220-byte big-endian
record. No trailing fields, payload, token_value or native pointer is permitted:

| Order | Field | Wire type |
|---|---|---|
| 1 | magic UMC1, operation probe=1/begin=2/end=3/status=4, three reserved zero bytes | 4s,u8,3x |
| 2 | control_generation (one-use, nonzero for begin/end) | u64 |
| 3 | authority_sha256, correlation_sha256, descriptor_sha256 | three 32-byte digests |
| 4 | map_id, lease_epoch, export_mem_id, remote_uba, size, segment_id, epoch, offset, length | nine u64 |
| 5 | backing_token_id, home_cna, gsva_token_id, cache_policy, p_tag, access_flags, owner_node, node_count, segment_flags | nine u32 |

`lease_epoch` is the observed **native GSVA token lease epoch**; it is distinct
from Catalog lease_generation. The latter is required in the server authority
envelope and therefore in authority_sha256. Offset/length are the exact native
resource-relative bytes of the authorized R operation, clipped within size.
One active grant per QEMU/UBC avoids attributing a CPU callback to an ambiguous
overlapping lease. C must serialize begin→Guest operation→end on that node's
existing exchange lock. Catalog R leases may still coexist; this node control
restriction serializes actual execution, not lease admission.

The server-minted canonical authority hashes the real pool/epoch/provider
session, backing/resource generation, client/access generation, lease ID and
lease_generation, block identity/generation, offset/length/read mode, committed
content ID/version and operation/control generation. Existing authenticated
frame node/boot/session/incarnation/correlation is retained; its binding must
also cover the authority. Bridge authenticates the direct daemon UnixStream
using SO_PEERCRED plus its real PID/birth/UID/namespace/executable. The server,
not forwarded client JSON, constructs this envelope and retains its obligation
until an exact end ACK. The codec itself grants no authority.

## Binding the real native route

`descriptor_sha256` is SHA256 of the original **192-byte lowercase hex text**
of the native 96-byte MSOB-v2 descriptor, matching the existing E safe-parser
opaque digest. Only documented public slices through offset88 are decoded.
Offset92/token_value is never decoded/exported by the host codec. The native
secret remains in the real QEMU route/home binding and strict remote-I/O path.

Probe uses the actual opaque descriptor digest/public fields, map_id=0 and
lease_epoch=0, and does not create a grant. QEMU resolves a unique active strict
GSVA route by complete key/home/backing-token identity, validates its token
internally and reconstructs the descriptor digest internally. On the home
node it also resolves the exact registered export ID via gsva_home_resolve_export.
QEMU returns its actual map_id/token lease_epoch and public native alias. C
compares both peer-verified home and importer aliases against the real provider
descriptor and the actual QEMU PID/birth/accepted executable profile. It must
not infer an alias from an expected address/pattern or naked map_id. Begin/end
use those actual nonzero map/lease-epoch values. Every miss still uses the full
strict native identity at the real home; no legacy dir.data fallback.

## Replies and lifecycle

Each qom-get returns bounded JSON (≤4096 ASCII bytes) with version1,
feature `uflow-managed-lease-cache-v1`, actual QEMU pid, operation,
request_sha256 (SHA256 of the **220 binary bytes**), decimal-string
control_generation, all three exact request digests, code
`ok|busy|denied|disabled|replay|full`, active boolean, in_flight and cache_pages,
and native alias/counters where available. The wrapper validates exact
request/authority/correlation/native binding. No payload/secret is returned.

Begin must ACK active, zero in_flight and zero pages **before** SDK access.
The cache starts empty and binds the whole native identity snapshot, actual
authority/generation/correlation and native range. Native epoch/token/map
changes deny access. READ_ACQUIRE/FENCE and INV conservatively invalidate
entries/fills; they do not mint or finish a grant. An epoch counter is checked
before/after strict remote fills. Late failed/revoked/invalidated fills are
discarded. The cache is bounded private simulator overhead (8MiB quota), with
bounded eviction; it is not a DDR allocation or authoritative backing.

End first disables new cache access, invalidates entries and drains any fill.
Busy is not an ACK. Only actual inactive/zero in_flight/zero pages yields ok.
The matching repeated end is idempotent; old begin/operation/control generations
are fenced. A bounded exhausted replay history refuses new grants. Process
death, timeout, EOF, birth change and uncertain ACK leave a Catalog Unknown
obligation and freeze conflicting actions; they do not count as end.

All managed overlapping W acquisition/execution, lease release, unmap and close
remain forbidden while a grant/revoke/fill obligation exists. New grants after
end→managed W/publish start empty and fetch fresh real bytes. The QEMU surface
also rejects overlapping CPU-window W/native close while a grant is live;
this is defense in depth, not a claim about unmanaged direct-home RAM writers.
At normal close, server and simulator both report zero obligations. Keep real
begin/end/native-binding/counter evidence, bounded errors, and unmodified
publication/retirement events; never turn a log-only fix into performance PASS.

The current HOST codec/interface publication is an integration contract, not
a new accepted ELF/profile or permission for a real Guest. Those pins and the
new ownership/current4d33 witness follow in the complete reviewed delivery.
