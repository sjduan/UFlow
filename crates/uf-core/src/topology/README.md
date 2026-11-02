# External DDR discovery and guards

`uf_core::topology::ddr::{DdrDiscovery, DiscoveryConfig}` consumes the daemon's
`GetExternalDdrSnapshot` projection. The Python `ExternalDdrClient.snapshot()`
exposes that projection too. Discovery does not create another catalog, issue
leases, or open and close mappings. The separate static `TopologySnapshot` and
`validate_static_phasec02()` retain their immutable generation-1 semantics.

## Observation and identity

Construct a long-lived discovery instance with an explicit realm, daemon control
socket, nonzero maximum age and host-budget policy ceiling, then call `snapshot()`.
It samples the broker twice around Linux NUMA, `/proc/meminfo` and cgroup
observations. Broker projections are individually atomic under the catalog lock;
changed projections, changed NUMA inventory, clock errors and expired observations
are rejected. This is not a cross-authority lock: a change and revert between
observations or a change after them is possible.

A snapshot retains the full host, boot, broker, owner incarnation, pool and
resource generation identities, object/placement/binding identities, lifecycle
epoch and state. Observer-local topology and capability generations are scoped to
the full identity and realm, not globally persistent identifiers. Missing broker
data is an error, not an empty inventory. Physical NUMA inventory is observed;
per-backing page residency, memory technology, arbitrary peer reachability and
performance are not inferred from it.

Each retained backing counts once, including a retired backing with outstanding
views; a view is neither another placement nor additional capacity. A revoked or
exited owner cannot grant new views, while already confirmed views remain subject
to their lifetime rules.

## Advisory budgets and capabilities

`host_budget_limit_bytes` is a discovery policy ceiling, not the daemon's quota.
Available-memory and cgroup readings bound an advisory remainder after retained
bytes are charged; missing available-memory observations do not become usable
capacity. Pool slices are informational, and `reservable_bytes` is zero: they
are not a shared, atomic reservation service and must not be sold as guaranteed
capacity to concurrent submitters.

Capability rows are directed and identify the backing, destination address
domain, permitted range and operation. Live map-read reachability remains
`Unknown` without evidence; the discovery projection's copy rows are
`Unsupported`. The latter describes this discovery interface, not the absence of
the separately implemented explicit external-DDR copy service. The FD covers
the whole backing: cooperative same-UID range checks are not OS subrange
isolation. Unknown reachability, page residency and costs are not zero-cost or
supported paths.

`DdrDiscovery::guard` checks the snapshot age, realm, resource identity, epoch,
versions, physical inventory, state and range by observing again. It is a
freshness check, not atomic admission for a future side effect. The explicit
copy path performs its own capability verification and revalidates/retains
resources at its admission boundary. `CapabilityEvidenceVerifier` is a
caller-supplied trust boundary; a test verifier is not production evidence.

## Examples

The repository includes the Rust discovery adapter
[`t2_discovery.rs`](../../examples/t2_discovery.rs) and the external DDR
[usage guide](../../../../examples/ddr_v0/README.md). The adapter exercises the
public discovery API against a deployed broker; executing scenario scripts
requires the isolated deployment precautions described in that guide.
