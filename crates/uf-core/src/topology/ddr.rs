//! Single-host DDR discovery, built from T1's authoritative broker projection.
//!
//! The broker projection is atomic under its catalog lock. Linux sysfs is a
//! separate authority: discovery brackets two stable inventory reads with two
//! identical broker projections. It does not claim a cross-authority lock or
//! that a guard makes a later side effect atomic. T3 must hold/revalidate the
//! relevant resources at its own actual side-effect boundary.
use super::json::{self, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[path = "ddr_diagnostic.rs"]
pub mod diagnostic;

const SCHEMA: &str = "uflow-external-ddr-snapshot-v1";
const MAX_FRAME: usize = 1_048_576;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BrokerIdentity {
    pub host_id: String,
    pub boot_id: String,
    pub broker_instance: String,
}

/// Every field of the T1 resource identity is preserved without renaming it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceIdentity {
    pub host_id: String,
    pub boot_id: String,
    pub broker_instance: String,
    pub owner_incarnation: String,
    pub pool_generation: u64,
    pub object_id: u64,
    pub resource_generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Backing {
    pub identity: ResourceIdentity,
    pub owner_name: String,
    pub owner_pid: u64,
    pub owner_alive: bool,
    pub placement_id: u64,
    pub placement_count: u64,
    pub binding_id: u64,
    pub size_bytes: u64,
    pub state: String,
    pub reclaimed: bool,
    /// T1 explicitly reports null; no physical page placement is inferred.
    pub numa_node: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Projection {
    identity: BrokerIdentity,
    epoch: u64,
    observed_at_ns: u128,
    backings: Vec<Backing>,
    committed_bytes: u64,
    active_views: u64,
    pending_handoffs: u64,
    in_flight: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NumaNode {
    pub node: u32,
    /// Linux online-node meminfo; absence/parse failure is unknown, not zero.
    pub total_bytes: Option<u64>,
    pub provenance: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PhysicalInventory {
    pub nodes: Option<Vec<NumaNode>>,
    pub provenance: &'static str,
    pub page_residency: &'static str,
    pub memory_technology: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PoolBudget {
    pub owner_incarnation: String,
    pub pool_generation: u64,
    /// Retained backing bytes, not multiplied by view or lease count.
    pub retained_bytes: u64,
    /// Informational slice only; not an atomic reservation or broker quota.
    pub advisory_additional_bytes: u64,
    /// The T1 API has no shared reservation operation. Never sell the advisory
    /// estimate as an enforceable quota before such an authority exists.
    pub reservable_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Support {
    Supported,
    Unsupported,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operation {
    MapRead,
    Copy,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectedCapability {
    pub source: ResourceIdentity,
    pub destination_address_domain: String,
    pub operation: Operation,
    pub support: Support,
    pub allowed_offset_bytes: u64,
    pub allowed_bytes: u64,
    pub permission: &'static str,
    pub provenance: &'static str,
    pub limitations: &'static str,
}

/// One refresh is a single value. No independent cache or second object catalog
/// is created. All resources and identities come from the T1 projection.
#[derive(Clone, Debug)]
pub struct DdrSnapshot {
    pub realm: String,
    pub identity: BrokerIdentity,
    pub epoch: u64,
    /// Observer-local revisions, scoped by the full broker identity and realm.
    pub topology_generation: u64,
    pub capability_generation: u64,
    pub observed_at_unix_ns: u128,
    pub observed_duration: Duration,
    pub physical: PhysicalInventory,
    pub backings: Vec<Backing>,
    pub committed_bytes: u64,
    pub active_views: u64,
    pub pending_handoffs: u64,
    pub in_flight: u64,
    pub host_budget_limit_bytes: u64,
    pub advisory_unallocated_bytes: u64,
    pub pools: Vec<PoolBudget>,
    pub capabilities: Vec<DirectedCapability>,
    pub provenance: &'static str,
    pub unknowns: &'static str,
    captured_at: Instant,
    max_age: Duration,
    source_observed_at_unix_ns: u128,
}

/// T2 policy cap is explicit; it is NOT T1's per-owner limit or daemon quota.
/// Budgets are advisory, never reservations: concurrent writers must reserve
/// against an authoritative shared ledger before using them.
#[derive(Clone, Debug)]
pub struct DiscoveryConfig {
    pub realm: String,
    pub control_socket: PathBuf,
    pub max_age: Duration,
    pub host_budget_limit_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Fingerprint {
    identity: BrokerIdentity,
    epoch: u64,
    physical: PhysicalInventory,
    backings: Vec<Backing>,
}

pub struct DdrDiscovery {
    config: DiscoveryConfig,
    last: Option<Fingerprint>,
    topology_generation: u64,
    capability_generation: u64,
}

impl DdrDiscovery {
    pub fn new(config: DiscoveryConfig) -> Result<Self, String> {
        if config.realm.trim().is_empty() || config.max_age.is_zero() {
            return Err("realm and non-zero freshness limit required".into());
        }
        Ok(Self {
            config,
            last: None,
            topology_generation: 0,
            capability_generation: 0,
        })
    }

    /// Reads the real T1 broker and real local Linux /sys and /proc. If either
    /// broker observation changes, or online NUMA inventory changes while
    /// sampling, fail closed rather than produce a torn snapshot.
    pub fn snapshot(&mut self) -> Result<DdrSnapshot, String> {
        let _span = diagnostic::span("snapshot");
        let start = Instant::now();
        let wall_start = unix_ns()?;
        let first = {
            let _span = diagnostic::span("snapshot.first-query");
            query_broker(&self.config.control_socket)?
        };
        let physical_a = observe_physical();
        let capacity_a = observe_available();
        let physical_b = observe_physical();
        let capacity_b = observe_available();
        let second = {
            let _span = diagnostic::span("snapshot.second-query");
            query_broker(&self.config.control_socket)?
        };
        let wall_end = unix_ns()?;
        let elapsed = start.elapsed();
        diagnostic::note(format!(
            "snapshot wall_start={wall_start} wall_end={wall_end} elapsed_ns={} max_age_ns={}",
            elapsed.as_nanos(),
            self.config.max_age.as_nanos()
        ));
        if wall_end < wall_start || elapsed > self.config.max_age {
            diagnostic::note("snapshot.reject=freshness".into());
            return Err("clock changed or observation exceeded freshness limit".into());
        }
        if first.observed_at_ns < wall_start
            || second.observed_at_ns > wall_end
            || first.observed_at_ns > second.observed_at_ns
        {
            return Err("broker observation time outside local sampling interval".into());
        }
        if !same_projection(&first, &second) || physical_a != physical_b {
            return Err("broker or NUMA inventory changed during observation; retry".into());
        }
        let fingerprint = Fingerprint {
            identity: second.identity.clone(),
            epoch: second.epoch,
            physical: physical_b.clone(),
            backings: second.backings.clone(),
        };
        if self.last.as_ref() != Some(&fingerprint) {
            self.topology_generation = self
                .topology_generation
                .checked_add(1)
                .ok_or("topology revision overflow")?;
            self.capability_generation = self
                .capability_generation
                .checked_add(1)
                .ok_or("capability revision overflow")?;
            self.last = Some(fingerprint);
        }
        let available = match (capacity_a, capacity_b) {
            (Some(a), Some(b)) => a.min(b),
            _ => 0, // unknown is not available capacity
        };
        let remaining_cap = self
            .config
            .host_budget_limit_bytes
            .saturating_sub(second.committed_bytes);
        let budget = available.min(remaining_cap);
        let pools = pools(&second.backings, budget)?;
        let capabilities = capabilities(&second.backings);
        Ok(DdrSnapshot {
            realm: self.config.realm.clone(), identity: second.identity,
            epoch: second.epoch, topology_generation: self.topology_generation,
            capability_generation: self.capability_generation,
            observed_at_unix_ns: wall_end, observed_duration: elapsed,
            physical: physical_b, backings: second.backings,
            committed_bytes: second.committed_bytes, active_views: second.active_views,
            pending_handoffs: second.pending_handoffs, in_flight: second.in_flight,
            host_budget_limit_bytes: self.config.host_budget_limit_bytes,
            advisory_unallocated_bytes: budget, pools, capabilities,
            provenance: "GetExternalDdrSnapshot under T1 catalog lock; /sys node online and node meminfo; /proc/meminfo and /sys/fs/cgroup",
            unknowns: "per-backing NUMA, page residency, link reachability, technology of each NUMA node, performance, owner lease id, destination leases",
            captured_at: start, max_age: self.config.max_age, source_observed_at_unix_ns: second.observed_at_ns,
        })
    }

    /// Re-query immediately before an intended side effect; reject stale age,
    /// realm, broker/boot, epoch, versions, NUMA inventory and resource tuple.
    /// This is a guard contract only, not an atomic test-and-act primitive.
    pub fn guard(
        &mut self,
        previous: &DdrSnapshot,
        resource: &ResourceIdentity,
        offset: u64,
        bytes: u64,
    ) -> Result<DdrSnapshot, String> {
        if previous.realm != self.config.realm
            || previous.max_age != self.config.max_age
            || previous.host_budget_limit_bytes != self.config.host_budget_limit_bytes
            || previous.captured_at.elapsed() > self.config.max_age
        {
            return Err("stale snapshot or realm mismatch".into());
        }
        let now = unix_ns()?;
        if now < previous.observed_at_unix_ns || now < previous.source_observed_at_unix_ns {
            return Err("wall clock moved backwards".into());
        }
        let current = self.snapshot()?;
        if previous.identity != current.identity
            || previous.epoch != current.epoch
            || previous.topology_generation != current.topology_generation
            || previous.capability_generation != current.capability_generation
            || previous.physical != current.physical
            || previous.backings != current.backings
        {
            return Err("stale broker, epoch, generation, topology or resources".into());
        }
        let backing = current
            .backings
            .iter()
            .find(|b| &b.identity == resource)
            .ok_or("stale resource identity")?;
        if !backing.owner_alive || backing.state != "active" || backing.reclaimed {
            return Err("resource not active".into());
        }
        if bytes == 0
            || offset
                .checked_add(bytes)
                .is_none_or(|end| end > backing.size_bytes)
        {
            return Err("resource range out of bounds".into());
        }
        if previous.captured_at.elapsed() > self.config.max_age {
            return Err("snapshot expired during guard".into());
        }
        Ok(current)
    }
}

/// T3 can supply a verifier for a real directed measurement; T2 has no such
/// verifier and never upgrades copy. Evidence must be bound to the complete
/// broker/resource/epoch/direction/range and expire. The verifier is an explicit
/// trust boundary, not a hard-coded `supported` flag.
#[derive(Clone, Debug)]
pub struct CapabilityEvidence {
    pub source: ResourceIdentity,
    pub destination_address_domain: String,
    pub destination_pool_generation: u64,
    pub realm: String,
    pub epoch: u64,
    pub topology_generation: u64,
    pub capability_generation: u64,
    pub operation: Operation,
    pub offset: u64,
    pub bytes: u64,
    pub expires_at_unix_ns: u128,
    pub provenance: String,
    pub proof: Vec<u8>,
}

pub trait CapabilityEvidenceVerifier {
    fn verify(&self, evidence: &CapabilityEvidence) -> bool;
}

pub fn verify_capability_evidence(
    snapshot: &DdrSnapshot,
    evidence: &CapabilityEvidence,
    verifier: &dyn CapabilityEvidenceVerifier,
) -> Result<Support, String> {
    if evidence.provenance.is_empty()
        || evidence.proof.is_empty()
        || evidence.epoch != snapshot.epoch
        || evidence.realm != snapshot.realm
        || evidence.topology_generation != snapshot.topology_generation
        || evidence.capability_generation != snapshot.capability_generation
        || evidence.expires_at_unix_ns <= unix_ns()?
        || snapshot.captured_at.elapsed() > snapshot.max_age
    {
        return Err("missing, stale or unscoped capability evidence".into());
    }
    let cap = snapshot
        .capabilities
        .iter()
        .find(|c| {
            c.source == evidence.source
                && c.destination_address_domain == evidence.destination_address_domain
                && c.operation == evidence.operation
        })
        .ok_or("unknown directed route")?;
    if !snapshot.backings.iter().any(|b| {
        b.identity == evidence.source && b.owner_alive && b.state == "active" && !b.reclaimed
    }) || !snapshot.pools.iter().any(|p| {
        p.owner_incarnation == evidence.destination_address_domain
            && p.pool_generation == evidence.destination_pool_generation
    }) {
        return Err("retired route cannot be upgraded".into());
    }
    if evidence.bytes == 0
        || evidence
            .offset
            .checked_add(evidence.bytes)
            .is_none_or(|end| end > cap.allowed_bytes)
        || !verifier.verify(evidence)
    {
        return Err("invalid evidence or range".into());
    }
    Ok(Support::Supported)
}

fn unix_ns() -> Result<u128, String> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "clock before epoch")?
        .as_nanos())
}

fn same_projection(a: &Projection, b: &Projection) -> bool {
    a.identity == b.identity
        && a.epoch == b.epoch
        && a.backings == b.backings
        && a.committed_bytes == b.committed_bytes
        && a.active_views == b.active_views
        && a.pending_handoffs == b.pending_handoffs
        && a.in_flight == b.in_flight
}

fn n64(value: &Value) -> Result<u64, String> {
    u64::try_from(value.number()?).map_err(|_| "integer exceeds u64".into())
}
fn s(value: &Value, key: &str) -> Result<String, String> {
    let text = value.field(key)?.string()?;
    if text.is_empty() {
        return Err(format!("empty {key}"));
    }
    Ok(text.to_string())
}
fn n(value: &Value, key: &str) -> Result<u64, String> {
    n64(value.field(key)?)
}
fn b(value: &Value, key: &str) -> Result<bool, String> {
    value.field(key)?.boolean()
}

fn parse_projection(fields: &BTreeMap<String, String>) -> Result<Projection, String> {
    if fields.get("status").map(String::as_str) != Some("ok") {
        return Err("broker returned error".into());
    }
    let doc = json::parse(
        fields
            .get("projection_json")
            .ok_or("missing projection_json")?,
    )?;
    if s(&doc, "schema")? != SCHEMA {
        return Err("unsupported T1 schema".into());
    }
    let identity = BrokerIdentity {
        host_id: s(&doc, "host_id")?,
        boot_id: s(&doc, "boot_id")?,
        broker_instance: s(&doc, "broker_instance")?,
    };
    let epoch = n(&doc, "lifecycle_epoch")?;
    if fields
        .get("lifecycle_epoch")
        .ok_or("missing wire epoch")?
        .parse::<u64>()
        .map_err(|_| "invalid wire epoch")?
        != epoch
    {
        return Err("wire epoch mismatch".into());
    }
    let wire = |key: &str| -> Result<u64, String> {
        fields
            .get(key)
            .ok_or_else(|| format!("missing {key}"))?
            .parse()
            .map_err(|_| format!("invalid {key}"))
    };
    let mut ids = BTreeSet::new();
    let mut placements = BTreeSet::new();
    let mut backings = Vec::new();
    let mut total = 0u64;
    for row in doc.field("backings")?.array()? {
        let resource = ResourceIdentity {
            host_id: identity.host_id.clone(),
            boot_id: identity.boot_id.clone(),
            broker_instance: identity.broker_instance.clone(),
            owner_incarnation: s(row, "owner_incarnation")?,
            pool_generation: n(row, "pool_generation")?,
            object_id: n(row, "object_id")?,
            resource_generation: n(row, "resource_generation")?,
        };
        let placement_id = n(row, "placement_id")?;
        let placement_count = n(row, "placement_count")?;
        let reclaimed = b(row, "reclaimed")?;
        let size_bytes = n(row, "size_bytes")?;
        if resource.object_id == 0
            || resource.pool_generation == 0
            || resource.resource_generation == 0
            || placement_id == 0
            || size_bytes == 0
            || placement_count != 1
            || !ids.insert(resource.object_id)
            || !placements.insert(placement_id)
        {
            return Err("invalid or duplicate backing/placement identity".into());
        }
        if row.field("numa_node")? != &Value::Null {
            return Err("unexpected T1 NUMA attribution".into());
        }
        let state = s(row, "state")?;
        if !matches!(
            state.as_str(),
            "active" | "pending" | "revoked" | "owner_exited" | "registration_rollback"
        ) {
            return Err("unknown backing state".into());
        }
        if reclaimed && matches!(state.as_str(), "active" | "pending") {
            return Err("reclaimed active backing".into());
        }
        if !reclaimed {
            total = total
                .checked_add(size_bytes)
                .ok_or("backing byte overflow")?;
        }
        backings.push(Backing {
            identity: resource,
            owner_name: s(row, "owner_name")?,
            owner_pid: n(row, "owner_pid")?,
            owner_alive: b(row, "owner_alive")?,
            placement_id,
            placement_count,
            binding_id: n(row, "binding_id")?,
            size_bytes,
            state,
            reclaimed,
            numa_node: None,
        });
    }
    backings.sort_by_key(|b| b.identity.object_id);
    if total != wire("committed_bytes")? {
        return Err("T1 committed-byte sum mismatch".into());
    }
    Ok(Projection {
        identity,
        epoch,
        observed_at_ns: doc.field("observed_at_unix_ns")?.number()?,
        backings,
        committed_bytes: total,
        active_views: wire("active_views")?,
        pending_handoffs: wire("pending_handoffs")?,
        in_flight: wire("in_flight")?,
    })
}

#[cfg(unix)]
fn query_broker(control: &Path) -> Result<Projection, String> {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::os::unix::net::UnixStream;
    let mut path = control.as_os_str().to_os_string();
    path.push(".external-ddr");
    let mut stream = {
        let _span = diagnostic::span("broker.connect");
        UnixStream::connect(path).map_err(|e| diagnostic::io_error("broker.connect", e))?
    };
    {
        let _span = diagnostic::span("broker.set-read-timeout");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .map_err(|e| diagnostic::io_error("broker.set-read-timeout", e))?;
    }
    {
        let _span = diagnostic::span("broker.set-write-timeout");
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .map_err(|e| diagnostic::io_error("broker.set-write-timeout", e))?;
    }
    {
        let _span = diagnostic::span("broker.write-request");
        stream
            .write_all(b"op=GetExternalDdrSnapshot\n")
            .map_err(|e| diagnostic::io_error("broker.write-request", e))?;
    }
    let mut buf = Vec::new();
    {
        let _span = diagnostic::span("broker.read-response");
        BufReader::new(stream)
            .take((MAX_FRAME + 1) as u64)
            .read_until(b'\n', &mut buf)
            .map_err(|e| diagnostic::io_error("broker.read-response", e))?;
    }
    let _span = diagnostic::span("broker.parse");
    if buf.len() > MAX_FRAME || !buf.ends_with(b"\n") {
        return Err("invalid or oversized broker response".into());
    }
    let line = std::str::from_utf8(&buf[..buf.len() - 1]).map_err(|_| "invalid broker UTF-8")?;
    let mut fields = BTreeMap::new();
    for item in line.split(';') {
        let (key, value) = item.split_once('=').ok_or("invalid broker frame")?;
        if key.is_empty() || fields.insert(key.to_string(), value.to_string()).is_some() {
            return Err("duplicate/empty broker field".into());
        }
    }
    let result = parse_projection(&fields);
    diagnostic::note(format!("broker.parse ok={}", result.is_ok()));
    result
}
#[cfg(not(unix))]
fn query_broker(_: &Path) -> Result<Projection, String> {
    Err("DDR discovery requires Unix".into())
}

fn parse_online(text: &str) -> Option<Vec<u32>> {
    let mut ids = BTreeSet::new();
    for part in text.trim().split(',') {
        let (start, end) = match part.split_once('-') {
            Some((a, b)) => (a.parse::<u32>().ok()?, b.parse::<u32>().ok()?),
            None => {
                let id = part.parse::<u32>().ok()?;
                (id, id)
            }
        };
        if end < start || end > 65535 || end - start > 4096 {
            return None;
        }
        for id in start..=end {
            if !ids.insert(id) {
                return None;
            }
        }
    }
    if ids.is_empty() {
        None
    } else {
        Some(ids.into_iter().collect())
    }
}
fn meminfo_bytes(text: &str, name: &str) -> Option<u64> {
    for line in text.lines() {
        let (key, value) = line.split_once(':')?;
        if key.split_whitespace().last()? == name {
            let mut parts = value.split_whitespace();
            let kb: u64 = parts.next()?.parse().ok()?;
            if parts.next()? != "kB" || parts.next().is_some() {
                return None;
            }
            return kb.checked_mul(1024);
        }
    }
    None
}
fn observe_physical() -> PhysicalInventory {
    let nodes = fs::read_to_string("/sys/devices/system/node/online")
        .ok()
        .and_then(|s| parse_online(&s))
        .map(|ids| {
            ids.into_iter()
                .map(|node| NumaNode {
                    node,
                    total_bytes: fs::read_to_string(format!(
                        "/sys/devices/system/node/node{node}/meminfo"
                    ))
                    .ok()
                    .and_then(|s| meminfo_bytes(&s, "MemTotal")),
                    provenance: "/sys/devices/system/node/online and node*/meminfo",
                })
                .collect()
        });
    PhysicalInventory {
        nodes,
        provenance: "Linux sysfs online inventory; unknown on read/parse failure",
        page_residency: "unknown: no page residency measurement",
        memory_technology: "unknown: sysfs node meminfo does not establish DDR versus HBM",
    }
}
fn read_num(path: &str) -> Option<u64> {
    let text = fs::read_to_string(path).ok()?;
    if text.trim() == "max" {
        Some(u64::MAX)
    } else {
        text.trim().parse().ok()
    }
}
fn observe_available() -> Option<u64> {
    let available = meminfo_bytes(&fs::read_to_string("/proc/meminfo").ok()?, "MemAvailable")?;
    let remaining = if let (Some(limit), Some(current)) = (
        read_num("/sys/fs/cgroup/memory.max"),
        read_num("/sys/fs/cgroup/memory.current"),
    ) {
        limit.saturating_sub(current)
    } else {
        let limit = read_num("/sys/fs/cgroup/memory/memory.limit_in_bytes")?;
        let current = read_num("/sys/fs/cgroup/memory/memory.usage_in_bytes")?;
        limit.saturating_sub(current)
    };
    Some(available.min(remaining))
}
fn pools(backings: &[Backing], budget: u64) -> Result<Vec<PoolBudget>, String> {
    let mut pools = BTreeMap::<(String, u64), (u64, bool)>::new();
    for backing in backings {
        let key = (
            backing.identity.owner_incarnation.clone(),
            backing.identity.pool_generation,
        );
        let entry = pools.entry(key).or_default();
        if !backing.reclaimed {
            entry.0 = entry
                .0
                .checked_add(backing.size_bytes)
                .ok_or("pool overflow")?;
        }
        entry.1 |= backing.owner_alive && backing.state == "active" && !backing.reclaimed;
    }
    let eligible = pools.values().filter(|(_, active)| *active).count() as u64;
    let mut remaining = budget;
    let mut left = eligible;
    Ok(pools
        .into_iter()
        .map(
            |((owner_incarnation, pool_generation), (retained_bytes, active))| {
                let advisory_additional_bytes = if active {
                    let slice = remaining / left;
                    remaining -= slice;
                    left -= 1;
                    slice
                } else {
                    0
                };
                PoolBudget {
                    owner_incarnation,
                    pool_generation,
                    retained_bytes,
                    advisory_additional_bytes,
                    reservable_bytes: 0,
                }
            },
        )
        .collect())
}
fn capabilities(backings: &[Backing]) -> Vec<DirectedCapability> {
    let domains: BTreeSet<_> = backings
        .iter()
        .filter(|b| b.owner_alive && b.state == "active" && !b.reclaimed)
        .map(|b| b.identity.owner_incarnation.clone())
        .collect();
    let mut caps = Vec::new();
    for backing in backings {
        for domain in &domains {
            for op in [Operation::MapRead, Operation::Copy] {
                let alive = backing.owner_alive && backing.state == "active" && !backing.reclaimed;
                caps.push(DirectedCapability { source: backing.identity.clone(), destination_address_domain: domain.clone(), operation: op,
                    support: if op == Operation::Copy || !alive { Support::Unsupported } else { Support::Unknown },
                    allowed_offset_bytes: 0, allowed_bytes: backing.size_bytes, permission: if op == Operation::MapRead { "read-only" } else { "not available" },
                    provenance: "T1 broker projection and T1 contract; no directed runtime probe",
                    limitations: "same UID cooperative; broker range checks only; FD covers full backing; no page-residency or performance proof" });
            }
        }
    }
    caps
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    fn socket_fixture(
        action: impl FnOnce(std::os::unix::net::UnixStream) + Send + 'static,
    ) -> (PathBuf, std::thread::JoinHandle<()>, PathBuf) {
        use std::os::unix::net::UnixListener;
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let base = std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target"));
        fs::create_dir_all(&base).unwrap();
        let root = base.parent().unwrap().join(format!(
            "cq-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        let control = root.join("u");
        let socket = root.join("u.external-ddr");
        let listener = UnixListener::bind(&socket).unwrap();
        let worker = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            action(stream);
        });
        (control, worker, root)
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn broker_read_timeout_keeps_original_five_second_boundary_and_errno_context() {
        use std::io::{BufRead, BufReader};
        let (control, worker, root) = socket_fixture(|stream| {
            let mut line = String::new();
            BufReader::new(&stream).read_line(&mut line).unwrap();
            assert_eq!(line, "op=GetExternalDdrSnapshot\n");
            // Controlled same-mechanism socket timeout, not physical I/O or
            // proof of which syscall produced the historical run130 failure.
            std::thread::sleep(Duration::from_millis(5500));
        });
        let start = Instant::now();
        let error = query_broker(&control).unwrap_err();
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_secs(5) && elapsed < Duration::from_secs(7),
            "{elapsed:?}"
        );
        assert!(
            error.contains("broker.read-response")
                && error.contains("kind=WouldBlock")
                && error.contains("raw_os_error=Some(11)"),
            "{error}"
        );
        worker.join().unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn broker_unavailable_and_malformed_responses_remain_rejections() {
        use std::io::Write;
        for response in [
            "status=error;detail=do-not-forward\n",
            "status=ok;status=ok\n",
            "status=ok\n",
            "bad-frame\n",
            "status=ok",
        ] {
            let (control, worker, root) = socket_fixture(move |mut stream| {
                stream.write_all(response.as_bytes()).unwrap();
            });
            assert!(query_broker(&control).is_err());
            worker.join().unwrap();
            fs::remove_dir_all(root).unwrap();
            assert!(query_broker(&control)
                .unwrap_err()
                .contains("broker.connect"));
        }
    }

    fn fields(rows: &str, committed: u64) -> BTreeMap<String, String> {
        let projection = format!(
            r#"{{"schema":"uflow-external-ddr-snapshot-v1","host_id":"host","boot_id":"boot","broker_instance":"broker","lifecycle_epoch":7,"observed_at_unix_ns":123,"backings":[{rows}]}}"#
        );
        [
            ("status", "ok".to_string()),
            ("projection_json", projection),
            ("lifecycle_epoch", "7".to_string()),
            ("committed_bytes", committed.to_string()),
            ("active_views", "2".to_string()),
            ("pending_handoffs", "0".to_string()),
            ("in_flight", "0".to_string()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect()
    }
    fn row(id: u64, owner: &str, state: &str, reclaimed: bool) -> String {
        format!(
            r#"{{"object_id":{id},"placement_id":{},"placement_count":1,"binding_id":{},"owner_name":"owner","owner_incarnation":"{owner}","owner_pid":42,"owner_alive":true,"pool_generation":{},"resource_generation":{},"size_bytes":100,"state":"{state}","reclaimed":{reclaimed},"numa_node":null}}"#,
            id + 100,
            id + 200,
            id + 300,
            id + 400
        )
    }

    #[test]
    fn authoritative_projection_counts_backings_not_views() {
        let rows = format!(
            "{},{}",
            row(1, "a", "active", false),
            row(2, "b", "revoked", false)
        );
        let p = parse_projection(&fields(&rows, 200)).unwrap();
        assert_eq!(p.active_views, 2);
        assert_eq!(p.backings.len(), 2);
        assert_eq!(p.committed_bytes, 200);
        assert_eq!(p.backings[0].numa_node, None);
        assert_eq!(p.backings[1].state, "revoked");
        assert!(parse_projection(&fields(&rows, 400)).is_err());
        assert!(parse_projection(&fields(
            &format!("{0},{0}", row(1, "a", "active", false)),
            200
        ))
        .is_err());
        assert!(parse_projection(&fields(&row(1, "a", "active", true), 0)).is_err());
    }

    #[test]
    fn pools_are_disjoint_advisory_slices_and_unknowns_never_supported() {
        let rows = format!(
            "{},{}",
            row(1, "a", "active", false),
            row(2, "b", "active", false)
        );
        let p = parse_projection(&fields(&rows, 200)).unwrap();
        let pool = pools(&p.backings, 101).unwrap();
        assert_eq!(pool.iter().map(|x| x.retained_bytes).sum::<u64>(), 200);
        assert_eq!(
            pool.iter()
                .map(|x| x.advisory_additional_bytes)
                .sum::<u64>(),
            101
        );
        assert!(pool.iter().all(|x| x.reservable_bytes == 0));
        let caps = capabilities(&p.backings);
        assert_eq!(caps.len(), 8);
        assert!(caps.iter().all(|c| c.support != Support::Supported));
        assert!(caps
            .iter()
            .filter(|c| c.operation == Operation::Copy)
            .all(|c| c.support == Support::Unsupported));
        assert!(caps
            .iter()
            .filter(|c| c.operation == Operation::MapRead)
            .all(|c| c.support == Support::Unknown));
        assert!(caps.iter().all(|c| c.allowed_bytes == 100));
    }

    #[test]
    fn linux_inventory_parsing_fails_closed() {
        assert_eq!(parse_online("0-2,4\n"), Some(vec![0, 1, 2, 4]));
        for bad in ["", "2-1", "0-99999", "0,0", "0-1,1"] {
            assert_eq!(parse_online(bad), None);
        }
        assert_eq!(
            meminfo_bytes("Node 0 MemTotal: 42 kB\n", "MemTotal"),
            Some(43008)
        );
        assert_eq!(meminfo_bytes("MemTotal: 42 MB\n", "MemTotal"), None);
        assert_eq!(meminfo_bytes("MemTotal: 42 kB\n", "MemAvailable"), None);
    }

    #[test]
    fn evidence_upgrade_requires_fresh_bound_route_and_explicit_verifier() {
        struct FixtureVerifier(bool);
        impl CapabilityEvidenceVerifier for FixtureVerifier {
            fn verify(&self, _: &CapabilityEvidence) -> bool {
                self.0
            }
        }
        let p = parse_projection(&fields(&row(1, "a", "active", false), 100)).unwrap();
        let now = unix_ns().unwrap();
        let snapshot = DdrSnapshot {
            realm: "physical:local".into(),
            identity: p.identity.clone(),
            epoch: p.epoch,
            topology_generation: 1,
            capability_generation: 1,
            observed_at_unix_ns: now,
            observed_duration: Duration::ZERO,
            physical: PhysicalInventory {
                nodes: None,
                provenance: "test",
                page_residency: "unknown",
                memory_technology: "unknown",
            },
            backings: p.backings.clone(),
            committed_bytes: 100,
            active_views: 0,
            pending_handoffs: 0,
            in_flight: 0,
            host_budget_limit_bytes: 1000,
            advisory_unallocated_bytes: 0,
            pools: pools(&p.backings, 0).unwrap(),
            capabilities: capabilities(&p.backings),
            provenance: "test",
            unknowns: "test",
            captured_at: Instant::now(),
            max_age: Duration::from_secs(2),
            source_observed_at_unix_ns: now,
        };
        let mut evidence = CapabilityEvidence {
            source: p.backings[0].identity.clone(),
            destination_address_domain: "a".into(),
            destination_pool_generation: p.backings[0].identity.pool_generation,
            realm: "physical:local".into(),
            epoch: 7,
            topology_generation: 1,
            capability_generation: 1,
            operation: Operation::Copy,
            offset: 2,
            bytes: 5,
            expires_at_unix_ns: now + 1_000_000_000,
            provenance: "fixture only".into(),
            proof: vec![1],
        };
        assert!(verify_capability_evidence(&snapshot, &evidence, &FixtureVerifier(false)).is_err());
        assert_eq!(
            verify_capability_evidence(&snapshot, &evidence, &FixtureVerifier(true)),
            Ok(Support::Supported)
        );
        evidence.epoch += 1;
        assert!(verify_capability_evidence(&snapshot, &evidence, &FixtureVerifier(true)).is_err());
        evidence.epoch -= 1;
        evidence.source.boot_id.push('x');
        assert!(verify_capability_evidence(&snapshot, &evidence, &FixtureVerifier(true)).is_err());
        evidence.source.boot_id.pop();
        evidence.offset = 100;
        assert!(verify_capability_evidence(&snapshot, &evidence, &FixtureVerifier(true)).is_err());
        evidence.offset = 2;
        evidence.expires_at_unix_ns = now;
        assert!(verify_capability_evidence(&snapshot, &evidence, &FixtureVerifier(true)).is_err());
    }

    #[test]
    fn static_phasec02_fixture_still_requires_immutable_generation_one() {
        let mut fixture = super::super::TopologySnapshot {
            snapshot_id: "fixture".into(),
            generation: 1,
            observed_at_ns: 1,
            execution_realm_id: "simulated:test".into(),
            immutable_for_run: true,
            nodes: vec![],
            devices: vec![],
            links: vec![],
        };
        assert!(fixture.validate_static_phasec02().is_ok());
        fixture.generation = 2;
        assert!(fixture.validate_static_phasec02().is_err());
        fixture.generation = 1;
        fixture.immutable_for_run = false;
        assert!(fixture.validate_static_phasec02().is_err());

        let mut capabilities = super::super::CapabilitySnapshot {
            snapshot_id: "fixture".into(),
            generation: 1,
            observed_at_ns: 1,
            execution_realm_id: "simulated:test".into(),
            immutable_for_run: true,
            contributions: vec![],
        };
        assert!(capabilities.validate_static_phasec02().is_ok());
        capabilities.generation = 2;
        assert!(capabilities.validate_static_phasec02().is_err());
        capabilities.generation = 1;
        capabilities.immutable_for_run = false;
        assert!(capabilities.validate_static_phasec02().is_err());
    }
}
