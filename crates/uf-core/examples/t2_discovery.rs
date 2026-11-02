//! Interactive adapter used only by the real multi-process T2 scenario.
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::time::Duration;
use uf_core::topology::ddr::{
    DdrDiscovery, DdrSnapshot, DiscoveryConfig, Operation, ResourceIdentity, Support,
};
use uf_core::{encode, kv};

fn snapshot_fields(s: &DdrSnapshot) -> uf_core::Kv {
    let nodes = s.physical.nodes.as_ref();
    let summary = s
        .backings
        .iter()
        .map(|b| format!("{}:{}:{}", b.identity.object_id, b.state, b.reclaimed))
        .collect::<Vec<_>>()
        .join(",");
    kv(&[
        ("status", "ok".into()),
        ("host_id", s.identity.host_id.clone()),
        ("boot_id", s.identity.boot_id.clone()),
        ("broker_instance", s.identity.broker_instance.clone()),
        ("realm", s.realm.clone()),
        ("epoch", s.epoch.to_string()),
        ("topology_generation", s.topology_generation.to_string()),
        ("capability_generation", s.capability_generation.to_string()),
        ("observed_at_unix_ns", s.observed_at_unix_ns.to_string()),
        ("nodes", nodes.map_or(0, Vec::len).to_string()),
        (
            "nodes_measured",
            nodes
                .map_or(0, |v| v.iter().filter(|n| n.total_bytes.is_some()).count())
                .to_string(),
        ),
        (
            "numa_unknown",
            s.backings.iter().all(|b| b.numa_node.is_none()).to_string(),
        ),
        (
            "page_unknown",
            s.physical.page_residency.starts_with("unknown").to_string(),
        ),
        (
            "technology_unknown",
            s.physical
                .memory_technology
                .starts_with("unknown")
                .to_string(),
        ),
        ("committed_bytes", s.committed_bytes.to_string()),
        ("active_views", s.active_views.to_string()),
        ("backing_count", s.backings.len().to_string()),
        ("backings", summary),
        (
            "retained_count",
            s.backings
                .iter()
                .filter(|b| !b.reclaimed)
                .count()
                .to_string(),
        ),
        (
            "placement_counts_one",
            s.backings
                .iter()
                .all(|b| b.placement_count == 1)
                .to_string(),
        ),
        ("pools", s.pools.len().to_string()),
        ("budget_limit", s.host_budget_limit_bytes.to_string()),
        ("advisory_budget", s.advisory_unallocated_bytes.to_string()),
        (
            "pool_advisory_sum",
            s.pools
                .iter()
                .map(|p| p.advisory_additional_bytes)
                .sum::<u64>()
                .to_string(),
        ),
        (
            "pool_reservable_sum",
            s.pools
                .iter()
                .map(|p| p.reservable_bytes)
                .sum::<u64>()
                .to_string(),
        ),
        (
            "pool_retained_sum",
            s.pools
                .iter()
                .map(|p| p.retained_bytes)
                .sum::<u64>()
                .to_string(),
        ),
        (
            "unknown_map",
            s.capabilities
                .iter()
                .filter(|c| c.operation == Operation::MapRead && c.support == Support::Unknown)
                .count()
                .to_string(),
        ),
        (
            "unsupported_copy",
            s.capabilities
                .iter()
                .filter(|c| c.operation == Operation::Copy && c.support == Support::Unsupported)
                .count()
                .to_string(),
        ),
        (
            "supported_copy",
            s.capabilities
                .iter()
                .filter(|c| c.operation == Operation::Copy && c.support == Support::Supported)
                .count()
                .to_string(),
        ),
    ])
}

fn run() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let control_socket = PathBuf::from(args.next().ok_or("control socket required")?);
    let max_age_ms = args
        .next()
        .ok_or("max age ms required")?
        .parse::<u64>()
        .map_err(|_| "bad max age")?;
    let mut discovery = DdrDiscovery::new(DiscoveryConfig {
        realm: "physical:local".into(),
        control_socket,
        max_age: Duration::from_millis(max_age_ms),
        host_budget_limit_bytes: 64 * 1024 * 1024,
    })?;
    let mut saved: Option<(DdrSnapshot, ResourceIdentity)> = None;
    for line in io::stdin().lock().lines() {
        let line = line.map_err(|e| e.to_string())?;
        let mut words = line.split_whitespace();
        let result: Result<uf_core::Kv, String> = (|| match words.next().unwrap_or("") {
            "snapshot" => Ok(snapshot_fields(&discovery.snapshot()?)),
            "capture" => {
                let id = words
                    .next()
                    .ok_or("missing object")?
                    .parse::<u64>()
                    .map_err(|_| "bad object")?;
                let snap = discovery.snapshot()?;
                let identity = snap
                    .backings
                    .iter()
                    .find(|b| b.identity.object_id == id && b.state == "active" && !b.reclaimed)
                    .ok_or("active object not found")?
                    .identity
                    .clone();
                let fields = snapshot_fields(&snap);
                saved = Some((snap, identity));
                Ok(fields)
            }
            "verify" => {
                let (mut snap, mut resource) = saved.clone().ok_or("no saved snapshot")?;
                let mode = words.next().unwrap_or("ok");
                let mut offset = 1;
                let mut len = 4;
                match mode {
                    "ok" => (),
                    "boot" => resource.boot_id.push('x'),
                    "host" => resource.host_id.push('x'),
                    "broker" => resource.broker_instance.push('x'),
                    "owner" => resource.owner_incarnation.push('x'),
                    "pool" => resource.pool_generation += 1,
                    "resource" => resource.resource_generation += 1,
                    "object" => resource.object_id += 999_999,
                    "epoch" => snap.epoch += 1,
                    "topology" => snap.topology_generation += 1,
                    "capability" => snap.capability_generation += 1,
                    "realm" => snap.realm.push('x'),
                    "range" => {
                        offset = u64::MAX;
                        len = 2;
                    }
                    _ => return Err("invalid verify mode".into()),
                }
                let checked = discovery.guard(&snap, &resource, offset, len)?;
                Ok(snapshot_fields(&checked))
            }
            "quit" => Ok(kv(&[("status", "bye".into())])),
            _ => Err("unknown command".into()),
        })();
        let fields = result.unwrap_or_else(|e| {
            kv(&[
                ("status", "error".into()),
                ("detail", e.replace([';', '=', '\n', '\r'], "_")),
            ])
        });
        print!("{}", encode(&fields));
        io::stdout().flush().map_err(|e| e.to_string())?;
        if line == "quit" {
            break;
        }
    }
    Ok(())
}
fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
