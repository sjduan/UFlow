//! Managed host resources extend the one Catalog, not the Python executor.
use crate::catalog::{Catalog, Client, Lease, Object};
use crate::ddr_process_identity::ProcessBirth;
use crate::pool_provider::{exited, opaque_id, pidfd};
use crate::pool_ranges::{RangeLease, Ranges};
use crate::pool_wire::{fields, number, text};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs::File;
use uf_core::idl::AccessBindingRecord;
use uf_core::provider::{
    AccessKind, Addressability, ProviderDescriptor, ProviderResourceRef, TransportKind,
};

pub struct Peer {
    pub birth: ProcessBirth,
    pub pidfd: File,
}
impl Peer {
    pub fn actor(&self) -> String {
        format!(
            "{}:{}:{}",
            self.birth.pid, self.birth.starttime_ticks, self.birth.pid_namespace_ino
        )
    }
}
pub fn birth_json(b: &ProcessBirth) -> Value {
    json!({"pid":b.pid,"uid":b.uid,"gid":b.gid,"starttime_ticks":b.starttime_ticks,
           "pid_namespace_dev":b.pid_namespace_dev,"pid_namespace_ino":b.pid_namespace_ino})
}
pub struct ChildView {
    pub birth: ProcessBirth,
    pub pidfd: File,
    pub unknown: bool,
}
pub struct Session {
    pub catalog_id: u64,
    pub access_id: u64,
    pub birth: ProcessBirth,
    pub pidfd: Option<File>,
    pub access: String,
    pub state: &'static str,
    pub closing: bool,
    pub children: BTreeMap<i32, ChildView>,
}
pub struct Pool {
    pub id: String,
    pub epoch: String,
    pub provider_session: String,
    pub object: u64,
    pub placement: u64,
    pub binding: u64,
    pub nbytes: u64,
    pub state: &'static str,
    pub canonical: Value,
    pub file: Option<File>,
    pub resource: Option<ProviderResourceRef>,
    pub descriptor: Option<ProviderDescriptor>,
    pub ranges: Ranges,
    pub clients: BTreeMap<String, Session>,
    pub last_error: String,
    pub release_result: Value,
}
impl Pool {
    pub fn description(&self) -> Value {
        json!({"schema":1,"pool_id":self.id,"pool_epoch":self.epoch,"provider_session":self.provider_session,
               "provider_kind":"posix_shm_simpler","nbytes":self.nbytes,"resource_id":self.object,
               "placement_id":self.placement,"binding_id":self.binding,"resource_generation":1,
               "canonical":self.canonical,"state":self.state})
    }
    pub fn frozen(&self) -> bool {
        self.clients.values().any(|client| {
            client.state == "Unknown" || client.children.values().any(|child| child.unknown)
        })
    }
    pub fn admission(&self) -> Result<(), String> {
        if self.state != "Allocated" || self.frozen() {
            Err("pool not admitting new work".into())
        } else {
            Ok(())
        }
    }
    pub fn block_json(&self, block: Value) -> Value {
        let mut v = block;
        v["pool_id"] = json!(self.id);
        v["pool_epoch"] = json!(self.epoch);
        v["provider_session"] = json!(self.provider_session);
        v
    }
    pub fn lease_json(&self, lease: &RangeLease, access_generation: u64) -> Value {
        let mut v = self.block_json(lease.identity());
        v["access_generation"] = json!(access_generation);
        v
    }
    pub fn validate_lease(
        &self,
        value: &Value,
        client: &str,
        access: u64,
    ) -> Result<RangeLease, String> {
        fields(
            value,
            &[
                "pool_id",
                "pool_epoch",
                "provider_session",
                "access_generation",
                "lease_id",
                "lease_generation",
                "client_session",
                "block_id",
                "generation",
                "offset",
                "nbytes",
                "mode",
                "content_version",
                "content_id",
            ],
            &[],
        )?;
        if value["pool_id"] != self.id
            || value["pool_epoch"] != self.epoch
            || value["provider_session"] != self.provider_session
            || value["access_generation"] != access
        {
            return Err("stale pool/access lease tuple".into());
        }
        let mut identity = value.clone();
        for key in [
            "pool_id",
            "pool_epoch",
            "provider_session",
            "access_generation",
        ] {
            identity.as_object_mut().unwrap().remove(key);
        }
        self.ranges.validate(&identity, client)
    }
    pub fn refresh(&mut self) {
        let mut lost = Vec::new();
        for (id, client) in &mut self.clients {
            if client.state == "Unmapped" {
                continue;
            }
            if client.pidfd.as_ref().is_none_or(exited) {
                client.state = "Unknown";
                lost.push(id.clone());
            }
            for child in client.children.values_mut() {
                if exited(&child.pidfd) {
                    child.unknown = true;
                }
            }
        }
        for id in lost {
            let leases = self
                .ranges
                .leases
                .values()
                .filter(|lease| lease.client == id && lease.state != "Closed")
                .cloned()
                .collect::<Vec<_>>();
            for lease in leases {
                self.ranges.unknown(&lease);
            }
        }
    }
}

pub struct Operation {
    pub request: String,
    pub state: &'static str,
    pub response: Value,
}
#[derive(Default)]
pub struct State {
    pub pools: BTreeMap<String, Pool>,
    pub operations: BTreeMap<String, Operation>,
    pub revision: u64,
    pub counts: BTreeMap<String, u64>,
}
impl State {
    pub fn owns_object(&self, id: u64) -> bool {
        self.pools.values().any(|pool| pool.object == id)
    }
    pub fn owns_lease(&self, id: u64) -> bool {
        self.pools.values().any(|pool| {
            pool.ranges
                .leases
                .values()
                .any(|lease| lease.catalog_id == id)
        })
    }
    pub fn sync_leases(&self, st: &mut Catalog) {
        for pool in self.pools.values() {
            for lease in pool.ranges.leases.values() {
                if let Some(record) = st.leases.get_mut(&lease.catalog_id) {
                    record.state = if lease.state == "Active" {
                        if lease.mode == "w" {
                            "Writing"
                        } else {
                            "Reading"
                        }
                    } else {
                        lease.state
                    }
                    .into();
                }
            }
        }
    }
    pub fn refresh(&mut self, st: &mut Catalog) {
        for pool in self.pools.values_mut() {
            pool.refresh();
        }
        self.sync_leases(st);
    }
    pub fn snapshot(&self, st: &Catalog) -> Value {
        json!({"schema":1,"scope":"this managed daemon incarnation; no inherited-domain recovery",
            "revision":self.revision,"charged_bytes":st.ddr_resources.charged_bytes(),
            "creation_reserved_bytes":st.ddr_resources.reserved_bytes(),"budget_bytes":st.ddr_resources.limit,
            "operation_records":self.operations.len(),"control_counts":self.counts,"native_jobs_executed":0,"payload_rpc_bytes":0,
            "pools":self.pools.values().map(|pool| {
                let mut value=pool.description();
                value["frozen"]=json!(pool.frozen()); value["ranges"]=pool.ranges.snapshot(); value["last_error"]=json!(pool.last_error);
                value["clients"]=json!(pool.clients.iter().map(|(id,client)|json!({"client_session":id,"access_generation":client.access_id,
                    "birth":birth_json(&client.birth),"state":client.state,"closing":client.closing,"access":client.access,
                    "children":client.children.values().map(|child|json!({"birth":birth_json(&child.birth),"unknown":child.unknown})).collect::<Vec<_>>()
                })).collect::<Vec<_>>());
                value["release_result"]=pool.release_result.clone(); value
            }).collect::<Vec<_>>()})
    }

    pub fn create_pending(
        &mut self,
        st: &mut Catalog,
        id: String,
        epoch: String,
        provider_session: String,
        nbytes: u64,
        reservation: &str,
    ) -> Result<Value, String> {
        if self.pools.len() >= 8 || nbytes == 0 || nbytes % 65536 != 0 || nbytes > 256 * 1024 * 1024
        {
            return Err("bounded aligned pool size required".into());
        }
        st.ddr_resources.reserve(reservation, nbytes)?;
        let object = st.take_next_object_id();
        let placement = st.take_next_placement_id();
        let record = Object {
            object_id: object,
            placement_id: placement,
            block_id: 0,
            placement: "ddr".into(),
            target: "host:managed".into(),
            ddr_fd: -1,
            requested_bytes: nbytes,
            actual_bytes: 0,
            state: "Creating".into(),
            name: id.clone(),
            role: "managed_pool".into(),
            ddr_fast_profile: "posix_shm_simpler".into(),
            ..Object::default()
        };
        st.record_legacy_object(&record);
        st.objects.insert(object, record);
        let binding = st
            .resource_bindings
            .values()
            .find(|b| b.placement_id == placement)
            .unwrap()
            .binding_id;
        self.pools.insert(
            id.clone(),
            Pool {
                id,
                epoch,
                provider_session,
                object,
                placement,
                binding,
                nbytes,
                state: "Creating",
                canonical: Value::Null,
                file: None,
                resource: None,
                descriptor: None,
                ranges: Ranges::default(),
                clients: BTreeMap::new(),
                last_error: String::new(),
                release_result: Value::Null,
            },
        );
        Ok(json!({"object_id":object,"placement_id":placement,"binding_id":binding}))
    }

    pub fn metadata(
        &mut self,
        st: &mut Catalog,
        peer: &Peer,
        request: &Value,
    ) -> Result<Value, String> {
        let op = text(request, "op")?;
        let id = text(request, "pool_id")?;
        let pool = self.pools.get_mut(id).ok_or("pool not found")?;
        if request["pool_epoch"] != pool.epoch
            || request
                .get("provider_session")
                .is_some_and(|session| session != &pool.provider_session)
        {
            return Err("stale pool/provider incarnation".into());
        }
        if op == "DescribeManagedPool" {
            return Ok(pool.description());
        }
        if op == "GetManagedPoolSnapshot" {
            if let Some(id) = request.get("client_session").and_then(Value::as_str) {
                let session = pool
                    .clients
                    .get(id)
                    .ok_or("snapshot client session not found")?;
                if session.birth != peer.birth || request["access_generation"] != session.access_id
                {
                    return Err("stale snapshot client".into());
                }
            }
            return Ok(self.snapshot(st));
        }
        if op == "RegisterPoolClient" {
            pool.admission()?;
            let access = text(request, "access")?;
            if !matches!(access, "r" | "rw")
                || pool.clients.len() >= 512
                || pool
                    .clients
                    .values()
                    .filter(|c| c.state != "Unmapped")
                    .count()
                    >= 32
            {
                return Err("client access/metadata limit".into());
            }
            let session = opaque_id()?;
            let catalog_id = st.take_next_client_id();
            let access_id = st.take_next_access_binding_id();
            pool.clients.insert(
                session.clone(),
                Session {
                    catalog_id,
                    access_id,
                    birth: peer.birth.clone(),
                    pidfd: Some(peer.pidfd.try_clone().map_err(|e| e.to_string())?),
                    access: access.into(),
                    state: "MappingPending",
                    closing: false,
                    children: BTreeMap::new(),
                },
            );
            st.clients.insert(
                catalog_id,
                Client {
                    client_id: catalog_id,
                    role: "managed_pool".into(),
                    os_pid: peer.birth.pid as i64,
                    bare_tgid: 0,
                    device_id: -1,
                },
            );
            st.access_bindings.insert(
                access_id,
                AccessBindingRecord {
                    access_binding_id: access_id,
                    placement_id: pool.placement,
                    source_binding_id: pool.binding,
                    accessor_node_id: "local".into(),
                    accessor_device_id: None,
                    accessor_provider_id: "posix_shm_simpler".into(),
                    accessor_provider_session_id: session.clone(),
                    accessor_resource_id: format!("mapped:{access_id}"),
                    access_binding_generation: access_id,
                    source_binding_generation: 1,
                    access_kind: AccessKind::Local,
                    transport_kind: TransportKind::Local,
                    addressability: Addressability::LocallyAddressable,
                    access_mode: access.into(),
                    allowed_offset_bytes: 0,
                    allowed_bytes: pool.nbytes,
                    topology_generation: 1,
                    capability_generation: 1,
                    opaque_token: Vec::new(),
                    state: "MappingPending".into(),
                },
            );
            let mut descriptor = pool.description();
            descriptor["client_session"] = json!(session);
            descriptor["access_generation"] = json!(access_id);
            descriptor["access"] = json!(access);
            return Ok(descriptor);
        }
        let client_id = text(request, "client_session")?;
        let session = pool
            .clients
            .get(client_id)
            .ok_or("client session not found")?;
        if session.birth != peer.birth || request["access_generation"] != session.access_id {
            return Err("stale client birth/access identity".into());
        }
        let catalog_client = session.catalog_id;
        let access_id = session.access_id;
        let admitted = session.state == "Mapped" && !session.closing;
        match op {
            "AckPoolMapping" => {
                if !matches!(session.state, "MappingPending" | "Mapped")
                    || session.closing
                    || request["dev"] != pool.canonical["dev"]
                    || request["inode"] != pool.canonical["inode"]
                    || request["nbytes"] != pool.nbytes
                    || request["access"] != session.access
                {
                    return Err("mapping ACK identity mismatch".into());
                }
                pool.clients.get_mut(client_id).unwrap().state = "Mapped";
                st.access_bindings.get_mut(&access_id).unwrap().state = "Mapped".into();
                Ok(json!({"mapped":true}))
            }
            "AllocatePoolRange" => {
                pool.admission()?;
                if !admitted || session.access != "rw" {
                    return Err("mapped writable client required".into());
                }
                let id = st.take_next_block_id();
                let block = pool.ranges.allocate(
                    id,
                    number(request, "nbytes")?,
                    number(request, "alignment")?,
                    pool.nbytes,
                )?;
                Ok(pool.block_json(block.json()))
            }
            "AcquirePoolRange" => {
                pool.admission()?;
                if !admitted {
                    return Err("registered mapped access required".into());
                }
                let block = request.get("block").ok_or("allocation required")?;
                fields(
                    block,
                    &[
                        "pool_id",
                        "pool_epoch",
                        "provider_session",
                        "block_id",
                        "generation",
                        "offset",
                        "nbytes",
                    ],
                    &[],
                )?;
                if block["pool_id"] != pool.id
                    || block["pool_epoch"] != pool.epoch
                    || block["provider_session"] != pool.provider_session
                {
                    return Err("stale allocation pool tuple".into());
                }
                let bid = number(block, "block_id")?;
                if pool
                    .ranges
                    .blocks
                    .get(&bid)
                    .is_none_or(|stored| pool.block_json(stored.json()) != *block)
                {
                    return Err("stale allocation identity".into());
                }
                let mode = text(request, "mode")?;
                if mode == "w" && session.access != "rw" {
                    return Err("read-only access cannot write".into());
                }
                let lid = st.take_next_lease_id();
                let lease = pool.ranges.acquire(
                    opaque_id()?,
                    lid,
                    client_id.into(),
                    bid,
                    number(request, "expected_generation")?,
                    number(request, "byte_offset")?,
                    number(request, "nbytes")?,
                    mode,
                )?;
                st.leases.insert(
                    lid,
                    Lease {
                        lease_id: lid,
                        object_id: pool.object,
                        block_id: bid,
                        client_id: catalog_client,
                        state: if mode == "w" { "Writing" } else { "Reading" }.into(),
                        allowed_offset_bytes: lease.offset,
                        allowed_bytes: lease.nbytes,
                    },
                );
                Ok(pool.lease_json(&lease, access_id))
            }
            "CommitPoolWrite" => {
                pool.admission()?;
                if !admitted {
                    return Err("mapped live writer required".into());
                }
                let lease = pool.validate_lease(&request["lease"], client_id, access_id)?;
                let version = pool.ranges.commit(
                    &lease,
                    text(request, "content_id")?,
                    number(request, "nbytes")?,
                )?;
                st.leases.get_mut(&lease.catalog_id).unwrap().state = "Closed".into();
                Ok(
                    json!({"committed":true,"content_version":version,"content_id":request["content_id"],"lease_id":lease.token}),
                )
            }
            "ReleasePoolLease" => {
                let lease = pool.validate_lease(&request["lease"], client_id, access_id)?;
                if lease.mode == "w" && request["write_not_started"] != true {
                    return Err("write release requires explicit never-started assertion".into());
                }
                pool.ranges.release(&lease)?;
                st.leases.get_mut(&lease.catalog_id).unwrap().state = "Closed".into();
                Ok(json!({"released":true,"lease_id":lease.token}))
            }
            "ReportPoolUnknown" => {
                if session.state == "Unmapped" {
                    return Err("access already unmapped".into());
                }
                text(request, "reason")?;
                if let Some(value) = request.get("lease") {
                    let lease = pool.validate_lease(value, client_id, access_id)?;
                    pool.ranges.unknown(&lease);
                    st.leases.get_mut(&lease.catalog_id).unwrap().state =
                        pool.ranges.leases[&lease.token].state.into();
                } else {
                    pool.clients.get_mut(client_id).unwrap().state = "Unknown";
                    st.access_bindings.get_mut(&access_id).unwrap().state = "Unknown".into();
                }
                Ok(json!({"unknown_retained":true,"pool_frozen":pool.frozen()}))
            }
            "BeginPoolUnregister" => {
                if session.state == "Unmapped" {
                    return Ok(json!({"stopping":true}));
                }
                pool.clients.get_mut(client_id).unwrap().closing = true;
                Ok(json!({"stopping":true}))
            }
            "AckPoolUnmapped" => {
                if !session.closing
                    || !session.children.is_empty()
                    || request["unmapped"] != true
                    || request["fd_closed"] != true
                {
                    return Err("explicit unmap/FD close and drained children required".into());
                }
                if pool
                    .ranges
                    .leases
                    .values()
                    .any(|l| l.client == client_id && l.state == "Active")
                {
                    return Err("active range obligations remain".into());
                }
                let unknown = session.state == "Unknown"
                    || pool
                        .ranges
                        .leases
                        .values()
                        .any(|l| l.client == client_id && l.state == "Unknown");
                if unknown && request["native_drained"] != true {
                    return Err(
                        "unknown requires explicit actual native drain and unmap, not timeout/PID"
                            .into(),
                    );
                }
                pool.ranges.reconcile_unmapped(client_id);
                let session = pool.clients.get_mut(client_id).unwrap();
                session.state = "Unmapped";
                session.pidfd = None;
                st.access_bindings.get_mut(&access_id).unwrap().state = "Closed".into();
                st.clients.remove(&catalog_client);
                for lease in pool
                    .ranges
                    .leases
                    .values()
                    .filter(|l| l.client == client_id)
                {
                    st.leases.get_mut(&lease.catalog_id).unwrap().state = lease.state.into();
                }
                Ok(json!({"unmapped":true,"unknown_content_invalid_not_committed":unknown}))
            }
            "AttachPoolChild" => {
                pool.admission()?;
                if !admitted || session.children.len() >= 64 {
                    return Err("live mapped parent required".into());
                }
                let pid = number(request, "pid")?;
                let pid = i32::try_from(pid).map_err(|_| "child PID overflow")?;
                let first =
                    crate::ddr_process_identity::observe(pid, peer.birth.uid, peer.birth.gid)
                        .map_err(|e| e.to_string())?;
                if birth_json(&first) != request["birth"] || !descendant(pid, peer.birth.pid) {
                    return Err("child birth/ancestry mismatch".into());
                }
                let fd = pidfd(pid)?;
                let second =
                    crate::ddr_process_identity::observe(pid, peer.birth.uid, peer.birth.gid)
                        .map_err(|e| e.to_string())?;
                if first != second || exited(&fd) {
                    return Err("child changed while attaching".into());
                }
                let children = &mut pool.clients.get_mut(client_id).unwrap().children;
                if children.contains_key(&pid) {
                    return Err("child already attached; query same operation".into());
                }
                children.insert(
                    pid,
                    ChildView {
                        birth: first,
                        pidfd: fd,
                        unknown: false,
                    },
                );
                Ok(json!({"attached":true}))
            }
            "DetachPoolChild" => {
                let pid =
                    i32::try_from(number(request, "pid")?).map_err(|_| "child PID overflow")?;
                let child = session.children.get(&pid).ok_or("child not attached")?;
                if request["native_drained"] != true
                    || request["birth"] != birth_json(&child.birth)
                    || !exited(&child.pidfd)
                {
                    return Err("child exit alone is insufficient; explicit native-drain and matching birth required".into());
                }
                pool.clients
                    .get_mut(client_id)
                    .unwrap()
                    .children
                    .remove(&pid);
                Ok(json!({"detached":true,"native_drain_attested":true}))
            }
            _ => Err("unknown metadata operation".into()),
        }
    }
}

fn descendant(mut child: i32, parent: i32) -> bool {
    for _ in 0..16 {
        let Ok(status) = std::fs::read_to_string(format!("/proc/{child}/status")) else {
            return false;
        };
        let Some(ppid) = status
            .lines()
            .find_map(|line| line.strip_prefix("PPid:"))
            .and_then(|v| v.trim().parse::<i32>().ok())
        else {
            return false;
        };
        if ppid == parent {
            return true;
        }
        if ppid <= 1 || ppid == child {
            return false;
        }
        child = ppid;
    }
    false
}
