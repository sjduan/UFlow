//! Physical, cooperative external DDR registration and FD broker.
//! The catalog remains authoritative; an AccessBinding is not another placement.
use crate::catalog::{Catalog, Client, Object, SharedCatalog};
use std::collections::HashMap;
use std::fs::{self, File, Permissions};
use std::io::{self, Read, Write};
use std::mem;
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uf_core::idl::AccessBindingRecord;
use uf_core::provider::{AccessKind, Addressability, TransportKind};
use uf_core::{decode, encode, err, get, ok, Kv};

const MAX_FRAME: usize = 8192;
const DEFAULT_OWNER_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_TOTAL_BYTES: u64 = 64 * 1024 * 1024;
// Tombstones preserve idempotency and stale-identity rejection for this daemon
// incarnation. Bound them rather than silently growing without limit.
const MAX_BACKING_RECORDS: usize = 512;
const MAX_VIEW_RECORDS: usize = 8192;
const MAX_SESSION_RECORDS: usize = 2048;

fn identity_file(path: &str) -> String {
    fs::read_to_string(path)
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn fresh_uuid() -> Result<String, String> {
    let id = identity_file("/proc/sys/kernel/random/uuid");
    if id.len() == 36 && id.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-') {
        Ok(id)
    } else {
        Err("kernel random UUID unavailable".to_string())
    }
}

fn number(req: &Kv, key: &str) -> Result<u64, String> {
    get(req, key)
        .parse()
        .map_err(|_| format!("invalid {}", key))
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 96
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}

struct Session {
    pid: i32,
    uid: u32,
    incarnation: String,
    pool_generation: u64,
    pidfd: Option<File>,
    alive: bool,
    // An identity is durable only after at least one handoff commits.
    committed: bool,
}

struct Backing {
    object_id: u64,
    placement_id: u64,
    binding_id: u64,
    owner_client_id: u64,
    owner_lease_id: u64,
    owner_name: String,
    resource_generation: u64,
    bytes: u64,
    dev: u64,
    ino: u64,
    state: &'static str,
    file: Option<File>,
}

struct View {
    object_id: u64,
    client_id: u64,
    lease_id: u64,
    offset: u64,
    bytes: u64,
    pending: bool,
    released: bool,
}

#[derive(Default)]
pub struct ExternalDdrState {
    host_id: String,
    boot_id: String,
    broker_instance: String,
    epoch: u64,
    next_generation: u64,
    sessions: HashMap<u64, Session>,
    current_pid: HashMap<i32, u64>,
    backings: HashMap<u64, Backing>,
    views: HashMap<u64, View>,
    owner_limit: u64,
    total_limit: u64,
}

#[derive(Clone, Copy)]
struct Peer {
    pid: i32,
    uid: u32,
}

enum Undo {
    View(u64),
    Registration(u64),
}

struct Reply {
    fields: Kv,
    fd: Option<File>,
    undo: Option<Undo>,
}

impl Reply {
    fn plain(fields: Kv) -> Self {
        Self {
            fields,
            fd: None,
            undo: None,
        }
    }
}

impl ExternalDdrState {
    pub fn new() -> Self {
        Self {
            // A container's /etc/machine-id can differ from the physical host.
            // Require a trusted host-scoped value instead of inventing one.
            host_id: std::env::var("UF_EXTERNAL_DDR_HOST_ID")
                .ok()
                .filter(|v| valid_name(v))
                .unwrap_or_default(),
            boot_id: identity_file("/proc/sys/kernel/random/boot_id"),
            broker_instance: fresh_uuid().unwrap_or_default(),
            next_generation: 1,
            owner_limit: std::env::var("UF_EXTERNAL_DDR_OWNER_MAX_BYTES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_OWNER_BYTES),
            total_limit: std::env::var("UF_EXTERNAL_DDR_TOTAL_MAX_BYTES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_TOTAL_BYTES),
            ..Self::default()
        }
    }

    pub fn contains(&self, object_id: u64) -> bool {
        self.backings.contains_key(&object_id)
    }

    pub fn configured(&self) -> bool {
        !self.host_id.is_empty() && !self.boot_id.is_empty() && !self.broker_instance.is_empty()
    }

    pub fn pidfd_observed(&self) -> bool {
        !self.sessions.is_empty()
    }

    pub fn committed_bytes(&self) -> u64 {
        self.backings
            .values()
            .filter(|b| b.file.is_some())
            .map(|b| b.bytes)
            .sum()
    }

    pub fn owns_lease(&self, lease_id: u64) -> bool {
        self.backings.values().any(|b| b.owner_lease_id == lease_id)
            || self.views.values().any(|v| v.lease_id == lease_id)
    }

    pub fn owns_view(&self, view_id: u64) -> bool {
        self.views.contains_key(&view_id)
    }

    fn generation(&mut self) -> u64 {
        let result = self.next_generation;
        self.next_generation += 1;
        result
    }

    fn session(&mut self, st: &mut Catalog, peer: Peer) -> Result<u64, String> {
        if let Some(id) = self.current_pid.get(&peer.pid).copied() {
            if self
                .sessions
                .get(&id)
                .is_some_and(|s| s.alive && s.uid == peer.uid)
            {
                return Ok(id);
            }
        }
        if self.sessions.len() >= MAX_SESSION_RECORDS {
            return Err("external DDR session metadata limit reached".to_string());
        }
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, peer.pid, 0) } as i32;
        if fd < 0 {
            return Err(format!("pidfd_open failed: {}", io::Error::last_os_error()));
        }
        let pidfd = unsafe { File::from_raw_fd(fd) };
        let incarnation = fresh_uuid()?;
        let pool_generation = self.generation();
        let client_id = st.take_next_client_id();
        st.clients.insert(
            client_id,
            Client {
                client_id,
                role: "external_ddr".to_string(),
                os_pid: peer.pid as i64,
                bare_tgid: 0,
                device_id: st.device,
            },
        );
        self.sessions.insert(
            client_id,
            Session {
                pid: peer.pid,
                uid: peer.uid,
                incarnation,
                pool_generation,
                pidfd: Some(pidfd),
                alive: true,
                committed: false,
            },
        );
        self.current_pid.insert(peer.pid, client_id);
        self.epoch += 1;
        Ok(client_id)
    }

    // The catalog lock serializes this check with other requests and handoffs.
    // Never detach an established session or one with a concurrent pending
    // registration/view. Rolled-back records for an uncommitted session can be
    // discarded without reusing their IDs; committed tombstones remain intact.
    fn discard_uncommitted_session(&mut self, st: &mut Catalog, client_id: u64) {
        let Some(session) = self.sessions.get(&client_id) else {
            return;
        };
        if session.committed
            || self.backings.values().any(|b| {
                b.owner_client_id == client_id
                    && (b.state == "active" || b.state == "pending" || b.file.is_some())
            })
            || self
                .views
                .values()
                .any(|v| v.client_id == client_id && !v.released)
        {
            return;
        }
        let views: Vec<u64> = self
            .views
            .iter()
            .filter(|(_, v)| v.client_id == client_id)
            .map(|(id, _)| *id)
            .collect();
        for id in views {
            if let Some(view) = self.views.remove(&id) {
                st.access_bindings.remove(&id);
                st.leases.remove(&view.lease_id);
            }
        }
        let backings: Vec<u64> = self
            .backings
            .values()
            .filter(|b| b.owner_client_id == client_id)
            .map(|b| b.object_id)
            .collect();
        for id in backings {
            if let Some(backing) = self.backings.remove(&id) {
                st.objects.remove(&id);
                st.data_objects.remove(&id);
                st.placements.remove(&backing.placement_id);
                st.resource_bindings.remove(&backing.binding_id);
                st.leases.remove(&backing.owner_lease_id);
            }
        }
        if let Some(session) = self.sessions.remove(&client_id) {
            if self.current_pid.get(&session.pid) == Some(&client_id) {
                self.current_pid.remove(&session.pid);
            }
            st.clients.remove(&client_id);
            self.epoch += 1;
        }
    }

    fn session_dead(session: &Session) -> Result<bool, String> {
        let pidfd = session.pidfd.as_ref().ok_or("live session has no pidfd")?;
        let mut p = libc::pollfd {
            fd: pidfd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let rc = unsafe { libc::poll(&mut p, 1, 0) };
        if rc < 0 {
            return Err(format!("pidfd poll failed: {}", io::Error::last_os_error()));
        }
        Ok(rc > 0 && p.revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) != 0)
    }

    fn close_view(&mut self, st: &mut Catalog, view_id: u64) {
        if let Some(view) = self.views.get_mut(&view_id) {
            if view.released {
                return;
            }
            view.released = true;
            view.pending = false;
            if let Some(lease) = st.leases.get_mut(&view.lease_id) {
                lease.state = "Closed".to_string();
            }
            if let Some(access) = st.access_bindings.get_mut(&view_id) {
                access.state = "Released".to_string();
            }
            self.epoch += 1;
        }
    }

    fn retire(&mut self, st: &mut Catalog, object_id: u64, state: &'static str) {
        if let Some(backing) = self.backings.get_mut(&object_id) {
            if backing.state != "active" && backing.state != "pending" {
                return;
            }
            backing.state = state;
            if let Some(lease) = st.leases.get_mut(&backing.owner_lease_id) {
                lease.state = "Closed".to_string();
            }
            if let Some(object) = st.objects.get_mut(&object_id) {
                object.state = "Retiring".to_string();
            }
            if let Some(object) = st.data_objects.get_mut(&object_id) {
                object.state = "Retiring".to_string();
            }
            if let Some(placement) = st.placements.get_mut(&backing.placement_id) {
                placement.state = "Retiring".to_string();
            }
            if let Some(binding) = st.resource_bindings.get_mut(&backing.binding_id) {
                binding.state = "Retiring".to_string();
            }
            self.epoch += 1;
        }
    }

    fn collect_retired(&mut self, st: &mut Catalog) {
        let ready: Vec<u64> = self
            .backings
            .values()
            .filter(|b| {
                b.state != "active"
                    && b.state != "pending"
                    && b.file.is_some()
                    && !self
                        .views
                        .values()
                        .any(|v| v.object_id == b.object_id && !v.released)
            })
            .map(|b| b.object_id)
            .collect();
        for id in ready {
            if let Some(backing) = self.backings.get_mut(&id) {
                backing.file = None;
            }
            st.objects.remove(&id);
            st.mark_object_released(id);
            self.epoch += 1;
        }
    }

    fn refresh(&mut self, st: &mut Catalog) -> Result<(), String> {
        let mut dead = Vec::new();
        for (id, session) in &self.sessions {
            if session.alive && Self::session_dead(session)? {
                dead.push(*id);
            }
        }
        for id in dead {
            if let Some(session) = self.sessions.get_mut(&id) {
                session.alive = false;
                session.pidfd = None;
                if self.current_pid.get(&session.pid) == Some(&id) {
                    self.current_pid.remove(&session.pid);
                }
            }
            st.clients.remove(&id);
            self.epoch += 1;
            let owned: Vec<u64> = self
                .backings
                .values()
                .filter(|b| {
                    b.owner_client_id == id && (b.state == "active" || b.state == "pending")
                })
                .map(|b| b.object_id)
                .collect();
            for object_id in owned {
                self.retire(st, object_id, "owner_exited");
            }
            let held: Vec<u64> = self
                .views
                .iter()
                .filter(|(_, v)| v.client_id == id && !v.released)
                .map(|(id, _)| *id)
                .collect();
            for view_id in held {
                self.close_view(st, view_id);
            }
        }
        self.collect_retired(st);
        Ok(())
    }

    fn descriptor(&self, backing: &Backing) -> Kv {
        let owner = &self.sessions[&backing.owner_client_id];
        ok(&[
            ("host_id", self.host_id.clone()),
            ("boot_id", self.boot_id.clone()),
            ("broker_instance", self.broker_instance.clone()),
            ("owner_incarnation", owner.incarnation.clone()),
            ("owner_name", backing.owner_name.clone()),
            ("pool_generation", owner.pool_generation.to_string()),
            (
                "resource_generation",
                backing.resource_generation.to_string(),
            ),
            ("object_id", backing.object_id.to_string()),
            ("placement_id", backing.placement_id.to_string()),
            ("binding_id", backing.binding_id.to_string()),
            ("owner_lease_id", backing.owner_lease_id.to_string()),
            ("size_bytes", backing.bytes.to_string()),
        ])
    }

    fn validate_descriptor(&self, req: &Kv, backing: &Backing) -> Result<(), String> {
        let owner = &self.sessions[&backing.owner_client_id];
        if get(req, "host_id") != self.host_id
            || get(req, "boot_id") != self.boot_id
            || get(req, "broker_instance") != self.broker_instance
            || get(req, "owner_incarnation") != owner.incarnation
            || number(req, "pool_generation")? != owner.pool_generation
            || number(req, "resource_generation")? != backing.resource_generation
        {
            return Err("stale external DDR identity or generation".to_string());
        }
        Ok(())
    }

    fn validate_view_identity(&self, req: &Kv, peer: Peer) -> Result<(), String> {
        // Reject an old broker before even looking up a colliding numeric ID.
        if get(req, "host_id") != self.host_id
            || get(req, "boot_id") != self.boot_id
            || get(req, "broker_instance") != self.broker_instance
        {
            return Err("stale external DDR view identity".to_string());
        }
        let view = self
            .views
            .get(&number(req, "view_id")?)
            .ok_or("view not found")?;
        let backing = self
            .backings
            .get(&view.object_id)
            .ok_or("backing not found")?;
        self.validate_descriptor(req, backing)?;
        let accessor = self
            .sessions
            .get(&view.client_id)
            .ok_or("accessor not found")?;
        if number(req, "object_id")? != view.object_id
            || number(req, "lease_id")? != view.lease_id
            || get(req, "accessor_address_domain") != accessor.incarnation
            || !accessor.alive
            || accessor.pid != peer.pid
            || accessor.uid != peer.uid
            || self.current_pid.get(&peer.pid) != Some(&view.client_id)
        {
            return Err("stale or foreign external DDR view identity".to_string());
        }
        Ok(())
    }

    fn register(
        &mut self,
        st: &mut Catalog,
        client_id: u64,
        req: &Kv,
        file: File,
    ) -> Result<Reply, String> {
        if self.host_id.is_empty() || self.boot_id.is_empty() || self.broker_instance.is_empty() {
            return Err("host, boot, or broker identity unavailable".to_string());
        }
        if self.backings.len() >= MAX_BACKING_RECORDS {
            return Err("external DDR backing metadata limit reached".to_string());
        }
        let name = get(req, "name");
        let owner_name = get(req, "owner_name");
        if !valid_name(name) || !valid_name(owner_name) {
            return Err("invalid name or owner_name".to_string());
        }
        let bytes = number(req, "size_bytes")?;
        if bytes == 0 {
            return Err("size_bytes must be positive".to_string());
        }
        let mut stat: libc::stat = unsafe { mem::zeroed() };
        if unsafe { libc::fstat(file.as_raw_fd(), &mut stat) } != 0 {
            return Err("fstat failed".to_string());
        }
        if stat.st_mode & libc::S_IFMT != libc::S_IFREG
            || stat.st_size < 0
            || stat.st_size as u64 != bytes
        {
            return Err("FD must be one complete regular backing of size_bytes".to_string());
        }
        let path = format!("/proc/self/fd/{}", file.as_raw_fd());
        let target = fs::read_link(&path).map_err(|e| format!("inspect FD: {}", e))?;
        if !target.to_string_lossy().starts_with("/memfd:") {
            return Err("FD is not a memfd".to_string());
        }
        let seals = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GET_SEALS) };
        if seals < 0
            || seals & (libc::F_SEAL_GROW | libc::F_SEAL_SHRINK)
                != libc::F_SEAL_GROW | libc::F_SEAL_SHRINK
        {
            return Err("memfd must be sealed against grow and shrink".to_string());
        }
        if self
            .backings
            .values()
            .any(|b| b.file.is_some() && b.dev == stat.st_dev as u64 && b.ino == stat.st_ino as u64)
        {
            return Err("backing already registered".to_string());
        }
        let total: u64 = self
            .backings
            .values()
            .filter(|b| b.file.is_some())
            .map(|b| b.bytes)
            .sum();
        let owner_total: u64 = self
            .backings
            .values()
            .filter(|b| b.file.is_some() && b.owner_client_id == client_id)
            .map(|b| b.bytes)
            .sum();
        if total
            .checked_add(bytes)
            .is_none_or(|v| v > self.total_limit)
            || owner_total
                .checked_add(bytes)
                .is_none_or(|v| v > self.owner_limit)
        {
            return Err("external DDR quota exceeded".to_string());
        }
        // Reopen read-only: the daemon never calls the create-path pre-touch, which writes zero.
        let readonly = File::open(&path).map_err(|e| format!("open read-only backing: {}", e))?;
        let generation = self.generation();
        let object_id = st.take_next_object_id();
        let placement_id = st.take_next_placement_id();
        let owner = &self.sessions[&client_id];
        let object = Object {
            object_id,
            placement_id,
            block_id: 0,
            placement: "ddr".to_string(),
            target: "host:unknown".to_string(),
            ddr_path: String::new(),
            ddr_fd: -1,
            ddr_service_ptr: 0,
            ddr_service_len: 0,
            requested_bytes: bytes,
            actual_bytes: bytes,
            state: "Creating".to_string(),
            creator_client_id: client_id,
            modified_offset_bytes: 0,
            modified_bytes: 0,
            model_id: String::new(),
            name: name.to_string(),
            role: "user".to_string(),
            shape: String::new(),
            dtype: String::new(),
            immutable: true,
            ddr_fast_profile: "external_memfd".to_string(),
            ddr_madvise_hugepage: false,
            ddr_pretouched: false,
            ddr_prepare_us: 0.0,
            ddr_madvise_us: 0.0,
            ddr_pretouch_us: 0.0,
            ddr_fallback_reason: String::new(),
            ssd_path: String::new(),
            ssd_offset_bytes: 0,
            ssd_alignment_bytes: 0,
            ssd_io_mode: String::new(),
            ssd_file_owned: false,
            ssd_preallocated: false,
        };
        st.objects.insert(object_id, object.clone());
        st.record_legacy_object(&object);
        if let Some(record) = st.data_objects.get_mut(&object_id) {
            record.owner = owner.incarnation.clone();
        }
        if let Some(placement) = st.placements.get_mut(&placement_id) {
            placement.node_id = self.host_id.clone();
            placement.domain = "process".to_string();
            placement.address_kind = "scm_rights_fd".to_string();
            placement.topology_generation = self.epoch + 1;
            placement.capability_generation = 0;
        }
        let binding_id = st
            .resource_bindings
            .values()
            .find(|b| b.placement_id == placement_id)
            .expect("legacy binding inserted")
            .binding_id;
        if let Some(binding) = st.resource_bindings.get_mut(&binding_id) {
            binding.provider_session_id = owner.incarnation.clone();
            binding.provider_resource_id = format!("external:{}:{}", object_id, generation);
            binding.binding_generation = generation;
            binding.opaque_descriptor.clear();
            binding.limitations = vec![
                "cooperative-immutable".to_string(),
                "fd-range-not-os-isolated".to_string(),
                "numa-unknown".to_string(),
            ];
        }
        let lease = st.make_lease(client_id, &object, 0, bytes);
        if let Some(record) = st.leases.get_mut(&lease.lease_id) {
            record.state = "Pending".to_string();
        }
        let backing = Backing {
            object_id,
            placement_id,
            binding_id,
            owner_client_id: client_id,
            owner_lease_id: lease.lease_id,
            owner_name: owner_name.to_string(),
            resource_generation: generation,
            bytes,
            dev: stat.st_dev as u64,
            ino: stat.st_ino as u64,
            state: "pending",
            file: Some(readonly),
        };
        let fields = self.descriptor(&backing);
        self.backings.insert(object_id, backing);
        self.epoch += 1;
        Ok(Reply {
            fields,
            fd: None,
            undo: Some(Undo::Registration(object_id)),
        })
    }

    fn grant_view(
        &mut self,
        st: &mut Catalog,
        client_id: u64,
        object_id: u64,
        offset: u64,
        bytes: u64,
    ) -> Result<Reply, String> {
        if self.views.len() >= MAX_VIEW_RECORDS {
            return Err("external DDR view metadata limit reached".to_string());
        }
        let backing = self.backings.get(&object_id).ok_or("backing not found")?;
        if backing.state != "active" || backing.file.is_none() {
            return Err("backing is not accepting imports".to_string());
        }
        if bytes == 0
            || offset
                .checked_add(bytes)
                .is_none_or(|end| end > backing.bytes)
        {
            return Err("view range out of bounds".to_string());
        }
        let fd = backing
            .file
            .as_ref()
            .unwrap()
            .try_clone()
            .map_err(|e| format!("duplicate FD: {}", e))?;
        let mut fields = self.descriptor(backing);
        let placement_id = backing.placement_id;
        let source_binding_id = backing.binding_id;
        let source_generation = backing.resource_generation;
        let owner_incarnation = self.sessions[&backing.owner_client_id].incarnation.clone();
        let accessor_incarnation = self.sessions[&client_id].incarnation.clone();
        let object = st
            .objects
            .get(&object_id)
            .ok_or("catalog object not found")?
            .clone();
        let lease = st.make_lease(client_id, &object, offset, bytes);
        if let Some(record) = st.leases.get_mut(&lease.lease_id) {
            record.state = "Pending".to_string();
        }
        let view_id = st.take_next_access_binding_id();
        st.access_bindings.insert(
            view_id,
            AccessBindingRecord {
                access_binding_id: view_id,
                placement_id,
                source_binding_id,
                accessor_node_id: self.host_id.clone(),
                accessor_device_id: None,
                accessor_provider_id: "local_ddr".to_string(),
                accessor_provider_session_id: accessor_incarnation.clone(),
                accessor_resource_id: format!("fd-view:{}", view_id),
                access_binding_generation: view_id,
                source_binding_generation: source_generation,
                access_kind: AccessKind::Local,
                transport_kind: TransportKind::Local,
                addressability: Addressability::LocallyAddressable,
                access_mode: "read".to_string(),
                allowed_offset_bytes: offset,
                allowed_bytes: bytes,
                topology_generation: self.epoch + 1,
                capability_generation: 0,
                opaque_token: Vec::new(),
                state: "Creating".to_string(),
            },
        );
        self.views.insert(
            view_id,
            View {
                object_id,
                client_id,
                lease_id: lease.lease_id,
                offset,
                bytes,
                pending: true,
                released: false,
            },
        );
        self.epoch += 1;
        fields.insert("view_id".to_string(), view_id.to_string());
        fields.insert("lease_id".to_string(), lease.lease_id.to_string());
        fields.insert("allowed_offset_bytes".to_string(), offset.to_string());
        fields.insert("allowed_bytes".to_string(), bytes.to_string());
        fields.insert("access_mode".to_string(), "read".to_string());
        fields.insert("source_address_domain".to_string(), owner_incarnation);
        fields.insert("accessor_address_domain".to_string(), accessor_incarnation);
        Ok(Reply {
            fields,
            fd: Some(fd),
            undo: Some(Undo::View(view_id)),
        })
    }

    fn snapshot(&self, st: &Catalog) -> Kv {
        let mut entries: Vec<&Backing> = self.backings.values().collect();
        entries.sort_by_key(|b| b.object_id);
        let mut rows = Vec::new();
        for b in entries {
            let s = &self.sessions[&b.owner_client_id];
            let placement_count = st
                .placements
                .values()
                .filter(|p| p.object_id == b.object_id)
                .count();
            rows.push(format!("{{\"object_id\":{},\"placement_id\":{},\"placement_count\":{},\"binding_id\":{},\"owner_name\":\"{}\",\"owner_incarnation\":\"{}\",\"owner_pid\":{},\"owner_alive\":{},\"pool_generation\":{},\"resource_generation\":{},\"size_bytes\":{},\"state\":\"{}\",\"reclaimed\":{},\"numa_node\":null}}",
                b.object_id, b.placement_id, placement_count, b.binding_id, b.owner_name, s.incarnation, s.pid, s.alive,
                s.pool_generation, b.resource_generation, b.bytes, b.state, b.file.is_none()));
        }
        let committed: u64 = self
            .backings
            .values()
            .filter(|b| b.file.is_some())
            .map(|b| b.bytes)
            .sum();
        let active_views = self
            .views
            .values()
            .filter(|v| !v.released && !v.pending)
            .count();
        let held = self.backings.values().filter(|b| b.file.is_some()).count();
        let pidfds = self.sessions.values().filter(|s| s.pidfd.is_some()).count();
        let owner_leases = self
            .backings
            .values()
            .filter(|b| b.state == "active")
            .count();
        let pending_handoffs = self
            .backings
            .values()
            .filter(|b| b.state == "pending")
            .count()
            + self
                .views
                .values()
                .filter(|v| !v.released && v.pending)
                .count();
        let observed_at_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let projection = format!("{{\"schema\":\"uflow-external-ddr-snapshot-v1\",\"host_id\":\"{}\",\"boot_id\":\"{}\",\"broker_instance\":\"{}\",\"lifecycle_epoch\":{},\"observed_at_unix_ns\":{},\"backings\":[{}]}}",
            self.host_id, self.boot_id, self.broker_instance, self.epoch, observed_at_ns, rows.join(","));
        ok(&[
            ("projection_json", projection),
            ("lifecycle_epoch", self.epoch.to_string()),
            ("committed_bytes", committed.to_string()),
            ("backing_fds", held.to_string()),
            ("session_pidfds", pidfds.to_string()),
            ("owner_leases", owner_leases.to_string()),
            ("active_views", active_views.to_string()),
            ("pending_handoffs", pending_handoffs.to_string()),
            ("in_flight", "0".to_string()),
        ])
    }

    fn handle(
        &mut self,
        st: &mut Catalog,
        peer: Peer,
        req: &Kv,
        incoming: Vec<File>,
    ) -> Result<Reply, String> {
        self.refresh(st)?;
        let op = get(req, "op");
        if op == "RegisterExternalBacking" && incoming.len() != 1 {
            return Err("register requires exactly one FD".to_string());
        }
        if op != "RegisterExternalBacking" && !incoming.is_empty() {
            return Err("unexpected FD".to_string());
        }
        if op == "GetExternalDdrSnapshot" {
            return Ok(Reply::plain(self.snapshot(st)));
        }
        if op == "RetainExternalView" || op == "ReleaseExternalView" {
            self.validate_view_identity(req, peer)?;
        }
        let client_id = self.session(st, peer)?;
        let result = (|| -> Result<Reply, String> {
            match op {
                "RegisterExternalBacking" => {
                    self.register(st, client_id, req, incoming.into_iter().next().unwrap())
                }
                "AcquireExternalRead" => {
                    let object_id = number(req, "object_id")?;
                    let backing = self.backings.get(&object_id).ok_or("backing not found")?;
                    self.validate_descriptor(req, backing)?;
                    if get(req, "access_mode") != "read" {
                        return Err("only read access is supported".to_string());
                    }
                    self.grant_view(
                        st,
                        client_id,
                        object_id,
                        number(req, "offset_bytes")?,
                        number(req, "nbytes")?,
                    )
                }
                "RetainExternalView" => {
                    let view_id = number(req, "view_id")?;
                    let view = self.views.get(&view_id).ok_or("view not found")?;
                    if view.client_id != client_id || view.released || view.pending {
                        return Err("view is not active for this process".to_string());
                    }
                    let relative = number(req, "offset_bytes")?;
                    let bytes = number(req, "nbytes")?;
                    if bytes == 0
                        || relative
                            .checked_add(bytes)
                            .is_none_or(|end| end > view.bytes)
                    {
                        return Err("derived view range out of bounds".to_string());
                    }
                    let object_id = view.object_id;
                    let offset = view
                        .offset
                        .checked_add(relative)
                        .ok_or("view offset overflow")?;
                    self.grant_view(st, client_id, object_id, offset, bytes)
                }
                "ReleaseExternalView" => {
                    let view_id = number(req, "view_id")?;
                    let view = self.views.get(&view_id).ok_or("view not found")?;
                    if view.client_id != client_id {
                        return Err("view belongs to another process".to_string());
                    }
                    self.close_view(st, view_id);
                    self.collect_retired(st);
                    Ok(Reply::plain(ok(&[])))
                }
                "RevokeExternalBacking" => {
                    let object_id = number(req, "object_id")?;
                    let backing = self.backings.get(&object_id).ok_or("backing not found")?;
                    self.validate_descriptor(req, backing)?;
                    if backing.owner_client_id != client_id {
                        return Err("only the owner may revoke".to_string());
                    }
                    self.retire(st, object_id, "revoked");
                    self.collect_retired(st);
                    Ok(Reply::plain(ok(&[])))
                }
                _ => Err("unknown external DDR operation".to_string()),
            }
        })();
        // A provisional reply keeps its session until commit/undo. On other
        // results only detach if no concurrent handoff needs this identity.
        if result.as_ref().map_or(true, |reply| reply.undo.is_none()) {
            self.collect_retired(st);
            self.discard_uncommitted_session(st, client_id);
        }
        result
    }

    fn undo(&mut self, st: &mut Catalog, action: Undo) {
        let client_id = match &action {
            Undo::View(id) => self.views.get(id).map(|view| view.client_id),
            Undo::Registration(id) => self.backings.get(id).map(|backing| backing.owner_client_id),
        };
        match action {
            Undo::View(id) => self.close_view(st, id),
            Undo::Registration(id) => self.retire(st, id, "registration_rollback"),
        }
        self.collect_retired(st);
        if let Some(client_id) = client_id {
            self.discard_uncommitted_session(st, client_id);
        }
    }

    fn commit(&mut self, st: &mut Catalog, action: &Undo) -> Result<(), String> {
        self.refresh(st)?;
        let client_id = match action {
            Undo::Registration(id) => {
                let backing = self.backings.get_mut(id).ok_or("registration vanished")?;
                if backing.state != "pending" || !self.sessions[&backing.owner_client_id].alive {
                    return Err("registration owner is no longer alive".to_string());
                }
                backing.state = "active";
                if let Some(object) = st.objects.get_mut(id) {
                    object.state = "Ready".to_string();
                }
                if let Some(object) = st.data_objects.get_mut(id) {
                    object.state = "Ready".to_string();
                }
                if let Some(placement) = st.placements.get_mut(&backing.placement_id) {
                    placement.state = "Ready".to_string();
                }
                if let Some(binding) = st.resource_bindings.get_mut(&backing.binding_id) {
                    binding.state = "Ready".to_string();
                }
                if let Some(lease) = st.leases.get_mut(&backing.owner_lease_id) {
                    lease.state = "Active".to_string();
                }
                backing.owner_client_id
            }
            Undo::View(id) => {
                let view = self.views.get_mut(id).ok_or("view vanished")?;
                if view.released
                    || !view.pending
                    || !self.sessions[&view.client_id].alive
                    || self
                        .backings
                        .get(&view.object_id)
                        .is_none_or(|b| b.state != "active")
                {
                    return Err("view or owner is no longer active".to_string());
                }
                view.pending = false;
                if let Some(lease) = st.leases.get_mut(&view.lease_id) {
                    lease.state = "Active".to_string();
                }
                if let Some(access) = st.access_bindings.get_mut(id) {
                    access.state = "Ready".to_string();
                }
                view.client_id
            }
        };
        self.sessions
            .get_mut(&client_id)
            .expect("handoff session")
            .committed = true;
        self.epoch += 1;
        Ok(())
    }
}

fn peer_credentials(stream: &UnixStream) -> io::Result<Peer> {
    let mut cred: libc::ucred = unsafe { mem::zeroed() };
    let mut len = mem::size_of::<libc::ucred>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            &mut cred as *mut _ as *mut _,
            &mut len,
        )
    };
    if rc != 0 || len as usize != mem::size_of::<libc::ucred>() {
        return Err(io::Error::last_os_error());
    }
    if cred.uid != unsafe { libc::geteuid() } || cred.pid <= 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "peer UID denied",
        ));
    }
    Ok(Peer {
        pid: cred.pid,
        uid: cred.uid,
    })
}

fn receive(stream: &mut UnixStream) -> io::Result<(String, Vec<File>)> {
    let mut data = [0u8; MAX_FRAME];
    let mut control = [0usize; 64];
    let mut iov = libc::iovec {
        iov_base: data.as_mut_ptr() as *mut _,
        iov_len: data.len(),
    };
    let mut msg: libc::msghdr = unsafe { mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr() as *mut _;
    msg.msg_controllen = mem::size_of_val(&control);
    let count = unsafe { libc::recvmsg(stream.as_raw_fd(), &mut msg, libc::MSG_CMSG_CLOEXEC) };
    if count <= 0 {
        return Err(io::Error::last_os_error());
    }
    let mut files = Vec::new();
    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS {
                let header = libc::CMSG_LEN(0) as usize;
                let payload = ((*cmsg).cmsg_len as usize).saturating_sub(header);
                let count = payload / mem::size_of::<RawFd>();
                for index in 0..count {
                    let raw = std::ptr::read_unaligned(
                        (libc::CMSG_DATA(cmsg) as *const RawFd).add(index),
                    );
                    files.push(File::from_raw_fd(raw));
                }
            }
            cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
        }
    }
    if msg.msg_flags & libc::MSG_CTRUNC != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "truncated ancillary data",
        ));
    }
    let mut bytes = data[..count as usize].to_vec();
    while !bytes.contains(&b'\n') && bytes.len() < MAX_FRAME {
        let mut more = [0u8; 1024];
        let n = stream.read(&mut more)?;
        if n == 0 {
            break;
        }
        bytes.extend_from_slice(&more[..n]);
    }
    if bytes.len() > MAX_FRAME
        || !bytes.ends_with(b"\n")
        || bytes[..bytes.len() - 1].contains(&b'\n')
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid request frame",
        ));
    }
    let line = String::from_utf8(bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid UTF-8"))?;
    Ok((line, files))
}

fn send_reply(stream: &mut UnixStream, fields: &Kv, fd: Option<&File>) -> io::Result<()> {
    let data = encode(fields).into_bytes();
    if let Some(fd) = fd {
        let mut control = [0usize; 8];
        let mut iov = libc::iovec {
            iov_base: data.as_ptr() as *mut _,
            iov_len: data.len(),
        };
        let mut msg: libc::msghdr = unsafe { mem::zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = control.as_mut_ptr() as *mut _;
        msg.msg_controllen = unsafe { libc::CMSG_SPACE(mem::size_of::<RawFd>() as u32) as usize };
        unsafe {
            let cmsg = libc::CMSG_FIRSTHDR(&msg);
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len = libc::CMSG_LEN(mem::size_of::<RawFd>() as u32) as usize;
            std::ptr::write_unaligned(libc::CMSG_DATA(cmsg) as *mut RawFd, fd.as_raw_fd());
        }
        let sent = unsafe { libc::sendmsg(stream.as_raw_fd(), &msg, libc::MSG_NOSIGNAL) };
        if sent <= 0 {
            return Err(io::Error::last_os_error());
        }
        stream.write_all(&data[sent as usize..])
    } else {
        stream.write_all(&data)
    }
}

fn serve(mut stream: UnixStream, shared: SharedCatalog) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    let peer = match peer_credentials(&stream) {
        Ok(peer) => peer,
        Err(_) => return,
    };
    let (line, incoming) = match receive(&mut stream) {
        Ok(v) => v,
        Err(_) => return,
    };
    let req = decode(&line);
    let mut reply = {
        let (lock, _) = &*shared;
        let mut st = lock.lock().unwrap();
        let mut external = mem::take(&mut st.external_ddr);
        let result = external.handle(&mut st, peer, &req, incoming);
        st.external_ddr = external;
        match result {
            Ok(reply) => reply,
            Err(message) => Reply::plain(err(message.replace(';', ",").replace('=', ":"))),
        }
    };
    let sent = send_reply(&mut stream, &reply.fields, reply.fd.as_ref()).is_ok();
    let committed = if sent && reply.undo.is_some() {
        // A registration or new view is provisional until the recipient has
        // received its descriptor/FD and successfully created its mapping.
        let mut ack = [0u8; 3];
        if stream.read_exact(&mut ack).is_ok() && &ack == b"OK\n" {
            let accepted = {
                let (lock, _) = &*shared;
                let mut st = lock.lock().unwrap();
                let mut external = mem::take(&mut st.external_ddr);
                let accepted = external
                    .commit(&mut st, reply.undo.as_ref().unwrap())
                    .is_ok();
                st.external_ddr = external;
                accepted
            };
            if accepted {
                stream.write_all(b"committed\n").is_ok()
            } else {
                let _ = stream.write_all(b"aborted\n");
                false
            }
        } else {
            false
        }
    } else {
        sent
    };
    if !committed {
        if let Some(undo) = reply.undo.take() {
            let (lock, _) = &*shared;
            let mut st = lock.lock().unwrap();
            let mut external = mem::take(&mut st.external_ddr);
            external.undo(&mut st, undo);
            st.external_ddr = external;
        }
    }
}

pub struct Broker {
    stop: Arc<AtomicBool>,
    worker: JoinHandle<()>,
    path: String,
}

impl Broker {
    pub fn stop(self) {
        self.stop.store(true, Ordering::Release);
        let _ = self.worker.join();
        let _ = fs::remove_file(self.path);
    }
}

pub fn start(control_socket: &str, shared: SharedCatalog) -> io::Result<Broker> {
    let path = format!("{}.external-ddr", control_socket);
    let listener = UnixListener::bind(&path)?;
    fs::set_permissions(&path, Permissions::from_mode(0o600))?;
    listener.set_nonblocking(true)?;
    let stop = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&stop);
    let worker = thread::spawn(move || {
        while !flag.load(Ordering::Acquire) {
            match listener.accept() {
                Ok((stream, _)) => {
                    let shared = Arc::clone(&shared);
                    thread::spawn(move || serve(stream, shared));
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5))
                }
                Err(_) => break,
            }
        }
    });
    Ok(Broker { stop, worker, path })
}
