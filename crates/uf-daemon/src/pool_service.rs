//! Host-pool control plane. Provider waits and all socket I/O are outside Catalog.
use crate::catalog::{Catalog, SharedCatalog};
use crate::managed_pool::{Operation, Peer, State};
use crate::pool_provider::{duplicate_access, exited, opaque_id, pidfd, Provider};
use crate::pool_wire::{self, fields, number, text};
use serde_json::{json, Value};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::mem;
use std::os::fd::{AsRawFd, IntoRawFd};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

pub(crate) struct Service {
    shared: SharedCatalog,
    provider: Mutex<Option<Provider>>,
    remote: Option<crate::mem_service_pool::MemServicePoolProvider>,
    provider_session: String,
    provider_loaded: Value,
    root: PathBuf,
    pub(crate) admin: String,
    persisted: Mutex<u64>,
    pub(crate) stop: AtomicBool,
    drop_reply_once: AtomicBool,
    durability_unknown: AtomicBool,
}

fn envelope(id: &str, result: Result<Value, String>, unknown: bool) -> Value {
    match result {
        Ok(value) => json!({"schema":1,"status":"ok","operation_id":id,"result":value}),
        Err(error) => json!({"schema":1,"status":"error","operation_id":id,
                          "code":if unknown {"Unknown"} else {"Rejected"},"detail":error}),
    }
}

fn validate(req: &Value) -> Result<String, String> {
    let op = text(req, "op")?;
    text(req, "operation_id")?;
    if number(req, "schema")? != 1 {
        return Err("managed schema version unsupported".into());
    }
    let mut required = vec!["schema", "op", "operation_id"];
    let mut optional = Vec::new();
    match op {
        "CreateManagedPool" => required.extend(["admin_token", "nbytes"]),
        "DestroyManagedPool" => {
            required.extend(["admin_token", "pool_id", "pool_epoch", "provider_session"])
        }
        "ShutdownManagedPool" => required.push("admin_token"),
        "QueryPoolOperation" => required.push("target_operation_id"),
        "DescribeManagedPool" => {
            required.extend(["pool_id", "pool_epoch"]);
            optional.push("provider_session");
        }
        "GetManagedPoolSnapshot" => {
            required.extend(["pool_id", "pool_epoch", "provider_session"]);
            optional.extend(["client_session", "access_generation"]);
        }
        "RegisterPoolClient" => {
            required.extend(["pool_id", "pool_epoch", "provider_session", "access"])
        }
        _ => {
            required.extend([
                "pool_id",
                "pool_epoch",
                "provider_session",
                "client_session",
                "access_generation",
            ]);
            match op {
                "AckPoolMapping" => required.extend(["dev", "inode", "nbytes", "access"]),
                "AllocatePoolRange" => required.extend(["nbytes", "alignment"]),
                "AcquirePoolRange" => required.extend([
                    "block",
                    "mode",
                    "expected_generation",
                    "byte_offset",
                    "nbytes",
                ]),
                "CommitPoolWrite" => required.extend(["lease", "content_id", "nbytes"]),
                "ReleasePoolLease" => required.extend(["lease", "write_not_started"]),
                "BeginPoolUnregister" => {}
                "AckPoolUnmapped" => required.extend(["unmapped", "fd_closed", "native_drained"]),
                "ReportPoolUnknown" => {
                    required.push("reason");
                    optional.push("lease");
                }
                "AttachPoolChild" => required.extend(["pid", "birth"]),
                "DetachPoolChild" => required.extend(["pid", "birth", "native_drained"]),
                _ => return Err("unsupported control operation; no transfer/native RPC".into()),
            }
        }
    }
    fields(req, &required, &optional)?;
    for key in [
        "pool_id",
        "pool_epoch",
        "provider_session",
        "client_session",
        "target_operation_id",
        "admin_token",
        "access",
        "reason",
        "mode",
        "content_id",
    ] {
        if req.get(key).is_some() {
            text(req, key)?;
        }
    }
    for key in [
        "access_generation",
        "dev",
        "inode",
        "nbytes",
        "alignment",
        "expected_generation",
        "byte_offset",
        "pid",
    ] {
        if req.get(key).is_some() {
            number(req, key)?;
        }
    }
    for key in [
        "unmapped",
        "fd_closed",
        "native_drained",
        "write_not_started",
    ] {
        if req.get(key).is_some_and(|v| !v.is_boolean()) {
            return Err(format!("boolean required: {key}"));
        }
    }
    if req.get("client_session").is_some() != req.get("access_generation").is_some()
        && op == "GetManagedPoolSnapshot"
    {
        return Err("snapshot needs complete client identity or none".into());
    }
    Ok(op.to_owned())
}

fn peer(stream: &UnixStream) -> Result<Peer, String> {
    let mut cred: libc::ucred = unsafe { mem::zeroed() };
    let mut len = mem::size_of::<libc::ucred>() as libc::socklen_t;
    if unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&mut cred as *mut libc::ucred).cast(),
            &mut len,
        )
    } != 0
        || cred.uid != unsafe { libc::geteuid() }
    {
        return Err("same-UID peer required".into());
    }
    let first = crate::ddr_process_identity::observe(cred.pid, cred.uid, cred.gid)
        .map_err(|e| e.to_string())?;
    let fd = pidfd(cred.pid)?;
    let second = crate::ddr_process_identity::observe(cred.pid, cred.uid, cred.gid)
        .map_err(|e| e.to_string())?;
    if first != second || exited(&fd) {
        return Err("peer birth not stable".into());
    }
    Ok(Peer {
        birth: first,
        pidfd: fd,
    })
}

impl Service {
    fn persist(&self, mut value: Value) -> Result<(), String> {
        let revision = value["revision"].as_u64().ok_or("revision missing")?;
        let mut last = self.persisted.lock().map_err(|_| "journal poisoned")?;
        if revision <= *last {
            return Ok(());
        }
        value["provider_loaded"] = self.provider_loaded.clone();
        let next = self.root.join("state-next.json");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&next)
            .map_err(|e| e.to_string())?;
        serde_json::to_writer(&mut file, &value).map_err(|e| e.to_string())?;
        file.write_all(b"\n")
            .and_then(|_| file.sync_all())
            .map_err(|e| e.to_string())?;
        fs::rename(next, self.root.join("state.json")).map_err(|e| e.to_string())?;
        File::open(&self.root)
            .and_then(|dir| dir.sync_all())
            .map_err(|e| e.to_string())?;
        *last = revision;
        Ok(())
    }

    pub(crate) fn catalog<R>(
        &self,
        apply: impl FnOnce(&mut State, &mut Catalog) -> Result<R, String>,
    ) -> Result<R, String> {
        if self.durability_unknown.load(Ordering::Acquire) {
            return Err(
                "durability unknown; obligations retained, restart/recovery forbidden".into(),
            );
        }
        let mut st = self
            .shared
            .0
            .lock()
            .map_err(|_| "Catalog poisoned; commitments unknown")?;
        let mut state = mem::take(&mut st.managed_pools);
        state.refresh(&mut st);
        let result = apply(&mut state, &mut st);
        state.revision = state.revision.checked_add(1).ok_or("revision exhausted")?;
        state.sync_leases(&mut st);
        let mut durable = state.snapshot(&st);
        durable["operations"]=json!(state.operations.iter().map(|(id,op)|json!({"key":id,"request":op.request,"state":op.state,"response":op.response})).collect::<Vec<_>>());
        st.managed_pools = state;
        drop(st);
        if let Err(error) = self.persist(durable) {
            self.durability_unknown.store(true, Ordering::Release);
            return Err(format!("durability unknown; obligations retained: {error}"));
        } // No Catalog lock while writing, syncing or provider I/O.
        result
    }

    fn fd_reply(&self, response: &Value) -> Result<Option<File>, String> {
        let result = &response["result"];
        if !result["client_session"].is_string() || response["status"] != "ok" {
            return Ok(None);
        }
        let st = self.shared.0.lock().map_err(|_| "Catalog poisoned")?;
        let pool = st
            .managed_pools
            .pools
            .get(result["pool_id"].as_str().ok_or("pool missing")?)
            .ok_or("pool not found")?;
        let client = pool
            .clients
            .get(result["client_session"].as_str().unwrap())
            .ok_or("registration disappeared")?;
        // Only a provisional handoff may receive a new FD. An already mapped
        // retry must reuse/query its original local view, not create an uncounted alias.
        if client.state != "MappingPending" {
            return Ok(None);
        }
        duplicate_access(
            pool.file.as_ref().ok_or("provider backing unavailable")?,
            &client.access,
        )
        .map(Some)
    }

    fn control_error(
        &self,
        id: &str,
        key: &str,
        fingerprint: &str,
        error: String,
    ) -> Result<(Value, Option<File>), String> {
        let response = self.catalog(|state, _| {
            let recorded = state.operations.get(key).map(|op| {
                (
                    op.state,
                    op.response
                        .get("result")
                        .and_then(|v| v.get("pool_id"))
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                )
            });
            let unknown = recorded
                .as_ref()
                .is_some_and(|(phase, pool)| *phase != "Pending" || pool.is_some());
            let response = envelope(id, Err(error.clone()), unknown);
            if recorded
                .as_ref()
                .is_none_or(|(phase, _)| *phase == "Pending")
            {
                if let Some((_, Some(pool_id))) = &recorded {
                    if let Some(pool) = state.pools.get_mut(pool_id) {
                        pool.state = if pool.state == "Creating" {
                            "CreationUnknown"
                        } else {
                            "ReleaseUnknown"
                        };
                        pool.last_error = error.clone();
                    }
                }
                state.operations.insert(
                    key.into(),
                    Operation {
                        request: fingerprint.into(),
                        state: if unknown { "Unknown" } else { "Rejected" },
                        response: response.clone(),
                    },
                );
            }
            Ok(response)
        })?;
        Ok((response, None))
    }

    fn handle(&self, peer: &Peer, request: &Value) -> Result<(Value, Option<File>), String> {
        if let Some(remote) = &self.remote {
            return remote.handle(self, peer, request).map(|value| (value, None));
        }
        let op = validate(request)?;
        let id = text(request, "operation_id")?;
        let key = format!("{}:{id}", peer.actor());
        if matches!(
            op.as_str(),
            "CreateManagedPool" | "DestroyManagedPool" | "ShutdownManagedPool"
        ) && request["admin_token"] != self.admin
        {
            return Err(
                "management capability required; serving cannot create/destroy pool".into(),
            );
        }
        if op == "QueryPoolOperation" {
            let target = text(request, "target_operation_id")?;
            let result = self.catalog(|state, _| {
                *state.counts.entry(op.clone()).or_default() += 1;
                Ok(state
                    .operations
                    .get(&format!("{}:{target}", peer.actor()))
                    .map(|op| json!({"state":op.state,"response":op.response}))
                    .unwrap_or(json!({"state":"NotFound","response":null})))
            })?;
            return Ok((envelope(id, Ok(result), false), None));
        }
        let readonly = matches!(
            op.as_str(),
            "GetManagedPoolSnapshot" | "DescribeManagedPool"
        );
        let mut fingerprint = request.clone();
        fingerprint.as_object_mut().unwrap().remove("admin_token");
        let fingerprint = fingerprint.to_string();
        let prior = self.catalog(|state, _| {
            *state.counts.entry(op.clone()).or_default() += 1;
            if readonly {
                return Ok(None);
            }
            if let Some(old) = state.operations.get(&key) {
                if old.request != fingerprint {
                    return Err("operation_id reused with different request".into());
                }
                return Ok(Some(old.response.clone()));
            }
            let admission = matches!(
                op.as_str(),
                "CreateManagedPool"
                    | "RegisterPoolClient"
                    | "AllocatePoolRange"
                    | "AcquirePoolRange"
                    | "AttachPoolChild"
            );
            if state.operations.len() >= 8192 || admission && state.operations.len() >= 4096 {
                return Err("operation metadata bound reached; cleanup headroom retained".into());
            }
            state.operations.insert(
                key.clone(),
                Operation {
                    request: fingerprint.clone(),
                    state: "Pending",
                    response: envelope(
                        id,
                        Err("operation in progress; query same operation_id".into()),
                        true,
                    ),
                },
            );
            Ok(None)
        })?;
        if let Some(response) = prior {
            let fd = self.fd_reply(&response)?;
            return Ok((response, fd));
        }
        if op == "CreateManagedPool" {
            return match self.create(peer, request, &key, &fingerprint) {
                Ok(result) => Ok(result),
                Err(error) => self.control_error(id, &key, &fingerprint, error),
            };
        }
        if op == "DestroyManagedPool" {
            return match self.destroy(request, &key, &fingerprint) {
                Ok(result) => Ok(result),
                Err(error) => self.control_error(id, &key, &fingerprint, error),
            };
        }
        if op == "ShutdownManagedPool" {
            let stopped = self.catalog(|state, st| {
                if state.pools.values().any(|p| p.state != "Released")
                    || st.ddr_resources.charged_bytes() != 0
                    || st.ddr_resources.reserved_bytes() != 0
                {
                    return Err("pool still allocated/unknown; stop forbidden".into());
                }
                st.shutdown_requested = true;
                state.operations.insert(
                    key.clone(),
                    Operation {
                        request: fingerprint.clone(),
                        state: "Completed",
                        response: envelope(id, Ok(json!({"stopping":true})), false),
                    },
                );
                self.stop.store(true, Ordering::Release);
                Ok(())
            });
            if let Err(error) = stopped {
                return self.control_error(id, &key, &fingerprint, error);
            }
            return Ok((envelope(id, Ok(json!({"stopping":true})), false), None));
        }
        let response = self.catalog(|state, st| {
            let admission = matches!(
                op.as_str(),
                "RegisterPoolClient" | "AllocatePoolRange" | "AcquirePoolRange" | "AttachPoolChild"
            );
            let result = if admission && st.shutdown_requested {
                Err("managed daemon stopping".into())
            } else {
                state.metadata(st, peer, request)
            };
            let response = envelope(id, result, false);
            if !readonly {
                state.operations.insert(
                    key,
                    Operation {
                        request: fingerprint,
                        state: if response["status"] == "ok" {
                            "Completed"
                        } else {
                            "Rejected"
                        },
                        response: response.clone(),
                    },
                );
            }
            Ok(response)
        })?;
        let fd = if op == "RegisterPoolClient" {
            self.fd_reply(&response)?
        } else {
            None
        };
        Ok((response, fd))
    }

    fn create(
        &self,
        _peer: &Peer,
        request: &Value,
        key: &str,
        fingerprint: &str,
    ) -> Result<(Value, Option<File>), String> {
        let id = text(request, "operation_id")?;
        let nbytes = number(request, "nbytes")?;
        let pool_id = opaque_id()?;
        let epoch = opaque_id()?;
        let info = self.catalog(|state, st| {
            if st.shutdown_requested {
                return Err("managed daemon stopping".into());
            }
            let info = state.create_pending(
                st,
                pool_id.clone(),
                epoch.clone(),
                self.provider_session.clone(),
                nbytes,
                key,
            )?;
            let mut pending = envelope(
                id,
                Err(format!("creation pending: {pool_id}:{epoch}")),
                true,
            );
            pending["result"] = json!({"pool_id":pool_id});
            state.operations.insert(
                key.into(),
                Operation {
                    request: fingerprint.into(),
                    state: "Pending",
                    response: pending,
                },
            );
            Ok(info)
        })?;
        let object = info["object_id"].as_u64().unwrap();
        let allocated = self
            .provider
            .lock()
            .map_err(|_| "provider executor poisoned")?
            .as_ref().ok_or("POSIX provider unavailable")?
            .allocate(&pool_id, object, nbytes);
        let response = self.catalog(|state, st| {
            let pool = state.pools.get_mut(&pool_id).unwrap();
            let result = match allocated {
                Ok((resource, descriptor, canonical, file)) => {
                    let dev = canonical["dev"].as_u64().ok_or("invalid actual dev")?;
                    let ino = canonical["inode"].as_u64().ok_or("invalid actual inode")?;
                    pool.file = Some(file);
                    pool.canonical = canonical;
                    pool.resource = Some(resource);
                    pool.descriptor = Some(descriptor);
                    match st.ddr_resources.commit(key, pool.object, dev, ino, nbytes) {
                        Ok(()) => {
                            pool.state = "Allocated";
                            let object = st.objects.get_mut(&pool.object).unwrap();
                            object.actual_bytes = nbytes;
                            object.state = "Allocated".into();
                            let object = object.clone();
                            st.record_legacy_object(&object);
                            let binding = st.resource_bindings.get_mut(&pool.binding).unwrap();
                            binding.provider_id = "posix_shm_simpler".into();
                            binding.provider_session_id = pool.provider_session.clone();
                            binding.provider_resource_id = pool.id.clone();
                            binding.binding_generation = 1;
                            binding.opaque_descriptor =
                                pool.descriptor.as_ref().unwrap().opaque_bytes.clone();
                            binding.state = "Allocated".into();
                            let placement = st.placements.get_mut(&pool.placement).unwrap();
                            placement.provider_id = "posix_shm_simpler".into();
                            placement.address_kind = "scm_rights_fd".into();
                            placement.execution_realm_id =
                                format!("physical:managed:{}", pool.provider_session);
                            Ok(pool.description())
                        }
                        Err(error) => Err(error),
                    }
                }
                Err(error) => Err(error),
            };
            let unknown = result.is_err();
            if let Err(error) = &result {
                pool.state = "CreationUnknown";
                pool.last_error = error.clone();
            }
            let response = envelope(id, result, unknown);
            state.operations.insert(
                key.into(),
                Operation {
                    request: fingerprint.into(),
                    state: if unknown { "Unknown" } else { "Completed" },
                    response: response.clone(),
                },
            );
            Ok(response)
        })?;
        Ok((response, None))
    }

    fn destroy(
        &self,
        request: &Value,
        key: &str,
        fingerprint: &str,
    ) -> Result<(Value, Option<File>), String> {
        let id = text(request, "operation_id")?;
        let pool_id = text(request, "pool_id")?;
        let (dev, ino) = self.catalog(|state, _| {
            let pool = state.pools.get_mut(pool_id).ok_or("pool not found")?;
            if request["pool_epoch"] != pool.epoch
                || request["provider_session"] != pool.provider_session
            {
                return Err("stale destroy tuple".into());
            }
            if pool.state == "Released" {
                return Err("resource already released; query original operation".into());
            }
            if !matches!(pool.state, "Allocated" | "ReleaseUnknown")
                || pool
                    .clients
                    .values()
                    .any(|c| c.state != "Unmapped" || !c.children.is_empty())
                || pool.ranges.leases.values().any(|l| l.state != "Closed")
            {
                return Err("mapped client/child/range/unknown obligation prevents destroy".into());
            }
            pool.state = "Releasing";
            let identity = (
                pool.canonical["dev"].as_u64().ok_or("dev missing")?,
                pool.canonical["inode"].as_u64().ok_or("inode missing")?,
            );
            let mut pending = envelope(id, Err("provider release pending".into()), true);
            pending["result"] = json!({"pool_id":pool_id});
            state.operations.insert(
                key.into(),
                Operation {
                    request: fingerprint.into(),
                    state: "Pending",
                    response: pending,
                },
            );
            Ok(identity)
        })?;
        let closed = self
            .provider
            .lock()
            .map_err(|_| "provider executor poisoned")?
            .as_ref().ok_or("POSIX provider unavailable")?
            .release(pool_id, dev, ino);
        let response=self.catalog(|state,st| {
            let pool=state.pools.get_mut(pool_id).unwrap();
            let result=match closed {
                Ok(receipt)=> {
                    let file=pool.file.take().ok_or("daemon backing FD missing")?;
                    let fd=file.into_raw_fd();
                    if unsafe {libc::close(fd)}!=0 {Err(format!("daemon FD close unknown: {fd}"))}
                    else {
                        pool.release_result=receipt;pool.state="Released";
                        st.ddr_resources.release_confirmed(pool.object);st.objects.remove(&pool.object);st.mark_object_released(pool.object);
                        Ok(json!({"released":true,"provider_release":pool.release_result,"daemon_fd_closed":true}))
                    }
                },Err(error)=>Err(error),
            };
            let unknown=result.is_err();if let Err(error)=&result {pool.state="ReleaseUnknown";pool.last_error=error.clone();}
            let response=envelope(id,result,unknown);
            state.operations.insert(key.into(),Operation {request:fingerprint.into(),state:if unknown {"Unknown"} else {"Completed"},response:response.clone()});
            Ok(response)
        })?;
        Ok((response, None))
    }
}

pub fn run(endpoint: &str, root: &Path, python: &str, budget: u64,
           mem_service_config: Option<&Path>) -> Result<(), String> {
    if budget == 0 || budget > 256 * 1024 * 1024 || budget % 65536 != 0 {
        return Err("bounded aligned host budget required".into());
    }
    fs::DirBuilder::new()
        .mode(0o700)
        .create(root)
        .map_err(|e| format!("fresh managed run root required: {e}"))?;
    let lock = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(root.join("authority.lock"))
        .map_err(|e| e.to_string())?;
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err("managed run lock unavailable".into());
    }
    let admin = opaque_id()?;
    let mut secret = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(root.join("admin.token"))
        .map_err(|e| e.to_string())?;
    secret
        .write_all(admin.as_bytes())
        .and_then(|_| secret.sync_all())
        .map_err(|e| e.to_string())?;
    drop(secret);
    let mut remote = mem_service_config.map(crate::mem_service_pool::MemServicePoolProvider::load).transpose()?;
    if remote.is_some() && budget > 128 * 1024 * 1024 {
        return Err("mem_service B pool budget is at most 128MiB".into());
    }
    let provider = if remote.is_none() { Some(Provider::start(root, python)?) } else { None };
    let mut catalog = Catalog::new(-1, Vec::new());
    catalog.ddr_resources.limit = budget;
    if let Some(p)=&mut remote {p.admit_bootstrap(&mut catalog)?;}
    let (provider_session, provider_loaded) = if let Some(p) = &provider {
        (p.session.clone(), p.loaded.clone())
    } else { let p = remote.as_ref().unwrap(); (p.session.clone(), p.loaded()) };
    let service = Arc::new(Service {
        shared: Arc::new((Mutex::new(catalog), Condvar::new())),
        provider: Mutex::new(provider),
        remote,
        provider_session,
        provider_loaded,
        root: root.into(),
        admin,
        persisted: Mutex::new(0),
        stop: AtomicBool::new(false),
        drop_reply_once: AtomicBool::new(false),
        durability_unknown: AtomicBool::new(false),
    });
    service.catalog(|_, _| Ok(()))?;
    let listener = UnixListener::bind(endpoint).map_err(|e| e.to_string())?;
    fs::set_permissions(endpoint, fs::Permissions::from_mode(0o600)).map_err(|e| e.to_string())?;
    let socket_identity = fs::symlink_metadata(endpoint).map_err(|e| e.to_string())?;
    listener.set_nonblocking(true).map_err(|e| e.to_string())?;
    eprintln!("managed host pool ready; no ACL/HBM/E10 persistent-domain initialization");
    let mut handlers: Vec<thread::JoinHandle<()>> = Vec::new();
    while !service.stop.load(Ordering::Acquire) {
        let mut active = Vec::new();
        for handle in handlers {
            if handle.is_finished() {
                let _ = handle.join();
            } else {
                active.push(handle);
            }
        }
        handlers = active;
        match listener.accept() {
            Ok((mut stream, _)) => {
                if handlers.len() >= 64 {
                    continue;
                }
                let owned = Arc::clone(&service);
                handlers.push(thread::spawn(move || {
                    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
                    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
                    let result = (|| {
                        let peer = peer(&stream)?;
                        let (request, incoming) = pool_wire::receive(&mut stream)?;
                        if !incoming.is_empty() {
                            return Err("clients may not register another allocator FD".into());
                        }
                        let id = request
                            .get("operation_id")
                            .and_then(Value::as_str)
                            .unwrap_or("invalid-frame");
                        let response = owned.handle(&peer, &request).unwrap_or_else(|e| {
                            (
                                envelope(
                                    id,
                                    Err(e),
                                    owned.durability_unknown.load(Ordering::Acquire),
                                ),
                                None,
                            )
                        });
                        let fault =
                            std::env::var("UFLOW_POOL_TEST_DROP_REPLY_ONCE").unwrap_or_default();
                        if request["op"] == fault
                            && !fault.is_empty()
                            && response.0["status"] == "ok"
                            && !owned.drop_reply_once.swap(true, Ordering::AcqRel)
                        {
                            return Ok(());
                        }
                        pool_wire::send(&mut stream, &response.0, response.1.as_ref())
                    })();
                    if let Err(error) = result {
                        let _ = pool_wire::send(
                            &mut stream,
                            &envelope("invalid-frame", Err(error), false),
                            None,
                        );
                    }
                }));
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5))
            }
            Err(error) => return Err(error.to_string()),
        }
    }
    drop(listener);
    for handler in handlers {
        handler
            .join()
            .map_err(|_| "managed handler panic; stop unknown")?;
    }
    if let Some(provider) = service.provider.lock().map_err(|_| "provider lock poisoned")?.as_mut() {
        provider.shutdown()?;
    }
    if let Some(remote) = &service.remote { remote.finish()?; }
    service.catalog(|state, _| {
        if state.pools.values().any(|pool| pool.state != "Released") {
            return Err("late live pool at stop".into());
        }
        Ok(())
    })?;
    let mut final_state = serde_json::from_slice::<Value>(
        &fs::read(root.join("state.json")).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    final_state["normal_shutdown"] = json!(true);
    final_state["provider_exited"] = json!(service.remote.is_none());
    final_state["external_guest_contexts_drained"] = json!(service.remote.is_some());
    fs::write(
        root.join("clean.json"),
        serde_json::to_vec(&final_state).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    if fs::symlink_metadata(endpoint).is_ok_and(|meta| {
        meta.dev() == socket_identity.dev() && meta.ino() == socket_identity.ino()
    }) {
        fs::remove_file(endpoint).map_err(|e| e.to_string())?;
    }
    eprintln!("managed pool drained; backend completion receipts and capacity release confirmed");
    Ok(())
}
