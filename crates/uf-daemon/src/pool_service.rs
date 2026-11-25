//! Explicit simulator host-pool adapter. Provider waits and all socket I/O are outside Catalog.
use crate::catalog::{Catalog, SharedCatalog};
use crate::managed_pool::{Peer, State};
use crate::pool_process::{exited, opaque_id, pidfd};
use crate::pool_wire;
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
    remote: Option<crate::mem_service_pool::MemServicePoolProvider>,
    provider_loaded: Value,
    root: PathBuf,
    pub(crate) admin: String,
    persisted: Mutex<u64>,
    pub(crate) stop: AtomicBool,
    drop_reply_once: AtomicBool,
    durability_unknown: AtomicBool,
    _responsibility_authority: Option<File>,
}

fn envelope(id: &str, result: Result<Value, String>, unknown: bool) -> Value {
    match result {
        Ok(value) => json!({"schema":1,"status":"ok","operation_id":id,"result":value}),
        Err(error) => json!({"schema":1,"status":"error","operation_id":id,
                          "code":if unknown {"Unknown"} else {"Rejected"},"detail":error}),
    }
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

    fn handle(&self, peer: &Peer, request: &Value) -> Result<Value, String> {
        self.remote
            .as_ref()
            .ok_or("simulated pool provider unavailable")?
            .handle(self, peer, request)
    }
}

pub fn run(endpoint: &str, root: &Path, budget: u64, config: &Path) -> Result<(), String> {
    #[cfg(not(test))]
    return run_inner(endpoint, root, budget, config);
    #[cfg(test)]
    return run_inner(endpoint, root, budget, config, None, None);
}

#[cfg(test)]
pub(crate) fn managed_read_cache_host_service(catalog:Catalog,root:&Path)->Service {
    Service{shared:Arc::new((Mutex::new(catalog),Condvar::new())),remote:None,
        provider_loaded:json!({"HOST_cache_transport_double":true}),root:root.into(),
        admin:"HOST_ONLY".into(),persisted:Mutex::new(0),stop:AtomicBool::new(false),drop_reply_once:AtomicBool::new(false),
        durability_unknown:AtomicBool::new(false),_responsibility_authority:None}
}

#[cfg(test)]
pub(crate) fn run_reconciliation_fixture(endpoint:&str,root:&Path,config:&Path,proof:&Value) -> Result<(),String> {
    run_inner(endpoint, root, 128 << 20, config, Some(proof), None)
}

#[cfg(test)]
pub(crate) fn run_terminal_staged_fixture(endpoint:&str,root:&Path,config:&Path,authority:&mut crate::pool_terminal_domain::TerminalAuthority)->Result<(),String>{
    run_inner(endpoint, root, 128 << 20, config, None, Some(authority))
}

/// Real SO_PEERCRED/control/persistence test seam. No provider executor,
/// bootstrap RPC, Guest, production root or caller-selected production proof.
#[cfg(test)]
pub(crate) fn terminal_control_fixture(catalog:Catalog,responsibility:File,config:&Path,root:&Path,listener:UnixListener)->Result<Value,String>{
    let remote=crate::mem_service_pool::MemServicePoolProvider::load(config)?;
    let service=Service{shared:Arc::new((Mutex::new(catalog),Condvar::new())),
        provider_loaded:remote.loaded(),remote:Some(remote),root:root.into(),
        admin:"CPU-TERMINAL-CONTROL-ONLY".into(),persisted:Mutex::new(0),stop:AtomicBool::new(false),
        drop_reply_once:AtomicBool::new(false),durability_unknown:AtomicBool::new(false),_responsibility_authority:Some(responsibility)};
    listener.set_nonblocking(true).map_err(|e|e.to_string())?;
    let deadline=std::time::Instant::now()+Duration::from_secs(10);let mut completed=0;
    while completed<3 {
        if std::time::Instant::now()>deadline{return Err("terminal CPU control fixture deadline".into());}
        match listener.accept(){
            Ok((mut stream,_))=>{
                stream.set_read_timeout(Some(Duration::from_secs(2))).map_err(|e|e.to_string())?;
                stream.set_write_timeout(Some(Duration::from_secs(2))).map_err(|e|e.to_string())?;
                let owner=peer(&stream)?;let(request,fds)=pool_wire::receive(&mut stream)?;
                if !fds.is_empty(){return Err("metadata-only terminal fixture".into());}
                let response=service.handle(&owner,&request).unwrap_or_else(|error|{
                    let mut value=envelope(request["operation_id"].as_str().unwrap(),Err(error),false);value["schema"]=json!(2);value});
                pool_wire::send(&mut stream,&response,None)?;completed+=1;
            },
            Err(error) if error.kind()==std::io::ErrorKind::WouldBlock=>thread::sleep(Duration::from_millis(5)),
            Err(error)=>return Err(error.to_string()),
        }
    }
    let st=service.shared.0.lock().map_err(|_|"terminal CPU Catalog poisoned")?;
    Ok(st.managed_pools.snapshot(&st))
}

fn run_inner(endpoint:&str,root:&Path,budget:u64,mem_service_config:&Path,
    #[cfg(test)] fixture:Option<&Value>,#[cfg(test)] terminal:Option<&mut crate::pool_terminal_domain::TerminalAuthority>) -> Result<(),String> {
    if budget == 0 || budget > 128 * 1024 * 1024 || budget % 65536 != 0 {
        return Err("bounded aligned simulator pool budget up to 128 MiB required".into());
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
    let mut remote = crate::mem_service_pool::MemServicePoolProvider::load(mem_service_config)?;
    crate::pool_experiments::guard_original_scope(Some(&remote))?;
    let mut catalog = Catalog::new(-1, Vec::new());
    catalog.ddr_resources.limit = budget;
    let restore_original=|st:&mut Catalog|crate::pool_experiments::restore(st,Some(&remote),root);
    #[cfg(not(test))]
    let responsibility=restore_original(&mut catalog)?;
    #[cfg(test)]
    let responsibility=if let Some(authority)=terminal {
        Some(crate::pool_terminal_domain::staged_fixture_attach(authority,&mut catalog,&remote.loaded())?)
    } else if let Some(proof)=fixture {
        Some(if proof["kind"]=="verified-closed-c039-prestart" {
            crate::pool_c039_prestart::restore_fixture(&mut catalog,proof,&root.join("fixture-lineage"),root)?
        } else if proof["kind"]=="verified-closed-c033-quarantine76" {
            crate::pool_quarantine76::restore_fixture(&mut catalog,proof,&root.join("fixture-lineage"),root)?
        } else if proof["kind"]=="verified-c027-retained8-succession" {
            crate::pool_retained8::restore_fixture(&mut catalog,proof,&root.join("fixture-lineage"),root)?
        } else {crate::pool_reconciliation::restore_fixture(&mut catalog,proof,&root.join("fixture-lineage"),root)?})
    } else {restore_original(&mut catalog)?};
    if catalog.managed_pools.reconciliation["phase"]=="ClosedC033Quarantine76" {
        // Persist full inherited 76 before reserving any new canaries.
        catalog.managed_pools.revision=1;
        let baseline=catalog.managed_pools.snapshot(&catalog);
        let mut f=OpenOptions::new().write(true).create_new(true).mode(0o600).custom_flags(libc::O_NOFOLLOW)
            .open(root.join("state.json")).map_err(|e|e.to_string())?;
        serde_json::to_writer(&mut f,&baseline).map_err(|e|e.to_string())?;
        f.write_all(b"\n").and_then(|_|f.sync_all()).map_err(|e|e.to_string())?;
        File::open(root).and_then(|f|f.sync_all()).map_err(|e|e.to_string())?;
    }
    remote.admit_bootstrap(&mut catalog)?;
    let provider_loaded = remote.loaded();
    let service = Arc::new(Service {
        shared: Arc::new((Mutex::new(catalog), Condvar::new())),
        remote: Some(remote),
        provider_loaded,
        root: root.into(),
        admin,
        persisted: Mutex::new(0),
        stop: AtomicBool::new(false),
        drop_reply_once: AtomicBool::new(false),
        durability_unknown: AtomicBool::new(false),
        _responsibility_authority: responsibility,
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
                            let mut error = envelope(id, Err(e), owned.durability_unknown.load(Ordering::Acquire));
                            if request["schema"] == 2 {
                                error["schema"] = json!(2);
                            }
                            error
                        });
                        let fault =
                            std::env::var("UFLOW_POOL_TEST_DROP_REPLY_ONCE").unwrap_or_default();
                        if request["op"] == fault
                            && !fault.is_empty()
                            && response["status"] == "ok"
                            && !owned.drop_reply_once.swap(true, Ordering::AcqRel)
                        {
                            return Ok(());
                        }
                        pool_wire::send(&mut stream, &response, None)
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
    service
        .remote
        .as_ref()
        .ok_or("simulated pool provider unavailable")?
        .finish()?;
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
    final_state["provider_exited"] = json!(false);
    final_state["external_guest_contexts_drained"] = json!(true);
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
