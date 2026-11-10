//! Opt-in simulated backend. Catalog/managed_pool/pool_ranges remain authority.
//! All provider calls are outside Catalog; no FD fiction or payload transport.
use crate::ddr_resources::BackingId;
use crate::catalog::Catalog;
use crate::managed_pool::{Operation, Peer};
use crate::pool_provider::opaque_id;
use crate::pool_service::Service;
use crate::pool_wire::{fields, number, parse, text};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex,OnceLock};
use std::time::Duration;
use uf_core::bridge::{BridgeFrame, BridgeMessageType};
use uf_core::provider::{AccessKind, Addressability, TransportKind, ExecutionMode, Fidelity};

pub struct MemServicePoolProvider {
    pub session: String,
    realm: String,
    nodes: BTreeMap<String, Value>,
    home: String,
    sequence: AtomicU64,
    bootstrap: BTreeMap<String, u64>,
    bootstrap_unknown: AtomicBool,
    deferred_bootstrap: bool,
    readiness_file: Option<PathBuf>,
    pub reconciliation: Option<Value>,
    active: OnceLock<Box<MemServicePoolProvider>>,
    activation: Mutex<()>,
    managed_cache_execution:Option<Value>,
    // No production constructor until the new native profile has been accepted.
    #[cfg(test)]
    managed_cache_host_test: bool,
    #[cfg(test)] managed_write_host_test:bool,
}

const CANARY_BYTES: u64 = 2 << 20;
const BUSINESS_SEQUENCE: u64 = 1u64 << 32;
fn ready_receipts(identity: &Value, worker: &Value, status: &Value) -> Result<u64,String> {
    let expected=&identity["startup"]["worker"];
    let lease=number(status,"lease_ms")?;
    if status["schema"]!=2 || status["phase"]!="completed" || status["node"]!=identity["node"] ||
        status["boot_id"]!=identity["boot_id"] || status["incarnation"]!=identity["incarnation"] ||
        status["readiness_generation"]!=1 || status["capabilities"]!=1 || status["channel_ready"]!=true ||
        status["directory_registered"]!=true || status["directory_ready"]!=true || status["required_count"]!=2 ||
        status["active_count"]!=2 || lease<100 || worker["schema"]!=2 || worker["phase"]!="completed" ||
        worker["worker_alive"]!=true || worker["worker_ready"]!=true || number(worker,"worker_refresh_count")?==0 ||
        worker["worker_pid"]!=expected["worker_pid"] || worker["worker_birth"]!=expected["worker_birth"] ||
        number(worker,"worker_pid")?==0 || number(worker,"worker_birth")?==0 ||
        worker["incarnation"]!=identity["incarnation"] || worker["readiness_generation"]!=1 || worker["capabilities"]!=1 ||
        worker["lease_ms"]!=lease || number(worker,"refresh_age_ms")?>=lease/2 {
        return Err("live same-boot channel/directory/worker refresh required before business".into());
    }
    // The SDK's optional Host in-process registry bit is preserved as a raw
    // diagnostic. Managed admission uses directory readiness + actual Guest
    // canary/channel + real worker refresh, exactly as the frozen worker does.
    let epoch=number(status,"directory_epoch")?;
    if epoch==0 || status["directory_epoch"]!=identity["startup"]["directory_epoch"] {
        return Err("directory incarnation changed since startup".into());
    }
    Ok(epoch)
}
fn canary_descriptor(value: &Value) -> Result<&str, String> {
    let s=value["canary_descriptor"].as_str().ok_or("opaque canary descriptor missing")?;
    if s.is_empty() || s.len()>384 || s.len()%2!=0 || !s.bytes().all(|b|b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
        return Err("bounded opaque canary descriptor required".into());
    }
    Ok(s)
}

fn envelope(id: &str, result: Result<Value, String>, unknown: bool) -> Value {
    match result {
        Ok(value) => json!({"schema":2,"status":"ok","operation_id":id,"result":value}),
        Err(error) => json!({"schema":2,"status":"error","operation_id":id,
            "code":if unknown {"Unknown"} else {"Rejected"},"detail":error}),
    }
}

impl MemServicePoolProvider {
    pub fn load(path: &Path) -> Result<Self, String> {
        let data = std::fs::read(path).map_err(|e| e.to_string())?;
        let c = parse(&data)?;
        fields(&c, &["schema", "backend", "realm", "session", "home", "nodes"], &["deferred_bootstrap","readiness_file","reconciliation","managed_cache_execution"])?;
        let managed_cache_execution=if c.get("managed_cache_execution").is_some(){Some(crate::pool_terminal_domain::case_chain::validate_managed_binding(&c["managed_cache_execution"])?)}else{None};
        if c["schema"] != 2 || c["backend"] != "mem_service_obmm" { return Err("explicit mem_service config v2 required".into()); }
        let deferred=c["deferred_bootstrap"]==true;
        if c.get("deferred_bootstrap").is_some_and(|value|!value.is_boolean()) || (!deferred && !c["reconciliation"].is_null()) {
            return Err("only staged bootstrap may import original responsibility".into());
        }
        if deferred && (c["reconciliation"].is_null() || !Path::new(text(&c,"readiness_file")?).is_absolute()) {
            return Err("deferred bootstrap requires original responsibility import and controlled readiness file".into());
        }
        let mut nodes = BTreeMap::new();
        for node in c["nodes"].as_array().ok_or("node array required")? {
            if deferred {fields(node,&["node","socket"],&[])?;}
            else {fields(node, &["node", "boot_id", "agent_session", "incarnation", "socket", "canary", "ready", "startup"], &[])?;}
            let id = text(node, "node")?;
            if !matches!(id, "nodeA" | "nodeB") || nodes.contains_key(id) || (!deferred && number(node, "incarnation")? == 0) {
                return Err("two unique bound logical nodes required".into());
            }
            if !deferred {text(node, "boot_id")?; text(node, "agent_session")?;}
            if !PathBuf::from(text(node, "socket")?).is_absolute() { return Err("absolute task endpoint required".into()); }
            nodes.insert(id.to_owned(), node.clone());
        }
        let home = text(&c, "home")?.to_owned();
        if !text(&c,"realm")?.starts_with("simulated:") { return Err("explicit simulated execution realm required".into()); }
        if nodes.len() != 2 || !nodes.contains_key(&home) { return Err("two nodes and bound home required".into()); }
        for (id,node) in &nodes {
            if deferred {continue;}
            let peer=&nodes[if id=="nodeA" {"nodeB"} else {"nodeA"}];
            let canary=&node["canary"];let ready=&node["ready"];
            canary_descriptor(canary)?;
            if canary["schema"]!=2 || canary["phase"]!="completed" || canary["canary_prepared"]!=true ||
                canary["canary_bytes"]!=CANARY_BYTES || canary["visible_bytes"]!=4096 ||
                number(canary,"checksum")?==0 || number(canary,"guest_pid")?==0 ||
                ready["schema"]!=2 || ready["phase"]!="completed" || ready["canary_ready"]!=true ||
                ready["verified_checksum"]!=peer["canary"]["checksum"] || ready["guest_pid"]!=canary["guest_pid"] {
                return Err("both actual same-context canary preparation and peer readiness required before Catalog".into());
            }
            if node["startup"]["managed_ready"]!=true {return Err("actual startup readiness proof missing".into());}
            ready_receipts(node,&node["startup"]["worker"],&node["startup"]["directory"])?;
            let registered=&node["startup"]["registration"];
            if registered["directory_registered"]!=true || registered["boot_id"]!=node["boot_id"] ||
                registered["incarnation"]!=node["incarnation"] || registered["readiness_generation"]!=1 || registered["capabilities"]!=1 {
                return Err("public directory registration binding missing".into());
            }
        }
        Ok(Self { session: text(&c, "session")?.into(), realm: text(&c, "realm")?.into(),
            nodes, home, sequence: AtomicU64::new(BUSINESS_SEQUENCE), bootstrap:BTreeMap::new(),bootstrap_unknown:AtomicBool::new(false),
            deferred_bootstrap:deferred,readiness_file:c["readiness_file"].as_str().map(PathBuf::from),
            reconciliation:if c["reconciliation"].is_null(){None}else{Some(c["reconciliation"].clone())},
            active:OnceLock::new(),activation:Mutex::new(()),managed_cache_execution,
            #[cfg(test)] managed_cache_host_test:false,managed_write_host_test:false })
    }

    fn reject_retained_reuse(&self, st:&Catalog)->Result<(),String> {
        self.reject_history(&st.managed_pools.reconciliation)
    }
    fn reject_history(&self,old:&Value)->Result<(),String>{
        crate::pool_terminal_domain::reject_fenced(old,&json!({"realm":self.realm,"session":self.session,"nodes":self.nodes}))?;
        if let Some(contexts)=old["historical_prestart_contexts"].as_array(){
            for c in contexts {
                if c["realm"]==self.realm || c["provider_session"]==self.session {
                    return Err("closed prestart realm/session/boot/incarnation cannot be revived".into());
                }
                if let Some(ids)=c["guest_identities"].as_array(){for i in ids {for n in self.nodes.values(){
                    for key in ["boot_id","agent_session","incarnation"] {
                        if let Some(v)=n.get(key){
                            let same=Some(v)==i.get(key);
                            let cross=matches!(key,"boot_id"|"agent_session") && (Some(v)==i.get("boot_id")||Some(v)==i.get("agent_session"));
                            if !v.is_null() && (same||cross){return Err("closed prestart boot/incarnation cannot be revived".into());}
                        }
                    }
                }}}
            }
        }
        if let Some(canaries)=old["retained_canaries"].as_array() {
            for c in canaries {
                if c["provider_session"]==self.session || c["backing"]["realm"]==self.realm ||
                    self.nodes.values().any(|n|["boot_id","agent_session","incarnation"].iter().any(|key|
                        n.get(*key).is_some_and(|v|!v.is_null() && (Some(v)==c.get(*key) ||
                            (matches!(*key,"boot_id"|"agent_session") && (Some(v)==c.get("boot_id") || Some(v)==c.get("agent_session"))) ||
                            (*key=="incarnation" && *v==c["backing"]["generation"]))))){
                    return Err("current runtime cannot reuse any retained boot/incarnation/session/realm identity".into());
                }
            }
        }
        Ok(())
    }
    pub fn admit_bootstrap(&mut self, st: &mut Catalog) -> Result<(), String> {
        self.reject_retained_reuse(st)?;
        let terminal=&st.managed_pools.reconciliation;
        if terminal["terminal_bootstrap_phase"]=="AwaitingNewGuestReadiness" {
            let namespace=&terminal["terminal_staged_namespace"];
            if namespace["realm"]!=self.realm || namespace["session"]!=self.session || st.ddr_resources.active_reserved_bytes_for_terminal()!=4<<20 {
                return Err("staged terminal namespace/reserved4 mismatch".into());
            }
            for (node,identity) in &self.nodes {
                let expected=namespace["nodes"].as_array().ok_or("staged nodes")?.iter().find(|v|v["node"]==*node).ok_or("staged node")?;
                if expected["socket"]!=identity["socket"]{return Err("staged terminal endpoint changed".into());}
                let id=terminal["terminal_canary_charge_objects"][node].as_u64().ok_or("staged canary id")?;
                let first=terminal["terminal_domain_fences"]["minimum_next_object_id"].as_u64().ok_or("staged object highwater")?;
                if id!=if node=="nodeA"{first}else{first.checked_add(1).ok_or("staged object overflow")?} || self.bootstrap.get(node).is_some_and(|old|*old!=id){return Err("staged canary charge identity mismatch".into());}
                self.bootstrap.insert(node.clone(),id);
            }
        }
        // Real, already-live canary exports share the one physical capacity ledger.
        // No charges for temporary peer verification views or application endpoints.
        for (node,identity) in &self.nodes {
            let reservation=format!("canary:{node}");
            let id=if let Some(id)=self.bootstrap.get(node){*id}else{
                let id=st.take_next_object_id();st.ddr_resources.reserve_unique(&reservation,CANARY_BYTES)?;id};
            if self.deferred_bootstrap {self.bootstrap.insert(node.clone(),id);continue;}
            st.ddr_resources.commit_backing(&reservation,id,BackingId::Provider {
                realm:self.realm.clone(),provider:"mem_service_obmm".into(),
                resource:format!("canary:{node}:{}",text(identity,"boot_id")?),
                generation:number(identity,"incarnation")?},CANARY_BYTES)?;
            self.bootstrap.insert(node.clone(),id);
        }
        if !self.deferred_bootstrap && st.managed_pools.reconciliation["terminal_bootstrap_phase"]=="AwaitingNewGuestReadiness" {
            st.managed_pools.reconciliation["terminal_bootstrap_phase"]=json!("ActiveNewNamespace");
        }
        Ok(())
    }

    pub fn loaded(&self) -> Value {
        json!({"backend":"mem_service_obmm","schema":2,"realm":self.realm,
            "session":self.session,"home":self.home,"nodes":self.nodes,"bootstrap_charge_objects":self.bootstrap,
            "source":"546985e26bce60798fa3f88329c3fbed8350c798",
            "fidelity":"functional-simulation","payload_rpc_bytes":0})
    }

    fn managed_write_enabled(&self)->bool {
        #[cfg(test)] {self.managed_write_host_test}
        #[cfg(not(test))] {false} // New W profile/context acceptance is required.
    }
    fn managed_cache_enabled(&self)->bool {
        #[cfg(test)] {self.managed_cache_host_test||self.managed_cache_execution.is_some()}
        #[cfg(not(test))] {self.managed_cache_execution.is_some()}
    }
    fn next_sequence(&self)->Result<u64,String>{
        self.sequence.fetch_update(Ordering::Relaxed,Ordering::Relaxed,|n|n.checked_add(1))
            .map_err(|_| "bridge request sequence exhausted; original session cannot wrap".into())
    }
    fn call(&self, node: &str, body: Value) -> Result<Value, String> {
        self.call_with_id(node,body,self.next_sequence()?)
    }
    fn call_with_id(&self,node:&str,mut body:Value,id:u64)->Result<Value,String>{
        let identity = self.nodes.get(node).ok_or("unbound logical node")?;
        body["schema"] = json!(2);
        let mut request = BridgeFrame::request(BridgeMessageType::PoolControlV2, id, id,
            &self.realm, node, text(identity,"boot_id")?, text(identity,"agent_session")?,
            number(identity,"incarnation")?, serde_json::to_vec(&body).map_err(|e| e.to_string())?);
        request.protocol_minor = 1;
        let mut connection = UnixStream::connect(text(identity,"socket")?).map_err(|e| format!("provider outcome unknown: {e}"))?;
        connection.set_read_timeout(Some(Duration::from_secs(120))).map_err(|e| e.to_string())?;
        connection.set_write_timeout(Some(Duration::from_secs(30))).map_err(|e| e.to_string())?;
        request.write_to(&mut connection).map_err(|e| format!("provider send unknown: {e:?}"))?;
        let reply = BridgeFrame::read_from(&mut connection).map_err(|e| format!("provider completion unknown: {e:?}"))?;
        if reply.protocol_minor != 1 || reply.request_id != id || reply.correlation_id != id
            || reply.execution_realm_id != self.realm || reply.node_id != node
            || reply.boot_id != request.boot_id || reply.provider_session_id != request.provider_session_id
            || reply.session_generation != request.session_generation
            || reply.message_type != BridgeMessageType::Completion || reply.status != 0 {
            return Err("identity/correlation/terminal mismatch; preserve original obligation".into());
        }
        let receipt = parse(&reply.body)?;
        if receipt["schema"] != 2 || receipt["phase"] != "completed" {
            return Err(format!("lower operation not completed: {receipt}"));
        }
        Ok(receipt)
    }

    pub fn finish(&self) -> Result<(), String> {
        if let Some(active)=self.active.get() {return active.finish();}
        if self.deferred_bootstrap {return Err("current bootstrap never activated; reservation remains".into());}
        for node in self.nodes.keys() {
            let receipt = self.call(node, json!({"op":"inspect"}))?;
            if receipt["contexts"] != 0 || receipt["pins"] != 0 || receipt["cleanup_required"] != false {
                return Err("actual lower context drain not confirmed".into());
            }
        }
        Ok(())
    }

    fn require_ready(&self) -> Result<(),String> {
        let mut epoch=None;
        for (name,identity) in &self.nodes {
            let worker=self.call(name,json!({"op":"worker_state"}))?;
            let status=self.call(name,json!({"op":"provider_status"}))?;
            let observed=ready_receipts(identity,&worker,&status)?;
            if epoch.is_some_and(|previous|previous!=observed) {return Err("two-node directory changed during readiness check".into());}
            epoch=Some(observed);
        }
        Ok(())
    }

    pub fn handle(&self, service: &Service, peer: &Peer, req: &Value) -> Result<Value, String> {
        if let Some(active)=self.active.get() {return active.handle(service,peer,req);}
        let id = text(req, "operation_id")?;
        let op = text(req, "op")?;
        let key = format!("{}:{id}", peer.actor());
        // Strict schema/backend discriminator; schema1 cannot access this mode.
        if number(req, "schema")? != 2 { return Err("mem_service requires explicit schema2 client".into()); }
        let common = ["schema","op","operation_id","pool_id","pool_epoch","provider_session","client_session","access_generation","access_binding_v2"];
        let extra: &[&str] = match op {
            "CreateManagedPool" => &["admin_token","nbytes"],
            "ActivateManagedBootstrap" => &["admin_token"],
            "DestroyManagedPool" | "ShutdownManagedPool" => &["admin_token"],
            "RegisterPoolClient" => &["access","node"],
            "AllocatePoolRange" => &["nbytes","alignment"],
            "AcquirePoolRange" => &["block","mode","expected_generation","byte_offset","nbytes"],
            "ExecutePoolRange" => &["lease","pattern_id"],
            "ReleasePoolLease" => &["lease","write_not_started"],
            "DescribeManagedPool" | "GetManagedPoolSnapshot" | "BeginPoolUnregister" => &[],
            "ReportPoolUnknown" => &["reason","lease"],
            "QueryPoolOperation" => &["target_operation_id"],
            _ => return Err("unsupported schema2 operation; no caller-supplied map/unmap/commit ACK".into()),
        };
        let mut allowed = common.to_vec(); allowed.extend_from_slice(extra);
        fields(req, &["schema","op","operation_id"], &allowed)?;
        if matches!(op,"CreateManagedPool"|"DestroyManagedPool"|"ShutdownManagedPool"|"ActivateManagedBootstrap") && req["admin_token"] != service.admin {
            return Err("management capability required".into());
        }
        let mut fingerprint = req.clone(); fingerprint.as_object_mut().unwrap().remove("admin_token");
        let fingerprint = fingerprint.to_string();
        let read_only = matches!(op,"DescribeManagedPool"|"GetManagedPoolSnapshot"|"QueryPoolOperation");
        let prior = service.catalog(|state,st| {
            crate::pool_terminal_domain::reject_fenced(&state.reconciliation,req)?;
            let original=&state.reconciliation["identity"];
            if op == "QueryPoolOperation" {
                let target = text(req,"target_operation_id")?;
                let historical=&state.reconciliation["quarantined_creation"];
                if historical["operation"]["response"]["operation_id"]==target {
                    return Ok(Some(envelope(id,Ok(json!({"state":"Unknown","context":"closed-c033-quarantined-inaccessible",
                        "response":historical["operation"]["response"],"pool":historical["pool"],
                        "reserved_bytes":historical["nbytes"],"SDK_ACK_Unmapped_or_canary_close_claimed":false})),false)));
                }
                if !original.is_null() && original["operation_id"]==target {
                    return Ok(Some(envelope(id,Ok(json!({"state":"VerifiedUncreatedTerminal",
                        "identity":original,"historical_unknown_response":state.reconciliation["historical_unknown_operation"]["response"],
                        "SDK_ACK_Unmapped_or_canary_close_claimed":false})),false)));
                }
                return Ok(Some(envelope(id,Ok(state.operations.get(&format!("{}:{target}",peer.actor()))
                    .map(|v|json!({"state":v.state,"response":v.response}))
                    .unwrap_or(json!({"state":"NotFound"}))),false)));
            }
            if !original.is_null() && (req["operation_id"]==original["operation_id"] ||
                req.get("pool_id").is_some_and(|pool|pool==&original["pool_id"])) {
                return Err("original c013 terminal history cannot be reused for admission or access".into());
            }
            let closed=&state.reconciliation["quarantined_creation"];
            if !closed.is_null() && (req["operation_id"]==closed["operation"]["response"]["operation_id"] ||
                req.get("pool_id").is_some_and(|v|v==&closed["pool"]["pool_id"]) ||
                req.get("provider_session").is_some_and(|v|v==&closed["pool"]["provider_session"])) {
                return Err("closed c033 CreationUnknown is inaccessible immutable history, not a replay/release target".into());
            }
            if read_only { return Ok(None); }
            if let Some(old) = state.operations.get(&key) {
                if old.request != fingerprint { return Err("operation identity replay mismatch".into()); }
                return Ok(Some(old.response.clone()));
            }
            let admission=matches!(op,"CreateManagedPool"|"RegisterPoolClient"|"AllocatePoolRange"|"AcquirePoolRange"|"ExecutePoolRange");
            if admission && st.shutdown_requested { return Err("Catalog closing; new admission forbidden".into()); }
            if state.operations.len() >= 8192 || admission && state.operations.len()>=4096 {
                return Err("bounded journal admission; cleanup headroom retained".into());
            }
            state.operations.insert(key.clone(),Operation { request:fingerprint.clone(),state:"Pending",
                response:envelope(id,Err("operation pending; query original ID".into()),true) });
            Ok(None)
        })?;
        if let Some(prior) = prior { return Ok(prior); }
        let result = self.execute(service,peer,req);
        // Any failure after a provider mutation keeps that exact pool/operation.
        // Metadata rejection is safe; execute marks uncertain lower work itself.
        service.catalog(|state,_| {
            let unknown = self.bootstrap_unknown.load(Ordering::Acquire) || state.pools.values().any(|p|p.state.contains("Unknown") || p.frozen()
                || p.ranges.leases.values().any(|l|l.state=="Unknown"));
            let response = envelope(id,result,unknown);
            if !read_only { state.operations.insert(key,Operation { request:fingerprint,
                state:if response["status"]=="ok" {"Completed"} else if unknown {"Unknown"} else {"Rejected"},response:response.clone() }); }
            Ok(response)
        })
    }

    fn execute(&self, service:&Service, peer:&Peer, req:&Value) -> Result<Value,String> {
        let op=text(req,"op")?;
        service.catalog(|state,_| {
            let old=&state.reconciliation["identity"];
            if (!old.is_null()) && (req.get("pool_id").is_some_and(|id|id==&old["pool_id"]) ||
                req.get("operation_id").is_some_and(|id|id==&old["operation_id"]) ||
                req.get("target_operation_id").is_some_and(|id|id==&old["operation_id"])) {
                return Err("original c013 is VerifiedUncreatedTerminal; historical Unknown is not an accessible pool or retry".into());
            }
            Ok(())
        })?;
        if op=="ActivateManagedBootstrap" {
            let _guard=self.activation.lock().map_err(|_|"bootstrap activation poisoned")?;
            if !self.deferred_bootstrap || self.active.get().is_some() {return Err("bootstrap already activated or not staged".into());}
            let mut active=Self::load(self.readiness_file.as_ref().ok_or("readiness file missing")?)?;
            if active.managed_cache_execution!=self.managed_cache_execution || active.deferred_bootstrap || active.realm!=self.realm || active.session!=self.session || active.home!=self.home ||
                active.nodes.iter().any(|(name,node)|node["socket"]!=self.nodes[name]["socket"]) {
                return Err("prepared readiness does not bind this pending authority".into());
            }
            service.catalog(|state,st|{
                active.reject_history(&state.reconciliation)?;
                if state.reconciliation["terminal_bootstrap_phase"]=="AwaitingNewGuestReadiness" {
                    let namespace=&state.reconciliation["terminal_staged_namespace"];
                    if namespace["realm"]!=active.realm || namespace["session"]!=active.session || st.ddr_resources.active_reserved_bytes_for_terminal()!=4<<20 ||
                        active.nodes.iter().any(|(name,node)|namespace["nodes"].as_array().is_none_or(|nodes|!nodes.iter().any(|n|n["node"]==*name&&n["socket"]==node["socket"]))) {
                        return Err("terminal prepared identities do not bind reserved staged namespace".into());
                    }
                }Ok(())
            })?;
            active.require_ready()?;
            active.bootstrap=self.bootstrap.clone();
            service.catalog(|state,st| {active.reject_history(&state.reconciliation)?;active.admit_bootstrap(st)?;
                if state.reconciliation["terminal_bootstrap_phase"]=="AwaitingNewGuestReadiness"{state.reconciliation["terminal_bootstrap_phase"]=json!("ActiveNewNamespace");}
                state.bootstrap_readiness=active.loaded();Ok(())})?;
            self.active.set(Box::new(active)).map_err(|_|"activation already set")?;
            return Ok(json!({"bootstrap_activated":true,"current_canary_bytes":4<<20}));
        }
        if self.deferred_bootstrap {return Err("original responsibility imported; current canaries reserved but not activated".into());}
        if matches!(op,"CreateManagedPool"|"RegisterPoolClient"|"AllocatePoolRange"|"AcquirePoolRange"|"ExecutePoolRange") {
            if let Err(error)=self.require_ready() {
                self.bootstrap_unknown.store(true,Ordering::Release);
                return Err(error); // No creation reservation or allocation RPC was made.
            }
        }
        if op=="CreateManagedPool" {
            let n=number(req,"nbytes")?;
            let id=opaque_id()?; let epoch=opaque_id()?;
            let reservation=format!("{}:{}",peer.actor(),text(req,"operation_id")?);
            service.catalog(|state,st| {
                if state.reconciliation["identity"]["pool_id"]==id || state.reconciliation["identity"]["pool_epoch"]==epoch {
                    return Err("generated pool identity collides with terminal original history".into());
                }
                if state.pools.values().any(|p|p.state!="Released") || st.shutdown_requested || service.stop.load(Ordering::Acquire) {
                    return Err("one backing only; original obligation prevents replacement pool".into());
                }
                state.create_pending(st,id.clone(),epoch.clone(),self.session.clone(),n,&reservation)?;
                let p=state.pools.get_mut(&id).unwrap();p.backend="mem_service_obmm";
                Ok(())
            })?;
            let result=self.call(&self.home,json!({"op":"allocate","pool_id":id,"epoch":epoch,"nbytes":n}));
            return service.catalog(|state,st| {
                let p=state.pools.get_mut(&id).unwrap();
                p.state="CreationUnknown"; // Already called lower allocate; errors retain reservation.
                match result {
                    Ok(receipt) if receipt["nbytes"]==n && receipt["key"]==id && receipt["state"]=="active"
                        && receipt["incarnation"]==self.nodes[&self.home]["incarnation"] => {
                        let generation=number(&receipt,"generation")?;
                        let backing=BackingId::Provider {realm:self.realm.clone(),provider:"mem_service_obmm".into(),
                            resource:format!("{}:{}:{}",self.home,number(&receipt,"incarnation")?,id),generation};
                        p.canonical=receipt;
                        st.ddr_resources.commit_backing(&reservation,p.object,backing,n)?;
                        p.state="Allocated";
                        let object=st.objects.get_mut(&p.object).unwrap();object.actual_bytes=n;object.state="Allocated".into();
                        object.target=format!("simulated:{}:{}",self.realm,self.home);object.ddr_fast_profile="mem_service_obmm".into();
                        let record=object.clone();st.record_legacy_object(&record);
                        let rb=st.resource_bindings.get_mut(&p.binding).unwrap();
                        rb.provider_id=p.backend.into();rb.provider_session_id=self.session.clone();rb.provider_resource_id=id.clone();
                        rb.binding_generation=generation;rb.opaque_descriptor=serde_json::to_vec(&p.canonical).map_err(|e|e.to_string())?;
                        rb.state="Allocated".into();
                        let placement=st.placements.get_mut(&p.placement).unwrap();
                        placement.provider_id=p.backend.into();placement.execution_realm_id=self.realm.clone();placement.address_kind="provider_only".into();
                        placement.node_id=self.home.clone();placement.domain="ub_domain".into();
                        placement.execution_mode=ExecutionMode::Simulated;placement.actual_fidelity=Fidelity::Functional;
                        placement.requested_fidelity=Fidelity::Functional;placement.addressability=Addressability::ProviderOnly;
                        Ok(p.description())
                    }
                    other => {p.state="CreationUnknown";p.last_error=format!("allocation outcome retained: {other:?}");Err(p.last_error.clone())}
                }
            });
        }
        if op=="ShutdownManagedPool" {
            service.catalog(|state,st| {
                if st.shutdown_requested {return Err("shutdown already in progress; query original operation".into());}
                for pool in state.pools.values() {pool.confirm_closed(st)?;}
                let baseline:u64=st.ddr_resources.retained_bytes()+self.bootstrap.values().filter_map(|id|st.ddr_resources.charged_object_bytes(*id)).sum::<u64>();
                if state.pools.values().any(|p|p.state!="Released") || st.ddr_resources.charged_bytes()!=baseline || st.ddr_resources.reserved_bytes()!=st.ddr_resources.retained_reserved_bytes() {
                    return Err("cannot shutdown with remaining backing/unknown".into());
                }
                st.shutdown_requested=true;
                Ok(())
            })?;
            for node in self.nodes.keys() {
                self.bootstrap_unknown.store(true,Ordering::Release);
                let stopped=self.call(node,json!({"op":"worker_stop"}))?;
                if stopped["worker_stopped"]!=true || stopped["worker_exit_code"]!=0 || stopped["directory_deregistered"]!=true {
                    return Err("actual worker stop/deregister not confirmed".into());
                }
            }
            for node in self.nodes.keys() {
                let drained=self.call(node,json!({"op":"provider_drain"}))?;
                if drained["directory_drained"]!=true || drained["active_count"]!=0 {
                    return Err("actual directory drain not confirmed".into());
                }
            }
            for (node,id) in &self.bootstrap {
                if service.catalog(|_,st|Ok(st.ddr_resources.charged_object_bytes(*id).is_some()))? {
                    self.bootstrap_unknown.store(true,Ordering::Release);
                    let receipt=self.call(node,json!({"op":"bootstrap_close"}))?;
                    if receipt["canary_closed"]!=true || receipt["canary_bytes"]!=0 || receipt["cleanup_required"]!=false {
                        return Err("canary checked-close unconfirmed; original bootstrap charge retained".into());
                    }
                    service.catalog(|_,st|{st.ddr_resources.release_confirmed(*id);Ok(())})?;
                }
            }
            self.finish()?;
            let (retained,reserved)=service.catalog(|_,st|if st.ddr_resources.charged_bytes()==st.ddr_resources.retained_bytes() && st.ddr_resources.reserved_bytes()==st.ddr_resources.retained_reserved_bytes() {Ok((st.ddr_resources.retained_bytes(),st.ddr_resources.retained_reserved_bytes()))} else {Err("current runtime not drained to retained-only ledger".into())})?;
            self.bootstrap_unknown.store(false,Ordering::Release);
            service.stop.store(true,Ordering::Release);
            return Ok(json!({"stopping":true,"bootstrap_closed":true,"charged_bytes":retained,"retained_bytes":retained,"active_bytes":0,
                "reserved_bytes":reserved,"retained_reserved_bytes":reserved,"active_reserved_bytes":0}));
        }
        let pool_id=text(req,"pool_id")?;
        // Validate incarnation on *every* operation, including metadata/cleanup.
        service.catalog(|state,st| {
            let p=state.pools.get(pool_id).ok_or("pool not found")?;
            if req["pool_epoch"]!=p.epoch || req["provider_session"]!=p.provider_session || p.backend!="mem_service_obmm" {
                return Err("stale backend/pool/epoch/provider tuple".into());
            }
            if req.get("client_session").is_some() {
                let access=number(req,"access_generation")?;
                let session=p.clients.get(text(req,"client_session")?).ok_or("client not found")?;
                if session.birth!=peer.birth || session.access_id!=access {return Err("client birth/access mismatch".into());}
                let binding=st.access_bindings.get(&access).ok_or("access binding missing")?;
                let issued:Value=serde_json::from_slice(&binding.opaque_token).map_err(|_|"stored binding unavailable")?;
                if req.get("access_binding_v2")!=Some(&issued) {
                    return Err("stale full backend/realm/node/incarnation/rights/range binding".into());
                }
            }
            Ok(())
        })?;
        if op=="DestroyManagedPool" {
            let canonical=service.catalog(|state,st| {
                let p=state.pools.get_mut(pool_id).unwrap();
                p.confirm_closed(st)?;
                if p.state!="Allocated" || p.clients.values().any(|c|c.state!="Unmapped") || p.ranges.leases.values().any(|l|l.state!="Closed") {
                    return Err("actual client/range/Unknown obligations prevent retire".into());
                }
                p.state="Releasing";Ok(p.canonical.clone())
            })?;
            let closed=self.call(&self.home,json!({"op":"retire","key":pool_id,"generation":canonical["generation"]}));
            return service.catalog(|state,st| {
                let p=state.pools.get_mut(pool_id).unwrap();
                match closed {
                    Ok(r) if r["state"]=="retired" && r["backing_bytes"]==0 && r["live_refs"]==0 && r["in_flight"]==0
                        && r["address_reserved_bytes"]==0 && r["export_mappings"]==0 && r["import_mappings"]==0 && r["quarantined_bytes"]==0 => {
                        p.record_confirmed_release(st,r)?;Ok(json!({"released":true,"lower":p.release_result}))
                    }
                    other=>{p.state="ReleaseUnknown";p.last_error=format!("retire retained: {other:?}");Err(p.last_error.clone())}
                }
            });
        }
        if op=="RegisterPoolClient" {
            let node=text(req,"node")?;if !self.nodes.contains_key(node){return Err("unbound logical node".into());}
            let registered=service.catalog(|state,st| {
                let registered=state.metadata(st,peer,req)?;
                let p=&state.pools[pool_id];
                let access=number(&registered,"access_generation")?;
                let binding=st.access_bindings.get_mut(&access).ok_or("pending binding missing")?;
                binding.accessor_node_id=node.into();binding.accessor_provider_id="mem_service_obmm".into();
                binding.source_binding_generation=number(&p.canonical,"generation")?;
                binding.access_kind=AccessKind::RemoteMap;binding.transport_kind=TransportKind::ObmmGsva;
                binding.addressability=Addressability::RemoteMapping;
                binding.opaque_token=serde_json::to_vec(&json!({"schema":2,"backend":"mem_service_obmm",
                    "realm":self.realm,"node":node,"boot_id":self.nodes[node]["boot_id"],
                    "incarnation":self.nodes[node]["incarnation"],"pool_id":p.id,"pool_epoch":p.epoch,
                    "client_session":registered["client_session"],"access_generation":access,"state":"MappingPending"}))
                    .map_err(|e|e.to_string())?;
                Ok(registered)
            })?;
            let client=text(&registered,"client_session")?.to_owned();
            let canonical=service.catalog(|state,_|Ok(state.pools[pool_id].canonical.clone()))?;
            let peer_node=if node=="nodeA" {"nodeB"} else {"nodeA"};
            let canary=&self.nodes[peer_node]["canary"];
            let mapped=self.call(node,json!({"op":"map","key":pool_id,"generation":canonical["generation"],
                "client_session":client,"access":registered["access"],
                "peer_canary_descriptor":canary_descriptor(canary)?,"checksum":canary["checksum"]}));
            return service.catalog(|state,st| {
                let p=state.pools.get_mut(pool_id).unwrap();let session=p.clients.get_mut(&client).unwrap();
                let binding=st.access_bindings.get_mut(&session.access_id).unwrap();
                match mapped {
                    Ok(receipt) if session.state=="MappingPending" && !session.closing && receipt["mapped"]==true && receipt["holder"]==true && receipt["client_session"]==client
                        && receipt["generation"]==canonical["generation"] && receipt["nbytes"]==p.nbytes => {
                        let versioned=json!({"schema":2,"backend":"mem_service_obmm","realm":self.realm,"node":node,
                            "boot_id":self.nodes[node]["boot_id"],"incarnation":self.nodes[node]["incarnation"],
                            "pool_id":p.id,"pool_epoch":p.epoch,"provider_session":p.provider_session,
                            "client_session":client,"access_generation":session.access_id,"rights":session.access,
                            "offset":0,"nbytes":p.nbytes,"receipt":receipt});
                        binding.accessor_node_id=node.into();binding.accessor_provider_id=p.backend.into();
                        binding.source_binding_generation=number(&canonical,"generation")?;
                        binding.access_kind=AccessKind::RemoteMap;binding.transport_kind=TransportKind::ObmmGsva;
                        binding.addressability=Addressability::RemoteMapping;binding.opaque_token=serde_json::to_vec(&versioned).map_err(|e|e.to_string())?;
                        binding.state="Mapped".into();session.state="Mapped";
                        let mut result=registered;result["access_binding_v2"]=versioned;Ok(result)
                    }
                    other=>{session.state="Unknown";binding.state="Unknown".into();Err(format!("map retained: {other:?}"))}
                }
            });
        }
        if matches!(op,"ExecutePoolRange"|"BeginPoolUnregister") {
            let client=text(req,"client_session")?;
            let control=self.next_sequence()?;
            let (node,lease,managed)=service.catalog(|state,st| {
                let p=state.pools.get_mut(pool_id).unwrap();let session=p.clients.get(client).ok_or("client not found")?;
                if session.birth!=peer.birth || req["access_generation"]!=session.access_id {return Err("client birth/access mismatch".into());}
                let node=st.access_bindings[&session.access_id].accessor_node_id.clone();
                if op=="BeginPoolUnregister" {
                    if p.ranges.cache.client_pending(client)||p.ranges.write.client_pending(client){return Err("native cache must be acknowledged revoked before unmap".into());}
                    if p.ranges.leases.values().any(|l|l.client==client && matches!(l.state,"Active"|"Executing")) {return Err("drain range obligations before unmap".into());}
                    p.clients.get_mut(client).unwrap().closing=true;return Ok((node,None,None));
                }
                p.admission()?;
                if session.closing || session.state!="Mapped" {return Err("mapped live client required".into());}
                let lease=p.validate_lease(&req["lease"],client,session.access_id)?;
                if p.ranges.leases[&lease.token].state!="Active" {return Err("range already executing/unknown/closed".into());}
                p.ranges.write.guard_access(lease.offset,lease.nbytes)?;
                if lease.mode=="w"{p.ranges.cache.guard_writer(lease.offset,lease.nbytes)?;}
                number(req,"pattern_id")?;
                p.ranges.leases.get_mut(&lease.token).unwrap().state="Executing";
                let managed=if self.managed_cache_enabled()&&lease.mode=="r" {
                    let prepared=crate::managed_read_cache::Authority::mint(p,st,&lease,&self.nodes[&node],&self.realm,text(req,"operation_id")?,control)
                        .and_then(|grant|p.ranges.cache.begin(grant,&req["pattern_id"]));
                    match prepared{Ok(body)=>Some(body),Err(error)=>{p.ranges.leases.get_mut(&lease.token).unwrap().state="Active";return Err(error);}}
                }else if self.managed_write_enabled()&&lease.mode=="w" {
                    let prepared=crate::managed_write::Authority::mint(p,st,&lease,&self.nodes[&node],&self.realm,text(req,"operation_id")?,control)
                        .and_then(|grant|p.ranges.write.begin(grant,&req["pattern_id"]));
                    match prepared{Ok(body)=>Some(body),Err(error)=>{p.ranges.leases.get_mut(&lease.token).unwrap().state="Active";return Err(error);}}
                }else{None};
                Ok((node,Some(lease),managed))
            })?;
            if let Some(lease)=lease {
                let cached=managed.is_some();
                let body=managed.unwrap_or_else(||json!({"op":if lease.mode=="w"{"write"}else{"read"},"client_session":client,
                    "offset":lease.offset,"nbytes":lease.nbytes,"pattern_id":req["pattern_id"],"lease_id":lease.token,
                    "content_version":lease.version}));
                let done=self.call_with_id(&node,body,control);
                return service.catalog(|state,st| {
                    let p=state.pools.get_mut(pool_id).unwrap();
                    if p.ranges.leases[&lease.token].state!="Executing" || p.clients[client].state!="Mapped" {
                        p.ranges.unknown(&lease);
                        if cached{if lease.mode=="w"{p.ranges.write.uncertain(control);}else{p.ranges.cache.uncertain(control);}}
                        st.leases.get_mut(&lease.catalog_id).unwrap().state="Unknown".into();
                        return Err("uncertainty/death during execution remains sticky; no commit".into());
                    }
                    match done {
                        Ok(receipt) if receipt["nbytes"]==lease.nbytes && receipt["offset"]==lease.offset
                            && receipt["all_bytes"]==true && receipt["pins"]==0 && receipt["visible"]==true
                            && receipt["pattern_id"]==req["pattern_id"] => {
                            if cached {
                                // Reject incomplete SDK data before consuming the independent W ACK.
                                let finish=if lease.mode=="w" {
                                    match text(&receipt,"checksum"){Ok(s) if !s.is_empty()=>p.ranges.write.finish(control,&receipt),_=>{p.ranges.write.uncertain(control);Err("missing actual W checksum".into())}}
                                }else{p.ranges.cache.finish(control,&receipt)};
                                if let Err(error)=finish{
                                    p.ranges.unknown(&lease);st.leases.get_mut(&lease.catalog_id).unwrap().state="Unknown".into();return Err(error);
                                }
                            }
                            p.ranges.leases.get_mut(&lease.token).unwrap().state="Active";
                            if lease.mode=="w" {
                                let version=p.ranges.commit(&lease,text(&receipt,"checksum")?,lease.nbytes)?;
                                st.leases.get_mut(&lease.catalog_id).unwrap().state="Closed".into();
                                Ok(json!({"committed":true,"content_version":version,"receipt":receipt}))
                            } else {Ok(json!({"verified":true,"content_version":lease.version,"receipt":receipt}))}
                        }
                        other=>{p.ranges.unknown(&lease);if cached{if lease.mode=="w"{p.ranges.write.uncertain(control);}else{p.ranges.cache.uncertain(control);}}
                            st.leases.get_mut(&lease.catalog_id).unwrap().state="Unknown".into();Err(format!("same lease/bytes outcome unknown: {other:?}"))}
                    }
                });
            }
            let unmapped=self.call(&node,json!({"op":"unmap","client_session":client}));
            return service.catalog(|state,st| {
                let p=state.pools.get_mut(pool_id).unwrap();
                match unmapped {
                    Ok(r) if r["client_session"]==client && r["unmapped"]==true && r["holder"]==false && r["pins"]==0 && r["cleanup_required"]==false => {
                        p.confirm_client_unmapped(st,client)?;
                        Ok(json!({"unmapped":true,"lower":r}))
                    }
                    other=>{let session=p.clients.get_mut(client).unwrap();session.state="Unknown";st.access_bindings.get_mut(&session.access_id).unwrap().state="Unknown".into();
                        Err(format!("same map/holder cleanup retained: {other:?}"))}
                }
            });
        }
        service.catalog(|state,st| {
            if op=="ReleasePoolLease" {
                if !req["write_not_started"].is_boolean() { return Err("boolean never-started assertion required".into()); }
                let p=&state.pools[pool_id];let client=text(req,"client_session")?;
                let session=p.clients.get(client).ok_or("client not found")?;
                let lease=p.validate_lease(&req["lease"],client,session.access_id)?;
                if p.ranges.leases[&lease.token].state!="Active" { return Err("cannot release executing or uncertain lower operation".into()); }
            }
            let mut result=state.metadata(st,peer,req)?;
            if op=="GetManagedPoolSnapshot" {
                result["bootstrap_charged_bytes"]=json!(self.bootstrap.values().filter_map(|id|st.ddr_resources.charged_object_bytes(*id)).sum::<u64>());
                result["bootstrap_unknown"]=json!(self.bootstrap_unknown.load(Ordering::Acquire));
            }
            Ok(result)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn real_execute_keeps_executing_and_independent_grant_until_exact_native_end_ack(){
        use std::{os::unix::net::UnixListener,sync::{Arc,mpsc},thread};
        for corrupt in [false,true] {
            let(mut st,mut state,lease,mut identity)=crate::managed_read_cache::tests::fixture();
            let root=std::env::temp_dir().join(format!("uf-cache-host-{}",opaque_id().unwrap()));std::fs::create_dir(&root).unwrap();
            let socket=root.join("b.sock");let listener=UnixListener::bind(&socket).unwrap();
            let access=state.pools["pool"].clients[&lease.client].access_id;
            state.pools.get_mut("pool").unwrap().ranges.leases.get_mut(&lease.token).unwrap().state="Active";
            st.access_bindings.get_mut(&access).unwrap().opaque_token=b"{}".to_vec();
            let lease_json=state.pools["pool"].lease_json(&lease,access);st.managed_pools=state;
            let service=Arc::new(crate::pool_service::managed_read_cache_host_service(st,&root));
            let worker=json!({"schema":2,"phase":"completed","worker_alive":true,"worker_ready":true,"worker_refresh_count":2,"worker_pid":41,"worker_birth":721,"incarnation":8,"readiness_generation":1,"capabilities":1,"lease_ms":1000,"refresh_age_ms":10});
            identity["startup"]=json!({"worker":worker,"directory_epoch":2});identity["socket"]=json!(socket.to_str().unwrap());
            let status=json!({"schema":2,"phase":"completed","node":"nodeB","boot_id":"boot-b","incarnation":8,"readiness_generation":1,"capabilities":1,"channel_ready":true,"directory_registered":true,"directory_ready":true,"required_count":2,"active_count":2,"lease_ms":1000,"directory_epoch":2});
            let provider=MemServicePoolProvider{session:"provider".into(),realm:"realm".into(),nodes:[("nodeB".into(),identity)].into_iter().collect(),home:"nodeB".into(),sequence:AtomicU64::new(BUSINESS_SEQUENCE),bootstrap:BTreeMap::new(),bootstrap_unknown:AtomicBool::new(false),deferred_bootstrap:false,readiness_file:None,reconciliation:None,active:OnceLock::new(),activation:Mutex::new(()),managed_cache_execution:None,managed_cache_host_test:true,managed_write_host_test:false};
            let(entered_tx,entered_rx)=mpsc::channel();let(resume_tx,resume_rx)=mpsc::channel();
            let transport_lease=lease_json.clone();
            let transport=thread::spawn(move||{
                for expected in ["worker_state","provider_status","read"] {
                    let(mut stream,_)=listener.accept().unwrap();let frame=BridgeFrame::read_from(&mut stream).unwrap();let body:Value=serde_json::from_slice(&frame.body).unwrap();assert_eq!(body["op"],expected);
                    let receipt=if expected=="worker_state"{worker.clone()}else if expected=="provider_status"{status.clone()}else{
                        assert_eq!(body["cache_lease_generation"],transport_lease["lease_generation"]);assert_eq!(body["cache_control_generation"],frame.request_id);
                        entered_tx.send(()).unwrap();resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                        json!({"schema":2,"phase":"completed","nbytes":body["nbytes"],"offset":body["offset"],"pattern_id":body["pattern_id"],"all_bytes":true,"visible":true,"pins":0,
                            "cache_version":1,"cache_control_generation":body["cache_control_generation"],"cache_authority_sha256":body["cache_authority_sha256"],"cache_frame_sha256":body["cache_frame_sha256"],"cache_begin_confirmed":true,"cache_end_confirmed":!corrupt,"cache_active":false,"cache_in_flight":0,"cache_pages":0,"cache_transcript_sha256":"ab".repeat(32)})};
                    let mut reply=frame;reply.message_type=BridgeMessageType::Completion;reply.body=serde_json::to_vec(&receipt).unwrap();reply.write_to(&mut stream).unwrap();
                }
            });
            let service_exec=Arc::clone(&service);let lease_token=lease.token.clone();let client=lease.client.clone();
            let execute=thread::spawn(move||{
                let pid=std::process::id() as i32;let peer=Peer{birth:crate::ddr_process_identity::observe(pid,unsafe{libc::geteuid()},unsafe{libc::getegid()}).unwrap(),pidfd:crate::pool_provider::pidfd(pid).unwrap()};
                provider.execute(&service_exec,&peer,&json!({"op":"ExecutePoolRange","operation_id":"host-real-execute","pool_id":"pool","pool_epoch":"epoch","provider_session":"provider","client_session":client,"access_generation":access,"access_binding_v2":{},"lease":lease_json,"pattern_id":17}))
            });
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            service.catalog(|state,_|{let p=&mut state.pools.get_mut("pool").unwrap();assert_eq!(p.ranges.leases[&lease_token].state,"Executing");assert!(!p.ranges.cache.is_empty());assert!(p.ranges.cache.guard_writer(0,65536).is_err());assert!(p.ranges.release(&lease).is_err());Ok(())}).unwrap();
            resume_tx.send(()).unwrap();let result=execute.join().unwrap();transport.join().unwrap();assert_eq!(result.is_err(),corrupt);
            service.catalog(|state,st|{let p=&state.pools["pool"];assert_eq!(p.ranges.cache.is_empty(),!corrupt);assert_eq!(p.ranges.leases[&lease_token].state,if corrupt{"Unknown"}else{"Active"});assert_eq!(st.ddr_resources.charged_bytes(),131072);if corrupt{assert!(p.frozen());assert_eq!(st.leases[&lease.catalog_id].state,"Unknown");}Ok(())}).unwrap();
            drop(service);std::fs::remove_dir_all(&root).unwrap();
        }
    }
    #[test]
    fn real_w_execute_requires_flush_end_before_commit_and_stays_unknown_on_races(){
        use std::{os::unix::net::UnixListener,sync::{Arc,mpsc},thread};
        for scenario in 0..7 {
            let corrupt=(1..=5).contains(&scenario);let enabled=scenario!=6;
            let(mut st,mut state,lease,mut identity)=crate::managed_write::tests::fixture();
            let root=std::env::temp_dir().join(format!("uf-write-host-{}",opaque_id().unwrap()));std::fs::create_dir(&root).unwrap();
            let socket=root.join("b.sock");let listener=UnixListener::bind(&socket).unwrap();
            let access=state.pools["pool"].clients[&lease.client].access_id;
            state.pools.get_mut("pool").unwrap().ranges.leases.get_mut(&lease.token).unwrap().state="Active";
            st.access_bindings.get_mut(&access).unwrap().opaque_token=b"{}".to_vec();
            let lease_json=state.pools["pool"].lease_json(&lease,access);st.managed_pools=state;
            let service=Arc::new(crate::pool_service::managed_read_cache_host_service(st,&root));
            let worker=json!({"schema":2,"phase":"completed","worker_alive":true,"worker_ready":true,"worker_refresh_count":2,"worker_pid":41,"worker_birth":721,"incarnation":8,"readiness_generation":1,"capabilities":1,"lease_ms":1000,"refresh_age_ms":10});
            identity["startup"]=json!({"worker":worker,"directory_epoch":2});identity["socket"]=json!(socket.to_str().unwrap());
            let status=json!({"schema":2,"phase":"completed","node":"nodeB","boot_id":"boot-b","incarnation":8,"readiness_generation":1,"capabilities":1,"channel_ready":true,"directory_registered":true,"directory_ready":true,"required_count":2,"active_count":2,"lease_ms":1000,"directory_epoch":2});
            let provider=MemServicePoolProvider{session:"provider".into(),realm:"realm".into(),nodes:[("nodeB".into(),identity)].into_iter().collect(),home:"nodeB".into(),sequence:AtomicU64::new(BUSINESS_SEQUENCE),bootstrap:BTreeMap::new(),bootstrap_unknown:AtomicBool::new(false),deferred_bootstrap:false,readiness_file:None,reconciliation:None,active:OnceLock::new(),activation:Mutex::new(()),managed_cache_execution:None,managed_cache_host_test:false,managed_write_host_test:enabled};
            let(entered_tx,entered_rx)=mpsc::channel();let(resume_tx,resume_rx)=mpsc::channel();
            let transport_lease=lease_json.clone();
            let transport=thread::spawn(move||{
                for expected in ["worker_state","provider_status","write"] {
                    let(mut stream,_)=listener.accept().unwrap();let frame=BridgeFrame::read_from(&mut stream).unwrap();let body:Value=serde_json::from_slice(&frame.body).unwrap();assert_eq!(body["op"],expected);
                    let receipt=if expected=="worker_state"{worker.clone()}else if expected=="provider_status"{status.clone()}else{
                        if enabled {assert_eq!(body["write_lease_generation"],transport_lease["lease_generation"]);assert_eq!(body["write_control_generation"],frame.request_id);assert_eq!(body["content_version"],0);assert!(body.get("write_content_id").is_none());}else{assert!(body.get("write_version").is_none());}
                        entered_tx.send(()).unwrap();resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                        let mut r=json!({"schema":2,"phase":"completed","nbytes":body["nbytes"],"offset":body["offset"],"pattern_id":body["pattern_id"],"all_bytes":true,"visible":true,"pins":0,"checksum":"0123456789abcdef"});
                        if enabled {for(k,v)in crate::managed_write::tests::ack(&body).as_object().unwrap(){r[k]=v.clone();}}
                        match scenario{1=>r["write_dirty_bytes"]=json!(1),2=>r["write_flush_confirmed"]=json!(false),3=>r["write_end_confirmed"]=json!(false),4=>{r.as_object_mut().unwrap().remove("checksum");},_=>{}}
                        r};
                    let mut reply=frame;reply.message_type=BridgeMessageType::Completion;reply.body=serde_json::to_vec(&receipt).unwrap();reply.write_to(&mut stream).unwrap();
                }
            });
            let service_exec=Arc::clone(&service);let lease_token=lease.token.clone();let client=lease.client.clone();
            let execute=thread::spawn(move||{
                let pid=std::process::id() as i32;let peer=Peer{birth:crate::ddr_process_identity::observe(pid,unsafe{libc::geteuid()},unsafe{libc::getegid()}).unwrap(),pidfd:crate::pool_provider::pidfd(pid).unwrap()};
                provider.execute(&service_exec,&peer,&json!({"op":"ExecutePoolRange","operation_id":"host-real-w-execute","pool_id":"pool","pool_epoch":"epoch","provider_session":"provider","client_session":client,"access_generation":access,"access_binding_v2":{},"lease":lease_json,"pattern_id":17}))
            });
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            service.catalog(|state,st|{let p=state.pools.get_mut("pool").unwrap();assert_eq!(p.ranges.leases[&lease_token].state,"Executing");assert_eq!(st.leases[&lease.catalog_id].state,"Writing");
                assert_eq!(p.ranges.write.is_empty(),!enabled);assert!(p.ranges.cache.is_empty());assert_eq!(p.ranges.blocks[&lease.block].segments.last().unwrap().version,0);
                if enabled{assert!(p.ranges.write.guard_access(65536,65536).is_err());assert!(p.ranges.release(&lease).is_err());assert!(p.ranges.commit(&lease,"early",65536).is_err());assert!(p.confirm_client_unmapped(st,&lease.client).is_err());}
                if scenario==5{p.ranges.unknown(&lease);}
                Ok(())}).unwrap();
            resume_tx.send(()).unwrap();let result=execute.join().unwrap();transport.join().unwrap();assert_eq!(result.is_err(),corrupt);
            service.catalog(|state,st|{let p=&state.pools["pool"];assert_eq!(p.ranges.write.is_empty(),!corrupt);assert_eq!(p.ranges.leases[&lease_token].state,if corrupt{"Unknown"}else{"Closed"});assert_eq!(st.ddr_resources.charged_bytes(),131072);
                if corrupt{assert!(p.frozen());assert_eq!(st.leases[&lease.catalog_id].state,"Unknown");assert_eq!(p.ranges.blocks[&lease.block].segments.last().unwrap().version,0);}else{assert_eq!(st.leases[&lease.catalog_id].state,"Closed");assert_eq!(p.ranges.blocks[&lease.block].segments.last().unwrap().content,"0123456789abcdef");}Ok(())}).unwrap();
            drop(service);std::fs::remove_dir_all(&root).unwrap();
        }
    }
    #[test]
    fn admission_requires_real_worker_freshness_directory_and_boot_binding() {
        let worker=json!({"schema":2,"phase":"completed","worker_alive":true,"worker_ready":true,
            "worker_refresh_count":2,"worker_pid":41,"worker_birth":721,"incarnation":7,
            "readiness_generation":1,"capabilities":1,"lease_ms":1000,"refresh_age_ms":10});
        let identity=json!({"node":"nodeA","boot_id":"boot-a","incarnation":7,
            "startup":{"worker":worker,"directory_epoch":2}});
        let status=json!({"schema":2,"phase":"completed","node":"nodeA","boot_id":"boot-a","incarnation":7,
            "readiness_generation":1,"capabilities":1,"channel_ready":true,"directory_registered":true,
            "directory_ready":true,"required_count":2,"active_count":2,"lease_ms":1000,"directory_epoch":2,
            "host_registry_data_plane_ready":false});
        assert_eq!(ready_receipts(&identity,&worker,&status).unwrap(),2);
        for (key,value) in [("worker_alive",json!(false)),("worker_ready",json!(false)),
            ("worker_refresh_count",json!(0)),("worker_pid",json!(42)),("worker_birth",json!(722)),
            ("incarnation",json!(8)),("readiness_generation",json!(2)),("capabilities",json!(0)),
            ("refresh_age_ms",json!(500)),("lease_ms",json!(900))] {
            let mut bad=worker.clone();bad[key]=value;assert!(ready_receipts(&identity,&bad,&status).is_err(),"{key}");
        }
        for (key,value) in [("directory_ready",json!(false)),("active_count",json!(1)),
            ("directory_registered",json!(false)),("channel_ready",json!(false)),("boot_id",json!("old")),
            ("incarnation",json!(8)),("directory_epoch",json!(3)),("readiness_generation",json!(2))] {
            let mut bad=status.clone();bad[key]=value;assert!(ready_receipts(&identity,&worker,&bad).is_err(),"{key}");
        }
    }
    #[test]
    fn canary_opaque_descriptor_bound_is_not_general_payload_permission() {
        assert!(canary_descriptor(&json!({"canary_descriptor":"ab".repeat(192)})).is_ok());
        for value in [json!("a"),json!("zz"),json!("ab".repeat(193)),json!(123)] {
            assert!(canary_descriptor(&json!({"canary_descriptor":value})).is_err());
        }
    }
    #[test]
    fn all_retained_generations_forbid_runtime_identity_reuse_before_rpc(){
        let mut st=Catalog::new(-1,Vec::new());st.managed_pools.reconciliation=crate::pool_c039_prestart::fixture_proof()["carry"].clone();
        let fresh=||MemServicePoolProvider {session:"fresh-session".into(),realm:"simulated:fresh".into(),
            nodes:[("nodeA".into(),json!({"boot_id":"fresh-a","agent_session":"fresh-a","incarnation":17})),
                ("nodeB".into(),json!({"boot_id":"fresh-b","agent_session":"fresh-b","incarnation":19}))].into_iter().collect(),
            home:"nodeA".into(),sequence:AtomicU64::new(BUSINESS_SEQUENCE),bootstrap:BTreeMap::new(),
            bootstrap_unknown:AtomicBool::new(false),deferred_bootstrap:true,readiness_file:None,reconciliation:None,
            active:OnceLock::new(),activation:Mutex::new(()),managed_cache_execution:None,managed_cache_host_test:false,managed_write_host_test:false};
        assert!(fresh().reject_retained_reuse(&st).is_ok());
        for c in st.managed_pools.reconciliation["retained_canaries"].as_array().unwrap(){
            let mut p=fresh();p.realm=c["backing"]["realm"].as_str().unwrap().into();assert!(p.reject_retained_reuse(&st).is_err());
            let mut p=fresh();p.session=c["provider_session"].as_str().unwrap().into();assert!(p.reject_retained_reuse(&st).is_err());
            for (key,value) in [("boot_id",c["boot_id"].clone()),("agent_session",c["boot_id"].clone()),
                    ("incarnation",c["backing"]["generation"].clone())]{
                let mut p=fresh();p.nodes.get_mut("nodeA").unwrap()[key]=value;
                assert!(p.reject_retained_reuse(&st).is_err(),"{key}");
            }
        }
        for c in st.managed_pools.reconciliation["historical_prestart_contexts"].as_array().unwrap(){
            let mut p=fresh();p.realm=c["realm"].as_str().unwrap().into();assert!(p.reject_retained_reuse(&st).is_err());
            let mut p=fresh();p.session=c["provider_session"].as_str().unwrap().into();assert!(p.reject_retained_reuse(&st).is_err());
            for identity in c["guest_identities"].as_array().unwrap(){for key in ["boot_id","agent_session","incarnation"]{
                let mut p=fresh();p.nodes.get_mut("nodeA").unwrap()[key]=identity[key].clone();assert!(p.reject_retained_reuse(&st).is_err());
            }}
        }
        assert_eq!(st.ddr_resources.charged_bytes(),0);
    }
    #[test]
    fn both_canary_backings_share_physical_budget_with_business_not_each_view() {
        let nodes=[("nodeA".into(),json!({"boot_id":"boot-a","incarnation":7})),
            ("nodeB".into(),json!({"boot_id":"boot-b","incarnation":8}))].into_iter().collect();
        let mut provider=MemServicePoolProvider {session:"test".into(),realm:"simulated:test".into(),
            nodes,home:"nodeA".into(),sequence:AtomicU64::new(BUSINESS_SEQUENCE),bootstrap:BTreeMap::new(),bootstrap_unknown:AtomicBool::new(false),
            deferred_bootstrap:false,readiness_file:None,reconciliation:None,active:OnceLock::new(),activation:Mutex::new(()),managed_cache_execution:None,managed_cache_host_test:false,managed_write_host_test:false};
        let mut st=Catalog::new(-1,Vec::new());st.ddr_resources.limit=128<<20;
        provider.admit_bootstrap(&mut st).unwrap();
        assert_eq!(provider.bootstrap.len(),2);assert_eq!(st.ddr_resources.charged_bytes(),4<<20);
        st.ddr_resources.reserve("business",64<<20).unwrap();
        assert_eq!(st.ddr_resources.charged_bytes()+st.ddr_resources.reserved_bytes(),68<<20);
        assert!(st.ddr_resources.reserve("over-budget",61<<20).is_err());
        for id in provider.bootstrap.values() {assert_eq!(st.ddr_resources.charged_object_bytes(*id),Some(2<<20));}
        // This capacity test does not call release_confirmed or invent lower close evidence.
    }
}
