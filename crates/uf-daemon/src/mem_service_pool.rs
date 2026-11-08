//! Opt-in simulated backend. Catalog/managed_pool/pool_ranges remain authority.
//! All provider calls are outside Catalog; no FD fiction or payload transport.
use crate::ddr_resources::BackingId;
use crate::managed_pool::{Operation, Peer};
use crate::pool_provider::opaque_id;
use crate::pool_service::Service;
use crate::pool_wire::{fields, number, parse, text};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use uf_core::bridge::{BridgeFrame, BridgeMessageType};
use uf_core::provider::{AccessKind, Addressability, TransportKind, ExecutionMode, Fidelity};

pub struct MemServicePoolProvider {
    pub session: String,
    realm: String,
    nodes: BTreeMap<String, Value>,
    home: String,
    sequence: AtomicU64,
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
        fields(&c, &["schema", "backend", "realm", "session", "home", "nodes"], &[])?;
        if c["schema"] != 2 || c["backend"] != "mem_service_obmm" { return Err("explicit mem_service config v2 required".into()); }
        let mut nodes = BTreeMap::new();
        for node in c["nodes"].as_array().ok_or("node array required")? {
            fields(node, &["node", "boot_id", "agent_session", "incarnation", "socket"], &[])?;
            let id = text(node, "node")?;
            if !matches!(id, "nodeA" | "nodeB") || nodes.contains_key(id) || number(node, "incarnation")? == 0 {
                return Err("two unique bound logical nodes required".into());
            }
            text(node, "boot_id")?; text(node, "agent_session")?;
            if !PathBuf::from(text(node, "socket")?).is_absolute() { return Err("absolute task endpoint required".into()); }
            nodes.insert(id.to_owned(), node.clone());
        }
        let home = text(&c, "home")?.to_owned();
        if !text(&c,"realm")?.starts_with("simulated:") { return Err("explicit simulated execution realm required".into()); }
        if nodes.len() != 2 || !nodes.contains_key(&home) { return Err("two nodes and bound home required".into()); }
        Ok(Self { session: text(&c, "session")?.into(), realm: text(&c, "realm")?.into(),
            nodes, home, sequence: AtomicU64::new(1) })
    }

    pub fn loaded(&self) -> Value {
        json!({"backend":"mem_service_obmm","schema":2,"realm":self.realm,
            "session":self.session,"home":self.home,"nodes":self.nodes,
            "source":"546985e26bce60798fa3f88329c3fbed8350c798",
            "fidelity":"functional-simulation","payload_rpc_bytes":0})
    }

    fn call(&self, node: &str, mut body: Value) -> Result<Value, String> {
        let identity = self.nodes.get(node).ok_or("unbound logical node")?;
        body["schema"] = json!(2);
        let id = self.sequence.fetch_update(Ordering::Relaxed,Ordering::Relaxed,|n|n.checked_add(1))
            .map_err(|_| "bridge request sequence exhausted; original session cannot wrap")?;
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
        for node in self.nodes.keys() {
            let receipt = self.call(node, json!({"op":"inspect"}))?;
            if receipt["contexts"] != 0 || receipt["pins"] != 0 || receipt["cleanup_required"] != false {
                return Err("actual lower context drain not confirmed".into());
            }
        }
        Ok(())
    }

    pub fn handle(&self, service: &Service, peer: &Peer, req: &Value) -> Result<Value, String> {
        let id = text(req, "operation_id")?;
        let op = text(req, "op")?;
        let key = format!("{}:{id}", peer.actor());
        // Strict schema/backend discriminator; schema1 cannot access this mode.
        if number(req, "schema")? != 2 { return Err("mem_service requires explicit schema2 client".into()); }
        let common = ["schema","op","operation_id","pool_id","pool_epoch","provider_session","client_session","access_generation","access_binding_v2"];
        let extra: &[&str] = match op {
            "CreateManagedPool" => &["admin_token","nbytes"],
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
        if matches!(op,"CreateManagedPool"|"DestroyManagedPool"|"ShutdownManagedPool") && req["admin_token"] != service.admin {
            return Err("management capability required".into());
        }
        let mut fingerprint = req.clone(); fingerprint.as_object_mut().unwrap().remove("admin_token");
        let fingerprint = fingerprint.to_string();
        let read_only = matches!(op,"DescribeManagedPool"|"GetManagedPoolSnapshot"|"QueryPoolOperation");
        let prior = service.catalog(|state,st| {
            if op == "QueryPoolOperation" {
                let target = text(req,"target_operation_id")?;
                return Ok(Some(envelope(id,Ok(state.operations.get(&format!("{}:{target}",peer.actor()))
                    .map(|v|json!({"state":v.state,"response":v.response}))
                    .unwrap_or(json!({"state":"NotFound"}))),false)));
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
            let unknown = state.pools.values().any(|p|p.state.contains("Unknown") || p.frozen()
                || p.ranges.leases.values().any(|l|l.state=="Unknown"));
            let response = envelope(id,result,unknown);
            if !read_only { state.operations.insert(key,Operation { request:fingerprint,
                state:if response["status"]=="ok" {"Completed"} else if unknown {"Unknown"} else {"Rejected"},response:response.clone() }); }
            Ok(response)
        })
    }

    fn execute(&self, service:&Service, peer:&Peer, req:&Value) -> Result<Value,String> {
        let op=text(req,"op")?;
        if op=="CreateManagedPool" {
            let n=number(req,"nbytes")?;
            let id=opaque_id()?; let epoch=opaque_id()?;
            let reservation=format!("{}:{}",peer.actor(),text(req,"operation_id")?);
            service.catalog(|state,st| {
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
                if state.pools.values().any(|p|p.state!="Released") || st.ddr_resources.charged_bytes()!=0 || st.ddr_resources.reserved_bytes()!=0 {
                    return Err("cannot shutdown with remaining backing/unknown".into());
                }
                st.shutdown_requested=true;
                Ok(())
            })?;
            self.finish()?;
            service.stop.store(true,Ordering::Release);
            return Ok(json!({"stopping":true}));
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
            let canonical=service.catalog(|state,_| {
                let p=state.pools.get_mut(pool_id).unwrap();
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
                        p.state="Released";p.release_result=r;st.ddr_resources.release_confirmed(p.object);
                        st.objects.remove(&p.object);st.mark_object_released(p.object);Ok(json!({"released":true,"lower":p.release_result}))
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
            let mapped=self.call(node,json!({"op":"map","key":pool_id,"generation":canonical["generation"],
                "client_session":client,"access":registered["access"]}));
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
            let (node,lease)=service.catalog(|state,st| {
                let p=state.pools.get_mut(pool_id).unwrap();let session=p.clients.get(client).ok_or("client not found")?;
                if session.birth!=peer.birth || req["access_generation"]!=session.access_id {return Err("client birth/access mismatch".into());}
                let node=st.access_bindings[&session.access_id].accessor_node_id.clone();
                if op=="BeginPoolUnregister" {
                    if p.ranges.leases.values().any(|l|l.client==client && matches!(l.state,"Active"|"Executing")) {return Err("drain range obligations before unmap".into());}
                    p.clients.get_mut(client).unwrap().closing=true;return Ok((node,None));
                }
                p.admission()?;
                if session.closing || session.state!="Mapped" {return Err("mapped live client required".into());}
                let lease=p.validate_lease(&req["lease"],client,session.access_id)?;
                if p.ranges.leases[&lease.token].state!="Active" {return Err("range already executing/unknown/closed".into());}
                number(req,"pattern_id")?;
                p.ranges.leases.get_mut(&lease.token).unwrap().state="Executing";
                Ok((node,Some(lease)))
            })?;
            if let Some(lease)=lease {
                let done=self.call(&node,json!({"op":if lease.mode=="w"{"write"}else{"read"},"client_session":client,
                    "offset":lease.offset,"nbytes":lease.nbytes,"pattern_id":req["pattern_id"],"lease_id":lease.token,
                    "content_version":lease.version}));
                return service.catalog(|state,st| {
                    let p=state.pools.get_mut(pool_id).unwrap();
                    if p.ranges.leases[&lease.token].state!="Executing" || p.clients[client].state!="Mapped" {
                        p.ranges.unknown(&lease);
                        return Err("uncertainty/death during execution remains sticky; no commit".into());
                    }
                    match done {
                        Ok(receipt) if receipt["nbytes"]==lease.nbytes && receipt["offset"]==lease.offset
                            && receipt["all_bytes"]==true && receipt["pins"]==0 && receipt["visible"]==true
                            && receipt["pattern_id"]==req["pattern_id"] => {
                            p.ranges.leases.get_mut(&lease.token).unwrap().state="Active";
                            if lease.mode=="w" {
                                let version=p.ranges.commit(&lease,text(&receipt,"checksum")?,lease.nbytes)?;
                                st.leases.get_mut(&lease.catalog_id).unwrap().state="Closed".into();
                                Ok(json!({"committed":true,"content_version":version,"receipt":receipt}))
                            } else {Ok(json!({"verified":true,"content_version":lease.version,"receipt":receipt}))}
                        }
                        other=>{p.ranges.unknown(&lease);Err(format!("same lease/bytes outcome unknown: {other:?}"))}
                    }
                });
            }
            let unmapped=self.call(&node,json!({"op":"unmap","client_session":client}));
            return service.catalog(|state,st| {
                let p=state.pools.get_mut(pool_id).unwrap();let session=p.clients.get_mut(client).unwrap();
                match unmapped {
                    Ok(r) if r["client_session"]==client && r["unmapped"]==true && r["holder"]==false && r["pins"]==0 && r["cleanup_required"]==false => {
                        p.ranges.reconcile_unmapped(client);
                        session.state="Unmapped";session.pidfd=None;
                        st.access_bindings.get_mut(&session.access_id).unwrap().state="Closed".into();st.clients.remove(&session.catalog_id);
                        Ok(json!({"unmapped":true,"lower":r}))
                    }
                    other=>{session.state="Unknown";st.access_bindings.get_mut(&session.access_id).unwrap().state="Unknown".into();
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
            state.metadata(st,peer,req)
        })
    }
}
