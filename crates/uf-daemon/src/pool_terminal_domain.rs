//! Private terminal CPU domain loss, distinct from normal SDK release.
//! Accepted E evidence is consumed via fresh source execution. A resolved
//! execution manifest and real E0 context are required before original I/O.
use crate::{catalog::Catalog,ddr_resources::DdrResources,ddr_recovery_coverage::digest,
            pool_retained8::{atomic_record,lock_lineage}};
use serde_json::{json,Value};
use std::{collections::{BTreeMap,BTreeSet},fs::{self,File,OpenOptions},io::Read,path::{Path,PathBuf}};
use std::os::unix::fs::{DirBuilderExt,MetadataExt,OpenOptionsExt};

const STATE_SHA:&str="b6592a7442e7cb8540f340168a215fe6cc8cf669c7e87fedb7403ced2fc6c307";
const SEMANTICS_SHA:&str="02975e42457aea8f881fbbe72d0ae8b70d62c879273a0084de8ac610e913fc02";
const NEXT:&str="successor-c044-terminal-domain-loss-v1";
const STATE:&[u8]=include_bytes!("../../../examples/mem_service_pool/terminal_domain/c044-state.json");
const PINS:&str=include_str!("../../../examples/mem_service_pool/terminal_domain/predecessor-pins.json");
const SEMANTICS:&[u8]=include_bytes!("../../../examples/mem_service_pool/terminal_domain/external-semantics.json");
const ROOT:&str="/workspace/uflow/runs/mem-service-shared-pool-20261108";
const E_PINS:&str=include_str!("../../../examples/mem_service_pool/terminal_domain/accepted-E-consumer-pins.json");
const E_PIN_SHA:&str="b793a03e31b41709ba7002a8305eb83b06efaa4f114997f67307760597d6fdb8";
const E_ADAPTER:&str=include_str!("../../../tools/phasec/terminal_domain_adapter.py");

/// Bare build probes must fail before reading original state or calling E.
pub(crate) fn production_terminal_only()->Result<Value,String>{
    Err("production-not-ready: explicit reviewed execution manifest and actual E0 context required; no authority/ledger/verifier/Guest action performed".into())
}

fn adapter(mut request:Value)->Result<Value,String>{
    use std::{io::Write,process::{Command,Stdio}};
    if digest(E_PINS.as_bytes())!=E_PIN_SHA{return Err("compiled external E pins drift".into());}
    request["consumer_pins"]=json!(E_PINS);
    let mut child=Command::new("/usr/local/python3.11.14/bin/python3.11")
        .args(["-B","-I","-S","-c",E_ADAPTER]).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped())
        .spawn().map_err(|e|e.to_string())?;
    child.stdin.take().ok_or("adapter stdin")?.write_all(&serde_json::to_vec(&request).map_err(|e|e.to_string())?).map_err(|e|e.to_string())?;
    let output=child.wait_with_output().map_err(|e|e.to_string())?;
    if !output.status.success()||output.stdout.len()>256<<10||output.stderr.len()>4096 {
        return Err(format!("fresh accepted E adapter rejected: {}",String::from_utf8_lossy(&output.stderr[..output.stderr.len().min(1200)])));
    }
    parsed(&output.stdout)
}

fn from_e_proof(bytes:&[u8],proof:Value)->Result<VerifiedCandidate,String>{
    let pins:Value=serde_json::from_str(E_PINS).map_err(|e|e.to_string())?;let c=&proof["validated_candidate"];
    if digest(bytes)!=STATE_SHA || proof["schema"]!=1 || proof["kind"]!="EValidatedTerminalDomainProofV1" ||
        proof["consumer_pins_sha256"]!=E_PIN_SHA || proof["producer_commit"]!=pins["producer_commit"] ||
        proof["verified_module_sources"]!=pins["modules"] ||
        c["candidate_id"]!=pins["candidate_id"] || c["state_sha256"]!=STATE_SHA || c["applies_to_current_domain"]!=true ||
        c["interface"]!="mem-service-private-qemu-domain-v1" || c["status"]!="PASS_HOST_CANDIDATE_ONLY" ||
        c["evidence_mode"]!="fixed-launch-source" || c["source_derived_inference"]!=true || c["historical_proc_or_QMP_capture_claimed"]!=false ||
        c["only_eligible_resource_ids"]!=json!([110,111,112]) || c["current_eligible_capacity_bytes"]!=52<<20 ||
        c["protected_historical_retained_bytes"]!=12<<20 || c["protected_historical_reserved_bytes"]!=64<<20 ||
        c["missing_evidence"]!=json!([]) || c["physical_closure"]["exact_union_files"]!=51 ||
        c["physical_closure"]["registry_head_sha256"]!=pins["metadata_registry_head_sha256"] {
        return Err("fresh real accepted current-domain proof/scope required".into());
    }
    for key in ["candidate","domain_plan","producer_source_manifest","companion_plan"] {
        if proof["evidence_references"][key]!=pins[key]{return Err("accepted evidence reference drift".into());}
    }
    for key in ["runtime_E1_E5_passed","normal_native_or_SDK_release_ACK_proven","ledger_fences_applied","authority_to_mutate_ledger",
        "authority_to_launch_new_Guest","authority_to_destroy_FM_payload_files","normal_history_state_transition_performed"] {
        if c[key]!=false{return Err("E evidence must not claim C authority/native ACK".into());}
    }
    let state=parsed(bytes)?;let fences=make_fences(&state);
    Ok(VerifiedCandidate{state,proof_sha:digest(&serde_json::to_vec(&proof).map_err(|e|e.to_string())?),proof,fences,
        terminal_ids:vec![110,111,112],source_sha:STATE_SHA.into(),prior_backings:json!({})})
}

fn verify_e(bytes:&[u8])->Result<VerifiedCandidate,String>{
    from_e_proof(bytes,adapter(json!({"action":"verify-current"}))?)
}

/// Explicit read-only HOST integration: no authority lock, transaction or Guest.
pub(crate) fn production_verify_readonly()->Result<Value,String>{
    let state=bounded(&Path::new(ROOT).join("runs/c044/state/state.json"),1<<20)?;
    Ok(verify_e(&state)?.proof)
}

fn execution_context(path:&Path,sha:&str,phase:&str)->Result<(),String>{
    let namespace=path.parent().and_then(|p|p.file_name()).and_then(|s|s.to_str()).unwrap_or("");
    let valid=namespace.strip_prefix("config-terminal-c").and_then(|s|s.split_once("-v"))
        .is_some_and(|(job,version)|job.len()==3&&job.bytes().all(|b|b.is_ascii_digit())&&
            !version.is_empty()&&version.len()<=3&&!version.starts_with('0')&&version.bytes().all(|b|b.is_ascii_digit()));
    if !valid || path.file_name().and_then(|s|s.to_str())!=Some("execution.json") ||
        path.parent().and_then(|p|p.parent())!=Some(Path::new(ROOT).join("implementation").as_path()) ||
        sha.len()!=64 || !sha.bytes().all(|b|b.is_ascii_hexdigit()) {
        return Err("production-not-ready: fixed task-local resolved execution manifest and external digest required before original I/O".into());
    }
    let raw=bounded(path,1<<20)?;
    if digest(&raw)!=sha{return Err("execution manifest external hash mismatch".into());}
    let result=adapter(json!({"action":"execution-context","execution_action":phase,
        "execution_manifest":{"path":path,"bytes":raw.len(),"sha256":sha},"daemon_pid":std::process::id(),
        "daemon_path":std::env::current_exe().map_err(|e|e.to_string())?}))?;
    if result["phase"]!=phase||result["execution_manifest_sha256"]!=sha{return Err("resolved execution context rejected".into());}Ok(())
}

/// Exercise the actual reviewed E0 execution context without E validation,
/// original state access, authority acquisition, transaction or Guest startup.
pub(crate) fn production_execution_check(path:&Path,sha:&str,phase:&str)->Result<Value,String>{
    execution_context(path,sha,phase)?;
    Ok(json!({"status":"PASS_READONLY_EXECUTION_CONTEXT_ONLY","phase":phase,
        "execution_manifest_sha256":sha,"original_authority_lock_or_state_or_E_proof_or_transaction_or_Guest":false}))
}

pub(crate) fn production_terminal_manifest(path:&Path,sha:&str)->Result<Value,String>{
    execution_context(path,sha,"terminal-only")?;
    let a=transact(&Path::new(ROOT).join("implementation/responsibility-c013"),
        &Path::new(ROOT).join("runs/c044/state/state.json"),verify_e,"")?;
    a.closure_report()
}

/// Recovery revalidates the accepted proof under the original authority, then
/// durably claims the successor and reserves4 before any Guest identity exists.
pub(crate) fn restore_staged(st:&mut Catalog,config:&Value,loaded:&Value)->Result<File,String>{
    let p=config["terminal_c044_execution_manifest"].as_str().ok_or("terminal execution manifest")?;
    let sha=config["terminal_c044_execution_manifest_sha256"].as_str().ok_or("terminal execution digest")?;
    execution_context(Path::new(p),sha,"successor")?;
    let mut a=recover_committed(&Path::new(ROOT).join("implementation/responsibility-c013"),
        &Path::new(ROOT).join("runs/c044/state/state.json"),verify_e)?;
    let nodes=loaded["nodes"].as_object().ok_or("deferred provider nodes")?.values().cloned().collect::<Vec<_>>();
    a.attach_staged(st,&json!({"realm":loaded["realm"],"session":loaded["session"],"nodes":nodes}))
}

fn bounded(path:&Path,max:u64)->Result<Vec<u8>,String>{
    let mut f=OpenOptions::new().read(true).custom_flags(libc::O_NOFOLLOW|libc::O_NONBLOCK).open(path).map_err(|e|e.to_string())?;
    let first=f.metadata().map_err(|e|e.to_string())?;
    if !first.is_file()||first.len()>max||first.nlink()!=1{return Err("single-link bounded ordinary metadata required".into());}
    let mut bytes=Vec::new();f.by_ref().take(max+1).read_to_end(&mut bytes).map_err(|e|e.to_string())?;
    let last=f.metadata().map_err(|e|e.to_string())?;let named=path.symlink_metadata().map_err(|e|e.to_string())?;
    if bytes.len() as u64!=first.len()||first.dev()!=last.dev()||first.ino()!=last.ino()||first.mtime()!=last.mtime()||first.mtime_nsec()!=last.mtime_nsec()||
        first.ctime()!=last.ctime()||first.ctime_nsec()!=last.ctime_nsec()||first.len()!=last.len()||named.dev()!=last.dev()||named.ino()!=last.ino()||named.file_type().is_symlink(){return Err("metadata changed while observed".into());}
    Ok(bytes)
}
fn parsed(bytes:&[u8])->Result<Value,String>{serde_json::from_slice(bytes).map_err(|e|e.to_string())}
fn rows(value:&Value)->Result<BTreeMap<u64,Value>,String>{
    let mut result=BTreeMap::new();
    for row in value["resources"].as_array().ok_or("resource array")? {
        if result.insert(row["object_id"].as_u64().ok_or("object id")?,row.clone()).is_some(){return Err("duplicate source object".into());}
    }Ok(result)
}

pub(crate) struct VerifiedCandidate {state:Value,proof:Value,proof_sha:String,fences:Value,
    terminal_ids:Vec<u64>,source_sha:String,prior_backings:Value}
impl VerifiedCandidate {
    pub(crate) fn source_ledger(&self)->&Value{&self.state["resource_ledger"]}
    pub(crate) fn matches_ledger(&self,ledger:&Value)->Result<(),String>{
        let source=self.source_ledger();
        if rows(source)?!=rows(ledger)? || ["charged_bytes","reserved_bytes","retained_bytes","retained_reserved_bytes","active_reserved_bytes","active_bytes",
            "reservations","retired_unstarted_object_ids","retained_uncertain_reservations","terminal_domain_loss_tombstones"].iter().any(|k|source[*k]!=ledger[*k]){
            return Err("exact original128 ledger/backing/witness required before terminal loss".into());
        }Ok(())
    }
    pub(crate) fn prior_backings(&self)->&Value{&self.prior_backings}
    pub(crate) fn terminal_ids(&self)->&[u64]{&self.terminal_ids}
    pub(crate) fn tombstone(&self)->Value{json!({"kind":"TerminalDomainDataLost","source_state_sha256":self.source_sha,
        "proof_sha256":self.proof_sha,"normal_SDK_ACK":false,"data_valid":false,"permanent_anti_revival":true})}
}

// Internal consumer schema is intentionally not presented as E's final public
// API. Only cfg(test) may mint this interim candidate until that API is accepted.
#[cfg(test)]
fn validate_candidate(bytes:&[u8],proof:&Value)->Result<VerifiedCandidate,String>{
    #[cfg(not(test))]
    if proof["CPU_fixture_only_not_real_platform_proof"]==true{return Err("synthetic fixture can never authorize production terminal loss".into());}
    if proof["applies_to_current_domain"]!=true || !matches!(proof["evidence_mode"].as_str(),Some("captured-runtime"|"fixed-launch-source")) {
        return Err("accepted real current-domain evidence method required; synthetic/non-current candidates cannot be consumed".into());
    }
    if digest(bytes)!=STATE_SHA || digest(SEMANTICS)!=SEMANTICS_SHA{return Err("fixed state/source semantics mismatch".into());}
    let state=parsed(bytes)?;let pool=&state["pools"][0];let external=parsed(SEMANTICS)?;
    if state["revision"]!=153 || state["charged_bytes"]!=64<<20 || state["creation_reserved_bytes"]!=64<<20 ||
        state["active_bytes"]!=52<<20 || pool["frozen"]!=true || pool["release_result"]!=Value::Null || pool["clients"].as_array().map(Vec::len)!=Some(3) ||
        proof["interface"]!="mem-service-private-qemu-domain-v1" || proof["status"]!="PASS_HOST_CANDIDATE_ONLY" ||
        proof["state_sha256"]!=STATE_SHA || proof["external_semantics_sha256"]!=SEMANTICS_SHA ||
        proof["scope"]!=external["candidate_scope"] || proof["source_runtime"]!=external["fixed_source_and_runtime"] ||
        proof["missing_evidence"]!=json!([]) || proof["required_output_truths"]!=external["required_output_truths"] {
        return Err("complete fixed terminal-domain scope and source/runtime binding required".into());
    }
    let closure=&proof["closure"];
    if closure["owners_gone"]!=15 || closure["original_carriers"]!=50 || closure["registered_dynamic_metadata"]!=1 ||
        closure["FD_holders"]!=0 || closure["VMA_holders"]!=0 || closure["denied"]!=0 || closure["actual_endpoints_inactive"]!=json!(["u.sock","l.sock","a.sock","b.sock","nodeA.qmp","nodeA.serial","nodeB.qmp","nodeB.serial"]) ||
        closure["hardware_or_external_backing"]!=false || closure["migration_snapshot_or_shared_RAM"]!=false || closure["all_owners_and_visibility_covered"]!=true {
        return Err("full owner/backing/control/visibility closure required; PID/FD absence alone insufficient".into());
    }
    // A real adapter must independently read/hash all of these named evidence
    // bytes under the lock; this interim pure consumer is fixture-only.
    for key in ["producer_manifest_sha256","validator_sha256","source_closure_sha256","runtime_closure_sha256","metadata_union_sha256","physical_closure_sha256"] {
        if proof[key].as_str().is_none_or(|s|s.len()!=64||!s.bytes().all(|b|b.is_ascii_hexdigit())){return Err(format!("missing full evidence binding: {key}"));}
    }
    let fences=make_fences(&state);
    Ok(VerifiedCandidate{state,proof:proof.clone(),proof_sha:digest(&serde_json::to_vec(proof).map_err(|e|e.to_string())?),fences,
        terminal_ids:vec![110,111,112],source_sha:STATE_SHA.into(),prior_backings:json!({})})
}

fn make_fences(state:&Value)->Value{
    fn visit(value:&Value,strings:&mut BTreeSet<String>,inc:&mut BTreeSet<u64>){
        match value {
            Value::Array(values)=>for v in values{visit(v,strings,inc)},
            Value::Object(values)=>for (key,v) in values {
                if matches!(key.as_str(),"realm"|"pool_id"|"pool_epoch"|"provider_session"|"session"|"boot_id"|"agent_session"|"client_session"|"operation_id"|"lease_id"|"socket"|"authority") {
                    if let Some(s)=v.as_str().filter(|s|!s.is_empty()){strings.insert(s.into());}
                }
                if key=="incarnation"{if let Some(n)=v.as_u64(){inc.insert(n);}}
                if key=="request"{if let Some(s)=v.as_str(){if let Ok(request)=serde_json::from_str::<Value>(s){visit(&request,strings,inc);}}}
                visit(v,strings,inc);
            },_=>{}
        }
    }
    let mut strings=BTreeSet::new();let mut inc=BTreeSet::new();visit(state,&mut strings,&mut inc);
    for op in state["operations"].as_array().unwrap(){
        if let Some(text)=op["request"].as_str(){if let Ok(v)=serde_json::from_str::<Value>(text){visit(&v,&mut strings,&mut inc);}}
    }
    for name in ["u.sock","l.sock","a.sock","b.sock","nodeA.qmp","nodeA.serial","nodeB.qmp","nodeB.serial"]{strings.insert(format!("{ROOT}/runs/c044/{name}"));}
    json!({"schema":1,"strings":strings,"incarnations":inc,"retired_object_ids":(100..=112).collect::<Vec<_>>(),
        "minimum_next_object_id":113,"minimum_next_placement_id":10003,"minimum_next_binding_id":40003,
        "minimum_next_access_id":50003,"minimum_next_lease_id":1008})
}

/// Applied before provider bootstrap/RPC and to every nested control identity.
/// Generation alone is not treated as an incarnation or global identity.
pub(crate) fn reject_fenced(reconciliation:&Value,request:&Value)->Result<(),String>{
    let Some(fences)=reconciliation.get("terminal_domain_fences") else{return Ok(())};
    if fences["version"]==2 && ["minimum_next_object_id","minimum_next_placement_id","minimum_next_binding_id","minimum_next_access_id","minimum_next_lease_id","minimum_next_client_id","minimum_next_block_id","minimum_next_transfer_plan_id","minimum_next_transfer_event_id"].iter().any(|k|fences[*k].as_u64().is_none_or(|n|n==0)) {
        return Err("complete per-type v2 identity highwaters required".into());
    }
    if fences["schema"]!=1 || fences["strings"].as_array().is_none_or(|a|a.is_empty()||a.iter().any(|v|v.as_str().is_none_or(|s|s.is_empty()))) ||
        fences["incarnations"].as_array().is_none_or(|a|a.is_empty()||a.iter().any(|v|v.as_u64().is_none())) {
        return Err("incomplete permanent terminal fences; refuse admission".into());
    }
    fn visit(value:&Value,f:&Value)->bool{
        match value {
            Value::String(s)=>f["strings"].as_array().unwrap().iter().any(|v|v==s),
            Value::Array(v)=>v.iter().any(|v|visit(v,f)),
            Value::Object(values)=>values.iter().any(|(key,v)| {
                (key=="incarnation" && f["incarnations"].as_array().unwrap().contains(v)) ||
                (matches!(key.as_str(),"resource_id"|"object_id") && v.as_u64().is_some_and(|id|id<f["minimum_next_object_id"].as_u64().unwrap_or(113))) ||
                (key=="placement_id" && v.as_u64().is_some_and(|id|id<f["minimum_next_placement_id"].as_u64().unwrap_or(10003))) ||
                (matches!(key.as_str(),"binding_id"|"resource_binding_id") && v.as_u64().is_some_and(|id|id<f["minimum_next_binding_id"].as_u64().unwrap_or(40003))) ||
                (matches!(key.as_str(),"access_generation"|"access_id") && v.as_u64().is_some_and(|id|id<f["minimum_next_access_id"].as_u64().unwrap_or(50003))) ||
                (key=="lease_generation" && v.as_u64().is_some_and(|id|id<f["minimum_next_lease_id"].as_u64().unwrap_or(1008))) ||
                (key=="client_id" && v.as_u64().is_some_and(|id|id<f["minimum_next_client_id"].as_u64().unwrap_or(0))) ||
                (key=="block_id" && v.as_u64().is_some_and(|id|id<f["minimum_next_block_id"].as_u64().unwrap_or(0))) ||
                (matches!(key.as_str(),"plan_id"|"transfer_plan_id") && v.as_u64().is_some_and(|id|id<f["minimum_next_transfer_plan_id"].as_u64().unwrap_or(0))) ||
                (matches!(key.as_str(),"event_id"|"transfer_event_id") && v.as_u64().is_some_and(|id|id<f["minimum_next_transfer_event_id"].as_u64().unwrap_or(0))) ||visit(v,f)
            }),_=>false
        }
    }
    if visit(request,fences){Err("terminal-domain identity permanently fenced; historical data lost, no SDK ACK or replay".into())}else{Ok(())}
}

pub(crate) struct TerminalAuthority {
    _original_lock:File,ledger:Option<DdrResources>,reconciliation:Value,path:PathBuf,
    source_sha:String,revision:u64,
}
impl TerminalAuthority {
    fn validate_commit(&self)->Result<(),String>{
        let commit=parsed(&bounded(&self.path.join("commit.json"),1<<20)?)?;
        let result=bounded(&self.path.join("resulting76.json"),1<<20)?;
        let fences=bounded(&self.path.join("permanent-fences.json"),1<<20)?;
        if commit["kind"]!="TerminalDomainAuthorityCommitted" || commit["resulting76_sha256"]!=digest(&result) ||
            commit["fences_sha256"]!=digest(&fences) || commit["original_state_sha256"]!=self.source_sha ||
            digest(&bounded(&self.path.join("historical-state.json"),1<<20)?)!=self.source_sha ||
            parsed(&fences)?!=self.reconciliation["terminal_domain_fences"] {
            return Err("terminal commit/history/fences drift; admission blocked".into());
        }
        let held=self._original_lock.metadata().map_err(|e|e.to_string())?;
        let named=self.path.parent().unwrap().join("authority.lock").symlink_metadata().map_err(|e|e.to_string())?;
        if held.dev()!=named.dev()||held.ino()!=named.ino()||held.nlink()!=1{return Err("original authority no longer pinned".into());}Ok(())
    }
    /// Consume only into a completely empty authority. The returned original
    /// lock guard must live in Service for its whole lifetime, just like the
    /// other restore functions. It remains locked if this wrapper is dropped.
    #[cfg(test)]
    pub(crate) fn attach(&mut self,st:&mut Catalog,new_context:&Value)->Result<File,String>{
        self.validate_commit()?;
        if st.ddr_resources.limit!=128<<20 || st.ddr_resources.charged_bytes()!=0 || st.ddr_resources.reserved_bytes()!=0 ||
            !st.objects.is_empty() || !st.managed_pools.pools.is_empty() || !st.managed_pools.operations.is_empty() || !st.managed_pools.reconciliation.is_null(){return Err("fresh empty successor authority only".into());}
        reject_fenced(&self.reconciliation,new_context)?;
        for k in ["realm","session"]{if new_context[k].as_str().is_none_or(|s|s.is_empty()){return Err("new complete namespace required".into());}}
        let ids=new_context["nodes"].as_array().ok_or("new node identities required")?;
        if ids.len()!=2 || ids.iter().any(|v|["boot_id","agent_session"].iter().any(|k|v[*k].as_str().is_none_or(|s|s.is_empty())) || v["incarnation"].as_u64().is_none_or(|n|n==0)) {return Err("two fresh complete Guest identities required before any new admission".into());}
        if ids.iter().map(|v|v["node"].as_str().unwrap_or("")).collect::<BTreeSet<_>>()!=["nodeA","nodeB"].into_iter().collect() ||
            ids[0]["boot_id"]==ids[1]["boot_id"] || ids[0]["incarnation"]==ids[1]["incarnation"] {return Err("new identities must be independent".into());}
        let lifetime=self._original_lock.try_clone().map_err(|e|e.to_string())?;
        atomic_record(&self.path,"service-admission-claim.json",&json!({"kind":"CPUFullIdentityServiceAdmissionPending","namespace":new_context,
            "source_terminal_commit_sha256":digest(&bounded(&self.path.join("commit.json"),1<<20)?),"native_SDK_ACK":false}))?;
        st.ddr_resources=self.ledger.take().ok_or("terminal authority already consumed")?;
        st.managed_pools.reconciliation=self.reconciliation.clone();st.managed_pools.revision=self.revision;
        self.advance_highwaters(st)?;
        Ok(lifetime)
    }
    /// Launcher ordering: admit only the two metadata reservations before Guest
    /// identities exist. A durable one-time claim prevents replaying the base76
    /// after a service has acquired any responsibility in a new namespace.
    pub(crate) fn attach_staged(&mut self,st:&mut Catalog,namespace:&Value)->Result<File,String>{
        self.validate_commit()?;reject_fenced(&self.reconciliation,namespace)?;
        if st.ddr_resources.limit!=128<<20 || st.ddr_resources.charged_bytes()!=0 || st.ddr_resources.reserved_bytes()!=0 ||
            !st.objects.is_empty()||!st.managed_pools.pools.is_empty()||!st.managed_pools.operations.is_empty()||!st.managed_pools.reconciliation.is_null(){return Err("empty staged successor only".into());}
        for k in ["realm","session"]{if namespace[k].as_str().is_none_or(|s|s.is_empty()){return Err("fresh staged realm/session required".into());}}
        let nodes=namespace["nodes"].as_array().ok_or("two staged nodes")?;
        if nodes.len()!=2 || nodes.iter().map(|v|v["node"].as_str().unwrap_or("")).collect::<BTreeSet<_>>()!=["nodeA","nodeB"].into_iter().collect() ||
            nodes.iter().any(|v|v.as_object().is_none_or(|o|o.len()!=2)||v["socket"].as_str().is_none_or(|s|!Path::new(s).is_absolute())) {
            return Err("staged nodes require only real future node/socket, never invented boot identity".into());
        }
        let first=self.reconciliation["terminal_domain_fences"]["minimum_next_object_id"].as_u64().ok_or("object highwater")?;
        let second=first.checked_add(1).ok_or("canary identity overflow")?;
        let next=second.checked_add(1).ok_or("business identity overflow")?;
        let lifetime=self._original_lock.try_clone().map_err(|e|e.to_string())?;
        let mut ledger=self.ledger.take().ok_or("terminal authority already consumed")?;
        ledger.reserve_unique("canary:nodeA",2<<20)?;ledger.reserve_unique("canary:nodeB",2<<20)?;
        let claim=json!({"kind":"SuccessorServiceAdmissionPending","namespace":namespace,"source_terminal_commit_sha256":digest(&bounded(&self.path.join("commit.json"),1<<20)?),
            "conservative_B_bytes":80<<20,"resource_ledger":ledger.snapshot(),"canary_charge_objects":{"nodeA":first,"nodeB":second},
            "Guest_identity_known":false,"business_admission_enabled":false,"native_SDK_ACK":false});
        atomic_record(&self.path,"service-admission-claim.json",&claim)?;
        let mut reconciliation=self.reconciliation.clone();reconciliation["terminal_bootstrap_phase"]=json!("AwaitingNewGuestReadiness");
        reconciliation["terminal_staged_namespace"]=namespace.clone();reconciliation["terminal_canary_charge_objects"]=claim["canary_charge_objects"].clone();
        st.ddr_resources=ledger;st.managed_pools.reconciliation=reconciliation;st.managed_pools.revision=self.revision;
        self.advance_highwaters(st)?;st.next_object_id=st.next_object_id.max(next);Ok(lifetime)
    }
    fn advance_highwaters(&self,st:&mut Catalog)->Result<(),String>{
        let f=&self.reconciliation["terminal_domain_fences"];
        macro_rules! advance {($field:ident,$key:literal,$fallback:expr)=>{st.$field=st.$field.max(f[$key].as_u64().unwrap_or($fallback));}}
        advance!(next_object_id,"minimum_next_object_id",113);advance!(next_placement_id,"minimum_next_placement_id",10003);
        advance!(next_resource_binding_id,"minimum_next_binding_id",40003);advance!(next_access_binding_id,"minimum_next_access_id",50003);
        advance!(next_lease_id,"minimum_next_lease_id",1008);advance!(next_client_id,"minimum_next_client_id",1_000_000);
        advance!(next_block_id,"minimum_next_block_id",1_000_000);advance!(next_transfer_plan_id,"minimum_next_transfer_plan_id",1);
        advance!(next_transfer_event_id,"minimum_next_transfer_event_id",1);Ok(())
    }
    pub(crate) fn historical(&self)->Result<Value,String>{
        self.validate_commit()?;
        parsed(&bounded(&self.path.join("historical-state.json"),1<<20)?)
    }
    pub(crate) fn closure_report(&self)->Result<Value,String>{
        self.validate_commit()?;
        Ok(json!({"status":"AdministrativelyClosedTerminalDomainDataLost","normal_Destroy_RPC_succeeded":false,
            "normal_Shutdown_RPC_succeeded":false,"native_SDK_ACK":false,"B_at_terminal_commit_bytes":76<<20,
            "scope":"terminal commit before later new-domain admission; query the live Catalog for later responsibility",
            "A_historical_unqueried_bytes":128<<20,"historical_Unknown_data_valid":false,"history_sha256":self.source_sha}))
    }
}

fn original_pins(dir:&Path,committed:bool)->Result<Value,String>{
    let pins:Value=serde_json::from_str(PINS).map_err(|e|e.to_string())?;
    let want:[&str;6]=["authority.lock","intent.json","result.json","successor-c027-v1","successor-c033-v1","successor-c039-prestart-v1"];
    let names=fs::read_dir(dir).map_err(|e|e.to_string())?.map(|v|v.map(|v|v.file_name().to_string_lossy().into_owned())).collect::<Result<BTreeSet<_>,_>>().map_err(|e|e.to_string())?;
    let mut wanted=want.into_iter().map(String::from).collect::<BTreeSet<_>>();if committed{wanted.insert(NEXT.into());}
    if names!=wanted{return Err("original lineage has unexpected successor/partial/extra".into());}
    for (name,row) in pins.as_object().unwrap(){
        let relative=Path::new(name);if let Some(parent)=relative.parent().filter(|p|!p.as_os_str().is_empty()) {
            let metadata=dir.join(parent).symlink_metadata().map_err(|e|e.to_string())?;
            if !metadata.is_dir()||metadata.file_type().is_symlink(){return Err("predecessor directory alias".into());}
            let entries=fs::read_dir(dir.join(parent)).map_err(|e|e.to_string())?.map(|v|v.map(|v|v.file_name().to_string_lossy().into_owned())).collect::<Result<BTreeSet<_>,_>>().map_err(|e|e.to_string())?;
            if entries!=["intent.json","result.json"].into_iter().map(String::from).collect(){return Err("predecessor incomplete/extra".into());}
        }
        let bytes=bounded(&dir.join(name),1<<20)?;
        if bytes.len() as u64!=row["bytes"].as_u64().ok_or("predecessor size")? || digest(&bytes)!=row["sha256"] {return Err("complete predecessor hash mismatch".into());}
    }Ok(pins)
}
fn fault(point:&str,crash:&str)->Result<(),String>{if point==crash{Err(format!("injected HOST crash after {point}; no new admission"))}else{Ok(())}}

fn terminal_reconciliation(p:&VerifiedCandidate,path:&Path)->Value{
    let mut value=p.state["reconciliation"].clone();value["terminal_domain_fences"]=p.fences.clone();
    value["previous_reconciliation_phase"]=value["phase"].clone();value["phase"]=json!("TerminalDomainDataLostC04476");
    value["terminal_proof_sha256"]=json!(p.proof_sha);
    value["terminal_domain_history"]=json!({"path":path.join("historical-state.json"),"sha256":STATE_SHA,"outcome":"TerminalDomainDataLost","normal_SDK_ACK":false});
    value
}

// No public/config-selected root or crash switch: production adapter currently
// cannot call this. Faults and alternate roots are exposed only to cfg(test).
fn transact(dir:&Path,state_path:&Path,verify:impl Fn(&[u8])->Result<VerifiedCandidate,String>,crash:&str)->Result<TerminalAuthority,String>{
    let lock=lock_lineage(dir)?;let identity=lock.metadata().map_err(|e|e.to_string())?;
    let named=dir.join("authority.lock").symlink_metadata().map_err(|e|e.to_string())?;
    if identity.dev()!=named.dev()||identity.ino()!=named.ino()||identity.nlink()!=1{return Err("original authority replaced".into());}
    let pins=original_pins(dir,false)?;let original=bounded(state_path,1<<20)?;let p=verify(&original)?;
    let mut ledger=DdrResources::inherit_terminal_source(&p)?;
    let path=dir.join(NEXT);fs::DirBuilder::new().mode(0o700).create(&path).map_err(|e|e.to_string())?;
    File::open(dir).and_then(|f|f.sync_all()).map_err(|e|e.to_string())?;
    let intent=json!({"schema":1,"kind":"TerminalDomainLossPending","source_state_sha256":STATE_SHA,"source_revision":153,
        "source_file":state_path,"proof_sha256":p.proof_sha,"verified_candidate":p.proof,"predecessor_files":pins,
        "resource_ids":[110,111,112],"terminal_bytes":52<<20,"before_B_bytes":128<<20,"after_B_bytes":76<<20,
        "native_SDK_ACK":false,"original_Unknown_history_preserved":true});
    atomic_record(&path,"intent.json",&intent)?;fault("intent",crash)?;
    // Preserve exact bytes, not just an equivalent reserialization.
    let mut history=OpenOptions::new().write(true).create_new(true).mode(0o600).custom_flags(libc::O_NOFOLLOW).open(path.join("historical-state.json")).map_err(|e|e.to_string())?;
    use std::io::Write;history.write_all(&original).and_then(|_|history.sync_all()).map_err(|e|e.to_string())?;File::open(&path).and_then(|f|f.sync_all()).map_err(|e|e.to_string())?;fault("historical128",crash)?;
    atomic_record(&path,"before128.json",&ledger.snapshot())?;fault("before128",crash)?;
    atomic_record(&path,"permanent-fences.json",&p.fences)?;fault("fences",crash)?;
    ledger.apply_terminal_loss(&p)?;fault("terminal52",crash)?;
    let reconciliation=terminal_reconciliation(&p,&path);
    let state=json!({"schema":1,"kind":"TerminalDomainDataLost","revision":154,"resource_ledger":ledger.snapshot(),
        "charged_bytes":12<<20,"retained_bytes":12<<20,"creation_reserved_bytes":64<<20,"active_bytes":0,"budget_bytes":128<<20,
        "B_conservative_bytes":76<<20,"A_historical_unqueried_bytes":128<<20,"global_conservative_bytes":204<<20,
        "reconciliation":reconciliation,"normal_destroy_or_shutdown_succeeded":false,"normal_SDK_ACK":false,
        "historical_clients_leases_operations_unchanged":true,"historical_data_valid":false,"new_admission_enabled":false});
    atomic_record(&path,"resulting76.json",&state)?;fault("resulting76",crash)?;
    // Recheck unchanged source and authority before the one durable commit.
    if bounded(state_path,1<<20)?!=original || dir.join("authority.lock").metadata().map_err(|e|e.to_string())?.ino()!=identity.ino(){return Err("source or original authority drift before commit".into());}
    if verify(&original)?.proof_sha!=p.proof_sha{return Err("source/runtime/backing/metadata/physical proof changed before commit".into());}
    let commitment=json!({"kind":"TerminalDomainAuthorityCommitted","proof_consumed_once":p.proof_sha,
        "original_state_sha256":STATE_SHA,"resulting76_sha256":digest(&bounded(&path.join("resulting76.json"),1<<20)?),
        "fences_sha256":digest(&bounded(&path.join("permanent-fences.json"),1<<20)?),"new_authority":path,
        "old_state_untouched":true,"normal_SDK_ACK":false});
    atomic_record(&path,"commit.json",&commitment)?;fault("commit",crash)?;
    Ok(TerminalAuthority{_original_lock:lock,ledger:Some(ledger),reconciliation,path,source_sha:STATE_SHA.into(),revision:154})
}

/// Reopen the complete terminal-only commit after its writer process exits.
/// This rebuilds the already committed accounting decision; it never writes a
/// second terminal result or discounts the old52 twice. Any subsequent service
/// claim, partial output or drift requires separate reconciliation instead.
fn recover_committed(dir:&Path,state_path:&Path,verify:impl Fn(&[u8])->Result<VerifiedCandidate,String>)->Result<TerminalAuthority,String>{
    let lock=lock_lineage(dir)?;let pins=original_pins(dir,true)?;let path=dir.join(NEXT);
    let names=fs::read_dir(&path).map_err(|e|e.to_string())?.map(|v|v.map(|v|v.file_name().to_string_lossy().into_owned())).collect::<Result<BTreeSet<_>,_>>().map_err(|e|e.to_string())?;
    if names!=["intent.json","historical-state.json","before128.json","permanent-fences.json","resulting76.json","commit.json"].into_iter().map(String::from).collect(){return Err("only complete unclaimed terminal commit may resume; partial or service claim blocks".into());}
    let original=bounded(state_path,1<<20)?;let p=verify(&original)?;
    if original!=bounded(&path.join("historical-state.json"),1<<20)?{return Err("historical state changed".into());}
    let intent=parsed(&bounded(&path.join("intent.json"),1<<20)?)?;
    if intent["proof_sha256"]!=p.proof_sha || intent["verified_candidate"]!=p.proof || intent["predecessor_files"]!=pins || intent["source_state_sha256"]!=STATE_SHA ||
        intent["kind"]!="TerminalDomainLossPending" || intent["source_revision"]!=153 || intent["resource_ids"]!=json!([110,111,112]) ||
        intent["terminal_bytes"]!=52<<20 || intent["before_B_bytes"]!=128<<20 || intent["after_B_bytes"]!=76<<20 || intent["native_SDK_ACK"]!=false || intent["original_Unknown_history_preserved"]!=true {
        return Err("committed source/proof/predecessor mismatch".into());
    }
    let before=parsed(&bounded(&path.join("before128.json"),1<<20)?)?;p.matches_ledger(&before)?;
    let mut ledger=DdrResources::inherit_terminal_source(&p)?;ledger.apply_terminal_loss(&p)?;
    let durable=parsed(&bounded(&path.join("resulting76.json"),1<<20)?)?;let expected=ledger.snapshot();
    if rows(&durable["resource_ledger"])?!=rows(&expected)? || ["charged_bytes","reserved_bytes","retained_uncertain_reservations","reservations","retired_unstarted_object_ids","terminal_domain_loss_tombstones"].iter().any(|k|durable["resource_ledger"][*k]!=expected[*k]) ||
        durable["kind"]!="TerminalDomainDataLost" || durable["B_conservative_bytes"]!=76<<20 || durable["normal_SDK_ACK"]!=false ||
        durable["reconciliation"]!=terminal_reconciliation(&p,&path) || durable["new_admission_enabled"]!=false ||
        durable["historical_clients_leases_operations_unchanged"]!=true || durable["historical_data_valid"]!=false || durable["revision"]!=154 ||
        durable["normal_destroy_or_shutdown_succeeded"]!=false || durable["charged_bytes"]!=12<<20 || durable["creation_reserved_bytes"]!=64<<20 || durable["budget_bytes"]!=128<<20 {
        return Err("committed terminal ledger differs from original protected76 and typed tombstones".into());
    }
    let authority=TerminalAuthority{_original_lock:lock,ledger:Some(ledger),reconciliation:durable["reconciliation"].clone(),path,source_sha:STATE_SHA.into(),revision:154};
    authority.validate_commit()?;
    if authority.reconciliation["terminal_domain_fences"]!=p.fences{return Err("committed permanent fences differ".into());}
    Ok(authority)
}

#[cfg(test)]
pub(crate) fn staged_fixture_attach(a:&mut TerminalAuthority,st:&mut Catalog,loaded:&Value)->Result<File,String>{
    let nodes=loaded["nodes"].as_object().ok_or("deferred fixture nodes")?.values().cloned().collect::<Vec<_>>();
    a.attach_staged(st,&json!({"realm":loaded["realm"],"session":loaded["session"],"nodes":nodes}))
}

/// No RPC is sent to missing/stale endpoints. Live endpoints still require the
/// original normal SDK path and every existing client/worker drain barrier.
pub(crate) fn shutdown_report(state:&Value,kernel_endpoints:&BTreeSet<String>)->Value{
    shutdown_report_at(state,&Path::new(ROOT).join("runs/c044"),kernel_endpoints)
}
fn shutdown_report_at(state:&Value,root:&Path,kernel_endpoints:&BTreeSet<String>)->Value{
    let active=["u.sock","l.sock","a.sock","b.sock","nodeA.qmp","nodeA.serial","nodeB.qmp","nodeB.serial"].iter()
        .filter(|n|kernel_endpoints.contains(&root.join(n).to_string_lossy().into_owned())).copied().collect::<Vec<_>>();
    json!({"status":if active.is_empty(){"AlreadyStoppedNoLiveEndpointResponsibilityRetained"}else{"LiveEndpointRequiresOwnedNormalDrain"},
        "normal_Destroy_RPC_succeeded":false,"normal_Shutdown_RPC_succeeded":false,"native_SDK_ACK":false,
        "active_endpoints":active,"original_Unknown_history":state["pools"],"charged_bytes":state["charged_bytes"],
        "reserved_bytes":state["creation_reserved_bytes"],"production_terminal_ready":false})
}

pub(crate) fn production_shutdown_report()->Result<Value,String>{
    let path=Path::new(ROOT).join("runs/c044/state/state.json");let bytes=bounded(&path,1<<20)?;
    if digest(&bytes)!=STATE_SHA{return Err("shutdown report source drift; no RPC performed".into());}
    Ok(shutdown_report(&parsed(&bytes)?,&kernel_endpoints()?))
}
fn kernel_endpoints()->Result<BTreeSet<String>,String>{
    let mut unix=Vec::new();File::open("/proc/net/unix").map_err(|e|e.to_string())?.take((16<<20)+1).read_to_end(&mut unix).map_err(|e|e.to_string())?;
    if unix.len()>16<<20{return Err("kernel endpoint inventory bound exceeded".into());}
    let text=std::str::from_utf8(&unix).map_err(|e|e.to_string())?;
    let endpoints=text.lines().skip(1).filter_map(|line|{let fields=line.split_whitespace().collect::<Vec<_>>();(fields.len()==8).then(||fields[7].to_string())}).collect();
    Ok(endpoints)
}

#[cfg(test)]
mod tests;

pub(crate) mod current;
pub(crate) mod case_chain;
