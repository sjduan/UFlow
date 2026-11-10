//! Current c064 HOST consumer seam. No accepted E v2 trust anchor exists yet.
//! Neither a descriptor nor a test fixture is a production loss permit.
use super::*;

const CURRENT_SHA:&str="163f7500836dc4fd199d34f83a0e08453ab05255f8b15a57338052df4601d5b0";
const SUCCESSOR:&str="successor-c064-terminal-domain-loss-v2";
const OLD_COMMIT:&str="31c4ab9126df335b4d25492fc6574c0c997237cab603ba8003fe8abf959c0ab4";
const OLD_CLAIM:&str="e571f2e683ad08fb9720928d5c1dd795318c66a434190dc6e7956148de5a6fcd";

/// Deliberately before descriptor/state/lock/verifier IO. Acceptance of a real
/// E v2 source interface and execution package is a separate reviewed change.
pub(crate) fn production_manifest(_path:&Path,_external_sha:&str)->Result<Value,String>{
    Err("production-not-ready: genuine E current-c064 v2 verifier/interface/pins and execution binding are not accepted; no descriptor/state/authority/verifier IO performed".into())
}

struct Case {descriptor:Value,descriptor_sha:String}
impl Case {
    fn parse(raw:&[u8],external_sha:&str)->Result<Self,String>{
        if digest(raw)!=external_sha{return Err("case external SHA mismatch".into());}
        let d=parsed(raw)?;
        if d["schema"]!=2 || d["current_state"]["sha256"]!=CURRENT_SHA || d["current_state"]["bytes"]!=60761 ||
            d["source_revision"]!=256 || d["resource_ids"]!=json!([113,114,115]) || d["eligible_bytes"]!=52<<20 ||
            d["before_B_bytes"]!=128<<20 || d["after_B_bytes"]!=76<<20 || d["protected_retained_bytes"]!=12<<20 ||
            d["protected_reserved_bytes"]!=64<<20 || d["previous_directory"]!=NEXT || d["new_successor"]!=SUCCESSOR ||
            d["previous_files"]["commit.json"]["sha256"]!=OLD_COMMIT || d["previous_files"]["service-admission-claim.json"]["sha256"]!=OLD_CLAIM {
            return Err("exact current-c064 case/protected76/previous claimed authority required".into());
        }
        Ok(Self{descriptor:d,descriptor_sha:external_sha.into()})
    }
    fn lineage(&self,dir:&Path,has_new:bool)->Result<Value,String>{
        let names=fs::read_dir(dir).map_err(|e|e.to_string())?.map(|e|e.map(|e|e.file_name().to_string_lossy().into_owned())).collect::<Result<BTreeSet<_>,_>>().map_err(|e|e.to_string())?;
        let mut expected=["authority.lock","intent.json","result.json","successor-c027-v1","successor-c033-v1","successor-c039-prestart-v1",NEXT].into_iter().map(String::from).collect::<BTreeSet<_>>();
        if has_new{expected.insert(SUCCESSOR.into());}if names!=expected{return Err("exact original lineage; partial/extra successor blocks".into());}
        let pins:Value=serde_json::from_str(PINS).map_err(|e|e.to_string())?;
        for (name,row) in pins.as_object().ok_or("predecessor pins")? {
            let p=dir.join(name);let raw=bounded(&p,1<<20)?;
            if digest(&raw)!=row["sha256"] || raw.len() as u64!=row["bytes"].as_u64().ok_or("predecessor bytes")? {return Err("predecessor bytes drift".into());}
            if let Some(parent)=Path::new(name).parent().filter(|p|!p.as_os_str().is_empty()) {
                exact_names(&dir.join(parent),&["intent.json","result.json"])?;
            }
        }
        let old=dir.join(NEXT);
        exact_names(&old,&["intent.json","historical-state.json","before128.json","permanent-fences.json","resulting76.json","commit.json","service-admission-claim.json"])?;
        let previous=self.descriptor["previous_files"].as_object().ok_or("previous file pins")?;
        if previous.len()!=7{return Err("complete seven previous metadata files required".into());}
        for (name,row) in previous {
            if !["intent.json","historical-state.json","before128.json","permanent-fences.json","resulting76.json","commit.json","service-admission-claim.json"].contains(&name.as_str()){return Err("unexpected prior metadata name".into());}
            let raw=bounded(&old.join(name),1<<20)?;
            if digest(&raw)!=row["sha256"] || raw.len() as u64!=row["bytes"].as_u64().ok_or("prior size")? {return Err("prior claimed terminal chain drift".into());}
        }
        let commit=parsed(&bounded(&old.join("commit.json"),1<<20)?)?;
        let history=bounded(&old.join("historical-state.json"),1<<20)?;
        let intent=parsed(&bounded(&old.join("intent.json"),1<<20)?)?;
        let p=from_e_proof(&history,intent["verified_candidate"].clone())?;
        if commit["original_state_sha256"]!=STATE_SHA || commit["proof_consumed_once"]!=p.proof_sha || intent["proof_sha256"]!=p.proof_sha ||
            commit["resulting76_sha256"]!=digest(&bounded(&old.join("resulting76.json"),1<<20)?) ||
            commit["fences_sha256"]!=digest(&bounded(&old.join("permanent-fences.json"),1<<20)?) {
            return Err("prior committed proof/history/result/fences mismatch".into());
        }
        let claim=parsed(&bounded(&old.join("service-admission-claim.json"),1<<20)?)?;
        if claim["source_terminal_commit_sha256"]!=OLD_COMMIT || claim["kind"]!="SuccessorServiceAdmissionPending" || claim["conservative_B_bytes"]!=80<<20 || claim["native_SDK_ACK"]!=false {
            return Err("original authority must retain its exact consumed service claim".into());
        }
        Ok(json!({"predecessors":pins,"previous_files":self.descriptor["previous_files"]}))
    }
}

fn exact_names(path:&Path,want:&[&str])->Result<(),String>{
    let info=path.symlink_metadata().map_err(|e|e.to_string())?;
    if !info.is_dir()||info.file_type().is_symlink(){return Err("ordinary metadata directory required".into());}
    let names=fs::read_dir(path).map_err(|e|e.to_string())?.map(|e|e.map(|e|e.file_name().to_string_lossy().into_owned())).collect::<Result<BTreeSet<_>,_>>().map_err(|e|e.to_string())?;
    if names!=want.iter().map(|s|s.to_string()).collect(){return Err("incomplete/claimed/extra successor metadata".into());}Ok(())
}

// Source-bound internal proof contract. A future accepted E adapter must
// independently establish all of it under the original lock; no caller may
// construct a production permit. The only constructor today is cfg(test).
fn current_candidate(bytes:&[u8],dir:&Path,proof:Value)->Result<VerifiedCandidate,String>{
    if digest(bytes)!=CURRENT_SHA{return Err("exact full current snapshot required".into());}
    let state=parsed(bytes)?;let old=dir.join(NEXT);
    let history=parsed(&bounded(&old.join("historical-state.json"),1<<20)?)?;
    let prior=parsed(&bounded(&old.join("resulting76.json"),1<<20)?)?;
    let fences=parsed(&bounded(&old.join("permanent-fences.json"),1<<20)?)?;
    let source=&state["resource_ledger"];let live=rows(source)?;
    if state["revision"]!=256 || state["charged_bytes"]!=64<<20 || state["creation_reserved_bytes"]!=64<<20 || state["active_bytes"]!=52<<20 ||
        state["reconciliation"]["terminal_domain_fences"]!=fences || source["terminal_domain_loss_tombstones"]!=prior["resource_ledger"]["terminal_domain_loss_tombstones"] ||
        source["retained_uncertain_reservations"]!=prior["resource_ledger"]["retained_uncertain_reservations"] || source["reservations"]!=prior["resource_ledger"]["reservations"] ||
        source["retired_unstarted_object_ids"]!=prior["resource_ledger"]["retired_unstarted_object_ids"] {return Err("current protected responsibility or prior history drift".into());}
    let protected=rows(&prior["resource_ledger"])?;
    for (id,row) in &protected{if live.get(id)!=Some(row){return Err("protected backing or retained witness drift".into());}}
    if live.len()!=protected.len()+3 || [113,114,115].iter().zip([2<<20,2<<20,48<<20]).any(|(id,n)|live.get(id).is_none_or(|row|row["nbytes"]!=n||!row["retained"].is_null())) {
        return Err("only current113/114/115 exact52 eligible".into());
    }
    let pool=&state["pools"][0];
    if state["pools"].as_array().map(Vec::len)!=Some(1)|| pool["resource_id"]!=115 || pool["frozen"]!=true || !pool["release_result"].is_null() || pool["clients"].as_array().map(Vec::len)!=Some(3) {
        return Err("exact frozen current pool/client history required".into());
    }
    let historical=rows(&history["resource_ledger"])?;let mut backings=serde_json::Map::new();
    for id in [110,111,112] {
        let tomb=&source["terminal_domain_loss_tombstones"][id.to_string()];let row=historical.get(&id).ok_or("prior backing missing")?;
        if tomb["object_id"]!=id || tomb["nbytes"]!=row["nbytes"] || tomb["witness"]["source_state_sha256"]!=STATE_SHA {return Err("prior tombstone/backing history mismatch".into());}
        backings.insert(id.to_string(),row.clone());
    }
    let union=union_fences(&state,&fences,&json!(backings))?;
    Ok(VerifiedCandidate{state,proof_sha:digest(&serde_json::to_vec(&proof).map_err(|e|e.to_string())?),proof,fences:union,
        terminal_ids:vec![113,114,115],source_sha:CURRENT_SHA.into(),prior_backings:json!(backings)})
}

fn union_fences(state:&Value,previous:&Value,backings:&Value)->Result<Value,String>{
    let mut strings=BTreeSet::<String>::new();let mut incarnations=BTreeSet::<u64>::new();let mut ids=BTreeSet::<u64>::new();
    for v in previous["strings"].as_array().ok_or("previous string fences")?{strings.insert(v.as_str().filter(|s|!s.is_empty()).ok_or("previous identity")?.into());}
    for v in previous["incarnations"].as_array().ok_or("previous incarnation fences")?{incarnations.insert(v.as_u64().ok_or("previous incarnation")?);}
    for v in previous["retired_object_ids"].as_array().ok_or("previous retired ids")?{ids.insert(v.as_u64().ok_or("previous retired id")?);}
    let fields=[("minimum_next_object_id",113),("minimum_next_placement_id",10003),("minimum_next_binding_id",40003),
        ("minimum_next_access_id",50003),("minimum_next_lease_id",1008),("minimum_next_client_id",1_000_000),
        ("minimum_next_block_id",1_000_000),("minimum_next_transfer_plan_id",1),("minimum_next_transfer_event_id",1)];
    let mut high=fields.iter().map(|(k,n)|(k.to_string(),previous[*k].as_u64().unwrap_or(*n).max(*n))).collect::<BTreeMap<_,_>>();
    fn visit(v:&Value,strings:&mut BTreeSet<String>,inc:&mut BTreeSet<u64>,ids:&mut BTreeSet<u64>,high:&mut BTreeMap<String,u64>)->Result<(),String>{
        match v {
            Value::Array(a)=>for v in a{visit(v,strings,inc,ids,high)?;},
            Value::Object(o)=>for (k,v) in o{
                if matches!(k.as_str(),"realm"|"pool_id"|"pool_epoch"|"provider_session"|"session"|"boot_id"|"agent_session"|"client_session"|"operation_id"|"lease_id"|"socket"|"authority"|"resource"|"key") {
                    if let Some(s)=v.as_str().filter(|s|!s.is_empty()){strings.insert(s.into());}
                }
                if k=="incarnation"{if let Some(n)=v.as_u64(){inc.insert(n);}}
                let field=match k.as_str(){"resource_id"|"object_id"=>Some("minimum_next_object_id"),"placement_id"=>Some("minimum_next_placement_id"),
                    "binding_id"|"resource_binding_id"=>Some("minimum_next_binding_id"),"access_generation"|"access_id"=>Some("minimum_next_access_id"),
                    "lease_generation"|"lease_id"=>Some("minimum_next_lease_id"),"client_id"=>Some("minimum_next_client_id"),"block_id"=>Some("minimum_next_block_id"),
                    "plan_id"|"transfer_plan_id"=>Some("minimum_next_transfer_plan_id"),"event_id"|"transfer_event_id"=>Some("minimum_next_transfer_event_id"),_=>None};
                if let (Some(field),Some(n))=(field,v.as_u64()){
                    let next=n.checked_add(1).ok_or("identity highwater overflow")?;let entry=high.get_mut(field).ok_or("identity type")?;*entry=(*entry).max(next);
                    if field=="minimum_next_object_id"{ids.insert(n);}
                }
                if high.contains_key(k){let n=v.as_u64().ok_or("serialized highwater type")?;let old=high.get_mut(k).unwrap();*old=(*old).max(n);}
                if k=="request"{if let Some(s)=v.as_str(){if let Ok(request)=serde_json::from_str::<Value>(s){visit(&request,strings,inc,ids,high)?;}}}
                visit(v,strings,inc,ids,high)?;
            },_=>{}
        }Ok(())
    }
    visit(state,&mut strings,&mut incarnations,&mut ids,&mut high)?;
    visit(backings,&mut strings,&mut incarnations,&mut ids,&mut high)?;
    for n in ["u.sock","l.sock","a.sock","b.sock","nodeA.qmp","nodeA.serial","nodeB.qmp","nodeB.serial"]{strings.insert(format!("{ROOT}/runs/c064/{n}"));}
    let mut result=json!({"schema":1,"version":2,"strings":strings,"incarnations":incarnations,"retired_object_ids":ids});
    for (k,n) in high{result[k]=json!(n);}Ok(result)
}

fn reconciliation(p:&VerifiedCandidate,path:&Path)->Value{
    let mut value=p.state["reconciliation"].clone();
    let old=value["terminal_domain_history"].clone();value["previous_terminal_domain_history"]=old;
    value["previous_reconciliation_phase"]=value["phase"].clone();value["phase"]=json!("TerminalDomainDataLostC064V2Protected76");
    value["terminal_domain_fences"]=p.fences.clone();value["terminal_proof_sha256"]=json!(p.proof_sha);
    value["terminal_domain_history"]=json!({"path":path.join("historical-state.json"),"sha256":CURRENT_SHA,"outcome":"TerminalDomainDataLost","normal_SDK_ACK":false});
    value
}

fn transact_current(dir:&Path,state_path:&Path,case:&Case,verify:impl Fn(&[u8],&Path)->Result<VerifiedCandidate,String>,crash:&str)->Result<TerminalAuthority,String>{
    let lock=lock_lineage(dir)?;let lineage=case.lineage(dir,false)?;let original=bounded(state_path,1<<20)?;
    check_lock(&lock,dir)?;
    let p=verify(&original,dir)?;let mut ledger=DdrResources::inherit_terminal_source(&p)?;
    let path=dir.join(SUCCESSOR);fs::DirBuilder::new().mode(0o700).create(&path).map_err(|e|e.to_string())?;
    File::open(dir).and_then(|f|f.sync_all()).map_err(|e|e.to_string())?;fault("directory",crash)?;
    let intent=json!({"kind":"CurrentTerminalLossPendingV2","case_sha256":case.descriptor_sha,"case":case.descriptor,
        "source_state_sha256":CURRENT_SHA,"source_revision":256,"proof_sha256":p.proof_sha,"verified_candidate":p.proof,
        "lineage":lineage,"resource_ids":[113,114,115],"normal_SDK_ACK":false});
    atomic_record(&path,"intent.json",&intent)?;fault("intent",crash)?;
    use std::io::Write;
    let mut history=OpenOptions::new().write(true).create_new(true).mode(0o600).custom_flags(libc::O_NOFOLLOW).open(path.join("historical-state.json")).map_err(|e|e.to_string())?;
    history.write_all(&original).and_then(|_|history.sync_all()).map_err(|e|e.to_string())?;File::open(&path).and_then(|f|f.sync_all()).map_err(|e|e.to_string())?;fault("historical128",crash)?;
    atomic_record(&path,"before128.json",&ledger.snapshot())?;fault("before128",crash)?;
    atomic_record(&path,"permanent-fences.json",&p.fences)?;fault("fences",crash)?;
    atomic_record(&path,"inherited-backing-identities.json",&p.prior_backings)?;fault("backings",crash)?;
    ledger.apply_terminal_loss(&p)?;fault("terminal52",crash)?;
    let result=json!({"kind":"CurrentTerminalDomainDataLostV2","revision":257,"resource_ledger":ledger.snapshot(),"reconciliation":reconciliation(&p,&path),
        "charged_bytes":12<<20,"creation_reserved_bytes":64<<20,"B_conservative_bytes":76<<20,"normal_SDK_ACK":false,"new_admission_enabled":false});
    atomic_record(&path,"resulting76.json",&result)?;fault("resulting76",crash)?;
    if bounded(state_path,1<<20)?!=original || case.lineage(dir,true)?!=lineage || verify(&original,dir)?.proof_sha!=p.proof_sha{return Err("source/predecessor/proof drift before final commit".into());}
    let held=lock.metadata().map_err(|e|e.to_string())?;let named=dir.join("authority.lock").symlink_metadata().map_err(|e|e.to_string())?;
    if held.dev()!=named.dev()||held.ino()!=named.ino()||held.nlink()!=1{return Err("original authority identity changed".into());}
    let hashes=["intent.json","historical-state.json","before128.json","permanent-fences.json","inherited-backing-identities.json","resulting76.json"].into_iter().map(|n|Ok((n.to_string(),digest(&bounded(&path.join(n),1<<20)?)))).collect::<Result<BTreeMap<_,_>,String>>()?;
    let commitment=json!({"kind":"TerminalDomainAuthorityCommitted","version":2,"original_state_sha256":CURRENT_SHA,"case_sha256":case.descriptor_sha,
        "proof_consumed_once":p.proof_sha,"files":hashes,"resulting76_sha256":hashes["resulting76.json"],"fences_sha256":hashes["permanent-fences.json"],"normal_SDK_ACK":false});
    atomic_record(&path,"commit.json",&commitment)?;fault("commit",crash)?;
    Ok(TerminalAuthority{_original_lock:lock,ledger:Some(ledger),reconciliation:result["reconciliation"].clone(),path,source_sha:CURRENT_SHA.into(),revision:257})
}

fn recover_current(dir:&Path,state_path:&Path,case:&Case,verify:impl Fn(&[u8],&Path)->Result<VerifiedCandidate,String>)->Result<TerminalAuthority,String>{
    let lock=lock_lineage(dir)?;let lineage=case.lineage(dir,true)?;let path=dir.join(SUCCESSOR);
    check_lock(&lock,dir)?;
    exact_names(&path,&["intent.json","historical-state.json","before128.json","permanent-fences.json","inherited-backing-identities.json","resulting76.json","commit.json"])?;
    let original=bounded(state_path,1<<20)?;let p=verify(&original,dir)?;
    let commit=parsed(&bounded(&path.join("commit.json"),1<<20)?)?;
    for name in ["intent.json","historical-state.json","before128.json","permanent-fences.json","inherited-backing-identities.json","resulting76.json"]{
        if commit["files"][name]!=digest(&bounded(&path.join(name),1<<20)?){return Err("committed metadata drift".into());}
    }
    let intent=parsed(&bounded(&path.join("intent.json"),1<<20)?)?;
    if bounded(&path.join("historical-state.json"),1<<20)?!=original || intent["case"]!=case.descriptor || intent["case_sha256"]!=case.descriptor_sha ||
        intent["lineage"]!=lineage || intent["proof_sha256"]!=p.proof_sha || intent["verified_candidate"]!=p.proof ||
        commit["proof_consumed_once"]!=p.proof_sha || commit["case_sha256"]!=case.descriptor_sha || commit["version"]!=2 ||
        intent["kind"]!="CurrentTerminalLossPendingV2" || intent["source_state_sha256"]!=CURRENT_SHA || intent["source_revision"]!=256 || intent["resource_ids"]!=json!([113,114,115]) || intent["normal_SDK_ACK"]!=false ||
        commit["kind"]!="TerminalDomainAuthorityCommitted" || commit["original_state_sha256"]!=CURRENT_SHA || commit["normal_SDK_ACK"]!=false {return Err("committed current case/lineage/proof mismatch".into());}
    let before=parsed(&bounded(&path.join("before128.json"),1<<20)?)?;p.matches_ledger(&before)?;
    let mut ledger=DdrResources::inherit_terminal_source(&p)?;ledger.apply_terminal_loss(&p)?;
    let result=parsed(&bounded(&path.join("resulting76.json"),1<<20)?)?;
    let expected=ledger.snapshot();let actual=&result["resource_ledger"];
    if rows(actual)?!=rows(&expected)? || ["charged_bytes","reserved_bytes","retained_bytes","retained_reserved_bytes","active_bytes","active_reserved_bytes","reservations","retained_uncertain_reservations","retired_unstarted_object_ids","terminal_domain_loss_tombstones"].iter().any(|k|actual[*k]!=expected[*k]) ||
        result["reconciliation"]!=reconciliation(&p,&path) || result["B_conservative_bytes"]!=76<<20 || result["normal_SDK_ACK"]!=false || result["new_admission_enabled"]!=false || result["revision"]!=257 ||
        result["kind"]!="CurrentTerminalDomainDataLostV2" || result["charged_bytes"]!=12<<20 || result["creation_reserved_bytes"]!=64<<20 ||
        parsed(&bounded(&path.join("inherited-backing-identities.json"),1<<20)?)?!=p.prior_backings {
        return Err("recovered responsibility/backing/fences differs from verified current state".into());
    }
    let a=TerminalAuthority{_original_lock:lock,ledger:Some(ledger),reconciliation:result["reconciliation"].clone(),path,source_sha:CURRENT_SHA.into(),revision:257};a.validate_commit()?;Ok(a)
}

fn check_lock(lock:&File,dir:&Path)->Result<(),String>{
    let held=lock.metadata().map_err(|e|e.to_string())?;let named=dir.join("authority.lock").symlink_metadata().map_err(|e|e.to_string())?;
    if !held.is_file()||held.nlink()!=1||named.file_type().is_symlink()||held.dev()!=named.dev()||held.ino()!=named.ino(){return Err("exact original single-link authority required".into());}Ok(())
}

#[cfg(test)]
mod tests;
