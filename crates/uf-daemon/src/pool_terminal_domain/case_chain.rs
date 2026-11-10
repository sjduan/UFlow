//! Versioned current-case core. Production has no accepted producer context.
//! Descriptor pins bind data; they never select Python or mint a live permit.
#![allow(dead_code)] // Production stays unbound; the same core is HOST-tested.
use super::*;
const FILES:&[&str]=&["intent.json","historical-state.json","before128.json","permanent-fences.json","inherited-backing-identities.json","resulting76.json","commit.json"];

pub(crate) fn production_manifest(_path:&Path,_external_sha:&str)->Result<Value,String>{
    Err("production-not-ready: accepted current-case E witness/profile/metadata context and exact execution package missing; no descriptor/state/authority/verifier IO".into())
}

struct Case {descriptor:Value,descriptor_sha:String}
struct Lineage {pins:Value,previous:Value,previous_fences:Value,backings:Value}
// DdrResources stores resources/reservations in HashMaps. Historical array
// order is not identity; normalize only these keyed sets, rejecting duplicates
// and comparing every original row/witness and every other field exactly.
fn normalized_ledger(value:&Value)->Result<Value,String>{
    let mut result=value.clone();result["resources"]=json!(rows(value)?);
    let mut reservations=BTreeMap::new();
    for row in value["retained_uncertain_reservations"].as_array().ok_or("typed uncertain reservation set")?{
        let key=row["key"].as_str().ok_or("reservation key")?;
        if reservations.insert(key.to_string(),row.clone()).is_some(){return Err("duplicate retained reservation".into());}
    }
    result["retained_uncertain_reservations"]=json!(reservations);Ok(result)
}
fn ledger_equal(a:&Value,b:&Value)->Result<bool,String>{Ok(normalized_ledger(a)?==normalized_ledger(b)?)}
impl Case {
    fn parse(raw:&[u8],pin:&str)->Result<Self,String>{
        if digest(raw)!=pin{return Err("case external digest mismatch".into());}
        let d=parsed(raw)?;let run=d["source_run"].as_str().ok_or("source run")?;let version=d["transaction_version"].as_u64().ok_or("case version")?;
        let ancestors=d["claimed_ancestors"].as_array().ok_or("claimed ancestors")?;
        let ids=d["resource_ids"].as_array().ok_or("eligible ids")?.iter().map(|v|v.as_u64().ok_or("typed resource id")).collect::<Result<Vec<_>,_>>()?;
        if d["schema"]!=3 || d["interface"]!="current-terminal-chain-case-v1" || ancestors.is_empty() || ancestors.len()>8 || version!=ancestors.len() as u64+1 ||
            run.len()!=4 || !run.starts_with('c') || !run[1..].bytes().all(|c|c.is_ascii_digit()) ||
            d["current_state"]["path"]!=format!("{ROOT}/runs/{run}/state/state.json") || d["current_state"]["bytes"].as_u64().is_none_or(|n|n==0||n>1<<20) ||
            d["current_state"]["sha256"].as_str().is_none_or(|s|s.len()!=64||!s.bytes().all(|c|c.is_ascii_hexdigit())) ||
            d["source_revision"].as_u64().is_none() || ids.len()!=3 || ids!=[110+ancestors.len() as u64*3,111+ancestors.len() as u64*3,112+ancestors.len() as u64*3] ||
            d["eligible_bytes"]!=52<<20 || d["before_B_bytes"]!=128<<20 || d["after_B_bytes"]!=76<<20 || d["protected_retained_bytes"]!=12<<20 || d["protected_reserved_bytes"]!=64<<20 ||
            d["new_successor"]!=format!("successor-{run}-terminal-domain-loss-v{version}") || d["normal_SDK_ACK"]!=false ||
            d["future_canary_ids"]!=json!([ids[2]+1,ids[2]+2]) || d["future_business_id"]!=ids[2]+3 || d["fresh_native_generation"]!=1 {
            return Err("complete typed versioned current case/protected76/ancestors required".into());
        }
        Ok(Self{descriptor:d,descriptor_sha:pin.into()})
    }
    fn sha(&self)->&str{self.descriptor["current_state"]["sha256"].as_str().unwrap()}
    fn version(&self)->u64{self.descriptor["transaction_version"].as_u64().unwrap()}
    fn revision(&self)->u64{self.descriptor["source_revision"].as_u64().unwrap()}
    fn successor(&self)->&str{self.descriptor["new_successor"].as_str().unwrap()}
    fn ids(&self)->Vec<u64>{self.descriptor["resource_ids"].as_array().unwrap().iter().map(|v|v.as_u64().unwrap()).collect()}
    fn lineage(&self,dir:&Path,has_new:bool)->Result<Lineage,String>{
        let pins:Value=serde_json::from_str(PINS).map_err(|e|e.to_string())?;
        if self.descriptor["predecessor_files"]!=pins{return Err("original eight predecessor anchors changed".into());}
        let mut expected=BTreeSet::from(["authority.lock".to_string()]);
        for (name,row) in pins.as_object().ok_or("pins")? {
            expected.insert(Path::new(name).components().next().ok_or("predecessor name")?.as_os_str().to_string_lossy().into_owned());
            checked_file(&dir.join(name),row)?;
            if let Some(parent)=Path::new(name).parent().filter(|p|!p.as_os_str().is_empty()){exact_names(&dir.join(parent),&["intent.json","result.json"])?;}
        }
        let ancestors=self.descriptor["claimed_ancestors"].as_array().unwrap();
        for a in ancestors {let name=a["directory"].as_str().ok_or("ancestor directory")?;if Path::new(name).components().count()!=1||!name.starts_with("successor-"){return Err("scoped ancestor required".into());}expected.insert(name.into());}
        if has_new{expected.insert(self.successor().into());}
        let found=fs::read_dir(dir).map_err(|e|e.to_string())?.map(|e|e.map(|e|e.file_name().to_string_lossy().into_owned())).collect::<Result<BTreeSet<_>,_>>().map_err(|e|e.to_string())?;
        if found!=expected{return Err("partial/extra original authority successor".into());}
        let mut previous=Value::Null;let mut fences=Value::Null;let mut backings=serde_json::Map::new();
        for (index,a) in ancestors.iter().enumerate(){
            let old=dir.join(a["directory"].as_str().unwrap());let mut names=FILES.to_vec();if index==0{names.retain(|n|*n!="inherited-backing-identities.json");}names.push("service-admission-claim.json");exact_names(&old,&names)?;
            let files=a["files"].as_object().ok_or("ancestor files")?;
            if files.keys().cloned().collect::<BTreeSet<_>>()!=names.iter().map(|s|s.to_string()).collect(){return Err("complete ancestor file pins required".into());}
            for (name,row) in files{checked_file(&old.join(name),row)?;}
            let history=bounded(&old.join("historical-state.json"),1<<20)?;let intent=parsed(&bounded(&old.join("intent.json"),1<<20)?)?;
            // Historical validators rederive accepted old proof/ledger data;
            // they execute no E code and confer no current-domain permit.
            let p=match index {0=>from_e_proof(&history,intent["verified_candidate"].clone())?,1=>current::from_current_e_proof(&history,dir,intent["verified_candidate"].clone())?,_=>return Err("additional historical producer context not accepted".into())};
            if a["version"]!=index as u64+1 || a["resource_ids"]!=json!(p.terminal_ids) || intent["proof_sha256"]!=p.proof_sha || p.prior_backings!=json!(backings){return Err("claimed ancestor proof/backing chain changed".into());}
            let commit=parsed(&bounded(&old.join("commit.json"),1<<20)?)?;
            if commit["kind"]!="TerminalDomainAuthorityCommitted" || commit["original_state_sha256"]!=digest(&history) || commit["proof_consumed_once"]!=p.proof_sha || commit["normal_SDK_ACK"]!=false ||
                commit["resulting76_sha256"]!=digest(&bounded(&old.join("resulting76.json"),1<<20)?) || commit["fences_sha256"]!=digest(&bounded(&old.join("permanent-fences.json"),1<<20)?) {
                return Err("ancestor commit/proof binding changed".into());
            }
            if index>0{
                let data=FILES.iter().filter(|n|**n!="commit.json").map(|n|Ok(((*n).into(),json!(digest(&bounded(&old.join(n),1<<20)?))))).collect::<Result<serde_json::Map<_,_>,String>>()?;
                if commit["version"]!=a["version"] || commit["files"]!=json!(data) || parsed(&bounded(&old.join("inherited-backing-identities.json"),1<<20)?)?!=p.prior_backings{return Err("ancestor complete hash/backing manifest changed".into());}
            }
            let mut ledger=DdrResources::inherit_terminal_source(&p)?;p.matches_ledger(&parsed(&bounded(&old.join("before128.json"),1<<20)?)?)?;ledger.apply_terminal_loss(&p)?;
            let after=parsed(&bounded(&old.join("resulting76.json"),1<<20)?)?;
            if !ledger_equal(&after["resource_ledger"],&ledger.snapshot())? || parsed(&bounded(&old.join("permanent-fences.json"),1<<20)?)?!=p.fences || after["reconciliation"]["terminal_domain_fences"]!=p.fences ||
                after["B_conservative_bytes"]!=76<<20 || after["normal_SDK_ACK"]!=false{return Err(format!("ancestor{} rederived76/fences changed",index+1));}
            let claim=parsed(&bounded(&old.join("service-admission-claim.json"),1<<20)?)?;
            ledger.reserve_unique("canary:nodeA",2<<20)?;ledger.reserve_unique("canary:nodeB",2<<20)?;
            let first=p.fences["minimum_next_object_id"].as_u64().ok_or("ancestor object highwater")?;
            if claim["kind"]!="SuccessorServiceAdmissionPending" || claim["source_terminal_commit_sha256"]!=digest(&bounded(&old.join("commit.json"),1<<20)?) ||
                !ledger_equal(&claim["resource_ledger"],&ledger.snapshot())? || claim["canary_charge_objects"]!=json!({"nodeA":first,"nodeB":first+1}) ||
                claim["conservative_B_bytes"]!=80<<20 || claim["native_SDK_ACK"]!=false || claim["Guest_identity_known"]!=false || claim["business_admission_enabled"]!=false{return Err("complete original consumed claim required".into());}
            reject_fenced(&after["reconciliation"],&claim["namespace"])?;
            for id in p.terminal_ids(){backings.insert(id.to_string(),rows(p.source_ledger())?.get(id).ok_or("original historical backing")?.clone());}
            previous=after;fences=p.fences;
        }
        Ok(Lineage{pins:self.descriptor.clone(),previous,previous_fences:fences,backings:json!(backings)})
    }
}
fn checked_file(path:&Path,row:&Value)->Result<(),String>{let raw=bounded(path,1<<20)?;if row["bytes"]!=raw.len()||row["sha256"]!=digest(&raw){return Err("source/ancestor exact bytes drift".into());}Ok(())}
fn exact_names(path:&Path,names:&[&str])->Result<(),String>{
    let i=path.symlink_metadata().map_err(|e|e.to_string())?;if !i.is_dir()||i.file_type().is_symlink(){return Err("ordinary metadata directory required".into());}
    let actual=fs::read_dir(path).map_err(|e|e.to_string())?.map(|e|e.map(|e|e.file_name().to_string_lossy().into_owned())).collect::<Result<BTreeSet<_>,_>>().map_err(|e|e.to_string())?;
    if actual!=names.iter().map(|s|s.to_string()).collect(){return Err("incomplete/claimed/extra successor metadata".into());}Ok(())
}
fn candidate(case:&Case,bytes:&[u8],lineage:&Lineage,proof:Value)->Result<VerifiedCandidate,String>{
    if digest(bytes)!=case.sha() || bytes.len() as u64!=case.descriptor["current_state"]["bytes"].as_u64().unwrap(){return Err("exact current source snapshot required".into());}
    let state=parsed(bytes)?;let source=&state["resource_ledger"];let live=rows(source)?;let prior=&lineage.previous["resource_ledger"];
    if state["revision"]!=case.revision() || state["charged_bytes"]!=64<<20 || state["creation_reserved_bytes"]!=64<<20 || state["active_bytes"]!=52<<20 ||
        state["reconciliation"]["terminal_domain_fences"]!=lineage.previous_fences || ["terminal_domain_loss_tombstones","retained_uncertain_reservations","reservations","retired_unstarted_object_ids"].iter().any(|k|source[*k]!=prior[*k]) {
        return Err("current protected76/prior history drift".into());
    }
    let protected=rows(prior)?;
    if protected.iter().any(|(id,row)|live.get(id)!=Some(row)) || live.len()!=protected.len()+3 ||
        case.ids().iter().zip([2<<20,2<<20,48<<20]).any(|(id,n)|live.get(id).is_none_or(|row|row["nbytes"]!=n||!row["retained"].is_null())) {return Err("only exact current three resources52 eligible".into());}
    let pool=&state["pools"][0];if state["pools"].as_array().map(Vec::len)!=Some(1)||pool["resource_id"]!=case.ids()[2]||pool["frozen"]!=true||!pool["release_result"].is_null()||pool["clients"].as_array().map(Vec::len)!=Some(3){return Err("exact frozen current pool required".into());}
    if lineage.backings.as_object().unwrap().len()!=case.ids()[0] as usize-110{return Err("all ancestral backing identities required".into());}
    let fences=current::union_fences_for(&state,&lineage.previous_fences,&lineage.backings,case.descriptor["source_run"].as_str().unwrap())?;
    Ok(VerifiedCandidate{state,proof_sha:digest(&serde_json::to_vec(&proof).map_err(|e|e.to_string())?),proof,fences,
        terminal_ids:case.ids(),source_sha:case.sha().into(),prior_backings:lineage.backings.clone()})
}
fn reconciliation(case:&Case,p:&VerifiedCandidate,path:&Path)->Value{
    let mut v=p.state["reconciliation"].clone();v["previous_terminal_domain_history"]=v["terminal_domain_history"].clone();
    v["previous_reconciliation_phase"]=v["phase"].clone();v["phase"]=json!(format!("TerminalDomainDataLost{}V{}Protected76",case.descriptor["source_run"].as_str().unwrap().to_uppercase(),case.version()));
    v["terminal_domain_fences"]=p.fences.clone();v["terminal_proof_sha256"]=json!(p.proof_sha);
    v["terminal_domain_history"]=json!({"path":path.join("historical-state.json"),"sha256":case.sha(),"outcome":"TerminalDomainDataLost","normal_SDK_ACK":false});v
}
fn check_lock(lock:&File,dir:&Path)->Result<(),String>{
    let held=lock.metadata().map_err(|e|e.to_string())?;let named=dir.join("authority.lock").symlink_metadata().map_err(|e|e.to_string())?;
    if !held.is_file()||held.nlink()!=1||named.file_type().is_symlink()||held.dev()!=named.dev()||held.ino()!=named.ino(){return Err("original single-link lock identity changed".into());}Ok(())
}
fn result(case:&Case,p:&VerifiedCandidate,path:&Path,ledger:&DdrResources)->Value{json!({"kind":"VersionedCurrentTerminalDomainDataLost","version":case.version(),"revision":case.revision()+1,
    "resource_ledger":ledger.snapshot(),"reconciliation":reconciliation(case,p,path),"charged_bytes":12<<20,"creation_reserved_bytes":64<<20,
    "B_conservative_bytes":76<<20,"normal_SDK_ACK":false,"new_admission_enabled":false})}
fn transact(dir:&Path,state_path:&Path,case:&Case,verify:impl Fn(&[u8],&Lineage)->Result<VerifiedCandidate,String>,crash:&str)->Result<TerminalAuthority,String>{
    let lock=lock_lineage(dir)?;check_lock(&lock,dir)?;let lineage=case.lineage(dir,false)?;let original=bounded(state_path,1<<20)?;
    let p=verify(&original,&lineage)?;let mut ledger=DdrResources::inherit_terminal_source(&p)?;
    let path=dir.join(case.successor());fs::DirBuilder::new().mode(0o700).create(&path).map_err(|e|e.to_string())?;File::open(dir).and_then(|f|f.sync_all()).map_err(|e|e.to_string())?;fault("directory",crash)?;
    atomic_record(&path,"intent.json",&json!({"kind":"VersionedCurrentTerminalLossPending","version":case.version(),"case":case.descriptor,"case_sha256":case.descriptor_sha,
        "source_state_sha256":case.sha(),"source_revision":case.revision(),"proof_sha256":p.proof_sha,"verified_candidate":p.proof,"lineage":lineage.pins,"resource_ids":case.ids(),"normal_SDK_ACK":false}))?;fault("intent",crash)?;
    use std::io::Write;
    let mut h=OpenOptions::new().write(true).create_new(true).mode(0o600).custom_flags(libc::O_NOFOLLOW).open(path.join("historical-state.json")).map_err(|e|e.to_string())?;
    h.write_all(&original).and_then(|_|h.sync_all()).map_err(|e|e.to_string())?;File::open(&path).and_then(|f|f.sync_all()).map_err(|e|e.to_string())?;fault("historical128",crash)?;
    atomic_record(&path,"before128.json",&ledger.snapshot())?;fault("before128",crash)?;atomic_record(&path,"permanent-fences.json",&p.fences)?;fault("fences",crash)?;
    atomic_record(&path,"inherited-backing-identities.json",&p.prior_backings)?;fault("backings",crash)?;ledger.apply_terminal_loss(&p)?;fault("terminal52",crash)?;
    let value=result(case,&p,&path,&ledger);atomic_record(&path,"resulting76.json",&value)?;fault("resulting76",crash)?;
    let fresh=case.lineage(dir,true)?;if bounded(state_path,1<<20)?!=original||fresh.pins!=lineage.pins||verify(&original,&fresh)?.proof_sha!=p.proof_sha{return Err("current source/ancestor/proof drift before commit".into());}check_lock(&lock,dir)?;
    let hashes=FILES.iter().filter(|n|**n!="commit.json").map(|n|Ok(((*n).to_string(),json!(digest(&bounded(&path.join(n),1<<20)?))))).collect::<Result<serde_json::Map<_,_>,String>>()?;
    atomic_record(&path,"commit.json",&json!({"kind":"TerminalDomainAuthorityCommitted","version":case.version(),"original_state_sha256":case.sha(),"case_sha256":case.descriptor_sha,
        "proof_consumed_once":p.proof_sha,"files":hashes,"resulting76_sha256":hashes["resulting76.json"],"fences_sha256":hashes["permanent-fences.json"],"normal_SDK_ACK":false}))?;fault("commit",crash)?;
    Ok(TerminalAuthority{_original_lock:lock,ledger:Some(ledger),reconciliation:value["reconciliation"].clone(),path,source_sha:case.sha().into(),revision:case.revision()+1})
}
fn recover(dir:&Path,state_path:&Path,case:&Case,verify:impl Fn(&[u8],&Lineage)->Result<VerifiedCandidate,String>)->Result<TerminalAuthority,String>{
    let lock=lock_lineage(dir)?;check_lock(&lock,dir)?;let lineage=case.lineage(dir,true)?;let path=dir.join(case.successor());exact_names(&path,FILES)?;
    let original=bounded(state_path,1<<20)?;let p=verify(&original,&lineage)?;let commit=parsed(&bounded(&path.join("commit.json"),1<<20)?)?;
    if commit["kind"]!="TerminalDomainAuthorityCommitted"||commit["version"]!=case.version()||commit["original_state_sha256"]!=case.sha()||commit["case_sha256"]!=case.descriptor_sha||commit["proof_consumed_once"]!=p.proof_sha||commit["normal_SDK_ACK"]!=false{return Err("committed case/proof binding changed".into());}
    if commit["files"].as_object().map(serde_json::Map::len)!=Some(FILES.len()-1){return Err("complete committed metadata hashes required".into());}
    for name in FILES.iter().filter(|n|**n!="commit.json"){if commit["files"][*name]!=digest(&bounded(&path.join(name),1<<20)?){return Err("committed metadata drift".into());}}
    let intent=parsed(&bounded(&path.join("intent.json"),1<<20)?)?;
    if intent["kind"]!="VersionedCurrentTerminalLossPending"||intent["version"]!=case.version()||intent["case"]!=case.descriptor||intent["case_sha256"]!=case.descriptor_sha||intent["lineage"]!=lineage.pins||intent["verified_candidate"]!=p.proof||intent["proof_sha256"]!=p.proof_sha||intent["source_state_sha256"]!=case.sha()||intent["source_revision"]!=case.revision()||intent["resource_ids"]!=json!(case.ids())||intent["normal_SDK_ACK"]!=false||bounded(&path.join("historical-state.json"),1<<20)?!=original{return Err("committed source/lineage/intent changed".into());}
    p.matches_ledger(&parsed(&bounded(&path.join("before128.json"),1<<20)?)?)?;let mut ledger=DdrResources::inherit_terminal_source(&p)?;ledger.apply_terminal_loss(&p)?;
    let after=parsed(&bounded(&path.join("resulting76.json"),1<<20)?)?;
    let mut expected=result(case,&p,&path,&ledger);let mut actual=after.clone();expected["resource_ledger"]=normalized_ledger(&expected["resource_ledger"])?;actual["resource_ledger"]=normalized_ledger(&actual["resource_ledger"])?;
    if actual!=expected||parsed(&bounded(&path.join("inherited-backing-identities.json"),1<<20)?)?!=p.prior_backings{return Err("rederived ledger/backing/fences changed".into());}
    let a=TerminalAuthority{_original_lock:lock,ledger:Some(ledger),reconciliation:after["reconciliation"].clone(),path,source_sha:case.sha().into(),revision:case.revision()+1};a.validate_commit()?;Ok(a)
}

#[cfg(test)]mod tests;
