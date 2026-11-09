//! One fixed closed-c039 PRESTART transition. Not a general Unknown rollback.
use crate::{catalog::Catalog,ddr_resources::{BackingId,DdrResources},pool_retained8::{atomic_record,lock_lineage},ddr_recovery_coverage::digest};
use serde_json::{json,Value};
use std::{fs::{self,File,OpenOptions},io::Read,path::Path,process::Command};
use std::os::unix::fs::{DirBuilderExt,OpenOptionsExt};
const ROOT:&str="/workspace/uflow/runs/mem-service-shared-pool-20261108";
const SHA:&str="aaa11ce813b19a5c325352a3d108497a8cb552683d1a67b05160d4e37ecd9ff1";
const EXPECTED:&str=include_str!("../../../examples/mem_service_pool/reconciliation/closed-c039-prestart-assessment.json");
const VERIFY:&str=include_str!("../../../tools/phasec/verify_c039_prestart.py");
const R8:&str=include_str!("../../../tools/phasec/verify_retained8_pool.py");
const Q:&str=include_str!("../../../tools/phasec/verify_quarantine76_pool.py");
const K:&str=include_str!("../../../tools/phasec/kernel_profile_gate.py");
const NEXT:&str="successor-c039-prestart-v1";
const MAX_PREDECESSOR:u64=1<<20;
const FILES:[&str;6]=["intent.json","result.json","successor-c027-v1/intent.json","successor-c027-v1/result.json","successor-c033-v1/intent.json","successor-c033-v1/result.json"];
fn expected()->Value{serde_json::from_str(EXPECTED).expect("embedded fixed assessment")}
fn names(dir:&Path,want:&[&str])->Result<(),String>{
    let m=dir.symlink_metadata().map_err(|e|e.to_string())?;if !m.is_dir()||m.file_type().is_symlink(){return Err("real predecessor directory".into());}
    let found=fs::read_dir(dir).map_err(|e|e.to_string())?.map(|e|e.map(|e|e.file_name().to_string_lossy().into_owned())).collect::<Result<std::collections::BTreeSet<_>,_>>().map_err(|e|e.to_string())?;
    if found!=want.iter().map(|s|s.to_string()).collect(){return Err("complete predecessor only; partial/reentry/restart refused".into());}Ok(())
}
fn bounded(path:&Path)->Result<Vec<u8>,String>{
    let mut f=OpenOptions::new().read(true).custom_flags(libc::O_NOFOLLOW|libc::O_NONBLOCK).open(path).map_err(|e|e.to_string())?;
    let m=f.metadata().map_err(|e|e.to_string())?;if !m.is_file()||m.len()>MAX_PREDECESSOR{return Err("bounded regular predecessor <=1MiB".into());}
    let mut b=Vec::new();f.by_ref().take(MAX_PREDECESSOR+1).read_to_end(&mut b).map_err(|e|e.to_string())?;
    if b.len() as u64!=m.len()||f.metadata().map_err(|e|e.to_string())?.len()!=m.len(){return Err("complete stable predecessor".into());}Ok(b)
}
fn predecessors(dir:&Path)->Result<Value,String>{
    names(dir,&["authority.lock","intent.json","result.json","successor-c027-v1","successor-c033-v1"])?;
    for n in ["successor-c027-v1","successor-c033-v1"]{names(&dir.join(n),&["intent.json","result.json"])?;}
    let a=expected();let mut last=Value::Null;
    for n in FILES{
        let b=bounded(&dir.join(n))?;let row=&a["files_requiring_fresh_exact_byte_validation"][format!("implementation/responsibility-c013/{n}")];
        if row["bytes"]!=b.len()||row["sha256"]!=digest(&b){return Err("fixed predecessor bytes/hash differ".into());}
        if n=="successor-c033-v1/result.json"{last=serde_json::from_slice(&b).map_err(|e|e.to_string())?;}
    }Ok(last)
}
fn checked(st:&Catalog,p:&Value,prior:&Value)->Result<DdrResources,String>{
    let a=expected();let pins:serde_json::Map<String,Value>=a["files_requiring_fresh_exact_byte_validation"].as_object().unwrap().iter().map(|(n,r)|(n.clone(),r["sha256"].clone())).collect();
    if !st.managed_pools.reconciliation.is_null()||!st.objects.is_empty()||!st.resource_bindings.is_empty()||!st.managed_pools.pools.is_empty()||!st.managed_pools.operations.is_empty()||st.ddr_resources.charged_bytes()!=0||st.ddr_resources.reserved_bytes()!=0||st.ddr_resources.limit<80<<20||st.ddr_resources.limit>128<<20||
        p["schema"]!=1||p["kind"]!="verified-closed-c039-prestart"||p["assessment_sha256"]!=SHA||p["predecessor_state_sha256"]!=a["predecessor_state_sha256"]||p["completed_succession_proof_sha256"]!=a["completed_succession_proof_sha256"]||p["before_total_bytes"]!=80<<20||p["cancel_keys"]!=a["current_unstarted_canary_reservations"]||p["evidence_sha256"]!=Value::Object(pins)||p["SDK_ACK_Unmapped_or_canary_close_claimed"]!=false||p["old64_cancelled"]!=false {
        return Err("only fixed complete80/prestart proof into empty bounded Catalog".into());
    }
    if prior["phase"]!="ClosedC033Quarantine76"||prior["proof_sha256"]!=a["completed_succession_proof_sha256"]{return Err("completed predecessor required".into());}
    for k in ["identity","historical_pool","historical_unknown_operation","retained_canaries","quarantined_creation"]{if p["carry"][k]!=prior[k]{return Err("complete historical identity/Unknown continuity".into());}}
    if p["carry"]["retained_canaries"]!=a["old_retained_canaries"]||p["carry"]["quarantined_creation"]!=a["old_uncertain_business_reservation"]||p["carry"]["historical_prestart_contexts"]!=json!([{"realm":"simulated:mem-service:d814dbe5-9d80-4b90-8fac-d10c5d8bea44","provider_session":"588207fc-01c2-4360-9f12-9e3e89efbbd9","guest_identities":a["c039_guest_identities"],"unstarted_reserved_ids":[108,109]}]) {return Err("all four historical runtime generations fenced".into());}
    let mut ledger=DdrResources::default();ledger.limit=st.ddr_resources.limit;
    for c in a["old_retained_canaries"].as_array().unwrap(){
        let id=c["object_id"].as_u64().ok_or("id")?;let b=&c["backing"];let key=format!("inherited-c039:{id}");ledger.reserve_unique(&key,2<<20)?;
        ledger.commit_backing(&key,id,BackingId::Provider{realm:b["realm"].as_str().ok_or("realm")?.into(),provider:b["provider"].as_str().ok_or("provider")?.into(),resource:b["resource"].as_str().ok_or("resource")?.into(),generation:b["generation"].as_u64().ok_or("generation")?},2<<20)?;ledger.retain_provider(id,c.clone())?;
    }
    let u=&a["old_uncertain_business_reservation"];let key=u["key"].as_str().ok_or("key")?;ledger.reserve_unique(key,64<<20)?;ledger.retain_uncertain_reservation(key,107,u.clone())?;
    for key in ["canary:nodeA","canary:nodeB"]{ledger.reserve_unique(key,2<<20)?;}
    ledger.protect_c039_unstarted_ids()?;
    if ledger.charged_bytes()!=12<<20||ledger.reserved_bytes()!=68<<20||ledger.retained_reserved_bytes()!=64<<20{return Err("full inherited80 required".into());}Ok(ledger)
}
fn fault(p:&Value,point:&str)->Result<(),String>{
    #[cfg(test)]if p["CPU_crash_at"]==point{return Err(format!("CPU crash after {point}; no restart"));}
    let _=(p,point);Ok(())
}
fn apply(st:&mut Catalog,p:&Value,dir:&Path,authority:&Path)->Result<(),String>{
    let prior=predecessors(dir)?;let ledger=checked(st,p,&prior)?;
    for n in ["state.json","prestart-inherited80.json","prestart-resulting76.json"]{if authority.join(n).symlink_metadata().is_ok(){return Err("state already exists or partial; no restart/replacement".into());}}
    let next=dir.join(NEXT);fs::DirBuilder::new().mode(0o700).create(&next).map_err(|e|e.to_string())?;File::open(dir).and_then(|f|f.sync_all()).map_err(|e|e.to_string())?;
    st.ddr_resources=ledger;st.next_object_id=st.next_object_id.max(110);st.next_placement_id=st.next_placement_id.max(10002);st.next_resource_binding_id=st.next_resource_binding_id.max(40002);
    let snapshot=|st:&Catalog,phase:&str|json!({"schema":1,"phase":phase,"authority":authority,"predecessor_state_sha256":p["predecessor_state_sha256"],"carry":p["carry"],"resource_ledger":st.ddr_resources.snapshot(),"minimum_next_object_id":st.next_object_id,"minimum_next_placement_id":st.next_placement_id,"minimum_next_binding_id":st.next_resource_binding_id});
    let baseline=snapshot(st,"Inherited80BeforePrestartCancellation");
    let mut stored_proof=p.clone();
    #[cfg(test)]for k in ["CPU_predecessor_bytes","CPU_crash_at"]{stored_proof.as_object_mut().unwrap().remove(k);}
    let mut record=p["carry"].clone();record["schema"]=json!(1);record["phase"]=json!("ClosedC039PrestartPending");record["authority"]=json!(authority);record["assessment_sha256"]=json!(SHA);record["proof_sha256"]=p["proof_sha256"].clone();record["verified_proof"]=stored_proof;record["predecessor_state_sha256"]=p["predecessor_state_sha256"].clone();
    record["predecessor_files"]=json!(FILES.iter().map(|n|(n.to_string(),expected()["files_requiring_fresh_exact_byte_validation"][format!("implementation/responsibility-c013/{n}")].clone())).collect::<std::collections::BTreeMap<_,_>>());
    record["cancel64_repeated"]=json!(false);record["SDK_ACK_Unmapped_or_canary_close_claimed"]=json!(false);record["prestart_cancel_intent"]=p["cancel_keys"].clone();record["resource_ledger"]=st.ddr_resources.snapshot();
    atomic_record(&next,"intent.json",&record)?;fault(p,"intent")?;
    atomic_record(authority,"prestart-inherited80.json",&baseline)?;fault(p,"inherited80")?;
    // Only this fixed proof path can make these two cancellations. Ordinary
    // APIs still cannot mutate the retained attempted64 or six old backings.
    for key in ["canary:nodeA","canary:nodeB"]{st.ddr_resources.cancel_uncreated(key);}
    if st.ddr_resources.charged_bytes()!=12<<20||st.ddr_resources.reserved_bytes()!=64<<20||st.ddr_resources.retained_reserved_bytes()!=64<<20{return Err("exact two prestart cancellations only".into());}
    fault(p,"cancel")?;let final76=snapshot(st,"Resulting76BeforeNewAdmission");atomic_record(authority,"prestart-resulting76.json",&final76)?;fault(p,"resulting76")?;
    record["phase"]=json!("ClosedC039Prestart76");record["prestart_unstarted_reservation_cancelled_bytes"]=json!(4<<20);record["resource_ledger"]=st.ddr_resources.snapshot();
    record["inherited80_snapshot_sha256"]=json!(digest(&bounded(&authority.join("prestart-inherited80.json"))?));record["resulting76_snapshot_sha256"]=json!(digest(&bounded(&authority.join("prestart-resulting76.json"))?));
    st.managed_pools.reconciliation=record.clone();st.managed_pools.revision=1;atomic_record(authority,"state.json",&st.managed_pools.snapshot(st))?;fault(p,"state76")?;
    atomic_record(&next,"result.json",&record)?;Ok(())
}
pub(crate) fn restore(st:&mut Catalog,c:&Value,authority:&Path)->Result<File,String>{
    if c.as_object().map(|m|m.len())!=Some(1){return Err("one exact prestart config, no mixed restore modes".into());}
    let path=Path::new(c["closed_c039_prestart_assessment"].as_str().ok_or("fixed prestart assessment")?);
    if !path.is_absolute()||!path.starts_with(Path::new(ROOT).join("implementation")){return Err("controlled assessment only".into());}
    let dir=Path::new(ROOT).join("implementation/responsibility-c013");let lock=lock_lineage(&dir)?;predecessors(&dir)?;
    let code=format!("R8_SOURCE={}\nQ_SOURCE={}\nK_SOURCE={}\n{}",serde_json::to_string(R8).unwrap(),serde_json::to_string(Q).unwrap(),serde_json::to_string(K).unwrap(),VERIFY);
    let output=Command::new("/usr/local/python3.11.14/bin/python3.11").args(["-B","-I","-S","-c",&code]).arg(path).arg(authority).output().map_err(|e|e.to_string())?;
    if !output.status.success()||output.stdout.len()>262144{return Err(format!("fixed prestart proof refused; retain80/no bootstrap: {}",String::from_utf8_lossy(&output.stderr).chars().take(1600).collect::<String>()));}
    let p:Value=serde_json::from_slice(&output.stdout).map_err(|e|e.to_string())?;
    let mut canonical=p.clone();canonical.as_object_mut().ok_or("proof object")?.remove("proof_sha256");
    if p["proof_sha256"]!=digest(&serde_json::to_vec(&canonical).map_err(|e|e.to_string())?){return Err("proof digest mismatch".into());}
    apply(st,&p,&dir,authority)?;Ok(lock)
}
#[cfg(test)]pub(crate) fn fixture_proof()->Value{serde_json::from_str(include_str!("../../../examples/mem_service_pool/reconciliation/closed-c039-cpu-proof.json")).unwrap()}
#[cfg(test)]pub(crate) fn restore_fixture(st:&mut Catalog,p:&Value,dir:&Path,authority:&Path)->Result<File,String>{
    fs::create_dir(dir).map_err(|e|e.to_string())?;File::create(dir.join("authority.lock")).map_err(|e|e.to_string())?;
    for n in ["successor-c027-v1","successor-c033-v1"]{fs::create_dir(dir.join(n)).map_err(|e|e.to_string())?;}
    for n in FILES{fs::write(dir.join(n),p["CPU_predecessor_bytes"][n].as_str().ok_or("CPU original bytes")?).map_err(|e|e.to_string())?;}
    let lock=lock_lineage(dir)?;apply(st,p,dir,authority)?;Ok(lock)
}
#[cfg(test)]mod tests {
    use super::*;use std::sync::atomic::{AtomicU64,Ordering};static N:AtomicU64=AtomicU64::new(0);
    struct Scratch(std::path::PathBuf);
    impl Scratch{fn new()->Self{let p=std::env::temp_dir().join(format!("uf-c039-prestart-{}-{}",std::process::id(),N.fetch_add(1,Ordering::Relaxed)));fs::create_dir(&p).unwrap();Self(p)}}
    impl Drop for Scratch{fn drop(&mut self){assert_eq!(self.0.parent().unwrap(),std::env::temp_dir());fs::remove_dir_all(&self.0).unwrap();}}
    fn catalog()->Catalog{let mut s=Catalog::new(-1,vec![]);s.ddr_resources.limit=128<<20;s}
    fn prior(p:&Value)->Value{serde_json::from_str(p["CPU_predecessor_bytes"]["successor-c033-v1/result.json"].as_str().unwrap()).unwrap()}
    #[test]fn staged_c039_prestart_fixture(){
        if let Ok(config)=std::env::var("UFLOW_RECON_CATALOG_CPU_CONFIG"){
            let c:Value=serde_json::from_slice(&fs::read(config).unwrap()).unwrap();
            crate::pool_service::run_reconciliation_fixture(c["endpoint"].as_str().unwrap(),Path::new(c["state"].as_str().unwrap()),Path::new(c["provider_config"].as_str().unwrap()),&fixture_proof()).unwrap();
        }else{let d=Scratch::new();let mut s=catalog();let _l=restore_fixture(&mut s,&fixture_proof(),&d.0.join("lineage"),&d.0).unwrap();assert_eq!(s.ddr_resources.reserved_bytes(),64<<20);}
    }
    #[test]fn durable80_then_exact_cancel76_before_new4_and48(){
        let d=Scratch::new();let mut s=catalog();let p=fixture_proof();let _l=restore_fixture(&mut s,&p,&d.0.join("lineage"),&d.0).unwrap();
        let a:Value=serde_json::from_slice(&fs::read(d.0.join("prestart-inherited80.json")).unwrap()).unwrap();let z:Value=serde_json::from_slice(&fs::read(d.0.join("prestart-resulting76.json")).unwrap()).unwrap();
        assert_eq!(a["resource_ledger"]["reserved_bytes"],68<<20);assert_eq!(z["resource_ledger"]["reserved_bytes"],64<<20);assert_eq!(a["resource_ledger"]["charged_bytes"],12<<20);
        assert_eq!((s.next_object_id,s.next_placement_id,s.next_resource_binding_id),(110,10002,40002));
        s.ddr_resources.reserve_unique("canary:nodeA",2<<20).unwrap();s.ddr_resources.reserve_unique("canary:nodeB",2<<20).unwrap();s.ddr_resources.reserve_unique("new48",48<<20).unwrap();
        assert_eq!(s.ddr_resources.charged_bytes()+s.ddr_resources.reserved_bytes(),128<<20);assert!(s.ddr_resources.reserve_unique("overflow",1).is_err());
        assert_eq!(s.managed_pools.reconciliation["prestart_unstarted_reservation_cancelled_bytes"],4<<20);
    }
    #[test]fn historical64_canaries12_and_unstarted_ids_cannot_be_refunded_or_reused(){
        let d=Scratch::new();let mut s=catalog();let p=fixture_proof();let _l=restore_fixture(&mut s,&p,&d.0.join("lineage"),&d.0).unwrap();let key=p["carry"]["quarantined_creation"]["key"].as_str().unwrap();
        s.ddr_resources.cancel_uncreated(key);assert!(s.ddr_resources.reserve_unique(key,64<<20).is_err());assert!(s.ddr_resources.commit(key,107,1,2,64<<20).is_err());
        for id in 100..=109{s.ddr_resources.release_confirmed(id);}
        for id in [107,108,109]{s.ddr_resources.reserve_unique("unrelated",1).unwrap();assert!(s.ddr_resources.commit("unrelated",id,1,2,1).is_err());s.ddr_resources.cancel_uncreated("unrelated");}
        assert_eq!(s.ddr_resources.charged_bytes(),12<<20);assert_eq!(s.ddr_resources.reserved_bytes(),64<<20);
        assert_eq!(s.ddr_resources.snapshot()["retired_unstarted_object_ids"],json!([108,109]));
    }
    #[test]fn wrong_proof_delta_old_witness_or_fourth_context_never_imports(){
        let p=fixture_proof();let old=prior(&p);
        for key in ["schema","assessment_sha256","predecessor_state_sha256","before_total_bytes","completed_succession_proof_sha256"]{let mut bad=p.clone();bad[key]=Value::Null;assert!(checked(&catalog(),&bad,&old).is_err());}
        for key in ["canary:nodeA","canary:nodeB"]{let mut bad=p.clone();bad["cancel_keys"][key]["nbytes"]=json!(1);assert!(checked(&catalog(),&bad,&old).is_err());}
        let mut bad=p.clone();bad["cancel_keys"]["old64"]=json!({"nbytes":64<<20});assert!(checked(&catalog(),&bad,&old).is_err());
        let mut bad=p.clone();bad["carry"]["retained_canaries"][0]["SDK_checked_close_confirmed"]=json!(true);assert!(checked(&catalog(),&bad,&old).is_err());
        let mut bad=p.clone();bad["carry"]["quarantined_creation"]["pool"]["canonical"]=json!({});assert!(checked(&catalog(),&bad,&old).is_err());
        let mut bad=p;bad["carry"]["historical_prestart_contexts"][0]["unstarted_reserved_ids"]=json!([106,107]);assert!(checked(&catalog(),&bad,&old).is_err());
    }
    #[test]fn partial_at_every_boundary_state_drift_reentry_restart_and_concurrent_lock_refuse(){
        for point in ["intent","inherited80","cancel","resulting76","state76"]{
            let d=Scratch::new();let dir=d.0.join("lineage");let mut p=fixture_proof();p["CPU_crash_at"]=json!(point);let mut s=catalog();assert!(restore_fixture(&mut s,&p,&dir,&d.0).is_err());
            assert!(dir.join(NEXT).join("intent.json").is_file());assert!(!dir.join(NEXT).join("result.json").exists());
            if point!="intent"{let baseline:Value=serde_json::from_slice(&fs::read(d.0.join("prestart-inherited80.json")).unwrap()).unwrap();assert_eq!(baseline["resource_ledger"]["reserved_bytes"],68<<20);}
            assert!(apply(&mut catalog(),&fixture_proof(),&dir,&d.0).is_err());
        }
        let d=Scratch::new();let dir=d.0.join("lineage");let mut s=catalog();let p=fixture_proof();let _l=restore_fixture(&mut s,&p,&dir,&d.0).unwrap();
        let other=dir.clone();assert!(std::thread::spawn(move||lock_lineage(&other).is_err()).join().unwrap());
        assert!(apply(&mut s,&p,&dir,&d.0).is_err());fs::write(d.0.join("state.json"),b"{\"minimum_next_object_id\":100}").unwrap();assert!(apply(&mut catalog(),&p,&dir,&d.0).is_err());
    }
    #[test]fn predecessor_drift_oversize_and_existing_state_fail_before_any_cancel(){
        let p=fixture_proof();
        let d=Scratch::new();let mut bad=p.clone();bad["CPU_predecessor_bytes"]["successor-c033-v1/result.json"]=json!("{}");assert!(restore_fixture(&mut catalog(),&bad,&d.0.join("lineage"),&d.0).is_err());
        let d=Scratch::new();let large=d.0.join("oversize");fs::write(&large,vec![0;MAX_PREDECESSOR as usize+1]).unwrap();assert!(bounded(&large).is_err());
        let d=Scratch::new();fs::write(d.0.join("state.json"),b"{}").unwrap();assert!(restore_fixture(&mut catalog(),&p,&d.0.join("lineage"),&d.0).is_err());assert!(!d.0.join("lineage").join(NEXT).exists());
    }
    #[test]fn live_reservation_or_pool_low_budget_or_budget_bypass_refuse(){
        let p=fixture_proof();let old=prior(&p);for limit in [79<<20,129<<20]{let mut s=catalog();s.ddr_resources.limit=limit;assert!(checked(&s,&p,&old).is_err());}
        let mut s=catalog();s.ddr_resources.reserve_unique("new-live-Unknown",1).unwrap();assert!(checked(&s,&p,&old).is_err());
        let mut s=catalog();s.managed_pools.reconciliation=json!({"phase":"ClosedC039Prestart76"});assert!(checked(&s,&p,&old).is_err());
    }
}
