//! Fixed c033 closed pure-QEMU quarantine. No release, backing or reconciliation
//! is inferred. Unknown is immutable inaccessible history; all 76MiB is charged.
use crate::{catalog::Catalog,ddr_resources::{BackingId,DdrResources},pool_retained8::{atomic_record,lock_lineage}};
use serde_json::{json,Value};
use std::{fs::{self,File},path::Path,process::Command};
use std::os::unix::fs::DirBuilderExt;
const ROOT:&str="/workspace/uflow/runs/mem-service-shared-pool-20261108";
const SHA:&str="0c147898be722733e5d48b4c27fa60a3fb5a97f1a836394eccf3307ff4e57e13";
const EXPECTED:&str=include_str!("../../../examples/mem_service_pool/reconciliation/quarantine76-assessment.json");
const VERIFY:&str=include_str!("../../../tools/phasec/verify_quarantine76_pool.py");
const R8:&str=include_str!("../../../tools/phasec/verify_retained8_pool.py");
const NEXT:&str="successor-c033-v1";

fn names(dir:&Path,expected:&[&str])->Result<(),String>{
    let m=dir.symlink_metadata().map_err(|e|e.to_string())?;
    if !m.is_dir() || m.file_type().is_symlink(){return Err("real lineage directory required".into());}
    let actual=fs::read_dir(dir).map_err(|e|e.to_string())?.map(|e|e.map(|e|e.file_name().to_string_lossy().into_owned()))
        .collect::<Result<std::collections::BTreeSet<_>,_>>().map_err(|e|e.to_string())?;
    if actual!=expected.iter().map(|s|s.to_string()).collect(){return Err("complete predecessor only; partial/existing succession rejects restart".into());}
    Ok(())
}
fn predecessors(dir:&Path,p:&Value)->Result<(),String>{
    names(dir,&["authority.lock","intent.json","result.json","successor-c027-v1"])?;
    names(&dir.join("successor-c027-v1"),&["intent.json","result.json"])?;
    for rel in ["intent.json","result.json","successor-c027-v1/intent.json","successor-c027-v1/result.json"] {
        let path=dir.join(rel);let m=path.symlink_metadata().map_err(|e|e.to_string())?;
        if !m.is_file() || m.file_type().is_symlink() || m.len()>262144{return Err("bounded regular predecessor".into());}
        let v:Value=serde_json::from_slice(&fs::read(path).map_err(|e|e.to_string())?).map_err(|e|e.to_string())?;
        if v!=p["predecessors"][format!("implementation/responsibility-c013/{rel}")] {return Err("actual complete predecessor differs".into());}
    }
    Ok(())
}
fn checked_resources(st:&Catalog,p:&Value)->Result<DdrResources,String>{
    let a:Value=serde_json::from_str(EXPECTED).map_err(|e|e.to_string())?;
    if !st.managed_pools.reconciliation.is_null() || !st.objects.is_empty() || !st.resource_bindings.is_empty() ||
        !st.managed_pools.pools.is_empty() || !st.managed_pools.operations.is_empty() ||
        st.ddr_resources.charged_bytes()!=0 || st.ddr_resources.reserved_bytes()!=0 ||
        p["kind"]!="verified-closed-c033-quarantine76" || p["schema"]!=1 || p["assessment_sha256"]!=SHA ||
        p["predecessor_state_sha256"]!=a["predecessor_state_sha256"] || p["retained_canaries"]!=a["retained_canaries"] ||
        p["uncertain_business_reservation"]!=a["uncertain_business_reservation"] ||
        p["before_total_bytes"]!=76<<20 || p["charged_bytes"]!=12<<20 || p["reserved_bytes"]!=64<<20 ||
        p["cancel64_repeated"]!=false || p["SDK_ACK_Unmapped_or_canary_close_claimed"]!=false {
        return Err("only fixed complete conservative closed-c03376 into empty Catalog".into());
    }
    let mut ledger=DdrResources::default();ledger.limit=st.ddr_resources.limit;
    for c in p["retained_canaries"].as_array().ok_or("six canaries")? {
        let id=c["object_id"].as_u64().ok_or("id")?;let b=&c["backing"];let op=format!("inherited-c033:{id}");
        ledger.reserve_unique(&op,2<<20)?;
        ledger.commit_backing(&op,id,BackingId::Provider{realm:b["realm"].as_str().ok_or("realm")?.into(),
            provider:b["provider"].as_str().ok_or("provider")?.into(),resource:b["resource"].as_str().ok_or("resource")?.into(),
            generation:b["generation"].as_u64().ok_or("generation")?},2<<20)?;
        ledger.retain_provider(id,c.clone())?;
    }
    let u=&p["uncertain_business_reservation"];let key=u["key"].as_str().ok_or("reservation key")?;
    ledger.reserve_unique(key,64<<20)?;ledger.retain_uncertain_reservation(key,107,u.clone())?;
    if ledger.charged_bytes()!=12<<20 || ledger.retained_bytes()!=12<<20 || ledger.reserved_bytes()!=64<<20 ||
        ledger.retained_reserved_bytes()!=64<<20{return Err("full76 must survive inheritance".into());}
    Ok(ledger)
}
fn apply(st:&mut Catalog,p:&Value,dir:&Path,authority:&Path)->Result<(),String>{
    // An existing/partial successor rejects even same-proof re-entry/restart.
    predecessors(dir,p)?;let ledger=checked_resources(st,p)?;
    let next=dir.join(NEXT);fs::DirBuilder::new().mode(0o700).create(&next).map_err(|e|e.to_string())?;
    File::open(dir).and_then(|f|f.sync_all()).map_err(|e|e.to_string())?;
    let mut record=json!({"schema":1,"phase":"ClosedC033Quarantine76Pending","authority":authority,
        "assessment_sha256":SHA,"predecessor_state_sha256":p["predecessor_state_sha256"],"verified_proof":p,
        "proof_sha256":p["proof_sha256"],"identity":p["identity"],"historical_pool":p["historical_pool"],
        "historical_unknown_operation":p["historical_unknown_operation"],"retained_canaries":p["retained_canaries"],
        "quarantined_creation":p["uncertain_business_reservation"],"cancel64_repeated":false,
        "SDK_ACK_Unmapped_or_canary_close_claimed":false,"resource_ledger":ledger.snapshot()});
    atomic_record(&next,"intent.json",&record)?;
    st.ddr_resources=ledger;st.managed_pools.reconciliation=record.clone();
    st.next_object_id=st.next_object_id.max(108);st.next_placement_id=st.next_placement_id.max(10002);
    st.next_resource_binding_id=st.next_resource_binding_id.max(40002);
    #[cfg(test)]
    if p["CPU_crash_after_intent"]==true{return Err("CPU crash after durable conservative76 intent".into());}
    record["phase"]=json!("ClosedC033Quarantine76");atomic_record(&next,"result.json",&record)?;
    st.managed_pools.reconciliation=record;Ok(())
}
pub(crate) fn restore(st:&mut Catalog,c:&Value,authority:&Path)->Result<File,String>{
    let dir=Path::new(ROOT).join("implementation/responsibility-c013");let lock=lock_lineage(&dir)?;
    names(&dir,&["authority.lock","intent.json","result.json","successor-c027-v1"])?;
    let assessment=Path::new(c["quarantine76_assessment"].as_str().ok_or("fixed quarantine assessment")?);
    if !assessment.is_absolute() || !assessment.starts_with(Path::new(ROOT).join("implementation")){return Err("controlled assessment path".into());}
    let code=format!("R8_SOURCE={}\n{}",serde_json::to_string(R8).map_err(|e|e.to_string())?,VERIFY);
    let result=Command::new("/usr/local/python3.11.14/bin/python3.11").args(["-I","-S","-c",&code]).arg(assessment).arg(authority).output().map_err(|e|e.to_string())?;
    if !result.status.success() || result.stdout.len()>524288{return Err(format!("fixed closed-c033 proof rejected; all76 retained, no bootstrap: {}",String::from_utf8_lossy(&result.stderr).chars().take(1600).collect::<String>()));}
    let p:Value=serde_json::from_slice(&result.stdout).map_err(|e|e.to_string())?;apply(st,&p,&dir,authority)?;Ok(lock)
}

#[cfg(test)]
pub(crate) fn fixture_proof()->Value {
    serde_json::from_str(include_str!("../../../examples/mem_service_pool/reconciliation/quarantine76-cpu-proof.json")).unwrap()
}
#[cfg(test)]
pub(crate) fn restore_fixture(st:&mut Catalog,p:&Value,dir:&Path,authority:&Path)->Result<File,String>{
    // This function only exists in the Rust test executable, never the daemon.
    fs::create_dir(dir).map_err(|e|e.to_string())?;File::create(dir.join("authority.lock")).map_err(|e|e.to_string())?;
    fs::create_dir(dir.join("successor-c027-v1")).map_err(|e|e.to_string())?;
    for rel in ["intent.json","result.json","successor-c027-v1/intent.json","successor-c027-v1/result.json"] {
        fs::write(dir.join(rel),serde_json::to_vec(&p["predecessors"][format!("implementation/responsibility-c013/{rel}")]).unwrap()).map_err(|e|e.to_string())?;
    }
    let lock=lock_lineage(dir)?;apply(st,p,dir,authority)?;Ok(lock)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64,Ordering};
    static N:AtomicU64=AtomicU64::new(0);
    struct Scratch(std::path::PathBuf);
    impl Scratch{fn new()->Self{let p=std::env::temp_dir().join(format!("uf-q76-{}-{}",std::process::id(),N.fetch_add(1,Ordering::Relaxed)));fs::create_dir(&p).unwrap();Self(p)}}
    impl Drop for Scratch{fn drop(&mut self){assert_eq!(self.0.parent().unwrap(),std::env::temp_dir());fs::remove_dir_all(&self.0).unwrap();}}
    fn catalog()->Catalog{let mut st=Catalog::new(-1,vec![]);st.ddr_resources.limit=128<<20;st}
    #[test]
    fn staged_quarantine76_fixture(){
        if let Ok(config)=std::env::var("UFLOW_RECON_CATALOG_CPU_CONFIG") {
            let c:Value=serde_json::from_slice(&fs::read(config).unwrap()).unwrap();
            crate::pool_service::run_reconciliation_fixture(c["endpoint"].as_str().unwrap(),Path::new(c["state"].as_str().unwrap()),Path::new(c["provider_config"].as_str().unwrap()),&fixture_proof()).unwrap();
        } else {let d=Scratch::new();let mut st=catalog();let _l=restore_fixture(&mut st,&fixture_proof(),&d.0.join("lineage"),&d.0).unwrap();assert_eq!(st.ddr_resources.reserved_bytes(),64<<20);}
    }
    #[test]
    fn full76_is_typed_and_ordinary_cancel_commit_or_release_cannot_refund(){
        let d=Scratch::new();let mut st=catalog();let p=fixture_proof();let _l=restore_fixture(&mut st,&p,&d.0.join("lineage"),&d.0).unwrap();
        let key=p["uncertain_business_reservation"]["key"].as_str().unwrap();
        st.ddr_resources.cancel_uncreated(key);assert!(st.ddr_resources.reserve(key,64<<20).is_err());
        assert!(st.ddr_resources.commit(key,107,0,0,64<<20).is_err());
        for id in 100..=107{st.ddr_resources.release_confirmed(id);}
        st.ddr_resources.reserve("unrelated",65536).unwrap();assert!(st.ddr_resources.commit("unrelated",107,0,0,65536).is_err());st.ddr_resources.cancel_uncreated("unrelated");
        assert_eq!(st.ddr_resources.charged_bytes(),12<<20);assert_eq!(st.ddr_resources.retained_bytes(),12<<20);
        assert_eq!(st.ddr_resources.reserved_bytes(),64<<20);assert_eq!(st.ddr_resources.retained_reserved_bytes(),64<<20);
        assert_eq!(st.next_object_id,108);assert_eq!(st.next_placement_id,10002);assert_eq!(st.next_resource_binding_id,40002);
        assert_eq!(st.managed_pools.reconciliation["quarantined_creation"]["pool"]["state"],"CreationUnknown");
    }
    #[test]
    fn baseline76_new4_peak128_normal76_and_negative64(){
        let d=Scratch::new();let mut st=catalog();let _l=restore_fixture(&mut st,&fixture_proof(),&d.0.join("lineage"),&d.0).unwrap();
        st.ddr_resources.reserve("new-canaries",4<<20).unwrap();assert!(st.ddr_resources.reserve("too-big",64<<20).is_err());
        st.ddr_resources.commit("new-canaries",108,2,3,4<<20).unwrap();st.ddr_resources.reserve("business",48<<20).unwrap();
        assert_eq!(st.ddr_resources.charged_bytes()+st.ddr_resources.reserved_bytes(),128<<20);assert!(st.ddr_resources.reserve("overflow",1).is_err());
        st.ddr_resources.commit("business",110,2,4,48<<20).unwrap();st.ddr_resources.release_confirmed(110);st.ddr_resources.release_confirmed(108);
        assert_eq!(st.ddr_resources.charged_bytes()+st.ddr_resources.reserved_bytes(),76<<20);
    }
    #[test]
    fn wrong_old_reservation_pool_or_canary_witness_never_persists(){
        let p=fixture_proof();
        for (key,v) in [("key",json!("wrong")),("nbytes",json!(1)),("state",json!("Released"))] {
            let mut bad=p.clone();bad["uncertain_business_reservation"][key]=v;
            assert!(checked_resources(&catalog(),&bad).is_err());
        }
        let mut bad=p.clone();bad["retained_canaries"][5]["SDK_checked_close_confirmed"]=json!(true);assert!(checked_resources(&catalog(),&bad).is_err());
        let mut bad=p;bad["uncertain_business_reservation"]["pool"]["canonical"]=json!({});assert!(checked_resources(&catalog(),&bad).is_err());
    }
    #[test]
    fn original_lock_content_and_complete_predecessor_survive_no_reentry(){
        let d=Scratch::new();let dir=d.0.join("lineage");let mut st=catalog();let p=fixture_proof();let _l=restore_fixture(&mut st,&p,&dir,&d.0).unwrap();
        assert!(lock_lineage(&dir).is_err());let old=fs::read(dir.join("result.json")).unwrap();
        assert!(apply(&mut st,&p,&dir,&d.0).is_err());assert_eq!(old,fs::read(dir.join("result.json")).unwrap());
        assert!(apply(&mut catalog(),&p,&dir,&d.0).is_err());
    }
    #[test]
    fn crash_after_intent_retains_full76_and_restart_is_refused(){
        let d=Scratch::new();let dir=d.0.join("lineage");let mut p=fixture_proof();p["CPU_crash_after_intent"]=json!(true);let mut st=catalog();
        assert!(restore_fixture(&mut st,&p,&dir,&d.0).is_err());assert!(dir.join(NEXT).join("intent.json").exists());assert!(!dir.join(NEXT).join("result.json").exists());
        assert_eq!(st.ddr_resources.charged_bytes()+st.ddr_resources.reserved_bytes(),76<<20);assert!(apply(&mut catalog(),&fixture_proof(),&dir,&d.0).is_err());
    }
    #[test]
    fn low_budget_nonempty_and_overflow_cannot_accept_import(){
        let p=fixture_proof();let mut st=catalog();st.ddr_resources.limit=75<<20;assert!(checked_resources(&st,&p).is_err());
        let mut st=catalog();st.ddr_resources.reserve("existing",1).unwrap();assert!(checked_resources(&st,&p).is_err());
        let mut bad=p;bad["charged_bytes"]=json!(u64::MAX);assert!(checked_resources(&catalog(),&bad).is_err());
    }
}
