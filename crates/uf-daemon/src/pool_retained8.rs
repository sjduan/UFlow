//! One append-only succession of the already completed c013 -> failed c027
//! authority. All eight MiB are inherited; no rollback/close is inferred.
use crate::{catalog::Catalog,ddr_resources::{BackingId,DdrResources}};
use serde_json::{json,Value};
use std::fs::{self,File,OpenOptions};
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt,OpenOptionsExt};
use std::path::{Path,PathBuf};
use std::process::Command;

const ROOT:&str="/workspace/uflow/runs/mem-service-shared-pool-20261108";
const ASSESSMENT:&str="b283ec290bb2487d3e81380cc5dc8cd0b5fd9db34b76eb0488171f8066600151";
const EXPECTED:&str=include_str!("../../../examples/mem_service_pool/reconciliation/retained8-assessment.json");
const OLD:&str=include_str!("../../../tools/phasec/verify_original_pool.py");
const VERIFY:&str=include_str!("../../../tools/phasec/verify_retained8_pool.py");
const SUCCESSOR:&str="successor-c027-v1";

pub(crate) fn atomic_record(dir:&Path,name:&str,value:&Value)->Result<(),String>{
    let next=dir.join(format!("{name}.next"));
    let mut f=OpenOptions::new().write(true).create_new(true).mode(0o600).custom_flags(libc::O_NOFOLLOW)
        .open(&next).map_err(|e|e.to_string())?;
    serde_json::to_writer(&mut f,value).map_err(|e|e.to_string())?;
    f.write_all(b"\n").and_then(|_|f.sync_all()).map_err(|e|e.to_string())?;
    // link is atomic and refuses replacement. A crash before unlink leaves a
    // visible .next and blocks restart; neither old nor new record is replaced.
    fs::hard_link(&next,dir.join(name)).map_err(|e|e.to_string())?;
    fs::remove_file(next).map_err(|e|e.to_string())?;
    File::open(dir).and_then(|f|f.sync_all()).map_err(|e|e.to_string())
}
pub(crate) fn lock_lineage(dir:&Path)->Result<File,String>{
    if dir.symlink_metadata().map_err(|e|e.to_string())?.file_type().is_symlink(){return Err("lineage symlink".into());}
    let lock=OpenOptions::new().read(true).write(true).custom_flags(libc::O_NOFOLLOW)
        .open(dir.join("authority.lock")).map_err(|e|e.to_string())?;
    if !lock.metadata().map_err(|e|e.to_string())?.is_file() ||
        unsafe{libc::flock(lock.as_raw_fd(),libc::LOCK_EX|libc::LOCK_NB)}!=0 {return Err("original lineage authority busy".into());}
    Ok(lock)
}
fn predecessor_only(dir:&Path)->Result<(),String>{
    let names=fs::read_dir(dir).map_err(|e|e.to_string())?.map(|e|e.map(|e|e.file_name().to_string_lossy().into_owned()))
        .collect::<Result<std::collections::BTreeSet<_>,_>>().map_err(|e|e.to_string())?;
    if names!=["authority.lock","intent.json","result.json"].into_iter().map(String::from).collect(){
        return Err("complete original predecessor only; existing successor/partial lineage blocks restart".into());
    }
    for n in ["intent.json","result.json"] {
        let m=dir.join(n).symlink_metadata().map_err(|e|e.to_string())?;
        if !m.is_file() || m.file_type().is_symlink(){return Err("regular predecessor records only".into());}
    }
    Ok(())
}
fn check_proof(st:&Catalog,p:&Value)->Result<DdrResources,String>{
    let expected:Value=serde_json::from_str(EXPECTED).map_err(|e|e.to_string())?;
    if !st.managed_pools.reconciliation.is_null() || !st.objects.is_empty() || !st.resource_bindings.is_empty() ||
        !st.managed_pools.pools.is_empty() || !st.managed_pools.operations.is_empty() ||
        st.ddr_resources.charged_bytes()!=0 || st.ddr_resources.reserved_bytes()!=0 ||
        p["schema"]!=1 || p["kind"]!="verified-c027-retained8-succession" || p["assessment_sha256"]!=ASSESSMENT ||
        p["original_proof_sha256"]!=expected["original_c013_proof_sha256"] || p["before_total_bytes"]!=8<<20 ||
        p["charged_bytes"]!=8<<20 || p["reserved_bytes"]!=0 || p["cancel64_repeated"]!=false ||
        p["SDK_ACK_Unmapped_or_canary_close_claimed"]!=false || p["retained_canaries"]!=expected["retained_objects"] ||
        p["predecessor_state_sha256"]!=expected["predecessor_state_sha256"] ||
        p["predecessor_result"]["phase"]!="VerifiedUncreatedTerminal" ||
        p["predecessor_result"]["cancelled_uncreated_bytes"]!=64<<20 ||
        p["identity"]!=expected["terminal_historical_business_identity"] {
        return Err("only exact verified c027 conservative eight MiB succession into empty Catalog".into());
    }
    let mut resources=DdrResources::default();resources.limit=st.ddr_resources.limit;
    for c in p["retained_canaries"].as_array().ok_or("four typed canaries")? {
        let b=&c["backing"];let id=c["object_id"].as_u64().ok_or("object id")?;
        let identity=BackingId::Provider{realm:b["realm"].as_str().ok_or("realm")?.into(),
            provider:b["provider"].as_str().ok_or("provider")?.into(),resource:b["resource"].as_str().ok_or("resource")?.into(),
            generation:b["generation"].as_u64().ok_or("generation")?};
        let operation=format!("inherited-c027:{id}");resources.reserve_unique(&operation,2<<20)?;
        resources.commit_backing(&operation,id,identity,2<<20)?;resources.retain_provider(id,c.clone())?;
    }
    if resources.charged_bytes()!=8<<20 || resources.retained_bytes()!=8<<20 || resources.reserved_bytes()!=0 {
        return Err("complete conservative eight required".into());
    }
    Ok(resources)
}
fn apply(st:&mut Catalog,p:&Value,lineage:&Path,authority:&Path)->Result<(),String>{
    let next=lineage.join(SUCCESSOR);
    for (name,key) in [("intent.json","predecessor_intent"),("result.json","predecessor_result")] {
        let path=lineage.join(name);let m=path.symlink_metadata().map_err(|e|e.to_string())?;
        if !m.is_file() || m.file_type().is_symlink(){return Err("regular predecessor required".into());}
        let actual:Value=serde_json::from_slice(&fs::read(path).map_err(|e|e.to_string())?).map_err(|e|e.to_string())?;
        if actual!=p[key]{return Err("actual predecessor document mismatch".into());}
    }
    if st.managed_pools.reconciliation["successor_phase"]=="Retained8" &&
        st.managed_pools.reconciliation["verified_proof"]==*p {
        if next.symlink_metadata().map_err(|e|e.to_string())?.file_type().is_symlink(){return Err("successor symlink".into());}
        let mut names=fs::read_dir(&next).map_err(|e|e.to_string())?.map(|p|p.map(|p|p.file_name().to_string_lossy().into_owned()))
            .collect::<Result<Vec<_>,_>>().map_err(|e|e.to_string())?;names.sort();
        if names!=["intent.json","result.json"]{return Err("successor partial/extra record".into());}
        let durable:Value=serde_json::from_slice(&fs::read(next.join("result.json")).map_err(|e|e.to_string())?).map_err(|e|e.to_string())?;
        let intent:Value=serde_json::from_slice(&fs::read(next.join("intent.json")).map_err(|e|e.to_string())?).map_err(|e|e.to_string())?;
        let mut expected=st.managed_pools.reconciliation.clone();expected["successor_phase"]=json!("Retained8Pending");
        if intent!=expected{return Err("successor intent drift".into());}
        return if durable==st.managed_pools.reconciliation{Ok(())}else{Err("successor result drift".into())};
    }
    predecessor_only(lineage)?;
    let resources=check_proof(st,p)?;
    let predecessor:Value=serde_json::from_slice(&fs::read(lineage.join("result.json")).map_err(|e|e.to_string())?).map_err(|e|e.to_string())?;
    if predecessor!=p["predecessor_result"]{return Err("actual predecessor mismatch".into());}
    fs::DirBuilder::new().mode(0o700).create(&next).map_err(|e|e.to_string())?;
    File::open(lineage).and_then(|f|f.sync_all()).map_err(|e|e.to_string())?;
    let mut record=json!({"schema":1,"phase":"VerifiedUncreatedTerminal","successor_phase":"Retained8Pending",
        "authority":authority,"predecessor_state_root":format!("{ROOT}/runs/c027/state"),
        "predecessor_state_sha256":p["predecessor_state_sha256"],"assessment_sha256":ASSESSMENT,
        "identity":p["identity"],"historical_pool":p["historical_pool"],"historical_unknown_operation":p["historical_unknown_operation"],
        "proof_sha256":p["proof_sha256"],"verified_proof":p,"retained_canaries":p["retained_canaries"],
        "cancel64_repeated":false,"SDK_ACK_Unmapped_or_canary_close_claimed":false,"resource_ledger":resources.snapshot()});
    atomic_record(&next,"intent.json",&record)?;
    st.ddr_resources=resources;st.managed_pools.reconciliation=record.clone();
    st.next_object_id=st.next_object_id.max(105);st.next_placement_id=st.next_placement_id.max(10001);
    st.next_resource_binding_id=st.next_resource_binding_id.max(40001);
    #[cfg(test)]
    if p["proof_sha256"]=="CPU-retained8-fixture-not-production" && p["CPU_crash_after_intent"]==true {
        return Err("CPU simulated crash after durable conservative8 intent".into());
    }
    record["successor_phase"]=json!("Retained8");atomic_record(&next,"result.json",&record)?;
    st.managed_pools.reconciliation=record;Ok(())
}
pub(crate) fn restore(st:&mut Catalog,c:&Value,authority:&Path)->Result<File,String>{
    let lineage=Path::new(ROOT).join("implementation/responsibility-c013");
    let lock=lock_lineage(&lineage)?;predecessor_only(&lineage)?;
    let paths=["assessment","bindings","retained8_assessment"].map(|n|c[n].as_str().map(PathBuf::from).ok_or(format!("missing {n}")));
    let paths=paths.into_iter().collect::<Result<Vec<_>,_>>()?;
    for p in &paths {if !p.is_absolute() || !p.starts_with(Path::new(ROOT).join("implementation")){return Err("controlled task assessment paths".into());}}
    let code=format!("ORIGINAL_SOURCE = {}\n{}",serde_json::to_string(OLD).map_err(|e|e.to_string())?,VERIFY);
    let result=Command::new("/usr/local/python3.11.14/bin/python3.11").args(["-I","-S","-c",&code]).args(paths).arg(authority)
        .output().map_err(|e|e.to_string())?;
    if !result.status.success() || result.stdout.len()>262144 {return Err(format!("retained8 verifier refused; keep predecessor8, no bootstrap: {}",String::from_utf8_lossy(&result.stderr).chars().take(1600).collect::<String>()));}
    let p:Value=serde_json::from_slice(&result.stdout).map_err(|e|e.to_string())?;
    apply(st,&p,&lineage,authority)?;Ok(lock)
}

#[cfg(test)]
pub(crate) fn fixture_proof()->Value {
    let a:Value=serde_json::from_str(EXPECTED).unwrap();
    json!({"schema":1,"kind":"verified-c027-retained8-succession","assessment_sha256":ASSESSMENT,
        "proof_sha256":"CPU-retained8-fixture-not-production","original_proof_sha256":a["original_c013_proof_sha256"],
        "predecessor_state_sha256":a["predecessor_state_sha256"],"before_total_bytes":8<<20,"charged_bytes":8<<20,"reserved_bytes":0,
        "cancel64_repeated":false,"SDK_ACK_Unmapped_or_canary_close_claimed":false,"retained_canaries":a["retained_objects"],
        "identity":a["terminal_historical_business_identity"],"historical_pool":{"state":"CreationUnknown"},
        "historical_unknown_operation":{"state":"Unknown","response":{"code":"Unknown"}},
        "predecessor_intent":{"phase":"CPU predecessor fixture"},
        "predecessor_result":{"phase":"VerifiedUncreatedTerminal","cancelled_uncreated_bytes":64<<20}})
}
#[cfg(test)]
pub(crate) fn restore_fixture(st:&mut Catalog,p:&Value,dir:&Path,authority:&Path)->Result<File,String>{
    if p["proof_sha256"]!="CPU-retained8-fixture-not-production" ||
        ["implementation/responsibility-c013","runs/c013","runs/c027"].iter().any(|n|dir.starts_with(Path::new(ROOT).join(n))){
        return Err("isolated CPU fixture required".into());
    }
    // The admitted build scratch is inside ROOT, so the caller supplies its new
    // per-fixture lineage, never the literal production responsibility path.
    fs::create_dir(dir).map_err(|e|e.to_string())?;
    File::create(dir.join("authority.lock")).map_err(|e|e.to_string())?;
    fs::write(dir.join("intent.json"),serde_json::to_vec(&p["predecessor_intent"]).unwrap()).map_err(|e|e.to_string())?;
    fs::write(dir.join("result.json"),serde_json::to_vec(&p["predecessor_result"]).unwrap()).map_err(|e|e.to_string())?;
    let lock=lock_lineage(dir)?;apply(st,p,dir,authority)?;Ok(lock)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64,Ordering};
    static NEXT:AtomicU64=AtomicU64::new(0);
    struct Scratch(PathBuf);
    impl Scratch {fn new()->Self {
        let p=std::env::temp_dir().join(format!("uf-r8-{}-{}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
        fs::create_dir(&p).unwrap();Self(p)
    }}
    impl Drop for Scratch {fn drop(&mut self){assert_eq!(self.0.parent().unwrap(),std::env::temp_dir());fs::remove_dir_all(&self.0).unwrap();}}
    fn catalog()->Catalog{let mut s=Catalog::new(-1,Vec::new());s.ddr_resources.limit=128<<20;s}
    fn predecessor(dir:&Path,p:&Value) {
        File::create(dir.join("authority.lock")).unwrap();fs::write(dir.join("intent.json"),serde_json::to_vec(&p["predecessor_intent"]).unwrap()).unwrap();
        fs::write(dir.join("result.json"),serde_json::to_vec(&p["predecessor_result"]).unwrap()).unwrap();
    }
    #[test]
    fn staged_retained8_fixture(){
        if let Ok(config)=std::env::var("UFLOW_RECON_CATALOG_CPU_CONFIG") {
            let c:Value=serde_json::from_slice(&fs::read(config).unwrap()).unwrap();
            crate::pool_service::run_reconciliation_fixture(c["endpoint"].as_str().unwrap(),Path::new(c["state"].as_str().unwrap()),
                Path::new(c["provider_config"].as_str().unwrap()),&fixture_proof()).unwrap();
        } else {
            let d=Scratch::new();let p=fixture_proof();let mut s=catalog();
            let _lock=restore_fixture(&mut s,&p,&d.0.join("fixture-lineage"),&d.0).unwrap();
            assert_eq!(s.ddr_resources.retained_bytes(),8<<20);assert_eq!(s.next_object_id,105);
        }
    }
    #[test]
    fn four_typed_retained_share_budget_and_cannot_be_released_or_cancelled(){
        let d=Scratch::new();let p=fixture_proof();predecessor(&d.0,&p);let _lock=lock_lineage(&d.0).unwrap();let mut s=catalog();
        let before=fs::read(d.0.join("result.json")).unwrap();apply(&mut s,&p,&d.0,&d.0).unwrap();
        assert_eq!(before,fs::read(d.0.join("result.json")).unwrap());
        assert_eq!(s.ddr_resources.snapshot()["resources"].as_array().unwrap().len(),4);
        assert_eq!(s.next_object_id,105);assert_eq!(s.next_placement_id,10001);assert_eq!(s.next_resource_binding_id,40001);
        for id in [100,101,103,104]{s.ddr_resources.release_confirmed(id);}
        s.ddr_resources.cancel_uncreated("c013:3196565:1314359845:4026531836:de21099a-5b02-4e8d-b1d7-3a6129ab9f22");
        assert_eq!(s.ddr_resources.charged_bytes(),8<<20);assert_eq!(s.ddr_resources.reserved_bytes(),0);
        for (id,n) in [(105,2<<20),(106,2<<20),(107,64<<20)] {
            let op=format!("new:{id}");s.ddr_resources.reserve_unique(&op,n).unwrap();
            s.ddr_resources.commit_backing(&op,id,BackingId::Provider{realm:"simulated:fresh".into(),provider:"mem_service_obmm".into(),resource:op.clone(),generation:1},n).unwrap();
        }
        assert_eq!(s.ddr_resources.charged_bytes(),76<<20);assert!(s.ddr_resources.reserve("overflow",53<<20).is_err());
        for id in [105,106,107]{s.ddr_resources.release_confirmed(id);}
        assert_eq!(s.ddr_resources.charged_bytes(),8<<20);assert_eq!(s.ddr_resources.retained_bytes(),8<<20);
        assert_eq!(s.managed_pools.reconciliation["cancel64_repeated"],false);
    }
    #[test]
    fn wrong_or_duplicate_identity_and_backing_or_receipt_never_persist(){
        let p=fixture_proof();let mut changes=Vec::new();
        for (key,v) in [("boot_id",json!("wrong")),("provider_session",json!("wrong")),("nbytes",json!(1)),
                ("SDK_checked_close_confirmed",json!(true)),("object_id",json!(100))] {
            let mut bad=p.clone();bad["retained_canaries"][2][key]=v;changes.push(bad);
        }
        let mut bad=p.clone();bad["retained_canaries"][2]["backing"]=bad["retained_canaries"][0]["backing"].clone();changes.push(bad);
        for (key,v) in [("canary_descriptor",json!("00")),("checksum",json!(1))]{
            let mut bad=p.clone();bad["retained_canaries"][2]["canary_prepare"][key]=v;changes.push(bad);
        }
        for (key,v) in [("assessment_sha256",json!("wrong")),("cancel64_repeated",json!(true)),("before_total_bytes",json!(4<<20)),
                ("SDK_ACK_Unmapped_or_canary_close_claimed",json!(true))]{let mut bad=p.clone();bad[key]=v;changes.push(bad);}
        for bad in changes {
            let d=Scratch::new();predecessor(&d.0,&p);let mut s=catalog();
            assert!(apply(&mut s,&bad,&d.0,&d.0).is_err());assert_eq!(s.ddr_resources.charged_bytes(),0);
            assert!(!d.0.join(SUCCESSOR).exists());
        }
    }
    #[test]
    fn same_authority_idempotent_but_drift_and_restart_fail_closed(){
        let d=Scratch::new();let p=fixture_proof();predecessor(&d.0,&p);let mut s=catalog();
        apply(&mut s,&p,&d.0,&d.0).unwrap();let before=fs::read(d.0.join(SUCCESSOR).join("result.json")).unwrap();
        apply(&mut s,&p,&d.0,&d.0).unwrap();assert_eq!(s.ddr_resources.retained_bytes(),8<<20);
        assert_eq!(before,fs::read(d.0.join(SUCCESSOR).join("result.json")).unwrap());
        assert!(apply(&mut catalog(),&p,&d.0,&d.0).is_err());
        let mut bad=p.clone();bad["proof_sha256"]=json!("drift");assert!(apply(&mut s,&bad,&d.0,&d.0).is_err());
        fs::write(d.0.join(SUCCESSOR).join("result.json"),b"{}").unwrap();assert!(apply(&mut s,&p,&d.0,&d.0).is_err());
    }
    #[test]
    fn exclusive_original_lock_blocks_concurrent_or_live_authority(){
        let d=Scratch::new();let p=fixture_proof();predecessor(&d.0,&p);
        let lock=lock_lineage(&d.0).unwrap();assert!(lock_lineage(&d.0).is_err());drop(lock);assert!(lock_lineage(&d.0).is_ok());
    }
    #[test]
    fn missing_predecessor_partial_extra_or_existing_successor_reject(){
        let p=fixture_proof();
        for extra in ["unknown.json","intent.json.next",SUCCESSOR] {
            let d=Scratch::new();predecessor(&d.0,&p);fs::write(d.0.join(extra),b"partial").unwrap();
            assert!(apply(&mut catalog(),&p,&d.0,&d.0).is_err());
        }
        let d=Scratch::new();predecessor(&d.0,&p);fs::remove_file(d.0.join("intent.json")).unwrap();
        assert!(apply(&mut catalog(),&p,&d.0,&d.0).is_err());
    }
    #[test]
    fn durable_intent8_survives_simulated_crash_no_resume_or_overwrite(){
        let d=Scratch::new();let mut p=fixture_proof();p["CPU_crash_after_intent"]=json!(true);predecessor(&d.0,&p);let mut s=catalog();
        assert!(apply(&mut s,&p,&d.0,&d.0).is_err());assert_eq!(s.ddr_resources.retained_bytes(),8<<20);
        let n=d.0.join(SUCCESSOR);assert!(!n.join("result.json").exists());
        let intent:Value=serde_json::from_slice(&fs::read(n.join("intent.json")).unwrap()).unwrap();
        assert_eq!(intent["resource_ledger"]["charged_bytes"],8<<20);assert_eq!(intent["cancel64_repeated"],false);
        assert!(apply(&mut catalog(),&p,&d.0,&d.0).is_err());
        assert!(atomic_record(&n,"intent.json",&json!({"overwrite":true})).is_err());
        assert_eq!(intent,serde_json::from_slice::<Value>(&fs::read(n.join("intent.json")).unwrap()).unwrap());
    }
    #[test]
    fn low_budget_or_wrong_predecessor_never_creates_successor(){
        let d=Scratch::new();let p=fixture_proof();predecessor(&d.0,&p);let mut s=catalog();s.ddr_resources.limit=7<<20;
        assert!(apply(&mut s,&p,&d.0,&d.0).is_err());assert!(!d.0.join(SUCCESSOR).exists());
        fs::write(d.0.join("result.json"),b"{}").unwrap();assert!(apply(&mut catalog(),&p,&d.0,&d.0).is_err());
    }
}
