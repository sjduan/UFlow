//! One evidence-bound pre-dispatch reservation rollback, never generic recovery.
use crate::catalog::Catalog;
use crate::ddr_resources::BackingId;
use serde_json::{json,Value};
use std::fs::{self,File,OpenOptions};
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::process::Command;

const VERIFIER:&str=include_str!("../../../tools/phasec/verify_original_pool.py");
const ROOT:&str="/workspace/uflow/runs/mem-service-shared-pool-20261108";
const ASSESSMENT:&str="f4e89d644df46d78cf4b611ed51f10efc5bc0b99591281b95f5a26a65e57aba4";

fn record(root:&Path,name:&str,value:&Value) -> Result<(),String> {
    let next=root.join(format!("{name}.next"));
    let mut f=OpenOptions::new().write(true).create_new(true).mode(0o600).open(&next).map_err(|e|e.to_string())?;
    serde_json::to_writer(&mut f,value).map_err(|e|e.to_string())?;
    f.write_all(b"\n").and_then(|_|f.sync_all()).map_err(|e|e.to_string())?;
    fs::rename(next,root.join(name)).map_err(|e|e.to_string())?;
    File::open(root).and_then(|f|f.sync_all()).map_err(|e|e.to_string())
}

fn apply(st:&mut Catalog,proof:&Value,dir:&Path,authority:&Path) -> Result<(),String> {
    if st.managed_pools.reconciliation["verified_proof"]==*proof && !st.managed_pools.reconciliation.is_null() &&
        st.managed_pools.reconciliation["phase"]=="VerifiedUncreatedTerminal" {
        let durable:Value=serde_json::from_slice(&fs::read(dir.join("result.json")).map_err(|e|e.to_string())?).map_err(|e|e.to_string())?;
        return if durable==st.managed_pools.reconciliation {Ok(())} else {Err("reconciliation record drift".into())};
    }
    if dir.join("intent.json").exists() || dir.join("result.json").exists() {return Err("existing lineage cannot be replaced or resumed".into());}
    if !st.managed_pools.reconciliation.is_null() || st.ddr_resources.charged_bytes()!=0 || st.ddr_resources.reserved_bytes()!=0 ||
        proof["schema"]!=1 || proof["kind"]!="verified-c013-uncreated-reservation" || proof["assessment_sha256"]!=ASSESSMENT ||
        proof["before_charged_bytes"]!=4<<20 || proof["before_reserved_bytes"]!=64<<20 ||
        proof["SDK_ACK_Unmapped_or_canary_close_claimed"]!=false || proof["identity"]["resource_id"]!=102 ||
        proof["identity"]["operation_id"]!="de21099a-5b02-4e8d-b1d7-3a6129ab9f22" {
        return Err("only complete verified original c013 may import into an empty authority".into());
    }
    let canaries=proof["retained_canaries"].as_array().ok_or("retained canaries missing")?;
    if canaries.len()!=2 {return Err("both original canary obligations required".into());}
    for c in canaries {
        let id=c["object_id"].as_u64().ok_or("old object id missing")?;
        if !matches!(id,100|101) || c["nbytes"]!=2<<20 || c["state"]!="UnknownCanaryClose" || c["SDK_checked_close_confirmed"]!=false {
            return Err("old canary responsibility mismatch".into());
        }
        let b=&c["backing"];
        let identity=BackingId::Provider {realm:b["realm"].as_str().ok_or("realm")?.into(),
            provider:b["provider"].as_str().ok_or("provider")?.into(),resource:b["resource"].as_str().ok_or("resource")?.into(),
            generation:b["generation"].as_u64().ok_or("generation")?};
        let reservation=format!("retained-c013:{id}");
        st.ddr_resources.reserve_unique(&reservation,2<<20)?;
        st.ddr_resources.commit_backing(&reservation,id,identity,2<<20)?;
        st.ddr_resources.retain_provider(id,c.clone())?;
    }
    let reservation="c013:3196565:1314359845:4026531836:de21099a-5b02-4e8d-b1d7-3a6129ab9f22";
    st.ddr_resources.reserve_unique(reservation,64<<20)?;
    st.next_object_id=st.next_object_id.max(103);st.next_placement_id=st.next_placement_id.max(10001);
    st.next_resource_binding_id=st.next_resource_binding_id.max(40001);
    let mut history=json!({"schema":1,"phase":"Restored68PendingProofCommit","authority":authority,
        "proof_sha256":proof["proof_sha256"],"assessment_sha256":ASSESSMENT,"identity":proof["identity"],
        "historical_pool":proof["historical_pool"],"historical_unknown_operation":proof["historical_unknown_operation"],
        "verified_proof":proof,"retained_canaries":canaries,
        "SDK_ACK_Unmapped_or_canary_close_claimed":false,"resource_ledger":st.ddr_resources.snapshot()});
    // The full conservative 68MiB is durable before cancellation. Any failure or
    // restart leaves the exclusive lineage closed; no new bootstrap may begin.
    record(dir,"intent.json",&history)?;
    st.managed_pools.reconciliation=history.clone();
    st.ddr_resources.cancel_uncreated(reservation);
    history["phase"]=json!("VerifiedUncreatedTerminal");
    history["cancelled_uncreated_bytes"]=json!(64<<20);
    history["resource_ledger"]=st.ddr_resources.snapshot();
    record(dir,"result.json",&history)?;
    st.managed_pools.reconciliation=history;Ok(())
}

/// Called before reserving any new canary or listening for new work. The lock
/// stays in Service until its entire current runtime closes. Original files RO.
pub fn restore(st:&mut Catalog,config:&Value,authority:&Path) -> Result<File,String> {
    let dir=Path::new(ROOT).join("implementation/responsibility-c013");
    fs::create_dir_all(&dir).map_err(|e|e.to_string())?;
    let lock=OpenOptions::new().write(true).create(true).truncate(false).mode(0o600).open(dir.join("authority.lock")).map_err(|e|e.to_string())?;
    if unsafe {libc::flock(lock.as_raw_fd(),libc::LOCK_EX|libc::LOCK_NB)}!=0 {return Err("original responsibility authority busy".into());}
    if fs::read_dir(&dir).map_err(|e|e.to_string())?.any(|p|p.is_ok_and(|p|p.file_name()!="authority.lock")) {
        return Err("existing reconciliation lineage or partial write; no restart/replacement authority".into());
    }
    let assessment=config["assessment"].as_str().ok_or("assessment path")?;
    let bindings=config["bindings"].as_str().ok_or("binding path")?;
    for p in [assessment,bindings] {
        if !Path::new(p).is_absolute() || !Path::new(p).starts_with(Path::new(ROOT).join("implementation")) {
            return Err("task-local controlled assessment/bindings only".into());
        }
    }
    let result=Command::new("/usr/local/python3.11.14/bin/python3.11").args(["-I","-S","-c",VERIFIER,assessment,bindings])
        .output().map_err(|e|e.to_string())?;
    if !result.status.success() || result.stdout.len()>262144 {
        let diagnostic=String::from_utf8_lossy(&result.stderr);
        return Err(format!("fresh verifier rejected; original68MiB retained, no bootstrap: {}",&diagnostic[diagnostic.char_indices().rev().nth(1200).map(|(i,_)|i).unwrap_or(0)..]));
    }
    let proof:Value=serde_json::from_slice(&result.stdout).map_err(|e|e.to_string())?;
    apply(st,&proof,&dir,authority)?;Ok(lock)
}

#[cfg(test)]
pub(crate) fn restore_fixture(st:&mut Catalog,proof:&Value,dir:&Path,authority:&Path) -> Result<File,String> {
    if proof["proof_sha256"]!="CPU-unit-fixture-not-a-production-proof" {return Err("explicit CPU fixture marker required".into());}
    fs::create_dir(dir).map_err(|e|e.to_string())?;
    let lock=OpenOptions::new().write(true).create_new(true).mode(0o600).open(dir.join("authority.lock")).map_err(|e|e.to_string())?;
    if unsafe{libc::flock(lock.as_raw_fd(),libc::LOCK_EX|libc::LOCK_NB)}!=0{return Err("fixture lock".into());}
    apply(st,proof,dir,authority)?;Ok(lock)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64,Ordering};
    static NEXT:AtomicU64=AtomicU64::new(0);
    struct Scratch(std::path::PathBuf);
    impl Scratch {fn new()->Self {
        let p=std::env::temp_dir().join(format!("uf-rec-{}-{}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
        fs::create_dir(&p).unwrap();Self(p)
    }}
    impl Drop for Scratch {fn drop(&mut self){assert_eq!(self.0.parent().unwrap(),std::env::temp_dir());fs::remove_dir_all(&self.0).unwrap();}}
    fn fixture()->Value {
        let a:Value=serde_json::from_str(include_str!("../../../examples/mem_service_pool/reconciliation/assessment.json")).unwrap();
        json!({"schema":1,"kind":"verified-c013-uncreated-reservation","assessment_sha256":ASSESSMENT,
            "before_charged_bytes":4<<20,"before_reserved_bytes":64<<20,"identity":a["original_identity"],
            "retained_canaries":a["retained_canary_objects"],"historical_pool":{"state":"CreationUnknown"},
            "historical_unknown_operation":{"state":"Unknown"},"SDK_ACK_Unmapped_or_canary_close_claimed":false,
            "proof_sha256":"CPU-unit-fixture-not-a-production-proof","evidence_sha256":{}})
    }
    fn catalog()->Catalog {let mut st=Catalog::new(-1,Vec::new());st.ddr_resources.limit=128<<20;st}
    #[test]
    fn staged_catalog_fixture() {
        if let Ok(config)=std::env::var("UFLOW_RECON_CATALOG_CPU_CONFIG") {
            let c:Value=serde_json::from_slice(&fs::read(config).unwrap()).unwrap();
            crate::pool_service::run_reconciliation_fixture(c["endpoint"].as_str().unwrap(),Path::new(c["state"].as_str().unwrap()),
                Path::new(c["provider_config"].as_str().unwrap()),&fixture()).unwrap();
        } else {
            let dir=Scratch::new();let mut st=catalog();let p=fixture();
            let _lock=restore_fixture(&mut st,&p,&dir.0.join("fixture"),&dir.0).unwrap();
            assert_eq!(st.ddr_resources.retained_bytes(),4<<20);assert_eq!(st.next_resource_binding_id,40001);
        }
    }
    #[test]
    fn only_uncreated64_cancelled_retained_typed4_shares_capacity_and_survives_close() {
        let dir=Scratch::new();let mut st=catalog();let p=fixture();apply(&mut st,&p,&dir.0,&dir.0).unwrap();
        assert_eq!(st.ddr_resources.charged_bytes(),4<<20);assert_eq!(st.ddr_resources.retained_bytes(),4<<20);
        assert_eq!(st.ddr_resources.reserved_bytes(),0);assert!(st.take_next_object_id()>102);
        for (id,n) in [(104,2<<20),(105,2<<20),(106,64<<20)] {
            let op=format!("current:{id}");st.ddr_resources.reserve(&op,n).unwrap();
            st.ddr_resources.commit_backing(&op,id,BackingId::Provider{realm:"simulated:current".into(),provider:"mem_service_obmm".into(),
                resource:op.clone(),generation:1},n).unwrap();
        }
        assert_eq!(st.ddr_resources.charged_bytes(),72<<20);assert!(st.ddr_resources.reserve("over",57<<20).is_err());
        for id in [100,101,104,105,106] {st.ddr_resources.release_confirmed(id);}
        assert_eq!(st.ddr_resources.charged_bytes(),4<<20);
        let state=st.managed_pools.snapshot(&st);
        assert_eq!(state["active_bytes"],0);assert_eq!(state["retained_bytes"],4<<20);
        assert_eq!(state["resource_ledger"]["resources"].as_array().unwrap().len(),2);
        assert_eq!(state["reconciliation"]["identity"],p["identity"]);
        assert!(state["reconciliation"]["verified_proof"].is_null());
    }
    #[test]
    fn same_proof_idempotent_drift_and_restart_forbidden() {
        let dir=Scratch::new();let mut st=catalog();let p=fixture();apply(&mut st,&p,&dir.0,&dir.0).unwrap();
        let before=fs::read(dir.0.join("result.json")).unwrap();apply(&mut st,&p,&dir.0,&dir.0).unwrap();
        assert_eq!(before,fs::read(dir.0.join("result.json")).unwrap());assert_eq!(st.ddr_resources.charged_bytes(),4<<20);
        let mut bad=p.clone();bad["reason"]=json!("sameID different proof");assert!(apply(&mut st,&bad,&dir.0,&dir.0).is_err());
        assert!(apply(&mut catalog(),&p,&dir.0,&dir.0).is_err());
        fs::write(dir.0.join("result.json"),b"{}").unwrap();assert!(apply(&mut st,&p,&dir.0,&dir.0).is_err());
    }
    #[test]
    fn partial_intent_or_result_never_admits_and_keeps_durable_conservative_record() {
        for name in ["intent.json.next","result.json.next"] {
            let dir=Scratch::new();fs::write(dir.0.join(name),b"partial").unwrap();let mut st=catalog();let p=fixture();
            assert!(apply(&mut st,&p,&dir.0,&dir.0).is_err());assert!(!dir.0.join("result.json").exists());
            if name=="intent.json.next" {assert_eq!(st.ddr_resources.reserved_bytes(),64<<20);}
            else {
                let intent:Value=serde_json::from_slice(&fs::read(dir.0.join("intent.json")).unwrap()).unwrap();
                assert_eq!(intent["resource_ledger"]["reserved_bytes"],64<<20);
                assert_eq!(intent["resource_ledger"]["charged_bytes"],4<<20);
                assert!(apply(&mut st,&p,&dir.0,&dir.0).is_err());
            }
        }
    }
}
