use super::*;
use crate::ddr_resources::BackingId;
use std::{process::Command,sync::atomic::{AtomicU64,Ordering}};
static N:AtomicU64=AtomicU64::new(0);
struct Fixture{root:PathBuf,dir:PathBuf,state:PathBuf,proof:Value,pins:BTreeMap<String,String>}
impl Fixture {
    fn new()->Self{
        let root=std::env::temp_dir().join(format!("uf-td-{}-{}",std::process::id(),N.fetch_add(1,Ordering::Relaxed)));
        fs::create_dir(&root).unwrap();let dir=root.join("lineage");fs::create_dir(&dir).unwrap();File::create(dir.join("authority.lock")).unwrap();
        let source=Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/mem_service_pool/terminal_domain/lineage");
        let predecessors:Value=serde_json::from_str(PINS).unwrap();
        for (name,_) in predecessors.as_object().unwrap(){let dest=dir.join(name);fs::create_dir_all(dest.parent().unwrap()).unwrap();fs::copy(source.join(name),dest).unwrap();}
        let state=root.join("original-state.json");fs::write(&state,STATE).unwrap();let external=parsed(SEMANTICS).unwrap();
        let mut proof=json!({"CPU_fixture_only_not_real_platform_proof":true,"interface":"mem-service-private-qemu-domain-v1",
            "status":"PASS_HOST_CANDIDATE_ONLY","state_sha256":STATE_SHA,"external_semantics_sha256":SEMANTICS_SHA,
            "scope":external["candidate_scope"],"source_runtime":external["fixed_source_and_runtime"],
            "required_output_truths":external["required_output_truths"],"missing_evidence":[],
            "closure":{"owners_gone":15,"original_carriers":50,"registered_dynamic_metadata":1,"FD_holders":0,"VMA_holders":0,"denied":0,
                "actual_endpoints_inactive":["u.sock","l.sock","a.sock","b.sock","nodeA.qmp","nodeA.serial","nodeB.qmp","nodeB.serial"],
                "hardware_or_external_backing":false,"migration_snapshot_or_shared_RAM":false,"all_owners_and_visibility_covered":true}});
        let mut pins=BTreeMap::new();
        // Hash real synthetic fixture files, never fabricate accepted E pins.
        // Production cannot enter this constructor or validate these fixtures.
        for key in ["producer_manifest_sha256","validator_sha256","source_closure_sha256","runtime_closure_sha256","metadata_union_sha256","physical_closure_sha256"]{
            let data=format!("CPU SYNTHETIC EVIDENCE ONLY: {key}\n");fs::write(root.join(key),data.as_bytes()).unwrap();
            let pin=digest(data.as_bytes());proof[key]=json!(pin);pins.insert(key.into(),pin);
        }
        Self{root,dir,state,proof,pins}
    }
    fn verify(&self,bytes:&[u8])->Result<VerifiedCandidate,String>{
        if self.proof["CPU_fixture_only_not_real_platform_proof"]!=true{return Err("only explicit CPU fixture".into());}
        for (key,pin) in &self.pins {
            if self.proof[key]!=*pin || digest(&bounded(&self.root.join(key),4096)?)!=*pin{return Err(format!("fixture producer/evidence pin drift: {key}"));}
        }
        validate_candidate(bytes,&self.proof)
    }
    fn run(&self,crash:&str)->Result<TerminalAuthority,String>{transact(&self.dir,&self.state,|bytes|self.verify(bytes),crash)}
}
impl Drop for Fixture{fn drop(&mut self){assert_eq!(self.root.parent().unwrap(),std::env::temp_dir());fs::remove_dir_all(&self.root).unwrap();}}
fn catalog()->Catalog{let mut c=Catalog::new(-1,vec![]);c.ddr_resources.limit=128<<20;c}
fn new_context()->Value{json!({"realm":"simulated:future-terminal-cpu","session":"new-terminal-session",
    "nodes":[{"node":"nodeA","boot_id":"new-A-boot","agent_session":"new-A-agent","incarnation":41},
             {"node":"nodeB","boot_id":"new-B-boot","agent_session":"new-B-agent","incarnation":42}]})}

#[test]
fn full_original128_typed_terminal76_preserves_history_and_budget(){
    let f=Fixture::new();let original=parsed(STATE).unwrap();let mut authority=f.run("").unwrap();
    let path=f.dir.join(NEXT);assert_eq!(fs::read(&f.state).unwrap(),STATE);assert_eq!(fs::read(path.join("historical-state.json")).unwrap(),STATE);
    let before=parsed(&bounded(&path.join("before128.json"),1<<20).unwrap()).unwrap();let after=parsed(&bounded(&path.join("resulting76.json"),1<<20).unwrap()).unwrap();
    assert_eq!(before["charged_bytes"],64<<20);assert_eq!(before["reserved_bytes"],64<<20);
    assert_eq!(after["resource_ledger"]["charged_bytes"],12<<20);assert_eq!(after["resource_ledger"]["reserved_bytes"],64<<20);
    assert_eq!(after["resource_ledger"]["terminal_domain_loss_tombstones"].as_object().unwrap().len(),3);
    let preserved=rows(&after["resource_ledger"]).unwrap();let prior=rows(&original["resource_ledger"]).unwrap();assert_eq!(preserved.len(),6);
    for (id,row) in &preserved{assert_eq!(row,&prior[id]);}
    for key in ["reservations","retained_uncertain_reservations","retired_unstarted_object_ids"]{assert_eq!(after["resource_ledger"][key],original["resource_ledger"][key]);}
    assert_eq!(authority.historical().unwrap(),original);assert_eq!(authority.historical().unwrap()["pools"][0]["clients"].as_array().unwrap().len(),3);
    assert_eq!(authority.historical().unwrap()["pools"][0]["ranges"]["leases"][0]["state"],"Unknown");
    assert_eq!(authority.closure_report().unwrap()["status"],"AdministrativelyClosedTerminalDomainDataLost");
    assert_eq!(after["normal_SDK_ACK"],false);assert_eq!(after["new_admission_enabled"],false);
    let mut st=catalog();authority.attach(&mut st,&new_context()).unwrap();assert!(authority.attach(&mut catalog(),&new_context()).is_err());
    assert_eq!((st.next_object_id,st.next_placement_id,st.next_resource_binding_id,st.next_access_binding_id,st.next_lease_id),(113,10003,40003,50003,1008));
    for (key,n) in [("future-A",2<<20),("future-B",2<<20),("future-business",48<<20)]{st.ddr_resources.reserve_unique(key,n).unwrap();}
    assert_eq!(st.ddr_resources.charged_bytes()+st.ddr_resources.reserved_bytes(),128<<20);assert!(st.ddr_resources.reserve_unique("extra",1).is_err());
    assert_eq!(st.managed_pools.snapshot(&st)["resource_ledger"]["terminal_domain_loss_tombstones"],after["resource_ledger"]["terminal_domain_loss_tombstones"]);
}

#[test]
fn terminal_backings_and_all_old_ids_never_recommit_or_normal_release(){
    let f=Fixture::new();let p=f.verify(STATE).unwrap();let mut ledger=DdrResources::inherit_terminal_source(&p).unwrap();ledger.apply_terminal_loss(&p).unwrap();
    for id in 100..=112{ledger.release_confirmed(id);}
    assert_eq!(ledger.charged_bytes(),12<<20);assert_eq!(ledger.reserved_bytes(),64<<20);
    let old=p.source_ledger()["reservations"].as_object().unwrap().keys().next().unwrap();ledger.cancel_uncreated(old);assert_eq!(ledger.reserved_bytes(),64<<20);
    assert!(ledger.apply_terminal_loss(&p).is_err());
    for id in [107,108,109,110,111,112]{ledger.reserve_unique("reuse",1).unwrap();assert!(ledger.commit("reuse",id,1,2,1).is_err());ledger.cancel_uncreated("reuse");}
    let row=rows(p.source_ledger()).unwrap()[&112].clone();let b=&row["backing"];
    ledger.reserve_unique("old-backing-new-id",48<<20).unwrap();assert!(ledger.commit_backing("old-backing-new-id",999,BackingId::Provider{
        realm:b["realm"].as_str().unwrap().into(),provider:"mem_service_obmm".into(),resource:b["resource"].as_str().unwrap().into(),generation:1},48<<20).is_err());
}

#[test]
fn every_persistent_boundary_crash_and_replay_refuses_admission(){
    for point in ["intent","historical128","before128","fences","terminal52","resulting76","commit"]{
        let f=Fixture::new();assert!(f.run(point).is_err());let path=f.dir.join(NEXT);
        assert!(path.join("intent.json").is_file());assert_eq!(path.join("commit.json").exists(),point=="commit");
        assert_eq!(fs::read(&f.state).unwrap(),STATE);assert!(f.run("").is_err());
        if path.join("historical-state.json").exists(){assert_eq!(fs::read(path.join("historical-state.json")).unwrap(),STATE);}
    }
    let f=Fixture::new();let authority=f.run("").unwrap();assert!(f.run("").is_err());drop(authority);assert!(f.run("").is_err());
}

#[test]
fn missing_wrong_source_state_producer_metadata_or_holder_refuses_before_intent(){
    for key in ["interface","status","scope","source_runtime","state_sha256","external_semantics_sha256","closure","required_output_truths"]{
        let mut f=Fixture::new();f.proof[key]=Value::Null;assert!(f.run("").is_err());assert!(!f.dir.join(NEXT).exists());
    }
    for key in ["producer_manifest_sha256","validator_sha256","source_closure_sha256","runtime_closure_sha256","metadata_union_sha256","physical_closure_sha256"]{
        let mut f=Fixture::new();f.proof[key]=json!("0".repeat(64));assert!(f.run("").is_err());assert!(!f.dir.join(NEXT).exists());
        let f=Fixture::new();fs::write(f.root.join(key),b"modified evidence").unwrap();assert!(f.run("").is_err());
    }
    for key in ["FD_holders","VMA_holders","denied","hardware_or_external_backing","migration_snapshot_or_shared_RAM","all_owners_and_visibility_covered","actual_endpoints_inactive"]{
        let mut f=Fixture::new();f.proof["closure"][key]=Value::Null;assert!(f.run("").is_err());
    }
    let mut f=Fixture::new();f.proof["missing_evidence"]=json!(["actual backing owners"]);assert!(f.run("").is_err());
    let f=Fixture::new();fs::write(&f.state,b"{}").unwrap();assert!(f.run("").is_err());
}

#[test]
fn all_predecessor_drift_partial_output_symlink_hardlink_and_oversize_refuse(){
    for name in serde_json::from_str::<Value>(PINS).unwrap().as_object().unwrap().keys(){let f=Fixture::new();fs::write(f.dir.join(name),b"{}").unwrap();assert!(f.run("").is_err());assert!(!f.dir.join(NEXT).exists());}
    let f=Fixture::new();fs::create_dir(f.dir.join(NEXT)).unwrap();fs::write(f.dir.join(NEXT).join("commit.json.next"),b"partial").unwrap();assert!(f.run("").is_err());
    let f=Fixture::new();let named=f.root.join("state-alias");fs::rename(&f.state,&named).unwrap();std::os::unix::fs::symlink(named,&f.state).unwrap();assert!(f.run("").is_err());
    let f=Fixture::new();fs::hard_link(&f.state,f.root.join("alias")).unwrap();assert!(f.run("").is_err());
    let f=Fixture::new();fs::write(&f.state,vec![0;(1<<20)+1]).unwrap();assert!(f.run("").is_err());
    let f=Fixture::new();fs::rename(f.dir.join("successor-c033-v1"),f.root.join("alias-dir")).unwrap();std::os::unix::fs::symlink(f.root.join("alias-dir"),f.dir.join("successor-c033-v1")).unwrap();assert!(f.run("").is_err());
}

#[test]
fn every_old_context_client_operation_and_nested_binding_is_fenced(){
    let f=Fixture::new();let mut a=f.run("").unwrap();let history=a.historical().unwrap();
    for field in ["pool_id","pool_epoch","provider_session"]{assert!(reject_fenced(&a.reconciliation,&json!({field:history["pools"][0][field]})).is_err());}
    for client in history["pools"][0]["clients"].as_array().unwrap(){assert!(reject_fenced(&a.reconciliation,&json!({"access_binding_v2":{"client_session":client["client_session"]}})).is_err());}
    for value in a.reconciliation["terminal_domain_fences"]["strings"].as_array().unwrap(){assert!(reject_fenced(&a.reconciliation,&json!({"nested":[{"value":value}]})).is_err());}
    for value in a.reconciliation["terminal_domain_fences"]["incarnations"].as_array().unwrap(){assert!(reject_fenced(&a.reconciliation,&json!({"incarnation":value})).is_err());}
    for id in 100..=112{assert!(reject_fenced(&a.reconciliation,&json!({"resource_id":id})).is_err());}
    let mut old=new_context();old["nodes"][0]=history["bootstrap_readiness"]["nodes"]["nodeA"].clone();assert!(a.attach(&mut catalog(),&old).is_err());
    assert!(reject_fenced(&a.reconciliation,&new_context()).is_ok());
    let mut bad=a.reconciliation.clone();bad["terminal_domain_fences"]["strings"]=Value::Null;assert!(reject_fenced(&bad,&new_context()).is_err());
    a.attach(&mut catalog(),&new_context()).unwrap();
}

#[test]
fn committed_state_or_fence_drift_and_nonempty_successor_block_attach(){
    for name in ["historical-state.json","resulting76.json","permanent-fences.json","commit.json"]{
        let f=Fixture::new();let mut a=f.run("").unwrap();fs::write(a.path.join(name),b"{}").unwrap();assert!(a.attach(&mut catalog(),&new_context()).is_err());assert!(a.closure_report().is_err());
    }
    let f=Fixture::new();let mut a=f.run("").unwrap();let mut st=catalog();st.ddr_resources.reserve_unique("other-live",1).unwrap();assert!(a.attach(&mut st,&new_context()).is_err());
    let mut st=catalog();st.ddr_resources.limit=129<<20;assert!(a.attach(&mut st,&new_context()).is_err());
    a.attach(&mut catalog(),&new_context()).unwrap();
}

#[test]
fn cross_process_original_authority_lock_is_held_through_attach(){
    let f=Fixture::new();let mut a=f.run("").unwrap();
    let check=|mode:&str|{assert!(Command::new(std::env::current_exe().unwrap()).args(["--exact","pool_terminal_domain::tests::child_original_authority_lock","--nocapture"])
        .env("UFLOW_TERMINAL_CPU_LOCK_DIR",&f.dir).env("UFLOW_TERMINAL_CPU_LOCK_EXPECT",mode).status().unwrap().success());};
    check("busy");a.attach(&mut catalog(),&new_context()).unwrap();check("busy");drop(a);check("available");
}
#[test]
fn child_original_authority_lock(){
    if let Ok(dir)=std::env::var("UFLOW_TERMINAL_CPU_LOCK_DIR"){
        let result=lock_lineage(Path::new(&dir));assert_eq!(result.is_ok(),std::env::var("UFLOW_TERMINAL_CPU_LOCK_EXPECT").unwrap()=="available");
    }
}

#[test]
fn actual_kernel_live_then_stale_socket_reports_no_fake_Destroy_or_Shutdown(){
    use std::os::unix::net::UnixListener;
    let f=Fixture::new();let state=parsed(STATE).unwrap();let socket=f.root.join("u.sock");let listener=UnixListener::bind(&socket).unwrap();
    let live=shutdown_report_at(&state,&f.root,&kernel_endpoints().unwrap());assert_eq!(live["status"],"LiveEndpointRequiresOwnedNormalDrain");
    assert_eq!(live["normal_Destroy_RPC_succeeded"],false);drop(listener);assert!(socket.exists());
    let stopped=shutdown_report_at(&state,&f.root,&kernel_endpoints().unwrap());assert_eq!(stopped["status"],"AlreadyStoppedNoLiveEndpointResponsibilityRetained");
    assert_eq!(stopped["original_Unknown_history"],state["pools"]);assert_eq!(stopped["charged_bytes"],64<<20);assert_eq!(stopped["reserved_bytes"],64<<20);
    assert_eq!(fs::read(&f.state).unwrap(),STATE);
}

#[test]
fn production_without_E_pins_fails_before_lock_or_verifier(){
    assert!(production_terminal_only().unwrap_err().contains("production-not-ready"));
}

#[test]
fn terminal_serialized_cpu_fixture(){
    let f=Fixture::new();let mut a=f.run("").unwrap();let mut c=catalog();a.attach(&mut c,&new_context()).unwrap();
    let after=c.managed_pools.snapshot(&c);
    assert_eq!(after["charged_bytes"],12<<20);assert_eq!(after["creation_reserved_bytes"],64<<20);
    if let Ok(root)=std::env::var("UFLOW_TERMINAL_LEDGER_CPU_OUTPUT"){
        let path=Path::new(&root);assert!(path.is_absolute() && path.starts_with(ROOT) && path.file_name().unwrap()=="terminal-ledger-cpu");
        fs::create_dir(path).unwrap();
        for name in ["intent.json","historical-state.json","before128.json","permanent-fences.json","resulting76.json","commit.json"]{fs::copy(a.path.join(name),path.join(name)).unwrap();}
        atomic_record(path,"attached-catalog76.json",&after).unwrap();
        atomic_record(path,"shutdown-report.json",&a.closure_report().unwrap()).unwrap();
        atomic_record(path,"results.json",&json!({"pass":true,"scope":"real Rust ledger and fsynced serialized CPU fixture, not real E proof or production retirement",
            "production_ready":false,"production_E_adapter_and_accepted_pins_missing":true,"original_state_sha256":STATE_SHA,
            "B_before_bytes":128<<20,"terminal_domain_loss_bytes":52<<20,"B_after_bytes":76<<20,
            "old_retained12_reserved64_exact":true,"Unknown_and_threeMapped_clients_unchanged":true,
            "normal_SDK_ACK":false,"new_Guest_or_production_authority_or_ledger_touched":false})).unwrap();
    }
}
