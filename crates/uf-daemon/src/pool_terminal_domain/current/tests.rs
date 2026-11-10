use super::*;
use crate::ddr_resources::BackingId;
use std::{process::Command,sync::atomic::{AtomicU64,Ordering}};
static COUNT:AtomicU64=AtomicU64::new(0);
const CASE_SHA:&str="7f942b77e5e9f0268a8babf58320ea503a6cc429562887ec9355bea98b814765";
struct Fixture{root:PathBuf,dir:PathBuf,state:PathBuf,case:Case,proof_pin:String,cleanup:bool}
impl Fixture{
 fn new()->Self{Self::at(std::env::temp_dir().join(format!("uf-current-{}-{}",std::process::id(),COUNT.fetch_add(1,Ordering::Relaxed))),true)}
 fn at(root:PathBuf,cleanup:bool)->Self{
  fs::create_dir(&root).unwrap();let dir=root.join("lineage");fs::create_dir(&dir).unwrap();File::create(dir.join("authority.lock")).unwrap();
  let source=Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/mem_service_pool");
  let old=source.join("terminal_domain/lineage");let pins:Value=serde_json::from_str(PINS).unwrap();
  for (name,_) in pins.as_object().unwrap(){let dest=dir.join(name);fs::create_dir_all(dest.parent().unwrap()).unwrap();fs::copy(old.join(name),dest).unwrap();}
  let fixture=source.join("current_terminal_host");fs::create_dir(dir.join(NEXT)).unwrap();
  for name in ["intent.json","historical-state.json","before128.json","permanent-fences.json","resulting76.json","commit.json","service-admission-claim.json"]{fs::copy(fixture.join(NEXT).join(name),dir.join(NEXT).join(name)).unwrap();}
  let state=root.join("original-state.json");fs::copy(fixture.join("current-state.json"),&state).unwrap();
  let raw=fs::read(fixture.join("case-host.json")).unwrap();let case=Case::parse(&raw,CASE_SHA).unwrap();fs::write(root.join("case-host.json"),raw).unwrap();
  let evidence=b"CPU ONLY current-domain loss fixture. No E proof, Guest, original authority or normal SDK ACK.\n";
  fs::write(root.join("CPU-proof-source.txt"),evidence).unwrap();let proof_pin=digest(evidence);fs::write(root.join("CPU-proof-pin.txt"),&proof_pin).unwrap();
  Self{root,dir,state,case,proof_pin,cleanup}
 }
 fn existing(root:PathBuf)->Self{
  let case=Case::parse(&fs::read(root.join("case-host.json")).unwrap(),CASE_SHA).unwrap();let proof_pin=fs::read_to_string(root.join("CPU-proof-pin.txt")).unwrap();
  Self{dir:root.join("lineage"),state:root.join("original-state.json"),root,case,proof_pin,cleanup:false}
 }
 fn verify(&self,bytes:&[u8],dir:&Path)->Result<VerifiedCandidate,String>{
  if digest(&bounded(&self.root.join("CPU-proof-source.txt"),4096)?)!=self.proof_pin{return Err("CPU evidence source changed".into());}
  current_candidate(bytes,dir,json!({"kind":"CPUFixtureCurrentDomainLossNotAcceptedEProof","source_sha256":CURRENT_SHA,
   "actual_CPU_evidence_sha256":self.proof_pin,"normal_SDK_ACK":false}))
 }
 fn run(&self,point:&str)->Result<TerminalAuthority,String>{transact_current(&self.dir,&self.state,&self.case,|b,d|self.verify(b,d),point)}
 fn recover(&self)->Result<TerminalAuthority,String>{recover_current(&self.dir,&self.state,&self.case,|b,d|self.verify(b,d))}
}
impl Drop for Fixture{fn drop(&mut self){if self.cleanup{assert_eq!(self.root.parent().unwrap(),std::env::temp_dir());fs::remove_dir_all(&self.root).unwrap();}}}
fn catalog()->Catalog{let mut c=Catalog::new(-1,vec![]);c.ddr_resources.limit=128<<20;c}
fn namespace(f:&Fixture)->Value{json!({"realm":"simulated:current-v2-fresh","session":"current-v2-fresh-session","nodes":[
 {"node":"nodeA","socket":f.root.join("fresh-a.sock")},{"node":"nodeB","socket":f.root.join("fresh-b.sock")}]})}

#[test]
fn production_v2_refuses_before_any_descriptor_state_lock_or_verifier_io(){
 let f=Fixture::new();let before=fs::read(&f.state).unwrap();
 for sha in ["true",CASE_SHA,""]{assert!(production_manifest(&f.root.join("nonexistent"),sha).unwrap_err().contains("no descriptor/state/authority/verifier IO"));}
 assert!(!f.dir.join(SUCCESSOR).exists());assert_eq!(fs::read(&f.state).unwrap(),before);
}
#[test]
fn two_generation_exact_history_backings_tombstones_protected76_and_native_one(){
 let f=Fixture::new();let original=fs::read(&f.state).unwrap();let old_claim=fs::read(f.dir.join(NEXT).join("service-admission-claim.json")).unwrap();
 let mut a=f.run("").unwrap();assert_eq!(fs::read(&f.state).unwrap(),original);assert_eq!(fs::read(f.dir.join(NEXT).join("service-admission-claim.json")).unwrap(),old_claim);
 let mut ledger=a.ledger.take().unwrap();let snapshot=ledger.snapshot();
 assert_eq!(snapshot["charged_bytes"],12<<20);assert_eq!(snapshot["reserved_bytes"],64<<20);assert_eq!(snapshot["terminal_domain_loss_tombstones"].as_object().unwrap().len(),6);
 let current=parsed(&original).unwrap();let old=parsed(&fs::read(f.dir.join(NEXT).join("historical-state.json")).unwrap()).unwrap();
 for (id,state) in [(112,&old),(115,&current)]{
  let row=rows(&state["resource_ledger"]).unwrap()[&id].clone();let b=&row["backing"];
  ledger.reserve_unique("revive-backing",48<<20).unwrap();assert!(ledger.commit_backing("revive-backing",999,BackingId::Provider{
   realm:b["realm"].as_str().unwrap().into(),provider:b["provider"].as_str().unwrap().into(),resource:b["resource"].as_str().unwrap().into(),generation:b["generation"].as_u64().unwrap()},48<<20).is_err());ledger.cancel_uncreated("revive-backing");
 }
 for id in 100..=115{ledger.reserve_unique("revive-id",1).unwrap();assert!(ledger.commit("revive-id",id,9,9,1).is_err());ledger.cancel_uncreated("revive-id");}
 ledger.reserve_unique("fresh",48<<20).unwrap();ledger.commit_backing("fresh",116,BackingId::Provider{realm:"simulated:fresh-actual".into(),provider:"mem_service_obmm".into(),resource:"brand-new-native-one".into(),generation:1},48<<20).unwrap();
 assert_eq!(ledger.charged_bytes()+ledger.reserved_bytes(),124<<20);
 for id in 100..=115{ledger.release_confirmed(id);}assert_eq!(ledger.retained_bytes(),12<<20);
}
#[test]
fn union_keeps_both_generations_and_independent_identity_highwaters(){
 let f=Fixture::new();let a=f.run("").unwrap();let rec=&a.reconciliation;let old=parsed(&fs::read(f.dir.join(NEXT).join("permanent-fences.json")).unwrap()).unwrap();let state=parsed(&fs::read(&f.state).unwrap()).unwrap();
 for s in old["strings"].as_array().unwrap(){assert!(reject_fenced(rec,&json!({"nested":s})).is_err());}
 for s in [&state["pools"][0]["pool_id"],&state["pools"][0]["ranges"]["leases"][0]["lease_id"]]{assert!(reject_fenced(rec,&json!({"nested":s})).is_err());}
 for (key,min) in [("resource_id","minimum_next_object_id"),("placement_id","minimum_next_placement_id"),("binding_id","minimum_next_binding_id"),("access_generation","minimum_next_access_id"),("lease_generation","minimum_next_lease_id"),("client_id","minimum_next_client_id"),("block_id","minimum_next_block_id")]{
  let n=rec["terminal_domain_fences"][min].as_u64().unwrap();assert!(reject_fenced(rec,&json!({key:n-1})).is_err());assert!(reject_fenced(rec,&json!({key:n})).is_ok());
 }
 assert_eq!(rec["terminal_domain_fences"]["minimum_next_object_id"],116);
 assert!(rec["terminal_domain_fences"]["minimum_next_lease_id"].as_u64().unwrap()>1011);
 assert!(reject_fenced(rec,&json!({"realm":"simulated:completely-new","pool_id":"brand-new","resource_id":116,"generation":1})).is_ok());
 let mut malformed=rec.clone();malformed["terminal_domain_fences"]["minimum_next_access_id"]=Value::Null;assert!(reject_fenced(&malformed,&json!({"realm":"fresh"})).is_err());
}
#[test]
fn each_fsync_partial_crash_is_fail_closed_and_only_final_commit_recovers_once(){
 for point in ["directory","intent","historical128","before128","fences","backings","terminal52","resulting76","commit"]{
  let f=Fixture::new();assert!(f.run(point).is_err());assert_eq!(digest(&fs::read(&f.state).unwrap()),CURRENT_SHA);assert!(f.run("").is_err());
  let recovered=f.recover();assert_eq!(recovered.is_ok(),point=="commit","{point}");
 }
 let f=Fixture::new();drop(f.run("").unwrap());let mut a=f.recover().unwrap();let mut st=catalog();let lifetime=a.attach_staged(&mut st,&namespace(&f)).unwrap();
 assert_eq!((st.ddr_resources.charged_bytes(),st.ddr_resources.reserved_bytes()),(12<<20,68<<20));assert_eq!(st.next_object_id,118);
 assert!(a.attach_staged(&mut catalog(),&namespace(&f)).is_err());drop(a);assert!(f.recover().is_err());drop(lifetime);assert!(f.recover().is_err());
 assert_eq!(st.managed_pools.reconciliation["terminal_canary_charge_objects"],json!({"nodeA":116,"nodeB":117}));
}
#[test]
fn state_predecessor_history_claim_backing_and_evidence_drift_never_commits(){
 for target in ["state","previous-history","previous-claim","previous-fences","predecessor","evidence"]{
  let f=Fixture::new();let p=match target{"state"=>f.state.clone(),"previous-history"=>f.dir.join(NEXT).join("historical-state.json"),"previous-claim"=>f.dir.join(NEXT).join("service-admission-claim.json"),"previous-fences"=>f.dir.join(NEXT).join("permanent-fences.json"),"predecessor"=>f.dir.join("result.json"),_=>f.root.join("CPU-proof-source.txt")};
  fs::write(p,b"drift").unwrap();assert!(f.run("").is_err(),"{target}");assert!(!f.dir.join(SUCCESSOR).exists());
 }
 let f=Fixture::new();let count=AtomicU64::new(0);
 assert!(transact_current(&f.dir,&f.state,&f.case,|b,d|{if count.fetch_add(1,Ordering::Relaxed)==1{fs::write(f.root.join("CPU-proof-source.txt"),b"late proof drift").unwrap();}f.verify(b,d)},"").is_err());
 assert!(!f.dir.join(SUCCESSOR).join("commit.json").exists());
}
#[test]
fn coherent_rewrite_cannot_drop_union_fences_or_prior_backings(){
 for file in ["resulting76.json","inherited-backing-identities.json","intent.json"]{
  let f=Fixture::new();drop(f.run("").unwrap());let path=f.dir.join(SUCCESSOR);let mut value=parsed(&fs::read(path.join(file)).unwrap()).unwrap();
  if file=="resulting76.json"{value["reconciliation"]["terminal_domain_fences"]["strings"]=json!(["new-only"]);}else if file=="intent.json"{value["resource_ids"]=json!([110,111,112]);}else{value["112"]["backing"]["resource"]=json!("erased-old-backing");}
  fs::write(path.join(file),serde_json::to_vec(&value).unwrap()).unwrap();let mut commit=parsed(&fs::read(path.join("commit.json")).unwrap()).unwrap();
  commit["files"][file]=json!(digest(&fs::read(path.join(file)).unwrap()));if file=="resulting76.json"{commit["resulting76_sha256"]=commit["files"][file].clone();}
  fs::write(path.join("commit.json"),serde_json::to_vec(&commit).unwrap()).unwrap();assert!(f.recover().is_err());
 }
}
#[test]
fn original_lock_survives_service_claim_for_entire_lifetime(){
 let f=Fixture::new();let mut a=f.run("").unwrap();
 let check=|mode:&str|assert!(Command::new(std::env::current_exe().unwrap()).args(["--exact","pool_terminal_domain::tests::child_original_authority_lock","--nocapture"])
  .env("UFLOW_TERMINAL_CPU_LOCK_DIR",&f.dir).env("UFLOW_TERMINAL_CPU_LOCK_EXPECT",mode).status().unwrap().success());
 check("busy");let lifetime=a.attach_staged(&mut catalog(),&namespace(&f)).unwrap();drop(a);check("busy");drop(lifetime);check("available");assert!(f.recover().is_err());
}
#[test]
fn current_terminal_commit_process(){
 if let Ok(config)=std::env::var("UFLOW_CURRENT_TERMINAL_CPU_CONFIG"){
  let c=parsed(&fs::read(config).unwrap()).unwrap();let root=PathBuf::from(c["terminal_origin"].as_str().unwrap());assert!(root.is_absolute()&&root.starts_with(ROOT)&&root.file_name().unwrap()=="terminal-origin");
  let f=Fixture::at(root,false);let a=f.run("").unwrap();assert_eq!(a.closure_report().unwrap()["B_at_terminal_commit_bytes"],76<<20);
 }
}
#[test]
fn current_terminal_service_process(){
 if let Ok(config)=std::env::var("UFLOW_CURRENT_TERMINAL_CPU_CONFIG"){
  let c=parsed(&fs::read(config).unwrap()).unwrap();let f=Fixture::existing(PathBuf::from(c["terminal_origin"].as_str().unwrap()));let mut a=f.recover().unwrap();
  crate::pool_service::run_terminal_staged_fixture(c["endpoint"].as_str().unwrap(),Path::new(c["state"].as_str().unwrap()),Path::new(c["provider_config"].as_str().unwrap()),&mut a).unwrap();
 }
}
