use super::*;
use crate::ddr_resources::BackingId;
use std::{process::Command,sync::atomic::{AtomicU64,Ordering}};
static COUNT:AtomicU64=AtomicU64::new(0);
const CASE_SHA:&str="bc6e61686b9ea283eb71249b9c93f5de1c4168818f196ce5d07d892ffb116db9";
struct Fixture{root:PathBuf,dir:PathBuf,state:PathBuf,case:Case,pin:String,cleanup:bool}
impl Fixture{
 fn new()->Self{Self::at(std::env::temp_dir().join(format!("uf-chain-{}-{}",std::process::id(),COUNT.fetch_add(1,Ordering::Relaxed))),true)}
 fn at(root:PathBuf,cleanup:bool)->Self{
  fs::create_dir(&root).unwrap();let source=Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/mem_service_pool/current_chain_host");
  let m=parsed(&fs::read(source.join("fixture-manifest.json")).unwrap()).unwrap();
  for (name,_) in m["origins"].as_object().unwrap(){let dest=root.join(name);fs::create_dir_all(dest.parent().unwrap()).unwrap();fs::copy(source.join(name),dest).unwrap();}
  let raw=fs::read(source.join("case-host.json")).unwrap();let case=Case::parse(&raw,CASE_SHA).unwrap();fs::write(root.join("case-host.json"),raw).unwrap();
  let dir=root.join("lineage");File::create(dir.join("authority.lock")).unwrap();let state=root.join("current-state.json");
  let raw=b"HOST ONLY actual c081 metadata fixture; never E permit/normal ACK/Guest/original authority.\n";let pin=digest(raw);fs::write(root.join("CPU-evidence.txt"),raw).unwrap();
  Self{root,dir,state,case,pin,cleanup}
 }
 fn existing(root:PathBuf)->Self{let case=Case::parse(&fs::read(root.join("case-host.json")).unwrap(),CASE_SHA).unwrap();let pin=digest(&fs::read(root.join("CPU-evidence.txt")).unwrap());Self{dir:root.join("lineage"),state:root.join("current-state.json"),root,case,pin,cleanup:false}}
 fn verify(&self,b:&[u8],l:&Lineage)->Result<VerifiedCandidate,String>{
  if digest(&bounded(&self.root.join("CPU-evidence.txt"),4096)?)!=self.pin{return Err("HOST evidence drift".into());}
  candidate(&self.case,b,l,json!({"kind":"HOSTFixtureThreeGenerationCurrentLoss","case_sha256":CASE_SHA,"source_sha256":self.case.sha(),"HOST_evidence_sha256":self.pin,"normal_SDK_ACK":false}))
 }
 fn run(&self,point:&str)->Result<TerminalAuthority,String>{transact(&self.dir,&self.state,&self.case,|b,l|self.verify(b,l),point)}
 fn recover(&self)->Result<TerminalAuthority,String>{recover(&self.dir,&self.state,&self.case,|b,l|self.verify(b,l))}
}
impl Drop for Fixture{fn drop(&mut self){if self.cleanup{assert_eq!(self.root.parent(),Some(std::env::temp_dir().as_path()));fs::remove_dir_all(&self.root).unwrap();}}}
fn catalog()->Catalog{let mut c=Catalog::new(-1,vec![]);c.ddr_resources.limit=128<<20;c}
fn namespace(f:&Fixture)->Value{json!({"realm":"simulated:chain-new","session":"chain-new-session","nodes":[{"node":"nodeA","socket":f.root.join("new-a.sock")},{"node":"nodeB","socket":f.root.join("new-b.sock")}]})}

#[test]fn chain_production_rejects_any_caller_before_original_io(){
 let f=Fixture::new();for pin in [CASE_SHA,"true",""]{assert!(production_manifest(&f.root.join("must-not-read"),pin).unwrap_err().contains("no descriptor/state/authority/verifier IO"));}
 assert!(!f.dir.join(f.case.successor()).exists());assert_eq!(digest(&fs::read(&f.state).unwrap()),f.case.sha());
}
#[test]fn chain_three_generations_all_six_backings_nine_tombstones_and_protected76(){
 let f=Fixture::new();let old=f.case.descriptor["claimed_ancestors"].clone();let mut a=f.run("").unwrap();let mut ledger=a.ledger.take().unwrap();let state=ledger.snapshot();
 assert_eq!(state["terminal_domain_loss_tombstones"].as_object().unwrap().len(),9);assert_eq!(state["charged_bytes"],12<<20);assert_eq!(state["reserved_bytes"],64<<20);
 let inherited=parsed(&fs::read(a.path.join("inherited-backing-identities.json")).unwrap()).unwrap();assert_eq!(inherited.as_object().unwrap().len(),6);
 for id in 110..=115{let b=&inherited[id.to_string()]["backing"];ledger.reserve_unique("revive",48<<20).unwrap();assert!(ledger.commit_backing("revive",999,BackingId::Provider{realm:b["realm"].as_str().unwrap().into(),provider:b["provider"].as_str().unwrap().into(),resource:b["resource"].as_str().unwrap().into(),generation:b["generation"].as_u64().unwrap()},48<<20).is_err());ledger.cancel_uncreated("revive");}
 for id in 100..=118{ledger.reserve_unique("old-id",1).unwrap();assert!(ledger.commit("old-id",id,9,9,1).is_err());ledger.cancel_uncreated("old-id");}
 ledger.reserve_unique("fresh",48<<20).unwrap();ledger.commit_backing("fresh",119,BackingId::Provider{realm:"simulated:new-chain-native".into(),provider:"mem_service_obmm".into(),resource:"never-seen-native-one".into(),generation:1},48<<20).unwrap();
 assert_eq!(digest(&fs::read(&f.state).unwrap()),f.case.sha());
 for ancestor in old.as_array().unwrap(){for (name,row) in ancestor["files"].as_object().unwrap(){checked_file(&f.dir.join(ancestor["directory"].as_str().unwrap()).join(name),row).unwrap();}}
}
#[test]fn chain_fence_union_and_independent_type_highwaters_cover_every_generation(){
 let f=Fixture::new();let a=f.run("").unwrap();let rec=&a.reconciliation;
 for ancestor in f.case.descriptor["claimed_ancestors"].as_array().unwrap(){let p=parsed(&fs::read(f.dir.join(ancestor["directory"].as_str().unwrap()).join("permanent-fences.json")).unwrap()).unwrap();for s in p["strings"].as_array().unwrap(){assert!(reject_fenced(rec,&json!({"value":s})).is_err());}for n in p["incarnations"].as_array().unwrap(){assert!(reject_fenced(rec,&json!({"incarnation":n})).is_err());}}
 let original=parsed(&fs::read(&f.state).unwrap()).unwrap();for v in [&original["pools"][0]["pool_id"],&original["pools"][0]["ranges"]["leases"][0]["lease_id"]]{assert!(reject_fenced(rec,&json!({"value":v})).is_err());}
 for (key,min) in [("resource_id","minimum_next_object_id"),("placement_id","minimum_next_placement_id"),("binding_id","minimum_next_binding_id"),("access_generation","minimum_next_access_id"),("lease_generation","minimum_next_lease_id"),("client_id","minimum_next_client_id"),("block_id","minimum_next_block_id"),("plan_id","minimum_next_transfer_plan_id"),("event_id","minimum_next_transfer_event_id")]{let n=rec["terminal_domain_fences"][min].as_u64().unwrap();assert!(reject_fenced(rec,&json!({key:n-1})).is_err());assert!(reject_fenced(rec,&json!({key:n})).is_ok());}
 assert_eq!(rec["terminal_domain_fences"]["minimum_next_object_id"],119);assert!(rec["terminal_domain_fences"]["minimum_next_lease_id"].as_u64().unwrap()>=1021);
 assert!(reject_fenced(rec,&json!({"resource_id":121,"generation":1,"realm":"simulated:truly-new"})).is_ok());
}
#[test]fn chain_each_durable_crash_partial_refusal_commit_recovery_and_one_claim(){
 for point in ["directory","intent","historical128","before128","fences","backings","terminal52","resulting76","commit"]{let f=Fixture::new();assert!(f.run(point).is_err());assert!(f.run("").is_err());assert_eq!(f.recover().is_ok(),point=="commit","{point}");}
 let f=Fixture::new();drop(f.run("").unwrap());let mut a=f.recover().unwrap();let mut st=catalog();let lifetime=a.attach_staged(&mut st,&namespace(&f)).unwrap();
 assert_eq!(st.next_object_id,121);assert_eq!((st.ddr_resources.charged_bytes(),st.ddr_resources.reserved_bytes()),(12<<20,68<<20));
 assert_eq!(st.managed_pools.reconciliation["terminal_canary_charge_objects"],json!({"nodeA":119,"nodeB":120}));
 assert!(a.attach_staged(&mut catalog(),&namespace(&f)).is_err());drop(a);assert!(f.recover().is_err());drop(lifetime);assert!(f.recover().is_err());
}
#[test]fn chain_source_ancestors_proof_backing_claim_and_extra_drift_never_commit(){
 for target in ["source","first-claim","second-claim","second-backings","predecessor","proof","extra"]{let f=Fixture::new();let ancestors=f.case.descriptor["claimed_ancestors"].as_array().unwrap();let p=match target{"source"=>f.state.clone(),"first-claim"=>f.dir.join(ancestors[0]["directory"].as_str().unwrap()).join("service-admission-claim.json"),"second-claim"=>f.dir.join(ancestors[1]["directory"].as_str().unwrap()).join("service-admission-claim.json"),"second-backings"=>f.dir.join(ancestors[1]["directory"].as_str().unwrap()).join("inherited-backing-identities.json"),"predecessor"=>f.dir.join("result.json"),"proof"=>f.root.join("CPU-evidence.txt"),_=>f.dir.join("unknown-successor")};fs::write(p,b"changed").unwrap();assert!(f.run("").is_err(),"{target}");assert!(!f.dir.join(f.case.successor()).exists());}
 let f=Fixture::new();let count=AtomicU64::new(0);assert!(transact(&f.dir,&f.state,&f.case,|b,l|{if count.fetch_add(1,Ordering::Relaxed)==1{fs::write(f.root.join("CPU-evidence.txt"),b"late").unwrap();}f.verify(b,l)},"").is_err());assert!(!f.dir.join(f.case.successor()).join("commit.json").exists());
}
#[test]fn chain_coherent_hash_rewrite_cannot_replace_rederived_fences_backings_or_ledger(){
 for name in ["resulting76.json","inherited-backing-identities.json","intent.json"]{let f=Fixture::new();drop(f.run("").unwrap());let p=f.dir.join(f.case.successor());let mut v=parsed(&fs::read(p.join(name)).unwrap()).unwrap();
 if name=="resulting76.json"{v["reconciliation"]["terminal_domain_fences"]["strings"]=json!([]);}else if name=="intent.json"{v["resource_ids"]=json!([113,114,115]);}else{v["115"]["backing"]["resource"]=json!("erased-original");}
 fs::write(p.join(name),serde_json::to_vec(&v).unwrap()).unwrap();let mut c=parsed(&fs::read(p.join("commit.json")).unwrap()).unwrap();c["files"][name]=json!(digest(&fs::read(p.join(name)).unwrap()));if name=="resulting76.json"{c["resulting76_sha256"]=c["files"][name].clone();}fs::write(p.join("commit.json"),serde_json::to_vec(&c).unwrap()).unwrap();assert!(f.recover().is_err());}
}
#[test]fn chain_original_lock_excludes_other_process_for_entire_service_lifetime(){
 let f=Fixture::new();let mut a=f.run("").unwrap();let check=||assert!(Command::new(std::env::current_exe().unwrap()).args(["--exact","pool_terminal_domain::case_chain::tests::chain_child_original_lock","--nocapture"]).env("UFLOW_CHAIN_LOCK",&f.dir).status().unwrap().success());check();
 let lifetime=a.attach_staged(&mut catalog(),&namespace(&f)).unwrap();drop(a);check();drop(lifetime);assert!(lock_lineage(&f.dir).is_ok());
}
#[test]fn chain_child_original_lock(){if let Ok(path)=std::env::var("UFLOW_CHAIN_LOCK"){assert!(lock_lineage(Path::new(&path)).is_err());}}
#[test]fn chain_commit_process(){if let Ok(path)=std::env::var("UFLOW_CHAIN_TERMINAL_CPU_CONFIG"){let c=parsed(&fs::read(path).unwrap()).unwrap();let root=PathBuf::from(c["terminal_origin"].as_str().unwrap());assert!(root.is_absolute()&&root.starts_with(ROOT)&&root.file_name().unwrap()=="terminal-origin");let f=Fixture::at(root,false);let a=f.run("").unwrap();assert_eq!(a.closure_report().unwrap()["B_at_terminal_commit_bytes"],76<<20);}}
#[test]fn chain_service_process(){if let Ok(path)=std::env::var("UFLOW_CHAIN_TERMINAL_CPU_CONFIG"){let c=parsed(&fs::read(path).unwrap()).unwrap();let f=Fixture::existing(PathBuf::from(c["terminal_origin"].as_str().unwrap()));let mut a=f.recover().unwrap();crate::pool_service::run_terminal_staged_fixture(c["endpoint"].as_str().unwrap(),Path::new(c["state"].as_str().unwrap()),Path::new(c["provider_config"].as_str().unwrap()),&mut a).unwrap();}}
