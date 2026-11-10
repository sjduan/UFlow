//! Compiled manager-accepted E v4 context and exact C execution policy.
use super::*;
const ANCHOR_SHA:&str="584f34c0a8acaaeca826fbf37d47b07fff514c8906fd3b127e31272f12a660d9";
const ANCHORS:&str=include_str!("../../../../../examples/mem_service_pool/accepted_c101_v4/anchors.json");
const HOST_CLOSEOUT:&str=include_str!("../../../../../examples/mem_service_pool/accepted_c101_v4/host-closeout.json");
const HOST_CLOSEOUT_SHA:&str="d089fa7bc5c95789c8c103b6d611177719c0304486d23fa8d2937d123d0fdcb1";
const POLICY_SHA:&str="48e7153e479acd0b033681d32a2ea8c28beacd584d18db634649d7b7b26fa746";
const POLICY:&str=include_str!("../../../../../examples/mem_service_pool/accepted_c101_v4/execution-policy.json");
const RECORDED:&[u8]=include_bytes!("../../../../../examples/mem_service_pool/accepted_c101_v4/candidate.json");
const CASE:&[u8]=include_bytes!("../../../../../examples/mem_service_pool/accepted_c101_v4/case-template.json");
const COMMON:&str=include_str!("../../../../../tools/phasec/accepted_producers.py");
const LOADER:&str=include_str!("../../../../../tools/phasec/accepted_c101_v4.py");
const ADAPTER:&str=include_str!("../../../../../tools/phasec/c101_execution_adapter.py");
pub(super) fn accepted()->Result<Value,String>{
 if digest(ANCHORS.as_bytes())!=ANCHOR_SHA || digest(POLICY.as_bytes())!=POLICY_SHA || digest(HOST_CLOSEOUT.as_bytes())!=HOST_CLOSEOUT_SHA{return Err("compiled accepted v4 context/policy/HOST closeout drift".into());}
 let a=parsed(ANCHORS.as_bytes())?;
 if a["accepted_context"]["sha256"]!="996f1ecf6abf9755090ff3c1122322c87873be44324e4261c1eec5ea925d83a8" || a["current_candidate"]["sha256"]!=digest(RECORDED){return Err("compiled complete accepted v4/candidate binding required".into());}Ok(a)
}
fn adapter(mut request:Value)->Result<Value,String>{
 use std::{io::Write,process::{Command,Stdio}};
 accepted()?;request["accepted_anchor_bytes"]=json!(ANCHORS);request["execution_policy_bytes"]=json!(POLICY);request["host_closeout_bytes"]=json!(HOST_CLOSEOUT);
 let script=format!("{COMMON}\n{LOADER}\n{ADAPTER}");
 let mut child=Command::new("/usr/local/python3.11.14/bin/python3.11").args(["-B","-I","-S","-c",&script]).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().map_err(|e|e.to_string())?;
 child.stdin.take().ok_or("v3 admission stdin")?.write_all(&serde_json::to_vec(&request).map_err(|e|e.to_string())?).map_err(|e|e.to_string())?;
 let output=child.wait_with_output().map_err(|e|e.to_string())?;
 if !output.status.success() || output.stdout.len()>256<<10 || output.stderr.len()>4096{return Err(format!("accepted v4 admission rejected: {}",String::from_utf8_lossy(&output.stderr[..output.stderr.len().min(1800)])));}
 parsed(&output.stdout)
}
pub(super) fn execution(path:&Path,sha:&str,phase:&str)->Result<Value,String>{
 let policy=parsed(POLICY.as_bytes())?;
 if path!=Path::new(policy["config_directory"].as_str().ok_or("compiled directory")?).join("execution.json") || sha.len()!=64 || !sha.bytes().all(|b|b.is_ascii_digit()||(b'a'..=b'f').contains(&b)) || policy["phases"].get(phase).is_none(){return Err("production-not-ready: exact compiled reviewed execution namespace required; no descriptor/state/authority/verifier IO".into());}
 adapter(json!({"action":"execution-context","execution_action":phase,"execution_manifest":{"path":path,"sha256":sha},"actor_pid":std::process::id(),"daemon_path":std::env::current_exe().map_err(|e|e.to_string())?}))
}
pub(super) fn proof()->Result<Value,String>{
 let value=adapter(json!({"action":"verify-current"}))?;validate_recorded_proof(&value)?;Ok(value)
}
pub(super) fn validate_recorded_proof(value:&Value)->Result<(),String>{
 let a=accepted()?;
 let mut sources=serde_json::Map::new();
 for row in a["same_release_python_dependencies"].as_object().ok_or("accepted source closure")?.values(){sources.insert(row["path"].as_str().ok_or("source path")?.into(),row.clone());}
 for role in ["metadata_producer","terminal_producer","profile_validator","runtime_wrapper","R_codec","W_codec"]{let row=&a["code_and_data"][role];sources.insert(row["path"].as_str().ok_or("accepted role path")?.into(),row.clone());}
 let runtime=Path::new(a["code_and_data"]["runtime_wrapper"]["path"].as_str().ok_or("actual wrapper")?).parent().ok_or("runtime directory")?;
 for name in ["uflow_managed_qemu_profile_v1.py","uflow_qemu_evidence.py","uflow_qemu_profile.py"]{let mut row=a["same_release_python_dependencies"][name].clone();row["path"]=json!(runtime.join(name));sources.insert(row["path"].as_str().unwrap().into(),row);}
 if value["schema"]!=4 || value["kind"]!="EAcceptedCurrentC101ProofV4" || value["consumer_anchor_sha256"]!=ANCHOR_SHA || value["accepted_context"]!=a["accepted_context"] || value["candidate"]!=a["current_candidate"] || value["verified_sources"]!=json!(sources) || value["validated_candidate"]!=parsed(RECORDED)? {return Err("whole externally accepted actual c101 E predicate result required".into());}Ok(())
}
pub(super) fn readonly_case()->Result<Case,String>{Case::parse(CASE,&digest(CASE))}
pub(super) fn validate_case(case:&Case)->Result<(),String>{
 if case.descriptor!=parsed(CASE)?||case.descriptor_sha!=digest(CASE){return Err("whole compiled actual c101 case required".into());}
 let a=accepted()?;let d=&case.descriptor;
 if d["kind"]!="C101TerminalProductionCaseV4" || d["consumer_anchor_sha256"]!=ANCHOR_SHA || d["accepted_context"]!=a["accepted_context"] || d["accepted_candidate"]!=a["current_candidate"] || d["source_run"]!="c101" || d["source_revision"]!=598 || d["current_state"]["sha256"]!=a["current_state_sha256"] || d.get("CPU_fixture_only_not_E_accepted_proof").is_some(){return Err("accepted current production case is distinct from HOST fixture".into());}Ok(())
}
#[cfg(test)]mod tests{
 #[test]fn wrong_execution_namespace_hash_and_fixture_never_reach_original_io(){
  use std::path::Path;
  for p in ["/missing-caller-case","/workspace/uflow/runs/mem-service-shared-pool-20261108/runs/c101/state/state.json"]{for sha in ["", "0", "7cfbdc76f1ab8d696ae3349b6bf5510d5b8e6b4f047f50ae90e579b72c4e62db"]{let e=super::execution(Path::new(p),sha,"terminal-only").unwrap_err();assert!(e.contains("no descriptor/state/authority/verifier IO"));}}
  let production=super::readonly_case().unwrap();super::validate_case(&production).unwrap();
  let raw=include_bytes!("../../../../../examples/mem_service_pool/current_chain_host/case-host.json");let fixture=super::Case::parse(raw,&super::digest(raw)).unwrap();assert!(super::validate_case(&fixture).is_err());
 }
}
