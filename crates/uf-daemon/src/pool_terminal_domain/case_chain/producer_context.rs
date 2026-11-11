//! Compiled manager-accepted E v6 context and exact C execution policy.
use super::*;
const ANCHOR_SHA:&str="e63ff8efde507b49b5fefc7849dce4eee508bbd0a116fd2e88902833f4de46fc";
const ANCHORS:&str=include_str!("../../../../../examples/mem_service_pool/accepted_c081_v3/anchors.json");
const POLICY_SHA:&str="824dd32fedde7d8b17fe4fd6d592fca7cf8cacbfd5745818d14e68606b348592";
const POLICY:&str=include_str!("../../../../../examples/mem_service_pool/accepted_c081_v3/execution-policy.json");
const RECORDED:&[u8]=include_bytes!("../../../../../examples/mem_service_pool/accepted_c081_v3/candidate.json");
const CASE:&[u8]=include_bytes!("../../../../../examples/mem_service_pool/accepted_c081_v3/case-template.json");
const COMMON:&str=include_str!("../../../../../tools/phasec/accepted_producers.py");
const LOADER:&str=include_str!("../../../../../tools/phasec/accepted_c081_v3.py");
const ADAPTER:&str=include_str!("../../../../../tools/phasec/c081_execution_adapter.py");
pub(super) fn accepted()->Result<Value,String>{
 if digest(ANCHORS.as_bytes())!=ANCHOR_SHA || digest(POLICY.as_bytes())!=POLICY_SHA{return Err("compiled accepted v6 context/policy drift".into());}
 let a=parsed(ANCHORS.as_bytes())?;
 if a["accepted_context"]["sha256"]!="e6241411f9217661c6ccf5376120c67a5c096629c501c48f02bfb4238fb8175c" || a["code_and_data"]["terminal_candidate"]["sha256"]!=digest(RECORDED){return Err("compiled complete accepted v6/candidate binding required".into());}Ok(a)
}
fn adapter(mut request:Value)->Result<Value,String>{
 use std::{io::Write,process::{Command,Stdio}};
 accepted()?;request["accepted_anchor_bytes"]=json!(ANCHORS);request["execution_policy_bytes"]=json!(POLICY);
 let script=format!("{COMMON}\n{LOADER}\n{ADAPTER}");
 let mut child=Command::new("/usr/local/python3.11.14/bin/python3.11").args(["-B","-I","-S","-c",&script]).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().map_err(|e|e.to_string())?;
 child.stdin.take().ok_or("v3 admission stdin")?.write_all(&serde_json::to_vec(&request).map_err(|e|e.to_string())?).map_err(|e|e.to_string())?;
 let output=child.wait_with_output().map_err(|e|e.to_string())?;
 if !output.status.success() || output.stdout.len()>256<<10 || output.stderr.len()>4096{return Err(format!("accepted v6 admission rejected: {}",String::from_utf8_lossy(&output.stderr[..output.stderr.len().min(1800)])));}
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
 if value["schema"]!=3 || value["kind"]!="EAcceptedCurrentC081ProofV3" || value["consumer_anchor_sha256"]!=ANCHOR_SHA || value["accepted_context"]!=a["accepted_context"] || value["candidate"]!=a["code_and_data"]["terminal_candidate"] || value["validated_candidate"]!=parsed(RECORDED)? {return Err("whole externally accepted actual c081 E predicate result required".into());}Ok(())
}
pub(super) fn readonly_case()->Result<Case,String>{Case::parse(CASE,&digest(CASE))}
pub(super) fn validate_case(case:&Case)->Result<(),String>{
 let a=accepted()?;let d=&case.descriptor;
 if d["kind"]!="C081TerminalProductionCaseV3" || d["consumer_anchor_sha256"]!=ANCHOR_SHA || d["accepted_context"]!=a["accepted_context"] || d["accepted_candidate"]!=a["code_and_data"]["terminal_candidate"] || d["source_run"]!="c081" || d["source_revision"]!=419 || d["current_state"]["sha256"]!=a["current"]["sha256"] || d.get("CPU_fixture_only_not_E_accepted_proof").is_some(){return Err("accepted current production case is distinct from HOST fixture".into());}Ok(())
}
#[cfg(test)]mod tests{
 #[test]fn wrong_execution_namespace_hash_and_fixture_never_reach_original_io(){
  use std::path::Path;
  for p in ["/missing-caller-case","/workspace/uflow/runs/mem-service-shared-pool-20261108/runs/c081/state/state.json"]{for sha in ["", "0", "7cfbdc76f1ab8d696ae3349b6bf5510d5b8e6b4f047f50ae90e579b72c4e62db"]{let e=super::execution(Path::new(p),sha,"terminal-only").unwrap_err();assert!(e.contains("no descriptor/state/authority/verifier IO"));}}
  let production=super::readonly_case().unwrap();super::validate_case(&production).unwrap();
  let raw=include_bytes!("../../../../../examples/mem_service_pool/current_chain_host/case-host.json");let fixture=super::Case::parse(raw,&super::digest(raw)).unwrap();assert!(super::validate_case(&fixture).is_err());
 }
}
