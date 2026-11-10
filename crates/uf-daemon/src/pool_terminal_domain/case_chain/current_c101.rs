//! Actual current-c101 production consumer, separate from historical v3.
use super::*;
fn production_case(path:&Path,sha:&str,phase:&str)->Result<Case,String>{
    let context=producer_context_v4::execution(path,sha,phase)?;let row=&context["case"];let p=Path::new(row["path"].as_str().ok_or("actual case path")?);
    if p!=path.parent().ok_or("execution directory")?.join("current-case.json"){return Err("case outside exact execution namespace".into());}
    let raw=bounded(p,1<<20)?;if row["bytes"]!=raw.len(){return Err("case byte binding".into());}let case=Case::parse(&raw,row["sha256"].as_str().ok_or("case SHA")?)?;producer_context_v4::validate_case(&case)?;Ok(case)
}
fn verify_actual(case:&Case,bytes:&[u8],lineage:&Lineage)->Result<VerifiedCandidate,String>{
    producer_context_v4::validate_case(case)?;candidate(case,bytes,lineage,producer_context_v4::proof()?)
}
pub(crate) fn production_manifest(path:&Path,external_sha:&str)->Result<Value,String>{
    let case=production_case(path,external_sha,"terminal-only")?;
    let a=transact(&Path::new(ROOT).join("implementation/responsibility-c013"),&Path::new(ROOT).join("runs/c101/state/state.json"),&case,|bytes,lineage|verify_actual(&case,bytes,lineage),"")?;a.closure_report()
}
pub(crate) fn production_verify_readonly()->Result<Value,String>{
    let case=producer_context_v4::readonly_case()?;producer_context_v4::validate_case(&case)?;let dir=Path::new(ROOT).join("implementation/responsibility-c013");
    if dir.join(case.successor()).exists(){return Err("case already consumed; no replay of terminal eligibility".into());}
    let lineage=case.lineage(&dir,false)?;let bytes=bounded(&Path::new(ROOT).join("runs/c101/state/state.json"),1<<20)?;Ok(verify_actual(&case,&bytes,&lineage)?.proof)
}
pub(crate) fn validate_managed_binding(value:&Value)->Result<Value,String>{
    let a=producer_context_v4::accepted()?;
    if value.as_object().map(serde_json::Map::len)!=Some(3) || value["accepted_context"]!=a["accepted_context"]{return Err("compiled accepted c101-v4 context binding required before IO".into());}
    let path=value["path"].as_str().ok_or("actual execution path")?;let sha=value["sha256"].as_str().ok_or("actual execution SHA")?;producer_context_v4::execution(Path::new(path),sha,"successor")?;Ok(value.clone())
}
pub(crate) fn restore_staged(st:&mut Catalog,config:&Value,loaded:&Value)->Result<File,String>{
    let path=config["terminal_c101_execution_manifest"].as_str().ok_or("v4 execution path")?;let sha=config["terminal_c101_execution_manifest_sha256"].as_str().ok_or("v4 execution SHA")?;
    let case=production_case(Path::new(path),sha,"successor")?;
    let mut a=recover(&Path::new(ROOT).join("implementation/responsibility-c013"),&Path::new(ROOT).join("runs/c101/state/state.json"),&case,|bytes,lineage|verify_actual(&case,bytes,lineage))?;
    let nodes=loaded["nodes"].as_object().ok_or("deferred node identities")?.values().cloned().collect::<Vec<_>>();a.attach_staged(st,&json!({"realm":loaded["realm"],"session":loaded["session"],"nodes":nodes}))
}
/// Re-derive consumption accounting from committed bytes, without replaying E
/// eligibility, opening the authority lock, or performing any original write.
pub(crate) fn production_audit_consumed(path:&Path,sha:&str)->Result<Value,String>{
    let case=production_case(path,sha,"postclose-audit")?;let dir=Path::new(ROOT).join("implementation/responsibility-c013");
    let lineage=case.lineage(&dir,true)?;let root=dir.join(case.successor());let mut names=FILES.to_vec();names.push("service-admission-claim.json");exact_names(&root,&names)?;
    let original=bounded(&Path::new(ROOT).join("runs/c101/state/state.json"),1<<20)?;
    let intent=parsed(&bounded(&root.join("intent.json"),1<<20)?)?;producer_context_v4::validate_recorded_proof(&intent["verified_candidate"])?;
    let p=candidate(&case,&original,&lineage,intent["verified_candidate"].clone())?;
    let raw=bounded(&root.join("commit.json"),1<<20)?;let commit=parsed(&raw)?;
    let hashes=FILES.iter().filter(|n|**n!="commit.json").map(|n|Ok(((*n).to_string(),json!(digest(&bounded(&root.join(n),1<<20)?))))).collect::<Result<serde_json::Map<_,_>,String>>()?;
    let expected_commit=json!({"kind":"TerminalDomainAuthorityCommitted","version":case.version(),"original_state_sha256":case.sha(),"case_sha256":case.descriptor_sha,
        "proof_consumed_once":p.proof_sha,"files":hashes,"resulting76_sha256":hashes["resulting76.json"],"fences_sha256":hashes["permanent-fences.json"],"normal_SDK_ACK":false});
    let expected_intent=json!({"kind":"VersionedCurrentTerminalLossPending","version":case.version(),"case":case.descriptor,"case_sha256":case.descriptor_sha,
        "source_state_sha256":case.sha(),"source_revision":case.revision(),"proof_sha256":p.proof_sha,"verified_candidate":p.proof,"lineage":lineage.pins,"resource_ids":case.ids(),"normal_SDK_ACK":false});
    if commit!=expected_commit||intent!=expected_intent||bounded(&root.join("historical-state.json"),1<<20)?!=original||
        parsed(&bounded(&root.join("inherited-backing-identities.json"),1<<20)?)?!=p.prior_backings||parsed(&bounded(&root.join("permanent-fences.json"),1<<20)?)?!=p.fences{return Err("consumed complete source/lineage/proof/hash/backing/fences drift".into());}
    p.matches_ledger(&parsed(&bounded(&root.join("before128.json"),1<<20)?)?)?;let mut ledger=DdrResources::inherit_terminal_source(&p)?;ledger.apply_terminal_loss(&p)?;
    let mut expected=result(&case,&p,&root,&ledger);let mut actual=parsed(&bounded(&root.join("resulting76.json"),1<<20)?)?;
    expected["resource_ledger"]=normalized_ledger(&expected["resource_ledger"])?;actual["resource_ledger"]=normalized_ledger(&actual["resource_ledger"])?;
    if actual!=expected{return Err("consumed protected76/current52/twelve tombstones changed".into());}
    let claim=parsed(&bounded(&root.join("service-admission-claim.json"),1<<20)?)?;
    ledger.reserve_unique("canary:nodeA",2<<20)?;ledger.reserve_unique("canary:nodeB",2<<20)?;
    let namespace=&claim["namespace"];reject_fenced(&actual["reconciliation"],namespace)?;
    for k in ["realm","session"]{if namespace[k].as_str().is_none_or(|s|s.is_empty()){return Err("consumed fresh staged namespace required".into());}}
    let expected_claim=json!({"kind":"SuccessorServiceAdmissionPending","namespace":namespace,"source_terminal_commit_sha256":digest(&raw),
        "conservative_B_bytes":80<<20,"resource_ledger":ledger.snapshot(),"canary_charge_objects":{"nodeA":122,"nodeB":123},
        "Guest_identity_known":false,"business_admission_enabled":false,"native_SDK_ACK":false});
    if !ledger_equal(&claim["resource_ledger"],&expected_claim["resource_ledger"])?{return Err("consumed reservation-before-identity ledger drift".into());}
    let mut checked=claim.clone();checked["resource_ledger"]=expected_claim["resource_ledger"].clone();
    if checked!=expected_claim{return Err("consumed one-claim/reserve122123 metadata drift".into());}
    Ok(json!({"path":root.join("commit.json"),"bytes":raw.len(),"sha256":digest(&raw)}))
}

