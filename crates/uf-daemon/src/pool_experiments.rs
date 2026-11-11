//! Explicit boundary for historical simulator recovery and task-specific proofs.
//!
//! The normal daemon does not load historical witnesses, invoke their Python
//! producers, or expose administrative recovery commands. Enabling the feature
//! preserves the original proof/authority/Unknown checks without changing them.
use crate::catalog::Catalog;
use crate::mem_service_pool::MemServicePoolProvider;
use serde_json::Value;
use std::fs::File;
use std::path::Path;

const FEATURE_ERROR: &str = "experimental simulator recovery requires --features experimental-reconciliation; no recovery IO performed";

pub(crate) fn validate_configuration(config: &Value) -> Result<(), String> {
    if !cfg!(any(test, feature = "experimental-reconciliation"))
        && ["reconciliation", "managed_cache_execution", "managed_write_execution"]
            .iter()
            .any(|key| config.get(*key).is_some_and(|value| !value.is_null()))
    {
        return Err(FEATURE_ERROR.into());
    }
    Ok(())
}

pub(crate) fn managed_binding(value: &Value, write: bool) -> Result<Value, String> {
    #[cfg(any(test, feature = "experimental-reconciliation"))]
    {
        if write {
            crate::pool_terminal_domain::case_chain::current_c101::validate_managed_binding(value)
        } else {
            crate::pool_terminal_domain::case_chain::validate_managed_binding(value)
        }
    }
    #[cfg(not(any(test, feature = "experimental-reconciliation")))]
    {
        let _ = (value, write);
        Err(FEATURE_ERROR.into())
    }
}

pub(crate) fn reject_fenced(history: &Value, namespace: &Value) -> Result<(), String> {
    #[cfg(any(test, feature = "experimental-reconciliation"))]
    {
        crate::pool_terminal_domain::reject_fenced(history, namespace)
    }
    #[cfg(not(any(test, feature = "experimental-reconciliation")))]
    {
        let _ = namespace;
        if history.is_null() || history.as_object().is_some_and(|value| value.is_empty()) {
            Ok(())
        } else {
            Err(FEATURE_ERROR.into())
        }
    }
}

pub(crate) fn guard_original_scope(remote: Option<&MemServicePoolProvider>) -> Result<(), String> {
    #[cfg(any(test, feature = "experimental-reconciliation"))]
    if remote.is_some_and(|provider| provider.reconciliation.is_none())
        && Path::new("/workspace/uflow/runs/mem-service-shared-pool-20261108/runs/c013/state/state.json").exists()
    {
        return Err("original c013 responsibility must enter this authority before new remote bootstrap/business".into());
    }
    let _ = remote;
    Ok(())
}

pub(crate) fn restore(catalog: &mut Catalog, remote: Option<&MemServicePoolProvider>, root: &Path) -> Result<Option<File>, String> {
    let Some(provider) = remote else { return Ok(None); };
    let Some(config) = provider.reconciliation.as_ref() else { return Ok(None); };
    #[cfg(any(test, feature = "experimental-reconciliation"))]
    {
        let loaded = provider.loaded();
        let authority = if config.get("terminal_c101_execution_manifest").is_some() {
            crate::pool_terminal_domain::case_chain::current_c101::restore_staged(catalog, config, &loaded)
        } else if config.get("terminal_case_execution_manifest").is_some() {
            crate::pool_terminal_domain::case_chain::restore_staged(catalog, config, &loaded)
        } else if config.get("terminal_c064_execution_manifest").is_some() {
            crate::pool_terminal_domain::current::restore_staged(catalog, config, &loaded)
        } else if config.get("terminal_c044_execution_manifest").is_some() {
            crate::pool_terminal_domain::restore_staged(catalog, config, &loaded)
        } else {
            crate::pool_reconciliation::restore(catalog, config, root)
        };
        authority.map(Some)
    }
    #[cfg(not(any(test, feature = "experimental-reconciliation")))]
    {
        let _ = (catalog, config, root);
        Err(FEATURE_ERROR.into())
    }
}

pub(crate) fn private_command(args: &[String]) -> Option<Result<Value, String>> {
    let selected = args.iter().any(|arg| arg.starts_with("--pool-terminal-") || arg == "--pool-shutdown-report");
    if !selected { return None; }
    if !cfg!(feature = "experimental-reconciliation") {
        return Some(Err(FEATURE_ERROR.into()));
    }
    #[cfg(feature = "experimental-reconciliation")]
    { Some(dispatch(args)) }
    #[cfg(not(feature = "experimental-reconciliation"))]
    { Some(Err(FEATURE_ERROR.into())) }
}

#[cfg(feature = "experimental-reconciliation")]
fn dispatch(args: &[String]) -> Result<Value, String> {
    use crate::pool_terminal_domain as terminal;
    match args {
        [mode,path,sha] if mode=="--pool-terminal-c101-manifest"=>terminal::case_chain::current_c101::production_manifest(Path::new(path),sha),
        [mode,path,sha] if mode=="--pool-terminal-c101-consumption-audit"=>terminal::case_chain::current_c101::production_audit_consumed(Path::new(path),sha),
        [mode] if mode=="--pool-terminal-c101-verify"=>terminal::case_chain::current_c101::production_verify_readonly(),
        [mode,path,sha] if mode=="--pool-terminal-case-manifest"=>terminal::case_chain::production_manifest(Path::new(path),sha),
        [mode,path,sha] if mode=="--pool-terminal-case-consumption-audit"=>terminal::case_chain::production_audit_consumed(Path::new(path),sha),
        [mode] if mode=="--pool-terminal-case-verify"=>terminal::case_chain::production_verify_readonly(),
        [mode] if mode=="--pool-terminal-domain-loss"=>terminal::production_terminal_only(),
        [mode,path,sha] if mode=="--pool-terminal-current-manifest"=>terminal::current::production_manifest(Path::new(path),sha),
        [mode] if mode=="--pool-terminal-current-verify"=>terminal::current::production_verify_readonly(),
        [mode,path,sha,phase] if mode=="--pool-terminal-current-execution-check"=>terminal::current::production_execution_check(Path::new(path),sha,phase),
        [mode] if mode=="--pool-terminal-domain-verify"=>terminal::production_verify_readonly(),
        [mode] if mode=="--pool-shutdown-report"=>terminal::production_shutdown_report(),
        [mode,manifest,path,pin,sha] if mode=="--pool-terminal-domain-loss"&&manifest=="--execution-manifest"&&pin=="--execution-manifest-sha256"=>terminal::production_terminal_manifest(Path::new(path),sha),
        [mode,manifest,path,pin,sha,phase_flag,phase] if mode=="--pool-terminal-execution-check"&&manifest=="--execution-manifest"&&pin=="--execution-manifest-sha256"&&phase_flag=="--phase"=>terminal::production_execution_check(Path::new(path),sha,phase),
        _=>Err("production-not-ready: private mode requires its exact read-only flag or resolved execution manifest; no original action".into())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn default_private_commands_reject_before_proof_or_state_io() {
        if cfg!(feature = "experimental-reconciliation") { return; }
        for args in [vec!["--pool-terminal-domain-loss"], vec!["--pool-terminal-c101-manifest", "/missing-proof", "bad-hash"], vec!["--pool-shutdown-report"]] {
            let owned = args.iter().map(|arg| arg.to_string()).collect::<Vec<_>>();
            assert_eq!(super::private_command(&owned).unwrap().unwrap_err(), super::FEATURE_ERROR);
        }
        assert!(super::private_command(&["--host-pool".into()]).is_none());
    }
}
