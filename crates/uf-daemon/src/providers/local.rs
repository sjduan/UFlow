use std::collections::{BTreeMap, BTreeSet};
use uf_core::provider::{
    Addressability, DataProvider, Fidelity, ProviderCapabilities, ProviderCompletionSnapshot,
    ProviderError, ProviderErrorCode, ProviderHealth, ProviderIdentity, ProviderKind,
    ProviderOperation, ProviderSubmission, PROVIDER_API_MAJOR, PROVIDER_API_MINOR,
};

pub(crate) struct LegacyLocalProvider {
    identity: ProviderIdentity,
    capabilities: ProviderCapabilities,
}

impl LegacyLocalProvider {
    pub(crate) fn new(provider_id: &str, medium: &str, addressability: Addressability) -> Self {
        let operations = BTreeSet::new();
        Self {
            identity: ProviderIdentity {
                provider_api_major: PROVIDER_API_MAJOR,
                provider_api_minor: PROVIDER_API_MINOR,
                provider_id: provider_id.to_string(),
                provider_kind: ProviderKind::Local,
                adapter_revision: env!("CARGO_PKG_VERSION").to_string(),
                external_platform_revision: String::new(),
                external_protocol_revision: "legacy-local-v1".to_string(),
                execution_realm_id: "physical:local".to_string(),
                node_id: "local".to_string(),
                session_id: format!("{}:process", provider_id),
                boot_id: "daemon-process".to_string(),
                session_generation: 1,
                fidelity: Fidelity::Physical,
                limitations: vec![
                    format!("medium={}", medium),
                    "resource-operations-use-legacy-adapter".to_string(),
                ],
            },
            capabilities: ProviderCapabilities {
                supported_operations: operations,
                max_transfer_bytes: u64::MAX,
                alignment_bytes: 1,
                queue_depth: 0,
                addressability,
                limitations: vec!["legacy-resource-adapter".to_string()],
            },
        }
    }
}

impl DataProvider for LegacyLocalProvider {
    fn identity(&self) -> ProviderIdentity {
        self.identity.clone()
    }

    fn capabilities(&self) -> ProviderCapabilities {
        self.capabilities.clone()
    }

    fn health(&self) -> ProviderHealth {
        ProviderHealth::Ready
    }

    fn submit_operation(
        &self,
        operation: ProviderOperation,
    ) -> Result<ProviderSubmission, ProviderError> {
        Err(ProviderError::new(
            ProviderErrorCode::ProviderOperationUnsupported,
            format!(
                "{} is still served by the physical compatibility adapter",
                operation.kind().as_str()
            ),
        ))
    }

    fn poll_completion(
        &self,
        _provider_completion_id: &str,
    ) -> Result<ProviderCompletionSnapshot, ProviderError> {
        Err(ProviderError::new(
            ProviderErrorCode::ProviderResourceNotFound,
            "local compatibility provider has no provider completion",
        ))
    }

    fn wait_completion(
        &self,
        provider_completion_id: &str,
        _timeout_ms: u64,
    ) -> Result<ProviderCompletionSnapshot, ProviderError> {
        self.poll_completion(provider_completion_id)
    }

    fn cancel_completion(&self, _provider_completion_id: &str) -> Result<(), ProviderError> {
        Err(ProviderError::new(
            ProviderErrorCode::CancelUnsupported,
            "local compatibility provider does not expose cancellation",
        ))
    }

    fn read_counters(&self) -> BTreeMap<String, u64> {
        BTreeMap::new()
    }
}
