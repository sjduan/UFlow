use std::collections::BTreeMap;
use std::sync::Arc;
use uf_core::provider::{
    DataProvider, ProviderError, ProviderErrorCode, ProviderHealth, ProviderIdentity,
};

pub(crate) struct ProviderRegistry {
    providers: BTreeMap<String, Arc<dyn DataProvider>>,
}

impl ProviderRegistry {
    pub(crate) fn new() -> Self {
        Self {
            providers: BTreeMap::new(),
        }
    }

    pub(crate) fn register(
        &mut self,
        provider: Arc<dyn DataProvider>,
    ) -> Result<(), ProviderError> {
        let identity = provider.identity();
        if identity.provider_id.is_empty() {
            return Err(ProviderError::new(
                ProviderErrorCode::ProviderProtocolError,
                "provider_id is required",
            ));
        }
        if self.providers.contains_key(&identity.provider_id) {
            return Err(ProviderError::new(
                ProviderErrorCode::ProviderProtocolError,
                format!("provider {} is already registered", identity.provider_id),
            ));
        }
        self.providers
            .insert(identity.provider_id.clone(), provider);
        Ok(())
    }

    pub(crate) fn find(&self, provider_id: &str) -> Option<Arc<dyn DataProvider>> {
        self.providers.get(provider_id).cloned()
    }

    pub(crate) fn require(
        &self,
        provider_id: &str,
    ) -> Result<Arc<dyn DataProvider>, ProviderError> {
        self.find(provider_id).ok_or_else(|| {
            ProviderError::new(
                ProviderErrorCode::ProviderUnavailable,
                format!("provider {} is not registered", provider_id),
            )
        })
    }

    pub(crate) fn identities(&self) -> Vec<ProviderIdentity> {
        self.providers
            .values()
            .map(|provider| provider.identity())
            .collect()
    }

    pub(crate) fn health_summary(&self) -> BTreeMap<String, ProviderHealth> {
        self.providers
            .iter()
            .map(|(provider_id, provider)| (provider_id.clone(), provider.health()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::MockProvider;

    #[test]
    fn duplicate_provider_ids_fail_closed() {
        let mut registry = ProviderRegistry::new();
        registry
            .register(Arc::new(MockProvider::new("mock", "realm", "nodeA")))
            .unwrap();
        let error = registry
            .register(Arc::new(MockProvider::new("mock", "realm", "nodeB")))
            .unwrap_err();
        assert_eq!(error.code, ProviderErrorCode::ProviderProtocolError);
    }
}
