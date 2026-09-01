use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use uf_core::provider::{
    Addressability, DataProvider, Fidelity, ProviderAccessRef, ProviderCapabilities,
    ProviderCompletionResult, ProviderCompletionSnapshot, ProviderCompletionState,
    ProviderDescriptor, ProviderError, ProviderErrorCode, ProviderHealth, ProviderIdentity,
    ProviderKind, ProviderOperation, ProviderOperationKind, ProviderResourceRef,
    ProviderSubmission, PROVIDER_API_MAJOR, PROVIDER_API_MINOR,
};

const MOCK_DESCRIPTOR_MAJOR: u16 = 1;

struct MockResource {
    generation: u64,
    bytes: Vec<u8>,
}

struct MockAccess {
    generation: u64,
    source_resource_id: String,
    source_generation: u64,
    allowed_offset_bytes: u64,
    allowed_bytes: u64,
    access_mode: String,
}

struct MockState {
    next_resource_id: u64,
    next_access_id: u64,
    next_completion_id: u64,
    resources: HashMap<String, MockResource>,
    accesses: HashMap<String, MockAccess>,
    completions: HashMap<String, ProviderCompletionSnapshot>,
    counters: BTreeMap<String, u64>,
}

pub(crate) struct MockProvider {
    identity: ProviderIdentity,
    capabilities: ProviderCapabilities,
    state: Mutex<MockState>,
}

impl MockProvider {
    pub(crate) fn new(provider_id: &str, execution_realm_id: &str, node_id: &str) -> Self {
        let supported_operations = [
            ProviderOperationKind::Allocate,
            ProviderOperationKind::Release,
            ProviderOperationKind::Publish,
            ProviderOperationKind::Import,
            ProviderOperationKind::Revoke,
            ProviderOperationKind::Checksum,
            ProviderOperationKind::Copy,
            ProviderOperationKind::Write,
        ]
        .into_iter()
        .collect::<BTreeSet<_>>();
        Self {
            identity: ProviderIdentity {
                provider_api_major: PROVIDER_API_MAJOR,
                provider_api_minor: PROVIDER_API_MINOR,
                provider_id: provider_id.to_string(),
                provider_kind: ProviderKind::Mock,
                adapter_revision: env!("CARGO_PKG_VERSION").to_string(),
                external_platform_revision: "mock".to_string(),
                external_protocol_revision: "mock-provider-v1".to_string(),
                execution_realm_id: execution_realm_id.to_string(),
                node_id: node_id.to_string(),
                session_id: format!("{}:{}:session-1", provider_id, node_id),
                boot_id: format!("{}:boot-1", node_id),
                session_generation: 1,
                fidelity: Fidelity::Semantic,
                limitations: vec!["contract-test-only".to_string()],
            },
            capabilities: ProviderCapabilities {
                supported_operations,
                max_transfer_bytes: 16 * 1024 * 1024,
                alignment_bytes: 1,
                queue_depth: 1024,
                addressability: Addressability::ProviderOnly,
                limitations: vec!["not-a-data-plane".to_string()],
            },
            state: Mutex::new(MockState {
                next_resource_id: 1,
                next_access_id: 1,
                next_completion_id: 1,
                resources: HashMap::new(),
                accesses: HashMap::new(),
                completions: HashMap::new(),
                counters: BTreeMap::new(),
            }),
        }
    }

    fn check_range(total: u64, offset: u64, nbytes: u64) -> Result<(), ProviderError> {
        if nbytes == 0 || offset.checked_add(nbytes).is_none() || offset + nbytes > total {
            return Err(ProviderError::new(
                ProviderErrorCode::RangeOutOfBounds,
                format!("range offset={} bytes={} exceeds {}", offset, nbytes, total),
            ));
        }
        Ok(())
    }

    fn resource_ref(id: &str, resource: &MockResource) -> ProviderResourceRef {
        ProviderResourceRef {
            provider_resource_id: id.to_string(),
            resource_generation: resource.generation,
            size_bytes: resource.bytes.len() as u64,
            allowed_offset_bytes: 0,
            allowed_bytes: resource.bytes.len() as u64,
            addressability: Addressability::ProviderOnly,
            opaque_token: Vec::new(),
        }
    }

    fn checksum(bytes: &[u8]) -> u64 {
        bytes.iter().fold(0xcbf29ce484222325u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
        })
    }

    fn decode_resource_id(descriptor: &ProviderDescriptor) -> Result<String, ProviderError> {
        if descriptor.schema_major != MOCK_DESCRIPTOR_MAJOR {
            return Err(ProviderError::new(
                ProviderErrorCode::ProviderVersionMismatch,
                "mock descriptor major mismatch",
            ));
        }
        String::from_utf8(descriptor.opaque_bytes.clone()).map_err(|_| {
            ProviderError::new(
                ProviderErrorCode::DescriptorMalformed,
                "mock descriptor resource id is not UTF-8",
            )
        })
    }

    fn execute(
        &self,
        state: &mut MockState,
        operation: ProviderOperation,
    ) -> Result<ProviderCompletionResult, ProviderError> {
        match operation {
            ProviderOperation::Allocate { size_bytes, .. } => {
                if size_bytes == 0 || size_bytes > self.capabilities.max_transfer_bytes {
                    return Err(ProviderError::new(
                        ProviderErrorCode::RangeOutOfBounds,
                        "mock allocation size is invalid",
                    ));
                }
                let id = format!("resource-{}", state.next_resource_id);
                state.next_resource_id += 1;
                let resource = MockResource {
                    generation: 1,
                    bytes: vec![0; size_bytes as usize],
                };
                let reference = Self::resource_ref(&id, &resource);
                state.resources.insert(id, resource);
                Ok(ProviderCompletionResult::Resource(reference))
            }
            ProviderOperation::Release { resource } => {
                let current = state
                    .resources
                    .get(&resource.provider_resource_id)
                    .ok_or_else(|| {
                        ProviderError::new(
                            ProviderErrorCode::ProviderResourceNotFound,
                            "mock resource does not exist",
                        )
                    })?;
                if current.generation != resource.resource_generation {
                    return Err(ProviderError::new(
                        ProviderErrorCode::ProviderResourceStale,
                        "mock resource generation is stale",
                    ));
                }
                if state.accesses.values().any(|access| {
                    access.source_resource_id == resource.provider_resource_id
                        && access.source_generation == resource.resource_generation
                }) {
                    return Err(ProviderError::new(
                        ProviderErrorCode::AccessDenied,
                        "mock resource still has an active access binding",
                    ));
                }
                state.resources.remove(&resource.provider_resource_id);
                Ok(ProviderCompletionResult::Unit)
            }
            ProviderOperation::InitializeDeterministic {
                resource,
                offset_bytes,
                nbytes,
                seed_id,
            } => {
                let current = state
                    .resources
                    .get_mut(&resource.provider_resource_id)
                    .ok_or_else(|| {
                        ProviderError::new(
                            ProviderErrorCode::ProviderResourceNotFound,
                            "mock seed resource does not exist",
                        )
                    })?;
                if current.generation != resource.resource_generation {
                    return Err(ProviderError::new(
                        ProviderErrorCode::ProviderResourceStale,
                        "mock seed resource generation is stale",
                    ));
                }
                Self::check_range(current.bytes.len() as u64, offset_bytes, nbytes)?;
                for (index, byte) in current.bytes
                    [offset_bytes as usize..(offset_bytes + nbytes) as usize]
                    .iter_mut()
                    .enumerate()
                {
                    *byte = (seed_id as u8).wrapping_add((index as u8).wrapping_mul(29));
                }
                Ok(ProviderCompletionResult::Bytes {
                    completed_bytes: nbytes,
                })
            }
            ProviderOperation::Publish {
                resource,
                offset_bytes,
                nbytes,
                access_mode,
            } => {
                let current = state
                    .resources
                    .get(&resource.provider_resource_id)
                    .ok_or_else(|| {
                        ProviderError::new(
                            ProviderErrorCode::ProviderResourceNotFound,
                            "mock publish resource does not exist",
                        )
                    })?;
                if current.generation != resource.resource_generation {
                    return Err(ProviderError::new(
                        ProviderErrorCode::ProviderResourceStale,
                        "mock publish generation is stale",
                    ));
                }
                Self::check_range(current.bytes.len() as u64, offset_bytes, nbytes)?;
                Ok(ProviderCompletionResult::Descriptor(ProviderDescriptor {
                    schema_major: MOCK_DESCRIPTOR_MAJOR,
                    schema_minor: 0,
                    provider_id: self.identity.provider_id.clone(),
                    provider_session_id: self.identity.session_id.clone(),
                    resource_generation: current.generation,
                    allowed_offset_bytes: offset_bytes,
                    allowed_bytes: nbytes,
                    access_mode,
                    opaque_bytes: resource.provider_resource_id.into_bytes(),
                }))
            }
            ProviderOperation::Import {
                descriptor,
                offset_bytes,
                nbytes,
                access_mode,
                ..
            } => {
                if descriptor.provider_id != self.identity.provider_id
                    || descriptor.provider_session_id != self.identity.session_id
                {
                    return Err(ProviderError::new(
                        ProviderErrorCode::ProviderSessionRestarted,
                        "mock descriptor belongs to another provider session",
                    ));
                }
                if access_mode != "read" || descriptor.access_mode != "read" {
                    return Err(ProviderError::new(
                        ProviderErrorCode::AccessDenied,
                        "PhaseC-02 mock import is read-only",
                    ));
                }
                let source_resource_id = Self::decode_resource_id(&descriptor)?;
                let source = state.resources.get(&source_resource_id).ok_or_else(|| {
                    ProviderError::new(
                        ProviderErrorCode::ProviderResourceNotFound,
                        "mock descriptor source no longer exists",
                    )
                })?;
                if source.generation != descriptor.resource_generation {
                    return Err(ProviderError::new(
                        ProviderErrorCode::ProviderResourceStale,
                        "mock descriptor resource generation is stale",
                    ));
                }
                Self::check_range(source.bytes.len() as u64, offset_bytes, nbytes)?;
                if offset_bytes < descriptor.allowed_offset_bytes
                    || offset_bytes + nbytes
                        > descriptor.allowed_offset_bytes + descriptor.allowed_bytes
                {
                    return Err(ProviderError::new(
                        ProviderErrorCode::RangeOutOfBounds,
                        "mock import exceeds descriptor range",
                    ));
                }
                let id = format!("access-{}", state.next_access_id);
                state.next_access_id += 1;
                let access = MockAccess {
                    generation: 1,
                    source_resource_id,
                    source_generation: source.generation,
                    allowed_offset_bytes: offset_bytes,
                    allowed_bytes: nbytes,
                    access_mode: access_mode.clone(),
                };
                let result = ProviderAccessRef {
                    accessor_provider_id: self.identity.provider_id.clone(),
                    accessor_provider_session_id: self.identity.session_id.clone(),
                    accessor_resource_id: id.clone(),
                    access_generation: access.generation,
                    source_resource_generation: access.source_generation,
                    allowed_offset_bytes: access.allowed_offset_bytes,
                    allowed_bytes: access.allowed_bytes,
                    access_mode,
                    addressability: Addressability::RemoteMapping,
                    opaque_token: Vec::new(),
                };
                state.accesses.insert(id, access);
                Ok(ProviderCompletionResult::Access(result))
            }
            ProviderOperation::Revoke { access } => {
                let current = state
                    .accesses
                    .get(&access.accessor_resource_id)
                    .ok_or_else(|| {
                        ProviderError::new(
                            ProviderErrorCode::ProviderResourceNotFound,
                            "mock access does not exist",
                        )
                    })?;
                if current.generation != access.access_generation {
                    return Err(ProviderError::new(
                        ProviderErrorCode::ProviderResourceStale,
                        "mock access generation is stale",
                    ));
                }
                state.accesses.remove(&access.accessor_resource_id);
                Ok(ProviderCompletionResult::Unit)
            }
            ProviderOperation::ChecksumResource {
                resource,
                offset_bytes,
                nbytes,
            } => {
                let current = state
                    .resources
                    .get(&resource.provider_resource_id)
                    .ok_or_else(|| {
                        ProviderError::new(
                            ProviderErrorCode::ProviderResourceNotFound,
                            "mock checksum resource does not exist",
                        )
                    })?;
                if current.generation != resource.resource_generation {
                    return Err(ProviderError::new(
                        ProviderErrorCode::ProviderResourceStale,
                        "mock checksum resource is stale",
                    ));
                }
                Self::check_range(current.bytes.len() as u64, offset_bytes, nbytes)?;
                let range = &current.bytes[offset_bytes as usize..(offset_bytes + nbytes) as usize];
                Ok(ProviderCompletionResult::Checksum {
                    algorithm: "fnv1a64".to_string(),
                    value: Self::checksum(range),
                })
            }
            ProviderOperation::ChecksumAccess {
                access,
                offset_bytes,
                nbytes,
            } => {
                let current = state
                    .accesses
                    .get(&access.accessor_resource_id)
                    .ok_or_else(|| {
                        ProviderError::new(
                            ProviderErrorCode::ProviderResourceNotFound,
                            "mock checksum access does not exist",
                        )
                    })?;
                if current.generation != access.access_generation
                    || current.source_generation != access.source_resource_generation
                {
                    return Err(ProviderError::new(
                        ProviderErrorCode::ProviderResourceStale,
                        "mock checksum access is stale",
                    ));
                }
                if current.access_mode != "read" {
                    return Err(ProviderError::new(
                        ProviderErrorCode::AccessDenied,
                        "mock checksum access is not readable",
                    ));
                }
                Self::check_range(current.allowed_bytes, offset_bytes, nbytes)?;
                let source = state
                    .resources
                    .get(&current.source_resource_id)
                    .ok_or_else(|| {
                        ProviderError::new(
                            ProviderErrorCode::ProviderResourceNotFound,
                            "mock checksum source no longer exists",
                        )
                    })?;
                let start = current.allowed_offset_bytes + offset_bytes;
                let range = &source.bytes[start as usize..(start + nbytes) as usize];
                Ok(ProviderCompletionResult::Checksum {
                    algorithm: "fnv1a64".to_string(),
                    value: Self::checksum(range),
                })
            }
            ProviderOperation::Copy {
                source,
                destination,
                source_offset_bytes,
                destination_offset_bytes,
                nbytes,
            } => {
                if source.provider_resource_id == destination.provider_resource_id {
                    return Err(ProviderError::new(
                        ProviderErrorCode::ProviderOperationUnsupported,
                        "mock self-copy is not supported",
                    ));
                }
                let source_bytes = {
                    let current = state
                        .resources
                        .get(&source.provider_resource_id)
                        .ok_or_else(|| {
                            ProviderError::new(
                                ProviderErrorCode::ProviderResourceNotFound,
                                "mock copy source does not exist",
                            )
                        })?;
                    Self::check_range(current.bytes.len() as u64, source_offset_bytes, nbytes)?;
                    current.bytes
                        [source_offset_bytes as usize..(source_offset_bytes + nbytes) as usize]
                        .to_vec()
                };
                let destination_resource = state
                    .resources
                    .get_mut(&destination.provider_resource_id)
                    .ok_or_else(|| {
                        ProviderError::new(
                            ProviderErrorCode::ProviderResourceNotFound,
                            "mock copy destination does not exist",
                        )
                    })?;
                Self::check_range(
                    destination_resource.bytes.len() as u64,
                    destination_offset_bytes,
                    nbytes,
                )?;
                destination_resource.bytes[destination_offset_bytes as usize
                    ..(destination_offset_bytes + nbytes) as usize]
                    .copy_from_slice(&source_bytes);
                Ok(ProviderCompletionResult::Bytes {
                    completed_bytes: nbytes,
                })
            }
        }
    }
}

impl DataProvider for MockProvider {
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
        if !self.capabilities.supports(operation.kind()) {
            return Err(ProviderError::new(
                ProviderErrorCode::ProviderOperationUnsupported,
                format!(
                    "mock provider does not support {}",
                    operation.kind().as_str()
                ),
            ));
        }
        let mut state = self.state.lock().map_err(|_| {
            ProviderError::new(
                ProviderErrorCode::InternalProviderAdapterError,
                "mock provider state is poisoned",
            )
        })?;
        let completion_id = format!("completion-{}", state.next_completion_id);
        state.next_completion_id += 1;
        let operation_name = operation.kind().as_str().to_string();
        let snapshot = match self.execute(&mut state, operation) {
            Ok(result) => ProviderCompletionSnapshot {
                provider_completion_id: completion_id.clone(),
                state: ProviderCompletionState::Complete,
                result: Some(result),
                error: None,
            },
            Err(error) => ProviderCompletionSnapshot {
                provider_completion_id: completion_id.clone(),
                state: ProviderCompletionState::Failed,
                result: None,
                error: Some(error),
            },
        };
        snapshot.validate()?;
        *state.counters.entry(operation_name).or_insert(0) += 1;
        state.completions.insert(completion_id.clone(), snapshot);
        Ok(ProviderSubmission {
            provider_completion_id: completion_id,
            initial_state: ProviderCompletionState::Complete,
        })
    }

    fn poll_completion(
        &self,
        provider_completion_id: &str,
    ) -> Result<ProviderCompletionSnapshot, ProviderError> {
        self.state
            .lock()
            .map_err(|_| {
                ProviderError::new(
                    ProviderErrorCode::InternalProviderAdapterError,
                    "mock provider state is poisoned",
                )
            })?
            .completions
            .get(provider_completion_id)
            .cloned()
            .ok_or_else(|| {
                ProviderError::new(
                    ProviderErrorCode::ProviderResourceNotFound,
                    "mock completion does not exist",
                )
            })
    }

    fn wait_completion(
        &self,
        provider_completion_id: &str,
        timeout_ms: u64,
    ) -> Result<ProviderCompletionSnapshot, ProviderError> {
        let deadline = Instant::now() + Duration::from_millis(timeout_ms);
        loop {
            let snapshot = self.poll_completion(provider_completion_id)?;
            if snapshot.state.is_terminal() {
                return Ok(snapshot);
            }
            if Instant::now() >= deadline {
                return Err(ProviderError::new(
                    ProviderErrorCode::Timeout,
                    "mock completion wait timed out",
                ));
            }
            std::thread::yield_now();
        }
    }

    fn cancel_completion(&self, provider_completion_id: &str) -> Result<(), ProviderError> {
        let snapshot = self.poll_completion(provider_completion_id)?;
        if snapshot.state.is_terminal() {
            return Err(ProviderError::new(
                ProviderErrorCode::CancelUnsupported,
                "mock completion is already terminal",
            ));
        }
        Err(ProviderError::new(
            ProviderErrorCode::CancelUnsupported,
            "mock provider has no pending operations",
        ))
    }

    fn read_counters(&self) -> BTreeMap<String, u64> {
        self.state
            .lock()
            .map(|state| state.counters.clone())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn complete(provider: &MockProvider, operation: ProviderOperation) -> ProviderCompletionResult {
        let submission = provider.submit_operation(operation).unwrap();
        let snapshot = provider
            .poll_completion(&submission.provider_completion_id)
            .unwrap();
        assert_eq!(snapshot.state, ProviderCompletionState::Complete);
        snapshot.result.unwrap()
    }

    #[test]
    fn remote_map_is_an_access_not_a_resource_copy() {
        let provider = MockProvider::new("mock", "ub_sim:test", "nodeA");
        let resource = match complete(
            &provider,
            ProviderOperation::Allocate {
                size_bytes: 4096,
                alignment_bytes: 4096,
                medium: "ddr".to_string(),
                node_id: "nodeA".to_string(),
            },
        ) {
            ProviderCompletionResult::Resource(resource) => resource,
            other => panic!("unexpected allocation result: {other:?}"),
        };
        let descriptor = match complete(
            &provider,
            ProviderOperation::Publish {
                resource,
                offset_bytes: 0,
                nbytes: 4096,
                access_mode: "read".to_string(),
            },
        ) {
            ProviderCompletionResult::Descriptor(descriptor) => descriptor,
            other => panic!("unexpected publish result: {other:?}"),
        };
        let access = match complete(
            &provider,
            ProviderOperation::Import {
                descriptor,
                accessor_node_id: "nodeB".to_string(),
                offset_bytes: 0,
                nbytes: 4096,
                access_mode: "read".to_string(),
            },
        ) {
            ProviderCompletionResult::Access(access) => access,
            other => panic!("unexpected import result: {other:?}"),
        };
        assert_eq!(access.allowed_bytes, 4096);
        assert!(access.accessor_resource_id.starts_with("access-"));
    }

    #[test]
    fn bad_descriptor_session_fails_in_its_single_completion() {
        let provider = MockProvider::new("mock", "ub_sim:test", "nodeA");
        let submission = provider
            .submit_operation(ProviderOperation::Import {
                descriptor: ProviderDescriptor {
                    schema_major: 1,
                    schema_minor: 0,
                    provider_id: "mock".to_string(),
                    provider_session_id: "stale".to_string(),
                    resource_generation: 1,
                    allowed_offset_bytes: 0,
                    allowed_bytes: 4096,
                    access_mode: "read".to_string(),
                    opaque_bytes: b"resource-1".to_vec(),
                },
                accessor_node_id: "nodeB".to_string(),
                offset_bytes: 0,
                nbytes: 4096,
                access_mode: "read".to_string(),
            })
            .unwrap();
        let snapshot = provider
            .poll_completion(&submission.provider_completion_id)
            .unwrap();
        assert_eq!(snapshot.state, ProviderCompletionState::Failed);
        assert_eq!(
            snapshot.error.unwrap().code,
            ProviderErrorCode::ProviderSessionRestarted
        );
    }
}
