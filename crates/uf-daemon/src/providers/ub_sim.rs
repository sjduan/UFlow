use super::MockProvider;
use std::collections::BTreeMap;
use std::collections::{BTreeSet, HashMap};
use std::env;
use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use uf_core::bridge::{BridgeFrame, BridgeMessageType};
use uf_core::provider::{
    Addressability, DataProvider, Fidelity, ProviderAccessRef, ProviderCapabilities,
    ProviderCompletionResult, ProviderCompletionSnapshot, ProviderCompletionState,
    ProviderDescriptor, ProviderError, ProviderErrorCode, ProviderHealth, ProviderIdentity,
    ProviderKind, ProviderOperation, ProviderOperationKind, ProviderResourceRef,
    ProviderSubmission, PROVIDER_API_MAJOR, PROVIDER_API_MINOR,
};
use uf_ub_sim_agent::MockCompanion;

struct GuestConnection {
    node_id: String,
    boot_id: String,
    session_id: String,
    session_generation: u64,
    stream: UnixStream,
}

#[derive(Clone)]
struct RealResource {
    node_id: String,
    provider_resource: ProviderResourceRef,
    external_resource_id: u64,
    external_session_generation: u64,
    seed_id: Option<u64>,
    checksum: Option<u64>,
    descriptor: Option<ProviderDescriptor>,
}

#[derive(Clone)]
struct RealAccess {
    node_id: String,
    provider_access: ProviderAccessRef,
    external_access_id: u64,
    external_session_generation: u64,
}

struct RealState {
    guests: HashMap<String, GuestConnection>,
    resources: HashMap<String, RealResource>,
    accesses: HashMap<String, RealAccess>,
    completions: HashMap<String, ProviderCompletionSnapshot>,
    counters: BTreeMap<String, u64>,
    next_completion_id: u64,
    next_access_id: u64,
}

pub(crate) struct UbSimProvider {
    identity: ProviderIdentity,
    capabilities: ProviderCapabilities,
    state: Mutex<RealState>,
    next_request_id: AtomicU64,
}

fn body(items: &[(&str, String)]) -> Vec<u8> {
    items
        .iter()
        .map(|(key, value)| format!("{}={}", key, value))
        .collect::<Vec<_>>()
        .join(";")
        .into_bytes()
}

fn hex_encode(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0xf) as usize] as char);
    }
    out
}

fn hex_decode(text: &str) -> Result<Vec<u8>, ProviderError> {
    if text.len() % 2 != 0 {
        return Err(ProviderError::new(
            ProviderErrorCode::DescriptorMalformed,
            "descriptor hex length is odd",
        ));
    }
    text.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).map_err(|_| {
                ProviderError::new(
                    ProviderErrorCode::DescriptorMalformed,
                    "descriptor hex is not UTF-8",
                )
            })?;
            u8::from_str_radix(text, 16).map_err(|_| {
                ProviderError::new(
                    ProviderErrorCode::DescriptorMalformed,
                    "descriptor hex contains a non-hex byte",
                )
            })
        })
        .collect()
}

fn parse_u64(
    values: &uf_core::Kv,
    key: &str,
    code: ProviderErrorCode,
) -> Result<u64, ProviderError> {
    let text = uf_core::get(values, key);
    text.parse::<u64>().map_err(|_| {
        ProviderError::new(code, format!("Guest response is missing numeric {}", key))
    })
}

fn bridge_error(response: &BridgeFrame) -> ProviderError {
    let values = uf_core::decode(std::str::from_utf8(&response.body).unwrap_or(""));
    let detail = uf_core::get(&values, "detail");
    let code = match response.error_code {
        -1002 => ProviderErrorCode::ProviderVersionMismatch,
        -1003 => ProviderErrorCode::ProviderUnavailable,
        -1004 => ProviderErrorCode::ProviderResourceNotFound,
        -1005 | -1009 => ProviderErrorCode::ProviderResourceStale,
        -1006 => ProviderErrorCode::AccessDenied,
        -1007 => ProviderErrorCode::RangeOutOfBounds,
        -1008 => ProviderErrorCode::DescriptorMalformed,
        -1012 => ProviderErrorCode::ChecksumMismatch,
        _ => ProviderErrorCode::InternalProviderAdapterError,
    };
    ProviderError {
        code,
        detail: if detail.is_empty() {
            "Guest provider operation failed".to_string()
        } else {
            detail.to_string()
        },
        external_code: Some(response.error_code as i64),
    }
}

fn capture_bridge_frame(direction: u8, frame: &BridgeFrame) {
    let path = match env::var("UF_UB_SIM_BRIDGE_CAPTURE") {
        Ok(path) if !path.is_empty() => path,
        _ => return,
    };
    let encoded = match frame.encode() {
        Ok(encoded) => encoded,
        Err(_) => return,
    };
    let mut record = Vec::with_capacity(encoded.len() + 13);
    record.extend_from_slice(b"UFCAP01");
    record.push(direction);
    record.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
    record.extend_from_slice(&encoded);
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = file.write_all(&record);
    }
}

impl UbSimProvider {
    pub(crate) fn from_env() -> Result<Option<Self>, ProviderError> {
        let node_a = env::var("UF_UB_SIM_NODE_A_SOCKET").ok();
        let node_b = env::var("UF_UB_SIM_NODE_B_SOCKET").ok();
        if node_a.is_none() && node_b.is_none() {
            return Ok(None);
        }
        if node_a.is_none() || node_b.is_none() {
            return Err(ProviderError::new(
                ProviderErrorCode::ProviderNotReady,
                "both UF_UB_SIM_NODE_A_SOCKET and UF_UB_SIM_NODE_B_SOCKET are required",
            ));
        }
        let realm = env::var("UF_UB_SIM_REALM_ID")
            .unwrap_or_else(|_| "simulated:ub_sim:default".to_string());
        let mut guests = HashMap::new();
        for (node_id, socket_path) in [
            ("nodeA", node_a.unwrap()),
            ("nodeB", node_b.unwrap()),
        ] {
            let mut stream = UnixStream::connect(&socket_path).map_err(|error| {
                ProviderError::new(
                    ProviderErrorCode::ProviderUnavailable,
                    format!("connect {} bridge {}: {}", node_id, socket_path, error),
                )
            })?;
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(30)))
                .map_err(|error| {
                    ProviderError::new(
                        ProviderErrorCode::InternalProviderAdapterError,
                        error.to_string(),
                    )
                })?;
            let request = BridgeFrame::request(
                BridgeMessageType::Identity,
                1,
                1,
                realm.clone(),
                node_id,
                "bootstrap",
                "bootstrap",
                1,
                body(&[("op", "identity".to_string())]),
            );
            capture_bridge_frame(1, &request);
            request.write_to(&mut stream).map_err(|error| {
                ProviderError::new(
                    ProviderErrorCode::ProviderProtocolError,
                    format!("identity write failed: {:?}", error),
                )
            })?;
            let response = BridgeFrame::read_from(&mut stream).map_err(|error| {
                ProviderError::new(
                    ProviderErrorCode::ProviderProtocolError,
                    format!("identity read failed: {:?}", error),
                )
            })?;
            capture_bridge_frame(2, &response);
            if response.message_type != BridgeMessageType::Completion || response.status != 0 {
                return Err(bridge_error(&response));
            }
            let values = uf_core::decode(std::str::from_utf8(&response.body).unwrap_or(""));
            if uf_core::get(&values, "provider") != "obmm"
                || uf_core::get(&values, "api_major") != "1"
                || uf_core::get(&values, "abi_major") != "1"
            {
                return Err(ProviderError::new(
                    ProviderErrorCode::ProviderVersionMismatch,
                    format!("{} companion identity is incompatible", node_id),
                ));
            }
            guests.insert(
                node_id.to_string(),
                GuestConnection {
                    node_id: node_id.to_string(),
                    boot_id: response.boot_id,
                    session_id: response.provider_session_id,
                    session_generation: response.session_generation,
                    stream,
                },
            );
        }
        let supported_operations = [
            ProviderOperationKind::Allocate,
            ProviderOperationKind::Release,
            ProviderOperationKind::Write,
            ProviderOperationKind::Publish,
            ProviderOperationKind::Import,
            ProviderOperationKind::Revoke,
            ProviderOperationKind::Checksum,
        ]
        .into_iter()
        .collect::<BTreeSet<_>>();
        Ok(Some(Self {
            identity: ProviderIdentity {
                provider_api_major: PROVIDER_API_MAJOR,
                provider_api_minor: PROVIDER_API_MINOR,
                provider_id: "ub_sim".to_string(),
                provider_kind: ProviderKind::Simulator,
                adapter_revision: env!("CARGO_PKG_VERSION").to_string(),
                external_platform_revision: env::var("UF_UB_SIM_PLATFORM_REVISION")
                    .unwrap_or_else(|_| "unknown".to_string()),
                external_protocol_revision: "uflow-sim-bridge-v1+obmm-sdk-v1".to_string(),
                execution_realm_id: realm.clone(),
                node_id: "nodeA+nodeB".to_string(),
                session_id: format!("ub_sim:{}:host", realm),
                boot_id: "host-adapter".to_string(),
                session_generation: 1,
                fidelity: Fidelity::Functional,
                limitations: vec![
                    "fixed-2node".to_string(),
                    "readonly-remote-map".to_string(),
                    "fixed-4096-byte-range".to_string(),
                ],
            },
            capabilities: ProviderCapabilities {
                supported_operations,
                max_transfer_bytes: 4096,
                alignment_bytes: 1,
                queue_depth: 64,
                addressability: Addressability::ProviderOnly,
                limitations: vec!["semantic-functional-simulation".to_string()],
            },
            state: Mutex::new(RealState {
                guests,
                resources: HashMap::new(),
                accesses: HashMap::new(),
                completions: HashMap::new(),
                counters: BTreeMap::new(),
                next_completion_id: 1,
                next_access_id: 1,
            }),
            next_request_id: AtomicU64::new(2),
        }))
    }

    fn call_guest(
        &self,
        state: &mut RealState,
        node_id: &str,
        message_type: BridgeMessageType,
        request_body: Vec<u8>,
    ) -> Result<uf_core::Kv, ProviderError> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let guest = state.guests.get_mut(node_id).ok_or_else(|| {
            ProviderError::new(
                ProviderErrorCode::ProviderUnavailable,
                format!("Guest {} is not connected", node_id),
            )
        })?;
        let request = BridgeFrame::request(
            message_type,
            request_id,
            request_id,
            self.identity.execution_realm_id.clone(),
            guest.node_id.clone(),
            guest.boot_id.clone(),
            guest.session_id.clone(),
            guest.session_generation,
            request_body,
        );
        capture_bridge_frame(1, &request);
        request.write_to(&mut guest.stream).map_err(|error| {
            ProviderError::new(
                ProviderErrorCode::ProviderProtocolError,
                format!("bridge write failed: {:?}", error),
            )
        })?;
        let response = BridgeFrame::read_from(&mut guest.stream).map_err(|error| {
            ProviderError::new(
                ProviderErrorCode::ProviderProtocolError,
                format!("bridge read failed: {:?}", error),
            )
        })?;
        capture_bridge_frame(2, &response);
        if response.request_id != request_id
            || response.correlation_id != request_id
            || response.provider_session_id != guest.session_id
            || response.session_generation != guest.session_generation
        {
            return Err(ProviderError::new(
                ProviderErrorCode::ProviderProtocolError,
                "Guest completion identity does not match its request",
            ));
        }
        *state
            .counters
            .entry(format!("bridge.{}.{}", node_id, message_type as u16))
            .or_insert(0) += 1;
        if response.message_type == BridgeMessageType::Error || response.status != 0 {
            return Err(bridge_error(&response));
        }
        if response.message_type != BridgeMessageType::Completion {
            return Err(ProviderError::new(
                ProviderErrorCode::ProviderProtocolError,
                "Guest provider operation did not return one terminal completion",
            ));
        }
        Ok(uf_core::decode(
            std::str::from_utf8(&response.body).unwrap_or(""),
        ))
    }

    fn external_resource_body(resource: &RealResource) -> Vec<(&'static str, String)> {
        vec![
            (
                "resource_id",
                resource.external_resource_id.to_string(),
            ),
            (
                "resource_generation",
                resource.provider_resource.resource_generation.to_string(),
            ),
            (
                "resource_session_generation",
                resource.external_session_generation.to_string(),
            ),
            (
                "resource_size",
                resource.provider_resource.size_bytes.to_string(),
            ),
        ]
    }

    fn external_access_body(access: &RealAccess) -> Vec<(&'static str, String)> {
        vec![
            ("access_id", access.external_access_id.to_string()),
            (
                "access_generation",
                access.provider_access.access_generation.to_string(),
            ),
            (
                "access_session_generation",
                access.external_session_generation.to_string(),
            ),
            (
                "access_offset",
                access.provider_access.allowed_offset_bytes.to_string(),
            ),
            (
                "access_nbytes",
                access.provider_access.allowed_bytes.to_string(),
            ),
        ]
    }

    fn execute(
        &self,
        state: &mut RealState,
        operation: ProviderOperation,
    ) -> Result<ProviderCompletionResult, ProviderError> {
        match operation {
            ProviderOperation::Allocate {
                size_bytes,
                medium,
                node_id,
                ..
            } => {
                if medium != "ddr" || size_bytes != 4096 || node_id != "nodeA" {
                    return Err(ProviderError::new(
                        ProviderErrorCode::ProviderOperationUnsupported,
                        "C02 real slice requires nodeA DDR with exactly 4096 bytes",
                    ));
                }
                let values = self.call_guest(
                    state,
                    &node_id,
                    BridgeMessageType::Allocate,
                    body(&[("size", size_bytes.to_string())]),
                )?;
                let external_id = parse_u64(
                    &values,
                    "resource_id",
                    ProviderErrorCode::ProviderProtocolError,
                )?;
                let generation = parse_u64(
                    &values,
                    "resource_generation",
                    ProviderErrorCode::ProviderProtocolError,
                )?;
                let session_generation = parse_u64(
                    &values,
                    "resource_session_generation",
                    ProviderErrorCode::ProviderProtocolError,
                )?;
                let resource_id = format!("{}:{}", node_id, external_id);
                let provider_resource = ProviderResourceRef {
                    provider_resource_id: resource_id.clone(),
                    resource_generation: generation,
                    size_bytes,
                    allowed_offset_bytes: 0,
                    allowed_bytes: size_bytes,
                    addressability: Addressability::ProviderOnly,
                    opaque_token: Vec::new(),
                };
                state.resources.insert(
                    resource_id,
                    RealResource {
                        node_id,
                        provider_resource: provider_resource.clone(),
                        external_resource_id: external_id,
                        external_session_generation: session_generation,
                        seed_id: None,
                        checksum: None,
                        descriptor: None,
                    },
                );
                Ok(ProviderCompletionResult::Resource(provider_resource))
            }
            ProviderOperation::InitializeDeterministic {
                resource,
                offset_bytes,
                nbytes,
                seed_id,
            } => {
                let current = state
                    .resources
                    .get(&resource.provider_resource_id)
                    .cloned()
                    .ok_or_else(|| {
                        ProviderError::new(
                            ProviderErrorCode::ProviderResourceNotFound,
                            "ub_sim resource does not exist",
                        )
                    })?;
                let mut items = Self::external_resource_body(&current);
                items.extend([
                    ("offset", offset_bytes.to_string()),
                    ("nbytes", nbytes.to_string()),
                    ("seed", seed_id.to_string()),
                ]);
                let values = self.call_guest(
                    state,
                    &current.node_id,
                    BridgeMessageType::InitializeDeterministic,
                    body(&items),
                )?;
                let checksum = parse_u64(
                    &values,
                    "checksum",
                    ProviderErrorCode::ProviderProtocolError,
                )?;
                let descriptor = ProviderDescriptor {
                    schema_major: 1,
                    schema_minor: 0,
                    provider_id: "ub_sim".to_string(),
                    provider_session_id: state.guests[&current.node_id].session_id.clone(),
                    resource_generation: resource.resource_generation,
                    allowed_offset_bytes: offset_bytes,
                    allowed_bytes: nbytes,
                    access_mode: "read".to_string(),
                    opaque_bytes: hex_decode(uf_core::get(&values, "descriptor"))?,
                };
                let stored = state.resources.get_mut(&resource.provider_resource_id).unwrap();
                stored.seed_id = Some(seed_id);
                stored.checksum = Some(checksum);
                stored.descriptor = Some(descriptor);
                Ok(ProviderCompletionResult::Bytes {
                    completed_bytes: nbytes,
                })
            }
            ProviderOperation::ChecksumResource { resource, .. } => {
                let current = state
                    .resources
                    .get(&resource.provider_resource_id)
                    .ok_or_else(|| {
                        ProviderError::new(
                            ProviderErrorCode::ProviderResourceNotFound,
                            "ub_sim resource does not exist",
                        )
                    })?;
                let checksum = current.checksum.ok_or_else(|| {
                    ProviderError::new(
                        ProviderErrorCode::ProviderNotReady,
                        "ub_sim resource has not been initialized",
                    )
                })?;
                Ok(ProviderCompletionResult::Checksum {
                    algorithm: "fnv1a64".to_string(),
                    value: checksum,
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
                    .cloned()
                    .ok_or_else(|| {
                        ProviderError::new(
                            ProviderErrorCode::ProviderResourceNotFound,
                            "ub_sim resource does not exist",
                        )
                    })?;
                if access_mode != "read" {
                    return Err(ProviderError::new(
                        ProviderErrorCode::AccessDenied,
                        "C02 remote mapping is read-only",
                    ));
                }
                let seed_id = current.seed_id.ok_or_else(|| {
                    ProviderError::new(
                        ProviderErrorCode::ProviderNotReady,
                        "resource must be initialized before publish",
                    )
                })?;
                let mut items = Self::external_resource_body(&current);
                items.extend([
                    ("offset", offset_bytes.to_string()),
                    ("nbytes", nbytes.to_string()),
                    ("seed", seed_id.to_string()),
                ]);
                let values = self.call_guest(
                    state,
                    &current.node_id,
                    BridgeMessageType::Publish,
                    body(&items),
                )?;
                let descriptor = ProviderDescriptor {
                    schema_major: 1,
                    schema_minor: 0,
                    provider_id: "ub_sim".to_string(),
                    provider_session_id: state.guests[&current.node_id].session_id.clone(),
                    resource_generation: resource.resource_generation,
                    allowed_offset_bytes: offset_bytes,
                    allowed_bytes: nbytes,
                    access_mode,
                    opaque_bytes: hex_decode(uf_core::get(&values, "descriptor"))?,
                };
                state
                    .resources
                    .get_mut(&resource.provider_resource_id)
                    .unwrap()
                    .descriptor = Some(descriptor.clone());
                Ok(ProviderCompletionResult::Descriptor(descriptor))
            }
            ProviderOperation::Import {
                descriptor,
                accessor_node_id,
                offset_bytes,
                nbytes,
                access_mode,
            } => {
                if accessor_node_id != "nodeB" || access_mode != "read" {
                    return Err(ProviderError::new(
                        ProviderErrorCode::AccessDenied,
                        "C02 import requires nodeB read-only access",
                    ));
                }
                let values = self.call_guest(
                    state,
                    &accessor_node_id,
                    BridgeMessageType::Import,
                    body(&[
                        ("descriptor", hex_encode(&descriptor.opaque_bytes)),
                        (
                            "source_session_id",
                            descriptor.provider_session_id.clone(),
                        ),
                        (
                            "source_session_generation",
                            state.guests["nodeA"].session_generation.to_string(),
                        ),
                        ("offset", offset_bytes.to_string()),
                        ("nbytes", nbytes.to_string()),
                    ]),
                )?;
                let external_id = parse_u64(
                    &values,
                    "access_id",
                    ProviderErrorCode::ProviderProtocolError,
                )?;
                let generation = parse_u64(
                    &values,
                    "access_generation",
                    ProviderErrorCode::ProviderProtocolError,
                )?;
                let external_session_generation = parse_u64(
                    &values,
                    "access_session_generation",
                    ProviderErrorCode::ProviderProtocolError,
                )?;
                let accessor_resource_id = format!("nodeB:access:{}", state.next_access_id);
                state.next_access_id += 1;
                let access = ProviderAccessRef {
                    accessor_provider_id: "ub_sim".to_string(),
                    accessor_provider_session_id: state.guests["nodeB"].session_id.clone(),
                    accessor_resource_id: accessor_resource_id.clone(),
                    access_generation: generation,
                    source_resource_generation: descriptor.resource_generation,
                    allowed_offset_bytes: offset_bytes,
                    allowed_bytes: nbytes,
                    access_mode,
                    addressability: Addressability::RemoteMapping,
                    opaque_token: Vec::new(),
                };
                state.accesses.insert(
                    accessor_resource_id,
                    RealAccess {
                        node_id: accessor_node_id,
                        provider_access: access.clone(),
                        external_access_id: external_id,
                        external_session_generation,
                    },
                );
                Ok(ProviderCompletionResult::Access(access))
            }
            ProviderOperation::ChecksumAccess {
                access,
                offset_bytes,
                nbytes,
            } => {
                let current = state
                    .accesses
                    .get(&access.accessor_resource_id)
                    .cloned()
                    .ok_or_else(|| {
                        ProviderError::new(
                            ProviderErrorCode::ProviderResourceStale,
                            "ub_sim access binding is stale",
                        )
                    })?;
                let mut items = Self::external_access_body(&current);
                items.extend([
                    ("offset", offset_bytes.to_string()),
                    ("nbytes", nbytes.to_string()),
                    ("expected_checksum", "0".to_string()),
                ]);
                let values = self.call_guest(
                    state,
                    &current.node_id,
                    BridgeMessageType::Checksum,
                    body(&items),
                )?;
                Ok(ProviderCompletionResult::Checksum {
                    algorithm: "fnv1a64".to_string(),
                    value: parse_u64(
                        &values,
                        "checksum",
                        ProviderErrorCode::ProviderProtocolError,
                    )?,
                })
            }
            ProviderOperation::Revoke { access } => {
                let current = state
                    .accesses
                    .get(&access.accessor_resource_id)
                    .cloned()
                    .ok_or_else(|| {
                        ProviderError::new(
                            ProviderErrorCode::ProviderResourceStale,
                            "ub_sim access binding is stale",
                        )
                    })?;
                self.call_guest(
                    state,
                    &current.node_id,
                    BridgeMessageType::Revoke,
                    body(&Self::external_access_body(&current)),
                )?;
                state.accesses.remove(&access.accessor_resource_id);
                Ok(ProviderCompletionResult::Unit)
            }
            ProviderOperation::Release { resource } => {
                let current = state
                    .resources
                    .get(&resource.provider_resource_id)
                    .cloned()
                    .ok_or_else(|| {
                        ProviderError::new(
                            ProviderErrorCode::ProviderResourceStale,
                            "ub_sim resource is stale",
                        )
                    })?;
                self.call_guest(
                    state,
                    &current.node_id,
                    BridgeMessageType::Release,
                    body(&Self::external_resource_body(&current)),
                )?;
                state.resources.remove(&resource.provider_resource_id);
                Ok(ProviderCompletionResult::Unit)
            }
            ProviderOperation::Copy { .. } => Err(ProviderError::new(
                ProviderErrorCode::ProviderOperationUnsupported,
                "C02 ub_sim provider does not implement copy",
            )),
        }
    }
}

impl DataProvider for UbSimProvider {
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
        let mut state = self.state.lock().unwrap();
        let completion_id = format!("ub-sim-completion-{}", state.next_completion_id);
        state.next_completion_id += 1;
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
            .unwrap()
            .completions
            .get(provider_completion_id)
            .cloned()
            .ok_or_else(|| {
                ProviderError::new(
                    ProviderErrorCode::ProviderResourceNotFound,
                    "ub_sim completion does not exist",
                )
            })
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
            "ub_sim provider operations complete synchronously in C02",
        ))
    }

    fn read_counters(&self) -> BTreeMap<String, u64> {
        self.state.lock().unwrap().counters.clone()
    }
}

pub(crate) struct MockBridgeUbSimProvider {
    delegate: MockProvider,
    companion: Mutex<MockCompanion>,
    next_request_id: AtomicU64,
}

impl MockBridgeUbSimProvider {
    pub(crate) fn new() -> Self {
        Self {
            delegate: MockProvider::new("ub_sim_mock", "simulated:ub_sim:mock", "nodeA+nodeB"),
            companion: Mutex::new(MockCompanion::default()),
            next_request_id: AtomicU64::new(1),
        }
    }

    fn bridge_message_type(operation: &ProviderOperation) -> BridgeMessageType {
        match operation {
            ProviderOperation::Allocate { .. } => BridgeMessageType::Allocate,
            ProviderOperation::Release { .. } => BridgeMessageType::Release,
            ProviderOperation::InitializeDeterministic { .. } => {
                BridgeMessageType::InitializeDeterministic
            }
            ProviderOperation::Publish { .. } => BridgeMessageType::Publish,
            ProviderOperation::Import { .. } => BridgeMessageType::Import,
            ProviderOperation::Revoke { .. } => BridgeMessageType::Revoke,
            ProviderOperation::ChecksumResource { .. }
            | ProviderOperation::ChecksumAccess { .. } => BridgeMessageType::Checksum,
            ProviderOperation::Copy { .. } => BridgeMessageType::Error,
        }
    }

    fn bridge_accept(&self, operation: &ProviderOperation) -> Result<(), ProviderError> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let identity = self.identity();
        let request = BridgeFrame::request(
            Self::bridge_message_type(operation),
            request_id,
            request_id,
            identity.execution_realm_id,
            identity.node_id,
            identity.boot_id,
            identity.session_id,
            identity.session_generation,
            format!("operation={}", operation.kind().as_str()).into_bytes(),
        );
        let encoded = request.encode().map_err(|error| {
            ProviderError::new(
                ProviderErrorCode::ProviderProtocolError,
                format!("bridge encode failed: {:?}", error),
            )
        })?;
        let decoded = BridgeFrame::decode(&encoded).map_err(|error| {
            ProviderError::new(
                ProviderErrorCode::ProviderProtocolError,
                format!("bridge decode failed: {:?}", error),
            )
        })?;
        let response = self.companion.lock().unwrap().handle(decoded).map_err(|error| {
            ProviderError::new(
                ProviderErrorCode::ProviderProtocolError,
                format!("mock companion failed: {:?}", error),
            )
        })?;
        if response.message_type != BridgeMessageType::Accepted || response.status != 0 {
            return Err(ProviderError::new(
                ProviderErrorCode::ProviderProtocolError,
                "bridge did not return an accepted control acknowledgement",
            ));
        }
        Ok(())
    }
}

impl DataProvider for MockBridgeUbSimProvider {
    fn identity(&self) -> ProviderIdentity {
        let mut identity = self.delegate.identity();
        identity.provider_kind = ProviderKind::Simulator;
        identity.external_platform_revision = "ub_sim-mock".to_string();
        identity.external_protocol_revision = "uflow-sim-bridge-v1".to_string();
        identity.limitations = vec![
            "mock-bridge-contract-ready".to_string(),
            "not-obmm-data-path".to_string(),
        ];
        identity
    }

    fn capabilities(&self) -> ProviderCapabilities {
        let mut capabilities = self.delegate.capabilities();
        capabilities
            .limitations
            .push("loopback-control-only".to_string());
        capabilities
    }

    fn health(&self) -> ProviderHealth {
        self.delegate.health()
    }

    fn submit_operation(
        &self,
        operation: ProviderOperation,
    ) -> Result<ProviderSubmission, ProviderError> {
        self.bridge_accept(&operation)?;
        self.delegate.submit_operation(operation)
    }

    fn poll_completion(
        &self,
        provider_completion_id: &str,
    ) -> Result<ProviderCompletionSnapshot, ProviderError> {
        self.delegate.poll_completion(provider_completion_id)
    }

    fn wait_completion(
        &self,
        provider_completion_id: &str,
        timeout_ms: u64,
    ) -> Result<ProviderCompletionSnapshot, ProviderError> {
        self.delegate
            .wait_completion(provider_completion_id, timeout_ms)
    }

    fn cancel_completion(&self, provider_completion_id: &str) -> Result<(), ProviderError> {
        self.delegate.cancel_completion(provider_completion_id)
    }

    fn read_counters(&self) -> BTreeMap<String, u64> {
        let mut counters = self.delegate.read_counters();
        for (key, value) in self.companion.lock().unwrap().counters() {
            counters.insert(format!("bridge.{}", key), value);
        }
        counters
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uf_core::provider::{ProviderCompletionState, ProviderOperation};

    #[test]
    fn bridge_ack_is_not_the_provider_terminal_completion() {
        let provider = MockBridgeUbSimProvider::new();
        let submission = provider
            .submit_operation(ProviderOperation::Allocate {
                size_bytes: 4096,
                alignment_bytes: 1,
                medium: "ddr".to_string(),
                node_id: "nodeA".to_string(),
            })
            .unwrap();
        let completion = provider
            .wait_completion(&submission.provider_completion_id, 1_000)
            .unwrap();
        assert_eq!(completion.state, ProviderCompletionState::Complete);
        assert_eq!(provider.identity().provider_kind, ProviderKind::Simulator);
        assert_eq!(provider.read_counters().get("bridge.request.3"), Some(&1));
    }
}
