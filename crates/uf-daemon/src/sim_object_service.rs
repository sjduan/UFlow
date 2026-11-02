use crate::catalog::SharedCatalog;
use crate::trace::{record_instant, span, TraceCategory};
use std::time::{SystemTime, UNIX_EPOCH};
use uf_core::idl::{
    AccessBindingRecord, DataObjectRecord, PlacementRecord, PlanStep, ResourceBindingRecord,
    TransferCost, TransferEvent, TransferPlan,
};
use uf_core::provider::{
    AccessKind, Addressability, ExecutionMode, Fidelity, ProviderCompletionResult,
    ProviderCompletionState, ProviderDescriptor, ProviderError, ProviderOperation,
    ProviderResourceRef, TransportKind,
};
use uf_core::{err, get, get_u64, ok, Kv};

fn provider_error(error: ProviderError) -> Kv {
    let mut response = err(error.detail);
    response.insert("error_code".to_string(), error.code.as_str().to_string());
    response
}

fn now_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

fn json_string(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('"');
    for character in value.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            ';' => escaped.push_str("\\u003b"),
            character if character.is_control() => {
                escaped.push_str(&format!("\\u{:04x}", character as u32));
            }
            character => escaped.push(character),
        }
    }
    escaped.push('"');
    escaped
}

fn optional_u64_json(value: Option<u64>) -> String {
    value.map_or_else(|| "null".to_string(), |value| value.to_string())
}

fn plan_json(plan: &TransferPlan) -> String {
    let steps = plan
        .steps
        .iter()
        .map(|step| {
            format!(
                concat!(
                    "{{\"step_id\":{},\"dependencies\":[{}],\"provider_id\":{},",
                    "\"operation\":{},\"src_placement_id\":{},\"dst_placement_id\":{},",
                    "\"access_binding_id\":{},\"offset_bytes\":{},\"nbytes\":{},",
                    "\"expected_completion_kind\":{},\"fallback_policy\":{},",
                    "\"provider_completion_id\":{},\"completion_state\":{}}}"
                ),
                step.step_id,
                step.dependencies
                    .iter()
                    .map(u64::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
                json_string(&step.provider_id),
                json_string(&step.operation),
                optional_u64_json(step.src_placement_id),
                optional_u64_json(step.dst_placement_id),
                optional_u64_json(step.access_binding_id),
                step.offset_bytes,
                step.nbytes,
                json_string(&step.expected_completion_kind),
                json_string(&step.fallback_policy),
                step.provider_completion_id
                    .as_deref()
                    .map_or_else(|| "null".to_string(), json_string),
                json_string(step.completion_state.as_str()),
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        concat!(
            "{{\"plan_id\":{},\"request_id\":{},\"src_placement_id\":{},",
            "\"dst_placement_id\":{},\"path\":{},\"engine\":{},",
            "\"completion_kind\":{},\"topology_generation\":{},",
            "\"capability_generation\":{},\"execution_realm_id\":{},",
            "\"fallback_used\":{},\"fallback_reason\":{},\"cost\":{{",
            "\"effort\":{},\"estimated_latency_us\":{},",
            "\"estimated_bandwidth_gib_s\":{},\"setup_cost_us\":{},",
            "\"hop_count\":{},\"contention_score\":{},",
            "\"reliability_score\":{},\"explanation\":{}}},\"steps\":[{}]}}"
        ),
        plan.plan_id,
        plan.request_id,
        plan.src_placement_id,
        plan.dst_placement_id,
        json_string(&plan.path),
        json_string(&plan.engine),
        json_string(&plan.completion_kind),
        plan.topology_generation,
        plan.capability_generation,
        json_string(&plan.execution_realm_id),
        plan.fallback_used,
        json_string(&plan.fallback_reason),
        plan.cost.effort,
        plan.cost.estimated_latency_us,
        plan.cost.estimated_bandwidth_gib_s,
        plan.cost.setup_cost_us,
        plan.cost.hop_count,
        plan.cost.contention_score,
        plan.cost.reliability_score,
        json_string(&plan.cost.explanation),
        steps,
    )
}

fn event_json(event: &TransferEvent) -> String {
    format!(
        concat!(
            "{{\"event_id\":{},\"plan_id\":{},\"parent_event_id\":{},",
            "\"attempt_id\":{},\"step_id\":{},\"provider_id\":{},",
            "\"provider_session_id\":{},\"status\":{},\"completion_kind\":{},",
            "\"submitted_at_ns\":{},\"started_at_ns\":{},\"completed_at_ns\":{},",
            "\"bytes_done\":{},\"checksum_verdict\":{},\"planned_path\":{},",
            "\"actual_path\":{},\"topology_generation\":{},",
            "\"capability_generation\":{},\"execution_mode\":{},\"fidelity\":{},",
            "\"error_code\":{},\"error_message\":{}}}"
        ),
        event.event_id,
        event.plan_id,
        optional_u64_json(event.parent_event_id),
        event.attempt_id,
        optional_u64_json(event.step_id),
        json_string(&event.provider_id),
        json_string(&event.provider_session_id),
        json_string(&event.status),
        json_string(&event.completion_kind),
        event.submitted_at_ns,
        event.started_at_ns,
        event.completed_at_ns,
        event.bytes_done,
        json_string(&event.checksum_verdict),
        json_string(&event.planned_path),
        json_string(&event.actual_path),
        event.topology_generation,
        event.capability_generation,
        json_string(event.execution_mode.as_str()),
        json_string(event.fidelity.as_str()),
        json_string(&event.error_code),
        json_string(&event.error_message),
    )
}

fn wait_result(
    provider: &dyn uf_core::provider::DataProvider,
    operation: ProviderOperation,
) -> Result<(String, ProviderCompletionResult), Kv> {
    let submission = provider
        .submit_operation(operation)
        .map_err(provider_error)?;
    let completion = provider
        .wait_completion(&submission.provider_completion_id, 30_000)
        .map_err(provider_error)?;
    completion.validate().map_err(provider_error)?;
    match (completion.result, completion.error) {
        (Some(result), None) => Ok((submission.provider_completion_id, result)),
        (_, Some(error)) => Err(provider_error(error)),
        _ => Err(err("provider returned no terminal result")),
    }
}

fn resource_for_binding(
    binding: &ResourceBindingRecord,
    placement: &PlacementRecord,
) -> ProviderResourceRef {
    ProviderResourceRef {
        provider_resource_id: binding.provider_resource_id.clone(),
        resource_generation: binding.binding_generation,
        size_bytes: placement.nbytes,
        allowed_offset_bytes: binding.allowed_offset_bytes,
        allowed_bytes: binding.allowed_bytes,
        addressability: placement.addressability,
        opaque_token: Vec::new(),
    }
}

fn binding_for_object(
    st: &crate::catalog::Catalog,
    object_id: u64,
) -> Option<(PlacementRecord, ResourceBindingRecord)> {
    let placement = st
        .placements
        .values()
        .find(|placement| {
            placement.object_id == object_id
                && placement.state == "Ready"
                && placement.execution_mode == ExecutionMode::Simulated
        })?
        .clone();
    let binding = st
        .resource_bindings
        .values()
        .find(|binding| binding.placement_id == placement.placement_id && binding.state == "Ready")?
        .clone();
    Some((placement, binding))
}

pub fn create_data_object(req: &Kv, shared: &SharedCatalog) -> Kv {
    let _trace = span(
        TraceCategory::Object,
        "provider.data_object.create",
        vec![
            ("execution_mode", "simulated".to_string()),
            ("provider_id", get(req, "provider").to_string()),
            ("node_id", get(req, "node_id").to_string()),
            ("medium", get(req, "medium").to_string()),
            ("bytes", get_u64(req, "size_bytes").to_string()),
            ("fidelity", "semantic".to_string()),
        ],
    );
    let provider_id = get(req, "provider");
    if provider_id.is_empty() {
        return err("simulated CreateDataObject requires provider");
    }
    let size_bytes = get_u64(req, "size_bytes");
    if size_bytes == 0 {
        return err("size_bytes must be a positive integer");
    }
    let client_id = get_u64(req, "client_id");
    let node_id = get(req, "node_id");
    if node_id.is_empty() {
        return err("simulated CreateDataObject requires node_id");
    }
    if get(req, "medium") != "ddr" && get(req, "placement") != "ddr" {
        return err("PhaseC-02 simulated object supports medium=ddr only");
    }
    let provider = {
        let (lock, _) = &**shared;
        let st = lock.lock().unwrap();
        if !st.clients.contains_key(&client_id) {
            return err("unknown client_id");
        }
        match st.providers.require(provider_id) {
            Ok(provider) => provider,
            Err(error) => return provider_error(error),
        }
    };
    let identity = provider.identity();
    let (_, allocation) = match wait_result(
        provider.as_ref(),
        ProviderOperation::Allocate {
            size_bytes,
            alignment_bytes: 1,
            medium: "ddr".to_string(),
            node_id: node_id.to_string(),
        },
    ) {
        Ok(value) => value,
        Err(response) => return response,
    };
    let resource = match allocation {
        ProviderCompletionResult::Resource(resource) => resource,
        _ => return err("provider allocate returned wrong result type"),
    };
    let seed_id = get_u64(req, "seed_id");
    let mut checksum = None;
    if seed_id != 0 {
        if let Err(response) = wait_result(
            provider.as_ref(),
            ProviderOperation::InitializeDeterministic {
                resource: resource.clone(),
                offset_bytes: 0,
                nbytes: size_bytes,
                seed_id,
            },
        ) {
            let _ = wait_result(
                provider.as_ref(),
                ProviderOperation::Release {
                    resource: resource.clone(),
                },
            );
            return response;
        }
        match wait_result(
            provider.as_ref(),
            ProviderOperation::ChecksumResource {
                resource: resource.clone(),
                offset_bytes: 0,
                nbytes: size_bytes,
            },
        ) {
            Ok((_, ProviderCompletionResult::Checksum { value, .. })) => checksum = Some(value),
            Ok(_) => return err("provider checksum returned wrong result type"),
            Err(response) => return response,
        }
    }
    let (lock, _) = &**shared;
    let mut st = lock.lock().unwrap();
    let object_id = st.take_next_object_id();
    let placement_id = st.take_next_placement_id();
    let binding_id = st.take_next_resource_binding_id();
    st.data_objects.insert(
        object_id,
        DataObjectRecord {
            object_id,
            namespace: get(req, "namespace").to_string(),
            name: get(req, "name").to_string(),
            role: if get(req, "role").is_empty() {
                "user".to_string()
            } else {
                get(req, "role").to_string()
            },
            size_bytes,
            consistency: "single_writer".to_string(),
            content_version: 1,
            owner: client_id.to_string(),
            shape: get(req, "shape").to_string(),
            dtype: get(req, "dtype").to_string(),
            state: "Ready".to_string(),
        },
    );
    st.placements.insert(
        placement_id,
        PlacementRecord {
            placement_id,
            object_id,
            medium: "ddr".to_string(),
            node_id: node_id.to_string(),
            device_id: None,
            domain: get(req, "domain").to_string(),
            address_kind: "virtual".to_string(),
            provider_id: provider_id.to_string(),
            execution_mode: ExecutionMode::Simulated,
            execution_realm_id: identity.execution_realm_id.clone(),
            requested_fidelity: Fidelity::Semantic,
            actual_fidelity: identity.fidelity,
            addressability: resource.addressability,
            topology_generation: 1,
            capability_generation: 1,
            offset_bytes: 0,
            nbytes: size_bytes,
            state: "Ready".to_string(),
        },
    );
    st.resource_bindings.insert(
        binding_id,
        ResourceBindingRecord {
            binding_id,
            placement_id,
            provider_id: provider_id.to_string(),
            provider_session_id: identity.session_id.clone(),
            provider_resource_id: resource.provider_resource_id.clone(),
            binding_generation: resource.resource_generation,
            opaque_descriptor: Vec::new(),
            allowed_offset_bytes: 0,
            allowed_bytes: size_bytes,
            limitations: identity.limitations.clone(),
            state: "Ready".to_string(),
        },
    );
    let mut response = ok(&[
        ("object_id", object_id.to_string()),
        ("placement_id", placement_id.to_string()),
        ("binding_id", binding_id.to_string()),
        ("size_bytes", size_bytes.to_string()),
        ("medium", "ddr".to_string()),
        ("node_id", node_id.to_string()),
        ("provider_id", provider_id.to_string()),
        ("provider_session_id", identity.session_id.clone()),
        ("execution_mode", "simulated".to_string()),
        ("execution_realm_id", identity.execution_realm_id.clone()),
        ("fidelity", identity.fidelity.as_str().to_string()),
        (
            "addressability",
            resource.addressability.as_str().to_string(),
        ),
        ("host_dereference", "0".to_string()),
        ("state", "Ready".to_string()),
    ]);
    if let Some(value) = checksum {
        response.insert("checksum".to_string(), value.to_string());
    }
    record_instant(
        TraceCategory::Backend,
        "provider.data_object.ready",
        vec![
            ("object_id", object_id.to_string()),
            ("placement_id", placement_id.to_string()),
            ("resource_binding_id", binding_id.to_string()),
            ("provider_id", provider_id.to_string()),
            ("provider_session_id", identity.session_id),
            ("node_id", node_id.to_string()),
            ("execution_realm_id", identity.execution_realm_id),
            ("actual_fidelity", identity.fidelity.as_str().to_string()),
            (
                "addressability",
                resource.addressability.as_str().to_string(),
            ),
        ],
    );
    response
}

pub fn publish(req: &Kv, shared: &SharedCatalog) -> Kv {
    let _trace = span(
        TraceCategory::Backend,
        "provider.publish",
        vec![
            ("object_id", get_u64(req, "object_id").to_string()),
            ("offset_bytes", get_u64(req, "offset_bytes").to_string()),
            ("bytes", get_u64(req, "nbytes").to_string()),
        ],
    );
    let object_id = get_u64(req, "object_id");
    let (provider, placement, binding) = {
        let (lock, _) = &**shared;
        let st = lock.lock().unwrap();
        let (placement, binding) = match binding_for_object(&st, object_id) {
            Some(value) => value,
            None => return err("simulated object binding not found"),
        };
        let provider = match st.providers.require(&binding.provider_id) {
            Ok(provider) => provider,
            Err(error) => return provider_error(error),
        };
        (provider, placement, binding)
    };
    let offset_bytes = get_u64(req, "offset_bytes");
    let nbytes = if get_u64(req, "nbytes") == 0 {
        placement.nbytes
    } else {
        get_u64(req, "nbytes")
    };
    let (completion_id, result) = match wait_result(
        provider.as_ref(),
        ProviderOperation::Publish {
            resource: resource_for_binding(&binding, &placement),
            offset_bytes,
            nbytes,
            access_mode: "read".to_string(),
        },
    ) {
        Ok(value) => value,
        Err(response) => return response,
    };
    let descriptor = match result {
        ProviderCompletionResult::Descriptor(descriptor) => descriptor,
        _ => return err("provider publish returned wrong result type"),
    };
    let (lock, _) = &**shared;
    let mut st = lock.lock().unwrap();
    if let Some(current) = st.resource_bindings.get_mut(&binding.binding_id) {
        current.opaque_descriptor = descriptor.opaque_bytes.clone();
        current.provider_session_id = descriptor.provider_session_id.clone();
        current.binding_generation = descriptor.resource_generation;
        current.allowed_offset_bytes = descriptor.allowed_offset_bytes;
        current.allowed_bytes = descriptor.allowed_bytes;
    }
    let timestamp = now_ns();
    let plan_id = st.take_next_transfer_plan_id();
    let parent_event_id = st.take_next_transfer_event_id();
    let step_event_id = st.take_next_transfer_event_id();
    st.typed_transfer_plans.insert(
        plan_id,
        TransferPlan {
            plan_id,
            request_id: plan_id,
            src_placement_id: placement.placement_id,
            dst_placement_id: placement.placement_id,
            path: "nodeA.ddr->ub_shmem->nodeB.remote_map".to_string(),
            engine: "obmm_gsva".to_string(),
            completion_kind: "ub_completion".to_string(),
            topology_generation: placement.topology_generation,
            capability_generation: placement.capability_generation,
            execution_realm_id: placement.execution_realm_id.clone(),
            steps: vec![PlanStep {
                step_id: 1,
                dependencies: Vec::new(),
                provider_id: binding.provider_id.clone(),
                operation: "publish".to_string(),
                src_placement_id: Some(placement.placement_id),
                dst_placement_id: None,
                access_binding_id: None,
                offset_bytes,
                nbytes,
                expected_completion_kind: "ub_completion".to_string(),
                fallback_policy: "fail_closed".to_string(),
                provider_completion_id: Some(completion_id.clone()),
                completion_state: ProviderCompletionState::Complete,
            }],
            cost: TransferCost {
                effort: 1.0,
                estimated_latency_us: 0.0,
                estimated_bandwidth_gib_s: 0.0,
                setup_cost_us: 0.0,
                hop_count: 1,
                contention_score: 0.0,
                reliability_score: 1.0,
                explanation: "functional simulation; physical performance is not inferred"
                    .to_string(),
            },
            fallback_used: false,
            fallback_reason: String::new(),
        },
    );
    st.typed_transfer_events.insert(
        parent_event_id,
        TransferEvent {
            event_id: parent_event_id,
            plan_id,
            parent_event_id: None,
            attempt_id: 1,
            step_id: None,
            provider_id: binding.provider_id.clone(),
            provider_session_id: descriptor.provider_session_id.clone(),
            status: "Running".to_string(),
            completion_kind: "ub_completion".to_string(),
            submitted_at_ns: timestamp,
            started_at_ns: timestamp,
            completed_at_ns: 0,
            bytes_done: 0,
            checksum_verdict: "pending".to_string(),
            planned_path: "nodeA.ddr->ub_shmem->nodeB.remote_map".to_string(),
            actual_path: "obmm_gsva".to_string(),
            topology_generation: placement.topology_generation,
            capability_generation: placement.capability_generation,
            execution_mode: ExecutionMode::Simulated,
            fidelity: Fidelity::Functional,
            error_code: String::new(),
            error_message: String::new(),
        },
    );
    st.typed_transfer_events.insert(
        step_event_id,
        TransferEvent {
            event_id: step_event_id,
            plan_id,
            parent_event_id: Some(parent_event_id),
            attempt_id: 1,
            step_id: Some(1),
            provider_id: binding.provider_id.clone(),
            provider_session_id: descriptor.provider_session_id.clone(),
            status: "Complete".to_string(),
            completion_kind: "ub_completion".to_string(),
            submitted_at_ns: timestamp,
            started_at_ns: timestamp,
            completed_at_ns: timestamp,
            bytes_done: nbytes,
            checksum_verdict: "pending".to_string(),
            planned_path: "nodeA.ddr->ub_shmem->nodeB.remote_map".to_string(),
            actual_path: "obmm_gsva".to_string(),
            topology_generation: placement.topology_generation,
            capability_generation: placement.capability_generation,
            execution_mode: ExecutionMode::Simulated,
            fidelity: Fidelity::Functional,
            error_code: String::new(),
            error_message: String::new(),
        },
    );
    st.simulated_object_plan_ids.insert(object_id, plan_id);
    st.simulated_plan_parent_event_ids
        .insert(plan_id, parent_event_id);
    ok(&[
        ("object_id", object_id.to_string()),
        ("binding_id", binding.binding_id.to_string()),
        ("provider_completion_id", completion_id),
        ("provider_id", descriptor.provider_id),
        ("provider_session_id", descriptor.provider_session_id),
        ("plan_id", plan_id.to_string()),
        ("parent_event_id", parent_event_id.to_string()),
        ("step_event_id", step_event_id.to_string()),
        (
            "resource_generation",
            descriptor.resource_generation.to_string(),
        ),
        ("offset_bytes", descriptor.allowed_offset_bytes.to_string()),
        ("nbytes", descriptor.allowed_bytes.to_string()),
        ("access_mode", descriptor.access_mode),
        (
            "descriptor_bytes",
            descriptor.opaque_bytes.len().to_string(),
        ),
    ])
}

pub fn import_read_only(req: &Kv, shared: &SharedCatalog) -> Kv {
    let _trace = span(
        TraceCategory::Backend,
        "provider.remote_map.import",
        vec![
            ("object_id", get_u64(req, "object_id").to_string()),
            ("accessor_node_id", get(req, "accessor_node_id").to_string()),
            ("access_mode", "read".to_string()),
            ("access_kind", "remote_map".to_string()),
            ("transport_kind", "obmm_gsva".to_string()),
            ("offset_bytes", get_u64(req, "offset_bytes").to_string()),
            ("bytes", get_u64(req, "nbytes").to_string()),
        ],
    );
    let object_id = get_u64(req, "object_id");
    let accessor_node_id = get(req, "accessor_node_id");
    if accessor_node_id.is_empty() {
        return err("ImportDataObject requires accessor_node_id");
    }
    let (provider, placement, binding, descriptor) = {
        let (lock, _) = &**shared;
        let st = lock.lock().unwrap();
        let (placement, binding) = match binding_for_object(&st, object_id) {
            Some(value) => value,
            None => return err("simulated object binding not found"),
        };
        if binding.opaque_descriptor.is_empty() {
            return err("object must be published before import");
        }
        let provider = match st.providers.require(&binding.provider_id) {
            Ok(provider) => provider,
            Err(error) => return provider_error(error),
        };
        let descriptor = ProviderDescriptor {
            schema_major: 1,
            schema_minor: 0,
            provider_id: binding.provider_id.clone(),
            provider_session_id: binding.provider_session_id.clone(),
            resource_generation: binding.binding_generation,
            allowed_offset_bytes: 0,
            allowed_bytes: binding.allowed_bytes,
            access_mode: "read".to_string(),
            opaque_bytes: binding.opaque_descriptor.clone(),
        };
        (provider, placement, binding, descriptor)
    };
    if accessor_node_id == placement.node_id {
        return err("remote import accessor must differ from placement node");
    }
    let offset_bytes = get_u64(req, "offset_bytes");
    let nbytes = if get_u64(req, "nbytes") == 0 {
        placement.nbytes
    } else {
        get_u64(req, "nbytes")
    };
    let (completion_id, result) = match wait_result(
        provider.as_ref(),
        ProviderOperation::Import {
            descriptor,
            accessor_node_id: accessor_node_id.to_string(),
            offset_bytes,
            nbytes,
            access_mode: "read".to_string(),
        },
    ) {
        Ok(value) => value,
        Err(response) => return response,
    };
    let access = match result {
        ProviderCompletionResult::Access(access) => access,
        _ => return err("provider import returned wrong result type"),
    };
    let (lock, _) = &**shared;
    let mut st = lock.lock().unwrap();
    let access_binding_id = st.take_next_access_binding_id();
    st.access_bindings.insert(
        access_binding_id,
        AccessBindingRecord {
            access_binding_id,
            placement_id: placement.placement_id,
            source_binding_id: binding.binding_id,
            accessor_node_id: accessor_node_id.to_string(),
            accessor_device_id: None,
            accessor_provider_id: access.accessor_provider_id.clone(),
            accessor_provider_session_id: access.accessor_provider_session_id.clone(),
            accessor_resource_id: access.accessor_resource_id.clone(),
            access_binding_generation: access.access_generation,
            source_binding_generation: access.source_resource_generation,
            access_kind: AccessKind::RemoteMap,
            transport_kind: TransportKind::ObmmGsva,
            addressability: Addressability::RemoteMapping,
            access_mode: "read".to_string(),
            allowed_offset_bytes: offset_bytes,
            allowed_bytes: nbytes,
            topology_generation: 1,
            capability_generation: 1,
            opaque_token: access.opaque_token,
            state: "Ready".to_string(),
        },
    );
    let placement_count = st
        .placements
        .values()
        .filter(|candidate| candidate.object_id == object_id)
        .count();
    let plan_id = match st.simulated_object_plan_ids.get(&object_id).copied() {
        Some(plan_id) => plan_id,
        None => return err("published simulated object has no typed validation plan"),
    };
    let parent_event_id = match st.simulated_plan_parent_event_ids.get(&plan_id).copied() {
        Some(event_id) => event_id,
        None => return err("simulated validation plan has no parent event"),
    };
    let step_event_id = st.take_next_transfer_event_id();
    if let Some(plan) = st.typed_transfer_plans.get_mut(&plan_id) {
        plan.steps.push(PlanStep {
            step_id: 2,
            dependencies: vec![1],
            provider_id: access.accessor_provider_id.clone(),
            operation: "import_read_only".to_string(),
            src_placement_id: Some(placement.placement_id),
            dst_placement_id: None,
            access_binding_id: Some(access_binding_id),
            offset_bytes,
            nbytes,
            expected_completion_kind: "ub_completion".to_string(),
            fallback_policy: "fail_closed".to_string(),
            provider_completion_id: Some(completion_id.clone()),
            completion_state: ProviderCompletionState::Complete,
        });
    }
    let timestamp = now_ns();
    st.typed_transfer_events.insert(
        step_event_id,
        TransferEvent {
            event_id: step_event_id,
            plan_id,
            parent_event_id: Some(parent_event_id),
            attempt_id: 1,
            step_id: Some(2),
            provider_id: access.accessor_provider_id.clone(),
            provider_session_id: access.accessor_provider_session_id.clone(),
            status: "Complete".to_string(),
            completion_kind: "ub_completion".to_string(),
            submitted_at_ns: timestamp,
            started_at_ns: timestamp,
            completed_at_ns: timestamp,
            bytes_done: nbytes,
            checksum_verdict: "pending".to_string(),
            planned_path: "nodeA.ddr->ub_shmem->nodeB.remote_map".to_string(),
            actual_path: "obmm_gsva".to_string(),
            topology_generation: placement.topology_generation,
            capability_generation: placement.capability_generation,
            execution_mode: ExecutionMode::Simulated,
            fidelity: Fidelity::Functional,
            error_code: String::new(),
            error_message: String::new(),
        },
    );
    st.simulated_access_plan_ids
        .insert(access_binding_id, plan_id);
    record_instant(
        TraceCategory::Backend,
        "provider.remote_map.ready",
        vec![
            ("object_id", object_id.to_string()),
            ("placement_id", placement.placement_id.to_string()),
            ("resource_binding_id", binding.binding_id.to_string()),
            ("access_binding_id", access_binding_id.to_string()),
            ("provider_id", access.accessor_provider_id.clone()),
            (
                "provider_session_id",
                access.accessor_provider_session_id.clone(),
            ),
            ("source_node_id", placement.node_id.clone()),
            ("accessor_node_id", accessor_node_id.to_string()),
            (
                "planned_path",
                "nodeA.ddr->ub_shmem->nodeB.remote_map".to_string(),
            ),
            ("actual_path", "obmm_gsva".to_string()),
            ("placement_count", placement_count.to_string()),
            ("remote_map_created_placement", "0".to_string()),
            ("fidelity", "functional".to_string()),
        ],
    );
    ok(&[
        ("object_id", object_id.to_string()),
        ("placement_id", placement.placement_id.to_string()),
        ("source_binding_id", binding.binding_id.to_string()),
        ("access_binding_id", access_binding_id.to_string()),
        ("accessor_node_id", accessor_node_id.to_string()),
        ("access_kind", "remote_map".to_string()),
        ("transport_kind", "obmm_gsva".to_string()),
        ("access_mode", "read".to_string()),
        ("accessor_provider_id", access.accessor_provider_id),
        (
            "accessor_provider_session_id",
            access.accessor_provider_session_id,
        ),
        ("accessor_resource_id", access.accessor_resource_id),
        (
            "access_binding_generation",
            access.access_generation.to_string(),
        ),
        (
            "source_binding_generation",
            access.source_resource_generation.to_string(),
        ),
        ("offset_bytes", offset_bytes.to_string()),
        ("nbytes", nbytes.to_string()),
        ("provider_completion_id", completion_id),
        ("plan_id", plan_id.to_string()),
        ("parent_event_id", parent_event_id.to_string()),
        ("step_event_id", step_event_id.to_string()),
        ("placement_count", placement_count.to_string()),
        ("remote_map_created_placement", "0".to_string()),
        ("state", "Ready".to_string()),
    ])
}

pub fn checksum_access(req: &Kv, shared: &SharedCatalog) -> Kv {
    let _trace = span(
        TraceCategory::Backend,
        "provider.remote_map.checksum",
        vec![
            (
                "access_binding_id",
                get_u64(req, "access_binding_id").to_string(),
            ),
            ("offset_bytes", get_u64(req, "offset_bytes").to_string()),
            ("bytes", get_u64(req, "nbytes").to_string()),
        ],
    );
    let access_binding_id = get_u64(req, "access_binding_id");
    let (provider, access) = {
        let (lock, _) = &**shared;
        let st = lock.lock().unwrap();
        if st.external_ddr.owns_view(access_binding_id) {
            return err("external DDR views are managed by the authenticated FD broker");
        }
        let access = match st.access_bindings.get(&access_binding_id) {
            Some(access) if access.state == "Ready" => access.clone(),
            _ => return err("access binding not found or stale"),
        };
        let provider = match st.providers.require(&access.accessor_provider_id) {
            Ok(provider) => provider,
            Err(error) => return provider_error(error),
        };
        (provider, access)
    };
    let provider_access = uf_core::provider::ProviderAccessRef {
        accessor_provider_id: access.accessor_provider_id.clone(),
        accessor_provider_session_id: access.accessor_provider_session_id.clone(),
        accessor_resource_id: access.accessor_resource_id.clone(),
        access_generation: access.access_binding_generation,
        source_resource_generation: access.source_binding_generation,
        allowed_offset_bytes: access.allowed_offset_bytes,
        allowed_bytes: access.allowed_bytes,
        access_mode: access.access_mode.clone(),
        addressability: access.addressability,
        opaque_token: access.opaque_token.clone(),
    };
    let (completion_id, result) = match wait_result(
        provider.as_ref(),
        ProviderOperation::ChecksumAccess {
            access: provider_access,
            offset_bytes: get_u64(req, "offset_bytes"),
            nbytes: if get_u64(req, "nbytes") == 0 {
                access.allowed_bytes
            } else {
                get_u64(req, "nbytes")
            },
        },
    ) {
        Ok(value) => value,
        Err(response) => return response,
    };
    let (algorithm, value) = match result {
        ProviderCompletionResult::Checksum { algorithm, value } => (algorithm, value),
        _ => return err("provider checksum returned wrong result type"),
    };
    let (lock, _) = &**shared;
    let mut st = lock.lock().unwrap();
    let plan_id = match st
        .simulated_access_plan_ids
        .get(&access_binding_id)
        .copied()
    {
        Some(plan_id) => plan_id,
        None => return err("simulated access has no typed validation plan"),
    };
    let parent_event_id = match st.simulated_plan_parent_event_ids.get(&plan_id).copied() {
        Some(event_id) => event_id,
        None => return err("simulated validation plan has no parent event"),
    };
    if st
        .typed_transfer_events
        .get(&parent_event_id)
        .is_some_and(|event| matches!(event.status.as_str(), "Complete" | "Failed" | "Canceled"))
    {
        return err("simulated validation parent event is already terminal");
    }
    let step_event_id = st.take_next_transfer_event_id();
    if let Some(plan) = st.typed_transfer_plans.get_mut(&plan_id) {
        plan.steps.push(PlanStep {
            step_id: 3,
            dependencies: vec![2],
            provider_id: access.accessor_provider_id.clone(),
            operation: "checksum".to_string(),
            src_placement_id: Some(access.placement_id),
            dst_placement_id: None,
            access_binding_id: Some(access_binding_id),
            offset_bytes: get_u64(req, "offset_bytes"),
            nbytes: if get_u64(req, "nbytes") == 0 {
                access.allowed_bytes
            } else {
                get_u64(req, "nbytes")
            },
            expected_completion_kind: "ub_completion".to_string(),
            fallback_policy: "fail_closed".to_string(),
            provider_completion_id: Some(completion_id.clone()),
            completion_state: ProviderCompletionState::Complete,
        });
    }
    let timestamp = now_ns();
    st.typed_transfer_events.insert(
        step_event_id,
        TransferEvent {
            event_id: step_event_id,
            plan_id,
            parent_event_id: Some(parent_event_id),
            attempt_id: 1,
            step_id: Some(3),
            provider_id: access.accessor_provider_id.clone(),
            provider_session_id: access.accessor_provider_session_id.clone(),
            status: "Complete".to_string(),
            completion_kind: "ub_completion".to_string(),
            submitted_at_ns: timestamp,
            started_at_ns: timestamp,
            completed_at_ns: timestamp,
            bytes_done: access.allowed_bytes,
            checksum_verdict: "pass".to_string(),
            planned_path: "nodeA.ddr->ub_shmem->nodeB.remote_map".to_string(),
            actual_path: "obmm_gsva".to_string(),
            topology_generation: access.topology_generation,
            capability_generation: access.capability_generation,
            execution_mode: ExecutionMode::Simulated,
            fidelity: Fidelity::Functional,
            error_code: String::new(),
            error_message: String::new(),
        },
    );
    if let Some(parent) = st.typed_transfer_events.get_mut(&parent_event_id) {
        parent.status = "Complete".to_string();
        parent.completed_at_ns = timestamp;
        parent.bytes_done = access.allowed_bytes;
        parent.provider_id = access.accessor_provider_id.clone();
        parent.provider_session_id = access.accessor_provider_session_id.clone();
        parent.checksum_verdict = "pass".to_string();
    }
    ok(&[
        ("access_binding_id", access_binding_id.to_string()),
        ("provider_completion_id", completion_id),
        ("algorithm", algorithm),
        ("checksum", value.to_string()),
        ("verdict", "pass".to_string()),
        ("plan_id", plan_id.to_string()),
        ("parent_event_id", parent_event_id.to_string()),
        ("step_event_id", step_event_id.to_string()),
        ("parent_terminal_count", "1".to_string()),
        ("parent_status", "Complete".to_string()),
    ])
}

pub fn get_validation(req: &Kv, shared: &SharedCatalog) -> Kv {
    let plan_id = get_u64(req, "plan_id");
    let (lock, _) = &**shared;
    let st = lock.lock().unwrap();
    let plan = match st.typed_transfer_plans.get(&plan_id) {
        Some(plan) => plan,
        None => return err("simulated validation plan not found"),
    };
    let mut events = st
        .typed_transfer_events
        .values()
        .filter(|event| event.plan_id == plan_id)
        .collect::<Vec<_>>();
    events.sort_by_key(|event| event.event_id);
    let events_json = format!(
        "[{}]",
        events
            .iter()
            .map(|event| event_json(event))
            .collect::<Vec<_>>()
            .join(",")
    );
    let access_binding_id = plan
        .steps
        .iter()
        .find_map(|step| step.access_binding_id)
        .unwrap_or(0);
    let access = st.access_bindings.get(&access_binding_id);
    let placement = st.placements.get(&plan.src_placement_id);
    let resource_binding_id = access.map_or(0, |access| access.source_binding_id);
    let object_id = placement.map_or(0, |placement| placement.object_id);
    let parent_terminal_count = events
        .iter()
        .filter(|event| {
            event.parent_event_id.is_none()
                && matches!(event.status.as_str(), "Complete" | "Failed" | "Canceled")
        })
        .count();
    let monitor_json = format!(
        concat!(
            "{{\"schema\":\"uflow-phasec02-monitor-snapshot-v1\",",
            "\"object_id\":{},\"placement_id\":{},\"resource_binding_id\":{},",
            "\"access_binding_id\":{},\"plan_id\":{},\"typed_step_count\":{},",
            "\"typed_event_count\":{},\"parent_terminal_count\":{},",
            "\"provider_id\":{},\"provider_session_id\":{},",
            "\"source_node_id\":{},\"accessor_node_id\":{},",
            "\"execution_mode\":\"simulated\",\"fidelity\":\"functional\"}}"
        ),
        object_id,
        plan.src_placement_id,
        resource_binding_id,
        access_binding_id,
        plan_id,
        plan.steps.len(),
        events.len(),
        parent_terminal_count,
        json_string(access.map_or("", |access| access.accessor_provider_id.as_str())),
        json_string(access.map_or("", |access| {
            access.accessor_provider_session_id.as_str()
        })),
        json_string(placement.map_or("", |placement| placement.node_id.as_str())),
        json_string(access.map_or("", |access| access.accessor_node_id.as_str())),
    );
    ok(&[
        ("plan_id", plan_id.to_string()),
        ("plan_json", plan_json(plan)),
        ("events_json", events_json),
        ("monitor_json", monitor_json),
        ("parent_terminal_count", parent_terminal_count.to_string()),
    ])
}

pub fn revoke_access(req: &Kv, shared: &SharedCatalog) -> Kv {
    let _trace = span(
        TraceCategory::Backend,
        "provider.remote_map.revoke",
        vec![(
            "access_binding_id",
            get_u64(req, "access_binding_id").to_string(),
        )],
    );
    let access_binding_id = get_u64(req, "access_binding_id");
    let (provider, access) = {
        let (lock, _) = &**shared;
        let st = lock.lock().unwrap();
        if st.external_ddr.owns_view(access_binding_id) {
            return err("external DDR views are managed by the authenticated FD broker");
        }
        let access = match st.access_bindings.get(&access_binding_id) {
            Some(access) if access.state == "Ready" => access.clone(),
            _ => return err("access binding not found or stale"),
        };
        let provider = match st.providers.require(&access.accessor_provider_id) {
            Ok(provider) => provider,
            Err(error) => return provider_error(error),
        };
        (provider, access)
    };
    let provider_access = uf_core::provider::ProviderAccessRef {
        accessor_provider_id: access.accessor_provider_id,
        accessor_provider_session_id: access.accessor_provider_session_id,
        accessor_resource_id: access.accessor_resource_id,
        access_generation: access.access_binding_generation,
        source_resource_generation: access.source_binding_generation,
        allowed_offset_bytes: access.allowed_offset_bytes,
        allowed_bytes: access.allowed_bytes,
        access_mode: access.access_mode,
        addressability: access.addressability,
        opaque_token: access.opaque_token,
    };
    let (completion_id, _) = match wait_result(
        provider.as_ref(),
        ProviderOperation::Revoke {
            access: provider_access,
        },
    ) {
        Ok(value) => value,
        Err(response) => return response,
    };
    let (lock, _) = &**shared;
    let mut st = lock.lock().unwrap();
    if let Some(current) = st.access_bindings.get_mut(&access_binding_id) {
        current.state = "Stale".to_string();
        current.access_binding_generation += 1;
    }
    ok(&[
        ("access_binding_id", access_binding_id.to_string()),
        ("provider_completion_id", completion_id),
        ("state", "Stale".to_string()),
    ])
}

pub fn release_data_object(req: &Kv, shared: &SharedCatalog) -> Kv {
    let _trace = span(
        TraceCategory::Backend,
        "provider.data_object.release",
        vec![("object_id", get_u64(req, "object_id").to_string())],
    );
    let object_id = get_u64(req, "object_id");
    let (provider, placement, binding) =
        {
            let (lock, _) = &**shared;
            let st = lock.lock().unwrap();
            let (placement, binding) = match binding_for_object(&st, object_id) {
                Some(value) => value,
                None => return err("simulated object binding not found"),
            };
            if st.access_bindings.values().any(|access| {
                access.source_binding_id == binding.binding_id && access.state == "Ready"
            }) {
                return err("simulated object still has an active AccessBinding");
            }
            let provider = match st.providers.require(&binding.provider_id) {
                Ok(provider) => provider,
                Err(error) => return provider_error(error),
            };
            (provider, placement, binding)
        };
    let (completion_id, _) = match wait_result(
        provider.as_ref(),
        ProviderOperation::Release {
            resource: resource_for_binding(&binding, &placement),
        },
    ) {
        Ok(value) => value,
        Err(response) => return response,
    };
    let (lock, _) = &**shared;
    let mut st = lock.lock().unwrap();
    st.mark_object_released(object_id);
    ok(&[
        ("object_id", object_id.to_string()),
        ("provider_completion_id", completion_id),
        ("state", "Released".to_string()),
    ])
}

pub fn dereference_handle(_req: &Kv, _shared: &SharedCatalog) -> Kv {
    let mut response = err("simulated provider-only handle is not locally addressable");
    response.insert(
        "error_code".to_string(),
        "HandleNotLocallyAddressable".to_string(),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Catalog;
    use std::sync::{Arc, Condvar, Mutex};
    use uf_core::{get, kv};

    fn shared_catalog() -> SharedCatalog {
        Arc::new((Mutex::new(Catalog::new(0, Vec::new())), Condvar::new()))
    }

    fn registered_catalog() -> (SharedCatalog, u64) {
        let shared = shared_catalog();
        let response = crate::object_service::register_client(
            &kv(&[
                ("role", "test".to_string()),
                ("device_id", "0".to_string()),
                ("os_pid", "1".to_string()),
                ("bare_tgid", "0".to_string()),
            ]),
            &shared,
        );
        (shared, get_u64(&response, "client_id"))
    }

    #[test]
    fn remote_map_creates_access_binding_not_placement() {
        let (shared, client_id) = registered_catalog();
        let created = create_data_object(
            &kv(&[
                ("client_id", client_id.to_string()),
                ("provider", "mock".to_string()),
                ("execution_mode", "simulated".to_string()),
                ("medium", "ddr".to_string()),
                ("node_id", "nodeA".to_string()),
                ("domain", "ub_domain0".to_string()),
                ("size_bytes", "4096".to_string()),
                ("seed_id", "42".to_string()),
                ("name", "phasec02-x".to_string()),
            ]),
            &shared,
        );
        assert_eq!(get(&created, "status"), "ok");
        let object_id = get_u64(&created, "object_id");
        let expected_checksum = get_u64(&created, "checksum");
        assert_ne!(expected_checksum, 0);

        let published = publish(&kv(&[("object_id", object_id.to_string())]), &shared);
        assert_eq!(get(&published, "status"), "ok");
        let imported = import_read_only(
            &kv(&[
                ("object_id", object_id.to_string()),
                ("accessor_node_id", "nodeB".to_string()),
                ("offset_bytes", "0".to_string()),
                ("nbytes", "4096".to_string()),
            ]),
            &shared,
        );
        assert_eq!(get(&imported, "status"), "ok");
        assert_eq!(get(&imported, "placement_count"), "1");
        assert_eq!(get(&imported, "remote_map_created_placement"), "0");
        let access_binding_id = get_u64(&imported, "access_binding_id");

        let checked = checksum_access(
            &kv(&[("access_binding_id", access_binding_id.to_string())]),
            &shared,
        );
        assert_eq!(get_u64(&checked, "checksum"), expected_checksum);
        assert_eq!(get(&checked, "verdict"), "pass");
        assert_eq!(get(&checked, "parent_terminal_count"), "1");

        let revoked = revoke_access(
            &kv(&[("access_binding_id", access_binding_id.to_string())]),
            &shared,
        );
        assert_eq!(get(&revoked, "state"), "Stale");
        let stale = checksum_access(
            &kv(&[("access_binding_id", access_binding_id.to_string())]),
            &shared,
        );
        assert_eq!(get(&stale, "status"), "error");

        let (lock, _) = &*shared;
        let st = lock.lock().unwrap();
        assert_eq!(
            st.placements
                .values()
                .filter(|placement| placement.object_id == object_id)
                .count(),
            1
        );
        let access = st.access_bindings.get(&access_binding_id).unwrap();
        assert_eq!(access.accessor_node_id, "nodeB");
        assert_eq!(access.state, "Stale");
        let plan_id = st.simulated_access_plan_ids[&access_binding_id];
        let plan = &st.typed_transfer_plans[&plan_id];
        assert_eq!(plan.steps.len(), 3);
        assert!(plan.steps.iter().all(|step| {
            step.provider_completion_id.is_some()
                && step.completion_state == ProviderCompletionState::Complete
        }));
        let parent_event_id = st.simulated_plan_parent_event_ids[&plan_id];
        assert_eq!(
            st.typed_transfer_events[&parent_event_id].status,
            "Complete"
        );
        assert_eq!(
            st.typed_transfer_events
                .values()
                .filter(|event| {
                    event.plan_id == plan_id
                        && event.parent_event_id.is_none()
                        && matches!(event.status.as_str(), "Complete" | "Failed" | "Canceled")
                })
                .count(),
            1
        );
    }

    #[test]
    fn simulated_request_never_falls_back_to_local() {
        let (shared, client_id) = registered_catalog();
        let response = create_data_object(
            &kv(&[
                ("client_id", client_id.to_string()),
                ("provider", "missing".to_string()),
                ("medium", "ddr".to_string()),
                ("node_id", "nodeA".to_string()),
                ("size_bytes", "4096".to_string()),
            ]),
            &shared,
        );
        assert_eq!(get(&response, "error_code"), "ProviderUnavailable");
    }
}
