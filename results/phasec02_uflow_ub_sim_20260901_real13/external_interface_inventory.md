# PhaseC-02 external interface inventory

| Boundary | Interface used | Payload permitted | Ownership |
|---|---|---:|---|
| UFlow SDK -> daemon | Unix control socket | no | UFlow |
| daemon -> Guest A agent | uflow-sim-bridge-v1 over dedicated virtio-serial | no | run-owned adapter |
| daemon -> Guest B agent | uflow-sim-bridge-v1 over dedicated virtio-serial | no | run-owned adapter |
| Guest agent -> mem_service | public OBMM provider SDK API/ABI 1 | yes, Guest-local only | mem_service |
| NodeA -> NodeB | ub-sim OBMM/GSVA fabric | yes | ub-sim simulator data plane |
| lifecycle | provider session + resource/access generations | no | UFlow/provider |

The Host bridge carries only opaque descriptors, operation metadata, status,
completion identifiers, checksums and counters. It never dereferences or relays
the application payload.
