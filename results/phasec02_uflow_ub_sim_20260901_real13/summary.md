# PhaseC-02 real ub-sim validation

- Status: PASS
- Slice: NodeA.DDR Placement -> NodeB read-only AccessBinding
- Range: offset 0, exactly 4096 bytes
- Provider: ub_sim / public OBMM SDK v1 / uflow-sim-bridge-v1
- Fidelity: functional simulation; no physical performance claim
- Catalog invariant: one Placement; remote map created no NodeB Placement
- Checksum: Guest A seed checksum equals Guest B observed checksum
- Payload path: Host bridge/control and ordinary artifacts have zero hits; ub-sim FM ring matches are recorded separately as simulator data-plane backing
- Lifecycle: revoke makes old AccessBinding stale; Host handle is not addressable
