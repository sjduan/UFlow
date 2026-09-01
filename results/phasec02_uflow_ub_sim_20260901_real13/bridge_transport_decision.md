# PhaseC-02 bridge transport decision

- Transport: dedicated per-Guest virtio-serial port.
- Host endpoints: /home/sj/phasec02_runs/phasec02_uflow_ub_sim_20260901_real13/bridge/nodeA.sock and /home/sj/phasec02_runs/phasec02_uflow_ub_sim_20260901_real13/bridge/nodeB.sock.
- Guest endpoints: /dev/virtio-ports/org.uflow.ubsim.nodeA and nodeB.
- Control plane: versioned descriptors, status, completion and checksum only.
- Data plane: Guest OBMM/GSVA only; no application payload in Host frames.
- Lifecycle: QMP/platform launcher owns QEMU; provider session generation makes old bindings stale.
- Cleanup: only PIDs, sockets and overlays rooted at this run are targeted.
