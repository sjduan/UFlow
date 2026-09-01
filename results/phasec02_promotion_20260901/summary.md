# PhaseC-02 internal promotion summary

- Status: PASS — internal formal-archive promotion complete.
- Exact commits: data-service `c429d16`, ub_sim `b624ee4`, mem_service `546caed`.
- Fetchability: each commit is stored in a verified complete-history Git bundle under `archives/`.
- C01: U0–U4 PASS; report is `last-known-good / repaired / ready=true` because the candidate is an offline downstream archive, not an upstream branch tip.
- C02: exact-commit `real13` PASS for the fixed two-node 4 KiB NodeA.DDR to NodeB read-only OBMM remote-map slice; the tracked checkout remained clean after build and test.
- Isolation: remote canonical `/home/sj/git/ub_sim@c68cfb8` and its `mem_service@366a335` remained clean and unchanged.
- Scope: this closes C02 and UP0 for internal development. It is not upstream/release-ready and does not claim general remote DDR, dynamic topology-aware routing or physical remote HBM.

The first C01 attempt exposed a stale copied Cargo target path, and the first U3 attempt exposed a missing candidate QEMU binary plus absent `distlib`. Both failures remain in `c01/commands.jsonl`; the accepted reruns used a fresh run-owned Cargo target and the verified canonical prebuilt QEMU binary without modifying canonical source.
