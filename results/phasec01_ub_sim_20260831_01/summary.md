# PhaseC-01 ub-sim baseline closure

Status: `latest-repaired-ready / manual-active`

## Baseline decision

- Upstream root: `0fecb3b1037a424d7d285ed8c2792f08031e88ed`.
- Raw upstream is still not reproducible: `mem_service@4b13ec0eae2d1f91a1e203fe2644c9df1e0765f6` and `qemu@15951308ea8fa1fbce600d434a5b8cf72e132f14` are not readable from their declared submodule repositories.
- Validated downstream repair candidate: `c68cfb8ab139312cc9d41fb26f0ebcd3b00c7072` in `/home/sj/phasec01_runs/phasec01_ub_sim_20260831_01/ub_sim`.
- The dirty canonical checkout `/home/sj/git/ub_sim@67c3ce5` was inspected but not reset, cleaned, checked out, or overwritten.
- C02-C07 may use this exact candidate or `candidate_repair.patch` for internal development. It is not an upstream release baseline until the repair is published or accepted upstream.

## Repair chain

1. `d78143e` changes the unreachable `mem_service` gitlink and lock to reachable `366a335275d14c277dc44d3a75070f35951a8fb4`, and changes the unreachable QEMU gitlink to reachable `3a247cbe1532438f27805efe2e124c4ab882528f`.
2. `c68cfb8` restores the backward-compatible `linqu_ub_bridge_register_qwen3_runtime_object_payload` Rust FFI/header alias required by reachable QEMU while retaining the model-neutral API.

The complete two-commit patch is preserved in `candidate_repair.patch`.

## Gate results

- U0/U1: PASS. Execution was `root` on `serving-b` in `openeuler-2403-DS`; workspace root `/home/sj`; all eight top-level gitlinks match their checked-out HEADs; `mem_service` gitlink, lock, VERSION, and HEAD agree.
- U2: PASS. `cargo test --workspace` passed, including the 156-pass/9-ignored sim-uapi suite; Python discovery passed 304/304; post-alias `sim-qemu` passed 25/25; QEMU UB focused tests passed 22/22; baseline guard passed 10/10.
- U3: PASS. QEMU with UB and ubmem-vmmu support built; guest kernel/modules/UAPI/initramfs built; all 24 app Makefiles passed in one matrix; 2-node Memory Service + OBMM provider conformance passed 1/1.
- U4: PASS. 4-node headless topology canary passed 1/1. Eight-node was not required because this repair changes no node-count, full-mesh, routing, scale-out, or release semantic.
- Cleanup: PASS. No `qemu-system-aarch64` process and no Unix socket remained under candidate runtime directories.

## Environment completion

The openEuler container was missing build prerequisites. The run installed `glib2-devel`, `pixman-devel`, `zlib-devel`, `flex`, `bison`, `bc`, `openssl-devel`, `elfutils-libelf-devel`, `rsync`, and `cpio`. QEMU must use `QEMU_CONFIGURE_ARGS="--disable-werror --python=/usr/bin/python3"` because the `/usr/local` Python has an incompatible partial distlib. The guest Makefiles expect Debian tool names, so two container-local compatibility symlinks were added:

```text
/usr/local/bin/aarch64-linux-gnu-gcc -> /usr/bin/aarch64-openEuler-linux-gcc
/usr/local/bin/aarch64-linux-gnu-ar  -> /usr/bin/ar
```

## Remaining debt

- Upstream must publish reachable atomic gitlinks/locks and absorb the QEMU bridge compatibility repair before this can become plain `latest-ready`.
- The QEMU/kernel/app builds contain non-blocking compiler warnings; they are preserved in raw logs and should be cleaned upstream rather than hidden with local edits.
- C01 currently has a reusable guard CLI but no installed daily watcher/CI trigger, so freshness mode is `manual-active`.

`baseline.json` is the machine-readable source of truth. Failed dependency/build/config attempts are intentionally retained alongside the final passing attempts.
