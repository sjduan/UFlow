# Reconstruct the frozen source stack

All remote references below are read-only inputs. The overlays are supplied
with UFlow; no unpublished external commit needs to be pushed or fetched.
Original file copyright/license notices remain in each patch. Build tools and
libraries continue to use their upstream licenses.

| Source | Fetchable base | Accepted overlay provenance |
|---|---|---|
| PyPTO | `be23606f791fa2251d7fccabdb91c75582268fad` | `8007f1146a800d679301c5ea14bb644b863da6bf` |
| Simpler | `6b6b88f2353cca12cfafe809750b50b59931b0d1` | `508becb93e182a3035a14e3f9226929dbe994346` |
| ub_sim tooling | `abeec108a1607feb37218d0d89987e37c579d567` | `c1e73ed2115e7a32400098a99c3b5bbf84405968` |
| QEMU | `f2ef54f454b661e551a8619f9feb7406752a1b1a` | Eleven exact accepted managed-write source files; resulting tree in lock |

Use Python 3.10 or newer and Git with `core.autocrlf=false`. For example,
from this UFlow checkout and with a new `$STACK` directory:

```sh
git -c core.autocrlf=false clone --no-checkout https://github.com/hw-native-sys/pypto.git "$STACK/pypto"
git -C "$STACK/pypto" checkout --detach be23606f791fa2251d7fccabdb91c75582268fad
python tools/apply_locked_source.py pypto "$STACK/pypto"
git -c core.autocrlf=false clone --no-checkout https://github.com/hw-native-sys/simpler.git "$STACK/pypto/runtime"
git -C "$STACK/pypto/runtime" checkout --detach 6b6b88f2353cca12cfafe809750b50b59931b0d1
python tools/apply_locked_source.py simpler "$STACK/pypto/runtime"

git -c core.autocrlf=false clone --no-checkout https://github.com/LL-mixed/ub_sim.git "$STACK/ub_sim"
git -C "$STACK/ub_sim" checkout --detach abeec108a1607feb37218d0d89987e37c579d567
python tools/apply_locked_source.py ub_sim "$STACK/ub_sim"
git -c core.autocrlf=false clone --no-checkout https://github.com/LL-mixed/qemu.git "$STACK/ub_sim/vendor/qemu_8.2.0_ub"
git -C "$STACK/ub_sim/vendor/qemu_8.2.0_ub" checkout --detach f2ef54f454b661e551a8619f9feb7406752a1b1a
python tools/apply_locked_source.py qemu "$STACK/ub_sim/vendor/qemu_8.2.0_ub"
```

If a base is not present in a clone, fetch that exact public base into the same
checkout before `checkout --detach`. Do not use a patched parent gitlink as a
submodule download instruction: the PyPTO runtime gitlink intentionally retains
the original local-only Simpler identity. Verify both source entries separately:

```sh
python tools/apply_locked_source.py pypto "$STACK/pypto" --check
python tools/apply_locked_source.py simpler "$STACK/pypto/runtime" --check
python tools/apply_locked_source.py ub_sim "$STACK/ub_sim" --check
python tools/apply_locked_source.py qemu "$STACK/ub_sim/vendor/qemu_8.2.0_ub" --check
```

Use this same patched Simpler for the ub_sim CPU-sim build; its vendor gitlink
`3794c430...` is not the validated runtime. Fixed upstream inputs are kernel
`c2def7c931e6451d89727dec0c6f892abe28ba00`, mem_service
`546985e26bce60798fa3f88329c3fbed8350c798`, PTO ISA
`217bb98952a8f9004b95fe938da97d8d5f68f702` and PTOAS release `v0.70`.
Their repository URLs are in the lock. The Serving checkout supplies the
matching Lib base and runtime-worker patch; follow its dependency README.

PyPTO/Simpler overlays change Python source and tests, not native ABI. Build the
fixed bases with their documented build/install scripts, then expose these
patched Python trees (or install them in editable mode) to the same interpreter.
Record the actually imported files, library paths and build options. A patched
source hash does not prove a native library was rebuilt or loaded.

For QEMU, use the ub_sim wrapper build route at this frozen base; do not run raw
configure/ninja or change the simulation fidelity. `qemu-managed-pool.patch`
already contains the flattened scatter-home, managed-read and managed-write
changes; **do not additionally apply** the nested historical patches. The kernel
profile requires `CONFIG_UB_UMMU_SVA_SEPARATED_PAGES=y` while retaining the fixed
base's other settings. The reconstructed ub_sim documents
`docs/uflow-kernel-profile.md`, `guest-linux/aarch64/docs/uflow-managed-cache-v1.md`
and `uflow-managed-write-v1.md` describe the corresponding profile/build closure.
Use new build/output roots, at most four build jobs, and the bounded supervisor.

Reconstruction gives source, not an accepted ELF, current producer context or
boot authority. The checked-in original profiles contain historical absolute
paths and seals; generate and verify new build/profile evidence for any later
deployment. No QEMU/kernel rebuild or Guest run is claimed by this code delivery.
