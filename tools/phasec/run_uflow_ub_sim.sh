#!/usr/bin/env bash
set -euo pipefail

UB_SIM_ROOT="/home/sj/git/ub_sim"
MEM_SERVICE_ROOT="${UF_PHASEC02_MEM_SERVICE_ROOT:-/home/sj/git/ub_sim/mem_service}"
RUN_ROOT=""
RUN_ID=""
NODES=2
PROVIDER="ub_sim"
SMOKE="cross-node-ddr"
SEED_ID=42
KEEP_RUNNING=0

usage() {
  printf '%s\n' \
    "Usage: $0 --run-root PATH --run-id ID [options]" \
    "  --ub-sim-root PATH       canonical ub-sim checkout" \
    "  --mem-service-root PATH  UP0-enabled mem_service source" \
    "  --nodes 2                C02 is fixed to two logical nodes" \
    "  --provider ub_sim        C02 real provider" \
    "  --smoke cross-node-ddr   fixed 4 KiB read-only validation" \
    "  --seed-id N              deterministic Guest seed (default 42)" \
    "  --keep-running           leave daemon and guests running after PASS"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ub-sim-root) UB_SIM_ROOT="$2"; shift 2 ;;
    --mem-service-root) MEM_SERVICE_ROOT="$2"; shift 2 ;;
    --run-root) RUN_ROOT="$2"; shift 2 ;;
    --run-id) RUN_ID="$2"; shift 2 ;;
    --nodes) NODES="$2"; shift 2 ;;
    --provider) PROVIDER="$2"; shift 2 ;;
    --smoke) SMOKE="$2"; shift 2 ;;
    --seed-id) SEED_ID="$2"; shift 2 ;;
    --keep-running) KEEP_RUNNING=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "$RUN_ROOT" || -z "$RUN_ID" ]]; then
  usage >&2
  exit 2
fi
if [[ "$RUN_ID" == *[!A-Za-z0-9._-]* ]]; then
  printf 'run id contains unsupported characters: %s\n' "$RUN_ID" >&2
  exit 2
fi
if [[ "$NODES" != "2" || "$PROVIDER" != "ub_sim" || "$SMOKE" != "cross-node-ddr" ]]; then
  printf 'PhaseC-02 is fixed to --nodes 2 --provider ub_sim --smoke cross-node-ddr\n' >&2
  exit 2
fi
if [[ -e "$RUN_ROOT" ]]; then
  printf 'run root already exists; choose a fresh path: %s\n' "$RUN_ROOT" >&2
  exit 2
fi

DATA_SERVICE_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PLATFORM_SCRIPT_DIR="$UB_SIM_ROOT/guest-linux/aarch64/scripts"
PLATFORM_ROOT="$UB_SIM_ROOT/guest-linux/aarch64"
OFFICIAL_LAUNCHER="$PLATFORM_SCRIPT_DIR/run_ub_dual_node_apps.sh"
KERNEL_IMAGE="$PLATFORM_ROOT/out/Image"
CANONICAL_INITRAMFS="$PLATFORM_ROOT/out/initramfs.cpio.gz"
KERNEL_BUILD="$PLATFORM_ROOT/out/kernel_build"
KERNEL_SOURCE="$UB_SIM_ROOT/guest-linux/kernel_ub"
VIRTIO_CONSOLE_KO="$KERNEL_BUILD/drivers/char/virtio_console.ko"
EVIDENCE="$RUN_ROOT/evidence"
BRIDGE_DIR="$RUN_ROOT/bridge"
SOCKET_DIR="$RUN_ROOT/sockets"
PID_DIR="$RUN_ROOT/pids"
BUILD_DIR="$RUN_ROOT/build"
OVERLAY_ROOT="$RUN_ROOT/overlay/root"
RUN_INITRAMFS="$RUN_ROOT/overlay/initramfs.cpio.gz"
SDK_PREFIX="$RUN_ROOT/provider-sdk"
PLATFORM_PID=""
DAEMON_PID=""

log() {
  printf '[phasec02] %s\n' "$*"
}

write_source_status() {
  local output="$1"
  {
    printf 'ub_sim_sha=%s\n' "$(git -C "$UB_SIM_ROOT" rev-parse HEAD)"
    printf 'ub_sim_status_begin\n'
    git -C "$UB_SIM_ROOT" status --short --untracked-files=no
    printf 'ub_sim_status_end\n'
    if git -C "$UB_SIM_ROOT/mem_service" rev-parse HEAD >/dev/null 2>&1; then
      printf 'mem_service_sha=%s\n' "$(git -C "$UB_SIM_ROOT/mem_service" rev-parse HEAD)"
      printf 'mem_service_status_begin\n'
      git -C "$UB_SIM_ROOT/mem_service" status --short --untracked-files=no
      printf 'mem_service_status_end\n'
    fi
  } >"$output"
}

write_up0_source_status() {
  local output="$1"
  local fallback_sha
  fallback_sha="$(git -C "$UB_SIM_ROOT/mem_service" rev-parse HEAD 2>/dev/null || printf unknown)"
  {
    printf 'up0_source_root=%s\n' "$MEM_SERVICE_ROOT"
    if git -C "$MEM_SERVICE_ROOT" rev-parse HEAD >/dev/null 2>&1; then
      printf 'up0_source_kind=git-worktree\n'
      printf 'up0_source_sha=%s\n' "$(git -C "$MEM_SERVICE_ROOT" rev-parse HEAD)"
      printf 'up0_source_status_begin\n'
      git -C "$MEM_SERVICE_ROOT" status --short
      printf 'up0_source_status_end\n'
    else
      printf 'up0_source_kind=isolated-source-copy\n'
      printf 'up0_source_sha=%s\n' "${UF_PHASEC02_MEM_SERVICE_SHA:-$fallback_sha}"
      printf 'up0_source_status=not-a-git-worktree; see up0_source_files.sha256\n'
    fi
  } >"$output"
}

write_evidence_checksums() {
  (
    cd "$EVIDENCE"
    find . -type f ! -name SHA256SUMS -print0 \
      | sort -z \
      | xargs -0 sha256sum >SHA256SUMS
  )
}

wait_for_path() {
  local path="$1"
  local timeout="$2"
  local deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    if [[ -e "$path" ]]; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

stop_exact_pid() {
  local pid="$1"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 50); do
      if ! kill -0 "$pid" 2>/dev/null; then
        return 0
      fi
      sleep 0.1
    done
    kill -9 "$pid" 2>/dev/null || true
  fi
}

cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  if [[ -n "$DAEMON_PID" ]] && kill -0 "$DAEMON_PID" 2>/dev/null; then
    PYTHONPATH="$DATA_SERVICE_ROOT/sdk/python" python3 -c \
      "from uflow.protocol import SocketClient; SocketClient('$SOCKET_DIR/uflow.sock').request(op='ShutdownDaemon')" \
      >/dev/null 2>&1 || true
    for _ in $(seq 1 50); do
      if ! kill -0 "$DAEMON_PID" 2>/dev/null; then
        break
      fi
      sleep 0.1
    done
    stop_exact_pid "$DAEMON_PID"
  fi
  stop_exact_pid "$PLATFORM_PID"
  if [[ -d "$EVIDENCE" ]]; then
    write_source_status "$EVIDENCE/external_source_after.txt" || true
    if cmp -s "$EVIDENCE/external_source_before.txt" "$EVIDENCE/external_source_after.txt"; then
      printf 'external_source_unchanged=1\n' >"$EVIDENCE/cleanup.txt"
    else
      printf 'external_source_unchanged=0\n' >"$EVIDENCE/cleanup.txt"
      rc=1
    fi
    printf 'daemon_pid_alive=%s\n' "$(kill -0 "${DAEMON_PID:-0}" 2>/dev/null && printf 1 || printf 0)" >>"$EVIDENCE/cleanup.txt"
    printf 'platform_pid_alive=%s\n' "$(kill -0 "${PLATFORM_PID:-0}" 2>/dev/null && printf 1 || printf 0)" >>"$EVIDENCE/cleanup.txt"
    write_evidence_checksums || rc=1
  fi
  exit "$rc"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for command in git make gcc ar pkg-config cargo python3 zsh gzip cpio sha256sum; do
  command -v "$command" >/dev/null
done
for path in "$OFFICIAL_LAUNCHER" "$KERNEL_IMAGE" "$CANONICAL_INITRAMFS"; do
  [[ -e "$path" ]] || { printf 'required platform artifact missing: %s\n' "$path" >&2; exit 1; }
done
[[ -f "$MEM_SERVICE_ROOT/apps/mem_service/Makefile" ]] || {
  printf 'UP0-enabled mem_service root is invalid: %s\n' "$MEM_SERVICE_ROOT" >&2
  exit 1
}

mkdir -p "$EVIDENCE" "$BRIDGE_DIR" "$SOCKET_DIR" "$PID_DIR" "$BUILD_DIR" "$OVERLAY_ROOT"
write_source_status "$EVIDENCE/external_source_before.txt"
write_up0_source_status "$EVIDENCE/up0_source_status.txt"
(
  cd "$MEM_SERVICE_ROOT"
  sha256sum \
    apps/mem_service/Makefile \
    components/mem_service/providers/mem_service_provider_obmm_sdk.c \
    components/mem_service/providers/mem_service_provider_obmm_sdk.h \
    tests/mem_service_obmm_sdk_contract.c \
    >"$EVIDENCE/up0_source_files.sha256"
)

UB_SIM_SHA="$(git -C "$UB_SIM_ROOT" rev-parse HEAD)"
CANONICAL_MEM_SERVICE_SHA="$(git -C "$UB_SIM_ROOT/mem_service" rev-parse HEAD 2>/dev/null || printf unknown)"
MEM_SERVICE_SHA="$(git -C "$MEM_SERVICE_ROOT" rev-parse HEAD 2>/dev/null || printf '%s' "${UF_PHASEC02_MEM_SERVICE_SHA:-$CANONICAL_MEM_SERVICE_SHA}")"
UFLOW_SHA="$(git -C "$DATA_SERVICE_ROOT" rev-parse HEAD 2>/dev/null || printf dirty-worktree)"
TOPOLOGY_FILE="$UB_SIM_ROOT/vendor/ub_topology_two_node_v0.ini"
cp "$TOPOLOGY_FILE" "$EVIDENCE/qemu_topology.ini"
sha256sum "$EVIDENCE/qemu_topology.ini" >"$EVIDENCE/qemu_topology.sha256"

cat >"$EVIDENCE/scenario.yaml" <<EOF
schema: uflow-phasec02-scenario-v1
run_id: $RUN_ID
nodes: [nodeA, nodeB]
execution_mode: simulated
execution_realm_id: ub_sim:$RUN_ID
provider: ub_sim
placement: {node_id: nodeA, medium: ddr, offset_bytes: 0, nbytes: 4096}
access: {node_id: nodeB, kind: remote_map, mode: read, transport: obmm_gsva}
fidelity: functional
seed_id: $SEED_ID
EOF
sha256sum "$EVIDENCE/scenario.yaml" >"$EVIDENCE/scenario.sha256"

{
  printf 'host=serving-b\n'
  printf 'container=openeuler-2403-DS\n'
  printf 'container_user=%s\n' "$(id -un)"
  printf 'workspace=/home/sj\n'
  printf 'run_root=%s\n' "$RUN_ROOT"
  printf 'uname=%s\n' "$(uname -a)"
  printf 'gcc=%s\n' "$(gcc --version | head -1)"
  printf 'rustc=%s\n' "$(rustc --version)"
  printf 'cargo=%s\n' "$(cargo --version)"
  printf 'python=%s\n' "$(python3 --version 2>&1)"
  printf 'ub_sim_root=%s\n' "$UB_SIM_ROOT"
  printf 'ub_sim_sha=%s\n' "$UB_SIM_SHA"
  printf 'canonical_mem_service_sha=%s\n' "$CANONICAL_MEM_SERVICE_SHA"
  printf 'up0_source_root=%s\n' "$MEM_SERVICE_ROOT"
  printf 'up0_source_sha=%s\n' "$MEM_SERVICE_SHA"
  printf 'uflow_root=%s\n' "$DATA_SERVICE_ROOT"
  printf 'uflow_sha=%s\n' "$UFLOW_SHA"
} >"$EVIDENCE/environment.txt"

cat >"$EVIDENCE/external_interface_inventory.md" <<EOF
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
EOF

cat >"$EVIDENCE/capability_map.json" <<EOF
{"schema":"uflow-phasec02-capability-map-v1","topology_generation":1,"capability_generation":1,"provider":"ub_sim","fidelity":"functional","source":{"node_id":"nodeA","medium":"ddr","operations":["allocate","initialize_deterministic","publish","release"]},"accessor":{"node_id":"nodeB","access_kind":"remote_map","access_mode":"read","transport":"obmm_gsva","operations":["import_read_only","checksum","revoke"]},"fallback":false}
EOF

cat >"$EVIDENCE/compatibility_matrix.json" <<EOF
{"schema":"uflow-phasec02-compatibility-v1","uflow_provider_contract":"1.0","uflow_bridge_protocol":"1","mem_service_provider_api":"1.0","mem_service_provider_abi":"1","ub_sim_revision":"$UB_SIM_SHA","topology":"two-node-v0","kernel_transport":"virtio-serial","status":"compatible"}
EOF

cat >"$EVIDENCE/bridge_transport_decision.md" <<EOF
# PhaseC-02 bridge transport decision

- Transport: dedicated per-Guest virtio-serial port.
- Host endpoints: $BRIDGE_DIR/nodeA.sock and $BRIDGE_DIR/nodeB.sock.
- Guest endpoints: /dev/virtio-ports/org.uflow.ubsim.nodeA and nodeB.
- Control plane: versioned descriptors, status, completion and checksum only.
- Data plane: Guest OBMM/GSVA only; no application payload in Host frames.
- Lifecycle: QMP/platform launcher owns QEMU; provider session generation makes old bindings stale.
- Cleanup: only PIDs, sockets and overlays rooted at this run are targeted.
EOF

log "building and installing the UP0 public OBMM SDK"
make -C "$MEM_SERVICE_ROOT/apps/mem_service" \
  CC=gcc CFLAGS="-O2 -Wall -Wextra -Werror -static" OBMM_AR=ar \
  PACKAGE_OUT_DIR="$BUILD_DIR/mem-service" \
  PREFIX="$SDK_PREFIX" \
  OBMM_SDK_SOURCE_SHA="$MEM_SERVICE_SHA" \
  obmm-provider-installed-consumer-smoke \
  >"$EVIDENCE/up0_build.log" 2>&1

log "building the statically linked per-Guest companion"
PKG_CONFIG_PATH="$SDK_PREFIX/lib/pkgconfig" make -C "$DATA_SERVICE_ROOT/crates/uf-ub-sim-agent" \
  CC=gcc CFLAGS="-O2 -Wall -Wextra -Werror -static" \
  OUT="$BUILD_DIR/uf-ub-sim-agent-obmm" \
  >"$EVIDENCE/guest_agent_build.log" 2>&1

if [[ ! -f "$VIRTIO_CONSOLE_KO" ]]; then
  log "building the Guest virtio console module from the official kernel tree"
  make -C "$KERNEL_SOURCE" O="$KERNEL_BUILD" ARCH=arm64 \
    drivers/char/virtio_console.ko -j8 \
    >"$EVIDENCE/virtio_console_build.log" 2>&1
fi

log "building UFlow daemon and provider adapter"
CARGO_TARGET_DIR="$BUILD_DIR/cargo" \
UF_ACL_SHIM_LIB_DIR="${UF_ACL_SHIM_LIB_DIR:-/home/sj/git/data-service/build/lib}" \
LIBRARY_PATH="${LIBRARY_PATH:-/usr/local/Ascend/cann-9.0.0/aarch64-linux/lib64:/usr/local/Ascend/cann-9.0.0/runtime/lib64:/usr/local/Ascend/driver/lib64/driver}" \
cargo build --manifest-path "$DATA_SERVICE_ROOT/Cargo.toml" -p uf-daemon \
  >"$EVIDENCE/uflow_build.log" 2>&1

log "running UFlow provider, bridge and regression contracts"
LD_LIBRARY_PATH="/home/sj/git/data-service/build/lib:/usr/local/Ascend/cann-9.0.0/aarch64-linux/lib64:/usr/local/Ascend/cann-9.0.0/runtime/lib64:/usr/local/Ascend/driver/lib64/driver:${LD_LIBRARY_PATH:-}" \
CARGO_TARGET_DIR="$BUILD_DIR/cargo" \
cargo test --manifest-path "$DATA_SERVICE_ROOT/Cargo.toml" --workspace \
  >"$EVIDENCE/unit_contract_tests.log" 2>&1

log "creating the run-owned Guest initramfs overlay"
(
  cd "$OVERLAY_ROOT"
  gzip -dc "$CANONICAL_INITRAMFS" | cpio -idm --quiet
)
mkdir -p "$OVERLAY_ROOT/lib/modules"
cp "$VIRTIO_CONSOLE_KO" "$OVERLAY_ROOT/lib/modules/virtio_console.ko"
cp "$BUILD_DIR/uf-ub-sim-agent-obmm" "$OVERLAY_ROOT/bin/uf-ub-sim-agent-obmm"
chmod 0755 "$OVERLAY_ROOT/bin/uf-ub-sim-agent-obmm"

python3 - "$OVERLAY_ROOT/bin/run_app" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
text = path.read_text()
needle = '''if [ "$shift_args" = "1" ]; then
  shift
fi
'''
replacement = needle + r'''
start_uflow_phasec02_agent() {
  role=""
  read_cmdline
  for token in $cmdline; do
    case "$token" in
      linqu_urma_dp_role=*) role="${token#linqu_urma_dp_role=}" ;;
    esac
  done
  if [ "$role" != "nodeA" ] && [ "$role" != "nodeB" ]; then
    log "uflow agent: invalid node role"
    return 1
  fi
  if [ ! -e "/sys/module/virtio_console" ]; then
    /bin/busybox insmod /lib/modules/virtio_console.ko || return 1
  fi
  port="/dev/virtio-ports/org.uflow.ubsim.${role}"
  attempts=0
  while [ ! -e "$port" ] && [ "$attempts" -lt 300 ]; do
    for candidate in /sys/class/virtio-ports/*; do
      if [ -r "$candidate/name" ] &&
         [ "$(/bin/busybox cat "$candidate/name")" = "org.uflow.ubsim.${role}" ]; then
        port="/dev/${candidate##*/}"
        break
      fi
    done
    /bin/busybox sleep 0.1
    attempts=$((attempts + 1))
  done
  if [ ! -e "$port" ]; then
    log "uflow agent: virtio port unavailable: $port"
    return 1
  fi
  /bin/uf-ub-sim-agent-obmm --device "$port" --node-id "$role" \
    </dev/null >/dev/console 2>&1 &
  echo $! >/tmp/uflow_phasec02_agent.pid
  log "uflow agent launched role=$role pid=$!"
}

if has_flag "linqu_uflow_agent=1"; then
  start_uflow_phasec02_agent || exit 1
fi
'''
if needle not in text:
    raise SystemExit("run_app patch point not found")
path.write_text(text.replace(needle, replacement, 1))
PY

(
  cd "$OVERLAY_ROOT"
  find . -print0 | cpio --null -o -H newc --quiet | gzip -9 >"$RUN_INITRAMFS"
)

PATCHED_LAUNCHER="$RUN_ROOT/platform/run_ub_dual_node_apps.sh"
mkdir -p "$(dirname "$PATCHED_LAUNCHER")"
cp "$OFFICIAL_LAUNCHER" "$PATCHED_LAUNCHER"
python3 - "$PATCHED_LAUNCHER" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
text = path.read_text()
anchor = 'WORKSPACE_ROOT="$(cd "$ROOT_DIR/../.." && pwd)"\n'
override = anchor + '''if [[ -n "${UFLOW_PLATFORM_SCRIPT_DIR:-}" ]]; then
  SCRIPT_DIR="$UFLOW_PLATFORM_SCRIPT_DIR"
  ROOT_DIR="$UFLOW_PLATFORM_ROOT"
  WORKSPACE_ROOT="$UFLOW_WORKSPACE_ROOT"
fi
'''
if anchor not in text:
    raise SystemExit("platform root patch point not found")
text = text.replace(anchor, override, 1)
text = text.replace('OUT_DIR="$ROOT_DIR/out"\nLOG_DIR="$ROOT_DIR/logs"',
                    'OUT_DIR="$UFLOW_RUN_ROOT/platform-out"\nLOG_DIR="$UFLOW_RUN_ROOT/platform-logs"', 1)
text = text.replace('REPORT_FILE="${REPORT_FILE:-$OUT_DIR/apps_report.latest.txt}"',
                    'REPORT_FILE="${REPORT_FILE:-$UFLOW_RUN_ROOT/evidence/platform_report.txt}"', 1)
text = text.replace('  local serial_args=()\n', '  local serial_args=()\n  local bridge_args=()\n', 1)
env_anchor = '  env \\\n    UB_FM_NODE_ID="$node_id" \\\n'
bridge_setup = '''  local bridge_socket="$UFLOW_BRIDGE_DIR/${role}.sock"
  mkdir -p "$UFLOW_BRIDGE_DIR"
  rm -f "$bridge_socket"
  bridge_args=(
    -chardev "socket,id=uflow0,path=$bridge_socket,server=on,wait=off"
    -device "virtio-serial-device,id=uflow-serial"
    -device "virtserialport,chardev=uflow0,name=org.uflow.ubsim.${role}"
  )

'''
if env_anchor not in text:
    raise SystemExit("QEMU environment patch point not found")
text = text.replace(env_anchor, bridge_setup + env_anchor, 1)
serial_anchor = '      "${serial_args[@]}" \\\n'
if serial_anchor not in text:
    raise SystemExit("QEMU serial patch point not found")
text = text.replace(serial_anchor, serial_anchor + '      "${bridge_args[@]}" \\\n', 1)
text = text.replace('  rm -f /tmp/ub-qemu/ub-bus-instance-*.lock\n',
                    '  rm -f "$SHARED_DIR"/ub-bus-instance-nodeA.lock "$SHARED_DIR"/ub-bus-instance-nodeB.lock\n', 1)
path.write_text(text)
PY
chmod 0755 "$PATCHED_LAUNCHER"

cat >"$EVIDENCE/baseline.json" <<EOF
{"schema":"uflow-phasec02-baseline-v1","run_id":"$RUN_ID","uflow_sha":"$UFLOW_SHA","ub_sim_sha":"$UB_SIM_SHA","canonical_mem_service_sha":"$CANONICAL_MEM_SERVICE_SHA","up0_source_sha":"$MEM_SERVICE_SHA","provider_api":"1.0","provider_abi":"1","bridge":"uflow-sim-bridge-v1","nodes":2,"nbytes":4096,"offset_bytes":0}
EOF
cat >"$EVIDENCE/bridge_transport_decision.json" <<EOF
{"schema":"uflow-phasec02-bridge-decision-v1","transport":"virtio-serial","nodeA_host_endpoint":"$BRIDGE_DIR/nodeA.sock","nodeB_host_endpoint":"$BRIDGE_DIR/nodeB.sock","guest_endpoint_pattern":"/dev/virtio-ports/org.uflow.ubsim.<node>","payload_allowed":false,"reconnect":"new provider session generation","cleanup_scope":"$RUN_ROOT"}
EOF
cp "$SDK_PREFIX/share/lingqu/mem_service/provider_version.json" "$EVIDENCE/provider_contract.json"

log "starting two official ub-sim logical nodes with the run-owned overlay"
env \
  UFLOW_PLATFORM_SCRIPT_DIR="$PLATFORM_SCRIPT_DIR" \
  UFLOW_PLATFORM_ROOT="$PLATFORM_ROOT" \
  UFLOW_WORKSPACE_ROOT="$UB_SIM_ROOT" \
  UFLOW_RUN_ROOT="$RUN_ROOT" \
  UFLOW_BRIDGE_DIR="$BRIDGE_DIR" \
  KERNEL_IMAGE="$KERNEL_IMAGE" \
  INITRAMFS_IMAGE="$RUN_INITRAMFS" \
  UB_USE_PREBUILT_QEMU=1 \
  UB_FM_SHARED_DIR="$RUN_ROOT/fm" \
  RUN_SECS=240 MAX_RUNTIME=900 \
  zsh "$PATCHED_LAUNCHER" \
    --run-id "$RUN_ID" \
    --app chat \
    --append-extra "linqu_uflow_agent=1" \
    --interactive-after-pass \
    >"$EVIDENCE/platform_launcher.log" 2>&1 &
PLATFORM_PID=$!
printf '%s\n' "$PLATFORM_PID" >"$PID_DIR/platform.pid"

wait_for_path "$BRIDGE_DIR/nodeA.sock" 90 || { printf 'nodeA bridge socket timeout\n' >&2; exit 1; }
wait_for_path "$BRIDGE_DIR/nodeB.sock" 90 || { printf 'nodeB bridge socket timeout\n' >&2; exit 1; }

log "starting UFlow daemon and performing bridge identity handshakes"
LD_LIBRARY_PATH="/home/sj/git/data-service/build/lib:/usr/local/Ascend/cann-9.0.0/aarch64-linux/lib64:/usr/local/Ascend/cann-9.0.0/runtime/lib64:/usr/local/Ascend/driver/lib64/driver:${LD_LIBRARY_PATH:-}" \
UF_EXECUTION_MODE=simulated \
UF_UB_SIM_NODE_A_SOCKET="$BRIDGE_DIR/nodeA.sock" \
UF_UB_SIM_NODE_B_SOCKET="$BRIDGE_DIR/nodeB.sock" \
UF_UB_SIM_REALM_ID="ub_sim:$RUN_ID" \
UF_UB_SIM_PLATFORM_REVISION="$UB_SIM_SHA" \
UF_UB_SIM_BRIDGE_CAPTURE="$EVIDENCE/bridge_frames.capture" \
UF_DAEMON_CONTROL_CAPTURE="$EVIDENCE/daemon_control_frames.capture" \
UF_TRACE_ENABLE=1 \
UF_TRACE_MODE=full \
UF_TRACE_RUN_ID="$RUN_ID" \
UF_TRACE_OUTPUT_DIR="$EVIDENCE/trace" \
"$BUILD_DIR/cargo/debug/uf-daemon" \
  --execution-mode simulated \
  --socket "$SOCKET_DIR/uflow.sock" \
  >"$EVIDENCE/uflow_daemon.log" 2>&1 &
DAEMON_PID=$!
printf '%s\n' "$DAEMON_PID" >"$PID_DIR/uflow-daemon.pid"
wait_for_path "$SOCKET_DIR/uflow.sock" 90 || { printf 'UFlow daemon socket timeout\n' >&2; exit 1; }

log "running the SDK-only 4 KiB NodeA.DDR to NodeB read-only slice"
PYTHONPATH="$DATA_SERVICE_ROOT/sdk/python" \
UF_ENABLE=1 UF_SOCKET="$SOCKET_DIR/uflow.sock" UF_REQUIRE_ACL=0 \
python3 "$DATA_SERVICE_ROOT/examples/phasec02/uflow_ub_sim_ddr_smoke.py" \
  --socket "$SOCKET_DIR/uflow.sock" \
  --run-id "$RUN_ID" \
  --seed-id "$SEED_ID" \
  --evidence "$EVIDENCE/golden_slice.json" \
  | tee "$EVIDENCE/sdk_smoke.log"

cp "$EVIDENCE/golden_slice.json" "$EVIDENCE/checksums.json"
cp "$EVIDENCE/golden_slice.json" "$EVIDENCE/placements.json"
cp "$EVIDENCE/golden_slice.json" "$EVIDENCE/resource_bindings.json"
cp "$EVIDENCE/golden_slice.json" "$EVIDENCE/access_bindings.json"
python3 - "$EVIDENCE/golden_slice.json" "$EVIDENCE" <<'PY'
import json
from pathlib import Path
import sys

golden = json.loads(Path(sys.argv[1]).read_text())
evidence = Path(sys.argv[2])
for name, value in (
    ("plans.json", [golden["typed_plan"]]),
    ("events.json", golden["typed_events"]),
    ("monitor_snapshot.json", golden["monitor_snapshot"]),
):
    (evidence / name).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
PY
if [[ -f "$EVIDENCE/trace/$RUN_ID/trace_events.json" ]]; then
  cp "$EVIDENCE/trace/$RUN_ID/trace_events.json" "$EVIDENCE/trace_events.json"
elif [[ -f "$EVIDENCE/trace/trace_events.json" ]]; then
  cp "$EVIDENCE/trace/trace_events.json" "$EVIDENCE/trace_events.json"
fi

find "$RUN_ROOT/platform-logs" -type f -name nodeA_guest.log -exec \
  grep -h "uf-ub-sim-agent:" {} + >"$EVIDENCE/guest_nodeA_agent.log" 2>/dev/null || true
find "$RUN_ROOT/platform-logs" -type f -name nodeB_guest.log -exec \
  grep -h "uf-ub-sim-agent:" {} + >"$EVIDENCE/guest_nodeB_agent.log" 2>/dev/null || true
cat "$EVIDENCE/guest_nodeA_agent.log" "$EVIDENCE/guest_nodeB_agent.log" \
  >"$EVIDENCE/guest_agents.log"
cp "$EVIDENCE/uflow_daemon.log" "$EVIDENCE/ub_sim_provider.log"

python3 - "$EVIDENCE" <<'PY'
import hashlib
import json
from pathlib import Path
import sys

evidence = Path(sys.argv[1])

def digest(name: str) -> str:
    return hashlib.sha256((evidence / name).read_bytes()).hexdigest()

manifest = {
    "schema": "uflow-phasec02-bridge-manifest-v1",
    "protocol": "uflow-sim-bridge-v1",
    "payload_allowed": False,
    "captures": {
        "provider_bridge": {
            "file": "bridge_frames.capture",
            "sha256": digest("bridge_frames.capture"),
        },
        "daemon_control": {
            "file": "daemon_control_frames.capture",
            "sha256": digest("daemon_control_frames.capture"),
        },
    },
}
(evidence / "bridge_manifest.json").write_text(
    json.dumps(manifest, indent=2, sort_keys=True) + "\n"
)

golden = json.loads((evidence / "golden_slice.json").read_text())
checks = {
    "host_dereference_rejected": golden["host_dereference_rejected"],
    "revoke_stale_rejected": golden["revoke_stale_rejected"],
    "remote_map_created_placement": golden["remote_map_created_placement"],
    "placement_count": golden["placement_count"],
    "parent_terminal_count": golden["parent_terminal_count"],
}
if checks != {
    "host_dereference_rejected": True,
    "revoke_stale_rejected": True,
    "remote_map_created_placement": False,
    "placement_count": 1,
    "parent_terminal_count": 1,
}:
    raise SystemExit(f"negative invariant failure: {checks}")
(evidence / "negative_tests.log").write_text(
    "status=pass\n"
    "fallback=fail_closed\n"
    "unit_contract_tests=pass\n"
    + "\n".join(f"{key}={str(value).lower()}" for key, value in checks.items())
    + "\n"
)
PY

log "scanning all Host run artifacts for the deterministic payload and canary"
python3 "$DATA_SERVICE_ROOT/tools/phasec/phasec02_payload_scan.py" \
  --run-root "$RUN_ROOT" \
  --output "$EVIDENCE/payload_path_assertion.json" \
  --seed-id "$SEED_ID" \
  --nbytes 4096 \
  | tee "$EVIDENCE/payload_scan.log"
cp "$EVIDENCE/payload_path_assertion.json" "$EVIDENCE/host_payload_scan_manifest.json"

cat >"$EVIDENCE/summary.md" <<EOF
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
EOF

if [[ "$KEEP_RUNNING" -eq 1 ]]; then
  trap - EXIT INT TERM
  printf 'PASS; services retained. daemon_pid=%s platform_pid=%s run_root=%s\n' \
    "$DAEMON_PID" "$PLATFORM_PID" "$RUN_ROOT"
  exit 0
fi

log "PASS; stopping only this run's daemon and QEMU processes"
