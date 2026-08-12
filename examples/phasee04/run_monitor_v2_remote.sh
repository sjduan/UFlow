#!/usr/bin/env bash
set -euo pipefail

source /home/sj/git/all_env.sh 2>/dev/null || true

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

device="${UF_E04_DEVICE:?UF_E04_DEVICE must be selected from the current npu-smi snapshot}"
profile="${UF_E04_PROFILE:-regular}"
if [[ "${profile}" != "regular" && "${profile}" != "fallback" ]]; then
  echo "UF_E04_PROFILE must be regular or fallback" >&2
  exit 2
fi
run_id="${UF_E04_RUN_ID:-phasee04_monitor_v2_${profile}_$(date +%Y%m%d_%H%M%S)}"
if [[ ! "${run_id}" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "UF_E04_RUN_ID must be a safe single path component" >&2
  exit 2
fi
run_dir="${UF_E04_RUN_DIR:-/tmp/proj_output/${run_id}}"
trace_root="${run_dir}/traces"
socket_path="/tmp/${run_id}.sock"
ddr_root="/dev/shm/${run_id}_ddr"
ssd_root="/data/${run_id}_ssd"
api_port="${UF_E04_API_PORT:-8082}"
daemon_bin="${UF_DAEMON_BIN:-./target/release/uf-daemon}"
python_bin="${UF_E04_PYTHON:-python3}"
hold_after_check="${UF_E04_HOLD_AFTER_CHECK:-0}"

mkdir -p "${run_dir}" "${trace_root}" "${ddr_root}" "${ssd_root}"
rm -f "${socket_path}"

export LD_LIBRARY_PATH="${repo_root}/build/lib:${LD_LIBRARY_PATH:-}"
export PYTHONPATH="${repo_root}/sdk/python:${PYTHONPATH:-}"
export UF_ACL_LIB="${UF_ACL_LIB:-${repo_root}/build/lib/libuf_acl_shim.so}"
export UF_DDR_ROOT="${ddr_root}"
export UF_SSD_ROOT="${ssd_root}"
export UF_TRACE_OUTPUT_DIR="${trace_root}"
export UF_SSD_DIRECT_ENABLE="${UF_SSD_DIRECT_ENABLE:-1}"
export UF_SSD_DIRECT_AUTO="${UF_SSD_DIRECT_AUTO:-1}"
export UF_SSD_HBM_DIRECT_MIN_BYTES="${UF_SSD_HBM_DIRECT_MIN_BYTES:-256MiB}"
export UF_PINNED_CHUNK_COUNT="${UF_PINNED_CHUNK_COUNT:-2}"
export UF_H2D_PINNED_CHUNK_BYTES="${UF_H2D_PINNED_CHUNK_BYTES:-16777216}"
export UF_D2H_PINNED_CHUNK_BYTES="${UF_D2H_PINNED_CHUNK_BYTES:-67108864}"
export UF_DDR_MADVISE_HUGEPAGE="${UF_DDR_MADVISE_HUGEPAGE:-1}"
export UF_DDR_PRETOUCH_ON_CREATE="${UF_DDR_PRETOUCH_ON_CREATE:-1}"

if [[ "${profile}" == "fallback" ]]; then
  export UF_ENABLE_TEST_FAILPOINTS=1
  export UF_TEST_SSD_DIRECT_FAIL_STAGE=before_acl
else
  unset UF_ENABLE_TEST_FAILPOINTS || true
  unset UF_TEST_SSD_DIRECT_FAIL_STAGE || true
fi

{
  echo "run_id=${run_id}"
  echo "profile=${profile}"
  echo "repo_root=${repo_root}"
  echo "hostname=$(cat /etc/hostname 2>/dev/null || echo unknown)"
  echo "kernel=$(uname -r)"
  echo "device=${device}"
  echo "code_commit=${UF_E04_CODE_COMMIT:-$(git rev-parse HEAD 2>/dev/null || echo isolated-tree)}"
  echo "sizes=${UF_E04_SIZES:-64MiB,256MiB}"
  echo "direct_sizes=${UF_E04_DIRECT_SIZES:-256MiB}"
  echo "warmup=${UF_E04_WARMUP:-0}"
  echo "repeat=${UF_E04_REPEAT:-1}"
  echo "api_port=${api_port}"
  echo "socket=${socket_path}"
  echo "trace_root=${trace_root}"
  echo "ddr_root=${ddr_root}"
  echo "ssd_root=${ssd_root}"
  echo "failpoints_enabled=${UF_ENABLE_TEST_FAILPOINTS:-0}"
  echo "fail_stage=${UF_TEST_SSD_DIRECT_FAIL_STAGE:-none}"
  npu-smi info || true
} > "${run_dir}/environment.txt"

"${daemon_bin}" \
  --device "${device}" \
  --socket "${socket_path}" \
  --startup-probe-bytes "${UF_HBM_STARTUP_PROBE_BYTES:-1048576}" \
  > "${run_dir}/daemon.log" 2>&1 &
daemon_pid=$!
api_pid=""
cleaned=0

cleanup() {
  if [[ "${cleaned}" == "1" ]]; then
    return
  fi
  cleaned=1
  if [[ -n "${api_pid}" ]]; then
    kill "${api_pid}" >/dev/null 2>&1 || true
    wait "${api_pid}" >/dev/null 2>&1 || true
  fi
  "${python_bin}" - "${socket_path}" <<'PY' >/dev/null 2>&1 || true
import socket
import sys
client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
client.connect(sys.argv[1])
client.sendall(b"op=ShutdownDaemon\n")
client.close()
PY
  wait "${daemon_pid}" >/dev/null 2>&1 || true
  rm -f "${socket_path}"
  rm -rf "${ddr_root}" "${ssd_root}"
}
trap cleanup EXIT INT TERM

for _attempt in $(seq 1 100); do
  [[ -S "${socket_path}" ]] && break
  sleep 0.2
done
[[ -S "${socket_path}" ]]

UF_MONITOR_PORT="${api_port}" \
UFLOW_API_BASE="http://127.0.0.1:${api_port}" \
UFLOW_TUNNEL_PATH="local -> ssh serving-b -> openeuler-2403-DS" \
"${python_bin}" -u tools/uflow_monitor_api.py \
  --socket "${socket_path}" \
  --host 127.0.0.1 \
  --port "${api_port}" \
  > "${run_dir}/monitor_api.log" 2>&1 &
api_pid=$!

for _attempt in $(seq 1 100); do
  if "${python_bin}" - "${api_port}" <<'PY' >/dev/null 2>&1
import sys
from urllib.request import urlopen
urlopen(f"http://127.0.0.1:{sys.argv[1]}/healthz", timeout=1).read()
PY
  then
    break
  fi
  sleep 0.1
done

"${python_bin}" - "${api_port}" "${run_id}" <<'PY' > "${run_dir}/trace_start.json"
import json
import sys
import urllib.request
port, run_id = sys.argv[1:]
body = json.dumps({
    "run_id": run_id,
    "mode": "full",
    "max_events": 200000,
    "include_args": 1,
}).encode()
request = urllib.request.Request(
    f"http://127.0.0.1:{port}/v1/trace/start",
    data=body,
    headers={"Content-Type": "application/json"},
)
with urllib.request.urlopen(request, timeout=10) as response:
    payload = json.loads(response.read())
assert payload["ok"] is True, payload
print(json.dumps(payload, indent=2, sort_keys=True))
PY

"${python_bin}" -u examples/phasee06/uflow_e06_trace_matrix.py \
  --socket "${socket_path}" \
  --device "${device}" \
  --profile "${profile}" \
  --sizes "${UF_E04_SIZES:-64MiB,256MiB}" \
  --direct-sizes "${UF_E04_DIRECT_SIZES:-256MiB}" \
  --warmup "${UF_E04_WARMUP:-0}" \
  --repeat "${UF_E04_REPEAT:-1}" \
  --trace-run-id "${run_id}" \
  --trace-root "${trace_root}" \
  --output-dir "${run_dir}" \
  --max-events 200000 \
  --trace-enabled 0 \
  > "${run_dir}/matrix.stdout" 2> "${run_dir}/matrix.stderr"

"${python_bin}" - "${api_port}" <<'PY' > "${run_dir}/trace_stop.json"
import json
import sys
import urllib.request
base = f"http://127.0.0.1:{sys.argv[1]}"
request = urllib.request.Request(
    base + "/v1/trace/stop",
    data=json.dumps({"flush": 1}).encode(),
    headers={"Content-Type": "application/json"},
)
with urllib.request.urlopen(request, timeout=60) as response:
    payload = json.loads(response.read())
assert payload["ok"] is True, payload
print(json.dumps(payload, indent=2, sort_keys=True))
PY

"${python_bin}" tools/phasee06/check_trace_v2.py \
  "${trace_root}/${run_id}" \
  --poll-events "${run_dir}/poll_events.json" \
  --case "${profile}" \
  --report "${run_dir}/trace_v2_check.json" \
  > "${run_dir}/trace_v2_check.stdout" 2> "${run_dir}/trace_v2_check.stderr"

"${python_bin}" tools/phasee04/check_monitor_v2.py \
  --base-url "http://127.0.0.1:${api_port}" \
  --profile "${profile}" \
  --run-id "${run_id}" \
  --output-dir "${run_dir}" \
  > "${run_dir}/monitor_v2_check.stdout" 2> "${run_dir}/monitor_v2_check.stderr"

{
  echo "daemon_pid=${daemon_pid}"
  echo "monitor_api_pid=${api_pid}"
  echo "daemon_socket=$(test -S "${socket_path}" && echo ready || echo missing)"
  echo "monitor_health=$(curl -fsS "http://127.0.0.1:${api_port}/healthz" | tr -d '\n')"
} > "${run_dir}/lifecycle_live.txt"

echo "UFLOW_E04_MONITOR_V2_PASS profile=${profile} run_dir=${run_dir}"
if [[ "${hold_after_check}" == "1" ]]; then
  echo "UFLOW_E04_MONITOR_V2_HOLD socket=${socket_path} api_port=${api_port}"
  while [[ ! -f "${run_dir}/release_hold" ]]; do
    sleep 1
  done
fi

cleanup
trap - EXIT INT TERM
"${python_bin}" tools/phasee06/check_remote_cleanup.py \
  --run-id "${run_id}" \
  --ignore-command-substring "examples/phasee04/run_monitor_v2_remote.sh" \
  --report "${run_dir}/cleanup_check.json" \
  > "${run_dir}/cleanup_check.stdout"
echo "${run_dir}"
