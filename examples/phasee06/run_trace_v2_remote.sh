#!/usr/bin/env bash
set -euo pipefail

source /home/sj/git/all_env.sh 2>/dev/null || true

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

device="${UF_E06_DEVICE:?UF_E06_DEVICE must be selected from the current npu-smi snapshot}"
profile="${UF_E06_PROFILE:-regular}"
run_id="${UF_E06_RUN_ID:-phasee06_trace_v2_${profile}_$(date +%Y%m%d_%H%M%S)}"
if [[ ! "${run_id}" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "UF_E06_RUN_ID must be a single safe path component" >&2
  exit 2
fi
run_dir="${UF_E06_RUN_DIR:-/tmp/proj_output/${run_id}}"
trace_root="${run_dir}/traces"
socket_path="/tmp/${run_id}.sock"
ddr_root="/dev/shm/${run_id}_ddr"
ssd_root="/data/${run_id}_ssd"
api_port="${UF_E06_API_PORT:-18082}"
daemon_bin="${UF_DAEMON_BIN:-./target/release/uf-daemon}"
python_bin="${UF_E06_PYTHON:-python3}"

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
if [[ "${profile}" == "cancel" ]]; then
  export UF_TRANSFER_WORKERS=1
fi

{
  echo "run_id=${run_id}"
  echo "profile=${profile}"
  echo "repo_root=${repo_root}"
  echo "hostname=$(cat /etc/hostname 2>/dev/null || echo unknown)"
  echo "kernel=$(uname -r)"
  echo "device=${device}"
  echo "code_commit=${UF_E06_CODE_COMMIT:-$(git rev-parse HEAD 2>/dev/null || echo isolated-worktree)}"
  echo "sizes=${UF_E06_SIZES:-64MiB,256MiB}"
  echo "direct_sizes=${UF_E06_DIRECT_SIZES:-256MiB}"
  echo "warmup=${UF_E06_WARMUP:-1}"
  echo "repeat=${UF_E06_REPEAT:-5}"
  echo "max_events=${UF_E06_MAX_EVENTS:-200000}"
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

cleanup() {
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
trap cleanup EXIT

for _ in $(seq 1 100); do
  [[ -S "${socket_path}" ]] && break
  sleep 0.2
done
[[ -S "${socket_path}" ]]

UF_MONITOR_PORT="${api_port}" \
UF_TRACE_OUTPUT_DIR="${trace_root}" \
"${python_bin}" -u tools/uflow_monitor_api.py \
  --socket "${socket_path}" \
  --host 127.0.0.1 \
  --port "${api_port}" \
  > "${run_dir}/monitor_api.log" 2>&1 &
api_pid=$!

for _ in $(seq 1 100); do
  if "${python_bin}" - "${api_port}" <<'PY' >/dev/null 2>&1
import sys
import urllib.request

urllib.request.urlopen(f"http://127.0.0.1:{sys.argv[1]}/healthz", timeout=1).read()
PY
  then
    break
  fi
  sleep 0.1
done

max_events="${UF_E06_MAX_EVENTS:-200000}"
if [[ "${profile}" == "drop" && "${max_events}" == "200000" ]]; then
  max_events=8
fi

"${python_bin}" -u examples/phasee06/uflow_e06_trace_matrix.py \
  --socket "${socket_path}" \
  --device "${device}" \
  --profile "${profile}" \
  --sizes "${UF_E06_SIZES:-64MiB,256MiB}" \
  --direct-sizes "${UF_E06_DIRECT_SIZES:-256MiB}" \
  --warmup "${UF_E06_WARMUP:-1}" \
  --repeat "${UF_E06_REPEAT:-5}" \
  --trace-run-id "${run_id}" \
  --trace-root "${trace_root}" \
  --output-dir "${run_dir}" \
  --max-events "${max_events}" \
  > "${run_dir}/matrix.stdout" 2> "${run_dir}/matrix.stderr"

artifact_dir="${trace_root}/${run_id}"
set +e
"${python_bin}" tools/phasee06/check_trace_v2.py \
  "${artifact_dir}" \
  --poll-events "${run_dir}/poll_events.json" \
  --case "${profile}" \
  --report "${run_dir}/trace_check.json" \
  > "${run_dir}/trace_check.stdout" 2> "${run_dir}/trace_check.stderr"
checker_code=$?
set -e

if [[ "${profile}" == "incomplete" || "${profile}" == "drop" ]]; then
  if [[ "${checker_code}" -eq 0 ]]; then
    echo "negative profile ${profile} unexpectedly passed trace checker" >&2
    exit 1
  fi
  "${python_bin}" - "${artifact_dir}/trace_summary.json" "${profile}" <<'PY'
import json
import sys

summary = json.load(open(sys.argv[1], encoding="utf-8"))
profile = sys.argv[2]
assert summary["complete"] is False, summary
if profile == "incomplete":
    assert summary["incomplete_event_ids"], summary
else:
    assert int(summary["dropped_events"]) > 0, summary
PY
else
  if [[ "${checker_code}" -ne 0 ]]; then
    cat "${run_dir}/trace_check.stdout" >&2
    exit "${checker_code}"
  fi
fi

"${python_bin}" - "${api_port}" "${run_id}" "${run_dir}" <<'PY' > "${run_dir}/http_artifact_check.json"
import json
import sys
import urllib.parse
import urllib.request
from pathlib import Path

port, run_id, run_dir = sys.argv[1:]
base = f"http://127.0.0.1:{port}"
download_dir = Path(run_dir) / "http_download"
download_dir.mkdir(parents=True, exist_ok=True)
results = {}
for endpoint in ("/healthz", "/v1/observability", "/v1/trace/status"):
    with urllib.request.urlopen(base + endpoint, timeout=5) as response:
        results[endpoint] = {"status": response.status, "bytes": len(response.read())}
for name in (
    "trace_events.json",
    "trace_summary.json",
    "trace_summary.csv",
    "transfer_summary.csv",
    "summary.md",
):
    query = urllib.parse.urlencode({"run_id": run_id, "file": name})
    with urllib.request.urlopen(base + "/v1/trace/artifact?" + query, timeout=30) as response:
        data = response.read()
        (download_dir / name).write_bytes(data)
        results[name] = {"status": response.status, "bytes": len(data)}
print(json.dumps(results, indent=2, sort_keys=True))
PY

echo "${run_dir}"
cat "${run_dir}/trace_check.stdout"
