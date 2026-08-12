#!/usr/bin/env bash
set -euo pipefail

source /home/sj/git/all_env.sh 2>/dev/null || true

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

device="${UF_E06_DEVICE:?UF_E06_DEVICE must be selected from the current npu-smi snapshot}"
run_id="${UF_E06_RUN_ID:-phasee06_trace_disabled_$(date +%Y%m%d_%H%M%S)}"
if [[ ! "${run_id}" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "UF_E06_RUN_ID must be a single safe path component" >&2
  exit 2
fi
run_dir="${UF_E06_RUN_DIR:-/tmp/proj_output/${run_id}}"
trace_root="${run_dir}/traces"
socket_path="/tmp/${run_id}.sock"
ddr_root="/dev/shm/${run_id}_ddr"
ssd_root="/data/${run_id}_ssd"
daemon_bin="${UF_DAEMON_BIN:-./target/release/uf-daemon}"
python_bin="${UF_E06_PYTHON:-python3}"

mkdir -p "${run_dir}" "${trace_root}" "${ddr_root}" "${ssd_root}"
rm -f "${socket_path}"

export LD_LIBRARY_PATH="${repo_root}/build/lib:${LD_LIBRARY_PATH:-}"
export PYTHONPATH="${repo_root}/sdk/python:${PYTHONPATH:-}"
export UF_ACL_LIB="${UF_ACL_LIB:-${repo_root}/build/lib/libuf_acl_shim.so}"
export UF_DDR_ROOT="${ddr_root}"
export UF_SSD_ROOT="${ssd_root}"
export UF_TRACE_ENABLE=0
export UF_TRACE_OUTPUT_DIR="${trace_root}"
export UF_SSD_DIRECT_ENABLE="${UF_SSD_DIRECT_ENABLE:-1}"
export UF_SSD_DIRECT_AUTO="${UF_SSD_DIRECT_AUTO:-1}"
export UF_SSD_HBM_DIRECT_MIN_BYTES="${UF_SSD_HBM_DIRECT_MIN_BYTES:-256MiB}"
export UF_PINNED_CHUNK_COUNT="${UF_PINNED_CHUNK_COUNT:-2}"
export UF_H2D_PINNED_CHUNK_BYTES="${UF_H2D_PINNED_CHUNK_BYTES:-16777216}"
export UF_D2H_PINNED_CHUNK_BYTES="${UF_D2H_PINNED_CHUNK_BYTES:-67108864}"
export UF_DDR_MADVISE_HUGEPAGE="${UF_DDR_MADVISE_HUGEPAGE:-1}"
export UF_DDR_PRETOUCH_ON_CREATE="${UF_DDR_PRETOUCH_ON_CREATE:-1}"
unset UF_ENABLE_TEST_FAILPOINTS || true
unset UF_TEST_SSD_DIRECT_FAIL_STAGE || true

{
  echo "run_id=${run_id}"
  echo "trace_enabled=0"
  echo "repo_root=${repo_root}"
  echo "hostname=$(cat /etc/hostname 2>/dev/null || echo unknown)"
  echo "kernel=$(uname -r)"
  echo "device=${device}"
  echo "code_commit=${UF_E06_CODE_COMMIT:-$(git rev-parse HEAD 2>/dev/null || echo isolated-worktree)}"
  echo "sizes=${UF_E06_SIZES:-64MiB,256MiB}"
  echo "direct_sizes=${UF_E06_DIRECT_SIZES:-256MiB}"
  echo "warmup=${UF_E06_WARMUP:-1}"
  echo "repeat=${UF_E06_REPEAT:-5}"
  echo "trace_root=${trace_root}"
  echo "ddr_root=${ddr_root}"
  echo "ssd_root=${ssd_root}"
  npu-smi info || true
} > "${run_dir}/environment.txt"

"${daemon_bin}" \
  --device "${device}" \
  --socket "${socket_path}" \
  --startup-probe-bytes "${UF_HBM_STARTUP_PROBE_BYTES:-1048576}" \
  > "${run_dir}/daemon.log" 2>&1 &
daemon_pid=$!

cleanup() {
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

"${python_bin}" -u examples/phasee06/uflow_e06_trace_matrix.py \
  --socket "${socket_path}" \
  --device "${device}" \
  --profile regular \
  --sizes "${UF_E06_SIZES:-64MiB,256MiB}" \
  --direct-sizes "${UF_E06_DIRECT_SIZES:-256MiB}" \
  --warmup "${UF_E06_WARMUP:-1}" \
  --repeat "${UF_E06_REPEAT:-5}" \
  --trace-enabled 0 \
  --trace-run-id "${run_id}" \
  --trace-root "${trace_root}" \
  --output-dir "${run_dir}" \
  > "${run_dir}/matrix.stdout" 2> "${run_dir}/matrix.stderr"

"${python_bin}" - "${run_dir}/poll_events.json" "${trace_root}" <<'PY' \
  > "${run_dir}/disabled_check.json"
import json
import sys
from pathlib import Path

poll_path = Path(sys.argv[1])
trace_root = Path(sys.argv[2])
rows = json.loads(poll_path.read_text(encoding="utf-8"))["events"]
trace_files = sorted(str(path) for path in trace_root.rglob("*") if path.is_file())
non_warmup = [row for row in rows if not row.get("warmup")]
failed = [row["event_id"] for row in non_warmup if row.get("status") != "complete"]
result = {
    "status": "PASS" if non_warmup and not failed and not trace_files else "FAIL",
    "transfer_count": len(non_warmup),
    "failed_event_ids": failed,
    "trace_file_count": len(trace_files),
    "trace_files": trace_files,
}
print(json.dumps(result, indent=2, sort_keys=True))
if result["status"] != "PASS":
    raise SystemExit(1)
PY

echo "${run_dir}"
cat "${run_dir}/disabled_check.json"
