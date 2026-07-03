#!/usr/bin/env bash
set -euo pipefail

source /home/sj/git/all_env.sh 2>/dev/null || true
cd /home/sj/git/data-service

device="${UF_TARGET_DEVICE:-${UF_E08_DEVICE:-7}}"
run_id="phasee08_low_overhead_executor_$(date +%Y%m%d_%H%M%S)"
run_dir="${UF_E08_RUN_DIR:-/tmp/proj_output/${run_id}}"
sock="/tmp/${run_id}.sock"
ddr_root="${UF_DDR_ROOT:-/dev/shm/${run_id}_ddr}"

mkdir -p "${run_dir}" "${ddr_root}"
rm -f "${sock}"

export LD_LIBRARY_PATH="/home/sj/git/data-service/build/lib:${LD_LIBRARY_PATH:-}"
export PYTHONPATH="/home/sj/git/data-service/sdk/python:${PYTHONPATH:-}"
export UF_ACL_LIB="${UF_ACL_LIB:-/home/sj/git/data-service/build/lib/libuf_acl_shim.so}"
export UF_DDR_ROOT="${ddr_root}"
export UF_DDR_USE_MEMFD="${UF_DDR_USE_MEMFD:-1}"
export UF_DDR_MADVISE_HUGEPAGE="${UF_DDR_MADVISE_HUGEPAGE:-1}"
export UF_DDR_PRETOUCH_ON_CREATE="${UF_DDR_PRETOUCH_ON_CREATE:-1}"
export UF_TRANSFER_WORKERS="${UF_TRANSFER_WORKERS:-1}"
export UF_DIRECT_H2D_MAX_LANES="${UF_DIRECT_H2D_MAX_LANES:-1}"
export UF_DIRECT_D2H_MAX_LANES="${UF_DIRECT_D2H_MAX_LANES:-1}"
export UF_DIRECT_IDLE_TTL_MS="${UF_DIRECT_IDLE_TTL_MS:-30000}"
export UF_TRACE_ENABLE="${UF_TRACE_ENABLE:-0}"
export UF_TRACE_RUN_ID="${UF_TRACE_RUN_ID:-${run_id}_trace}"
export UF_TRACE_OUTPUT_DIR="${UF_TRACE_OUTPUT_DIR:-${run_dir}/traces}"

daemon_bin="${UF_DAEMON_BIN:-./target/release/uf-daemon}"

{
  echo "UFLOW_E08_LOW_OVERHEAD_BEGIN"
  echo "run_dir=${run_dir}"
  echo "device=${device}"
  echo "sizes=${UF_E08_SIZES:-64MiB,256MiB,1GiB,2GiB}"
  echo "directions=${UF_E08_DIRECTIONS:-h2d,d2h}"
  echo "repeats=${UF_E08_REPEATS:-3}"
  echo "mode=${UF_E08_MODE:-auto}"
  echo "transfer_workers=${UF_TRANSFER_WORKERS}"
  echo "direct_h2d_max_lanes=${UF_DIRECT_H2D_MAX_LANES}"
  echo "direct_d2h_max_lanes=${UF_DIRECT_D2H_MAX_LANES}"
  echo "direct_idle_ttl_ms=${UF_DIRECT_IDLE_TTL_MS}"
  echo "ddr_use_memfd=${UF_DDR_USE_MEMFD}"
  echo "ddr_pretouch_on_create=${UF_DDR_PRETOUCH_ON_CREATE}"
  echo "ddr_madvise_hugepage=${UF_DDR_MADVISE_HUGEPAGE}"
} | tee "${run_dir}/environment.txt"

npu-smi info > "${run_dir}/npu_smi_before.txt" 2>&1 || true

"${daemon_bin}" \
  --device "${device}" \
  --socket "${sock}" \
  --startup-probe-bytes "${UF_HBM_STARTUP_PROBE_BYTES:-1048576}" \
  > "${run_dir}/daemon.log" 2>&1 &
daemon_pid=$!
echo "${daemon_pid}" > "${run_dir}/daemon.pid"

cleanup() {
  python3 - <<PY >/dev/null 2>&1 || true
import socket
sock_path = "${sock}"
try:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(sock_path)
    s.sendall(b"op=ShutdownDaemon\n")
    s.close()
except Exception:
    pass
PY
  wait "${daemon_pid}" >/dev/null 2>&1 || true
  rm -f "${sock}" || true
  rm -rf "${ddr_root}" || true
}
trap cleanup EXIT

for _ in $(seq 1 50); do
  test -S "${sock}" && break
  sleep 0.2
done
test -S "${sock}"

set +e
python3 -u examples/phasee08/uflow_e08_low_overhead_executor.py \
  --socket "${sock}" \
  --device "${device}" \
  --sizes "${UF_E08_SIZES:-64MiB,256MiB,1GiB,2GiB}" \
  --directions "${UF_E08_DIRECTIONS:-h2d,d2h}" \
  --repeats "${UF_E08_REPEATS:-3}" \
  --mode "${UF_E08_MODE:-auto}" \
  --ddr-target "${UF_DDR_TARGET:-host:0}" \
  --output-dir "${run_dir}" \
  > "${run_dir}/benchmark.stdout" 2> "${run_dir}/benchmark.stderr"
code=$?
set -e
echo "${code}" > "${run_dir}/exit_code"

if [[ "${UF_TRACE_ENABLE}" == "1" ]]; then
  python3 - <<PY >/dev/null 2>&1 || true
import socket
sock_path = "${sock}"
try:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(sock_path)
    s.sendall(b"op=FlushTrace\n")
    s.close()
except Exception:
    pass
PY
fi

npu-smi info > "${run_dir}/npu_smi_after.txt" 2>&1 || true

echo "${run_dir}"
tail -n 200 "${run_dir}/benchmark.stdout" || true
tail -n 120 "${run_dir}/benchmark.stderr" || true
tail -n 200 "${run_dir}/daemon.log" || true
exit "${code}"
