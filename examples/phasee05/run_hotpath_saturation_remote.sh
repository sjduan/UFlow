#!/usr/bin/env bash
set -euo pipefail

source /home/sj/git/all_env.sh 2>/dev/null || true
cd /home/sj/git/data-service

device="${UF_E05_DEVICE:-7}"
run_id="phasee05_hotpath_$(date +%Y%m%d_%H%M%S)"
run_dir="${UF_E05_RUN_DIR:-/tmp/proj_output/${run_id}}"
sock="/tmp/${run_id}.sock"
ddr_root="${UF_DDR_ROOT:-/dev/shm/${run_id}_ddr}"

mkdir -p "${run_dir}" "${ddr_root}"
rm -f "${sock}"

export LD_LIBRARY_PATH="/home/sj/git/data-service/build/lib:${LD_LIBRARY_PATH:-}"
export PYTHONPATH="/home/sj/git/data-service/sdk/python:${PYTHONPATH:-}"
export UF_ACL_LIB="${UF_ACL_LIB:-/home/sj/git/data-service/build/lib/libuf_acl_shim.so}"
export UF_DDR_ROOT="${ddr_root}"
export UF_PINNED_CHUNK_COUNT="${UF_PINNED_CHUNK_COUNT:-2}"
export UF_H2D_PINNED_CHUNK_BYTES="${UF_H2D_PINNED_CHUNK_BYTES:-16777216}"
export UF_D2H_PINNED_CHUNK_BYTES="${UF_D2H_PINNED_CHUNK_BYTES:-67108864}"
export UF_PINNED_MAX_BYTES="${UF_PINNED_MAX_BYTES:-0}"
export UF_PINNED_IDLE_TTL_MS="${UF_PINNED_IDLE_TTL_MS:-30000}"
export UF_H2D_MAX_LANES="${UF_H2D_MAX_LANES:-0}"
export UF_D2H_MAX_LANES="${UF_D2H_MAX_LANES:-0}"
export UF_DDR_MADVISE_HUGEPAGE="${UF_DDR_MADVISE_HUGEPAGE:-1}"
export UF_DDR_PRETOUCH_ON_CREATE="${UF_DDR_PRETOUCH_ON_CREATE:-1}"
export UF_D2H_REGISTER_DDR="${UF_D2H_REGISTER_DDR:-0}"
export UF_DDR_REGISTER_USE_V2="${UF_DDR_REGISTER_USE_V2:-0}"

daemon_bin="${UF_DAEMON_BIN:-./target/release/uf-daemon}"

{
  echo "UFLOW_E05_HOTPATH_BEGIN"
  echo "run_dir=${run_dir}"
  echo "device=${device}"
  echo "sizes=${UF_E05_SIZES:-1GiB}"
  echo "lanes=${UF_E05_LANES:-1,2,4,6,8}"
  echo "directions=${UF_E05_DIRECTIONS:-h2d,d2h,bidir}"
  echo "h2d_chunk=${UF_H2D_PINNED_CHUNK_BYTES}"
  echo "d2h_chunk=${UF_D2H_PINNED_CHUNK_BYTES}"
  echo "chunk_count=${UF_PINNED_CHUNK_COUNT}"
  echo "pinned_max=${UF_PINNED_MAX_BYTES}"
  echo "h2d_max_lanes=${UF_H2D_MAX_LANES}"
  echo "d2h_max_lanes=${UF_D2H_MAX_LANES}"
  echo "ddr_pretouch_on_create=${UF_DDR_PRETOUCH_ON_CREATE}"
  echo "ddr_madvise_hugepage=${UF_DDR_MADVISE_HUGEPAGE}"
  echo "d2h_register_ddr=${UF_D2H_REGISTER_DDR}"
  echo "ddr_register_use_v2=${UF_DDR_REGISTER_USE_V2}"
} | tee "${run_dir}/environment.txt"

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
}
trap cleanup EXIT

for _ in $(seq 1 50); do
  test -S "${sock}" && break
  sleep 0.2
done
test -S "${sock}"

set +e
python3 -u examples/phasee05/uflow_e05_hotpath_saturation.py \
  --socket "${sock}" \
  --device "${device}" \
  --sizes "${UF_E05_SIZES:-1GiB}" \
  --lanes "${UF_E05_LANES:-1,2,4,6,8}" \
  --directions "${UF_E05_DIRECTIONS:-h2d,d2h,bidir}" \
  --mode "${UF_E05_MODE:-auto}" \
  --ddr-target "${UF_DDR_TARGET:-host:0}" \
  > "${run_dir}/hotpath.stdout" 2> "${run_dir}/hotpath.stderr"
code=$?
set -e
echo "${code}" > "${run_dir}/exit_code"

echo "${run_dir}"
tail -n 200 "${run_dir}/hotpath.stdout" || true
tail -n 120 "${run_dir}/hotpath.stderr" || true
tail -n 200 "${run_dir}/daemon.log" || true
exit "${code}"
