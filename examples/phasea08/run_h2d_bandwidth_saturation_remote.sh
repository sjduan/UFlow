#!/usr/bin/env bash
set -euo pipefail

source /home/sj/git/all_env.sh 2>/dev/null || true
cd /home/sj/git/data-service

device="${UF_A08_DEVICE:-7}"
run_id="phasea08_h2d_bw_$(date +%Y%m%d_%H%M%S)"
run_dir="/tmp/proj_output/${run_id}"
sock="/tmp/${run_id}.sock"
ddr_base="${UF_H2D_BW_DDR_BASE:-/tmp}"
ddr_root="${UF_DDR_ROOT:-${ddr_base}/${run_id}_ddr}"

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

daemon_bin="${UF_DAEMON_BIN:-./target/release/uf-daemon}"

"${daemon_bin}" \
  --device "${device}" \
  --socket "${sock}" \
  --startup-probe-bytes "${UF_HBM_STARTUP_PROBE_BYTES:-1048576}" \
  > "${run_dir}/daemon.log" 2>&1 &
daemon_pid=$!
echo "${daemon_pid}" > "${run_dir}/daemon.pid"

cleanup() {
  python - <<PY >/dev/null 2>&1 || true
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
python examples/phasea08/uflow_h2d_bandwidth_saturation.py \
  --socket "${sock}" \
  --device "${device}" \
  --sizes "${UF_H2D_BW_SIZES:-256MiB,512MiB,1GiB}" \
  --lanes "${UF_H2D_BW_LANES:-1,2}" \
  --mode "${UF_H2D_BW_MODE:-pinned_async}" \
  --ddr-target "${UF_DDR_TARGET:-host:0}" \
  > "${run_dir}/h2d_bw.stdout" 2> "${run_dir}/h2d_bw.stderr"
code=$?
set -e
echo "${code}" > "${run_dir}/exit_code"

echo "${run_dir}"
tail -n 160 "${run_dir}/h2d_bw.stdout" || true
tail -n 120 "${run_dir}/h2d_bw.stderr" || true
tail -n 160 "${run_dir}/daemon.log" || true
exit "${code}"
