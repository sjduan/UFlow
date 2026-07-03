#!/usr/bin/env bash
set -euo pipefail

source /home/sj/git/all_env.sh 2>/dev/null || true
cd /home/sj/git/data-service

device="${UF_A08_DEVICE:-7}"
bytes="${UF_A08_H0_BYTES:-33554432}"
run_id="phasea08_gateh0_hotpath_$(date +%Y%m%d_%H%M%S)"
run_dir="/tmp/proj_output/${run_id}"
sock="/tmp/${run_id}.sock"
ddr_root="/tmp/${run_id}_ddr"

mkdir -p "${run_dir}" "${ddr_root}"
rm -f "${sock}"

export LD_LIBRARY_PATH="/home/sj/git/data-service/build/lib:${LD_LIBRARY_PATH:-}"
export PYTHONPATH="/home/sj/git/data-service/sdk/python:${PYTHONPATH:-}"
export UF_ACL_LIB="${UF_ACL_LIB:-/home/sj/git/data-service/build/lib/libuf_acl_shim.so}"
export UF_DDR_ROOT="${ddr_root}"
export UF_DDR_USE_MEMFD="${UF_DDR_USE_MEMFD:-1}"
export UF_DDR_MADVISE_HUGEPAGE="${UF_DDR_MADVISE_HUGEPAGE:-1}"
export UF_DDR_PRETOUCH_ON_CREATE="${UF_DDR_PRETOUCH_ON_CREATE:-1}"
export UF_PINNED_CHUNK_COUNT="${UF_PINNED_CHUNK_COUNT:-2}"
export UF_H2D_PINNED_CHUNK_BYTES="${UF_H2D_PINNED_CHUNK_BYTES:-16777216}"
export UF_D2H_PINNED_CHUNK_BYTES="${UF_D2H_PINNED_CHUNK_BYTES:-67108864}"
export UF_DAEMON_ACCEPT_POLL_US="${UF_DAEMON_ACCEPT_POLL_US:-1000}"

./target/debug/uf-daemon \
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
python examples/phasea08/uflow_gate_h0_hotpath_smoke.py \
  --socket "${sock}" \
  --device "${device}" \
  --bytes "${bytes}" \
  --mode "${UF_A08_H0_MODE:-auto}" \
  --ddr-target "${UF_DDR_TARGET:-host:0}" \
  > "${run_dir}/gate_h0.stdout" 2> "${run_dir}/gate_h0.stderr"
code=$?
set -e
echo "${code}" > "${run_dir}/exit_code"

echo "${run_dir}"
tail -n 120 "${run_dir}/gate_h0.stdout" || true
tail -n 120 "${run_dir}/gate_h0.stderr" || true
tail -n 160 "${run_dir}/daemon.log" || true
exit "${code}"
