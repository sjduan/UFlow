#!/usr/bin/env bash
set -euo pipefail

source /home/sj/git/all_env.sh 2>/dev/null || true
cd /home/sj/git/data-service

src_device="${UF_A08_SRC_DEVICE:-7}"
dst_device="${UF_A08_DST_DEVICE:-6}"
bytes="${UF_A08_BYTES:-1048576}"
run_id="phasea08_gatea_$(date +%Y%m%d_%H%M%S)"
run_dir="/tmp/proj_output/${run_id}"
sock="/tmp/${run_id}.sock"
ddr_root="/tmp/${run_id}_ddr"

mkdir -p "${run_dir}" "${ddr_root}"
rm -f "${sock}"

export LD_LIBRARY_PATH="/home/sj/git/data-service/build/lib:${LD_LIBRARY_PATH:-}"
export PYTHONPATH="/home/sj/git/data-service/sdk/python:${PYTHONPATH:-}"
export UF_ACL_LIB="${UF_ACL_LIB:-/home/sj/git/data-service/build/lib/libuf_acl_shim.so}"
export UF_DDR_ROOT="${ddr_root}"

./target/debug/uf-daemon \
  --device "${src_device}" \
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
python examples/phasea08/uflow_phasee_idl_gate_a.py all \
  --socket "${sock}" \
  --src-device "${src_device}" \
  --dst-device "${dst_device}" \
  --require-cross-device \
  --bytes "${bytes}" \
  --ddr-target host:0 \
  --ddr-dst-target host:0 \
  > "${run_dir}/gate_a.stdout" 2> "${run_dir}/gate_a.stderr"
code=$?
set -e
echo "${code}" > "${run_dir}/exit_code"

echo "${run_dir}"
tail -n 120 "${run_dir}/gate_a.stdout" || true
tail -n 120 "${run_dir}/gate_a.stderr" || true
tail -n 120 "${run_dir}/daemon.log" || true
exit "${code}"
