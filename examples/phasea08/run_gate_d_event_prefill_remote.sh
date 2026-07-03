#!/usr/bin/env bash
set -euo pipefail

source /home/sj/git/all_env.sh 2>/dev/null || true
cd /home/sj/git/data-service

device="${UF_A08_DEVICE:-7}"
model_dir="${UF_A08_MODEL_DIR:-/data/models/Qwen/Qwen3-14B}"
prompt="${UF_A08_PROMPT:-Huawei is}"
max_seq_len="${UF_A08_MAX_SEQ_LEN:-128}"
max_new_tokens="${UF_A08_MAX_NEW_TOKENS:-1}"
run_id="phasea08_gated_event_prefill_$(date +%Y%m%d_%H%M%S)"
run_dir="/tmp/proj_output/${run_id}"
sock="/tmp/${run_id}.sock"
ddr_root="/tmp/${run_id}_ddr"

mkdir -p "${run_dir}/kernels" "${ddr_root}"
rm -f "${sock}"

export LD_LIBRARY_PATH="/home/sj/git/data-service/build/lib:${LD_LIBRARY_PATH:-}"
export PYTHONPATH="/home/sj/git/data-service/sdk/python:/home/sj/git/pypto-serving:/home/sj/git/pypto/python:/home/sj/git/pypto/runtime/python:${PYTHONPATH:-}"
export UF_ACL_LIB="${UF_ACL_LIB:-/home/sj/git/data-service/build/lib/libuf_acl_shim.so}"
export UF_DDR_ROOT="${ddr_root}"
export UF_ENABLE=1
export UF_SOCKET="${sock}"
export UF_TARGET_DEVICE="${device}"
export UF_MANAGE_WEIGHTS=0
export UF_MANAGE_KVCACHE=0
export UF_FAIL_FAST=1
export PYPTO_RUNTIME_ROOT="${PYPTO_RUNTIME_ROOT:-/home/sj/git/pypto/runtime}"
export PTO2_RING_HEAP="${PTO2_RING_HEAP:-4294967296}"
export PTO2_RING_TASK_WINDOW="${PTO2_RING_TASK_WINDOW:-1048576}"
export PTO2_RING_DEP_POOL="${PTO2_RING_DEP_POOL:-1048576}"

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

cd /home/sj/git/pypto-serving
set +e
UF_A08_COMPILE_PREFILL_LAYER=1 \
UF_A08_PREFILL_LAYER_TASKS=1 \
UF_A08_PREFILL_LAYER_DEBUG=1 \
UF_A08_PREFILL_EVENT_SMOKE=1 \
python examples/model/qwen3_14b/npu_generate.py \
  --model-dir "${model_dir}" \
  --prompt "${prompt}" \
  --platform "${UF_PYPTO_PLATFORM:-a2a3}" \
  --device-id "${device}" \
  --max-seq-len "${max_seq_len}" \
  --max-new-tokens "${max_new_tokens}" \
  --save-kernels-dir "${run_dir}/kernels" \
  > "${run_dir}/qwen.stdout" 2> "${run_dir}/qwen.stderr"
code=$?
set -e
echo "${code}" > "${run_dir}/exit_code"

python - <<PY > "${run_dir}/summary.txt"
from pathlib import Path
run_dir = Path("${run_dir}")
stdout = (run_dir / "qwen.stdout").read_text(errors="replace")
token_lines = [line for line in stdout.splitlines() if "token_ids:" in line or line.startswith("token_ids")]
event_waits = [line for line in stdout.splitlines() if "prefill_layer_event_wait" in line]
layer_done = [line for line in stdout.splitlines() if "prefill_layer_done" in line]
print(f"token_line={token_lines[-1] if token_lines else ''}")
print(f"event_wait_count={len(event_waits)}")
print(f"layer_done_count={len(layer_done)}")
print(f"match_token={str(bool(token_lines) and token_lines[-1].strip() == 'token_ids: [264]').lower()}")
print(f"all_layers_waited={str(len(event_waits) == 40 and len(layer_done) == 40).lower()}")
PY

echo "${run_dir}"
cat "${run_dir}/summary.txt"
tail -n 140 "${run_dir}/qwen.stdout" || true
tail -n 160 "${run_dir}/qwen.stderr" || true
tail -n 160 "${run_dir}/daemon.log" || true

if [[ "${code}" -ne 0 ]]; then
  exit "${code}"
fi
grep -q "match_token=true" "${run_dir}/summary.txt"
grep -q "all_layers_waited=true" "${run_dir}/summary.txt"
