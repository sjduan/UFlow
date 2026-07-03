#!/usr/bin/env bash
set -euo pipefail

source /home/sj/git/all_env.sh 2>/dev/null || true

device="${UF_A08_DEVICE:-7}"
model_dir="${UF_A08_MODEL_DIR:-/data/models/Qwen/Qwen3-14B}"
prompt="${UF_A08_PROMPT:-Huawei is}"
max_seq_len="${UF_A08_MAX_SEQ_LEN:-512}"
max_new_tokens="${UF_A08_MAX_NEW_TOKENS:-1}"
run_id="phasea08_gateb_prefill_layer_$(date +%Y%m%d_%H%M%S)"
run_dir="/tmp/proj_output/${run_id}"
mkdir -p "${run_dir}/kernels_fused" "${run_dir}/kernels_layer"

cd /home/sj/git/pypto-serving

export PTO2_RING_HEAP="${PTO2_RING_HEAP:-4294967296}"
export PTO2_RING_TASK_WINDOW="${PTO2_RING_TASK_WINDOW:-1048576}"
export PTO2_RING_DEP_POOL="${PTO2_RING_DEP_POOL:-1048576}"
export PYTHONPATH="/home/sj/git/pypto-serving:/home/sj/git/data-service/sdk/python:/home/sj/git/pypto/python:/home/sj/git/pypto/runtime/python:${PYTHONPATH:-}"
export UF_ENABLE=0

common_args=(
  examples/model/qwen3_14b/npu_generate.py
  --model-dir "${model_dir}"
  --prompt "${prompt}"
  --platform "${UF_PYPTO_PLATFORM:-a2a3}"
  --device-id "${device}"
  --max-seq-len "${max_seq_len}"
  --max-new-tokens "${max_new_tokens}"
)

set +e
python "${common_args[@]}" \
  --save-kernels-dir "${run_dir}/kernels_fused" \
  > "${run_dir}/fused.stdout" 2> "${run_dir}/fused.stderr"
fused_code=$?
set -e
echo "${fused_code}" > "${run_dir}/fused.exit_code"

if [[ "${fused_code}" -ne 0 ]]; then
  echo "${run_dir}"
  echo "FUSED_FAILED"
  tail -n 120 "${run_dir}/fused.stdout" || true
  tail -n 160 "${run_dir}/fused.stderr" || true
  exit "${fused_code}"
fi

set +e
UF_A08_COMPILE_PREFILL_LAYER=1 \
UF_A08_PREFILL_LAYER_TASKS=1 \
python "${common_args[@]}" \
  --save-kernels-dir "${run_dir}/kernels_layer" \
  > "${run_dir}/layer.stdout" 2> "${run_dir}/layer.stderr"
layer_code=$?
set -e
echo "${layer_code}" > "${run_dir}/layer.exit_code"

python - <<PY > "${run_dir}/compare.txt"
from pathlib import Path
run_dir = Path("${run_dir}")
def extract(path):
    text = path.read_text(errors="replace")
    lines = [line for line in text.splitlines() if "token_ids:" in line or line.startswith("token_ids")]
    return lines[-1] if lines else ""
fused = extract(run_dir / "fused.stdout")
layer = extract(run_dir / "layer.stdout")
print(f"fused_token_line={fused}")
print(f"layer_token_line={layer}")
print(f"match={str(bool(fused) and fused == layer).lower()}")
PY

echo "${run_dir}"
cat "${run_dir}/compare.txt"
tail -n 80 "${run_dir}/fused.stdout" || true
tail -n 120 "${run_dir}/layer.stdout" || true
tail -n 160 "${run_dir}/layer.stderr" || true

if [[ "${layer_code}" -ne 0 ]]; then
  exit "${layer_code}"
fi
grep -q "match=true" "${run_dir}/compare.txt"
