#!/usr/bin/env bash
set -euo pipefail

source /home/sj/git/all_env.sh 2>/dev/null || true
cd /home/sj/git/data-service

device="${UF_E03_DEVICE:-7}"
run_id="phasee03_direct_fastpath_matrix_$(date +%Y%m%d_%H%M%S)"
run_dir="${UF_E03_RUN_DIR:-/tmp/proj_output/${run_id}}"
sizes="${UF_E03_SIZES:-64MiB,256MiB,1GiB,2GiB}"
directions="${UF_E03_DIRECTIONS:-h2d,d2h}"

mkdir -p "${run_dir}"

export LD_LIBRARY_PATH="/home/sj/git/data-service/build/lib:${LD_LIBRARY_PATH:-}"

{
  echo "UFLOW_E03_DIRECT_FASTPATH_MATRIX_BEGIN"
  echo "run_dir=${run_dir}"
  echo "device=${device}"
  echo "sizes=${sizes}"
  echo "directions=${directions}"
  echo "warmups=${UF_E03_WARMUPS:-1}"
  echo "repeats=${UF_E03_REPEATS:-3}"
  echo "chunk_bytes=${UF_E03_CHUNK_BYTES:-16MiB}"
  echo "chunk_count=${UF_E03_CHUNK_COUNT:-2}"
} | tee "${run_dir}/environment.txt"

IFS=',' read -r -a size_items <<< "${sizes}"
IFS=',' read -r -a direction_items <<< "${directions}"

for direction in "${direction_items[@]}"; do
  direction="$(echo "${direction}" | xargs)"
  for size in "${size_items[@]}"; do
    size="$(echo "${size}" | xargs)"
    out="${run_dir}/${direction}_${size}"
    mkdir -p "${out}"
    echo "UFLOW_E03_DIRECT_FASTPATH_CASE direction=${direction} size=${size} output=${out}" | tee -a "${run_dir}/cases.log"
    ./build/bin/acl_h2d_memfd_fastpath_matrix \
      --device "${device}" \
      --direction "${direction}" \
      --bytes "${size}" \
      --chunk-bytes "${UF_E03_CHUNK_BYTES:-16MiB}" \
      --chunk-count "${UF_E03_CHUNK_COUNT:-2}" \
      --warmups "${UF_E03_WARMUPS:-1}" \
      --repeats "${UF_E03_REPEATS:-3}" \
      --output-dir "${out}" \
      > "${out}/stdout.log" 2> "${out}/stderr.log"
    tail -n 20 "${out}/stdout.log" | tee -a "${run_dir}/cases.log"
  done
done

python3 - "${run_dir}" <<'PY' > "${run_dir}/matrix_summary.csv"
import csv
import json
import sys
from pathlib import Path

run_dir = Path(sys.argv[1])
rows = []
for path in sorted(run_dir.glob("*_*/*_stage_summary.json")):
    data = json.loads(path.read_text())
    direction = data["direction"]
    size = data["bytes"]
    for result in data["results"]:
        rows.append({
            "direction": direction,
            "bytes": size,
            "selection": result["selection"],
            "status": result["status"],
            "hot_avg_gib_s": result["hot_avg_gib_s"],
            "hot_min_gib_s": result["hot_min_gib_s"],
            "hot_max_gib_s": result["hot_max_gib_s"],
            "register_ms": result["register_ms"],
            "warmup_ms": result["warmup_ms"],
            "verified": result["verified"],
            "file": str(path.relative_to(run_dir)),
        })

writer = csv.DictWriter(sys.stdout, fieldnames=[
    "direction", "bytes", "selection", "status", "hot_avg_gib_s",
    "hot_min_gib_s", "hot_max_gib_s", "register_ms", "warmup_ms",
    "verified", "file",
])
writer.writeheader()
writer.writerows(rows)
PY

echo "${run_dir}"
tail -n 40 "${run_dir}/matrix_summary.csv"
