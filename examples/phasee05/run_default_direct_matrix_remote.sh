#!/usr/bin/env bash
set -euo pipefail

source /home/sj/git/all_env.sh 2>/dev/null || true
cd /home/sj/git/data-service

device="${UF_E05_DEVICE:-7}"
run_id="phasee05_default_direct_matrix_$(date +%Y%m%d_%H%M%S)"
run_dir="${UF_E05_RUN_DIR:-/tmp/proj_output/${run_id}}"
sizes="${UF_E05_SIZES:-64MiB,256MiB,1GiB,2GiB}"
directions="${UF_E05_DIRECTIONS:-h2d,d2h,bidir}"
lanes="${UF_E05_LANES:-1}"
ddr_root="${UF_DDR_ROOT:-/dev/shm/${run_id}_ddr}"

mkdir -p "${run_dir}" "${ddr_root}"

export UF_DDR_ROOT="${ddr_root}"
export UF_DDR_MADVISE_HUGEPAGE="${UF_DDR_MADVISE_HUGEPAGE:-1}"
export UF_DDR_PRETOUCH_ON_CREATE="${UF_DDR_PRETOUCH_ON_CREATE:-1}"
export UF_PINNED_CHUNK_COUNT="${UF_PINNED_CHUNK_COUNT:-2}"
export UF_H2D_PINNED_CHUNK_BYTES="${UF_H2D_PINNED_CHUNK_BYTES:-16777216}"
export UF_D2H_PINNED_CHUNK_BYTES="${UF_D2H_PINNED_CHUNK_BYTES:-67108864}"

{
  echo "UFLOW_E05_DEFAULT_DIRECT_MATRIX_BEGIN"
  echo "run_dir=${run_dir}"
  echo "device=${device}"
  echo "sizes=${sizes}"
  echo "directions=${directions}"
  echo "lanes=${lanes}"
  echo "ddr_root=${UF_DDR_ROOT}"
  echo "ddr_madvise_hugepage=${UF_DDR_MADVISE_HUGEPAGE}"
  echo "ddr_pretouch_on_create=${UF_DDR_PRETOUCH_ON_CREATE}"
} | tee "${run_dir}/environment.txt"

run_case() {
  local mode="$1"
  local label="$2"
  local out="${run_dir}/${label}"
  mkdir -p "${out}"
  UF_E05_RUN_DIR="${out}" \
  UF_E05_DEVICE="${device}" \
  UF_E05_SIZES="${sizes}" \
  UF_E05_DIRECTIONS="${directions}" \
  UF_E05_LANES="${lanes}" \
  UF_E05_MODE="${mode}" \
  UF_DDR_ROOT="${UF_DDR_ROOT}/${label}" \
  examples/phasee05/run_hotpath_saturation_remote.sh \
    > "${out}/runner.stdout" 2> "${out}/runner.stderr"
}

run_case "auto" "auto_direct"
run_case "pinned_async" "pinned_fallback"

python3 - "${run_dir}" <<'PY' > "${run_dir}/daemon_matrix_summary.csv"
import re
import sys
from pathlib import Path

run_dir = Path(sys.argv[1])
print("mode,label,directions,bytes_per_transfer,transfers,actual_engines,actual_paths,daemon_bandwidth_gib_s,channel_bandwidth_gib_s,client_effective_bandwidth_gib_s,daemon_wall_ms,channel_wall_ms,line")
for label in ["auto_direct", "pinned_fallback"]:
    stdout = run_dir / label / "hotpath.stdout"
    if not stdout.exists():
        # run_hotpath_saturation_remote writes inside nested run dir when UF_E05_RUN_DIR points at label dir.
        stdout = run_dir / label / "runner.stdout"
    for line in stdout.read_text(errors="ignore").splitlines():
        if "UFLOW_E05_HOTPATH_RESULT" not in line:
            continue
        fields = dict(re.findall(r"([a-zA-Z0-9_]+)=([^ ]+)", line))
        print(",".join([
            "auto" if label == "auto_direct" else "pinned_async",
            fields.get("label", ""),
            fields.get("directions", ""),
            fields.get("bytes_per_transfer", ""),
            fields.get("transfers", ""),
            fields.get("actual_engines", ""),
            fields.get("actual_paths", ""),
            fields.get("daemon_bandwidth_gib_s", ""),
            fields.get("channel_bandwidth_gib_s", ""),
            fields.get("client_effective_bandwidth_gib_s", ""),
            fields.get("daemon_wall_ms", ""),
            fields.get("channel_wall_ms", ""),
            line.replace(",", ";"),
        ]))
PY

echo "${run_dir}"
tail -n 60 "${run_dir}/daemon_matrix_summary.csv"
