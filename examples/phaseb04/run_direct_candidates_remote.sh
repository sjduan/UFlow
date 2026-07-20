#!/usr/bin/env bash
set -euo pipefail

source /home/sj/git/all_env.sh 2>/dev/null || true
cd /home/sj/git/data-service

device="${UF_TARGET_DEVICE:-${UF_B04_DEVICE:-7}}"
run_id="phaseb04_ssd_hbm_direct_$(date +%Y%m%d_%H%M%S)"
run_dir="${UF_B04_RUN_DIR:-/tmp/proj_output/${run_id}}"
sizes="${UF_B04_SIZES:-4MiB,16MiB,32MiB,64MiB,128MiB,256MiB,512MiB,1024MiB,2048MiB}"
candidates="${UF_B04_CANDIDATES:-file_mmap_acl_direct,file_mmap_thp_pretouch_acl_direct,file_mmap_hostregister_v1,file_mmap_hostregister_v2,file_mmap_thp_pretouch_hostregister_v1,file_mmap_thp_pretouch_hostregister_v2}"
include_relay="${UF_B04_INCLUDE_RELAY_BASELINE:-1}"
mode="${UF_B04_MODE:-ssd_hbm_direct}"

mkdir -p "${run_dir}"

export LD_LIBRARY_PATH="/home/sj/git/data-service/build/lib:${LD_LIBRARY_PATH:-}"
export PYTHONPATH="/home/sj/git/data-service/sdk/python:${PYTHONPATH:-}"
export UF_ACL_LIB="${UF_ACL_LIB:-/home/sj/git/data-service/build/lib/libuf_acl_shim.so}"
export UF_HBM_STARTUP_PROBE_BYTES="${UF_HBM_STARTUP_PROBE_BYTES:-0}"
export UF_B04_TEST_CHUNK_BYTES="${UF_B04_TEST_CHUNK_BYTES:-16777216}"

daemon_bin="${UF_DAEMON_BIN:-./target/release/uf-daemon}"

{
  echo "UFLOW_B04_DIRECT_CANDIDATES_BEGIN"
  echo "run_dir=${run_dir}"
  echo "device=${device}"
  echo "sizes=${sizes}"
  echo "candidates=${candidates}"
  echo "chunk_bytes=${UF_B04_TEST_CHUNK_BYTES}"
  echo "include_relay_baseline=${include_relay}"
  echo "mode=${mode}"
} | tee "${run_dir}/environment.txt"

npu-smi info > "${run_dir}/npu_smi_before.txt" 2>&1 || true

summary_csv="${run_dir}/candidate_matrix.csv"
echo "candidate,exit_code,output_dir,stdout,stderr,daemon_log" > "${summary_csv}"

IFS=',' read -r -a candidate_array <<< "${candidates}"
for candidate in "${candidate_array[@]}"; do
  candidate="$(echo "${candidate}" | xargs)"
  [[ -z "${candidate}" ]] && continue
  label="${candidate//[^a-zA-Z0-9_]/_}"
  out="${run_dir}/${label}"
  sock="/tmp/${run_id}_${label}.sock"
  ssd_root="${UF_SSD_ROOT:-/data/uflow_ssd_${run_id}_${label}}"
  mkdir -p "${out}" "${ssd_root}"
  rm -f "${sock}"

  export UF_SSD_ROOT="${ssd_root}"
  export UF_SSD_DIRECT_ENABLE=1
  export UF_SSD_DIRECT_AUTO=1
  if [[ "${mode}" == "auto" && "${candidate}" == "auto_policy" ]]; then
    unset UF_SSD_HBM_DIRECT_CANDIDATE
  else
    export UF_SSD_HBM_DIRECT_CANDIDATE="${candidate}"
  fi

  "${daemon_bin}" \
    --device "${device}" \
    --socket "${sock}" \
    --startup-probe-bytes "${UF_HBM_STARTUP_PROBE_BYTES}" \
    > "${out}/daemon.log" 2>&1 &
  daemon_pid=$!
  echo "${daemon_pid}" > "${out}/daemon.pid"

  cleanup_one() {
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
    rm -rf "${ssd_root}" || true
  }

  for _ in $(seq 1 50); do
    test -S "${sock}" && break
    sleep 0.2
  done

  set +e
  relay_args=()
  if [[ "${include_relay}" == "1" || "${include_relay}" == "true" || "${include_relay}" == "yes" ]]; then
    relay_args+=(--include-relay-baseline)
  fi
  python3 -u examples/phaseb04/uflow_b04_ssd_hbm_direct_candidates.py \
    --socket "${sock}" \
    --device "${device}" \
    --sizes "${sizes}" \
    --mode "${mode}" \
    "${relay_args[@]}" \
    --output-dir "${out}" \
    > "${out}/benchmark.stdout" 2> "${out}/benchmark.stderr"
  code=$?
  set -e
  echo "${code}" > "${out}/exit_code"
  cleanup_one
  echo "${candidate},${code},${out},${out}/benchmark.stdout,${out}/benchmark.stderr,${out}/daemon.log" >> "${summary_csv}"
done

npu-smi info > "${run_dir}/npu_smi_after.txt" 2>&1 || true

python3 examples/phaseb04/uflow_b04_analyze_candidate_matrix.py "${run_dir}" \
  > "${run_dir}/analysis.stdout" 2> "${run_dir}/analysis.stderr" || true

echo "${run_dir}"
tail -n 80 "${summary_csv}" || true
tail -n 80 "${run_dir}/analysis_summary.md" 2>/dev/null || true
exit 0
