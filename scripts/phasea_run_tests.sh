#!/usr/bin/env bash
set -euo pipefail

mode="${1:-all}"
shift || true

preferred_device="${UF_TARGET_DEVICE:-7}"
run_id="${RUN_ID:-phasea_$(date +%Y%m%d_%H%M%S)}"
run_dir="${RUN_DIR:-/tmp/proj_output/${run_id}}"
bytes="${TEST_BYTES:-1048576}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --preferred-device)
      preferred_device="$2"
      shift 2
      ;;
    --run-id)
      run_id="$2"
      run_dir="/tmp/proj_output/${run_id}"
      shift 2
      ;;
    --bytes)
      bytes="$2"
      shift 2
      ;;
    *)
      echo "unknown arg: $1" >&2
      exit 2
      ;;
  esac
done

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "${run_dir}"

export ASCEND_HOME_PATH="${ASCEND_HOME_PATH:-/usr/local/Ascend/cann-8.5.1}"
ORIGINAL_LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}"
export LD_LIBRARY_PATH="${repo_root}/build/lib:${ORIGINAL_LD_LIBRARY_PATH}:${ASCEND_HOME_PATH}/aarch64-linux/lib64:${ASCEND_HOME_PATH}/aarch64-linux/devlib"

npu_smi_info() {
  if [[ -n "${ORIGINAL_LD_LIBRARY_PATH}" ]]; then
    LD_LIBRARY_PATH="${ORIGINAL_LD_LIBRARY_PATH}" npu-smi info
  else
    env -u LD_LIBRARY_PATH npu-smi info
  fi
}

log_env() {
  {
    echo "run_id=${run_id}"
    echo "run_dir=${run_dir}"
    echo "repo_root=${repo_root}"
    echo "ASCEND_HOME_PATH=${ASCEND_HOME_PATH}"
    echo "LD_LIBRARY_PATH=${LD_LIBRARY_PATH}"
    gcc --version | head -1 || true
    g++ --version | head -1 || true
    cmake --version | head -1 || true
    cargo --version || true
    rustc --version || true
    echo "npu-smi-before"
    npu_smi_info || true
  } > "${run_dir}/env.log" 2>&1
}

choose_device() {
  local info="${run_dir}/npu_select.log"
  npu_smi_info > "${info}" 2>&1 || true
  if grep -q "No running processes found in NPU ${preferred_device}" "${info}"; then
    echo "${preferred_device}"
    return 0
  fi
  echo "preferred device ${preferred_device} appears occupied; selecting another idle device" >> "${info}"
  for dev in $(seq 0 15); do
    if grep -q "No running processes found in NPU ${dev}" "${info}"; then
      echo "${dev}"
      return 0
    fi
  done
  echo "no idle NPU found" >&2
  return 1
}

build_all() {
  cd "${repo_root}"
  cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo > "${run_dir}/cmake_configure.log" 2>&1
  cmake --build build -j "$(nproc)" > "${run_dir}/cmake_build.log" 2>&1
  UF_ACL_SHIM_LIB_DIR="${repo_root}/build/lib" cargo build --workspace > "${run_dir}/cargo_build.log" 2>&1
}

run_phase02() {
  local dev="$1"
  local bin="${repo_root}/build/bin"
  echo "${dev}" > "${run_dir}/device_id"
  "${bin}/acl_hbm_copy_smoke" --mode normal --device "${dev}" --bytes "${bytes}" \
    > "${run_dir}/phase02_normal.log" 2>&1
  "${bin}/acl_hbm_copy_smoke" --mode physical --device "${dev}" --bytes "${bytes}" \
    > "${run_dir}/phase02_physical.log" 2>&1
  "${bin}/acl_hbm_copy_smoke" --mode physical --device "${dev}" --bytes "$((bytes + 4))" \
    > "${run_dir}/phase02_physical_nonaligned.log" 2>&1

  local socket="/tmp/uf_acl_hbm_share_${run_id}.sock"
  rm -f "${socket}"
  set +e
  (
    "${bin}/acl_hbm_share_exporter" --device "${dev}" --bytes "${bytes}" --socket "${socket}"
    echo $? > "${run_dir}/phase02_exporter.exit"
  ) > "${run_dir}/phase02_exporter.log" 2>&1 &
  local exporter_pid=$!
  for _ in $(seq 1 200); do
    [[ -S "${socket}" ]] && break
    sleep 0.05
  done
  "${bin}/acl_hbm_share_importer" --device "${dev}" --socket "${socket}" \
    --overlay-offset-elements 1024 --overlay-count-elements 4096 \
    > "${run_dir}/phase02_importer.log" 2>&1
  local importer_rc=$?
  echo "${importer_rc}" > "${run_dir}/phase02_importer.exit"
  wait "${exporter_pid}"
  local exporter_rc
  exporter_rc="$(cat "${run_dir}/phase02_exporter.exit" 2>/dev/null || echo 99)"
  set -e
  if [[ "${importer_rc}" != "0" || "${exporter_rc}" != "0" ]]; then
    echo "phase02 shareable failed exporter=${exporter_rc} importer=${importer_rc}" >&2
    return 1
  fi

  local oob_socket="/tmp/uf_acl_hbm_share_oob_${run_id}.sock"
  rm -f "${oob_socket}"
  set +e
  (
    "${bin}/acl_hbm_share_exporter" --device "${dev}" --bytes 4096 --socket "${oob_socket}"
    echo $? > "${run_dir}/phase02_oob_exporter.exit"
  ) > "${run_dir}/phase02_oob_exporter.log" 2>&1 &
  local oob_exporter_pid=$!
  for _ in $(seq 1 200); do
    [[ -S "${oob_socket}" ]] && break
    sleep 0.05
  done
  "${bin}/acl_hbm_share_importer" --device "${dev}" --socket "${oob_socket}" \
    --overlay-offset-elements 2048 --overlay-count-elements 16 \
    > "${run_dir}/phase02_oob_importer.log" 2>&1
  local oob_importer_rc=$?
  echo "${oob_importer_rc}" > "${run_dir}/phase02_oob_importer.exit"
  wait "${oob_exporter_pid}"
  local oob_exporter_rc
  oob_exporter_rc="$(cat "${run_dir}/phase02_oob_exporter.exit" 2>/dev/null || echo 99)"
  set -e
  if [[ "${oob_importer_rc}" == "0" || "${oob_exporter_rc}" == "0" ]]; then
    echo "phase02 OOB negative unexpectedly succeeded exporter=${oob_exporter_rc} importer=${oob_importer_rc}" >&2
    return 1
  fi
}

run_phase03_pair() {
  local dev="$1"
  local suffix="$2"
  local bin="${repo_root}/build/bin"
  local socket="$3"
  local overlay_extra="${4:-}"
  local object_file="/tmp/uf_phasea03_object_${run_id}_${suffix}"
  rm -f "${object_file}"
  set +e
  (
    "${bin}/phasea03_writer_process" --device "${dev}" --socket "${socket}" --bytes "${bytes}" \
      --object-file "${object_file}"
    echo $? > "${run_dir}/phase03_writer_${suffix}.exit"
  ) > "${run_dir}/phase03_writer_${suffix}.log" 2>&1 &
  local writer_pid=$!
  for _ in $(seq 1 300); do
    [[ -s "${object_file}" ]] && break
    sleep 0.05
  done
  "${bin}/phasea03_overlay_process" --device "${dev}" --socket "${socket}" \
    --object-file "${object_file}" --overlay-offset-elements 1024 --overlay-count-elements 4096 \
    ${overlay_extra} \
    > "${run_dir}/phase03_overlay_${suffix}.log" 2>&1
  local overlay_rc=$?
  echo "${overlay_rc}" > "${run_dir}/phase03_overlay_${suffix}.exit"
  wait "${writer_pid}"
  local writer_rc
  writer_rc="$(cat "${run_dir}/phase03_writer_${suffix}.exit" 2>/dev/null || echo 99)"
  set -e
  if [[ "${writer_rc}" != "0" || "${overlay_rc}" != "0" ]]; then
    echo "phase03 pair ${suffix} failed writer=${writer_rc} overlay=${overlay_rc}" >&2
    return 1
  fi
}

run_phase03() {
  local dev="$1"
  local daemon="${repo_root}/target/debug/uf-daemon"
  local socket="/tmp/uf_phasea03_${run_id}.sock"
  local phase03_rc=0
  rm -f "${socket}"
  set +e
  "${daemon}" --device "${dev}" --socket "${socket}" --block-bytes "${bytes}" --block-count 4 \
    > "${run_dir}/phase03_daemon.log" 2>&1 &
  local daemon_pid=$!
  echo "${daemon_pid}" > "${run_dir}/phase03_daemon.pid"
  set -e
  for _ in $(seq 1 300); do
    [[ -S "${socket}" ]] && break
    if ! kill -0 "${daemon_pid}" 2>/dev/null; then
      echo "daemon exited before socket became ready" >&2
      phase03_rc=1
      break
    fi
    sleep 0.05
  done
  if [[ "${phase03_rc}" == "0" && ! -S "${socket}" ]]; then
    echo "daemon socket not ready" >&2
    phase03_rc=1
  fi
  if [[ "${phase03_rc}" == "0" ]]; then
    run_phase03_pair "${dev}" "first" "${socket}" || phase03_rc=1
  fi
  if [[ "${phase03_rc}" == "0" ]]; then
    run_phase03_pair "${dev}" "reuse" "${socket}" || phase03_rc=1
  fi
  if [[ "${phase03_rc}" == "0" ]]; then
    run_phase03_pair "${dev}" "stale" "${socket}" "--skip-close-lease" || phase03_rc=1
  fi
  if [[ "${phase03_rc}" == "0" ]] && ! grep -q '"event":"lease_stale"' "${run_dir}/phase03_daemon.log"; then
    echo "phase03 stale lease scenario did not record lease_stale" >&2
    phase03_rc=1
  fi
  set +e
  kill "${daemon_pid}" 2>/dev/null
  sleep 2
  if kill -0 "${daemon_pid}" 2>/dev/null; then
    kill -9 "${daemon_pid}" 2>/dev/null
  fi
  wait "${daemon_pid}" 2>/dev/null
  echo $? > "${run_dir}/phase03_daemon.exit"
  set -e
  return "${phase03_rc}"
}

log_env
device_id="$(choose_device)"
export UF_TARGET_DEVICE="${device_id}"

case "${mode}" in
  build)
    build_all
    ;;
  phase02)
    run_phase02 "${device_id}"
    ;;
  phase03)
    run_phase03 "${device_id}"
    ;;
  all)
    build_all
    run_phase02 "${device_id}"
    run_phase03 "${device_id}"
    ;;
  *)
    echo "unknown mode: ${mode}" >&2
    exit 2
    ;;
esac

{
  echo "npu-smi-after"
  npu_smi_info || true
} > "${run_dir}/npu_after.log" 2>&1

echo "0" > "${run_dir}/exit_code"
echo "${run_dir}"
