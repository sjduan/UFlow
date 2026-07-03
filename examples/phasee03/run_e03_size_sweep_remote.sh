#!/usr/bin/env bash
set -euo pipefail

DEVICE="${UF_TARGET_DEVICE:-7}"
SIZES="${UF_E03_SIZES:-100k,256k,512k,1m,2m,4m,8m,16m,32m,64m,128m,256m,512m}"
STRATEGIES="${UF_E03_STRATEGIES:-memfd_shared,memfd_plus_pinned_staging,registered_memfd_shared}"
WARMUPS="${UF_E03_WARMUPS:-1}"
REPEATS="${UF_E03_REPEATS:-10}"
RUN_DIR="${UF_E03_RUN_DIR:-/tmp/proj_output/phasee03_sizesweep_$(date +%Y%m%d_%H%M%S)}"

source /home/sj/git/all_env.sh >/dev/null 2>&1 || true
export LD_LIBRARY_PATH="/home/sj/git/data-service/build/lib:${LD_LIBRARY_PATH:-}"
export UF_ACL_LIB="${UF_ACL_LIB:-/home/sj/git/data-service/build/lib/libuf_acl_shim.so}"

mkdir -p "${RUN_DIR}"
cd /home/sj/git/data-service

{
  echo "UFLOW_E03_SIZE_SWEEP_BEGIN"
  echo "run_dir=${RUN_DIR}"
  echo "device=${DEVICE}"
  echo "sizes=${SIZES}"
  echo "strategies=${STRATEGIES}"
  echo "warmups=${WARMUPS}"
  echo "repeats=${REPEATS}"
  echo "uf_acl_lib=${UF_ACL_LIB}"
  python3 -u examples/phasee03/uflow_e03_ddr_strategy_bench.py \
    --device "${DEVICE}" \
    --sizes "${SIZES}" \
    --strategies "${STRATEGIES}" \
    --warmups "${WARMUPS}" \
    --repeats "${REPEATS}" \
    --run-dir "${RUN_DIR}"
  echo "UFLOW_E03_SIZE_SWEEP_DONE run_dir=${RUN_DIR}"
} 2>&1 | tee "${RUN_DIR}/bench.log"
