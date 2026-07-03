#!/usr/bin/env bash
set -euo pipefail

DEVICE="${UF_TARGET_DEVICE:-7}"
OBJECT_SIZES="${UF_E03_CHUNK_OBJECT_SIZES:-64m,128m,256m,512m,1g}"
CHUNK_SIZES="${UF_E03_CHUNK_SIZES:-8m,16m,32m,64m,128m}"
CHUNK_COUNTS="${UF_E03_CHUNK_COUNTS:-1,2,3}"
MODES="${UF_E03_CHUNK_MODES:-full_size,chunked}"
OPS="${UF_E03_CHUNK_OPS:-h2d,d2h}"
WARMUPS="${UF_E03_CHUNK_WARMUPS:-1}"
REPEATS="${UF_E03_CHUNK_REPEATS:-3}"
RUN_DIR="${UF_E03_CHUNK_RUN_DIR:-/tmp/proj_output/phasee03_chunk_pipeline_$(date +%Y%m%d_%H%M%S)}"

source /home/sj/git/all_env.sh >/dev/null 2>&1 || true
export LD_LIBRARY_PATH="/home/sj/git/data-service/build/lib:${LD_LIBRARY_PATH:-}"
export UF_ACL_LIB="${UF_ACL_LIB:-/home/sj/git/data-service/build/lib/libuf_acl_shim.so}"

mkdir -p "${RUN_DIR}"
cd /home/sj/git/data-service

{
  echo "UFLOW_E03_CHUNK_PIPELINE_BEGIN"
  echo "run_dir=${RUN_DIR}"
  echo "device=${DEVICE}"
  echo "object_sizes=${OBJECT_SIZES}"
  echo "chunk_sizes=${CHUNK_SIZES}"
  echo "chunk_counts=${CHUNK_COUNTS}"
  echo "modes=${MODES}"
  echo "ops=${OPS}"
  echo "warmups=${WARMUPS}"
  echo "repeats=${REPEATS}"
  echo "uf_acl_lib=${UF_ACL_LIB}"
  python3 -u examples/phasee03/uflow_e03_chunk_pipeline_bench.py \
    --device "${DEVICE}" \
    --object-sizes "${OBJECT_SIZES}" \
    --chunk-sizes "${CHUNK_SIZES}" \
    --chunk-counts "${CHUNK_COUNTS}" \
    --modes "${MODES}" \
    --ops "${OPS}" \
    --warmups "${WARMUPS}" \
    --repeats "${REPEATS}" \
    --run-dir "${RUN_DIR}"
  echo "UFLOW_E03_CHUNK_PIPELINE_DONE run_dir=${RUN_DIR}"
} 2>&1 | tee "${RUN_DIR}/bench.log"
