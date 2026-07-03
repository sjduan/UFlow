cmake_minimum_required(VERSION 3.16)

project(uflow_phasea LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT DEFINED ASCEND_HOME_PATH)
  if(DEFINED ENV{ASCEND_HOME_PATH})
    set(ASCEND_HOME_PATH "$ENV{ASCEND_HOME_PATH}")
  else()
    set(ASCEND_HOME_PATH "/usr/local/Ascend/cann-8.5.1")
  endif()
endif()

set(ASCEND_INCLUDE_DIR "${ASCEND_HOME_PATH}/aarch64-linux/include")
set(ASCEND_RUNTIME_LIB_DIR "${ASCEND_HOME_PATH}/lib64")
set(ASCEND_LIB_DIR "${ASCEND_HOME_PATH}/aarch64-linux/lib64")
set(ASCEND_DEVLIB_DIR "${ASCEND_HOME_PATH}/aarch64-linux/devlib")
set(ASCEND_DRIVER_LIB_DIR "/usr/local/Ascend/driver/lib64/driver")
set(ASCEND_DRIVER_COMMON_LIB_DIR "/usr/local/Ascend/driver/lib64/common")
set(UFLOW_RUNTIME_RPATH
  "${ASCEND_RUNTIME_LIB_DIR};${ASCEND_DRIVER_LIB_DIR};${ASCEND_DRIVER_COMMON_LIB_DIR};${ASCEND_LIB_DIR}"
)
set(CMAKE_BUILD_RPATH "${UFLOW_RUNTIME_RPATH}")

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")

include_directories(
  "${ASCEND_INCLUDE_DIR}"
  "${CMAKE_CURRENT_SOURCE_DIR}/experiments/acl_hbm_smoke/include"
  "${CMAKE_CURRENT_SOURCE_DIR}/native/acl_shim/include"
)

link_directories(
  "${ASCEND_RUNTIME_LIB_DIR}"
  "${ASCEND_LIB_DIR}"
  "${ASCEND_DRIVER_LIB_DIR}"
  "${ASCEND_DRIVER_COMMON_LIB_DIR}"
  "${ASCEND_DEVLIB_DIR}"
)

add_library(uf_phasea_common STATIC
  experiments/acl_hbm_smoke/src/acl_common.cpp
  experiments/acl_hbm_smoke/src/ipc.cpp
)
target_link_libraries(uf_phasea_common PUBLIC ascendcl pthread)

add_library(uf_acl_shim SHARED
  native/acl_shim/src/uf_acl.cpp
)
target_link_libraries(uf_acl_shim PUBLIC ascendcl pthread)
set_target_properties(uf_acl_shim PROPERTIES
  BUILD_RPATH "${UFLOW_RUNTIME_RPATH}"
  INSTALL_RPATH "${UFLOW_RUNTIME_RPATH}"
)

add_executable(acl_hbm_copy_smoke
  experiments/acl_hbm_smoke/src/acl_hbm_copy_smoke.cpp
)
target_link_libraries(acl_hbm_copy_smoke PRIVATE uf_phasea_common)

add_executable(acl_memcpy_bandwidth
  experiments/acl_hbm_smoke/src/acl_memcpy_bandwidth.cpp
)
target_link_libraries(acl_memcpy_bandwidth PRIVATE uf_phasea_common)

add_executable(acl_h2d_stage_breakdown
  experiments/acl_hbm_smoke/src/acl_h2d_stage_breakdown.cpp
)
target_link_libraries(acl_h2d_stage_breakdown PRIVATE uf_phasea_common)

add_executable(acl_h2d_memfd_fastpath_matrix
  experiments/acl_hbm_smoke/src/acl_h2d_memfd_fastpath_matrix.cpp
)
target_link_libraries(acl_h2d_memfd_fastpath_matrix PRIVATE uf_phasea_common)

add_executable(acl_hbm_share_exporter
  experiments/acl_hbm_smoke/src/acl_hbm_share_exporter.cpp
)
target_link_libraries(acl_hbm_share_exporter PRIVATE uf_phasea_common)

add_executable(acl_hbm_share_importer
  experiments/acl_hbm_smoke/src/acl_hbm_share_importer.cpp
)
target_link_libraries(acl_hbm_share_importer PRIVATE uf_phasea_common)

add_executable(phasea03_writer_process
  examples/phasea03/writer_process.cpp
)
target_link_libraries(phasea03_writer_process PRIVATE uf_phasea_common uf_acl_shim)
set_target_properties(phasea03_writer_process PROPERTIES
  BUILD_RPATH "${CMAKE_BINARY_DIR}/lib;${UFLOW_RUNTIME_RPATH}"
)

add_executable(phasea03_overlay_process
  examples/phasea03/overlay_process.cpp
)
target_link_libraries(phasea03_overlay_process PRIVATE uf_phasea_common uf_acl_shim)
set_target_properties(phasea03_overlay_process PROPERTIES
  BUILD_RPATH "${CMAKE_BINARY_DIR}/lib;${UFLOW_RUNTIME_RPATH}"
)
