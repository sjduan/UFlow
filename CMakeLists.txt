cmake_minimum_required(VERSION 3.16)
project(uflow LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(ASCEND_HOME_PATH "$ENV{ASCEND_HOME_PATH}" CACHE PATH "A5 CANN toolkit root")
if(NOT ASCEND_HOME_PATH)
  set(ASCEND_HOME_PATH "$ENV{CANN_ROOT}")
endif()
if(NOT IS_DIRECTORY "${ASCEND_HOME_PATH}")
  message(FATAL_ERROR "Set ASCEND_HOME_PATH or CANN_ROOT to the installed A5 CANN toolkit.")
endif()

set(CANN_ARCH_DIR "${ASCEND_HOME_PATH}/${CMAKE_SYSTEM_PROCESSOR}-linux")
find_path(CANN_ACL_INCLUDE acl/acl.h
  PATHS "${ASCEND_HOME_PATH}/include" "${CANN_ARCH_DIR}/include"
  NO_DEFAULT_PATH)
find_library(CANN_ASCENDCL ascendcl
  PATHS "${ASCEND_HOME_PATH}/lib64" "${CANN_ARCH_DIR}/lib64"
  NO_DEFAULT_PATH)
if(NOT CANN_ACL_INCLUDE OR NOT CANN_ASCENDCL)
  message(FATAL_ERROR "ACL headers and the ascendcl runtime library are required under ASCEND_HOME_PATH.")
endif()
find_package(Threads REQUIRED)
get_filename_component(CANN_ACL_LIB_DIR "${CANN_ASCENDCL}" DIRECTORY)

add_library(uf_acl_shim SHARED native/acl_shim/src/uf_acl.cpp)
target_include_directories(uf_acl_shim
  PUBLIC native/acl_shim/include
  PRIVATE "${CANN_ACL_INCLUDE}")
target_link_libraries(uf_acl_shim PRIVATE "${CANN_ASCENDCL}" Threads::Threads)
set_target_properties(uf_acl_shim PROPERTIES
  LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
  BUILD_RPATH "${CANN_ACL_LIB_DIR}"
  INSTALL_RPATH "${CANN_ACL_LIB_DIR}"
)
