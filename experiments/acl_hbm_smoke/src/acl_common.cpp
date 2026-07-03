#include "acl_common.hpp"

#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>

namespace uf::phasea {

std::string acl_error_message(aclError err) {
  const char* recent = aclGetRecentErrMsg();
  std::ostringstream oss;
  oss << "acl_error=" << static_cast<int64_t>(err);
  if (recent != nullptr && std::strlen(recent) > 0) {
    oss << " recent=\"" << recent << "\"";
  }
  return oss.str();
}

void check_acl(aclError err, const char* expr, const char* file, int line) {
  if (err == ACL_SUCCESS) {
    return;
  }
  std::ostringstream oss;
  oss << file << ":" << line << " " << expr << " failed: " << acl_error_message(err);
  throw std::runtime_error(oss.str());
}

AclRuntime::AclRuntime(int32_t device_id) : device_id_(device_id) {
  UF_ACL_CHECK(aclInit(nullptr));
  initialized_ = true;
  UF_ACL_CHECK(aclrtSetDevice(device_id_));
  device_set_ = true;
}

AclRuntime::~AclRuntime() {
  if (device_set_) {
    (void)aclrtResetDevice(device_id_);
  }
  if (initialized_) {
    (void)aclFinalize();
  }
}

uint64_t align_up(uint64_t value, uint64_t alignment) {
  if (alignment == 0) {
    return value;
  }
  return ((value + alignment - 1) / alignment) * alignment;
}

uint64_t checksum_words(const std::vector<uint32_t>& data) {
  uint64_t h = 1469598103934665603ull;
  for (uint32_t v : data) {
    h ^= static_cast<uint64_t>(v);
    h *= 1099511628211ull;
  }
  return h;
}

std::vector<uint32_t> make_pattern(size_t count, uint32_t seed) {
  std::vector<uint32_t> out(count);
  for (size_t i = 0; i < count; ++i) {
    out[i] = seed ^ static_cast<uint32_t>(i * 2654435761u) ^ static_cast<uint32_t>(i >> 7);
  }
  return out;
}

std::vector<uint32_t> make_overlay(size_t count, uint32_t seed) {
  std::vector<uint32_t> out(count);
  for (size_t i = 0; i < count; ++i) {
    out[i] = seed + static_cast<uint32_t>(i * 1315423911u) + 0x5a5a0000u;
  }
  return out;
}

CompareResult compare_words(const std::vector<uint32_t>& expected,
                            const std::vector<uint32_t>& actual) {
  CompareResult r;
  if (expected.size() != actual.size()) {
    r.first_mismatch = expected.size() < actual.size() ? expected.size() : actual.size();
    return r;
  }
  for (size_t i = 0; i < expected.size(); ++i) {
    if (expected[i] != actual[i]) {
      r.first_mismatch = i;
      r.expected = expected[i];
      r.actual = actual[i];
      return r;
    }
  }
  r.ok = true;
  return r;
}

MemInfo get_hbm_mem_info() {
  size_t free_bytes = 0;
  size_t total_bytes = 0;
  UF_ACL_CHECK(aclrtGetMemInfo(ACL_HBM_MEM, &free_bytes, &total_bytes));
  return MemInfo{static_cast<uint64_t>(free_bytes), static_cast<uint64_t>(total_bytes)};
}

int32_t get_bare_tgid() {
  int32_t pid = -1;
  UF_ACL_CHECK(aclrtDeviceGetBareTgid(&pid));
  return pid;
}

aclrtPhysicalMemProp make_physical_prop(int32_t device_id) {
  aclrtPhysicalMemProp prop;
  std::memset(&prop, 0, sizeof(prop));
  prop.handleType = ACL_MEM_HANDLE_TYPE_NONE;
  prop.allocationType = ACL_MEM_ALLOCATION_TYPE_PINNED;
  prop.memAttr = ACL_HBM_MEM_HUGE;
  prop.location.id = device_id;
  prop.location.type = ACL_MEM_LOCATION_TYPE_DEVICE;
  return prop;
}

uint64_t allocation_granularity(int32_t device_id) {
  auto prop = make_physical_prop(device_id);
  size_t granularity = 0;
  UF_ACL_CHECK(aclrtMemGetAllocationGranularity(
      &prop, static_cast<aclrtMemGranularityOptions>(0), &granularity));
  return static_cast<uint64_t>(granularity);
}

PhysicalAllocation allocate_physical_mapped(int32_t device_id, uint64_t requested_bytes) {
  PhysicalAllocation alloc;
  alloc.device_id = device_id;
  alloc.requested_bytes = requested_bytes;
  alloc.granularity = allocation_granularity(device_id);
  alloc.actual_bytes = align_up(requested_bytes, alloc.granularity);
  auto prop = make_physical_prop(device_id);
  UF_ACL_CHECK(aclrtMallocPhysical(&alloc.handle, static_cast<size_t>(alloc.actual_bytes), &prop, 0));
  UF_ACL_CHECK(aclrtReserveMemAddress(&alloc.ptr, static_cast<size_t>(alloc.actual_bytes), 0, nullptr, 0));
  UF_ACL_CHECK(aclrtMapMem(alloc.ptr, static_cast<size_t>(alloc.actual_bytes), 0, alloc.handle, 0));
  alloc.mapped = true;
  return alloc;
}

PhysicalAllocation import_physical_mapped(int32_t device_id,
                                          uint64_t shareable_handle,
                                          uint64_t actual_bytes) {
  PhysicalAllocation alloc;
  alloc.device_id = device_id;
  alloc.requested_bytes = actual_bytes;
  alloc.actual_bytes = actual_bytes;
  alloc.shareable_handle = shareable_handle;
  alloc.imported = true;
  UF_ACL_CHECK(aclrtMemImportFromShareableHandle(shareable_handle, device_id, &alloc.handle));
  UF_ACL_CHECK(aclrtReserveMemAddress(&alloc.ptr, static_cast<size_t>(actual_bytes), 0, nullptr, 0));
  UF_ACL_CHECK(aclrtMapMem(alloc.ptr, static_cast<size_t>(actual_bytes), 0, alloc.handle, 0));
  alloc.mapped = true;
  return alloc;
}

void export_shareable(PhysicalAllocation& alloc) {
  UF_ACL_CHECK(aclrtMemExportToShareableHandle(
      alloc.handle, ACL_MEM_HANDLE_TYPE_NONE, 0, &alloc.shareable_handle));
}

void set_shareable_pid(uint64_t shareable_handle, int32_t bare_tgid) {
  int32_t pid = bare_tgid;
  UF_ACL_CHECK(aclrtMemSetPidToShareableHandle(shareable_handle, &pid, 1));
}

void cleanup_physical(PhysicalAllocation& alloc) {
  if (alloc.mapped && alloc.ptr != nullptr) {
    (void)aclrtUnmapMem(alloc.ptr);
    alloc.mapped = false;
  }
  if (alloc.ptr != nullptr) {
    (void)aclrtReleaseMemAddress(alloc.ptr);
    alloc.ptr = nullptr;
  }
  if (alloc.handle != nullptr) {
    (void)aclrtFreePhysical(alloc.handle);
    alloc.handle = nullptr;
  }
}

void copy_h2d(void* dst_device, const void* src_host, uint64_t bytes) {
  UF_ACL_CHECK(aclrtMemcpy(dst_device,
                           static_cast<size_t>(bytes),
                           src_host,
                           static_cast<size_t>(bytes),
                           ACL_MEMCPY_HOST_TO_DEVICE));
}

void copy_d2h(void* dst_host, const void* src_device, uint64_t bytes) {
  UF_ACL_CHECK(aclrtMemcpy(dst_host,
                           static_cast<size_t>(bytes),
                           src_device,
                           static_cast<size_t>(bytes),
                           ACL_MEMCPY_DEVICE_TO_HOST));
}

void print_json_summary(const std::string& test,
                        const std::string& role,
                        int32_t device_id,
                        uint64_t requested_bytes,
                        uint64_t actual_bytes,
                        uint64_t checksum_a,
                        uint64_t checksum_b,
                        const std::string& result,
                        const std::string& detail) {
  std::cout << "{"
            << "\"test\":\"" << test << "\","
            << "\"role\":\"" << role << "\","
            << "\"device_id\":" << device_id << ","
            << "\"requested_bytes\":" << requested_bytes << ","
            << "\"actual_bytes\":" << actual_bytes << ","
            << "\"checksum_a\":\"0x" << std::hex << checksum_a << "\","
            << "\"checksum_b\":\"0x" << checksum_b << std::dec << "\","
            << "\"result\":\"" << result << "\"";
  if (!detail.empty()) {
    std::cout << ",\"detail\":\"" << detail << "\"";
  }
  std::cout << "}" << std::endl;
}

}  // namespace uf::phasea

