#pragma once

#include <acl/acl.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace uf::phasea {

struct MemInfo {
  uint64_t free_bytes = 0;
  uint64_t total_bytes = 0;
};

struct CompareResult {
  bool ok = false;
  size_t first_mismatch = 0;
  uint32_t expected = 0;
  uint32_t actual = 0;
};

std::string acl_error_message(aclError err);
void check_acl(aclError err, const char* expr, const char* file, int line);

#define UF_ACL_CHECK(expr) ::uf::phasea::check_acl((expr), #expr, __FILE__, __LINE__)

class AclRuntime {
 public:
  explicit AclRuntime(int32_t device_id);
  ~AclRuntime();

  AclRuntime(const AclRuntime&) = delete;
  AclRuntime& operator=(const AclRuntime&) = delete;

  int32_t device_id() const { return device_id_; }

 private:
  int32_t device_id_ = -1;
  bool initialized_ = false;
  bool device_set_ = false;
};

uint64_t align_up(uint64_t value, uint64_t alignment);
uint64_t checksum_words(const std::vector<uint32_t>& data);
std::vector<uint32_t> make_pattern(size_t count, uint32_t seed);
std::vector<uint32_t> make_overlay(size_t count, uint32_t seed);
CompareResult compare_words(const std::vector<uint32_t>& expected,
                            const std::vector<uint32_t>& actual);
MemInfo get_hbm_mem_info();
int32_t get_bare_tgid();

struct PhysicalAllocation {
  aclrtDrvMemHandle handle = nullptr;
  void* ptr = nullptr;
  uint64_t requested_bytes = 0;
  uint64_t actual_bytes = 0;
  uint64_t granularity = 0;
  uint64_t shareable_handle = 0;
  int32_t device_id = -1;
  bool mapped = false;
  bool imported = false;
};

aclrtPhysicalMemProp make_physical_prop(int32_t device_id);
uint64_t allocation_granularity(int32_t device_id);
PhysicalAllocation allocate_physical_mapped(int32_t device_id, uint64_t requested_bytes);
PhysicalAllocation import_physical_mapped(int32_t device_id,
                                          uint64_t shareable_handle,
                                          uint64_t actual_bytes);
void export_shareable(PhysicalAllocation& alloc);
void set_shareable_pid(uint64_t shareable_handle, int32_t bare_tgid);
void cleanup_physical(PhysicalAllocation& alloc);

void copy_h2d(void* dst_device, const void* src_host, uint64_t bytes);
void copy_d2h(void* dst_host, const void* src_device, uint64_t bytes);

void print_json_summary(const std::string& test,
                        const std::string& role,
                        int32_t device_id,
                        uint64_t requested_bytes,
                        uint64_t actual_bytes,
                        uint64_t checksum_a,
                        uint64_t checksum_b,
                        const std::string& result,
                        const std::string& detail = "");

}  // namespace uf::phasea

