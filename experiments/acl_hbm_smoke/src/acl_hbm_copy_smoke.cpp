#include "acl_common.hpp"

#include <acl/acl.h>

#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

using namespace uf::phasea;

namespace {

struct Args {
  std::string mode = "normal";
  int32_t device = 0;
  uint64_t bytes = 1ull << 20;
};

Args parse_args(int argc, char** argv) {
  Args args;
  for (int i = 1; i < argc; ++i) {
    std::string key = argv[i];
    auto next = [&]() -> std::string {
      if (i + 1 >= argc) {
        throw std::runtime_error("missing value for " + key);
      }
      return argv[++i];
    };
    if (key == "--mode") {
      args.mode = next();
    } else if (key == "--device") {
      args.device = static_cast<int32_t>(std::stoi(next()));
    } else if (key == "--bytes") {
      args.bytes = static_cast<uint64_t>(std::stoull(next()));
    } else {
      throw std::runtime_error("unknown arg: " + key);
    }
  }
  if (args.bytes == 0 || (args.bytes % sizeof(uint32_t)) != 0) {
    throw std::runtime_error("--bytes must be a non-zero multiple of 4");
  }
  return args;
}

void run_normal(const Args& args) {
  AclRuntime rt(args.device);
  auto before = get_hbm_mem_info();
  const size_t words = args.bytes / sizeof(uint32_t);
  auto host_a = make_pattern(words, 0x12345678u);
  std::vector<uint32_t> host_b(words, 0);
  void* dev = nullptr;
  UF_ACL_CHECK(aclrtMalloc(&dev, static_cast<size_t>(args.bytes), ACL_MEM_MALLOC_HUGE_FIRST));
  copy_h2d(dev, host_a.data(), args.bytes);
  copy_d2h(host_b.data(), dev, args.bytes);
  UF_ACL_CHECK(aclrtFree(dev));
  auto after = get_hbm_mem_info();
  auto cmp = compare_words(host_a, host_b);
  std::string detail;
  if (!cmp.ok) {
    detail = "first_mismatch=" + std::to_string(cmp.first_mismatch);
  }
  detail += " hbm_free_before=" + std::to_string(before.free_bytes);
  detail += " hbm_free_after=" + std::to_string(after.free_bytes);
  print_json_summary("phasea02_copy_normal",
                     "single",
                     args.device,
                     args.bytes,
                     args.bytes,
                     checksum_words(host_a),
                     checksum_words(host_b),
                     cmp.ok ? "pass" : "fail",
                     detail);
  if (!cmp.ok) {
    throw std::runtime_error("normal copy compare failed");
  }
}

void run_physical(const Args& args) {
  AclRuntime rt(args.device);
  auto before = get_hbm_mem_info();
  const size_t words = args.bytes / sizeof(uint32_t);
  auto host_a = make_pattern(words, 0x87654321u);
  std::vector<uint32_t> host_b(words, 0);
  PhysicalAllocation alloc;
  try {
    alloc = allocate_physical_mapped(args.device, args.bytes);
    copy_h2d(alloc.ptr, host_a.data(), args.bytes);
    copy_d2h(host_b.data(), alloc.ptr, args.bytes);
  } catch (...) {
    cleanup_physical(alloc);
    throw;
  }
  cleanup_physical(alloc);
  auto after = get_hbm_mem_info();
  auto cmp = compare_words(host_a, host_b);
  std::string detail;
  if (!cmp.ok) {
    detail = "first_mismatch=" + std::to_string(cmp.first_mismatch);
  }
  detail += " granularity=" + std::to_string(alloc.granularity);
  detail += " hbm_free_before=" + std::to_string(before.free_bytes);
  detail += " hbm_free_after=" + std::to_string(after.free_bytes);
  print_json_summary("phasea02_copy_physical",
                     "single",
                     args.device,
                     args.bytes,
                     alloc.actual_bytes,
                     checksum_words(host_a),
                     checksum_words(host_b),
                     cmp.ok ? "pass" : "fail",
                     detail);
  if (!cmp.ok) {
    throw std::runtime_error("physical copy compare failed");
  }
}

}  // namespace

int main(int argc, char** argv) {
  try {
    Args args = parse_args(argc, argv);
    if (args.mode == "normal") {
      run_normal(args);
    } else if (args.mode == "physical") {
      run_physical(args);
    } else {
      throw std::runtime_error("unknown --mode: " + args.mode);
    }
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "acl_hbm_copy_smoke error: " << e.what() << std::endl;
    return 1;
  }
}

