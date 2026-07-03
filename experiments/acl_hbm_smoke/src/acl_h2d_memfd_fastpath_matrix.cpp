#include "acl_common.hpp"

#include <acl/acl.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <utility>
#include <vector>

using namespace uf::phasea;

namespace {

constexpr uint64_t kKiB = 1024ull;
constexpr uint64_t kMiB = 1024ull * 1024ull;
constexpr uint64_t kGiB = 1024ull * 1024ull * 1024ull;
constexpr uint32_t kHostRegMapped = 0x2u;
constexpr uint32_t kHostRegPinned = 0x10000000u;

#ifndef MFD_HUGETLB
#define MFD_HUGETLB 0x0004U
#endif

#ifndef MFD_HUGE_SHIFT
#define MFD_HUGE_SHIFT 26
#endif

#ifndef MFD_HUGE_2MB
#define MFD_HUGE_2MB (21U << MFD_HUGE_SHIFT)
#endif

#ifndef MADV_HUGEPAGE
#define MADV_HUGEPAGE 14
#endif

struct Args {
  int32_t device = 7;
  uint64_t bytes = 2ull * kGiB;
  uint64_t chunk_bytes = 16ull * kMiB;
  int chunk_count = 2;
  int warmups = 1;
  int repeats = 3;
  std::string output_dir = "/tmp/proj_output/phasee03_h2d_memfd_fastpath";
  std::string direction = "h2d";
  std::string hbm_kind = "normal";
  std::string only_selection;
  bool verify = true;
};

struct SkipStrategy : public std::runtime_error {
  explicit SkipStrategy(const std::string& msg) : std::runtime_error(msg) {}
};

struct StageEvent {
  std::string selection;
  std::string stage;
  uint64_t bytes = 0;
  double ts_us = 0.0;
  double dur_us = 0.0;
  int32_t device = 0;
  int32_t iteration = -1;
  std::string phase;
  std::string detail;
};

struct IterTiming {
  int32_t iteration = -1;
  std::string phase;
  double wall_us = 0.0;
  double submit_us = 0.0;
  double wait_us = 0.0;
  double cpu_copy_us = 0.0;
  bool verified = false;
};

struct SmapsInfo {
  uint64_t anon_huge_kb = 0;
  uint64_t shmem_pmd_mapped_kb = 0;
  uint64_t file_pmd_mapped_kb = 0;
  uint64_t kernel_page_kb = 0;
  uint64_t mmu_page_kb = 0;
};

struct SelectionSummary {
  std::string selection;
  std::string status = "ok";
  std::string error;
  uint64_t bytes = 0;
  double setup_us = 0.0;
  double hbm_alloc_us = 0.0;
  double memfd_mmap_us = 0.0;
  double acl_malloc_host_us = 0.0;
  double pinned_pool_alloc_us = 0.0;
  double pretouch_us = 0.0;
  double mlock_us = 0.0;
  double madvise_us = 0.0;
  double register_us = 0.0;
  double unregister_us = 0.0;
  double warmup_us = 0.0;
  double hot_avg_us = 0.0;
  double hot_min_us = 0.0;
  double hot_max_us = 0.0;
  double hot_avg_gib_s = 0.0;
  double hot_min_gib_s = 0.0;
  double hot_max_gib_s = 0.0;
  double hot_submit_us = 0.0;
  double hot_wait_us = 0.0;
  double hot_cpu_copy_us = 0.0;
  uint32_t v2_flags = 0;
  bool used_v2 = false;
  bool used_mlock = false;
  bool used_thp = false;
  bool used_hugetlb = false;
  bool used_pinned_staging = false;
  std::string mmap_kind;
  SmapsInfo smaps;
  int warmups = 0;
  int repeats = 0;
  bool verified = false;
  std::vector<IterTiming> iterations;
};

using Clock = std::chrono::steady_clock;

Clock::time_point g_t0;
std::vector<StageEvent> g_events;

uint64_t parse_size(const std::string& text) {
  size_t pos = 0;
  double value = std::stod(text, &pos);
  const std::string suffix = text.substr(pos);
  uint64_t scale = 1;
  if (suffix == "K" || suffix == "KB" || suffix == "KiB") {
    scale = 1ull << 10;
  } else if (suffix == "M" || suffix == "MB" || suffix == "MiB") {
    scale = 1ull << 20;
  } else if (suffix == "G" || suffix == "GB" || suffix == "GiB") {
    scale = 1ull << 30;
  } else if (!suffix.empty()) {
    throw std::runtime_error("unknown size suffix: " + suffix);
  }
  return static_cast<uint64_t>(value * static_cast<double>(scale));
}

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
    if (key == "--device") {
      args.device = std::stoi(next());
    } else if (key == "--bytes") {
      args.bytes = parse_size(next());
    } else if (key == "--chunk-bytes") {
      args.chunk_bytes = parse_size(next());
    } else if (key == "--chunk-count") {
      args.chunk_count = std::stoi(next());
    } else if (key == "--warmups") {
      args.warmups = std::stoi(next());
    } else if (key == "--repeats") {
      args.repeats = std::stoi(next());
    } else if (key == "--output-dir") {
      args.output_dir = next();
    } else if (key == "--direction") {
      args.direction = next();
    } else if (key == "--hbm-kind") {
      args.hbm_kind = next();
    } else if (key == "--only-selection") {
      args.only_selection = next();
    } else if (key == "--no-verify") {
      args.verify = false;
    } else {
      throw std::runtime_error("unknown arg: " + key);
    }
  }
  if (args.bytes == 0 || args.chunk_bytes == 0 || args.chunk_count <= 0 || args.warmups < 0 ||
      args.repeats <= 0) {
    throw std::runtime_error("bytes/chunk-bytes/chunk-count/warmups/repeats are invalid");
  }
  if (args.direction != "h2d" && args.direction != "d2h") {
    throw std::runtime_error("--direction must be h2d or d2h");
  }
  if (args.hbm_kind != "normal" && args.hbm_kind != "physical") {
    throw std::runtime_error("--hbm-kind must be normal or physical");
  }
  return args;
}

bool selection_enabled(const Args& args, const std::string& selection) {
  if (args.only_selection.empty()) {
    return true;
  }
  std::stringstream ss(args.only_selection);
  std::string item;
  while (std::getline(ss, item, ',')) {
    item.erase(std::remove_if(item.begin(), item.end(), [](unsigned char ch) {
                 return std::isspace(ch) != 0;
               }),
               item.end());
    if (item == selection) {
      return true;
    }
  }
  return false;
}

double us_since_start(Clock::time_point t) {
  return std::chrono::duration<double, std::micro>(t - g_t0).count();
}

double elapsed_us(Clock::time_point start, Clock::time_point end) {
  return std::chrono::duration<double, std::micro>(end - start).count();
}

double gib_per_s(uint64_t bytes, double us) {
  if (us <= 0.0) {
    return 0.0;
  }
  return (static_cast<double>(bytes) / static_cast<double>(kGiB)) / (us / 1'000'000.0);
}

std::string json_escape(const std::string& value) {
  std::ostringstream out;
  for (char c : value) {
    switch (c) {
      case '\\':
        out << "\\\\";
        break;
      case '"':
        out << "\\\"";
        break;
      case '\n':
        out << "\\n";
        break;
      case '\r':
        out << "\\r";
        break;
      case '\t':
        out << "\\t";
        break;
      default:
        out << c;
        break;
    }
  }
  return out.str();
}

std::string hex_u32(uint32_t value) {
  std::ostringstream out;
  out << "0x" << std::hex << std::nouppercase << value;
  return out.str();
}

void record_stage(const std::string& selection,
                  const std::string& stage,
                  Clock::time_point start,
                  Clock::time_point end,
                  uint64_t bytes,
                  int32_t device,
                  const std::string& phase = "",
                  int32_t iteration = -1,
                  const std::string& detail = "") {
  g_events.push_back(StageEvent{
      selection,
      stage,
      bytes,
      us_since_start(start),
      elapsed_us(start, end),
      device,
      iteration,
      phase,
      detail,
  });
}

void ensure_dir(const std::string& path) {
  std::string cmd = "mkdir -p '" + path + "'";
  if (std::system(cmd.c_str()) != 0) {
    throw std::runtime_error("failed to create output dir: " + path);
  }
}

class DeviceBuffer {
 public:
  DeviceBuffer(int32_t device, uint64_t bytes, std::string kind) : bytes_(bytes), kind_(std::move(kind)) {
    if (kind_ == "physical") {
      physical_ = allocate_physical_mapped(device, bytes_);
      ptr_ = physical_.ptr;
      bytes_ = physical_.actual_bytes;
      return;
    }
    if (kind_ != "normal") {
      throw std::runtime_error("unknown HBM kind: " + kind_);
    }
    UF_ACL_CHECK(aclrtMalloc(&ptr_, static_cast<size_t>(bytes_), ACL_MEM_MALLOC_HUGE_FIRST));
  }

  ~DeviceBuffer() {
    if (kind_ == "physical") {
      cleanup_physical(physical_);
      ptr_ = nullptr;
    } else if (ptr_ != nullptr) {
      (void)aclrtFree(ptr_);
    }
  }

  DeviceBuffer(const DeviceBuffer&) = delete;
  DeviceBuffer& operator=(const DeviceBuffer&) = delete;

  void* ptr() const { return ptr_; }
  uint64_t bytes() const { return bytes_; }

 private:
  void* ptr_ = nullptr;
  uint64_t bytes_ = 0;
  std::string kind_;
  PhysicalAllocation physical_;
};

class StreamEvent {
 public:
  StreamEvent() {
    UF_ACL_CHECK(aclrtCreateStream(&stream_));
    UF_ACL_CHECK(aclrtCreateEvent(&event_));
  }

  ~StreamEvent() {
    if (event_ != nullptr) {
      (void)aclrtDestroyEvent(event_);
    }
    if (stream_ != nullptr) {
      (void)aclrtDestroyStream(stream_);
    }
  }

  StreamEvent(const StreamEvent&) = delete;
  StreamEvent& operator=(const StreamEvent&) = delete;

  aclrtStream stream() const { return stream_; }
  aclrtEvent event() const { return event_; }

 private:
  aclrtStream stream_ = nullptr;
  aclrtEvent event_ = nullptr;
};

class StreamEventPool {
 public:
  explicit StreamEventPool(int count) {
    UF_ACL_CHECK(aclrtCreateStream(&stream_));
    events_.resize(static_cast<size_t>(count), nullptr);
    for (auto& event : events_) {
      UF_ACL_CHECK(aclrtCreateEvent(&event));
    }
  }

  ~StreamEventPool() {
    for (auto event : events_) {
      if (event != nullptr) {
        (void)aclrtDestroyEvent(event);
      }
    }
    if (stream_ != nullptr) {
      (void)aclrtDestroyStream(stream_);
    }
  }

  StreamEventPool(const StreamEventPool&) = delete;
  StreamEventPool& operator=(const StreamEventPool&) = delete;

  aclrtStream stream() const { return stream_; }
  aclrtEvent event(size_t index) const { return events_.at(index); }
  size_t size() const { return events_.size(); }

 private:
  aclrtStream stream_ = nullptr;
  std::vector<aclrtEvent> events_;
};

class MemfdMapping {
 public:
  MemfdMapping(uint64_t bytes, const std::string& name, uint32_t flags, bool hugetlb)
      : bytes_(bytes), hugetlb_(hugetlb) {
#ifdef SYS_memfd_create
    fd_ = static_cast<int>(syscall(SYS_memfd_create, name.c_str(), flags));
#else
    fd_ = -1;
#endif
    if (fd_ < 0) {
      if (hugetlb_) {
        throw SkipStrategy("memfd_create hugetlb failed errno=" + std::to_string(errno));
      }
      throw std::runtime_error("memfd_create failed errno=" + std::to_string(errno));
    }
    if (ftruncate(fd_, static_cast<off_t>(bytes_)) != 0) {
      const int err = errno;
      if (hugetlb_) {
        throw SkipStrategy("ftruncate hugetlb memfd failed errno=" + std::to_string(err));
      }
      throw std::runtime_error("ftruncate memfd failed errno=" + std::to_string(err));
    }
    ptr_ = mmap(nullptr, static_cast<size_t>(bytes_), PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    if (ptr_ == MAP_FAILED) {
      ptr_ = nullptr;
      const int err = errno;
      if (hugetlb_) {
        throw SkipStrategy("mmap hugetlb memfd failed errno=" + std::to_string(err));
      }
      throw std::runtime_error("mmap memfd failed errno=" + std::to_string(err));
    }
  }

  ~MemfdMapping() {
    if (ptr_ != nullptr) {
      (void)munmap(ptr_, static_cast<size_t>(bytes_));
    }
    if (fd_ >= 0) {
      (void)close(fd_);
    }
  }

  MemfdMapping(const MemfdMapping&) = delete;
  MemfdMapping& operator=(const MemfdMapping&) = delete;

  void* ptr() const { return ptr_; }
  uint64_t bytes() const { return bytes_; }
  bool hugetlb() const { return hugetlb_; }

 private:
  int fd_ = -1;
  void* ptr_ = nullptr;
  uint64_t bytes_ = 0;
  bool hugetlb_ = false;
};

class PinnedHostBuffer {
 public:
  explicit PinnedHostBuffer(uint64_t bytes) : bytes_(bytes) {
    UF_ACL_CHECK(aclrtMallocHost(&ptr_, static_cast<size_t>(bytes_)));
  }

  ~PinnedHostBuffer() {
    if (ptr_ != nullptr) {
      (void)aclrtFreeHost(ptr_);
    }
  }

  PinnedHostBuffer(const PinnedHostBuffer&) = delete;
  PinnedHostBuffer& operator=(const PinnedHostBuffer&) = delete;

  void* ptr() const { return ptr_; }
  uint64_t bytes() const { return bytes_; }

 private:
  void* ptr_ = nullptr;
  uint64_t bytes_ = 0;
};

class HostRegistrationV2 {
 public:
  HostRegistrationV2(void* ptr, uint64_t bytes, uint32_t flags, bool get_device_ptr, double* register_us)
      : ptr_(ptr), bytes_(bytes), flags_(flags) {
    auto start = Clock::now();
    UF_ACL_CHECK(aclrtHostRegisterV2(ptr_, bytes_, flags_));
    if (get_device_ptr) {
      const aclError err = aclrtHostGetDevicePointer(ptr_, &device_ptr_, 0);
      if (err != ACL_SUCCESS) {
        (void)aclrtHostUnregister(ptr_);
        ptr_ = nullptr;
        UF_ACL_CHECK(err);
      }
    }
    auto end = Clock::now();
    if (register_us != nullptr) {
      *register_us = elapsed_us(start, end);
    }
  }

  ~HostRegistrationV2() {
    if (ptr_ != nullptr) {
      (void)aclrtHostUnregister(ptr_);
    }
  }

  HostRegistrationV2(const HostRegistrationV2&) = delete;
  HostRegistrationV2& operator=(const HostRegistrationV2&) = delete;

  double unregister_now() {
    if (ptr_ == nullptr) {
      return 0.0;
    }
    auto start = Clock::now();
    UF_ACL_CHECK(aclrtHostUnregister(ptr_));
    auto end = Clock::now();
    ptr_ = nullptr;
    return elapsed_us(start, end);
  }

  void* device_ptr() const { return device_ptr_; }
  uint32_t flags() const { return flags_; }

 private:
  void* ptr_ = nullptr;
  void* device_ptr_ = nullptr;
  uint64_t bytes_ = 0;
  uint32_t flags_ = 0;
};

void pretouch_zero(void* ptr, uint64_t bytes) {
  auto* data = static_cast<volatile uint8_t*>(ptr);
  const long sys_page = sysconf(_SC_PAGESIZE);
  const uint64_t page = sys_page > 0 ? static_cast<uint64_t>(sys_page) : 4096ull;
  for (uint64_t offset = 0; offset < bytes; offset += page) {
    data[offset] = 0;
  }
  if (bytes > 0) {
    data[bytes - 1] = 0;
  }
}

SmapsInfo read_smaps_info(void* ptr) {
  SmapsInfo info;
  std::ifstream in("/proc/self/smaps");
  if (!in) {
    return info;
  }
  const auto target = reinterpret_cast<uintptr_t>(ptr);
  bool in_region = false;
  std::string line;
  while (std::getline(in, line)) {
    unsigned long start = 0;
    unsigned long end = 0;
    if (std::sscanf(line.c_str(), "%lx-%lx", &start, &end) == 2) {
      in_region = target >= static_cast<uintptr_t>(start) && target < static_cast<uintptr_t>(end);
      continue;
    }
    if (!in_region) {
      continue;
    }
    auto parse_kb = [&](const char* key, uint64_t* out) {
      const size_t key_len = std::strlen(key);
      if (line.compare(0, key_len, key) == 0) {
        unsigned long value = 0;
        if (std::sscanf(line.c_str() + key_len, " %lu kB", &value) == 1) {
          *out = static_cast<uint64_t>(value);
        }
      }
    };
    parse_kb("AnonHugePages:", &info.anon_huge_kb);
    parse_kb("ShmemPmdMapped:", &info.shmem_pmd_mapped_kb);
    parse_kb("FilePmdMapped:", &info.file_pmd_mapped_kb);
    parse_kb("KernelPageSize:", &info.kernel_page_kb);
    parse_kb("MMUPageSize:", &info.mmu_page_kb);
  }
  return info;
}

void poison_hbm(void* dev, uint64_t bytes, const std::string& selection, const std::string& phase, int iteration, int32_t device) {
  auto start = Clock::now();
  UF_ACL_CHECK(aclrtMemset(dev, static_cast<size_t>(bytes), 0xa5, static_cast<size_t>(bytes)));
  auto end = Clock::now();
  record_stage(selection, "pre_iter.poison_hbm", start, end, bytes, device, phase, iteration);
}

uint8_t d2h_pattern_for(const std::string& phase, int iteration) {
  const int phase_bias = phase == "warmup" ? 0 : 17;
  return static_cast<uint8_t>(0x30 + ((iteration + phase_bias) % 64));
}

void fill_hbm_pattern(void* dev,
                      uint64_t bytes,
                      uint8_t pattern,
                      const std::string& selection,
                      const std::string& phase,
                      int iteration,
                      int32_t device) {
  auto start = Clock::now();
  UF_ACL_CHECK(aclrtMemset(dev, static_cast<size_t>(bytes), pattern, static_cast<size_t>(bytes)));
  auto end = Clock::now();
  record_stage(selection,
               "pre_iter.fill_hbm_pattern",
               start,
               end,
               bytes,
               device,
               phase,
               iteration,
               "pattern=" + std::to_string(pattern));
}

bool verify_zero_samples(void* dev, uint64_t bytes, int32_t samples) {
  std::vector<uint64_t> offsets;
  offsets.push_back(0);
  offsets.push_back(std::min<uint64_t>(bytes - 1, 4096));
  for (int32_t i = 1; i <= samples - 3; ++i) {
    offsets.push_back((bytes / static_cast<uint64_t>(samples - 2)) * static_cast<uint64_t>(i));
  }
  offsets.push_back(bytes - 1);
  offsets.resize(static_cast<size_t>(samples));
  auto* base = static_cast<uint8_t*>(dev);
  for (auto offset : offsets) {
    uint8_t actual = 0xff;
    UF_ACL_CHECK(aclrtMemcpy(&actual, 1, base + offset, 1, ACL_MEMCPY_DEVICE_TO_HOST));
    if (actual != 0) {
      return false;
    }
  }
  return true;
}

bool verify_host_samples(void* host, uint64_t bytes, int32_t samples, uint8_t expected) {
  std::vector<uint64_t> offsets;
  offsets.push_back(0);
  offsets.push_back(std::min<uint64_t>(bytes - 1, 4096));
  for (int32_t i = 1; i <= samples - 3; ++i) {
    offsets.push_back((bytes / static_cast<uint64_t>(samples - 2)) * static_cast<uint64_t>(i));
  }
  offsets.push_back(bytes - 1);
  offsets.resize(static_cast<size_t>(samples));
  auto* base = static_cast<uint8_t*>(host);
  for (auto offset : offsets) {
    if (base[offset] != expected) {
      return false;
    }
  }
  return true;
}

IterTiming direct_h2d_once(const std::string& selection,
                           void* dev,
                           void* host,
                           uint64_t bytes,
                           StreamEvent& se,
                           const std::string& phase,
                           int iteration,
                           int32_t device,
                           bool verify) {
  IterTiming timing;
  timing.iteration = iteration;
  timing.phase = phase;
  auto wall_start = Clock::now();
  auto submit_start = Clock::now();
  UF_ACL_CHECK(aclrtMemcpyAsync(dev,
                                static_cast<size_t>(bytes),
                                host,
                                static_cast<size_t>(bytes),
                                ACL_MEMCPY_HOST_TO_DEVICE,
                                se.stream()));
  UF_ACL_CHECK(aclrtRecordEvent(se.event(), se.stream()));
  auto submit_end = Clock::now();
  record_stage(selection, "direct.acl_submit.h2d", submit_start, submit_end, bytes, device, phase, iteration);
  auto wait_start = Clock::now();
  UF_ACL_CHECK(aclrtSynchronizeEvent(se.event()));
  auto wait_end = Clock::now();
  record_stage(selection, "direct.acl_wait.h2d", wait_start, wait_end, bytes, device, phase, iteration);
  auto wall_end = Clock::now();
  record_stage(selection, "transfer." + phase, wall_start, wall_end, bytes, device, phase, iteration);
  timing.wall_us = elapsed_us(wall_start, wall_end);
  timing.submit_us = elapsed_us(submit_start, submit_end);
  timing.wait_us = elapsed_us(wait_start, wait_end);
  if (verify) {
    auto verify_start = Clock::now();
    timing.verified = verify_zero_samples(dev, bytes, 32);
    auto verify_end = Clock::now();
    record_stage(selection, "verify.sample_d2h", verify_start, verify_end, 32, device, phase, iteration);
  } else {
    timing.verified = true;
  }
  return timing;
}

IterTiming direct_d2h_once(const std::string& selection,
                           void* dev,
                           void* host,
                           uint64_t bytes,
                           StreamEvent& se,
                           const std::string& phase,
                           int iteration,
                           int32_t device,
                           bool verify,
                           uint8_t expected) {
  IterTiming timing;
  timing.iteration = iteration;
  timing.phase = phase;
  auto wall_start = Clock::now();
  auto submit_start = Clock::now();
  UF_ACL_CHECK(aclrtMemcpyAsync(host,
                                static_cast<size_t>(bytes),
                                dev,
                                static_cast<size_t>(bytes),
                                ACL_MEMCPY_DEVICE_TO_HOST,
                                se.stream()));
  UF_ACL_CHECK(aclrtRecordEvent(se.event(), se.stream()));
  auto submit_end = Clock::now();
  record_stage(selection, "direct.acl_submit.d2h", submit_start, submit_end, bytes, device, phase, iteration);
  auto wait_start = Clock::now();
  UF_ACL_CHECK(aclrtSynchronizeEvent(se.event()));
  auto wait_end = Clock::now();
  record_stage(selection, "direct.acl_wait.d2h", wait_start, wait_end, bytes, device, phase, iteration);
  auto wall_end = Clock::now();
  record_stage(selection, "transfer." + phase, wall_start, wall_end, bytes, device, phase, iteration);
  timing.wall_us = elapsed_us(wall_start, wall_end);
  timing.submit_us = elapsed_us(submit_start, submit_end);
  timing.wait_us = elapsed_us(wait_start, wait_end);
  if (verify) {
    auto verify_start = Clock::now();
    timing.verified = verify_host_samples(host, bytes, 32, expected);
    auto verify_end = Clock::now();
    record_stage(selection, "verify.sample_host", verify_start, verify_end, 32, device, phase, iteration);
  } else {
    timing.verified = true;
  }
  return timing;
}

IterTiming pinned_chunk_h2d_once(const std::string& selection,
                                 void* dev,
                                 void* host,
                                 uint64_t bytes,
                                 const std::vector<std::unique_ptr<PinnedHostBuffer>>& chunks,
                                 StreamEventPool& se,
                                 const std::string& phase,
                                 int iteration,
                                 int32_t device,
                                 bool verify) {
  IterTiming timing;
  timing.iteration = iteration;
  timing.phase = phase;
  auto* src = static_cast<uint8_t*>(host);
  auto* dst = static_cast<uint8_t*>(dev);
  struct Inflight {
    bool active = false;
    uint64_t bytes = 0;
    uint64_t chunk_index = 0;
  };
  std::vector<Inflight> inflight(chunks.size());
  auto wall_start = Clock::now();
  for (uint64_t offset = 0, chunk_index = 0; offset < bytes; offset += chunks[0]->bytes(), ++chunk_index) {
    const size_t slot = static_cast<size_t>(chunk_index % static_cast<uint64_t>(chunks.size()));
    if (inflight[slot].active) {
      auto wait_start = Clock::now();
      UF_ACL_CHECK(aclrtSynchronizeEvent(se.event(slot)));
      auto wait_end = Clock::now();
      timing.wait_us += elapsed_us(wait_start, wait_end);
      record_stage(selection,
                   "chunk.acl_wait.h2d",
                   wait_start,
                   wait_end,
                   inflight[slot].bytes,
                   device,
                   phase,
                   iteration,
                   "chunk_index=" + std::to_string(inflight[slot].chunk_index));
      inflight[slot].active = false;
    }
    const uint64_t n = std::min<uint64_t>(chunks[0]->bytes(), bytes - offset);
    auto copy_start = Clock::now();
    std::memcpy(chunks[slot]->ptr(), src + offset, static_cast<size_t>(n));
    auto copy_end = Clock::now();
    timing.cpu_copy_us += elapsed_us(copy_start, copy_end);
    record_stage(selection, "chunk.cpu_copy.memfd_to_pinned", copy_start, copy_end, n, device, phase, iteration);

    auto submit_start = Clock::now();
    UF_ACL_CHECK(aclrtMemcpyAsync(dst + offset,
                                  static_cast<size_t>(n),
                                  chunks[slot]->ptr(),
                                  static_cast<size_t>(n),
                                  ACL_MEMCPY_HOST_TO_DEVICE,
                                  se.stream()));
    UF_ACL_CHECK(aclrtRecordEvent(se.event(slot), se.stream()));
    auto submit_end = Clock::now();
    timing.submit_us += elapsed_us(submit_start, submit_end);
    record_stage(selection, "chunk.acl_submit.h2d", submit_start, submit_end, n, device, phase, iteration);
    inflight[slot] = Inflight{true, n, chunk_index};
  }
  for (size_t slot = 0; slot < inflight.size(); ++slot) {
    if (!inflight[slot].active) {
      continue;
    }
    auto wait_start = Clock::now();
    UF_ACL_CHECK(aclrtSynchronizeEvent(se.event(slot)));
    auto wait_end = Clock::now();
    timing.wait_us += elapsed_us(wait_start, wait_end);
    record_stage(selection,
                 "chunk.acl_wait.h2d",
                 wait_start,
                 wait_end,
                 inflight[slot].bytes,
                 device,
                 phase,
                 iteration,
                 "chunk_index=" + std::to_string(inflight[slot].chunk_index));
  }
  auto wall_end = Clock::now();
  record_stage(selection, "transfer." + phase, wall_start, wall_end, bytes, device, phase, iteration);
  timing.wall_us = elapsed_us(wall_start, wall_end);
  if (verify) {
    auto verify_start = Clock::now();
    timing.verified = verify_zero_samples(dev, bytes, 32);
    auto verify_end = Clock::now();
    record_stage(selection, "verify.sample_d2h", verify_start, verify_end, 32, device, phase, iteration);
  } else {
    timing.verified = true;
  }
  return timing;
}

IterTiming pinned_chunk_d2h_once(const std::string& selection,
                                 void* dev,
                                 void* host,
                                 uint64_t bytes,
                                 const std::vector<std::unique_ptr<PinnedHostBuffer>>& chunks,
                                 StreamEventPool& se,
                                 const std::string& phase,
                                 int iteration,
                                 int32_t device,
                                 bool verify,
                                 uint8_t expected) {
  IterTiming timing;
  timing.iteration = iteration;
  timing.phase = phase;
  auto* src = static_cast<uint8_t*>(dev);
  auto* dst = static_cast<uint8_t*>(host);
  struct Inflight {
    bool active = false;
    uint64_t offset = 0;
    uint64_t bytes = 0;
    uint64_t chunk_index = 0;
  };
  std::vector<Inflight> inflight(chunks.size());
  auto wall_start = Clock::now();
  for (uint64_t offset = 0, chunk_index = 0; offset < bytes; offset += chunks[0]->bytes(), ++chunk_index) {
    const size_t slot = static_cast<size_t>(chunk_index % static_cast<uint64_t>(chunks.size()));
    if (inflight[slot].active) {
      auto wait_start = Clock::now();
      UF_ACL_CHECK(aclrtSynchronizeEvent(se.event(slot)));
      auto wait_end = Clock::now();
      timing.wait_us += elapsed_us(wait_start, wait_end);
      record_stage(selection,
                   "chunk.acl_wait.d2h",
                   wait_start,
                   wait_end,
                   inflight[slot].bytes,
                   device,
                   phase,
                   iteration,
                   "chunk_index=" + std::to_string(inflight[slot].chunk_index));
      auto copy_start = Clock::now();
      std::memcpy(dst + inflight[slot].offset, chunks[slot]->ptr(), static_cast<size_t>(inflight[slot].bytes));
      auto copy_end = Clock::now();
      timing.cpu_copy_us += elapsed_us(copy_start, copy_end);
      record_stage(selection, "chunk.cpu_copy.pinned_to_memfd", copy_start, copy_end, inflight[slot].bytes, device, phase, iteration);
      inflight[slot].active = false;
    }
    const uint64_t n = std::min<uint64_t>(chunks[0]->bytes(), bytes - offset);
    auto submit_start = Clock::now();
    UF_ACL_CHECK(aclrtMemcpyAsync(chunks[slot]->ptr(),
                                  static_cast<size_t>(n),
                                  src + offset,
                                  static_cast<size_t>(n),
                                  ACL_MEMCPY_DEVICE_TO_HOST,
                                  se.stream()));
    UF_ACL_CHECK(aclrtRecordEvent(se.event(slot), se.stream()));
    auto submit_end = Clock::now();
    timing.submit_us += elapsed_us(submit_start, submit_end);
    record_stage(selection, "chunk.acl_submit.d2h", submit_start, submit_end, n, device, phase, iteration);
    inflight[slot] = Inflight{true, offset, n, chunk_index};
  }
  for (size_t slot = 0; slot < inflight.size(); ++slot) {
    if (!inflight[slot].active) {
      continue;
    }
    auto wait_start = Clock::now();
    UF_ACL_CHECK(aclrtSynchronizeEvent(se.event(slot)));
    auto wait_end = Clock::now();
    timing.wait_us += elapsed_us(wait_start, wait_end);
    record_stage(selection,
                 "chunk.acl_wait.d2h",
                 wait_start,
                 wait_end,
                 inflight[slot].bytes,
                 device,
                 phase,
                 iteration,
                 "chunk_index=" + std::to_string(inflight[slot].chunk_index));
    auto copy_start = Clock::now();
    std::memcpy(dst + inflight[slot].offset, chunks[slot]->ptr(), static_cast<size_t>(inflight[slot].bytes));
    auto copy_end = Clock::now();
    timing.cpu_copy_us += elapsed_us(copy_start, copy_end);
    record_stage(selection, "chunk.cpu_copy.pinned_to_memfd", copy_start, copy_end, inflight[slot].bytes, device, phase, iteration);
  }
  auto wall_end = Clock::now();
  record_stage(selection, "transfer." + phase, wall_start, wall_end, bytes, device, phase, iteration);
  timing.wall_us = elapsed_us(wall_start, wall_end);
  if (verify) {
    auto verify_start = Clock::now();
    timing.verified = verify_host_samples(host, bytes, 32, expected);
    auto verify_end = Clock::now();
    record_stage(selection, "verify.sample_host", verify_start, verify_end, 32, device, phase, iteration);
  } else {
    timing.verified = true;
  }
  return timing;
}

void finalize_hot_stats(SelectionSummary& summary) {
  double total_wall = 0.0;
  double total_submit = 0.0;
  double total_wait = 0.0;
  double total_cpu = 0.0;
  summary.hot_min_us = std::numeric_limits<double>::max();
  summary.hot_max_us = 0.0;
  summary.verified = true;
  int hot_count = 0;
  for (const auto& iter : summary.iterations) {
    summary.verified = summary.verified && iter.verified;
    if (iter.phase != "hot") {
      if (iter.phase == "warmup") {
        summary.warmup_us += iter.wall_us;
      }
      continue;
    }
    ++hot_count;
    total_wall += iter.wall_us;
    total_submit += iter.submit_us;
    total_wait += iter.wait_us;
    total_cpu += iter.cpu_copy_us;
    summary.hot_min_us = std::min(summary.hot_min_us, iter.wall_us);
    summary.hot_max_us = std::max(summary.hot_max_us, iter.wall_us);
  }
  if (hot_count == 0) {
    summary.hot_min_us = 0.0;
    return;
  }
  summary.hot_avg_us = total_wall / static_cast<double>(hot_count);
  summary.hot_submit_us = total_submit / static_cast<double>(hot_count);
  summary.hot_wait_us = total_wait / static_cast<double>(hot_count);
  summary.hot_cpu_copy_us = total_cpu / static_cast<double>(hot_count);
  summary.hot_avg_gib_s = gib_per_s(summary.bytes, summary.hot_avg_us);
  summary.hot_min_gib_s = gib_per_s(summary.bytes, summary.hot_max_us);
  summary.hot_max_gib_s = gib_per_s(summary.bytes, summary.hot_min_us);
}

struct StrategySpec {
  std::string name;
  bool pretouch = false;
  bool mlock = false;
  bool thp = false;
  bool hugetlb = false;
  bool register_v2 = false;
  uint32_t v2_flags = 0;
  bool acl_malloc_host = false;
  bool pinned_staging = false;
};

SelectionSummary run_strategy(const Args& args, const StrategySpec& spec) {
  SelectionSummary summary;
  summary.selection = spec.name;
  summary.bytes = args.bytes;
  summary.warmups = args.warmups;
  summary.repeats = args.repeats;
  summary.v2_flags = spec.v2_flags;
  summary.used_v2 = spec.register_v2;
  summary.used_mlock = spec.mlock;
  summary.used_thp = spec.thp;
  summary.used_hugetlb = spec.hugetlb;
  summary.used_pinned_staging = spec.pinned_staging;
  summary.mmap_kind = spec.acl_malloc_host ? "aclrtMallocHost" : (spec.hugetlb ? "hugetlb_memfd" : "memfd");

  auto setup_start = Clock::now();
  auto hbm_start = Clock::now();
  DeviceBuffer dev(args.device, args.bytes, args.hbm_kind);
  auto hbm_end = Clock::now();
  summary.hbm_alloc_us = elapsed_us(hbm_start, hbm_end);
  record_stage(spec.name, "setup.hbm_aclrtMalloc", hbm_start, hbm_end, args.bytes, args.device);

  std::unique_ptr<MemfdMapping> memfd;
  std::unique_ptr<PinnedHostBuffer> host_full;
  std::vector<std::unique_ptr<PinnedHostBuffer>> chunks;
  void* host_ptr = nullptr;

  if (spec.acl_malloc_host) {
    auto host_start = Clock::now();
    host_full.reset(new PinnedHostBuffer(args.bytes));
    auto host_end = Clock::now();
    summary.acl_malloc_host_us = elapsed_us(host_start, host_end);
    record_stage(spec.name, "setup.aclrtMallocHost_full", host_start, host_end, args.bytes, args.device);
    auto zero_start = Clock::now();
    std::memset(host_full->ptr(), 0, static_cast<size_t>(args.bytes));
    auto zero_end = Clock::now();
    summary.pretouch_us = elapsed_us(zero_start, zero_end);
    record_stage(spec.name, "setup.host_zero", zero_start, zero_end, args.bytes, args.device);
    host_ptr = host_full->ptr();
  } else {
    const uint32_t memfd_flags = spec.hugetlb ? static_cast<uint32_t>(MFD_HUGETLB | MFD_HUGE_2MB) : 0u;
    auto memfd_start = Clock::now();
    memfd.reset(new MemfdMapping(args.bytes, "uflow_e03_" + spec.name, memfd_flags, spec.hugetlb));
    auto memfd_end = Clock::now();
    summary.memfd_mmap_us = elapsed_us(memfd_start, memfd_end);
    record_stage(spec.name, "setup.memfd_create_mmap", memfd_start, memfd_end, args.bytes, args.device);
    host_ptr = memfd->ptr();

    if (spec.thp) {
      auto madvise_start = Clock::now();
      errno = 0;
      const int rc = madvise(host_ptr, static_cast<size_t>(args.bytes), MADV_HUGEPAGE);
      const int err = errno;
      auto madvise_end = Clock::now();
      summary.madvise_us = elapsed_us(madvise_start, madvise_end);
      record_stage(spec.name,
                   "setup.madvise_hugepage",
                   madvise_start,
                   madvise_end,
                   args.bytes,
                   args.device,
                   "",
                   -1,
                   "rc=" + std::to_string(rc) + " errno=" + std::to_string(err));
      if (rc != 0) {
        throw SkipStrategy("madvise(MADV_HUGEPAGE) failed errno=" + std::to_string(err));
      }
    }

    if (spec.pretouch || spec.thp) {
      auto pretouch_start = Clock::now();
      pretouch_zero(host_ptr, args.bytes);
      auto pretouch_end = Clock::now();
      summary.pretouch_us = elapsed_us(pretouch_start, pretouch_end);
      record_stage(spec.name, "setup.pretouch_zero", pretouch_start, pretouch_end, args.bytes, args.device);
    }

    if (spec.mlock) {
      struct rlimit limit;
      if (getrlimit(RLIMIT_MEMLOCK, &limit) == 0 && limit.rlim_cur != RLIM_INFINITY &&
          limit.rlim_cur < static_cast<rlim_t>(args.bytes)) {
        throw SkipStrategy("RLIMIT_MEMLOCK soft limit " + std::to_string(limit.rlim_cur) +
                           " < bytes " + std::to_string(args.bytes));
      }
      auto mlock_start = Clock::now();
      errno = 0;
      const int rc = mlock(host_ptr, static_cast<size_t>(args.bytes));
      const int err = errno;
      auto mlock_end = Clock::now();
      summary.mlock_us = elapsed_us(mlock_start, mlock_end);
      record_stage(spec.name,
                   "setup.mlock",
                   mlock_start,
                   mlock_end,
                   args.bytes,
                   args.device,
                   "",
                   -1,
                   "rc=" + std::to_string(rc) + " errno=" + std::to_string(err));
      if (rc != 0) {
        throw SkipStrategy("mlock failed errno=" + std::to_string(err));
      }
    }
  }

  std::unique_ptr<HostRegistrationV2> registration;
  if (spec.register_v2) {
    double register_us = 0.0;
    const bool get_device_ptr = (spec.v2_flags & kHostRegMapped) != 0;
    auto register_start = Clock::now();
    registration.reset(new HostRegistrationV2(host_ptr, args.bytes, spec.v2_flags, get_device_ptr, &register_us));
    auto register_end = Clock::now();
    summary.register_us = register_us;
    record_stage(spec.name,
                 "setup.aclrtHostRegisterV2",
                 register_start,
                 register_end,
                 args.bytes,
                 args.device,
                 "",
                 -1,
                 "flags=" + hex_u32(spec.v2_flags));
  }

  if (spec.pinned_staging) {
    auto pinned_start = Clock::now();
    for (int i = 0; i < args.chunk_count; ++i) {
      chunks.emplace_back(new PinnedHostBuffer(args.chunk_bytes));
    }
    auto pinned_end = Clock::now();
    summary.pinned_pool_alloc_us = elapsed_us(pinned_start, pinned_end);
    record_stage(spec.name,
                 "setup.aclrtMallocHost_chunks",
                 pinned_start,
                 pinned_end,
                 args.chunk_bytes * static_cast<uint64_t>(args.chunk_count),
                 args.device);
  }

  summary.smaps = spec.acl_malloc_host ? SmapsInfo{} : read_smaps_info(host_ptr);
  auto setup_end = Clock::now();
  summary.setup_us = elapsed_us(setup_start, setup_end);
  record_stage(spec.name, "setup.total", setup_start, setup_end, args.bytes, args.device);

  std::unique_ptr<StreamEvent> direct_se;
  std::unique_ptr<StreamEventPool> pool_se;
  if (spec.pinned_staging) {
    pool_se.reset(new StreamEventPool(static_cast<int>(chunks.size())));
  } else {
    direct_se.reset(new StreamEvent());
  }
  for (int i = 0; i < args.warmups; ++i) {
    if (args.direction == "d2h") {
      const uint8_t pattern = d2h_pattern_for("warmup", i);
      fill_hbm_pattern(dev.ptr(), args.bytes, pattern, spec.name, "warmup", i, args.device);
      summary.iterations.push_back(spec.pinned_staging ? pinned_chunk_d2h_once(spec.name,
                                                                               dev.ptr(),
                                                                               host_ptr,
                                                                               args.bytes,
                                                                               chunks,
                                                                               *pool_se,
                                                                               "warmup",
                                                                               i,
                                                                               args.device,
                                                                               args.verify,
                                                                               pattern)
                                                      : direct_d2h_once(spec.name,
                                                                        dev.ptr(),
                                                                        host_ptr,
                                                                        args.bytes,
                                                                        *direct_se,
                                                                        "warmup",
                                                                        i,
                                                                        args.device,
                                                                        args.verify,
                                                                        pattern));
    } else {
      poison_hbm(dev.ptr(), args.bytes, spec.name, "warmup", i, args.device);
      summary.iterations.push_back(spec.pinned_staging ? pinned_chunk_h2d_once(spec.name,
                                                                               dev.ptr(),
                                                                               host_ptr,
                                                                               args.bytes,
                                                                               chunks,
                                                                               *pool_se,
                                                                               "warmup",
                                                                               i,
                                                                               args.device,
                                                                               args.verify)
                                                      : direct_h2d_once(spec.name,
                                                                        dev.ptr(),
                                                                        host_ptr,
                                                                        args.bytes,
                                                                        *direct_se,
                                                                        "warmup",
                                                                        i,
                                                                        args.device,
                                                                        args.verify));
    }
  }
  for (int i = 0; i < args.repeats; ++i) {
    if (args.direction == "d2h") {
      const uint8_t pattern = d2h_pattern_for("hot", i);
      fill_hbm_pattern(dev.ptr(), args.bytes, pattern, spec.name, "hot", i, args.device);
      summary.iterations.push_back(spec.pinned_staging ? pinned_chunk_d2h_once(spec.name,
                                                                               dev.ptr(),
                                                                               host_ptr,
                                                                               args.bytes,
                                                                               chunks,
                                                                               *pool_se,
                                                                               "hot",
                                                                               i,
                                                                               args.device,
                                                                               args.verify,
                                                                               pattern)
                                                      : direct_d2h_once(spec.name,
                                                                        dev.ptr(),
                                                                        host_ptr,
                                                                        args.bytes,
                                                                        *direct_se,
                                                                        "hot",
                                                                        i,
                                                                        args.device,
                                                                        args.verify,
                                                                        pattern));
    } else {
      poison_hbm(dev.ptr(), args.bytes, spec.name, "hot", i, args.device);
      summary.iterations.push_back(spec.pinned_staging ? pinned_chunk_h2d_once(spec.name,
                                                                               dev.ptr(),
                                                                               host_ptr,
                                                                               args.bytes,
                                                                               chunks,
                                                                               *pool_se,
                                                                               "hot",
                                                                               i,
                                                                               args.device,
                                                                               args.verify)
                                                      : direct_h2d_once(spec.name,
                                                                        dev.ptr(),
                                                                        host_ptr,
                                                                        args.bytes,
                                                                        *direct_se,
                                                                        "hot",
                                                                        i,
                                                                        args.device,
                                                                        args.verify));
    }
  }

  finalize_hot_stats(summary);

  if (registration) {
    auto unregister_start = Clock::now();
    summary.unregister_us = registration->unregister_now();
    auto unregister_end = Clock::now();
    record_stage(spec.name,
                 "cleanup.aclrtHostUnregister",
                 unregister_start,
                 unregister_end,
                 args.bytes,
                 args.device);
  }
  if (spec.mlock && host_ptr != nullptr) {
    (void)munlock(host_ptr, static_cast<size_t>(args.bytes));
  }
  return summary;
}

SelectionSummary skipped_summary(const Args& args, const StrategySpec& spec, const std::string& why, const std::string& status) {
  SelectionSummary s;
  s.selection = spec.name;
  s.status = status;
  s.error = why;
  s.bytes = args.bytes;
  s.warmups = args.warmups;
  s.repeats = args.repeats;
  s.v2_flags = spec.v2_flags;
  s.used_v2 = spec.register_v2;
  s.used_mlock = spec.mlock;
  s.used_thp = spec.thp;
  s.used_hugetlb = spec.hugetlb;
  s.used_pinned_staging = spec.pinned_staging;
  s.mmap_kind = spec.acl_malloc_host ? "aclrtMallocHost" : (spec.hugetlb ? "hugetlb_memfd" : "memfd");
  return s;
}

int selection_index(const std::string& selection) {
  static const std::vector<std::string> names = {
      "memfd_direct",
      "memfd_pretouch",
      "memfd_mlock",
      "memfd_thp",
      "memfd_v2_pinned",
      "memfd_v2_mapped",
      "memfd_v2_mapped_pinned",
      "memfd_mlock_v2_pinned",
      "memfd_thp_v2_pinned",
      "hugetlb_memfd_v2_pinned",
      "acl_malloc_host_dma_hbm",
      "memfd_pinned_chunk_hbm",
  };
  for (size_t i = 0; i < names.size(); ++i) {
    if (names[i] == selection) {
      return static_cast<int>(i);
    }
  }
  return 99;
}

int tid_for_stage(const StageEvent& e) {
  const int base = (selection_index(e.selection) + 1) * 100;
  if (e.stage.find("setup") == 0) {
    return base + 1;
  }
  if (e.stage.find("poison") != std::string::npos) {
    return base + 2;
  }
  if (e.stage.find("cpu_copy") != std::string::npos) {
    return base + 3;
  }
  if (e.stage.find("acl_submit") != std::string::npos) {
    return base + 4;
  }
  if (e.stage.find("acl_wait") != std::string::npos) {
    return base + 5;
  }
  if (e.stage.find("transfer") == 0) {
    return base;
  }
  if (e.stage.find("verify") == 0 || e.stage.find("cleanup") == 0) {
    return base + 6;
  }
  return base + 9;
}

void write_trace_json(const std::string& output_dir, const Args& args, const std::vector<SelectionSummary>& summaries) {
  const std::string prefix = args.direction + "_stage";
  std::ofstream out(output_dir + "/" + prefix + "_trace.json");
  out << std::fixed << std::setprecision(3);
  out << "{\n"
      << "  \"displayTimeUnit\": \"ms\",\n"
      << "  \"metadata\": {\"title\": \"UFlow PhaseE-03 " << args.direction << " memfd fast path matrix\","
      << "\"hbm_kind\":\"" << json_escape(args.hbm_kind) << "\"},\n"
      << "  \"traceEvents\": [\n";
  bool first = true;
  auto write_event = [&](const std::string& raw) {
    if (!first) {
      out << ",\n";
    }
    first = false;
    out << raw;
  };
  for (const auto& summary : summaries) {
    const int base = (selection_index(summary.selection) + 1) * 100;
    const std::vector<std::pair<int, std::string>> tids = {
        {base, summary.selection + " transfer"},
        {base + 1, summary.selection + " setup"},
        {base + 2, summary.selection + " poison"},
        {base + 3, summary.selection + " CPU copy"},
        {base + 4, summary.selection + " ACL submit"},
        {base + 5, summary.selection + " ACL wait"},
        {base + 6, summary.selection + " verify/cleanup"},
    };
    for (const auto& [tid, name] : tids) {
      std::ostringstream event;
      event << "    {\"name\":\"thread_name\",\"ph\":\"M\",\"pid\":1,\"tid\":" << tid
            << ",\"args\":{\"name\":\"" << json_escape(name) << "\"}}";
      write_event(event.str());
    }
  }
  for (const auto& e : g_events) {
    std::ostringstream event;
    event << "    {"
          << "\"name\":\"" << json_escape(e.stage) << "\","
          << "\"cat\":\"uflow." << args.direction << ".memfd_matrix\","
          << "\"ph\":\"X\","
          << "\"pid\":1,"
          << "\"tid\":" << tid_for_stage(e) << ","
          << "\"ts\":" << e.ts_us << ","
          << "\"dur\":" << std::max(1.0, e.dur_us) << ","
          << "\"args\":{"
          << "\"selection\":\"" << json_escape(e.selection) << "\","
          << "\"bytes\":" << e.bytes << ","
          << "\"device_id\":" << e.device;
    if (!e.phase.empty()) {
      event << ",\"phase\":\"" << json_escape(e.phase) << "\"";
    }
    if (e.iteration >= 0) {
      event << ",\"iteration\":" << e.iteration;
    }
    if (!e.detail.empty()) {
      event << ",\"detail\":\"" << json_escape(e.detail) << "\"";
    }
    event << "}}";
    write_event(event.str());
  }
  out << "\n  ]\n}\n";
}

void write_summary_csv(const std::string& output_dir, const Args& args, const std::vector<SelectionSummary>& summaries) {
  const std::string prefix = args.direction + "_stage";
  std::ofstream out(output_dir + "/" + prefix + "_summary.csv");
  out << "direction,hbm_kind,selection,status,error,bytes,gib,setup_us,hbm_alloc_us,memfd_mmap_us,acl_malloc_host_us,pinned_pool_alloc_us,pretouch_us,mlock_us,madvise_us,register_us,unregister_us,warmup_us,hot_avg_us,hot_min_us,hot_max_us,hot_submit_us,hot_wait_us,hot_cpu_copy_us,hot_avg_gib_s,hot_min_gib_s,hot_max_gib_s,warmups,repeats,v2_flags,mmap_kind,used_mlock,used_thp,used_hugetlb,used_pinned_staging,anon_huge_kb,shmem_pmd_mapped_kb,file_pmd_mapped_kb,kernel_page_kb,mmu_page_kb,verified\n";
  out << std::fixed << std::setprecision(6);
  for (const auto& s : summaries) {
    out << args.direction << ","
        << args.hbm_kind << ","
        << s.selection << ","
        << s.status << ","
        << "\"" << json_escape(s.error) << "\","
        << s.bytes << ","
        << (static_cast<double>(s.bytes) / static_cast<double>(kGiB)) << ","
        << s.setup_us << ","
        << s.hbm_alloc_us << ","
        << s.memfd_mmap_us << ","
        << s.acl_malloc_host_us << ","
        << s.pinned_pool_alloc_us << ","
        << s.pretouch_us << ","
        << s.mlock_us << ","
        << s.madvise_us << ","
        << s.register_us << ","
        << s.unregister_us << ","
        << s.warmup_us << ","
        << s.hot_avg_us << ","
        << s.hot_min_us << ","
        << s.hot_max_us << ","
        << s.hot_submit_us << ","
        << s.hot_wait_us << ","
        << s.hot_cpu_copy_us << ","
        << s.hot_avg_gib_s << ","
        << s.hot_min_gib_s << ","
        << s.hot_max_gib_s << ","
        << s.warmups << ","
        << s.repeats << ","
        << hex_u32(s.v2_flags) << ","
        << s.mmap_kind << ","
        << (s.used_mlock ? "true" : "false") << ","
        << (s.used_thp ? "true" : "false") << ","
        << (s.used_hugetlb ? "true" : "false") << ","
        << (s.used_pinned_staging ? "true" : "false") << ","
        << s.smaps.anon_huge_kb << ","
        << s.smaps.shmem_pmd_mapped_kb << ","
        << s.smaps.file_pmd_mapped_kb << ","
        << s.smaps.kernel_page_kb << ","
        << s.smaps.mmu_page_kb << ","
        << (s.verified ? "true" : "false") << "\n";
  }
}

void write_summary_json(const std::string& output_dir, const Args& args, const std::vector<SelectionSummary>& summaries) {
  const std::string prefix = args.direction + "_stage";
  std::ofstream out(output_dir + "/" + prefix + "_summary.json");
  out << std::fixed << std::setprecision(6);
  out << "{\n"
      << "  \"title\": \"UFlow PhaseE-03 " << args.direction << " memfd fast path matrix\",\n"
      << "  \"direction\": \"" << args.direction << "\",\n"
      << "  \"hbm_kind\": \"" << json_escape(args.hbm_kind) << "\",\n"
      << "  \"device_id\": " << args.device << ",\n"
      << "  \"bytes\": " << args.bytes << ",\n"
      << "  \"gib\": " << (static_cast<double>(args.bytes) / static_cast<double>(kGiB)) << ",\n"
      << "  \"warmups\": " << args.warmups << ",\n"
      << "  \"repeats\": " << args.repeats << ",\n"
      << "  \"finest_trace\": \"" << prefix << "_trace.json\",\n"
      << "  \"results\": [\n";
  for (size_t i = 0; i < summaries.size(); ++i) {
    const auto& s = summaries[i];
    out << "    {\n"
        << "      \"selection\": \"" << json_escape(s.selection) << "\",\n"
        << "      \"status\": \"" << json_escape(s.status) << "\",\n"
        << "      \"error\": \"" << json_escape(s.error) << "\",\n"
        << "      \"bytes\": " << s.bytes << ",\n"
        << "      \"setup_ms\": " << (s.setup_us / 1000.0) << ",\n"
        << "      \"register_ms\": " << (s.register_us / 1000.0) << ",\n"
        << "      \"unregister_ms\": " << (s.unregister_us / 1000.0) << ",\n"
        << "      \"warmup_ms\": " << (s.warmup_us / 1000.0) << ",\n"
        << "      \"hot_avg_gib_s\": " << s.hot_avg_gib_s << ",\n"
        << "      \"hot_min_gib_s\": " << s.hot_min_gib_s << ",\n"
        << "      \"hot_max_gib_s\": " << s.hot_max_gib_s << ",\n"
        << "      \"hot_avg_ms\": " << (s.hot_avg_us / 1000.0) << ",\n"
        << "      \"hot_min_ms\": " << (s.hot_min_us / 1000.0) << ",\n"
        << "      \"hot_max_ms\": " << (s.hot_max_us / 1000.0) << ",\n"
        << "      \"v2_flags\": \"" << hex_u32(s.v2_flags) << "\",\n"
        << "      \"mmap_kind\": \"" << json_escape(s.mmap_kind) << "\",\n"
        << "      \"used_mlock\": " << (s.used_mlock ? "true" : "false") << ",\n"
        << "      \"used_thp\": " << (s.used_thp ? "true" : "false") << ",\n"
        << "      \"used_hugetlb\": " << (s.used_hugetlb ? "true" : "false") << ",\n"
        << "      \"used_pinned_staging\": " << (s.used_pinned_staging ? "true" : "false") << ",\n"
        << "      \"verified\": " << (s.verified ? "true" : "false") << ",\n"
        << "      \"smaps\": {\"anon_huge_kb\": " << s.smaps.anon_huge_kb
        << ", \"shmem_pmd_mapped_kb\": " << s.smaps.shmem_pmd_mapped_kb
        << ", \"file_pmd_mapped_kb\": " << s.smaps.file_pmd_mapped_kb
        << ", \"kernel_page_kb\": " << s.smaps.kernel_page_kb
        << ", \"mmu_page_kb\": " << s.smaps.mmu_page_kb << "},\n"
        << "      \"iterations\": [";
    for (size_t j = 0; j < s.iterations.size(); ++j) {
      const auto& it = s.iterations[j];
      out << "{\"phase\":\"" << json_escape(it.phase) << "\","
          << "\"iteration\":" << it.iteration << ","
          << "\"wall_ms\":" << (it.wall_us / 1000.0) << ","
          << "\"submit_ms\":" << (it.submit_us / 1000.0) << ","
          << "\"wait_ms\":" << (it.wait_us / 1000.0) << ","
          << "\"cpu_copy_ms\":" << (it.cpu_copy_us / 1000.0) << ","
          << "\"gib_s\":" << gib_per_s(s.bytes, it.wall_us) << ","
          << "\"verified\":" << (it.verified ? "true" : "false") << "}";
      if (j + 1 != s.iterations.size()) {
        out << ", ";
      }
    }
    out << "]\n"
        << "    }";
    if (i + 1 != summaries.size()) {
      out << ",";
    }
    out << "\n";
  }
  out << "  ]\n"
      << "}\n";
}

void write_summary_md(const std::string& output_dir, const Args& args, const std::vector<SelectionSummary>& summaries) {
  const std::string prefix = args.direction + "_stage";
  std::ofstream out(output_dir + "/summary.md");
  out << "# PhaseE-03 " << args.direction << " Memfd Fast Path Matrix\n\n"
      << "- device: NPU" << args.device << "\n"
      << "- direction: `" << args.direction << "`\n"
      << "- hbm_kind: `" << args.hbm_kind << "`\n"
      << "- bytes: " << args.bytes << " (" << (static_cast<double>(args.bytes) / kGiB) << " GiB)\n"
      << "- warmups: " << args.warmups << "\n"
      << "- repeats: " << args.repeats << "\n"
      << "- finest trace: `" << prefix << "_trace.json`\n\n"
      << "| selection | status | hot avg GiB/s | hot min GiB/s | hot max GiB/s | hot avg ms | register ms | warmup ms | flags | verified | note |\n"
      << "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|\n"
      << std::fixed << std::setprecision(3);
  for (const auto& s : summaries) {
    out << "| `" << s.selection << "` | " << s.status << " | "
        << s.hot_avg_gib_s << " | "
        << s.hot_min_gib_s << " | "
        << s.hot_max_gib_s << " | "
        << (s.hot_avg_us / 1000.0) << " | "
        << (s.register_us / 1000.0) << " | "
        << (s.warmup_us / 1000.0) << " | `"
        << hex_u32(s.v2_flags) << "` | "
        << (s.verified ? "true" : "false") << " | "
        << json_escape(s.error) << " |\n";
  }
}

}  // namespace

int main(int argc, char** argv) {
  try {
    Args args = parse_args(argc, argv);
    ensure_dir(args.output_dir);
    g_t0 = Clock::now();
    AclRuntime rt(args.device);

    const std::vector<StrategySpec> specs = {
        {"memfd_direct"},
        {"memfd_pretouch", true},
        {"memfd_mlock", false, true},
        {"memfd_thp", false, false, true},
        {"memfd_v2_pinned", false, false, false, false, true, kHostRegPinned},
        {"memfd_v2_mapped", false, false, false, false, true, kHostRegMapped},
        {"memfd_v2_mapped_pinned", false, false, false, false, true, kHostRegMapped | kHostRegPinned},
        {"memfd_mlock_v2_pinned", false, true, false, false, true, kHostRegPinned},
        {"memfd_thp_v2_pinned", false, false, true, false, true, kHostRegPinned},
        {"hugetlb_memfd_v2_pinned", false, false, false, true, true, kHostRegPinned},
        {"acl_malloc_host_dma_hbm", false, false, false, false, false, 0, true},
        {"memfd_pinned_chunk_hbm", false, false, false, false, false, 0, false, true},
    };

    std::vector<SelectionSummary> summaries;
    for (const auto& spec : specs) {
      if (!selection_enabled(args, spec.name)) {
        continue;
      }
      try {
        auto summary = run_strategy(args, spec);
        summaries.push_back(summary);
        std::cout << std::fixed << std::setprecision(6)
                  << "UFLOW_E03_" << (args.direction == "d2h" ? "D2H" : "H2D")
                  << "_MATRIX_RESULT selection=" << summary.selection
                  << " hbm_kind=" << args.hbm_kind
                  << " status=" << summary.status
                  << " hot_avg_gib_s=" << summary.hot_avg_gib_s
                  << " hot_min_gib_s=" << summary.hot_min_gib_s
                  << " hot_max_gib_s=" << summary.hot_max_gib_s
                  << " register_us=" << summary.register_us
                  << " warmup_us=" << summary.warmup_us
                  << " verified=" << (summary.verified ? "true" : "false")
                  << std::endl;
      } catch (const SkipStrategy& e) {
        auto summary = skipped_summary(args, spec, e.what(), "skipped");
        summaries.push_back(summary);
        std::cout << "UFLOW_E03_" << (args.direction == "d2h" ? "D2H" : "H2D")
                  << "_MATRIX_RESULT selection=" << summary.selection
                  << " hbm_kind=" << args.hbm_kind
                  << " status=skipped error=\"" << e.what() << "\"" << std::endl;
      } catch (const std::exception& e) {
        auto summary = skipped_summary(args, spec, e.what(), "error");
        summaries.push_back(summary);
        std::cerr << "UFLOW_E03_" << (args.direction == "d2h" ? "D2H" : "H2D")
                  << "_MATRIX_ERROR selection=" << summary.selection
                  << " hbm_kind=" << args.hbm_kind
                  << " error=\"" << e.what() << "\"" << std::endl;
      }
    }

    write_trace_json(args.output_dir, args, summaries);
    write_summary_json(args.output_dir, args, summaries);
    write_summary_csv(args.output_dir, args, summaries);
    write_summary_md(args.output_dir, args, summaries);
    std::cout << "UFLOW_E03_" << (args.direction == "d2h" ? "D2H" : "H2D")
              << "_MATRIX_OUTPUT " << args.output_dir << std::endl;
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "acl_h2d_memfd_fastpath_matrix error: " << e.what() << std::endl;
    return 1;
  }
}
