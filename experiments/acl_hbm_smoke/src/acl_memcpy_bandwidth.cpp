#include "acl_common.hpp"

#include <acl/acl.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

using namespace uf::phasea;

namespace {

struct Args {
  int32_t device = 0;
  uint64_t min_bytes = 16ull << 20;
  uint64_t max_bytes = 2ull << 30;
  int warmup = 2;
  int iters = 10;
  std::string host_kind = "malloc";
  std::string copy_mode = "sync";
  std::string label = "default";
  bool verify = true;
  uint64_t verify_stride = 4096;
};

uint64_t parse_size(const std::string& text) {
  if (text.empty()) {
    throw std::runtime_error("empty size");
  }
  size_t pos = 0;
  double value = std::stod(text, &pos);
  std::string suffix = text.substr(pos);
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
      args.device = static_cast<int32_t>(std::stoi(next()));
    } else if (key == "--min-bytes") {
      args.min_bytes = parse_size(next());
    } else if (key == "--max-bytes") {
      args.max_bytes = parse_size(next());
    } else if (key == "--warmup") {
      args.warmup = std::stoi(next());
    } else if (key == "--iters") {
      args.iters = std::stoi(next());
    } else if (key == "--host-kind") {
      args.host_kind = next();
    } else if (key == "--copy-mode") {
      args.copy_mode = next();
    } else if (key == "--label") {
      args.label = next();
    } else if (key == "--no-verify") {
      args.verify = false;
    } else if (key == "--verify-stride") {
      args.verify_stride = parse_size(next());
    } else {
      throw std::runtime_error("unknown arg: " + key);
    }
  }
  if (args.min_bytes == 0 || args.max_bytes < args.min_bytes) {
    throw std::runtime_error("invalid byte range");
  }
  if (args.iters <= 0 || args.warmup < 0) {
    throw std::runtime_error("--iters must be > 0 and --warmup must be >= 0");
  }
  if (args.host_kind != "malloc" && args.host_kind != "pinned") {
    throw std::runtime_error("--host-kind must be malloc or pinned");
  }
  if (args.copy_mode != "sync" && args.copy_mode != "async") {
    throw std::runtime_error("--copy-mode must be sync or async");
  }
  return args;
}

class HostBuffer {
 public:
  HostBuffer(uint64_t bytes, std::string kind) : bytes_(bytes), kind_(std::move(kind)) {
    if (kind_ == "pinned") {
      UF_ACL_CHECK(aclrtMallocHost(&ptr_, static_cast<size_t>(bytes_)));
      return;
    }
    void* p = nullptr;
    if (posix_memalign(&p, 4096, static_cast<size_t>(bytes_)) != 0) {
      throw std::runtime_error("posix_memalign failed");
    }
    ptr_ = p;
  }

  ~HostBuffer() {
    if (ptr_ == nullptr) {
      return;
    }
    if (kind_ == "pinned") {
      (void)aclrtFreeHost(ptr_);
    } else {
      std::free(ptr_);
    }
  }

  HostBuffer(const HostBuffer&) = delete;
  HostBuffer& operator=(const HostBuffer&) = delete;

  void* ptr() const { return ptr_; }
  uint64_t bytes() const { return bytes_; }

 private:
  void* ptr_ = nullptr;
  uint64_t bytes_ = 0;
  std::string kind_;
};

class AclStreamEvent {
 public:
  AclStreamEvent() {
    UF_ACL_CHECK(aclrtCreateStream(&stream_));
    try {
      UF_ACL_CHECK(aclrtCreateEvent(&event_));
    } catch (...) {
      (void)aclrtDestroyStream(stream_);
      stream_ = nullptr;
      throw;
    }
  }

  ~AclStreamEvent() {
    if (event_ != nullptr) {
      (void)aclrtDestroyEvent(event_);
    }
    if (stream_ != nullptr) {
      (void)aclrtDestroyStream(stream_);
    }
  }

  AclStreamEvent(const AclStreamEvent&) = delete;
  AclStreamEvent& operator=(const AclStreamEvent&) = delete;

  aclrtStream stream() const { return stream_; }
  aclrtEvent event() const { return event_; }

 private:
  aclrtStream stream_ = nullptr;
  aclrtEvent event_ = nullptr;
};

void fill_pattern(void* ptr, uint64_t bytes, uint32_t seed) {
  auto* data = static_cast<uint32_t*>(ptr);
  uint64_t words = bytes / sizeof(uint32_t);
  for (uint64_t i = 0; i < words; ++i) {
    data[i] = seed ^ static_cast<uint32_t>(i * 2654435761u) ^ static_cast<uint32_t>(i >> 9);
  }
  auto* tail = static_cast<uint8_t*>(ptr) + words * sizeof(uint32_t);
  for (uint64_t i = words * sizeof(uint32_t); i < bytes; ++i) {
    *tail++ = static_cast<uint8_t>((i ^ seed) & 0xffu);
  }
}

uint8_t expected_pattern_byte(uint64_t offset, uint32_t seed) {
  uint64_t word_index = offset / sizeof(uint32_t);
  uint64_t byte_index = offset % sizeof(uint32_t);
  uint32_t word = seed ^ static_cast<uint32_t>(word_index * 2654435761u) ^
                  static_cast<uint32_t>(word_index >> 9);
  return static_cast<uint8_t>((word >> (byte_index * 8)) & 0xffu);
}

bool verify_pattern(const void* ptr,
                    uint64_t bytes,
                    uint32_t seed,
                    uint64_t stride,
                    uint64_t* mismatch) {
  const auto* data = static_cast<const uint8_t*>(ptr);
  auto check_one = [&](uint64_t i) -> bool {
    if (data[i] != expected_pattern_byte(i, seed)) {
      if (mismatch != nullptr) {
        *mismatch = i;
      }
      return false;
    }
    return true;
  };
  const uint64_t prefix = std::min<uint64_t>(bytes, 4096);
  for (uint64_t i = 0; i < prefix; ++i) {
    if (!check_one(i)) {
      return false;
    }
  }
  if (stride == 0) {
    for (uint64_t i = prefix; i < bytes; ++i) {
      if (!check_one(i)) {
        return false;
      }
    }
    return true;
  }
  for (uint64_t i = prefix; i < bytes; i += stride) {
    if (!check_one(i)) {
      return false;
    }
  }
  if (bytes > 0 && !check_one(bytes - 1)) {
    return false;
  }
  return true;
}

double percentile(std::vector<double> values, double q) {
  if (values.empty()) {
    return 0.0;
  }
  std::sort(values.begin(), values.end());
  size_t index = static_cast<size_t>(q * static_cast<double>(values.size() - 1));
  return values[index];
}

struct TimingStats {
  double min_ms = 0.0;
  double avg_ms = 0.0;
  double median_ms = 0.0;
  double p95_ms = 0.0;
  double best_gib_s = 0.0;
  double avg_gib_s = 0.0;
};

TimingStats summarize(const std::vector<double>& ms, uint64_t bytes) {
  TimingStats out;
  out.min_ms = *std::min_element(ms.begin(), ms.end());
  out.avg_ms = std::accumulate(ms.begin(), ms.end(), 0.0) / static_cast<double>(ms.size());
  out.median_ms = percentile(ms, 0.50);
  out.p95_ms = percentile(ms, 0.95);
  const double gib = static_cast<double>(bytes) / static_cast<double>(1ull << 30);
  out.best_gib_s = gib / (out.min_ms / 1000.0);
  out.avg_gib_s = gib / (out.avg_ms / 1000.0);
  return out;
}

std::vector<double> time_copies(void* dst,
                                size_t dst_max,
                                const void* src,
                                size_t src_max,
                                uint64_t bytes,
                                aclrtMemcpyKind kind,
                                const std::string& copy_mode,
                                int warmup,
                                int iters) {
  if (copy_mode == "async") {
    AclStreamEvent se;
    for (int i = 0; i < warmup; ++i) {
      UF_ACL_CHECK(aclrtMemcpyAsync(dst, dst_max, src, src_max, kind, se.stream()));
      UF_ACL_CHECK(aclrtRecordEvent(se.event(), se.stream()));
      UF_ACL_CHECK(aclrtSynchronizeEvent(se.event()));
    }
    std::vector<double> ms;
    ms.reserve(static_cast<size_t>(iters));
    for (int i = 0; i < iters; ++i) {
      auto start = std::chrono::steady_clock::now();
      UF_ACL_CHECK(aclrtMemcpyAsync(dst, dst_max, src, src_max, kind, se.stream()));
      UF_ACL_CHECK(aclrtRecordEvent(se.event(), se.stream()));
      UF_ACL_CHECK(aclrtSynchronizeEvent(se.event()));
      auto stop = std::chrono::steady_clock::now();
      std::chrono::duration<double, std::milli> elapsed = stop - start;
      ms.push_back(elapsed.count());
    }
    return ms;
  }

  for (int i = 0; i < warmup; ++i) {
    UF_ACL_CHECK(aclrtMemcpy(dst, dst_max, src, src_max, kind));
  }
  std::vector<double> ms;
  ms.reserve(static_cast<size_t>(iters));
  for (int i = 0; i < iters; ++i) {
    auto start = std::chrono::steady_clock::now();
    UF_ACL_CHECK(aclrtMemcpy(dst, dst_max, src, src_max, kind));
    auto stop = std::chrono::steady_clock::now();
    std::chrono::duration<double, std::milli> elapsed = stop - start;
    ms.push_back(elapsed.count());
  }
  return ms;
}

void print_result(const Args& args,
                  uint64_t bytes,
                  const std::string& direction,
                  const TimingStats& stats) {
  std::cout << std::fixed << std::setprecision(6)
            << "{"
            << "\"test\":\"acl_memcpy_bandwidth\","
            << "\"label\":\"" << args.label << "\","
            << "\"device_id\":" << args.device << ","
            << "\"host_kind\":\"" << args.host_kind << "\","
            << "\"copy_mode\":\"" << args.copy_mode << "\","
            << "\"direction\":\"" << direction << "\","
            << "\"bytes\":" << bytes << ","
            << "\"mib\":" << (static_cast<double>(bytes) / static_cast<double>(1ull << 20)) << ","
            << "\"iters\":" << args.iters << ","
            << "\"warmup\":" << args.warmup << ","
            << "\"min_ms\":" << stats.min_ms << ","
            << "\"avg_ms\":" << stats.avg_ms << ","
            << "\"median_ms\":" << stats.median_ms << ","
            << "\"p95_ms\":" << stats.p95_ms << ","
            << "\"best_gib_s\":" << stats.best_gib_s << ","
            << "\"avg_gib_s\":" << stats.avg_gib_s
            << "}" << std::endl;
}

void run_one_size(const Args& args, uint64_t bytes) {
  void* dev = nullptr;
  UF_ACL_CHECK(aclrtMalloc(&dev, static_cast<size_t>(bytes), ACL_MEM_MALLOC_HUGE_FIRST));
  try {
    std::vector<double> h2d_ms;
    {
      HostBuffer host_src(bytes, args.host_kind);
      fill_pattern(host_src.ptr(), bytes, 0x31415926u);
      h2d_ms = time_copies(dev,
                           static_cast<size_t>(bytes),
                           host_src.ptr(),
                           static_cast<size_t>(bytes),
                           bytes,
                           ACL_MEMCPY_HOST_TO_DEVICE,
                           args.copy_mode,
                           args.warmup,
                           args.iters);
    }
    std::vector<double> d2h_ms;
    {
      HostBuffer host_dst(bytes, args.host_kind);
      std::memset(host_dst.ptr(), 0, static_cast<size_t>(bytes));
      d2h_ms = time_copies(host_dst.ptr(),
                           static_cast<size_t>(bytes),
                           dev,
                           static_cast<size_t>(bytes),
                           bytes,
                           ACL_MEMCPY_DEVICE_TO_HOST,
                           args.copy_mode,
                           args.warmup,
                           args.iters);
      if (args.verify) {
        uint64_t mismatch = 0;
        if (!verify_pattern(host_dst.ptr(), bytes, 0x31415926u, args.verify_stride, &mismatch)) {
          std::ostringstream oss;
          oss << "verify failed bytes=" << bytes << " mismatch_offset=" << mismatch;
          throw std::runtime_error(oss.str());
        }
      }
    }
    print_result(args, bytes, "h2d", summarize(h2d_ms, bytes));
    print_result(args, bytes, "d2h", summarize(d2h_ms, bytes));
  } catch (...) {
    (void)aclrtFree(dev);
    throw;
  }
  UF_ACL_CHECK(aclrtFree(dev));
}

}  // namespace

int main(int argc, char** argv) {
  try {
    Args args = parse_args(argc, argv);
    AclRuntime rt(args.device);
    std::cout << "{"
              << "\"test\":\"acl_memcpy_bandwidth_start\","
              << "\"label\":\"" << args.label << "\","
              << "\"device_id\":" << args.device << ","
              << "\"host_kind\":\"" << args.host_kind << "\","
              << "\"copy_mode\":\"" << args.copy_mode << "\","
              << "\"min_bytes\":" << args.min_bytes << ","
              << "\"max_bytes\":" << args.max_bytes << ","
              << "\"iters\":" << args.iters << ","
              << "\"warmup\":" << args.warmup
              << "}" << std::endl;
    for (uint64_t bytes = args.min_bytes; bytes <= args.max_bytes; bytes <<= 1) {
      run_one_size(args, bytes);
      if (bytes > (UINT64_MAX >> 1)) {
        break;
      }
    }
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "acl_memcpy_bandwidth error: " << e.what() << std::endl;
    return 1;
  }
}
