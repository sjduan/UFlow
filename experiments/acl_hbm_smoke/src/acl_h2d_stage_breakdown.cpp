#include "acl_common.hpp"

#include <acl/acl.h>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

using namespace uf::phasea;

namespace {

constexpr uint64_t kMiB = 1024ull * 1024ull;
constexpr uint64_t kGiB = 1024ull * 1024ull * 1024ull;

struct Args {
  int32_t device = 7;
  uint64_t bytes = 2ull * kGiB;
  uint64_t chunk_bytes = 16ull * kMiB;
  int chunk_count = 2;
  std::string output_dir = "/tmp/proj_output/phasee06_h2d_stage";
  bool verify = true;
};

struct StageEvent {
  std::string selection;
  std::string stage;
  uint64_t chunk_index = UINT64_MAX;
  uint64_t bytes = 0;
  double ts_us = 0.0;
  double dur_us = 0.0;
  int32_t device = 0;
  std::string detail;
};

struct SelectionSummary {
  std::string selection;
  std::string status = "ok";
  std::string error;
  uint64_t bytes = 0;
  double wall_us = 0.0;
  double setup_us = 0.0;
  double hot_us = 0.0;
  double cpu_copy_us = 0.0;
  double acl_submit_us = 0.0;
  double acl_wait_us = 0.0;
  double register_us = 0.0;
  double unregister_us = 0.0;
  uint64_t chunks = 0;
  uint64_t chunk_bytes = 0;
  uint64_t pinned_footprint_bytes = 0;
  bool verified = false;
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
    } else if (key == "--output-dir") {
      args.output_dir = next();
    } else if (key == "--no-verify") {
      args.verify = false;
    } else {
      throw std::runtime_error("unknown arg: " + key);
    }
  }
  if (args.bytes == 0 || args.chunk_bytes == 0 || args.chunk_count <= 0) {
    throw std::runtime_error("bytes/chunk-bytes/chunk-count must be positive");
  }
  return args;
}

double us_since_start(Clock::time_point t) {
  return std::chrono::duration<double, std::micro>(t - g_t0).count();
}

double elapsed_us(Clock::time_point start, Clock::time_point end) {
  return std::chrono::duration<double, std::micro>(end - start).count();
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

void record_stage(const std::string& selection,
                  const std::string& stage,
                  Clock::time_point start,
                  Clock::time_point end,
                  uint64_t bytes,
                  int32_t device,
                  uint64_t chunk_index = UINT64_MAX,
                  const std::string& detail = "") {
  g_events.push_back(StageEvent{
      selection,
      stage,
      chunk_index,
      bytes,
      us_since_start(start),
      elapsed_us(start, end),
      device,
      detail,
  });
}

class DeviceBuffer {
 public:
  explicit DeviceBuffer(uint64_t bytes) : bytes_(bytes) {
    UF_ACL_CHECK(aclrtMalloc(&ptr_, static_cast<size_t>(bytes_), ACL_MEM_MALLOC_HUGE_FIRST));
  }

  ~DeviceBuffer() {
    if (ptr_ != nullptr) {
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

 private:
  aclrtStream stream_ = nullptr;
  std::vector<aclrtEvent> events_;
};

class MemfdMapping {
 public:
  MemfdMapping(uint64_t bytes, const std::string& name) : bytes_(bytes) {
#ifdef SYS_memfd_create
    fd_ = static_cast<int>(syscall(SYS_memfd_create, name.c_str(), 0));
#else
    fd_ = -1;
#endif
    if (fd_ < 0) {
      throw std::runtime_error("memfd_create failed");
    }
    if (ftruncate(fd_, static_cast<off_t>(bytes_)) != 0) {
      throw std::runtime_error("ftruncate memfd failed");
    }
    ptr_ = mmap(nullptr, static_cast<size_t>(bytes_), PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    if (ptr_ == MAP_FAILED) {
      ptr_ = nullptr;
      throw std::runtime_error("mmap memfd failed");
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

 private:
  int fd_ = -1;
  void* ptr_ = nullptr;
  uint64_t bytes_ = 0;
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

class HostRegistration {
 public:
  HostRegistration(void* ptr, uint64_t bytes, double* register_us) : ptr_(ptr), bytes_(bytes) {
    void* device_ptr = nullptr;
    auto start = Clock::now();
    UF_ACL_CHECK(aclrtHostRegister(ptr_, bytes_, ACL_HOST_REGISTER_MAPPED, &device_ptr));
    auto end = Clock::now();
    if (register_us != nullptr) {
      *register_us = elapsed_us(start, end);
    }
  }

  ~HostRegistration() {
    if (ptr_ != nullptr) {
      (void)aclrtHostUnregister(ptr_);
    }
  }

  HostRegistration(const HostRegistration&) = delete;
  HostRegistration& operator=(const HostRegistration&) = delete;

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

 private:
  void* ptr_ = nullptr;
  uint64_t bytes_ = 0;
};

void fill_repeated_block(void* ptr, uint64_t bytes, uint32_t seed) {
  constexpr uint64_t block_bytes = 8ull * kMiB;
  std::vector<uint8_t> block(static_cast<size_t>(std::min<uint64_t>(block_bytes, bytes)));
  for (size_t i = 0; i < block.size(); ++i) {
    block[i] = static_cast<uint8_t>((static_cast<uint64_t>(i) * 1315423911ull + seed) & 0xffu);
  }
  auto* dst = static_cast<uint8_t*>(ptr);
  uint64_t offset = 0;
  while (offset < bytes) {
    uint64_t n = std::min<uint64_t>(block.size(), bytes - offset);
    std::memcpy(dst + offset, block.data(), static_cast<size_t>(n));
    offset += n;
  }
}

uint8_t expected_byte(uint64_t offset, uint32_t seed) {
  uint64_t in_block = offset % (8ull * kMiB);
  return static_cast<uint8_t>((in_block * 1315423911ull + seed) & 0xffu);
}

bool verify_device_samples(void* dev, uint64_t bytes, uint32_t seed, int32_t samples) {
  std::vector<uint8_t> actual(static_cast<size_t>(samples), 0);
  std::vector<uint64_t> offsets;
  offsets.push_back(0);
  offsets.push_back(std::min<uint64_t>(bytes - 1, 4096));
  for (int32_t i = 1; i <= samples - 3; ++i) {
    offsets.push_back((bytes / static_cast<uint64_t>(samples - 2)) * static_cast<uint64_t>(i));
  }
  offsets.push_back(bytes - 1);
  offsets.resize(static_cast<size_t>(samples));
  auto* base = static_cast<uint8_t*>(dev);
  for (size_t i = 0; i < offsets.size(); ++i) {
    UF_ACL_CHECK(aclrtMemcpy(&actual[i], 1, base + offsets[i], 1, ACL_MEMCPY_DEVICE_TO_HOST));
    if (actual[i] != expected_byte(offsets[i], seed)) {
      std::ostringstream oss;
      oss << "verify failed offset=" << offsets[i] << " actual=" << static_cast<int>(actual[i])
          << " expected=" << static_cast<int>(expected_byte(offsets[i], seed));
      throw std::runtime_error(oss.str());
    }
  }
  return true;
}

void ensure_dir(const std::string& path) {
  std::string cmd = "mkdir -p '" + path + "'";
  if (std::system(cmd.c_str()) != 0) {
    throw std::runtime_error("failed to create output dir: " + path);
  }
}

double gib_per_s(uint64_t bytes, double us) {
  if (us <= 0.0) {
    return 0.0;
  }
  return (static_cast<double>(bytes) / static_cast<double>(kGiB)) / (us / 1'000'000.0);
}

SelectionSummary run_memfd_pinned(const Args& args) {
  const std::string selection = "memfd_pinned_chunk_hbm";
  SelectionSummary summary;
  summary.selection = selection;
  summary.bytes = args.bytes;
  summary.chunk_bytes = args.chunk_bytes;
  summary.pinned_footprint_bytes = args.chunk_bytes * static_cast<uint64_t>(args.chunk_count);

  auto setup_start = Clock::now();
  DeviceBuffer dev(args.bytes);
  auto dev_ready = Clock::now();
  record_stage(selection, "setup.hbm_aclrtMalloc", setup_start, dev_ready, args.bytes, args.device);

  auto memfd_start = Clock::now();
  MemfdMapping memfd(args.bytes, "uflow_h2d_stage_memfd");
  auto memfd_ready = Clock::now();
  record_stage(selection, "setup.memfd_create_mmap", memfd_start, memfd_ready, args.bytes, args.device);

  auto fill_start = Clock::now();
  fill_repeated_block(memfd.ptr(), args.bytes, 0x51u);
  auto fill_end = Clock::now();
  record_stage(selection, "setup.memfd_fill", fill_start, fill_end, args.bytes, args.device);

  std::vector<PinnedHostBuffer*> chunks;
  chunks.reserve(static_cast<size_t>(args.chunk_count));
  auto pinned_start = Clock::now();
  try {
    for (int i = 0; i < args.chunk_count; ++i) {
      chunks.push_back(new PinnedHostBuffer(args.chunk_bytes));
    }
  } catch (...) {
    for (auto* ptr : chunks) {
      delete ptr;
    }
    throw;
  }
  auto pinned_end = Clock::now();
  record_stage(selection,
               "setup.aclrtMallocHost_chunks",
               pinned_start,
               pinned_end,
               summary.pinned_footprint_bytes,
               args.device);

  StreamEventPool se(args.chunk_count);
  auto transfer_start = Clock::now();
  struct Inflight {
    bool active = false;
    uint64_t chunk_index = 0;
    uint64_t offset = 0;
    uint64_t bytes = 0;
  };
  std::vector<Inflight> inflight(static_cast<size_t>(args.chunk_count));
  uint64_t offset = 0;
  uint64_t chunk_index = 0;
  auto* memfd_bytes = static_cast<uint8_t*>(memfd.ptr());
  auto* dev_bytes = static_cast<uint8_t*>(dev.ptr());
  while (offset < args.bytes) {
    const size_t slot = static_cast<size_t>(chunk_index % static_cast<uint64_t>(args.chunk_count));
    if (inflight[slot].active) {
      auto wait_start = Clock::now();
      UF_ACL_CHECK(aclrtSynchronizeEvent(se.event(slot)));
      auto wait_end = Clock::now();
      record_stage(selection,
                   "chunk.acl_wait.h2d",
                   wait_start,
                   wait_end,
                   inflight[slot].bytes,
                   args.device,
                   inflight[slot].chunk_index);
      summary.acl_wait_us += elapsed_us(wait_start, wait_end);
      inflight[slot].active = false;
    }
    const uint64_t n = std::min<uint64_t>(args.chunk_bytes, args.bytes - offset);
    auto copy_start = Clock::now();
    std::memcpy(chunks[slot]->ptr(), memfd_bytes + offset, static_cast<size_t>(n));
    auto copy_end = Clock::now();
    record_stage(selection, "chunk.cpu_copy.memfd_to_pinned", copy_start, copy_end, n, args.device, chunk_index);
    summary.cpu_copy_us += elapsed_us(copy_start, copy_end);

    auto submit_start = Clock::now();
    UF_ACL_CHECK(aclrtMemcpyAsync(dev_bytes + offset,
                                  static_cast<size_t>(n),
                                  chunks[slot]->ptr(),
                                  static_cast<size_t>(n),
                                  ACL_MEMCPY_HOST_TO_DEVICE,
                                  se.stream()));
    UF_ACL_CHECK(aclrtRecordEvent(se.event(slot), se.stream()));
    auto submit_end = Clock::now();
    record_stage(selection, "chunk.acl_submit.h2d", submit_start, submit_end, n, args.device, chunk_index);
    summary.acl_submit_us += elapsed_us(submit_start, submit_end);
    inflight[slot] = Inflight{true, chunk_index, offset, n};
    offset += n;
    ++chunk_index;
  }
  for (size_t slot = 0; slot < inflight.size(); ++slot) {
    if (!inflight[slot].active) {
      continue;
    }
    auto wait_start = Clock::now();
    UF_ACL_CHECK(aclrtSynchronizeEvent(se.event(slot)));
    auto wait_end = Clock::now();
    record_stage(selection,
                 "chunk.acl_wait.h2d",
                 wait_start,
                 wait_end,
                 inflight[slot].bytes,
                 args.device,
                 inflight[slot].chunk_index);
    summary.acl_wait_us += elapsed_us(wait_start, wait_end);
  }
  auto transfer_end = Clock::now();
  record_stage(selection, "transfer.wall", transfer_start, transfer_end, args.bytes, args.device);
  summary.wall_us = elapsed_us(transfer_start, transfer_end);
  summary.hot_us = summary.wall_us;
  summary.setup_us = elapsed_us(setup_start, transfer_start);
  summary.chunks = chunk_index;
  if (args.verify) {
    auto verify_start = Clock::now();
    summary.verified = verify_device_samples(dev.ptr(), args.bytes, 0x51u, 32);
    auto verify_end = Clock::now();
    record_stage(selection, "verify.sample_d2h", verify_start, verify_end, 32, args.device);
  }
  for (auto* ptr : chunks) {
    delete ptr;
  }
  return summary;
}

SelectionSummary run_memfd_registered(const Args& args) {
  const std::string selection = "memfd_registered_hbm";
  SelectionSummary summary;
  summary.selection = selection;
  summary.bytes = args.bytes;
  auto setup_start = Clock::now();
  DeviceBuffer dev(args.bytes);
  auto dev_ready = Clock::now();
  record_stage(selection, "setup.hbm_aclrtMalloc", setup_start, dev_ready, args.bytes, args.device);

  auto memfd_start = Clock::now();
  MemfdMapping memfd(args.bytes, "uflow_h2d_stage_registered_memfd");
  auto memfd_ready = Clock::now();
  record_stage(selection, "setup.memfd_create_mmap", memfd_start, memfd_ready, args.bytes, args.device);

  auto fill_start = Clock::now();
  fill_repeated_block(memfd.ptr(), args.bytes, 0x62u);
  auto fill_end = Clock::now();
  record_stage(selection, "setup.memfd_fill", fill_start, fill_end, args.bytes, args.device);

  double register_us = 0.0;
  auto register_stage_start = Clock::now();
  HostRegistration registration(memfd.ptr(), args.bytes, &register_us);
  auto register_stage_end = Clock::now();
  record_stage(selection, "setup.aclrtHostRegister_v1", register_stage_start, register_stage_end, args.bytes, args.device);
  summary.register_us = register_us;

  StreamEventPool se(1);
  auto* dev_bytes = static_cast<uint8_t*>(dev.ptr());
  auto transfer_start = Clock::now();
  auto submit_start = Clock::now();
  UF_ACL_CHECK(aclrtMemcpyAsync(dev_bytes,
                                static_cast<size_t>(args.bytes),
                                memfd.ptr(),
                                static_cast<size_t>(args.bytes),
                                ACL_MEMCPY_HOST_TO_DEVICE,
                                se.stream()));
  UF_ACL_CHECK(aclrtRecordEvent(se.event(0), se.stream()));
  auto submit_end = Clock::now();
  record_stage(selection, "direct.acl_submit.h2d", submit_start, submit_end, args.bytes, args.device);
  summary.acl_submit_us = elapsed_us(submit_start, submit_end);
  auto wait_start = Clock::now();
  UF_ACL_CHECK(aclrtSynchronizeEvent(se.event(0)));
  auto wait_end = Clock::now();
  record_stage(selection, "direct.acl_wait.h2d", wait_start, wait_end, args.bytes, args.device);
  summary.acl_wait_us = elapsed_us(wait_start, wait_end);
  auto transfer_end = Clock::now();
  record_stage(selection, "transfer.wall", transfer_start, transfer_end, args.bytes, args.device);
  summary.wall_us = elapsed_us(transfer_start, transfer_end);
  summary.hot_us = summary.wall_us;
  summary.setup_us = elapsed_us(setup_start, transfer_start);
  summary.chunks = 1;
  if (args.verify) {
    auto verify_start = Clock::now();
    summary.verified = verify_device_samples(dev.ptr(), args.bytes, 0x62u, 32);
    auto verify_end = Clock::now();
    record_stage(selection, "verify.sample_d2h", verify_start, verify_end, 32, args.device);
  }
  auto unregister_start = Clock::now();
  summary.unregister_us = registration.unregister_now();
  auto unregister_end = Clock::now();
  record_stage(selection, "cleanup.aclrtHostUnregister", unregister_start, unregister_end, args.bytes, args.device);
  return summary;
}

SelectionSummary run_host_dma(const Args& args) {
  const std::string selection = "acl_malloc_host_dma_hbm";
  SelectionSummary summary;
  summary.selection = selection;
  summary.bytes = args.bytes;

  auto setup_start = Clock::now();
  DeviceBuffer dev(args.bytes);
  auto dev_ready = Clock::now();
  record_stage(selection, "setup.hbm_aclrtMalloc", setup_start, dev_ready, args.bytes, args.device);

  auto host_start = Clock::now();
  PinnedHostBuffer host(args.bytes);
  auto host_ready = Clock::now();
  record_stage(selection, "setup.aclrtMallocHost_full", host_start, host_ready, args.bytes, args.device);
  summary.pinned_footprint_bytes = args.bytes;

  auto fill_start = Clock::now();
  fill_repeated_block(host.ptr(), args.bytes, 0x73u);
  auto fill_end = Clock::now();
  record_stage(selection, "setup.host_fill", fill_start, fill_end, args.bytes, args.device);

  StreamEventPool se(1);
  auto* dev_bytes = static_cast<uint8_t*>(dev.ptr());
  auto transfer_start = Clock::now();
  auto submit_start = Clock::now();
  UF_ACL_CHECK(aclrtMemcpyAsync(dev_bytes,
                                static_cast<size_t>(args.bytes),
                                host.ptr(),
                                static_cast<size_t>(args.bytes),
                                ACL_MEMCPY_HOST_TO_DEVICE,
                                se.stream()));
  UF_ACL_CHECK(aclrtRecordEvent(se.event(0), se.stream()));
  auto submit_end = Clock::now();
  record_stage(selection, "direct.acl_submit.h2d", submit_start, submit_end, args.bytes, args.device);
  summary.acl_submit_us = elapsed_us(submit_start, submit_end);
  auto wait_start = Clock::now();
  UF_ACL_CHECK(aclrtSynchronizeEvent(se.event(0)));
  auto wait_end = Clock::now();
  record_stage(selection, "direct.acl_wait.h2d", wait_start, wait_end, args.bytes, args.device);
  summary.acl_wait_us = elapsed_us(wait_start, wait_end);
  auto transfer_end = Clock::now();
  record_stage(selection, "transfer.wall", transfer_start, transfer_end, args.bytes, args.device);
  summary.wall_us = elapsed_us(transfer_start, transfer_end);
  summary.hot_us = summary.wall_us;
  summary.setup_us = elapsed_us(setup_start, transfer_start);
  summary.chunks = 1;
  if (args.verify) {
    auto verify_start = Clock::now();
    summary.verified = verify_device_samples(dev.ptr(), args.bytes, 0x73u, 32);
    auto verify_end = Clock::now();
    record_stage(selection, "verify.sample_d2h", verify_start, verify_end, 32, args.device);
  }
  return summary;
}

void write_summary_csv(const std::string& output_dir, const std::vector<SelectionSummary>& summaries) {
  std::ofstream out(output_dir + "/h2d_stage_summary.csv");
  out << "selection,status,error,bytes,gib,wall_us,hot_us,setup_us,cpu_copy_us,acl_submit_us,acl_wait_us,register_us,unregister_us,chunks,chunk_bytes,pinned_footprint_bytes,hot_gib_s,verified\n";
  out << std::fixed << std::setprecision(6);
  for (const auto& s : summaries) {
    out << s.selection << ","
        << s.status << ","
        << "\"" << json_escape(s.error) << "\","
        << s.bytes << ","
        << (static_cast<double>(s.bytes) / static_cast<double>(kGiB)) << ","
        << s.wall_us << ","
        << s.hot_us << ","
        << s.setup_us << ","
        << s.cpu_copy_us << ","
        << s.acl_submit_us << ","
        << s.acl_wait_us << ","
        << s.register_us << ","
        << s.unregister_us << ","
        << s.chunks << ","
        << s.chunk_bytes << ","
        << s.pinned_footprint_bytes << ","
        << gib_per_s(s.bytes, s.hot_us) << ","
        << (s.verified ? "true" : "false") << "\n";
  }
}

void write_summary_json(const std::string& output_dir, const Args& args, const std::vector<SelectionSummary>& summaries) {
  std::ofstream out(output_dir + "/h2d_stage_summary.json");
  out << std::fixed << std::setprecision(6);
  out << "{\n"
      << "  \"title\": \"UFlow PhaseE-06 native H2D per-stage timestamp\",\n"
      << "  \"direction\": \"h2d\",\n"
      << "  \"device_id\": " << args.device << ",\n"
      << "  \"bytes\": " << args.bytes << ",\n"
      << "  \"gib\": " << (static_cast<double>(args.bytes) / static_cast<double>(kGiB)) << ",\n"
      << "  \"chunk_bytes\": " << args.chunk_bytes << ",\n"
      << "  \"chunk_count\": " << args.chunk_count << ",\n"
      << "  \"results\": [\n";
  for (size_t i = 0; i < summaries.size(); ++i) {
    const auto& s = summaries[i];
    out << "    {"
        << "\"selection\":\"" << json_escape(s.selection) << "\","
        << "\"status\":\"" << json_escape(s.status) << "\","
        << "\"error\":\"" << json_escape(s.error) << "\","
        << "\"bytes\":" << s.bytes << ","
        << "\"hot_gib_s\":" << gib_per_s(s.bytes, s.hot_us) << ","
        << "\"wall_ms\":" << (s.wall_us / 1000.0) << ","
        << "\"setup_ms\":" << (s.setup_us / 1000.0) << ","
        << "\"cpu_copy_ms\":" << (s.cpu_copy_us / 1000.0) << ","
        << "\"acl_submit_ms\":" << (s.acl_submit_us / 1000.0) << ","
        << "\"acl_wait_ms\":" << (s.acl_wait_us / 1000.0) << ","
        << "\"register_ms\":" << (s.register_us / 1000.0) << ","
        << "\"unregister_ms\":" << (s.unregister_us / 1000.0) << ","
        << "\"chunks\":" << s.chunks << ","
        << "\"chunk_bytes\":" << s.chunk_bytes << ","
        << "\"pinned_footprint_bytes\":" << s.pinned_footprint_bytes << ","
        << "\"verified\":" << (s.verified ? "true" : "false")
        << "}";
    if (i + 1 != summaries.size()) {
      out << ",";
    }
    out << "\n";
  }
  out << "  ]\n"
      << "}\n";
}

int tid_for_stage(const StageEvent& event) {
  int base = 100;
  if (event.selection == "memfd_pinned_chunk_hbm") {
    base = 100;
  } else if (event.selection == "memfd_registered_hbm") {
    base = 200;
  } else if (event.selection == "acl_malloc_host_dma_hbm") {
    base = 300;
  }
  if (event.stage.find("setup") == 0) {
    return base + 1;
  }
  if (event.stage.find("chunk.cpu") == 0) {
    return base + 2;
  }
  if (event.stage.find("acl_submit") != std::string::npos) {
    return base + 3;
  }
  if (event.stage.find("acl_wait") != std::string::npos) {
    return base + 4;
  }
  if (event.stage.find("transfer.wall") == 0) {
    return base;
  }
  return base + 9;
}

void write_trace_json(const std::string& output_dir) {
  std::ofstream out(output_dir + "/h2d_stage_trace.json");
  out << std::fixed << std::setprecision(3);
  out << "{\n"
      << "  \"displayTimeUnit\": \"ms\",\n"
      << "  \"metadata\": {\"title\": \"UFlow PhaseE-06 native H2D per-stage timestamp\"},\n"
      << "  \"traceEvents\": [\n";
  bool first = true;
  auto write_event = [&](const std::string& raw) {
    if (!first) {
      out << ",\n";
    }
    first = false;
    out << raw;
  };
  const std::vector<std::pair<int, std::string>> tids = {
      {100, "memfd_pinned transfer wall"},
      {101, "memfd_pinned setup"},
      {102, "memfd_pinned CPU copy"},
      {103, "memfd_pinned ACL submit"},
      {104, "memfd_pinned ACL wait"},
      {200, "registered_memfd transfer wall"},
      {201, "registered_memfd setup/register"},
      {203, "registered_memfd ACL submit"},
      {204, "registered_memfd ACL wait"},
      {300, "acl_malloc_host transfer wall"},
      {301, "acl_malloc_host setup"},
      {303, "acl_malloc_host ACL submit"},
      {304, "acl_malloc_host ACL wait"},
  };
  for (const auto& [tid, name] : tids) {
    std::ostringstream event;
    event << "    {\"name\":\"thread_name\",\"ph\":\"M\",\"pid\":1,\"tid\":" << tid
          << ",\"args\":{\"name\":\"" << json_escape(name) << "\"}}";
    write_event(event.str());
  }
  for (const auto& e : g_events) {
    std::ostringstream event;
    event << "    {"
          << "\"name\":\"" << json_escape(e.stage) << "\","
          << "\"cat\":\"uflow.h2d.stage\","
          << "\"ph\":\"X\","
          << "\"pid\":1,"
          << "\"tid\":" << tid_for_stage(e) << ","
          << "\"ts\":" << e.ts_us << ","
          << "\"dur\":" << std::max(1.0, e.dur_us) << ","
          << "\"args\":{"
          << "\"selection\":\"" << json_escape(e.selection) << "\","
          << "\"bytes\":" << e.bytes << ","
          << "\"device_id\":" << e.device;
    if (e.chunk_index != UINT64_MAX) {
      event << ",\"chunk_index\":" << e.chunk_index;
    }
    event << "}}";
    write_event(event.str());
  }
  out << "\n  ]\n}\n";
}

void write_summary_md(const std::string& output_dir, const Args& args, const std::vector<SelectionSummary>& summaries) {
  std::ofstream out(output_dir + "/summary.md");
  out << "# PhaseE-06 H2D Native Per-Stage Timestamp\n\n"
      << "- device: NPU" << args.device << "\n"
      << "- bytes: " << args.bytes << " (" << (static_cast<double>(args.bytes) / kGiB) << " GiB)\n"
      << "- chunk: " << args.chunk_bytes << " bytes x " << args.chunk_count << "\n"
      << "- finest trace: `h2d_stage_trace.json`\n\n"
      << "| selection | status | hot GiB/s | wall ms | setup ms | CPU copy ms | ACL submit ms | ACL wait ms | register ms | unregister ms | chunks | verified |\n"
      << "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n"
      << std::fixed << std::setprecision(3);
  for (const auto& s : summaries) {
    out << "| `" << s.selection << "` | " << s.status << " | "
        << gib_per_s(s.bytes, s.hot_us) << " | "
        << (s.wall_us / 1000.0) << " | "
        << (s.setup_us / 1000.0) << " | "
        << (s.cpu_copy_us / 1000.0) << " | "
        << (s.acl_submit_us / 1000.0) << " | "
        << (s.acl_wait_us / 1000.0) << " | "
        << (s.register_us / 1000.0) << " | "
        << (s.unregister_us / 1000.0) << " | "
        << s.chunks << " | "
        << (s.verified ? "true" : "false") << " |\n";
  }
}

}  // namespace

int main(int argc, char** argv) {
  try {
    Args args = parse_args(argc, argv);
    ensure_dir(args.output_dir);
    g_t0 = Clock::now();
    AclRuntime rt(args.device);

    std::vector<SelectionSummary> summaries;
    auto run_selection = [&](const std::string& name, auto&& fn) {
      try {
        summaries.push_back(fn(args));
      } catch (const std::exception& e) {
        SelectionSummary summary;
        summary.selection = name;
        summary.status = "error";
        summary.error = e.what();
        summary.bytes = args.bytes;
        summaries.push_back(summary);
        std::cerr << "UFLOW_E06_H2D_STAGE_ERROR selection=" << name << " error=" << e.what() << std::endl;
      }
    };
    run_selection("memfd_pinned_chunk_hbm", run_memfd_pinned);
    run_selection("memfd_registered_hbm", run_memfd_registered);
    run_selection("acl_malloc_host_dma_hbm", run_host_dma);

    write_summary_csv(args.output_dir, summaries);
    write_summary_json(args.output_dir, args, summaries);
    write_trace_json(args.output_dir);
    write_summary_md(args.output_dir, args, summaries);

    for (const auto& s : summaries) {
      std::cout << std::fixed << std::setprecision(6)
                << "UFLOW_E06_H2D_STAGE_RESULT "
                << "selection=" << s.selection
                << " status=" << s.status
                << " bytes=" << s.bytes
                << " wall_us=" << s.wall_us
                << " setup_us=" << s.setup_us
                << " cpu_copy_us=" << s.cpu_copy_us
                << " acl_submit_us=" << s.acl_submit_us
                << " acl_wait_us=" << s.acl_wait_us
                << " register_us=" << s.register_us
                << " unregister_us=" << s.unregister_us
                << " chunks=" << s.chunks
                << " hot_gib_s=" << gib_per_s(s.bytes, s.hot_us)
                << " verified=" << (s.verified ? "true" : "false")
                << " error=\"" << s.error << "\""
                << std::endl;
    }
    std::cout << "UFLOW_E06_H2D_STAGE_OUTPUT " << args.output_dir << std::endl;
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "acl_h2d_stage_breakdown error: " << e.what() << std::endl;
    return 1;
  }
}
