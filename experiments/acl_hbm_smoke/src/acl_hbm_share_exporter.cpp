#include "acl_common.hpp"
#include "ipc.hpp"

#include <unistd.h>

#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

using namespace uf::phasea;

namespace {

struct Args {
  int32_t device = 0;
  uint64_t bytes = 1ull << 20;
  std::string socket = "/tmp/uf_acl_hbm_share.sock";
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
    if (key == "--device") {
      args.device = static_cast<int32_t>(std::stoi(next()));
    } else if (key == "--bytes") {
      args.bytes = static_cast<uint64_t>(std::stoull(next()));
    } else if (key == "--socket") {
      args.socket = next();
    } else {
      throw std::runtime_error("unknown arg: " + key);
    }
  }
  if (args.bytes == 0 || (args.bytes % sizeof(uint32_t)) != 0) {
    throw std::runtime_error("--bytes must be a non-zero multiple of 4");
  }
  return args;
}

}  // namespace

int main(int argc, char** argv) {
  int server_fd = -1;
  int client_fd = -1;
  PhysicalAllocation alloc;
  try {
    Args args = parse_args(argc, argv);
    AclRuntime rt(args.device);
    const size_t words = args.bytes / sizeof(uint32_t);
    auto original = make_pattern(words, 0x11112222u);
    std::vector<uint32_t> final(words, 0);

    alloc = allocate_physical_mapped(args.device, args.bytes);
    export_shareable(alloc);
    server_fd = create_server_socket(args.socket);
    std::cerr << "exporter_ready socket=" << args.socket << " shareable=" << alloc.shareable_handle
              << " actual_bytes=" << alloc.actual_bytes << std::endl;

    client_fd = accept_one(server_fd);
    auto reg = kv_decode(recv_line(client_fd));
    if (kv_get(reg, "op") != "REGISTER") {
      throw std::runtime_error("expected REGISTER from importer");
    }
    int32_t importer_tgid = static_cast<int32_t>(kv_get_i64(reg, "bare_tgid", -1));
    uint64_t overlay_offset = kv_get_u64(reg, "overlay_offset_elements", 0);
    uint64_t overlay_count = kv_get_u64(reg, "overlay_count_elements", 0);
    if (importer_tgid <= 0) {
      throw std::runtime_error("invalid importer bare_tgid");
    }
    if (overlay_offset + overlay_count > words) {
      send_line(client_fd, kv_encode({{"op", "ERROR"}, {"detail", "overlay_out_of_range"}}));
      throw std::runtime_error("overlay range out of bounds");
    }

    set_shareable_pid(alloc.shareable_handle, importer_tgid);
    copy_h2d(alloc.ptr, original.data(), args.bytes);

    send_line(client_fd,
              kv_encode({{"op", "HANDLE"},
                         {"device_id", std::to_string(args.device)},
                         {"shareable", std::to_string(alloc.shareable_handle)},
                         {"requested_bytes", std::to_string(args.bytes)},
                         {"actual_bytes", std::to_string(alloc.actual_bytes)},
                         {"element_count", std::to_string(words)},
                         {"overlay_offset_elements", std::to_string(overlay_offset)},
                         {"overlay_count_elements", std::to_string(overlay_count)}}));

    auto done = kv_decode(recv_line(client_fd));
    if (kv_get(done, "op") != "DONE" || kv_get(done, "status") != "ok") {
      throw std::runtime_error("importer did not report successful overlay");
    }

    copy_d2h(final.data(), alloc.ptr, args.bytes);
    auto expected = original;
    auto overlay = make_overlay(static_cast<size_t>(overlay_count), 0x33334444u);
    for (size_t i = 0; i < overlay.size(); ++i) {
      expected[static_cast<size_t>(overlay_offset) + i] = overlay[i];
    }
    auto cmp = compare_words(expected, final);
    std::string detail;
    if (!cmp.ok) {
      detail = "first_mismatch=" + std::to_string(cmp.first_mismatch);
    }
    detail += " overlay_offset_elements=" + std::to_string(overlay_offset);
    detail += " overlay_count_elements=" + std::to_string(overlay_count);
    print_json_summary("phasea02_shareable_overlay",
                       "exporter",
                       args.device,
                       args.bytes,
                       alloc.actual_bytes,
                       checksum_words(expected),
                       checksum_words(final),
                       cmp.ok ? "pass" : "fail",
                       detail);
    close_fd(client_fd);
    close_fd(server_fd);
    unlink_socket(args.socket);
    cleanup_physical(alloc);
    if (!cmp.ok) {
      return 2;
    }
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "acl_hbm_share_exporter error: " << e.what() << std::endl;
    close_fd(client_fd);
    close_fd(server_fd);
    cleanup_physical(alloc);
    return 1;
  }
}

