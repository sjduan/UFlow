#include "acl_common.hpp"
#include "ipc.hpp"

#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

using namespace uf::phasea;

namespace {

struct Args {
  int32_t device = 0;
  std::string socket = "/tmp/uf_acl_hbm_share.sock";
  uint64_t overlay_offset_elements = 1024;
  uint64_t overlay_count_elements = 4096;
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
    } else if (key == "--socket") {
      args.socket = next();
    } else if (key == "--overlay-offset-elements") {
      args.overlay_offset_elements = static_cast<uint64_t>(std::stoull(next()));
    } else if (key == "--overlay-count-elements") {
      args.overlay_count_elements = static_cast<uint64_t>(std::stoull(next()));
    } else {
      throw std::runtime_error("unknown arg: " + key);
    }
  }
  if (args.overlay_count_elements == 0) {
    throw std::runtime_error("--overlay-count-elements must be non-zero");
  }
  return args;
}

}  // namespace

int main(int argc, char** argv) {
  int fd = -1;
  PhysicalAllocation imported;
  try {
    Args args = parse_args(argc, argv);
    AclRuntime rt(args.device);
    int32_t bare_tgid = get_bare_tgid();
    fd = connect_socket_retry(args.socket, 200, 50);
    send_line(fd,
              kv_encode({{"op", "REGISTER"},
                         {"bare_tgid", std::to_string(bare_tgid)},
                         {"overlay_offset_elements", std::to_string(args.overlay_offset_elements)},
                         {"overlay_count_elements", std::to_string(args.overlay_count_elements)}}));
    auto handle_msg = kv_decode(recv_line(fd));
    if (kv_get(handle_msg, "op") == "ERROR") {
      throw std::runtime_error("exporter rejected request: " + kv_get(handle_msg, "detail"));
    }
    if (kv_get(handle_msg, "op") != "HANDLE") {
      throw std::runtime_error("expected HANDLE from exporter");
    }
    uint64_t shareable = kv_get_u64(handle_msg, "shareable");
    uint64_t actual_bytes = kv_get_u64(handle_msg, "actual_bytes");
    uint64_t element_count = kv_get_u64(handle_msg, "element_count");
    uint64_t overlay_offset = kv_get_u64(handle_msg, "overlay_offset_elements");
    uint64_t overlay_count = kv_get_u64(handle_msg, "overlay_count_elements");
    if (overlay_offset + overlay_count > element_count) {
      throw std::runtime_error("overlay range out of bounds after HANDLE");
    }
    imported = import_physical_mapped(args.device, shareable, actual_bytes);
    auto overlay = make_overlay(static_cast<size_t>(overlay_count), 0x33334444u);
    uint64_t offset_bytes = overlay_offset * sizeof(uint32_t);
    uint64_t overlay_bytes = overlay_count * sizeof(uint32_t);
    copy_h2d(static_cast<char*>(imported.ptr) + offset_bytes, overlay.data(), overlay_bytes);
    cleanup_physical(imported);
    send_line(fd, kv_encode({{"op", "DONE"}, {"status", "ok"}}));
    close_fd(fd);
    print_json_summary("phasea02_shareable_overlay",
                       "importer",
                       args.device,
                       overlay_bytes,
                       actual_bytes,
                       checksum_words(overlay),
                       checksum_words(overlay),
                       "pass",
                       "bare_tgid=" + std::to_string(bare_tgid));
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "acl_hbm_share_importer error: " << e.what() << std::endl;
    cleanup_physical(imported);
    if (fd >= 0) {
      try {
        send_line(fd, kv_encode({{"op", "DONE"}, {"status", "error"}, {"detail", e.what()}}));
      } catch (...) {
      }
    }
    close_fd(fd);
    return 1;
  }
}

