#include "acl_common.hpp"
#include "uf_acl.h"
#include "ipc.hpp"

#include <unistd.h>

#include <chrono>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace uf::phasea;

namespace {

struct Args {
  int32_t device = 0;
  std::string socket = "/tmp/uf_phasea03.sock";
  std::string object_file = "/tmp/uf_phasea03_object_id";
  uint64_t object_id = 0;
  uint64_t overlay_offset_elements = 1024;
  uint64_t overlay_count_elements = 4096;
  bool skip_close_lease = false;
};

void check_uf(int rc, const UfAclStatus& st, const std::string& what) {
  if (rc != 0) {
    throw std::runtime_error(what + " failed: " + st.message);
  }
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
    } else if (key == "--socket") {
      args.socket = next();
    } else if (key == "--object-file") {
      args.object_file = next();
    } else if (key == "--object-id") {
      args.object_id = static_cast<uint64_t>(std::stoull(next()));
    } else if (key == "--overlay-offset-elements") {
      args.overlay_offset_elements = static_cast<uint64_t>(std::stoull(next()));
    } else if (key == "--overlay-count-elements") {
      args.overlay_count_elements = static_cast<uint64_t>(std::stoull(next()));
    } else if (key == "--skip-close-lease") {
      args.skip_close_lease = true;
    } else {
      throw std::runtime_error("unknown arg: " + key);
    }
  }
  return args;
}

uint64_t wait_object_id(const Args& args) {
  if (args.object_id != 0) {
    return args.object_id;
  }
  for (int i = 0; i < 600; ++i) {
    std::ifstream f(args.object_file);
    uint64_t id = 0;
    if (f >> id) {
      return id;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  throw std::runtime_error("timed out waiting for object file " + args.object_file);
}

Kv request(int fd, const Kv& kv) {
  send_line(fd, kv_encode(kv));
  auto resp = kv_decode(recv_line(fd));
  if (kv_get(resp, "status") != "ok") {
    throw std::runtime_error("daemon request failed: " + kv_get(resp, "detail", "unknown"));
  }
  return resp;
}

}  // namespace

int main(int argc, char** argv) {
  UfAclMappedMemory mapped{};
  bool mapped_ok = false;
  int fd = -1;
  try {
    Args args = parse_args(argc, argv);
    uint64_t object_id = wait_object_id(args);
    UfAclStatus st{};
    check_uf(uf_acl_client_init(args.device, &st), st, "uf_acl_client_init");
    int64_t bare_tgid = -1;
    check_uf(uf_acl_client_get_bare_tgid(&bare_tgid, &st), st, "uf_acl_client_get_bare_tgid");

    fd = connect_socket_retry(args.socket, 200, 50);
    auto reg = request(fd,
                       {{"op", "RegisterClient"},
                        {"role", "overlay"},
                        {"device_id", std::to_string(args.device)},
                        {"os_pid", std::to_string(static_cast<int64_t>(::getpid()))},
                        {"bare_tgid", std::to_string(bare_tgid)}});
    uint64_t client_id = kv_get_u64(reg, "client_id");
    uint64_t offset_bytes = args.overlay_offset_elements * sizeof(uint32_t);
    uint64_t overlay_bytes = args.overlay_count_elements * sizeof(uint32_t);
    auto opened = request(fd,
                          {{"op", "OpenBuffer"},
                           {"client_id", std::to_string(client_id)},
                           {"object_id", std::to_string(object_id)},
                           {"access", "read_write"},
                           {"allowed_offset_bytes", std::to_string(offset_bytes)},
                           {"allowed_bytes", std::to_string(overlay_bytes)}});
    uint64_t lease_id = kv_get_u64(opened, "lease_id");
    uint64_t actual_bytes = kv_get_u64(opened, "actual_bytes");
    uint64_t shareable = kv_get_u64(opened, "shareable");

    UfAclClientImportRequest import_req{};
    import_req.device_id = args.device;
    import_req.shareable_handle_payload[0] = shareable;
    import_req.shareable_handle_bytes = sizeof(uint64_t);
    import_req.actual_bytes = actual_bytes;
    check_uf(uf_acl_import_and_map(&import_req, &mapped, &st), st, "uf_acl_import_and_map");
    mapped_ok = true;
    auto overlay = make_overlay(static_cast<size_t>(args.overlay_count_elements), 0x20202020u);
    check_uf(uf_acl_h2d(mapped.device_ptr, offset_bytes, overlay.data(), overlay_bytes, &st), st, "uf_acl_h2d");
    request(fd,
            {{"op", "MarkModified"},
             {"client_id", std::to_string(client_id)},
             {"object_id", std::to_string(object_id)},
             {"lease_id", std::to_string(lease_id)},
             {"modified_offset_bytes", std::to_string(offset_bytes)},
             {"modified_bytes", std::to_string(overlay_bytes)}});
    if (!args.skip_close_lease) {
      request(fd,
              {{"op", "CloseLease"},
               {"client_id", std::to_string(client_id)},
               {"lease_id", std::to_string(lease_id)}});
    }
    check_uf(uf_acl_unmap_and_release(&mapped, &st), st, "uf_acl_unmap_and_release");
    mapped_ok = false;
    check_uf(uf_acl_client_finalize(&st), st, "uf_acl_client_finalize");
    close_fd(fd);
    print_json_summary("phasea03_writer_overlay",
                       "overlay",
                       args.device,
                       overlay_bytes,
                       actual_bytes,
                       checksum_words(overlay),
                       checksum_words(overlay),
                       "pass",
                       "object_id=" + std::to_string(object_id));
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "phasea03_overlay_process error: " << e.what() << std::endl;
    UfAclStatus st{};
    if (mapped_ok) {
      (void)uf_acl_unmap_and_release(&mapped, &st);
    }
    (void)uf_acl_client_finalize(&st);
    close_fd(fd);
    return 1;
  }
}
