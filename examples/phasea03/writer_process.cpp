#include "acl_common.hpp"
#include "uf_acl.h"
#include "ipc.hpp"

#include <unistd.h>

#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

using namespace uf::phasea;

namespace {

struct Args {
  int32_t device = 0;
  uint64_t bytes = 1ull << 20;
  std::string socket = "/tmp/uf_phasea03.sock";
  std::string object_file = "/tmp/uf_phasea03_object_id";
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
    } else if (key == "--bytes") {
      args.bytes = static_cast<uint64_t>(std::stoull(next()));
    } else if (key == "--socket") {
      args.socket = next();
    } else if (key == "--object-file") {
      args.object_file = next();
    } else {
      throw std::runtime_error("unknown arg: " + key);
    }
  }
  if (args.bytes == 0 || (args.bytes % sizeof(uint32_t)) != 0) {
    throw std::runtime_error("--bytes must be a non-zero multiple of 4");
  }
  return args;
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
    UfAclStatus st{};
    check_uf(uf_acl_client_init(args.device, &st), st, "uf_acl_client_init");
    int64_t bare_tgid = -1;
    check_uf(uf_acl_client_get_bare_tgid(&bare_tgid, &st), st, "uf_acl_client_get_bare_tgid");

    fd = connect_socket_retry(args.socket, 200, 50);
    auto reg = request(fd,
                       {{"op", "RegisterClient"},
                        {"role", "writer"},
                        {"device_id", std::to_string(args.device)},
                        {"os_pid", std::to_string(static_cast<int64_t>(::getpid()))},
                        {"bare_tgid", std::to_string(bare_tgid)}});
    uint64_t client_id = kv_get_u64(reg, "client_id");
    auto created = request(fd,
                           {{"op", "CreateBuffer"},
                            {"client_id", std::to_string(client_id)},
                            {"size_bytes", std::to_string(args.bytes)},
                            {"object_type", "test_buffer"},
                            {"access", "read_write"}});
    uint64_t object_id = kv_get_u64(created, "object_id");
    uint64_t lease_id = kv_get_u64(created, "lease_id");
    uint64_t actual_bytes = kv_get_u64(created, "actual_bytes");
    uint64_t shareable = kv_get_u64(created, "shareable");

    {
      std::ofstream f(args.object_file);
      f << object_id << "\n";
    }

    UfAclClientImportRequest import_req{};
    import_req.device_id = args.device;
    import_req.shareable_handle_payload[0] = shareable;
    import_req.shareable_handle_bytes = sizeof(uint64_t);
    import_req.actual_bytes = actual_bytes;
    check_uf(uf_acl_import_and_map(&import_req, &mapped, &st), st, "uf_acl_import_and_map");
    mapped_ok = true;

    const size_t words = args.bytes / sizeof(uint32_t);
    auto original = make_pattern(words, 0x10101010u);
    check_uf(uf_acl_h2d(mapped.device_ptr, 0, original.data(), args.bytes, &st), st, "uf_acl_h2d");
    request(fd,
            {{"op", "MarkReady"},
             {"client_id", std::to_string(client_id)},
             {"object_id", std::to_string(object_id)},
             {"lease_id", std::to_string(lease_id)}});

    auto modified = request(fd,
                            {{"op", "WaitObjectEvent"},
                             {"client_id", std::to_string(client_id)},
                             {"object_id", std::to_string(object_id)},
                             {"event", "Modified"},
                             {"timeout_ms", "30000"}});
    uint64_t modified_offset = kv_get_u64(modified, "modified_offset_bytes");
    uint64_t modified_bytes = kv_get_u64(modified, "modified_bytes");

    std::vector<uint32_t> final(words, 0);
    check_uf(uf_acl_d2h(final.data(), mapped.device_ptr, 0, args.bytes, &st), st, "uf_acl_d2h");
    auto expected = original;
    uint64_t overlay_offset_words = modified_offset / sizeof(uint32_t);
    uint64_t overlay_words = modified_bytes / sizeof(uint32_t);
    auto overlay = make_overlay(static_cast<size_t>(overlay_words), 0x20202020u);
    for (size_t i = 0; i < overlay.size(); ++i) {
      expected[static_cast<size_t>(overlay_offset_words) + i] = overlay[i];
    }
    auto cmp = compare_words(expected, final);

    request(fd,
            {{"op", "CloseLease"},
             {"client_id", std::to_string(client_id)},
             {"lease_id", std::to_string(lease_id)}});
    request(fd,
            {{"op", "ReleaseBuffer"},
             {"client_id", std::to_string(client_id)},
             {"object_id", std::to_string(object_id)}});

    check_uf(uf_acl_unmap_and_release(&mapped, &st), st, "uf_acl_unmap_and_release");
    mapped_ok = false;
    check_uf(uf_acl_client_finalize(&st), st, "uf_acl_client_finalize");
    close_fd(fd);

    print_json_summary("phasea03_writer_overlay",
                       "writer",
                       args.device,
                       args.bytes,
                       actual_bytes,
                       checksum_words(expected),
                       checksum_words(final),
                       cmp.ok ? "pass" : "fail",
                       "object_id=" + std::to_string(object_id));
    return cmp.ok ? 0 : 2;
  } catch (const std::exception& e) {
    std::cerr << "phasea03_writer_process error: " << e.what() << std::endl;
    UfAclStatus st{};
    if (mapped_ok) {
      (void)uf_acl_unmap_and_release(&mapped, &st);
    }
    (void)uf_acl_client_finalize(&st);
    close_fd(fd);
    return 1;
  }
}

