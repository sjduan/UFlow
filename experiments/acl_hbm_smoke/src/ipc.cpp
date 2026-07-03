#include "ipc.hpp"

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <stdexcept>
#include <thread>

namespace uf::phasea {

std::string kv_encode(const Kv& kv) {
  std::string out;
  bool first = true;
  for (const auto& [k, v] : kv) {
    if (!first) {
      out.push_back(';');
    }
    first = false;
    out += k;
    out.push_back('=');
    out += v;
  }
  out.push_back('\n');
  return out;
}

Kv kv_decode(const std::string& line) {
  Kv out;
  size_t start = 0;
  while (start < line.size()) {
    size_t end = line.find(';', start);
    if (end == std::string::npos) {
      end = line.size();
    }
    std::string part = line.substr(start, end - start);
    if (!part.empty() && part.back() == '\n') {
      part.pop_back();
    }
    size_t eq = part.find('=');
    if (eq != std::string::npos) {
      out[part.substr(0, eq)] = part.substr(eq + 1);
    }
    start = end + 1;
  }
  return out;
}

std::string kv_get(const Kv& kv, const std::string& key, const std::string& fallback) {
  auto it = kv.find(key);
  return it == kv.end() ? fallback : it->second;
}

uint64_t kv_get_u64(const Kv& kv, const std::string& key, uint64_t fallback) {
  auto it = kv.find(key);
  return it == kv.end() ? fallback : static_cast<uint64_t>(std::stoull(it->second));
}

int64_t kv_get_i64(const Kv& kv, const std::string& key, int64_t fallback) {
  auto it = kv.find(key);
  return it == kv.end() ? fallback : static_cast<int64_t>(std::stoll(it->second));
}

static sockaddr_un make_addr(const std::string& path) {
  if (path.size() >= sizeof(sockaddr_un::sun_path)) {
    throw std::runtime_error("unix socket path too long: " + path);
  }
  sockaddr_un addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
  return addr;
}

void unlink_socket(const std::string& path) {
  ::unlink(path.c_str());
}

int create_server_socket(const std::string& path) {
  unlink_socket(path);
  int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) {
    throw std::runtime_error("socket failed");
  }
  auto addr = make_addr(path);
  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    ::close(fd);
    throw std::runtime_error("bind failed for " + path);
  }
  if (::listen(fd, 8) != 0) {
    ::close(fd);
    throw std::runtime_error("listen failed");
  }
  return fd;
}

int accept_one(int server_fd) {
  int fd = ::accept(server_fd, nullptr, nullptr);
  if (fd < 0) {
    throw std::runtime_error("accept failed");
  }
  return fd;
}

int connect_socket_retry(const std::string& path, int retries, int sleep_ms) {
  auto addr = make_addr(path);
  for (int i = 0; i <= retries; ++i) {
    int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
      throw std::runtime_error("socket failed");
    }
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
      return fd;
    }
    ::close(fd);
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
  }
  throw std::runtime_error("connect failed for " + path);
}

void send_line(int fd, const std::string& line) {
  const char* p = line.data();
  size_t left = line.size();
  while (left > 0) {
    ssize_t n = ::send(fd, p, left, 0);
    if (n <= 0) {
      throw std::runtime_error("send failed");
    }
    p += n;
    left -= static_cast<size_t>(n);
  }
}

std::string recv_line(int fd) {
  std::string out;
  char ch = 0;
  while (true) {
    ssize_t n = ::recv(fd, &ch, 1, 0);
    if (n <= 0) {
      throw std::runtime_error("recv failed or connection closed");
    }
    out.push_back(ch);
    if (ch == '\n') {
      return out;
    }
  }
}

void close_fd(int fd) {
  if (fd >= 0) {
    ::close(fd);
  }
}

}  // namespace uf::phasea

