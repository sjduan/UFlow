#pragma once

#include <cstdint>
#include <map>
#include <string>

namespace uf::phasea {

using Kv = std::map<std::string, std::string>;

std::string kv_encode(const Kv& kv);
Kv kv_decode(const std::string& line);

std::string kv_get(const Kv& kv, const std::string& key, const std::string& fallback = "");
uint64_t kv_get_u64(const Kv& kv, const std::string& key, uint64_t fallback = 0);
int64_t kv_get_i64(const Kv& kv, const std::string& key, int64_t fallback = 0);

int create_server_socket(const std::string& path);
int accept_one(int server_fd);
int connect_socket_retry(const std::string& path, int retries, int sleep_ms);
void send_line(int fd, const std::string& line);
std::string recv_line(int fd);
void close_fd(int fd);
void unlink_socket(const std::string& path);

}  // namespace uf::phasea

