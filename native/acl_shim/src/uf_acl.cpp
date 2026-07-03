#include "uf_acl.h"

#include <acl/acl.h>

#include <cstring>
#include <mutex>
#include <string>
#include <unordered_map>

namespace {

struct BackendHandle {
  aclrtDrvMemHandle handle = nullptr;
  void* service_ptr = nullptr;
  uint64_t shareable = 0;
  uint64_t requested_bytes = 0;
  uint64_t actual_bytes = 0;
  uint64_t granularity = 0;
  uint64_t service_mapping_id = 0;
  int32_t device_id = -1;
};

struct ImportedHandle {
  aclrtDrvMemHandle handle = nullptr;
  void* ptr = nullptr;
  uint64_t actual_bytes = 0;
  int32_t device_id = -1;
};

struct RegisteredHost {
  void* host_ptr = nullptr;
  void* device_ptr = nullptr;
  uint64_t bytes = 0;
  int32_t device_id = -1;
  bool use_v2 = false;
};

std::mutex g_mu;
std::unordered_map<uint64_t, BackendHandle> g_backend;
std::unordered_map<uint64_t, ImportedHandle> g_imported;
std::unordered_map<uint64_t, void*> g_host;
std::unordered_map<uint64_t, uint64_t> g_host_bytes;
std::unordered_map<uint64_t, RegisteredHost> g_registered_host;
std::unordered_map<uint64_t, aclrtStream> g_streams;
std::unordered_map<uint64_t, aclrtEvent> g_events;
uint64_t g_next_backend_id = 1;
uint64_t g_next_imported_id = 1;
uint64_t g_next_host_id = 1;
uint64_t g_next_registered_host_id = 1;
uint64_t g_next_stream_id = 1;
uint64_t g_next_event_id = 1;
bool g_initialized = false;
int32_t g_device_id = -1;

void set_status(UfAclStatus* status, int32_t code, const std::string& msg) {
  if (status == nullptr) {
    return;
  }
  status->code = code;
  std::memset(status->message, 0, sizeof(status->message));
  std::strncpy(status->message, msg.c_str(), sizeof(status->message) - 1);
}

int fail(UfAclStatus* status, const std::string& msg, aclError err = ACL_SUCCESS) {
  std::string full = msg;
  if (err != ACL_SUCCESS) {
    full += " acl_error=" + std::to_string(static_cast<int64_t>(err));
    const char* recent = aclGetRecentErrMsg();
    if (recent != nullptr && std::strlen(recent) > 0) {
      full += " recent=";
      full += recent;
    }
  }
  set_status(status, err == ACL_SUCCESS ? -1 : static_cast<int32_t>(err), full);
  return -1;
}

int ok(UfAclStatus* status) {
  set_status(status, 0, "ok");
  return 0;
}

uint64_t align_up(uint64_t value, uint64_t alignment) {
  if (alignment == 0) {
    return value;
  }
  return ((value + alignment - 1) / alignment) * alignment;
}

aclrtPhysicalMemProp make_prop(int32_t device_id) {
  aclrtPhysicalMemProp prop;
  std::memset(&prop, 0, sizeof(prop));
  prop.handleType = ACL_MEM_HANDLE_TYPE_NONE;
  prop.allocationType = ACL_MEM_ALLOCATION_TYPE_PINNED;
  prop.memAttr = ACL_HBM_MEM_HUGE;
  prop.location.id = device_id;
  prop.location.type = ACL_MEM_LOCATION_TYPE_DEVICE;
  return prop;
}

int ensure_runtime(int32_t device_id, UfAclStatus* status) {
  std::lock_guard<std::mutex> lock(g_mu);
  if (!g_initialized) {
    aclError err = aclInit(nullptr);
    if (err != ACL_SUCCESS) {
      return fail(status, "aclInit failed", err);
    }
    g_initialized = true;
  }
  aclError err = aclrtSetDevice(device_id);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtSetDevice failed", err);
  }
  g_device_id = device_id;

  aclrtContext context = nullptr;
  err = aclrtGetCurrentContext(&context);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtGetCurrentContext failed", err);
  }
  if (context == nullptr) {
    err = aclrtCreateContext(&context, device_id);
    if (err != ACL_SUCCESS) {
      return fail(status, "aclrtCreateContext failed", err);
    }
    err = aclrtSetCurrentContext(context);
    if (err != ACL_SUCCESS) {
      return fail(status, "aclrtSetCurrentContext failed", err);
    }
  }
  return ok(status);
}

int ensure_current_thread_runtime(UfAclStatus* status) {
  int32_t device_id = 0;
  {
    std::lock_guard<std::mutex> lock(g_mu);
    device_id = g_device_id >= 0 ? g_device_id : 0;
  }
  return ensure_runtime(device_id, status);
}

uint64_t get_granularity_or_zero(int32_t device_id) {
  auto prop = make_prop(device_id);
  size_t granularity = 0;
  aclError err = aclrtMemGetAllocationGranularity(
      &prop, static_cast<aclrtMemGranularityOptions>(0), &granularity);
  if (err != ACL_SUCCESS) {
    return 0;
  }
  return static_cast<uint64_t>(granularity);
}

aclrtStream stream_from_id(uint64_t stream_id, UfAclStatus* status) {
  if (stream_id == 0) {
    return nullptr;
  }
  auto it = g_streams.find(stream_id);
  if (it == g_streams.end()) {
    fail(status, "stream_id not found");
    return nullptr;
  }
  return it->second;
}

aclrtEvent event_from_id(uint64_t event_id, UfAclStatus* status) {
  if (event_id == 0) {
    return nullptr;
  }
  auto it = g_events.find(event_id);
  if (it == g_events.end()) {
    fail(status, "event_id not found");
    return nullptr;
  }
  return it->second;
}

int set_mem_access(void* ptr, uint64_t bytes, int32_t device_id, UfAclStatus* status) {
  aclrtMemAccessDesc desc;
  std::memset(&desc, 0, sizeof(desc));
  desc.location.type = ACL_MEM_LOCATION_TYPE_DEVICE;
  desc.location.id = device_id;
  desc.flags = ACL_RT_MEM_ACCESS_FLAGS_READWRITE;
  aclError err = aclrtMemSetAccess(ptr, static_cast<size_t>(bytes), &desc, 1);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtMemSetAccess failed", err);
  }
  return ok(status);
}

int map_physical_handle(aclrtDrvMemHandle handle, uint64_t bytes, int32_t device_id, void** out_ptr,
                        UfAclStatus* status) {
  if (out_ptr == nullptr) {
    return fail(status, "out_ptr is null");
  }
  void* ptr = nullptr;
  aclError err = aclrtReserveMemAddress(&ptr, static_cast<size_t>(bytes), 0, nullptr, 0);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtReserveMemAddress failed", err);
  }
  err = aclrtMapMem(ptr, static_cast<size_t>(bytes), 0, handle, 0);
  if (err != ACL_SUCCESS) {
    (void)aclrtReleaseMemAddress(ptr);
    return fail(status, "aclrtMapMem failed", err);
  }
  if (set_mem_access(ptr, bytes, device_id, status) != 0) {
    (void)aclrtUnmapMem(ptr);
    (void)aclrtReleaseMemAddress(ptr);
    return -1;
  }
  *out_ptr = ptr;
  return ok(status);
}

void unmap_physical_ptr(void* ptr) {
  if (ptr != nullptr) {
    (void)aclrtUnmapMem(ptr);
    (void)aclrtReleaseMemAddress(ptr);
  }
}

aclError memcpy_async_with_fallback(void* dst, size_t dst_size, const void* src, size_t src_size,
                                    aclrtMemcpyKind primary_kind, aclrtStream stream) {
  aclError err = aclrtMemcpyAsync(dst, dst_size, src, src_size, primary_kind, stream);
  if (err == ACL_SUCCESS || primary_kind != ACL_MEMCPY_DEVICE_TO_DEVICE) {
    return err;
  }
  err = aclrtMemcpyAsync(dst, dst_size, src, src_size, ACL_MEMCPY_INNER_DEVICE_TO_DEVICE, stream);
  if (err == ACL_SUCCESS) {
    return err;
  }
  return aclrtMemcpyAsync(dst, dst_size, src, src_size, ACL_MEMCPY_DEFAULT, stream);
}

int enable_peer_access_if_needed(int32_t device_id, int32_t peer_device_id, UfAclStatus* status) {
  if (device_id == peer_device_id) {
    return ok(status);
  }
  if (ensure_runtime(device_id, status) != 0) {
    return -1;
  }
  int32_t can_access = 0;
  aclError err = aclrtDeviceCanAccessPeer(&can_access, device_id, peer_device_id);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtDeviceCanAccessPeer failed", err);
  }
  if (can_access == 0) {
    return fail(status, "peer access is not supported");
  }
  err = aclrtDeviceEnablePeerAccess(peer_device_id, 0);
  if (err != ACL_SUCCESS) {
    const char* recent = aclGetRecentErrMsg();
    if (recent == nullptr || std::string(recent).find("already") == std::string::npos) {
      return fail(status, "aclrtDeviceEnablePeerAccess failed", err);
    }
  }
  return ok(status);
}

}  // namespace

extern "C" {

int uf_acl_backend_init(const UfAclInitOptions* options, UfAclStatus* status) {
  if (options == nullptr) {
    return fail(status, "options is null");
  }
  return ensure_runtime(options->device_id, status);
}

int uf_acl_backend_finalize(UfAclStatus* status) {
  std::lock_guard<std::mutex> lock(g_mu);
  for (auto& it : g_events) {
    if (it.second != nullptr) {
      (void)aclrtDestroyEvent(it.second);
    }
  }
  g_events.clear();
  for (auto& it : g_streams) {
    if (it.second != nullptr) {
      (void)aclrtDestroyStream(it.second);
    }
  }
  g_streams.clear();
  for (auto& it : g_host) {
    if (it.second != nullptr) {
      (void)aclrtFreeHost(it.second);
    }
  }
  g_host.clear();
  g_host_bytes.clear();
  for (auto& it : g_backend) {
    unmap_physical_ptr(it.second.service_ptr);
    if (it.second.handle != nullptr) {
      (void)aclrtFreePhysical(it.second.handle);
    }
  }
  g_backend.clear();
  if (g_device_id >= 0) {
    (void)aclrtResetDevice(g_device_id);
    g_device_id = -1;
  }
  if (g_initialized) {
    (void)aclFinalize();
    g_initialized = false;
  }
  return ok(status);
}

int uf_acl_get_mem_info(int32_t device_id, UfAclMemInfo* out, UfAclStatus* status) {
  if (out == nullptr) {
    return fail(status, "out is null");
  }
  if (ensure_runtime(device_id, status) != 0) {
    return -1;
  }
  size_t free_bytes = 0;
  size_t total_bytes = 0;
  aclError err = aclrtGetMemInfo(ACL_HBM_MEM, &free_bytes, &total_bytes);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtGetMemInfo failed", err);
  }
  out->free_bytes = static_cast<uint64_t>(free_bytes);
  out->total_bytes = static_cast<uint64_t>(total_bytes);
  return ok(status);
}

int uf_acl_get_allocation_granularity(int32_t device_id, uint64_t* out, UfAclStatus* status) {
  if (out == nullptr) {
    return fail(status, "out is null");
  }
  if (ensure_runtime(device_id, status) != 0) {
    return -1;
  }
  auto prop = make_prop(device_id);
  size_t granularity = 0;
  aclError err = aclrtMemGetAllocationGranularity(
      &prop, static_cast<aclrtMemGranularityOptions>(0), &granularity);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtMemGetAllocationGranularity failed", err);
  }
  *out = static_cast<uint64_t>(granularity);
  return ok(status);
}

int uf_acl_alloc_physical(const UfAclHbmAllocRequest* req, UfAclHbmBlock* out, UfAclStatus* status) {
  if (req == nullptr || out == nullptr) {
    return fail(status, "req/out is null");
  }
  if (ensure_runtime(req->device_id, status) != 0) {
    return -1;
  }
  uint64_t granularity = get_granularity_or_zero(req->device_id);
  if (granularity == 0) {
    return fail(status, "failed to query granularity");
  }
  uint64_t alignment = req->alignment == 0 ? granularity : req->alignment;
  uint64_t actual = align_up(req->requested_bytes, alignment);
  auto prop = make_prop(req->device_id);
  aclrtDrvMemHandle handle = nullptr;
  aclError err = aclrtMallocPhysical(&handle, static_cast<size_t>(actual), &prop, 0);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtMallocPhysical failed", err);
  }
  void* service_ptr = nullptr;
  if (map_physical_handle(handle, actual, req->device_id, &service_ptr, status) != 0) {
    (void)aclrtFreePhysical(handle);
    return -1;
  }

  uint64_t id = 0;
  {
    std::lock_guard<std::mutex> lock(g_mu);
    id = g_next_backend_id++;
    g_backend[id] = BackendHandle{handle, service_ptr, 0, req->requested_bytes, actual, granularity, id, req->device_id};
  }
  std::memset(out, 0, sizeof(*out));
  out->raw_handle_id = id;
  out->requested_bytes = req->requested_bytes;
  out->actual_bytes = actual;
  out->granularity = granularity;
  out->service_mapping_id = id;
  out->service_device_ptr = service_ptr;
  out->device_id = req->device_id;
  return ok(status);
}

int uf_acl_export_shareable(uint64_t raw_handle_id, UfAclHbmBlock* inout, UfAclStatus* status) {
  if (inout == nullptr) {
    return fail(status, "inout is null");
  }
  std::lock_guard<std::mutex> lock(g_mu);
  auto it = g_backend.find(raw_handle_id);
  if (it == g_backend.end()) {
    return fail(status, "raw_handle_id not found");
  }
  uint64_t shareable = 0;
  aclError err = aclrtMemExportToShareableHandle(
      it->second.handle, ACL_MEM_HANDLE_TYPE_NONE, 0, &shareable);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtMemExportToShareableHandle failed", err);
  }
  it->second.shareable = shareable;
  inout->raw_handle_id = raw_handle_id;
  inout->shareable_handle_payload[0] = shareable;
  inout->shareable_handle_bytes = sizeof(uint64_t);
  inout->requested_bytes = it->second.requested_bytes;
  inout->actual_bytes = it->second.actual_bytes;
  inout->granularity = it->second.granularity;
  inout->service_mapping_id = it->second.service_mapping_id;
  inout->service_device_ptr = it->second.service_ptr;
  inout->device_id = it->second.device_id;
  return ok(status);
}

int uf_acl_set_pid_access(uint64_t raw_handle_id, int64_t bare_tgid, UfAclStatus* status) {
  std::lock_guard<std::mutex> lock(g_mu);
  auto it = g_backend.find(raw_handle_id);
  if (it == g_backend.end()) {
    return fail(status, "raw_handle_id not found");
  }
  if (it->second.shareable == 0) {
    return fail(status, "shareable handle not exported");
  }
  int32_t pid = static_cast<int32_t>(bare_tgid);
  aclError err = aclrtMemSetPidToShareableHandle(it->second.shareable, &pid, 1);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtMemSetPidToShareableHandle failed", err);
  }
  return ok(status);
}

int uf_acl_free_physical(uint64_t raw_handle_id, UfAclStatus* status) {
  BackendHandle h;
  {
    std::lock_guard<std::mutex> lock(g_mu);
    auto it = g_backend.find(raw_handle_id);
    if (it == g_backend.end()) {
      return fail(status, "raw_handle_id not found");
    }
    h = it->second;
    g_backend.erase(it);
  }
  if (h.handle != nullptr) {
    unmap_physical_ptr(h.service_ptr);
    aclError err = aclrtFreePhysical(h.handle);
    if (err != ACL_SUCCESS) {
      return fail(status, "aclrtFreePhysical failed", err);
    }
  }
  return ok(status);
}

int uf_acl_client_init(int32_t device_id, UfAclStatus* status) {
  return ensure_runtime(device_id, status);
}

int uf_acl_client_get_bare_tgid(int64_t* out, UfAclStatus* status) {
  if (out == nullptr) {
    return fail(status, "out is null");
  }
  int32_t pid = -1;
  aclError err = aclrtDeviceGetBareTgid(&pid);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtDeviceGetBareTgid failed", err);
  }
  *out = static_cast<int64_t>(pid);
  return ok(status);
}

int uf_acl_import_and_map(const UfAclClientImportRequest* req, UfAclMappedMemory* out, UfAclStatus* status) {
  if (req == nullptr || out == nullptr) {
    return fail(status, "req/out is null");
  }
  if (ensure_runtime(req->device_id, status) != 0) {
    return -1;
  }
  uint64_t shareable = req->shareable_handle_payload[0];
  aclrtDrvMemHandle handle = nullptr;
  aclError err = aclrtMemImportFromShareableHandle(shareable, req->device_id, &handle);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtMemImportFromShareableHandle failed", err);
  }
  void* ptr = nullptr;
  if (map_physical_handle(handle, req->actual_bytes, req->device_id, &ptr, status) != 0) {
    (void)aclrtFreePhysical(handle);
    return -1;
  }
  uint64_t id = 0;
  {
    std::lock_guard<std::mutex> lock(g_mu);
    id = g_next_imported_id++;
    g_imported[id] = ImportedHandle{handle, ptr, req->actual_bytes, req->device_id};
  }
  out->imported_handle_id = id;
  out->device_ptr = ptr;
  out->actual_bytes = req->actual_bytes;
  out->device_id = req->device_id;
  return ok(status);
}

int uf_acl_h2d(void* device_ptr, uint64_t dst_offset_bytes, const void* host_src, uint64_t bytes, UfAclStatus* status) {
  int32_t device_id = 0;
  {
    std::lock_guard<std::mutex> lock(g_mu);
    device_id = g_device_id >= 0 ? g_device_id : 0;
  }
  return uf_acl_h2d_on_device(device_id, device_ptr, dst_offset_bytes, host_src, bytes, status);
}

int uf_acl_d2h(void* host_dst, const void* device_ptr, uint64_t src_offset_bytes, uint64_t bytes, UfAclStatus* status) {
  int32_t device_id = 0;
  {
    std::lock_guard<std::mutex> lock(g_mu);
    device_id = g_device_id >= 0 ? g_device_id : 0;
  }
  return uf_acl_d2h_on_device(device_id, host_dst, device_ptr, src_offset_bytes, bytes, status);
}

int uf_acl_d2d(void* dst_device_ptr, uint64_t dst_offset_bytes, const void* src_device_ptr,
               uint64_t src_offset_bytes, uint64_t bytes, UfAclStatus* status) {
  int32_t device_id = 0;
  {
    std::lock_guard<std::mutex> lock(g_mu);
    device_id = g_device_id >= 0 ? g_device_id : 0;
  }
  return uf_acl_d2d_on_devices(device_id, dst_device_ptr, dst_offset_bytes, device_id, src_device_ptr,
                               src_offset_bytes, bytes, status);
}

int uf_acl_h2d_on_device(int32_t device_id, void* device_ptr, uint64_t dst_offset_bytes,
                         const void* host_src, uint64_t bytes, UfAclStatus* status) {
  if (ensure_runtime(device_id, status) != 0) {
    return -1;
  }
  char* dst = static_cast<char*>(device_ptr) + dst_offset_bytes;
  aclError err = aclrtMemcpy(dst, static_cast<size_t>(bytes), host_src, static_cast<size_t>(bytes),
                             ACL_MEMCPY_HOST_TO_DEVICE);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtMemcpy H2D failed", err);
  }
  return ok(status);
}

int uf_acl_d2h_on_device(int32_t device_id, void* host_dst, const void* device_ptr,
                         uint64_t src_offset_bytes, uint64_t bytes, UfAclStatus* status) {
  if (ensure_runtime(device_id, status) != 0) {
    return -1;
  }
  const char* src = static_cast<const char*>(device_ptr) + src_offset_bytes;
  aclError err = aclrtMemcpy(host_dst, static_cast<size_t>(bytes), src, static_cast<size_t>(bytes),
                             ACL_MEMCPY_DEVICE_TO_HOST);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtMemcpy D2H failed", err);
  }
  return ok(status);
}

int uf_acl_d2d_on_devices(int32_t dst_device_id, void* dst_device_ptr, uint64_t dst_offset_bytes,
                          int32_t src_device_id, const void* src_device_ptr,
                          uint64_t src_offset_bytes, uint64_t bytes, UfAclStatus* status) {
  return uf_acl_d2d_async_wait_on_devices(dst_device_id, dst_device_ptr, dst_offset_bytes, src_device_id,
                                          src_device_ptr, src_offset_bytes, bytes, status);
}

int uf_acl_h2d_async_wait_on_device(int32_t device_id, void* device_ptr, uint64_t dst_offset_bytes,
                                    const void* host_src, uint64_t bytes, UfAclStatus* status) {
  if (ensure_runtime(device_id, status) != 0) {
    return -1;
  }
  aclrtStream stream = nullptr;
  aclError err = aclrtCreateStream(&stream);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtCreateStream failed", err);
  }
  char* dst = static_cast<char*>(device_ptr) + dst_offset_bytes;
  err = aclrtMemcpyAsync(dst, static_cast<size_t>(bytes), host_src, static_cast<size_t>(bytes),
                         ACL_MEMCPY_HOST_TO_DEVICE, stream);
  if (err == ACL_SUCCESS) {
    err = aclrtSynchronizeStream(stream);
  }
  (void)aclrtDestroyStream(stream);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtMemcpyAsync H2D wait failed", err);
  }
  return ok(status);
}

int uf_acl_d2h_async_wait_on_device(int32_t device_id, void* host_dst, const void* device_ptr,
                                    uint64_t src_offset_bytes, uint64_t bytes, UfAclStatus* status) {
  if (ensure_runtime(device_id, status) != 0) {
    return -1;
  }
  aclrtStream stream = nullptr;
  aclError err = aclrtCreateStream(&stream);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtCreateStream failed", err);
  }
  const char* src = static_cast<const char*>(device_ptr) + src_offset_bytes;
  err = aclrtMemcpyAsync(host_dst, static_cast<size_t>(bytes), src, static_cast<size_t>(bytes),
                         ACL_MEMCPY_DEVICE_TO_HOST, stream);
  if (err == ACL_SUCCESS) {
    err = aclrtSynchronizeStream(stream);
  }
  (void)aclrtDestroyStream(stream);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtMemcpyAsync D2H wait failed", err);
  }
  return ok(status);
}

int uf_acl_d2d_async_wait_on_devices(int32_t dst_device_id, void* dst_device_ptr, uint64_t dst_offset_bytes,
                                     int32_t src_device_id, const void* src_device_ptr,
                                     uint64_t src_offset_bytes, uint64_t bytes, UfAclStatus* status) {
  if (enable_peer_access_if_needed(dst_device_id, src_device_id, status) != 0) {
    return -1;
  }
  if (enable_peer_access_if_needed(src_device_id, dst_device_id, status) != 0) {
    return -1;
  }
  if (ensure_runtime(dst_device_id, status) != 0) {
    return -1;
  }
  aclrtStream stream = nullptr;
  aclError err = aclrtCreateStream(&stream);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtCreateStream failed", err);
  }
  char* dst = static_cast<char*>(dst_device_ptr) + dst_offset_bytes;
  const char* src = static_cast<const char*>(src_device_ptr) + src_offset_bytes;
  err = memcpy_async_with_fallback(dst, static_cast<size_t>(bytes), src, static_cast<size_t>(bytes),
                                   ACL_MEMCPY_DEVICE_TO_DEVICE, stream);
  if (err == ACL_SUCCESS) {
    err = aclrtSynchronizeStream(stream);
  }
  (void)aclrtDestroyStream(stream);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtMemcpyAsync D2D wait failed", err);
  }
  return ok(status);
}

int uf_acl_device_can_access_peer(int32_t device_id, int32_t peer_device_id, int32_t* out_can_access, UfAclStatus* status) {
  if (out_can_access == nullptr) {
    return fail(status, "out_can_access is null");
  }
  if (ensure_runtime(device_id, status) != 0) {
    return -1;
  }
  aclError err = aclrtDeviceCanAccessPeer(out_can_access, device_id, peer_device_id);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtDeviceCanAccessPeer failed", err);
  }
  return ok(status);
}

int uf_acl_enable_peer_access(int32_t device_id, int32_t peer_device_id, UfAclStatus* status) {
  return enable_peer_access_if_needed(device_id, peer_device_id, status);
}

int uf_acl_malloc_host(uint64_t bytes, UfAclHostMemory* out, UfAclStatus* status) {
  if (out == nullptr) {
    return fail(status, "out is null");
  }
  if (bytes == 0) {
    return fail(status, "bytes must be positive");
  }
  if (ensure_runtime(g_device_id >= 0 ? g_device_id : 0, status) != 0) {
    return -1;
  }
  void* ptr = nullptr;
  aclError err = aclrtMallocHost(&ptr, static_cast<size_t>(bytes));
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtMallocHost failed", err);
  }
  uint64_t id = 0;
  {
    std::lock_guard<std::mutex> lock(g_mu);
    id = g_next_host_id++;
    g_host[id] = ptr;
    g_host_bytes[id] = bytes;
  }
  out->host_handle_id = id;
  out->host_ptr = ptr;
  out->bytes = bytes;
  return ok(status);
}

int uf_acl_free_host(UfAclHostMemory* host, UfAclStatus* status) {
  if (host == nullptr) {
    return fail(status, "host is null");
  }
  void* ptr = nullptr;
  {
    std::lock_guard<std::mutex> lock(g_mu);
    auto it = g_host.find(host->host_handle_id);
    if (it == g_host.end()) {
      return fail(status, "host_handle_id not found");
    }
    ptr = it->second;
    g_host.erase(it);
    g_host_bytes.erase(host->host_handle_id);
  }
  if (ptr != nullptr) {
    aclError err = aclrtFreeHost(ptr);
    if (err != ACL_SUCCESS) {
      return fail(status, "aclrtFreeHost failed", err);
    }
  }
  host->host_handle_id = 0;
  host->host_ptr = nullptr;
  host->bytes = 0;
  return ok(status);
}

int uf_acl_host_register(const UfAclHostRegisterRequest* req, UfAclHostRegisterInfo* out, UfAclStatus* status) {
  if (req == nullptr) {
    return fail(status, "req is null");
  }
  if (out == nullptr) {
    return fail(status, "out is null");
  }
  if (req->host_ptr == nullptr) {
    return fail(status, "host_ptr is null");
  }
  if (req->bytes == 0) {
    return fail(status, "bytes must be positive");
  }
  const int32_t device_id = req->device_id >= 0 ? req->device_id : (g_device_id >= 0 ? g_device_id : 0);
  if (ensure_runtime(device_id, status) != 0) {
    return -1;
  }

  void* device_ptr = nullptr;
  aclError err = ACL_SUCCESS;
  if (req->use_v2 != 0) {
    err = aclrtHostRegisterV2(req->host_ptr, req->bytes, req->flags);
    if (err != ACL_SUCCESS) {
      return fail(status, "aclrtHostRegisterV2 failed", err);
    }
    err = aclrtHostGetDevicePointer(req->host_ptr, &device_ptr, 0);
    if (err != ACL_SUCCESS) {
      (void)aclrtHostUnregister(req->host_ptr);
      return fail(status, "aclrtHostGetDevicePointer after V2 register failed", err);
    }
  } else {
    err = aclrtHostRegister(req->host_ptr, req->bytes, ACL_HOST_REGISTER_MAPPED, &device_ptr);
    if (err != ACL_SUCCESS) {
      return fail(status, "aclrtHostRegister failed", err);
    }
  }

  uint64_t id = 0;
  {
    std::lock_guard<std::mutex> lock(g_mu);
    id = g_next_registered_host_id++;
    g_registered_host[id] = RegisteredHost{
        req->host_ptr,
        device_ptr,
        req->bytes,
        device_id,
        req->use_v2 != 0,
    };
  }
  out->registered_host_id = id;
  out->host_ptr = req->host_ptr;
  out->device_ptr = device_ptr;
  out->bytes = req->bytes;
  out->device_id = device_id;
  out->use_v2 = req->use_v2 != 0 ? 1 : 0;
  return ok(status);
}

int uf_acl_host_get_device_pointer(void* host_ptr, void** out_device_ptr, UfAclStatus* status) {
  if (host_ptr == nullptr) {
    return fail(status, "host_ptr is null");
  }
  if (out_device_ptr == nullptr) {
    return fail(status, "out_device_ptr is null");
  }
  if (ensure_runtime(g_device_id >= 0 ? g_device_id : 0, status) != 0) {
    return -1;
  }
  void* device_ptr = nullptr;
  aclError err = aclrtHostGetDevicePointer(host_ptr, &device_ptr, 0);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtHostGetDevicePointer failed", err);
  }
  *out_device_ptr = device_ptr;
  return ok(status);
}

int uf_acl_host_unregister(UfAclHostRegisterInfo* info, UfAclStatus* status) {
  if (info == nullptr) {
    return fail(status, "info is null");
  }
  RegisteredHost registered;
  bool found = false;
  {
    std::lock_guard<std::mutex> lock(g_mu);
    auto it = g_registered_host.find(info->registered_host_id);
    if (it != g_registered_host.end()) {
      registered = it->second;
      g_registered_host.erase(it);
      found = true;
    }
  }
  if (!found) {
    if (info->host_ptr == nullptr) {
      return fail(status, "registered_host_id not found");
    }
    registered.host_ptr = info->host_ptr;
    registered.device_ptr = info->device_ptr;
    registered.bytes = info->bytes;
    registered.device_id = info->device_id >= 0 ? info->device_id : (g_device_id >= 0 ? g_device_id : 0);
    registered.use_v2 = info->use_v2 != 0;
  }
  if (ensure_runtime(registered.device_id, status) != 0) {
    return -1;
  }
  aclError err = aclrtHostUnregister(registered.host_ptr);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtHostUnregister failed", err);
  }
  info->registered_host_id = 0;
  info->host_ptr = nullptr;
  info->device_ptr = nullptr;
  info->bytes = 0;
  info->device_id = -1;
  info->use_v2 = 0;
  return ok(status);
}

int uf_acl_create_stream(uint64_t* out_stream_id, UfAclStatus* status) {
  if (out_stream_id == nullptr) {
    return fail(status, "out_stream_id is null");
  }
  if (ensure_runtime(g_device_id >= 0 ? g_device_id : 0, status) != 0) {
    return -1;
  }
  aclrtStream stream = nullptr;
  aclError err = aclrtCreateStream(&stream);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtCreateStream failed", err);
  }
  std::lock_guard<std::mutex> lock(g_mu);
  uint64_t id = g_next_stream_id++;
  g_streams[id] = stream;
  *out_stream_id = id;
  return ok(status);
}

int uf_acl_destroy_stream(uint64_t stream_id, UfAclStatus* status) {
  aclrtStream stream = nullptr;
  {
    std::lock_guard<std::mutex> lock(g_mu);
    auto it = g_streams.find(stream_id);
    if (it == g_streams.end()) {
      return fail(status, "stream_id not found");
    }
    stream = it->second;
    g_streams.erase(it);
  }
  if (stream != nullptr) {
    aclError err = aclrtDestroyStream(stream);
    if (err != ACL_SUCCESS) {
      return fail(status, "aclrtDestroyStream failed", err);
    }
  }
  return ok(status);
}

int uf_acl_create_event(uint64_t* out_event_id, UfAclStatus* status) {
  if (out_event_id == nullptr) {
    return fail(status, "out_event_id is null");
  }
  if (ensure_runtime(g_device_id >= 0 ? g_device_id : 0, status) != 0) {
    return -1;
  }
  aclrtEvent event = nullptr;
  aclError err = aclrtCreateEvent(&event);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtCreateEvent failed", err);
  }
  std::lock_guard<std::mutex> lock(g_mu);
  uint64_t id = g_next_event_id++;
  g_events[id] = event;
  *out_event_id = id;
  return ok(status);
}

int uf_acl_destroy_event(uint64_t event_id, UfAclStatus* status) {
  aclrtEvent event = nullptr;
  {
    std::lock_guard<std::mutex> lock(g_mu);
    auto it = g_events.find(event_id);
    if (it == g_events.end()) {
      return fail(status, "event_id not found");
    }
    event = it->second;
    g_events.erase(it);
  }
  if (event != nullptr) {
    aclError err = aclrtDestroyEvent(event);
    if (err != ACL_SUCCESS) {
      return fail(status, "aclrtDestroyEvent failed", err);
    }
  }
  return ok(status);
}

int uf_acl_get_event_handle(uint64_t event_id, uint64_t* out_event_handle, UfAclStatus* status) {
  if (out_event_handle == nullptr) {
    return fail(status, "out_event_handle is null");
  }
  std::lock_guard<std::mutex> lock(g_mu);
  aclrtEvent event = event_from_id(event_id, status);
  if (event_id != 0 && event == nullptr) {
    return -1;
  }
  *out_event_handle = reinterpret_cast<uint64_t>(event);
  return ok(status);
}

int uf_acl_record_event(uint64_t event_id, uint64_t stream_id, UfAclStatus* status) {
  aclrtEvent event = nullptr;
  aclrtStream stream = nullptr;
  {
    std::lock_guard<std::mutex> lock(g_mu);
    event = event_from_id(event_id, status);
    if (event_id != 0 && event == nullptr) {
      return -1;
    }
    stream = stream_from_id(stream_id, status);
    if (stream_id != 0 && stream == nullptr) {
      return -1;
    }
  }
  aclError err = aclrtRecordEvent(event, stream);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtRecordEvent failed", err);
  }
  return ok(status);
}

int uf_acl_stream_wait_event(uint64_t stream_id, uint64_t event_id, UfAclStatus* status) {
  aclrtStream stream = nullptr;
  aclrtEvent event = nullptr;
  {
    std::lock_guard<std::mutex> lock(g_mu);
    stream = stream_from_id(stream_id, status);
    if (stream_id != 0 && stream == nullptr) {
      return -1;
    }
    event = event_from_id(event_id, status);
    if (event_id != 0 && event == nullptr) {
      return -1;
    }
  }
  aclError err = aclrtStreamWaitEvent(stream, event);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtStreamWaitEvent failed", err);
  }
  return ok(status);
}

int uf_acl_synchronize_stream(uint64_t stream_id, UfAclStatus* status) {
  aclrtStream stream = nullptr;
  {
    std::lock_guard<std::mutex> lock(g_mu);
    stream = stream_from_id(stream_id, status);
    if (stream_id != 0 && stream == nullptr) {
      return -1;
    }
  }
  aclError err = aclrtSynchronizeStream(stream);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtSynchronizeStream failed", err);
  }
  return ok(status);
}

int uf_acl_synchronize_event(uint64_t event_id, UfAclStatus* status) {
  aclrtEvent event = nullptr;
  {
    std::lock_guard<std::mutex> lock(g_mu);
    event = event_from_id(event_id, status);
    if (event_id != 0 && event == nullptr) {
      return -1;
    }
  }
  aclError err = aclrtSynchronizeEvent(event);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtSynchronizeEvent failed", err);
  }
  return ok(status);
}

int uf_acl_h2d_async(void* device_ptr, uint64_t dst_offset_bytes, const void* host_src, uint64_t bytes,
                     uint64_t stream_id, uint64_t event_id, UfAclStatus* status) {
  if (ensure_current_thread_runtime(status) != 0) {
    return -1;
  }
  aclrtStream stream = nullptr;
  aclrtEvent event = nullptr;
  {
    std::lock_guard<std::mutex> lock(g_mu);
    stream = stream_from_id(stream_id, status);
    if (stream_id != 0 && stream == nullptr) {
      return -1;
    }
    event = event_from_id(event_id, status);
    if (event_id != 0 && event == nullptr) {
      return -1;
    }
  }
  char* dst = static_cast<char*>(device_ptr) + dst_offset_bytes;
  aclError err = aclrtMemcpyAsync(dst, static_cast<size_t>(bytes), host_src, static_cast<size_t>(bytes),
                                  ACL_MEMCPY_HOST_TO_DEVICE, stream);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtMemcpyAsync H2D failed", err);
  }
  if (event != nullptr) {
    err = aclrtRecordEvent(event, stream);
    if (err != ACL_SUCCESS) {
      return fail(status, "aclrtRecordEvent after H2D failed", err);
    }
  }
  return ok(status);
}

int uf_acl_h2d_async_on_device(int32_t device_id, void* device_ptr, uint64_t dst_offset_bytes,
                               const void* host_src, uint64_t bytes, uint64_t stream_id,
                               uint64_t event_id, UfAclStatus* status) {
  if (ensure_runtime(device_id, status) != 0) {
    return -1;
  }
  return uf_acl_h2d_async(device_ptr, dst_offset_bytes, host_src, bytes, stream_id, event_id, status);
}

int uf_acl_d2h_async(void* host_dst, const void* device_ptr, uint64_t src_offset_bytes, uint64_t bytes,
                     uint64_t stream_id, uint64_t event_id, UfAclStatus* status) {
  if (ensure_current_thread_runtime(status) != 0) {
    return -1;
  }
  aclrtStream stream = nullptr;
  aclrtEvent event = nullptr;
  {
    std::lock_guard<std::mutex> lock(g_mu);
    stream = stream_from_id(stream_id, status);
    if (stream_id != 0 && stream == nullptr) {
      return -1;
    }
    event = event_from_id(event_id, status);
    if (event_id != 0 && event == nullptr) {
      return -1;
    }
  }
  const char* src = static_cast<const char*>(device_ptr) + src_offset_bytes;
  aclError err = aclrtMemcpyAsync(host_dst, static_cast<size_t>(bytes), src, static_cast<size_t>(bytes),
                                  ACL_MEMCPY_DEVICE_TO_HOST, stream);
  if (err != ACL_SUCCESS) {
    return fail(status, "aclrtMemcpyAsync D2H failed", err);
  }
  if (event != nullptr) {
    err = aclrtRecordEvent(event, stream);
    if (err != ACL_SUCCESS) {
      return fail(status, "aclrtRecordEvent after D2H failed", err);
    }
  }
  return ok(status);
}

int uf_acl_d2h_async_on_device(int32_t device_id, void* host_dst, const void* device_ptr,
                               uint64_t src_offset_bytes, uint64_t bytes, uint64_t stream_id,
                               uint64_t event_id, UfAclStatus* status) {
  if (ensure_runtime(device_id, status) != 0) {
    return -1;
  }
  return uf_acl_d2h_async(host_dst, device_ptr, src_offset_bytes, bytes, stream_id, event_id, status);
}

int uf_acl_unmap_and_release(UfAclMappedMemory* mapped, UfAclStatus* status) {
  if (mapped == nullptr) {
    return fail(status, "mapped is null");
  }
  ImportedHandle h;
  {
    std::lock_guard<std::mutex> lock(g_mu);
    auto it = g_imported.find(mapped->imported_handle_id);
    if (it == g_imported.end()) {
      return fail(status, "imported_handle_id not found");
    }
    h = it->second;
    g_imported.erase(it);
  }
  if (h.ptr != nullptr) {
    (void)aclrtUnmapMem(h.ptr);
    (void)aclrtReleaseMemAddress(h.ptr);
  }
  if (h.handle != nullptr) {
    (void)aclrtFreePhysical(h.handle);
  }
  mapped->imported_handle_id = 0;
  mapped->device_ptr = nullptr;
  return ok(status);
}

int uf_acl_client_finalize(UfAclStatus* status) {
  std::lock_guard<std::mutex> lock(g_mu);
  for (auto& it : g_events) {
    if (it.second != nullptr) {
      (void)aclrtDestroyEvent(it.second);
    }
  }
  g_events.clear();
  for (auto& it : g_streams) {
    if (it.second != nullptr) {
      (void)aclrtDestroyStream(it.second);
    }
  }
  g_streams.clear();
  for (auto& it : g_host) {
    if (it.second != nullptr) {
      (void)aclrtFreeHost(it.second);
    }
  }
  g_host.clear();
  g_host_bytes.clear();
  for (auto& it : g_imported) {
    if (it.second.ptr != nullptr) {
      (void)aclrtUnmapMem(it.second.ptr);
      (void)aclrtReleaseMemAddress(it.second.ptr);
    }
    if (it.second.handle != nullptr) {
      (void)aclrtFreePhysical(it.second.handle);
    }
  }
  g_imported.clear();
  if (g_device_id >= 0) {
    (void)aclrtResetDevice(g_device_id);
    g_device_id = -1;
  }
  if (g_initialized) {
    (void)aclFinalize();
    g_initialized = false;
  }
  return ok(status);
}

}  // extern "C"
