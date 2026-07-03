#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct UfAclStatus {
  int32_t code;
  char message[256];
} UfAclStatus;

typedef struct UfAclInitOptions {
  int32_t device_id;
  uint32_t flags;
} UfAclInitOptions;

typedef struct UfAclMemInfo {
  uint64_t free_bytes;
  uint64_t total_bytes;
} UfAclMemInfo;

typedef struct UfAclHbmAllocRequest {
  int32_t device_id;
  uint64_t requested_bytes;
  uint64_t alignment;
  uint32_t memory_type;
} UfAclHbmAllocRequest;

typedef struct UfAclHbmBlock {
  uint64_t raw_handle_id;
  uint64_t shareable_handle_payload[8];
  uint64_t shareable_handle_bytes;
  uint64_t requested_bytes;
  uint64_t actual_bytes;
  uint64_t granularity;
  uint64_t service_mapping_id;
  void* service_device_ptr;
  int32_t device_id;
} UfAclHbmBlock;

typedef struct UfAclClientImportRequest {
  int32_t device_id;
  uint64_t shareable_handle_payload[8];
  uint64_t shareable_handle_bytes;
  uint64_t actual_bytes;
} UfAclClientImportRequest;

typedef struct UfAclMappedMemory {
  uint64_t imported_handle_id;
  void* device_ptr;
  uint64_t actual_bytes;
  int32_t device_id;
} UfAclMappedMemory;

typedef struct UfAclHostMemory {
  uint64_t host_handle_id;
  void* host_ptr;
  uint64_t bytes;
} UfAclHostMemory;

typedef struct UfAclHostRegisterRequest {
  int32_t device_id;
  void* host_ptr;
  uint64_t bytes;
  uint32_t flags;
  uint32_t use_v2;
} UfAclHostRegisterRequest;

typedef struct UfAclHostRegisterInfo {
  uint64_t registered_host_id;
  void* host_ptr;
  void* device_ptr;
  uint64_t bytes;
  int32_t device_id;
  uint32_t use_v2;
} UfAclHostRegisterInfo;

int uf_acl_backend_init(const UfAclInitOptions* options, UfAclStatus* status);
int uf_acl_backend_finalize(UfAclStatus* status);
int uf_acl_get_mem_info(int32_t device_id, UfAclMemInfo* out, UfAclStatus* status);
int uf_acl_get_allocation_granularity(int32_t device_id, uint64_t* out, UfAclStatus* status);
int uf_acl_alloc_physical(const UfAclHbmAllocRequest* req, UfAclHbmBlock* out, UfAclStatus* status);
int uf_acl_export_shareable(uint64_t raw_handle_id, UfAclHbmBlock* inout, UfAclStatus* status);
int uf_acl_set_pid_access(uint64_t raw_handle_id, int64_t bare_tgid, UfAclStatus* status);
int uf_acl_free_physical(uint64_t raw_handle_id, UfAclStatus* status);

int uf_acl_client_init(int32_t device_id, UfAclStatus* status);
int uf_acl_client_get_bare_tgid(int64_t* out, UfAclStatus* status);
int uf_acl_import_and_map(const UfAclClientImportRequest* req, UfAclMappedMemory* out, UfAclStatus* status);
int uf_acl_h2d(void* device_ptr, uint64_t dst_offset_bytes, const void* host_src, uint64_t bytes, UfAclStatus* status);
int uf_acl_d2h(void* host_dst, const void* device_ptr, uint64_t src_offset_bytes, uint64_t bytes, UfAclStatus* status);
int uf_acl_d2d(void* dst_device_ptr, uint64_t dst_offset_bytes, const void* src_device_ptr,
               uint64_t src_offset_bytes, uint64_t bytes, UfAclStatus* status);
int uf_acl_h2d_on_device(int32_t device_id, void* device_ptr, uint64_t dst_offset_bytes,
                         const void* host_src, uint64_t bytes, UfAclStatus* status);
int uf_acl_d2h_on_device(int32_t device_id, void* host_dst, const void* device_ptr,
                         uint64_t src_offset_bytes, uint64_t bytes, UfAclStatus* status);
int uf_acl_d2d_on_devices(int32_t dst_device_id, void* dst_device_ptr, uint64_t dst_offset_bytes,
                          int32_t src_device_id, const void* src_device_ptr,
                          uint64_t src_offset_bytes, uint64_t bytes, UfAclStatus* status);
int uf_acl_h2d_async_wait_on_device(int32_t device_id, void* device_ptr, uint64_t dst_offset_bytes,
                                    const void* host_src, uint64_t bytes, UfAclStatus* status);
int uf_acl_d2h_async_wait_on_device(int32_t device_id, void* host_dst, const void* device_ptr,
                                    uint64_t src_offset_bytes, uint64_t bytes, UfAclStatus* status);
int uf_acl_d2d_async_wait_on_devices(int32_t dst_device_id, void* dst_device_ptr, uint64_t dst_offset_bytes,
                                     int32_t src_device_id, const void* src_device_ptr,
                                     uint64_t src_offset_bytes, uint64_t bytes, UfAclStatus* status);
int uf_acl_device_can_access_peer(int32_t device_id, int32_t peer_device_id, int32_t* out_can_access, UfAclStatus* status);
int uf_acl_enable_peer_access(int32_t device_id, int32_t peer_device_id, UfAclStatus* status);
int uf_acl_malloc_host(uint64_t bytes, UfAclHostMemory* out, UfAclStatus* status);
int uf_acl_free_host(UfAclHostMemory* host, UfAclStatus* status);
int uf_acl_host_register(const UfAclHostRegisterRequest* req, UfAclHostRegisterInfo* out, UfAclStatus* status);
int uf_acl_host_get_device_pointer(void* host_ptr, void** out_device_ptr, UfAclStatus* status);
int uf_acl_host_unregister(UfAclHostRegisterInfo* info, UfAclStatus* status);
int uf_acl_create_stream(uint64_t* out_stream_id, UfAclStatus* status);
int uf_acl_destroy_stream(uint64_t stream_id, UfAclStatus* status);
int uf_acl_create_event(uint64_t* out_event_id, UfAclStatus* status);
int uf_acl_destroy_event(uint64_t event_id, UfAclStatus* status);
int uf_acl_get_event_handle(uint64_t event_id, uint64_t* out_event_handle, UfAclStatus* status);
int uf_acl_record_event(uint64_t event_id, uint64_t stream_id, UfAclStatus* status);
int uf_acl_stream_wait_event(uint64_t stream_id, uint64_t event_id, UfAclStatus* status);
int uf_acl_synchronize_stream(uint64_t stream_id, UfAclStatus* status);
int uf_acl_synchronize_event(uint64_t event_id, UfAclStatus* status);
int uf_acl_h2d_async(void* device_ptr, uint64_t dst_offset_bytes, const void* host_src, uint64_t bytes,
                     uint64_t stream_id, uint64_t event_id, UfAclStatus* status);
int uf_acl_h2d_async_on_device(int32_t device_id, void* device_ptr, uint64_t dst_offset_bytes,
                               const void* host_src, uint64_t bytes, uint64_t stream_id,
                               uint64_t event_id, UfAclStatus* status);
int uf_acl_d2h_async(void* host_dst, const void* device_ptr, uint64_t src_offset_bytes, uint64_t bytes,
                     uint64_t stream_id, uint64_t event_id, UfAclStatus* status);
int uf_acl_d2h_async_on_device(int32_t device_id, void* host_dst, const void* device_ptr,
                               uint64_t src_offset_bytes, uint64_t bytes, uint64_t stream_id,
                               uint64_t event_id, UfAclStatus* status);
int uf_acl_unmap_and_release(UfAclMappedMemory* mapped, UfAclStatus* status);
int uf_acl_client_finalize(UfAclStatus* status);

#ifdef __cplusplus
}
#endif
