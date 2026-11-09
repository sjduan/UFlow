#ifndef UFLOW_POOL_PLATFORM_H
#define UFLOW_POOL_PLATFORM_H
#include <mem_service_provider.h>
struct uflow_pool_platform;
struct uflow_pool_startup_diagnostic {
    const char *stage;
    int error;
    bool registration_attempted;
    uint64_t base, size, generation;
    unsigned node_id, node_count;
};
/* Platform-owned virtual address registry only, never a backing allocation.
 * On failure retain the complete diagnostic; do not infer rollback or ACK. */
int uflow_pool_platform_worker_bootstrap(struct uflow_pool_platform *platform,
    unsigned node_id, struct uflow_pool_startup_diagnostic *diagnostic);
/* Only this neutral/opaque seam is visible to the generic Guest client. */
/* open returns a NOT-ready endpoint. Only activate may return a channel. */
#define UFLOW_POOL_CANARY_BYTES (2ULL << 20)
#define UFLOW_POOL_CANARY_VISIBLE 4096ULL
#define UFLOW_POOL_CANARY_HEX 385
int uflow_pool_platform_open(const char *instance, struct uflow_pool_platform **out);
int uflow_pool_platform_prepare(struct uflow_pool_platform *platform, uint8_t seed,
                               char descriptor[UFLOW_POOL_CANARY_HEX], uint64_t *checksum);
int uflow_pool_platform_activate(struct uflow_pool_platform *platform,
    const char *peer_descriptor, uint64_t checksum, struct mem_service_provider_channel *channel);
int uflow_pool_platform_close(struct uflow_pool_platform **platform);
#endif
