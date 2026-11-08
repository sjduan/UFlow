#ifndef UFLOW_POOL_PLATFORM_H
#define UFLOW_POOL_PLATFORM_H
#include <mem_service_provider.h>
struct uflow_pool_platform;
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
