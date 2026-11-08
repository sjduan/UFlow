#ifndef UFLOW_POOL_PLATFORM_H
#define UFLOW_POOL_PLATFORM_H
#include <mem_service_provider.h>
struct uflow_pool_platform;
/* Only this neutral/opaque seam is visible to the generic Guest client. */
int uflow_pool_platform_open(const char *instance, struct uflow_pool_platform **out,
                             struct mem_service_provider_channel *channel);
int uflow_pool_platform_close(struct uflow_pool_platform **platform);
#endif
