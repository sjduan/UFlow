/* Official frozen MEM_SERVICE_ROOT source integration. No implementation copy. */
#include "pool_platform.h"
#include "components/mem_service/providers/mem_service_provider_obmm.h"
#include <stdlib.h>

struct uflow_pool_platform {
    struct mem_service_provider_obmm_endpoint endpoint;
    struct mem_service_provider_registry registry;
};

int uflow_pool_platform_open(const char *instance, struct uflow_pool_platform **out,
                             struct mem_service_provider_channel *channel)
{
    if (!out || *out || !channel) return -1;
    struct uflow_pool_platform *p = calloc(1, sizeof(*p));
    if (!p) return -1;
    /* Ownership is returned even on failed setup; checked close must resolve it. */
    *out = p;
    struct mem_service_provider_obmm_config config = {
        .instance = instance, .import_region_bytes = 128ULL << 20,
        .max_remote_mappings = 4, .required_peer_mappings = 1,
    };
    struct mem_service_provider_registration registration;
    if (mem_service_provider_registry_init(&p->registry) ||
        mem_service_provider_obmm_endpoint_open(&p->endpoint, &config) ||
        mem_service_provider_obmm_endpoint_registration(&p->endpoint, &registration) ||
        mem_service_provider_registry_register(&p->registry, &registration) ||
        mem_service_provider_registry_refresh(&p->registry)) return -1;
    return mem_service_provider_channel_bind(&p->registry, registration.name,
                                             registration.instance,
                                             MEM_SERVICE_PROVIDER_CAP_PEER_MAPPING,
                                             channel);
}

int uflow_pool_platform_close(struct uflow_pool_platform **platform)
{
    if (!platform || !*platform) return 0;
    struct mem_service_provider_obmm_resources_v1 resources;
    int observed = mem_service_provider_obmm_endpoint_resources_v1(&(*platform)->endpoint, &resources);
    if (!observed && (resources.export_handles || resources.import_handles || resources.vma_count ||
        resources.accessible_views || resources.cleanup_mappings)) return -1;
    /* A failed close retains the exact endpoint; never free/replace its context. */
    if (mem_service_provider_obmm_endpoint_close_checked(&(*platform)->endpoint)) return -1;
    free(*platform); *platform = NULL;
    return 0;
}
