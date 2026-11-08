/* Official frozen MEM_SERVICE_ROOT source integration. No implementation copy. */
#include "pool_platform.h"
#include "pool_control.h"
#include "components/mem_service/providers/mem_service_provider_obmm.h"
#include <stdlib.h>

struct uflow_pool_platform {
    struct mem_service_provider_obmm_endpoint endpoint;
    struct mem_service_provider_registry registry;
    struct mem_service_provider_registration registration;
    bool prepared, activated;
};

int uflow_pool_platform_open(const char *instance, struct uflow_pool_platform **out)
{
    if (!out || *out) return -1;
    struct uflow_pool_platform *p = calloc(1, sizeof(*p));
    if (!p) return -1;
    /* Ownership is returned even on failed setup; checked close must resolve it. */
    *out = p;
    struct mem_service_provider_obmm_config config = {
        .instance = instance, .import_region_bytes = 128ULL << 20,
        .max_remote_mappings = 4, .required_peer_mappings = 1,
    };
    if (mem_service_provider_registry_init(&p->registry) ||
        mem_service_provider_obmm_endpoint_open(&p->endpoint, &config) ||
        mem_service_provider_obmm_endpoint_registration(&p->endpoint, &p->registration) ||
        mem_service_provider_registry_register(&p->registry, &p->registration)) return -1;
    return 0;
}

int uflow_pool_platform_prepare(struct uflow_pool_platform *p, uint8_t seed,
                               char descriptor[UFLOW_POOL_CANARY_HEX], uint64_t *checksum)
{
    if (!p || p->prepared || p->activated) return -1;
    struct mem_service_region region;
    struct mem_service_provider_remote_region remote;
    unsigned char wire[MEM_SERVICE_PROVIDER_REGION_WIRE_MAX_LEN]; size_t len;
    /* Official endpoint retains the export and any failed cleanup obligation. */
    if (mem_service_provider_obmm_endpoint_prepare_canary_region(&p->endpoint,
        UFLOW_POOL_CANARY_BYTES, UFLOW_POOL_CANARY_VISIBLE, seed, &region, &remote, checksum) ||
        mem_service_provider_remote_region_encode(&remote,wire,sizeof(wire),&len) ||
        pool_hex(wire,len,descriptor,UFLOW_POOL_CANARY_HEX)) return -1;
    p->prepared=true;
    return 0;
}

int uflow_pool_platform_activate(struct uflow_pool_platform *p,
    const char *peer_descriptor, uint64_t checksum, struct mem_service_provider_channel *channel)
{
    if (!p || p->activated || !channel || !checksum) return -1;
    unsigned char wire[MEM_SERVICE_PROVIDER_REGION_WIRE_MAX_LEN]; size_t len;
    struct mem_service_provider_remote_region remote;
    if (pool_unhex(peer_descriptor,wire,sizeof(wire),&len) ||
        mem_service_provider_remote_region_decode(wire,len,&remote) ||
        remote.len != UFLOW_POOL_CANARY_BYTES ||
        mem_service_provider_obmm_endpoint_verify_mapping(&p->endpoint,&remote,0,
            UFLOW_POOL_CANARY_VISIBLE,checksum,10000) ||
        mem_service_provider_registry_refresh(&p->registry)) return -1;
    if (!mem_service_provider_registry_data_plane_ready(&p->registry) ||
        mem_service_provider_channel_bind(&p->registry,p->registration.name,p->registration.instance,
            MEM_SERVICE_PROVIDER_CAP_PEER_MAPPING,channel)) return -1;
    p->activated=true;
    return 0;
}

int uflow_pool_platform_close(struct uflow_pool_platform **platform)
{
    if (!platform || !*platform) return 0;
    /* Business owner/domain must already be drained by the caller. Official
     * checked-close owns canary exports and retained verification mappings too. */
    /* A failed close retains the exact endpoint; never free/replace its context. */
    if (mem_service_provider_obmm_endpoint_close_checked(&(*platform)->endpoint)) return -1;
    free(*platform); *platform = NULL;
    return 0;
}
