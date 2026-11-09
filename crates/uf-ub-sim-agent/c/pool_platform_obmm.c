/* Official frozen MEM_SERVICE_ROOT source integration. No implementation copy. */
#include "pool_platform.h"
#include "pool_control.h"
#include "components/mem_service/providers/mem_service_provider_obmm.h"
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include "gva_manager.h"

struct uflow_pool_platform {
    struct mem_service_provider_obmm_endpoint endpoint;
    struct mem_service_provider_registry registry;
    struct mem_service_provider_registration registration;
    bool prepared, activated, worker_bootstrapped;
};

int uflow_pool_platform_worker_bootstrap(struct uflow_pool_platform *p,
    unsigned node_id, struct uflow_pool_startup_diagnostic *d)
{
    if (!d) return -1;
    *d=(struct uflow_pool_startup_diagnostic){.stage="identity",.base=0x700000000000ULL,
        .size=256ULL<<20,.generation=1,.node_id=node_id,.node_count=2};
    int fd=-1, result=-1;
    if (!p || !p->activated || p->worker_bootstrapped || node_id>=2) {errno=EINVAL;goto done;}
    struct obmm_cmd_gsva_aperture expected={.base=d->base,.size=d->size,.generation=d->generation,
        .node_id=node_id,.node_count=2}, observed={0};
    struct obmm_cmd_gsva_enumerate_v1 inventory={.version=OBMM_GSVA_ABI_VERSION};
    d->stage="device-open";
    fd=open("/dev/obmm",O_RDWR|O_CLOEXEC);if(fd<0)goto done;
    d->stage="aperture-query";
    if(ioctl(fd,OBMM_CMD_GSVA_APERTURE_QUERY,&observed))goto done;
    if(observed.flags & OBMM_GSVA_APERTURE_F_ACTIVE) {
        /* Never overwrite or clear another active profile, even on mismatch. */
        d->stage="aperture-validate";
        if(gva_manager_validate_aperture(fd,&expected,&observed))goto done;
    } else {
        d->stage="aperture-register-validate";d->registration_attempted=true;
        if(gva_manager_register_aperture(fd,&expected,&observed))goto done;
    }
    d->stage="segment-enumerate";
    if(gva_manager_enumerate_segments(fd,&inventory))goto done;
    /* Only a fresh, empty kernel inventory is supported here. A nonempty
     * inventory is a separate recovery obligation, never an empty success. */
    if(inventory.flags!=OBMM_GSVA_ENUM_END){errno=EBUSY;goto done;}
    p->worker_bootstrapped=true;d->stage="ready";result=0;
done:
    d->error=result ? errno : 0;
    if(fd>=0)close(fd);
    errno=d->error;
    return result;
}

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
