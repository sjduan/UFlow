/* B kind=build CPU contract only. Real facade + installed neutral provider.c;
 * official endpoint calls below are explicit test doubles, not OBMM evidence.
 * Include the facade in this one test TU so only the platform TU consumes the
 * frozen source-only header. Never inspect endpoint.implementation. */
#include "pool_platform_obmm.c"
#include <assert.h>
#include <stdio.h>

struct fixture {const struct mem_service_provider_obmm_endpoint *endpoint;const char *instance;bool ready,exported,closed;};
static struct fixture endpoints[8];static unsigned count,verifies,closes;static bool fail_verify,fail_close;
static struct fixture *lookup(const struct mem_service_provider_obmm_endpoint *endpoint) {
    for(unsigned i=0;i<count;i++)if(endpoints[i].endpoint==endpoint&&!endpoints[i].closed)return &endpoints[i];
    assert(0);return NULL;
}
static int probe(void *context,enum mem_service_provider_state *state) {
    *state=lookup(context)->ready?MEM_SERVICE_PROVIDER_STATE_READY:MEM_SERVICE_PROVIDER_STATE_DEGRADED;return 0;
}
static int map(void *c,const struct mem_service_mapping_request *r,struct mem_service_mapping *m) {(void)c;(void)r;(void)m;return -1;}
static int unmap(void *c,uint64_t h) {(void)c;(void)h;return -1;}
static int visible(void *c,const struct mem_service_mapping_range_request *r,struct mem_service_visibility_completion *o) {(void)c;(void)r;(void)o;return -1;}
static const struct mem_service_provider_ops ops={.probe=probe,.map_remote_region=map,.unmap_remote_region=unmap,
    .publish_range=visible,.invalidate_range=visible,.wait_range_visible=visible};
int mem_service_provider_obmm_endpoint_open(struct mem_service_provider_obmm_endpoint *endpoint,
    const struct mem_service_provider_obmm_config *config) {
    assert(count<8 && config->required_peer_mappings==1);
    endpoints[count++]=(struct fixture){.endpoint=endpoint,.instance=config->instance};return 0;
}
int mem_service_provider_obmm_endpoint_registration(struct mem_service_provider_obmm_endpoint *endpoint,
    struct mem_service_provider_registration *out) {
    struct fixture *f=lookup(endpoint);
    *out=(struct mem_service_provider_registration){.name="obmm",.instance=f->instance,
        .capabilities=MEM_SERVICE_PROVIDER_CAP_PEER_MAPPING,.ops=&ops,.context=endpoint};return 0;
}
int mem_service_provider_obmm_endpoint_prepare_canary_region(struct mem_service_provider_obmm_endpoint *endpoint,
    uint64_t region_len,uint64_t visible_len,uint8_t seed,struct mem_service_region *region,
    struct mem_service_provider_remote_region *remote,uint64_t *checksum) {
    assert(region_len==UFLOW_POOL_CANARY_BYTES && visible_len==UFLOW_POOL_CANARY_VISIBLE);
    struct fixture *f=lookup(endpoint);assert(!f->exported);f->exported=true;
    unsigned char bytes[4096];for(unsigned i=0;i<4096;i++)bytes[i]=(uint8_t)(seed+i*29);
    *checksum=mem_service_provider_checksum64(bytes,sizeof(bytes));
    *region=(struct mem_service_region){.len=region_len};
    memset(remote,0,sizeof(*remote));strcpy(remote->provider_name,"obmm");remote->len=region_len;
    remote->memory_kind=MEM_SERVICE_MEMORY_HOST;remote->descriptor.len=1;remote->descriptor.bytes[0]=seed;return 0;
}
int mem_service_provider_obmm_endpoint_verify_mapping(struct mem_service_provider_obmm_endpoint *endpoint,
    const struct mem_service_provider_remote_region *remote,uint64_t offset,uint64_t len,uint64_t checksum,uint64_t timeout) {
    assert(remote->len==UFLOW_POOL_CANARY_BYTES && offset==0 && len==4096 && checksum && timeout==10000);
    verifies++;if(fail_verify)return -1;lookup(endpoint)->ready=true;return 0;
}
int mem_service_provider_obmm_endpoint_close_checked(struct mem_service_provider_obmm_endpoint *endpoint) {
    struct fixture *f=lookup(endpoint);closes++;if(fail_close)return -1;
    f->exported=false;f->closed=true;return 0;
}
int main(void) {
    struct uflow_pool_platform *root=NULL,*app=NULL;struct mem_service_provider_channel channel={0};
    char descriptor[UFLOW_POOL_CANARY_HEX];uint64_t checksum;
    assert(!uflow_pool_platform_open("root",&root));
    assert(!uflow_pool_platform_prepare(root,17,descriptor,&checksum));
    assert(!uflow_pool_platform_activate(root,descriptor,checksum,&channel));assert(verifies==1);
    assert(channel.provider && channel.provider->state==MEM_SERVICE_PROVIDER_STATE_READY);
    /* A second actual endpoint must verify itself; root readiness cannot bind it. */
    assert(!uflow_pool_platform_open("app",&app));channel=(struct mem_service_provider_channel){0};
    fail_verify=true;assert(uflow_pool_platform_activate(app,descriptor,checksum,&channel));
    assert(!channel.provider);assert(verifies==2);
    fail_verify=false;assert(!uflow_pool_platform_activate(app,descriptor,checksum,&channel));assert(verifies==3);
    assert(!uflow_pool_platform_close(&app));assert(!app);
    /* Owned export is handled by checked-close, not an impossible zero-export precheck. */
    fail_close=true;struct uflow_pool_platform *original=root;
    assert(uflow_pool_platform_close(&root));assert(root==original);assert(lookup(endpoints[0].endpoint)->exported);
    fail_close=false;assert(!uflow_pool_platform_close(&root));assert(!root);assert(closes==3);
    assert(!uflow_pool_platform_close(&root));assert(!endpoints[0].exported);
    puts("platform CPU contract passed; endpoint calls are test doubles; real OBMM pending");return 0;
}
