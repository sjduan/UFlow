/* CPU-only canary double. No OBMM, device, mmap or business allocation.
 * The production agent and client are otherwise linked unchanged. */
#include "pool_platform.h"
#include "pool_control.h"
#include <stdlib.h>
struct uflow_pool_platform {bool prepared,ready;};
static uint64_t checksum(uint8_t seed) {
    uint8_t bytes[4096];for(unsigned i=0;i<4096;i++)bytes[i]=(uint8_t)(seed+i*29);
    return mem_service_provider_checksum64(bytes,sizeof(bytes));
}
int uflow_pool_platform_open(const char *instance,struct uflow_pool_platform **out) {
    if(!instance||!out||*out)return -1;*out=calloc(1,sizeof(**out));return *out?0:-1;
}
int uflow_pool_platform_prepare(struct uflow_pool_platform *p,uint8_t seed,
    char descriptor[UFLOW_POOL_CANARY_HEX],uint64_t *sum) {
    if(!p||p->prepared)return -1;
    struct mem_service_provider_remote_region r={0};
    strcpy(r.provider_name,"cpu-fixture");r.len=UFLOW_POOL_CANARY_BYTES;
    r.memory_kind=MEM_SERVICE_MEMORY_HOST;r.descriptor.len=1;r.descriptor.bytes[0]=seed;
    unsigned char wire[MEM_SERVICE_PROVIDER_REGION_WIRE_MAX_LEN];size_t n;
    if(mem_service_provider_remote_region_encode(&r,wire,sizeof(wire),&n)||pool_hex(wire,n,descriptor,UFLOW_POOL_CANARY_HEX))return -1;
    p->prepared=true;*sum=checksum(seed);return 0;
}
int uflow_pool_platform_activate(struct uflow_pool_platform *p,const char *descriptor,
    uint64_t sum,struct mem_service_provider_channel *channel) {
    unsigned char wire[MEM_SERVICE_PROVIDER_REGION_WIRE_MAX_LEN];size_t n;
    struct mem_service_provider_remote_region r;
    if(!p||!p->prepared||p->ready||pool_unhex(descriptor,wire,sizeof(wire),&n)||
       mem_service_provider_remote_region_decode(wire,n,&r)||strcmp(r.provider_name,"cpu-fixture")||
       r.len!=UFLOW_POOL_CANARY_BYTES||r.descriptor.len!=1||checksum(r.descriptor.bytes[0])!=sum)return -1;
    /* This double deliberately cannot bind/map business memory. */
    memset(channel,0,sizeof(*channel));p->ready=true;return 0;
}
int uflow_pool_platform_close(struct uflow_pool_platform **p) {
    if(!p)return -1;free(*p);*p=NULL;return 0;
}
