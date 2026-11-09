/* CPU-only canary double. No OBMM, device, mmap or business allocation.
 * The production agent and client are otherwise linked unchanged. */
#include "pool_platform.h"
#include "pool_control.h"
#include <stdlib.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
struct uflow_pool_platform {bool prepared,ready;struct mem_service_provider_registry registry;void *mapped;uint64_t len;};
static bool data_mode(void){const char *m=getenv("UFLOW_POOL_FIXTURE_MODE");return m&&!strcmp(m,"matrix-data");}
static int probe(void *context,enum mem_service_provider_state *out){(void)context;*out=MEM_SERVICE_PROVIDER_STATE_READY;return 0;}
static int map(void *context,const struct mem_service_mapping_request *r,struct mem_service_mapping *out){
    struct uflow_pool_platform *p=context;const char *file=getenv("UFLOW_POOL_FIXTURE_MEMORY");
    if(!data_mode()||!file||p->mapped||r->remote_descriptor.len!=1||r->remote_descriptor.bytes[0]!='M'||r->offset||r->len!=(48ULL<<20))return -1;
    int fd=open(file,(r->flags&MEM_SERVICE_MAPPING_FLAG_WRITE?O_RDWR:O_RDONLY)|O_NOFOLLOW|O_CLOEXEC);if(fd<0)return -1;
    struct stat st;if(fstat(fd,&st)||!S_ISREG(st.st_mode)||(uint64_t)st.st_size!=r->len){close(fd);return -1;}
    int prot=PROT_READ|(r->flags&MEM_SERVICE_MAPPING_FLAG_WRITE?PROT_WRITE:0);
    void *base=mmap(r->requested_address,r->len,prot,MAP_SHARED|MAP_FIXED_NOREPLACE,fd,0);close(fd);
    if(base==MAP_FAILED)return -1;p->mapped=base;p->len=r->len;
    *out=(struct mem_service_mapping){.handle=1,.base=base,.len=r->len,.memory_kind=MEM_SERVICE_MEMORY_HOST};return 0;
}
static int unmap(void *context,uint64_t handle){struct uflow_pool_platform *p=context;if(handle!=1||!p->mapped||munmap(p->mapped,p->len))return -1;p->mapped=NULL;p->len=0;return 0;}
static int visible(void *context,const struct mem_service_mapping_range_request *r,struct mem_service_visibility_completion *out){
    struct uflow_pool_platform *p=context;if(!p->mapped||r->mapping_handle!=1||r->offset>p->len||r->len>p->len-r->offset)return -1;
    uint64_t checksum=mem_service_provider_checksum64((char*)p->mapped+r->offset,r->len);
    if(checksum!=r->expected_checksum)return -1;
    *out=(struct mem_service_visibility_completion){.status=0,.visible_bytes=r->len,.checksum=checksum};return 0;
}
static const struct mem_service_provider_ops ops={.probe=probe,.map_remote_region=map,.unmap_remote_region=unmap,
    .publish_range=visible,.invalidate_range=visible,.wait_range_visible=visible};
int uflow_pool_platform_worker_bootstrap(struct uflow_pool_platform *p,unsigned node_id,
    struct uflow_pool_startup_diagnostic *d) {
    *d=(struct uflow_pool_startup_diagnostic){.stage="ready",.base=0x700000000000ULL,
        .size=256ULL<<20,.generation=1,.node_id=node_id,.node_count=2};
    const char *mode=getenv("UFLOW_POOL_FIXTURE_MODE");
    if(mode && !strncmp(mode,"bootstrap-",10)) {
        d->stage=!strcmp(mode,"bootstrap-open")?"device-open":!strcmp(mode,"bootstrap-query")?"aperture-query":"segment-enumerate";
        d->error=!strcmp(mode,"bootstrap-open")?ENOENT:!strcmp(mode,"bootstrap-query")?ENOTTY:EAGAIN;
        errno=d->error;return -1;
    }
    return !p||!p->ready||node_id>=2?-1:0;
}
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
    if(!p||(!p->prepared&&!data_mode())||p->ready||pool_unhex(descriptor,wire,sizeof(wire),&n)||
       mem_service_provider_remote_region_decode(wire,n,&r)||strcmp(r.provider_name,"cpu-fixture")||
       r.len!=UFLOW_POOL_CANARY_BYTES||r.descriptor.len!=1||checksum(r.descriptor.bytes[0])!=sum)return -1;
    if(data_mode()) {
        struct mem_service_provider_registration registration={.name="cpu-fixture",.instance="matrix-only",.capabilities=MEM_SERVICE_PROVIDER_CAP_PEER_MAPPING,.ops=&ops,.context=p};
        if(mem_service_provider_registry_init(&p->registry)||mem_service_provider_registry_register(&p->registry,&registration)||
            mem_service_provider_registry_refresh(&p->registry)||mem_service_provider_channel_bind(&p->registry,"cpu-fixture","matrix-only",MEM_SERVICE_PROVIDER_CAP_PEER_MAPPING,channel))return -1;
        p->ready=true;return 0;
    }
    /* Default startup double deliberately cannot map business memory. */
    memset(channel,0,sizeof(*channel));p->ready=true;return 0;
}
int uflow_pool_platform_close(struct uflow_pool_platform **p) {
    if(!p||(*p&&(*p)->mapped))return -1;free(*p);*p=NULL;return 0;
}
