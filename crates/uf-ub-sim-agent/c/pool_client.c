#define _GNU_SOURCE
#include "pool_control.h"
#include "pool_platform.h"
#include <mem_service_mapping_owner.h>
#include <inttypes.h>
#include <time.h>
#include <unistd.h>

/* One process/SDK context per application client, plus a metadata-only owner.
 * Never inspect the opaque lower provider implementation or serialize a VA. */
struct context {
    struct mem_service_client client;
    struct mem_service_provider_channel channel;
    struct uflow_pool_platform *platform;
    struct mem_service_access_domain *domain;
    struct mem_service_mapping_owner *owner;
    struct mem_service_client_allocation allocation;
    struct mem_service_client_object_mapping mapping;
    struct mem_service_client_mapping_lifecycle lifecycle;
    char session[64], node[64];
    char owner_release_operation[80], retire_operation[80];
    uint64_t incarnation, sequence;
    bool holder, writable;
};

static uint64_t now_ms(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC,&ts);
    return (uint64_t)ts.tv_sec*1000+(uint64_t)ts.tv_nsec/1000000;
}
static void operation(struct context *c,char id[80],const char *kind) {
    snprintf(id,80,"%s-%s-%" PRIu64,c->session,kind,++c->sequence);
}
static uint8_t pattern(uint64_t seed,uint64_t index) {
    return (uint8_t)((index*UINT64_C(1315423911)+seed*UINT64_C(2654435761))^
                     ((index>>7)*17)^(index>>19));
}
static uint64_t pattern_checksum(uint64_t seed,uint64_t off,uint64_t n) {
    uint64_t h=UINT64_C(1469598103934665603);
    for(uint64_t i=0;i<n;i++)h=(h^pattern(seed,off+i))*UINT64_C(1099511628211);
    return h;
}

struct access_job { bool write; uint64_t offset,nbytes,pattern,checksum; };
static int access_data(void *data,const struct mem_service_provider_channel *channel,
    const struct mem_service_client_object_mapping *mapping,
    const struct mem_service_client_mapping_lifecycle *lifecycle,
    const struct mem_service_mapping_access_view *view)
{
    (void)lifecycle;
    struct access_job *job=data;
    struct mem_service_visibility_completion completion;
    const uint64_t flags=MEM_SERVICE_MAPPING_FLAG_READ|MEM_SERVICE_MAPPING_FLAG_WRITE;
    if(view->offset!=job->offset || view->len!=job->nbytes ||
       (view->flags & ~flags) || !view->base) return -1;
    job->checksum=pattern_checksum(job->pattern,job->offset,job->nbytes);
    if(!job->write &&
       (mem_service_provider_channel_invalidate_range(channel,&mapping->binding,
             job->offset,job->nbytes,job->checksum,&completion) ||
        mem_service_provider_channel_wait_range_visible(channel,&mapping->binding,
             job->offset,job->nbytes,job->checksum,10000,&completion))) return -1;
    /* The SDK's bounded view.base already points to this range, not the pool base. */
    uint8_t *bytes=view->base;
    if(job->write)for(uint64_t i=0;i<job->nbytes;i++)bytes[i]=pattern(job->pattern,job->offset+i);
    for(uint64_t i=0;i<job->nbytes;i++)if(bytes[i]!=pattern(job->pattern,job->offset+i))return -1;
    if(mem_service_provider_checksum64(bytes,job->nbytes)!=job->checksum)return -1;
    if(job->write && mem_service_provider_channel_publish_range(channel,&mapping->binding,
         job->offset,job->nbytes,job->checksum,&completion))return -1;
    return completion.status==0 && completion.visible_bytes==job->nbytes && completion.checksum==job->checksum?0:-1;
}

static int wait_state(struct context *c,const char *key,const char *state) {
    uint64_t deadline=now_ms()+25000;
    do {
        enum mem_service_wire_status status;
        struct mem_service_client_allocation view;
        if(mem_service_client_inspect_allocation(&c->client,key,&view,&status) || status!=MEM_SERVICE_WIRE_STATUS_OK)return -1;
        if(!strcmp(view.state,state)){c->allocation=view;return 0;}
        if(!strcmp(view.state,"quarantined"))return -1;
        usleep(10000);
    }while(now_ms()<deadline);
    return -1;
}

static int execute(struct context *c,const struct pool_control *req,char reply[4096]) {
    const char *op=pool_get(req,"op");enum mem_service_wire_status status;
    char id[80];uint64_t n=0,generation=0;
    if(!strcmp(op,"allocate")) {
        const char *key=pool_get(req,"pool_id");
        if(!*key||pool_u64(req,"nbytes",&n)||!n||n>(128ULL<<20)||n%65536)return -1;
        if(snprintf(c->session,sizeof(c->session),"owner-%s",key)>=(int)sizeof(c->session))return -1;
        operation(c,id,"allocate");
        struct mem_service_client_allocate request={.key=key,.idempotency_key=id,.session_id=c->session,
            .size_bytes=n,.alignment_bytes=65536,.capabilities=1};
        c->holder=true; /* Uncertain allocate already carries the named responsibility. */
        if(mem_service_client_allocate_object_at_home(&c->client,&request,c->node,&c->allocation,&status) || status!=MEM_SERVICE_WIRE_STATUS_OK)return -1;
        if(wait_state(c,key,"active"))return -1;
        snprintf(reply,4096,"{\"schema\":2,\"phase\":\"completed\",\"key\":\"%s\",\"state\":\"active\",\"generation\":%" PRIu64 ",\"incarnation\":%" PRIu64 ",\"nbytes\":%" PRIu64 "}",key,c->allocation.generation,c->allocation.provider_incarnation,c->allocation.size_bytes);
        return 0;
    }
    if(!strcmp(op,"map")) {
        const char *key=pool_get(req,"key"),*session=pool_get(req,"client_session"),*access=pool_get(req,"access");
        if(c->holder||c->owner||!*session||strlen(session)>=sizeof(c->session)||pool_u64(req,"generation",&generation)||!generation)return -1;
        if(strcmp(access,"r")&&strcmp(access,"rw"))return -1;
        strcpy(c->session,session);c->writable=!strcmp(access,"rw");operation(c,id,"acquire");
        c->holder=true;
        if(mem_service_client_acquire_object_at_node(&c->client,key,id,c->session,c->node,c->incarnation,
             true,generation,&c->allocation,&status)||status!=MEM_SERVICE_WIRE_STATUS_OK)return -1;
        if(uflow_pool_platform_open(c->session,&c->platform,&c->channel))return -1;
        operation(c,id,"map");
        uint64_t rights=MEM_SERVICE_CLIENT_MAP_READ|(c->writable?MEM_SERVICE_CLIENT_MAP_WRITE:0);
        if(mem_service_client_map_managed_allocation(&c->client,&c->channel,&c->allocation,c->session,id,
              rights,&c->mapping,&c->lifecycle,&status)||status!=MEM_SERVICE_WIRE_STATUS_OK)return -1;
        if(mem_service_access_domain_create(&c->client,&c->channel,&c->domain))return -1;
        operation(c,id,"release");
        if(mem_service_mapping_owner_adopt(c->domain,&c->mapping,&c->lifecycle,id,8,&c->owner,&status)||status!=MEM_SERVICE_WIRE_STATUS_OK)return -1;
        snprintf(reply,4096,"{\"schema\":2,\"phase\":\"completed\",\"mapped\":true,\"holder\":true,\"client_session\":\"%s\",\"generation\":%" PRIu64 ",\"nbytes\":%" PRIu64 ",\"guest_pid\":%u,\"pins\":0}",c->session,c->allocation.generation,c->allocation.size_bytes,(unsigned)getpid());
        return 0;
    }
    if(!strcmp(op,"write")||!strcmp(op,"read")) {
        struct access_job job={.write=!strcmp(op,"write")};
        if(!c->owner||strcmp(pool_get(req,"client_session"),c->session)||
           pool_u64(req,"offset",&job.offset)||pool_u64(req,"nbytes",&job.nbytes)||
           pool_u64(req,"pattern_id",&job.pattern)||!job.nbytes||job.nbytes>(20ULL<<20)||
           job.offset>c->allocation.size_bytes||job.nbytes>c->allocation.size_bytes-job.offset||
           (job.write&&!c->writable))return -1;
        struct mem_service_mapping_lease *pin=NULL;struct mem_service_mapping_access_view view;
        uint64_t rights=MEM_SERVICE_MAPPING_FLAG_READ|(job.write?MEM_SERVICE_MAPPING_FLAG_WRITE:0);
        if(mem_service_mapping_owner_acquire(c->owner,MEM_SERVICE_MAPPING_ACCESS_CPU,job.offset,job.nbytes,rights,&pin,&view))return -1;
        int rc=mem_service_mapping_lease_apply(pin,access_data,&job);
        int release=mem_service_mapping_lease_release(pin);
        if(rc||release)return -1; /* A failed release never becomes a successful zero-pin receipt. */
        struct mem_service_mapping_owner_stats stats;
        if(mem_service_mapping_owner_inspect(c->owner,&stats)||stats.cpu_pins||stats.compute_pins||stats.in_flight_pins)return -1;
        snprintf(reply,4096,"{\"schema\":2,\"phase\":\"completed\",\"offset\":%" PRIu64 ",\"nbytes\":%" PRIu64 ",\"pattern_id\":%" PRIu64 ",\"checksum\":\"%016" PRIx64 "\",\"all_bytes\":true,\"visible\":true,\"pins\":0}",job.offset,job.nbytes,job.pattern,job.checksum);
        return 0;
    }
    if(!strcmp(op,"unmap")) {
        if(strcmp(pool_get(req,"client_session"),c->session))return -1;
        if(c->owner) {
            if(mem_service_mapping_owner_close(c->owner,&status)||status!=MEM_SERVICE_WIRE_STATUS_OK)return -1;
            c->holder=false;
            if(mem_service_mapping_owner_destroy(c->owner))return -1;c->owner=NULL;
        } else {
            if(c->lifecycle.pending && (mem_service_client_unmap_managed_allocation(&c->client,&c->channel,&c->mapping,&c->lifecycle,&status)||status!=MEM_SERVICE_WIRE_STATUS_OK))return -1;
            if(c->holder) {
                operation(c,id,"release");
                if(mem_service_client_release_object(&c->client,c->allocation.key,id,c->session,true,c->allocation.generation,&c->allocation,&status)||status!=MEM_SERVICE_WIRE_STATUS_OK)return -1;
                c->holder=false;
            }
        }
        if(c->domain){if(mem_service_access_domain_destroy(c->domain))return -1;c->domain=NULL;}
        if(uflow_pool_platform_close(&c->platform))return -1;
        snprintf(reply,4096,"{\"schema\":2,\"phase\":\"completed\",\"client_session\":\"%s\",\"unmapped\":true,\"holder\":false,\"pins\":0,\"cleanup_required\":false}",c->session);return 0;
    }
    if(!strcmp(op,"retire")) {
        const char *key=pool_get(req,"key");
        if(pool_u64(req,"generation",&generation)||generation!=c->allocation.generation||strcmp(key,c->allocation.key)||c->owner)return -1;
        if(c->holder){if(!*c->owner_release_operation)operation(c,c->owner_release_operation,"owner-release");
            if(mem_service_client_release_object(&c->client,key,c->owner_release_operation,c->session,true,generation,&c->allocation,&status)||status!=MEM_SERVICE_WIRE_STATUS_OK)return -1;
            c->holder=false;}
        if(!*c->retire_operation)operation(c,c->retire_operation,"retire");
        if(mem_service_client_retire_object(&c->client,key,c->retire_operation,true,generation,&c->allocation,&status)||status!=MEM_SERVICE_WIRE_STATUS_OK||wait_state(c,key,"retired"))return -1;
        struct mem_service_client_allocation_stats stats;
        if(mem_service_client_allocation_stats(&c->client,&stats,&status)||status!=MEM_SERVICE_WIRE_STATUS_OK)return -1;
        if(stats.backing_allocated_bytes||stats.address_reserved_bytes||stats.export_mappings||stats.import_mappings||
           stats.live_objects||stats.live_refs||stats.in_flight||stats.quarantined_bytes||stats.quarantined_objects)return -1;
        snprintf(reply,4096,"{\"schema\":2,\"phase\":\"completed\",\"state\":\"retired\",\"backing_bytes\":0,\"address_reserved_bytes\":0,\"export_mappings\":0,\"import_mappings\":0,\"live_refs\":0,\"in_flight\":0,\"quarantined_bytes\":0}");return 0;
    }
    if(!strcmp(op,"inspect")) {
        struct mem_service_mapping_owner_stats stats={0};
        if(c->owner&&mem_service_mapping_owner_inspect(c->owner,&stats))return -1;
        snprintf(reply,4096,"{\"schema\":2,\"phase\":\"completed\",\"contexts\":%u,\"pins\":%u,\"cleanup_required\":%s}",
            (unsigned)(c->holder||c->owner||c->lifecycle.pending),stats.cpu_pins+stats.compute_pins+stats.in_flight_pins,
            c->lifecycle.pending||stats.mapping_pending||stats.holder_pending?"true":"false");return 0;
    }
    return -1;
}

int uflow_pool_client(int fd,const char *connect,const char *node,uint64_t incarnation) {
    struct context c={0};strncpy(c.node,node,sizeof(c.node)-1);c.incarnation=incarnation;
    struct mem_service_wire_client_options options;mem_service_wire_client_options_init(&options);
    options.timeout_ms=10000;options.max_attempts=1;
    mem_service_client_init_with_options(&c.client,connect,&options);
    FILE *input=fdopen(dup(fd),"r"),*output=fdopen(fd,"w");if(!input||!output)return 2;setvbuf(output,NULL,_IOLBF,0);
    char line[4096],reply[4096];
    while(fgets(line,sizeof(line),input)) {
        struct pool_control req;uint64_t schema=0;
        if(!strchr(line,'\n')||pool_parse(line,&req)||pool_u64(&req,"schema",&schema)||schema!=2||execute(&c,&req,reply)) {
            fprintf(output,"UFPOOL_FAILURE_NOTICE\n");fflush(output);
            snprintf(reply,sizeof(reply),"{\"schema\":2,\"phase\":\"unknown\",\"reason\":\"SDK operation unconfirmed; retain same context\",\"errno\":%u,\"holder_pending\":%s,\"mapping_pending\":%s,\"owner_present\":%s}",
                (unsigned)errno,c.holder?"true":"false",c.lifecycle.pending?"true":"false",c.owner?"true":"false");
        }
        fprintf(output,"%s\n",reply);fflush(output);
    }
    /* No destructor fabricates unmap/release on a disconnected command pipe. */
    return c.holder||c.owner||c.lifecycle.pending||c.platform?3:0;
}
