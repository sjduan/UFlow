/* Explicit CPU-only shared-file provider for the full matrix fixture. This is
 * never linked into the production agent or official frozen worker. */
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
static bool fixture_data(void){const char *m=getenv("UFLOW_POOL_FIXTURE_MODE");return m&&!strcmp(m,"matrix-data");}
static int fixture_publish(struct mem_service_client *client,const char *node,uint64_t inc,
                           struct mem_service_client_allocation *work,enum mem_service_wire_status *status){
    const char *file=getenv("UFLOW_POOL_FIXTURE_MEMORY");if(!file||!fixture_data())return -1;
    if(!strcmp(work->state,"allocating")){
        if(!work->size_bytes||work->size_bytes>(48ULL<<20))return -1;
        int fd=open(file,O_RDWR|O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC,0600);if(fd<0)return -1;
        int rc=ftruncate(fd,(off_t)work->size_bytes);close(fd);if(rc)return -1;
        struct mem_service_provider_remote_region r={0};strcpy(r.provider_name,"cpu-fixture");r.len=work->size_bytes;
        r.memory_kind=MEM_SERVICE_MEMORY_HOST;r.descriptor.len=1;r.descriptor.bytes[0]='M';
        unsigned char descriptor[MEM_SERVICE_PROVIDER_REGION_WIRE_MAX_LEN];size_t n;
        if(mem_service_provider_remote_region_encode(&r,descriptor,sizeof(descriptor),&n))return -1;
        return mem_service_client_publish_allocation(client,work->key,node,inc,work->generation,descriptor,(uint32_t)n,
            UINT64_C(0x5000000000),work->size_bytes,work,status);
    }
    if(!strcmp(work->state,"retiring")){
        if(unlink(file))return -1;
        return mem_service_client_reclaim_allocation(client,work->key,node,inc,work->generation,true,work,status);
    }
    return -1;
}
