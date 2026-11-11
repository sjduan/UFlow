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
        /* Managed publish takes the opaque provider descriptor, not the
         * canary remote-region envelope. The neutral SDK builds that envelope. */
        const uint8_t descriptor='M';
        return mem_service_client_publish_allocation(client,work->key,node,inc,work->generation,&descriptor,1,
            UINT64_C(0x5000000000),work->size_bytes,work,status);
    }
    if(!strcmp(work->state,"retiring")){
        if(unlink(file))return -1;
        return mem_service_client_reclaim_allocation(client,work->key,node,inc,work->generation,true,work,status);
    }
    return -1;
}
