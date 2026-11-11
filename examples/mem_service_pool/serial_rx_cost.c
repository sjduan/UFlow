/* CPU regression only: model an expensive stdin read syscall after bootstrap.
 * Never linked/preloaded into the production static Guest agent. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

ssize_t read(int fd, void *buf, size_t count) {
    static ssize_t (*original)(int,void *,size_t);
    if(!original)original=dlsym(RTLD_NEXT,"read");
    const char *marker=getenv("UFLOW_CPU_RX_COST_MARKER");
    if(fd==0 && marker && access(marker,F_OK)==0) {
        struct timespec delay={.tv_sec=0,.tv_nsec=3000000};
        nanosleep(&delay,NULL);
    }
    return original(fd,buf,count);
}
