#define _GNU_SOURCE
#include "pool_worker.h"
#include "pool_agent_io.h"
#include "pool_control.h"
#include <fcntl.h>
#include <inttypes.h>
#include <signal.h>
#include <sys/prctl.h>
#include <sys/wait.h>
#include <unistd.h>

int pool_worker_start(struct pool_worker *w,const char *binary,const char *config) {
    if(w->started)return -1;
    int pipes[2][2];
    if(pipe2(pipes[0],O_CLOEXEC|O_NONBLOCK))return -1;
    if(pipe2(pipes[1],O_CLOEXEC|O_NONBLOCK)){close(pipes[0][0]);close(pipes[0][1]);return -1;}
    pid_t parent=getpid(),pid=fork();
    if(pid<0){for(unsigned i=0;i<2;i++)for(unsigned j=0;j<2;j++)close(pipes[i][j]);return -1;}
    if(!pid) {
        if(prctl(PR_SET_PDEATHSIG,SIGTERM)||getppid()!=parent)_exit(126);
        if(dup2(pipes[0][1],1)<0 || dup2(pipes[1][1],2)<0)_exit(126);
        /* Preserve producer bytes with ordinary pipe backpressure; only the
         * parent reader/forwarder is nonblocking. Parent output/log budgets and
         * the external TERM/KILL deadline bound a stalled producer. */
        for(int fd=1;fd<=2;fd++){int flags=fcntl(fd,F_GETFL);if(flags<0 || fcntl(fd,F_SETFL,flags&~O_NONBLOCK)<0)_exit(126);}
        for(int fd=3;fd<1024;fd++)close(fd);
        execl(binary,"linqu_mem_service_provider_obmm","serve-allocations","--config",config,NULL);
        const char why[]="worker exec failed\n";(void)write(2,why,sizeof(why)-1);_exit(127);
    }
    for(unsigned i=0;i<2;i++){close(pipes[i][1]);w->logs[i]=pipes[i][0];}
    w->pid=pid;w->started=true;w->started_ms=pool_now_ms();
    if(pool_birth(pid,&w->birth))return -1;
    return 0;
}
int pool_worker_poll(struct pool_worker *w) {
    if(!w->started || w->exited)return 0;
    pid_t got=waitpid(w->pid,&w->status,WNOHANG);
    if(got==w->pid){w->exited=true;return 1;}
    if(got<0 && errno!=EINTR)return -1;
    uint64_t current;
    if(pool_birth(w->pid,&current) || current!=w->birth)return -1;
    return 0;
}
bool pool_worker_alive(struct pool_worker *w) {
    uint64_t current;
    return w->started && !w->exited && !pool_birth(w->pid,&current) && current==w->birth;
}
bool pool_worker_fresh(struct pool_worker *w,uint64_t now) {
    return pool_worker_alive(w) && !w->stopping && w->refresh_count && w->lease_ms &&
        now>=w->last_refresh_ms && now-w->last_refresh_ms<w->lease_ms;
}
int pool_worker_logs(struct pool_worker *w) {
    /* At most 2*512 bytes per event-loop tick; logs cannot starve metadata. */
    for(unsigned i=0;i<2;i++)if(w->logs[i]>=0) {
        unsigned char bytes[512];ssize_t n=read(w->logs[i],bytes,sizeof(bytes));
        if(n==0){close(w->logs[i]);w->logs[i]=-1;continue;}
        if(n<0){if(errno==EAGAIN || errno==EWOULDBLOCK || errno==EINTR)continue;return -1;}
        w->log_bytes+=(unsigned)n;
        if(w->log_bytes>256*1024)return -1;
        char hex[1025];if(pool_hex(bytes,(size_t)n,hex,sizeof(hex)))return -1;
        if(pool_emit("UFPOOL_WORKER_LOG %u %" PRIu64 " %s %s\n",(unsigned)w->pid,w->birth,i?"stderr":"stdout",hex))return -1;
    }
    return 0;
}
int pool_worker_stop(struct pool_worker *w) {
    if(!pool_worker_alive(w) || w->stopping)return -1;
    /* This unreaped direct child cannot be PID-reused; check its birth again
     * before the exact signal. Never scan/kill by process name. */
    w->stopping=true;
    return kill(w->pid,SIGTERM);
}
