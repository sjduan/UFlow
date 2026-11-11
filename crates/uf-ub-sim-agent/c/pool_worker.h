#ifndef UFLOW_POOL_WORKER_H
#define UFLOW_POOL_WORKER_H
#include <stdbool.h>
#include <stdint.h>
#include <sys/types.h>
struct pool_worker {
    pid_t pid;
    uint64_t birth,refresh_count,last_refresh_ms,lease_ms,started_ms;
    int logs[2],status;
    unsigned log_bytes;
    bool started,exited,stopping,deregistered;
};
int pool_worker_start(struct pool_worker *,const char *binary,const char *config);
/* 1=just reaped, 0=unchanged, -1=identity/observation failure. No diagnostics. */
int pool_worker_poll(struct pool_worker *);
int pool_worker_logs(struct pool_worker *);
int pool_worker_stop(struct pool_worker *);
bool pool_worker_alive(struct pool_worker *);
bool pool_worker_fresh(struct pool_worker *,uint64_t now);
#endif
