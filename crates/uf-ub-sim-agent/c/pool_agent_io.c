#define _GNU_SOURCE
#include "pool_agent_io.h"
#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* One bounded, nonblocking UART queue; no log-reader can stall control/close.
 * A stuck output fails within five seconds; no diagnostics wait for flush. */
static int output_fd;
static unsigned char queue[256*1024];
static size_t used, sent;
static uint64_t queued_at;

uint64_t pool_now_ms(void) {
    struct timespec t; clock_gettime(CLOCK_MONOTONIC,&t);
    return (uint64_t)t.tv_sec*1000+(uint64_t)t.tv_nsec/1000000;
}
int pool_output_init(int fd) {
    output_fd=fd;
    int flags=fcntl(fd,F_GETFL);
    return flags<0 || fcntl(fd,F_SETFL,flags|O_NONBLOCK)<0 ? -1:0;
}
int pool_output_flush(uint64_t now) {
    if(used && now-queued_at>=5000)return -1;
    if(sent<used) {
        ssize_t n=write(output_fd,queue+sent,used-sent);
        if(n>0)sent+=(size_t)n;
        else if(n<0 && errno!=EAGAIN && errno!=EWOULDBLOCK && errno!=EINTR)return -1;
    }
    if(sent==used){used=sent=0;queued_at=0;}
    return 0;
}
int pool_output_pending(void) {return sent<used;}
int pool_emit(const char *format, ...) {
    char frame[20000]; va_list args; va_start(args,format);
    int n=vsnprintf(frame,sizeof(frame),format,args); va_end(args);
    if(n<0 || (size_t)n>=sizeof(frame))return -1;
    if(sent){memmove(queue,queue+sent,used-sent);used-=sent;sent=0;}
    if((size_t)n>sizeof(queue)-used)return -1;
    if(!used)queued_at=pool_now_ms();
    memcpy(queue+used,frame,(size_t)n);used+=(size_t)n;
    return pool_output_flush(pool_now_ms());
}
int pool_birth(int pid,uint64_t *birth) {
    char path[64],data[4096];snprintf(path,sizeof(path),"/proc/%d/stat",pid);
    FILE *f=fopen(path,"r");if(!f)return -1;
    char *got=fgets(data,sizeof(data),f);fclose(f);if(!got)return -1;
    char *tail=strrchr(data,')'),*end;
    if(!tail)return -1;
    tail+=2;
    for(unsigned field=3;field<22;field++) {
        tail=strchr(tail,' ');if(!tail)return -1;tail++;
    }
    errno=0;unsigned long long value=strtoull(tail,&end,10);
    if(errno || !value || (*end && *end!=' '))return -1;
    *birth=value;return 0;
}
