#ifndef UFLOW_POOL_LOWER_WIRE_H
#define UFLOW_POOL_LOWER_WIRE_H
#include "pool_control.h"
#include <mem_service_wire.h>
#include <inttypes.h>
#define POOL_WIRE_MAX (MEM_SERVICE_WIRE_HEADER_LEN+MEM_SERVICE_WIRE_MAX_PAYLOAD_LEN)
static inline int pool_wire_header(const unsigned char *data,size_t n,struct mem_service_wire_header *h) {
    if(n<sizeof(*h))return 0;
    memcpy(h,data,sizeof(*h));
    if(!mem_service_wire_header_is_compatible(h) || !h->request_id || h->flags ||
       h->payload_len>MEM_SERVICE_WIRE_MAX_PAYLOAD_LEN || n>sizeof(*h)+h->payload_len)return -1;
    if(n<sizeof(*h)+h->payload_len)return 0;
    return mem_service_wire_checksum(data+sizeof(*h),h->payload_len)==h->payload_checksum?1:-1;
}
/* Parse only public newline key=value metadata. Never decode provider bytes. */
static inline int pool_wire_field(const unsigned char *data,size_t n,const char *key,char *out,size_t capacity) {
    size_t keylen=strlen(key);bool found=false;
    for(size_t i=48;i<n;) {
        size_t end=i;while(end<n && data[end]!='\n')end++;
        if(end-i>keylen && !memcmp(data+i,key,keylen) && data[i+keylen]=='=') {
            size_t len=end-i-keylen-1;
            if(found || len+1>capacity)return -1;
            memcpy(out,data+i+keylen+1,len);out[len]=0;found=true;
        }
        i=end+1;
    }
    return found?0:-1;
}
static inline int pool_wire_number(const unsigned char *data,size_t n,const char *key,uint64_t *out) {
    char value[32],*end;
    if(pool_wire_field(data,n,key,value,sizeof(value)) || !*value)return -1;
    for(char *p=value;*p;p++)if(*p<'0'||*p>'9')return -1;
    errno=0;unsigned long long v=strtoull(value,&end,10);if(errno||*end)return -1;*out=v;return 0;
}
static inline int pool_wire_own_provider(const unsigned char *data,size_t n,const char *node,uint64_t incarnation,uint64_t lease) {
    unsigned matched=0;
    for(size_t i=48;i<n;) {
        size_t end=i;while(end<n && data[end]!='\n')end++;
        if(end-i>9 && !memcmp(data+i,"provider=",9)) {
            char line[512],name[64];uint64_t inc,generation,caps,age;int consumed=0;
            if(end-i>=sizeof(line))return -1;
            memcpy(line,data+i,end-i);line[end-i]=0;errno=0;
            if(sscanf(line,"provider=%63s incarnation=%" SCNu64 " readiness_generation=%" SCNu64 " capabilities=%" SCNu64 " age_ms=%" SCNu64 "%n",
                name,&inc,&generation,&caps,&age,&consumed)!=5 || errno || line[consumed])return -1;
            if(!strcmp(name,node)) {
                if(++matched!=1 || inc!=incarnation || generation!=1 || caps!=1 || age>=lease)return -1;
            }
        }
        i=end+1;
    }
    return matched==1?0:-1;
}
#endif
