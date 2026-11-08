#ifndef UFLOW_POOL_CONTROL_H
#define UFLOW_POOL_CONTROL_H
#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Bounded flat JSON grammar. No escapes, arrays, payload strings or pointers. */
struct pool_field { char key[40], value[257]; bool quoted; };
struct pool_control { struct pool_field fields[24]; unsigned count; };
static inline void pool_space(const char **p) { while (**p==' '||**p=='\r'||**p=='\n'||**p=='\t') ++*p; }
static inline int pool_string(const char **p, char *out, size_t max) {
    if (*(*p)++ != '"') return -1;
    size_t n=0;
    while (**p && **p!='"') {
        unsigned char c=(unsigned char)*(*p)++;
        if (c<32 || c=='\\' || n+1>=max) return -1;
        out[n++]=(char)c;
    }
    if (*(*p)++!='"') return -1;
    out[n]=0; return 0;
}
static inline int pool_parse(const char *p, struct pool_control *c) {
    memset(c,0,sizeof(*c)); pool_space(&p); if (*p++!='{') return -1;
    for (;;) {
        pool_space(&p); if (*p=='}') {++p;break;}
        if (c->count==24) return -1;
        struct pool_field *f=&c->fields[c->count];
        if (pool_string(&p,f->key,sizeof(f->key))) return -1;
        for (unsigned i=0;i<c->count;i++) if (!strcmp(c->fields[i].key,f->key)) return -1;
        pool_space(&p);if (*p++!=':') return -1; pool_space(&p);
        if (*p=='"') {f->quoted=true;if(pool_string(&p,f->value,sizeof(f->value)))return -1;}
        else {size_t n=0;while ((*p>='0'&&*p<='9')||(*p>='a'&&*p<='z')) {
            if(n+1>=sizeof(f->value))return -1;f->value[n++]=*p++;
        }f->value[n]=0;if(!n)return -1;}
        c->count++;pool_space(&p);if(*p=='}'){++p;break;}if(*p++!=',')return -1;
    }
    pool_space(&p);return *p ? -1:0;
}
static inline const char *pool_get(const struct pool_control *c,const char *key) {
    for(unsigned i=0;i<c->count;i++)if(!strcmp(c->fields[i].key,key))return c->fields[i].value;
    return "";
}
static inline int pool_u64(const struct pool_control *c,const char *key,uint64_t *out) {
    const char *s=pool_get(c,key);char *end=NULL;
    for(unsigned i=0;i<c->count;i++)if(!strcmp(c->fields[i].key,key)&&c->fields[i].quoted)return -1;
    if(!*s)return -1;for(const char *p=s;*p;p++)if(*p<'0'||*p>'9')return -1;
    errno=0;unsigned long long n=strtoull(s,&end,10);if(errno||*end)return -1;*out=n;return 0;
}
static inline int pool_hex(const unsigned char *p,size_t n,char *out,size_t capacity) {
    static const char digits[]="0123456789abcdef";if(n>(capacity-1)/2)return -1;
    for(size_t i=0;i<n;i++){out[2*i]=digits[p[i]>>4];out[2*i+1]=digits[p[i]&15];}out[2*n]=0;return 0;
}
static inline int pool_unhex(const char *s,unsigned char *out,size_t capacity,size_t *n) {
    size_t len=strlen(s);if(len%2||len/2>capacity)return -1;
    for(size_t i=0;i<len;i+=2){unsigned v=0;for(unsigned j=0;j<2;j++){
        unsigned char c=(unsigned char)s[i+j];unsigned d=c>='0'&&c<='9'?c-'0':c>='a'&&c<='f'?c-'a'+10:16;
        if(d>15)return -1;v=v*16+d;}out[i/2]=(unsigned char)v;}
    *n=len/2;return 0;
}
#endif
