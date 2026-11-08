#define _GNU_SOURCE
#include "pool_control.h"
#include <mem_service_wire.h>
#include <fcntl.h>
#include <inttypes.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <termios.h>
#include <time.h>
#include <unistd.h>

int uflow_pool_client(int,const char *,const char *,uint64_t);
#define SLOTS 5
#define RELAYS 32
#define WIRE_MAX (MEM_SERVICE_WIRE_HEADER_LEN+MEM_SERVICE_WIRE_MAX_PAYLOAD_LEN)
struct child {pid_t pid;int fd;char session[64];bool obligation,canary;char reply[4096];size_t used;};
struct relay {int fd;uint64_t id,deadline;unsigned char input[WIRE_MAX],output[WIRE_MAX];size_t in,out,sent;bool requested;};
struct replay {uint64_t request,correlation,hash;char reply[8400];};
static struct child clients[SLOTS];static struct relay relays[RELAYS];static struct replay cache[64];
static char node[64],boot[96];static uint64_t incarnation,high_request,relay_sequence;static pid_t worker;
static uint64_t now_ms(void){struct timespec t;clock_gettime(CLOCK_MONOTONIC,&t);return (uint64_t)t.tv_sec*1000+t.tv_nsec/1000000;}
static uint64_t hash(const unsigned char *p,size_t n){uint64_t h=0xcbf29ce484222325ULL;for(size_t i=0;i<n;i++)h=(h^p[i])*0x100000001b3ULL;return h;}
static int fail(const char *why){
    /* First externally observable notice precedes any diagnostic/cleanup. Host
     * bridge immediately writes its inherited monitor FD on this marker. */
    puts("UFPOOL_FATAL");fflush(stdout);fprintf(stderr,"pool agent: %s\n",why);return 2;
}
static int exact_write(int fd,const void *p,size_t n){const char *s=p;while(n){ssize_t k=write(fd,s,n);if(k<0&&errno==EINTR)continue;if(k<=0)return -1;s+=k;n-=(size_t)k;}return 0;}
static int create_client(unsigned slot,const char *session){
    if(slot>=SLOTS||clients[slot].pid||strlen(session)>=64)return -1;
    int fds[2];if(socketpair(AF_UNIX,SOCK_STREAM|SOCK_CLOEXEC,0,fds))return -1;
    pid_t pid=fork();if(pid<0)return -1;
    if(!pid){
        close(fds[0]);
        for(int fd=0;fd<1024;fd++)if(fd!=fds[1]&&fd!=2)close(fd);
        int sink=open("/dev/null",O_RDWR);if(sink<0)_exit(2);
        if(dup2(sink,0)<0||dup2(sink,1)<0)_exit(2);if(sink>2&&sink!=fds[1])close(sink);
        _exit(uflow_pool_client(fds[1],"unix:/tmp/uflow-pool-lower.sock",node,incarnation));
    }
    close(fds[1]);clients[slot].pid=pid;clients[slot].fd=fds[0];strcpy(clients[slot].session,session);return 0;
}
static int start_worker(void){
    if(worker)return -1;
    FILE *f=fopen("/tmp/uflow-pool-worker.conf","wx");if(!f)return -1;
    fprintf(f,"connect=unix:/tmp/uflow-pool-lower.sock\nnode_id=%s\nstate_file=/tmp/uflow-pool-worker.state\nincarnation=%" PRIu64 "\nreadiness_generation=1\nallocation_granularity_bytes=2097152\nfast_allocation=0\n",node,incarnation);fclose(f);
    worker=fork();if(worker<0)return -1;
    if(!worker){int log=open("/tmp/uflow-pool-worker.log",O_WRONLY|O_CREAT|O_EXCL,0600);if(log<0)_exit(2);
        dup2(log,1);dup2(log,2);for(int fd=3;fd<1024;fd++)close(fd);
        execl("/bin/linqu_mem_service_provider_obmm","linqu_mem_service_provider_obmm","serve-allocations","--config","/tmp/uflow-pool-worker.conf",NULL);_exit(2);}
    return create_client(0,"owner");
}
static void identity(void){printf("UFLOW_ENV_IDENTITY {\"node\":\"%s\",\"boot_id\":\"%s\",\"engine\":\"initramfs\",\"machine\":\"aarch64\",\"uid\":%u,\"agent_session\":\"%s\",\"incarnation\":%" PRIu64 "}\n",node,boot,(unsigned)getuid(),boot,incarnation);fflush(stdout);}
static int relay_allowed(const unsigned char *data,size_t n){
    if(n<MEM_SERVICE_WIRE_HEADER_LEN)return 0;
    struct mem_service_wire_header h;memcpy(&h,data,sizeof(h));
    if(!mem_service_wire_header_is_compatible(&h)||h.payload_len>MEM_SERVICE_WIRE_MAX_PAYLOAD_LEN||h.flags||h.status||h.error_code)return -1;
    /* Lifecycle/directory metadata only. No PUT/GET/materialize/reference data. */
    if(!(h.operation==1||h.operation==2||h.operation==3||(h.operation>=0x70&&h.operation<=0x7d)))return -1;
    return n==sizeof(h)+h.payload_len?1:0;
}
int main(void){
    for(unsigned i=0;i<SLOTS;i++)clients[i].fd=-1;
    for(unsigned i=0;i<RELAYS;i++)relays[i].fd=-1;
    FILE *f=fopen("/proc/cmdline","r");char cmd[4096];if(!f||!fgets(cmd,sizeof(cmd),f))return fail("cmdline");fclose(f);
    const char *p=strstr(cmd,"uflow_node=");if(!p||sscanf(p,"uflow_node=%63s",node)!=1|| (strcmp(node,"nodeA")&&strcmp(node,"nodeB")))return fail("node identity");
    f=fopen("/proc/sys/kernel/random/boot_id","r");if(!f||!fgets(boot,sizeof(boot),f))return fail("boot identity");fclose(f);boot[strcspn(boot,"\r\n")]=0;
    incarnation=hash((unsigned char*)boot,strlen(boot));if(!incarnation)incarnation=1;
    struct termios terminal;if(!tcgetattr(0,&terminal)){cfmakeraw(&terminal);tcsetattr(0,TCSANOW,&terminal);}
    int listener=socket(AF_UNIX,SOCK_STREAM|SOCK_CLOEXEC,0);struct sockaddr_un address={.sun_family=AF_UNIX};strcpy(address.sun_path,"/tmp/uflow-pool-lower.sock");
    if(listener<0||bind(listener,(void*)&address,sizeof(address))||chmod(address.sun_path,0600)||listen(listener,32))return fail("private lower relay");
    char input[12000];size_t input_used=0;int active=-1;uint64_t request_id=0,correlation=0,request_hash=0;bool quitting=false,drained=false;uint64_t quit_deadline=0;
    identity();
    for(;;){
        struct pollfd polls[2+SLOTS+RELAYS];polls[0]=(struct pollfd){.fd=0,.events=POLLIN};polls[1]=(struct pollfd){.fd=listener,.events=POLLIN};
        for(unsigned i=0;i<SLOTS;i++)polls[2+i]=(struct pollfd){.fd=clients[i].fd,.events=POLLIN};
        for(unsigned i=0;i<RELAYS;i++)polls[2+SLOTS+i]=(struct pollfd){.fd=relays[i].fd,.events=(short)(POLLIN|(relays[i].out>relays[i].sent?POLLOUT:0))};
        if(poll(polls,2+SLOTS+RELAYS,100)<0&&errno!=EINTR)return fail("poll");
        if(polls[1].revents&POLLIN){
            unsigned slot=0;while(slot<RELAYS&&relays[slot].fd>=0)slot++;
            if(slot==RELAYS)return fail("lower relay bound");
            int fd=accept4(listener,NULL,NULL,SOCK_CLOEXEC|SOCK_NONBLOCK);if(fd<0)return fail("relay accept");
            relays[slot]=(struct relay){.fd=fd,.id=++relay_sequence,.deadline=now_ms()+30000};
        }
        for(unsigned i=0;i<RELAYS;i++)if(relays[i].fd>=0){
            struct relay *r=&relays[i];short events=polls[2+SLOTS+i].revents;
            if(now_ms()>r->deadline)return fail("lower RPC deadline; responsibility unknown");
            if((events&POLLIN)&&!r->requested){
                ssize_t n=read(r->fd,r->input+r->in,sizeof(r->input)-r->in);
                if(n<=0)return fail("lower request EOF");r->in+=(size_t)n;
                int ready=relay_allowed(r->input,r->in);if(ready<0)return fail("non-lifecycle wire operation");
                if(ready){char hex[WIRE_MAX*2+1];pool_hex(r->input,r->in,hex,sizeof(hex));printf("UFPOOL_RPC %" PRIu64 " %s\n",r->id,hex);fflush(stdout);r->requested=true;}
            }
            if(events&POLLOUT){
                ssize_t n=write(r->fd,r->output+r->sent,r->out-r->sent);if(n>0)r->sent+=(size_t)n;
                if(r->sent>=sizeof(struct mem_service_wire_header)){
                    struct mem_service_wire_header h;memcpy(&h,r->output,sizeof(h));
                    if(!mem_service_wire_header_is_compatible(&h)||h.payload_len>4096)return fail("lower response header");
                    if(r->sent==sizeof(h)+h.payload_len){close(r->fd);r->fd=-1;}
                }
            }
        }
        for(unsigned i=0;i<SLOTS;i++)if(clients[i].fd>=0&&(polls[2+i].revents&POLLIN)){
            struct child *c=&clients[i];ssize_t n=read(c->fd,c->reply+c->used,sizeof(c->reply)-1-c->used);
            if(n<=0)return fail("SDK context EOF");c->used+=(size_t)n;c->reply[c->used]=0;
            char *end;
            while((end=strchr(c->reply,'\n'))){
                size_t consumed=(size_t)(end-c->reply)+1;
                *end=0;
                if(!strcmp(c->reply,"UFPOOL_FAILURE_NOTICE")) {
                    puts("UFPOOL_FAILURE_NOTICE");fflush(stdout);
                    memmove(c->reply,c->reply+consumed,c->used-consumed);c->used-=consumed;c->reply[c->used]=0;
                    continue;
                }
                if(active!=(int)i)return fail("unexpected SDK response");*end=0;
                struct pool_control response;if(pool_parse(c->reply,&response))return fail("SDK response metadata");
                if(!strcmp(pool_get(&response,"phase"),"completed")){
                    if(!strcmp(pool_get(&response,"canary_prepared"),"true"))c->canary=true;
                    if(!strcmp(pool_get(&response,"canary_closed"),"true"))c->canary=false;
                    if(!strcmp(pool_get(&response,"mapped"),"true")||!strcmp(pool_get(&response,"state"),"active"))c->obligation=true;
                    if(!strcmp(pool_get(&response,"unmapped"),"true")||!strcmp(pool_get(&response,"state"),"retired"))c->obligation=false;
                }else c->obligation=true;
                char hex[8400];pool_hex((unsigned char*)c->reply,strlen(c->reply),hex,sizeof(hex));
                struct replay *saved=&cache[request_id%64];saved->request=request_id;saved->correlation=correlation;saved->hash=request_hash;strcpy(saved->reply,hex);
                printf("UFPOOL_REPLY %" PRIu64 " %" PRIu64 " %s\n",request_id,correlation,hex);fflush(stdout);
                memmove(c->reply,c->reply+consumed,c->used-consumed);c->used-=consumed;c->reply[c->used]=0;
                active=-1;
            }
        }
        if(polls[0].revents&POLLIN){
            char byte;ssize_t n=read(0,&byte,1);if(n<=0)return fail("host control EOF");
            if(byte!='\n'){if(input_used+1>=sizeof(input))return fail("control line limit");input[input_used++]=byte;continue;}
            input[input_used]=0;input_used=0;
            if(!strcmp(input,"identity")){identity();continue;}
            if(!strcmp(input,"sdk-check")){puts("UFLOW_ENV_SDK_OK");fflush(stdout);continue;} /* ABI compiled; business health is checked after start. */
            if(!strcmp(input,"start")){if(start_worker())return fail("managed worker start");puts("UFPOOL_STARTED");fflush(stdout);continue;}
            if(!strcmp(input,"quit")){
                if(!drained)return fail("prepare-quit required before QMP handoff");
                close(listener);puts("UFLOW_ENV_DAEMON_CLOSED");fflush(stdout);sleep(30);return fail("QMP close deadline");
            }
            if(!strcmp(input,"prepare-quit")){
                for(unsigned i=0;i<SLOTS;i++)if(clients[i].obligation||clients[i].canary)return fail("quit with lower obligation");
                if(active>=0)return fail("quit while operation active");
                quitting=true;quit_deadline=now_ms()+9000;
                for(unsigned i=0;i<SLOTS;i++)if(clients[i].fd>=0){close(clients[i].fd);clients[i].fd=-1;}
                if(worker&&kill(worker,SIGTERM))return fail("worker stop");continue;
            }
            if(!strncmp(input,"wire ",5)){
                uint64_t id=0;char hex[4096];if(sscanf(input,"wire %" SCNu64 " %4095s",&id,hex)!=2)return fail("wire reply syntax");
                unsigned i=0;while(i<RELAYS&&(relays[i].fd<0||relays[i].id!=id))i++;if(i==RELAYS)return fail("unknown relay ID");
                struct relay *r=&relays[i];size_t count;
                if(pool_unhex(hex,r->output+r->out,sizeof(r->output)-r->out,&count))return fail("wire reply bounds");r->out+=count;continue;
            }
            if(!strncmp(input,"v2 ",3)){
                char hex[8192],json[4096];size_t count=0;
                if(active>=0||quitting||sscanf(input,"v2 %" SCNu64 " %" SCNu64 " %8191s",&request_id,&correlation,hex)!=3||!request_id||!correlation||
                   pool_unhex(hex,(unsigned char*)json,sizeof(json)-1,&count))return fail("v2 control syntax");json[count]=0;
                struct pool_control req;uint64_t schema;if(pool_parse(json,&req)||pool_u64(&req,"schema",&schema)||schema!=2)return fail("v2 body");
                request_hash=hash((unsigned char*)json,count);struct replay *old=&cache[request_id%64];
                if(old->request==request_id){if(old->hash!=request_hash||old->correlation!=correlation)return fail("replay mutation");printf("UFPOOL_REPLY %" PRIu64 " %" PRIu64 " %s\n",request_id,correlation,old->reply);fflush(stdout);continue;}
                if(request_id<=high_request&&high_request-request_id>=64)return fail("retired request replay");if(request_id>high_request)high_request=request_id;
                const char *op=pool_get(&req,"op"),*session=pool_get(&req,"client_session");
                if(!strcmp(op,"inspect")){
                    unsigned count=0;for(unsigned i=0;i<SLOTS;i++)count+=clients[i].obligation+clients[i].canary;
                    char result[256],encoded[512];snprintf(result,sizeof(result),"{\"schema\":2,\"phase\":\"completed\",\"contexts\":%u,\"pins\":0,\"cleanup_required\":%s}",count,count?"true":"false");pool_hex((unsigned char*)result,strlen(result),encoded,sizeof(encoded));
                    printf("UFPOOL_REPLY %" PRIu64 " %" PRIu64 " %s\n",request_id,correlation,encoded);fflush(stdout);continue;
                }
                unsigned slot=0;
                if(*session){slot=1;while(slot<SLOTS&&clients[slot].pid&&strcmp(clients[slot].session,session))slot++;
                    if(slot==SLOTS)return fail("client context bound");
                    if(!clients[slot].pid){if(strcmp(op,"map")||create_client(slot,session))return fail("missing context");}
                }
                if(!clients[slot].pid)return fail("start required");
                if(exact_write(clients[slot].fd,json,count)||exact_write(clients[slot].fd,"\n",1))return fail("context command");active=(int)slot;
                continue;
            }
            return fail("unknown command");
        }
        if(quitting){
            bool any=false;
            for(unsigned i=0;i<SLOTS;i++)if(clients[i].pid){int status;pid_t got=waitpid(clients[i].pid,&status,WNOHANG);
                if(got==clients[i].pid){if(!WIFEXITED(status)||WEXITSTATUS(status))return fail("SDK context retained on close");clients[i].pid=0;}else any=true;}
            if(worker){int status;pid_t got=waitpid(worker,&status,WNOHANG);if(got==worker){if(!WIFEXITED(status)||WEXITSTATUS(status))return fail("managed worker close failed");worker=0;}else any=true;}
            if(!any){quitting=false;drained=true;puts("UFPOOL_WORKERS_CLOSED");fflush(stdout);}
            if(now_ms()>quit_deadline)return fail("Guest service drain deadline");
        }
    }
}
