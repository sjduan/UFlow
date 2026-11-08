#define _GNU_SOURCE
#include "pool_control.h"
#include "pool_lower_wire.h"
#include "pool_worker.h"
#include "pool_agent_io.h"
#include <fcntl.h>
#include <inttypes.h>
#include <poll.h>
#include <signal.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <termios.h>
#include <unistd.h>

int uflow_pool_client(int,const char *,const char *,const char *,uint64_t);
#define SLOTS 5
#define RELAYS 32
struct child {
    pid_t pid;uint64_t birth;int fd;char session[64];
    bool obligation,canary,ready,registered;char reply[4096];size_t used;
};
struct relay {
    int fd;pid_t peer;uint64_t birth,id,deadline,control_request,correlation;
    const char *role;unsigned char input[POOL_WIRE_MAX],output[POOL_WIRE_MAX];
    size_t in,out,sent;bool requested,observed;
};
struct replay {uint64_t request,correlation,hash;char reply[8400];};
static struct child clients[SLOTS];static struct relay relays[RELAYS];static struct replay cache[64];
static struct pool_worker worker={.logs={-1,-1}};
static char node[64],boot[96],connect_to[128],worker_config[256],worker_binary[512];
static uint64_t incarnation,high_request,relay_sequence,request_id,correlation,request_hash;
static int active=-1;
static uint64_t hash(const unsigned char *p,size_t n){uint64_t h=0xcbf29ce484222325ULL;for(size_t i=0;i<n;i++)h=(h^p[i])*0x100000001b3ULL;return h;}
static int fail(const char *why) {
    (void)pool_emit("UFPOOL_FAILURE_NOTICE\n");
    (void)pool_emit("UFPOOL_FATAL %s\n",why);
    (void)pool_output_flush(pool_now_ms());return 2;
}
static int exact_write(int fd,const void *data,size_t n) {
    const char *p=data;
    while(n){ssize_t sent=write(fd,p,n);if(sent<0&&errno==EINTR)continue;if(sent<=0)return -1;p+=sent;n-=(size_t)sent;}
    return 0;
}
static int create_client(unsigned slot,const char *session) {
    if(slot>=SLOTS||clients[slot].pid||strlen(session)>=64)return -1;
    int fds[2];if(socketpair(AF_UNIX,SOCK_STREAM|SOCK_CLOEXEC,0,fds))return -1;
    pid_t parent=getpid(),pid=fork();if(pid<0){close(fds[0]);close(fds[1]);return -1;}
    if(!pid) {
        if(prctl(PR_SET_PDEATHSIG,SIGTERM)||getppid()!=parent)_exit(2);
        close(fds[0]);
        for(int fd=0;fd<1024;fd++)if(fd!=fds[1]&&fd!=2)close(fd);
        int sink=open("/dev/null",O_RDWR);if(sink<0)_exit(2);
        if(dup2(sink,0)<0||dup2(sink,1)<0)_exit(2);
        if(sink>2&&sink!=fds[1])close(sink);
        _exit(uflow_pool_client(fds[1],connect_to,node,boot,incarnation));
    }
    close(fds[1]);clients[slot].pid=pid;clients[slot].fd=fds[0];
    if(pool_birth(pid,&clients[slot].birth))return -1;
    int flags=fcntl(fds[0],F_GETFL);if(flags<0||fcntl(fds[0],F_SETFL,flags|O_NONBLOCK)<0)return -1;
    strcpy(clients[slot].session,session);return 0;
}
static int identity(void) {
#ifdef UFLOW_POOL_CPU_FIXTURE
    const char *engine="cpu-fixture";
#else
    const char *engine="initramfs";
#endif
    return pool_emit("UFLOW_ENV_IDENTITY {\"node\":\"%s\",\"boot_id\":\"%s\",\"engine\":\"%s\",\"machine\":\"aarch64\",\"uid\":%u,\"agent_session\":\"%s\",\"incarnation\":%" PRIu64 "}\n",
        node,boot,engine,(unsigned)getuid(),boot,incarnation);
}
static int completed(const char *body) {
    char hex[8400];if(pool_hex((const unsigned char*)body,strlen(body),hex,sizeof(hex)))return -1;
    struct replay *saved=&cache[request_id%64];
    saved->request=request_id;saved->correlation=correlation;saved->hash=request_hash;strcpy(saved->reply,hex);
    return pool_emit("UFPOOL_REPLY %" PRIu64 " %" PRIu64 " %s\n",request_id,correlation,hex);
}
static bool business(const char *op) {
    return !strcmp(op,"allocate")||!strcmp(op,"map")||!strcmp(op,"write")||!strcmp(op,"read");
}
static int directory_identity(const struct relay *r,const struct mem_service_wire_header *h) {
    char value[64];uint64_t inc,generation,caps;
    if(h->operation<0x76||h->operation>0x79||h->operation==0x78)return 0;
    if(pool_wire_field(r->input,r->in,"node_id",value,sizeof(value))||strcmp(value,node)||
       pool_wire_number(r->input,r->in,"incarnation",&inc)||inc!=incarnation)return -1;
    if(h->operation!=0x79 && (pool_wire_number(r->input,r->in,"readiness_generation",&generation)||generation!=1))return -1;
    if(h->operation==0x76 && (strcmp(r->role,"root")||!clients[0].ready||worker.started||
       pool_wire_number(r->input,r->in,"capabilities",&caps)||caps!=1))return -1;
    if((h->operation==0x77||h->operation==0x79) && strcmp(r->role,"worker"))return -1;
    return 0;
}
static int relay_allowed(struct relay *r) {
    struct mem_service_wire_header h;int complete=pool_wire_header(r->input,r->in,&h);
    if(complete!=1)return complete;
    if(h.status||h.error_code||!(h.operation==1||h.operation==2||h.operation==3||(h.operation>=0x70&&h.operation<=0x7d)))return -1;
    return directory_identity(r,&h)?-1:1;
}
static int observe_response(struct relay *r) {
    struct mem_service_wire_header request,response;
    if(pool_wire_header(r->input,r->in,&request)!=1||pool_wire_header(r->output,r->out,&response)!=1||
       response.request_id!=request.request_id||response.operation!=request.operation||response.status>10)return -1;
    if(!strcmp(r->role,"root")&&request.operation==0x78&&!response.status&&!response.error_code) {
        uint64_t ready,lease;
        if(pool_wire_number(r->output,r->out,"provider_directory_ready",&ready)||
           pool_wire_number(r->output,r->out,"lease_ms",&lease)||!lease)return -1;
        if(ready && pool_wire_own_provider(r->output,r->out,node,incarnation,lease))return -1;
    }
    if(!strcmp(r->role,"worker") && (request.operation==0x77||request.operation==0x79)) {
        char value[64];
        if(response.status||response.error_code||pool_wire_field(r->output,r->out,"status",value,sizeof(value))||strcmp(value,"ok")||
           pool_wire_field(r->output,r->out,"node_id",value,sizeof(value))||strcmp(value,node))return -1;
        if(request.operation==0x77) {
            uint64_t lease;
            if(pool_wire_number(r->output,r->out,"lease_ms",&lease)||lease<100||lease>86400000)return -1;
            worker.refresh_count++;worker.last_refresh_ms=pool_now_ms();worker.lease_ms=lease;
            if(pool_emit("UFPOOL_WORKER_REFRESH %u %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 "\n",
                (unsigned)worker.pid,worker.birth,worker.refresh_count,worker.lease_ms,request.request_id))return -1;
        } else worker.deregistered=true;
    }
    r->observed=true;return 0;
}
static int start_worker(void) {
    if(!clients[0].ready||!clients[0].registered||worker.started)return -1;
    FILE *f=fopen(worker_config,"wx");if(!f)return -1;
    fprintf(f,"connect=%s\nnode_id=%s\nstate_file=%s.state\nincarnation=%" PRIu64 "\nreadiness_generation=1\nallocation_granularity_bytes=2097152\nfast_allocation=0\n",
        connect_to,node,worker_config,incarnation);fclose(f);
    if(pool_worker_start(&worker,worker_binary,worker_config))return -1;
    return pool_emit("UFPOOL_WORKER_START %u %" PRIu64 "\n",(unsigned)worker.pid,worker.birth);
}

int main(int argc,char **argv) {
    if(pool_output_init(1))return 2;
    for(unsigned i=0;i<SLOTS;i++)clients[i].fd=-1;
    for(unsigned i=0;i<RELAYS;i++)relays[i].fd=-1;
    char directory[256]="/tmp/uflow-pool",lower_path[108];
#ifdef UFLOW_POOL_CPU_FIXTURE
    if(argc!=5||strlen(argv[1])>=sizeof(directory)||strlen(argv[2])>=sizeof(node)||strlen(argv[3])>=sizeof(boot))return fail("CPU fixture arguments");
    strcpy(directory,argv[1]);strcpy(node,argv[2]);strcpy(boot,argv[3]);
    const char *fixture_worker=getenv("UFLOW_POOL_FIXTURE_WORKER");
    if(!fixture_worker||strlen(fixture_worker)>=sizeof(worker_binary))return fail("CPU worker fixture required");
    strcpy(worker_binary,fixture_worker);
    if(strcmp(argv[4],"metadata-only-no-OBMM"))return fail("CPU fixture marker required");
#else
    (void)argc;(void)argv;
    FILE *f=fopen("/proc/cmdline","r");char command[4096];
    if(!f||!fgets(command,sizeof(command),f))return fail("cmdline");fclose(f);
    const char *p=strstr(command,"uflow_node=");if(!p||sscanf(p,"uflow_node=%63s",node)!=1)return fail("node identity");
    f=fopen("/proc/sys/kernel/random/boot_id","r");if(!f||!fgets(boot,sizeof(boot),f))return fail("boot identity");fclose(f);
    boot[strcspn(boot,"\r\n")]=0;
    strcpy(worker_binary,"/bin/linqu_mem_service_provider_obmm");
    struct termios terminal;if(!tcgetattr(0,&terminal)){cfmakeraw(&terminal);tcsetattr(0,TCSANOW,&terminal);}
#endif
    if(strcmp(node,"nodeA")&&strcmp(node,"nodeB"))return fail("bound node identity");
    if(!*boot)return fail("empty boot identity");
    for(const char *p=boot;*p;p++)if(!((*p>='a'&&*p<='z')||(*p>='A'&&*p<='Z')||(*p>='0'&&*p<='9')||*p=='-'))return fail("boot token");
    incarnation=hash((const unsigned char*)boot,strlen(boot));if(!incarnation)incarnation=1;
    if(mkdir(directory,0700)||snprintf(lower_path,sizeof(lower_path),"%s/l.sock",directory)>=(int)sizeof(lower_path)||
       snprintf(worker_config,sizeof(worker_config),"%s/worker.conf",directory)>=(int)sizeof(worker_config))return fail("fresh owned paths");
    snprintf(connect_to,sizeof(connect_to),"unix:%s",lower_path);
    int listener=socket(AF_UNIX,SOCK_STREAM|SOCK_CLOEXEC,0);struct sockaddr_un address={.sun_family=AF_UNIX};strcpy(address.sun_path,lower_path);
    if(listener<0||bind(listener,(void*)&address,sizeof(address))||chmod(lower_path,0600)||listen(listener,32))return fail("private lower relay");
    char input[12000];size_t input_used=0;bool quitting=false,drained=false;uint64_t quit_deadline=0,worker_stop_deadline=0;
    if(identity())return fail("identity output bound");
    for(;;) {
        if(pool_output_flush(pool_now_ms()))return fail("bounded output queue");
        int changed=pool_worker_poll(&worker);
        if(changed<0 || (changed==1 && (!worker.stopping||!WIFEXITED(worker.status)||WEXITSTATUS(worker.status)))) {
            (void)pool_emit("UFPOOL_FAILURE_NOTICE\n");
            (void)pool_emit("UFPOOL_WORKER_EXIT %u %" PRIu64 " %u %u\n",(unsigned)worker.pid,worker.birth,
                WIFEXITED(worker.status)?(unsigned)WEXITSTATUS(worker.status):0,WIFSIGNALED(worker.status)?(unsigned)WTERMSIG(worker.status):0);
            (void)pool_worker_logs(&worker);return fail("worker exited or birth changed");
        }
        if(pool_worker_logs(&worker))return fail("worker log budget/read failure");
        if(worker.started&&!worker.exited&&!worker.stopping &&
           ((!worker.refresh_count && pool_now_ms()-worker.started_ms>5000)||
            (worker.refresh_count&&!pool_worker_fresh(&worker,pool_now_ms()))))return fail("missing or expired worker refresh");
        if(active==-2 && worker.exited && worker.logs[0]<0 && worker.logs[1]<0) {
            if(!worker.deregistered)return fail("worker exit without actual directory deregister completion");
            char result[512];snprintf(result,sizeof(result),"{\"schema\":2,\"phase\":\"completed\",\"worker_stopped\":true,\"worker_pid\":%u,\"worker_birth\":%" PRIu64 ",\"worker_exit_code\":0,\"directory_deregistered\":true}",(unsigned)worker.pid,worker.birth);
            if(completed(result))return fail("worker close output");active=-1;
        }
        if(active==-2 && pool_now_ms()>worker_stop_deadline)return fail("worker stop deadline");
        struct pollfd polls[4+SLOTS+RELAYS];
        polls[0]=(struct pollfd){.fd=0,.events=POLLIN};polls[1]=(struct pollfd){.fd=listener,.events=POLLIN};
        for(unsigned i=0;i<SLOTS;i++)polls[2+i]=(struct pollfd){.fd=clients[i].fd,.events=POLLIN};
        for(unsigned i=0;i<RELAYS;i++)polls[2+SLOTS+i]=(struct pollfd){.fd=relays[i].fd,.events=(short)(POLLIN|(relays[i].observed&&relays[i].out>relays[i].sent?POLLOUT:0))};
        for(unsigned i=0;i<2;i++)polls[2+SLOTS+RELAYS+i]=(struct pollfd){.fd=worker.logs[i],.events=POLLIN};
        if(poll(polls,4+SLOTS+RELAYS,50)<0&&errno!=EINTR)return fail("poll");
        if(polls[1].revents&POLLIN) {
            unsigned slot=0;while(slot<RELAYS&&relays[slot].fd>=0)slot++;
            if(slot==RELAYS)return fail("relay bound");
            int fd=accept4(listener,NULL,NULL,SOCK_CLOEXEC|SOCK_NONBLOCK);struct ucred peer;socklen_t len=sizeof(peer);
            if(fd<0||getsockopt(fd,SOL_SOCKET,SO_PEERCRED,&peer,&len)||peer.uid!=getuid())return fail("relay peer credentials");
            const char *role=NULL;uint64_t birth=0;
            if(worker.started&&!worker.exited&&peer.pid==worker.pid){role="worker";birth=worker.birth;}
            for(unsigned i=0;i<SLOTS;i++)if(clients[i].pid==peer.pid){role=i?"client":"root";birth=clients[i].birth;}
            uint64_t current;if(!role||pool_birth(peer.pid,&current)||birth!=current)return fail("unowned relay peer");
            relays[slot]=(struct relay){.fd=fd,.peer=peer.pid,.birth=birth,.role=role,.id=++relay_sequence,.deadline=pool_now_ms()+15000};
            if(active>=0&&clients[active].pid==peer.pid){relays[slot].control_request=request_id;relays[slot].correlation=correlation;}
        }
        for(unsigned i=0;i<RELAYS;i++)if(relays[i].fd>=0) {
            struct relay *r=&relays[i];short events=polls[2+SLOTS+i].revents;
            if(pool_now_ms()>r->deadline)return fail("lower RPC deadline; retain original outcome");
            if((events&POLLIN)&&!r->requested) {
                ssize_t n=read(r->fd,r->input+r->in,sizeof(r->input)-r->in);
                if(n<=0)return fail("lower request EOF");r->in+=(size_t)n;
                int complete=relay_allowed(r);if(complete<0)return fail("lower request identity/checksum/op");
                if(complete) {
                    char hex[POOL_WIRE_MAX*2+1];pool_hex(r->input,r->in,hex,sizeof(hex));
                    if(pool_emit("UFPOOL_RPC %" PRIu64 " %s %u %" PRIu64 " %" PRIu64 " %" PRIu64 " %s\n",r->id,r->role,(unsigned)r->peer,r->birth,r->control_request,r->correlation,hex))return fail("lower output bound");
                    r->requested=true;
                }
            }
            if(events&POLLOUT) {
                ssize_t n=write(r->fd,r->output+r->sent,r->out-r->sent);
                if(n>0)r->sent+=(size_t)n;
                else if(n<0&&errno!=EAGAIN&&errno!=EWOULDBLOCK&&errno!=EINTR)return fail("lower reply send");
                if(r->observed&&r->sent==r->out){close(r->fd);r->fd=-1;}
            }
        }
        for(unsigned i=0;i<SLOTS;i++)if(clients[i].fd>=0&&(polls[2+i].revents&(POLLIN|POLLHUP))) {
            struct child *c=&clients[i];ssize_t n=read(c->fd,c->reply+c->used,sizeof(c->reply)-1-c->used);
            if(n<=0)return fail("SDK context EOF");c->used+=(size_t)n;c->reply[c->used]=0;
            char *end;
            while((end=strchr(c->reply,'\n'))) {
                size_t consumed=(size_t)(end-c->reply)+1;*end=0;
                if(!strcmp(c->reply,"UFPOOL_FAILURE_NOTICE")) {
                    if(pool_emit("UFPOOL_FAILURE_NOTICE\n"))return fail("failure notice output");
                } else {
                    if(active!=(int)i)return fail("unexpected SDK reply");
                    struct pool_control reply;if(pool_parse(c->reply,&reply))return fail("SDK reply grammar");
                    if(!strcmp(pool_get(&reply,"phase"),"completed")) {
                        if(!strcmp(pool_get(&reply,"canary_prepared"),"true"))c->canary=true;
                        if(!strcmp(pool_get(&reply,"canary_ready"),"true"))c->ready=true;
                        if(!strcmp(pool_get(&reply,"canary_closed"),"true")){c->canary=false;c->ready=false;}
                        if(*pool_get(&reply,"directory_registered"))c->registered=!strcmp(pool_get(&reply,"directory_registered"),"true");
                        if(!strcmp(pool_get(&reply,"mapped"),"true")||!strcmp(pool_get(&reply,"state"),"active"))c->obligation=true;
                        if(!strcmp(pool_get(&reply,"unmapped"),"true")||!strcmp(pool_get(&reply,"state"),"retired"))c->obligation=false;
                    } else c->obligation=true;
                    if(completed(c->reply))return fail("SDK output bound");active=-1;
                }
                memmove(c->reply,c->reply+consumed,c->used-consumed);c->used-=consumed;c->reply[c->used]=0;
            }
            if(c->used==sizeof(c->reply)-1)return fail("SDK reply line bound");
        }
        if(polls[0].revents&(POLLIN|POLLHUP)) {
            char byte;ssize_t n=read(0,&byte,1);if(n<=0)return fail("host control EOF");
            if(byte!='\n'){if(input_used+1>=sizeof(input))return fail("control line bound");input[input_used++]=byte;continue;}
            input[input_used]=0;input_used=0;
            if(!strcmp(input,"identity")){if(identity())return fail("identity output");continue;}
            if(!strcmp(input,"sdk-check")){if(pool_emit("UFLOW_ENV_SDK_OK\n"))return fail("ABI output");continue;}
            if(!strcmp(input,"start")){if(create_client(0,"owner")||pool_emit("UFPOOL_STARTED\n"))return fail("root client start");continue;}
            if(!strcmp(input,"quit")) {
                if(!drained)return fail("prepare-quit required");
                close(listener);if(pool_emit("UFLOW_ENV_DAEMON_CLOSED\n"))return fail("closed output");
                uint64_t until=pool_now_ms()+30000;while(pool_now_ms()<until){if(pool_output_flush(pool_now_ms()))return 2;usleep(10000);}return fail("QMP close deadline");
            }
            if(!strcmp(input,"prepare-quit")) {
                for(unsigned i=0;i<SLOTS;i++)if(clients[i].obligation||clients[i].canary||clients[i].registered)return fail("quit with lower obligation");
                if(active!=-1||!worker.exited||!worker.deregistered)return fail("actual worker stop required");
                quitting=true;quit_deadline=pool_now_ms()+9000;
                for(unsigned i=0;i<SLOTS;i++)if(clients[i].fd>=0){close(clients[i].fd);clients[i].fd=-1;}
                continue;
            }
            if(!strncmp(input,"wire ",5)) {
                uint64_t id;char hex[4096];if(sscanf(input,"wire %" SCNu64 " %4095s",&id,hex)!=2)return fail("wire syntax");
                unsigned i=0;while(i<RELAYS&&(relays[i].fd<0||relays[i].id!=id))i++;
                if(i==RELAYS)return fail("unknown relay ID");
                struct relay *r=&relays[i];size_t count;
                if(r->observed||pool_unhex(hex,r->output+r->out,sizeof(r->output)-r->out,&count))return fail("reply bounds/replay");r->out+=count;
                struct mem_service_wire_header h;int complete=pool_wire_header(r->output,r->out,&h);
                if(complete<0||(complete==1&&observe_response(r)))return fail("reply identity/checksum/worker outcome");
                continue;
            }
            if(!strncmp(input,"v2 ",3)) {
                char hex[8192],json[4096];size_t count;
                if(active!=-1||quitting||sscanf(input,"v2 %" SCNu64 " %" SCNu64 " %8191s",&request_id,&correlation,hex)!=3||!request_id||!correlation||
                   pool_unhex(hex,(unsigned char*)json,sizeof(json)-1,&count))return fail("v2 syntax");json[count]=0;
                struct pool_control req;uint64_t schema;if(pool_parse(json,&req)||pool_u64(&req,"schema",&schema)||schema!=2)return fail("v2 body");
                request_hash=hash((const unsigned char*)json,count);struct replay *old=&cache[request_id%64];
                if(old->request==request_id){if(old->hash!=request_hash||old->correlation!=correlation)return fail("replay mutation");if(pool_emit("UFPOOL_REPLY %" PRIu64 " %" PRIu64 " %s\n",request_id,correlation,old->reply))return fail("replay output");continue;}
                if(request_id<=high_request&&high_request-request_id>=64)return fail("retired replay");if(request_id>high_request)high_request=request_id;
                const char *op=pool_get(&req,"op"),*session=pool_get(&req,"client_session");
                if(!strcmp(op,"worker_start")) {
                    if(start_worker())return fail("worker start requires verified registration");
                    char result[256];snprintf(result,sizeof(result),"{\"schema\":2,\"phase\":\"completed\",\"worker_started\":true,\"worker_pid\":%u,\"worker_birth\":%" PRIu64 "}",(unsigned)worker.pid,worker.birth);
                    if(completed(result))return fail("worker start output");continue;
                }
                if(!strcmp(op,"worker_state")) {
                    bool alive=pool_worker_alive(&worker),fresh=pool_worker_fresh(&worker,pool_now_ms());char result[768];
                    snprintf(result,sizeof(result),"{\"schema\":2,\"phase\":\"completed\",\"worker_pid\":%u,\"worker_birth\":%" PRIu64 ",\"worker_alive\":%s,\"worker_ready\":%s,\"worker_refresh_count\":%" PRIu64 ",\"refresh_age_ms\":%" PRIu64 ",\"lease_ms\":%" PRIu64 ",\"incarnation\":%" PRIu64 ",\"readiness_generation\":1,\"capabilities\":1}",
                        (unsigned)worker.pid,worker.birth,alive?"true":"false",fresh&&clients[0].registered&&clients[0].ready?"true":"false",worker.refresh_count,
                        worker.refresh_count?pool_now_ms()-worker.last_refresh_ms:0,worker.lease_ms,incarnation);
                    if(completed(result))return fail("worker state output");continue;
                }
                if(!strcmp(op,"worker_stop")) {
                    for(unsigned i=0;i<SLOTS;i++)if(clients[i].obligation)return fail("worker stop with business obligation");
                    if(pool_worker_stop(&worker))return fail("worker stop identity");active=-2;worker_stop_deadline=pool_now_ms()+9000;continue;
                }
                if(!strcmp(op,"provider_drain")&&(!worker.exited||!worker.deregistered))return fail("provider drain before worker SDK close");
                if(business(op)&&!pool_worker_fresh(&worker,pool_now_ms()))return fail("worker not fresh before business");
                if(!strcmp(op,"inspect")) {
                    unsigned obligations=worker.started&&!worker.exited;
                    for(unsigned i=0;i<SLOTS;i++)obligations+=clients[i].obligation+clients[i].canary+clients[i].registered;
                    char result[256];snprintf(result,sizeof(result),"{\"schema\":2,\"phase\":\"completed\",\"contexts\":%u,\"pins\":0,\"cleanup_required\":%s}",obligations,obligations?"true":"false");
                    if(completed(result))return fail("inspect output");continue;
                }
                unsigned slot=0;
                if(*session) {
                    slot=1;while(slot<SLOTS&&clients[slot].pid&&strcmp(clients[slot].session,session))slot++;
                    if(slot==SLOTS)return fail("client bound");
                    if(!clients[slot].pid && (strcmp(op,"map")||create_client(slot,session)))return fail("missing client context");
                }
                if(!clients[slot].pid||exact_write(clients[slot].fd,json,count)||exact_write(clients[slot].fd,"\n",1))return fail("context command");active=(int)slot;continue;
            }
            return fail("unknown command");
        }
        if(quitting) {
            bool any=false;
            for(unsigned i=0;i<SLOTS;i++)if(clients[i].pid) {
                int status;pid_t got=waitpid(clients[i].pid,&status,WNOHANG);
                if(got==clients[i].pid){if(!WIFEXITED(status)||WEXITSTATUS(status))return fail("SDK context retained");clients[i].pid=0;}else any=true;
            }
            if(!any){quitting=false;drained=true;if(pool_emit("UFPOOL_WORKERS_CLOSED\n"))return fail("drained output");}
            if(pool_now_ms()>quit_deadline)return fail("context drain deadline");
        }
    }
}
