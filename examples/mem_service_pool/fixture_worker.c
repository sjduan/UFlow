/* Real child process / neutral SDK refresh fixture; never opens an endpoint. */
#define _GNU_SOURCE
#include <mem_service_client.h>
#include <inttypes.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
static volatile sig_atomic_t stop;
static void stopping(int signo){(void)signo;stop=1;}
int main(int argc,char **argv) {
    if(argc!=4||strcmp(argv[1],"serve-allocations")||strcmp(argv[2],"--config"))return 2;
    char connect[256]="",node[64]="",line[512];uint64_t inc=0,generation=0;
    FILE *f=fopen(argv[3],"r");if(!f)return 2;
    while(fgets(line,sizeof(line),f)) {
        line[strcspn(line,"\r\n")]=0;
        if(!strncmp(line,"connect=",8))snprintf(connect,sizeof(connect),"%s",line+8);
        if(!strncmp(line,"node_id=",8))snprintf(node,sizeof(node),"%s",line+8);
        if(!strncmp(line,"incarnation=",12))inc=strtoull(line+12,NULL,10);
        if(!strncmp(line,"readiness_generation=",21))generation=strtoull(line+21,NULL,10);
    }
    fclose(f);if(!*connect||!*node||!inc||generation!=1)return 2;
    const char *mode=getenv("UFLOW_POOL_FIXTURE_MODE");if(!mode)mode="normal";
    setvbuf(stdout,NULL,_IONBF,0);setvbuf(stderr,NULL,_IONBF,0);
    printf("fixture stdout pid=%u mode=%s\n",(unsigned)getpid(),mode);
    fprintf(stderr,"fixture stderr pid=%u mode=%s\n",(unsigned)getpid(),mode);
    if(!strcmp(mode,"exit")) {
        char tail[2048];memset(tail,'E',sizeof(tail));
        if(write(2,tail,sizeof(tail))!=(ssize_t)sizeof(tail))return 9;
        fputs("\nfixture stderr final tail\n",stderr);return 7;
    }
    if(!strcmp(mode,"burst")){char bytes[4096];memset(bytes,'L',sizeof(bytes));for(unsigned i=0;i<80;i++)if(write(1,bytes,sizeof(bytes))<0)return 9;}
    signal(SIGTERM,stopping);signal(SIGINT,stopping);
    struct mem_service_client client;
    struct mem_service_wire_client_options options;mem_service_wire_client_options_init(&options);
    options.timeout_ms=1500;options.max_attempts=1;mem_service_client_init_with_options(&client,connect,&options);
    bool refreshed=false;unsigned count=0;
    if(!strcmp(mode,"wrong-inc"))inc++;
    if(!strcmp(mode,"wrong-generation"))generation++;
    while(!stop) {
        if(strcmp(mode,"no-refresh") && (strcmp(mode,"stale")||!count)) {
            struct mem_service_client_provider_directory view;enum mem_service_wire_status status;
            if(mem_service_client_provider_refresh(&client,node,inc,generation,&view,&status)||status!=MEM_SERVICE_WIRE_STATUS_OK) {
                fprintf(stderr,"actual SDK refresh rejected status=%u\n",(unsigned)status);return 8;
            }
            refreshed=true;count++;
            if(view.directory_ready) {
                struct mem_service_client_allocation empty;
                int polled=mem_service_client_poll_allocation(&client,node,inc,0,&empty,&status);
                if(polled<0||status!=MEM_SERVICE_WIRE_STATUS_NOT_FOUND) {
                    fprintf(stderr,"CPU fixture expected an empty actual SDK work queue\n");return 12;
                }
            }
        }
        usleep(50000);
    }
    if(refreshed) {
        struct mem_service_client_provider_directory view;enum mem_service_wire_status status;
        if(mem_service_client_provider_deregister(&client,node,inc,&view,&status)||status!=MEM_SERVICE_WIRE_STATUS_OK)return 10;
    }
    puts("fixture stopped with actual SDK deregister");return refreshed?0:11;
}
