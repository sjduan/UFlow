/* Task-private PID1 seam. No device API, ACK inference or E0 base changes.
 * An unreaped direct child owns the dedicated group through TERM/KILL.
 * PID1 stays alive only within a finite host-QMP/monitor handoff window. */
#define _GNU_SOURCE
#include <errno.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/prctl.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static volatile sig_atomic_t stopped;
static void signal_stop(int sig){(void)sig;stopped=1;}
static unsigned long long ms(void){struct timespec t;clock_gettime(CLOCK_MONOTONIC,&t);return (unsigned long long)t.tv_sec*1000+t.tv_nsec/1000000;}
static void tick(void){struct timespec t={0,10000000};nanosleep(&t,NULL);}
#include "pool_journal.h"
int main(int argc,char **argv) {
    unsigned term_ms=2000,kill_ms=2000,handoff_ms=30000;
    char *production[]={"/bin/uflow_pool_agent",NULL};char **command=production;
#ifdef UFLOW_POOL_LAUNCHER_FIXTURE
    if(argc<5)return 125;
    term_ms=(unsigned)strtoul(argv[1],NULL,10);kill_ms=(unsigned)strtoul(argv[2],NULL,10);handoff_ms=(unsigned)strtoul(argv[3],NULL,10);command=argv+4;
    if(!term_ms||term_ms>2000||!kill_ms||kill_ms>2000||handoff_ms>30000)return 125;
#else
    (void)argc;(void)argv;if(getpid()!=1)return 125;
#endif
    if(prctl(PR_SET_CHILD_SUBREAPER,1,0,0,0))return 125;
    struct sigaction action={.sa_handler=signal_stop};sigemptyset(&action.sa_mask);
    if(sigaction(SIGTERM,&action,NULL)||sigaction(SIGINT,&action,NULL))return 125;
    pid_t parent=getpid(),child=fork();if(child<0)return 125;
    if(!child){
        if(prctl(PR_SET_PDEATHSIG,SIGKILL)||getppid()!=parent||setsid()<0)_exit(126);
        signal(SIGTERM,SIG_DFL);signal(SIGINT,SIG_DFL);
        execv(command[0],command);_exit(127);
    }
    siginfo_t info={0};
    while(!stopped){
        if(waitid(P_PID,(id_t)child,&info,WEXITED|WNOHANG|WNOWAIT)<0 && errno!=EINTR)return 125;
        if(info.si_pid)break;
        tick();
    }
    bool failed=stopped||info.si_code!=CLD_EXITED||info.si_status!=0;
    if(failed){fputs("UFPOOL_FAILURE_NOTICE\n",stdout);fflush(stdout);}
    unsigned long long handoff_deadline=ms()+term_ms+kill_ms+handoff_ms;
    /* Keep the group leader unreaped until both signal phases are complete,
     * so its ID cannot be reused for another process group. */
    (void)kill(-child,SIGTERM);
    unsigned long long until=ms()+term_ms;
    while(ms()<until)tick();
    (void)kill(-child,SIGKILL);
    until=ms()+kill_ms;bool empty=false;
    while(ms()<until){
        int status;pid_t got=waitpid(-1,&status,WNOHANG);
        if(got>0){printf("UFPOOL_LAUNCHER_WAIT pid=%d raw_status=%d\n",got,status);fflush(stdout);}
        if(got<0 && errno==ECHILD){empty=true;break;}
        if(got<=0)tick();
    }
    if(!empty && !failed){fputs("UFPOOL_FAILURE_NOTICE\n",stdout);fflush(stdout);failed=true;}
    /* Notice precedes diagnosis. Diagnostic time consumes, never extends, the
     * original finite handoff; blocked serial output becomes incomplete. */
    unsigned long long diagnostic_deadline=ms()+1000;
    if(diagnostic_deadline>handoff_deadline)diagnostic_deadline=handoff_deadline;
    journal_capture(diagnostic_deadline);
    printf("UFPOOL_LAUNCHER_CLOSED child=%d exit=%d signal=%d children_empty=%u logical_ACK_inferred=0\n",
        child,info.si_code==CLD_EXITED?info.si_status:-1,info.si_code==CLD_KILLED?info.si_status:0,empty);fflush(stdout);
    /* The frozen group waits for this marker before QMP quit. It proves
     * actual process closure; complete journals still need their own gate. */
    if(!failed && empty){fputs("UFLOW_ENV_DAEMON_CLOSED\n",stdout);fflush(stdout);}
    /* Normal: authenticated host QMP quits after actual SDK drain. Failure:
     * independent host monitor kills this exact QEMU. No indefinite PID1 park.
     * If that external handoff fails, deadline exit may panic PID1; it is an
     * explicit bounded fallback, never a normal drain/ACK. */
    until=handoff_deadline;
    while(ms()<until && !stopped)tick();
    return failed?1:0;
}
