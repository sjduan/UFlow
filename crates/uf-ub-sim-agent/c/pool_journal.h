/* Only the frozen worker's known metadata journal. No caller path or SDK ACK. */
#include <fcntl.h>
#include <stdint.h>
#include <string.h>
#include <sys/stat.h>
#define JOURNAL_MAX 65536U
#ifdef UFLOW_POOL_LAUNCHER_FIXTURE
#define JOURNAL_PATH "worker.conf.state"
#else
#define JOURNAL_PATH "/tmp/uflow-pool/worker.conf.state"
#endif
static int journal_line(const char *s,unsigned long long deadline){
    size_t n=strlen(s),off=0;
    while(off<n && ms()<deadline){
        ssize_t written=write(1,s+off,n-off);
        if(written>0)off+=(size_t)written;
        else if(written<0 && errno==EINTR)continue;
        else if(written<0 && (errno==EAGAIN||errno==EWOULDBLOCK))tick();
        else break;
    }
    return off==n?0:-1;
}
static void journal_capture(unsigned long long deadline){
    char line[800];unsigned char raw[JOURNAL_MAX+1];size_t used=0;int error=0;
    int flags=fcntl(1,F_GETFL);if(flags<0||fcntl(1,F_SETFL,flags|O_NONBLOCK)<0)return;
    int fd=open(JOURNAL_PATH,O_RDONLY|O_CLOEXEC|O_NOFOLLOW|O_NONBLOCK);
    if(fd<0){snprintf(line,sizeof(line),"UFPOOL_JOURNAL_END status=%s errno=%d bytes=0 complete=0\n",errno==ENOENT?"missing":"open-error",errno);journal_line(line,deadline);return;}
    struct stat before,after;
    if(fstat(fd,&before)||!S_ISREG(before.st_mode)||before.st_uid!=geteuid()||before.st_nlink!=1){
        close(fd);journal_line("UFPOOL_JOURNAL_END status=not-owned-regular errno=0 bytes=0 complete=0\n",deadline);return;
    }
    while(used<sizeof(raw)&&ms()<deadline){
        ssize_t n=read(fd,raw+used,sizeof(raw)-used);
        if(n>0)used+=(size_t)n;else if(!n)break;else if(errno!=EINTR){error=errno;break;}
    }
    if(fstat(fd,&after))error=errno;
    close(fd);
    const char *status="complete";
    if(error)status="read-error";
    else if(ms()>=deadline)status="deadline";
    else if(before.st_size!=after.st_size || before.st_mtim.tv_sec!=after.st_mtim.tv_sec || before.st_mtim.tv_nsec!=after.st_mtim.tv_nsec)status="changed";
    else if(used>JOURNAL_MAX || (uint64_t)before.st_size>JOURNAL_MAX)status="truncated";
    else if((uint64_t)before.st_size!=used)status="short-read";
    size_t count=used>JOURNAL_MAX?JOURNAL_MAX:used;
    snprintf(line,sizeof(line),"UFPOOL_JOURNAL_BEGIN source=worker.conf.state dev=%llu inode=%llu file_bytes=%llu max_bytes=%u\n",
        (unsigned long long)before.st_dev,(unsigned long long)before.st_ino,(unsigned long long)before.st_size,JOURNAL_MAX);
    if(journal_line(line,deadline))return;
    uint64_t hash=UINT64_C(1469598103934665603);const char *hex="0123456789abcdef";
    for(size_t off=0;off<count;off+=256){
        size_t n=count-off;if(n>256)n=256;char encoded[513];
        for(size_t i=0;i<n;i++){encoded[2*i]=hex[raw[off+i]>>4];encoded[2*i+1]=hex[raw[off+i]&15];hash=(hash^raw[off+i])*UINT64_C(1099511628211);}encoded[2*n]=0;
        snprintf(line,sizeof(line),"UFPOOL_JOURNAL_CHUNK offset=%zu hex=%s\n",off,encoded);
        if(journal_line(line,deadline))return; /* Missing END is explicitly incomplete in consumer. */
    }
    snprintf(line,sizeof(line),"UFPOOL_JOURNAL_END status=%s errno=%d bytes=%zu fnv64=%016llx complete=%u\n",status,error,count,(unsigned long long)hash,!strcmp(status,"complete"));
    journal_line(line,deadline);
}
