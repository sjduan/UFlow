/* TEST ONLY. Installed solely by the inherited-root/private-namespace driver.
 * Delays a real snapshot request before its handler takes catalog. The client
 * really blocks on read and hits its unchanged 5s SO_RCVTIMEO. This is a
 * controlled same-mechanism negative, NOT physical I/O or run130 reproduction.
 */
#define _GNU_SOURCE
#include <sys/socket.h>
#include <sys/mman.h>
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_once_t recv_once = PTHREAD_ONCE_INIT;
static pthread_once_t memfd_once = PTHREAD_ONCE_INIT;
static ssize_t (*real_recvmsg)(int, struct msghdr *, int);
static int (*real_memfd_create)(const char *, unsigned int);
static void init_recv(void) { real_recvmsg = dlsym(RTLD_NEXT, "recvmsg"); }
static void init_memfd(void) { real_memfd_create = dlsym(RTLD_NEXT, "memfd_create"); }
static int take_arm(const char *name) {
    const char *path = getenv(name);
    if (!path) return 0;
    int result = 0;
    pthread_mutex_lock(&lock);
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd >= 0) {
        char buf[32] = {0};
        ssize_t n = read(fd, buf, sizeof(buf)-1);
        close(fd);
        int remaining = n > 0 ? atoi(buf) : 0;
        if (remaining > 0) {
            fd = open(path, O_WRONLY | O_TRUNC | O_CLOEXEC);
            if (fd >= 0) { dprintf(fd, "%d\n", remaining-1); close(fd); }
        } else if (unlink(path) == 0) result = 1;
    }
    pthread_mutex_unlock(&lock);
    return result;
}
ssize_t recvmsg(int fd, struct msghdr *msg, int flags) {
    pthread_once(&recv_once, init_recv);
    ssize_t n = real_recvmsg(fd, msg, flags);
    const char request[] = "op=GetExternalDdrSnapshot\n";
    if (n == (ssize_t)(sizeof(request)-1) && msg->msg_iovlen == 1 && msg->msg_iov[0].iov_len >= (size_t)n &&
        memcmp(msg->msg_iov[0].iov_base, request, sizeof(request)-1) == 0 &&
        take_arm("UF_T4_PRIVATE_SOCKET_ARM")) {
        const char text[] = "T4_PRIVATE_SNAPSHOT_HANDLER_DELAY_5.5_SECONDS\n";
        (void)write(STDERR_FILENO, text, sizeof(text)-1);
        struct timespec delay = {5,500000000};
        while (nanosleep(&delay,&delay) != 0 && errno == EINTR) {}
    }
    return n;
}
int memfd_create(const char *name, unsigned int flags) {
    pthread_once(&memfd_once, init_memfd);
    if (strcmp(name,"uflow-broker-private-probe") == 0 && take_arm("UF_T4_PRIVATE_MEMFD_ARM")) {
        errno = EAGAIN; return -1;
    }
    return real_memfd_create(name,flags);
}
