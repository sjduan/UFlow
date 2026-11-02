/* PRIVATE TEST ONLY: delay a real production physical-inventory read.
 * No fabricated clock value/response, no change to discovery or receipt TTL.
 * The guarded namespace driver alone installs this preload in its own daemon.
 * Arm file contains the number of node/online opens to skip (4 => post-probe
 * snapshot in Plan). Removing the arm before sleeping makes this one-shot.
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static void observation_delay(const char *path, int (*real_open)(const char *, int, ...)) {
    static int loaded = 0, remaining = 0;
    const char *arm = getenv("UF_T35_PRIVATE_DELAY_ARM");
    if (!arm || strcmp(path, "/sys/devices/system/node/online") != 0) return;
    int fd = real_open(arm, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return;
    if (!loaded) {
        char buffer[32] = {0};
        ssize_t count = read(fd, buffer, sizeof(buffer) - 1);
        if (count > 0) remaining = atoi(buffer);
        loaded = 1;
    }
    close(fd);
    if (remaining-- > 0) return;
    if (unlink(arm) != 0) return;
    loaded = 0;
    const char message[] = "T35_PRIVATE_REAL_OBSERVATION_DELAY_5.3_SECONDS\n";
    (void)write(STDERR_FILENO, message, sizeof(message) - 1);
    struct timespec delay = {5, 300000000};
    while (nanosleep(&delay, &delay) != 0 && errno == EINTR) {}
}

int open64(const char *path, int flags, ...) {
    static int (*next)(const char *, int, ...) = NULL;
    if (!next) next = dlsym(RTLD_NEXT, "open64");
    mode_t mode = 0;
    if ((flags & O_CREAT) || (flags & O_TMPFILE) == O_TMPFILE) {
        va_list args; va_start(args, flags); mode = va_arg(args, int); va_end(args);
    }
    observation_delay(path, next);
    return next(path, flags, mode);
}

int open(const char *path, int flags, ...) {
    static int (*next)(const char *, int, ...) = NULL;
    if (!next) next = dlsym(RTLD_NEXT, "open");
    mode_t mode = 0;
    if ((flags & O_CREAT) || (flags & O_TMPFILE) == O_TMPFILE) {
        va_list args; va_start(args, flags); mode = va_arg(args, int); va_end(args);
    }
    observation_delay(path, next);
    return next(path, flags, mode);
}
