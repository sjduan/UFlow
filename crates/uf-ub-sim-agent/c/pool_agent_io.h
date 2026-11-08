#ifndef UFLOW_POOL_AGENT_IO_H
#define UFLOW_POOL_AGENT_IO_H
#include <stddef.h>
#include <stdint.h>
int pool_output_init(int fd);
int pool_emit(const char *format, ...);
int pool_output_flush(uint64_t now_ms);
uint64_t pool_now_ms(void);
int pool_birth(int pid, uint64_t *birth);
#endif
