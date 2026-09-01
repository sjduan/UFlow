#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "mem_service_provider_obmm_sdk.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define BRIDGE_MAGIC "UFSIMB01"
#define BRIDGE_MAJOR 1U
#define BRIDGE_MINOR 0U
#define BRIDGE_HEADER_BYTES 68U
#define BRIDGE_MAX_BODY (64U * 1024U)
#define BRIDGE_MAX_FRAME (BRIDGE_HEADER_BYTES + 4U * UINT16_MAX + BRIDGE_MAX_BODY)
#define BRIDGE_TYPE_IDENTITY 1U
#define BRIDGE_TYPE_PROBE 2U
#define BRIDGE_TYPE_ALLOCATE 3U
#define BRIDGE_TYPE_RELEASE 4U
#define BRIDGE_TYPE_SEED 5U
#define BRIDGE_TYPE_PUBLISH 6U
#define BRIDGE_TYPE_IMPORT 7U
#define BRIDGE_TYPE_REVOKE 8U
#define BRIDGE_TYPE_CHECKSUM 9U
#define BRIDGE_TYPE_COMPLETION 10U
#define BRIDGE_TYPE_ERROR 11U
#define BRIDGE_TYPE_SHUTDOWN 12U
#define BRIDGE_TYPE_ACCEPTED 13U
#define BRIDGE_BODY_SCRATCH 4096U
#define REPLAY_SLOTS 64U

struct bridge_frame {
    uint16_t message_type;
    uint16_t flags;
    uint64_t request_id;
    uint64_t correlation_id;
    uint64_t session_generation;
    int32_t status;
    int32_t error_code;
    char execution_realm_id[128];
    char node_id[64];
    char boot_id[96];
    char provider_session_id[MEM_SERVICE_OBMM_SDK_ID_MAX];
    uint8_t *body;
    uint32_t body_len;
};

struct replay_slot {
    bool active;
    uint64_t request_id;
    uint64_t fingerprint;
    uint8_t *response;
    uint32_t response_len;
};

struct agent_state {
    struct mem_service_obmm_sdk_session *session;
    struct mem_service_obmm_sdk_identity identity;
    char node_id[64];
    char boot_id[96];
    uint64_t request_count;
    struct replay_slot replay[REPLAY_SLOTS];
};

static uint16_t get_u16(const uint8_t *bytes)
{
    return ((uint16_t)bytes[0] << 8) | bytes[1];
}

static uint32_t get_u32(const uint8_t *bytes)
{
    return ((uint32_t)bytes[0] << 24) | ((uint32_t)bytes[1] << 16) |
           ((uint32_t)bytes[2] << 8) | bytes[3];
}

static uint64_t get_u64(const uint8_t *bytes)
{
    return ((uint64_t)get_u32(bytes) << 32) | get_u32(bytes + 4);
}

static void put_u16(uint8_t *bytes, uint16_t value)
{
    bytes[0] = (uint8_t)(value >> 8);
    bytes[1] = (uint8_t)value;
}

static void put_u32(uint8_t *bytes, uint32_t value)
{
    bytes[0] = (uint8_t)(value >> 24);
    bytes[1] = (uint8_t)(value >> 16);
    bytes[2] = (uint8_t)(value >> 8);
    bytes[3] = (uint8_t)value;
}

static void put_u64(uint8_t *bytes, uint64_t value)
{
    put_u32(bytes, (uint32_t)(value >> 32));
    put_u32(bytes + 4, (uint32_t)value);
}

static uint64_t checksum64(const uint8_t *bytes, size_t len)
{
    uint64_t hash = UINT64_C(0xcbf29ce484222325);
    size_t i;

    for (i = 0; i < len; ++i) {
        hash = (hash ^ bytes[i]) * UINT64_C(0x100000001b3);
    }
    return hash;
}

static int read_exact(int fd, void *buffer, size_t len)
{
    uint8_t *bytes = buffer;
    size_t done = 0;

    while (done < len) {
        ssize_t rc = read(fd, bytes + done, len - done);
        if (rc == 0) {
            return 1;
        }
        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        done += (size_t)rc;
    }
    return 0;
}

static int write_exact(int fd, const void *buffer, size_t len)
{
    const uint8_t *bytes = buffer;
    size_t done = 0;

    while (done < len) {
        ssize_t rc = write(fd, bytes + done, len - done);
        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        done += (size_t)rc;
    }
    return 0;
}

static int copy_field(char *out,
                      size_t out_len,
                      const uint8_t *bytes,
                      size_t len)
{
    if (out == NULL || out_len == 0 || len == 0 || len >= out_len ||
        memchr(bytes, '\0', len) != NULL) {
        return -1;
    }
    memcpy(out, bytes, len);
    out[len] = '\0';
    return 0;
}

static void free_frame(struct bridge_frame *frame)
{
    if (frame != NULL) {
        free(frame->body);
        memset(frame, 0, sizeof(*frame));
    }
}

static int decode_frame(const uint8_t *bytes,
                        uint32_t frame_len,
                        struct bridge_frame *frame)
{
    uint16_t lengths[4];
    uint32_t body_len;
    uint64_t expected_checksum;
    size_t cursor = BRIDGE_HEADER_BYTES;
    size_t expected_len;
    size_t i;
    char *fields[4];
    size_t field_sizes[4];

    if (bytes == NULL || frame == NULL || frame_len < BRIDGE_HEADER_BYTES ||
        memcmp(bytes, BRIDGE_MAGIC, 8) != 0 || get_u16(bytes + 8) != BRIDGE_MAJOR) {
        return -1;
    }
    body_len = get_u32(bytes + 40);
    if (body_len > BRIDGE_MAX_BODY) {
        return -1;
    }
    for (i = 0; i < 4; ++i) {
        lengths[i] = get_u16(bytes + 60 + i * 2);
    }
    expected_len = BRIDGE_HEADER_BYTES + body_len;
    for (i = 0; i < 4; ++i) {
        expected_len += lengths[i];
    }
    if (expected_len != frame_len || get_u64(bytes + 16) == 0 ||
        get_u64(bytes + 24) == 0 || get_u64(bytes + 32) == 0) {
        return -1;
    }
    memset(frame, 0, sizeof(*frame));
    frame->message_type = get_u16(bytes + 12);
    frame->flags = get_u16(bytes + 14);
    frame->request_id = get_u64(bytes + 16);
    frame->correlation_id = get_u64(bytes + 24);
    frame->session_generation = get_u64(bytes + 32);
    frame->status = (int32_t)get_u32(bytes + 44);
    frame->error_code = (int32_t)get_u32(bytes + 48);
    expected_checksum = get_u64(bytes + 52);
    fields[0] = frame->execution_realm_id;
    fields[1] = frame->node_id;
    fields[2] = frame->boot_id;
    fields[3] = frame->provider_session_id;
    field_sizes[0] = sizeof(frame->execution_realm_id);
    field_sizes[1] = sizeof(frame->node_id);
    field_sizes[2] = sizeof(frame->boot_id);
    field_sizes[3] = sizeof(frame->provider_session_id);
    for (i = 0; i < 4; ++i) {
        if (copy_field(fields[i], field_sizes[i], bytes + cursor, lengths[i]) != 0) {
            return -1;
        }
        cursor += lengths[i];
    }
    frame->body_len = body_len;
    frame->body = malloc((size_t)body_len + 1U);
    if (frame->body == NULL) {
        return -1;
    }
    memcpy(frame->body, bytes + cursor, body_len);
    frame->body[body_len] = '\0';
    if (checksum64(frame->body, body_len) != expected_checksum) {
        free_frame(frame);
        return -1;
    }
    return 0;
}

static int encode_frame(const struct bridge_frame *frame,
                        uint8_t **bytes_out,
                        uint32_t *len_out)
{
    const char *fields[4];
    uint16_t lengths[4];
    size_t total = BRIDGE_HEADER_BYTES + frame->body_len;
    size_t cursor = BRIDGE_HEADER_BYTES;
    uint8_t *bytes;
    size_t i;

    fields[0] = frame->execution_realm_id;
    fields[1] = frame->node_id;
    fields[2] = frame->boot_id;
    fields[3] = frame->provider_session_id;
    if (frame->request_id == 0 || frame->correlation_id == 0 ||
        frame->session_generation == 0 || frame->body_len > BRIDGE_MAX_BODY) {
        return -1;
    }
    for (i = 0; i < 4; ++i) {
        size_t len = strlen(fields[i]);
        if (len == 0 || len > UINT16_MAX) {
            return -1;
        }
        lengths[i] = (uint16_t)len;
        total += len;
    }
    if (total > UINT32_MAX) {
        return -1;
    }
    bytes = calloc(1, total);
    if (bytes == NULL) {
        return -1;
    }
    memcpy(bytes, BRIDGE_MAGIC, 8);
    put_u16(bytes + 8, BRIDGE_MAJOR);
    put_u16(bytes + 10, BRIDGE_MINOR);
    put_u16(bytes + 12, frame->message_type);
    put_u16(bytes + 14, frame->flags);
    put_u64(bytes + 16, frame->request_id);
    put_u64(bytes + 24, frame->correlation_id);
    put_u64(bytes + 32, frame->session_generation);
    put_u32(bytes + 40, frame->body_len);
    put_u32(bytes + 44, (uint32_t)frame->status);
    put_u32(bytes + 48, (uint32_t)frame->error_code);
    put_u64(bytes + 52, checksum64(frame->body, frame->body_len));
    for (i = 0; i < 4; ++i) {
        put_u16(bytes + 60 + i * 2, lengths[i]);
        memcpy(bytes + cursor, fields[i], lengths[i]);
        cursor += lengths[i];
    }
    memcpy(bytes + cursor, frame->body, frame->body_len);
    *bytes_out = bytes;
    *len_out = (uint32_t)total;
    return 0;
}

static int read_frame(int fd,
                      struct bridge_frame *frame,
                      uint64_t *fingerprint_out)
{
    uint8_t length_bytes[4];
    uint8_t *bytes;
    uint32_t len;
    int rc;

    rc = read_exact(fd, length_bytes, sizeof(length_bytes));
    if (rc != 0) {
        return rc;
    }
    len = get_u32(length_bytes);
    if (len < BRIDGE_HEADER_BYTES || len > BRIDGE_MAX_FRAME) {
        return -1;
    }
    bytes = malloc(len);
    if (bytes == NULL) {
        return -1;
    }
    rc = read_exact(fd, bytes, len);
    if (rc == 0) {
        *fingerprint_out = checksum64(bytes, len);
        rc = decode_frame(bytes, len, frame);
    }
    free(bytes);
    return rc;
}

static int write_encoded_frame(int fd, const uint8_t *bytes, uint32_t len)
{
    uint8_t length_bytes[4];

    put_u32(length_bytes, len);
    return write_exact(fd, length_bytes, sizeof(length_bytes)) == 0 &&
                   write_exact(fd, bytes, len) == 0
               ? 0
               : -1;
}

static const char *body_value(const struct bridge_frame *frame,
                              const char *key,
                              char *out,
                              size_t out_len)
{
    const char *body = (const char *)frame->body;
    size_t key_len = strlen(key);
    const char *cursor = body;

    if (out == NULL || out_len == 0) {
        return NULL;
    }
    while (*cursor != '\0') {
        const char *end = strchr(cursor, ';');
        size_t part_len = end != NULL ? (size_t)(end - cursor) : strlen(cursor);
        if (part_len > key_len + 1U && memcmp(cursor, key, key_len) == 0 &&
            cursor[key_len] == '=') {
            size_t value_len = part_len - key_len - 1U;
            if (value_len >= out_len) {
                return NULL;
            }
            memcpy(out, cursor + key_len + 1U, value_len);
            out[value_len] = '\0';
            return out;
        }
        if (end == NULL) {
            break;
        }
        cursor = end + 1;
    }
    return NULL;
}

static int body_u64(const struct bridge_frame *frame,
                    const char *key,
                    uint64_t *value_out)
{
    char text[64];
    char *end = NULL;
    unsigned long long value;

    if (body_value(frame, key, text, sizeof(text)) == NULL) {
        return -1;
    }
    errno = 0;
    value = strtoull(text, &end, 0);
    if (errno != 0 || end == text || *end != '\0') {
        return -1;
    }
    *value_out = (uint64_t)value;
    return 0;
}

static int hex_decode(const char *text, uint8_t *bytes, size_t capacity, uint32_t *len_out)
{
    size_t text_len = strlen(text);
    size_t i;

    if ((text_len & 1U) != 0 || text_len / 2U > capacity) {
        return -1;
    }
    for (i = 0; i < text_len / 2U; ++i) {
        unsigned int value;
        if (sscanf(text + i * 2U, "%2x", &value) != 1) {
            return -1;
        }
        bytes[i] = (uint8_t)value;
    }
    *len_out = (uint32_t)(text_len / 2U);
    return 0;
}

static int hex_encode(const uint8_t *bytes,
                      uint32_t len,
                      char *out,
                      size_t out_len)
{
    static const char digits[] = "0123456789abcdef";
    uint32_t i;

    if (out_len < (size_t)len * 2U + 1U) {
        return -1;
    }
    for (i = 0; i < len; ++i) {
        out[i * 2U] = digits[bytes[i] >> 4];
        out[i * 2U + 1U] = digits[bytes[i] & 0xfU];
    }
    out[len * 2U] = '\0';
    return 0;
}

static void init_response(const struct agent_state *state,
                          const struct bridge_frame *request,
                          struct bridge_frame *response,
                          uint16_t message_type,
                          const char *body)
{
    memset(response, 0, sizeof(*response));
    response->message_type = message_type;
    response->flags = 1U;
    response->request_id = request->request_id;
    response->correlation_id = request->correlation_id;
    response->session_generation = state->identity.provider_session_generation;
    snprintf(response->execution_realm_id,
             sizeof(response->execution_realm_id),
             "%s",
             request->execution_realm_id);
    snprintf(response->node_id, sizeof(response->node_id), "%s", state->node_id);
    snprintf(response->boot_id, sizeof(response->boot_id), "%s", state->boot_id);
    snprintf(response->provider_session_id,
             sizeof(response->provider_session_id),
             "%s",
             state->identity.provider_session_id);
    response->body_len = (uint32_t)strlen(body);
    response->body = malloc((size_t)response->body_len + 1U);
    if (response->body != NULL) {
        memcpy(response->body, body, response->body_len + 1U);
    }
}

static void fail_response(const struct agent_state *state,
                          const struct bridge_frame *request,
                          struct bridge_frame *response,
                          int error_code,
                          const char *detail)
{
    char body[512];

    snprintf(body,
             sizeof(body),
             "terminal=failed;error_code=%d;detail=%s",
             error_code,
             detail);
    init_response(state, request, response, BRIDGE_TYPE_ERROR, body);
    response->status = -1;
    response->error_code = error_code;
}

static int submit_and_poll(struct agent_state *state,
                           struct mem_service_obmm_sdk_submission *submission,
                           struct mem_service_obmm_sdk_completion *completion)
{
    uint64_t completion_id = 0;
    int rc;

    rc = mem_service_obmm_sdk_submit(state->session, submission, &completion_id);
    if (rc != MEM_SERVICE_OBMM_SDK_OK) {
        return rc;
    }
    rc = mem_service_obmm_sdk_poll(state->session, completion_id, completion);
    if (rc != MEM_SERVICE_OBMM_SDK_OK) {
        return rc;
    }
    return completion->state == MEM_SERVICE_OBMM_SDK_COMPLETION_SUCCEEDED
               ? MEM_SERVICE_OBMM_SDK_OK
               : completion->error_code;
}

static int fill_resource(const struct bridge_frame *request,
                         struct mem_service_obmm_sdk_resource *resource)
{
    return body_u64(request, "resource_id", &resource->resource_id) == 0 &&
                   body_u64(request,
                            "resource_generation",
                            &resource->resource_generation) == 0 &&
                   body_u64(request,
                            "resource_session_generation",
                            &resource->provider_session_generation) == 0 &&
                   body_u64(request, "resource_size", &resource->size_bytes) == 0
               ? 0
               : -1;
}

static int fill_access(const struct bridge_frame *request,
                       struct mem_service_obmm_sdk_access *access)
{
    return body_u64(request, "access_id", &access->access_id) == 0 &&
                   body_u64(request,
                            "access_generation",
                            &access->access_generation) == 0 &&
                   body_u64(request,
                            "access_session_generation",
                            &access->provider_session_generation) == 0 &&
                   body_u64(request, "access_offset", &access->offset_bytes) == 0 &&
                   body_u64(request, "access_nbytes", &access->nbytes) == 0
               ? 0
               : -1;
}

static void handle_request(struct agent_state *state,
                           const struct bridge_frame *request,
                           struct bridge_frame *response)
{
    struct mem_service_obmm_sdk_submission submission;
    struct mem_service_obmm_sdk_completion completion;
    enum mem_service_obmm_sdk_health health;
    char body[BRIDGE_BODY_SCRATCH];
    char value[1024];
    char descriptor_hex[MEM_SERVICE_OBMM_SDK_DESCRIPTOR_MAX * 2U + 1U];
    uint64_t number;
    int rc;

    if (request->message_type == BRIDGE_TYPE_IDENTITY) {
        snprintf(body,
                 sizeof(body),
                 "terminal=complete;provider=obmm;api_major=%u;api_minor=%u;"
                 "abi_major=%u;session_id=%s;session_generation=%" PRIu64
                 ";capabilities=%" PRIu64,
                 state->identity.api_major,
                 state->identity.api_minor,
                 state->identity.abi_major,
                 state->identity.provider_session_id,
                 state->identity.provider_session_generation,
                 state->identity.capabilities);
        init_response(state, request, response, BRIDGE_TYPE_COMPLETION, body);
        return;
    }
    if (request->provider_session_id[0] == '\0' ||
        strcmp(request->provider_session_id,
               state->identity.provider_session_id) != 0 ||
        request->session_generation !=
            state->identity.provider_session_generation) {
        fail_response(state,
                      request,
                      response,
                      MEM_SERVICE_OBMM_SDK_SESSION_STALE,
                      "provider-session-stale");
        return;
    }
    if (request->message_type == BRIDGE_TYPE_PROBE) {
        rc = mem_service_obmm_sdk_probe(state->session, &health);
        if (rc != MEM_SERVICE_OBMM_SDK_OK) {
            fail_response(state, request, response, rc, "probe-failed");
            return;
        }
        snprintf(body,
                 sizeof(body),
                 "terminal=complete;health=%d;fidelity=functional;limitations=fixed-2node-readonly-4k",
                 health);
        init_response(state, request, response, BRIDGE_TYPE_COMPLETION, body);
        return;
    }
    if (request->message_type == BRIDGE_TYPE_SHUTDOWN) {
        init_response(state,
                      request,
                      response,
                      BRIDGE_TYPE_COMPLETION,
                      "terminal=complete;shutdown=accepted");
        return;
    }

    memset(&submission, 0, sizeof(submission));
    memset(&completion, 0, sizeof(completion));
    submission.request_id = request->request_id;
    if (request->message_type == BRIDGE_TYPE_ALLOCATE) {
        submission.operation = MEM_SERVICE_OBMM_SDK_OP_ALLOCATE;
        if (body_u64(request, "size", &submission.size_bytes) != 0) {
            fail_response(state, request, response, -1001, "missing-size");
            return;
        }
    } else if (request->message_type == BRIDGE_TYPE_PUBLISH ||
               request->message_type == BRIDGE_TYPE_SEED) {
        submission.operation = MEM_SERVICE_OBMM_SDK_OP_SEED_PUBLISH;
        if (fill_resource(request, &submission.resource) != 0 ||
            body_u64(request, "offset", &submission.offset_bytes) != 0 ||
            body_u64(request, "nbytes", &submission.nbytes) != 0 ||
            body_u64(request, "seed", &number) != 0 || number > UINT8_MAX) {
            fail_response(state, request, response, -1001, "invalid-publish-fields");
            return;
        }
        submission.deterministic_seed = (uint8_t)number;
    } else if (request->message_type == BRIDGE_TYPE_IMPORT) {
        submission.operation = MEM_SERVICE_OBMM_SDK_OP_IMPORT_READ_ONLY;
        if (body_value(request, "descriptor", value, sizeof(value)) == NULL ||
            hex_decode(value,
                       submission.descriptor.bytes,
                       sizeof(submission.descriptor.bytes),
                       &submission.descriptor.len) != 0 ||
            body_value(request,
                       "source_session_id",
                       value,
                       sizeof(value)) == NULL) {
            fail_response(state, request, response, -1008, "invalid-descriptor");
            return;
        }
        submission.expected_source_session_id = strdup(value);
        if (submission.expected_source_session_id == NULL ||
            body_u64(request,
                     "source_session_generation",
                     &submission.expected_source_session_generation) != 0 ||
            body_u64(request, "offset", &submission.offset_bytes) != 0 ||
            body_u64(request, "nbytes", &submission.nbytes) != 0) {
            free((void *)submission.expected_source_session_id);
            fail_response(state, request, response, -1001, "invalid-import-fields");
            return;
        }
    } else if (request->message_type == BRIDGE_TYPE_CHECKSUM) {
        submission.operation = MEM_SERVICE_OBMM_SDK_OP_CHECKSUM;
        if (fill_access(request, &submission.access) != 0 ||
            body_u64(request, "offset", &submission.offset_bytes) != 0 ||
            body_u64(request, "nbytes", &submission.nbytes) != 0) {
            fail_response(state, request, response, -1001, "invalid-checksum-fields");
            return;
        }
        if (body_u64(request, "expected_checksum", &number) == 0) {
            submission.expected_checksum = number;
        }
        submission.timeout_ms = 30000U;
    } else if (request->message_type == BRIDGE_TYPE_REVOKE) {
        submission.operation = MEM_SERVICE_OBMM_SDK_OP_REVOKE;
        if (fill_access(request, &submission.access) != 0) {
            fail_response(state, request, response, -1001, "invalid-access");
            return;
        }
    } else if (request->message_type == BRIDGE_TYPE_RELEASE) {
        submission.operation = MEM_SERVICE_OBMM_SDK_OP_RELEASE;
        if (fill_resource(request, &submission.resource) != 0) {
            fail_response(state, request, response, -1001, "invalid-resource");
            return;
        }
    } else {
        fail_response(state, request, response, -1001, "unknown-operation");
        return;
    }

    rc = submit_and_poll(state, &submission, &completion);
    free((void *)submission.expected_source_session_id);
    if (rc != MEM_SERVICE_OBMM_SDK_OK) {
        fail_response(state,
                      request,
                      response,
                      rc,
                      mem_service_obmm_sdk_error_name(rc));
        return;
    }
    if (completion.operation == MEM_SERVICE_OBMM_SDK_OP_ALLOCATE) {
        snprintf(body,
                 sizeof(body),
                 "terminal=complete;completion_id=%" PRIu64
                 ";resource_id=%" PRIu64 ";resource_generation=%" PRIu64
                 ";resource_session_generation=%" PRIu64
                 ";resource_size=%" PRIu64,
                 completion.completion_id,
                 completion.resource.resource_id,
                 completion.resource.resource_generation,
                 completion.resource.provider_session_generation,
                 completion.resource.size_bytes);
    } else if (completion.operation == MEM_SERVICE_OBMM_SDK_OP_SEED_PUBLISH) {
        if (hex_encode(completion.descriptor.bytes,
                       completion.descriptor.len,
                       descriptor_hex,
                       sizeof(descriptor_hex)) != 0) {
            fail_response(state, request, response, -1099, "descriptor-encode");
            return;
        }
        snprintf(body,
                 sizeof(body),
                 "terminal=complete;completion_id=%" PRIu64
                 ";descriptor=%s;checksum=%" PRIu64 ";bytes_done=%" PRIu64,
                 completion.completion_id,
                 descriptor_hex,
                 completion.checksum,
                 completion.bytes_done);
    } else if (completion.operation == MEM_SERVICE_OBMM_SDK_OP_IMPORT_READ_ONLY) {
        snprintf(body,
                 sizeof(body),
                 "terminal=complete;completion_id=%" PRIu64
                 ";access_id=%" PRIu64 ";access_generation=%" PRIu64
                 ";access_session_generation=%" PRIu64
                 ";access_offset=%" PRIu64 ";access_nbytes=%" PRIu64
                 ";read_only=1",
                 completion.completion_id,
                 completion.access.access_id,
                 completion.access.access_generation,
                 completion.access.provider_session_generation,
                 completion.access.offset_bytes,
                 completion.access.nbytes);
    } else {
        snprintf(body,
                 sizeof(body),
                 "terminal=complete;completion_id=%" PRIu64
                 ";checksum=%" PRIu64 ";bytes_done=%" PRIu64,
                 completion.completion_id,
                 completion.checksum,
                 completion.bytes_done);
    }
    init_response(state, request, response, BRIDGE_TYPE_COMPLETION, body);
}

static struct replay_slot *find_replay(struct agent_state *state,
                                       uint64_t request_id)
{
    size_t i;

    for (i = 0; i < REPLAY_SLOTS; ++i) {
        if (state->replay[i].active &&
            state->replay[i].request_id == request_id) {
            return &state->replay[i];
        }
    }
    return NULL;
}

static void remember_replay(struct agent_state *state,
                            uint64_t request_id,
                            uint64_t fingerprint,
                            const uint8_t *response,
                            uint32_t response_len)
{
    struct replay_slot *slot = &state->replay[
        (size_t)((request_id - 1U) % REPLAY_SLOTS)];

    free(slot->response);
    memset(slot, 0, sizeof(*slot));
    slot->response = malloc(response_len);
    if (slot->response == NULL) {
        return;
    }
    memcpy(slot->response, response, response_len);
    slot->active = true;
    slot->request_id = request_id;
    slot->fingerprint = fingerprint;
    slot->response_len = response_len;
}

static int parse_args(int argc,
                      char **argv,
                      const char **device_out,
                      const char **node_id_out)
{
    int i;

    *device_out = NULL;
    *node_id_out = NULL;
    for (i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--device") == 0 && i + 1 < argc) {
            *device_out = argv[++i];
        } else if (strcmp(argv[i], "--node-id") == 0 && i + 1 < argc) {
            *node_id_out = argv[++i];
        } else {
            return -1;
        }
    }
    return *device_out != NULL && *node_id_out != NULL ? 0 : -1;
}

int main(int argc, char **argv)
{
    struct mem_service_obmm_sdk_config config;
    struct agent_state state;
    const char *device;
    const char *node_id;
    int fd;
    int rc = 1;

    if (parse_args(argc, argv, &device, &node_id) != 0) {
        fprintf(stderr,
                "usage: %s --device <virtio-port> --node-id <nodeA|nodeB>\n",
                argv[0]);
        return 2;
    }
    memset(&state, 0, sizeof(state));
    snprintf(state.node_id, sizeof(state.node_id), "%s", node_id);
    snprintf(state.boot_id,
             sizeof(state.boot_id),
             "%s:%ld",
             node_id,
             (long)getpid());
    mem_service_obmm_sdk_default_config(&config);
    config.instance = node_id;
    config.max_remote_mappings = 8U;
    config.required_peer_mappings = 1U;
    if (mem_service_obmm_sdk_session_create(&config, &state.session) !=
            MEM_SERVICE_OBMM_SDK_OK ||
        mem_service_obmm_sdk_get_identity(state.session, &state.identity) !=
            MEM_SERVICE_OBMM_SDK_OK) {
        fprintf(stderr, "uf-ub-sim-agent: provider session create failed\n");
        goto done;
    }
    fd = open(device, O_RDWR);
    if (fd < 0) {
        fprintf(stderr,
                "uf-ub-sim-agent: open %s failed: %s\n",
                device,
                strerror(errno));
        goto done;
    }
    fprintf(stderr,
            "uf-ub-sim-agent: ready node=%s session=%s generation=%" PRIu64
            " bridge=uflow-sim-bridge-v1\n",
            state.node_id,
            state.identity.provider_session_id,
            state.identity.provider_session_generation);
    while (true) {
        struct bridge_frame request;
        struct bridge_frame response;
        struct replay_slot *replay;
        uint64_t fingerprint = 0;
        uint8_t *encoded = NULL;
        uint32_t encoded_len = 0;
        int read_rc = read_frame(fd, &request, &fingerprint);

        if (read_rc == 1) {
            rc = 0;
            break;
        }
        if (read_rc != 0) {
            fprintf(stderr, "uf-ub-sim-agent: malformed frame\n");
            continue;
        }
        replay = find_replay(&state, request.request_id);
        if (replay != NULL && replay->fingerprint == fingerprint) {
            (void)write_encoded_frame(fd, replay->response, replay->response_len);
            free_frame(&request);
            continue;
        }
        if (replay != NULL) {
            fail_response(&state,
                          &request,
                          &response,
                          -1006,
                          "request-id-replay-conflict");
        } else {
            handle_request(&state, &request, &response);
        }
        if (response.body == NULL || encode_frame(&response, &encoded, &encoded_len) != 0 ||
            write_encoded_frame(fd, encoded, encoded_len) != 0) {
            free(encoded);
            free_frame(&response);
            free_frame(&request);
            break;
        }
        remember_replay(&state,
                        request.request_id,
                        fingerprint,
                        encoded,
                        encoded_len);
        state.request_count += 1U;
        free(encoded);
        free_frame(&response);
        free_frame(&request);
    }
    close(fd);

done:
    mem_service_obmm_sdk_session_destroy(state.session);
    for (size_t i = 0; i < REPLAY_SLOTS; ++i) {
        free(state.replay[i].response);
    }
    return rc;
}
