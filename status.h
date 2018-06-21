#ifndef __KVS_STATUS_H__
#define __KVS_STATUS_H__
#include <stdint.h>

typedef int32_t kvs_status;

#define KVS_OK (0)
#define KVS_INVALID_VARIANT_TYPE (-1)
#define KVS_INSUFFICIENT_BUFFER (-2)
#define KVS_OUT_OF_MEMORY (-3)
#define KVS_STORE_EOF (-100)
#define KVS_STORE_CORRUPTED (-101)
#define KVS_STORE_INTERNAL_ERROR (-102)
#define KVS_SCHEMA_COLUMN_NOT_FOUND (-200)
#define KVS_SCHEMA_JIT_NOT_SUPPORTED (-201)
#define KVS_SCHEMA_JIT_INVALID_RUNTIME (-202)
#define KVS_SCHEMA_JIT_INTERNAL_ERROR (-203)

#define KVS_FAILED(st) ((st) != KVS_OK)

#endif /* __KVS_STATUS_H__ */
