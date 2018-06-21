#ifndef __KVS_SCHEMA_H__
#define __KVS_SCHEMA_H__
#include "variant.h"
#include "buffer.h"
#include "record.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct kvs_column {
  const char *name;
  kvs_variant_type type;
  int32_t pk;
  /* leave the next field for schema to initialize */
  size_t index;
} kvs_column;

typedef struct kvs_schema kvs_schema;

typedef enum kvs_schema_flag {
  KVS_SCHEMA_FLAG_DEFAULT = 0,
  KVS_SCHEMA_FLAG_INTERPRETED = 0,
  KVS_SCHEMA_FLAG_PREPARED = 1 << 0,
  KVS_SCHEMA_FLAG_JIT = 1 << 1
} kvs_schema_flag;

typedef struct kvs_schema_projection kvs_schema_projection;

kvs_record *kvs_schema_record_create(const kvs_schema *schema);
kvs_schema *kvs_schema_create(const kvs_column *columns, size_t size, int32_t flags);
void kvs_schema_destroy(kvs_schema *schema);

void kvs_schema_record_serialize(const kvs_schema *schema, kvs_record *record, kvs_buffer *key, kvs_buffer *value);
void kvs_schema_record_deserialize(const kvs_schema *schema, kvs_buffer *key, kvs_buffer *value, kvs_record *dest);
kvs_variant **kvs_schema_record_get(const kvs_schema *schema, kvs_record *record, const char *column);
kvs_schema_projection *kvs_schema_projection_create(const kvs_schema *schema, const char **columns, size_t num_column);
void kvs_schema_projection_destroy(kvs_schema_projection *projection);
kvs_variant **kvs_schema_projection_record_get(const kvs_schema_projection *projection, kvs_record *record, size_t index);

#ifdef __cplusplus
}
#endif

#endif /* __KVS_SCHEMA_H__ */
