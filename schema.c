#include "schema.h"
#include "record.h"
#include "util.h"
#include "status.h"
#include <stdlib.h>
#include <string.h>

typedef void (*kvs_schema_serializer)(const kvs_column **keys, size_t key_size, const kvs_column **values, size_t value_size, kvs_record *record, kvs_buffer *key, kvs_buffer *value, void *opaque);
typedef void (*kvs_schema_codec_destructor)(const kvs_column **keys, size_t key_size, const kvs_column **values, size_t value_size, void *opaque);
typedef void (*kvs_schema_deserializer)(const kvs_column **keys, size_t key_size, const kvs_column **values, size_t value_size, kvs_buffer *key, kvs_buffer *value, void *opaque, kvs_record *dest);

struct kvs_schema {
  kvs_column *columns;
  kvs_variant **dfts;
  size_t size;
  const kvs_column **keys;
  size_t key_size;
  const kvs_column **values;
  size_t value_size;
  void *codec;
  kvs_schema_serializer serializer;
  kvs_schema_deserializer deserializer;
  kvs_schema_codec_destructor codec_destructor;
};

struct kvs_schema_projection {
  size_t *index;
};

#define __KVS_SCHEMA_INTERNAL_H__
#include "interpret.h"
#include "prepared.h"
#include "jit.h"
#undef __KVS_SCHEMA_INTERNAL_H__

kvs_schema *kvs_schema_create(const kvs_column *columns, size_t size, int32_t flags) {
  size_t idx, fixed = 0, varlen, fixed_size, key_size = 0, key = 0, value = 0;
  kvs_schema *schema;
  kvs_column *current;
  for (idx = 0; idx < size; ++idx) {
    if (columns[idx].pk) {
      key_size++;
    }
  }
  if (key_size == 0) {
    return NULL;
  }
  schema = malloc(sizeof(kvs_schema) + (sizeof(kvs_column) * size) + (sizeof(kvs_column *) * size) + (sizeof(kvs_variant *) * size));
  schema->columns = KVS_UNSAFE_CAST(schema, sizeof(kvs_schema));
  schema->keys = KVS_UNSAFE_CAST(schema->columns, sizeof(kvs_column) * size);
  schema->values = KVS_UNSAFE_CAST(schema->keys, sizeof(kvs_column *) * key_size);
  schema->dfts = KVS_UNSAFE_CAST(schema->values, sizeof(kvs_column *) * (size - key_size));
  schema->size = size;
  varlen = size;
  for (idx = 0; idx < size; ++idx) {
    if ((fixed_size = kvs_variant_type_size(columns[idx].type)) != 0) {
      schema->dfts[fixed] = kvs_variant_create(columns[idx].type);
      *(current = schema->columns + fixed) = columns[idx];
      current->index = fixed++;
    } else {
      schema->dfts[--varlen] = kvs_variant_create(columns[idx].type);
      *(current = schema->columns + (varlen)) = columns[idx];
      current->index = varlen;
    }
    current->name = strdup(columns[idx].name);
    if (current->pk) {
      schema->keys[key++] = current;
    } else {
      schema->values[value++] = current;
    }
  }
  schema->key_size = key_size;
  schema->value_size = size - key_size;
  if ((flags & KVS_SCHEMA_FLAG_JIT) == KVS_SCHEMA_FLAG_JIT) {
    schema->serializer = kvs_schema_jit_serializer;
    schema->deserializer = kvs_schema_jit_deserializer;
    schema->codec = kvs_schema_jit_codec_create(schema->keys, schema->key_size, schema->values, schema->value_size);
    schema->codec_destructor = kvs_schema_jit_codec_destroy;
  } else if ((flags & KVS_SCHEMA_FLAG_PREPARED) == KVS_SCHEMA_FLAG_PREPARED) {
    schema->serializer = kvs_schema_prepared_serializer;
    schema->deserializer = kvs_schema_prepared_deserializer;
    schema->codec = kvs_schema_prepared_codec_create(schema->keys, schema->key_size, schema->values, schema->value_size);
    schema->codec_destructor = kvs_schema_prepared_codec_destroy;
  } else {
    schema->serializer = kvs_schema_interpret_serializer;
    schema->deserializer = kvs_schema_interpret_deserializer;
    schema->codec = kvs_schema_interpret_codec_create(schema->keys, schema->key_size, schema->values, schema->value_size);
    schema->codec_destructor = kvs_schema_interpret_codec_destroy;
  }
  if (schema->codec == NULL) {
    kvs_schema_destroy(schema);
    schema = NULL;
  }
  return schema;
}

void kvs_schema_destroy(kvs_schema *schema) {
  size_t idx;
  for (idx = 0; idx < schema->size; ++idx) {
    kvs_variant_destroy(schema->dfts[idx]);
    free((char *) schema->columns[idx].name);
  }
  schema->codec_destructor(schema->keys, schema->key_size, schema->values, schema->value_size, schema->codec);
  free(schema);
}

kvs_record *kvs_schema_record_create(const kvs_schema *schema) {
  return kvs_record_create(schema->dfts, schema->size);
}

void kvs_schema_record_serialize(const kvs_schema *schema, kvs_record *record, kvs_buffer *key, kvs_buffer *value) {
  schema->serializer(schema->keys, schema->key_size, schema->values, schema->value_size, record, key, value, schema->codec);
}

void kvs_schema_record_deserialize(const kvs_schema *schema, kvs_buffer *key, kvs_buffer *value, kvs_record *dest) {
  schema->deserializer(schema->keys, schema->key_size, schema->values, schema->value_size, key, value, schema->codec, dest);
}

static kvs_status kvs_schema_column_lookup(const kvs_schema *schema, const char *column, size_t *index) {
  /* TODO implement this with hash lookup */
  size_t idx;
  for (idx = 0; idx < schema->size; ++idx) {
    if (strcasecmp(schema->columns[idx].name, column) == 0) {
      *index = schema->columns[idx].index;
      return KVS_OK;
    }
  }
  return KVS_SCHEMA_COLUMN_NOT_FOUND;
}

kvs_variant **kvs_schema_record_get(const kvs_schema *schema, kvs_record *record, const char *column) {
  size_t index;
  if (kvs_schema_column_lookup(schema, column, &index) == KVS_OK) {
    return kvs_record_get(record, index);
  }
  return NULL;
}

kvs_schema_projection *kvs_schema_projection_create(const kvs_schema *schema, const char **columns, size_t num_column) {
  size_t idx, index;
  kvs_schema_projection *projection = malloc(sizeof(kvs_schema_projection) + (sizeof(size_t) * num_column));
  projection->index = KVS_UNSAFE_CAST(projection, sizeof(kvs_schema_projection));
  for (idx = 0; idx < num_column; ++idx) {
    if (kvs_schema_column_lookup(schema, columns[idx], &index) == KVS_OK) {
      projection->index[idx] = index;
    } else {
      free(projection);
      projection = NULL;
      break;
    }
  }
  return projection;
}

void kvs_schema_projection_destroy(kvs_schema_projection *projection) {
  free(projection);
}

kvs_variant **kvs_schema_projection_record_get(const kvs_schema_projection *projection, kvs_record *record, size_t index) {
  return kvs_record_get(record, projection->index[index]);
}
