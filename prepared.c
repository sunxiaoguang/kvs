#include "schema.h"
#include "buffer.h"
#include "record.h"
#include "util.h"
#define __KVS_SCHEMA_INTERNAL_H__
#include "prepared.h"
#undef __KVS_SCHEMA_INTERNAL_H__
#include <string.h>

typedef void (*kvs_schema_prepared_field_serializer)(const kvs_variant *variant, kvs_buffer *buffer);

typedef struct kvs_schema_prepared_serializer_descriptor {
  kvs_schema_prepared_field_serializer *key;
  kvs_schema_prepared_field_serializer *value;
} kvs_schema_prepared_serializer_descriptor;

typedef kvs_variant *(*kvs_schema_prepared_field_deserializer)(kvs_variant *dest, kvs_buffer *data);

typedef struct kvs_schema_prepared_deserializer_descriptor {
  kvs_schema_prepared_field_deserializer *key;
  kvs_schema_prepared_field_deserializer *value;
} kvs_schema_prepared_deserializer_descriptor;

typedef struct kvs_schema_prepared_codec {
  kvs_schema_prepared_serializer_descriptor *serializer;
  kvs_schema_prepared_deserializer_descriptor *deserializer;
} kvs_schema_prepared_codec;

void kvs_schema_prepared_serializer(const kvs_column **keys, size_t key_size, const kvs_column **values, size_t value_size, kvs_record *record, kvs_buffer *key, kvs_buffer *value, void *opaque) {
  size_t idx;
  const kvs_variant *variant;
  kvs_schema_prepared_codec *codec = opaque;
  kvs_schema_prepared_serializer_descriptor *descriptor = codec->serializer;
  for (idx = 0; idx < key_size; ++idx) {
    variant = *kvs_record_get(record, keys[idx]->index);
    descriptor->key[idx](variant, key);
  }
  for (idx = 0; idx < value_size; ++idx) {
    variant = *kvs_record_get(record, values[idx]->index);
    descriptor->value[idx](variant, value);
  }
}

void *kvs_schema_prepared_codec_create(const kvs_column **keys, size_t key_size, const kvs_column **values, size_t value_size) {
  size_t idx;
  const kvs_column *column;
  kvs_schema_prepared_codec *codec = malloc(sizeof(kvs_schema_prepared_codec) + 
      sizeof(kvs_schema_prepared_serializer_descriptor) + (sizeof(kvs_schema_prepared_field_serializer) * (key_size + value_size)) + 
      sizeof(kvs_schema_prepared_deserializer_descriptor) + (sizeof(kvs_schema_prepared_field_deserializer) * (key_size + value_size)));
  kvs_schema_prepared_serializer_descriptor *serializer = KVS_UNSAFE_CAST(codec, sizeof(kvs_schema_prepared_codec));
  serializer->key = KVS_UNSAFE_CAST(serializer, sizeof(kvs_schema_prepared_serializer_descriptor));
  serializer->value = KVS_UNSAFE_CAST(serializer->key, sizeof(kvs_schema_prepared_field_serializer) * key_size);
  for (idx = 0; idx < key_size; ++idx) {
    column = keys[idx];
    switch (column->type) {
      case KVS_VARIANT_TYPE_INT32:
        serializer->key[idx] = kvs_variant_serialize_comparable_int32;
        break;
      case KVS_VARIANT_TYPE_INT64:
        serializer->key[idx] = kvs_variant_serialize_comparable_int64;
        break;
      case KVS_VARIANT_TYPE_FLOAT:
        serializer->key[idx] = kvs_variant_serialize_comparable_float;
        break;
      case KVS_VARIANT_TYPE_DOUBLE:
        serializer->key[idx] = kvs_variant_serialize_comparable_double;
        break;
      case KVS_VARIANT_TYPE_OPAQUE:
        serializer->key[idx] = kvs_variant_serialize_comparable_opaque;
        break;
      default:
        break;
    }
  }
  for (idx = 0; idx < value_size; ++idx) {
    column = values[idx];
    switch (column->type) {
      case KVS_VARIANT_TYPE_INT32:
        serializer->value[idx] = kvs_variant_serialize_int32;
        break;
      case KVS_VARIANT_TYPE_INT64:
        serializer->value[idx] = kvs_variant_serialize_int64;
        break;
      case KVS_VARIANT_TYPE_FLOAT:
        serializer->value[idx] = kvs_variant_serialize_float;
        break;
      case KVS_VARIANT_TYPE_DOUBLE:
        serializer->value[idx] = kvs_variant_serialize_double;
        break;
      case KVS_VARIANT_TYPE_OPAQUE:
        serializer->value[idx] = kvs_variant_serialize_opaque;
        break;
      default:
        break;
    }
  }

  kvs_schema_prepared_deserializer_descriptor *deserializer = KVS_UNSAFE_CAST(serializer->value, sizeof(kvs_schema_prepared_field_serializer) * value_size);
  deserializer->key = KVS_UNSAFE_CAST(deserializer, sizeof(kvs_schema_prepared_deserializer_descriptor));
  deserializer->value = KVS_UNSAFE_CAST(deserializer->key, sizeof(kvs_schema_prepared_field_deserializer) * key_size);
  for (idx = 0; idx < key_size; ++idx) {
    column = keys[idx];
    switch (column->type) {
      case KVS_VARIANT_TYPE_INT32:
        deserializer->key[idx] = kvs_variant_deserialize_comparable_int32;
        break;
      case KVS_VARIANT_TYPE_INT64:
        deserializer->key[idx] = kvs_variant_deserialize_comparable_int64;
        break;
      case KVS_VARIANT_TYPE_FLOAT:
        deserializer->key[idx] = kvs_variant_deserialize_comparable_float;
        break;
      case KVS_VARIANT_TYPE_DOUBLE:
        deserializer->key[idx] = kvs_variant_deserialize_comparable_double;
        break;
      case KVS_VARIANT_TYPE_OPAQUE:
        deserializer->key[idx] = kvs_variant_deserialize_comparable_opaque;
        break;
      default:
        break;
    }
  }
  for (idx = 0; idx < value_size; ++idx) {
    column = values[idx];
    switch (column->type) {
      case KVS_VARIANT_TYPE_INT32:
        deserializer->value[idx] = kvs_variant_deserialize_int32;
        break;
      case KVS_VARIANT_TYPE_INT64:
        deserializer->value[idx] = kvs_variant_deserialize_int64;
        break;
      case KVS_VARIANT_TYPE_FLOAT:
        deserializer->value[idx] = kvs_variant_deserialize_float;
        break;
      case KVS_VARIANT_TYPE_DOUBLE:
        deserializer->value[idx] = kvs_variant_deserialize_double;
        break;
      case KVS_VARIANT_TYPE_OPAQUE:
        deserializer->value[idx] = kvs_variant_deserialize_opaque;
        break;
      default:
        break;
    }
  }

  codec->serializer = serializer;
  codec->deserializer = deserializer;
  return codec;
}

void kvs_schema_prepared_codec_destroy(const kvs_column **keys, size_t key_size, const kvs_column **values, size_t value_size, void *opaque) {
  free(opaque);
}

void kvs_schema_prepared_deserializer(const kvs_column **keys, size_t key_size, const kvs_column **values, size_t value_size, kvs_buffer *key, kvs_buffer *value, void *opaque, kvs_record *record) {
  size_t idx;
  kvs_variant **variant;
  kvs_schema_prepared_codec *codec = opaque;
  kvs_schema_prepared_deserializer_descriptor *descriptor = codec->deserializer;
  for (idx = 0; idx < key_size; ++idx) {
    variant = kvs_record_get(record, keys[idx]->index);
    *variant = descriptor->key[idx](*variant, key);
  }
  for (idx = 0; idx < value_size; ++idx) {
    variant = kvs_record_get(record, values[idx]->index);
    *variant = descriptor->value[idx](*variant, value);
  }
}
