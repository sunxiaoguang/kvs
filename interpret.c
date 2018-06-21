#include "schema.h"
#include "buffer.h"
#include "record.h"
#define __KVS_SCHEMA_INTERNAL_H__
#include "interpret.h"
#undef __KVS_SCHEMA_INTERNAL_H__
#include <string.h>

static uint8_t kvs_schema_interpreter_codec = 0;

static void kvs_schema_interpret_serialize_key(const kvs_column **columns, size_t size, kvs_record *record, kvs_buffer *buffer) {
  size_t idx;
  const kvs_column *column;
  const kvs_variant *variant;
  for (idx = 0; idx < size; ++idx) {
    column = columns[idx];
    variant = *kvs_record_get(record, column->index);
    switch (column->type) {
      case KVS_VARIANT_TYPE_INT32:
        kvs_variant_serialize_comparable_int32(variant, buffer);
        break;
      case KVS_VARIANT_TYPE_INT64:
        kvs_variant_serialize_comparable_int64(variant, buffer);
        break;
      case KVS_VARIANT_TYPE_FLOAT:
        kvs_variant_serialize_comparable_float(variant, buffer);
        break;
      case KVS_VARIANT_TYPE_DOUBLE:
        kvs_variant_serialize_comparable_double(variant, buffer);
        break;
      case KVS_VARIANT_TYPE_OPAQUE:
        kvs_variant_serialize_comparable_opaque(variant, buffer);
        break;
      default:
        break;
    }
  }
}

static void kvs_schema_interpret_serialize_value(const kvs_column **columns, size_t size, kvs_record *record, kvs_buffer *buffer) {
  size_t idx;
  const kvs_column *column;
  const kvs_variant *variant;
  for (idx = 0; idx < size; ++idx) {
    column = columns[idx];
    variant = *kvs_record_get(record, column->index);
    switch (column->type) {
      case KVS_VARIANT_TYPE_INT32:
        kvs_variant_serialize_int32(variant, buffer);
        break;
      case KVS_VARIANT_TYPE_INT64:
        kvs_variant_serialize_int64(variant, buffer);
        break;
      case KVS_VARIANT_TYPE_FLOAT:
        kvs_variant_serialize_float(variant, buffer);
        break;
      case KVS_VARIANT_TYPE_DOUBLE:
        kvs_variant_serialize_double(variant, buffer);
        break;
      case KVS_VARIANT_TYPE_OPAQUE:
        kvs_variant_serialize_opaque(variant, buffer);
        break;
      default:
        break;
    }
  }
}

void kvs_schema_interpret_serializer(const kvs_column **keys, size_t key_size, const kvs_column **values, size_t value_size, kvs_record *record, kvs_buffer *key, kvs_buffer *value, void *opaque) {
  kvs_schema_interpret_serialize_key(keys, key_size, record, key);
  kvs_schema_interpret_serialize_value(values, value_size, record, value);
}

static void kvs_schema_interpret_deserialize_key(const kvs_column **columns, size_t size, kvs_buffer *buffer, kvs_record *dest) {
  size_t idx;
  const kvs_column *column;
  kvs_variant **variant;
  for (idx = 0; idx < size; ++idx) {
    column = columns[idx];
    variant = kvs_record_get(dest, column->index);
    switch (column->type) {
      case KVS_VARIANT_TYPE_INT32:
        *variant = kvs_variant_deserialize_comparable_int32(*variant, buffer);
        break;
      case KVS_VARIANT_TYPE_INT64:
        *variant = kvs_variant_deserialize_comparable_int64(*variant, buffer);
        break;
      case KVS_VARIANT_TYPE_FLOAT:
        *variant = kvs_variant_deserialize_comparable_float(*variant, buffer);
        break;
      case KVS_VARIANT_TYPE_DOUBLE:
        *variant = kvs_variant_deserialize_comparable_double(*variant, buffer);
        break;
      case KVS_VARIANT_TYPE_OPAQUE:
        *variant = kvs_variant_deserialize_comparable_opaque(*variant, buffer);
        break;
      default:
        break;
    }
  }
}

static void kvs_schema_interpret_deserialize_value(const kvs_column **columns, size_t size, kvs_buffer *buffer, kvs_record *dest) {
  size_t idx;
  const kvs_column *column;
  kvs_variant **variant;
  for (idx = 0; idx < size; ++idx) {
    column = columns[idx];
    variant = kvs_record_get(dest, column->index);
    switch (column->type) {
      case KVS_VARIANT_TYPE_INT32:
        *variant = kvs_variant_deserialize_int32(*variant, buffer);
        break;
      case KVS_VARIANT_TYPE_INT64:
        *variant = kvs_variant_deserialize_int64(*variant, buffer);
        break;
      case KVS_VARIANT_TYPE_FLOAT:
        *variant = kvs_variant_deserialize_float(*variant, buffer);
        break;
      case KVS_VARIANT_TYPE_DOUBLE:
        *variant = kvs_variant_deserialize_double(*variant, buffer);
        break;
      case KVS_VARIANT_TYPE_OPAQUE:
        *variant = kvs_variant_deserialize_opaque(*variant, buffer);
        break;
      default:
        break;
    }
  }
}

void kvs_schema_interpret_deserializer(const kvs_column **keys, size_t key_size, const kvs_column **values, size_t value_size, kvs_buffer *key, kvs_buffer *value, void *opaque, kvs_record *record) {
  kvs_schema_interpret_deserialize_key(keys, key_size, key, record);
  kvs_schema_interpret_deserialize_value(values, value_size, value, record);
}

void *kvs_schema_interpret_codec_create(const kvs_column **keys, size_t key_size, const kvs_column **values, size_t value_size) {
  return &kvs_schema_interpreter_codec;
}

void kvs_schema_interpret_codec_destroy(const kvs_column **keys, size_t key_size, const kvs_column **values, size_t value_size, void *opaque) {
}
