#ifndef __KVS_VARIANT_H__
#define __KVS_VARIANT_H__

#include <stdint.h>
#include <stddef.h>
#include "buffer.h"
#include "status.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum kvs_variant_type {
  KVS_VARIANT_TYPE_INVALID = 0,
  KVS_VARIANT_TYPE_INT32,
  KVS_VARIANT_TYPE_INT64,
  KVS_VARIANT_TYPE_FLOAT,
  KVS_VARIANT_TYPE_DOUBLE,
  KVS_VARIANT_TYPE_OPAQUE
} kvs_variant_type;

typedef struct kvs_variant kvs_variant;

size_t kvs_variant_type_size(kvs_variant_type ty);
kvs_variant *kvs_variant_create(kvs_variant_type ty);
kvs_variant *kvs_variant_create_int32(void);
kvs_variant *kvs_variant_create_int64(void);
kvs_variant *kvs_variant_create_float(void);
kvs_variant *kvs_variant_create_double(void);
kvs_variant *kvs_variant_create_opaque(void);
kvs_variant *kvs_variant_create_from_int32(int32_t from);
kvs_variant *kvs_variant_create_from_int64(int64_t from);
kvs_variant *kvs_variant_create_from_float(float from);
kvs_variant *kvs_variant_create_from_double(double from);
kvs_variant *kvs_variant_create_from_opaque(const void *from, size_t size);
kvs_variant *kvs_variant_create_from_opaque_no_copy(const void *from, size_t size);
kvs_variant *kvs_variant_clone(const kvs_variant *from);
kvs_variant *kvs_variant_reset_int32(kvs_variant *variant, int32_t to);
kvs_variant *kvs_variant_reset_int64(kvs_variant *variant, int64_t to);
kvs_variant *kvs_variant_reset_float(kvs_variant *variant, float to);
kvs_variant *kvs_variant_reset_double(kvs_variant *variant, double to);
kvs_variant *kvs_variant_reset_opaque(kvs_variant *variant, const void *to, size_t size);
kvs_variant *kvs_variant_reset_opaque_no_copy(kvs_variant *variant, const void *to, size_t size);
kvs_variant_type kvs_variant_get_type(const kvs_variant *variant);
kvs_status kvs_variant_get_int32(const kvs_variant *variant, int32_t *dest);
kvs_status kvs_variant_get_int64(const kvs_variant *variant, int64_t *dest);
kvs_status kvs_variant_get_float(const kvs_variant *variant, float *dest);
kvs_status kvs_variant_get_double(const kvs_variant *variant, double *dest);
kvs_status kvs_variant_get_opaque(const kvs_variant *variant, const void **data, size_t *size);
kvs_status kvs_variant_copy_opaque(const kvs_variant *variant, void *buffer, size_t *size);
size_t kvs_variant_serialized_size(const kvs_variant *variant);
void kvs_variant_destroy(kvs_variant *variant);
int32_t kvs_variant_compare(const kvs_variant *lhs, const kvs_variant *rhs);

size_t kvs_variant_type_offset(void);
size_t kvs_variant_int32_offset(void);
size_t kvs_variant_int64_offset(void);
size_t kvs_variant_float_offset(void);
size_t kvs_variant_double_offset(void);
size_t kvs_variant_opaque_data_offset(void);
size_t kvs_variant_opaque_size_offset(void);

void kvs_variant_serialize(const kvs_variant *variant, kvs_buffer *buffer);
void kvs_variant_serialize_int32(const kvs_variant *variant, kvs_buffer *buffer);
void kvs_variant_serialize_int64(const kvs_variant *variant, kvs_buffer *buffer);
void kvs_variant_serialize_float(const kvs_variant *variant, kvs_buffer *buffer);
void kvs_variant_serialize_double(const kvs_variant *variant, kvs_buffer *buffer);
void kvs_variant_serialize_opaque(const kvs_variant *variant, kvs_buffer *buffer);

kvs_variant *kvs_variant_deserialize(kvs_variant *dest, kvs_variant_type type, kvs_buffer *buffer);
kvs_variant *kvs_variant_deserialize_int32(kvs_variant *dest, kvs_buffer *data);
kvs_variant *kvs_variant_deserialize_int64(kvs_variant *dest, kvs_buffer *data);
kvs_variant *kvs_variant_deserialize_float(kvs_variant *dest, kvs_buffer *data);
kvs_variant *kvs_variant_deserialize_double(kvs_variant *dest, kvs_buffer *data);
kvs_variant *kvs_variant_deserialize_opaque(kvs_variant *dest, kvs_buffer *data);

void kvs_variant_serialize_comparable(const kvs_variant *variant, kvs_buffer *buffer);
void kvs_variant_serialize_comparable_int32(const kvs_variant *variant, kvs_buffer *buffer);
void kvs_variant_serialize_comparable_int64(const kvs_variant *variant, kvs_buffer *buffer);
void kvs_variant_serialize_comparable_float(const kvs_variant *variant, kvs_buffer *buffer);
void kvs_variant_serialize_comparable_double(const kvs_variant *variant, kvs_buffer *buffer);
void kvs_variant_serialize_comparable_opaque(const kvs_variant *variant, kvs_buffer *buffer);

kvs_variant *kvs_variant_deserialize_comparable(kvs_variant *dest, kvs_variant_type type, kvs_buffer *buffer);
kvs_variant *kvs_variant_deserialize_comparable_int32(kvs_variant *dest, kvs_buffer *data);
kvs_variant *kvs_variant_deserialize_comparable_int64(kvs_variant *dest, kvs_buffer *data);
kvs_variant *kvs_variant_deserialize_comparable_float(kvs_variant *dest, kvs_buffer *data);
kvs_variant *kvs_variant_deserialize_comparable_double(kvs_variant *dest, kvs_buffer *data);
kvs_variant *kvs_variant_deserialize_comparable_opaque(kvs_variant *dest, kvs_buffer *data);

#ifdef __cplusplus
}
#endif

#endif /* __KVS_VARIANT_H__ */
