#include "util.h"
#include "variant.h"
#include "util.h"
#include <string.h>
#include <stdlib.h>
#include <math.h>

#define SIGN_MASK_U32 (0x80000000)
#define SIGN_MASK_U64 (0x8000000000000000L)
#define ESCAPE_LENGTH (9)
#define ENCODED_SIZE(SIZE) (((SIZE + (ESCAPE_LENGTH - 2)) / (ESCAPE_LENGTH - 1)) * ESCAPE_LENGTH)
static const uint8_t EMPTY_BYTES[] = {
  0, 0, 0, 0, 0, 0, 0, 0
};
typedef int32_t kvs_int32;
typedef int64_t kvs_int64;
typedef float kvs_float;
typedef double kvs_double;
typedef struct kvs_opaque {
  int32_t size;
  int32_t capacity;
  const void *data;
} kvs_opaque;

struct kvs_variant {
  kvs_variant_type type;
  union {
    kvs_int32 i32;
    kvs_float f;
    kvs_int64 i64;
    kvs_double d;
    kvs_opaque opaque;
  } value;
};

static inline void kvs_variant_serialize_comparable_uint32(uint32_t u32, kvs_buffer *buffer) {
  uint8_t *ubuffer = kvs_buffer_allocate(buffer, sizeof(int32_t));
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  ubuffer[3] = (u32 & 0xFF);
  ubuffer[2] = (u32 & 0xFF00) >> 8;
  ubuffer[1] = (u32 & 0xFF0000) >> 16;
  ubuffer[0] = (u32 & 0xFF000000) >> 24;
#else
  memcpy(ubuffer, &u32, sizeof(u32));
#endif
}

void kvs_variant_serialize_comparable_int32(const kvs_variant *variant, kvs_buffer *buffer) {
  kvs_variant_serialize_comparable_uint32(((uint32_t) variant->value.i32) ^ SIGN_MASK_U32, buffer);
}

static void kvs_variant_serialize_comparable_uint64(uint64_t u64, kvs_buffer *buffer) {
  uint8_t *ubuffer = kvs_buffer_allocate(buffer, sizeof(int64_t));
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  ubuffer[7] = (u64 & 0xFFL);
  ubuffer[6] = (u64 & 0xFF00L) >> 8;
  ubuffer[5] = (u64 & 0xFF0000L) >> 16;
  ubuffer[4] = (u64 & 0xFF000000L) >> 24;
  ubuffer[3] = (u64 & 0xFF00000000L) >> 32;
  ubuffer[2] = (u64 & 0xFF0000000000L) >> 40;
  ubuffer[1] = (u64 & 0xFF000000000000L) >> 48;
  ubuffer[0] = (u64 & 0xFF00000000000000L) >> 56;
#else
  memcpy(ubuffer, &u64, sizeof(u64));
#endif
}

void kvs_variant_serialize_comparable_int64(const kvs_variant *variant, kvs_buffer *buffer) {
  kvs_variant_serialize_comparable_uint64(((uint64_t) variant->value.i64) ^ SIGN_MASK_U64, buffer);
}

void kvs_variant_serialize_comparable_float(const kvs_variant *variant, kvs_buffer *buffer) {
  uint32_t u32;
  memcpy(&u32, &variant->value.f, sizeof(u32));
  if (variant->value.f >= 0) {
    u32 |= SIGN_MASK_U32;
  } else {
    u32 = ~u32;
  }
  kvs_variant_serialize_comparable_uint32(u32, buffer);
}

void kvs_variant_serialize_comparable_double(const kvs_variant *variant, kvs_buffer *buffer) {
  uint64_t u64;
  memcpy(&u64, &variant->value.d, sizeof(u64));;
  if (variant->value.d >= 0) {
    u64 |= SIGN_MASK_U64;
  } else {
    u64 = ~u64;
  }
  kvs_variant_serialize_comparable_uint64(u64, buffer);
}

void kvs_variant_serialize_comparable_opaque(const kvs_variant *variant, kvs_buffer *buffer) {
  size_t size = variant->value.opaque.size;
  size_t encoded_size = ENCODED_SIZE(size);
  if (encoded_size == 0) {
    encoded_size = ESCAPE_LENGTH;
  }
  const char *cdata = variant->value.opaque.data;
  char *cbuffer = kvs_buffer_allocate(buffer, encoded_size);
  while (1) {
    // Figure out how many bytes to copy, copy them and adjust pointers
    size_t copy_len = ESCAPE_LENGTH - 1 < size ? ESCAPE_LENGTH - 1 : size;
    memcpy(cbuffer, cdata, copy_len);
    cdata += copy_len;
    cbuffer += copy_len;
    size -= copy_len;
    // Are we at the end of the input?
    if (size == 0) {
      // pad with zeros if necessary;
      size_t padding_bytes = ESCAPE_LENGTH - 1 - copy_len;
      if (padding_bytes > 0) {
        memset(cbuffer, 0, padding_bytes);
        cbuffer += padding_bytes;
      }
      // Put the flag byte (0 - N-1) in the output
      *(cbuffer++) = copy_len;
      break;
    }
    // We have more data - put the flag byte (N) in and continue
    *(cbuffer++) = ESCAPE_LENGTH;
  }
}

static inline uint32_t kvs_variant_deserialize_comparable_uint32(kvs_buffer *data) {
  uint32_t u32;
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  uint8_t ubuffer[sizeof(uint32_t)];
  kvs_buffer_read(data, ubuffer, sizeof(ubuffer));
  u32 = ((uint32_t) ubuffer[3]) | (((uint32_t) ubuffer[2]) << 8) | 
    (((uint32_t) ubuffer[1]) << 16) | (((uint32_t) ubuffer[0]) << 24);
#else
  kvs_buffer_read(buffer, &u32, sizeof(u32));
#endif
  return u32;
}

kvs_variant *kvs_variant_deserialize_comparable_int32(kvs_variant *dest, kvs_buffer *data) {
  uint32_t u32 = kvs_variant_deserialize_comparable_uint32(data) ^ SIGN_MASK_U32;
  if (dest == NULL) {
    dest = kvs_variant_create_from_int32(0);
  }
  kvs_variant_reset_int32(dest, (int32_t) u32);
  return dest;
}

static inline uint64_t kvs_variant_deserialize_comparable_uint64(kvs_buffer *data) {
  uint64_t u64;
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  uint8_t ubuffer[sizeof(uint64_t)];
  kvs_buffer_read(data, ubuffer, sizeof(ubuffer));
  u64 = ((uint64_t) ubuffer[7]) | (((uint64_t) ubuffer[6]) << 8) | 
    (((uint64_t) ubuffer[5]) << 16) | (((uint64_t) ubuffer[4]) << 24) | 
    (((uint64_t) ubuffer[3]) << 32) | (((uint64_t) ubuffer[2]) << 40) | 
    (((uint64_t) ubuffer[1]) << 48) | (((uint64_t) ubuffer[0]) << 56);
#else
  kvs_buffer_read(buffer, &u64, sizeof(u64));
#endif
  return u64;
}

kvs_variant *kvs_variant_deserialize_comparable_int64(kvs_variant *dest, kvs_buffer *data) {
  uint64_t u64 = kvs_variant_deserialize_comparable_uint64(data) ^ SIGN_MASK_U64;
  if (dest == NULL) {
    dest = kvs_variant_create_from_int64(0);
  }
  kvs_variant_reset_int64(dest, (int64_t) u64);
  return dest;
}

kvs_variant *kvs_variant_deserialize_comparable_float(kvs_variant *dest, kvs_buffer *data) {
  uint32_t u32 = kvs_variant_deserialize_comparable_uint32(data);
  float f;
  if ((u32 & SIGN_MASK_U32) > 0) {
    u32 &= ~SIGN_MASK_U32;
  } else {
    u32 = ~u32;
  }
  if (dest == NULL) {
    dest = kvs_variant_create_from_float(0.0f);
  }
  memcpy(&f, &u32, sizeof(f));
  kvs_variant_reset_float(dest, f);
  return dest;
}

kvs_variant *kvs_variant_deserialize_comparable_double(kvs_variant *dest, kvs_buffer *data) {
  uint64_t u64 = kvs_variant_deserialize_comparable_uint64(data);
  double d;
  if ((u64 & SIGN_MASK_U64) > 0) {
    u64 &= ~SIGN_MASK_U64;
  } else {
    u64 = ~u64;
  }
  if (dest == NULL) {
    dest = kvs_variant_create_from_double(0.0);
  }
  memcpy(&d, &u64, sizeof(d));
  kvs_variant_reset_double(dest, d);
  return dest;
}

kvs_variant *kvs_variant_deserialize_comparable_opaque(kvs_variant *dest, kvs_buffer *data) {
  kvs_buffer *result = kvs_buffer_create(512);
  uint8_t group[ESCAPE_LENGTH], group_size;
  const uint8_t *flat;
  uint8_t *to_free = NULL;
  size_t size;
  while (1) {
    if (kvs_buffer_size(data) < ESCAPE_LENGTH) {
      abort();
    }
    kvs_buffer_read(data, &group, sizeof(group));
    if ((group_size = group[ESCAPE_LENGTH - 1]) > ESCAPE_LENGTH) {
      abort();
    }
    if (group_size < ESCAPE_LENGTH) {
      /* Check validity of padding bytes */
      if (memcmp(group + group_size, EMPTY_BYTES, ESCAPE_LENGTH - 1 - group_size) != 0) {
        abort();
      }
      memcpy(kvs_buffer_allocate(result, group_size), group, group_size);
      break;
    } else {
      memcpy(kvs_buffer_allocate(result, ESCAPE_LENGTH - 1), group, ESCAPE_LENGTH - 1);
    }
  }
  if ((size = kvs_buffer_size(result)) > 0) {
    if ((flat = kvs_buffer_peek(result, size)) == NULL) {
      flat = to_free = malloc(size);
      kvs_buffer_read(result, to_free, size);
    }
  } else {
    flat = NULL;
  }
  dest = dest == NULL ? kvs_variant_create_from_opaque(flat, size) : kvs_variant_reset_opaque(dest, flat, size);
  kvs_buffer_destroy(result);
  if (to_free != NULL) {
    free(to_free);
  }
  return dest;
}

kvs_variant *kvs_variant_deserialize_opaque(kvs_variant *dest, kvs_buffer *data) {
  int32_t nbytes;
  char *buffer;
  if (kvs_buffer_size(data) < sizeof(int32_t)) {
    return NULL;
  }
  kvs_buffer_read(data, &nbytes, sizeof(nbytes));
  if (dest->type != KVS_VARIANT_TYPE_OPAQUE || dest->value.opaque.capacity < nbytes) {
    /* reallocate a new variant */
    dest = realloc(dest, sizeof(kvs_variant) + nbytes);
    dest->value.opaque.capacity = nbytes;
  }
  buffer = KVS_UNSAFE_CAST(dest, sizeof(*dest));
  dest->value.opaque.data = buffer;
  kvs_buffer_read(data, buffer, nbytes);
  dest->value.opaque.size = nbytes;
  dest->type = KVS_VARIANT_TYPE_OPAQUE;
  return dest;
}

kvs_variant_type kvs_variant_get_type(const kvs_variant *variant) {
  return variant->type;
}

size_t kvs_variant_type_size(kvs_variant_type ty) {
  switch (ty) {
    case KVS_VARIANT_TYPE_INT32:
      return sizeof(int32_t);
    case KVS_VARIANT_TYPE_INT64:
      return sizeof(int64_t);
    case KVS_VARIANT_TYPE_FLOAT:
      return sizeof(float);
    case KVS_VARIANT_TYPE_DOUBLE:
      return sizeof(double);
    default:
      return 0;
  }
}

static kvs_variant *kvs_variant_create_internal(kvs_variant_type type, size_t extra_size) {
  kvs_variant *variant = calloc(1, sizeof(kvs_variant) + extra_size);
  variant->type = type;
  return variant;
}

kvs_variant *kvs_variant_create(kvs_variant_type type) {
  kvs_variant *variant = calloc(1, sizeof(kvs_variant));
  variant->type = type;
  return variant;
}

kvs_variant *kvs_variant_create_int32(void) {
  return kvs_variant_create_from_int32(0);
}

kvs_variant *kvs_variant_create_int64(void) {
  return kvs_variant_create_from_int64(0L);
}

kvs_variant *kvs_variant_create_float(void) {
  return kvs_variant_create_from_float(0.0f);
}

kvs_variant *kvs_variant_create_double(void) {
  return kvs_variant_create_from_double(0.0);
}

kvs_variant *kvs_variant_create_opaque(void) {
  return kvs_variant_create_from_opaque_no_copy(NULL, 0);
}

kvs_variant *kvs_variant_create_from_int32(int32_t from) {
  kvs_variant *variant = kvs_variant_create_internal(KVS_VARIANT_TYPE_INT32, 0);
  variant->value.i32 = from;
  return variant;
}

kvs_variant *kvs_variant_create_from_int64(int64_t from) {
  kvs_variant *variant = kvs_variant_create_internal(KVS_VARIANT_TYPE_INT64, 0);
  variant->value.i64 = from;
  return variant;
}

kvs_variant *kvs_variant_create_from_float(float from) {
  kvs_variant *variant = kvs_variant_create_internal(KVS_VARIANT_TYPE_FLOAT, 0);
  variant->value.f = from;
  return variant;
}

kvs_variant *kvs_variant_create_from_double(double from) {
  kvs_variant *variant = kvs_variant_create_internal(KVS_VARIANT_TYPE_DOUBLE, 0);
  variant->value.d = from;
  return variant;
}

kvs_variant *kvs_variant_create_from_opaque(const void *from, size_t size) {
  kvs_variant *variant = kvs_variant_create_internal(KVS_VARIANT_TYPE_OPAQUE, size);
  void *buffer = KVS_UNSAFE_CAST(variant, sizeof(*variant));
  memcpy(buffer, from, size);
  variant->value.opaque.data = buffer;
  variant->value.opaque.size = size;
  return variant;
}

kvs_variant *kvs_variant_create_from_opaque_no_copy(const void *from, size_t size) {
  kvs_variant *variant = kvs_variant_create_internal(KVS_VARIANT_TYPE_OPAQUE, 0);
  variant->value.opaque.data = from;
  variant->value.opaque.size = size;
  return variant;
}

kvs_variant *kvs_variant_reset_int32(kvs_variant *variant, int32_t to) {
  variant->type = KVS_VARIANT_TYPE_INT32;
  variant->value.i32 = to;
  return variant;
}

kvs_variant *kvs_variant_reset_int64(kvs_variant *variant, int64_t to) {
  variant->type = KVS_VARIANT_TYPE_INT64;
  variant->value.i64 = to;
  return variant;
}

kvs_variant *kvs_variant_reset_float(kvs_variant *variant, float to) {
  variant->type = KVS_VARIANT_TYPE_FLOAT;
  variant->value.f = to;
  return variant;
}

kvs_variant *kvs_variant_reset_double(kvs_variant *variant, double to) {
  variant->type = KVS_VARIANT_TYPE_DOUBLE;
  variant->value.d = to;
  return variant;
}

kvs_variant *kvs_variant_reset_opaque(kvs_variant *variant, const void *to, size_t size) {
  if (variant->type != KVS_VARIANT_TYPE_OPAQUE || variant->value.opaque.capacity < (int32_t) size) {
    /* reallocate a new variant */
    variant = realloc(variant, sizeof(kvs_variant) + size);
    variant->value.opaque.data = KVS_UNSAFE_CAST(variant, sizeof(kvs_variant));
    variant->value.opaque.capacity = size;
  }
  variant->type = KVS_VARIANT_TYPE_OPAQUE;
  memcpy((void *) variant->value.opaque.data, to, size);
  variant->value.opaque.size = size;
  return variant;
}

kvs_variant *kvs_variant_reset_opaque_no_copy(kvs_variant *variant, const void *to, size_t size) {
  variant->type = KVS_VARIANT_TYPE_OPAQUE;
  variant->value.opaque.data = to;
  variant->value.opaque.size = size;
  variant->value.opaque.capacity = 0;
  return variant;
}

kvs_variant *kvs_variant_clone(const kvs_variant *variant) {
  switch (variant->type) {
    case KVS_VARIANT_TYPE_INT32:
      return kvs_variant_create_from_int32(variant->value.i32);
    case KVS_VARIANT_TYPE_INT64:
      return kvs_variant_create_from_int64(variant->value.i64);
    case KVS_VARIANT_TYPE_FLOAT:
      return kvs_variant_create_from_float(variant->value.f);
    case KVS_VARIANT_TYPE_DOUBLE:
      return kvs_variant_create_from_double(variant->value.d);
    case KVS_VARIANT_TYPE_OPAQUE:
      return kvs_variant_create_from_opaque(variant->value.opaque.data, variant->value.opaque.size); 
    default:
      return calloc(1, sizeof(kvs_variant));
  }
}

void kvs_variant_destroy(kvs_variant *variant) {
  free(variant);
}

kvs_status kvs_variant_get_int32(const kvs_variant *variant, int32_t *dest) {
  if (variant->type != KVS_VARIANT_TYPE_INT32) {
    return KVS_INVALID_VARIANT_TYPE;
  }
  *dest = variant->value.i32;
  return KVS_OK;
}

kvs_status kvs_variant_get_int64(const kvs_variant *variant, int64_t *dest) {
  if (variant->type != KVS_VARIANT_TYPE_INT64) {
    return KVS_INVALID_VARIANT_TYPE;
  }
  *dest = variant->value.i64;
  return KVS_OK;
}

kvs_status kvs_variant_get_float(const kvs_variant *variant, float *dest) {
  if (variant->type != KVS_VARIANT_TYPE_FLOAT) {
    return KVS_INVALID_VARIANT_TYPE;
  }
  *dest = variant->value.f;
  return KVS_OK;
}

kvs_status kvs_variant_get_double(const kvs_variant *variant, double *dest) {
  if (variant->type != KVS_VARIANT_TYPE_DOUBLE) {
    return KVS_INVALID_VARIANT_TYPE;
  }
  *dest = variant->value.d;
  return KVS_OK;
}

kvs_status kvs_variant_get_opaque(const kvs_variant *variant, const void **data, size_t *size) {
  if (variant->type != KVS_VARIANT_TYPE_OPAQUE) {
    return KVS_INVALID_VARIANT_TYPE;
  }
  *data = variant->value.opaque.data;
  *size = variant->value.opaque.size;
  return KVS_OK;
}

kvs_status kvs_variant_copy_opaque(const kvs_variant *variant, void *buffer, size_t *size) {
  if (variant->type != KVS_VARIANT_TYPE_OPAQUE) {
    return KVS_INVALID_VARIANT_TYPE;
  }
  if (variant->value.opaque.size > (int32_t) *size) {
    *size = variant->value.opaque.size;
    return KVS_INSUFFICIENT_BUFFER;
  }
  memcpy(buffer, variant->value.opaque.data, variant->value.opaque.size);
  *size = variant->value.opaque.size;
  return KVS_OK;
}

size_t kvs_variant_type_offset(void) {
  return KVS_OFFSET_OF(kvs_variant, type);
}

size_t kvs_variant_int32_offset(void) {
  return KVS_OFFSET_OF(kvs_variant, value.i32);
}

size_t kvs_variant_int64_offset(void) {
  return KVS_OFFSET_OF(kvs_variant, value.i64);
}

size_t kvs_variant_float_offset(void) {
  return KVS_OFFSET_OF(kvs_variant, value.f);
}

size_t kvs_variant_double_offset(void) {
  return KVS_OFFSET_OF(kvs_variant, value.d);
}

size_t kvs_variant_opaque_data_offset(void) {
  return KVS_OFFSET_OF(kvs_variant, value.opaque.data);
}

size_t kvs_variant_opaque_size_offset(void) {
  return KVS_OFFSET_OF(kvs_variant, value.opaque.size);
}

size_t kvs_variant_serialized_size(const kvs_variant *variant) {
  switch (variant->type) {
    case KVS_VARIANT_TYPE_INT32:
      return sizeof(int32_t);
    case KVS_VARIANT_TYPE_INT64:
      return sizeof(int64_t);
    case KVS_VARIANT_TYPE_FLOAT:
      return sizeof(float);
    case KVS_VARIANT_TYPE_DOUBLE:
      return sizeof(double);
    case KVS_VARIANT_TYPE_OPAQUE:
      return variant->value.opaque.size + sizeof(int32_t);
    default:
      return 0;
  }
}

#define kvs_variant_compare_primitive(LHS, RHS) \
  do {                                          \
    if (LHS < RHS) {                            \
      return -1;                                \
    } else if (LHS > RHS) {                     \
      return 1;                                 \
    } else {                                    \
      return 0;                                 \
    }                                           \
  } while (0)

#define kvs_variant_compare_float(LHS, RHS)     \
  do {                                          \
      if (isnan(LHS) || isnan(RHS)) {           \
        if (isnan(LHS) && isnan(RHS)) {         \
          return 0;                             \
        } else if (isnan(LHS)) {                \
          return -1;                            \
        }                                       \
        return 1;                               \
      }                                         \
  } while (0);                                  \
  kvs_variant_compare_primitive(LHS, RHS)

static inline int32_t kvs_variant_compare_opaque(const kvs_opaque *lhs, const kvs_opaque *rhs) {
  size_t min_size;
  int32_t rc;
  if (lhs->data == NULL || rhs->data == NULL) {
    return (rhs->data ? 0 : 1) - (lhs->data ? 0 : 1);
  }
  min_size = lhs->size > rhs->size ? rhs->size : lhs->size;
  if ((rc = memcmp(lhs->data, rhs->data, min_size)) != 0) {
    return rc;
  }
  return (lhs->size > rhs->size ? 1 : 0) - (lhs->size < rhs->size ? 1 : 0);
}

int32_t kvs_variant_compare(const kvs_variant *lhs, const kvs_variant *rhs) {
  if (lhs->type != rhs->type) {
    if (lhs->type < rhs->type) {
      return -1;
    } else {
      return 1;
    }
  }
  switch (lhs->type) {
    case KVS_VARIANT_TYPE_INT32:
      kvs_variant_compare_primitive(lhs->value.i32, rhs->value.i32);
    case KVS_VARIANT_TYPE_INT64:
      kvs_variant_compare_primitive(lhs->value.i64, rhs->value.i64);
    case KVS_VARIANT_TYPE_FLOAT:
      kvs_variant_compare_float(lhs->value.f, rhs->value.f);
    case KVS_VARIANT_TYPE_DOUBLE:
      kvs_variant_compare_float(lhs->value.d, rhs->value.d);
    case KVS_VARIANT_TYPE_OPAQUE:
      return kvs_variant_compare_opaque(&lhs->value.opaque, &rhs->value.opaque);
    default:
      return 0;
  }
}

static void kvs_variant_serialize_primitive(const kvs_variant *variant, size_t offset, size_t expect, kvs_buffer *buffer) {
  kvs_buffer_write(buffer, KVS_UNSAFE_CAST(variant, offset), expect);
}

void kvs_variant_serialize_int32(const kvs_variant *variant, kvs_buffer *buffer) {
  kvs_variant_serialize_primitive(variant, kvs_variant_int32_offset(), sizeof(kvs_int32), buffer);
}

void kvs_variant_serialize_int64(const kvs_variant *variant, kvs_buffer *buffer) {
  kvs_variant_serialize_primitive(variant, kvs_variant_int64_offset(), sizeof(kvs_int64), buffer);
}

void kvs_variant_serialize_float(const kvs_variant *variant, kvs_buffer *buffer) {
  kvs_variant_serialize_primitive(variant, kvs_variant_float_offset(), sizeof(kvs_float), buffer);
}

void kvs_variant_serialize_double(const kvs_variant *variant, kvs_buffer *buffer) {
  kvs_variant_serialize_primitive(variant, kvs_variant_double_offset(), sizeof(kvs_double), buffer);
}

void kvs_variant_serialize_opaque(const kvs_variant *variant, kvs_buffer *buffer) {
  int32_t size = variant->value.opaque.size;
  memcpy(kvs_buffer_allocate(buffer, sizeof(size)), &size, sizeof(size));
  memcpy(kvs_buffer_allocate(buffer, variant->value.opaque.size), variant->value.opaque.data, variant->value.opaque.size);
}

void kvs_variant_serialize(const kvs_variant *variant, kvs_buffer *buffer) {
  switch (variant->type) {
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

kvs_variant *kvs_variant_deserialize(kvs_variant *dest, kvs_variant_type type, kvs_buffer *data) {
  switch (type) {
    case KVS_VARIANT_TYPE_OPAQUE:
      return kvs_variant_deserialize_opaque(dest, data);
    case KVS_VARIANT_TYPE_INT32:
      return kvs_variant_deserialize_int32(dest, data);
    case KVS_VARIANT_TYPE_INT64:
      return kvs_variant_deserialize_int64(dest, data);
    case KVS_VARIANT_TYPE_FLOAT:
      return kvs_variant_deserialize_float(dest, data);
    case KVS_VARIANT_TYPE_DOUBLE:
      return kvs_variant_deserialize_double(dest, data);
    default:
      return NULL;
  }
}

static inline kvs_variant *kvs_variant_deserialize_primitive(kvs_variant *variant, size_t offset, size_t expect, kvs_buffer *data) {
  if (expect > kvs_buffer_size(data)) {
    return NULL;
  }
  kvs_buffer_read(data, KVS_UNSAFE_CAST(variant, offset), expect);
  return variant;
}

kvs_variant *kvs_variant_deserialize_int32(kvs_variant *dest, kvs_buffer *data) {
  return kvs_variant_deserialize_primitive(dest, kvs_variant_int32_offset(), sizeof(kvs_int32), data);
}

kvs_variant *kvs_variant_deserialize_int64(kvs_variant *dest, kvs_buffer *data) {
  return kvs_variant_deserialize_primitive(dest, kvs_variant_int64_offset(), sizeof(kvs_int64), data);
}

kvs_variant *kvs_variant_deserialize_float(kvs_variant *dest, kvs_buffer *data) {
  return kvs_variant_deserialize_primitive(dest, kvs_variant_float_offset(), sizeof(kvs_float), data);
}

kvs_variant *kvs_variant_deserialize_double(kvs_variant *dest, kvs_buffer *data) {
  return kvs_variant_deserialize_primitive(dest, kvs_variant_double_offset(), sizeof(kvs_double), data);
}

void kvs_variant_serialize_comparable(const kvs_variant *variant, kvs_buffer *buffer) {
  switch (variant->type) {
    case KVS_VARIANT_TYPE_OPAQUE:
      kvs_variant_serialize_comparable_opaque(variant, buffer);
      break;
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
    default:
      break;
  }
}

kvs_variant *kvs_variant_deserialize_comparable(kvs_variant *dest, kvs_variant_type type, kvs_buffer *data) {
  switch (dest->type) {
    case KVS_VARIANT_TYPE_OPAQUE:
      return kvs_variant_deserialize_comparable_opaque(dest, data);
    case KVS_VARIANT_TYPE_INT32:
      return kvs_variant_deserialize_comparable_int32(dest, data);
    case KVS_VARIANT_TYPE_INT64:
      return kvs_variant_deserialize_comparable_int64(dest, data);
    case KVS_VARIANT_TYPE_FLOAT:
      return kvs_variant_deserialize_comparable_float(dest, data);
    case KVS_VARIANT_TYPE_DOUBLE:
      return kvs_variant_deserialize_comparable_double(dest, data);
    default:
      return dest;
  }
}
