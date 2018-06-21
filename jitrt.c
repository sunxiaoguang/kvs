#include "record.h"
#include "variant.h"
#include "buffer.h"

int64_t kvs_jit_rt_record_get(int64_t record, int64_t idx);
int64_t kvs_jit_rt_record_get(int64_t record, int64_t idx) {
  return (int64_t)(intptr_t) (kvs_record_get((kvs_record *)(intptr_t) record, (size_t) idx));
}

int64_t kvs_jit_rt_record_get_deref(int64_t record, int64_t idx);
int64_t kvs_jit_rt_record_get_deref(int64_t record, int64_t idx) {
  return (int64_t)(intptr_t) (*kvs_record_get((kvs_record *)(intptr_t) record, (size_t) idx));
}

void kvs_jit_rt_buffer_write(int64_t buffer, int64_t data, int64_t size);
void kvs_jit_rt_buffer_write(int64_t buffer, int64_t data, int64_t size) {
  kvs_buffer_write((kvs_buffer *)(intptr_t) buffer, (void *)(intptr_t) data, (size_t) size);
}

void kvs_jit_rt_buffer_read(int64_t buffer, int64_t data, int64_t size);
void kvs_jit_rt_buffer_read(int64_t buffer, int64_t data, int64_t size) {
  kvs_buffer_read((kvs_buffer *)(intptr_t) buffer, (void *)(intptr_t) data, (size_t) size);
}

void kvs_jit_rt_variant_serialize_comparable_int32(int64_t variant, int64_t buffer);
void kvs_jit_rt_variant_serialize_comparable_int32(int64_t variant, int64_t buffer) {
  kvs_variant_serialize_comparable_int32(*(kvs_variant **)(intptr_t) variant, (void *)(intptr_t) buffer);
}

void kvs_jit_rt_variant_serialize_comparable_int64(int64_t variant, int64_t buffer);
void kvs_jit_rt_variant_serialize_comparable_int64(int64_t variant, int64_t buffer) {
  kvs_variant_serialize_comparable_int64(*(kvs_variant **)(intptr_t) variant, (void *)(intptr_t) buffer);
}

void kvs_jit_rt_variant_serialize_comparable_float(int64_t variant, int64_t buffer);
void kvs_jit_rt_variant_serialize_comparable_float(int64_t variant, int64_t buffer) {
  kvs_variant_serialize_comparable_float(*(kvs_variant **)(intptr_t) variant, (void *)(intptr_t) buffer);
}

void kvs_jit_rt_variant_serialize_comparable_double(int64_t variant, int64_t buffer);
void kvs_jit_rt_variant_serialize_comparable_double(int64_t variant, int64_t buffer) {
  kvs_variant_serialize_comparable_double(*(kvs_variant **)(intptr_t) variant, (void *)(intptr_t) buffer);
}

void kvs_jit_rt_variant_serialize_comparable_opaque(int64_t variant, int64_t buffer);
void kvs_jit_rt_variant_serialize_comparable_opaque(int64_t variant, int64_t buffer) {
  kvs_variant_serialize_comparable_opaque(*(kvs_variant **)(intptr_t) variant, (void *)(intptr_t) buffer);
}

void kvs_jit_rt_variant_deserialize_comparable_int32(int64_t variant, int64_t buffer);
void kvs_jit_rt_variant_deserialize_comparable_int32(int64_t variant, int64_t buffer) {
  kvs_variant **real_variant = (kvs_variant **)(intptr_t) variant;
  *real_variant = kvs_variant_deserialize_comparable_int32(*real_variant, (void *)(intptr_t) buffer);
}

void kvs_jit_rt_variant_deserialize_comparable_int64(int64_t variant, int64_t buffer);
void kvs_jit_rt_variant_deserialize_comparable_int64(int64_t variant, int64_t buffer) {
  kvs_variant **real_variant = (kvs_variant **)(intptr_t) variant;
  *real_variant = kvs_variant_deserialize_comparable_int64(*real_variant, (void *)(intptr_t) buffer);
}

void kvs_jit_rt_variant_deserialize_comparable_float(int64_t variant, int64_t buffer);
void kvs_jit_rt_variant_deserialize_comparable_float(int64_t variant, int64_t buffer) {
  kvs_variant **real_variant = (kvs_variant **)(intptr_t) variant;
  *real_variant = kvs_variant_deserialize_comparable_float(*real_variant, (void *)(intptr_t) buffer);
}

void kvs_jit_rt_variant_deserialize_comparable_double(int64_t variant, int64_t buffer);
void kvs_jit_rt_variant_deserialize_comparable_double(int64_t variant, int64_t buffer) {
  kvs_variant **real_variant = (kvs_variant **)(intptr_t) variant;
  *real_variant = kvs_variant_deserialize_comparable_double(*real_variant, (void *)(intptr_t) buffer);
}

void kvs_jit_rt_variant_deserialize_comparable_opaque(int64_t variant, int64_t buffer);
void kvs_jit_rt_variant_deserialize_comparable_opaque(int64_t variant, int64_t buffer) {
  kvs_variant **real_variant = (kvs_variant **)(intptr_t) variant;
  *real_variant = kvs_variant_deserialize_comparable_opaque(*real_variant, (void *)(intptr_t) buffer);
}

void kvs_jit_rt_variant_deserialize_opaque(int64_t variant, int64_t buffer);
void kvs_jit_rt_variant_deserialize_opaque(int64_t variant, int64_t buffer) {
  kvs_variant **real_variant = (kvs_variant **)(intptr_t) variant;
  *real_variant = kvs_variant_deserialize_opaque(*real_variant, (kvs_buffer *) buffer);
}
