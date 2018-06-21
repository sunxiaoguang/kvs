#include "kvs.h"
#include "cmdline.h"
#include <sys/time.h>
#include <stdio.h>
#include <string.h>

static void display_title(void) {
  size_t idx;
  for (idx = 0; idx < columns_num; ++idx) {
    if (idx == 0) {
      printf("|");
    }
    printf(" '%s' |", columns_name[idx]);
  }
  printf("\n");
}

static void display_int32(kvs_variant *variant) {
  int32_t i32;
  kvs_variant_get_int32(variant, &i32);
  printf("%d", i32);
}

static void display_int64(kvs_variant *variant) {
  int64_t i64;
  kvs_variant_get_int64(variant, &i64);
  printf("%lld", (long long) i64);
}

static void display_float(kvs_variant *variant) {
  float f;
  kvs_variant_get_float(variant, &f);
  printf("%f", (double) f);
}

static void display_double(kvs_variant *variant) {
  double d;
  kvs_variant_get_double(variant, &d);
  printf("%f", d);
}

static void display_opaque(kvs_variant *variant) {
  char stack_buffer[256];
  char *buffer;
  const void *data;
  size_t size;
  kvs_variant_get_opaque(variant, &data, &size);
  if (size < sizeof(stack_buffer) - 1) {
    buffer = stack_buffer;
  } else {
    buffer = malloc(size + 1);
  }
  memcpy(buffer, data, size);
  stack_buffer[size] = '\0';
  printf("%s", buffer);
  if (buffer != stack_buffer) {
    free(buffer);
  }
}

static void display_checksum(kvs_variant *variant) {
  const void *data;
  const uint8_t *cdata;
  size_t size, idx;
  kvs_variant_get_opaque(variant, &data, &size);
  cdata = data;
  printf(" ");
  for (idx = 0; idx < size; ++idx) {
    printf("%02x", cdata[idx]);
  }
}

static void display_variant(kvs_variant *variant) {
  switch (kvs_variant_get_type(variant)) {
    case KVS_VARIANT_TYPE_INT32:
      display_int32(variant);
      break;
    case KVS_VARIANT_TYPE_INT64:
      display_int64(variant);
      break;
    case KVS_VARIANT_TYPE_FLOAT:
      display_float(variant);
      break;
    case KVS_VARIANT_TYPE_DOUBLE:
      display_double(variant);
      break;
    case KVS_VARIANT_TYPE_OPAQUE:
      display_opaque(variant);
      break;
    default:
      break;
  }
}

static void display_record(kvs_schema_projection *projection, kvs_record *record) {
  size_t idx;
  for (idx = 0; idx < columns_num - 1; ++idx) {
    if (idx == 0) {
      printf("|");
    }
    printf(" '");
    display_variant(*kvs_schema_projection_record_get(projection, record, idx));
    printf("' |");
  }
  display_checksum(*kvs_schema_projection_record_get(projection, record, idx));
  printf(" |\n");
}

static void kvs_select(kvs_store *store, const char *prefix, int32_t limit) {
  const void *url_token_data;
  size_t url_token_size;
  size_t prefix_len = strlen(prefix);
  kvs_store_cursor *cursor;
  kvs_buffer *key, *value, *seek;
  kvs_schema *schema;
  kvs_record *record;
  kvs_schema_projection *projection;
  cursor = kvs_store_cursor_open(store);
  schema = kvs_schema_create(columns_meta, columns_num, KVS_SCHEMA_FLAG_JIT);
  projection = kvs_schema_projection_create(schema, columns_name, columns_num);
  record = kvs_schema_record_create(schema);
  key = kvs_buffer_create(4096);
  value = kvs_buffer_create(4096);
  seek = kvs_buffer_create(128);
  kvs_variant *prefix_variant = kvs_variant_create_from_opaque_no_copy(prefix, strlen(prefix));
  kvs_variant_serialize_comparable_opaque(prefix_variant, seek);
  kvs_variant_destroy(prefix_variant);
  display_title();
  if (!KVS_FAILED(kvs_store_cursor_seek(cursor, seek))) {
    while (!KVS_FAILED(kvs_store_cursor_next(cursor, key, value)) && limit-- > 0) {
      kvs_schema_record_deserialize(schema, key, value, record);
      kvs_variant_get_opaque(*kvs_schema_projection_record_get(projection, record, 0), &url_token_data, &url_token_size);
      if (url_token_size < prefix_len || memcmp(url_token_data, prefix, prefix_len) != 0) {
        break;
      }
      if (!kvs_record_verify_checksum(projection, record, 18)) {
        printf("Corrupted record: invalid record checksum\n");
        break;
      }
      display_record(projection, record);
    }
  }
  kvs_buffer_destroy(key);
  kvs_buffer_destroy(value);
  kvs_buffer_destroy(seek);
  kvs_record_destroy(record);
  kvs_schema_destroy(schema);
  kvs_store_cursor_close(cursor);
}

int main(int argc, char **argv) {
  kvs_store *store;
  if (argc != 4) {
    printf("Usage:\n%s <path> <url token prefix> <limit>\n", argv[0]);
    return 1;
  }
  store = kvs_store_open(argv[1], 0);
  if (store == NULL) {
    kvs_cmdline_fatal("Could not open kvs store");
  }
  kvs_select(store, argv[2], strtol(argv[3], NULL, 10));
  kvs_store_destroy(store);
  return 0;
}
