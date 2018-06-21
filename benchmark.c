#include "kvs.h"
#include "cmdline.h"
#include <sys/time.h>
#include <stdio.h>

static void elapsed(const char *suite, int64_t elapsed) {
  printf("It took %lld us to benchmark '%s'\n", (long long) elapsed, suite);
}

static int64_t benchmark(kvs_store *store, int32_t flags) {
  struct timeval start, end;
  kvs_store_cursor *cursor;
  kvs_buffer *key, *value;
  kvs_schema *schema;
  kvs_record *record;
  cursor = kvs_store_cursor_open(store);
  schema = kvs_schema_create(columns_meta, columns_num, flags);
  record = kvs_schema_record_create(schema);
  key = kvs_buffer_create(4096);
  value = kvs_buffer_create(4096);
  gettimeofday(&start, NULL);
  while (!KVS_FAILED(kvs_store_cursor_next(cursor, key, value))) {
    kvs_schema_record_deserialize(schema, key, value, record);
  }
  gettimeofday(&end, NULL);
  kvs_buffer_destroy(key);
  kvs_buffer_destroy(value);
  kvs_record_destroy(record);
  kvs_schema_destroy(schema);
  kvs_store_cursor_close(cursor);
  return (end.tv_sec * 1000000L + end.tv_usec) - (start.tv_sec * 1000000L + start.tv_usec);
}

int main(int argc, char **argv) {
  kvs_store *store;
  if (argc != 2) {
    printf("Usage:\n%s <path>\n", argv[0]);
    return 1;
  }
  store = kvs_store_open(argv[1], 0);
  if (store == NULL) {
    kvs_cmdline_fatal("Could not open kvs store");
  }
  /* warmup cache first */
  benchmark(store, KVS_SCHEMA_FLAG_PREPARED);
  elapsed("interpreted codec", benchmark(store, 0));
  elapsed("prepared codec", benchmark(store, KVS_SCHEMA_FLAG_PREPARED));
  elapsed("jit codec", benchmark(store, KVS_SCHEMA_FLAG_JIT));
  kvs_store_destroy(store);
  return 0;
}
