#include "kvs.h"
#include "cmdline.h"
#include <stdio.h>
#include <math.h>

static void kvs_record_populate(kvs_store_txn *txn, kvs_schema *schema, 
    kvs_schema_projection *projection, kvs_record *record, int32_t idx) {
  char buffer[4096];
  int32_t ver, size;
  kvs_buffer *key, *value;
  kvs_variant **url_token = kvs_schema_projection_record_get(projection, record, 0);
  kvs_variant **version = kvs_schema_projection_record_get(projection, record, 1);
  kvs_variant **content = kvs_schema_projection_record_get(projection, record, 2);
  kvs_variant **word_len = kvs_schema_projection_record_get(projection, record, 3);
  kvs_variant **member_id = kvs_schema_projection_record_get(projection, record, 4);
  kvs_variant **question_id = kvs_schema_projection_record_get(projection, record, 5);
  kvs_variant **is_copyable = kvs_schema_projection_record_get(projection, record, 6);
  kvs_variant **copyright_status = kvs_schema_projection_record_get(projection, record, 7);
  kvs_variant **is_delete = kvs_schema_projection_record_get(projection, record, 8);
  kvs_variant **is_muted = kvs_schema_projection_record_get(projection, record, 9);
  kvs_variant **is_collapsed = kvs_schema_projection_record_get(projection, record, 10);
  kvs_variant **status_info = kvs_schema_projection_record_get(projection, record, 11);
  kvs_variant **comment_permission = kvs_schema_projection_record_get(projection, record, 12);
  kvs_variant **delete_by = kvs_schema_projection_record_get(projection, record, 13);
  kvs_variant **created = kvs_schema_projection_record_get(projection, record, 14);
  kvs_variant **last_updated = kvs_schema_projection_record_get(projection, record, 15);
  kvs_variant **score = kvs_schema_projection_record_get(projection, record, 16);
  kvs_variant **credit = kvs_schema_projection_record_get(projection, record, 17);
  *url_token = kvs_variant_reset_opaque(*url_token, buffer, snprintf(buffer, sizeof(buffer), "url_token_%d", idx));
  key = kvs_buffer_create(1024);
  value = kvs_buffer_create(1024);
  for (ver = 0; ver < 10; ++ver) {
    kvs_variant_reset_int32(*version, ver);
    size = snprintf(buffer, sizeof(buffer), "content for (url_token_%d:%d)", idx, ver);
    *content = kvs_variant_reset_opaque(*content, buffer, size);
    kvs_variant_reset_int32(*word_len, size);
    kvs_variant_reset_int64(*member_id, idx + ver + 100);
    kvs_variant_reset_int64(*question_id, idx + ver + 200);
    kvs_variant_reset_int32(*is_copyable, idx + ver % 2);
    kvs_variant_reset_int32(*copyright_status, idx + ver % 2);
    kvs_variant_reset_int32(*is_delete, idx + ver % 2);
    kvs_variant_reset_int32(*is_muted, idx + ver % 2);
    kvs_variant_reset_int32(*is_collapsed, idx + ver % 2);
    *status_info = kvs_variant_reset_opaque(*status_info, buffer, snprintf(buffer, sizeof(buffer), "status info for (url_token_%d:%d)", idx, ver));
    kvs_variant_reset_int32(*comment_permission, idx + ver % 8);
    kvs_variant_reset_int64(*delete_by, idx + ver + 3000);
    kvs_variant_reset_int64(*created, idx + ver + 100000L),
    kvs_variant_reset_int64(*last_updated, idx + ver + 200000L);
    kvs_variant_reset_float(*score, logf(idx + ver + 1));
    kvs_variant_reset_double(*credit, log(idx + ver + 1) * 2.0);
    kvs_record_update_checksum(projection, record, 18);
    kvs_schema_record_serialize(schema, record, key, value);
    if (KVS_FAILED(kvs_store_txn_put(txn, key, value))) {
      kvs_cmdline_fatal("Failed to put data");
    }
  }
  kvs_buffer_destroy(key);
  kvs_buffer_destroy(value);
}

int main(int argc, char **argv) {
  int32_t number, idx;
  kvs_store *store;
  kvs_store_txn *txn;
  kvs_schema *schema;
  kvs_schema_projection *projection;
  kvs_record *record;
  if (argc != 3) {
    printf("Usage:\n%s <path> <number of records>\n", argv[0]);
    return 1;
  }
  number = strtol(argv[2], NULL, 10);
  store = kvs_store_open(argv[1], 0);
  if (store == NULL) {
    kvs_cmdline_fatal("Could not open kvs store");
  }
  schema = kvs_schema_create(columns_meta, columns_num, KVS_SCHEMA_FLAG_JIT);
  projection = kvs_schema_projection_create(schema, columns_name, columns_num);
  record = kvs_schema_record_create(schema);
  txn = kvs_store_txn_begin(store, 0);
  for (idx = 0; idx < number; ++idx) {
    kvs_record_populate(txn, schema, projection, record, idx);
  }
  kvs_store_txn_commit(txn);
  kvs_record_destroy(record);
  kvs_schema_projection_destroy(projection);
  kvs_schema_destroy(schema);
  kvs_store_destroy(store);
  return 0;
}
