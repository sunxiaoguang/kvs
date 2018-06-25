#include "util.h"
#include "record.h"

struct kvs_record {
  kvs_variant **fields;
  size_t size;
};

kvs_record *kvs_record_create(kvs_variant **dfts, size_t size) {
  size_t idx;
  kvs_record *record = malloc(sizeof(kvs_record) + (sizeof(kvs_variant *) * size));
  record->fields = KVS_UNSAFE_CAST(record, sizeof(kvs_record));
  record->size = size;
  for (idx = 0; idx < size; ++idx) {
    if ((record->fields[idx] = kvs_variant_clone(dfts[idx])) == NULL) {
      kvs_record_destroy(record);
      record = NULL;
      break;
    }
  }
  return record;
}

void kvs_record_destroy(kvs_record *record) {
  size_t idx;
  for (idx = 0; idx < record->size; ++idx) {
    kvs_variant_destroy(record->fields[idx]);
  }
  free(record);
}

kvs_variant **kvs_record_get(kvs_record *record, size_t idx) {
  if (idx >= record->size) {
    return NULL;
  }
  return record->fields + idx;
}

size_t kvs_record_fields_offset(void) {
  return KVS_OFFSET_OF(kvs_record, fields);
}
