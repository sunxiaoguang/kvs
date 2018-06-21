#include "cmdline.h"
#include <openssl/md5.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>

kvs_column columns_meta[] = {
  {"url_token", KVS_VARIANT_TYPE_OPAQUE, 1},
  {"version", KVS_VARIANT_TYPE_INT32, 1},
  {"content", KVS_VARIANT_TYPE_OPAQUE, 0},
  {"word_len", KVS_VARIANT_TYPE_INT32, 0},
  {"member_id", KVS_VARIANT_TYPE_INT64, 0},
  {"question_id", KVS_VARIANT_TYPE_INT64, 0},
  {"is_copyable", KVS_VARIANT_TYPE_INT32, 0},
  {"copyright_status", KVS_VARIANT_TYPE_INT32, 0},
  {"is_delete", KVS_VARIANT_TYPE_INT32, 0},
  {"is_muted", KVS_VARIANT_TYPE_INT32, 0},
  {"is_collapsed", KVS_VARIANT_TYPE_INT32, 0},
  {"status_info", KVS_VARIANT_TYPE_OPAQUE, 0},
  {"comment_permission", KVS_VARIANT_TYPE_INT32, 0},
  {"delete_by", KVS_VARIANT_TYPE_INT64, 0},
  {"created", KVS_VARIANT_TYPE_INT64, 0},
  {"last_updated", KVS_VARIANT_TYPE_INT64, 0},
  {"score", KVS_VARIANT_TYPE_FLOAT, 0},
  {"credit", KVS_VARIANT_TYPE_DOUBLE, 0},
  {"checksum", KVS_VARIANT_TYPE_OPAQUE, 0},
};

const char *columns_name[] = {
  "url_token",          /*  0 */
  "version",            /*  1 */
  "content",            /*  2 */
  "word_len",           /*  3 */
  "member_id",          /*  4 */
  "question_id",        /*  5 */
  "is_copyable",        /*  6 */
  "copyright_status",   /*  7 */
  "is_delete",          /*  8 */
  "is_muted",           /*  9 */
  "is_collapsed",       /* 10 */
  "status_info",        /* 11 */
  "comment_permission", /* 12 */
  "delete_by",          /* 13 */
  "created",            /* 14 */
  "last_updated",       /* 15 */
  "score",              /* 16 */
  "credit",             /* 17 */
  "checksum",           /* 18 */
};

size_t columns_num = sizeof(columns_meta) / sizeof(columns_meta[0]);

static void kvs_record_checksum(kvs_schema_projection *projection, kvs_record *record, size_t checksum_field, uint8_t *md5) {
  size_t idx = 0, size;
  int32_t i32;
  int64_t i64;
  float f;
  double d;
  const void *opaque;
  MD5_CTX ctx;
  MD5_Init(&ctx);
  kvs_variant *variant;
  memset(md5, 0, MD5_DIGEST_LENGTH);
  for (idx = 0; idx < checksum_field; ++ idx) {
    variant = *kvs_schema_projection_record_get(projection, record, idx);
    switch (kvs_variant_get_type(variant)) {
      case KVS_VARIANT_TYPE_INT32:
        kvs_variant_get_int32(variant, &i32);
        MD5_Update(&ctx, &i32, sizeof(i32));
        break;
      case KVS_VARIANT_TYPE_INT64:
        kvs_variant_get_int64(variant, &i64);
        MD5_Update(&ctx, &i64, sizeof(i64));
        break;
      case KVS_VARIANT_TYPE_FLOAT:
        kvs_variant_get_float(variant, &f);
        MD5_Update(&ctx, &f, sizeof(f));
        break;
      case KVS_VARIANT_TYPE_DOUBLE:
        kvs_variant_get_double(variant, &d);
        MD5_Update(&ctx, &d, sizeof(d));
        break;
      case KVS_VARIANT_TYPE_OPAQUE:
        kvs_variant_get_opaque(variant, &opaque, &size);
        MD5_Update(&ctx, opaque, size);
        break;
      default:
        break;
    }
  }
  MD5_Final(md5, &ctx);
}

void kvs_record_update_checksum(kvs_schema_projection *projection, kvs_record *record, size_t checksum_field) {
  uint8_t md5[MD5_DIGEST_LENGTH];
  kvs_variant **variant;
  kvs_record_checksum(projection, record, checksum_field, md5);
  variant = kvs_schema_projection_record_get(projection, record, checksum_field);
  *variant = kvs_variant_reset_opaque(*variant, md5, sizeof(md5));
}

bool kvs_record_verify_checksum(kvs_schema_projection *projection, kvs_record *record, size_t checksum_field) {
  uint8_t md5[MD5_DIGEST_LENGTH];
  size_t size;
  const void *opaque;
  kvs_variant *variant;
  kvs_record_checksum(projection, record, checksum_field, md5);
  variant = *kvs_schema_projection_record_get(projection, record, checksum_field);
  kvs_variant_get_opaque(variant, &opaque, &size);
  return size == MD5_DIGEST_LENGTH && memcmp(opaque, md5, MD5_DIGEST_LENGTH) == 0;
}

void kvs_cmdline_fatal(const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  vfprintf(stderr, fmt, args);
  va_end(args);
  abort();
}
