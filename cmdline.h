#ifndef __KVS_CMDLINE_H__
#define __KVS_CMDLINE_H__

#include "kvs.h"
#include <stdbool.h>

extern kvs_column columns_meta[];
extern const char *columns_name[];
extern size_t columns_num;

void kvs_record_update_checksum(kvs_schema_projection *projection, kvs_record *record, size_t checksum_field);
bool kvs_record_verify_checksum(kvs_schema_projection *projection, kvs_record *record, size_t checksum_field);

void kvs_cmdline_fatal(const char *fmt, ...);

#endif /* __KVS_CMDLINE_H__ */
