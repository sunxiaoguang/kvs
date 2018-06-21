#ifndef __KVS_RECORD_H__
#define __KVS_RECORD_H__

#include "variant.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct kvs_record kvs_record;

kvs_record *kvs_record_create(kvs_variant **dfts, size_t size);
void kvs_record_destroy(kvs_record *record);
kvs_variant **kvs_record_get(kvs_record *record, size_t idx);

#ifdef __cplusplus
}
#endif

#endif /* __KVS_RECORD_H__ */
