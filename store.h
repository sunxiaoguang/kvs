#ifndef __KVS_STORE_H__
#define __KVS_STORE_H__

#include <stdint.h>
#include <stdlib.h>
#include "buffer.h"
#include "status.h"

typedef struct kvs_store kvs_store;

typedef enum kvs_store_flag {
  KVS_STORE_FLAG_VOLATILE = 1 << 0,
} kvs_store_flag;

typedef struct kvs_store_txn kvs_store_txn;

typedef enum kvs_store_txn_flag {
  KVS_STORE_TXN_FLAG_READONLY = 1 << 0,
} kvs_store_txn_flag;

typedef struct kvs_store_cursor kvs_store_cursor;

kvs_store *kvs_store_open(const char *path, int32_t flags);
void kvs_store_destroy(kvs_store *store);
kvs_status kvs_store_put(kvs_store *store, kvs_buffer *key, kvs_buffer *value);

kvs_store_txn *kvs_store_txn_begin(kvs_store *store, int32_t flags);
kvs_status kvs_store_txn_commit(kvs_store_txn *txn);
void kvs_store_txn_abort(kvs_store_txn *txn);
kvs_status kvs_store_txn_put(kvs_store_txn *txn, kvs_buffer *key, kvs_buffer *value);

kvs_store_cursor *kvs_store_cursor_open(kvs_store *store);
kvs_status kvs_store_cursor_seek(kvs_store_cursor *cursor, kvs_buffer *key);
kvs_status kvs_store_cursor_next(kvs_store_cursor *cursor, kvs_buffer *key, kvs_buffer *value);
kvs_status kvs_store_cursor_next_no_copy(kvs_store_cursor *cursor, const void **key, size_t *key_size, const void **value, size_t *value_size);
void kvs_store_cursor_close(kvs_store_cursor *cursor);

#endif /* __KVS_STORE_H__ */
