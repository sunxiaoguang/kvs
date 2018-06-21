#include "store.h"
#include "buffer.h"
#include "status.h"
#include <stdlib.h>
#include <sys/stat.h>
#include <lmdb.h>
#include <string.h>

struct kvs_store {
  MDB_env *env;
  MDB_dbi dbi;
};

struct kvs_store_txn {
  MDB_dbi dbi;
  MDB_txn *txn;
};

struct kvs_store_cursor {
  MDB_txn *txn;
  MDB_cursor *cursor;
};

static inline int32_t kvs_store_convert_lmdb_status(int st) {
  switch (st) {
    case MDB_SUCCESS:
      return KVS_OK;
    case MDB_NOTFOUND:
      return KVS_STORE_EOF;
    case MDB_PAGE_NOTFOUND:
    case MDB_CORRUPTED:
    case MDB_INVALID:
      return KVS_STORE_CORRUPTED;
    default:
      return KVS_STORE_INTERNAL_ERROR;
  }
}

static uint32_t kvs_store_flags_to_mdb_env_flags(int32_t flags) {
  if ((flags & KVS_STORE_FLAG_VOLATILE) == KVS_STORE_FLAG_VOLATILE) {
    return MDB_NOMETASYNC | MDB_NOSYNC;
  } else {
    return 0;
  }
}

kvs_store *kvs_store_open(const char *path, int32_t flags) {
  kvs_store *store = calloc(1, sizeof(kvs_store));
  MDB_txn *txn = NULL;
  mkdir(path, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
  if (mdb_env_create(&store->env) != 0) {
    goto error;
  }

  if (mdb_env_set_mapsize(store->env, 0x4FFFFFFFF) != 0) {
    goto error;
  }

  if (mdb_env_open(store->env, path, kvs_store_flags_to_mdb_env_flags(flags), S_IRWXU | S_IRGRP) != 0) {
    goto error;
  }

  if (mdb_txn_begin(store->env, NULL, 0, &txn) != 0) {
    goto error;
  }

  if (mdb_dbi_open(txn, NULL, MDB_CREATE, &store->dbi) != 0) {
    goto error;
  }

  mdb_txn_commit(txn);
  return store;

error:
  if (txn != NULL) {
    mdb_txn_abort(txn);
  }
  kvs_store_destroy(store);
  return NULL;
}

void kvs_store_destroy(kvs_store *store) {
  if (store == NULL) {
    return;
  }
  if (store->env != NULL) {
    mdb_dbi_close(store->env, store->dbi);
    mdb_env_close(store->env);
  }
  free(store);
}

kvs_status kvs_store_put(kvs_store *store, kvs_buffer *key, kvs_buffer *value) {
  kvs_status st;
  kvs_store_txn *txn = kvs_store_txn_begin(store, 0);
  if (KVS_FAILED(st = kvs_store_txn_put(txn, key, value))) {
    kvs_store_txn_abort(txn);
  } else {
    kvs_store_txn_commit(txn);
  }
  return st;
}

static uint32_t kvs_store_txn_flags_to_mdb_txn_flags(int32_t flags) {
  if ((flags & KVS_STORE_TXN_FLAG_READONLY) == KVS_STORE_TXN_FLAG_READONLY) {
    return MDB_RDONLY;
  } else {
    return 0;
  }
}

kvs_store_txn *kvs_store_txn_begin(kvs_store *store, int32_t flags) {
  kvs_store_txn *txn = malloc(sizeof(kvs_store_txn));
  if (mdb_txn_begin(store->env, NULL, kvs_store_txn_flags_to_mdb_txn_flags(flags), &txn->txn) != 0) {
    free(txn);
    txn = NULL;
  } else {
    txn->dbi = store->dbi;
  }
  return txn;
}

kvs_status kvs_store_txn_commit(kvs_store_txn *txn) {
  int32_t rc = mdb_txn_commit(txn->txn);
  free(txn);
  return kvs_store_convert_lmdb_status(rc);
}

void kvs_store_txn_abort(kvs_store_txn *txn) {
  mdb_txn_abort(txn->txn);
  free(txn);
}

kvs_status kvs_store_txn_put(kvs_store_txn *txn, kvs_buffer *key, kvs_buffer *value) {
  MDB_val mkey, mval;
  int32_t rc;
  void *free_key, *free_value;
  mkey.mv_size = kvs_buffer_size(key);
  mval.mv_size = kvs_buffer_size(value);
  if ((mkey.mv_data = (void *) kvs_buffer_peek(key, mkey.mv_size)) == NULL) {
    /* slow path */
    kvs_buffer_read(key, free_key = mkey.mv_data = malloc(mkey.mv_size), mkey.mv_size);
  } else {
    free_key = NULL;
  }
  if ((mval.mv_data = (void *) kvs_buffer_peek(value, mval.mv_size)) == NULL) {
    /* slow path */
    kvs_buffer_read(value, free_value = mval.mv_data = malloc(mval.mv_size), mval.mv_size);
  } else {
    free_value = NULL;
  }
  rc = mdb_put(txn->txn, txn->dbi, &mkey, &mval, 0);
  if (free_key != NULL) {
    free(free_key);
  } else {
    kvs_buffer_skip(key, mkey.mv_size);
  }
  if (free_value != NULL) {
    free(free_value);
  } else {
    kvs_buffer_skip(value, mval.mv_size);
  }
  return kvs_store_convert_lmdb_status(rc);
}

kvs_store_cursor *kvs_store_cursor_open(kvs_store *store) {
  kvs_store_cursor *cursor = calloc(1, sizeof(kvs_store_cursor));
  if (mdb_txn_begin(store->env, NULL, MDB_RDONLY, &cursor->txn) != 0) {
    goto error;
  }
  if (mdb_cursor_open(cursor->txn, store->dbi, &cursor->cursor) != 0) {
    goto error;
  }
  return cursor;
error:
  if (cursor->txn != NULL) {
    mdb_txn_abort(cursor->txn);
  }
  free(cursor);
  return NULL;
}

kvs_status kvs_store_cursor_seek(kvs_store_cursor *cursor, kvs_buffer *key) {
  MDB_val mkey;
  void *to_free = NULL;
  int32_t rc;
  mkey.mv_size = kvs_buffer_size(key);
  if ((mkey.mv_data = (void *) kvs_buffer_peek(key, mkey.mv_size)) == NULL) {
    kvs_buffer_read(key, mkey.mv_data = to_free = malloc(mkey.mv_size), mkey.mv_size);
  }
  rc = mdb_cursor_get(cursor->cursor, &mkey, NULL, MDB_SET_RANGE);
  if (to_free != NULL) {
    free(to_free);
  }
  return kvs_store_convert_lmdb_status(rc);
}

kvs_status kvs_store_cursor_next_no_copy(kvs_store_cursor *cursor, const void **key, size_t *key_size, const void **value, size_t *value_size) {
  MDB_val mkey, mval;
  memset(&mkey, 0, sizeof(mkey));
  memset(&mval, 0, sizeof(mval));
  int32_t rc;
  if ((rc = mdb_cursor_get(cursor->cursor, &mkey, &mval, MDB_NEXT)) == MDB_SUCCESS) {
    *key = mkey.mv_data;
    *key_size = mkey.mv_size;
    *value = mval.mv_data;
    *value_size = mval.mv_size;
  }
  return kvs_store_convert_lmdb_status(rc);
}

kvs_status kvs_store_cursor_next(kvs_store_cursor *cursor, kvs_buffer *key, kvs_buffer *value) {
  const void *k, *v;
  size_t ks, vs;
  kvs_status st;
  if ((st = kvs_store_cursor_next_no_copy(cursor, &k, &ks, &v, &vs)) == KVS_OK) {
    memcpy(kvs_buffer_allocate(key, ks), k, ks);
    memcpy(kvs_buffer_allocate(value, vs), v, vs);
  }
  return st;
}

void kvs_store_cursor_close(kvs_store_cursor *cursor) {
  mdb_cursor_close(cursor->cursor);
  mdb_txn_commit(cursor->txn);
  free(cursor);
}
