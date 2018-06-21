#ifdef __KVS_SCHEMA_INTERNAL_H__
void kvs_schema_jit_serializer(const kvs_column **keys, size_t key_size, const kvs_column **values, size_t value_size, kvs_record *record, kvs_buffer *key, kvs_buffer *value, void *opaque);
void kvs_schema_jit_deserializer(const kvs_column **keys, size_t key_size, const kvs_column **values, size_t value_size, kvs_buffer *key, kvs_buffer *value, void *opaque, kvs_record *dest);
void *kvs_schema_jit_codec_create(const kvs_column **keys, size_t key_size, const kvs_column **values, size_t value_size);
void kvs_schema_jit_codec_destroy(const kvs_column **keys, size_t key_size, const kvs_column **values, size_t value_size, void *opaque);
#else
#error "Internal Header Used"
#endif
