#ifndef __KVS_BUFFER_H__
#define __KVS_BUFFER_H__

#include <stdint.h>
#include <stdlib.h>

/**
 * LIMITATION: total size of chained buffer can not exceed 2GB
 **/
typedef struct kvs_buffer kvs_buffer;

void *kvs_buffer_allocate(kvs_buffer *buffer, size_t size);
void kvs_buffer_write(kvs_buffer *buffer, const void *data, size_t size);
void *kvs_buffer_reserve(kvs_buffer *buffer, size_t size);
void kvs_buffer_commit(kvs_buffer *buffer, size_t size);
kvs_buffer *kvs_buffer_create(int32_t block_size);
void kvs_buffer_destroy(kvs_buffer *buffer);
size_t kvs_buffer_size(kvs_buffer *buffer);
size_t kvs_buffer_read(kvs_buffer *buffer, void *dest, size_t size);
size_t kvs_buffer_skip(kvs_buffer *buffer, size_t size);
const void *kvs_buffer_peek(kvs_buffer *buffer, size_t size);

#endif /* __KVS_BUFFER_H__ */
