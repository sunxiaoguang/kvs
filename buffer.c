#include "buffer.h"
#include "util.h"
#include <string.h>

#define BUFFER_ALIGN(size, align) (((size) + (align) - 1) & ~((align) - 1))
#define BUFFER_SIZE(size) BUFFER_ALIGN(size + sizeof(kvs_block_buffer), 8)

#define kvs_block_buffer_available(BUFFER) ((BUFFER)->capacity - (BUFFER)->size)
#define kvs_block_buffer_size(BUFFER) ((BUFFER)->size)
#define kvs_block_buffer_get(BUFFER, OFFSET) ((BUFFER)->buffer + (OFFSET))

typedef enum kvs_block_buffer_flag {
  KVS_BLOCK_BUFFER_FLAG_ATTACHED = 1 << 0
} kvs_block_buffer_flag;

typedef struct kvs_block_buffer kvs_block_buffer;

struct kvs_block_buffer {
  kvs_block_buffer *next;
  int32_t offset; /* read offset of the buffer */
  int32_t size;
  int32_t capacity;
  int32_t flags;
  uint8_t *buffer;
};

struct kvs_buffer {
  kvs_block_buffer *head;
  kvs_block_buffer *current;
  kvs_block_buffer **next;
  int32_t num_blocks;
  int32_t block_size;
  int32_t size;
};

static void *kvs_block_buffer_allocate(kvs_block_buffer *buffer, size_t size) {
  void *result = buffer->buffer + buffer->size;
  buffer->size += size;
  return result;
}

static void kvs_block_buffer_destroy(kvs_block_buffer **buffer_address, int32_t limit) {
  kvs_block_buffer *next;
  kvs_block_buffer *buffer = *buffer_address;
  while (buffer && limit > 0) {
    next = buffer->next;
    if ((buffer->flags & KVS_BLOCK_BUFFER_FLAG_ATTACHED) == KVS_BLOCK_BUFFER_FLAG_ATTACHED) {
      free(buffer->buffer);
    }
    free(buffer);
    buffer = next;
    --limit;
  }
  *buffer_address = buffer;
}

static kvs_block_buffer *kvs_block_buffer_create(int32_t size)
{
  int32_t allocation_size = BUFFER_SIZE(size);
  int32_t capacity = allocation_size - sizeof(kvs_block_buffer);
  kvs_block_buffer *new_buffer = malloc(allocation_size);
  memset(new_buffer, 0, KVS_OFFSET_OF(kvs_block_buffer, buffer));
  new_buffer->capacity = capacity;
  new_buffer->buffer = KVS_UNSAFE_CAST(new_buffer, sizeof(kvs_block_buffer));
  return new_buffer;
}

static void kvs_block_buffer_chain(kvs_block_buffer **head, kvs_block_buffer **current,
    kvs_block_buffer ***next_address, int32_t *num_buffers, kvs_block_buffer *next) {
  *current = next;
  if (!*head) {
    *head = *current;
  }
  (*num_buffers)++;
  **next_address = next;
  *next_address = &next->next;
}

static kvs_block_buffer *kvs_block_buffer_check(kvs_block_buffer **head, kvs_block_buffer **current,
    kvs_block_buffer ***next_address, int32_t *num_buffers, size_t required, int32_t chunk_size) {
  kvs_block_buffer *current_buffer = *current;
  if (!current_buffer || kvs_block_buffer_available(current_buffer) < (int32_t)(required)) {
    if ((int32_t) required < chunk_size) {
      required = chunk_size;
    }
    kvs_block_buffer_chain(head, current, next_address, num_buffers, current_buffer = kvs_block_buffer_create(required));
  }
  return current_buffer;
}

void *kvs_buffer_allocate(kvs_buffer *buffer, size_t size) {
  kvs_block_buffer *block = kvs_block_buffer_check(&buffer->head, &buffer->current, &buffer->next, &buffer->num_blocks, size, buffer->block_size);
  buffer->size += size;
  return kvs_block_buffer_allocate(block, size);
}

void kvs_buffer_write(kvs_buffer *buffer, const void *data, size_t size) {
  memcpy(kvs_buffer_allocate(buffer, size), data, size);
}

size_t kvs_buffer_read(kvs_buffer *buffer, void *dest, size_t size) {
  char *cdest = dest;
  int32_t left = size, read = size;
  kvs_block_buffer *block;
  while (left > 0 && (block = buffer->head) != NULL && read > 0) {
    read = (left > block->size - block->offset) ? block->size : left;
    if (cdest != NULL) {
      memcpy(cdest, block->buffer + block->offset, read);
    }
    block->offset += read;
    if (block->offset == block->size) {
      if (buffer->num_blocks > 1) {
        kvs_block_buffer_destroy(&buffer->head, 1);
        buffer->num_blocks--;
      } else {
        buffer->head->offset = 0;
        buffer->head->size = 0;
      }
    }
    buffer->size -= read;
    left -= read;
  }
  return size - left;
}


void *kvs_buffer_reserve(kvs_buffer *buffer, size_t size) {
  kvs_block_buffer *block = kvs_block_buffer_check(&buffer->head, &buffer->current, &buffer->next, &buffer->num_blocks, size, buffer->block_size);
  return block->buffer + block->size;
}

void kvs_buffer_commit(kvs_buffer *buffer, size_t size) {
  buffer->current->size += size;
  buffer->size += size;
}

kvs_buffer *kvs_buffer_create(int32_t block_size) {
  kvs_buffer *buffer = calloc(1, sizeof(kvs_buffer));
  buffer->next = &buffer->current;
  buffer->block_size = block_size;
  return buffer;
}

void kvs_buffer_destroy(kvs_buffer *buffer) {
  kvs_block_buffer_destroy(&buffer->head, 0x7FFFFFFF);
  free(buffer);
}

size_t kvs_buffer_size(kvs_buffer *buffer) {
  return buffer->size;
}

size_t kvs_buffer_skip(kvs_buffer *buffer, size_t size) {
  return kvs_buffer_read(buffer, NULL, size);
}

const void *kvs_buffer_peek(kvs_buffer *buffer, size_t size) {
  int32_t expected = (int32_t) size;
  if (buffer->size >= expected && (buffer->head->size - buffer->head->offset) >= expected) {
    return buffer->head->buffer + buffer->head->offset;
  } else {
    return NULL;
  }
}
