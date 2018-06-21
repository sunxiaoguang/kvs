#ifndef __KVS_UTIL_H__
#define __KVS_UTIL_H__

#include "status.h"

#define KVS_OFFSET_OF(TYPE, FIELD) (((size_t)(&((TYPE *) sizeof(TYPE))->FIELD)) - sizeof(TYPE))
#define KVS_UNSAFE_CAST(BASE, OFFSET) ((void *) (((char *) (BASE)) + OFFSET))
#define KVS_ARRAY_SIZE(ARRAY) (sizeof(ARRAY) / sizeof(ARRAY[0]))

#define KVS_DO(ST, X)                     \
  do {                                    \
    if (KVS_FAILED(ST = (X))) {           \
      return ST;                          \
    }                                     \
  } while (0)

#define KVS_DO_GOTO(ST, LABEL, X)         \
  do {                                    \
    if (KVS_FAILED(ST = (X))) {           \
      goto LABEL;                         \
    }                                     \
  } while (0)

#define KVS_CHECK_OOM(X) if ((X) == NULL) { return KVS_OUT_OF_MEMORY; }
#define KVS_CHECK_OOM_GOTO(ST, LABEL, X) if ((X) == NULL) { ST = KVS_OUT_OF_MEMORY; goto LABEL; }

#endif /* __KVS_UTIL_H__ */
