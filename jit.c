#include "schema.h"
#include "util.h"
#define __KVS_SCHEMA_INTERNAL_H__
#include "jit.h"
#undef __KVS_SCHEMA_INTERNAL_H__
#include <llvm-c/Analysis.h>
#include <llvm-c/BitReader.h>
#include <llvm-c/BitWriter.h>
#include <llvm-c/Core.h>
#include <llvm-c/OrcBindings.h>
#include <llvm-c/Support.h>
#include <llvm-c/Target.h>
#include <llvm-c/Transforms/IPO.h>
#include <llvm-c/Transforms/PassManagerBuilder.h>
#include <llvm-c/Transforms/Scalar.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <pthread.h>
#include <unistd.h>

#define KVS_JIT_CHECK_LLVM_ERROR(ERR, X) if ((X) == NULL) { return KVS_SCHEMA_JIT_##ERR; }
#define KVS_JIT_CHECK_LLVM_ERROR_GOTO(ST, ERR, LABEL, X) if ((X) == NULL) { ST = KVS_SCHEMA_JIT_##ERR ; goto LABEL; }

typedef struct llvm_context {
  LLVMModuleRef module;
  LLVMSharedModuleRef shared_module;
  LLVMOrcModuleHandle orc;

  LLVMValueRef record_get;
  LLVMValueRef record_get_deref;
  LLVMValueRef serialize_comparable_int32;
  LLVMValueRef serialize_comparable_int64;
  LLVMValueRef serialize_comparable_float;
  LLVMValueRef serialize_comparable_double;
  LLVMValueRef serialize_comparable_opaque;
  LLVMValueRef deserialize_comparable_int32;
  LLVMValueRef deserialize_comparable_int64;
  LLVMValueRef deserialize_comparable_float;
  LLVMValueRef deserialize_comparable_double;
  LLVMValueRef deserialize_comparable_opaque;
  LLVMValueRef deserialize_opaque;
  LLVMValueRef buffer_write;
  LLVMValueRef buffer_read;
  LLVMValueRef reset_opaque;

  LLVMTypeRef entry_type;
  LLVMTypeRef int32_type;
  LLVMTypeRef int64_type;
  LLVMTypeRef void_type;
  LLVMTypeRef int32_pointer;
  LLVMTypeRef int64_pointer;

  LLVMValueRef variant_int32_size;
  LLVMValueRef variant_int32_offset;
  LLVMValueRef variant_int64_offset;
  LLVMValueRef variant_int64_size;
  LLVMValueRef variant_float_offset;
  LLVMValueRef variant_float_size;
  LLVMValueRef variant_double_offset;
  LLVMValueRef variant_double_size;
  LLVMValueRef variant_opaque_data_offset;
  LLVMValueRef variant_opaque_size_offset;
  LLVMValueRef variant_opaque_size_size;

  char name_buffer[512];
  size_t name_len;
} llvm_context;

typedef void (*jit_serializer_entry)(const kvs_record *record, kvs_buffer *key, kvs_buffer *value);
typedef void (*jit_deserializer_entry)(kvs_record *record, const kvs_buffer *key, const kvs_buffer *value);

typedef struct kvs_schema_jit_codec {
  llvm_context llvm;
  jit_serializer_entry serializer;
  jit_deserializer_entry deserializer;
} kvs_schema_jit_codec;

static pthread_once_t init_llvm = PTHREAD_ONCE_INIT;
static LLVMTargetMachineRef llvm_target_machine = NULL;
static LLVMOrcJITStackRef llvm_orc = NULL;

static void kvs_fini_llvm_once(void) {
  if (llvm_orc != NULL) {
    LLVMOrcDisposeInstance(llvm_orc);
  }
  if (llvm_target_machine != NULL) {
    LLVMDisposeTargetMachine(llvm_target_machine);
  }
  LLVMShutdown();
}

static void kvs_init_llvm_once(void) {
  char     *error = NULL;
  LLVMInitializeNativeTarget();
  LLVMInitializeNativeAsmPrinter();
  LLVMInitializeNativeAsmParser();
  char *llvm_triple = LLVMGetDefaultTargetTriple();
  LLVMTargetRef llvm_target = NULL;
  LLVMGetTargetFromTriple(llvm_triple, &llvm_target, &error);
  llvm_target_machine = LLVMCreateTargetMachine(llvm_target, llvm_triple, "", "",
      LLVMCodeGenLevelAggressive, LLVMRelocDefault, LLVMCodeModelJITDefault);
  LLVMDisposeMessage(llvm_triple);
  LLVMLoadLibraryPermanently(NULL);
  llvm_orc = LLVMOrcCreateInstance(llvm_target_machine);
  atexit(kvs_fini_llvm_once);
}

static void kvs_init_llvm(void) {
  pthread_once(&init_llvm, kvs_init_llvm_once);
}

#ifndef PATH_MAX
#define PATH_MAX (1024)
#endif
#ifdef __LINUX__
static size_t populate_exec_path(char *buffer, size_t limit) {
  if (readlink("/proc/self/exe", buffer, limit) == -1) {
    buffer = getcwd(buffer, limit);
  } else {
    *strrchr(buffer, '/') = '\0';
  }
  return strlen(buffer);
}
#else
#ifdef __DARWIN__
#include <mach-o/dyld.h>
static size_t populate_exec_path(char *buffer, size_t limit) {
  uint32_t size = limit;
  _NSGetExecutablePath(buffer, &size);
  buffer[size] = '\0';
  *strrchr(buffer, '/') = '\0';
  return strlen(buffer);
}
#endif
#endif

static kvs_status load_byte_code(const char *filename, LLVMModuleRef *module) {
  kvs_status st = KVS_OK;
  char *msg;
  char pathbuf[PATH_MAX];
  memset(pathbuf, 0, sizeof(pathbuf));
  char *start = pathbuf + populate_exec_path(pathbuf, PATH_MAX - 1);
  snprintf(start, sizeof(pathbuf) - (start - pathbuf), "/%s", filename);
  LLVMMemoryBufferRef buf;
  if (LLVMCreateMemoryBufferWithContentsOfFile(pathbuf, &buf, &msg)) {
    free(msg);
    return KVS_OUT_OF_MEMORY;
  }
  if (LLVMParseBitcode2(buf, module)) {
    st = KVS_SCHEMA_JIT_INVALID_RUNTIME;
  }
  LLVMDisposeMemoryBuffer(buf);
  return st;
}

static kvs_status kvs_jit_llvm_context_init(llvm_context *llvm) {
  kvs_status st;
  kvs_init_llvm();
  KVS_CHECK_OOM(llvm->module = LLVMModuleCreateWithName("kvs_jit"));
  KVS_DO(st, load_byte_code("jitrt.bc", &llvm->module));
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, llvm->int32_type = LLVMInt32Type());
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, llvm->int64_type = LLVMInt64Type());
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, llvm->void_type = LLVMVoidType());
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, llvm->int32_pointer = LLVMPointerType(llvm->int32_type, 0));
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, llvm->int64_pointer = LLVMPointerType(llvm->int64_type, 0));
  LLVMTypeRef params[] = {
    llvm->int64_type, /* record */
    llvm->int64_type, /* kvs_buffer *key */
    llvm->int64_type, /* kvs_buffer *value */
  };
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, llvm->entry_type = LLVMFunctionType(llvm->void_type, params, KVS_ARRAY_SIZE(params), 0));
  KVS_JIT_CHECK_LLVM_ERROR(INVALID_RUNTIME, llvm->record_get = LLVMGetNamedFunction(llvm->module, "kvs_jit_rt_record_get"));
  KVS_JIT_CHECK_LLVM_ERROR(INVALID_RUNTIME, llvm->record_get_deref = LLVMGetNamedFunction(llvm->module, "kvs_jit_rt_record_get_deref"));
  KVS_JIT_CHECK_LLVM_ERROR(INVALID_RUNTIME, llvm->serialize_comparable_int32 = LLVMGetNamedFunction(llvm->module, "kvs_jit_rt_variant_serialize_comparable_int32"));
  KVS_JIT_CHECK_LLVM_ERROR(INVALID_RUNTIME, llvm->serialize_comparable_int64 = LLVMGetNamedFunction(llvm->module, "kvs_jit_rt_variant_serialize_comparable_int64"));
  KVS_JIT_CHECK_LLVM_ERROR(INVALID_RUNTIME, llvm->serialize_comparable_float = LLVMGetNamedFunction(llvm->module, "kvs_jit_rt_variant_serialize_comparable_float"));
  KVS_JIT_CHECK_LLVM_ERROR(INVALID_RUNTIME, llvm->serialize_comparable_double = LLVMGetNamedFunction(llvm->module, "kvs_jit_rt_variant_serialize_comparable_double"));
  KVS_JIT_CHECK_LLVM_ERROR(INVALID_RUNTIME, llvm->serialize_comparable_opaque = LLVMGetNamedFunction(llvm->module, "kvs_jit_rt_variant_serialize_comparable_opaque"));
  KVS_JIT_CHECK_LLVM_ERROR(INVALID_RUNTIME, llvm->deserialize_comparable_int32 = LLVMGetNamedFunction(llvm->module, "kvs_jit_rt_variant_deserialize_comparable_int32"));
  KVS_JIT_CHECK_LLVM_ERROR(INVALID_RUNTIME, llvm->deserialize_comparable_int64 = LLVMGetNamedFunction(llvm->module, "kvs_jit_rt_variant_deserialize_comparable_int64"));
  KVS_JIT_CHECK_LLVM_ERROR(INVALID_RUNTIME, llvm->deserialize_comparable_float = LLVMGetNamedFunction(llvm->module, "kvs_jit_rt_variant_deserialize_comparable_float"));
  KVS_JIT_CHECK_LLVM_ERROR(INVALID_RUNTIME, llvm->deserialize_comparable_double = LLVMGetNamedFunction(llvm->module, "kvs_jit_rt_variant_deserialize_comparable_double"));
  KVS_JIT_CHECK_LLVM_ERROR(INVALID_RUNTIME, llvm->deserialize_comparable_opaque = LLVMGetNamedFunction(llvm->module, "kvs_jit_rt_variant_deserialize_comparable_opaque"));
  KVS_JIT_CHECK_LLVM_ERROR(INVALID_RUNTIME, llvm->deserialize_opaque = LLVMGetNamedFunction(llvm->module, "kvs_jit_rt_variant_deserialize_opaque"));
  KVS_JIT_CHECK_LLVM_ERROR(INVALID_RUNTIME, llvm->buffer_write = LLVMGetNamedFunction(llvm->module, "kvs_jit_rt_buffer_write"));
  KVS_JIT_CHECK_LLVM_ERROR(INVALID_RUNTIME, llvm->buffer_read = LLVMGetNamedFunction(llvm->module, "kvs_jit_rt_buffer_read"));

  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, llvm->variant_int32_offset = LLVMConstInt(llvm->int64_type, kvs_variant_int32_offset(), 0));
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, llvm->variant_int32_size = LLVMConstInt(llvm->int64_type, sizeof(int32_t), 0));
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, llvm->variant_int64_offset = LLVMConstInt(llvm->int64_type, kvs_variant_int64_offset(), 0));
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, llvm->variant_int64_size = LLVMConstInt(llvm->int64_type, sizeof(int64_t), 0));
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, llvm->variant_float_offset = LLVMConstInt(llvm->int64_type, kvs_variant_float_offset(), 0));
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, llvm->variant_float_size = LLVMConstInt(llvm->int64_type, sizeof(float), 0));
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, llvm->variant_double_offset = LLVMConstInt(llvm->int64_type, kvs_variant_double_offset(), 0));
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, llvm->variant_double_size = LLVMConstInt(llvm->int64_type, sizeof(double), 0));
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, llvm->variant_opaque_data_offset = LLVMConstInt(llvm->int64_type, kvs_variant_opaque_data_offset(), 0));
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, llvm->variant_opaque_size_offset = LLVMConstInt(llvm->int64_type, kvs_variant_opaque_size_offset(), 0));
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, llvm->variant_opaque_size_size = LLVMConstInt(llvm->int64_type, sizeof(int32_t), 0));
  return KVS_OK;
}

static void kvs_jit_llvm_context_fini(llvm_context *llvm) {
  if (llvm->shared_module != NULL) {
    LLVMOrcRemoveModule(llvm_orc, llvm->orc);
    LLVMOrcDisposeSharedModuleRef(llvm->shared_module);
  }
  if (llvm->module != NULL) {
    LLVMDisposeModule(llvm->module);
  }
}

static const char *llvm_name_with_suffix(llvm_context *llvm, const char *suffix) {
  size_t len = strlen(suffix);
  memcpy(llvm->name_buffer + llvm->name_len, suffix, len);
  llvm->name_buffer[llvm->name_len + len] = '\0';
  return llvm->name_buffer;
}

static const char *llvm_serialize_name(llvm_context *llvm, const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  llvm->name_len = vsnprintf(llvm->name_buffer, sizeof(llvm->name_buffer), fmt, args);
  va_end(args);
  return llvm->name_buffer;
}

static kvs_status kvs_schema_jit_generate_primitive_serializer(llvm_context *llvm, LLVMBuilderRef builder, LLVMValueRef buffer, LLVMValueRef field, LLVMValueRef offset, LLVMValueRef size) {
  LLVMValueRef data = LLVMBuildAdd(builder, field, offset, llvm_name_with_suffix(llvm, "@primitie_address"));
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, data);
  LLVMValueRef buffer_write_args[] = { buffer, data, size };
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, LLVMBuildCall(builder, llvm->buffer_write, buffer_write_args, KVS_ARRAY_SIZE(buffer_write_args), ""));
  return KVS_OK;
}

static kvs_status kvs_schema_jit_generate_opaque_serializer(llvm_context *llvm, LLVMBuilderRef builder, LLVMValueRef buffer, LLVMValueRef field, LLVMValueRef data_offset, LLVMValueRef size_offset, LLVMValueRef size_size) {
  LLVMValueRef size = LLVMBuildAdd(builder, field, size_offset, llvm_name_with_suffix(llvm, "@opaque_size_address"));
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, size);
  LLVMValueRef data = LLVMBuildAdd(builder, field, data_offset, llvm_name_with_suffix(llvm, "@opaque_data_address"));
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, data);
  LLVMValueRef buffer_write_args[] = { buffer, size, size_size };
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, LLVMBuildCall(builder, llvm->buffer_write, buffer_write_args, KVS_ARRAY_SIZE(buffer_write_args), ""));
  LLVMValueRef size_int32_ptr = LLVMBuildIntToPtr(builder, size, llvm->int32_pointer, llvm_name_with_suffix(llvm, "@opaque_size_int32_address"));
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, size_int32_ptr);
  LLVMValueRef size_int32 = LLVMBuildLoad(builder, size_int32_ptr, llvm_name_with_suffix(llvm, "@opaque_size_int32"));
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, size_int32);
  LLVMValueRef size_int64 = LLVMBuildIntCast(builder, size_int32, llvm->int64_type, llvm_name_with_suffix(llvm, "@opaque_size_int64"));
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, size_int64);
  LLVMValueRef data_int64_ptr = LLVMBuildIntToPtr(builder, data, llvm->int64_pointer, llvm_name_with_suffix(llvm, "@opaque_data_pointer_to_address"));
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, data_int64_ptr);
  LLVMValueRef data_int64 = LLVMBuildLoad(builder, data_int64_ptr, llvm_name_with_suffix(llvm, "@opaque_data_address"));
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, data_int64);

  buffer_write_args[1] = data_int64;
  buffer_write_args[2] = size_int64;
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, LLVMBuildCall(builder, llvm->buffer_write, buffer_write_args, KVS_ARRAY_SIZE(buffer_write_args), ""));
  return KVS_OK;
}

static kvs_status kvs_schema_jit_generate_primitive_deserializer(llvm_context *llvm, LLVMBuilderRef builder, LLVMValueRef buffer, LLVMValueRef field, LLVMValueRef offset, LLVMValueRef size) {
  LLVMValueRef data_address_ptr = LLVMBuildIntToPtr(builder, field, llvm->int64_pointer, llvm_name_with_suffix(llvm, "@pointer_to_address"));
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, data_address_ptr);
  LLVMValueRef data_address = LLVMBuildLoad(builder, data_address_ptr, llvm_name_with_suffix(llvm, "@address"));
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, data_address);
  LLVMValueRef data = LLVMBuildAdd(builder, data_address, offset, llvm_name_with_suffix(llvm, "@primitie_address"));
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, data);
  LLVMValueRef buffer_read_args[] = { buffer, data, size };
  KVS_JIT_CHECK_LLVM_ERROR(INTERNAL_ERROR, LLVMBuildCall(builder, llvm->buffer_read, buffer_read_args, KVS_ARRAY_SIZE(buffer_read_args), ""));
  return KVS_OK;
}

static kvs_status kvs_schema_jit_optimize(LLVMModuleRef module) {
  int32_t optlevel = 3;
  kvs_status st = KVS_OK;
  LLVMPassManagerBuilderRef pmb = NULL;
  LLVMPassManagerRef mpm = NULL, fpm = NULL;
  LLVMValueRef func = NULL;

  KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, pmb = LLVMPassManagerBuilderCreate());
  LLVMPassManagerBuilderSetOptLevel(pmb, optlevel);
  KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, fpm = LLVMCreateFunctionPassManagerForModule(module));

  if (optlevel == 3) {
    LLVMPassManagerBuilderUseInlinerWithThreshold(pmb, 512);
  } else {
    LLVMAddPromoteMemoryToRegisterPass(fpm);
  }

  LLVMPassManagerBuilderPopulateFunctionPassManager(pmb, fpm);
  LLVMInitializeFunctionPassManager(fpm);
  for (func = LLVMGetFirstFunction(module); func != NULL; func = LLVMGetNextFunction(func)) {
    LLVMRunFunctionPassManager(fpm, func);
  }
  LLVMFinalizeFunctionPassManager(fpm);

  KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, mpm = LLVMCreatePassManager());
  LLVMPassManagerBuilderPopulateModulePassManager(pmb, mpm);
  LLVMAddAlwaysInlinerPass(mpm);
  LLVMAddFunctionInliningPass(mpm);
  LLVMRunPassManager(mpm, module);

cleanup_exit:
  if (fpm != NULL) { LLVMDisposePassManager(fpm); }
  if (mpm != NULL) { LLVMDisposePassManager(mpm); }
  if (pmb != NULL) { LLVMPassManagerBuilderDispose(pmb); }
  return st;
}

static uint64_t kvs_schema_jit_resolve_symbol(const char *symname, void *ctx) {
#ifdef __DARWIN__
  /* strip the leading underscore on darwin */
  void *data = (LLVMSearchForAddressOfSymbol(symname + 1));
#else
  void *data = (LLVMSearchForAddressOfSymbol(symname));
#endif
  return (uintptr_t) data;
}

static kvs_status kvs_schema_jit_compile(llvm_context *llvm) {
  kvs_schema_jit_optimize(llvm->module);
  LLVMSharedModuleRef shared_module = LLVMOrcMakeSharedModule(llvm->module);
  if (LLVMOrcAddEagerlyCompiledIR(llvm_orc, &llvm->orc, shared_module, kvs_schema_jit_resolve_symbol, NULL) != LLVMOrcErrSuccess) {
    return KVS_SCHEMA_JIT_INTERNAL_ERROR;
  } else {
    llvm->shared_module = shared_module;
    llvm->module = NULL;
    return KVS_OK;
  }
}

void *kvs_schema_jit_codec_create(const kvs_column **keys, size_t key_size, const kvs_column **values, size_t value_size) {
  size_t idx;
  kvs_status st;
  LLVMBuilderRef builder = NULL;
  LLVMValueRef deserializer = NULL, serializer = NULL;
  kvs_schema_jit_codec *jit = calloc(1, sizeof(kvs_schema_jit_codec));
  KVS_CHECK_OOM_GOTO(st, cleanup_exit, jit);
  KVS_DO_GOTO(st, cleanup_exit, kvs_jit_llvm_context_init(&jit->llvm));
  llvm_context *llvm = &jit->llvm;
  KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, builder = LLVMCreateBuilderInContext(LLVMGetGlobalContext()));
  KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, serializer = LLVMAddFunction(llvm->module, "serialize", llvm->entry_type));
  LLVMBasicBlockRef body = LLVMAppendBasicBlock(serializer, "serialize_body");
  KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, body);
  LLVMPositionBuilderAtEnd(builder, body);
  LLVMValueRef record = LLVMGetParam(serializer, 0);
  KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, record);
  LLVMValueRef key = LLVMGetParam(serializer, 1);
  KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, key);
  LLVMValueRef value = LLVMGetParam(serializer, 2);
  KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, value);

  for (idx = 0; idx < key_size; ++idx) {
    const kvs_column *column = keys[idx];
    LLVMValueRef field_index = LLVMConstInt(llvm->int64_type, column->index, 0);
    KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, field_index);
    LLVMValueRef record_get_args[] = { record, field_index };
    LLVMValueRef field = LLVMBuildCall(builder, llvm->record_get, record_get_args, KVS_ARRAY_SIZE(record_get_args),
        llvm_serialize_name(llvm, "pk@%zd", idx));
    KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, field);
    LLVMValueRef serialize_args[] = { field, key };
    switch (column->type) {
      case KVS_VARIANT_TYPE_INT32:
        KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, 
            LLVMBuildCall(builder, llvm->serialize_comparable_int32, serialize_args, KVS_ARRAY_SIZE(serialize_args), ""));
        break;
      case KVS_VARIANT_TYPE_INT64:
        KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, 
            LLVMBuildCall(builder, llvm->serialize_comparable_int64, serialize_args, KVS_ARRAY_SIZE(serialize_args), ""));
        break;
      case KVS_VARIANT_TYPE_FLOAT:
        KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, 
            LLVMBuildCall(builder, llvm->serialize_comparable_float, serialize_args, KVS_ARRAY_SIZE(serialize_args), ""));
        break;
      case KVS_VARIANT_TYPE_DOUBLE:
        KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, 
            LLVMBuildCall(builder, llvm->serialize_comparable_double, serialize_args, KVS_ARRAY_SIZE(serialize_args), ""));
        break;
      case KVS_VARIANT_TYPE_OPAQUE:
        KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, 
            LLVMBuildCall(builder, llvm->serialize_comparable_opaque, serialize_args, KVS_ARRAY_SIZE(serialize_args), ""));
        break;
      default:
        break;
    }
  }

  for (idx = 0; idx < value_size; ++idx) {
    const kvs_column *column = values[idx];
    LLVMValueRef field_index = LLVMConstInt(llvm->int64_type, column->index, 0);
    KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, field_index);
    LLVMValueRef record_get_args[] = { record, field_index };
    LLVMValueRef field = LLVMBuildCall(builder, llvm->record_get_deref, record_get_args, KVS_ARRAY_SIZE(record_get_args),
        llvm_serialize_name(llvm, "column@%zd", idx));
    KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, field);
    switch (column->type) {
      case KVS_VARIANT_TYPE_INT32:
        KVS_DO_GOTO(st, cleanup_exit, kvs_schema_jit_generate_primitive_serializer(llvm, builder, value, field, llvm->variant_int32_offset, llvm->variant_int32_size));
        break;
      case KVS_VARIANT_TYPE_INT64:
        KVS_DO_GOTO(st, cleanup_exit, kvs_schema_jit_generate_primitive_serializer(llvm, builder, value, field, llvm->variant_int64_offset, llvm->variant_int64_size));
        break;
      case KVS_VARIANT_TYPE_FLOAT:
        KVS_DO_GOTO(st, cleanup_exit, kvs_schema_jit_generate_primitive_serializer(llvm, builder, value, field, llvm->variant_float_offset, llvm->variant_float_size));
        break;
      case KVS_VARIANT_TYPE_DOUBLE:
        KVS_DO_GOTO(st, cleanup_exit, kvs_schema_jit_generate_primitive_serializer(llvm, builder, value, field, llvm->variant_double_offset, llvm->variant_double_size));
        break;
      case KVS_VARIANT_TYPE_OPAQUE:
        KVS_DO_GOTO(st, cleanup_exit, kvs_schema_jit_generate_opaque_serializer(llvm, builder, value, field, llvm->variant_opaque_data_offset, llvm->variant_opaque_size_offset, llvm->variant_opaque_size_size));
        break;
      default:
        break;
    }
  }
  KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, LLVMBuildRetVoid(builder));

  KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, 
      deserializer = LLVMAddFunction(llvm->module, "deserialize", llvm->entry_type));
  KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, 
      body = LLVMAppendBasicBlock(deserializer, "deserialize_body"));
  LLVMPositionBuilderAtEnd(builder, body);
  KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, record = LLVMGetParam(deserializer, 0));
  KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, key = LLVMGetParam(deserializer, 1));
  KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, value = LLVMGetParam(deserializer, 2));

  for (idx = 0; idx < key_size; ++idx) {
    const kvs_column *column = keys[idx];
    LLVMValueRef field_index = LLVMConstInt(llvm->int64_type, column->index, 0);
    KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, field_index);
    LLVMValueRef record_get_args[] = { record, field_index };
    LLVMValueRef field = LLVMBuildCall(builder, llvm->record_get, record_get_args, KVS_ARRAY_SIZE(record_get_args),
        llvm_serialize_name(llvm, "pk@%zd", idx));
    KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, field);
    LLVMValueRef deserialize_args[] = { field, key };
    switch (column->type) {
      case KVS_VARIANT_TYPE_INT32:
        KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, 
            LLVMBuildCall(builder, llvm->deserialize_comparable_int32, deserialize_args, KVS_ARRAY_SIZE(deserialize_args), ""));
        break;
      case KVS_VARIANT_TYPE_INT64:
        KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, 
            LLVMBuildCall(builder, llvm->deserialize_comparable_int64, deserialize_args, KVS_ARRAY_SIZE(deserialize_args), ""));
        break;
      case KVS_VARIANT_TYPE_FLOAT:
        KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, 
            LLVMBuildCall(builder, llvm->deserialize_comparable_float, deserialize_args, KVS_ARRAY_SIZE(deserialize_args), ""));
        break;
      case KVS_VARIANT_TYPE_DOUBLE:
        KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, 
            LLVMBuildCall(builder, llvm->deserialize_comparable_double, deserialize_args, KVS_ARRAY_SIZE(deserialize_args), ""));
        break;
      case KVS_VARIANT_TYPE_OPAQUE:
        KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, 
            LLVMBuildCall(builder, llvm->deserialize_comparable_opaque, deserialize_args, KVS_ARRAY_SIZE(deserialize_args), ""));
        break;
      default:
        break;
    }
  }

  for (idx = 0; idx < value_size; ++idx) {
    const kvs_column *column = values[idx];
    LLVMValueRef field_index = LLVMConstInt(llvm->int64_type, column->index, 0);
    KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, field_index);
    LLVMValueRef record_get_args[] = { record, field_index };
    LLVMValueRef field = LLVMBuildCall(builder, llvm->record_get, record_get_args, KVS_ARRAY_SIZE(record_get_args),
        llvm_serialize_name(llvm, "column@%zd", idx));
    KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, field);
    switch (column->type) {
      case KVS_VARIANT_TYPE_INT32:
        KVS_DO_GOTO(st, cleanup_exit, kvs_schema_jit_generate_primitive_deserializer(llvm, builder, value, field, llvm->variant_int32_offset, llvm->variant_int32_size));
        break;
      case KVS_VARIANT_TYPE_INT64:
        KVS_DO_GOTO(st, cleanup_exit, kvs_schema_jit_generate_primitive_deserializer(llvm, builder, value, field, llvm->variant_int64_offset, llvm->variant_int64_size));
        break;
      case KVS_VARIANT_TYPE_FLOAT:
        KVS_DO_GOTO(st, cleanup_exit, kvs_schema_jit_generate_primitive_deserializer(llvm, builder, value, field, llvm->variant_float_offset, llvm->variant_float_size));
        break;
      case KVS_VARIANT_TYPE_DOUBLE:
        KVS_DO_GOTO(st, cleanup_exit, kvs_schema_jit_generate_primitive_deserializer(llvm, builder, value, field, llvm->variant_double_offset, llvm->variant_double_size));
        break;
      case KVS_VARIANT_TYPE_OPAQUE:
        {
          LLVMValueRef args[] = { field, value };
          KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, LLVMBuildCall(builder, llvm->deserialize_opaque, args, KVS_ARRAY_SIZE(args), ""));
        }
        break;
      default:
        break;
    }
  }
  KVS_JIT_CHECK_LLVM_ERROR_GOTO(st, INTERNAL_ERROR, cleanup_exit, LLVMBuildRetVoid(builder));
  LLVMDisposeBuilder(builder);
  KVS_DO_GOTO(st, cleanup_exit, kvs_schema_jit_compile(llvm));
  LLVMOrcTargetAddress serialize_addr = 0, deserialize_addr = 0;
  if (LLVMOrcGetSymbolAddress(llvm_orc, &serialize_addr, "serialize") != LLVMOrcErrSuccess) {
    goto cleanup_exit;
  }
  if (LLVMOrcGetSymbolAddress(llvm_orc, &deserialize_addr, "deserialize") != LLVMOrcErrSuccess) {
    goto cleanup_exit;
  }
  jit->serializer = (jit_serializer_entry) serialize_addr;
  jit->deserializer = (jit_deserializer_entry) deserialize_addr;
  return jit;
cleanup_exit:
  if (jit != NULL) { kvs_jit_llvm_context_fini(&jit->llvm); free(jit); }
  if (builder != NULL) { LLVMDisposeBuilder(builder); }
  return NULL;
}

void kvs_schema_jit_codec_destroy(const kvs_column **keys, size_t key_size, const kvs_column **values, size_t value_size, void *opaque) {
  kvs_schema_jit_codec *codec = opaque;
  if (codec == NULL) {
    return;
  }
  kvs_jit_llvm_context_fini(&codec->llvm);
  free(codec);
}

void kvs_schema_jit_serializer(const kvs_column **keys, size_t key_size, const kvs_column **values, size_t value_size, kvs_record *record, kvs_buffer *key, kvs_buffer *value, void *opaque) {
  kvs_schema_jit_codec *serializer = opaque;
  serializer->serializer(record, key, value);
}

void kvs_schema_jit_deserializer(const kvs_column **keys, size_t key_size, const kvs_column **values, size_t value_size, kvs_buffer *key, kvs_buffer *value, void *opaque, kvs_record *dest) {
  kvs_schema_jit_codec *serializer = opaque;
  serializer->deserializer(dest, key, value);
}
