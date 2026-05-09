#include <ffi.h>
#include <stdint.h>
#include <stdlib.h>

#define ZUZU_FFI_VOID 0
#define ZUZU_FFI_BOOL 1
#define ZUZU_FFI_SINT64 2
#define ZUZU_FFI_UINT64 3
#define ZUZU_FFI_DOUBLE 4
#define ZUZU_FFI_POINTER 5

typedef union {
    uint8_t bool_value;
    int64_t sint64_value;
    uint64_t uint64_value;
    double double_value;
    void *pointer_value;
} ZuzuFfiValue;

typedef struct {
    int32_t type_code;
    ZuzuFfiValue value;
} ZuzuFfiArg;

typedef struct {
    int32_t type_code;
    ZuzuFfiValue value;
} ZuzuFfiResult;

static ffi_type *zuzu_ffi_type_for(int32_t type_code) {
    switch (type_code) {
    case ZUZU_FFI_VOID:
        return &ffi_type_void;
    case ZUZU_FFI_BOOL:
        return &ffi_type_uint8;
    case ZUZU_FFI_SINT64:
        return &ffi_type_sint64;
    case ZUZU_FFI_UINT64:
        return &ffi_type_uint64;
    case ZUZU_FFI_DOUBLE:
        return &ffi_type_double;
    case ZUZU_FFI_POINTER:
        return &ffi_type_pointer;
    default:
        return NULL;
    }
}

const char *zuzu_ffi_call(
    void *function,
    int32_t return_type,
    const int32_t *param_types,
    ZuzuFfiArg *args,
    size_t nargs,
    ZuzuFfiResult *result
) {
    ffi_cif cif;
    ffi_type *rtype = zuzu_ffi_type_for(return_type);
    if (function == NULL) {
        return "C function pointer is null";
    }
    if (rtype == NULL) {
        return "unsupported C return type";
    }

    ffi_type **atypes = calloc(nargs > 0 ? nargs : 1, sizeof(*atypes));
    void **avalues = calloc(nargs > 0 ? nargs : 1, sizeof(*avalues));
    if (atypes == NULL || avalues == NULL) {
        free(atypes);
        free(avalues);
        return "could not allocate libffi call frame";
    }

    for (size_t i = 0; i < nargs; i++) {
        atypes[i] = zuzu_ffi_type_for(param_types[i]);
        if (atypes[i] == NULL || param_types[i] == ZUZU_FFI_VOID) {
            free(atypes);
            free(avalues);
            return "unsupported C parameter type";
        }
        avalues[i] = &args[i].value;
    }

    ffi_status status = ffi_prep_cif(
        &cif,
        FFI_DEFAULT_ABI,
        (unsigned int)nargs,
        rtype,
        atypes
    );
    if (status != FFI_OK) {
        free(atypes);
        free(avalues);
        return "ffi_prep_cif failed";
    }

    result->type_code = return_type;
    result->value.uint64_value = 0;
    ffi_call(&cif, FFI_FN(function), &result->value, avalues);

    free(atypes);
    free(avalues);
    return NULL;
}
