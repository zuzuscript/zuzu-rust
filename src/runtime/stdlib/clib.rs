use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::path::Path;
use std::rc::Rc;

use super::super::{MethodValue, ObjectValue, Runtime, TraitValue, UserClassValue, Value};
use crate::error::{Result, ZuzuRustError};

#[cfg(unix)]
#[link(name = "dl")]
extern "C" {
    fn dlopen(filename: *const c_char, flags: c_int) -> *mut c_void;
    fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
    fn dlclose(handle: *mut c_void) -> c_int;
    fn dlerror() -> *const c_char;
}

#[cfg(unix)]
const RTLD_NOW: c_int = 2;

const ZUZU_FFI_VOID: c_int = 0;
const ZUZU_FFI_BOOL: c_int = 1;
const ZUZU_FFI_SINT64: c_int = 2;
const ZUZU_FFI_UINT64: c_int = 3;
const ZUZU_FFI_DOUBLE: c_int = 4;
const ZUZU_FFI_POINTER: c_int = 5;

#[repr(C)]
#[derive(Clone, Copy)]
union FfiValue {
    bool_value: u8,
    sint64_value: i64,
    uint64_value: u64,
    double_value: f64,
    pointer_value: *mut c_void,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct FfiArg {
    type_code: c_int,
    value: FfiValue,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct FfiResult {
    type_code: c_int,
    value: FfiValue,
}

extern "C" {
    fn zuzu_ffi_call(
        function: *mut c_void,
        return_type: c_int,
        param_types: *const c_int,
        args: *mut FfiArg,
        nargs: usize,
        result: *mut FfiResult,
    ) -> *const c_char;
}

#[derive(Default)]
pub(crate) struct ClibState {
    next_id: usize,
    libraries: HashMap<String, ClibLibraryState>,
    functions: HashMap<String, ClibFunctionState>,
}

struct ClibLibraryState {
    handle: *mut c_void,
    closed: bool,
}

impl Drop for ClibLibraryState {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            #[cfg(unix)]
            unsafe {
                let _ = dlclose(self.handle);
            }
        }
    }
}

struct ClibFunctionState {
    library_id: String,
    name: String,
    symbol: *mut c_void,
    params: Vec<ClibDescriptor>,
    return_type: ClibDescriptor,
    free_symbol: Option<*mut c_void>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ClibKind {
    Null,
    Bool,
    Int,
    Float,
    Binary,
}

#[derive(Clone, Debug)]
struct ClibDescriptor {
    kind: ClibKind,
    signed: bool,
    terminated_by_nul: bool,
    nullable: bool,
    length: Option<usize>,
    length_arg: Option<usize>,
    free: Option<String>,
}

pub(super) fn exports() -> HashMap<String, Value> {
    HashMap::from([
        ("CLib".to_owned(), Value::builtin_class("CLib".to_owned())),
        (
            "CLibrary".to_owned(),
            Value::builtin_class("CLibrary".to_owned()),
        ),
        (
            "CFunction".to_owned(),
            Value::builtin_class("CFunction".to_owned()),
        ),
    ])
}

pub(super) fn construct_clib(args: Vec<Value>, named_args: Vec<(String, Value)>) -> Result<Value> {
    if !args.is_empty() || !named_args.is_empty() {
        return Err(ZuzuRustError::runtime(
            "CLib constructor takes no arguments",
        ));
    }
    Ok(Value::builtin_class("CLib".to_owned()))
}

pub(super) fn call_class_method(
    runtime: &Runtime,
    class_name: &str,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    let value = match (class_name, name) {
        ("CLib", "open") => clib_open(runtime, args),
        _ => return None,
    };
    Some(value)
}

pub(super) fn has_builtin_object_method(class_name: &str, name: &str) -> bool {
    matches!(
        (class_name, name),
        ("CLibrary", "func")
            | ("CLibrary", "has_symbol")
            | ("CLibrary", "close")
            | ("CFunction", "call")
    )
}

pub(super) fn call_object_method(
    runtime: &Runtime,
    class_name: &str,
    builtin_value: &Value,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    let value = match (class_name, name) {
        ("CLibrary", "func") => library_func(runtime, builtin_value, args),
        ("CLibrary", "has_symbol") => library_has_symbol(runtime, builtin_value, args),
        ("CLibrary", "close") => library_close(runtime, builtin_value, args),
        ("CFunction", "call") => function_call(runtime, builtin_value, args),
        _ => return None,
    };
    Some(value)
}

fn clib_open(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    runtime.assert_capability("clib", "CLib.open is denied by runtime policy")?;
    if args.len() != 1 {
        return Err(ZuzuRustError::runtime("CLib.open() expects a path"));
    }
    let raw_path = runtime.render_value(&args[0])?;
    let path = super::io::resolve_fs_path(runtime, Path::new(&raw_path));
    let path_text = path.to_string_lossy().to_string();
    let c_path = CString::new(path_text.clone())
        .map_err(|_| ZuzuRustError::runtime("CLib.open path contains NUL byte"))?;
    #[cfg(unix)]
    let handle = unsafe { dlopen(c_path.as_ptr(), RTLD_NOW) };
    #[cfg(not(unix))]
    let handle: *mut c_void = std::ptr::null_mut();
    if handle.is_null() {
        return Err(ZuzuRustError::runtime(format!(
            "Could not load C library '{}': {}",
            raw_path,
            dl_error_message()
        )));
    }

    let mut state = runtime.clib_state.borrow_mut();
    let id = state.alloc_id("lib");
    state.libraries.insert(
        id.clone(),
        ClibLibraryState {
            handle,
            closed: false,
        },
    );
    Ok(clib_object("CLibrary", &id))
}

fn library_func(runtime: &Runtime, builtin_value: &Value, args: &[Value]) -> Result<Value> {
    if args.len() < 3 || args.len() > 4 {
        return Err(ZuzuRustError::runtime(
            "CLibrary.func() expects name, params, return type, and optional options",
        ));
    }
    let library_id = object_id(builtin_value)?;
    let symbol_name = runtime.render_value(&args[0])?;
    if symbol_name.is_empty() {
        return Err(ZuzuRustError::runtime("C function name must not be empty"));
    }
    let params = normalize_params(&args[1])?;
    let return_type = normalize_descriptor(&args[2], "return")?;

    let mut state = runtime.clib_state.borrow_mut();
    let library = state.open_library(&library_id)?;
    let symbol = lookup_symbol(library.handle, &symbol_name)?;
    let free_symbol = if let Some(free_name) = &return_type.free {
        Some(lookup_symbol(library.handle, free_name)?)
    } else {
        None
    };
    let function_id = state.alloc_id("fn");
    state.functions.insert(
        function_id.clone(),
        ClibFunctionState {
            library_id,
            name: symbol_name,
            symbol,
            params,
            return_type,
            free_symbol,
        },
    );
    Ok(clib_object("CFunction", &function_id))
}

fn library_has_symbol(runtime: &Runtime, builtin_value: &Value, args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(ZuzuRustError::runtime(
            "CLibrary.has_symbol() expects a symbol name",
        ));
    }
    let library_id = object_id(builtin_value)?;
    let symbol_name = runtime.render_value(&args[0])?;
    let mut state = runtime.clib_state.borrow_mut();
    let library = state.open_library(&library_id)?;
    Ok(Value::Boolean(
        find_symbol(library.handle, &symbol_name).is_some(),
    ))
}

fn library_close(runtime: &Runtime, builtin_value: &Value, args: &[Value]) -> Result<Value> {
    if !args.is_empty() {
        return Err(ZuzuRustError::runtime(
            "CLibrary.close() expects no arguments",
        ));
    }
    let library_id = object_id(builtin_value)?;
    let mut state = runtime.clib_state.borrow_mut();
    if let Some(library) = state.libraries.get_mut(&library_id) {
        library.closed = true;
    }
    Ok(Value::Null)
}

fn function_call(runtime: &Runtime, builtin_value: &Value, args: &[Value]) -> Result<Value> {
    let function_id = object_id(builtin_value)?;
    let state = runtime.clib_state.borrow();
    let function = state
        .functions
        .get(&function_id)
        .ok_or_else(|| ZuzuRustError::runtime("CFunction is invalid"))?;
    let library = state
        .libraries
        .get(&function.library_id)
        .ok_or_else(|| ZuzuRustError::runtime("CFunction belongs to a closed CLibrary"))?;
    if library.closed {
        return Err(ZuzuRustError::runtime(
            "CFunction belongs to a closed CLibrary",
        ));
    }
    if args.len() != function.params.len() {
        return Err(ZuzuRustError::runtime(format!(
            "Function '{}' expects {} arguments, got {}",
            function.name,
            function.params.len(),
            args.len()
        )));
    }

    let mut temps = Vec::<Vec<u8>>::new();
    let prepared = function
        .params
        .iter()
        .zip(args.iter())
        .enumerate()
        .map(|(index, (desc, value))| prepare_arg(desc, value, index, &mut temps))
        .collect::<Result<Vec<_>>>()?;
    unsafe { dispatch_call(function, &prepared, args) }
}

unsafe fn dispatch_call(
    function: &ClibFunctionState,
    args: &[FfiArg],
    original_args: &[Value],
) -> Result<Value> {
    let mut param_types = function
        .params
        .iter()
        .map(|desc| ffi_type_code(desc, false))
        .collect::<Result<Vec<_>>>()?;
    let return_type = ffi_type_code(&function.return_type, true)?;
    let mut call_args = args.to_vec();
    let mut result = FfiResult {
        type_code: return_type,
        value: FfiValue { uint64_value: 0 },
    };

    let err = zuzu_ffi_call(
        function.symbol,
        return_type,
        param_types.as_mut_ptr(),
        call_args.as_mut_ptr(),
        call_args.len(),
        &mut result,
    );
    if !err.is_null() {
        return Err(ZuzuRustError::runtime(format!(
            "C function '{}' failed: {}",
            function.name,
            CStr::from_ptr(err).to_string_lossy()
        )));
    }

    finish_return(function, result, original_args)
}

unsafe fn finish_return(
    function: &ClibFunctionState,
    result: FfiResult,
    original_args: &[Value],
) -> Result<Value> {
    match function.return_type.kind {
        ClibKind::Null => Ok(Value::Null),
        ClibKind::Bool => Ok(Value::Boolean(result.value.bool_value != 0)),
        ClibKind::Int if function.return_type.signed => {
            Ok(Value::Number(result.value.sint64_value as f64))
        }
        ClibKind::Int => Ok(Value::Number(result.value.uint64_value as f64)),
        ClibKind::Float => Ok(Value::Number(result.value.double_value)),
        ClibKind::Binary => finish_binary_return(
            function,
            result.value.pointer_value as *mut c_char,
            original_args,
        ),
    }
}

unsafe fn finish_binary_return(
    function: &ClibFunctionState,
    ptr: *mut c_char,
    original_args: &[Value],
) -> Result<Value> {
    if ptr.is_null() {
        return Ok(Value::Null);
    }
    let bytes = if let Some(length) = return_length(&function.return_type, original_args)? {
        std::slice::from_raw_parts(ptr as *const u8, length).to_vec()
    } else if function.return_type.terminated_by_nul {
        CStr::from_ptr(ptr).to_bytes().to_vec()
    } else {
        return Err(ZuzuRustError::runtime(
            "binary return requires length, length_arg, or terminated_by",
        ));
    };
    if let Some(free_symbol) = function.free_symbol {
        let free_fn: extern "C" fn(*mut c_void) = std::mem::transmute(free_symbol);
        free_fn(ptr as *mut c_void);
    }
    Ok(Value::BinaryString(bytes))
}

fn prepare_arg(
    desc: &ClibDescriptor,
    value: &Value,
    index: usize,
    temps: &mut Vec<Vec<u8>>,
) -> Result<FfiArg> {
    if let Value::Shared(value) = value {
        return prepare_arg(desc, &value.borrow(), index, temps);
    }
    match desc.kind {
        ClibKind::Null => match value {
            Value::Null => Ok(pointer_arg(std::ptr::null_mut())),
            other => Err(type_error(index, "Null", other)),
        },
        ClibKind::Bool => match value {
            Value::Boolean(value) => Ok(FfiArg {
                type_code: ZUZU_FFI_BOOL,
                value: FfiValue {
                    bool_value: u8::from(*value),
                },
            }),
            other => Err(type_error(index, "Boolean", other)),
        },
        ClibKind::Int => match value {
            Value::Number(value) if desc.signed => Ok(FfiArg {
                type_code: ZUZU_FFI_SINT64,
                value: FfiValue {
                    sint64_value: *value as i64,
                },
            }),
            Value::Number(value) if *value >= 0.0 => Ok(FfiArg {
                type_code: ZUZU_FFI_UINT64,
                value: FfiValue {
                    uint64_value: *value as u64,
                },
            }),
            Value::Number(_) => Err(ZuzuRustError::runtime(format!(
                "argument {index} must be non-negative for unsigned int"
            ))),
            other => Err(type_error(index, "Number", other)),
        },
        ClibKind::Float => match value {
            Value::Number(value) => Ok(FfiArg {
                type_code: ZUZU_FFI_DOUBLE,
                value: FfiValue {
                    double_value: *value,
                },
            }),
            other => Err(type_error(index, "Number", other)),
        },
        ClibKind::Binary => match value {
            Value::Null if desc.nullable => Ok(pointer_arg(std::ptr::null_mut())),
            Value::BinaryString(bytes) => {
                let mut owned = bytes.clone();
                if desc.terminated_by_nul {
                    owned.push(0);
                }
                let ptr = owned.as_ptr() as *mut c_void;
                temps.push(owned);
                Ok(pointer_arg(ptr))
            }
            other => Err(type_error(index, "BinaryString", other)),
        },
    }
}

fn pointer_arg(ptr: *mut c_void) -> FfiArg {
    FfiArg {
        type_code: ZUZU_FFI_POINTER,
        value: FfiValue { pointer_value: ptr },
    }
}

fn ffi_type_code(desc: &ClibDescriptor, is_return: bool) -> Result<c_int> {
    match desc.kind {
        ClibKind::Null if is_return => Ok(ZUZU_FFI_VOID),
        ClibKind::Null => Ok(ZUZU_FFI_POINTER),
        ClibKind::Bool => Ok(ZUZU_FFI_BOOL),
        ClibKind::Int if desc.signed => Ok(ZUZU_FFI_SINT64),
        ClibKind::Int => Ok(ZUZU_FFI_UINT64),
        ClibKind::Float => Ok(ZUZU_FFI_DOUBLE),
        ClibKind::Binary => Ok(ZUZU_FFI_POINTER),
    }
}

fn type_error(index: usize, expected: &str, value: &Value) -> ZuzuRustError {
    ZuzuRustError::runtime(format!(
        "argument {index} must be {expected}, got {}",
        value.type_name()
    ))
}

fn normalize_params(value: &Value) -> Result<Vec<ClibDescriptor>> {
    if let Value::Shared(value) = value {
        return normalize_params(&value.borrow());
    }
    let Value::Array(items) = value else {
        return Err(ZuzuRustError::runtime("params descriptor must be Array"));
    };
    items
        .iter()
        .enumerate()
        .map(|(index, item)| normalize_descriptor(item, &format!("parameter {index}")))
        .collect()
}

fn normalize_descriptor(value: &Value, context: &str) -> Result<ClibDescriptor> {
    if let Value::Shared(value) = value {
        return normalize_descriptor(&value.borrow(), context);
    }
    let mut map = match value {
        Value::String(kind) => HashMap::from([("type".to_owned(), Value::String(kind.clone()))]),
        Value::Dict(map) => map.clone(),
        other => {
            return Err(ZuzuRustError::runtime(format!(
                "{context} descriptor must be String or Dict, got {}",
                other.type_name()
            )))
        }
    };
    let kind_name = map
        .remove("type")
        .map(|value| value.render())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let kind = match kind_name.as_str() {
        "null" => ClibKind::Null,
        "bool" => ClibKind::Bool,
        "int" => {
            let bits = map_number(&map, "bits").unwrap_or(64.0) as i64;
            if bits != 64 {
                return Err(ZuzuRustError::runtime(format!(
                    "{context} int descriptor only supports bits=64"
                )));
            }
            ClibKind::Int
        }
        "float" => {
            let bits = map_number(&map, "bits").unwrap_or(64.0) as i64;
            if bits != 64 {
                return Err(ZuzuRustError::runtime(format!(
                    "{context} float descriptor only supports bits=64"
                )));
            }
            ClibKind::Float
        }
        "binary" => ClibKind::Binary,
        _ => {
            return Err(ZuzuRustError::runtime(format!(
                "{context} descriptor has unsupported type '{kind_name}'"
            )))
        }
    };
    Ok(ClibDescriptor {
        kind,
        signed: map_bool(&map, "signed").unwrap_or(true),
        terminated_by_nul: matches!(
            map.get("terminated_by"),
            Some(Value::String(value)) if value == "nul"
        ),
        nullable: map_bool(&map, "nullable").unwrap_or(false),
        length: map_number(&map, "length").map(|value| value as usize),
        length_arg: map_number(&map, "length_arg").map(|value| value as usize),
        free: map.get("free").map(Value::render),
    })
}

fn return_length(desc: &ClibDescriptor, original_args: &[Value]) -> Result<Option<usize>> {
    if let Some(length) = desc.length {
        return Ok(Some(length));
    }
    let Some(index) = desc.length_arg else {
        return Ok(None);
    };
    match original_args.get(index) {
        Some(Value::Number(value)) if *value >= 0.0 => Ok(Some(*value as usize)),
        _ => Err(ZuzuRustError::runtime(
            "binary return length_arg value must be non-negative",
        )),
    }
}

fn map_number(map: &HashMap<String, Value>, key: &str) -> Option<f64> {
    match map.get(key) {
        Some(Value::Shared(value)) => map_number(
            &HashMap::from([(key.to_owned(), value.borrow().clone())]),
            key,
        ),
        Some(Value::Number(value)) => Some(*value),
        Some(Value::Boolean(value)) => Some(if *value { 1.0 } else { 0.0 }),
        Some(Value::String(value)) => value.parse::<f64>().ok(),
        _ => None,
    }
}

fn map_bool(map: &HashMap<String, Value>, key: &str) -> Option<bool> {
    match map.get(key) {
        Some(Value::Shared(value)) => map_bool(
            &HashMap::from([(key.to_owned(), value.borrow().clone())]),
            key,
        ),
        Some(Value::Boolean(value)) => Some(*value),
        Some(Value::Number(value)) => Some(*value != 0.0),
        Some(Value::String(value)) => Some(!value.is_empty()),
        _ => None,
    }
}

fn clib_object(class_name: &str, id: &str) -> Value {
    Value::Object(Rc::new(RefCell::new(ObjectValue {
        class: Rc::new(UserClassValue {
            name: class_name.to_owned(),
            base: None,
            traits: Vec::<Rc<TraitValue>>::new(),
            fields: Vec::new(),
            methods: HashMap::<String, Rc<MethodValue>>::new(),
            static_methods: HashMap::<String, Rc<MethodValue>>::new(),
            nested_classes: HashMap::new(),
            source_decl: None,
            closure_env: None,
        }),
        fields: HashMap::from([("id".to_owned(), Value::String(id.to_owned()))]),
        weak_fields: std::collections::HashSet::new(),
        builtin_value: Some(Value::Dict(HashMap::from([(
            "id".to_owned(),
            Value::String(id.to_owned()),
        )]))),
    })))
}

fn object_id(value: &Value) -> Result<String> {
    if let Value::Shared(value) = value {
        return object_id(&value.borrow());
    }
    match value {
        Value::Dict(fields) => match fields.get("id") {
            Some(Value::String(id)) => Ok(id.clone()),
            _ => Err(ZuzuRustError::runtime("CLib object is missing id")),
        },
        _ => Err(ZuzuRustError::runtime("Expected CLib object")),
    }
}

impl ClibState {
    fn alloc_id(&mut self, prefix: &str) -> String {
        self.next_id = self.next_id.saturating_add(1);
        format!("{prefix}-{}", self.next_id)
    }

    fn open_library(&mut self, id: &str) -> Result<&mut ClibLibraryState> {
        let library = self
            .libraries
            .get_mut(id)
            .ok_or_else(|| ZuzuRustError::runtime("CLibrary is invalid"))?;
        if library.closed {
            return Err(ZuzuRustError::runtime("CLibrary is closed"));
        }
        Ok(library)
    }
}

fn lookup_symbol(handle: *mut c_void, name: &str) -> Result<*mut c_void> {
    find_symbol(handle, name).ok_or_else(|| {
        ZuzuRustError::runtime(format!(
            "Could not bind C symbol '{}': {}",
            name,
            dl_error_message()
        ))
    })
}

fn find_symbol(handle: *mut c_void, name: &str) -> Option<*mut c_void> {
    let c_name = CString::new(name).ok()?;
    #[cfg(unix)]
    let ptr = unsafe { dlsym(handle, c_name.as_ptr()) };
    #[cfg(not(unix))]
    let ptr: *mut c_void = std::ptr::null_mut();
    (!ptr.is_null()).then_some(ptr)
}

fn dl_error_message() -> String {
    #[cfg(unix)]
    unsafe {
        let err = dlerror();
        if !err.is_null() {
            return CStr::from_ptr(err).to_string_lossy().to_string();
        }
    }
    "unknown dynamic loader error".to_owned()
}
