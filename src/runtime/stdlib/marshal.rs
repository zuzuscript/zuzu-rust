use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::rc::Rc;

use crate::ast::{
    BlockStatement, CallArgument, ClassDeclaration, ClassMember, DictKey, Expression,
    FieldDeclaration, Parameter, Statement, TraitDeclaration,
};
use crate::codegen;

use super::super::{
    Environment, FunctionBody, FunctionValue, MethodValue, ObjectValue, Runtime, TraitValue,
    UserClassValue, Value,
};
use super::{io, time};
use crate::error::{Result, ZuzuRustError};

mod cbor;

use cbor::{decode_one, encode_one, CborValue};

const KIND_PAIR: i128 = 1;
const KIND_ARRAY: i128 = 2;
const KIND_DICT: i128 = 3;
const KIND_PAIRLIST: i128 = 4;
const KIND_SET: i128 = 5;
const KIND_BAG: i128 = 6;
const KIND_OBJECT: i128 = 7;
const KIND_FUNCTION: i128 = 8;
const KIND_CLASS: i128 = 9;
const KIND_TRAIT: i128 = 10;
const KIND_BOUND_METHOD: i128 = 11;
const KIND_TIME: i128 = 12;
const KIND_PATH: i128 = 13;

const CODE_FUNCTION: i128 = 1;
const CODE_CLASS: i128 = 2;
const CODE_TRAIT: i128 = 3;

const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

pub(super) fn exports() -> HashMap<String, Value> {
    let mut exports = HashMap::new();
    for func in ["dump", "load", "safe_to_dump"] {
        exports.insert(func.to_owned(), Value::native_function(func.to_owned()));
    }
    for class_name in ["MarshallingException", "UnmarshallingException"] {
        exports.insert(
            class_name.to_owned(),
            Value::builtin_class(class_name.to_owned()),
        );
    }
    exports
}

pub(super) fn call(runtime: &Runtime, name: &str, args: &[Value]) -> Option<Result<Value>> {
    let value = match name {
        "dump" => dump(runtime, args),
        "load" => load(runtime, args),
        "safe_to_dump" => safe_to_dump(runtime, args),
        _ => return None,
    };
    Some(value)
}

fn dump(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    expect_arity("dump", args, 1)?;
    Ok(Value::BinaryString(dump_value(runtime, &args[0])?))
}

pub(in crate::runtime) fn dump_value(runtime: &Runtime, value: &Value) -> Result<Vec<u8>> {
    let envelope = encode_envelope(runtime, value).map_err(|err| {
        ZuzuRustError::thrown(format!(
            "MarshallingException: std/marshal.dump failed: {}",
            exception_text(err)
        ))
    })?;
    let bytes = encode_one(&envelope).map_err(|err| {
        ZuzuRustError::thrown(format!(
            "MarshallingException: std/marshal.dump failed: {}",
            exception_text(err)
        ))
    })?;
    Ok(bytes)
}

fn load(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    expect_arity("load", args, 1)?;
    let Value::BinaryString(bytes) = &args[0] else {
        return Err(ZuzuRustError::thrown(format!(
            "TypeException: load expects BinaryString, got {}",
            args[0].type_name()
        )));
    };
    load_value(runtime, bytes)
}

pub(in crate::runtime) fn load_value(runtime: &Runtime, bytes: &[u8]) -> Result<Value> {
    load_binary(runtime, bytes).map_err(|err| {
        ZuzuRustError::thrown(format!(
            "UnmarshallingException: std/marshal.load failed: {}",
            exception_text(err)
        ))
    })
}

fn safe_to_dump(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    expect_arity("safe_to_dump", args, 1)?;
    Ok(Value::Boolean(dump(runtime, args).is_ok()))
}

struct DumpState {
    objects: Vec<Option<CborValue>>,
    strong_ids: HashMap<usize, usize>,
    code: Vec<Option<CborValue>>,
    code_ids: HashMap<usize, usize>,
    on_dump: HashSet<usize>,
}

impl DumpState {
    fn new() -> Self {
        Self {
            objects: Vec::new(),
            strong_ids: HashMap::new(),
            code: Vec::new(),
            code_ids: HashMap::new(),
            on_dump: HashSet::new(),
        }
    }

    fn reserve_anonymous(&mut self) -> usize {
        let id = self.objects.len();
        self.objects.push(None);
        id
    }

    fn reserve_pointer(&mut self, pointer: usize) -> (usize, bool) {
        if let Some(id) = self.strong_ids.get(&pointer) {
            return (*id, false);
        }
        let id = self.reserve_anonymous();
        self.strong_ids.insert(pointer, id);
        (id, true)
    }

    fn set_object(&mut self, id: usize, kind: i128, payload: CborValue) {
        self.objects[id] = Some(CborValue::Array(vec![CborValue::Integer(kind), payload]));
    }

    fn finish(self) -> Result<(Vec<CborValue>, Vec<CborValue>)> {
        let objects = self
            .objects
            .into_iter()
            .enumerate()
            .map(|(id, value)| {
                value.ok_or_else(|| ZuzuRustError::runtime(format!("object table slot {id} empty")))
            })
            .collect::<Result<Vec<_>>>()?;
        let code = self
            .code
            .into_iter()
            .enumerate()
            .map(|(id, value)| {
                value.ok_or_else(|| ZuzuRustError::runtime(format!("code table slot {id} empty")))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((objects, code))
    }

    fn reserve_code_pointer(&mut self, pointer: usize) -> (usize, bool) {
        if let Some(id) = self.code_ids.get(&pointer) {
            return (*id, false);
        }
        let id = self.code.len();
        self.code.push(None);
        self.code_ids.insert(pointer, id);
        (id, true)
    }

    fn set_code(&mut self, id: usize, record: CborValue) {
        self.code[id] = Some(record);
    }
}

fn encode_envelope(runtime: &Runtime, value: &Value) -> Result<CborValue> {
    let mut state = DumpState::new();
    let root = encode_value(runtime, value, &mut state)?;
    let (objects, code) = state.finish()?;
    Ok(CborValue::Tag {
        tag: 55_799,
        value: Box::new(CborValue::Array(vec![
            CborValue::Text("ZUZU-MARSHAL".to_owned()),
            CborValue::Integer(1),
            CborValue::Map(Vec::new()),
            root,
            CborValue::Array(objects),
            CborValue::Array(code),
        ])),
    })
}

fn encode_value(runtime: &Runtime, value: &Value, state: &mut DumpState) -> Result<CborValue> {
    if value.is_weak_value() {
        return encode_weak_storage_value(runtime, value, state);
    }
    match value {
        Value::Null => Ok(CborValue::Null),
        Value::Boolean(value) => Ok(CborValue::Bool(*value)),
        Value::Number(value) => number_to_cbor(*value),
        Value::String(value) => Ok(CborValue::Text(value.clone())),
        Value::BinaryString(bytes) => Ok(CborValue::Bytes(bytes.clone())),
        Value::Shared(value) => encode_shared(runtime, value, state),
        Value::Array(values) => encode_anonymous_collection(runtime, KIND_ARRAY, values, state),
        Value::Set(values) => encode_anonymous_collection(runtime, KIND_SET, values, state),
        Value::Bag(values) => encode_anonymous_collection(runtime, KIND_BAG, values, state),
        Value::Dict(values) => encode_anonymous_dict(runtime, values, state),
        Value::PairList(values) => encode_anonymous_pairlist(runtime, values, state),
        Value::Pair(key, value) => encode_anonymous_pair(runtime, key, value, state),
        Value::Function(function) => encode_function_object(runtime, function, state),
        Value::UserClass(class) => encode_user_class(runtime, class, state),
        Value::Trait(trait_value) => encode_trait_object(runtime, trait_value, state),
        Value::Method(method) => encode_bound_method(runtime, method, state),
        Value::Object(object) => encode_runtime_object(runtime, object, state),
        other => Err(ZuzuRustError::runtime(format!(
            "Value of type {} is not marshalable in this phase",
            other.type_name()
        ))),
    }
}

fn encode_stored_value(
    runtime: &Runtime,
    value: &Value,
    state: &mut DumpState,
    is_weak_storage: bool,
) -> Result<CborValue> {
    if is_weak_storage || value.is_weak_value() {
        encode_weak_storage_value(runtime, value, state)
    } else {
        encode_value(runtime, value, state)
    }
}

fn encode_weak_storage_value(
    runtime: &Runtime,
    value: &Value,
    state: &mut DumpState,
) -> Result<CborValue> {
    let payload = if value.is_weak_value() {
        let resolved = value.resolve_weak_value();
        if matches!(resolved, Value::Null) {
            CborValue::Null
        } else if let Some(id) = strong_ref_id_for_value(&resolved, state) {
            strong_ref(id)
        } else if resolved.scalar_type_name().is_some() {
            encode_value(runtime, &resolved, state)?
        } else {
            CborValue::Null
        }
    } else if value.scalar_type_name().is_some() || matches!(value, Value::Null) {
        encode_value(runtime, value, state)?
    } else if let Some(id) = strong_ref_id_for_value(value, state) {
        strong_ref(id)
    } else {
        CborValue::Null
    };
    Ok(CborValue::Array(vec![CborValue::Integer(1), payload]))
}

fn strong_ref_id_for_value(value: &Value, state: &DumpState) -> Option<usize> {
    match value {
        Value::Shared(value) => state.strong_ids.get(&(Rc::as_ptr(value) as usize)).copied(),
        Value::Function(value) => state.strong_ids.get(&(Rc::as_ptr(value) as usize)).copied(),
        Value::UserClass(value) => state.strong_ids.get(&(Rc::as_ptr(value) as usize)).copied(),
        Value::Trait(value) => state.strong_ids.get(&(Rc::as_ptr(value) as usize)).copied(),
        Value::Method(value) => state.strong_ids.get(&(Rc::as_ptr(value) as usize)).copied(),
        Value::Object(value) => state.strong_ids.get(&(Rc::as_ptr(value) as usize)).copied(),
        _ => None,
    }
}

fn encode_shared(
    runtime: &Runtime,
    value: &Rc<RefCell<Value>>,
    state: &mut DumpState,
) -> Result<CborValue> {
    let borrowed = value.borrow();
    if !is_object_table_value(&borrowed) {
        return encode_value(runtime, &borrowed, state);
    }
    let pointer = Rc::as_ptr(value) as usize;
    let (id, is_new) = state.reserve_pointer(pointer);
    if !is_new {
        return Ok(strong_ref(id));
    }
    match &*borrowed {
        Value::Array(values) => {
            let payload = encode_item_payload(runtime, values, state)?;
            state.set_object(id, KIND_ARRAY, payload);
        }
        Value::Set(values) => {
            let payload = encode_item_payload(runtime, values, state)?;
            state.set_object(id, KIND_SET, payload);
        }
        Value::Bag(values) => {
            let payload = encode_item_payload(runtime, values, state)?;
            state.set_object(id, KIND_BAG, payload);
        }
        Value::Dict(values) => {
            let payload = encode_dict_payload(runtime, values, state)?;
            state.set_object(id, KIND_DICT, payload);
        }
        Value::PairList(values) => {
            let payload = encode_pairlist_payload(runtime, values, state)?;
            state.set_object(id, KIND_PAIRLIST, payload);
        }
        Value::Pair(key, value) => {
            let payload = encode_pair_payload(runtime, key, value, state)?;
            state.set_object(id, KIND_PAIR, payload);
        }
        Value::Function(function) => {
            let code_id = encode_function_code(runtime, function, state, None)?;
            state.set_object(
                id,
                KIND_FUNCTION,
                CborValue::Array(vec![CborValue::Integer(code_id as i128)]),
            );
        }
        Value::UserClass(class) => {
            let code_id = encode_class_code(runtime, class, state, None)?;
            state.set_object(
                id,
                KIND_CLASS,
                CborValue::Array(vec![CborValue::Integer(code_id as i128)]),
            );
        }
        Value::Trait(trait_value) => {
            let code_id = encode_trait_code(runtime, trait_value, state, None)?;
            state.set_object(
                id,
                KIND_TRAIT,
                CborValue::Array(vec![CborValue::Integer(code_id as i128)]),
            );
        }
        Value::Method(method) => {
            let payload = encode_bound_method_payload(runtime, method, state)?;
            state.set_object(id, KIND_BOUND_METHOD, payload);
        }
        _ => unreachable!(),
    }
    Ok(strong_ref(id))
}

fn encode_runtime_object(
    runtime: &Runtime,
    object: &Rc<RefCell<ObjectValue>>,
    state: &mut DumpState,
) -> Result<CborValue> {
    let class_name = object.borrow().class.name.clone();
    if class_name == "Time" {
        return encode_time_object(object, state);
    }
    if class_name == "Path" {
        return encode_path_object(object, state);
    }
    encode_user_object(runtime, object, state)
}

fn encode_time_object(
    object: &Rc<RefCell<ObjectValue>>,
    state: &mut DumpState,
) -> Result<CborValue> {
    let pointer = Rc::as_ptr(object) as usize;
    let (id, is_new) = state.reserve_pointer(pointer);
    if !is_new {
        return Ok(strong_ref(id));
    }
    let epoch = match &object.borrow().builtin_value {
        Some(Value::Number(epoch)) => *epoch,
        _ => {
            return Err(ZuzuRustError::runtime(
                "Time value has invalid internal epoch",
            ))
        }
    };
    state.set_object(
        id,
        KIND_TIME,
        CborValue::Array(vec![number_to_cbor(epoch)?]),
    );
    Ok(strong_ref(id))
}

fn encode_path_object(
    object: &Rc<RefCell<ObjectValue>>,
    state: &mut DumpState,
) -> Result<CborValue> {
    let pointer = Rc::as_ptr(object) as usize;
    let (id, is_new) = state.reserve_pointer(pointer);
    if !is_new {
        return Ok(strong_ref(id));
    }
    let path = match &object.borrow().builtin_value {
        Some(Value::String(path)) => path.clone(),
        _ => {
            return Err(ZuzuRustError::runtime(
                "Path value has invalid internal path",
            ))
        }
    };
    state.set_object(id, KIND_PATH, CborValue::Array(vec![CborValue::Text(path)]));
    Ok(strong_ref(id))
}

fn encode_user_object(
    runtime: &Runtime,
    object: &Rc<RefCell<ObjectValue>>,
    state: &mut DumpState,
) -> Result<CborValue> {
    let pointer = Rc::as_ptr(object) as usize;
    if let Some(id) = state.strong_ids.get(&pointer) {
        return Ok(strong_ref(*id));
    }
    if state.on_dump.insert(pointer) {
        runtime
            .marshal_call_object_hook(object, "__on_dump__")
            .map_err(|err| {
                ZuzuRustError::runtime(format!("__on_dump__ hook failed: {}", exception_text(err)))
            })?;
    }
    let id = state.reserve_anonymous();
    state.strong_ids.insert(pointer, id);
    let class = object.borrow().class.clone();
    let mut keys = object.borrow().fields.keys().cloned().collect::<Vec<_>>();
    keys.sort();
    let slots = keys
        .into_iter()
        .map(|key| {
            let value = object
                .borrow()
                .fields
                .get(&key)
                .cloned()
                .unwrap_or(Value::Null);
            Ok(CborValue::Array(vec![
                CborValue::Text(key.clone()),
                encode_stored_value(
                    runtime,
                    &value,
                    state,
                    object.borrow().weak_fields.contains(&key),
                )?,
            ]))
        })
        .collect::<Result<Vec<_>>>()?;
    let payload = CborValue::Array(vec![
        encode_user_class(runtime, &class, state)?,
        CborValue::Array(slots),
    ]);
    state.set_object(id, KIND_OBJECT, payload);
    Ok(strong_ref(id))
}

fn encode_function_object(
    runtime: &Runtime,
    function: &Rc<FunctionValue>,
    state: &mut DumpState,
) -> Result<CborValue> {
    let pointer = Rc::as_ptr(function) as usize;
    let (id, is_new) = state.reserve_pointer(pointer);
    if !is_new {
        return Ok(strong_ref(id));
    }
    let code_id = encode_function_code(runtime, function, state, None)?;
    state.set_object(
        id,
        KIND_FUNCTION,
        CborValue::Array(vec![CborValue::Integer(code_id as i128)]),
    );
    Ok(strong_ref(id))
}

fn encode_user_class(
    runtime: &Runtime,
    class: &Rc<UserClassValue>,
    state: &mut DumpState,
) -> Result<CborValue> {
    let pointer = Rc::as_ptr(class) as usize;
    let (id, is_new) = state.reserve_pointer(pointer);
    if !is_new {
        return Ok(strong_ref(id));
    }
    let code_id = encode_class_code(runtime, class, state, None)?;
    state.set_object(
        id,
        KIND_CLASS,
        CborValue::Array(vec![CborValue::Integer(code_id as i128)]),
    );
    Ok(strong_ref(id))
}

fn encode_trait_object(
    runtime: &Runtime,
    trait_value: &Rc<TraitValue>,
    state: &mut DumpState,
) -> Result<CborValue> {
    let pointer = Rc::as_ptr(trait_value) as usize;
    let (id, is_new) = state.reserve_pointer(pointer);
    if !is_new {
        return Ok(strong_ref(id));
    }
    let code_id = encode_trait_code(runtime, trait_value, state, None)?;
    state.set_object(
        id,
        KIND_TRAIT,
        CborValue::Array(vec![CborValue::Integer(code_id as i128)]),
    );
    Ok(strong_ref(id))
}

fn encode_bound_method(
    runtime: &Runtime,
    method: &Rc<MethodValue>,
    state: &mut DumpState,
) -> Result<CborValue> {
    let pointer = Rc::as_ptr(method) as usize;
    let (id, is_new) = state.reserve_pointer(pointer);
    if !is_new {
        return Ok(strong_ref(id));
    }
    let payload = encode_bound_method_payload(runtime, method, state)?;
    state.set_object(id, KIND_BOUND_METHOD, payload);
    Ok(strong_ref(id))
}

fn encode_bound_method_payload(
    runtime: &Runtime,
    method: &Rc<MethodValue>,
    state: &mut DumpState,
) -> Result<CborValue> {
    let receiver = method.bound_receiver.as_ref().ok_or_else(|| {
        ZuzuRustError::runtime("Unbound method values are not marshalable in this phase")
    })?;
    let method_name = method.bound_name.as_deref().unwrap_or(&method.name);
    Ok(CborValue::Array(vec![
        encode_value(runtime, receiver, state)?,
        CborValue::Text(method_name.to_owned()),
    ]))
}

fn encode_function_code(
    runtime: &Runtime,
    function: &Rc<FunctionValue>,
    state: &mut DumpState,
    preferred_name: Option<&str>,
) -> Result<usize> {
    let pointer = Rc::as_ptr(function) as usize;
    let (id, is_new) = state.reserve_code_pointer(pointer);
    if !is_new {
        return Ok(id);
    }
    let binding_name = function_code_binding_name(function, id, preferred_name);
    let source = render_function_value(function);
    let analysis = analyse_code(
        runtime,
        &function.env,
        function_initial_bindings(function),
        collect_function_free_names(function)?,
    )?;
    let (captures, dependencies) = encode_code_analysis(runtime, &analysis, state)?;
    state.set_code(
        id,
        CborValue::Array(vec![
            CborValue::Integer(CODE_FUNCTION),
            CborValue::Text(binding_name),
            CborValue::Text(source),
            CborValue::Array(captures),
            CborValue::Array(dependencies),
        ]),
    );
    Ok(id)
}

fn encode_class_code(
    runtime: &Runtime,
    class: &Rc<UserClassValue>,
    state: &mut DumpState,
    preferred_name: Option<&str>,
) -> Result<usize> {
    let pointer = Rc::as_ptr(class) as usize;
    let (id, is_new) = state.reserve_code_pointer(pointer);
    if !is_new {
        return Ok(id);
    }
    let source_decl = class.source_decl.as_ref().ok_or_else(|| {
        ZuzuRustError::runtime(format!(
            "Class value {} does not have ZuzuScript source",
            class.name
        ))
    })?;
    let env = class.closure_env.as_ref().ok_or_else(|| {
        ZuzuRustError::runtime(format!(
            "Class value {} does not have a marshalable lexical environment",
            class.name
        ))
    })?;
    let analysis = analyse_code(
        runtime,
        env,
        class_initial_bindings(source_decl),
        collect_class_free_names(source_decl)?,
    )?;
    let (captures, dependencies) = encode_code_analysis(runtime, &analysis, state)?;
    state.set_code(
        id,
        CborValue::Array(vec![
            CborValue::Integer(CODE_CLASS),
            CborValue::Text(class_code_binding_name(class, id, preferred_name)),
            CborValue::Text(render_class_declaration(source_decl)),
            CborValue::Array(captures),
            CborValue::Array(dependencies),
        ]),
    );
    Ok(id)
}

fn encode_trait_code(
    runtime: &Runtime,
    trait_value: &Rc<TraitValue>,
    state: &mut DumpState,
    preferred_name: Option<&str>,
) -> Result<usize> {
    let pointer = Rc::as_ptr(trait_value) as usize;
    let (id, is_new) = state.reserve_code_pointer(pointer);
    if !is_new {
        return Ok(id);
    }
    let source_decl = trait_value.source_decl.as_ref().ok_or_else(|| {
        ZuzuRustError::runtime(format!(
            "Trait value {} does not have ZuzuScript source",
            trait_value.name
        ))
    })?;
    let env = trait_value.closure_env.as_ref().ok_or_else(|| {
        ZuzuRustError::runtime(format!(
            "Trait value {} does not have a marshalable lexical environment",
            trait_value.name
        ))
    })?;
    let analysis = analyse_code(
        runtime,
        env,
        trait_initial_bindings(source_decl),
        collect_trait_free_names(source_decl)?,
    )?;
    let (captures, dependencies) = encode_code_analysis(runtime, &analysis, state)?;
    state.set_code(
        id,
        CborValue::Array(vec![
            CborValue::Integer(CODE_TRAIT),
            CborValue::Text(trait_code_binding_name(trait_value, id, preferred_name)),
            CborValue::Text(render_trait_declaration(source_decl)),
            CborValue::Array(captures),
            CborValue::Array(dependencies),
        ]),
    );
    Ok(id)
}

fn function_code_binding_name(
    function: &FunctionValue,
    id: usize,
    preferred_name: Option<&str>,
) -> String {
    if let Some(name) = preferred_name {
        if is_identifier(name) {
            return name.to_owned();
        }
    }
    if let Some(name) = &function.name {
        if is_identifier(name) {
            return name.clone();
        }
    }
    format!("__zuzu_marshal_fn_{id}")
}

fn class_code_binding_name(
    class: &UserClassValue,
    id: usize,
    preferred_name: Option<&str>,
) -> String {
    if let Some(name) = preferred_name {
        if is_identifier(name) {
            return name.to_owned();
        }
    }
    if is_identifier(&class.name) {
        return class.name.clone();
    }
    format!("__zuzu_marshal_class_{id}")
}

fn trait_code_binding_name(
    trait_value: &TraitValue,
    id: usize,
    preferred_name: Option<&str>,
) -> String {
    if let Some(name) = preferred_name {
        if is_identifier(name) {
            return name.to_owned();
        }
    }
    if is_identifier(&trait_value.name) {
        return trait_value.name.clone();
    }
    format!("__zuzu_marshal_trait_{id}")
}

struct CodeAnalysis {
    captures: Vec<(String, Value)>,
    dependencies: Vec<(String, Value)>,
}

fn analyse_code(
    runtime: &Runtime,
    env: &Rc<Environment>,
    bound: HashSet<String>,
    free_names: HashSet<String>,
) -> Result<CodeAnalysis> {
    let mut captures = Vec::new();
    let mut dependencies = Vec::new();
    let mut names = free_names.into_iter().collect::<Vec<_>>();
    names.sort();
    for name in names {
        if bound.contains(&name) {
            continue;
        }
        if name == "__global__" || name == "__system__" {
            continue;
        }
        let Some((value, is_const)) = runtime.marshal_binding(env, &name) else {
            return Err(ZuzuRustError::runtime(format!(
                "Code source references unresolved name '{name}'"
            )));
        };
        match value {
            Value::NativeFunction(_) | Value::Class(_) => continue,
            Value::UserClass(class) if is_ambient_builtin_user_class(&class.name) => continue,
            Value::Function(_) | Value::UserClass(_) | Value::Trait(_) => {
                if !is_const {
                    return Err(ZuzuRustError::runtime(format!(
                        "Code dependency '{name}' is not const"
                    )));
                }
                dependencies.push((name, value));
            }
            other => {
                if !is_const {
                    return Err(ZuzuRustError::runtime(format!(
                        "Code capture '{name}' is not const"
                    )));
                }
                if !is_scalar_capture_value(&other) {
                    return Err(ZuzuRustError::runtime(format!(
                        "Code capture '{name}' is not a scalar value"
                    )));
                }
                captures.push((name, other));
            }
        }
    }
    Ok(CodeAnalysis {
        captures,
        dependencies,
    })
}

fn is_ambient_builtin_user_class(name: &str) -> bool {
    matches!(
        name,
        "Exception"
            | "BailOutException"
            | "TypeException"
            | "CancelledException"
            | "TimeoutException"
            | "ChannelClosedException"
            | "MarshallingException"
            | "UnmarshallingException"
            | "ExhaustedException"
    )
}

fn encode_code_analysis(
    runtime: &Runtime,
    analysis: &CodeAnalysis,
    state: &mut DumpState,
) -> Result<(Vec<CborValue>, Vec<CborValue>)> {
    let captures = analysis
        .captures
        .iter()
        .map(|(name, value)| {
            Ok(CborValue::Array(vec![
                CborValue::Text(name.clone()),
                encode_scalar_capture(value)?,
            ]))
        })
        .collect::<Result<Vec<_>>>()?;
    let dependencies = analysis
        .dependencies
        .iter()
        .map(|(name, value)| encode_internal_dependency(runtime, name, value, state))
        .collect::<Result<Vec<_>>>()?;
    Ok((captures, dependencies))
}

fn encode_internal_dependency(
    runtime: &Runtime,
    name: &str,
    value: &Value,
    state: &mut DumpState,
) -> Result<CborValue> {
    let code_id = match value {
        Value::Function(function) => encode_function_code(runtime, function, state, Some(name))?,
        Value::UserClass(class) => encode_class_code(runtime, class, state, Some(name))?,
        Value::Trait(trait_value) => encode_trait_code(runtime, trait_value, state, Some(name))?,
        other => {
            return Err(ZuzuRustError::runtime(format!(
                "Code dependency '{name}' has unsupported type {}",
                other.type_name()
            )))
        }
    };
    Ok(CborValue::Array(vec![
        CborValue::Integer(0),
        CborValue::Integer(code_id as i128),
    ]))
}

fn is_scalar_capture_value(value: &Value) -> bool {
    matches!(
        value,
        Value::Null
            | Value::Boolean(_)
            | Value::Number(_)
            | Value::String(_)
            | Value::BinaryString(_)
    )
}

fn encode_scalar_capture(value: &Value) -> Result<CborValue> {
    match value {
        Value::Null => Ok(CborValue::Null),
        Value::Boolean(value) => Ok(CborValue::Bool(*value)),
        Value::Number(value) => number_to_cbor(*value),
        Value::String(value) => Ok(CborValue::Text(value.clone())),
        Value::BinaryString(value) => Ok(CborValue::Bytes(value.clone())),
        other => Err(ZuzuRustError::runtime(format!(
            "Capture value of type {} is not scalar",
            other.type_name()
        ))),
    }
}

fn function_initial_bindings(function: &FunctionValue) -> HashSet<String> {
    let mut bound = function
        .params
        .iter()
        .map(|param| param.name.clone())
        .collect::<HashSet<_>>();
    bound.insert("__argc__".to_owned());
    bound
}

fn class_initial_bindings(node: &ClassDeclaration) -> HashSet<String> {
    let mut bound = HashSet::from([node.name.clone()]);
    for member in &node.body {
        if let ClassMember::Class(class) = member {
            bound.insert(class.name.clone());
        }
    }
    bound
}

fn trait_initial_bindings(node: &TraitDeclaration) -> HashSet<String> {
    HashSet::from([node.name.clone()])
}

fn collect_function_free_names(function: &FunctionValue) -> Result<HashSet<String>> {
    let mut bound = function_initial_bindings(function);
    let mut free = HashSet::new();
    match &function.body {
        FunctionBody::Block(block) => collect_free_names_from_block(block, &mut bound, &mut free)?,
        FunctionBody::Expression(expr) => collect_free_names_from_expr(expr, &bound, &mut free)?,
    }
    Ok(free)
}

fn collect_class_free_names(node: &ClassDeclaration) -> Result<HashSet<String>> {
    let base_bound = class_initial_bindings(node);
    let mut free = HashSet::new();
    if let Some(base) = &node.base {
        if !base_bound.contains(base) {
            free.insert(base.clone());
        }
    }
    for trait_name in &node.traits {
        if !base_bound.contains(trait_name) {
            free.insert(trait_name.clone());
        }
    }
    for member in &node.body {
        match member {
            ClassMember::Field(field) => {
                if let Some(init) = &field.default_value {
                    collect_free_names_from_expr(init, &base_bound, &mut free)?;
                }
            }
            ClassMember::Method(method) => {
                let mut bound = base_bound.clone();
                bound.insert("self".to_owned());
                bound.insert("this".to_owned());
                bound.insert("super".to_owned());
                bound.insert("__argc__".to_owned());
                for field in class_fields(node) {
                    bound.insert(field.name.clone());
                }
                for param in &method.params {
                    bound.insert(param.name.clone());
                }
                collect_free_names_from_block(&method.body, &mut bound, &mut free)?;
            }
            ClassMember::Class(class) => {
                let mut nested_free = collect_class_free_names(class)?;
                free.extend(nested_free.drain());
            }
            ClassMember::Trait(trait_node) => {
                let mut trait_free = collect_trait_free_names(trait_node)?;
                free.extend(trait_free.drain());
            }
        }
    }
    Ok(free)
}

fn class_fields(node: &ClassDeclaration) -> Vec<&FieldDeclaration> {
    node.body
        .iter()
        .filter_map(|member| match member {
            ClassMember::Field(field) => Some(field),
            _ => None,
        })
        .collect()
}

fn collect_trait_free_names(node: &TraitDeclaration) -> Result<HashSet<String>> {
    let base_bound = trait_initial_bindings(node);
    let mut free = HashSet::new();
    for member in &node.body {
        if let ClassMember::Method(method) = member {
            let mut bound = base_bound.clone();
            bound.insert("self".to_owned());
            bound.insert("this".to_owned());
            bound.insert("super".to_owned());
            bound.insert("__argc__".to_owned());
            for param in &method.params {
                bound.insert(param.name.clone());
            }
            collect_free_names_from_block(&method.body, &mut bound, &mut free)?;
        }
    }
    Ok(free)
}

fn collect_free_names_from_block(
    block: &BlockStatement,
    bound: &mut HashSet<String>,
    free: &mut HashSet<String>,
) -> Result<()> {
    for statement in &block.statements {
        collect_free_names_from_statement(statement, bound, free)?;
    }
    Ok(())
}

fn collect_free_names_from_statement(
    statement: &Statement,
    bound: &mut HashSet<String>,
    free: &mut HashSet<String>,
) -> Result<()> {
    match statement {
        Statement::VariableDeclaration(node) => {
            if let Some(init) = &node.init {
                collect_free_names_from_expr(init, bound, free)?;
            }
            bound.insert(node.name.clone());
        }
        Statement::FunctionDeclaration(node) => {
            bound.insert(node.name.clone());
        }
        Statement::ClassDeclaration(node) => {
            bound.insert(node.name.clone());
        }
        Statement::TraitDeclaration(node) => {
            bound.insert(node.name.clone());
        }
        Statement::ReturnStatement(node) => {
            if let Some(argument) = &node.argument {
                collect_free_names_from_expr(argument, bound, free)?;
            }
        }
        Statement::ExpressionStatement(node) => {
            collect_free_names_from_expr(&node.expression, bound, free)?;
        }
        Statement::Block(block) => {
            let mut local = bound.clone();
            collect_free_names_from_block(block, &mut local, free)?;
        }
        Statement::IfStatement(node) => {
            collect_free_names_from_expr(&node.test, bound, free)?;
            let mut consequent_bound = bound.clone();
            collect_free_names_from_block(&node.consequent, &mut consequent_bound, free)?;
            if let Some(alternate) = &node.alternate {
                let mut alternate_bound = bound.clone();
                collect_free_names_from_statement(alternate, &mut alternate_bound, free)?;
            }
        }
        Statement::WhileStatement(node) => {
            collect_free_names_from_expr(&node.test, bound, free)?;
            let mut body_bound = bound.clone();
            collect_free_names_from_block(&node.body, &mut body_bound, free)?;
        }
        Statement::ThrowStatement(node) => {
            collect_free_names_from_expr(&node.argument, bound, free)?
        }
        Statement::DieStatement(node) => collect_free_names_from_expr(&node.argument, bound, free)?,
        Statement::ImportDeclaration(node) => {
            if let Some(condition) = &node.condition {
                collect_free_names_from_expr(&condition.test, bound, free)?;
            }
            if !node.import_all {
                for specifier in &node.specifiers {
                    bound.insert(specifier.local.clone());
                }
            }
        }
        Statement::LoopControlStatement(_) | Statement::KeywordStatement(_) => {}
        other => {
            return Err(ZuzuRustError::runtime(format!(
                "Unsupported statement for marshal capture analysis: {:?}",
                other
            )))
        }
    }
    Ok(())
}

fn collect_free_names_from_expr(
    expression: &Expression,
    bound: &HashSet<String>,
    free: &mut HashSet<String>,
) -> Result<()> {
    match expression {
        Expression::Identifier { name, .. } => {
            if !bound.contains(name) {
                free.insert(name.clone());
            }
        }
        Expression::NumberLiteral { .. }
        | Expression::StringLiteral { .. }
        | Expression::RegexLiteral { .. }
        | Expression::BooleanLiteral { .. }
        | Expression::NullLiteral { .. } => {}
        Expression::ArrayLiteral { elements, .. }
        | Expression::SetLiteral { elements, .. }
        | Expression::BagLiteral { elements, .. } => {
            for element in elements {
                collect_free_names_from_expr(element, bound, free)?;
            }
        }
        Expression::DictLiteral { entries, .. } | Expression::PairListLiteral { entries, .. } => {
            for entry in entries {
                collect_free_names_from_dict_key(&entry.key, bound, free)?;
                collect_free_names_from_expr(&entry.value, bound, free)?;
            }
        }
        Expression::TemplateLiteral { parts, .. } => {
            for part in parts {
                if let crate::ast::TemplatePart::Expression { expression, .. } = part {
                    collect_free_names_from_expr(expression, bound, free)?;
                }
            }
        }
        Expression::Unary { argument, .. } => {
            collect_free_names_from_expr(argument, bound, free)?;
        }
        Expression::Binary { left, right, .. }
        | Expression::DefinedOr { left, right, .. }
        | Expression::Assignment { left, right, .. } => {
            collect_free_names_from_expr(left, bound, free)?;
            collect_free_names_from_expr(right, bound, free)?;
        }
        Expression::Ternary {
            test,
            consequent,
            alternate,
            ..
        } => {
            collect_free_names_from_expr(test, bound, free)?;
            collect_free_names_from_expr(consequent, bound, free)?;
            collect_free_names_from_expr(alternate, bound, free)?;
        }
        Expression::Call {
            callee, arguments, ..
        } => {
            collect_free_names_from_expr(callee, bound, free)?;
            collect_free_names_from_call_arguments(arguments, bound, free)?;
        }
        Expression::MemberAccess { object, .. } => {
            collect_free_names_from_expr(object, bound, free)?;
        }
        Expression::DynamicMemberCall {
            object,
            member,
            arguments,
            ..
        } => {
            collect_free_names_from_expr(object, bound, free)?;
            collect_free_names_from_expr(member, bound, free)?;
            collect_free_names_from_call_arguments(arguments, bound, free)?;
        }
        Expression::Index { object, index, .. } => {
            collect_free_names_from_expr(object, bound, free)?;
            collect_free_names_from_expr(index, bound, free)?;
        }
        Expression::Slice {
            object, start, end, ..
        } => {
            collect_free_names_from_expr(object, bound, free)?;
            if let Some(start) = start {
                collect_free_names_from_expr(start, bound, free)?;
            }
            if let Some(end) = end {
                collect_free_names_from_expr(end, bound, free)?;
            }
        }
        Expression::DictAccess { object, key, .. } => {
            collect_free_names_from_expr(object, bound, free)?;
            collect_free_names_from_dict_key(key, bound, free)?;
        }
        Expression::PostfixUpdate { argument, .. } => {
            collect_free_names_from_expr(argument, bound, free)?;
        }
        Expression::Lambda { params, body, .. } => {
            let mut nested_bound = bound.clone();
            nested_bound.insert("__argc__".to_owned());
            for param in params {
                nested_bound.insert(param.name.clone());
            }
            collect_free_names_from_expr(body, &nested_bound, free)?;
        }
        Expression::FunctionExpression { params, body, .. } => {
            let mut nested_bound = bound.clone();
            nested_bound.insert("__argc__".to_owned());
            for param in params {
                nested_bound.insert(param.name.clone());
            }
            collect_free_names_from_block(body, &mut nested_bound, free)?;
        }
        Expression::LetExpression {
            init,
            name,
            kind: _,
            ..
        } => {
            if let Some(init) = init {
                collect_free_names_from_expr(init, bound, free)?;
            }
            let _ = name;
        }
        Expression::TryExpression { body, .. }
        | Expression::DoExpression { body, .. }
        | Expression::AwaitExpression { body, .. }
        | Expression::SpawnExpression { body, .. } => {
            let mut nested_bound = bound.clone();
            collect_free_names_from_block(body, &mut nested_bound, free)?;
        }
        Expression::SuperCall { arguments, .. } => {
            collect_free_names_from_call_arguments(arguments, bound, free)?;
        }
    }
    Ok(())
}

fn collect_free_names_from_dict_key(
    key: &DictKey,
    bound: &HashSet<String>,
    free: &mut HashSet<String>,
) -> Result<()> {
    if let DictKey::Expression { expression, .. } = key {
        collect_free_names_from_expr(expression, bound, free)?;
    }
    Ok(())
}

fn collect_free_names_from_call_arguments(
    arguments: &[CallArgument],
    bound: &HashSet<String>,
    free: &mut HashSet<String>,
) -> Result<()> {
    for argument in arguments {
        match argument {
            CallArgument::Positional { value, .. } => {
                collect_free_names_from_expr(value, bound, free)?;
            }
            CallArgument::Named { name, value, .. } => {
                collect_free_names_from_dict_key(name, bound, free)?;
                collect_free_names_from_expr(value, bound, free)?;
            }
        }
    }
    Ok(())
}

fn encode_anonymous_collection(
    runtime: &Runtime,
    kind: i128,
    values: &[Value],
    state: &mut DumpState,
) -> Result<CborValue> {
    let id = state.reserve_anonymous();
    let payload = encode_item_payload(runtime, values, state)?;
    state.set_object(id, kind, payload);
    Ok(strong_ref(id))
}

fn encode_anonymous_dict(
    runtime: &Runtime,
    values: &HashMap<String, Value>,
    state: &mut DumpState,
) -> Result<CborValue> {
    let id = state.reserve_anonymous();
    let payload = encode_dict_payload(runtime, values, state)?;
    state.set_object(id, KIND_DICT, payload);
    Ok(strong_ref(id))
}

fn encode_anonymous_pairlist(
    runtime: &Runtime,
    values: &[(String, Value)],
    state: &mut DumpState,
) -> Result<CborValue> {
    let id = state.reserve_anonymous();
    let payload = encode_pairlist_payload(runtime, values, state)?;
    state.set_object(id, KIND_PAIRLIST, payload);
    Ok(strong_ref(id))
}

fn encode_anonymous_pair(
    runtime: &Runtime,
    key: &str,
    value: &Value,
    state: &mut DumpState,
) -> Result<CborValue> {
    let id = state.reserve_anonymous();
    let payload = encode_pair_payload(runtime, key, value, state)?;
    state.set_object(id, KIND_PAIR, payload);
    Ok(strong_ref(id))
}

fn encode_pair_payload(
    runtime: &Runtime,
    key: &str,
    value: &Value,
    state: &mut DumpState,
) -> Result<CborValue> {
    Ok(CborValue::Array(vec![
        CborValue::Text(key.to_owned()),
        encode_stored_value(runtime, value, state, false)?,
    ]))
}

fn encode_item_payload(
    runtime: &Runtime,
    values: &[Value],
    state: &mut DumpState,
) -> Result<CborValue> {
    Ok(CborValue::Array(
        values
            .iter()
            .map(|value| encode_stored_value(runtime, value, state, false))
            .collect::<Result<Vec<_>>>()?,
    ))
}

fn encode_dict_payload(
    runtime: &Runtime,
    values: &HashMap<String, Value>,
    state: &mut DumpState,
) -> Result<CborValue> {
    let mut keys = values.keys().cloned().collect::<Vec<_>>();
    keys.sort();
    Ok(CborValue::Array(
        keys.into_iter()
            .map(|key| {
                Ok(CborValue::Array(vec![
                    CborValue::Text(key.clone()),
                    encode_stored_value(runtime, &values[&key], state, false)?,
                ]))
            })
            .collect::<Result<Vec<_>>>()?,
    ))
}

fn encode_pairlist_payload(
    runtime: &Runtime,
    values: &[(String, Value)],
    state: &mut DumpState,
) -> Result<CborValue> {
    Ok(CborValue::Array(
        values
            .iter()
            .map(|(key, value)| {
                Ok(CborValue::Array(vec![
                    CborValue::Text(key.clone()),
                    encode_stored_value(runtime, value, state, false)?,
                ]))
            })
            .collect::<Result<Vec<_>>>()?,
    ))
}

fn is_object_table_value(value: &Value) -> bool {
    matches!(
        value,
        Value::Array(_)
            | Value::Set(_)
            | Value::Bag(_)
            | Value::Dict(_)
            | Value::PairList(_)
            | Value::Pair(_, _)
            | Value::Function(_)
            | Value::UserClass(_)
            | Value::Trait(_)
            | Value::Method(_)
    )
}

fn strong_ref(id: usize) -> CborValue {
    CborValue::Array(vec![CborValue::Integer(0), CborValue::Integer(id as i128)])
}

fn number_to_cbor(value: f64) -> Result<CborValue> {
    if !value.is_finite() {
        return Err(ZuzuRustError::runtime("Number value is not finite"));
    }
    if is_cbor_integer(value) {
        return Ok(CborValue::Integer(value as i128));
    }
    Ok(CborValue::Float(value))
}

fn is_cbor_integer(value: f64) -> bool {
    value.fract() == 0.0
        && value.abs() <= MAX_SAFE_INTEGER
        && value.to_bits() != (-0.0f64).to_bits()
}

fn load_binary(runtime: &Runtime, bytes: &[u8]) -> Result<Value> {
    let decoded = decode_one(bytes)?;
    decode_envelope(runtime, decoded)
}

fn decode_envelope(runtime: &Runtime, decoded: CborValue) -> Result<Value> {
    let CborValue::Tag { tag, value } = decoded else {
        return Err(ZuzuRustError::runtime("Top-level item is not tag 55799"));
    };
    if tag != 55_799 {
        return Err(ZuzuRustError::runtime("Top-level item is not tag 55799"));
    }
    let CborValue::Array(envelope) = *value else {
        return Err(ZuzuRustError::runtime("Envelope must be an array"));
    };
    if envelope.len() != 6 {
        return Err(ZuzuRustError::runtime(
            "Envelope must contain exactly 6 fields",
        ));
    }
    if envelope[0] != CborValue::Text("ZUZU-MARSHAL".to_owned()) {
        return Err(ZuzuRustError::runtime("Envelope magic is invalid"));
    }
    if envelope[1] != CborValue::Integer(1) {
        return Err(ZuzuRustError::runtime("Unsupported Zuzu Marshal version"));
    }
    let CborValue::Map(_) = &envelope[2] else {
        return Err(ZuzuRustError::runtime("Envelope options must be a map"));
    };
    let CborValue::Array(objects) = &envelope[4] else {
        return Err(ZuzuRustError::runtime(
            "Envelope object table must be an array",
        ));
    };
    let CborValue::Array(code) = &envelope[5] else {
        return Err(ZuzuRustError::runtime(
            "Envelope code table must be an array",
        ));
    };
    let code_values = load_code_table(runtime, code)?;
    let placeholders = allocate_object_placeholders(runtime, objects, &code_values)?;
    fill_object_placeholders(runtime, objects, &placeholders)?;
    let value = decode_value(runtime, &envelope[3], &placeholders, false, "Envelope root")?;
    run_on_load_hooks(runtime, objects, &placeholders)?;
    Ok(value)
}

#[derive(Clone)]
struct CodeRecord {
    kind: i128,
    binding_name: String,
    source: String,
    captures: Vec<CborValue>,
    dependencies: Vec<CborValue>,
}

fn load_code_table(runtime: &Runtime, code: &[CborValue]) -> Result<Vec<Value>> {
    let shared = runtime.marshal_current_or_builtin_env();
    let mut records = Vec::new();
    let mut names = HashSet::new();
    for (id, record) in code.iter().enumerate() {
        let record = validate_code_record(id, record)?;
        if !matches!(record.kind, CODE_FUNCTION | CODE_CLASS | CODE_TRAIT) {
            return Err(ZuzuRustError::runtime(format!(
                "Unsupported code kind {} in current loader",
                record.kind
            )));
        }
        if !names.insert(record.binding_name.clone()) {
            return Err(ZuzuRustError::runtime(format!(
                "Duplicate code binding '{}'",
                record.binding_name
            )));
        }
        runtime.marshal_define_env(&shared, &record.binding_name, Value::Null, false);
        records.push(record);
    }
    for (id, record) in records.iter().enumerate() {
        install_external_dependencies(runtime, &shared, id, record, records.len())?;
    }
    let mut values = vec![None; records.len()];
    let mut status = vec![CodeLoadStatus::Pending; records.len()];
    for id in 0..records.len() {
        load_code_record_by_id(runtime, &shared, &records, &mut values, &mut status, id)?;
    }
    values
        .into_iter()
        .enumerate()
        .map(|(id, value)| {
            value.ok_or_else(|| {
                ZuzuRustError::runtime(format!("Code table entry {id} was not loaded"))
            })
        })
        .collect()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CodeLoadStatus {
    Pending,
    Loading,
    Done,
}

fn validate_code_record(id: usize, record: &CborValue) -> Result<CodeRecord> {
    let items = expect_array(record, &format!("Code table entry {id}"))?;
    if items.len() != 5 {
        return Err(ZuzuRustError::runtime(format!(
            "Code table entry {id} must be a five-item array"
        )));
    }
    let kind = expect_integer(&items[0], &format!("Code table entry {id} kind"))?;
    let binding_name = expect_text(&items[1], &format!("Code table entry {id} binding name"))?;
    if !is_identifier(binding_name) {
        return Err(ZuzuRustError::runtime(format!(
            "Code table entry {id} binding name is not a valid identifier"
        )));
    }
    let source = expect_text(&items[2], &format!("Code table entry {id} source"))?;
    let captures = expect_array(&items[3], &format!("Code table entry {id} captures"))?.to_vec();
    let dependencies =
        expect_array(&items[4], &format!("Code table entry {id} dependencies"))?.to_vec();
    Ok(CodeRecord {
        kind,
        binding_name: binding_name.to_owned(),
        source: source.to_owned(),
        captures,
        dependencies,
    })
}

fn install_external_dependencies(
    runtime: &Runtime,
    shared: &Rc<Environment>,
    id: usize,
    record: &CodeRecord,
    record_count: usize,
) -> Result<()> {
    for dependency in &record.dependencies {
        let items = expect_array(dependency, &format!("Code dependency in record {id}"))?;
        if items.is_empty() {
            return Err(ZuzuRustError::runtime(format!(
                "Code dependency in record {id} must not be empty"
            )));
        }
        let kind = expect_integer(&items[0], &format!("Code dependency in record {id} kind"))?;
        match kind {
            0 => {
                if items.len() != 2 {
                    return Err(ZuzuRustError::runtime(format!(
                        "Internal dependency in record {id} must be [0, code_id]"
                    )));
                }
                reject_weak_storage_record(
                    &items[1],
                    &format!("Internal dependency in record {id} code id"),
                )?;
                let code_id = expect_integer(
                    &items[1],
                    &format!("Internal dependency in record {id} code id"),
                )?;
                if code_id < 0 || code_id as usize >= record_count {
                    return Err(ZuzuRustError::runtime(format!(
                        "Internal dependency in record {id} has invalid code id"
                    )));
                }
            }
            1 => {
                install_external_dependency(runtime, shared, id, items)?;
            }
            _ => {
                return Err(ZuzuRustError::runtime(format!(
                    "Unsupported code dependency kind {kind} in record {id}"
                )))
            }
        }
    }
    Ok(())
}

fn install_external_dependency(
    runtime: &Runtime,
    shared: &Rc<Environment>,
    id: usize,
    items: &[CborValue],
) -> Result<()> {
    if items.len() != 4 {
        return Err(ZuzuRustError::runtime(format!(
            "External dependency in record {id} must have four fields"
        )));
    }
    let local_name = expect_text(
        &items[1],
        &format!("External dependency local_name in record {id}"),
    )?;
    let module_name = expect_text(
        &items[2],
        &format!("External dependency module in record {id}"),
    )?;
    let export_name = expect_text(
        &items[3],
        &format!("External dependency export_name in record {id}"),
    )?;
    if !is_identifier(local_name) {
        return Err(ZuzuRustError::runtime(format!(
            "External dependency local name '{local_name}' is not valid"
        )));
    }
    if !module_name.starts_with("std/") {
        return Err(ZuzuRustError::runtime(format!(
            "External dependency module '{module_name}' is not a stdlib module"
        )));
    }
    let value = runtime.marshal_load_module_export(module_name, export_name)?;
    if let Some((existing, _)) = runtime.marshal_binding(shared, local_name) {
        if !matches!(existing, Value::Null) {
            return Err(ZuzuRustError::runtime(format!(
                "External dependency '{local_name}' conflicts with code binding"
            )));
        }
    }
    runtime.marshal_define_env(shared, local_name, value, false);
    Ok(())
}

fn load_code_record_by_id(
    runtime: &Runtime,
    shared: &Rc<Environment>,
    records: &[CodeRecord],
    values: &mut [Option<Value>],
    status: &mut [CodeLoadStatus],
    id: usize,
) -> Result<Value> {
    if id >= records.len() {
        return Err(ZuzuRustError::runtime(
            "Internal dependency has invalid code id",
        ));
    }
    if status[id] == CodeLoadStatus::Done {
        return values[id].clone().ok_or_else(|| {
            ZuzuRustError::runtime(format!("Code table entry {id} was not loaded"))
        });
    }
    if status[id] == CodeLoadStatus::Loading {
        return Err(ZuzuRustError::runtime(format!(
            "Cyclic class or trait code dependency involving record {id}"
        )));
    }
    status[id] = CodeLoadStatus::Loading;
    if matches!(records[id].kind, CODE_CLASS | CODE_TRAIT) {
        for dependency in &records[id].dependencies {
            let items = expect_array(dependency, &format!("Code dependency in record {id}"))?;
            let kind = expect_integer(&items[0], &format!("Code dependency in record {id} kind"))?;
            if kind != 0 {
                continue;
            }
            let dependency_id = expect_integer(
                &items[1],
                &format!("Internal dependency in record {id} code id"),
            )? as usize;
            if records[dependency_id].kind != CODE_FUNCTION {
                load_code_record_by_id(runtime, shared, records, values, status, dependency_id)?;
            }
        }
    }
    let value = load_code_record(runtime, shared, id, &records[id])?;
    runtime.marshal_refresh_env(shared, &records[id].binding_name, value.clone());
    values[id] = Some(value.clone());
    status[id] = CodeLoadStatus::Done;
    Ok(value)
}

fn load_code_record(
    runtime: &Runtime,
    shared: &Rc<Environment>,
    id: usize,
    record: &CodeRecord,
) -> Result<Value> {
    let private = runtime.marshal_child_env(Rc::clone(shared));
    install_captures(runtime, &private, id, record)?;
    let expression_result = record.kind == CODE_FUNCTION;
    let value = runtime.eval_marshal_code_value_in_env(
        &record.source,
        &record.binding_name,
        private,
        expression_result,
    )?;
    let ok = match record.kind {
        CODE_FUNCTION => matches!(value, Value::Function(_)),
        CODE_CLASS => matches!(value, Value::UserClass(_)),
        CODE_TRAIT => matches!(value, Value::Trait(_)),
        _ => false,
    };
    if !ok {
        let expected = match record.kind {
            CODE_FUNCTION => "Function",
            CODE_CLASS => "Class",
            CODE_TRAIT => "Trait",
            _ => "supported code value",
        };
        return Err(ZuzuRustError::runtime(format!(
            "Code record {id} did not evaluate to a {expected}"
        )));
    }
    Ok(value)
}

fn install_captures(
    runtime: &Runtime,
    private: &Rc<Environment>,
    id: usize,
    record: &CodeRecord,
) -> Result<()> {
    let mut names = HashSet::new();
    for capture in &record.captures {
        let items = expect_array(capture, &format!("Capture in code record {id}"))?;
        if items.len() != 2 {
            return Err(ZuzuRustError::runtime(format!(
                "Capture in code record {id} must be a two-item array"
            )));
        }
        let name = expect_text(&items[0], &format!("Capture name in code record {id}"))?;
        if !is_identifier(name) {
            return Err(ZuzuRustError::runtime(format!(
                "Capture name '{name}' in code record {id} is not valid"
            )));
        }
        if !names.insert(name.to_owned()) {
            return Err(ZuzuRustError::runtime(format!(
                "Duplicate capture '{name}' in code record {id}"
            )));
        }
        reject_weak_storage_record(&items[1], &format!("Capture '{name}' in code record {id}"))?;
        let value = decode_scalar_capture(&items[1])?;
        runtime.marshal_define_env(private, name, value, false);
    }
    Ok(())
}

fn decode_scalar_capture(value: &CborValue) -> Result<Value> {
    match value {
        CborValue::Null => Ok(Value::Null),
        CborValue::Bool(value) => Ok(Value::Boolean(*value)),
        CborValue::Integer(value) => Ok(Value::Number(*value as f64)),
        CborValue::Float(value) => Ok(Value::Number(*value)),
        CborValue::Text(value) => Ok(Value::String(value.clone())),
        CborValue::Bytes(value) => Ok(Value::BinaryString(value.clone())),
        _ => Err(ZuzuRustError::runtime(
            "Capture value is not an inline scalar",
        )),
    }
}

fn allocate_object_placeholders(
    runtime: &Runtime,
    objects: &[CborValue],
    code_values: &[Value],
) -> Result<Vec<Value>> {
    objects
        .iter()
        .enumerate()
        .map(|(id, entry)| {
            let (kind, payload) = object_entry(id, entry)?;
            match kind {
                KIND_PAIR => Ok(Value::Shared(Rc::new(RefCell::new(Value::Pair(
                    String::new(),
                    Box::new(Value::Null),
                ))))),
                KIND_ARRAY => Ok(Value::Shared(Rc::new(RefCell::new(Value::Array(
                    Vec::new(),
                ))))),
                KIND_DICT => Ok(Value::Shared(Rc::new(RefCell::new(Value::Dict(
                    HashMap::new(),
                ))))),
                KIND_PAIRLIST => Ok(Value::Shared(Rc::new(RefCell::new(Value::PairList(
                    Vec::new(),
                ))))),
                KIND_SET => Ok(Value::Shared(Rc::new(RefCell::new(Value::Set(Vec::new()))))),
                KIND_BAG => Ok(Value::Shared(Rc::new(RefCell::new(Value::Bag(Vec::new()))))),
                KIND_OBJECT => Ok(Value::Object(Rc::new(RefCell::new(ObjectValue {
                    class: Rc::new(pending_class()),
                    fields: HashMap::new(),
                    weak_fields: std::collections::HashSet::new(),
                    builtin_value: None,
                })))),
                KIND_FUNCTION => decode_function_payload(id, payload, code_values),
                KIND_CLASS => decode_class_payload(id, payload, code_values),
                KIND_TRAIT => decode_trait_payload(id, payload, code_values),
                KIND_BOUND_METHOD => Ok(Value::Shared(Rc::new(RefCell::new(Value::Null)))),
                KIND_TIME => time::construct_time(runtime, vec![Value::Number(0.0)], Vec::new()),
                KIND_PATH => Ok(io::path_object(PathBuf::from("."))),
                _ => Err(ZuzuRustError::runtime(format!(
                    "Unsupported object kind {kind} in current loader"
                ))),
            }
        })
        .collect()
}

fn fill_object_placeholders(
    runtime: &Runtime,
    objects: &[CborValue],
    placeholders: &[Value],
) -> Result<()> {
    for (id, entry) in objects.iter().enumerate() {
        let (kind, payload) = object_entry(id, entry)?;
        match kind {
            KIND_PAIR => {
                let items = expect_array(payload, &format!("Pair object payload {id}"))?;
                if items.len() != 2 {
                    return Err(ZuzuRustError::runtime(format!(
                        "Pair object payload {id} must be a two-item array"
                    )));
                }
                let key = expect_text(&items[0], &format!("Pair object payload {id} key"))?;
                let value = decode_value(
                    runtime,
                    &items[1],
                    placeholders,
                    true,
                    &format!("Pair object payload {id} value"),
                )?;
                replace_shared(
                    &placeholders[id],
                    Value::Pair(key.to_owned(), Box::new(value)),
                )?;
            }
            KIND_ARRAY => {
                let items = expect_array(payload, &format!("Array object payload {id}"))?
                    .iter()
                    .map(|item| {
                        decode_value(
                            runtime,
                            item,
                            placeholders,
                            true,
                            &format!("Array object payload {id} item"),
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                replace_shared(&placeholders[id], Value::Array(items))?;
            }
            KIND_DICT => {
                let map = decode_dict_payload(runtime, id, payload, placeholders)?;
                replace_shared(&placeholders[id], Value::Dict(map))?;
            }
            KIND_PAIRLIST => {
                let values = decode_pairlist_payload(runtime, id, payload, placeholders)?;
                replace_shared(&placeholders[id], Value::PairList(values))?;
            }
            KIND_SET => {
                let mut values = Vec::new();
                for item in decode_item_payload(
                    runtime,
                    &format!("Set object payload {id}"),
                    payload,
                    placeholders,
                )? {
                    push_unique(&mut values, item);
                }
                replace_shared(&placeholders[id], Value::Set(values))?;
            }
            KIND_BAG => {
                let items = decode_item_payload(
                    runtime,
                    &format!("Bag object payload {id}"),
                    payload,
                    placeholders,
                )?;
                replace_shared(&placeholders[id], Value::Bag(items))?;
            }
            KIND_OBJECT => fill_user_object_payload(runtime, id, payload, placeholders)?,
            KIND_FUNCTION | KIND_CLASS | KIND_TRAIT | KIND_BOUND_METHOD => {}
            KIND_TIME => fill_time_placeholder(id, payload, &placeholders[id])?,
            KIND_PATH => fill_path_placeholder(id, payload, &placeholders[id])?,
            _ => {}
        }
    }
    for (id, entry) in objects.iter().enumerate() {
        let (kind, payload) = object_entry(id, entry)?;
        if kind == KIND_BOUND_METHOD {
            fill_bound_method_payload(runtime, id, payload, placeholders)?;
        }
    }
    Ok(())
}

fn object_entry(id: usize, entry: &CborValue) -> Result<(i128, &CborValue)> {
    let CborValue::Array(items) = entry else {
        return Err(ZuzuRustError::runtime(format!(
            "Object table entry {id} must be a two-item array"
        )));
    };
    if items.len() != 2 {
        return Err(ZuzuRustError::runtime(format!(
            "Object table entry {id} must be a two-item array"
        )));
    }
    let CborValue::Integer(kind) = items[0] else {
        return Err(ZuzuRustError::runtime(format!(
            "Object table entry {id} kind must be an integer"
        )));
    };
    Ok((kind, &items[1]))
}

fn decode_function_payload(id: usize, payload: &CborValue, code_values: &[Value]) -> Result<Value> {
    let items = expect_array(payload, &format!("Function object payload {id}"))?;
    if items.len() != 1 {
        return Err(ZuzuRustError::runtime(format!(
            "Function object payload {id} must be a one-item array"
        )));
    }
    reject_weak_storage_record(&items[0], &format!("Function object payload {id} code id"))?;
    let code_id = expect_integer(&items[0], &format!("Function object payload {id} code id"))?;
    if code_id < 0 || code_id as usize >= code_values.len() {
        return Err(ZuzuRustError::runtime(format!(
            "Function object payload {id} code id is outside the code table"
        )));
    }
    let value = code_values[code_id as usize].clone();
    if !matches!(value, Value::Function(_)) {
        return Err(ZuzuRustError::runtime(format!(
            "Function object payload {id} code record is not a Function"
        )));
    }
    Ok(value)
}

fn decode_class_payload(id: usize, payload: &CborValue, code_values: &[Value]) -> Result<Value> {
    let items = expect_array(payload, &format!("Class object payload {id}"))?;
    if items.len() != 1 {
        return Err(ZuzuRustError::runtime(format!(
            "Class object payload {id} must be a one-item array"
        )));
    }
    reject_weak_storage_record(&items[0], &format!("Class object payload {id} code id"))?;
    let code_id = expect_integer(&items[0], &format!("Class object payload {id} code id"))?;
    if code_id < 0 || code_id as usize >= code_values.len() {
        return Err(ZuzuRustError::runtime(format!(
            "Class object payload {id} code id is outside the code table"
        )));
    }
    let value = code_values[code_id as usize].clone();
    if !matches!(value, Value::UserClass(_)) {
        return Err(ZuzuRustError::runtime(format!(
            "Class object payload {id} code record is not a Class"
        )));
    }
    Ok(value)
}

fn decode_trait_payload(id: usize, payload: &CborValue, code_values: &[Value]) -> Result<Value> {
    let items = expect_array(payload, &format!("Trait object payload {id}"))?;
    if items.len() != 1 {
        return Err(ZuzuRustError::runtime(format!(
            "Trait object payload {id} must be a one-item array"
        )));
    }
    reject_weak_storage_record(&items[0], &format!("Trait object payload {id} code id"))?;
    let code_id = expect_integer(&items[0], &format!("Trait object payload {id} code id"))?;
    if code_id < 0 || code_id as usize >= code_values.len() {
        return Err(ZuzuRustError::runtime(format!(
            "Trait object payload {id} code id is outside the code table"
        )));
    }
    let value = code_values[code_id as usize].clone();
    if !matches!(value, Value::Trait(_)) {
        return Err(ZuzuRustError::runtime(format!(
            "Trait object payload {id} code record is not a Trait"
        )));
    }
    Ok(value)
}

fn fill_bound_method_payload(
    runtime: &Runtime,
    id: usize,
    payload: &CborValue,
    placeholders: &[Value],
) -> Result<()> {
    let items = expect_array(payload, &format!("Bound method object payload {id}"))?;
    if items.len() != 2 {
        return Err(ZuzuRustError::runtime(format!(
            "Bound method object payload {id} must be a two-item array"
        )));
    }
    let receiver = decode_value(
        runtime,
        &items[0],
        placeholders,
        false,
        &format!("Bound method object payload {id} receiver"),
    )?;
    let method_name = expect_text(
        &items[1],
        &format!("Bound method object payload {id} method name"),
    )?;
    let receiver = match receiver {
        Value::Shared(cell) => cell.borrow().clone(),
        other => other,
    };
    let (Value::Object(_) | Value::UserClass(_)) = receiver else {
        return Err(ZuzuRustError::runtime(format!(
            "Bound method object payload {id} receiver must resolve to an Object"
        )));
    };
    let bound = runtime
        .marshal_bind_method(receiver, method_name)
        .map_err(|_| {
            ZuzuRustError::runtime(format!(
                "Bound method object payload {id} method '{method_name}' was not found"
            ))
        })?;
    replace_shared(&placeholders[id], bound)
}

fn fill_user_object_payload(
    runtime: &Runtime,
    id: usize,
    payload: &CborValue,
    placeholders: &[Value],
) -> Result<()> {
    let items = expect_array(payload, &format!("Object payload {id}"))?;
    if items.len() != 2 {
        return Err(ZuzuRustError::runtime(format!(
            "Object payload {id} must be a two-item array"
        )));
    }
    let class_value = decode_value(
        runtime,
        &items[0],
        placeholders,
        false,
        &format!("Object payload {id} class"),
    )?;
    let Value::UserClass(class) = class_value else {
        return Err(ZuzuRustError::runtime(format!(
            "Object payload {id} class must resolve to a Class"
        )));
    };
    let slots = expect_array(&items[1], &format!("Object payload {id} slots"))?;
    let Value::Object(object) = &placeholders[id] else {
        return Err(ZuzuRustError::runtime(
            "Object placeholder is not an object",
        ));
    };
    object.borrow_mut().class = class;
    let mut slot_names = HashSet::new();
    for record in slots {
        let record_items = expect_array(record, &format!("Object payload {id} slot records"))?;
        if record_items.len() != 2 {
            return Err(ZuzuRustError::runtime(format!(
                "Object payload {id} slot records must be two-item arrays"
            )));
        }
        let name = expect_text(&record_items[0], &format!("Object payload {id} slot names"))?;
        if !slot_names.insert(name.to_owned()) {
            return Err(ZuzuRustError::runtime(format!(
                "Object payload {id} contains duplicate slot '{name}'"
            )));
        }
        let is_weak_storage = is_weak_storage_record(&record_items[1]);
        let value = decode_value(
            runtime,
            &record_items[1],
            placeholders,
            true,
            &format!("Object payload {id} slot '{name}'"),
        )?;
        runtime.marshal_set_object_field(object, name, value, is_weak_storage)?;
    }
    Ok(())
}

fn decode_value(
    runtime: &Runtime,
    value: &CborValue,
    placeholders: &[Value],
    allow_weak: bool,
    context: &str,
) -> Result<Value> {
    match value {
        CborValue::Null => Ok(Value::Null),
        CborValue::Bool(value) => Ok(Value::Boolean(*value)),
        CborValue::Integer(value) => Ok(Value::Number(*value as f64)),
        CborValue::Float(value) => Ok(Value::Number(*value)),
        CborValue::Text(value) => Ok(Value::String(value.clone())),
        CborValue::Bytes(value) => Ok(Value::BinaryString(value.clone())),
        CborValue::Array(items) => {
            decode_reference(runtime, items, placeholders, allow_weak, context)
        }
        _ => Err(ZuzuRustError::runtime(
            "Envelope root is not a scalar or supported reference",
        )),
    }
}

fn decode_reference(
    runtime: &Runtime,
    items: &[CborValue],
    placeholders: &[Value],
    allow_weak: bool,
    context: &str,
) -> Result<Value> {
    if items.len() != 2 {
        return Err(ZuzuRustError::runtime(format!(
            "{context} array must be [0, id] or [1, value]"
        )));
    }
    let CborValue::Integer(marker) = items[0] else {
        return Err(ZuzuRustError::runtime(format!(
            "{context} marker must be 0 or 1"
        )));
    };
    if marker != 0 && marker != 1 {
        return Err(ZuzuRustError::runtime(format!(
            "{context} marker must be 0 or 1"
        )));
    }
    if marker == 1 {
        validate_weak_storage_record(runtime, items, placeholders, context)?;
        if !allow_weak {
            return Err(ZuzuRustError::runtime(format!(
                "{context} weak storage record is not allowed here"
            )));
        }
        let value = decode_value(
            runtime,
            &items[1],
            placeholders,
            false,
            &format!("{context} weak storage value"),
        )?;
        if matches!(value, Value::Null) || value.scalar_type_name().is_some() {
            return Ok(Value::WeakStoredScalar(Box::new(value)));
        }
        return Ok(value.stored_with_weak_policy(true));
    }
    let CborValue::Integer(id) = items[1] else {
        return Err(ZuzuRustError::runtime(
            "Encoded reference id must be an integer",
        ));
    };
    if id < 0 || id as usize >= placeholders.len() {
        return Err(ZuzuRustError::runtime(format!(
            "Reference id {id} is outside the object table"
        )));
    }
    Ok(placeholders[id as usize].clone())
}

fn validate_weak_storage_record(
    runtime: &Runtime,
    record: &[CborValue],
    placeholders: &[Value],
    context: &str,
) -> Result<()> {
    if record.len() != 2 {
        return Err(ZuzuRustError::runtime(format!(
            "{context} weak storage record must be [1, value]"
        )));
    }
    if is_weak_storage_record(&record[1]) {
        return Err(ZuzuRustError::runtime(format!(
            "{context} nested weak storage records are invalid"
        )));
    }
    decode_value(
        runtime,
        &record[1],
        placeholders,
        false,
        &format!("{context} weak storage value"),
    )?;
    Ok(())
}

fn is_weak_storage_record(value: &CborValue) -> bool {
    matches!(
        value,
        CborValue::Array(items)
            if items.len() == 2 && matches!(items[0], CborValue::Integer(1))
    )
}

fn decode_item_payload(
    runtime: &Runtime,
    context: &str,
    payload: &CborValue,
    placeholders: &[Value],
) -> Result<Vec<Value>> {
    expect_array(payload, context)?
        .iter()
        .map(|item| {
            decode_value(
                runtime,
                item,
                placeholders,
                true,
                &format!("{context} item"),
            )
        })
        .collect()
}

fn decode_dict_payload(
    runtime: &Runtime,
    id: usize,
    payload: &CborValue,
    placeholders: &[Value],
) -> Result<HashMap<String, Value>> {
    let mut map = HashMap::new();
    for pair in expect_array(payload, &format!("Dict object payload {id}"))? {
        let (key, value) = decode_key_value_record(
            runtime,
            &format!("Dict object payload {id}"),
            pair,
            placeholders,
        )?;
        if map.contains_key(&key) {
            return Err(ZuzuRustError::runtime(format!(
                "Dict object payload {id} contains duplicate key '{key}'"
            )));
        }
        map.insert(key, value);
    }
    Ok(map)
}

fn decode_pairlist_payload(
    runtime: &Runtime,
    id: usize,
    payload: &CborValue,
    placeholders: &[Value],
) -> Result<Vec<(String, Value)>> {
    expect_array(payload, &format!("PairList object payload {id}"))?
        .iter()
        .map(|pair| {
            decode_key_value_record(
                runtime,
                &format!("PairList object payload {id}"),
                pair,
                placeholders,
            )
        })
        .collect()
}

fn decode_key_value_record(
    runtime: &Runtime,
    context: &str,
    pair: &CborValue,
    placeholders: &[Value],
) -> Result<(String, Value)> {
    let CborValue::Array(items) = pair else {
        return Err(ZuzuRustError::runtime(format!(
            "{context} entries must be two-item arrays"
        )));
    };
    if items.len() != 2 {
        return Err(ZuzuRustError::runtime(format!(
            "{context} entries must be two-item arrays"
        )));
    }
    let CborValue::Text(key) = &items[0] else {
        return Err(ZuzuRustError::runtime(format!(
            "{context} keys must be text strings"
        )));
    };
    Ok((
        key.clone(),
        decode_value(
            runtime,
            &items[1],
            placeholders,
            true,
            &format!("{context} value"),
        )?,
    ))
}

fn fill_time_placeholder(id: usize, payload: &CborValue, placeholder: &Value) -> Result<()> {
    let items = expect_array(payload, &format!("Time object payload {id}"))?;
    if items.len() != 1 {
        return Err(ZuzuRustError::runtime(format!(
            "Time object payload {id} must be a one-item array"
        )));
    }
    let epoch = match items[0] {
        CborValue::Integer(value) => value as f64,
        CborValue::Float(value) => value,
        _ => {
            return Err(ZuzuRustError::runtime(format!(
                "Time object payload {id} epoch must be a number"
            )))
        }
    };
    let Value::Object(object) = placeholder else {
        return Err(ZuzuRustError::runtime("Time placeholder is not an object"));
    };
    let mut object = object.borrow_mut();
    object
        .fields
        .insert("epoch".to_owned(), Value::Number(epoch));
    object.builtin_value = Some(Value::Number(epoch));
    Ok(())
}

fn fill_path_placeholder(id: usize, payload: &CborValue, placeholder: &Value) -> Result<()> {
    let items = expect_array(payload, &format!("Path object payload {id}"))?;
    if items.len() != 1 {
        return Err(ZuzuRustError::runtime(format!(
            "Path object payload {id} must be a one-item array"
        )));
    }
    let CborValue::Text(path) = &items[0] else {
        return Err(ZuzuRustError::runtime(format!(
            "Path object payload {id} path must be a text string"
        )));
    };
    let Value::Object(object) = placeholder else {
        return Err(ZuzuRustError::runtime("Path placeholder is not an object"));
    };
    let mut object = object.borrow_mut();
    object
        .fields
        .insert("path".to_owned(), Value::String(path.clone()));
    object.builtin_value = Some(Value::String(path.clone()));
    Ok(())
}

fn run_on_load_hooks(
    runtime: &Runtime,
    objects: &[CborValue],
    placeholders: &[Value],
) -> Result<()> {
    for (id, entry) in objects.iter().enumerate() {
        let (kind, _) = object_entry(id, entry)?;
        if kind != KIND_OBJECT {
            continue;
        }
        let Value::Object(object) = &placeholders[id] else {
            continue;
        };
        runtime
            .marshal_call_object_hook(object, "__on_load__")
            .map_err(|err| {
                ZuzuRustError::runtime(format!("__on_load__ hook failed: {}", exception_text(err)))
            })?;
    }
    Ok(())
}

fn expect_array<'a>(value: &'a CborValue, context: &str) -> Result<&'a [CborValue]> {
    let CborValue::Array(items) = value else {
        return Err(ZuzuRustError::runtime(format!(
            "{context} must be an array"
        )));
    };
    Ok(items)
}

fn expect_integer(value: &CborValue, context: &str) -> Result<i128> {
    let CborValue::Integer(value) = value else {
        return Err(ZuzuRustError::runtime(format!(
            "{context} must be an integer"
        )));
    };
    Ok(*value)
}

fn expect_text<'a>(value: &'a CborValue, context: &str) -> Result<&'a str> {
    let CborValue::Text(value) = value else {
        return Err(ZuzuRustError::runtime(format!(
            "{context} must be a text string"
        )));
    };
    Ok(value)
}

fn reject_weak_storage_record(value: &CborValue, context: &str) -> Result<()> {
    if is_weak_storage_record(value) {
        return Err(ZuzuRustError::runtime(format!(
            "{context} weak storage record is not allowed here"
        )));
    }
    Ok(())
}

fn pending_class() -> UserClassValue {
    UserClassValue {
        name: "<pending marshal object>".to_owned(),
        base: None,
        traits: Vec::new(),
        fields: Vec::new(),
        methods: HashMap::new(),
        static_methods: HashMap::new(),
        nested_classes: HashMap::new(),
        source_decl: None,
        closure_env: None,
    }
}

fn replace_shared(placeholder: &Value, value: Value) -> Result<()> {
    let Value::Shared(cell) = placeholder else {
        return Err(ZuzuRustError::runtime("placeholder is not shared"));
    };
    *cell.borrow_mut() = value;
    Ok(())
}

fn push_unique(values: &mut Vec<Value>, value: Value) {
    if !values
        .iter()
        .any(|existing| marshal_set_items_equal(existing, &value))
    {
        values.push(value);
    }
}

fn marshal_set_items_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Shared(left), Value::Shared(right)) => Rc::ptr_eq(left, right),
        (Value::Shared(_), _) | (_, Value::Shared(_)) => false,
        _ => left.strict_eq(right),
    }
}

fn expect_arity(name: &str, args: &[Value], expected: usize) -> Result<()> {
    if args.len() == expected {
        return Ok(());
    }
    let noun = if expected == 1 {
        "argument"
    } else {
        "arguments"
    };
    Err(ZuzuRustError::thrown(format!(
        "TypeException: {name} expects {expected} {noun}, got {}",
        args.len()
    )))
}

fn render_function_value(function: &FunctionValue) -> String {
    let mut out = String::new();
    if function.is_async {
        out.push_str("async ");
    }
    out.push_str("function (");
    out.push_str(
        &function
            .params
            .iter()
            .map(render_parameter)
            .collect::<Vec<_>>()
            .join(", "),
    );
    out.push(')');
    if let Some(return_type) = &function.return_type {
        out.push_str(" -> ");
        out.push_str(return_type);
    }
    out.push(' ');
    match &function.body {
        FunctionBody::Block(block) => out.push_str(&render_block(block)),
        FunctionBody::Expression(expression) => {
            out.push_str("{ return ");
            out.push_str(&render_expression(expression));
            out.push_str("; }");
        }
    }
    out
}

fn render_class_declaration(node: &ClassDeclaration) -> String {
    codegen::render_class_declaration(node)
}

fn render_trait_declaration(node: &TraitDeclaration) -> String {
    codegen::render_trait_declaration(node)
}

fn render_parameter(param: &Parameter) -> String {
    let mut out = String::new();
    if param.variadic {
        out.push_str("...");
    }
    if let Some(declared_type) = &param.declared_type {
        out.push_str(declared_type);
        out.push(' ');
    }
    out.push_str(&param.name);
    if param.optional {
        out.push('?');
    }
    if let Some(default_value) = &param.default_value {
        out.push_str(" := ");
        out.push_str(&render_expression(default_value));
    }
    out
}

fn render_block(block: &BlockStatement) -> String {
    codegen::render_block(block)
}

fn render_expression(expression: &Expression) -> String {
    codegen::render_expression(expression)
}

fn is_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn exception_text(err: ZuzuRustError) -> String {
    let text = err.to_string();
    text.strip_prefix("runtime error: ")
        .unwrap_or(&text)
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|path| path.parent())
            .expect("repo root should exist")
            .to_path_buf()
    }

    fn fixture_bytes(name: &str) -> Vec<u8> {
        let path = repo_root()
            .join("t/fixtures/marshal/golden")
            .join(format!("{name}.b64"));
        decode_base64(
            &std::fs::read_to_string(path).expect("marshal golden fixture should be readable"),
        )
    }

    fn decode_base64(text: &str) -> Vec<u8> {
        let mut out = Vec::new();
        let mut buffer = 0u32;
        let mut bits = 0u8;
        for byte in text.bytes().filter(|byte| !byte.is_ascii_whitespace()) {
            if byte == b'=' {
                break;
            }
            let value = match byte {
                b'A'..=b'Z' => byte - b'A',
                b'a'..=b'z' => byte - b'a' + 26,
                b'0'..=b'9' => byte - b'0' + 52,
                b'+' => 62,
                b'/' => 63,
                _ => panic!("invalid base64 byte {byte}"),
            };
            buffer = (buffer << 6) | value as u32;
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                out.push(((buffer >> bits) & 0xff) as u8);
            }
        }
        out
    }

    #[test]
    fn round_trip_preserves_self_cyclic_array_identity() {
        let runtime = Runtime::from_repo_root(&repo_root());
        let cell = Rc::new(RefCell::new(Value::Array(Vec::new())));
        let root = Value::Shared(Rc::clone(&cell));
        *cell.borrow_mut() = Value::Array(vec![root.clone()]);

        let envelope = encode_envelope(&runtime, &root).expect("cycle should encode");
        let bytes = encode_one(&envelope).expect("cycle envelope should encode");
        let loaded = load_binary(&runtime, &bytes).expect("cycle should load");

        let Value::Shared(loaded_cell) = loaded else {
            panic!("loaded cycle root should be shared");
        };
        let loaded_items = loaded_cell.borrow();
        let Value::Array(items) = &*loaded_items else {
            panic!("loaded cycle root should be an array");
        };
        let Value::Shared(first_item) = &items[0] else {
            panic!("loaded cycle item should be shared");
        };
        assert!(Rc::ptr_eq(&loaded_cell, first_item));
    }

    #[test]
    fn loads_perl_phase25_data_golden_fixtures() {
        let runtime = Runtime::from_repo_root(&repo_root());

        assert!(matches!(
            load_binary(&runtime, &fixture_bytes("scalar-null")).expect("scalar fixture"),
            Value::Null
        ));

        let array_cycle =
            load_binary(&runtime, &fixture_bytes("array-cycle")).expect("array fixture");
        let Value::Shared(array_cell) = array_cycle else {
            panic!("array-cycle fixture should load as shared array");
        };
        let array_items = array_cell.borrow();
        let Value::Array(items) = &*array_items else {
            panic!("array-cycle fixture should load as array");
        };
        let Value::Shared(first_item) = &items[0] else {
            panic!("array-cycle first item should be shared");
        };
        assert!(Rc::ptr_eq(&array_cell, first_item));

        let dict_pairlist =
            load_binary(&runtime, &fixture_bytes("dict-pairlist")).expect("dict fixture");
        let Value::Shared(dict_cell) = dict_pairlist else {
            panic!("dict-pairlist fixture should load as shared value");
        };
        assert!(matches!(&*dict_cell.borrow(), Value::Array(values) if values.len() == 2));

        let time_path = load_binary(&runtime, &fixture_bytes("time-path")).expect("time fixture");
        let Value::Shared(time_path_cell) = time_path else {
            panic!("time-path fixture should load as shared array");
        };
        let borrowed = time_path_cell.borrow();
        let Value::Array(values) = &*borrowed else {
            panic!("time-path fixture root should be an array");
        };
        assert!(
            matches!(&values[0], Value::Object(object) if object.borrow().class.name == "Time")
        );
        assert!(
            matches!(&values[1], Value::Object(object) if object.borrow().class.name == "Path")
        );
    }

    #[test]
    fn loads_perl_object_instance_golden_fixture() {
        let runtime = Runtime::from_repo_root(&repo_root());
        let object =
            load_binary(&runtime, &fixture_bytes("object-instance")).expect("object fixture");
        let Value::Object(object) = object else {
            panic!("object-instance fixture should load as object");
        };
        assert_eq!(object.borrow().class.name, "GoldenBox");
        let label = runtime
            .call_object_method(&object, "label", &[], Vec::new())
            .expect("loaded object method should run");
        assert!(matches!(label, Value::String(value) if value == "Ada:box"));
    }

    #[test]
    fn loads_perl_code_table_golden_fixtures() {
        let runtime = Runtime::from_repo_root(&repo_root());

        let function = load_binary(&runtime, &fixture_bytes("function")).expect("function fixture");
        let result = runtime
            .call_value(function, vec![Value::Number(41.0)], Vec::new())
            .expect("loaded function should run");
        assert!(matches!(result, Value::Number(value) if value == 42.0));

        let class = load_binary(&runtime, &fixture_bytes("class")).expect("class fixture");
        let object = runtime
            .call_value(
                class,
                Vec::new(),
                vec![("x".to_owned(), Value::Number(1.0))],
            )
            .expect("loaded class should construct");
        let Value::Object(object) = object else {
            panic!("loaded class constructor should return object");
        };
        let total = runtime
            .call_object_method(&object, "total", &[Value::Number(1.0)], Vec::new())
            .expect("loaded class method should run");
        assert!(matches!(total, Value::Number(value) if value == 42.0));

        let trait_value = load_binary(&runtime, &fixture_bytes("trait")).expect("trait fixture");
        assert!(
            matches!(trait_value, Value::Trait(trait_value) if trait_value.name == "GoldenLabelled")
        );
    }

    #[test]
    fn loader_accepts_weak_storage_records_in_stored_positions() {
        let runtime = Runtime::from_repo_root(&repo_root());
        let root_weak = test_envelope(
            CborValue::Array(vec![CborValue::Integer(1), CborValue::Null]),
            Vec::new(),
        );
        let err = match load_binary(&runtime, &root_weak) {
            Ok(_) => panic!("weak root should fail"),
            Err(err) => err,
        };
        assert!(err
            .to_string()
            .contains("Envelope root weak storage record is not allowed here"));

        let stored_weak = test_envelope(
            CborValue::Array(vec![CborValue::Integer(0), CborValue::Integer(0)]),
            vec![CborValue::Array(vec![
                CborValue::Integer(KIND_ARRAY),
                CborValue::Array(vec![CborValue::Array(vec![
                    CborValue::Integer(1),
                    CborValue::Null,
                ])]),
            ])],
        );
        let value = load_binary(&runtime, &stored_weak).expect("weak array item should load");
        let Value::Shared(array) = value else {
            panic!("weak item fixture root should be shared array");
        };
        assert!(matches!(
            &*array.borrow(),
            Value::Array(values)
                if values.len() == 1
                    && matches!(values[0].resolve_weak_value(), Value::Null)
                    && values[0].is_weak_value()
        ));
    }

    #[test]
    fn weak_scalar_collection_entries_dump_as_weak_records() {
        let runtime = Runtime::from_repo_root(&repo_root());
        let root = Value::Array(vec![Value::Boolean(true).stored_with_weak_policy(true)]);
        let envelope = encode_envelope(&runtime, &root).expect("weak scalar should encode");

        let CborValue::Tag { value, .. } = envelope else {
            panic!("marshal envelope should be tagged");
        };
        let CborValue::Array(envelope_items) = *value else {
            panic!("marshal envelope payload should be an array");
        };
        let CborValue::Array(objects) = &envelope_items[4] else {
            panic!("marshal envelope should include an object table");
        };
        let CborValue::Array(entry) = &objects[0] else {
            panic!("array object table entry should be an array");
        };
        let CborValue::Array(payload) = &entry[1] else {
            panic!("array object payload should be an array");
        };

        assert!(matches!(
            &payload[0],
            CborValue::Array(record)
                if record.len() == 2
                    && matches!(record[0], CborValue::Integer(1))
                    && matches!(record[1], CborValue::Bool(true))
        ));
    }

    fn test_envelope(root: CborValue, objects: Vec<CborValue>) -> Vec<u8> {
        encode_one(&CborValue::Tag {
            tag: 55_799,
            value: Box::new(CborValue::Array(vec![
                CborValue::Text("ZUZU-MARSHAL".to_owned()),
                CborValue::Integer(1),
                CborValue::Map(Vec::new()),
                root,
                CborValue::Array(objects),
                CborValue::Array(Vec::new()),
            ])),
        })
        .expect("test envelope should encode")
    }
}
