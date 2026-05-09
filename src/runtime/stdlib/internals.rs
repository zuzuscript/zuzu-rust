use std::collections::HashMap;

use super::super::collection::common::{require_arity, require_arity_range};
use super::super::{Runtime, Value};
use crate::error::Result;

pub(super) fn exports() -> HashMap<String, Value> {
    let mut exports = HashMap::new();
    for func in [
        "class_name",
        "object_slots",
        "ansi_esc",
        "ref_id",
        "setprop",
        "setupperprop",
        "getprop",
        "getupperprop",
        "make_instance",
        "load_module",
        "to_binary",
    ] {
        exports.insert(func.to_owned(), Value::native_function(func.to_owned()));
    }
    for (export_name, native_name) in [
        ("to_String", "std/internals.to_String"),
        ("to_Number", "std/internals.to_Number"),
        ("to_Boolean", "std/internals.to_Boolean"),
        ("to_Regexp", "std/internals.to_Regexp"),
    ] {
        exports.insert(
            export_name.to_owned(),
            Value::native_function(native_name.to_owned()),
        );
    }
    exports
}

pub(super) fn call(
    runtime: &Runtime,
    name: &str,
    args: &[Value],
    named_args: &[(String, Value)],
) -> Option<Result<Value>> {
    if !is_internal_function(name) {
        return None;
    }
    if !named_args.is_empty() {
        return Some(Err(crate::error::ZuzuRustError::runtime(
            "named call arguments are not implemented for native functions",
        )));
    }
    let value = match name {
        "class_name" => require_arity(name, args, 1).map(|_| match &args[0] {
            Value::Object(object) => Value::String(object.borrow().class.name.clone()),
            Value::Pair(_, _) => Value::String("Pair".to_owned()),
            _ => Value::Null,
        }),
        "object_slots" => require_arity(name, args, 1).map(|_| match &args[0] {
            Value::Object(object) => {
                let mut map = HashMap::new();
                for (key, value) in &object.borrow().fields {
                    if !key.starts_with('_') {
                        map.insert(key.clone(), value.clone());
                    }
                }
                Value::Dict(map)
            }
            Value::Pair(key, value) => {
                let mut map = HashMap::new();
                map.insert(
                    "pair".to_owned(),
                    Value::Array(vec![Value::String(key.clone()), (**value).clone()]),
                );
                Value::Dict(map)
            }
            _ => Value::Null,
        }),
        "ansi_esc" => require_arity(name, args, 0).map(|_| Value::String("\u{1b}".to_owned())),
        "ref_id" => require_arity(name, args, 1).map(|_| match &args[0] {
            Value::Shared(value) => Value::String(format!("{:p}", std::rc::Rc::as_ptr(value))),
            Value::Object(object) => Value::String(format!("{:p}", std::rc::Rc::as_ptr(object))),
            Value::Ref(reference) => Value::String(format!("{:p}", std::rc::Rc::as_ptr(reference))),
            Value::Array(_)
            | Value::Set(_)
            | Value::Bag(_)
            | Value::Dict(_)
            | Value::PairList(_)
            | Value::Pair(_, _) => Value::String(format!(
                "{}:{}",
                args[0].type_name(),
                runtime.render_value(&args[0]).unwrap_or_default()
            )),
            _ => Value::Null,
        }),
        "setupperprop" => require_arity(name, args, 3).and_then(|_| {
            let level = runtime.value_to_number(&args[0])? as usize;
            let key = match &args[1] {
                Value::String(value) => value.clone(),
                _ => {
                    return Err(crate::error::ZuzuRustError::thrown(
                        "setupperprop key must be String",
                    ))
                }
            };
            Ok(runtime.set_special_prop_at_level(level, &key, args[2].clone()))
        }),
        "setprop" => require_arity(name, args, 2).and_then(|_| {
            let key = match &args[0] {
                Value::String(value) => value.clone(),
                _ => {
                    return Err(crate::error::ZuzuRustError::thrown(
                        "setprop key must be String",
                    ))
                }
            };
            Ok(runtime.set_special_prop(&key, args[1].clone()))
        }),
        "getprop" => require_arity(name, args, 1).and_then(|_| {
            let key = match &args[0] {
                Value::String(value) => value.clone(),
                _ => {
                    return Err(crate::error::ZuzuRustError::thrown(
                        "getprop key must be String",
                    ))
                }
            };
            Ok(runtime.get_special_prop(&key))
        }),
        "getupperprop" => require_arity(name, args, 2).and_then(|_| {
            let level = runtime.value_to_number(&args[0])? as usize;
            let key = match &args[1] {
                Value::String(value) => value.clone(),
                _ => {
                    return Err(crate::error::ZuzuRustError::thrown(
                        "getupperprop key must be String",
                    ))
                }
            };
            Ok(runtime.get_special_prop_at_level(level, &key))
        }),
        "make_instance" => require_arity_range(name, args, 1, 2).and_then(|_| {
            let class = match &args[0] {
                Value::UserClass(class) => std::rc::Rc::clone(class),
                _ => {
                    return Err(crate::error::ZuzuRustError::thrown(
                        "make_instance expects a user class",
                    ))
                }
            };
            let slots = match args.get(1) {
                None | Some(Value::Null) => HashMap::new(),
                Some(Value::Dict(map)) => map.clone(),
                Some(_) => {
                    return Err(crate::error::ZuzuRustError::thrown(
                        "make_instance slot values must be Dict",
                    ))
                }
            };
            runtime.make_user_instance_without_build(class, slots)
        }),
        "load_module" => require_arity_range(name, args, 1, 2).and_then(|_| {
            let module_name = match &args[0] {
                Value::String(value) => value,
                _ => {
                    return Err(crate::error::ZuzuRustError::thrown(
                        "load_module module must be String",
                    ))
                }
            };
            let exports = runtime.load_module_exports(module_name)?;

            if let Some(symbol) = args.get(1) {
                let symbol_name = match symbol {
                    Value::String(value) => value,
                    _ => {
                        return Err(crate::error::ZuzuRustError::thrown(
                            "load_module symbol must be String",
                        ))
                    }
                };
                return exports
                    .get(symbol_name)
                    .cloned()
                    .ok_or_else(|| {
                        crate::error::ZuzuRustError::runtime(format!(
                            "module '{}' has no export '{}'",
                            module_name, symbol_name
                        ))
                    })
                    .and_then(|value| runtime.normalize_value(value));
            }

            let mut map = HashMap::new();
            for (export_name, value) in exports {
                map.insert(export_name, runtime.normalize_value(value)?);
            }
            Ok(Value::Dict(map))
        }),
        "to_binary" => require_arity(name, args, 1).and_then(|_| match &args[0] {
            Value::BinaryString(bytes) => Ok(Value::BinaryString(bytes.clone())),
            value => Ok(Value::BinaryString(
                runtime.render_value(value)?.into_bytes(),
            )),
        }),
        "to_string" => require_arity(name, args, 1).and_then(|_| match &args[0] {
            Value::BinaryString(bytes) => String::from_utf8(bytes.clone())
                .map(Value::String)
                .map_err(|_| {
                    crate::error::ZuzuRustError::runtime("BinaryString is not valid UTF-8")
                }),
            value => Ok(Value::String(runtime.render_value(value)?)),
        }),
        "std/internals.to_String" => require_arity(name, args, 1)
            .and_then(|_| Ok(Value::String(runtime.value_to_operator_string(&args[0])?))),
        "std/internals.to_Number" => require_arity(name, args, 1)
            .and_then(|_| Ok(Value::Number(runtime.value_to_number(&args[0])?))),
        "std/internals.to_Boolean" => require_arity(name, args, 1)
            .and_then(|_| Ok(Value::Boolean(runtime.value_is_truthy(&args[0])?))),
        "std/internals.to_Regexp" => require_arity(name, args, 1).and_then(|_| match &args[0] {
            Value::Regex(pattern, flags) => Ok(Value::Regex(pattern.clone(), flags.clone())),
            value => {
                let pattern = runtime.value_to_operator_string(value)?;
                runtime.compile_regex(&pattern, "")?;
                Ok(Value::Regex(pattern, String::new()))
            }
        }),
        _ => unreachable!(),
    };
    Some(value)
}

fn is_internal_function(name: &str) -> bool {
    matches!(
        name,
        "class_name"
            | "object_slots"
            | "ansi_esc"
            | "ref_id"
            | "setprop"
            | "setupperprop"
            | "getprop"
            | "getupperprop"
            | "make_instance"
            | "load_module"
            | "to_binary"
            | "to_string"
            | "std/internals.to_String"
            | "std/internals.to_Number"
            | "std/internals.to_Boolean"
            | "std/internals.to_Regexp"
    )
}
