use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use super::super::{
    FieldSpec, MethodValue, ObjectValue, Runtime, TraitValue, UserClassValue, Value,
};
use crate::error::{Result, ZuzuRustError};

pub(super) fn exports() -> HashMap<String, Value> {
    HashMap::from([(
        "BigNum".to_owned(),
        Value::builtin_class("BigNum".to_owned()),
    )])
}

pub(super) fn call_class_method(
    runtime: &Runtime,
    class_name: &str,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    if class_name != "BigNum" {
        return None;
    }
    Some(match name {
        "from_dec" => make_from_dec(runtime, args),
        "from_hex" => make_from_hex(runtime, args),
        _ => return None,
    })
}

pub(super) fn call_object_method(
    runtime: &Runtime,
    class_name: &str,
    builtin_value: &Value,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    if class_name != "BigNum" {
        return None;
    }
    let Value::Dict(fields) = builtin_value else {
        return Some(Err(ZuzuRustError::runtime("BigNum internal state missing")));
    };
    let value = fields
        .get("value")
        .and_then(|v| v.to_number().ok())
        .unwrap_or(0.0);
    let text = fields
        .get("text")
        .map(render_string)
        .unwrap_or_else(|| value.to_string());
    let is_int = fields.get("is_int").map(Value::is_truthy).unwrap_or(false);
    Some(match name {
        "is_int" => Ok(Value::Boolean(is_int)),
        "bcmp" => compare_to(runtime, value, args),
        "beq" => compare_bool(runtime, value, args, |cmp| cmp == 0),
        "bne" => compare_bool(runtime, value, args, |cmp| cmp != 0),
        "blt" => compare_bool(runtime, value, args, |cmp| cmp < 0),
        "ble" => compare_bool(runtime, value, args, |cmp| cmp <= 0),
        "bgt" => compare_bool(runtime, value, args, |cmp| cmp > 0),
        "bge" => compare_bool(runtime, value, args, |cmp| cmp >= 0),
        "babs" => Ok(make_bignum(
            value.abs(),
            trim_decimal(&value.abs().to_string()),
            is_int,
        )),
        "bneg" => Ok(make_bignum(
            -value,
            trim_decimal(&(-value).to_string()),
            is_int,
        )),
        "binv" => Ok(make_bignum_auto(1.0 / value)),
        "bsin" => Ok(make_bignum(
            value.sin(),
            trim_decimal(&value.sin().to_string()),
            false,
        )),
        "bcos" => Ok(make_bignum_auto(value.cos())),
        "btan" => Ok(make_bignum(
            value.tan(),
            trim_decimal(&value.tan().to_string()),
            false,
        )),
        "bsqrt" => Ok(make_bignum_auto(value.sqrt())),
        "bround" => Ok(make_bignum(
            value.round(),
            trim_decimal(&value.round().to_string()),
            true,
        )),
        "bfloor" => Ok(make_bignum(
            value.floor(),
            trim_decimal(&value.floor().to_string()),
            true,
        )),
        "bceil" => Ok(make_bignum(
            value.ceil(),
            trim_decimal(&value.ceil().to_string()),
            true,
        )),
        "badd" => binary_num(
            runtime,
            value,
            args,
            |left, right| left + right,
            Some(false),
        ),
        "bsub" => binary_num(
            runtime,
            value,
            args,
            |left, right| left - right,
            Some(false),
        ),
        "bmul" => binary_num(
            runtime,
            value,
            args,
            |left, right| left * right,
            Some(false),
        ),
        "bdiv" => binary_num(runtime, value, args, |left, right| left / right, None),
        "bmod" => binary_num(runtime, value, args, |left, right| left % right, None),
        "bpow" => binary_num(runtime, value, args, |left, right| left.powf(right), None),
        "to_hex" => Ok(Value::String(format!("0x{:x}", value.trunc() as i64))),
        "to_dec" | "to_String" => Ok(if is_int {
            Value::Number(value.trunc())
        } else {
            Value::String(trim_decimal(&text))
        }),
        "to_Number" => Ok(Value::Number(value)),
        _ => return None,
    })
}

pub(super) fn has_builtin_object_method(class_name: &str, name: &str) -> bool {
    class_name == "BigNum"
        && matches!(
            name,
            "is_int"
                | "bcmp"
                | "beq"
                | "bne"
                | "blt"
                | "ble"
                | "bgt"
                | "bge"
                | "babs"
                | "bneg"
                | "binv"
                | "bsin"
                | "bcos"
                | "btan"
                | "bsqrt"
                | "bround"
                | "bfloor"
                | "bceil"
                | "badd"
                | "bsub"
                | "bmul"
                | "bdiv"
                | "bmod"
                | "bpow"
                | "to_hex"
                | "to_dec"
                | "to_String"
                | "to_Number"
        )
}

fn class() -> Rc<UserClassValue> {
    Rc::new(UserClassValue {
        name: "BigNum".to_owned(),
        base: None,
        traits: Vec::<Rc<TraitValue>>::new(),
        fields: vec![
            FieldSpec {
                name: "value".to_owned(),
                declared_type: Some("Number".to_owned()),
                mutable: true,
                accessors: Vec::new(),
                default_value: None,
                is_weak_storage: false,
            },
            FieldSpec {
                name: "text".to_owned(),
                declared_type: Some("String".to_owned()),
                mutable: true,
                accessors: Vec::new(),
                default_value: None,
                is_weak_storage: false,
            },
            FieldSpec {
                name: "is_int".to_owned(),
                declared_type: Some("Boolean".to_owned()),
                mutable: true,
                accessors: Vec::new(),
                default_value: None,
                is_weak_storage: false,
            },
        ],
        methods: HashMap::<String, Rc<MethodValue>>::new(),
        static_methods: HashMap::<String, Rc<MethodValue>>::new(),
        nested_classes: HashMap::new(),
        source_decl: None,
        closure_env: None,
    })
}

fn make_from_dec(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    let text = args
        .first()
        .map(|value| runtime.render_value(value))
        .transpose()?
        .unwrap_or_else(|| "0".to_owned());
    let trimmed = text.trim();
    let parsed = trimmed.parse::<f64>().unwrap_or(0.0);
    Ok(make_bignum(
        parsed,
        if trimmed.is_empty() {
            "0".to_owned()
        } else {
            trimmed.to_owned()
        },
        !trimmed.contains(['.', 'e', 'E']),
    ))
}

fn make_from_hex(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    let text = args
        .first()
        .map(|value| runtime.render_value(value))
        .transpose()?
        .unwrap_or_else(|| "0".to_owned());
    let trimmed = text
        .trim()
        .trim_start_matches("0x")
        .trim_start_matches("0X");
    let parsed =
        i64::from_str_radix(if trimmed.is_empty() { "0" } else { trimmed }, 16).unwrap_or(0) as f64;
    Ok(make_bignum(parsed, trim_decimal(&parsed.to_string()), true))
}

fn make_bignum(value: f64, text: String, is_int: bool) -> Value {
    let fields = HashMap::from([
        ("value".to_owned(), Value::Number(value)),
        ("text".to_owned(), Value::String(text.clone())),
        ("is_int".to_owned(), Value::Boolean(is_int)),
    ]);
    Value::Object(Rc::new(RefCell::new(ObjectValue {
        class: class(),
        fields: fields.clone(),
        weak_fields: std::collections::HashSet::new(),
        builtin_value: Some(Value::Dict(fields)),
    })))
}

fn binary_num(
    _runtime: &Runtime,
    left: f64,
    args: &[Value],
    op: impl FnOnce(f64, f64) -> f64,
    is_int: Option<bool>,
) -> Result<Value> {
    let right = args.first().map(coerce_other).transpose()?.unwrap_or(0.0);
    let value = op(left, right);
    Ok(match is_int {
        Some(is_int) => make_bignum(value, trim_decimal(&value.to_string()), is_int),
        None => make_bignum_auto(value),
    })
}

fn compare_to(_runtime: &Runtime, left: f64, args: &[Value]) -> Result<Value> {
    let right = args.first().map(coerce_other).transpose()?.unwrap_or(0.0);
    Ok(Value::Number(if left < right {
        -1.0
    } else if left > right {
        1.0
    } else {
        0.0
    }))
}

fn compare_bool(
    _runtime: &Runtime,
    left: f64,
    args: &[Value],
    test: impl FnOnce(i32) -> bool,
) -> Result<Value> {
    let right = args.first().map(coerce_other).transpose()?.unwrap_or(0.0);
    let cmp = if left < right {
        -1
    } else if left > right {
        1
    } else {
        0
    };
    Ok(Value::Boolean(test(cmp)))
}

fn coerce_other(value: &Value) -> Result<f64> {
    match value {
        Value::Object(object) if object.borrow().class.name == "BigNum" => Ok(object
            .borrow()
            .fields
            .get("value")
            .and_then(|v| v.to_number().ok())
            .unwrap_or(0.0)),
        other => other.to_number(),
    }
}

fn render_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        other => other.render(),
    }
}

fn trim_decimal(text: &str) -> String {
    if text.contains('.') {
        text.trim_end_matches('0').trim_end_matches('.').to_owned()
    } else {
        text.to_owned()
    }
}

fn make_bignum_auto(value: f64) -> Value {
    make_bignum(
        value,
        trim_decimal(&value.to_string()),
        value.fract() == 0.0,
    )
}
