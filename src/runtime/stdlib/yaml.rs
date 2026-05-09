use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::rc::Rc;

use serde_yaml::{Mapping as YamlMapping, Value as YamlValue};

use super::super::{
    FieldSpec, MethodValue, ObjectValue, Runtime, TraitValue, UserClassValue, Value,
};
use super::io::{path_buf_from_value, resolve_fs_path};
use crate::error::{Result, ZuzuRustError};

pub(super) fn exports() -> HashMap<String, Value> {
    HashMap::from([("YAML".to_owned(), Value::builtin_class("YAML".to_owned()))])
}

pub(super) fn construct_yaml(_args: Vec<Value>, named_args: Vec<(String, Value)>) -> Result<Value> {
    let mut fields = HashMap::new();
    fields.insert("pretty".to_owned(), Value::Boolean(false));
    fields.insert("canonical".to_owned(), Value::Boolean(false));
    for (name, value) in named_args {
        fields.insert(name, value);
    }
    Ok(Value::Object(Rc::new(RefCell::new(ObjectValue {
        class: class(),
        fields: fields.clone(),
        weak_fields: std::collections::HashSet::new(),
        builtin_value: Some(Value::Dict(fields)),
    }))))
}

pub(super) fn call_object_method(
    runtime: &Runtime,
    class_name: &str,
    builtin_value: &Value,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    if class_name != "YAML" {
        return None;
    }
    let Value::Dict(fields) = builtin_value else {
        return Some(Err(ZuzuRustError::runtime("YAML internal state missing")));
    };
    let pretty = fields.get("pretty").map(Value::is_truthy).unwrap_or(false);
    let value = match name {
        "encode" => {
            let source = args.first().cloned().unwrap_or(Value::Null);
            encode_yaml(&normalize_value(&source), pretty).map(Value::String)
        }
        "decode" => match args.first() {
            Some(value) => match runtime.render_value(value) {
                Ok(text) => decode_yaml(&text),
                Err(err) => Err(err),
            },
            None => decode_yaml(""),
        },
        "dump" => dump_yaml(runtime, args, pretty),
        "load" => load_yaml(runtime, args),
        _ => return None,
    };
    Some(value)
}

fn class() -> Rc<UserClassValue> {
    Rc::new(UserClassValue {
        name: "YAML".to_owned(),
        base: None,
        traits: Vec::<Rc<TraitValue>>::new(),
        fields: vec![
            FieldSpec {
                name: "pretty".to_owned(),
                declared_type: Some("Boolean".to_owned()),
                mutable: true,
                accessors: Vec::new(),
                default_value: None,
                is_weak_storage: false,
            },
            FieldSpec {
                name: "canonical".to_owned(),
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

fn dump_yaml(runtime: &Runtime, args: &[Value], pretty: bool) -> Result<Value> {
    let Some(target) = args.first() else {
        return Err(ZuzuRustError::thrown(
            "TypeException: YAML.dump expects Path as first argument",
        ));
    };
    let path = extract_path(runtime, target, "YAML.dump")?;
    let payload = encode_yaml(
        &normalize_value(&args.get(1).cloned().unwrap_or(Value::Null)),
        pretty,
    )?;
    fs::write(path, payload).map_err(|err| ZuzuRustError::thrown(format!("dump failed: {err}")))?;
    Ok(target.clone())
}

fn load_yaml(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    let Some(target) = args.first() else {
        return Err(ZuzuRustError::thrown(
            "TypeException: YAML.load expects Path as first argument",
        ));
    };
    let path = extract_path(runtime, target, "YAML.load")?;
    let text = fs::read_to_string(path)
        .map_err(|err| ZuzuRustError::thrown(format!("load failed: {err}")))?;
    decode_yaml(&text)
}

fn extract_path(runtime: &Runtime, value: &Value, method_name: &str) -> Result<std::path::PathBuf> {
    match value {
        Value::Object(object) if object.borrow().class.name == "Path" => {
            Ok(resolve_fs_path(runtime, &path_buf_from_value(value)))
        }
        _ => Err(ZuzuRustError::thrown(format!(
            "TypeException: {method_name} expects Path as first argument"
        ))),
    }
}

fn encode_yaml(value: &Value, pretty: bool) -> Result<String> {
    let yaml_value = value_to_yaml(value)?;
    let text = serde_yaml::to_string(&yaml_value)
        .map_err(|err| ZuzuRustError::thrown(format!("encode failed: {err}")))?;
    Ok(if pretty {
        text
    } else {
        text.strip_suffix('\n').unwrap_or(&text).to_owned()
    })
}

fn decode_yaml(text: &str) -> Result<Value> {
    let yaml_value = serde_yaml::from_str::<YamlValue>(text)
        .map_err(|err| ZuzuRustError::thrown(format!("decode failed: {err}")))?;
    Ok(yaml_to_value(yaml_value))
}

fn normalize_value(value: &Value) -> Value {
    match value {
        Value::Shared(shared) => normalize_value(&shared.borrow()),
        Value::PairList(items) => {
            let mut out = HashMap::new();
            for (key, inner) in items {
                out.entry(key.clone())
                    .or_insert_with(|| normalize_value(inner));
            }
            Value::Dict(out)
        }
        Value::Set(values) | Value::Bag(values) => {
            let mut items = values.iter().map(normalize_value).collect::<Vec<_>>();
            items.sort_by_key(Value::render);
            Value::Array(items)
        }
        Value::Array(values) => Value::Array(values.iter().map(normalize_value).collect()),
        Value::Dict(map) => Value::Dict(
            map.iter()
                .map(|(key, value)| (key.clone(), normalize_value(value)))
                .collect(),
        ),
        other => other.clone(),
    }
}

fn value_to_yaml(value: &Value) -> Result<YamlValue> {
    match value {
        Value::Shared(shared) => value_to_yaml(&shared.borrow()),
        Value::Null => Ok(YamlValue::Null),
        Value::Boolean(value) => Ok(YamlValue::Bool(*value)),
        Value::Number(value) => yaml_number(*value),
        Value::String(value) => Ok(YamlValue::String(value.clone())),
        Value::BinaryString(bytes) => Ok(YamlValue::String(
            String::from_utf8_lossy(bytes).into_owned(),
        )),
        Value::Array(values) | Value::Set(values) | Value::Bag(values) => values
            .iter()
            .map(value_to_yaml)
            .collect::<Result<Vec<_>>>()
            .map(YamlValue::Sequence),
        Value::Dict(map) => {
            let mut keys = map.keys().cloned().collect::<Vec<_>>();
            keys.sort();
            let mut out = YamlMapping::new();
            for key in keys {
                out.insert(
                    YamlValue::String(key.clone()),
                    value_to_yaml(map.get(&key).unwrap_or(&Value::Null))?,
                );
            }
            Ok(YamlValue::Mapping(out))
        }
        Value::PairList(items) => {
            let mut seen = HashMap::<String, bool>::new();
            let mut out = YamlMapping::new();
            for (key, value) in items {
                if seen.contains_key(key) {
                    continue;
                }
                seen.insert(key.clone(), true);
                out.insert(YamlValue::String(key.clone()), value_to_yaml(value)?);
            }
            Ok(YamlValue::Mapping(out))
        }
        other => Ok(YamlValue::String(other.render())),
    }
}

fn yaml_number(value: f64) -> Result<YamlValue> {
    if value.is_finite()
        && value.fract() == 0.0
        && value >= i64::MIN as f64
        && value <= i64::MAX as f64
    {
        return serde_yaml::to_value(value as i64)
            .map_err(|err| ZuzuRustError::thrown(format!("encode failed: {err}")));
    }
    serde_yaml::to_value(value)
        .map_err(|err| ZuzuRustError::thrown(format!("encode failed: {err}")))
}

fn yaml_to_value(value: YamlValue) -> Value {
    match value {
        YamlValue::Null => Value::Null,
        YamlValue::Bool(value) => Value::Boolean(value),
        YamlValue::Number(value) => {
            if let Some(value) = value.as_i64() {
                Value::Number(value as f64)
            } else if let Some(value) = value.as_u64() {
                Value::Number(value as f64)
            } else if let Some(value) = value.as_f64() {
                Value::Number(value)
            } else {
                Value::Null
            }
        }
        YamlValue::String(value) => Value::String(value),
        YamlValue::Sequence(values) => {
            Value::Array(values.into_iter().map(yaml_to_value).collect())
        }
        YamlValue::Mapping(values) => {
            let mut out = HashMap::new();
            for (key, value) in values {
                out.insert(yaml_key_to_string(key), yaml_to_value(value));
            }
            Value::Dict(out)
        }
        YamlValue::Tagged(value) => yaml_to_value(value.value),
    }
}

fn yaml_key_to_string(value: YamlValue) -> String {
    match yaml_to_value(value) {
        Value::String(value) => value,
        other => other.render(),
    }
}
