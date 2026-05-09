use std::collections::HashMap;

use crate::{Result, ZuzuRustError};

use super::super::{Runtime, Value};
use super::common::{
    expect_function, expect_pair_like, make_iterator, require_arity, require_arity_range,
    sorted_keys, stored_arg,
};

impl Runtime {
    pub(in crate::runtime) fn call_dict_method(
        &self,
        values: &mut HashMap<String, Value>,
        name: &str,
        args: &[Value],
    ) -> Result<Value> {
        match name {
            "keys" => {
                require_arity(name, args, 0)?;
                Ok(Value::Set(
                    sorted_keys(values).into_iter().map(Value::String).collect(),
                ))
            }
            "values" => {
                require_arity(name, args, 0)?;
                Ok(Value::Bag(
                    sorted_keys(values)
                        .into_iter()
                        .map(|key| values.get(&key).cloned().unwrap_or(Value::Null))
                        .collect(),
                ))
            }
            "enumerate" => {
                require_arity(name, args, 0)?;
                Ok(Value::Bag(
                    sorted_keys(values)
                        .into_iter()
                        .map(|key| {
                            Value::Pair(
                                key.clone(),
                                Box::new(values.get(&key).cloned().unwrap_or(Value::Null)),
                            )
                        })
                        .collect(),
                ))
            }
            "has" | "exists" | "contains" => {
                require_arity(name, args, 1)?;
                Ok(Value::Boolean(values.contains_key(&args[0].render())))
            }
            "defined" => {
                require_arity(name, args, 1)?;
                let key = args[0].render();
                Ok(Value::Boolean(
                    values
                        .get(&key)
                        .map(|value| !matches!(value.resolve_weak_value(), Value::Null))
                        .unwrap_or(false),
                ))
            }
            "get" => {
                require_arity_range(name, args, 1, 2)?;
                let key = args[0].render();
                Ok(values
                    .get(&key)
                    .cloned()
                    .unwrap_or_else(|| args.get(1).cloned().unwrap_or(Value::Null)))
            }
            "add" => {
                if args.len() == 2 {
                    values.insert(args[0].render(), args[1].clone().into_shared_if_composite());
                } else {
                    for pair in args {
                        if let Value::Array(_) = pair {
                            if let Ok((key, value)) = expect_pair_like(pair) {
                                values.insert(key, value.into_shared_if_composite());
                            }
                        }
                    }
                }
                Ok(Value::Dict(values.clone()))
            }
            "add_weak" => {
                require_arity(name, args, 2)?;
                values.insert(args[0].render(), stored_arg(&args[1], true));
                Ok(Value::Dict(values.clone()))
            }
            "set" => {
                require_arity(name, args, 2)?;
                values.insert(args[0].render(), args[1].clone().into_shared_if_composite());
                Ok(Value::Dict(values.clone()))
            }
            "set_weak" => {
                require_arity(name, args, 2)?;
                values.insert(args[0].render(), stored_arg(&args[1], true));
                Ok(Value::Dict(values.clone()))
            }
            "kv" => {
                require_arity(name, args, 0)?;
                let mut out = Vec::new();
                for key in sorted_keys(values) {
                    out.push(Value::String(key.clone()));
                    out.push(values.get(&key).cloned().unwrap_or(Value::Null));
                }
                Ok(Value::Array(out))
            }
            "sorted_keys" => {
                require_arity(name, args, 0)?;
                Ok(Value::Array(
                    sorted_keys(values).into_iter().map(Value::String).collect(),
                ))
            }
            "remove" => {
                require_arity(name, args, 1)?;
                match &args[0] {
                    Value::Function(func) => {
                        let keys = sorted_keys(values);
                        for key in keys {
                            let pair = Value::Pair(
                                key.clone(),
                                Box::new(values.get(&key).cloned().unwrap_or(Value::Null)),
                            );
                            if self.predicate_callback(func, pair)? {
                                values.remove(&key);
                            }
                        }
                    }
                    Value::Pair(_, _) => {}
                    other => {
                        values.remove(&other.render());
                    }
                }
                Ok(Value::Dict(values.clone()))
            }
            "length" | "count" => {
                require_arity(name, args, 0)?;
                Ok(Value::Number(values.len() as f64))
            }
            "empty" => {
                require_arity(name, args, 0)?;
                Ok(Value::Boolean(values.is_empty()))
            }
            "clear" => {
                require_arity(name, args, 0)?;
                values.clear();
                Ok(Value::Dict(values.clone()))
            }
            "to_Array" => {
                require_arity(name, args, 0)?;
                Ok(Value::Array(
                    sorted_keys(values)
                        .into_iter()
                        .map(|key| {
                            Value::Pair(
                                key.clone(),
                                Box::new(values.get(&key).cloned().unwrap_or(Value::Null)),
                            )
                        })
                        .collect(),
                ))
            }
            "to_Iterator" => {
                require_arity(name, args, 0)?;
                Ok(make_iterator(
                    sorted_keys(values).into_iter().map(Value::String).collect(),
                ))
            }
            "for_each_pair" => {
                require_arity(name, args, 1)?;
                let func =
                    expect_function(&args[0], "Collection method expects a function callback")?;
                for key in sorted_keys(values) {
                    let pair = Value::Pair(
                        key.clone(),
                        Box::new(values.get(&key).cloned().unwrap_or(Value::Null)),
                    );
                    let _ = self.call_function(&func, vec![pair], Vec::new())?;
                }
                Ok(Value::Dict(values.clone()))
            }
            "for_each_key" => {
                require_arity(name, args, 1)?;
                let func =
                    expect_function(&args[0], "Collection method expects a function callback")?;
                for key in sorted_keys(values) {
                    let _ = self.call_function(&func, vec![Value::String(key)], Vec::new())?;
                }
                Ok(Value::Dict(values.clone()))
            }
            "for_each_value" => {
                require_arity(name, args, 1)?;
                let func =
                    expect_function(&args[0], "Collection method expects a function callback")?;
                for key in sorted_keys(values) {
                    let _ = self.call_function(
                        &func,
                        vec![values.get(&key).cloned().unwrap_or(Value::Null)],
                        Vec::new(),
                    )?;
                }
                Ok(Value::Dict(values.clone()))
            }
            other => Err(ZuzuRustError::thrown(format!(
                "unsupported Dict method '{}'",
                other
            ))),
        }
    }
}
