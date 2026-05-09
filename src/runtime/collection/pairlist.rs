use crate::{Result, ZuzuRustError};

use super::super::{Runtime, Value};
use super::common::{
    expect_function, expect_pair_like, make_iterator, require_arity, require_arity_range,
    stored_arg,
};

impl Runtime {
    pub(in crate::runtime) fn call_pairlist_method(
        &self,
        values: &mut Vec<(String, Value)>,
        name: &str,
        args: &[Value],
    ) -> Result<Value> {
        match name {
            "keys" => {
                require_arity(name, args, 0)?;
                Ok(Value::Array(
                    values
                        .iter()
                        .map(|(key, _)| Value::String(key.clone()))
                        .collect(),
                ))
            }
            "values" => {
                require_arity(name, args, 0)?;
                Ok(Value::Array(
                    values.iter().map(|(_, value)| value.clone()).collect(),
                ))
            }
            "enumerate" => {
                require_arity(name, args, 0)?;
                Ok(Value::Bag(
                    values
                        .iter()
                        .map(|(key, value)| Value::Pair(key.clone(), Box::new(value.clone())))
                        .collect(),
                ))
            }
            "has" | "exists" => {
                require_arity(name, args, 1)?;
                let key = args[0].render();
                Ok(Value::Boolean(
                    values.iter().any(|(existing, _)| existing == &key),
                ))
            }
            "defined" => {
                require_arity(name, args, 1)?;
                let key = args[0].render();
                Ok(Value::Boolean(values.iter().any(|(existing, value)| {
                    existing == &key && !matches!(value.resolve_weak_value(), Value::Null)
                })))
            }
            "get" => {
                require_arity_range(name, args, 1, 2)?;
                let key = args[0].render();
                for (existing, value) in values.iter() {
                    if existing == &key {
                        return Ok(value.clone());
                    }
                }
                Ok(args.get(1).cloned().unwrap_or(Value::Null))
            }
            "get_all" | "all" => {
                require_arity(name, args, 1)?;
                let key = args[0].render();
                Ok(Value::Array(
                    values
                        .iter()
                        .filter(|(existing, _)| existing == &key)
                        .map(|(_, value)| value.clone())
                        .collect(),
                ))
            }
            "add" => {
                if args.len() == 2 {
                    values.push((args[0].render(), args[1].clone().into_shared_if_composite()));
                } else {
                    for pair in args {
                        if let Ok((key, value)) = expect_pair_like(pair) {
                            values.push((key, value.into_shared_if_composite()));
                        }
                    }
                }
                Ok(Value::PairList(values.clone()))
            }
            "add_weak" => {
                require_arity(name, args, 2)?;
                values.push((args[0].render(), stored_arg(&args[1], true)));
                Ok(Value::PairList(values.clone()))
            }
            "set" => {
                require_arity(name, args, 2)?;
                values.push((args[0].render(), args[1].clone().into_shared_if_composite()));
                Ok(Value::PairList(values.clone()))
            }
            "set_weak" => {
                require_arity(name, args, 2)?;
                values.push((args[0].render(), stored_arg(&args[1], true)));
                Ok(Value::PairList(values.clone()))
            }
            "kv" => {
                require_arity(name, args, 0)?;
                let mut out = Vec::new();
                for (key, value) in values.iter() {
                    out.push(Value::String(key.clone()));
                    out.push(value.clone());
                }
                Ok(Value::Array(out))
            }
            "sorted_keys" => {
                require_arity(name, args, 0)?;
                let mut keys: Vec<_> = values.iter().map(|(key, _)| key.clone()).collect();
                keys.sort();
                Ok(Value::Array(keys.into_iter().map(Value::String).collect()))
            }
            "remove" => {
                require_arity(name, args, 1)?;
                match &args[0] {
                    Value::Function(func) => {
                        values.retain(|(key, value)| {
                            !self
                                .predicate_callback(
                                    func,
                                    Value::Pair(key.clone(), Box::new(value.clone())),
                                )
                                .unwrap_or(false)
                        });
                    }
                    Value::Pair(_, _) => {}
                    other => {
                        let key = other.render();
                        values.retain(|(existing, _)| existing != &key);
                    }
                }
                Ok(Value::PairList(values.clone()))
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
                Ok(Value::PairList(values.clone()))
            }
            "to_Array" => {
                require_arity(name, args, 0)?;
                Ok(Value::Array(
                    values
                        .iter()
                        .map(|(key, value)| Value::Pair(key.clone(), Box::new(value.clone())))
                        .collect(),
                ))
            }
            "to_Iterator" => {
                require_arity(name, args, 0)?;
                Ok(make_iterator(
                    values
                        .iter()
                        .map(|(key, _)| Value::String(key.clone()))
                        .collect(),
                ))
            }
            "for_each_pair" => {
                require_arity(name, args, 1)?;
                let func =
                    expect_function(&args[0], "Collection method expects a function callback")?;
                for (key, value) in values.iter() {
                    let _ = self.call_function(
                        &func,
                        vec![Value::Pair(key.clone(), Box::new(value.clone()))],
                        Vec::new(),
                    )?;
                }
                Ok(Value::PairList(values.clone()))
            }
            "for_each_key" => {
                require_arity(name, args, 1)?;
                let func =
                    expect_function(&args[0], "Collection method expects a function callback")?;
                for (key, _) in values.iter() {
                    let _ =
                        self.call_function(&func, vec![Value::String(key.clone())], Vec::new())?;
                }
                Ok(Value::PairList(values.clone()))
            }
            "for_each_value" => {
                require_arity(name, args, 1)?;
                let func =
                    expect_function(&args[0], "Collection method expects a function callback")?;
                for (_, value) in values.iter() {
                    let _ = self.call_function(&func, vec![value.clone()], Vec::new())?;
                }
                Ok(Value::PairList(values.clone()))
            }
            other => Err(ZuzuRustError::thrown(format!(
                "unsupported PairList method '{}'",
                other
            ))),
        }
    }
}
