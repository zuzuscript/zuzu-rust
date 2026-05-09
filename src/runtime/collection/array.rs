use crate::{Result, ZuzuRustError};

use super::super::{Runtime, Value};
use super::common::{
    collection_get, collection_product, collection_sum, expect_function, make_iterator,
    optional_count, require_arity, stored_arg, unique_values, CollectionTarget,
};

impl Runtime {
    pub(in crate::runtime) fn call_array_method(
        &self,
        values: &mut Vec<Value>,
        name: &str,
        args: &[Value],
    ) -> Result<Value> {
        match name {
            "append" | "push" | "add" => {
                values.extend(args.iter().cloned().map(Value::into_shared_if_composite));
                Ok(Value::Array(values.clone()))
            }
            "push_weak" => {
                values.extend(args.iter().map(|value| stored_arg(value, true)));
                Ok(Value::Array(values.clone()))
            }
            "pop" => {
                require_arity(name, args, 0)?;
                Ok(values.pop().unwrap_or(Value::Null))
            }
            "prepend" | "unshift" => {
                let mut out = args
                    .iter()
                    .cloned()
                    .map(Value::into_shared_if_composite)
                    .collect::<Vec<_>>();
                out.extend(values.iter().cloned());
                *values = out;
                Ok(Value::Array(values.clone()))
            }
            "unshift_weak" => {
                let mut out = args
                    .iter()
                    .map(|value| stored_arg(value, true))
                    .collect::<Vec<_>>();
                out.extend(values.iter().cloned());
                *values = out;
                Ok(Value::Array(values.clone()))
            }
            "shift" => {
                require_arity(name, args, 0)?;
                if values.is_empty() {
                    Ok(Value::Null)
                } else {
                    Ok(values.remove(0))
                }
            }
            "length" | "count" => {
                require_arity(name, args, 0)?;
                Ok(Value::Number(values.len() as f64))
            }
            "empty" | "is_empty" => {
                require_arity(name, args, 0)?;
                Ok(Value::Boolean(values.is_empty()))
            }
            "get" => Ok(collection_get(values, args)),
            "set" => {
                require_arity(name, args, 2)?;
                let index = super::common::to_index(&args[0]);
                if index >= values.len() {
                    values.resize(index, Value::Null);
                    values.push(args[1].clone().into_shared_if_composite());
                } else {
                    values[index] = args[1].clone().into_shared_if_composite();
                }
                Ok(Value::Array(values.clone()))
            }
            "set_weak" => {
                require_arity(name, args, 2)?;
                let index = super::common::to_index(&args[0]);
                let value = stored_arg(&args[1], true);
                if index >= values.len() {
                    values.resize(index, Value::Null);
                    values.push(value);
                } else {
                    values[index] = value;
                }
                Ok(Value::Array(values.clone()))
            }
            "clear" => {
                require_arity(name, args, 0)?;
                values.clear();
                Ok(Value::Array(values.clone()))
            }
            "to_Set" => Ok(Value::Set(unique_values(values))),
            "to_Bag" => Ok(Value::Bag(values.clone())),
            "to_Iterator" => {
                require_arity(name, args, 0)?;
                Ok(make_iterator(values.clone()))
            }
            "sort" => {
                require_arity(name, args, 1)?;
                let cmp = expect_function(&args[0], "Collection sort expects a function callback")?;
                let mut out = values.clone();
                out.sort_by(|left, right| {
                    self.compare_via_callback(&cmp, left.clone(), right.clone())
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                Ok(Value::Array(out))
            }
            "sortstr" => {
                require_arity(name, args, 0)?;
                let mut out = values.clone();
                out.sort_by_key(|value| value.render());
                Ok(Value::Array(out))
            }
            "sortnum" => {
                require_arity(name, args, 0)?;
                let mut out = values.clone();
                out.sort_by(|left, right| {
                    let lhs = left.to_number().unwrap_or(0.0);
                    let rhs = right.to_number().unwrap_or(0.0);
                    lhs.partial_cmp(&rhs).unwrap_or(std::cmp::Ordering::Equal)
                });
                for value in &mut out {
                    if let Ok(number) = value.to_number() {
                        *value = Value::Number(number);
                    }
                }
                Ok(Value::Array(out))
            }
            "reverse" => {
                require_arity(name, args, 0)?;
                let mut out = values.clone();
                out.reverse();
                Ok(Value::Array(out))
            }
            "head" => {
                let n = optional_count(args, 1)?;
                Ok(Value::Array(values.iter().take(n).cloned().collect()))
            }
            "tail" => {
                let n = optional_count(args, 1)?;
                if n == 0 {
                    return Ok(Value::Array(Vec::new()));
                }
                let start = values.len().saturating_sub(n);
                Ok(Value::Array(values[start..].to_vec()))
            }
            "slice" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(ZuzuRustError::runtime(
                        "slice() expects one or two arguments",
                    ));
                }
                let len = values.len() as isize;
                let mut start = args[0].to_number().unwrap_or(0.0) as isize;
                if start < 0 {
                    start += len;
                }
                start = start.clamp(0, len);
                let end = if args.len() == 2 {
                    let mut end = args[1].to_number().unwrap_or(len as f64) as isize;
                    if end < 0 {
                        end += len;
                    }
                    end.clamp(0, len)
                } else {
                    len
                };
                let from = start.min(end) as usize;
                let to = start.max(end) as usize;
                Ok(Value::Array(values[from..to].to_vec()))
            }
            "join" => {
                require_arity(name, args, 1)?;
                let separator = args[0].render();
                Ok(Value::String(
                    values
                        .iter()
                        .map(|value| value.render())
                        .collect::<Vec<_>>()
                        .join(&separator),
                ))
            }
            "sum" => {
                require_arity(name, args, 0)?;
                Ok(Value::Number(collection_sum(values)?))
            }
            "product" => {
                require_arity(name, args, 0)?;
                Ok(Value::Number(collection_product(values)?))
            }
            "shuffle" => {
                require_arity(name, args, 0)?;
                Ok(Value::Array(values.clone()))
            }
            "sample" => {
                let n = optional_count(args, 1)?;
                Ok(Value::Array(values.iter().take(n).cloned().collect()))
            }
            "contains" => {
                require_arity(name, args, 1)?;
                Ok(Value::Boolean(
                    values.iter().any(|value| value.strict_eq(&args[0])),
                ))
            }
            "map" => self.map_values(values, args, "map", CollectionTarget::Array),
            "grep" => self.filter_values(values, args, "grep", CollectionTarget::Array),
            "any" => self.any_values(values, args),
            "all" => self.all_values(values, args),
            "first" => self.first_value(values, args),
            "remove" => {
                require_arity(name, args, 1)?;
                let pred =
                    expect_function(&args[0], "Collection method expects a function callback")?;
                values.retain(|value| {
                    !self
                        .predicate_callback(&pred, value.clone())
                        .unwrap_or(false)
                });
                Ok(Value::Array(values.clone()))
            }
            "first_index" => {
                require_arity(name, args, 1)?;
                let pred =
                    expect_function(&args[0], "Collection method expects a function callback")?;
                for (index, value) in values.iter().enumerate() {
                    if self.predicate_callback(&pred, value.clone())? {
                        return Ok(Value::Number(index as f64));
                    }
                }
                Ok(Value::Number(-1.0))
            }
            "for_each_value" => {
                require_arity(name, args, 1)?;
                let func =
                    expect_function(&args[0], "Collection method expects a function callback")?;
                for value in values.iter().cloned() {
                    let _ = self.call_function(&func, vec![value], Vec::new())?;
                }
                Ok(Value::Array(values.clone()))
            }
            "reduce" => self.reduce_values(values, args, false),
            "reductions" => self.reduce_values(values, args, true),
            other => Err(ZuzuRustError::thrown(format!(
                "unsupported Array method '{}'",
                other
            ))),
        }
    }
}
