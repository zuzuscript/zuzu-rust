use crate::{Result, ZuzuRustError};

use super::super::{Runtime, Value};
use super::common::{
    collection_product, collection_sum, expect_function, make_iterator, require_arity, stored_arg,
    unique_values, CollectionTarget,
};

impl Runtime {
    pub(in crate::runtime) fn call_bag_method(
        &self,
        values: &mut Vec<Value>,
        name: &str,
        args: &[Value],
    ) -> Result<Value> {
        match name {
            "add" | "push" => {
                values.extend(args.iter().cloned().map(Value::into_shared_if_composite));
                Ok(Value::Bag(values.clone()))
            }
            "add_weak" => {
                values.extend(args.iter().map(|value| stored_arg(value, true)));
                Ok(Value::Bag(values.clone()))
            }
            "remove" | "remove_first" => {
                require_arity(name, args, 1)?;
                let mut removed = false;
                values.retain(|value| {
                    if !removed && value.strict_eq(&args[0]) {
                        removed = true;
                        false
                    } else {
                        true
                    }
                });
                Ok(Value::Bag(values.clone()))
            }
            "length" => {
                require_arity(name, args, 0)?;
                Ok(Value::Number(values.len() as f64))
            }
            "count" => {
                if args.is_empty() {
                    return Ok(Value::Number(values.len() as f64));
                }
                require_arity(name, args, 1)?;
                Ok(Value::Number(
                    values
                        .iter()
                        .filter(|value| value.strict_eq(&args[0]))
                        .count() as f64,
                ))
            }
            "empty" => {
                require_arity(name, args, 0)?;
                Ok(Value::Boolean(values.is_empty()))
            }
            "clear" => {
                require_arity(name, args, 0)?;
                values.clear();
                Ok(Value::Bag(values.clone()))
            }
            "contains" => {
                require_arity(name, args, 1)?;
                Ok(Value::Boolean(
                    values.iter().any(|value| value.strict_eq(&args[0])),
                ))
            }
            "to_Array" => {
                require_arity(name, args, 0)?;
                Ok(Value::Array(values.clone()))
            }
            "to_Set" | "uniq" => {
                require_arity(name, args, 0)?;
                Ok(Value::Set(unique_values(values)))
            }
            "to_Iterator" => {
                require_arity(name, args, 0)?;
                Ok(make_iterator(values.clone()))
            }
            "sum" => {
                require_arity(name, args, 0)?;
                Ok(Value::Number(collection_sum(values)?))
            }
            "product" => {
                require_arity(name, args, 0)?;
                Ok(Value::Number(collection_product(values)?))
            }
            "sort" => {
                let mut out = values.clone();
                self.call_array_method(&mut out, "sort", args)
            }
            "sortstr" => {
                let mut out = values.clone();
                self.call_array_method(&mut out, "sortstr", args)
            }
            "sortnum" => {
                let mut out = values.clone();
                self.call_array_method(&mut out, "sortnum", args)
            }
            "map" => self.map_values(values, args, "map", CollectionTarget::Bag),
            "grep" => self.filter_values(values, args, "grep", CollectionTarget::Bag),
            "any" => self.any_values(values, args),
            "all" => self.all_values(values, args),
            "first" => self.first_value(values, args),
            "remove_if" => {
                require_arity(name, args, 1)?;
                let pred =
                    expect_function(&args[0], "Collection method expects a function callback")?;
                values.retain(|value| {
                    !self
                        .predicate_callback(&pred, value.clone())
                        .unwrap_or(false)
                });
                Ok(Value::Bag(values.clone()))
            }
            "for_each_value" => {
                require_arity(name, args, 1)?;
                let func =
                    expect_function(&args[0], "Collection method expects a function callback")?;
                for value in values.iter().cloned() {
                    let _ = self.call_function(&func, vec![value], Vec::new())?;
                }
                Ok(Value::Bag(values.clone()))
            }
            other => Err(ZuzuRustError::thrown(format!(
                "unsupported Bag method '{}'",
                other
            ))),
        }
    }
}
