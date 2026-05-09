use crate::{Result, ZuzuRustError};

use super::super::{Runtime, Value};
use super::common::{
    expect_function, expect_set, make_iterator, push_unique, require_arity, stored_arg,
    CollectionTarget,
};

impl Runtime {
    pub(in crate::runtime) fn call_set_method(
        &self,
        values: &mut Vec<Value>,
        name: &str,
        args: &[Value],
    ) -> Result<Value> {
        match name {
            "add" | "push" => {
                for value in args {
                    push_unique(values, value.clone().into_shared_if_composite());
                }
                Ok(Value::Set(values.clone()))
            }
            "add_weak" => {
                for value in args {
                    push_unique(values, stored_arg(value, true));
                }
                Ok(Value::Set(values.clone()))
            }
            "remove" => {
                require_arity(name, args, 1)?;
                values.retain(|value| !value.strict_eq(&args[0]));
                Ok(Value::Set(values.clone()))
            }
            "length" | "count" => {
                require_arity(name, args, 0)?;
                Ok(Value::Number(values.len() as f64))
            }
            "empty" | "is_empty" => {
                require_arity(name, args, 0)?;
                Ok(Value::Boolean(values.is_empty()))
            }
            "clear" => {
                require_arity(name, args, 0)?;
                values.clear();
                Ok(Value::Set(values.clone()))
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
            "to_Bag" => {
                require_arity(name, args, 0)?;
                Ok(Value::Bag(values.clone()))
            }
            "to_Iterator" => {
                require_arity(name, args, 0)?;
                Ok(make_iterator(values.clone()))
            }
            "union"
            | "intersection"
            | "difference"
            | "symmetric_difference"
            | "is_subset"
            | "is_superset"
            | "is_disjoint"
            | "equals" => {
                require_arity(name, args, 1)?;
                let other = expect_set(&args[0])?;
                match name {
                    "union" => {
                        let mut out = values.clone();
                        for value in other {
                            push_unique(&mut out, value);
                        }
                        Ok(Value::Set(out))
                    }
                    "intersection" => Ok(Value::Set(
                        values
                            .iter()
                            .filter(|value| other.iter().any(|o| o.strict_eq(value)))
                            .cloned()
                            .collect(),
                    )),
                    "difference" => Ok(Value::Set(
                        values
                            .iter()
                            .filter(|value| !other.iter().any(|o| o.strict_eq(value)))
                            .cloned()
                            .collect(),
                    )),
                    "symmetric_difference" => {
                        let left: Vec<_> = values
                            .iter()
                            .filter(|value| !other.iter().any(|o| o.strict_eq(value)))
                            .cloned()
                            .collect();
                        let mut out = left;
                        for value in other {
                            if !values.iter().any(|existing| existing.strict_eq(&value)) {
                                push_unique(&mut out, value);
                            }
                        }
                        Ok(Value::Set(out))
                    }
                    "is_subset" => Ok(Value::Boolean(
                        values
                            .iter()
                            .all(|value| other.iter().any(|o| o.strict_eq(value))),
                    )),
                    "is_superset" => Ok(Value::Boolean(
                        other
                            .iter()
                            .all(|value| values.iter().any(|o| o.strict_eq(value))),
                    )),
                    "is_disjoint" => Ok(Value::Boolean(
                        values
                            .iter()
                            .all(|value| !other.iter().any(|o| o.strict_eq(value))),
                    )),
                    "equals" => Ok(Value::Boolean(
                        values.len() == other.len()
                            && values
                                .iter()
                                .all(|value| other.iter().any(|o| o.strict_eq(value))),
                    )),
                    _ => unreachable!(),
                }
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
            "map" => self.map_values(values, args, "map", CollectionTarget::Set),
            "grep" => self.filter_values(values, args, "grep", CollectionTarget::Set),
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
                Ok(Value::Set(values.clone()))
            }
            "for_each_value" => {
                require_arity(name, args, 1)?;
                let func =
                    expect_function(&args[0], "Collection method expects a function callback")?;
                for value in values.iter().cloned() {
                    let _ = self.call_function(&func, vec![value], Vec::new())?;
                }
                Ok(Value::Set(values.clone()))
            }
            other => Err(ZuzuRustError::thrown(format!(
                "unsupported Set method '{}'",
                other
            ))),
        }
    }
}
