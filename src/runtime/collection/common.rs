use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::rc::Rc;

use crate::{Result, ZuzuRustError};

use super::super::{FunctionValue, IteratorState, Runtime, Value};

#[derive(Clone, Copy)]
pub(in crate::runtime) enum CollectionTarget {
    Array,
    Set,
    Bag,
}

impl CollectionTarget {
    pub(in crate::runtime) fn wrap(self, values: Vec<Value>) -> Value {
        match self {
            CollectionTarget::Array => Value::Array(values),
            CollectionTarget::Set => Value::Set(unique_values(&values)),
            CollectionTarget::Bag => Value::Bag(values),
        }
    }
}

pub(in crate::runtime) fn is_mutating_collection_method(name: &str) -> bool {
    matches!(
        name,
        "append"
            | "push"
            | "push_weak"
            | "add"
            | "add_weak"
            | "pop"
            | "prepend"
            | "unshift"
            | "unshift_weak"
            | "shift"
            | "set"
            | "set_weak"
            | "clear"
            | "remove"
            | "remove_first"
            | "remove_if"
    )
}

pub(in crate::runtime) fn stored_arg(value: &Value, weak: bool) -> Value {
    value.clone().stored_with_weak_policy(weak)
}

pub(in crate::runtime) fn reject_named_args(
    name: &str,
    named_args: &[(String, Value)],
) -> Result<()> {
    if named_args.is_empty() {
        Ok(())
    } else {
        Err(ZuzuRustError::runtime(format!(
            "{name} constructor does not support named arguments here"
        )))
    }
}

pub(in crate::runtime) fn construct_pair(
    args: Vec<Value>,
    named_args: Vec<(String, Value)>,
) -> Result<Value> {
    if args.is_empty() {
        for (name, value) in named_args {
            if name == "pair" {
                let (key, value) = expect_pair_like(&value)?;
                return Ok(Value::Pair(key, Box::new(value)));
            }
        }
    }
    if args.len() == 2 {
        return Ok(Value::Pair(args[0].render(), Box::new(args[1].clone())));
    }
    if args.len() == 1 {
        let (key, value) = expect_pair_like(&args[0])?;
        return Ok(Value::Pair(key, Box::new(value)));
    }
    Err(ZuzuRustError::runtime(
        "Pair constructor expects a pair: named 'pair' or two positional values",
    ))
}

pub(in crate::runtime) fn construct_pairlist(
    args: Vec<Value>,
    named_args: Vec<(String, Value)>,
) -> Result<Value> {
    reject_named_args("PairList", &named_args)?;
    let mut out = Vec::new();
    for arg in args {
        let (key, value) = expect_pair_like(&arg)?;
        out.push((key, value));
    }
    Ok(Value::PairList(out))
}

pub(in crate::runtime) fn expect_pair_like(value: &Value) -> Result<(String, Value)> {
    match value {
        Value::Shared(shared) => expect_pair_like(&shared.borrow()),
        Value::Array(items) if items.len() >= 2 => Ok((items[0].render(), items[1].clone())),
        Value::Pair(key, value) => Ok((key.clone(), (**value).clone())),
        _ => Err(ZuzuRustError::runtime("expected Pair-like value")),
    }
}

pub(in crate::runtime) fn expect_set(value: &Value) -> Result<Vec<Value>> {
    match value {
        Value::Shared(shared) => expect_set(&shared.borrow()),
        Value::Set(values) => Ok(values.clone()),
        _ => Err(ZuzuRustError::runtime("Set method expects a Set argument")),
    }
}

pub(in crate::runtime) fn expect_function(
    value: &Value,
    message: &str,
) -> Result<Rc<FunctionValue>> {
    match value {
        Value::Function(function) => Ok(Rc::clone(function)),
        _ => Err(ZuzuRustError::runtime(message)),
    }
}

pub(in crate::runtime) fn require_arity(name: &str, args: &[Value], expected: usize) -> Result<()> {
    if args.len() == expected {
        Ok(())
    } else {
        Err(ZuzuRustError::runtime(format!(
            "{name}() expects {expected} argument{}",
            if expected == 1 { "" } else { "s" }
        )))
    }
}

pub(in crate::runtime) fn require_arity_range(
    name: &str,
    args: &[Value],
    min: usize,
    max: usize,
) -> Result<()> {
    if args.len() >= min && args.len() <= max {
        Ok(())
    } else {
        Err(ZuzuRustError::runtime(format!(
            "{name}() expects between {min} and {max} arguments"
        )))
    }
}

pub(in crate::runtime) fn optional_count(args: &[Value], default: usize) -> Result<usize> {
    if args.is_empty() {
        Ok(default)
    } else if args.len() == 1 {
        Ok(to_index(&args[0]))
    } else {
        Err(ZuzuRustError::runtime(
            "method expects at most one argument",
        ))
    }
}

pub(in crate::runtime) fn to_index(value: &Value) -> usize {
    let index = value.to_number().unwrap_or(0.0) as isize;
    if index < 0 {
        0
    } else {
        index as usize
    }
}

pub(in crate::runtime) fn collection_get(values: &[Value], args: &[Value]) -> Value {
    let index = args.first().map(to_index).unwrap_or(0);
    let default = args.get(1).cloned().unwrap_or(Value::Null);
    values.get(index).cloned().unwrap_or(default)
}

pub(in crate::runtime) fn collection_sum(values: &[Value]) -> Result<f64> {
    let mut total = 0.0;
    for value in values {
        total += value.to_number()?;
    }
    Ok(total)
}

pub(in crate::runtime) fn collection_product(values: &[Value]) -> Result<f64> {
    let mut total = 1.0;
    for value in values {
        total *= value.to_number()?;
    }
    Ok(total)
}

pub(in crate::runtime) fn unique_values(values: &[Value]) -> Vec<Value> {
    let mut out = Vec::new();
    for value in values {
        push_unique(&mut out, value.clone());
    }
    out
}

pub(in crate::runtime) fn make_iterator(items: Vec<Value>) -> Value {
    Value::Iterator(Rc::new(RefCell::new(IteratorState { items, index: 0 })))
}

pub(in crate::runtime) fn sorted_keys(map: &HashMap<String, Value>) -> Vec<String> {
    let mut keys: Vec<_> = map.keys().cloned().collect();
    keys.sort();
    keys
}

pub(in crate::runtime) fn collection_contains(collection: &Value, needle: &Value) -> bool {
    match collection {
        Value::Shared(value) => collection_contains(&value.borrow(), needle),
        Value::Array(values) | Value::Set(values) | Value::Bag(values) => {
            values.iter().any(|value| value.strict_eq(needle))
        }
        Value::Dict(values) => values.contains_key(&needle.render()),
        Value::PairList(values) => values.iter().any(|(key, _)| key == &needle.render()),
        _ => false,
    }
}

pub(in crate::runtime) fn collection_union(left: Value, right: Value) -> Result<Value> {
    let mut left = coerce_set_like(left)?;
    for item in coerce_set_like(right)? {
        push_unique(&mut left, item);
    }
    Ok(Value::Set(left))
}

pub(in crate::runtime) fn collection_intersection(left: Value, right: Value) -> Result<Value> {
    let left = coerce_set_like(left)?;
    let right = coerce_set_like(right)?;
    Ok(Value::Set(
        left.into_iter()
            .filter(|item| right.iter().any(|other| other.strict_eq(item)))
            .collect(),
    ))
}

pub(in crate::runtime) fn collection_difference(left: Value, right: Value) -> Result<Value> {
    let left = coerce_set_like(left)?;
    let right = coerce_set_like(right)?;
    Ok(Value::Set(
        left.into_iter()
            .filter(|item| !right.iter().any(|other| other.strict_eq(item)))
            .collect(),
    ))
}

pub(in crate::runtime) fn collection_subset(left: &Value, right: &Value) -> Result<bool> {
    let left = coerce_set_like(left.clone())?;
    let right = coerce_set_like(right.clone())?;
    Ok(left
        .iter()
        .all(|item| right.iter().any(|other| other.strict_eq(item))))
}

fn coerce_set_like(value: Value) -> Result<Vec<Value>> {
    match value {
        Value::Shared(value) => coerce_set_like(value.borrow().clone()),
        Value::Set(values) => Ok(values),
        Value::Array(values) | Value::Bag(values) => Ok(unique_values(&values)),
        Value::Dict(values) => Ok(values.keys().cloned().map(Value::String).collect()),
        Value::PairList(values) => Ok(unique_values(
            &values
                .into_iter()
                .map(|(key, _)| Value::String(key))
                .collect::<Vec<_>>(),
        )),
        _ => Err(ZuzuRustError::runtime(
            "Set operator expects Array, Dict, Set, Bag, or PairList",
        )),
    }
}

pub(in crate::runtime) fn pairlist_eq(left: &[(String, Value)], right: &[(String, Value)]) -> bool {
    left.len() == right.len()
        && left.iter().zip(right.iter()).all(
            |((left_key, left_value), (right_key, right_value))| {
                left_key == right_key && left_value.strict_eq(right_value)
            },
        )
}

pub(in crate::runtime) fn push_unique(values: &mut Vec<Value>, value: Value) {
    if !values.iter().any(|existing| existing.strict_eq(&value)) {
        values.push(value);
    }
}

impl Runtime {
    pub(in crate::runtime) fn call_pair_method(
        &self,
        key: &str,
        value: &mut Box<Value>,
        name: &str,
        args: &[Value],
    ) -> Result<Value> {
        match name {
            "key" => {
                require_arity(name, args, 0)?;
                Ok(Value::String(key.to_owned()))
            }
            "value" => {
                require_arity(name, args, 0)?;
                Ok((**value).clone())
            }
            other => Err(ZuzuRustError::thrown(format!(
                "unsupported Pair method '{}'",
                other
            ))),
        }
    }

    pub(in crate::runtime) fn compare_via_callback(
        &self,
        function: &FunctionValue,
        left: Value,
        right: Value,
    ) -> Result<Ordering> {
        let result = self
            .await_if_task(self.call_function(function, vec![left, right], Vec::new())?)?
            .to_number()?;
        Ok(if result < 0.0 {
            Ordering::Less
        } else if result > 0.0 {
            Ordering::Greater
        } else {
            Ordering::Equal
        })
    }

    pub(in crate::runtime) fn predicate_callback(
        &self,
        function: &FunctionValue,
        item: Value,
    ) -> Result<bool> {
        Ok(self
            .await_if_task(self.call_function(function, vec![item], Vec::new())?)?
            .is_truthy())
    }

    pub(in crate::runtime) fn map_values(
        &self,
        values: &[Value],
        args: &[Value],
        name: &str,
        target: CollectionTarget,
    ) -> Result<Value> {
        require_arity(name, args, 1)?;
        let func = expect_function(&args[0], "Collection method expects a function callback")?;
        let mut out = Vec::new();
        for value in values.iter().cloned() {
            let result = self.call_function(&func, vec![value], Vec::new())?;
            out.push(self.await_if_task(result)?);
        }
        Ok(target.wrap(out))
    }

    pub(in crate::runtime) fn filter_values(
        &self,
        values: &[Value],
        args: &[Value],
        name: &str,
        target: CollectionTarget,
    ) -> Result<Value> {
        require_arity(name, args, 1)?;
        let func = expect_function(&args[0], "Collection method expects a function callback")?;
        let mut out = Vec::new();
        for value in values.iter().cloned() {
            if self.predicate_callback(&func, value.clone())? {
                out.push(value);
            }
        }
        Ok(target.wrap(out))
    }

    pub(in crate::runtime) fn any_values(&self, values: &[Value], args: &[Value]) -> Result<Value> {
        require_arity("any", args, 1)?;
        let func = expect_function(&args[0], "Collection method expects a function callback")?;
        for value in values.iter().cloned() {
            if self.predicate_callback(&func, value)? {
                return Ok(Value::Boolean(true));
            }
        }
        Ok(Value::Boolean(false))
    }

    pub(in crate::runtime) fn all_values(&self, values: &[Value], args: &[Value]) -> Result<Value> {
        require_arity("all", args, 1)?;
        let func = expect_function(&args[0], "Collection method expects a function callback")?;
        for value in values.iter().cloned() {
            if !self.predicate_callback(&func, value)? {
                return Ok(Value::Boolean(false));
            }
        }
        Ok(Value::Boolean(true))
    }

    pub(in crate::runtime) fn first_value(
        &self,
        values: &[Value],
        args: &[Value],
    ) -> Result<Value> {
        require_arity("first", args, 1)?;
        let func = expect_function(&args[0], "Collection method expects a function callback")?;
        for value in values.iter().cloned() {
            if self.predicate_callback(&func, value.clone())? {
                return Ok(value);
            }
        }
        Ok(Value::Null)
    }

    pub(in crate::runtime) fn reduce_values(
        &self,
        values: &[Value],
        args: &[Value],
        all_steps: bool,
    ) -> Result<Value> {
        require_arity(if all_steps { "reductions" } else { "reduce" }, args, 1)?;
        let func = expect_function(&args[0], "Collection reduce expects a function callback")?;
        if values.is_empty() {
            return Ok(if all_steps {
                Value::Array(Vec::new())
            } else {
                Value::Null
            });
        }
        let mut acc = values[0].clone();
        let mut out = vec![acc.clone()];
        for value in values.iter().skip(1).cloned() {
            let result = self.call_function(&func, vec![acc, value], Vec::new())?;
            acc = self.await_if_task(result)?;
            if all_steps {
                out.push(acc.clone());
            }
        }
        if all_steps {
            Ok(Value::Array(out))
        } else {
            Ok(acc)
        }
    }
}
