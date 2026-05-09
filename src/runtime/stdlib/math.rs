use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use super::super::collection::common::require_arity;
use super::super::{Runtime, Value};
use crate::error::{Result, ZuzuRustError};

pub(super) fn exports() -> HashMap<String, Value> {
    let mut exports = HashMap::new();
    exports.insert("Math".to_owned(), Value::builtin_class("Math".to_owned()));
    exports.insert("π".to_owned(), Value::Number(std::f64::consts::PI));
    exports
}

pub(super) fn call_class_method(
    runtime: &Runtime,
    class_name: &str,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    if class_name != "Math" {
        return None;
    }
    Some(call_math_method(runtime, name, args))
}

fn call_math_method(runtime: &Runtime, name: &str, args: &[Value]) -> Result<Value> {
    match name {
        "pi" => {
            require_arity(name, args, 0)?;
            Ok(Value::Number(std::f64::consts::PI))
        }
        "sin" => Ok(Value::Number(math_unary_number(
            runtime,
            name,
            args,
            f64::sin,
        )?)),
        "cos" => Ok(Value::Number(math_unary_number(
            runtime,
            name,
            args,
            f64::cos,
        )?)),
        "tan" => Ok(Value::Number(math_unary_number(
            runtime,
            name,
            args,
            f64::tan,
        )?)),
        "cosec" => Ok(Value::Number(reciprocal(math_unary_number(
            runtime,
            name,
            args,
            f64::sin,
        )?))),
        "sec" => Ok(Value::Number(reciprocal(math_unary_number(
            runtime,
            name,
            args,
            f64::cos,
        )?))),
        "cotan" => Ok(Value::Number(reciprocal(math_unary_number(
            runtime,
            name,
            args,
            f64::tan,
        )?))),
        "asin" => Ok(Value::Number(math_unary_number(
            runtime,
            name,
            args,
            f64::asin,
        )?)),
        "acos" => Ok(Value::Number(math_unary_number(
            runtime,
            name,
            args,
            f64::acos,
        )?)),
        "atan" => Ok(Value::Number(math_unary_number(
            runtime,
            name,
            args,
            f64::atan,
        )?)),
        "atan2" => {
            require_arity(name, args, 2)?;
            Ok(Value::Number(
                runtime
                    .value_to_number(&args[0])?
                    .atan2(runtime.value_to_number(&args[1])?),
            ))
        }
        "acosec" => Ok(Value::Number(
            (1.0 / math_unary_number(runtime, name, args, identity)?).asin(),
        )),
        "asec" => Ok(Value::Number(
            (1.0 / math_unary_number(runtime, name, args, identity)?).acos(),
        )),
        "acotan" => Ok(Value::Number(
            (1.0 / math_unary_number(runtime, name, args, identity)?).atan(),
        )),
        "sinh" => Ok(Value::Number(math_unary_number(
            runtime,
            name,
            args,
            f64::sinh,
        )?)),
        "cosh" => Ok(Value::Number(math_unary_number(
            runtime,
            name,
            args,
            f64::cosh,
        )?)),
        "tanh" => Ok(Value::Number(math_unary_number(
            runtime,
            name,
            args,
            f64::tanh,
        )?)),
        "cosech" => Ok(Value::Number(reciprocal(math_unary_number(
            runtime,
            name,
            args,
            f64::sinh,
        )?))),
        "sech" => Ok(Value::Number(reciprocal(math_unary_number(
            runtime,
            name,
            args,
            f64::cosh,
        )?))),
        "cotanh" => Ok(Value::Number(reciprocal(math_unary_number(
            runtime,
            name,
            args,
            f64::tanh,
        )?))),
        "asinh" => Ok(Value::Number(math_unary_number(
            runtime,
            name,
            args,
            f64::asinh,
        )?)),
        "acosh" => Ok(Value::Number(math_unary_number(
            runtime,
            name,
            args,
            f64::acosh,
        )?)),
        "atanh" => Ok(Value::Number(math_unary_number(
            runtime,
            name,
            args,
            f64::atanh,
        )?)),
        "acosech" => Ok(Value::Number(
            (1.0 / math_unary_number(runtime, name, args, identity)?).asinh(),
        )),
        "asech" => Ok(Value::Number(
            (1.0 / math_unary_number(runtime, name, args, identity)?).acosh(),
        )),
        "acotanh" => Ok(Value::Number(
            (1.0 / math_unary_number(runtime, name, args, identity)?).atanh(),
        )),
        "exp" => Ok(Value::Number(math_unary_number(
            runtime,
            name,
            args,
            f64::exp,
        )?)),
        "log" => Ok(Value::Number(math_unary_number(
            runtime,
            name,
            args,
            f64::ln,
        )?)),
        "log10" => Ok(Value::Number(math_unary_number(
            runtime,
            name,
            args,
            f64::log10,
        )?)),
        "pow" => {
            require_arity(name, args, 2)?;
            Ok(Value::Number(
                runtime
                    .value_to_number(&args[0])?
                    .powf(runtime.value_to_number(&args[1])?),
            ))
        }
        "rand" => {
            if args.len() > 1 {
                return Err(ZuzuRustError::runtime(
                    "rand() expects zero or one arguments",
                ));
            }
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.subsec_nanos())
                .unwrap_or(0);
            let mut value = (nanos as f64) / 1_000_000_000.0;
            if let Some(max) = args.first() {
                value *= runtime.value_to_number(max)?;
            }
            Ok(Value::Number(value))
        }
        "sum" => {
            let values = math_numeric_arguments(runtime, args)?;
            Ok(Value::Number(values.into_iter().sum()))
        }
        "min" => {
            let values = math_numeric_arguments(runtime, args)?;
            let min = values
                .into_iter()
                .reduce(f64::min)
                .ok_or_else(|| ZuzuRustError::runtime("Math.min requires at least one value"))?;
            Ok(Value::Number(min))
        }
        "max" => {
            let values = math_numeric_arguments(runtime, args)?;
            let max = values
                .into_iter()
                .reduce(f64::max)
                .ok_or_else(|| ZuzuRustError::runtime("Math.max requires at least one value"))?;
            Ok(Value::Number(max))
        }
        "clamp" => {
            require_arity(name, args, 3)?;
            let value = runtime.value_to_number(&args[0])?;
            let min = runtime.value_to_number(&args[1])?;
            let max = runtime.value_to_number(&args[2])?;
            Ok(Value::Number(value.clamp(min, max)))
        }
        "hypot" => {
            require_arity(name, args, 2)?;
            Ok(Value::Number(
                runtime
                    .value_to_number(&args[0])?
                    .hypot(runtime.value_to_number(&args[1])?),
            ))
        }
        "deg2rad" => Ok(Value::Number(math_unary_number(
            runtime,
            name,
            args,
            |value| value.to_radians(),
        )?)),
        "rad2deg" => Ok(Value::Number(math_unary_number(
            runtime,
            name,
            args,
            |value| value.to_degrees(),
        )?)),
        "hex2dec" => convert_radix(runtime, args, 16, 10),
        "hex2oct" => convert_radix(runtime, args, 16, 8),
        "hex2bin" => convert_radix(runtime, args, 16, 2),
        "dec2hex" => convert_radix(runtime, args, 10, 16),
        "dec2oct" => convert_radix(runtime, args, 10, 8),
        "dec2bin" => convert_radix(runtime, args, 10, 2),
        "oct2hex" => convert_radix(runtime, args, 8, 16),
        "oct2dec" => convert_radix(runtime, args, 8, 10),
        "oct2bin" => convert_radix(runtime, args, 8, 2),
        "bin2hex" => convert_radix(runtime, args, 2, 16),
        "bin2dec" => convert_radix(runtime, args, 2, 10),
        "bin2oct" => convert_radix(runtime, args, 2, 8),
        _ => Err(ZuzuRustError::runtime(format!(
            "unsupported static method '{}' for Math",
            name
        ))),
    }
}

fn math_unary_number(
    runtime: &Runtime,
    name: &str,
    args: &[Value],
    f: impl FnOnce(f64) -> f64,
) -> Result<f64> {
    require_arity(name, args, 1)?;
    Ok(f(runtime.value_to_number(&args[0])?))
}

fn math_numeric_arguments(runtime: &Runtime, args: &[Value]) -> Result<Vec<f64>> {
    let mut values = Vec::new();
    if args.len() == 1 {
        match &args[0] {
            Value::Array(items) | Value::Set(items) | Value::Bag(items) => {
                for item in items {
                    values.push(runtime.value_to_number(item)?);
                }
                return Ok(values);
            }
            Value::PairList(items) => {
                for (_, item) in items {
                    values.push(runtime.value_to_number(item)?);
                }
                return Ok(values);
            }
            _ => {}
        }
    }
    for arg in args {
        values.push(runtime.value_to_number(arg)?);
    }
    Ok(values)
}

fn convert_radix(runtime: &Runtime, args: &[Value], from: u32, to: u32) -> Result<Value> {
    require_arity("radix conversion", args, 1)?;
    let raw = runtime.render_value(&args[0])?;
    let negative = raw.starts_with('-');
    let trimmed = if negative { &raw[1..] } else { raw.as_str() };
    let normalized = strip_radix_prefix(trimmed, from);
    let parsed = u128::from_str_radix(normalized, from)
        .map_err(|_| ZuzuRustError::runtime(format!("invalid base-{} number '{}'", from, raw)))?;
    let mut converted = match to {
        2 => format!("{parsed:b}"),
        8 => format!("{parsed:o}"),
        10 => parsed.to_string(),
        16 => format!("{parsed:x}"),
        _ => {
            return Err(ZuzuRustError::runtime(format!(
                "unsupported radix conversion target {}",
                to
            )))
        }
    };
    if negative {
        converted.insert(0, '-');
    }
    Ok(Value::String(converted))
}

fn identity(value: f64) -> f64 {
    value
}

fn reciprocal(value: f64) -> f64 {
    1.0 / value
}

fn strip_radix_prefix(value: &str, radix: u32) -> &str {
    match radix {
        16 => value
            .strip_prefix("0x")
            .or_else(|| value.strip_prefix("0X"))
            .unwrap_or(value),
        8 => value
            .strip_prefix("0o")
            .or_else(|| value.strip_prefix("0O"))
            .unwrap_or(value),
        2 => value
            .strip_prefix("0b")
            .or_else(|| value.strip_prefix("0B"))
            .unwrap_or(value),
        _ => value,
    }
}
