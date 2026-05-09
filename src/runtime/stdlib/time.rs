use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

use super::super::{
    FieldSpec, MethodValue, ObjectValue, Runtime, TraitValue, UserClassValue, Value,
};
use crate::error::Result;

pub(super) fn exports() -> HashMap<String, Value> {
    let mut exports = HashMap::new();
    exports.insert("Time".to_owned(), Value::builtin_class("Time".to_owned()));
    exports.insert(
        "TimeParser".to_owned(),
        Value::builtin_class("TimeParser".to_owned()),
    );
    exports
}

pub(super) fn construct_time(
    runtime: &Runtime,
    args: Vec<Value>,
    named_args: Vec<(String, Value)>,
) -> Result<Value> {
    let mut epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs_f64())
        .unwrap_or(0.0);
    if let Some(value) = args.first() {
        epoch = runtime.value_to_number(value)?;
    }
    for (name, value) in named_args {
        if name == "epoch" {
            epoch = runtime.value_to_number(&value)?;
        }
    }
    Ok(time_object(epoch))
}

pub(super) fn construct_time_parser(
    args: Vec<Value>,
    named_args: Vec<(String, Value)>,
) -> Result<Value> {
    let mut format = "%Y-%m-%d".to_owned();
    if let Some(Value::String(value)) = args.first() {
        format = value.clone();
    }
    for (name, value) in named_args {
        if name == "format" {
            if let Value::String(text) = value {
                format = text;
            }
        }
    }
    Ok(time_parser_object(format))
}

pub(super) fn call_object_method(
    _runtime: &Runtime,
    class_name: &str,
    builtin_value: &Value,
    name: &str,
    _args: &[Value],
) -> Option<Result<Value>> {
    match class_name {
        "Time" => {
            let epoch = match builtin_value {
                Value::Number(epoch) => *epoch,
                _ => 0.0,
            };
            let value = match name {
                "epoch" => Ok(Value::Number(epoch)),
                "sec" => Ok(Value::Number(second_of_day(epoch) as f64)),
                "min" => Ok(Value::Number(minute_of_day(epoch) as f64)),
                "hour" => Ok(Value::Number(hour_of_day(epoch) as f64)),
                "day_of_month" => Ok(Value::Number(day_of_month(epoch) as f64)),
                "mon" | "month" => Ok(Value::Number(month_of_year(epoch) as f64)),
                "year" => Ok(Value::Number(year_number(epoch) as f64)),
                "add_seconds" => add_offset(_runtime, epoch, name, _args, 1.0),
                "add_minutes" => add_offset(_runtime, epoch, name, _args, 60.0),
                "add_hours" => add_offset(_runtime, epoch, name, _args, 3600.0),
                "add_days" => add_offset(_runtime, epoch, name, _args, 86_400.0),
                "add_weeks" => add_offset(_runtime, epoch, name, _args, 604_800.0),
                "add_months" => add_months(_runtime, epoch, _args),
                "add_years" => add_years(_runtime, epoch, _args),
                "datetime" | "to_String" => Ok(Value::String(format_time(epoch))),
                "strftime" => strftime(_runtime, epoch, _args),
                _ => return None,
            };
            Some(value)
        }
        "TimeParser" => {
            let format = match builtin_value {
                Value::String(value) => value.clone(),
                _ => "%Y-%m-%d".to_owned(),
            };
            let value = match name {
                "parse" => parse_time(_runtime, &format, _args),
                _ => return None,
            };
            Some(value)
        }
        _ => None,
    }
}

pub(super) fn has_builtin_object_method(class_name: &str, name: &str) -> bool {
    matches!(
        (class_name, name),
        ("Time", "epoch")
            | ("Time", "sec")
            | ("Time", "min")
            | ("Time", "hour")
            | ("Time", "day_of_month")
            | ("Time", "mon")
            | ("Time", "month")
            | ("Time", "year")
            | ("Time", "add_seconds")
            | ("Time", "add_minutes")
            | ("Time", "add_hours")
            | ("Time", "add_days")
            | ("Time", "add_weeks")
            | ("Time", "add_months")
            | ("Time", "add_years")
            | ("Time", "datetime")
            | ("Time", "strftime")
            | ("Time", "to_String")
            | ("TimeParser", "parse")
    )
}

pub(super) fn time_object(epoch: f64) -> Value {
    Value::Object(Rc::new(RefCell::new(ObjectValue {
        class: Rc::new(UserClassValue {
            name: "Time".to_owned(),
            base: None,
            traits: Vec::<Rc<TraitValue>>::new(),
            fields: vec![FieldSpec {
                name: "epoch".to_owned(),
                declared_type: Some("Number".to_owned()),
                mutable: true,
                accessors: Vec::new(),
                default_value: None,
                is_weak_storage: false,
            }],
            methods: HashMap::<String, Rc<MethodValue>>::new(),
            static_methods: HashMap::<String, Rc<MethodValue>>::new(),
            nested_classes: HashMap::new(),
            source_decl: None,
            closure_env: None,
        }),
        fields: HashMap::from([("epoch".to_owned(), Value::Number(epoch))]),
        weak_fields: std::collections::HashSet::new(),
        builtin_value: Some(Value::Number(epoch)),
    })))
}

fn time_parser_object(format: String) -> Value {
    Value::Object(Rc::new(RefCell::new(ObjectValue {
        class: Rc::new(UserClassValue {
            name: "TimeParser".to_owned(),
            base: None,
            traits: Vec::<Rc<TraitValue>>::new(),
            fields: vec![FieldSpec {
                name: "format".to_owned(),
                declared_type: Some("String".to_owned()),
                mutable: true,
                accessors: Vec::new(),
                default_value: None,
                is_weak_storage: false,
            }],
            methods: HashMap::<String, Rc<MethodValue>>::new(),
            static_methods: HashMap::<String, Rc<MethodValue>>::new(),
            nested_classes: HashMap::new(),
            source_decl: None,
            closure_env: None,
        }),
        fields: HashMap::from([("format".to_owned(), Value::String(format.clone()))]),
        weak_fields: std::collections::HashSet::new(),
        builtin_value: Some(Value::String(format)),
    })))
}

fn format_time(epoch: f64) -> String {
    let seconds = epoch as i64;
    let days = seconds.div_euclid(86_400);
    let secs_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = secs_of_day / 3600;
    let minute = (secs_of_day % 3600) / 60;
    let second = secs_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}")
}

fn strftime(runtime: &Runtime, epoch: f64, args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(crate::error::ZuzuRustError::runtime(
            "strftime() expects 1 argument",
        ));
    }
    let format = runtime.render_value(&args[0])?;
    Ok(Value::String(format_strftime(epoch, &format)))
}

fn format_strftime(epoch: f64, format: &str) -> String {
    let seconds = epoch as i64;
    let days = seconds.div_euclid(86_400);
    let secs_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = secs_of_day / 3600;
    let minute = (secs_of_day % 3600) / 60;
    let second = secs_of_day % 60;
    let mut out = String::new();
    let mut chars = format.chars();
    while let Some(ch) = chars.next() {
        if ch != '%' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('%') => out.push('%'),
            Some('Y') => out.push_str(&format!("{year:04}")),
            Some('m') => out.push_str(&format!("{month:02}")),
            Some('d') => out.push_str(&format!("{day:02}")),
            Some('H') => out.push_str(&format!("{hour:02}")),
            Some('M') => out.push_str(&format!("{minute:02}")),
            Some('S') => out.push_str(&format!("{second:02}")),
            Some('z') => out.push_str("+0000"),
            Some('Z') => out.push_str("UTC"),
            Some(other) => {
                out.push('%');
                out.push(other);
            }
            None => out.push('%'),
        }
    }
    out
}

fn add_offset(
    runtime: &Runtime,
    epoch: f64,
    name: &str,
    args: &[Value],
    seconds: f64,
) -> Result<Value> {
    if args.len() != 1 {
        return Err(crate::error::ZuzuRustError::runtime(format!(
            "{name}() expects 1 argument"
        )));
    }
    Ok(time_object(
        epoch + runtime.value_to_number(&args[0])? * seconds,
    ))
}

fn add_months(runtime: &Runtime, epoch: f64, args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(crate::error::ZuzuRustError::runtime(
            "add_months() expects 1 argument",
        ));
    }
    let delta = runtime.value_to_number(&args[0])? as i64;
    let (year, month, day) = civil_from_days((epoch as i64).div_euclid(86_400));
    let secs = (epoch as i64).rem_euclid(86_400);
    let total_months = (year * 12 + (month - 1)) + delta;
    let next_year = total_months.div_euclid(12);
    let next_month = total_months.rem_euclid(12) + 1;
    let clamped_day = day.min(days_in_month(next_year, next_month));
    Ok(time_object(
        days_from_civil(next_year, next_month, clamped_day) as f64 * 86_400.0 + secs as f64,
    ))
}

fn add_years(runtime: &Runtime, epoch: f64, args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(crate::error::ZuzuRustError::runtime(
            "add_years() expects 1 argument",
        ));
    }
    let delta = runtime.value_to_number(&args[0])? as i64;
    let (year, month, day) = civil_from_days((epoch as i64).div_euclid(86_400));
    let secs = (epoch as i64).rem_euclid(86_400);
    let next_year = year + delta;
    let clamped_day = day.min(days_in_month(next_year, month));
    Ok(time_object(
        days_from_civil(next_year, month, clamped_day) as f64 * 86_400.0 + secs as f64,
    ))
}

fn parse_time(_runtime: &Runtime, _format: &str, args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(crate::error::ZuzuRustError::runtime(
            "parse() expects 1 argument",
        ));
    }
    let Value::String(text) = &args[0] else {
        return Err(crate::error::ZuzuRustError::runtime(
            "parse() expects String",
        ));
    };
    let regex = regex::Regex::new(r"(?i)(\d{1,2})(?:st|nd|rd|th)\s+([A-Za-z]{3}),\s+(\d{4})")
        .map_err(|_| crate::error::ZuzuRustError::runtime("invalid time parser regex"))?;
    let captures = regex.captures(text).ok_or_else(|| {
        crate::error::ZuzuRustError::thrown("Exception: unable to parse time string")
    })?;
    let day = captures
        .get(1)
        .and_then(|m| m.as_str().parse::<i64>().ok())
        .unwrap_or(1);
    let mon_name = captures
        .get(2)
        .map(|m| m.as_str().to_ascii_lowercase())
        .unwrap_or_default();
    let year = captures
        .get(3)
        .and_then(|m| m.as_str().parse::<i64>().ok())
        .unwrap_or(1970);
    let month = match mon_name.as_str() {
        "jan" => 1,
        "feb" => 2,
        "mar" => 3,
        "apr" => 4,
        "may" => 5,
        "jun" => 6,
        "jul" => 7,
        "aug" => 8,
        "sep" => 9,
        "oct" => 10,
        "nov" => 11,
        "dec" => 12,
        _ => {
            return Err(crate::error::ZuzuRustError::thrown(
                "Exception: unable to parse month",
            ))
        }
    };
    Ok(time_object(
        days_from_civil(year, month, day) as f64 * 86_400.0,
    ))
}

fn second_of_day(epoch: f64) -> i64 {
    (epoch as i64).rem_euclid(60)
}

fn minute_of_day(epoch: f64) -> i64 {
    ((epoch as i64).rem_euclid(3600)) / 60
}

fn hour_of_day(epoch: f64) -> i64 {
    ((epoch as i64).rem_euclid(86_400)) / 3600
}

fn day_of_month(epoch: f64) -> i64 {
    let (_, _, day) = civil_from_days((epoch as i64).div_euclid(86_400));
    day
}

fn month_of_year(epoch: f64) -> i64 {
    let (_, month, _) = civil_from_days((epoch as i64).div_euclid(86_400));
    month
}

fn year_number(epoch: f64) -> i64 {
    let (year, _, _) = civil_from_days((epoch as i64).div_euclid(86_400));
    year
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let mp = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 30,
    }
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let mut y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    if m <= 2 {
        y += 1;
    }
    (y, m, d)
}
