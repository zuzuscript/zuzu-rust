use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{
    DateTime, Datelike, Duration as ChronoDuration, FixedOffset, LocalResult, NaiveDate,
    NaiveDateTime, Offset, TimeZone as ChronoTimeZone, Timelike, Utc,
};
use chrono_tz::Tz;

use super::super::{
    FieldSpec, MethodValue, ObjectValue, Runtime, TraitValue, UserClassValue, Value,
};
use crate::error::{Result, ZuzuRustError};

#[derive(Clone)]
enum Zone {
    Fixed(String, FixedOffset),
    Named(String, Tz),
}

impl Zone {
    fn parse(raw: &str) -> Result<Self> {
        let mut name = raw.trim().to_owned();
        if name.is_empty() || name.eq_ignore_ascii_case("z") || name.eq_ignore_ascii_case("gmt") {
            name = "UTC".to_owned();
        } else if name.eq_ignore_ascii_case("local") {
            name = iana_time_zone::get_timezone().unwrap_or_else(|_| "UTC".to_owned());
        }
        if let Some(offset) = parse_offset(&name) {
            return Ok(Self::Fixed(
                format_offset(offset.local_minus_utc(), true),
                offset,
            ));
        }
        let tz = name
            .parse::<Tz>()
            .map_err(|_| ZuzuRustError::runtime(format!("Unknown timezone '{}'", raw.trim())))?;
        Ok(Self::Named(name, tz))
    }

    fn label(&self) -> String {
        match self {
            Self::Fixed(label, _) => label.clone(),
            Self::Named(label, _) => label.clone(),
        }
    }

    fn offset_at(&self, epoch: f64) -> i32 {
        match self {
            Self::Fixed(_, offset) => offset.local_minus_utc(),
            Self::Named(_, zone) => epoch_datetime(epoch)
                .with_timezone(zone)
                .offset()
                .fix()
                .local_minus_utc(),
        }
    }

    fn wall_parts(&self, epoch: f64) -> WallParts {
        match self {
            Self::Fixed(_, offset) => {
                WallParts::from_datetime(epoch_datetime(epoch).with_timezone(offset))
            }
            Self::Named(_, zone) => {
                WallParts::from_datetime(epoch_datetime(epoch).with_timezone(zone))
            }
        }
    }

    fn from_local(&self, naive: &NaiveDateTime) -> LocalResult<DateTime<Utc>> {
        match self {
            Self::Fixed(_, offset) => offset
                .from_local_datetime(naive)
                .map(|dt| dt.with_timezone(&Utc)),
            Self::Named(_, zone) => zone
                .from_local_datetime(naive)
                .map(|dt| dt.with_timezone(&Utc)),
        }
    }
}

#[derive(Clone, Copy)]
struct WallParts {
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    fraction: f64,
}

impl WallParts {
    fn from_datetime<TzOffset: chrono::TimeZone>(dt: DateTime<TzOffset>) -> Self
    where
        TzOffset::Offset: std::fmt::Display,
    {
        Self {
            year: dt.year(),
            month: dt.month(),
            day: dt.day(),
            hour: dt.hour(),
            minute: dt.minute(),
            second: dt.second(),
            fraction: 0.0,
        }
    }
}

pub(super) fn exports() -> HashMap<String, Value> {
    let mut exports = HashMap::new();
    for class in ["Time", "TimeZone", "Duration", "TimeFormat", "TimeParser"] {
        exports.insert(class.to_owned(), Value::builtin_class(class.to_owned()));
    }
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
    let mut zone = "UTC".to_owned();
    if let Some(value) = args.first() {
        epoch = runtime.value_to_number(value)?;
    }
    for (name, value) in named_args {
        match name.as_str() {
            "epoch" => epoch = runtime.value_to_number(&value)?,
            "timezone" => zone = zone_arg(runtime, &value)?,
            _ => {}
        }
    }
    time_object_with_zone(epoch, zone)
}

pub(super) fn construct_time_zone(
    runtime: &Runtime,
    args: Vec<Value>,
    named_args: Vec<(String, Value)>,
) -> Result<Value> {
    let mut name = "UTC".to_owned();
    if let Some(value) = args.first() {
        name = runtime.render_value(value)?;
    }
    for (key, value) in named_args {
        if key == "name" {
            name = runtime.render_value(&value)?;
        }
    }
    Ok(time_zone_object(Zone::parse(&name)?.label()))
}

pub(super) fn construct_duration(
    runtime: &Runtime,
    args: Vec<Value>,
    named_args: Vec<(String, Value)>,
) -> Result<Value> {
    let mut parts = duration_map();
    if let Some(value) = args.first() {
        parts.insert(
            "seconds".to_owned(),
            Value::Number(runtime.value_to_number(value)?),
        );
    }
    for (name, value) in named_args {
        if parts.contains_key(&name) {
            parts.insert(name, Value::Number(runtime.value_to_number(&value)?));
        }
    }
    Ok(native_object("Duration", parts))
}

pub(super) fn construct_time_format(
    runtime: &Runtime,
    args: Vec<Value>,
    named_args: Vec<(String, Value)>,
) -> Result<Value> {
    let mut pattern = String::new();
    let mut kind = "strftime".to_owned();
    let mut timezone = Value::Null;
    if let Some(value) = args.first() {
        pattern = runtime.render_value(value)?;
    }
    for (name, value) in named_args {
        match name.as_str() {
            "pattern" => pattern = runtime.render_value(&value)?,
            "kind" => kind = runtime.render_value(&value)?,
            "timezone" => timezone = Value::String(zone_arg(runtime, &value)?),
            _ => {}
        }
    }
    let fields = HashMap::from([
        ("kind".to_owned(), Value::String(kind)),
        ("pattern".to_owned(), Value::String(pattern)),
        ("timezone".to_owned(), timezone),
    ]);
    Ok(native_object("TimeFormat", fields))
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
    Ok(native_object(
        "TimeParser",
        HashMap::from([("format".to_owned(), Value::String(format))]),
    ))
}

pub(super) fn call_class_method(
    runtime: &Runtime,
    class_name: &str,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    let result = || -> Result<Value> {
        match (class_name, name) {
            ("Time", "parse") => parse_time_static(runtime, args, &[]),
            ("TimeZone", "utc") => Ok(time_zone_object("UTC".to_owned())),
            ("TimeZone", "local") => Ok(time_zone_object(
                iana_time_zone::get_timezone().unwrap_or_else(|_| "UTC".to_owned()),
            )),
            ("TimeZone", "named") => {
                require_arity(name, args, 1)?;
                let zone = Zone::parse(&runtime.render_value(&args[0])?)?;
                Ok(time_zone_object(zone.label()))
            }
            ("TimeZone", "offset") => {
                require_arity(name, args, 1)?;
                Ok(time_zone_object(format_offset(
                    runtime.value_to_number(&args[0])? as i32,
                    true,
                )))
            }
            ("Duration", "seconds") => duration_single(runtime, "seconds", args),
            ("Duration", "minutes") => duration_single(runtime, "minutes", args),
            ("Duration", "hours") => duration_single(runtime, "hours", args),
            ("Duration", "days") => duration_single(runtime, "days", args),
            ("Duration", "weeks") => duration_single(runtime, "weeks", args),
            ("Duration", "months") => duration_single(runtime, "months", args),
            ("Duration", "years") => duration_single(runtime, "years", args),
            ("TimeFormat", "iso8601") => time_format_object("iso8601", "", None),
            ("TimeFormat", "rfc3339") => time_format_object("rfc3339", "", None),
            ("TimeFormat", "rfc5322") => time_format_object("rfc5322", "", None),
            ("TimeFormat", "strftime") => {
                require_arity(name, args, 1)?;
                time_format_object("strftime", &runtime.render_value(&args[0])?, None)
            }
            _ => unreachable!(),
        }
    };
    if has_class_method(class_name, name) {
        Some(result())
    } else {
        None
    }
}

pub(super) fn call_class_method_named(
    runtime: &Runtime,
    class_name: &str,
    name: &str,
    args: &[Value],
    named_args: &[(String, Value)],
) -> Option<Result<Value>> {
    let result = || -> Result<Value> {
        match (class_name, name) {
            ("Time", "parse") => parse_time_static(runtime, args, named_args),
            ("TimeFormat", "strftime") => {
                if args.len() != 1 {
                    return Err(ZuzuRustError::runtime("strftime() expects 1 argument"));
                }
                let mut zone = None;
                for (key, value) in named_args {
                    if key == "timezone" {
                        zone = Some(zone_arg(runtime, value)?);
                    }
                }
                time_format_object("strftime", &runtime.render_value(&args[0])?, zone)
            }
            _ => unreachable!(),
        }
    };
    if matches!(
        (class_name, name),
        ("Time", "parse") | ("TimeFormat", "strftime")
    ) {
        Some(result())
    } else {
        None
    }
}

pub(super) fn has_class_method(class_name: &str, name: &str) -> bool {
    matches!(
        (class_name, name),
        ("Time", "parse")
            | ("TimeZone", "utc")
            | ("TimeZone", "local")
            | ("TimeZone", "named")
            | ("TimeZone", "offset")
            | ("Duration", "seconds")
            | ("Duration", "minutes")
            | ("Duration", "hours")
            | ("Duration", "days")
            | ("Duration", "weeks")
            | ("Duration", "months")
            | ("Duration", "years")
            | ("TimeFormat", "iso8601")
            | ("TimeFormat", "rfc3339")
            | ("TimeFormat", "rfc5322")
            | ("TimeFormat", "strftime")
    )
}

pub(super) fn call_object_method(
    runtime: &Runtime,
    class_name: &str,
    builtin_value: &Value,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    match class_name {
        "Time" => Some(call_time_method(runtime, builtin_value, name, args)),
        "TimeZone" => Some(call_zone_method(builtin_value, name, args)),
        "Duration" => Some(call_duration_method(builtin_value, name, args)),
        "TimeFormat" => Some(call_format_method(runtime, builtin_value, name, args)),
        "TimeParser" => Some(call_parser_method(runtime, builtin_value, name, args)),
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
            | ("Time", "subtract_seconds")
            | ("Time", "subtract_minutes")
            | ("Time", "subtract_hours")
            | ("Time", "subtract_days")
            | ("Time", "subtract_weeks")
            | ("Time", "subtract_months")
            | ("Time", "subtract_years")
            | ("Time", "add")
            | ("Time", "subtract")
            | ("Time", "elapsed_seconds_until")
            | ("Time", "compare")
            | ("Time", "is_before")
            | ("Time", "is_after")
            | ("Time", "timezone")
            | ("Time", "with_timezone")
            | ("Time", "reinterpret_timezone")
            | ("Time", "as_utc")
            | ("Time", "as_local")
            | ("Time", "datetime")
            | ("Time", "strftime")
            | ("Time", "to_iso8601")
            | ("Time", "to_rfc3339")
            | ("Time", "to_rfc5322")
            | ("Time", "format")
            | ("Time", "to_String")
            | ("TimeZone", "name")
            | ("TimeZone", "to_String")
            | ("Duration", "seconds")
            | ("Duration", "minutes")
            | ("Duration", "hours")
            | ("Duration", "days")
            | ("Duration", "weeks")
            | ("Duration", "months")
            | ("Duration", "years")
            | ("TimeFormat", "format")
            | ("TimeFormat", "parse")
            | ("TimeParser", "parse")
    )
}

pub(super) fn time_object(epoch: f64) -> Value {
    time_object_with_zone(epoch, "UTC".to_owned()).expect("UTC timezone should be valid")
}

pub(super) fn time_object_with_zone(epoch: f64, zone: String) -> Result<Value> {
    let zone = Zone::parse(&zone)?.label();
    let fields = HashMap::from([
        ("epoch".to_owned(), Value::Number(epoch)),
        ("timezone".to_owned(), Value::String(zone.clone())),
    ]);
    Ok(native_object("Time", fields))
}

fn call_time_method(
    runtime: &Runtime,
    builtin_value: &Value,
    name: &str,
    args: &[Value],
) -> Result<Value> {
    let (epoch, zone_name) = time_parts(builtin_value)?;
    let zone = Zone::parse(&zone_name)?;
    let parts = zone.wall_parts(epoch);
    match name {
        "epoch" => Ok(Value::Number(epoch)),
        "sec" => Ok(Value::Number(parts.second as f64)),
        "min" => Ok(Value::Number(parts.minute as f64)),
        "hour" => Ok(Value::Number(parts.hour as f64)),
        "day_of_month" => Ok(Value::Number(parts.day as f64)),
        "mon" | "month" => Ok(Value::Number(parts.month as f64)),
        "year" => Ok(Value::Number(parts.year as f64)),
        "timezone" => Ok(time_zone_object(zone_name)),
        "with_timezone" => {
            require_arity(name, args, 1)?;
            time_object_with_zone(epoch, zone_arg(runtime, &args[0])?)
        }
        "reinterpret_timezone" => {
            require_arity(name, args, 1)?;
            let new_zone = zone_arg(runtime, &args[0])?;
            time_object_with_zone(same_wall_epoch(parts, &Zone::parse(&new_zone)?)?, new_zone)
        }
        "as_utc" => time_object_with_zone(epoch, "UTC".to_owned()),
        "as_local" => time_object_with_zone(
            epoch,
            iana_time_zone::get_timezone().unwrap_or_else(|_| "UTC".to_owned()),
        ),
        "add_seconds" => add_elapsed(runtime, epoch, &zone_name, name, args, 1.0),
        "add_minutes" => add_elapsed(runtime, epoch, &zone_name, name, args, 60.0),
        "add_hours" => add_elapsed(runtime, epoch, &zone_name, name, args, 3600.0),
        "subtract_seconds" => add_elapsed(runtime, epoch, &zone_name, name, args, -1.0),
        "subtract_minutes" => add_elapsed(runtime, epoch, &zone_name, name, args, -60.0),
        "subtract_hours" => add_elapsed(runtime, epoch, &zone_name, name, args, -3600.0),
        "add_days" => calendar_add(runtime, epoch, &zone, &zone_name, name, args, 0, 0, 1),
        "add_weeks" => calendar_add(runtime, epoch, &zone, &zone_name, name, args, 0, 0, 7),
        "add_months" => calendar_add(runtime, epoch, &zone, &zone_name, name, args, 0, 1, 0),
        "add_years" => calendar_add(runtime, epoch, &zone, &zone_name, name, args, 1, 0, 0),
        "subtract_days" => calendar_add(runtime, epoch, &zone, &zone_name, name, args, 0, 0, -1),
        "subtract_weeks" => calendar_add(runtime, epoch, &zone, &zone_name, name, args, 0, 0, -7),
        "subtract_months" => calendar_add(runtime, epoch, &zone, &zone_name, name, args, 0, -1, 0),
        "subtract_years" => calendar_add(runtime, epoch, &zone, &zone_name, name, args, -1, 0, 0),
        "add" => add_duration(runtime, epoch, &zone, &zone_name, args, 1.0),
        "subtract" => add_duration(runtime, epoch, &zone, &zone_name, args, -1.0),
        "elapsed_seconds_until" => {
            require_arity(name, args, 1)?;
            Ok(Value::Number(time_epoch(&args[0])? - epoch))
        }
        "compare" => {
            require_arity(name, args, 1)?;
            Ok(Value::Number(if epoch < time_epoch(&args[0])? {
                -1.0
            } else if epoch > time_epoch(&args[0])? {
                1.0
            } else {
                0.0
            }))
        }
        "is_before" => {
            require_arity(name, args, 1)?;
            Ok(Value::Boolean(epoch < time_epoch(&args[0])?))
        }
        "is_after" => {
            require_arity(name, args, 1)?;
            Ok(Value::Boolean(epoch > time_epoch(&args[0])?))
        }
        "datetime" | "to_String" => Ok(Value::String(format_strftime(
            epoch,
            &zone,
            "%Y-%m-%dT%H:%M:%S",
        ))),
        "strftime" => {
            require_arity(name, args, 1)?;
            Ok(Value::String(format_strftime(
                epoch,
                &zone,
                &runtime.render_value(&args[0])?,
            )))
        }
        "to_iso8601" | "to_rfc3339" => Ok(Value::String(format_rfc3339(epoch, &zone))),
        "to_rfc5322" => Ok(Value::String(format_rfc5322(epoch, &zone, true))),
        "format" => {
            require_arity(name, args, 1)?;
            let (kind, pattern, format_zone) = format_parts(&args[0])?;
            let target_zone = Zone::parse(format_zone.as_deref().unwrap_or(&zone_name))?;
            match kind.as_str() {
                "iso8601" | "rfc3339" => Ok(Value::String(format_rfc3339(epoch, &target_zone))),
                "rfc5322" => Ok(Value::String(format_rfc5322(epoch, &target_zone, true))),
                _ => Ok(Value::String(format_strftime(
                    epoch,
                    &target_zone,
                    &pattern,
                ))),
            }
        }
        _ => Err(ZuzuRustError::runtime(format!(
            "unsupported Time method '{name}'"
        ))),
    }
}

fn call_zone_method(builtin_value: &Value, name: &str, args: &[Value]) -> Result<Value> {
    require_arity(name, args, 0)?;
    let Value::String(zone) = builtin_value else {
        return Err(ZuzuRustError::runtime(
            "TimeZone has invalid internal value",
        ));
    };
    match name {
        "name" | "to_String" => Ok(Value::String(zone.clone())),
        _ => Err(ZuzuRustError::runtime(format!(
            "unsupported TimeZone method '{name}'"
        ))),
    }
}

fn call_duration_method(builtin_value: &Value, name: &str, args: &[Value]) -> Result<Value> {
    require_arity(name, args, 0)?;
    let parts = duration_parts(builtin_value)?;
    parts
        .get(name)
        .cloned()
        .ok_or_else(|| ZuzuRustError::runtime(format!("unsupported Duration method '{name}'")))
}

fn call_format_method(
    runtime: &Runtime,
    builtin_value: &Value,
    name: &str,
    args: &[Value],
) -> Result<Value> {
    match name {
        "format" => {
            require_arity(name, args, 1)?;
            let (kind, pattern, format_zone) = format_raw_parts(builtin_value)?;
            let epoch = time_epoch(&args[0])?;
            let zone = Zone::parse(format_zone.as_deref().unwrap_or("UTC"))?;
            match kind.as_str() {
                "iso8601" | "rfc3339" => Ok(Value::String(format_rfc3339(epoch, &zone))),
                "rfc5322" => Ok(Value::String(format_rfc5322(epoch, &zone, true))),
                _ => Ok(Value::String(format_strftime(epoch, &zone, &pattern))),
            }
        }
        "parse" => {
            require_arity(name, args, 1)?;
            let (_kind, _pattern, format_zone) = format_raw_parts(builtin_value)?;
            let zone = format_zone
                .ok_or_else(|| ZuzuRustError::runtime("TimeFormat.parse() requires a timezone"))?;
            let (epoch, parsed_zone) =
                parse_time_text(&runtime.render_value(&args[0])?, Some(zone.clone()), true)?;
            time_object_with_zone(epoch, if zone.is_empty() { parsed_zone } else { zone })
        }
        _ => Err(ZuzuRustError::runtime(format!(
            "unsupported TimeFormat method '{name}'"
        ))),
    }
}

fn call_parser_method(
    runtime: &Runtime,
    builtin_value: &Value,
    name: &str,
    args: &[Value],
) -> Result<Value> {
    require_arity(name, args, 1)?;
    if name != "parse" {
        return Err(ZuzuRustError::runtime(format!(
            "unsupported TimeParser method '{name}'"
        )));
    }
    let _format = match builtin_value {
        Value::SystemDict(values) => values.get("format"),
        _ => None,
    };
    let text = runtime.render_value(&args[0])?;
    let regex = regex::Regex::new(
        r"(?i)(?:[A-Za-z]+\.?\s+)?(\d{1,2})(?:st|nd|rd|th)\s+([A-Za-z]{3}),\s+(\d{4})",
    )
    .map_err(|_| ZuzuRustError::runtime("invalid time parser regex"))?;
    if let Some(captures) = regex.captures(&text) {
        let day = captures[1].parse::<u32>().unwrap_or(1);
        let month = month_number(&captures[2])?;
        let year = captures[3].parse::<i32>().unwrap_or(1970);
        return time_object_with_zone(
            same_wall_epoch(
                WallParts {
                    year,
                    month,
                    day,
                    hour: 0,
                    minute: 0,
                    second: 0,
                    fraction: 0.0,
                },
                &Zone::parse("UTC")?,
            )?,
            "UTC".to_owned(),
        );
    }
    let (epoch, _) = parse_time_text(&text, Some("UTC".to_owned()), false)
        .map_err(|_| ZuzuRustError::thrown("Exception: unable to parse time string"))?;
    time_object_with_zone(epoch, "UTC".to_owned())
}

fn parse_time_static(
    runtime: &Runtime,
    args: &[Value],
    named_args: &[(String, Value)],
) -> Result<Value> {
    require_arity("parse", args, 1)?;
    let mut zone = None;
    for (name, value) in named_args {
        if name == "timezone" {
            zone = Some(zone_arg(runtime, value)?);
        }
    }
    let (epoch, parsed_zone) =
        parse_time_text(&runtime.render_value(&args[0])?, zone.clone(), true)?;
    time_object_with_zone(epoch, zone.unwrap_or(parsed_zone))
}

fn parse_time_text(
    text: &str,
    default_zone: Option<String>,
    require_zone: bool,
) -> Result<(f64, String)> {
    let iso = regex::Regex::new(r"^(\d{4})-(\d\d)-(\d\d)(?:[Tt ](\d\d):(\d\d)(?::(\d\d)(?:\.\d+)?)?)?(?:\s*(Z|[+-]\d\d:?\d\d))?$")
        .map_err(|_| ZuzuRustError::runtime("invalid ISO time regex"))?;
    if let Some(captures) = iso.captures(text) {
        if require_zone && captures.get(7).is_none() && default_zone.is_none() {
            return Err(ZuzuRustError::runtime("Time.parse() requires a timezone"));
        }
        let zone_name = if let Some(offset) = captures.get(7) {
            format_offset(
                parse_offset(offset.as_str())
                    .ok_or_else(|| ZuzuRustError::runtime("invalid timezone offset"))?
                    .local_minus_utc(),
                true,
            )
        } else {
            default_zone.clone().unwrap_or_else(|| "UTC".to_owned())
        };
        let parts = WallParts {
            year: captures[1].parse().unwrap_or(1970),
            month: captures[2].parse().unwrap_or(1),
            day: captures[3].parse().unwrap_or(1),
            hour: captures
                .get(4)
                .and_then(|m| m.as_str().parse().ok())
                .unwrap_or(0),
            minute: captures
                .get(5)
                .and_then(|m| m.as_str().parse().ok())
                .unwrap_or(0),
            second: captures
                .get(6)
                .and_then(|m| m.as_str().parse().ok())
                .unwrap_or(0),
            fraction: 0.0,
        };
        return Ok((
            same_wall_epoch(parts, &Zone::parse(&zone_name)?)?,
            zone_name,
        ));
    }
    let mail = regex::Regex::new(r"(?i)^(?:[A-Za-z]{3},\s*)?(\d{1,2})\s+([A-Za-z]{3})\s+(\d{4})\s+(\d\d):(\d\d)(?::(\d\d))?\s+(Z|[+-]\d\d:?\d\d|UT|UTC|GMT)$")
        .map_err(|_| ZuzuRustError::runtime("invalid RFC time regex"))?;
    if let Some(captures) = mail.captures(text) {
        let offset = parse_offset(&captures[7])
            .ok_or_else(|| ZuzuRustError::runtime("invalid timezone offset"))?;
        let zone_name = format_offset(offset.local_minus_utc(), true);
        let parts = WallParts {
            year: captures[3].parse().unwrap_or(1970),
            month: month_number(&captures[2])?,
            day: captures[1].parse().unwrap_or(1),
            hour: captures[4].parse().unwrap_or(0),
            minute: captures[5].parse().unwrap_or(0),
            second: captures
                .get(6)
                .and_then(|m| m.as_str().parse().ok())
                .unwrap_or(0),
            fraction: 0.0,
        };
        return Ok((
            same_wall_epoch(parts, &Zone::parse(&zone_name)?)?,
            zone_name,
        ));
    }
    Err(ZuzuRustError::runtime("Error parsing time"))
}

fn epoch_datetime(epoch: f64) -> DateTime<Utc> {
    let sec = epoch.floor() as i64;
    let nanos = ((epoch - sec as f64) * 1_000_000_000.0).round().max(0.0) as u32;
    Utc.timestamp_opt(sec, nanos)
        .single()
        .unwrap_or_else(|| Utc.timestamp_opt(0, 0).unwrap())
}

fn same_wall_epoch(parts: WallParts, zone: &Zone) -> Result<f64> {
    for shift in 0..=180 {
        let base = naive_from_parts(parts)?;
        let naive = base + ChronoDuration::minutes(shift);
        match zone.from_local(&naive) {
            LocalResult::Single(dt) => return Ok(dt.timestamp() as f64 + parts.fraction),
            LocalResult::Ambiguous(a, b) => {
                return Ok(a.timestamp().min(b.timestamp()) as f64 + parts.fraction)
            }
            LocalResult::None => {}
        }
    }
    Err(ZuzuRustError::runtime("Invalid local time"))
}

fn naive_from_parts(parts: WallParts) -> Result<NaiveDateTime> {
    let date = NaiveDate::from_ymd_opt(parts.year, parts.month, parts.day)
        .ok_or_else(|| ZuzuRustError::runtime("Invalid date"))?;
    date.and_hms_opt(parts.hour, parts.minute, parts.second)
        .ok_or_else(|| ZuzuRustError::runtime("Invalid time"))
}

fn calendar_add(
    runtime: &Runtime,
    epoch: f64,
    zone: &Zone,
    zone_name: &str,
    name: &str,
    args: &[Value],
    years_factor: i32,
    months_factor: i32,
    days_factor: i64,
) -> Result<Value> {
    require_arity(name, args, 1)?;
    let count = runtime.value_to_number(&args[0])? as i32;
    let mut parts = zone.wall_parts(epoch);
    parts.fraction = epoch - epoch.floor();
    let months = count * (years_factor * 12 + months_factor);
    if months != 0 {
        parts = add_months(parts, months);
    }
    let days = count as i64 * days_factor;
    if days != 0 {
        let naive = naive_from_parts(parts)? + ChronoDuration::days(days);
        parts.year = naive.year();
        parts.month = naive.month();
        parts.day = naive.day();
        parts.hour = naive.hour();
        parts.minute = naive.minute();
        parts.second = naive.second();
    }
    time_object_with_zone(same_wall_epoch(parts, zone)?, zone_name.to_owned())
}

fn add_duration(
    _runtime: &Runtime,
    epoch: f64,
    zone: &Zone,
    zone_name: &str,
    args: &[Value],
    sign: f64,
) -> Result<Value> {
    require_arity("add", args, 1)?;
    let parts = duration_parts_from_value(&args[0])?;
    let elapsed = number_field(&parts, "seconds")?
        + number_field(&parts, "minutes")? * 60.0
        + number_field(&parts, "hours")? * 3600.0;
    let next = epoch + elapsed * sign;
    let months = ((number_field(&parts, "years")? * 12.0) + number_field(&parts, "months")?) * sign;
    let days = (number_field(&parts, "weeks")? * 7.0 + number_field(&parts, "days")?) * sign;
    let mut wall = zone.wall_parts(next);
    wall.fraction = next - next.floor();
    if months != 0.0 {
        wall = add_months(wall, months as i32);
    }
    if days != 0.0 {
        let naive = naive_from_parts(wall)? + ChronoDuration::days(days as i64);
        wall.year = naive.year();
        wall.month = naive.month();
        wall.day = naive.day();
    }
    time_object_with_zone(same_wall_epoch(wall, zone)?, zone_name.to_owned())
}

fn add_elapsed(
    runtime: &Runtime,
    epoch: f64,
    zone: &str,
    name: &str,
    args: &[Value],
    seconds: f64,
) -> Result<Value> {
    require_arity(name, args, 1)?;
    time_object_with_zone(
        epoch + runtime.value_to_number(&args[0])? * seconds,
        zone.to_owned(),
    )
}

fn add_months(parts: WallParts, months: i32) -> WallParts {
    let total = parts.year * 12 + (parts.month as i32 - 1) + months;
    let year = total.div_euclid(12);
    let month = total.rem_euclid(12) + 1;
    let day = parts.day.min(days_in_month(year, month as u32));
    WallParts {
        year,
        month: month as u32,
        day,
        ..parts
    }
}

fn days_in_month(year: i32, month: u32) -> u32 {
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    (NaiveDate::from_ymd_opt(next_year, next_month, 1).unwrap() - ChronoDuration::days(1)).day()
}

fn format_strftime(epoch: f64, zone: &Zone, format: &str) -> String {
    let parts = zone.wall_parts(epoch);
    let offset = zone.offset_at(epoch);
    let mut out = String::new();
    let mut chars = format.chars();
    while let Some(ch) = chars.next() {
        if ch != '%' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('%') => out.push('%'),
            Some('Y') => out.push_str(&format!("{:04}", parts.year)),
            Some('m') => out.push_str(&format!("{:02}", parts.month)),
            Some('d') => out.push_str(&format!("{:02}", parts.day)),
            Some('H') => out.push_str(&format!("{:02}", parts.hour)),
            Some('M') => out.push_str(&format!("{:02}", parts.minute)),
            Some('S') => out.push_str(&format!("{:02}", parts.second)),
            Some('z') => out.push_str(&format_offset(offset, false)),
            Some('Z') => out.push_str(&zone.label()),
            Some(other) => {
                out.push('%');
                out.push(other);
            }
            None => out.push('%'),
        }
    }
    out
}

fn format_rfc3339(epoch: f64, zone: &Zone) -> String {
    let parts = zone.wall_parts(epoch);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}{}",
        parts.year,
        parts.month,
        parts.day,
        parts.hour,
        parts.minute,
        parts.second,
        format_offset(zone.offset_at(epoch), true)
    )
}

fn format_rfc5322(epoch: f64, zone: &Zone, include_weekday: bool) -> String {
    let parts = zone.wall_parts(epoch);
    let weekday = NaiveDate::from_ymd_opt(parts.year, parts.month, parts.day)
        .map(|date| {
            ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
                [date.weekday().num_days_from_monday() as usize]
        })
        .unwrap_or("Mon");
    let core = format!(
        "{:02} {} {:04} {:02}:{:02}:{:02} {}",
        parts.day,
        month_abbr(parts.month),
        parts.year,
        parts.hour,
        parts.minute,
        parts.second,
        format_offset(zone.offset_at(epoch), false)
    );
    if include_weekday {
        format!("{weekday}, {core}")
    } else {
        core
    }
}

fn native_object(class_name: &str, fields: HashMap<String, Value>) -> Value {
    Value::Object(Rc::new(RefCell::new(ObjectValue {
        class: Rc::new(UserClassValue {
            name: class_name.to_owned(),
            base: None,
            traits: Vec::<Rc<TraitValue>>::new(),
            fields: fields
                .keys()
                .map(|name| FieldSpec {
                    name: name.clone(),
                    declared_type: None,
                    mutable: true,
                    accessors: Vec::new(),
                    default_value: None,
                    is_weak_storage: false,
                })
                .collect(),
            methods: HashMap::<String, Rc<MethodValue>>::new(),
            static_methods: HashMap::<String, Rc<MethodValue>>::new(),
            nested_classes: HashMap::new(),
            source_decl: None,
            closure_env: None,
        }),
        fields: fields.clone(),
        weak_fields: std::collections::HashSet::new(),
        builtin_value: Some(Value::SystemDict(fields)),
    })))
}

fn time_zone_object(zone: String) -> Value {
    Value::Object(Rc::new(RefCell::new(ObjectValue {
        class: Rc::new(UserClassValue {
            name: "TimeZone".to_owned(),
            base: None,
            traits: Vec::<Rc<TraitValue>>::new(),
            fields: vec![FieldSpec {
                name: "name".to_owned(),
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
        fields: HashMap::from([("name".to_owned(), Value::String(zone.clone()))]),
        weak_fields: std::collections::HashSet::new(),
        builtin_value: Some(Value::String(zone)),
    })))
}

fn time_format_object(kind: &str, pattern: &str, zone: Option<String>) -> Result<Value> {
    Ok(native_object(
        "TimeFormat",
        HashMap::from([
            ("kind".to_owned(), Value::String(kind.to_owned())),
            ("pattern".to_owned(), Value::String(pattern.to_owned())),
            (
                "timezone".to_owned(),
                zone.map(Value::String).unwrap_or(Value::Null),
            ),
        ]),
    ))
}

fn duration_map() -> HashMap<String, Value> {
    HashMap::from([
        ("seconds".to_owned(), Value::Number(0.0)),
        ("minutes".to_owned(), Value::Number(0.0)),
        ("hours".to_owned(), Value::Number(0.0)),
        ("days".to_owned(), Value::Number(0.0)),
        ("weeks".to_owned(), Value::Number(0.0)),
        ("months".to_owned(), Value::Number(0.0)),
        ("years".to_owned(), Value::Number(0.0)),
    ])
}

fn duration_single(runtime: &Runtime, unit: &str, args: &[Value]) -> Result<Value> {
    require_arity(unit, args, 1)?;
    let mut values = duration_map();
    values.insert(
        unit.to_owned(),
        Value::Number(runtime.value_to_number(&args[0])?),
    );
    Ok(native_object("Duration", values))
}

fn time_parts(value: &Value) -> Result<(f64, String)> {
    let Value::SystemDict(fields) = value else {
        return Err(ZuzuRustError::runtime("Time has invalid internal value"));
    };
    let epoch = match fields.get("epoch") {
        Some(Value::Number(value)) => *value,
        _ => return Err(ZuzuRustError::runtime("Time has invalid epoch")),
    };
    let zone = match fields.get("timezone") {
        Some(Value::String(value)) => value.clone(),
        _ => "UTC".to_owned(),
    };
    Ok((epoch, zone))
}

fn time_epoch(value: &Value) -> Result<f64> {
    let Value::Object(object) = value else {
        return Err(ZuzuRustError::runtime("Expected Time object"));
    };
    let Some(builtin) = &object.borrow().builtin_value else {
        return Err(ZuzuRustError::runtime("Expected Time object"));
    };
    Ok(time_parts(builtin)?.0)
}

fn zone_arg(runtime: &Runtime, value: &Value) -> Result<String> {
    if let Value::Object(object) = value {
        if object.borrow().class.name == "TimeZone" {
            if let Some(Value::String(zone)) = &object.borrow().builtin_value {
                return Ok(zone.clone());
            }
        }
    }
    let label = runtime.render_value(value)?;
    Ok(Zone::parse(&label)?.label())
}

fn duration_parts(value: &Value) -> Result<HashMap<String, Value>> {
    let Value::SystemDict(fields) = value else {
        return Err(ZuzuRustError::runtime(
            "Duration has invalid internal value",
        ));
    };
    Ok(fields.clone())
}

fn duration_parts_from_value(value: &Value) -> Result<HashMap<String, Value>> {
    let Value::Object(object) = value else {
        return Err(ZuzuRustError::runtime("Expected Duration object"));
    };
    let Some(builtin) = &object.borrow().builtin_value else {
        return Err(ZuzuRustError::runtime("Expected Duration object"));
    };
    duration_parts(builtin)
}

fn format_parts(value: &Value) -> Result<(String, String, Option<String>)> {
    let Value::Object(object) = value else {
        return Err(ZuzuRustError::runtime("Expected TimeFormat object"));
    };
    let Some(builtin) = &object.borrow().builtin_value else {
        return Err(ZuzuRustError::runtime("Expected TimeFormat object"));
    };
    format_raw_parts(builtin)
}

fn format_raw_parts(value: &Value) -> Result<(String, String, Option<String>)> {
    let Value::SystemDict(fields) = value else {
        return Err(ZuzuRustError::runtime(
            "TimeFormat has invalid internal value",
        ));
    };
    let kind = match fields.get("kind") {
        Some(Value::String(value)) => value.clone(),
        _ => "strftime".to_owned(),
    };
    let pattern = match fields.get("pattern") {
        Some(Value::String(value)) => value.clone(),
        _ => String::new(),
    };
    let zone = match fields.get("timezone") {
        Some(Value::String(value)) => Some(value.clone()),
        _ => None,
    };
    Ok((kind, pattern, zone))
}

fn number_field(fields: &HashMap<String, Value>, name: &str) -> Result<f64> {
    match fields.get(name) {
        Some(Value::Number(value)) => Ok(*value),
        _ => Ok(0.0),
    }
}

fn require_arity(name: &str, args: &[Value], expected: usize) -> Result<()> {
    if args.len() != expected {
        return Err(ZuzuRustError::runtime(format!(
            "{name}() expects {expected} argument{}",
            if expected == 1 { "" } else { "s" }
        )));
    }
    Ok(())
}

fn parse_offset(value: &str) -> Option<FixedOffset> {
    if matches!(
        value.to_ascii_uppercase().as_str(),
        "Z" | "UTC" | "UT" | "GMT"
    ) {
        return FixedOffset::east_opt(0);
    }
    let captures = regex::Regex::new(r"^([+-])(\d\d):?(\d\d)$")
        .ok()?
        .captures(value)?;
    let hours = captures[2].parse::<i32>().ok()?;
    let minutes = captures[3].parse::<i32>().ok()?;
    if hours > 23 || minutes > 59 {
        return None;
    }
    let seconds = hours * 3600 + minutes * 60;
    if &captures[1] == "-" {
        FixedOffset::west_opt(seconds)
    } else {
        FixedOffset::east_opt(seconds)
    }
}

fn format_offset(seconds: i32, colon: bool) -> String {
    let sign = if seconds < 0 { '-' } else { '+' };
    let value = seconds.abs();
    if colon {
        format!("{sign}{:02}:{:02}", value / 3600, (value % 3600) / 60)
    } else {
        format!("{sign}{:02}{:02}", value / 3600, (value % 3600) / 60)
    }
}

fn month_number(name: &str) -> Result<u32> {
    match &name.to_ascii_lowercase()[..3] {
        "jan" => Ok(1),
        "feb" => Ok(2),
        "mar" => Ok(3),
        "apr" => Ok(4),
        "may" => Ok(5),
        "jun" => Ok(6),
        "jul" => Ok(7),
        "aug" => Ok(8),
        "sep" => Ok(9),
        "oct" => Ok(10),
        "nov" => Ok(11),
        "dec" => Ok(12),
        _ => Err(ZuzuRustError::runtime("Error parsing time")),
    }
}

fn month_abbr(month: u32) -> &'static str {
    match month {
        1 => "Jan",
        2 => "Feb",
        3 => "Mar",
        4 => "Apr",
        5 => "May",
        6 => "Jun",
        7 => "Jul",
        8 => "Aug",
        9 => "Sep",
        10 => "Oct",
        11 => "Nov",
        12 => "Dec",
        _ => "Jan",
    }
}
