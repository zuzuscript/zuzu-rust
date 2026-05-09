use std::collections::HashMap;

use super::super::collection::common::require_arity;
use super::super::{Runtime, Value};
use crate::error::{Result, ZuzuRustError};

pub(super) fn exports() -> HashMap<String, Value> {
    let mut exports = HashMap::new();
    for func in [
        "join",
        "substr",
        "trim",
        "chomp",
        "pad",
        "title",
        "snake",
        "kebab",
        "camel",
        "index",
        "rindex",
        "contains",
        "chr",
        "ord",
        "starts_with",
        "ends_with",
        "replace",
        "search",
        "matches",
        "sprint",
        "split",
        "pattern_to_regexp",
    ] {
        exports.insert(func.to_owned(), Value::native_function(func.to_owned()));
    }
    exports
}

pub(super) fn call(runtime: &Runtime, name: &str, args: &[Value]) -> Option<Result<Value>> {
    let value = match name {
        "join" => require_arity(name, args, 2).and_then(|_| {
            let separator = runtime.render_value(&args[0])?;
            match &args[1] {
                Value::Array(values) => Ok(Value::String(
                    values
                        .iter()
                        .map(|value| runtime.render_value(value))
                        .collect::<Result<Vec<_>>>()?
                        .join(&separator),
                )),
                _ => Err(ZuzuRustError::runtime(
                    "join() expects an Array as its second argument",
                )),
            }
        }),
        "trim" => require_arity(name, args, 1).and_then(|_| {
            Ok(Value::String(
                runtime.render_value(&args[0])?.trim().to_owned(),
            ))
        }),
        "pad" => pad(runtime, args),
        "title" => title(runtime, args),
        "snake" => snake(runtime, args),
        "kebab" => kebab(runtime, args),
        "camel" => camel(runtime, args),
        "chomp" => chomp(runtime, args),
        "index" => index(runtime, args),
        "rindex" => rindex(runtime, args),
        "contains" => contains(runtime, args),
        "chr" => chr(runtime, args),
        "ord" => ord(runtime, args),
        "starts_with" => starts_with(runtime, args),
        "ends_with" => ends_with(runtime, args),
        "substr" => substr(runtime, args),
        "pattern_to_regexp" => pattern_to_regexp(runtime, args),
        "search" => search(runtime, args),
        "matches" => matches(runtime, args),
        "replace" => replace(runtime, args),
        "sprint" => sprint(runtime, args),
        "split" => split(runtime, args),
        _ => return None,
    };
    Some(value)
}

fn chomp(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    require_arity("chomp", args, 1)?;
    let mut text = runtime.render_value(&args[0])?;
    if text.ends_with("\r\n") {
        text.truncate(text.len() - 2);
    } else if text.ends_with('\n') || text.ends_with('\r') {
        text.pop();
    }
    Ok(Value::String(text))
}

fn index(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(ZuzuRustError::runtime(
            "index() expects two or three arguments",
        ));
    }
    let text = runtime.render_value(&args[0])?;
    let needle = runtime.render_value(&args[1])?;
    let start = args
        .get(2)
        .map(|value| runtime.value_to_number(value))
        .transpose()?
        .unwrap_or(0.0)
        .max(0.0) as usize;
    let found = if start > text.chars().count() {
        None
    } else {
        let byte_start = byte_index_for_char(&text, start);
        text[byte_start..]
            .find(&needle)
            .map(|offset| char_index_for_byte(&text, byte_start + offset))
    };
    Ok(Value::Number(found.map(|idx| idx as f64).unwrap_or(-1.0)))
}

fn rindex(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(ZuzuRustError::runtime(
            "rindex() expects two or three arguments",
        ));
    }
    let text = runtime.render_value(&args[0])?;
    let needle = runtime.render_value(&args[1])?;
    let upto = args
        .get(2)
        .map(|value| runtime.value_to_number(value))
        .transpose()?
        .map(|value| value.max(0.0) as usize + needle.chars().count())
        .unwrap_or_else(|| text.chars().count())
        .min(text.chars().count());
    let byte_upto = byte_index_for_char(&text, upto);
    let found = text[..byte_upto]
        .rfind(&needle)
        .map(|idx| char_index_for_byte(&text, idx));
    Ok(Value::Number(found.map(|idx| idx as f64).unwrap_or(-1.0)))
}

fn contains(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    require_arity("contains", args, 2)?;
    Ok(Value::Boolean(
        runtime
            .render_value(&args[0])?
            .contains(&runtime.render_value(&args[1])?),
    ))
}

fn byte_index_for_char(text: &str, char_index: usize) -> usize {
    text.char_indices()
        .nth(char_index)
        .map(|(idx, _)| idx)
        .unwrap_or(text.len())
}

fn char_index_for_byte(text: &str, byte_index: usize) -> usize {
    text[..byte_index].chars().count()
}

fn integer_arg(runtime: &Runtime, value: &Value, label: &str) -> Result<i64> {
    let number = runtime.value_to_number(value)?;
    if number.fract() != 0.0 {
        return Err(ZuzuRustError::runtime(format!(
            "{label} expects an integer"
        )));
    }
    Ok(number as i64)
}

fn chr(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    require_arity("chr", args, 1)?;
    let codepoint = integer_arg(runtime, &args[0], "chr()")?;
    if !(0..=0x10FFFF).contains(&codepoint) {
        return Err(ZuzuRustError::runtime(
            "chr() expects a Unicode code point in 0..0x10FFFF",
        ));
    }
    if (0xD800..=0xDFFF).contains(&codepoint) {
        return Err(ZuzuRustError::runtime(
            "chr() rejects surrogate code points",
        ));
    }
    let ch = char::from_u32(codepoint as u32)
        .ok_or_else(|| ZuzuRustError::runtime("chr() expects a valid Unicode scalar value"))?;
    Ok(Value::String(ch.to_string()))
}

fn ord(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(ZuzuRustError::runtime("ord() expects one or two arguments"));
    }
    let text = runtime.render_value(&args[0])?;
    let index = args
        .get(1)
        .map(|value| integer_arg(runtime, value, "ord()"))
        .transpose()?
        .unwrap_or(0);
    if index < 0 {
        return Err(ZuzuRustError::runtime("ord() index out of range"));
    }
    let ch = text
        .chars()
        .nth(index as usize)
        .ok_or_else(|| ZuzuRustError::runtime("ord() index out of range"))?;
    Ok(Value::Number(ch as u32 as f64))
}

fn starts_with(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    require_arity("starts_with", args, 2)?;
    Ok(Value::Boolean(
        runtime
            .render_value(&args[0])?
            .starts_with(&runtime.render_value(&args[1])?),
    ))
}

fn ends_with(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    require_arity("ends_with", args, 2)?;
    Ok(Value::Boolean(
        runtime
            .render_value(&args[0])?
            .ends_with(&runtime.render_value(&args[1])?),
    ))
}

fn substr(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(ZuzuRustError::runtime(
            "substr() expects two or three arguments",
        ));
    }
    let text = runtime.render_value(&args[0])?;
    let chars: Vec<char> = text.chars().collect();
    let start = runtime.value_to_number(&args[1])? as isize;
    let len = args
        .get(2)
        .map(|value| runtime.value_to_number(value))
        .transpose()?;
    let start = if start < 0 { 0 } else { start as usize };
    if start >= chars.len() {
        return Ok(Value::String(String::new()));
    }
    let end = match len {
        Some(len) if len <= 0.0 => start,
        Some(len) => (start + len as usize).min(chars.len()),
        None => chars.len(),
    };
    Ok(Value::String(chars[start..end].iter().collect()))
}

fn pattern_to_regexp(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    if args.len() < 1 || args.len() > 2 {
        return Err(ZuzuRustError::runtime(
            "pattern_to_regexp() expects one or two arguments",
        ));
    }
    let pattern = runtime.render_value(&args[0])?;
    let flags = if matches!(args.get(1), Some(Value::Boolean(true))) {
        "i".to_owned()
    } else {
        String::new()
    };
    Ok(Value::Regex(pattern, flags))
}

fn search(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(ZuzuRustError::runtime(
            "search() expects two or three arguments",
        ));
    }
    let text = runtime.render_value(&args[0])?;
    let (pattern, mut flags) = regex_pattern_and_flags(runtime, &args[1])?;
    if let Some(extra_flags) = args.get(2) {
        flags.push_str(&runtime.render_value(extra_flags)?);
    }
    let regex = runtime.compile_regex(&pattern, &flags)?;
    if let Some(captures) = regex.captures(&text) {
        if let Some(full) = captures.get(0) {
            Ok(Value::String(full.as_str().to_owned()))
        } else {
            Ok(Value::Null)
        }
    } else {
        Ok(Value::Null)
    }
}

fn matches(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(ZuzuRustError::runtime(
            "matches() expects two or three arguments",
        ));
    }
    let text = runtime.render_value(&args[0])?;
    let (pattern, mut flags) = regex_pattern_and_flags(runtime, &args[1])?;
    if let Some(extra_flags) = args.get(2) {
        flags.push_str(&runtime.render_value(extra_flags)?);
    }
    let regex = runtime.compile_regex(&pattern, &flags)?;
    Ok(Value::Boolean(regex.is_match(&text)))
}

fn replace(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    if args.len() < 3 || args.len() > 4 {
        return Err(ZuzuRustError::runtime(
            "replace() expects three or four arguments",
        ));
    }
    let text = runtime.render_value(&args[0])?;
    let replacement = runtime.render_value(&args[2])?;
    let (pattern, mut flags) = regex_pattern_and_flags(runtime, &args[1])?;
    if let Some(extra_flags) = args.get(3) {
        flags.push_str(&runtime.render_value(extra_flags)?);
    }
    let regex = runtime.compile_regex(&pattern, &flags)?;
    let replaced = if flags.contains('g') {
        regex.replace_all(&text, replacement.as_str()).into_owned()
    } else {
        regex.replace(&text, replacement.as_str()).into_owned()
    };
    Ok(Value::String(replaced))
}

fn sprint(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(ZuzuRustError::runtime(
            "sprint() expects at least one argument",
        ));
    }
    let mut fmt = runtime.render_value(&args[0])?;
    for arg in &args[1..] {
        let rendered = runtime.render_value(arg)?;
        if let Some((start, end, replacement)) = format_percent_placeholder(runtime, &fmt, arg)? {
            fmt.replace_range(start..end, &replacement);
            continue;
        }
        if fmt.contains("{}") {
            fmt = fmt.replacen("{}", &rendered, 1);
            continue;
        }
        fmt.push_str(&rendered);
    }
    Ok(Value::String(fmt))
}

fn format_percent_placeholder(
    runtime: &Runtime,
    fmt: &str,
    arg: &Value,
) -> Result<Option<(usize, usize, String)>> {
    let chars: Vec<(usize, char)> = fmt.char_indices().collect();
    let mut i = 0usize;
    while i < chars.len() {
        let (start, ch) = chars[i];
        if ch != '%' {
            i += 1;
            continue;
        }
        if i + 1 < chars.len() && chars[i + 1].1 == '%' {
            i += 2;
            continue;
        }

        let mut j = i + 1;
        let mut zero_pad = false;
        if j < chars.len() && chars[j].1 == '0' {
            zero_pad = true;
            j += 1;
        }

        let width_start = j;
        while j < chars.len() && chars[j].1.is_ascii_digit() {
            j += 1;
        }
        let width = if j > width_start {
            fmt[chars[width_start].0..chars[j].0].parse::<usize>().ok()
        } else {
            None
        };

        let mut precision = None;
        if j < chars.len() && chars[j].1 == '.' {
            j += 1;
            let precision_start = j;
            while j < chars.len() && chars[j].1.is_ascii_digit() {
                j += 1;
            }
            precision = if j > precision_start {
                fmt[chars[precision_start].0..chars[j].0]
                    .parse::<usize>()
                    .ok()
            } else {
                Some(0)
            };
        }

        if j >= chars.len() {
            break;
        }

        let spec = chars[j].1;
        let end = if j + 1 < chars.len() {
            chars[j + 1].0
        } else {
            fmt.len()
        };

        let replacement = match spec {
            'c' => {
                let code = runtime.value_to_number(arg)? as u32;
                let value = char::from_u32(code).unwrap_or('\u{FFFD}').to_string();
                pad_formatted(value, width, false)
            }
            'd' => {
                let value = runtime.value_to_number(arg)? as i64;
                format_integer(value, width, zero_pad)
            }
            'f' => {
                let value = runtime.value_to_number(arg)?;
                format_float(value, width, precision, zero_pad)
            }
            's' => {
                let value = runtime.render_value(arg)?;
                pad_formatted(value, width, false)
            }
            _ => {
                i += 1;
                continue;
            }
        };
        return Ok(Some((start, end, replacement)));
    }

    Ok(None)
}

fn pad_formatted(value: String, width: Option<usize>, zero_pad: bool) -> String {
    match width {
        Some(width) if value.len() < width => {
            let pad = if zero_pad { '0' } else { ' ' };
            format!("{}{}", pad.to_string().repeat(width - value.len()), value)
        }
        _ => value,
    }
}

fn format_integer(value: i64, width: Option<usize>, zero_pad: bool) -> String {
    let abs = value.abs().to_string();
    match width {
        Some(width) if zero_pad && value < 0 && abs.len() + 1 < width => {
            format!("-{}{}", "0".repeat(width - abs.len() - 1), abs)
        }
        Some(width) if zero_pad && abs.len() < width => {
            format!("{}{}", "0".repeat(width - abs.len()), abs)
        }
        Some(width) if abs.len() + usize::from(value < 0) < width => {
            pad_formatted(value.to_string(), Some(width), false)
        }
        _ => value.to_string(),
    }
}

fn format_float(
    value: f64,
    width: Option<usize>,
    precision: Option<usize>,
    zero_pad: bool,
) -> String {
    let base = match precision {
        Some(precision) => format!("{value:.precision$}"),
        None => value.to_string(),
    };
    match width {
        Some(width) if zero_pad => {
            if let Some(rest) = base.strip_prefix('-') {
                if base.len() < width {
                    format!("-{}{}", "0".repeat(width - base.len()), rest)
                } else {
                    base
                }
            } else if base.len() < width {
                format!("{}{}", "0".repeat(width - base.len()), base)
            } else {
                base
            }
        }
        Some(width) => pad_formatted(base, Some(width), false),
        None => base,
    }
}

fn split(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(ZuzuRustError::runtime(
            "split() expects two or three arguments",
        ));
    }
    let text = runtime.render_value(&args[0])?;
    let limit = args
        .get(2)
        .map(|value| runtime.value_to_number(value))
        .transpose()?
        .map(|value| value.max(0.0) as usize);
    let values = match &args[1] {
        Value::Regex(pattern, flags) => {
            let regex = runtime.compile_regex(pattern, flags)?;
            match limit {
                Some(limit) if limit > 0 => regex
                    .splitn(&text, limit)
                    .map(|item| Value::String(item.to_owned()))
                    .collect(),
                _ => regex
                    .split(&text)
                    .map(|item| Value::String(item.to_owned()))
                    .collect(),
            }
        }
        other => {
            let separator = runtime.render_value(other)?;
            if separator.is_empty() {
                match limit {
                    Some(limit) if limit > 0 => text
                        .chars()
                        .take(limit)
                        .map(|ch| Value::String(ch.to_string()))
                        .collect(),
                    _ => text
                        .chars()
                        .map(|ch| Value::String(ch.to_string()))
                        .collect(),
                }
            } else {
                match limit {
                    Some(limit) if limit > 0 => text
                        .splitn(limit, &separator)
                        .map(|item| Value::String(item.to_owned()))
                        .collect(),
                    _ => text
                        .split(&separator)
                        .map(|item| Value::String(item.to_owned()))
                        .collect(),
                }
            }
        }
    };
    Ok(Value::Array(values))
}

fn regex_pattern_and_flags(runtime: &Runtime, value: &Value) -> Result<(String, String)> {
    match value {
        Value::Regex(pattern, flags) => Ok((pattern.clone(), flags.clone())),
        other => Ok((runtime.render_value(other)?, String::new())),
    }
}

fn pad(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 4 {
        return Err(ZuzuRustError::runtime(
            "pad() expects two to four arguments",
        ));
    }
    let text = runtime.render_value(&args[0])?;
    let width = runtime.value_to_number(&args[1])?.max(0.0) as usize;
    let fill = args
        .get(2)
        .map(|value| runtime.render_value(value))
        .transpose()?
        .unwrap_or_else(|| " ".to_owned());
    let side = args
        .get(3)
        .map(|value| runtime.render_value(value))
        .transpose()?
        .unwrap_or_else(|| "right".to_owned());
    if text.len() >= width || fill.is_empty() {
        return Ok(Value::String(text));
    }
    let mut padding = String::new();
    while text.len() + padding.len() < width {
        padding.push_str(&fill);
    }
    padding.truncate(width.saturating_sub(text.len()));
    if side == "left" {
        Ok(Value::String(format!("{padding}{text}")))
    } else {
        Ok(Value::String(format!("{text}{padding}")))
    }
}

fn title(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    require_arity("title", args, 1)?;
    Ok(Value::String(
        split_words(&runtime.render_value(&args[0])?)
            .into_iter()
            .map(title_word)
            .collect::<Vec<_>>()
            .join(" "),
    ))
}

fn snake(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    require_arity("snake", args, 1)?;
    Ok(Value::String(
        split_words(&runtime.render_value(&args[0])?).join("_"),
    ))
}

fn kebab(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    require_arity("kebab", args, 1)?;
    Ok(Value::String(
        split_words(&runtime.render_value(&args[0])?).join("-"),
    ))
}

fn camel(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    require_arity("camel", args, 1)?;
    let mut words = split_words(&runtime.render_value(&args[0])?).into_iter();
    let Some(mut first) = words.next() else {
        return Ok(Value::String(String::new()));
    };
    let mut out = std::mem::take(&mut first);
    for word in words {
        out.push_str(&title_word(word));
    }
    Ok(Value::String(out))
}

fn title_word(word: String) -> String {
    let mut chars = word.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    let mut out = String::new();
    out.extend(first.to_uppercase());
    out.extend(chars);
    out
}

fn split_words(text: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    let chars = text.chars().collect::<Vec<_>>();
    for (index, ch) in chars.iter().copied().enumerate() {
        let prev = index.checked_sub(1).and_then(|idx| chars.get(idx)).copied();
        let next = chars.get(index + 1).copied();
        let boundary = matches!(ch, '_' | '-' | ' ' | '\t' | '\n' | '\r')
            || (ch.is_uppercase() && prev.map(|value| value.is_lowercase()).unwrap_or(false))
            || (ch.is_uppercase()
                && prev.map(|value| value.is_uppercase()).unwrap_or(false)
                && next.map(|value| value.is_lowercase()).unwrap_or(false));
        if matches!(ch, '_' | '-' | ' ' | '\t' | '\n' | '\r') {
            if !current.is_empty() {
                words.push(current.to_ascii_lowercase());
                current = String::new();
            }
            continue;
        }
        if boundary && !current.is_empty() {
            words.push(current.to_ascii_lowercase());
            current = String::new();
        }
        current.push(ch);
    }
    if !current.is_empty() {
        words.push(current.to_ascii_lowercase());
    }
    words
}
