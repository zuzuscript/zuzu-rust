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
        "quotemeta",
        "to_binary",
        "to_string",
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
        "quotemeta" => quotemeta(runtime, args),
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

fn quotemeta(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    require_arity("quotemeta", args, 1)?;
    let text = runtime.render_value(&args[0])?;
    let mut out = String::new();
    for ch in text.chars() {
        if matches!(
            ch,
            '\\' | '/'
                | '^'
                | '$'
                | '.'
                | '|'
                | '?'
                | '*'
                | '+'
                | '('
                | ')'
                | '['
                | ']'
                | '{'
                | '}'
                | '"'
                | '\''
        ) {
            out.push('\\');
        }
        out.push(ch);
    }
    Ok(Value::String(out))
}

fn sprint(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(ZuzuRustError::runtime(
            "sprint() expects at least one argument",
        ));
    }

    let fmt = runtime.render_value(&args[0])?;
    let printf_args = args[1..]
        .iter()
        .map(|arg| ZuzuPrintfArg::new(runtime, arg))
        .collect::<Result<Vec<_>>>()?;
    let mut arg_refs = printf_args
        .iter()
        .map(|arg| arg as &dyn sprintf::Printf)
        .collect::<Vec<_>>();

    loop {
        match sprintf::vsprintf(&fmt, &arg_refs) {
            Ok(text) => return Ok(Value::String(text)),
            Err(sprintf::PrintfError::TooManyArgs) if !arg_refs.is_empty() => {
                arg_refs.pop();
            }
            Err(err) => {
                return Err(ZuzuRustError::runtime(format!(
                    "sprint() format failed: {err}"
                )));
            }
        }
    }
}

struct ZuzuPrintfArg {
    rendered: String,
    number: Option<f64>,
}

impl ZuzuPrintfArg {
    fn new(runtime: &Runtime, value: &Value) -> Result<Self> {
        let rendered = runtime.render_value(value)?;
        let number = match value {
            Value::Number(number) => Some(*number),
            _ => None,
        };
        Ok(Self { rendered, number })
    }
}

impl sprintf::Printf for ZuzuPrintfArg {
    fn format(&self, spec: &sprintf::ConversionSpecifier) -> sprintf::Result<String> {
        if let Some(number) = self.number {
            match (number as i64).format(spec) {
                Ok(text) => return Ok(text),
                Err(sprintf::PrintfError::WrongType) => {}
                Err(err) => return Err(err),
            }
            if number >= 0.0 {
                match (number as u64).format(spec) {
                    Ok(text) => return Ok(text),
                    Err(sprintf::PrintfError::WrongType) => {}
                    Err(err) => return Err(err),
                }
            }
            match number.format(spec) {
                Ok(text) => return Ok(text),
                Err(sprintf::PrintfError::WrongType) => {}
                Err(err) => return Err(err),
            }
        }
        self.rendered.format(spec)
    }

    fn as_int(&self) -> Option<i32> {
        self.number.map(|number| number as i32)
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
