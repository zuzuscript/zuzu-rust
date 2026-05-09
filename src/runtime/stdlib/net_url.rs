use std::collections::HashMap;

use super::super::{Runtime, Value};
use crate::error::{Result, ZuzuRustError};

pub(super) fn exports() -> HashMap<String, Value> {
    HashMap::from([
        (
            "escape".to_owned(),
            Value::native_function("escape".to_owned()),
        ),
        (
            "unescape".to_owned(),
            Value::native_function("unescape".to_owned()),
        ),
        (
            "parse".to_owned(),
            Value::native_function("parse".to_owned()),
        ),
        (
            "fill_template".to_owned(),
            Value::native_function("fill_template".to_owned()),
        ),
    ])
}

pub(super) fn call(runtime: &Runtime, name: &str, args: &[Value]) -> Option<Result<Value>> {
    let value = match name {
        "escape" => Some(escape(runtime, args)),
        "unescape" => Some(unescape(runtime, args)),
        "parse" => Some(parse_url(runtime, args)),
        "fill_template" => Some(fill_template(runtime, args)),
        _ => None,
    }?;
    Some(value)
}

fn escape(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    let text = args
        .first()
        .map(|value| runtime.render_value(value))
        .transpose()?
        .unwrap_or_default();
    Ok(Value::String(percent_encode(&text)))
}

fn unescape(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    let text = args
        .first()
        .map(|value| runtime.render_value(value))
        .transpose()?
        .unwrap_or_default();
    Ok(Value::String(percent_decode(&text)))
}

fn parse_url(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    let text = args
        .first()
        .map(|value| runtime.render_value(value))
        .transpose()?
        .unwrap_or_default();
    let mut out = HashMap::new();
    out.insert("url".to_owned(), Value::String(text.clone()));
    out.insert("scheme".to_owned(), Value::Null);
    out.insert("authority".to_owned(), Value::Null);
    out.insert("userinfo".to_owned(), Value::Null);
    out.insert("host".to_owned(), Value::Null);
    out.insert("port".to_owned(), Value::Null);
    out.insert("path".to_owned(), Value::String(String::new()));
    out.insert("query".to_owned(), Value::Null);
    out.insert("fragment".to_owned(), Value::Null);
    out.insert("query_params".to_owned(), Value::Dict(HashMap::new()));

    let (scheme, rest) = if let Some((scheme, rest)) = text.split_once("://") {
        (Some(scheme.to_owned()), rest.to_owned())
    } else {
        (None, text.clone())
    };
    if let Some(scheme) = scheme {
        out.insert("scheme".to_owned(), Value::String(scheme));
    }

    let (before_fragment, fragment) = if let Some((head, frag)) = rest.split_once('#') {
        (head.to_owned(), Some(frag.to_owned()))
    } else {
        (rest, None)
    };
    if let Some(fragment) = fragment {
        out.insert("fragment".to_owned(), Value::String(fragment));
    }

    let (before_query, query) = if let Some((head, query)) = before_fragment.split_once('?') {
        (head.to_owned(), Some(query.to_owned()))
    } else {
        (before_fragment, None)
    };
    if let Some(query) = &query {
        out.insert("query".to_owned(), Value::String(query.clone()));
        out.insert(
            "query_params".to_owned(),
            Value::Dict(parse_query_params(query)),
        );
    }

    let (authority, path) = if before_query.starts_with('/') {
        (None, before_query)
    } else if let Some((authority, path)) = before_query.split_once('/') {
        (Some(authority.to_owned()), format!("/{path}"))
    } else {
        (Some(before_query), String::new())
    };

    out.insert("path".to_owned(), Value::String(path));
    if let Some(authority) = authority {
        out.insert("authority".to_owned(), Value::String(authority.clone()));
        let (userinfo, host_port) = if let Some((userinfo, host_port)) = authority.rsplit_once('@')
        {
            (Some(userinfo.to_owned()), host_port.to_owned())
        } else {
            (None, authority)
        };
        if let Some(userinfo) = userinfo {
            out.insert("userinfo".to_owned(), Value::String(userinfo));
        }
        let (host, port) = if let Some((host, port)) = host_port.rsplit_once(':') {
            if port.chars().all(|ch| ch.is_ascii_digit()) {
                (host.to_owned(), Some(port.to_owned()))
            } else {
                (host_port, None)
            }
        } else {
            (host_port, None)
        };
        out.insert("host".to_owned(), Value::String(host));
        if let Some(port) = port {
            out.insert("port".to_owned(), Value::String(port));
        }
    }

    Ok(Value::Dict(out))
}

fn fill_template(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(ZuzuRustError::runtime(
            "fill_template() expects two arguments",
        ));
    }
    let template = runtime.render_value(&args[0])?;
    let values = match &args[1] {
        Value::Dict(map) => map.clone(),
        Value::PairList(items) => items.iter().cloned().collect(),
        _ => HashMap::new(),
    };
    let mut out = String::new();
    let mut rest = template.as_str();
    while let Some(start) = rest.find('{') {
        out.push_str(&rest[..start]);
        let remainder = &rest[start + 1..];
        let Some(end) = remainder.find('}') else {
            return Err(ZuzuRustError::runtime("invalid URL template"));
        };
        let token = &remainder[..end];
        if let Some(names) = token.strip_prefix('?') {
            let mut pairs = Vec::new();
            for name in names
                .split(',')
                .map(str::trim)
                .filter(|name| !name.is_empty())
            {
                if let Some(value) = values.get(name) {
                    let rendered = runtime.render_value(value)?;
                    pairs.push(format!(
                        "{}={}",
                        percent_encode(name),
                        percent_encode(&rendered)
                    ));
                }
            }
            if !pairs.is_empty() {
                out.push('?');
                out.push_str(&pairs.join("&"));
            }
        } else if let Some(value) = values.get(token) {
            out.push_str(&percent_encode(&runtime.render_value(value)?));
        }
        rest = &remainder[end + 1..];
    }
    out.push_str(rest);
    Ok(Value::String(out))
}

fn parse_query_params(query: &str) -> HashMap<String, Value> {
    let mut out = HashMap::new();
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        out.insert(percent_decode(key), Value::String(percent_decode(value)));
    }
    out
}

fn percent_encode(text: &str) -> String {
    let mut out = String::new();
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(byte as char);
        } else {
            out.push('%');
            out.push_str(&format!("{byte:02X}"));
        }
    }
    out
}

fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            let hex = &text[index + 1..index + 3];
            if let Ok(value) = u8::from_str_radix(hex, 16) {
                out.push(value);
                index += 3;
                continue;
            }
        }
        out.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}
