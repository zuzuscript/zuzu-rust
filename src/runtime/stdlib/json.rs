use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::rc::Rc;

use super::super::{
    FieldSpec, MethodValue, ObjectValue, Runtime, TraitValue, UserClassValue, Value,
};
use super::io::{path_buf_from_value, resolve_fs_path};
use crate::error::{Result, ZuzuRustError};

pub(super) fn exports() -> HashMap<String, Value> {
    let mut exports = HashMap::new();
    exports.insert("JSON".to_owned(), Value::builtin_class("JSON".to_owned()));
    exports
}

fn json_class() -> Rc<UserClassValue> {
    Rc::new(UserClassValue {
        name: "JSON".to_owned(),
        base: None,
        traits: Vec::<Rc<TraitValue>>::new(),
        fields: vec![
            FieldSpec {
                name: "pretty".to_owned(),
                declared_type: Some("Boolean".to_owned()),
                mutable: true,
                accessors: Vec::new(),
                default_value: None,
                is_weak_storage: false,
            },
            FieldSpec {
                name: "canonical".to_owned(),
                declared_type: Some("Boolean".to_owned()),
                mutable: true,
                accessors: Vec::new(),
                default_value: None,
                is_weak_storage: false,
            },
            FieldSpec {
                name: "pairlists".to_owned(),
                declared_type: Some("Boolean".to_owned()),
                mutable: true,
                accessors: Vec::new(),
                default_value: None,
                is_weak_storage: false,
            },
        ],
        methods: HashMap::<String, Rc<MethodValue>>::new(),
        static_methods: HashMap::<String, Rc<MethodValue>>::new(),
        nested_classes: HashMap::new(),
        source_decl: None,
        closure_env: None,
    })
}

pub(super) fn construct_json(_args: Vec<Value>, named_args: Vec<(String, Value)>) -> Result<Value> {
    let mut fields = HashMap::new();
    fields.insert("pretty".to_owned(), Value::Boolean(false));
    fields.insert("canonical".to_owned(), Value::Boolean(false));
    fields.insert("pairlists".to_owned(), Value::Boolean(false));
    for (name, value) in named_args {
        fields.insert(name, value);
    }
    Ok(Value::Object(Rc::new(RefCell::new(ObjectValue {
        class: json_class(),
        fields: fields.clone(),
        weak_fields: std::collections::HashSet::new(),
        builtin_value: Some(Value::Dict(fields)),
    }))))
}

pub(super) fn call_object_method(
    runtime: &Runtime,
    class_name: &str,
    builtin_value: &Value,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    if class_name != "JSON" {
        return None;
    }
    let Value::Dict(fields) = builtin_value else {
        return Some(Err(ZuzuRustError::runtime(
            "internal error: JSON object missing config",
        )));
    };
    let pretty = fields.get("pretty").map(Value::is_truthy).unwrap_or(false);
    let canonical = fields
        .get("canonical")
        .map(Value::is_truthy)
        .unwrap_or(false);
    let pairlists = fields
        .get("pairlists")
        .map(Value::is_truthy)
        .unwrap_or(false);

    let value = match name {
        "encode" => {
            let source = args.first().cloned().unwrap_or(Value::Null);
            Ok(Value::String(encode_json_value(
                &source, pretty, canonical, 0,
            )))
        }
        "decode" => match args.first() {
            Some(value) => match runtime.render_value(value) {
                Ok(text) => parse_json_text(&text, pairlists),
                Err(err) => Err(err),
            },
            None => parse_json_text("", pairlists),
        },
        "load" => {
            let Some(target) = args.first() else {
                return Some(Err(ZuzuRustError::thrown(
                    "TypeException: JSON.load expects Path as first argument",
                )));
            };
            match extract_path(runtime, target, "JSON.load") {
                Ok(path) => match fs::read_to_string(path) {
                    Ok(text) => parse_json_text(&text, pairlists),
                    Err(err) => Err(ZuzuRustError::thrown(format!("load failed: {err}"))),
                },
                Err(err) => Err(err),
            }
        }
        "dump" => {
            if args.is_empty() {
                return Some(Err(ZuzuRustError::thrown(
                    "TypeException: JSON.dump expects Path as first argument",
                )));
            }
            match extract_path(runtime, &args[0], "JSON.dump") {
                Ok(path) => {
                    let payload = encode_json_value(
                        &args.get(1).cloned().unwrap_or(Value::Null),
                        pretty,
                        canonical,
                        0,
                    );
                    match fs::write(path, payload) {
                        Ok(()) => Ok(args[0].clone()),
                        Err(err) => Err(ZuzuRustError::thrown(format!("dump failed: {err}"))),
                    }
                }
                Err(err) => Err(err),
            }
        }
        _ => return None,
    };
    Some(value)
}

fn extract_path(runtime: &Runtime, value: &Value, method_name: &str) -> Result<std::path::PathBuf> {
    match value {
        Value::Object(object) if object.borrow().class.name == "Path" => {
            Ok(resolve_fs_path(runtime, &path_buf_from_value(value)))
        }
        _ => Err(ZuzuRustError::thrown(format!(
            "TypeException: {method_name} expects Path as first argument"
        ))),
    }
}

pub(super) fn parse_json_text(text: &str, pairlists: bool) -> Result<Value> {
    let mut parser = JsonParser::new(text, pairlists);
    let value = parser.parse_value()?;
    parser.skip_ws();
    if !parser.is_eof() {
        return Err(ZuzuRustError::thrown("decode failed: trailing characters"));
    }
    Ok(value)
}

struct JsonParser<'a> {
    text: &'a str,
    index: usize,
    pairlists: bool,
}

impl<'a> JsonParser<'a> {
    fn new(text: &'a str, pairlists: bool) -> Self {
        Self {
            text,
            index: 0,
            pairlists,
        }
    }

    fn is_eof(&self) -> bool {
        self.index >= self.text.len()
    }

    fn rest(&self) -> &'a str {
        &self.text[self.index..]
    }

    fn next_char(&self) -> Option<char> {
        self.rest().chars().next()
    }

    fn bump(&mut self) -> Option<char> {
        let ch = self.next_char()?;
        self.index += ch.len_utf8();
        Some(ch)
    }

    fn skip_ws(&mut self) {
        while matches!(self.next_char(), Some(ch) if ch.is_whitespace()) {
            self.bump();
        }
    }

    fn parse_value(&mut self) -> Result<Value> {
        self.skip_ws();
        match self.next_char() {
            Some('{') => self.parse_object(),
            Some('[') => self.parse_array(),
            Some('"') => self.parse_string().map(Value::String),
            Some('t') => self.expect_keyword("true", Value::Boolean(true)),
            Some('f') => self.expect_keyword("false", Value::Boolean(false)),
            Some('n') => self.expect_keyword("null", Value::Null),
            Some('-' | '0'..='9') => self.parse_number(),
            _ => Err(ZuzuRustError::thrown("decode failed: invalid JSON value")),
        }
    }

    fn expect_keyword(&mut self, keyword: &str, value: Value) -> Result<Value> {
        if self.rest().starts_with(keyword) {
            self.index += keyword.len();
            Ok(value)
        } else {
            Err(ZuzuRustError::thrown("decode failed: invalid literal"))
        }
    }

    fn parse_object(&mut self) -> Result<Value> {
        self.bump();
        self.skip_ws();
        let mut dict = HashMap::new();
        let mut pairlist = Vec::new();
        if self.next_char() == Some('}') {
            self.bump();
            return Ok(if self.pairlists {
                Value::PairList(pairlist)
            } else {
                Value::Dict(dict)
            });
        }
        loop {
            self.skip_ws();
            let key = self.parse_string()?;
            self.skip_ws();
            if self.bump() != Some(':') {
                return Err(ZuzuRustError::thrown("decode failed: expected ':'"));
            }
            let value = self.parse_value()?;
            if self.pairlists {
                pairlist.push((key, value));
            } else {
                dict.insert(key, value);
            }
            self.skip_ws();
            match self.bump() {
                Some('}') => break,
                Some(',') => continue,
                _ => return Err(ZuzuRustError::thrown("decode failed: expected ',' or '}'")),
            }
        }
        Ok(if self.pairlists {
            Value::PairList(pairlist)
        } else {
            Value::Dict(dict)
        })
    }

    fn parse_array(&mut self) -> Result<Value> {
        self.bump();
        self.skip_ws();
        let mut items = Vec::new();
        if self.next_char() == Some(']') {
            self.bump();
            return Ok(Value::Array(items));
        }
        loop {
            items.push(self.parse_value()?);
            self.skip_ws();
            match self.bump() {
                Some(']') => break,
                Some(',') => continue,
                _ => return Err(ZuzuRustError::thrown("decode failed: expected ',' or ']'")),
            }
        }
        Ok(Value::Array(items))
    }

    fn parse_string(&mut self) -> Result<String> {
        if self.bump() != Some('"') {
            return Err(ZuzuRustError::thrown("decode failed: expected string"));
        }
        let mut out = String::new();
        while let Some(ch) = self.bump() {
            match ch {
                '"' => return Ok(out),
                '\\' => {
                    let escaped = self
                        .bump()
                        .ok_or_else(|| ZuzuRustError::thrown("decode failed: truncated escape"))?;
                    match escaped {
                        '"' | '\\' | '/' => out.push(escaped),
                        'b' => out.push('\u{0008}'),
                        'f' => out.push('\u{000c}'),
                        'n' => out.push('\n'),
                        'r' => out.push('\r'),
                        't' => out.push('\t'),
                        'u' => {
                            let hex = self.take_n(4)?;
                            let value = u16::from_str_radix(&hex, 16).map_err(|_| {
                                ZuzuRustError::thrown("decode failed: bad unicode escape")
                            })?;
                            let Some(chr) = char::from_u32(value as u32) else {
                                return Err(ZuzuRustError::thrown(
                                    "decode failed: bad unicode scalar",
                                ));
                            };
                            out.push(chr);
                        }
                        _ => {
                            return Err(ZuzuRustError::thrown(
                                "decode failed: invalid escape sequence",
                            ))
                        }
                    }
                }
                _ => out.push(ch),
            }
        }
        Err(ZuzuRustError::thrown("decode failed: unterminated string"))
    }

    fn take_n(&mut self, n: usize) -> Result<String> {
        let mut out = String::new();
        for _ in 0..n {
            out.push(
                self.bump()
                    .ok_or_else(|| ZuzuRustError::thrown("decode failed: unexpected EOF"))?,
            );
        }
        Ok(out)
    }

    fn parse_number(&mut self) -> Result<Value> {
        let start = self.index;
        if self.next_char() == Some('-') {
            self.bump();
        }
        match self.next_char() {
            Some('0') => {
                self.bump();
            }
            Some('1'..='9') => {
                self.bump();
                while matches!(self.next_char(), Some('0'..='9')) {
                    self.bump();
                }
            }
            _ => return Err(ZuzuRustError::thrown("decode failed: invalid number")),
        }
        if self.next_char() == Some('.') {
            self.bump();
            if !matches!(self.next_char(), Some('0'..='9')) {
                return Err(ZuzuRustError::thrown("decode failed: invalid number"));
            }
            while matches!(self.next_char(), Some('0'..='9')) {
                self.bump();
            }
        }
        if matches!(self.next_char(), Some('e' | 'E')) {
            self.bump();
            if matches!(self.next_char(), Some('+' | '-')) {
                self.bump();
            }
            if !matches!(self.next_char(), Some('0'..='9')) {
                return Err(ZuzuRustError::thrown("decode failed: invalid exponent"));
            }
            while matches!(self.next_char(), Some('0'..='9')) {
                self.bump();
            }
        }
        let number = self.text[start..self.index]
            .parse::<f64>()
            .map_err(|_| ZuzuRustError::thrown("decode failed: invalid number"))?;
        Ok(Value::Number(number))
    }
}

fn encode_json_value(value: &Value, pretty: bool, canonical: bool, indent: usize) -> String {
    match value {
        Value::Shared(value) => encode_json_value(&value.borrow(), pretty, canonical, indent),
        Value::Null => "null".to_owned(),
        Value::Boolean(true) => "true".to_owned(),
        Value::Boolean(false) => "false".to_owned(),
        Value::Number(number) => {
            if number.fract() == 0.0 {
                format!("{}", *number as i64)
            } else {
                number.to_string()
            }
        }
        Value::String(text) => format!("\"{}\"", escape_json_string(text)),
        Value::BinaryString(bytes) => {
            let text = String::from_utf8_lossy(bytes);
            format!("\"{}\"", escape_json_string(&text))
        }
        Value::Array(items) => {
            let child_indent = indent + 2;
            let encoded = items
                .iter()
                .map(|item| encode_json_value(item, pretty, canonical, child_indent))
                .collect::<Vec<_>>();
            if pretty && !encoded.is_empty() {
                format!(
                    "[\n{}\n{}]",
                    encoded
                        .iter()
                        .map(|item| format!("{}{}", " ".repeat(child_indent), item))
                        .collect::<Vec<_>>()
                        .join(",\n"),
                    " ".repeat(indent)
                )
            } else {
                format!("[{}]", encoded.join(","))
            }
        }
        Value::Set(items) | Value::Bag(items) => {
            let mut items = items.clone();
            items.sort_by(|left, right| left.render().cmp(&right.render()));
            encode_json_value(&Value::Array(items), pretty, canonical, indent)
        }
        Value::Dict(map) => {
            let mut keys = map.keys().cloned().collect::<Vec<_>>();
            if canonical {
                keys.sort();
            }
            encode_json_object(
                keys.into_iter()
                    .map(|key| (key.clone(), map.get(&key).cloned().unwrap_or(Value::Null)))
                    .collect(),
                pretty,
                canonical,
                indent,
            )
        }
        Value::PairList(items) => encode_json_object(items.clone(), pretty, canonical, indent),
        Value::Pair(key, value) => encode_json_object(
            vec![(key.clone(), (**value).clone())],
            pretty,
            canonical,
            indent,
        ),
        _ => format!("\"{}\"", escape_json_string(&value.render())),
    }
}

fn encode_json_object(
    mut items: Vec<(String, Value)>,
    pretty: bool,
    canonical: bool,
    indent: usize,
) -> String {
    if canonical {
        items.sort_by(|left, right| left.0.cmp(&right.0));
    }
    let child_indent = indent + 2;
    let encoded = items
        .into_iter()
        .map(|(key, value)| {
            let rendered = encode_json_value(&value, pretty, canonical, child_indent);
            if pretty {
                format!(
                    "{}\"{}\": {}",
                    " ".repeat(child_indent),
                    escape_json_string(&key),
                    rendered
                )
            } else {
                format!("\"{}\":{}", escape_json_string(&key), rendered)
            }
        })
        .collect::<Vec<_>>();
    if pretty && !encoded.is_empty() {
        format!("{{\n{}\n{}}}", encoded.join(",\n"), " ".repeat(indent))
    } else {
        format!("{{{}}}", encoded.join(","))
    }
}

fn escape_json_string(text: &str) -> String {
    let mut out = String::new();
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{0008}' => out.push_str("\\b"),
            '\u{000c}' => out.push_str("\\f"),
            ch if ch.is_control() => out.push_str(&format!("\\u{:04x}", ch as u32)),
            _ => out.push(ch),
        }
    }
    out
}
