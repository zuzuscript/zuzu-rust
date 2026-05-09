use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::rc::Rc;

use super::super::{IteratorState, ObjectValue, Runtime, TraitValue, UserClassValue, Value};
use super::io::{path_buf_from_value, resolve_fs_path};
use crate::error::{Result, ZuzuRustError};

pub(super) fn exports() -> HashMap<String, Value> {
    HashMap::from([
        ("CSV".to_owned(), Value::builtin_class("CSV".to_owned())),
        (
            "CSVReader".to_owned(),
            Value::builtin_class("CSVReader".to_owned()),
        ),
        (
            "CSVWriter".to_owned(),
            Value::builtin_class("CSVWriter".to_owned()),
        ),
    ])
}

pub(super) fn construct_csv(
    _runtime: &Runtime,
    _args: Vec<Value>,
    named_args: Vec<(String, Value)>,
) -> Result<Value> {
    let mut fields = HashMap::new();
    for (name, value) in named_args {
        fields.insert(name, value);
    }
    Ok(Value::Object(Rc::new(RefCell::new(ObjectValue {
        class: simple_class("CSV"),
        fields: fields.clone(),
        weak_fields: std::collections::HashSet::new(),
        builtin_value: Some(Value::Dict(fields)),
    }))))
}

pub(super) fn call_object_method(
    runtime: &Runtime,
    object: &Rc<RefCell<ObjectValue>>,
    class_name: &str,
    builtin_value: &Value,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    match class_name {
        "CSV" => Some(call_csv_method(runtime, builtin_value, name, args)),
        "CSVReader" => Some(call_csv_reader_method(runtime, object, name, args)),
        "CSVWriter" => Some(call_csv_writer_method(runtime, object, name, args)),
        _ => None,
    }
}

fn call_csv_method(
    runtime: &Runtime,
    builtin_value: &Value,
    name: &str,
    args: &[Value],
) -> Result<Value> {
    let config = CsvConfig::from_value(builtin_value);
    match name {
        "decode" => {
            let text = expect_string_like(args.first(), "CSV.decode")?;
            decode_text(&config, &text, false)
        }
        "decode_report" => {
            let text = expect_string_like(args.first(), "CSV.decode_report")?;
            decode_text(&config, &text, true)
        }
        "encode_row" => {
            let row = args.first().cloned().unwrap_or(Value::Array(Vec::new()));
            Ok(Value::String(encode_row_text(
                &config,
                &row_to_values(&config, &row)?,
            )))
        }
        "load" => {
            let path = expect_path(runtime, args.first(), "CSV.load")?;
            let text = fs::read_to_string(path)
                .map_err(|err| ZuzuRustError::thrown(format!("load failed: {err}")))?;
            decode_text(&config, &text, false)
        }
        "load_report" => {
            let path = expect_path(runtime, args.first(), "CSV.load_report")?;
            let text = fs::read_to_string(path)
                .map_err(|err| ZuzuRustError::thrown(format!("load failed: {err}")))?;
            decode_text(&config, &text, true)
        }
        "dump" => {
            let path = expect_path(runtime, args.first(), "CSV.dump")?;
            let rows = args.get(1).cloned().unwrap_or(Value::Array(Vec::new()));
            let text = encode_rows(&config, &rows)?;
            fs::write(path, text)
                .map_err(|err| ZuzuRustError::thrown(format!("dump failed: {err}")))?;
            Ok(args[0].clone())
        }
        "open" => {
            let path = expect_path(runtime, args.first(), "CSV.open")?;
            let text = fs::read_to_string(path)
                .map_err(|err| ZuzuRustError::thrown(format!("open failed: {err}")))?;
            let parsed = decode_internal(&config, &text);
            Ok(new_csv_reader(&config, parsed))
        }
        "open_writer" => {
            let path = expect_path(runtime, args.first(), "CSV.open_writer")?;
            let mut merged = config.clone();
            if let Some(options) = args.get(1).and_then(dict_items) {
                merged.apply_dict(&options);
            }
            if !merged.append {
                let _ = fs::write(&path, "");
            }
            Ok(new_csv_writer(&path, &merged))
        }
        "sniff" => {
            let text = match args.first() {
                Some(Value::Object(object)) if object.borrow().class.name == "Path" => {
                    let path = expect_path(runtime, args.first(), "CSV.sniff")?;
                    fs::read_to_string(path)
                        .map_err(|err| ZuzuRustError::thrown(format!("sniff failed: {err}")))?
                }
                _ => expect_string_like(args.first(), "CSV.sniff")?,
            };
            Ok(sniff_text(&text))
        }
        "transpose" => {
            let rows = match args.first() {
                Some(Value::Array(rows)) => rows.clone(),
                _ => Vec::new(),
            };
            Ok(transpose_rows(rows))
        }
        "dump_table" => {
            let Some(path) = args.first() else {
                return Err(ZuzuRustError::thrown(
                    "TypeException: CSV.dump_table expects Path as first argument",
                ));
            };
            let path_buf = expect_path(runtime, Some(path), "CSV.dump_table")?;
            let dbh = args.get(1).cloned().unwrap_or(Value::Null);
            let table = args.get(2).and_then(stringify).unwrap_or_default();
            let rows =
                db_query_all_dict(runtime, dbh, format!("select * from {table}"), Vec::new())?;
            let text = encode_rows(&config, &Value::Array(rows))?;
            fs::write(path_buf, text)
                .map_err(|err| ZuzuRustError::thrown(format!("dump failed: {err}")))?;
            Ok(path.clone())
        }
        "dump_query" => {
            let Some(path) = args.first() else {
                return Err(ZuzuRustError::thrown(
                    "TypeException: CSV.dump_query expects Path as first argument",
                ));
            };
            let path_buf = expect_path(runtime, Some(path), "CSV.dump_query")?;
            let dbh = args.get(1).cloned().unwrap_or(Value::Null);
            let sql = args.get(2).and_then(stringify).unwrap_or_default();
            let binds = array_items(args.get(3));
            let mut query_config = config.clone();
            if let Some(options) = args.get(4).and_then(dict_items) {
                query_config.apply_dict(&options);
            }
            let rows = db_query_all_dict(runtime, dbh, sql, binds)?;
            let text = encode_rows(&query_config, &Value::Array(rows))?;
            fs::write(path_buf, text)
                .map_err(|err| ZuzuRustError::thrown(format!("dump failed: {err}")))?;
            Ok(path.clone())
        }
        "load_table" => {
            let path = expect_path(runtime, args.first(), "CSV.load_table")?;
            let dbh = args.get(1).cloned().unwrap_or(Value::Null);
            let table = args.get(2).and_then(stringify).unwrap_or_default();
            let options = args.get(3).and_then(dict_items).unwrap_or_default();
            let text = fs::read_to_string(path)
                .map_err(|err| ZuzuRustError::thrown(format!("load failed: {err}")))?;
            let mut load_config = config.clone();
            load_config.apply_dict(&options);
            let rows = match decode_text(&load_config, &text, false)? {
                Value::Array(rows) => rows,
                _ => Vec::new(),
            };
            if options
                .get("create_table")
                .map(Value::is_truthy)
                .unwrap_or(false)
            {
                let create_sql = build_create_table_sql(&table, &rows, &options);
                let _ = db_exec(runtime, dbh.clone(), create_sql, Vec::new())?;
            }
            if options
                .get("transaction")
                .map(Value::is_truthy)
                .unwrap_or(false)
            {
                let _ = db_call(runtime, dbh.clone(), "begin", Vec::new())?;
            }
            let conflict_ignore = options
                .get("conflict")
                .and_then(stringify)
                .map(|value| value == "ignore")
                .unwrap_or(false);
            let column_map = options
                .get("column_map")
                .and_then(dict_items)
                .unwrap_or_default();
            let mut inserted = 0usize;
            for row in rows {
                let Value::Dict(mut dict) = row else {
                    continue;
                };
                if !column_map.is_empty() {
                    let mut remapped = HashMap::new();
                    for (key, value) in dict.drain() {
                        let mapped = column_map.get(&key).and_then(stringify).unwrap_or(key);
                        remapped.insert(mapped, value);
                    }
                    dict = remapped;
                }
                let columns = dict.keys().cloned().collect::<Vec<_>>();
                if conflict_ignore && should_skip_conflict(runtime, &dbh, &table, &dict)? {
                    inserted += 1;
                    continue;
                }
                let sql = build_insert_sql(&table, &columns);
                let binds = columns
                    .iter()
                    .map(|column| dict.get(column).cloned().unwrap_or(Value::Null))
                    .collect::<Vec<_>>();
                let _ = db_exec(runtime, dbh.clone(), sql, binds)?;
                inserted += 1;
            }
            if options
                .get("transaction")
                .map(Value::is_truthy)
                .unwrap_or(false)
            {
                let _ = db_call(runtime, dbh, "commit", Vec::new())?;
            }
            Ok(Value::Number(inserted as f64))
        }
        _ => Err(ZuzuRustError::thrown(format!(
            "unsupported method '{name}' for CSV"
        ))),
    }
}

fn call_csv_reader_method(
    _runtime: &Runtime,
    object: &Rc<RefCell<ObjectValue>>,
    name: &str,
    args: &[Value],
) -> Result<Value> {
    let _ = args;
    match name {
        "headers" => Ok(object
            .borrow()
            .fields
            .get("headers")
            .cloned()
            .unwrap_or(Value::Array(Vec::new()))),
        "columns" => Ok(active_columns(object)),
        "set_columns" => {
            let value = args.first().cloned().unwrap_or(Value::Array(Vec::new()));
            object
                .borrow_mut()
                .fields
                .insert("columns".to_owned(), value.clone());
            Ok(value)
        }
        "row_number" => Ok(object
            .borrow()
            .fields
            .get("cursor")
            .cloned()
            .unwrap_or(Value::Number(0.0))),
        "errors" => Ok(object
            .borrow()
            .fields
            .get("errors")
            .cloned()
            .unwrap_or(Value::Array(Vec::new()))),
        "skip_lines" => {
            let delta = args.first().and_then(number_value).unwrap_or(0.0).max(0.0);
            let cursor = object
                .borrow()
                .fields
                .get("cursor")
                .and_then(number_value)
                .unwrap_or(0.0);
            let next = cursor + delta;
            object
                .borrow_mut()
                .fields
                .insert("cursor".to_owned(), Value::Number(next));
            Ok(Value::Number(next))
        }
        "next_array" => next_array_row(object),
        "next_dict" => next_dict_row(object),
        "next" => {
            let columns = active_columns(object);
            if matches!(columns, Value::Array(ref values) if !values.is_empty()) {
                next_dict_row(object)
            } else {
                next_array_row(object)
            }
        }
        "all_array" => collect_remaining(object, true),
        "all_dict" => collect_remaining(object, false),
        "to_Iterator" => {
            let items = match collect_remaining(object, false)? {
                Value::Array(rows) => rows,
                _ => Vec::new(),
            };
            Ok(Value::Iterator(Rc::new(RefCell::new(IteratorState {
                items,
                index: 0,
            }))))
        }
        "close" => Ok(Value::Null),
        _ => Err(ZuzuRustError::thrown(format!(
            "unsupported method '{name}' for CSVReader"
        ))),
    }
}

fn call_csv_writer_method(
    _runtime: &Runtime,
    object: &Rc<RefCell<ObjectValue>>,
    name: &str,
    args: &[Value],
) -> Result<Value> {
    match name {
        "write_header" => {
            let columns = if let Some(value) = args.first() {
                array_items(Some(value))
            } else {
                array_items(object.borrow().fields.get("columns"))
            };
            object
                .borrow_mut()
                .fields
                .insert("columns".to_owned(), Value::Array(columns.clone()));
            let line = encode_row_text(&CsvConfig::from_writer_object(object), &columns);
            append_text(object, &line)?;
            Ok(Value::Array(columns))
        }
        "write_row" | "print_row" => {
            let config = CsvConfig::from_writer_object(object);
            let row = args.first().cloned().unwrap_or(Value::Dict(HashMap::new()));
            let values = row_to_values(&config, &row)?;
            let line = encode_row_text(&config, &values);
            append_text(object, &line)?;
            let row_number = object
                .borrow()
                .fields
                .get("row_number")
                .and_then(number_value)
                .unwrap_or(0.0)
                + 1.0;
            object
                .borrow_mut()
                .fields
                .insert("row_number".to_owned(), Value::Number(row_number));
            Ok(Value::Array(values))
        }
        "columns" => Ok(object
            .borrow()
            .fields
            .get("columns")
            .cloned()
            .unwrap_or(Value::Array(Vec::new()))),
        "row_number" => Ok(object
            .borrow()
            .fields
            .get("row_number")
            .cloned()
            .unwrap_or(Value::Number(0.0))),
        "close" => Ok(Value::Null),
        _ => Err(ZuzuRustError::thrown(format!(
            "unsupported method '{name}' for CSVWriter"
        ))),
    }
}

#[derive(Clone, Default)]
struct CsvConfig {
    headers: bool,
    sep_char: char,
    quote_char: char,
    comment_char: Option<char>,
    skip_lines: usize,
    skip_empty_rows: bool,
    trim_headers: bool,
    lowercase_headers: bool,
    duplicate_headers: Option<String>,
    ragged: Option<String>,
    fill_value: String,
    on_error: Option<String>,
    append: bool,
    columns: Vec<String>,
    rename_headers: HashMap<String, String>,
    types: HashMap<String, String>,
    defaults: HashMap<String, Value>,
}

impl CsvConfig {
    fn from_value(value: &Value) -> Self {
        let mut config = Self {
            headers: false,
            sep_char: ',',
            quote_char: '"',
            comment_char: None,
            skip_lines: 0,
            skip_empty_rows: false,
            trim_headers: false,
            lowercase_headers: false,
            duplicate_headers: None,
            ragged: None,
            fill_value: String::new(),
            on_error: None,
            append: false,
            columns: Vec::new(),
            rename_headers: HashMap::new(),
            types: HashMap::new(),
            defaults: HashMap::new(),
        };
        if let Some(fields) = dict_items(value) {
            config.apply_dict(&fields);
        }
        config
    }

    fn from_writer_object(object: &Rc<RefCell<ObjectValue>>) -> Self {
        let mut config = Self::from_value(
            object
                .borrow()
                .fields
                .get("config")
                .unwrap_or(&Value::Dict(HashMap::new())),
        );
        config.columns = array_items(object.borrow().fields.get("columns"))
            .into_iter()
            .filter_map(|value| stringify(&value))
            .collect();
        config
    }

    fn apply_dict(&mut self, fields: &HashMap<String, Value>) {
        if let Some(value) = fields.get("headers") {
            self.headers = value.is_truthy();
        }
        if let Some(value) = fields.get("sep_char").and_then(stringify) {
            self.sep_char = value.chars().next().unwrap_or(',');
        }
        if let Some(value) = fields.get("quote_char").and_then(stringify) {
            self.quote_char = value.chars().next().unwrap_or('"');
        }
        if let Some(value) = fields.get("comment_char").and_then(stringify) {
            self.comment_char = value.chars().next();
        }
        if let Some(value) = fields.get("skip_lines").and_then(number_value) {
            self.skip_lines = value.max(0.0) as usize;
        }
        if let Some(value) = fields.get("skip_empty_rows") {
            self.skip_empty_rows = value.is_truthy();
        }
        if let Some(value) = fields.get("trim_headers") {
            self.trim_headers = value.is_truthy();
        }
        if let Some(value) = fields.get("lowercase_headers") {
            self.lowercase_headers = value.is_truthy();
        }
        if let Some(value) = fields.get("duplicate_headers").and_then(stringify) {
            self.duplicate_headers = Some(value);
        }
        if let Some(value) = fields.get("ragged").and_then(stringify) {
            self.ragged = Some(value);
        }
        if let Some(value) = fields.get("fill_value").and_then(stringify) {
            self.fill_value = value;
        }
        if let Some(value) = fields.get("on_error").and_then(stringify) {
            self.on_error = Some(value);
        }
        if let Some(value) = fields.get("append") {
            self.append = value.is_truthy();
        }
        if fields.contains_key("columns") {
            self.columns = array_items(fields.get("columns"))
                .iter()
                .filter_map(stringify)
                .collect();
        }
        if let Some(rename_headers) = fields.get("rename_headers").and_then(dict_items) {
            self.rename_headers = rename_headers
                .iter()
                .filter_map(|(key, value)| stringify(value).map(|value| (key.clone(), value)))
                .collect();
        }
        if let Some(types) = fields.get("types").and_then(dict_items) {
            self.types = types
                .iter()
                .filter_map(|(key, value)| stringify(value).map(|value| (key.clone(), value)))
                .collect();
        }
        if let Some(defaults) = fields.get("defaults").and_then(dict_items) {
            self.defaults = defaults;
        }
    }
}

#[derive(Clone)]
struct ParsedCsv {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
    errors: Vec<HashMap<String, Value>>,
}

fn decode_text(config: &CsvConfig, text: &str, collect: bool) -> Result<Value> {
    let parsed = decode_internal(config, text);
    if !collect && !parsed.errors.is_empty() {
        return Err(ZuzuRustError::thrown(
            parsed.errors[0]
                .get("message")
                .and_then(stringify)
                .unwrap_or_else(|| "decode failed".to_owned()),
        ));
    }
    if collect {
        return Ok(Value::Dict(HashMap::from([
            (
                "rows".to_owned(),
                build_rows_value(config, &parsed.headers, &parsed.rows),
            ),
            (
                "errors".to_owned(),
                Value::Array(
                    parsed
                        .errors
                        .into_iter()
                        .map(Value::Dict)
                        .collect::<Vec<_>>(),
                ),
            ),
        ])));
    }
    Ok(build_rows_value(config, &parsed.headers, &parsed.rows))
}

fn decode_internal(config: &CsvConfig, text: &str) -> ParsedCsv {
    let mut parsed_rows = Vec::new();
    let mut errors = Vec::new();
    let mut raw_lines = text.replace("\r\n", "\n").replace('\r', "\n");
    if raw_lines.ends_with('\n') {
        raw_lines.pop();
    }
    for (index, line) in raw_lines.split('\n').enumerate() {
        if index < config.skip_lines {
            continue;
        }
        if config.skip_empty_rows && line.is_empty() {
            continue;
        }
        if let Some(comment_char) = config.comment_char {
            if line.starts_with(comment_char) {
                continue;
            }
        }
        match parse_csv_line(line, config.sep_char, config.quote_char) {
            Ok(row) => parsed_rows.push(row),
            Err(message) => {
                errors.push(HashMap::from([
                    ("line".to_owned(), Value::Number((index + 1) as f64)),
                    (
                        "message".to_owned(),
                        Value::String(format!("line {}: {message}", index + 1)),
                    ),
                ]));
                break;
            }
        }
    }
    let mut headers = Vec::new();
    if config.headers && !parsed_rows.is_empty() {
        headers = normalize_headers(config, &parsed_rows.remove(0));
    } else if !config.columns.is_empty() {
        headers = config.columns.clone();
    }
    if !headers.is_empty() {
        for row in &mut parsed_rows {
            if row.len() < headers.len() && config.ragged.as_deref() == Some("fill") {
                while row.len() < headers.len() {
                    row.push(config.fill_value.clone());
                }
            }
        }
    }
    ParsedCsv {
        headers,
        rows: parsed_rows,
        errors,
    }
}

fn build_rows_value(config: &CsvConfig, headers: &[String], rows: &[Vec<String>]) -> Value {
    if !headers.is_empty() {
        Value::Array(
            rows.iter()
                .map(|row| Value::Dict(row_to_dict_with_types(config, headers, row)))
                .collect(),
        )
    } else {
        Value::Array(
            rows.iter()
                .map(|row| Value::Array(row.iter().cloned().map(Value::String).collect::<Vec<_>>()))
                .collect(),
        )
    }
}

fn normalize_headers(config: &CsvConfig, row: &[String]) -> Vec<String> {
    let mut seen = HashMap::<String, usize>::new();
    row.iter()
        .map(|value| {
            let mut name = value.clone();
            if config.trim_headers {
                name = name.trim().to_owned();
            }
            if config.lowercase_headers {
                name = name.to_ascii_lowercase();
            }
            if let Some(rename) = config.rename_headers.get(&name) {
                name = rename.clone();
            }
            if config.duplicate_headers.as_deref() == Some("suffix") {
                let counter = seen.entry(name.clone()).or_insert(0);
                *counter += 1;
                if *counter > 1 {
                    name = format!("{name}_{counter}");
                }
            }
            name
        })
        .collect()
}

fn row_to_dict_with_types(
    config: &CsvConfig,
    headers: &[String],
    row: &[String],
) -> HashMap<String, Value> {
    let mut out = HashMap::new();
    for (index, header) in headers.iter().enumerate() {
        let raw = row.get(index).cloned().unwrap_or_default();
        out.insert(header.clone(), coerce_field(config, header, &raw));
    }
    for (key, value) in &config.defaults {
        out.entry(key.clone()).or_insert_with(|| value.clone());
    }
    out
}

fn coerce_field(config: &CsvConfig, header: &str, raw: &str) -> Value {
    match config.types.get(header).map(String::as_str) {
        Some("integer") => raw
            .parse::<f64>()
            .map(Value::Number)
            .unwrap_or_else(|_| Value::String(raw.to_owned())),
        Some("boolean") => match raw.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Value::Boolean(true),
            "false" | "0" | "no" => Value::Boolean(false),
            _ => Value::String(raw.to_owned()),
        },
        _ => Value::String(raw.to_owned()),
    }
}

fn parse_csv_line(line: &str, sep: char, quote: char) -> std::result::Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut chars = line.chars().peekable();
    let mut in_quotes = false;
    let mut just_closed_quote = false;
    while let Some(ch) = chars.next() {
        if in_quotes {
            if ch == quote {
                if chars.peek() == Some(&quote) {
                    current.push(quote);
                    chars.next();
                } else {
                    in_quotes = false;
                    just_closed_quote = true;
                }
            } else {
                current.push(ch);
            }
            continue;
        }
        if just_closed_quote {
            if ch == sep {
                out.push(current.clone());
                current.clear();
                just_closed_quote = false;
                continue;
            }
            return Err("unexpected character after closing quote".to_owned());
        }
        if ch == quote {
            in_quotes = true;
            continue;
        }
        if ch == sep {
            out.push(current.clone());
            current.clear();
            continue;
        }
        current.push(ch);
    }
    if in_quotes {
        return Err("unterminated quoted field".to_owned());
    }
    out.push(current);
    Ok(out)
}

fn encode_rows(config: &CsvConfig, rows_value: &Value) -> Result<String> {
    let rows = match rows_value {
        Value::Array(rows) => rows.clone(),
        other => {
            return Err(ZuzuRustError::thrown(format!(
                "TypeException: CSV.dump expects Array rows, got {}",
                other.type_name()
            )))
        }
    };
    let mut out = String::new();
    let columns = detect_columns(config, &rows);
    if config.headers && !columns.is_empty() {
        let header_values = columns
            .iter()
            .cloned()
            .map(Value::String)
            .collect::<Vec<_>>();
        out.push_str(&encode_row_text(config, &header_values));
    }
    for row in rows {
        out.push_str(&encode_row_text(
            config,
            &row_to_values_with_columns(config, &row, &columns)?,
        ));
    }
    Ok(out)
}

fn detect_columns(config: &CsvConfig, rows: &[Value]) -> Vec<String> {
    if !config.columns.is_empty() {
        return config.columns.clone();
    }
    for row in rows {
        match row {
            Value::Dict(map) => {
                let mut keys = map.keys().cloned().collect::<Vec<_>>();
                keys.sort();
                return keys;
            }
            Value::Shared(value) => {
                if let Value::Dict(map) = &*value.borrow() {
                    let mut keys = map.keys().cloned().collect::<Vec<_>>();
                    keys.sort();
                    return keys;
                }
            }
            _ => {}
        }
    }
    Vec::new()
}

fn row_to_values(config: &CsvConfig, row: &Value) -> Result<Vec<Value>> {
    let columns = detect_columns(config, std::slice::from_ref(row));
    row_to_values_with_columns(config, row, &columns)
}

fn row_to_values_with_columns(
    _config: &CsvConfig,
    row: &Value,
    columns: &[String],
) -> Result<Vec<Value>> {
    match row {
        Value::Array(values) => Ok(values.clone()),
        Value::Dict(map) => Ok(columns
            .iter()
            .map(|column| map.get(column).cloned().unwrap_or(Value::Null))
            .collect()),
        Value::Shared(value) => {
            row_to_values_with_columns(_config, &value.borrow().clone(), columns)
        }
        other => Err(ZuzuRustError::thrown(format!(
            "TypeException: CSV row expects Array or Dict, got {}",
            other.type_name()
        ))),
    }
}

fn encode_row_text(config: &CsvConfig, values: &[Value]) -> String {
    let sep = config.sep_char.to_string();
    let row = values
        .iter()
        .map(|value| encode_field(config, value))
        .collect::<Vec<_>>()
        .join(&sep);
    format!("{row}\n")
}

fn encode_field(config: &CsvConfig, value: &Value) -> String {
    let text = match value {
        Value::Null => String::new(),
        Value::Boolean(boolean) => {
            if *boolean {
                "true".to_owned()
            } else {
                "false".to_owned()
            }
        }
        Value::Number(number) => {
            if number.fract() == 0.0 {
                format!("{}", *number as i64)
            } else {
                number.to_string()
            }
        }
        Value::String(text) => text.clone(),
        Value::BinaryString(bytes) => String::from_utf8_lossy(bytes).to_string(),
        other => other.render(),
    };
    if text.contains(config.sep_char) || text.contains(config.quote_char) || text.contains('\n') {
        let doubled = text.replace(
            config.quote_char,
            &format!("{}{}", config.quote_char, config.quote_char),
        );
        format!("{}{}{}", config.quote_char, doubled, config.quote_char)
    } else {
        text
    }
}

fn sniff_text(text: &str) -> Value {
    let first_line = text.lines().next().unwrap_or_default();
    let tab_count = first_line.matches('\t').count();
    let comma_count = first_line.matches(',').count();
    let sep = if tab_count > comma_count { "\t" } else { "," };
    let rows = text.lines().take(2).collect::<Vec<_>>();
    let headers = if rows.len() >= 2 {
        rows[0]
            .split(if sep == "\t" { '\t' } else { ',' })
            .all(|field| {
                field
                    .chars()
                    .all(|ch| ch.is_alphabetic() || ch == '_' || ch == ' ')
            })
    } else {
        false
    };
    Value::Dict(HashMap::from([
        ("sep_char".to_owned(), Value::String(sep.to_owned())),
        ("headers".to_owned(), Value::Boolean(headers)),
    ]))
}

fn transpose_rows(rows: Vec<Value>) -> Value {
    let arrays = rows
        .into_iter()
        .filter_map(|row| match row {
            Value::Array(values) => Some(values),
            Value::Shared(value) => match &*value.borrow() {
                Value::Array(values) => Some(values.clone()),
                _ => None,
            },
            _ => None,
        })
        .collect::<Vec<_>>();
    let width = arrays.iter().map(Vec::len).max().unwrap_or(0);
    let mut out = Vec::new();
    for index in 0..width {
        let mut row = Vec::new();
        for source in &arrays {
            row.push(source.get(index).cloned().unwrap_or(Value::Null));
        }
        out.push(Value::Array(row));
    }
    Value::Array(out)
}

fn new_csv_reader(config: &CsvConfig, parsed: ParsedCsv) -> Value {
    let fields = HashMap::from([
        (
            "rows".to_owned(),
            Value::Array(
                parsed
                    .rows
                    .iter()
                    .map(|row| {
                        Value::Array(row.iter().cloned().map(Value::String).collect::<Vec<_>>())
                    })
                    .collect(),
            ),
        ),
        (
            "headers".to_owned(),
            Value::Array(
                parsed
                    .headers
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect::<Vec<_>>(),
            ),
        ),
        ("columns".to_owned(), Value::Array(Vec::new())),
        ("cursor".to_owned(), Value::Number(0.0)),
        (
            "errors".to_owned(),
            Value::Array(parsed.errors.into_iter().map(Value::Dict).collect()),
        ),
        (
            "config".to_owned(),
            Value::Dict(HashMap::from([
                ("headers".to_owned(), Value::Boolean(config.headers)),
                (
                    "sep_char".to_owned(),
                    Value::String(config.sep_char.to_string()),
                ),
            ])),
        ),
    ]);
    Value::Object(Rc::new(RefCell::new(ObjectValue {
        class: simple_class("CSVReader"),
        fields: fields.clone(),
        weak_fields: std::collections::HashSet::new(),
        builtin_value: Some(Value::Dict(fields)),
    })))
}

fn new_csv_writer(path: &std::path::Path, config: &CsvConfig) -> Value {
    let fields = HashMap::from([
        (
            "path".to_owned(),
            Value::String(path.to_string_lossy().to_string()),
        ),
        (
            "config".to_owned(),
            Value::Dict(HashMap::from([
                ("headers".to_owned(), Value::Boolean(config.headers)),
                (
                    "sep_char".to_owned(),
                    Value::String(config.sep_char.to_string()),
                ),
                ("append".to_owned(), Value::Boolean(config.append)),
            ])),
        ),
        (
            "columns".to_owned(),
            Value::Array(
                config
                    .columns
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect::<Vec<_>>(),
            ),
        ),
        ("row_number".to_owned(), Value::Number(0.0)),
    ]);
    Value::Object(Rc::new(RefCell::new(ObjectValue {
        class: simple_class("CSVWriter"),
        fields: fields.clone(),
        weak_fields: std::collections::HashSet::new(),
        builtin_value: Some(Value::Dict(fields)),
    })))
}

fn simple_class(name: &str) -> Rc<UserClassValue> {
    Rc::new(UserClassValue {
        name: name.to_owned(),
        base: None,
        traits: Vec::<Rc<TraitValue>>::new(),
        fields: Vec::new(),
        methods: HashMap::new(),
        static_methods: HashMap::new(),
        nested_classes: HashMap::new(),
        source_decl: None,
        closure_env: None,
    })
}

fn expect_path(
    runtime: &Runtime,
    value: Option<&Value>,
    label: &str,
) -> Result<std::path::PathBuf> {
    match value {
        Some(Value::Object(object)) if object.borrow().class.name == "Path" => Ok(resolve_fs_path(
            runtime,
            &path_buf_from_value(value.unwrap()),
        )),
        _ => Err(ZuzuRustError::thrown(format!(
            "TypeException: {label} expects Path as first argument"
        ))),
    }
}

fn expect_string_like(value: Option<&Value>, label: &str) -> Result<String> {
    match value {
        Some(Value::String(text)) => Ok(text.clone()),
        Some(other) => Err(ZuzuRustError::thrown(format!(
            "TypeException: {label} expects String, got {}",
            other.type_name()
        ))),
        None => Ok(String::new()),
    }
}

fn stringify(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) if number.fract() == 0.0 => Some(format!("{}", *number as i64)),
        Value::Number(number) => Some(number.to_string()),
        Value::Boolean(boolean) => Some(if *boolean {
            "true".to_owned()
        } else {
            "false".to_owned()
        }),
        _ => None,
    }
}

fn number_value(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => Some(*number),
        _ => None,
    }
}

fn array_items(value: Option<&Value>) -> Vec<Value> {
    match value {
        Some(Value::Array(values)) => values.clone(),
        Some(Value::Shared(value)) => match &*value.borrow() {
            Value::Array(values) => values.clone(),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

fn dict_items(value: &Value) -> Option<HashMap<String, Value>> {
    match value {
        Value::Dict(map) => Some(map.clone()),
        Value::Shared(value) => match &*value.borrow() {
            Value::Dict(map) => Some(map.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn active_columns(object: &Rc<RefCell<ObjectValue>>) -> Value {
    let object_ref = object.borrow();
    let columns = object_ref
        .fields
        .get("columns")
        .cloned()
        .unwrap_or(Value::Array(Vec::new()));
    if matches!(&columns, Value::Array(values) if !values.is_empty()) {
        columns
    } else {
        object_ref
            .fields
            .get("headers")
            .cloned()
            .unwrap_or(Value::Array(Vec::new()))
    }
}

fn next_array_row(object: &Rc<RefCell<ObjectValue>>) -> Result<Value> {
    let cursor = object
        .borrow()
        .fields
        .get("cursor")
        .and_then(number_value)
        .unwrap_or(0.0) as usize;
    let row = object
        .borrow()
        .fields
        .get("rows")
        .and_then(|value| match value {
            Value::Array(rows) => rows.get(cursor).cloned(),
            _ => None,
        })
        .unwrap_or(Value::Array(Vec::new()));
    object
        .borrow_mut()
        .fields
        .insert("cursor".to_owned(), Value::Number((cursor + 1) as f64));
    Ok(row)
}

fn next_dict_row(object: &Rc<RefCell<ObjectValue>>) -> Result<Value> {
    let row = next_array_row(object)?;
    let Value::Array(values) = row else {
        return Ok(Value::Dict(HashMap::new()));
    };
    let headers = match active_columns(object) {
        Value::Array(values) => values
            .into_iter()
            .filter_map(|value| stringify(&value))
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };
    if values.is_empty() {
        return Ok(Value::Dict(HashMap::new()));
    }
    let mut out = HashMap::new();
    for (index, header) in headers.iter().enumerate() {
        out.insert(
            header.clone(),
            values.get(index).cloned().unwrap_or(Value::Null),
        );
    }
    Ok(Value::Dict(out))
}

fn collect_remaining(object: &Rc<RefCell<ObjectValue>>, arrays: bool) -> Result<Value> {
    let mut out = Vec::new();
    loop {
        let next = if arrays {
            next_array_row(object)?
        } else {
            next_dict_row(object)?
        };
        let is_empty = matches!(&next, Value::Array(values) if values.is_empty())
            || matches!(&next, Value::Dict(values) if values.is_empty());
        if is_empty {
            break;
        }
        out.push(next);
    }
    Ok(Value::Array(out))
}

fn append_text(object: &Rc<RefCell<ObjectValue>>, text: &str) -> Result<()> {
    let path = object
        .borrow()
        .fields
        .get("path")
        .and_then(stringify)
        .unwrap_or_default();
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|err| ZuzuRustError::thrown(format!("write failed: {err}")))?;
    file.write_all(text.as_bytes())
        .map_err(|err| ZuzuRustError::thrown(format!("write failed: {err}")))?;
    Ok(())
}

fn db_call(runtime: &Runtime, mut receiver: Value, name: &str, args: Vec<Value>) -> Result<Value> {
    runtime.call_method_named(&mut receiver, name, &args, Vec::new())
}

fn db_exec(runtime: &Runtime, dbh: Value, sql: String, binds: Vec<Value>) -> Result<Value> {
    let stmt = db_call(runtime, dbh, "prepare", vec![Value::String(sql)])?;
    let mut stmt_receiver = stmt;
    runtime.call_method_named(&mut stmt_receiver, "execute", &binds, Vec::new())
}

fn db_query_all_dict(
    runtime: &Runtime,
    dbh: Value,
    sql: String,
    binds: Vec<Value>,
) -> Result<Vec<Value>> {
    let stmt = db_call(runtime, dbh, "prepare", vec![Value::String(sql)])?;
    let mut stmt_receiver = stmt;
    let _ = runtime.call_method_named(&mut stmt_receiver, "execute", &binds, Vec::new())?;
    match runtime.call_method_named(&mut stmt_receiver, "all_dict", &[], Vec::new())? {
        Value::Array(rows) => Ok(rows),
        _ => Ok(Vec::new()),
    }
}

fn build_create_table_sql(table: &str, rows: &[Value], options: &HashMap<String, Value>) -> String {
    let mut columns = HashMap::<String, String>::new();
    if let Some(map) = options.get("column_types").and_then(dict_items) {
        for (key, value) in map {
            if let Some(type_name) = stringify(&value) {
                columns.insert(key, type_name);
            }
        }
    }
    if columns.is_empty() {
        for row in rows {
            if let Value::Dict(map) = row {
                for key in map.keys() {
                    columns
                        .entry(key.clone())
                        .or_insert_with(|| "text".to_owned());
                }
                break;
            }
        }
    }
    let mut parts = columns
        .into_iter()
        .map(|(name, type_name)| format!("{name} {type_name}"))
        .collect::<Vec<_>>();
    parts.sort();
    format!("create table {table} ({})", parts.join(", "))
}

fn build_insert_sql(table: &str, columns: &[String]) -> String {
    let placeholders = vec!["?"; columns.len()].join(", ");
    format!(
        "insert into {table} ({}) values ({placeholders})",
        columns.join(", ")
    )
}

fn should_skip_conflict(
    runtime: &Runtime,
    dbh: &Value,
    table: &str,
    dict: &HashMap<String, Value>,
) -> Result<bool> {
    let Some(id) = dict.get("id").cloned() else {
        return Ok(false);
    };
    let rows = db_query_all_dict(
        runtime,
        dbh.clone(),
        format!("select id from {table} where id = ?"),
        vec![id],
    )?;
    Ok(!rows.is_empty())
}
