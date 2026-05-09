use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::io::{self, IsTerminal, Write};
use std::path::{Path, MAIN_SEPARATOR};

use rustyline::completion::{Completer, Pair};
use rustyline::config::BellStyle;
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::history::DefaultHistory;
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::{CompletionType, Config, Context, Editor, Helper};

use super::super::collection::common::require_arity;
use super::super::{Runtime, Value};
use crate::error::Result;

pub(super) fn exports() -> HashMap<String, Value> {
    let mut exports = HashMap::new();
    for name in [
        "ansi_esc",
        "supports_ansi",
        "colour_text",
        "write",
        "write_line",
        "readline",
        "readline_supports_completion",
        "filename_completions",
        "directory_completions",
    ] {
        exports.insert(name.to_owned(), Value::native_function(name.to_owned()));
    }
    exports
}

fn supports_ansi() -> bool {
    if std::env::var_os("NO_COLOR").is_some() {
        return false;
    }
    if !io::stdout().is_terminal() {
        return false;
    }
    if cfg!(windows) {
        return std::env::var_os("ANSICON").is_some()
            || std::env::var_os("WT_SESSION").is_some()
            || std::env::var_os("ConEmuANSI").is_some()
            || std::env::var_os("TERM_PROGRAM").is_some();
    }
    let term = std::env::var("TERM").unwrap_or_default();
    if term.is_empty() || term == "dumb" {
        return false;
    }
    true
}

fn ansi_code(colour: &str) -> Option<u8> {
    match colour.to_ascii_lowercase().as_str() {
        "black" => Some(30),
        "red" => Some(31),
        "green" => Some(32),
        "yellow" => Some(33),
        "blue" => Some(34),
        "magenta" => Some(35),
        "cyan" => Some(36),
        "white" => Some(37),
        _ => None,
    }
}

fn value_text(runtime: &Runtime, value: Option<&Value>) -> Result<String> {
    match value {
        Some(Value::Null) | None => Ok(String::new()),
        Some(value) => runtime.render_value(value),
    }
}

fn colour_text(runtime: &Runtime, text: &Value, colour: &Value) -> Result<Value> {
    let text = value_text(runtime, Some(text))?;
    let colour = value_text(runtime, Some(colour))?;
    let Some(code) = ansi_code(&colour) else {
        return Ok(Value::String(text));
    };
    if !supports_ansi() {
        return Ok(Value::String(text));
    }
    Ok(Value::String(format!("\u{1b}[{code}m{text}\u{1b}[0m")))
}

fn completion_values(value: Value) -> Vec<Pair> {
    match value {
        Value::Array(values) | Value::Set(values) | Value::Bag(values) => values
            .into_iter()
            .map(|value| value.render())
            .map(|display| Pair {
                replacement: display.clone(),
                display,
            })
            .collect(),
        value => vec![Pair {
            replacement: value.render(),
            display: value.render(),
        }],
    }
}

struct CallbackReadlineHelper<'a> {
    runtime: &'a Runtime,
    callback: Value,
    prompt: String,
    last_displayed: RefCell<Option<String>>,
}

impl Helper for CallbackReadlineHelper<'_> {}

impl Completer for CallbackReadlineHelper<'_> {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let _ = ctx;
        let prefix = line.get(..pos).unwrap_or(line).to_owned();
        let completions = self
            .runtime
            .call_value(
                self.callback.clone(),
                vec![Value::String(prefix)],
                Vec::new(),
            )
            .map(completion_values)
            .unwrap_or_default();
        self.maybe_display_completions(line, pos, &completions);
        Ok((0, completions))
    }
}

impl CallbackReadlineHelper<'_> {
    fn maybe_display_completions(&self, line: &str, pos: usize, completions: &[Pair]) {
        if completions.len() <= 1 {
            return;
        }
        let key = format!("{}\0{}\0{}", pos, line, completions.len());
        if self.last_displayed.borrow().as_deref() == Some(key.as_str()) {
            return;
        }
        *self.last_displayed.borrow_mut() = Some(key);

        let width = std::env::var("COLUMNS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(80);
        let max_len = completions
            .iter()
            .map(|candidate| candidate.display.chars().count())
            .max()
            .unwrap_or(0);
        let col_width = (max_len + 2).max(1);
        let columns = (width / col_width).max(1);
        let rows = completions.len().div_ceil(columns);

        let mut out = String::from("\r\n");
        for row in 0..rows {
            for column in 0..columns {
                let index = row + column * rows;
                let Some(candidate) = completions.get(index) else {
                    continue;
                };
                out.push_str(&candidate.display);
                if column + 1 < columns {
                    let padding = col_width.saturating_sub(candidate.display.chars().count());
                    out.push_str(&" ".repeat(padding));
                }
            }
            out.push_str("\r\n");
        }
        out.push_str(&self.prompt);
        out.push_str(line);

        let _ = io::stdout().write_all(out.as_bytes());
        let _ = io::stdout().flush();
    }
}

impl Hinter for CallbackReadlineHelper<'_> {
    type Hint = String;

    fn hint(&self, _line: &str, _pos: usize, _ctx: &Context<'_>) -> Option<String> {
        None
    }
}

impl Highlighter for CallbackReadlineHelper<'_> {}

impl Validator for CallbackReadlineHelper<'_> {
    fn validate(&self, _ctx: &mut ValidationContext<'_>) -> rustyline::Result<ValidationResult> {
        Ok(ValidationResult::Valid(None))
    }
}

fn read_line(
    runtime: &Runtime,
    prompt: &str,
    default: &str,
    completion_callback: Option<Value>,
) -> Result<String> {
    if io::stdin().is_terminal() {
        let config = Config::builder()
            .completion_type(CompletionType::List)
            .bell_style(BellStyle::None)
            .build();
        let mut editor = Editor::<CallbackReadlineHelper, DefaultHistory>::with_config(config)
            .map_err(|err| crate::error::ZuzuRustError::runtime(err.to_string()))?;
        if let Some(callback) = completion_callback {
            editor.set_helper(Some(CallbackReadlineHelper {
                runtime,
                callback,
                prompt: prompt.to_owned(),
                last_displayed: RefCell::new(None),
            }));
        }
        return match editor.readline(prompt) {
            Ok(line) if line.is_empty() => Ok(default.to_owned()),
            Ok(line) => Ok(line),
            Err(ReadlineError::Interrupted | ReadlineError::Eof) => Ok(default.to_owned()),
            Err(err) => Err(crate::error::ZuzuRustError::runtime(err.to_string())),
        };
    }

    runtime_free_read_line(prompt, default)
}

fn join_completion_path(dir: &str, name: &str) -> String {
    if dir.is_empty() {
        name.to_owned()
    } else if dir.ends_with('/') || dir.ends_with('\\') {
        format!("{dir}{name}")
    } else {
        format!("{dir}{MAIN_SEPARATOR}{name}")
    }
}

fn runtime_free_read_line(prompt: &str, default: &str) -> Result<String> {
    print!("{prompt}");
    let mut line = String::new();
    let bytes = io::stdin().read_line(&mut line)?;
    if bytes == 0 {
        return Ok(default.to_owned());
    }
    while line.ends_with('\n') || line.ends_with('\r') {
        line.pop();
    }
    if line.is_empty() {
        Ok(default.to_owned())
    } else {
        Ok(line)
    }
}

fn path_completions(text: &str, directory_only: bool) -> Value {
    let path = Path::new(text);
    let (dir_text, base_text) = if text.ends_with('/') || text.ends_with('\\') {
        (text, "")
    } else {
        (
            path.parent()
                .and_then(Path::to_str)
                .filter(|parent| !parent.is_empty())
                .unwrap_or("."),
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(text),
        )
    };
    let visible_dir = if dir_text == "." { "" } else { dir_text };
    let Ok(entries) = fs::read_dir(dir_text) else {
        return Value::Array(Vec::new());
    };
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.starts_with(base_text) {
            continue;
        }
        let is_dir = entry.file_type().map(|kind| kind.is_dir()).unwrap_or(false);
        if directory_only && !is_dir {
            continue;
        }
        let mut candidate = join_completion_path(visible_dir, &name);
        if is_dir {
            candidate.push(MAIN_SEPARATOR);
        }
        out.push(Value::String(candidate));
    }
    out.sort_by_key(|value| value.render());
    Value::Array(out)
}

pub(super) fn call(
    runtime: &Runtime,
    name: &str,
    args: &[Value],
    named_args: &[(String, Value)],
) -> Option<Result<Value>> {
    if !named_args.is_empty() {
        return Some(Err(crate::error::ZuzuRustError::runtime(
            "named call arguments are not implemented for native functions",
        )));
    }
    let value = match name {
        "ansi_esc" => require_arity(name, args, 0).map(|_| Value::String("\u{1b}".to_owned())),
        "supports_ansi" => require_arity(name, args, 0).map(|_| Value::Boolean(supports_ansi())),
        "colour_text" => {
            require_arity(name, args, 2).and_then(|_| colour_text(runtime, &args[0], &args[1]))
        }
        "write" => require_arity(name, args, 2).and_then(|_| {
            let text = match colour_text(runtime, &args[0], &args[1])? {
                Value::String(text) => text,
                value => runtime.render_value(&value)?,
            };
            runtime.emit_stdout(&text)?;
            Ok(Value::Null)
        }),
        "write_line" => require_arity(name, args, 2).and_then(|_| {
            let text = match colour_text(runtime, &args[0], &args[1])? {
                Value::String(text) => text,
                value => runtime.render_value(&value)?,
            };
            runtime.emit_stdout(&format!("{text}\n"))?;
            Ok(Value::Null)
        }),
        "readline" => require_arity(name, args, 3).and_then(|_| {
            let prompt = value_text(runtime, Some(&args[0]))?;
            let default = value_text(runtime, Some(&args[1]))?;
            let completion_callback = match &args[2] {
                Value::Null => None,
                Value::Function(_) | Value::NativeFunction(_) => Some(args[2].clone()),
                value => Some(value.clone()),
            };
            if let Some(value) = &completion_callback {
                if !matches!(value, Value::Function(_) | Value::NativeFunction(_)) {
                    return Err(crate::error::ZuzuRustError::runtime(
                        "readline completion must be Function or null",
                    ));
                }
            }
            if io::stdin().is_terminal() {
                return read_line(runtime, &prompt, &default, completion_callback)
                    .map(Value::String);
            }
            runtime.emit_stdout(&prompt)?;
            read_line(runtime, "", &default, completion_callback).map(Value::String)
        }),
        "readline_supports_completion" => {
            require_arity(name, args, 0).map(|_| Value::Boolean(io::stdin().is_terminal()))
        }
        "filename_completions" => require_arity(name, args, 1).and_then(|_| {
            if runtime.is_effectively_denied("fs") {
                return Ok(Value::Array(Vec::new()));
            }
            let text = value_text(runtime, Some(&args[0]))?;
            Ok(path_completions(&text, false))
        }),
        "directory_completions" => require_arity(name, args, 1).and_then(|_| {
            if runtime.is_effectively_denied("fs") {
                return Ok(Value::Array(Vec::new()));
            }
            let text = value_text(runtime, Some(&args[0]))?;
            Ok(path_completions(&text, true))
        }),
        _ => return None,
    };
    Some(value)
}
