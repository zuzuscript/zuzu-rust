use crate::span::Span;
use std::error::Error;
use std::fmt;

pub type Result<T> = std::result::Result<T, ZuzuRustError>;

#[derive(Debug)]
pub enum ZuzuRustError {
    Lex {
        message: String,
        source_file: Option<String>,
        line: usize,
        column: usize,
    },
    Parse {
        message: String,
        source_file: Option<String>,
        line: usize,
        column: usize,
    },
    IncompleteParse {
        message: String,
        source_file: Option<String>,
        line: usize,
        column: usize,
    },
    Semantic {
        message: String,
        source_file: Option<String>,
        line: usize,
        column: usize,
    },
    Thrown {
        value: String,
        token: Option<String>,
    },
    Runtime {
        message: String,
    },
    Cli {
        message: String,
    },
    Io(std::io::Error),
}

impl ZuzuRustError {
    pub fn parse(message: impl Into<String>, span: Span) -> Self {
        Self::Parse {
            message: message.into(),
            source_file: None,
            line: span.line,
            column: span.column,
        }
    }

    pub fn incomplete_parse(message: impl Into<String>, span: Span) -> Self {
        Self::IncompleteParse {
            message: message.into(),
            source_file: None,
            line: span.line,
            column: span.column,
        }
    }

    pub fn lex(message: impl Into<String>, line: usize, column: usize) -> Self {
        Self::Lex {
            message: message.into(),
            source_file: None,
            line,
            column,
        }
    }

    pub fn semantic(message: impl Into<String>, line: usize) -> Self {
        Self::Semantic {
            message: message.into(),
            source_file: None,
            line,
            column: 1,
        }
    }

    pub fn cli(message: impl Into<String>) -> Self {
        Self::Cli {
            message: message.into(),
        }
    }

    pub fn runtime(message: impl Into<String>) -> Self {
        Self::Runtime {
            message: message.into(),
        }
    }

    pub fn thrown(value: impl Into<String>) -> Self {
        Self::Thrown {
            value: value.into(),
            token: None,
        }
    }

    pub fn thrown_with_token(value: impl Into<String>, token: impl Into<String>) -> Self {
        Self::Thrown {
            value: value.into(),
            token: Some(token.into()),
        }
    }

    pub fn with_source_file(mut self, source_file: Option<&str>) -> Self {
        let Some(source_file) = source_file else {
            return self;
        };
        let source_file = source_file.to_owned();
        match &mut self {
            ZuzuRustError::Lex {
                source_file: current,
                ..
            }
            | ZuzuRustError::Parse {
                source_file: current,
                ..
            }
            | ZuzuRustError::IncompleteParse {
                source_file: current,
                ..
            }
            | ZuzuRustError::Semantic {
                source_file: current,
                ..
            } => {
                current.get_or_insert(source_file);
            }
            _ => {}
        }
        self
    }

    pub fn is_iterator_exhausted(&self) -> bool {
        matches!(self, ZuzuRustError::Runtime { message } if message == "iterator exhausted")
    }

    pub fn thrown_value(&self) -> Option<&str> {
        match self {
            ZuzuRustError::Thrown { value, .. } => Some(value),
            _ => None,
        }
    }
}

impl fmt::Display for ZuzuRustError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ZuzuRustError::Lex {
                message,
                source_file,
                line,
                column,
            } => write_diagnostic(
                f,
                "lex error",
                source_file.as_deref(),
                *line,
                *column,
                message,
            ),
            ZuzuRustError::Parse {
                message,
                source_file,
                line,
                column,
            } => write_diagnostic(
                f,
                "parse error",
                source_file.as_deref(),
                *line,
                *column,
                message,
            ),
            ZuzuRustError::IncompleteParse {
                message,
                source_file,
                line,
                column,
            } => write_diagnostic(
                f,
                "incomplete parse error",
                source_file.as_deref(),
                *line,
                *column,
                message,
            ),
            ZuzuRustError::Semantic {
                message,
                source_file,
                line,
                column,
            } => write_diagnostic(
                f,
                "semantic error",
                source_file.as_deref(),
                *line,
                *column,
                message,
            ),
            ZuzuRustError::Thrown { value, .. } => {
                write!(f, "uncaught exception: {value}")
            }
            ZuzuRustError::Runtime { message } => write!(f, "runtime error: {message}"),
            ZuzuRustError::Cli { message } => write!(f, "{message}"),
            ZuzuRustError::Io(err) => write!(f, "io error: {err}"),
        }
    }
}

fn write_diagnostic(
    f: &mut fmt::Formatter<'_>,
    kind: &str,
    source_file: Option<&str>,
    line: usize,
    column: usize,
    message: &str,
) -> fmt::Result {
    match source_file {
        Some(source_file) => write!(f, "{kind} at {source_file}:{line}:{column}: {message}"),
        None => write!(f, "{kind} at {line}:{column}: {message}"),
    }
}

impl Error for ZuzuRustError {}

impl From<std::io::Error> for ZuzuRustError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}
