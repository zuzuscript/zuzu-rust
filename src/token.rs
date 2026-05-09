use crate::span::Span;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenKind {
    Keyword(&'static str),
    Identifier(String),
    Number(String),
    String(String),
    Regex { pattern: String, flags: String },
    Template(Vec<TemplatePart>),
    Operator(String),
    Punct(char),
    Eof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    pub kind: TokenKind,
    pub span: Span,
}

impl Token {
    pub fn new(kind: TokenKind, span: Span) -> Self {
        Self { kind, span }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemplatePart {
    Text { line: usize, value: String },
    Expr { line: usize, source: String },
}
