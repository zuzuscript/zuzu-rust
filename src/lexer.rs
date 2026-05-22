use crate::error::{Result, ZuzuRustError};
use crate::span::Span;
use crate::token::{TemplatePart, Token, TokenKind};

const KEYWORDS: &[&str] = &[
    "let",
    "const",
    "function",
    "async",
    "await",
    "spawn",
    "class",
    "trait",
    "method",
    "static",
    "extends",
    "with",
    "but",
    "from",
    "import",
    "as",
    "try",
    "catch",
    "if",
    "else",
    "unless",
    "while",
    "for",
    "in",
    "switch",
    "case",
    "default",
    "return",
    "next",
    "continue",
    "last",
    "throw",
    "die",
    "do",
    "fn",
    "new",
    "super",
    "null",
    "true",
    "false",
    "and",
    "or",
    "xor",
    "nand",
    "not",
    "mod",
    "eq",
    "ne",
    "gt",
    "ge",
    "lt",
    "le",
    "cmp",
    "eqi",
    "nei",
    "gti",
    "gei",
    "lti",
    "lei",
    "cmpi",
    "say",
    "print",
    "warn",
    "assert",
    "debug",
    "instanceof",
    "does",
    "can",
    "union",
    "intersection",
    "subsetof",
    "supersetof",
    "equivalentof",
    "abs",
    "sqrt",
    "floor",
    "ceil",
    "round",
    "int",
    "uc",
    "lc",
    "length",
    "typeof",
];

const TWO_CHAR_OPERATORS: &[&str] = &[
    ":=", "+=", "-=", "*=", "/=", "_=", "~=", "**", "==", "!=", "<=", ">=", "++", "--", "->", "@?",
    "@@", "?:", "|>", "<|", "×=", "÷=", "≤", "≥", "≠", "≡", "≢", "≶", "≷", "⋀", "⋁", "⊻", "⊼", "∈",
    "∉", "⋃", "⋂", "∖", "¬", "√", "⊂", "⊃", ">>", "<<", "⌊", "⌋", "⌈", "⌉", "«", "»", "→",
];

const THREE_CHAR_OPERATORS: &[&str] = &["**=", "?:=", "<=>", ".(", "⊂⊃", "<<<", ">>>", "..."];

pub fn lex(source: &str) -> Result<Vec<Token>> {
    let chars: Vec<char> = source.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0usize;
    let mut line = 1usize;
    let mut column = 1usize;

    while i < chars.len() {
        let ch = chars[i];

        if i == 0 && ch == '#' && i + 1 < chars.len() && chars[i + 1] == '!' {
            i += 2;
            column += 2;
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
                column += 1;
            }
            continue;
        }

        if column == 1 && ch == '=' && starts_pod_command(&chars, i) {
            let (next_i, next_line, next_column) = skip_pod_block(&chars, i, line, column);
            i = next_i;
            line = next_line;
            column = next_column;
            continue;
        }

        if ch == ' ' || ch == '\t' || ch == '\r' {
            i += 1;
            column += 1;
            continue;
        }
        if ch == '\n' {
            i += 1;
            line += 1;
            column = 1;
            continue;
        }
        if ch == '/' && i + 1 < chars.len() && chars[i + 1] == '/' {
            i += 2;
            column += 2;
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
                column += 1;
            }
            continue;
        }
        if ch == '/' && i + 1 < chars.len() && chars[i + 1] == '*' {
            i += 2;
            column += 2;
            while i + 1 < chars.len() {
                if chars[i] == '*' && chars[i + 1] == '/' {
                    i += 2;
                    column += 2;
                    break;
                }
                if chars[i] == '\n' {
                    i += 1;
                    line += 1;
                    column = 1;
                } else {
                    i += 1;
                    column += 1;
                }
            }
            continue;
        }

        let start = i;
        let start_line = line;
        let start_column = column;

        if ch == '/' && can_start_regex(&tokens) {
            let (pattern, parts, flags, end, new_line, new_column) =
                lex_regex(&chars, i, line, column)?;
            tokens.push(Token::new(
                TokenKind::Regex {
                    pattern,
                    parts,
                    flags,
                },
                Span::new(start, end, start_line, start_column),
            ));
            i = end;
            line = new_line;
            column = new_column;
            continue;
        }

        if ch == '^' && i + 1 < chars.len() && chars[i + 1] == '^' {
            tokens.push(Token::new(
                TokenKind::Identifier("^^".to_owned()),
                Span::new(start, start + 2, start_line, start_column),
            ));
            i += 2;
            column += 2;
            continue;
        }

        if let Some(operator) = match_operator(&chars, i) {
            let width = operator.chars().count();
            let operator = if operator == "→" { "->" } else { operator };
            tokens.push(Token::new(
                TokenKind::Operator(operator.to_owned()),
                Span::new(start, start + width, start_line, start_column),
            ));
            i += width;
            column += width;
            continue;
        }

        if ch == '_' && i + 1 < chars.len() && is_identifier_continue(chars[i + 1]) {
            let (value, end, end_column) = lex_identifier(&chars, i, column);
            tokens.push(Token::new(
                TokenKind::Identifier(value),
                Span::new(start, end, start_line, start_column),
            ));
            i = end;
            column = end_column;
            continue;
        }

        match ch {
            '(' | ')' | '{' | '}' | '[' | ']' | ',' | ';' => {
                tokens.push(Token::new(
                    TokenKind::Punct(ch),
                    Span::new(start, start + 1, start_line, start_column),
                ));
                i += 1;
                column += 1;
            }
            '.' | ':' | '?' | '+' | '-' | '*' | '/' | '_' | '=' | '<' | '>' | '!' | '&' | '|'
            | '^' | '~' | '@' | '\\' | '×' | '÷' | '≤' | '≥' | '≠' | '≡' | '≢' | '≶' | '≷'
            | '⋀' | '⋁' | '⊻' | '⊼' | '∈' | '∉' | '⋃' | '⋂' | '∖' | '¬' | '√' | '⊂' | '⊃' | '⌊'
            | '⌋' | '⌈' | '⌉' | '«' | '»' | '▷' | '◁' => {
                tokens.push(Token::new(
                    TokenKind::Operator(ch.to_string()),
                    Span::new(start, start + 1, start_line, start_column),
                ));
                i += 1;
                column += 1;
            }
            '"' => {
                let (value, end, new_line, new_column) =
                    if i + 2 < chars.len() && chars[i + 1] == '"' && chars[i + 2] == '"' {
                        lex_triple_quoted_string(&chars, i, line, column)?
                    } else {
                        lex_string(&chars, i, line, column)?
                    };
                tokens.push(Token::new(
                    TokenKind::String(value),
                    Span::new(start, end, start_line, start_column),
                ));
                i = end;
                line = new_line;
                column = new_column;
            }
            '\'' => {
                let (value, end, new_line, new_column) =
                    if i + 2 < chars.len() && chars[i + 1] == '\'' && chars[i + 2] == '\'' {
                        lex_triple_single_quoted_binary(&chars, i, line, column)?
                    } else {
                        lex_single_quoted_binary(&chars, i, line, column)?
                    };
                tokens.push(Token::new(
                    TokenKind::BinaryString(value),
                    Span::new(start, end, start_line, start_column),
                ));
                i = end;
                line = new_line;
                column = new_column;
            }
            '`' => {
                let (value, end, new_line, new_column) = lex_template(&chars, i, line, column)?;
                tokens.push(Token::new(
                    TokenKind::Template(value),
                    Span::new(start, end, start_line, start_column),
                ));
                i = end;
                line = new_line;
                column = new_column;
            }
            '0'..='9' => {
                let (value, end, end_column) = lex_number(&chars, i, column);
                tokens.push(Token::new(
                    TokenKind::Number(value),
                    Span::new(start, end, start_line, start_column),
                ));
                i = end;
                column = end_column;
            }
            '⊤' => {
                tokens.push(Token::new(
                    TokenKind::Keyword("true"),
                    Span::new(start, start + 1, start_line, start_column),
                ));
                i += 1;
                column += 1;
            }
            '⊥' => {
                tokens.push(Token::new(
                    TokenKind::Keyword("false"),
                    Span::new(start, start + 1, start_line, start_column),
                ));
                i += 1;
                column += 1;
            }
            _ if is_identifier_start(ch) => {
                let (value, end, end_column) = lex_identifier(&chars, i, column);
                let kind = if KEYWORDS.contains(&value.as_str()) {
                    TokenKind::Keyword(Box::leak(value.into_boxed_str()))
                } else {
                    TokenKind::Identifier(value)
                };
                tokens.push(Token::new(
                    kind,
                    Span::new(start, end, start_line, start_column),
                ));
                i = end;
                column = end_column;
            }
            _ => {
                return Err(ZuzuRustError::lex(
                    format!("unexpected character '{}'", ch),
                    line,
                    column,
                ));
            }
        }
    }

    tokens.push(Token::new(
        TokenKind::Eof,
        Span::new(source.len(), source.len(), line, column),
    ));
    Ok(tokens)
}

fn match_operator(chars: &[char], index: usize) -> Option<&'static str> {
    for operator in THREE_CHAR_OPERATORS {
        if matches_text(chars, index, operator) {
            return Some(operator);
        }
    }
    for operator in TWO_CHAR_OPERATORS {
        if matches_text(chars, index, operator) {
            return Some(operator);
        }
    }
    None
}

fn matches_text(chars: &[char], index: usize, text: &str) -> bool {
    let len = text.chars().count();
    if index + len > chars.len() {
        return false;
    }
    chars[index..index + len].iter().copied().eq(text.chars())
}

fn hex_value(ch: char) -> Option<u32> {
    match ch {
        '0'..='9' => Some(ch as u32 - '0' as u32),
        'a'..='f' => Some(ch as u32 - 'a' as u32 + 10),
        'A'..='F' => Some(ch as u32 - 'A' as u32 + 10),
        _ => None,
    }
}

fn read_hex_escape(
    chars: &[char],
    index: usize,
    digits: usize,
    line: usize,
    column: usize,
    name: &str,
) -> Result<u32> {
    if index + digits > chars.len() {
        return Err(ZuzuRustError::lex(
            format!("invalid {name} escape"),
            line,
            column,
        ));
    }
    let mut value = 0u32;
    for offset in 0..digits {
        let Some(digit) = hex_value(chars[index + offset]) else {
            return Err(ZuzuRustError::lex(
                format!("invalid {name} escape"),
                line,
                column,
            ));
        };
        value = value * 16 + digit;
    }
    Ok(value)
}

fn decode_text_escape(
    chars: &[char],
    index: &mut usize,
    column: &mut usize,
    line: usize,
    allow_unicode: bool,
    literal_name: &str,
) -> Result<char> {
    if *index >= chars.len() {
        return Err(ZuzuRustError::lex(
            format!("unterminated {literal_name} escape"),
            line,
            *column,
        ));
    }
    let escape_line = line;
    let escape_column = *column;
    let ch = chars[*index];
    let value = match ch {
        'n' => {
            *index += 1;
            *column += 1;
            '\n'
        }
        'r' => {
            *index += 1;
            *column += 1;
            '\r'
        }
        't' => {
            *index += 1;
            *column += 1;
            '\t'
        }
        '\\' | '\'' | '"' | '`' | '/' | '$' => {
            *index += 1;
            *column += 1;
            ch
        }
        'x' => {
            let code = read_hex_escape(chars, *index + 1, 2, escape_line, escape_column, "\\x")?;
            *index += 3;
            *column += 3;
            char::from_u32(code).expect("\\xHH is always a valid Unicode scalar")
        }
        'u' if allow_unicode => {
            let code = read_hex_escape(chars, *index + 1, 4, escape_line, escape_column, "\\u")?;
            let Some(ch) = char::from_u32(code) else {
                return Err(ZuzuRustError::lex(
                    "invalid Unicode escape",
                    escape_line,
                    escape_column,
                ));
            };
            *index += 5;
            *column += 5;
            ch
        }
        'u' => {
            return Err(ZuzuRustError::lex(
                "Unicode escapes are not supported in binary strings",
                escape_line,
                escape_column,
            ))
        }
        _ => {
            return Err(ZuzuRustError::lex(
                format!("invalid {literal_name} escape"),
                escape_line,
                escape_column,
            ))
        }
    };
    Ok(value)
}

fn decode_binary_escape(
    chars: &[char],
    index: &mut usize,
    column: &mut usize,
    line: usize,
) -> Result<Vec<u8>> {
    if *index >= chars.len() {
        return Err(ZuzuRustError::lex(
            "unterminated binary string escape",
            line,
            *column,
        ));
    }
    let escape_line = line;
    let escape_column = *column;
    let ch = chars[*index];
    let byte = match ch {
        'n' => Some(b'\n'),
        'r' => Some(b'\r'),
        't' => Some(b'\t'),
        '\\' => Some(b'\\'),
        '\'' => Some(b'\''),
        '"' => Some(b'"'),
        '`' => Some(b'`'),
        '/' => Some(b'/'),
        '$' => Some(b'$'),
        'x' => {
            let value = read_hex_escape(chars, *index + 1, 2, escape_line, escape_column, "\\x")?;
            *index += 3;
            *column += 3;
            return Ok(vec![value as u8]);
        }
        'u' => {
            return Err(ZuzuRustError::lex(
                "Unicode escapes are not supported in binary strings",
                escape_line,
                escape_column,
            ));
        }
        _ => None,
    };
    let Some(byte) = byte else {
        return Err(ZuzuRustError::lex(
            "invalid binary string escape",
            escape_line,
            escape_column,
        ));
    };
    *index += 1;
    *column += 1;
    Ok(vec![byte])
}

fn lex_string(
    chars: &[char],
    start: usize,
    line: usize,
    column: usize,
) -> Result<(String, usize, usize, usize)> {
    let mut value = String::new();
    let mut i = start + 1;
    let current_line = line;
    let mut current_column = column + 1;

    while i < chars.len() {
        let ch = chars[i];
        match ch {
            '"' => {
                return Ok((value, i + 1, current_line, current_column + 1));
            }
            '\\' => {
                i += 1;
                current_column += 1;
                let escaped = decode_text_escape(
                    chars,
                    &mut i,
                    &mut current_column,
                    current_line,
                    true,
                    "string",
                )?;
                value.push(escaped);
            }
            '\n' => {
                return Err(ZuzuRustError::lex(
                    "unterminated string literal",
                    current_line,
                    current_column,
                ));
            }
            _ => {
                value.push(ch);
                i += 1;
                current_column += 1;
            }
        }
    }

    Err(ZuzuRustError::lex(
        "unterminated string literal",
        line,
        column,
    ))
}

fn lex_single_quoted_binary(
    chars: &[char],
    start: usize,
    line: usize,
    column: usize,
) -> Result<(Vec<u8>, usize, usize, usize)> {
    let mut value = Vec::new();
    let mut i = start + 1;
    let current_line = line;
    let mut current_column = column + 1;

    while i < chars.len() {
        let ch = chars[i];
        match ch {
            '\'' => return Ok((value, i + 1, current_line, current_column + 1)),
            '\\' => {
                i += 1;
                current_column += 1;
                value.extend(decode_binary_escape(
                    chars,
                    &mut i,
                    &mut current_column,
                    current_line,
                )?);
            }
            '\n' => {
                return Err(ZuzuRustError::lex(
                    "unterminated binary string literal",
                    current_line,
                    current_column,
                ));
            }
            _ => {
                let mut buf = [0u8; 4];
                value.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                i += 1;
                current_column += 1;
            }
        }
    }

    Err(ZuzuRustError::lex(
        "unterminated binary string literal",
        line,
        column,
    ))
}

fn lex_triple_single_quoted_binary(
    chars: &[char],
    start: usize,
    line: usize,
    column: usize,
) -> Result<(Vec<u8>, usize, usize, usize)> {
    let mut value = Vec::new();
    let mut i = start + 3;
    let mut current_line = line;
    let mut current_column = column + 3;

    while i < chars.len() {
        if i + 2 < chars.len() && chars[i] == '\'' && chars[i + 1] == '\'' && chars[i + 2] == '\'' {
            return Ok((value, i + 3, current_line, current_column + 3));
        }
        let ch = chars[i];
        let mut buf = [0u8; 4];
        value.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
        i += 1;
        if ch == '\n' {
            current_line += 1;
            current_column = 1;
        } else {
            current_column += 1;
        }
    }

    Err(ZuzuRustError::lex(
        "unterminated triple-quoted binary string literal",
        line,
        column,
    ))
}

fn lex_triple_quoted_string(
    chars: &[char],
    start: usize,
    line: usize,
    column: usize,
) -> Result<(String, usize, usize, usize)> {
    let mut value = String::new();
    let mut i = start + 3;
    let mut current_line = line;
    let mut current_column = column + 3;

    while i < chars.len() {
        if i + 2 < chars.len() && chars[i] == '"' && chars[i + 1] == '"' && chars[i + 2] == '"' {
            return Ok((value, i + 3, current_line, current_column + 3));
        }
        let ch = chars[i];
        value.push(ch);
        i += 1;
        if ch == '\n' {
            current_line += 1;
            current_column = 1;
        } else {
            current_column += 1;
        }
    }

    Err(ZuzuRustError::lex(
        "unterminated triple-quoted string literal",
        line,
        column,
    ))
}

fn lex_template(
    chars: &[char],
    start: usize,
    line: usize,
    column: usize,
) -> Result<(Vec<TemplatePart>, usize, usize, usize)> {
    if start + 2 < chars.len() && chars[start + 1] == '`' && chars[start + 2] == '`' {
        return lex_triple_backtick_template(chars, start, line, column);
    }

    let mut parts = Vec::new();
    let mut text_part = String::new();
    let mut text_line = line;
    let mut i = start + 1;
    let mut current_line = line;
    let mut current_column = column + 1;

    while i < chars.len() {
        let ch = chars[i];
        if ch == '`' {
            if !text_part.is_empty() {
                parts.push(TemplatePart::Text {
                    line: text_line,
                    value: text_part,
                });
            }
            return Ok((parts, i + 1, current_line, current_column + 1));
        }
        if ch == '\\' {
            i += 1;
            current_column += 1;
            let escaped = decode_text_escape(
                chars,
                &mut i,
                &mut current_column,
                current_line,
                true,
                "template literal",
            )?;
            text_part.push(escaped);
            continue;
        }
        if ch == '$' && i + 1 < chars.len() && chars[i + 1] == '{' {
            if !text_part.is_empty() {
                parts.push(TemplatePart::Text {
                    line: text_line,
                    value: std::mem::take(&mut text_part),
                });
            }
            let expr_line = current_line;
            let (expr, end, new_line, new_column) =
                lex_template_interpolation(chars, i + 2, current_line, current_column + 2)?;
            parts.push(TemplatePart::Expr {
                line: expr_line,
                source: expr,
            });
            i = end;
            current_line = new_line;
            current_column = new_column;
            text_line = current_line;
            continue;
        }
        if ch == '\n' {
            return Err(ZuzuRustError::lex(
                "unterminated template literal",
                current_line,
                current_column,
            ));
        }
        text_part.push(ch);
        i += 1;
        current_column += 1;
    }

    Err(ZuzuRustError::lex(
        "unterminated template literal",
        line,
        column,
    ))
}

fn lex_triple_backtick_template(
    chars: &[char],
    start: usize,
    line: usize,
    column: usize,
) -> Result<(Vec<TemplatePart>, usize, usize, usize)> {
    let mut parts = Vec::new();
    let mut text_part = String::new();
    let mut text_line = line;
    let mut i = start + 3;
    let mut current_line = line;
    let mut current_column = column + 3;

    while i < chars.len() {
        if i + 2 < chars.len() && chars[i] == '`' && chars[i + 1] == '`' && chars[i + 2] == '`' {
            if !text_part.is_empty() {
                parts.push(TemplatePart::Text {
                    line: text_line,
                    value: text_part,
                });
            }
            return Ok((parts, i + 3, current_line, current_column + 3));
        }
        if chars[i] == '$' && i + 1 < chars.len() && chars[i + 1] == '{' {
            if !text_part.is_empty() {
                parts.push(TemplatePart::Text {
                    line: text_line,
                    value: std::mem::take(&mut text_part),
                });
            }
            let expr_line = current_line;
            let (expr, end, new_line, new_column) =
                lex_template_interpolation(chars, i + 2, current_line, current_column + 2)?;
            parts.push(TemplatePart::Expr {
                line: expr_line,
                source: expr,
            });
            i = end;
            current_line = new_line;
            current_column = new_column;
            text_line = current_line;
            continue;
        }
        let ch = chars[i];
        if text_part.is_empty() {
            text_line = current_line;
        }
        text_part.push(ch);
        i += 1;
        if ch == '\n' {
            current_line += 1;
            current_column = 1;
        } else {
            current_column += 1;
        }
    }

    Err(ZuzuRustError::lex(
        "unterminated triple-backtick template literal",
        line,
        column,
    ))
}

fn lex_template_interpolation(
    chars: &[char],
    start: usize,
    line: usize,
    column: usize,
) -> Result<(String, usize, usize, usize)> {
    let mut i = start;
    let mut current_line = line;
    let mut current_column = column;
    let mut depth = 1usize;
    let expr_start = start;

    while i < chars.len() {
        let ch = chars[i];
        if ch == '"' || ch == '\'' {
            let (end, new_line, new_column) =
                skip_quoted(chars, i, current_line, current_column, ch)?;
            i = end;
            current_line = new_line;
            current_column = new_column;
            continue;
        }
        if ch == '`' {
            let (_, end, new_line, new_column) =
                lex_template(chars, i, current_line, current_column)?;
            i = end;
            current_line = new_line;
            current_column = new_column;
            continue;
        }
        if ch == '/' && i + 1 < chars.len() && chars[i + 1] == '/' {
            i += 2;
            current_column += 2;
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
                current_column += 1;
            }
            continue;
        }
        if ch == '/' && i + 1 < chars.len() && chars[i + 1] == '*' {
            i += 2;
            current_column += 2;
            while i + 1 < chars.len() {
                if chars[i] == '*' && chars[i + 1] == '/' {
                    i += 2;
                    current_column += 2;
                    break;
                }
                if chars[i] == '\n' {
                    i += 1;
                    current_line += 1;
                    current_column = 1;
                } else {
                    i += 1;
                    current_column += 1;
                }
            }
            continue;
        }
        if ch == '{' {
            depth += 1;
            i += 1;
            current_column += 1;
            continue;
        }
        if ch == '}' {
            depth -= 1;
            if depth == 0 {
                let expr: String = chars[expr_start..i].iter().collect();
                return Ok((expr, i + 1, current_line, current_column + 1));
            }
            i += 1;
            current_column += 1;
            continue;
        }
        if ch == '\n' {
            i += 1;
            current_line += 1;
            current_column = 1;
            continue;
        }
        i += 1;
        current_column += 1;
    }

    Err(ZuzuRustError::lex(
        "unterminated template interpolation",
        line,
        column,
    ))
}

fn skip_quoted(
    chars: &[char],
    start: usize,
    line: usize,
    column: usize,
    quote: char,
) -> Result<(usize, usize, usize)> {
    let mut i = start + 1;
    let mut current_line = line;
    let mut current_column = column + 1;

    while i < chars.len() {
        let ch = chars[i];
        if ch == '\\' {
            i += 1;
            current_column += 1;
            if i < chars.len() {
                if chars[i] == '\n' {
                    current_line += 1;
                    current_column = 1;
                } else {
                    current_column += 1;
                }
                i += 1;
            }
            continue;
        }
        if ch == quote {
            return Ok((i + 1, current_line, current_column + 1));
        }
        if ch == '\n' {
            current_line += 1;
            current_column = 1;
            i += 1;
            continue;
        }
        i += 1;
        current_column += 1;
    }

    Err(ZuzuRustError::lex(
        "unterminated quoted section in template literal",
        line,
        column,
    ))
}

fn lex_number(chars: &[char], start: usize, column: usize) -> (String, usize, usize) {
    let mut end = start + 1;
    let mut end_column = column + 1;
    let mut seen_dot = false;
    while end < chars.len() {
        if chars[end].is_ascii_digit() {
            end += 1;
            end_column += 1;
            continue;
        }
        if !seen_dot
            && chars[end] == '.'
            && end + 1 < chars.len()
            && chars[end + 1].is_ascii_digit()
        {
            seen_dot = true;
            end += 1;
            end_column += 1;
            continue;
        }
        break;
    }
    (chars[start..end].iter().collect(), end, end_column)
}

fn can_start_regex(tokens: &[Token]) -> bool {
    let Some(token) = tokens.last() else {
        return true;
    };
    match &token.kind {
        TokenKind::Keyword(_) | TokenKind::Operator(_) | TokenKind::Regex { .. } => true,
        TokenKind::Punct(ch) => matches!(ch, '(' | '[' | '{' | ',' | ';'),
        TokenKind::Identifier(_)
        | TokenKind::Number(_)
        | TokenKind::String(_)
        | TokenKind::BinaryString(_)
        | TokenKind::Template(_)
        | TokenKind::Eof => false,
    }
}

fn lex_regex(
    chars: &[char],
    start: usize,
    line: usize,
    column: usize,
) -> Result<(String, Vec<TemplatePart>, String, usize, usize, usize)> {
    let mut value = String::new();
    let mut parts = Vec::new();
    let mut text_part = String::new();
    let mut text_line = line;
    let mut i = start + 1;
    let mut current_line = line;
    let mut current_column = column + 1;
    let mut in_class = false;
    let mut saw_interpolation = false;

    while i < chars.len() {
        let ch = chars[i];
        if ch == '\\' {
            value.push(ch);
            text_part.push(ch);
            i += 1;
            current_column += 1;
            if i >= chars.len() {
                break;
            }
            let escaped = chars[i];
            value.push(escaped);
            text_part.push(escaped);
            if escaped == '\n' {
                current_line += 1;
                current_column = 1;
            } else {
                current_column += 1;
            }
            i += 1;
            continue;
        }
        if ch == '$' && i + 1 < chars.len() && chars[i + 1] == '{' {
            if !text_part.is_empty() {
                parts.push(TemplatePart::Text {
                    line: text_line,
                    value: std::mem::take(&mut text_part),
                });
            }
            let expr_line = current_line;
            let (expr, end, new_line, new_column) =
                lex_template_interpolation(chars, i + 2, current_line, current_column + 2)?;
            parts.push(TemplatePart::Expr {
                line: expr_line,
                source: expr,
            });
            saw_interpolation = true;
            value.push('$');
            value.push('{');
            let expr_source: String = chars[i + 2..end - 1].iter().collect();
            value.push_str(&expr_source);
            value.push('}');
            i = end;
            current_line = new_line;
            current_column = new_column;
            text_line = current_line;
            continue;
        }
        if ch == '[' {
            in_class = true;
        } else if ch == ']' {
            in_class = false;
        } else if ch == '/' && !in_class {
            i += 1;
            current_column += 1;
            let flags_start = i;
            while i < chars.len() && chars[i].is_ascii_alphabetic() {
                i += 1;
                current_column += 1;
            }
            let flags: String = chars[flags_start..i].iter().collect();
            if saw_interpolation && !text_part.is_empty() {
                parts.push(TemplatePart::Text {
                    line: text_line,
                    value: text_part,
                });
            }
            return Ok((value, parts, flags, i, current_line, current_column));
        }
        value.push(ch);
        if text_part.is_empty() {
            text_line = current_line;
        }
        text_part.push(ch);
        if ch == '\n' {
            current_line += 1;
            current_column = 1;
        } else {
            current_column += 1;
        }
        i += 1;
    }

    Err(ZuzuRustError::lex(
        "unterminated regex literal",
        line,
        column,
    ))
}

fn lex_identifier(chars: &[char], start: usize, column: usize) -> (String, usize, usize) {
    let mut end = start + 1;
    let mut end_column = column + 1;
    while end < chars.len() && is_identifier_continue(chars[end]) {
        end += 1;
        end_column += 1;
    }
    (chars[start..end].iter().collect(), end, end_column)
}

fn starts_pod_command(chars: &[char], index: usize) -> bool {
    if index + 1 >= chars.len() {
        return false;
    }
    chars[index + 1].is_ascii_alphabetic()
}

fn skip_pod_block(
    chars: &[char],
    start: usize,
    line: usize,
    column: usize,
) -> (usize, usize, usize) {
    let mut i = start;
    let mut current_line = line;
    let mut current_column = column;

    loop {
        while i < chars.len() && chars[i] != '\n' {
            i += 1;
            current_column += 1;
        }

        let line_text: String = chars[start_of_line(chars, i)..i].iter().collect();
        if line_text.starts_with("=cut") {
            if i < chars.len() && chars[i] == '\n' {
                i += 1;
                current_line += 1;
                current_column = 1;
            }
            break;
        }

        if i >= chars.len() {
            break;
        }

        i += 1;
        current_line += 1;
        current_column = 1;

        if i >= chars.len() {
            break;
        }

        if chars[i] != '=' || !starts_pod_command(chars, i) {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
                current_column += 1;
            }
            if i < chars.len() && chars[i] == '\n' {
                i += 1;
                current_line += 1;
                current_column = 1;
            }
        }
    }

    (i, current_line, current_column)
}

fn start_of_line(chars: &[char], index: usize) -> usize {
    let mut pos = index;
    while pos > 0 && chars[pos - 1] != '\n' {
        pos -= 1;
    }
    pos
}

fn is_identifier_start(ch: char) -> bool {
    ch == '_' || ch.is_alphabetic()
}

fn is_identifier_continue(ch: char) -> bool {
    ch == '_' || ch.is_alphanumeric()
}
