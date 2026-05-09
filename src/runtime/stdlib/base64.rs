use std::collections::HashMap;

use super::super::Value;
use crate::error::{Result, ZuzuRustError};

const STANDARD_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const URLSAFE_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

pub(super) fn exports() -> HashMap<String, Value> {
    let mut exports = HashMap::new();
    for func in ["encode", "decode", "encode_urlsafe", "decode_urlsafe"] {
        exports.insert(func.to_owned(), Value::native_function(func.to_owned()));
    }
    exports
}

pub(super) fn call(name: &str, args: &[Value]) -> Option<Result<Value>> {
    let value = match name {
        "encode" => encode(args, STANDARD_ALPHABET, true),
        "decode" => decode(args, false),
        "encode_urlsafe" => encode(args, URLSAFE_ALPHABET, false),
        "decode_urlsafe" => decode(args, true),
        _ => return None,
    };
    Some(value)
}

fn encode(args: &[Value], alphabet: &[u8; 64], pad: bool) -> Result<Value> {
    if args.len() != 1 {
        return Err(ZuzuRustError::runtime(
            "base64 encode expects exactly one argument",
        ));
    }
    let Value::BinaryString(bytes) = &args[0] else {
        return Err(ZuzuRustError::thrown(format!(
            "TypeException: encode expects BinaryString, got {}",
            args[0].type_name()
        )));
    };
    let mut out = String::new();
    let mut index = 0usize;
    while index < bytes.len() {
        let b0 = bytes[index];
        let b1 = bytes.get(index + 1).copied().unwrap_or(0);
        let b2 = bytes.get(index + 2).copied().unwrap_or(0);
        let triple = ((b0 as u32) << 16) | ((b1 as u32) << 8) | (b2 as u32);
        out.push(alphabet[((triple >> 18) & 0x3f) as usize] as char);
        out.push(alphabet[((triple >> 12) & 0x3f) as usize] as char);
        if index + 1 < bytes.len() {
            out.push(alphabet[((triple >> 6) & 0x3f) as usize] as char);
        } else if pad {
            out.push('=');
        }
        if index + 2 < bytes.len() {
            out.push(alphabet[(triple & 0x3f) as usize] as char);
        } else if pad {
            out.push('=');
        }
        index += 3;
    }
    Ok(Value::String(out))
}

fn decode(args: &[Value], urlsafe: bool) -> Result<Value> {
    if args.len() != 1 {
        return Err(ZuzuRustError::runtime(
            "base64 decode expects exactly one argument",
        ));
    }
    let Value::String(text) = &args[0] else {
        return Err(ZuzuRustError::thrown(format!(
            "TypeException: decode expects String, got {}",
            args[0].type_name()
        )));
    };
    let mut normalized = text.clone();
    if urlsafe {
        normalized = normalized.replace('-', "+").replace('_', "/");
        while normalized.len() % 4 != 0 {
            normalized.push('=');
        }
    }
    let cleaned: Vec<u8> = normalized
        .bytes()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect();
    if cleaned.len() % 4 != 0 {
        return Err(ZuzuRustError::thrown("invalid base64 length"));
    }
    let mut out = Vec::new();
    let mut index = 0usize;
    while index < cleaned.len() {
        let c0 = decode_char(cleaned[index])?;
        let c1 = decode_char(cleaned[index + 1])?;
        let c2 = decode_char(cleaned[index + 2])?;
        let c3 = decode_char(cleaned[index + 3])?;
        let triple = ((c0 as u32) << 18) | ((c1 as u32) << 12) | ((c2 as u32) << 6) | (c3 as u32);
        out.push(((triple >> 16) & 0xff) as u8);
        if cleaned[index + 2] != b'=' {
            out.push(((triple >> 8) & 0xff) as u8);
        }
        if cleaned[index + 3] != b'=' {
            out.push((triple & 0xff) as u8);
        }
        index += 4;
    }
    Ok(Value::BinaryString(out))
}

fn decode_char(byte: u8) -> Result<u8> {
    match byte {
        b'A'..=b'Z' => Ok(byte - b'A'),
        b'a'..=b'z' => Ok(byte - b'a' + 26),
        b'0'..=b'9' => Ok(byte - b'0' + 52),
        b'+' => Ok(62),
        b'/' => Ok(63),
        b'=' => Ok(0),
        _ => Err(ZuzuRustError::thrown("invalid base64 character")),
    }
}
