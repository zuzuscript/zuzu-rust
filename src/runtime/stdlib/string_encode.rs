use std::collections::HashMap;

use super::super::Value;
use crate::error::{Result, ZuzuRustError};

// std/string/encode: character-encoding conversions between String and
// BinaryString. UTF-16 and UTF-32 encode to big-endian without a BOM (the
// deterministic canonical form shared by all runtimes); decode honours a
// leading BOM and otherwise assumes big-endian.

pub(super) fn exports() -> HashMap<String, Value> {
    let mut exports = HashMap::new();
    // The native-function namespace is shared across all stdlib modules
    // (dispatch is by bare name), and std/string/base64 already claims
    // "encode"/"decode" — so the internal names are prefixed.
    exports.insert(
        "encode".to_owned(),
        Value::native_function("string_encode__encode".to_owned()),
    );
    exports.insert(
        "decode".to_owned(),
        Value::native_function("string_encode__decode".to_owned()),
    );
    exports.insert(
        "ENCODING_UTF8".to_owned(),
        Value::String("UTF-8".to_owned()),
    );
    exports.insert(
        "ENCODING_UTF16".to_owned(),
        Value::String("UTF-16".to_owned()),
    );
    exports.insert(
        "ENCODING_UTF32".to_owned(),
        Value::String("UTF-32".to_owned()),
    );
    exports.insert(
        "ENCODING_LATIN".to_owned(),
        Value::String("ISO-8859-1".to_owned()),
    );
    exports
}

pub(super) fn call(name: &str, args: &[Value]) -> Option<Result<Value>> {
    let value = match name {
        "string_encode__encode" => encode(args),
        "string_encode__decode" => decode(args),
        _ => return None,
    };
    Some(value)
}

enum Codec {
    Utf8,
    Utf16,
    Utf32,
    Latin1,
}

fn codec_for(name: &str) -> Result<Codec> {
    match name.to_ascii_uppercase().as_str() {
        "UTF-8" | "UTF8" => Ok(Codec::Utf8),
        "UTF-16" | "UTF16" | "UTF-16BE" => Ok(Codec::Utf16),
        "UTF-32" | "UTF32" | "UTF-32BE" => Ok(Codec::Utf32),
        "ISO-8859-1" | "ISO8859-1" | "LATIN-1" | "LATIN1" | "LATIN" => Ok(Codec::Latin1),
        _ => Err(ZuzuRustError::thrown(format!(
            "Unsupported encoding: {}",
            name
        ))),
    }
}

fn encoding_arg(args: &[Value], index: usize, func: &str) -> Result<Codec> {
    match args.get(index) {
        None => codec_for("UTF-8"),
        Some(Value::String(name)) => codec_for(name),
        Some(other) => Err(ZuzuRustError::thrown(format!(
            "TypeException: {} encoding name must be String, got {}",
            func,
            other.type_name()
        ))),
    }
}

fn encode(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(ZuzuRustError::runtime(
            "encode expects a String and an encoding name",
        ));
    }
    let Value::String(text) = &args[0] else {
        return Err(ZuzuRustError::thrown(format!(
            "TypeException: encode expects String, got {}",
            args[0].type_name()
        )));
    };
    let codec = encoding_arg(args, 1, "encode")?;
    let bytes = match codec {
        Codec::Utf8 => text.as_bytes().to_vec(),
        Codec::Utf16 => {
            let mut out = Vec::with_capacity(text.len() * 2);
            for unit in text.encode_utf16() {
                out.extend_from_slice(&unit.to_be_bytes());
            }
            out
        }
        Codec::Utf32 => {
            let mut out = Vec::with_capacity(text.len() * 4);
            for ch in text.chars() {
                out.extend_from_slice(&(ch as u32).to_be_bytes());
            }
            out
        }
        Codec::Latin1 => {
            let mut out = Vec::with_capacity(text.len());
            for ch in text.chars() {
                let code = ch as u32;
                if code > 0xFF {
                    return Err(ZuzuRustError::thrown(format!(
                        "Character U+{:04X} cannot be encoded as ISO-8859-1",
                        code
                    )));
                }
                out.push(code as u8);
            }
            out
        }
    };
    Ok(Value::BinaryString(bytes))
}

fn decode(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(ZuzuRustError::runtime(
            "decode expects a BinaryString and an encoding name",
        ));
    }
    let Value::BinaryString(bytes) = &args[0] else {
        return Err(ZuzuRustError::thrown(format!(
            "TypeException: decode expects BinaryString, got {}",
            args[0].type_name()
        )));
    };
    let codec = encoding_arg(args, 1, "decode")?;
    let text = match codec {
        Codec::Utf8 => String::from_utf8(bytes.clone())
            .map_err(|_| ZuzuRustError::thrown("Invalid UTF-8 in BinaryString"))?,
        Codec::Utf16 => decode_utf16_bytes(bytes)?,
        Codec::Utf32 => decode_utf32_bytes(bytes)?,
        Codec::Latin1 => bytes.iter().map(|&byte| byte as char).collect(),
    };
    Ok(Value::String(text))
}

fn decode_utf16_bytes(bytes: &[u8]) -> Result<String> {
    let (data, big_endian) = match bytes {
        [0xFE, 0xFF, rest @ ..] => (rest, true),
        [0xFF, 0xFE, rest @ ..] => (rest, false),
        _ => (bytes, true),
    };
    if data.len() % 2 != 0 {
        return Err(ZuzuRustError::thrown(
            "UTF-16 input length must be a multiple of 2 bytes",
        ));
    }
    let units: Vec<u16> = data
        .chunks_exact(2)
        .map(|pair| {
            if big_endian {
                u16::from_be_bytes([pair[0], pair[1]])
            } else {
                u16::from_le_bytes([pair[0], pair[1]])
            }
        })
        .collect();
    String::from_utf16(&units).map_err(|_| ZuzuRustError::thrown("Invalid UTF-16 in BinaryString"))
}

fn decode_utf32_bytes(bytes: &[u8]) -> Result<String> {
    let (data, big_endian) = match bytes {
        [0x00, 0x00, 0xFE, 0xFF, rest @ ..] => (rest, true),
        [0xFF, 0xFE, 0x00, 0x00, rest @ ..] => (rest, false),
        _ => (bytes, true),
    };
    if data.len() % 4 != 0 {
        return Err(ZuzuRustError::thrown(
            "UTF-32 input length must be a multiple of 4 bytes",
        ));
    }
    let mut out = String::with_capacity(data.len() / 4);
    for quad in data.chunks_exact(4) {
        let code = if big_endian {
            u32::from_be_bytes([quad[0], quad[1], quad[2], quad[3]])
        } else {
            u32::from_le_bytes([quad[0], quad[1], quad[2], quad[3]])
        };
        let ch = char::from_u32(code)
            .ok_or_else(|| ZuzuRustError::thrown("Invalid UTF-32 in BinaryString"))?;
        out.push(ch);
    }
    Ok(out)
}
