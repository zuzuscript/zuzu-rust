#![allow(dead_code)]

use std::collections::BTreeMap;

use serde_cbor::Value as SerdeCborValue;

use crate::error::{Result, ZuzuRustError};

const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

#[derive(Clone, Debug, PartialEq)]
pub(super) enum CborValue {
    Null,
    Bool(bool),
    Integer(i128),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
    Array(Vec<CborValue>),
    Map(Vec<(CborValue, CborValue)>),
    Tag { tag: u64, value: Box<CborValue> },
}

pub(super) fn encode_one(item: &CborValue) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    encode_item(item, &mut bytes)?;
    validate_profile(&bytes)?;
    Ok(bytes)
}

pub(super) fn decode_one(bytes: &[u8]) -> Result<CborValue> {
    validate_profile(bytes)?;
    let value =
        serde_cbor::from_slice(bytes).map_err(|err| cbor_error(format!("decode failed: {err}")))?;
    from_serde_cbor(value)
}

pub(super) fn validate_profile(bytes: &[u8]) -> Result<()> {
    let offset = scan_item(bytes, 0, true)?;
    if offset != bytes.len() {
        return Err(cbor_error("trailing bytes after item"));
    }
    Ok(())
}

fn scan_item(bytes: &[u8], offset: usize, top_level: bool) -> Result<usize> {
    require_available(bytes, offset, 1, "initial byte")?;
    let initial = bytes[offset];
    let major = initial / 32;
    let ai = initial % 32;
    let mut offset = offset + 1;
    if ai == 31 {
        return Err(cbor_error("indefinite-length item"));
    }
    if (28..=30).contains(&ai) {
        return Err(cbor_error("reserved additional information"));
    }
    if major == 7 {
        return scan_simple_or_float(bytes, offset, ai);
    }

    let (value, next) = read_argument(bytes, offset, ai)?;
    offset = next;
    match major {
        0 => {
            assert_unsigned_number_range(value)?;
            Ok(offset)
        }
        1 => {
            assert_negative_number_range(value)?;
            Ok(offset)
        }
        2 | 3 => {
            require_available(bytes, offset, value as usize, "string payload")?;
            Ok(offset + value as usize)
        }
        4 => {
            let mut offset = offset;
            for _ in 0..value {
                offset = scan_item(bytes, offset, false)?;
            }
            Ok(offset)
        }
        5 => {
            let mut offset = offset;
            for _ in 0..value {
                offset = scan_item(bytes, offset, false)?;
                offset = scan_item(bytes, offset, false)?;
            }
            Ok(offset)
        }
        6 => {
            if !top_level || value != 55_799 {
                return Err(cbor_error(format!("unsupported tag {value}")));
            }
            scan_item(bytes, offset, false)
        }
        _ => Err(cbor_error("unsupported major type")),
    }
}

fn scan_simple_or_float(bytes: &[u8], offset: usize, ai: u8) -> Result<usize> {
    match ai {
        20..=22 => Ok(offset),
        0..=24 => Err(cbor_error("unsupported simple value")),
        25 => Err(cbor_error("half-precision float is invalid")),
        26 => Err(cbor_error("single-precision float is invalid")),
        27 => {
            require_available(bytes, offset, 8, "binary64 float")?;
            let raw = u64::from_be_bytes(
                bytes[offset..offset + 8]
                    .try_into()
                    .expect("slice length checked above"),
            );
            let value = f64::from_bits(raw);
            if value.is_nan() {
                return Err(cbor_error("NaN is invalid"));
            }
            if !value.is_finite() {
                return Err(cbor_error("infinite float is invalid"));
            }
            Ok(offset + 8)
        }
        _ => Err(cbor_error("unsupported simple value")),
    }
}

fn encode_item(item: &CborValue, out: &mut Vec<u8>) -> Result<()> {
    match item {
        CborValue::Null => out.push(0xf6),
        CborValue::Bool(false) => out.push(0xf4),
        CborValue::Bool(true) => out.push(0xf5),
        CborValue::Integer(value) => {
            if *value >= 0 {
                encode_unsigned(0, *value as u128, out)?;
            } else {
                let magnitude = (-1i128)
                    .checked_sub(*value)
                    .ok_or_else(|| cbor_error("negative integer is outside supported range"))?;
                encode_unsigned(1, magnitude as u128, out)?;
            }
        }
        CborValue::Float(value) => {
            if value.is_nan() {
                return Err(cbor_error("NaN is invalid"));
            }
            if !value.is_finite() {
                return Err(cbor_error("infinite float is invalid"));
            }
            out.push(0xfb);
            out.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        CborValue::Text(value) => {
            encode_unsigned(3, value.len() as u128, out)?;
            out.extend_from_slice(value.as_bytes());
        }
        CborValue::Bytes(value) => {
            encode_unsigned(2, value.len() as u128, out)?;
            out.extend_from_slice(value);
        }
        CborValue::Array(values) => {
            encode_unsigned(4, values.len() as u128, out)?;
            for value in values {
                encode_item(value, out)?;
            }
        }
        CborValue::Map(entries) => {
            encode_unsigned(5, entries.len() as u128, out)?;
            for (key, value) in entries {
                encode_item(key, out)?;
                encode_item(value, out)?;
            }
        }
        CborValue::Tag { tag, value } => {
            encode_unsigned(6, *tag as u128, out)?;
            encode_item(value, out)?;
        }
    }
    Ok(())
}

fn encode_unsigned(major: u8, value: u128, out: &mut Vec<u8>) -> Result<()> {
    let prefix = major << 5;
    if value <= 23 {
        out.push(prefix | value as u8);
    } else if value <= u8::MAX as u128 {
        out.push(prefix | 24);
        out.push(value as u8);
    } else if value <= u16::MAX as u128 {
        out.push(prefix | 25);
        out.extend_from_slice(&(value as u16).to_be_bytes());
    } else if value <= u32::MAX as u128 {
        out.push(prefix | 26);
        out.extend_from_slice(&(value as u32).to_be_bytes());
    } else if value <= u64::MAX as u128 {
        out.push(prefix | 27);
        out.extend_from_slice(&(value as u64).to_be_bytes());
    } else {
        return Err(cbor_error("integer is outside supported range"));
    }
    Ok(())
}

fn read_argument(bytes: &[u8], offset: usize, ai: u8) -> Result<(u64, usize)> {
    match ai {
        0..=23 => Ok((ai as u64, offset)),
        24 => {
            require_available(bytes, offset, 1, "uint8 argument")?;
            let value = bytes[offset] as u64;
            assert_shortest_argument(value, 24)?;
            Ok((value, offset + 1))
        }
        25 => {
            require_available(bytes, offset, 2, "uint16 argument")?;
            let value = u16::from_be_bytes(
                bytes[offset..offset + 2]
                    .try_into()
                    .expect("slice length checked above"),
            ) as u64;
            assert_shortest_argument(value, 256)?;
            Ok((value, offset + 2))
        }
        26 => {
            require_available(bytes, offset, 4, "uint32 argument")?;
            let value = u32::from_be_bytes(
                bytes[offset..offset + 4]
                    .try_into()
                    .expect("slice length checked above"),
            ) as u64;
            assert_shortest_argument(value, 65_536)?;
            Ok((value, offset + 4))
        }
        27 => {
            require_available(bytes, offset, 8, "uint64 argument")?;
            let value = u64::from_be_bytes(
                bytes[offset..offset + 8]
                    .try_into()
                    .expect("slice length checked above"),
            );
            assert_shortest_argument(value, 4_294_967_296)?;
            Ok((value, offset + 8))
        }
        _ => Err(cbor_error("unsupported argument width")),
    }
}

fn assert_shortest_argument(value: u64, minimum: u64) -> Result<()> {
    if value < minimum {
        return Err(cbor_error("non-shortest integer or length"));
    }
    Ok(())
}

fn assert_unsigned_number_range(value: u64) -> Result<()> {
    if value > MAX_SAFE_INTEGER {
        return Err(cbor_error("integer outside Zuzu Number range"));
    }
    Ok(())
}

fn assert_negative_number_range(value: u64) -> Result<()> {
    if value > MAX_SAFE_INTEGER - 1 {
        return Err(cbor_error("integer outside Zuzu Number range"));
    }
    Ok(())
}

fn require_available(bytes: &[u8], offset: usize, length: usize, label: &str) -> Result<()> {
    if offset
        .checked_add(length)
        .is_none_or(|end| end > bytes.len())
    {
        return Err(cbor_error(format!("incomplete {label}")));
    }
    Ok(())
}

fn to_serde_cbor(value: &CborValue) -> Result<SerdeCborValue> {
    Ok(match value {
        CborValue::Null => SerdeCborValue::Null,
        CborValue::Bool(value) => SerdeCborValue::Bool(*value),
        CborValue::Integer(value) => SerdeCborValue::Integer((*value).into()),
        CborValue::Float(value) => SerdeCborValue::Float(*value),
        CborValue::Text(value) => SerdeCborValue::Text(value.clone()),
        CborValue::Bytes(value) => SerdeCborValue::Bytes(value.clone()),
        CborValue::Array(values) => SerdeCborValue::Array(
            values
                .iter()
                .map(to_serde_cbor)
                .collect::<Result<Vec<_>>>()?,
        ),
        CborValue::Map(values) => {
            let mut map = BTreeMap::new();
            for (key, value) in values {
                map.insert(to_serde_cbor(key)?, to_serde_cbor(value)?);
            }
            SerdeCborValue::Map(map)
        }
        CborValue::Tag { tag, value } => SerdeCborValue::Tag(*tag, Box::new(to_serde_cbor(value)?)),
    })
}

fn from_serde_cbor(value: SerdeCborValue) -> Result<CborValue> {
    Ok(match value {
        SerdeCborValue::Null => CborValue::Null,
        SerdeCborValue::Bool(value) => CborValue::Bool(value),
        SerdeCborValue::Integer(value) => CborValue::Integer(value.into()),
        SerdeCborValue::Float(value) => CborValue::Float(value),
        SerdeCborValue::Bytes(value) => CborValue::Bytes(value),
        SerdeCborValue::Text(value) => CborValue::Text(value),
        SerdeCborValue::Array(values) => CborValue::Array(
            values
                .into_iter()
                .map(from_serde_cbor)
                .collect::<Result<Vec<_>>>()?,
        ),
        SerdeCborValue::Map(values) => CborValue::Map(
            values
                .into_iter()
                .map(|(key, value)| Ok((from_serde_cbor(key)?, from_serde_cbor(value)?)))
                .collect::<Result<Vec<_>>>()?,
        ),
        SerdeCborValue::Tag(tag, value) => CborValue::Tag {
            tag,
            value: Box::new(from_serde_cbor(*value)?),
        },
        _ => return Err(cbor_error("unsupported decoded CBOR adapter value")),
    })
}

fn cbor_error(message: impl Into<String>) -> ZuzuRustError {
    ZuzuRustError::runtime(format!("Invalid Zuzu Marshal CBOR: {}", message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_bytes(name: &str) -> Vec<u8> {
        let repo_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let path = repo_root
            .join("stdlib/test-fixtures/marshal/golden")
            .join(format!("{name}.b64"));
        decode_base64(
            &std::fs::read_to_string(path).expect("marshal golden fixture should be readable"),
        )
    }

    fn decode_base64(text: &str) -> Vec<u8> {
        let mut out = Vec::new();
        let mut buffer = 0u32;
        let mut bits = 0u8;
        for byte in text.bytes().filter(|byte| !byte.is_ascii_whitespace()) {
            if byte == b'=' {
                break;
            }
            let value = match byte {
                b'A'..=b'Z' => byte - b'A',
                b'a'..=b'z' => byte - b'a' + 26,
                b'0'..=b'9' => byte - b'0' + 52,
                b'+' => 62,
                b'/' => 63,
                _ => panic!("invalid base64 byte {byte}"),
            };
            buffer = (buffer << 6) | value as u32;
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                out.push(((buffer >> bits) & 0xff) as u8);
            }
        }
        out
    }

    fn envelope_from_fixture(name: &str) -> Vec<CborValue> {
        let CborValue::Tag { tag, value } =
            decode_one(&fixture_bytes(name)).expect("fixture should decode")
        else {
            panic!("fixture should decode as a tagged envelope");
        };
        assert_eq!(tag, 55_799);
        let CborValue::Array(envelope) = *value else {
            panic!("tagged envelope value should be an array");
        };
        assert_eq!(envelope.len(), 6);
        assert_eq!(envelope[0], CborValue::Text("ZUZU-MARSHAL".to_owned()));
        assert_eq!(envelope[1], CborValue::Integer(1));
        envelope
    }

    #[test]
    fn scalar_null_fixture_decodes_and_reencodes_exactly() {
        let bytes = fixture_bytes("scalar-null");
        assert_eq!(
            envelope_from_fixture("scalar-null"),
            vec![
                CborValue::Text("ZUZU-MARSHAL".to_owned()),
                CborValue::Integer(1),
                CborValue::Map(Vec::new()),
                CborValue::Null,
                CborValue::Array(Vec::new()),
                CborValue::Array(Vec::new()),
            ],
        );
        let envelope = CborValue::Tag {
            tag: 55_799,
            value: Box::new(CborValue::Array(vec![
                CborValue::Text("ZUZU-MARSHAL".to_owned()),
                CborValue::Integer(1),
                CborValue::Map(Vec::new()),
                CborValue::Null,
                CborValue::Array(Vec::new()),
                CborValue::Array(Vec::new()),
            ])),
        };
        assert_eq!(
            encode_one(&envelope).expect("envelope should encode"),
            bytes
        );
    }

    #[test]
    fn all_perl_golden_fixtures_decode_as_tagged_envelopes() {
        for name in [
            "array-cycle",
            "class",
            "dict-pairlist",
            "function",
            "object-instance",
            "scalar-null",
            "time-path",
            "trait",
        ] {
            envelope_from_fixture(name);
        }
    }

    #[test]
    fn byte_strings_round_trip_inside_the_marshal_envelope() {
        let envelope = CborValue::Tag {
            tag: 55_799,
            value: Box::new(CborValue::Array(vec![
                CborValue::Text("ZUZU-MARSHAL".to_owned()),
                CborValue::Integer(1),
                CborValue::Map(Vec::new()),
                CborValue::Bytes(vec![0, 65, 255]),
                CborValue::Array(Vec::new()),
                CborValue::Array(Vec::new()),
            ])),
        };
        let bytes = encode_one(&envelope).expect("byte-string envelope should encode");
        assert_eq!(&bytes[..3], &[0xd9, 0xd9, 0xf7]);
        let decoded = decode_one(&bytes).expect("byte-string envelope should decode");
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn profile_validation_rejects_unsupported_tags() {
        let nested_tag = [0x82, 0xd9, 0xd9, 0xf7, 0xf6, 0xf6];
        let err = validate_profile(&nested_tag).expect_err("nested tag should fail");
        assert!(err
            .to_string()
            .contains("Invalid Zuzu Marshal CBOR: unsupported tag 55799"));
    }
}
