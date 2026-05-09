use std::collections::HashMap;

use hmac::{Hmac, Mac};
use md5::Md5;
use sha1::Sha1;
use sha2::{Digest, Sha224, Sha256, Sha384, Sha512};

use super::super::Value;
use crate::error::{Result, ZuzuRustError};

const BASE64_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

pub(super) fn md5_exports() -> HashMap<String, Value> {
    HashMap::from([
        ("md5".to_owned(), Value::native_function("md5".to_owned())),
        (
            "md5_hex".to_owned(),
            Value::native_function("md5_hex".to_owned()),
        ),
        (
            "md5_b64".to_owned(),
            Value::native_function("md5_b64".to_owned()),
        ),
    ])
}

pub(super) fn sha_exports() -> HashMap<String, Value> {
    let mut exports = HashMap::new();
    for name in [
        "sha1",
        "sha1_hex",
        "sha1_b64",
        "sha224",
        "sha224_hex",
        "sha224_b64",
        "sha256",
        "sha256_hex",
        "sha256_b64",
        "sha384",
        "sha384_hex",
        "sha384_b64",
        "sha512",
        "sha512_hex",
        "sha512_b64",
        "hmac_sha1",
        "hmac_sha1_hex",
        "hmac_sha1_b64",
        "hmac_sha224",
        "hmac_sha224_hex",
        "hmac_sha224_b64",
        "hmac_sha256",
        "hmac_sha256_hex",
        "hmac_sha256_b64",
        "hmac_sha384",
        "hmac_sha384_hex",
        "hmac_sha384_b64",
        "hmac_sha512",
        "hmac_sha512_hex",
        "hmac_sha512_b64",
    ] {
        exports.insert(name.to_owned(), Value::native_function(name.to_owned()));
    }
    exports
}

pub(super) fn call(name: &str, args: &[Value]) -> Option<Result<Value>> {
    Some(match name {
        "md5" => md5_binary(args.first(), "md5"),
        "md5_hex" => md5_hex(args.first(), "md5_hex"),
        "md5_b64" => md5_b64(args.first(), "md5_b64"),
        "sha1" => sha_binary::<Sha1>(args.first(), "sha1"),
        "sha1_hex" => sha_hex::<Sha1>(args.first(), "sha1_hex"),
        "sha1_b64" => sha_b64::<Sha1>(args.first(), "sha1_b64"),
        "sha224" => sha_binary::<Sha224>(args.first(), "sha224"),
        "sha224_hex" => sha_hex::<Sha224>(args.first(), "sha224_hex"),
        "sha224_b64" => sha_b64::<Sha224>(args.first(), "sha224_b64"),
        "sha256" => sha_binary::<Sha256>(args.first(), "sha256"),
        "sha256_hex" => sha_hex::<Sha256>(args.first(), "sha256_hex"),
        "sha256_b64" => sha_b64::<Sha256>(args.first(), "sha256_b64"),
        "sha384" => sha_binary::<Sha384>(args.first(), "sha384"),
        "sha384_hex" => sha_hex::<Sha384>(args.first(), "sha384_hex"),
        "sha384_b64" => sha_b64::<Sha384>(args.first(), "sha384_b64"),
        "sha512" => sha_binary::<Sha512>(args.first(), "sha512"),
        "sha512_hex" => sha_hex::<Sha512>(args.first(), "sha512_hex"),
        "sha512_b64" => sha_b64::<Sha512>(args.first(), "sha512_b64"),
        "hmac_sha1" => hmac_digest_sha1(args, "hmac_sha1").map(Value::BinaryString),
        "hmac_sha1_hex" => {
            hmac_digest_sha1(args, "hmac_sha1_hex").map(|value| Value::String(to_hex(&value)))
        }
        "hmac_sha1_b64" => hmac_digest_sha1(args, "hmac_sha1_b64")
            .map(|value| Value::String(to_base64_no_pad(&value))),
        "hmac_sha224" => hmac_digest_sha224(args, "hmac_sha224").map(Value::BinaryString),
        "hmac_sha224_hex" => {
            hmac_digest_sha224(args, "hmac_sha224_hex").map(|value| Value::String(to_hex(&value)))
        }
        "hmac_sha224_b64" => hmac_digest_sha224(args, "hmac_sha224_b64")
            .map(|value| Value::String(to_base64_no_pad(&value))),
        "hmac_sha256" => hmac_digest_sha256(args, "hmac_sha256").map(Value::BinaryString),
        "hmac_sha256_hex" => {
            hmac_digest_sha256(args, "hmac_sha256_hex").map(|value| Value::String(to_hex(&value)))
        }
        "hmac_sha256_b64" => hmac_digest_sha256(args, "hmac_sha256_b64")
            .map(|value| Value::String(to_base64_no_pad(&value))),
        "hmac_sha384" => hmac_digest_sha384(args, "hmac_sha384").map(Value::BinaryString),
        "hmac_sha384_hex" => {
            hmac_digest_sha384(args, "hmac_sha384_hex").map(|value| Value::String(to_hex(&value)))
        }
        "hmac_sha384_b64" => hmac_digest_sha384(args, "hmac_sha384_b64")
            .map(|value| Value::String(to_base64_no_pad(&value))),
        "hmac_sha512" => hmac_digest_sha512(args, "hmac_sha512").map(Value::BinaryString),
        "hmac_sha512_hex" => {
            hmac_digest_sha512(args, "hmac_sha512_hex").map(|value| Value::String(to_hex(&value)))
        }
        "hmac_sha512_b64" => hmac_digest_sha512(args, "hmac_sha512_b64")
            .map(|value| Value::String(to_base64_no_pad(&value))),
        _ => return None,
    })
}

fn md5_binary(value: Option<&Value>, label: &str) -> Result<Value> {
    let bytes = require_binary(value, label)?;
    Ok(Value::BinaryString(Md5::digest(bytes).to_vec()))
}

fn md5_hex(value: Option<&Value>, label: &str) -> Result<Value> {
    let bytes = require_binary(value, label)?;
    Ok(Value::String(to_hex(&Md5::digest(bytes))))
}

fn md5_b64(value: Option<&Value>, label: &str) -> Result<Value> {
    let bytes = require_binary(value, label)?;
    Ok(Value::String(to_base64_no_pad(&Md5::digest(bytes))))
}

fn sha_binary<D>(value: Option<&Value>, label: &str) -> Result<Value>
where
    D: Digest,
{
    let bytes = require_binary(value, label)?;
    Ok(Value::BinaryString(D::digest(bytes).to_vec()))
}

fn sha_hex<D>(value: Option<&Value>, label: &str) -> Result<Value>
where
    D: Digest,
{
    let bytes = require_binary(value, label)?;
    Ok(Value::String(to_hex(&D::digest(bytes))))
}

fn sha_b64<D>(value: Option<&Value>, label: &str) -> Result<Value>
where
    D: Digest,
{
    let bytes = require_binary(value, label)?;
    Ok(Value::String(to_base64_no_pad(&D::digest(bytes))))
}

fn require_binary<'a>(value: Option<&'a Value>, label: &str) -> Result<&'a [u8]> {
    match value {
        Some(Value::BinaryString(bytes)) => Ok(bytes.as_slice()),
        Some(other) => Err(ZuzuRustError::thrown(format!(
            "TypeException: {label} expects BinaryString, got {}",
            other.type_name()
        ))),
        None => Err(ZuzuRustError::thrown(format!(
            "TypeException: {label} expects BinaryString, got Null"
        ))),
    }
}

fn require_hmac_inputs<'a>(args: &'a [Value], label: &str) -> Result<(&'a [u8], &'a [u8])> {
    let value = require_binary(args.first(), label)?;
    let key = match args.get(1) {
        Some(Value::BinaryString(bytes)) => bytes.as_slice(),
        Some(other) => {
            return Err(ZuzuRustError::thrown(format!(
                "TypeException: {label} expects BinaryString key, got {}",
                other.type_name()
            )))
        }
        None => {
            return Err(ZuzuRustError::thrown(format!(
                "TypeException: {label} expects BinaryString key, got Null"
            )))
        }
    };
    Ok((value, key))
}

fn hmac_digest_sha1(args: &[Value], label: &str) -> Result<Vec<u8>> {
    let (value, key) = require_hmac_inputs(args, label)?;
    let mut mac = Hmac::<Sha1>::new_from_slice(key).map_err(|_| {
        ZuzuRustError::thrown(format!(
            "TypeException: {label} expects BinaryString key, got Null"
        ))
    })?;
    mac.update(value);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn hmac_digest_sha224(args: &[Value], label: &str) -> Result<Vec<u8>> {
    let (value, key) = require_hmac_inputs(args, label)?;
    let mut mac = Hmac::<Sha224>::new_from_slice(key).map_err(|_| {
        ZuzuRustError::thrown(format!(
            "TypeException: {label} expects BinaryString key, got Null"
        ))
    })?;
    mac.update(value);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn hmac_digest_sha256(args: &[Value], label: &str) -> Result<Vec<u8>> {
    let (value, key) = require_hmac_inputs(args, label)?;
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(|_| {
        ZuzuRustError::thrown(format!(
            "TypeException: {label} expects BinaryString key, got Null"
        ))
    })?;
    mac.update(value);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn hmac_digest_sha384(args: &[Value], label: &str) -> Result<Vec<u8>> {
    let (value, key) = require_hmac_inputs(args, label)?;
    let mut mac = Hmac::<Sha384>::new_from_slice(key).map_err(|_| {
        ZuzuRustError::thrown(format!(
            "TypeException: {label} expects BinaryString key, got Null"
        ))
    })?;
    mac.update(value);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn hmac_digest_sha512(args: &[Value], label: &str) -> Result<Vec<u8>> {
    let (value, key) = require_hmac_inputs(args, label)?;
    let mut mac = Hmac::<Sha512>::new_from_slice(key).map_err(|_| {
        ZuzuRustError::thrown(format!(
            "TypeException: {label} expects BinaryString key, got Null"
        ))
    })?;
    mac.update(value);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn to_base64_no_pad(bytes: &[u8]) -> String {
    let mut out = String::new();
    let mut index = 0usize;
    while index < bytes.len() {
        let b0 = bytes[index];
        let b1 = bytes.get(index + 1).copied().unwrap_or(0);
        let b2 = bytes.get(index + 2).copied().unwrap_or(0);
        let triple = ((b0 as u32) << 16) | ((b1 as u32) << 8) | (b2 as u32);
        out.push(BASE64_ALPHABET[((triple >> 18) & 0x3f) as usize] as char);
        out.push(BASE64_ALPHABET[((triple >> 12) & 0x3f) as usize] as char);
        if index + 1 < bytes.len() {
            out.push(BASE64_ALPHABET[((triple >> 6) & 0x3f) as usize] as char);
        }
        if index + 2 < bytes.len() {
            out.push(BASE64_ALPHABET[(triple & 0x3f) as usize] as char);
        }
        index += 3;
    }
    out
}
