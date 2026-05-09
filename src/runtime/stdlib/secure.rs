use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

use super::super::collection::common::require_arity;
use super::super::{
    ClassBase, FieldSpec, MethodValue, ObjectValue, Runtime, TraitValue, UserClassValue, Value,
};
use crate::error::{Result, ZuzuRustError};
use aes_gcm::aead::AeadInPlace;
use aes_gcm::{Aes256Gcm, KeyInit, Nonce, Tag};
use argon2::{Algorithm, Argon2, Params as Argon2Params, Version};
use ed25519_dalek::pkcs8::{DecodePrivateKey, DecodePublicKey, EncodePrivateKey, EncodePublicKey};
use ed25519_dalek::{Signature, Signer, SigningKey as Ed25519SigningKey, Verifier, VerifyingKey};
use hmac::{Hmac, Mac};
use openssl::pkcs12::Pkcs12;
use openssl::stack::Stack;
use openssl::symm::{decrypt_aead, encrypt_aead, Cipher as OpenSslCipher};
use openssl::x509::store::X509StoreBuilder;
use openssl::x509::verify::X509VerifyParam;
use openssl::x509::{X509StoreContext, X509};
use p256::ecdsa::{
    Signature as P256Signature, SigningKey as P256SigningKey, VerifyingKey as P256VerifyingKey,
};
use p384::ecdsa::{
    Signature as P384Signature, SigningKey as P384SigningKey, VerifyingKey as P384VerifyingKey,
};
use pbkdf2::pbkdf2_hmac;
use scrypt::{scrypt, Params as ScryptParams};
use sha2::{Digest, Sha256};
use x25519_dalek::{PublicKey as X25519PublicKey, StaticSecret as X25519StaticSecret};

use super::time::time_object;

const BASE64URL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
const BASE64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const HOST: &str = "rust";
const MAX_SAFE_INT: f64 = 9_007_199_254_740_992.0;
const RANDOM_INT_SPACE: u64 = 1u64 << 56;
const RANDOM_CAPABILITIES: [&str; 3] = ["bytes", "token", "int"];
const PASSWORD_HASH_CAPABILITIES: [&str; 3] = ["argon2id", "pbkdf2-sha256", "scrypt"];
const KDF_CAPABILITIES: [&str; 1] = ["hkdf-sha256"];
const CIPHER_CAPABILITIES: [&str; 2] = ["aes-256-gcm", "chacha20-poly1305"];
const KEY_AGREEMENT_CAPABILITIES: [&str; 1] = ["x25519"];
const SIGNING_CAPABILITIES: [&str; 3] = ["ed25519", "ecdsa-p256-sha256", "ecdsa-p384-sha384"];
const CERTIFICATE_CAPABILITIES: [&str; 5] = [
    "parse-x509",
    "parse-x509-der",
    "fingerprint-sha256",
    "public-key",
    "verify-chain",
];
const TLS_IDENTITY_CAPABILITIES: [&str; 2] = ["pem", "pkcs12"];
const DEFAULT_PASSWORD_HASH_ALGORITHM: &str = "pbkdf2-sha256";
const HKDF_SHA256_HASH_LENGTH: usize = 32;
const HKDF_SHA256_MAX_LENGTH: usize = 255 * HKDF_SHA256_HASH_LENGTH;
const CIPHER_KEY_LENGTH: usize = 32;
const CIPHER_NONCE_LENGTH: usize = 12;
const CIPHER_TAG_LENGTH: usize = 16;
const PASSWORD_HASH_SALT_LENGTH: usize = 16;
const PASSWORD_HASH_LENGTH: usize = 32;
const PBKDF2_SHA256_ITERATIONS: usize = 600_000;
const ARGON2ID_MEMORY: usize = 19_456;
const ARGON2ID_ITERATIONS: usize = 2;
const ARGON2ID_PARALLELISM: usize = 1;
const SCRYPT_LOG_N: usize = 17;
const SCRYPT_R: usize = 8;
const SCRYPT_P: usize = 1;

const CLASSES: [&str; 12] = [
    "Secure",
    "SecureRandom",
    "PasswordHash",
    "KeyDerivation",
    "Cipher",
    "KeyAgreement",
    "SigningKey",
    "Certificate",
    "PrivateKey",
    "PublicKey",
    "SealedBox",
    "TlsIdentity",
];

pub(super) fn exports() -> HashMap<String, Value> {
    let mut exports = HashMap::new();
    for name in CLASSES {
        exports.insert(name.to_owned(), Value::builtin_class(name.to_owned()));
    }
    exports
}

pub(super) fn call_class_method(
    runtime: &Runtime,
    class_name: &str,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    match class_name {
        "Secure" => Some(call_secure_method(name, args)),
        "SecureRandom" => Some(call_random_method(name, args)),
        "PasswordHash" => Some(call_password_hash_method(runtime, name, args)),
        "KeyDerivation" => Some(call_key_derivation_method(runtime, name, args)),
        "Cipher" => Some(call_cipher_method(runtime, name, args)),
        "KeyAgreement" => Some(call_key_agreement_class_method(runtime, name, args)),
        "SigningKey" => Some(call_signing_key_class_method(runtime, name, args)),
        "Certificate" => Some(call_certificate_class_method(name, args)),
        "TlsIdentity" => Some(call_tls_identity_class_method(name, args)),
        _ => None,
    }
}

pub(super) fn has_class_method(class_name: &str, name: &str) -> bool {
    match class_name {
        "Secure" => matches!(name, "capabilities" | "has" | "require"),
        "SecureRandom" => matches!(name, "bytes" | "token" | "int"),
        "PasswordHash" => matches!(
            name,
            "default_algorithm"
                | "hash"
                | "hash_async"
                | "verify"
                | "verify_async"
                | "needs_rehash"
                | "derive_key"
                | "derive_key_async"
        ),
        "KeyDerivation" => matches!(name, "hkdf_sha256" | "hkdf_sha256_async"),
        "Cipher" => matches!(
            name,
            "generate_key" | "encrypt" | "decrypt" | "encrypt_async" | "decrypt_async"
        ),
        "KeyAgreement" => matches!(
            name,
            "generate"
                | "generate_async"
                | "import_private"
                | "import_private_async"
                | "import_public"
                | "import_public_async"
        ),
        "SigningKey" => matches!(
            name,
            "generate"
                | "generate_async"
                | "import_private"
                | "import_private_async"
                | "import_public"
                | "import_public_async"
        ),
        "Certificate" => matches!(name, "parse" | "parse_chain" | "verify_chain"),
        "TlsIdentity" => matches!(name, "from_pem" | "from_pkcs12"),
        _ => false,
    }
}

pub(super) fn call_object_method(
    runtime: &Runtime,
    class_name: &str,
    builtin_value: &Value,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    match class_name {
        "KeyAgreement" => Some(call_key_agreement_object_method(
            runtime,
            builtin_value,
            name,
            args,
        )),
        "SigningKey" => Some(call_signing_key_object_method(
            runtime,
            builtin_value,
            name,
            args,
        )),
        "PublicKey" => Some(call_public_key_object_method(
            runtime,
            builtin_value,
            name,
            args,
        )),
        "Certificate" => Some(call_certificate_object_method(builtin_value, name, args)),
        "TlsIdentity" => Some(call_tls_identity_object_method(builtin_value, name, args)),
        _ => None,
    }
}

pub(super) fn has_object_method(class_name: &str, name: &str) -> bool {
    match class_name {
        "KeyAgreement" => matches!(
            name,
            "public_key" | "derive" | "derive_async" | "export_private"
        ),
        "SigningKey" => matches!(
            name,
            "public_key" | "sign" | "sign_async" | "export_private"
        ),
        "PublicKey" => matches!(name, "verify" | "verify_async" | "export"),
        "Certificate" => matches!(
            name,
            "subject"
                | "issuer"
                | "serial_number"
                | "not_before"
                | "not_after"
                | "fingerprint"
                | "to_der"
                | "to_pem"
                | "public_key"
        ),
        "TlsIdentity" => matches!(name, "certificate" | "private_key"),
        _ => false,
    }
}

fn call_secure_method(name: &str, args: &[Value]) -> Result<Value> {
    match name {
        "capabilities" => capabilities(args),
        "has" => Ok(Value::Boolean(has_capability(args.first(), args.get(1)))),
        "require" => require_capability(args.first(), args.get(1)),
        _ => Err(ZuzuRustError::runtime(format!(
            "unsupported method '{name}' for Secure"
        ))),
    }
}

fn capabilities(args: &[Value]) -> Result<Value> {
    require_arity("capabilities", args, 0)?;
    Ok(capability_dict())
}

fn capability_dict() -> Value {
    Value::Dict(HashMap::from([
        ("host".to_owned(), Value::String(HOST.to_owned())),
        ("random".to_owned(), Value::Boolean(true)),
        (
            "password_hash".to_owned(),
            Value::Array(
                PASSWORD_HASH_CAPABILITIES
                    .iter()
                    .map(|name| Value::String((*name).to_owned()))
                    .collect(),
            ),
        ),
        (
            "kdf".to_owned(),
            Value::Array(
                KDF_CAPABILITIES
                    .iter()
                    .map(|name| Value::String((*name).to_owned()))
                    .collect(),
            ),
        ),
        (
            "cipher".to_owned(),
            Value::Array(
                CIPHER_CAPABILITIES
                    .iter()
                    .map(|name| Value::String((*name).to_owned()))
                    .collect(),
            ),
        ),
        (
            "key_agreement".to_owned(),
            Value::Array(
                KEY_AGREEMENT_CAPABILITIES
                    .iter()
                    .map(|name| Value::String((*name).to_owned()))
                    .collect(),
            ),
        ),
        (
            "signing".to_owned(),
            Value::Array(
                SIGNING_CAPABILITIES
                    .iter()
                    .map(|name| Value::String((*name).to_owned()))
                    .collect(),
            ),
        ),
        (
            "certificate".to_owned(),
            Value::Array(
                CERTIFICATE_CAPABILITIES
                    .iter()
                    .map(|name| Value::String((*name).to_owned()))
                    .collect(),
            ),
        ),
        (
            "tls_identity".to_owned(),
            Value::Array(
                TLS_IDENTITY_CAPABILITIES
                    .iter()
                    .map(|name| Value::String((*name).to_owned()))
                    .collect(),
            ),
        ),
        (
            "async_required".to_owned(),
            Value::Dict(HashMap::from([
                ("cipher".to_owned(), Value::Boolean(false)),
                ("kdf".to_owned(), Value::Boolean(false)),
                ("password_hash".to_owned(), Value::Boolean(false)),
                ("signing".to_owned(), Value::Boolean(false)),
                ("key_agreement".to_owned(), Value::Boolean(false)),
            ])),
        ),
    ]))
}

fn capability_part(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Number(number)) => {
            if number.fract() == 0.0 {
                format!("{number:.0}")
            } else {
                number.to_string()
            }
        }
        Some(Value::Boolean(value)) => value.to_string(),
        Some(Value::Null) | None => String::new(),
        Some(other) => other.type_name().to_owned(),
    }
}

fn has_capability(area: Option<&Value>, name: Option<&Value>) -> bool {
    let area = capability_part(area);
    let name = capability_part(name);
    match area.as_str() {
        "random" => RANDOM_CAPABILITIES.contains(&name.as_str()),
        "password_hash" => PASSWORD_HASH_CAPABILITIES.contains(&name.as_str()),
        "kdf" => KDF_CAPABILITIES.contains(&name.as_str()),
        "cipher" => CIPHER_CAPABILITIES.contains(&name.as_str()),
        "key_agreement" => KEY_AGREEMENT_CAPABILITIES.contains(&name.as_str()),
        "signing" => SIGNING_CAPABILITIES.contains(&name.as_str()),
        "certificate" => CERTIFICATE_CAPABILITIES.contains(&name.as_str()),
        "tls_identity" => TLS_IDENTITY_CAPABILITIES.contains(&name.as_str()),
        _ => false,
    }
}

fn require_capability(area: Option<&Value>, name: Option<&Value>) -> Result<Value> {
    if has_capability(area, name) {
        return Ok(Value::Boolean(true));
    }
    let area = capability_part(area);
    let name = capability_part(name);
    Err(ZuzuRustError::thrown(format!(
        "Secure capability '{area}/{name}' is not available on host '{HOST}'"
    )))
}

fn call_random_method(name: &str, args: &[Value]) -> Result<Value> {
    match name {
        "bytes" => {
            require_arity(name, args, 1)?;
            let length = non_negative_integer(&args[0], "SecureRandom.bytes")?;
            let mut bytes = vec![0u8; length];
            fill_random(&mut bytes)?;
            Ok(Value::BinaryString(bytes))
        }
        "token" => {
            if args.len() > 1 {
                return Err(ZuzuRustError::runtime(
                    "SecureRandom.token expects zero or one argument",
                ));
            }
            let length = match args.first() {
                Some(Value::Null) | None => 32usize,
                Some(value) => non_negative_integer(value, "SecureRandom.token")?,
            };
            let mut bytes = vec![0u8; length];
            fill_random(&mut bytes)?;
            Ok(Value::String(base64url(&bytes)))
        }
        "int" => {
            require_arity(name, args, 1)?;
            let max = positive_integer(&args[0], "SecureRandom.int")?;
            Ok(Value::Number(random_int(max)? as f64))
        }
        _ => Err(ZuzuRustError::runtime(format!(
            "unsupported method '{name}' for SecureRandom"
        ))),
    }
}

fn call_password_hash_method(runtime: &Runtime, name: &str, args: &[Value]) -> Result<Value> {
    match name {
        "default_algorithm" => {
            require_arity(name, args, 0)?;
            Ok(Value::String(DEFAULT_PASSWORD_HASH_ALGORITHM.to_owned()))
        }
        "hash" => password_hash(args),
        "hash_async" => Ok(runtime.task_resolved(password_hash(args)?)),
        "verify" => password_hash_verify(args),
        "verify_async" => Ok(runtime.task_resolved(password_hash_verify(args)?)),
        "needs_rehash" => password_hash_needs_rehash(args),
        "derive_key" => password_hash_derive_key(args),
        "derive_key_async" => Ok(runtime.task_resolved(password_hash_derive_key(args)?)),
        _ => Err(ZuzuRustError::runtime(format!(
            "unsupported method '{name}' for PasswordHash"
        ))),
    }
}

fn call_key_derivation_method(runtime: &Runtime, name: &str, args: &[Value]) -> Result<Value> {
    match name {
        "hkdf_sha256" => hkdf_sha256(args),
        "hkdf_sha256_async" => Ok(runtime.task_resolved(hkdf_sha256(args)?)),
        _ => Err(ZuzuRustError::runtime(format!(
            "unsupported method '{name}' for KeyDerivation"
        ))),
    }
}

fn call_cipher_method(runtime: &Runtime, name: &str, args: &[Value]) -> Result<Value> {
    match name {
        "generate_key" => cipher_generate_key(args),
        "encrypt" => cipher_encrypt(args),
        "decrypt" => cipher_decrypt(args),
        "encrypt_async" => Ok(runtime.task_resolved(cipher_encrypt(args)?)),
        "decrypt_async" => Ok(runtime.task_resolved(cipher_decrypt(args)?)),
        _ => Err(ZuzuRustError::runtime(format!(
            "unsupported method '{name}' for Cipher"
        ))),
    }
}

fn call_key_agreement_class_method(runtime: &Runtime, name: &str, args: &[Value]) -> Result<Value> {
    match name {
        "generate" => key_agreement_generate(args),
        "generate_async" => Ok(runtime.task_resolved(key_agreement_generate(args)?)),
        "import_private" => key_agreement_import_private(args),
        "import_private_async" => Ok(runtime.task_resolved(key_agreement_import_private(args)?)),
        "import_public" => key_agreement_import_public(args),
        "import_public_async" => Ok(runtime.task_resolved(key_agreement_import_public(args)?)),
        _ => Err(ZuzuRustError::runtime(format!(
            "unsupported method '{name}' for KeyAgreement"
        ))),
    }
}

fn call_key_agreement_object_method(
    runtime: &Runtime,
    builtin_value: &Value,
    name: &str,
    args: &[Value],
) -> Result<Value> {
    match name {
        "public_key" => {
            require_arity(name, args, 0)?;
            let (algorithm, _, public) =
                key_agreement_state(builtin_value, "KeyAgreement.public_key")?;
            Ok(public_key_object(algorithm, public.to_vec()))
        }
        "derive" => key_agreement_derive(builtin_value, args),
        "derive_async" => Ok(runtime.task_resolved(key_agreement_derive(builtin_value, args)?)),
        "export_private" => key_agreement_export_private(builtin_value, args),
        _ => Err(ZuzuRustError::runtime(format!(
            "unsupported method '{name}' for KeyAgreement"
        ))),
    }
}

fn call_signing_key_class_method(runtime: &Runtime, name: &str, args: &[Value]) -> Result<Value> {
    match name {
        "generate" => signing_key_generate(args),
        "generate_async" => Ok(runtime.task_resolved(signing_key_generate(args)?)),
        "import_private" => signing_key_import_private(args),
        "import_private_async" => Ok(runtime.task_resolved(signing_key_import_private(args)?)),
        "import_public" => signing_key_import_public(args),
        "import_public_async" => Ok(runtime.task_resolved(signing_key_import_public(args)?)),
        _ => Err(ZuzuRustError::runtime(format!(
            "unsupported method '{name}' for SigningKey"
        ))),
    }
}

fn call_signing_key_object_method(
    runtime: &Runtime,
    builtin_value: &Value,
    name: &str,
    args: &[Value],
) -> Result<Value> {
    match name {
        "public_key" => {
            require_arity(name, args, 0)?;
            let (algorithm, _, public) = signing_key_state(builtin_value, "SigningKey.public_key")?;
            Ok(public_key_object(algorithm, public.to_vec()))
        }
        "sign" => signing_key_sign(builtin_value, args),
        "sign_async" => Ok(runtime.task_resolved(signing_key_sign(builtin_value, args)?)),
        "export_private" => signing_key_export_private(builtin_value, args),
        _ => Err(ZuzuRustError::runtime(format!(
            "unsupported method '{name}' for SigningKey"
        ))),
    }
}

fn call_public_key_object_method(
    runtime: &Runtime,
    builtin_value: &Value,
    name: &str,
    args: &[Value],
) -> Result<Value> {
    match name {
        "verify" => public_key_verify(builtin_value, args),
        "verify_async" => Ok(runtime.task_resolved(public_key_verify(builtin_value, args)?)),
        "export" => public_key_export(builtin_value, args),
        _ => Err(ZuzuRustError::runtime(format!(
            "unsupported method '{name}' for PublicKey"
        ))),
    }
}

fn call_certificate_class_method(name: &str, args: &[Value]) -> Result<Value> {
    match name {
        "parse" => certificate_parse(args),
        "parse_chain" => certificate_parse_chain(args),
        "verify_chain" => certificate_verify_chain(args),
        _ => Err(ZuzuRustError::runtime(format!(
            "unsupported method '{name}' for Certificate"
        ))),
    }
}

fn call_certificate_object_method(
    builtin_value: &Value,
    name: &str,
    args: &[Value],
) -> Result<Value> {
    let state = certificate_state(builtin_value, &format!("Certificate.{name}"))?;
    match name {
        "subject" => {
            require_arity(name, args, 0)?;
            Ok(Value::String(state.subject.clone()))
        }
        "issuer" => {
            require_arity(name, args, 0)?;
            Ok(Value::String(state.issuer.clone()))
        }
        "serial_number" => {
            require_arity(name, args, 0)?;
            Ok(Value::String(state.serial.clone()))
        }
        "not_before" => {
            require_arity(name, args, 0)?;
            Ok(time_object(state.not_before as f64))
        }
        "not_after" => {
            require_arity(name, args, 0)?;
            Ok(time_object(state.not_after as f64))
        }
        "fingerprint" => {
            if args.len() > 1 {
                return Err(ZuzuRustError::runtime(
                    "Certificate.fingerprint expects zero or one argument",
                ));
            }
            let algorithm = match args.first() {
                Some(Value::Null) | None => "sha256",
                Some(Value::String(text)) if text.eq_ignore_ascii_case("sha256") => "sha256",
                _ => {
                    return Err(ZuzuRustError::thrown(
                        "Certificate.fingerprint only supports sha256",
                    ))
                }
            };
            let _ = algorithm;
            Ok(Value::BinaryString(Sha256::digest(&state.der).to_vec()))
        }
        "to_der" => {
            require_arity(name, args, 0)?;
            Ok(Value::BinaryString(state.der.clone()))
        }
        "to_pem" => {
            require_arity(name, args, 0)?;
            Ok(Value::String(certificate_der_to_pem(&state.der)))
        }
        "public_key" => {
            require_arity(name, args, 0)?;
            certificate_public_key(state)
        }
        _ => Err(ZuzuRustError::runtime(format!(
            "unsupported method '{name}' for Certificate"
        ))),
    }
}

fn call_tls_identity_class_method(name: &str, args: &[Value]) -> Result<Value> {
    match name {
        "from_pem" => tls_identity_from_pem(args),
        "from_pkcs12" => tls_identity_from_pkcs12(args),
        _ => Err(ZuzuRustError::runtime(format!(
            "unsupported method '{name}' for TlsIdentity"
        ))),
    }
}

fn call_tls_identity_object_method(
    builtin_value: &Value,
    name: &str,
    args: &[Value],
) -> Result<Value> {
    match name {
        "certificate" => {
            require_arity(name, args, 0)?;
            tls_identity_certificate(builtin_value)
        }
        "private_key" => {
            require_arity(name, args, 0)?;
            tls_identity_private_key(builtin_value)
        }
        _ => Err(ZuzuRustError::runtime(format!(
            "unsupported method '{name}' for TlsIdentity"
        ))),
    }
}

fn password_hash(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(ZuzuRustError::runtime(
            "PasswordHash.hash expects one or two arguments",
        ));
    }
    let label = "PasswordHash.hash";
    let password = string_bytes_arg(args.first(), label, "String password")?;
    let options = optional_dict_arg(args.get(1), label, "Dict options")?;
    let algorithm = password_hash_algorithm(options, label)?;
    let salt = match options.and_then(|map| map.get("salt")) {
        Some(value) => binary_arg(Some(value), label, "BinaryString salt")?.to_vec(),
        None => {
            let mut salt = vec![0u8; PASSWORD_HASH_SALT_LENGTH];
            fill_random(&mut salt)?;
            salt
        }
    };
    let hash = password_hash_derive_bytes(&password, algorithm, &salt, options, label)?;

    match algorithm {
        "pbkdf2-sha256" => {
            let (iterations, length) = pbkdf2_options(options, label)?;
            Ok(Value::String(format!(
                "$zuzu-pbkdf2-sha256$v=1$i={iterations},l={length}${}${}",
                base64url(&salt),
                base64url(&hash)
            )))
        }
        "argon2id" => {
            let (memory, iterations, parallelism, _) = argon2id_options(options, label)?;
            Ok(Value::String(format!(
                "$argon2id$v=19$m={memory},t={iterations},p={parallelism}${}${}",
                base64_nopad(&salt),
                base64_nopad(&hash)
            )))
        }
        "scrypt" => {
            let (log_n, _, r, p, length) = scrypt_options(options, label)?;
            Ok(Value::String(format!(
                "$scrypt$ln={log_n},r={r},p={p},l={length}${}${}",
                base64url(&salt),
                base64url(&hash)
            )))
        }
        _ => Err(ZuzuRustError::thrown(format!(
            "{label} password hash algorithm '{algorithm}' is not available"
        ))),
    }
}

fn password_hash_verify(args: &[Value]) -> Result<Value> {
    require_arity("verify", args, 2)?;
    let label = "PasswordHash.verify";
    let password = string_bytes_arg(args.first(), label, "String password")?;
    let encoded = string_arg(args.get(1), label, "String encoded_hash")?;
    let Some(parsed) = parse_password_hash(encoded) else {
        return Ok(Value::Boolean(false));
    };
    if !PASSWORD_HASH_CAPABILITIES.contains(&parsed.algorithm) {
        return Ok(Value::Boolean(false));
    }
    let hash = match password_hash_derive_bytes(
        &password,
        parsed.algorithm,
        &parsed.salt,
        Some(&parsed.options),
        label,
    ) {
        Ok(hash) => hash,
        Err(_) => return Ok(Value::Boolean(false)),
    };
    Ok(Value::Boolean(constant_time_eq(&hash, &parsed.hash)))
}

fn password_hash_needs_rehash(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(ZuzuRustError::runtime(
            "PasswordHash.needs_rehash expects one or two arguments",
        ));
    }
    let label = "PasswordHash.needs_rehash";
    let encoded = string_arg(args.first(), label, "String encoded_hash")?;
    let options = optional_dict_arg(args.get(1), label, "Dict options")?;
    let target = password_hash_algorithm(options, label)?;
    let Some(parsed) = parse_password_hash(encoded) else {
        return Ok(Value::Boolean(true));
    };
    if parsed.algorithm != target {
        return Ok(Value::Boolean(true));
    }

    let needs = match target {
        "pbkdf2-sha256" => {
            let (iterations, length) = pbkdf2_options(options, label)?;
            parsed.number("iterations").unwrap_or(0) < iterations || parsed.hash.len() != length
        }
        "argon2id" => {
            let (memory, iterations, parallelism, length) = argon2id_options(options, label)?;
            parsed.number("memory").unwrap_or(0) < memory
                || parsed.number("iterations").unwrap_or(0) < iterations
                || parsed.number("parallelism").unwrap_or(0) != parallelism
                || parsed.hash.len() != length
        }
        "scrypt" => {
            let (log_n, _, r, p, length) = scrypt_options(options, label)?;
            parsed.number("log_n").unwrap_or(0) < log_n
                || parsed.number("r").unwrap_or(0) != r
                || parsed.number("p").unwrap_or(0) != p
                || parsed.hash.len() != length
        }
        _ => true,
    };
    Ok(Value::Boolean(needs))
}

fn password_hash_derive_key(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(ZuzuRustError::runtime(
            "PasswordHash.derive_key expects one or two arguments",
        ));
    }
    let label = "PasswordHash.derive_key";
    let password = string_bytes_arg(args.first(), label, "String password")?;
    let options = optional_dict_arg(args.get(1), label, "Dict options")?;
    let algorithm = password_hash_algorithm(options, label)?;
    let Some(map) = options else {
        return Err(ZuzuRustError::thrown(format!(
            "{label} expects BinaryString salt"
        )));
    };
    let salt = binary_arg(map.get("salt"), label, "BinaryString salt")?;
    Ok(Value::BinaryString(password_hash_derive_bytes(
        &password, algorithm, salt, options, label,
    )?))
}

fn hkdf_sha256(args: &[Value]) -> Result<Value> {
    let (input_key_material, length, salt, info) = hkdf_args(args)?;
    Ok(Value::BinaryString(hkdf_sha256_bytes(
        input_key_material,
        length,
        salt,
        info,
    )?))
}

fn cipher_generate_key(args: &[Value]) -> Result<Value> {
    if args.len() > 1 {
        return Err(ZuzuRustError::runtime(
            "Cipher.generate_key expects zero or one argument",
        ));
    }
    let meta = cipher_meta(
        cipher_algorithm(args.first(), "Cipher.generate_key")?,
        "Cipher.generate_key",
    )?;
    let mut bytes = vec![0u8; meta.key_length];
    fill_random(&mut bytes)?;
    Ok(Value::BinaryString(bytes))
}

fn cipher_encrypt(args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(ZuzuRustError::runtime(
            "Cipher.encrypt expects two or three arguments",
        ));
    }
    let label = "Cipher.encrypt";
    let plaintext = binary_arg(args.first(), label, "BinaryString plaintext")?;
    let options = cipher_options(args.get(2), label)?;
    let meta = cipher_meta(options.algorithm, label)?;
    let key = cipher_key(args.get(1), label, meta)?;
    let mut nonce = vec![0u8; meta.nonce_length];
    fill_random(&mut nonce)?;

    let (ciphertext, tag) = if options.algorithm == "aes-256-gcm" {
        let cipher = Aes256Gcm::new_from_slice(key)
            .map_err(|_| ZuzuRustError::thrown("Cipher.encrypt failed"))?;
        let mut ciphertext = plaintext.to_vec();
        let tag = cipher
            .encrypt_in_place_detached(Nonce::from_slice(&nonce), options.aad, &mut ciphertext)
            .map_err(|_| ZuzuRustError::thrown("Cipher.encrypt failed"))?;
        (ciphertext, tag.to_vec())
    } else {
        let mut tag = vec![0u8; meta.tag_length];
        let ciphertext = encrypt_aead(
            OpenSslCipher::chacha20_poly1305(),
            key,
            Some(&nonce),
            options.aad,
            plaintext,
            &mut tag,
        )
        .map_err(|_| ZuzuRustError::thrown("Cipher.encrypt failed"))?;
        (ciphertext, tag)
    };

    Ok(cipher_envelope_value(
        options.algorithm,
        nonce,
        ciphertext,
        tag,
    ))
}

fn cipher_decrypt(args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(ZuzuRustError::runtime(
            "Cipher.decrypt expects two or three arguments",
        ));
    }
    let label = "Cipher.decrypt";
    let envelope = cipher_envelope(args.first(), label)?;
    let options = cipher_options(args.get(2), label)?;
    if options.algorithm_supplied && options.algorithm != envelope.algorithm {
        return Err(ZuzuRustError::thrown(
            "Cipher.decrypt options.algorithm does not match envelope.algorithm",
        ));
    }
    let meta = cipher_meta(envelope.algorithm, label)?;
    let key = cipher_key(args.get(1), label, meta)?;
    let plaintext = if envelope.algorithm == "aes-256-gcm" {
        let cipher = Aes256Gcm::new_from_slice(key)
            .map_err(|_| ZuzuRustError::thrown("Cipher.decrypt failed"))?;
        let mut plaintext = envelope.ciphertext;
        cipher
            .decrypt_in_place_detached(
                Nonce::from_slice(&envelope.nonce),
                options.aad,
                &mut plaintext,
                Tag::from_slice(&envelope.tag),
            )
            .map_err(|_| ZuzuRustError::thrown("Cipher.decrypt authentication failed"))?;
        plaintext
    } else {
        decrypt_aead(
            OpenSslCipher::chacha20_poly1305(),
            key,
            Some(&envelope.nonce),
            options.aad,
            &envelope.ciphertext,
            &envelope.tag,
        )
        .map_err(|_| ZuzuRustError::thrown("Cipher.decrypt authentication failed"))?
    };

    Ok(Value::BinaryString(plaintext))
}

struct CipherOptions<'a> {
    algorithm: &'static str,
    algorithm_supplied: bool,
    aad: &'a [u8],
}

struct CipherEnvelope {
    algorithm: &'static str,
    nonce: Vec<u8>,
    ciphertext: Vec<u8>,
    tag: Vec<u8>,
}

struct CipherMeta {
    key_length: usize,
    nonce_length: usize,
    tag_length: usize,
}

struct ParsedPasswordHash {
    algorithm: &'static str,
    salt: Vec<u8>,
    hash: Vec<u8>,
    options: HashMap<String, Value>,
}

impl ParsedPasswordHash {
    fn number(&self, key: &str) -> Option<usize> {
        match self.options.get(key) {
            Some(Value::Number(number)) if *number >= 0.0 && number.fract() == 0.0 => {
                Some(*number as usize)
            }
            _ => None,
        }
    }
}

fn key_agreement_generate(args: &[Value]) -> Result<Value> {
    if args.len() > 1 {
        return Err(ZuzuRustError::runtime(
            "KeyAgreement.generate expects zero or one argument",
        ));
    }
    key_agreement_algorithm(args.first(), "KeyAgreement.generate")?;
    let mut private = [0u8; 32];
    fill_random(&mut private)?;
    Ok(key_agreement_key_object_from_private(private))
}

fn key_agreement_import_private(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(ZuzuRustError::runtime(
            "KeyAgreement.import_private expects one or two arguments",
        ));
    }
    let label = "KeyAgreement.import_private";
    let options = optional_dict_arg(args.get(1), label, "Dict options")?;
    key_agreement_algorithm_option(options, label)?;
    let format = key_format(args.first(), args.get(1), label)?;
    if format != "raw" {
        return Err(ZuzuRustError::thrown(format!(
            "{label} only supports raw format"
        )));
    }
    let bytes = binary_arg(args.first(), label, "BinaryString key")?;
    let private = x25519_private_bytes(bytes, label)?;
    Ok(key_agreement_key_object_from_private(private))
}

fn key_agreement_import_public(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(ZuzuRustError::runtime(
            "KeyAgreement.import_public expects one or two arguments",
        ));
    }
    let label = "KeyAgreement.import_public";
    let options = optional_dict_arg(args.get(1), label, "Dict options")?;
    key_agreement_algorithm_option(options, label)?;
    let format = key_format(args.first(), args.get(1), label)?;
    if format != "raw" {
        return Err(ZuzuRustError::thrown(format!(
            "{label} only supports raw format"
        )));
    }
    let bytes = binary_arg(args.first(), label, "BinaryString key")?;
    let public = x25519_public_bytes(bytes, label)?;
    Ok(public_key_object("x25519", public.to_vec()))
}

fn key_agreement_export_private(builtin_value: &Value, args: &[Value]) -> Result<Value> {
    if args.len() > 1 {
        return Err(ZuzuRustError::runtime(
            "KeyAgreement.export_private expects zero or one argument",
        ));
    }
    let label = "KeyAgreement.export_private";
    let (_, private, _) = key_agreement_state(builtin_value, label)?;
    let format = export_format(args.first(), label)?;
    if format != "raw" {
        return Err(ZuzuRustError::thrown(format!(
            "{label} only supports raw format"
        )));
    }
    Ok(Value::BinaryString(private.to_vec()))
}

fn key_agreement_derive(builtin_value: &Value, args: &[Value]) -> Result<Value> {
    require_arity("derive", args, 1)?;
    let label = "KeyAgreement.derive";
    let (_, private, _) = key_agreement_state(builtin_value, label)?;
    let (peer_algorithm, peer_public) = public_key_argument(args.first(), label)?;
    if peer_algorithm != "x25519" {
        return Err(ZuzuRustError::thrown(format!(
            "{label} expects an x25519 public key"
        )));
    }
    let private = X25519StaticSecret::from(x25519_private_bytes(private, label)?);
    let public = X25519PublicKey::from(x25519_public_bytes(&peer_public, label)?);
    Ok(Value::BinaryString(
        private.diffie_hellman(&public).as_bytes().to_vec(),
    ))
}

fn signing_key_generate(args: &[Value]) -> Result<Value> {
    if args.len() > 1 {
        return Err(ZuzuRustError::runtime(
            "SigningKey.generate expects zero or one argument",
        ));
    }
    let algorithm = signing_algorithm(args.first(), "SigningKey.generate")?;
    match algorithm {
        "ed25519" => {
            let mut seed = [0u8; 32];
            fill_random(&mut seed)?;
            let signing = Ed25519SigningKey::from_bytes(&seed);
            Ok(signing_key_object(
                algorithm,
                signing.to_bytes().to_vec(),
                signing.verifying_key().to_bytes().to_vec(),
            ))
        }
        "ecdsa-p256-sha256" => loop {
            let mut private = [0u8; 32];
            fill_random(&mut private)?;
            if let Ok(signing) = P256SigningKey::from_slice(&private) {
                return Ok(signing_key_object(
                    algorithm,
                    signing.to_bytes().to_vec(),
                    signing.verifying_key().to_sec1_bytes().to_vec(),
                ));
            }
        },
        "ecdsa-p384-sha384" => loop {
            let mut private = [0u8; 48];
            fill_random(&mut private)?;
            if let Ok(signing) = P384SigningKey::from_slice(&private) {
                return Ok(signing_key_object(
                    algorithm,
                    signing.to_bytes().to_vec(),
                    signing.verifying_key().to_sec1_bytes().to_vec(),
                ));
            }
        },
        _ => Err(ZuzuRustError::runtime("unreachable signing algorithm")),
    }
}

fn signing_key_import_private(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(ZuzuRustError::runtime(
            "SigningKey.import_private expects one or two arguments",
        ));
    }
    let label = "SigningKey.import_private";
    let options = optional_dict_arg(args.get(1), label, "Dict options")?;
    let mut algorithm = signing_algorithm_option(options, label)?;
    let format = key_format(args.first(), args.get(1), label)?;
    match format {
        "raw" => {
            let bytes = binary_arg(args.first(), label, "BinaryString key")?;
            algorithm.get_or_insert("ed25519");
            match algorithm.unwrap() {
                "ed25519" => {
                    if bytes.len() != 32 {
                        return Err(ZuzuRustError::thrown(format!(
                            "{label} expects a 32-byte raw private key"
                        )));
                    }
                    let seed: [u8; 32] = bytes.try_into().map_err(|_| {
                        ZuzuRustError::thrown(format!("{label} expects a 32-byte raw private key"))
                    })?;
                    let signing = Ed25519SigningKey::from_bytes(&seed);
                    Ok(signing_key_object(
                        "ed25519",
                        signing.to_bytes().to_vec(),
                        signing.verifying_key().to_bytes().to_vec(),
                    ))
                }
                "ecdsa-p256-sha256" => {
                    let signing = P256SigningKey::from_slice(bytes).map_err(|_| {
                        ZuzuRustError::thrown(format!("{label} expects a valid P-256 private key"))
                    })?;
                    Ok(signing_key_object(
                        "ecdsa-p256-sha256",
                        signing.to_bytes().to_vec(),
                        signing.verifying_key().to_sec1_bytes().to_vec(),
                    ))
                }
                "ecdsa-p384-sha384" => {
                    let signing = P384SigningKey::from_slice(bytes).map_err(|_| {
                        ZuzuRustError::thrown(format!("{label} expects a valid P-384 private key"))
                    })?;
                    Ok(signing_key_object(
                        "ecdsa-p384-sha384",
                        signing.to_bytes().to_vec(),
                        signing.verifying_key().to_sec1_bytes().to_vec(),
                    ))
                }
                _ => Err(ZuzuRustError::runtime("unreachable signing algorithm")),
            }
        }
        "pem" => {
            let pem = string_arg(args.first(), label, "String key")?;
            if let Some(algorithm) = algorithm {
                let (actual, private, public) = private_key_from_pem(algorithm, pem, label)?;
                if actual != algorithm {
                    return Err(ZuzuRustError::thrown(format!(
                        "{label} PEM key algorithm does not match {algorithm}"
                    )));
                }
                Ok(signing_key_object(actual, private, public))
            } else {
                let (algorithm, private, public) = infer_private_key_from_pem(pem, label)?;
                Ok(signing_key_object(algorithm, private, public))
            }
        }
        _ => Err(ZuzuRustError::thrown(format!(
            "{label} only supports raw and pem formats"
        ))),
    }
}

fn signing_key_import_public(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(ZuzuRustError::runtime(
            "SigningKey.import_public expects one or two arguments",
        ));
    }
    let label = "SigningKey.import_public";
    let options = optional_dict_arg(args.get(1), label, "Dict options")?;
    let mut algorithm = signing_algorithm_option(options, label)?;
    let format = key_format(args.first(), args.get(1), label)?;
    match format {
        "raw" => {
            let bytes = binary_arg(args.first(), label, "BinaryString key")?;
            if algorithm.is_none() {
                algorithm = Some(algorithm_from_raw_public(bytes, label)?);
            }
            let algorithm = algorithm.unwrap();
            validate_raw_public_length(bytes, algorithm, label)?;
            match algorithm {
                "ed25519" => {
                    let public: [u8; 32] = bytes.try_into().map_err(|_| {
                        ZuzuRustError::thrown(format!("{label} expects a 32-byte raw public key"))
                    })?;
                    VerifyingKey::from_bytes(&public).map_err(|_| {
                        ZuzuRustError::thrown(format!("{label} expects a valid Ed25519 public key"))
                    })?;
                    Ok(public_key_object(algorithm, bytes.to_vec()))
                }
                "ecdsa-p256-sha256" => {
                    P256VerifyingKey::from_sec1_bytes(bytes).map_err(|_| {
                        ZuzuRustError::thrown(format!("{label} expects a valid P-256 public key"))
                    })?;
                    Ok(public_key_object(algorithm, bytes.to_vec()))
                }
                "ecdsa-p384-sha384" => {
                    P384VerifyingKey::from_sec1_bytes(bytes).map_err(|_| {
                        ZuzuRustError::thrown(format!("{label} expects a valid P-384 public key"))
                    })?;
                    Ok(public_key_object(algorithm, bytes.to_vec()))
                }
                _ => Err(ZuzuRustError::runtime("unreachable signing algorithm")),
            }
        }
        "pem" => {
            let pem = string_arg(args.first(), label, "String key")?;
            if let Some(algorithm) = algorithm {
                let (actual, public) = public_key_from_pem(algorithm, pem, label)?;
                if actual != algorithm {
                    return Err(ZuzuRustError::thrown(format!(
                        "{label} PEM key algorithm does not match {algorithm}"
                    )));
                }
                Ok(public_key_object(actual, public))
            } else {
                let (algorithm, public) = infer_public_key_from_pem(pem, label)?;
                Ok(public_key_object(algorithm, public))
            }
        }
        _ => Err(ZuzuRustError::thrown(format!(
            "{label} only supports raw and pem formats"
        ))),
    }
}

fn signing_key_sign(builtin_value: &Value, args: &[Value]) -> Result<Value> {
    require_arity("sign", args, 1)?;
    let label = "SigningKey.sign";
    let (algorithm, private, _) = signing_key_state(builtin_value, label)?;
    let message = binary_arg(args.first(), label, "BinaryString message")?;
    match algorithm {
        "ed25519" => {
            let seed: [u8; 32] = private
                .try_into()
                .map_err(|_| ZuzuRustError::runtime("SigningKey internal state invalid"))?;
            let signing = Ed25519SigningKey::from_bytes(&seed);
            Ok(Value::BinaryString(
                signing.sign(message).to_bytes().to_vec(),
            ))
        }
        "ecdsa-p256-sha256" => {
            let signing = P256SigningKey::from_slice(private)
                .map_err(|_| ZuzuRustError::runtime("SigningKey internal state invalid"))?;
            let signature: P256Signature = signing.sign(message);
            Ok(Value::BinaryString(signature.to_der().as_bytes().to_vec()))
        }
        "ecdsa-p384-sha384" => {
            let signing = P384SigningKey::from_slice(private)
                .map_err(|_| ZuzuRustError::runtime("SigningKey internal state invalid"))?;
            let signature: P384Signature = signing.sign(message);
            Ok(Value::BinaryString(signature.to_der().as_bytes().to_vec()))
        }
        _ => Err(ZuzuRustError::runtime(
            "SigningKey internal algorithm invalid",
        )),
    }
}

fn signing_key_export_private(builtin_value: &Value, args: &[Value]) -> Result<Value> {
    if args.len() > 1 {
        return Err(ZuzuRustError::runtime(
            "SigningKey.export_private expects zero or one argument",
        ));
    }
    let label = "SigningKey.export_private";
    let (algorithm, private, _) = signing_key_state(builtin_value, label)?;
    let format = export_format(args.first(), label)?;
    match format {
        "raw" => Ok(Value::BinaryString(private.to_vec())),
        "pem" => {
            let pem = match algorithm {
                "ed25519" => {
                    let seed: [u8; 32] = private
                        .try_into()
                        .map_err(|_| ZuzuRustError::runtime("SigningKey internal state invalid"))?;
                    Ed25519SigningKey::from_bytes(&seed)
                        .to_pkcs8_pem(Default::default())
                        .map_err(|_| ZuzuRustError::thrown(format!("{label} failed")))?
                        .to_string()
                }
                "ecdsa-p256-sha256" => P256SigningKey::from_slice(private)
                    .map_err(|_| ZuzuRustError::runtime("SigningKey internal state invalid"))?
                    .to_pkcs8_pem(Default::default())
                    .map_err(|_| ZuzuRustError::thrown(format!("{label} failed")))?
                    .to_string(),
                "ecdsa-p384-sha384" => P384SigningKey::from_slice(private)
                    .map_err(|_| ZuzuRustError::runtime("SigningKey internal state invalid"))?
                    .to_pkcs8_pem(Default::default())
                    .map_err(|_| ZuzuRustError::thrown(format!("{label} failed")))?
                    .to_string(),
                _ => {
                    return Err(ZuzuRustError::runtime(
                        "SigningKey internal algorithm invalid",
                    ))
                }
            };
            Ok(Value::String(pem.to_string()))
        }
        _ => Err(ZuzuRustError::thrown(format!(
            "{label} only supports raw and pem formats"
        ))),
    }
}

fn public_key_verify(builtin_value: &Value, args: &[Value]) -> Result<Value> {
    require_arity("verify", args, 2)?;
    let label = "PublicKey.verify";
    let (algorithm, public) = public_key_state(builtin_value, label)?;
    let message = binary_arg(args.first(), label, "BinaryString message")?;
    let signature_bytes = binary_arg(args.get(1), label, "BinaryString signature")?;
    match algorithm {
        "ed25519" => {
            if signature_bytes.len() != 64 {
                return Ok(Value::Boolean(false));
            }
            let public: [u8; 32] = public
                .try_into()
                .map_err(|_| ZuzuRustError::runtime("PublicKey internal state invalid"))?;
            let signature_bytes: [u8; 64] = signature_bytes.try_into().map_err(|_| {
                ZuzuRustError::thrown("PublicKey.verify expects a 64-byte Ed25519 signature")
            })?;
            let verifying = VerifyingKey::from_bytes(&public)
                .map_err(|_| ZuzuRustError::runtime("PublicKey internal state invalid"))?;
            let signature = Signature::from_bytes(&signature_bytes);
            Ok(Value::Boolean(
                verifying.verify(message, &signature).is_ok(),
            ))
        }
        "ecdsa-p256-sha256" => {
            let Ok(verifying) = P256VerifyingKey::from_sec1_bytes(public) else {
                return Ok(Value::Boolean(false));
            };
            let Ok(signature) = P256Signature::from_der(signature_bytes) else {
                return Ok(Value::Boolean(false));
            };
            Ok(Value::Boolean(
                verifying.verify(message, &signature).is_ok(),
            ))
        }
        "ecdsa-p384-sha384" => {
            let Ok(verifying) = P384VerifyingKey::from_sec1_bytes(public) else {
                return Ok(Value::Boolean(false));
            };
            let Ok(signature) = P384Signature::from_der(signature_bytes) else {
                return Ok(Value::Boolean(false));
            };
            Ok(Value::Boolean(
                verifying.verify(message, &signature).is_ok(),
            ))
        }
        "x25519" => Err(ZuzuRustError::thrown(format!(
            "{label} expects a signing public key"
        ))),
        _ => Err(ZuzuRustError::runtime(
            "PublicKey internal algorithm invalid",
        )),
    }
}

fn public_key_export(builtin_value: &Value, args: &[Value]) -> Result<Value> {
    if args.len() > 1 {
        return Err(ZuzuRustError::runtime(
            "PublicKey.export expects zero or one argument",
        ));
    }
    let label = "PublicKey.export";
    let (algorithm, public) = public_key_state(builtin_value, label)?;
    let format = export_format(args.first(), label)?;
    match format {
        "raw" => Ok(Value::BinaryString(public.to_vec())),
        "pem" => {
            let pem = match algorithm {
                "ed25519" => {
                    let public: [u8; 32] = public
                        .try_into()
                        .map_err(|_| ZuzuRustError::runtime("PublicKey internal state invalid"))?;
                    VerifyingKey::from_bytes(&public)
                        .map_err(|_| ZuzuRustError::runtime("PublicKey internal state invalid"))?
                        .to_public_key_pem(Default::default())
                        .map_err(|_| ZuzuRustError::thrown(format!("{label} failed")))?
                }
                "ecdsa-p256-sha256" => P256VerifyingKey::from_sec1_bytes(public)
                    .map_err(|_| ZuzuRustError::runtime("PublicKey internal state invalid"))?
                    .to_public_key_pem(Default::default())
                    .map_err(|_| ZuzuRustError::thrown(format!("{label} failed")))?,
                "ecdsa-p384-sha384" => P384VerifyingKey::from_sec1_bytes(public)
                    .map_err(|_| ZuzuRustError::runtime("PublicKey internal state invalid"))?
                    .to_public_key_pem(Default::default())
                    .map_err(|_| ZuzuRustError::thrown(format!("{label} failed")))?,
                "x25519" => {
                    return Err(ZuzuRustError::thrown(format!(
                        "{label} only supports raw format for x25519 public keys"
                    )))
                }
                _ => {
                    return Err(ZuzuRustError::runtime(
                        "PublicKey internal algorithm invalid",
                    ))
                }
            };
            Ok(Value::String(pem))
        }
        _ => Err(ZuzuRustError::thrown(format!(
            "{label} only supports raw and pem formats"
        ))),
    }
}

#[derive(Clone)]
struct CertificateState {
    der: Vec<u8>,
    subject: String,
    issuer: String,
    serial: String,
    not_before: i64,
    not_after: i64,
    public_algorithm: String,
    public_key: Vec<u8>,
}

struct DerElement<'a> {
    tag: u8,
    body: &'a [u8],
}

struct DerReader<'a> {
    bytes: &'a [u8],
    offset: usize,
    label: &'static str,
}

impl<'a> DerReader<'a> {
    fn new(bytes: &'a [u8], label: &'static str) -> Self {
        Self {
            bytes,
            offset: 0,
            label,
        }
    }

    fn is_empty(&self) -> bool {
        self.offset >= self.bytes.len()
    }

    fn read_element(&mut self, expected: Option<u8>) -> Result<DerElement<'a>> {
        if self.offset + 2 > self.bytes.len() {
            return Err(der_x509_error(self.label));
        }
        let tag = self.bytes[self.offset];
        self.offset += 1;
        if let Some(expected) = expected {
            if tag != expected {
                return Err(der_x509_error(self.label));
            }
        }
        let mut length = self.bytes[self.offset] as usize;
        self.offset += 1;
        if length & 0x80 != 0 {
            let count = length & 0x7f;
            if count == 0 || count > 4 || self.offset + count > self.bytes.len() {
                return Err(der_x509_error(self.label));
            }
            length = 0;
            for _ in 0..count {
                length = (length << 8) | self.bytes[self.offset] as usize;
                self.offset += 1;
            }
        }
        if self.offset + length > self.bytes.len() {
            return Err(der_x509_error(self.label));
        }
        let body = &self.bytes[self.offset..self.offset + length];
        self.offset += length;
        Ok(DerElement { tag, body })
    }

    fn read_body(&mut self, expected: u8) -> Result<&'a [u8]> {
        Ok(self.read_element(Some(expected))?.body)
    }
}

fn der_x509_error(label: &str) -> ZuzuRustError {
    ZuzuRustError::thrown(format!("{label} expects DER X.509 certificate data"))
}

fn certificate_parse(args: &[Value]) -> Result<Value> {
    require_arity("parse", args, 1)?;
    let label = "Certificate.parse";
    match args.first() {
        Some(Value::BinaryString(bytes)) => certificate_object(parse_x509_der(bytes, label)?),
        Some(Value::String(pem)) => {
            let blocks = certificate_pem_blocks(pem, label)?;
            certificate_object(parse_x509_der(&blocks[0], label)?)
        }
        Some(other) => Err(ZuzuRustError::thrown(format!(
            "TypeException: {label} expects String pem or BinaryString der, got {}",
            other.type_name()
        ))),
        None => Err(ZuzuRustError::thrown(format!(
            "TypeException: {label} expects String pem or BinaryString der, got Null"
        ))),
    }
}

fn certificate_parse_chain(args: &[Value]) -> Result<Value> {
    require_arity("parse_chain", args, 1)?;
    let label = "Certificate.parse_chain";
    match args.first() {
        Some(Value::BinaryString(bytes)) => Ok(Value::Array(vec![certificate_object(
            parse_x509_der(bytes, label)?,
        )?])),
        Some(Value::String(pem)) => Ok(Value::Array(
            certificate_pem_blocks(pem, label)?
                .iter()
                .map(|der| certificate_object(parse_x509_der(der, label)?))
                .collect::<Result<Vec<_>>>()?,
        )),
        Some(other) => Err(ZuzuRustError::thrown(format!(
            "TypeException: {label} expects String pem or BinaryString der, got {}",
            other.type_name()
        ))),
        None => Err(ZuzuRustError::thrown(format!(
            "TypeException: {label} expects String pem or BinaryString der, got Null"
        ))),
    }
}

fn certificate_verify_chain(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(ZuzuRustError::runtime(
            "Certificate.verify_chain expects one or two arguments",
        ));
    }
    let label = "Certificate.verify_chain";
    let chain_values = certificate_chain_arg(args.first(), label)?;
    let options = optional_dict_arg(args.get(1), label, "Dict options")?;
    let roots_value = options.and_then(|map| map.get("roots"));
    let use_system_roots = match options.and_then(|map| map.get("use_system_roots")) {
        Some(Value::Boolean(value)) => *value,
        Some(other) => {
            return Err(ZuzuRustError::thrown(format!(
                "TypeException: {label} option 'use_system_roots' expects Boolean, got {}",
                other.type_name()
            )))
        }
        None => false,
    };
    let hostname = match options.and_then(|map| map.get("hostname")) {
        Some(Value::Null) | None => None,
        Some(Value::String(text)) => Some(text.clone()),
        Some(other) => {
            return Err(ZuzuRustError::thrown(format!(
                "TypeException: {label} option 'hostname' expects String or null, got {}",
                other.type_name()
            )))
        }
    };
    let verified_at = certificate_verify_time(options.and_then(|map| map.get("time")), label)?;

    let mut store_builder = X509StoreBuilder::new().map_err(|err| {
        ZuzuRustError::runtime(format!("{label} failed to initialize trust store: {err}"))
    })?;
    if use_system_roots {
        store_builder.set_default_paths().map_err(|err| {
            ZuzuRustError::runtime(format!("{label} failed to load system roots: {err}"))
        })?;
    }
    for root in certificate_root_x509s(roots_value, label)? {
        store_builder.add_cert(root).map_err(|err| {
            ZuzuRustError::runtime(format!("{label} failed to add trust root: {err}"))
        })?;
    }

    let mut verify_params = X509VerifyParam::new().map_err(|err| {
        ZuzuRustError::runtime(format!(
            "{label} failed to initialize verify parameters: {err}"
        ))
    })?;
    verify_params.set_time(verified_at as _);
    if let Some(hostname) = hostname.as_ref() {
        if let Ok(ip) = hostname.parse::<IpAddr>() {
            verify_params.set_ip(ip).map_err(|err| {
                ZuzuRustError::runtime(format!("{label} failed to set hostname: {err}"))
            })?;
        } else {
            verify_params.set_host(hostname).map_err(|err| {
                ZuzuRustError::runtime(format!("{label} failed to set hostname: {err}"))
            })?;
        }
    }
    store_builder.set_param(&verify_params).map_err(|err| {
        ZuzuRustError::runtime(format!("{label} failed to configure trust store: {err}"))
    })?;
    let store = store_builder.build();

    let leaf_der = certificate_der_arg(&chain_values[0], label)?;
    let leaf = X509::from_der(&leaf_der)
        .map_err(|_| ZuzuRustError::thrown(format!("{label} expects Certificate chain")))?;
    let mut intermediates = Stack::new().map_err(|err| {
        ZuzuRustError::runtime(format!("{label} failed to initialize chain: {err}"))
    })?;
    for cert in chain_values.iter().skip(1) {
        let der = certificate_der_arg(cert, label)?;
        intermediates
            .push(
                X509::from_der(&der).map_err(|_| {
                    ZuzuRustError::thrown(format!("{label} expects Certificate chain"))
                })?,
            )
            .map_err(|err| {
                ZuzuRustError::runtime(format!("{label} failed to initialize chain: {err}"))
            })?;
    }

    let mut context = X509StoreContext::new().map_err(|err| {
        ZuzuRustError::runtime(format!("{label} failed to initialize verification: {err}"))
    })?;
    let verify_result = context
        .init(&store, &leaf, &intermediates, |ctx| {
            let ok = ctx.verify_cert()?;
            Ok((ok, ctx.error().error_string()))
        })
        .map_err(|err| ZuzuRustError::runtime(format!("{label} failed: {err}")))?;
    let valid = verify_result.0;
    let error = if valid {
        None
    } else {
        Some(verify_result.1.to_owned())
    };
    let reason = if valid {
        "ok".to_owned()
    } else {
        certificate_verify_reason(error.as_deref().unwrap_or(""))
    };

    Ok(certificate_verify_result(
        valid,
        reason,
        error,
        hostname,
        verified_at,
        chain_values.len(),
    ))
}

fn certificate_pem_blocks(pem: &str, label: &str) -> Result<Vec<Vec<u8>>> {
    let mut blocks = Vec::new();
    let mut rest = pem;
    while let Some(start) = rest.find("-----BEGIN CERTIFICATE-----") {
        let after_start = &rest[start + "-----BEGIN CERTIFICATE-----".len()..];
        let Some(end) = after_start.find("-----END CERTIFICATE-----") else {
            return Err(ZuzuRustError::thrown(format!(
                "{label} expects PEM certificate text"
            )));
        };
        let body: String = after_start[..end]
            .chars()
            .filter(|ch| !ch.is_whitespace() && *ch != '=')
            .collect();
        let Some(der) = base64_decode(&body, BASE64) else {
            return Err(ZuzuRustError::thrown(format!(
                "{label} expects PEM certificate text"
            )));
        };
        blocks.push(der);
        rest = &after_start[end + "-----END CERTIFICATE-----".len()..];
    }
    if blocks.is_empty() {
        return Err(ZuzuRustError::thrown(format!(
            "{label} expects PEM certificate text"
        )));
    }
    Ok(blocks)
}

fn parse_x509_der(der: &[u8], label: &'static str) -> Result<CertificateState> {
    let mut cert = DerReader::new(der, label);
    let mut cert_seq = DerReader::new(cert.read_body(0x30)?, label);
    if !cert.is_empty() {
        return Err(der_x509_error(label));
    }
    let mut tbs = DerReader::new(cert_seq.read_body(0x30)?, label);
    if !tbs.is_empty() {
        let saved = tbs.offset;
        let element = tbs.read_element(None)?;
        if element.tag != 0xa0 {
            tbs.offset = saved;
        }
    }
    let serial = serial_hex(tbs.read_body(0x02)?);
    let _signature = tbs.read_body(0x30)?;
    let issuer = x509_name_string(tbs.read_body(0x30)?, label)?;
    let mut validity = DerReader::new(tbs.read_body(0x30)?, label);
    let not_before = x509_time_epoch(validity.read_element(None)?, label)?;
    let not_after = x509_time_epoch(validity.read_element(None)?, label)?;
    let subject = x509_name_string(tbs.read_body(0x30)?, label)?;
    let (public_algorithm, public_key) = x509_spki(tbs.read_body(0x30)?, label)?;

    Ok(CertificateState {
        der: der.to_vec(),
        subject,
        issuer,
        serial,
        not_before,
        not_after,
        public_algorithm,
        public_key,
    })
}

fn serial_hex(bytes: &[u8]) -> String {
    let mut value = bytes;
    while value.len() > 1 && value.first().copied() == Some(0) {
        value = &value[1..];
    }
    let text = value
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<String>();
    if text.is_empty() {
        "00".to_owned()
    } else {
        text
    }
}

fn der_oid(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return String::new();
    }
    let mut parts = vec![(bytes[0] / 40) as u32, (bytes[0] % 40) as u32];
    let mut value = 0u32;
    for byte in &bytes[1..] {
        value = (value << 7) | (byte & 0x7f) as u32;
        if byte & 0x80 == 0 {
            parts.push(value);
            value = 0;
        }
    }
    parts
        .iter()
        .map(|part| part.to_string())
        .collect::<Vec<_>>()
        .join(".")
}

fn oid_name(oid: &str) -> &str {
    match oid {
        "2.5.4.3" => "CN",
        "2.5.4.6" => "C",
        "2.5.4.7" => "L",
        "2.5.4.8" => "ST",
        "2.5.4.10" => "O",
        "2.5.4.11" => "OU",
        _ => oid,
    }
}

fn x509_text_value(tag: u8, body: &[u8]) -> String {
    match tag {
        0x0c | 0x13 | 0x14 | 0x16 => String::from_utf8_lossy(body).into_owned(),
        0x1e => body
            .chunks_exact(2)
            .filter_map(|pair| char::from_u32(((pair[0] as u32) << 8) | pair[1] as u32))
            .collect(),
        _ => serial_hex(body),
    }
}

fn x509_name_string(bytes: &[u8], label: &'static str) -> Result<String> {
    let mut seq = DerReader::new(bytes, label);
    let mut parts = Vec::new();
    while !seq.is_empty() {
        let mut set = DerReader::new(seq.read_body(0x31)?, label);
        while !set.is_empty() {
            let mut attr = DerReader::new(set.read_body(0x30)?, label);
            let oid = der_oid(attr.read_body(0x06)?);
            let value = attr.read_element(None)?;
            parts.push(format!(
                "{}={}",
                oid_name(&oid),
                x509_text_value(value.tag, value.body)
            ));
        }
    }
    Ok(parts.join(", "))
}

fn x509_time_epoch(element: DerElement<'_>, label: &str) -> Result<i64> {
    let text = std::str::from_utf8(element.body).map_err(|_| der_x509_error(label))?;
    let digits = text
        .strip_suffix('Z')
        .ok_or_else(|| der_x509_error(label))?;
    let (year, rest) = match element.tag {
        0x17 if digits.len() == 12 => {
            let yy = parse_i64(&digits[0..2], label)?;
            (if yy >= 50 { 1900 + yy } else { 2000 + yy }, &digits[2..])
        }
        0x18 if digits.len() == 14 => (parse_i64(&digits[0..4], label)?, &digits[4..]),
        _ => return Err(der_x509_error(label)),
    };
    let month = parse_i64(&rest[0..2], label)?;
    let day = parse_i64(&rest[2..4], label)?;
    let hour = parse_i64(&rest[4..6], label)?;
    let minute = parse_i64(&rest[6..8], label)?;
    let second = parse_i64(&rest[8..10], label)?;
    Ok(days_from_civil_local(year, month, day) * 86_400 + hour * 3600 + minute * 60 + second)
}

fn parse_i64(text: &str, label: &str) -> Result<i64> {
    text.parse::<i64>().map_err(|_| der_x509_error(label))
}

fn days_from_civil_local(year: i64, month: i64, day: i64) -> i64 {
    let year = year - if month <= 2 { 1 } else { 0 };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * month_prime + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn x509_spki(bytes: &[u8], label: &'static str) -> Result<(String, Vec<u8>)> {
    let mut seq = DerReader::new(bytes, label);
    let mut alg = DerReader::new(seq.read_body(0x30)?, label);
    let alg_oid = der_oid(alg.read_body(0x06)?);
    let curve_oid = if alg.is_empty() {
        String::new()
    } else {
        let param = alg.read_element(Some(0x06))?;
        der_oid(param.body)
    };
    let bit_string = seq.read_body(0x03)?;
    if bit_string.first().copied() != Some(0) {
        return Err(der_x509_error(label));
    }
    let public = bit_string[1..].to_vec();
    let algorithm = match (alg_oid.as_str(), curve_oid.as_str()) {
        ("1.3.101.112", _) => "ed25519",
        ("1.2.840.10045.2.1", "1.2.840.10045.3.1.7") => "ecdsa-p256-sha256",
        ("1.2.840.10045.2.1", "1.3.132.0.34") => "ecdsa-p384-sha384",
        _ => "unsupported",
    };
    Ok((algorithm.to_owned(), public))
}

fn certificate_state(builtin_value: &Value, label: &str) -> Result<CertificateState> {
    let Value::Dict(fields) = builtin_value else {
        return Err(ZuzuRustError::runtime("Certificate internal state missing"));
    };
    let der = binary_arg(fields.get("der"), label, "BinaryString der")?.to_vec();
    let subject = string_arg(fields.get("subject"), label, "String subject")?.to_owned();
    let issuer = string_arg(fields.get("issuer"), label, "String issuer")?.to_owned();
    let serial = string_arg(fields.get("serial"), label, "String serial")?.to_owned();
    let not_before = number_i64(fields.get("not_before"), label)?;
    let not_after = number_i64(fields.get("not_after"), label)?;
    let public_algorithm = string_arg(
        fields.get("public_algorithm"),
        label,
        "String public_algorithm",
    )?
    .to_owned();
    let public_key =
        binary_arg(fields.get("public_key"), label, "BinaryString public_key")?.to_vec();
    Ok(CertificateState {
        der,
        subject,
        issuer,
        serial,
        not_before,
        not_after,
        public_algorithm,
        public_key,
    })
}

fn number_i64(value: Option<&Value>, label: &str) -> Result<i64> {
    match value {
        Some(Value::Number(number)) => Ok(*number as i64),
        _ => Err(ZuzuRustError::runtime(format!(
            "{label} internal number missing"
        ))),
    }
}

fn certificate_object(state: CertificateState) -> Result<Value> {
    Ok(native_object(
        "Certificate",
        HashMap::from([
            ("der".to_owned(), Value::BinaryString(state.der)),
            ("subject".to_owned(), Value::String(state.subject)),
            ("issuer".to_owned(), Value::String(state.issuer)),
            ("serial".to_owned(), Value::String(state.serial)),
            (
                "not_before".to_owned(),
                Value::Number(state.not_before as f64),
            ),
            (
                "not_after".to_owned(),
                Value::Number(state.not_after as f64),
            ),
            (
                "public_algorithm".to_owned(),
                Value::String(state.public_algorithm),
            ),
            (
                "public_key".to_owned(),
                Value::BinaryString(state.public_key),
            ),
        ]),
    ))
}

fn certificate_chain_arg(value: Option<&Value>, label: &str) -> Result<Vec<Value>> {
    let Some(Value::Array(items)) = value else {
        return Err(ZuzuRustError::thrown(format!(
            "TypeException: {label} expects Array chain"
        )));
    };
    if items.is_empty() {
        return Err(ZuzuRustError::thrown(format!(
            "{label} expects a non-empty certificate chain"
        )));
    }
    for item in items {
        certificate_der_arg(item, label)?;
    }
    Ok(items.clone())
}

fn certificate_der_arg(value: &Value, label: &str) -> Result<Vec<u8>> {
    let Value::Object(object) = value else {
        return Err(ZuzuRustError::thrown(format!(
            "TypeException: {label} expects Certificate"
        )));
    };
    let borrowed = object.borrow();
    if borrowed.class.name != "Certificate" {
        return Err(ZuzuRustError::thrown(format!(
            "TypeException: {label} expects Certificate"
        )));
    }
    let Some(Value::Dict(fields)) = borrowed.builtin_value.as_ref() else {
        return Err(ZuzuRustError::runtime("Certificate internal state missing"));
    };
    Ok(binary_arg(fields.get("der"), label, "BinaryString der")?.to_vec())
}

fn certificate_root_x509s(value: Option<&Value>, label: &str) -> Result<Vec<X509>> {
    match value {
        Some(Value::Null) | None => Ok(Vec::new()),
        Some(cert @ Value::Object(_)) => Ok(vec![X509::from_der(&certificate_der_arg(
            cert, label,
        )?)
        .map_err(|_| ZuzuRustError::thrown(format!("{label} expects Certificate roots")))?]),
        Some(Value::String(pem)) => X509::stack_from_pem(pem.as_bytes())
            .map_err(|_| ZuzuRustError::thrown(format!("{label} expects PEM certificate roots"))),
        Some(Value::Array(items)) => {
            let mut roots = Vec::new();
            for item in items {
                roots.extend(certificate_root_x509s(Some(item), label)?);
            }
            Ok(roots)
        }
        Some(other) => Err(ZuzuRustError::thrown(format!(
            "TypeException: {label} expects roots to be Certificate, String PEM, Array, or null, got {}",
            other.type_name()
        ))),
    }
}

fn certificate_verify_time(value: Option<&Value>, label: &str) -> Result<i64> {
    match value {
        Some(Value::Null) | None => Ok(SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs() as i64)
            .unwrap_or(0)),
        Some(Value::Number(number)) => Ok(*number as i64),
        Some(Value::Object(object)) => {
            let borrowed = object.borrow();
            if borrowed.class.name == "Time" {
                if let Some(Value::Number(epoch)) = borrowed.builtin_value.as_ref() {
                    return Ok(*epoch as i64);
                }
            }
            Err(ZuzuRustError::thrown(format!(
                "TypeException: {label} option 'time' expects Time, Number, or null"
            )))
        }
        Some(other) => Err(ZuzuRustError::thrown(format!(
            "TypeException: {label} option 'time' expects Time, Number, or null, got {}",
            other.type_name()
        ))),
    }
}

fn certificate_verify_reason(error: &str) -> String {
    let lower = error.to_ascii_lowercase();
    if lower.is_empty() || lower == "ok" {
        "ok"
    } else if lower.contains("hostname") {
        "hostname-mismatch"
    } else if lower.contains("not yet valid") {
        "not-yet-valid"
    } else if lower.contains("expired") {
        "expired"
    } else if lower.contains("unable to get")
        || lower.contains("self-signed")
        || lower.contains("unable to verify")
        || lower.contains("issuer certificate")
    {
        "untrusted-root"
    } else {
        "invalid-chain"
    }
    .to_owned()
}

fn certificate_verify_result(
    valid: bool,
    reason: String,
    error: Option<String>,
    hostname: Option<String>,
    verified_at: i64,
    chain_length: usize,
) -> Value {
    Value::Dict(HashMap::from([
        ("valid".to_owned(), Value::Boolean(valid)),
        ("reason".to_owned(), Value::String(reason)),
        (
            "error".to_owned(),
            error.map(Value::String).unwrap_or(Value::Null),
        ),
        (
            "hostname".to_owned(),
            hostname.map(Value::String).unwrap_or(Value::Null),
        ),
        ("verified_at".to_owned(), Value::Number(verified_at as f64)),
        (
            "chain_length".to_owned(),
            Value::Number(chain_length as f64),
        ),
    ]))
}

fn certificate_public_key(state: CertificateState) -> Result<Value> {
    match state.public_algorithm.as_str() {
        "ed25519" | "ecdsa-p256-sha256" | "ecdsa-p384-sha384" => {
            Ok(public_key_object(&state.public_algorithm, state.public_key))
        }
        _ => Err(ZuzuRustError::thrown(
            "Certificate.public_key certificate public-key algorithm is unsupported",
        )),
    }
}

fn certificate_der_to_pem(der: &[u8]) -> String {
    let encoded = base64_padded(der);
    let mut pem = String::from("-----BEGIN CERTIFICATE-----\n");
    for chunk in encoded.as_bytes().chunks(64) {
        pem.push_str(std::str::from_utf8(chunk).unwrap_or(""));
        pem.push('\n');
    }
    pem.push_str("-----END CERTIFICATE-----\n");
    pem
}

struct TlsIdentityState {
    cert_pem: String,
    key_pem: String,
    password: String,
}

fn tls_identity_from_pem(args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(ZuzuRustError::runtime(
            "TlsIdentity.from_pem expects two or three arguments",
        ));
    }
    let label = "TlsIdentity.from_pem";
    let cert_pem = string_arg(args.first(), label, "String certificate_pem")?;
    let key_pem = string_arg(args.get(1), label, "String private_key_pem")?;
    let password = password_arg(args.get(2), label)?;
    let blocks = certificate_pem_blocks(cert_pem, label)?;
    parse_x509_der(&blocks[0], label)?;
    if !key_pem.contains("-----BEGIN ") || !key_pem.contains("PRIVATE KEY-----") {
        return Err(ZuzuRustError::thrown(format!(
            "{label} expects PEM private key text"
        )));
    }
    let cert_pem = certificate_der_to_pem(&blocks[0]);
    let chain_pem = blocks
        .iter()
        .map(|der| certificate_der_to_pem(der))
        .collect::<String>();
    Ok(tls_identity_object(
        cert_pem,
        key_pem.to_owned(),
        password,
        chain_pem,
        "pem",
    ))
}

fn tls_identity_from_pkcs12(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(ZuzuRustError::runtime(
            "TlsIdentity.from_pkcs12 expects one or two arguments",
        ));
    }
    let label = "TlsIdentity.from_pkcs12";
    let bytes = binary_arg(args.first(), label, "BinaryString bytes")?;
    let password = password_arg(args.get(1), label)?;
    let pkcs12 = Pkcs12::from_der(bytes)
        .map_err(|_| ZuzuRustError::thrown(format!("{label} expects PKCS#12 data")))?;
    let parsed = pkcs12
        .parse2(&password)
        .map_err(|_| ZuzuRustError::thrown(format!("{label} failed to decrypt PKCS#12 data")))?;
    let cert = parsed.cert.ok_or_else(|| {
        ZuzuRustError::thrown(format!(
            "{label} expects PKCS#12 data with certificate and private key"
        ))
    })?;
    let pkey = parsed.pkey.ok_or_else(|| {
        ZuzuRustError::thrown(format!(
            "{label} expects PKCS#12 data with certificate and private key"
        ))
    })?;
    let cert_pem = String::from_utf8(
        cert.to_pem()
            .map_err(|_| ZuzuRustError::thrown(format!("{label} failed to read certificate")))?,
    )
    .map_err(|_| ZuzuRustError::thrown(format!("{label} failed to read certificate")))?;
    let key_pem = String::from_utf8(
        pkey.private_key_to_pem_pkcs8()
            .map_err(|_| ZuzuRustError::thrown(format!("{label} failed to read private key")))?,
    )
    .map_err(|_| ZuzuRustError::thrown(format!("{label} failed to read private key")))?;
    let mut chain_pem = cert_pem.clone();
    if let Some(ca) = parsed.ca {
        for cert in ca {
            let pem = String::from_utf8(cert.to_pem().map_err(|_| {
                ZuzuRustError::thrown(format!("{label} failed to read certificate chain"))
            })?)
            .map_err(|_| {
                ZuzuRustError::thrown(format!("{label} failed to read certificate chain"))
            })?;
            chain_pem.push_str(&pem);
        }
    }
    Ok(tls_identity_object(
        cert_pem,
        key_pem,
        String::new(),
        chain_pem,
        "pkcs12",
    ))
}

fn tls_identity_certificate(builtin_value: &Value) -> Result<Value> {
    let state = tls_identity_state(builtin_value, "TlsIdentity.certificate")?;
    let blocks = certificate_pem_blocks(&state.cert_pem, "TlsIdentity.certificate")?;
    certificate_object(parse_x509_der(&blocks[0], "TlsIdentity.certificate")?)
}

fn tls_identity_private_key(builtin_value: &Value) -> Result<Value> {
    let state = tls_identity_state(builtin_value, "TlsIdentity.private_key")?;
    match infer_private_key_from_pem_with_password(
        &state.key_pem,
        &state.password,
        "TlsIdentity.private_key",
    ) {
        Ok((algorithm, private, public)) => Ok(signing_key_object(algorithm, private, public)),
        Err(_) => Err(ZuzuRustError::thrown(
            "TlsIdentity.private_key only supports Ed25519, ECDSA P-256, and ECDSA P-384 private keys",
        )),
    }
}

fn tls_identity_state(builtin_value: &Value, label: &str) -> Result<TlsIdentityState> {
    let Value::Dict(fields) = builtin_value else {
        return Err(ZuzuRustError::runtime("TlsIdentity internal state missing"));
    };
    Ok(TlsIdentityState {
        cert_pem: string_arg(fields.get("cert_pem"), label, "String cert_pem")?.to_owned(),
        key_pem: string_arg(fields.get("key_pem"), label, "String key_pem")?.to_owned(),
        password: string_arg(fields.get("password"), label, "String password")?.to_owned(),
    })
}

fn tls_identity_object(
    cert_pem: String,
    key_pem: String,
    password: String,
    chain_pem: String,
    source: &str,
) -> Value {
    native_object(
        "TlsIdentity",
        HashMap::from([
            ("cert_pem".to_owned(), Value::String(cert_pem)),
            ("key_pem".to_owned(), Value::String(key_pem)),
            ("password".to_owned(), Value::String(password)),
            ("chain_pem".to_owned(), Value::String(chain_pem)),
            ("source".to_owned(), Value::String(source.to_owned())),
        ]),
    )
}

fn password_arg(value: Option<&Value>, label: &str) -> Result<String> {
    match value {
        Some(Value::Null) | None => Ok(String::new()),
        Some(Value::String(text)) => Ok(text.clone()),
        Some(other) => Err(ZuzuRustError::thrown(format!(
            "TypeException: {label} expects String password, got {}",
            other.type_name()
        ))),
    }
}

fn base64_padded(bytes: &[u8]) -> String {
    let mut out = base64_encode(bytes, BASE64);
    while out.len() % 4 != 0 {
        out.push('=');
    }
    out
}

fn private_key_from_pem(
    algorithm: &'static str,
    pem: &str,
    label: &str,
) -> Result<(&'static str, Vec<u8>, Vec<u8>)> {
    match algorithm {
        "ed25519" => {
            let signing = Ed25519SigningKey::from_pkcs8_pem(pem)
                .map_err(|_| ZuzuRustError::thrown(format!("{label} expects PEM private key")))?;
            Ok((
                "ed25519",
                signing.to_bytes().to_vec(),
                signing.verifying_key().to_bytes().to_vec(),
            ))
        }
        "ecdsa-p256-sha256" => {
            let signing = P256SigningKey::from_pkcs8_pem(pem)
                .map_err(|_| ZuzuRustError::thrown(format!("{label} expects PEM private key")))?;
            Ok((
                "ecdsa-p256-sha256",
                signing.to_bytes().to_vec(),
                signing.verifying_key().to_sec1_bytes().to_vec(),
            ))
        }
        "ecdsa-p384-sha384" => {
            let signing = P384SigningKey::from_pkcs8_pem(pem)
                .map_err(|_| ZuzuRustError::thrown(format!("{label} expects PEM private key")))?;
            Ok((
                "ecdsa-p384-sha384",
                signing.to_bytes().to_vec(),
                signing.verifying_key().to_sec1_bytes().to_vec(),
            ))
        }
        _ => Err(ZuzuRustError::runtime("unreachable signing algorithm")),
    }
}

fn infer_private_key_from_pem(pem: &str, label: &str) -> Result<(&'static str, Vec<u8>, Vec<u8>)> {
    for algorithm in ["ed25519", "ecdsa-p256-sha256", "ecdsa-p384-sha384"] {
        if let Ok(key) = private_key_from_pem(algorithm, pem, label) {
            return Ok(key);
        }
    }
    Err(ZuzuRustError::thrown(format!(
        "{label} expects an Ed25519, P-256, or P-384 PEM private key"
    )))
}

fn infer_private_key_from_pem_with_password(
    pem: &str,
    password: &str,
    label: &str,
) -> Result<(&'static str, Vec<u8>, Vec<u8>)> {
    if password.is_empty() {
        if let Ok(key) = infer_private_key_from_pem(pem, label) {
            return Ok(key);
        }
    }
    let normalized = normalize_private_key_pem(pem, password, label)?;
    infer_private_key_from_pem(&normalized, label)
}

fn normalize_private_key_pem(pem: &str, password: &str, label: &str) -> Result<String> {
    let pkey = if password.is_empty() {
        openssl::pkey::PKey::private_key_from_pem(pem.as_bytes())
    } else {
        openssl::pkey::PKey::private_key_from_pem_passphrase(pem.as_bytes(), password.as_bytes())
    }
    .map_err(|_| ZuzuRustError::thrown(format!("{label} expects PEM private key")))?;
    String::from_utf8(
        pkey.private_key_to_pem_pkcs8()
            .map_err(|_| ZuzuRustError::thrown(format!("{label} expects PEM private key")))?,
    )
    .map_err(|_| ZuzuRustError::thrown(format!("{label} expects PEM private key")))
}

fn public_key_from_pem(
    algorithm: &'static str,
    pem: &str,
    label: &str,
) -> Result<(&'static str, Vec<u8>)> {
    match algorithm {
        "ed25519" => {
            let verifying = VerifyingKey::from_public_key_pem(pem)
                .map_err(|_| ZuzuRustError::thrown(format!("{label} expects PEM public key")))?;
            Ok(("ed25519", verifying.to_bytes().to_vec()))
        }
        "ecdsa-p256-sha256" => {
            let verifying = P256VerifyingKey::from_public_key_pem(pem)
                .map_err(|_| ZuzuRustError::thrown(format!("{label} expects PEM public key")))?;
            Ok(("ecdsa-p256-sha256", verifying.to_sec1_bytes().to_vec()))
        }
        "ecdsa-p384-sha384" => {
            let verifying = P384VerifyingKey::from_public_key_pem(pem)
                .map_err(|_| ZuzuRustError::thrown(format!("{label} expects PEM public key")))?;
            Ok(("ecdsa-p384-sha384", verifying.to_sec1_bytes().to_vec()))
        }
        _ => Err(ZuzuRustError::runtime("unreachable signing algorithm")),
    }
}

fn infer_public_key_from_pem(pem: &str, label: &str) -> Result<(&'static str, Vec<u8>)> {
    for algorithm in ["ed25519", "ecdsa-p256-sha256", "ecdsa-p384-sha384"] {
        if let Ok(key) = public_key_from_pem(algorithm, pem, label) {
            return Ok(key);
        }
    }
    Err(ZuzuRustError::thrown(format!(
        "{label} expects an Ed25519, P-256, or P-384 PEM public key"
    )))
}

fn algorithm_from_raw_public(bytes: &[u8], label: &str) -> Result<&'static str> {
    match (bytes.len(), bytes.first().copied()) {
        (32, _) => Ok("ed25519"),
        (65, Some(0x04)) => Ok("ecdsa-p256-sha256"),
        (97, Some(0x04)) => Ok("ecdsa-p384-sha384"),
        _ => Err(ZuzuRustError::thrown(format!(
			"{label} expects a 32-byte Ed25519 key, 65-byte P-256 public key, or 97-byte P-384 public key"
		))),
    }
}

fn validate_raw_public_length(bytes: &[u8], algorithm: &str, label: &str) -> Result<()> {
    let expected = match algorithm {
        "ed25519" => 32,
        "ecdsa-p256-sha256" => 65,
        "ecdsa-p384-sha384" => 97,
        _ => return Err(ZuzuRustError::runtime("unreachable signing algorithm")),
    };
    if bytes.len() != expected {
        return Err(ZuzuRustError::thrown(format!(
            "{label} expects a {expected}-byte raw public key"
        )));
    }
    if algorithm != "ed25519" && bytes.first().copied() != Some(0x04) {
        return Err(ZuzuRustError::thrown(format!(
            "{label} expects an uncompressed EC public key"
        )));
    }
    Ok(())
}

fn x25519_private_bytes(bytes: &[u8], label: &str) -> Result<[u8; 32]> {
    bytes
        .try_into()
        .map_err(|_| ZuzuRustError::thrown(format!("{label} expects a 32-byte raw private key")))
}

fn x25519_public_bytes(bytes: &[u8], label: &str) -> Result<[u8; 32]> {
    bytes
        .try_into()
        .map_err(|_| ZuzuRustError::thrown(format!("{label} expects a 32-byte raw public key")))
}

fn signing_algorithm(value: Option<&Value>, label: &str) -> Result<&'static str> {
    match value {
        Some(Value::Null) | None => Ok("ed25519"),
        Some(Value::String(algorithm)) if algorithm == "ed25519" => Ok("ed25519"),
        Some(Value::String(algorithm)) if algorithm == "ecdsa-p256-sha256" => {
            Ok("ecdsa-p256-sha256")
        }
        Some(Value::String(algorithm)) if algorithm == "ecdsa-p384-sha384" => {
            Ok("ecdsa-p384-sha384")
        }
        _ => Err(ZuzuRustError::thrown(format!(
            "{label} only supports ed25519, ecdsa-p256-sha256, and ecdsa-p384-sha384"
        ))),
    }
}

fn signing_algorithm_option(
    options: Option<&HashMap<String, Value>>,
    label: &str,
) -> Result<Option<&'static str>> {
    match options.and_then(|map| map.get("algorithm")) {
        Some(value) => Ok(Some(signing_algorithm(Some(value), label)?)),
        None => Ok(None),
    }
}

fn key_agreement_algorithm(value: Option<&Value>, label: &str) -> Result<&'static str> {
    match value {
        Some(Value::Null) | None => Ok("x25519"),
        Some(Value::String(algorithm)) if algorithm == "x25519" => Ok("x25519"),
        _ => Err(ZuzuRustError::thrown(format!(
            "{label} only supports x25519"
        ))),
    }
}

fn key_agreement_algorithm_option(
    options: Option<&HashMap<String, Value>>,
    label: &str,
) -> Result<Option<&'static str>> {
    match options.and_then(|map| map.get("algorithm")) {
        Some(value) => Ok(Some(key_agreement_algorithm(Some(value), label)?)),
        None => Ok(None),
    }
}

fn key_format<'a>(key: Option<&Value>, options: Option<&'a Value>, label: &str) -> Result<&'a str> {
    let options = optional_dict_arg(options, label, "Dict options")?;
    if let Some(map) = options {
        if let Some(Value::String(format)) = map.get("format") {
            return Ok(match format.as_str() {
                "raw" => "raw",
                "pem" => "pem",
                _ => "unsupported",
            });
        }
    }
    Ok(if matches!(key, Some(Value::BinaryString(_))) {
        "raw"
    } else {
        "pem"
    })
}

fn export_format(value: Option<&Value>, label: &str) -> Result<&'static str> {
    let options = optional_dict_arg(value, label, "Dict options")?;
    match options.and_then(|map| map.get("format")) {
        Some(Value::String(format)) if format == "raw" => Ok("raw"),
        Some(Value::String(format)) if format == "pem" => Ok("pem"),
        Some(_) => Ok("unsupported"),
        None => Ok("raw"),
    }
}

fn signing_key_object(algorithm: &str, private: Vec<u8>, public: Vec<u8>) -> Value {
    native_object(
        "SigningKey",
        HashMap::from([
            ("kind".to_owned(), Value::String("private".to_owned())),
            ("algorithm".to_owned(), Value::String(algorithm.to_owned())),
            ("private".to_owned(), Value::BinaryString(private)),
            ("public".to_owned(), Value::BinaryString(public)),
        ]),
    )
}

fn key_agreement_key_object_from_private(private: [u8; 32]) -> Value {
    let secret = X25519StaticSecret::from(private);
    let public = X25519PublicKey::from(&secret);
    native_object(
        "KeyAgreement",
        HashMap::from([
            ("kind".to_owned(), Value::String("private".to_owned())),
            ("algorithm".to_owned(), Value::String("x25519".to_owned())),
            ("private".to_owned(), Value::BinaryString(private.to_vec())),
            (
                "public".to_owned(),
                Value::BinaryString(public.as_bytes().to_vec()),
            ),
        ]),
    )
}

fn public_key_object(algorithm: &str, public: Vec<u8>) -> Value {
    native_object(
        "PublicKey",
        HashMap::from([
            ("kind".to_owned(), Value::String("public".to_owned())),
            ("algorithm".to_owned(), Value::String(algorithm.to_owned())),
            ("public".to_owned(), Value::BinaryString(public)),
        ]),
    )
}

fn signing_key_state<'a>(
    builtin_value: &'a Value,
    label: &str,
) -> Result<(&'a str, &'a [u8], &'a [u8])> {
    let Value::Dict(fields) = builtin_value else {
        return Err(ZuzuRustError::runtime("SigningKey internal state missing"));
    };
    let algorithm = string_arg(fields.get("algorithm"), label, "String algorithm")?;
    let private = binary_arg(fields.get("private"), label, "BinaryString private key")?;
    let public = binary_arg(fields.get("public"), label, "BinaryString public key")?;
    Ok((algorithm, private, public))
}

fn key_agreement_state<'a>(
    builtin_value: &'a Value,
    label: &str,
) -> Result<(&'a str, &'a [u8], &'a [u8])> {
    let Value::Dict(fields) = builtin_value else {
        return Err(ZuzuRustError::runtime(
            "KeyAgreement internal state missing",
        ));
    };
    let algorithm = string_arg(fields.get("algorithm"), label, "String algorithm")?;
    if algorithm != "x25519" {
        return Err(ZuzuRustError::runtime(
            "KeyAgreement internal algorithm invalid",
        ));
    }
    let private = binary_arg(fields.get("private"), label, "BinaryString private key")?;
    let public = binary_arg(fields.get("public"), label, "BinaryString public key")?;
    Ok((algorithm, private, public))
}

fn public_key_state<'a>(builtin_value: &'a Value, label: &str) -> Result<(&'a str, &'a [u8])> {
    let Value::Dict(fields) = builtin_value else {
        return Err(ZuzuRustError::runtime("PublicKey internal state missing"));
    };
    let algorithm = match fields.get("algorithm") {
        Some(Value::String(text)) => match text.as_str() {
            "ed25519" => "ed25519",
            "ecdsa-p256-sha256" => "ecdsa-p256-sha256",
            "ecdsa-p384-sha384" => "ecdsa-p384-sha384",
            "x25519" => "x25519",
            _ => {
                return Err(ZuzuRustError::runtime(
                    "PublicKey internal algorithm invalid",
                ))
            }
        },
        _ => return Err(ZuzuRustError::runtime("PublicKey internal state missing")),
    };
    let public = binary_arg(fields.get("public"), label, "BinaryString public key")?;
    Ok((algorithm, public))
}

fn public_key_argument(value: Option<&Value>, label: &str) -> Result<(&'static str, Vec<u8>)> {
    let Some(Value::Object(object)) = value else {
        return Err(ZuzuRustError::thrown(format!(
            "TypeException: {label} expects PublicKey"
        )));
    };
    let borrowed = object.borrow();
    if borrowed.class.name != "PublicKey" {
        return Err(ZuzuRustError::thrown(format!(
            "TypeException: {label} expects PublicKey"
        )));
    }
    let Some(Value::Dict(fields)) = borrowed.builtin_value.as_ref() else {
        return Err(ZuzuRustError::runtime("PublicKey internal state missing"));
    };
    let algorithm = match fields.get("algorithm") {
        Some(Value::String(text)) => match text.as_str() {
            "ed25519" => "ed25519",
            "ecdsa-p256-sha256" => "ecdsa-p256-sha256",
            "ecdsa-p384-sha384" => "ecdsa-p384-sha384",
            "x25519" => "x25519",
            _ => {
                return Err(ZuzuRustError::runtime(
                    "PublicKey internal algorithm invalid",
                ))
            }
        },
        _ => return Err(ZuzuRustError::runtime("PublicKey internal state missing")),
    };
    let public = match fields.get("public") {
        Some(Value::BinaryString(bytes)) => bytes.clone(),
        _ => return Err(ZuzuRustError::runtime("PublicKey internal state missing")),
    };
    Ok((algorithm, public))
}

fn native_object(class_name: &str, builtin_value: HashMap<String, Value>) -> Value {
    Value::Object(Rc::new(RefCell::new(ObjectValue {
        class: class_named(class_name),
        fields: HashMap::new(),
        weak_fields: HashSet::new(),
        builtin_value: Some(Value::Dict(builtin_value)),
    })))
}

fn class_named(name: &str) -> Rc<UserClassValue> {
    Rc::new(UserClassValue {
        name: name.to_owned(),
        base: None::<ClassBase>,
        traits: Vec::<Rc<TraitValue>>::new(),
        fields: Vec::<FieldSpec>::new(),
        methods: HashMap::<String, Rc<MethodValue>>::new(),
        static_methods: HashMap::<String, Rc<MethodValue>>::new(),
        nested_classes: HashMap::new(),
        source_decl: None,
        closure_env: None,
    })
}

fn cipher_options<'a>(value: Option<&'a Value>, label: &str) -> Result<CipherOptions<'a>> {
    let options = optional_dict_arg(value, label, "Dict options")?;
    let algorithm_value = options.and_then(|map| map.get("algorithm"));
    let algorithm = cipher_algorithm(algorithm_value, label)?;
    let algorithm_supplied = !matches!(algorithm_value, Some(Value::Null) | None);
    let aad = optional_binary_arg(
        options.and_then(|map| map.get("aad")),
        label,
        "BinaryString aad",
    )?;
    Ok(CipherOptions {
        algorithm,
        algorithm_supplied,
        aad,
    })
}

fn cipher_algorithm(value: Option<&Value>, label: &str) -> Result<&'static str> {
    match value {
        Some(Value::Null) | None => Ok("aes-256-gcm"),
        Some(Value::String(algorithm)) if algorithm == "aes-256-gcm" => Ok("aes-256-gcm"),
        Some(Value::String(algorithm)) if algorithm == "chacha20-poly1305" => {
            Ok("chacha20-poly1305")
        }
        Some(Value::String(algorithm)) => Err(ZuzuRustError::thrown(format!(
            "{label} cipher algorithm '{algorithm}' is not available"
        ))),
        _ => Err(ZuzuRustError::thrown(format!(
            "{label} only supports aes-256-gcm and chacha20-poly1305"
        ))),
    }
}

fn cipher_meta(algorithm: &'static str, _label: &str) -> Result<&'static CipherMeta> {
    static AES_256_GCM: CipherMeta = CipherMeta {
        key_length: CIPHER_KEY_LENGTH,
        nonce_length: CIPHER_NONCE_LENGTH,
        tag_length: CIPHER_TAG_LENGTH,
    };
    static CHACHA20_POLY1305: CipherMeta = CipherMeta {
        key_length: CIPHER_KEY_LENGTH,
        nonce_length: CIPHER_NONCE_LENGTH,
        tag_length: CIPHER_TAG_LENGTH,
    };
    match algorithm {
        "aes-256-gcm" => Ok(&AES_256_GCM),
        "chacha20-poly1305" => Ok(&CHACHA20_POLY1305),
        _ => Err(ZuzuRustError::runtime("Cipher internal algorithm invalid")),
    }
}

fn cipher_key<'a>(value: Option<&'a Value>, label: &str, meta: &CipherMeta) -> Result<&'a [u8]> {
    let key = binary_arg(value, label, "BinaryString key")?;
    if key.len() != meta.key_length {
        return Err(ZuzuRustError::thrown(format!(
            "{label} expects a {}-byte key",
            meta.key_length
        )));
    }
    Ok(key)
}

fn optional_dict_arg<'a>(
    value: Option<&'a Value>,
    label: &str,
    arg_name: &str,
) -> Result<Option<&'a HashMap<String, Value>>> {
    match value {
        Some(Value::Null) | None => Ok(None),
        Some(Value::Dict(map)) => Ok(Some(map)),
        Some(other) => Err(ZuzuRustError::thrown(format!(
            "TypeException: {label} expects {arg_name}, got {}",
            other.type_name()
        ))),
    }
}

fn cipher_envelope(value: Option<&Value>, label: &str) -> Result<CipherEnvelope> {
    let Some(map) = optional_dict_arg(value, label, "Dict envelope")? else {
        return Err(ZuzuRustError::thrown(format!(
            "TypeException: {label} expects Dict envelope, got Null"
        )));
    };
    match map.get("version") {
        Some(Value::Number(version)) if *version == 1.0 => {}
        _ => {
            return Err(ZuzuRustError::thrown(format!(
                "{label} expects envelope.version 1"
            )));
        }
    }
    let algorithm = cipher_algorithm(map.get("algorithm"), label)?;
    let meta = cipher_meta(algorithm, label)?;

    Ok(CipherEnvelope {
        algorithm,
        nonce: envelope_bytes(map, "nonce", label, Some(meta.nonce_length))?.to_vec(),
        ciphertext: envelope_bytes(map, "ciphertext", label, None)?.to_vec(),
        tag: envelope_bytes(map, "tag", label, Some(meta.tag_length))?.to_vec(),
    })
}

fn envelope_bytes<'a>(
    map: &'a HashMap<String, Value>,
    field: &str,
    label: &str,
    length: Option<usize>,
) -> Result<&'a [u8]> {
    let bytes = binary_arg(
        map.get(field),
        label,
        &format!("BinaryString envelope.{field}"),
    )?;
    if let Some(length) = length {
        if bytes.len() != length {
            return Err(ZuzuRustError::thrown(format!(
                "{label} expects envelope.{field} to be {length} bytes"
            )));
        }
    }
    Ok(bytes)
}

fn cipher_envelope_value(
    algorithm: &'static str,
    nonce: Vec<u8>,
    ciphertext: Vec<u8>,
    tag: Vec<u8>,
) -> Value {
    Value::Dict(HashMap::from([
        ("version".to_owned(), Value::Number(1.0)),
        ("algorithm".to_owned(), Value::String(algorithm.to_owned())),
        ("nonce".to_owned(), Value::BinaryString(nonce)),
        ("ciphertext".to_owned(), Value::BinaryString(ciphertext)),
        ("tag".to_owned(), Value::BinaryString(tag)),
    ]))
}

fn string_arg<'a>(value: Option<&'a Value>, label: &str, arg_name: &str) -> Result<&'a str> {
    match value {
        Some(Value::String(text)) => Ok(text),
        Some(other) => Err(ZuzuRustError::thrown(format!(
            "TypeException: {label} expects {arg_name}, got {}",
            other.type_name()
        ))),
        None => Err(ZuzuRustError::thrown(format!(
            "TypeException: {label} expects {arg_name}, got Null"
        ))),
    }
}

fn string_bytes_arg(value: Option<&Value>, label: &str, arg_name: &str) -> Result<Vec<u8>> {
    Ok(string_arg(value, label, arg_name)?.as_bytes().to_vec())
}

fn option_string<'a>(
    options: Option<&'a HashMap<String, Value>>,
    key: &str,
    default: &'static str,
) -> Result<&'a str> {
    match options.and_then(|map| map.get(key)) {
        Some(Value::String(text)) => Ok(text),
        Some(Value::Null) | None => Ok(default),
        Some(other) => Err(ZuzuRustError::thrown(format!(
            "TypeException: option '{key}' expects String, got {}",
            other.type_name()
        ))),
    }
}

fn option_positive_integer(
    options: Option<&HashMap<String, Value>>,
    key: &str,
    default: usize,
    label: &str,
) -> Result<usize> {
    match options.and_then(|map| map.get(key)) {
        Some(value) => non_negative_integer(value, &format!("{label} option '{key}'")),
        None => Ok(default),
    }
    .and_then(|value| {
        if value == 0 {
            Err(ZuzuRustError::thrown(format!(
                "{label} option '{key}' expects a positive integer"
            )))
        } else {
            Ok(value)
        }
    })
}

fn password_hash_algorithm(
    options: Option<&HashMap<String, Value>>,
    label: &str,
) -> Result<&'static str> {
    let algorithm = option_string(options, "algorithm", DEFAULT_PASSWORD_HASH_ALGORITHM)?;
    match algorithm {
        "argon2id" | "pbkdf2-sha256" | "scrypt" => Ok(match algorithm {
            "argon2id" => "argon2id",
            "pbkdf2-sha256" => "pbkdf2-sha256",
            _ => "scrypt",
        }),
        _ => Err(ZuzuRustError::thrown(format!(
            "{label} password hash algorithm '{algorithm}' is not available"
        ))),
    }
}

fn pbkdf2_options(options: Option<&HashMap<String, Value>>, label: &str) -> Result<(usize, usize)> {
    Ok((
        option_positive_integer(options, "iterations", PBKDF2_SHA256_ITERATIONS, label)?,
        option_positive_integer(options, "length", PASSWORD_HASH_LENGTH, label)?,
    ))
}

fn argon2id_options(
    options: Option<&HashMap<String, Value>>,
    label: &str,
) -> Result<(usize, usize, usize, usize)> {
    Ok((
        option_positive_integer(options, "memory", ARGON2ID_MEMORY, label)?,
        option_positive_integer(options, "iterations", ARGON2ID_ITERATIONS, label)?,
        option_positive_integer(options, "parallelism", ARGON2ID_PARALLELISM, label)?,
        option_positive_integer(options, "length", PASSWORD_HASH_LENGTH, label)?,
    ))
}

fn scrypt_options(
    options: Option<&HashMap<String, Value>>,
    label: &str,
) -> Result<(usize, usize, usize, usize, usize)> {
    let mut log_n = option_positive_integer(options, "log_n", SCRYPT_LOG_N, label)?;
    let cost = option_positive_integer(options, "cost", 1usize << log_n, label)?;
    if cost < 2 || !cost.is_power_of_two() {
        return Err(ZuzuRustError::thrown(format!(
            "{label} option 'cost' must be a power of two"
        )));
    }
    log_n = cost.trailing_zeros() as usize;
    Ok((
        log_n,
        cost,
        option_positive_integer(options, "r", SCRYPT_R, label)?,
        option_positive_integer(options, "p", SCRYPT_P, label)?,
        option_positive_integer(options, "length", PASSWORD_HASH_LENGTH, label)?,
    ))
}

fn password_hash_derive_bytes(
    password: &[u8],
    algorithm: &str,
    salt: &[u8],
    options: Option<&HashMap<String, Value>>,
    label: &str,
) -> Result<Vec<u8>> {
    match algorithm {
        "pbkdf2-sha256" => {
            let (iterations, length) = pbkdf2_options(options, label)?;
            let mut out = vec![0u8; length];
            pbkdf2_hmac::<Sha256>(password, salt, iterations as u32, &mut out);
            Ok(out)
        }
        "argon2id" => {
            let (memory, iterations, parallelism, length) = argon2id_options(options, label)?;
            let params = Argon2Params::new(
                memory as u32,
                iterations as u32,
                parallelism as u32,
                Some(length),
            )
            .map_err(|_| ZuzuRustError::thrown(format!("{label} invalid Argon2id options")))?;
            let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
            let mut out = vec![0u8; length];
            argon
                .hash_password_into(password, salt, &mut out)
                .map_err(|_| ZuzuRustError::thrown(format!("{label} Argon2id failed")))?;
            Ok(out)
        }
        "scrypt" => {
            let (log_n, _, r, p, length) = scrypt_options(options, label)?;
            let params = ScryptParams::new(log_n as u8, r as u32, p as u32, length)
                .map_err(|_| ZuzuRustError::thrown(format!("{label} invalid scrypt options")))?;
            let mut out = vec![0u8; length];
            scrypt(password, salt, &params, &mut out)
                .map_err(|_| ZuzuRustError::thrown(format!("{label} scrypt failed")))?;
            Ok(out)
        }
        _ => Err(ZuzuRustError::thrown(format!(
            "{label} does not support algorithm '{algorithm}'"
        ))),
    }
}

fn parse_param_list(text: &str) -> Option<HashMap<String, String>> {
    let mut out = HashMap::new();
    for part in text.split(',') {
        let (key, value) = part.split_once('=')?;
        if key.is_empty() || value.is_empty() {
            return None;
        }
        out.insert(key.to_owned(), value.to_owned());
    }
    Some(out)
}

fn parse_usize_param(params: &HashMap<String, String>, key: &str) -> Option<usize> {
    params.get(key)?.parse::<usize>().ok()
}

fn parse_password_hash(encoded: &str) -> Option<ParsedPasswordHash> {
    let parts: Vec<&str> = encoded.split('$').collect();
    if parts.first().copied() != Some("") {
        return None;
    }

    if parts.len() == 6 && parts[1] == "zuzu-pbkdf2-sha256" && parts[2] == "v=1" {
        let params = parse_param_list(parts[3])?;
        let iterations = parse_usize_param(&params, "i")?;
        let length = parse_usize_param(&params, "l")?;
        let salt = base64url_decode(parts[4])?;
        let hash = base64url_decode(parts[5])?;
        return Some(ParsedPasswordHash {
            algorithm: "pbkdf2-sha256",
            salt,
            hash,
            options: HashMap::from([
                ("iterations".to_owned(), Value::Number(iterations as f64)),
                ("length".to_owned(), Value::Number(length as f64)),
            ]),
        });
    }

    if parts.len() == 6 && parts[1] == "argon2id" && parts[2] == "v=19" {
        let params = parse_param_list(parts[3])?;
        let memory = parse_usize_param(&params, "m")?;
        let iterations = parse_usize_param(&params, "t")?;
        let parallelism = parse_usize_param(&params, "p")?;
        let salt = base64_nopad_decode(parts[4])?;
        let hash = base64_nopad_decode(parts[5])?;
        return Some(ParsedPasswordHash {
            algorithm: "argon2id",
            salt,
            hash: hash.clone(),
            options: HashMap::from([
                ("memory".to_owned(), Value::Number(memory as f64)),
                ("iterations".to_owned(), Value::Number(iterations as f64)),
                ("parallelism".to_owned(), Value::Number(parallelism as f64)),
                ("length".to_owned(), Value::Number(hash.len() as f64)),
            ]),
        });
    }

    if parts.len() == 5 && parts[1] == "scrypt" {
        let params = parse_param_list(parts[2])?;
        let log_n = parse_usize_param(&params, "ln")?;
        let r = parse_usize_param(&params, "r")?;
        let p = parse_usize_param(&params, "p")?;
        let length = parse_usize_param(&params, "l")?;
        let salt = base64url_decode(parts[3])?;
        let hash = base64url_decode(parts[4])?;
        return Some(ParsedPasswordHash {
            algorithm: "scrypt",
            salt,
            hash,
            options: HashMap::from([
                ("log_n".to_owned(), Value::Number(log_n as f64)),
                ("r".to_owned(), Value::Number(r as f64)),
                ("p".to_owned(), Value::Number(p as f64)),
                ("length".to_owned(), Value::Number(length as f64)),
            ]),
        });
    }

    None
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0u8;
    for (left, right) in left.iter().zip(right.iter()) {
        diff |= *left ^ *right;
    }
    diff == 0
}

fn hkdf_args(args: &[Value]) -> Result<(&[u8], usize, &[u8], &[u8])> {
    if args.len() < 2 || args.len() > 4 {
        return Err(ZuzuRustError::runtime(
            "KeyDerivation.hkdf_sha256 expects two to four arguments",
        ));
    }
    let input_key_material = binary_arg(args.first(), "KeyDerivation.hkdf_sha256", "BinaryString")?;
    let length = hkdf_length(args.get(1), "KeyDerivation.hkdf_sha256")?;
    let salt = optional_binary_arg(
        args.get(2),
        "KeyDerivation.hkdf_sha256",
        "BinaryString salt",
    )?;
    let info = optional_binary_arg(
        args.get(3),
        "KeyDerivation.hkdf_sha256",
        "BinaryString info",
    )?;
    Ok((input_key_material, length, salt, info))
}

fn binary_arg<'a>(value: Option<&'a Value>, label: &str, arg_name: &str) -> Result<&'a [u8]> {
    match value {
        Some(Value::BinaryString(bytes)) => Ok(bytes.as_slice()),
        Some(other) => Err(ZuzuRustError::thrown(format!(
            "TypeException: {label} expects {arg_name}, got {}",
            other.type_name()
        ))),
        None => Err(ZuzuRustError::thrown(format!(
            "TypeException: {label} expects {arg_name}, got Null"
        ))),
    }
}

fn optional_binary_arg<'a>(
    value: Option<&'a Value>,
    label: &str,
    arg_name: &str,
) -> Result<&'a [u8]> {
    match value {
        Some(Value::Null) | None => Ok(&[]),
        Some(_) => binary_arg(value, label, arg_name),
    }
}

fn hkdf_length(value: Option<&Value>, label: &str) -> Result<usize> {
    let Some(Value::Number(number)) = value else {
        return Err(ZuzuRustError::thrown(format!(
            "{label} expects length between 0 and {HKDF_SHA256_MAX_LENGTH}"
        )));
    };
    if *number < 0.0 || number.fract() != 0.0 || *number > HKDF_SHA256_MAX_LENGTH as f64 {
        return Err(ZuzuRustError::thrown(format!(
            "{label} expects length between 0 and {HKDF_SHA256_MAX_LENGTH}"
        )));
    }
    Ok(*number as usize)
}

fn hmac_sha256_bytes(key: &[u8], data: &[u8]) -> Result<Vec<u8>> {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key)
        .map_err(|_| ZuzuRustError::thrown("KeyDerivation.hkdf_sha256 failed"))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn hkdf_sha256_bytes(
    input_key_material: &[u8],
    length: usize,
    salt: &[u8],
    info: &[u8],
) -> Result<Vec<u8>> {
    if length == 0 {
        return Ok(Vec::new());
    }
    let prk = hmac_sha256_bytes(salt, input_key_material)?;
    let mut previous = Vec::new();
    let mut out = Vec::with_capacity(length);
    let mut counter = 1u8;
    while out.len() < length {
        let mut data = Vec::with_capacity(previous.len() + info.len() + 1);
        data.extend_from_slice(&previous);
        data.extend_from_slice(info);
        data.push(counter);
        previous = hmac_sha256_bytes(&prk, &data)?;
        out.extend_from_slice(&previous);
        counter = counter.wrapping_add(1);
    }
    out.truncate(length);
    Ok(out)
}

fn non_negative_integer(value: &Value, label: &str) -> Result<usize> {
    let Value::Number(number) = value else {
        return Err(ZuzuRustError::thrown(format!(
            "{label} expects a non-negative integer"
        )));
    };
    if *number < 0.0 || number.fract() != 0.0 {
        return Err(ZuzuRustError::thrown(format!(
            "{label} expects a non-negative integer"
        )));
    }
    if *number > usize::MAX as f64 {
        return Err(ZuzuRustError::thrown(format!(
            "{label} length is too large"
        )));
    }
    Ok(*number as usize)
}

fn positive_integer(value: &Value, label: &str) -> Result<u64> {
    let Value::Number(number) = value else {
        return Err(ZuzuRustError::thrown(format!(
            "{label} expects a positive integer"
        )));
    };
    if *number <= 0.0 || number.fract() != 0.0 {
        return Err(ZuzuRustError::thrown(format!(
            "{label} expects a positive integer"
        )));
    }
    if *number > MAX_SAFE_INT {
        return Err(ZuzuRustError::thrown(format!(
            "{label} maximum is too large"
        )));
    }
    Ok(*number as u64)
}

fn fill_random(bytes: &mut [u8]) -> Result<()> {
    getrandom::getrandom(bytes)
        .map_err(|err| ZuzuRustError::thrown(format!("Secure random failed: {err}")))
}

fn base64url(bytes: &[u8]) -> String {
    base64_encode(bytes, BASE64URL)
}

fn base64_nopad(bytes: &[u8]) -> String {
    base64_encode(bytes, BASE64)
}

fn base64_encode(bytes: &[u8], alphabet: &[u8; 64]) -> String {
    let mut out = String::new();
    let mut index = 0usize;
    while index < bytes.len() {
        let b0 = bytes[index];
        let b1 = bytes.get(index + 1).copied().unwrap_or(0);
        let b2 = bytes.get(index + 2).copied().unwrap_or(0);
        let triple = ((b0 as u32) << 16) | ((b1 as u32) << 8) | b2 as u32;

        out.push(alphabet[((triple >> 18) & 0x3f) as usize] as char);
        out.push(alphabet[((triple >> 12) & 0x3f) as usize] as char);
        if index + 1 < bytes.len() {
            out.push(alphabet[((triple >> 6) & 0x3f) as usize] as char);
        }
        if index + 2 < bytes.len() {
            out.push(alphabet[(triple & 0x3f) as usize] as char);
        }

        index += 3;
    }
    out
}

fn base64url_decode(text: &str) -> Option<Vec<u8>> {
    base64_decode(text, BASE64URL)
}

fn base64_nopad_decode(text: &str) -> Option<Vec<u8>> {
    base64_decode(text, BASE64)
}

fn base64_decode(text: &str, alphabet: &[u8; 64]) -> Option<Vec<u8>> {
    let mut map = [255u8; 256];
    for (index, byte) in alphabet.iter().enumerate() {
        map[*byte as usize] = index as u8;
    }

    let mut out = Vec::with_capacity(text.len() * 3 / 4);
    let mut bits = 0u32;
    let mut bit_len = 0u8;
    for byte in text.bytes() {
        let value = map[byte as usize];
        if value == 255 {
            return None;
        }
        bits = (bits << 6) | value as u32;
        bit_len += 6;
        while bit_len >= 8 {
            bit_len -= 8;
            out.push(((bits >> bit_len) & 0xff) as u8);
        }
    }
    if bit_len > 0 && (bits & ((1u32 << bit_len) - 1)) != 0 {
        return None;
    }
    Some(out)
}

fn random_int(max: u64) -> Result<u64> {
    if max == 1 {
        return Ok(0);
    }
    let limit = RANDOM_INT_SPACE - (RANDOM_INT_SPACE % max);
    loop {
        let mut bytes = [0u8; 8];
        fill_random(&mut bytes[1..])?;
        let value = u64::from_be_bytes(bytes);
        if value < limit {
            return Ok(value % max);
        }
    }
}
