use std::collections::HashMap;
use std::fs;
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};

use bzip2::read::BzDecoder;
use bzip2::write::BzEncoder;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::{Compression, GzBuilder};
use tar::{Archive as TarArchive, Builder as TarBuilder, Header};
use zip::write::FileOptions;
use zip::{CompressionMethod, ZipArchive, ZipWriter};

use super::super::{Runtime, Value};
use super::io::{path_buf_from_value, resolve_fs_path};
use crate::error::{Result, ZuzuRustError};

#[derive(Clone)]
struct ArchiveEntry {
    path: Option<String>,
    data: Vec<u8>,
}

pub(super) fn exports() -> HashMap<String, Value> {
    HashMap::from([(
        "Archive".to_owned(),
        Value::builtin_class("Archive".to_owned()),
    )])
}

pub(super) fn call_class_method(
    runtime: &Runtime,
    class_name: &str,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    if class_name != "Archive" {
        return None;
    }
    let value = match name {
        "encode" => call_encode(runtime, args),
        "decode" => call_decode(args),
        "load" => call_load(runtime, args),
        "dump" => call_dump(runtime, args),
        _ => return None,
    };
    Some(value)
}

fn call_encode(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    let archive = args.first().ok_or_else(|| {
        ZuzuRustError::thrown("TypeException: Archive.encode expects Dict archive")
    })?;
    let (entries, archive_format) = archive_entries(runtime, archive, "Archive.encode")?;
    let mut format = normalize_format(args.get(1).and_then(stringify).as_deref())?;
    if format == "auto" && archive_format != "auto" {
        format = archive_format;
    }
    if format == "auto" {
        return Err(ZuzuRustError::thrown(
            "Archive.encode requires an explicit format",
        ));
    }
    Ok(Value::BinaryString(encode_archive(&format, &entries)?))
}

fn call_decode(args: &[Value]) -> Result<Value> {
    let bytes = match args.first() {
        Some(Value::BinaryString(bytes)) => bytes.as_slice(),
        Some(other) => {
            return Err(ZuzuRustError::thrown(format!(
                "TypeException: Archive.decode expects BinaryString, got {}",
                other.type_name()
            )))
        }
        None => {
            return Err(ZuzuRustError::thrown(
                "TypeException: Archive.decode expects BinaryString, got Null",
            ))
        }
    };
    let mut format = normalize_format(args.get(1).and_then(stringify).as_deref())?;
    if format == "auto" {
        format = detect_format_from_bytes(bytes)?;
    }
    let entries = decode_archive(bytes, &format, None)?;
    Ok(archive_to_value(&format, entries))
}

fn call_load(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    let Some(target) = args.first() else {
        return Err(ZuzuRustError::thrown(
            "TypeException: Archive.load expects Path as first argument",
        ));
    };
    let path = expect_path(runtime, target, "Archive.load")?;
    let bytes =
        fs::read(&path).map_err(|err| ZuzuRustError::thrown(format!("load failed: {err}")))?;
    let mut format = normalize_format(args.get(1).and_then(stringify).as_deref())?;
    if format == "auto" {
        format = infer_format_from_path(&path).unwrap_or_else(|| "auto".to_owned());
    }
    if format == "auto" {
        format = detect_format_from_bytes(&bytes)?;
    }
    let default_name = derive_single_entry_name_from_path(&path, &format);
    let entries = decode_archive(&bytes, &format, default_name)?;
    Ok(archive_to_value(&format, entries))
}

fn call_dump(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(ZuzuRustError::thrown(
            "TypeException: Archive.dump expects Path as first argument",
        ));
    }
    let path = expect_path(runtime, &args[0], "Archive.dump")?;
    let archive = args
        .get(1)
        .ok_or_else(|| ZuzuRustError::thrown("TypeException: Archive.dump expects Dict archive"))?;
    let (entries, archive_format) = archive_entries(runtime, archive, "Archive.dump")?;
    let format = resolve_format_for_encode(&path, &archive_format, args.get(2))?;
    let blob = encode_archive(&format, &entries)?;
    fs::write(path, blob).map_err(|err| ZuzuRustError::thrown(format!("dump failed: {err}")))?;
    Ok(args[0].clone())
}

fn expect_path(runtime: &Runtime, value: &Value, label: &str) -> Result<PathBuf> {
    match value {
        Value::Object(object) if object.borrow().class.name == "Path" => {
            Ok(resolve_fs_path(runtime, &path_buf_from_value(value)))
        }
        _ => Err(ZuzuRustError::thrown(format!(
            "TypeException: {label} expects Path as first argument"
        ))),
    }
}

fn archive_dict(value: &Value) -> Option<&HashMap<String, Value>> {
    match value {
        Value::Dict(map) => Some(map),
        _ => None,
    }
}

fn archive_entries(
    runtime: &Runtime,
    value: &Value,
    label: &str,
) -> Result<(Vec<ArchiveEntry>, String)> {
    let archive_value = match value {
        Value::Shared(value) => value.borrow().clone(),
        other => other.clone(),
    };
    let archive = archive_dict(&archive_value).ok_or_else(|| {
        ZuzuRustError::thrown(format!(
            "TypeException: {label} expects Dict archive, got {}",
            value.type_name()
        ))
    })?;
    let entries = match archive.get("entries") {
        Some(Value::Array(entries)) => entries.clone(),
        Some(Value::Shared(value)) => match &*value.borrow() {
            Value::Array(entries) => entries.clone(),
            other => {
                return Err(ZuzuRustError::thrown(format!(
                    "TypeException: {label} expects archive.entries to be an Array, got {}",
                    other.type_name()
                )))
            }
        },
        Some(other) => {
            return Err(ZuzuRustError::thrown(format!(
                "TypeException: {label} expects archive.entries to be an Array, got {}",
                other.type_name()
            )))
        }
        None => {
            return Err(ZuzuRustError::thrown(format!(
                "TypeException: {label} expects archive.entries to be an Array"
            )))
        }
    };

    let mut out = Vec::new();
    for (index, entry) in entries.into_iter().enumerate() {
        let entry = match entry {
            Value::Shared(value) => value.borrow().clone(),
            other => other,
        };
        let Value::Dict(entry_map) = entry else {
            return Err(ZuzuRustError::thrown(format!(
                "TypeException: {label} expects archive.entries[{index}] to be a Dict"
            )));
        };
        let path = entry_map.get("path").and_then(stringify);
        let data = match entry_map.get("data") {
            Some(Value::BinaryString(bytes)) => bytes.clone(),
            Some(Value::Null) | None => match entry_map.get("data_from") {
                Some(Value::Null) | None => {
                    return Err(ZuzuRustError::thrown(format!(
                        "TypeException: {label} archive.entries[{index}] expects BinaryString data or Path data_from"
                    )))
                }
                Some(value) => {
                    let path = expect_path(
                        runtime,
                        value,
                        &format!("{label} archive.entries[{index}].data_from"),
                    )?;
                    fs::read(path).map_err(|err| {
                        ZuzuRustError::thrown(format!("archive read failed: {err}"))
                    })?
                }
            },
            Some(other) => {
                return Err(ZuzuRustError::thrown(format!(
                "TypeException: {label} archive.entries[{index}].data expects BinaryString, got {}",
                other.type_name()
            )))
            }
        };
        out.push(ArchiveEntry { path, data });
    }

    let archive_format = normalize_format(archive.get("format").and_then(stringify).as_deref())?;
    Ok((out, archive_format))
}

fn archive_to_value(format: &str, entries: Vec<ArchiveEntry>) -> Value {
    Value::Dict(HashMap::from([
        ("format".to_owned(), Value::String(format.to_owned())),
        (
            "entries".to_owned(),
            Value::Array(
                entries
                    .into_iter()
                    .map(|entry| {
                        Value::Dict(HashMap::from([
                            (
                                "path".to_owned(),
                                entry.path.map(Value::String).unwrap_or(Value::Null),
                            ),
                            ("data".to_owned(), Value::BinaryString(entry.data)),
                        ]))
                    })
                    .collect(),
            ),
        ),
    ]))
}

fn encode_archive(format: &str, entries: &[ArchiveEntry]) -> Result<Vec<u8>> {
    match format {
        "zip" => encode_zip(entries),
        "tar" => encode_tar(entries),
        "tar.gz" => gzip_bytes(encode_tar(entries)?, None),
        "tar.bz2" => bzip2_bytes(encode_tar(entries)?),
        "gz" | "bz2" => encode_single_compressed(format, entries),
        other => Err(ZuzuRustError::thrown(format!(
            "Unsupported archive format '{other}'"
        ))),
    }
}

fn decode_archive(
    bytes: &[u8],
    format: &str,
    default_name: Option<String>,
) -> Result<Vec<ArchiveEntry>> {
    match format {
        "zip" => decode_zip(bytes),
        "tar" => decode_tar(bytes),
        "tar.gz" => decode_tar(&gunzip_bytes(bytes)?.0),
        "tar.bz2" => decode_tar(&bunzip2_bytes(bytes)?),
        "gz" => {
            let (data, name) = gunzip_bytes(bytes)?;
            Ok(vec![ArchiveEntry {
                path: name.or(default_name),
                data,
            }])
        }
        "bz2" => Ok(vec![ArchiveEntry {
            path: default_name,
            data: bunzip2_bytes(bytes)?,
        }]),
        other => Err(ZuzuRustError::thrown(format!(
            "Unsupported archive format '{other}'"
        ))),
    }
}

fn encode_zip(entries: &[ArchiveEntry]) -> Result<Vec<u8>> {
    let cursor = Cursor::new(Vec::new());
    let mut writer = ZipWriter::new(cursor);
    let options = FileOptions::default()
        .compression_method(CompressionMethod::Deflated)
        .unix_permissions(0o644);
    for entry in entries {
        let path = archive_entry_path(entry.path.as_deref(), "Archive.encode")?;
        writer
            .start_file(path, options)
            .map_err(|err| ZuzuRustError::thrown(format!("Archive encode failed: {err}")))?;
        writer
            .write_all(&entry.data)
            .map_err(|err| ZuzuRustError::thrown(format!("Archive encode failed: {err}")))?;
    }
    let cursor = writer
        .finish()
        .map_err(|err| ZuzuRustError::thrown(format!("Archive encode failed: {err}")))?;
    Ok(cursor.into_inner())
}

fn decode_zip(bytes: &[u8]) -> Result<Vec<ArchiveEntry>> {
    let cursor = Cursor::new(bytes);
    let mut archive = ZipArchive::new(cursor)
        .map_err(|err| ZuzuRustError::thrown(format!("Archive decode failed: {err}")))?;
    let mut entries = Vec::new();
    for index in 0..archive.len() {
        let mut file = archive
            .by_index(index)
            .map_err(|err| ZuzuRustError::thrown(format!("Archive decode failed: {err}")))?;
        if file.is_dir() {
            continue;
        }
        let mut data = Vec::new();
        file.read_to_end(&mut data)
            .map_err(|err| ZuzuRustError::thrown(format!("Archive decode failed: {err}")))?;
        entries.push(ArchiveEntry {
            path: Some(file.name().to_owned()),
            data,
        });
    }
    Ok(entries)
}

fn encode_tar(entries: &[ArchiveEntry]) -> Result<Vec<u8>> {
    let mut builder = TarBuilder::new(Vec::new());
    for entry in entries {
        let path = archive_entry_path(entry.path.as_deref(), "Archive.encode")?;
        let mut header = Header::new_gnu();
        header.set_size(entry.data.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, path, entry.data.as_slice())
            .map_err(|err| ZuzuRustError::thrown(format!("Archive encode failed: {err}")))?;
    }
    builder
        .into_inner()
        .map_err(|err| ZuzuRustError::thrown(format!("Archive encode failed: {err}")))
}

fn decode_tar(bytes: &[u8]) -> Result<Vec<ArchiveEntry>> {
    let cursor = Cursor::new(bytes);
    let mut archive = TarArchive::new(cursor);
    let mut out = Vec::new();
    let entries = archive
        .entries()
        .map_err(|err| ZuzuRustError::thrown(format!("Archive decode failed: {err}")))?;
    for entry in entries {
        let mut entry =
            entry.map_err(|err| ZuzuRustError::thrown(format!("Archive decode failed: {err}")))?;
        if !entry.header().entry_type().is_file() {
            continue;
        }
        let path = entry
            .path()
            .map_err(|err| ZuzuRustError::thrown(format!("Archive decode failed: {err}")))?
            .to_string_lossy()
            .to_string();
        let mut data = Vec::new();
        entry
            .read_to_end(&mut data)
            .map_err(|err| ZuzuRustError::thrown(format!("Archive decode failed: {err}")))?;
        out.push(ArchiveEntry {
            path: Some(path),
            data,
        });
    }
    Ok(out)
}

fn encode_single_compressed(format: &str, entries: &[ArchiveEntry]) -> Result<Vec<u8>> {
    if entries.len() != 1 {
        return Err(ZuzuRustError::thrown(format!(
            "Archive.encode with format '{format}' expects exactly one entry"
        )));
    }
    let entry = &entries[0];
    match format {
        "gz" => {
            let filename = entry
                .path
                .as_deref()
                .map(|path| archive_entry_path(Some(path), "Archive.encode"))
                .transpose()?
                .and_then(|path| Path::new(&path).file_name().map(|name| name.to_os_string()))
                .map(|name| name.to_string_lossy().to_string());
            gzip_bytes(entry.data.clone(), filename.as_deref())
        }
        "bz2" => bzip2_bytes(entry.data.clone()),
        other => Err(ZuzuRustError::thrown(format!(
            "Unsupported archive format '{other}'"
        ))),
    }
}

fn gzip_bytes(bytes: Vec<u8>, filename: Option<&str>) -> Result<Vec<u8>> {
    let mut encoder = if let Some(filename) = filename {
        GzBuilder::new()
            .filename(filename)
            .write(Vec::new(), Compression::default())
    } else {
        GzEncoder::new(Vec::new(), Compression::default())
    };
    encoder
        .write_all(&bytes)
        .map_err(|err| ZuzuRustError::thrown(format!("Archive encode failed: {err}")))?;
    encoder
        .finish()
        .map_err(|err| ZuzuRustError::thrown(format!("Archive encode failed: {err}")))
}

fn bzip2_bytes(bytes: Vec<u8>) -> Result<Vec<u8>> {
    let mut encoder = BzEncoder::new(Vec::new(), bzip2::Compression::best());
    encoder
        .write_all(&bytes)
        .map_err(|err| ZuzuRustError::thrown(format!("Archive encode failed: {err}")))?;
    encoder
        .finish()
        .map_err(|err| ZuzuRustError::thrown(format!("Archive encode failed: {err}")))
}

fn gunzip_bytes(bytes: &[u8]) -> Result<(Vec<u8>, Option<String>)> {
    let name = parse_gzip_name(bytes);
    let mut decoder = GzDecoder::new(bytes);
    let mut out = Vec::new();
    decoder
        .read_to_end(&mut out)
        .map_err(|err| ZuzuRustError::thrown(format!("Archive decode failed: {err}")))?;
    Ok((out, name))
}

fn bunzip2_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = BzDecoder::new(bytes);
    let mut out = Vec::new();
    decoder
        .read_to_end(&mut out)
        .map_err(|err| ZuzuRustError::thrown(format!("Archive decode failed: {err}")))?;
    Ok(out)
}

fn normalize_format(raw: Option<&str>) -> Result<String> {
    let Some(raw) = raw else {
        return Ok("auto".to_owned());
    };
    if raw.is_empty() {
        return Ok("auto".to_owned());
    }
    let normalized = raw.trim().trim_start_matches('.').to_ascii_lowercase();
    let normalized = match normalized.as_str() {
        "auto" => "auto",
        "zip" => "zip",
        "tar" => "tar",
        "tar.gz" | "tgz" | "gzip+tar" => "tar.gz",
        "tar.bz2" | "tbz" | "tbz2" | "bzip2+tar" => "tar.bz2",
        "gz" | "gzip" => "gz",
        "bz2" | "bzip2" => "bz2",
        _ => {
            return Err(ZuzuRustError::thrown(format!(
                "Unsupported archive format '{raw}'"
            )))
        }
    };
    Ok(normalized.to_owned())
}

fn resolve_format_for_encode(
    path: &Path,
    archive_format: &str,
    given_format: Option<&Value>,
) -> Result<String> {
    let mut format = normalize_format(given_format.and_then(stringify).as_deref())?;
    if format == "auto" && archive_format != "auto" {
        format = archive_format.to_owned();
    }
    if format == "auto" {
        format = infer_format_from_path(path).unwrap_or_else(|| "auto".to_owned());
    }
    if format == "auto" {
        return Err(ZuzuRustError::thrown(
            "Archive format is required when it cannot be inferred from path",
        ));
    }
    Ok(format)
}

fn infer_format_from_path(path: &Path) -> Option<String> {
    let text = path.to_string_lossy().to_ascii_lowercase();
    if text.ends_with(".tar.gz") || text.ends_with(".tgz") {
        return Some("tar.gz".to_owned());
    }
    if text.ends_with(".tar.bz2") || text.ends_with(".tbz") || text.ends_with(".tbz2") {
        return Some("tar.bz2".to_owned());
    }
    if text.ends_with(".zip") {
        return Some("zip".to_owned());
    }
    if text.ends_with(".tar") {
        return Some("tar".to_owned());
    }
    if text.ends_with(".gz") {
        return Some("gz".to_owned());
    }
    if text.ends_with(".bz2") {
        return Some("bz2".to_owned());
    }
    None
}

fn detect_format_from_bytes(bytes: &[u8]) -> Result<String> {
    if bytes.len() >= 4 && bytes[0] == b'P' && bytes[1] == b'K' {
        return Ok("zip".to_owned());
    }
    if bytes.len() >= 2 && bytes[0] == 0x1f && bytes[1] == 0x8b {
        let (raw, _) = gunzip_bytes(bytes)?;
        return Ok(if looks_like_tar(&raw) {
            "tar.gz".to_owned()
        } else {
            "gz".to_owned()
        });
    }
    if bytes.len() >= 3 && bytes[0] == b'B' && bytes[1] == b'Z' && bytes[2] == b'h' {
        let raw = bunzip2_bytes(bytes)?;
        return Ok(if looks_like_tar(&raw) {
            "tar.bz2".to_owned()
        } else {
            "bz2".to_owned()
        });
    }
    if looks_like_tar(bytes) {
        return Ok("tar".to_owned());
    }
    Err(ZuzuRustError::thrown(
        "Could not detect archive format from bytes",
    ))
}

fn looks_like_tar(bytes: &[u8]) -> bool {
    bytes.len() >= 512 && &bytes[257..262] == b"ustar"
}

fn parse_gzip_name(bytes: &[u8]) -> Option<String> {
    if bytes.len() < 10 || bytes[0] != 0x1f || bytes[1] != 0x8b {
        return None;
    }
    let flags = bytes[3];
    let mut index = 10;
    if flags & 0x04 != 0 {
        if index + 1 >= bytes.len() {
            return None;
        }
        let len = usize::from(bytes[index]) | (usize::from(bytes[index + 1]) << 8);
        index += 2 + len;
    }
    if flags & 0x08 == 0 {
        return None;
    }
    let end = bytes[index..].iter().position(|byte| *byte == 0)?;
    let name = String::from_utf8_lossy(&bytes[index..index + end]).to_string();
    (!name.is_empty()).then_some(name)
}

fn derive_single_entry_name_from_path(path: &Path, format: &str) -> Option<String> {
    let mut name = path.file_name()?.to_string_lossy().to_string();
    if format == "gz" {
        for suffix in [".gzip", ".gz"] {
            if name.to_ascii_lowercase().ends_with(suffix) {
                name.truncate(name.len() - suffix.len());
                break;
            }
        }
    } else if format == "bz2" && name.to_ascii_lowercase().ends_with(".bz2") {
        name.truncate(name.len() - 4);
    }
    (!name.is_empty()).then_some(name)
}

fn archive_entry_path(raw: Option<&str>, label: &str) -> Result<String> {
    let value = raw.unwrap_or_default();
    if value.is_empty() {
        return Err(ZuzuRustError::thrown(format!("{label} requires path")));
    }
    Ok(value.to_owned())
}

fn stringify(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Boolean(true) => Some("1".to_owned()),
        Value::Boolean(false) => Some("0".to_owned()),
        Value::Shared(value) => stringify(&value.borrow()),
        _ => None,
    }
}
