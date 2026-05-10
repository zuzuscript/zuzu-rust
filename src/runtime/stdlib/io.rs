use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::Shutdown;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use super::super::collection::common::require_arity;
use super::super::{
    FieldSpec, MethodValue, ObjectValue, Runtime, TraitValue, UserClassValue, Value,
};
use crate::error::{Result, ZuzuRustError};
use tokio::io::AsyncWriteExt;

pub(super) fn exports() -> HashMap<String, Value> {
    let mut exports = HashMap::new();
    exports.insert("Path".to_owned(), Value::builtin_class("Path".to_owned()));
    exports.insert("STDIN".to_owned(), stream_object("StandardInputStream"));
    exports.insert("STDOUT".to_owned(), stream_object("StandardOutputStream"));
    exports.insert("STDERR".to_owned(), stream_object("StandardErrorStream"));
    exports
}

pub(super) fn socks_exports() -> HashMap<String, Value> {
    let mut exports = HashMap::new();
    for name in [
        "listen_tcp",
        "connect_tcp",
        "bind_udp",
        "connect_udp",
        "listen_unix",
        "connect_unix",
    ] {
        exports.insert(name.to_owned(), Value::native_function(name.to_owned()));
    }
    exports
}

fn path_class() -> Rc<UserClassValue> {
    Rc::new(UserClassValue {
        name: "Path".to_owned(),
        base: None,
        traits: Vec::<Rc<TraitValue>>::new(),
        fields: vec![FieldSpec {
            name: "path".to_owned(),
            declared_type: Some("String".to_owned()),
            mutable: true,
            accessors: Vec::new(),
            default_value: None,
            is_weak_storage: false,
        }],
        methods: HashMap::<String, Rc<MethodValue>>::new(),
        static_methods: HashMap::<String, Rc<MethodValue>>::new(),
        nested_classes: HashMap::new(),
        source_decl: None,
        closure_env: None,
    })
}

pub(super) fn path_object(path: PathBuf) -> Value {
    Value::Object(Rc::new(RefCell::new(ObjectValue {
        class: path_class(),
        fields: HashMap::from([(
            "path".to_owned(),
            Value::String(path.to_string_lossy().to_string()),
        )]),
        weak_fields: std::collections::HashSet::new(),
        builtin_value: Some(Value::String(path.to_string_lossy().to_string())),
    })))
}

fn stream_object(class_name: &str) -> Value {
    Value::Object(Rc::new(RefCell::new(ObjectValue {
        class: Rc::new(UserClassValue {
            name: class_name.to_owned(),
            base: None,
            traits: Vec::<Rc<TraitValue>>::new(),
            fields: Vec::<FieldSpec>::new(),
            methods: HashMap::<String, Rc<MethodValue>>::new(),
            static_methods: HashMap::<String, Rc<MethodValue>>::new(),
            nested_classes: HashMap::new(),
            source_decl: None,
            closure_env: None,
        }),
        fields: HashMap::new(),
        weak_fields: std::collections::HashSet::new(),
        builtin_value: Some(Value::String(class_name.to_owned())),
    })))
}

fn warn_blocking_path_operation(runtime: &Runtime, operation: &str) -> Result<()> {
    runtime.warn_blocking_operation(&format!("std/io Path.{operation}"))
}

pub(super) fn path_buf_from_value(value: &Value) -> PathBuf {
    match value {
        Value::Object(object) if object.borrow().class.name == "Path" => {
            match object.borrow().fields.get("path") {
                Some(Value::String(path)) => PathBuf::from(path),
                _ => PathBuf::new(),
            }
        }
        Value::String(path) => PathBuf::from(path),
        other => PathBuf::from(other.render()),
    }
}

pub(super) fn repo_root(runtime: &Runtime) -> PathBuf {
    if let Ok(mut current) = std::env::current_dir() {
        loop {
            if current.join("modules").join("std").is_dir()
                || current.join("modules").is_dir()
                || current.join("stdlib").join("modules").join("std").is_dir()
            {
                return current;
            }
            if !current.pop() {
                break;
            }
        }
    }

    for module_root in &runtime.module_roots {
        if module_root.join("std").is_dir() {
            if let Some(root) = module_root.parent() {
                if root.file_name().and_then(|name| name.to_str()) == Some("stdlib") {
                    if let Some(project_root) = root.parent() {
                        return project_root.to_path_buf();
                    }
                }
                return root.to_path_buf();
            }
        }
    }

    runtime
        .module_roots
        .first()
        .and_then(|module_root| module_root.parent().map(Path::to_path_buf))
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
}

pub(super) fn resolve_fs_path(runtime: &Runtime, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        repo_root(runtime).join(path)
    }
}

fn io_path_error(path: &Path, err: std::io::Error) -> ZuzuRustError {
    ZuzuRustError::runtime(format!("IOError: {}: {err}", path.display()))
}

fn path_cursor_key(path: &Path, raw: bool) -> String {
    format!(
        "{}:{}",
        path.to_string_lossy(),
        if raw { "raw" } else { "text" }
    )
}

fn stat_dict(metadata: &fs::Metadata) -> Value {
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;

    let mut out = HashMap::new();
    out.insert("size".to_owned(), Value::Number(metadata.len() as f64));
    #[cfg(unix)]
    {
        out.insert("dev".to_owned(), Value::Number(metadata.dev() as f64));
        out.insert("ino".to_owned(), Value::Number(metadata.ino() as f64));
        out.insert("mode".to_owned(), Value::Number(metadata.mode() as f64));
        out.insert("nlink".to_owned(), Value::Number(metadata.nlink() as f64));
        out.insert("uid".to_owned(), Value::Number(metadata.uid() as f64));
        out.insert("gid".to_owned(), Value::Number(metadata.gid() as f64));
        out.insert("rdev".to_owned(), Value::Number(metadata.rdev() as f64));
        out.insert("atime".to_owned(), Value::Number(metadata.atime() as f64));
        out.insert("mtime".to_owned(), Value::Number(metadata.mtime() as f64));
        out.insert("ctime".to_owned(), Value::Number(metadata.ctime() as f64));
        out.insert(
            "blksize".to_owned(),
            Value::Number(metadata.blksize() as f64),
        );
        out.insert("blocks".to_owned(), Value::Number(metadata.blocks() as f64));
    }
    #[cfg(not(unix))]
    {
        for key in [
            "dev", "ino", "mode", "nlink", "uid", "gid", "rdev", "atime", "mtime", "ctime",
            "blksize", "blocks",
        ] {
            out.insert(key.to_owned(), Value::Number(0.0));
        }
    }
    Value::Dict(out)
}

fn format_size_human(size: u64) -> String {
    if size < 1024 {
        return format!("{size}B");
    }
    let units = ["KiB", "MiB", "GiB", "TiB"];
    let mut value = size as f64;
    let mut unit = "B";
    for next in units {
        value /= 1024.0;
        unit = next;
        if value < 1024.0 {
            break;
        }
    }
    format!("{value:.1}{unit}")
}

pub(super) fn call_class_method(
    runtime: &Runtime,
    class_name: &str,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    if class_name != "Path" {
        return None;
    }
    let value = match name {
        "cwd" => Ok(path_object(repo_root(runtime))),
        "tempfile" => {
            let mut path = std::env::temp_dir();
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or(0);
            path.push(format!("zuzu-rust-{nanos}.tmp"));
            Ok(path_object(path))
        }
        "tempdir" => {
            let mut path = std::env::temp_dir();
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or(0);
            path.push(format!("zuzu-rust-{nanos}.d"));
            let _ = fs::create_dir_all(&path);
            Ok(path_object(path))
        }
        "rootdir" => {
            #[cfg(windows)]
            {
                Ok(path_object(PathBuf::from("C:\\")))
            }
            #[cfg(not(windows))]
            {
                Ok(path_object(PathBuf::from("/")))
            }
        }
        "join" => require_arity(name, args, 1).and_then(|_| {
            let mut path = PathBuf::new();
            if let Value::Array(parts) = &args[0] {
                for part in parts {
                    path.push(runtime.render_value(part)?);
                }
            }
            Ok(path_object(path))
        }),
        "split" => require_arity(name, args, 1).and_then(|_| {
            let path = path_buf_from_value(&args[0]);
            let parts = path
                .components()
                .map(|component| Value::String(component.as_os_str().to_string_lossy().to_string()))
                .collect::<Vec<_>>();
            Ok(Value::Array(parts))
        }),
        "normalize" => require_arity(name, args, 1).and_then(|_| {
            let path = path_buf_from_value(&args[0]);
            let mut normalized = PathBuf::new();
            for component in path.components() {
                use std::path::Component;
                match component {
                    Component::CurDir => {}
                    Component::ParentDir => {
                        normalized.pop();
                    }
                    other => normalized.push(other.as_os_str()),
                }
            }
            Ok(path_object(normalized))
        }),
        "glob" => require_arity(name, args, 1).and_then(|_| {
            let pattern = runtime.render_value(&args[0])?;
            let (base, suffix) = pattern
                .rsplit_once(std::path::MAIN_SEPARATOR)
                .map(|(dir, leaf)| (PathBuf::from(dir), leaf.to_owned()))
                .unwrap_or_else(|| (PathBuf::from("."), pattern.clone()));
            let fs_base = resolve_fs_path(runtime, &base);
            let mut out = Vec::new();
            if let Ok(entries) = fs::read_dir(&fs_base) {
                for entry in entries.flatten() {
                    let entry_path = entry.path();
                    let file_name = entry_path
                        .file_name()
                        .map(|value| value.to_string_lossy().to_string())
                        .unwrap_or_default();
                    if glob_matches(&suffix, &file_name) {
                        let display = entry_path
                            .strip_prefix(repo_root(runtime))
                            .unwrap_or(&entry_path)
                            .to_path_buf();
                        out.push(path_object(display));
                    }
                }
            }
            Ok(Value::Array(out))
        }),
        _ => return None,
    };
    Some(value)
}

pub(super) fn construct_path(args: Vec<Value>, named_args: Vec<(String, Value)>) -> Result<Value> {
    if args.len() > 1 {
        return Err(ZuzuRustError::runtime(
            "constructor for 'Path' accepts at most one positional argument",
        ));
    }
    let mut path = args.first().map(path_buf_from_value).unwrap_or_default();
    for (name, value) in named_args {
        if name == "path" {
            path = path_buf_from_value(&value);
        }
    }
    Ok(path_object(path))
}

pub(super) fn has_builtin_object_method(class_name: &str, name: &str) -> bool {
    if matches!(
        class_name,
        "StandardInputStream" | "StandardOutputStream" | "StandardErrorStream"
    ) {
        return match class_name {
            "StandardInputStream" => matches!(name, "next_line" | "each_line"),
            _ => matches!(name, "print" | "say"),
        };
    }
    if class_name == "Path" {
        return matches!(
            name,
            "to_String"
                | "child"
                | "basename"
                | "parent"
                | "sibling"
                | "absolute"
                | "exists"
                | "is_file"
                | "is_dir"
                | "is_relative"
                | "is_absolute"
                | "is_rootdir"
                | "touchpath"
                | "touch"
                | "mkdir"
                | "mkdir_exclusive"
                | "size"
                | "size_human"
                | "stat"
                | "lstat"
                | "chmod"
                | "copy"
                | "move"
                | "remove"
                | "remove_tree"
                | "spew_utf8"
                | "spew_utf8_async"
                | "slurp_utf8"
                | "slurp_utf8_async"
                | "append_utf8"
                | "append_utf8_async"
                | "spew"
                | "spew_async"
                | "append"
                | "append_async"
                | "slurp"
                | "slurp_async"
                | "each_line"
                | "next_line"
                | "lines"
                | "lines_async"
                | "lines_utf8"
                | "lines_utf8_async"
                | "children"
                | "iterator"
                | "canonpath"
                | "realpath"
                | "subsumes"
                | "volume"
        );
    }
    has_socket_object_method(class_name, name)
}

pub(super) fn call_object_method(
    runtime: &Runtime,
    class_name: &str,
    builtin_value: &Value,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    if matches!(
        class_name,
        "StandardInputStream" | "StandardOutputStream" | "StandardErrorStream"
    ) {
        return call_standard_stream_method(runtime, class_name, name, args);
    }
    if class_name != "Path" {
        return call_socket_object_method(runtime, class_name, builtin_value, name, args);
    }
    let path = path_buf_from_value(builtin_value);
    let fs_path = resolve_fs_path(runtime, &path);
    let value = match name {
        "to_String" => Ok(Value::String(path.to_string_lossy().to_string())),
        "child" => require_arity(name, args, 1)
            .and_then(|_| Ok(path_object(path.join(runtime.render_value(&args[0])?)))),
        "basename" => require_arity(name, args, 0).and_then(|_| {
            Ok(Value::String(
                path.file_name()
                    .map(|value| value.to_string_lossy().to_string())
                    .unwrap_or_default(),
            ))
        }),
        "parent" => require_arity(name, args, 0).and_then(|_| {
            Ok(path_object(
                path.parent()
                    .unwrap_or_else(|| Path::new("."))
                    .to_path_buf(),
            ))
        }),
        "sibling" => require_arity(name, args, 1).and_then(|_| {
            let sibling = runtime.render_value(&args[0])?;
            Ok(path_object(
                path.parent()
                    .unwrap_or_else(|| Path::new("."))
                    .join(sibling),
            ))
        }),
        "absolute" => require_arity(name, args, 0).and_then(|_| {
            warn_blocking_path_operation(runtime, "absolute")?;
            Ok(path_object(fs::canonicalize(&fs_path).unwrap_or_else(
                |_| {
                    if path.is_absolute() {
                        path.clone()
                    } else {
                        repo_root(runtime).join(&path)
                    }
                },
            )))
        }),
        "exists" => require_arity(name, args, 0).and_then(|_| {
            warn_blocking_path_operation(runtime, "exists")?;
            Ok(Value::Boolean(fs_path.exists()))
        }),
        "is_file" => require_arity(name, args, 0).and_then(|_| {
            warn_blocking_path_operation(runtime, "is_file")?;
            Ok(Value::Boolean(fs_path.is_file()))
        }),
        "is_dir" => require_arity(name, args, 0).and_then(|_| {
            warn_blocking_path_operation(runtime, "is_dir")?;
            Ok(Value::Boolean(fs_path.is_dir()))
        }),
        "is_relative" => {
            require_arity(name, args, 0).and_then(|_| Ok(Value::Boolean(path.is_relative())))
        }
        "is_absolute" => {
            require_arity(name, args, 0).and_then(|_| Ok(Value::Boolean(path.is_absolute())))
        }
        "is_rootdir" => {
            require_arity(name, args, 0).and_then(|_| Ok(Value::Boolean(path.parent().is_none())))
        }
        "touchpath" => require_arity(name, args, 0).and_then(|_| {
            warn_blocking_path_operation(runtime, "touchpath")?;
            let _ = fs::create_dir_all(fs_path.parent().unwrap_or_else(|| Path::new(".")));
            let _ = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&fs_path);
            Ok(Value::Null)
        }),
        "touch" => require_arity(name, args, 0).and_then(|_| {
            warn_blocking_path_operation(runtime, "touch")?;
            let _ = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&fs_path);
            Ok(path_object(path))
        }),
        "mkdir" => require_arity(name, args, 0).and_then(|_| {
            warn_blocking_path_operation(runtime, "mkdir")?;
            Ok(Value::Boolean(fs::create_dir_all(&fs_path).is_ok()))
        }),
        "mkdir_exclusive" => require_arity(name, args, 0).and_then(|_| {
            warn_blocking_path_operation(runtime, "mkdir_exclusive")?;
            match fs::create_dir(&fs_path) {
                Ok(()) => Ok(Value::Boolean(true)),
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    Ok(Value::Boolean(false))
                }
                Err(err) => Err(ZuzuRustError::runtime(format!(
                    "IOError: mkdir_exclusive failed for {}: {err}",
                    path.to_string_lossy()
                ))),
            }
        }),
        "size" => require_arity(name, args, 0).and_then(|_| {
            warn_blocking_path_operation(runtime, "size")?;
            Ok(Value::Number(
                fs::metadata(&fs_path)
                    .map(|meta| meta.len() as f64)
                    .unwrap_or(0.0),
            ))
        }),
        "size_human" => require_arity(name, args, 0).and_then(|_| {
            warn_blocking_path_operation(runtime, "size_human")?;
            Ok(Value::String(format_size_human(
                fs::metadata(&fs_path).map(|meta| meta.len()).unwrap_or(0),
            )))
        }),
        "stat" => require_arity(name, args, 0).and_then(|_| {
            warn_blocking_path_operation(runtime, "stat")?;
            let metadata = fs::metadata(&fs_path)
                .map_err(|err| ZuzuRustError::runtime(format!("IOError: {err}")))?;
            Ok(stat_dict(&metadata))
        }),
        "lstat" => require_arity(name, args, 0).and_then(|_| {
            warn_blocking_path_operation(runtime, "lstat")?;
            let metadata = fs::symlink_metadata(&fs_path)
                .map_err(|err| ZuzuRustError::runtime(format!("IOError: {err}")))?;
            Ok(stat_dict(&metadata))
        }),
        "chmod" => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if args.len() != 1 {
                    Err(ZuzuRustError::runtime("chmod() expects one argument"))
                } else {
                    let mode = match runtime.value_to_number(&args[0]) {
                        Ok(mode) => mode as u32,
                        Err(err) => return Some(Err(err)),
                    };
                    if let Err(err) = warn_blocking_path_operation(runtime, "chmod") {
                        return Some(Err(err));
                    }
                    match fs::metadata(&fs_path) {
                        Ok(metadata) => {
                            let mut perms = metadata.permissions();
                            perms.set_mode(mode);
                            let ok = fs::set_permissions(&fs_path, perms).is_ok();
                            Ok(Value::Number(if ok { 1.0 } else { 0.0 }))
                        }
                        Err(_) => Ok(Value::Number(0.0)),
                    }
                }
            }
            #[cfg(not(unix))]
            {
                let _ = args;
                Ok(Value::Number(0.0))
            }
        }
        "copy" => require_arity(name, args, 1).and_then(|_| {
            warn_blocking_path_operation(runtime, "copy")?;
            let target = path_buf_from_value(&args[0]);
            let target_fs = resolve_fs_path(runtime, &target);
            fs::copy(&fs_path, &target_fs).map_err(|err| io_path_error(&target_fs, err))?;
            Ok(path_object(target))
        }),
        "move" => require_arity(name, args, 1).and_then(|_| {
            warn_blocking_path_operation(runtime, "move")?;
            let target = path_buf_from_value(&args[0]);
            let target_fs = resolve_fs_path(runtime, &target);
            fs::rename(&fs_path, &target_fs).map_err(|err| io_path_error(&target_fs, err))?;
            Ok(path_object(target))
        }),
        "remove" => require_arity(name, args, 0).and_then(|_| {
            warn_blocking_path_operation(runtime, "remove")?;
            Ok(Value::Boolean(
                fs::remove_file(&fs_path).is_ok() || fs::remove_dir(&fs_path).is_ok(),
            ))
        }),
        "remove_tree" => require_arity(name, args, 0).and_then(|_| {
            warn_blocking_path_operation(runtime, "remove_tree")?;
            Ok(Value::Boolean(fs::remove_dir_all(&fs_path).is_ok()))
        }),
        "spew_utf8" | "spew_utf8_async" => {
            require_arity(name, args, 1).and_then(|_| match &args[0] {
                Value::String(text) => {
                    if name.ends_with("_async") {
                        Ok(async_write_task(
                            runtime,
                            fs_path.clone(),
                            text.clone(),
                            false,
                        ))
                    } else {
                        warn_blocking_path_operation(runtime, "spew_utf8")?;
                        fs::write(&fs_path, text).map_err(|err| io_path_error(&fs_path, err))?;
                        Ok(Value::Null)
                    }
                }
                other => Err(ZuzuRustError::thrown(format!(
                    "TypeException: Path.spew_utf8 expects String, got {}",
                    runtime.typeof_name(other)
                ))),
            })
        }
        "slurp_utf8" | "slurp_utf8_async" => require_arity(name, args, 0).and_then(|_| {
            if name.ends_with("_async") {
                Ok(async_read_utf8_task(runtime, fs_path.clone()))
            } else {
                warn_blocking_path_operation(runtime, "slurp_utf8")?;
                Ok(Value::String(fs::read_to_string(&fs_path).map_err(
                    |err| ZuzuRustError::runtime(format!("IOError: {err}")),
                )?))
            }
        }),
        "append_utf8" | "append_utf8_async" => {
            require_arity(name, args, 1).and_then(|_| match &args[0] {
                Value::String(text) => {
                    if name.ends_with("_async") {
                        Ok(async_write_task(
                            runtime,
                            fs_path.clone(),
                            text.clone(),
                            true,
                        ))
                    } else {
                        warn_blocking_path_operation(runtime, "append_utf8")?;
                        let mut file = fs::OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(&fs_path)
                            .map_err(|err| io_path_error(&fs_path, err))?;
                        use std::io::Write;
                        file.write_all(text.as_bytes())
                            .map_err(|err| io_path_error(&fs_path, err))?;
                        Ok(Value::Null)
                    }
                }
                other => Err(ZuzuRustError::thrown(format!(
                    "TypeException: Path.append_utf8 expects String, got {}",
                    runtime.typeof_name(other)
                ))),
            })
        }
        "spew" | "spew_async" => require_arity(name, args, 1).and_then(|_| match &args[0] {
            Value::BinaryString(bytes) => {
                if name.ends_with("_async") {
                    Ok(async_write_bytes_task(
                        runtime,
                        fs_path.clone(),
                        bytes.clone(),
                        false,
                    ))
                } else {
                    warn_blocking_path_operation(runtime, "spew")?;
                    fs::write(&fs_path, bytes).map_err(|err| io_path_error(&fs_path, err))?;
                    Ok(Value::Null)
                }
            }
            other => Err(ZuzuRustError::thrown(format!(
                "TypeException: Path.spew expects BinaryString, got {}",
                runtime.typeof_name(other)
            ))),
        }),
        "append" | "append_async" => require_arity(name, args, 1).and_then(|_| match &args[0] {
            Value::BinaryString(bytes) => {
                if name.ends_with("_async") {
                    Ok(async_write_bytes_task(
                        runtime,
                        fs_path.clone(),
                        bytes.clone(),
                        true,
                    ))
                } else {
                    warn_blocking_path_operation(runtime, "append")?;
                    let mut file = fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&fs_path)
                        .map_err(|err| ZuzuRustError::runtime(format!("IOError: {err}")))?;
                    use std::io::Write;
                    file.write_all(bytes)
                        .map_err(|err| ZuzuRustError::runtime(format!("IOError: {err}")))?;
                    Ok(Value::Null)
                }
            }
            other => Err(ZuzuRustError::thrown(format!(
                "TypeException: Path.append expects BinaryString, got {}",
                runtime.typeof_name(other)
            ))),
        }),
        "slurp" | "slurp_async" => require_arity(name, args, 0).and_then(|_| {
            if name.ends_with("_async") {
                Ok(async_read_bytes_task(runtime, fs_path.clone()))
            } else {
                warn_blocking_path_operation(runtime, "slurp")?;
                Ok(Value::BinaryString(fs::read(&fs_path).map_err(|err| {
                    ZuzuRustError::runtime(format!("IOError: {err}"))
                })?))
            }
        }),
        "each_line" => {
            if args.is_empty() || args.len() > 2 {
                Err(ZuzuRustError::runtime(
                    "each_line() expects one or two arguments",
                ))
            } else {
                let callback = args[0].clone();
                let raw = args.get(1).map(|value| value.is_truthy()).unwrap_or(false);
                if let Err(err) = warn_blocking_path_operation(runtime, "each_line") {
                    return Some(Err(err));
                }
                let file = match fs::File::open(&fs_path) {
                    Ok(file) => file,
                    Err(err) => {
                        return Some(Err(ZuzuRustError::runtime(format!("IOError: {err}"))))
                    }
                };
                let reader = BufReader::new(file);
                for line in reader.split(b'\n') {
                    let mut payload = match line {
                        Ok(payload) => payload,
                        Err(err) => {
                            return Some(Err(ZuzuRustError::runtime(format!("IOError: {err}"))))
                        }
                    };
                    payload.push(b'\n');
                    let line_value = if raw {
                        Value::BinaryString(payload)
                    } else {
                        Value::String(String::from_utf8_lossy(&payload).to_string())
                    };
                    if let Err(err) =
                        runtime.call_value(callback.clone(), vec![line_value], Vec::new())
                    {
                        return Some(Err(err));
                    }
                }
                Ok(path_object(path))
            }
        }
        "next_line" => {
            if args.len() > 1 {
                Err(ZuzuRustError::runtime(
                    "next_line() expects zero or one argument",
                ))
            } else {
                let raw = args.first().map(|value| value.is_truthy()).unwrap_or(false);
                if let Err(err) = warn_blocking_path_operation(runtime, "next_line") {
                    return Some(Err(err));
                }
                let bytes = match fs::read(&fs_path) {
                    Ok(bytes) => bytes,
                    Err(err) => {
                        return Some(Err(ZuzuRustError::runtime(format!("IOError: {err}"))))
                    }
                };
                let lines = bytes
                    .split_inclusive(|byte| *byte == b'\n')
                    .map(|chunk| {
                        if raw {
                            Value::BinaryString(chunk.to_vec())
                        } else {
                            Value::String(String::from_utf8_lossy(chunk).to_string())
                        }
                    })
                    .collect::<Vec<_>>();
                let key = path_cursor_key(&fs_path, raw);
                let cursor = runtime
                    .path_line_cursors
                    .borrow()
                    .get(&key)
                    .copied()
                    .unwrap_or(0);
                let line = lines.get(cursor).cloned().unwrap_or(Value::Null);
                runtime
                    .path_line_cursors
                    .borrow_mut()
                    .insert(key, cursor + 1);
                Ok(line)
            }
        }
        "lines" | "lines_async" => require_arity(name, args, 0).and_then(|_| {
            if name.ends_with("_async") {
                Ok(async_lines_task(runtime, fs_path.clone(), true))
            } else {
                warn_blocking_path_operation(runtime, "lines")?;
                let bytes = fs::read(&fs_path)
                    .map_err(|err| ZuzuRustError::runtime(format!("IOError: {err}")))?;
                Ok(Value::Array(binary_lines(bytes)))
            }
        }),
        "lines_utf8" | "lines_utf8_async" => require_arity(name, args, 0).and_then(|_| {
            if name.ends_with("_async") {
                Ok(async_lines_task(runtime, fs_path.clone(), false))
            } else {
                warn_blocking_path_operation(runtime, "lines_utf8")?;
                let bytes = fs::read(&fs_path)
                    .map_err(|err| ZuzuRustError::runtime(format!("IOError: {err}")))?;
                Ok(Value::Array(utf8_lines(bytes)))
            }
        }),
        "children" => require_arity(name, args, 0).and_then(|_| {
            warn_blocking_path_operation(runtime, "children")?;
            let mut out = Vec::new();
            if let Ok(entries) = fs::read_dir(&fs_path) {
                for entry in entries.flatten() {
                    let entry_path = entry.path();
                    let display = entry_path
                        .strip_prefix(repo_root(runtime))
                        .unwrap_or(&entry_path)
                        .to_path_buf();
                    out.push(path_object(display));
                }
            }
            Ok(Value::Array(out))
        }),
        "iterator" => require_arity(name, args, 0).and_then(|_| {
            warn_blocking_path_operation(runtime, "iterator")?;
            let mut out = Vec::new();
            if let Ok(entries) = fs::read_dir(&fs_path) {
                for entry in entries.flatten() {
                    let entry_path = entry.path();
                    let display = entry_path
                        .strip_prefix(repo_root(runtime))
                        .unwrap_or(&entry_path)
                        .to_path_buf();
                    out.push(path_object(display));
                }
            }
            Ok(Value::Iterator(Rc::new(RefCell::new(
                super::super::IteratorState {
                    items: out,
                    index: 0,
                },
            ))))
        }),
        "canonpath" | "realpath" => require_arity(name, args, 0).and_then(|_| {
            let canonical = fs::canonicalize(&fs_path).unwrap_or_else(|_| fs_path.clone());
            let display = canonical
                .strip_prefix(repo_root(runtime))
                .unwrap_or(&canonical)
                .to_path_buf();
            Ok(path_object(display))
        }),
        "subsumes" => require_arity(name, args, 1).and_then(|_| {
            let other = resolve_fs_path(runtime, &path_buf_from_value(&args[0]));
            let base = fs::canonicalize(&fs_path).unwrap_or(fs_path.clone());
            let probe = fs::canonicalize(&other).unwrap_or(other);
            Ok(Value::Boolean(probe.starts_with(base)))
        }),
        "volume" => require_arity(name, args, 0).and_then(|_| Ok(Value::String(String::new()))),
        _ => return None,
    };
    Some(value)
}

fn call_standard_stream_method(
    runtime: &Runtime,
    class_name: &str,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    match class_name {
        "StandardOutputStream" | "StandardErrorStream" => match name {
            "print" | "say" => Some((|| {
                let mut text = args
                    .iter()
                    .map(|value| runtime.render_value(value))
                    .collect::<Result<Vec<_>>>()?
                    .join("");
                if name == "say" {
                    text.push('\n');
                }
                if class_name == "StandardErrorStream" {
                    runtime.emit_stderr(&text)?;
                } else {
                    runtime.emit_stdout(&text)?;
                }
                Ok(Value::Null)
            })()),
            _ => None,
        },
        "StandardInputStream" => match name {
            "next_line" => Some((|| {
                if args.len() > 1 {
                    return Err(ZuzuRustError::runtime(
                        "next_line() expects zero or one argument",
                    ));
                }
                let raw = args.first().map(|value| value.is_truthy()).unwrap_or(false);
                let mut line = String::new();
                let count = std::io::stdin()
                    .lock()
                    .read_line(&mut line)
                    .map_err(|err| ZuzuRustError::runtime(format!("IOError: {err}")))?;
                if count == 0 {
                    return Ok(Value::Null);
                }
                if raw {
                    Ok(Value::BinaryString(line.into_bytes()))
                } else {
                    Ok(Value::String(line))
                }
            })()),
            "each_line" => Some((|| {
                if args.is_empty() || args.len() > 2 {
                    return Err(ZuzuRustError::runtime(
                        "each_line() expects one or two arguments",
                    ));
                }
                let callback = args[0].clone();
                let raw = args.get(1).map(|value| value.is_truthy()).unwrap_or(false);
                let stdin = std::io::stdin();
                let mut reader = stdin.lock();
                loop {
                    let mut line = String::new();
                    let count = reader
                        .read_line(&mut line)
                        .map_err(|err| ZuzuRustError::runtime(format!("IOError: {err}")))?;
                    if count == 0 {
                        break;
                    }
                    let value = if raw {
                        Value::BinaryString(line.into_bytes())
                    } else {
                        Value::String(line)
                    };
                    runtime.call_value(callback.clone(), vec![value], Vec::new())?;
                }
                Ok(stream_object("StandardInputStream"))
            })()),
            _ => None,
        },
        _ => None,
    }
}

fn async_write_task(runtime: &Runtime, path: PathBuf, text: String, append: bool) -> Value {
    async_write_bytes_task(runtime, path, text.into_bytes(), append)
}

fn async_write_bytes_task(runtime: &Runtime, path: PathBuf, bytes: Vec<u8>, append: bool) -> Value {
    let cancel_requested = Rc::new(Cell::new(false));
    let task_cancel = Rc::clone(&cancel_requested);
    let future = async move {
        reject_if_cancelled(&task_cancel)?;
        if append {
            let mut file = tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .await
                .map_err(|err| ZuzuRustError::runtime(format!("IOError: {err}")))?;
            file.write_all(&bytes)
                .await
                .map_err(|err| ZuzuRustError::runtime(format!("IOError: {err}")))?;
        } else {
            tokio::fs::write(&path, bytes)
                .await
                .map_err(|err| ZuzuRustError::runtime(format!("IOError: {err}")))?;
        }
        reject_if_cancelled(&task_cancel)?;
        Ok(Value::Null)
    };
    runtime.task_native_async(future, Some(cancel_requested))
}

fn async_read_utf8_task(runtime: &Runtime, path: PathBuf) -> Value {
    let cancel_requested = Rc::new(Cell::new(false));
    let task_cancel = Rc::clone(&cancel_requested);
    let future = async move {
        reject_if_cancelled(&task_cancel)?;
        let text = tokio::fs::read_to_string(&path)
            .await
            .map_err(|err| ZuzuRustError::runtime(format!("IOError: {err}")))?;
        reject_if_cancelled(&task_cancel)?;
        Ok(Value::String(text))
    };
    runtime.task_native_async(future, Some(cancel_requested))
}

fn async_read_bytes_task(runtime: &Runtime, path: PathBuf) -> Value {
    let cancel_requested = Rc::new(Cell::new(false));
    let task_cancel = Rc::clone(&cancel_requested);
    let future = async move {
        reject_if_cancelled(&task_cancel)?;
        let bytes = tokio::fs::read(&path)
            .await
            .map_err(|err| ZuzuRustError::runtime(format!("IOError: {err}")))?;
        reject_if_cancelled(&task_cancel)?;
        Ok(Value::BinaryString(bytes))
    };
    runtime.task_native_async(future, Some(cancel_requested))
}

fn async_lines_task(runtime: &Runtime, path: PathBuf, raw: bool) -> Value {
    let cancel_requested = Rc::new(Cell::new(false));
    let task_cancel = Rc::clone(&cancel_requested);
    let future = async move {
        reject_if_cancelled(&task_cancel)?;
        let bytes = tokio::fs::read(&path)
            .await
            .map_err(|err| ZuzuRustError::runtime(format!("IOError: {err}")))?;
        reject_if_cancelled(&task_cancel)?;
        Ok(Value::Array(if raw {
            binary_lines(bytes)
        } else {
            utf8_lines(bytes)
        }))
    };
    runtime.task_native_async(future, Some(cancel_requested))
}

fn reject_if_cancelled(cancel_requested: &Cell<bool>) -> Result<()> {
    if cancel_requested.get() {
        Err(ZuzuRustError::runtime("filesystem task cancelled"))
    } else {
        Ok(())
    }
}

fn binary_lines(bytes: Vec<u8>) -> Vec<Value> {
    bytes
        .split_inclusive(|byte| *byte == b'\n')
        .map(|chunk| Value::BinaryString(chunk.to_vec()))
        .collect()
}

fn utf8_lines(bytes: Vec<u8>) -> Vec<Value> {
    bytes
        .split_inclusive(|byte| *byte == b'\n')
        .map(|chunk| Value::String(String::from_utf8_lossy(chunk).to_string()))
        .collect()
}

fn glob_matches(pattern: &str, text: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some((prefix, suffix)) = pattern.split_once('*') {
        return text.starts_with(prefix) && text.ends_with(suffix);
    }
    pattern == text
}

pub(super) fn call(runtime: &Runtime, name: &str, args: &[Value]) -> Option<Result<Value>> {
    let value = match name {
        "listen_tcp" => {
            let host = match string_arg(runtime, args, 0, "127.0.0.1") {
                Ok(host) => host,
                Err(err) => return Some(Err(err)),
            };
            let port = numeric_arg(runtime, args, 1, 0.0) as u16;
            let listener = match std::net::TcpListener::bind((host.as_str(), port)) {
                Ok(listener) => listener,
                Err(_) => return Some(Ok(Value::Null)),
            };
            let local_addr = match listener.local_addr() {
                Ok(addr) => addr,
                Err(_) => return Some(Ok(Value::Null)),
            };
            let mut state = runtime.socket_state.borrow_mut();
            let server_id = alloc_socket_id(&mut state, "tcp-server");
            state.tcp_servers.insert(server_id.clone(), listener);
            Ok(socket_object(
                "TCPServer",
                &server_id,
                HashMap::from([
                    (
                        "host".to_owned(),
                        Value::String(local_addr.ip().to_string()),
                    ),
                    ("port".to_owned(), Value::Number(local_addr.port() as f64)),
                ]),
            ))
        }
        "connect_tcp" => {
            let host = match string_arg(runtime, args, 0, "127.0.0.1") {
                Ok(host) => host,
                Err(err) => return Some(Err(err)),
            };
            let port = numeric_arg(runtime, args, 1, 0.0) as u16;
            let stream = match std::net::TcpStream::connect((host.as_str(), port)) {
                Ok(stream) => stream,
                Err(_) => return Some(Ok(Value::Null)),
            };
            let _ = stream.set_nodelay(true);
            let mut state = runtime.socket_state.borrow_mut();
            let client_id = alloc_socket_id(&mut state, "tcp-sock");
            state.tcp_sockets.insert(
                client_id.clone(),
                super::super::TcpSocketState {
                    stream,
                    read_buffer: Vec::new(),
                },
            );
            Ok(socket_object("TCPSocket", &client_id, HashMap::new()))
        }
        "bind_udp" => {
            let host = match string_arg(runtime, args, 0, "127.0.0.1") {
                Ok(host) => host,
                Err(err) => return Some(Err(err)),
            };
            let port = numeric_arg(runtime, args, 1, 0.0) as u16;
            let socket = match std::net::UdpSocket::bind((host.as_str(), port)) {
                Ok(socket) => socket,
                Err(_) => return Some(Ok(Value::Null)),
            };
            let local_addr = match socket.local_addr() {
                Ok(addr) => addr,
                Err(_) => return Some(Ok(Value::Null)),
            };
            let mut state = runtime.socket_state.borrow_mut();
            let socket_id = alloc_socket_id(&mut state, "udp-sock");
            state.udp_sockets.insert(socket_id.clone(), socket);
            Ok(socket_object(
                "UDPSocket",
                &socket_id,
                HashMap::from([
                    (
                        "host".to_owned(),
                        Value::String(local_addr.ip().to_string()),
                    ),
                    ("port".to_owned(), Value::Number(local_addr.port() as f64)),
                ]),
            ))
        }
        "connect_udp" => {
            let host = match string_arg(runtime, args, 0, "127.0.0.1") {
                Ok(host) => host,
                Err(err) => return Some(Err(err)),
            };
            let port = numeric_arg(runtime, args, 1, 0.0) as u16;
            let bind_addr = if host.contains(':') {
                "[::]:0"
            } else {
                "0.0.0.0:0"
            };
            let socket = match std::net::UdpSocket::bind(bind_addr) {
                Ok(socket) => socket,
                Err(_) => return Some(Ok(Value::Null)),
            };
            if socket.connect((host.as_str(), port)).is_err() {
                return Some(Ok(Value::Null));
            }
            let local_addr = match socket.local_addr() {
                Ok(addr) => addr,
                Err(_) => return Some(Ok(Value::Null)),
            };
            let mut state = runtime.socket_state.borrow_mut();
            let socket_id = alloc_socket_id(&mut state, "udp-sock");
            state.udp_sockets.insert(socket_id.clone(), socket);
            Ok(socket_object(
                "UDPSocket",
                &socket_id,
                HashMap::from([
                    (
                        "host".to_owned(),
                        Value::String(local_addr.ip().to_string()),
                    ),
                    ("port".to_owned(), Value::Number(local_addr.port() as f64)),
                ]),
            ))
        }
        "listen_unix" => {
            let path = match args.first() {
                Some(value) => match runtime.render_value(value) {
                    Ok(path) => path,
                    Err(err) => return Some(Err(err)),
                },
                None => String::new(),
            };
            #[cfg(unix)]
            {
                if path.is_empty() {
                    return Some(Ok(Value::Null));
                }
                let _ = fs::remove_file(&path);
                let listener = match std::os::unix::net::UnixListener::bind(&path) {
                    Ok(listener) => listener,
                    Err(_) => return Some(Ok(Value::Null)),
                };
                let mut state = runtime.socket_state.borrow_mut();
                let server_id = alloc_socket_id(&mut state, "unix-server");
                state.unix_servers.insert(
                    server_id.clone(),
                    super::super::UnixServerState {
                        listener,
                        path: PathBuf::from(&path),
                    },
                );
                Ok(socket_object(
                    "UnixServer",
                    &server_id,
                    HashMap::from([("path".to_owned(), Value::String(path))]),
                ))
            }
            #[cfg(not(unix))]
            {
                let _ = path;
                Ok(Value::Null)
            }
        }
        "connect_unix" => {
            let path = match args.first() {
                Some(value) => match runtime.render_value(value) {
                    Ok(path) => path,
                    Err(err) => return Some(Err(err)),
                },
                None => String::new(),
            };
            #[cfg(unix)]
            {
                let stream = match std::os::unix::net::UnixStream::connect(&path) {
                    Ok(stream) => stream,
                    Err(_) => return Some(Ok(Value::Null)),
                };
                let mut state = runtime.socket_state.borrow_mut();
                let socket_id = alloc_socket_id(&mut state, "unix-sock");
                state.unix_sockets.insert(
                    socket_id.clone(),
                    super::super::UnixSocketState {
                        stream,
                        read_buffer: Vec::new(),
                    },
                );
                Ok(socket_object("UnixSocket", &socket_id, HashMap::new()))
            }
            #[cfg(not(unix))]
            {
                let _ = path;
                Ok(Value::Null)
            }
        }
        _ => return None,
    };
    Some(value)
}

fn string_arg(runtime: &Runtime, args: &[Value], index: usize, default: &str) -> Result<String> {
    match args.get(index) {
        Some(Value::Null) | None => Ok(default.to_owned()),
        Some(value) => runtime.render_value(value),
    }
}

fn numeric_arg(runtime: &Runtime, args: &[Value], index: usize, default: f64) -> f64 {
    args.get(index)
        .and_then(|value| runtime.value_to_number(value).ok())
        .unwrap_or(default)
}

fn socket_payload(runtime: &Runtime, values: &[Value]) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    for value in values {
        match value {
            Value::BinaryString(bytes) => payload.extend_from_slice(bytes),
            other => payload.extend_from_slice(runtime.render_value(other)?.as_bytes()),
        }
    }
    Ok(payload)
}

fn socket_text_value(bytes: Vec<u8>) -> Value {
    Value::String(String::from_utf8_lossy(&bytes).to_string())
}

fn socket_io_error(err: std::io::Error) -> ZuzuRustError {
    ZuzuRustError::runtime(format!("SocketError: {err}"))
}

fn read_stream_bytes<S: Read>(
    stream: &mut S,
    read_buffer: &mut Vec<u8>,
    max_bytes: usize,
) -> std::io::Result<Option<Vec<u8>>> {
    let max_bytes = max_bytes.max(1);
    if !read_buffer.is_empty() {
        let count = max_bytes.min(read_buffer.len());
        return Ok(Some(read_buffer.drain(..count).collect()));
    }

    let mut buffer = vec![0; max_bytes];
    let count = stream.read(&mut buffer)?;
    if count == 0 {
        return Ok(None);
    }
    buffer.truncate(count);
    Ok(Some(buffer))
}

fn read_stream_line<S: Read>(
    stream: &mut S,
    read_buffer: &mut Vec<u8>,
) -> std::io::Result<Option<Vec<u8>>> {
    loop {
        if let Some(newline) = read_buffer.iter().position(|byte| *byte == b'\n') {
            return Ok(Some(read_buffer.drain(..=newline).collect()));
        }

        let mut byte = [0_u8; 1];
        let count = stream.read(&mut byte)?;
        if count == 0 {
            if read_buffer.is_empty() {
                return Ok(None);
            }
            return Ok(Some(read_buffer.drain(..).collect()));
        }
        read_buffer.push(byte[0]);
    }
}

fn udp_socket_id_from_value(value: &Value) -> Option<String> {
    match value {
        Value::Object(object) if object.borrow().class.name == "UDPSocket" => object
            .borrow()
            .builtin_value
            .as_ref()
            .map(socket_id_from_value),
        _ => None,
    }
}

fn socket_object(class_name: &str, id: &str, extra_fields: HashMap<String, Value>) -> Value {
    let mut fields = extra_fields;
    fields.insert("id".to_owned(), Value::String(id.to_owned()));
    Value::Object(Rc::new(RefCell::new(ObjectValue {
        class: Rc::new(UserClassValue {
            name: class_name.to_owned(),
            base: None,
            traits: Vec::<Rc<TraitValue>>::new(),
            fields: Vec::new(),
            methods: HashMap::<String, Rc<MethodValue>>::new(),
            static_methods: HashMap::<String, Rc<MethodValue>>::new(),
            nested_classes: HashMap::new(),
            source_decl: None,
            closure_env: None,
        }),
        fields,
        weak_fields: std::collections::HashSet::new(),
        builtin_value: Some(Value::Dict(HashMap::from([(
            "id".to_owned(),
            Value::String(id.to_owned()),
        )]))),
    })))
}

fn socket_id_from_value(builtin_value: &Value) -> String {
    match builtin_value {
        Value::Dict(fields) => fields
            .get("id")
            .map(|value| value.render())
            .unwrap_or_default(),
        _ => String::new(),
    }
}

fn alloc_socket_id(state: &mut super::super::SocketState, prefix: &str) -> String {
    state.next_socket_id = state.next_socket_id.saturating_add(1);
    format!("{prefix}-{}", state.next_socket_id)
}

fn call_tcp_socket_method(
    runtime: &Runtime,
    socket_id: &str,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    match name {
        "read" => Some((|| {
            if args.len() > 1 {
                return Err(ZuzuRustError::runtime(
                    "read() expects zero or one argument",
                ));
            }
            let max_bytes = numeric_arg(runtime, args, 0, 4096.0) as usize;
            let mut state = runtime.socket_state.borrow_mut();
            let Some(socket) = state.tcp_sockets.get_mut(socket_id) else {
                return Ok(Value::Null);
            };
            read_stream_bytes(&mut socket.stream, &mut socket.read_buffer, max_bytes)
                .map(|bytes| bytes.map(socket_text_value).unwrap_or(Value::Null))
                .map_err(socket_io_error)
        })()),
        "write" => Some((|| {
            let payload = socket_payload(runtime, args)?;
            let mut state = runtime.socket_state.borrow_mut();
            let Some(socket) = state.tcp_sockets.get_mut(socket_id) else {
                return Ok(Value::Number(0.0));
            };
            socket.stream.write_all(&payload).map_err(socket_io_error)?;
            socket.stream.flush().map_err(socket_io_error)?;
            Ok(Value::Number(payload.len() as f64))
        })()),
        "print" | "say" => Some((|| {
            let mut payload = socket_payload(runtime, args)?;
            if name == "say" {
                payload.push(b'\n');
            }
            let mut state = runtime.socket_state.borrow_mut();
            let Some(socket) = state.tcp_sockets.get_mut(socket_id) else {
                return Ok(Value::Null);
            };
            socket.stream.write_all(&payload).map_err(socket_io_error)?;
            socket.stream.flush().map_err(socket_io_error)?;
            Ok(Value::Null)
        })()),
        "next_line" => Some((|| {
            if args.len() > 1 {
                return Err(ZuzuRustError::runtime(
                    "next_line() expects zero or one argument",
                ));
            }
            let mut state = runtime.socket_state.borrow_mut();
            let Some(socket) = state.tcp_sockets.get_mut(socket_id) else {
                return Ok(Value::Null);
            };
            read_stream_line(&mut socket.stream, &mut socket.read_buffer)
                .map(|line| line.map(socket_text_value).unwrap_or(Value::Null))
                .map_err(socket_io_error)
        })()),
        "each_line" => Some((|| {
            if args.is_empty() || args.len() > 2 {
                return Err(ZuzuRustError::runtime(
                    "each_line() expects one or two arguments",
                ));
            }
            let callback = args[0].clone();
            loop {
                let line = {
                    let mut state = runtime.socket_state.borrow_mut();
                    let Some(socket) = state.tcp_sockets.get_mut(socket_id) else {
                        break;
                    };
                    read_stream_line(&mut socket.stream, &mut socket.read_buffer)
                        .map_err(socket_io_error)?
                };
                let Some(line) = line else {
                    break;
                };
                runtime.call_value(callback.clone(), vec![socket_text_value(line)], Vec::new())?;
            }
            Ok(socket_object("TCPSocket", socket_id, HashMap::new()))
        })()),
        "close" => Some(Ok({
            let socket = runtime
                .socket_state
                .borrow_mut()
                .tcp_sockets
                .remove(socket_id);
            if let Some(socket) = socket {
                let _ = socket.stream.shutdown(Shutdown::Both);
                Value::Number(1.0)
            } else {
                Value::Number(0.0)
            }
        })),
        "is_open" => Some(Ok(Value::Number(
            if runtime
                .socket_state
                .borrow()
                .tcp_sockets
                .contains_key(socket_id)
            {
                1.0
            } else {
                0.0
            },
        ))),
        "peer_host" => Some(Ok(runtime
            .socket_state
            .borrow()
            .tcp_sockets
            .get(socket_id)
            .and_then(|socket| socket.stream.peer_addr().ok())
            .map(|addr| Value::String(addr.ip().to_string()))
            .unwrap_or(Value::Null))),
        "peer_port" => Some(Ok(runtime
            .socket_state
            .borrow()
            .tcp_sockets
            .get(socket_id)
            .and_then(|socket| socket.stream.peer_addr().ok())
            .map(|addr| Value::Number(addr.port() as f64))
            .unwrap_or(Value::Null))),
        _ => None,
    }
}

#[cfg(unix)]
fn call_unix_socket_method(
    runtime: &Runtime,
    socket_id: &str,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    match name {
        "read" => Some((|| {
            if args.len() > 1 {
                return Err(ZuzuRustError::runtime(
                    "read() expects zero or one argument",
                ));
            }
            let max_bytes = numeric_arg(runtime, args, 0, 4096.0) as usize;
            let mut state = runtime.socket_state.borrow_mut();
            let Some(socket) = state.unix_sockets.get_mut(socket_id) else {
                return Ok(Value::Null);
            };
            read_stream_bytes(&mut socket.stream, &mut socket.read_buffer, max_bytes)
                .map(|bytes| bytes.map(socket_text_value).unwrap_or(Value::Null))
                .map_err(socket_io_error)
        })()),
        "write" => Some((|| {
            let payload = socket_payload(runtime, args)?;
            let mut state = runtime.socket_state.borrow_mut();
            let Some(socket) = state.unix_sockets.get_mut(socket_id) else {
                return Ok(Value::Number(0.0));
            };
            socket.stream.write_all(&payload).map_err(socket_io_error)?;
            socket.stream.flush().map_err(socket_io_error)?;
            Ok(Value::Number(payload.len() as f64))
        })()),
        "print" | "say" => Some((|| {
            let mut payload = socket_payload(runtime, args)?;
            if name == "say" {
                payload.push(b'\n');
            }
            let mut state = runtime.socket_state.borrow_mut();
            let Some(socket) = state.unix_sockets.get_mut(socket_id) else {
                return Ok(Value::Null);
            };
            socket.stream.write_all(&payload).map_err(socket_io_error)?;
            socket.stream.flush().map_err(socket_io_error)?;
            Ok(Value::Null)
        })()),
        "next_line" => Some((|| {
            if args.len() > 1 {
                return Err(ZuzuRustError::runtime(
                    "next_line() expects zero or one argument",
                ));
            }
            let mut state = runtime.socket_state.borrow_mut();
            let Some(socket) = state.unix_sockets.get_mut(socket_id) else {
                return Ok(Value::Null);
            };
            read_stream_line(&mut socket.stream, &mut socket.read_buffer)
                .map(|line| line.map(socket_text_value).unwrap_or(Value::Null))
                .map_err(socket_io_error)
        })()),
        "each_line" => Some((|| {
            if args.is_empty() || args.len() > 2 {
                return Err(ZuzuRustError::runtime(
                    "each_line() expects one or two arguments",
                ));
            }
            let callback = args[0].clone();
            loop {
                let line = {
                    let mut state = runtime.socket_state.borrow_mut();
                    let Some(socket) = state.unix_sockets.get_mut(socket_id) else {
                        break;
                    };
                    read_stream_line(&mut socket.stream, &mut socket.read_buffer)
                        .map_err(socket_io_error)?
                };
                let Some(line) = line else {
                    break;
                };
                runtime.call_value(callback.clone(), vec![socket_text_value(line)], Vec::new())?;
            }
            Ok(socket_object("UnixSocket", socket_id, HashMap::new()))
        })()),
        "close" => Some(Ok({
            let socket = runtime
                .socket_state
                .borrow_mut()
                .unix_sockets
                .remove(socket_id);
            if let Some(socket) = socket {
                let _ = socket.stream.shutdown(std::net::Shutdown::Both);
                Value::Number(1.0)
            } else {
                Value::Number(0.0)
            }
        })),
        "is_open" => Some(Ok(Value::Number(
            if runtime
                .socket_state
                .borrow()
                .unix_sockets
                .contains_key(socket_id)
            {
                1.0
            } else {
                0.0
            },
        ))),
        _ => None,
    }
}

fn call_socket_object_method(
    runtime: &Runtime,
    class_name: &str,
    builtin_value: &Value,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    let socket_id = socket_id_from_value(builtin_value);
    match class_name {
        "TCPServer" => match name {
            "port" => Some(Ok(runtime
                .socket_state
                .borrow()
                .tcp_servers
                .get(&socket_id)
                .and_then(|server| server.local_addr().ok())
                .map(|addr| Value::Number(addr.port() as f64))
                .unwrap_or(Value::Null))),
            "host" => Some(Ok(runtime
                .socket_state
                .borrow()
                .tcp_servers
                .get(&socket_id)
                .and_then(|server| server.local_addr().ok())
                .map(|addr| Value::String(addr.ip().to_string()))
                .unwrap_or(Value::Null))),
            "accept" => Some((|| {
                let accepted = {
                    let mut state = runtime.socket_state.borrow_mut();
                    let Some(server) = state.tcp_servers.get_mut(&socket_id) else {
                        return Ok(Value::Null);
                    };
                    server.accept()
                };
                let (stream, _) = match accepted {
                    Ok(accepted) => accepted,
                    Err(_) => return Ok(Value::Null),
                };
                let _ = stream.set_nodelay(true);
                let mut state = runtime.socket_state.borrow_mut();
                let peer_id = alloc_socket_id(&mut state, "tcp-sock");
                state.tcp_sockets.insert(
                    peer_id.clone(),
                    super::super::TcpSocketState {
                        stream,
                        read_buffer: Vec::new(),
                    },
                );
                Ok(socket_object("TCPSocket", &peer_id, HashMap::new()))
            })()),
            "close" => Some(Ok(
                if runtime
                    .socket_state
                    .borrow_mut()
                    .tcp_servers
                    .remove(&socket_id)
                    .is_some()
                {
                    Value::Number(1.0)
                } else {
                    Value::Number(0.0)
                },
            )),
            "is_open" => Some(Ok(Value::Number(
                if runtime
                    .socket_state
                    .borrow()
                    .tcp_servers
                    .contains_key(&socket_id)
                {
                    1.0
                } else {
                    0.0
                },
            ))),
            _ => None,
        },
        "TCPSocket" => call_tcp_socket_method(runtime, &socket_id, name, args),
        "UDPSocket" => match name {
            "port" => Some(Ok(runtime
                .socket_state
                .borrow()
                .udp_sockets
                .get(&socket_id)
                .and_then(|socket| socket.local_addr().ok())
                .map(|addr| Value::Number(addr.port() as f64))
                .unwrap_or(Value::Null))),
            "host" => Some(Ok(runtime
                .socket_state
                .borrow()
                .udp_sockets
                .get(&socket_id)
                .and_then(|socket| socket.local_addr().ok())
                .map(|addr| Value::String(addr.ip().to_string()))
                .unwrap_or(Value::Null))),
            "send" => Some((|| {
                if args.is_empty() {
                    return Err(ZuzuRustError::runtime(
                        "send() expects at least one argument",
                    ));
                }
                let payload = socket_payload(runtime, &args[..1])?;
                let flags_or_peer = args.get(1);
                let mut state = runtime.socket_state.borrow_mut();
                let peer_addr =
                    flags_or_peer
                        .and_then(udp_socket_id_from_value)
                        .and_then(|peer_id| {
                            state
                                .udp_sockets
                                .get(&peer_id)
                                .and_then(|socket| socket.peer_addr().ok())
                        });
                let Some(socket) = state.udp_sockets.get_mut(&socket_id) else {
                    return Ok(Value::Number(0.0));
                };
                let written = if let Some(peer_addr) = peer_addr {
                    socket.send_to(&payload, peer_addr)
                } else {
                    socket.send(&payload)
                }
                .unwrap_or(0);
                Ok(Value::Number(written as f64))
            })()),
            "recv" => Some((|| {
                if args.len() > 2 {
                    return Err(ZuzuRustError::runtime(
                        "recv() expects zero, one, or two arguments",
                    ));
                }
                let max_bytes = numeric_arg(runtime, args, 0, 65536.0).max(1.0) as usize;
                let mut buffer = vec![0; max_bytes];
                let mut state = runtime.socket_state.borrow_mut();
                let Some(socket) = state.udp_sockets.get_mut(&socket_id) else {
                    return Ok(Value::Null);
                };
                match socket.recv(&mut buffer) {
                    Ok(count) => {
                        buffer.truncate(count);
                        Ok(socket_text_value(buffer))
                    }
                    Err(_) => Ok(Value::Null),
                }
            })()),
            "close" => Some(Ok(
                if runtime
                    .socket_state
                    .borrow_mut()
                    .udp_sockets
                    .remove(&socket_id)
                    .is_some()
                {
                    Value::Number(1.0)
                } else {
                    Value::Number(0.0)
                },
            )),
            "is_open" => Some(Ok(Value::Number(
                if runtime
                    .socket_state
                    .borrow()
                    .udp_sockets
                    .contains_key(&socket_id)
                {
                    1.0
                } else {
                    0.0
                },
            ))),
            _ => None,
        },
        "UnixServer" => {
            #[cfg(unix)]
            {
                match name {
                    "accept" => Some((|| {
                        let accepted = {
                            let mut state = runtime.socket_state.borrow_mut();
                            let Some(server) = state.unix_servers.get_mut(&socket_id) else {
                                return Ok(Value::Null);
                            };
                            server.listener.accept()
                        };
                        let (stream, _) = match accepted {
                            Ok(accepted) => accepted,
                            Err(_) => return Ok(Value::Null),
                        };
                        let mut state = runtime.socket_state.borrow_mut();
                        let peer_id = alloc_socket_id(&mut state, "unix-sock");
                        state.unix_sockets.insert(
                            peer_id.clone(),
                            super::super::UnixSocketState {
                                stream,
                                read_buffer: Vec::new(),
                            },
                        );
                        Ok(socket_object("UnixSocket", &peer_id, HashMap::new()))
                    })()),
                    "path" => Some(Ok(runtime
                        .socket_state
                        .borrow()
                        .unix_servers
                        .get(&socket_id)
                        .map(|server| Value::String(server.path.to_string_lossy().to_string()))
                        .unwrap_or(Value::Null))),
                    "close" => Some(Ok({
                        let server = runtime
                            .socket_state
                            .borrow_mut()
                            .unix_servers
                            .remove(&socket_id);
                        if let Some(server) = server {
                            let _ = fs::remove_file(&server.path);
                            Value::Number(1.0)
                        } else {
                            Value::Number(0.0)
                        }
                    })),
                    "is_open" => Some(Ok(Value::Number(
                        if runtime
                            .socket_state
                            .borrow()
                            .unix_servers
                            .contains_key(&socket_id)
                        {
                            1.0
                        } else {
                            0.0
                        },
                    ))),
                    _ => None,
                }
            }
            #[cfg(not(unix))]
            {
                let _ = (runtime, socket_id, args);
                match name {
                    "accept" | "path" | "close" | "is_open" => Some(Ok(Value::Null)),
                    _ => None,
                }
            }
        }
        "UnixSocket" => {
            #[cfg(unix)]
            {
                call_unix_socket_method(runtime, &socket_id, name, args)
            }
            #[cfg(not(unix))]
            {
                let _ = (runtime, socket_id, name, args);
                None
            }
        }
        _ => None,
    }
}

fn has_socket_object_method(class_name: &str, name: &str) -> bool {
    matches!(
        (class_name, name),
        ("TCPServer", "port")
            | ("TCPServer", "host")
            | ("TCPServer", "accept")
            | ("TCPServer", "close")
            | ("TCPServer", "is_open")
            | ("TCPSocket", "read")
            | ("TCPSocket", "write")
            | ("TCPSocket", "print")
            | ("TCPSocket", "say")
            | ("TCPSocket", "next_line")
            | ("TCPSocket", "each_line")
            | ("TCPSocket", "close")
            | ("TCPSocket", "is_open")
            | ("TCPSocket", "peer_host")
            | ("TCPSocket", "peer_port")
            | ("UDPSocket", "port")
            | ("UDPSocket", "host")
            | ("UDPSocket", "send")
            | ("UDPSocket", "recv")
            | ("UDPSocket", "close")
            | ("UDPSocket", "is_open")
            | ("UnixServer", "accept")
            | ("UnixServer", "path")
            | ("UnixServer", "close")
            | ("UnixServer", "is_open")
            | ("UnixSocket", "read")
            | ("UnixSocket", "write")
            | ("UnixSocket", "print")
            | ("UnixSocket", "say")
            | ("UnixSocket", "next_line")
            | ("UnixSocket", "each_line")
            | ("UnixSocket", "close")
            | ("UnixSocket", "is_open")
    )
}
