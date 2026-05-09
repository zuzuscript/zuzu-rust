use std::collections::HashMap;

mod archive;
mod base64;
mod bignum;
mod clib;
mod csv;
mod db;
mod digest;
mod eval;
pub(in crate::runtime) mod gui;
mod internals;
mod io;
mod json;
mod marshal;
mod math;
mod net_dns;
pub(in crate::runtime) mod net_http;
mod net_smtp;
mod net_url;
mod proc;
mod secure;
mod string;
mod task;
mod time;
mod tui;
mod worker;
mod xml;
mod yaml;

use super::{Runtime, Value};
use crate::error::Result;

pub(super) use clib::ClibState;
pub(super) use db::DbState;

pub(super) fn load_runtime_supported_module(name: &str) -> Option<HashMap<String, Value>> {
    match name {
        "std/internals" => Some(internals::exports()),
        "std/tui" => Some(tui::exports()),
        "std/string" => Some(string::exports()),
        "std/string/base64" => Some(base64::exports()),
        "std/math" => Some(math::exports()),
        "std/math/bignum" => Some(bignum::exports()),
        "std/proc" => Some(proc::exports()),
        "std/task" => Some(task::exports()),
        "std/worker" => Some(worker::exports()),
        "std/archive" => Some(archive::exports()),
        "std/eval" => Some(eval::exports()),
        "std/gui/objects" => Some(gui::exports()),
        "std/db" => Some(db::exports()),
        "std/clib" => Some(clib::exports()),
        "std/io" => Some(io::exports()),
        "std/io/socks" => Some(io::socks_exports()),
        "std/net/dns" => Some(net_dns::exports()),
        "std/net/url" => Some(net_url::exports()),
        "std/net/http" => Some(net_http::exports()),
        "std/net/smtp" => Some(net_smtp::exports()),
        "std/data/csv" => Some(csv::exports()),
        "std/data/json" => Some(json::exports()),
        "std/marshal" => Some(marshal::exports()),
        "std/data/yaml" => Some(yaml::exports()),
        "std/digest/md5" => Some(digest::md5_exports()),
        "std/digest/sha" => Some(digest::sha_exports()),
        "std/secure" => Some(secure::exports()),
        "std/time" => Some(time::exports()),
        "std/data/xml" => Some(xml::exports()),
        _ => None,
    }
}

pub(super) fn call_native_function(
    runtime: &Runtime,
    name: &str,
    args: Vec<Value>,
    named_args: Vec<(String, Value)>,
) -> Result<Value> {
    if let Some(result) = internals::call(runtime, name, &args, &named_args) {
        return result;
    }
    if let Some(result) = eval::call(runtime, name, &args, &named_args) {
        return result;
    }
    if let Some(result) = tui::call(runtime, name, &args, &named_args) {
        return result;
    }
    if !named_args.is_empty() {
        return Err(crate::error::ZuzuRustError::runtime(
            "named call arguments are not implemented for native functions",
        ));
    }
    if let Some(result) = proc::call(runtime, name, &args) {
        return result;
    }
    if let Some(result) = task::call(runtime, name, &args) {
        return result;
    }
    if let Some(result) = io::call(runtime, name, &args) {
        return result;
    }
    if let Some(result) = net_dns::call(runtime, name, &args) {
        return result;
    }
    if let Some(result) = net_url::call(runtime, name, &args) {
        return result;
    }
    if let Some(result) = string::call(runtime, name, &args) {
        return result;
    }
    if let Some(result) = gui::call(runtime, name, &args) {
        return result;
    }
    if let Some(result) = base64::call(name, &args) {
        return result;
    }
    if let Some(result) = marshal::call(runtime, name, &args) {
        return result;
    }
    if let Some(result) = digest::call(name, &args) {
        return result;
    }
    Err(crate::error::ZuzuRustError::runtime(format!(
        "unsupported native function '{}'",
        name
    )))
}

pub(super) fn call_builtin_class_method(
    runtime: &Runtime,
    class_name: &str,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    proc::call_class_method(runtime, class_name, name, args)
        .or_else(|| clib::call_class_method(runtime, class_name, name, args))
        .or_else(|| db::call_class_method(runtime, class_name, name, args))
        .or_else(|| bignum::call_class_method(runtime, class_name, name, args))
        .or_else(|| math::call_class_method(runtime, class_name, name, args))
        .or_else(|| secure::call_class_method(runtime, class_name, name, args))
        .or_else(|| net_smtp::call_class_method(class_name, name, args))
        .or_else(|| io::call_class_method(runtime, class_name, name, args))
        .or_else(|| archive::call_class_method(runtime, class_name, name, args))
        .or_else(|| xml::call_class_method(runtime, class_name, name, args))
}

pub(super) fn call_builtin_class_method_named(
    runtime: &Runtime,
    class_name: &str,
    name: &str,
    args: &[Value],
    named_args: &[(String, Value)],
) -> Option<Result<Value>> {
    worker::call_class_method(runtime, class_name, name, args, named_args)
}

pub(super) fn has_builtin_class_method(class_name: &str, name: &str) -> bool {
    worker::has_class_method(class_name, name)
        || secure::has_class_method(class_name, name)
        || net_smtp::has_class_method(class_name, name)
}

pub(super) fn call_builtin_object_method(
    runtime: &Runtime,
    object: &std::rc::Rc<std::cell::RefCell<super::ObjectValue>>,
    class_name: &str,
    builtin_value: &Value,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    net_http::call_object_method(runtime, object, class_name, builtin_value, name, args)
        .or_else(|| net_smtp::call_object_method(runtime, object, class_name, name, args))
        .or_else(|| worker::call_object_method(runtime, object, class_name, name, args))
        .or_else(|| bignum::call_object_method(runtime, class_name, builtin_value, name, args))
        .or_else(|| yaml::call_object_method(runtime, class_name, builtin_value, name, args))
        .or_else(|| {
            net_http::call_response_object_method(
                runtime,
                object,
                class_name,
                builtin_value,
                name,
                args,
            )
        })
        .or_else(|| db::call_object_method(runtime, class_name, builtin_value, name, args))
        .or_else(|| clib::call_object_method(runtime, class_name, builtin_value, name, args))
        .or_else(|| io::call_object_method(runtime, class_name, builtin_value, name, args))
        .or_else(|| csv::call_object_method(runtime, object, class_name, builtin_value, name, args))
        .or_else(|| json::call_object_method(runtime, class_name, builtin_value, name, args))
        .or_else(|| time::call_object_method(runtime, class_name, builtin_value, name, args))
        .or_else(|| secure::call_object_method(runtime, class_name, builtin_value, name, args))
        .or_else(|| gui::call_object_method(runtime, object, class_name, name, args))
        .or_else(|| xml::call_object_method(runtime, object, class_name, builtin_value, name, args))
}

pub(super) fn has_builtin_object_method(class_name: &str, name: &str) -> bool {
    db::has_builtin_object_method(class_name, name)
        || net_http::has_builtin_object_method(class_name, name)
        || worker::has_builtin_object_method(class_name, name)
        || bignum::has_builtin_object_method(class_name, name)
        || clib::has_builtin_object_method(class_name, name)
        || io::has_builtin_object_method(class_name, name)
        || time::has_builtin_object_method(class_name, name)
        || net_smtp::has_object_method(class_name, name)
        || gui::has_builtin_object_method(class_name, name)
        || xml::has_builtin_object_method(class_name, name)
        || secure::has_object_method(class_name, name)
        || matches!(
            (class_name, name),
            ("CSVReader", "next_array")
                | ("CSVReader", "next_dict")
                | ("CSVReader", "next")
                | ("CSVReader", "all_array")
                | ("CSVReader", "all_dict")
                | ("CSVReader", "headers")
                | ("CSVReader", "columns")
                | ("CSVReader", "set_columns")
                | ("CSVReader", "row_number")
                | ("CSVReader", "skip_lines")
                | ("CSVReader", "errors")
                | ("CSVReader", "close")
                | ("CSVReader", "to_Iterator")
                | ("CSVWriter", "write_header")
                | ("CSVWriter", "write_row")
                | ("CSVWriter", "print_row")
                | ("CSVWriter", "columns")
                | ("CSVWriter", "row_number")
                | ("CSVWriter", "close")
        )
}

pub(super) fn construct_builtin_object(
    runtime: &Runtime,
    class_name: &str,
    args: Vec<Value>,
    named_args: Vec<(String, Value)>,
) -> Option<Result<Value>> {
    match class_name {
        "Path" => Some(io::construct_path(args, named_args)),
        "JSON" => Some(json::construct_json(args, named_args)),
        "YAML" => Some(yaml::construct_yaml(args, named_args)),
        "CSV" => Some(csv::construct_csv(runtime, args, named_args)),
        "Time" => Some(time::construct_time(runtime, args, named_args)),
        "TimeParser" => Some(time::construct_time_parser(args, named_args)),
        "CookieJar" => Some(net_http::construct_cookie_jar(args, named_args)),
        "UserAgent" => Some(net_http::construct_user_agent(args, named_args)),
        "Mailer" => Some(net_smtp::construct_mailer(args, named_args)),
        "CLib" => Some(clib::construct_clib(args, named_args)),
        class_name if gui::is_gui_class(class_name) => {
            Some(gui::construct_object(runtime, class_name, args, named_args))
        }
        _ => None,
    }
}
