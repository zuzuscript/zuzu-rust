use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::fs;
use std::rc::Rc;
use std::time::Duration;

use super::super::{
    FieldSpec, MethodValue, ObjectValue, Runtime, TraitValue, UserClassValue, Value,
};
use super::json;
use crate::error::{Result, ZuzuRustError};

#[derive(Clone)]
struct HttpRequestTask {
    user_agent_fields: HashMap<String, Value>,
    request_fields: HashMap<String, Value>,
}

#[derive(Clone)]
struct HttpRequestSpec {
    method: String,
    url: String,
    headers: HashMap<String, String>,
    body: Option<String>,
    timeout: Option<Duration>,
    retries: usize,
    max_redirect: usize,
    download_to: Option<String>,
    cookie_jar: Option<Rc<RefCell<ObjectValue>>>,
    tls_identity: Option<TlsIdentitySpec>,
    tls_ca_pem: Option<String>,
    tls_verify: bool,
    tls_min_version: Option<reqwest::tls::Version>,
}

#[derive(Clone)]
struct TlsIdentitySpec {
    chain_pem: String,
    key_pem: String,
}

pub(super) fn exports() -> HashMap<String, Value> {
    HashMap::from([
        (
            "CookieJar".to_owned(),
            Value::builtin_class("CookieJar".to_owned()),
        ),
        (
            "UserAgent".to_owned(),
            Value::builtin_class("UserAgent".to_owned()),
        ),
        (
            "Request".to_owned(),
            Value::builtin_class("Request".to_owned()),
        ),
        (
            "Response".to_owned(),
            Value::builtin_class("Response".to_owned()),
        ),
    ])
}

pub(super) fn construct_cookie_jar(
    _args: Vec<Value>,
    _named_args: Vec<(String, Value)>,
) -> Result<Value> {
    Ok(object(
        "CookieJar",
        HashMap::from([("cookies".to_owned(), Value::Dict(HashMap::new()))]),
    ))
}

pub(super) fn construct_user_agent(
    _args: Vec<Value>,
    named_args: Vec<(String, Value)>,
) -> Result<Value> {
    let mut config = HashMap::new();
    config.insert("agent".to_owned(), Value::Null);
    config.insert("default_headers".to_owned(), Value::Dict(HashMap::new()));
    config.insert("cookie_jar".to_owned(), Value::Null);
    config.insert("tls_identity".to_owned(), Value::Null);
    config.insert("tls_ca".to_owned(), Value::Null);
    config.insert("tls_verify".to_owned(), Value::Boolean(true));
    config.insert("tls_server_name".to_owned(), Value::Null);
    config.insert("tls_min_version".to_owned(), Value::Null);
    config.insert("tls_ciphers".to_owned(), Value::Null);
    config.insert("max_redirect".to_owned(), Value::Number(10.0));
    for (name, value) in named_args {
        config.insert(name, value);
    }
    Ok(object("UserAgent", config))
}

pub(super) fn call_object_method(
    runtime: &Runtime,
    object: &Rc<RefCell<ObjectValue>>,
    class_name: &str,
    _builtin_value: &Value,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    Some(match class_name {
        "CookieJar" => cookie_jar_method(runtime, object, name, args),
        "UserAgent" => user_agent_method(runtime, object, name, args),
        "Request" => request_method(runtime, object, name, args),
        _ => return None,
    })
}

pub(super) fn call_response_object_method(
    _runtime: &Runtime,
    object: &Rc<RefCell<ObjectValue>>,
    class_name: &str,
    _builtin_value: &Value,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    if class_name != "Response" {
        return None;
    }
    let fields = object.borrow().fields.clone();
    Some(match name {
        "status" => Ok(fields.get("status").cloned().unwrap_or(Value::Number(0.0))),
        "reason" => Ok(fields.get("reason").cloned().unwrap_or(Value::Null)),
        "url" => Ok(fields.get("url").cloned().unwrap_or(Value::Null)),
        "content" => Ok(fields.get("content").cloned().unwrap_or(Value::Null)),
        "headers" => Ok(fields
            .get("headers")
            .cloned()
            .unwrap_or(Value::Dict(HashMap::new()))),
        "header" => {
            let key = args
                .first()
                .map(render_string)
                .unwrap_or_default()
                .to_ascii_lowercase();
            Ok(match fields.get("headers") {
                Some(Value::Dict(headers)) => headers.get(&key).cloned().unwrap_or(Value::Null),
                _ => Value::Null,
            })
        }
        "success" => Ok(Value::Boolean(
            fields.get("success").map(Value::is_truthy).unwrap_or(false),
        )),
        "json" => {
            let content = fields.get("content").map(render_string).unwrap_or_default();
            json::parse_json_text(&content, false)
        }
        "expect_success" => {
            let success = fields.get("success").map(Value::is_truthy).unwrap_or(false);
            if success {
                Ok(Value::Object(Rc::clone(object)))
            } else {
                let status = fields
                    .get("status")
                    .map(render_string)
                    .unwrap_or_else(|| "0".to_owned());
                Err(ZuzuRustError::thrown(format!(
                    "HTTP request failed with status {status}"
                )))
            }
        }
        "to_Dict" => Ok(Value::Dict(HashMap::from([
            (
                "status".to_owned(),
                fields.get("status").cloned().unwrap_or(Value::Number(0.0)),
            ),
            (
                "reason".to_owned(),
                fields.get("reason").cloned().unwrap_or(Value::Null),
            ),
            (
                "url".to_owned(),
                fields.get("url").cloned().unwrap_or(Value::Null),
            ),
            (
                "content".to_owned(),
                fields.get("content").cloned().unwrap_or(Value::Null),
            ),
            (
                "headers".to_owned(),
                fields
                    .get("headers")
                    .cloned()
                    .unwrap_or(Value::Dict(HashMap::new())),
            ),
            (
                "success".to_owned(),
                Value::Boolean(fields.get("success").map(Value::is_truthy).unwrap_or(false)),
            ),
        ]))),
        _ => return None,
    })
}

pub(super) fn has_builtin_object_method(class_name: &str, name: &str) -> bool {
    match class_name {
        "CookieJar" => matches!(name, "add" | "cookie_header" | "clear"),
        "UserAgent" => matches!(
            name,
            "build_request"
                | "send"
                | "send_async"
                | "request"
                | "request_async"
                | "get"
                | "get_async"
                | "head"
                | "head_async"
                | "delete"
                | "delete_async"
                | "options"
                | "options_async"
                | "post"
                | "post_async"
                | "put"
                | "put_async"
                | "patch"
                | "patch_async"
        ),
        "Request" => matches!(
            name,
            "query"
                | "auth_bearer"
                | "headers"
                | "body"
                | "timeout"
                | "retries"
                | "max_redirect"
                | "download_to"
                | "upload_from"
                | "tls_identity"
                | "method"
                | "url"
                | "json"
                | "header"
                | "send"
                | "send_async"
        ),
        "Response" => matches!(
            name,
            "status"
                | "reason"
                | "url"
                | "content"
                | "headers"
                | "header"
                | "success"
                | "json"
                | "expect_success"
                | "to_Dict"
        ),
        _ => false,
    }
}

fn cookie_jar_method(
    runtime: &Runtime,
    object: &Rc<RefCell<ObjectValue>>,
    name: &str,
    args: &[Value],
) -> Result<Value> {
    match name {
        "add" => {
            if args.len() != 2 {
                return Err(ZuzuRustError::runtime(
                    "CookieJar.add() expects two arguments",
                ));
            }
            let url = runtime.render_value(&args[0])?;
            let set_cookie = runtime.render_value(&args[1])?;
            if let Some(Value::Dict(cookies)) = object.borrow_mut().fields.get_mut("cookies") {
                cookies.insert(url, Value::String(set_cookie));
            }
            Ok(Value::Object(Rc::clone(object)))
        }
        "cookie_header" => {
            let url = args
                .first()
                .map(|value| runtime.render_value(value))
                .transpose()?
                .unwrap_or_default();
            Ok(match object.borrow().fields.get("cookies") {
                Some(Value::Dict(cookies)) => cookies.get(&url).cloned().unwrap_or(Value::Null),
                _ => Value::Null,
            })
        }
        "clear" => {
            object
                .borrow_mut()
                .fields
                .insert("cookies".to_owned(), Value::Dict(HashMap::new()));
            Ok(Value::Object(Rc::clone(object)))
        }
        _ => Err(ZuzuRustError::thrown(format!(
            "unsupported method '{}' for CookieJar",
            name
        ))),
    }
}

fn user_agent_method(
    runtime: &Runtime,
    object: &Rc<RefCell<ObjectValue>>,
    name: &str,
    args: &[Value],
) -> Result<Value> {
    match name {
        "build_request" => {
            if args.len() != 2 {
                return Err(ZuzuRustError::runtime(
                    "UserAgent.build_request() expects method and url",
                ));
            }
            let method = runtime.render_value(&args[0])?;
            let url = runtime.render_value(&args[1])?;
            Ok(request_object(&method, &url))
        }
        "send" | "send_async" => {
            let Some(Value::Object(request)) = args.first() else {
                return Err(ZuzuRustError::runtime("UserAgent.send expects a Request"));
            };
            let task = http_request_task(object, request);
            Ok(if name.ends_with("_async") {
                task_http_request_async(runtime, task)
            } else {
                execute_http_task(runtime, task)?
            })
        }
        "request" | "request_async" => {
            if args.len() < 2 {
                return Err(ZuzuRustError::runtime(
                    "UserAgent.request expects method and url",
                ));
            }
            let method = runtime.render_value(&args[0])?;
            let url = runtime.render_value(&args[1])?;
            let req = request_object(&method, &url);
            if let Value::Object(request) = &req {
                if let Some(data) = args.get(2) {
                    request
                        .borrow_mut()
                        .fields
                        .insert("body".to_owned(), data.clone());
                }
                if let Some(Value::Dict(extra)) = args.get(3) {
                    request
                        .borrow_mut()
                        .fields
                        .insert("headers".to_owned(), Value::Dict(extra.clone()));
                }
            }
            if let Value::Object(request) = req {
                let task = http_request_task(object, &request);
                Ok(if name.ends_with("_async") {
                    task_http_request_async(runtime, task)
                } else {
                    execute_http_task(runtime, task)?
                })
            } else {
                unreachable!()
            }
        }
        "get" | "get_async" | "head" | "head_async" | "delete" | "delete_async" | "options"
        | "options_async" | "post" | "post_async" | "put" | "put_async" | "patch"
        | "patch_async" => {
            let method = name.trim_end_matches("_async").to_ascii_uppercase();
            let url = args
                .first()
                .map(|value| runtime.render_value(value))
                .transpose()?
                .unwrap_or_default();
            let req = request_object(&method, &url);
            if let Value::Object(request) = req {
                let (data, headers) = if matches!(method.as_str(), "POST" | "PUT" | "PATCH") {
                    (args.get(1), args.get(2))
                } else {
                    (None, args.get(1))
                };
                if let Some(data) = data {
                    request
                        .borrow_mut()
                        .fields
                        .insert("body".to_owned(), data.clone());
                }
                if let Some(Value::Dict(extra)) = headers {
                    request
                        .borrow_mut()
                        .fields
                        .insert("headers".to_owned(), Value::Dict(extra.clone()));
                }
                let task = http_request_task(object, &request);
                Ok(if name.ends_with("_async") {
                    task_http_request_async(runtime, task)
                } else {
                    execute_http_task(runtime, task)?
                })
            } else {
                unreachable!()
            }
        }
        _ => Err(ZuzuRustError::thrown(format!(
            "unsupported method '{}' for UserAgent",
            name
        ))),
    }
}

fn request_method(
    runtime: &Runtime,
    object: &Rc<RefCell<ObjectValue>>,
    name: &str,
    args: &[Value],
) -> Result<Value> {
    match name {
        "query" => {
            let query = match args.first() {
                Some(Value::Dict(map)) => map.clone(),
                Some(Value::PairList(items)) => items.iter().cloned().collect(),
                _ => HashMap::new(),
            };
            object
                .borrow_mut()
                .fields
                .insert("query".to_owned(), Value::Dict(query));
            Ok(Value::Object(Rc::clone(object)))
        }
        "auth_bearer" => {
            let token = args
                .first()
                .map(|value| runtime.render_value(value))
                .transpose()?
                .unwrap_or_default();
            let mut headers = match object.borrow().fields.get("headers") {
                Some(Value::Dict(map)) => map.clone(),
                _ => HashMap::new(),
            };
            headers.insert(
                "authorization".to_owned(),
                Value::String(format!("Bearer {token}")),
            );
            object
                .borrow_mut()
                .fields
                .insert("headers".to_owned(), Value::Dict(headers));
            Ok(Value::Object(Rc::clone(object)))
        }
        "headers" => {
            let Some(Value::Dict(input)) = args.first() else {
                return Ok(Value::Object(Rc::clone(object)));
            };
            let mut headers = match object.borrow().fields.get("headers") {
                Some(Value::Dict(map)) => map.clone(),
                _ => HashMap::new(),
            };
            for (key, value) in input {
                headers.insert(
                    key.to_ascii_lowercase(),
                    Value::String(render_string(value)),
                );
            }
            object
                .borrow_mut()
                .fields
                .insert("headers".to_owned(), Value::Dict(headers));
            Ok(Value::Object(Rc::clone(object)))
        }
        "body" | "timeout" | "retries" | "max_redirect" | "download_to" | "upload_from"
        | "tls_identity" => {
            if let Some(value) = args.first() {
                object
                    .borrow_mut()
                    .fields
                    .insert(name.to_owned(), value.clone());
            }
            Ok(Value::Object(Rc::clone(object)))
        }
        "method" | "url" => {
            if let Some(value) = args.first() {
                object
                    .borrow_mut()
                    .fields
                    .insert(name.to_owned(), Value::String(runtime.render_value(value)?));
            }
            Ok(Value::Object(Rc::clone(object)))
        }
        "json" => {
            if let Some(value) = args.first() {
                object.borrow_mut().fields.insert(
                    "body".to_owned(),
                    Value::String(runtime.render_value(value)?),
                );
                let mut headers = match object.borrow().fields.get("headers") {
                    Some(Value::Dict(map)) => map.clone(),
                    _ => HashMap::new(),
                };
                headers
                    .entry("content-type".to_owned())
                    .or_insert_with(|| Value::String("application/json".to_owned()));
                object
                    .borrow_mut()
                    .fields
                    .insert("headers".to_owned(), Value::Dict(headers));
            }
            Ok(Value::Object(Rc::clone(object)))
        }
        "header" => {
            if args.len() == 2 {
                let key = runtime.render_value(&args[0])?.to_ascii_lowercase();
                let value = runtime.render_value(&args[1])?;
                let mut headers = match object.borrow().fields.get("headers") {
                    Some(Value::Dict(map)) => map.clone(),
                    _ => HashMap::new(),
                };
                headers.insert(key, Value::String(value));
                object
                    .borrow_mut()
                    .fields
                    .insert("headers".to_owned(), Value::Dict(headers));
                Ok(Value::Object(Rc::clone(object)))
            } else {
                Err(ZuzuRustError::runtime(
                    "Request.header expects name and value",
                ))
            }
        }
        "send" => {
            let Some(Value::Object(ua)) = args.first() else {
                return Err(ZuzuRustError::runtime("Request.send expects a UserAgent"));
            };
            execute_http_task(runtime, http_request_task(ua, object))
        }
        "send_async" => {
            let Some(Value::Object(ua)) = args.first() else {
                return Err(ZuzuRustError::runtime(
                    "Request.send_async expects a UserAgent",
                ));
            };
            Ok(task_http_request_async(
                runtime,
                http_request_task(ua, object),
            ))
        }
        _ => Err(ZuzuRustError::thrown(format!(
            "unsupported method '{}' for Request",
            name
        ))),
    }
}

fn http_request_task(
    user_agent: &Rc<RefCell<ObjectValue>>,
    request: &Rc<RefCell<ObjectValue>>,
) -> HttpRequestTask {
    HttpRequestTask {
        user_agent_fields: user_agent.borrow().fields.clone(),
        request_fields: request.borrow().fields.clone(),
    }
}

fn task_http_request_async(runtime: &Runtime, task: HttpRequestTask) -> Value {
    let cancel_requested = Rc::new(Cell::new(false));
    let future = execute_http_task_async(task, Rc::clone(&cancel_requested));
    runtime.task_native_async(future, Some(cancel_requested))
}

fn execute_http_task(runtime: &Runtime, task: HttpRequestTask) -> Result<Value> {
    runtime.warn_blocking_operation("std/net/http request")?;
    execute_http_task_blocking(task)
}

fn http_request_spec(task: HttpRequestTask) -> Result<HttpRequestSpec> {
    let request_fields = task.request_fields;
    let user_agent_fields = task.user_agent_fields;
    let mut url = request_fields
        .get("url")
        .map(render_string)
        .unwrap_or_else(|| "https://example.com/".to_owned());
    if let Some(Value::Dict(query)) = request_fields.get("query") {
        let mut keys = query.keys().cloned().collect::<Vec<_>>();
        keys.sort();
        if !keys.is_empty() {
            let pairs = keys
                .into_iter()
                .map(|key| {
                    format!(
                        "{}={}",
                        key,
                        percent_encode(&render_string(query.get(&key).unwrap_or(&Value::Null)))
                    )
                })
                .collect::<Vec<_>>();
            url.push(if url.contains('?') { '&' } else { '?' });
            url.push_str(&pairs.join("&"));
        }
    }
    let timeout = request_fields
        .get("timeout")
        .or_else(|| user_agent_fields.get("timeout"))
        .and_then(|value| value.to_number().ok())
        .filter(|seconds| seconds.is_finite() && *seconds > 0.0)
        .map(Duration::from_secs_f64);
    let method = request_fields
        .get("method")
        .map(render_string)
        .unwrap_or_else(|| "GET".to_owned())
        .to_ascii_uppercase();
    let mut headers = header_map_from_value(user_agent_fields.get("default_headers"));
    headers.extend(header_map_from_value(request_fields.get("headers")));

    if let Some(Value::Object(cookie_jar)) = user_agent_fields.get("cookie_jar") {
        let cookie_header = match cookie_jar.borrow().fields.get("cookies") {
            Some(Value::Dict(map)) => map.get(&url).cloned().unwrap_or(Value::Null),
            _ => Value::Null,
        };
        if !matches!(cookie_header, Value::Null) {
            headers.insert("cookie".to_owned(), render_string(&cookie_header));
        }
    }

    let body = if let Some(upload_from) = request_fields.get("upload_from") {
        let path = render_string(upload_from);
        Some(fs::read_to_string(&path).map_err(|err| {
            ZuzuRustError::runtime(format!(
                "HTTP request upload_from open failed for '{path}': {err}"
            ))
        })?)
    } else {
        request_fields.get("body").and_then(|value| {
            if matches!(value, Value::Null) {
                None
            } else {
                Some(render_string(value))
            }
        })
    };
    let retries = request_fields
        .get("retries")
        .and_then(|value| value.to_number().ok())
        .map(|value| value.max(0.0) as usize)
        .unwrap_or(0);
    let max_redirect = request_fields
        .get("max_redirect")
        .or_else(|| user_agent_fields.get("max_redirect"))
        .and_then(|value| value.to_number().ok())
        .filter(|value| value.is_finite())
        .map(|value| value.max(0.0) as usize)
        .unwrap_or(10);
    let download_to = request_fields.get("download_to").map(render_string);
    let cookie_jar = match user_agent_fields.get("cookie_jar") {
        Some(Value::Object(cookie_jar)) => Some(Rc::clone(cookie_jar)),
        _ => None,
    };
    reject_unsupported_tls_policy(&user_agent_fields)?;
    let tls_identity_value = request_fields
        .get("tls_identity")
        .or_else(|| user_agent_fields.get("tls_identity"));
    let tls_identity = match tls_identity_value {
        Some(Value::Null) | None => None,
        Some(value) => Some(tls_identity_spec(value, "std/net/http tls_identity")?),
    };
    let tls_ca_pem = match user_agent_fields.get("tls_ca") {
        Some(Value::Null) | None => None,
        Some(value) => Some(tls_ca_pem(value, "std/net/http tls_ca")?),
    };
    let tls_verify = user_agent_fields
        .get("tls_verify")
        .map(Value::is_truthy)
        .unwrap_or(true);
    let tls_min_version = match user_agent_fields.get("tls_min_version") {
        Some(Value::Null) | None => None,
        Some(value) => Some(tls_min_version(value)?),
    };

    Ok(HttpRequestSpec {
        method,
        url,
        headers,
        body,
        timeout,
        retries,
        max_redirect,
        download_to,
        cookie_jar,
        tls_identity,
        tls_ca_pem,
        tls_verify,
        tls_min_version,
    })
}

fn reject_unsupported_tls_policy(fields: &HashMap<String, Value>) -> Result<()> {
    if !matches!(fields.get("tls_server_name"), None | Some(Value::Null)) {
        return Err(ZuzuRustError::thrown(
            "std/net/http tls_server_name is not supported by the Rust HTTP backend",
        ));
    }
    if !matches!(fields.get("tls_ciphers"), None | Some(Value::Null)) {
        return Err(ZuzuRustError::thrown(
            "std/net/http tls_ciphers is not supported by the Rust HTTP backend",
        ));
    }
    Ok(())
}

fn tls_identity_spec(value: &Value, label: &str) -> Result<TlsIdentitySpec> {
    let Value::Object(object) = value else {
        return Err(ZuzuRustError::thrown(format!(
            "TypeException: {label} expects TlsIdentity"
        )));
    };
    if object.borrow().class.name != "TlsIdentity" {
        return Err(ZuzuRustError::thrown(format!(
            "TypeException: {label} expects TlsIdentity"
        )));
    }
    let builtin_value = object
        .borrow()
        .builtin_value
        .clone()
        .ok_or_else(|| ZuzuRustError::runtime("TlsIdentity internal state missing"))?;
    let Value::Dict(fields) = builtin_value else {
        return Err(ZuzuRustError::runtime("TlsIdentity internal state missing"));
    };
    let chain_pem = string_field(&fields, "chain_pem", label)
        .or_else(|_| string_field(&fields, "cert_pem", label))?;
    let key_pem = string_field(&fields, "key_pem", label)?;
    Ok(TlsIdentitySpec { chain_pem, key_pem })
}

fn tls_ca_pem(value: &Value, label: &str) -> Result<String> {
    match value {
        Value::String(text) => {
            if !text.contains("-----BEGIN CERTIFICATE-----") {
                return Err(ZuzuRustError::thrown(format!(
                    "{label} expects PEM certificate text"
                )));
            }
            Ok(text.clone())
        }
        Value::Object(object) if object.borrow().class.name == "Certificate" => {
            let builtin_value = object
                .borrow()
                .builtin_value
                .clone()
                .ok_or_else(|| ZuzuRustError::runtime("Certificate internal state missing"))?;
            let Value::Dict(fields) = builtin_value else {
                return Err(ZuzuRustError::runtime("Certificate internal state missing"));
            };
            let der = match fields.get("der") {
                Some(Value::BinaryString(bytes)) => bytes.clone(),
                _ => return Err(ZuzuRustError::runtime("Certificate internal state missing")),
            };
            Ok(certificate_der_to_pem_local(&der))
        }
        Value::Array(items) => items
            .iter()
            .map(|item| tls_ca_pem(item, label))
            .collect::<Result<Vec<_>>>()
            .map(|parts| parts.join("")),
        other => Err(ZuzuRustError::thrown(format!(
            "TypeException: {label} expects Certificate, String PEM, or Array, got {}",
            other.type_name()
        ))),
    }
}

fn string_field(fields: &HashMap<String, Value>, name: &str, label: &str) -> Result<String> {
    match fields.get(name) {
        Some(Value::String(text)) => Ok(text.clone()),
        _ => Err(ZuzuRustError::runtime(format!(
            "{label} internal state missing"
        ))),
    }
}

fn tls_min_version(value: &Value) -> Result<reqwest::tls::Version> {
    match render_string(value).to_ascii_lowercase().as_str() {
        "tls1.2" => Ok(reqwest::tls::Version::TLS_1_2),
        "tls1.3" => Ok(reqwest::tls::Version::TLS_1_3),
        _ => Err(ZuzuRustError::thrown(
            "std/net/http tls_min_version must be 'tls1.2' or 'tls1.3'",
        )),
    }
}

fn certificate_der_to_pem_local(der: &[u8]) -> String {
    const BASE64_CHARS: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::new();
    for chunk in der.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied().unwrap_or(0);
        let b2 = chunk.get(2).copied().unwrap_or(0);
        encoded.push(BASE64_CHARS[(b0 >> 2) as usize] as char);
        encoded.push(BASE64_CHARS[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            encoded.push(BASE64_CHARS[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            encoded.push('=');
        }
        if chunk.len() > 2 {
            encoded.push(BASE64_CHARS[(b2 & 0x3f) as usize] as char);
        } else {
            encoded.push('=');
        }
    }
    let mut pem = String::from("-----BEGIN CERTIFICATE-----\n");
    for chunk in encoded.as_bytes().chunks(64) {
        pem.push_str(std::str::from_utf8(chunk).unwrap_or(""));
        pem.push('\n');
    }
    pem.push_str("-----END CERTIFICATE-----\n");
    pem
}

fn apply_tls_to_blocking_client(
    mut client: reqwest::blocking::ClientBuilder,
    spec: &HttpRequestSpec,
) -> Result<reqwest::blocking::ClientBuilder> {
    if !spec.tls_verify {
        client = client.tls_danger_accept_invalid_certs(true);
    }
    if let Some(version) = spec.tls_min_version {
        client = client.min_tls_version(version);
    }
    if let Some(ca_pem) = &spec.tls_ca_pem {
        for cert in reqwest::tls::Certificate::from_pem_bundle(ca_pem.as_bytes())
            .map_err(|err| ZuzuRustError::thrown(format!("std/net/http tls_ca failed: {err}")))?
        {
            client = client.add_root_certificate(cert);
        }
    }
    if let Some(identity) = &spec.tls_identity {
        let pkcs8_key = openssl::pkey::PKey::private_key_from_pem(identity.key_pem.as_bytes())
            .and_then(|key| key.private_key_to_pem_pkcs8())
            .map_err(|_| ZuzuRustError::thrown("std/net/http tls_identity private key failed"))?;
        let identity = reqwest::Identity::from_pkcs8_pem(identity.chain_pem.as_bytes(), &pkcs8_key)
            .map_err(|err| {
                ZuzuRustError::thrown(format!("std/net/http tls_identity failed: {err}"))
            })?;
        client = client.identity(identity);
    }
    Ok(client)
}

fn apply_tls_to_async_client(
    mut client: reqwest::ClientBuilder,
    spec: &HttpRequestSpec,
) -> Result<reqwest::ClientBuilder> {
    if !spec.tls_verify {
        client = client.tls_danger_accept_invalid_certs(true);
    }
    if let Some(version) = spec.tls_min_version {
        client = client.min_tls_version(version);
    }
    if let Some(ca_pem) = &spec.tls_ca_pem {
        for cert in reqwest::tls::Certificate::from_pem_bundle(ca_pem.as_bytes())
            .map_err(|err| ZuzuRustError::thrown(format!("std/net/http tls_ca failed: {err}")))?
        {
            client = client.add_root_certificate(cert);
        }
    }
    if let Some(identity) = &spec.tls_identity {
        let pkcs8_key = openssl::pkey::PKey::private_key_from_pem(identity.key_pem.as_bytes())
            .and_then(|key| key.private_key_to_pem_pkcs8())
            .map_err(|_| ZuzuRustError::thrown("std/net/http tls_identity private key failed"))?;
        let identity = reqwest::Identity::from_pkcs8_pem(identity.chain_pem.as_bytes(), &pkcs8_key)
            .map_err(|err| {
                ZuzuRustError::thrown(format!("std/net/http tls_identity failed: {err}"))
            })?;
        client = client.identity(identity);
    }
    Ok(client)
}

fn execute_http_task_blocking(task: HttpRequestTask) -> Result<Value> {
    let spec = http_request_spec(task)?;
    let mut client = reqwest::blocking::Client::builder().use_native_tls();
    if let Some(timeout) = spec.timeout {
        client = client.timeout(timeout);
    }
    client = if spec.max_redirect == 0 {
        client.redirect(reqwest::redirect::Policy::none())
    } else {
        client.redirect(reqwest::redirect::Policy::limited(spec.max_redirect))
    };
    client = apply_tls_to_blocking_client(client, &spec)?;
    let client = client
        .build()
        .map_err(|err| ZuzuRustError::runtime(format!("HTTP client failed: {err}")))?;

    let mut last_error = None;
    let mut response_result = None;
    for attempt in 0..=spec.retries {
        let mut request_builder = client.request(
            reqwest_method(&spec.method),
            spec.url
                .parse::<reqwest::Url>()
                .map_err(|err| ZuzuRustError::runtime(format!("HTTP URL failed: {err}")))?,
        );
        for (key, value) in &spec.headers {
            request_builder = request_builder.header(key, value);
        }
        if let Some(body) = &spec.body {
            request_builder = request_builder.body(body.clone());
        }
        let result = request_builder.send();
        match result {
            Ok(response) => {
                let status = response.status().as_u16();
                response_result = Some(response);
                if !(500..=599).contains(&status) || attempt == spec.retries {
                    break;
                }
            }
            Err(err) => {
                last_error = Some(err);
                break;
            }
        }
    }

    let Some(response) = response_result else {
        let reason = last_error
            .map(|err| err.to_string())
            .unwrap_or_else(|| "Connection Failed".to_owned());
        return Ok(response_object(
            599.0,
            &reason,
            &spec.url,
            &reason,
            default_error_headers(),
        ));
    };

    response_value_from_blocking_response(response, &spec)
}

async fn execute_http_task_async(
    task: HttpRequestTask,
    cancel_requested: Rc<Cell<bool>>,
) -> Result<Value> {
    let spec = http_request_spec(task)?;
    let mut client = reqwest::Client::builder().use_native_tls();
    if let Some(timeout) = spec.timeout {
        client = client.timeout(timeout);
    }
    client = if spec.max_redirect == 0 {
        client.redirect(reqwest::redirect::Policy::none())
    } else {
        client.redirect(reqwest::redirect::Policy::limited(spec.max_redirect))
    };
    client = apply_tls_to_async_client(client, &spec)?;
    let client = client
        .build()
        .map_err(|err| ZuzuRustError::runtime(format!("HTTP client failed: {err}")))?;

    let mut last_error = None;
    let mut response_result = None;
    for attempt in 0..=spec.retries {
        if cancel_requested.get() {
            return Err(ZuzuRustError::runtime("HTTP request cancelled"));
        }
        let mut request_builder = client.request(
            reqwest_method(&spec.method),
            spec.url
                .parse::<reqwest::Url>()
                .map_err(|err| ZuzuRustError::runtime(format!("HTTP URL failed: {err}")))?,
        );
        for (key, value) in &spec.headers {
            request_builder = request_builder.header(key, value);
        }
        if let Some(body) = &spec.body {
            request_builder = request_builder.body(body.clone());
        }
        let mut send = Box::pin(request_builder.send());
        let result = loop {
            if cancel_requested.get() {
                return Err(ZuzuRustError::runtime("HTTP request cancelled"));
            }
            match tokio::time::timeout(Duration::from_millis(5), &mut send).await {
                Ok(result) => break result,
                Err(_) => continue,
            }
        };
        match result {
            Ok(response) => {
                let status = response.status().as_u16();
                response_result = Some(response);
                if !(500..=599).contains(&status) || attempt == spec.retries {
                    break;
                }
            }
            Err(err) => {
                last_error = Some(err);
                break;
            }
        }
    }

    let Some(response) = response_result else {
        let reason = last_error
            .map(|err| err.to_string())
            .unwrap_or_else(|| "Connection Failed".to_owned());
        return Ok(response_object(
            599.0,
            &reason,
            &spec.url,
            &reason,
            default_error_headers(),
        ));
    };

    response_value_from_async_response(response, &spec, cancel_requested).await
}

fn response_value_from_blocking_response(
    response: reqwest::blocking::Response,
    spec: &HttpRequestSpec,
) -> Result<Value> {
    let status_code = response.status();
    let status = status_code.as_u16() as f64;
    let reason = status_code.canonical_reason().unwrap_or("").to_owned();
    let mut response_headers = HashMap::new();
    for (name, value) in response.headers() {
        let value = value.to_str().unwrap_or_default().to_owned();
        response_headers.insert(name.as_str().to_ascii_lowercase(), Value::String(value));
    }
    if let Some(cookie_jar) = &spec.cookie_jar {
        if let Some(Value::String(cookie)) = response_headers.get("set-cookie") {
            if let Some(Value::Dict(cookies)) = cookie_jar.borrow_mut().fields.get_mut("cookies") {
                cookies.insert(spec.url.clone(), Value::String(cookie.clone()));
            }
        }
    }
    let final_url = response.url().to_string();
    let body = response
        .bytes()
        .map_err(|err| ZuzuRustError::runtime(format!("HTTP response read failed: {err}")))?;
    if let Some(path) = &spec.download_to {
        fs::write(&path, &body).map_err(|err| {
            ZuzuRustError::runtime(format!(
                "HTTP request download_to open failed for '{path}': {err}"
            ))
        })?;
    }
    let content = String::from_utf8_lossy(&body).to_string();
    Ok(response_object(
        status,
        &reason,
        &final_url,
        &content,
        response_headers,
    ))
}

async fn response_value_from_async_response(
    response: reqwest::Response,
    spec: &HttpRequestSpec,
    cancel_requested: Rc<Cell<bool>>,
) -> Result<Value> {
    let status_code = response.status();
    let status = status_code.as_u16() as f64;
    let reason = status_code.canonical_reason().unwrap_or("").to_owned();
    let mut response_headers = HashMap::new();
    for (name, value) in response.headers() {
        let value = value.to_str().unwrap_or_default().to_owned();
        response_headers.insert(name.as_str().to_ascii_lowercase(), Value::String(value));
    }
    if let Some(cookie_jar) = &spec.cookie_jar {
        if let Some(Value::String(cookie)) = response_headers.get("set-cookie") {
            if let Some(Value::Dict(cookies)) = cookie_jar.borrow_mut().fields.get_mut("cookies") {
                cookies.insert(spec.url.clone(), Value::String(cookie.clone()));
            }
        }
    }
    let final_url = response.url().to_string();
    let mut bytes = Box::pin(response.bytes());
    let body = loop {
        if cancel_requested.get() {
            return Err(ZuzuRustError::runtime("HTTP request cancelled"));
        }
        match tokio::time::timeout(Duration::from_millis(5), &mut bytes).await {
            Ok(result) => break result.unwrap_or_default(),
            Err(_) => continue,
        }
    };
    if let Some(path) = &spec.download_to {
        tokio::fs::write(&path, &body).await.map_err(|err| {
            ZuzuRustError::runtime(format!(
                "HTTP request download_to open failed for '{path}': {err}"
            ))
        })?;
    }
    let content = String::from_utf8_lossy(&body).to_string();
    Ok(response_object(
        status,
        &reason,
        &final_url,
        &content,
        response_headers,
    ))
}

fn request_object(method: &str, url: &str) -> Value {
    object(
        "Request",
        HashMap::from([
            (
                "method".to_owned(),
                Value::String(method.to_ascii_uppercase()),
            ),
            ("url".to_owned(), Value::String(url.to_owned())),
            ("headers".to_owned(), Value::Dict(HashMap::new())),
            ("query".to_owned(), Value::Dict(HashMap::new())),
            ("body".to_owned(), Value::Null),
        ]),
    )
}

fn response_object(
    status: f64,
    reason: &str,
    url: &str,
    content: &str,
    headers: HashMap<String, Value>,
) -> Value {
    object(
        "Response",
        HashMap::from([
            ("status".to_owned(), Value::Number(status)),
            ("reason".to_owned(), Value::String(reason.to_owned())),
            ("url".to_owned(), Value::String(url.to_owned())),
            ("content".to_owned(), Value::String(content.to_owned())),
            ("headers".to_owned(), Value::Dict(headers)),
            (
                "success".to_owned(),
                Value::Boolean((200.0..300.0).contains(&status)),
            ),
        ]),
    )
}

fn default_error_headers() -> HashMap<String, Value> {
    HashMap::from([(
        "content-type".to_owned(),
        Value::String("text/plain".to_owned()),
    )])
}

fn object(class_name: &str, fields: HashMap<String, Value>) -> Value {
    Value::Object(Rc::new(RefCell::new(ObjectValue {
        class: class(class_name),
        fields: fields.clone(),
        weak_fields: std::collections::HashSet::new(),
        builtin_value: Some(Value::Dict(fields)),
    })))
}

fn reqwest_method(method: &str) -> reqwest::Method {
    reqwest::Method::from_bytes(method.as_bytes()).unwrap_or(reqwest::Method::GET)
}

fn class(name: &str) -> Rc<UserClassValue> {
    Rc::new(UserClassValue {
        name: name.to_owned(),
        base: None,
        traits: Vec::<Rc<TraitValue>>::new(),
        fields: Vec::<FieldSpec>::new(),
        methods: HashMap::<String, Rc<MethodValue>>::new(),
        static_methods: HashMap::<String, Rc<MethodValue>>::new(),
        nested_classes: HashMap::new(),
        source_decl: None,
        closure_env: None,
    })
}

fn render_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Boolean(true) => "true".to_owned(),
        Value::Boolean(false) => "false".to_owned(),
        Value::Null => String::new(),
        other => other.render(),
    }
}

fn header_map_from_value(value: Option<&Value>) -> HashMap<String, String> {
    let Some(Value::Dict(headers)) = value else {
        return HashMap::new();
    };
    headers
        .iter()
        .map(|(key, value)| (key.to_ascii_lowercase(), render_string(value)))
        .collect()
}

fn percent_encode(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}
