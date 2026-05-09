use std::collections::HashMap;
use std::net::{IpAddr, ToSocketAddrs};

use hickory_resolver::config::{ResolverConfig, ResolverOpts};
use hickory_resolver::proto::rr::{RData, RecordType};
use hickory_resolver::Resolver;

use super::super::{Runtime, Value};
use crate::error::{Result, ZuzuRustError};

pub(super) fn exports() -> HashMap<String, Value> {
    HashMap::from([
        (
            "lookup".to_owned(),
            Value::native_function("dns.lookup".to_owned()),
        ),
        (
            "lookup_async".to_owned(),
            Value::native_function("dns.lookup_async".to_owned()),
        ),
        (
            "addresses".to_owned(),
            Value::native_function("dns.addresses".to_owned()),
        ),
        (
            "addresses_async".to_owned(),
            Value::native_function("dns.addresses_async".to_owned()),
        ),
        (
            "reverse".to_owned(),
            Value::native_function("dns.reverse".to_owned()),
        ),
        (
            "reverse_async".to_owned(),
            Value::native_function("dns.reverse_async".to_owned()),
        ),
    ])
}

pub(super) fn call(runtime: &Runtime, name: &str, args: &[Value]) -> Option<Result<Value>> {
    let result = match name {
        "dns.lookup" => runtime
            .warn_blocking_operation("std/net/dns lookup")
            .and_then(|_| lookup(runtime, args)),
        "dns.lookup_async" => lookup(runtime, args).map(|value| runtime.task_resolved(value)),
        "dns.addresses" => runtime
            .warn_blocking_operation("std/net/dns addresses")
            .and_then(|_| addresses(runtime, args)),
        "dns.addresses_async" => addresses(runtime, args).map(|value| runtime.task_resolved(value)),
        "dns.reverse" => runtime
            .warn_blocking_operation("std/net/dns reverse")
            .and_then(|_| reverse(runtime, args)),
        "dns.reverse_async" => reverse(runtime, args).map(|value| runtime.task_resolved(value)),
        _ => return None,
    };
    Some(result)
}

fn lookup(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    expect_arity("lookup()", args, 1, 2)?;
    let name = string_arg(runtime, args.first(), "")?;
    let record_type = record_type(
        &args
            .get(1)
            .map(|value| runtime.render_value(value))
            .transpose()?
            .unwrap_or_else(|| "A".to_owned()),
    )?;
    let resolver = resolver()?;
    let lookup = match resolver.lookup(name.as_str(), record_type) {
        Ok(lookup) => lookup,
        Err(err) if is_no_data_error(&err.to_string()) => return Ok(Value::Array(Vec::new())),
        Err(err) => return Err(dns_error(err.to_string())),
    };
    let records = lookup
        .iter()
        .filter_map(|rdata| record_value(&name, record_type, rdata))
        .collect();
    Ok(Value::Array(records))
}

fn addresses(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    expect_arity("addresses()", args, 1, 2)?;
    let name = string_arg(runtime, args.first(), "")?;
    let family = args
        .get(1)
        .map(|value| runtime.render_value(value))
        .transpose()?
        .unwrap_or_else(|| "any".to_owned())
        .to_ascii_lowercase();
    if !matches!(family.as_str(), "any" | "ipv4" | "ipv6") {
        return Err(dns_error(format!("unsupported address family '{family}'")));
    }

    let addrs = match (name.as_str(), 0).to_socket_addrs() {
        Ok(addrs) => addrs,
        Err(err) if is_no_data_error(&err.to_string()) => return Ok(Value::Array(Vec::new())),
        Err(err) => return Err(dns_error(err.to_string())),
    };
    let mut values = Vec::new();
    for addr in addrs {
        let ip = addr.ip();
        if family == "ipv4" && !ip.is_ipv4() {
            continue;
        }
        if family == "ipv6" && !ip.is_ipv6() {
            continue;
        }
        let text = ip.to_string();
        if !values
            .iter()
            .any(|value| matches!(value, Value::String(existing) if existing == &text))
        {
            values.push(Value::String(text));
        }
    }
    Ok(Value::Array(values))
}

fn reverse(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    expect_arity("reverse()", args, 1, 1)?;
    let address = string_arg(runtime, args.first(), "")?;
    let ip: IpAddr = address
        .parse()
        .map_err(|_| dns_error(format!("invalid IP address '{address}'")))?;
    let resolver = resolver()?;
    let lookup = match resolver.reverse_lookup(ip) {
        Ok(lookup) => lookup,
        Err(err) if is_no_data_error(&err.to_string()) => return Ok(Value::Array(Vec::new())),
        Err(err) => return Err(dns_error(err.to_string())),
    };
    Ok(Value::Array(
        lookup
            .iter()
            .map(|name| Value::String(name.to_string()))
            .collect(),
    ))
}

fn resolver() -> Result<Resolver> {
    Resolver::from_system_conf()
        .or_else(|_| Resolver::new(ResolverConfig::default(), ResolverOpts::default()))
        .map_err(|err| dns_error(err.to_string()))
}

fn expect_arity(name: &str, args: &[Value], min: usize, max: usize) -> Result<()> {
    if args.len() >= min && args.len() <= max {
        return Ok(());
    }
    let range = if min == max {
        min.to_string()
    } else {
        format!("{min} or {max}")
    };
    Err(dns_error(format!("{name} expects {range} arguments")))
}

fn string_arg(runtime: &Runtime, value: Option<&Value>, fallback: &str) -> Result<String> {
    value
        .map(|value| runtime.render_value(value))
        .transpose()
        .map(|value| value.unwrap_or_else(|| fallback.to_owned()))
}

fn record_type(value: &str) -> Result<RecordType> {
    match value.to_ascii_uppercase().as_str() {
        "A" => Ok(RecordType::A),
        "AAAA" => Ok(RecordType::AAAA),
        "CNAME" => Ok(RecordType::CNAME),
        "MX" => Ok(RecordType::MX),
        "NS" => Ok(RecordType::NS),
        "PTR" => Ok(RecordType::PTR),
        "SRV" => Ok(RecordType::SRV),
        "TXT" => Ok(RecordType::TXT),
        other => Err(dns_error(format!("unsupported DNS record type '{other}'"))),
    }
}

fn record_value(name: &str, record_type: RecordType, rdata: &RData) -> Option<Value> {
    let type_name = record_type.to_string();
    let mut out = HashMap::from([
        ("type".to_owned(), Value::String(type_name.clone())),
        ("name".to_owned(), Value::String(name.to_owned())),
        ("ttl".to_owned(), Value::Null),
    ]);

    match (record_type, rdata) {
        (RecordType::A, RData::A(address)) => {
            let address = address.to_string();
            out.insert("address".to_owned(), Value::String(address.clone()));
            out.insert("value".to_owned(), Value::String(address));
        }
        (RecordType::AAAA, RData::AAAA(address)) => {
            let address = address.to_string();
            out.insert("address".to_owned(), Value::String(address.clone()));
            out.insert("value".to_owned(), Value::String(address));
        }
        (RecordType::CNAME, RData::CNAME(target)) => {
            let target = target.to_string();
            out.insert("target".to_owned(), Value::String(target.clone()));
            out.insert("value".to_owned(), Value::String(target));
        }
        (RecordType::NS, RData::NS(target)) => {
            let target = target.to_string();
            out.insert("target".to_owned(), Value::String(target.clone()));
            out.insert("value".to_owned(), Value::String(target));
        }
        (RecordType::PTR, RData::PTR(target)) => {
            let target = target.to_string();
            out.insert("target".to_owned(), Value::String(target.clone()));
            out.insert("value".to_owned(), Value::String(target));
        }
        (RecordType::MX, RData::MX(mx)) => {
            let exchange = mx.exchange().to_string();
            out.insert("exchange".to_owned(), Value::String(exchange.clone()));
            out.insert(
                "preference".to_owned(),
                Value::Number(f64::from(mx.preference())),
            );
            out.insert("value".to_owned(), Value::String(exchange));
        }
        (RecordType::TXT, RData::TXT(txt)) => {
            let strings: Vec<Value> = txt
                .txt_data()
                .iter()
                .map(|bytes| Value::String(String::from_utf8_lossy(bytes).into_owned()))
                .collect();
            let text = strings
                .iter()
                .filter_map(|value| match value {
                    Value::String(text) => Some(text.as_str()),
                    _ => None,
                })
                .collect::<String>();
            out.insert("strings".to_owned(), Value::Array(strings));
            out.insert("text".to_owned(), Value::String(text.clone()));
            out.insert("value".to_owned(), Value::String(text));
        }
        (RecordType::SRV, RData::SRV(srv)) => {
            let target = srv.target().to_string();
            out.insert("target".to_owned(), Value::String(target.clone()));
            out.insert("port".to_owned(), Value::Number(f64::from(srv.port())));
            out.insert(
                "priority".to_owned(),
                Value::Number(f64::from(srv.priority())),
            );
            out.insert("weight".to_owned(), Value::Number(f64::from(srv.weight())));
            out.insert("value".to_owned(), Value::String(target));
        }
        _ => return None,
    }

    if !out.contains_key("value") {
        out.insert("value".to_owned(), Value::String(String::new()));
    }
    Some(Value::Dict(out))
}

fn is_no_data_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("no records found")
        || lower.contains("no record found")
        || lower.contains("nxdomain")
        || lower.contains("no such host")
        || lower.contains("nodata")
}

fn dns_error(message: impl Into<String>) -> ZuzuRustError {
    ZuzuRustError::runtime(format!("DNSException: {}", message.into()))
}
