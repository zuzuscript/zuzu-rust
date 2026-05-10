use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::net::TcpStream;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};
use std::rc::Rc;
use std::time::Duration;

use openssl::ssl::{SslConnector, SslMethod, SslStream, SslVerifyMode};

use super::super::{
    FieldSpec, MethodValue, ObjectValue, Runtime, TraitValue, UserClassValue, Value,
};
use crate::error::{Result, ZuzuRustError};

const SENDMAIL_PATHS: &[&str] = &[
    "/usr/sbin/sendmail",
    "/usr/lib/sendmail",
    "/sbin/sendmail",
    "/usr/bin/sendmail",
];

#[derive(Clone)]
struct MailSendSpec {
    config: MailerConfig,
    from: String,
    recipients: Vec<String>,
    message: Vec<u8>,
    message_id: Option<String>,
}

#[derive(Clone)]
struct MailerConfig {
    transport: String,
    host: String,
    port: u16,
    timeout: Duration,
    tls: bool,
    starttls: bool,
    tls_verify: bool,
    tls_server_name: Option<String>,
    username: Option<String>,
    password: Option<String>,
    auth: Option<String>,
    smtputf8: bool,
    allow_insecure_auth: bool,
    reject_partial: bool,
    sendmail_path: Option<String>,
    sendmail_args: Vec<String>,
}

#[derive(Clone)]
struct MailOutcome {
    transport: String,
    accepted: Vec<String>,
    rejected: Vec<String>,
    message_id: Option<String>,
    response: String,
}

enum SmtpStream {
    Plain(TcpStream),
    Tls(SslStream<TcpStream>),
}

impl SmtpStream {
    fn connect(config: &MailerConfig) -> Result<Self> {
        let address = format!("{}:{}", config.host, config.port);
        let tcp = TcpStream::connect(&address)
            .map_err(|err| mail_error("mail.connection", format!("could not connect: {err}")))?;
        tcp.set_read_timeout(Some(config.timeout))
            .map_err(|err| mail_error("mail.connection", format!("set timeout failed: {err}")))?;
        tcp.set_write_timeout(Some(config.timeout))
            .map_err(|err| mail_error("mail.connection", format!("set timeout failed: {err}")))?;
        if config.tls {
            return tls_connect(tcp, config);
        }
        Ok(Self::Plain(tcp))
    }

    fn into_plain(self) -> Result<TcpStream> {
        match self {
            Self::Plain(stream) => Ok(stream),
            Self::Tls(_) => Err(mail_error(
                "mail.tls",
                "cannot start TLS on an already encrypted SMTP stream",
            )),
        }
    }
}

impl Read for SmtpStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.read(buf),
            Self::Tls(stream) => stream.read(buf),
        }
    }
}

impl Write for SmtpStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.write(buf),
            Self::Tls(stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(stream) => stream.flush(),
            Self::Tls(stream) => stream.flush(),
        }
    }
}

pub(super) fn exports() -> HashMap<String, Value> {
    HashMap::from([
        (
            "Mailer".to_owned(),
            Value::builtin_class("Mailer".to_owned()),
        ),
        (
            "MailResult".to_owned(),
            Value::builtin_class("MailResult".to_owned()),
        ),
    ])
}

pub(super) fn construct_mailer(
    args: Vec<Value>,
    named_args: Vec<(String, Value)>,
) -> Result<Value> {
    let mut config = HashMap::new();
    if let Some(first) = args.first() {
        config.extend(config_from_value(first));
    }
    for (key, value) in named_args {
        config.insert(key, value);
    }
    Ok(object(
        "Mailer",
        HashMap::from([(
            "_config".to_owned(),
            config_to_value(normalize_config(config)?),
        )]),
    ))
}

pub(super) fn call_class_method(
    class_name: &str,
    name: &str,
    _args: &[Value],
) -> Option<Result<Value>> {
    Some(match (class_name, name) {
        ("Mailer", "capabilities") => Ok(capabilities()),
        _ => return None,
    })
}

pub(super) fn has_class_method(class_name: &str, name: &str) -> bool {
    matches!((class_name, name), ("Mailer", "capabilities"))
}

pub(super) fn call_object_method(
    runtime: &Runtime,
    object: &Rc<RefCell<ObjectValue>>,
    class_name: &str,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    Some(match class_name {
        "Mailer" => mailer_method(runtime, object, name, args),
        "MailResult" => mail_result_method(object, name),
        _ => return None,
    })
}

pub(super) fn has_object_method(class_name: &str, name: &str) -> bool {
    matches!(
        (class_name, name),
        ("Mailer", "send") | ("Mailer", "send_async") | ("MailResult", "to_Dict")
    )
}

fn mailer_method(
    runtime: &Runtime,
    object: &Rc<RefCell<ObjectValue>>,
    name: &str,
    args: &[Value],
) -> Result<Value> {
    match name {
        "send" => {
            runtime.warn_blocking_operation("std/net/smtp Mailer.send")?;
            let spec = prepare_send(object, args)?;
            wrap_result(execute_send(spec)?)
        }
        "send_async" => {
            let spec = prepare_send(object, args)?;
            let cancel_requested = Rc::new(Cell::new(false));
            let future = async move {
                tokio::task::spawn_blocking(move || execute_send(spec))
                    .await
                    .map_err(|err| {
                        ZuzuRustError::runtime(format!("mail async worker failed: {err}"))
                    })?
                    .and_then(wrap_result)
            };
            Ok(runtime.task_native_async(future, Some(cancel_requested)))
        }
        _ => Err(ZuzuRustError::thrown(format!(
            "unsupported method '{name}' for Mailer"
        ))),
    }
}

fn mail_result_method(object: &Rc<RefCell<ObjectValue>>, name: &str) -> Result<Value> {
    match name {
        "to_Dict" => Ok(Value::Dict(object.borrow().fields.clone())),
        _ => Err(ZuzuRustError::thrown(format!(
            "unsupported method '{name}' for MailResult"
        ))),
    }
}

fn capabilities() -> Value {
    Value::Dict(HashMap::from([
        ("smtp".to_owned(), Value::Boolean(true)),
        (
            "sendmail".to_owned(),
            Value::Boolean(default_sendmail_path().is_some()),
        ),
        ("tls".to_owned(), Value::Boolean(true)),
        ("starttls".to_owned(), Value::Boolean(true)),
        (
            "auth".to_owned(),
            Value::Array(
                ["plain", "login", "xoauth2"]
                    .into_iter()
                    .map(|name| Value::String(name.to_owned()))
                    .collect(),
            ),
        ),
        ("async".to_owned(), Value::Boolean(true)),
    ]))
}

fn prepare_send(object: &Rc<RefCell<ObjectValue>>, args: &[Value]) -> Result<MailSendSpec> {
    if args.len() < 4 {
        return Err(mail_error(
            "mail.invalid_address",
            "Mailer.send expects envelope_from, envelope_to, headers, and body",
        ));
    }
    let base_config = match object.borrow().fields.get("_config") {
        Some(Value::Dict(fields)) => normalize_config(fields.clone())?,
        _ => normalize_config(HashMap::new())?,
    };
    let config = if let Some(options) = args.get(4) {
        let mut merged = config_value_to_map(&base_config);
        merged.extend(config_from_value(options));
        normalize_config(merged)?
    } else {
        base_config
    };
    reject_unsupported_security_options(&config)?;
    let from = validate_address(&render_string(&args[0]))?;
    let recipients = recipient_list(&args[1])?;
    validate_envelope_ascii(&config, std::iter::once(&from).chain(recipients.iter()))?;
    let (message, message_id) = serialize_message(&args[2], &args[3])?;
    Ok(MailSendSpec {
        config,
        from,
        recipients,
        message,
        message_id,
    })
}

fn execute_send(spec: MailSendSpec) -> Result<MailOutcome> {
    match spec.config.transport.as_str() {
        "sendmail" => sendmail_send(spec),
        "smtp" => smtp_send(spec),
        _ => Err(mail_error(
            "mail.unsupported",
            "transport must be 'smtp' or 'sendmail'",
        )),
    }
}

fn sendmail_send(spec: MailSendSpec) -> Result<MailOutcome> {
    let path = spec
        .config
        .sendmail_path
        .clone()
        .or_else(default_sendmail_path)
        .ok_or_else(|| {
            mail_error(
                "mail.unsupported",
                "sendmail transport is unavailable; configure sendmail_path",
            )
        })?;
    let mut args = spec.config.sendmail_args.clone();
    args.push("-i".to_owned());
    args.push("-f".to_owned());
    args.push(spec.from.clone());
    args.extend(spec.recipients.clone());

    let mut child = Command::new(&path)
        .args(&args)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| mail_error("mail.process", format!("sendmail failed to start: {err}")))?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(&spec.message)
            .map_err(|err| mail_error("mail.process", format!("sendmail stdin failed: {err}")))?;
    }
    let output = child
        .wait_with_output()
        .map_err(|err| mail_error("mail.process", format!("sendmail wait failed: {err}")))?;
    if !output.status.success() {
        let exit = output.status.code().unwrap_or(-1);
        let diag = String::from_utf8_lossy(&output.stderr)
            .chars()
            .take(4096)
            .collect::<String>();
        return Err(mail_error(
            "mail.process",
            format!(
                "sendmail exited with status {exit}{}",
                if diag.is_empty() {
                    String::new()
                } else {
                    format!(": {diag}")
                }
            ),
        ));
    }

    Ok(MailOutcome {
        transport: "sendmail".to_owned(),
        accepted: spec.recipients,
        rejected: Vec::new(),
        message_id: spec.message_id,
        response: "sendmail exit 0".to_owned(),
    })
}

fn smtp_send(spec: MailSendSpec) -> Result<MailOutcome> {
    reject_unsupported_smtp(&spec.config)?;
    let mut stream = SmtpStream::connect(&spec.config)?;

    let mut response = read_response(&mut stream)?;
    expect_code("mail.connection", &response, &[220], "SMTP greeting")?;

    response = command(&mut stream, "EHLO localhost")?;
    let mut extensions = HashSet::new();
    if response.code >= 500 {
        response = command(&mut stream, "HELO localhost")?;
        expect_code("mail.connection", &response, &[250], "HELO")?;
    } else {
        expect_code("mail.connection", &response, &[250], "EHLO")?;
        extensions = response.extensions();
    }
    if spec.config.starttls {
        if !extensions.contains("STARTTLS") {
            return Err(mail_error(
                "mail.tls",
                "SMTP server did not advertise STARTTLS",
            ));
        }
        response = command(&mut stream, "STARTTLS")?;
        expect_code("mail.tls", &response, &[220], "STARTTLS")?;
        stream = tls_connect(stream.into_plain()?, &spec.config)?;
        response = command(&mut stream, "EHLO localhost")?;
        expect_code("mail.connection", &response, &[250], "EHLO after STARTTLS")?;
        extensions = response.extensions();
    }
    smtp_authenticate(&mut stream, &spec.config, &response)?;
    if spec.config.smtputf8 && !extensions.contains("SMTPUTF8") {
        return Err(mail_error(
            "mail.unsupported",
            "SMTPUTF8 was requested but not advertised",
        ));
    }

    let mut mail_from = format!("MAIL FROM:<{}>", spec.from);
    if spec.config.smtputf8 {
        mail_from.push_str(" SMTPUTF8");
    }
    response = command(&mut stream, &mail_from)?;
    expect_code("mail.recipient", &response, &[250], "MAIL FROM")?;

    let mut accepted = Vec::new();
    let mut rejected = Vec::new();
    for recipient in &spec.recipients {
        response = command(&mut stream, &format!("RCPT TO:<{recipient}>"))?;
        if matches!(response.code, 250 | 251 | 252) {
            accepted.push(recipient.clone());
        } else {
            rejected.push(recipient.clone());
        }
    }
    if accepted.is_empty() {
        let _ = command(&mut stream, "QUIT");
        return Err(mail_error("mail.recipient", "all recipients were rejected"));
    }
    if !rejected.is_empty() && spec.config.reject_partial {
        let _ = command(&mut stream, "QUIT");
        return Err(mail_error(
            "mail.recipient",
            "one or more recipients were rejected",
        ));
    }

    response = command(&mut stream, "DATA")?;
    expect_code("mail.data", &response, &[354], "DATA")?;
    let stuffed = dot_stuff(&spec.message);
    stream
        .write_all(&stuffed)
        .map_err(|err| mail_error("mail.connection", format!("SMTP write failed: {err}")))?;
    if !spec.message.ends_with(b"\r\n") {
        stream
            .write_all(b"\r\n")
            .map_err(|err| mail_error("mail.connection", format!("SMTP write failed: {err}")))?;
    }
    stream
        .write_all(b".\r\n")
        .map_err(|err| mail_error("mail.connection", format!("SMTP write failed: {err}")))?;
    response = read_response(&mut stream)?;
    expect_code("mail.data", &response, &[250], "message DATA")?;
    let data_response = response.text();
    let _ = command(&mut stream, "QUIT");

    Ok(MailOutcome {
        transport: "smtp".to_owned(),
        accepted,
        rejected,
        message_id: spec.message_id,
        response: data_response,
    })
}

fn reject_unsupported_smtp(config: &MailerConfig) -> Result<()> {
    reject_unsupported_security_options(config)
}

fn reject_unsupported_security_options(config: &MailerConfig) -> Result<()> {
    if let Some(auth) = &config.auth {
        if !matches!(auth.as_str(), "plain" | "login" | "xoauth2") {
            return Err(mail_error(
                "mail.auth",
                format!("unsupported SMTP auth mechanism '{auth}'"),
            ));
        }
    }
    if (config.username.is_some() || config.password.is_some() || config.auth.is_some())
        && !config.tls
        && !config.starttls
        && !config.allow_insecure_auth
    {
        return Err(mail_error(
            "mail.auth",
            "SMTP authentication without TLS requires allow_insecure_auth: true",
        ));
    }
    Ok(())
}

fn tls_connect(stream: TcpStream, config: &MailerConfig) -> Result<SmtpStream> {
    let mut builder = SslConnector::builder(SslMethod::tls())
        .map_err(|err| mail_error("mail.tls", format!("TLS setup failed: {err}")))?;
    if !config.tls_verify {
        builder.set_verify(SslVerifyMode::NONE);
    }
    let connector = builder.build();
    let domain = config
        .tls_server_name
        .as_deref()
        .unwrap_or(config.host.as_str());
    connector
        .connect(domain, stream)
        .map(SmtpStream::Tls)
        .map_err(|err| mail_error("mail.tls", format!("TLS handshake failed: {err}")))
}

fn smtp_authenticate(
    stream: &mut SmtpStream,
    config: &MailerConfig,
    response: &SmtpResponse,
) -> Result<()> {
    if config.username.is_none() && config.password.is_none() && config.auth.is_none() {
        return Ok(());
    }
    let username = config.username.as_deref().unwrap_or("");
    let password = config.password.as_deref().unwrap_or("");
    let advertised = response.auth_mechanisms();
    let mut mechanism = config.auth.clone().unwrap_or_default();
    if mechanism.is_empty() {
        for candidate in ["plain", "login", "xoauth2"] {
            if advertised.contains(candidate) {
                mechanism = candidate.to_owned();
                break;
            }
        }
        if mechanism.is_empty() {
            mechanism = "plain".to_owned();
        }
    }
    if !matches!(mechanism.as_str(), "plain" | "login" | "xoauth2") {
        return Err(mail_error(
            "mail.auth",
            format!("unsupported SMTP auth mechanism '{mechanism}'"),
        ));
    }
    if !advertised.is_empty() && !advertised.contains(&mechanism) {
        return Err(mail_error(
            "mail.auth",
            format!("SMTP server did not advertise AUTH {mechanism}"),
        ));
    }
    match mechanism.as_str() {
        "plain" => {
            let token = base64(&[b"\0", username.as_bytes(), b"\0", password.as_bytes()].concat());
            let response = command(stream, &format!("AUTH PLAIN {token}"))?;
            expect_code("mail.auth", &response, &[235], "AUTH PLAIN")
        }
        "login" => {
            let mut response = command(stream, "AUTH LOGIN")?;
            expect_code("mail.auth", &response, &[334], "AUTH LOGIN")?;
            response = command(stream, &base64(username.as_bytes()))?;
            expect_code("mail.auth", &response, &[334], "AUTH username")?;
            response = command(stream, &base64(password.as_bytes()))?;
            expect_code("mail.auth", &response, &[235], "AUTH password")
        }
        "xoauth2" => {
            let token = format!("user={username}\x01auth=Bearer {password}\x01\x01");
            let response = command(
                stream,
                &format!("AUTH XOAUTH2 {}", base64(token.as_bytes())),
            )?;
            expect_code("mail.auth", &response, &[235], "AUTH XOAUTH2")
        }
        _ => unreachable!(),
    }
}

#[derive(Debug)]
struct SmtpResponse {
    code: u16,
    lines: Vec<String>,
}

impl SmtpResponse {
    fn text(&self) -> String {
        self.lines.join("\n")
    }

    fn extensions(&self) -> HashSet<String> {
        self.lines
            .iter()
            .filter_map(|line| line.get(4..))
            .filter_map(|text| text.trim().split_whitespace().next())
            .map(|name| name.to_ascii_uppercase())
            .collect()
    }

    fn auth_mechanisms(&self) -> HashSet<String> {
        let mut mechanisms = HashSet::new();
        for line in &self.lines {
            let Some(text) = line.get(4..) else {
                continue;
            };
            let text = text.trim();
            let mut parts = text.split_whitespace();
            let Some(first) = parts.next() else {
                continue;
            };
            if !first.eq_ignore_ascii_case("AUTH") {
                continue;
            }
            for mechanism in parts {
                mechanisms.insert(mechanism.to_ascii_lowercase());
            }
        }
        mechanisms
    }
}

fn read_response(stream: &mut SmtpStream) -> Result<SmtpResponse> {
    let mut lines = Vec::new();
    loop {
        let mut raw = Vec::new();
        let mut byte = [0_u8; 1];
        loop {
            let read = stream
                .read(&mut byte)
                .map_err(|err| mail_error("mail.connection", format!("SMTP read failed: {err}")))?;
            if read == 0 {
                return Err(mail_error(
                    "mail.connection",
                    "SMTP server closed the connection",
                ));
            }
            raw.push(byte[0]);
            if byte[0] == b'\n' {
                break;
            }
        }
        let mut line = String::from_utf8_lossy(&raw).to_string();
        while line.ends_with('\n') || line.ends_with('\r') {
            line.pop();
        }
        let done = line.as_bytes().get(3) == Some(&b' ');
        let continued = line.as_bytes().get(3) == Some(&b'-');
        lines.push(line);
        if done || !continued {
            break;
        }
    }
    let code = lines
        .first()
        .and_then(|line| line.get(0..3))
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| {
            mail_error(
                "mail.connection",
                "SMTP server returned an invalid response",
            )
        })?;
    Ok(SmtpResponse { code, lines })
}

fn command(stream: &mut SmtpStream, line: &str) -> Result<SmtpResponse> {
    stream
        .write_all(line.as_bytes())
        .and_then(|_| stream.write_all(b"\r\n"))
        .map_err(|err| mail_error("mail.connection", format!("SMTP write failed: {err}")))?;
    stream
        .flush()
        .map_err(|err| mail_error("mail.connection", format!("SMTP write failed: {err}")))?;
    read_response(stream)
}

fn expect_code(
    category: &str,
    response: &SmtpResponse,
    codes: &[u16],
    context: &str,
) -> Result<()> {
    if codes.contains(&response.code) {
        return Ok(());
    }
    Err(mail_error(
        category,
        format!("{context} failed: {}", response.text()),
    ))
}

fn serialize_message(headers: &Value, body: &Value) -> Result<(Vec<u8>, Option<String>)> {
    let headers = dereference_shared(headers);
    let body = dereference_shared(body);
    let Value::PairList(pairs) = &headers else {
        return Err(mail_error(
            "mail.invalid_headers",
            format!("headers expects PairList, got {}", headers.type_name()),
        ));
    };
    let Value::BinaryString(body_bytes) = &body else {
        return Err(ZuzuRustError::thrown(format!(
            "TypeException: Mailer.send body expects BinaryString, got {}",
            body.type_name()
        )));
    };
    let mut message = Vec::new();
    let mut message_id = None;
    for (name, value) in pairs {
        let name = validate_header_name(name)?;
        let value_bytes = header_value_bytes(&name, value)?;
        message.extend_from_slice(name.as_bytes());
        message.extend_from_slice(b": ");
        message.extend_from_slice(&value_bytes);
        message.extend_from_slice(b"\r\n");
        if message_id.is_none() && name.eq_ignore_ascii_case("message-id") {
            message_id = Some(String::from_utf8_lossy(&value_bytes).to_string());
        }
    }
    message.extend_from_slice(b"\r\n");
    message.extend_from_slice(body_bytes);
    Ok((message, message_id))
}

fn validate_header_name(name: &str) -> Result<String> {
    if name.is_empty() {
        return Err(mail_error(
            "mail.invalid_headers",
            "header name must not be empty",
        ));
    }
    if !name
        .bytes()
        .all(|byte| matches!(byte, b'!'..=b'9' | b';'..=b'~'))
    {
        return Err(mail_error(
            "mail.invalid_headers",
            format!("invalid header name '{name}'"),
        ));
    }
    Ok(name.to_owned())
}

fn header_value_bytes(name: &str, value: &Value) -> Result<Vec<u8>> {
    let value = dereference_shared(value);
    let bytes = match &value {
        Value::String(text) => text.as_bytes().to_vec(),
        Value::BinaryString(bytes) => bytes.clone(),
        other => {
            return Err(mail_error(
                "mail.invalid_headers",
                format!(
                    "header '{name}' expects String or BinaryString, got {}",
                    other.type_name()
                ),
            ))
        }
    };
    if bytes.iter().any(|byte| matches!(byte, b'\r' | b'\n')) {
        return Err(mail_error(
            "mail.invalid_headers",
            format!("header '{name}' value must not contain CR or LF"),
        ));
    }
    Ok(bytes)
}

fn dot_stuff(message: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(message.len() + 8);
    if message.first() == Some(&b'.') {
        out.push(b'.');
    }
    for (index, byte) in message.iter().enumerate() {
        out.push(*byte);
        if *byte == b'\n'
            && index > 0
            && message[index - 1] == b'\r'
            && message.get(index + 1) == Some(&b'.')
        {
            out.push(b'.');
        }
    }
    out
}

fn recipient_list(value: &Value) -> Result<Vec<String>> {
    let value = dereference_shared(value);
    let recipients = match &value {
        Value::Array(items) => items.iter().map(render_string).collect::<Vec<_>>(),
        other => vec![render_string(other)],
    };
    if recipients.is_empty() {
        return Err(mail_error(
            "mail.invalid_address",
            "at least one envelope recipient is required",
        ));
    }
    for recipient in &recipients {
        validate_address(recipient)?;
    }
    Ok(recipients)
}

fn validate_address(address: &str) -> Result<String> {
    if address.is_empty() {
        return Err(mail_error(
            "mail.invalid_address",
            "envelope address must not be empty",
        ));
    }
    if address.bytes().any(|byte| byte <= 0x1f || byte == 0x7f) {
        return Err(mail_error(
            "mail.invalid_address",
            "envelope address contains a control character",
        ));
    }
    if address.contains('<') || address.contains('>') {
        return Err(mail_error(
            "mail.invalid_address",
            "envelope address must not contain angle brackets",
        ));
    }
    Ok(address.to_owned())
}

fn validate_envelope_ascii<'a>(
    config: &MailerConfig,
    addresses: impl Iterator<Item = &'a String>,
) -> Result<()> {
    if config.smtputf8 {
        return Ok(());
    }
    for address in addresses {
        if !address.is_ascii() {
            return Err(mail_error(
                "mail.invalid_address",
                "non-ASCII envelope addresses require smtputf8: true",
            ));
        }
    }
    Ok(())
}

fn normalize_config(mut raw: HashMap<String, Value>) -> Result<MailerConfig> {
    let transport = raw
        .remove("transport")
        .map(|value| render_string(&value).to_ascii_lowercase())
        .unwrap_or_else(|| "smtp".to_owned());
    if transport != "smtp" && transport != "sendmail" {
        return Err(mail_error(
            "mail.unsupported",
            "transport must be 'smtp' or 'sendmail'",
        ));
    }
    let submission = raw.get("submission").map(Value::is_truthy).unwrap_or(false);
    let host = raw
        .remove("host")
        .map(|value| render_string(&value))
        .unwrap_or_else(|| "localhost".to_owned());
    let port = raw
        .remove("port")
        .and_then(|value| value.to_number().ok())
        .map(|value| value as u16)
        .unwrap_or(if submission { 587 } else { 25 });
    let timeout_seconds = raw
        .remove("timeout")
        .and_then(|value| value.to_number().ok())
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or(30.0);
    let starttls = if raw.contains_key("starttls") {
        raw.remove("starttls")
            .map(|value| value.is_truthy())
            .unwrap_or(false)
    } else {
        submission
    };
    Ok(MailerConfig {
        transport,
        host,
        port,
        timeout: Duration::from_secs_f64(timeout_seconds),
        tls: raw
            .remove("tls")
            .map(|value| value.is_truthy())
            .unwrap_or(false),
        starttls,
        tls_verify: raw
            .remove("tls_verify")
            .map(|value| value.is_truthy())
            .unwrap_or(true),
        tls_server_name: string_option(raw.remove("tls_server_name")),
        username: string_option(raw.remove("username")),
        password: string_option(raw.remove("password")),
        auth: string_option(raw.remove("auth")),
        smtputf8: raw
            .remove("smtputf8")
            .map(|value| value.is_truthy())
            .unwrap_or(false),
        allow_insecure_auth: raw
            .remove("allow_insecure_auth")
            .map(|value| value.is_truthy())
            .unwrap_or(false),
        reject_partial: raw
            .remove("reject_partial")
            .map(|value| value.is_truthy())
            .unwrap_or(false),
        sendmail_path: string_option(raw.remove("sendmail_path")),
        sendmail_args: match raw.remove("sendmail_args") {
            Some(Value::Array(items)) => {
                validate_sendmail_args(items.iter().map(render_string).collect())?
            }
            Some(Value::Null) | None => Vec::new(),
            Some(other) => {
                return Err(mail_error(
                    "mail.invalid_address",
                    format!("sendmail_args expects Array, got {}", other.type_name()),
                ))
            }
        },
    })
}

fn config_from_value(value: &Value) -> HashMap<String, Value> {
    match dereference_shared(value) {
        Value::Dict(map) => map,
        Value::PairList(pairs) => pairs.into_iter().collect(),
        _ => HashMap::new(),
    }
}

fn validate_sendmail_args(args: Vec<String>) -> Result<Vec<String>> {
    for arg in &args {
        if arg == "--read-recipients" {
            return Err(mail_error(
                "mail.unsupported",
                "sendmail_args must not enable header-derived recipients",
            ));
        }
        if let Some(rest) = arg.strip_prefix('-') {
            if !rest.starts_with('-')
                && !rest.is_empty()
                && rest.chars().all(|ch| ch.is_ascii_alphabetic())
                && rest.contains('t')
            {
                return Err(mail_error(
                    "mail.unsupported",
                    "sendmail_args must not enable header-derived recipients",
                ));
            }
        }
    }
    Ok(args)
}

fn config_value_to_map(config: &MailerConfig) -> HashMap<String, Value> {
    match config_to_value(config.clone()) {
        Value::Dict(map) => map,
        _ => HashMap::new(),
    }
}

fn config_to_value(config: MailerConfig) -> Value {
    Value::Dict(HashMap::from([
        ("transport".to_owned(), Value::String(config.transport)),
        ("host".to_owned(), Value::String(config.host)),
        ("port".to_owned(), Value::Number(f64::from(config.port))),
        (
            "timeout".to_owned(),
            Value::Number(config.timeout.as_secs_f64()),
        ),
        ("tls".to_owned(), Value::Boolean(config.tls)),
        ("starttls".to_owned(), Value::Boolean(config.starttls)),
        ("tls_verify".to_owned(), Value::Boolean(config.tls_verify)),
        (
            "tls_server_name".to_owned(),
            config
                .tls_server_name
                .map(Value::String)
                .unwrap_or(Value::Null),
        ),
        (
            "username".to_owned(),
            config.username.map(Value::String).unwrap_or(Value::Null),
        ),
        (
            "password".to_owned(),
            config.password.map(Value::String).unwrap_or(Value::Null),
        ),
        (
            "auth".to_owned(),
            config.auth.map(Value::String).unwrap_or(Value::Null),
        ),
        ("smtputf8".to_owned(), Value::Boolean(config.smtputf8)),
        (
            "allow_insecure_auth".to_owned(),
            Value::Boolean(config.allow_insecure_auth),
        ),
        (
            "reject_partial".to_owned(),
            Value::Boolean(config.reject_partial),
        ),
        (
            "sendmail_path".to_owned(),
            config
                .sendmail_path
                .map(Value::String)
                .unwrap_or(Value::Null),
        ),
        (
            "sendmail_args".to_owned(),
            Value::Array(
                config
                    .sendmail_args
                    .into_iter()
                    .map(Value::String)
                    .collect(),
            ),
        ),
    ]))
}

fn string_option(value: Option<Value>) -> Option<String> {
    match value {
        Some(Value::Null) | None => None,
        Some(value) => Some(render_string(&value)),
    }
}

fn default_sendmail_path() -> Option<String> {
    SENDMAIL_PATHS
        .iter()
        .find(|path| is_executable_file(path))
        .map(|path| (*path).to_owned())
}

#[cfg(unix)]
fn is_executable_file(path: &str) -> bool {
    std::fs::metadata(path)
        .map(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn is_executable_file(path: &str) -> bool {
    std::fs::metadata(path)
        .map(|meta| meta.is_file())
        .unwrap_or(false)
}

fn wrap_result(outcome: MailOutcome) -> Result<Value> {
    Ok(object(
        "MailResult",
        HashMap::from([
            ("transport".to_owned(), Value::String(outcome.transport)),
            (
                "accepted".to_owned(),
                Value::Array(outcome.accepted.into_iter().map(Value::String).collect()),
            ),
            (
                "rejected".to_owned(),
                Value::Array(outcome.rejected.into_iter().map(Value::String).collect()),
            ),
            (
                "message_id".to_owned(),
                outcome.message_id.map(Value::String).unwrap_or(Value::Null),
            ),
            ("response".to_owned(), Value::String(outcome.response)),
        ]),
    ))
}

fn object(class_name: &str, fields: HashMap<String, Value>) -> Value {
    Value::Object(Rc::new(RefCell::new(ObjectValue {
        class: class(class_name),
        fields: fields.clone(),
        weak_fields: HashSet::new(),
        builtin_value: Some(Value::Dict(fields)),
    })))
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
        Value::Shared(cell) => render_string(&cell.borrow()),
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Boolean(true) => "true".to_owned(),
        Value::Boolean(false) => "false".to_owned(),
        Value::Null => String::new(),
        other => other.render(),
    }
}

fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    let mut i = 0;
    while i < bytes.len() {
        let b0 = bytes[i];
        let b1 = bytes.get(i + 1).copied().unwrap_or(0);
        let b2 = bytes.get(i + 2).copied().unwrap_or(0);
        let triple = ((b0 as u32) << 16) | ((b1 as u32) << 8) | (b2 as u32);
        out.push(ALPHABET[((triple >> 18) & 0x3f) as usize] as char);
        out.push(ALPHABET[((triple >> 12) & 0x3f) as usize] as char);
        if i + 1 < bytes.len() {
            out.push(ALPHABET[((triple >> 6) & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
        if i + 2 < bytes.len() {
            out.push(ALPHABET[(triple & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
        i += 3;
    }
    out
}

fn dereference_shared(value: &Value) -> Value {
    match value {
        Value::Shared(cell) => cell.borrow().clone(),
        _ => value.clone(),
    }
}

fn mail_error(category: &str, message: impl Into<String>) -> ZuzuRustError {
    ZuzuRustError::thrown(format!("{category}: {}", message.into()))
}
