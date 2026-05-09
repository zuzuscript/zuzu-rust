use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;
use std::rc::Rc;

use mysql::prelude::Queryable;
use mysql::{Opts, OptsBuilder, Params as MyParams, Value as MyValue};
use postgres::types::{ToSql, Type as PgType};
use postgres::{Client as PgClient, NoTls};
use rusqlite::types::{Value as SqliteValue, ValueRef as SqliteValueRef};
use rusqlite::{params_from_iter, Connection as SqliteConnection};

use super::super::{
    FieldSpec, IteratorState, MethodValue, ObjectValue, Runtime, TraitValue, UserClassValue, Value,
};
use super::io::{path_buf_from_value, resolve_fs_path};
use crate::error::{Result, ZuzuRustError};

#[derive(Default)]
pub(crate) struct DbState {
    next_db_id: usize,
    next_stmt_id: usize,
    dbs: HashMap<String, DbHandle>,
    db_statements: HashMap<String, DbStatement>,
}

struct DbHandle {
    connection: DbConnection,
    settings: ConnectSettings,
}

enum DbConnection {
    Sqlite(SqliteConnection),
    Postgres(PgClient),
    MySql(mysql::Conn),
}

struct DbStatement {
    db_id: String,
    sql: String,
    rows: Vec<Vec<Value>>,
    columns: Vec<ColumnMeta>,
    cursor: usize,
}

#[derive(Debug, Clone)]
struct ConnectSettings {
    auto_commit: bool,
    isolation_level: Option<String>,
}

#[derive(Clone)]
struct ColumnMeta {
    name: String,
    type_name: String,
    type_code: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DbBackend {
    Sqlite,
    Postgres,
    MySql,
}

#[derive(Debug, Clone)]
struct DbTarget {
    backend: DbBackend,
    body: String,
    params: HashMap<String, String>,
}

enum PgParam {
    Null(Option<String>),
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
}

impl PgParam {
    fn as_tosql(&self) -> &(dyn ToSql + Sync) {
        match self {
            PgParam::Null(value) => value,
            PgParam::Bool(value) => value,
            PgParam::Int(value) => value,
            PgParam::Float(value) => value,
            PgParam::Text(value) => value,
            PgParam::Bytes(value) => value,
        }
    }
}

pub(super) fn exports() -> HashMap<String, Value> {
    HashMap::from([("DB".to_owned(), Value::builtin_class("DB".to_owned()))])
}

pub(super) fn call_class_method(
    runtime: &Runtime,
    class_name: &str,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    if class_name != "DB" {
        return None;
    }
    let value = match name {
        "temp" => call_db_temp(runtime, args),
        "connect" => call_db_connect(runtime, args),
        "open" => call_db_open(runtime, args),
        _ => return None,
    };
    Some(value)
}

pub(super) fn call_object_method(
    runtime: &Runtime,
    class_name: &str,
    builtin_value: &Value,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    let id = match builtin_value {
        Value::Dict(fields) => fields.get("id").map(render_string),
        _ => None,
    }?;
    let value = match class_name {
        "DatabaseHandle" => call_db_handle_method(runtime, &id, name, args),
        "StatementHandle" => call_statement_method(runtime, &id, name, args),
        _ => return None,
    };
    Some(value)
}

pub(super) fn has_builtin_object_method(class_name: &str, name: &str) -> bool {
    matches!(
        (class_name, name),
        ("DatabaseHandle", "prepare")
            | ("DatabaseHandle", "quote")
            | ("DatabaseHandle", "begin")
            | ("DatabaseHandle", "commit")
            | ("DatabaseHandle", "rollback")
            | ("DatabaseHandle", "execute_batch")
            | ("StatementHandle", "execute")
            | ("StatementHandle", "execute_batch")
            | ("StatementHandle", "column_names")
            | ("StatementHandle", "column_types")
            | ("StatementHandle", "next_array")
            | ("StatementHandle", "next_dict")
            | ("StatementHandle", "all_array")
            | ("StatementHandle", "all_dict")
            | ("StatementHandle", "next_typed_array")
            | ("StatementHandle", "next_typed_dict")
            | ("StatementHandle", "all_typed_array")
            | ("StatementHandle", "all_typed_dict")
            | ("StatementHandle", "to_Iterator")
    )
}

fn call_db_temp(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    let settings = parse_connect_settings(args.first());
    let connection = SqliteConnection::open_in_memory()
        .map_err(|err| db_runtime_error("connect failed", err))?;
    let mut handle = DbHandle {
        connection: DbConnection::Sqlite(connection),
        settings,
    };
    begin_if_autocommit_disabled(&mut handle)?;
    Ok(new_db_handle(runtime, handle))
}

fn call_db_connect(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    let dsn = args.first().map(render_string).unwrap_or_default();
    let target = DbTarget::from_dbi_dsn(&dsn).ok_or_else(|| {
        ZuzuRustError::thrown(format!(
            "connect failed [08001] for DSN '{dsn}': this runtime supports {} DSNs only",
            DbBackend::policy_names()
        ))
    })?;
    let settings = parse_connect_settings(args.get(1));
    let mut handle = connect_target(runtime, &target, settings)?;
    begin_if_autocommit_disabled(&mut handle)?;
    Ok(new_db_handle(runtime, handle))
}

fn call_db_open(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    runtime.assert_capability("fs", "DB.open is denied by runtime policy")?;
    let path = coerce_path_or_string(runtime, args.first());
    let settings = parse_connect_settings(args.get(1));
    let connection = SqliteConnection::open(&path).map_err(|err| {
        db_runtime_error(
            &format!("connect failed for DSN 'dbi:SQLite:dbname={path}'"),
            err,
        )
    })?;
    let mut handle = DbHandle {
        connection: DbConnection::Sqlite(connection),
        settings,
    };
    begin_if_autocommit_disabled(&mut handle)?;
    Ok(new_db_handle(runtime, handle))
}

fn connect_target(
    runtime: &Runtime,
    target: &DbTarget,
    settings: ConnectSettings,
) -> Result<DbHandle> {
    let connection = match target.backend {
        DbBackend::Sqlite => {
            let dbname = target
                .param("dbname")
                .or_else(|| target.param("database"))
                .unwrap_or(":memory:");
            let connection = if dbname.eq_ignore_ascii_case(":memory:") || dbname.is_empty() {
                SqliteConnection::open_in_memory()
            } else {
                let path = resolve_fs_path(runtime, Path::new(dbname));
                SqliteConnection::open(path)
            }
            .map_err(|err| db_runtime_error("connect failed", err))?;
            DbConnection::Sqlite(connection)
        }
        DbBackend::Postgres => {
            let conninfo = postgres_conninfo(target);
            DbConnection::Postgres(PgClient::connect(&conninfo, NoTls).map_err(|err| {
                db_runtime_error(
                    &format!("connect failed for DSN '{}'", target.original_dsn()),
                    err,
                )
            })?)
        }
        DbBackend::MySql => {
            let opts = mysql_opts(target).map_err(|err| {
                ZuzuRustError::thrown(format!(
                    "connect failed for DSN '{}': {err}",
                    target.original_dsn()
                ))
            })?;
            DbConnection::MySql(mysql::Conn::new(opts).map_err(|err| {
                db_runtime_error(
                    &format!("connect failed for DSN '{}'", target.original_dsn()),
                    err,
                )
            })?)
        }
    };
    Ok(DbHandle {
        connection,
        settings,
    })
}

fn call_db_handle_method(
    runtime: &Runtime,
    db_id: &str,
    name: &str,
    args: &[Value],
) -> Result<Value> {
    match name {
        "prepare" => {
            let sql = args.first().map(render_string).unwrap_or_default();
            if sql.trim().is_empty() {
                return Err(ZuzuRustError::thrown(
                    "prepare failed: empty SQL statement".to_owned(),
                ));
            }
            validate_prepare(runtime, db_id, &sql)?;
            let stmt_id = alloc_id(&mut runtime.db_state.borrow_mut().next_stmt_id, "stmt");
            runtime.db_state.borrow_mut().db_statements.insert(
                stmt_id.clone(),
                DbStatement {
                    db_id: db_id.to_owned(),
                    sql,
                    rows: Vec::new(),
                    columns: Vec::new(),
                    cursor: 0,
                },
            );
            Ok(new_statement_handle(&stmt_id))
        }
        "quote" => {
            let raw = args.first().map(render_string);
            Ok(Value::String(sql_quote(raw.as_deref())))
        }
        "begin" => {
            let mut state = runtime.db_state.borrow_mut();
            let Some(handle) = state.dbs.get_mut(db_id) else {
                return Ok(Value::Null);
            };
            begin_transaction(handle)?;
            Ok(clone_db_handle(db_id))
        }
        "commit" => {
            let mut state = runtime.db_state.borrow_mut();
            let Some(handle) = state.dbs.get_mut(db_id) else {
                return Ok(Value::Null);
            };
            execute_db_statement(handle, "commit", &[])?;
            Ok(clone_db_handle(db_id))
        }
        "rollback" => {
            let mut state = runtime.db_state.borrow_mut();
            let Some(handle) = state.dbs.get_mut(db_id) else {
                return Ok(Value::Null);
            };
            execute_db_statement(handle, "rollback", &[])?;
            Ok(clone_db_handle(db_id))
        }
        "execute_batch" => {
            let sql = args.first().map(render_string).unwrap_or_default();
            let rows = args.get(1).map(array_values).unwrap_or_default();
            let mut state = runtime.db_state.borrow_mut();
            let Some(handle) = state.dbs.get_mut(db_id) else {
                return Ok(Value::Null);
            };
            for row in rows {
                let binds = array_values(&row);
                execute_db_statement(handle, &sql, &binds)?;
            }
            Ok(clone_db_handle(db_id))
        }
        _ => Err(ZuzuRustError::thrown(format!(
            "unsupported method '{}' for DatabaseHandle",
            name
        ))),
    }
}

fn call_statement_method(
    runtime: &Runtime,
    stmt_id: &str,
    name: &str,
    args: &[Value],
) -> Result<Value> {
    match name {
        "execute" => {
            let binds = args.to_vec();
            let (db_id, sql) = {
                let state = runtime.db_state.borrow();
                let Some(stmt) = state.db_statements.get(stmt_id) else {
                    return Ok(Value::Null);
                };
                (stmt.db_id.clone(), stmt.sql.clone())
            };
            let result = {
                let mut state = runtime.db_state.borrow_mut();
                let Some(handle) = state.dbs.get_mut(&db_id) else {
                    return Ok(Value::Null);
                };
                execute_db_statement(handle, &sql, &binds)?
            };
            if let Some(stmt) = runtime.db_state.borrow_mut().db_statements.get_mut(stmt_id) {
                stmt.rows = result.rows;
                stmt.columns = result.columns;
                stmt.cursor = 0;
            }
            Ok(clone_statement_handle(stmt_id))
        }
        "execute_batch" => {
            let rows = args.first().map(array_values).unwrap_or_default();
            let (db_id, sql) = {
                let state = runtime.db_state.borrow();
                let Some(stmt) = state.db_statements.get(stmt_id) else {
                    return Ok(Value::Null);
                };
                (stmt.db_id.clone(), stmt.sql.clone())
            };
            let mut state = runtime.db_state.borrow_mut();
            let Some(handle) = state.dbs.get_mut(&db_id) else {
                return Ok(Value::Null);
            };
            for row in rows {
                let binds = array_values(&row);
                execute_db_statement(handle, &sql, &binds)?;
            }
            Ok(clone_statement_handle(stmt_id))
        }
        "column_names" => {
            let state = runtime.db_state.borrow();
            let Some(stmt) = state.db_statements.get(stmt_id) else {
                return Ok(Value::Null);
            };
            Ok(Value::Array(
                stmt.columns
                    .iter()
                    .map(|column| Value::String(column.name.clone()))
                    .collect(),
            ))
        }
        "column_types" => {
            let state = runtime.db_state.borrow();
            let Some(stmt) = state.db_statements.get(stmt_id) else {
                return Ok(Value::Null);
            };
            Ok(Value::Array(
                stmt.columns
                    .iter()
                    .map(|column| {
                        Value::Dict(HashMap::from([
                            ("name".to_owned(), Value::String(column.type_name.clone())),
                            ("code".to_owned(), column.type_code.clone()),
                        ]))
                    })
                    .collect(),
            ))
        }
        "next_array" => {
            Ok(next_row_array(runtime, stmt_id, false).unwrap_or(Value::Array(Vec::new())))
        }
        "next_typed_array" => {
            Ok(next_row_array(runtime, stmt_id, true).unwrap_or(Value::Array(Vec::new())))
        }
        "next_dict" => {
            Ok(next_row_dict(runtime, stmt_id, false).unwrap_or(Value::Dict(HashMap::new())))
        }
        "next_typed_dict" => {
            Ok(next_row_dict(runtime, stmt_id, true).unwrap_or(Value::Dict(HashMap::new())))
        }
        "all_array" => {
            Ok(all_rows_array(runtime, stmt_id, false).unwrap_or(Value::Array(Vec::new())))
        }
        "all_typed_array" => {
            Ok(all_rows_array(runtime, stmt_id, true).unwrap_or(Value::Array(Vec::new())))
        }
        "all_dict" => {
            Ok(all_rows_dict(runtime, stmt_id, false).unwrap_or(Value::Array(Vec::new())))
        }
        "all_typed_dict" => {
            Ok(all_rows_dict(runtime, stmt_id, true).unwrap_or(Value::Array(Vec::new())))
        }
        "to_Iterator" => Ok(statement_iterator(runtime, stmt_id)),
        _ => Err(ZuzuRustError::thrown(format!(
            "unsupported method '{}' for StatementHandle",
            name
        ))),
    }
}

fn validate_prepare(runtime: &Runtime, db_id: &str, sql: &str) -> Result<()> {
    let mut state = runtime.db_state.borrow_mut();
    let Some(handle) = state.dbs.get_mut(db_id) else {
        return Ok(());
    };
    match &mut handle.connection {
        DbConnection::Sqlite(connection) => connection
            .prepare(sql)
            .map(|_| ())
            .map_err(|err| db_runtime_error("prepare failed", err)),
        DbConnection::Postgres(_) | DbConnection::MySql(_) => Ok(()),
    }
}

struct QueryResult {
    rows: Vec<Vec<Value>>,
    columns: Vec<ColumnMeta>,
}

fn execute_db_statement(handle: &mut DbHandle, sql: &str, binds: &[Value]) -> Result<QueryResult> {
    match &mut handle.connection {
        DbConnection::Sqlite(connection) => execute_sqlite(connection, sql, binds),
        DbConnection::Postgres(client) => execute_postgres(client, sql, binds),
        DbConnection::MySql(connection) => execute_mysql(connection, sql, binds),
    }
}

fn execute_sqlite(
    connection: &mut SqliteConnection,
    sql: &str,
    binds: &[Value],
) -> Result<QueryResult> {
    let params = binds.iter().map(sqlite_param).collect::<Vec<_>>();
    let mut stmt = connection
        .prepare(sql)
        .map_err(|err| db_runtime_error("prepare failed", err))?;
    let column_count = stmt.column_count();
    if column_count == 0 {
        stmt.execute(params_from_iter(params.iter()))
            .map_err(|err| db_runtime_error("execute failed", err))?;
        return Ok(QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        });
    }

    let names = stmt
        .column_names()
        .iter()
        .map(|name| (*name).to_owned())
        .collect::<Vec<_>>();
    let mut rows = Vec::new();
    {
        let mut sqlite_rows = stmt
            .query(params_from_iter(params.iter()))
            .map_err(|err| db_runtime_error("execute failed", err))?;
        while let Some(row) = sqlite_rows
            .next()
            .map_err(|err| db_runtime_error("fetch row failed", err))?
        {
            let mut values = Vec::new();
            for index in 0..column_count {
                values.push(sqlite_value_to_zuzu(
                    row.get_ref(index)
                        .map_err(|err| db_runtime_error("fetch row failed", err))?,
                ));
            }
            rows.push(values);
        }
    }
    let columns = infer_columns_from_rows(names, &rows);
    Ok(QueryResult { rows, columns })
}

fn execute_postgres(client: &mut PgClient, sql: &str, binds: &[Value]) -> Result<QueryResult> {
    let sql = translate_postgres_placeholders(sql);
    let params = binds.iter().map(pg_param).collect::<Vec<_>>();
    let params = params
        .iter()
        .map(PgParam::as_tosql)
        .collect::<Vec<&(dyn ToSql + Sync)>>();
    let statement = client
        .prepare(&sql)
        .map_err(|err| db_runtime_error("prepare failed", err))?;
    let columns = statement
        .columns()
        .iter()
        .map(pg_column_meta)
        .collect::<Vec<_>>();
    let rows = client
        .query(&statement, &params)
        .map_err(|err| db_runtime_error("execute failed", err))?;
    let values = rows
        .iter()
        .map(pg_row_to_values)
        .collect::<Result<Vec<_>>>()?;
    Ok(QueryResult {
        rows: values,
        columns,
    })
}

fn execute_mysql(connection: &mut mysql::Conn, sql: &str, binds: &[Value]) -> Result<QueryResult> {
    let params = MyParams::Positional(binds.iter().map(mysql_param).collect());
    let mut result = connection
        .exec_iter(sql, params)
        .map_err(|err| db_runtime_error("execute failed", err))?;
    let columns = result
        .columns()
        .as_ref()
        .iter()
        .map(mysql_column_meta)
        .collect::<Vec<_>>();
    let mut rows = Vec::new();
    for row in result.by_ref() {
        let row = row.map_err(|err| db_runtime_error("fetch row failed", err))?;
        rows.push(
            row.unwrap_raw()
                .into_iter()
                .map(mysql_value_to_zuzu)
                .collect(),
        );
    }
    Ok(QueryResult { rows, columns })
}

fn next_row_array(runtime: &Runtime, stmt_id: &str, typed: bool) -> Option<Value> {
    let mut state = runtime.db_state.borrow_mut();
    let stmt = state.db_statements.get_mut(stmt_id)?;
    let row = stmt.rows.get(stmt.cursor).cloned().unwrap_or_default();
    stmt.cursor = stmt.cursor.saturating_add(1);
    if typed {
        Some(Value::Array(coerce_row_types(&row, &stmt.columns)))
    } else {
        Some(Value::Array(row))
    }
}

fn next_row_dict(runtime: &Runtime, stmt_id: &str, typed: bool) -> Option<Value> {
    let mut state = runtime.db_state.borrow_mut();
    let stmt = state.db_statements.get_mut(stmt_id)?;
    if stmt.cursor >= stmt.rows.len() {
        stmt.cursor = stmt.cursor.saturating_add(1);
        return Some(Value::Dict(HashMap::new()));
    }
    let row = stmt.rows.get(stmt.cursor).cloned().unwrap_or_default();
    stmt.cursor = stmt.cursor.saturating_add(1);
    let row = if typed {
        coerce_row_types(&row, &stmt.columns)
    } else {
        row
    };
    Some(Value::Dict(row_to_dict(&stmt.columns, &row)))
}

fn all_rows_array(runtime: &Runtime, stmt_id: &str, typed: bool) -> Option<Value> {
    let mut rows = Vec::new();
    while let Some(value) = next_row_array(runtime, stmt_id, typed) {
        if matches!(&value, Value::Array(values) if values.is_empty()) {
            break;
        }
        rows.push(value);
    }
    Some(Value::Array(rows))
}

fn all_rows_dict(runtime: &Runtime, stmt_id: &str, typed: bool) -> Option<Value> {
    let mut rows = Vec::new();
    while let Some(value) = next_row_dict(runtime, stmt_id, typed) {
        if matches!(&value, Value::Dict(values) if values.is_empty()) {
            break;
        }
        rows.push(value);
    }
    Some(Value::Array(rows))
}

fn statement_iterator(runtime: &Runtime, stmt_id: &str) -> Value {
    let mut rows = Vec::new();
    while let Some(value) = next_row_dict(runtime, stmt_id, true) {
        if matches!(&value, Value::Dict(values) if values.is_empty()) {
            break;
        }
        rows.push(value);
    }
    Value::Iterator(Rc::new(RefCell::new(IteratorState {
        items: rows,
        index: 0,
    })))
}

fn next_class(name: &str) -> Rc<UserClassValue> {
    Rc::new(UserClassValue {
        name: name.to_owned(),
        base: None,
        traits: Vec::<Rc<TraitValue>>::new(),
        fields: vec![FieldSpec {
            name: "id".to_owned(),
            declared_type: Some("String".to_owned()),
            mutable: false,
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

fn new_db_handle(runtime: &Runtime, handle: DbHandle) -> Value {
    let db_id = alloc_id(&mut runtime.db_state.borrow_mut().next_db_id, "db");
    runtime
        .db_state
        .borrow_mut()
        .dbs
        .insert(db_id.clone(), handle);
    object_with_id("DatabaseHandle", &db_id)
}

fn new_statement_handle(stmt_id: &str) -> Value {
    object_with_id("StatementHandle", stmt_id)
}

fn clone_db_handle(db_id: &str) -> Value {
    object_with_id("DatabaseHandle", db_id)
}

fn clone_statement_handle(stmt_id: &str) -> Value {
    object_with_id("StatementHandle", stmt_id)
}

fn object_with_id(class_name: &str, id: &str) -> Value {
    let fields = HashMap::from([("id".to_owned(), Value::String(id.to_owned()))]);
    Value::Object(Rc::new(RefCell::new(ObjectValue {
        class: next_class(class_name),
        fields: fields.clone(),
        weak_fields: std::collections::HashSet::new(),
        builtin_value: Some(Value::Dict(fields)),
    })))
}

fn begin_if_autocommit_disabled(handle: &mut DbHandle) -> Result<()> {
    if handle.settings.auto_commit {
        return Ok(());
    }
    begin_transaction(handle)
}

fn begin_transaction(handle: &mut DbHandle) -> Result<()> {
    let mode = handle
        .settings
        .isolation_level
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase();
    match &mut handle.connection {
        DbConnection::Sqlite(_)
            if matches!(mode.as_str(), "deferred" | "immediate" | "exclusive") =>
        {
            execute_db_statement(handle, &format!("begin {mode} transaction"), &[])?;
        }
        _ => {
            execute_db_statement(handle, "begin", &[])?;
        }
    }
    Ok(())
}

fn parse_connect_settings(value: Option<&Value>) -> ConnectSettings {
    let mut settings = ConnectSettings {
        auto_commit: true,
        isolation_level: None,
    };
    let Some(Value::Dict(raw)) = value else {
        return settings;
    };
    if let Some(auto_commit) = raw.get("auto_commit") {
        settings.auto_commit = auto_commit.is_truthy();
    }
    if let Some(level) = raw.get("isolation_level") {
        let normalized = render_string(level).to_ascii_lowercase();
        if matches!(normalized.as_str(), "immediate" | "exclusive" | "deferred") {
            settings.isolation_level = Some(normalized);
        }
    }
    settings
}

impl DbBackend {
    fn from_driver(driver: &str) -> Option<Self> {
        match driver.to_ascii_lowercase().as_str() {
            "sqlite" => Some(Self::Sqlite),
            "pg" | "postgres" | "postgresql" => Some(Self::Postgres),
            "mysql" | "mariadb" => Some(Self::MySql),
            _ => None,
        }
    }

    fn policy_names() -> &'static str {
        "SQLite, PostgreSQL, and MariaDB/MySQL"
    }
}

impl DbTarget {
    fn from_dbi_dsn(dsn: &str) -> Option<Self> {
        let trimmed = dsn.trim();
        let after_dbi = strip_prefix_ci(trimmed, "dbi:")?;
        let (driver, body) = after_dbi.split_once(':').unwrap_or((after_dbi, ""));
        let backend = DbBackend::from_driver(driver)?;
        Some(Self {
            backend,
            body: body.to_owned(),
            params: parse_dbi_params(body),
        })
    }

    fn param(&self, key: &str) -> Option<&str> {
        self.params
            .get(&key.to_ascii_lowercase())
            .map(String::as_str)
    }

    fn original_dsn(&self) -> String {
        let driver = match self.backend {
            DbBackend::Sqlite => "SQLite",
            DbBackend::Postgres => "Pg",
            DbBackend::MySql => "mysql",
        };
        format!("dbi:{driver}:{}", self.body)
    }
}

fn parse_dbi_params(body: &str) -> HashMap<String, String> {
    let mut params = HashMap::new();
    for chunk in body.split(';') {
        let Some((name, value)) = chunk.split_once('=') else {
            continue;
        };
        params.insert(name.trim().to_ascii_lowercase(), value.trim().to_owned());
    }
    params
}

fn postgres_conninfo(target: &DbTarget) -> String {
    if target.body.starts_with("postgres://") || target.body.starts_with("postgresql://") {
        return target.body.clone();
    }
    let mut parts = Vec::new();
    for (dbi_key, pg_key) in [
        ("host", "host"),
        ("port", "port"),
        ("dbname", "dbname"),
        ("database", "dbname"),
        ("user", "user"),
        ("username", "user"),
        ("password", "password"),
    ] {
        if let Some(value) = target.param(dbi_key) {
            parts.push(format!("{pg_key}='{}'", value.replace('\'', "\\'")));
        }
    }
    parts.join(" ")
}

fn mysql_opts(target: &DbTarget) -> std::result::Result<Opts, mysql::UrlError> {
    if target.body.starts_with("mysql://") {
        return Opts::from_url(&target.body);
    }
    let mut builder = OptsBuilder::new();
    if let Some(host) = target.param("host") {
        builder = builder.ip_or_hostname(Some(host));
    }
    if let Some(port) = target
        .param("port")
        .and_then(|value| value.parse::<u16>().ok())
    {
        builder = builder.tcp_port(port);
    }
    if let Some(user) = target.param("user").or_else(|| target.param("username")) {
        builder = builder.user(Some(user));
    }
    if let Some(password) = target.param("password") {
        builder = builder.pass(Some(password));
    }
    if let Some(dbname) = target
        .param("dbname")
        .or_else(|| target.param("database"))
        .or_else(|| target.param("db"))
    {
        builder = builder.db_name(Some(dbname));
    }
    Ok(Opts::from(builder))
}

fn sqlite_param(value: &Value) -> SqliteValue {
    match value {
        Value::Shared(value) => sqlite_param(&value.borrow()),
        Value::Null => SqliteValue::Null,
        Value::Boolean(value) => SqliteValue::Integer(if *value { 1 } else { 0 }),
        Value::Number(value) if value.is_finite() && value.fract() == 0.0 => {
            SqliteValue::Integer(*value as i64)
        }
        Value::Number(value) => SqliteValue::Real(*value),
        Value::String(value) => SqliteValue::Text(value.clone()),
        Value::BinaryString(bytes) => SqliteValue::Blob(bytes.clone()),
        other => SqliteValue::Text(other.render()),
    }
}

fn pg_param(value: &Value) -> PgParam {
    match value {
        Value::Shared(value) => pg_param(&value.borrow()),
        Value::Null => PgParam::Null(None),
        Value::Boolean(value) => PgParam::Bool(*value),
        Value::Number(value) if value.is_finite() && value.fract() == 0.0 => {
            PgParam::Int(*value as i64)
        }
        Value::Number(value) => PgParam::Float(*value),
        Value::String(value) => PgParam::Text(value.clone()),
        Value::BinaryString(bytes) => PgParam::Bytes(bytes.clone()),
        other => PgParam::Text(other.render()),
    }
}

fn mysql_param(value: &Value) -> MyValue {
    match value {
        Value::Shared(value) => mysql_param(&value.borrow()),
        Value::Null => MyValue::NULL,
        Value::Boolean(value) => MyValue::Int(if *value { 1 } else { 0 }),
        Value::Number(value) if value.is_finite() && value.fract() == 0.0 => {
            MyValue::Int(*value as i64)
        }
        Value::Number(value) => MyValue::Double(*value),
        Value::String(value) => MyValue::Bytes(value.as_bytes().to_vec()),
        Value::BinaryString(bytes) => MyValue::Bytes(bytes.clone()),
        other => MyValue::Bytes(other.render().into_bytes()),
    }
}

fn sqlite_value_to_zuzu(value: SqliteValueRef<'_>) -> Value {
    match value {
        SqliteValueRef::Null => Value::Null,
        SqliteValueRef::Integer(value) => Value::Number(value as f64),
        SqliteValueRef::Real(value) => Value::Number(value),
        SqliteValueRef::Text(bytes) => Value::String(String::from_utf8_lossy(bytes).to_string()),
        SqliteValueRef::Blob(bytes) => Value::BinaryString(bytes.to_vec()),
    }
}

fn pg_row_to_values(row: &postgres::Row) -> Result<Vec<Value>> {
    row.columns()
        .iter()
        .enumerate()
        .map(|(index, column)| pg_value_to_zuzu(row, index, column.type_()))
        .collect()
}

fn pg_value_to_zuzu(row: &postgres::Row, index: usize, ty: &PgType) -> Result<Value> {
    if *ty == PgType::BOOL {
        return Ok(row
            .try_get::<usize, Option<bool>>(index)
            .map_err(|err| db_runtime_error("fetch row failed", err))?
            .map(Value::Boolean)
            .unwrap_or(Value::Null));
    }
    if matches!(*ty, PgType::INT2 | PgType::INT4 | PgType::OID) {
        return Ok(row
            .try_get::<usize, Option<i32>>(index)
            .map_err(|err| db_runtime_error("fetch row failed", err))?
            .map(|value| Value::Number(value as f64))
            .unwrap_or(Value::Null));
    }
    if *ty == PgType::INT8 {
        return Ok(row
            .try_get::<usize, Option<i64>>(index)
            .map_err(|err| db_runtime_error("fetch row failed", err))?
            .map(|value| Value::Number(value as f64))
            .unwrap_or(Value::Null));
    }
    if *ty == PgType::FLOAT4 {
        return Ok(row
            .try_get::<usize, Option<f32>>(index)
            .map_err(|err| db_runtime_error("fetch row failed", err))?
            .map(|value| Value::Number(value as f64))
            .unwrap_or(Value::Null));
    }
    if *ty == PgType::FLOAT8 {
        return Ok(row
            .try_get::<usize, Option<f64>>(index)
            .map_err(|err| db_runtime_error("fetch row failed", err))?
            .map(Value::Number)
            .unwrap_or(Value::Null));
    }
    if *ty == PgType::BYTEA {
        return Ok(row
            .try_get::<usize, Option<Vec<u8>>>(index)
            .map_err(|err| db_runtime_error("fetch row failed", err))?
            .map(Value::BinaryString)
            .unwrap_or(Value::Null));
    }
    Ok(row
        .try_get::<usize, Option<String>>(index)
        .map_err(|err| db_runtime_error("fetch row failed", err))?
        .map(Value::String)
        .unwrap_or(Value::Null))
}

fn mysql_value_to_zuzu(value: Option<MyValue>) -> Value {
    match value.unwrap_or(MyValue::NULL) {
        MyValue::NULL => Value::Null,
        MyValue::Bytes(bytes) => match String::from_utf8(bytes.clone()) {
            Ok(text) => Value::String(text),
            Err(_) => Value::BinaryString(bytes),
        },
        MyValue::Int(value) => Value::Number(value as f64),
        MyValue::UInt(value) => Value::Number(value as f64),
        MyValue::Float(value) => Value::Number(value as f64),
        MyValue::Double(value) => Value::Number(value),
        MyValue::Date(year, month, day, hour, minute, second, micros) => Value::String(format!(
            "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{micros:06}"
        )),
        MyValue::Time(negative, days, hour, minute, second, micros) => Value::String(format!(
            "{}{days} {hour:02}:{minute:02}:{second:02}.{micros:06}",
            if negative { "-" } else { "" }
        )),
    }
}

fn pg_column_meta(column: &postgres::Column) -> ColumnMeta {
    let type_name = column.type_().name().to_owned();
    ColumnMeta {
        name: column.name().to_owned(),
        type_name: type_name.clone(),
        type_code: Value::String(type_name),
    }
}

fn mysql_column_meta(column: &mysql::Column) -> ColumnMeta {
    let type_name = format!("{:?}", column.column_type()).to_ascii_lowercase();
    ColumnMeta {
        name: column.name_str().to_string(),
        type_name: type_name.clone(),
        type_code: Value::String(type_name),
    }
}

fn infer_columns_from_rows(names: Vec<String>, rows: &[Vec<Value>]) -> Vec<ColumnMeta> {
    names
        .into_iter()
        .enumerate()
        .map(|(index, name)| {
            let type_name = rows
                .iter()
                .filter_map(|row| row.get(index))
                .find(|value| !matches!(value, Value::Null))
                .map(infer_type_name)
                .unwrap_or_else(|| "unknown".to_owned());
            ColumnMeta {
                name,
                type_name: type_name.clone(),
                type_code: Value::String(type_name),
            }
        })
        .collect()
}

fn infer_type_name(value: &Value) -> String {
    match value {
        Value::Number(value) if value.fract() == 0.0 => "integer",
        Value::Number(_) => "real",
        Value::Boolean(_) => "boolean",
        Value::BinaryString(_) => "blob",
        Value::String(_) => "text",
        _ => "unknown",
    }
    .to_owned()
}

fn row_to_dict(columns: &[ColumnMeta], row: &[Value]) -> HashMap<String, Value> {
    let mut out = HashMap::new();
    for (index, column) in columns.iter().enumerate() {
        out.insert(
            column.name.clone(),
            row.get(index).cloned().unwrap_or(Value::Null),
        );
    }
    out
}

fn coerce_row_types(row: &[Value], columns: &[ColumnMeta]) -> Vec<Value> {
    row.iter()
        .enumerate()
        .map(|(index, value)| coerce_db_value(value.clone(), columns.get(index)))
        .collect()
}

fn coerce_db_value(value: Value, column: Option<&ColumnMeta>) -> Value {
    let type_name = column
        .map(|column| column.type_name.to_ascii_lowercase())
        .unwrap_or_default();
    if is_integer_type(&type_name) || is_boolean_type(&type_name) {
        Value::Number(value.to_number().unwrap_or(0.0).round())
    } else if is_float_type(&type_name) {
        Value::Number(value.to_number().unwrap_or(0.0))
    } else {
        value
    }
}

fn is_integer_type(type_name: &str) -> bool {
    type_name.contains("int")
        || matches!(
            type_name,
            "serial" | "bigserial" | "smallserial" | "oid" | "long" | "longlong" | "short"
        )
}

fn is_boolean_type(type_name: &str) -> bool {
    type_name.contains("bool") || type_name.contains("tiny")
}

fn is_float_type(type_name: &str) -> bool {
    type_name.contains("real")
        || type_name.contains("float")
        || type_name.contains("double")
        || type_name.contains("decimal")
        || type_name.contains("numeric")
}

fn array_values(value: &Value) -> Vec<Value> {
    match value {
        Value::Array(values) => values.clone(),
        Value::Shared(shared) => array_values(&shared.borrow()),
        _ => Vec::new(),
    }
}

fn translate_postgres_placeholders(sql: &str) -> String {
    let mut out = String::new();
    let mut index = 1;
    let mut chars = sql.chars().peekable();
    let mut in_single = false;
    while let Some(ch) = chars.next() {
        if ch == '\'' {
            out.push(ch);
            if in_single && chars.peek() == Some(&'\'') {
                out.push(chars.next().unwrap());
            } else {
                in_single = !in_single;
            }
            continue;
        }
        if ch == '?' && !in_single {
            out.push('$');
            out.push_str(&index.to_string());
            index += 1;
        } else {
            out.push(ch);
        }
    }
    out
}

fn strip_prefix_ci<'a>(text: &'a str, prefix: &str) -> Option<&'a str> {
    text.get(..prefix.len())
        .filter(|head| head.eq_ignore_ascii_case(prefix))
        .map(|_| &text[prefix.len()..])
}

fn sql_quote(value: Option<&str>) -> String {
    match value {
        Some(value) => format!("'{}'", value.replace('\'', "''")),
        None => "NULL".to_owned(),
    }
}

fn alloc_id(next: &mut usize, prefix: &str) -> String {
    *next = next.saturating_add(1);
    format!("{prefix}-{next}")
}

fn coerce_path_or_string(runtime: &Runtime, value: Option<&Value>) -> String {
    value
        .map(path_buf_from_value)
        .map(|path| resolve_fs_path(runtime, &path))
        .unwrap_or_default()
        .to_string_lossy()
        .replace('\\', "/")
}

fn render_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::BinaryString(bytes) => String::from_utf8_lossy(bytes).to_string(),
        Value::Number(value) => value.to_string(),
        Value::Boolean(value) => {
            if *value {
                "true".to_owned()
            } else {
                "false".to_owned()
            }
        }
        Value::Null => String::new(),
        other => other.render(),
    }
}

fn db_runtime_error(context: &str, err: impl std::fmt::Display) -> ZuzuRustError {
    ZuzuRustError::thrown(format!("{context}: {err}"))
}
