use std::collections::HashMap;
use std::rc::Rc;

use super::super::{ControlFlow, Environment, Runtime, Value};
use crate::ast::Statement;
use crate::error::{Result, ZuzuRustError};
use crate::{
    parse_program_with_compile_options_and_source_file, OptimizationOptions, ParseOptions,
};

pub(super) fn exports() -> HashMap<String, Value> {
    HashMap::from([("eval".to_owned(), Value::native_function("eval".to_owned()))])
}

pub(super) fn call(
    runtime: &Runtime,
    name: &str,
    args: &[Value],
    named_args: &[(String, Value)],
) -> Option<Result<Value>> {
    if name != "eval" {
        return None;
    }
    Some(eval_source(runtime, args, named_args))
}

fn eval_source(runtime: &Runtime, args: &[Value], named_args: &[(String, Value)]) -> Result<Value> {
    let Some(Value::String(source)) = args.first() else {
        let got = args
            .first()
            .map(|value| value.type_name())
            .unwrap_or("Null");
        return Err(ZuzuRustError::runtime(format!(
            "TypeException: eval expects String, got {got}"
        )));
    };
    for (key, _) in named_args {
        if !matches!(
            key.as_str(),
            "deny_fs"
                | "deny_net"
                | "deny_perl"
                | "deny_js"
                | "deny_proc"
                | "deny_db"
                | "deny_clib"
                | "deny_gui"
                | "deny_worker"
        ) {
            return Err(ZuzuRustError::runtime(format!(
                "Unknown named argument '{key}' for eval"
            )));
        }
    }
    let Some(env) = runtime.current_env() else {
        return Err(ZuzuRustError::runtime(
            "eval requires an active caller scope",
        ));
    };
    let env = Rc::new(Environment::new(Some(env)));
    let options = ParseOptions::new(false, true, OptimizationOptions::o1());
    let program = match parse_program_with_compile_options_and_source_file(
        source,
        &options,
        Some("<std/eval>"),
    ) {
        Ok(program) => program,
        Err(err) => return Err(eval_compile_exception(runtime, err)?),
    };
    let mut scoped_denials = Vec::new();
    for (key, value) in named_args {
        if runtime.value_is_truthy(value)? {
            scoped_denials.push(key.clone());
        }
    }
    if !scoped_denials.is_empty() {
        runtime.push_special_props_scope();
        for key in &scoped_denials {
            runtime.set_special_prop(key, Value::Boolean(true));
        }
    }
    let result = eval_program_in_env(runtime, &program.statements, env);
    if !scoped_denials.is_empty() {
        runtime.pop_special_props_scope();
    }
    result
}

fn eval_compile_exception(runtime: &Runtime, err: ZuzuRustError) -> Result<ZuzuRustError> {
    let (message, line, code) = match err {
        ZuzuRustError::Lex { message, line, .. } => (message, line, "E_COMPILE_LEX"),
        ZuzuRustError::Parse { message, line, .. } => (message, line, "E_COMPILE_SYNTAX"),
        ZuzuRustError::IncompleteParse { message, line, .. } => {
            (message, line, "E_COMPILE_INCOMPLETE")
        }
        ZuzuRustError::Semantic { message, line, .. } => (message, line, "E_COMPILE_SEMANTIC"),
        other => return Ok(other),
    };
    let value = runtime.make_exception_object_with_code(
        message,
        Some("<std/eval>"),
        line,
        Value::String(code.to_owned()),
    );
    Ok(ZuzuRustError::thrown_with_token(
        runtime.render_value(&value)?,
        runtime.store_thrown_value(value)?,
    ))
}

fn eval_program_in_env(
    runtime: &Runtime,
    statements: &[Statement],
    env: Rc<Environment>,
) -> Result<Value> {
    if statements.is_empty() {
        return Ok(Value::Null);
    }
    for statement in &statements[..statements.len().saturating_sub(1)] {
        match runtime.eval_statement(statement, Rc::clone(&env))? {
            ControlFlow::Normal => {}
            ControlFlow::Throw(value) => {
                return match &value {
                    Value::String(text) => Err(ZuzuRustError::thrown(text.clone())),
                    _ => Err(ZuzuRustError::thrown(runtime.render_value(&value)?)),
                };
            }
            ControlFlow::Return(_) => {
                return Err(ZuzuRustError::runtime("return is not valid in std/eval"))
            }
            ControlFlow::Break | ControlFlow::Continue => {
                return Err(ZuzuRustError::runtime(
                    "loop control is not valid in std/eval",
                ))
            }
        }
    }

    let Some(last) = statements.last() else {
        return Ok(Value::Null);
    };
    match last {
        Statement::ExpressionStatement(node) => runtime.eval_expression(&node.expression, env),
        other => match runtime.eval_statement(other, env)? {
            ControlFlow::Normal => Ok(Value::Null),
            ControlFlow::Throw(value) => match &value {
                Value::String(text) => Err(ZuzuRustError::thrown(text.clone())),
                _ => Err(ZuzuRustError::thrown(runtime.render_value(&value)?)),
            },
            ControlFlow::Return(_) => {
                Err(ZuzuRustError::runtime("return is not valid in std/eval"))
            }
            ControlFlow::Break | ControlFlow::Continue => Err(ZuzuRustError::runtime(
                "loop control is not valid in std/eval",
            )),
        },
    }
}
