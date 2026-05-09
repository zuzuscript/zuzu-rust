use std::collections::HashMap;

use super::super::{Runtime, Value};
use crate::error::{Result, ZuzuRustError};

pub(super) fn exports() -> HashMap<String, Value> {
    HashMap::from([
        ("Task".to_owned(), Value::builtin_class("Task".to_owned())),
        (
            "Channel".to_owned(),
            Value::builtin_class("Channel".to_owned()),
        ),
        (
            "CancellationToken".to_owned(),
            Value::builtin_class("CancellationToken".to_owned()),
        ),
        (
            "CancellationSource".to_owned(),
            Value::builtin_class("CancellationSource".to_owned()),
        ),
        (
            "resolved".to_owned(),
            Value::native_function("task.resolved".to_owned()),
        ),
        (
            "failed".to_owned(),
            Value::native_function("task.failed".to_owned()),
        ),
        (
            "sleep".to_owned(),
            Value::native_function("task.sleep".to_owned()),
        ),
        (
            "yield".to_owned(),
            Value::native_function("task.yield".to_owned()),
        ),
        (
            "all".to_owned(),
            Value::native_function("task.all".to_owned()),
        ),
        (
            "race".to_owned(),
            Value::native_function("task.race".to_owned()),
        ),
        (
            "timeout".to_owned(),
            Value::native_function("task.timeout".to_owned()),
        ),
    ])
}

pub(super) fn call(runtime: &Runtime, name: &str, args: &[Value]) -> Option<Result<Value>> {
    let result = match name {
        "task.resolved" => Ok(runtime.task_resolved(args.first().cloned().unwrap_or(Value::Null))),
        "task.failed" => Ok(runtime.task_rejected(
            args.first()
                .cloned()
                .unwrap_or_else(|| Value::String("Task failed".to_owned()))
                .render(),
        )),
        "task.sleep" => Ok(runtime.task_sleep(
            args.first()
                .map(|value| value.to_number().unwrap_or(0.0))
                .unwrap_or(0.0),
        )),
        "task.yield" => Ok(runtime.task_yield()),
        "task.all" => {
            let tasks = match args.first() {
                Some(Value::Array(items)) => items.clone(),
                Some(_) => {
                    return Some(Err(ZuzuRustError::runtime(
                        "TypeException: task combinator expects Array",
                    )))
                }
                None => Vec::new(),
            };
            if tasks.iter().any(|task| !matches!(task, Value::Task(_))) {
                return Some(Err(ZuzuRustError::runtime("all expects only Task values")));
            }
            Ok(runtime.task_all(tasks))
        }
        "task.race" => {
            let tasks = match args.first() {
                Some(Value::Array(items)) => items.clone(),
                Some(_) => {
                    return Some(Err(ZuzuRustError::runtime(
                        "TypeException: task combinator expects Array",
                    )))
                }
                None => Vec::new(),
            };
            if tasks.is_empty() {
                return Some(Err(ZuzuRustError::runtime(
                    "race expects at least one task",
                )));
            }
            if tasks.iter().any(|task| !matches!(task, Value::Task(_))) {
                return Some(Err(ZuzuRustError::runtime("race expects only Task values")));
            }
            Ok(runtime.task_race(tasks))
        }
        "task.timeout" => {
            if args.len() != 2 {
                Err(ZuzuRustError::runtime("timeout() expects seconds and task"))
            } else if !matches!(args[1], Value::Task(_)) {
                Err(ZuzuRustError::runtime("timeout expects a Task"))
            } else {
                Ok(runtime.task_timeout(args[0].to_number().unwrap_or(0.0), args[1].clone()))
            }
        }
        _ => return None,
    };
    Some(result)
}
