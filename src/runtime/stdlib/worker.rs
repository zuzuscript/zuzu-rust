use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::mpsc::{self, TryRecvError};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::Duration;

use super::super::{
    FieldSpec, MethodValue, ObjectValue, Runtime, TraitValue, UserClassValue, Value,
    WorkerEndpointState, WorkerFrame, DENIAL_CAPABILITIES,
};
use super::marshal;
use crate::error::{Result, ZuzuRustError};

type WorkerReply = std::result::Result<Vec<u8>, String>;

pub(super) fn exports() -> HashMap<String, Value> {
    HashMap::from([
        (
            "Worker".to_owned(),
            Value::builtin_class("Worker".to_owned()),
        ),
        (
            "WorkerHandle".to_owned(),
            Value::builtin_class("WorkerHandle".to_owned()),
        ),
    ])
}

pub(super) fn call_class_method(
    runtime: &Runtime,
    class_name: &str,
    name: &str,
    args: &[Value],
    named_args: &[(String, Value)],
) -> Option<Result<Value>> {
    if class_name != "Worker" {
        return None;
    }
    let value = match name {
        "spawn" => worker_spawn(runtime, args, named_args),
        "spawn_handle" => worker_spawn_handle(runtime, args, named_args),
        _ => Err(ZuzuRustError::thrown(format!(
            "unsupported method '{name}' for Worker"
        ))),
    };
    Some(value)
}

pub(super) fn has_class_method(class_name: &str, name: &str) -> bool {
    class_name == "Worker" && matches!(name, "spawn" | "spawn_handle")
}

pub(super) fn call_object_method(
    runtime: &Runtime,
    object: &Rc<RefCell<ObjectValue>>,
    class_name: &str,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    if !matches!(class_name, "WorkerHandle" | "WorkerInbox") {
        return None;
    }
    let value = match (class_name, name) {
        ("WorkerHandle", "send") => worker_endpoint_send(runtime, object, args, "WorkerHandle"),
        ("WorkerHandle", "recv") => worker_endpoint_recv(runtime, object, args, "WorkerHandle"),
        ("WorkerHandle", "close") => worker_endpoint_close(runtime, object, args, "WorkerHandle"),
        ("WorkerHandle", "cancel") => worker_handle_cancel(runtime, object, args),
        ("WorkerHandle", "result") => worker_handle_result(object, args),
        ("WorkerHandle", "status") => worker_handle_status(runtime, object, args),
        ("WorkerHandle", "done") => worker_handle_done(runtime, object, args),
        ("WorkerInbox", "send") => worker_endpoint_send(runtime, object, args, "WorkerInbox"),
        ("WorkerInbox", "recv") => worker_endpoint_recv(runtime, object, args, "WorkerInbox"),
        ("WorkerInbox", "close") => worker_endpoint_close(runtime, object, args, "WorkerInbox"),
        _ => Err(ZuzuRustError::thrown(format!(
            "unsupported method '{name}' for {class_name}"
        ))),
    };
    Some(value)
}

pub(super) fn has_builtin_object_method(class_name: &str, name: &str) -> bool {
    matches!(
        (class_name, name),
        ("WorkerHandle", "send")
            | ("WorkerHandle", "recv")
            | ("WorkerHandle", "close")
            | ("WorkerHandle", "cancel")
            | ("WorkerHandle", "result")
            | ("WorkerHandle", "status")
            | ("WorkerHandle", "done")
            | ("WorkerInbox", "send")
            | ("WorkerInbox", "recv")
            | ("WorkerInbox", "close")
    )
}

fn worker_spawn(
    runtime: &Runtime,
    args: &[Value],
    named_args: &[(String, Value)],
) -> Result<Value> {
    runtime.assert_capability("worker", "Worker.spawn is denied by runtime policy")?;
    if args.is_empty() || args.len() > 2 {
        return Err(ZuzuRustError::runtime(
            "Worker.spawn expects Callable and optional Array arguments",
        ));
    }

    let worker_args = match args.get(1) {
        Some(Value::Array(values)) => values.clone(),
        Some(value) => {
            return Err(ZuzuRustError::runtime(format!(
                "TypeException: Worker.spawn expects Array arguments, got {}",
                value.type_name()
            )));
        }
        None => Vec::new(),
    };
    let extra_denials = parse_denial_args("Worker.spawn", named_args)?;
    let child_policy = runtime.child_runtime_policy(&extra_denials);
    let module_roots = runtime.module_roots_clone();
    let request = Value::Array(vec![args[0].clone(), Value::Array(worker_args)]);
    let request_bytes = marshal::dump_value(runtime, &request)?;
    let (tx, rx) = mpsc::channel::<WorkerReply>();
    let parent_cancel_requested = Rc::new(Cell::new(false));
    let worker_cancel_requested = Arc::new(AtomicBool::new(false));
    let thread_cancel_requested = Arc::clone(&worker_cancel_requested);

    thread::spawn(move || {
        let reply = run_worker(
            module_roots,
            child_policy,
            request_bytes,
            thread_cancel_requested,
        )
        .map_err(error_text);
        let _ = tx.send(reply);
    });

    let task_runtime = runtime.clone();
    let load_runtime = runtime.clone();
    let future_cancel_requested = Rc::clone(&parent_cancel_requested);
    let future_worker_cancel_requested = Arc::clone(&worker_cancel_requested);
    Ok(task_runtime.task_native_async(
        async move {
            loop {
                if future_cancel_requested.get() {
                    future_worker_cancel_requested.store(true, Ordering::SeqCst);
                    return Err(ZuzuRustError::thrown(
                        "CancelledException: worker cancelled",
                    ));
                }
                match rx.try_recv() {
                    Ok(Ok(bytes)) => return marshal::load_value(&load_runtime, &bytes),
                    Ok(Err(message)) => return Err(ZuzuRustError::thrown(message)),
                    Err(TryRecvError::Empty) => {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    Err(TryRecvError::Disconnected) => {
                        return Err(ZuzuRustError::runtime("worker thread disconnected"));
                    }
                }
            }
        },
        Some(parent_cancel_requested),
    ))
}

fn worker_spawn_handle(
    runtime: &Runtime,
    args: &[Value],
    named_args: &[(String, Value)],
) -> Result<Value> {
    runtime.assert_capability("worker", "Worker.spawn_handle is denied by runtime policy")?;
    if args.is_empty() || args.len() > 2 {
        return Err(ZuzuRustError::runtime(
            "Worker.spawn_handle expects Callable and optional Array arguments",
        ));
    }

    let worker_args = match args.get(1) {
        Some(Value::Array(values)) => values.clone(),
        Some(value) => {
            return Err(ZuzuRustError::runtime(format!(
                "TypeException: Worker.spawn_handle expects Array arguments, got {}",
                value.type_name()
            )));
        }
        None => Vec::new(),
    };
    let extra_denials = parse_denial_args("Worker.spawn_handle", named_args)?;
    let child_policy = runtime.child_runtime_policy(&extra_denials);
    let module_roots = runtime.module_roots_clone();
    let request = Value::Array(vec![args[0].clone(), Value::Array(worker_args)]);
    let request_bytes = marshal::dump_value(runtime, &request)?;

    let (parent_tx, child_rx) = mpsc::channel::<WorkerFrame>();
    let (child_tx, parent_rx) = mpsc::channel::<WorkerFrame>();
    let (result_tx, result_rx) = mpsc::channel::<WorkerReply>();
    let parent_cancel_requested = Rc::new(Cell::new(false));
    let worker_cancel_requested = Arc::new(AtomicBool::new(false));
    let thread_cancel_requested = Arc::clone(&worker_cancel_requested);

    thread::spawn(move || {
        let reply = run_worker_handle(
            module_roots,
            child_policy,
            request_bytes,
            child_rx,
            child_tx,
            thread_cancel_requested,
        )
        .map_err(error_text);
        let _ = result_tx.send(reply);
    });

    let endpoint_id = runtime.register_worker_endpoint(WorkerEndpointState {
        sender: parent_tx,
        receiver: parent_rx,
        queue: Vec::new(),
        local_closed: false,
        remote_closed: false,
        cancelled: false,
        cancel_requested: Arc::clone(&worker_cancel_requested),
    });

    let task_runtime = runtime.clone();
    let load_runtime = runtime.clone();
    let future_cancel_requested = Rc::clone(&parent_cancel_requested);
    let future_worker_cancel_requested = Arc::clone(&worker_cancel_requested);
    let result_task = task_runtime.task_native_async(
        async move {
            loop {
                if future_cancel_requested.get() {
                    future_worker_cancel_requested.store(true, Ordering::SeqCst);
                    return Err(ZuzuRustError::thrown(
                        "CancelledException: worker cancelled",
                    ));
                }
                match result_rx.try_recv() {
                    Ok(Ok(bytes)) => return marshal::load_value(&load_runtime, &bytes),
                    Ok(Err(message)) => return Err(ZuzuRustError::thrown(message)),
                    Err(TryRecvError::Empty) => {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    Err(TryRecvError::Disconnected) => {
                        return Err(ZuzuRustError::runtime("worker thread disconnected"));
                    }
                }
            }
        },
        Some(parent_cancel_requested),
    );

    Ok(worker_endpoint_object(
        "WorkerHandle",
        endpoint_id,
        Some(result_task),
    ))
}

fn parse_denial_args(label: &str, named_args: &[(String, Value)]) -> Result<Vec<(String, bool)>> {
    let mut denials = Vec::new();
    for (key, value) in named_args {
        let Some(capability) = key.strip_prefix("deny_") else {
            return Err(ZuzuRustError::runtime(format!(
                "Unknown named argument '{key}' for {label}"
            )));
        };
        if !DENIAL_CAPABILITIES.contains(&capability) {
            return Err(ZuzuRustError::runtime(format!(
                "Unknown named argument '{key}' for {label}"
            )));
        }
        let Value::Boolean(denied) = value else {
            return Err(ZuzuRustError::runtime(format!(
                "TypeException: {label} named argument '{key}' expects Boolean, got {}",
                value.type_name()
            )));
        };
        denials.push((capability.to_owned(), *denied));
    }
    Ok(denials)
}

fn run_worker_handle(
    module_roots: Vec<std::path::PathBuf>,
    policy: super::super::RuntimePolicy,
    request_bytes: Vec<u8>,
    inbox_rx: mpsc::Receiver<WorkerFrame>,
    inbox_tx: mpsc::Sender<WorkerFrame>,
    cancel_requested: Arc<AtomicBool>,
) -> Result<Vec<u8>> {
    let runtime = Runtime::with_policy_and_worker_cancel(
        module_roots,
        policy,
        Some(Arc::clone(&cancel_requested)),
    );
    runtime.enter_async_context(|| {
        run_worker_handle_inner(
            &runtime,
            request_bytes,
            inbox_rx,
            inbox_tx,
            cancel_requested,
        )
    })
}

fn run_worker_handle_inner(
    runtime: &Runtime,
    request_bytes: Vec<u8>,
    inbox_rx: mpsc::Receiver<WorkerFrame>,
    inbox_tx: mpsc::Sender<WorkerFrame>,
    cancel_requested: Arc<AtomicBool>,
) -> Result<Vec<u8>> {
    runtime.check_worker_cancelled()?;
    let request = unwrap_shared(marshal::load_value(&runtime, &request_bytes)?);
    let Value::Array(mut request) = request else {
        return Err(ZuzuRustError::runtime(
            "Worker.spawn_handle payload did not decode as Array",
        ));
    };
    if request.len() != 2 {
        return Err(ZuzuRustError::runtime(
            "Worker.spawn_handle payload has invalid arity",
        ));
    }
    let arguments = unwrap_shared(request.pop().unwrap_or(Value::Array(Vec::new())));
    let callable = request.pop().unwrap_or(Value::Null);
    let Value::Array(arguments) = arguments else {
        return Err(ZuzuRustError::runtime(
            "Worker.spawn_handle payload arguments did not decode as Array",
        ));
    };

    let endpoint_id = runtime.register_worker_endpoint(WorkerEndpointState {
        sender: inbox_tx,
        receiver: inbox_rx,
        queue: Vec::new(),
        local_closed: false,
        remote_closed: false,
        cancelled: false,
        cancel_requested,
    });
    let inbox = worker_endpoint_object("WorkerInbox", endpoint_id, None);
    runtime.check_worker_cancelled()?;
    let mut call_args = Vec::with_capacity(arguments.len() + 1);
    call_args.push(inbox);
    call_args.extend(arguments);
    let result = runtime.call_value(callable, call_args, Vec::new())?;
    let result = runtime.await_if_task(result)?;
    runtime.check_worker_cancelled()?;
    let _ = runtime
        .worker_endpoints
        .borrow()
        .get(&endpoint_id)
        .map(|endpoint| endpoint.sender.send(WorkerFrame::Close));
    marshal::dump_value(&runtime, &result)
}

fn run_worker(
    module_roots: Vec<std::path::PathBuf>,
    policy: super::super::RuntimePolicy,
    request_bytes: Vec<u8>,
    cancel_requested: Arc<AtomicBool>,
) -> Result<Vec<u8>> {
    let runtime =
        Runtime::with_policy_and_worker_cancel(module_roots, policy, Some(cancel_requested));
    runtime.enter_async_context(|| run_worker_inner(&runtime, request_bytes))
}

fn run_worker_inner(runtime: &Runtime, request_bytes: Vec<u8>) -> Result<Vec<u8>> {
    runtime.check_worker_cancelled()?;
    let request = unwrap_shared(marshal::load_value(&runtime, &request_bytes)?);
    let Value::Array(mut request) = request else {
        return Err(ZuzuRustError::runtime(
            "Worker.spawn payload did not decode as Array",
        ));
    };
    if request.len() != 2 {
        return Err(ZuzuRustError::runtime(
            "Worker.spawn payload has invalid arity",
        ));
    }
    let arguments = unwrap_shared(request.pop().unwrap_or(Value::Array(Vec::new())));
    let callable = request.pop().unwrap_or(Value::Null);
    let Value::Array(arguments) = arguments else {
        return Err(ZuzuRustError::runtime(
            "Worker.spawn payload arguments did not decode as Array",
        ));
    };
    runtime.check_worker_cancelled()?;
    let result = runtime.call_value(callable, arguments, Vec::new())?;
    let result = runtime.await_if_task(result)?;
    runtime.check_worker_cancelled()?;
    marshal::dump_value(&runtime, &result)
}

fn unwrap_shared(value: Value) -> Value {
    match value {
        Value::Shared(value) => value.borrow().clone(),
        other => other,
    }
}

fn error_text(error: ZuzuRustError) -> String {
    match error {
        ZuzuRustError::Thrown { value, .. } => value,
        other => other.to_string(),
    }
}

fn worker_endpoint_object(
    class_name: &str,
    endpoint_id: usize,
    result_task: Option<Value>,
) -> Value {
    let mut fields = HashMap::from([(
        "__worker_endpoint_id".to_owned(),
        Value::Number(endpoint_id as f64),
    )]);
    if let Some(task) = result_task {
        fields.insert("__worker_result_task".to_owned(), task);
    }
    Value::Object(Rc::new(RefCell::new(ObjectValue {
        class: worker_object_class(class_name),
        fields,
        weak_fields: std::collections::HashSet::new(),
        builtin_value: Some(Value::Null),
    })))
}

fn worker_object_class(name: &str) -> Rc<UserClassValue> {
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

fn endpoint_id(object: &Rc<RefCell<ObjectValue>>) -> Result<usize> {
    match object.borrow().fields.get("__worker_endpoint_id") {
        Some(Value::Number(value)) if value.is_finite() && *value >= 1.0 => Ok(*value as usize),
        _ => Err(ZuzuRustError::runtime(
            "Worker endpoint object has invalid endpoint id",
        )),
    }
}

fn result_task(object: &Rc<RefCell<ObjectValue>>) -> Result<Rc<RefCell<super::super::TaskState>>> {
    match object.borrow().fields.get("__worker_result_task") {
        Some(Value::Task(task)) => Ok(Rc::clone(task)),
        _ => Err(ZuzuRustError::runtime(
            "WorkerHandle object has invalid result task",
        )),
    }
}

fn worker_endpoint_send(
    runtime: &Runtime,
    object: &Rc<RefCell<ObjectValue>>,
    args: &[Value],
    label: &str,
) -> Result<Value> {
    require_arity(&format!("{label}.send"), args, 1)?;
    let endpoint_id = endpoint_id(object)?;
    if label == "WorkerHandle" {
        if let Ok(task) = result_task(object) {
            let _ = runtime.poll_task(&task)?;
            if task.borrow().outcome.is_some() {
                return Ok(
                    runtime.task_rejected("ChannelClosedException: send on closed worker handle")
                );
            }
        }
    }
    let bytes = match marshal::dump_value(runtime, &args[0]) {
        Ok(bytes) => bytes,
        Err(error) => return Ok(runtime.task_rejected(error_text(error))),
    };
    let mut endpoints = runtime.worker_endpoints.borrow_mut();
    let Some(endpoint) = endpoints.get_mut(&endpoint_id) else {
        return Ok(runtime.task_rejected(format!("ChannelClosedException: {label} is closed")));
    };
    if endpoint.local_closed || endpoint.remote_closed || endpoint.cancelled {
        return Ok(runtime.task_rejected(format!("ChannelClosedException: {label} is closed")));
    }
    if endpoint.sender.send(WorkerFrame::Message(bytes)).is_err() {
        endpoint.local_closed = true;
        return Ok(runtime.task_rejected(format!("ChannelClosedException: {label} is closed")));
    }
    Ok(runtime.task_resolved(args[0].clone()))
}

fn worker_endpoint_recv(
    runtime: &Runtime,
    object: &Rc<RefCell<ObjectValue>>,
    args: &[Value],
    label: &str,
) -> Result<Value> {
    require_arity(&format!("{label}.recv"), args, 0)?;
    let endpoint_id = endpoint_id(object)?;
    let task_runtime = runtime.clone();
    let label = label.to_owned();
    Ok(runtime.task_native_async(
        async move {
            loop {
                match worker_endpoint_recv_once(&task_runtime, endpoint_id, &label)? {
                    RecvOutcome::Value(value) => return Ok(value),
                    RecvOutcome::Pending => {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }
            }
        },
        None,
    ))
}

enum RecvOutcome {
    Pending,
    Value(Value),
}

fn worker_endpoint_recv_once(
    runtime: &Runtime,
    endpoint_id: usize,
    label: &str,
) -> Result<RecvOutcome> {
    let bytes = {
        let mut endpoints = runtime.worker_endpoints.borrow_mut();
        let Some(endpoint) = endpoints.get_mut(&endpoint_id) else {
            return Err(ZuzuRustError::thrown(format!(
                "ChannelClosedException: {label} is closed"
            )));
        };
        if let Some(value) = endpoint.queue.pop() {
            return Ok(RecvOutcome::Value(value));
        }
        if endpoint.cancelled {
            return Err(ZuzuRustError::thrown(
                "CancelledException: worker cancelled",
            ));
        }
        if endpoint.remote_closed {
            return Err(ZuzuRustError::thrown(format!(
                "ChannelClosedException: {label} is closed"
            )));
        }
        match endpoint.receiver.try_recv() {
            Ok(WorkerFrame::Message(bytes)) => bytes,
            Ok(WorkerFrame::Close) => {
                endpoint.remote_closed = true;
                return Err(ZuzuRustError::thrown(format!(
                    "ChannelClosedException: {label} is closed"
                )));
            }
            Ok(WorkerFrame::Cancel(reason)) => {
                endpoint.cancelled = true;
                endpoint.cancel_requested.store(true, Ordering::SeqCst);
                return Err(ZuzuRustError::thrown(format!(
                    "CancelledException: {reason}"
                )));
            }
            Err(TryRecvError::Empty) => return Ok(RecvOutcome::Pending),
            Err(TryRecvError::Disconnected) => {
                endpoint.remote_closed = true;
                return Err(ZuzuRustError::thrown(format!(
                    "ChannelClosedException: {label} is closed"
                )));
            }
        }
    };
    let value = marshal::load_value(runtime, &bytes)?;
    Ok(RecvOutcome::Value(value))
}

fn worker_endpoint_close(
    runtime: &Runtime,
    object: &Rc<RefCell<ObjectValue>>,
    args: &[Value],
    label: &str,
) -> Result<Value> {
    require_arity(&format!("{label}.close"), args, 0)?;
    let endpoint_id = endpoint_id(object)?;
    if let Some(endpoint) = runtime.worker_endpoints.borrow_mut().get_mut(&endpoint_id) {
        if !endpoint.local_closed {
            endpoint.local_closed = true;
            let _ = endpoint.sender.send(WorkerFrame::Close);
        }
    }
    Ok(runtime.task_resolved(Value::Null))
}

fn worker_handle_cancel(
    runtime: &Runtime,
    object: &Rc<RefCell<ObjectValue>>,
    args: &[Value],
) -> Result<Value> {
    if args.len() > 1 {
        return Err(ZuzuRustError::runtime(
            "WorkerHandle.cancel expects an optional reason",
        ));
    }
    let reason = args
        .first()
        .map(Value::render)
        .unwrap_or_else(|| "worker cancelled".to_owned());
    let endpoint_id = endpoint_id(object)?;
    if let Some(endpoint) = runtime.worker_endpoints.borrow_mut().get_mut(&endpoint_id) {
        endpoint.cancelled = true;
        endpoint.local_closed = true;
        endpoint.remote_closed = true;
        endpoint.cancel_requested.store(true, Ordering::SeqCst);
        let _ = endpoint.sender.send(WorkerFrame::Cancel(reason.clone()));
    }
    let task = result_task(object)?;
    runtime.cancel_task(&task, Value::String(reason));
    Ok(Value::Object(Rc::clone(object)))
}

fn worker_handle_result(object: &Rc<RefCell<ObjectValue>>, args: &[Value]) -> Result<Value> {
    require_arity("WorkerHandle.result", args, 0)?;
    Ok(Value::Task(result_task(object)?))
}

fn worker_handle_status(
    runtime: &Runtime,
    object: &Rc<RefCell<ObjectValue>>,
    args: &[Value],
) -> Result<Value> {
    require_arity("WorkerHandle.status", args, 0)?;
    let task = result_task(object)?;
    let _ = runtime.poll_task(&task)?;
    let status = task.borrow().status.clone();
    Ok(Value::String(status))
}

fn worker_handle_done(
    runtime: &Runtime,
    object: &Rc<RefCell<ObjectValue>>,
    args: &[Value],
) -> Result<Value> {
    require_arity("WorkerHandle.done", args, 0)?;
    let task = result_task(object)?;
    let _ = runtime.poll_task(&task)?;
    let done = task.borrow().outcome.is_some();
    Ok(Value::Boolean(done))
}

fn require_arity(name: &str, args: &[Value], expected: usize) -> Result<()> {
    if args.len() != expected {
        return Err(ZuzuRustError::runtime(format!(
            "{name} expects {expected} argument(s), got {}",
            args.len()
        )));
    }
    Ok(())
}
