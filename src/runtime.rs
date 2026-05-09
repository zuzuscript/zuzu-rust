use regex::{Regex, RegexBuilder};
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::ffi::c_void;
use std::fmt;
use std::fs;
use std::future::Future;
use std::io::{self, Write};
use std::net::{TcpListener, TcpStream, UdpSocket};
use std::ops::Deref;
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::rc::{Rc, Weak};
use std::sync::mpsc;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

mod collection;
mod executor;
mod stdlib;

use self::collection::common::{
    collection_contains, collection_difference, collection_intersection, collection_subset,
    collection_union, construct_pair, construct_pairlist, expect_pair_like,
    is_mutating_collection_method, pairlist_eq, reject_named_args, require_arity,
};
use self::executor::AsyncExecutor;
use crate::ast::{
    BlockStatement, CallArgument, ClassDeclaration, ClassMember, DictKey, Expression,
    FieldDeclaration, ForStatement, FunctionDeclaration, ImportDeclaration, MethodDeclaration,
    Parameter, Program, Statement, SwitchStatement, TemplatePart, TraitDeclaration, TryStatement,
};
use crate::sema;
use crate::{
    parse_program_with_compile_options, parse_program_with_compile_options_and_source_file,
    OptimizationOptions, OptimizationPass, ParseOptions, Result, ZuzuRustError,
};

#[derive(Clone, Default)]
pub struct ExecutionOutput {
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum HostValue {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Binary(Vec<u8>),
    Array(Vec<HostValue>),
    Dict(HashMap<String, HostValue>),
    PairList(Vec<(String, HostValue)>),
    Path(PathBuf),
}

#[derive(Clone)]
pub struct Runtime {
    inner: Rc<RuntimeInner>,
}

pub struct RuntimeInner {
    module_roots: Vec<PathBuf>,
    policy: RuntimePolicy,
    run_sema: bool,
    infer_types: bool,
    optimizations: OptimizationOptions,
    module_cache: RefCell<HashMap<String, ModuleRecord>>,
    regex_cache: RefCell<HashMap<(String, String), Regex>>,
    module_loading: RefCell<HashSet<String>>,
    output: RefCell<ExecutionOutput>,
    special_props: RefCell<Vec<HashMap<String, Value>>>,
    thrown_values: RefCell<HashMap<String, Value>>,
    next_thrown_id: RefCell<usize>,
    path_line_cursors: RefCell<HashMap<String, usize>>,
    socket_state: RefCell<SocketState>,
    current_env_stack: RefCell<Vec<Rc<Environment>>>,
    current_method_stack: RefCell<Vec<Rc<MethodValue>>>,
    signal_handlers: RefCell<HashMap<String, Vec<Value>>>,
    db_state: RefCell<self::stdlib::DbState>,
    clib_state: RefCell<self::stdlib::ClibState>,
    running_async_functions: RefCell<Vec<usize>>,
    polling_tasks: RefCell<Vec<usize>>,
    background_tasks: RefCell<Vec<Rc<RefCell<TaskState>>>>,
    worker_endpoints: RefCell<HashMap<usize, WorkerEndpointState>>,
    next_worker_endpoint_id: RefCell<usize>,
    worker_cancel_requested: Option<Arc<AtomicBool>>,
    async_executor: AsyncExecutor,
}

impl Deref for Runtime {
    type Target = RuntimeInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct ReplEvalResult {
    pub output: ExecutionOutput,
    pub value: String,
}

pub struct ReplSession<'runtime> {
    runtime: &'runtime Runtime,
    env: Rc<Environment>,
}

pub struct LoadedScript {
    _runtime: Runtime,
    env: Rc<Environment>,
}

impl LoadedScript {
    pub fn has_function(&self, name: &str) -> bool {
        matches!(
            self.env.get_optional(name),
            Some(Value::Function(_)) | Some(Value::NativeFunction(_))
        )
    }

    pub fn call(&self, name: &str, args: Vec<HostValue>) -> Result<HostValue> {
        self._runtime
            .async_executor
            .enter(|| self.call_inner(name, args))
    }

    pub fn call_request(&self, env: HostValue) -> Result<HostValue> {
        self.call("__request__", vec![env])
    }

    fn call_inner(&self, name: &str, args: Vec<HostValue>) -> Result<HostValue> {
        let callee = self.env.get(name)?;
        let args = args
            .into_iter()
            .map(|value| self._runtime.host_value_to_value(value))
            .collect::<Result<Vec<_>>>()?;
        let result = self._runtime.call_value(callee, args, Vec::new())?;
        let result = self._runtime.await_if_task(result)?;
        self._runtime.value_to_host_value(result)
    }
}

#[derive(Clone, Default)]
pub struct RuntimePolicy {
    denied_capabilities: HashSet<String>,
    denied_modules: HashSet<String>,
    debug_level: u32,
}

impl RuntimePolicy {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn deny_capability(mut self, capability: impl Into<String>) -> Self {
        let capability = capability.into();
        if !capability.is_empty() {
            self.denied_capabilities.insert(capability);
        }
        self
    }

    pub fn deny_module(mut self, module: impl Into<String>) -> Self {
        let module = module.into();
        if !module.is_empty() {
            self.denied_modules.insert(module);
        }
        self
    }

    pub fn debug_level(mut self, level: u32) -> Self {
        self.debug_level = level;
        self
    }
}

pub(in crate::runtime) const DENIAL_CAPABILITIES: &[&str] = &[
    "fs", "net", "perl", "js", "proc", "db", "clib", "gui", "worker",
];

fn quote_zuzu_string(text: &str) -> String {
    let mut out = String::from("\"");
    for ch in text.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            other => out.push(other),
        }
    }
    out.push('"');
    out
}

#[derive(Clone)]
struct ModuleRecord {
    exports: HashMap<String, Value>,
}

#[derive(Default)]
struct SocketState {
    next_socket_id: usize,
    tcp_servers: HashMap<String, TcpListener>,
    tcp_sockets: HashMap<String, TcpSocketState>,
    udp_sockets: HashMap<String, UdpSocket>,
    #[cfg(unix)]
    unix_servers: HashMap<String, UnixServerState>,
    #[cfg(unix)]
    unix_sockets: HashMap<String, UnixSocketState>,
}

struct TcpSocketState {
    stream: TcpStream,
    read_buffer: Vec<u8>,
}

#[cfg(unix)]
struct UnixServerState {
    listener: UnixListener,
    path: PathBuf,
}

#[cfg(unix)]
struct UnixSocketState {
    stream: UnixStream,
    read_buffer: Vec<u8>,
}

#[cfg(unix)]
impl Drop for UnixServerState {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[derive(Clone)]
enum Value {
    Null,
    Boolean(bool),
    Number(f64),
    String(String),
    BinaryString(Vec<u8>),
    Regex(String, String),
    Array(Vec<Value>),
    Set(Vec<Value>),
    Bag(Vec<Value>),
    Dict(HashMap<String, Value>),
    PairList(Vec<(String, Value)>),
    Pair(String, Box<Value>),
    Function(Rc<FunctionValue>),
    NativeFunction(Rc<String>),
    Method(Rc<MethodValue>),
    Iterator(Rc<RefCell<IteratorState>>),
    Class(Rc<String>),
    UserClass(Rc<UserClassValue>),
    Trait(Rc<TraitValue>),
    Object(Rc<RefCell<ObjectValue>>),
    Task(Rc<RefCell<TaskState>>),
    Channel(Rc<RefCell<ChannelState>>),
    CancellationSource(Rc<RefCell<CancellationState>>),
    CancellationToken(Rc<RefCell<CancellationState>>),
    Shared(Rc<RefCell<Value>>),
    Ref(Rc<LvalueRef>),
    AliasRef(Rc<LvalueRef>),
    WeakFunction(Weak<FunctionValue>),
    WeakNativeFunction(Weak<String>),
    WeakMethod(Weak<MethodValue>),
    WeakIterator(Weak<RefCell<IteratorState>>),
    WeakClass(Weak<String>),
    WeakUserClass(Weak<UserClassValue>),
    WeakTrait(Weak<TraitValue>),
    WeakObject(Weak<RefCell<ObjectValue>>),
    WeakTask(Weak<RefCell<TaskState>>),
    WeakChannel(Weak<RefCell<ChannelState>>),
    WeakCancellationSource(Weak<RefCell<CancellationState>>),
    WeakCancellationToken(Weak<RefCell<CancellationState>>),
    WeakShared(Weak<RefCell<Value>>),
    WeakRef(Weak<LvalueRef>),
    WeakAliasRef(Weak<LvalueRef>),
    WeakStoredScalar(Box<Value>),
}

struct IteratorState {
    items: Vec<Value>,
    index: usize,
}

#[derive(Clone)]
struct FunctionValue {
    name: Option<String>,
    params: Vec<Parameter>,
    return_type: Option<String>,
    body: FunctionBody,
    env: Rc<Environment>,
    is_async: bool,
    current_method: Option<Rc<MethodValue>>,
}

#[derive(Clone)]
enum FunctionBody {
    Block(BlockStatement),
    Expression(Expression),
}

struct UserClassValue {
    name: String,
    base: Option<ClassBase>,
    traits: Vec<Rc<TraitValue>>,
    fields: Vec<FieldSpec>,
    methods: HashMap<String, Rc<MethodValue>>,
    static_methods: HashMap<String, Rc<MethodValue>>,
    nested_classes: HashMap<String, Rc<UserClassValue>>,
    source_decl: Option<ClassDeclaration>,
    closure_env: Option<Rc<Environment>>,
}

#[derive(Clone)]
struct FieldSpec {
    name: String,
    declared_type: Option<String>,
    mutable: bool,
    accessors: Vec<String>,
    default_value: Option<Expression>,
    is_weak_storage: bool,
}

struct MethodValue {
    name: String,
    params: Vec<Parameter>,
    return_type: Option<String>,
    body: BlockStatement,
    env: Rc<Environment>,
    #[allow(dead_code)]
    is_static: bool,
    is_async: bool,
    bound_receiver: Option<Value>,
    bound_name: Option<String>,
}

pub(in crate::runtime) enum TaskKind {
    Resolved,
    Sleep {
        deadline: Instant,
    },
    Function {
        function: Rc<FunctionValue>,
        args: Vec<Value>,
        named_args: Vec<(String, Value)>,
        started: bool,
    },
    FunctionWaiting {
        awaited: Rc<RefCell<TaskState>>,
        frames: Vec<AsyncFrame>,
        disposition: AwaitDisposition,
    },
    Spawn {
        body: BlockStatement,
        env: Rc<Environment>,
        started: bool,
    },
    SpawnWaiting {
        awaited: Rc<RefCell<TaskState>>,
        frames: Vec<AsyncFrame>,
        disposition: AwaitDisposition,
    },
    All {
        tasks: Vec<Value>,
    },
    Race {
        tasks: Vec<Value>,
    },
    Timeout {
        deadline: Instant,
        seconds: f64,
        task: Value,
    },
    ChannelRecv {
        channel: Rc<RefCell<ChannelState>>,
    },
    NativeAsync {
        cancel_requested: Option<Rc<Cell<bool>>>,
    },
}

#[derive(Clone)]
enum AsyncFrame {
    Function {
        statements: Vec<Statement>,
        index: usize,
        env: Rc<Environment>,
        return_type: Option<String>,
        name: Option<String>,
    },
    Block {
        statements: Vec<Statement>,
        index: usize,
        env: Rc<Environment>,
        cleanup_env: bool,
    },
    Do {
        statements: Vec<Statement>,
        index: usize,
        env: Rc<Environment>,
        cleanup_env: bool,
        last: Value,
    },
}

#[derive(Clone)]
enum AwaitDisposition {
    Discard,
    StoreLast,
    Return,
}

enum AsyncPoll {
    Complete(Value),
    Await {
        awaited: Rc<RefCell<TaskState>>,
        frames: Vec<AsyncFrame>,
        disposition: AwaitDisposition,
    },
}

#[derive(Clone)]
pub(in crate::runtime) enum TaskOutcome {
    Fulfilled(Value),
    Rejected(String),
    Cancelled(String),
}

pub(in crate::runtime) struct TaskState {
    pub(in crate::runtime) status: String,
    pub(in crate::runtime) kind: TaskKind,
    pub(in crate::runtime) outcome: Option<TaskOutcome>,
}

pub(in crate::runtime) struct ChannelState {
    pub(in crate::runtime) messages: Vec<Value>,
    pub(in crate::runtime) closed: bool,
}

pub(in crate::runtime) struct CancellationState {
    pub(in crate::runtime) cancelled: bool,
    pub(in crate::runtime) reason: Value,
    pub(in crate::runtime) watched: Vec<Rc<RefCell<TaskState>>>,
}

pub(in crate::runtime) enum WorkerFrame {
    Message(Vec<u8>),
    Close,
    Cancel(String),
}

pub(in crate::runtime) struct WorkerEndpointState {
    pub(in crate::runtime) sender: mpsc::Sender<WorkerFrame>,
    pub(in crate::runtime) receiver: mpsc::Receiver<WorkerFrame>,
    pub(in crate::runtime) queue: Vec<Value>,
    pub(in crate::runtime) local_closed: bool,
    pub(in crate::runtime) remote_closed: bool,
    pub(in crate::runtime) cancelled: bool,
    pub(in crate::runtime) cancel_requested: Arc<AtomicBool>,
}

struct TraitValue {
    name: String,
    methods: HashMap<String, Rc<MethodValue>>,
    source_decl: Option<TraitDeclaration>,
    closure_env: Option<Rc<Environment>>,
}

struct ObjectValue {
    class: Rc<UserClassValue>,
    fields: HashMap<String, Value>,
    weak_fields: HashSet<String>,
    builtin_value: Option<Value>,
}

#[derive(Clone)]
enum ClassBase {
    User(Rc<UserClassValue>),
    Builtin(String),
}

enum LvalueRef {
    Expression {
        env: Rc<Environment>,
        target: Expression,
    },
    ObjectField {
        object: Weak<RefCell<ObjectValue>>,
        name: String,
    },
}

struct Environment {
    parent: Option<Rc<Environment>>,
    bindings: RefCell<HashMap<String, Binding>>,
}

#[derive(Clone)]
struct Binding {
    value: Value,
    mutable: bool,
    is_weak_storage: bool,
}

#[allow(dead_code)]
#[derive(Clone)]
struct SimpleRegex {
    parts: Vec<RegexPart>,
    case_insensitive: bool,
}

#[allow(dead_code)]
#[derive(Clone)]
enum RegexPart {
    Literal(String),
    DigitClassPlus,
    LowerAlphaClassPlus,
    Group(Vec<RegexPart>),
}

#[allow(dead_code)]
struct RegexMatch {
    start: usize,
    end: usize,
    groups: Vec<String>,
}

enum ControlFlow {
    Normal,
    Return(Value),
    #[allow(dead_code)]
    Throw(Value),
    Continue,
    Break,
}

#[derive(Clone, Copy)]
enum CollectionKind {
    Array,
    Set,
    Bag,
}

impl Runtime {
    pub fn new(module_roots: Vec<PathBuf>) -> Self {
        Self::with_policy(module_roots, RuntimePolicy::default())
    }

    pub fn with_policy(module_roots: Vec<PathBuf>, policy: RuntimePolicy) -> Self {
        Self::with_policy_and_worker_cancel(module_roots, policy, None)
    }

    pub(in crate::runtime) fn with_policy_and_worker_cancel(
        module_roots: Vec<PathBuf>,
        policy: RuntimePolicy,
        worker_cancel_requested: Option<Arc<AtomicBool>>,
    ) -> Self {
        Self {
            inner: Rc::new(RuntimeInner {
                module_roots,
                policy,
                run_sema: true,
                infer_types: true,
                optimizations: OptimizationOptions::default(),
                module_cache: RefCell::new(HashMap::new()),
                regex_cache: RefCell::new(HashMap::new()),
                module_loading: RefCell::new(HashSet::new()),
                output: RefCell::new(ExecutionOutput::default()),
                special_props: RefCell::new(vec![HashMap::new()]),
                thrown_values: RefCell::new(HashMap::new()),
                next_thrown_id: RefCell::new(0),
                path_line_cursors: RefCell::new(HashMap::new()),
                socket_state: RefCell::new(SocketState::default()),
                current_env_stack: RefCell::new(Vec::new()),
                current_method_stack: RefCell::new(Vec::new()),
                signal_handlers: RefCell::new(HashMap::new()),
                db_state: RefCell::new(self::stdlib::DbState::default()),
                clib_state: RefCell::new(self::stdlib::ClibState::default()),
                running_async_functions: RefCell::new(Vec::new()),
                polling_tasks: RefCell::new(Vec::new()),
                background_tasks: RefCell::new(Vec::new()),
                worker_endpoints: RefCell::new(HashMap::new()),
                next_worker_endpoint_id: RefCell::new(1),
                worker_cancel_requested,
                async_executor: AsyncExecutor::new(),
            }),
        }
    }

    pub fn from_repo_root(repo_root: &Path) -> Self {
        let mut module_roots = Vec::new();
        if let Some(home) = std::env::var_os("HOME") {
            module_roots.push(PathBuf::from(home).join(".zuzu").join("modules"));
        }
        module_roots.push(PathBuf::from("/var/lib/zuzu/modules"));
        module_roots.push(repo_root.join("modules"));
        Self::new(module_roots)
    }

    pub fn from_repo_root_with_policy(repo_root: &Path, policy: RuntimePolicy) -> Self {
        let mut module_roots = Vec::new();
        if let Some(home) = std::env::var_os("HOME") {
            module_roots.push(PathBuf::from(home).join(".zuzu").join("modules"));
        }
        module_roots.push(PathBuf::from("/var/lib/zuzu/modules"));
        module_roots.push(repo_root.join("modules"));
        Self::with_policy(module_roots, policy)
    }

    pub fn with_parse_options(mut self, run_sema: bool, infer_types: bool) -> Self {
        let inner = Rc::get_mut(&mut self.inner)
            .expect("parse options must be set before runtime is cloned");
        inner.run_sema = run_sema;
        inner.infer_types = infer_types;
        self
    }

    pub fn with_optimization_options(mut self, optimizations: OptimizationOptions) -> Self {
        let inner = Rc::get_mut(&mut self.inner)
            .expect("optimization options must be set before runtime is cloned");
        inner.optimizations = optimizations;
        self
    }

    pub fn repl_session(&self) -> ReplSession<'_> {
        let env = Rc::new(Environment::new(None));
        self.install_builtins(&env);
        ReplSession { runtime: self, env }
    }

    #[allow(dead_code)]
    fn composite_value_ref_target(&self, env: Rc<Environment>, field_name: &str) -> Value {
        Value::Ref(Rc::new(LvalueRef::Expression {
            env,
            target: Expression::DictAccess {
                line: 0,
                source_file: None,
                object: Box::new(Expression::Identifier {
                    line: 0,
                    source_file: None,
                    name: "self".to_owned(),
                    inferred_type: None,
                    binding_depth: None,
                }),
                key: Box::new(DictKey::Identifier {
                    line: 0,
                    source_file: None,
                    name: field_name.to_owned(),
                }),
                inferred_type: None,
            },
        }))
    }

    fn is_ref_backed_composite(&self, value: &Value) -> bool {
        matches!(
            value,
            Value::Array(_)
                | Value::Set(_)
                | Value::Bag(_)
                | Value::Dict(_)
                | Value::PairList(_)
                | Value::Pair(_, _)
                | Value::Shared(_)
        )
    }

    fn deref_value(&self, value: &Value) -> Result<Value> {
        let mut current = value.clone();
        for _ in 0..32 {
            match current {
                value if value.is_weak_value() => {
                    current = value.resolve_weak_value();
                }
                Value::Ref(reference) | Value::AliasRef(reference) => {
                    current = self.call_ref(Rc::clone(&reference), Vec::new(), Vec::new())?;
                }
                Value::Shared(value) => {
                    current = value.borrow().clone();
                }
                _ => return Ok(current),
            }
        }
        Err(ZuzuRustError::runtime(
            "reference dereference recursion limit reached",
        ))
    }

    pub(in crate::runtime) fn normalize_value(&self, value: Value) -> Result<Value> {
        match value {
            value if value.is_weak_value() => self.deref_value(&value),
            Value::AliasRef(_) | Value::Shared(_) => self.deref_value(&value),
            other => Ok(other),
        }
    }

    fn with_current_env<T>(
        &self,
        env: Rc<Environment>,
        f: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        self.current_env_stack.borrow_mut().push(env);
        let result = f();
        self.current_env_stack.borrow_mut().pop();
        result
    }

    fn current_env(&self) -> Option<Rc<Environment>> {
        self.current_env_stack.borrow().last().cloned()
    }

    fn parse_options(&self) -> ParseOptions {
        ParseOptions::new(self.run_sema, self.infer_types, self.optimizations.clone())
    }

    fn emit_semantic_warnings(&self, program: &Program) -> Result<()> {
        if !self.run_sema {
            return Ok(());
        }
        for warning in sema::weak_storage_warnings(program) {
            self.emit_stderr(&format!("{warning}\n"))?;
        }
        Ok(())
    }

    fn assign_reference(&self, reference: Rc<LvalueRef>, value: Value) -> Result<Value> {
        self.call_ref(reference, vec![value], Vec::new())
    }

    fn assign_reference_with_weak_write(
        &self,
        reference: Rc<LvalueRef>,
        value: Value,
        weak_write: bool,
    ) -> Result<Value> {
        self.call_ref(
            reference,
            vec![value, Value::Boolean(weak_write)],
            Vec::new(),
        )
    }

    fn maybe_return_trivial_field_getter(
        &self,
        object: &Rc<RefCell<ObjectValue>>,
        method: &Rc<MethodValue>,
    ) -> Option<Result<Value>> {
        if !method.params.is_empty() {
            return None;
        }
        let [statement] = method.body.statements.as_slice() else {
            return None;
        };
        let field_name = match statement {
            Statement::ReturnStatement(node) => match node.argument.as_ref() {
                Some(Expression::Identifier { name, .. }) => Some(name.as_str()),
                _ => None,
            },
            Statement::ExpressionStatement(node) => match &node.expression {
                Expression::Identifier { name, .. } => Some(name.as_str()),
                _ => None,
            },
            _ => None,
        }?;
        let value = object
            .borrow()
            .fields
            .get(field_name)
            .cloned()
            .unwrap_or(Value::Null);
        Some(Ok(if value.is_weak_value() {
            value.resolve_weak_value()
        } else {
            value
        }))
    }

    pub fn run_script_file(&self, script_path: &Path) -> Result<ExecutionOutput> {
        self.run_script_file_with_args(script_path, &[])
    }

    pub fn run_script_file_with_args(
        &self,
        script_path: &Path,
        argv: &[String],
    ) -> Result<ExecutionOutput> {
        let source = fs::read_to_string(script_path)?;
        self.run_script_source_with_args_and_source_file(
            &source,
            argv,
            Some(&script_path.display().to_string()),
        )
    }

    pub fn last_output(&self) -> ExecutionOutput {
        self.output.borrow().clone()
    }

    pub(crate) fn emit_stdout(&self, text: &str) -> Result<()> {
        self.output.borrow_mut().stdout.push_str(text);
        let mut stdout = io::stdout().lock();
        stdout.write_all(text.as_bytes())?;
        stdout.flush()?;
        Ok(())
    }

    pub(crate) fn emit_stderr(&self, text: &str) -> Result<()> {
        self.output.borrow_mut().stderr.push_str(text);
        let mut stderr = io::stderr().lock();
        stderr.write_all(text.as_bytes())?;
        stderr.flush()?;
        Ok(())
    }

    pub(crate) fn warn_blocking_operation(&self, operation: &str) -> Result<()> {
        if self.policy.debug_level == 0 || self.polling_tasks.borrow().is_empty() {
            return Ok(());
        }
        self.emit_stderr(&format!(
            "BlockingOperation: {operation} called while polling async task\n"
        ))
    }

    pub fn run_script_source(&self, source: &str) -> Result<ExecutionOutput> {
        self.run_script_source_with_args(source, &[])
    }

    pub fn run_script_source_with_args(
        &self,
        source: &str,
        argv: &[String],
    ) -> Result<ExecutionOutput> {
        self.run_script_source_with_args_and_source_file(source, argv, None)
    }

    pub fn run_script_source_with_args_and_source_file(
        &self,
        source: &str,
        argv: &[String],
        source_file: Option<&str>,
    ) -> Result<ExecutionOutput> {
        self.async_executor.enter(|| {
            self.run_script_source_with_args_and_source_file_inner(source, argv, source_file)
        })
    }

    pub fn load_program_without_main(
        &self,
        program: &Program,
        _source_file: Option<&str>,
    ) -> Result<LoadedScript> {
        self.async_executor
            .enter(|| self.load_program_without_main_inner(program))
    }

    /// Builds a GTK widget preview for Zuzu GUI XML.
    ///
    /// The XML is parsed through `std/gui.gui_from_xml`, so pure module
    /// behaviour is still exercised normally. The returned pointer is a newly
    /// created `GtkWidget`, usually carrying a floating reference like normal
    /// GTK widget constructors.
    pub fn gui_xml_preview_widget(&self, xml: &str) -> Result<*mut c_void> {
        self.async_executor
            .enter(|| self.gui_xml_preview_widget_inner(xml))
    }

    fn gui_xml_preview_widget_inner(&self, xml: &str) -> Result<*mut c_void> {
        let source = format!(
            "from std/gui import gui_from_xml;\n\
            gui_from_xml({});\n",
            quote_zuzu_string(xml),
        );
        let options = self.parse_options();
        let program = parse_program_with_compile_options(&source, &options)?;
        self.emit_semantic_warnings(&program)?;
        let env = Rc::new(Environment::new(None));
        self.install_builtins(&env);
        let value = self.eval_repl_program(&program, env)?;
        stdlib::gui::preview_widget(self, &value)
    }

    fn run_script_source_with_args_and_source_file_inner(
        &self,
        source: &str,
        argv: &[String],
        source_file: Option<&str>,
    ) -> Result<ExecutionOutput> {
        self.reset_top_level_execution_state();
        let options = self.parse_options();
        let program =
            parse_program_with_compile_options_and_source_file(source, &options, source_file)?;
        self.emit_semantic_warnings(&program)?;
        let env = Rc::new(Environment::new(None));
        self.install_builtins(&env);
        match self.eval_program(&program, Rc::clone(&env)) {
            Err(ZuzuRustError::Thrown { value, .. }) => Err(ZuzuRustError::runtime(format!(
                "uncaught exception: {value}"
            ))),
            Err(err) => Err(err),
            Ok(flow) => match flow {
                ControlFlow::Normal => {
                    self.call_main_if_present(Rc::clone(&env), argv)?;
                    self.cancel_background_tasks(Value::String("shutdown".to_owned()));
                    Ok(self.output.borrow().clone())
                }
                ControlFlow::Return(_) => Err(ZuzuRustError::runtime(
                    "return is not valid at top-level scope",
                )),
                ControlFlow::Throw(value) => Err(ZuzuRustError::runtime(format!(
                    "uncaught exception: {}",
                    value
                ))),
                ControlFlow::Continue | ControlFlow::Break => Err(ZuzuRustError::runtime(
                    "loop control is not valid at top-level scope",
                )),
            },
        }
    }

    fn load_program_without_main_inner(&self, program: &Program) -> Result<LoadedScript> {
        self.reset_top_level_execution_state();
        self.emit_semantic_warnings(program)?;
        let env = Rc::new(Environment::new(None));
        self.install_builtins(&env);
        match self.eval_program(program, Rc::clone(&env)) {
            Err(ZuzuRustError::Thrown { value, .. }) => Err(ZuzuRustError::runtime(format!(
                "uncaught exception: {value}"
            ))),
            Err(err) => Err(err),
            Ok(flow) => match flow {
                ControlFlow::Normal => Ok(LoadedScript {
                    _runtime: self.clone(),
                    env,
                }),
                ControlFlow::Return(_) => Err(ZuzuRustError::runtime(
                    "return is not valid at top-level scope",
                )),
                ControlFlow::Throw(value) => Err(ZuzuRustError::runtime(format!(
                    "uncaught exception: {}",
                    value
                ))),
                ControlFlow::Continue | ControlFlow::Break => Err(ZuzuRustError::runtime(
                    "loop control is not valid at top-level scope",
                )),
            },
        }
    }

    fn reset_top_level_execution_state(&self) {
        *self.output.borrow_mut() = ExecutionOutput::default();
        let mut special_props = self.special_props.borrow_mut();
        special_props.clear();
        special_props.push(HashMap::new());
        drop(special_props);
        self.thrown_values.borrow_mut().clear();
        *self.next_thrown_id.borrow_mut() = 0;
        self.path_line_cursors.borrow_mut().clear();
        self.background_tasks.borrow_mut().clear();
        *self.socket_state.borrow_mut() = SocketState::default();
    }

    fn host_value_to_value(&self, value: HostValue) -> Result<Value> {
        match value {
            HostValue::Null => Ok(Value::Null),
            HostValue::Bool(value) => Ok(Value::Boolean(value)),
            HostValue::Number(value) => Ok(Value::Number(value)),
            HostValue::String(value) => Ok(Value::String(value)),
            HostValue::Binary(value) => Ok(Value::BinaryString(value)),
            HostValue::Array(values) => values
                .into_iter()
                .map(|value| self.host_value_to_value(value))
                .collect::<Result<Vec<_>>>()
                .map(Value::Array),
            HostValue::Dict(values) => values
                .into_iter()
                .map(|(key, value)| Ok((key, self.host_value_to_value(value)?)))
                .collect::<Result<HashMap<_, _>>>()
                .map(Value::Dict),
            HostValue::PairList(values) => values
                .into_iter()
                .map(|(key, value)| Ok((key, self.host_value_to_value(value)?)))
                .collect::<Result<Vec<_>>>()
                .map(Value::PairList),
            HostValue::Path(path) => self.construct_builtin_class(
                "Path",
                vec![Value::String(path.to_string_lossy().to_string())],
                Vec::new(),
            ),
        }
    }

    fn value_to_host_value(&self, value: Value) -> Result<HostValue> {
        let value = self.deref_value(&value)?;
        match value {
            Value::Null => Ok(HostValue::Null),
            Value::Boolean(value) => Ok(HostValue::Bool(value)),
            Value::Number(value) => Ok(HostValue::Number(value)),
            Value::String(value) => Ok(HostValue::String(value)),
            Value::BinaryString(value) => Ok(HostValue::Binary(value)),
            Value::Array(values) => values
                .into_iter()
                .map(|value| self.value_to_host_value(value))
                .collect::<Result<Vec<_>>>()
                .map(HostValue::Array),
            Value::Dict(values) => values
                .into_iter()
                .map(|(key, value)| Ok((key, self.value_to_host_value(value)?)))
                .collect::<Result<HashMap<_, _>>>()
                .map(HostValue::Dict),
            Value::PairList(values) => values
                .into_iter()
                .map(|(key, value)| Ok((key, self.value_to_host_value(value)?)))
                .collect::<Result<Vec<_>>>()
                .map(HostValue::PairList),
            Value::Object(object) if object.borrow().class.name == "Path" => {
                let path = match object.borrow().fields.get("path") {
                    Some(Value::String(path)) => PathBuf::from(path),
                    _ => PathBuf::new(),
                };
                Ok(HostValue::Path(path))
            }
            other => Err(ZuzuRustError::runtime(format!(
                "cannot convert {} to HostValue",
                self.typeof_name(&other)
            ))),
        }
    }

    fn call_main_if_present(&self, env: Rc<Environment>, argv: &[String]) -> Result<()> {
        let Some(callee) = env.get_optional("__main__") else {
            return Ok(());
        };
        let should_await_main = matches!(&callee, Value::Function(function) if function.is_async);
        let args = Value::Array(argv.iter().map(|arg| Value::String(arg.clone())).collect());
        let result = self.call_value(callee, vec![args], Vec::new())?;
        if should_await_main {
            let _ = self.await_value(result)?;
        }
        Ok(())
    }

    fn eval_program(&self, program: &Program, env: Rc<Environment>) -> Result<ControlFlow> {
        self.eval_statements(&program.statements, env)
    }

    fn eval_repl_program(&self, program: &Program, env: Rc<Environment>) -> Result<Value> {
        let mut result = Value::Null;
        for statement in &program.statements {
            result = self.eval_repl_statement(statement, Rc::clone(&env))?;
        }
        Ok(result)
    }

    fn eval_repl_statement(&self, statement: &Statement, env: Rc<Environment>) -> Result<Value> {
        match statement {
            Statement::VariableDeclaration(node) => {
                let value = match node.init.as_ref() {
                    Some(init) => self.eval_expression(init, Rc::clone(&env))?,
                    None => Value::Null,
                };
                if node.init.is_some() && node.runtime_typecheck_required != Some(false) {
                    self.assert_declared_type(node.declared_type.as_deref(), &value, &node.name)?;
                }
                env.define_with_storage(
                    node.name.clone(),
                    value.clone(),
                    node.kind != "const",
                    node.is_weak_storage,
                );
                Ok(value)
            }
            Statement::FunctionDeclaration(node) => {
                let func = self.make_function_from_decl(node, Rc::clone(&env));
                let value = Value::Function(Rc::new(func));
                env.define(node.name.clone(), value.clone(), false);
                Ok(value)
            }
            Statement::ClassDeclaration(node) => {
                let class = self.make_user_class_from_decl(node, Rc::clone(&env))?;
                let value = Value::UserClass(Rc::new(class));
                env.define(node.name.clone(), value.clone(), false);
                Ok(value)
            }
            Statement::TraitDeclaration(node) => {
                let mut methods = HashMap::new();
                for member in &node.body {
                    if let ClassMember::Method(method) = member {
                        methods.insert(
                            method.name.clone(),
                            Rc::new(self.make_method_from_decl(method, Rc::clone(&env))),
                        );
                    }
                }
                let value = Value::Trait(Rc::new(TraitValue {
                    name: node.name.clone(),
                    methods,
                    source_decl: Some(node.clone()),
                    closure_env: Some(Rc::clone(&env)),
                }));
                env.define(node.name.clone(), value.clone(), false);
                Ok(value)
            }
            Statement::ExpressionStatement(node) => self.eval_expression(&node.expression, env),
            _ => match self.eval_statement(statement, env)? {
                ControlFlow::Normal => Ok(Value::Null),
                ControlFlow::Return(_) => Err(ZuzuRustError::runtime(
                    "return is not valid at top-level scope",
                )),
                ControlFlow::Throw(value) => Err(ZuzuRustError::runtime(format!(
                    "uncaught exception: {}",
                    value
                ))),
                ControlFlow::Continue | ControlFlow::Break => Err(ZuzuRustError::runtime(
                    "loop control is not valid at top-level scope",
                )),
            },
        }
    }

    fn eval_statements(
        &self,
        statements: &[Statement],
        env: Rc<Environment>,
    ) -> Result<ControlFlow> {
        for statement in statements {
            let flow = self.eval_statement(statement, Rc::clone(&env))?;
            if !matches!(flow, ControlFlow::Normal) {
                return Ok(flow);
            }
        }
        Ok(ControlFlow::Normal)
    }

    fn eval_function_statements(
        &self,
        statements: &[Statement],
        env: Rc<Environment>,
    ) -> Result<ControlFlow> {
        for (index, statement) in statements.iter().enumerate() {
            let is_last = index + 1 == statements.len();
            if is_last {
                if let Statement::ExpressionStatement(node) = statement {
                    return Ok(ControlFlow::Return(
                        self.eval_expression(&node.expression, env)?,
                    ));
                }
            }
            let flow = self.eval_statement(statement, Rc::clone(&env))?;
            if !matches!(flow, ControlFlow::Normal) {
                return Ok(flow);
            }
            let do_return = self.get_special_prop("__do_return__");
            if !matches!(do_return, Value::Null) {
                self.set_special_prop("__do_return__", Value::Null);
                return Ok(ControlFlow::Return(do_return));
            }
        }
        Ok(ControlFlow::Normal)
    }

    fn eval_statement(&self, statement: &Statement, env: Rc<Environment>) -> Result<ControlFlow> {
        match statement {
            Statement::Block(block) => self.eval_block(block, env),
            Statement::VariableDeclaration(node) => {
                let value = match node.init.as_ref() {
                    Some(init) => self.eval_expression(init, Rc::clone(&env))?,
                    None => Value::Null,
                };
                if node.init.is_some() && node.runtime_typecheck_required != Some(false) {
                    self.assert_declared_type(node.declared_type.as_deref(), &value, &node.name)?;
                }
                env.define_with_storage(
                    node.name.clone(),
                    value,
                    node.kind != "const",
                    node.is_weak_storage,
                );
                Ok(ControlFlow::Normal)
            }
            Statement::FunctionDeclaration(node) => {
                let func = self.make_function_from_decl(node, Rc::clone(&env));
                env.define(node.name.clone(), Value::Function(Rc::new(func)), false);
                Ok(ControlFlow::Normal)
            }
            Statement::ClassDeclaration(node) => {
                let class = self.make_user_class_from_decl(node, Rc::clone(&env))?;
                env.define(node.name.clone(), Value::UserClass(Rc::new(class)), false);
                Ok(ControlFlow::Normal)
            }
            Statement::TraitDeclaration(node) => {
                let mut methods = HashMap::new();
                for member in &node.body {
                    if let ClassMember::Method(method) = member {
                        methods.insert(
                            method.name.clone(),
                            Rc::new(self.make_method_from_decl(method, Rc::clone(&env))),
                        );
                    }
                }
                env.define(
                    node.name.clone(),
                    Value::Trait(Rc::new(TraitValue {
                        name: node.name.clone(),
                        methods,
                        source_decl: Some(node.clone()),
                        closure_env: Some(Rc::clone(&env)),
                    })),
                    false,
                );
                Ok(ControlFlow::Normal)
            }
            Statement::ImportDeclaration(node) => {
                self.eval_import(node, env)?;
                Ok(ControlFlow::Normal)
            }
            Statement::IfStatement(node) => {
                if self.value_is_truthy(&self.eval_expression(&node.test, Rc::clone(&env))?)? {
                    self.eval_block(&node.consequent, env)
                } else if let Some(alternate) = &node.alternate {
                    self.eval_statement(alternate, env)
                } else {
                    Ok(ControlFlow::Normal)
                }
            }
            Statement::WhileStatement(node) => {
                loop {
                    if !self.value_is_truthy(&self.eval_expression(&node.test, Rc::clone(&env))?)? {
                        break;
                    }
                    match self.eval_block(&node.body, Rc::clone(&env))? {
                        ControlFlow::Normal => {}
                        ControlFlow::Continue => continue,
                        ControlFlow::Break => break,
                        other => return Ok(other),
                    }
                }
                Ok(ControlFlow::Normal)
            }
            Statement::TryStatement(node) => self.eval_try_statement(node, env),
            Statement::ReturnStatement(node) => Ok(ControlFlow::Return(
                self.eval_optional_expr(node.argument.as_ref(), env)?,
            )),
            Statement::LoopControlStatement(node) => match node.keyword.as_str() {
                "next" | "continue" => Ok(ControlFlow::Continue),
                "last" => Ok(ControlFlow::Break),
                _ => Err(ZuzuRustError::runtime(format!(
                    "unsupported loop control '{}'",
                    node.keyword
                ))),
            },
            Statement::ThrowStatement(node) => {
                let value = self.eval_expression(&node.argument, env)?;
                self.annotate_exception_metadata(&value, node.source_file.as_deref(), node.line);
                match &value {
                    Value::String(text) => Err(ZuzuRustError::thrown(text.clone())),
                    _ => Err(ZuzuRustError::thrown_with_token(
                        self.render_value(&value)?,
                        self.store_thrown_value(value)?,
                    )),
                }
            }
            Statement::DieStatement(node) => {
                let value = self.eval_expression(&node.argument, env)?;
                let value =
                    self.normalize_die_value(value, node.source_file.as_deref(), node.line)?;
                Err(ZuzuRustError::thrown_with_token(
                    self.render_value(&value)?,
                    self.store_thrown_value(value)?,
                ))
            }
            Statement::PostfixConditionalStatement(node) => {
                let test =
                    self.value_is_truthy(&self.eval_expression(&node.test, Rc::clone(&env))?)?;
                let should_run = if node.keyword == "if" { test } else { !test };
                if should_run {
                    self.eval_statement(&node.statement, env)
                } else {
                    Ok(ControlFlow::Normal)
                }
            }
            Statement::KeywordStatement(node) => {
                match node.keyword.as_str() {
                    "say" => {
                        let values = self.eval_arguments(&node.arguments, env)?;
                        let line = values
                            .iter()
                            .map(|value| self.render_value(value))
                            .collect::<Result<Vec<_>>>()?
                            .join("");
                        self.emit_stdout(&format!("{line}\n"))?;
                    }
                    "print" => {
                        let values = self.eval_arguments(&node.arguments, env)?;
                        let text = values
                            .iter()
                            .map(|value| self.render_value(value))
                            .collect::<Result<Vec<_>>>()?
                            .join("");
                        self.emit_stdout(&text)?;
                    }
                    "warn" => {
                        let values = self.eval_arguments(&node.arguments, env)?;
                        let text = values
                            .iter()
                            .map(|value| self.render_value(value))
                            .collect::<Result<Vec<_>>>()?
                            .join("");
                        self.emit_stderr(&format!("{text}\n"))?;
                    }
                    "debug" => {
                        let level = if let Some(expr) = node.arguments.first() {
                            self.value_to_number(&self.eval_expression(expr, Rc::clone(&env))?)?
                                as u32
                        } else {
                            0
                        };
                        if level <= self.policy.debug_level {
                            let message = if let Some(expr) = node.arguments.get(1) {
                                self.render_value(&self.eval_expression(expr, env)?)?
                            } else {
                                String::new()
                            };
                            self.emit_stderr(&format!("{message}\n"))?;
                        }
                    }
                    "assert" => {
                        if self.policy.debug_level > 0 {
                            let value = if let Some(expr) = node.arguments.first() {
                                self.eval_expression(expr, env)?
                            } else {
                                Value::Null
                            };
                            if !self.value_is_truthy(&value)? {
                                return Err(ZuzuRustError::thrown("Assertion failed"));
                            }
                        }
                    }
                    other => {
                        return Err(ZuzuRustError::runtime(format!(
                            "unsupported keyword statement '{}'",
                            other
                        )))
                    }
                }
                Ok(ControlFlow::Normal)
            }
            Statement::ExpressionStatement(node) => {
                let _ = self.eval_expression(&node.expression, env)?;
                Ok(ControlFlow::Normal)
            }
            Statement::ForStatement(node) => self.eval_for_statement(node, env),
            Statement::SwitchStatement(node) => self.eval_switch_statement(node, env),
        }
    }

    fn eval_block(&self, block: &BlockStatement, env: Rc<Environment>) -> Result<ControlFlow> {
        let block_env = if block.needs_lexical_scope {
            Rc::new(Environment::new(Some(env)))
        } else {
            env
        };
        self.push_special_props_scope();
        let result = self.eval_statements(&block.statements, Rc::clone(&block_env));
        if block.needs_lexical_scope {
            self.cleanup_scope(&block_env)?;
        }
        self.pop_special_props_scope();
        result
    }

    fn eval_try_statement(&self, node: &TryStatement, env: Rc<Environment>) -> Result<ControlFlow> {
        match self.eval_block(&node.body, Rc::clone(&env)) {
            Err(ZuzuRustError::Thrown { value, token }) => {
                let thrown_value = self.lookup_thrown_value(token.as_deref());
                for handler in &node.handlers {
                    if !self.catch_clause_matches(
                        handler.binding.as_ref(),
                        &value,
                        thrown_value.as_ref(),
                    ) {
                        continue;
                    }
                    let catch_env = Rc::new(Environment::new(Some(Rc::clone(&env))));
                    let binding_name = handler
                        .binding
                        .as_ref()
                        .and_then(|binding| binding.name.clone())
                        .unwrap_or_else(|| "e".to_owned());
                    let caught_value = self.make_catch_binding_value(
                        handler.binding.as_ref(),
                        &value,
                        thrown_value.as_ref(),
                    );
                    catch_env.define(binding_name, caught_value, true);
                    let flow = self.eval_block(&handler.body, catch_env)?;
                    return Ok(flow);
                }
                match token {
                    Some(token) => Err(ZuzuRustError::thrown_with_token(value, token)),
                    None => Err(ZuzuRustError::thrown(value)),
                }
            }
            Err(err) => {
                let rendered = err.to_string();
                for handler in &node.handlers {
                    if !self.catch_clause_matches(handler.binding.as_ref(), &rendered, None) {
                        continue;
                    }
                    let catch_env = Rc::new(Environment::new(Some(Rc::clone(&env))));
                    let binding_name = handler
                        .binding
                        .as_ref()
                        .and_then(|binding| binding.name.clone())
                        .unwrap_or_else(|| "e".to_owned());
                    let caught_value =
                        self.make_catch_binding_value(handler.binding.as_ref(), &rendered, None);
                    catch_env.define(binding_name, caught_value, true);
                    let flow = self.eval_block(&handler.body, catch_env)?;
                    return Ok(flow);
                }
                Err(err)
            }
            Ok(ControlFlow::Throw(value)) => {
                for handler in &node.handlers {
                    if !self.catch_clause_matches(
                        handler.binding.as_ref(),
                        &value.render(),
                        Some(&value),
                    ) {
                        continue;
                    }
                    let catch_env = Rc::new(Environment::new(Some(Rc::clone(&env))));
                    if let Some(binding) = &handler.binding {
                        if let Some(name) = &binding.name {
                            catch_env.define(name.clone(), value.clone(), true);
                        }
                    }
                    let flow = self.eval_block(&handler.body, catch_env)?;
                    return Ok(flow);
                }
                Ok(ControlFlow::Throw(value))
            }
            Ok(other) => Ok(other),
        }
    }

    fn catch_clause_matches(
        &self,
        binding: Option<&crate::ast::CatchBinding>,
        thrown_value: &str,
        thrown_runtime_value: Option<&Value>,
    ) -> bool {
        let Some(binding) = binding else {
            return true;
        };
        let Some(declared_type) = binding.declared_type.as_deref() else {
            return true;
        };
        if declared_type == "Any" {
            return true;
        }
        if let Some(value) = thrown_runtime_value {
            return self.value_matches_declared_type(declared_type, value);
        }
        match declared_type {
            "Exception" => true,
            "BailOutException" => thrown_value.starts_with("Bail out!"),
            "TypeException" => thrown_value.starts_with("TypeException:"),
            "CancelledException" => thrown_value.starts_with("CancelledException:"),
            "TimeoutException" => thrown_value.starts_with("TimeoutException:"),
            "ChannelClosedException" => thrown_value.starts_with("ChannelClosedException:"),
            "MarshallingException" => thrown_value.starts_with("MarshallingException:"),
            "UnmarshallingException" => thrown_value.starts_with("UnmarshallingException:"),
            "ExhaustedException" => {
                thrown_value == "ExhaustedException"
                    || thrown_value.starts_with("ExhaustedException:")
            }
            _ => false,
        }
    }

    fn eval_for_statement(&self, node: &ForStatement, env: Rc<Environment>) -> Result<ControlFlow> {
        let iterable = self.eval_expression(&node.iterable, Rc::clone(&env))?;
        let items = self.iterable_items(iterable)?;

        if items.is_empty() {
            if let Some(else_block) = &node.else_block {
                return self.eval_block(else_block, env);
            }
            return Ok(ControlFlow::Normal);
        }

        for item in items {
            let loop_env = Rc::new(Environment::new(Some(Rc::clone(&env))));
            loop_env.define(
                node.variable.clone(),
                item,
                node.binding_kind.as_deref() != Some("const"),
            );
            match self.eval_statements(&node.body.statements, loop_env)? {
                ControlFlow::Normal => {}
                ControlFlow::Continue => continue,
                ControlFlow::Break => break,
                other => return Ok(other),
            }
        }
        Ok(ControlFlow::Normal)
    }

    fn eval_switch_statement(
        &self,
        node: &SwitchStatement,
        env: Rc<Environment>,
    ) -> Result<ControlFlow> {
        let discriminant = self.eval_expression(&node.discriminant, Rc::clone(&env))?;
        if let Some(index) = &node.index {
            if let Some(key) = self.switch_index_key(&discriminant) {
                if let Some(entry) = index.iter().find(|entry| entry.key == key) {
                    return self.eval_switch_cases_from(node, entry.case_index, env);
                }
            }
        }
        let mut matched = false;
        for case in &node.cases {
            if !matched {
                for value in &case.values {
                    let candidate = self.eval_expression(value, Rc::clone(&env))?;
                    if self.switch_matches(&discriminant, &candidate, node.comparator.as_deref())? {
                        matched = true;
                        break;
                    }
                }
            }
            if matched {
                let case_env = Rc::new(Environment::new(Some(Rc::clone(&env))));
                match self.eval_statements(&case.consequent, case_env)? {
                    ControlFlow::Normal => return Ok(ControlFlow::Normal),
                    ControlFlow::Continue => continue,
                    other => return Ok(other),
                }
            }
        }
        if matched {
            if let Some(default) = &node.default {
                let default_env = Rc::new(Environment::new(Some(env)));
                return self.eval_statements(default, default_env);
            }
            return Ok(ControlFlow::Normal);
        }
        if let Some(default) = &node.default {
            let default_env = Rc::new(Environment::new(Some(env)));
            return self.eval_statements(default, default_env);
        }
        Ok(ControlFlow::Normal)
    }

    fn eval_switch_cases_from(
        &self,
        node: &SwitchStatement,
        start: usize,
        env: Rc<Environment>,
    ) -> Result<ControlFlow> {
        for case in node.cases.iter().skip(start) {
            let case_env = Rc::new(Environment::new(Some(Rc::clone(&env))));
            match self.eval_statements(&case.consequent, case_env)? {
                ControlFlow::Normal => return Ok(ControlFlow::Normal),
                ControlFlow::Continue => continue,
                other => return Ok(other),
            }
        }
        if let Some(default) = &node.default {
            let default_env = Rc::new(Environment::new(Some(env)));
            return self.eval_statements(default, default_env);
        }
        Ok(ControlFlow::Normal)
    }

    fn switch_index_key(&self, value: &Value) -> Option<String> {
        match self.deref_value(value).ok()? {
            Value::Null => Some("n:".to_owned()),
            Value::Boolean(value) => Some(format!("b:{value}")),
            Value::Number(value) => Some(format!("f:{value}")),
            Value::String(value) => Some(format!("s:{value}")),
            _ => None,
        }
    }

    fn switch_matches(
        &self,
        discriminant: &Value,
        candidate: &Value,
        comparator: Option<&str>,
    ) -> Result<bool> {
        match comparator.unwrap_or("=") {
            "=" | "==" | "≡" => Ok(discriminant.coerced_eq(candidate)),
            "eq" => Ok(self.render_value(discriminant)? == self.render_value(candidate)?),
            "~" => {
                let result = self.eval_regex_match(&self.render_value(discriminant)?, candidate)?;
                self.value_is_truthy(&result)
            }
            other => Err(ZuzuRustError::runtime(format!(
                "unsupported switch comparator '{}'",
                other
            ))),
        }
    }

    fn eval_collection_elements(
        &self,
        elements: &[Expression],
        capacity_hint: Option<usize>,
        env: Rc<Environment>,
        kind: CollectionKind,
    ) -> Result<Vec<Value>> {
        let mut values = Vec::with_capacity(capacity_hint.unwrap_or(elements.len()));
        for element in elements {
            let value = self.eval_expression(element, Rc::clone(&env))?;
            if matches!(
                element,
                Expression::Binary { operator, .. } if operator == "..."
            ) {
                match value {
                    Value::Array(items) => values.extend(items),
                    other => values.push(other),
                }
            } else {
                values.push(value);
            }
        }
        if matches!(kind, CollectionKind::Set) {
            let mut unique = Vec::with_capacity(values.len());
            for value in values {
                push_unique(&mut unique, value);
            }
            return Ok(unique);
        }
        Ok(values)
    }

    fn eval_import(&self, node: &ImportDeclaration, env: Rc<Environment>) -> Result<()> {
        if let Some(condition) = &node.condition {
            let test =
                self.value_is_truthy(&self.eval_expression(&condition.test, Rc::clone(&env))?)?;
            let should_import = if condition.keyword == "if" {
                test
            } else {
                !test
            };
            if !should_import {
                self.bind_null_imports(node, env);
                return Ok(());
            }
        }

        let exports = match self.load_module_exports(&node.source) {
            Ok(exports) => exports,
            Err(_) if node.try_mode => {
                self.bind_null_imports(node, env);
                return Ok(());
            }
            Err(err) => return Err(err),
        };
        if node.import_all {
            for (name, value) in exports {
                if name.starts_with('_') {
                    continue;
                }
                env.define(name, value, true);
            }
        } else {
            for specifier in &node.specifiers {
                let value = exports.get(&specifier.imported).cloned().ok_or_else(|| {
                    ZuzuRustError::runtime(format!(
                        "module '{}' does not export '{}'",
                        node.source, specifier.imported
                    ))
                })?;
                env.define(specifier.local.clone(), value, true);
            }
        }
        Ok(())
    }

    fn bind_null_imports(&self, node: &ImportDeclaration, env: Rc<Environment>) {
        if node.import_all {
            return;
        }
        for specifier in &node.specifiers {
            env.define(specifier.local.clone(), Value::Null, false);
        }
    }

    pub(in crate::runtime) fn load_module_exports(
        &self,
        name: &str,
    ) -> Result<HashMap<String, Value>> {
        if name.split('/').any(|segment| segment == "..") {
            return Err(ZuzuRustError::runtime(
                "import module path cannot contain '..' segments",
            ));
        }
        if self.module_denied_as_missing(name) {
            return Err(ZuzuRustError::runtime(format!(
                "module '{}' not found",
                name
            )));
        }
        if self.is_denied_module(name) {
            return Err(ZuzuRustError::runtime(format!(
                "module '{}' is denied by runtime policy",
                name
            )));
        }
        if let Some(cached) = self.module_cache.borrow().get(name) {
            return Ok(cached.exports.clone());
        }
        if self.module_loading.borrow().contains(name) {
            return Err(ZuzuRustError::thrown("circular module loading detected"));
        }

        if name == "std/gui/objects" && self.is_denied("gui") {
            return Err(ZuzuRustError::thrown(
                "std/gui/objects is denied by runtime policy",
            ));
        }

        if let Some(exports) = self.load_runtime_supported_module(name) {
            self.module_cache.borrow_mut().insert(
                name.to_owned(),
                ModuleRecord {
                    exports: exports.clone(),
                },
            );
            return Ok(exports);
        }

        self.module_loading.borrow_mut().insert(name.to_owned());
        let result = (|| {
            let path = self.resolve_module_path(name)?;
            let source = fs::read_to_string(&path)?;
            let source_file = path.display().to_string();
            let options = self.parse_options();
            let program = parse_program_with_compile_options_and_source_file(
                &source,
                &options,
                Some(&source_file),
            )?;
            self.emit_semantic_warnings(&program)?;
            let env = Rc::new(Environment::new(None));
            self.install_builtins(&env);
            match self.eval_program(&program, Rc::clone(&env))? {
                ControlFlow::Normal => {}
                ControlFlow::Return(_) => {
                    return Err(ZuzuRustError::runtime(format!(
                        "module '{}' returned from top level",
                        name
                    )))
                }
                ControlFlow::Throw(value) => {
                    return Err(ZuzuRustError::runtime(format!(
                        "module '{}' threw during import: {}",
                        name, value
                    )))
                }
                ControlFlow::Continue | ControlFlow::Break => {
                    return Err(ZuzuRustError::runtime(format!(
                        "module '{}' used loop control at top level",
                        name
                    )))
                }
            }

            let exports = env.export_public_aliases(Rc::clone(&env));
            self.module_cache.borrow_mut().insert(
                name.to_owned(),
                ModuleRecord {
                    exports: exports.clone(),
                },
            );
            Ok(exports)
        })();
        self.module_loading.borrow_mut().remove(name);
        result
    }

    fn resolve_module_path(&self, name: &str) -> Result<PathBuf> {
        for root in &self.module_roots {
            for ext in ["zzm", "zzs"] {
                let path = root.join(format!("{name}.{ext}"));
                if path.exists() {
                    return Ok(path);
                }
            }
        }
        Err(ZuzuRustError::runtime(format!(
            "module '{}' not found",
            name
        )))
    }

    pub(super) fn is_denied(&self, capability: &str) -> bool {
        if capability == "js" {
            return true;
        }
        self.policy.denied_capabilities.contains(capability)
            || matches!(
                self.get_special_prop(&format!("deny_{capability}")),
                Value::Boolean(true)
            )
    }

    #[allow(dead_code)]
    pub(super) fn assert_capability(&self, capability: &str, message: &str) -> Result<()> {
        if self.is_denied(capability) {
            return Err(ZuzuRustError::runtime(message.to_owned()));
        }
        Ok(())
    }

    fn is_denied_module(&self, name: &str) -> bool {
        self.policy.denied_modules.contains(name)
    }

    fn module_denied_as_missing(&self, name: &str) -> bool {
        (self.is_denied("fs") && name.starts_with("std/io"))
            || (self.is_denied("net") && name.starts_with("std/net"))
            || (self.is_denied("net") && name == "std/io/socks")
            || (self.is_denied("proc") && name == "std/proc")
            || (self.is_denied("db") && name == "std/db")
            || (self.is_denied("clib") && name == "std/clib")
            || (self.is_denied("js") && name == "javascript")
            || (self.is_denied("worker") && name == "std/worker")
    }

    pub(in crate::runtime) fn push_special_props_scope(&self) {
        self.special_props.borrow_mut().push(HashMap::new());
    }

    pub(in crate::runtime) fn pop_special_props_scope(&self) {
        let mut props = self.special_props.borrow_mut();
        if props.len() > 1 {
            props.pop();
        }
    }

    pub(in crate::runtime) fn is_effectively_denied(&self, capability: &str) -> bool {
        self.is_denied(capability)
    }

    pub(in crate::runtime) fn denial_flag(&self, capability: &str) -> bool {
        matches!(capability, "perl" | "js") || self.is_denied(capability)
    }

    pub(in crate::runtime) fn child_runtime_policy(
        &self,
        extra_denials: &[(String, bool)],
    ) -> RuntimePolicy {
        let mut policy = self.policy.clone();
        for capability in DENIAL_CAPABILITIES {
            if self.denial_flag(capability) {
                policy = policy.deny_capability(*capability);
            }
        }
        for (capability, denied) in extra_denials {
            if *denied {
                policy = policy.deny_capability(capability.clone());
            }
        }
        policy
    }

    pub(in crate::runtime) fn module_roots_clone(&self) -> Vec<PathBuf> {
        self.module_roots.clone()
    }

    pub(in crate::runtime) fn enter_async_context<T>(&self, f: impl FnOnce() -> T) -> T {
        self.async_executor.enter(f)
    }

    pub(in crate::runtime) fn check_worker_cancelled(&self) -> Result<()> {
        if self
            .worker_cancel_requested
            .as_ref()
            .map(|flag| flag.load(Ordering::SeqCst))
            .unwrap_or(false)
        {
            return Err(ZuzuRustError::thrown(
                "CancelledException: worker cancelled",
            ));
        }
        Ok(())
    }

    fn get_special_prop(&self, key: &str) -> Value {
        for frame in self.special_props.borrow().iter().rev() {
            if let Some(value) = frame.get(key) {
                return value.clone();
            }
        }
        Value::Null
    }

    pub(in crate::runtime) fn set_special_prop(&self, key: &str, value: Value) -> Value {
        let mut props = self.special_props.borrow_mut();
        if let Some(frame) = props.last_mut() {
            frame.insert(key.to_owned(), value.clone());
        }
        value
    }

    fn set_special_prop_at_level(&self, level: usize, key: &str, value: Value) -> Value {
        let mut props = self.special_props.borrow_mut();
        let Some(index) = props.len().checked_sub(1 + level) else {
            return value;
        };
        if let Some(frame) = props.get_mut(index) {
            frame.insert(key.to_owned(), value.clone());
        }
        value
    }

    fn get_special_prop_at_level(&self, level: usize, key: &str) -> Value {
        let props = self.special_props.borrow();
        let Some(index) = props.len().checked_sub(1 + level) else {
            return Value::Null;
        };
        props
            .get(index)
            .and_then(|frame| frame.get(key))
            .cloned()
            .unwrap_or(Value::Null)
    }

    pub(in crate::runtime) fn store_thrown_value(&self, value: Value) -> Result<String> {
        let mut next_id = self.next_thrown_id.borrow_mut();
        *next_id += 1;
        let token = format!("thrown:{}", *next_id);
        self.thrown_values.borrow_mut().insert(token.clone(), value);
        Ok(token)
    }

    fn lookup_thrown_value(&self, token: Option<&str>) -> Option<Value> {
        token.and_then(|key| self.thrown_values.borrow().get(key).cloned())
    }

    fn load_runtime_supported_module(&self, name: &str) -> Option<HashMap<String, Value>> {
        stdlib::load_runtime_supported_module(name)
    }

    fn current_system_value(&self) -> Value {
        Value::Dict(HashMap::from([
            ("runtime".to_owned(), Value::String("zuzu-rust".to_owned())),
            ("language_version".to_owned(), Value::Number(0.0)),
            (
                "runtime_version".to_owned(),
                Value::String(env!("CARGO_PKG_VERSION").to_owned()),
            ),
            (
                "platform".to_owned(),
                Value::String(std::env::consts::OS.to_owned()),
            ),
            (
                "inc".to_owned(),
                Value::String(
                    self.module_roots
                        .iter()
                        .map(|path| path.to_string_lossy().into_owned())
                        .collect::<Vec<_>>()
                        .join(":"),
                ),
            ),
            ("deny_fs".to_owned(), Value::Boolean(self.is_denied("fs"))),
            ("deny_net".to_owned(), Value::Boolean(self.is_denied("net"))),
            ("deny_perl".to_owned(), Value::Boolean(true)),
            ("deny_js".to_owned(), Value::Boolean(true)),
            (
                "deny_proc".to_owned(),
                Value::Boolean(self.is_denied("proc")),
            ),
            ("deny_db".to_owned(), Value::Boolean(self.is_denied("db"))),
            (
                "deny_clib".to_owned(),
                Value::Boolean(self.is_denied("clib")),
            ),
            ("deny_gui".to_owned(), Value::Boolean(self.is_denied("gui"))),
            (
                "deny_worker".to_owned(),
                Value::Boolean(self.is_denied("worker")),
            ),
        ]))
    }

    fn make_function_from_decl(
        &self,
        node: &FunctionDeclaration,
        env: Rc<Environment>,
    ) -> FunctionValue {
        FunctionValue {
            name: Some(node.name.clone()),
            params: node.params.clone(),
            return_type: node.return_type.clone(),
            body: FunctionBody::Block(node.body.clone()),
            env,
            is_async: node.is_async,
            current_method: None,
        }
    }

    fn make_user_class_from_decl(
        &self,
        node: &ClassDeclaration,
        env: Rc<Environment>,
    ) -> Result<UserClassValue> {
        let base = match &node.base {
            Some(name) => match env.get(name)? {
                Value::UserClass(class) => Some(ClassBase::User(class)),
                Value::Class(name) => Some(ClassBase::Builtin(name.as_ref().clone())),
                _ => return Err(ZuzuRustError::runtime(format!("'{}' is not a class", name))),
            },
            None => None,
        };

        let class_env = Rc::new(Environment::new(Some(Rc::clone(&env))));
        let mut fields = Vec::new();
        let mut nested_classes = HashMap::new();
        for member in &node.body {
            match member {
                ClassMember::Field(field) => fields.push(self.make_field_spec(field)),
                ClassMember::Class(class) => {
                    let mut nested_class =
                        self.make_user_class_from_decl(class, Rc::clone(&class_env))?;
                    nested_class.name = format!("{}{{\"{}\"}}", node.name, class.name);
                    let nested = Rc::new(nested_class);
                    class_env.define(
                        class.name.clone(),
                        Value::UserClass(Rc::clone(&nested)),
                        false,
                    );
                    nested_classes.insert(class.name.clone(), nested);
                }
                ClassMember::Method(_) | ClassMember::Trait(_) => {}
            }
        }

        let mut methods = HashMap::new();
        let mut static_methods = HashMap::new();
        for member in &node.body {
            match member {
                ClassMember::Method(method) => {
                    let method_value =
                        Rc::new(self.make_method_from_decl(method, Rc::clone(&class_env)));
                    if method.is_static {
                        static_methods.insert(method.name.clone(), method_value);
                    } else {
                        methods.insert(method.name.clone(), method_value);
                    }
                }
                ClassMember::Field(_) | ClassMember::Class(_) | ClassMember::Trait(_) => {}
            }
        }

        let mut traits = Vec::new();
        for trait_name in &node.traits {
            match env.get(trait_name)? {
                Value::Trait(trait_value) => traits.push(trait_value),
                _ => {
                    return Err(ZuzuRustError::runtime(format!(
                        "'{}' is not a trait",
                        trait_name
                    )))
                }
            }
        }

        Ok(UserClassValue {
            name: node.name.clone(),
            base,
            traits,
            fields,
            methods,
            static_methods,
            nested_classes,
            source_decl: Some(node.clone()),
            closure_env: Some(env),
        })
    }

    fn make_field_spec(&self, node: &FieldDeclaration) -> FieldSpec {
        FieldSpec {
            name: node.name.clone(),
            declared_type: node.declared_type.clone(),
            mutable: node.kind != "const",
            accessors: node.accessors.clone(),
            default_value: node.default_value.clone(),
            is_weak_storage: node.is_weak_storage,
        }
    }

    fn make_method_from_decl(&self, node: &MethodDeclaration, env: Rc<Environment>) -> MethodValue {
        MethodValue {
            name: node.name.clone(),
            params: node.params.clone(),
            return_type: node.return_type.clone(),
            body: node.body.clone(),
            env,
            is_static: node.is_static,
            is_async: node.is_async,
            bound_receiver: None,
            bound_name: None,
        }
    }

    fn install_builtins(&self, env: &Environment) {
        for name in [
            "Any",
            "Class",
            "Null",
            "Object",
            "Collection",
            "Boolean",
            "Number",
            "String",
            "BinaryString",
            "Regexp",
            "Function",
            "Pair",
            "PairList",
            "Array",
            "Bag",
            "Set",
            "Dict",
            "ExhaustedException",
            "TypeException",
            "CancelledException",
            "TimeoutException",
            "ChannelClosedException",
        ] {
            env.define(
                name.to_owned(),
                Value::builtin_class(name.to_owned()),
                false,
            );
        }
        env.define(
            "Exception".to_owned(),
            Value::UserClass(Rc::new(UserClassValue {
                name: "Exception".to_owned(),
                base: None,
                traits: Vec::new(),
                fields: exception_field_specs(),
                methods: HashMap::new(),
                static_methods: HashMap::new(),
                nested_classes: HashMap::new(),
                source_decl: None,
                closure_env: None,
            })),
            false,
        );
        env.define(
            "DEBUG".to_owned(),
            Value::Number(self.policy.debug_level as f64),
            false,
        );
        for name in ["to_binary", "to_string"] {
            env.define(
                name.to_owned(),
                Value::native_function(name.to_owned()),
                false,
            );
        }
        env.define("__global__".to_owned(), Value::Dict(HashMap::new()), true);
        env.define("__system__".to_owned(), self.current_system_value(), false);
    }

    fn eval_arguments(&self, arguments: &[Expression], env: Rc<Environment>) -> Result<Vec<Value>> {
        arguments
            .iter()
            .map(|argument| self.eval_expression(argument, Rc::clone(&env)))
            .collect()
    }

    fn eval_optional_expr(&self, expr: Option<&Expression>, env: Rc<Environment>) -> Result<Value> {
        match expr {
            Some(expr) => self.eval_expression(expr, env),
            None => Ok(Value::Null),
        }
    }

    fn eval_expression(&self, expr: &Expression, env: Rc<Environment>) -> Result<Value> {
        match expr {
            Expression::Identifier {
                name,
                binding_depth,
                ..
            } => {
                if name == "__system__" {
                    return Ok(self.current_system_value());
                }
                let value = env.get_resolved(*binding_depth, name)?;
                match value {
                    Value::AliasRef(reference) => self.call_ref(reference, Vec::new(), Vec::new()),
                    other => Ok(other),
                }
            }
            Expression::NumberLiteral { value, .. } => {
                Ok(Value::Number(value.parse::<f64>().map_err(|_| {
                    ZuzuRustError::runtime(format!("invalid number literal '{}'", value))
                })?))
            }
            Expression::StringLiteral { value, .. } => Ok(Value::String(value.clone())),
            Expression::RegexLiteral {
                pattern,
                flags,
                cache_key,
                ..
            } => {
                if cache_key.is_some() && self.optimizations.enables(OptimizationPass::RegexCache) {
                    let _ = self.compile_regex(pattern, flags)?;
                }
                Ok(Value::Regex(pattern.clone(), flags.clone()))
            }
            Expression::BooleanLiteral { value, .. } => Ok(Value::Boolean(*value)),
            Expression::NullLiteral { .. } => Ok(Value::Null),
            Expression::ArrayLiteral {
                elements,
                capacity_hint,
                ..
            } => Ok(Value::Array(
                self.eval_collection_elements(
                    elements,
                    *capacity_hint,
                    env,
                    CollectionKind::Array,
                )?
                .into_iter()
                .map(Value::into_shared_if_composite)
                .collect(),
            )),
            Expression::SetLiteral {
                elements,
                capacity_hint,
                ..
            } => {
                let mut values = Vec::with_capacity(capacity_hint.unwrap_or(elements.len()));
                for value in self.eval_collection_elements(
                    elements,
                    *capacity_hint,
                    env,
                    CollectionKind::Set,
                )? {
                    push_unique(&mut values, value.into_shared_if_composite());
                }
                Ok(Value::Set(values))
            }
            Expression::BagLiteral {
                elements,
                capacity_hint,
                ..
            } => Ok(Value::Bag(
                self.eval_collection_elements(elements, *capacity_hint, env, CollectionKind::Bag)?
                    .into_iter()
                    .map(Value::into_shared_if_composite)
                    .collect(),
            )),
            Expression::DictLiteral {
                entries,
                capacity_hint,
                ..
            } => {
                let mut map = HashMap::with_capacity(capacity_hint.unwrap_or(entries.len()));
                for entry in entries {
                    let key = self.eval_dict_key(&entry.key, Rc::clone(&env))?;
                    let value = self.eval_expression(&entry.value, Rc::clone(&env))?;
                    map.insert(key, value.into_shared_if_composite());
                }
                Ok(Value::Dict(map))
            }
            Expression::PairListLiteral {
                entries,
                capacity_hint,
                ..
            } => {
                let mut values = Vec::with_capacity(capacity_hint.unwrap_or(entries.len()));
                for entry in entries {
                    let key = self.eval_dict_key(&entry.key, Rc::clone(&env))?;
                    let value = self.eval_expression(&entry.value, Rc::clone(&env))?;
                    values.push((key, value.into_shared_if_composite()));
                }
                Ok(Value::PairList(values))
            }
            Expression::TemplateLiteral { parts, .. } => {
                let mut out = String::new();
                for part in parts {
                    match part {
                        TemplatePart::Text { value, .. } => out.push_str(value),
                        TemplatePart::Expression { expression, .. } => {
                            let value = self.eval_expression(expression, Rc::clone(&env))?;
                            out.push_str(&self.render_value(&value)?);
                        }
                    }
                }
                Ok(Value::String(out))
            }
            Expression::Unary {
                operator, argument, ..
            } => match operator.as_str() {
                "new" => self.eval_new_expression(argument, env),
                "not" | "!" | "¬" => Ok(Value::Boolean(
                    !self.value_is_truthy(&self.eval_expression(argument, env)?)?,
                )),
                "+" => Ok(Value::Number(
                    self.value_to_number(&self.eval_expression(argument, env)?)?,
                )),
                "-" => Ok(Value::Number(
                    -self.value_to_number(&self.eval_expression(argument, env)?)?,
                )),
                "abs" => Ok(Value::Number(
                    self.value_to_number(&self.eval_expression(argument, env)?)?
                        .abs(),
                )),
                "sqrt" | "√" => Ok(Value::Number(
                    self.value_to_number(&self.eval_expression(argument, env)?)?
                        .sqrt(),
                )),
                "floor" => Ok(Value::Number(
                    self.value_to_number(&self.eval_expression(argument, env)?)?
                        .floor(),
                )),
                "ceil" => Ok(Value::Number(
                    self.value_to_number(&self.eval_expression(argument, env)?)?
                        .ceil(),
                )),
                "round" => Ok(Value::Number(
                    self.value_to_number(&self.eval_expression(argument, env)?)?
                        .round(),
                )),
                "int" => Ok(Value::Number(
                    self.value_to_number(&self.eval_expression(argument, env)?)?
                        .trunc(),
                )),
                "uc" => Ok(Value::String(
                    self.value_to_operator_string(&self.eval_expression(argument, env)?)?
                        .to_uppercase(),
                )),
                "lc" => Ok(Value::String(
                    self.value_to_operator_string(&self.eval_expression(argument, env)?)?
                        .to_lowercase(),
                )),
                "length" => Ok(Value::Number(
                    match self.eval_expression(argument, env)? {
                        Value::BinaryString(bytes) => bytes.len() as f64,
                        value => self.value_to_operator_string(&value)?.chars().count() as f64,
                    },
                )),
                "typeof" => Ok(Value::String(
                    self.typeof_name(&self.eval_expression(argument, env)?),
                )),
                "~" => {
                    let value = self.eval_expression(argument, env)?;
                    match value {
                        Value::BinaryString(bytes) => Ok(Value::BinaryString(
                            bytes.into_iter().map(|byte| !byte).collect(),
                        )),
                        _ => Ok(Value::Number(
                            !(self.value_to_number(&value)? as i64) as f64,
                        )),
                    }
                }
                "\\" if matches!(argument.as_ref(), Expression::Binary { operator, .. } if is_path_operator(operator)) => {
                    self.eval_path_ref(argument, env)
                }
                "\\" => Ok(Value::Ref(Rc::new(LvalueRef::Expression {
                    env,
                    target: (**argument).clone(),
                }))),
                "++" if matches!(argument.as_ref(), Expression::Binary { operator, .. } if is_path_operator(operator)) => {
                    self.eval_path_update(argument, env, 1.0, true)
                }
                "--" if matches!(argument.as_ref(), Expression::Binary { operator, .. } if is_path_operator(operator)) => {
                    self.eval_path_update(argument, env, -1.0, true)
                }
                "++" => self.update_lvalue(argument, env, |value| {
                    Ok(Value::Number(self.value_to_number(&value)? + 1.0))
                }),
                "--" => self.update_lvalue(argument, env, |value| {
                    Ok(Value::Number(self.value_to_number(&value)? - 1.0))
                }),
                other => Err(ZuzuRustError::runtime(format!(
                    "unsupported unary operator '{}'",
                    other
                ))),
            },
            Expression::Binary {
                operator,
                left,
                right,
                ..
            } => self.eval_binary(operator, left, right, env),
            Expression::Ternary {
                test,
                consequent,
                alternate,
                ..
            } => {
                if self.value_is_truthy(&self.eval_expression(test, Rc::clone(&env))?)? {
                    self.eval_expression(consequent, env)
                } else {
                    self.eval_expression(alternate, env)
                }
            }
            Expression::DefinedOr { left, right, .. } => {
                let left_value = self.eval_expression(left, Rc::clone(&env))?;
                if self.value_is_truthy(&left_value)? {
                    Ok(left_value)
                } else {
                    self.eval_expression(right, env)
                }
            }
            Expression::Assignment {
                operator,
                left,
                right,
                is_weak_write,
                ..
            } => {
                if operator == "~=" {
                    if let Expression::Binary {
                        operator: path_operator,
                        left: path_left,
                        right: path_right,
                        ..
                    } = left.as_ref()
                    {
                        if is_path_operator(path_operator) {
                            let payload = self.build_path_regex_payload(right, Rc::clone(&env))?;
                            return self.assign_path_target(
                                path_operator,
                                path_left,
                                path_right,
                                operator,
                                payload,
                                false,
                                env,
                            );
                        }
                    }
                }
                if operator == "~=" {
                    let current = self.eval_expression(left, Rc::clone(&env))?;
                    let replacement =
                        self.apply_regex_replacement(current, right, Rc::clone(&env))?;
                    self.assign_lvalue(left, replacement, false, env)
                } else {
                    let right_value = self.eval_expression(right, Rc::clone(&env))?;
                    self.assign(operator, left, right_value, *is_weak_write, env)
                }
            }
            Expression::Call {
                callee, arguments, ..
            } => match callee.as_ref() {
                Expression::MemberAccess { object, member, .. } => {
                    self.invoke_method_expression(object, member, arguments, env)
                }
                _ => {
                    let callee = self.eval_expression(callee, Rc::clone(&env))?;
                    let (values, named_args) =
                        if matches!(callee, Value::Class(_) | Value::UserClass(_)) {
                            self.eval_constructor_call_arguments(arguments, Rc::clone(&env))?
                        } else {
                            self.eval_call_arguments(arguments, Rc::clone(&env))?
                        };
                    self.with_current_env(env, || self.call_value(callee, values, named_args))
                }
            },
            Expression::DynamicMemberCall {
                object,
                member,
                arguments,
                ..
            } => {
                let receiver = self.eval_expression(object, Rc::clone(&env))?;
                let member_value = self.eval_expression(member, Rc::clone(&env))?;
                let method_name = match member_value {
                    Value::Method(method) => method.name.clone(),
                    other => self.render_value(&other)?,
                };
                let (values, named_args) =
                    self.eval_method_call_arguments(arguments, Rc::clone(&env))?;
                let mut receiver = receiver;
                self.call_method_named(&mut receiver, &method_name, &values, named_args)
            }
            Expression::MemberAccess { object, member, .. } => {
                let mut object = self.eval_expression(object, env)?;
                self.call_method(&mut object, member, &[])
            }
            Expression::Index { object, index, .. } => {
                let object = self.eval_expression(object, Rc::clone(&env))?;
                let index = self.eval_expression(index, env)?;
                self.eval_index(object, index)
            }
            Expression::Slice {
                object, start, end, ..
            } => {
                let object = self.eval_expression(object, Rc::clone(&env))?;
                let start = self.eval_optional_expr(start.as_deref(), Rc::clone(&env))?;
                let end = self.eval_optional_expr(end.as_deref(), env)?;
                self.eval_slice(object, start, end)
            }
            Expression::DictAccess { object, key, .. } => {
                let object = self.deref_value(&self.eval_expression(object, Rc::clone(&env))?)?;
                let key = self.eval_dict_key(key, env)?;
                match object {
                    Value::Dict(map) => Ok(map.get(&key).cloned().unwrap_or(Value::Null)),
                    Value::Pair(pair_key, value) => Ok(if key == "pair" {
                        Value::Array(vec![Value::String(pair_key), *value])
                    } else {
                        Value::Null
                    }),
                    Value::PairList(values) => Ok(values
                        .iter()
                        .find(|(entry_key, _)| entry_key == &key)
                        .map(|(_, value)| value.clone())
                        .unwrap_or(Value::Null)),
                    Value::Object(object) => Ok(object
                        .borrow()
                        .fields
                        .get(&key)
                        .cloned()
                        .or_else(|| {
                            self.find_method(&object.borrow().class, &key)
                                .and_then(|_| {
                                    self.marshal_bind_method(
                                        Value::Object(Rc::clone(&object)),
                                        &key,
                                    )
                                    .ok()
                                })
                        })
                        .or_else(|| {
                            object
                                .borrow()
                                .class
                                .nested_classes
                                .get(&key)
                                .map(|class| Value::UserClass(Rc::clone(class)))
                        })
                        .unwrap_or(Value::Null)),
                    Value::UserClass(class) => Ok(class
                        .methods
                        .get(&key)
                        .cloned()
                        .map(Value::Method)
                        .or_else(|| class.static_methods.get(&key).cloned().map(Value::Method))
                        .or_else(|| {
                            class
                                .nested_classes
                                .get(&key)
                                .map(|nested| Value::UserClass(Rc::clone(nested)))
                        })
                        .unwrap_or(Value::Null)),
                    _ => Err(ZuzuRustError::runtime(
                        "dict access requires a Dict or object value",
                    )),
                }
            }
            Expression::PostfixUpdate {
                operator, argument, ..
            } => {
                if matches!(argument.as_ref(), Expression::Binary { operator, .. } if is_path_operator(operator))
                {
                    return self.eval_path_update(
                        argument,
                        env,
                        if operator == "++" { 1.0 } else { -1.0 },
                        false,
                    );
                }
                let previous = self.eval_expression(argument, Rc::clone(&env))?;
                let delta = match operator.as_str() {
                    "++" => 1.0,
                    "--" => -1.0,
                    other => {
                        return Err(ZuzuRustError::runtime(format!(
                            "unsupported postfix operator '{}'",
                            other
                        )))
                    }
                };
                let _ = self.update_lvalue(argument, env, |value| {
                    Ok(Value::Number(self.value_to_number(&value)? + delta))
                })?;
                Ok(previous)
            }
            Expression::FunctionExpression {
                params,
                return_type,
                body,
                is_async,
                ..
            } => Ok(Value::Function(Rc::new(FunctionValue {
                name: None,
                params: params.clone(),
                return_type: return_type.clone(),
                body: FunctionBody::Block(body.clone()),
                env,
                is_async: *is_async,
                current_method: None,
            }))),
            Expression::Lambda {
                params,
                body,
                is_async,
                ..
            } => Ok(Value::Function(Rc::new(FunctionValue {
                name: None,
                params: params.clone(),
                return_type: None,
                body: FunctionBody::Expression((**body).clone()),
                env,
                is_async: *is_async,
                current_method: None,
            }))),
            Expression::LetExpression {
                kind,
                name,
                init,
                is_weak_storage,
                ..
            } => {
                let value = match init {
                    Some(init) => self.eval_expression(init, Rc::clone(&env))?,
                    None => Value::Null,
                };
                env.define_with_storage(
                    name.clone(),
                    value.clone(),
                    kind != "const",
                    *is_weak_storage,
                );
                Ok(value)
            }
            Expression::DoExpression { body, .. } => self.eval_do_expression(body, env),
            Expression::AwaitExpression { body, .. } => {
                let value = self.eval_do_expression(body, env)?;
                self.await_value(value)
            }
            Expression::SpawnExpression { body, .. } => {
                let task = Rc::new(RefCell::new(TaskState {
                    status: "pending".to_owned(),
                    kind: TaskKind::Spawn {
                        body: body.clone(),
                        env,
                        started: false,
                    },
                    outcome: None,
                }));
                self.background_tasks.borrow_mut().push(Rc::clone(&task));
                self.schedule_task_driver(Rc::clone(&task));
                Ok(Value::Task(task))
            }
            Expression::TryExpression { body, handlers, .. } => {
                self.eval_try_expression(body, handlers, env)
            }
            Expression::SuperCall { arguments, .. } => {
                let (values, named_args) = self.eval_call_arguments(arguments, Rc::clone(&env))?;
                if !named_args.is_empty() {
                    return Err(ZuzuRustError::runtime(
                        "named arguments are not implemented for super()",
                    ));
                }
                self.eval_super_call(values, env)
            }
        }
    }

    fn eval_call_arguments(
        &self,
        arguments: &[CallArgument],
        env: Rc<Environment>,
    ) -> Result<(Vec<Value>, Vec<(String, Value)>)> {
        let mut positional = Vec::with_capacity(arguments.len());
        let mut named = Vec::new();
        for argument in arguments {
            match argument {
                CallArgument::Positional { value, .. } => {
                    positional.push(self.eval_expression(value, Rc::clone(&env))?);
                }
                CallArgument::Named { name, value, .. } => {
                    named.push((
                        self.eval_dict_key(name, Rc::clone(&env))?,
                        self.eval_expression(value, Rc::clone(&env))?,
                    ));
                }
            }
        }
        Ok((positional, named))
    }

    fn eval_method_call_arguments(
        &self,
        arguments: &[CallArgument],
        env: Rc<Environment>,
    ) -> Result<(Vec<Value>, Vec<(String, Value)>)> {
        self.eval_call_arguments(arguments, env)
    }

    fn eval_constructor_call_arguments(
        &self,
        arguments: &[CallArgument],
        env: Rc<Environment>,
    ) -> Result<(Vec<Value>, Vec<(String, Value)>)> {
        let mut positional = Vec::with_capacity(arguments.len());
        let mut named = Vec::new();
        for argument in arguments {
            match argument {
                CallArgument::Positional { value, .. } => {
                    positional.push(self.eval_expression(value, Rc::clone(&env))?);
                }
                CallArgument::Named { name, value, .. } => {
                    named.push((
                        self.eval_dict_key(name, Rc::clone(&env))?,
                        self.eval_expression(value, Rc::clone(&env))?,
                    ));
                }
            }
        }
        Ok((positional, named))
    }

    fn eval_path_haystack_argument(
        &self,
        expr: &Expression,
        env: Rc<Environment>,
    ) -> Result<Value> {
        self.eval_expression(expr, env)
    }

    fn eval_binary(
        &self,
        operator: &str,
        left: &Expression,
        right: &Expression,
        env: Rc<Environment>,
    ) -> Result<Value> {
        match operator {
            "and" | "⋀" => {
                let left_value = self.eval_expression(left, Rc::clone(&env))?;
                if !self.value_is_truthy(&left_value)? {
                    return Ok(Value::Boolean(false));
                }
                Ok(Value::Boolean(
                    self.value_is_truthy(&self.eval_expression(right, env)?)?,
                ))
            }
            "or" | "⋁" => {
                let left_value = self.eval_expression(left, Rc::clone(&env))?;
                if self.value_is_truthy(&left_value)? {
                    return Ok(Value::Boolean(true));
                }
                Ok(Value::Boolean(
                    self.value_is_truthy(&self.eval_expression(right, env)?)?,
                ))
            }
            "xor" | "⊻" => {
                let lhs = self.value_is_truthy(&self.eval_expression(left, Rc::clone(&env))?)?;
                let rhs = self.value_is_truthy(&self.eval_expression(right, env)?)?;
                Ok(Value::Boolean(lhs ^ rhs))
            }
            "nand" | "⊼" => {
                let lhs = self.value_is_truthy(&self.eval_expression(left, Rc::clone(&env))?)?;
                let rhs = self.value_is_truthy(&self.eval_expression(right, env)?)?;
                Ok(Value::Boolean(!(lhs && rhs)))
            }
            "..." => {
                let start =
                    self.value_to_number(&self.eval_expression(left, Rc::clone(&env))?)? as i64;
                let end = self.value_to_number(&self.eval_expression(right, env)?)? as i64;
                let mut values = Vec::new();
                if start <= end {
                    for value in start..=end {
                        values.push(Value::Number(value as f64));
                    }
                } else {
                    for value in (end..=start).rev() {
                        values.push(Value::Number(value as f64));
                    }
                }
                Ok(Value::Array(values))
            }
            "_" => {
                let lhs = self.eval_expression(left, Rc::clone(&env))?;
                let rhs = self.eval_expression(right, env)?;
                self.concat_values(lhs, rhs)
            }
            "+" | "-" | "*" | "×" | "/" | "÷" | "mod" | "**" => {
                let lhs = self.value_to_number(&self.eval_expression(left, Rc::clone(&env))?)?;
                let rhs = self.value_to_number(&self.eval_expression(right, env)?)?;
                let value = match operator {
                    "+" => lhs + rhs,
                    "-" => lhs - rhs,
                    "*" | "×" => lhs * rhs,
                    "/" | "÷" => lhs / rhs,
                    "mod" => lhs % rhs,
                    "**" => lhs.powf(rhs),
                    _ => unreachable!(),
                };
                Ok(Value::Number(value))
            }
            ">" | "<" | ">=" | "≤" | "<=" | "≥" | "=" | "!=" | "≠" | "==" | "≡" | "≢" | "eq"
            | "ne" | "gt" | "ge" | "lt" | "le" | "eqi" | "nei" | "gti" | "gei" | "lti" | "lei" => {
                let lhs = self.eval_expression(left, Rc::clone(&env))?;
                let rhs = self.eval_expression(right, env)?;
                let result = match operator {
                    ">" => self.value_to_number(&lhs)? > self.value_to_number(&rhs)?,
                    "<" => self.value_to_number(&lhs)? < self.value_to_number(&rhs)?,
                    ">=" | "≥" => self.value_to_number(&lhs)? >= self.value_to_number(&rhs)?,
                    "<=" | "≤" => self.value_to_number(&lhs)? <= self.value_to_number(&rhs)?,
                    "=" => {
                        if lhs.is_numeric_comparable() && rhs.is_numeric_comparable() {
                            self.value_to_number(&lhs)? == self.value_to_number(&rhs)?
                        } else {
                            lhs.coerced_eq(&rhs)
                        }
                    }
                    "!=" | "≠" => {
                        if lhs.is_numeric_comparable() && rhs.is_numeric_comparable() {
                            self.value_to_number(&lhs)? != self.value_to_number(&rhs)?
                        } else {
                            !lhs.strict_eq(&rhs)
                        }
                    }
                    "==" | "≡" => lhs.strict_eq(&rhs),
                    "≢" => !lhs.strict_eq(&rhs),
                    "eq" => {
                        self.value_to_operator_string(&lhs)?
                            == self.value_to_operator_string(&rhs)?
                    }
                    "ne" => {
                        self.value_to_operator_string(&lhs)?
                            != self.value_to_operator_string(&rhs)?
                    }
                    "gt" => {
                        self.value_to_operator_string(&lhs)?
                            > self.value_to_operator_string(&rhs)?
                    }
                    "ge" => {
                        self.value_to_operator_string(&lhs)?
                            >= self.value_to_operator_string(&rhs)?
                    }
                    "lt" => {
                        self.value_to_operator_string(&lhs)?
                            < self.value_to_operator_string(&rhs)?
                    }
                    "le" => {
                        self.value_to_operator_string(&lhs)?
                            <= self.value_to_operator_string(&rhs)?
                    }
                    "eqi" => {
                        self.value_to_operator_string(&lhs)?.to_lowercase()
                            == self.value_to_operator_string(&rhs)?.to_lowercase()
                    }
                    "nei" => {
                        self.value_to_operator_string(&lhs)?.to_lowercase()
                            != self.value_to_operator_string(&rhs)?.to_lowercase()
                    }
                    "gti" => {
                        self.value_to_operator_string(&lhs)?.to_lowercase()
                            > self.value_to_operator_string(&rhs)?.to_lowercase()
                    }
                    "gei" => {
                        self.value_to_operator_string(&lhs)?.to_lowercase()
                            >= self.value_to_operator_string(&rhs)?.to_lowercase()
                    }
                    "lti" => {
                        self.value_to_operator_string(&lhs)?.to_lowercase()
                            < self.value_to_operator_string(&rhs)?.to_lowercase()
                    }
                    "lei" => {
                        self.value_to_operator_string(&lhs)?.to_lowercase()
                            <= self.value_to_operator_string(&rhs)?.to_lowercase()
                    }
                    _ => unreachable!(),
                };
                Ok(Value::Boolean(result))
            }
            "~" => {
                let target =
                    self.value_to_operator_string(&self.eval_expression(left, Rc::clone(&env))?)?;
                let regex = self.eval_expression(right, env)?;
                self.eval_regex_match(&target, &regex)
            }
            "<=>" | "≶" | "≷" => {
                let lhs = self.value_to_number(&self.eval_expression(left, Rc::clone(&env))?)?;
                let rhs = self.value_to_number(&self.eval_expression(right, env)?)?;
                let cmp = if lhs < rhs {
                    -1.0
                } else if lhs > rhs {
                    1.0
                } else {
                    0.0
                };
                Ok(Value::Number(cmp))
            }
            "cmp" | "cmpi" => {
                let lhs =
                    self.value_to_operator_string(&self.eval_expression(left, Rc::clone(&env))?)?;
                let rhs = self.value_to_operator_string(&self.eval_expression(right, env)?)?;
                let (lhs, rhs) = if operator == "cmpi" {
                    (lhs.to_lowercase(), rhs.to_lowercase())
                } else {
                    (lhs, rhs)
                };
                let cmp = if lhs < rhs {
                    -1.0
                } else if lhs > rhs {
                    1.0
                } else {
                    0.0
                };
                Ok(Value::Number(cmp))
            }
            "&" | "|" | "^" => {
                let lhs = self.eval_expression(left, Rc::clone(&env))?;
                let rhs = self.eval_expression(right, env)?;
                self.eval_bitwise(operator, lhs, rhs)
            }
            "instanceof" => {
                let lhs = self.eval_expression(left, Rc::clone(&env))?;
                let rhs = self.eval_expression(right, env)?;
                Ok(Value::Boolean(self.value_instanceof(&lhs, &rhs)))
            }
            "does" => {
                let lhs = self.eval_expression(left, Rc::clone(&env))?;
                let rhs = self.eval_expression(right, env)?;
                Ok(Value::Boolean(self.value_does_trait(&lhs, &rhs)))
            }
            "can" => {
                let lhs = self.eval_expression(left, Rc::clone(&env))?;
                let member = self.member_name_operand(right, env)?;
                Ok(Value::Boolean(self.value_can_method(&lhs, &member)))
            }
            "@" | "@@" | "@?" => self.eval_path_operator(operator, left, right, env),
            "in" | "∈" | "∉" => {
                let needle = self.eval_expression(left, Rc::clone(&env))?;
                let haystack = self.eval_expression(right, env)?;
                let contains = match &haystack {
                    Value::Object(object) => {
                        self.object_builtin_collection_contains(object, &needle)
                    }
                    _ => collection_contains(&haystack, &needle),
                };
                if operator == "∉" {
                    Ok(Value::Boolean(!contains))
                } else {
                    Ok(Value::Boolean(contains))
                }
            }
            "union" | "⋃" => {
                let lhs = self.eval_expression(left, Rc::clone(&env))?;
                let rhs = self.eval_expression(right, env)?;
                collection_union(lhs, rhs)
            }
            "intersection" | "⋂" => {
                let lhs = self.eval_expression(left, Rc::clone(&env))?;
                let rhs = self.eval_expression(right, env)?;
                collection_intersection(lhs, rhs)
            }
            "\\" | "∖" => {
                let lhs = self.eval_expression(left, Rc::clone(&env))?;
                let rhs = self.eval_expression(right, env)?;
                collection_difference(lhs, rhs)
            }
            "subsetof" | "⊂" => {
                let lhs = self.eval_expression(left, Rc::clone(&env))?;
                let rhs = self.eval_expression(right, env)?;
                Ok(Value::Boolean(collection_subset(&lhs, &rhs)?))
            }
            "supersetof" | "⊃" => {
                let lhs = self.eval_expression(left, Rc::clone(&env))?;
                let rhs = self.eval_expression(right, env)?;
                Ok(Value::Boolean(collection_subset(&rhs, &lhs)?))
            }
            "equivalentof" | "⊂⊃" => {
                let lhs = self.eval_expression(left, Rc::clone(&env))?;
                let rhs = self.eval_expression(right, env)?;
                Ok(Value::Boolean(
                    collection_subset(&lhs, &rhs)? && collection_subset(&rhs, &lhs)?,
                ))
            }
            other => Err(ZuzuRustError::runtime(format!(
                "unsupported binary operator '{}'",
                other
            ))),
        }
    }

    fn assign(
        &self,
        operator: &str,
        left: &Expression,
        right: Value,
        weak_write: bool,
        env: Rc<Environment>,
    ) -> Result<Value> {
        if let Expression::Binary {
            operator: path_operator,
            left: path_left,
            right: path_right,
            ..
        } = left
        {
            if is_path_operator(path_operator) {
                return self.assign_path_target(
                    path_operator,
                    path_left,
                    path_right,
                    operator,
                    right,
                    weak_write,
                    env,
                );
            }
        }
        match operator {
            ":=" => self.assign_lvalue(left, right, weak_write, env),
            "?:=" => {
                let current = self.eval_expression(left, Rc::clone(&env))?;
                if matches!(current, Value::Null) {
                    self.assign_lvalue(left, right, false, env)
                } else {
                    Ok(current)
                }
            }
            "+=" => {
                let current =
                    self.value_to_number(&self.eval_expression(left, Rc::clone(&env))?)?;
                self.assign_lvalue(
                    left,
                    Value::Number(current + self.value_to_number(&right)?),
                    false,
                    env,
                )
            }
            "-=" => {
                let current =
                    self.value_to_number(&self.eval_expression(left, Rc::clone(&env))?)?;
                self.assign_lvalue(
                    left,
                    Value::Number(current - self.value_to_number(&right)?),
                    false,
                    env,
                )
            }
            "*=" | "×=" => {
                let current =
                    self.value_to_number(&self.eval_expression(left, Rc::clone(&env))?)?;
                self.assign_lvalue(
                    left,
                    Value::Number(current * self.value_to_number(&right)?),
                    false,
                    env,
                )
            }
            "/=" | "÷=" => {
                let current =
                    self.value_to_number(&self.eval_expression(left, Rc::clone(&env))?)?;
                self.assign_lvalue(
                    left,
                    Value::Number(current / self.value_to_number(&right)?),
                    false,
                    env,
                )
            }
            "**=" => {
                let current =
                    self.value_to_number(&self.eval_expression(left, Rc::clone(&env))?)?;
                self.assign_lvalue(
                    left,
                    Value::Number(current.powf(self.value_to_number(&right)?)),
                    false,
                    env,
                )
            }
            "_=" => {
                let current =
                    self.value_to_operator_string(&self.eval_expression(left, Rc::clone(&env))?)?;
                self.assign_lvalue(
                    left,
                    Value::String(current + &self.value_to_operator_string(&right)?),
                    false,
                    env,
                )
            }
            other => Err(ZuzuRustError::runtime(format!(
                "unsupported assignment operator '{}'",
                other
            ))),
        }
    }

    fn assign_lvalue(
        &self,
        target: &Expression,
        value: Value,
        weak_write: bool,
        env: Rc<Environment>,
    ) -> Result<Value> {
        match target {
            Expression::Binary {
                operator,
                left,
                right,
                ..
            } if is_path_operator(operator) => {
                self.assign_path_target(operator, left, right, ":=", value, weak_write, env)
            }
            Expression::Identifier {
                name,
                binding_depth,
                ..
            } => match env.get_resolved(*binding_depth, name) {
                Ok(Value::AliasRef(reference)) => {
                    let _ = self.assign_reference_with_weak_write(
                        reference,
                        value.clone(),
                        weak_write,
                    )?;
                    Ok(value)
                }
                Ok(Value::Ref(reference)) => {
                    let _ = self.assign_reference_with_weak_write(
                        reference,
                        value.clone(),
                        weak_write,
                    )?;
                    Ok(value)
                }
                Ok(_) => {
                    env.assign_resolved_with_weak_write(
                        *binding_depth,
                        name,
                        value.clone(),
                        weak_write,
                    )?;
                    Ok(value)
                }
                Err(err) => Err(err),
            },
            Expression::MemberAccess { object, member, .. } => {
                let object_value = self.eval_expression(object, Rc::clone(&env))?;
                match self.deref_value(&object_value)? {
                    Value::Object(object) => {
                        self.object_store_slot(&object, member, value.clone(), weak_write)?;
                        Ok(value)
                    }
                    other => Err(ZuzuRustError::runtime(format!(
                        "member assignment requires an object value, got {}",
                        self.typeof_name(&other)
                    ))),
                }
            }
            Expression::Index { object, index, .. } => {
                self.assign_index_target(object, index, value, weak_write, env)
            }
            Expression::DictAccess { object, key, .. } => {
                self.assign_dict_target(object, key, value, weak_write, env)
            }
            Expression::Slice {
                object, start, end, ..
            } => self.assign_slice_target(object, start.as_deref(), end.as_deref(), value, env),
            _ => Err(ZuzuRustError::runtime("unsupported assignment target")),
        }
    }

    fn update_lvalue<F>(
        &self,
        target: &Expression,
        env: Rc<Environment>,
        update: F,
    ) -> Result<Value>
    where
        F: FnOnce(Value) -> Result<Value>,
    {
        match target {
            Expression::Identifier {
                name,
                binding_depth,
                ..
            } => {
                let current = env.get_resolved(*binding_depth, name)?;
                let reference = match &current {
                    Value::Ref(reference) | Value::AliasRef(reference) => {
                        Some(Rc::clone(reference))
                    }
                    _ => None,
                };
                let current_value = if let Some(reference) = &reference {
                    self.deref_value(&Value::AliasRef(Rc::clone(reference)))?
                } else {
                    current
                };
                let updated = update(current_value)?;
                if let Some(reference) = reference {
                    let _ = self.assign_reference(reference, updated.clone())?;
                } else {
                    env.assign_resolved(*binding_depth, name, updated.clone())?;
                }
                Ok(updated)
            }
            Expression::MemberAccess { object, member, .. } => {
                let current = self.eval_expression(target, Rc::clone(&env))?;
                let updated = update(current)?;
                let object_value = self.eval_expression(object, Rc::clone(&env))?;
                match self.deref_value(&object_value)? {
                    Value::Object(object) => {
                        self.object_store_slot(&object, member, updated.clone(), false)?;
                        Ok(updated)
                    }
                    other => Err(ZuzuRustError::runtime(format!(
                        "member update requires an object value, got {}",
                        self.typeof_name(&other)
                    ))),
                }
            }
            Expression::Index { object, index, .. } => {
                let current = self.eval_expression(target, Rc::clone(&env))?;
                let updated = update(current)?;
                self.assign_index_target(object, index, updated, false, env)
            }
            Expression::DictAccess { object, key, .. } => {
                let current = self.eval_expression(target, Rc::clone(&env))?;
                let updated = update(current)?;
                self.assign_dict_target(object, key, updated, false, env)
            }
            Expression::Slice {
                object, start, end, ..
            } => {
                let current = self.eval_expression(target, Rc::clone(&env))?;
                let updated = update(current)?;
                self.assign_slice_target(object, start.as_deref(), end.as_deref(), updated, env)
            }
            _ => Err(ZuzuRustError::runtime("unsupported update target")),
        }
    }

    fn eval_path_operator(
        &self,
        operator: &str,
        left: &Expression,
        right: &Expression,
        env: Rc<Environment>,
    ) -> Result<Value> {
        let haystack = self.eval_path_haystack_argument(left, Rc::clone(&env))?;
        let path = self.eval_expression(right, Rc::clone(&env))?;
        let mut path_object = self.resolve_path_operand(path)?;
        match operator {
            "@" => self.call_method(&mut path_object, "first", &[haystack, Value::Null]),
            "@@" => self.call_method(&mut path_object, "query", &[haystack]),
            "@?" => {
                let value = self.call_method(&mut path_object, "exists", &[haystack])?;
                Ok(Value::Boolean(value.is_truthy()))
            }
            _ => Err(ZuzuRustError::runtime(format!(
                "unsupported path operator '{}'",
                operator
            ))),
        }
    }

    fn eval_path_ref(&self, argument: &Expression, env: Rc<Environment>) -> Result<Value> {
        let Expression::Binary {
            operator,
            left,
            right,
            ..
        } = argument
        else {
            return Err(ZuzuRustError::runtime("expected path expression"));
        };
        let path = self.eval_expression(right, Rc::clone(&env))?;
        let path_object = self.resolve_path_operand(path)?;
        let haystack = self.eval_path_haystack_argument(left, Rc::clone(&env))?;
        let mut path_object = path_object;
        match operator.as_str() {
            "@" => self.call_method(&mut path_object, "ref_first", &[haystack]),
            "@@" => self.call_method(&mut path_object, "ref_all", &[haystack]),
            "@?" => self.call_method(&mut path_object, "ref_maybe", &[haystack]),
            _ => Err(ZuzuRustError::runtime(format!(
                "unsupported path operator '{}'",
                operator
            ))),
        }
    }

    fn eval_path_update(
        &self,
        argument: &Expression,
        env: Rc<Environment>,
        delta: f64,
        prefix: bool,
    ) -> Result<Value> {
        let Expression::Binary {
            operator,
            left,
            right,
            ..
        } = argument
        else {
            return Err(ZuzuRustError::runtime("expected path expression"));
        };
        let path = self.eval_expression(right, Rc::clone(&env))?;
        let path_object = self.resolve_path_operand(path)?;
        let haystack = self.eval_expression(left, Rc::clone(&env))?;
        let mut path_object = path_object;
        match operator.as_str() {
            "@" => {
                let current =
                    self.call_method(&mut path_object, "first", &[haystack.clone(), Value::Null])?;
                let updated = Value::Number(self.value_to_number(&current)? + delta);
                self.call_method(
                    &mut path_object,
                    "assign_first",
                    &[haystack, updated.clone(), Value::String(":=".to_owned())],
                )?;
                if prefix {
                    Ok(updated)
                } else {
                    Ok(current)
                }
            }
            "@@" => {
                let current = self.call_method(&mut path_object, "query", &[haystack.clone()])?;
                let Value::Array(values) = current.clone() else {
                    return Err(ZuzuRustError::runtime("@@ query did not return an Array"));
                };
                let updated_values = values
                    .iter()
                    .map(|value| Ok(Value::Number(self.value_to_number(value)? + delta)))
                    .collect::<Result<Vec<_>>>()?;
                let op = if delta >= 0.0 { "+=" } else { "-=" };
                self.call_method(
                    &mut path_object,
                    "assign_all",
                    &[
                        haystack,
                        Value::Number(delta.abs()),
                        Value::String(op.to_owned()),
                    ],
                )?;
                if prefix {
                    Ok(Value::Array(updated_values))
                } else {
                    Ok(Value::Array(values))
                }
            }
            "@?" => {
                let exists = self.call_method(&mut path_object, "exists", &[haystack.clone()])?;
                let matched = exists.is_truthy();
                if matched {
                    let op = if delta >= 0.0 { "+=" } else { "-=" };
                    self.call_method(
                        &mut path_object,
                        "assign_maybe",
                        &[
                            haystack,
                            Value::Number(delta.abs()),
                            Value::String(op.to_owned()),
                        ],
                    )?;
                }
                Ok(Value::Boolean(matched))
            }
            _ => Err(ZuzuRustError::runtime(format!(
                "unsupported path operator '{}'",
                operator
            ))),
        }
    }

    fn assign_path_target(
        &self,
        path_operator: &str,
        path_left: &Expression,
        path_right: &Expression,
        assignment_operator: &str,
        value: Value,
        weak_write: bool,
        env: Rc<Environment>,
    ) -> Result<Value> {
        let path = self.eval_expression(path_right, Rc::clone(&env))?;
        let path_object = self.resolve_path_operand(path)?;
        let haystack = self.eval_path_haystack_argument(path_left, Rc::clone(&env))?;
        let mut path_object = path_object;
        let method = match path_operator {
            "@" => "assign_first",
            "@@" => "assign_all",
            "@?" => "assign_maybe",
            _ => {
                return Err(ZuzuRustError::runtime(format!(
                    "unsupported path operator '{}'",
                    path_operator
                )))
            }
        };
        self.call_method(
            &mut path_object,
            method,
            &[
                haystack,
                value,
                Value::String(assignment_operator.to_owned()),
                Value::Boolean(weak_write),
            ],
        )
    }

    fn build_path_regex_payload(
        &self,
        replacement: &Expression,
        env: Rc<Environment>,
    ) -> Result<Value> {
        match replacement {
            Expression::Binary {
                operator,
                left,
                right,
                ..
            } if operator == "->" => {
                let regex_value = self.eval_expression(left, Rc::clone(&env))?;
                let (pattern, flags) = self.coerce_regex_operand(&regex_value).map_err(|_| {
                    ZuzuRustError::runtime("~= expects a regexp -> replacement expression")
                })?;
                let callback = FunctionValue {
                    name: None,
                    params: vec![Parameter {
                        line: right.line(),
                        source_file: right.source_file().map(str::to_owned),
                        declared_type: None,
                        name: "m".to_owned(),
                        optional: false,
                        variadic: false,
                        default_value: None,
                    }],
                    return_type: None,
                    body: FunctionBody::Expression((**right).clone()),
                    env,
                    is_async: false,
                    current_method: None,
                };
                Ok(Value::Array(vec![
                    Value::Regex(pattern, flags),
                    Value::Function(Rc::new(callback)),
                ]))
            }
            _ => Err(ZuzuRustError::runtime(
                "~= expects a regexp -> replacement expression",
            )),
        }
    }

    fn resolve_path_operand(&self, path: Value) -> Result<Value> {
        match path {
            Value::Object(_) => Ok(path),
            Value::String(text) => {
                let class_value = match self.get_special_prop("paths") {
                    Value::Null => self
                        .load_module_exports("std/path/zz")?
                        .get("ZZPath")
                        .cloned()
                        .ok_or_else(|| {
                            ZuzuRustError::runtime("module 'std/path/zz' does not export 'ZZPath'")
                        })?,
                    value => value,
                };
                self.call_value(
                    class_value,
                    Vec::new(),
                    vec![("path".to_owned(), Value::String(text))],
                )
            }
            other => Err(ZuzuRustError::runtime(format!(
                "path operand must be String or path object, got {}",
                other.type_name()
            ))),
        }
    }

    pub(in crate::runtime) fn task_resolved(&self, value: Value) -> Value {
        Value::Task(Rc::new(RefCell::new(TaskState {
            status: "fulfilled".to_owned(),
            kind: TaskKind::Resolved,
            outcome: Some(TaskOutcome::Fulfilled(value)),
        })))
    }

    pub(in crate::runtime) fn task_rejected(&self, message: impl Into<String>) -> Value {
        Value::Task(Rc::new(RefCell::new(TaskState {
            status: "rejected".to_owned(),
            kind: TaskKind::Resolved,
            outcome: Some(TaskOutcome::Rejected(message.into())),
        })))
    }

    pub(in crate::runtime) fn task_sleep(&self, seconds: f64) -> Value {
        let seconds = if seconds.is_finite() && seconds > 0.0 {
            seconds
        } else {
            0.0
        };
        Value::Task(Rc::new(RefCell::new(TaskState {
            status: "sleeping".to_owned(),
            kind: TaskKind::Sleep {
                deadline: Instant::now() + Duration::from_secs_f64(seconds),
            },
            outcome: None,
        })))
    }

    pub(in crate::runtime) fn task_yield(&self) -> Value {
        if self.check_worker_cancelled().is_err() {
            return self.task_rejected("CancelledException: worker cancelled");
        }
        self.task_sleep(0.0)
    }

    pub(in crate::runtime) fn task_all(&self, tasks: Vec<Value>) -> Value {
        let task = Rc::new(RefCell::new(TaskState {
            status: "waiting".to_owned(),
            kind: TaskKind::All { tasks },
            outcome: None,
        }));
        self.schedule_task_driver(Rc::clone(&task));
        Value::Task(task)
    }

    pub(in crate::runtime) fn task_race(&self, tasks: Vec<Value>) -> Value {
        let task = Rc::new(RefCell::new(TaskState {
            status: "waiting".to_owned(),
            kind: TaskKind::Race { tasks },
            outcome: None,
        }));
        self.schedule_task_driver(Rc::clone(&task));
        Value::Task(task)
    }

    pub(in crate::runtime) fn task_timeout(&self, seconds: f64, task: Value) -> Value {
        let seconds = if seconds.is_finite() && seconds > 0.0 {
            seconds
        } else {
            0.0
        };
        let task = Rc::new(RefCell::new(TaskState {
            status: "waiting".to_owned(),
            kind: TaskKind::Timeout {
                deadline: Instant::now() + Duration::from_secs_f64(seconds),
                seconds,
                task,
            },
            outcome: None,
        }));
        self.schedule_task_driver(Rc::clone(&task));
        Value::Task(task)
    }

    pub(in crate::runtime) fn task_channel_recv(
        &self,
        channel: Rc<RefCell<ChannelState>>,
    ) -> Value {
        Value::Task(Rc::new(RefCell::new(TaskState {
            status: "waiting".to_owned(),
            kind: TaskKind::ChannelRecv { channel },
            outcome: None,
        })))
    }

    pub(in crate::runtime) fn task_native_async<F>(
        &self,
        future: F,
        cancel_requested: Option<Rc<Cell<bool>>>,
    ) -> Value
    where
        F: Future<Output = Result<Value>> + 'static,
    {
        let task = Rc::new(RefCell::new(TaskState {
            status: "pending".to_owned(),
            kind: TaskKind::NativeAsync { cancel_requested },
            outcome: None,
        }));
        self.async_executor
            .spawn_local(drive_native_async_task(Rc::clone(&task), future));
        Value::Task(task)
    }

    pub(in crate::runtime) fn register_worker_endpoint(
        &self,
        endpoint: WorkerEndpointState,
    ) -> usize {
        let id = {
            let mut next_id = self.next_worker_endpoint_id.borrow_mut();
            let id = *next_id;
            *next_id += 1;
            id
        };
        self.worker_endpoints.borrow_mut().insert(id, endpoint);
        id
    }

    fn task_from_function(
        &self,
        function: Rc<FunctionValue>,
        args: Vec<Value>,
        named_args: Vec<(String, Value)>,
    ) -> Value {
        Value::Task(Rc::new(RefCell::new(TaskState {
            status: "pending".to_owned(),
            kind: TaskKind::Function {
                function,
                args,
                named_args,
                started: false,
            },
            outcome: None,
        })))
    }

    pub(in crate::runtime) fn await_if_task(&self, value: Value) -> Result<Value> {
        match value {
            Value::Task(task) => self.await_task(task),
            other => Ok(other),
        }
    }

    fn await_value(&self, value: Value) -> Result<Value> {
        match value {
            Value::Task(task) => self.await_task(task),
            other => Err(ZuzuRustError::runtime(format!(
                "await block must return a Task, got {}",
                other.type_name()
            ))),
        }
    }

    fn await_task(&self, task: Rc<RefCell<TaskState>>) -> Result<Value> {
        loop {
            self.check_worker_cancelled()?;
            if self.poll_task(&task)? {
                return self.task_result(&task);
            }
            self.check_worker_cancelled()?;
            self.wait_for_task_progress(&task);
        }
    }

    fn schedule_task_driver(&self, task: Rc<RefCell<TaskState>>) {
        let runtime = Rc::downgrade(&self.inner);
        self.async_executor
            .spawn_local(drive_scheduled_task(runtime, task));
    }

    fn wait_for_task_progress(&self, task: &Rc<RefCell<TaskState>>) {
        if let Some(deadline) = self.next_scheduler_deadline(Some(task)) {
            let deadline = if self.worker_cancel_requested.is_some() {
                deadline.min(Instant::now() + Duration::from_millis(10))
            } else {
                deadline
            };
            self.async_executor.sleep_until(deadline);
        } else {
            self.async_executor.sleep_for(Duration::from_millis(1));
        }
    }

    fn next_scheduler_deadline(&self, target: Option<&Rc<RefCell<TaskState>>>) -> Option<Instant> {
        let mut deadline = target.and_then(|task| self.next_task_deadline(task));
        for task in self.background_tasks.borrow().iter() {
            if target
                .map(|target| Rc::ptr_eq(target, task))
                .unwrap_or(false)
            {
                continue;
            }
            deadline = earliest_deadline(deadline, self.next_task_deadline(task));
        }
        deadline
    }

    fn next_task_deadline(&self, task: &Rc<RefCell<TaskState>>) -> Option<Instant> {
        enum Children {
            None,
            One(Rc<RefCell<TaskState>>),
            Many(Vec<Value>),
        }

        let (deadline, children) = {
            let state = task.borrow();
            match &state.kind {
                TaskKind::Sleep { deadline } => (Some(*deadline), Children::None),
                TaskKind::Timeout {
                    deadline,
                    task: inner,
                    ..
                } => (Some(*deadline), Children::Many(vec![inner.clone()])),
                TaskKind::FunctionWaiting { awaited, .. }
                | TaskKind::SpawnWaiting { awaited, .. } => {
                    (None, Children::One(Rc::clone(awaited)))
                }
                TaskKind::All { tasks } | TaskKind::Race { tasks } => {
                    (None, Children::Many(tasks.clone()))
                }
                TaskKind::Function { .. } | TaskKind::Spawn { .. } => {
                    (Some(Instant::now()), Children::None)
                }
                TaskKind::Resolved
                | TaskKind::ChannelRecv { .. }
                | TaskKind::NativeAsync { .. } => (None, Children::None),
            }
        };

        match children {
            Children::None => deadline,
            Children::One(child) => earliest_deadline(deadline, self.next_task_deadline(&child)),
            Children::Many(values) => values.into_iter().fold(deadline, |current, value| {
                let Value::Task(child) = value else {
                    return current;
                };
                earliest_deadline(current, self.next_task_deadline(&child))
            }),
        }
    }

    fn task_result(&self, task: &Rc<RefCell<TaskState>>) -> Result<Value> {
        match task.borrow().outcome.clone() {
            Some(TaskOutcome::Fulfilled(value)) => Ok(value),
            Some(TaskOutcome::Rejected(message)) => Err(ZuzuRustError::thrown(message)),
            Some(TaskOutcome::Cancelled(reason)) => Err(ZuzuRustError::thrown(format!(
                "CancelledException: {reason}"
            ))),
            None => Err(ZuzuRustError::runtime("task is not complete")),
        }
    }

    fn task_outcome_value(&self, task: &Rc<RefCell<TaskState>>) -> Option<TaskOutcome> {
        task.borrow().outcome.clone()
    }

    fn poll_task(&self, task: &Rc<RefCell<TaskState>>) -> Result<bool> {
        if task.borrow().outcome.is_some() {
            return Ok(true);
        }

        let task_id = Rc::as_ptr(task) as usize;
        {
            let mut polling_tasks = self.polling_tasks.borrow_mut();
            if polling_tasks.contains(&task_id) {
                return Ok(false);
            }
            polling_tasks.push(task_id);
        }
        let _polling_guard = PollTaskGuard {
            stack: &self.polling_tasks,
            task_id,
        };

        let mut complete = None;
        {
            let mut state = task.borrow_mut();
            match &mut state.kind {
                TaskKind::Resolved => {}
                TaskKind::Sleep { deadline } => {
                    if Instant::now() >= *deadline {
                        complete = Some(TaskOutcome::Fulfilled(Value::Null));
                    }
                }
                TaskKind::Function {
                    function,
                    args,
                    named_args,
                    started,
                } => {
                    *started = true;
                    let function = Rc::clone(function);
                    let args = args.clone();
                    let named_args = named_args.clone();
                    state.status = "running".to_owned();
                    drop(state);
                    let result = self.start_async_function_task(&function, args, named_args);
                    let mut state = task.borrow_mut();
                    match result {
                        Ok(AsyncPoll::Complete(value)) => {
                            complete = Some(TaskOutcome::Fulfilled(value));
                        }
                        Ok(AsyncPoll::Await {
                            awaited,
                            frames,
                            disposition,
                        }) => {
                            state.status = "waiting".to_owned();
                            state.kind = TaskKind::FunctionWaiting {
                                awaited,
                                frames,
                                disposition,
                            };
                            return Ok(false);
                        }
                        Err(ZuzuRustError::Thrown { value, .. }) => {
                            complete = Some(TaskOutcome::Rejected(value));
                        }
                        Err(err) => {
                            complete = Some(TaskOutcome::Rejected(err.to_string()));
                        }
                    }
                    state.status = "running".to_owned();
                }
                TaskKind::FunctionWaiting {
                    awaited,
                    frames,
                    disposition,
                } => {
                    let awaited = Rc::clone(awaited);
                    let frames = std::mem::take(frames);
                    let disposition = disposition.clone();
                    drop(state);
                    if !self.poll_task(&awaited)? {
                        let mut state = task.borrow_mut();
                        state.kind = TaskKind::FunctionWaiting {
                            awaited,
                            frames,
                            disposition,
                        };
                        return Ok(false);
                    }
                    match self.task_result(&awaited) {
                        Ok(awaited_value) => {
                            if matches!(disposition, AwaitDisposition::Return) {
                                let value = self.finish_async_frames(&frames, awaited_value);
                                complete = Some(match value {
                                    Ok(value) => TaskOutcome::Fulfilled(value),
                                    Err(ZuzuRustError::Thrown { value, .. }) => {
                                        TaskOutcome::Rejected(value)
                                    }
                                    Err(err) => TaskOutcome::Rejected(err.to_string()),
                                });
                            } else {
                                let result = self.poll_async_frames(frames);
                                let mut state = task.borrow_mut();
                                match result {
                                    Ok(AsyncPoll::Complete(value)) => {
                                        complete = Some(TaskOutcome::Fulfilled(value));
                                    }
                                    Ok(AsyncPoll::Await {
                                        awaited,
                                        frames,
                                        disposition,
                                    }) => {
                                        state.status = "waiting".to_owned();
                                        state.kind = TaskKind::FunctionWaiting {
                                            awaited,
                                            frames,
                                            disposition,
                                        };
                                        return Ok(false);
                                    }
                                    Err(ZuzuRustError::Thrown { value, .. }) => {
                                        complete = Some(TaskOutcome::Rejected(value));
                                    }
                                    Err(err) => {
                                        complete = Some(TaskOutcome::Rejected(err.to_string()));
                                    }
                                }
                                state.status = "running".to_owned();
                            }
                        }
                        Err(ZuzuRustError::Thrown { value, .. }) => {
                            self.cleanup_async_frames(&frames)?;
                            complete = Some(TaskOutcome::Rejected(value));
                        }
                        Err(err) => {
                            self.cleanup_async_frames(&frames)?;
                            complete = Some(TaskOutcome::Rejected(err.to_string()));
                        }
                    }
                }
                TaskKind::Spawn { body, env, started } => {
                    *started = true;
                    let body = body.clone();
                    let env = Rc::clone(env);
                    state.status = "running".to_owned();
                    drop(state);
                    let needs_lexical_scope = body.needs_lexical_scope;
                    let body_env = if needs_lexical_scope {
                        Rc::new(Environment::new(Some(env)))
                    } else {
                        env
                    };
                    let result = self.poll_async_frames(vec![AsyncFrame::Do {
                        statements: body.statements,
                        index: 0,
                        env: body_env,
                        cleanup_env: needs_lexical_scope,
                        last: Value::Null,
                    }]);
                    let mut state = task.borrow_mut();
                    match result {
                        Ok(AsyncPoll::Complete(value)) => {
                            complete = Some(TaskOutcome::Fulfilled(value));
                        }
                        Ok(AsyncPoll::Await {
                            awaited,
                            frames,
                            disposition,
                        }) => {
                            state.status = "waiting".to_owned();
                            state.kind = TaskKind::SpawnWaiting {
                                awaited,
                                frames,
                                disposition,
                            };
                            return Ok(false);
                        }
                        Err(ZuzuRustError::Thrown { value, .. }) => {
                            complete = Some(TaskOutcome::Rejected(value));
                        }
                        Err(err) => {
                            complete = Some(TaskOutcome::Rejected(err.to_string()));
                        }
                    }
                    state.status = "running".to_owned();
                }
                TaskKind::SpawnWaiting {
                    awaited,
                    frames,
                    disposition,
                } => {
                    let awaited = Rc::clone(awaited);
                    let mut frames = std::mem::take(frames);
                    let disposition = disposition.clone();
                    drop(state);
                    if !self.poll_task(&awaited)? {
                        let mut state = task.borrow_mut();
                        state.kind = TaskKind::SpawnWaiting {
                            awaited,
                            frames,
                            disposition,
                        };
                        return Ok(false);
                    }
                    match self.task_result(&awaited) {
                        Ok(awaited_value) => {
                            if matches!(disposition, AwaitDisposition::StoreLast) {
                                if let Some(frame) = frames.last_mut() {
                                    frame.set_last(awaited_value);
                                }
                            }
                            let result = self.poll_async_frames(frames);
                            let mut state = task.borrow_mut();
                            match result {
                                Ok(AsyncPoll::Complete(value)) => {
                                    complete = Some(TaskOutcome::Fulfilled(value));
                                }
                                Ok(AsyncPoll::Await {
                                    awaited,
                                    frames,
                                    disposition,
                                }) => {
                                    state.status = "waiting".to_owned();
                                    state.kind = TaskKind::SpawnWaiting {
                                        awaited,
                                        frames,
                                        disposition,
                                    };
                                    return Ok(false);
                                }
                                Err(ZuzuRustError::Thrown { value, .. }) => {
                                    complete = Some(TaskOutcome::Rejected(value));
                                }
                                Err(err) => {
                                    complete = Some(TaskOutcome::Rejected(err.to_string()));
                                }
                            }
                            state.status = "running".to_owned();
                        }
                        Err(ZuzuRustError::Thrown { value, .. }) => {
                            self.cleanup_async_frames(&frames)?;
                            complete = Some(TaskOutcome::Rejected(value));
                        }
                        Err(err) => {
                            self.cleanup_async_frames(&frames)?;
                            complete = Some(TaskOutcome::Rejected(err.to_string()));
                        }
                    }
                }
                TaskKind::All { tasks } => {
                    let tasks = tasks.clone();
                    drop(state);
                    let mut results = Vec::new();
                    let mut all_done = true;
                    let mut failed_child: Option<Rc<RefCell<TaskState>>> = None;
                    for value in &tasks {
                        match value {
                            Value::Task(child) => {
                                if !self.poll_task(child)? {
                                    all_done = false;
                                    results.push(Value::Null);
                                    continue;
                                }
                                match self.task_outcome_value(child) {
                                    Some(TaskOutcome::Fulfilled(value)) => results.push(value),
                                    Some(TaskOutcome::Rejected(message)) => {
                                        complete = Some(TaskOutcome::Rejected(message));
                                        failed_child = Some(Rc::clone(child));
                                        break;
                                    }
                                    Some(TaskOutcome::Cancelled(reason)) => {
                                        complete = Some(TaskOutcome::Cancelled(reason));
                                        failed_child = Some(Rc::clone(child));
                                        break;
                                    }
                                    None => all_done = false,
                                }
                            }
                            other => results.push(other.clone()),
                        }
                    }
                    if complete.is_some() {
                        for value in tasks {
                            if let Value::Task(child) = value {
                                if failed_child
                                    .as_ref()
                                    .map(|failed| Rc::ptr_eq(failed, &child))
                                    .unwrap_or(false)
                                {
                                    continue;
                                }
                                self.cancel_task(
                                    &child,
                                    Value::String("Task cancelled".to_owned()),
                                );
                            }
                        }
                    }
                    if complete.is_none() && all_done {
                        complete = Some(TaskOutcome::Fulfilled(Value::Array(results)));
                    }
                }
                TaskKind::Race { tasks } => {
                    let tasks = tasks.clone();
                    drop(state);
                    let mut winner: Option<Rc<RefCell<TaskState>>> = None;
                    for value in &tasks {
                        match value {
                            Value::Task(child) => {
                                if self.poll_task(&child)? {
                                    winner = Some(Rc::clone(&child));
                                    complete = self.task_outcome_value(&child);
                                    break;
                                }
                            }
                            other => {
                                complete = Some(TaskOutcome::Fulfilled(other.clone()));
                                break;
                            }
                        }
                    }
                    if complete.is_some() {
                        for value in tasks {
                            if let Value::Task(child) = value {
                                if winner
                                    .as_ref()
                                    .map(|winner| Rc::ptr_eq(winner, &child))
                                    .unwrap_or(false)
                                {
                                    continue;
                                }
                                self.cancel_task(
                                    &child,
                                    Value::String("race loser cancelled".to_owned()),
                                );
                            }
                        }
                    }
                }
                TaskKind::Timeout {
                    deadline,
                    seconds,
                    task: inner,
                } => {
                    let expired = Instant::now() >= *deadline;
                    let inner = inner.clone();
                    let message = format!("timeout after {seconds}s");
                    drop(state);
                    if expired {
                        if let Value::Task(child) = &inner {
                            self.cancel_task(child, Value::String(message.clone()));
                        }
                        complete = Some(TaskOutcome::Rejected(format!(
                            "TimeoutException: {message}"
                        )));
                    } else if let Value::Task(child) = inner {
                        if self.poll_task(&child)? {
                            complete = self.task_outcome_value(&child);
                        }
                    } else {
                        complete = Some(TaskOutcome::Fulfilled(inner));
                    }
                }
                TaskKind::ChannelRecv { channel } => {
                    let mut channel = channel.borrow_mut();
                    if !channel.messages.is_empty() {
                        complete = Some(TaskOutcome::Fulfilled(channel.messages.remove(0)));
                    } else if channel.closed {
                        complete = Some(TaskOutcome::Fulfilled(Value::Null));
                    }
                }
                TaskKind::NativeAsync { .. } => {}
            }
        }

        if let Some(outcome) = complete {
            let mut state = task.borrow_mut();
            state.status = match &outcome {
                TaskOutcome::Fulfilled(_) => "fulfilled",
                TaskOutcome::Rejected(_) => "rejected",
                TaskOutcome::Cancelled(_) => "cancelled",
            }
            .to_owned();
            state.outcome = Some(outcome);
            return Ok(true);
        }
        Ok(false)
    }

    fn cancel_task(&self, task: &Rc<RefCell<TaskState>>, reason: Value) {
        if task.borrow().outcome.is_some() {
            return;
        }
        let cancel_requested = {
            let state = task.borrow();
            match &state.kind {
                TaskKind::NativeAsync {
                    cancel_requested: Some(cancel_requested),
                } => Some(Rc::clone(cancel_requested)),
                _ => None,
            }
        };
        if let Some(cancel_requested) = cancel_requested {
            cancel_requested.set(true);
        }
        let children = child_tasks_of(task);
        let frames = async_frames_of(task);
        let reason = reason.render();
        {
            let mut state = task.borrow_mut();
            state.status = "cancelled".to_owned();
            state.outcome = Some(TaskOutcome::Cancelled(reason.clone()));
        }
        for child in children {
            self.cancel_task(&child, Value::String(reason.clone()));
        }
        if let Some(frames) = frames {
            let _ = self.cleanup_async_frames(&frames);
        }
    }

    fn cancel_background_tasks(&self, reason: Value) {
        let tasks = self.background_tasks.borrow().clone();
        for task in tasks {
            self.cancel_task(&task, reason.clone());
        }
    }

    fn call_value(
        &self,
        callee: Value,
        args: Vec<Value>,
        named_args: Vec<(String, Value)>,
    ) -> Result<Value> {
        match callee {
            value if value.is_weak_value() => {
                self.call_value(value.resolve_weak_value(), args, named_args)
            }
            Value::Shared(value) => self.call_value(value.borrow().clone(), args, named_args),
            Value::Function(function) => self.call_function(&function, args, named_args),
            Value::NativeFunction(name) => self.call_native_function(&name, args, named_args),
            Value::Method(method) => self.call_bound_method(&method, args, named_args),
            Value::Iterator(state) => {
                if !args.is_empty() || !named_args.is_empty() {
                    return Err(ZuzuRustError::runtime(
                        "iterator values do not take arguments",
                    ));
                }
                self.iterator_next(&state)
            }
            Value::Ref(reference) => self.call_ref(reference, args, named_args),
            Value::AliasRef(reference) => {
                let callee = self.deref_value(&Value::AliasRef(Rc::clone(&reference)))?;
                self.call_value(callee, args, named_args)
            }
            Value::Class(name) => self.construct_builtin_class(&name, args, named_args),
            Value::UserClass(class) => self.construct_user_class(class, args, named_args),
            _ => Err(ZuzuRustError::runtime(
                "attempted to call a non-function value",
            )),
        }
    }

    fn call_native_function(
        &self,
        name: &str,
        args: Vec<Value>,
        named_args: Vec<(String, Value)>,
    ) -> Result<Value> {
        if matches!(name, "dump" | "safe_to_dump" | "ref_id") {
            return stdlib::call_native_function(self, name, args, named_args);
        }
        let args = args
            .into_iter()
            .map(|value| self.normalize_value(value))
            .collect::<Result<Vec<_>>>()?;
        let named_args = named_args
            .into_iter()
            .map(|(key, value)| {
                let value = self.normalize_value(value)?;
                Ok((key, value))
            })
            .collect::<Result<Vec<_>>>()?;
        stdlib::call_native_function(self, name, args, named_args)
    }

    fn call_bound_method(
        &self,
        method: &Rc<MethodValue>,
        args: Vec<Value>,
        named_args: Vec<(String, Value)>,
    ) -> Result<Value> {
        let Some(receiver) = method.bound_receiver.clone() else {
            return Err(ZuzuRustError::runtime(
                "attempted to call an unbound method value",
            ));
        };
        let name = method.bound_name.as_deref().unwrap_or(&method.name);
        match receiver {
            Value::Object(object) => self.call_object_method(&object, name, &args, named_args),
            Value::UserClass(class) => {
                self.call_class_method(Rc::clone(&class), class, name, &args, named_args)
            }
            _ => Err(ZuzuRustError::runtime(
                "bound method receiver must be an Object or Class",
            )),
        }
    }

    fn invoke_method_expression(
        &self,
        object: &Expression,
        name: &str,
        arguments: &[CallArgument],
        env: Rc<Environment>,
    ) -> Result<Value> {
        let receiver_value = self.eval_expression(object, Rc::clone(&env))?;
        let (args, named_args) = self.eval_method_call_arguments(arguments, Rc::clone(&env))?;
        if let Expression::Identifier { name: binding, .. } = object {
            let mut receiver = env.get(binding)?;
            let should_write_back = !matches!(receiver, Value::Ref(_) | Value::AliasRef(_))
                && is_mutating_collection_method(name)
                && matches!(
                    receiver,
                    Value::Array(_)
                        | Value::Set(_)
                        | Value::Bag(_)
                        | Value::Dict(_)
                        | Value::PairList(_)
                );
            let result = self.call_method_named(&mut receiver, name, &args, named_args.clone())?;
            if binding == "self" {
                if let Value::Object(object) = &receiver {
                    self.refresh_method_field_bindings(&env, object);
                }
            }
            if should_write_back {
                env.assign(binding, receiver)?;
            }
            return Ok(result);
        }

        let mut receiver = receiver_value;
        self.call_method_named(&mut receiver, name, &args, named_args)
    }

    fn call_method(&self, receiver: &mut Value, name: &str, args: &[Value]) -> Result<Value> {
        self.call_method_named(receiver, name, args, Vec::new())
    }

    fn refresh_method_field_bindings(
        &self,
        env: &Rc<Environment>,
        object: &Rc<RefCell<ObjectValue>>,
    ) {
        let fields = object.borrow().fields.clone();
        for (name, value) in fields {
            if !self.is_ref_backed_composite(&value) {
                continue;
            }
            if matches!(
                env.get_optional(&name),
                Some(Value::Ref(_)) | Some(Value::AliasRef(_))
            ) {
                continue;
            }
            env.refresh_existing_binding(&name, value.into_shared_if_composite());
        }
    }

    fn deref_alias_method_args(
        &self,
        args: &[Value],
        named_args: &[(String, Value)],
    ) -> Result<(Vec<Value>, Vec<(String, Value)>)> {
        let args = args
            .iter()
            .cloned()
            .map(|value| self.normalize_value(value))
            .collect::<Result<Vec<_>>>()?;
        let named_args = named_args
            .iter()
            .cloned()
            .map(|(key, value)| {
                let value = self.normalize_value(value)?;
                Ok((key, value))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((args, named_args))
    }

    fn preserve_shared_method_args(
        &self,
        args: &[Value],
        named_args: &[(String, Value)],
    ) -> Result<(Vec<Value>, Vec<(String, Value)>)> {
        let args = args
            .iter()
            .cloned()
            .map(|value| match value {
                Value::AliasRef(_) => self.deref_value(&value),
                other => Ok(other),
            })
            .collect::<Result<Vec<_>>>()?;
        let named_args = named_args
            .iter()
            .cloned()
            .map(|(key, value)| match value {
                Value::AliasRef(_) => Ok((key, self.deref_value(&value)?)),
                other => Ok((key, other)),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((args, named_args))
    }

    fn call_method_named(
        &self,
        receiver: &mut Value,
        name: &str,
        args: &[Value],
        named_args: Vec<(String, Value)>,
    ) -> Result<Value> {
        if receiver.is_weak_value() {
            *receiver = receiver.resolve_weak_value();
        }
        if let Value::Ref(reference) | Value::AliasRef(reference) = receiver {
            let mut current = self.deref_value(&Value::AliasRef(Rc::clone(reference)))?;
            let result = self.call_method_named(&mut current, name, args, named_args.clone())?;
            if is_mutating_collection_method(name) && self.is_ref_backed_composite(&current) {
                let _ = self.assign_reference(Rc::clone(reference), current)?;
            }
            return Ok(result);
        }
        if let Value::Shared(shared) = receiver {
            let mut current = shared.borrow().clone();
            let result = self.call_method_named(&mut current, name, args, named_args.clone())?;
            if is_mutating_collection_method(name) && self.is_ref_backed_composite(&current) {
                *shared.borrow_mut() = current;
            }
            return Ok(result);
        }
        match receiver {
            Value::Array(values) => {
                let (args, named_args) = if is_mutating_collection_method(name) {
                    self.preserve_shared_method_args(args, &named_args)?
                } else {
                    self.deref_alias_method_args(args, &named_args)?
                };
                reject_named_args(name, &named_args)?;
                self.call_array_method(values, name, &args)
            }
            Value::Set(values) => {
                let (args, named_args) = if is_mutating_collection_method(name) {
                    self.preserve_shared_method_args(args, &named_args)?
                } else {
                    self.deref_alias_method_args(args, &named_args)?
                };
                reject_named_args(name, &named_args)?;
                self.call_set_method(values, name, &args)
            }
            Value::Bag(values) => {
                let (args, named_args) = if is_mutating_collection_method(name) {
                    self.preserve_shared_method_args(args, &named_args)?
                } else {
                    self.deref_alias_method_args(args, &named_args)?
                };
                reject_named_args(name, &named_args)?;
                self.call_bag_method(values, name, &args)
            }
            Value::Dict(values) => {
                let (args, named_args) = if is_mutating_collection_method(name) {
                    self.preserve_shared_method_args(args, &named_args)?
                } else {
                    self.deref_alias_method_args(args, &named_args)?
                };
                reject_named_args(name, &named_args)?;
                self.call_dict_method(values, name, &args)
            }
            Value::PairList(values) => {
                let (args, named_args) = if is_mutating_collection_method(name) {
                    self.preserve_shared_method_args(args, &named_args)?
                } else {
                    self.deref_alias_method_args(args, &named_args)?
                };
                reject_named_args(name, &named_args)?;
                self.call_pairlist_method(values, name, &args)
            }
            Value::Pair(key, value) => {
                let (args, named_args) = self.deref_alias_method_args(args, &named_args)?;
                reject_named_args(name, &named_args)?;
                self.call_pair_method(key, value, name, &args)
            }
            Value::Iterator(state) => {
                let (args, named_args) = self.deref_alias_method_args(args, &named_args)?;
                reject_named_args(name, &named_args)?;
                self.call_iterator_method(state, name, &args)
            }
            Value::Class(class_name) => {
                let (args, named_args) = self.deref_alias_method_args(args, &named_args)?;
                if let Some(result) = stdlib::call_builtin_class_method_named(
                    self,
                    class_name,
                    name,
                    &args,
                    &named_args,
                ) {
                    return result;
                }
                reject_named_args(name, &named_args)?;
                if let Some(result) =
                    stdlib::call_builtin_class_method(self, class_name, name, &args)
                {
                    result
                } else {
                    Err(ZuzuRustError::thrown(format!(
                        "unsupported method '{}' for {}",
                        name,
                        receiver.type_name()
                    )))
                }
            }
            Value::UserClass(class) => {
                self.call_class_method(Rc::clone(class), Rc::clone(class), name, args, named_args)
            }
            Value::Object(object) => self.call_object_method(object, name, args, named_args),
            Value::Task(task) => {
                let (args, named_args) = self.deref_alias_method_args(args, &named_args)?;
                reject_named_args(name, &named_args)?;
                self.call_task_method(task, name, &args)
            }
            Value::Channel(channel) => {
                let (args, named_args) = self.deref_alias_method_args(args, &named_args)?;
                reject_named_args(name, &named_args)?;
                self.call_channel_method(channel, name, &args)
            }
            Value::CancellationSource(source) => {
                let (args, named_args) = self.deref_alias_method_args(args, &named_args)?;
                reject_named_args(name, &named_args)?;
                self.call_cancellation_source_method(source, name, &args)
            }
            Value::CancellationToken(token) => {
                let (args, named_args) = self.deref_alias_method_args(args, &named_args)?;
                reject_named_args(name, &named_args)?;
                self.call_cancellation_token_method(token, name, &args)
            }
            Value::Null
            | Value::Boolean(_)
            | Value::Number(_)
            | Value::String(_)
            | Value::BinaryString(_)
            | Value::Regex(_, _) => {
                let (args, named_args) = self.deref_alias_method_args(args, &named_args)?;
                reject_named_args(name, &named_args)?;
                self.call_scalar_method(receiver, name, &args)
            }
            other => Err(ZuzuRustError::thrown(format!(
                "unsupported method '{}' for {}",
                name,
                other.type_name()
            ))),
        }
    }

    fn call_scalar_method(&self, receiver: &Value, name: &str, args: &[Value]) -> Result<Value> {
        require_arity(name, args, 0)?;
        match name {
            "primitive_value" | "value" => Ok(receiver.clone()),
            "length" | "count" => match receiver {
                Value::String(text) => Ok(Value::Number(text.chars().count() as f64)),
                Value::BinaryString(bytes) => Ok(Value::Number(bytes.len() as f64)),
                Value::Null => Ok(Value::Number(0.0)),
                _ => Err(ZuzuRustError::thrown(format!(
                    "unsupported method '{}' for {}",
                    name,
                    receiver.type_name()
                ))),
            },
            "string_value" => {
                if matches!(receiver, Value::Null) {
                    Ok(Value::Null)
                } else {
                    Ok(Value::String(self.render_value(receiver)?))
                }
            }
            "number_value" => {
                if matches!(receiver, Value::Null) {
                    Ok(Value::Null)
                } else {
                    match self.value_to_number(receiver) {
                        Ok(number) => Ok(Value::Number(number)),
                        Err(_) => Ok(Value::Null),
                    }
                }
            }
            "type" => Ok(Value::String(self.typeof_name(receiver))),
            _ => Err(ZuzuRustError::thrown(format!(
                "unsupported method '{}' for {}",
                name,
                receiver.type_name()
            ))),
        }
    }

    fn call_iterator_method(
        &self,
        state: &Rc<RefCell<IteratorState>>,
        name: &str,
        args: &[Value],
    ) -> Result<Value> {
        match name {
            "next" => {
                require_arity(name, args, 0)?;
                match self.iterator_next(state) {
                    Ok(value) => Ok(value),
                    Err(err) if err.is_iterator_exhausted() => Ok(Value::Null),
                    Err(err) => Err(err),
                }
            }
            _ => Err(ZuzuRustError::thrown(format!(
                "unsupported method '{}' for Iterator",
                name
            ))),
        }
    }

    fn call_task_method(
        &self,
        task: &Rc<RefCell<TaskState>>,
        name: &str,
        args: &[Value],
    ) -> Result<Value> {
        match name {
            "status" => {
                require_arity(name, args, 0)?;
                let _ = self.poll_task(task)?;
                Ok(Value::String(task.borrow().status.clone()))
            }
            "done" | "is_done" => {
                require_arity(name, args, 0)?;
                let _ = self.poll_task(task)?;
                Ok(Value::Boolean(task.borrow().outcome.is_some()))
            }
            "poll" => {
                require_arity(name, args, 0)?;
                Ok(Value::Boolean(self.poll_task(task)?))
            }
            "cancel" => {
                if args.len() > 1 {
                    return Err(ZuzuRustError::runtime(
                        "task.cancel() expects an optional reason",
                    ));
                }
                self.cancel_task(
                    task,
                    args.first()
                        .cloned()
                        .unwrap_or_else(|| Value::String("Task cancelled".to_owned())),
                );
                Ok(Value::Task(Rc::clone(task)))
            }
            _ => Err(ZuzuRustError::thrown(format!(
                "unsupported method '{}' for Task",
                name
            ))),
        }
    }

    fn call_channel_method(
        &self,
        channel: &Rc<RefCell<ChannelState>>,
        name: &str,
        args: &[Value],
    ) -> Result<Value> {
        match name {
            "send" => {
                require_arity(name, args, 1)?;
                let mut channel_ref = channel.borrow_mut();
                if channel_ref.closed {
                    return Ok(self.task_rejected("ChannelClosedException: send on closed channel"));
                }
                channel_ref.messages.push(args[0].clone());
                Ok(self.task_resolved(args[0].clone()))
            }
            "recv" => {
                require_arity(name, args, 0)?;
                Ok(self.task_channel_recv(Rc::clone(channel)))
            }
            "close" => {
                require_arity(name, args, 0)?;
                channel.borrow_mut().closed = true;
                Ok(Value::Null)
            }
            _ => Err(ZuzuRustError::thrown(format!(
                "unsupported method '{}' for Channel",
                name
            ))),
        }
    }

    fn call_cancellation_source_method(
        &self,
        source: &Rc<RefCell<CancellationState>>,
        name: &str,
        args: &[Value],
    ) -> Result<Value> {
        match name {
            "token" => {
                require_arity(name, args, 0)?;
                Ok(Value::CancellationToken(Rc::clone(source)))
            }
            "cancel" => {
                if args.len() > 1 {
                    return Err(ZuzuRustError::runtime(
                        "cancel() expects an optional reason",
                    ));
                }
                let reason = args
                    .first()
                    .cloned()
                    .unwrap_or_else(|| Value::String("Task cancelled".to_owned()));
                let watched = {
                    let mut state = source.borrow_mut();
                    state.cancelled = true;
                    state.reason = reason.clone();
                    state.watched.clone()
                };
                for task in watched {
                    self.cancel_task(&task, reason.clone());
                }
                Ok(Value::Null)
            }
            "cancelled" => {
                require_arity(name, args, 0)?;
                Ok(Value::Boolean(source.borrow().cancelled))
            }
            "reason" => {
                require_arity(name, args, 0)?;
                Ok(source.borrow().reason.clone())
            }
            _ => Err(ZuzuRustError::thrown(format!(
                "unsupported method '{}' for CancellationSource",
                name
            ))),
        }
    }

    fn call_cancellation_token_method(
        &self,
        token: &Rc<RefCell<CancellationState>>,
        name: &str,
        args: &[Value],
    ) -> Result<Value> {
        match name {
            "cancelled" => {
                require_arity(name, args, 0)?;
                Ok(Value::Boolean(token.borrow().cancelled))
            }
            "reason" => {
                require_arity(name, args, 0)?;
                Ok(token.borrow().reason.clone())
            }
            "throw_if_cancelled" => {
                require_arity(name, args, 0)?;
                let state = token.borrow();
                if state.cancelled {
                    Err(ZuzuRustError::thrown(format!(
                        "CancelledException: {}",
                        state.reason.render()
                    )))
                } else {
                    Ok(Value::Null)
                }
            }
            "watch" => {
                require_arity(name, args, 1)?;
                if let Value::Task(task) = &args[0] {
                    let mut state = token.borrow_mut();
                    if state.cancelled {
                        self.cancel_task(task, state.reason.clone());
                    } else {
                        state.watched.push(Rc::clone(task));
                    }
                    Ok(args[0].clone())
                } else {
                    Err(ZuzuRustError::runtime("watch() expects a Task"))
                }
            }
            _ => Err(ZuzuRustError::thrown(format!(
                "unsupported method '{}' for CancellationToken",
                name
            ))),
        }
    }

    fn call_class_method(
        &self,
        dispatch_class: Rc<UserClassValue>,
        receiver_class: Rc<UserClassValue>,
        name: &str,
        args: &[Value],
        named_args: Vec<(String, Value)>,
    ) -> Result<Value> {
        let method = self
            .find_static_method(&dispatch_class, name)
            .ok_or_else(|| {
                ZuzuRustError::thrown(format!(
                    "unsupported static method '{}' for {}",
                    name, dispatch_class.name
                ))
            })?;
        self.invoke_class_method_with_method(receiver_class, name, &method, args, named_args)
    }

    fn invoke_class_method_with_method(
        &self,
        receiver_class: Rc<UserClassValue>,
        name: &str,
        method: &Rc<MethodValue>,
        args: &[Value],
        named_args: Vec<(String, Value)>,
    ) -> Result<Value> {
        let field_env = Rc::new(Environment::new(Some(Rc::clone(&method.env))));
        field_env.define(
            "self".to_owned(),
            Value::UserClass(Rc::clone(&receiver_class)),
            false,
        );
        field_env.define(
            "this".to_owned(),
            Value::UserClass(Rc::clone(&receiver_class)),
            false,
        );
        field_env.define(
            "__current_class__".to_owned(),
            Value::UserClass(Rc::clone(&receiver_class)),
            false,
        );
        field_env.define(
            "__method_name__".to_owned(),
            Value::String(name.to_owned()),
            false,
        );
        field_env.define(
            "__is_static_method__".to_owned(),
            Value::Boolean(true),
            false,
        );

        let function = FunctionValue {
            name: Some(method.name.clone()),
            params: method.params.clone(),
            return_type: method.return_type.clone(),
            body: FunctionBody::Block(method.body.clone()),
            env: field_env,
            is_async: method.is_async,
            current_method: Some(Rc::clone(&method)),
        };
        self.call_function(&function, args.to_vec(), named_args)
    }

    fn eval_super_call(&self, args: Vec<Value>, env: Rc<Environment>) -> Result<Value> {
        let _current_class = match env.get("__current_class__")? {
            Value::UserClass(class) => class,
            _ => {
                return Err(ZuzuRustError::runtime(
                    "super() is only valid inside methods",
                ))
            }
        };
        let method_name = match env.get("__method_name__")? {
            Value::String(name) => name,
            _ => {
                return Err(ZuzuRustError::runtime(
                    "super() is only valid inside methods",
                ))
            }
        };
        let is_static = matches!(env.get("__is_static_method__")?, Value::Boolean(true));

        if is_static {
            let receiver_class = match env.get("self")? {
                Value::UserClass(class) => class,
                _ => {
                    return Err(ZuzuRustError::runtime(
                        "static super() requires class receiver",
                    ))
                }
            };
            let method = self
                .find_next_method(&receiver_class, &method_name, true)
                .ok_or_else(|| {
                    ZuzuRustError::runtime("super() has no static parent method to call")
                })?;
            return self.invoke_class_method_with_method(
                Rc::clone(&receiver_class),
                &method_name,
                &method,
                &args,
                Vec::new(),
            );
        }

        let self_object = match env.get("self")? {
            Value::Object(object) => object,
            _ => return Err(ZuzuRustError::runtime("super() requires object receiver")),
        };
        let method = self
            .find_next_method(&self_object.borrow().class, &method_name, false)
            .ok_or_else(|| {
                ZuzuRustError::runtime(format!(
                    "super() has no parent method '{}' to call",
                    method_name
                ))
            })?;
        let object_class = self_object.borrow().class.clone();
        self.invoke_object_method_with_context(
            &self_object,
            object_class,
            &method_name,
            &method,
            &args,
            Vec::new(),
        )
    }

    fn invoke_object_method_with_context(
        &self,
        object: &Rc<RefCell<ObjectValue>>,
        current_class: Rc<UserClassValue>,
        invoked_name: &str,
        method: &Rc<MethodValue>,
        args: &[Value],
        named_args: Vec<(String, Value)>,
    ) -> Result<Value> {
        let field_env = Rc::new(Environment::new(Some(Rc::clone(&method.env))));
        field_env.define("self".to_owned(), Value::Object(Rc::clone(object)), false);
        field_env.define("this".to_owned(), Value::Object(Rc::clone(object)), false);
        field_env.define(
            "__current_class__".to_owned(),
            Value::UserClass(Rc::clone(&current_class)),
            false,
        );
        field_env.define(
            "__method_name__".to_owned(),
            Value::String(invoked_name.to_owned()),
            false,
        );
        field_env.define(
            "__is_static_method__".to_owned(),
            Value::Boolean(false),
            false,
        );
        let fields = self.collect_field_specs(&current_class);
        let mut original_values = HashMap::new();
        for field in &fields {
            let value = {
                let mut object_mut = object.borrow_mut();
                let value = object_mut
                    .fields
                    .get(&field.name)
                    .cloned()
                    .unwrap_or(Value::Null);
                if self.is_ref_backed_composite(&value) && !matches!(value, Value::Shared(_)) {
                    let shared = value.into_shared_if_composite();
                    object_mut.fields.insert(field.name.clone(), shared.clone());
                    shared
                } else {
                    value
                }
            };
            original_values.insert(field.name.clone(), value.clone());
            if field.mutable {
                field_env.define(
                    field.name.clone(),
                    Value::AliasRef(Rc::new(LvalueRef::ObjectField {
                        object: Rc::downgrade(object),
                        name: field.name.clone(),
                    })),
                    false,
                );
            } else {
                field_env.define(field.name.clone(), value, false);
            }
        }

        if let Some(result) = self.maybe_return_trivial_field_getter(object, method) {
            return result;
        }

        let function = FunctionValue {
            name: Some(method.name.clone()),
            params: method.params.clone(),
            return_type: method.return_type.clone(),
            body: FunctionBody::Block(method.body.clone()),
            env: Rc::clone(&field_env),
            is_async: method.is_async,
            current_method: Some(Rc::clone(method)),
        };
        let result = self.call_function(&function, args.to_vec(), named_args)?;

        let mut object_mut = object.borrow_mut();
        for field in &fields {
            if field.mutable {
                continue;
            }
            if let Ok(value) = field_env.get(&field.name) {
                let current_object_value = object_mut
                    .fields
                    .get(&field.name)
                    .cloned()
                    .unwrap_or(Value::Null);
                let original_value = original_values
                    .get(&field.name)
                    .cloned()
                    .unwrap_or(Value::Null);
                if !current_object_value.coerced_eq(&original_value)
                    && value.coerced_eq(&original_value)
                {
                    continue;
                }
                object_mut.fields.insert(field.name.clone(), value);
            }
        }
        Ok(result)
    }

    fn call_function(
        &self,
        function: &FunctionValue,
        args: Vec<Value>,
        named_args: Vec<(String, Value)>,
    ) -> Result<Value> {
        let function_id = function as *const FunctionValue as usize;
        if function.is_async && !self.running_async_functions.borrow().contains(&function_id) {
            return Ok(self.task_from_function(Rc::new(function.clone()), args, named_args));
        }
        self.call_function_body(function, args, named_args)
    }

    fn run_async_function_body(
        &self,
        function: &FunctionValue,
        args: Vec<Value>,
        named_args: Vec<(String, Value)>,
    ) -> Result<Value> {
        let function_id = function as *const FunctionValue as usize;
        self.running_async_functions.borrow_mut().push(function_id);
        let result = self.call_function_body(function, args, named_args);
        self.running_async_functions.borrow_mut().pop();
        result
    }

    fn start_async_function_task(
        &self,
        function: &FunctionValue,
        args: Vec<Value>,
        named_args: Vec<(String, Value)>,
    ) -> Result<AsyncPoll> {
        let FunctionBody::Block(body) = &function.body else {
            return self
                .run_async_function_body(function, args, named_args)
                .map(AsyncPoll::Complete);
        };

        let call_env = Rc::new(Environment::new(Some(Rc::clone(&function.env))));
        let argc = args.len();
        self.bind_function_params(function, &call_env, args, named_args)?;
        call_env.define("__argc__".to_owned(), Value::Number(argc as f64), false);

        self.poll_async_frames(vec![AsyncFrame::Function {
            statements: body.statements.clone(),
            index: 0,
            env: call_env,
            return_type: function.return_type.clone(),
            name: function.name.clone(),
        }])
    }

    fn poll_async_frames(&self, mut frames: Vec<AsyncFrame>) -> Result<AsyncPoll> {
        loop {
            let Some(frame) = frames.last_mut() else {
                return Ok(AsyncPoll::Complete(Value::Null));
            };

            if frame.is_complete() {
                let frame = frames.pop().expect("checked frame presence");
                let do_value = match &frame {
                    AsyncFrame::Do { last, .. } => Some(last.clone()),
                    _ => None,
                };
                self.cleanup_async_frame(&frame)?;
                if matches!(frame, AsyncFrame::Function { .. }) {
                    return Ok(AsyncPoll::Complete(Value::Null));
                }
                if let Some(value) = do_value {
                    if let Some(parent) = frames.last_mut() {
                        parent.set_last(value);
                        continue;
                    }
                    return Ok(AsyncPoll::Complete(value));
                }
                continue;
            }

            let is_function_frame = matches!(frame, AsyncFrame::Function { .. });
            let is_do_frame = matches!(frame, AsyncFrame::Do { .. });
            let is_last = frame.index() + 1 == frame.statement_count();
            let env = frame.env();
            let statement = frame.current_statement().clone();

            match statement {
                Statement::ExpressionStatement(node) => {
                    if let Expression::AwaitExpression { body, .. } = node.expression {
                        frame.advance();
                        let awaited = self.eval_do_expression(&body, env)?;
                        let Value::Task(awaited) = awaited else {
                            return Err(ZuzuRustError::runtime(format!(
                                "await block must return a Task, got {}",
                                awaited.type_name()
                            )));
                        };
                        let disposition = if is_function_frame && is_last {
                            AwaitDisposition::Return
                        } else if is_do_frame {
                            AwaitDisposition::StoreLast
                        } else {
                            AwaitDisposition::Discard
                        };
                        return Ok(AsyncPoll::Await {
                            awaited,
                            frames,
                            disposition,
                        });
                    }
                    if is_function_frame && is_last {
                        let value = self.eval_expression(&node.expression, env)?;
                        let value = self.finish_async_frames(&frames, value)?;
                        return Ok(AsyncPoll::Complete(value));
                    }
                    let value = self.eval_expression(&node.expression, env)?;
                    if is_do_frame {
                        frame.set_last(value);
                    }
                    frame.advance();
                }
                Statement::ReturnStatement(node) => {
                    if let Some(Expression::AwaitExpression { body, .. }) = &node.argument {
                        frame.advance();
                        let awaited = self.eval_do_expression(&body, env)?;
                        let Value::Task(awaited) = awaited else {
                            return Err(ZuzuRustError::runtime(format!(
                                "await block must return a Task, got {}",
                                awaited.type_name()
                            )));
                        };
                        return Ok(AsyncPoll::Await {
                            awaited,
                            frames,
                            disposition: AwaitDisposition::Return,
                        });
                    }
                    let value = self.eval_optional_expr(node.argument.as_ref(), env)?;
                    let value = self.finish_async_frames(&frames, value)?;
                    return Ok(AsyncPoll::Complete(value));
                }
                Statement::Block(block) => {
                    frame.advance();
                    let block_env = if block.needs_lexical_scope {
                        Rc::new(Environment::new(Some(env)))
                    } else {
                        env
                    };
                    if is_do_frame {
                        frames.push(AsyncFrame::Do {
                            statements: block.statements,
                            index: 0,
                            env: block_env,
                            cleanup_env: block.needs_lexical_scope,
                            last: Value::Null,
                        });
                    } else {
                        frames.push(AsyncFrame::Block {
                            statements: block.statements,
                            index: 0,
                            env: block_env,
                            cleanup_env: block.needs_lexical_scope,
                        });
                    }
                }
                other => {
                    if is_do_frame {
                        if let Some(value) =
                            self.eval_statement_value_in_do(&other, Rc::clone(&env))?
                        {
                            frame.set_last(value);
                        }
                        frame.advance();
                        let do_return = self.get_special_prop("__do_return__");
                        if !matches!(do_return, Value::Null) {
                            self.set_special_prop("__do_return__", Value::Null);
                            let value = self.finish_async_frames(&frames, do_return)?;
                            return Ok(AsyncPoll::Complete(value));
                        }
                        continue;
                    }
                    let flow = self.eval_statement(&other, Rc::clone(&env))?;
                    frame.advance();
                    match flow {
                        ControlFlow::Normal => {
                            let do_return = self.get_special_prop("__do_return__");
                            if !matches!(do_return, Value::Null) {
                                self.set_special_prop("__do_return__", Value::Null);
                                let value = self.finish_async_frames(&frames, do_return)?;
                                return Ok(AsyncPoll::Complete(value));
                            }
                        }
                        ControlFlow::Return(value) => {
                            let value = self.finish_async_frames(&frames, value)?;
                            return Ok(AsyncPoll::Complete(value));
                        }
                        ControlFlow::Throw(value) => match &value {
                            Value::String(text) => return Err(ZuzuRustError::thrown(text.clone())),
                            _ => {
                                return Err(ZuzuRustError::thrown_with_token(
                                    self.render_value(&value)?,
                                    self.store_thrown_value(value)?,
                                ))
                            }
                        },
                        ControlFlow::Continue | ControlFlow::Break => {
                            return Err(ZuzuRustError::runtime(
                                "loop control escaped function body",
                            ))
                        }
                    }
                }
            }
        }
    }

    fn finish_async_frames(&self, frames: &[AsyncFrame], value: Value) -> Result<Value> {
        self.cleanup_async_frames(frames)?;
        if let Some((return_type, name)) = frames.iter().find_map(|frame| match frame {
            AsyncFrame::Function {
                return_type, name, ..
            } => Some((return_type.as_deref(), name.as_deref())),
            AsyncFrame::Block { .. } | AsyncFrame::Do { .. } => None,
        }) {
            self.assert_return_type(return_type, name, &value)?;
        }
        Ok(value)
    }

    fn cleanup_async_frames(&self, frames: &[AsyncFrame]) -> Result<()> {
        for frame in frames.iter().rev() {
            self.cleanup_async_frame(frame)?;
        }
        Ok(())
    }

    fn cleanup_async_frame(&self, frame: &AsyncFrame) -> Result<()> {
        match frame {
            AsyncFrame::Function { env, .. } => self.cleanup_scope(env),
            AsyncFrame::Block {
                env, cleanup_env, ..
            }
            | AsyncFrame::Do {
                env, cleanup_env, ..
            } => {
                if *cleanup_env {
                    self.cleanup_scope(env)?;
                }
                Ok(())
            }
        }
    }

    fn call_function_body(
        &self,
        function: &FunctionValue,
        args: Vec<Value>,
        named_args: Vec<(String, Value)>,
    ) -> Result<Value> {
        let call_env = Rc::new(Environment::new(Some(Rc::clone(&function.env))));
        self.push_special_props_scope();
        if let Some(method) = &function.current_method {
            self.current_method_stack
                .borrow_mut()
                .push(Rc::clone(method));
        }
        let result = (|| -> Result<Value> {
            let argc = args.len();
            self.bind_function_params(function, &call_env, args, named_args)?;
            call_env.define("__argc__".to_owned(), Value::Number(argc as f64), false);

            let flow = match &function.body {
                FunctionBody::Block(body) => {
                    self.eval_function_statements(&body.statements, Rc::clone(&call_env))?
                }
                FunctionBody::Expression(expr) => {
                    ControlFlow::Return(self.eval_expression(expr, Rc::clone(&call_env))?)
                }
            };
            self.cleanup_scope(&call_env)?;
            let value = match flow {
                ControlFlow::Normal => Ok(Value::Null),
                ControlFlow::Return(value) => Ok(value),
                ControlFlow::Throw(value) => match &value {
                    Value::String(text) => Err(ZuzuRustError::thrown(text.clone())),
                    _ => Err(ZuzuRustError::thrown_with_token(
                        self.render_value(&value)?,
                        self.store_thrown_value(value)?,
                    )),
                },
                ControlFlow::Continue | ControlFlow::Break => {
                    Err(ZuzuRustError::runtime("loop control escaped function body"))
                }
            }?;
            self.assert_return_type(
                function.return_type.as_deref(),
                function.name.as_deref(),
                &value,
            )?;
            Ok(value)
        })();
        if function.current_method.is_some() {
            self.current_method_stack.borrow_mut().pop();
        }
        self.pop_special_props_scope();
        result
    }

    fn eval_dict_key(&self, key: &DictKey, env: Rc<Environment>) -> Result<String> {
        match key {
            DictKey::Identifier { name, .. } => Ok(name.clone()),
            DictKey::StringLiteral { value, .. } => Ok(value.clone()),
            DictKey::Expression { expression, .. } => {
                Ok(self.render_value(&self.eval_expression(expression, env)?)?)
            }
        }
    }

    fn eval_index(&self, object: Value, index: Value) -> Result<Value> {
        let object = self.deref_value(&object)?;
        let index = self.value_to_number(&index)? as isize;
        match object {
            Value::Array(values) => Ok(resolve_index(values.len(), index)
                .and_then(|resolved| values.get(resolved).cloned())
                .unwrap_or(Value::Null)),
            Value::Set(values) => Ok(resolve_index(values.len(), index)
                .and_then(|resolved| values.get(resolved).cloned())
                .unwrap_or(Value::Null)),
            Value::Bag(values) => Ok(resolve_index(values.len(), index)
                .and_then(|resolved| values.get(resolved).cloned())
                .unwrap_or(Value::Null)),
            Value::PairList(values) => Ok(resolve_index(values.len(), index)
                .and_then(|resolved| values.get(resolved))
                .map(|(key, value)| Value::Pair(key.clone(), Box::new(value.clone())))
                .unwrap_or(Value::Null)),
            _ => Err(ZuzuRustError::runtime(
                "index access requires an indexable value",
            )),
        }
    }

    fn eval_slice(&self, object: Value, start: Value, end: Value) -> Result<Value> {
        let object = self.deref_value(&object)?;
        match object {
            Value::Array(values) => {
                let len = values.len() as isize;
                let mut start_index = match start {
                    Value::Null => 0,
                    other => self.value_to_number(&other)? as isize,
                };
                if start_index < 0 {
                    start_index += len;
                }
                start_index = start_index.clamp(0, len);

                let end_index = match end {
                    Value::Null => len,
                    other => {
                        let raw_end = self.value_to_number(&other)? as isize;
                        if raw_end < 0 {
                            (len + raw_end).clamp(0, len)
                        } else {
                            (start_index + raw_end).clamp(0, len)
                        }
                    }
                };
                let from = start_index.min(end_index) as usize;
                let to = end_index.max(start_index) as usize;
                Ok(Value::Array(values[from..to].to_vec()))
            }
            Value::String(text) => {
                let chars = text.chars().collect::<Vec<_>>();
                let len = chars.len() as isize;
                let mut start_index = match start {
                    Value::Null => 0,
                    other => self.value_to_number(&other)? as isize,
                };
                if start_index < 0 {
                    start_index += len;
                }
                start_index = start_index.clamp(0, len);

                let end_index = match end {
                    Value::Null => len,
                    other => {
                        let raw_end = self.value_to_number(&other)? as isize;
                        if raw_end < 0 {
                            (len + raw_end).clamp(0, len)
                        } else {
                            (start_index + raw_end).clamp(0, len)
                        }
                    }
                };
                let from = start_index.min(end_index) as usize;
                let to = end_index.max(start_index) as usize;
                Ok(Value::String(chars[from..to].iter().collect()))
            }
            _ => Err(ZuzuRustError::runtime(
                "slice access requires an Array or String value",
            )),
        }
    }

    fn iterable_items(&self, iterable: Value) -> Result<Vec<Value>> {
        let iterable = self.deref_value(&iterable)?;
        match iterable {
            Value::Array(items) | Value::Set(items) | Value::Bag(items) => Ok(items),
            Value::Dict(map) => {
                let mut keys: Vec<_> = map.keys().cloned().collect();
                keys.sort();
                Ok(keys.into_iter().map(Value::String).collect())
            }
            Value::PairList(values) => Ok(values
                .into_iter()
                .map(|(key, _)| Value::String(key))
                .collect()),
            Value::Null => Ok(Vec::new()),
            Value::Function(function) => {
                let mut items = Vec::new();
                loop {
                    match self.call_function(&function, Vec::new(), Vec::new()) {
                        Ok(value) => items.push(value),
                        Err(ZuzuRustError::Thrown { value, .. })
                            if value == "ExhaustedException"
                                || value.starts_with("ExhaustedException:") =>
                        {
                            break;
                        }
                        Err(err) => return Err(err),
                    }
                }
                Ok(items)
            }
            Value::Object(object) => {
                if self.object_has_method(&object, "to_Iterator") {
                    return self.iterable_items(self.call_object_method(
                        &object,
                        "to_Iterator",
                        &[],
                        Vec::new(),
                    )?);
                }
                if self.object_has_method(&object, "to_Array") {
                    return self.iterable_items(self.call_object_method(
                        &object,
                        "to_Array",
                        &[],
                        Vec::new(),
                    )?);
                }
                Err(ZuzuRustError::runtime(format!(
                    "for loop iterable is not supported yet: {}",
                    self.typeof_name(&Value::Object(Rc::clone(&object)))
                )))
            }
            Value::Iterator(state) => {
                let mut items = Vec::new();
                loop {
                    match self.iterator_next(&state) {
                        Ok(value) => items.push(value),
                        Err(err) if err.is_iterator_exhausted() => break,
                        Err(err) => return Err(err),
                    }
                }
                Ok(items)
            }
            other => Err(ZuzuRustError::runtime(format!(
                "for loop iterable is not supported yet: {}",
                other.type_name()
            ))),
        }
    }

    fn iterator_next(&self, state: &Rc<RefCell<IteratorState>>) -> Result<Value> {
        let mut state = state.borrow_mut();
        if state.index >= state.items.len() {
            return Err(ZuzuRustError::runtime("iterator exhausted"));
        }
        let value = state.items[state.index].clone();
        state.index += 1;
        Ok(value)
    }

    fn eval_new_expression(&self, argument: &Expression, env: Rc<Environment>) -> Result<Value> {
        match argument {
            Expression::Call {
                callee, arguments, ..
            } if matches!(callee.as_ref(), Expression::MemberAccess { .. }) => {
                let (object, member) = match callee.as_ref() {
                    Expression::MemberAccess { object, member, .. } => {
                        (object.as_ref(), member.as_str())
                    }
                    _ => unreachable!(),
                };
                let (args, named_args) = self.eval_call_arguments(arguments, Rc::clone(&env))?;
                let mut receiver = self.eval_new_expression(object, env)?;
                self.call_method_named(&mut receiver, member, &args, named_args)
            }
            Expression::MemberAccess { object, member, .. } => {
                let mut receiver = self.eval_new_expression(object, env)?;
                self.call_method(&mut receiver, member, &[])
            }
            Expression::Call {
                callee, arguments, ..
            } => {
                let callee = self.eval_expression(callee, Rc::clone(&env))?;
                let (args, named_args) = self.eval_call_arguments(arguments, env)?;
                self.call_value(callee, args, named_args)
            }
            _ => Err(ZuzuRustError::runtime(
                "new expects a constructor call expression",
            )),
        }
    }

    fn construct_builtin_class(
        &self,
        name: &str,
        args: Vec<Value>,
        named_args: Vec<(String, Value)>,
    ) -> Result<Value> {
        match name {
            "Pair" => construct_pair(args, named_args),
            "PairList" => construct_pairlist(args, named_args),
            "Array" => {
                reject_named_args(name, &named_args)?;
                Ok(Value::Array(args))
            }
            "Bag" => {
                reject_named_args(name, &named_args)?;
                Ok(Value::Bag(args))
            }
            "Set" => {
                reject_named_args(name, &named_args)?;
                let mut items = Vec::new();
                for value in args {
                    push_unique(&mut items, value);
                }
                Ok(Value::Set(items))
            }
            "Dict" => {
                reject_named_args(name, &named_args)?;
                let mut map = HashMap::new();
                for pair in args {
                    let (key, value) = expect_pair_like(&pair)?;
                    map.insert(key, value);
                }
                Ok(Value::Dict(map))
            }
            "Channel" => {
                reject_named_args(name, &named_args)?;
                if !args.is_empty() {
                    return Err(ZuzuRustError::runtime("Channel() expects no arguments"));
                }
                Ok(Value::Channel(Rc::new(RefCell::new(ChannelState {
                    messages: Vec::new(),
                    closed: false,
                }))))
            }
            "CancellationSource" => {
                reject_named_args(name, &named_args)?;
                if !args.is_empty() {
                    return Err(ZuzuRustError::runtime(
                        "CancellationSource() expects no arguments",
                    ));
                }
                Ok(Value::CancellationSource(Rc::new(RefCell::new(
                    CancellationState {
                        cancelled: false,
                        reason: Value::Null,
                        watched: Vec::new(),
                    },
                ))))
            }
            "Path" | "JSON" | "YAML" | "CSV" | "Time" | "TimeParser" | "CookieJar"
            | "UserAgent" | "Mailer" | "CLib" | "Widget" | "Window" | "VBox" | "HBox" | "Frame"
            | "Label" | "Text" | "RichText" | "Image" | "Input" | "DatePicker" | "Checkbox"
            | "Radio" | "RadioGroup" | "Select" | "Menu" | "MenuItem" | "Button" | "Separator"
            | "Slider" | "Progress" | "Tabs" | "Tab" | "ListView" | "TreeView" | "Event"
            | "ListenerToken" => stdlib::construct_builtin_object(self, name, args, named_args)
                .unwrap_or_else(|| unreachable!()),
            "Exception"
            | "ExhaustedException"
            | "TypeException"
            | "CancelledException"
            | "TimeoutException"
            | "ChannelClosedException"
            | "MarshallingException"
            | "UnmarshallingException" => {
                let mut message = name.to_owned();
                for (key, value) in named_args {
                    if key == "message" {
                        message = format!("{name}: {}", self.render_value(&value)?);
                    }
                }
                if args.len() == 1 {
                    message = format!("{name}: {}", self.render_value(&args[0])?);
                }
                Ok(Value::String(message))
            }
            other => Err(ZuzuRustError::runtime(format!(
                "cannot call class '{}' yet",
                other
            ))),
        }
    }

    fn construct_user_class(
        &self,
        class: Rc<UserClassValue>,
        args: Vec<Value>,
        named_args: Vec<(String, Value)>,
    ) -> Result<Value> {
        self.construct_user_class_inner(class, args, named_args, true)
    }

    pub(in crate::runtime) fn make_user_instance_without_build(
        &self,
        class: Rc<UserClassValue>,
        slots: HashMap<String, Value>,
    ) -> Result<Value> {
        self.construct_user_class_inner(class, Vec::new(), slots.into_iter().collect(), false)
    }

    fn construct_user_class_inner(
        &self,
        class: Rc<UserClassValue>,
        args: Vec<Value>,
        named_args: Vec<(String, Value)>,
        call_build: bool,
    ) -> Result<Value> {
        let builtin_value = match self.class_builtin_base_name(&class) {
            Some(base) if base == "Array" => {
                Some(self.construct_builtin_class("Array", args.clone(), Vec::new())?)
            }
            Some(base) if base == "Bag" => {
                Some(self.construct_builtin_class("Bag", args.clone(), Vec::new())?)
            }
            Some(base) if base == "Set" => {
                Some(self.construct_builtin_class("Set", args.clone(), Vec::new())?)
            }
            Some(base) if base == "Dict" => {
                Some(self.construct_builtin_class("Dict", args.clone(), Vec::new())?)
            }
            Some(_) => {
                if !args.is_empty() {
                    return Err(ZuzuRustError::runtime(format!(
                        "constructor for '{}' does not accept positional arguments yet",
                        class.name
                    )));
                }
                None
            }
            None => {
                if !args.is_empty() {
                    return Err(ZuzuRustError::runtime(format!(
                        "constructor for '{}' does not accept positional arguments yet",
                        class.name
                    )));
                }
                None
            }
        };

        let object = Rc::new(RefCell::new(ObjectValue {
            class: Rc::clone(&class),
            fields: HashMap::new(),
            weak_fields: HashSet::new(),
            builtin_value,
        }));

        let fields = self.collect_field_specs(&class);
        for field in &fields {
            let value = match &field.default_value {
                Some(default_value) => {
                    self.eval_expression(default_value, Rc::clone(&self.class_decl_env(&class)))?
                }
                None => Value::Null,
            };
            self.object_set_field(&object, field, value)?;
        }

        for (name, value) in named_args {
            let field = fields
                .iter()
                .find(|field| field.name == name)
                .ok_or_else(|| {
                    ZuzuRustError::runtime(format!(
                        "constructor for '{}' does not have a '{}' field",
                        class.name, name
                    ))
                })?;
            self.object_set_field(&object, field, value)?;
        }

        if call_build && self.object_has_method(&object, "__build__") {
            let _ = self.call_object_method(&object, "__build__", &[], Vec::new())?;
        }

        Ok(Value::Object(object))
    }

    pub(in crate::runtime) fn marshal_builtin_env(&self) -> Rc<Environment> {
        let env = Rc::new(Environment::new(None));
        self.install_builtins(&env);
        env
    }

    pub(in crate::runtime) fn marshal_current_or_builtin_env(&self) -> Rc<Environment> {
        self.current_env()
            .unwrap_or_else(|| self.marshal_builtin_env())
    }

    pub(in crate::runtime) fn marshal_child_env(&self, parent: Rc<Environment>) -> Rc<Environment> {
        Rc::new(Environment::new(Some(parent)))
    }

    pub(in crate::runtime) fn marshal_define_env(
        &self,
        env: &Rc<Environment>,
        name: &str,
        value: Value,
        mutable: bool,
    ) {
        env.define(name.to_owned(), value, mutable);
    }

    pub(in crate::runtime) fn marshal_refresh_env(
        &self,
        env: &Rc<Environment>,
        name: &str,
        value: Value,
    ) -> bool {
        env.refresh_existing_binding(name, value)
    }

    pub(in crate::runtime) fn marshal_load_module_export(
        &self,
        module_name: &str,
        export_name: &str,
    ) -> Result<Value> {
        self.load_module_exports(module_name)?
            .get(export_name)
            .cloned()
            .ok_or_else(|| {
                ZuzuRustError::runtime(format!(
                    "module '{}' does not export '{}'",
                    module_name, export_name
                ))
            })
    }

    pub(in crate::runtime) fn marshal_binding(
        &self,
        env: &Rc<Environment>,
        name: &str,
    ) -> Option<(Value, bool)> {
        env.find_binding(name)
            .map(|binding| (binding.value.clone(), !binding.mutable))
    }

    #[allow(dead_code)]
    pub(in crate::runtime) fn eval_marshal_code_value(
        &self,
        source: &str,
        binding_name: &str,
    ) -> Result<Value> {
        let parent = self
            .current_env()
            .unwrap_or_else(|| self.marshal_builtin_env());
        let env = self.marshal_child_env(parent);
        self.eval_marshal_code_value_in_env(source, binding_name, env, false)
    }

    pub(in crate::runtime) fn eval_marshal_code_value_in_env(
        &self,
        source: &str,
        binding_name: &str,
        env: Rc<Environment>,
        expression_result: bool,
    ) -> Result<Value> {
        let source = if expression_result {
            format!("let __zuzu_marshal_value := {source}; __zuzu_marshal_value;")
        } else {
            source.to_owned()
        };
        let options = ParseOptions::new(false, self.infer_types, self.optimizations.clone());
        let program = parse_program_with_compile_options_and_source_file(
            &source,
            &options,
            Some("<std/marshal-code>"),
        )?;
        match self.eval_statements(&program.statements, Rc::clone(&env))? {
            ControlFlow::Normal => {
                if expression_result {
                    env.get("__zuzu_marshal_value")
                } else {
                    env.get(binding_name)
                }
            }
            ControlFlow::Return(_) => Err(ZuzuRustError::runtime(
                "return is not valid at top-level scope",
            )),
            ControlFlow::Throw(value) => Err(ZuzuRustError::thrown(self.render_value(&value)?)),
            ControlFlow::Break | ControlFlow::Continue => Err(ZuzuRustError::runtime(
                "loop control escaped marshal code record",
            )),
        }
    }

    pub(in crate::runtime) fn marshal_bind_method(
        &self,
        receiver: Value,
        method_name: &str,
    ) -> Result<Value> {
        match receiver.clone() {
            Value::Object(object) => {
                let class = object.borrow().class.clone();
                let method = self.find_method(&class, method_name).ok_or_else(|| {
                    ZuzuRustError::runtime(format!("method '{}' was not found", method_name))
                })?;
                Ok(Value::Method(Rc::new(MethodValue {
                    name: method.name.clone(),
                    params: method.params.clone(),
                    return_type: method.return_type.clone(),
                    body: method.body.clone(),
                    env: Rc::clone(&method.env),
                    is_static: method.is_static,
                    is_async: method.is_async,
                    bound_receiver: Some(receiver),
                    bound_name: Some(method_name.to_owned()),
                })))
            }
            Value::UserClass(class) => {
                let method = self
                    .find_static_method(&class, method_name)
                    .ok_or_else(|| {
                        ZuzuRustError::runtime(format!("method '{}' was not found", method_name))
                    })?;
                Ok(Value::Method(Rc::new(MethodValue {
                    name: method.name.clone(),
                    params: method.params.clone(),
                    return_type: method.return_type.clone(),
                    body: method.body.clone(),
                    env: Rc::clone(&method.env),
                    is_static: method.is_static,
                    is_async: method.is_async,
                    bound_receiver: Some(receiver),
                    bound_name: Some(method_name.to_owned()),
                })))
            }
            _ => Err(ZuzuRustError::runtime(
                "bound method receiver must be an Object or Class",
            )),
        }
    }

    pub(in crate::runtime) fn marshal_set_object_field(
        &self,
        object: &Rc<RefCell<ObjectValue>>,
        name: &str,
        value: Value,
        is_weak_storage: bool,
    ) -> Result<()> {
        let class = object.borrow().class.clone();
        if let Some(field) = self.find_field_spec(&class, name) {
            return self.object_set_field_with_weak_write(object, &field, value, is_weak_storage);
        }
        self.object_store_slot(object, name, value, is_weak_storage)?;
        Ok(())
    }

    fn object_store_slot(
        &self,
        object: &Rc<RefCell<ObjectValue>>,
        name: &str,
        value: Value,
        weak_write: bool,
    ) -> Result<()> {
        let class = object.borrow().class.clone();
        if let Some(field) = self.find_field_spec(&class, name) {
            return self.object_set_field_with_weak_write(object, &field, value, weak_write);
        }
        let mut object = object.borrow_mut();
        if weak_write {
            object.weak_fields.insert(name.to_owned());
        } else {
            object.weak_fields.remove(name);
        }
        object
            .fields
            .insert(name.to_owned(), value.stored_with_weak_policy(weak_write));
        Ok(())
    }

    pub(in crate::runtime) fn marshal_call_object_hook(
        &self,
        object: &Rc<RefCell<ObjectValue>>,
        name: &str,
    ) -> Result<()> {
        if self.object_has_method(object, name) {
            let _ = self.call_object_method(object, name, &[], Vec::new())?;
        }
        Ok(())
    }

    fn call_ref(
        &self,
        reference: Rc<LvalueRef>,
        args: Vec<Value>,
        named_args: Vec<(String, Value)>,
    ) -> Result<Value> {
        if !named_args.is_empty() {
            return Err(ZuzuRustError::runtime(
                "lvalue refs do not accept named arguments",
            ));
        }
        match reference.as_ref() {
            LvalueRef::Expression { env, target } => match args.len() {
                0 => self.eval_expression(target, Rc::clone(env)),
                1 => self.assign_lvalue(
                    target,
                    args.into_iter().next().unwrap(),
                    false,
                    Rc::clone(env),
                ),
                2 => {
                    let mut args = args.into_iter();
                    let value = args.next().unwrap();
                    let weak_write = self.value_is_truthy(&args.next().unwrap())?;
                    self.assign_lvalue(target, value, weak_write, Rc::clone(env))
                }
                _ => Err(ZuzuRustError::runtime(
                    "lvalue refs accept zero, one, or two arguments",
                )),
            },
            LvalueRef::ObjectField { object, name } => match args.len() {
                0 => {
                    let value = object
                        .upgrade()
                        .and_then(|object| object.borrow().fields.get(name).cloned())
                        .unwrap_or(Value::Null);
                    Ok(if value.is_weak_value() {
                        value.resolve_weak_value()
                    } else {
                        value
                    })
                }
                1 => {
                    let Some(object) = object.upgrade() else {
                        return Ok(Value::Null);
                    };
                    let value = args.into_iter().next().unwrap();
                    self.object_store_slot(&object, name, value.clone(), false)?;
                    Ok(value)
                }
                2 => {
                    let Some(object) = object.upgrade() else {
                        return Ok(Value::Null);
                    };
                    let mut args = args.into_iter();
                    let value = args.next().unwrap();
                    let weak_write = self.value_is_truthy(&args.next().unwrap())?;
                    self.object_store_slot(&object, name, value.clone(), weak_write)?;
                    Ok(value)
                }
                _ => Err(ZuzuRustError::runtime(
                    "lvalue refs accept zero, one, or two arguments",
                )),
            },
        }
    }

    fn bind_function_params(
        &self,
        function: &FunctionValue,
        env: &Environment,
        args: Vec<Value>,
        named_args: Vec<(String, Value)>,
    ) -> Result<()> {
        let mut arg_index = 0usize;
        let mut variadic_seen = false;
        let has_variadic_param = function.params.iter().any(|param| param.variadic);
        let named_pairlist_param = function
            .params
            .iter()
            .any(|param| has_variadic_param && param.declared_type.as_deref() == Some("PairList"));
        if !named_args.is_empty() && !named_pairlist_param {
            return Err(ZuzuRustError::thrown(
                "Named arguments are not accepted by this function call",
            ));
        }
        for (index, param) in function.params.iter().enumerate() {
            let fixed_remaining_after_current = function.params[index + 1..]
                .iter()
                .filter(|later| {
                    !named_pairlist_param || later.declared_type.as_deref() != Some("PairList")
                })
                .count();
            let value = if has_variadic_param && param.declared_type.as_deref() == Some("PairList")
            {
                Value::PairList(named_args.clone())
            } else if named_pairlist_param
                && param.declared_type.as_deref() == Some("Array")
                && fixed_remaining_after_current == 0
            {
                let rest = args[arg_index..].to_vec();
                arg_index = args.len();
                Value::Array(rest)
            } else if param.variadic {
                variadic_seen = true;
                let rest = args[arg_index..].to_vec();
                arg_index = args.len();
                Value::Array(rest)
            } else if param.declared_type.as_deref() == Some("Array")
                && args.len().saturating_sub(arg_index) > fixed_remaining_after_current + 1
            {
                let take = args.len() - arg_index - fixed_remaining_after_current;
                let rest = args[arg_index..arg_index + take].to_vec();
                arg_index += take;
                Value::Array(rest)
            } else if let Some(value) = args.get(arg_index).cloned() {
                arg_index += 1;
                value
            } else if let Some(default) = &param.default_value {
                self.eval_expression(
                    default,
                    Rc::new(Environment::new(Some(Rc::clone(&function.env)))),
                )?
            } else if param.optional {
                Value::Null
            } else {
                return Err(ZuzuRustError::thrown(
                    "Wrong number of arguments for function call",
                ));
            };
            if !(param.optional && matches!(value, Value::Null)) {
                self.assert_declared_type(param.declared_type.as_deref(), &value, &param.name)?;
            }
            env.define(param.name.clone(), value, true);
        }
        if arg_index < args.len() && !variadic_seen {
            return Err(ZuzuRustError::thrown(
                "Wrong number of arguments for function call",
            ));
        }
        Ok(())
    }

    fn concat_values(&self, lhs: Value, rhs: Value) -> Result<Value> {
        match (lhs, rhs) {
            (Value::BinaryString(mut left), Value::BinaryString(right)) => {
                left.extend(right);
                Ok(Value::BinaryString(left))
            }
            (Value::String(left), Value::BinaryString(right)) => {
                Ok(Value::String(left + &self.binary_ascii_string(&right)?))
            }
            (Value::BinaryString(left), Value::String(right)) => {
                Ok(Value::String(self.binary_ascii_string(&left)? + &right))
            }
            (left, right) => Ok(Value::String(
                self.value_to_operator_string(&left)? + &self.value_to_operator_string(&right)?,
            )),
        }
    }

    fn binary_ascii_string(&self, bytes: &[u8]) -> Result<String> {
        if bytes.iter().all(|byte| byte.is_ascii()) {
            Ok(bytes.iter().map(|byte| *byte as char).collect())
        } else {
            Err(ZuzuRustError::thrown(
                "Cannot implicitly concatenate non-ASCII BinaryString",
            ))
        }
    }

    fn eval_bitwise(&self, operator: &str, lhs: Value, rhs: Value) -> Result<Value> {
        match (lhs, rhs) {
            (Value::BinaryString(left), Value::BinaryString(right)) => {
                if left.len() != right.len() {
                    return Err(ZuzuRustError::thrown(
                        "BinaryString bitwise operands must be equal length",
                    ));
                }
                let op = match operator {
                    "&" => |a, b| a & b,
                    "|" => |a, b| a | b,
                    "^" => |a, b| a ^ b,
                    _ => unreachable!(),
                };
                Ok(Value::BinaryString(
                    left.into_iter().zip(right).map(|(a, b)| op(a, b)).collect(),
                ))
            }
            (left, right) => {
                let lhs = self.value_to_number(&left)? as i64;
                let rhs = self.value_to_number(&right)? as i64;
                let value = match operator {
                    "&" => lhs & rhs,
                    "|" => lhs | rhs,
                    "^" => lhs ^ rhs,
                    _ => unreachable!(),
                };
                Ok(Value::Number(value as f64))
            }
        }
    }

    fn assign_index_target(
        &self,
        object: &Expression,
        index: &Expression,
        value: Value,
        weak_write: bool,
        env: Rc<Environment>,
    ) -> Result<Value> {
        let idx = self.value_to_number(&self.eval_expression(index, Rc::clone(&env))?)? as usize;
        let object_value = self.eval_expression(object, Rc::clone(&env))?;
        if let Value::Shared(shared) = &object_value {
            let mut current = shared.borrow().clone();
            return match &mut current {
                Value::Array(values) => {
                    if idx >= values.len() {
                        values.resize(idx + 1, Value::Null);
                    }
                    values[idx] = value.clone().stored_with_weak_policy(weak_write);
                    *shared.borrow_mut() = current;
                    Ok(value)
                }
                other => Err(ZuzuRustError::runtime(format!(
                    "index assignment requires an Array value, got {}",
                    self.typeof_name(other)
                ))),
            };
        }
        if let Value::Ref(reference) | Value::AliasRef(reference) = object_value {
            return match self.deref_value(&Value::AliasRef(Rc::clone(&reference)))? {
                Value::Array(mut values) => {
                    if idx >= values.len() {
                        values.resize(idx + 1, Value::Null);
                    }
                    values[idx] = value.clone().stored_with_weak_policy(weak_write);
                    let _ = self.assign_reference(reference, Value::Array(values))?;
                    Ok(value)
                }
                other => Err(ZuzuRustError::runtime(format!(
                    "index assignment requires an Array value, got {}",
                    self.typeof_name(&other)
                ))),
            };
        }
        match object_value {
            Value::Array(mut values) => {
                if idx >= values.len() {
                    values.resize(idx + 1, Value::Null);
                }
                values[idx] = value.clone().stored_with_weak_policy(weak_write);
                match object {
                    Expression::Identifier { name, .. } => {
                        env.assign(name, Value::Array(values))?;
                        Ok(value)
                    }
                    _ => {
                        self.assign_lvalue(object, Value::Array(values), false, env)?;
                        Ok(value)
                    }
                }
            }
            other => Err(ZuzuRustError::runtime(format!(
                "index assignment requires an Array value, got {}",
                self.typeof_name(&other)
            ))),
        }
    }

    fn assign_dict_target(
        &self,
        object: &Expression,
        key: &DictKey,
        value: Value,
        weak_write: bool,
        env: Rc<Environment>,
    ) -> Result<Value> {
        let key_name = self.eval_dict_key(key, Rc::clone(&env))?;
        let object_value = self.eval_expression(object, Rc::clone(&env))?;
        if let Value::Shared(shared) = &object_value {
            let mut current = shared.borrow().clone();
            return match &mut current {
                Value::Dict(map) => {
                    map.insert(key_name, value.clone().stored_with_weak_policy(weak_write));
                    *shared.borrow_mut() = current;
                    Ok(value)
                }
                Value::PairList(values) => {
                    if let Some((_, entry_value)) = values
                        .iter_mut()
                        .find(|(entry_key, _)| entry_key == &key_name)
                    {
                        *entry_value = value.clone().stored_with_weak_policy(weak_write);
                    } else {
                        values.push((key_name, value.clone().stored_with_weak_policy(weak_write)));
                    }
                    *shared.borrow_mut() = current;
                    Ok(value)
                }
                other => Err(ZuzuRustError::runtime(format!(
                    "unsupported assignment target: {}",
                    self.typeof_name(other)
                ))),
            };
        }
        if let Value::Ref(reference) | Value::AliasRef(reference) = object_value {
            return match self.deref_value(&Value::AliasRef(Rc::clone(&reference)))? {
                Value::Dict(mut map) => {
                    map.insert(key_name, value.clone().stored_with_weak_policy(weak_write));
                    let _ = self.assign_reference(reference, Value::Dict(map))?;
                    Ok(value)
                }
                Value::PairList(mut values) => {
                    if let Some((_, entry_value)) = values
                        .iter_mut()
                        .find(|(entry_key, _)| entry_key == &key_name)
                    {
                        *entry_value = value.clone().stored_with_weak_policy(weak_write);
                    } else {
                        values.push((key_name, value.clone().stored_with_weak_policy(weak_write)));
                    }
                    let _ = self.assign_reference(reference, Value::PairList(values))?;
                    Ok(value)
                }
                Value::Object(object) => {
                    self.object_store_slot(&object, &key_name, value.clone(), weak_write)?;
                    Ok(value)
                }
                _ => Err(ZuzuRustError::runtime("unsupported assignment target")),
            };
        }
        match object_value {
            Value::Dict(mut map) => {
                map.insert(key_name, value.clone().stored_with_weak_policy(weak_write));
                match object {
                    Expression::Identifier { name, .. } => {
                        env.assign(name, Value::Dict(map))?;
                        Ok(value)
                    }
                    _ => {
                        self.assign_lvalue(object, Value::Dict(map), false, env)?;
                        Ok(value)
                    }
                }
            }
            Value::PairList(mut values) => {
                if let Some((_, entry_value)) = values
                    .iter_mut()
                    .find(|(entry_key, _)| entry_key == &key_name)
                {
                    *entry_value = value.clone().stored_with_weak_policy(weak_write);
                } else {
                    values.push((key_name, value.clone().stored_with_weak_policy(weak_write)));
                }
                match object {
                    Expression::Identifier { name, .. } => {
                        env.assign(name, Value::PairList(values))?;
                        Ok(value)
                    }
                    _ => {
                        self.assign_lvalue(object, Value::PairList(values), false, env)?;
                        Ok(value)
                    }
                }
            }
            Value::Object(object) => {
                let existing = object.borrow().fields.get(&key_name).cloned();
                if let Some(Value::Ref(reference) | Value::AliasRef(reference)) = existing {
                    let _ = self.assign_reference_with_weak_write(
                        reference,
                        value.clone(),
                        weak_write,
                    )?;
                } else {
                    self.object_store_slot(&object, &key_name, value.clone(), weak_write)?;
                }
                Ok(value)
            }
            _ => Err(ZuzuRustError::runtime("unsupported assignment target")),
        }
    }

    fn assign_slice_target(
        &self,
        object: &Expression,
        start: Option<&Expression>,
        end: Option<&Expression>,
        value: Value,
        env: Rc<Environment>,
    ) -> Result<Value> {
        let start_value = self.eval_optional_expr(start, Rc::clone(&env))?;
        let count_value = self.eval_optional_expr(end, Rc::clone(&env))?;
        if let Expression::Identifier { name, .. } = object {
            match env.get(name)? {
                Value::Shared(shared) => {
                    let mut current = shared.borrow().clone();
                    match &mut current {
                        Value::Array(values) => {
                            let replacement = match value {
                                Value::Array(values) => values,
                                _ => {
                                    return Err(ZuzuRustError::runtime(
                                        "slice assignment requires an Array value",
                                    ))
                                }
                            };
                            let start = match start_value {
                                Value::Null => 0,
                                ref other => self.value_to_number(other)? as usize,
                            };
                            let count = match count_value {
                                Value::Null => values.len().saturating_sub(start),
                                ref other => self.value_to_number(other)? as usize,
                            };
                            let from = start.min(values.len());
                            let to = (from + count).min(values.len());
                            values.splice(from..to, replacement.clone());
                            *shared.borrow_mut() = current;
                            Ok(Value::Array(replacement))
                        }
                        Value::String(text) => {
                            let replacement = match value {
                                Value::String(text) => text,
                                _ => {
                                    return Err(ZuzuRustError::runtime(
                                        "string slice assignment requires a String value",
                                    ))
                                }
                            };
                            let mut chars = text.chars().collect::<Vec<_>>();
                            let start = match start_value {
                                Value::Null => 0,
                                ref other => self.value_to_number(other)? as usize,
                            };
                            let count = match count_value {
                                Value::Null => chars.len().saturating_sub(start),
                                ref other => self.value_to_number(other)? as usize,
                            };
                            let from = start.min(chars.len());
                            let to = (from + count).min(chars.len());
                            chars.splice(from..to, replacement.chars());
                            let updated = chars.into_iter().collect::<String>();
                            *shared.borrow_mut() = Value::String(updated);
                            Ok(Value::String(replacement))
                        }
                        _ => Err(ZuzuRustError::runtime(
                            "slice assignment requires an Array or String value",
                        )),
                    }
                }
                Value::Array(mut values) => {
                    let replacement = match value {
                        Value::Array(values) => values,
                        _ => {
                            return Err(ZuzuRustError::runtime(
                                "slice assignment requires an Array value",
                            ))
                        }
                    };
                    let start = match start_value {
                        Value::Null => 0,
                        ref other => self.value_to_number(other)? as usize,
                    };
                    let count = match count_value {
                        Value::Null => values.len().saturating_sub(start),
                        ref other => self.value_to_number(other)? as usize,
                    };
                    let from = start.min(values.len());
                    let to = (from + count).min(values.len());
                    values.splice(from..to, replacement.clone());
                    env.assign(name, Value::Array(values))?;
                    Ok(Value::Array(replacement))
                }
                Value::String(text) => {
                    let replacement = match value {
                        Value::String(text) => text,
                        _ => {
                            return Err(ZuzuRustError::runtime(
                                "string slice assignment requires a String value",
                            ))
                        }
                    };
                    let mut chars = text.chars().collect::<Vec<_>>();
                    let start = match start_value {
                        Value::Null => 0,
                        ref other => self.value_to_number(other)? as usize,
                    };
                    let count = match count_value {
                        Value::Null => chars.len().saturating_sub(start),
                        ref other => self.value_to_number(other)? as usize,
                    };
                    let from = start.min(chars.len());
                    let to = (from + count).min(chars.len());
                    chars.splice(from..to, replacement.chars());
                    env.assign(name, Value::String(chars.into_iter().collect()))?;
                    Ok(Value::String(replacement))
                }
                _ => Err(ZuzuRustError::runtime(
                    "slice assignment requires an Array or String value",
                )),
            }
        } else {
            Err(ZuzuRustError::runtime("unsupported assignment target"))
        }
    }

    fn apply_regex_replacement(
        &self,
        current: Value,
        replacement: &Expression,
        env: Rc<Environment>,
    ) -> Result<Value> {
        let source = self.value_to_operator_string(&current)?;
        let (regex_pattern, regex_flags, replacement_expr) = match replacement {
            Expression::Binary {
                operator,
                left,
                right,
                ..
            } if operator == "->" => {
                let regex_value = self.eval_expression(left, Rc::clone(&env))?;
                let (pattern, flags) = self.coerce_regex_operand(&regex_value).map_err(|_| {
                    ZuzuRustError::runtime("~= expects a regexp -> replacement expression")
                })?;
                (pattern, flags, right.as_ref())
            }
            _ => {
                return Err(ZuzuRustError::runtime(
                    "~= expects a regexp -> replacement expression",
                ))
            }
        };
        let compiled = self.compile_regex(&regex_pattern, &regex_flags)?;
        let mut out = String::new();
        let mut cursor = 0usize;
        let global = regex_flags.contains('g');

        while cursor <= source.len() {
            let Some(captures) = compiled.captures(&source[cursor..]) else {
                break;
            };
            let Some(full) = captures.get(0) else {
                break;
            };
            let start = cursor + full.start();
            let end = cursor + full.end();
            out.push_str(&source[cursor..start]);
            let match_env = Rc::new(Environment::new(Some(Rc::clone(&env))));
            match_env.define(
                "m".to_owned(),
                Value::Array(
                    captures
                        .iter()
                        .map(|item| {
                            Value::String(item.map(|m| m.as_str()).unwrap_or("").to_owned())
                        })
                        .collect(),
                ),
                true,
            );
            let replacement_value =
                self.eval_expression(replacement_expr, Rc::clone(&match_env))?;
            out.push_str(&self.render_value(&replacement_value)?);
            cursor = end;
            if !global {
                break;
            }
            if end == start {
                break;
            }
        }
        out.push_str(&source[cursor..]);
        Ok(Value::String(out))
    }

    pub(in crate::runtime) fn render_value(&self, value: &Value) -> Result<String> {
        let value = self.deref_value(value)?;
        match &value {
            Value::Object(object) if self.object_has_method(object, "to_String") => {
                let rendered = self.call_object_method(object, "to_String", &[], Vec::new())?;
                self.render_value(&rendered)
            }
            Value::Object(object) => {
                if let Some(rendered) = self.object_builtin_render(object)? {
                    Ok(rendered)
                } else {
                    Ok(value.render())
                }
            }
            _ => Ok(value.render()),
        }
    }

    pub(in crate::runtime) fn value_to_operator_string(&self, value: &Value) -> Result<String> {
        let value = self.deref_value(value)?;
        match &value {
            Value::Null => Ok(String::new()),
            Value::Boolean(true) => Ok("true".to_owned()),
            Value::Boolean(false) => Ok("false".to_owned()),
            Value::Number(number) => Ok(render_number(*number)),
            Value::String(text) => Ok(text.clone()),
            Value::Regex(pattern, _) => Ok(pattern.clone()),
            Value::Object(object) if self.object_has_method(object, "to_String") => {
                let rendered = self.call_object_method(object, "to_String", &[], Vec::new())?;
                self.value_to_operator_string(&rendered)
            }
            _ => Err(ZuzuRustError::runtime(format!(
                "cannot coerce {} to String",
                self.typeof_name(&value)
            ))),
        }
    }

    fn render_repl_value(&self, value: &Value) -> Result<String> {
        let value = self.deref_value(value)?;
        match &value {
            Value::Null => Ok("Null".to_owned()),
            Value::Boolean(_) | Value::Number(_) | Value::String(_) | Value::BinaryString(_) => {
                self.render_value(&value)
            }
            _ => Ok(self.typeof_name(&value)),
        }
    }

    fn value_to_number(&self, value: &Value) -> Result<f64> {
        let value = self.deref_value(value)?;
        match &value {
            Value::Object(object) if self.object_has_method(object, "to_Number") => {
                let coerced = self.call_object_method(object, "to_Number", &[], Vec::new())?;
                self.value_to_number(&coerced)
            }
            _ => value.to_number(),
        }
    }

    pub(in crate::runtime) fn value_is_truthy(&self, value: &Value) -> Result<bool> {
        let value = self.deref_value(value)?;
        match &value {
            Value::Object(object) if self.object_has_method(object, "to_Boolean") => {
                let coerced = self.call_object_method(object, "to_Boolean", &[], Vec::new())?;
                self.value_is_truthy(&coerced)
            }
            Value::Object(object) => {
                if let Some(result) = self.object_builtin_truthy(object) {
                    Ok(result)
                } else {
                    Ok(value.is_truthy())
                }
            }
            _ => Ok(value.is_truthy()),
        }
    }

    fn typeof_name(&self, value: &Value) -> String {
        let value = self.deref_value(value).unwrap_or_else(|_| value.clone());
        match value {
            Value::Object(object) => object.borrow().class.name.clone(),
            Value::UserClass(_) | Value::Class(_) => "Class".to_owned(),
            Value::Trait(_) => "Trait".to_owned(),
            Value::Method(_) => "Method".to_owned(),
            _ => value.type_name().to_owned(),
        }
    }

    fn member_name_operand(&self, expr: &Expression, env: Rc<Environment>) -> Result<String> {
        match expr {
            Expression::Identifier { name, .. } => Ok(name.clone()),
            Expression::StringLiteral { value, .. } => Ok(value.clone()),
            _ => self.render_value(&self.eval_expression(expr, env)?),
        }
    }

    fn value_instanceof(&self, value: &Value, class_value: &Value) -> bool {
        let value = self.deref_value(value).unwrap_or_else(|_| value.clone());
        let class_value = self
            .deref_value(class_value)
            .unwrap_or_else(|_| class_value.clone());
        match (&value, &class_value) {
            (_, Value::Class(name)) if name.as_str() == "Any" => true,
            (Value::Class(_) | Value::UserClass(_), Value::Class(name)) => name.as_str() == "Class",
            (Value::Object(object), Value::UserClass(class)) => {
                self.class_matches_user(&object.borrow().class, class)
            }
            (Value::String(_), Value::UserClass(class)) => self.class_matches(class, "Exception"),
            (Value::Null, Value::Class(name)) => name.as_str() == "Null",
            (Value::Boolean(_), Value::Class(name)) => name.as_str() == "Boolean",
            (Value::Number(_), Value::Class(name)) => name.as_str() == "Number",
            (Value::String(_), Value::Class(name)) => name.as_str() == "String",
            (Value::BinaryString(_), Value::Class(name)) => name.as_str() == "BinaryString",
            (Value::Array(_), Value::Class(name)) => {
                matches!(name.as_str(), "Array" | "Collection" | "Object")
            }
            (Value::Set(_), Value::Class(name)) => {
                matches!(name.as_str(), "Set" | "Collection" | "Object")
            }
            (Value::Bag(_), Value::Class(name)) => {
                matches!(name.as_str(), "Bag" | "Collection" | "Object")
            }
            (Value::Dict(_), Value::Class(name)) => {
                matches!(name.as_str(), "Dict" | "Collection" | "Object")
            }
            (Value::PairList(_), Value::Class(name)) => name.as_str() == "PairList",
            (Value::Pair(_, _), Value::Class(name)) => name.as_str() == "Pair",
            (Value::Regex(_, _), Value::Class(name)) => name.as_str() == "Regexp",
            (Value::Object(object), Value::Class(name)) => {
                name.as_str() == "Object"
                    || self.class_matches(&object.borrow().class, name)
                    || matches!(
                        (self.class_builtin_base_name(&object.borrow().class), name.as_str()),
                        (Some(base), "Collection")
                            if matches!(base.as_str(), "Array" | "Set" | "Bag" | "Dict")
                    )
            }
            (
                Value::Function(_) | Value::NativeFunction(_) | Value::Iterator(_),
                Value::Class(name),
            ) => name.as_str() == "Function",
            (Value::Task(_), Value::Class(name)) => name.as_str() == "Task",
            (Value::Channel(_), Value::Class(name)) => name.as_str() == "Channel",
            (Value::CancellationSource(_), Value::Class(name)) => {
                name.as_str() == "CancellationSource"
            }
            (Value::CancellationToken(_), Value::Class(name)) => {
                name.as_str() == "CancellationToken"
            }
            _ => false,
        }
    }

    fn value_does_trait(&self, value: &Value, trait_value: &Value) -> bool {
        let value = self.deref_value(value).unwrap_or_else(|_| value.clone());
        let trait_value = self
            .deref_value(trait_value)
            .unwrap_or_else(|_| trait_value.clone());
        let trait_name = match &trait_value {
            Value::Trait(trait_value) => trait_value.name.as_str(),
            Value::UserClass(class) => class.name.as_str(),
            Value::Class(name) => name.as_str(),
            _ => return false,
        };
        match &value {
            Value::Object(object) => self.class_has_trait(&object.borrow().class, trait_name),
            _ => false,
        }
    }

    fn value_can_method(&self, value: &Value, name: &str) -> bool {
        let value = self.deref_value(value).unwrap_or_else(|_| value.clone());
        match &value {
            Value::Class(class_name) => stdlib::has_builtin_class_method(class_name, name),
            Value::Object(object) => self.object_has_method(object, name),
            _ => false,
        }
    }

    fn class_matches(&self, class: &Rc<UserClassValue>, name: &str) -> bool {
        if class.name == name {
            return true;
        }
        match &class.base {
            Some(ClassBase::User(base)) => self.class_matches(base, name),
            Some(ClassBase::Builtin(base)) => base == name,
            None => false,
        }
    }

    fn class_matches_user(&self, class: &Rc<UserClassValue>, target: &Rc<UserClassValue>) -> bool {
        if Rc::ptr_eq(class, target) {
            return true;
        }
        if class.name == target.name
            && is_builtin_exception_class(&class.name)
            && is_builtin_exception_class(&target.name)
        {
            return true;
        }
        match &class.base {
            Some(ClassBase::User(base)) => self.class_matches_user(base, target),
            Some(ClassBase::Builtin(_)) | None => false,
        }
    }

    fn class_has_trait(&self, class: &Rc<UserClassValue>, name: &str) -> bool {
        if class
            .traits
            .iter()
            .any(|trait_value| trait_value.name == name)
        {
            return true;
        }
        match &class.base {
            Some(ClassBase::User(base)) => self.class_has_trait(base, name),
            None => false,
            Some(ClassBase::Builtin(_)) => false,
        }
    }

    fn class_decl_env(&self, class: &Rc<UserClassValue>) -> Rc<Environment> {
        if let Some(method) = class.methods.values().next() {
            return Rc::clone(&method.env);
        }
        if let Some(method) = class.static_methods.values().next() {
            return Rc::clone(&method.env);
        }
        if let Some(base) = &class.base {
            if let ClassBase::User(base) = base {
                return self.class_decl_env(base);
            }
        }
        if let Some(trait_value) = class.traits.first() {
            if let Some(method) = trait_value.methods.values().next() {
                return Rc::clone(&method.env);
            }
        }
        if let Some(class) = class.nested_classes.values().next() {
            return self.class_decl_env(class);
        }
        Rc::new(Environment::new(None))
    }

    fn find_method(&self, class: &Rc<UserClassValue>, name: &str) -> Option<Rc<MethodValue>> {
        if let Some(method) = class.methods.get(name) {
            return Some(Rc::clone(method));
        }
        for trait_value in &class.traits {
            if let Some(method) = trait_value.methods.get(name) {
                return Some(Rc::clone(method));
            }
        }
        match &class.base {
            Some(ClassBase::User(base)) => self.find_method(base, name),
            None => None,
            Some(ClassBase::Builtin(_)) => None,
        }
    }

    fn collect_method_candidates(
        &self,
        class: &Rc<UserClassValue>,
        name: &str,
        is_static: bool,
        candidates: &mut Vec<Rc<MethodValue>>,
    ) {
        if is_static {
            if let Some(method) = class.static_methods.get(name) {
                candidates.push(Rc::clone(method));
            }
        } else {
            if let Some(method) = class.methods.get(name) {
                candidates.push(Rc::clone(method));
            }
            for trait_value in &class.traits {
                if let Some(method) = trait_value.methods.get(name) {
                    candidates.push(Rc::clone(method));
                }
            }
        }

        if let Some(ClassBase::User(base)) = &class.base {
            self.collect_method_candidates(base, name, is_static, candidates);
        }
    }

    fn find_next_method(
        &self,
        class: &Rc<UserClassValue>,
        name: &str,
        is_static: bool,
    ) -> Option<Rc<MethodValue>> {
        let current = self.current_method_stack.borrow().last().cloned()?;
        let mut candidates = Vec::new();
        self.collect_method_candidates(class, name, is_static, &mut candidates);
        for (index, candidate) in candidates.iter().enumerate() {
            if Rc::ptr_eq(candidate, &current) {
                return candidates.get(index + 1).cloned();
            }
        }
        None
    }

    fn find_static_method(
        &self,
        class: &Rc<UserClassValue>,
        name: &str,
    ) -> Option<Rc<MethodValue>> {
        if let Some(method) = class.static_methods.get(name) {
            return Some(Rc::clone(method));
        }
        match &class.base {
            Some(ClassBase::User(base)) => self.find_static_method(base, name),
            None => None,
            Some(ClassBase::Builtin(_)) => None,
        }
    }

    fn class_builtin_base_name(&self, class: &Rc<UserClassValue>) -> Option<String> {
        match &class.base {
            Some(ClassBase::Builtin(name)) => Some(name.clone()),
            Some(ClassBase::User(base)) => self.class_builtin_base_name(base),
            None => None,
        }
    }

    fn object_builtin_value(&self, object: &Rc<RefCell<ObjectValue>>) -> Option<Value> {
        object.borrow().builtin_value.clone()
    }

    fn object_builtin_collection_contains(
        &self,
        object: &Rc<RefCell<ObjectValue>>,
        needle: &Value,
    ) -> bool {
        match self.object_builtin_value(object) {
            Some(value) => collection_contains(&value, needle),
            None => false,
        }
    }

    fn object_builtin_truthy(&self, object: &Rc<RefCell<ObjectValue>>) -> Option<bool> {
        self.object_builtin_value(object)
            .map(|value| value.is_truthy())
    }

    fn object_builtin_render(&self, object: &Rc<RefCell<ObjectValue>>) -> Result<Option<String>> {
        match self.object_builtin_value(object) {
            Some(value) => Ok(Some(self.render_value(&value)?)),
            None => Ok(None),
        }
    }

    fn collect_field_specs(&self, class: &Rc<UserClassValue>) -> Vec<FieldSpec> {
        let mut fields = match &class.base {
            Some(ClassBase::User(base)) => self.collect_field_specs(base),
            None => Vec::new(),
            Some(ClassBase::Builtin(_)) => Vec::new(),
        };
        fields.extend(class.fields.iter().cloned());
        fields
    }

    fn find_field_spec<'a>(&self, class: &'a Rc<UserClassValue>, name: &str) -> Option<FieldSpec> {
        self.collect_field_specs(class)
            .into_iter()
            .find(|field| field.name == name)
    }

    fn object_has_method(&self, object: &Rc<RefCell<ObjectValue>>, name: &str) -> bool {
        let object_ref = object.borrow();
        if self.class_matches(&object_ref.class, "Exception")
            && matches!(name, "to_String" | "message")
        {
            return true;
        }
        if self.find_method(&object_ref.class, name).is_some() {
            return true;
        }
        if stdlib::has_builtin_object_method(&object_ref.class.name, name) {
            return true;
        }
        self.find_field_spec(&object_ref.class, name.strip_prefix("get_").unwrap_or(""))
            .map(|field| {
                name.starts_with("get_") && field.accessors.iter().any(|item| item == "get")
            })
            .unwrap_or(false)
            || self
                .find_field_spec(&object_ref.class, name.strip_prefix("set_").unwrap_or(""))
                .map(|field| {
                    name.starts_with("set_") && field.accessors.iter().any(|item| item == "set")
                })
                .unwrap_or(false)
            || self
                .find_field_spec(&object_ref.class, name.strip_prefix("clear_").unwrap_or(""))
                .map(|field| {
                    name.starts_with("clear_") && field.accessors.iter().any(|item| item == "clear")
                })
                .unwrap_or(false)
            || self
                .find_field_spec(&object_ref.class, name.strip_prefix("has_").unwrap_or(""))
                .map(|field| {
                    name.starts_with("has_") && field.accessors.iter().any(|item| item == "has")
                })
                .unwrap_or(false)
    }

    fn call_object_method(
        &self,
        object: &Rc<RefCell<ObjectValue>>,
        name: &str,
        args: &[Value],
        named_args: Vec<(String, Value)>,
    ) -> Result<Value> {
        let class = Rc::clone(&object.borrow().class);
        if let Some(field_name) = name.strip_prefix("get_") {
            if let Some(field) = self.find_field_spec(&class, field_name) {
                if field.accessors.iter().any(|item| item == "get") {
                    reject_named_args(name, &named_args)?;
                    require_arity(name, args, 0)?;
                    return Ok(object
                        .borrow()
                        .fields
                        .get(field_name)
                        .cloned()
                        .unwrap_or(Value::Null));
                }
            }
        }
        if let Some(field_name) = name.strip_prefix("set_") {
            if let Some(field) = self.find_field_spec(&class, field_name) {
                if field.accessors.iter().any(|item| item == "set") {
                    reject_named_args(name, &named_args)?;
                    require_arity(name, args, 1)?;
                    self.object_set_field(object, &field, args[0].clone())?;
                    return Ok(Value::Object(Rc::clone(object)));
                }
            }
        }
        if let Some(field_name) = name.strip_prefix("clear_") {
            if let Some(field) = self.find_field_spec(&class, field_name) {
                if field.accessors.iter().any(|item| item == "clear") {
                    reject_named_args(name, &named_args)?;
                    require_arity(name, args, 0)?;
                    self.object_set_field(object, &field, Value::Null)?;
                    return Ok(Value::Object(Rc::clone(object)));
                }
            }
        }
        if let Some(field_name) = name.strip_prefix("has_") {
            if let Some(field) = self.find_field_spec(&class, field_name) {
                if field.accessors.iter().any(|item| item == "has") {
                    reject_named_args(name, &named_args)?;
                    require_arity(name, args, 0)?;
                    let has_value = object
                        .borrow()
                        .fields
                        .get(field_name)
                        .map(|value| !matches!(value.resolve_weak_value(), Value::Null))
                        .unwrap_or(false);
                    return Ok(Value::Boolean(has_value));
                }
            }
        }

        let Some(method) = self.find_method(&class, name) else {
            if self.class_matches(&class, "Exception") {
                reject_named_args(name, &named_args)?;
                match name {
                    "message" => {
                        require_arity(name, args, 0)?;
                        return Ok(object
                            .borrow()
                            .fields
                            .get("message")
                            .cloned()
                            .unwrap_or(Value::Null));
                    }
                    "to_String" => {
                        require_arity(name, args, 0)?;
                        let message = object
                            .borrow()
                            .fields
                            .get("message")
                            .cloned()
                            .unwrap_or(Value::Null);
                        let message = self.render_value(&message)?;
                        let class_name = class.name.as_str();
                        if message == class_name || message.starts_with(&format!("{class_name}:")) {
                            return Ok(Value::String(message));
                        }
                        if message.is_empty() {
                            return Ok(Value::String(class.name.clone()));
                        }
                        return Ok(Value::String(format!("{}: {message}", class.name)));
                    }
                    _ => {}
                }
            }
            if let Some(mut builtin_value) = self.object_builtin_value(object) {
                if let Some(result) = stdlib::call_builtin_object_method(
                    self,
                    object,
                    &class.name,
                    &builtin_value,
                    name,
                    args,
                ) {
                    return result;
                }
                return self.call_method_named(&mut builtin_value, name, args, named_args);
            }
            return Err(ZuzuRustError::thrown(format!(
                "unsupported method '{}' for {}",
                name, class.name
            )));
        };

        self.invoke_object_method_with_context(object, class, name, &method, args, named_args)
    }

    fn object_set_field(
        &self,
        object: &Rc<RefCell<ObjectValue>>,
        field: &FieldSpec,
        value: Value,
    ) -> Result<()> {
        self.object_set_field_with_weak_write(object, field, value, false)
    }

    fn object_set_field_with_weak_write(
        &self,
        object: &Rc<RefCell<ObjectValue>>,
        field: &FieldSpec,
        value: Value,
        weak_write: bool,
    ) -> Result<()> {
        let is_weak_storage = field.is_weak_storage || weak_write;
        if matches!(value, Value::Null) && field.declared_type.is_some() {
            let mut object = object.borrow_mut();
            if is_weak_storage {
                object.weak_fields.insert(field.name.clone());
            } else {
                object.weak_fields.remove(&field.name);
            }
            object.fields.insert(field.name.clone(), Value::Null);
            return Ok(());
        }
        self.assert_declared_type(field.declared_type.as_deref(), &value, &field.name)?;
        let mut object = object.borrow_mut();
        if is_weak_storage {
            object.weak_fields.insert(field.name.clone());
        } else {
            object.weak_fields.remove(&field.name);
        }
        object.fields.insert(
            field.name.clone(),
            value.stored_with_weak_policy(is_weak_storage),
        );
        Ok(())
    }

    fn eval_do_expression(&self, body: &BlockStatement, env: Rc<Environment>) -> Result<Value> {
        let block_env = if body.needs_lexical_scope {
            Rc::new(Environment::new(Some(env)))
        } else {
            env
        };
        self.push_special_props_scope();
        let mut last = Value::Null;
        for statement in &body.statements {
            if let Some(value) =
                self.eval_statement_value_in_do(statement, Rc::clone(&block_env))?
            {
                last = value;
            }
            if !matches!(self.get_special_prop("__do_return__"), Value::Null) {
                break;
            }
        }
        if body.needs_lexical_scope {
            self.cleanup_scope(&block_env)?;
        }
        self.pop_special_props_scope();
        Ok(last)
    }

    fn eval_statement_value_in_do(
        &self,
        statement: &Statement,
        env: Rc<Environment>,
    ) -> Result<Option<Value>> {
        match statement {
            Statement::ExpressionStatement(node) => {
                Ok(Some(self.eval_expression(&node.expression, env)?))
            }
            Statement::Block(block) => Ok(Some(self.eval_do_expression(block, env)?)),
            Statement::IfStatement(node) => {
                if self.value_is_truthy(&self.eval_expression(&node.test, Rc::clone(&env))?)? {
                    Ok(Some(self.eval_do_expression(&node.consequent, env)?))
                } else if let Some(alternate) = &node.alternate {
                    self.eval_statement_value_in_do(alternate, env)
                } else {
                    Ok(Some(Value::Null))
                }
            }
            other => match self.eval_statement(other, env)? {
                ControlFlow::Normal => Ok(None),
                ControlFlow::Return(value) => {
                    self.set_special_prop_at_level(1, "__do_return__", value.clone());
                    Ok(Some(value))
                }
                ControlFlow::Throw(value) => Err(ZuzuRustError::thrown(self.render_value(&value)?)),
                ControlFlow::Continue | ControlFlow::Break => {
                    Err(ZuzuRustError::runtime("loop control escaped do expression"))
                }
            },
        }
    }

    fn eval_try_expression(
        &self,
        body: &BlockStatement,
        handlers: &[crate::ast::CatchClause],
        env: Rc<Environment>,
    ) -> Result<Value> {
        match self.eval_do_expression(body, Rc::clone(&env)) {
            Ok(value) => Ok(value),
            Err(ZuzuRustError::Thrown { value, token }) => {
                let thrown_value = self.lookup_thrown_value(token.as_deref());
                for handler in handlers {
                    if !self.catch_clause_matches(
                        handler.binding.as_ref(),
                        &value,
                        thrown_value.as_ref(),
                    ) {
                        continue;
                    }
                    let catch_env = Rc::new(Environment::new(Some(Rc::clone(&env))));
                    let binding_name = handler
                        .binding
                        .as_ref()
                        .and_then(|binding| binding.name.clone())
                        .unwrap_or_else(|| "e".to_owned());
                    let caught_value = self.make_catch_binding_value(
                        handler.binding.as_ref(),
                        &value,
                        thrown_value.as_ref(),
                    );
                    catch_env.define(binding_name, caught_value, true);
                    return self.eval_do_expression(&handler.body, catch_env);
                }
                match token {
                    Some(token) => Err(ZuzuRustError::thrown_with_token(value, token)),
                    None => Err(ZuzuRustError::thrown(value)),
                }
            }
            Err(err) => Err(err),
        }
    }

    fn cleanup_scope(&self, env: &Rc<Environment>) -> Result<()> {
        let values = env
            .bindings
            .borrow()
            .values()
            .map(|binding| binding.value.clone())
            .collect::<Vec<_>>();
        for value in values {
            if let Value::Object(object) = value {
                if Rc::strong_count(&object) > 2 {
                    continue;
                }
                if self.object_has_method(&object, "__demolish__") {
                    let _ = self.call_object_method(&object, "__demolish__", &[], Vec::new())?;
                }
            }
        }
        Ok(())
    }

    fn make_catch_binding_value(
        &self,
        binding: Option<&crate::ast::CatchBinding>,
        thrown_value: &str,
        thrown_runtime_value: Option<&Value>,
    ) -> Value {
        if let Some(value) = thrown_runtime_value {
            return value.clone();
        }
        let declared_type = binding.and_then(|item| item.declared_type.as_deref());
        match declared_type {
            Some("Exception")
            | Some("BailOutException")
            | Some("TypeException")
            | Some("CancelledException")
            | Some("TimeoutException")
            | Some("ChannelClosedException")
            | Some("MarshallingException")
            | Some("UnmarshallingException")
            | Some("ExhaustedException") => {
                let class_name = inferred_exception_class(thrown_value)
                    .or(declared_type)
                    .unwrap_or("Exception");
                let class_value = if class_name == "Exception" {
                    Value::UserClass(Rc::new(UserClassValue {
                        name: "Exception".to_owned(),
                        base: None,
                        traits: Vec::new(),
                        fields: exception_field_specs(),
                        methods: HashMap::new(),
                        static_methods: HashMap::new(),
                        nested_classes: HashMap::new(),
                        source_decl: None,
                        closure_env: None,
                    }))
                } else {
                    Value::UserClass(Rc::new(UserClassValue {
                        name: class_name.to_owned(),
                        base: Some(ClassBase::User(Rc::new(UserClassValue {
                            name: "Exception".to_owned(),
                            base: None,
                            traits: Vec::new(),
                            fields: exception_field_specs(),
                            methods: HashMap::new(),
                            static_methods: HashMap::new(),
                            nested_classes: HashMap::new(),
                            source_decl: None,
                            closure_env: None,
                        }))),
                        traits: Vec::new(),
                        fields: Vec::new(),
                        methods: HashMap::new(),
                        static_methods: HashMap::new(),
                        nested_classes: HashMap::new(),
                        source_decl: None,
                        closure_env: None,
                    }))
                };
                let mut message = thrown_value.to_owned();
                if let Some(stripped) = thrown_value
                    .strip_prefix(&format!("{class_name}: "))
                    .or_else(|| thrown_value.strip_prefix("Exception: "))
                    .or_else(|| thrown_value.strip_prefix("Bail out! "))
                {
                    message = stripped.to_owned();
                }
                match class_value {
                    Value::UserClass(class) => Value::Object(Rc::new(RefCell::new(ObjectValue {
                        class,
                        fields: HashMap::from([
                            ("message".to_owned(), Value::String(message)),
                            ("file".to_owned(), Value::String("<unknown>".to_owned())),
                            ("line".to_owned(), Value::Number(1.0)),
                            ("code".to_owned(), Value::Null),
                        ]),
                        weak_fields: HashSet::new(),
                        builtin_value: None,
                    }))),
                    _ => Value::String(thrown_value.to_owned()),
                }
            }
            _ => Value::String(thrown_value.to_owned()),
        }
    }

    fn annotate_exception_metadata(&self, value: &Value, source_file: Option<&str>, line: usize) {
        let resolved = self.deref_value(value).unwrap_or_else(|_| value.clone());
        let Value::Object(object) = resolved else {
            return;
        };
        if !self.class_matches(&object.borrow().class, "Exception") {
            return;
        }
        let mut object = object.borrow_mut();
        if matches!(object.fields.get("file"), None | Some(Value::Null)) {
            let file = source_file.unwrap_or("<unknown>");
            object
                .fields
                .insert("file".to_owned(), Value::String(file.to_owned()));
        }
        if matches!(object.fields.get("line"), None | Some(Value::Null)) {
            object
                .fields
                .insert("line".to_owned(), Value::Number(line as f64));
        }
    }

    fn normalize_die_value(
        &self,
        value: Value,
        source_file: Option<&str>,
        line: usize,
    ) -> Result<Value> {
        let resolved = self.deref_value(&value).unwrap_or_else(|_| value.clone());
        if let Value::Object(object) = &resolved {
            if self.class_matches(&object.borrow().class, "Exception") {
                self.annotate_exception_metadata(&value, source_file, line);
            }
            return Ok(value);
        }

        let message = self.render_value(&value)?;
        Ok(self.make_exception_object(message, source_file, line))
    }

    fn make_exception_object(
        &self,
        message: String,
        source_file: Option<&str>,
        line: usize,
    ) -> Value {
        self.make_exception_object_with_code(message, source_file, line, Value::Null)
    }

    pub(in crate::runtime) fn make_exception_object_with_code(
        &self,
        message: String,
        source_file: Option<&str>,
        line: usize,
        code: Value,
    ) -> Value {
        Value::Object(Rc::new(RefCell::new(ObjectValue {
            class: Rc::new(UserClassValue {
                name: "Exception".to_owned(),
                base: None,
                traits: Vec::new(),
                fields: exception_field_specs(),
                methods: HashMap::new(),
                static_methods: HashMap::new(),
                nested_classes: HashMap::new(),
                source_decl: None,
                closure_env: None,
            }),
            fields: HashMap::from([
                ("message".to_owned(), Value::String(message)),
                (
                    "file".to_owned(),
                    Value::String(source_file.unwrap_or("<unknown>").to_owned()),
                ),
                ("line".to_owned(), Value::Number(line as f64)),
                ("code".to_owned(), code),
            ]),
            weak_fields: HashSet::new(),
            builtin_value: None,
        })))
    }

    fn assert_declared_type(
        &self,
        declared_type: Option<&str>,
        value: &Value,
        name: &str,
    ) -> Result<()> {
        let declared_type = declared_type.unwrap_or("Any");
        if declared_type == "Any" || self.value_matches_declared_type(declared_type, value) {
            return Ok(());
        }
        Err(ZuzuRustError::thrown(format!(
            "TypeException: '{name}' must be {declared_type}, got {}",
            self.typeof_name(value)
        )))
    }

    fn assert_return_type(
        &self,
        declared_type: Option<&str>,
        name: Option<&str>,
        value: &Value,
    ) -> Result<()> {
        let Some(name) = name else {
            return self.assert_declared_type(declared_type, value, "return value");
        };
        self.assert_declared_type(declared_type, value, &format!("return value of '{name}'"))
    }

    fn value_matches_declared_type(&self, declared_type: &str, value: &Value) -> bool {
        let resolved = match self.normalize_value(value.clone()) {
            Ok(value) => value,
            Err(_) => return false,
        };
        match declared_type {
            "Any" => true,
            "Class" => matches!(resolved, Value::Class(_) | Value::UserClass(_)),
            "Number" => resolved.type_name() == "Number",
            "String" => resolved.type_name() == "String",
            "BinaryString" => resolved.type_name() == "BinaryString",
            "Boolean" => resolved.type_name() == "Boolean",
            "Null" => resolved.type_name() == "Null",
            "Object" => matches!(
                resolved,
                Value::Array(_) | Value::Set(_) | Value::Bag(_) | Value::Dict(_) | Value::Object(_)
            ),
            "Collection" => matches!(
                resolved,
                Value::Array(_) | Value::Set(_) | Value::Bag(_) | Value::Dict(_)
            ),
            "Array" => matches!(resolved, Value::Array(_)),
            "Set" => matches!(resolved, Value::Set(_)),
            "Bag" => matches!(resolved, Value::Bag(_)),
            "Dict" => matches!(resolved, Value::Dict(_)),
            "PairList" => matches!(resolved, Value::PairList(_)),
            "Pair" => matches!(resolved, Value::Pair(_, _)),
            "Regexp" => matches!(resolved, Value::Regex(_, _)),
            "Function" => matches!(
                resolved,
                Value::Function(_) | Value::NativeFunction(_) | Value::Iterator(_)
            ),
            other => match &resolved {
                Value::Object(object) => self.class_matches(&object.borrow().class, other),
                Value::UserClass(class) => self.class_matches(class, other),
                _ => self.typeof_name(&resolved) == other,
            },
        }
    }

    fn compile_regex(&self, pattern: &str, flags: &str) -> Result<Regex> {
        if !self.optimizations.enables(OptimizationPass::RegexCache) {
            return RegexBuilder::new(pattern)
                .case_insensitive(flags.contains('i'))
                .build()
                .map_err(|err| ZuzuRustError::runtime(format!("invalid regexp: {err}")));
        }
        let key = (pattern.to_owned(), flags.to_owned());
        if let Some(regex) = self.regex_cache.borrow().get(&key) {
            return Ok(regex.clone());
        }
        RegexBuilder::new(pattern)
            .case_insensitive(flags.contains('i'))
            .build()
            .map_err(|err| ZuzuRustError::runtime(format!("invalid regexp: {err}")))
            .map(|regex| {
                self.regex_cache.borrow_mut().insert(key, regex.clone());
                regex
            })
    }

    fn coerce_regex_operand(&self, regex: &Value) -> Result<(String, String)> {
        let regex = self.deref_value(regex)?;
        match &regex {
            Value::Regex(pattern, flags) => Ok((pattern.clone(), flags.clone())),
            value => Ok((self.value_to_operator_string(value)?, String::new())),
        }
    }

    fn eval_regex_match(&self, target: &str, regex: &Value) -> Result<Value> {
        let (pattern, flags) = self.coerce_regex_operand(regex)?;
        let compiled = self.compile_regex(&pattern, &flags)?;
        if flags.contains('g') {
            let mut all = Vec::new();
            for captures in compiled.captures_iter(target) {
                let groups = captures
                    .iter()
                    .map(|item| Value::String(item.map(|m| m.as_str()).unwrap_or("").to_owned()))
                    .collect();
                all.push(Value::Array(groups));
            }
            if all.is_empty() {
                Ok(Value::Boolean(false))
            } else {
                Ok(Value::Array(all))
            }
        } else if let Some(captures) = compiled.captures(target) {
            Ok(Value::Array(
                captures
                    .iter()
                    .map(|item| Value::String(item.map(|m| m.as_str()).unwrap_or("").to_owned()))
                    .collect(),
            ))
        } else {
            Ok(Value::Boolean(false))
        }
    }
}

impl ReplSession<'_> {
    pub fn eval_source(&self, source: &str) -> Result<ReplEvalResult> {
        self.runtime
            .async_executor
            .enter(|| self.eval_source_inner(source))
    }

    fn eval_source_inner(&self, source: &str) -> Result<ReplEvalResult> {
        *self.runtime.output.borrow_mut() = ExecutionOutput::default();
        let mut special_props = self.runtime.special_props.borrow_mut();
        special_props.clear();
        special_props.push(HashMap::new());
        drop(special_props);
        self.runtime.thrown_values.borrow_mut().clear();
        *self.runtime.next_thrown_id.borrow_mut() = 0;
        self.runtime.path_line_cursors.borrow_mut().clear();

        let options = ParseOptions::new(
            false,
            self.runtime.infer_types,
            self.runtime.optimizations.clone(),
        );
        let program = parse_program_with_compile_options(source, &options)?;
        let value = self
            .runtime
            .eval_repl_program(&program, Rc::clone(&self.env))?;
        Ok(ReplEvalResult {
            output: self.runtime.output.borrow().clone(),
            value: self.runtime.render_repl_value(&value)?,
        })
    }
}

impl Environment {
    fn new(parent: Option<Rc<Environment>>) -> Self {
        Self {
            parent,
            bindings: RefCell::new(HashMap::new()),
        }
    }

    fn define(&self, name: String, value: Value, mutable: bool) {
        self.define_with_storage(name, value, mutable, false);
    }

    fn define_with_storage(
        &self,
        name: String,
        value: Value,
        mutable: bool,
        is_weak_storage: bool,
    ) {
        self.bindings.borrow_mut().insert(
            name,
            Binding {
                value: value.stored_with_weak_policy(is_weak_storage),
                mutable,
                is_weak_storage,
            },
        );
    }

    fn get(&self, name: &str) -> Result<Value> {
        if let Some(binding) = self.bindings.borrow().get(name) {
            return Ok(binding.value.clone());
        }
        if let Some(parent) = &self.parent {
            return parent.get(name);
        }
        Err(ZuzuRustError::runtime(format!(
            "use of undeclared identifier '{}'",
            name
        )))
    }

    fn get_at(&self, depth: usize, name: &str) -> Result<Value> {
        if depth == 0 {
            return self
                .bindings
                .borrow()
                .get(name)
                .map(|binding| binding.value.clone())
                .ok_or_else(|| {
                    ZuzuRustError::runtime(format!("use of undeclared identifier '{}'", name))
                });
        }
        let Some(parent) = &self.parent else {
            return self.get(name);
        };
        parent.get_at(depth - 1, name)
    }

    fn get_resolved(&self, depth: Option<usize>, name: &str) -> Result<Value> {
        match depth {
            Some(depth) => self.get_at(depth, name).or_else(|_| self.get(name)),
            None => self.get(name),
        }
    }

    fn get_optional(&self, name: &str) -> Option<Value> {
        if let Some(binding) = self.bindings.borrow().get(name) {
            return Some(binding.value.clone());
        }
        self.parent
            .as_ref()
            .and_then(|parent| parent.get_optional(name))
    }

    fn find_binding(&self, name: &str) -> Option<Binding> {
        if let Some(binding) = self.bindings.borrow().get(name) {
            return Some(binding.clone());
        }
        self.parent
            .as_ref()
            .and_then(|parent| parent.find_binding(name))
    }

    fn assign(&self, name: &str, value: Value) -> Result<()> {
        self.assign_with_weak_write(name, value, false)
    }

    fn assign_with_weak_write(&self, name: &str, value: Value, weak_write: bool) -> Result<()> {
        let mut bindings = self.bindings.borrow_mut();
        if let Some(binding) = bindings.get_mut(name) {
            if !binding.mutable {
                return Err(ZuzuRustError::runtime(format!(
                    "cannot assign to immutable binding '{}'",
                    name
                )));
            }
            binding.value = value.stored_with_weak_policy(binding.is_weak_storage || weak_write);
            return Ok(());
        }
        drop(bindings);
        if let Some(parent) = &self.parent {
            return parent.assign_with_weak_write(name, value, weak_write);
        }
        Err(ZuzuRustError::runtime(format!(
            "assignment to undeclared identifier '{}'",
            name
        )))
    }

    fn assign_at_with_weak_write(
        &self,
        depth: usize,
        name: &str,
        value: Value,
        weak_write: bool,
    ) -> Result<()> {
        if depth == 0 {
            let mut bindings = self.bindings.borrow_mut();
            if let Some(binding) = bindings.get_mut(name) {
                if !binding.mutable {
                    return Err(ZuzuRustError::runtime(format!(
                        "cannot assign to immutable binding '{}'",
                        name
                    )));
                }
                binding.value =
                    value.stored_with_weak_policy(binding.is_weak_storage || weak_write);
                return Ok(());
            }
            return Err(ZuzuRustError::runtime(format!(
                "assignment to undeclared identifier '{}'",
                name
            )));
        }
        let Some(parent) = &self.parent else {
            return self.assign_with_weak_write(name, value, weak_write);
        };
        parent.assign_at_with_weak_write(depth - 1, name, value, weak_write)
    }

    fn assign_resolved(&self, depth: Option<usize>, name: &str, value: Value) -> Result<()> {
        self.assign_resolved_with_weak_write(depth, name, value, false)
    }

    fn assign_resolved_with_weak_write(
        &self,
        depth: Option<usize>,
        name: &str,
        value: Value,
        weak_write: bool,
    ) -> Result<()> {
        match depth {
            Some(depth) => self
                .assign_at_with_weak_write(depth, name, value.clone(), weak_write)
                .or_else(|_| self.assign_with_weak_write(name, value, weak_write)),
            None => self.assign_with_weak_write(name, value, weak_write),
        }
    }

    fn refresh_existing_binding(&self, name: &str, value: Value) -> bool {
        let mut bindings = self.bindings.borrow_mut();
        if let Some(binding) = bindings.get_mut(name) {
            binding.value = value.stored_with_weak_policy(binding.is_weak_storage);
            return true;
        }
        drop(bindings);
        if let Some(parent) = &self.parent {
            return parent.refresh_existing_binding(name, value);
        }
        false
    }

    fn export_public_aliases(&self, env: Rc<Environment>) -> HashMap<String, Value> {
        self.bindings
            .borrow()
            .iter()
            .filter(|(name, _)| !name.starts_with("__"))
            .map(|(name, binding)| {
                let value = if binding.mutable {
                    Value::AliasRef(Rc::new(LvalueRef::Expression {
                        env: Rc::clone(&env),
                        target: Expression::Identifier {
                            line: 0,
                            source_file: None,
                            name: name.clone(),
                            inferred_type: None,
                            binding_depth: None,
                        },
                    }))
                } else {
                    binding.value.clone()
                };
                (name.clone(), value)
            })
            .collect()
    }
}

impl Value {
    pub(in crate::runtime) fn builtin_class(name: impl Into<String>) -> Value {
        Value::Class(Rc::new(name.into()))
    }

    pub(in crate::runtime) fn native_function(name: impl Into<String>) -> Value {
        Value::NativeFunction(Rc::new(name.into()))
    }

    pub(in crate::runtime) fn stored_with_weak_policy(self, is_weak_storage: bool) -> Value {
        if is_weak_storage {
            let value = self
                .make_weak_value()
                .expect("weak storage conversion should not fail")
                .into_shared_if_composite();
            if value.is_weak_value() {
                value
            } else {
                Value::WeakStoredScalar(Box::new(value))
            }
        } else {
            self.into_shared_if_composite()
        }
    }

    fn is_weak_value(&self) -> bool {
        matches!(
            self,
            Value::WeakFunction(_)
                | Value::WeakNativeFunction(_)
                | Value::WeakMethod(_)
                | Value::WeakIterator(_)
                | Value::WeakClass(_)
                | Value::WeakUserClass(_)
                | Value::WeakTrait(_)
                | Value::WeakObject(_)
                | Value::WeakTask(_)
                | Value::WeakChannel(_)
                | Value::WeakCancellationSource(_)
                | Value::WeakCancellationToken(_)
                | Value::WeakShared(_)
                | Value::WeakRef(_)
                | Value::WeakAliasRef(_)
                | Value::WeakStoredScalar(_)
        )
    }

    fn into_shared_if_composite(self) -> Value {
        match self {
            Value::Shared(value) => Value::Shared(value),
            Value::Array(values) => Value::Shared(Rc::new(RefCell::new(Value::Array(
                values
                    .into_iter()
                    .map(Value::into_shared_if_composite)
                    .collect(),
            )))),
            Value::Set(values) => Value::Shared(Rc::new(RefCell::new(Value::Set(
                values
                    .into_iter()
                    .map(Value::into_shared_if_composite)
                    .collect(),
            )))),
            Value::Bag(values) => Value::Shared(Rc::new(RefCell::new(Value::Bag(
                values
                    .into_iter()
                    .map(Value::into_shared_if_composite)
                    .collect(),
            )))),
            Value::Dict(values) => Value::Shared(Rc::new(RefCell::new(Value::Dict(
                values
                    .into_iter()
                    .map(|(key, value)| (key, value.into_shared_if_composite()))
                    .collect(),
            )))),
            Value::PairList(values) => Value::Shared(Rc::new(RefCell::new(Value::PairList(
                values
                    .into_iter()
                    .map(|(key, value)| (key, value.into_shared_if_composite()))
                    .collect(),
            )))),
            Value::Pair(key, value) => Value::Shared(Rc::new(RefCell::new(Value::Pair(
                key,
                Box::new((*value).into_shared_if_composite()),
            )))),
            other => other,
        }
    }

    fn numeric_string_value(&self) -> Option<f64> {
        match self {
            Value::String(value) => value.parse::<f64>().ok(),
            _ => None,
        }
    }

    fn is_numeric_comparable(&self) -> bool {
        matches!(self, Value::Null | Value::Boolean(_) | Value::Number(_))
            || self.numeric_string_value().is_some()
    }

    fn scalar_type_name(&self) -> Option<&'static str> {
        match self {
            Value::Null => Some("Null"),
            Value::Boolean(_) => Some("Boolean"),
            Value::Number(_) => Some("Number"),
            Value::String(_) => Some("String"),
            Value::BinaryString(_) => Some("BinaryString"),
            Value::Regex(_, _) => Some("Regexp"),
            Value::Shared(value) => value.borrow().scalar_type_name(),
            _ => None,
        }
    }

    #[allow(dead_code)]
    fn is_weakable_value(&self) -> bool {
        if self.is_weak_value() {
            return true;
        }
        self.scalar_type_name().is_none()
    }

    #[allow(dead_code)]
    fn make_weak_value(&self) -> Result<Value> {
        if !self.is_weakable_value() {
            return Ok(self.clone());
        }
        let value = self.clone().into_shared_if_composite();
        Ok(match value {
            Value::Function(value) => Value::WeakFunction(Rc::downgrade(&value)),
            Value::NativeFunction(value) => Value::WeakNativeFunction(Rc::downgrade(&value)),
            Value::Method(value) => Value::WeakMethod(Rc::downgrade(&value)),
            Value::Iterator(value) => Value::WeakIterator(Rc::downgrade(&value)),
            Value::Class(value) => Value::WeakClass(Rc::downgrade(&value)),
            Value::UserClass(value) => Value::WeakUserClass(Rc::downgrade(&value)),
            Value::Trait(value) => Value::WeakTrait(Rc::downgrade(&value)),
            Value::Object(value) => Value::WeakObject(Rc::downgrade(&value)),
            Value::Task(value) => Value::WeakTask(Rc::downgrade(&value)),
            Value::Channel(value) => Value::WeakChannel(Rc::downgrade(&value)),
            Value::CancellationSource(value) => {
                Value::WeakCancellationSource(Rc::downgrade(&value))
            }
            Value::CancellationToken(value) => Value::WeakCancellationToken(Rc::downgrade(&value)),
            Value::Shared(value) => Value::WeakShared(Rc::downgrade(&value)),
            Value::Ref(value) => Value::WeakRef(Rc::downgrade(&value)),
            Value::AliasRef(value) => Value::WeakAliasRef(Rc::downgrade(&value)),
            value if value.is_weak_value() => value,
            scalar => scalar,
        })
    }

    #[allow(dead_code)]
    fn resolve_weak_value(&self) -> Value {
        match self {
            Value::WeakFunction(value) => {
                value.upgrade().map(Value::Function).unwrap_or(Value::Null)
            }
            Value::WeakNativeFunction(value) => value
                .upgrade()
                .map(Value::NativeFunction)
                .unwrap_or(Value::Null),
            Value::WeakMethod(value) => value.upgrade().map(Value::Method).unwrap_or(Value::Null),
            Value::WeakIterator(value) => {
                value.upgrade().map(Value::Iterator).unwrap_or(Value::Null)
            }
            Value::WeakClass(value) => value.upgrade().map(Value::Class).unwrap_or(Value::Null),
            Value::WeakUserClass(value) => {
                value.upgrade().map(Value::UserClass).unwrap_or(Value::Null)
            }
            Value::WeakTrait(value) => value.upgrade().map(Value::Trait).unwrap_or(Value::Null),
            Value::WeakObject(value) => value.upgrade().map(Value::Object).unwrap_or(Value::Null),
            Value::WeakTask(value) => value.upgrade().map(Value::Task).unwrap_or(Value::Null),
            Value::WeakChannel(value) => value.upgrade().map(Value::Channel).unwrap_or(Value::Null),
            Value::WeakCancellationSource(value) => value
                .upgrade()
                .map(Value::CancellationSource)
                .unwrap_or(Value::Null),
            Value::WeakCancellationToken(value) => value
                .upgrade()
                .map(Value::CancellationToken)
                .unwrap_or(Value::Null),
            Value::WeakShared(value) => value.upgrade().map(Value::Shared).unwrap_or(Value::Null),
            Value::WeakRef(value) => value.upgrade().map(Value::Ref).unwrap_or(Value::Null),
            Value::WeakAliasRef(value) => {
                value.upgrade().map(Value::AliasRef).unwrap_or(Value::Null)
            }
            Value::WeakStoredScalar(value) => (**value).clone(),
            value => value.clone(),
        }
    }

    fn is_truthy(&self) -> bool {
        if self.is_weak_value() {
            return self.resolve_weak_value().is_truthy();
        }
        match self {
            Value::Shared(value) => value.borrow().is_truthy(),
            Value::Null => false,
            Value::Boolean(value) => *value,
            Value::Number(value) => *value != 0.0,
            Value::String(value) => !value.is_empty(),
            Value::BinaryString(bytes) => !bytes.is_empty(),
            Value::Regex(_, _) => true,
            Value::Array(values) => !values.is_empty(),
            Value::Set(values) => !values.is_empty(),
            Value::Bag(values) => !values.is_empty(),
            Value::Dict(values) => !values.is_empty(),
            Value::PairList(values) => !values.is_empty(),
            Value::Pair(_, _)
            | Value::Function(_)
            | Value::NativeFunction(_)
            | Value::Method(_)
            | Value::Iterator(_)
            | Value::Class(_)
            | Value::UserClass(_)
            | Value::Trait(_)
            | Value::Object(_)
            | Value::Task(_)
            | Value::Channel(_)
            | Value::CancellationSource(_)
            | Value::CancellationToken(_)
            | Value::Ref(_)
            | Value::AliasRef(_) => true,
            _ => unreachable!("weak cases handled above"),
        }
    }

    fn to_number(&self) -> Result<f64> {
        if self.is_weak_value() {
            return self.resolve_weak_value().to_number();
        }
        match self {
            Value::Shared(value) => value.borrow().to_number(),
            Value::Null => Ok(0.0),
            Value::Number(value) => Ok(*value),
            Value::Boolean(true) => Ok(1.0),
            Value::Boolean(false) => Ok(0.0),
            Value::String(value) => value.parse::<f64>().map_err(|_| {
                ZuzuRustError::runtime(format!("cannot coerce '{}' to Number", value))
            }),
            _ => Err(ZuzuRustError::runtime("value cannot be coerced to Number")),
        }
    }

    fn render(&self) -> String {
        if self.is_weak_value() {
            return self.resolve_weak_value().render();
        }
        match self {
            Value::Shared(value) => value.borrow().render(),
            Value::Null => "".to_owned(),
            Value::Boolean(true) => "1".to_owned(),
            Value::Boolean(false) => "0".to_owned(),
            Value::Number(value) => render_number(*value),
            Value::String(value) => value.clone(),
            Value::BinaryString(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            Value::Regex(pattern, _) => pattern.clone(),
            Value::Array(values) => {
                let parts = values.iter().map(Value::render).collect::<Vec<_>>();
                format!("[{}]", parts.join(", "))
            }
            Value::Set(values) => {
                let parts = values.iter().map(Value::render).collect::<Vec<_>>();
                format!("<< {} >>", parts.join(", "))
            }
            Value::Bag(values) => {
                let parts = values.iter().map(Value::render).collect::<Vec<_>>();
                format!("<<< {} >>>", parts.join(", "))
            }
            Value::Dict(values) => {
                let mut parts = values
                    .iter()
                    .map(|(key, value)| format!("{key}: {}", value.render()))
                    .collect::<Vec<_>>();
                parts.sort();
                format!("{{{}}}", parts.join(", "))
            }
            Value::PairList(values) => {
                let parts = values
                    .iter()
                    .map(|(key, value)| format!("{key}: {}", value.render()))
                    .collect::<Vec<_>>();
                format!("{{{{{}}}}}", parts.join(", "))
            }
            Value::Pair(key, value) => format!("new Pair(pair:[\"{}\",{}])", key, value.render()),
            Value::Function(_) => "<Function>".to_owned(),
            Value::NativeFunction(name) => format!("<NativeFunction {}>", name.as_str()),
            Value::Method(method) => method.name.clone(),
            Value::Iterator(_) => "<Iterator>".to_owned(),
            Value::Class(name) => format!("<Class {}>", name.as_str()),
            Value::UserClass(class) => format!("<Class {}>", class.name),
            Value::Trait(trait_value) => format!("<Trait {}>", trait_value.name),
            Value::Object(object) => format!("<{} instance>", object.borrow().class.name),
            Value::Task(_) => "<Task>".to_owned(),
            Value::Channel(_) => "<Channel>".to_owned(),
            Value::CancellationSource(_) => "<CancellationSource>".to_owned(),
            Value::CancellationToken(_) => "<CancellationToken>".to_owned(),
            Value::Ref(_) | Value::AliasRef(_) => "<Ref>".to_owned(),
            _ => unreachable!("weak cases handled above"),
        }
    }

    fn type_name(&self) -> &'static str {
        if self.is_weak_value() {
            return self.resolve_weak_value().type_name();
        }
        if let Some(name) = self.scalar_type_name() {
            return name;
        }
        match self {
            Value::Shared(value) => value.borrow().type_name(),
            Value::Array(_) => "Array",
            Value::Set(_) => "Set",
            Value::Bag(_) => "Bag",
            Value::Dict(_) => "Dict",
            Value::PairList(_) => "PairList",
            Value::Pair(_, _) => "Pair",
            Value::Method(_) => "Method",
            Value::Function(_) | Value::NativeFunction(_) | Value::Iterator(_) => "Function",
            Value::Class(_) | Value::UserClass(_) => "Class",
            Value::Trait(_) => "Trait",
            Value::Object(_) => "Object",
            Value::Task(_) => "Task",
            Value::Channel(_) => "Channel",
            Value::CancellationSource(_) => "CancellationSource",
            Value::CancellationToken(_) => "CancellationToken",
            Value::Ref(_) | Value::AliasRef(_) => "Ref",
            Value::Null
            | Value::Boolean(_)
            | Value::Number(_)
            | Value::String(_)
            | Value::BinaryString(_)
            | Value::Regex(_, _) => {
                unreachable!("scalar cases handled above")
            }
            _ => unreachable!("weak cases handled above"),
        }
    }

    fn strict_eq(&self, other: &Value) -> bool {
        if self.is_weak_value() {
            return self.resolve_weak_value().strict_eq(other);
        }
        if other.is_weak_value() {
            return self.strict_eq(&other.resolve_weak_value());
        }
        match (self, other) {
            (Value::Shared(value), other) => return value.borrow().strict_eq(other),
            (this, Value::Shared(value)) => return this.strict_eq(&value.borrow()),
            _ => {}
        }
        match (self, other) {
            (Value::Null, Value::Null) => return true,
            (Value::Boolean(a), Value::Boolean(b)) => return a == b,
            (Value::Number(a), Value::Number(b)) => return a == b,
            (Value::String(a), Value::String(b)) => return a == b,
            (Value::BinaryString(a), Value::BinaryString(b)) => return a == b,
            _ => {}
        }
        match (self, other) {
            (Value::Array(a), Value::Array(b)) => sequence_eq(a, b),
            (Value::Set(a), Value::Set(b)) => collection_items_eq(a, b),
            (Value::Bag(a), Value::Bag(b)) => bag_items_eq(a, b),
            (Value::Pair(a_key, a_value), Value::Pair(b_key, b_value)) => {
                a_key == b_key && a_value.strict_eq(b_value)
            }
            (Value::Regex(a_pattern, a_flags), Value::Regex(b_pattern, b_flags)) => {
                a_pattern == b_pattern && a_flags == b_flags
            }
            (Value::Class(a), Value::Class(b)) => a == b,
            (Value::UserClass(a), Value::UserClass(b)) => Rc::ptr_eq(a, b),
            (Value::Trait(a), Value::Trait(b)) => Rc::ptr_eq(a, b),
            (Value::Function(a), Value::Function(b)) => Rc::ptr_eq(a, b),
            (Value::NativeFunction(a), Value::NativeFunction(b)) => a == b,
            (Value::Method(a), Value::Method(b)) => Rc::ptr_eq(a, b),
            (Value::Iterator(a), Value::Iterator(b)) => Rc::ptr_eq(a, b),
            (Value::Object(a), Value::Object(b)) => Rc::ptr_eq(a, b),
            (Value::Task(a), Value::Task(b)) => Rc::ptr_eq(a, b),
            (Value::Channel(a), Value::Channel(b)) => Rc::ptr_eq(a, b),
            (Value::CancellationSource(a), Value::CancellationSource(b))
            | (Value::CancellationToken(a), Value::CancellationToken(b))
            | (Value::CancellationSource(a), Value::CancellationToken(b))
            | (Value::CancellationToken(a), Value::CancellationSource(b)) => Rc::ptr_eq(a, b),
            (Value::Ref(a), Value::Ref(b))
            | (Value::AliasRef(a), Value::AliasRef(b))
            | (Value::Ref(a), Value::AliasRef(b))
            | (Value::AliasRef(a), Value::Ref(b)) => Rc::ptr_eq(a, b),
            (Value::Dict(a), Value::Dict(b)) => dict_eq(a, b),
            (Value::PairList(a), Value::PairList(b)) => pairlist_eq(a, b),
            _ => false,
        }
    }

    fn coerced_eq(&self, other: &Value) -> bool {
        if self.is_weak_value() {
            return self.resolve_weak_value().coerced_eq(other);
        }
        if other.is_weak_value() {
            return self.coerced_eq(&other.resolve_weak_value());
        }
        match (self, other) {
            (Value::Shared(value), other) => return value.borrow().coerced_eq(other),
            (this, Value::Shared(value)) => return this.coerced_eq(&value.borrow()),
            _ => {}
        }
        if self.strict_eq(other) {
            return true;
        }
        match (self, other) {
            (Value::Boolean(a), Value::Number(b)) => (*a as i32 as f64) == *b,
            (Value::Number(a), Value::Boolean(b)) => *a == (*b as i32 as f64),
            _ => false,
        }
    }
}

#[allow(dead_code)]
impl SimpleRegex {
    fn parse(pattern: &str, case_insensitive: bool) -> Result<Self> {
        let chars: Vec<char> = pattern.chars().collect();
        let mut index = 0usize;
        let parts = Self::parse_parts(&chars, &mut index, false)?;
        Ok(Self {
            parts,
            case_insensitive,
        })
    }

    fn parse_parts(chars: &[char], index: &mut usize, in_group: bool) -> Result<Vec<RegexPart>> {
        let mut parts = Vec::new();
        let mut literal = String::new();
        while *index < chars.len() {
            let ch = chars[*index];
            if in_group && ch == ')' {
                break;
            }
            match ch {
                '(' => {
                    if !literal.is_empty() {
                        parts.push(RegexPart::Literal(std::mem::take(&mut literal)));
                    }
                    *index += 1;
                    let inner = Self::parse_parts(chars, index, true)?;
                    if *index >= chars.len() || chars[*index] != ')' {
                        return Err(ZuzuRustError::runtime("unsupported regexp pattern"));
                    }
                    parts.push(RegexPart::Group(inner));
                }
                '[' => {
                    if !literal.is_empty() {
                        parts.push(RegexPart::Literal(std::mem::take(&mut literal)));
                    }
                    let part = if chars.get(*index..(*index + 5))
                        == Some(&['[', '0', '-', '9', ']'])
                    {
                        RegexPart::DigitClassPlus
                    } else if chars.get(*index..(*index + 5)) == Some(&['[', 'a', '-', 'z', ']']) {
                        RegexPart::LowerAlphaClassPlus
                    } else {
                        return Err(ZuzuRustError::runtime("unsupported regexp character class"));
                    };
                    *index += 4;
                    if chars.get(*index + 1) == Some(&'+') {
                        *index += 1;
                    } else {
                        return Err(ZuzuRustError::runtime("unsupported regexp quantifier"));
                    }
                    parts.push(part);
                }
                ')' => break,
                _ => literal.push(ch),
            }
            *index += 1;
        }
        if !literal.is_empty() {
            parts.push(RegexPart::Literal(literal));
        }
        Ok(parts)
    }

    fn find(&self, text: &str, start: usize) -> Option<RegexMatch> {
        let chars: Vec<char> = text.chars().collect();
        for offset in start..=chars.len() {
            let mut captures = Vec::new();
            if let Some(end) = self.match_parts(&self.parts, &chars, offset, &mut captures) {
                let full: String = chars[offset..end].iter().collect();
                let mut groups = vec![full];
                groups.extend(captures);
                return Some(RegexMatch {
                    start: offset,
                    end,
                    groups,
                });
            }
        }
        None
    }

    fn match_parts(
        &self,
        parts: &[RegexPart],
        chars: &[char],
        pos: usize,
        captures: &mut Vec<String>,
    ) -> Option<usize> {
        if parts.is_empty() {
            return Some(pos);
        }
        match &parts[0] {
            RegexPart::Literal(text) => {
                let lit_chars: Vec<char> = text.chars().collect();
                if pos + lit_chars.len() > chars.len() {
                    return None;
                }
                for (offset, ch) in lit_chars.iter().enumerate() {
                    if !self.chars_equal(chars[pos + offset], *ch) {
                        return None;
                    }
                }
                self.match_parts(&parts[1..], chars, pos + lit_chars.len(), captures)
            }
            RegexPart::DigitClassPlus => {
                let mut end = pos;
                while end < chars.len() && chars[end].is_ascii_digit() {
                    end += 1;
                }
                if end == pos {
                    return None;
                }
                for candidate_end in (pos + 1..=end).rev() {
                    let mut cloned = captures.clone();
                    if let Some(rest_end) =
                        self.match_parts(&parts[1..], chars, candidate_end, &mut cloned)
                    {
                        *captures = cloned;
                        return Some(rest_end);
                    }
                }
                None
            }
            RegexPart::LowerAlphaClassPlus => {
                let mut end = pos;
                while end < chars.len() && chars[end].is_ascii_lowercase() {
                    end += 1;
                }
                if end == pos {
                    return None;
                }
                for candidate_end in (pos + 1..=end).rev() {
                    let mut cloned = captures.clone();
                    if let Some(rest_end) =
                        self.match_parts(&parts[1..], chars, candidate_end, &mut cloned)
                    {
                        *captures = cloned;
                        return Some(rest_end);
                    }
                }
                None
            }
            RegexPart::Group(inner) => {
                let mut inner_captures = captures.clone();
                let end = self.match_parts(inner, chars, pos, &mut inner_captures)?;
                let capture: String = chars[pos..end].iter().collect();
                inner_captures.push(capture);
                let rest_end = self.match_parts(&parts[1..], chars, end, &mut inner_captures)?;
                *captures = inner_captures;
                Some(rest_end)
            }
        }
    }

    fn chars_equal(&self, left: char, right: char) -> bool {
        if self.case_insensitive {
            left.to_lowercase().to_string() == right.to_lowercase().to_string()
        } else {
            left == right
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.render())
    }
}

fn is_path_operator(operator: &str) -> bool {
    matches!(operator, "@" | "@@" | "@?")
}

fn render_number(value: f64) -> String {
    if value.fract() == 0.0 {
        if value >= i64::MIN as f64 && value <= i64::MAX as f64 {
            format!("{}", value as i64)
        } else {
            format!("{value:.0}")
        }
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod weak_classification_tests {
    use super::*;

    fn empty_env() -> Rc<Environment> {
        Rc::new(Environment {
            parent: None,
            bindings: RefCell::new(HashMap::new()),
        })
    }

    fn empty_block() -> BlockStatement {
        BlockStatement {
            line: 0,
            source_file: None,
            statements: Vec::new(),
            needs_lexical_scope: false,
        }
    }

    fn identifier_expr() -> Expression {
        Expression::Identifier {
            line: 0,
            source_file: None,
            name: "x".to_owned(),
            inferred_type: None,
            binding_depth: None,
        }
    }

    fn method_value(env: Rc<Environment>) -> Rc<MethodValue> {
        Rc::new(MethodValue {
            name: "m".to_owned(),
            params: Vec::new(),
            return_type: None,
            body: empty_block(),
            env,
            is_static: false,
            is_async: false,
            bound_receiver: None,
            bound_name: None,
        })
    }

    fn user_class(env: Rc<Environment>) -> Rc<UserClassValue> {
        Rc::new(UserClassValue {
            name: "C".to_owned(),
            base: None,
            traits: Vec::new(),
            fields: Vec::new(),
            methods: HashMap::new(),
            static_methods: HashMap::new(),
            nested_classes: HashMap::new(),
            source_decl: None,
            closure_env: Some(env),
        })
    }

    #[test]
    fn scalar_values_are_not_weakable() {
        let values = vec![
            Value::Null,
            Value::Boolean(true),
            Value::Number(42.0),
            Value::String("hello".to_owned()),
            Value::BinaryString(vec![1, 2, 3]),
            Value::Regex("x".to_owned(), "i".to_owned()),
            Value::Shared(Rc::new(RefCell::new(Value::String("shared".to_owned())))),
        ];

        for value in values {
            assert!(
                !value.is_weakable_value(),
                "{} should not be weakable",
                value.type_name()
            );
            assert!(value.make_weak_value().is_ok());
            assert!(value.strict_eq(&value.resolve_weak_value()));
        }
    }

    #[test]
    fn reference_capable_values_are_weakable() {
        let env = empty_env();
        let function = Rc::new(FunctionValue {
            name: Some("f".to_owned()),
            params: Vec::new(),
            return_type: None,
            body: FunctionBody::Block(empty_block()),
            env: Rc::clone(&env),
            is_async: false,
            current_method: None,
        });
        let method = method_value(Rc::clone(&env));
        let trait_value = Rc::new(TraitValue {
            name: "T".to_owned(),
            methods: HashMap::new(),
            source_decl: None,
            closure_env: Some(Rc::clone(&env)),
        });
        let class = user_class(Rc::clone(&env));
        let object = Rc::new(RefCell::new(ObjectValue {
            class: Rc::clone(&class),
            fields: HashMap::new(),
            weak_fields: HashSet::new(),
            builtin_value: None,
        }));
        let task = Rc::new(RefCell::new(TaskState {
            status: "pending".to_owned(),
            kind: TaskKind::Resolved,
            outcome: None,
        }));
        let channel = Rc::new(RefCell::new(ChannelState {
            messages: Vec::new(),
            closed: false,
        }));
        let cancellation = Rc::new(RefCell::new(CancellationState {
            cancelled: false,
            reason: Value::Null,
            watched: Vec::new(),
        }));
        let lvalue = Rc::new(LvalueRef::Expression {
            env: Rc::clone(&env),
            target: identifier_expr(),
        });

        let values = vec![
            Value::Array(Vec::new()),
            Value::Set(Vec::new()),
            Value::Bag(Vec::new()),
            Value::Dict(HashMap::new()),
            Value::PairList(Vec::new()),
            Value::Pair("key".to_owned(), Box::new(Value::Null)),
            Value::Function(function),
            Value::native_function("native".to_owned()),
            Value::Method(method),
            Value::Iterator(Rc::new(RefCell::new(IteratorState {
                items: Vec::new(),
                index: 0,
            }))),
            Value::builtin_class("Builtin".to_owned()),
            Value::UserClass(class),
            Value::Trait(trait_value),
            Value::Object(object),
            Value::Task(task),
            Value::Channel(channel),
            Value::CancellationSource(Rc::clone(&cancellation)),
            Value::CancellationToken(cancellation),
            Value::Shared(Rc::new(RefCell::new(Value::Array(Vec::new())))),
            Value::Ref(Rc::clone(&lvalue)),
            Value::AliasRef(lvalue),
        ];

        for value in values {
            assert!(
                value.is_weakable_value(),
                "{} should be weakable",
                value.type_name()
            );
            let weak = value.make_weak_value().expect("weak wrapping should work");
            assert!(
                weak.is_weak_value(),
                "{} should become weak",
                value.type_name()
            );
            assert_eq!(value.resolve_weak_value().type_name(), value.type_name());
        }
    }

    #[test]
    fn live_weak_values_resolve_for_core_value_behaviour() {
        let env = empty_env();
        let class = user_class(Rc::clone(&env));
        let object = Rc::new(RefCell::new(ObjectValue {
            class: Rc::clone(&class),
            fields: HashMap::new(),
            weak_fields: HashSet::new(),
            builtin_value: None,
        }));
        let object_value = Value::Object(Rc::clone(&object));
        let weak_object = object_value.make_weak_value().unwrap();

        assert!(weak_object.strict_eq(&object_value));
        assert_eq!(weak_object.type_name(), "Object");
        assert_eq!(weak_object.render(), "<C instance>");
        assert!(weak_object.is_truthy());

        let class_value = Value::builtin_class("Array");
        let weak_class = class_value.make_weak_value().unwrap();
        assert!(weak_class.strict_eq(&class_value));
        assert_eq!(weak_class.type_name(), "Class");

        let native_value = Value::native_function("ref_id");
        let weak_native = native_value.make_weak_value().unwrap();
        assert!(weak_native.strict_eq(&native_value));
        assert_eq!(weak_native.type_name(), "Function");
    }

    #[test]
    fn dead_weak_values_resolve_to_null() {
        let weak_object = {
            let env = empty_env();
            let class = user_class(env);
            let object = Rc::new(RefCell::new(ObjectValue {
                class,
                fields: HashMap::new(),
                weak_fields: HashSet::new(),
                builtin_value: None,
            }));
            Value::Object(object).make_weak_value().unwrap()
        };

        assert!(matches!(weak_object.resolve_weak_value(), Value::Null));
        assert_eq!(weak_object.type_name(), "Null");
        assert_eq!(weak_object.render(), "");
        assert!(!weak_object.is_truthy());
    }

    #[test]
    fn weak_values_participate_in_collections_and_type_checks() {
        let runtime = Runtime::new(Vec::new());
        let strong_array = Value::Array(vec![Value::Number(1.0)]).into_shared_if_composite();
        let weak_array = strong_array.make_weak_value().unwrap();

        assert!(Value::Array(vec![weak_array.clone()])
            .strict_eq(&Value::Array(vec![strong_array.clone()])));
        runtime
            .assert_declared_type(Some("Array"), &weak_array, "weak array")
            .unwrap();
        assert!(runtime.value_instanceof(&weak_array, &Value::builtin_class("Array")));
    }

    #[test]
    fn live_weak_values_can_be_called_or_receive_methods() {
        let runtime = Runtime::new(Vec::new());

        let class_value = Value::builtin_class("Array");
        let weak_class = class_value.make_weak_value().unwrap();
        let constructed = runtime
            .call_value(weak_class, vec![Value::Number(7.0)], Vec::new())
            .unwrap();
        assert!(constructed.strict_eq(&Value::Array(vec![Value::Number(7.0)])));

        let env = empty_env();
        let class = user_class(env);
        let object = Value::Object(Rc::new(RefCell::new(ObjectValue {
            class,
            fields: HashMap::new(),
            weak_fields: HashSet::new(),
            builtin_value: None,
        })));
        let native_value = Value::native_function("ref_id");
        let weak_native = native_value.make_weak_value().unwrap();
        let ref_id = runtime
            .call_value(weak_native, vec![object], Vec::new())
            .unwrap();
        assert_eq!(ref_id.type_name(), "String");

        let shared_array = Value::Array(vec![Value::Number(1.0)]).into_shared_if_composite();
        let mut weak_receiver = shared_array.make_weak_value().unwrap();
        let length = runtime
            .call_method_named(&mut weak_receiver, "length", &[], Vec::new())
            .unwrap();
        assert!(length.strict_eq(&Value::Number(1.0)));
    }
}

fn push_unique(values: &mut Vec<Value>, value: Value) {
    if !values.iter().any(|existing| existing.strict_eq(&value)) {
        values.push(value);
    }
}

fn exception_field_specs() -> Vec<FieldSpec> {
    ["message", "file", "line", "code"]
        .into_iter()
        .map(|name| FieldSpec {
            name: name.to_owned(),
            declared_type: None,
            mutable: true,
            accessors: Vec::new(),
            default_value: None,
            is_weak_storage: false,
        })
        .collect()
}

fn is_builtin_exception_class(name: &str) -> bool {
    matches!(
        name,
        "BailOutException"
            | "TypeException"
            | "CancelledException"
            | "TimeoutException"
            | "ChannelClosedException"
            | "MarshallingException"
            | "UnmarshallingException"
            | "ExhaustedException"
            | "Exception"
    )
}

fn inferred_exception_class(message: &str) -> Option<&'static str> {
    [
        "BailOutException",
        "TypeException",
        "CancelledException",
        "TimeoutException",
        "ChannelClosedException",
        "MarshallingException",
        "UnmarshallingException",
        "ExhaustedException",
        "Exception",
    ]
    .into_iter()
    .find(|class_name| {
        message == *class_name
            || message.starts_with(&format!("{class_name}:"))
            || (*class_name == "BailOutException" && message.starts_with("Bail out!"))
    })
}

fn sequence_eq(left: &[Value], right: &[Value]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(left, right)| left.strict_eq(right))
}

fn collection_items_eq(left: &[Value], right: &[Value]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .all(|item| right.iter().any(|other| other.strict_eq(item)))
}

fn bag_items_eq(left: &[Value], right: &[Value]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut used = vec![false; right.len()];
    for item in left {
        let mut matched = false;
        for (index, candidate) in right.iter().enumerate() {
            if !used[index] && candidate.strict_eq(item) {
                used[index] = true;
                matched = true;
                break;
            }
        }
        if !matched {
            return false;
        }
    }
    true
}

fn resolve_index(len: usize, index: isize) -> Option<usize> {
    let len = len as isize;
    let resolved = if index < 0 { len + index } else { index };
    if (0..len).contains(&resolved) {
        Some(resolved as usize)
    } else {
        None
    }
}

fn dict_eq(left: &HashMap<String, Value>, right: &HashMap<String, Value>) -> bool {
    left.len() == right.len()
        && left.iter().all(|(key, value)| {
            right
                .get(key)
                .map(|other| other.strict_eq(value))
                .unwrap_or(false)
        })
}

fn child_tasks_of(task: &Rc<RefCell<TaskState>>) -> Vec<Rc<RefCell<TaskState>>> {
    let state = task.borrow();
    match &state.kind {
        TaskKind::All { tasks } | TaskKind::Race { tasks } => tasks
            .iter()
            .filter_map(|value| match value {
                Value::Task(task) => Some(Rc::clone(task)),
                _ => None,
            })
            .collect(),
        TaskKind::Timeout {
            task: Value::Task(task),
            ..
        } => vec![Rc::clone(task)],
        TaskKind::FunctionWaiting { awaited, .. } => vec![Rc::clone(awaited)],
        TaskKind::SpawnWaiting { awaited, .. } => vec![Rc::clone(awaited)],
        _ => Vec::new(),
    }
}

fn earliest_deadline(left: Option<Instant>, right: Option<Instant>) -> Option<Instant> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
        (None, None) => None,
    }
}

async fn drive_scheduled_task(runtime: Weak<RuntimeInner>, task: Rc<RefCell<TaskState>>) {
    loop {
        let Some(inner) = runtime.upgrade() else {
            break;
        };
        let runtime = Runtime { inner };
        if task.borrow().outcome.is_some() {
            break;
        }

        match runtime.poll_task(&task) {
            Ok(true) => break,
            Ok(false) => {}
            Err(err) => {
                let mut state = task.borrow_mut();
                if state.outcome.is_none() {
                    state.status = "rejected".to_owned();
                    state.outcome = Some(TaskOutcome::Rejected(err.to_string()));
                }
                break;
            }
        }

        let deadline = runtime.next_task_deadline(&task);
        drop(runtime);
        match deadline {
            Some(deadline) if deadline > Instant::now() => {
                tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
            }
            Some(_) => {
                tokio::task::yield_now().await;
            }
            None => {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }
}

async fn drive_native_async_task<F>(task: Rc<RefCell<TaskState>>, future: F)
where
    F: Future<Output = Result<Value>>,
{
    {
        let mut state = task.borrow_mut();
        if state.outcome.is_none() {
            state.status = "running".to_owned();
        }
    }

    let result = future.await;
    let mut state = task.borrow_mut();
    if state.outcome.is_some() {
        return;
    }

    let outcome = match result {
        Ok(value) => TaskOutcome::Fulfilled(value),
        Err(ZuzuRustError::Thrown { value, .. }) => TaskOutcome::Rejected(value),
        Err(err) => TaskOutcome::Rejected(err.to_string()),
    };
    state.status = match &outcome {
        TaskOutcome::Fulfilled(_) => "fulfilled",
        TaskOutcome::Rejected(_) => "rejected",
        TaskOutcome::Cancelled(_) => "cancelled",
    }
    .to_owned();
    state.outcome = Some(outcome);
}

fn async_frames_of(task: &Rc<RefCell<TaskState>>) -> Option<Vec<AsyncFrame>> {
    let state = task.borrow();
    match &state.kind {
        TaskKind::FunctionWaiting { frames, .. } | TaskKind::SpawnWaiting { frames, .. } => {
            Some(frames.clone())
        }
        _ => None,
    }
}

struct PollTaskGuard<'a> {
    stack: &'a RefCell<Vec<usize>>,
    task_id: usize,
}

impl Drop for PollTaskGuard<'_> {
    fn drop(&mut self) {
        let mut stack = self.stack.borrow_mut();
        if stack.last() == Some(&self.task_id) {
            stack.pop();
        } else if let Some(index) = stack.iter().rposition(|id| *id == self.task_id) {
            stack.remove(index);
        }
    }
}

impl AsyncFrame {
    fn statement_count(&self) -> usize {
        match self {
            AsyncFrame::Function { statements, .. }
            | AsyncFrame::Block { statements, .. }
            | AsyncFrame::Do { statements, .. } => statements.len(),
        }
    }

    fn index(&self) -> usize {
        match self {
            AsyncFrame::Function { index, .. }
            | AsyncFrame::Block { index, .. }
            | AsyncFrame::Do { index, .. } => *index,
        }
    }

    fn advance(&mut self) {
        match self {
            AsyncFrame::Function { index, .. }
            | AsyncFrame::Block { index, .. }
            | AsyncFrame::Do { index, .. } => {
                *index += 1;
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.index() >= self.statement_count()
    }

    fn env(&self) -> Rc<Environment> {
        match self {
            AsyncFrame::Function { env, .. }
            | AsyncFrame::Block { env, .. }
            | AsyncFrame::Do { env, .. } => Rc::clone(env),
        }
    }

    fn set_last(&mut self, value: Value) {
        if let AsyncFrame::Do { last, .. } = self {
            *last = value;
        }
    }

    fn current_statement(&self) -> &Statement {
        match self {
            AsyncFrame::Function {
                statements, index, ..
            }
            | AsyncFrame::Block {
                statements, index, ..
            }
            | AsyncFrame::Do {
                statements, index, ..
            } => &statements[*index],
        }
    }
}
