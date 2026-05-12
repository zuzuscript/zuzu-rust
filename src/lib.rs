pub mod ast;
pub mod codegen;
pub mod error;
pub mod infer;
pub mod lexer;
pub mod optimizer;
pub mod parser;
pub mod runtime;
pub mod sema;
pub mod span;
pub mod token;
pub mod web;

pub use ast::{Expression, Program};
pub use error::{Result, ZuzuRustError};
pub use optimizer::{OptimizationLevel, OptimizationOptions, OptimizationPass};
pub use runtime::{
    module_search_roots, ExecutionOutput, HostValue, LoadedScript, Runtime, RuntimePolicy,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseOptions {
    pub run_sema: bool,
    pub infer_types: bool,
    pub optimizations: OptimizationOptions,
}

impl Default for ParseOptions {
    fn default() -> Self {
        Self {
            run_sema: true,
            infer_types: true,
            optimizations: OptimizationOptions::default(),
        }
    }
}

impl ParseOptions {
    pub fn new(run_sema: bool, infer_types: bool, optimizations: OptimizationOptions) -> Self {
        Self {
            run_sema,
            infer_types,
            optimizations,
        }
    }
}

pub fn parse_program(source: &str) -> Result<Program> {
    parse_program_with_options(source, true, true)
}

pub fn parse_program_with_options(
    source: &str,
    run_sema: bool,
    infer_types: bool,
) -> Result<Program> {
    parse_program_with_compile_options(
        source,
        &ParseOptions::new(run_sema, infer_types, OptimizationOptions::disabled()),
    )
}

pub fn parse_program_with_options_and_source_file(
    source: &str,
    run_sema: bool,
    infer_types: bool,
    source_file: Option<&str>,
) -> Result<Program> {
    parse_program_with_compile_options_and_source_file(
        source,
        &ParseOptions::new(run_sema, infer_types, OptimizationOptions::disabled()),
        source_file,
    )
}

pub fn parse_program_with_compile_options(source: &str, options: &ParseOptions) -> Result<Program> {
    parse_program_with_compile_options_and_source_file(source, options, None)
}

pub fn parse_program_with_compile_options_and_source_file(
    source: &str,
    options: &ParseOptions,
    source_file: Option<&str>,
) -> Result<Program> {
    let tokens = lexer::lex(source)?;
    let mut parser = match source_file {
        Some(source_file) => parser::Parser::with_source_file(tokens, source_file),
        None => parser::Parser::new(tokens),
    };
    let mut program = parser.parse_program()?;
    if options.run_sema {
        sema::validate_program(&program)?;
    }
    if options.infer_types {
        infer::annotate_program(&mut program);
    }
    optimizer::optimize_program(&mut program, &options.optimizations);
    Ok(program)
}

pub fn parse_expression(source: &str) -> Result<Expression> {
    parse_expression_with_source_file(source, None)
}

pub fn parse_expression_with_source_file(
    source: &str,
    source_file: Option<&str>,
) -> Result<Expression> {
    let tokens = lexer::lex(source)?;
    match source_file {
        Some(source_file) => parser::Parser::with_source_file(tokens, source_file),
        None => parser::Parser::new(tokens),
    }
    .parse_expression_root()
}
