use std::env;
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use zuzu_rust::{
    module_search_roots, optimizer, parse_program_with_compile_options, OptimizationLevel,
    OptimizationOptions, ParseOptions, Result, Runtime, RuntimePolicy, ZuzuRustError,
};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::from(1)
        }
    }
}

fn run() -> Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    let mut dump_ast = false;
    let mut dump_zuzu = false;
    let mut run_sema = true;
    let mut infer_types = true;
    let mut optimizations = zuzu_rust::OptimizationOptions::default();
    let mut inline_snippets = Vec::new();
    let mut preload_modules = Vec::new();
    let mut include_dirs = Vec::new();
    let mut denied_capabilities = Vec::new();
    let mut denied_modules = Vec::new();
    let mut debug_level = 0;
    let mut script_path: Option<String> = None;
    let mut script_argv = Vec::new();
    let mut show_version = false;
    let mut show_verbose_version = false;
    let mut repl_mode = false;

    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        if script_path.is_some() {
            script_argv.extend(args[index..].iter().cloned());
            break;
        }
        match arg.as_str() {
            "--dump-ast" => {
                dump_ast = true;
            }
            "--dump-zuzu" => {
                dump_zuzu = true;
            }
            "--no-sema" => {
                run_sema = false;
            }
            "--no-infer" => {
                infer_types = false;
            }
            "-o0" | "-o1" | "-o2" | "-o3" => {
                let level =
                    optimizer::parse_level(arg).expect("literal optimization level should parse");
                optimizations = zuzu_rust::OptimizationOptions::for_level(level);
            }
            "--" => {
                if inline_snippets.is_empty() {
                    let script = args
                        .get(index + 1)
                        .ok_or_else(|| ZuzuRustError::cli("expected script path after --"))?;
                    script_path = Some(script.clone());
                    script_argv.extend(args[index + 2..].iter().cloned());
                } else {
                    script_argv.extend(args[index + 1..].iter().cloned());
                }
                break;
            }
            "-e" => {
                index += 1;
                let source = args
                    .get(index)
                    .ok_or_else(|| ZuzuRustError::cli("expected source string after -e"))?;
                inline_snippets.push(source.clone());
            }
            _ if arg.starts_with("-e") && arg.len() > 2 => {
                inline_snippets.push(arg[2..].to_owned());
            }
            "-I" => {
                index += 1;
                let path = args
                    .get(index)
                    .ok_or_else(|| ZuzuRustError::cli("expected path after -I"))?;
                include_dirs.push(PathBuf::from(path));
            }
            "-M" => {
                index += 1;
                let module = args
                    .get(index)
                    .ok_or_else(|| ZuzuRustError::cli("expected module after -M"))?;
                preload_modules.extend(split_csv(module));
            }
            "--deny" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| ZuzuRustError::cli("expected capability after --deny"))?;
                denied_capabilities.extend(split_csv(value));
            }
            "--denymodule" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| ZuzuRustError::cli("expected module after --denymodule"))?;
                denied_modules.extend(split_csv(value));
            }
            "-v" => {
                show_version = true;
            }
            "-V" => {
                show_verbose_version = true;
            }
            "-R" | "--repl" => {
                repl_mode = true;
            }
            "-h" | "--help" => {
                print_help();
                return Ok(());
            }
            "--opt-help" => {
                print_opt_help();
                return Ok(());
            }
            "-d" => {
                debug_level = 1;
            }
            _ if arg.starts_with("-d=") => {
                debug_level = parse_debug_level(&arg[3..])?;
            }
            _ if arg.starts_with("-o") && arg.len() > 2 => {
                let level = optimizer::parse_level(&arg[2..])
                    .ok_or_else(|| ZuzuRustError::cli(format!("unsupported option: {arg}")))?;
                optimizations = zuzu_rust::OptimizationOptions::for_level(level);
            }
            _ if is_debug_short_option(arg) => {
                debug_level = parse_debug_level(&arg[2..])?;
            }
            _ if arg.starts_with("-I") && arg.len() > 2 => {
                include_dirs.push(PathBuf::from(&arg[2..]));
            }
            _ if arg.starts_with("-M") && arg.len() > 2 => {
                preload_modules.extend(split_csv(&arg[2..]));
            }
            _ if arg.starts_with('-') => {
                if let Some(value) = arg.strip_prefix("--deny=") {
                    denied_capabilities.extend(split_csv(value));
                    index += 1;
                    continue;
                }
                if let Some(value) = arg.strip_prefix("--denymodule=") {
                    denied_modules.extend(split_csv(value));
                    index += 1;
                    continue;
                }
                if let Some(value) = arg.strip_prefix("--opt=") {
                    for name in split_csv(value) {
                        optimizations.enable(&name)?;
                    }
                    index += 1;
                    continue;
                }
                if let Some(value) = arg.strip_prefix("--no-opt=") {
                    for name in split_csv(value) {
                        optimizations.disable(&name)?;
                    }
                    index += 1;
                    continue;
                }
                return Err(ZuzuRustError::cli(format!("unsupported option: {arg}")));
            }
            _ => {
                if inline_snippets.is_empty() {
                    script_path = Some(arg.clone());
                    script_argv.extend(args[index + 1..].iter().cloned());
                } else {
                    script_argv.extend(args[index..].iter().cloned());
                }
                break;
            }
        }
        index += 1;
    }

    if show_version || show_verbose_version {
        print_version(show_verbose_version, &include_dirs)?;
        return Ok(());
    }

    if dump_ast && dump_zuzu {
        return Err(ZuzuRustError::cli(
            "expected at most one dump mode: --dump-ast or --dump-zuzu",
        ));
    }

    if repl_mode {
        if !inline_snippets.is_empty() {
            return Err(ZuzuRustError::cli(
                "-R/--repl cannot be combined with -e snippets",
            ));
        }
        if script_path.is_some() || !script_argv.is_empty() {
            return Err(ZuzuRustError::cli(
                "-R/--repl does not accept a script path or argv values",
            ));
        }
        let runtime = Runtime::with_policy(
            module_search_roots(include_dirs),
            runtime_policy(denied_capabilities, denied_modules).debug_level(debug_level),
        )
        .with_parse_options(run_sema, infer_types)
        .with_optimization_options(optimizations);
        run_repl(&runtime, &preload_modules)?;
        return Ok(());
    }

    let inline_source = if inline_snippets.is_empty() {
        None
    } else {
        Some(prepare_inline_source(&preload_modules, &inline_snippets))
    };

    let source = match (inline_source.as_ref(), script_path.as_ref()) {
        (Some(source), None) => source.clone(),
        (None, Some(script_path)) => {
            let source = fs::read_to_string(script_path)?;
            prepare_file_source(&preload_modules, &source)
        }
        (None, None) => {
            return Err(ZuzuRustError::cli(
                "usage: zuzu-rust [options] path/to/script.zzs [arg ...]",
            ));
        }
        (Some(_), Some(_)) => {
            return Err(ZuzuRustError::cli(
                "expected exactly one input source: either -e code or a script path",
            ));
        }
    };

    if dump_ast || dump_zuzu {
        let source_file = script_path.as_ref().map(|path| path.to_owned());
        let options = ParseOptions::new(run_sema, infer_types, optimizations.clone());
        let program = zuzu_rust::parse_program_with_compile_options_and_source_file(
            &source,
            &options,
            source_file.as_deref(),
        )?;
        if run_sema {
            for warning in zuzu_rust::sema::weak_storage_warnings(&program) {
                eprintln!("{warning}");
            }
        }
        if dump_ast {
            println!("{}", program.to_json_pretty());
        } else {
            print!("{}", zuzu_rust::codegen::render_program(&program));
        }
        return Ok(());
    }

    let runtime = Runtime::with_policy(
        module_search_roots(include_dirs),
        runtime_policy(denied_capabilities, denied_modules).debug_level(debug_level),
    )
    .with_parse_options(run_sema, infer_types)
    .with_optimization_options(optimizations);
    match (inline_source.as_ref(), script_path.as_ref()) {
        (Some(_), None) => runtime.run_script_source_with_args(&source, &script_argv)?,
        (None, Some(path)) => {
            let source_file = path.to_owned();
            runtime.run_script_source_with_args_and_source_file(
                &source,
                &script_argv,
                Some(&source_file),
            )?
        }
        _ => unreachable!(),
    };
    Ok(())
}

fn print_help() {
    println!(
        "\
Usage: zuzu-rust [options] path/to/script.zzs [arg ...]
       zuzu-rust [options] -e 'code' [arg ...]
Options:
  -d[=N]                 set debug level (accepted for CLI parity)
  -I/path/to/lib         add module include directory
  --deny=CAP             deny runtime capability (repeatable)
  --denymodule=MODULE    deny a specific module (repeatable)
  -e 'code'              evaluate inline code (repeatable)
  -Mmodule               preload module with wildcard import
  -oN                    set optimization level: -o0, -o1, -o2, or -o3
  --opt=NAME             enable a named optimization pass
  --no-opt=NAME          disable a named optimization pass
  --opt-help             list named optimization passes
  -R, --repl             start interactive REPL shell
  --dump-ast             print parsed AST as stable JSON
  --dump-zuzu            print parsed AST as ZuzuScript source
  --no-sema              skip semantic validation
  --no-infer             skip type inference annotations
  -h, --help             show this help
  -v                     print version
  -V                     print verbose version details"
    );
}

fn print_opt_help() {
    println!("Available optimization passes:");
    for pass in optimizer::all_passes() {
        println!("  {}", pass.name());
    }

    println!();
    println!("Optimization levels:");
    for (name, level) in [
        ("-o0", OptimizationLevel::O0),
        ("-o1", OptimizationLevel::O1),
        ("-o2", OptimizationLevel::O2),
        ("-o3", OptimizationLevel::O3),
    ] {
        let options = OptimizationOptions::for_level(level);
        let names: Vec<&str> = optimizer::all_passes()
            .iter()
            .copied()
            .filter(|pass| options.enables(*pass))
            .map(optimizer::OptimizationPass::name)
            .collect();
        if names.is_empty() {
            println!("  {name}: (none)");
        } else {
            println!("  {name}: {}", names.join(", "));
        }
    }
}

fn split_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_owned)
        .collect()
}

fn is_debug_short_option(arg: &str) -> bool {
    let Some(value) = arg.strip_prefix("-d") else {
        return false;
    };
    !value.is_empty() && value.chars().all(|ch| ch.is_ascii_digit())
}

fn parse_debug_level(value: &str) -> Result<u32> {
    if value.is_empty() {
        return Ok(1);
    }
    value
        .parse::<u32>()
        .map_err(|_| ZuzuRustError::cli("debug level must be a non-negative integer"))
}

fn prepare_inline_source(preload_modules: &[String], snippets: &[String]) -> String {
    let mut chunks = preload_source(preload_modules);
    chunks.extend(snippets.iter().cloned());
    chunks.join("\n")
}

fn prepare_file_source(preload_modules: &[String], source: &str) -> String {
    let mut chunks = preload_source(preload_modules);
    chunks.push(source.to_owned());
    chunks.join("\n")
}

fn preload_source(preload_modules: &[String]) -> Vec<String> {
    preload_modules
        .iter()
        .map(|module| format!("from {module} import *;"))
        .collect()
}

fn runtime_policy(denied_capabilities: Vec<String>, denied_modules: Vec<String>) -> RuntimePolicy {
    let mut policy = RuntimePolicy::new();
    for capability in denied_capabilities {
        policy = policy.deny_capability(capability);
    }
    for module in denied_modules {
        policy = policy.deny_module(module);
    }
    policy
}

fn print_version(verbose: bool, include_dirs: &[PathBuf]) -> Result<()> {
    println!("zuzu-rust version {}", env!("CARGO_PKG_VERSION"));
    if verbose {
        println!();
        println!("lib search paths:");
        for path in module_search_roots(include_dirs.to_vec()) {
            println!("  {}", path.display());
        }
    }
    Ok(())
}

fn run_repl(runtime: &Runtime, preload_modules: &[String]) -> Result<()> {
    let session = runtime.repl_session();
    let preload = preload_source(preload_modules).join("\n");
    if !preload.trim().is_empty() {
        match session.eval_source(&preload) {
            Ok(_) => {}
            Err(err) => repl_print_error(&err.to_string())?,
        }
    }

    let stdin = io::stdin();
    let mut lines = stdin.lock().lines();
    let mut buffer = Vec::new();
    let mut expecting_more = false;

    loop {
        repl_print_prompt(expecting_more)?;
        let Some(line) = lines.next() else {
            println!();
            return Ok(());
        };
        let line = line?;
        if buffer.is_empty() && line.trim().is_empty() {
            continue;
        }

        if expecting_more && !buffer.is_empty() && line.trim() == ";" {
            eval_repl_buffer(&session, &buffer)?;
            buffer.clear();
            expecting_more = false;
            continue;
        }

        buffer.push(line);
        let source = buffer.join("\n");
        if repl_structural_depth(&source) > 0 {
            expecting_more = true;
            continue;
        }
        if repl_has_open_multiline_literal(&source) {
            expecting_more = true;
            continue;
        }

        match try_parse_repl_source(&source) {
            Ok(effective_source) => {
                eval_repl_source(&session, &effective_source)?;
                buffer.clear();
                expecting_more = false;
            }
            Err(err) if is_incomplete_parse(&err) => {
                expecting_more = true;
            }
            Err(err) => {
                repl_print_error(&err.to_string())?;
                buffer.clear();
                expecting_more = false;
            }
        }
    }
}

fn eval_repl_buffer(
    session: &zuzu_rust::runtime::ReplSession<'_>,
    buffer: &[String],
) -> Result<()> {
    let source = buffer.join("\n");
    match try_parse_repl_source(&source) {
        Ok(effective_source) => eval_repl_source(session, &effective_source),
        Err(err) => repl_print_error(&err.to_string()),
    }
}

fn eval_repl_source(session: &zuzu_rust::runtime::ReplSession<'_>, source: &str) -> Result<()> {
    match session.eval_source(source) {
        Ok(result) => repl_print_output(&result.value),
        Err(err) => repl_print_error(&err.to_string()),
    }
}

fn try_parse_repl_source(source: &str) -> Result<String> {
    let options = ParseOptions::new(false, true, zuzu_rust::OptimizationOptions::default());
    match parse_program_with_compile_options(source, &options) {
        Ok(_) => Ok(source.to_owned()),
        Err(first_err) => {
            let trimmed = source.trim_end();
            if !trimmed.is_empty()
                && !trimmed.ends_with(';')
                && !trimmed.ends_with('{')
                && !trimmed.ends_with('}')
            {
                let with_semicolon = format!("{trimmed};");
                if parse_program_with_compile_options(&with_semicolon, &options).is_ok() {
                    return Ok(with_semicolon);
                }
            }
            Err(first_err)
        }
    }
}

fn is_incomplete_parse(err: &ZuzuRustError) -> bool {
    match err {
        ZuzuRustError::IncompleteParse { .. } => true,
        ZuzuRustError::Lex { message, .. } => {
            message.starts_with("unterminated triple-quoted string literal")
                || message.starts_with("unterminated triple-backtick template literal")
        }
        _ => false,
    }
}

fn repl_print_prompt(continuation: bool) -> Result<()> {
    print!("{}", repl_prompt_coloured(continuation));
    io::stdout().flush()?;
    Ok(())
}

fn repl_prompt_coloured(continuation: bool) -> String {
    let colour = if continuation {
        "\x1b[1;35m"
    } else {
        "\x1b[1;36m"
    };
    format!("{colour}{}\x1b[0m", repl_prompt_label(continuation))
}

fn repl_prompt_label(continuation: bool) -> &'static str {
    if env::var_os("ZUZU_EMOJI").is_some() {
        return if continuation {
            "zuzu 🦝 ⏳ > "
        } else {
            "zuzu 🦝 💤 > "
        };
    }

    if continuation {
        "zuzu (...)> "
    } else {
        "zuzu (^_^)> "
    }
}

fn repl_print_output(message: &str) -> Result<()> {
    println!("\x1b[1;32m{message}\x1b[0m");
    Ok(())
}

fn repl_print_error(message: &str) -> Result<()> {
    eprintln!("\x1b[1;31m{message}\x1b[0m");
    Ok(())
}

fn repl_structural_depth(source: &str) -> isize {
    let mut depth = 0;
    let mut in_single = false;
    let mut in_double = false;
    let mut escaped = false;

    for ch in source.chars() {
        if escaped {
            escaped = false;
            continue;
        }
        if in_single {
            if ch == '\\' {
                escaped = true;
            } else if ch == '\'' {
                in_single = false;
            }
            continue;
        }
        if in_double {
            if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_double = false;
            }
            continue;
        }
        match ch {
            '\'' => in_single = true,
            '"' => in_double = true,
            '{' | '(' | '[' => depth += 1,
            '}' | ')' | ']' => depth -= 1,
            _ => {}
        }
    }

    depth
}

fn repl_has_open_multiline_literal(source: &str) -> bool {
    delimiter_is_open(source, "\"\"\"") || delimiter_is_open(source, "```")
}

fn delimiter_is_open(source: &str, delimiter: &str) -> bool {
    let mut count = 0usize;
    let mut start = 0usize;
    while let Some(offset) = source[start..].find(delimiter) {
        let index = start + offset;
        if !is_escaped(source, index) {
            count += 1;
        }
        start = index + delimiter.len();
    }
    count % 2 == 1
}

fn is_escaped(source: &str, index: usize) -> bool {
    let mut backslashes = 0usize;
    for byte in source[..index].bytes().rev() {
        if byte != b'\\' {
            break;
        }
        backslashes += 1;
    }
    backslashes % 2 == 1
}
