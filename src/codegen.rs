use crate::ast::{
    BlockStatement, CallArgument, CatchBinding, CatchClause, ClassDeclaration, ClassMember,
    DictEntry, DictKey, Expression, FieldDeclaration, FunctionDeclaration, ImportDeclaration,
    MethodDeclaration, Parameter, Program, Statement, SwitchCase, TemplatePart, TraitDeclaration,
};

const PREC_ASSIGNMENT: u8 = 1;
const PREC_TERNARY: u8 = 2;
const PREC_OR: u8 = 3;
const PREC_XOR: u8 = 4;
const PREC_AND: u8 = 5;
const PREC_EQUALITY: u8 = 6;
const PREC_COMPARISON: u8 = 7;
const PREC_BITWISE_OR: u8 = 8;
const PREC_BITWISE_XOR: u8 = 9;
const PREC_BITWISE_AND: u8 = 10;
const PREC_SET: u8 = 11;
const PREC_CONCAT: u8 = 12;
const PREC_ADDITIVE: u8 = 13;
const PREC_MULTIPLICATIVE: u8 = 14;
const PREC_EXPONENT: u8 = 15;
const PREC_PREFIX: u8 = 16;
const PREC_POSTFIX: u8 = 17;
const PREC_ATOM: u8 = 18;

pub fn render_program(program: &Program) -> String {
    let mut out = String::new();
    for statement in &program.statements {
        render_statement(statement, 0, &mut out);
        out.push('\n');
    }
    out
}

pub fn render_function_declaration(node: &FunctionDeclaration) -> String {
    let mut out = String::new();
    render_function_declaration_into(node, 0, &mut out);
    out
}

pub fn render_class_declaration(node: &ClassDeclaration) -> String {
    let mut out = String::new();
    render_class_declaration_into(node, 0, &mut out);
    out
}

pub fn render_trait_declaration(node: &TraitDeclaration) -> String {
    let mut out = String::new();
    render_trait_declaration_into(node, 0, &mut out);
    out
}

pub fn render_block(block: &BlockStatement) -> String {
    let mut out = String::new();
    render_block_into(block, 0, &mut out);
    out
}

pub fn render_expression(expression: &Expression) -> String {
    render_expr(expression, 0)
}

pub fn render_function_literal(
    params: &[Parameter],
    return_type: Option<&str>,
    body: &BlockStatement,
    is_async: bool,
) -> String {
    let mut out = String::new();
    if is_async {
        out.push_str("async ");
    }
    out.push_str("function ");
    render_parameter_list(params, &mut out);
    render_return_type(return_type, &mut out);
    out.push(' ');
    render_block_into(body, 0, &mut out);
    out
}

fn render_statement(statement: &Statement, indent: usize, out: &mut String) {
    match statement {
        Statement::Block(block) => {
            push_indent(out, indent);
            render_block_into(block, indent, out);
        }
        Statement::VariableDeclaration(node) => {
            push_indent(out, indent);
            out.push_str(&render_variable_head(
                &node.kind,
                node.declared_type.as_deref(),
                &node.name,
                node.init.as_ref(),
                node.is_weak_storage,
            ));
            out.push(';');
        }
        Statement::FunctionDeclaration(node) => render_function_declaration_into(node, indent, out),
        Statement::ClassDeclaration(node) => render_class_declaration_into(node, indent, out),
        Statement::TraitDeclaration(node) => render_trait_declaration_into(node, indent, out),
        Statement::ImportDeclaration(node) => render_import_declaration(node, indent, out),
        Statement::IfStatement(node) => {
            push_indent(out, indent);
            out.push_str("if ( ");
            out.push_str(&render_expression(&node.test));
            out.push_str(" ) ");
            render_block_into(&node.consequent, indent, out);
            if let Some(alternate) = &node.alternate {
                out.push('\n');
                push_indent(out, indent);
                out.push_str("else");
                match alternate.as_ref() {
                    Statement::IfStatement(_) => {
                        out.push(' ');
                        render_statement_without_indent(alternate, indent, out);
                    }
                    Statement::Block(block) => {
                        out.push(' ');
                        render_block_into(block, indent, out);
                    }
                    other => {
                        out.push('\n');
                        render_statement(other, indent + 1, out);
                    }
                }
            }
        }
        Statement::WhileStatement(node) => {
            push_indent(out, indent);
            out.push_str("while ( ");
            out.push_str(&render_expression(&node.test));
            out.push_str(" ) ");
            render_block_into(&node.body, indent, out);
        }
        Statement::ForStatement(node) => {
            push_indent(out, indent);
            out.push_str("for ( ");
            if let Some(kind) = &node.binding_kind {
                out.push_str(kind);
                out.push(' ');
            }
            out.push_str(&node.variable);
            out.push_str(" in ");
            out.push_str(&render_expression(&node.iterable));
            out.push_str(" ) ");
            render_block_into(&node.body, indent, out);
            if let Some(else_block) = &node.else_block {
                out.push('\n');
                push_indent(out, indent);
                out.push_str("else ");
                render_block_into(else_block, indent, out);
            }
        }
        Statement::SwitchStatement(node) => {
            push_indent(out, indent);
            out.push_str("switch ( ");
            out.push_str(&render_expression(&node.discriminant));
            if let Some(comparator) = &node.comparator {
                out.push_str(" : ");
                out.push_str(comparator);
            }
            out.push_str(" ) {\n");
            for case in &node.cases {
                render_switch_case(case, indent + 1, out);
            }
            if let Some(default) = &node.default {
                push_indent(out, indent + 1);
                out.push_str("default:\n");
                render_statement_list(default, indent + 2, out);
            }
            push_indent(out, indent);
            out.push('}');
        }
        Statement::TryStatement(node) => {
            push_indent(out, indent);
            out.push_str("try ");
            render_block_into(&node.body, indent, out);
            for handler in &node.handlers {
                out.push('\n');
                push_indent(out, indent);
                render_catch_clause(handler, indent, out);
            }
        }
        Statement::ReturnStatement(node) => {
            push_indent(out, indent);
            out.push_str("return");
            if let Some(argument) = &node.argument {
                out.push(' ');
                out.push_str(&render_expression(argument));
            }
            out.push(';');
        }
        Statement::LoopControlStatement(node) => {
            push_indent(out, indent);
            out.push_str(&node.keyword);
            out.push(';');
        }
        Statement::ThrowStatement(node) => {
            push_indent(out, indent);
            out.push_str("throw ");
            out.push_str(&render_expression(&node.argument));
            out.push(';');
        }
        Statement::DieStatement(node) => {
            push_indent(out, indent);
            out.push_str("die ");
            out.push_str(&render_expression(&node.argument));
            out.push(';');
        }
        Statement::PostfixConditionalStatement(node) => {
            push_indent(out, indent);
            out.push_str(&render_inline_statement(&node.statement));
            out.push(' ');
            out.push_str(&node.keyword);
            out.push(' ');
            out.push_str(&render_expression(&node.test));
            out.push(';');
        }
        Statement::KeywordStatement(node) => {
            push_indent(out, indent);
            out.push_str(&node.keyword);
            if !node.arguments.is_empty() {
                out.push(' ');
                out.push_str(&render_expression_list(&node.arguments));
            }
            out.push(';');
        }
        Statement::ExpressionStatement(node) => {
            push_indent(out, indent);
            out.push_str(&render_expression(&node.expression));
            out.push(';');
        }
    }
}

fn render_statement_without_indent(statement: &Statement, indent: usize, out: &mut String) {
    let mut rendered = String::new();
    render_statement(statement, indent, &mut rendered);
    out.push_str(rendered.trim_start_matches('\t'));
}

fn render_statement_list(statements: &[Statement], indent: usize, out: &mut String) {
    for statement in statements {
        render_statement(statement, indent, out);
        out.push('\n');
    }
}

fn render_block_into(block: &BlockStatement, indent: usize, out: &mut String) {
    out.push_str("{\n");
    render_statement_list(&block.statements, indent + 1, out);
    push_indent(out, indent);
    out.push('}');
}

fn render_function_declaration_into(node: &FunctionDeclaration, indent: usize, out: &mut String) {
    push_indent(out, indent);
    if node.is_async {
        out.push_str("async ");
    }
    out.push_str("function ");
    out.push_str(&node.name);
    out.push(' ');
    render_parameter_list(&node.params, out);
    render_return_type(node.return_type.as_deref(), out);
    out.push(' ');
    render_block_into(&node.body, indent, out);
}

fn render_class_declaration_into(node: &ClassDeclaration, indent: usize, out: &mut String) {
    push_indent(out, indent);
    out.push_str("class ");
    out.push_str(&node.name);
    if let Some(base) = &node.base {
        out.push_str(" extends ");
        out.push_str(base);
    }
    if !node.traits.is_empty() {
        out.push_str(" with ");
        out.push_str(&node.traits.join(", "));
    }
    if node.shorthand {
        out.push(';');
        return;
    }
    out.push_str(" {\n");
    for member in &node.body {
        render_class_member(member, indent + 1, out);
        out.push('\n');
    }
    push_indent(out, indent);
    out.push('}');
}

fn render_trait_declaration_into(node: &TraitDeclaration, indent: usize, out: &mut String) {
    push_indent(out, indent);
    out.push_str("trait ");
    out.push_str(&node.name);
    if node.shorthand {
        out.push(';');
        return;
    }
    out.push_str(" {\n");
    for member in &node.body {
        render_class_member(member, indent + 1, out);
        out.push('\n');
    }
    push_indent(out, indent);
    out.push('}');
}

fn render_class_member(member: &ClassMember, indent: usize, out: &mut String) {
    match member {
        ClassMember::Field(field) => render_field_declaration(field, indent, out),
        ClassMember::Method(method) => render_method_declaration(method, indent, out),
        ClassMember::Class(class) => render_class_declaration_into(class, indent, out),
        ClassMember::Trait(trait_node) => render_trait_declaration_into(trait_node, indent, out),
    }
}

fn render_field_declaration(field: &FieldDeclaration, indent: usize, out: &mut String) {
    push_indent(out, indent);
    out.push_str(&field.kind);
    out.push(' ');
    if let Some(declared_type) = &field.declared_type {
        out.push_str(declared_type);
        out.push(' ');
    }
    out.push_str(&field.name);
    if !field.accessors.is_empty() {
        out.push_str(" with ");
        out.push_str(&field.accessors.join(", "));
    }
    if let Some(default_value) = &field.default_value {
        out.push_str(" := ");
        out.push_str(&render_expression(default_value));
    }
    if field.is_weak_storage {
        out.push_str(" but weak");
    }
    out.push(';');
}

fn render_method_declaration(method: &MethodDeclaration, indent: usize, out: &mut String) {
    push_indent(out, indent);
    if method.is_static {
        out.push_str("static ");
    }
    if method.is_async {
        out.push_str("async ");
    }
    out.push_str("method ");
    out.push_str(&method.name);
    out.push(' ');
    render_parameter_list(&method.params, out);
    render_return_type(method.return_type.as_deref(), out);
    out.push(' ');
    render_block_into(&method.body, indent, out);
}

fn render_variable_head(
    kind: &str,
    declared_type: Option<&str>,
    name: &str,
    init: Option<&Expression>,
    is_weak_storage: bool,
) -> String {
    let mut out = String::new();
    out.push_str(kind);
    out.push(' ');
    if let Some(declared_type) = declared_type {
        out.push_str(declared_type);
        out.push(' ');
    }
    out.push_str(name);
    if let Some(init) = init {
        out.push_str(" := ");
        out.push_str(&render_expression(init));
    }
    if is_weak_storage {
        out.push_str(" but weak");
    }
    out
}

fn render_import_declaration(node: &ImportDeclaration, indent: usize, out: &mut String) {
    push_indent(out, indent);
    out.push_str("from ");
    out.push_str(&node.source);
    out.push(' ');
    if node.try_mode {
        out.push_str("try ");
    }
    out.push_str("import ");
    if node.import_all {
        out.push('*');
    } else {
        out.push_str(
            &node
                .specifiers
                .iter()
                .map(|specifier| {
                    if specifier.imported == specifier.local {
                        specifier.imported.clone()
                    } else {
                        format!("{} as {}", specifier.imported, specifier.local)
                    }
                })
                .collect::<Vec<_>>()
                .join(", "),
        );
    }
    if let Some(condition) = &node.condition {
        out.push(' ');
        out.push_str(&condition.keyword);
        out.push(' ');
        out.push_str(&render_expression(&condition.test));
    }
    out.push(';');
}

fn render_switch_case(case: &SwitchCase, indent: usize, out: &mut String) {
    push_indent(out, indent);
    out.push_str("case ");
    out.push_str(&render_expression_list(&case.values));
    out.push_str(":\n");
    render_statement_list(&case.consequent, indent + 1, out);
}

fn render_catch_clause(handler: &CatchClause, indent: usize, out: &mut String) {
    out.push_str("catch");
    if let Some(binding) = &handler.binding {
        out.push_str(" ( ");
        out.push_str(&render_catch_binding(binding));
        out.push_str(" )");
    }
    out.push(' ');
    render_block_into(&handler.body, indent, out);
}

fn render_catch_binding(binding: &CatchBinding) -> String {
    match (binding.declared_type.as_deref(), binding.name.as_deref()) {
        (Some(declared_type), Some(name)) => format!("{declared_type} {name}"),
        (None, Some(name)) => name.to_owned(),
        (Some(declared_type), None) => declared_type.to_owned(),
        (None, None) => String::new(),
    }
}

fn render_inline_statement(statement: &Statement) -> String {
    match statement {
        Statement::ExpressionStatement(node) => render_expression(&node.expression),
        Statement::ReturnStatement(node) => match &node.argument {
            Some(argument) => format!("return {}", render_expression(argument)),
            None => "return".to_owned(),
        },
        Statement::LoopControlStatement(node) => node.keyword.clone(),
        Statement::ThrowStatement(node) => format!("throw {}", render_expression(&node.argument)),
        Statement::DieStatement(node) => format!("die {}", render_expression(&node.argument)),
        Statement::KeywordStatement(node) => {
            if node.arguments.is_empty() {
                node.keyword.clone()
            } else {
                format!(
                    "{} {}",
                    node.keyword,
                    render_expression_list(&node.arguments)
                )
            }
        }
        other => {
            let mut out = String::new();
            render_statement(other, 0, &mut out);
            out.trim_end_matches(';').to_owned()
        }
    }
}

fn render_parameter_list(params: &[Parameter], out: &mut String) {
    out.push('(');
    if !params.is_empty() {
        out.push(' ');
        out.push_str(
            &params
                .iter()
                .map(render_parameter)
                .collect::<Vec<_>>()
                .join(", "),
        );
        out.push(' ');
    }
    out.push(')');
}

fn render_return_type(return_type: Option<&str>, out: &mut String) {
    if let Some(return_type) = return_type {
        out.push_str(" -> ");
        out.push_str(return_type);
    }
}

fn render_parameter(param: &Parameter) -> String {
    let mut out = String::new();
    if param.variadic {
        out.push_str("...");
    }
    if let Some(declared_type) = &param.declared_type {
        out.push_str(declared_type);
        out.push(' ');
    }
    out.push_str(&param.name);
    if param.optional {
        out.push('?');
    }
    if let Some(default_value) = &param.default_value {
        out.push_str(" := ");
        out.push_str(&render_expression(default_value));
    }
    out
}

fn render_expr(expression: &Expression, parent_prec: u8) -> String {
    let (text, prec) = match expression {
        Expression::Identifier { name, .. } => (name.clone(), PREC_ATOM),
        Expression::NumberLiteral { value, .. } => (value.clone(), PREC_ATOM),
        Expression::StringLiteral { value, .. } => (quote_string(value), PREC_ATOM),
        Expression::RegexLiteral { pattern, flags, .. } => (
            format!("/{}/{}", quote_regex_pattern(pattern), flags),
            PREC_ATOM,
        ),
        Expression::BooleanLiteral { value, .. } => {
            (if *value { "true" } else { "false" }.to_owned(), PREC_ATOM)
        }
        Expression::NullLiteral { .. } => ("null".to_owned(), PREC_ATOM),
        Expression::ArrayLiteral { elements, .. } => (
            format!("[ {} ]", render_expression_list(elements)),
            PREC_ATOM,
        ),
        Expression::SetLiteral { elements, .. } => (
            format!("<< {} >>", render_expression_list(elements)),
            PREC_ATOM,
        ),
        Expression::BagLiteral { elements, .. } => (
            format!("<<< {} >>>", render_expression_list(elements)),
            PREC_ATOM,
        ),
        Expression::DictLiteral { entries, .. } => {
            (format!("{{ {} }}", render_dict_entries(entries)), PREC_ATOM)
        }
        Expression::PairListLiteral { entries, .. } => (
            format!("{{{{ {} }}}}", render_dict_entries(entries)),
            PREC_ATOM,
        ),
        Expression::TemplateLiteral { parts, .. } => (render_template_literal(parts), PREC_ATOM),
        Expression::Unary {
            operator, argument, ..
        } => {
            let argument = render_expr(argument, PREC_PREFIX);
            let text = if is_word_operator(operator) {
                format!("{operator} {argument}")
            } else {
                format!("{operator}{argument}")
            };
            (text, PREC_PREFIX)
        }
        Expression::Binary {
            operator,
            left,
            right,
            ..
        } => {
            let prec = infix_precedence(operator);
            let right_parent = if is_right_associative(operator) {
                prec
            } else {
                prec + 1
            };
            (
                format!(
                    "{} {} {}",
                    render_expr(left, prec),
                    operator,
                    render_expr(right, right_parent)
                ),
                prec,
            )
        }
        Expression::Ternary {
            test,
            consequent,
            alternate,
            ..
        } => (
            format!(
                "{} ? {} : {}",
                render_expr(test, PREC_TERNARY),
                render_expression(consequent),
                render_expr(alternate, PREC_TERNARY)
            ),
            PREC_TERNARY,
        ),
        Expression::DefinedOr { left, right, .. } => (
            format!(
                "{} ?: {}",
                render_expr(left, PREC_TERNARY),
                render_expr(right, PREC_TERNARY)
            ),
            PREC_TERNARY,
        ),
        Expression::Assignment {
            operator,
            left,
            right,
            is_weak_write,
            ..
        } => (
            {
                let mut out = format!(
                    "{} {} {}",
                    render_expr(left, PREC_ASSIGNMENT),
                    operator,
                    render_expr(right, PREC_ASSIGNMENT)
                );
                if *is_weak_write {
                    out.push_str(" but weak");
                }
                out
            },
            PREC_ASSIGNMENT,
        ),
        Expression::Call {
            callee, arguments, ..
        } => (
            format!(
                "{}{}",
                render_expr(callee, PREC_POSTFIX),
                render_call_arguments(arguments)
            ),
            PREC_POSTFIX,
        ),
        Expression::MemberAccess { object, member, .. } => (
            format!("{}.{}", render_expr(object, PREC_POSTFIX), member),
            PREC_POSTFIX,
        ),
        Expression::DynamicMemberCall {
            object,
            member,
            arguments,
            ..
        } => (
            format!(
                "{}.( {} ){}",
                render_expr(object, PREC_POSTFIX),
                render_expression(member),
                render_call_arguments(arguments)
            ),
            PREC_POSTFIX,
        ),
        Expression::Index { object, index, .. } => (
            format!(
                "{}[{}]",
                render_expr(object, PREC_POSTFIX),
                render_expression(index)
            ),
            PREC_POSTFIX,
        ),
        Expression::Slice {
            object, start, end, ..
        } => {
            let start = start
                .as_ref()
                .map(|expr| render_expression(expr))
                .unwrap_or_default();
            let end = end
                .as_ref()
                .map(|expr| render_expression(expr))
                .unwrap_or_default();
            (
                format!("{}[{}:{}]", render_expr(object, PREC_POSTFIX), start, end),
                PREC_POSTFIX,
            )
        }
        Expression::DictAccess { object, key, .. } => (
            format!(
                "{}{{{}}}",
                render_expr(object, PREC_POSTFIX),
                render_dict_access_key(key)
            ),
            PREC_POSTFIX,
        ),
        Expression::PostfixUpdate {
            operator, argument, ..
        } => (
            format!("{}{}", render_expr(argument, PREC_POSTFIX), operator),
            PREC_POSTFIX,
        ),
        Expression::Lambda {
            params,
            body,
            is_async,
            ..
        } => {
            let mut out = String::new();
            if *is_async {
                out.push_str("async ");
            }
            out.push_str("fn ");
            render_parameter_list(params, &mut out);
            out.push_str(" -> ");
            out.push_str(&render_expression(body));
            (out, PREC_ATOM)
        }
        Expression::FunctionExpression {
            params,
            return_type,
            body,
            is_async,
            ..
        } => (
            render_function_literal(params, return_type.as_deref(), body, *is_async),
            PREC_ATOM,
        ),
        Expression::LetExpression {
            kind,
            declared_type,
            name,
            init,
            is_weak_storage,
            ..
        } => {
            let mut out = String::new();
            out.push_str(kind);
            out.push(' ');
            if let Some(declared_type) = declared_type {
                out.push_str(declared_type);
                out.push(' ');
            }
            out.push_str(name);
            if let Some(init) = init {
                out.push_str(" := ");
                out.push_str(&render_expression(init));
            }
            if *is_weak_storage {
                out.push_str(" but weak");
            }
            (out, PREC_PREFIX)
        }
        Expression::TryExpression { body, handlers, .. } => {
            let mut out = String::from("try ");
            render_block_into(body, 0, &mut out);
            for handler in handlers {
                out.push(' ');
                render_catch_clause(handler, 0, &mut out);
            }
            (out, PREC_ATOM)
        }
        Expression::DoExpression { body, .. } => {
            let mut out = String::from("do ");
            render_block_into(body, 0, &mut out);
            (out, PREC_ATOM)
        }
        Expression::AwaitExpression { body, .. } => {
            let mut out = String::from("await ");
            render_block_into(body, 0, &mut out);
            (out, PREC_ATOM)
        }
        Expression::SpawnExpression { body, .. } => {
            let mut out = String::from("spawn ");
            render_block_into(body, 0, &mut out);
            (out, PREC_ATOM)
        }
        Expression::SuperCall { arguments, .. } => (
            format!("super{}", render_call_arguments(arguments)),
            PREC_ATOM,
        ),
    };
    if prec < parent_prec {
        format!("({text})")
    } else {
        text
    }
}

fn render_expression_list(expressions: &[Expression]) -> String {
    expressions
        .iter()
        .map(render_expression)
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_dict_entries(entries: &[DictEntry]) -> String {
    entries
        .iter()
        .map(|entry| {
            format!(
                "{}: {}",
                render_dict_literal_key(&entry.key),
                render_expression(&entry.value)
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_call_arguments(arguments: &[CallArgument]) -> String {
    if arguments.is_empty() {
        return "()".to_owned();
    }
    format!(
        "( {} )",
        arguments
            .iter()
            .map(|argument| match argument {
                CallArgument::Positional { value, .. } => render_expression(value),
                CallArgument::Named { name, value, .. } => {
                    format!("{}: {}", render_call_key(name), render_expression(value))
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn render_dict_literal_key(key: &DictKey) -> String {
    match key {
        DictKey::Identifier { name, .. } => name.clone(),
        DictKey::StringLiteral { value, .. } => quote_string(value),
        DictKey::Expression { expression, .. } => format!("({})", render_expression(expression)),
    }
}

fn render_dict_access_key(key: &DictKey) -> String {
    match key {
        DictKey::Identifier { name, .. } => name.clone(),
        DictKey::StringLiteral { value, .. } => quote_string(value),
        DictKey::Expression { expression, .. } => render_expression(expression),
    }
}

fn render_call_key(key: &DictKey) -> String {
    match key {
        DictKey::Identifier { name, .. } => name.clone(),
        DictKey::StringLiteral { value, .. } => quote_string(value),
        DictKey::Expression { expression, .. } => format!("({})", render_expression(expression)),
    }
}

fn render_template_literal(parts: &[TemplatePart]) -> String {
    let mut out = String::from("`");
    for part in parts {
        match part {
            TemplatePart::Text { value, .. } => out.push_str(&quote_template_text(value)),
            TemplatePart::Expression { expression, .. } => {
                out.push_str("${ ");
                out.push_str(&render_expression(expression));
                out.push_str(" }");
            }
        }
    }
    out.push('`');
    out
}

fn infix_precedence(operator: &str) -> u8 {
    match operator {
        "or" | "⋁" => PREC_OR,
        "xor" | "⊻" => PREC_XOR,
        "and" | "⋀" | "nand" | "⊼" => PREC_AND,
        "==" | "≡" | "!=" | "≢" => PREC_EQUALITY,
        "=" | "≠" | "<" | ">" | "<=" | "≤" | ">=" | "≥" | "<=>" | "≶" | "≷" | "eq" | "ne"
        | "gt" | "ge" | "lt" | "le" | "cmp" | "eqi" | "nei" | "gti" | "gei" | "lti" | "lei"
        | "cmpi" | "in" | "∈" | "∉" | "subsetof" | "⊂" | "supersetof" | "⊃" | "equivalentof"
        | "⊂⊃" | "instanceof" | "does" | "can" | "~" | "->" | "@" | "@?" | "@@" => {
            PREC_COMPARISON
        }
        "|" => PREC_BITWISE_OR,
        "^" => PREC_BITWISE_XOR,
        "&" => PREC_BITWISE_AND,
        "union" | "⋃" | "intersection" | "⋂" | "\\" | "∖" | "..." => PREC_SET,
        "_" => PREC_CONCAT,
        "+" | "-" => PREC_ADDITIVE,
        "*" | "/" | "×" | "÷" | "mod" => PREC_MULTIPLICATIVE,
        "**" => PREC_EXPONENT,
        _ => PREC_COMPARISON,
    }
}

fn is_right_associative(operator: &str) -> bool {
    operator == "**"
}

fn is_word_operator(operator: &str) -> bool {
    operator
        .chars()
        .all(|ch| ch == '_' || ch.is_ascii_alphabetic())
}

fn quote_string(value: &str) -> String {
    let escaped = value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t");
    format!("\"{escaped}\"")
}

fn quote_regex_pattern(pattern: &str) -> String {
    pattern.replace('\\', "\\\\").replace('/', "\\/")
}

fn quote_template_text(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('`', "\\`")
        .replace("${", "\\${")
}

fn push_indent(out: &mut String, indent: usize) {
    for _ in 0..indent {
        out.push('\t');
    }
}
