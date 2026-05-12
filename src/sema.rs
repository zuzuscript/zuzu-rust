use std::collections::{HashMap, HashSet};

use crate::ast::{
    CallArgument, CatchClause, ClassDeclaration, ClassMember, DictEntry, DictKey, Expression,
    FunctionDeclaration, ImportDeclaration, MethodDeclaration, Program, Statement, TemplatePart,
    TraitDeclaration,
};
use crate::error::{Result, ZuzuRustError};

#[derive(Clone, Copy)]
struct Context {
    in_function: bool,
    in_async: bool,
    in_loop: bool,
    in_switch: bool,
}

pub fn validate_program(program: &Program) -> Result<()> {
    let mut validator = Validator {
        scopes: vec![Scope::new_root()],
    };
    let context = Context {
        in_function: false,
        in_async: true,
        in_loop: false,
        in_switch: false,
    };
    validator.validate_statements(&program.statements, context)
}

pub fn weak_storage_warnings(program: &Program) -> Vec<String> {
    let mut warnings = Vec::new();
    collect_statement_warnings(&program.statements, &mut warnings);
    warnings
}

fn collect_statement_warnings(statements: &[Statement], warnings: &mut Vec<String>) {
    for statement in statements {
        match statement {
            Statement::Block(node) => collect_statement_warnings(&node.statements, warnings),
            Statement::VariableDeclaration(node) => {
                collect_weak_decl_warning(
                    node.is_weak_storage,
                    node.declared_type.as_deref(),
                    &node.name,
                    node.line,
                    warnings,
                );
                if let Some(init) = &node.init {
                    collect_expression_warnings(init, warnings);
                }
            }
            Statement::FunctionDeclaration(node) => {
                for param in &node.params {
                    if let Some(default_value) = &param.default_value {
                        collect_expression_warnings(default_value, warnings);
                    }
                }
                collect_statement_warnings(&node.body.statements, warnings);
            }
            Statement::ClassDeclaration(node) => {
                for member in &node.body {
                    match member {
                        ClassMember::Field(field) => {
                            if let Some(default_value) = &field.default_value {
                                collect_expression_warnings(default_value, warnings);
                            }
                        }
                        ClassMember::Method(method) => {
                            for param in &method.params {
                                if let Some(default_value) = &param.default_value {
                                    collect_expression_warnings(default_value, warnings);
                                }
                            }
                            collect_statement_warnings(&method.body.statements, warnings);
                        }
                        ClassMember::Class(class_decl) => collect_statement_warnings(
                            &[Statement::ClassDeclaration(class_decl.clone())],
                            warnings,
                        ),
                        ClassMember::Trait(trait_decl) => collect_statement_warnings(
                            &[Statement::TraitDeclaration(trait_decl.clone())],
                            warnings,
                        ),
                    }
                }
            }
            Statement::TraitDeclaration(node) => {
                for member in &node.body {
                    if let ClassMember::Method(method) = member {
                        for param in &method.params {
                            if let Some(default_value) = &param.default_value {
                                collect_expression_warnings(default_value, warnings);
                            }
                        }
                        collect_statement_warnings(&method.body.statements, warnings);
                    }
                }
            }
            Statement::ImportDeclaration(node) => {
                if let Some(condition) = &node.condition {
                    collect_expression_warnings(&condition.test, warnings);
                }
            }
            Statement::IfStatement(node) => {
                collect_expression_warnings(&node.test, warnings);
                collect_statement_warnings(&node.consequent.statements, warnings);
                if let Some(alternate) = &node.alternate {
                    collect_statement_warnings(&[*alternate.clone()], warnings);
                }
            }
            Statement::WhileStatement(node) => {
                collect_expression_warnings(&node.test, warnings);
                collect_statement_warnings(&node.body.statements, warnings);
            }
            Statement::ForStatement(node) => {
                collect_expression_warnings(&node.iterable, warnings);
                collect_statement_warnings(&node.body.statements, warnings);
                if let Some(else_block) = &node.else_block {
                    collect_statement_warnings(&else_block.statements, warnings);
                }
            }
            Statement::SwitchStatement(node) => {
                collect_expression_warnings(&node.discriminant, warnings);
                for case in &node.cases {
                    for value in &case.values {
                        collect_expression_warnings(value, warnings);
                    }
                    collect_statement_warnings(&case.consequent, warnings);
                }
                if let Some(default) = &node.default {
                    collect_statement_warnings(default, warnings);
                }
            }
            Statement::TryStatement(node) => {
                collect_statement_warnings(&node.body.statements, warnings);
                for handler in &node.handlers {
                    collect_statement_warnings(&handler.body.statements, warnings);
                }
            }
            Statement::ReturnStatement(node) => {
                if let Some(argument) = &node.argument {
                    collect_expression_warnings(argument, warnings);
                }
            }
            Statement::ThrowStatement(node) => {
                collect_expression_warnings(&node.argument, warnings);
            }
            Statement::DieStatement(node) => collect_expression_warnings(&node.argument, warnings),
            Statement::PostfixConditionalStatement(node) => {
                collect_statement_warnings(&[(*node.statement).clone()], warnings);
                collect_expression_warnings(&node.test, warnings);
            }
            Statement::KeywordStatement(node) => {
                for argument in &node.arguments {
                    collect_expression_warnings(argument, warnings);
                }
            }
            Statement::ExpressionStatement(node) => {
                collect_expression_warnings(&node.expression, warnings);
            }
            Statement::LoopControlStatement(_) => {}
        }
    }
}

fn collect_expression_warnings(expression: &Expression, warnings: &mut Vec<String>) {
    match expression {
        Expression::ArrayLiteral { elements, .. }
        | Expression::SetLiteral { elements, .. }
        | Expression::BagLiteral { elements, .. } => {
            for element in elements {
                collect_expression_warnings(element, warnings);
            }
        }
        Expression::DictLiteral { entries, .. } | Expression::PairListLiteral { entries, .. } => {
            for entry in entries {
                collect_dict_key_warnings(&entry.key, warnings);
                collect_expression_warnings(&entry.value, warnings);
            }
        }
        Expression::TemplateLiteral { parts, .. } => {
            for part in parts {
                if let TemplatePart::Expression { expression, .. } = part {
                    collect_expression_warnings(expression, warnings);
                }
            }
        }
        Expression::Unary { argument, .. } | Expression::PostfixUpdate { argument, .. } => {
            collect_expression_warnings(argument, warnings);
        }
        Expression::Binary { left, right, .. }
        | Expression::DefinedOr { left, right, .. }
        | Expression::Assignment { left, right, .. } => {
            collect_expression_warnings(left, warnings);
            collect_expression_warnings(right, warnings);
        }
        Expression::Ternary {
            test,
            consequent,
            alternate,
            ..
        } => {
            collect_expression_warnings(test, warnings);
            collect_expression_warnings(consequent, warnings);
            collect_expression_warnings(alternate, warnings);
        }
        Expression::Call {
            callee, arguments, ..
        } => {
            collect_expression_warnings(callee, warnings);
            collect_call_argument_warnings(arguments, warnings);
        }
        Expression::MemberAccess { object, .. } => {
            collect_expression_warnings(object, warnings);
        }
        Expression::DynamicMemberCall {
            object,
            member,
            arguments,
            ..
        } => {
            collect_expression_warnings(object, warnings);
            collect_expression_warnings(member, warnings);
            collect_call_argument_warnings(arguments, warnings);
        }
        Expression::Index { object, index, .. } => {
            collect_expression_warnings(object, warnings);
            collect_expression_warnings(index, warnings);
        }
        Expression::Slice {
            object, start, end, ..
        } => {
            collect_expression_warnings(object, warnings);
            if let Some(start) = start {
                collect_expression_warnings(start, warnings);
            }
            if let Some(end) = end {
                collect_expression_warnings(end, warnings);
            }
        }
        Expression::DictAccess { object, key, .. } => {
            collect_expression_warnings(object, warnings);
            collect_dict_key_warnings(key, warnings);
        }
        Expression::Lambda { body, .. } => collect_expression_warnings(body, warnings),
        Expression::FunctionExpression { params, body, .. } => {
            for param in params {
                if let Some(default_value) = &param.default_value {
                    collect_expression_warnings(default_value, warnings);
                }
            }
            collect_statement_warnings(&body.statements, warnings);
        }
        Expression::LetExpression {
            line,
            declared_type,
            name,
            init,
            is_weak_storage,
            ..
        } => {
            collect_weak_decl_warning(
                *is_weak_storage,
                declared_type.as_deref(),
                name,
                *line,
                warnings,
            );
            if let Some(init) = init {
                collect_expression_warnings(init, warnings);
            }
        }
        Expression::TryExpression { body, handlers, .. } => {
            collect_statement_warnings(&body.statements, warnings);
            for handler in handlers {
                collect_statement_warnings(&handler.body.statements, warnings);
            }
        }
        Expression::DoExpression { body, .. }
        | Expression::AwaitExpression { body, .. }
        | Expression::SpawnExpression { body, .. } => {
            collect_statement_warnings(&body.statements, warnings);
        }
        Expression::SuperCall { arguments, .. } => {
            collect_call_argument_warnings(arguments, warnings);
        }
        Expression::Identifier { .. }
        | Expression::NumberLiteral { .. }
        | Expression::StringLiteral { .. }
        | Expression::RegexLiteral { .. }
        | Expression::BooleanLiteral { .. }
        | Expression::NullLiteral { .. } => {}
    }
}

fn collect_call_argument_warnings(arguments: &[CallArgument], warnings: &mut Vec<String>) {
    for argument in arguments {
        match argument {
            CallArgument::Positional { value, .. } => collect_expression_warnings(value, warnings),
            CallArgument::Named { name, value, .. } => {
                collect_dict_key_warnings(name, warnings);
                collect_expression_warnings(value, warnings);
            }
        }
    }
}

fn collect_dict_key_warnings(key: &DictKey, warnings: &mut Vec<String>) {
    if let DictKey::Expression { expression, .. } = key {
        collect_expression_warnings(expression, warnings);
    }
}

fn collect_weak_decl_warning(
    is_weak_storage: bool,
    declared_type: Option<&str>,
    name: &str,
    line: usize,
    warnings: &mut Vec<String>,
) {
    let Some(declared_type) = declared_type else {
        return;
    };
    if !is_weak_storage || !is_unweakable_declared_type(declared_type) {
        return;
    }
    warnings.push(format!(
        "SemanticWarning at line {line}: 'but weak' on {declared_type} binding '{name}' has no effect because {declared_type} values are scalar"
    ));
}

fn is_unweakable_declared_type(declared_type: &str) -> bool {
    matches!(
        declared_type,
        "Number" | "String" | "BinaryString" | "Boolean" | "Null" | "Regexp"
    )
}

struct Validator {
    scopes: Vec<Scope>,
}

struct Scope {
    names: HashMap<String, BindingInfo>,
    has_wildcard_import: bool,
}

#[derive(Clone, Copy)]
struct BindingInfo {
    mutable: bool,
}

impl Scope {
    fn new_root() -> Self {
        let mut names = HashMap::new();
        for builtin in [
            "Exception",
            "AssertionException",
            "TypeException",
            "CancelledException",
            "TimeoutException",
            "ChannelClosedException",
            "Array",
            "Dict",
            "PairList",
            "Set",
            "Bag",
            "Pair",
            "String",
            "BinaryString",
            "Number",
            "Boolean",
            "Regexp",
            "Function",
            "__file__",
            "__global__",
            "__system__",
        ] {
            names.insert(builtin.to_owned(), BindingInfo { mutable: false });
        }
        Self {
            names,
            has_wildcard_import: false,
        }
    }

    fn new_child() -> Self {
        Self {
            names: HashMap::new(),
            has_wildcard_import: false,
        }
    }
}

impl Validator {
    fn validate_statements(&mut self, statements: &[Statement], context: Context) -> Result<()> {
        for statement in statements {
            self.validate_statement(statement, context)?;
        }
        Ok(())
    }

    fn validate_statement(&mut self, statement: &Statement, context: Context) -> Result<()> {
        match statement {
            Statement::Block(block) => {
                self.with_scope(|this| this.validate_statements(&block.statements, context))
            }
            Statement::VariableDeclaration(node) => {
                if let Some(init) = &node.init {
                    self.validate_expression(init, context)?;
                }
                self.declare_name(&node.name, node.line, node.kind != "const")
            }
            Statement::FunctionDeclaration(node) => {
                self.declare_name(&node.name, node.line, false)?;
                self.validate_function(node)
            }
            Statement::ClassDeclaration(node) => {
                self.declare_name(&node.name, node.line, false)?;
                self.validate_class(node)
            }
            Statement::TraitDeclaration(node) => {
                self.declare_name(&node.name, node.line, false)?;
                self.validate_trait(node)
            }
            Statement::ImportDeclaration(node) => self.validate_import(node),
            Statement::IfStatement(node) => {
                self.validate_expression(&node.test, context)?;
                self.validate_statement(&Statement::Block(node.consequent.clone()), context)?;
                if let Some(alternate) = &node.alternate {
                    self.validate_statement(alternate, context)?;
                }
                Ok(())
            }
            Statement::WhileStatement(node) => {
                self.validate_expression(&node.test, context)?;
                self.validate_statement(
                    &Statement::Block(node.body.clone()),
                    Context {
                        in_loop: true,
                        in_switch: false,
                        ..context
                    },
                )
            }
            Statement::ForStatement(node) => {
                self.validate_expression(&node.iterable, context)?;
                self.with_scope(|this| {
                    if let Some(kind) = &node.binding_kind {
                        this.declare_name(&node.variable, node.line, kind != "const")?;
                    } else {
                        this.declare_name(&node.variable, node.line, true)?;
                    }
                    this.validate_statements(
                        &node.body.statements,
                        Context {
                            in_loop: true,
                            in_switch: false,
                            ..context
                        },
                    )
                })?;
                if let Some(else_block) = &node.else_block {
                    self.validate_statement(&Statement::Block(else_block.clone()), context)?;
                }
                Ok(())
            }
            Statement::SwitchStatement(node) => {
                self.validate_expression(&node.discriminant, context)?;
                for case in &node.cases {
                    for value in &case.values {
                        self.validate_expression(value, context)?;
                    }
                    self.with_scope(|this| {
                        this.validate_statements(
                            &case.consequent,
                            Context {
                                in_switch: true,
                                ..context
                            },
                        )
                    })?;
                }
                if let Some(default) = &node.default {
                    self.with_scope(|this| {
                        this.validate_statements(
                            default,
                            Context {
                                in_switch: true,
                                ..context
                            },
                        )
                    })?;
                }
                Ok(())
            }
            Statement::TryStatement(node) => {
                self.validate_statement(&Statement::Block(node.body.clone()), context)?;
                for handler in &node.handlers {
                    self.validate_catch_clause(handler, context)?;
                }
                Ok(())
            }
            Statement::ReturnStatement(node) => {
                if !context.in_function {
                    return Err(ZuzuRustError::semantic(
                        "return is not valid outside function scope",
                        node.line,
                    ));
                }
                if let Some(argument) = &node.argument {
                    self.validate_expression(argument, context)?;
                }
                Ok(())
            }
            Statement::LoopControlStatement(node) => {
                if node.keyword == "continue" {
                    if !context.in_loop && !context.in_switch {
                        return Err(ZuzuRustError::semantic(
                            format!("{} is not valid outside loop scope", node.keyword),
                            node.line,
                        ));
                    }
                    return Ok(());
                }
                if !context.in_loop {
                    return Err(ZuzuRustError::semantic(
                        format!("{} is not valid outside loop scope", node.keyword),
                        node.line,
                    ));
                }
                Ok(())
            }
            Statement::ThrowStatement(node) => self.validate_expression(&node.argument, context),
            Statement::DieStatement(node) => self.validate_expression(&node.argument, context),
            Statement::PostfixConditionalStatement(node) => {
                self.validate_statement(&node.statement, context)?;
                self.validate_expression(&node.test, context)
            }
            Statement::KeywordStatement(node) => {
                for argument in &node.arguments {
                    self.validate_expression(argument, context)?;
                }
                Ok(())
            }
            Statement::ExpressionStatement(node) => {
                self.validate_expression(&node.expression, context)
            }
        }
    }

    fn validate_function(&mut self, node: &FunctionDeclaration) -> Result<()> {
        self.with_scope(|this| {
            for param in &node.params {
                if let Some(default_value) = &param.default_value {
                    this.validate_expression(
                        default_value,
                        Context {
                            in_function: true,
                            in_async: node.is_async,
                            in_loop: false,
                            in_switch: false,
                        },
                    )?;
                }
                this.declare_name(&param.name, param.line, true)?;
            }
            this.validate_statements(
                &node.body.statements,
                Context {
                    in_function: true,
                    in_async: node.is_async,
                    in_loop: false,
                    in_switch: false,
                },
            )
        })
    }

    fn validate_class(&mut self, node: &ClassDeclaration) -> Result<()> {
        self.with_scope(|this| {
            let mut method_names = HashSet::new();
            let mut type_member_names = HashSet::new();
            for member in &node.body {
                match member {
                    ClassMember::Field(field) => {
                        if let Some(value) = &field.default_value {
                            this.validate_expression(
                                value,
                                Context {
                                    in_function: false,
                                    in_async: false,
                                    in_loop: false,
                                    in_switch: false,
                                },
                            )?;
                        }
                        this.declare_name(&field.name, field.line, field.kind != "const")?;
                    }
                    ClassMember::Method(method) => {
                        if !method_names.insert(method.name.clone()) {
                            return Err(ZuzuRustError::semantic(
                                format!("Redeclaration of '{}' in the same scope", method.name),
                                method.line,
                            ));
                        }
                        this.validate_method(method)?;
                    }
                    ClassMember::Class(class_decl) => {
                        if !type_member_names.insert(class_decl.name.clone()) {
                            return Err(ZuzuRustError::semantic(
                                format!("Redeclaration of '{}' in the same scope", class_decl.name),
                                class_decl.line,
                            ));
                        }
                        this.validate_class(class_decl)?;
                    }
                    ClassMember::Trait(trait_decl) => {
                        if !type_member_names.insert(trait_decl.name.clone()) {
                            return Err(ZuzuRustError::semantic(
                                format!("Redeclaration of '{}' in the same scope", trait_decl.name),
                                trait_decl.line,
                            ));
                        }
                        this.validate_trait(trait_decl)?;
                    }
                }
            }
            Ok(())
        })
    }

    fn validate_trait(&mut self, node: &TraitDeclaration) -> Result<()> {
        self.with_scope(|this| {
            for member in &node.body {
                match member {
                    ClassMember::Method(method) => {
                        this.declare_name(&method.name, method.line, false)?;
                        this.validate_method(method)?;
                    }
                    ClassMember::Class(class_decl) => {
                        this.declare_name(&class_decl.name, class_decl.line, false)?;
                        this.validate_class(class_decl)?;
                    }
                    ClassMember::Trait(trait_decl) => {
                        this.declare_name(&trait_decl.name, trait_decl.line, false)?;
                        this.validate_trait(trait_decl)?;
                    }
                    ClassMember::Field(field) => {
                        return Err(ZuzuRustError::semantic(
                            format!("field '{}' is not valid inside trait body", field.name),
                            field.line,
                        ));
                    }
                }
            }
            Ok(())
        })
    }

    fn validate_method(&mut self, node: &MethodDeclaration) -> Result<()> {
        self.with_scope(|this| {
            this.declare_name("self", node.line, true)?;
            this.declare_name("this", node.line, true)?;
            for param in &node.params {
                if let Some(default_value) = &param.default_value {
                    this.validate_expression(
                        default_value,
                        Context {
                            in_function: true,
                            in_async: node.is_async,
                            in_loop: false,
                            in_switch: false,
                        },
                    )?;
                }
                this.declare_name(&param.name, param.line, true)?;
            }
            this.validate_statements(
                &node.body.statements,
                Context {
                    in_function: true,
                    in_async: node.is_async,
                    in_loop: false,
                    in_switch: false,
                },
            )
        })
    }

    fn validate_import(&mut self, node: &ImportDeclaration) -> Result<()> {
        if node.import_all {
            self.scopes
                .last_mut()
                .expect("scope stack should not be empty")
                .has_wildcard_import = true;
        }
        for specifier in &node.specifiers {
            self.declare_name(&specifier.local, specifier.line, true)?;
        }
        if let Some(condition) = &node.condition {
            self.validate_expression(
                &condition.test,
                Context {
                    in_function: false,
                    in_async: false,
                    in_loop: false,
                    in_switch: false,
                },
            )?;
        }
        Ok(())
    }

    fn validate_catch_clause(&mut self, clause: &CatchClause, context: Context) -> Result<()> {
        self.with_scope(|this| {
            if let Some(binding) = &clause.binding {
                if let Some(name) = &binding.name {
                    this.declare_name(name, binding.line, true)?;
                }
            }
            this.validate_statements(&clause.body.statements, context)
        })
    }

    fn validate_expression(&mut self, expression: &Expression, context: Context) -> Result<()> {
        match expression {
            Expression::Identifier { name, line, .. } => self.require_name(name, *line),
            Expression::NumberLiteral { .. }
            | Expression::StringLiteral { .. }
            | Expression::RegexLiteral { .. }
            | Expression::BooleanLiteral { .. }
            | Expression::NullLiteral { .. } => Ok(()),
            Expression::ArrayLiteral { elements, .. }
            | Expression::SetLiteral { elements, .. }
            | Expression::BagLiteral { elements, .. } => {
                for element in elements {
                    self.validate_expression(element, context)?;
                }
                Ok(())
            }
            Expression::DictLiteral { entries, .. }
            | Expression::PairListLiteral { entries, .. } => {
                for entry in entries {
                    self.validate_dict_entry(entry, context)?;
                }
                Ok(())
            }
            Expression::TemplateLiteral { parts, .. } => {
                for part in parts {
                    self.validate_template_part(part, context)?;
                }
                Ok(())
            }
            Expression::Unary {
                line,
                operator,
                argument,
                ..
            } => {
                if operator == "++" || operator == "--" || operator == "\\" {
                    if !is_assignable(argument) {
                        return Err(ZuzuRustError::semantic(
                            format!("invalid target for unary operator '{}'", operator),
                            *line,
                        ));
                    }
                    self.require_mutable_target(argument, *line)?;
                }
                self.validate_expression(argument, context)
            }
            Expression::Binary {
                operator,
                left,
                right,
                ..
            } => {
                self.validate_expression(left, context)?;
                if operator == "can"
                    && matches!(
                        right.as_ref(),
                        Expression::Identifier { .. } | Expression::StringLiteral { .. }
                    )
                {
                    Ok(())
                } else {
                    self.validate_expression(right, context)
                }
            }
            Expression::Ternary {
                test,
                consequent,
                alternate,
                ..
            } => {
                self.validate_expression(test, context)?;
                self.validate_expression(consequent, context)?;
                self.validate_expression(alternate, context)
            }
            Expression::DefinedOr { left, right, .. } => {
                self.validate_expression(left, context)?;
                self.validate_expression(right, context)
            }
            Expression::Assignment {
                line,
                operator,
                left,
                right,
                ..
            } => {
                if !is_assignable(left) {
                    return Err(ZuzuRustError::semantic("invalid assignment target", *line));
                }
                self.require_mutable_target(left, *line)?;
                self.validate_expression(left, context)?;
                if operator == "~=" {
                    return self.validate_regex_replacement_expression(right, context);
                }
                self.validate_expression(right, context)
            }
            Expression::Call {
                callee, arguments, ..
            } => {
                self.validate_expression(callee, context)?;
                for argument in arguments {
                    self.validate_call_argument(argument, context)?;
                }
                Ok(())
            }
            Expression::MemberAccess { object, .. } => self.validate_expression(object, context),
            Expression::DynamicMemberCall {
                object,
                member,
                arguments,
                ..
            } => {
                self.validate_expression(object, context)?;
                self.validate_expression(member, context)?;
                for argument in arguments {
                    self.validate_call_argument(argument, context)?;
                }
                Ok(())
            }
            Expression::Index { object, index, .. } => {
                self.validate_expression(object, context)?;
                self.validate_expression(index, context)
            }
            Expression::Slice {
                object, start, end, ..
            } => {
                self.validate_expression(object, context)?;
                if let Some(start) = start {
                    self.validate_expression(start, context)?;
                }
                if let Some(end) = end {
                    self.validate_expression(end, context)?;
                }
                Ok(())
            }
            Expression::DictAccess { object, key, .. } => {
                self.validate_expression(object, context)?;
                self.validate_dict_key(key, context)
            }
            Expression::PostfixUpdate {
                line,
                operator,
                argument,
                ..
            } => {
                if !is_assignable(argument) {
                    return Err(ZuzuRustError::semantic(
                        format!("invalid target for postfix operator '{}'", operator),
                        *line,
                    ));
                }
                self.require_mutable_target(argument, *line)?;
                self.validate_expression(argument, context)
            }
            Expression::Lambda {
                params,
                body,
                is_async,
                ..
            } => self.with_scope(|this| {
                for param in params {
                    if let Some(default_value) = &param.default_value {
                        this.validate_expression(
                            default_value,
                            Context {
                                in_function: true,
                                in_async: *is_async,
                                in_loop: false,
                                in_switch: false,
                            },
                        )?;
                    }
                    this.declare_name(&param.name, param.line, true)?;
                }
                this.validate_expression(
                    body,
                    Context {
                        in_function: true,
                        in_async: *is_async,
                        in_loop: false,
                        in_switch: false,
                    },
                )
            }),
            Expression::FunctionExpression {
                params,
                body,
                is_async,
                ..
            } => self.with_scope(|this| {
                for param in params {
                    if let Some(default_value) = &param.default_value {
                        this.validate_expression(
                            default_value,
                            Context {
                                in_function: true,
                                in_async: *is_async,
                                in_loop: false,
                                in_switch: false,
                            },
                        )?;
                    }
                    this.declare_name(&param.name, param.line, true)?;
                }
                this.validate_statements(
                    &body.statements,
                    Context {
                        in_function: true,
                        in_async: *is_async,
                        in_loop: false,
                        in_switch: false,
                    },
                )
            }),
            Expression::LetExpression {
                kind,
                init,
                name,
                line,
                ..
            } => self.with_scope(|this| {
                if let Some(init) = init {
                    this.validate_expression(init, context)?;
                }
                this.declare_name(name, *line, kind != "const")
            }),
            Expression::TryExpression { body, handlers, .. } => {
                self.validate_statements(&body.statements, context)?;
                for handler in handlers {
                    self.validate_catch_clause(handler, context)?;
                }
                Ok(())
            }
            Expression::DoExpression { body, .. } => {
                self.with_scope(|this| this.validate_statements(&body.statements, context))
            }
            Expression::AwaitExpression { line, body, .. } => {
                if !context.in_async {
                    return Err(ZuzuRustError::semantic(
                        "await may only be used inside async code",
                        *line,
                    ));
                }
                self.with_scope(|this| this.validate_statements(&body.statements, context))
            }
            Expression::SpawnExpression { body, .. } => self.with_scope(|this| {
                this.validate_statements(
                    &body.statements,
                    Context {
                        in_async: true,
                        ..context
                    },
                )
            }),
            Expression::SuperCall { arguments, .. } => {
                for argument in arguments {
                    self.validate_call_argument(argument, context)?;
                }
                Ok(())
            }
        }
    }

    fn validate_template_part(&mut self, part: &TemplatePart, context: Context) -> Result<()> {
        match part {
            TemplatePart::Text { .. } => Ok(()),
            TemplatePart::Expression { expression, .. } => {
                self.validate_expression(expression, context)
            }
        }
    }

    fn validate_dict_entry(&mut self, entry: &DictEntry, context: Context) -> Result<()> {
        self.validate_dict_key(&entry.key, context)?;
        self.validate_expression(&entry.value, context)
    }

    fn validate_regex_replacement_expression(
        &mut self,
        expression: &Expression,
        context: Context,
    ) -> Result<()> {
        match expression {
            Expression::Binary {
                operator,
                left,
                right,
                ..
            } if operator == "->" => {
                self.validate_expression(left, context)?;
                self.with_scope(|this| {
                    this.declare_name("m", right.line(), true)?;
                    this.validate_expression(right, context)
                })
            }
            _ => self.validate_expression(expression, context),
        }
    }

    fn validate_dict_key(&mut self, key: &DictKey, context: Context) -> Result<()> {
        match key {
            DictKey::Identifier { .. } | DictKey::StringLiteral { .. } => Ok(()),
            DictKey::Expression { expression, .. } => self.validate_expression(expression, context),
        }
    }

    fn validate_call_argument(&mut self, argument: &CallArgument, context: Context) -> Result<()> {
        match argument {
            CallArgument::Positional { value, .. } => self.validate_expression(value, context),
            CallArgument::Named { name, value, .. } => {
                self.validate_dict_key(name, context)?;
                self.validate_expression(value, context)
            }
        }
    }

    fn declare_name(&mut self, name: &str, line: usize, mutable: bool) -> Result<()> {
        let scope = self
            .scopes
            .last_mut()
            .expect("scope stack should not be empty");
        if scope.names.contains_key(name) {
            return Err(ZuzuRustError::semantic(
                format!("Redeclaration of '{}' in the same scope", name),
                line,
            ));
        }
        scope.names.insert(name.to_owned(), BindingInfo { mutable });
        Ok(())
    }

    fn require_name(&self, name: &str, line: usize) -> Result<()> {
        if name == "__argc__" || is_implicit_builtin_name(name) {
            return Ok(());
        }
        for scope in self.scopes.iter().rev() {
            if scope.names.contains_key(name) {
                return Ok(());
            }
            if scope.has_wildcard_import {
                return Ok(());
            }
        }
        Err(ZuzuRustError::semantic(
            format!("Use of undeclared identifier '{}' (compile-time)", name),
            line,
        ))
    }

    fn require_mutable_target(&self, expression: &Expression, line: usize) -> Result<()> {
        if let Expression::Identifier { name, .. } = expression {
            if let Some(binding) = self.lookup_binding(name) {
                if !binding.mutable {
                    return Err(ZuzuRustError::semantic(
                        format!("cannot modify const binding '{}'", name),
                        line,
                    ));
                }
            }
        }
        Ok(())
    }

    fn lookup_binding(&self, name: &str) -> Option<BindingInfo> {
        for scope in self.scopes.iter().rev() {
            if let Some(binding) = scope.names.get(name) {
                return Some(*binding);
            }
            if scope.has_wildcard_import {
                return None;
            }
        }
        None
    }

    fn with_scope<T>(&mut self, f: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        self.scopes.push(Scope::new_child());
        let result = f(self);
        self.scopes.pop();
        result
    }
}

fn is_implicit_builtin_name(name: &str) -> bool {
    matches!(
        name,
        "class_name"
            | "object_slots"
            | "ansi_esc"
            | "ref_id"
            | "setprop"
            | "getprop"
            | "to_binary"
            | "to_string"
            | "make_instance"
    )
}

fn is_assignable(expression: &Expression) -> bool {
    match expression {
        Expression::Identifier { .. } => true,
        Expression::MemberAccess { .. } => true,
        Expression::Index { .. } => true,
        Expression::Slice { .. } => true,
        Expression::DictAccess { .. } => true,
        Expression::Binary {
            operator,
            left: _,
            right: _,
            ..
        } if operator == "@" || operator == "@@" || operator == "@?" => true,
        _ => false,
    }
}
