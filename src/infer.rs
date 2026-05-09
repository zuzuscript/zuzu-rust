use std::collections::HashMap;

use crate::ast::{
    BlockStatement, CallArgument, CatchClause, ClassDeclaration, ClassMember, DictEntry, DictKey,
    Expression, FunctionDeclaration, MethodDeclaration, Program, Statement, TemplatePart,
};

pub fn annotate_program(program: &mut Program) {
    let mut inferencer = Inferencer::new(program);
    inferencer.infer_statements(&mut program.statements);
}

struct Inferencer {
    scopes: Vec<HashMap<String, String>>,
    function_returns: HashMap<String, String>,
    method_returns: HashMap<String, HashMap<String, String>>,
    current_return_type: Option<String>,
}

impl Inferencer {
    fn new(program: &Program) -> Self {
        let mut inferencer = Self {
            scopes: vec![HashMap::new()],
            function_returns: HashMap::new(),
            method_returns: HashMap::new(),
            current_return_type: None,
        };
        inferencer.collect_signatures(&program.statements);
        inferencer
    }

    fn collect_signatures(&mut self, statements: &[Statement]) {
        for statement in statements {
            match statement {
                Statement::FunctionDeclaration(node) => {
                    if let Some(return_type) = &node.return_type {
                        self.function_returns
                            .insert(node.name.clone(), return_type.clone());
                    }
                }
                Statement::ClassDeclaration(node) => self.collect_class_signatures(node),
                Statement::Block(node) => self.collect_signatures(&node.statements),
                _ => {}
            }
        }
    }

    fn collect_class_signatures(&mut self, node: &ClassDeclaration) {
        let mut methods = HashMap::new();
        for member in &node.body {
            match member {
                ClassMember::Method(method) => {
                    if let Some(return_type) = &method.return_type {
                        methods.insert(method.name.clone(), return_type.clone());
                    }
                }
                ClassMember::Class(class_decl) => self.collect_class_signatures(class_decl),
                _ => {}
            }
        }
        if !methods.is_empty() {
            self.method_returns.insert(node.name.clone(), methods);
        }
    }

    fn infer_statements(&mut self, statements: &mut [Statement]) {
        for statement in statements {
            self.infer_statement(statement);
        }
    }

    fn infer_statement(&mut self, statement: &mut Statement) {
        match statement {
            Statement::Block(node) => self.with_scope(|this| {
                this.infer_statements(&mut node.statements);
            }),
            Statement::VariableDeclaration(node) => {
                if let Some(init) = &mut node.init {
                    this_infer_expression(self, init);
                }
                node.runtime_typecheck_required = typecheck_requirement(
                    node.declared_type.as_deref(),
                    node.init.as_ref().and_then(Expression::inferred_type),
                );
                if let Some(var_type) = node.declared_type.clone().or_else(|| {
                    node.init
                        .as_ref()
                        .and_then(|expr| expr.inferred_type().map(ToOwned::to_owned))
                }) {
                    self.bind(node.name.clone(), var_type);
                }
            }
            Statement::FunctionDeclaration(node) => {
                self.bind(node.name.clone(), "Function".to_owned());
                self.infer_function(node);
            }
            Statement::ClassDeclaration(node) => {
                self.bind(node.name.clone(), node.name.clone());
                self.infer_class(node);
            }
            Statement::TraitDeclaration(node) => {
                self.bind(node.name.clone(), "Trait".to_owned());
                self.with_scope(|this| {
                    for member in &mut node.body {
                        if let ClassMember::Method(method) = member {
                            this.infer_method(method);
                        }
                    }
                });
            }
            Statement::ImportDeclaration(node) => {
                for specifier in &node.specifiers {
                    self.bind(specifier.local.clone(), "Imported".to_owned());
                }
            }
            Statement::IfStatement(node) => {
                this_infer_expression(self, &mut node.test);
                self.with_scope(|this| this.infer_statements(&mut node.consequent.statements));
                if let Some(alternate) = &mut node.alternate {
                    self.with_scope(|this| this.infer_statement(alternate));
                }
            }
            Statement::WhileStatement(node) => {
                this_infer_expression(self, &mut node.test);
                self.with_scope(|this| this.infer_statements(&mut node.body.statements));
            }
            Statement::ForStatement(node) => {
                this_infer_expression(self, &mut node.iterable);
                self.with_scope(|this| {
                    this.bind(node.variable.clone(), "Unknown".to_owned());
                    this.infer_statements(&mut node.body.statements);
                });
                if let Some(else_block) = &mut node.else_block {
                    self.with_scope(|this| this.infer_statements(&mut else_block.statements));
                }
            }
            Statement::SwitchStatement(node) => {
                this_infer_expression(self, &mut node.discriminant);
                for case in &mut node.cases {
                    for value in &mut case.values {
                        this_infer_expression(self, value);
                    }
                    self.with_scope(|this| this.infer_statements(&mut case.consequent));
                }
                if let Some(default) = &mut node.default {
                    self.with_scope(|this| this.infer_statements(default));
                }
            }
            Statement::TryStatement(node) => {
                self.with_scope(|this| this.infer_statements(&mut node.body.statements));
                for handler in &mut node.handlers {
                    self.infer_catch(handler);
                }
            }
            Statement::ReturnStatement(node) => {
                if let Some(argument) = &mut node.argument {
                    this_infer_expression(self, argument);
                }
                node.runtime_typecheck_required = typecheck_requirement(
                    self.current_return_type.as_deref(),
                    node.argument.as_ref().and_then(Expression::inferred_type),
                );
            }
            Statement::ThrowStatement(node) => this_infer_expression(self, &mut node.argument),
            Statement::DieStatement(node) => this_infer_expression(self, &mut node.argument),
            Statement::PostfixConditionalStatement(node) => {
                self.infer_statement(&mut node.statement);
                this_infer_expression(self, &mut node.test);
            }
            Statement::KeywordStatement(node) => {
                for argument in &mut node.arguments {
                    this_infer_expression(self, argument);
                }
            }
            Statement::ExpressionStatement(node) => {
                this_infer_expression(self, &mut node.expression);
            }
            Statement::LoopControlStatement(_) => {}
        }
    }

    fn infer_function(&mut self, node: &mut FunctionDeclaration) {
        let previous = self.current_return_type.clone();
        self.current_return_type = node.return_type.clone();
        self.with_scope(|this| {
            for param in &mut node.params {
                if let Some(default_value) = &mut param.default_value {
                    this_infer_expression(this, default_value);
                }
                if let Some(param_type) = param.declared_type.clone().or_else(|| {
                    param
                        .default_value
                        .as_ref()
                        .and_then(|expr| expr.inferred_type().map(ToOwned::to_owned))
                }) {
                    this.bind(param.name.clone(), param_type);
                }
            }
            this.infer_statements(&mut node.body.statements);
        });
        self.current_return_type = previous;
    }

    fn infer_method(&mut self, node: &mut MethodDeclaration) {
        let previous = self.current_return_type.clone();
        self.current_return_type = node.return_type.clone();
        self.with_scope(|this| {
            for param in &mut node.params {
                if let Some(default_value) = &mut param.default_value {
                    this_infer_expression(this, default_value);
                }
                if let Some(param_type) = param.declared_type.clone().or_else(|| {
                    param
                        .default_value
                        .as_ref()
                        .and_then(|expr| expr.inferred_type().map(ToOwned::to_owned))
                }) {
                    this.bind(param.name.clone(), param_type);
                }
            }
            this.infer_statements(&mut node.body.statements);
        });
        self.current_return_type = previous;
    }

    fn infer_class(&mut self, node: &mut ClassDeclaration) {
        self.with_scope(|this| {
            for member in &mut node.body {
                match member {
                    ClassMember::Field(field) => {
                        if let Some(default_value) = &mut field.default_value {
                            this_infer_expression(this, default_value);
                        }
                        field.runtime_typecheck_required = typecheck_requirement(
                            field.declared_type.as_deref(),
                            field
                                .default_value
                                .as_ref()
                                .and_then(Expression::inferred_type),
                        );
                        if let Some(field_type) = field.declared_type.clone().or_else(|| {
                            field
                                .default_value
                                .as_ref()
                                .and_then(|expr| expr.inferred_type().map(ToOwned::to_owned))
                        }) {
                            this.bind(field.name.clone(), field_type);
                        }
                    }
                    ClassMember::Method(method) => this.infer_method(method),
                    ClassMember::Class(class_decl) => this.infer_class(class_decl),
                    ClassMember::Trait(_) => {}
                }
            }
        });
    }

    fn infer_catch(&mut self, clause: &mut CatchClause) {
        self.with_scope(|this| {
            if let Some(binding) = &clause.binding {
                if let Some(name) = &binding.name {
                    let ty = binding
                        .declared_type
                        .clone()
                        .unwrap_or_else(|| "Exception".to_owned());
                    this.bind(name.clone(), ty);
                }
            }
            this.infer_statements(&mut clause.body.statements);
        });
    }

    fn bind(&mut self, name: String, ty: String) {
        if ty != "Unknown" {
            self.scopes
                .last_mut()
                .expect("scope stack should not be empty")
                .insert(name, ty);
        }
    }

    fn lookup(&self, name: &str) -> Option<String> {
        for scope in self.scopes.iter().rev() {
            if let Some(value) = scope.get(name) {
                return Some(value.clone());
            }
        }
        None
    }

    fn with_scope(&mut self, f: impl FnOnce(&mut Self)) {
        self.scopes.push(HashMap::new());
        f(self);
        self.scopes.pop();
    }
}

fn this_infer_expression(inferencer: &mut Inferencer, expression: &mut Expression) {
    let inferred = match expression {
        Expression::Identifier { name, .. } => inferencer.lookup(name),
        Expression::NumberLiteral { .. } => Some("Number".to_owned()),
        Expression::StringLiteral { .. } => Some("String".to_owned()),
        Expression::RegexLiteral { .. } => Some("Regexp".to_owned()),
        Expression::BooleanLiteral { .. } => Some("Boolean".to_owned()),
        Expression::NullLiteral { .. } => Some("Null".to_owned()),
        Expression::ArrayLiteral { elements, .. } => {
            for element in elements {
                this_infer_expression(inferencer, element);
            }
            Some("Array".to_owned())
        }
        Expression::SetLiteral { elements, .. } => {
            for element in elements {
                this_infer_expression(inferencer, element);
            }
            Some("Set".to_owned())
        }
        Expression::BagLiteral { elements, .. } => {
            for element in elements {
                this_infer_expression(inferencer, element);
            }
            Some("Bag".to_owned())
        }
        Expression::DictLiteral { entries, .. } => {
            infer_dict_entries(inferencer, entries);
            Some("Dict".to_owned())
        }
        Expression::PairListLiteral { entries, .. } => {
            infer_dict_entries(inferencer, entries);
            Some("PairList".to_owned())
        }
        Expression::TemplateLiteral { parts, .. } => {
            for part in parts {
                if let TemplatePart::Expression { expression, .. } = part {
                    this_infer_expression(inferencer, expression);
                }
            }
            Some("String".to_owned())
        }
        Expression::Unary {
            operator, argument, ..
        } => {
            this_infer_expression(inferencer, argument);
            match operator.as_str() {
                "!" | "not" | "¬" => Some("Boolean".to_owned()),
                "+" | "-" | "abs" | "sqrt" | "√" | "floor" | "ceil" | "round" | "int"
                | "length" | "++" | "--" => Some("Number".to_owned()),
                "uc" | "lc" | "typeof" => Some("String".to_owned()),
                "new" => infer_new_type(argument),
                _ => None,
            }
        }
        Expression::Binary {
            operator,
            left,
            right,
            ..
        } => {
            this_infer_expression(inferencer, left);
            this_infer_expression(inferencer, right);
            match operator.as_str() {
                "or" | "and" | "xor" | "nand" | "⋀" | "⋁" | "⊻" | "⊼" | "==" | "!=" | "=" | "≠"
                | "≡" | "≢" | "<" | "<=" | ">" | ">=" | "≤" | "≥" | "eq" | "ne" | "gt" | "ge"
                | "lt" | "le" | "eqi" | "nei" | "gti" | "gei" | "lti" | "lei" | "in" | "∈"
                | "∉" | "subsetof" | "⊂" | "supersetof" | "⊃" | "equivalentof" | "⊂⊃"
                | "instanceof" | "does" | "can" | "@?" => Some("Boolean".to_owned()),
                "_" => Some("String".to_owned()),
                "+" | "-" | "*" | "/" | "×" | "÷" | "mod" | "**" | "<=>" | "≶" | "≷" | "cmp"
                | "cmpi" => Some("Number".to_owned()),
                "union" | "⋃" | "intersection" | "⋂" | "\\" | "∖" => same_type(left, right),
                "~=" | "->" => Some("String".to_owned()),
                "@" | "@@" => None,
                _ => None,
            }
        }
        Expression::Ternary {
            test,
            consequent,
            alternate,
            ..
        } => {
            this_infer_expression(inferencer, test);
            this_infer_expression(inferencer, consequent);
            this_infer_expression(inferencer, alternate);
            same_type(consequent, alternate)
        }
        Expression::DefinedOr { left, right, .. } => {
            this_infer_expression(inferencer, left);
            this_infer_expression(inferencer, right);
            same_type(left, right)
        }
        Expression::Assignment {
            operator,
            left,
            right,
            runtime_typecheck_required,
            ..
        } => {
            this_infer_expression(inferencer, left);
            this_infer_expression(inferencer, right);
            let result_type = assignment_result_type(operator, left, right);
            *runtime_typecheck_required = typecheck_requirement(
                infer_lvalue_type(inferencer, left).as_deref(),
                result_type.as_deref(),
            );
            if let Expression::Identifier { name, .. } = left.as_ref() {
                if let Some(bound) =
                    infer_lvalue_type(inferencer, left).or_else(|| result_type.clone())
                {
                    inferencer.bind(name.clone(), bound);
                }
            }
            result_type
        }
        Expression::Call {
            callee, arguments, ..
        } => {
            this_infer_expression(inferencer, callee);
            infer_call_arguments(inferencer, arguments);
            infer_call_type(inferencer, callee)
        }
        Expression::MemberAccess { object, .. } => {
            this_infer_expression(inferencer, object);
            None
        }
        Expression::DynamicMemberCall {
            object,
            member,
            arguments,
            ..
        } => {
            this_infer_expression(inferencer, object);
            this_infer_expression(inferencer, member);
            infer_call_arguments(inferencer, arguments);
            None
        }
        Expression::Index { object, index, .. } => {
            this_infer_expression(inferencer, object);
            this_infer_expression(inferencer, index);
            None
        }
        Expression::Slice {
            object, start, end, ..
        } => {
            this_infer_expression(inferencer, object);
            if let Some(start) = start {
                this_infer_expression(inferencer, start);
            }
            if let Some(end) = end {
                this_infer_expression(inferencer, end);
            }
            object.inferred_type().map(ToOwned::to_owned)
        }
        Expression::DictAccess { object, key, .. } => {
            this_infer_expression(inferencer, object);
            infer_dict_key(inferencer, key);
            None
        }
        Expression::PostfixUpdate { argument, .. } => {
            this_infer_expression(inferencer, argument);
            Some("Number".to_owned())
        }
        Expression::Lambda { params, body, .. } => {
            inferencer.with_scope(|this| {
                for param in params {
                    if let Some(default_value) = &mut param.default_value {
                        this_infer_expression(this, default_value);
                    }
                    if let Some(param_type) = param.declared_type.clone().or_else(|| {
                        param
                            .default_value
                            .as_ref()
                            .and_then(|expr| expr.inferred_type().map(ToOwned::to_owned))
                    }) {
                        this.bind(param.name.clone(), param_type);
                    }
                }
                this_infer_expression(this, body);
            });
            Some("Function".to_owned())
        }
        Expression::FunctionExpression {
            params,
            return_type,
            body,
            ..
        } => {
            let previous = inferencer.current_return_type.clone();
            inferencer.current_return_type = return_type.clone();
            inferencer.with_scope(|this| {
                for param in params {
                    if let Some(default_value) = &mut param.default_value {
                        this_infer_expression(this, default_value);
                    }
                    if let Some(param_type) = param.declared_type.clone().or_else(|| {
                        param
                            .default_value
                            .as_ref()
                            .and_then(|expr| expr.inferred_type().map(ToOwned::to_owned))
                    }) {
                        this.bind(param.name.clone(), param_type);
                    }
                }
                this.infer_statements(&mut body.statements);
            });
            inferencer.current_return_type = previous;
            Some("Function".to_owned())
        }
        Expression::LetExpression {
            declared_type,
            name,
            init,
            runtime_typecheck_required,
            ..
        } => {
            if let Some(init) = init {
                this_infer_expression(inferencer, init);
            }
            *runtime_typecheck_required = typecheck_requirement(
                declared_type.as_deref(),
                init.as_ref().and_then(|expr| expr.inferred_type()),
            );
            let expr_type = declared_type.clone().or_else(|| {
                init.as_ref()
                    .and_then(|expr| expr.inferred_type().map(ToOwned::to_owned))
            });
            if let Some(expr_type) = &expr_type {
                inferencer.bind(name.clone(), expr_type.clone());
            }
            expr_type
        }
        Expression::TryExpression { body, handlers, .. } => {
            inferencer.with_scope(|this| this.infer_statements(&mut body.statements));
            for handler in handlers {
                inferencer.infer_catch(handler);
            }
            None
        }
        Expression::DoExpression { body, .. } => {
            inferencer.with_scope(|this| this.infer_statements(&mut body.statements));
            infer_type_from_block(body)
        }
        Expression::AwaitExpression { body, .. } => {
            inferencer.with_scope(|this| this.infer_statements(&mut body.statements));
            None
        }
        Expression::SpawnExpression { body, .. } => {
            inferencer.with_scope(|this| this.infer_statements(&mut body.statements));
            Some("Task".to_owned())
        }
        Expression::SuperCall { arguments, .. } => {
            infer_call_arguments(inferencer, arguments);
            None
        }
    };
    expression.set_inferred_type(inferred);
}

fn infer_type_from_block(block: &BlockStatement) -> Option<String> {
    block.statements.last().and_then(|stmt| match stmt {
        Statement::ExpressionStatement(node) => {
            node.expression.inferred_type().map(ToOwned::to_owned)
        }
        Statement::ReturnStatement(node) => node
            .argument
            .as_ref()
            .and_then(|arg| arg.inferred_type().map(ToOwned::to_owned)),
        _ => None,
    })
}

fn infer_dict_entries(inferencer: &mut Inferencer, entries: &mut [DictEntry]) {
    for entry in entries {
        infer_dict_key(inferencer, &mut entry.key);
        this_infer_expression(inferencer, &mut entry.value);
    }
}

fn infer_dict_key(inferencer: &mut Inferencer, key: &mut DictKey) {
    if let DictKey::Expression { expression, .. } = key {
        this_infer_expression(inferencer, expression);
    }
}

fn infer_call_arguments(inferencer: &mut Inferencer, arguments: &mut [CallArgument]) {
    for argument in arguments {
        match argument {
            CallArgument::Positional { value, .. } => this_infer_expression(inferencer, value),
            CallArgument::Named { name, value, .. } => {
                infer_dict_key(inferencer, name);
                this_infer_expression(inferencer, value);
            }
        }
    }
}

fn infer_call_type(inferencer: &Inferencer, callee: &Expression) -> Option<String> {
    match callee {
        Expression::Identifier { name, .. } => inferencer.function_returns.get(name).cloned(),
        Expression::MemberAccess { object, member, .. } => object
            .inferred_type()
            .and_then(|class_name| inferencer.method_returns.get(class_name))
            .and_then(|methods| methods.get(member))
            .cloned(),
        _ => None,
    }
}

fn infer_new_type(argument: &Expression) -> Option<String> {
    match argument {
        Expression::Call { callee, .. } => match callee.as_ref() {
            Expression::Identifier { name, .. } => Some(name.clone()),
            _ => None,
        },
        Expression::Identifier { name, .. } => Some(name.clone()),
        _ => None,
    }
}

fn assignment_result_type(operator: &str, left: &Expression, right: &Expression) -> Option<String> {
    match operator {
        ":=" | "?:=" => right.inferred_type().map(ToOwned::to_owned),
        "+=" | "-=" | "*=" | "×=" | "/=" | "÷=" | "**=" => Some("Number".to_owned()),
        "_=" | "~=" => Some("String".to_owned()),
        _ => left
            .inferred_type()
            .map(ToOwned::to_owned)
            .or_else(|| right.inferred_type().map(ToOwned::to_owned)),
    }
}

fn infer_lvalue_type(inferencer: &Inferencer, expression: &Expression) -> Option<String> {
    match expression {
        Expression::Identifier { name, .. } => inferencer.lookup(name),
        _ => expression.inferred_type().map(ToOwned::to_owned),
    }
}

fn same_type(left: &Expression, right: &Expression) -> Option<String> {
    let left = left.inferred_type()?;
    let right = right.inferred_type()?;
    if left == right {
        Some(left.to_owned())
    } else {
        None
    }
}

fn typecheck_requirement(declared_type: Option<&str>, inferred_type: Option<&str>) -> Option<bool> {
    let declared_type = declared_type?;
    let inferred_type = inferred_type?;
    Some(declared_type != inferred_type)
}
