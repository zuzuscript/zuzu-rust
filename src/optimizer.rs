use std::collections::{HashMap, HashSet};

use crate::ast::{
    BlockStatement, CallArgument, CatchClause, ClassMember, DictEntry, DictKey, Expression,
    ExpressionStatement, IfStatement, Program, Statement, SwitchIndexEntry, SwitchStatement,
    TemplatePart, VariableDeclaration, WhileStatement,
};
use crate::error::{Result, ZuzuRustError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizationLevel {
    O0,
    O1,
    O2,
    O3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OptimizationPass {
    BlockScopeElision,
    ConstantFolding,
    ConstantConditionPruning,
    UnreachablePruning,
    RegexCache,
    IdentifierResolution,
    TypecheckSkip,
    OperatorEnum,
    CollectionPresize,
    SwitchIndexing,
    RangeArrayLoopLowering,
}

const ALL_OPTIMIZATION_PASSES: &[OptimizationPass] = &[
    OptimizationPass::BlockScopeElision,
    OptimizationPass::ConstantFolding,
    OptimizationPass::ConstantConditionPruning,
    OptimizationPass::UnreachablePruning,
    OptimizationPass::RegexCache,
    OptimizationPass::IdentifierResolution,
    OptimizationPass::TypecheckSkip,
    OptimizationPass::OperatorEnum,
    OptimizationPass::CollectionPresize,
    OptimizationPass::SwitchIndexing,
    OptimizationPass::RangeArrayLoopLowering,
];

impl OptimizationPass {
    pub fn name(self) -> &'static str {
        match self {
            OptimizationPass::BlockScopeElision => "block-scope-elision",
            OptimizationPass::ConstantFolding => "constant-folding",
            OptimizationPass::ConstantConditionPruning => "constant-condition-pruning",
            OptimizationPass::UnreachablePruning => "unreachable-pruning",
            OptimizationPass::RegexCache => "regex-cache",
            OptimizationPass::IdentifierResolution => "identifier-resolution",
            OptimizationPass::TypecheckSkip => "typecheck-skip",
            OptimizationPass::OperatorEnum => "operator-enum",
            OptimizationPass::CollectionPresize => "collection-presize",
            OptimizationPass::SwitchIndexing => "switch-indexing",
            OptimizationPass::RangeArrayLoopLowering => "range-array-loop-lowering",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizationOptions {
    level: OptimizationLevel,
    enabled: HashSet<OptimizationPass>,
}

impl Default for OptimizationOptions {
    fn default() -> Self {
        Self::for_level(OptimizationLevel::O2)
    }
}

impl OptimizationOptions {
    pub fn for_level(level: OptimizationLevel) -> Self {
        let mut enabled = HashSet::new();
        match level {
            OptimizationLevel::O0 => {}
            OptimizationLevel::O1 | OptimizationLevel::O2 | OptimizationLevel::O3 => {
                enabled.insert(OptimizationPass::BlockScopeElision);
                enabled.insert(OptimizationPass::ConstantFolding);
                enabled.insert(OptimizationPass::UnreachablePruning);
                enabled.insert(OptimizationPass::RegexCache);
                enabled.insert(OptimizationPass::TypecheckSkip);
                enabled.insert(OptimizationPass::CollectionPresize);
            }
        }
        match level {
            OptimizationLevel::O2 | OptimizationLevel::O3 => {
                enabled.insert(OptimizationPass::IdentifierResolution);
                enabled.insert(OptimizationPass::OperatorEnum);
            }
            _ => {}
        }
        if matches!(level, OptimizationLevel::O3) {
            enabled.insert(OptimizationPass::SwitchIndexing);
            enabled.insert(OptimizationPass::RangeArrayLoopLowering);
        }
        Self { level, enabled }
    }

    pub fn o1() -> Self {
        Self::for_level(OptimizationLevel::O1)
    }

    pub fn disabled() -> Self {
        Self::for_level(OptimizationLevel::O0)
    }

    pub fn level(&self) -> OptimizationLevel {
        self.level
    }

    pub fn enables(&self, pass: OptimizationPass) -> bool {
        self.enabled.contains(&pass)
    }

    pub fn is_empty(&self) -> bool {
        self.enabled.is_empty()
    }

    pub fn enable(&mut self, name: &str) -> Result<()> {
        let pass = parse_pass_name(name)?;
        self.enabled.insert(pass);
        Ok(())
    }

    pub fn disable(&mut self, name: &str) -> Result<()> {
        let pass = parse_pass_name(name)?;
        self.enabled.remove(&pass);
        Ok(())
    }
}

pub fn parse_level(text: &str) -> Option<OptimizationLevel> {
    match text {
        "-o0" | "0" => Some(OptimizationLevel::O0),
        "-o1" | "1" => Some(OptimizationLevel::O1),
        "-o2" | "2" => Some(OptimizationLevel::O2),
        "-o3" | "3" => Some(OptimizationLevel::O3),
        _ => None,
    }
}

pub fn parse_pass_name(name: &str) -> Result<OptimizationPass> {
    for pass in ALL_OPTIMIZATION_PASSES {
        if pass.name() == name {
            return Ok(*pass);
        }
    }
    Err(ZuzuRustError::cli(format!(
        "unknown optimization pass: {name}"
    )))
}

pub fn all_passes() -> &'static [OptimizationPass] {
    ALL_OPTIMIZATION_PASSES
}

pub fn optimize_program(program: &mut Program, options: &OptimizationOptions) {
    if options.is_empty() {
        return;
    }
    let mut optimizer = Optimizer { options };
    optimizer.optimize_statements(&mut program.statements);
    if options.enables(OptimizationPass::ConstantFolding) {
        propagate_constants(&mut program.statements);
        let mut post_propagation_options = options.clone();
        post_propagation_options
            .enabled
            .remove(&OptimizationPass::ConstantConditionPruning);
        post_propagation_options
            .enabled
            .remove(&OptimizationPass::UnreachablePruning);
        let mut post_propagation_optimizer = Optimizer {
            options: &post_propagation_options,
        };
        post_propagation_optimizer.optimize_statements(&mut program.statements);
    }
    if options.enables(OptimizationPass::IdentifierResolution) {
        annotate_identifier_depths(program);
    }
}

struct Optimizer<'a> {
    options: &'a OptimizationOptions,
}

impl Optimizer<'_> {
    fn optimize_statements(&mut self, statements: &mut Vec<Statement>) {
        let mut optimized = Vec::with_capacity(statements.len());
        for mut statement in std::mem::take(statements) {
            self.optimize_statement(&mut statement);
            if self
                .options
                .enables(OptimizationPass::ConstantConditionPruning)
            {
                if let Some(replacement) = self.prune_statement(statement) {
                    for mut replacement in replacement {
                        self.optimize_statement(&mut replacement);
                        optimized.push(replacement);
                    }
                }
            } else {
                optimized.push(statement);
            }
            if self.options.enables(OptimizationPass::UnreachablePruning)
                && optimized.last().map(is_terminal_statement).unwrap_or(false)
            {
                break;
            }
        }
        *statements = optimized;
    }

    fn optimize_statement(&mut self, statement: &mut Statement) {
        match statement {
            Statement::Block(block) => self.optimize_block(block),
            Statement::VariableDeclaration(node) => {
                if let Some(init) = &mut node.init {
                    self.optimize_expression(init);
                }
                if self.options.enables(OptimizationPass::TypecheckSkip)
                    && node.runtime_typecheck_required == Some(false)
                {
                    node.runtime_typecheck_required = Some(false);
                }
            }
            Statement::FunctionDeclaration(node) => self.optimize_block(&mut node.body),
            Statement::ClassDeclaration(node) => {
                for member in &mut node.body {
                    self.optimize_class_member(member);
                }
            }
            Statement::TraitDeclaration(node) => {
                for member in &mut node.body {
                    self.optimize_class_member(member);
                }
            }
            Statement::ImportDeclaration(node) => {
                if let Some(condition) = &mut node.condition {
                    self.optimize_expression(&mut condition.test);
                }
            }
            Statement::IfStatement(node) => {
                self.optimize_expression(&mut node.test);
                self.optimize_block(&mut node.consequent);
                if let Some(alternate) = &mut node.alternate {
                    self.optimize_statement(alternate);
                }
            }
            Statement::WhileStatement(node) => {
                self.optimize_expression(&mut node.test);
                self.optimize_block(&mut node.body);
            }
            Statement::ForStatement(node) => {
                self.optimize_expression(&mut node.iterable);
                self.optimize_block(&mut node.body);
                if let Some(else_block) = &mut node.else_block {
                    self.optimize_block(else_block);
                }
            }
            Statement::SwitchStatement(node) => self.optimize_switch(node),
            Statement::TryStatement(node) => {
                self.optimize_block(&mut node.body);
                for handler in &mut node.handlers {
                    self.optimize_block(&mut handler.body);
                }
            }
            Statement::ReturnStatement(node) => {
                if let Some(argument) = &mut node.argument {
                    self.optimize_expression(argument);
                }
            }
            Statement::ThrowStatement(node) => self.optimize_expression(&mut node.argument),
            Statement::DieStatement(node) => self.optimize_expression(&mut node.argument),
            Statement::PostfixConditionalStatement(node) => {
                self.optimize_statement(&mut node.statement);
                self.optimize_expression(&mut node.test);
            }
            Statement::KeywordStatement(node) => {
                for argument in &mut node.arguments {
                    self.optimize_expression(argument);
                }
            }
            Statement::ExpressionStatement(node) => self.optimize_expression(&mut node.expression),
            Statement::LoopControlStatement(_) => {}
        }
        if self
            .options
            .enables(OptimizationPass::RangeArrayLoopLowering)
        {
            if let Some(mut lowered) = lower_range_array_loop(statement) {
                self.optimize_statement(&mut lowered);
                *statement = lowered;
            }
        }
    }

    fn optimize_class_member(&mut self, member: &mut ClassMember) {
        match member {
            ClassMember::Field(field) => {
                if let Some(default_value) = &mut field.default_value {
                    self.optimize_expression(default_value);
                }
            }
            ClassMember::Method(method) => self.optimize_block(&mut method.body),
            ClassMember::Class(class) => {
                for member in &mut class.body {
                    self.optimize_class_member(member);
                }
            }
            ClassMember::Trait(trait_decl) => {
                for member in &mut trait_decl.body {
                    self.optimize_class_member(member);
                }
            }
        }
    }

    fn optimize_block(&mut self, block: &mut BlockStatement) {
        self.optimize_statements(&mut block.statements);
        if self.options.enables(OptimizationPass::BlockScopeElision) {
            block.needs_lexical_scope = block_needs_lexical_scope(block);
        }
    }

    fn optimize_switch(&mut self, node: &mut SwitchStatement) {
        self.optimize_expression(&mut node.discriminant);
        if let Some(comparator) = &mut node.comparator {
            *comparator = preferred_operator(comparator).to_owned();
        }
        for case in &mut node.cases {
            for value in &mut case.values {
                self.optimize_expression(value);
            }
            self.optimize_statements(&mut case.consequent);
        }
        if let Some(default) = &mut node.default {
            self.optimize_statements(default);
        }
        if self.options.enables(OptimizationPass::SwitchIndexing) {
            node.index = build_switch_index(node);
        }
    }

    fn optimize_expression(&mut self, expr: &mut Expression) {
        match expr {
            Expression::Unary {
                operator, argument, ..
            } => {
                self.optimize_expression(argument);
                if self.options.enables(OptimizationPass::OperatorEnum) && operator != "\\" {
                    *operator = preferred_operator(operator).to_owned();
                }
            }
            Expression::Binary {
                operator,
                left,
                right,
                ..
            } => {
                self.optimize_expression(left);
                self.optimize_expression(right);
                if self.options.enables(OptimizationPass::OperatorEnum) {
                    *operator = preferred_operator(operator).to_owned();
                }
            }
            Expression::Ternary {
                test,
                consequent,
                alternate,
                ..
            } => {
                self.optimize_expression(test);
                self.optimize_expression(consequent);
                self.optimize_expression(alternate);
            }
            Expression::DefinedOr { left, right, .. } => {
                self.optimize_expression(left);
                self.optimize_expression(right);
            }
            Expression::Assignment {
                operator,
                left,
                right,
                ..
            } => {
                self.optimize_expression(left);
                self.optimize_expression(right);
                if self.options.enables(OptimizationPass::OperatorEnum) {
                    *operator = preferred_operator(operator).to_owned();
                }
            }
            Expression::Call {
                callee, arguments, ..
            } => {
                self.optimize_expression(callee);
                for argument in arguments {
                    self.optimize_call_argument(argument);
                }
            }
            Expression::MemberAccess { object, .. } => self.optimize_expression(object),
            Expression::DynamicMemberCall {
                object,
                member,
                arguments,
                ..
            } => {
                self.optimize_expression(object);
                self.optimize_expression(member);
                for argument in arguments {
                    self.optimize_call_argument(argument);
                }
            }
            Expression::Index { object, index, .. } => {
                self.optimize_expression(object);
                self.optimize_expression(index);
            }
            Expression::Slice {
                object, start, end, ..
            } => {
                self.optimize_expression(object);
                if let Some(start) = start {
                    self.optimize_expression(start);
                }
                if let Some(end) = end {
                    self.optimize_expression(end);
                }
            }
            Expression::DictAccess { object, key, .. } => {
                self.optimize_expression(object);
                self.optimize_dict_key(key);
            }
            Expression::PostfixUpdate {
                operator, argument, ..
            } => {
                self.optimize_expression(argument);
                if self.options.enables(OptimizationPass::OperatorEnum) {
                    *operator = preferred_operator(operator).to_owned();
                }
            }
            Expression::Lambda { body, .. } => self.optimize_expression(body),
            Expression::FunctionExpression { body, .. } => self.optimize_block(body),
            Expression::LetExpression { init, .. } => {
                if let Some(init) = init {
                    self.optimize_expression(init);
                }
            }
            Expression::TryExpression { body, handlers, .. } => {
                self.optimize_block(body);
                for CatchClause { body, .. } in handlers {
                    self.optimize_block(body);
                }
            }
            Expression::DoExpression { body, .. }
            | Expression::AwaitExpression { body, .. }
            | Expression::SpawnExpression { body, .. } => self.optimize_block(body),
            Expression::ArrayLiteral {
                elements,
                capacity_hint,
                ..
            } => {
                for element in &mut *elements {
                    self.optimize_expression(element);
                }
                if self.options.enables(OptimizationPass::CollectionPresize) {
                    *capacity_hint = Some(collection_element_capacity_hint(elements));
                }
            }
            Expression::SetLiteral {
                elements,
                capacity_hint,
                ..
            }
            | Expression::BagLiteral {
                elements,
                capacity_hint,
                ..
            } => {
                for element in &mut *elements {
                    self.optimize_expression(element);
                }
                if self.options.enables(OptimizationPass::CollectionPresize) {
                    *capacity_hint = Some(collection_element_capacity_hint(elements));
                }
            }
            Expression::DictLiteral {
                entries,
                capacity_hint,
                ..
            }
            | Expression::PairListLiteral {
                entries,
                capacity_hint,
                ..
            } => {
                for entry in &mut *entries {
                    self.optimize_dict_entry(entry);
                }
                if self.options.enables(OptimizationPass::CollectionPresize) {
                    *capacity_hint = Some(entries.len());
                }
            }
            Expression::TemplateLiteral { parts, .. } => {
                for part in parts {
                    if let TemplatePart::Expression { expression, .. } = part {
                        self.optimize_expression(expression);
                    }
                }
            }
            Expression::SuperCall { arguments, .. } => {
                for argument in arguments {
                    self.optimize_call_argument(argument);
                }
            }
            Expression::RegexLiteral {
                pattern,
                flags,
                cache_key,
                ..
            } => {
                if self.options.enables(OptimizationPass::RegexCache) {
                    *cache_key = Some(regex_cache_key(pattern, flags));
                }
            }
            Expression::Identifier { .. }
            | Expression::NumberLiteral { .. }
            | Expression::StringLiteral { .. }
            | Expression::BooleanLiteral { .. }
            | Expression::NullLiteral { .. } => {}
        }
        if self.options.enables(OptimizationPass::ConstantFolding) {
            if let Some(folded) = fold_expression(expr) {
                *expr = folded;
            }
        }
    }

    fn optimize_dict_entry(&mut self, entry: &mut DictEntry) {
        self.optimize_dict_key(&mut entry.key);
        self.optimize_expression(&mut entry.value);
    }

    fn optimize_dict_key(&mut self, key: &mut DictKey) {
        if let DictKey::Expression { expression, .. } = key {
            self.optimize_expression(expression);
        }
    }

    fn optimize_call_argument(&mut self, argument: &mut CallArgument) {
        match argument {
            CallArgument::Positional { value, .. } => self.optimize_expression(value),
            CallArgument::Named { name, value, .. } => {
                self.optimize_dict_key(name);
                self.optimize_expression(value);
            }
        }
    }

    fn prune_statement(&self, statement: Statement) -> Option<Vec<Statement>> {
        match statement {
            Statement::IfStatement(node) => match literal_boolean(&node.test) {
                Some(true) => Some(node.consequent.statements),
                Some(false) => match node.alternate {
                    Some(alternate) => Some(vec![*alternate]),
                    None => Some(Vec::new()),
                },
                None => Some(vec![Statement::IfStatement(node)]),
            },
            Statement::WhileStatement(node) if literal_boolean(&node.test) == Some(false) => {
                Some(Vec::new())
            }
            Statement::PostfixConditionalStatement(node) => {
                let truth = literal_boolean(&node.test)?;
                let should_run = if node.keyword == "if" { truth } else { !truth };
                if should_run {
                    Some(vec![*node.statement])
                } else {
                    Some(Vec::new())
                }
            }
            other => Some(vec![other]),
        }
    }
}

fn lower_range_array_loop(statement: &Statement) -> Option<Statement> {
    let Statement::ForStatement(node) = statement else {
        return None;
    };
    let (start, end) = range_array_expression(&node.iterable)?;
    if !is_simple_range_endpoint(start) || !is_simple_range_endpoint(end) {
        return None;
    }

    let prefix = format!("__zuzu_range_{}_{}", node.line, node.variable);
    let current_name = format!("{prefix}_current");
    let end_name = format!("{prefix}_end");
    let step_name = format!("{prefix}_step");
    let seen_name = format!("{prefix}_seen");
    let generated = [
        current_name.as_str(),
        end_name.as_str(),
        step_name.as_str(),
        seen_name.as_str(),
    ];
    let mut identifiers = HashSet::new();
    collect_statement_identifiers(statement, &mut identifiers);
    if generated.iter().any(|name| identifiers.contains(*name)) {
        return None;
    }

    let line = node.line;
    let source_file = node.source_file.clone();
    let mut statements = vec![
        variable_statement(
            line,
            source_file.clone(),
            "let",
            &current_name,
            start.clone(),
        ),
        variable_statement(line, source_file.clone(), "let", &end_name, end.clone()),
        variable_statement(
            line,
            source_file.clone(),
            "let",
            &step_name,
            Expression::Ternary {
                line,
                source_file: source_file.clone(),
                test: Box::new(binary_expression(
                    line,
                    source_file.clone(),
                    "≤",
                    identifier_expression(line, source_file.clone(), &current_name),
                    identifier_expression(line, source_file.clone(), &end_name),
                )),
                consequent: Box::new(number_expression(line, source_file.clone(), "1")),
                alternate: Box::new(number_expression(line, source_file.clone(), "-1")),
                inferred_type: None,
            },
        ),
    ];

    if node.else_block.is_some() {
        statements.push(variable_statement(
            line,
            source_file.clone(),
            "let",
            &seen_name,
            Expression::BooleanLiteral {
                line,
                source_file: source_file.clone(),
                value: false,
                inferred_type: None,
            },
        ));
    }

    let mut body_statements = Vec::new();
    if node.else_block.is_some() {
        body_statements.push(expression_statement(
            line,
            source_file.clone(),
            assignment_expression(
                line,
                source_file.clone(),
                identifier_expression(line, source_file.clone(), &seen_name),
                Expression::BooleanLiteral {
                    line,
                    source_file: source_file.clone(),
                    value: true,
                    inferred_type: None,
                },
            ),
        ));
    }
    body_statements.push(variable_statement(
        line,
        source_file.clone(),
        node.binding_kind.as_deref().unwrap_or("let"),
        &node.variable,
        identifier_expression(line, source_file.clone(), &current_name),
    ));
    body_statements.push(expression_statement(
        line,
        source_file.clone(),
        Expression::Assignment {
            line,
            source_file: source_file.clone(),
            operator: "+=".to_owned(),
            left: Box::new(identifier_expression(
                line,
                source_file.clone(),
                &current_name,
            )),
            right: Box::new(identifier_expression(line, source_file.clone(), &step_name)),
            is_weak_write: false,
            inferred_type: None,
            runtime_typecheck_required: None,
        },
    ));
    body_statements.extend(node.body.statements.clone());

    statements.push(Statement::WhileStatement(WhileStatement {
        line,
        source_file: source_file.clone(),
        test: range_loop_test(
            line,
            source_file.clone(),
            &current_name,
            &end_name,
            &step_name,
        ),
        body: BlockStatement {
            line,
            source_file: source_file.clone(),
            statements: body_statements,
            needs_lexical_scope: true,
        },
    }));

    if let Some(else_block) = &node.else_block {
        statements.push(Statement::IfStatement(IfStatement {
            line,
            source_file: source_file.clone(),
            test: Expression::Unary {
                line,
                source_file: source_file.clone(),
                operator: "¬".to_owned(),
                argument: Box::new(identifier_expression(line, source_file.clone(), &seen_name)),
                inferred_type: None,
            },
            consequent: else_block.clone(),
            alternate: None,
        }));
    }

    Some(Statement::Block(BlockStatement {
        line,
        source_file,
        statements,
        needs_lexical_scope: true,
    }))
}

fn regex_cache_key(pattern: &str, flags: &str) -> String {
    format!("{}:{pattern}:{flags}", pattern.len())
}

fn collection_element_capacity_hint(elements: &[Expression]) -> usize {
    elements
        .iter()
        .map(|element| range_literal_len(element).unwrap_or(1))
        .sum()
}

fn range_literal_len(expression: &Expression) -> Option<usize> {
    let Expression::Binary {
        operator,
        left,
        right,
        ..
    } = expression
    else {
        return None;
    };
    if operator != "..." {
        return None;
    }
    let start = literal_number(left)? as i64;
    let end = literal_number(right)? as i64;
    Some(start.abs_diff(end) as usize + 1)
}

fn range_array_expression(iterable: &Expression) -> Option<(&Expression, &Expression)> {
    let Expression::ArrayLiteral { elements, .. } = iterable else {
        return None;
    };
    let [Expression::Binary {
        operator,
        left,
        right,
        ..
    }] = elements.as_slice()
    else {
        return None;
    };
    if operator != "..." {
        return None;
    }
    Some((left, right))
}

fn is_simple_range_endpoint(expression: &Expression) -> bool {
    matches!(
        expression,
        Expression::NumberLiteral { .. } | Expression::Identifier { .. }
    )
}

fn range_loop_test(
    line: usize,
    source_file: Option<String>,
    current_name: &str,
    end_name: &str,
    step_name: &str,
) -> Expression {
    binary_expression(
        line,
        source_file.clone(),
        "⋁",
        binary_expression(
            line,
            source_file.clone(),
            "⋀",
            binary_expression(
                line,
                source_file.clone(),
                ">",
                identifier_expression(line, source_file.clone(), step_name),
                number_expression(line, source_file.clone(), "0"),
            ),
            binary_expression(
                line,
                source_file.clone(),
                "≤",
                identifier_expression(line, source_file.clone(), current_name),
                identifier_expression(line, source_file.clone(), end_name),
            ),
        ),
        binary_expression(
            line,
            source_file.clone(),
            "⋀",
            binary_expression(
                line,
                source_file.clone(),
                "<",
                identifier_expression(line, source_file.clone(), step_name),
                number_expression(line, source_file.clone(), "0"),
            ),
            binary_expression(
                line,
                source_file.clone(),
                "≥",
                identifier_expression(line, source_file.clone(), current_name),
                identifier_expression(line, source_file, end_name),
            ),
        ),
    )
}

fn variable_statement(
    line: usize,
    source_file: Option<String>,
    kind: &str,
    name: &str,
    init: Expression,
) -> Statement {
    Statement::VariableDeclaration(VariableDeclaration {
        line,
        source_file,
        kind: kind.to_owned(),
        declared_type: None,
        name: name.to_owned(),
        init: Some(init),
        is_weak_storage: false,
        runtime_typecheck_required: None,
    })
}

fn expression_statement(
    line: usize,
    source_file: Option<String>,
    expression: Expression,
) -> Statement {
    Statement::ExpressionStatement(ExpressionStatement {
        line,
        source_file,
        expression,
    })
}

fn assignment_expression(
    line: usize,
    source_file: Option<String>,
    left: Expression,
    right: Expression,
) -> Expression {
    Expression::Assignment {
        line,
        source_file,
        operator: ":=".to_owned(),
        left: Box::new(left),
        right: Box::new(right),
        is_weak_write: false,
        inferred_type: None,
        runtime_typecheck_required: None,
    }
}

fn binary_expression(
    line: usize,
    source_file: Option<String>,
    operator: &str,
    left: Expression,
    right: Expression,
) -> Expression {
    Expression::Binary {
        line,
        source_file,
        operator: operator.to_owned(),
        left: Box::new(left),
        right: Box::new(right),
        inferred_type: None,
    }
}

fn identifier_expression(line: usize, source_file: Option<String>, name: &str) -> Expression {
    Expression::Identifier {
        line,
        source_file,
        name: name.to_owned(),
        inferred_type: None,
        binding_depth: None,
    }
}

fn number_expression(line: usize, source_file: Option<String>, value: &str) -> Expression {
    Expression::NumberLiteral {
        line,
        source_file,
        value: value.to_owned(),
        inferred_type: None,
    }
}

fn collect_statement_identifiers(statement: &Statement, names: &mut HashSet<String>) {
    match statement {
        Statement::Block(block) => collect_block_identifiers(block, names),
        Statement::VariableDeclaration(node) => {
            names.insert(node.name.clone());
            if let Some(init) = &node.init {
                collect_expression_identifiers(init, names);
            }
        }
        Statement::FunctionDeclaration(node) => {
            names.insert(node.name.clone());
            for param in &node.params {
                names.insert(param.name.clone());
                if let Some(default_value) = &param.default_value {
                    collect_expression_identifiers(default_value, names);
                }
            }
            collect_block_identifiers(&node.body, names);
        }
        Statement::ClassDeclaration(node) => {
            names.insert(node.name.clone());
            for member in &node.body {
                collect_class_member_identifiers(member, names);
            }
        }
        Statement::TraitDeclaration(node) => {
            names.insert(node.name.clone());
            for member in &node.body {
                collect_class_member_identifiers(member, names);
            }
        }
        Statement::ImportDeclaration(node) => {
            for specifier in &node.specifiers {
                names.insert(specifier.local.clone());
            }
            if let Some(condition) = &node.condition {
                collect_expression_identifiers(&condition.test, names);
            }
        }
        Statement::IfStatement(node) => {
            collect_expression_identifiers(&node.test, names);
            collect_block_identifiers(&node.consequent, names);
            if let Some(alternate) = &node.alternate {
                collect_statement_identifiers(alternate, names);
            }
        }
        Statement::WhileStatement(node) => {
            collect_expression_identifiers(&node.test, names);
            collect_block_identifiers(&node.body, names);
        }
        Statement::ForStatement(node) => {
            names.insert(node.variable.clone());
            collect_expression_identifiers(&node.iterable, names);
            collect_block_identifiers(&node.body, names);
            if let Some(else_block) = &node.else_block {
                collect_block_identifiers(else_block, names);
            }
        }
        Statement::SwitchStatement(node) => {
            collect_expression_identifiers(&node.discriminant, names);
            for case in &node.cases {
                for value in &case.values {
                    collect_expression_identifiers(value, names);
                }
                for statement in &case.consequent {
                    collect_statement_identifiers(statement, names);
                }
            }
            if let Some(default) = &node.default {
                for statement in default {
                    collect_statement_identifiers(statement, names);
                }
            }
        }
        Statement::TryStatement(node) => {
            collect_block_identifiers(&node.body, names);
            for handler in &node.handlers {
                if let Some(name) = handler
                    .binding
                    .as_ref()
                    .and_then(|binding| binding.name.as_ref())
                {
                    names.insert(name.clone());
                }
                collect_block_identifiers(&handler.body, names);
            }
        }
        Statement::ReturnStatement(node) => {
            if let Some(argument) = &node.argument {
                collect_expression_identifiers(argument, names);
            }
        }
        Statement::ThrowStatement(node) => collect_expression_identifiers(&node.argument, names),
        Statement::DieStatement(node) => collect_expression_identifiers(&node.argument, names),
        Statement::PostfixConditionalStatement(node) => {
            collect_statement_identifiers(&node.statement, names);
            collect_expression_identifiers(&node.test, names);
        }
        Statement::KeywordStatement(node) => {
            for argument in &node.arguments {
                collect_expression_identifiers(argument, names);
            }
        }
        Statement::ExpressionStatement(node) => {
            collect_expression_identifiers(&node.expression, names);
        }
        Statement::LoopControlStatement(_) => {}
    }
}

fn collect_block_identifiers(block: &BlockStatement, names: &mut HashSet<String>) {
    for statement in &block.statements {
        collect_statement_identifiers(statement, names);
    }
}

fn collect_class_member_identifiers(member: &ClassMember, names: &mut HashSet<String>) {
    match member {
        ClassMember::Field(field) => {
            names.insert(field.name.clone());
            if let Some(default_value) = &field.default_value {
                collect_expression_identifiers(default_value, names);
            }
        }
        ClassMember::Method(method) => {
            names.insert(method.name.clone());
            for param in &method.params {
                names.insert(param.name.clone());
                if let Some(default_value) = &param.default_value {
                    collect_expression_identifiers(default_value, names);
                }
            }
            collect_block_identifiers(&method.body, names);
        }
        ClassMember::Class(class) => {
            names.insert(class.name.clone());
            for member in &class.body {
                collect_class_member_identifiers(member, names);
            }
        }
        ClassMember::Trait(trait_decl) => {
            names.insert(trait_decl.name.clone());
            for member in &trait_decl.body {
                collect_class_member_identifiers(member, names);
            }
        }
    }
}

fn collect_expression_identifiers(expression: &Expression, names: &mut HashSet<String>) {
    match expression {
        Expression::Identifier { name, .. } => {
            names.insert(name.clone());
        }
        Expression::Unary { argument, .. } => collect_expression_identifiers(argument, names),
        Expression::Binary { left, right, .. }
        | Expression::DefinedOr { left, right, .. }
        | Expression::Assignment { left, right, .. } => {
            collect_expression_identifiers(left, names);
            collect_expression_identifiers(right, names);
        }
        Expression::Ternary {
            test,
            consequent,
            alternate,
            ..
        } => {
            collect_expression_identifiers(test, names);
            collect_expression_identifiers(consequent, names);
            collect_expression_identifiers(alternate, names);
        }
        Expression::Call {
            callee, arguments, ..
        } => {
            collect_expression_identifiers(callee, names);
            for argument in arguments {
                collect_call_argument_identifiers(argument, names);
            }
        }
        Expression::MemberAccess { object, .. } => {
            collect_expression_identifiers(object, names);
        }
        Expression::DynamicMemberCall {
            object,
            member,
            arguments,
            ..
        } => {
            collect_expression_identifiers(object, names);
            collect_expression_identifiers(member, names);
            for argument in arguments {
                collect_call_argument_identifiers(argument, names);
            }
        }
        Expression::Index { object, index, .. } => {
            collect_expression_identifiers(object, names);
            collect_expression_identifiers(index, names);
        }
        Expression::Slice {
            object, start, end, ..
        } => {
            collect_expression_identifiers(object, names);
            if let Some(start) = start {
                collect_expression_identifiers(start, names);
            }
            if let Some(end) = end {
                collect_expression_identifiers(end, names);
            }
        }
        Expression::DictAccess { object, key, .. } => {
            collect_expression_identifiers(object, names);
            collect_dict_key_identifiers(key, names);
        }
        Expression::PostfixUpdate { argument, .. } => {
            collect_expression_identifiers(argument, names);
        }
        Expression::Lambda { params, body, .. } => {
            for param in params {
                names.insert(param.name.clone());
                if let Some(default_value) = &param.default_value {
                    collect_expression_identifiers(default_value, names);
                }
            }
            collect_expression_identifiers(body, names);
        }
        Expression::FunctionExpression { params, body, .. } => {
            for param in params {
                names.insert(param.name.clone());
                if let Some(default_value) = &param.default_value {
                    collect_expression_identifiers(default_value, names);
                }
            }
            collect_block_identifiers(body, names);
        }
        Expression::LetExpression { name, init, .. } => {
            names.insert(name.clone());
            if let Some(init) = init {
                collect_expression_identifiers(init, names);
            }
        }
        Expression::TryExpression { body, handlers, .. } => {
            collect_block_identifiers(body, names);
            for handler in handlers {
                if let Some(name) = handler
                    .binding
                    .as_ref()
                    .and_then(|binding| binding.name.as_ref())
                {
                    names.insert(name.clone());
                }
                collect_block_identifiers(&handler.body, names);
            }
        }
        Expression::DoExpression { body, .. }
        | Expression::AwaitExpression { body, .. }
        | Expression::SpawnExpression { body, .. } => collect_block_identifiers(body, names),
        Expression::ArrayLiteral { elements, .. }
        | Expression::SetLiteral { elements, .. }
        | Expression::BagLiteral { elements, .. } => {
            for element in elements {
                collect_expression_identifiers(element, names);
            }
        }
        Expression::DictLiteral { entries, .. } | Expression::PairListLiteral { entries, .. } => {
            for entry in entries {
                collect_dict_key_identifiers(&entry.key, names);
                collect_expression_identifiers(&entry.value, names);
            }
        }
        Expression::TemplateLiteral { parts, .. } => {
            for part in parts {
                if let TemplatePart::Expression { expression, .. } = part {
                    collect_expression_identifiers(expression, names);
                }
            }
        }
        Expression::SuperCall { arguments, .. } => {
            for argument in arguments {
                collect_call_argument_identifiers(argument, names);
            }
        }
        Expression::NumberLiteral { .. }
        | Expression::StringLiteral { .. }
        | Expression::RegexLiteral { .. }
        | Expression::BooleanLiteral { .. }
        | Expression::NullLiteral { .. } => {}
    }
}

fn collect_call_argument_identifiers(argument: &CallArgument, names: &mut HashSet<String>) {
    match argument {
        CallArgument::Positional { value, .. } => collect_expression_identifiers(value, names),
        CallArgument::Named { name, value, .. } => {
            collect_dict_key_identifiers(name, names);
            collect_expression_identifiers(value, names);
        }
    }
}

fn collect_dict_key_identifiers(key: &DictKey, names: &mut HashSet<String>) {
    if let DictKey::Expression { expression, .. } = key {
        collect_expression_identifiers(expression, names);
    }
}

fn block_needs_lexical_scope(block: &BlockStatement) -> bool {
    block.statements.iter().any(statement_needs_lexical_scope)
}

fn statement_needs_lexical_scope(statement: &Statement) -> bool {
    match statement {
        Statement::VariableDeclaration(_)
        | Statement::FunctionDeclaration(_)
        | Statement::ClassDeclaration(_)
        | Statement::TraitDeclaration(_)
        | Statement::ImportDeclaration(_) => true,
        Statement::ExpressionStatement(node) => expression_declares_binding(&node.expression),
        Statement::KeywordStatement(node) => node.arguments.iter().any(expression_declares_binding),
        Statement::ReturnStatement(node) => node
            .argument
            .as_ref()
            .map(expression_declares_binding)
            .unwrap_or(false),
        Statement::ThrowStatement(node) => expression_declares_binding(&node.argument),
        Statement::DieStatement(node) => expression_declares_binding(&node.argument),
        _ => false,
    }
}

fn expression_declares_binding(expr: &Expression) -> bool {
    match expr {
        Expression::LetExpression { .. } => true,
        Expression::Unary { argument, .. } => expression_declares_binding(argument),
        Expression::Binary { left, right, .. }
        | Expression::DefinedOr { left, right, .. }
        | Expression::Assignment { left, right, .. } => {
            expression_declares_binding(left) || expression_declares_binding(right)
        }
        Expression::Ternary {
            test,
            consequent,
            alternate,
            ..
        } => {
            expression_declares_binding(test)
                || expression_declares_binding(consequent)
                || expression_declares_binding(alternate)
        }
        Expression::Call {
            callee, arguments, ..
        } => {
            expression_declares_binding(callee)
                || arguments.iter().any(call_argument_declares_binding)
        }
        Expression::DynamicMemberCall {
            object,
            member,
            arguments,
            ..
        } => {
            expression_declares_binding(object)
                || expression_declares_binding(member)
                || arguments.iter().any(call_argument_declares_binding)
        }
        Expression::MemberAccess { object, .. } => expression_declares_binding(object),
        Expression::Index { object, index, .. } => {
            expression_declares_binding(object) || expression_declares_binding(index)
        }
        Expression::Slice {
            object, start, end, ..
        } => {
            expression_declares_binding(object)
                || start
                    .as_ref()
                    .map(|expr| expression_declares_binding(expr))
                    .unwrap_or(false)
                || end
                    .as_ref()
                    .map(|expr| expression_declares_binding(expr))
                    .unwrap_or(false)
        }
        Expression::DictAccess { object, key, .. } => {
            expression_declares_binding(object) || dict_key_declares_binding(key)
        }
        Expression::PostfixUpdate { argument, .. } => expression_declares_binding(argument),
        Expression::Lambda { body, .. } => expression_declares_binding(body),
        Expression::FunctionExpression { body, .. }
        | Expression::TryExpression { body, .. }
        | Expression::DoExpression { body, .. }
        | Expression::AwaitExpression { body, .. }
        | Expression::SpawnExpression { body, .. } => block_needs_lexical_scope(body),
        Expression::ArrayLiteral { elements, .. }
        | Expression::SetLiteral { elements, .. }
        | Expression::BagLiteral { elements, .. } => {
            elements.iter().any(expression_declares_binding)
        }
        Expression::DictLiteral { entries, .. } | Expression::PairListLiteral { entries, .. } => {
            entries.iter().any(|entry| {
                dict_key_declares_binding(&entry.key) || expression_declares_binding(&entry.value)
            })
        }
        Expression::TemplateLiteral { parts, .. } => parts.iter().any(|part| match part {
            TemplatePart::Expression { expression, .. } => expression_declares_binding(expression),
            TemplatePart::Text { .. } => false,
        }),
        Expression::SuperCall { arguments, .. } => {
            arguments.iter().any(call_argument_declares_binding)
        }
        Expression::Identifier { .. }
        | Expression::NumberLiteral { .. }
        | Expression::StringLiteral { .. }
        | Expression::RegexLiteral { .. }
        | Expression::BooleanLiteral { .. }
        | Expression::NullLiteral { .. } => false,
    }
}

fn call_argument_declares_binding(argument: &CallArgument) -> bool {
    match argument {
        CallArgument::Positional { value, .. } => expression_declares_binding(value),
        CallArgument::Named { name, value, .. } => {
            dict_key_declares_binding(name) || expression_declares_binding(value)
        }
    }
}

fn dict_key_declares_binding(key: &DictKey) -> bool {
    match key {
        DictKey::Expression { expression, .. } => expression_declares_binding(expression),
        DictKey::Identifier { .. } | DictKey::StringLiteral { .. } => false,
    }
}

fn is_terminal_statement(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::ReturnStatement(_)
            | Statement::ThrowStatement(_)
            | Statement::DieStatement(_)
            | Statement::LoopControlStatement(_)
    )
}

fn fold_expression(expr: &Expression) -> Option<Expression> {
    match expr {
        Expression::Unary {
            line,
            source_file,
            operator,
            argument,
            inferred_type,
        } => fold_unary(
            *line,
            source_file.clone(),
            operator,
            argument,
            inferred_type.clone(),
        ),
        Expression::Binary {
            line,
            source_file,
            operator,
            left,
            right,
            inferred_type,
        } => fold_binary(
            *line,
            source_file.clone(),
            operator,
            left,
            right,
            inferred_type.clone(),
        ),
        Expression::Ternary {
            test,
            consequent,
            alternate,
            ..
        } => match literal_truth(test)? {
            true => Some((**consequent).clone()),
            false => Some((**alternate).clone()),
        },
        Expression::TemplateLiteral {
            line,
            source_file,
            parts,
            inferred_type,
        } => {
            let mut text = String::new();
            for part in parts {
                match part {
                    TemplatePart::Text { value, .. } => text.push_str(value),
                    TemplatePart::Expression { expression, .. } => {
                        text.push_str(&literal_render(expression)?);
                    }
                }
            }
            Some(Expression::StringLiteral {
                line: *line,
                source_file: source_file.clone(),
                value: text,
                inferred_type: inferred_type.clone(),
            })
        }
        _ => None,
    }
}

fn fold_unary(
    line: usize,
    source_file: Option<String>,
    operator: &str,
    argument: &Expression,
    inferred_type: Option<String>,
) -> Option<Expression> {
    match preferred_operator(operator) {
        "+" => Some(number_literal(
            line,
            source_file,
            literal_number(argument)?,
            inferred_type,
        )),
        "-" => Some(number_literal(
            line,
            source_file,
            -literal_number(argument)?,
            inferred_type,
        )),
        "¬" | "!" => Some(Expression::BooleanLiteral {
            line,
            source_file,
            value: !literal_truth(argument)?,
            inferred_type,
        }),
        "√" => Some(number_literal(
            line,
            source_file,
            literal_number(argument)?.sqrt(),
            inferred_type,
        )),
        "abs" => Some(number_literal(
            line,
            source_file,
            literal_number(argument)?.abs(),
            inferred_type,
        )),
        "floor" => Some(number_literal(
            line,
            source_file,
            literal_number(argument)?.floor(),
            inferred_type,
        )),
        "ceil" => Some(number_literal(
            line,
            source_file,
            literal_number(argument)?.ceil(),
            inferred_type,
        )),
        "round" => Some(number_literal(
            line,
            source_file,
            literal_number(argument)?.round(),
            inferred_type,
        )),
        "int" => Some(number_literal(
            line,
            source_file,
            literal_number(argument)?.trunc(),
            inferred_type,
        )),
        "uc" => Some(Expression::StringLiteral {
            line,
            source_file,
            value: literal_string(argument)?.to_uppercase(),
            inferred_type,
        }),
        "lc" => Some(Expression::StringLiteral {
            line,
            source_file,
            value: literal_string(argument)?.to_lowercase(),
            inferred_type,
        }),
        "length" => Some(number_literal(
            line,
            source_file,
            literal_string(argument)?.chars().count() as f64,
            inferred_type,
        )),
        _ => None,
    }
}

fn fold_binary(
    line: usize,
    source_file: Option<String>,
    operator: &str,
    left: &Expression,
    right: &Expression,
    inferred_type: Option<String>,
) -> Option<Expression> {
    let operator = preferred_operator(operator);
    match operator {
        "+" | "-" | "×" | "÷" | "mod" | "**" => {
            let lhs = literal_number(left)?;
            let rhs = literal_number(right)?;
            let value = match operator {
                "+" => lhs + rhs,
                "-" => lhs - rhs,
                "×" => lhs * rhs,
                "÷" => lhs / rhs,
                "mod" => lhs % rhs,
                "**" => lhs.powf(rhs),
                _ => unreachable!(),
            };
            Some(number_literal(line, source_file, value, inferred_type))
        }
        "_" => Some(Expression::StringLiteral {
            line,
            source_file,
            value: format!("{}{}", literal_render(left)?, literal_render(right)?),
            inferred_type,
        }),
        "⋀" => Some(Expression::BooleanLiteral {
            line,
            source_file,
            value: literal_truth(left)? && literal_truth(right)?,
            inferred_type,
        }),
        "⋁" => Some(Expression::BooleanLiteral {
            line,
            source_file,
            value: literal_truth(left)? || literal_truth(right)?,
            inferred_type,
        }),
        "⊻" => Some(Expression::BooleanLiteral {
            line,
            source_file,
            value: literal_truth(left)? ^ literal_truth(right)?,
            inferred_type,
        }),
        "⊼" => Some(Expression::BooleanLiteral {
            line,
            source_file,
            value: !(literal_truth(left)? && literal_truth(right)?),
            inferred_type,
        }),
        "=" | "≠" | "<" | ">" | "≤" | "≥" => {
            let lhs = literal_number(left)?;
            let rhs = literal_number(right)?;
            let value = match operator {
                "=" => lhs == rhs,
                "≠" => lhs != rhs,
                "<" => lhs < rhs,
                ">" => lhs > rhs,
                "≤" => lhs <= rhs,
                "≥" => lhs >= rhs,
                _ => unreachable!(),
            };
            Some(Expression::BooleanLiteral {
                line,
                source_file,
                value,
                inferred_type,
            })
        }
        "eq" | "ne" | "gt" | "ge" | "lt" | "le" | "eqi" | "nei" | "gti" | "gei" | "lti" | "lei" => {
            let mut lhs = literal_render(left)?;
            let mut rhs = literal_render(right)?;
            if matches!(operator, "eqi" | "nei" | "gti" | "gei" | "lti" | "lei") {
                lhs = lhs.to_lowercase();
                rhs = rhs.to_lowercase();
            }
            let value = match operator {
                "eq" | "eqi" => lhs == rhs,
                "ne" | "nei" => lhs != rhs,
                "gt" | "gti" => lhs > rhs,
                "ge" | "gei" => lhs >= rhs,
                "lt" | "lti" => lhs < rhs,
                "le" | "lei" => lhs <= rhs,
                _ => unreachable!(),
            };
            Some(Expression::BooleanLiteral {
                line,
                source_file,
                value,
                inferred_type,
            })
        }
        "≡" | "≢" => {
            let value = literal_eq(left, right)?;
            Some(Expression::BooleanLiteral {
                line,
                source_file,
                value: if operator == "≡" { value } else { !value },
                inferred_type,
            })
        }
        "..." => None,
        _ => None,
    }
}

fn number_literal(
    line: usize,
    source_file: Option<String>,
    value: f64,
    inferred_type: Option<String>,
) -> Expression {
    Expression::NumberLiteral {
        line,
        source_file,
        value: if value.fract() == 0.0 {
            format!("{value:.0}")
        } else {
            value.to_string()
        },
        inferred_type,
    }
}

fn literal_number(expr: &Expression) -> Option<f64> {
    match expr {
        Expression::NumberLiteral { value, .. } => value.parse().ok(),
        _ => None,
    }
}

fn literal_string(expr: &Expression) -> Option<&str> {
    match expr {
        Expression::StringLiteral { value, .. } => Some(value),
        _ => None,
    }
}

fn literal_truth(expr: &Expression) -> Option<bool> {
    match expr {
        Expression::BooleanLiteral { value, .. } => Some(*value),
        Expression::NullLiteral { .. } => Some(false),
        Expression::NumberLiteral { value, .. } => Some(value.parse::<f64>().ok()? != 0.0),
        Expression::StringLiteral { value, .. } => Some(!value.is_empty()),
        _ => None,
    }
}

fn literal_boolean(expr: &Expression) -> Option<bool> {
    match expr {
        Expression::BooleanLiteral { value, .. } => Some(*value),
        _ => None,
    }
}

fn literal_render(expr: &Expression) -> Option<String> {
    match expr {
        Expression::StringLiteral { value, .. } => Some(value.clone()),
        Expression::NumberLiteral { value, .. } => Some(value.clone()),
        Expression::BooleanLiteral { value, .. } => {
            Some(if *value { "true" } else { "false" }.to_owned())
        }
        Expression::NullLiteral { .. } => Some(String::new()),
        _ => None,
    }
}

fn literal_eq(left: &Expression, right: &Expression) -> Option<bool> {
    match (left, right) {
        (Expression::NullLiteral { .. }, Expression::NullLiteral { .. }) => Some(true),
        (
            Expression::BooleanLiteral { value: lhs, .. },
            Expression::BooleanLiteral { value: rhs, .. },
        ) => Some(lhs == rhs),
        (
            Expression::NumberLiteral { value: lhs, .. },
            Expression::NumberLiteral { value: rhs, .. },
        ) => Some(lhs.parse::<f64>().ok()? == rhs.parse::<f64>().ok()?),
        (
            Expression::StringLiteral { value: lhs, .. },
            Expression::StringLiteral { value: rhs, .. },
        ) => Some(lhs == rhs),
        _ => None,
    }
}

pub fn preferred_operator(operator: &str) -> &str {
    match operator {
        "sqrt" => "√",
        "*" => "×",
        "/" => "÷",
        "<=" => "≤",
        ">=" => "≥",
        "<=>" | "≷" => "≶",
        "==" => "≡",
        "!=" => "≢",
        "not" => "¬",
        "and" => "⋀",
        "nand" => "⊼",
        "xor" => "⊻",
        "or" => "⋁",
        "union" => "⋃",
        "intersection" => "⋂",
        "\\" => "∖",
        "in" => "∈",
        "subsetof" => "⊂",
        "supersetof" => "⊃",
        "equivalentof" => "⊂⊃",
        "*=" => "×=",
        "/=" => "÷=",
        other => other,
    }
}

fn build_switch_index(node: &SwitchStatement) -> Option<Vec<SwitchIndexEntry>> {
    let comparator = node.comparator.as_deref().map(preferred_operator);
    if !matches!(comparator, None | Some("=") | Some("≡")) {
        return None;
    }
    let mut entries = Vec::new();
    let mut seen = HashSet::new();
    for (case_index, case) in node.cases.iter().enumerate() {
        for value in &case.values {
            let key = switch_key(value)?;
            if !seen.insert(key.clone()) {
                return None;
            }
            entries.push(SwitchIndexEntry { key, case_index });
        }
    }
    if entries.is_empty() {
        None
    } else {
        Some(entries)
    }
}

fn switch_key(expr: &Expression) -> Option<String> {
    match expr {
        Expression::NullLiteral { .. } => Some("n:".to_owned()),
        Expression::BooleanLiteral { value, .. } => Some(format!("b:{value}")),
        Expression::NumberLiteral { value, .. } => {
            Some(format!("f:{}", value.parse::<f64>().ok()?))
        }
        Expression::StringLiteral { value, .. } => Some(format!("s:{value}")),
        _ => None,
    }
}

fn propagate_constants(statements: &mut [Statement]) {
    let mut scopes = vec![HashMap::<String, Expression>::new()];
    propagate_constants_statements(statements, &mut scopes);
}

fn propagate_constants_statements(
    statements: &mut [Statement],
    scopes: &mut Vec<HashMap<String, Expression>>,
) {
    for statement in statements {
        propagate_constants_statement(statement, scopes);
        record_constant_declaration(statement, scopes);
    }
}

fn propagate_constants_statement(
    statement: &mut Statement,
    scopes: &mut Vec<HashMap<String, Expression>>,
) {
    match statement {
        Statement::Block(block) => propagate_constants_block(block, scopes),
        Statement::VariableDeclaration(node) => {
            if let Some(init) = &mut node.init {
                propagate_constants_expression(init, scopes, false);
            }
        }
        Statement::FunctionDeclaration(node) => {
            forget_assigned_in_block(scopes, &node.body);
        }
        Statement::ClassDeclaration(node) => {
            for member in &mut node.body {
                propagate_constants_class_member(member);
            }
        }
        Statement::TraitDeclaration(node) => {
            for member in &mut node.body {
                propagate_constants_class_member(member);
            }
        }
        Statement::ImportDeclaration(node) => {
            if let Some(condition) = &mut node.condition {
                propagate_constants_expression(&mut condition.test, scopes, false);
            }
        }
        Statement::IfStatement(node) => {
            propagate_constants_expression(&mut node.test, scopes, false);
            let mut consequent_scopes = scopes.clone();
            propagate_constants_block(&mut node.consequent, &mut consequent_scopes);
            if let Some(alternate) = &mut node.alternate {
                let mut alternate_scopes = scopes.clone();
                propagate_constants_statement(alternate, &mut alternate_scopes);
            }
            forget_assigned_in_block(scopes, &node.consequent);
            if let Some(alternate) = &node.alternate {
                forget_assigned_in_statement(scopes, alternate);
            }
        }
        Statement::WhileStatement(node) => {
            let mut body_scopes = scopes.clone();
            forget_assigned_in_block(&mut body_scopes, &node.body);
            propagate_constants_expression(&mut node.test, &mut body_scopes, false);
            propagate_constants_block(&mut node.body, &mut body_scopes);
            forget_assigned_in_block(scopes, &node.body);
        }
        Statement::ForStatement(node) => {
            propagate_constants_expression(&mut node.iterable, scopes, false);
            let mut body_scopes = scopes.clone();
            body_scopes.push(HashMap::new());
            forget_assigned_in_block(&mut body_scopes, &node.body);
            forget_constant(&mut body_scopes, &node.variable);
            propagate_constants_statements(&mut node.body.statements, &mut body_scopes);
            if let Some(else_block) = &mut node.else_block {
                let mut else_scopes = scopes.clone();
                propagate_constants_block(else_block, &mut else_scopes);
            }
            forget_assigned_in_block(scopes, &node.body);
            if let Some(else_block) = &node.else_block {
                forget_assigned_in_block(scopes, else_block);
            }
        }
        Statement::SwitchStatement(node) => {
            propagate_constants_expression(&mut node.discriminant, scopes, false);
            let mut case_assigned_scopes = scopes.clone();
            for case in &node.cases {
                forget_assigned_in_statements(&mut case_assigned_scopes, &case.consequent);
            }
            if let Some(default) = &node.default {
                forget_assigned_in_statements(&mut case_assigned_scopes, default);
            }
            for case in &mut node.cases {
                for value in &mut case.values {
                    propagate_constants_expression(value, scopes, false);
                }
                let mut case_scopes = case_assigned_scopes.clone();
                case_scopes.push(HashMap::new());
                propagate_constants_statements(&mut case.consequent, &mut case_scopes);
            }
            if let Some(default) = &mut node.default {
                let mut default_scopes = case_assigned_scopes.clone();
                default_scopes.push(HashMap::new());
                propagate_constants_statements(default, &mut default_scopes);
            }
            for case in &node.cases {
                forget_assigned_in_statements(scopes, &case.consequent);
            }
            if let Some(default) = &node.default {
                forget_assigned_in_statements(scopes, default);
            }
        }
        Statement::TryStatement(node) => {
            let mut body_scopes = scopes.clone();
            propagate_constants_block(&mut node.body, &mut body_scopes);
            for handler in &mut node.handlers {
                let mut handler_scopes = scopes.clone();
                handler_scopes.push(HashMap::new());
                if let Some(name) = handler
                    .binding
                    .as_ref()
                    .and_then(|binding| binding.name.as_ref())
                {
                    forget_constant(&mut handler_scopes, name);
                }
                propagate_constants_statements(&mut handler.body.statements, &mut handler_scopes);
            }
            forget_assigned_in_block(scopes, &node.body);
            for handler in &node.handlers {
                forget_assigned_in_block(scopes, &handler.body);
            }
        }
        Statement::ReturnStatement(node) => {
            if let Some(argument) = &mut node.argument {
                propagate_constants_expression(argument, scopes, false);
            }
        }
        Statement::ThrowStatement(node) => {
            propagate_constants_expression(&mut node.argument, scopes, false);
        }
        Statement::DieStatement(node) => {
            propagate_constants_expression(&mut node.argument, scopes, false);
        }
        Statement::PostfixConditionalStatement(node) => {
            propagate_constants_statement(&mut node.statement, scopes);
            propagate_constants_expression(&mut node.test, scopes, false);
        }
        Statement::KeywordStatement(node) => {
            for argument in &mut node.arguments {
                propagate_constants_expression(argument, scopes, false);
            }
        }
        Statement::ExpressionStatement(node) => {
            propagate_constants_expression(&mut node.expression, scopes, false);
        }
        Statement::LoopControlStatement(_) => {}
    }
}

fn propagate_constants_block(
    block: &mut BlockStatement,
    scopes: &mut Vec<HashMap<String, Expression>>,
) {
    if block.needs_lexical_scope {
        scopes.push(HashMap::new());
        propagate_constants_statements(&mut block.statements, scopes);
        scopes.pop();
    } else {
        propagate_constants_statements(&mut block.statements, scopes);
    }
}

fn propagate_constants_class_member(member: &mut ClassMember) {
    match member {
        ClassMember::Field(field) => {
            if let Some(default_value) = &mut field.default_value {
                let mut scopes = vec![HashMap::new()];
                propagate_constants_expression(default_value, &mut scopes, false);
            }
        }
        ClassMember::Method(method) => {
            let _ = method;
        }
        ClassMember::Class(class) => {
            for member in &mut class.body {
                propagate_constants_class_member(member);
            }
        }
        ClassMember::Trait(trait_decl) => {
            for member in &mut trait_decl.body {
                propagate_constants_class_member(member);
            }
        }
    }
}

fn record_constant_declaration(
    statement: &Statement,
    scopes: &mut Vec<HashMap<String, Expression>>,
) {
    match statement {
        Statement::VariableDeclaration(node) => {
            if let Some(init) = node.init.as_ref().and_then(literal_constant) {
                if let Some(scope) = scopes.last_mut() {
                    scope.insert(node.name.clone(), init);
                }
            } else {
                forget_constant(scopes, &node.name);
            }
        }
        Statement::FunctionDeclaration(node) => forget_constant(scopes, &node.name),
        Statement::ClassDeclaration(node) => forget_constant(scopes, &node.name),
        Statement::TraitDeclaration(node) => forget_constant(scopes, &node.name),
        Statement::ImportDeclaration(node) => {
            for specifier in &node.specifiers {
                forget_constant(scopes, &specifier.local);
            }
        }
        _ => {}
    }
}

fn propagate_constants_expression(
    expr: &mut Expression,
    scopes: &mut Vec<HashMap<String, Expression>>,
    lvalue_context: bool,
) {
    match expr {
        Expression::Identifier {
            line,
            source_file,
            name,
            inferred_type,
            ..
        } if !lvalue_context => {
            if let Some(value) = lookup_constant(scopes, name) {
                *expr = constant_for_use(&value, *line, source_file.clone(), inferred_type.clone());
            }
        }
        Expression::Unary {
            operator, argument, ..
        } if operator == "\\" => {
            propagate_constants_expression(argument, scopes, true);
        }
        Expression::Unary {
            operator, argument, ..
        } if matches!(operator.as_str(), "++" | "--") => {
            propagate_constants_expression(argument, scopes, true);
            forget_lvalue_constants(scopes, argument);
        }
        Expression::Unary { argument, .. } => {
            propagate_constants_expression(argument, scopes, false);
        }
        Expression::Binary { left, right, .. } | Expression::DefinedOr { left, right, .. } => {
            propagate_constants_expression(left, scopes, false);
            propagate_constants_expression(right, scopes, false);
        }
        Expression::Assignment {
            operator,
            left,
            right,
            ..
        } => {
            propagate_constants_expression(left, scopes, true);
            if operator == "~=" {
                propagate_regex_replacement_pattern(right, scopes);
            } else {
                propagate_constants_expression(right, scopes, false);
            }
            forget_lvalue_constants(scopes, left);
        }
        Expression::Ternary {
            test,
            consequent,
            alternate,
            ..
        } => {
            propagate_constants_expression(test, scopes, false);
            let mut consequent_scopes = scopes.clone();
            propagate_constants_expression(consequent, &mut consequent_scopes, false);
            let mut alternate_scopes = scopes.clone();
            propagate_constants_expression(alternate, &mut alternate_scopes, false);
        }
        Expression::Call {
            callee, arguments, ..
        } => {
            let mutates_caller_scope = call_may_mutate_caller_scope(callee);
            propagate_constants_expression(callee, scopes, false);
            for argument in arguments {
                propagate_constants_call_argument(argument, scopes);
            }
            if mutates_caller_scope {
                clear_constants(scopes);
            }
        }
        Expression::MemberAccess { object, .. } => {
            propagate_constants_expression(object, scopes, lvalue_context);
        }
        Expression::DynamicMemberCall {
            object,
            member,
            arguments,
            ..
        } => {
            propagate_constants_expression(object, scopes, false);
            propagate_constants_expression(member, scopes, false);
            for argument in arguments {
                propagate_constants_call_argument(argument, scopes);
            }
        }
        Expression::Index { object, index, .. } => {
            propagate_constants_expression(object, scopes, lvalue_context);
            propagate_constants_expression(index, scopes, false);
        }
        Expression::Slice {
            object, start, end, ..
        } => {
            propagate_constants_expression(object, scopes, lvalue_context);
            if let Some(start) = start {
                propagate_constants_expression(start, scopes, false);
            }
            if let Some(end) = end {
                propagate_constants_expression(end, scopes, false);
            }
        }
        Expression::DictAccess { object, key, .. } => {
            propagate_constants_expression(object, scopes, lvalue_context);
            propagate_constants_dict_key(key, scopes);
        }
        Expression::PostfixUpdate { argument, .. } => {
            propagate_constants_expression(argument, scopes, true);
            forget_lvalue_constants(scopes, argument);
        }
        Expression::Lambda { body, .. } => {
            forget_assigned_in_expression(scopes, body);
        }
        Expression::FunctionExpression { body, .. } => {
            forget_assigned_in_block(scopes, body);
        }
        Expression::LetExpression { name, init, .. } => {
            if let Some(init) = init {
                propagate_constants_expression(init, scopes, false);
                if let Some(value) = literal_constant(init) {
                    if let Some(scope) = scopes.last_mut() {
                        scope.insert(name.clone(), value);
                    }
                } else {
                    forget_constant(scopes, name);
                }
            } else {
                forget_constant(scopes, name);
            }
        }
        Expression::TryExpression { body, handlers, .. } => {
            let mut body_scopes = scopes.clone();
            propagate_constants_block(body, &mut body_scopes);
            for handler in handlers {
                let mut handler_scopes = scopes.clone();
                handler_scopes.push(HashMap::new());
                if let Some(name) = handler
                    .binding
                    .as_ref()
                    .and_then(|binding| binding.name.as_ref())
                {
                    forget_constant(&mut handler_scopes, name);
                }
                propagate_constants_statements(&mut handler.body.statements, &mut handler_scopes);
            }
        }
        Expression::DoExpression { body, .. }
        | Expression::AwaitExpression { body, .. }
        | Expression::SpawnExpression { body, .. } => {
            let mut body_scopes = scopes.clone();
            propagate_constants_block(body, &mut body_scopes);
            forget_assigned_in_block(scopes, body);
        }
        Expression::ArrayLiteral { elements, .. }
        | Expression::SetLiteral { elements, .. }
        | Expression::BagLiteral { elements, .. } => {
            for element in elements {
                propagate_constants_expression(element, scopes, false);
            }
        }
        Expression::DictLiteral { entries, .. } | Expression::PairListLiteral { entries, .. } => {
            for entry in entries {
                propagate_constants_dict_key(&mut entry.key, scopes);
                propagate_constants_expression(&mut entry.value, scopes, false);
            }
        }
        Expression::TemplateLiteral { parts, .. } => {
            for part in parts {
                if let TemplatePart::Expression { expression, .. } = part {
                    propagate_constants_expression(expression, scopes, false);
                }
            }
        }
        Expression::SuperCall { arguments, .. } => {
            for argument in arguments {
                propagate_constants_call_argument(argument, scopes);
            }
        }
        Expression::Identifier { .. }
        | Expression::NumberLiteral { .. }
        | Expression::StringLiteral { .. }
        | Expression::RegexLiteral { .. }
        | Expression::BooleanLiteral { .. }
        | Expression::NullLiteral { .. } => {}
    }
    if !lvalue_context {
        if let Some(folded) = fold_expression(expr) {
            *expr = folded;
        }
    }
}

fn propagate_constants_call_argument(
    argument: &mut CallArgument,
    scopes: &mut Vec<HashMap<String, Expression>>,
) {
    match argument {
        CallArgument::Positional { value, .. } => {
            propagate_constants_expression(value, scopes, false);
        }
        CallArgument::Named { name, value, .. } => {
            propagate_constants_dict_key(name, scopes);
            propagate_constants_expression(value, scopes, false);
        }
    }
}

fn call_may_mutate_caller_scope(callee: &Expression) -> bool {
    matches!(callee, Expression::Identifier { name, .. } if name == "eval")
}

fn clear_constants(scopes: &mut [HashMap<String, Expression>]) {
    for scope in scopes {
        scope.clear();
    }
}

fn propagate_regex_replacement_pattern(
    replacement: &mut Expression,
    scopes: &mut Vec<HashMap<String, Expression>>,
) {
    if let Expression::Binary { operator, left, .. } = replacement {
        if operator == "->" {
            propagate_constants_expression(left, scopes, false);
        }
    }
}

fn propagate_constants_dict_key(key: &mut DictKey, scopes: &mut Vec<HashMap<String, Expression>>) {
    if let DictKey::Expression { expression, .. } = key {
        propagate_constants_expression(expression, scopes, false);
    }
}

fn lookup_constant(scopes: &[HashMap<String, Expression>], name: &str) -> Option<Expression> {
    for scope in scopes.iter().rev() {
        if let Some(value) = scope.get(name) {
            return Some(value.clone());
        }
    }
    None
}

fn forget_constant(scopes: &mut [HashMap<String, Expression>], name: &str) {
    for scope in scopes.iter_mut().rev() {
        if scope.remove(name).is_some() {
            return;
        }
    }
}

fn forget_lvalue_constants(scopes: &mut [HashMap<String, Expression>], target: &Expression) {
    match target {
        Expression::Identifier { name, .. } => forget_constant(scopes, name),
        Expression::Binary { left, .. } => forget_lvalue_constants(scopes, left),
        Expression::MemberAccess { object, .. }
        | Expression::Index { object, .. }
        | Expression::Slice { object, .. }
        | Expression::DictAccess { object, .. } => forget_lvalue_constants(scopes, object),
        _ => {}
    }
}

fn forget_assigned_in_block(scopes: &mut [HashMap<String, Expression>], block: &BlockStatement) {
    forget_assigned_in_statements(scopes, &block.statements);
}

fn forget_assigned_in_statements(
    scopes: &mut [HashMap<String, Expression>],
    statements: &[Statement],
) {
    for statement in statements {
        forget_assigned_in_statement(scopes, statement);
    }
}

fn forget_assigned_in_statement(scopes: &mut [HashMap<String, Expression>], statement: &Statement) {
    match statement {
        Statement::Block(block) => forget_assigned_in_block(scopes, block),
        Statement::VariableDeclaration(_) => {}
        Statement::FunctionDeclaration(_) => {}
        Statement::ClassDeclaration(_) => {}
        Statement::TraitDeclaration(_) => {}
        Statement::ImportDeclaration(_) => {}
        Statement::IfStatement(node) => {
            forget_assigned_in_block(scopes, &node.consequent);
            if let Some(alternate) = &node.alternate {
                forget_assigned_in_statement(scopes, alternate);
            }
        }
        Statement::WhileStatement(node) => forget_assigned_in_block(scopes, &node.body),
        Statement::ForStatement(node) => {
            forget_assigned_in_block(scopes, &node.body);
            if let Some(else_block) = &node.else_block {
                forget_assigned_in_block(scopes, else_block);
            }
        }
        Statement::SwitchStatement(node) => {
            for case in &node.cases {
                forget_assigned_in_statements(scopes, &case.consequent);
            }
            if let Some(default) = &node.default {
                forget_assigned_in_statements(scopes, default);
            }
        }
        Statement::TryStatement(node) => {
            forget_assigned_in_block(scopes, &node.body);
            for handler in &node.handlers {
                forget_assigned_in_block(scopes, &handler.body);
            }
        }
        Statement::ReturnStatement(node) => {
            if let Some(argument) = &node.argument {
                forget_assigned_in_expression(scopes, argument);
            }
        }
        Statement::ThrowStatement(node) => forget_assigned_in_expression(scopes, &node.argument),
        Statement::DieStatement(node) => forget_assigned_in_expression(scopes, &node.argument),
        Statement::PostfixConditionalStatement(node) => {
            forget_assigned_in_statement(scopes, &node.statement);
            forget_assigned_in_expression(scopes, &node.test);
        }
        Statement::KeywordStatement(node) => {
            for argument in &node.arguments {
                forget_assigned_in_expression(scopes, argument);
            }
        }
        Statement::ExpressionStatement(node) => {
            forget_assigned_in_expression(scopes, &node.expression);
        }
        Statement::LoopControlStatement(_) => {}
    }
}

fn forget_assigned_in_expression(scopes: &mut [HashMap<String, Expression>], expr: &Expression) {
    match expr {
        Expression::Unary {
            operator, argument, ..
        } if matches!(operator.as_str(), "++" | "--") => {
            forget_lvalue_constants(scopes, argument);
        }
        Expression::Unary { argument, .. } => forget_assigned_in_expression(scopes, argument),
        Expression::Binary { left, right, .. } | Expression::DefinedOr { left, right, .. } => {
            forget_assigned_in_expression(scopes, left);
            forget_assigned_in_expression(scopes, right);
        }
        Expression::Assignment { left, right, .. } => {
            forget_lvalue_constants(scopes, left);
            forget_assigned_in_expression(scopes, right);
        }
        Expression::Ternary {
            test,
            consequent,
            alternate,
            ..
        } => {
            forget_assigned_in_expression(scopes, test);
            forget_assigned_in_expression(scopes, consequent);
            forget_assigned_in_expression(scopes, alternate);
        }
        Expression::Call {
            callee, arguments, ..
        } => {
            forget_assigned_in_expression(scopes, callee);
            for argument in arguments {
                forget_assigned_in_call_argument(scopes, argument);
            }
        }
        Expression::MemberAccess { object, .. } => forget_assigned_in_expression(scopes, object),
        Expression::DynamicMemberCall {
            object,
            member,
            arguments,
            ..
        } => {
            forget_assigned_in_expression(scopes, object);
            forget_assigned_in_expression(scopes, member);
            for argument in arguments {
                forget_assigned_in_call_argument(scopes, argument);
            }
        }
        Expression::Index { object, index, .. } => {
            forget_assigned_in_expression(scopes, object);
            forget_assigned_in_expression(scopes, index);
        }
        Expression::Slice {
            object, start, end, ..
        } => {
            forget_assigned_in_expression(scopes, object);
            if let Some(start) = start {
                forget_assigned_in_expression(scopes, start);
            }
            if let Some(end) = end {
                forget_assigned_in_expression(scopes, end);
            }
        }
        Expression::DictAccess { object, key, .. } => {
            forget_assigned_in_expression(scopes, object);
            forget_assigned_in_dict_key(scopes, key);
        }
        Expression::PostfixUpdate { argument, .. } => forget_lvalue_constants(scopes, argument),
        Expression::LetExpression { init, .. } => {
            if let Some(init) = init {
                forget_assigned_in_expression(scopes, init);
            }
        }
        Expression::TryExpression { body, handlers, .. } => {
            forget_assigned_in_block(scopes, body);
            for handler in handlers {
                forget_assigned_in_block(scopes, &handler.body);
            }
        }
        Expression::DoExpression { body, .. }
        | Expression::AwaitExpression { body, .. }
        | Expression::SpawnExpression { body, .. } => forget_assigned_in_block(scopes, body),
        Expression::ArrayLiteral { elements, .. }
        | Expression::SetLiteral { elements, .. }
        | Expression::BagLiteral { elements, .. } => {
            for element in elements {
                forget_assigned_in_expression(scopes, element);
            }
        }
        Expression::DictLiteral { entries, .. } | Expression::PairListLiteral { entries, .. } => {
            for entry in entries {
                forget_assigned_in_dict_key(scopes, &entry.key);
                forget_assigned_in_expression(scopes, &entry.value);
            }
        }
        Expression::TemplateLiteral { parts, .. } => {
            for part in parts {
                if let TemplatePart::Expression { expression, .. } = part {
                    forget_assigned_in_expression(scopes, expression);
                }
            }
        }
        Expression::SuperCall { arguments, .. } => {
            for argument in arguments {
                forget_assigned_in_call_argument(scopes, argument);
            }
        }
        Expression::Lambda { .. }
        | Expression::FunctionExpression { .. }
        | Expression::Identifier { .. }
        | Expression::NumberLiteral { .. }
        | Expression::StringLiteral { .. }
        | Expression::RegexLiteral { .. }
        | Expression::BooleanLiteral { .. }
        | Expression::NullLiteral { .. } => {}
    }
}

fn forget_assigned_in_call_argument(
    scopes: &mut [HashMap<String, Expression>],
    argument: &CallArgument,
) {
    match argument {
        CallArgument::Positional { value, .. } => forget_assigned_in_expression(scopes, value),
        CallArgument::Named { name, value, .. } => {
            forget_assigned_in_dict_key(scopes, name);
            forget_assigned_in_expression(scopes, value);
        }
    }
}

fn forget_assigned_in_dict_key(scopes: &mut [HashMap<String, Expression>], key: &DictKey) {
    if let DictKey::Expression { expression, .. } = key {
        forget_assigned_in_expression(scopes, expression);
    }
}

fn literal_constant(expr: &Expression) -> Option<Expression> {
    match expr {
        Expression::NumberLiteral { .. }
        | Expression::StringLiteral { .. }
        | Expression::BooleanLiteral { .. }
        | Expression::NullLiteral { .. } => Some(expr.clone()),
        _ => None,
    }
}

fn constant_for_use(
    value: &Expression,
    line: usize,
    source_file: Option<String>,
    inferred_type: Option<String>,
) -> Expression {
    match value {
        Expression::NumberLiteral { value, .. } => Expression::NumberLiteral {
            line,
            source_file,
            value: value.clone(),
            inferred_type,
        },
        Expression::StringLiteral { value, .. } => Expression::StringLiteral {
            line,
            source_file,
            value: value.clone(),
            inferred_type,
        },
        Expression::BooleanLiteral { value, .. } => Expression::BooleanLiteral {
            line,
            source_file,
            value: *value,
            inferred_type,
        },
        Expression::NullLiteral { .. } => Expression::NullLiteral {
            line,
            source_file,
            inferred_type,
        },
        other => other.clone(),
    }
}

fn annotate_identifier_depths(program: &mut Program) {
    let mut scopes = vec![root_scope()];
    annotate_statements(&mut program.statements, &mut scopes);
}

fn root_scope() -> HashSet<String> {
    [
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
        "__global__",
        "__system__",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn annotate_statements(statements: &mut [Statement], scopes: &mut Vec<HashSet<String>>) {
    for statement in statements {
        annotate_statement(statement, scopes);
        declare_statement_binding(statement, scopes);
    }
}

fn annotate_statement(statement: &mut Statement, scopes: &mut Vec<HashSet<String>>) {
    match statement {
        Statement::Block(block) => {
            annotate_block(block, scopes);
        }
        Statement::VariableDeclaration(node) => {
            if let Some(init) = &mut node.init {
                annotate_expression(init, scopes);
            }
        }
        Statement::FunctionDeclaration(node) => {
            scopes.push(function_scope(&node.params));
            annotate_statements(&mut node.body.statements, scopes);
            scopes.pop();
        }
        Statement::ClassDeclaration(node) => {
            for member in &mut node.body {
                annotate_class_member(member, scopes);
            }
        }
        Statement::TraitDeclaration(node) => {
            for member in &mut node.body {
                annotate_class_member(member, scopes);
            }
        }
        Statement::ImportDeclaration(node) => {
            if let Some(condition) = &mut node.condition {
                annotate_expression(&mut condition.test, scopes);
            }
        }
        Statement::IfStatement(node) => {
            annotate_expression(&mut node.test, scopes);
            annotate_block(&mut node.consequent, scopes);
            if let Some(alternate) = &mut node.alternate {
                annotate_statement(alternate, scopes);
            }
        }
        Statement::WhileStatement(node) => {
            annotate_expression(&mut node.test, scopes);
            annotate_block(&mut node.body, scopes);
        }
        Statement::ForStatement(node) => {
            annotate_expression(&mut node.iterable, scopes);
            let mut scope = HashSet::new();
            scope.insert(node.variable.clone());
            scopes.push(scope);
            annotate_statements(&mut node.body.statements, scopes);
            scopes.pop();
            if let Some(else_block) = &mut node.else_block {
                annotate_block(else_block, scopes);
            }
        }
        Statement::SwitchStatement(node) => {
            annotate_expression(&mut node.discriminant, scopes);
            for case in &mut node.cases {
                for value in &mut case.values {
                    annotate_expression(value, scopes);
                }
                scopes.push(HashSet::new());
                annotate_statements(&mut case.consequent, scopes);
                scopes.pop();
            }
            if let Some(default) = &mut node.default {
                scopes.push(HashSet::new());
                annotate_statements(default, scopes);
                scopes.pop();
            }
        }
        Statement::TryStatement(node) => {
            annotate_block(&mut node.body, scopes);
            for handler in &mut node.handlers {
                let mut scope = HashSet::new();
                if let Some(name) = handler
                    .binding
                    .as_ref()
                    .and_then(|binding| binding.name.clone())
                {
                    scope.insert(name);
                }
                scopes.push(scope);
                annotate_statements(&mut handler.body.statements, scopes);
                scopes.pop();
            }
        }
        Statement::ReturnStatement(node) => {
            if let Some(argument) = &mut node.argument {
                annotate_expression(argument, scopes);
            }
        }
        Statement::ThrowStatement(node) => annotate_expression(&mut node.argument, scopes),
        Statement::DieStatement(node) => annotate_expression(&mut node.argument, scopes),
        Statement::PostfixConditionalStatement(node) => {
            annotate_statement(&mut node.statement, scopes);
            annotate_expression(&mut node.test, scopes);
        }
        Statement::KeywordStatement(node) => {
            for argument in &mut node.arguments {
                annotate_expression(argument, scopes);
            }
        }
        Statement::ExpressionStatement(node) => annotate_expression(&mut node.expression, scopes),
        Statement::LoopControlStatement(_) => {}
    }
}

fn declare_statement_binding(statement: &Statement, scopes: &mut [HashSet<String>]) {
    let Some(scope) = scopes.last_mut() else {
        return;
    };
    match statement {
        Statement::VariableDeclaration(node) => {
            scope.insert(node.name.clone());
        }
        Statement::FunctionDeclaration(node) => {
            scope.insert(node.name.clone());
        }
        Statement::ClassDeclaration(node) => {
            scope.insert(node.name.clone());
        }
        Statement::TraitDeclaration(node) => {
            scope.insert(node.name.clone());
        }
        Statement::ImportDeclaration(node) => {
            for specifier in &node.specifiers {
                scope.insert(specifier.local.clone());
            }
        }
        _ => {}
    }
}

fn annotate_block(block: &mut BlockStatement, scopes: &mut Vec<HashSet<String>>) {
    if block.needs_lexical_scope {
        scopes.push(HashSet::new());
        annotate_statements(&mut block.statements, scopes);
        scopes.pop();
    } else {
        annotate_statements(&mut block.statements, scopes);
    }
}

fn function_scope(params: &[crate::ast::Parameter]) -> HashSet<String> {
    params.iter().map(|param| param.name.clone()).collect()
}

fn annotate_class_member(member: &mut ClassMember, scopes: &mut Vec<HashSet<String>>) {
    match member {
        ClassMember::Field(field) => {
            if let Some(default_value) = &mut field.default_value {
                annotate_expression(default_value, scopes);
            }
        }
        ClassMember::Method(method) => {
            let mut scope = function_scope(&method.params);
            scope.insert("self".to_owned());
            scopes.push(scope);
            annotate_statements(&mut method.body.statements, scopes);
            scopes.pop();
        }
        ClassMember::Class(class) => {
            for member in &mut class.body {
                annotate_class_member(member, scopes);
            }
        }
        ClassMember::Trait(trait_decl) => {
            for member in &mut trait_decl.body {
                annotate_class_member(member, scopes);
            }
        }
    }
}

fn annotate_expression(expr: &mut Expression, scopes: &mut Vec<HashSet<String>>) {
    match expr {
        Expression::Identifier {
            name,
            binding_depth,
            ..
        } => {
            *binding_depth = resolve_depth(name, scopes);
        }
        Expression::Unary { argument, .. } => annotate_expression(argument, scopes),
        Expression::Binary { left, right, .. }
        | Expression::DefinedOr { left, right, .. }
        | Expression::Assignment { left, right, .. } => {
            annotate_expression(left, scopes);
            annotate_expression(right, scopes);
        }
        Expression::Ternary {
            test,
            consequent,
            alternate,
            ..
        } => {
            annotate_expression(test, scopes);
            annotate_expression(consequent, scopes);
            annotate_expression(alternate, scopes);
        }
        Expression::Call {
            callee, arguments, ..
        } => {
            annotate_expression(callee, scopes);
            for argument in arguments {
                annotate_call_argument(argument, scopes);
            }
        }
        Expression::MemberAccess { object, .. } => annotate_expression(object, scopes),
        Expression::DynamicMemberCall {
            object,
            member,
            arguments,
            ..
        } => {
            annotate_expression(object, scopes);
            annotate_expression(member, scopes);
            for argument in arguments {
                annotate_call_argument(argument, scopes);
            }
        }
        Expression::Index { object, index, .. } => {
            annotate_expression(object, scopes);
            annotate_expression(index, scopes);
        }
        Expression::Slice {
            object, start, end, ..
        } => {
            annotate_expression(object, scopes);
            if let Some(start) = start {
                annotate_expression(start, scopes);
            }
            if let Some(end) = end {
                annotate_expression(end, scopes);
            }
        }
        Expression::DictAccess { object, key, .. } => {
            annotate_expression(object, scopes);
            annotate_dict_key(key, scopes);
        }
        Expression::PostfixUpdate { argument, .. } => annotate_expression(argument, scopes),
        Expression::Lambda { params, body, .. } => {
            scopes.push(function_scope(params));
            annotate_expression(body, scopes);
            scopes.pop();
        }
        Expression::FunctionExpression { params, body, .. } => {
            scopes.push(function_scope(params));
            annotate_statements(&mut body.statements, scopes);
            scopes.pop();
        }
        Expression::LetExpression { name, init, .. } => {
            if let Some(init) = init {
                annotate_expression(init, scopes);
            }
            if let Some(scope) = scopes.last_mut() {
                scope.insert(name.clone());
            }
        }
        Expression::TryExpression { body, handlers, .. } => {
            annotate_block(body, scopes);
            for handler in handlers {
                let mut scope = HashSet::new();
                if let Some(name) = handler
                    .binding
                    .as_ref()
                    .and_then(|binding| binding.name.clone())
                {
                    scope.insert(name);
                }
                scopes.push(scope);
                annotate_statements(&mut handler.body.statements, scopes);
                scopes.pop();
            }
        }
        Expression::DoExpression { body, .. }
        | Expression::AwaitExpression { body, .. }
        | Expression::SpawnExpression { body, .. } => {
            annotate_block(body, scopes);
        }
        Expression::ArrayLiteral { elements, .. }
        | Expression::SetLiteral { elements, .. }
        | Expression::BagLiteral { elements, .. } => {
            for element in elements {
                annotate_expression(element, scopes);
            }
        }
        Expression::DictLiteral { entries, .. } | Expression::PairListLiteral { entries, .. } => {
            for entry in entries {
                annotate_dict_key(&mut entry.key, scopes);
                annotate_expression(&mut entry.value, scopes);
            }
        }
        Expression::TemplateLiteral { parts, .. } => {
            for part in parts {
                if let TemplatePart::Expression { expression, .. } = part {
                    annotate_expression(expression, scopes);
                }
            }
        }
        Expression::SuperCall { arguments, .. } => {
            for argument in arguments {
                annotate_call_argument(argument, scopes);
            }
        }
        Expression::NumberLiteral { .. }
        | Expression::StringLiteral { .. }
        | Expression::RegexLiteral { .. }
        | Expression::BooleanLiteral { .. }
        | Expression::NullLiteral { .. } => {}
    }
}

fn annotate_call_argument(argument: &mut CallArgument, scopes: &mut Vec<HashSet<String>>) {
    match argument {
        CallArgument::Positional { value, .. } => annotate_expression(value, scopes),
        CallArgument::Named { name, value, .. } => {
            annotate_dict_key(name, scopes);
            annotate_expression(value, scopes);
        }
    }
}

fn annotate_dict_key(key: &mut DictKey, scopes: &mut Vec<HashSet<String>>) {
    if let DictKey::Expression { expression, .. } = key {
        annotate_expression(expression, scopes);
    }
}

fn resolve_depth(name: &str, scopes: &[HashSet<String>]) -> Option<usize> {
    for (depth, scope) in scopes.iter().rev().enumerate() {
        if scope.contains(name) {
            return Some(depth);
        }
    }
    None
}
