use crate::ast::{
    BlockStatement, CallArgument, CatchBinding, CatchClause, ClassDeclaration, ClassMember,
    DictEntry, DictKey, DieStatement, Expression, ExpressionStatement, FieldDeclaration,
    ForStatement, FunctionDeclaration, IfStatement, ImportDeclaration, ImportSpecifier,
    KeywordStatement, LoopControlStatement, MethodDeclaration, Parameter, PostfixCondition,
    PostfixConditionalStatement, Program, ReturnStatement, Statement, SwitchCase, SwitchStatement,
    TemplatePart as AstTemplatePart, ThrowStatement, TraitDeclaration, TryStatement,
    VariableDeclaration, WhileStatement,
};
use crate::error::{Result, ZuzuRustError};
use crate::token::{TemplatePart as TokenTemplatePart, Token, TokenKind};

pub struct Parser {
    tokens: Vec<Token>,
    index: usize,
    source_file: Option<String>,
}

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

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            index: 0,
            source_file: None,
        }
    }

    pub fn with_source_file(tokens: Vec<Token>, source_file: impl Into<String>) -> Self {
        Self {
            tokens,
            index: 0,
            source_file: Some(source_file.into()),
        }
    }

    fn source_file(&self) -> Option<String> {
        self.source_file.clone()
    }

    pub fn parse_program(&mut self) -> Result<Program> {
        let line = self.current_line();
        let mut statements = Vec::new();
        while !self.at_eof() {
            self.consume_semicolons();
            if self.at_eof() {
                break;
            }
            statements.push(self.parse_statement()?);
            self.consume_semicolons();
        }
        Ok(Program {
            line,
            source_file: self.source_file(),
            statements,
        })
    }

    pub fn parse_expression_root(mut self) -> Result<Expression> {
        let expression = self.parse_expression()?;
        if !self.at_eof() {
            return Err(self.error_current("Unexpected token after expression"));
        }
        Ok(expression)
    }

    fn parse_statement(&mut self) -> Result<Statement> {
        let statement = match self.current_kind() {
            TokenKind::Punct('{') => Statement::Block(self.parse_block_statement()?),
            TokenKind::Keyword("from") => {
                Statement::ImportDeclaration(self.parse_import_declaration()?)
            }
            TokenKind::Keyword("let") | TokenKind::Keyword("const") => {
                Statement::VariableDeclaration(self.parse_variable_declaration()?)
            }
            TokenKind::Keyword("function") | TokenKind::Keyword("async") => {
                Statement::FunctionDeclaration(self.parse_function_declaration()?)
            }
            TokenKind::Keyword("class") => {
                Statement::ClassDeclaration(self.parse_class_declaration()?)
            }
            TokenKind::Keyword("trait") => {
                Statement::TraitDeclaration(self.parse_trait_declaration()?)
            }
            TokenKind::Keyword("if") => Statement::IfStatement(self.parse_if_statement()?),
            TokenKind::Keyword("while") => Statement::WhileStatement(self.parse_while_statement()?),
            TokenKind::Keyword("for") => Statement::ForStatement(self.parse_for_statement()?),
            TokenKind::Keyword("switch") => {
                Statement::SwitchStatement(self.parse_switch_statement()?)
            }
            TokenKind::Keyword("try") => Statement::TryStatement(self.parse_try_statement()?),
            TokenKind::Keyword("return") => {
                Statement::ReturnStatement(self.parse_return_statement()?)
            }
            TokenKind::Keyword("next")
            | TokenKind::Keyword("continue")
            | TokenKind::Keyword("last") => {
                Statement::LoopControlStatement(self.parse_loop_control_statement()?)
            }
            TokenKind::Keyword("throw") => Statement::ThrowStatement(self.parse_throw_statement()?),
            TokenKind::Keyword("die") => Statement::DieStatement(self.parse_die_statement()?),
            TokenKind::Keyword("say")
            | TokenKind::Keyword("print")
            | TokenKind::Keyword("warn")
            | TokenKind::Keyword("assert")
            | TokenKind::Keyword("debug") => {
                Statement::KeywordStatement(self.parse_keyword_statement()?)
            }
            _ => Statement::ExpressionStatement(self.parse_expression_statement()?),
        };

        if statement_supports_postfix_condition(&statement) {
            if let Some(keyword) = self.match_postfix_condition_keyword() {
                let test = self.parse_expression()?;
                return Ok(Statement::PostfixConditionalStatement(
                    PostfixConditionalStatement {
                        line: statement.line(),
                        source_file: statement.source_file().map(str::to_owned),
                        statement: Box::new(statement),
                        keyword,
                        test,
                    },
                ));
            }
        }

        Ok(statement)
    }

    fn parse_block_statement(&mut self) -> Result<BlockStatement> {
        let line = self.current_line();
        self.expect_punct('{', "Expected '{'")?;
        let mut statements = Vec::new();
        self.consume_semicolons();
        while !self.check_punct('}') {
            statements.push(self.parse_statement()?);
            self.consume_semicolons();
        }
        self.expect_punct('}', "Expected '}' after block")?;
        Ok(BlockStatement {
            line,
            source_file: self.source_file(),
            statements,
            needs_lexical_scope: true,
        })
    }

    fn parse_variable_declaration(&mut self) -> Result<VariableDeclaration> {
        let line = self.current_line();
        let kind = self.expect_keyword_any(&["let", "const"])?;
        let (declared_type, name) = self.parse_typed_name()?;
        let mut is_weak_storage = self.parse_optional_weak_modifier("declaration")?;
        let init = if self.match_operator(":=") {
            let init = self.parse_expression()?;
            is_weak_storage |= self.parse_optional_weak_modifier("declaration")?;
            Some(init)
        } else {
            None
        };
        Ok(VariableDeclaration {
            line,
            source_file: self.source_file(),
            kind,
            declared_type,
            name,
            init,
            is_weak_storage,
            runtime_typecheck_required: None,
        })
    }

    fn parse_function_declaration(&mut self) -> Result<FunctionDeclaration> {
        let line = self.current_line();
        let is_async = self.match_keyword("async");
        self.expect_keyword("function")?;
        let name = self.expect_name("Expected function name")?;
        let params = self.parse_parameter_list()?;
        let return_type = self.parse_optional_return_type()?;
        let body = self.parse_block_statement()?;
        Ok(FunctionDeclaration {
            line,
            source_file: self.source_file(),
            name,
            params,
            return_type,
            body,
            is_async,
        })
    }

    fn parse_class_declaration(&mut self) -> Result<ClassDeclaration> {
        let line = self.current_line();
        self.expect_keyword("class")?;
        let name = self.expect_identifier("Expected class name")?;
        let base = if self.match_keyword("extends") {
            Some(self.expect_identifier("Expected base class name after extends")?)
        } else {
            None
        };
        let traits = self.parse_trait_composition_list()?;
        if self.match_punct(';') {
            return Ok(ClassDeclaration {
                line,
                source_file: self.source_file(),
                name,
                base,
                traits,
                body: Vec::new(),
                shorthand: true,
            });
        }

        self.expect_punct('{', "Expected '{' to start class body")?;
        let mut body = Vec::new();
        self.consume_semicolons();
        while !self.check_punct('}') {
            body.push(self.parse_class_member()?);
            self.consume_semicolons();
        }
        self.expect_punct('}', "Expected '}' after class body")?;
        Ok(ClassDeclaration {
            line,
            source_file: self.source_file(),
            name,
            base,
            traits,
            body,
            shorthand: false,
        })
    }

    fn parse_trait_declaration(&mut self) -> Result<TraitDeclaration> {
        let line = self.current_line();
        self.expect_keyword("trait")?;
        let name = self.expect_identifier("Expected trait name")?;
        if self.match_punct(';') {
            return Ok(TraitDeclaration {
                line,
                source_file: self.source_file(),
                name,
                body: Vec::new(),
                shorthand: true,
            });
        }
        self.expect_punct('{', "Expected '{' to start trait body")?;
        let mut body = Vec::new();
        self.consume_semicolons();
        while !self.check_punct('}') {
            body.push(self.parse_trait_member()?);
            self.consume_semicolons();
        }
        self.expect_punct('}', "Expected '}' after trait body")?;
        Ok(TraitDeclaration {
            line,
            source_file: self.source_file(),
            name,
            body,
            shorthand: false,
        })
    }

    fn parse_class_member(&mut self) -> Result<ClassMember> {
        match self.current_kind() {
            TokenKind::Keyword("static")
            | TokenKind::Keyword("async")
            | TokenKind::Keyword("method") => {
                Ok(ClassMember::Method(self.parse_method_declaration(true)?))
            }
            TokenKind::Keyword("let") | TokenKind::Keyword("const") => {
                Ok(ClassMember::Field(self.parse_field_declaration()?))
            }
            TokenKind::Keyword("class") => Ok(ClassMember::Class(self.parse_class_declaration()?)),
            TokenKind::Keyword("trait") => Ok(ClassMember::Trait(self.parse_trait_declaration()?)),
            _ => Err(self.error_current("Unsupported class member")),
        }
    }

    fn parse_trait_member(&mut self) -> Result<ClassMember> {
        match self.current_kind() {
            TokenKind::Keyword("async") | TokenKind::Keyword("method") => {
                Ok(ClassMember::Method(self.parse_method_declaration(false)?))
            }
            TokenKind::Keyword("class") => Ok(ClassMember::Class(self.parse_class_declaration()?)),
            TokenKind::Keyword("trait") => Ok(ClassMember::Trait(self.parse_trait_declaration()?)),
            _ => Err(self.error_current("Unsupported trait member")),
        }
    }

    fn parse_method_declaration(&mut self, allow_static: bool) -> Result<MethodDeclaration> {
        let line = self.current_line();
        let mut is_static = false;
        let mut is_async = false;
        while self.check_keyword("static") || self.check_keyword("async") {
            if self.match_keyword("static") {
                if !allow_static {
                    return Err(self.error_current("static methods are not allowed here"));
                }
                if is_static {
                    return Err(self.error_current("Duplicate static method modifier"));
                }
                is_static = true;
                continue;
            }
            if self.match_keyword("async") {
                if is_async {
                    return Err(self.error_current("Duplicate async method modifier"));
                }
                is_async = true;
            }
        }
        self.expect_keyword("method")?;
        let name = self.expect_name("Expected method name")?;
        let params = self.parse_parameter_list()?;
        let return_type = self.parse_optional_return_type()?;
        let body = self.parse_block_statement()?;
        Ok(MethodDeclaration {
            line,
            source_file: self.source_file(),
            name,
            params,
            return_type,
            body,
            is_static,
            is_async,
        })
    }

    fn parse_field_declaration(&mut self) -> Result<FieldDeclaration> {
        let line = self.current_line();
        let kind = self.expect_keyword_any(&["let", "const"])?;
        let (declared_type, name) = self.parse_typed_name()?;
        let mut accessors = Vec::new();
        if self.match_keyword("with") {
            loop {
                accessors.push(self.expect_name("Expected accessor name")?);
                if !self.match_punct(',') {
                    break;
                }
            }
        }
        let mut is_weak_storage = self.parse_optional_weak_modifier("field declaration")?;
        let default_value = if self.match_operator(":=") {
            let value = self.parse_expression()?;
            is_weak_storage |= self.parse_optional_weak_modifier("field declaration")?;
            Some(value)
        } else {
            None
        };
        Ok(FieldDeclaration {
            line,
            source_file: self.source_file(),
            kind,
            declared_type,
            name,
            accessors,
            default_value,
            is_weak_storage,
            runtime_typecheck_required: None,
        })
    }

    fn parse_import_declaration(&mut self) -> Result<ImportDeclaration> {
        let line = self.current_line();
        self.expect_keyword("from")?;
        let mut source = String::new();
        while !self.check_keyword("import") && !self.check_keyword("try") {
            source.push_str(&self.current_text());
            self.advance();
        }
        let try_mode = self.match_keyword("try");
        self.expect_keyword("import")?;
        let mut specifiers = Vec::new();
        let import_all = if self.match_operator("*") {
            true
        } else {
            loop {
                let spec_line = self.current_line();
                let imported = self.expect_identifier("Expected imported name")?;
                let local = if self.match_keyword("as") {
                    self.expect_identifier("Expected alias name after as")?
                } else {
                    imported.clone()
                };
                specifiers.push(ImportSpecifier {
                    line: spec_line,
                    source_file: self.source_file(),
                    imported,
                    local,
                });
                if !self.match_punct(',') {
                    break;
                }
            }
            false
        };
        let condition = if let Some(keyword) = self.match_postfix_condition_keyword() {
            Some(PostfixCondition {
                line: self.current_line(),
                source_file: self.source_file(),
                keyword,
                test: self.parse_expression()?,
            })
        } else {
            None
        };
        Ok(ImportDeclaration {
            line,
            source_file: self.source_file(),
            source,
            try_mode,
            import_all,
            specifiers,
            condition,
        })
    }

    fn parse_if_statement(&mut self) -> Result<IfStatement> {
        let line = self.current_line();
        self.expect_keyword("if")?;
        let test = self.parse_parenthesized_expression()?;
        let consequent = self.parse_block_statement()?;
        let alternate = if self.match_keyword("else") {
            if self.check_keyword("if") {
                Some(Box::new(Statement::IfStatement(self.parse_if_statement()?)))
            } else {
                Some(Box::new(Statement::Block(self.parse_block_statement()?)))
            }
        } else {
            None
        };
        Ok(IfStatement {
            line,
            source_file: self.source_file(),
            test,
            consequent,
            alternate,
        })
    }

    fn parse_while_statement(&mut self) -> Result<WhileStatement> {
        let line = self.current_line();
        self.expect_keyword("while")?;
        let test = self.parse_parenthesized_expression()?;
        let body = self.parse_block_statement()?;
        Ok(WhileStatement {
            line,
            source_file: self.source_file(),
            test,
            body,
        })
    }

    fn parse_for_statement(&mut self) -> Result<ForStatement> {
        let line = self.current_line();
        self.expect_keyword("for")?;
        self.expect_punct('(', "Expected '(' after for")?;
        let binding_kind = if self.match_keyword("let") {
            Some("let".to_owned())
        } else if self.match_keyword("const") {
            Some("const".to_owned())
        } else {
            None
        };
        let variable = self.expect_identifier("Expected loop variable")?;
        self.expect_keyword("in")?;
        let iterable = self.parse_expression()?;
        self.expect_punct(')', "Expected ')' after for header")?;
        let body = self.parse_block_statement()?;
        let else_block = if self.match_keyword("else") {
            Some(self.parse_block_statement()?)
        } else {
            None
        };
        Ok(ForStatement {
            line,
            source_file: self.source_file(),
            binding_kind,
            variable,
            iterable,
            body,
            else_block,
        })
    }

    fn parse_switch_statement(&mut self) -> Result<SwitchStatement> {
        let line = self.current_line();
        self.expect_keyword("switch")?;
        self.expect_punct('(', "Expected '(' after switch")?;
        let discriminant = self.parse_expression()?;
        let comparator = if self.match_operator(":") {
            Some(self.expect_comparator_text("Expected switch comparator operator")?)
        } else {
            None
        };
        self.expect_punct(')', "Expected ')' after switch header")?;
        self.expect_punct('{', "Expected '{' before switch body")?;
        let mut cases = Vec::new();
        let mut default = None;
        self.consume_semicolons();
        while !self.check_punct('}') {
            if self.match_keyword("case") {
                let case_line = self.previous_line();
                let mut values = vec![self.parse_expression()?];
                while self.match_punct(',') {
                    values.push(self.parse_expression()?);
                }
                self.expect_operator(":", "Expected ':' after case values")?;
                let consequent = self.parse_switch_consequent()?;
                cases.push(SwitchCase {
                    line: case_line,
                    source_file: self.source_file(),
                    values,
                    consequent,
                });
            } else if self.match_keyword("default") {
                self.expect_operator(":", "Expected ':' after default")?;
                default = Some(self.parse_switch_consequent()?);
            } else {
                return Err(self.error_current("Expected case or default in switch"));
            }
            self.consume_semicolons();
        }
        self.expect_punct('}', "Expected '}' after switch")?;
        Ok(SwitchStatement {
            line,
            source_file: self.source_file(),
            discriminant,
            comparator,
            cases,
            default,
            index: None,
        })
    }

    fn parse_switch_consequent(&mut self) -> Result<Vec<Statement>> {
        let mut statements = Vec::new();
        self.consume_semicolons();
        while !self.check_keyword("case")
            && !self.check_keyword("default")
            && !self.check_punct('}')
        {
            statements.push(self.parse_statement()?);
            self.consume_semicolons();
        }
        Ok(statements)
    }

    fn parse_try_statement(&mut self) -> Result<TryStatement> {
        let line = self.current_line();
        self.expect_keyword("try")?;
        let body = self.parse_block_statement()?;
        let mut handlers = Vec::new();
        while self.match_keyword("catch") {
            handlers.push(self.parse_catch_clause()?);
        }
        if handlers.is_empty() {
            return Err(self.error_current("Expected at least one catch block"));
        }
        Ok(TryStatement {
            line,
            source_file: self.source_file(),
            body,
            handlers,
        })
    }

    fn parse_catch_clause(&mut self) -> Result<CatchClause> {
        let line = self.previous_line();
        let binding = if self.match_punct('(') {
            let binding = if self.match_punct(')') {
                None
            } else {
                let first = self.expect_identifier("Expected catch binding")?;
                let binding = if self.check_identifier() {
                    let name = self.expect_identifier("Expected catch variable name")?;
                    CatchBinding {
                        line,
                        source_file: self.source_file(),
                        declared_type: Some(first),
                        name: Some(name),
                    }
                } else {
                    CatchBinding {
                        line,
                        source_file: self.source_file(),
                        declared_type: None,
                        name: Some(first),
                    }
                };
                self.expect_punct(')', "Expected ')' after catch binding")?;
                Some(binding)
            };
            binding
        } else {
            None
        };
        let body = self.parse_block_statement()?;
        Ok(CatchClause {
            line,
            source_file: self.source_file(),
            binding,
            body,
        })
    }

    fn parse_return_statement(&mut self) -> Result<ReturnStatement> {
        let line = self.current_line();
        self.expect_keyword("return")?;
        let argument = if self.statement_terminator_here() {
            None
        } else {
            Some(self.parse_expression()?)
        };
        Ok(ReturnStatement {
            line,
            source_file: self.source_file(),
            argument,
            runtime_typecheck_required: None,
        })
    }

    fn parse_loop_control_statement(&mut self) -> Result<LoopControlStatement> {
        let line = self.current_line();
        let keyword = self.expect_keyword_any(&["next", "continue", "last"])?;
        Ok(LoopControlStatement {
            line,
            source_file: self.source_file(),
            keyword,
        })
    }

    fn parse_throw_statement(&mut self) -> Result<ThrowStatement> {
        let line = self.current_line();
        self.expect_keyword("throw")?;
        let argument = self.parse_expression()?;
        Ok(ThrowStatement {
            line,
            source_file: self.source_file(),
            argument,
        })
    }

    fn parse_die_statement(&mut self) -> Result<DieStatement> {
        let line = self.current_line();
        self.expect_keyword("die")?;
        let argument = self.parse_expression()?;
        Ok(DieStatement {
            line,
            source_file: self.source_file(),
            argument,
        })
    }

    fn parse_keyword_statement(&mut self) -> Result<KeywordStatement> {
        let line = self.current_line();
        let keyword = self.expect_keyword_any(&["say", "print", "warn", "assert", "debug"])?;
        let mut arguments = Vec::new();
        if !self.statement_terminator_here() {
            arguments.push(self.parse_expression()?);
            while self.match_punct(',') {
                arguments.push(self.parse_expression()?);
            }
        }
        Ok(KeywordStatement {
            line,
            source_file: self.source_file(),
            keyword,
            arguments,
        })
    }

    fn parse_expression_statement(&mut self) -> Result<ExpressionStatement> {
        let expression = self.parse_expression()?;
        Ok(ExpressionStatement {
            line: expression.line(),
            source_file: expression.source_file().map(str::to_owned),
            expression,
        })
    }

    fn parse_expression(&mut self) -> Result<Expression> {
        self.parse_expression_prec(PREC_ASSIGNMENT)
    }

    fn parse_expression_prec(&mut self, min_prec: u8) -> Result<Expression> {
        let mut left = self.parse_prefix_expression()?;

        loop {
            if min_prec <= PREC_ASSIGNMENT {
                if let Some(operator) = self.current_assignment_operator() {
                    self.advance();
                    let right = self.parse_expression_prec(PREC_ASSIGNMENT)?;
                    let is_weak_write = self.parse_optional_weak_modifier("assignment")?;
                    if is_weak_write && operator != ":=" {
                        return Err(
                            self.error_current("but weak is only valid on ':=' assignments")
                        );
                    }
                    if is_weak_write && Self::is_maybe_path_expression(&left) {
                        return Err(
                            self.error_current("but weak is not valid on @? path assignments")
                        );
                    }
                    left = Expression::Assignment {
                        line: left.line(),
                        source_file: left.source_file().map(str::to_owned),
                        operator,
                        left: Box::new(left),
                        right: Box::new(right),
                        is_weak_write,
                        inferred_type: None,
                        runtime_typecheck_required: None,
                    };
                    continue;
                }
            }

            if min_prec <= PREC_TERNARY && self.match_operator("?") {
                let consequent = self.parse_expression()?;
                self.expect_operator(":", "Expected ':' in ternary expression")?;
                let alternate = self.parse_expression_prec(PREC_TERNARY)?;
                left = Expression::Ternary {
                    line: left.line(),
                    source_file: left.source_file().map(str::to_owned),
                    test: Box::new(left),
                    consequent: Box::new(consequent),
                    alternate: Box::new(alternate),
                    inferred_type: None,
                };
                continue;
            }

            if min_prec <= PREC_TERNARY && self.match_operator("?:") {
                let right = self.parse_expression_prec(PREC_TERNARY)?;
                left = Expression::DefinedOr {
                    line: left.line(),
                    source_file: left.source_file().map(str::to_owned),
                    left: Box::new(left),
                    right: Box::new(right),
                    inferred_type: None,
                };
                continue;
            }

            let Some((operator, precedence, right_assoc)) = self.current_infix_operator() else {
                break;
            };
            if precedence < min_prec {
                break;
            }
            self.advance();
            let next_prec = if right_assoc {
                precedence
            } else {
                precedence + 1
            };
            let right = self.parse_expression_prec(next_prec)?;
            left = Expression::Binary {
                line: left.line(),
                source_file: left.source_file().map(str::to_owned),
                operator,
                left: Box::new(left),
                right: Box::new(right),
                inferred_type: None,
            };
        }

        Ok(left)
    }

    fn parse_prefix_expression(&mut self) -> Result<Expression> {
        match self.current_kind() {
            TokenKind::Keyword("let") | TokenKind::Keyword("const") => self.parse_let_expression(),
            TokenKind::Keyword("try") => self.parse_try_expression(),
            TokenKind::Keyword("do") => self.parse_do_expression(),
            TokenKind::Keyword("await") => self.parse_await_expression(),
            TokenKind::Keyword("spawn") => self.parse_spawn_expression(),
            TokenKind::Keyword("fn") => self.parse_lambda_expression(),
            TokenKind::Keyword("async") => self.parse_async_expression(),
            TokenKind::Operator(op)
                if ["+", "-", "!", "~", "++", "--", "¬", "√", "\\"].contains(&op.as_str()) =>
            {
                let operator = op.clone();
                self.advance();
                let argument = self.parse_expression_prec(PREC_PREFIX)?;
                Ok(Expression::Unary {
                    line: self.previous_line(),
                    source_file: self.source_file(),
                    operator,
                    argument: Box::new(argument),
                    inferred_type: None,
                })
            }
            TokenKind::Keyword("not")
            | TokenKind::Keyword("new")
            | TokenKind::Keyword("abs")
            | TokenKind::Keyword("sqrt")
            | TokenKind::Keyword("floor")
            | TokenKind::Keyword("ceil")
            | TokenKind::Keyword("round")
            | TokenKind::Keyword("int")
            | TokenKind::Keyword("uc")
            | TokenKind::Keyword("lc")
            | TokenKind::Keyword("length")
            | TokenKind::Keyword("typeof") => {
                let operator = self.current_text();
                self.advance();
                let argument = self.parse_expression_prec(PREC_PREFIX)?;
                Ok(Expression::Unary {
                    line: self.previous_line(),
                    source_file: self.source_file(),
                    operator,
                    argument: Box::new(argument),
                    inferred_type: None,
                })
            }
            _ => self.parse_postfix_expression(),
        }
    }

    fn parse_postfix_expression(&mut self) -> Result<Expression> {
        let mut expr = self.parse_primary_expression()?;
        loop {
            if self.match_punct('(') {
                let arguments = self.parse_call_arguments_after_open()?;
                expr = Expression::Call {
                    line: expr.line(),
                    source_file: expr.source_file().map(str::to_owned),
                    callee: Box::new(expr),
                    arguments,
                    inferred_type: None,
                };
                continue;
            }
            if self.match_operator(".(") {
                let member = self.parse_expression()?;
                self.expect_punct(')', "Expected ')' after dynamic member expression")?;
                self.expect_punct('(', "Expected '(' after dynamic member")?;
                let arguments = self.parse_call_arguments_after_open()?;
                expr = Expression::DynamicMemberCall {
                    line: expr.line(),
                    source_file: expr.source_file().map(str::to_owned),
                    object: Box::new(expr),
                    member: Box::new(member),
                    arguments,
                    inferred_type: None,
                };
                continue;
            }
            if self.match_operator(".") {
                let member = self.expect_name("Expected member name after '.'")?;
                expr = Expression::MemberAccess {
                    line: expr.line(),
                    source_file: expr.source_file().map(str::to_owned),
                    object: Box::new(expr),
                    member,
                    inferred_type: None,
                };
                continue;
            }
            if self.match_punct('[') {
                if self.match_operator(":") {
                    let end = if self.check_punct(']') {
                        None
                    } else {
                        Some(Box::new(self.parse_expression()?))
                    };
                    self.expect_punct(']', "Expected ']' after slice")?;
                    expr = Expression::Slice {
                        line: expr.line(),
                        source_file: expr.source_file().map(str::to_owned),
                        object: Box::new(expr),
                        start: None,
                        end,
                        inferred_type: None,
                    };
                    continue;
                }
                let first = self.parse_expression()?;
                if self.match_operator(":") {
                    let end = if self.check_punct(']') {
                        None
                    } else {
                        Some(Box::new(self.parse_expression()?))
                    };
                    self.expect_punct(']', "Expected ']' after slice")?;
                    expr = Expression::Slice {
                        line: expr.line(),
                        source_file: expr.source_file().map(str::to_owned),
                        object: Box::new(expr),
                        start: Some(Box::new(first)),
                        end,
                        inferred_type: None,
                    };
                    continue;
                }
                self.expect_punct(']', "Expected ']' after index")?;
                expr = Expression::Index {
                    line: expr.line(),
                    source_file: expr.source_file().map(str::to_owned),
                    object: Box::new(expr),
                    index: Box::new(first),
                    inferred_type: None,
                };
                continue;
            }
            if self.match_punct('{') {
                let key = self.parse_dict_key_until_rbrace()?;
                self.expect_punct('}', "Expected '}' after dict access")?;
                expr = Expression::DictAccess {
                    line: expr.line(),
                    source_file: expr.source_file().map(str::to_owned),
                    object: Box::new(expr),
                    key: Box::new(key),
                    inferred_type: None,
                };
                continue;
            }
            if self.match_operator("++") || self.match_operator("--") {
                let operator = self.previous_text();
                expr = Expression::PostfixUpdate {
                    line: expr.line(),
                    source_file: expr.source_file().map(str::to_owned),
                    operator,
                    argument: Box::new(expr),
                    inferred_type: None,
                };
                continue;
            }
            break;
        }
        Ok(expr)
    }

    fn parse_primary_expression(&mut self) -> Result<Expression> {
        match self.current_kind() {
            TokenKind::Identifier(name) => {
                let value = name.clone();
                self.advance();
                Ok(Expression::Identifier {
                    line: self.previous_line(),
                    source_file: self.source_file(),
                    name: value,
                    inferred_type: None,
                    binding_depth: None,
                })
            }
            TokenKind::Number(value) => {
                let value = value.clone();
                self.advance();
                Ok(Expression::NumberLiteral {
                    line: self.previous_line(),
                    source_file: self.source_file(),
                    value,
                    inferred_type: None,
                })
            }
            TokenKind::String(value) => {
                let value = value.clone();
                self.advance();
                Ok(Expression::StringLiteral {
                    line: self.previous_line(),
                    source_file: self.source_file(),
                    value,
                    inferred_type: None,
                })
            }
            TokenKind::Regex { pattern, flags } => {
                let pattern = pattern.clone();
                let flags = flags.clone();
                self.advance();
                Ok(Expression::RegexLiteral {
                    line: self.previous_line(),
                    source_file: self.source_file(),
                    pattern,
                    flags,
                    cache_key: None,
                    inferred_type: None,
                })
            }
            TokenKind::Template(parts) => {
                let line = self.current_line();
                let parts = parts.clone();
                self.advance();
                let mut ast_parts = Vec::new();
                for part in parts {
                    match part {
                        TokenTemplatePart::Text { line, value } => {
                            ast_parts.push(AstTemplatePart::Text {
                                line,
                                source_file: self.source_file(),
                                value,
                            })
                        }
                        TokenTemplatePart::Expr { line, source } => {
                            let padded_source =
                                format!("{}{}", "\n".repeat(line.saturating_sub(1)), source,);
                            let expression = crate::parse_expression_with_source_file(
                                &padded_source,
                                self.source_file.as_deref(),
                            )
                            .map_err(|_| {
                                ZuzuRustError::semantic("invalid template interpolation", line)
                            })?;
                            ast_parts.push(AstTemplatePart::Expression {
                                line: expression.line(),
                                source_file: expression.source_file().map(str::to_owned),
                                expression: Box::new(expression),
                            });
                        }
                    }
                }
                Ok(Expression::TemplateLiteral {
                    line,
                    source_file: self.source_file(),
                    parts: ast_parts,
                    inferred_type: None,
                })
            }
            TokenKind::Keyword("true") => {
                self.advance();
                Ok(Expression::BooleanLiteral {
                    line: self.previous_line(),
                    source_file: self.source_file(),
                    value: true,
                    inferred_type: None,
                })
            }
            TokenKind::Keyword("false") => {
                self.advance();
                Ok(Expression::BooleanLiteral {
                    line: self.previous_line(),
                    source_file: self.source_file(),
                    value: false,
                    inferred_type: None,
                })
            }
            TokenKind::Keyword("null") => {
                self.advance();
                Ok(Expression::NullLiteral {
                    line: self.previous_line(),
                    source_file: self.source_file(),
                    inferred_type: None,
                })
            }
            TokenKind::Keyword("super") => self.parse_super_call_expression(),
            TokenKind::Keyword("function") => self.parse_function_expression(),
            TokenKind::Punct('(') => {
                self.advance();
                let expr = self.parse_expression()?;
                self.expect_punct(')', "Expected ')' after expression")?;
                Ok(expr)
            }
            TokenKind::Punct('[') => self.parse_array_literal(),
            TokenKind::Punct('{') if self.peek_punct('{') => self.parse_pairlist_literal(),
            TokenKind::Punct('{') => self.parse_dict_literal(),
            TokenKind::Operator(op) if op == "<<" || op == "«" => self.parse_set_literal(),
            TokenKind::Operator(op) if op == "<<<" => self.parse_bag_literal(),
            TokenKind::Operator(op) if op == "⌊" => self.parse_grouped_unary("floor", "⌋"),
            TokenKind::Operator(op) if op == "⌈" => self.parse_grouped_unary("ceil", "⌉"),
            _ => Err(self.error_current("Expected expression")),
        }
    }

    fn parse_super_call_expression(&mut self) -> Result<Expression> {
        let line = self.current_line();
        self.expect_keyword("super")?;
        self.expect_punct('(', "Expected '(' after super")?;
        let arguments = self.parse_call_arguments_after_open()?;
        Ok(Expression::SuperCall {
            line,
            source_file: self.source_file(),
            arguments,
            inferred_type: None,
        })
    }

    fn parse_array_literal(&mut self) -> Result<Expression> {
        let line = self.current_line();
        self.expect_punct('[', "Expected '['")?;
        let mut elements = Vec::new();
        self.consume_commas();
        if !self.check_punct(']') {
            loop {
                elements.push(self.parse_expression()?);
                self.consume_commas();
                if self.check_punct(']') {
                    break;
                }
                if !self.previous_was_comma() {
                    break;
                }
            }
        }
        self.expect_punct(']', "Expected ']' after array literal")?;
        Ok(Expression::ArrayLiteral {
            line,
            source_file: self.source_file(),
            elements,
            capacity_hint: None,
            inferred_type: None,
        })
    }

    fn parse_set_literal(&mut self) -> Result<Expression> {
        let line = self.current_line();
        let close = if self.match_operator("<<") {
            ">>"
        } else {
            self.expect_operator("«", "Expected set literal start")?;
            "»"
        };
        let elements = self.parse_expression_list_until_operator(close)?;
        Ok(Expression::SetLiteral {
            line,
            source_file: self.source_file(),
            elements,
            capacity_hint: None,
            inferred_type: None,
        })
    }

    fn parse_bag_literal(&mut self) -> Result<Expression> {
        let line = self.current_line();
        self.expect_operator("<<<", "Expected bag literal start")?;
        let elements = self.parse_expression_list_until_operator(">>>")?;
        Ok(Expression::BagLiteral {
            line,
            source_file: self.source_file(),
            elements,
            capacity_hint: None,
            inferred_type: None,
        })
    }

    fn parse_dict_literal(&mut self) -> Result<Expression> {
        let line = self.current_line();
        self.expect_punct('{', "Expected '{'")?;
        let mut entries = Vec::new();
        self.consume_commas();
        if !self.check_punct('}') {
            loop {
                let key = self.parse_dict_key_before_colon()?;
                self.expect_operator(":", "Expected ':' after dict key")?;
                let value = self.parse_expression()?;
                entries.push(DictEntry {
                    line: key.line(),
                    source_file: key.source_file().map(str::to_owned),
                    key,
                    value,
                });
                self.consume_commas();
                if self.check_punct('}') {
                    break;
                }
                if !self.previous_was_comma() {
                    break;
                }
            }
        }
        self.expect_punct('}', "Expected '}' after dict literal")?;
        Ok(Expression::DictLiteral {
            line,
            source_file: self.source_file(),
            entries,
            capacity_hint: None,
            inferred_type: None,
        })
    }

    fn parse_pairlist_literal(&mut self) -> Result<Expression> {
        let line = self.current_line();
        self.expect_punct('{', "Expected '{'")?;
        self.expect_punct('{', "Expected second '{' in pairlist literal")?;
        let mut entries = Vec::new();
        self.consume_commas();
        if !(self.check_punct('}') && self.peek_punct('}')) {
            loop {
                let key = self.parse_dict_key_before_colon()?;
                self.expect_operator(":", "Expected ':' after pairlist key")?;
                let value = self.parse_expression()?;
                entries.push(DictEntry {
                    line: key.line(),
                    source_file: key.source_file().map(str::to_owned),
                    key,
                    value,
                });
                self.consume_commas();
                if self.check_punct('}') && self.peek_punct('}') {
                    break;
                }
                if !self.previous_was_comma() {
                    break;
                }
            }
        }
        self.expect_punct('}', "Expected '}' after pairlist literal")?;
        self.expect_punct('}', "Expected second '}' after pairlist literal")?;
        Ok(Expression::PairListLiteral {
            line,
            source_file: self.source_file(),
            entries,
            capacity_hint: None,
            inferred_type: None,
        })
    }

    fn parse_let_expression(&mut self) -> Result<Expression> {
        let line = self.current_line();
        let kind = self.expect_keyword_any(&["let", "const"])?;
        let (declared_type, name) = self.parse_typed_name()?;
        let mut is_weak_storage = self.parse_optional_weak_modifier("declaration")?;
        let init = if self.match_operator(":=") {
            let init = self.parse_expression()?;
            is_weak_storage |= self.parse_optional_weak_modifier("declaration")?;
            Some(Box::new(init))
        } else {
            None
        };
        Ok(Expression::LetExpression {
            line,
            source_file: self.source_file(),
            kind,
            declared_type,
            name,
            init,
            is_weak_storage,
            inferred_type: None,
            runtime_typecheck_required: None,
        })
    }

    fn parse_try_expression(&mut self) -> Result<Expression> {
        let line = self.current_line();
        self.expect_keyword("try")?;
        let body = self.parse_block_statement()?;
        let mut handlers = Vec::new();
        while self.match_keyword("catch") {
            handlers.push(self.parse_catch_clause()?);
        }
        if handlers.is_empty() {
            return Err(self.error_current("Expected at least one catch block"));
        }
        Ok(Expression::TryExpression {
            line,
            source_file: self.source_file(),
            body,
            handlers,
            inferred_type: None,
        })
    }

    fn parse_do_expression(&mut self) -> Result<Expression> {
        let line = self.current_line();
        self.expect_keyword("do")?;
        let body = self.parse_block_statement()?;
        Ok(Expression::DoExpression {
            line,
            source_file: self.source_file(),
            body,
            inferred_type: None,
        })
    }

    fn parse_await_expression(&mut self) -> Result<Expression> {
        let line = self.current_line();
        self.expect_keyword("await")?;
        let body = self.parse_block_statement()?;
        Ok(Expression::AwaitExpression {
            line,
            source_file: self.source_file(),
            body,
            inferred_type: None,
        })
    }

    fn parse_spawn_expression(&mut self) -> Result<Expression> {
        let line = self.current_line();
        self.expect_keyword("spawn")?;
        let body = self.parse_block_statement()?;
        Ok(Expression::SpawnExpression {
            line,
            source_file: self.source_file(),
            body,
            inferred_type: None,
        })
    }

    fn parse_async_expression(&mut self) -> Result<Expression> {
        self.expect_keyword("async")?;
        match self.current_kind() {
            TokenKind::Keyword("fn") => self.parse_lambda_expression_with_async(true),
            TokenKind::Keyword("function") => self.parse_function_expression_with_async(true),
            _ => Err(self.error_current("Expected function or fn after async")),
        }
    }

    fn parse_lambda_expression(&mut self) -> Result<Expression> {
        self.parse_lambda_expression_with_async(false)
    }

    fn parse_lambda_expression_with_async(&mut self, is_async: bool) -> Result<Expression> {
        let line = self.current_line();
        self.expect_keyword("fn")?;
        let params = if self.check_punct('(') {
            self.parse_parameter_list()?
        } else {
            vec![self.parse_parameter()?]
        };
        self.expect_operator("->", "Expected '->' after lambda parameters")?;
        let body = self.parse_expression()?;
        Ok(Expression::Lambda {
            line,
            source_file: self.source_file(),
            params,
            body: Box::new(body),
            is_async,
            inferred_type: None,
        })
    }

    fn parse_function_expression(&mut self) -> Result<Expression> {
        self.parse_function_expression_with_async(false)
    }

    fn parse_function_expression_with_async(&mut self, is_async: bool) -> Result<Expression> {
        let line = self.current_line();
        self.expect_keyword("function")?;
        let params = self.parse_parameter_list()?;
        let return_type = self.parse_optional_return_type()?;
        let body = self.parse_block_statement()?;
        Ok(Expression::FunctionExpression {
            line,
            source_file: self.source_file(),
            params,
            return_type,
            body,
            is_async,
            inferred_type: None,
        })
    }

    fn parse_parameter_list(&mut self) -> Result<Vec<Parameter>> {
        self.expect_punct('(', "Expected '('")?;
        let mut params = Vec::new();
        if !self.check_punct(')') {
            loop {
                if self.match_punct(',') {
                    continue;
                }
                params.push(self.parse_parameter()?);
                if self.check_operator("...") {
                    params.push(self.parse_parameter()?);
                }
                if !self.match_punct(',') {
                    break;
                }
                if self.check_punct(')') {
                    break;
                }
            }
        }
        self.expect_punct(')', "Expected ')' after parameter list")?;
        Ok(params)
    }

    fn parse_parameter(&mut self) -> Result<Parameter> {
        let line = self.current_line();
        let variadic = self.match_operator("...");
        let (declared_type, name) = self.parse_typed_name()?;
        let optional = self.match_operator("?");
        let default_value = if self.match_operator(":=") {
            Some(self.parse_expression()?)
        } else {
            None
        };
        Ok(Parameter {
            line,
            source_file: self.source_file(),
            declared_type,
            name,
            optional,
            variadic,
            default_value,
        })
    }

    fn parse_optional_return_type(&mut self) -> Result<Option<String>> {
        if self.match_operator("->") {
            Ok(Some(
                self.expect_identifier("Expected return type after '->'")?,
            ))
        } else {
            Ok(None)
        }
    }

    fn parse_parenthesized_expression(&mut self) -> Result<Expression> {
        self.expect_punct('(', "Expected '('")?;
        let expr = self.parse_expression()?;
        self.expect_punct(')', "Expected ')' after expression")?;
        Ok(expr)
    }

    fn parse_call_arguments_after_open(&mut self) -> Result<Vec<CallArgument>> {
        let mut arguments = Vec::new();
        if !self.check_punct(')') {
            loop {
                if self.match_punct(',') {
                    continue;
                }
                arguments.push(self.parse_call_argument()?);
                if self.match_punct(',') {
                    if self.check_punct(')') {
                        break;
                    }
                    continue;
                }
                if self.starts_named_argument() {
                    continue;
                }
                break;
            }
        }
        self.expect_punct(')', "Expected ')' after arguments")?;
        Ok(arguments)
    }

    fn parse_call_argument(&mut self) -> Result<CallArgument> {
        let expr = self.parse_expression()?;
        let line = expr.line();
        if self.match_operator(":") {
            let value = self.parse_expression()?;
            let key = match expr {
                Expression::Identifier {
                    line,
                    source_file,
                    name,
                    ..
                } => DictKey::Identifier {
                    line,
                    source_file,
                    name,
                },
                Expression::StringLiteral {
                    line,
                    source_file,
                    value,
                    ..
                } => DictKey::StringLiteral {
                    line,
                    source_file,
                    value,
                },
                other => DictKey::Expression {
                    line: other.line(),
                    source_file: other.source_file().map(str::to_owned),
                    expression: Box::new(other),
                },
            };
            Ok(CallArgument::Named {
                line,
                source_file: key.source_file().map(str::to_owned),
                name: key,
                value,
            })
        } else {
            Ok(CallArgument::Positional {
                line,
                source_file: expr.source_file().map(str::to_owned),
                value: expr,
            })
        }
    }

    fn parse_typed_name(&mut self) -> Result<(Option<String>, String)> {
        let first = self.expect_name("Expected name")?;
        if self.check_identifier() {
            let second = self.expect_name("Expected identifier")?;
            Ok((Some(first), second))
        } else {
            Ok((None, first))
        }
    }

    fn parse_trait_composition_list(&mut self) -> Result<Vec<String>> {
        let mut traits = Vec::new();
        if self.match_keyword("with") || self.match_keyword("but") {
            loop {
                traits.push(self.expect_identifier("Expected trait name")?);
                if !self.match_punct(',') {
                    break;
                }
            }
        }
        Ok(traits)
    }

    fn parse_dict_key_before_colon(&mut self) -> Result<DictKey> {
        match self.current_kind() {
            TokenKind::Identifier(name) => {
                let name = name.clone();
                self.advance();
                Ok(DictKey::Identifier {
                    line: self.previous_line(),
                    source_file: self.source_file(),
                    name,
                })
            }
            TokenKind::Keyword(value) => {
                let name = (*value).to_owned();
                self.advance();
                Ok(DictKey::Identifier {
                    line: self.previous_line(),
                    source_file: self.source_file(),
                    name,
                })
            }
            TokenKind::String(value) => {
                let value = value.clone();
                self.advance();
                Ok(DictKey::StringLiteral {
                    line: self.previous_line(),
                    source_file: self.source_file(),
                    value,
                })
            }
            TokenKind::Punct('(') => {
                let line = self.current_line();
                self.advance();
                let expr = self.parse_expression()?;
                self.expect_punct(')', "Expected ')' after dict key expression")?;
                Ok(DictKey::Expression {
                    line,
                    source_file: self.source_file(),
                    expression: Box::new(expr),
                })
            }
            _ => Err(self.error_current("Expected dict key")),
        }
    }

    fn parse_dict_key_until_rbrace(&mut self) -> Result<DictKey> {
        match self.current_kind() {
            TokenKind::Identifier(name) => {
                if self
                    .tokens
                    .get(self.index + 1)
                    .map(|token| matches!(token.kind, TokenKind::Punct('}')))
                    .unwrap_or(false)
                {
                    let name = name.clone();
                    self.advance();
                    Ok(DictKey::Identifier {
                        line: self.previous_line(),
                        source_file: self.source_file(),
                        name,
                    })
                } else {
                    let line = self.current_line();
                    let expr = self.parse_expression()?;
                    Ok(DictKey::Expression {
                        line,
                        source_file: self.source_file(),
                        expression: Box::new(expr),
                    })
                }
            }
            TokenKind::Keyword(value) => {
                if self
                    .tokens
                    .get(self.index + 1)
                    .map(|token| matches!(token.kind, TokenKind::Punct('}')))
                    .unwrap_or(false)
                {
                    let name = (*value).to_owned();
                    self.advance();
                    Ok(DictKey::Identifier {
                        line: self.previous_line(),
                        source_file: self.source_file(),
                        name,
                    })
                } else {
                    let line = self.current_line();
                    let expr = self.parse_expression()?;
                    Ok(DictKey::Expression {
                        line,
                        source_file: self.source_file(),
                        expression: Box::new(expr),
                    })
                }
            }
            TokenKind::String(value) => {
                if self
                    .tokens
                    .get(self.index + 1)
                    .map(|token| matches!(token.kind, TokenKind::Punct('}')))
                    .unwrap_or(false)
                {
                    let value = value.clone();
                    self.advance();
                    Ok(DictKey::StringLiteral {
                        line: self.previous_line(),
                        source_file: self.source_file(),
                        value,
                    })
                } else {
                    let line = self.current_line();
                    let expr = self.parse_expression()?;
                    Ok(DictKey::Expression {
                        line,
                        source_file: self.source_file(),
                        expression: Box::new(expr),
                    })
                }
            }
            TokenKind::Punct('(') => {
                let line = self.current_line();
                self.advance();
                let expr = self.parse_expression()?;
                self.expect_punct(')', "Expected ')' after dict key expression")?;
                Ok(DictKey::Expression {
                    line,
                    source_file: self.source_file(),
                    expression: Box::new(expr),
                })
            }
            _ => Err(self.error_current("Expected dict access key")),
        }
    }

    fn parse_expression_list_until_operator(&mut self, close: &str) -> Result<Vec<Expression>> {
        let mut elements = Vec::new();
        if !self.check_operator(close) {
            loop {
                elements.push(self.parse_expression()?);
                if !self.match_punct(',') {
                    break;
                }
                if self.check_operator(close) {
                    break;
                }
            }
        }
        self.expect_operator(close, "Expected collection literal terminator")?;
        Ok(elements)
    }

    fn parse_grouped_unary(&mut self, operator: &str, close: &str) -> Result<Expression> {
        let line = self.current_line();
        self.expect_operator(&self.current_text(), "Expected grouped unary opener")?;
        let argument = self.parse_expression()?;
        self.expect_operator(close, "Expected grouped unary closer")?;
        Ok(Expression::Unary {
            line,
            source_file: self.source_file(),
            operator: operator.to_owned(),
            argument: Box::new(argument),
            inferred_type: None,
        })
    }

    fn current_assignment_operator(&self) -> Option<String> {
        match self.current_kind() {
            TokenKind::Operator(op)
                if [
                    ":=", "+=", "-=", "*=", "×=", "/=", "÷=", "_=", "~=", "**=", "?:=",
                ]
                .contains(&op.as_str()) =>
            {
                Some(op.clone())
            }
            _ => None,
        }
    }

    fn parse_optional_weak_modifier(&mut self, context: &str) -> Result<bool> {
        if !self.match_keyword("but") {
            return Ok(false);
        }
        let modifier = self.current_text();
        if modifier != "weak" {
            return Err(self.error_current(format!(
                "Unknown but modifier '{modifier}' in {context}; expected 'but weak'"
            )));
        }
        self.advance();
        Ok(true)
    }

    fn is_maybe_path_expression(expr: &Expression) -> bool {
        matches!(
            expr,
            Expression::Binary {
                operator,
                ..
            } if operator == "@?"
        )
    }

    fn current_infix_operator(&self) -> Option<(String, u8, bool)> {
        let text = self.current_text();
        let entry = match text.as_str() {
            "or" | "⋁" => Some((PREC_OR, false)),
            "xor" | "⊻" => Some((PREC_XOR, false)),
            "and" | "⋀" | "nand" | "⊼" => Some((PREC_AND, false)),
            "==" | "≡" | "!=" | "≢" => Some((PREC_EQUALITY, false)),
            "=" | "≠" | "<" | ">" | "<=" | "≤" | ">=" | "≥" | "<=>" | "≶" | "≷" | "eq" | "ne"
            | "gt" | "ge" | "lt" | "le" | "cmp" | "eqi" | "nei" | "gti" | "gei" | "lti" | "lei"
            | "cmpi" | "in" | "∈" | "∉" | "subsetof" | "⊂" | "supersetof" | "⊃"
            | "equivalentof" | "⊂⊃" | "instanceof" | "does" | "can" | "~" | "->" | "@" | "@?"
            | "@@" => Some((PREC_COMPARISON, false)),
            "|" => Some((PREC_BITWISE_OR, false)),
            "^" => Some((PREC_BITWISE_XOR, false)),
            "&" => Some((PREC_BITWISE_AND, false)),
            "union" | "⋃" | "intersection" | "⋂" | "\\" | "∖" => Some((PREC_SET, false)),
            "..." => Some((PREC_SET, false)),
            "_" => Some((PREC_CONCAT, false)),
            "+" | "-" => Some((PREC_ADDITIVE, false)),
            "*" | "/" | "×" | "÷" | "mod" => Some((PREC_MULTIPLICATIVE, false)),
            "**" => Some((PREC_EXPONENT, true)),
            _ => None,
        }?;
        Some((text, entry.0, entry.1))
    }

    fn match_postfix_condition_keyword(&mut self) -> Option<String> {
        if self.match_keyword("if") {
            Some("if".to_owned())
        } else if self.match_keyword("unless") {
            Some("unless".to_owned())
        } else {
            None
        }
    }

    fn statement_terminator_here(&self) -> bool {
        matches!(
            self.current_kind(),
            TokenKind::Punct(';') | TokenKind::Punct('}') | TokenKind::Eof
        )
    }

    fn current_kind(&self) -> &TokenKind {
        &self.tokens[self.index].kind
    }

    fn current_line(&self) -> usize {
        self.tokens[self.index].span.line
    }

    fn previous_line(&self) -> usize {
        if self.index == 0 {
            self.tokens[0].span.line
        } else {
            self.tokens[self.index - 1].span.line
        }
    }

    fn current_text(&self) -> String {
        token_text(self.current_kind())
    }

    fn previous_text(&self) -> String {
        token_text(&self.tokens[self.index - 1].kind)
    }

    fn at_eof(&self) -> bool {
        matches!(self.current_kind(), TokenKind::Eof)
    }

    fn advance(&mut self) {
        if !self.at_eof() {
            self.index += 1;
        }
    }

    fn consume_semicolons(&mut self) {
        while self.match_punct(';') {}
    }

    fn consume_commas(&mut self) {
        while self.match_punct(',') {}
    }

    fn previous_was_comma(&self) -> bool {
        self.index > 0 && matches!(self.tokens[self.index - 1].kind, TokenKind::Punct(','))
    }

    fn check_identifier(&self) -> bool {
        matches!(self.current_kind(), TokenKind::Identifier(_))
    }

    fn starts_named_argument(&self) -> bool {
        let Some(next) = self.tokens.get(self.index + 1) else {
            return false;
        };
        matches!(
            self.current_kind(),
            TokenKind::Identifier(_)
                | TokenKind::Keyword(_)
                | TokenKind::String(_)
                | TokenKind::Punct('(')
        ) && matches!(&next.kind, TokenKind::Operator(value) if value == ":")
    }

    #[allow(dead_code)]
    fn check_name(&self) -> bool {
        matches!(
            self.current_kind(),
            TokenKind::Identifier(_) | TokenKind::Keyword(_)
        )
    }

    fn check_keyword(&self, keyword: &str) -> bool {
        matches!(self.current_kind(), TokenKind::Keyword(value) if *value == keyword)
    }

    fn check_punct(&self, punct: char) -> bool {
        matches!(self.current_kind(), TokenKind::Punct(value) if *value == punct)
    }

    fn peek_punct(&self, punct: char) -> bool {
        matches!(
            self.tokens.get(self.index + 1).map(|token| &token.kind),
            Some(TokenKind::Punct(value)) if *value == punct
        )
    }

    fn check_operator(&self, operator: &str) -> bool {
        matches!(self.current_kind(), TokenKind::Operator(value) if value == operator)
    }

    fn match_keyword(&mut self, keyword: &str) -> bool {
        if self.check_keyword(keyword) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn match_punct(&mut self, punct: char) -> bool {
        if self.check_punct(punct) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn match_operator(&mut self, operator: &str) -> bool {
        match self.current_kind() {
            TokenKind::Operator(value) if value == operator => {
                self.advance();
                true
            }
            _ => false,
        }
    }

    fn expect_keyword(&mut self, keyword: &str) -> Result<()> {
        if self.match_keyword(keyword) {
            Ok(())
        } else {
            Err(self.error_current(format!("Expected keyword '{}'", keyword)))
        }
    }

    fn expect_keyword_any(&mut self, keywords: &[&str]) -> Result<String> {
        for keyword in keywords {
            if self.match_keyword(keyword) {
                return Ok((*keyword).to_owned());
            }
        }
        Err(self.error_current("Expected keyword"))
    }

    fn expect_identifier(&mut self, message: &str) -> Result<String> {
        match self.current_kind() {
            TokenKind::Identifier(value) => {
                let value = value.clone();
                self.advance();
                Ok(value)
            }
            _ => Err(self.error_current(message)),
        }
    }

    fn expect_name(&mut self, message: &str) -> Result<String> {
        match self.current_kind() {
            TokenKind::Identifier(value) => {
                let value = value.clone();
                self.advance();
                Ok(value)
            }
            TokenKind::Keyword(value) => {
                let value = (*value).to_owned();
                self.advance();
                Ok(value)
            }
            _ => Err(self.error_current(message)),
        }
    }

    fn expect_punct(&mut self, punct: char, message: &str) -> Result<()> {
        if self.match_punct(punct) {
            Ok(())
        } else {
            Err(self.error_current(message))
        }
    }

    fn expect_operator(&mut self, operator: &str, message: &str) -> Result<()> {
        if self.match_operator(operator) {
            Ok(())
        } else {
            Err(self.error_current(message))
        }
    }

    fn expect_comparator_text(&mut self, message: &str) -> Result<String> {
        match self.current_kind() {
            TokenKind::Operator(value) => {
                let value = value.clone();
                self.advance();
                Ok(value)
            }
            TokenKind::Keyword(value)
                if [
                    "eq",
                    "ne",
                    "gt",
                    "ge",
                    "lt",
                    "le",
                    "cmp",
                    "eqi",
                    "nei",
                    "gti",
                    "gei",
                    "lti",
                    "lei",
                    "cmpi",
                    "instanceof",
                    "does",
                    "can",
                    "and",
                    "or",
                    "xor",
                    "nand",
                    "mod",
                    "union",
                    "intersection",
                ]
                .contains(value) =>
            {
                let value = (*value).to_owned();
                self.advance();
                Ok(value)
            }
            _ => Err(self.error_current(message)),
        }
    }

    fn error_current(&self, message: impl Into<String>) -> ZuzuRustError {
        let token = &self.tokens[self.index];
        match token.kind {
            TokenKind::Eof => ZuzuRustError::incomplete_parse(message, token.span),
            _ => ZuzuRustError::parse(message, token.span),
        }
    }
}

fn token_text(kind: &TokenKind) -> String {
    match kind {
        TokenKind::Keyword(value) => (*value).to_owned(),
        TokenKind::Identifier(value) => value.clone(),
        TokenKind::Number(value) => value.clone(),
        TokenKind::String(value) => value.clone(),
        TokenKind::Regex { pattern, flags } => format!("/{pattern}/{flags}"),
        TokenKind::Template(_) => "<template>".to_owned(),
        TokenKind::Operator(value) => value.clone(),
        TokenKind::Punct(value) => value.to_string(),
        TokenKind::Eof => "<eof>".to_owned(),
    }
}

fn statement_supports_postfix_condition(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::ExpressionStatement(_)
            | Statement::ReturnStatement(_)
            | Statement::LoopControlStatement(_)
            | Statement::ThrowStatement(_)
            | Statement::DieStatement(_)
            | Statement::KeywordStatement(_)
    )
}
