#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Program {
    pub line: usize,
    pub source_file: Option<String>,
    pub statements: Vec<Statement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    Block(BlockStatement),
    VariableDeclaration(VariableDeclaration),
    FunctionDeclaration(FunctionDeclaration),
    ClassDeclaration(ClassDeclaration),
    TraitDeclaration(TraitDeclaration),
    ImportDeclaration(ImportDeclaration),
    IfStatement(IfStatement),
    WhileStatement(WhileStatement),
    ForStatement(ForStatement),
    SwitchStatement(SwitchStatement),
    TryStatement(TryStatement),
    ReturnStatement(ReturnStatement),
    LoopControlStatement(LoopControlStatement),
    ThrowStatement(ThrowStatement),
    DieStatement(DieStatement),
    PostfixConditionalStatement(PostfixConditionalStatement),
    KeywordStatement(KeywordStatement),
    ExpressionStatement(ExpressionStatement),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockStatement {
    pub line: usize,
    pub source_file: Option<String>,
    pub statements: Vec<Statement>,
    pub needs_lexical_scope: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VariableDeclaration {
    pub line: usize,
    pub source_file: Option<String>,
    pub kind: String,
    pub declared_type: Option<String>,
    pub name: String,
    pub init: Option<Expression>,
    pub is_weak_storage: bool,
    pub runtime_typecheck_required: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionDeclaration {
    pub line: usize,
    pub source_file: Option<String>,
    pub name: String,
    pub params: Vec<Parameter>,
    pub return_type: Option<String>,
    pub body: BlockStatement,
    pub is_async: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassDeclaration {
    pub line: usize,
    pub source_file: Option<String>,
    pub name: String,
    pub base: Option<String>,
    pub traits: Vec<String>,
    pub body: Vec<ClassMember>,
    pub shorthand: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraitDeclaration {
    pub line: usize,
    pub source_file: Option<String>,
    pub name: String,
    pub body: Vec<ClassMember>,
    pub shorthand: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClassMember {
    Field(FieldDeclaration),
    Method(MethodDeclaration),
    Class(ClassDeclaration),
    Trait(TraitDeclaration),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDeclaration {
    pub line: usize,
    pub source_file: Option<String>,
    pub kind: String,
    pub declared_type: Option<String>,
    pub name: String,
    pub accessors: Vec<String>,
    pub default_value: Option<Expression>,
    pub is_weak_storage: bool,
    pub runtime_typecheck_required: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MethodDeclaration {
    pub line: usize,
    pub source_file: Option<String>,
    pub name: String,
    pub params: Vec<Parameter>,
    pub return_type: Option<String>,
    pub body: BlockStatement,
    pub is_static: bool,
    pub is_async: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parameter {
    pub line: usize,
    pub source_file: Option<String>,
    pub declared_type: Option<String>,
    pub name: String,
    pub optional: bool,
    pub variadic: bool,
    pub default_value: Option<Expression>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportDeclaration {
    pub line: usize,
    pub source_file: Option<String>,
    pub source: String,
    pub try_mode: bool,
    pub import_all: bool,
    pub specifiers: Vec<ImportSpecifier>,
    pub condition: Option<PostfixCondition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportSpecifier {
    pub line: usize,
    pub source_file: Option<String>,
    pub imported: String,
    pub local: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostfixCondition {
    pub line: usize,
    pub source_file: Option<String>,
    pub keyword: String,
    pub test: Expression,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IfStatement {
    pub line: usize,
    pub source_file: Option<String>,
    pub test: Expression,
    pub consequent: BlockStatement,
    pub alternate: Option<Box<Statement>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WhileStatement {
    pub line: usize,
    pub source_file: Option<String>,
    pub test: Expression,
    pub body: BlockStatement,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForStatement {
    pub line: usize,
    pub source_file: Option<String>,
    pub binding_kind: Option<String>,
    pub variable: String,
    pub iterable: Expression,
    pub body: BlockStatement,
    pub else_block: Option<BlockStatement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwitchStatement {
    pub line: usize,
    pub source_file: Option<String>,
    pub discriminant: Expression,
    pub comparator: Option<String>,
    pub cases: Vec<SwitchCase>,
    pub default: Option<Vec<Statement>>,
    pub index: Option<Vec<SwitchIndexEntry>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwitchIndexEntry {
    pub key: String,
    pub case_index: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwitchCase {
    pub line: usize,
    pub source_file: Option<String>,
    pub values: Vec<Expression>,
    pub consequent: Vec<Statement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TryStatement {
    pub line: usize,
    pub source_file: Option<String>,
    pub body: BlockStatement,
    pub handlers: Vec<CatchClause>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatchClause {
    pub line: usize,
    pub source_file: Option<String>,
    pub binding: Option<CatchBinding>,
    pub body: BlockStatement,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatchBinding {
    pub line: usize,
    pub source_file: Option<String>,
    pub declared_type: Option<String>,
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReturnStatement {
    pub line: usize,
    pub source_file: Option<String>,
    pub argument: Option<Expression>,
    pub runtime_typecheck_required: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoopControlStatement {
    pub line: usize,
    pub source_file: Option<String>,
    pub keyword: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThrowStatement {
    pub line: usize,
    pub source_file: Option<String>,
    pub argument: Expression,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DieStatement {
    pub line: usize,
    pub source_file: Option<String>,
    pub argument: Expression,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostfixConditionalStatement {
    pub line: usize,
    pub source_file: Option<String>,
    pub statement: Box<Statement>,
    pub keyword: String,
    pub test: Expression,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeywordStatement {
    pub line: usize,
    pub source_file: Option<String>,
    pub keyword: String,
    pub arguments: Vec<Expression>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpressionStatement {
    pub line: usize,
    pub source_file: Option<String>,
    pub expression: Expression,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expression {
    Identifier {
        line: usize,
        source_file: Option<String>,
        name: String,
        inferred_type: Option<String>,
        binding_depth: Option<usize>,
    },
    NumberLiteral {
        line: usize,
        source_file: Option<String>,
        value: String,
        inferred_type: Option<String>,
    },
    StringLiteral {
        line: usize,
        source_file: Option<String>,
        value: String,
        inferred_type: Option<String>,
    },
    RegexLiteral {
        line: usize,
        source_file: Option<String>,
        pattern: String,
        flags: String,
        cache_key: Option<String>,
        inferred_type: Option<String>,
    },
    BooleanLiteral {
        line: usize,
        source_file: Option<String>,
        value: bool,
        inferred_type: Option<String>,
    },
    NullLiteral {
        line: usize,
        source_file: Option<String>,
        inferred_type: Option<String>,
    },
    ArrayLiteral {
        line: usize,
        source_file: Option<String>,
        elements: Vec<Expression>,
        capacity_hint: Option<usize>,
        inferred_type: Option<String>,
    },
    SetLiteral {
        line: usize,
        source_file: Option<String>,
        elements: Vec<Expression>,
        capacity_hint: Option<usize>,
        inferred_type: Option<String>,
    },
    BagLiteral {
        line: usize,
        source_file: Option<String>,
        elements: Vec<Expression>,
        capacity_hint: Option<usize>,
        inferred_type: Option<String>,
    },
    DictLiteral {
        line: usize,
        source_file: Option<String>,
        entries: Vec<DictEntry>,
        capacity_hint: Option<usize>,
        inferred_type: Option<String>,
    },
    PairListLiteral {
        line: usize,
        source_file: Option<String>,
        entries: Vec<DictEntry>,
        capacity_hint: Option<usize>,
        inferred_type: Option<String>,
    },
    TemplateLiteral {
        line: usize,
        source_file: Option<String>,
        parts: Vec<TemplatePart>,
        inferred_type: Option<String>,
    },
    Unary {
        line: usize,
        source_file: Option<String>,
        operator: String,
        argument: Box<Expression>,
        inferred_type: Option<String>,
    },
    Binary {
        line: usize,
        source_file: Option<String>,
        operator: String,
        left: Box<Expression>,
        right: Box<Expression>,
        inferred_type: Option<String>,
    },
    Ternary {
        line: usize,
        source_file: Option<String>,
        test: Box<Expression>,
        consequent: Box<Expression>,
        alternate: Box<Expression>,
        inferred_type: Option<String>,
    },
    DefinedOr {
        line: usize,
        source_file: Option<String>,
        left: Box<Expression>,
        right: Box<Expression>,
        inferred_type: Option<String>,
    },
    Assignment {
        line: usize,
        source_file: Option<String>,
        operator: String,
        left: Box<Expression>,
        right: Box<Expression>,
        is_weak_write: bool,
        inferred_type: Option<String>,
        runtime_typecheck_required: Option<bool>,
    },
    Call {
        line: usize,
        source_file: Option<String>,
        callee: Box<Expression>,
        arguments: Vec<CallArgument>,
        inferred_type: Option<String>,
    },
    MemberAccess {
        line: usize,
        source_file: Option<String>,
        object: Box<Expression>,
        member: String,
        inferred_type: Option<String>,
    },
    DynamicMemberCall {
        line: usize,
        source_file: Option<String>,
        object: Box<Expression>,
        member: Box<Expression>,
        arguments: Vec<CallArgument>,
        inferred_type: Option<String>,
    },
    Index {
        line: usize,
        source_file: Option<String>,
        object: Box<Expression>,
        index: Box<Expression>,
        inferred_type: Option<String>,
    },
    Slice {
        line: usize,
        source_file: Option<String>,
        object: Box<Expression>,
        start: Option<Box<Expression>>,
        end: Option<Box<Expression>>,
        inferred_type: Option<String>,
    },
    DictAccess {
        line: usize,
        source_file: Option<String>,
        object: Box<Expression>,
        key: Box<DictKey>,
        inferred_type: Option<String>,
    },
    PostfixUpdate {
        line: usize,
        source_file: Option<String>,
        operator: String,
        argument: Box<Expression>,
        inferred_type: Option<String>,
    },
    Lambda {
        line: usize,
        source_file: Option<String>,
        params: Vec<Parameter>,
        body: Box<Expression>,
        is_async: bool,
        inferred_type: Option<String>,
    },
    FunctionExpression {
        line: usize,
        source_file: Option<String>,
        params: Vec<Parameter>,
        return_type: Option<String>,
        body: BlockStatement,
        is_async: bool,
        inferred_type: Option<String>,
    },
    LetExpression {
        line: usize,
        source_file: Option<String>,
        kind: String,
        declared_type: Option<String>,
        name: String,
        init: Option<Box<Expression>>,
        is_weak_storage: bool,
        inferred_type: Option<String>,
        runtime_typecheck_required: Option<bool>,
    },
    TryExpression {
        line: usize,
        source_file: Option<String>,
        body: BlockStatement,
        handlers: Vec<CatchClause>,
        inferred_type: Option<String>,
    },
    DoExpression {
        line: usize,
        source_file: Option<String>,
        body: BlockStatement,
        inferred_type: Option<String>,
    },
    AwaitExpression {
        line: usize,
        source_file: Option<String>,
        body: BlockStatement,
        inferred_type: Option<String>,
    },
    SpawnExpression {
        line: usize,
        source_file: Option<String>,
        body: BlockStatement,
        inferred_type: Option<String>,
    },
    SuperCall {
        line: usize,
        source_file: Option<String>,
        arguments: Vec<CallArgument>,
        inferred_type: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemplatePart {
    Text {
        line: usize,
        source_file: Option<String>,
        value: String,
    },
    Expression {
        line: usize,
        source_file: Option<String>,
        expression: Box<Expression>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DictEntry {
    pub line: usize,
    pub source_file: Option<String>,
    pub key: DictKey,
    pub value: Expression,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DictKey {
    Identifier {
        line: usize,
        source_file: Option<String>,
        name: String,
    },
    StringLiteral {
        line: usize,
        source_file: Option<String>,
        value: String,
    },
    Expression {
        line: usize,
        source_file: Option<String>,
        expression: Box<Expression>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallArgument {
    Positional {
        line: usize,
        source_file: Option<String>,
        value: Expression,
    },
    Named {
        line: usize,
        source_file: Option<String>,
        name: DictKey,
        value: Expression,
    },
}

impl Program {
    pub fn to_json_pretty(&self) -> String {
        let mut out = String::new();
        self.write_json(&mut out, 0);
        out
    }

    fn write_json(&self, out: &mut String, indent: usize) {
        out.push_str("{\n");
        write_string_field(out, indent + 1, "type", "Program", true);
        write_number_field(out, indent + 1, "line", self.line, true);
        write_field_name(out, indent + 1, "statements");
        write_array(out, indent + 1, &self.statements, |out, indent, stmt| {
            stmt.write_json(out, indent)
        });
        out.push('\n');
        write_indent(out, indent);
        out.push('}');
    }
}

impl Statement {
    pub fn line(&self) -> usize {
        match self {
            Statement::Block(node) => node.line,
            Statement::VariableDeclaration(node) => node.line,
            Statement::FunctionDeclaration(node) => node.line,
            Statement::ClassDeclaration(node) => node.line,
            Statement::TraitDeclaration(node) => node.line,
            Statement::ImportDeclaration(node) => node.line,
            Statement::IfStatement(node) => node.line,
            Statement::WhileStatement(node) => node.line,
            Statement::ForStatement(node) => node.line,
            Statement::SwitchStatement(node) => node.line,
            Statement::TryStatement(node) => node.line,
            Statement::ReturnStatement(node) => node.line,
            Statement::LoopControlStatement(node) => node.line,
            Statement::ThrowStatement(node) => node.line,
            Statement::DieStatement(node) => node.line,
            Statement::PostfixConditionalStatement(node) => node.line,
            Statement::KeywordStatement(node) => node.line,
            Statement::ExpressionStatement(node) => node.line,
        }
    }

    pub fn source_file(&self) -> Option<&str> {
        match self {
            Statement::Block(node) => node.source_file.as_deref(),
            Statement::VariableDeclaration(node) => node.source_file.as_deref(),
            Statement::FunctionDeclaration(node) => node.source_file.as_deref(),
            Statement::ClassDeclaration(node) => node.source_file.as_deref(),
            Statement::TraitDeclaration(node) => node.source_file.as_deref(),
            Statement::ImportDeclaration(node) => node.source_file.as_deref(),
            Statement::IfStatement(node) => node.source_file.as_deref(),
            Statement::WhileStatement(node) => node.source_file.as_deref(),
            Statement::ForStatement(node) => node.source_file.as_deref(),
            Statement::SwitchStatement(node) => node.source_file.as_deref(),
            Statement::TryStatement(node) => node.source_file.as_deref(),
            Statement::ReturnStatement(node) => node.source_file.as_deref(),
            Statement::LoopControlStatement(node) => node.source_file.as_deref(),
            Statement::ThrowStatement(node) => node.source_file.as_deref(),
            Statement::DieStatement(node) => node.source_file.as_deref(),
            Statement::PostfixConditionalStatement(node) => node.source_file.as_deref(),
            Statement::KeywordStatement(node) => node.source_file.as_deref(),
            Statement::ExpressionStatement(node) => node.source_file.as_deref(),
        }
    }

    fn write_json(&self, out: &mut String, indent: usize) {
        match self {
            Statement::Block(node) => node.write_json(out, indent),
            Statement::VariableDeclaration(node) => node.write_json(out, indent),
            Statement::FunctionDeclaration(node) => node.write_json(out, indent),
            Statement::ClassDeclaration(node) => node.write_json(out, indent),
            Statement::TraitDeclaration(node) => node.write_json(out, indent),
            Statement::ImportDeclaration(node) => node.write_json(out, indent),
            Statement::IfStatement(node) => node.write_json(out, indent),
            Statement::WhileStatement(node) => node.write_json(out, indent),
            Statement::ForStatement(node) => node.write_json(out, indent),
            Statement::SwitchStatement(node) => node.write_json(out, indent),
            Statement::TryStatement(node) => node.write_json(out, indent),
            Statement::ReturnStatement(node) => node.write_json(out, indent),
            Statement::LoopControlStatement(node) => node.write_json(out, indent),
            Statement::ThrowStatement(node) => node.write_json(out, indent),
            Statement::DieStatement(node) => node.write_json(out, indent),
            Statement::PostfixConditionalStatement(node) => node.write_json(out, indent),
            Statement::KeywordStatement(node) => node.write_json(out, indent),
            Statement::ExpressionStatement(node) => node.write_json(out, indent),
        }
    }
}

impl BlockStatement {
    fn write_json(&self, out: &mut String, indent: usize) {
        write_object_start(out);
        write_string_field(out, indent + 1, "type", "BlockStatement", true);
        write_number_field(out, indent + 1, "line", self.line, true);
        write_bool_field(
            out,
            indent + 1,
            "needs_lexical_scope",
            self.needs_lexical_scope,
            true,
        );
        write_field_name(out, indent + 1, "statements");
        write_array(out, indent + 1, &self.statements, |out, indent, stmt| {
            stmt.write_json(out, indent)
        });
        out.push('\n');
        write_object_end(out, indent);
    }
}

impl VariableDeclaration {
    fn write_json(&self, out: &mut String, indent: usize) {
        write_object_start(out);
        write_string_field(out, indent + 1, "type", "VariableDeclaration", true);
        write_number_field(out, indent + 1, "line", self.line, true);
        write_string_field(out, indent + 1, "kind", &self.kind, true);
        write_optional_string_field(
            out,
            indent + 1,
            "declared_type",
            self.declared_type.as_deref(),
            true,
        );
        write_string_field(out, indent + 1, "name", &self.name, true);
        write_bool_field(
            out,
            indent + 1,
            "is_weak_storage",
            self.is_weak_storage,
            true,
        );
        write_optional_bool_field(
            out,
            indent + 1,
            "runtime_typecheck_required",
            self.runtime_typecheck_required,
            true,
        );
        write_optional_expr_field(out, indent + 1, "init", self.init.as_ref(), false);
        write_object_end(out, indent);
    }
}

impl FunctionDeclaration {
    fn write_json(&self, out: &mut String, indent: usize) {
        write_object_start(out);
        write_string_field(out, indent + 1, "type", "FunctionDeclaration", true);
        write_number_field(out, indent + 1, "line", self.line, true);
        write_string_field(out, indent + 1, "name", &self.name, true);
        write_bool_field(out, indent + 1, "is_async", self.is_async, true);
        write_optional_string_field(
            out,
            indent + 1,
            "return_type",
            self.return_type.as_deref(),
            true,
        );
        write_field_name(out, indent + 1, "params");
        write_array(out, indent + 1, &self.params, |out, indent, param| {
            param.write_json(out, indent)
        });
        out.push_str(",\n");
        write_field_name(out, indent + 1, "body");
        self.body.write_json(out, indent + 1);
        out.push('\n');
        write_object_end(out, indent);
    }
}

impl ClassDeclaration {
    fn write_json(&self, out: &mut String, indent: usize) {
        write_object_start(out);
        write_string_field(out, indent + 1, "type", "ClassDeclaration", true);
        write_number_field(out, indent + 1, "line", self.line, true);
        write_string_field(out, indent + 1, "name", &self.name, true);
        write_optional_string_field(out, indent + 1, "base", self.base.as_deref(), true);
        write_field_name(out, indent + 1, "traits");
        write_string_array(out, indent + 1, &self.traits);
        out.push_str(",\n");
        write_bool_field(out, indent + 1, "shorthand", self.shorthand, true);
        write_field_name(out, indent + 1, "body");
        write_array(out, indent + 1, &self.body, |out, indent, member| {
            member.write_json(out, indent)
        });
        out.push('\n');
        write_object_end(out, indent);
    }
}

impl TraitDeclaration {
    fn write_json(&self, out: &mut String, indent: usize) {
        write_object_start(out);
        write_string_field(out, indent + 1, "type", "TraitDeclaration", true);
        write_number_field(out, indent + 1, "line", self.line, true);
        write_string_field(out, indent + 1, "name", &self.name, true);
        write_bool_field(out, indent + 1, "shorthand", self.shorthand, true);
        write_field_name(out, indent + 1, "body");
        write_array(out, indent + 1, &self.body, |out, indent, member| {
            member.write_json(out, indent)
        });
        out.push('\n');
        write_object_end(out, indent);
    }
}

impl ClassMember {
    pub fn line(&self) -> usize {
        match self {
            ClassMember::Field(node) => node.line,
            ClassMember::Method(node) => node.line,
            ClassMember::Class(node) => node.line,
            ClassMember::Trait(node) => node.line,
        }
    }

    fn write_json(&self, out: &mut String, indent: usize) {
        match self {
            ClassMember::Field(node) => node.write_json(out, indent),
            ClassMember::Method(node) => node.write_json(out, indent),
            ClassMember::Class(node) => node.write_json(out, indent),
            ClassMember::Trait(node) => node.write_json(out, indent),
        }
    }
}

impl FieldDeclaration {
    fn write_json(&self, out: &mut String, indent: usize) {
        write_object_start(out);
        write_string_field(out, indent + 1, "type", "FieldDeclaration", true);
        write_number_field(out, indent + 1, "line", self.line, true);
        write_string_field(out, indent + 1, "kind", &self.kind, true);
        write_optional_string_field(
            out,
            indent + 1,
            "declared_type",
            self.declared_type.as_deref(),
            true,
        );
        write_string_field(out, indent + 1, "name", &self.name, true);
        write_field_name(out, indent + 1, "accessors");
        write_string_array(out, indent + 1, &self.accessors);
        out.push_str(",\n");
        write_bool_field(
            out,
            indent + 1,
            "is_weak_storage",
            self.is_weak_storage,
            true,
        );
        write_optional_bool_field(
            out,
            indent + 1,
            "runtime_typecheck_required",
            self.runtime_typecheck_required,
            true,
        );
        write_optional_expr_field(
            out,
            indent + 1,
            "default_value",
            self.default_value.as_ref(),
            false,
        );
        write_object_end(out, indent);
    }
}

impl MethodDeclaration {
    fn write_json(&self, out: &mut String, indent: usize) {
        write_object_start(out);
        write_string_field(out, indent + 1, "type", "MethodDeclaration", true);
        write_number_field(out, indent + 1, "line", self.line, true);
        write_string_field(out, indent + 1, "name", &self.name, true);
        write_bool_field(out, indent + 1, "is_static", self.is_static, true);
        write_bool_field(out, indent + 1, "is_async", self.is_async, true);
        write_optional_string_field(
            out,
            indent + 1,
            "return_type",
            self.return_type.as_deref(),
            true,
        );
        write_field_name(out, indent + 1, "params");
        write_array(out, indent + 1, &self.params, |out, indent, param| {
            param.write_json(out, indent)
        });
        out.push_str(",\n");
        write_field_name(out, indent + 1, "body");
        self.body.write_json(out, indent + 1);
        out.push('\n');
        write_object_end(out, indent);
    }
}

impl Parameter {
    fn write_json(&self, out: &mut String, indent: usize) {
        write_object_start(out);
        write_string_field(out, indent + 1, "type", "Parameter", true);
        write_number_field(out, indent + 1, "line", self.line, true);
        write_optional_string_field(
            out,
            indent + 1,
            "declared_type",
            self.declared_type.as_deref(),
            true,
        );
        write_string_field(out, indent + 1, "name", &self.name, true);
        write_bool_field(out, indent + 1, "optional", self.optional, true);
        write_bool_field(out, indent + 1, "variadic", self.variadic, true);
        write_optional_expr_field(
            out,
            indent + 1,
            "default_value",
            self.default_value.as_ref(),
            false,
        );
        write_object_end(out, indent);
    }
}

impl ImportDeclaration {
    fn write_json(&self, out: &mut String, indent: usize) {
        write_object_start(out);
        write_string_field(out, indent + 1, "type", "ImportDeclaration", true);
        write_number_field(out, indent + 1, "line", self.line, true);
        write_string_field(out, indent + 1, "source", &self.source, true);
        write_bool_field(out, indent + 1, "try_mode", self.try_mode, true);
        write_bool_field(out, indent + 1, "import_all", self.import_all, true);
        write_field_name(out, indent + 1, "specifiers");
        write_array(out, indent + 1, &self.specifiers, |out, indent, spec| {
            spec.write_json(out, indent)
        });
        out.push_str(",\n");
        match &self.condition {
            Some(condition) => {
                write_field_name(out, indent + 1, "condition");
                condition.write_json(out, indent + 1);
                out.push('\n');
            }
            None => {
                write_null_field(out, indent + 1, "condition", false);
            }
        }
        write_object_end(out, indent);
    }
}

impl ImportSpecifier {
    fn write_json(&self, out: &mut String, indent: usize) {
        write_object_start(out);
        write_string_field(out, indent + 1, "type", "ImportSpecifier", true);
        write_number_field(out, indent + 1, "line", self.line, true);
        write_string_field(out, indent + 1, "imported", &self.imported, true);
        write_string_field(out, indent + 1, "local", &self.local, false);
        write_object_end(out, indent);
    }
}

impl PostfixCondition {
    fn write_json(&self, out: &mut String, indent: usize) {
        write_object_start(out);
        write_number_field(out, indent + 1, "line", self.line, true);
        write_string_field(out, indent + 1, "keyword", &self.keyword, true);
        write_field_name(out, indent + 1, "test");
        self.test.write_json(out, indent + 1);
        out.push('\n');
        write_object_end(out, indent);
    }
}

impl IfStatement {
    fn write_json(&self, out: &mut String, indent: usize) {
        write_object_start(out);
        write_string_field(out, indent + 1, "type", "IfStatement", true);
        write_number_field(out, indent + 1, "line", self.line, true);
        write_field_name(out, indent + 1, "test");
        self.test.write_json(out, indent + 1);
        out.push_str(",\n");
        write_field_name(out, indent + 1, "consequent");
        self.consequent.write_json(out, indent + 1);
        out.push_str(",\n");
        match &self.alternate {
            Some(alternate) => {
                write_field_name(out, indent + 1, "alternate");
                alternate.write_json(out, indent + 1);
                out.push('\n');
            }
            None => write_null_field(out, indent + 1, "alternate", false),
        }
        write_object_end(out, indent);
    }
}

impl WhileStatement {
    fn write_json(&self, out: &mut String, indent: usize) {
        write_object_start(out);
        write_string_field(out, indent + 1, "type", "WhileStatement", true);
        write_number_field(out, indent + 1, "line", self.line, true);
        write_field_name(out, indent + 1, "test");
        self.test.write_json(out, indent + 1);
        out.push_str(",\n");
        write_field_name(out, indent + 1, "body");
        self.body.write_json(out, indent + 1);
        out.push('\n');
        write_object_end(out, indent);
    }
}

impl ForStatement {
    fn write_json(&self, out: &mut String, indent: usize) {
        write_object_start(out);
        write_string_field(out, indent + 1, "type", "ForStatement", true);
        write_number_field(out, indent + 1, "line", self.line, true);
        write_optional_string_field(
            out,
            indent + 1,
            "binding_kind",
            self.binding_kind.as_deref(),
            true,
        );
        write_string_field(out, indent + 1, "variable", &self.variable, true);
        write_field_name(out, indent + 1, "iterable");
        self.iterable.write_json(out, indent + 1);
        out.push_str(",\n");
        write_field_name(out, indent + 1, "body");
        self.body.write_json(out, indent + 1);
        out.push_str(",\n");
        match &self.else_block {
            Some(body) => {
                write_field_name(out, indent + 1, "else_block");
                body.write_json(out, indent + 1);
                out.push('\n');
            }
            None => write_null_field(out, indent + 1, "else_block", false),
        }
        write_object_end(out, indent);
    }
}

impl SwitchStatement {
    fn write_json(&self, out: &mut String, indent: usize) {
        write_object_start(out);
        write_string_field(out, indent + 1, "type", "SwitchStatement", true);
        write_number_field(out, indent + 1, "line", self.line, true);
        write_field_name(out, indent + 1, "discriminant");
        self.discriminant.write_json(out, indent + 1);
        out.push_str(",\n");
        write_optional_string_field(
            out,
            indent + 1,
            "comparator",
            self.comparator.as_deref(),
            true,
        );
        write_field_name(out, indent + 1, "cases");
        write_array(out, indent + 1, &self.cases, |out, indent, case| {
            case.write_json(out, indent)
        });
        out.push_str(",\n");
        match &self.index {
            Some(index) => {
                write_field_name(out, indent + 1, "index");
                write_array(out, indent + 1, index, |out, indent, entry| {
                    entry.write_json(out, indent)
                });
                out.push_str(",\n");
            }
            None => write_null_field(out, indent + 1, "index", true),
        }
        match &self.default {
            Some(default) => {
                write_field_name(out, indent + 1, "default");
                write_array(out, indent + 1, default, |out, indent, stmt| {
                    stmt.write_json(out, indent)
                });
                out.push('\n');
            }
            None => write_null_field(out, indent + 1, "default", false),
        }
        write_object_end(out, indent);
    }
}

impl SwitchCase {
    fn write_json(&self, out: &mut String, indent: usize) {
        write_object_start(out);
        write_string_field(out, indent + 1, "type", "SwitchCase", true);
        write_number_field(out, indent + 1, "line", self.line, true);
        write_field_name(out, indent + 1, "values");
        write_array(out, indent + 1, &self.values, |out, indent, expr| {
            expr.write_json(out, indent)
        });
        out.push_str(",\n");
        write_field_name(out, indent + 1, "consequent");
        write_array(out, indent + 1, &self.consequent, |out, indent, stmt| {
            stmt.write_json(out, indent)
        });
        out.push('\n');
        write_object_end(out, indent);
    }
}

impl SwitchIndexEntry {
    fn write_json(&self, out: &mut String, indent: usize) {
        write_object_start(out);
        write_string_field(out, indent + 1, "key", &self.key, true);
        write_number_field(out, indent + 1, "case_index", self.case_index, false);
        write_object_end(out, indent);
    }
}

impl TryStatement {
    fn write_json(&self, out: &mut String, indent: usize) {
        write_object_start(out);
        write_string_field(out, indent + 1, "type", "TryStatement", true);
        write_number_field(out, indent + 1, "line", self.line, true);
        write_field_name(out, indent + 1, "body");
        self.body.write_json(out, indent + 1);
        out.push_str(",\n");
        write_field_name(out, indent + 1, "handlers");
        write_array(out, indent + 1, &self.handlers, |out, indent, handler| {
            handler.write_json(out, indent)
        });
        out.push('\n');
        write_object_end(out, indent);
    }
}

impl CatchClause {
    fn write_json(&self, out: &mut String, indent: usize) {
        write_object_start(out);
        write_string_field(out, indent + 1, "type", "CatchClause", true);
        write_number_field(out, indent + 1, "line", self.line, true);
        match &self.binding {
            Some(binding) => {
                write_field_name(out, indent + 1, "binding");
                binding.write_json(out, indent + 1);
                out.push_str(",\n");
            }
            None => write_null_field(out, indent + 1, "binding", true),
        }
        write_field_name(out, indent + 1, "body");
        self.body.write_json(out, indent + 1);
        out.push('\n');
        write_object_end(out, indent);
    }
}

impl CatchBinding {
    fn write_json(&self, out: &mut String, indent: usize) {
        write_object_start(out);
        write_number_field(out, indent + 1, "line", self.line, true);
        write_optional_string_field(
            out,
            indent + 1,
            "declared_type",
            self.declared_type.as_deref(),
            true,
        );
        write_optional_string_field(out, indent + 1, "name", self.name.as_deref(), false);
        write_object_end(out, indent);
    }
}

impl ReturnStatement {
    fn write_json(&self, out: &mut String, indent: usize) {
        write_object_start(out);
        write_string_field(out, indent + 1, "type", "ReturnStatement", true);
        write_number_field(out, indent + 1, "line", self.line, true);
        write_optional_bool_field(
            out,
            indent + 1,
            "runtime_typecheck_required",
            self.runtime_typecheck_required,
            true,
        );
        write_optional_expr_field(out, indent + 1, "argument", self.argument.as_ref(), false);
        write_object_end(out, indent);
    }
}

impl LoopControlStatement {
    fn write_json(&self, out: &mut String, indent: usize) {
        write_object_start(out);
        write_string_field(out, indent + 1, "type", "LoopControlStatement", true);
        write_number_field(out, indent + 1, "line", self.line, true);
        write_string_field(out, indent + 1, "keyword", &self.keyword, false);
        write_object_end(out, indent);
    }
}

impl ThrowStatement {
    fn write_json(&self, out: &mut String, indent: usize) {
        write_object_start(out);
        write_string_field(out, indent + 1, "type", "ThrowStatement", true);
        write_number_field(out, indent + 1, "line", self.line, true);
        write_field_name(out, indent + 1, "argument");
        self.argument.write_json(out, indent + 1);
        out.push('\n');
        write_object_end(out, indent);
    }
}

impl DieStatement {
    fn write_json(&self, out: &mut String, indent: usize) {
        write_object_start(out);
        write_string_field(out, indent + 1, "type", "DieStatement", true);
        write_number_field(out, indent + 1, "line", self.line, true);
        write_field_name(out, indent + 1, "argument");
        self.argument.write_json(out, indent + 1);
        out.push('\n');
        write_object_end(out, indent);
    }
}

impl PostfixConditionalStatement {
    fn write_json(&self, out: &mut String, indent: usize) {
        write_object_start(out);
        write_string_field(out, indent + 1, "type", "PostfixConditionalStatement", true);
        write_number_field(out, indent + 1, "line", self.line, true);
        write_string_field(out, indent + 1, "keyword", &self.keyword, true);
        write_field_name(out, indent + 1, "statement");
        self.statement.write_json(out, indent + 1);
        out.push_str(",\n");
        write_field_name(out, indent + 1, "test");
        self.test.write_json(out, indent + 1);
        out.push('\n');
        write_object_end(out, indent);
    }
}

impl KeywordStatement {
    fn write_json(&self, out: &mut String, indent: usize) {
        write_object_start(out);
        write_string_field(out, indent + 1, "type", "KeywordStatement", true);
        write_number_field(out, indent + 1, "line", self.line, true);
        write_string_field(out, indent + 1, "keyword", &self.keyword, true);
        write_field_name(out, indent + 1, "arguments");
        write_array(out, indent + 1, &self.arguments, |out, indent, expr| {
            expr.write_json(out, indent)
        });
        out.push('\n');
        write_object_end(out, indent);
    }
}

impl ExpressionStatement {
    fn write_json(&self, out: &mut String, indent: usize) {
        write_object_start(out);
        write_string_field(out, indent + 1, "type", "ExpressionStatement", true);
        write_number_field(out, indent + 1, "line", self.line, true);
        write_field_name(out, indent + 1, "expression");
        self.expression.write_json(out, indent + 1);
        out.push('\n');
        write_object_end(out, indent);
    }
}

impl Expression {
    pub fn line(&self) -> usize {
        match self {
            Expression::Identifier { line, .. } => *line,
            Expression::NumberLiteral { line, .. } => *line,
            Expression::StringLiteral { line, .. } => *line,
            Expression::RegexLiteral { line, .. } => *line,
            Expression::BooleanLiteral { line, .. } => *line,
            Expression::NullLiteral { line, .. } => *line,
            Expression::ArrayLiteral { line, .. } => *line,
            Expression::SetLiteral { line, .. } => *line,
            Expression::BagLiteral { line, .. } => *line,
            Expression::DictLiteral { line, .. } => *line,
            Expression::PairListLiteral { line, .. } => *line,
            Expression::TemplateLiteral { line, .. } => *line,
            Expression::Unary { line, .. } => *line,
            Expression::Binary { line, .. } => *line,
            Expression::Ternary { line, .. } => *line,
            Expression::DefinedOr { line, .. } => *line,
            Expression::Assignment { line, .. } => *line,
            Expression::Call { line, .. } => *line,
            Expression::MemberAccess { line, .. } => *line,
            Expression::DynamicMemberCall { line, .. } => *line,
            Expression::Index { line, .. } => *line,
            Expression::Slice { line, .. } => *line,
            Expression::DictAccess { line, .. } => *line,
            Expression::PostfixUpdate { line, .. } => *line,
            Expression::Lambda { line, .. } => *line,
            Expression::FunctionExpression { line, .. } => *line,
            Expression::LetExpression { line, .. } => *line,
            Expression::TryExpression { line, .. } => *line,
            Expression::DoExpression { line, .. } => *line,
            Expression::AwaitExpression { line, .. } => *line,
            Expression::SpawnExpression { line, .. } => *line,
            Expression::SuperCall { line, .. } => *line,
        }
    }

    pub fn source_file(&self) -> Option<&str> {
        match self {
            Expression::Identifier { source_file, .. }
            | Expression::NumberLiteral { source_file, .. }
            | Expression::StringLiteral { source_file, .. }
            | Expression::RegexLiteral { source_file, .. }
            | Expression::BooleanLiteral { source_file, .. }
            | Expression::NullLiteral { source_file, .. }
            | Expression::ArrayLiteral { source_file, .. }
            | Expression::SetLiteral { source_file, .. }
            | Expression::BagLiteral { source_file, .. }
            | Expression::DictLiteral { source_file, .. }
            | Expression::PairListLiteral { source_file, .. }
            | Expression::TemplateLiteral { source_file, .. }
            | Expression::Unary { source_file, .. }
            | Expression::Binary { source_file, .. }
            | Expression::Ternary { source_file, .. }
            | Expression::DefinedOr { source_file, .. }
            | Expression::Assignment { source_file, .. }
            | Expression::Call { source_file, .. }
            | Expression::MemberAccess { source_file, .. }
            | Expression::DynamicMemberCall { source_file, .. }
            | Expression::Index { source_file, .. }
            | Expression::Slice { source_file, .. }
            | Expression::DictAccess { source_file, .. }
            | Expression::PostfixUpdate { source_file, .. }
            | Expression::Lambda { source_file, .. }
            | Expression::FunctionExpression { source_file, .. }
            | Expression::LetExpression { source_file, .. }
            | Expression::TryExpression { source_file, .. }
            | Expression::DoExpression { source_file, .. }
            | Expression::AwaitExpression { source_file, .. }
            | Expression::SpawnExpression { source_file, .. }
            | Expression::SuperCall { source_file, .. } => source_file.as_deref(),
        }
    }

    pub fn inferred_type(&self) -> Option<&str> {
        match self {
            Expression::Identifier { inferred_type, .. }
            | Expression::NumberLiteral { inferred_type, .. }
            | Expression::StringLiteral { inferred_type, .. }
            | Expression::RegexLiteral { inferred_type, .. }
            | Expression::BooleanLiteral { inferred_type, .. }
            | Expression::NullLiteral { inferred_type, .. }
            | Expression::ArrayLiteral { inferred_type, .. }
            | Expression::SetLiteral { inferred_type, .. }
            | Expression::BagLiteral { inferred_type, .. }
            | Expression::DictLiteral { inferred_type, .. }
            | Expression::PairListLiteral { inferred_type, .. }
            | Expression::TemplateLiteral { inferred_type, .. }
            | Expression::Unary { inferred_type, .. }
            | Expression::Binary { inferred_type, .. }
            | Expression::Ternary { inferred_type, .. }
            | Expression::DefinedOr { inferred_type, .. }
            | Expression::Assignment { inferred_type, .. }
            | Expression::Call { inferred_type, .. }
            | Expression::MemberAccess { inferred_type, .. }
            | Expression::DynamicMemberCall { inferred_type, .. }
            | Expression::Index { inferred_type, .. }
            | Expression::Slice { inferred_type, .. }
            | Expression::DictAccess { inferred_type, .. }
            | Expression::PostfixUpdate { inferred_type, .. }
            | Expression::Lambda { inferred_type, .. }
            | Expression::FunctionExpression { inferred_type, .. }
            | Expression::LetExpression { inferred_type, .. }
            | Expression::TryExpression { inferred_type, .. }
            | Expression::DoExpression { inferred_type, .. }
            | Expression::AwaitExpression { inferred_type, .. }
            | Expression::SpawnExpression { inferred_type, .. }
            | Expression::SuperCall { inferred_type, .. } => inferred_type.as_deref(),
        }
    }

    pub fn set_inferred_type(&mut self, value: Option<String>) {
        match self {
            Expression::Identifier { inferred_type, .. }
            | Expression::NumberLiteral { inferred_type, .. }
            | Expression::StringLiteral { inferred_type, .. }
            | Expression::RegexLiteral { inferred_type, .. }
            | Expression::BooleanLiteral { inferred_type, .. }
            | Expression::NullLiteral { inferred_type, .. }
            | Expression::ArrayLiteral { inferred_type, .. }
            | Expression::SetLiteral { inferred_type, .. }
            | Expression::BagLiteral { inferred_type, .. }
            | Expression::DictLiteral { inferred_type, .. }
            | Expression::PairListLiteral { inferred_type, .. }
            | Expression::TemplateLiteral { inferred_type, .. }
            | Expression::Unary { inferred_type, .. }
            | Expression::Binary { inferred_type, .. }
            | Expression::Ternary { inferred_type, .. }
            | Expression::DefinedOr { inferred_type, .. }
            | Expression::Assignment { inferred_type, .. }
            | Expression::Call { inferred_type, .. }
            | Expression::MemberAccess { inferred_type, .. }
            | Expression::DynamicMemberCall { inferred_type, .. }
            | Expression::Index { inferred_type, .. }
            | Expression::Slice { inferred_type, .. }
            | Expression::DictAccess { inferred_type, .. }
            | Expression::PostfixUpdate { inferred_type, .. }
            | Expression::Lambda { inferred_type, .. }
            | Expression::FunctionExpression { inferred_type, .. }
            | Expression::LetExpression { inferred_type, .. }
            | Expression::TryExpression { inferred_type, .. }
            | Expression::DoExpression { inferred_type, .. }
            | Expression::AwaitExpression { inferred_type, .. }
            | Expression::SpawnExpression { inferred_type, .. }
            | Expression::SuperCall { inferred_type, .. } => *inferred_type = value,
        }
    }

    pub fn runtime_typecheck_required(&self) -> Option<bool> {
        match self {
            Expression::Assignment {
                runtime_typecheck_required,
                ..
            }
            | Expression::LetExpression {
                runtime_typecheck_required,
                ..
            } => *runtime_typecheck_required,
            _ => None,
        }
    }

    pub fn set_runtime_typecheck_required(&mut self, value: Option<bool>) {
        match self {
            Expression::Assignment {
                runtime_typecheck_required,
                ..
            }
            | Expression::LetExpression {
                runtime_typecheck_required,
                ..
            } => *runtime_typecheck_required = value,
            _ => {}
        }
    }

    pub fn is_weak_write(&self) -> bool {
        match self {
            Expression::Assignment { is_weak_write, .. } => *is_weak_write,
            _ => false,
        }
    }

    pub fn is_weak_storage(&self) -> bool {
        match self {
            Expression::LetExpression {
                is_weak_storage, ..
            } => *is_weak_storage,
            _ => false,
        }
    }

    fn write_json(&self, out: &mut String, indent: usize) {
        let inferred_type = self.inferred_type();
        match self {
            Expression::Identifier {
                line,
                name,
                binding_depth,
                ..
            } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "Identifier", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_optional_number_field(out, indent + 1, "binding_depth", *binding_depth, true);
                write_string_field(out, indent + 1, "name", name, false);
                write_object_end(out, indent);
            }
            Expression::NumberLiteral { line, value, .. } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "NumberLiteral", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_string_field(out, indent + 1, "value", value, false);
                write_object_end(out, indent);
            }
            Expression::StringLiteral { line, value, .. } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "StringLiteral", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_string_field(out, indent + 1, "value", value, false);
                write_object_end(out, indent);
            }
            Expression::RegexLiteral {
                line,
                pattern,
                flags,
                cache_key,
                ..
            } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "RegexLiteral", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_string_field(out, indent + 1, "pattern", pattern, true);
                write_string_field(out, indent + 1, "flags", flags, true);
                write_optional_string_field(
                    out,
                    indent + 1,
                    "regex_cache_key",
                    cache_key.as_deref(),
                    false,
                );
                write_object_end(out, indent);
            }
            Expression::BooleanLiteral { line, value, .. } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "BooleanLiteral", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_field_name(out, indent + 1, "value");
                out.push_str(if *value { "true" } else { "false" });
                out.push('\n');
                write_object_end(out, indent);
            }
            Expression::NullLiteral { line, .. } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "NullLiteral", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, false);
                write_object_end(out, indent);
            }
            Expression::ArrayLiteral {
                line,
                elements,
                capacity_hint,
                ..
            } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "ArrayLiteral", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_optional_number_field(out, indent + 1, "capacity_hint", *capacity_hint, true);
                write_field_name(out, indent + 1, "elements");
                write_array(out, indent + 1, elements, |out, indent, expr| {
                    expr.write_json(out, indent)
                });
                out.push('\n');
                write_object_end(out, indent);
            }
            Expression::SetLiteral {
                line,
                elements,
                capacity_hint,
                ..
            } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "SetLiteral", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_optional_number_field(out, indent + 1, "capacity_hint", *capacity_hint, true);
                write_field_name(out, indent + 1, "elements");
                write_array(out, indent + 1, elements, |out, indent, expr| {
                    expr.write_json(out, indent)
                });
                out.push('\n');
                write_object_end(out, indent);
            }
            Expression::BagLiteral {
                line,
                elements,
                capacity_hint,
                ..
            } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "BagLiteral", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_optional_number_field(out, indent + 1, "capacity_hint", *capacity_hint, true);
                write_field_name(out, indent + 1, "elements");
                write_array(out, indent + 1, elements, |out, indent, expr| {
                    expr.write_json(out, indent)
                });
                out.push('\n');
                write_object_end(out, indent);
            }
            Expression::DictLiteral {
                line,
                entries,
                capacity_hint,
                ..
            } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "DictLiteral", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_optional_number_field(out, indent + 1, "capacity_hint", *capacity_hint, true);
                write_field_name(out, indent + 1, "entries");
                write_array(out, indent + 1, entries, |out, indent, entry| {
                    entry.write_json(out, indent)
                });
                out.push('\n');
                write_object_end(out, indent);
            }
            Expression::PairListLiteral {
                line,
                entries,
                capacity_hint,
                ..
            } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "PairListLiteral", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_optional_number_field(out, indent + 1, "capacity_hint", *capacity_hint, true);
                write_field_name(out, indent + 1, "entries");
                write_array(out, indent + 1, entries, |out, indent, entry| {
                    entry.write_json(out, indent)
                });
                out.push('\n');
                write_object_end(out, indent);
            }
            Expression::TemplateLiteral { line, parts, .. } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "TemplateLiteral", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_field_name(out, indent + 1, "parts");
                write_array(out, indent + 1, parts, |out, indent, part| {
                    part.write_json(out, indent)
                });
                out.push('\n');
                write_object_end(out, indent);
            }
            Expression::Unary {
                line,
                operator,
                argument,
                ..
            } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "UnaryExpression", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_string_field(out, indent + 1, "operator", operator, true);
                write_string_field(
                    out,
                    indent + 1,
                    "operator_kind",
                    operator_kind(operator),
                    true,
                );
                write_field_name(out, indent + 1, "argument");
                argument.write_json(out, indent + 1);
                out.push('\n');
                write_object_end(out, indent);
            }
            Expression::Binary {
                line,
                operator,
                left,
                right,
                ..
            } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "BinaryExpression", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_string_field(out, indent + 1, "operator", operator, true);
                write_string_field(
                    out,
                    indent + 1,
                    "operator_kind",
                    operator_kind(operator),
                    true,
                );
                write_field_name(out, indent + 1, "left");
                left.write_json(out, indent + 1);
                out.push_str(",\n");
                write_field_name(out, indent + 1, "right");
                right.write_json(out, indent + 1);
                out.push('\n');
                write_object_end(out, indent);
            }
            Expression::Ternary {
                line,
                test,
                consequent,
                alternate,
                ..
            } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "TernaryExpression", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_field_name(out, indent + 1, "test");
                test.write_json(out, indent + 1);
                out.push_str(",\n");
                write_field_name(out, indent + 1, "consequent");
                consequent.write_json(out, indent + 1);
                out.push_str(",\n");
                write_field_name(out, indent + 1, "alternate");
                alternate.write_json(out, indent + 1);
                out.push('\n');
                write_object_end(out, indent);
            }
            Expression::DefinedOr {
                line, left, right, ..
            } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "DefinedOrExpression", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_field_name(out, indent + 1, "left");
                left.write_json(out, indent + 1);
                out.push_str(",\n");
                write_field_name(out, indent + 1, "right");
                right.write_json(out, indent + 1);
                out.push('\n');
                write_object_end(out, indent);
            }
            Expression::Assignment {
                line,
                operator,
                left,
                right,
                ..
            } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "AssignmentExpression", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_optional_bool_field(
                    out,
                    indent + 1,
                    "runtime_typecheck_required",
                    self.runtime_typecheck_required(),
                    true,
                );
                write_bool_field(out, indent + 1, "is_weak_write", self.is_weak_write(), true);
                write_string_field(out, indent + 1, "operator", operator, true);
                write_string_field(
                    out,
                    indent + 1,
                    "operator_kind",
                    operator_kind(operator),
                    true,
                );
                write_field_name(out, indent + 1, "left");
                left.write_json(out, indent + 1);
                out.push_str(",\n");
                write_field_name(out, indent + 1, "right");
                right.write_json(out, indent + 1);
                out.push('\n');
                write_object_end(out, indent);
            }
            Expression::Call {
                line,
                callee,
                arguments,
                ..
            } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "CallExpression", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_field_name(out, indent + 1, "callee");
                callee.write_json(out, indent + 1);
                out.push_str(",\n");
                write_field_name(out, indent + 1, "arguments");
                write_array(out, indent + 1, arguments, |out, indent, arg| {
                    arg.write_json(out, indent)
                });
                out.push('\n');
                write_object_end(out, indent);
            }
            Expression::MemberAccess {
                line,
                object,
                member,
                ..
            } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "MemberAccess", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_field_name(out, indent + 1, "object");
                object.write_json(out, indent + 1);
                out.push_str(",\n");
                write_string_field(out, indent + 1, "member", member, false);
                write_object_end(out, indent);
            }
            Expression::DynamicMemberCall {
                line,
                object,
                member,
                arguments,
                ..
            } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "DynamicMemberCall", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_field_name(out, indent + 1, "object");
                object.write_json(out, indent + 1);
                out.push_str(",\n");
                write_field_name(out, indent + 1, "member");
                member.write_json(out, indent + 1);
                out.push_str(",\n");
                write_field_name(out, indent + 1, "arguments");
                write_array(out, indent + 1, arguments, |out, indent, arg| {
                    arg.write_json(out, indent)
                });
                out.push('\n');
                write_object_end(out, indent);
            }
            Expression::Index {
                line,
                object,
                index,
                ..
            } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "IndexExpression", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_field_name(out, indent + 1, "object");
                object.write_json(out, indent + 1);
                out.push_str(",\n");
                write_field_name(out, indent + 1, "index");
                index.write_json(out, indent + 1);
                out.push('\n');
                write_object_end(out, indent);
            }
            Expression::Slice {
                line,
                object,
                start,
                end,
                ..
            } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "SliceExpression", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_field_name(out, indent + 1, "object");
                object.write_json(out, indent + 1);
                out.push_str(",\n");
                write_optional_boxed_expr_field(out, indent + 1, "start", start.as_ref(), true);
                write_optional_boxed_expr_field(out, indent + 1, "end", end.as_ref(), false);
                write_object_end(out, indent);
            }
            Expression::DictAccess {
                line, object, key, ..
            } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "DictAccessExpression", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_field_name(out, indent + 1, "object");
                object.write_json(out, indent + 1);
                out.push_str(",\n");
                write_field_name(out, indent + 1, "key");
                key.write_json(out, indent + 1);
                out.push('\n');
                write_object_end(out, indent);
            }
            Expression::PostfixUpdate {
                line,
                operator,
                argument,
                ..
            } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "PostfixUpdateExpression", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_string_field(out, indent + 1, "operator", operator, true);
                write_string_field(
                    out,
                    indent + 1,
                    "operator_kind",
                    operator_kind(operator),
                    true,
                );
                write_field_name(out, indent + 1, "argument");
                argument.write_json(out, indent + 1);
                out.push('\n');
                write_object_end(out, indent);
            }
            Expression::Lambda {
                line,
                params,
                body,
                is_async,
                ..
            } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "LambdaExpression", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_bool_field(out, indent + 1, "is_async", *is_async, true);
                write_field_name(out, indent + 1, "params");
                write_array(out, indent + 1, params, |out, indent, param| {
                    param.write_json(out, indent)
                });
                out.push_str(",\n");
                write_field_name(out, indent + 1, "body");
                body.write_json(out, indent + 1);
                out.push('\n');
                write_object_end(out, indent);
            }
            Expression::FunctionExpression {
                line,
                params,
                return_type,
                body,
                is_async,
                ..
            } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "FunctionExpression", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_bool_field(out, indent + 1, "is_async", *is_async, true);
                write_optional_string_field(
                    out,
                    indent + 1,
                    "return_type",
                    return_type.as_deref(),
                    true,
                );
                write_field_name(out, indent + 1, "params");
                write_array(out, indent + 1, params, |out, indent, param| {
                    param.write_json(out, indent)
                });
                out.push_str(",\n");
                write_field_name(out, indent + 1, "body");
                body.write_json(out, indent + 1);
                out.push('\n');
                write_object_end(out, indent);
            }
            Expression::LetExpression {
                line,
                kind,
                declared_type,
                name,
                init,
                ..
            } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "LetExpression", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_string_field(out, indent + 1, "kind", kind, true);
                write_optional_string_field(
                    out,
                    indent + 1,
                    "declared_type",
                    declared_type.as_deref(),
                    true,
                );
                write_string_field(out, indent + 1, "name", name, true);
                write_bool_field(
                    out,
                    indent + 1,
                    "is_weak_storage",
                    self.is_weak_storage(),
                    true,
                );
                write_optional_bool_field(
                    out,
                    indent + 1,
                    "runtime_typecheck_required",
                    self.runtime_typecheck_required(),
                    true,
                );
                match init {
                    Some(init) => {
                        write_field_name(out, indent + 1, "init");
                        init.write_json(out, indent + 1);
                        out.push('\n');
                    }
                    None => write_null_field(out, indent + 1, "init", false),
                }
                write_object_end(out, indent);
            }
            Expression::TryExpression {
                line,
                body,
                handlers,
                ..
            } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "TryExpression", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_field_name(out, indent + 1, "body");
                body.write_json(out, indent + 1);
                out.push_str(",\n");
                write_field_name(out, indent + 1, "handlers");
                write_array(out, indent + 1, handlers, |out, indent, clause| {
                    clause.write_json(out, indent)
                });
                out.push('\n');
                write_object_end(out, indent);
            }
            Expression::DoExpression { line, body, .. } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "DoExpression", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_field_name(out, indent + 1, "body");
                body.write_json(out, indent + 1);
                out.push('\n');
                write_object_end(out, indent);
            }
            Expression::AwaitExpression { line, body, .. } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "AwaitExpression", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_field_name(out, indent + 1, "body");
                body.write_json(out, indent + 1);
                out.push('\n');
                write_object_end(out, indent);
            }
            Expression::SpawnExpression { line, body, .. } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "SpawnExpression", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_field_name(out, indent + 1, "body");
                body.write_json(out, indent + 1);
                out.push('\n');
                write_object_end(out, indent);
            }
            Expression::SuperCall {
                line, arguments, ..
            } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "SuperCallExpression", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_optional_string_field(out, indent + 1, "inferred_type", inferred_type, true);
                write_field_name(out, indent + 1, "arguments");
                write_array(out, indent + 1, arguments, |out, indent, arg| {
                    arg.write_json(out, indent)
                });
                out.push('\n');
                write_object_end(out, indent);
            }
        }
    }
}

impl TemplatePart {
    fn write_json(&self, out: &mut String, indent: usize) {
        match self {
            TemplatePart::Text { line, value, .. } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "TemplateText", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_string_field(out, indent + 1, "value", value, false);
                write_object_end(out, indent);
            }
            TemplatePart::Expression {
                line, expression, ..
            } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "TemplateExpression", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_field_name(out, indent + 1, "expression");
                expression.write_json(out, indent + 1);
                out.push('\n');
                write_object_end(out, indent);
            }
        }
    }
}

impl DictEntry {
    fn write_json(&self, out: &mut String, indent: usize) {
        write_object_start(out);
        write_number_field(out, indent + 1, "line", self.line, true);
        write_field_name(out, indent + 1, "key");
        self.key.write_json(out, indent + 1);
        out.push_str(",\n");
        write_field_name(out, indent + 1, "value");
        self.value.write_json(out, indent + 1);
        out.push('\n');
        write_object_end(out, indent);
    }
}

impl DictKey {
    pub fn line(&self) -> usize {
        match self {
            DictKey::Identifier { line, .. } => *line,
            DictKey::StringLiteral { line, .. } => *line,
            DictKey::Expression { line, .. } => *line,
        }
    }

    pub fn source_file(&self) -> Option<&str> {
        match self {
            DictKey::Identifier { source_file, .. }
            | DictKey::StringLiteral { source_file, .. }
            | DictKey::Expression { source_file, .. } => source_file.as_deref(),
        }
    }

    fn write_json(&self, out: &mut String, indent: usize) {
        match self {
            DictKey::Identifier { line, name, .. } => {
                write_simple_object(out, indent, "IdentifierKey", *line, "name", name)
            }
            DictKey::StringLiteral { line, value, .. } => {
                write_simple_object(out, indent, "StringKey", *line, "value", value)
            }
            DictKey::Expression {
                line, expression, ..
            } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "ExpressionKey", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_field_name(out, indent + 1, "expression");
                expression.write_json(out, indent + 1);
                out.push('\n');
                write_object_end(out, indent);
            }
        }
    }
}

impl CallArgument {
    pub fn line(&self) -> usize {
        match self {
            CallArgument::Positional { line, .. } => *line,
            CallArgument::Named { line, .. } => *line,
        }
    }

    fn write_json(&self, out: &mut String, indent: usize) {
        match self {
            CallArgument::Positional { line, value, .. } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "PositionalArgument", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_field_name(out, indent + 1, "value");
                value.write_json(out, indent + 1);
                out.push('\n');
                write_object_end(out, indent);
            }
            CallArgument::Named {
                line, name, value, ..
            } => {
                write_object_start(out);
                write_string_field(out, indent + 1, "type", "NamedArgument", true);
                write_number_field(out, indent + 1, "line", *line, true);
                write_field_name(out, indent + 1, "name");
                name.write_json(out, indent + 1);
                out.push_str(",\n");
                write_field_name(out, indent + 1, "value");
                value.write_json(out, indent + 1);
                out.push('\n');
                write_object_end(out, indent);
            }
        }
    }
}

fn write_array<T>(
    out: &mut String,
    indent: usize,
    items: &[T],
    mut write_item: impl FnMut(&mut String, usize, &T),
) {
    if items.is_empty() {
        out.push_str("[]");
        return;
    }
    out.push_str("[\n");
    for (index, item) in items.iter().enumerate() {
        write_indent(out, indent + 1);
        write_item(out, indent + 1, item);
        if index + 1 != items.len() {
            out.push(',');
        }
        out.push('\n');
    }
    write_indent(out, indent);
    out.push(']');
}

fn write_string_array(out: &mut String, indent: usize, items: &[String]) {
    write_array(out, indent, items, |out, _indent, item| {
        write_json_string(out, item)
    });
}

fn operator_kind(operator: &str) -> &str {
    match operator {
        "+" => "plus",
        "-" => "minus",
        "×" | "*" => "multiply",
        "÷" | "/" => "divide",
        "mod" => "modulo",
        "**" => "exponent",
        "=" => "numeric_equal",
        "≠" => "numeric_not_equal",
        "<" => "less_than",
        ">" => "greater_than",
        "≤" | "<=" => "less_equal",
        "≥" | ">=" => "greater_equal",
        "≶" | "<=>" | "≷" => "numeric_compare",
        "≡" | "==" => "strict_equal",
        "≢" | "!=" => "strict_not_equal",
        "⋀" | "and" => "logical_and",
        "⋁" | "or" => "logical_or",
        "⊻" | "xor" => "logical_xor",
        "⊼" | "nand" => "logical_nand",
        "¬" | "not" | "!" => "logical_not",
        "√" | "sqrt" => "sqrt",
        "⋃" | "union" => "set_union",
        "⋂" | "intersection" => "set_intersection",
        "∖" | "\\" => "set_difference",
        "∈" | "in" => "membership",
        "∉" => "not_membership",
        "⊂" | "subsetof" => "subset",
        "⊃" | "supersetof" => "superset",
        "⊂⊃" | "equivalentof" => "set_equivalent",
        "@" => "path_first",
        "@@" => "path_all",
        "@?" => "path_exists",
        ":=" => "assign",
        "+=" => "add_assign",
        "-=" => "subtract_assign",
        "×=" | "*=" => "multiply_assign",
        "÷=" | "/=" => "divide_assign",
        "_=" => "concat_assign",
        "~=" => "regex_replace_assign",
        "**=" => "exponent_assign",
        "?:=" => "defined_assign",
        "++" => "increment",
        "--" => "decrement",
        other => other,
    }
}

fn write_optional_expr_field(
    out: &mut String,
    indent: usize,
    key: &str,
    expr: Option<&Expression>,
    trailing_comma: bool,
) {
    match expr {
        Some(expr) => {
            write_field_name(out, indent, key);
            expr.write_json(out, indent);
            if trailing_comma {
                out.push_str(",\n");
            } else {
                out.push('\n');
            }
        }
        None => write_null_field(out, indent, key, trailing_comma),
    }
}

fn write_optional_boxed_expr_field(
    out: &mut String,
    indent: usize,
    key: &str,
    expr: Option<&Box<Expression>>,
    trailing_comma: bool,
) {
    write_optional_expr_field(out, indent, key, expr.map(Box::as_ref), trailing_comma);
}

fn write_string_field(
    out: &mut String,
    indent: usize,
    key: &str,
    value: &str,
    trailing_comma: bool,
) {
    write_field_name(out, indent, key);
    write_json_string(out, value);
    if trailing_comma {
        out.push_str(",\n");
    } else {
        out.push('\n');
    }
}

fn write_number_field(
    out: &mut String,
    indent: usize,
    key: &str,
    value: usize,
    trailing_comma: bool,
) {
    write_field_name(out, indent, key);
    out.push_str(&value.to_string());
    if trailing_comma {
        out.push_str(",\n");
    } else {
        out.push('\n');
    }
}

fn write_optional_string_field(
    out: &mut String,
    indent: usize,
    key: &str,
    value: Option<&str>,
    trailing_comma: bool,
) {
    match value {
        Some(value) => write_string_field(out, indent, key, value, trailing_comma),
        None => write_null_field(out, indent, key, trailing_comma),
    }
}

fn write_optional_number_field(
    out: &mut String,
    indent: usize,
    key: &str,
    value: Option<usize>,
    trailing_comma: bool,
) {
    write_field_name(out, indent, key);
    match value {
        Some(value) => out.push_str(&value.to_string()),
        None => out.push_str("null"),
    }
    if trailing_comma {
        out.push_str(",\n");
    } else {
        out.push('\n');
    }
}

fn write_bool_field(out: &mut String, indent: usize, key: &str, value: bool, trailing_comma: bool) {
    write_field_name(out, indent, key);
    out.push_str(if value { "true" } else { "false" });
    if trailing_comma {
        out.push_str(",\n");
    } else {
        out.push('\n');
    }
}

fn write_optional_bool_field(
    out: &mut String,
    indent: usize,
    key: &str,
    value: Option<bool>,
    trailing_comma: bool,
) {
    write_field_name(out, indent, key);
    match value {
        Some(true) => out.push_str("true"),
        Some(false) => out.push_str("false"),
        None => out.push_str("null"),
    }
    if trailing_comma {
        out.push_str(",\n");
    } else {
        out.push('\n');
    }
}

fn write_null_field(out: &mut String, indent: usize, key: &str, trailing_comma: bool) {
    write_field_name(out, indent, key);
    out.push_str("null");
    if trailing_comma {
        out.push_str(",\n");
    } else {
        out.push('\n');
    }
}

fn write_field_name(out: &mut String, indent: usize, key: &str) {
    write_indent(out, indent);
    write_json_string(out, key);
    out.push_str(": ");
}

fn write_indent(out: &mut String, indent: usize) {
    for _ in 0..indent {
        out.push_str("  ");
    }
}

fn write_json_string(out: &mut String, value: &str) {
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(ch),
        }
    }
    out.push('"');
}

fn write_object_start(out: &mut String) {
    out.push_str("{\n");
}

fn write_object_end(out: &mut String, indent: usize) {
    write_indent(out, indent);
    out.push('}');
}

fn write_simple_object(
    out: &mut String,
    indent: usize,
    type_name: &str,
    line: usize,
    key: &str,
    value: &str,
) {
    write_object_start(out);
    write_string_field(out, indent + 1, "type", type_name, true);
    write_number_field(out, indent + 1, "line", line, true);
    write_string_field(out, indent + 1, key, value, false);
    write_object_end(out, indent);
}
