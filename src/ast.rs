#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SourcePos {
    pub line: usize,
    pub col: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Span {
    pub start: SourcePos,
    pub end: SourcePos,
}

impl Span {
    pub fn contains_zero_based(&self, line: u32, character: u32) -> bool {
        if self.start.line == 0 || self.end.line == 0 {
            return false;
        }
        let line_1 = line as usize + 1;
        let col_1 = character as usize + 1;
        if line_1 < self.start.line || line_1 > self.end.line {
            return false;
        }
        if self.start.line == self.end.line {
            return col_1 >= self.start.col && col_1 <= self.end.col;
        }
        if line_1 == self.start.line {
            return col_1 >= self.start.col;
        }
        if line_1 == self.end.line {
            return col_1 <= self.end.col;
        }
        true
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Program {
    pub statements: Vec<Statement>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Import(ImportStmt),
    Pipe(PipeFlow),
    Function(FunctionDef),
    Comment(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ImportStmt {
    pub path: String,
    pub alias: Option<String>,
    pub comments: Vec<String>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PipeFlow {
    pub source: Source,
    pub operations: Vec<(PipeOp, Destination)>,
    pub on_fail: Option<OnFail>,
    pub comments: Vec<String>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OnFail {
    pub alias: Option<String>,
    pub handler: Box<FlowOrBranch>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PipeOp {
    Safe,  // >>
    Force, // >>>
    Move,  // ->
}

#[derive(Debug, Clone, PartialEq)]
pub enum Source {
    Directive(DirectiveFlow),
    FunctionCall(FunctionCall),
    Expression(Expression),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Destination {
    Branch(Branch),
    Directive(DirectiveFlow),
    FunctionCall(FunctionCall),
    Expression(Expression),
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionCall {
    pub name: String,
    pub arguments: Vec<Expression>,
    pub alias: Option<String>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DirectiveFlow {
    pub name: String,
    pub arguments: Vec<Expression>,
    pub alias: Option<String>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BranchItem {
    Flow(PipeFlow),
    Comment(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Branch {
    pub items: Vec<BranchItem>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FlowOrBranch {
    Flow(PipeFlow),
    Branch(Branch),
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionDef {
    pub name: String,
    pub parameters: Vec<String>,
    pub body: FlowOrBranch,
    pub comments: Vec<String>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Path(String),
    String(String),
    Number(f64),
    Boolean(bool),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Literal(Literal),
    Identifier(String),
    BinaryOp(Box<Expression>, String, Box<Expression>),
    UnaryOp(String, Box<Expression>),
    Lambda(Lambda),
    FunctionCall(FunctionCall),
    MemberAccess(Vec<String>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Lambda {
    pub param: String,
    pub body: Box<Expression>,
    pub span: Span,
}
