#[derive(Debug, Clone, PartialEq)]
pub struct Program {
    pub statements: Vec<Statement>,
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
}

#[derive(Debug, Clone, PartialEq)]
pub struct PipeFlow {
    pub source: Source,
    pub operations: Vec<(PipeOp, Destination)>,
    pub on_fail: Option<OnFail>,
    pub comments: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OnFail {
    pub alias: Option<String>,
    pub handler: Box<FlowOrBranch>,
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
}

#[derive(Debug, Clone, PartialEq)]
pub struct DirectiveFlow {
    pub name: String,
    pub arguments: Vec<Expression>,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BranchItem {
    Flow(PipeFlow),
    Comment(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Branch {
    pub items: Vec<BranchItem>,
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
}
