use crate::ast::*;
use pest::Parser;
use pest::iterators::Pair;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "loom.pest"]
pub struct LoomParser;

// Error wrapper that can give us spans
#[derive(Debug, Clone)]
pub struct ParseError {
    pub message: String,
    pub line: usize,
    pub col: usize,
}

pub fn parse(source_code: &str) -> Result<Program, Vec<ParseError>> {
    let mut ast_statements = Vec::new();
    let mut errors = Vec::new();
    let mut program_span = Span::default();

    match LoomParser::parse(Rule::Program, source_code) {
        Ok(mut pairs) => {
            // we have a valid parse at the top level
            if let Some(program_pair) = pairs.next() {
                program_span = pair_span(&program_pair);
                for statement_pair in program_pair.into_inner() {
                    match statement_pair.as_rule() {
                        Rule::Statement => match build_statement(statement_pair) {
                            Ok(stmt) => ast_statements.push(stmt),
                            Err(e) => errors.push(e),
                        },
                        Rule::COMMENT => {
                            let comment_text = statement_pair.as_str().to_string();
                            ast_statements.push(Statement::Comment(comment_text));
                        }
                        Rule::EOI => break,
                        _ => {}
                    }
                }
            }
        }
        Err(e) => {
            let (line, col) = match e.line_col {
                pest::error::LineColLocation::Pos((l, c)) => (l, c),
                pest::error::LineColLocation::Span((l, c), _) => (l, c),
            };
            errors.push(ParseError {
                message: format!("{}", e),
                line,
                col,
            });
        }
    }

    if errors.is_empty() {
        Ok(Program {
            statements: ast_statements,
            span: program_span,
        })
    } else {
        Err(errors)
    }
}

fn make_error(pair: &Pair<Rule>, msg: &str) -> ParseError {
    let (line, col) = pair.line_col();
    ParseError {
        message: msg.to_string(),
        line,
        col,
    }
}

fn missing_expected_error(pair: &Pair<Rule>, expected: &str) -> ParseError {
    make_error(pair, &format!("Missing expected {}", expected))
}

fn pair_span(pair: &Pair<Rule>) -> Span {
    let span = pair.as_span();
    let (sl, sc) = span.start_pos().line_col();
    let (el, ec) = span.end_pos().line_col();
    Span {
        start: SourcePos { line: sl, col: sc },
        end: SourcePos { line: el, col: ec },
    }
}

fn unescape_string_contents(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars();

    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }

        match chars.next() {
            Some('"') => out.push('"'),
            Some('\\') => out.push('\\'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('t') => out.push('\t'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }

    out
}

fn next_non_comment_or_error<'a>(
    iter: &mut impl Iterator<Item = Pair<'a, Rule>>,
    comments: &mut Vec<String>,
    context: &Pair<'a, Rule>,
    expected: &str,
) -> Result<Pair<'a, Rule>, ParseError> {
    next_non_comment(iter, comments).ok_or_else(|| missing_expected_error(context, expected))
}

fn build_statement(pair: Pair<Rule>) -> Result<Statement, ParseError> {
    let pair_for_err = pair.clone();
    let mut inner = pair.into_inner();
    let mut comments = Vec::new();
    let statement_inner =
        next_non_comment_or_error(&mut inner, &mut comments, &pair_for_err, "statement")?;

    match statement_inner.as_rule() {
        Rule::ImportStmt => Ok(Statement::Import(build_import(statement_inner, comments)?)),
        Rule::FunctionDef => Ok(Statement::Function(build_function(
            statement_inner,
            comments,
        )?)),
        Rule::PipeFlow => Ok(Statement::Pipe(build_pipe_flow(statement_inner, comments)?)),
        _ => Err(make_error(&statement_inner, "Expected valid statement")),
    }
}

fn next_non_comment<'a>(
    iter: &mut impl Iterator<Item = Pair<'a, Rule>>,
    comments: &mut Vec<String>,
) -> Option<Pair<'a, Rule>> {
    for pair in iter {
        if pair.as_rule() == Rule::COMMENT {
            comments.push(pair.as_str().to_string());
        } else {
            return Some(pair);
        }
    }
    None
}

fn build_import(pair: Pair<Rule>, comments: Vec<String>) -> Result<ImportStmt, ParseError> {
    let span = pair_span(&pair);
    let pair_for_err = pair.clone();
    let mut inner = pair.into_inner();
    let mut skipped_comments = Vec::new();
    let path_pair = next_non_comment_or_error(
        &mut inner,
        &mut skipped_comments,
        &pair_for_err,
        "import path literal",
    )?;
    let mut path_inner = path_pair.clone().into_inner();
    let path = path_inner
        .next()
        .ok_or_else(|| missing_expected_error(&path_pair, "import path string"))?
        .as_str()
        .to_string(); // PathLiteral -> InnerStr
    let alias = next_non_comment(&mut inner, &mut Vec::new()).map(|p| p.as_str().to_string());
    Ok(ImportStmt {
        path,
        alias,
        comments,
        span,
    })
}

fn build_function(pair: Pair<Rule>, comments: Vec<String>) -> Result<FunctionDef, ParseError> {
    let span = pair_span(&pair);
    let pair_for_err = pair.clone();
    let mut inner = pair.into_inner();
    let name =
        next_non_comment_or_error(&mut inner, &mut Vec::new(), &pair_for_err, "function name")?
            .as_str()
            .to_string();
    let mut parameters = Vec::new();
    let next = next_non_comment_or_error(
        &mut inner,
        &mut Vec::new(),
        &pair_for_err,
        "function body or params",
    )?;
    let body_pair = if next.as_rule() == Rule::ParamList {
        for param in next.into_inner() {
            if param.as_rule() != Rule::COMMENT {
                parameters.push(param.as_str().to_string());
            }
        }
        next_non_comment_or_error(&mut inner, &mut Vec::new(), &pair_for_err, "function body")?
    } else {
        next
    };
    let body = build_flow_or_branch(body_pair)?;
    Ok(FunctionDef {
        name,
        parameters,
        body,
        comments,
        span,
    })
}

fn build_flow_or_branch(pair: Pair<Rule>) -> Result<FlowOrBranch, ParseError> {
    let pair_for_err = pair.clone();
    let inner = next_non_comment_or_error(
        &mut pair.into_inner(),
        &mut Vec::new(),
        &pair_for_err,
        "flow or branch",
    )?;
    match inner.as_rule() {
        Rule::Branch => Ok(FlowOrBranch::Branch(build_branch(inner)?)),
        Rule::PipeFlow => Ok(FlowOrBranch::Flow(Box::new(build_pipe_flow(
            inner,
            Vec::new(),
        )?))),
        _ => Err(make_error(&inner, "Expected flow or branch")),
    }
}

fn build_branch(pair: Pair<Rule>) -> Result<Branch, ParseError> {
    let span = pair_span(&pair);
    let mut items = Vec::new();
    let mut comments = Vec::new();
    let mut inner = pair.into_inner();

    while let Some(next) = next_non_comment(&mut inner, &mut comments) {
        for comment in comments.drain(..) {
            items.push(BranchItem::Comment(comment));
        }
        items.push(BranchItem::Flow(Box::new(build_pipe_flow(
            next,
            Vec::new(),
        )?)));
    }
    // Handle any trailing comments
    for comment in comments {
        items.push(BranchItem::Comment(comment));
    }

    Ok(Branch { items, span })
}

fn build_pipe_flow(pair: Pair<Rule>, mut comments: Vec<String>) -> Result<PipeFlow, ParseError> {
    let span = pair_span(&pair);
    let pair_for_err = pair.clone();
    let mut inner = pair.into_inner();
    let source = build_source(next_non_comment_or_error(
        &mut inner,
        &mut comments,
        &pair_for_err,
        "flow source",
    )?)?;

    let mut operations = Vec::new();
    let mut on_fail = None;

    while let Some(next) = next_non_comment(&mut inner, &mut comments) {
        match next.as_rule() {
            Rule::PipeOp => {
                let op = match next.as_str() {
                    ">>>" => PipeOp::Force,
                    "->" => PipeOp::Move,
                    _ => PipeOp::Safe,
                };
                let maybe_next = next_non_comment_or_error(
                    &mut inner,
                    &mut comments,
                    &pair_for_err,
                    "pipe destination or on_fail handler",
                )?;
                if maybe_next.as_rule() == Rule::FlowOrBranch {
                    // on_fail supports an optional leading pipe op: on_fail >> <flow-or-branch>
                    let on_fail_span = pair_span(&maybe_next);
                    let handler = Box::new(build_flow_or_branch(maybe_next)?);
                    on_fail = Some(OnFail {
                        alias: None,
                        handler,
                        span: on_fail_span,
                    });
                } else {
                    let dest = build_destination(maybe_next)?;
                    operations.push((op, dest));
                }
            }
            Rule::Identifier => {
                // alias for on_fail
                let alias = Some(next.as_str().to_string());
                let next_piece = next_non_comment_or_error(
                    &mut inner,
                    &mut comments,
                    &pair_for_err,
                    "on_fail handler",
                )?;
                let handler_pair = if next_piece.as_rule() == Rule::PipeOp {
                    // on_fail as err >> <flow-or-branch>
                    next_non_comment_or_error(
                        &mut inner,
                        &mut comments,
                        &pair_for_err,
                        "on_fail flow or branch after pipe op",
                    )?
                } else {
                    next_piece
                };
                let on_fail_span = pair_span(&handler_pair);
                let handler = Box::new(build_flow_or_branch(handler_pair)?);
                on_fail = Some(OnFail {
                    alias,
                    handler,
                    span: on_fail_span,
                });
            }
            Rule::FlowOrBranch => {
                // on_fail without alias
                let on_fail_span = pair_span(&next);
                let handler = Box::new(build_flow_or_branch(next)?);
                on_fail = Some(OnFail {
                    alias: None,
                    handler,
                    span: on_fail_span,
                });
            }
            _ => return Err(make_error(&next, "Unexpected token in pipe flow")),
        }
    }

    Ok(PipeFlow {
        source,
        operations,
        on_fail,
        comments,
        span,
    })
}

fn build_source(pair: Pair<Rule>) -> Result<Source, ParseError> {
    let pair_for_err = pair.clone();
    let inner = next_non_comment_or_error(
        &mut pair.into_inner(),
        &mut Vec::new(),
        &pair_for_err,
        "source expression",
    )?;
    match inner.as_rule() {
        Rule::DirectiveFlow => Ok(Source::Directive(build_directive_flow(inner)?)),
        Rule::FunctionCall => Ok(Source::FunctionCall(build_function_call(inner)?)),
        Rule::NonLambdaExpression => {
            let inner_for_err = inner.clone();
            let expr_inner = next_non_comment_or_error(
                &mut inner.into_inner(),
                &mut Vec::new(),
                &inner_for_err,
                "source expression value",
            )?;
            Ok(Source::Expression(build_expression_part(expr_inner)?))
        }
        _ => Err(make_error(&inner, "Expected source")),
    }
}

fn build_destination(pair: Pair<Rule>) -> Result<Destination, ParseError> {
    let pair_for_err = pair.clone();
    let inner = next_non_comment_or_error(
        &mut pair.into_inner(),
        &mut Vec::new(),
        &pair_for_err,
        "destination expression",
    )?;
    match inner.as_rule() {
        Rule::Branch => Ok(Destination::Branch(build_branch(inner)?)),
        Rule::DirectiveFlow => Ok(Destination::Directive(build_directive_flow(inner)?)),
        Rule::FunctionCall => Ok(Destination::FunctionCall(build_function_call(inner)?)),
        Rule::NonLambdaExpression => {
            let inner_for_err = inner.clone();
            let expr_inner = next_non_comment_or_error(
                &mut inner.into_inner(),
                &mut Vec::new(),
                &inner_for_err,
                "destination expression value",
            )?;
            Ok(Destination::Expression(build_expression_part(expr_inner)?))
        }
        _ => Err(make_error(&inner, "Expected destination")),
    }
}

fn build_function_call(pair: Pair<Rule>) -> Result<FunctionCall, ParseError> {
    let span = pair_span(&pair);
    let pair_for_err = pair.clone();
    let mut inner = pair.into_inner();
    let name = next_non_comment_or_error(
        &mut inner,
        &mut Vec::new(),
        &pair_for_err,
        "function call name",
    )?
    .as_str()
    .to_string();
    let mut arguments = Vec::new();
    let mut named_arguments = Vec::new();
    let mut alias = None;
    while let Some(next) = next_non_comment(&mut inner, &mut Vec::new()) {
        match next.as_rule() {
            Rule::CallArguments => {
                append_call_arguments(next, &mut arguments, &mut named_arguments)?;
            }
            Rule::Identifier => {
                alias = Some(next.as_str().to_string());
            }
            _ => (),
        }
    }
    Ok(FunctionCall {
        name,
        arguments,
        named_arguments,
        alias,
        span,
    })
}

fn build_directive_flow(pair: Pair<Rule>) -> Result<DirectiveFlow, ParseError> {
    let span = pair_span(&pair);
    let pair_for_err = pair.clone();
    let mut inner = pair.into_inner();
    let name =
        next_non_comment_or_error(&mut inner, &mut Vec::new(), &pair_for_err, "directive name")?
            .as_str()
            .to_string();
    let mut arguments = Vec::new();
    let mut named_arguments = Vec::new();
    let mut alias = None;

    while let Some(next) = next_non_comment(&mut inner, &mut Vec::new()) {
        match next.as_rule() {
            Rule::CallArguments => {
                append_call_arguments(next, &mut arguments, &mut named_arguments)?;
            }
            Rule::Identifier => {
                alias = Some(next.as_str().to_string());
            }
            _ => (),
        }
    }
    Ok(DirectiveFlow {
        name,
        arguments,
        named_arguments,
        alias,
        span,
    })
}

fn build_secret_expression(pair: Pair<Rule>) -> Result<Expression, ParseError> {
    let span = pair_span(&pair);
    let pair_for_err = pair.clone();
    let mut inner = pair.into_inner();
    let mut arguments = Vec::new();
    let mut named_arguments = Vec::new();

    while let Some(next) = next_non_comment(&mut inner, &mut Vec::new()) {
        if next.as_rule() != Rule::CallArguments {
            continue;
        }
        append_call_arguments(next, &mut arguments, &mut named_arguments)?;
    }

    if !named_arguments.is_empty() {
        return Err(make_error(
            &pair_for_err,
            "@secret does not support named arguments",
        ));
    }
    if arguments.len() != 1 {
        return Err(make_error(
            &pair_for_err,
            "@secret expects exactly one argument",
        ));
    }

    Ok(Expression::SecretCall(SecretCall {
        arguments,
        named_arguments,
        span,
    }))
}

fn append_call_arguments(
    call_arguments: Pair<Rule>,
    positional: &mut Vec<Expression>,
    named: &mut Vec<NamedArgument>,
) -> Result<(), ParseError> {
    let mut arg_inner = call_arguments.into_inner();
    while let Some(arg) = next_non_comment(&mut arg_inner, &mut Vec::new()) {
        let arg = if arg.as_rule() == Rule::CallArgument {
            let arg_for_err = arg.clone();
            next_non_comment_or_error(
                &mut arg.into_inner(),
                &mut Vec::new(),
                &arg_for_err,
                "call argument",
            )?
        } else {
            arg
        };
        match arg.as_rule() {
            Rule::NamedArgument => named.push(parse_named_argument(arg)?),
            _ => positional.push(build_expression(arg)?),
        }
    }
    Ok(())
}

fn parse_named_argument(named_arg: Pair<Rule>) -> Result<NamedArgument, ParseError> {
    let named_pair_for_err = named_arg.clone();
    let mut named_inner = named_arg.into_inner();
    let name = next_non_comment_or_error(
        &mut named_inner,
        &mut Vec::new(),
        &named_pair_for_err,
        "named argument name",
    )?
    .as_str()
    .to_string();
    let value = build_expression(next_non_comment_or_error(
        &mut named_inner,
        &mut Vec::new(),
        &named_pair_for_err,
        "named argument value",
    )?)?;
    Ok(NamedArgument { name, value })
}

fn build_expression(pair: Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_for_err = pair.clone();
    let inner = next_non_comment_or_error(
        &mut pair.into_inner(),
        &mut Vec::new(),
        &pair_for_err,
        "expression",
    )?;
    match inner.as_rule() {
        Rule::Lambda => {
            let lambda_span = pair_span(&inner);
            let inner_for_err = inner.clone();
            let mut l_inner = inner.into_inner();
            let param = next_non_comment_or_error(
                &mut l_inner,
                &mut Vec::new(),
                &inner_for_err,
                "lambda parameter",
            )?
            .as_str()
            .to_string();
            let body = Box::new(build_expression_part(next_non_comment_or_error(
                &mut l_inner,
                &mut Vec::new(),
                &inner_for_err,
                "lambda body",
            )?)?);
            Ok(Expression::Lambda(Lambda {
                param,
                body,
                span: lambda_span,
            }))
        }
        Rule::BinExpr => build_expression_part(inner),
        Rule::UnaryExpr => build_expression_part(inner),
        Rule::SecretExpr => build_expression_part(inner),
        Rule::FunctionCall => build_expression_part(inner),
        Rule::MemberAccess => build_expression_part(inner),
        Rule::ObjectLiteral => build_expression_part(inner),
        Rule::Literal => build_expression_part(inner),
        Rule::Identifier => build_expression_part(inner),
        _ => Err(make_error(&inner, "Expected expression")),
    }
}

// BinExpr, UnaryExpr, MemberAccess, Literal, Identifier are expression parts
fn build_expression_part(pair: Pair<Rule>) -> Result<Expression, ParseError> {
    match pair.as_rule() {
        Rule::BinExpr => {
            let pair_for_err = pair.clone();
            let mut inner = pair.into_inner();
            let left = Box::new(build_expression_part(next_non_comment_or_error(
                &mut inner,
                &mut Vec::new(),
                &pair_for_err,
                "left side of binary expression",
            )?)?);
            let op = next_non_comment_or_error(
                &mut inner,
                &mut Vec::new(),
                &pair_for_err,
                "binary operator",
            )?
            .as_str()
            .to_string();
            let right = Box::new(build_expression(next_non_comment_or_error(
                &mut inner,
                &mut Vec::new(),
                &pair_for_err,
                "right side of binary expression",
            )?)?);
            Ok(fix_precedence(Expression::BinaryOp(left, op, right)))
        }
        Rule::UnaryExpr => {
            let pair_for_err = pair.clone();
            let inner = next_non_comment_or_error(
                &mut pair.into_inner(),
                &mut Vec::new(),
                &pair_for_err,
                "unary expression body",
            )?;
            let expr = Box::new(build_expression_part(inner)?);
            Ok(Expression::UnaryOp("!".to_string(), expr))
        }
        Rule::SecretExpr => build_secret_expression(pair),
        Rule::FunctionCall => Ok(Expression::FunctionCall(build_function_call(pair)?)),
        Rule::MemberAccess => {
            let parts = pair
                .into_inner()
                .filter(|p| p.as_rule() != Rule::COMMENT)
                .map(|p| p.as_str().to_string())
                .collect::<Vec<_>>();
            Ok(Expression::MemberAccess(parts))
        }
        Rule::ObjectLiteral => {
            let mut entries = Vec::new();
            for entry in pair.into_inner() {
                if entry.as_rule() != Rule::ObjectEntry {
                    continue;
                }
                let entry_for_err = entry.clone();
                let mut inner = entry.into_inner();
                let key_pair = next_non_comment_or_error(
                    &mut inner,
                    &mut Vec::new(),
                    &entry_for_err,
                    "object key",
                )?;
                let key_pair = if key_pair.as_rule() == Rule::ObjectKey {
                    let key_for_err = key_pair.clone();
                    next_non_comment_or_error(
                        &mut key_pair.into_inner(),
                        &mut Vec::new(),
                        &key_for_err,
                        "object key value",
                    )?
                } else {
                    key_pair
                };
                let key = match key_pair.as_rule() {
                    Rule::Identifier => ObjectKey::Identifier(key_pair.as_str().to_string()),
                    Rule::StringLiteral | Rule::PathLiteral => {
                        let key_for_err = key_pair.clone();
                        let mut key_inner = key_pair.into_inner();
                        let raw = next_non_comment_or_error(
                            &mut key_inner,
                            &mut Vec::new(),
                            &key_for_err,
                            "object string key body",
                        )?
                        .as_str()
                        .to_string();
                        if key_for_err.as_rule() == Rule::StringLiteral {
                            ObjectKey::String(unescape_string_contents(&raw))
                        } else {
                            ObjectKey::Path(raw)
                        }
                    }
                    _ => return Err(make_error(&key_pair, "Invalid object key")),
                };
                let value = build_expression(next_non_comment_or_error(
                    &mut inner,
                    &mut Vec::new(),
                    &entry_for_err,
                    "object value",
                )?)?;
                entries.push((key, value));
            }
            Ok(Expression::ObjectLiteral(entries))
        }
        Rule::Literal => {
            let pair_for_err = pair.clone();
            let inner = next_non_comment_or_error(
                &mut pair.into_inner(),
                &mut Vec::new(),
                &pair_for_err,
                "literal value",
            )?;
            match inner.as_rule() {
                Rule::StringLiteral => {
                    let inner_for_err = inner.clone();
                    let mut lit_inner = inner.into_inner();
                    let s = next_non_comment_or_error(
                        &mut lit_inner,
                        &mut Vec::new(),
                        &inner_for_err,
                        "string literal body",
                    )?
                    .as_str()
                    .to_string();
                    Ok(Expression::Literal(Literal::String(
                        unescape_string_contents(&s),
                    )))
                }
                Rule::PathLiteral => {
                    let inner_for_err = inner.clone();
                    let mut lit_inner = inner.into_inner();
                    let s = next_non_comment_or_error(
                        &mut lit_inner,
                        &mut Vec::new(),
                        &inner_for_err,
                        "path literal body",
                    )?
                    .as_str()
                    .to_string();
                    Ok(Expression::Literal(Literal::Path(s)))
                }
                Rule::Number => {
                    let n = inner
                        .as_str()
                        .parse()
                        .map_err(|_| make_error(&inner, "Invalid numeric literal"))?;
                    Ok(Expression::Literal(Literal::Number(n)))
                }
                Rule::Boolean => {
                    let b = inner.as_str() == "true";
                    Ok(Expression::Literal(Literal::Boolean(b)))
                }
                _ => Err(make_error(&inner, "Expected valid literal")),
            }
        }
        Rule::Identifier => Ok(Expression::Identifier(pair.as_str().to_string())),
        _ => Err(make_error(
            &pair,
            &format!("Unexpected expression part: {:?}", pair.as_rule()),
        )),
    }
}

/// Operator precedence (higher number = tighter binding).
fn op_precedence(op: &str) -> u8 {
    match op {
        "||" => 1,
        "&&" => 2,
        "==" | "!=" => 3,
        ">" | "<" | ">=" | "<=" => 4,
        "+" | "-" => 5,
        "*" | "/" => 6,
        _ => 0,
    }
}

/// Recursively fix operator precedence in a BinaryOp tree.
///
/// The PEG grammar produces right-associative trees:
///   `a == b && c == d` → `BinOp(a, ==, BinOp(b, &&, BinOp(c, ==, d)))`
///
/// This rotates into the correct precedence:
///   `BinOp(BinOp(a, ==, b), &&, BinOp(c, ==, d))`
fn fix_precedence(expr: Expression) -> Expression {
    match expr {
        Expression::BinaryOp(left, op, right) => {
            let left = fix_precedence(*left);
            let right = fix_precedence(*right);
            rotate_if_needed(left, op, right)
        }
        other => other,
    }
}

fn rotate_if_needed(left: Expression, op: String, right: Expression) -> Expression {
    match right {
        Expression::BinaryOp(mid, right_op, right_right)
            if op_precedence(&op) > op_precedence(&right_op) =>
        {
            // left op_high (mid op_low right_right) → (left op_high mid) op_low right_right
            let new_left = rotate_if_needed(left, op, *mid);
            rotate_if_needed(new_left, right_op, *right_right)
        }
        _ => Expression::BinaryOp(Box::new(left), op, Box::new(right)),
    }
}

#[cfg(test)]
#[path = "../tests/unit/parser_tests.rs"]
mod parser_tests;
