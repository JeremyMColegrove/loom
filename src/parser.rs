use pest::Parser;
use pest::iterators::Pair;
use pest_derive::Parser;
use crate::ast::*;

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
                        Rule::Statement => {
                            match build_statement(statement_pair) {
                                Ok(stmt) => ast_statements.push(stmt),
                                Err(e) => errors.push(e),
                            }
                        }
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
        col
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
        Rule::FunctionDef => Ok(Statement::Function(build_function(statement_inner, comments)?)),
        Rule::PipeFlow => Ok(Statement::Pipe(build_pipe_flow(statement_inner, comments)?)),
        _ => Err(make_error(&statement_inner, "Expected valid statement"))
    }
}

fn next_non_comment<'a>(iter: &mut impl Iterator<Item = Pair<'a, Rule>>, comments: &mut Vec<String>) -> Option<Pair<'a, Rule>> {
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
    let path_pair = next_non_comment_or_error(&mut inner, &mut skipped_comments, &pair_for_err, "import path literal")?;
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
    let name = next_non_comment_or_error(&mut inner, &mut Vec::new(), &pair_for_err, "function name")?
        .as_str()
        .to_string();
    let mut parameters = Vec::new();
    let next = next_non_comment_or_error(&mut inner, &mut Vec::new(), &pair_for_err, "function body or params")?;
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
    let inner = next_non_comment_or_error(&mut pair.into_inner(), &mut Vec::new(), &pair_for_err, "flow or branch")?;
    match inner.as_rule() {
        Rule::Branch => Ok(FlowOrBranch::Branch(build_branch(inner)?)),
        Rule::PipeFlow => Ok(FlowOrBranch::Flow(build_pipe_flow(inner, Vec::new())?)),
        _ => Err(make_error(&inner, "Expected flow or branch"))
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
        items.push(BranchItem::Flow(build_pipe_flow(next, Vec::new())?));
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
    let source = build_source(next_non_comment_or_error(&mut inner, &mut comments, &pair_for_err, "flow source")?)?;
    
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
                let maybe_next = next_non_comment_or_error(&mut inner, &mut comments, &pair_for_err, "pipe destination or on_fail handler")?;
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
            Rule::Identifier => { // alias for on_fail
                let alias = Some(next.as_str().to_string());
                let next_piece = next_non_comment_or_error(&mut inner, &mut comments, &pair_for_err, "on_fail handler")?;
                let handler_pair = if next_piece.as_rule() == Rule::PipeOp {
                    // on_fail as err >> <flow-or-branch>
                    next_non_comment_or_error(&mut inner, &mut comments, &pair_for_err, "on_fail flow or branch after pipe op")?
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
            Rule::FlowOrBranch => { // on_fail without alias
                let on_fail_span = pair_span(&next);
                let handler = Box::new(build_flow_or_branch(next)?);
                on_fail = Some(OnFail {
                    alias: None,
                    handler,
                    span: on_fail_span,
                });
            }
            _ => return Err(make_error(&next, "Unexpected token in pipe flow"))
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
    let inner = next_non_comment_or_error(&mut pair.into_inner(), &mut Vec::new(), &pair_for_err, "source expression")?;
    match inner.as_rule() {
        Rule::DirectiveFlow => Ok(Source::Directive(build_directive_flow(inner)?)),
        Rule::FunctionCall => Ok(Source::FunctionCall(build_function_call(inner)?)),
        Rule::NonLambdaExpression => {
            let inner_for_err = inner.clone();
            let expr_inner = next_non_comment_or_error(&mut inner.into_inner(), &mut Vec::new(), &inner_for_err, "source expression value")?;
            Ok(Source::Expression(build_expression_part(expr_inner)?))
        }
        _ => Err(make_error(&inner, "Expected source"))
    }
}

fn build_destination(pair: Pair<Rule>) -> Result<Destination, ParseError> {
    let pair_for_err = pair.clone();
    let inner = next_non_comment_or_error(&mut pair.into_inner(), &mut Vec::new(), &pair_for_err, "destination expression")?;
    match inner.as_rule() {
        Rule::Branch => Ok(Destination::Branch(build_branch(inner)?)),
        Rule::DirectiveFlow => Ok(Destination::Directive(build_directive_flow(inner)?)),
        Rule::FunctionCall => Ok(Destination::FunctionCall(build_function_call(inner)?)),
        Rule::NonLambdaExpression => {
            let inner_for_err = inner.clone();
            let expr_inner = next_non_comment_or_error(&mut inner.into_inner(), &mut Vec::new(), &inner_for_err, "destination expression value")?;
            Ok(Destination::Expression(build_expression_part(expr_inner)?))
        }
        _ => Err(make_error(&inner, "Expected destination"))
    }
}

fn build_function_call(pair: Pair<Rule>) -> Result<FunctionCall, ParseError> {
    let span = pair_span(&pair);
    let pair_for_err = pair.clone();
    let mut inner = pair.into_inner();
    let name = next_non_comment_or_error(&mut inner, &mut Vec::new(), &pair_for_err, "function call name")?
        .as_str()
        .to_string();
    let mut arguments = Vec::new();
    let mut alias = None;
    while let Some(next) = next_non_comment(&mut inner, &mut Vec::new()) {
        match next.as_rule() {
            Rule::Arguments => {
                let mut arg_inner = next.into_inner();
                while let Some(arg) = next_non_comment(&mut arg_inner, &mut Vec::new()) {
                    arguments.push(build_expression(arg)?);
                }
            }
            Rule::Identifier => {
                alias = Some(next.as_str().to_string());
            }
            _ => ()
        }
    }
    Ok(FunctionCall {
        name,
        arguments,
        alias,
        span,
    })
}

fn build_directive_flow(pair: Pair<Rule>) -> Result<DirectiveFlow, ParseError> {
    let span = pair_span(&pair);
    let pair_for_err = pair.clone();
    let mut inner = pair.into_inner();
    let name = next_non_comment_or_error(&mut inner, &mut Vec::new(), &pair_for_err, "directive name")?
        .as_str()
        .to_string();
    let mut arguments = Vec::new();
    let mut alias = None;
    
    while let Some(next) = next_non_comment(&mut inner, &mut Vec::new()) {
        match next.as_rule() {
            Rule::Arguments => {
                let mut arg_inner = next.into_inner();
                while let Some(arg) = next_non_comment(&mut arg_inner, &mut Vec::new()) {
                    arguments.push(build_expression(arg)?);
                }
            }
            Rule::Identifier => {
                alias = Some(next.as_str().to_string());
            }
            _ => ()
        }
    }
    Ok(DirectiveFlow {
        name,
        arguments,
        alias,
        span,
    })
}

fn build_expression(pair: Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_for_err = pair.clone();
    let inner = next_non_comment_or_error(&mut pair.into_inner(), &mut Vec::new(), &pair_for_err, "expression")?;
    match inner.as_rule() {
        Rule::Lambda => {
            let lambda_span = pair_span(&inner);
            let inner_for_err = inner.clone();
            let mut l_inner = inner.into_inner();
            let param = next_non_comment_or_error(&mut l_inner, &mut Vec::new(), &inner_for_err, "lambda parameter")?
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
        Rule::FunctionCall => build_expression_part(inner),
        Rule::MemberAccess => build_expression_part(inner),
        Rule::Literal => build_expression_part(inner),
        Rule::Identifier => build_expression_part(inner),
        _ => Err(make_error(&inner, "Expected expression"))
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
            let op = next_non_comment_or_error(&mut inner, &mut Vec::new(), &pair_for_err, "binary operator")?
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
            let inner = next_non_comment_or_error(&mut pair.into_inner(), &mut Vec::new(), &pair_for_err, "unary expression body")?;
            let expr = Box::new(build_expression_part(inner)?);
            Ok(Expression::UnaryOp("!".to_string(), expr))
        }
        Rule::FunctionCall => {
            Ok(Expression::FunctionCall(build_function_call(pair)?))
        }
        Rule::MemberAccess => {
            let parts = pair
                .into_inner()
                .filter(|p| p.as_rule() != Rule::COMMENT)
                .map(|p| p.as_str().to_string())
                .collect::<Vec<_>>();
            Ok(Expression::MemberAccess(parts))
        }
        Rule::Literal => {
            let pair_for_err = pair.clone();
            let inner = next_non_comment_or_error(&mut pair.into_inner(), &mut Vec::new(), &pair_for_err, "literal value")?;
            match inner.as_rule() {
                Rule::StringLiteral => {
                    let inner_for_err = inner.clone();
                    let mut lit_inner = inner.into_inner();
                    let s = next_non_comment_or_error(&mut lit_inner, &mut Vec::new(), &inner_for_err, "string literal body")?
                        .as_str()
                        .to_string();
                    Ok(Expression::Literal(Literal::String(s)))
                }
                Rule::PathLiteral => {
                    let inner_for_err = inner.clone();
                    let mut lit_inner = inner.into_inner();
                    let s = next_non_comment_or_error(&mut lit_inner, &mut Vec::new(), &inner_for_err, "path literal body")?
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
                _ => Err(make_error(&inner, "Expected valid literal"))
            }
        }
        Rule::Identifier => {
            Ok(Expression::Identifier(pair.as_str().to_string()))
        }
        _ => Err(make_error(&pair, &format!("Unexpected expression part: {:?}", pair.as_rule())))
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
mod tests {
    use super::*;

    #[test]
    fn parses_path_literal_without_prefix() {
        let program = parse("\"hello-world.txt\" >> output").expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else {
            panic!("expected pipe statement");
        };
        let Source::Expression(Expression::Literal(Literal::Path(path))) = &flow.source else {
            panic!("expected path literal source");
        };
        assert_eq!(path, "hello-world.txt");
    }

    #[test]
    fn parses_string_literal_with_escaped_quote_prefix() {
        let program = parse(r#"\"hello-world.txt" >> output"#).expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else {
            panic!("expected pipe statement");
        };
        let Source::Expression(Expression::Literal(Literal::String(text))) = &flow.source else {
            panic!("expected string literal source");
        };
        assert_eq!(text, "hello-world.txt");
    }

    #[test]
    fn parses_qualified_function_call() {
        let program = parse(r#"\"x" >> mt.resize(800)"#).expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else {
            panic!("expected pipe statement");
        };
        let (_, Destination::FunctionCall(call)) = &flow.operations[0] else {
            panic!("expected function-call destination");
        };
        assert_eq!(call.name, "mt.resize");
    }

    #[test]
    fn parses_nested_member_access() {
        let program = parse("event.file.path >> out").expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else {
            panic!("expected pipe statement");
        };
        let Source::Expression(Expression::MemberAccess(parts)) = &flow.source else {
            panic!("expected member-access source");
        };
        assert_eq!(parts, &vec!["event".to_string(), "file".to_string(), "path".to_string()]);
    }

    #[test]
    fn parses_filter_lambda_with_comparison_operator() {
        let program = parse("\"customers.csv\" >> csv.parse >> @filter(row >> row.Index > 90) >> \"high.txt\"")
            .expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else {
            panic!("expected pipe statement");
        };
        assert_eq!(flow.operations.len(), 3);
        let (_, Destination::Directive(filter)) = &flow.operations[1] else {
            panic!("expected filter directive destination");
        };
        let Some(Expression::Lambda(lambda)) = filter.arguments.first() else {
            panic!("expected lambda argument for filter");
        };
        let Expression::BinaryOp(_, op, _) = lambda.body.as_ref() else {
            panic!("expected binary expression in filter lambda");
        };
        assert_eq!(op, ">");
    }

    #[test]
    fn parses_filter_lambda_with_logical_and_operator() {
        let program = parse("\"customers.csv\" >> csv.parse >> @filter(row >> 1 && 2) >> \"high.txt\"")
            .expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else {
            panic!("expected pipe statement");
        };
        let (_, Destination::Directive(filter)) = &flow.operations[1] else {
            panic!("expected filter directive destination");
        };
        let Some(Expression::Lambda(lambda)) = filter.arguments.first() else {
            panic!("expected lambda argument for filter");
        };
        let Expression::BinaryOp(_, op, _) = lambda.body.as_ref() else {
            panic!("expected binary expression in filter lambda");
        };
        assert_eq!(op, "&&");
    }

    #[test]
    fn parses_read_then_csv_parse_in_branch() {
        let src = r#"x >> [
            @read(event.file.path) >> @csv.parse as data >> [
                stdout
            ]
        ]"#;
        let program = parse(src).expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else {
            panic!("expected pipe statement");
        };
        // x >> [branch]
        assert_eq!(flow.operations.len(), 1);
        let (_, Destination::Branch(branch)) = &flow.operations[0] else {
            panic!("expected branch destination");
        };
        // Inside the branch: @read(event.file.path) >> @csv.parse as data >> [...]
        let inner_item = &branch.items[0];
        let BranchItem::Flow(inner_flow) = inner_item else {
            panic!("expected flow in branch item");
        };
        let Source::Directive(read_dir) = &inner_flow.source else {
            panic!("expected @read directive as source, got {:?}", inner_flow.source);
        };
        assert_eq!(read_dir.name, "read");
        // Should have 2 operations: >> @csv.parse as data, >> [...]
        assert_eq!(inner_flow.operations.len(), 2, "expected 2 operations, got: {:?}", inner_flow.operations);
        let (_, Destination::Directive(csv_dir)) = &inner_flow.operations[0] else {
            panic!("expected @csv.parse directive destination, got: {:?}", inner_flow.operations[0]);
        };
        assert_eq!(csv_dir.name, "csv.parse");
        assert_eq!(csv_dir.alias, Some("data".to_string()));
    }

    // ── Operator Precedence ──────────────────────────────────────────────

    #[test]
    fn precedence_multiply_binds_tighter_than_add() {
        // a + b * c should become BinOp(a, +, BinOp(b, *, c))
        let program = parse("x >> filter(a + b * c)").expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else { panic!("expected pipe"); };
        let (_, Destination::FunctionCall(call)) = &flow.operations[0] else { panic!("expected call"); };
        let Expression::BinaryOp(left, op, right) = &call.arguments[0] else { panic!("expected binop"); };
        assert_eq!(op, "+");
        assert!(matches!(left.as_ref(), Expression::Identifier(n) if n == "a"));
        let Expression::BinaryOp(_, inner_op, _) = right.as_ref() else { panic!("expected inner binop"); };
        assert_eq!(inner_op, "*");
    }

    #[test]
    fn precedence_equality_binds_tighter_than_logical_and() {
        // a == b && c == d → BinOp(BinOp(a,==,b), &&, BinOp(c,==,d))
        let program = parse("x >> filter(a == b && c == d)").expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else { panic!("expected pipe"); };
        let (_, Destination::FunctionCall(call)) = &flow.operations[0] else { panic!("expected call"); };
        let Expression::BinaryOp(left, op, right) = &call.arguments[0] else { panic!("expected binop"); };
        assert_eq!(op, "&&");
        let Expression::BinaryOp(_, left_op, _) = left.as_ref() else { panic!("expected left binop"); };
        assert_eq!(left_op, "==");
        let Expression::BinaryOp(_, right_op, _) = right.as_ref() else { panic!("expected right binop"); };
        assert_eq!(right_op, "==");
    }

    #[test]
    fn precedence_and_binds_tighter_than_or() {
        // a || b && c → BinOp(a, ||, BinOp(b, &&, c))
        let program = parse("x >> filter(a || b && c)").expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else { panic!("expected pipe"); };
        let (_, Destination::FunctionCall(call)) = &flow.operations[0] else { panic!("expected call"); };
        let Expression::BinaryOp(left, op, right) = &call.arguments[0] else { panic!("expected binop"); };
        assert_eq!(op, "||");
        assert!(matches!(left.as_ref(), Expression::Identifier(n) if n == "a"));
        let Expression::BinaryOp(_, inner_op, _) = right.as_ref() else { panic!("expected inner binop"); };
        assert_eq!(inner_op, "&&");
    }

    #[test]
    fn precedence_triple_level_chain() {
        // a > b && c < d || e == f
        // Expected: BinOp(BinOp(BinOp(a,>,b), &&, BinOp(c,<,d)), ||, BinOp(e,==,f))
        let program = parse("x >> filter(a > b && c < d || e == f)").expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else { panic!("expected pipe"); };
        let (_, Destination::FunctionCall(call)) = &flow.operations[0] else { panic!("expected call"); };
        let Expression::BinaryOp(_, top_op, _) = &call.arguments[0] else { panic!("expected binop"); };
        assert_eq!(top_op, "||");
    }

    #[test]
    fn precedence_arithmetic_then_comparison_then_logical() {
        // a + b > c && d → BinOp(BinOp(BinOp(a,+,b), >, c), &&, d)
        let program = parse("x >> filter(a + b > c && d)").expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else { panic!("expected pipe"); };
        let (_, Destination::FunctionCall(call)) = &flow.operations[0] else { panic!("expected call"); };
        let Expression::BinaryOp(_, top_op, _) = &call.arguments[0] else { panic!("expected binop"); };
        assert_eq!(top_op, "&&");
    }

    // ── Complex Expression Parsing ───────────────────────────────────────

    #[test]
    fn parses_deeply_nested_member_access() {
        let program = parse("a.b.c.d.e >> out").expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else { panic!("expected pipe"); };
        let Source::Expression(Expression::MemberAccess(parts)) = &flow.source else {
            panic!("expected member access source");
        };
        assert_eq!(parts, &["a", "b", "c", "d", "e"]);
    }

    #[test]
    fn parses_unary_not_on_member_access() {
        let program = parse("x >> filter(!event.active)").expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else { panic!("expected pipe"); };
        let (_, Destination::FunctionCall(call)) = &flow.operations[0] else { panic!("expected call"); };
        let Expression::UnaryOp(op, inner) = &call.arguments[0] else { panic!("expected unary"); };
        assert_eq!(op, "!");
        assert!(matches!(inner.as_ref(), Expression::MemberAccess(_)));
    }

    #[test]
    fn parses_unary_not_on_function_call() {
        let program = parse("x >> filter(!is_valid(x))").expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else { panic!("expected pipe"); };
        let (_, Destination::FunctionCall(call)) = &flow.operations[0] else { panic!("expected call"); };
        let Expression::UnaryOp(op, inner) = &call.arguments[0] else { panic!("expected unary"); };
        assert_eq!(op, "!");
        assert!(matches!(inner.as_ref(), Expression::FunctionCall(_)));
    }

    #[test]
    fn parses_lambda_with_compound_condition() {
        // row >> row.id != null && row.price > 0
        let src = r#""data.csv" >> @filter(row >> row.id != null && row.price > 0) >> "out.csv""#;
        let program = parse(src).expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else { panic!("expected pipe"); };
        let (_, Destination::Directive(dir)) = &flow.operations[0] else { panic!("expected directive"); };
        let Some(Expression::Lambda(lambda)) = dir.arguments.first() else { panic!("expected lambda"); };
        assert_eq!(lambda.param, "row");
        // Top-level operator should be && after precedence fix
        let Expression::BinaryOp(_, top_op, _) = lambda.body.as_ref() else { panic!("expected binop"); };
        assert_eq!(top_op, "&&");
    }

    #[test]
    fn parses_nested_function_calls_in_expression() {
        let program = parse("x >> filter(outer(inner(x)))").expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else { panic!("expected pipe"); };
        let (_, Destination::FunctionCall(call)) = &flow.operations[0] else { panic!("expected call"); };
        let Expression::FunctionCall(outer) = &call.arguments[0] else { panic!("expected outer call"); };
        assert_eq!(outer.name, "outer");
        let Expression::FunctionCall(inner) = &outer.arguments[0] else { panic!("expected inner call"); };
        assert_eq!(inner.name, "inner");
    }

    #[test]
    fn parses_boolean_literals_as_function_args() {
        let program = parse("x >> func(true, false)").expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else { panic!("expected pipe"); };
        let (_, Destination::FunctionCall(call)) = &flow.operations[0] else { panic!("expected call"); };
        assert_eq!(call.arguments.len(), 2);
        assert!(matches!(&call.arguments[0], Expression::Literal(Literal::Boolean(true))));
        assert!(matches!(&call.arguments[1], Expression::Literal(Literal::Boolean(false))));
    }

    #[test]
    fn parses_negative_number() {
        let program = parse("-42.5 >> out").expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else { panic!("expected pipe"); };
        let Source::Expression(Expression::Literal(Literal::Number(n))) = &flow.source else {
            panic!("expected number literal source");
        };
        assert_eq!(*n, -42.5);
    }

    #[test]
    fn parses_string_concatenation_expression() {
        let program = parse(r#"x >> filter(\"hello" + \" world")"#).expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else { panic!("expected pipe"); };
        let (_, Destination::FunctionCall(call)) = &flow.operations[0] else { panic!("expected call"); };
        let Expression::BinaryOp(left, op, right) = &call.arguments[0] else { panic!("expected binop"); };
        assert_eq!(op, "+");
        assert!(matches!(left.as_ref(), Expression::Literal(Literal::String(s)) if s == "hello"));
        assert!(matches!(right.as_ref(), Expression::Literal(Literal::String(s)) if s == " world"));
    }

    // ── Import Parsing ───────────────────────────────────────────────────

    #[test]
    fn parses_import_without_alias() {
        let program = parse(r#"@import "utils""#).expect("parse should succeed");
        let Statement::Import(import) = &program.statements[0] else { panic!("expected import"); };
        assert_eq!(import.path, "utils");
        assert_eq!(import.alias, None);
    }

    #[test]
    fn parses_import_with_dotted_path_and_alias() {
        let program = parse(r#"@import "std.csv" as csv"#).expect("parse should succeed");
        let Statement::Import(import) = &program.statements[0] else { panic!("expected import"); };
        assert_eq!(import.path, "std.csv");
        assert_eq!(import.alias, Some("csv".to_string()));
    }

    #[test]
    fn parses_multiple_imports() {
        let src = r#"
            @import "std.csv" as csv
            @import "std.out" as stdout
            @import "logic" as util
        "#;
        let program = parse(src).expect("parse should succeed");
        assert_eq!(program.statements.len(), 3);
        assert!(program.statements.iter().all(|s| matches!(s, Statement::Import(_))));
    }

    // ── Function Definition Edge Cases ───────────────────────────────────

    #[test]
    fn parses_zero_parameter_function() {
        let program = parse(r#"greet() => \"hello" >> output"#).expect("parse should succeed");
        let Statement::Function(func) = &program.statements[0] else { panic!("expected function"); };
        assert_eq!(func.name, "greet");
        assert!(func.parameters.is_empty());
    }

    #[test]
    fn parses_multi_parameter_function() {
        let program = parse("add(a, b, c) => a").expect("parse should succeed");
        let Statement::Function(func) = &program.statements[0] else { panic!("expected function"); };
        assert_eq!(func.name, "add");
        assert_eq!(func.parameters, vec!["a", "b", "c"]);
    }

    #[test]
    fn parses_function_body_as_branch() {
        let src = r#"handler(x) => [x >> "out1.txt", x >> "out2.txt"]"#;
        let program = parse(src).expect("parse should succeed");
        let Statement::Function(func) = &program.statements[0] else { panic!("expected function"); };
        assert_eq!(func.name, "handler");
        assert!(matches!(func.body, FlowOrBranch::Branch(_)));
    }

    #[test]
    fn parses_function_with_binary_expr_body() {
        let program = parse("is_valid(row) => row.id != null && row.price > 0").expect("parse should succeed");
        let Statement::Function(func) = &program.statements[0] else { panic!("expected function"); };
        assert_eq!(func.name, "is_valid");
        assert_eq!(func.parameters, vec!["row"]);
        // Body should be a Flow whose source is a binary expression
        let FlowOrBranch::Flow(flow) = &func.body else { panic!("expected flow body"); };
        assert!(matches!(&flow.source, Source::Expression(Expression::BinaryOp(_, _, _))));
    }

    // ── Pipe Flow Edge Cases ─────────────────────────────────────────────

    #[test]
    fn parses_chained_pipes() {
        let program = parse("a >> b >> c >> d").expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else { panic!("expected pipe"); };
        assert_eq!(flow.operations.len(), 3);
        assert!(flow.operations.iter().all(|(op, _)| *op == PipeOp::Safe));
    }

    #[test]
    fn parses_mixed_pipe_operators() {
        let src = r#"a >> "b.txt" >>> "c.txt" -> "d/""#;
        let program = parse(src).expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else { panic!("expected pipe"); };
        assert_eq!(flow.operations.len(), 3);
        assert_eq!(flow.operations[0].0, PipeOp::Safe);
        assert_eq!(flow.operations[1].0, PipeOp::Force);
        assert_eq!(flow.operations[2].0, PipeOp::Move);
    }

    #[test]
    fn parses_empty_branch() {
        let program = parse("x >> []").expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else { panic!("expected pipe"); };
        let (_, Destination::Branch(branch)) = &flow.operations[0] else { panic!("expected branch"); };
        assert!(branch.items.is_empty());
    }

    #[test]
    fn parses_nested_branches() {
        let src = r#"x >> [a >> [b, c], d >> "out.txt"]"#;
        let program = parse(src).expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else { panic!("expected pipe"); };
        let (_, Destination::Branch(outer)) = &flow.operations[0] else { panic!("expected outer branch"); };
        assert_eq!(outer.items.len(), 2);
        // First flow's destination should be a nested branch
        let BranchItem::Flow(first_flow) = &outer.items[0] else { panic!("expected first branch item to be a flow"); };
        let (_, Destination::Branch(inner)) = &first_flow.operations[0] else {
            panic!("expected inner branch");
        };
        assert_eq!(inner.items.len(), 2);
    }

    #[test]
    fn parses_on_fail_without_alias() {
        let src = r#"x >> y on_fail >> "error.log""#;
        let program = parse(src).expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else { panic!("expected pipe"); };
        assert!(flow.on_fail.is_some());
        let on_fail = flow.on_fail.as_ref().unwrap();
        assert_eq!(on_fail.alias, None);
    }

    #[test]
    fn parses_on_fail_with_alias_and_branch() {
        let src = r#"x >> y on_fail as e >> [e >> "log.txt"]"#;
        let program = parse(src).expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else { panic!("expected pipe"); };
        assert!(flow.on_fail.is_some());
        let on_fail = flow.on_fail.as_ref().unwrap();
        assert_eq!(on_fail.alias, Some("e".to_string()));
        assert!(matches!(on_fail.handler.as_ref(), FlowOrBranch::Branch(_)));
    }

    #[test]
    fn parses_force_pipe_operator() {
        let src = r#""input.txt" >>> "output.txt""#;
        let program = parse(src).expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else { panic!("expected pipe"); };
        assert_eq!(flow.operations[0].0, PipeOp::Force);
    }

    #[test]
    fn parses_move_pipe_operator() {
        let src = r#""input.txt" -> "archive/""#;
        let program = parse(src).expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else { panic!("expected pipe"); };
        assert_eq!(flow.operations[0].0, PipeOp::Move);
    }

    // ── Directive Edge Cases ─────────────────────────────────────────────

    #[test]
    fn parses_directive_with_no_args_and_alias() {
        let src = r#"x >> @atomic as txn >> "out.txt""#;
        let program = parse(src).expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else { panic!("expected pipe"); };
        let (_, Destination::Directive(dir)) = &flow.operations[0] else { panic!("expected directive"); };
        assert_eq!(dir.name, "atomic");
        assert!(dir.arguments.is_empty());
        assert_eq!(dir.alias, Some("txn".to_string()));
    }

    #[test]
    fn parses_directive_with_multiple_args() {
        let program = parse("x >> @resize(800, 600)").expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else { panic!("expected pipe"); };
        let (_, Destination::Directive(dir)) = &flow.operations[0] else { panic!("expected directive"); };
        assert_eq!(dir.name, "resize");
        assert_eq!(dir.arguments.len(), 2);
        assert!(matches!(&dir.arguments[0], Expression::Literal(Literal::Number(n)) if *n == 800.0));
        assert!(matches!(&dir.arguments[1], Expression::Literal(Literal::Number(n)) if *n == 600.0));
    }

    #[test]
    fn parses_qualified_directive_with_alias() {
        let program = parse("x >> @csv.parse as data").expect("parse should succeed");
        let Statement::Pipe(flow) = &program.statements[0] else { panic!("expected pipe"); };
        let (_, Destination::Directive(dir)) = &flow.operations[0] else { panic!("expected directive"); };
        assert_eq!(dir.name, "csv.parse");
        assert_eq!(dir.alias, Some("data".to_string()));
    }

    // ── Error Cases (should fail parsing) ────────────────────────────────

    #[test]
    fn rejects_unclosed_bracket() {
        let result = parse("x >> [");
        assert!(result.is_err(), "should reject unclosed bracket");
    }

    #[test]
    fn rejects_bare_directive_without_name() {
        let result = parse("x >> @");
        assert!(result.is_err(), "should reject bare @ without directive name");
    }

    #[test]
    fn rejects_empty_program_content_in_statement() {
        // Double pipe with nothing in between
        let result = parse(">> >>");
        assert!(result.is_err(), "should reject invalid pipe without source");
    }

    // ── Mixed Programs ──────────────────────────────────────────────────

    #[test]
    fn parses_full_program_with_imports_functions_and_flows() {
        let src = r#"
            @import "std.csv" as csv
            @import "logic" as util

            is_valid(row) => row.id != null

            @watch("./inbox/") as event >> [
                filter(event.file.ext == "csv") >> @read(event.file.path) >> @csv.parse as data >> [
                    data >> "output.csv"
                ]
            ] on_fail as err >> [
                err >> "error.log"
            ]
        "#;
        let program = parse(src).expect("parse should succeed");
        assert_eq!(program.statements.len(), 4); // 2 imports + 1 function + 1 pipe
        assert!(matches!(&program.statements[0], Statement::Import(_)));
        assert!(matches!(&program.statements[1], Statement::Import(_)));
        assert!(matches!(&program.statements[2], Statement::Function(_)));
        assert!(matches!(&program.statements[3], Statement::Pipe(_)));
    }

    #[test]
    fn parses_comments_are_preserved() {
        let src = r#"
            // This is a comment
            x >> y // inline comment
        "#;
        let program = parse(src).expect("parse should succeed");
        assert_eq!(program.statements.len(), 2);
    }
}
