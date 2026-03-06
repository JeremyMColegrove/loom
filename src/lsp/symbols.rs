use crate::ast::{Expression, FlowOrBranch, Program, Source, Span, Statement};
use tower_lsp::lsp_types::{DocumentSymbol, Position, Range, SymbolKind};

#[derive(Clone, Copy, Debug)]
pub(crate) struct SymbolAtPos<'a> {
    pub(crate) name: &'a str,
}

pub(crate) fn lsp_range_from_span(span: Span) -> Range {
    Range {
        start: Position {
            line: span.start.line.saturating_sub(1) as u32,
            character: span.start.col.saturating_sub(1) as u32,
        },
        end: Position {
            line: span.end.line.saturating_sub(1) as u32,
            character: span.end.col.saturating_sub(1) as u32,
        },
    }
}

fn find_expression_symbol_at_pos<'a>(
    expr: &'a Expression,
    pos: Position,
) -> Option<SymbolAtPos<'a>> {
    match expr {
        Expression::FunctionCall(call) => {
            if call.span.contains_zero_based(pos.line, pos.character) {
                Some(SymbolAtPos { name: &call.name })
            } else {
                None
            }
        }
        Expression::SecretCall(call) => {
            if call.span.contains_zero_based(pos.line, pos.character) {
                Some(SymbolAtPos { name: "secret" })
            } else {
                None
            }
        }
        Expression::Lambda(lambda) => find_expression_symbol_at_pos(&lambda.body, pos),
        Expression::BinaryOp(left, _, right) => find_expression_symbol_at_pos(left, pos)
            .or_else(|| find_expression_symbol_at_pos(right, pos)),
        Expression::UnaryOp(_, inner) => find_expression_symbol_at_pos(inner, pos),
        Expression::ObjectLiteral(entries) => {
            for (_, value) in entries {
                if let Some(found) = find_expression_symbol_at_pos(value, pos) {
                    return Some(found);
                }
            }
            None
        }
        _ => None,
    }
}

fn find_flow_or_branch_symbol_at_pos<'a>(
    body: &'a FlowOrBranch,
    pos: Position,
) -> Option<SymbolAtPos<'a>> {
    match body {
        FlowOrBranch::Flow(flow) => find_pipe_flow_symbol_at_pos(flow, pos),
        FlowOrBranch::Branch(branch) => find_branch_symbol_at_pos(branch, pos),
    }
}

fn find_branch_symbol_at_pos<'a>(
    branch: &'a crate::ast::Branch,
    pos: Position,
) -> Option<SymbolAtPos<'a>> {
    if !branch.span.contains_zero_based(pos.line, pos.character) {
        return None;
    }
    for item in &branch.items {
        if let crate::ast::BranchItem::Flow(flow) = item
            && let Some(found) = find_pipe_flow_symbol_at_pos(flow, pos)
        {
            return Some(found);
        }
    }
    None
}

fn find_pipe_flow_symbol_at_pos<'a>(
    flow: &'a crate::ast::PipeFlow,
    pos: Position,
) -> Option<SymbolAtPos<'a>> {
    if !flow.span.contains_zero_based(pos.line, pos.character) {
        return None;
    }

    if let Some(found) = find_source_symbol_at_pos(&flow.source, pos) {
        return Some(found);
    }

    for (_, dest) in &flow.operations {
        if let Some(found) = find_destination_symbol_at_pos(dest, pos) {
            return Some(found);
        }
    }

    if let Some(on_fail) = &flow.on_fail
        && on_fail.span.contains_zero_based(pos.line, pos.character)
        && let Some(found) = find_flow_or_branch_symbol_at_pos(&on_fail.handler, pos)
    {
        return Some(found);
    }

    None
}

fn find_source_symbol_at_pos<'a>(source: &'a Source, pos: Position) -> Option<SymbolAtPos<'a>> {
    match source {
        Source::Directive(dir) if dir.span.contains_zero_based(pos.line, pos.character) => {
            Some(SymbolAtPos { name: &dir.name })
        }
        Source::FunctionCall(call) if call.span.contains_zero_based(pos.line, pos.character) => {
            Some(SymbolAtPos { name: &call.name })
        }
        Source::Expression(expr) => find_expression_symbol_at_pos(expr, pos),
        _ => None,
    }
}

fn find_destination_symbol_at_pos<'a>(
    dest: &'a crate::ast::Destination,
    pos: Position,
) -> Option<SymbolAtPos<'a>> {
    match dest {
        crate::ast::Destination::Directive(dir)
            if dir.span.contains_zero_based(pos.line, pos.character) =>
        {
            Some(SymbolAtPos { name: &dir.name })
        }
        crate::ast::Destination::FunctionCall(call)
            if call.span.contains_zero_based(pos.line, pos.character) =>
        {
            Some(SymbolAtPos { name: &call.name })
        }
        crate::ast::Destination::Branch(branch) => find_branch_symbol_at_pos(branch, pos),
        crate::ast::Destination::Expression(expr) => find_expression_symbol_at_pos(expr, pos),
        _ => None,
    }
}

pub(crate) fn find_symbol_at_position<'a>(
    program: &'a Program,
    pos: Position,
) -> Option<SymbolAtPos<'a>> {
    for stmt in &program.statements {
        match stmt {
            Statement::Import(imp) if imp.span.contains_zero_based(pos.line, pos.character) => {
                return Some(SymbolAtPos { name: &imp.path });
            }
            Statement::Function(func) if func.span.contains_zero_based(pos.line, pos.character) => {
                if let Some(found) = find_flow_or_branch_symbol_at_pos(&func.body, pos) {
                    return Some(found);
                }
                return Some(SymbolAtPos { name: &func.name });
            }
            Statement::Pipe(flow) if flow.span.contains_zero_based(pos.line, pos.character) => {
                if let Some(found) = find_pipe_flow_symbol_at_pos(flow, pos) {
                    return Some(found);
                }
            }
            _ => {}
        }
    }
    None
}

pub(crate) fn document_symbols(program: &Program) -> Vec<DocumentSymbol> {
    let mut symbols: Vec<DocumentSymbol> = Vec::new();

    for stmt in &program.statements {
        match stmt {
            Statement::Comment(_) => {}
            Statement::Import(imp) => {
                let label = if let Some(alias) = &imp.alias {
                    format!("@import \"{}\" as {}", imp.path, alias)
                } else {
                    format!("@import \"{}\"", imp.path)
                };
                let range = lsp_range_from_span(imp.span);
                #[allow(deprecated)]
                symbols.push(DocumentSymbol {
                    name: label,
                    detail: Some(imp.path.clone()),
                    kind: SymbolKind::MODULE,
                    tags: None,
                    deprecated: None,
                    range,
                    selection_range: range,
                    children: None,
                });
            }
            Statement::Function(func) => {
                let params_str = func.parameters.join(", ");
                let label = format!("{}({})", func.name, params_str);
                let range = lsp_range_from_span(func.span);
                #[allow(deprecated)]
                symbols.push(DocumentSymbol {
                    name: label,
                    detail: Some("function".to_string()),
                    kind: SymbolKind::FUNCTION,
                    tags: None,
                    deprecated: None,
                    range,
                    selection_range: range,
                    children: None,
                });
            }
            Statement::Pipe(flow) => {
                let label = match &flow.source {
                    Source::Directive(dir) => {
                        if let Some(alias) = &dir.alias {
                            format!("@{} as {}", dir.name, alias)
                        } else {
                            format!("@{}", dir.name)
                        }
                    }
                    Source::FunctionCall(call) => call.name.clone(),
                    Source::Expression(expr) => format!("{:?}", expr).chars().take(40).collect(),
                };
                let range = lsp_range_from_span(flow.span);
                #[allow(deprecated)]
                symbols.push(DocumentSymbol {
                    name: label,
                    detail: Some("pipeline".to_string()),
                    kind: SymbolKind::EVENT,
                    tags: None,
                    deprecated: None,
                    range,
                    selection_range: range,
                    children: None,
                });
            }
        }
    }

    symbols
}
