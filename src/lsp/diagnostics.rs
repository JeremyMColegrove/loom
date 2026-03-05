use crate::lsp::catalog::{BUILTIN_FUNCTIONS, DIRECTIVES};
use crate::lsp::symbols::lsp_range_from_span;
use std::collections::HashSet;
use tower_lsp::lsp_types::{Diagnostic, DiagnosticSeverity, Position, Range};

pub(crate) fn full_document_range(text: &str) -> Range {
    let mut line: u32 = 0;
    let mut character: u32 = 0;

    for ch in text.chars() {
        if ch == '\n' {
            line += 1;
            character = 0;
        } else {
            character += 1;
        }
    }

    Range {
        start: Position {
            line: 0,
            character: 0,
        },
        end: Position { line, character },
    }
}

pub(crate) fn validate_program(program: &crate::ast::Program) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    let mut defined_funcs = HashSet::new();

    for stmt in &program.statements {
        if let crate::ast::Statement::Function(func) = stmt {
            defined_funcs.insert(func.name.clone());
        }
    }

    for stmt in &program.statements {
        if let crate::ast::Statement::Pipe(flow) = stmt {
            validate_pipe_flow(flow, &defined_funcs, &mut diagnostics);
        } else if let crate::ast::Statement::Function(func) = stmt {
            match &func.body {
                crate::ast::FlowOrBranch::Flow(flow) => {
                    validate_pipe_flow(flow, &defined_funcs, &mut diagnostics)
                }
                crate::ast::FlowOrBranch::Branch(branch) => {
                    for item in &branch.items {
                        if let crate::ast::BranchItem::Flow(f) = item {
                            validate_pipe_flow(f, &defined_funcs, &mut diagnostics);
                        }
                    }
                }
            }
        }
    }

    diagnostics
}

fn validate_pipe_flow(
    flow: &crate::ast::PipeFlow,
    defined_funcs: &HashSet<String>,
    diagnostics: &mut Vec<Diagnostic>,
) {
    if let crate::ast::Source::Directive(dir) = &flow.source {
        if !DIRECTIVES.iter().any(|d| d.name == dir.name) {
            let token = format!("@{}", dir.name);
            diagnostics.push(Diagnostic {
                range: lsp_range_from_span(dir.span),
                severity: Some(DiagnosticSeverity::ERROR),
                message: format!("Unknown directive: {}", token),
                source: Some("loom".to_string()),
                ..Default::default()
            });
        }
    }

    for (_, dest) in &flow.operations {
        match dest {
            crate::ast::Destination::Directive(dir) => {
                if !DIRECTIVES.iter().any(|d| d.name == dir.name) {
                    let token = format!("@{}", dir.name);
                    diagnostics.push(Diagnostic {
                        range: lsp_range_from_span(dir.span),
                        severity: Some(DiagnosticSeverity::ERROR),
                        message: format!("Unknown directive: {}", token),
                        source: Some("loom".to_string()),
                        ..Default::default()
                    });
                }
            }
            crate::ast::Destination::FunctionCall(call) => {
                if !call.name.contains('.')
                    && !defined_funcs.contains(&call.name)
                    && !BUILTIN_FUNCTIONS
                        .iter()
                        .any(|(name, _, _)| *name == &call.name)
                {
                    diagnostics.push(Diagnostic {
                        range: lsp_range_from_span(call.span),
                        severity: Some(DiagnosticSeverity::WARNING),
                        message: format!("Unknown function: {}", call.name),
                        source: Some("loom".to_string()),
                        ..Default::default()
                    });
                }
            }
            crate::ast::Destination::Branch(branch) => {
                for item in &branch.items {
                    if let crate::ast::BranchItem::Flow(f) = item {
                        validate_pipe_flow(f, defined_funcs, diagnostics);
                    }
                }
            }
            _ => {}
        }
    }

    if let Some(fail) = &flow.on_fail {
        match fail.handler.as_ref() {
            crate::ast::FlowOrBranch::Flow(f) => validate_pipe_flow(f, defined_funcs, diagnostics),
            crate::ast::FlowOrBranch::Branch(b) => {
                for item in &b.items {
                    if let crate::ast::BranchItem::Flow(f) = item {
                        validate_pipe_flow(f, defined_funcs, diagnostics);
                    }
                }
            }
        }
    }
}
