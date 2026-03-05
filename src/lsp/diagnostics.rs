use crate::lsp::symbols::lsp_range_from_span;
use crate::validator::{ValidationSeverity, validate_program as validate_semantics};
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
    validate_semantics(program)
        .into_iter()
        .map(|issue| Diagnostic {
            range: lsp_range_from_span(issue.span),
            severity: Some(match issue.severity {
                ValidationSeverity::Error => DiagnosticSeverity::ERROR,
                ValidationSeverity::Warning => DiagnosticSeverity::WARNING,
            }),
            message: issue.message,
            source: Some("loom".to_string()),
            ..Default::default()
        })
        .collect()
}
