use crate::ast::{BranchItem, Destination, FlowOrBranch, PipeFlow, Program, Source, Statement};
use crate::builtin_spec::is_known_runtime_directive;
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationSeverity {
    Error,
    Warning,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationIssue {
    pub message: String,
    pub span: crate::ast::Span,
    pub severity: ValidationSeverity,
}

pub fn validate_program(program: &Program) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();
    let mut defined_funcs = HashSet::new();

    for stmt in &program.statements {
        if let Statement::Function(func) = stmt {
            defined_funcs.insert(func.name.clone());
        }
    }

    for stmt in &program.statements {
        match stmt {
            Statement::Pipe(flow) => validate_pipe_flow(flow, &defined_funcs, &mut issues),
            Statement::Function(func) => match &func.body {
                FlowOrBranch::Flow(flow) => validate_pipe_flow(flow, &defined_funcs, &mut issues),
                FlowOrBranch::Branch(branch) => {
                    for item in &branch.items {
                        if let BranchItem::Flow(flow) = item {
                            validate_pipe_flow(flow, &defined_funcs, &mut issues);
                        }
                    }
                }
            },
            _ => {}
        }
    }

    issues
}

fn validate_pipe_flow(
    flow: &PipeFlow,
    defined_funcs: &HashSet<String>,
    issues: &mut Vec<ValidationIssue>,
) {
    if let Source::Directive(dir) = &flow.source
        && !is_known_runtime_directive(&dir.name)
    {
        issues.push(ValidationIssue {
            message: format!("Unknown directive: @{}", dir.name),
            span: dir.span,
            severity: ValidationSeverity::Error,
        });
    }

    for (_, dest) in &flow.operations {
        match dest {
            Destination::Directive(dir) => {
                if !is_known_runtime_directive(&dir.name) {
                    issues.push(ValidationIssue {
                        message: format!("Unknown directive: @{}", dir.name),
                        span: dir.span,
                        severity: ValidationSeverity::Error,
                    });
                }
            }
            Destination::FunctionCall(call) => {
                if !call.name.contains('.')
                    && !defined_funcs.contains(&call.name)
                    && !is_known_builtin_function(&call.name)
                {
                    issues.push(ValidationIssue {
                        message: format!("Unknown function: {}", call.name),
                        span: call.span,
                        severity: ValidationSeverity::Warning,
                    });
                }
            }
            Destination::Branch(branch) => {
                for item in &branch.items {
                    if let BranchItem::Flow(flow) = item {
                        validate_pipe_flow(flow, defined_funcs, issues);
                    }
                }
            }
            _ => {}
        }
    }

    if let Some(fail) = &flow.on_fail {
        match fail.handler.as_ref() {
            FlowOrBranch::Flow(flow) => validate_pipe_flow(flow, defined_funcs, issues),
            FlowOrBranch::Branch(branch) => {
                for item in &branch.items {
                    if let BranchItem::Flow(flow) = item {
                        validate_pipe_flow(flow, defined_funcs, issues);
                    }
                }
            }
        }
    }
}

fn is_known_builtin_function(name: &str) -> bool {
    matches!(name, "filter" | "map" | "print" | "concat" | "exists")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse;

    #[test]
    fn unknown_parse_directive_is_an_error() {
        let program = parse("\"x\" >> @json.parse").expect("parse should succeed");
        let issues = validate_program(&program);
        assert!(
            issues
                .iter()
                .any(|issue| issue.message == "Unknown directive: @json.parse"
                    && issue.severity == ValidationSeverity::Error)
        );
    }
}
