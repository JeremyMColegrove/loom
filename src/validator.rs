use crate::ast::{
    BranchItem, Destination, Expression, FlowOrBranch, FunctionCall, PipeFlow, Program, Source,
    Span, Statement,
};
use crate::builtin_spec::{
    is_known_builtin_function, is_known_runtime_directive, required_std_module_for_directive,
};
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

#[derive(Clone)]
struct ScopeStack {
    scopes: Vec<HashSet<String>>,
}

impl ScopeStack {
    fn with_globals(globals: HashSet<String>) -> Self {
        Self {
            scopes: vec![globals],
        }
    }

    fn push_scope(&mut self) {
        self.scopes.push(HashSet::new());
    }

    fn pop_scope(&mut self) {
        if self.scopes.len() > 1 {
            self.scopes.pop();
        }
    }

    fn define(&mut self, name: &str) {
        if let Some(scope) = self.scopes.last_mut() {
            scope.insert(name.to_string());
        }
    }

    fn is_defined(&self, name: &str) -> bool {
        self.scopes.iter().rev().any(|scope| scope.contains(name))
    }
}

pub fn validate_program(program: &Program) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();
    let mut defined_funcs = HashSet::new();
    let mut imported_std_modules = HashSet::new();
    let mut import_aliases = HashSet::new();

    for stmt in &program.statements {
        match stmt {
            Statement::Function(func) => {
                defined_funcs.insert(func.name.clone());
            }
            Statement::Import(imp) => {
                if let Some(module) = imp.path.strip_prefix("std.") {
                    imported_std_modules.insert(module.to_string());
                }
                if let Some(alias) = &imp.alias {
                    import_aliases.insert(alias.clone());
                }
            }
            _ => {}
        }
    }

    let mut globals = HashSet::new();
    globals.insert("null".to_string());
    for alias in import_aliases {
        globals.insert(alias);
    }
    for func in &defined_funcs {
        globals.insert(func.clone());
    }
    let mut scopes = ScopeStack::with_globals(globals);

    for stmt in &program.statements {
        match stmt {
            Statement::Pipe(flow) => {
                validate_pipe_flow(
                    flow,
                    &defined_funcs,
                    &imported_std_modules,
                    &mut scopes,
                    &mut issues,
                )
            }
            Statement::Function(func) => match &func.body {
                FlowOrBranch::Flow(flow) => {
                    let mut fn_scopes = scopes.clone();
                    fn_scopes.push_scope();
                    for param in &func.parameters {
                        fn_scopes.define(param);
                    }
                    validate_pipe_flow(
                        flow,
                        &defined_funcs,
                        &imported_std_modules,
                        &mut fn_scopes,
                        &mut issues,
                    );
                }
                FlowOrBranch::Branch(branch) => {
                    let mut fn_scopes = scopes.clone();
                    fn_scopes.push_scope();
                    for param in &func.parameters {
                        fn_scopes.define(param);
                    }
                    for item in &branch.items {
                        if let BranchItem::Flow(flow) = item {
                            validate_pipe_flow(
                                flow,
                                &defined_funcs,
                                &imported_std_modules,
                                &mut fn_scopes,
                                &mut issues,
                            );
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
    imported_std_modules: &HashSet<String>,
    scopes: &mut ScopeStack,
    issues: &mut Vec<ValidationIssue>,
) {
    match &flow.source {
        Source::Directive(dir) => {
            if !is_known_runtime_directive(&dir.name)
                && !is_valid_imported_std_directive(&dir.name, imported_std_modules)
            {
                issues.push(ValidationIssue {
                    message: unknown_directive_message(&dir.name, imported_std_modules),
                    span: dir.span,
                    severity: ValidationSeverity::Error,
                });
            }
            validate_secret_invocation(
                &dir.name,
                &dir.arguments,
                &dir.named_arguments,
                dir.span,
                issues,
            );
            for arg in &dir.arguments {
                validate_expression(arg, dir.span, defined_funcs, scopes, issues);
            }
            for named in &dir.named_arguments {
                validate_expression(&named.value, dir.span, defined_funcs, scopes, issues);
            }
            if let Some(alias) = &dir.alias {
                scopes.define(alias);
            }
        }
        Source::FunctionCall(call) => {
            validate_function_call(call, defined_funcs, scopes, issues);
            if let Some(alias) = &call.alias {
                scopes.define(alias);
            }
        }
        Source::Expression(expr) => validate_expression(expr, flow.span, defined_funcs, scopes, issues),
    }

    for (_, dest) in &flow.operations {
        match dest {
            Destination::Directive(dir) => {
                if !is_known_runtime_directive(&dir.name)
                    && !is_valid_imported_std_directive(&dir.name, imported_std_modules)
                {
                    issues.push(ValidationIssue {
                        message: unknown_directive_message(&dir.name, imported_std_modules),
                        span: dir.span,
                        severity: ValidationSeverity::Error,
                    });
                }
                validate_secret_invocation(
                    &dir.name,
                    &dir.arguments,
                    &dir.named_arguments,
                    dir.span,
                    issues,
                );
                for arg in &dir.arguments {
                    validate_expression(arg, dir.span, defined_funcs, scopes, issues);
                }
                for named in &dir.named_arguments {
                    validate_expression(&named.value, dir.span, defined_funcs, scopes, issues);
                }
                if let Some(alias) = &dir.alias {
                    scopes.define(alias);
                }
            }
            Destination::FunctionCall(call) => {
                validate_function_call(call, defined_funcs, scopes, issues);
                if let Some(alias) = &call.alias {
                    scopes.define(alias);
                }
            }
            Destination::Branch(branch) => {
                for item in &branch.items {
                    if let BranchItem::Flow(flow) = item {
                        scopes.push_scope();
                        scopes.define("_");
                        validate_pipe_flow(flow, defined_funcs, imported_std_modules, scopes, issues);
                        scopes.pop_scope();
                    }
                }
            }
            Destination::Expression(Expression::Identifier(name)) => {
                if !is_callable_name(name, defined_funcs) {
                    scopes.define(name);
                }
            }
            Destination::Expression(Expression::MemberAccess(parts)) => {
                let qualified = parts.join(".");
                if !is_callable_name(&qualified, defined_funcs) {
                    validate_member_access(parts, flow.span, scopes, issues);
                }
            }
            Destination::Expression(expr) => {
                validate_expression(expr, flow.span, defined_funcs, scopes, issues)
            }
        }
    }

    if let Some(fail) = &flow.on_fail {
        scopes.push_scope();
        scopes.define("err");
        if let Some(alias) = &fail.alias {
            scopes.define(alias);
        }
        match fail.handler.as_ref() {
            FlowOrBranch::Flow(flow) => {
                validate_pipe_flow(flow, defined_funcs, imported_std_modules, scopes, issues)
            }
            FlowOrBranch::Branch(branch) => {
                for item in &branch.items {
                    if let BranchItem::Flow(flow) = item {
                        validate_pipe_flow(flow, defined_funcs, imported_std_modules, scopes, issues);
                    }
                }
            }
        }
        scopes.pop_scope();
    }
}

fn validate_function_call(
    call: &FunctionCall,
    defined_funcs: &HashSet<String>,
    scopes: &mut ScopeStack,
    issues: &mut Vec<ValidationIssue>,
) {
    validate_function_name(&call.name, call.span, defined_funcs, scopes, issues);
    for arg in &call.arguments {
        validate_expression(arg, call.span, defined_funcs, scopes, issues);
    }
    for named in &call.named_arguments {
        validate_expression(&named.value, call.span, defined_funcs, scopes, issues);
    }
}

fn validate_function_name(
    name: &str,
    span: Span,
    defined_funcs: &HashSet<String>,
    scopes: &mut ScopeStack,
    issues: &mut Vec<ValidationIssue>,
) {
    if is_callable_name(name, defined_funcs) {
        return;
    }

    if let Some(root) = name.split('.').next()
        && name.contains('.')
        && !scopes.is_defined(root)
    {
        issues.push(ValidationIssue {
            message: format!("Undefined variable: {}", root),
            span,
            severity: ValidationSeverity::Error,
        });
        return;
    }

    issues.push(ValidationIssue {
        message: format!("Unknown function: {}", name),
        span,
        severity: ValidationSeverity::Error,
    });
}

fn validate_member_access(
    parts: &[String],
    span: Span,
    scopes: &ScopeStack,
    issues: &mut Vec<ValidationIssue>,
) {
    if let Some(root) = parts.first()
        && !scopes.is_defined(root)
    {
        issues.push(ValidationIssue {
            message: format!("Undefined variable: {}", root),
            span,
            severity: ValidationSeverity::Error,
        });
    }
}

fn validate_expression(
    expr: &Expression,
    fallback_span: Span,
    defined_funcs: &HashSet<String>,
    scopes: &mut ScopeStack,
    issues: &mut Vec<ValidationIssue>,
) {
    match expr {
        Expression::SecretCall(secret) => {
            validate_secret_invocation(
                "secret",
                &secret.arguments,
                &secret.named_arguments,
                secret.span,
                issues,
            );
            for arg in &secret.arguments {
                validate_expression(arg, secret.span, defined_funcs, scopes, issues);
            }
            for named in &secret.named_arguments {
                validate_expression(&named.value, secret.span, defined_funcs, scopes, issues);
            }
        }
        Expression::BinaryOp(left, _, right) => {
            validate_expression(left, fallback_span, defined_funcs, scopes, issues);
            validate_expression(right, fallback_span, defined_funcs, scopes, issues);
        }
        Expression::UnaryOp(_, inner) => {
            validate_expression(inner, fallback_span, defined_funcs, scopes, issues)
        }
        Expression::Lambda(lambda) => {
            scopes.push_scope();
            scopes.define(&lambda.param);
            validate_expression(&lambda.body, lambda.span, defined_funcs, scopes, issues);
            scopes.pop_scope();
        }
        Expression::FunctionCall(call) => validate_function_call(call, defined_funcs, scopes, issues),
        Expression::ObjectLiteral(entries) => {
            for (_, value) in entries {
                validate_expression(value, fallback_span, defined_funcs, scopes, issues);
            }
        }
        Expression::Identifier(name) => {
            if !scopes.is_defined(name) {
                issues.push(ValidationIssue {
                    message: format!("Undefined variable: {}", name),
                    span: fallback_span,
                    severity: ValidationSeverity::Error,
                });
            }
        }
        Expression::MemberAccess(parts) => validate_member_access(parts, fallback_span, scopes, issues),
        Expression::Literal(_) => {}
    }
}

fn is_callable_name(name: &str, defined_funcs: &HashSet<String>) -> bool {
    is_known_builtin_function(name) || defined_funcs.contains(name)
}

fn validate_secret_invocation(
    name: &str,
    arguments: &[Expression],
    named_arguments: &[crate::ast::NamedArgument],
    span: Span,
    issues: &mut Vec<ValidationIssue>,
) {
    if name != "secret" {
        return;
    }
    if !named_arguments.is_empty() {
        issues.push(ValidationIssue {
            message: "@secret does not support named arguments".to_string(),
            span,
            severity: ValidationSeverity::Error,
        });
    }
    if arguments.len() != 1 {
        issues.push(ValidationIssue {
            message: "@secret expects exactly one argument".to_string(),
            span,
            severity: ValidationSeverity::Error,
        });
    }
}

fn is_valid_imported_std_directive(name: &str, imported_std_modules: &HashSet<String>) -> bool {
    required_std_module_for_directive(name)
        .is_some_and(|module| imported_std_modules.contains(module))
}

fn unknown_directive_message(name: &str, imported_std_modules: &HashSet<String>) -> String {
    if let Some(module) = required_std_module_for_directive(name)
        && !imported_std_modules.contains(module)
    {
        return format!(
            "Directive @{} requires @import \"std.{}\" as {}",
            name, module, module
        );
    }
    format!("Unknown directive: @{}", name)
}

#[cfg(test)]
#[path = "../tests/unit/validator_tests.rs"]
mod validator_tests;
