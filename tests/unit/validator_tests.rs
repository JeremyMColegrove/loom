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

#[test]
fn http_post_requires_std_http_import() {
    let program =
        parse("\"x\" >> @http.post(\"http://127.0.0.1:8123\")").expect("parse should succeed");
    let issues = validate_program(&program);
    assert!(issues.iter().any(|issue| {
        issue.message == "Directive @http.post requires @import \"std.http\" as http"
            && issue.severity == ValidationSeverity::Error
    }));
}

#[test]
fn http_post_is_allowed_when_std_http_is_imported() {
    let program =
        parse("@import \"std.http\" as http\n\"x\" >> @http.post(\"http://127.0.0.1:8123\")")
            .expect("parse should succeed");
    let issues = validate_program(&program);
    assert!(
        !issues
            .iter()
            .any(|issue| issue.message.contains("@http.post")),
        "http.post should validate when std.http is imported: {:?}",
        issues
    );
}

#[test]
fn secret_directive_is_known_and_has_no_unknown_directive_error() {
    let program = parse("\"x\" >> @secret(\"API_KEY\")").expect("parse should succeed");
    let issues = validate_program(&program);
    assert!(
        !issues
            .iter()
            .any(|issue| issue.message.contains("Unknown directive: @secret")),
        "secret should be known: {:?}",
        issues
    );
}

#[test]
fn secret_directive_requires_exactly_one_argument() {
    let program = parse("\"x\" >> @secret()").expect("parse should succeed");
    let issues = validate_program(&program);
    assert!(
        issues.iter().any(|issue| {
            issue.message == "@secret expects exactly one argument"
                && issue.severity == ValidationSeverity::Error
        }),
        "expected @secret arity validation error: {:?}",
        issues
    );
}

#[test]
fn undefined_identifier_is_an_error() {
    let program = parse("value >> @log").expect("parse should succeed");
    let issues = validate_program(&program);
    assert!(issues.iter().any(|issue| {
        issue.message == "Undefined variable: value"
            && issue.severity == ValidationSeverity::Error
    }));
}

#[test]
fn destination_identifier_defines_variable_for_later_usage() {
    let program = parse("\"x\" >> result\nresult >> @log").expect("parse should succeed");
    let issues = validate_program(&program);
    assert!(
        !issues
            .iter()
            .any(|issue| issue.message.contains("Undefined variable: result")),
        "result should be defined by destination assignment: {:?}",
        issues
    );
}

#[test]
fn function_unknown_call_is_an_error() {
    let program = parse("\"x\" >> missing_fn()").expect("parse should succeed");
    let issues = validate_program(&program);
    assert!(issues.iter().any(|issue| {
        issue.message == "Unknown function: missing_fn"
            && issue.severity == ValidationSeverity::Error
    }));
}
