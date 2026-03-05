use loom::parser::parse;
use loom::validator::{ValidationSeverity, validate_program};

#[test]
fn malformed_directive_is_parse_error() {
    let result = parse("\"input\" >> @read(");
    assert!(result.is_err(), "malformed directive syntax must fail");
}

#[test]
fn unknown_directive_is_validation_error() {
    let program = parse("\"input\" >> @foo.parse").expect("program should parse");
    let issues = validate_program(&program);
    assert!(issues.iter().any(|issue| {
        issue.severity == ValidationSeverity::Error
            && issue.message == "Unknown directive: @foo.parse"
    }));
}

#[test]
fn unknown_function_is_validation_warning() {
    let program = parse("\"input\" >> missing()").expect("program should parse");
    let issues = validate_program(&program);
    assert!(issues.iter().any(|issue| {
        issue.severity == ValidationSeverity::Warning
            && issue.message == "Unknown function: missing"
    }));
}
