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
