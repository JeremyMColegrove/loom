#[cfg(test)]
mod tests {
    use loom::formatter::Formatter;
    use loom::parser::parse;

    #[test]
    fn formats_simple_pipe() {
        let src = r#"
            // Top comment
            "hello" >> print // Inline comment
        "#;
        let program = parse(src).expect("parse");
        let formatted = Formatter::format(&program);

        let expected = "// Top comment\n// Inline comment\n\"hello\" >> print\n";
        assert_eq!(formatted, expected);
    }

    #[test]
    fn formats_branch_and_function() {
        let src = r#"
            // Func comment
            my_func(a, b) => [
                // Inside branch
                // Branch 1
                a >> print,
                // Branch 2
                b >> log
            ]
        "#;
        let program = parse(src).expect("parse");
        let formatted = Formatter::format(&program);

        let expected = "// Func comment\nmy_func(a, b) => [\n    // Inside branch\n    // Branch 1\n    a >> print,\n    // Branch 2\n    b >> log\n]\n";
        assert_eq!(formatted, expected);
    }

    #[test]
    fn formats_branch_destination_with_commas() {
        let src = r#"filter(r >> r.id > 10) >> [stdout, "file.txt"]"#;
        let program = parse(src).expect("parse");
        let formatted = Formatter::format(&program);

        let expected = "filter(r >> r.id > 10) >> [\n    stdout,\n    \"file.txt\"\n]\n";
        assert_eq!(formatted, expected);
    }
}
