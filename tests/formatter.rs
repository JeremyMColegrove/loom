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

    #[test]
    fn formats_escaped_string_literal_contents() {
        let src = r#"\"{\"msg\":\"ok\"}" >> output"#;
        let program = parse(src).expect("parse");
        let formatted = Formatter::format(&program);

        let expected = "\\\"{\\\"msg\\\":\\\"ok\\\"}\" >> output\n";
        assert_eq!(formatted, expected);
    }

    #[test]
    fn formats_secret_expression_call() {
        let src = r#"data >> \"hello: " + @secret(\"NAME")"#;
        let program = parse(src).expect("parse");
        let formatted = Formatter::format(&program);

        let expected = "data >> \\\"hello: \" + @secret(\\\"NAME\")\n";
        assert_eq!(formatted, expected);
    }

    #[test]
    fn preserves_escaped_string_object_keys() {
        let src = r#"x >> fn_call({ \"Authorization": \"Bearer x" })"#;
        let program = parse(src).expect("parse");
        let formatted = Formatter::format(&program);

        let expected = "x >> fn_call({\\\"Authorization\": \\\"Bearer x\"})\n";
        assert_eq!(formatted, expected);
    }

    #[test]
    fn wraps_long_pipe_chains_when_max_width_is_exceeded() {
        let src = r#"source_data >> very_long_transformation_name >> another_long_operation_name >> final_output"#;
        let program = parse(src).expect("parse");
        let formatted = Formatter::format_with_max_width(&program, 35);

        let expected = "source_data\n    >> very_long_transformation_name\n    >> another_long_operation_name\n    >> final_output\n";
        assert_eq!(formatted, expected);
    }

    #[test]
    fn keeps_pipe_chain_on_one_line_when_within_max_width() {
        let src = r#"a >> b >> c"#;
        let program = parse(src).expect("parse");
        let formatted = Formatter::format_with_max_width(&program, 80);

        let expected = "a >> b >> c\n";
        assert_eq!(formatted, expected);
    }
}
