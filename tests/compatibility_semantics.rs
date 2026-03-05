use loom::ast::{Expression, Literal, PipeFlow, Source, Span};
use loom::parser::parse;
use loom::runtime::Runtime;
use loom::runtime::env::Value;
use loom::runtime::security::{SecurityPolicy, TrustMode};
use tempfile::tempdir;

async fn eval_expression(expr: Expression) -> Result<Value, String> {
    let flow = PipeFlow {
        comments: vec![],
        span: Span::default(),
        source: Source::Expression(expr),
        operations: vec![],
        on_fail: None,
    };
    let mut runtime = Runtime::new();
    runtime.execute_flow(&flow).await.map_err(|e| e.to_string())
}

#[tokio::test(flavor = "multi_thread")]
async fn boolean_short_circuit_semantics_are_compatible() {
    let lhs_false = Expression::BinaryOp(
        Box::new(Expression::Literal(Literal::Boolean(false))),
        "&&".to_string(),
        Box::new(Expression::BinaryOp(
            Box::new(Expression::Literal(Literal::Number(1.0))),
            "/".to_string(),
            Box::new(Expression::Literal(Literal::Number(0.0))),
        )),
    );
    let and_result = eval_expression(lhs_false)
        .await
        .expect("short-circuited false && expr should not fail");
    assert!(matches!(and_result, Value::Boolean(false)));

    let lhs_true = Expression::BinaryOp(
        Box::new(Expression::Literal(Literal::Boolean(true))),
        "||".to_string(),
        Box::new(Expression::BinaryOp(
            Box::new(Expression::Literal(Literal::Number(1.0))),
            "/".to_string(),
            Box::new(Expression::Literal(Literal::Number(0.0))),
        )),
    );
    let or_result = eval_expression(lhs_true)
        .await
        .expect("short-circuited true || expr should not fail");
    assert!(matches!(or_result, Value::Boolean(true)));
}

#[tokio::test(flavor = "multi_thread")]
async fn numeric_comparison_coercion_semantics_are_compatible() {
    let expr = Expression::BinaryOp(
        Box::new(Expression::Literal(Literal::String("10".to_string()))),
        ">".to_string(),
        Box::new(Expression::Literal(Literal::Number(2.0))),
    );

    let result = eval_expression(expr)
        .await
        .expect("string numeric comparisons should evaluate");
    assert!(matches!(result, Value::Boolean(true)));
}

#[tokio::test(flavor = "multi_thread")]
async fn repeated_import_uses_cached_module_exports() {
    let dir = tempdir().expect("tempdir");
    let module_path = dir.path().join("shared.loom");
    let marker_path = dir.path().join("marker.txt");

    std::fs::write(&module_path, "1 >> \"marker.txt\"\nanswer() => \"42\"\n")
        .expect("write module");

    let main = r#"
        @import "shared" as first
        @import "shared" as second
    "#;
    let program = parse(main).expect("main should parse");

    let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().expect("path"));
    runtime
        .set_security_policy(SecurityPolicy::allow_all())
        .expect("set allow-all policy");
    runtime.set_trust_mode(TrustMode::Trusted);
    runtime.execute(&program).await.expect("program should run");

    let marker = std::fs::read_to_string(&marker_path).expect("marker file should exist");
    assert_eq!(marker.lines().count(), 1, "module should execute once");
}

#[tokio::test(flavor = "multi_thread")]
async fn dotted_import_paths_resolve_to_module_files() {
    let dir = tempdir().expect("tempdir");
    let util_dir = dir.path().join("util");
    std::fs::create_dir_all(&util_dir).expect("create util dir");
    std::fs::write(util_dir.join("math.loom"), "1 >> \"dotted-marker.txt\"\n")
        .expect("write module");

    let main = r#"@import "util.math" as math"#;
    let program = parse(main).expect("main should parse");

    let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().expect("path"));
    runtime
        .set_security_policy(SecurityPolicy::allow_all())
        .expect("set allow-all policy");
    runtime.set_trust_mode(TrustMode::Trusted);
    runtime.execute(&program).await.expect("program should run");

    let marker = dir.path().join("util").join("dotted-marker.txt");
    assert!(marker.exists(), "dotted import should execute module");
}

#[tokio::test(flavor = "multi_thread")]
async fn cyclic_imports_are_rejected() {
    let dir = tempdir().expect("tempdir");
    std::fs::write(dir.path().join("a.loom"), "@import \"b\" as b\n").expect("write a module");
    std::fs::write(dir.path().join("b.loom"), "@import \"a\" as a\n").expect("write b module");

    let main = r#"@import "a" as a"#;
    let program = parse(main).expect("main should parse");

    let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().expect("path"));
    runtime
        .set_security_policy(SecurityPolicy::allow_all())
        .expect("set allow-all policy");
    runtime.set_trust_mode(TrustMode::Trusted);
    let result = runtime.execute(&program).await;

    assert!(result.is_err(), "cyclic imports must fail");
    let msg = result.err().unwrap_or_default();
    assert!(
        msg.contains("Cyclic import detected"),
        "unexpected error message: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn parse_failure_does_not_leave_stale_loading_state() {
    let dir = tempdir().expect("tempdir");
    let module_path = dir.path().join("shared.loom");
    std::fs::write(&module_path, "broken() =>").expect("write invalid module");

    let main = r#"@import "shared" as shared"#;
    let program = parse(main).expect("main should parse");

    let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().expect("path"));
    runtime
        .set_security_policy(SecurityPolicy::allow_all())
        .expect("set allow-all policy");
    runtime.set_trust_mode(TrustMode::Trusted);

    let first_result = runtime.execute(&program).await;
    assert!(first_result.is_err(), "invalid import should fail");
    let first_error = first_result.err().unwrap_or_default();
    assert!(
        first_error.contains("Parse errors in"),
        "unexpected first error message: {first_error}"
    );

    std::fs::write(&module_path, "shared_value() => \"ok\"\n").expect("fix module");

    let second_result = runtime.execute(&program).await;
    assert!(
        second_result.is_ok(),
        "import should recover after parse failure, got: {:?}",
        second_result.err()
    );
}
