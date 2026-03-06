use loom::ast::*;
use loom::runtime::Runtime;
use loom::runtime::env::Value;
use loom::runtime::security::{SecurityPolicy, TrustMode};
use std::time::Duration;
use tempfile::tempdir;

async fn call_builtin(
    runtime: &mut Runtime,
    name: &str,
    arguments: Vec<Expression>,
) -> Result<Value, String> {
    let flow = PipeFlow {
        comments: vec![],
        span: Span::default(),
        source: Source::FunctionCall(FunctionCall {
            span: Span::default(),
            name: name.to_string(),
            arguments,
            named_arguments: vec![],
            alias: None,
        }),
        operations: vec![],
        on_fail: None,
    };
    runtime.execute_flow(&flow).await.map_err(|e| e.to_string())
}

fn trusted_runtime() -> Runtime {
    let mut runtime = Runtime::new();
    runtime
        .set_security_policy(SecurityPolicy::allow_all())
        .expect("set allow-all policy");
    runtime.set_trust_mode(TrustMode::Trusted);
    runtime
}

fn trusted_runtime_with_script_dir(script_dir: &str) -> Runtime {
    let mut runtime = Runtime::new().with_script_dir(script_dir);
    runtime
        .set_security_policy(SecurityPolicy::allow_all())
        .expect("set allow-all policy");
    runtime.set_trust_mode(TrustMode::Trusted);
    runtime
}

#[tokio::test(flavor = "multi_thread")]
async fn path_source_reads_and_destination_appends() {
    let dir = tempdir().expect("tempdir should be created");
    let input_path = dir.path().join("hello-world.txt");
    let output_path = dir.path().join("another.txt");

    std::fs::write(&input_path, "hello").expect("should write input");

    let flow = PipeFlow {
        comments: vec![],
        span: Span::default(),
        source: Source::Expression(Expression::Literal(Literal::Path(
            input_path.to_string_lossy().to_string(),
        ))),
        operations: vec![(
            PipeOp::Safe,
            Destination::Expression(Expression::Literal(Literal::Path(
                output_path.to_string_lossy().to_string(),
            ))),
        )],
        on_fail: None,
    };

    let mut runtime = trusted_runtime();
    runtime
        .execute_flow(&flow)
        .await
        .expect("flow should execute");
    runtime
        .execute_flow(&flow)
        .await
        .expect("flow should execute twice");

    let output = std::fs::read_to_string(&output_path).expect("should read output");
    assert_eq!(output, "hello\nhello\n");
}

#[tokio::test(flavor = "multi_thread")]
async fn path_destination_overwrites_with_force_pipe() {
    let dir = tempdir().expect("tempdir should be created");
    let input_path = dir.path().join("hello-world.txt");
    let output_path = dir.path().join("another.txt");

    std::fs::write(&input_path, "hello").expect("should write input");
    std::fs::write(&output_path, "existing").expect("should seed output");

    let flow = PipeFlow {
        comments: vec![],
        span: Span::default(),
        source: Source::Expression(Expression::Literal(Literal::Path(
            input_path.to_string_lossy().to_string(),
        ))),
        operations: vec![(
            PipeOp::Force,
            Destination::Expression(Expression::Literal(Literal::Path(
                output_path.to_string_lossy().to_string(),
            ))),
        )],
        on_fail: None,
    };

    let mut runtime = trusted_runtime();
    runtime
        .execute_flow(&flow)
        .await
        .expect("flow should execute");

    let output = std::fs::read_to_string(&output_path).expect("should read output");
    assert_eq!(output, "hello");
}

#[tokio::test(flavor = "multi_thread")]
async fn atomic_rolls_back_file_write_on_failure() {
    let dir = tempdir().expect("tempdir should be created");
    let output_path = dir.path().join("atomic.txt");

    let flow = PipeFlow {
        comments: vec![],
        span: Span::default(),
        source: Source::Expression(Expression::Literal(Literal::String("hello".to_string()))),
        operations: vec![
            (
                PipeOp::Safe,
                Destination::Directive(DirectiveFlow {
                    span: Span::default(),
                    name: "atomic".to_string(),
                    arguments: vec![],
                    named_arguments: vec![],
                    alias: None,
                }),
            ),
            (
                PipeOp::Safe,
                Destination::Expression(Expression::Literal(Literal::Path(
                    output_path.to_string_lossy().to_string(),
                ))),
            ),
            (
                PipeOp::Safe,
                Destination::FunctionCall(FunctionCall {
                    span: Span::default(),
                    name: "unknown_function".to_string(),
                    arguments: vec![],
                    named_arguments: vec![],
                    alias: None,
                }),
            ),
        ],
        on_fail: None,
    };

    let mut runtime = trusted_runtime();
    let result = runtime.execute_flow(&flow).await;
    assert!(result.is_err(), "flow should fail");
    assert!(!output_path.exists(), "output should be rolled back");
}

#[tokio::test(flavor = "multi_thread")]
async fn user_blueprint_receives_piped_first_argument() {
    let dir = tempdir().expect("tempdir should be created");
    let output_path = dir.path().join("result.txt");

    let flow = PipeFlow {
        comments: vec![],
        span: Span::default(),
        source: Source::Expression(Expression::Literal(Literal::String("hi".to_string()))),
        operations: vec![
            (
                PipeOp::Safe,
                Destination::FunctionCall(FunctionCall {
                    span: Span::default(),
                    name: "greet".to_string(),
                    arguments: vec![],
                    named_arguments: vec![],
                    alias: None,
                }),
            ),
            (
                PipeOp::Safe,
                Destination::Expression(Expression::Literal(Literal::Path(
                    output_path.to_string_lossy().to_string(),
                ))),
            ),
        ],
        on_fail: None,
    };

    let mut runtime = trusted_runtime_with_script_dir(dir.path().to_str().unwrap());
    runtime.env.register_function(FunctionDef {
        comments: vec![],
        span: Span::default(),
        name: "greet".to_string(),
        parameters: vec!["x".to_string()],
        body: FlowOrBranch::Flow(Box::new(PipeFlow {
            comments: vec![],
            span: Span::default(),
            source: Source::Expression(Expression::BinaryOp(
                Box::new(Expression::Identifier("x".to_string())),
                "+".to_string(),
                Box::new(Expression::Literal(Literal::String(" there".to_string()))),
            )),
            operations: vec![],
            on_fail: None,
        })),
    });

    runtime
        .execute_flow(&flow)
        .await
        .expect("flow should execute");

    let output = std::fs::read_to_string(&output_path).expect("should read output");
    assert!(output.contains("hi there"));
}

#[tokio::test(flavor = "multi_thread")]
async fn identifier_destination_invokes_user_function_shorthand() {
    let flow = PipeFlow {
        comments: vec![],
        span: Span::default(),
        source: Source::Expression(Expression::Literal(Literal::String("hi".to_string()))),
        operations: vec![(
            PipeOp::Safe,
            Destination::Expression(Expression::Identifier("add_bang".to_string())),
        )],
        on_fail: None,
    };

    let mut runtime = trusted_runtime();
    runtime.env.register_function(FunctionDef {
        comments: vec![],
        span: Span::default(),
        name: "add_bang".to_string(),
        parameters: vec!["x".to_string()],
        body: FlowOrBranch::Flow(Box::new(PipeFlow {
            comments: vec![],
            span: Span::default(),
            source: Source::Expression(Expression::BinaryOp(
                Box::new(Expression::Identifier("x".to_string())),
                "+".to_string(),
                Box::new(Expression::Literal(Literal::String("!".to_string()))),
            )),
            operations: vec![],
            on_fail: None,
        })),
    });

    let result = runtime
        .execute_flow(&flow)
        .await
        .expect("flow should execute");
    assert_eq!(result.as_string(), "hi!");
}

#[tokio::test(flavor = "multi_thread")]
async fn chunk_directive_rejects_zero_size() {
    let dir = tempdir().expect("tempdir");
    let input = dir.path().join("tiny.txt");
    std::fs::write(&input, "abc").expect("write");

    let flow = PipeFlow {
        comments: vec![],
        span: Span::default(),
        source: Source::Expression(Expression::Literal(Literal::Path(
            input.to_string_lossy().to_string(),
        ))),
        operations: vec![(
            PipeOp::Safe,
            Destination::Directive(DirectiveFlow {
                span: Span::default(),
                name: "chunk".to_string(),
                arguments: vec![Expression::Literal(Literal::String("0".to_string()))],
                named_arguments: vec![],
                alias: None,
            }),
        )],
        on_fail: None,
    };
    let mut runtime = trusted_runtime_with_script_dir(dir.path().to_str().unwrap());
    let result = runtime.execute_flow(&flow).await;
    assert!(result.is_err(), "chunk with size 0 should fail");
}

#[tokio::test(flavor = "multi_thread")]
async fn concat_function_joins_values() {
    let mut runtime = trusted_runtime();
    let result = call_builtin(
        &mut runtime,
        "concat",
        vec![
            Expression::Literal(Literal::String("hello".to_string())),
            Expression::Literal(Literal::String(" ".to_string())),
            Expression::Literal(Literal::String("world".to_string())),
        ],
    )
    .await
    .expect("concat should succeed");
    assert_eq!(result.as_string(), "hello world");
}

#[tokio::test(flavor = "multi_thread")]
async fn exists_function_returns_true_for_existing_file() {
    let dir = tempdir().expect("tempdir");
    let file = dir.path().join("present.txt");
    std::fs::write(&file, "x").expect("write");

    let mut runtime = trusted_runtime();
    let result = call_builtin(
        &mut runtime,
        "exists",
        vec![Expression::Literal(Literal::String(
            file.to_string_lossy().to_string(),
        ))],
    )
    .await
    .expect("exists should succeed");
    assert!(matches!(result, Value::Boolean(true)));
}

#[tokio::test(flavor = "multi_thread")]
async fn print_function_returns_argument_as_string() {
    let mut runtime = trusted_runtime();
    let result = call_builtin(
        &mut runtime,
        "print",
        vec![Expression::Literal(Literal::Number(42.0))],
    )
    .await
    .expect("print should succeed");
    assert_eq!(result.as_string(), "42");
}

#[tokio::test(flavor = "multi_thread")]
async fn full_pipeline_read_parse_filter_write() {
    let dir = tempdir().expect("tempdir");
    let csv_input = dir.path().join("products.csv");
    let csv_output = dir.path().join("expensive.csv");

    std::fs::write(
        &csv_input,
        "name,price\nWidget,500\nGadget,1500\nThing,200\n",
    )
    .expect("write csv");

    let flow = PipeFlow {
        comments: vec![],
        span: Span::default(),
        source: Source::Directive(DirectiveFlow {
            span: Span::default(),
            name: "read".to_string(),
            arguments: vec![Expression::Literal(Literal::String(
                csv_input.to_string_lossy().to_string(),
            ))],
            named_arguments: vec![],
            alias: None,
        }),
        operations: vec![
            (
                PipeOp::Safe,
                Destination::Directive(DirectiveFlow {
                    span: Span::default(),
                    name: "csv.parse".to_string(),
                    arguments: vec![],
                    named_arguments: vec![],
                    alias: None,
                }),
            ),
            (
                PipeOp::Safe,
                Destination::FunctionCall(FunctionCall {
                    span: Span::default(),
                    name: "filter".to_string(),
                    arguments: vec![Expression::Lambda(Lambda {
                        span: Span::default(),
                        param: "r".to_string(),
                        body: Box::new(Expression::BinaryOp(
                            Box::new(Expression::MemberAccess(vec![
                                "r".to_string(),
                                "price".to_string(),
                            ])),
                            ">".to_string(),
                            Box::new(Expression::Literal(Literal::Number(1000.0))),
                        )),
                    })],
                    named_arguments: vec![],
                    alias: None,
                }),
            ),
            (
                PipeOp::Safe,
                Destination::Expression(Expression::Literal(Literal::Path(
                    csv_output.to_string_lossy().to_string(),
                ))),
            ),
        ],
        on_fail: None,
    };

    let mut runtime = trusted_runtime_with_script_dir(dir.path().to_str().unwrap());
    runtime
        .execute_flow(&flow)
        .await
        .expect("pipeline should run");

    let out = std::fs::read_to_string(csv_output).expect("read output");
    assert!(out.contains("Gadget"));
    assert!(!out.contains("Widget"));
}

#[tokio::test(flavor = "multi_thread")]
async fn max_file_size_limit_blocks_large_reads() {
    let dir = tempdir().expect("tempdir");
    let input_path = dir.path().join("big.txt");
    let output_path = dir.path().join("out.txt");
    std::fs::write(&input_path, "1234567890").expect("write input");

    let flow = PipeFlow {
        comments: vec![],
        span: Span::default(),
        source: Source::Expression(Expression::Literal(Literal::Path(
            input_path.to_string_lossy().to_string(),
        ))),
        operations: vec![(
            PipeOp::Safe,
            Destination::Expression(Expression::Literal(Literal::Path(
                output_path.to_string_lossy().to_string(),
            ))),
        )],
        on_fail: None,
    };

    let mut runtime = trusted_runtime_with_script_dir(dir.path().to_str().unwrap());
    runtime.limits.max_file_size_bytes = 4;
    let result = runtime.execute_flow(&flow).await;
    assert!(result.is_err(), "expected file size limit to fail read");
}

#[tokio::test(flavor = "multi_thread")]
async fn csv_parse_respects_row_limit() {
    let dir = tempdir().expect("tempdir");
    let csv_input = dir.path().join("rows.csv");
    std::fs::write(&csv_input, "name,price\nA,1\nB,2\nC,3\n").expect("write csv");

    let flow = PipeFlow {
        comments: vec![],
        span: Span::default(),
        source: Source::Expression(Expression::Literal(Literal::Path(
            csv_input.to_string_lossy().to_string(),
        ))),
        operations: vec![(
            PipeOp::Safe,
            Destination::Directive(DirectiveFlow {
                span: Span::default(),
                name: "csv.parse".to_string(),
                arguments: vec![],
                named_arguments: vec![],
                alias: None,
            }),
        )],
        on_fail: None,
    };

    let mut runtime = trusted_runtime_with_script_dir(dir.path().to_str().unwrap());
    runtime.limits.max_rows = 2;
    let result = runtime.execute_flow(&flow).await;
    assert!(result.is_err(), "expected max_rows limit to fail parse");
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_memory_limit_rejects_large_values() {
    let flow = PipeFlow {
        comments: vec![],
        span: Span::default(),
        source: Source::Expression(Expression::Literal(Literal::String("x".repeat(1024)))),
        operations: vec![],
        on_fail: None,
    };

    let mut runtime = trusted_runtime();
    runtime.limits.max_pipeline_memory_bytes = 32;
    runtime.limits.timeout_budget = Duration::from_secs(10);
    let result = runtime.execute_flow(&flow).await;
    assert!(result.is_err(), "expected max pipeline memory to fail");
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_parse_directive_fails_deterministically() {
    let flow = PipeFlow {
        comments: vec![],
        span: Span::default(),
        source: Source::Expression(Expression::Literal(Literal::String("x".to_string()))),
        operations: vec![(
            PipeOp::Safe,
            Destination::Directive(DirectiveFlow {
                span: Span::default(),
                name: "json.parse".to_string(),
                arguments: vec![],
                named_arguments: vec![],
                alias: None,
            }),
        )],
        on_fail: None,
    };

    let mut runtime = trusted_runtime();
    let err = runtime
        .execute_flow(&flow)
        .await
        .expect_err("unknown directives must fail")
        .to_string();
    assert!(err.contains("Unknown directive: @json.parse"));
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_import_path_fails() {
    let program = Program {
        span: Span::default(),
        statements: vec![Statement::Import(ImportStmt {
            path: "does/not/exist".to_string(),
            alias: Some("bad".to_string()),
            comments: vec![],
            span: Span::default(),
        })],
    };

    let mut runtime = trusted_runtime();
    let err = runtime
        .execute(&program)
        .await
        .expect_err("invalid import should fail");
    assert!(err.contains("Import module not found"));
}

#[tokio::test(flavor = "multi_thread")]
async fn force_pipe_preserves_previous_value_on_partial_failure() {
    let dir = tempdir().expect("tempdir");
    let output_path = dir.path().join("force.txt");

    let flow = PipeFlow {
        comments: vec![],
        span: Span::default(),
        source: Source::Expression(Expression::Literal(Literal::String("safe".to_string()))),
        operations: vec![
            (
                PipeOp::Force,
                Destination::FunctionCall(FunctionCall {
                    span: Span::default(),
                    name: "missing_function".to_string(),
                    arguments: vec![],
                    named_arguments: vec![],
                    alias: None,
                }),
            ),
            (
                PipeOp::Safe,
                Destination::Expression(Expression::Literal(Literal::Path(
                    output_path.to_string_lossy().to_string(),
                ))),
            ),
        ],
        on_fail: None,
    };

    let mut runtime = trusted_runtime();
    runtime
        .execute_flow(&flow)
        .await
        .expect("force should keep pipeline alive");

    let output = std::fs::read_to_string(&output_path).expect("output should exist");
    assert_eq!(output, "safe\n");
}

#[tokio::test(flavor = "multi_thread")]
async fn atomic_rollback_restores_existing_file_contents() {
    let dir = tempdir().expect("tempdir");
    let output_path = dir.path().join("restore.txt");
    std::fs::write(&output_path, "before").expect("seed output");

    let flow = PipeFlow {
        comments: vec![],
        span: Span::default(),
        source: Source::Expression(Expression::Literal(Literal::String("after".to_string()))),
        operations: vec![
            (
                PipeOp::Safe,
                Destination::Directive(DirectiveFlow {
                    span: Span::default(),
                    name: "atomic".to_string(),
                    arguments: vec![],
                    named_arguments: vec![],
                    alias: None,
                }),
            ),
            (
                PipeOp::Force,
                Destination::Expression(Expression::Literal(Literal::Path(
                    output_path.to_string_lossy().to_string(),
                ))),
            ),
            (
                PipeOp::Safe,
                Destination::FunctionCall(FunctionCall {
                    span: Span::default(),
                    name: "unknown_function".to_string(),
                    arguments: vec![],
                    named_arguments: vec![],
                    alias: None,
                }),
            ),
        ],
        on_fail: None,
    };

    let mut runtime = trusted_runtime();
    let result = runtime.execute_flow(&flow).await;
    assert!(result.is_err(), "flow should fail");

    let output = std::fs::read_to_string(&output_path).expect("output should still exist");
    assert_eq!(output, "before");
}

#[tokio::test(flavor = "multi_thread")]
async fn on_fail_handler_recovers_from_step_failure() {
    let dir = tempdir().expect("tempdir");
    let output_path = dir.path().join("recovered.txt");

    let flow = PipeFlow {
        comments: vec![],
        span: Span::default(),
        source: Source::Expression(Expression::Literal(Literal::String("start".to_string()))),
        operations: vec![(
            PipeOp::Safe,
            Destination::FunctionCall(FunctionCall {
                span: Span::default(),
                name: "missing_function".to_string(),
                arguments: vec![],
                named_arguments: vec![],
                alias: None,
            }),
        )],
        on_fail: Some(OnFail {
            alias: Some("err".to_string()),
            span: Span::default(),
            handler: Box::new(FlowOrBranch::Flow(Box::new(PipeFlow {
                comments: vec![],
                span: Span::default(),
                source: Source::Expression(Expression::Identifier("err".to_string())),
                operations: vec![(
                    PipeOp::Safe,
                    Destination::Expression(Expression::Literal(Literal::Path(
                        output_path.to_string_lossy().to_string(),
                    ))),
                )],
                on_fail: None,
            }))),
        }),
    };

    let mut runtime = trusted_runtime();
    runtime
        .execute_flow(&flow)
        .await
        .expect("on_fail should recover flow");

    let output = std::fs::read_to_string(&output_path).expect("output should exist");
    assert!(output.contains("Unknown function: missing_function"));
}

#[tokio::test(flavor = "multi_thread")]
async fn filter_rejection_does_not_trigger_on_fail_handler() {
    let dir = tempdir().expect("tempdir");
    let marker_path = dir.path().join("on_fail_should_not_run.txt");

    let flow = PipeFlow {
        comments: vec![],
        span: Span::default(),
        source: Source::Expression(Expression::Literal(Literal::String("start".to_string()))),
        operations: vec![(
            PipeOp::Safe,
            Destination::FunctionCall(FunctionCall {
                span: Span::default(),
                name: "filter".to_string(),
                arguments: vec![Expression::Literal(Literal::Boolean(false))],
                named_arguments: vec![],
                alias: None,
            }),
        )],
        on_fail: Some(OnFail {
            alias: Some("err".to_string()),
            span: Span::default(),
            handler: Box::new(FlowOrBranch::Flow(Box::new(PipeFlow {
                comments: vec![],
                span: Span::default(),
                source: Source::Expression(Expression::Literal(Literal::String(
                    "unexpected".to_string(),
                ))),
                operations: vec![(
                    PipeOp::Safe,
                    Destination::Expression(Expression::Literal(Literal::Path(
                        marker_path.to_string_lossy().to_string(),
                    ))),
                )],
                on_fail: None,
            }))),
        }),
    };

    let mut runtime = trusted_runtime();
    let result = runtime.execute_flow(&flow).await;
    assert!(result.is_err(), "filter rejection should stop this flow");
    assert_eq!(
        result.expect_err("expected filter rejection").to_string(),
        "Filter condition failed"
    );
    assert!(
        !marker_path.exists(),
        "on_fail handler should not run on filter rejection"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn http_post_uses_piped_body_and_writes_response_to_file() {
    let dir = tempdir().expect("tempdir");
    let out_path = dir.path().join("response.txt");
    let url = "mock://api.local/post?echo_body=1".to_string();
    let source = format!(
        "@import \"std.http\" as http\n42 >> @http.post(\\\"{}\") >> \"{}\"",
        url,
        out_path.to_string_lossy()
    );
    let program = loom::parser::parse(&source).expect("parse");

    let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().expect("path"));
    runtime
        .set_security_policy(SecurityPolicy::allow_all())
        .expect("policy");
    runtime.set_trust_mode(TrustMode::Trusted);
    runtime.execute(&program).await.expect("execute");

    let output = std::fs::read_to_string(out_path).expect("read output");
    assert_eq!(output, "42\n");
}

#[tokio::test(flavor = "multi_thread")]
async fn http_post_named_data_overrides_pipe_and_sends_headers() {
    let dir = tempdir().expect("tempdir");
    let out_path = dir.path().join("auth.txt");
    let url = "mock://api.local/post?echo_header=authorization".to_string();
    let source = format!(
        "@import \"std.http\" as http\n100 >> @http.post(url: \\\"{}\", headers: {{ \"Authorization\": \\\"Bearer abc\" }}, data: \\\"override\") >> \"{}\"",
        url,
        out_path.to_string_lossy()
    );
    let program = loom::parser::parse(&source).expect("parse");

    let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().expect("path"));
    runtime
        .set_security_policy(SecurityPolicy::allow_all())
        .expect("policy");
    runtime.set_trust_mode(TrustMode::Trusted);
    runtime.execute(&program).await.expect("execute");

    let output = std::fs::read_to_string(out_path).expect("read output");
    assert_eq!(output, "Bearer abc\n");
}

#[tokio::test(flavor = "multi_thread")]
async fn http_post_sets_default_content_type_for_string_body() {
    let dir = tempdir().expect("tempdir");
    let out_path = dir.path().join("content_type.txt");
    let url = "mock://api.local/post?echo_header=content-type".to_string();
    let source = format!(
        "@import \"std.http\" as http\n\\\"Hello, World!\" >> @http.post(\\\"{}\") >> \"{}\"",
        url,
        out_path.to_string_lossy()
    );
    let program = loom::parser::parse(&source).expect("parse");

    let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().expect("path"));
    runtime
        .set_security_policy(SecurityPolicy::allow_all())
        .expect("policy");
    runtime.set_trust_mode(TrustMode::Trusted);
    runtime.execute(&program).await.expect("execute");

    let output = std::fs::read_to_string(out_path).expect("read output");
    assert_eq!(output, "text/plain; charset=utf-8\n");
}

#[tokio::test(flavor = "multi_thread")]
async fn http_post_requires_std_http_import() {
    let source = "42 >> @http.post(\\\"mock://api.local/post?echo_body=1\")".to_string();
    let program = loom::parser::parse(&source).expect("parse");

    let mut runtime = trusted_runtime();
    let err = runtime
        .execute(&program)
        .await
        .expect_err("expected failure");
    assert!(err.contains("requires @import \"std.http\" as http"));
}

#[tokio::test(flavor = "multi_thread")]
async fn http_post_respects_network_host_allowlist() {
    let dir = tempdir().expect("tempdir");
    let url = "mock://blocked.local/post?echo_body=1".to_string();
    let source = format!(
        "@import \"std.http\" as http\n42 >> @http.post(\\\"{}\")",
        url
    );
    let program = loom::parser::parse(&source).expect("parse");

    let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().expect("path"));
    runtime
        .set_security_policy(SecurityPolicy::restricted().with_network_hosts(vec![]))
        .expect("policy");
    runtime.set_trust_mode(TrustMode::Trusted);
    let err = runtime
        .execute(&program)
        .await
        .expect_err("expected unauthorized host");
    assert!(err.contains("Unauthorized Network access"));
}

#[tokio::test(flavor = "multi_thread")]
async fn secret_expression_resolves_from_dotenv_file() {
    let dir = tempdir().expect("tempdir");
    std::fs::write(dir.path().join(".env"), "NAME=from-dotenv\n").expect("write .env");
    let out_path = dir.path().join("out.txt");
    let source = format!(
        "\\\"hello: \" + @secret(\\\"NAME\") >> \"{}\"",
        out_path.to_string_lossy()
    );
    let program = loom::parser::parse(&source).expect("parse");

    let mut runtime = trusted_runtime_with_script_dir(dir.path().to_str().expect("path"));
    runtime.execute(&program).await.expect("execute");

    let output = std::fs::read_to_string(out_path).expect("read output");
    assert_eq!(output, "hello: from-dotenv\n");
}

#[tokio::test(flavor = "multi_thread")]
async fn secret_reads_key_name_from_path_literal_argument() {
    let dir = tempdir().expect("tempdir");
    std::fs::write(dir.path().join(".env"), "NAME=from-dotenv\n").expect("write .env");
    std::fs::write(dir.path().join("secret-key-name.txt"), "NAME").expect("write key file");
    let out_path = dir.path().join("out.txt");
    let source = format!(
        "@secret(\"{}\") >> \"{}\"",
        dir.path().join("secret-key-name.txt").to_string_lossy(),
        out_path.to_string_lossy()
    );
    let program = loom::parser::parse(&source).expect("parse");

    let mut runtime = trusted_runtime_with_script_dir(dir.path().to_str().expect("path"));
    runtime.execute(&program).await.expect("execute");

    let output = std::fs::read_to_string(out_path).expect("read output");
    assert_eq!(output, "from-dotenv\n");
}

#[tokio::test(flavor = "multi_thread")]
async fn secret_uses_process_env_when_dotenv_key_missing() {
    let dir = tempdir().expect("tempdir");
    let out_path = dir.path().join("out.txt");
    let source = format!("@secret(\\\"PATH\") >> \"{}\"", out_path.to_string_lossy());
    let program = loom::parser::parse(&source).expect("parse");

    let mut runtime = trusted_runtime_with_script_dir(dir.path().to_str().expect("path"));
    runtime.execute(&program).await.expect("execute");

    let output = std::fs::read_to_string(out_path).expect("read output");
    let expected = std::env::var("PATH").expect("PATH should exist");
    assert_eq!(output, format!("{}\n", expected));
}

#[tokio::test(flavor = "multi_thread")]
async fn dotenv_value_takes_precedence_over_process_env() {
    let dir = tempdir().expect("tempdir");
    std::fs::write(dir.path().join(".env"), "PATH=from-dotenv\n").expect("write .env");
    let out_path = dir.path().join("out.txt");
    let source = format!("@secret(\\\"PATH\") >> \"{}\"", out_path.to_string_lossy());
    let program = loom::parser::parse(&source).expect("parse");

    let mut runtime = trusted_runtime_with_script_dir(dir.path().to_str().expect("path"));
    runtime.execute(&program).await.expect("execute");

    let output = std::fs::read_to_string(out_path).expect("read output");
    assert_eq!(output, "from-dotenv\n");
}

#[tokio::test(flavor = "multi_thread")]
async fn secret_missing_key_returns_runtime_error() {
    let dir = tempdir().expect("tempdir");
    let key = format!(
        "LOOM_MISSING_SECRET_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    );
    let source = format!("@secret(\\\"{}\")", key);
    let program = loom::parser::parse(&source).expect("parse");

    let mut runtime = trusted_runtime_with_script_dir(dir.path().to_str().expect("path"));
    let err = runtime
        .execute(&program)
        .await
        .expect_err("missing secret should fail");
    assert!(err.contains(&format!("Missing secret '{}'", key)));
}
