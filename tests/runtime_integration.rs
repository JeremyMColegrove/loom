use loom::ast::*;
use loom::runtime::Runtime;
use loom::runtime::env::Value;
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
            alias: None,
        }),
        operations: vec![],
        on_fail: None,
    };
    runtime.execute_flow(&flow).await.map_err(|e| e.to_string())
}

#[tokio::test]
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

    let mut runtime = Runtime::new();
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

#[tokio::test]
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

    let mut runtime = Runtime::new();
    runtime
        .execute_flow(&flow)
        .await
        .expect("flow should execute");

    let output = std::fs::read_to_string(&output_path).expect("should read output");
    assert_eq!(output, "hello");
}

#[tokio::test]
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
                    alias: None,
                }),
            ),
        ],
        on_fail: None,
    };

    let mut runtime = Runtime::new();
    let result = runtime.execute_flow(&flow).await;
    assert!(result.is_err(), "flow should fail");
    assert!(!output_path.exists(), "output should be rolled back");
}

#[tokio::test]
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

    let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().unwrap());
    runtime.env.register_function(FunctionDef {
        comments: vec![],
        span: Span::default(),
        name: "greet".to_string(),
        parameters: vec!["x".to_string()],
        body: FlowOrBranch::Flow(PipeFlow {
            comments: vec![],
            span: Span::default(),
            source: Source::Expression(Expression::BinaryOp(
                Box::new(Expression::Identifier("x".to_string())),
                "+".to_string(),
                Box::new(Expression::Literal(Literal::String(" there".to_string()))),
            )),
            operations: vec![],
            on_fail: None,
        }),
    });

    runtime
        .execute_flow(&flow)
        .await
        .expect("flow should execute");

    let output = std::fs::read_to_string(&output_path).expect("should read output");
    assert!(output.contains("hi there"));
}

#[tokio::test]
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
                alias: None,
            }),
        )],
        on_fail: None,
    };
    let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().unwrap());
    let result = runtime.execute_flow(&flow).await;
    assert!(result.is_err(), "chunk with size 0 should fail");
}

#[tokio::test]
async fn concat_function_joins_values() {
    let mut runtime = Runtime::new();
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

#[tokio::test]
async fn exists_function_returns_true_for_existing_file() {
    let dir = tempdir().expect("tempdir");
    let file = dir.path().join("present.txt");
    std::fs::write(&file, "x").expect("write");

    let mut runtime = Runtime::new();
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

#[tokio::test]
async fn print_function_returns_argument_as_string() {
    let mut runtime = Runtime::new();
    let result = call_builtin(
        &mut runtime,
        "print",
        vec![Expression::Literal(Literal::Number(42.0))],
    )
    .await
    .expect("print should succeed");
    assert_eq!(result.as_string(), "42");
}

#[tokio::test]
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
            arguments: vec![Expression::Literal(Literal::Path(
                csv_input.to_string_lossy().to_string(),
            ))],
            alias: None,
        }),
        operations: vec![
            (
                PipeOp::Safe,
                Destination::Directive(DirectiveFlow {
                    span: Span::default(),
                    name: "csv.parse".to_string(),
                    arguments: vec![],
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

    let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().unwrap());
    runtime
        .execute_flow(&flow)
        .await
        .expect("pipeline should run");

    let out = std::fs::read_to_string(csv_output).expect("read output");
    assert!(out.contains("Gadget"));
    assert!(!out.contains("Widget"));
}
