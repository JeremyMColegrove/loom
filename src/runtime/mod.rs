pub mod builtins;
mod directives;
pub mod env;
mod eval;
pub mod fs;
mod functions;
mod imports;
mod pipeline;
mod watch;

use crate::ast::*;
use crate::runtime::builtins::BuiltinRegistry;
use crate::runtime::env::Value;
use crate::runtime::fs::{AtomicContext, AtomicTransaction};
use log::debug;
use std::collections::HashSet;

pub struct Runtime {
    pub env: env::Environment,
    pub builtins: BuiltinRegistry,
    /// Directory of the currently executing script (for resolving imports)
    pub script_dir: Option<String>,
    atomic_active: bool,
    atomic_context: Option<AtomicContext>,
    atomic_txn: Option<AtomicTransaction>,
    callable_sinks: HashSet<String>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl Runtime {
    pub fn new() -> Self {
        let mut env = env::Environment::new();
        env.set("null", Value::Null);
        let (shutdown_tx, _shutdown_rx) = tokio::sync::watch::channel(false);
        Self {
            env,
            builtins: BuiltinRegistry::new(),
            script_dir: None,
            atomic_active: false,
            atomic_context: None,
            atomic_txn: None,
            callable_sinks: HashSet::new(),
            shutdown_tx,
        }
    }

    pub fn with_script_dir(mut self, dir: &str) -> Self {
        self.script_dir = Some(dir.to_string());
        self
    }

    pub fn request_shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    pub fn shutdown_trigger(&self) -> tokio::sync::watch::Sender<bool> {
        self.shutdown_tx.clone()
    }

    pub(crate) fn subscribe_shutdown(&self) -> tokio::sync::watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    pub async fn execute(&mut self, program: &Program) -> Result<(), String> {
        for stmt in &program.statements {
            match stmt {
                Statement::Comment(_) => {}
                Statement::Pipe(flow) => {
                    self.execute_flow(flow).await?;
                }
                Statement::Import(import) => {
                    self.execute_import(import).await?;
                }
                Statement::Function(func_def) => {
                    self.env.register_function(func_def.clone());
                    debug!("registered function: {}", func_def.name);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

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
    async fn safe_pipe_moves_file_when_destination_is_directory() {
        let dir = tempdir().expect("tempdir should be created");
        let src = dir.path().join("input.txt");
        let target_dir = dir.path().join("archive");
        std::fs::write(&src, "hello").expect("should write input");

        let flow = PipeFlow {
            comments: vec![],
            span: Span::default(),
            source: Source::Expression(Expression::Literal(Literal::Path(
                src.to_string_lossy().to_string(),
            ))),
            operations: vec![(
                PipeOp::Safe,
                Destination::Expression(Expression::Literal(Literal::Path(format!(
                    "{}/",
                    target_dir.to_string_lossy()
                )))),
            )],
            on_fail: None,
        };

        let mut runtime = Runtime::new();
        runtime
            .execute_flow(&flow)
            .await
            .expect("flow should execute");

        let moved = target_dir.join("input.txt");
        assert!(!src.exists(), "source should be moved");
        assert!(moved.exists(), "destination file should exist");
        let output = std::fs::read_to_string(&moved).expect("should read moved file");
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
                    Destination::Directive(DirectiveFlow {
                        span: Span::default(),
                        name: "missing.directive".to_string(),
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
        assert!(
            !output_path.exists(),
            "atomic rollback should remove output"
        );
    }

    #[tokio::test]
    async fn user_blueprint_receives_piped_first_argument() {
        let dir = tempdir().expect("tempdir should be created");
        let output_path = dir.path().join("fn.txt");

        let mut runtime = Runtime::new();
        runtime.env.register_function(FunctionDef {
            comments: vec![],
            span: Span::default(),
            name: "identity".to_string(),
            parameters: vec!["input".to_string()],
            body: FlowOrBranch::Flow(PipeFlow {
                comments: vec![],
                span: Span::default(),
                source: Source::Expression(Expression::Identifier("input".to_string())),
                operations: vec![],
                on_fail: None,
            }),
        });

        let flow = PipeFlow {
            comments: vec![],
            span: Span::default(),
            source: Source::Expression(Expression::Literal(Literal::String("hello".to_string()))),
            operations: vec![
                (
                    PipeOp::Safe,
                    Destination::FunctionCall(FunctionCall {
                        span: Span::default(),
                        name: "identity".to_string(),
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

        runtime
            .execute_flow(&flow)
            .await
            .expect("flow should execute");
        let output = std::fs::read_to_string(output_path).expect("should read output");
        assert_eq!(output, "hello\n");
    }

    #[tokio::test]
    async fn import_alias_supports_qualified_function_calls() {
        let dir = tempdir().expect("tempdir should be created");
        let module_path = dir.path().join("tools.loom");
        std::fs::write(&module_path, "identity(v) => v").expect("should write module");
        let script_path = dir.path().join("main.loom");
        let output_path = dir.path().join("out.txt");

        let script = format!(
            "@import \"tools.loom\" as t\n\\\"hello\" >> t.identity() >> \"{}\"",
            output_path.to_string_lossy()
        );
        std::fs::write(&script_path, &script).expect("should write main script");

        let program = crate::parser::parse(&script).expect("script should parse");
        let mut runtime = Runtime::new().with_script_dir(dir.path().to_string_lossy().as_ref());
        runtime
            .execute(&program)
            .await
            .expect("script should execute");

        let output = std::fs::read_to_string(output_path).expect("should read output");
        assert_eq!(output, "hello\n");
    }

    #[tokio::test]
    async fn import_resolves_dot_module_paths() {
        let dir = tempdir().expect("tempdir should be created");
        let module_dir = dir.path().join("pkg");
        std::fs::create_dir_all(&module_dir).expect("should create module dir");
        let module_path = module_dir.join("utils.loom");
        std::fs::write(&module_path, "id(v) => v").expect("should write module");
        let output_path = dir.path().join("dot-out.txt");

        let script = format!(
            "@import \"pkg.utils\" as p\n\\\"ok\" >> p.id() >> \"{}\"",
            output_path.to_string_lossy()
        );
        let program = crate::parser::parse(&script).expect("script should parse");
        let mut runtime = Runtime::new().with_script_dir(dir.path().to_string_lossy().as_ref());
        runtime
            .execute(&program)
            .await
            .expect("script should execute");

        let output = std::fs::read_to_string(output_path).expect("should read output");
        assert_eq!(output, "ok\n");
    }

    #[tokio::test]
    async fn watch_alias_is_bound_for_event_operations() {
        let dir = tempdir().expect("tempdir should be created");
        let watched = dir.path().join("master.txt");
        std::fs::write(&watched, "hello").expect("should create watched file");

        let watch = DirectiveFlow {
            span: Span::default(),
            name: "watch".to_string(),
            arguments: vec![Expression::Literal(Literal::Path(
                watched.to_string_lossy().to_string(),
            ))],
            alias: Some("event".to_string()),
        };

        let flow = PipeFlow {
            comments: vec![],
            span: Span::default(),
            source: Source::Directive(watch.clone()),
            operations: vec![(
                PipeOp::Safe,
                Destination::Expression(Expression::MemberAccess(vec![
                    "event".to_string(),
                    "type".to_string(),
                ])),
            )],
            on_fail: None,
        };

        let mut runtime = Runtime::new();
        let event = runtime
            .make_watch_event(watched.to_string_lossy().as_ref(), "modified")
            .expect("event should be created");

        let result = runtime
            .run_watch_event(&flow, &watch, event)
            .await
            .expect("watch event should execute");
        assert_eq!(result.as_string(), "modified");
    }

    #[tokio::test]
    async fn nested_member_access_path_writes_literal_path_without_read() {
        let dir = tempdir().expect("tempdir should be created");
        let watched = dir.path().join("master.txt");
        let output = dir.path().join("hello_world.txt");
        std::fs::write(&watched, "hello").expect("should create watched file");

        let mut file = std::collections::HashMap::new();
        file.insert(
            "path".to_string(),
            Value::String(watched.to_string_lossy().to_string()),
        );
        let mut event = std::collections::HashMap::new();
        event.insert("file".to_string(), Value::Record(file));

        let flow = PipeFlow {
            comments: vec![],
            span: Span::default(),
            source: Source::Expression(Expression::Literal(Literal::String("seed".to_string()))),
            operations: vec![
                (
                    PipeOp::Safe,
                    Destination::Expression(Expression::MemberAccess(vec![
                        "event".to_string(),
                        "file".to_string(),
                        "path".to_string(),
                    ])),
                ),
                (
                    PipeOp::Safe,
                    Destination::Expression(Expression::Literal(Literal::Path(
                        output.to_string_lossy().to_string(),
                    ))),
                ),
            ],
            on_fail: None,
        };

        let mut runtime = Runtime::new();
        runtime.env.set("event", Value::Record(event));
        runtime
            .execute_flow(&flow)
            .await
            .expect("flow should execute");

        let written = std::fs::read_to_string(&output).expect("should read output");
        assert_eq!(written, format!("{}\n", watched.to_string_lossy()));
    }

    #[tokio::test]
    async fn read_directive_reads_nested_member_access_path() {
        let dir = tempdir().expect("tempdir should be created");
        let watched = dir.path().join("master.txt");
        let output = dir.path().join("hello_world.txt");
        std::fs::write(&watched, "hello").expect("should create watched file");

        let mut file = std::collections::HashMap::new();
        file.insert(
            "path".to_string(),
            Value::String(watched.to_string_lossy().to_string()),
        );
        let mut event = std::collections::HashMap::new();
        event.insert("file".to_string(), Value::Record(file));

        let flow = PipeFlow {
            comments: vec![],
            span: Span::default(),
            source: Source::Expression(Expression::Literal(Literal::String("seed".to_string()))),
            operations: vec![
                (
                    PipeOp::Safe,
                    Destination::Directive(DirectiveFlow {
                        span: Span::default(),
                        name: "read".to_string(),
                        arguments: vec![Expression::MemberAccess(vec![
                            "event".to_string(),
                            "file".to_string(),
                            "path".to_string(),
                        ])],
                        alias: None,
                    }),
                ),
                (
                    PipeOp::Safe,
                    Destination::Expression(Expression::Literal(Literal::Path(
                        output.to_string_lossy().to_string(),
                    ))),
                ),
            ],
            on_fail: None,
        };

        let mut runtime = Runtime::new();
        runtime.env.set("event", Value::Record(event));
        runtime
            .execute_flow(&flow)
            .await
            .expect("flow should execute");

        let written = std::fs::read_to_string(&output).expect("should read output");
        assert_eq!(written, "hello\n");
    }

    #[tokio::test]
    async fn writing_to_two_path_literals_implicitly_reads_first_file() {
        let dir = tempdir().expect("tempdir should be created");
        let dest = dir.path().join("dest.txt");
        let copy = dir.path().join("copy.txt");

        let flow = PipeFlow {
            comments: vec![],
            span: Span::default(),
            source: Source::Expression(Expression::Literal(Literal::String(
                "hello, world!".to_string(),
            ))),
            operations: vec![
                (
                    PipeOp::Safe,
                    Destination::Expression(Expression::Literal(Literal::Path(
                        dest.to_string_lossy().to_string(),
                    ))),
                ),
                (
                    PipeOp::Safe,
                    Destination::Expression(Expression::Literal(Literal::Path(
                        copy.to_string_lossy().to_string(),
                    ))),
                ),
            ],
            on_fail: None,
        };

        let mut runtime = Runtime::new();
        runtime
            .execute_flow(&flow)
            .await
            .expect("flow should execute");

        let dest_contents = std::fs::read_to_string(&dest).expect("should read dest");
        let copy_contents = std::fs::read_to_string(&copy).expect("should read copy");
        assert_eq!(dest_contents, "hello, world!\n");
        assert_eq!(copy_contents, "hello, world!\n");
    }

    #[tokio::test]
    async fn std_csv_import_registers_parse_function() {
        let script = "@import \"std.csv\" as csv\n\\\"name,age
Ada,30\" >> csv.parse() >> parsed";
        let program = crate::parser::parse(script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime
            .execute(&program)
            .await
            .expect("script should execute");

        let parsed = runtime
            .env
            .get("parsed")
            .cloned()
            .expect("parsed should be set");
        let Value::Record(root) = parsed else {
            panic!("parsed should be a record");
        };
        let Some(Value::List(rows)) = root.get("rows") else {
            panic!("rows should be a list");
        };
        assert_eq!(rows.len(), 1);
        let Value::Record(row) = rows[0].clone() else {
            panic!("row should be a record");
        };
        let Some(Value::String(name)) = row.get("name") else {
            panic!("name should be present");
        };
        let Some(Value::String(age)) = row.get("age") else {
            panic!("age should be present");
        };
        assert_eq!(name, "Ada");
        assert_eq!(age, "30");
    }

    #[tokio::test]
    async fn std_csv_member_style_call_uses_import_alias() {
        let script = "@import \"std.csv\" as csv\n\\\"name,age
Ada,30\" >> csv.parse >> parsed";
        let program = crate::parser::parse(script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime
            .execute(&program)
            .await
            .expect("script should execute");

        let parsed = runtime
            .env
            .get("parsed")
            .cloned()
            .expect("parsed should be set");
        let Value::Record(root) = parsed else {
            panic!("parsed should be a record");
        };
        let Some(Value::List(rows)) = root.get("rows") else {
            panic!("rows should be a list");
        };
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn std_csv_parse_reads_file_when_given_path_literal() {
        let dir = tempdir().expect("tempdir should be created");
        let csv_path = dir.path().join("customers.csv");
        std::fs::write(&csv_path, "name,age\nAda,30\nBob,41\n").expect("should write csv");

        let script = format!(
            "@import \"std.csv\" as csv\n\"{}\" >> csv.parse() >> parsed",
            csv_path.to_string_lossy()
        );
        let program = crate::parser::parse(&script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime
            .execute(&program)
            .await
            .expect("script should execute");

        let parsed = runtime
            .env
            .get("parsed")
            .cloned()
            .expect("parsed should be set");
        let Value::Record(root) = parsed else {
            panic!("parsed should be a record");
        };
        let Some(Value::List(rows)) = root.get("rows") else {
            panic!("rows should be a list");
        };
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn std_csv_parse_reports_missing_file_for_path_literal() {
        let script = "@import \"std.csv\" as csv\n\"missing.csv\" >> csv.parse() >> parsed";
        let program = crate::parser::parse(script).expect("script should parse");
        let mut runtime = Runtime::new();
        let err = runtime
            .execute(&program)
            .await
            .expect_err("script should fail");
        assert!(err.contains("Failed to read 'missing.csv'"));
    }

    #[tokio::test]
    async fn filter_directive_style_works_with_lambda() {
        let script = "\\\"name,Index
Ada,91
Bob,10\" >> csv.parse >> @filter(row >> row.Index == \\\"91\") >> filtered";
        let program = crate::parser::parse(script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime.env.register_function(FunctionDef {
            comments: vec![],
            span: Span::default(),
            name: "csv.parse".to_string(),
            parameters: vec!["input".to_string()],
            body: FlowOrBranch::Flow(PipeFlow {
                comments: vec![],
                span: Span::default(),
                source: Source::Expression(Expression::Identifier("input".to_string())),
                operations: vec![(
                    PipeOp::Safe,
                    Destination::Directive(DirectiveFlow {
                        span: Span::default(),
                        name: "csv.parse".to_string(),
                        arguments: vec![],
                        alias: None,
                    }),
                )],
                on_fail: None,
            }),
        });

        runtime
            .execute(&program)
            .await
            .expect("script should execute");
        let filtered = runtime
            .env
            .get("filtered")
            .cloned()
            .expect("filtered should be set");
        let Value::List(rows) = filtered else {
            panic!("filtered should be a list");
        };
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn filter_function_style_accepts_csv_parse_record() {
        let script = "\\\"name,Index
Ada,91
Bob,10\" >> csv.parse() >> filter(row >> row.Index == \\\"91\") >> filtered";
        let program = crate::parser::parse(script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime.env.register_function(FunctionDef {
            comments: vec![],
            span: Span::default(),
            name: "csv.parse".to_string(),
            parameters: vec!["input".to_string()],
            body: FlowOrBranch::Flow(PipeFlow {
                comments: vec![],
                span: Span::default(),
                source: Source::Expression(Expression::Identifier("input".to_string())),
                operations: vec![(
                    PipeOp::Safe,
                    Destination::Directive(DirectiveFlow {
                        span: Span::default(),
                        name: "csv.parse".to_string(),
                        arguments: vec![],
                        alias: None,
                    }),
                )],
                on_fail: None,
            }),
        });

        runtime
            .execute(&program)
            .await
            .expect("script should execute");
        let filtered = runtime
            .env
            .get("filtered")
            .cloned()
            .expect("filtered should be set");
        let Value::List(rows) = filtered else {
            panic!("filtered should be a list");
        };
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn filter_with_lowercase_member_and_numeric_comparison_works() {
        let script = "@import \"std.csv\" as csv\n\\\"Index,name
89,A
91,B
100,C\" >> csv.parse >> filter(row >> row.index > 90) >> filtered";
        let program = crate::parser::parse(script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime
            .execute(&program)
            .await
            .expect("script should execute");

        let filtered = runtime
            .env
            .get("filtered")
            .cloned()
            .expect("filtered should be set");
        let Value::List(rows) = filtered else {
            panic!("filtered should be a list");
        };
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn filter_supports_logical_and_short_circuit() {
        let script = "@import \"std.csv\" as csv\n\\\"Index,name
89,A
91,B\" >> csv.parse >> filter(row >> false && missing_value) >> filtered";
        let program = crate::parser::parse(script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime
            .execute(&program)
            .await
            .expect("script should execute");

        let filtered = runtime
            .env
            .get("filtered")
            .cloned()
            .expect("filtered should be set");
        let Value::List(rows) = filtered else {
            panic!("filtered should be a list");
        };
        assert_eq!(rows.len(), 0);
    }

    #[tokio::test]
    async fn filter_as_binding_iterates_per_row() {
        // filter(...) as row >> [row >>> output] should iterate per-item
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("out.csv");
        let script = format!(
            "@import \"std.csv\" as csv\n\\\"Index,name\n89,A\n91,B\n100,C\" >> csv.parse >> filter(row >> row.index > 90) as row >> [\n    row >>> \"{}\"\n]",
            output.display()
        );
        let program = crate::parser::parse(&script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime
            .execute(&program)
            .await
            .expect("script should execute");
        let content = std::fs::read_to_string(&output).expect("output file should exist");
        // Each row should be written individually (force-appended)
        assert!(content.contains("91"), "Expected row with index 91");
        assert!(content.contains("100"), "Expected row with index 100");
        assert!(
            !content.contains("89"),
            "Should not contain filtered-out row 89"
        );
    }

    #[tokio::test]
    async fn std_out_import_alias_can_be_used_as_pipe_sink_identifier() {
        let script = "@import \"std.out\" as stdout\n\\\"hello\" >> stdout";
        let program = crate::parser::parse(script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime
            .execute(&program)
            .await
            .expect("script should execute");
    }

    #[tokio::test]
    async fn map_function_works_in_pipe_chain() {
        let script = "\\\"Index,name\n1,A\n2,B\" >> csv.parse() >> map(row >> row.name) >> names";
        let program = crate::parser::parse(script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime.env.register_function(FunctionDef {
            comments: vec![],
            span: Span::default(),
            name: "csv.parse".to_string(),
            parameters: vec!["input".to_string()],
            body: FlowOrBranch::Flow(PipeFlow {
                comments: vec![],
                span: Span::default(),
                source: Source::Expression(Expression::Identifier("input".to_string())),
                operations: vec![(
                    PipeOp::Safe,
                    Destination::Directive(DirectiveFlow {
                        span: Span::default(),
                        name: "csv.parse".to_string(),
                        arguments: vec![],
                        alias: None,
                    }),
                )],
                on_fail: None,
            }),
        });

        runtime
            .execute(&program)
            .await
            .expect("script should execute");
        let names = runtime
            .env
            .get("names")
            .cloned()
            .expect("names should be set");
        let Value::List(items) = names else {
            panic!("names should be a list");
        };
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].as_string(), "A");
        assert_eq!(items[1].as_string(), "B");
    }

    #[tokio::test]
    async fn writing_filtered_csv_rows_outputs_csv_format() {
        let dir = tempdir().expect("tempdir should be created");
        let out = dir.path().join("high.csv");
        let script = format!(
            "@import \"std.csv\" as csv\n\\\"Index,name\n89,A\n91,B\n100,C\" >> csv.parse >> filter(row >> row.index > 90) >>> \"{}\"",
            out.to_string_lossy()
        );
        let program = crate::parser::parse(&script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime
            .execute(&program)
            .await
            .expect("script should execute");

        let text = std::fs::read_to_string(out).expect("should read output");
        let lines = text.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 3);
        assert!(lines[0].contains("Index"));
        assert!(lines[0].contains("name"));
        assert!(lines[1].contains("91"));
        assert!(lines[2].contains("100"));
    }

    #[tokio::test]
    async fn csv_parse_handles_quoted_commas_without_extra_columns() {
        let dir = tempdir().expect("tempdir should be created");
        let input = dir.path().join("quoted-input.csv");
        let out = dir.path().join("quoted.csv");
        std::fs::write(
            &input,
            "Index,Company,City\n1,\"Dominguez, Mcmillan and Donovan\",Bensonview\n2,\"Martin, Lang and Andrade\",West Priscilla\n",
        )
        .expect("should write input");
        let script = format!(
            "@import \"std.csv\" as csv\n\"{}\" >> csv.parse >> filter(row >> row.index > 0) >>> \"{}\"",
            input.to_string_lossy(),
            out.to_string_lossy()
        );
        let program = crate::parser::parse(&script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime
            .execute(&program)
            .await
            .expect("script should execute");

        let output = std::fs::read_to_string(&out).expect("should read output");
        assert!(!output.lines().next().unwrap_or("").contains("col13"));

        let mut reader = csv::ReaderBuilder::new()
            .trim(csv::Trim::All)
            .from_reader(output.as_bytes());
        let headers = reader.headers().expect("headers should parse").clone();
        assert_eq!(headers.len(), 3);

        let rows = reader
            .records()
            .map(|r| r.expect("row should parse"))
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 2);
        assert_eq!(&rows[0][1], "Dominguez, Mcmillan and Donovan");
        assert_eq!(&rows[1][1], "Martin, Lang and Andrade");
    }

    #[tokio::test]
    async fn csv_parse_handles_spaces_before_quoted_fields() {
        let dir = tempdir().expect("tempdir should be created");
        let input = dir.path().join("spaced-input.csv");
        std::fs::write(
            &input,
            "Index,Company,City\n1, \"Dominguez, Mcmillan and Donovan\", Bensonview\n",
        )
        .expect("should write input");

        let script = format!(
            "@import \"std.csv\" as csv\n\"{}\" >> csv.parse >> parsed",
            input.to_string_lossy()
        );
        let program = crate::parser::parse(&script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime
            .execute(&program)
            .await
            .expect("script should execute");

        let parsed = runtime
            .env
            .get("parsed")
            .cloned()
            .expect("parsed should be set");
        let Value::Record(root) = parsed else {
            panic!("parsed should be record");
        };
        let Some(Value::List(rows)) = root.get("rows") else {
            panic!("rows should be list");
        };
        let Value::Record(row) = rows[0].clone() else {
            panic!("row should be record");
        };
        let Some(Value::String(company)) = row.get("Company") else {
            panic!("company should be present");
        };
        assert_eq!(company, "Dominguez, Mcmillan and Donovan");
        assert!(!row.contains_key("col4"));
    }

    #[tokio::test]
    async fn branch_flows_consume_same_piped_input_in_order() {
        let dir = tempdir().expect("tempdir should be created");
        let input = dir.path().join("customers.csv");
        let high = dir.path().join("high.csv");
        let audit = dir.path().join("audit_trail.csv");
        std::fs::write(&input, "Index,Company\n90,Alpha\n92,Beta\n100,Gamma\n")
            .expect("should write input");

        let script = format!(
            "@import \"std.csv\" as csv\n\"{}\" >> csv.parse >> data\ndata >> [ filter(row >> row.Index > 91) >> \"{}\", \"{}\" ]",
            input.to_string_lossy(),
            high.to_string_lossy(),
            audit.to_string_lossy()
        );

        let program = crate::parser::parse(&script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime
            .execute(&program)
            .await
            .expect("script should execute");

        let high_text = std::fs::read_to_string(&high).expect("high should be written");
        assert!(high_text.contains("92"));
        assert!(high_text.contains("100"));
        assert!(!high_text.contains("90"));

        let audit_text = std::fs::read_to_string(&audit).expect("audit should be written");
        assert!(audit_text.contains("90"));
        assert!(audit_text.contains("92"));
        assert!(audit_text.contains("100"));
    }

    #[tokio::test]
    async fn branch_path_source_keeps_file_contents_without_appending_input() {
        let dir = tempdir().expect("tempdir should be created");
        let backup = dir.path().join("backup.txt");
        let second = dir.path().join("second_backup.txt");

        let script = format!(
            "@atomic >> [ \\\"Hello, world!\" >>> \"{}\", \"{}\" >>> \"{}\" ]",
            backup.to_string_lossy(),
            backup.to_string_lossy(),
            second.to_string_lossy()
        );

        let program = crate::parser::parse(&script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime
            .execute(&program)
            .await
            .expect("script should execute");

        let backup_text = std::fs::read_to_string(&backup).expect("backup should exist");
        let second_text = std::fs::read_to_string(&second).expect("second should exist");
        assert_eq!(backup_text, "Hello, world!");
        assert_eq!(second_text, "Hello, world!");
    }

    #[tokio::test]
    async fn atomic_branch_failure_rolls_back_and_runs_on_fail_with_pipeop_syntax() {
        let dir = tempdir().expect("tempdir should be created");
        let backup = dir.path().join("backup.txt");
        let second = dir.path().join("second_backup.txt");
        let status = dir.path().join("status.log");
        let failure = dir.path().join("failure.log");

        let script = format!(
            "@atomic >> [ \\\"It's working!\" >>> \"{}\", \"{}\" >>> \"{}\", \\\"done!\" >>> \"{}\" ] on_fail >> \\\"A failure occured!\" >>> \"{}\"",
            backup.to_string_lossy(),
            dir.path().join("backup.txt22").to_string_lossy(),
            second.to_string_lossy(),
            status.to_string_lossy(),
            failure.to_string_lossy()
        );

        let program = crate::parser::parse(&script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime
            .execute(&program)
            .await
            .expect("script should execute via on_fail");

        assert!(
            !backup.exists(),
            "atomic rollback should remove first write"
        );
        assert!(!second.exists(), "later steps should not be executed");
        assert!(!status.exists(), "later steps should not be executed");
        let failure_text = std::fs::read_to_string(&failure).expect("on_fail output should exist");
        assert_eq!(failure_text, "A failure occured!");
    }

    #[test]
    pub(crate) fn watch_path_is_resolved_to_absolute_path() {
        let dir = tempdir().expect("tempdir should be created");
        let original_cwd = std::env::current_dir().expect("should read current dir");
        std::env::set_current_dir(dir.path()).expect("should switch cwd");
        std::fs::write("master.txt", "hello").expect("should create file");

        let runtime = Runtime::new();
        let resolved = runtime
            .absolutize_watch_path("./master.txt")
            .expect("path should resolve");

        std::env::set_current_dir(original_cwd).expect("should restore cwd");
        assert!(std::path::Path::new(&resolved).is_absolute());
    }

    // ── @log directive ──────────────────────────────────────────────────

    #[tokio::test]
    async fn log_directive_passes_through_pipe_value() {
        let dir = tempdir().expect("tempdir");
        let out = dir.path().join("out.txt");

        let program = Program {
            span: Span::default(),
            statements: vec![Statement::Pipe(PipeFlow {
                comments: vec![],
                span: Span::default(),
                source: Source::Expression(Expression::Literal(Literal::String(
                    "hello from log".to_string(),
                ))),
                operations: vec![
                    (
                        PipeOp::Safe,
                        Destination::Directive(DirectiveFlow {
                            span: Span::default(),
                            name: "log".to_string(),
                            arguments: vec![],
                            alias: None,
                        }),
                    ),
                    (
                        PipeOp::Safe,
                        Destination::Expression(Expression::Literal(Literal::Path(
                            out.to_string_lossy().to_string(),
                        ))),
                    ),
                ],
                on_fail: None,
            })],
        };
        let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().unwrap());
        runtime.execute(&program).await.expect("should execute");

        let content = std::fs::read_to_string(&out).expect("should read output");
        assert!(content.contains("hello from log"));
    }

    // ── @write directive ────────────────────────────────────────────────

    #[tokio::test]
    async fn write_directive_writes_content_to_file() {
        let dir = tempdir().expect("tempdir");
        let out = dir.path().join("written.txt");

        let flow = PipeFlow {
            comments: vec![],
            span: Span::default(),
            source: Source::Expression(Expression::Literal(Literal::String(
                "written data".to_string(),
            ))),
            operations: vec![(
                PipeOp::Safe,
                Destination::Directive(DirectiveFlow {
                    span: Span::default(),
                    name: "write".to_string(),
                    arguments: vec![Expression::Literal(Literal::Path(
                        out.to_string_lossy().to_string(),
                    ))],
                    alias: None,
                }),
            )],
            on_fail: None,
        };
        let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().unwrap());
        runtime.execute_flow(&flow).await.expect("should execute");

        let content = std::fs::read_to_string(&out).expect("should read output");
        assert_eq!(content, "written data");
    }

    // ── @lines directive ────────────────────────────────────────────────

    #[tokio::test]
    async fn lines_directive_reads_file_into_list() {
        let dir = tempdir().expect("tempdir");
        let input = dir.path().join("data.txt");
        std::fs::write(&input, "alpha\nbeta\ngamma").expect("write input");

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
                    name: "lines".to_string(),
                    arguments: vec![],
                    alias: Some("data".to_string()),
                }),
            )],
            on_fail: None,
        };
        let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().unwrap());
        let result = runtime.execute_flow(&flow).await.expect("should execute");

        match result {
            Value::List(items) => {
                assert_eq!(items.len(), 3);
                assert_eq!(items[0].as_string(), "alpha");
                assert_eq!(items[1].as_string(), "beta");
                assert_eq!(items[2].as_string(), "gamma");
            }
            _ => panic!("expected list, got {:?}", result),
        }
    }

    // ── @chunk directive ────────────────────────────────────────────────

    #[tokio::test]
    async fn chunk_directive_splits_file_into_chunks() {
        let dir = tempdir().expect("tempdir");
        let input = dir.path().join("big.txt");
        // 10 bytes of data, chunk at 4 bytes → 3 chunks
        std::fs::write(&input, "0123456789").expect("write input");

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
                    arguments: vec![Expression::Literal(Literal::String("4".to_string()))],
                    alias: None,
                }),
            )],
            on_fail: None,
        };
        let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().unwrap());
        let result = runtime.execute_flow(&flow).await.expect("should execute");

        match result {
            Value::List(chunks) => {
                assert_eq!(chunks.len(), 3);
                assert_eq!(chunks[0].as_string(), "0123");
                assert_eq!(chunks[1].as_string(), "4567");
                assert_eq!(chunks[2].as_string(), "89");
            }
            _ => panic!("expected list of chunks, got {:?}", result),
        }
    }

    #[tokio::test]
    async fn chunk_directive_rejects_zero_size() {
        let dir = tempdir().expect("tempdir");
        let input = dir.path().join("data.txt");
        std::fs::write(&input, "abc").expect("write input");

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

    // ── Builtin functions ───────────────────────────────────────────────

    #[tokio::test]
    async fn concat_function_joins_values() {
        let mut runtime = Runtime::new();
        let result = runtime
            .call_function(
                "concat",
                vec![
                    Value::String("hello".to_string()),
                    Value::String(" ".to_string()),
                    Value::String("world".to_string()),
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
        let result = runtime
            .call_function(
                "exists",
                vec![Value::String(file.to_string_lossy().to_string())],
            )
            .await
            .expect("exists should succeed");
        assert!(matches!(result, Value::Boolean(true)));
    }

    #[tokio::test]
    async fn exists_function_returns_false_for_missing_file() {
        let mut runtime = Runtime::new();
        let result = runtime
            .call_function(
                "exists",
                vec![Value::String("/nonexistent/file.txt".to_string())],
            )
            .await
            .expect("exists should succeed");
        assert!(matches!(result, Value::Boolean(false)));
    }

    #[tokio::test]
    async fn print_function_returns_argument_as_string() {
        let mut runtime = Runtime::new();
        let result = runtime
            .call_function("print", vec![Value::Number(42.0)])
            .await
            .expect("print should succeed");
        assert_eq!(result.as_string(), "42");
    }

    // ── Expression evaluation at runtime ────────────────────────────────

    #[tokio::test]
    async fn binary_op_arithmetic_in_expression() {
        let mut runtime = Runtime::new();
        // 2 + 3 * 4 should be 14 (precedence: * before +)
        let expr = Expression::BinaryOp(
            Box::new(Expression::Literal(Literal::Number(2.0))),
            "+".to_string(),
            Box::new(Expression::BinaryOp(
                Box::new(Expression::Literal(Literal::Number(3.0))),
                "*".to_string(),
                Box::new(Expression::Literal(Literal::Number(4.0))),
            )),
        );
        let result = runtime.eval_expression(&expr).await.expect("should eval");
        assert!(matches!(result, Value::Number(n) if n == 14.0));
    }

    #[tokio::test]
    async fn binary_op_string_concatenation() {
        let mut runtime = Runtime::new();
        let expr = Expression::BinaryOp(
            Box::new(Expression::Literal(Literal::String("foo".to_string()))),
            "+".to_string(),
            Box::new(Expression::Literal(Literal::String("bar".to_string()))),
        );
        let result = runtime.eval_expression(&expr).await.expect("should eval");
        assert_eq!(result.as_string(), "foobar");
    }

    #[tokio::test]
    async fn binary_op_division_by_zero_errors() {
        let mut runtime = Runtime::new();
        let expr = Expression::BinaryOp(
            Box::new(Expression::Literal(Literal::Number(10.0))),
            "/".to_string(),
            Box::new(Expression::Literal(Literal::Number(0.0))),
        );
        let result = runtime.eval_expression(&expr).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Division by zero"));
    }

    #[tokio::test]
    async fn binary_op_comparison_operators() {
        let mut runtime = Runtime::new();
        // 5 > 3
        let expr = Expression::BinaryOp(
            Box::new(Expression::Literal(Literal::Number(5.0))),
            ">".to_string(),
            Box::new(Expression::Literal(Literal::Number(3.0))),
        );
        let result = runtime.eval_expression(&expr).await.expect("should eval");
        assert!(matches!(result, Value::Boolean(true)));

        // 5 <= 3
        let expr2 = Expression::BinaryOp(
            Box::new(Expression::Literal(Literal::Number(5.0))),
            "<=".to_string(),
            Box::new(Expression::Literal(Literal::Number(3.0))),
        );
        let result2 = runtime.eval_expression(&expr2).await.expect("should eval");
        assert!(matches!(result2, Value::Boolean(false)));
    }

    #[tokio::test]
    async fn logical_and_short_circuits_on_false() {
        let mut runtime = Runtime::new();
        // false && <undefined_var> should NOT error — short-circuit
        let expr = Expression::BinaryOp(
            Box::new(Expression::Literal(Literal::Boolean(false))),
            "&&".to_string(),
            Box::new(Expression::Identifier("undefined_thing".to_string())),
        );
        let result = runtime
            .eval_expression(&expr)
            .await
            .expect("should short-circuit");
        assert!(matches!(result, Value::Boolean(false)));
    }

    #[tokio::test]
    async fn logical_or_short_circuits_on_true() {
        let mut runtime = Runtime::new();
        // true || <undefined_var> should NOT error — short-circuit
        let expr = Expression::BinaryOp(
            Box::new(Expression::Literal(Literal::Boolean(true))),
            "||".to_string(),
            Box::new(Expression::Identifier("undefined_thing".to_string())),
        );
        let result = runtime
            .eval_expression(&expr)
            .await
            .expect("should short-circuit");
        assert!(matches!(result, Value::Boolean(true)));
    }

    #[tokio::test]
    async fn unary_not_on_boolean() {
        let mut runtime = Runtime::new();
        let expr = Expression::UnaryOp(
            "!".to_string(),
            Box::new(Expression::Literal(Literal::Boolean(true))),
        );
        let result = runtime.eval_expression(&expr).await.expect("should eval");
        assert!(matches!(result, Value::Boolean(false)));
    }

    #[tokio::test]
    async fn unary_not_on_non_boolean_errors() {
        let mut runtime = Runtime::new();
        let expr = Expression::UnaryOp(
            "!".to_string(),
            Box::new(Expression::Literal(Literal::Number(42.0))),
        );
        let result = runtime.eval_expression(&expr).await;
        assert!(result.is_err());
    }

    // ── is_truthy edge cases ────────────────────────────────────────────

    #[test]
    pub(crate) fn is_truthy_edge_cases() {
        let runtime = Runtime::new();
        assert!(!runtime.is_truthy(&Value::Null));
        assert!(!runtime.is_truthy(&Value::Boolean(false)));
        assert!(!runtime.is_truthy(&Value::Number(0.0)));
        assert!(runtime.is_truthy(&Value::Boolean(true)));
        assert!(runtime.is_truthy(&Value::Number(1.0)));
        assert!(runtime.is_truthy(&Value::Number(-1.0)));
        assert!(runtime.is_truthy(&Value::String("hello".to_string())));
        assert!(!runtime.is_truthy(&Value::String("".to_string())));
    }

    // ── Multi-stage pipeline ────────────────────────────────────────────

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

        // @read >> @csv.parse >> filter(lambda) >> "output"
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
                        alias: Some("data".to_string()),
                    }),
                ),
                (
                    PipeOp::Safe,
                    Destination::Directive(DirectiveFlow {
                        span: Span::default(),
                        name: "filter".to_string(),
                        arguments: vec![Expression::Lambda(Lambda {
                            span: Span::default(),
                            param: "row".to_string(),
                            body: Box::new(Expression::BinaryOp(
                                Box::new(Expression::MemberAccess(vec![
                                    "row".to_string(),
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
            .expect("pipeline should succeed");

        let output = std::fs::read_to_string(&csv_output).expect("read output");
        assert!(output.contains("Gadget"));
        assert!(!output.contains("Widget"));
        assert!(!output.contains("Thing"));
    }

    // ── @filter with empty list ─────────────────────────────────────────

    #[tokio::test]
    async fn filter_with_empty_list_returns_empty_list() {
        let mut runtime = Runtime::new();
        let result = runtime
            .call_filter(vec![
                Value::List(vec![]),
                Value::Lambda(Lambda {
                    span: Span::default(),
                    param: "x".to_string(),
                    body: Box::new(Expression::Literal(Literal::Boolean(true))),
                }),
            ])
            .await
            .expect("filter should succeed");
        assert!(matches!(result, Value::List(items) if items.is_empty()));
    }

    #[tokio::test]
    async fn filter_with_no_matches_returns_empty_list() {
        let mut runtime = Runtime::new();
        let mut row = std::collections::HashMap::new();
        row.insert("val".to_string(), Value::Number(5.0));
        let result = runtime
            .call_filter(vec![
                Value::List(vec![Value::Record(row)]),
                Value::Lambda(Lambda {
                    span: Span::default(),
                    param: "x".to_string(),
                    body: Box::new(Expression::BinaryOp(
                        Box::new(Expression::MemberAccess(vec![
                            "x".to_string(),
                            "val".to_string(),
                        ])),
                        ">".to_string(),
                        Box::new(Expression::Literal(Literal::Number(100.0))),
                    )),
                }),
            ])
            .await
            .expect("filter should succeed");
        assert!(matches!(result, Value::List(items) if items.is_empty()));
    }

    // ── on_fail error propagation ───────────────────────────────────────

    #[tokio::test]
    async fn on_fail_binds_error_message_to_alias() {
        let dir = tempdir().expect("tempdir");
        let error_log = dir.path().join("err.txt");

        // Source reads a non-existent file → triggers on_fail
        // on_fail as e >> e >> "err.txt"
        let flow = PipeFlow {
            comments: vec![],
            span: Span::default(),
            source: Source::Directive(DirectiveFlow {
                span: Span::default(),
                name: "read".to_string(),
                arguments: vec![Expression::Literal(Literal::Path(
                    "/definitely/not/a/real/file.txt".to_string(),
                ))],
                alias: None,
            }),
            operations: vec![(
                PipeOp::Safe,
                Destination::Expression(Expression::Literal(Literal::Path(
                    "unused.txt".to_string(),
                ))),
            )],
            on_fail: Some(OnFail {
                span: Span::default(),
                alias: Some("e".to_string()),
                handler: Box::new(FlowOrBranch::Flow(PipeFlow {
                    comments: vec![],
                    span: Span::default(),
                    source: Source::Expression(Expression::Identifier("e".to_string())),
                    operations: vec![(
                        PipeOp::Safe,
                        Destination::Expression(Expression::Literal(Literal::Path(
                            error_log.to_string_lossy().to_string(),
                        ))),
                    )],
                    on_fail: None,
                })),
            }),
        };
        let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().unwrap());
        runtime
            .execute_flow(&flow)
            .await
            .expect("on_fail should handle gracefully");

        let err_content = std::fs::read_to_string(&error_log).expect("error log should exist");
        assert!(
            err_content.contains("Failed to read"),
            "error message should mention the failure: {}",
            err_content
        );
    }

    // ── Branch with multiple file outputs ───────────────────────────────

    #[tokio::test]
    async fn branch_fans_out_to_multiple_file_destinations() {
        let dir = tempdir().expect("tempdir");
        let out_a = dir.path().join("a.txt");
        let out_b = dir.path().join("b.txt");

        let flow = PipeFlow {
            comments: vec![],
            span: Span::default(),
            source: Source::Expression(Expression::Literal(Literal::String(
                "shared data".to_string(),
            ))),
            operations: vec![(
                PipeOp::Safe,
                Destination::Branch(Branch {
                    span: Span::default(),
                    items: vec![
                        BranchItem::Flow(PipeFlow {
                            comments: vec![],
                            span: Span::default(),
                            source: Source::Expression(Expression::Literal(Literal::Path(
                                out_a.to_string_lossy().to_string(),
                            ))),
                            operations: vec![],
                            on_fail: None,
                        }),
                        BranchItem::Flow(PipeFlow {
                            comments: vec![],
                            span: Span::default(),
                            source: Source::Expression(Expression::Literal(Literal::Path(
                                out_b.to_string_lossy().to_string(),
                            ))),
                            operations: vec![],
                            on_fail: None,
                        }),
                    ],
                }),
            )],
            on_fail: None,
        };
        let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().unwrap());
        runtime
            .execute_flow(&flow)
            .await
            .expect("branch should succeed");

        let a_content = std::fs::read_to_string(&out_a).expect("a.txt should exist");
        let b_content = std::fs::read_to_string(&out_b).expect("b.txt should exist");
        assert!(a_content.contains("shared data"));
        assert!(b_content.contains("shared data"));
    }

    // ── User-defined function called via pipe ───────────────────────────

    #[tokio::test]
    async fn user_function_called_in_pipeline() {
        let dir = tempdir().expect("tempdir");
        let out = dir.path().join("result.txt");

        // Define: shout(x) => x + "!"
        // Then: "hello" >> shout >> "result.txt"
        let program = Program {
            span: Span::default(),
            statements: vec![
                Statement::Function(FunctionDef {
                    comments: vec![],
                    span: Span::default(),
                    name: "shout".to_string(),
                    parameters: vec!["x".to_string()],
                    body: FlowOrBranch::Flow(PipeFlow {
                        comments: vec![],
                        span: Span::default(),
                        source: Source::Expression(Expression::BinaryOp(
                            Box::new(Expression::Identifier("x".to_string())),
                            "+".to_string(),
                            Box::new(Expression::Literal(Literal::String("!".to_string()))),
                        )),
                        operations: vec![],
                        on_fail: None,
                    }),
                }),
                Statement::Pipe(PipeFlow {
                    comments: vec![],
                    span: Span::default(),
                    source: Source::Expression(Expression::Literal(Literal::String(
                        "hello".to_string(),
                    ))),
                    operations: vec![
                        (
                            PipeOp::Safe,
                            Destination::FunctionCall(FunctionCall {
                                span: Span::default(),
                                name: "shout".to_string(),
                                arguments: vec![],
                                alias: None,
                            }),
                        ),
                        (
                            PipeOp::Safe,
                            Destination::Expression(Expression::Literal(Literal::Path(
                                out.to_string_lossy().to_string(),
                            ))),
                        ),
                    ],
                    on_fail: None,
                }),
            ],
        };
        let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().unwrap());
        runtime.execute(&program).await.expect("should execute");

        let content = std::fs::read_to_string(&out).expect("should read output");
        assert!(content.contains("hello!"));
    }

    // ── @map over list ──────────────────────────────────────────────────

    #[tokio::test]
    async fn map_transforms_each_item_in_list() {
        let mut runtime = Runtime::new();
        let result = runtime
            .call_map(vec![
                Value::List(vec![
                    Value::Number(1.0),
                    Value::Number(2.0),
                    Value::Number(3.0),
                ]),
                Value::Lambda(Lambda {
                    span: Span::default(),
                    param: "x".to_string(),
                    body: Box::new(Expression::BinaryOp(
                        Box::new(Expression::Identifier("x".to_string())),
                        "*".to_string(),
                        Box::new(Expression::Literal(Literal::Number(10.0))),
                    )),
                }),
            ])
            .await
            .expect("map should succeed");

        match result {
            Value::List(items) => {
                assert_eq!(items.len(), 3);
                assert!(matches!(&items[0], Value::Number(n) if *n == 10.0));
                assert!(matches!(&items[1], Value::Number(n) if *n == 20.0));
                assert!(matches!(&items[2], Value::Number(n) if *n == 30.0));
            }
            _ => panic!("expected list, got {:?}", result),
        }
    }
}
