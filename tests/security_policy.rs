use loom::ast::{Destination, DirectiveFlow, Expression, Literal, PipeFlow, PipeOp, Source, Span};
use loom::runtime::Runtime;
use loom::runtime::security::{AuditOperation, AuditOutcome, SecurityPolicy, TrustMode};
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread")]
async fn blocks_read_outside_read_paths() {
    let allowed = tempdir().expect("allowed tempdir");
    let denied = tempdir().expect("denied tempdir");

    let denied_input = denied.path().join("secret.txt");
    std::fs::write(&denied_input, "nope").expect("write denied input");
    let allowed_output = allowed.path().join("out.txt");

    let flow = PipeFlow {
        comments: vec![],
        span: Span::default(),
        source: Source::Expression(Expression::Literal(Literal::Path(
            denied_input.to_string_lossy().to_string(),
        ))),
        operations: vec![(
            PipeOp::Safe,
            Destination::Expression(Expression::Literal(Literal::Path(
                allowed_output.to_string_lossy().to_string(),
            ))),
        )],
        on_fail: None,
    };

    let policy = SecurityPolicy::restricted()
        .with_read_paths(vec![allowed.path().to_path_buf()])
        .with_write_paths(vec![allowed.path().to_path_buf()])
        .with_import_paths(vec![allowed.path().to_path_buf()])
        .with_watch_paths(vec![allowed.path().to_path_buf()]);

    let mut runtime = Runtime::new().with_script_dir(allowed.path().to_str().unwrap());
    runtime.set_security_policy(policy).expect("policy set");
    runtime.set_trust_mode(TrustMode::Trusted);

    let err = runtime
        .execute_flow(&flow)
        .await
        .expect_err("read must be blocked");
    assert!(err.to_string().contains("Unauthorized Read"));
}

#[tokio::test(flavor = "multi_thread")]
async fn blocks_write_outside_write_paths() {
    let allowed = tempdir().expect("allowed tempdir");
    let denied = tempdir().expect("denied tempdir");

    let denied_output = denied.path().join("blocked.txt");
    let flow = PipeFlow {
        comments: vec![],
        span: Span::default(),
        source: Source::Expression(Expression::Literal(Literal::String("payload".to_string()))),
        operations: vec![(
            PipeOp::Safe,
            Destination::Expression(Expression::Literal(Literal::Path(
                denied_output.to_string_lossy().to_string(),
            ))),
        )],
        on_fail: None,
    };

    let policy = SecurityPolicy::restricted()
        .with_read_paths(vec![allowed.path().to_path_buf()])
        .with_write_paths(vec![allowed.path().to_path_buf()])
        .with_import_paths(vec![allowed.path().to_path_buf()])
        .with_watch_paths(vec![allowed.path().to_path_buf()]);

    let mut runtime = Runtime::new().with_script_dir(allowed.path().to_str().unwrap());
    runtime.set_security_policy(policy).expect("policy set");
    runtime.set_trust_mode(TrustMode::Trusted);

    let err = runtime
        .execute_flow(&flow)
        .await
        .expect_err("write must be blocked");
    assert!(err.to_string().contains("Unauthorized Write"));
}

#[tokio::test(flavor = "multi_thread")]
async fn deny_globs_override_allowlist() {
    let dir = tempdir().expect("tempdir");
    let blocked_output = dir.path().join("secret.txt");

    let flow = PipeFlow {
        comments: vec![],
        span: Span::default(),
        source: Source::Expression(Expression::Literal(Literal::String("payload".to_string()))),
        operations: vec![(
            PipeOp::Safe,
            Destination::Expression(Expression::Literal(Literal::Path(
                blocked_output.to_string_lossy().to_string(),
            ))),
        )],
        on_fail: None,
    };

    let policy = SecurityPolicy::restricted()
        .with_read_paths(vec![dir.path().to_path_buf()])
        .with_write_paths(vec![dir.path().to_path_buf()])
        .with_import_paths(vec![dir.path().to_path_buf()])
        .with_watch_paths(vec![dir.path().to_path_buf()])
        .with_deny_globs(vec!["**/secret.txt".to_string()])
        .expect("valid glob");

    let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().unwrap());
    runtime.set_security_policy(policy).expect("policy set");
    runtime.set_trust_mode(TrustMode::Trusted);

    let err = runtime
        .execute_flow(&flow)
        .await
        .expect_err("deny glob must block write");
    assert!(err.to_string().contains("denied by deny_globs"));
}

#[tokio::test(flavor = "multi_thread")]
async fn restricted_mode_disables_dangerous_operations() {
    let dir = tempdir().expect("tempdir");
    let input = dir.path().join("in.txt");
    std::fs::write(&input, "x").expect("write input");

    let move_flow = PipeFlow {
        comments: vec![],
        span: Span::default(),
        source: Source::Expression(Expression::Literal(Literal::Path(
            input.to_string_lossy().to_string(),
        ))),
        operations: vec![(
            PipeOp::Move,
            Destination::Expression(Expression::Literal(Literal::Path(
                dir.path().join("archive").to_string_lossy().to_string(),
            ))),
        )],
        on_fail: None,
    };

    let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().unwrap());
    runtime.set_trust_mode(TrustMode::Restricted);

    let err = runtime
        .execute_flow(&move_flow)
        .await
        .expect_err("move should be blocked in restricted mode");
    assert!(err.to_string().contains("disabled in restricted mode"));

    let watch_flow = PipeFlow {
        comments: vec![],
        span: Span::default(),
        source: Source::Directive(DirectiveFlow {
            span: Span::default(),
            name: "watch".to_string(),
            arguments: vec![Expression::Literal(Literal::Path(
                dir.path().to_string_lossy().to_string(),
            ))],
            named_arguments: vec![],
            alias: None,
        }),
        operations: vec![],
        on_fail: None,
    };

    let err = runtime
        .execute_flow(&watch_flow)
        .await
        .expect_err("watch should be blocked in restricted mode");
    assert!(err.to_string().contains("disabled in restricted mode"));
}

#[tokio::test(flavor = "multi_thread")]
async fn import_paths_are_enforced() {
    let script_dir = tempdir().expect("script tempdir");
    let external_dir = tempdir().expect("external tempdir");

    let module_path = external_dir.path().join("secret.loom");
    std::fs::write(&module_path, "@fn hello() => \"ok\"").expect("write module");

    let source = format!("@import \"{}\" as ext", module_path.to_string_lossy());
    let program = loom::parser::parse(&source).expect("parse import program");

    let policy = SecurityPolicy::restricted()
        .with_read_paths(vec![script_dir.path().to_path_buf()])
        .with_write_paths(vec![script_dir.path().to_path_buf()])
        .with_import_paths(vec![script_dir.path().to_path_buf()])
        .with_watch_paths(vec![script_dir.path().to_path_buf()]);

    let mut runtime = Runtime::new().with_script_dir(script_dir.path().to_str().unwrap());
    runtime.set_security_policy(policy).expect("policy set");
    runtime.set_trust_mode(TrustMode::Trusted);

    let err = runtime
        .execute(&program)
        .await
        .expect_err("import outside allowlist must be blocked");
    assert!(err.contains("Unauthorized Import"));
}

#[tokio::test(flavor = "multi_thread")]
async fn audit_log_records_side_effect_attempts() {
    let dir = tempdir().expect("tempdir");
    let input = dir.path().join("in.txt");
    let output = dir.path().join("out.txt");
    std::fs::write(&input, "hello").expect("write input");

    let flow = PipeFlow {
        comments: vec![],
        span: Span::default(),
        source: Source::Expression(Expression::Literal(Literal::Path(
            input.to_string_lossy().to_string(),
        ))),
        operations: vec![(
            PipeOp::Safe,
            Destination::Expression(Expression::Literal(Literal::Path(
                output.to_string_lossy().to_string(),
            ))),
        )],
        on_fail: None,
    };

    let policy = SecurityPolicy::restricted()
        .with_read_paths(vec![dir.path().to_path_buf()])
        .with_write_paths(vec![dir.path().to_path_buf()])
        .with_import_paths(vec![dir.path().to_path_buf()])
        .with_watch_paths(vec![dir.path().to_path_buf()])
        .with_deny_globs(vec!["**/out.txt".to_string()])
        .expect("valid glob");

    let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().unwrap());
    runtime.set_security_policy(policy).expect("policy set");
    runtime.set_trust_mode(TrustMode::Trusted);

    let _ = runtime
        .execute_flow(&flow)
        .await
        .expect_err("write denied by glob");

    let has_read_allowed = runtime
        .audit_log()
        .iter()
        .any(|evt| evt.operation == AuditOperation::Read && evt.outcome == AuditOutcome::Allowed);
    let has_write_denied = runtime
        .audit_log()
        .iter()
        .any(|evt| evt.operation == AuditOperation::Write && evt.outcome == AuditOutcome::Denied);

    assert!(has_read_allowed, "audit must include allowed read");
    assert!(has_write_denied, "audit must include denied write");
}
