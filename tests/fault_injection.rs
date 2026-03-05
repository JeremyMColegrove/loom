use loom::ast::{Destination, DirectiveFlow, Expression, Literal, PipeFlow, PipeOp, Source, Span};
use loom::runtime::Runtime;
use loom::runtime::security::{SecurityPolicy, TrustMode};
use tempfile::tempdir;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

#[tokio::test(flavor = "multi_thread")]
async fn write_fails_with_permission_denied() {
    let dir = tempdir().expect("tempdir");
    let locked_dir = dir.path().join("locked");
    std::fs::create_dir_all(&locked_dir).expect("create locked dir");

    #[cfg(unix)]
    {
        let mut perms = std::fs::metadata(&locked_dir)
            .expect("metadata")
            .permissions();
        perms.set_mode(0o555);
        std::fs::set_permissions(&locked_dir, perms).expect("set readonly permissions");
    }

    let target_path = locked_dir.join("out.txt");
    let flow = PipeFlow {
        comments: vec![],
        span: Span::default(),
        source: Source::Expression(Expression::Literal(Literal::String("payload".to_string()))),
        operations: vec![(
            PipeOp::Safe,
            Destination::Expression(Expression::Literal(Literal::Path(
                target_path.to_string_lossy().to_string(),
            ))),
        )],
        on_fail: None,
    };

    let mut runtime = Runtime::new();
    runtime
        .set_security_policy(SecurityPolicy::allow_all())
        .expect("set allow-all policy");
    runtime.set_trust_mode(TrustMode::Trusted);
    let result = runtime.execute_flow(&flow).await;

    #[cfg(unix)]
    {
        assert!(result.is_err(), "permission-denied write should fail");
        let msg = result.err().map(|e| e.to_string()).unwrap_or_default();
        assert!(
            msg.contains("Failed to open") || msg.contains("Permission denied"),
            "unexpected error: {msg}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn write_fails_when_disk_is_full_device() {
    let full_device = std::path::Path::new("/dev/full");
    if !full_device.exists() {
        return;
    }

    let flow = PipeFlow {
        comments: vec![],
        span: Span::default(),
        source: Source::Expression(Expression::Literal(Literal::String(
            "large-payload".to_string(),
        ))),
        operations: vec![(
            PipeOp::Force,
            Destination::Expression(Expression::Literal(Literal::Path("/dev/full".to_string()))),
        )],
        on_fail: None,
    };

    let mut runtime = Runtime::new();
    runtime
        .set_security_policy(SecurityPolicy::allow_all())
        .expect("set allow-all policy");
    runtime.set_trust_mode(TrustMode::Trusted);
    let result = runtime.execute_flow(&flow).await;
    assert!(result.is_err(), "write to /dev/full should fail");
}

#[tokio::test(flavor = "multi_thread")]
async fn move_fails_when_source_disappears_before_rename() {
    let dir = tempdir().expect("tempdir");
    let input_path = dir.path().join("input.txt");
    std::fs::write(&input_path, "payload").expect("write input");

    // Simulate a rename race: source vanishes between discovery and rename call.
    std::fs::remove_file(&input_path).expect("remove input");

    let flow = PipeFlow {
        comments: vec![],
        span: Span::default(),
        source: Source::Expression(Expression::Literal(Literal::Path(
            input_path.to_string_lossy().to_string(),
        ))),
        operations: vec![(
            PipeOp::Move,
            Destination::Expression(Expression::Literal(Literal::Path(
                dir.path().join("archive").to_string_lossy().to_string(),
            ))),
        )],
        on_fail: None,
    };

    let mut runtime = Runtime::new();
    runtime
        .set_security_policy(SecurityPolicy::allow_all())
        .expect("set allow-all policy");
    runtime.set_trust_mode(TrustMode::Trusted);
    let result = runtime.execute_flow(&flow).await;

    assert!(result.is_err(), "missing source rename should fail");
    let msg = result.err().map(|e| e.to_string()).unwrap_or_default();
    assert!(
        msg.contains("Failed to move") || msg.contains("Failed to resolve path"),
        "unexpected error: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn watch_option_validation_rejects_invalid_debounce() {
    let watch = DirectiveFlow {
        span: Span::default(),
        name: "watch".to_string(),
        arguments: vec![
            Expression::Literal(Literal::Path(".".to_string())),
            Expression::Literal(Literal::Number(-5.0)),
        ],
        alias: Some("event".to_string()),
    };

    let flow = PipeFlow {
        comments: vec![],
        span: Span::default(),
        source: Source::Directive(watch),
        operations: vec![],
        on_fail: None,
    };

    let mut runtime = Runtime::new();
    let result = runtime.execute_flow(&flow).await;

    assert!(result.is_err(), "invalid debounce must fail fast");
    let msg = result.err().map(|e| e.to_string()).unwrap_or_default();
    assert!(
        msg.contains("non-negative"),
        "unexpected watch validation message: {msg}"
    );
}
