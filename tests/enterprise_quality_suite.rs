mod qa;

use loom::runtime::security::{AuditOperation, AuditOutcome, SecurityPolicy, TrustMode};
use qa::{QaCase, QaHarness};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread")]
async fn enterprise_suite_validates_complex_output_and_performance() {
    let harness = QaHarness::new();
    let case = QaCase::new(
        "csv-filter-output",
        "\"orders.csv\" >> @csv.parse >> filter(row >> row.amount > 1000) >> \"high_value.csv\"",
    )
    .with_fixture(
        "orders.csv",
        "id,amount,owner\n1,20,amy\n2,2000,bob\n3,4500,sam\n",
    )
    .with_limits(|limits| {
        limits.timeout_budget = Duration::from_secs(5);
    });

    let report = harness.run_case(&case).await;
    report.assert_ok();
    report.assert_elapsed_under(Duration::from_secs(2));

    let output = harness.read_file("high_value.csv");
    assert!(
        output.contains("bob"),
        "missing expected filtered row for bob"
    );
    assert!(
        output.contains("sam"),
        "missing expected filtered row for sam"
    );
    assert!(
        !output.contains("amy"),
        "unexpected low-value row present in output"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn enterprise_suite_rejects_pipeline_memory_overflow() {
    let harness = QaHarness::new();
    let case = QaCase::new("memory-guardrail", "\"wide.csv\" >> @csv.parse")
        .with_fixture(
            "wide.csv",
            "id,payload\n1,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n2,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\n",
        )
        .with_limits(|limits| {
            limits.max_pipeline_memory_bytes = 64;
            limits.timeout_budget = Duration::from_secs(5);
        });

    let report = harness.run_case(&case).await;
    report.assert_err_contains("Pipeline memory estimate exceeded");
}

#[tokio::test(flavor = "multi_thread")]
async fn enterprise_suite_enforces_filesystem_sandbox_and_audits_denials() {
    let harness = QaHarness::new();
    let policy = SecurityPolicy::restricted()
        .with_read_paths(vec![harness.workspace_path().to_path_buf()])
        .with_write_paths(vec![harness.workspace_path().to_path_buf()])
        .with_import_paths(vec![harness.workspace_path().to_path_buf()])
        .with_watch_paths(vec![harness.workspace_path().to_path_buf()]);

    let case = QaCase::new(
        "sandbox-deny-parent-write",
        "\"input.txt\" >> \"../blocked.txt\"",
    )
    .with_fixture("input.txt", "payload")
    .with_policy(policy);

    let report = harness.run_case(&case).await;
    report.assert_err_contains("Unauthorized Write");
    assert_eq!(
        report.count_audit(AuditOperation::Write, AuditOutcome::Denied),
        1,
        "expected one denied write audit event"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn enterprise_suite_watch_flow_is_interruptible_without_hanging() {
    let harness = QaHarness::new();
    let case = QaCase::new("watch-shutdown", "@watch(\".\")")
        .with_trust_mode(TrustMode::Trusted)
        .with_limits(|limits| {
            limits.timeout_budget = Duration::from_secs(30);
            limits.watch_queue_capacity = 32;
        });

    let report = harness
        .run_case_with_shutdown(&case, Duration::from_millis(120))
        .await;
    report.assert_ok();
    report.assert_elapsed_under(Duration::from_secs(2));
}
