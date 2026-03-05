use loom::ast::Program;
use loom::parser::parse;
use loom::runtime::security::{
    AuditEvent, AuditOperation, AuditOutcome, SecurityPolicy, TrustMode,
};
use loom::runtime::{Runtime, RuntimeLimits};
use std::path::Path;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::time::sleep;

#[derive(Debug, Clone)]
pub struct FileFixture {
    pub relative_path: String,
    pub contents: String,
}

#[derive(Debug, Clone)]
pub struct QaCase {
    pub name: String,
    pub script: String,
    pub fixtures: Vec<FileFixture>,
    pub limits: Option<RuntimeLimits>,
    pub policy: Option<SecurityPolicy>,
    pub trust_mode: TrustMode,
}

impl QaCase {
    pub fn new(name: impl Into<String>, script: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            script: script.into(),
            fixtures: Vec::new(),
            limits: None,
            policy: None,
            trust_mode: TrustMode::Trusted,
        }
    }

    pub fn with_fixture(
        mut self,
        relative_path: impl Into<String>,
        contents: impl Into<String>,
    ) -> Self {
        self.fixtures.push(FileFixture {
            relative_path: relative_path.into(),
            contents: contents.into(),
        });
        self
    }

    pub fn with_limits(mut self, configure: impl FnOnce(&mut RuntimeLimits)) -> Self {
        let mut limits = RuntimeLimits::default();
        configure(&mut limits);
        self.limits = Some(limits);
        self
    }

    pub fn with_policy(mut self, policy: SecurityPolicy) -> Self {
        self.policy = Some(policy);
        self
    }

    pub fn with_trust_mode(mut self, mode: TrustMode) -> Self {
        self.trust_mode = mode;
        self
    }
}

#[derive(Debug, Clone)]
pub struct QaReport {
    pub name: String,
    pub elapsed: Duration,
    pub result: Result<(), String>,
    pub audit_log: Vec<AuditEvent>,
}

impl QaReport {
    pub fn assert_ok(&self) {
        if let Err(err) = &self.result {
            panic!("case '{}' failed unexpectedly: {}", self.name, err);
        }
    }

    pub fn assert_err_contains(&self, needle: &str) {
        match &self.result {
            Ok(()) => panic!(
                "case '{}' succeeded unexpectedly; expected error containing '{}'",
                self.name, needle
            ),
            Err(err) => assert!(
                err.contains(needle),
                "case '{}' error mismatch. expected '{}', got '{}'",
                self.name,
                needle,
                err
            ),
        }
    }

    pub fn assert_elapsed_under(&self, limit: Duration) {
        assert!(
            self.elapsed <= limit,
            "case '{}' took {:?}, expected <= {:?}",
            self.name,
            self.elapsed,
            limit
        );
    }

    pub fn count_audit(&self, operation: AuditOperation, outcome: AuditOutcome) -> usize {
        self.audit_log
            .iter()
            .filter(|evt| evt.operation == operation && evt.outcome == outcome)
            .count()
    }
}

pub struct QaHarness {
    workspace: TempDir,
}

impl QaHarness {
    pub fn new() -> Self {
        Self {
            workspace: tempfile::tempdir().expect("qa tempdir"),
        }
    }

    pub fn workspace_path(&self) -> &Path {
        self.workspace.path()
    }

    pub fn read_file(&self, relative_path: &str) -> String {
        std::fs::read_to_string(self.workspace.path().join(relative_path))
            .expect("expected output file to exist")
    }

    pub async fn run_case(&self, case: &QaCase) -> QaReport {
        self.run_internal(case, None).await
    }

    pub async fn run_case_with_shutdown(
        &self,
        case: &QaCase,
        shutdown_after: Duration,
    ) -> QaReport {
        self.run_internal(case, Some(shutdown_after)).await
    }

    async fn run_internal(&self, case: &QaCase, shutdown_after: Option<Duration>) -> QaReport {
        self.seed_fixtures(&case.fixtures);
        let program = parse_program(&case.script);
        let mut runtime = Runtime::new().with_script_dir(&self.workspace.path().to_string_lossy());
        runtime.set_trust_mode(case.trust_mode);
        if let Some(limits) = &case.limits {
            runtime.limits = limits.clone();
        }
        let default_policy = SecurityPolicy::restricted()
            .with_read_paths(vec![self.workspace.path().to_path_buf()])
            .with_write_paths(vec![self.workspace.path().to_path_buf()])
            .with_import_paths(vec![self.workspace.path().to_path_buf()])
            .with_watch_paths(vec![self.workspace.path().to_path_buf()]);
        runtime
            .set_security_policy(case.policy.clone().unwrap_or(default_policy))
            .expect("security policy");

        let started = Instant::now();
        let result = if let Some(delay) = shutdown_after {
            let shutdown = runtime.shutdown_trigger();
            let execute = runtime.execute(&program);
            tokio::pin!(execute);
            tokio::select! {
                res = &mut execute => res,
                _ = sleep(delay) => {
                    let _ = shutdown.send(true);
                    execute.await
                }
            }
        } else {
            runtime.execute(&program).await
        };

        QaReport {
            name: case.name.clone(),
            elapsed: started.elapsed(),
            result,
            audit_log: runtime.audit_log().to_vec(),
        }
    }

    fn seed_fixtures(&self, fixtures: &[FileFixture]) {
        for fixture in fixtures {
            let absolute = self.workspace.path().join(&fixture.relative_path);
            if let Some(parent) = absolute.parent() {
                std::fs::create_dir_all(parent).expect("fixture parent dir");
            }
            std::fs::write(absolute, &fixture.contents).expect("fixture write");
        }
    }
}

fn parse_program(script: &str) -> Program {
    parse(script).unwrap_or_else(|errors| {
        let formatted = errors
            .iter()
            .map(|e| format!("{}:{} {}", e.line, e.col, e.message))
            .collect::<Vec<_>>()
            .join("\n");
        panic!("failed to parse qa case script:\n{formatted}");
    })
}
